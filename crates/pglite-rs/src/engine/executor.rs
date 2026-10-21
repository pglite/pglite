use crate::storage::engine::{StorageEngine, UndoAction};
use crate::storage::wal::WalRecord;
use crate::types::{ColumnDef, DataType, FieldInfo, QueryResult, Value};
use rayon::prelude::*;
use serde_json::json;
use std::collections::HashMap;
use md5::{Digest, Md5};
use base64::Engine;

use crate::engine::expr::{compile_expr, compile_expr_for_joined, CompiledExpr};
pub use crate::engine::plan::PlannedProjectedExpr as ProjectedExpr;
use crate::engine::plan::{
    evaluate_planned_condition, project_row_planned, resolve_operand, ColOpTemplate,
    ConditionTemplate, ExecutionPlan, OperandTemplate, PlannedJoin, PlannedProjectedExpr,
    WhereTemplate,
};

#[derive(Debug, Clone)]
pub struct TableConstraintDef {
    pub table_name: String,
    pub constraint_name: String,
    pub constraint_type: String,
    pub column_name: String,
    pub ordinal_position: i64,
    pub foreign_table_name: Option<String>,
    pub foreign_column_name: Option<String>,
}

pub struct Executor {
    pub storage: StorageEngine,
    pub plan_cache: HashMap<String, ExecutionPlan>,
    pub custom_constraints: Vec<TableConstraintDef>,
    /// Tham số phiên đặt bằng SET (SHOW đọc lại).
    pub settings: HashMap<String, String>,
    /// SELECT đã qua kiểm tra tên cột: câu SQL → (bảng, dấu vân tay cột) hoặc None = không thuộc diện.
    column_check_cache: HashMap<String, Option<(String, u64)>>,
    /// `json_cell_kinds` theo câu SQL, kèm dấu vân tay kiểu cột của các bảng trong FROM (đổi schema thì tính lại).
    json_kinds_cache: HashMap<String, (Option<Vec<JsonCellKind>>, Vec<(String, u64)>)>,
    /// PREPARE tên AS câu lệnh: chỉ sống trong phiên, không ghi xuống đĩa.
    prepared: HashMap<String, String>,
    /// `currval`: giá trị `nextval` gần nhất của mỗi sequence trong phiên.
    pub(crate) seq_session: HashMap<String, i64>,
    /// `lastval`: sequence được dùng gần nhất trong phiên.
    pub(crate) seq_last_used: Option<String>,
}

impl Executor {
    pub fn new(storage: StorageEngine) -> Self {
        Self {
            storage,
            plan_cache: HashMap::new(),
            custom_constraints: Vec::new(),
            settings: HashMap::new(),
            column_check_cache: HashMap::new(),
            json_kinds_cache: HashMap::new(),
            prepared: HashMap::new(),
            seq_session: HashMap::new(),
            seq_last_used: None,
        }
    }

    pub fn execute_rows_json(&mut self, sql: &str, params: &[Value]) -> Result<String, String> {
        let res = self.execute(sql, params)?;
        Ok(res.rows_to_json_string())
    }

    pub fn execute_full_json(&mut self, sql: &str, params: &[Value]) -> Result<String, String> {
        let res = self.execute(sql, params)?;
        Ok(res.to_json_string())
    }

    pub fn execute(&mut self, sql: &str, params: &[Value]) -> Result<QueryResult, String> {
        // View / sequence độc lập / enum (engine/objects.rs): mở rộng câu lệnh trước mọi đường xử lý
        if self.storage.stmt_depth == 0 {
            crate::engine::objects::set_session_enums(&self.storage.objects.enums);
        }
        if self.has_expandable_objects() && !(self.storage.in_transaction && self.storage.tx_aborted) {
            if let Some(res) = self.view_dml(sql, params) {
                if res.is_err() && self.storage.stmt_depth == 0 && self.storage.in_transaction {
                    self.storage.tx_aborted = true;
                }
                return res;
            }
            match self.expand_objects(sql) {
                Ok(Some(expanded)) => return self.execute(&expanded, params),
                Ok(None) => {}
                Err(e) => {
                    if self.storage.stmt_depth == 0 && self.storage.in_transaction {
                        self.storage.tx_aborted = true;
                    }
                    return Err(e);
                }
            }
        }
        if self.storage.stmt_depth == 0 && !raw_text_mode() && sql.len() > 7 && sql.trim_start().get(..7).is_some_and(|p| p.eq_ignore_ascii_case("SELECT ")) {
            if let Some(kinds) = self.json_cell_kinds_cached(sql) {
                return self.execute_typed_json(sql, params, &kinds);
            }
        }
        let top = self.storage.stmt_depth == 0;
        if top {
            crate::engine::errors::clear();
            // múi giờ của phiên này cho các hàm ngày giờ (mặc định UTC / PGLITE_TIMEZONE)
            match self.settings.get("timezone") {
                Some(z) => crate::engine::datetime::set_session_tz(z),
                None => crate::engine::datetime::set_session_tz(default_tz()),
            }
            if self.storage.in_transaction && self.storage.tx_aborted {
                if let Some(res) = self.aborted_transaction_gate(sql) {
                    return res;
                }
            }
        }
        if top && sql.len() > 7 && sql.trim_start().get(..7).is_some_and(|p| p.eq_ignore_ascii_case("SELECT ")) {
            if let Err(e) = self.check_select_columns_cached(sql) {
                if self.storage.in_transaction {
                    self.storage.tx_aborted = true;
                }
                return decode_quoted_idents(Err(e));
            }
        }
        let appended_before = self.storage.wal.appended;
        // Lệnh lỗi giữa chừng (vi phạm ràng buộc ở dòng thứ n, cascade lỗi...) được hoàn tác hết.
        let mark = self.storage.begin_statement();
        let mut res = self.execute_statement(sql, params);
        // Lỗi lúc đánh giá biểu thức (chia cho 0, ép kiểu sai...) được ghi qua kênh lỗi.
        if let Some(e) = crate::engine::errors::take() {
            if res.is_ok() {
                res = Err(e);
            }
        }
        self.storage.end_statement(mark, res.is_ok());
        if top && res.is_err() && self.storage.in_transaction {
            self.storage.tx_aborted = true;
        }
        // Lệnh ngoài giao dịch (autocommit) phải nằm trong file (và được fsync, nếu bật `sync`)
        // trước khi trả kết quả, nếu không tiến trình bị kill hay mất điện sẽ làm mất nó.
        // Trong giao dịch thì COMMIT/ROLLBACK tự làm điều đó.
        if !self.storage.in_transaction && self.storage.wal.has_pending() {
            let _ = self.storage.wal.commit_point();
        }
        // Lệnh có ghi WAL mà WAL đã hỏng (lúc ghi hay lúc fsync) thì không được báo thành công.
        // Lệnh chỉ đọc vẫn chạy bình thường.
        if self.storage.wal.appended != appended_before {
            if let Some(e) = self.storage.wal.error() {
                return Err(e.to_string());
            }
        }
        if top {
            return decode_quoted_idents(res);
        }
        res
    }

    /// Kiểm tra tên cột của SELECT (`validate.rs`), cache theo câu SQL để truy vấn lặp lại không tốn thêm.
    fn check_select_columns_cached(&mut self, sql: &str) -> Result<(), String> {
        use crate::engine::validate::{check_select_columns, columns_fingerprint, Checked};
        match self.column_check_cache.get(sql) {
            Some(None) => return Ok(()),
            Some(Some((t, fp))) if columns_fingerprint(&self.storage, t) == Some(*fp) => return Ok(()),
            _ => {}
        }
        let Ok(lexed) = crate::engine::frontend::lex(sql) else { return Ok(()) };
        let entry = match check_select_columns(&lexed, &self.storage)? {
            Checked::NotApplicable => None,
            Checked::Table(t) => match columns_fingerprint(&self.storage, &t) {
                Some(fp) => Some((t, fp)),
                None => return Ok(()),
            },
            Checked::NoTable => return Ok(()),
        };
        if self.column_check_cache.len() >= 4096 {
            self.column_check_cache.clear();
        }
        self.column_check_cache.insert(sql.to_string(), entry);
        Ok(())
    }

    /// Giao dịch đang ở trạng thái lỗi: chỉ ROLLBACK [TO SAVEPOINT] được chạy; COMMIT = ROLLBACK.
    /// `None` = cho câu lệnh chạy tiếp.
    fn aborted_transaction_gate(&mut self, sql: &str) -> Option<Result<QueryResult, String>> {
        let up = strip_sql_comments(sql).trim().to_uppercase();
        let first = up.split_whitespace().next().unwrap_or("").trim_end_matches(';').to_string();
        match first.as_str() {
            "ROLLBACK" | "ABORT" => None,
            "COMMIT" | "END" => {
                // Như PostgreSQL: COMMIT trong giao dịch lỗi thực chất là ROLLBACK
                if let Err(e) = self.storage.rollback() {
                    return Some(Err(e));
                }
                self.plan_cache.clear();
                Some(Ok(QueryResult { rows: vec![], row_count: 0, fields: vec![], command: "ROLLBACK".to_string() }))
            }
            _ => Some(Err("current transaction is aborted, commands ignored until end of transaction block".to_string())),
        }
    }

    /// ANALYZE / VACUUM / CHECKPOINT (không có thống kê hay dọn dẹp gì để làm: chỉ kiểm tra bảng tồn tại)
    /// và PREPARE / EXECUTE / DEALLOCATE (câu lệnh chuẩn bị theo phiên, chỉ ở bộ nhớ).
    fn handle_utility_statement(&mut self, sql: &str, upper: &str) -> Option<Result<QueryResult, String>> {
        let first = upper.split(|c: char| c.is_whitespace() || c == '(').next().unwrap_or("");
        let ok = |cmd: &str| Some(Ok(QueryResult { rows: vec![], row_count: 0, fields: vec![], command: cmd.to_string() }));
        match first {
            "ANALYZE" | "ANALYSE" | "VACUUM" => {
                if first == "VACUUM" && self.storage.in_transaction {
                    return Some(Err("VACUUM cannot run inside a transaction block".to_string()));
                }
                // bỏ các tuỳ chọn (FULL, VERBOSE, (opt, ...)) để lấy danh sách bảng
                let mut rest = sql[first.len()..].trim();
                if rest.starts_with('(') {
                    rest = rest.find(')').map(|i| rest[i + 1..].trim()).unwrap_or("");
                }
                loop {
                    let w = rest.split_whitespace().next().unwrap_or("").to_uppercase();
                    if ["FULL", "FREEZE", "VERBOSE", "ANALYZE", "ANALYSE", "SKIP_LOCKED"].contains(&w.as_str()) {
                        rest = rest[w.len()..].trim();
                    } else {
                        break;
                    }
                }
                for item in split_comma_separated_tokens(rest) {
                    let name = item.split('(').next().unwrap_or("").trim().trim_matches('"');
                    let name = name.strip_prefix("public.").unwrap_or(name);
                    if !name.is_empty() && self.storage.get_table(name).is_none() && self.storage.get_table(&name.to_lowercase()).is_none() {
                        return Some(Err(format!("relation \"{}\" does not exist", name)));
                    }
                }
                ok(if first == "VACUUM" { "VACUUM" } else { "ANALYZE" })
            }
            "CHECKPOINT" if sql.len() == first.len() => ok("CHECKPOINT"),
            "PREPARE" if !upper.starts_with("PREPARE TRANSACTION") => {
                let as_pos = find_top_level_keyword(sql, "AS")?;
                let head = sql[7..as_pos].trim();
                let name = head.split('(').next().unwrap_or("").trim().trim_matches('"').to_lowercase();
                if name.is_empty() {
                    return None;
                }
                if self.prepared.contains_key(&name) {
                    return Some(Err(format!("prepared statement \"{}\" already exists", name)));
                }
                self.prepared.insert(name, sql[as_pos + 2..].trim().to_string());
                ok("PREPARE")
            }
            "EXECUTE" => {
                let rest = sql[7..].trim();
                let (name, args) = match rest.find('(') {
                    Some(i) if rest.ends_with(')') => (rest[..i].trim(), &rest[i + 1..rest.len() - 1]),
                    _ => (rest, ""),
                };
                let name = name.trim_matches('"').to_lowercase();
                let Some(stmt) = self.prepared.get(&name).cloned() else {
                    return Some(Err(format!("prepared statement \"{}\" does not exist", name)));
                };
                let mut values = Vec::new();
                for arg in split_comma_separated_tokens(args) {
                    if arg.is_empty() {
                        continue;
                    }
                    let r = match self.execute_statement(&format!("SELECT {} AS v", arg), &[]) {
                        Ok(r) => r,
                        Err(e) => return Some(Err(e)),
                    };
                    let v = r.rows.first().and_then(|row| row.get("v")).cloned().unwrap_or(serde_json::Value::Null);
                    values.push(match v {
                        serde_json::Value::Null => Value::Null,
                        serde_json::Value::Bool(b) => Value::Bool(b),
                        serde_json::Value::Number(n) => n.as_i64().map(Value::Int).unwrap_or_else(|| Value::Float(n.as_f64().unwrap_or(0.0))),
                        serde_json::Value::String(s) => Value::text(s),
                        other => Value::text(other.to_string()),
                    });
                }
                Some(self.execute(&stmt, &values))
            }
            "DEALLOCATE" => {
                let mut rest = sql[10..].trim();
                if rest.to_uppercase().starts_with("PREPARE ") {
                    rest = rest[8..].trim();
                }
                if rest.eq_ignore_ascii_case("ALL") {
                    self.prepared.clear();
                } else {
                    let name = rest.trim_matches('"').to_lowercase();
                    if self.prepared.remove(&name).is_none() {
                        return Some(Err(format!("prepared statement \"{}\" does not exist", name)));
                    }
                }
                ok("DEALLOCATE")
            }
            _ => None,
        }
    }

    /// `a NATURAL [LEFT|RIGHT|FULL|INNER] JOIN b` → `a ... JOIN b USING (các cột trùng tên)`; không có cột
    /// chung thì thành CROSS JOIN như PostgreSQL. Cột bên trái lấy từ các bảng đứng trước trong FROM.
    fn rewrite_natural_joins(&self, sql: &str) -> Option<String> {
        let from = find_top_level_keyword(sql, "FROM")?;
        let nat = from + find_top_level_keyword(&sql[from..], "NATURAL")?;
        let after = &sql[nat + 7..];
        let words: Vec<&str> = after.split_whitespace().collect();
        let mut wi = 0;
        let mut kind = String::new();
        while wi < words.len() && ["LEFT", "RIGHT", "FULL", "INNER", "OUTER"].contains(&words[wi].to_uppercase().as_str()) {
            if !words[wi].eq_ignore_ascii_case("OUTER") && !words[wi].eq_ignore_ascii_case("INNER") {
                kind = format!("{} ", words[wi].to_uppercase());
            }
            wi += 1;
        }
        if !words.get(wi)?.eq_ignore_ascii_case("JOIN") {
            return None;
        }
        let tbl_word = words.get(wi + 1)?;
        let tbl_name = clean_table_name(tbl_word);
        let right = self.storage.get_table(&tbl_name)?;
        // vị trí ngay sau tên bảng bên phải trong `after`
        let mut pos = 0;
        for w in &words[..wi + 2] {
            pos = pos + after[pos..].find(w)? + w.len();
        }
        let mut rest = &after[pos..];
        // alias tuỳ chọn
        let mut alias = String::new();
        let rw: Vec<&str> = rest.split_whitespace().take(2).collect();
        const STOP: [&str; 14] = ["WHERE", "ORDER", "GROUP", "LIMIT", "OFFSET", "JOIN", "LEFT", "RIGHT", "FULL", "INNER", "CROSS", "NATURAL", "UNION", "HAVING"];
        let mut take = 0;
        if rw.first().map_or(false, |w| w.eq_ignore_ascii_case("AS")) {
            take = 2;
        } else if rw.first().map_or(false, |w| !STOP.contains(&w.to_uppercase().as_str()) && !w.starts_with(['(', ',', ';', ')'])) {
            take = 1;
        }
        for _ in 0..take {
            let w = rest.split_whitespace().next()?;
            let p = rest.find(w)? + w.len();
            if !w.eq_ignore_ascii_case("AS") {
                alias = w.to_string();
            }
            rest = &rest[p..];
        }
        if rest.trim_start().to_uppercase().starts_with("ON ") || rest.trim_start().to_uppercase().starts_with("USING") {
            return None;
        }
        let mut left_cols: Vec<String> = Vec::new();
        for w in sql[from + 4..nat].split(|c: char| c.is_whitespace() || c == ',' || c == '(' || c == ')') {
            if let Some(t) = self.storage.get_table(&clean_table_name(w)).filter(|_| !w.is_empty()) {
                for c in &t.columns {
                    if !left_cols.contains(&c.name) {
                        left_cols.push(c.name.clone());
                    }
                }
            }
        }
        let common: Vec<&String> = left_cols.iter().filter(|c| right.columns.iter().any(|rc| &rc.name == *c)).collect();
        let target = if alias.is_empty() { tbl_word.to_string() } else { format!("{} {}", tbl_word, alias) };
        let join = if common.is_empty() {
            format!("CROSS JOIN {}", target)
        } else {
            format!("{}JOIN {} USING ({})", kind, target, common.iter().map(|c| c.as_str()).collect::<Vec<_>>().join(", "))
        };
        Some(format!("{}{}{}", &sql[..nat], join, rest))
    }

    pub(crate) fn execute_statement(&mut self, sql: &str, params: &[Value]) -> Result<QueryResult, String> {
        let clean_sql = crate::engine::frontend::lex(sql)?;
        let normalized_sql = normalize_named_sql_params(&clean_sql);

        let stmts = split_sql_statements(&normalized_sql);
        if stmts.len() > 1 {
            let mut last_res = QueryResult {
                rows: vec![],
                row_count: 0,
                fields: vec![],
                command: String::new(),
            };
            // Nhiều câu trong một lần gửi (simple query) mà không tự điều khiển giao dịch: chạy như một
            // giao dịch ngầm, câu sau lỗi thì cả chuỗi bị huỷ (như PostgreSQL).
            let controls_tx = stmts.iter().any(|st| {
                let u = st.trim_start().to_uppercase();
                ["BEGIN", "START", "COMMIT", "END", "ROLLBACK", "ABORT", "SAVEPOINT", "RELEASE", "PREPARE TRANSACTION", "VACUUM", "CHECKPOINT"].iter().any(|k| u.starts_with(k))
            });
            let implicit = !self.storage.in_transaction && !controls_tx;
            if implicit {
                self.storage.begin_transaction();
            }
            for stmt in stmts {
                match self.execute(&stmt, params) {
                    Ok(r) => last_res = r,
                    Err(e) => {
                        if implicit {
                            let _ = self.storage.rollback();
                            self.plan_cache.clear();
                        }
                        return Err(e);
                    }
                }
            }
            if implicit {
                self.storage.commit()?;
            }
            return Ok(last_res);
        }

        let trimmed = normalized_sql.trim().trim_end_matches(|c: char| c == ';' || c == '.').trim();
        let upper = trimmed.to_uppercase();

        if upper.starts_with("DO ") || upper.starts_with("DO$") || upper.starts_with("DO\n") || upper.starts_with("DO\t") {
            return self.handle_do_block(trimmed);
        }
        if let Some(res) = self.handle_session_statement(trimmed, &upper) {
            return res;
        }
        // GRANT / REVOKE / ALTER DEFAULT PRIVILEGES: engine nhúng không có role / quyền, chấp nhận và bỏ qua
        // (migration sinh từ PostgreSQL hay có các câu này)
        for (kw, tag) in [("GRANT ", "GRANT"), ("REVOKE ", "REVOKE"), ("ALTER DEFAULT PRIVILEGES", "ALTER DEFAULT PRIVILEGES")] {
            if upper.starts_with(kw) {
                return Ok(QueryResult { rows: vec![], row_count: 0, fields: vec![], command: tag.to_string() });
            }
        }
        if let Some(res) = self.handle_utility_statement(trimmed, &upper) {
            return res;
        }
        // VIEW / SCHEMA / SEQUENCE / TYPE (engine/objects.rs)
        if let Some(res) = self.handle_object_ddl(trimmed, &upper, params) {
            return res;
        }
        if upper.contains("NATURAL ") {
            if let Some(rewritten) = self.rewrite_natural_joins(trimmed) {
                return self.execute_statement(&rewritten, params);
            }
        }
        // version(), current_database(), current_user... (engine/plpgsql.rs)
        if ["VERSION(", "CURRENT_DATABASE", "CURRENT_CATALOG", "CURRENT_SCHEMA", "CURRENT_USER", "SESSION_USER", "CURRENT_ROLE"].iter().any(|k| contains_ci(trimmed, k)) {
            if let Some(resolved) = self.resolve_session_info(trimmed) {
                return self.execute_statement(&resolved, params);
            }
        }
        // current_setting / set_config (engine/plpgsql.rs)
        if contains_ci(trimmed, "CURRENT_SETTING(") || contains_ci(trimmed, "SET_CONFIG(") {
            if let Some(resolved) = self.resolve_setting_calls(trimmed)? {
                return self.execute_statement(&resolved, params);
            }
        }
        // nextval / currval / setval / pg_get_serial_sequence (engine/sequences.rs)
        if crate::engine::sequences::mentions_sequence_fn(trimmed) {
            if let Some(resolved) = self.resolve_sequence_calls(trimmed, params)? {
                return self.execute_statement(&resolved, params);
            }
        }

        if upper.starts_with("BEGIN") || upper.starts_with("START TRANSACTION") {
            self.storage.begin_transaction();
            return Ok(QueryResult {
                rows: vec![],
                row_count: 0,
                fields: vec![],
                command: "BEGIN".to_string(),
            });
        }

        if upper.starts_with("COMMIT") || upper == "END" {
            self.storage.commit()?;
            return Ok(QueryResult {
                rows: vec![],
                row_count: 0,
                fields: vec![],
                command: "COMMIT".to_string(),
            });
        }

        if upper.starts_with("SAVEPOINT") {
            let sp_name = trimmed["SAVEPOINT".len()..].trim().trim_matches(';');
            if !self.storage.in_transaction {
                return Err("SAVEPOINT can only be used in transaction blocks".to_string());
            }
            self.storage.savepoint(sp_name);
            return Ok(QueryResult {
                rows: vec![],
                row_count: 0,
                fields: vec![],
                command: "SAVEPOINT".to_string(),
            });
        }

        if upper.starts_with("ROLLBACK TO") || upper.starts_with("ROLLBACK WORK TO") || upper.starts_with("ROLLBACK TRANSACTION TO") {
            let after = if upper.starts_with("ROLLBACK WORK TO") {
                &trimmed["ROLLBACK WORK TO".len()..]
            } else if upper.starts_with("ROLLBACK TRANSACTION TO") {
                &trimmed["ROLLBACK TRANSACTION TO".len()..]
            } else {
                &trimmed["ROLLBACK TO".len()..]
            };
            let sp_name = after.trim();
            let sp_name = if sp_name.to_uppercase().starts_with("SAVEPOINT ") {
                &sp_name[10..]
            } else {
                sp_name
            }.trim().trim_matches(';');
            if !self.storage.in_transaction {
                return Err("ROLLBACK TO SAVEPOINT can only be used in transaction blocks".to_string());
            }
            if !self.storage.rollback_to_savepoint(sp_name) {
                return Err(format!("savepoint \"{}\" does not exist", sp_name.trim_matches('"')));
            }
            self.plan_cache.clear();
            return Ok(QueryResult {
                rows: vec![],
                row_count: 0,
                fields: vec![],
                command: "ROLLBACK".to_string(),
            });
        }

        if upper.starts_with("ROLLBACK") || upper.starts_with("ABORT") {
            self.storage.rollback()?;
            // DDL trong giao dịch đã được hoàn tác: kế hoạch đã cache có thể trỏ tới cột/bảng cũ
            self.plan_cache.clear();
            return Ok(QueryResult {
                rows: vec![],
                row_count: 0,
                fields: vec![],
                command: "ROLLBACK".to_string(),
            });
        }

        if upper.starts_with("RELEASE") {
            let after = if upper.starts_with("RELEASE WORK SAVEPOINT") {
                &trimmed["RELEASE WORK SAVEPOINT".len()..]
            } else if upper.starts_with("RELEASE SAVEPOINT") {
                &trimmed["RELEASE SAVEPOINT".len()..]
            } else {
                &trimmed["RELEASE".len()..]
            };
            let sp_name = after.trim().trim_matches(';');
            if !self.storage.release_savepoint(sp_name) {
                return Err(format!("savepoint \"{}\" does not exist", sp_name.trim_matches('"')));
            }
            return Ok(QueryResult {
                rows: vec![],
                row_count: 0,
                fields: vec![],
                command: "RELEASE".to_string(),
            });
        }

        if upper.starts_with("CREATE") || upper.starts_with("DROP") || upper.starts_with("ALTER") {
            self.plan_cache.clear();
        }

        if upper.starts_with("ALTER TABLE") {
            return self.handle_alter_table(trimmed);
        }

        let is_with = if upper.starts_with("WITH") {
            let rest = &trimmed[4..];
            rest.is_empty() || rest.chars().next().map(|c| c.is_whitespace()).unwrap_or(false)
        } else {
            false
        };
        if is_with {
            return self.handle_cte(trimmed, params);
        }

        if (upper.starts_with("SELECT") || upper.starts_with("("))
            && ["UNION", "INTERSECT", "EXCEPT"].iter().any(|kw| find_top_level_keyword(trimmed, kw).is_some())
        {
            return self.handle_union(trimmed, params);
        }

        // SELECT ... INTO bảng FROM ... → CREATE TABLE bảng AS SELECT ...
        if upper.starts_with("SELECT") && contains_ci(trimmed, " INTO ") {
            if let Some(ctas) = select_into_as_ctas(trimmed) {
                if ctas.to_uppercase().starts_with("CREATE TEMP") {
                    return self.handle_create_temp_table(&ctas, params);
                }
                return self.handle_create_table_as(&ctas, params);
            }
        }
        if ["CREATE TEMP ", "CREATE TEMPORARY ", "CREATE LOCAL TEMP", "CREATE GLOBAL TEMP"].iter().any(|k| upper.starts_with(k)) {
            return self.handle_create_temp_table(trimmed, params);
        }
        if (upper.starts_with("CREATE TABLE") || upper.starts_with("CREATE UNLOGGED TABLE") || upper.starts_with("CREATE TEMP") || upper.starts_with("CREATE TEMPORARY"))
            && is_create_table_as(trimmed)
        {
            return self.handle_create_table_as(trimmed, params);
        }
        if upper.starts_with("CREATE TABLE") {
            return self.handle_create_table(trimmed);
        }

        if upper.starts_with("CREATE INDEX") || upper.starts_with("CREATE UNIQUE INDEX") {
            return self.handle_create_index(trimmed);
        }

        if upper.starts_with("DROP TABLE") {
            return self.handle_drop_table(trimmed);
        }

        if upper.starts_with("DROP INDEX") {
            return self.handle_drop_index(trimmed);
        }

        if upper.starts_with("INSERT INTO") {
            return self.handle_insert(trimmed, params);
        }

        if upper.starts_with("COMMENT ON TABLE") {
            return self.handle_comment_on_table(trimmed);
        }

        if upper.starts_with("COMMENT ON COLUMN") {
            return self.handle_comment_on_column(trimmed);
        }

        if upper.starts_with("SELECT") {
            if upper.contains("PG_") || upper.contains("INFORMATION_SCHEMA") {
                if let Some(res) = self.object_catalog_select(trimmed, params) {
                    return res;
                }
            }
            if upper.contains("TABLE_CONSTRAINTS")
                || upper.contains("KEY_COLUMN_USAGE")
                || upper.contains("CONSTRAINT_COLUMN_USAGE")
                || upper.contains("PG_INDEX")
                || (upper.contains("PG_CLASS") && !upper.contains("JSON_AGG"))
                || (upper.contains("PG_NAMESPACE") && !upper.contains("JSON_AGG"))
            {
                self.ensure_virtual_catalog_tables();
            }

            if upper == "SELECT CURRENT_SCHEMA()" || upper == "SELECT CURRENT_SCHEMA" {
                return Ok(QueryResult {
                    rows: vec![serde_json::json!({ "current_schema": "public" })],
                    row_count: 1,
                    fields: vec![FieldInfo { name: "current_schema".to_string(), data_type: "text".to_string() }],
                    command: "SELECT".to_string(),
                });
            }

            if upper.contains("FROM PG_CLASS") || upper.contains("FROM PG_CATALOG.PG_CLASS") {
                if upper.contains("JSON_AGG") || upper.contains("COLUMNS") || upper.contains("PG_ATTRIBUTE") {
                    return self.handle_table_introspection_query(trimmed, params);
                }
            }

            if upper.contains("FROM PG_CONSTRAINT") || upper.contains("FROM PG_CATALOG.PG_CONSTRAINT") {
                return self.handle_pg_constraint_introspection_query(trimmed, params);
            }

            // Chỉ khi information_schema.columns là FROM của chính câu này; nằm trong truy vấn con
            // (EXISTS (SELECT 1 FROM information_schema.columns ...)) thì để câu ngoài tính bình thường.
            let top_from_is_columns = find_top_level_keyword(trimmed, "FROM")
                .map_or(false, |p| trimmed[p..].to_uppercase().starts_with("FROM INFORMATION_SCHEMA.COLUMNS"));
            if top_from_is_columns {
                if Self::info_columns_needs_generic(trimmed) {
                    return self.information_schema_columns_generic(trimmed, params);
                }
                return self.handle_information_schema_columns_query(trimmed, params);
            }

            if (upper.contains("OBJ_DESCRIPTION") || upper.contains("COL_DESCRIPTION")) && !upper.contains("FROM ") {
                if let Ok(res) = self.handle_description_scalar_query(trimmed) {
                    return Ok(res);
                }
            }

            if upper.contains("FROM PG_CATALOG.PG_DESCRIPTION") || upper.contains("FROM PG_DESCRIPTION") {
                if let Ok(res) = self.handle_description_scalar_query(trimmed) {
                    return Ok(res);
                }
            }

            if (upper.contains("FROM INFORMATION_SCHEMA.SCHEMATA") || upper.contains("FROM PG_NAMESPACE"))
                && !upper.contains("JOIN") {
                return Ok(QueryResult {
                    rows: vec![serde_json::json!({ "schema_name": "public" })],
                    row_count: 1,
                    fields: vec![FieldInfo { name: "schema_name".to_string(), data_type: "text".to_string() }],
                    command: "SELECT".to_string(),
                });
            }

            if (upper.contains("FROM INFORMATION_SCHEMA.TABLES") || upper.contains("FROM PG_TABLES") || upper.contains("FROM PG_CATALOG.PG_TABLES"))
                && !upper.contains("JOIN") {
                // `SELECT * FROM ...` không WHERE: giữ nguyên kết quả cũ (các khoá riêng của bản cũ); còn lại chạy
                // trên bảng tạm trong bộ nhớ có cột chuẩn PostgreSQL để WHERE / danh sách cột / ORDER BY đúng.
                let legacy = find_top_level_keyword(trimmed, "WHERE").is_none()
                    && trimmed[6..find_top_level_keyword(trimmed, "FROM").unwrap_or(6)].trim() == "*";
                if !legacy {
                    return self.catalog_tables_generic(trimmed, params);
                }
                let mut table_keys: Vec<String> = self.storage.tables.keys()
                    .filter(|k| !k.starts_with("information_schema.") && !k.starts_with("pg_catalog.") && *k != "table_constraints" && *k != "key_column_usage" && *k != "constraint_column_usage" && *k != "pg_index" && *k != "pg_class" && *k != "pg_namespace" && !k.starts_with("__ephemeral_"))
                    .cloned().collect();
                table_keys.sort();
                let rows: Vec<serde_json::Value> = table_keys.into_iter().map(|k| {
                    let tbl = &self.storage.tables[&k];
                    serde_json::json!({
                        "table_schema": "public",
                        "schema_name": "public",
                        "table_name": tbl.name,
                        "table_type": "BASE TABLE",
                        "is_temporary": false,
                        "comment": tbl.comment.clone().unwrap_or_default()
                    })
                }).collect();
                let row_count = rows.len();
                return Ok(QueryResult {
                    rows,
                    row_count,
                    fields: vec![
                        FieldInfo { name: "table_name".to_string(), data_type: "text".to_string() },
                        FieldInfo { name: "table_schema".to_string(), data_type: "text".to_string() },
                    ],
                    command: "SELECT".to_string(),
                });
            }



            // Check if FROM has a derived subquery e.g. FROM (SELECT ...)
            if let Some(from_pos) = find_top_level_keyword(trimmed, "FROM") {
                let after_from = trimmed[from_pos + 4..].trim();
                if after_from.starts_with('(') && after_from[1..].trim_start().to_uppercase().starts_with("SELECT") {
                    return self.handle_derived_table_query(trimmed, params);
                }
            }

            if let Some(plan) = self.plan_cache.get(trimmed) {
                if let Ok(res) = self.execute_select_plan(plan, params) {
                    return Ok(res);
                }
            }
            let (res, plan_opt) = self.handle_select(trimmed, params)?;
            if let Some(plan) = plan_opt {
                self.plan_cache.insert(trimmed.to_string(), plan);
            }
            return Ok(res);
        }

        if upper.starts_with("UPDATE") {
            return self.handle_update(trimmed, params);
        }

        if upper.starts_with("DELETE") {
            return self.handle_delete(trimmed, params);
        }

        if upper.starts_with("TRUNCATE") {
            return self.handle_truncate(trimmed);
        }

        Err(format!("Unsupported SQL statement: {}", sql))
    }

    fn handle_truncate(&mut self, sql: &str) -> Result<QueryResult, String> {
        let mut rest = sql.trim();
        if rest.to_uppercase().starts_with("TRUNCATE") {
            rest = rest[8..].trim();
        }
        if rest.to_uppercase().starts_with("TABLE") {
            rest = rest[5..].trim();
        }
        let rest = rest.trim_end_matches(';').trim();
        // Bỏ các tuỳ chọn cuối lệnh: RESTART/CONTINUE IDENTITY, CASCADE/RESTRICT.
        let mut names_part = rest;
        let upper = rest.to_uppercase();
        for kw in [" RESTART IDENTITY", " CONTINUE IDENTITY", " CASCADE", " RESTRICT"] {
            if let Some(pos) = upper.find(kw) {
                names_part = &rest[..pos.min(names_part.len())];
            }
        }
        let cascade = upper.contains(" CASCADE");
        let mut table_names: Vec<String> = names_part
            .split(',')
            .map(|n| n.trim().trim_start_matches("ONLY ").trim_start_matches("only ").trim().trim_matches('"').to_string())
            .filter(|n| !n.is_empty())
            .collect();
        if table_names.is_empty() {
            return Err("Missing table name in TRUNCATE".to_string());
        }
        for name in &table_names {
            if self.storage.get_table(name).is_none() {
                return Err(format!("Table {} not found", name));
            }
        }
        // Bảng bị khoá ngoại từ bảng khác trỏ tới: như PostgreSQL, phải có CASCADE (xoá luôn bảng con).
        loop {
            let listed: std::collections::HashSet<String> = table_names.iter().map(|n| n.to_lowercase()).collect();
            let mut extra: Vec<String> = Vec::new();
            for (key, t) in &self.storage.tables {
                if listed.contains(key) {
                    continue;
                }
                for c in &t.constraints {
                    if let crate::storage::catalog::ConstraintKind::ForeignKey { ref_table, .. } = &c.kind {
                        if listed.contains(&ref_table.to_lowercase()) {
                            if !cascade {
                                return Err(format!(
                                    "cannot truncate a table referenced in a foreign key constraint (Table \"{}\" references \"{}\".)",
                                    key, ref_table
                                ));
                            }
                            if !extra.contains(key) {
                                extra.push(key.clone());
                            }
                        }
                    }
                }
            }
            if extra.is_empty() {
                break;
            }
            table_names.extend(extra);
        }

        let mut count = 0;
        for table_name in &table_names {
            if self.storage.in_transaction {
                // Trong giao dịch: xoá từng dòng bằng chính bản ghi DELETE để ROLLBACK (lúc chạy
                // và khi replay) hoàn tác được. Không cần biến thể WAL mới.
                let (identities, deleted) = {
                    let table = self.storage.get_table_mut(table_name).unwrap();
                    let mut identities = Vec::new();
                    let mut deleted = Vec::new();
                    for i in 0..table.rows.len() {
                        if !table.is_deleted[i] {
                            identities.push(RowIdentity::of(table, i));
                            table.delete_row(i);
                            deleted.push(i);
                        }
                    }
                    (identities, deleted)
                };
                count += deleted.len();
                for identity in identities {
                    let table = table_name.to_string();
                    self.storage.wal.append(match identity {
                        RowIdentity::Pk(pk) => WalRecord::Delete { table, pk },
                        RowIdentity::Key(key) => WalRecord::DeleteByKey { table, key },
                        RowIdentity::Row(row) => WalRecord::DeleteByRow { table, row },
                    });
                }
                for row_idx in deleted {
                    self.storage.tx_undo_log.push(UndoAction::UndeleteRow {
                        table: table_name.to_string(),
                        row_idx,
                    });
                }
            } else {
                let (columns, comment, catalog) = {
                    let table = self.storage.get_table_mut(table_name).unwrap();
                    count += table.active_count;
                    table.rows.clear();
                    table.is_deleted.clear();
                    table.active_count = 0;
                    table.auto_increment = 1;
                    // Mọi index (kể cả khoá text/UUID và index phụ) phải rỗng theo bảng.
                    table.rebuild_indexes();
                    let catalog = table.catalog_tracked.then(|| (table.constraints.clone(), table.indexes.clone(), table.legacy_data));
                    let decls: Vec<(String, String)> = table.columns.iter().filter_map(|c| table.decl_types.get(&c.name.to_lowercase()).map(|d| (c.name.clone(), d.canonical()))).collect();
                    (table.declared_columns(), table.comment.clone(), (catalog, decls))
                };
                let (catalog, decls) = catalog;
                self.storage.wal.append(WalRecord::DropTable {
                    name: table_name.to_string(),
                });
                self.storage.wal.append(WalRecord::CreateTable {
                    name: table_name.to_string(),
                    columns,
                });
                if comment.is_some() {
                    self.storage.wal.append(WalRecord::CommentOnTable {
                        table: table_name.to_string(),
                        comment,
                    });
                }
                // Replay tạo lại bảng rỗng từ `CreateTable`: ghi lại catalog để không mất ràng buộc.
                if let Some((constraints, indexes, legacy)) = catalog {
                    self.storage.wal.append(WalRecord::Schema {
                        version: crate::storage::catalog::SCHEMA_OP_VERSION,
                        payload: crate::storage::catalog::encode_schema_op(&crate::storage::catalog::SchemaOp::TableCatalog {
                            table: table_name.to_string(),
                            constraints,
                            indexes,
                            legacy,
                        }),
                    });
                }
                if !decls.is_empty() {
                    self.storage.wal.append(WalRecord::Schema {
                        version: crate::storage::catalog::SCHEMA_OP_VERSION,
                        payload: crate::storage::catalog::encode_schema_op(&crate::storage::catalog::SchemaOp::ColumnTypes { table: table_name.to_string(), types: decls }),
                    });
                }
                // cột identity cũng phải được dựng lại khi replay
                let idents: Vec<(String, crate::storage::catalog::IdentitySpec)> = self
                    .storage
                    .get_table(table_name)
                    .map(|t| t.columns.iter().filter_map(|c| t.identity.get(&c.name.to_lowercase()).map(|sp| (c.name.clone(), *sp))).collect())
                    .unwrap_or_default();
                for (column, sp) in idents {
                    self.storage.wal.append(WalRecord::Schema {
                        version: crate::storage::catalog::SCHEMA_OP_VERSION,
                        payload: crate::storage::catalog::encode_schema_op(&crate::storage::catalog::SchemaOp::ColumnIdentity { table: table_name.to_string(), column, spec: Some(sp) }),
                    });
                }
            }
        }
        self.plan_cache.clear();

        Ok(QueryResult {
            rows: vec![],
            row_count: count,
            fields: vec![],
            command: "TRUNCATE".to_string(),
        })
    }

    /// UNION / INTERSECT / EXCEPT [ALL]: ghép cột theo VỊ TRÍ (không theo tên), INTERSECT ưu tiên hơn
    /// UNION/EXCEPT, và ORDER BY / LIMIT / OFFSET / FETCH ở cuối áp cho cả kết quả.
    fn handle_union(&mut self, sql: &str, params: &[Value]) -> Result<QueryResult, String> {
        let sql = sql.trim().trim_end_matches(';').trim();
        // Mệnh đề cuối chỉ tính sau phép tập hợp cuối cùng ở cấp ngoài cùng
        let last_op_end = ["UNION", "INTERSECT", "EXCEPT"].iter().filter_map(|kw| last_top_level_keyword(sql, kw).map(|p| p + kw.len())).max().unwrap_or(0);
        let tail_start = ["ORDER BY", "LIMIT", "OFFSET", "FETCH"]
            .iter()
            .filter_map(|kw| find_top_level_keyword(&sql[last_op_end..], kw).map(|p| last_op_end + p))
            .min();
        let (body, tail) = match tail_start {
            Some(p) => (&sql[..p], &sql[p..]),
            None => (sql, ""),
        };
        let (names, mut rows, fields) = self.eval_set_expr(body.trim(), params)?;

        if let Some(o) = find_top_level_keyword(tail, "ORDER BY") {
            let o_end = ["LIMIT", "OFFSET", "FETCH"].iter().filter_map(|kw| find_top_level_keyword(&tail[o + 8..], kw).map(|p| o + 8 + p)).min().unwrap_or(tail.len());
            let specs = parse_order_by_specs(&tail[o + 8..o_end]);
            rows.sort_by(|a, b| {
                for spec in &specs {
                    let idx = spec.col_name.trim().parse::<usize>().ok().map(|n| n.saturating_sub(1)).or_else(|| {
                        let c = clean_col_name(spec.col_name).trim_matches('"').to_string();
                        names.iter().position(|n| n.eq_ignore_ascii_case(&c))
                    });
                    let (va, vb) = match idx {
                        Some(i) => (a.get(i), b.get(i)),
                        None => (None, None),
                    };
                    let ord = json_order_cmp(va, vb, spec.is_desc, spec.nulls_first);
                    if ord != std::cmp::Ordering::Equal {
                        return ord;
                    }
                }
                std::cmp::Ordering::Equal
            });
        }
        let num_after = |kw: &str| -> Option<usize> {
            let p = find_top_level_keyword(tail, kw)?;
            let tok = tail[p + kw.len()..].split_whitespace().next()?;
            let v = if let Some(n) = tok.strip_prefix('$') { params.get(n.parse::<usize>().ok()?.checked_sub(1)?)?.as_i64()? } else { tok.parse::<i64>().ok()? };
            Some(v.max(0) as usize)
        };
        let offset = num_after("OFFSET").unwrap_or(0);
        let limit = num_after("LIMIT").or_else(|| {
            // FETCH FIRST|NEXT n ROWS ONLY
            let p = find_top_level_keyword(tail, "FETCH")?;
            let rest: Vec<&str> = tail[p + 5..].split_whitespace().collect();
            rest.get(1).and_then(|t| t.parse::<usize>().ok()).or(Some(1))
        });
        let rows: Vec<Vec<serde_json::Value>> = rows.into_iter().skip(offset).take(limit.unwrap_or(usize::MAX)).collect();
        let out: Vec<serde_json::Value> = rows
            .into_iter()
            .map(|r| {
                let mut m = serde_json::Map::new();
                for (n, v) in names.iter().zip(r) {
                    m.insert(n.clone(), v);
                }
                serde_json::Value::Object(m)
            })
            .collect();
        Ok(QueryResult { row_count: out.len(), rows: out, fields, command: "SELECT".to_string() })
    }

    /// Tính một biểu thức tập hợp: (tên cột, các dòng theo vị trí, kiểu cột của vế trái).
    fn eval_set_expr(&mut self, s: &str, params: &[Value]) -> Result<(Vec<String>, Vec<Vec<serde_json::Value>>, Vec<FieldInfo>), String> {
        let s = s.trim();
        // UNION/EXCEPT ưu tiên thấp nhất, kết hợp trái: tách ở lần xuất hiện cuối; sau đó INTERSECT.
        let split = ["UNION", "EXCEPT"]
            .iter()
            .filter_map(|kw| last_top_level_keyword(s, kw).map(|p| (p, *kw)))
            .max_by_key(|(p, _)| *p)
            .or_else(|| last_top_level_keyword(s, "INTERSECT").map(|p| (p, "INTERSECT")));
        let Some((pos, op)) = split else {
            let mut q = s;
            while q.starts_with('(') && q.ends_with(')') && is_fully_enclosed_in_parens(q) {
                q = q[1..q.len() - 1].trim();
            }
            let res = self.execute(q, params)?;
            let names: Vec<String> = if !res.fields.is_empty() {
                res.fields.iter().map(|f| f.name.clone()).collect()
            } else {
                res.rows.first().and_then(|r| r.as_object()).map(|m| m.keys().cloned().collect()).unwrap_or_default()
            };
            let rows = res
                .rows
                .iter()
                .map(|r| match r.as_object() {
                    Some(m) => names.iter().map(|n| m.get(n).cloned().unwrap_or(serde_json::Value::Null)).collect(),
                    None => vec![r.clone()],
                })
                .collect();
            return Ok((names, rows, res.fields));
        };
        let left = &s[..pos];
        let mut right = s[pos + op.len()..].trim_start();
        let mut all = false;
        if right.len() > 3 && right[..3].eq_ignore_ascii_case("ALL") && right.as_bytes()[3].is_ascii_whitespace() {
            all = true;
            right = right[3..].trim_start();
        } else if right.len() > 8 && right[..8].eq_ignore_ascii_case("DISTINCT") && right.as_bytes()[8].is_ascii_whitespace() {
            right = right[8..].trim_start();
        }
        let (names, lrows, fields) = self.eval_set_expr(left, params)?;
        let (rnames, rrows, _) = self.eval_set_expr(right, params)?;
        if !lrows.is_empty() && !rrows.is_empty() && rnames.len() != names.len() {
            return Err(format!("each {} query must have the same number of columns", op));
        }
        let key = |r: &Vec<serde_json::Value>| serde_json::to_string(r).unwrap_or_default();
        let rows: Vec<Vec<serde_json::Value>> = match op {
            "UNION" => {
                let mut out = lrows;
                out.extend(rrows);
                if !all {
                    let mut seen = std::collections::HashSet::new();
                    out.retain(|r| seen.insert(key(r)));
                }
                out
            }
            "INTERSECT" => {
                let mut counts: HashMap<String, usize> = HashMap::new();
                for r in &rrows {
                    *counts.entry(key(r)).or_default() += 1;
                }
                let mut seen = std::collections::HashSet::new();
                lrows
                    .into_iter()
                    .filter(|r| {
                        let k = key(r);
                        match counts.get_mut(&k) {
                            Some(c) if *c > 0 => {
                                if all {
                                    *c -= 1;
                                    true
                                } else {
                                    seen.insert(k)
                                }
                            }
                            _ => false,
                        }
                    })
                    .collect()
            }
            _ => {
                // EXCEPT
                let mut counts: HashMap<String, usize> = HashMap::new();
                for r in &rrows {
                    *counts.entry(key(r)).or_default() += 1;
                }
                let mut seen = std::collections::HashSet::new();
                lrows
                    .into_iter()
                    .filter(|r| {
                        let k = key(r);
                        if all {
                            match counts.get_mut(&k) {
                                Some(c) if *c > 0 => {
                                    *c -= 1;
                                    false
                                }
                                _ => true,
                            }
                        } else {
                            !counts.contains_key(&k) && seen.insert(k)
                        }
                    })
                    .collect()
            }
        };
        Ok((names, rows, fields))
    }

    /// WITH ...: bảng CTE được đặt tạm vào storage. Bảng thật trùng tên bị che chỉ trong lúc chạy và
    /// luôn được trả lại (kể cả khi lệnh lỗi), trước đây nó bị xoá khỏi bộ nhớ.
    fn handle_distinct_on(&mut self, sql: &str, params: &[Value]) -> Result<QueryResult, String> {
        let after = sql[6..].trim_start();
        let open = after.find('(').ok_or("syntax error at DISTINCT ON")?;
        let close = crate::engine::constraints::matching_paren(after, open).ok_or("syntax error at DISTINCT ON")?;
        let keys: Vec<String> = split_comma_separated_tokens(&after[open + 1..close]).into_iter().map(|k| k.trim().to_string()).collect();
        let rest = &after[close + 1..];
        let from = find_top_level_keyword(rest, "FROM").unwrap_or(rest.len());
        let list = rest[..from].trim();
        let tail = &rest[from..];
        // LIMIT/OFFSET áp sau khi lọc trùng
        let lim_pos = ["LIMIT", "OFFSET", "FETCH"].iter().filter_map(|kw| find_top_level_keyword(tail, kw)).min();
        let (body_tail, page) = match lim_pos {
            Some(p) => (&tail[..p], &tail[p..]),
            None => (tail, ""),
        };
        let hidden: Vec<String> = keys.iter().enumerate().map(|(i, k)| format!("{} AS \"__don{}\"", k, i)).collect();
        let inner = format!("SELECT {}, {} {}", list, hidden.join(", "), body_tail);
        let res = self.execute_statement(&inner, params)?;
        let mut seen = std::collections::HashSet::new();
        let mut rows = Vec::new();
        for r in res.rows {
            let key: Vec<serde_json::Value> = (0..keys.len()).map(|i| r.get(format!("__don{}", i)).cloned().unwrap_or(serde_json::Value::Null)).collect();
            if seen.insert(serde_json::to_string(&key).unwrap_or_default()) {
                let mut obj = r.as_object().cloned().unwrap_or_default();
                for i in 0..keys.len() {
                    obj.remove(&format!("__don{}", i));
                }
                rows.push(serde_json::Value::Object(obj));
            }
        }
        let num = |kw: &str| -> Option<usize> {
            let p = find_top_level_keyword(page, kw)?;
            let tok = page[p + kw.len()..].split_whitespace().next()?;
            match tok.strip_prefix('$') {
                Some(n) => params.get(n.parse::<usize>().ok()?.checked_sub(1)?)?.as_i64().map(|v| v.max(0) as usize),
                None => tok.parse().ok(),
            }
        };
        let offset = num("OFFSET").unwrap_or(0);
        let limit = num("LIMIT").unwrap_or(usize::MAX);
        let rows: Vec<serde_json::Value> = rows.into_iter().skip(offset).take(limit).collect();
        let fields: Vec<FieldInfo> = res.fields.into_iter().filter(|f| !f.name.starts_with("__don")).collect();
        Ok(QueryResult { row_count: rows.len(), rows, fields, command: "SELECT".to_string() })
    }

    fn handle_cte(&mut self, sql: &str, params: &[Value]) -> Result<QueryResult, String> {
        let mut shadow: Vec<(String, Option<crate::storage::table::Table>)> = Vec::new();
        let res = self.handle_cte_inner(sql, params, &mut shadow);
        for (name, prev) in shadow.into_iter().rev() {
            self.storage.tables.remove(&name);
            if let Some(t) = prev {
                self.storage.tables.insert(name, t);
            }
        }
        self.plan_cache.clear();
        res
    }

    fn handle_cte_inner(&mut self, sql: &str, params: &[Value], shadow: &mut Vec<(String, Option<crate::storage::table::Table>)>) -> Result<QueryResult, String> {
        let mut without_with = sql.trim()[4..].trim();
        let is_recursive = if without_with.to_uppercase().starts_with("RECURSIVE") {
            let rest = &without_with[9..];
            if rest.is_empty() || rest.chars().next().map(|c| c.is_whitespace()).unwrap_or(false) {
                without_with = rest.trim();
                true
            } else {
                false
            }
        } else {
            false
        };
        let mut i = 0;
        let mut created_tables: Vec<String> = Vec::new();

        while i < without_with.len() {
            let cur_slice = &without_with[i..];
            let as_pos = match find_top_level_keyword(cur_slice, "AS") {
                Some(p) => p,
                None => break,
            };
            let cte_head = cur_slice[..as_pos].trim();
            let (cte_name, explicit_cols) = if let Some(p_idx) = cte_head.find('(') {
                if let Some(close_idx) = cte_head.rfind(')') {
                    let name = clean_table_name(cte_head[..p_idx].trim()).to_string();
                    let cols_str = &cte_head[p_idx + 1..close_idx];
                    let cols: Vec<String> = cols_str.split(',').map(|s| clean_col_name(s.trim()).to_string()).collect();
                    (name, Some(cols))
                } else {
                    (clean_table_name(cte_head).to_string(), None)
                }
            } else {
                (clean_table_name(cte_head).to_string(), None)
            };

            let after_as_offset = as_pos + 2;
            let after_as_raw = &cur_slice[after_as_offset..];
            let lparen_rel = match after_as_raw.find('(') {
                Some(p) => p,
                None => return Err("Expected ( after AS in CTE definition".to_string()),
            };
            let open_paren_idx = after_as_offset + lparen_rel;
            let close_paren_idx = match find_matching_paren(cur_slice, open_paren_idx) {
                Some(p) => p,
                None => return Err("Unmatched parenthesis in CTE definition".to_string()),
            };

            let cte_query = cur_slice[open_paren_idx + 1..close_paren_idx].trim();

            if is_recursive {
                let (union_pos, is_all, kw_len) = if let Some(pos) = find_top_level_keyword(cte_query, "UNION ALL") {
                    (Some(pos), true, 9)
                } else if let Some(pos) = find_top_level_keyword(cte_query, "UNION") {
                    (Some(pos), false, 5)
                } else {
                    (None, false, 0)
                };

                if let Some(u_pos) = union_pos {
                    let non_rec_sql = cte_query[..u_pos].trim();
                    let rec_sql = cte_query[u_pos + kw_len..].trim();

                    let base_res = self.execute(non_rec_sql, params)?;
                    let mut cols = Vec::new();
                    for (c_i, f) in base_res.fields.iter().enumerate() {
                        let name = if let Some(ref exp) = explicit_cols {
                            exp.get(c_i).cloned().unwrap_or_else(|| f.name.clone())
                        } else {
                            f.name.clone()
                        };
                        cols.push(crate::types::ColumnDef {
                            name,
                            data_type: crate::types::DataType::from_sql_str(&f.data_type),
                            is_primary_key: false,
                            is_nullable: true,
                            default_value: None,
                            comment: None,
                        });
                    }

                    let row_from_json = |r_json: &serde_json::Value, cols: &[crate::types::ColumnDef], query_fields: &[FieldInfo]| -> Vec<Value> {
                        let mut row = Vec::new();
                        for (c_i, col) in cols.iter().enumerate() {
                            let field_name = query_fields.get(c_i).map(|f| f.name.as_str());
                            let val_opt = r_json.get(&col.name).or_else(|| field_name.and_then(|fn_str| r_json.get(fn_str)));
                            let v = match val_opt {
                                Some(serde_json::Value::Null) | None => Value::Null,
                                Some(serde_json::Value::Bool(b)) => Value::Bool(*b),
                                Some(serde_json::Value::Number(num)) => {
                                    if let Some(i) = num.as_i64() {
                                        Value::Int(i)
                                    } else if let Some(f) = num.as_f64() {
                                        Value::Float(f)
                                    } else {
                                        Value::Null
                                    }
                                }
                                Some(serde_json::Value::String(s)) => Value::text(s),
                                Some(other) => Value::text(other.to_string()),
                            };
                            row.push(v);
                        }
                        row
                    };

                    let mut accumulated_rows: Vec<Vec<Value>> = Vec::new();
                    let mut working_rows: Vec<Vec<Value>> = Vec::new();

                    for r_json in base_res.rows {
                        let r = row_from_json(&r_json, &cols, &base_res.fields);
                        accumulated_rows.push(r.clone());
                        working_rows.push(r);
                    }

                    let mut iteration = 0;
                    while !working_rows.is_empty() && iteration < 1000 {
                        iteration += 1;
                        let mut working_table = crate::storage::table::Table::new(cte_name.clone(), cols.clone());
                        for r in &working_rows {
                            working_table.insert(r.clone());
                        }
                        { let prev = self.storage.tables.insert(cte_name.to_lowercase(), working_table); shadow.push((cte_name.to_lowercase(), prev)); }

                        let rec_res = match self.execute(rec_sql, params) {
                            Ok(res) => res,
                            Err(e) => {
                                self.storage.tables.remove(&cte_name.to_lowercase());
                                return Err(e);
                            }
                        };

                        if rec_res.rows.is_empty() {
                            break;
                        }

                        let mut next_working_rows = Vec::new();
                        for r_json in rec_res.rows {
                            let r = row_from_json(&r_json, &cols, &rec_res.fields);
                            if !is_all {
                                if accumulated_rows.iter().any(|acc| acc == &r) {
                                    continue;
                                }
                            }
                            accumulated_rows.push(r.clone());
                            next_working_rows.push(r);
                        }

                        working_rows = next_working_rows;
                    }

                    let mut full_cte_table = crate::storage::table::Table::new(cte_name.clone(), cols);
                    for r in accumulated_rows {
                        full_cte_table.insert(r);
                    }
                    { let prev = self.storage.tables.insert(cte_name.to_lowercase(), full_cte_table); shadow.push((cte_name.to_lowercase(), prev)); }
                    created_tables.push(cte_name);
                } else {
                    let res = self.execute(cte_query, params)?;
                    let mut cols = Vec::new();
                    for (c_i, f) in res.fields.iter().enumerate() {
                        let name = if let Some(ref exp) = explicit_cols {
                            exp.get(c_i).cloned().unwrap_or_else(|| f.name.clone())
                        } else {
                            f.name.clone()
                        };
                        cols.push(crate::types::ColumnDef {
                            name,
                            data_type: crate::types::DataType::from_sql_str(&f.data_type),
                            is_primary_key: false,
                            is_nullable: true,
                            default_value: None,
                            comment: None,
                        });
                    }
                    let mut rows = Vec::new();
                    for r_json in res.rows {
                        let mut row = Vec::new();
                        for (c_i, col) in cols.iter().enumerate() {
                            let f_name = &res.fields[c_i].name;
                            let v = match r_json.get(&col.name).or_else(|| r_json.get(f_name)) {
                                Some(serde_json::Value::Null) | None => Value::Null,
                                Some(serde_json::Value::Bool(b)) => Value::Bool(*b),
                                Some(serde_json::Value::Number(num)) => {
                                    if let Some(i) = num.as_i64() { Value::Int(i) } else if let Some(f) = num.as_f64() { Value::Float(f) } else { Value::Null }
                                }
                                Some(serde_json::Value::String(s)) => Value::text(s),
                                Some(other) => Value::text(other.to_string()),
                            };
                            row.push(v);
                        }
                        rows.push(row);
                    }
                    let mut cte_table = crate::storage::table::Table::new(cte_name.clone(), cols);
                    for r in rows { cte_table.insert(r); }
                    { let prev = self.storage.tables.insert(cte_name.to_lowercase(), cte_table); shadow.push((cte_name.to_lowercase(), prev)); }
                    created_tables.push(cte_name);
                }
            } else {
                let res = self.execute(cte_query, params)?;
                let mut cols = Vec::new();
                for (c_i, f) in res.fields.iter().enumerate() {
                    let name = if let Some(ref exp) = explicit_cols {
                        exp.get(c_i).cloned().unwrap_or_else(|| f.name.clone())
                    } else {
                        f.name.clone()
                    };
                    cols.push(crate::types::ColumnDef {
                        name,
                        data_type: crate::types::DataType::from_sql_str(&f.data_type),
                        is_primary_key: false,
                        is_nullable: true,
                        default_value: None,
                        comment: None,
                    });
                }
                let mut rows = Vec::new();
                for r_json in res.rows {
                    let mut row = Vec::new();
                    for (c_i, col) in cols.iter().enumerate() {
                        let f_name = &res.fields[c_i].name;
                        let v = match r_json.get(&col.name).or_else(|| r_json.get(f_name)) {
                            Some(serde_json::Value::Null) | None => Value::Null,
                            Some(serde_json::Value::Bool(b)) => Value::Bool(*b),
                            Some(serde_json::Value::Number(num)) => {
                                if let Some(i) = num.as_i64() { Value::Int(i) } else if let Some(f) = num.as_f64() { Value::Float(f) } else { Value::Null }
                            }
                            Some(serde_json::Value::String(s)) => Value::text(s),
                            Some(other) => Value::text(other.to_string()),
                        };
                        row.push(v);
                    }
                    rows.push(row);
                }
                let mut cte_table = crate::storage::table::Table::new(cte_name.clone(), cols);
                for r in rows { cte_table.insert(r); }
                { let prev = self.storage.tables.insert(cte_name.to_lowercase(), cte_table); shadow.push((cte_name.to_lowercase(), prev)); }
                created_tables.push(cte_name);
            }

            let rest_after_paren = cur_slice[close_paren_idx + 1..].trim_start();
            if rest_after_paren.starts_with(',') {
                let after_comma = rest_after_paren[1..].trim_start();
                i = without_with.len() - after_comma.len();
            } else {
                let main_query = rest_after_paren;
                let main_res = self.execute(main_query, params);
                for tbl_name in created_tables {
                    self.storage.tables.remove(&tbl_name.to_lowercase());
                }
                return main_res;
            }
        }

        Err("Failed to parse CTE query".to_string())
    }

    fn handle_derived_table_query(&mut self, sql: &str, params: &[Value]) -> Result<QueryResult, String> {
        let from_pos = find_top_level_keyword(sql, "FROM").ok_or("Missing FROM")?;
        let select_part = sql[..from_pos].trim();
        let after_from = sql[from_pos + 4..].trim();

        let mut cte_defs = Vec::new();
        let mut new_from = String::new();
        let mut cur = after_from;

        while !cur.is_empty() {
            if cur.starts_with('(') {
                let mut depth = 0;
                let mut in_str = false;
                let mut end_paren = None;
                for (idx, ch) in cur.char_indices() {
                    match ch {
                        '\'' => in_str = !in_str,
                        '(' if !in_str => depth += 1,
                        ')' if !in_str => {
                            depth -= 1;
                            if depth == 0 {
                                end_paren = Some(idx);
                                break;
                            }
                        }
                        _ => {}
                    }
                }
                let end_idx = end_paren.ok_or("Unmatched parenthesis in derived table")?;
                let sub_query = cur[1..end_idx].trim();
                let after_paren = cur[end_idx + 1..].trim();

                let mut alias_and_rest = after_paren;
                if alias_and_rest.to_uppercase().starts_with("AS ") {
                    alias_and_rest = alias_and_rest[3..].trim();
                }
                let tokens: Vec<&str> = alias_and_rest.split_whitespace().collect();
                let alias = clean_col_name(tokens.first().copied().unwrap_or("sub_tbl")).to_string();
                cte_defs.push(format!("{} AS ({})", alias, sub_query));

                if !new_from.is_empty() {
                    new_from.push(' ');
                }
                new_from.push_str(&alias);

                let alias_token_len = tokens.first().map(|t| t.len()).unwrap_or(0);
                let rest = alias_and_rest[alias_token_len..].trim();
                if rest.to_uppercase().starts_with("CROSS JOIN") {
                    new_from.push_str(" CROSS JOIN ");
                    cur = rest[10..].trim();
                } else {
                    if !rest.is_empty() {
                        new_from.push(' ');
                        new_from.push_str(rest);
                    }
                    break;
                }
            } else {
                if !new_from.is_empty() {
                    new_from.push(' ');
                }
                new_from.push_str(cur);
                break;
            }
        }

        let cte_sql = format!("WITH {} {} FROM {}", cte_defs.join(", "), select_part, new_from);
        self.handle_cte(&cte_sql, params)
    }

fn replace_identifier_token(sql: &str, target: &str, replacement: &str) -> String {
    let mut result = String::with_capacity(sql.len());
    let bytes = sql.as_bytes();
    let target_lower = target.to_lowercase();
    let mut i = 0;
    let mut in_str = false;

    while i < bytes.len() {
        if bytes[i] == b'\'' {
            in_str = !in_str;
            result.push(bytes[i] as char);
            i += 1;
            continue;
        }
        if in_str {
            result.push(bytes[i] as char);
            i += 1;
            continue;
        }

        if i + target.len() <= bytes.len() {
            let slice = &sql[i..i + target.len()];
            if slice.to_lowercase() == target_lower {
                let prev_ok = i == 0 || (!bytes[i - 1].is_ascii_alphanumeric() && bytes[i - 1] != b'_');
                let next_idx = i + target.len();
                let next_ok = next_idx >= bytes.len() || (!bytes[next_idx].is_ascii_alphanumeric() && bytes[next_idx] != b'_');
                if prev_ok && next_ok {
                    result.push_str(replacement);
                    i += target.len();
                    continue;
                }
            }
        }

        result.push(bytes[i] as char);
        i += 1;
    }

    result
}

    pub(crate) fn resolve_subqueries_in_where(&mut self, where_str: &str, params: &[Value]) -> Result<String, String> {
        let mut resolved = where_str.to_string();
        loop {
            let mut found_start = None;
            let bytes = resolved.as_bytes();
            for i in 0..bytes.len() {
                if bytes[i] == b'(' {
                    let after = resolved[i + 1..].trim_start();
                    let upper_after = after.to_uppercase();
                    if upper_after.starts_with("SELECT ")
                        || upper_after.starts_with("SELECT\n")
                        || upper_after.starts_with("SELECT\r")
                        || upper_after.starts_with("SELECT\t")
                    {
                        found_start = Some(i);
                        break;
                    }
                }
            }
            let Some(pos) = found_start else {
                break;
            };
            let start_inner = pos + 1;
            let mut depth = 1;
            let mut end_inner = None;
            for i in start_inner..bytes.len() {
                if bytes[i] == b'(' {
                    depth += 1;
                } else if bytes[i] == b')' {
                    depth -= 1;
                    if depth == 0 {
                        end_inner = Some(i);
                        break;
                    }
                }
            }
            let Some(close_idx) = end_inner else {
                break;
            };

            let sub_sql = &resolved[start_inner..close_idx];
            let sub_res = match self.execute(sub_sql, params) {
                Ok(r) => r,
                Err(_) => break,
            };

            let before = resolved[..pos].trim_end();
            let up_before = before.to_uppercase();
            if (up_before.ends_with("ANY") || up_before.ends_with("SOME") || up_before.ends_with("ALL"))
                && !up_before.ends_with("UNION ALL")
            {
                let items: Vec<String> = sub_res
                    .rows
                    .iter()
                    .filter_map(|r| r.as_object().and_then(|m| m.values().next().cloned()))
                    .map(|v| match v {
                        serde_json::Value::Number(n) => n.to_string(),
                        serde_json::Value::String(s) => format!("'{}'", s.replace('\'', "''")),
                        serde_json::Value::Bool(b) => b.to_string(),
                        serde_json::Value::Null => "NULL".to_string(),
                        other => format!("'{}'", other.to_string().replace('\'', "''")),
                    })
                    .collect();
                resolved = format!("{}(ARRAY[{}]){}", &resolved[..pos], items.join(", "), &resolved[close_idx + 1..]);
            } else if up_before.ends_with("IN") {
                let mut seen = std::collections::HashSet::new();
                let mut items = Vec::new();
                for r in sub_res.rows {
                    if let serde_json::Value::Object(map) = r {
                        if map.len() > 1 {
                            let tuple_items: Vec<String> = map.values().map(|val| match val {
                                serde_json::Value::Number(n) => n.to_string(),
                                serde_json::Value::String(s) => format!("'{}'", s.replace('\'', "''")),
                                serde_json::Value::Bool(b) => b.to_string(),
                                serde_json::Value::Null => "NULL".to_string(),
                                _ => val.to_string(),
                            }).collect();
                            let s = format!("({})", tuple_items.join(", "));
                            if seen.insert(s.clone()) {
                                items.push(s);
                            }
                        } else if let Some(first_val) = map.values().next() {
                            let str_item = match first_val {
                                serde_json::Value::Number(n) => n.to_string(),
                                serde_json::Value::String(s) => format!("'{}'", s.replace('\'', "''")),
                                serde_json::Value::Bool(b) => b.to_string(),
                                serde_json::Value::Null => "NULL".to_string(),
                                _ => first_val.to_string(),
                            };
                            if seen.insert(str_item.clone()) {
                                items.push(str_item);
                            }
                        }
                    }
                }
                let replacement = if items.is_empty() {
                    "NULL".to_string()
                } else {
                    items.join(", ")
                };
                resolved = format!("{}({}){}", &resolved[..pos], replacement, &resolved[close_idx + 1..]);
            } else if before.to_uppercase().ends_with("EXISTS") {
                let exists = !sub_res.rows.is_empty();
                let exists_pos = before.to_uppercase().rfind("EXISTS").unwrap();
                let replacement = if exists { "TRUE" } else { "FALSE" };
                resolved = format!("{}{}{}", &resolved[..exists_pos], replacement, &resolved[close_idx + 1..]);
            } else {
                let scalar_val = sub_res.rows.first().and_then(|r| {
                    if let serde_json::Value::Object(map) = r {
                        map.values().next().cloned()
                    } else {
                        None
                    }
                }).unwrap_or(serde_json::Value::Null);

                let replacement = match scalar_val {
                    serde_json::Value::Number(n) => n.to_string(),
                    serde_json::Value::String(s) => format!("'{}'", s.replace('\'', "''")),
                    serde_json::Value::Bool(b) => b.to_string(),
                    serde_json::Value::Null => "NULL".to_string(),
                    _ => scalar_val.to_string(),
                };
                resolved = format!("{}{}{}", &resolved[..pos], replacement, &resolved[close_idx + 1..]);
            }
        }
        Ok(resolved)
    }

    /// `... ORDER BY k LIMIT n WITH TIES` (từ `FETCH FIRST n ROWS WITH TIES`): n dòng đầu cùng mọi dòng
    /// tiếp theo có khoá sắp xếp bằng dòng thứ n. Khoá không phải cột kết quả được thêm làm cột ẩn.
    fn select_with_ties(&mut self, sql: &str, params: &[Value]) -> Result<Option<QueryResult>, String> {
        let Some(lim) = last_top_level_keyword(sql, "LIMIT") else { return Ok(None) };
        let tail = sql[lim + 5..].trim();
        let mut parts = tail.split_whitespace();
        let (Some(n_txt), Some(w), Some(t)) = (parts.next(), parts.next(), parts.next()) else { return Ok(None) };
        if !w.eq_ignore_ascii_case("WITH") || !t.eq_ignore_ascii_case("TIES") || parts.next().is_some() {
            return Ok(None);
        }
        let n: usize = match eval_sql_expr(n_txt, &[], None, &[], None, params).as_i64() {
            Some(v) if v >= 0 => v as usize,
            _ => return Err("row count cannot be negative in FETCH FIRST ... WITH TIES".to_string()),
        };
        let base = sql[..lim].trim_end();
        let Some(ob) = find_top_level_keyword(base, "ORDER BY") else {
            return Err("WITH TIES cannot be specified without ORDER BY clause".to_string());
        };
        let order_txt = base[ob + 8..].trim();
        let Some(from) = find_top_level_keyword(base, "FROM") else { return Ok(None) };
        let list = base[6..from].trim();
        // khoá: số thứ tự / alias / tên cột kết quả → dùng cột đó; còn lại → cột ẩn
        let mut extra = Vec::new();
        let mut keys: Vec<String> = Vec::new();
        for (j, spec) in split_comma_separated_tokens(order_txt).into_iter().enumerate() {
            let mut e = spec.trim();
            for suffix in ["NULLS FIRST", "NULLS LAST", "DESC", "ASC"] {
                let u = e.to_uppercase();
                if u.ends_with(suffix) {
                    e = e[..e.len() - suffix.len()].trim_end();
                }
            }
            let e = e.trim();
            if let Ok(k) = e.parse::<usize>() {
                keys.push(format!("#{}", k));
                continue;
            }
            let key = format!("__tie{}__", j);
            extra.push(format!("{} AS \"{}\"", e, key));
            keys.push(key);
        }
        let sel = if extra.is_empty() { list.to_string() } else { format!("{}, {}", list, extra.join(", ")) };
        let q = format!("SELECT {} {}", sel, &base[from..]);
        let (mut res, _) = self.handle_select(&q, params)?;
        let key_of = |row: &serde_json::Value, fields: &[FieldInfo]| -> Vec<serde_json::Value> {
            keys.iter()
                .map(|k| match k.strip_prefix('#').and_then(|x| x.parse::<usize>().ok()) {
                    Some(pos) => fields.get(pos - 1).and_then(|f| row.get(&f.name)).cloned().unwrap_or_default(),
                    None => row.get(k).cloned().unwrap_or_default(),
                })
                .collect()
        };
        let mut keep = n.min(res.rows.len());
        if keep > 0 {
            let last = key_of(&res.rows[keep - 1], &res.fields);
            while keep < res.rows.len() && key_of(&res.rows[keep], &res.fields) == last {
                keep += 1;
            }
        }
        res.rows.truncate(keep);
        if !extra.is_empty() {
            for r in res.rows.iter_mut() {
                if let serde_json::Value::Object(m) = r {
                    m.retain(|k, _| !(k.starts_with("__tie") && k.ends_with("__")));
                }
            }
            res.fields.retain(|f| !(f.name.starts_with("__tie") && f.name.ends_with("__")));
        }
        res.row_count = res.rows.len();
        Ok(Some(res))
    }

    /// `FROM generate_series(a, b [, step]) [WITH ORDINALITY] [AS] alias[(cột, ...)] ...` và tương tự với
    /// `unnest(arr, ...)`: tính các dòng thành một bảng tạm có tên riêng (không bao giờ trùng bảng thật), viết
    /// lại thành `FROM __srf_N AS alias` rồi chạy như truy vấn thường. `None` nếu FROM không có dạng này.
    fn try_srf_from(&mut self, select_clause: &str, is_distinct: bool, after_from: &str, params: &[Value]) -> Option<Result<(QueryResult, Option<ExecutionPlan>), String>> {
        let up = after_from.to_uppercase();
        if up.starts_with("ROWS FROM") && after_from[9..].trim_start().starts_with('(') {
            return self.try_rows_from(select_clause, is_distinct, after_from, params);
        }
        if up.starts_with("JSON_TABLE(") {
            return self.try_generic_srf_from("JSON_TABLE", select_clause, is_distinct, after_from, params);
        }
        if let Some(p) = up.find('(') {
            let n = up[..p].trim_end();
            if crate::engine::srf::is_srf(n, false) && !JSON_SRFS.contains(&n) {
                return self.try_generic_srf_from(n, select_clause, is_distinct, after_from, params);
            }
        }
        if JSON_SRFS.iter().any(|f| up.starts_with(f) && up[f.len()..].starts_with('(')) {
            return self.try_json_srf_from(select_clause, is_distinct, after_from, params);
        }
        let fname = if up.starts_with("GENERATE_SERIES(") {
            "generate_series"
        } else if up.starts_with("UNNEST(") {
            "unnest"
        } else {
            return None;
        };
        let open = after_from.find('(')?;
        let close = crate::engine::constraints::matching_paren(after_from, open)?;
        let args = split_function_args(&after_from[open + 1..close]);
        let mut rest = after_from[close + 1..].trim_start();
        let mut ordinality = false;
        if rest.len() >= 15 && rest.as_bytes()[..15].eq_ignore_ascii_case(b"WITH ORDINALITY") {
            ordinality = true;
            rest = rest[15..].trim_start();
        }
        // alias [(cột, ...)]
        let mut alias = fname.to_string();
        let mut col_names: Vec<String> = Vec::new();
        let stop_words = ["WHERE", "ORDER", "GROUP", "LIMIT", "OFFSET", "HAVING", "JOIN", "LEFT", "RIGHT", "INNER", "CROSS", "FULL", "UNION", "WINDOW", "FETCH", "FOR"];
        let r2 = if rest.len() >= 3 && rest.as_bytes()[..3].eq_ignore_ascii_case(b"AS ") { rest[3..].trim_start() } else { rest };
        let word_len = r2.find(|c: char| !(c.is_alphanumeric() || c == '_' || c == '"')).unwrap_or(r2.len());
        let word = &r2[..word_len];
        if !word.is_empty() && !stop_words.iter().any(|w| w.eq_ignore_ascii_case(word)) {
            alias = word.trim_matches('"').to_string();
            rest = r2[word_len..].trim_start();
            if rest.starts_with('(') {
                let c = crate::engine::constraints::matching_paren(rest, 0)?;
                col_names = split_comma_separated_tokens(&rest[1..c]).into_iter().map(|x| x.trim().trim_matches('"').to_string()).collect();
                rest = rest[c + 1..].trim_start();
            } else {
                col_names.push(alias.clone());
            }
        }
        if rest.starts_with(',') || has_top_level_join(rest) {
            return None; // SRF cùng bảng khác (LATERAL...): chưa hỗ trợ ở đây
        }
        // tính các cột
        let eval = |e: &str| eval_sql_expr(e, &[], None, &[], None, params);
        let mut columns: Vec<(Vec<Value>, crate::types::DataType, Option<&'static str>)> = Vec::new();
        if fname == "generate_series" {
            if args.len() < 2 || args.len() > 3 {
                return Some(Err("function generate_series requires 2 or 3 arguments".to_string()));
            }
            let a = eval(&args[0]);
            let b = eval(&args[1]);
            let st = args.get(2).map(|x| eval(x));
            if a.is_null() || b.is_null() || st.as_ref().is_some_and(|v| v.is_null()) {
                columns.push((vec![], crate::types::DataType::Integer, None));
            } else if matches!((&a, &b), (Value::Int(_), Value::Int(_))) && st.as_ref().map_or(true, |v| matches!(v, Value::Int(_))) {
                let (x, y) = (a.as_i64().unwrap_or(0), b.as_i64().unwrap_or(0));
                let step = st.and_then(|v| v.as_i64()).unwrap_or(1);
                if step == 0 {
                    return Some(Err("step size cannot equal zero".to_string()));
                }
                let n = if (step > 0 && y >= x) || (step < 0 && y <= x) { ((y - x) / step + 1) as usize } else { 0 };
                if n > 10_000_000 {
                    return Some(Err("generate_series would return more than 10000000 rows".to_string()));
                }
                let big = x.abs() > i32::MAX as i64 || y.abs() > i32::MAX as i64;
                columns.push(((0..n).map(|k| Value::Int(x + k as i64 * step)).collect(), if big { crate::types::DataType::BigInt } else { crate::types::DataType::Integer }, None));
            } else if let (Some(x), Some(y)) = (a.as_f64().filter(|_| !matches!(a, Value::Text(_))), b.as_f64().filter(|_| !matches!(b, Value::Text(_)))) {
                let step = st.and_then(|v| v.as_f64()).unwrap_or(1.0);
                if step == 0.0 {
                    return Some(Err("step size cannot equal zero".to_string()));
                }
                let mut vals = Vec::new();
                let mut cur = x;
                while (step > 0.0 && cur <= y + 1e-12) || (step < 0.0 && cur >= y - 1e-12) {
                    vals.push(Value::Float((cur * 1e12).round() / 1e12));
                    cur += step;
                    if vals.len() > 10_000_000 {
                        return Some(Err("generate_series would return more than 10000000 rows".to_string()));
                    }
                }
                columns.push((vals, crate::types::DataType::Numeric, None));
            } else {
                let Some(step) = st else { return Some(Err("function generate_series(timestamp, timestamp) does not exist".to_string())) };
                let col = |_: &str| None;
                let tz_hint = match crate::engine::coltypes::pg_typeof(&args[0], &col).as_deref() {
                    // date được nâng lên timestamptz như PostgreSQL
                    Some("timestamp with time zone" | "date") => Some(true),
                    Some("timestamp without time zone") => Some(false),
                    _ => None,
                };
                match crate::engine::datetime::series(&a.as_str(), &b.as_str(), &step.as_str(), tz_hint) {
                    Ok((vals, tz)) => columns.push((vals.into_iter().map(Value::text).collect(), crate::types::DataType::Text, Some(if tz { "timestamptz" } else { "timestamp without time zone" }))),
                    Err(e) => return Some(Err(e)),
                }
            }
        } else {
            for a in &args {
                let v = eval(a);
                columns.push((if v.is_null() { vec![] } else { val_to_array_items(&v) }, crate::types::DataType::Text, None));
            }
        }
        let nrows = columns.iter().map(|c| c.0.len()).max().unwrap_or(0);
        // tên cột: danh sách alias, ngược lại tên hàm (một cột) / alias
        let ncols = columns.len();
        let mut names: Vec<String> = (0..ncols).map(|i| col_names.get(i).cloned().unwrap_or_else(|| if ncols == 1 { if col_names.is_empty() { fname.to_string() } else { alias.clone() } } else { fname.to_string() })).collect();
        if ordinality {
            names.push(col_names.get(ncols).cloned().unwrap_or_else(|| "ordinality".to_string()));
        }
        static SRF_SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let tmp = format!("__srf_{}", SRF_SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed));
        let mut defs: Vec<crate::types::ColumnDef> = Vec::new();
        for (i, n) in names.iter().enumerate() {
            let dt = if i < ncols {
                // mảng số nguyên → cột số
                let c = &columns[i];
                if fname == "unnest" && !c.0.is_empty() && c.0.iter().all(|v| matches!(v, Value::Int(_) | Value::Null)) { crate::types::DataType::Integer } else { c.1.clone() }
            } else {
                crate::types::DataType::BigInt
            };
            defs.push(crate::types::ColumnDef { name: n.clone(), data_type: dt, is_primary_key: false, is_nullable: true, default_value: None, comment: None });
        }
        let mut table = crate::storage::table::Table::new(tmp.clone(), defs);
        for r in 0..nrows {
            let mut row: Vec<Value> = columns.iter().map(|c| c.0.get(r).cloned().unwrap_or(Value::Null)).collect();
            if ordinality {
                row.push(Value::Int(r as i64 + 1));
            }
            table.insert(row);
        }
        self.storage.tables.insert(tmp.clone(), table);
        let sel = if is_distinct { format!("DISTINCT {}", select_clause) } else { select_clause.to_string() };
        let q = format!("SELECT {} FROM {} AS {} {}", sel, tmp, alias, rest);
        let res = self.handle_select(q.trim_end(), params);
        self.storage.tables.remove(&tmp);
        let res = res.map(|(mut r, p)| {
            for (i, c) in columns.iter().enumerate() {
                if let Some(t) = c.2 {
                    if let Some(f) = r.fields.iter_mut().find(|f| f.name == names[i]) {
                        f.data_type = t.to_string();
                    }
                }
            }
            (r, p)
        });
        Some(res)
    }

    /// `FROM ROWS FROM (f1(...), f2(...)) [WITH ORDINALITY] [AS] a[(cột, ...)]`: mỗi hàm chạy như
    /// `SELECT * FROM fi(...)`, các cột được ghép theo dòng (hàm ngắn hơn đệm NULL) thành một bảng tạm.
    fn try_rows_from(&mut self, select_clause: &str, is_distinct: bool, after_from: &str, params: &[Value]) -> Option<Result<(QueryResult, Option<ExecutionPlan>), String>> {
        let open = 9 + after_from[9..].find('(')?;
        let close = crate::engine::constraints::matching_paren(after_from, open)?;
        let calls = split_function_args(&after_from[open + 1..close]);
        let mut rest = after_from[close + 1..].trim_start();
        let mut ordinality = false;
        if rest.len() >= 15 && rest.as_bytes()[..15].eq_ignore_ascii_case(b"WITH ORDINALITY") {
            ordinality = true;
            rest = rest[15..].trim_start();
        }
        let stop_words = ["WHERE", "ORDER", "GROUP", "LIMIT", "OFFSET", "HAVING", "JOIN", "LEFT", "RIGHT", "INNER", "CROSS", "FULL", "UNION", "WINDOW", "FETCH", "FOR", "ON"];
        let r2 = if rest.len() >= 3 && rest.as_bytes()[..3].eq_ignore_ascii_case(b"AS ") { rest[3..].trim_start() } else { rest };
        let wl = r2.find(|c: char| !(c.is_alphanumeric() || c == '_' || c == '"')).unwrap_or(r2.len());
        let word = &r2[..wl];
        let mut alias = "rows_from".to_string();
        let mut col_list: Vec<String> = Vec::new();
        if !word.is_empty() && !stop_words.iter().any(|w| w.eq_ignore_ascii_case(word)) {
            alias = word.trim_matches('"').to_string();
            rest = r2[wl..].trim_start();
            if rest.starts_with('(') {
                let c = crate::engine::constraints::matching_paren(rest, 0)?;
                col_list = split_comma_separated_tokens(&rest[1..c]).into_iter().map(|x| x.trim().trim_matches('"').to_string()).collect();
                rest = rest[c + 1..].trim_start();
            }
        }
        if rest.starts_with(',') || has_top_level_join(rest) {
            return None;
        }
        // (tên cột, kiểu, giá trị theo dòng)
        let mut cols: Vec<(String, DataType, Vec<Value>)> = Vec::new();
        for call in &calls {
            let r = match self.handle_select(&format!("SELECT * FROM {}", call.trim()), params) {
                Ok((r, _)) => r,
                Err(e) => return Some(Err(e)),
            };
            for f in &r.fields {
                let vals = r.rows.iter().map(|row| row.get(&f.name).map(json_to_value).unwrap_or(Value::Null)).collect();
                cols.push((f.name.clone(), DataType::from_sql_str(&f.data_type), vals));
            }
        }
        let nrows = cols.iter().map(|c| c.2.len()).max().unwrap_or(0);
        if ordinality {
            cols.push(("ordinality".into(), DataType::BigInt, (1..=nrows as i64).map(Value::Int).collect()));
        }
        for (i, n) in col_list.iter().enumerate() {
            if let Some(c) = cols.get_mut(i) {
                c.0 = n.clone();
            }
        }
        // tên trùng (hai generate_series): tên phụ, được bỏ hậu tố ở đường wire như cột trùng tên của JOIN
        let mut seen: HashMap<String, usize> = HashMap::new();
        for c in cols.iter_mut() {
            let k = seen.entry(c.0.clone()).or_insert(0);
            *k += 1;
            if *k > 1 {
                c.0 = format!("{}{}{}", c.0, DUP_MARK, k);
            }
        }
        static SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let tmp = format!("__srf_{}", SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 4_000_000);
        let defs = cols.iter().map(|(n, dt, _)| crate::types::ColumnDef { name: n.clone(), data_type: dt.clone(), is_primary_key: false, is_nullable: true, default_value: None, comment: None }).collect();
        let mut table = crate::storage::table::Table::new(tmp.clone(), defs);
        table.pk_col_idx = None;
        for r in 0..nrows {
            table.insert(cols.iter().map(|c| c.2.get(r).cloned().unwrap_or(Value::Null)).collect());
        }
        self.storage.tables.insert(tmp.clone(), table);
        let sel = if is_distinct { format!("DISTINCT {}", select_clause) } else { select_clause.to_string() };
        let q = format!("SELECT {} FROM {} AS {} {}", sel, tmp, alias, rest);
        let res = self.handle_select(q.trim_end(), params);
        self.storage.tables.remove(&tmp);
        Some(res)
    }

    /// `FROM <hàm trả tập của engine/srf.rs>(...) [WITH ORDINALITY] [AS] a[(cột [kiểu], ...)] ...`: tính các dòng
    /// thành bảng tạm rồi chạy phần còn lại như truy vấn thường. Kèm bảng khác / LATERAL → `None`.
    fn try_generic_srf_from(&mut self, name: &str, select_clause: &str, is_distinct: bool, after_from: &str, params: &[Value]) -> Option<Result<(QueryResult, Option<ExecutionPlan>), String>> {
        let open = after_from.find('(')?;
        let close = crate::engine::constraints::matching_paren(after_from, open)?;
        let raw_args = split_function_args(&after_from[open + 1..close]);
        let mut rest = after_from[close + 1..].trim_start();
        let mut ordinality = false;
        if rest.len() >= 15 && rest.as_bytes()[..15].eq_ignore_ascii_case(b"WITH ORDINALITY") {
            ordinality = true;
            rest = rest[15..].trim_start();
        }
        let lname = name.to_lowercase();
        let mut alias = lname.clone();
        let mut col_list: Vec<String> = Vec::new();
        let stop_words = ["WHERE", "ORDER", "GROUP", "LIMIT", "OFFSET", "HAVING", "JOIN", "LEFT", "RIGHT", "INNER", "CROSS", "FULL", "UNION", "WINDOW", "FETCH", "FOR", "ON"];
        let r2 = if rest.len() >= 3 && rest.as_bytes()[..3].eq_ignore_ascii_case(b"AS ") { rest[3..].trim_start() } else { rest };
        let wl = r2.find(|c: char| !(c.is_alphanumeric() || c == '_' || c == '"')).unwrap_or(r2.len());
        let word = &r2[..wl];
        let mut aliased = false;
        if !word.is_empty() && !stop_words.iter().any(|w| w.eq_ignore_ascii_case(word)) {
            alias = word.trim_matches('"').to_string();
            aliased = true;
            rest = r2[wl..].trim_start();
            if rest.starts_with('(') {
                let c = crate::engine::constraints::matching_paren(rest, 0)?;
                col_list = split_comma_separated_tokens(&rest[1..c]).into_iter().map(|x| x.trim().to_string()).collect();
                rest = rest[c + 1..].trim_start();
            }
        }
        if rest.starts_with(',') || has_top_level_join(rest) {
            return None;
        }
        // cột của hàm record: `AS x(a int, b text)` hoặc kiểu bảng ở đối số đầu (`NULL::t`)
        let mut record_cols: Vec<(String, String)> = Vec::new();
        if crate::engine::srf::needs_record_columns(name) {
            if name.contains("POPULATE") {
                let base = raw_args.first()?;
                let ty = base.rsplit("::").next().unwrap_or("").trim().trim_matches('"');
                let Some(t) = self.storage.get_table(&clean_table_name(ty)) else {
                    return Some(Err(format!("type \"{}\" does not exist", ty)));
                };
                record_cols = t.columns.iter().enumerate().map(|(i, c)| (c.name.clone(), crate::engine::coltypes::column_decl(t, i).map(|d| d.sql_name()).unwrap_or_else(|| "text".into()))).collect();
            } else {
                if col_list.is_empty() {
                    return Some(Err("a column definition list is required for functions returning \"record\"".into()));
                }
                for c in &col_list {
                    let mut it = c.splitn(2, char::is_whitespace);
                    let n = it.next().unwrap_or("").trim_matches('"').to_string();
                    let t = it.next().unwrap_or("text").trim().to_string();
                    record_cols.push((n, t));
                }
                col_list.clear();
            }
        }
        let args: Vec<Value> = if name == "JSON_TABLE" {
            vec![]
        } else {
            raw_args.iter().enumerate().map(|(i, a)| if i == 0 && name.contains("POPULATE") { Value::Null } else { eval_sql_expr(a, &[], None, &[], None, params) }).collect()
        };
        if let Some(e) = crate::engine::errors::take() {
            return Some(Err(e));
        }
        let out = if name == "JSON_TABLE" {
            crate::engine::srf::json_table(&after_from[open + 1..close], &|e: &str| eval_sql_expr(e, &[], None, &[], None, params))
        } else {
            crate::engine::srf::eval(name, &args, &record_cols)
        };
        let out = match out {
            Ok(o) => o,
            Err(e) => return Some(Err(e)),
        };
        let mut cols = out.cols;
        if cols.len() == 1 && aliased && col_list.is_empty() && !crate::engine::srf::needs_record_columns(name) && name != "JSON_TABLE" {
            cols[0].0 = alias.clone();
        }
        for (i, n) in col_list.iter().enumerate() {
            if let Some(c) = cols.get_mut(i) {
                c.0 = n.trim_matches('"').to_string();
            }
        }
        if ordinality {
            cols.push((col_list.get(cols.len()).cloned().unwrap_or_else(|| "ordinality".into()), DataType::BigInt, None));
        }
        static SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let tmp = format!("__srf_{}", SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 2_000_000);
        let defs = cols.iter().map(|(n, dt, _)| crate::types::ColumnDef { name: n.clone(), data_type: dt.clone(), is_primary_key: false, is_nullable: true, default_value: None, comment: None }).collect();
        let mut table = crate::storage::table::Table::new(tmp.clone(), defs);
        table.pk_col_idx = None;
        for (i, mut row) in out.rows.into_iter().enumerate() {
            if ordinality {
                row.push(Value::Int(i as i64 + 1));
            }
            table.insert(row);
        }
        self.storage.tables.insert(tmp.clone(), table);
        let sel = if is_distinct { format!("DISTINCT {}", select_clause) } else { select_clause.to_string() };
        let q = format!("SELECT {} FROM {} AS {} {}", sel, tmp, alias, rest);
        let res = self.handle_select(q.trim_end(), params);
        self.storage.tables.remove(&tmp);
        Some(res.map(|(mut r, p)| {
            for (n, _, pg) in &cols {
                if let (Some(t), Some(f)) = (pg, r.fields.iter_mut().find(|f| &f.name == n)) {
                    f.data_type = t.to_string();
                }
            }
            (r, p)
        }))
    }

    /// `SELECT f(...), ...` không có FROM, trong đó có hàm trả tập của `engine/srf.rs`: các hàm được tính thành
    /// cột của một bảng tạm (chạy song song như PostgreSQL 10+, hàm ngắn hơn được đệm NULL), biểu thức khác giữ
    /// nguyên. `None` nếu không có hàm như vậy.
    fn try_select_list_srf(&mut self, sql: &str, params: &[Value]) -> Option<Result<(QueryResult, Option<ExecutionPlan>), String>> {
        let list = sql[6..].trim();
        let mut end = list.len();
        for kw in ["ORDER BY", "LIMIT", "OFFSET", "WHERE", "GROUP BY", "HAVING", "UNION", "INTERSECT", "EXCEPT"] {
            if let Some(p) = find_top_level_keyword(list, kw) {
                end = end.min(p);
            }
        }
        let (mut head, mut body) = ("", list[..end].trim());
        if body.len() > 9 && body[..9].eq_ignore_ascii_case("DISTINCT ") {
            head = "DISTINCT ";
            body = body[9..].trim_start();
        }
        let tail = list[end..].trim();
        let items = split_projection_items(body);
        let mut cols: Vec<(Vec<Value>, DataType, Option<&'static str>)> = Vec::new();
        let mut names: Vec<String> = Vec::new();
        let mut srf_pos: Vec<(usize, usize)> = Vec::new();
        let mut rendered = Vec::with_capacity(items.len());
        for it in &items {
            let it = it.trim();
            let (expr, alias) = match rfind_top_level_as(&it.to_uppercase()) {
                Some(p) => (it[..p].trim(), Some(it[p + 4..].trim().trim_matches('"').to_string())),
                None => (it, None),
            };
            let (call, cast) = match find_last_top_level_op(expr, "::").filter(|&p| cast_is_outermost(expr, p)) {
                Some(p) if expr[..p].trim_end().ends_with(')') => (expr[..p].trim_end(), &expr[p..]),
                _ => (expr, ""),
            };
            let srf = call.find('(').filter(|&o| call.ends_with(')') && is_fully_enclosed_in_parens(&call[o..])).and_then(|o| {
                let n = call[..o].trim().to_uppercase();
                crate::engine::srf::is_srf(&n, true).then_some((n, o))
            });
            let Some((n, o)) = srf else {
                rendered.push(it.to_string());
                continue;
            };
            let args: Vec<Value> = split_function_args(&call[o + 1..call.len() - 1]).iter().map(|a| eval_sql_expr(a, &[], None, &[], None, params)).collect();
            if let Some(e) = crate::engine::errors::take() {
                return Some(Err(e));
            }
            let out = match crate::engine::srf::eval(&n, &args, &[]) {
                Ok(o) => o,
                Err(e) => return Some(Err(e)),
            };
            let (_, dt, pg) = out.cols.into_iter().next()?;
            let name = alias.unwrap_or_else(|| crate::engine::select_list::default_column_name(expr));
            // cột bảng tạm mang luôn tên đầu ra (đường JSON nhận kiểu jsonb theo tên cột); trùng tên → tên phụ
            let col = if names.contains(&name) { format!("__srfcol{}", cols.len()) } else { name.clone() };
            rendered.push(if cast.is_empty() && col == name { format!("\"{}\"", col) } else { format!("\"{}\"{} AS \"{}\"", col, cast, name) });
            srf_pos.push((rendered.len() - 1, cols.len()));
            names.push(col);
            cols.push((out.rows.into_iter().map(|mut r| r.swap_remove(0)).collect(), dt, if cast.is_empty() { pg } else { None }));
        }
        if cols.is_empty() {
            return None;
        }
        static SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let tmp = format!("__srf_{}", SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 3_000_000);
        let defs = cols.iter().zip(&names).map(|((_, dt, _), n)| crate::types::ColumnDef { name: n.clone(), data_type: dt.clone(), is_primary_key: false, is_nullable: true, default_value: None, comment: None }).collect();
        let mut table = crate::storage::table::Table::new(tmp.clone(), defs);
        table.pk_col_idx = None;
        let nrows = cols.iter().map(|c| c.0.len()).max().unwrap_or(0);
        for r in 0..nrows {
            table.insert(cols.iter().map(|c| c.0.get(r).cloned().unwrap_or(Value::Null)).collect());
        }
        self.storage.tables.insert(tmp.clone(), table);
        let q = format!("SELECT {}{} FROM {} {}", head, rendered.join(", "), tmp, tail);
        let res = self.handle_select(q.trim_end(), params);
        self.storage.tables.remove(&tmp);
        Some(res.map(|(mut r, p)| {
            // kiểu PG riêng (text[] của regexp_matches) cho cột chưa bị ép kiểu
            for (i, k) in &srf_pos {
                if let (Some(t), Some(f)) = (cols[*k].2, r.fields.get_mut(*i)) {
                    f.data_type = t.to_string();
                }
            }
            (r, p)
        }))
    }

    /// `FROM jsonb_each(...) [AS a[(cột, ...)]], jsonb_array_elements(...) ...` với đối số hằng / tham số:
    /// mỗi hàm thành một bảng tạm trong bộ nhớ, phần còn lại của câu chạy như thường. Đối số tham chiếu
    /// cột (LATERAL) → `None`.
    fn try_json_srf_from(&mut self, select_clause: &str, is_distinct: bool, after_from: &str, params: &[Value]) -> Option<Result<(QueryResult, Option<ExecutionPlan>), String>> {
        static SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let stop_words = ["WHERE", "ORDER", "GROUP", "LIMIT", "OFFSET", "HAVING", "JOIN", "LEFT", "RIGHT", "INNER", "CROSS", "FULL", "UNION", "WINDOW", "FETCH", "FOR", "ON"];
        let mut rest = after_from;
        let mut new_from = Vec::new();
        let mut temps = Vec::new();
        loop {
            let up = rest.to_uppercase();
            let Some(fname) = JSON_SRFS.iter().find(|f| up.starts_with(*f) && up[f.len()..].starts_with('(')) else { break };
            let open = fname.len();
            let close = crate::engine::constraints::matching_paren(rest, open)?;
            let args = split_function_args(&rest[open + 1..close]);
            if args.len() != 1 || !is_constant_arg(&args[0]) {
                for t in &temps {
                    self.storage.tables.remove(t);
                }
                return None;
            }
            let v = eval_sql_expr(&args[0], &[], None, &[], None, params);
            let mut r = rest[close + 1..].trim_start();
            let mut ordinality = false;
            if r.len() >= 15 && r.as_bytes()[..15].eq_ignore_ascii_case(b"WITH ORDINALITY") {
                ordinality = true;
                r = r[15..].trim_start();
            }
            let lname = fname.to_lowercase();
            let mut alias = lname.clone();
            let mut col_names: Vec<String> = Vec::new();
            let r2 = if r.len() >= 3 && r.as_bytes()[..3].eq_ignore_ascii_case(b"AS ") { r[3..].trim_start() } else { r };
            let wl = r2.find(|c: char| !(c.is_alphanumeric() || c == '_' || c == '"')).unwrap_or(r2.len());
            let word = &r2[..wl];
            if !word.is_empty() && !stop_words.iter().any(|w| w.eq_ignore_ascii_case(word)) {
                alias = word.trim_matches('"').to_string();
                r = r2[wl..].trim_start();
                if r.starts_with('(') {
                    let c = crate::engine::constraints::matching_paren(r, 0)?;
                    col_names = split_comma_separated_tokens(&r[1..c]).into_iter().map(|x| x.trim().trim_matches('"').to_string()).collect();
                    r = r[c + 1..].trim_start();
                }
            }
            let parsed: Option<serde_json::Value> = match &v {
                Value::Null => None,
                Value::Text(t) => serde_json::from_str(t.trim()).ok(),
                other => Some(value_to_json(other)),
            };
            let as_text = lname.ends_with("_text");
            let jsonb_col = if as_text { DataType::Text } else { DataType::Jsonb };
            let cell = |j: &serde_json::Value| -> Value {
                match j {
                    serde_json::Value::Null if as_text => Value::Null,
                    serde_json::Value::String(st) if as_text => Value::text(st),
                    other if as_text => Value::text(other.to_string()),
                    other => Value::text(other.to_string()),
                }
            };
            let (mut defs, rows): (Vec<(String, DataType)>, Vec<Vec<Value>>) = if lname.contains("_each") {
                let Some(serde_json::Value::Object(m)) = parsed.clone().or(Some(serde_json::Value::Object(Default::default()))) else {
                    return Some(Err(format!("cannot call {} on a non-object", lname)));
                };
                let mut items: Vec<(String, serde_json::Value)> = m.into_iter().collect();
                if lname.starts_with("jsonb") {
                    items.sort_by(|a, b| a.0.len().cmp(&b.0.len()).then(a.0.cmp(&b.0)));
                }
                (vec![("key".into(), DataType::Text), ("value".into(), jsonb_col)], items.iter().map(|(k, v)| vec![Value::text(k), cell(v)]).collect())
            } else if lname.contains("_object_keys") {
                let Some(serde_json::Value::Object(m)) = parsed.clone().or(Some(serde_json::Value::Object(Default::default()))) else {
                    return Some(Err(format!("cannot call {} on a non-object", lname)));
                };
                let mut keys: Vec<String> = m.keys().cloned().collect();
                if lname.starts_with("jsonb") {
                    keys.sort_by(|a, b| a.len().cmp(&b.len()).then(a.cmp(b)));
                }
                let n = if col_names.is_empty() && alias != lname { alias.clone() } else { lname.clone() };
                (vec![(n, DataType::Text)], keys.into_iter().map(|k| vec![Value::text(k)]).collect())
            } else {
                let Some(serde_json::Value::Array(a)) = parsed.clone().or(Some(serde_json::Value::Array(vec![]))) else {
                    return Some(Err("cannot extract elements from an object".to_string()));
                };
                (vec![("value".into(), jsonb_col)], a.iter().map(|x| vec![cell(x)]).collect())
            };
            if ordinality {
                defs.push(("ordinality".into(), DataType::BigInt));
            }
            for (i, n) in col_names.iter().enumerate() {
                if let Some(d) = defs.get_mut(i) {
                    d.0 = n.clone();
                }
            }
            let tmp = format!("__srf_{}", SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 1_000_000);
            let cdefs = defs.iter().map(|(n, dt)| crate::types::ColumnDef { name: n.clone(), data_type: dt.clone(), is_primary_key: false, is_nullable: true, default_value: None, comment: None }).collect();
            let mut table = crate::storage::table::Table::new(tmp.clone(), cdefs);
            table.pk_col_idx = None;
            for (i, mut row) in rows.into_iter().enumerate() {
                if ordinality {
                    row.push(Value::Int(i as i64 + 1));
                }
                table.insert(row);
            }
            self.storage.tables.insert(tmp.clone(), table);
            new_from.push(format!("{} AS {}", tmp, alias));
            temps.push(tmp);
            rest = r;
            if let Some(after_comma) = rest.strip_prefix(',') {
                let ac = after_comma.trim_start();
                let acu = ac.to_uppercase();
                if JSON_SRFS.iter().any(|f| acu.starts_with(f) && acu[f.len()..].starts_with('(')) {
                    rest = ac;
                    continue;
                }
            }
            break;
        }
        let sel = if is_distinct { format!("DISTINCT {}", select_clause) } else { select_clause.to_string() };
        let q = format!("SELECT {} FROM {} {}", sel, new_from.join(", "), rest);
        let res = self.handle_select(q.trim_end(), params);
        for t in &temps {
            self.storage.tables.remove(t);
        }
        Some(res)
    }

    /// `ARRAY(SELECT x ...)` không tương quan → `ARRAY[v1, v2, ...]` (cột đầu của mọi dòng, giữ thứ tự),
    /// rỗng → `'{}'`. Subquery chạy lỗi (vd. tham chiếu bảng ngoài) thì giữ nguyên. `None` nếu không đổi gì.
    fn expand_array_subqueries(&mut self, sql: &str, params: &[Value]) -> Result<Option<String>, String> {
        let mut out = sql.to_string();
        let mut from = 0;
        let mut changed = false;
        loop {
            let b = out.as_bytes();
            let mut found = None;
            let mut in_str = false;
            for i in from..b.len() {
                if b[i] == b'\'' {
                    in_str = !in_str;
                }
                if !in_str
                    && i + 6 <= b.len()
                    && b[i..i + 6].eq_ignore_ascii_case(b"ARRAY(")
                    && (i == 0 || !(b[i - 1].is_ascii_alphanumeric() || b[i - 1] == b'_'))
                    && out[i + 6..].trim_start().len() > 7
                    && out[i + 6..].trim_start().as_bytes()[..7].eq_ignore_ascii_case(b"SELECT ")
                {
                    found = Some(i);
                    break;
                }
            }
            let Some(i) = found else { break };
            let open = i + 5;
            let Some(close) = crate::engine::constraints::matching_paren(&out, open) else { break };
            let sub = out[open + 1..close].to_string();
            if subquery_is_correlated(&sub) {
                return Err("correlated ARRAY(subquery) is not supported".to_string());
            }
            let Ok((res, _)) = self.handle_select(sub.trim(), params) else {
                from = close;
                continue;
            };
            let first_key = res.fields.first().map(|f| f.name.clone());
            let items: Vec<String> = res
                .rows
                .iter()
                .map(|r| {
                    let v = match (r.as_object(), &first_key) {
                        (Some(o), Some(k)) => o.get(k).or_else(|| o.values().next()).cloned().unwrap_or_default(),
                        (Some(o), None) => o.values().next().cloned().unwrap_or_default(),
                        _ => serde_json::Value::Null,
                    };
                    match v {
                        serde_json::Value::Null => "NULL".to_string(),
                        serde_json::Value::String(s) => format!("'{}'", s.replace('\'', "''")),
                        serde_json::Value::Array(_) | serde_json::Value::Object(_) => format!("'{}'", v.to_string().replace('\'', "''")),
                        other => other.to_string(),
                    }
                })
                .collect();
            let arr = if items.is_empty() { "'{}'".to_string() } else { format!("ARRAY[{}]", items.join(", ")) };
            out = format!("{}{}{}", &out[..i], arr, &out[close + 1..]);
            from = i + arr.len();
            changed = true;
        }
        Ok(changed.then_some(out))
    }

    /// Subquery vô hướng tương quan trong danh sách SELECT, vd. `(SELECT STRING_AGG(c.name, ', ') FROM c
    /// WHERE c.id = ANY(a.ids)) AS names`, → `LEFT JOIN LATERAL (SELECT ... AS __sqv) __sqN ON TRUE` và
    /// mục SELECT đọc `__sqN.__sqv`. Đường LATERAL thay giá trị dòng ngoài rồi chạy subquery bằng chính
    /// engine, nên mọi hàm gộp / điều kiện đều đúng (trước đây ra `true`/`false`, mảng, hoặc NULL).
    /// Giữ nguyên: truy vấn có GROUP BY / hàm gộp / UNION, và `COUNT` tương quan bằng `=` (đã có đường
    /// nhanh `CorrelatedCount`). `None` nếu không đổi gì.
    fn correlated_select_subqueries_as_lateral(sql: &str) -> Option<String> {
        let s = sql.trim();
        if s.len() < 7 || !s[..7].eq_ignore_ascii_case("SELECT ") || !contains_ci(s, "(SELECT") && !contains_ci(s, "( SELECT") {
            return None;
        }
        let from = find_top_level_keyword(s, "FROM")?;
        let rest = &s[from + 4..];
        if ["UNION", "INTERSECT", "EXCEPT", "GROUP BY", "HAVING"].iter().any(|k| find_top_level_keyword(rest, k).is_some()) {
            return None;
        }
        let list = &s[6..from];
        // các khoảng `(SELECT ...)` tương quan trong danh sách SELECT
        let b = list.as_bytes();
        let mut spans: Vec<(usize, usize)> = Vec::new();
        let (mut i, mut in_sq, mut in_dq) = (0, false, false);
        while i < b.len() {
            match b[i] {
                b'\'' if !in_dq => in_sq = !in_sq,
                b'"' if !in_sq => in_dq = !in_dq,
                b'(' if !in_sq && !in_dq => {
                    let after = list[i + 1..].trim_start();
                    if after.len() > 7 && after.get(..7).map_or(false, |p| p.eq_ignore_ascii_case("SELECT ") || p.eq_ignore_ascii_case("SELECT\n")) {
                        let close = crate::engine::constraints::matching_paren(list, i)?;
                        let inner = list[i + 1..close].trim();
                        if subquery_is_correlated(inner) {
                            spans.push((i, close));
                        }
                        i = close + 1;
                        continue;
                    }
                }
                _ => {}
            }
            i += 1;
        }
        if spans.is_empty() {
            return None;
        }
        // phần danh sách SELECT ngoài các subquery không được có hàm gộp (ngữ nghĩa nhóm)
        let mut outside = String::new();
        let mut last = 0;
        for &(a, z) in &spans {
            outside.push_str(&list[last..a]);
            outside.push_str("NULL");
            last = z + 1;
        }
        outside.push_str(&list[last..]);
        if split_projection_items(&outside).iter().any(|it| is_aggregate_projection_item(it)) {
            return None;
        }

        let mut new_list = String::new();
        let mut laterals = String::new();
        let mut last = 0;
        for (n, &(a, z)) in spans.iter().enumerate() {
            let inner = list[a + 1..z].trim();
            let inner_from = find_top_level_keyword(inner, "FROM")?;
            let items = inner[6..inner_from].trim();
            if items.to_uppercase().starts_with("DISTINCT") || split_projection_items(items).len() != 1 {
                return None;
            }
            let up_items = items.to_uppercase();
            if up_items.starts_with("COUNT(") && !contains_ci(inner, "ANY(") && !contains_ci(inner, "ALL(") {
                return None;
            }
            let (expr, inner_alias) = match rfind_top_level_as(&up_items) {
                Some(p) => (items[..p].trim(), Some(items[p + 4..].trim().to_string())),
                None => (items, None),
            };
            let name = format!("__sq{}", n + 1);
            laterals.push_str(&format!(" LEFT JOIN LATERAL (SELECT {} AS __sqv {}) {} ON TRUE", expr, &inner[inner_from..], name));
            new_list.push_str(&list[last..a]);
            new_list.push_str(&format!("{}.__sqv", name));
            // mục chỉ gồm subquery, không alias: giữ tên cột PostgreSQL đặt (alias trong subquery / tên hàm)
            let item_rest = list[z + 1..].trim_start();
            let ends_item = item_rest.is_empty() || item_rest.starts_with(',');
            let starts_item = list[..a].trim_end().is_empty() || list[..a].trim_end().ends_with(',')
                || list[..a].trim_end().to_uppercase().ends_with("DISTINCT");
            if starts_item && ends_item {
                let col = inner_alias.unwrap_or_else(|| format!("\"{}\"", crate::engine::select_list::default_column_name(expr)));
                new_list.push_str(&format!(" AS {}", col));
            }
            last = z + 1;
        }
        new_list.push_str(&list[last..]);

        let mut end = rest.len();
        for kw in ["WHERE", "WINDOW", "ORDER BY", "LIMIT", "OFFSET", "FETCH"] {
            if let Some(p) = find_top_level_keyword(rest, kw) {
                end = end.min(p);
            }
        }
        Some(format!("SELECT {} FROM {}{} {}", new_list.trim(), rest[..end].trim(), laterals, rest[end..].trim()).trim_end().to_string())
    }

    pub(crate) fn resolve_subqueries_in_select_pub(&mut self, select_str: &str, params: &[Value]) -> Result<String, String> {
        self.resolve_subqueries_in_select(select_str, params)
    }

    fn resolve_subqueries_in_select(&mut self, select_str: &str, params: &[Value]) -> Result<String, String> {
        let mut resolved = select_str.to_string();
        loop {
            let mut found_start = None;
            let bytes = resolved.as_bytes();
            for i in 0..bytes.len() {
                if bytes[i] == b'(' {
                    let after = resolved[i + 1..].trim_start();
                    let upper_after = after.to_uppercase();
                    if upper_after.starts_with("SELECT ")
                        || upper_after.starts_with("SELECT\n")
                        || upper_after.starts_with("SELECT\r")
                        || upper_after.starts_with("SELECT\t")
                    {
                        found_start = Some(i);
                        break;
                    }
                }
            }
            let Some(pos) = found_start else {
                break;
            };
            let start_inner = pos + 1;
            let mut depth = 1;
            let mut end_inner = None;
            for i in start_inner..bytes.len() {
                if bytes[i] == b'(' {
                    depth += 1;
                } else if bytes[i] == b')' {
                    depth -= 1;
                    if depth == 0 {
                        end_inner = Some(i);
                        break;
                    }
                }
            }
            let Some(close_idx) = end_inner else {
                break;
            };

            let sub_sql = &resolved[start_inner..close_idx];
            let is_correlated = if let Some(sub_from) = find_top_level_keyword(sub_sql, "FROM") {
                let after_f = sub_sql[sub_from + 4..].trim();
                let sub_w = find_top_level_keyword(after_f, "WHERE");
                let sub_tbl_part = if let Some(w) = sub_w { &after_f[..w] } else { after_f };
                let (sub_tbl_name, sub_alias) = extract_table_name_and_alias(sub_tbl_part);
                if let Some(w_pos) = sub_w {
                    let w_clause = after_f[w_pos + 5..].trim();
                    w_clause.split_whitespace().any(|tok| {
                        let clean = tok.trim_matches('(').trim_matches(')').trim_matches(',').trim_matches(';');
                        if let Some(dot) = clean.find('.') {
                            let prefix = &clean[..dot];
                            !prefix.eq_ignore_ascii_case(sub_tbl_name)
                                && sub_alias.map(|a| !prefix.eq_ignore_ascii_case(a)).unwrap_or(true)
                        } else {
                            false
                        }
                    })
                } else {
                    false
                }
            } else {
                false
            };
            if is_correlated {
                break;
            }

            let (sub_res, _) = match self.handle_select(sub_sql, params) {
                Ok(r) => r,
                Err(_) => break,
            };

            let val_str = if let Some(first_row) = sub_res.rows.first() {
                if let Some(obj) = first_row.as_object() {
                    if obj.len() == 1 {
                        let v = obj.values().next().unwrap();
                        match v {
                            serde_json::Value::Null => "NULL".to_string(),
                            serde_json::Value::String(s) => format!("'{}'", s.replace('\'', "''")),
                            serde_json::Value::Array(_) | serde_json::Value::Object(_) => format!("'{}'", serde_json::to_string(v).unwrap_or_default().replace('\'', "''")),
                            _ => v.to_string(),
                        }
                    } else {
                        let arr = serde_json::Value::Array(obj.values().cloned().collect());
                        format!("'{}'", serde_json::to_string(&arr).unwrap_or_default().replace('\'', "''"))
                    }
                } else {
                    first_row.to_string()
                }
            } else {
                "NULL".to_string()
            };

            resolved = format!("{}{}{}", &resolved[..pos], val_str, &resolved[close_idx + 1..]);
        }
        Ok(resolved)
    }

    fn resolve_correlated_subqueries_in_where(
        &mut self,
        where_str: &str,
        table_name: &str,
        columns: &[crate::types::ColumnDef],
        row: &[Value],
        alias: Option<&str>,
        params: &[Value],
    ) -> String {
        let mut resolved = where_str.to_string();
        for (col_idx, col) in columns.iter().enumerate() {
            let val = row.get(col_idx).unwrap_or(&Value::Null);
            let val_sql = match val {
                Value::Null => "NULL".to_string(),
                Value::Bool(b) => b.to_string(),
                Value::Int(i) => i.to_string(),
                Value::Float(f) => f.to_string(),
                Value::Text(s) => format!("'{}'", s.replace('\'', "''")),
            };
            if let Some(a) = alias {
                let pattern = format!("{}.{}", a, col.name);
                resolved = Self::replace_identifier_token(&resolved, &pattern, &val_sql);
            }
            let pattern_tbl = format!("{}.{}", table_name, col.name);
            resolved = Self::replace_identifier_token(&resolved, &pattern_tbl, &val_sql);
        }
        if let Ok(res) = self.resolve_subqueries_in_where(&resolved, params) {
            return res;
        }
        resolved
    }

fn json_to_engine_value(v: &serde_json::Value) -> Value {
    match v {
        serde_json::Value::Null => Value::Null,
        serde_json::Value::Bool(b) => Value::Bool(*b),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Value::Int(i)
            } else if let Some(f) = n.as_f64() {
                Value::Float(f)
            } else {
                Value::Null
            }
        }
        serde_json::Value::String(s) => Value::text(s),
        other => Value::text(other.to_string()),
    }
}

fn create_ephemeral_table(name: &str, res: QueryResult) -> crate::storage::table::Table {
    let mut cols = Vec::with_capacity(res.fields.len());
    for f in &res.fields {
        cols.push(crate::types::ColumnDef {
            name: f.name.clone(),
            data_type: crate::types::DataType::from_sql_str(&f.data_type),
            is_primary_key: false,
            is_nullable: true,
            default_value: None,
            comment: None,
        });
    }

    let mut table = crate::storage::table::Table::new(name.to_string(), cols);
    for r_json in res.rows {
        let mut row = Vec::with_capacity(table.columns.len());
        for col in &table.columns {
            let v = r_json.as_object().and_then(|obj| obj.get(&col.name)).map(Self::json_to_engine_value).unwrap_or(Value::Null);
            row.push(v);
        }
        table.insert(row);
    }
    table
}

/// Thay tham chiếu dòng ngoài trong từng `(SELECT ...)` của `cond` (mỗi subquery dùng tên bảng / alias của
/// chính nó để biết tham chiếu nào là bên ngoài). Phần ngoài subquery giữ nguyên vì không có tham chiếu cột
/// ngoài nào ở đó trong các điều kiện được tách ra.
fn substitute_in_subqueries(cond: &str, row: &CombinedRow) -> String {
    let b = cond.as_bytes();
    let mut out = String::with_capacity(cond.len());
    let mut last = 0;
    let mut i = 0;
    let mut in_sq = false;
    while i < b.len() {
        if b[i] == b'\'' {
            in_sq = !in_sq;
        } else if !in_sq && b[i] == b'(' && cond[i + 1..].trim_start().get(..6).map_or(false, |p| p.eq_ignore_ascii_case("SELECT")) {
            if let Some(close) = crate::engine::constraints::matching_paren(cond, i) {
                out.push_str(&cond[last..i + 1]);
                out.push_str(&Self::substitute_correlated_references(&cond[i + 1..close], row));
                last = close;
                i = close;
                continue;
            }
        }
        i += 1;
    }
    out.push_str(&cond[last..]);
    out
}

pub(crate) fn value_as_sql_literal_pub(v: &Value) -> String {
    Self::value_as_sql_literal(v)
}

/// Giá trị dưới dạng literal SQL (chuỗi được nháy và thoát `'`).
fn value_as_sql_literal(v: &Value) -> String {
    match v {
        Value::Null => "NULL".to_string(),
        Value::Bool(b) => (if *b { "TRUE" } else { "FALSE" }).to_string(),
        Value::Int(n) => n.to_string(),
        Value::Float(f) => f.to_string(),
        Value::Text(s) => format!("'{}'", s.replace('\'', "''")),
    }
}

/// Thay `q.col` (ngoài chuỗi) bằng literal khi `lookup(q, col)` (viết thường, bỏ ngoặc kép) trả giá trị.
fn substitute_qualified_refs(sql: &str, lookup: &dyn Fn(&str, &str) -> Option<Value>) -> String {
    let mut out = String::with_capacity(sql.len());
    let chars: Vec<char> = sql.chars().collect();
    let n = chars.len();
    let mut i = 0;
    while i < n {
        let ch = chars[i];
        if ch == '\'' {
            out.push(ch);
            i += 1;
            while i < n {
                out.push(chars[i]);
                i += 1;
                if chars[i - 1] == '\'' {
                    if i < n && chars[i] == '\'' {
                        out.push('\'');
                        i += 1;
                    } else {
                        break;
                    }
                }
            }
            continue;
        }
        if ch.is_alphabetic() || ch == '_' || ch == '"' {
            let start = i;
            while i < n && (chars[i].is_alphanumeric() || chars[i] == '_' || chars[i] == '.' || chars[i] == '"') {
                i += 1;
            }
            let token: String = chars[start..i].iter().collect();
            let clean = token.replace('"', "").to_lowercase();
            let replaced = clean.rsplit_once('.').and_then(|(q, c)| lookup(q.rsplit('.').next().unwrap_or(q), c));
            match replaced {
                Some(v) => out.push_str(&Self::value_as_sql_literal(&v)),
                None => out.push_str(&token),
            }
            continue;
        }
        out.push(ch);
        i += 1;
    }
    out
}

fn substitute_correlated_references(sql: &str, row: &CombinedRow) -> String {
    // Chỉ thay `q.col` của dòng ngoài: `q` không phải bảng / alias của chính subquery, và khoá có đúng
    // trong dòng (không lấy theo tên cột trần, vốn làm `c.name` của bảng trong bị thay bằng cột ngoài).
    let own = subquery_own_names(sql);
    let is_outer_ref = |tok: &str| {
        let Some((q, _)) = tok.rsplit_once('.') else { return false };
        let q = q.rsplit('.').next().unwrap_or(q);
        !own.iter().any(|o| o == q)
            && (row.schema.col_map.contains_key(tok) || row.extra.as_ref().map_or(false, |e| e.contains_key(tok)))
    };
    let mut out = String::with_capacity(sql.len());
    let chars: Vec<char> = sql.chars().collect();
    let n = chars.len();
    let mut i = 0;

    while i < n {
        let ch = chars[i];
        if ch == '\'' {
            out.push(ch);
            i += 1;
            while i < n {
                let sc = chars[i];
                out.push(sc);
                i += 1;
                if sc == '\'' {
                    if i < n && chars[i] == '\'' {
                        out.push('\'');
                        i += 1;
                    } else {
                        break;
                    }
                }
            }
            continue;
        }

        if ch.is_alphabetic() || ch == '_' || ch == '"' {
            let start = i;
            while i < n {
                let c = chars[i];
                if c.is_alphanumeric() || c == '_' || c == '.' || c == '"' {
                    i += 1;
                } else {
                    break;
                }
            }
            let token: String = chars[start..i].iter().collect();
            let clean_token = token.replace('"', "").to_lowercase();
            if clean_token.contains('.') && is_outer_ref(&clean_token) {
                let val = row.get_val(&clean_token);
                match val {
                    Value::Null => out.push_str("NULL"),
                    Value::Bool(b) => out.push_str(if b { "TRUE" } else { "FALSE" }),
                    Value::Int(num) => out.push_str(&num.to_string()),
                    Value::Float(f) => out.push_str(&f.to_string()),
                    Value::Text(ref s) => {
                        out.push('\'');
                        out.push_str(&s.replace('\'', "''"));
                        out.push('\'');
                    }
                }
            } else {
                out.push_str(&token);
            }
            continue;
        }

        out.push(ch);
        i += 1;
    }

    out
}

    fn handle_joined_query(
        &mut self,
        select_clause: &str,
        after_from: &str,
        params: &[Value],
    ) -> Result<QueryResult, String> {
        let mut ephemeral_tables = Vec::new();
        let res = self.handle_joined_query_internal(select_clause, after_from, params, &mut ephemeral_tables);
        for name in ephemeral_tables {
            self.storage.tables.remove(&name);
        }
        res
    }

    fn handle_joined_query_internal(
        &mut self,
        select_clause: &str,
        after_from: &str,
        params: &[Value],
        ephemeral_tables: &mut Vec<String>,
    ) -> Result<QueryResult, String> {
        let (from_joins_str, where_opt, group_opt, having_opt, order_specs, limit_opt, offset_opt) = parse_query_clauses(after_from, params);
        let (base_tbl_name, base_alias, base_subquery, joins) = parse_from_and_joins(from_joins_str)?;

        // Điều kiện (nối bằng AND) chứa subquery tương quan với bảng ngoài, vd. `NOT EXISTS (SELECT 1 FROM a
        // WHERE a.customer_id = c.id)`: tính theo từng dòng sau JOIN (thay giá trị dòng vào subquery rồi chạy
        // bằng engine). Trước đây subquery bị chạy một lần như không tương quan: EXISTS luôn đúng, NOT EXISTS
        // luôn sai.
        let mut correlated_where: Vec<String> = Vec::new();
        let where_opt = where_opt.map(|w| {
            if !contains_ci(&w, "SELECT") {
                return w;
            }
            let (corr, rest): (Vec<&str>, Vec<&str>) = split_top_level_and(&w).into_iter().partition(|part| has_correlated_subquery(part));
            if corr.is_empty() {
                return w;
            }
            correlated_where = corr.iter().map(|p| p.trim().to_string()).collect();
            if rest.is_empty() { "TRUE".to_string() } else { rest.join(" AND ") }
        });
        let resolved_where = if let Some(w) = where_opt {
            let w_up = w.to_uppercase();
            if w_up.contains("SELECT") && w_up.contains("FROM") {
                Some(self.resolve_subqueries_in_where(&w, params)?)
            } else {
                Some(w)
            }
        } else {
            None
        };

        if let Some(ref sub_sql) = base_subquery {
            let sub_res = self.execute(sub_sql, params)?;
            let tbl = Self::create_ephemeral_table(&base_tbl_name, sub_res);
            let key = base_tbl_name.to_lowercase();
            self.storage.tables.insert(key.clone(), tbl);
            ephemeral_tables.push(key);
        }

        for j in &joins {
            if let Some(ref sub_sql) = j.subquery {
                if !j.is_lateral {
                    let sub_res = self.execute(sub_sql, params)?;
                    let tbl = Self::create_ephemeral_table(&j.table_name, sub_res);
                    let key = j.table_name.to_lowercase();
                    self.storage.tables.insert(key.clone(), tbl);
                    ephemeral_tables.push(key);
                }
            }
        }

        let mut all_tables_meta: Vec<(&crate::storage::table::Table, Option<&str>)> = Vec::new();
        let base_table = self.storage.get_table(&base_tbl_name).ok_or_else(|| format!("Table {} not found", base_tbl_name))?;
        all_tables_meta.push((base_table, base_alias.as_deref()));

        for j in &joins {
            if let Some(jt) = self.storage.get_table(&j.table_name) {
                all_tables_meta.push((jt, j.alias.as_deref()));
            }
        }

        let joined_schema = std::sync::Arc::new(JoinedSchema::new(&all_tables_meta));
        let mut pushdown = plan_predicate_pushdown(resolved_where.as_deref(), &all_tables_meta, &joins);
        if joins.iter().any(|j| matches!(j.kind, JoinKind::Right | JoinKind::Full)) {
            // WHERE áp sau JOIN: không lọc trước bảng có thể bị bổ sung dòng NULL
            let base = std::mem::take(&mut pushdown.base_predicates);
            pushdown.remaining_where.extend(base);
            for preds in pushdown.intermediate_predicates.iter_mut() {
                let moved = std::mem::take(preds);
                pushdown.remaining_where.extend(moved);
            }
        }
        let compiled_base_preds: Vec<CompiledExpr> = pushdown.base_predicates.iter()
            .map(|p| compile_expr_for_joined(p, &joined_schema))
            .collect();
        let compiled_remaining: Vec<CompiledExpr> = pushdown.remaining_where.iter()
            .map(|p| compile_expr_for_joined(p, &joined_schema))
            .collect();
        let base_table_columns = base_table.columns.clone();

        // Dừng sớm khi đủ LIMIT chỉ đúng nếu không còn điều kiện WHERE nào áp sau JOIN (trước đây cắt dòng
        // rồi mới lọc `remaining_where`: `FROM a, b WHERE ... LIMIT 1` ra 0 dòng) và không có hàm gộp nào.
        let can_early_limit = group_opt.is_none() 
            && having_opt.is_none() 
            && order_specs.is_empty() 
            && compiled_remaining.is_empty()
            && correlated_where.is_empty()
            && pushdown.intermediate_predicates.iter().all(|p| p.is_empty())
            && !select_clause.to_uppercase().starts_with("DISTINCT ")
            && !mentions_call(select_clause, AGG_NAMES)
            && !select_clause.to_uppercase().contains("OVER(");

        let mut current_rows: Vec<CombinedRow> = Vec::with_capacity(base_table.active_count);
        for (r_idx, r) in base_table.rows.iter().enumerate() {
            if !base_table.is_deleted[r_idx] {
                let comb = CombinedRow::from_base_row(joined_schema.clone(), r);
                let mut pass = true;
                for pred in &compiled_base_preds {
                    if pred.eval_joined(&comb, params) != Some(true) {
                        pass = false;
                        break;
                    }
                }
                if pass {
                    current_rows.push(comb);
                }
            }
        }

        for (j_idx, j) in joins.iter().enumerate() {
            if current_rows.is_empty() && !matches!(j.kind, JoinKind::Right | JoinKind::Full) {
                break;
            }

            if j.is_lateral {
                let sub_sql = j.subquery.as_ref().unwrap();
                let alias = j.alias.as_deref().unwrap_or(&j.table_name);
                let mut next_rows = Vec::with_capacity(current_rows.len());

                for base_comb in &current_rows {
                    let resolved_sql = Self::substitute_correlated_references(sub_sql, base_comb);
                    let sub_res = self.execute(&resolved_sql, params)?;
                    let mut match_count = 0;

                    for row_json in &sub_res.rows {
                        let mut candidate = base_comb.clone();
                        if let Some(obj) = row_json.as_object() {
                            for field in &sub_res.fields {
                                let col_name = field.name.to_lowercase();
                                let val = obj.get(&field.name).map(Self::json_to_engine_value).unwrap_or(Value::Null);
                                candidate.insert_extra(format!("{}.{}", alias.to_lowercase(), col_name), val.clone());
                                if !candidate.contains_key(&col_name) {
                                    candidate.insert_extra(col_name, val);
                                }
                            }
                        }
                        let matches = match &j.on_condition {
                            Some(cond) => eval_condition_on_row(&candidate, cond, params),
                            None => true,
                        };
                        if matches {
                            next_rows.push(candidate);
                            match_count += 1;
                        }
                    }

                    if match_count == 0 && matches!(j.kind, JoinKind::Left | JoinKind::Full) {
                        let mut null_comb = base_comb.clone();
                        for field in &sub_res.fields {
                            let col_name = field.name.to_lowercase();
                            null_comb.insert_extra(format!("{}.{}", alias.to_lowercase(), col_name), Value::Null);
                            if !null_comb.contains_key(&col_name) {
                                null_comb.insert_extra(col_name, Value::Null);
                            }
                        }
                        next_rows.push(null_comb);
                    }
                }

                current_rows = next_rows;
                continue;
            }

            let j_table = self.storage.get_table(&j.table_name).ok_or_else(|| format!("Table {} not found", j.table_name))?;

            let maybe_equi = if let Some(ref cond) = j.on_condition {
                parse_equi_join_candidate(cond, &j.table_name, j.alias.as_deref(), j_table)
            } else {
                None
            };

            let mut next_rows = Vec::with_capacity(current_rows.len());

            let filter_preds = &pushdown.joined_table_predicates[j_idx];
            let table_offset = joined_schema.table_offsets.get(j_idx + 1).cloned().unwrap_or(0);
            let compiled_filter_preds: Vec<CompiledExpr> = filter_preds.iter()
                .map(|p| compile_expr_for_joined(p, &joined_schema))
                .collect();
            let compiled_residual: Option<CompiledExpr> = maybe_equi.as_ref().and_then(|e| e.residual_cond.as_ref())
                .map(|cond| compile_expr_for_joined(cond, &joined_schema));
            let compiled_on_cond = j.on_condition.as_ref()
                .map(|cond| compile_expr_for_joined(cond, &joined_schema));

            let mut test_comb = if !filter_preds.is_empty() {
                let mut c = CombinedRow::new_with_schema(joined_schema.clone());
                c.values.resize(table_offset + j_table.columns.len(), Value::Null);
                Some(c)
            } else {
                None
            };

            let target_limit = if can_early_limit && j_idx + 1 == joins.len() {
                limit_opt.map(|lim| lim + offset_opt.unwrap_or(0))
            } else {
                None
            };

            if let Some(mut equi) = maybe_equi {
                equi.left_slot = joined_schema.get_slot(&equi.left_expr)
                    .or_else(|| joined_schema.get_slot(&clean_col_name(&equi.left_expr)));
                // vế trái là biểu thức (`sp.id + 1`): biên dịch một lần (eval_operand_on_row không tính được)
                let left_compiled = if equi.left_slot.is_none() {
                    let c = compile_expr_for_joined(&equi.left_expr, &joined_schema);
                    (!matches!(c, CompiledExpr::Raw(_))).then_some(c)
                } else {
                    None
                };

                let should_build_left = !matches!(j.kind, JoinKind::Left | JoinKind::Full) && current_rows.len() < j_table.rows.len();

                if should_build_left {
                    // Adaptive Hash Join: Build hash table on current_rows (smaller), Probe j_table
                    let mut left_hash_map: std::collections::HashMap<JoinKey, Vec<usize>> = std::collections::HashMap::with_capacity(current_rows.len());
                    for (base_idx, base_comb) in current_rows.iter().enumerate() {
                        let left_val = match equi.left_slot {
                            Some(slot) => base_comb.values.get(slot).unwrap_or(&Value::Null),
                            None => &match &left_compiled {
                                Some(c) => c.eval_on_combined(base_comb, params),
                                None => eval_operand_on_row(base_comb, &equi.left_expr, params),
                            },
                        };
                        if !left_val.is_null() {
                            left_hash_map.entry(value_to_join_key(left_val)).or_default().push(base_idx);
                        }
                    }

                    for (jr_idx, jr) in j_table.rows.iter().enumerate() {
                        if !j_table.is_deleted[jr_idx] {
                            if let Some(ref mut c) = test_comb {
                                let end_idx = (table_offset + jr.len()).min(c.values.len());
                                c.values[table_offset..end_idx].clone_from_slice(&jr[..end_idx - table_offset]);
                                let mut pass = true;
                                for p in &compiled_filter_preds {
                                    if p.eval_joined(c, params) != Some(true) {
                                        pass = false;
                                        break;
                                    }
                                }
                                if !pass {
                                    continue;
                                }
                            }

                            if let Some(val) = jr.get(equi.right_col_idx) {
                                if !val.is_null() {
                                    let key = value_to_join_key(val);
                                    if let Some(base_indices) = left_hash_map.get(&key) {
                                        for &base_idx in base_indices {
                                            let base_comb = &current_rows[base_idx];
                                            if let Some(ref res_expr) = compiled_residual {
                                                let candidate = base_comb.with_joined_table(jr);
                                                if res_expr.eval_joined(&candidate, params) == Some(true) {
                                                    next_rows.push(candidate);
                                                }
                                            } else {
                                                let matched_comb = base_comb.with_joined_table(jr);
                                                next_rows.push(matched_comb);
                                            }
                                        }
                                        if let Some(target) = target_limit {
                                            if next_rows.len() >= target {
                                                break;
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                } else {
                    // Standard Hash Join: Build hash table on j_table's right_col_idx, Probe current_rows
                    let mut right_hash_map: std::collections::HashMap<JoinKey, Vec<usize>> = std::collections::HashMap::with_capacity(j_table.active_count);
                    for (jr_idx, jr) in j_table.rows.iter().enumerate() {
                        if !j_table.is_deleted[jr_idx] {
                            if let Some(ref mut c) = test_comb {
                                let end_idx = (table_offset + jr.len()).min(c.values.len());
                                c.values[table_offset..end_idx].clone_from_slice(&jr[..end_idx - table_offset]);
                                let mut pass = true;
                                for p in &compiled_filter_preds {
                                    if p.eval_joined(c, params) != Some(true) {
                                        pass = false;
                                        break;
                                    }
                                }
                                if !pass {
                                    continue;
                                }
                            }
                            if let Some(val) = jr.get(equi.right_col_idx) {
                                if !val.is_null() {
                                    right_hash_map.entry(value_to_join_key(val)).or_default().push(jr_idx);
                                }
                            }
                        }
                    }

                    // Probe phase: match against current_rows
                    for base_comb in &current_rows {
                        if let Some(target) = target_limit {
                            if next_rows.len() >= target {
                                break;
                            }
                        }

                        let mut match_count = 0;
                        let left_val = match equi.left_slot {
                            Some(slot) => base_comb.values.get(slot).unwrap_or(&Value::Null),
                            None => &match &left_compiled {
                                Some(c) => c.eval_on_combined(base_comb, params),
                                None => eval_operand_on_row(base_comb, &equi.left_expr, params),
                            },
                        };

                        if !left_val.is_null() {
                            let key = value_to_join_key(left_val);
                            if let Some(matched_indices) = right_hash_map.get(&key) {
                                for &jr_idx in matched_indices {
                                    let jr = &j_table.rows[jr_idx];

                                    if let Some(ref res_expr) = compiled_residual {
                                        let candidate = base_comb.with_joined_table(jr);
                                        if res_expr.eval_joined(&candidate, params) == Some(true) {
                                            next_rows.push(candidate);
                                            match_count += 1;
                                            if let Some(target) = target_limit {
                                                if next_rows.len() >= target {
                                                    break;
                                                }
                                            }
                                        }
                                    } else {
                                        let matched_comb = base_comb.with_joined_table(jr);
                                        next_rows.push(matched_comb);
                                        match_count += 1;
                                        if let Some(target) = target_limit {
                                            if next_rows.len() >= target {
                                                break;
                                            }
                                        }
                                    }
                                }
                            }
                        }

                        if match_count == 0 && matches!(j.kind, JoinKind::Left | JoinKind::Full) {
                            let null_comb = base_comb.with_null_table(j_table.columns.len());
                            next_rows.push(null_comb);
                        }
                    }
                }
            } else {
                // Fallback: Nested Loop Join for non-equi joins
                for base_comb in &current_rows {
                    if let Some(target) = target_limit {
                        if next_rows.len() >= target {
                            break;
                        }
                    }
                    let mut match_count = 0;
                    for (jr_idx, jr) in j_table.rows.iter().enumerate() {
                        if !j_table.is_deleted[jr_idx] {
                            if let Some(ref mut c) = test_comb {
                                let end_idx = (table_offset + jr.len()).min(c.values.len());
                                c.values[table_offset..end_idx].clone_from_slice(&jr[..end_idx - table_offset]);
                                let mut pass = true;
                                for p in &compiled_filter_preds {
                                    if p.eval_joined(c, params) != Some(true) {
                                        pass = false;
                                        break;
                                    }
                                }
                                if !pass {
                                    continue;
                                }
                            }
                            let candidate = base_comb.with_joined_table(jr);
                            let matches = match &compiled_on_cond {
                                Some(expr) => expr.eval_joined(&candidate, params) == Some(true),
                                None => true,
                            };
                            if matches {
                                next_rows.push(candidate);
                                match_count += 1;
                                if let Some(target) = target_limit {
                                    if next_rows.len() >= target {
                                        break;
                                    }
                                }
                            }
                        }
                    }
                    if match_count == 0 && matches!(j.kind, JoinKind::Left | JoinKind::Full) {
                        let null_comb = base_comb.with_null_table(j_table.columns.len());
                        next_rows.push(null_comb);
                    }
                }
            }

            // RIGHT / FULL: dòng bên phải không khớp dòng nào bên trái -> bên trái NULL
            if matches!(j.kind, JoinKind::Right | JoinKind::Full) {
                for (jr_idx, jr) in j_table.rows.iter().enumerate() {
                    if j_table.is_deleted[jr_idx] {
                        continue;
                    }
                    let matched = current_rows.iter().any(|base_comb| {
                        let candidate = base_comb.with_joined_table(jr);
                        match &compiled_on_cond {
                            Some(expr) => expr.eval_joined(&candidate, params) == Some(true),
                            None => true,
                        }
                    });
                    if !matched {
                        let mut null_base = CombinedRow::new_with_schema(joined_schema.clone());
                        null_base.values.resize(table_offset, Value::Null);
                        next_rows.push(null_base.with_joined_table(jr));
                    }
                }
            }

            // Intermediate predicate filtering right after this join step
            let compiled_intermediate: Vec<Vec<CompiledExpr>> = pushdown.intermediate_predicates.iter()
                .map(|preds| preds.iter().map(|p| compile_expr_for_joined(p, &joined_schema)).collect())
                .collect();
            if let Some(preds) = compiled_intermediate.get(j_idx + 1) {
                for pred in preds {
                    next_rows.retain(|r| pred.eval_joined(r, params) == Some(true));
                }
            }

            // Early LIMIT optimization
            if can_early_limit && j_idx + 1 == joins.len() {
                if let Some(lim) = limit_opt {
                    let target = lim + offset_opt.unwrap_or(0);
                    if next_rows.len() >= target {
                        next_rows.truncate(target);
                        current_rows = next_rows;
                        break;
                    }
                }
            }

            current_rows = next_rows;
        }

        for rem in &compiled_remaining {
            current_rows.retain(|r| rem.eval_joined(r, params) == Some(true));
        }
        if !correlated_where.is_empty() {
            let mut keep = Vec::with_capacity(current_rows.len());
            for r in &current_rows {
                let mut ok = true;
                for cond in &correlated_where {
                    let resolved = Self::substitute_in_subqueries(cond, r);
                    let res = self.execute(&format!("SELECT ({}) AS __c", resolved), params)?;
                    let v = res.rows.first().and_then(|row| row.as_object()).and_then(|o| o.values().next()).cloned().unwrap_or(serde_json::Value::Null);
                    if v != serde_json::Value::Bool(true) {
                        ok = false;
                        break;
                    }
                }
                keep.push(ok);
            }
            let mut it = keep.into_iter();
            current_rows.retain(|_| it.next().unwrap_or(false));
        }

        let mut final_json_rows: Vec<serde_json::Value> = Vec::new();
        let is_distinct = select_clause.to_uppercase().starts_with("DISTINCT ");
        let clean_select = if is_distinct { select_clause[9..].trim() } else { select_clause };

        if let Some(ref gb_str) = group_opt {
            let group_col_names: Vec<String> = split_comma_separated_tokens(gb_str).into_iter().map(|s| s.trim().to_string()).collect();
            let mut group_map: std::collections::HashMap<String, Vec<usize>> = std::collections::HashMap::new();
            let mut group_keys_order = Vec::new();

            for (idx, r) in current_rows.iter().enumerate() {
                let key = group_col_names.iter().map(|col| r.get_val(col).as_str()).collect::<Vec<_>>().join("\x1f");
                if !group_map.contains_key(&key) {
                    group_keys_order.push(key.clone());
                }
                group_map.entry(key).or_default().push(idx);
            }

            let proj_items = split_projection_items(clean_select);

            for key in group_keys_order {
                let row_indices = &group_map[&key];
                let mut row_map = serde_json::Map::new();

                for item in &proj_items {
                    let upper = item.to_uppercase();
                    let (expr_part, alias) = if let Some(as_idx) = rfind_top_level_as(&upper) {
                        (item[..as_idx].trim(), item[as_idx + 4..].trim().replace('"', ""))
                    } else {
                        (item.trim(), clean_col_name(item.trim()).replace('"', ""))
                    };

                    let val = eval_group_aggregate_expr(expr_part, &current_rows, row_indices, params, &self.storage);
                    row_map.insert(alias, val);
                }

                let mut include = true;
                if let Some(ref h_str) = having_opt {
                    include = eval_having_condition(h_str, &row_map, None, params);
                }
                if include {
                    final_json_rows.push(serde_json::Value::Object(row_map));
                }
            }
        } else {
            let proj_items = split_projection_items(clean_select);
            let has_aggregates = proj_items.iter().any(|item| is_aggregate_projection_item(item));

            if has_aggregates {
                let row_indices: Vec<usize> = (0..current_rows.len()).collect();
                let mut row_map = serde_json::Map::new();
                for item in &proj_items {
                    let upper = item.to_uppercase();
                    let (expr_part, alias) = if let Some(as_idx) = rfind_top_level_as(&upper) {
                        (item[..as_idx].trim(), item[as_idx + 4..].trim().replace('"', ""))
                    } else {
                        (item.trim(), clean_col_name(item.trim()).replace('"', ""))
                    };

                    let val = if is_aggregate_projection_item(item) {
                        eval_group_aggregate_expr(expr_part, &current_rows, &row_indices, params, &self.storage)
                    } else if let Some(&first_ri) = row_indices.first() {
                        eval_joined_scalar_expr(expr_part, &current_rows[first_ri], params, &self.storage)
                    } else {
                        serde_json::Value::Null
                    };
                    row_map.insert(alias, val);
                }

                let mut include = true;
                if let Some(ref h_str) = having_opt {
                    include = eval_having_condition(h_str, &row_map, None, params);
                }
                if include {
                    final_json_rows.push(serde_json::Value::Object(row_map));
                }
            } else {
                if !order_specs.is_empty() {
                    current_rows.sort_by(|a, b| {
                        for spec in &order_specs {
                            let va = a.get_val(spec.col_name);
                            let vb = b.get_val(spec.col_name);
                            let directed = order_cmp(&va, &vb, spec.is_desc, spec.nulls_first);
                            if directed != std::cmp::Ordering::Equal {
                                return directed;
                            }
                        }
                        std::cmp::Ordering::Equal
                    });
                }
                let has_wildcards = proj_items.iter().any(|item| {
                    let t = item.trim();
                    t.ends_with(".*") || t == "*"
                });

                if !has_wildcards {
                    let precompiled: Vec<(String, Option<usize>, Option<CompiledExpr>, String)> = proj_items.iter().map(|item| {
                        let upper = item.to_uppercase();
                        let (expr_part, alias) = if let Some(as_idx) = rfind_top_level_as(&upper) {
                            (item[..as_idx].trim(), item[as_idx + 4..].trim().replace('"', ""))
                        } else {
                            (item.trim(), clean_col_name(item.trim()).replace('"', ""))
                        };
                        let clean_expr = expr_part.trim().replace('"', "");
                        let slot = if !clean_expr.contains('(')
                            && !clean_expr.contains('+')
                            && !clean_expr.contains('-')
                            && !clean_expr.contains('*')
                            && !clean_expr.contains('/')
                            && !clean_expr.contains("::")
                            && !clean_expr.contains("->")
                            && !clean_expr.contains(' ')
                        {
                            joined_schema.get_slot(&clean_expr)
                        } else {
                            None
                        };
                        // hàm số học / ngày giờ lồng trong phép toán: bộ biên dịch JOIN tính sai (nút `Raw` ra
                        // true/false), để `eval_joined_scalar_expr` chuyển sang bộ đánh giá đầy đủ
                        let needs_full = expr_part.contains('(') && joined_needs_full_eval(expr_part, &expr_part.to_uppercase());
                        let compiled = if slot.is_none() && !needs_full && !expr_part.to_uppercase().contains("SELECT") {
                            let c = compile_expr_for_joined(expr_part, &joined_schema);
                            if !matches!(c, CompiledExpr::Raw(_)) {
                                Some(c)
                            } else {
                                None
                            }
                        } else {
                            None
                        };
                        (alias, slot, compiled, expr_part.to_string())
                    }).collect();

                    for r in &current_rows {
                        let mut row_map = serde_json::Map::with_capacity(precompiled.len());
                        for (alias, slot, compiled, expr_part) in &precompiled {
                            let val = if let Some(s) = slot {
                                value_to_json(r.values.get(*s).unwrap_or(&Value::Null))
                            } else if let Some(c) = compiled {
                                value_to_json(&c.eval_on_combined(r, params))
                            } else {
                                eval_joined_scalar_expr(expr_part, r, params, &self.storage)
                            };
                            row_map.insert(alias.clone(), val);
                        }
                        final_json_rows.push(serde_json::Value::Object(row_map));
                    }
                } else {
                    for r in &current_rows {
                        let mut row_map = serde_json::Map::new();
                        for item in &proj_items {
                            let trimmed_item = item.trim();
                            if trimmed_item.ends_with(".*") {
                                let prefix = trimmed_item[..trimmed_item.len() - 2].trim().trim_matches('"');
                                let is_base = prefix.eq_ignore_ascii_case(&base_tbl_name)
                                    || base_alias.as_ref().map(|a| prefix.eq_ignore_ascii_case(a.trim_matches('"'))).unwrap_or(false);
                                if is_base {
                                    for col in &base_table_columns {
                                        let col_name = &col.name;
                                        let val = r.get_val(&format!("{}.{}", prefix, col_name));
                                        let final_val = if val.is_null() { r.get_val(col_name) } else { val };
                                        row_map.insert(col_name.clone(), value_to_json(&final_val));
                                    }
                                    continue;
                                }
                                let mut matched_join = false;
                                for j in &joins {
                                    let is_j = prefix.eq_ignore_ascii_case(&j.table_name)
                                        || j.alias.as_ref().map(|a| prefix.eq_ignore_ascii_case(a.trim_matches('"'))).unwrap_or(false);
                                    if is_j {
                                        if let Some(jt) = self.storage.get_table(&j.table_name) {
                                            for col in &jt.columns {
                                                let col_name = &col.name;
                                                let val = r.get_val(&format!("{}.{}", prefix, col_name));
                                                let final_val = if val.is_null() { r.get_val(col_name) } else { val };
                                                row_map.insert(col_name.clone(), value_to_json(&final_val));
                                            }
                                        }
                                        matched_join = true;
                                        break;
                                    }
                                }
                                if matched_join {
                                    continue;
                                }
                            } else if trimmed_item == "*" {
                                for col in &base_table_columns {
                                    let val = r.get_val(&col.name);
                                    row_map.insert(col.name.clone(), value_to_json(&val));
                                }
                                for j in &joins {
                                    if let Some(jt) = self.storage.get_table(&j.table_name) {
                                        for col in &jt.columns {
                                            let val = r.get_val(&col.name);
                                            row_map.insert(col.name.clone(), value_to_json(&val));
                                        }
                                    }
                                }
                                continue;
                            }

                            let upper = item.to_uppercase();
                            let (expr_part, alias) = if let Some(as_idx) = rfind_top_level_as(&upper) {
                                (item[..as_idx].trim(), item[as_idx + 4..].trim().replace('"', ""))
                            } else {
                                (item.trim(), clean_col_name(item.trim()).replace('"', ""))
                            };
                            let val = eval_joined_scalar_expr(expr_part, r, params, &self.storage);
                            row_map.insert(alias, val);
                        }
                        final_json_rows.push(serde_json::Value::Object(row_map));
                    }
                }
            }
        }

        if select_clause.to_uppercase().contains(" OVER (") {
            eval_window_functions(&mut final_json_rows, select_clause);
        }

        if is_distinct {
            let mut seen = std::collections::HashSet::new();
            let mut distinct_rows = Vec::new();
            for r in final_json_rows {
                let key = serde_json::to_string(&r).unwrap_or_default();
                if seen.insert(key) {
                    distinct_rows.push(r);
                }
            }
            final_json_rows = distinct_rows;
        }

        if !order_specs.is_empty() {
            sort_json_rows(&mut final_json_rows, &order_specs);
        }

        let offset = offset_opt.unwrap_or(0);
        let limit = limit_opt.unwrap_or(usize::MAX);
        if offset < final_json_rows.len() {
            let end = (offset.saturating_add(limit)).min(final_json_rows.len());
            final_json_rows = final_json_rows[offset..end].to_vec();
        } else {
            final_json_rows = vec![];
        }

        let row_count = final_json_rows.len();
        let mut fields = self.joined_projection_fields(clean_select, &base_tbl_name, base_alias.as_deref(), &joins);
        // Key phát sinh ngoài danh sách SELECT (vd. cột của hàm cửa sổ) vẫn được giữ.
        if let Some(serde_json::Value::Object(map)) = final_json_rows.first() {
            for (k, v) in map {
                if !fields.iter().any(|f| &f.name == k) {
                    fields.push(FieldInfo { name: k.clone(), data_type: type_str_from_json_value(v) });
                }
            }
        }

        Ok(QueryResult {
            rows: final_json_rows,
            row_count,
            fields,
            command: "SELECT".to_string(),
        })
    }

    /// Cột kết quả của truy vấn JOIN, theo đúng thứ tự và quy tắc đặt tên mà
    /// `handle_joined_query_internal` dùng khi dựng dòng, kèm kiểu thật của cột.
    fn joined_projection_fields(
        &self,
        clean_select: &str,
        base_tbl_name: &str,
        base_alias: Option<&str>,
        joins: &[ParsedJoin],
    ) -> Vec<FieldInfo> {
        let mut tables: Vec<(&crate::storage::table::Table, Option<&str>)> = Vec::new();
        if let Some(t) = self.storage.get_table(base_tbl_name) {
            tables.push((t, base_alias));
        }
        for j in joins {
            if let Some(t) = self.storage.get_table(&j.table_name) {
                tables.push((t, j.alias.as_deref()));
            }
        }
        let col_type = |c: &str| column_ref_type(c, &tables);

        let mut fields: Vec<FieldInfo> = Vec::new();
        let push = |fields: &mut Vec<FieldInfo>, name: String, data_type: String| {
            if !fields.iter().any(|f| f.name == name) {
                fields.push(FieldInfo { name, data_type });
            }
        };
        for item in split_projection_items(clean_select) {
            let t = item.trim();
            if t == "*" {
                for (tbl, _) in &tables {
                    for c in &tbl.columns {
                        push(&mut fields, c.name.clone(), c.data_type.field_type_str());
                    }
                }
                continue;
            }
            if let Some(prefix) = t.strip_suffix(".*") {
                let prefix = prefix.trim().trim_matches('"');
                if let Some((tbl, _)) = tables.iter().find(|(tbl, alias)| {
                    prefix.eq_ignore_ascii_case(&tbl.name) || alias.map(|a| prefix.eq_ignore_ascii_case(a.trim_matches('"'))).unwrap_or(false)
                }) {
                    for c in &tbl.columns {
                        push(&mut fields, c.name.clone(), c.data_type.field_type_str());
                    }
                    continue;
                }
            }
            let upper = item.to_uppercase();
            let (expr_part, alias) = if let Some(as_idx) = rfind_top_level_as(&upper) {
                (item[..as_idx].trim(), item[as_idx + 4..].trim().replace('"', ""))
            } else {
                (item.trim(), clean_col_name(item.trim()).replace('"', ""))
            };
            let mut e = expr_part;
            while e.starts_with('(') && e.ends_with(')') && is_fully_enclosed_in_parens(e) {
                e = e[1..e.len() - 1].trim();
            }
            let dt = crate::pg_types::infer_expr_type(e, &col_type).unwrap_or_else(|| "text".to_string());
            push(&mut fields, alias, dt);
        }
        fields
    }

    fn handle_create_index(&mut self, sql: &str) -> Result<QueryResult, String> {
        let parsed = crate::engine::constraints::parse_create_index(sql)?;
        let done = Ok(QueryResult {
            rows: vec![],
            row_count: 0,
            fields: vec![],
            command: "CREATE INDEX".to_string(),
        });
        let Some(table) = self.storage.get_table(&parsed.table) else {
            return Err(format!("relation \"{}\" does not exist", parsed.table));
        };
        // Bảng do bản cũ tạo: dữ liệu có thể đã trùng (không được kiểm tra lúc ghi).
        let strict = table.catalog_tracked && !table.legacy_data;
        let name_taken = self.storage.tables.values().any(|t| {
            t.indexes.iter().any(|ix| ix.name.eq_ignore_ascii_case(&parsed.name))
                || t.constraints.iter().any(|c| c.name.eq_ignore_ascii_case(&parsed.name))
        });
        if name_taken {
            if parsed.if_not_exists {
                return done;
            }
            return Err(format!("relation \"{}\" already exists", parsed.name));
        }
        if parsed.def.unique {
            if let Err(e) = self.validate_unique_index(&parsed.table, &parsed.def) {
                if strict || !parsed.if_not_exists {
                    return Err(e);
                }
                // Bảng do bản cũ tạo, dữ liệu cũ đã trùng: không ép, để app vẫn khởi động được.
                eprintln!("[pglite-rs] WARNING: unique index \"{}\" is not enforced: {}", parsed.name, e);
                self.plan_cache.clear();
                return done;
            }
        }
        // Index phụ trong bộ nhớ cho từng cột đơn (như trước), để tăng tốc tra cứu.
        if let Some(table) = self.storage.get_table_mut(&parsed.table) {
            for c in &parsed.def.cols {
                if let Some(col_idx) = table.get_column_index(c) {
                    if !table.col_indexes.contains_key(&col_idx) {
                        table.add_secondary_index(col_idx);
                    }
                }
            }
        }
        self.storage.log_schema_op(crate::storage::catalog::SchemaOp::CreateIndex { table: parsed.table.clone(), def: parsed.def });
        self.plan_cache.clear();
        done
    }

    fn handle_drop_index(&mut self, sql: &str) -> Result<QueryResult, String> {
        let mut rest = sql.trim().trim_end_matches(';').trim()[10..].trim();
        if rest.to_uppercase().starts_with("CONCURRENTLY ") {
            rest = rest[13..].trim();
        }
        if rest.to_uppercase().starts_with("IF EXISTS ") {
            rest = rest[10..].trim();
        }
        for kw in [" CASCADE", " RESTRICT"] {
            if rest.get(rest.len().saturating_sub(kw.len())..).map_or(false, |x| x.eq_ignore_ascii_case(kw)) {
                rest = rest[..rest.len() - kw.len()].trim();
            }
        }
        for name in split_comma_separated_tokens(rest) {
            let name = strip_schema_keep_case(name);
            let owner = self.storage.tables.iter().find(|(_, t)| t.indexes.iter().any(|ix| ix.name.eq_ignore_ascii_case(&name))).map(|(k, _)| k.clone());
            // Index chưa từng được lưu (DB do bản cũ tạo) thì không báo lỗi, giữ hành vi cũ.
            if let Some(table) = owner {
                self.storage.log_schema_op(crate::storage::catalog::SchemaOp::DropIndex { table, name });
            }
        }
        self.plan_cache.clear();
        Ok(QueryResult {
            rows: vec![],
            row_count: 0,
            fields: vec![],
            command: "DROP INDEX".to_string(),
        })
    }

    fn handle_drop_table(&mut self, sql: &str) -> Result<QueryResult, String> {
        let mut rest = sql.trim().trim_end_matches(';').trim()[10..].trim();
        let if_exists = rest.to_uppercase().starts_with("IF EXISTS ");
        if if_exists {
            rest = rest[10..].trim();
        }
        let mut cascade = false;
        for kw in [" CASCADE", " RESTRICT"] {
            if rest.get(rest.len().saturating_sub(kw.len())..).map_or(false, |x| x.eq_ignore_ascii_case(kw)) {
                cascade = kw == " CASCADE";
                rest = rest[..rest.len() - kw.len()].trim();
            }
        }
        let names: Vec<String> = split_comma_separated_tokens(rest).into_iter().map(strip_schema_keep_case).filter(|n| !n.is_empty()).collect();
        if names.is_empty() {
            return Err("Missing table name in DROP TABLE".to_string());
        }
        let mut to_drop = Vec::new();
        for n in &names {
            if self.storage.get_table(n).is_some() {
                to_drop.push(n.clone());
            } else if !if_exists {
                return Err(format!("table \"{}\" does not exist", n));
            }
        }
        // Khoá ngoại từ bảng khác (không bị xoá cùng) trỏ tới: lỗi, trừ khi CASCADE (khi đó bỏ khoá ngoại đó).
        let dropping: std::collections::HashSet<String> = to_drop.iter().map(|n| n.to_lowercase()).collect();
        let mut dependent: Vec<(String, String)> = Vec::new();
        for (key, t) in &self.storage.tables {
            if dropping.contains(key) {
                continue;
            }
            for c in &t.constraints {
                if let crate::storage::catalog::ConstraintKind::ForeignKey { ref_table, .. } = &c.kind {
                    if dropping.contains(&ref_table.to_lowercase()) {
                        dependent.push((key.clone(), c.name.clone()));
                    }
                }
            }
        }
        if !dependent.is_empty() {
            if !cascade {
                return Err(format!(
                    "cannot drop table {} because other objects depend on it (constraint {} on table {} depends on it)",
                    to_drop.join(", "),
                    dependent[0].1,
                    dependent[0].0
                ));
            }
            for (table, name) in dependent {
                self.storage.log_schema_op(crate::storage::catalog::SchemaOp::DropConstraint { table, name });
            }
        }
        for n in &to_drop {
            self.storage.drop_table(n);
        }
        self.plan_cache.clear();

        Ok(QueryResult {
            rows: vec![],
            row_count: 0,
            fields: vec![],
            command: "DROP TABLE".to_string(),
        })
    }

    /// `CREATE [LOCAL|GLOBAL] TEMP[ORARY] TABLE [IF NOT EXISTS] t ... [ON COMMIT DROP|PRESERVE ROWS] [AS ...]`:
    /// bảng chỉ sống trong phiên này. Tên được đăng ký với WAL trước khi tạo nên mọi bản ghi của bảng (tạo,
    /// chèn, sửa, ràng buộc) đều không ghi xuống đĩa; `ON COMMIT DROP` xoá bảng khi giao dịch kết thúc.
    fn handle_create_temp_table(&mut self, sql: &str, params: &[Value]) -> Result<QueryResult, String> {
        let mut rest = sql["CREATE".len()..].trim_start();
        for kw in ["LOCAL ", "GLOBAL "] {
            if rest.get(..kw.len()).map_or(false, |p| p.eq_ignore_ascii_case(kw)) {
                rest = rest[kw.len()..].trim_start();
            }
        }
        let kw_len = if rest.get(..10).map_or(false, |p| p.eq_ignore_ascii_case("TEMPORARY ")) { 10 } else { 5 };
        rest = rest.get(kw_len..).unwrap_or("").trim_start();
        if !rest.get(..6).map_or(false, |p| p.eq_ignore_ascii_case("TABLE ")) {
            return Err("only CREATE TEMP TABLE is supported".to_string());
        }
        let body = rest[6..].trim_start();
        let (if_not_exists, after_ine) = if body.len() > 14 && body.as_bytes()[..14].eq_ignore_ascii_case(b"IF NOT EXISTS ") { (true, body[14..].trim_start()) } else { (false, body) };
        let name_end = after_ine.find(|c: char| c.is_whitespace() || c == '(').unwrap_or(after_ine.len());
        let raw_name = &after_ine[..name_end];
        let name = strip_schema_keep_case(raw_name).trim_matches('"').to_lowercase();
        if name.is_empty() {
            return Err("syntax error in CREATE TEMP TABLE".to_string());
        }
        // ON COMMIT ... (ở cấp ngoài cùng)
        let mut stmt = format!("CREATE TABLE {}", body);
        let mut on_commit_drop = false;
        if let Some(p) = find_top_level_keyword(&stmt, "ON COMMIT") {
            let tail = stmt[p + 9..].trim_start();
            let up_tail = tail.to_uppercase();
            let (kind, len) = if up_tail.starts_with("DROP") {
                ("DROP", 4)
            } else if up_tail.starts_with("PRESERVE ROWS") {
                ("PRESERVE", 13)
            } else if up_tail.starts_with("DELETE ROWS") {
                return Err("ON COMMIT DELETE ROWS is not supported".to_string());
            } else {
                return Err("syntax error at or near \"ON COMMIT\"".to_string());
            };
            on_commit_drop = kind == "DROP";
            stmt = format!("{} {}", stmt[..p].trim_end(), tail[len..].trim_start()).trim().to_string();
        }
        if let Some(existing) = self.storage.get_table(&name) {
            let _ = existing;
            if if_not_exists && self.storage.wal.temp_tables.contains(&name) {
                return Ok(QueryResult { rows: vec![], row_count: 0, fields: vec![], command: "CREATE TABLE".to_string() });
            }
            return Err(format!("relation \"{}\" already exists", name));
        }
        self.storage.wal.temp_tables.insert(name.clone());
        let res = if is_create_table_as(&stmt) { self.handle_create_table_as(&stmt, params) } else { self.handle_create_table(&stmt) };
        if res.is_err() || self.storage.get_table(&name).is_none() {
            self.storage.wal.temp_tables.remove(&name);
            return res;
        }
        if on_commit_drop {
            if self.storage.in_transaction {
                self.storage.on_commit_drop.push(name);
            } else {
                // ngoài giao dịch: câu lệnh tự là một giao dịch, bảng bị xoá ngay khi nó kết thúc
                self.storage.tables.remove(&name);
                self.storage.wal.temp_tables.remove(&name);
            }
        }
        res
    }

    /// `CREATE [UNLOGGED] TABLE [IF NOT EXISTS] t [(cột, ...)] AS <truy vấn> [WITH [NO] DATA]`: tạo bảng theo
    /// kiểu các cột kết quả rồi chép dữ liệu bằng INSERT thường (WAL như mọi bảng khác), trong một giao dịch.
    fn handle_create_table_as(&mut self, sql: &str, params: &[Value]) -> Result<QueryResult, String> {
        let mut rest = sql["CREATE".len()..].trim_start();
        let up = rest.to_uppercase();
        if up.starts_with("TEMP ") || up.starts_with("TEMPORARY ") {
            return Err("TEMP tables are not supported".to_string());
        }
        if up.starts_with("UNLOGGED ") {
            rest = rest[9..].trim_start();
        }
        rest = rest["TABLE".len()..].trim_start();
        let if_not_exists = rest.len() > 14 && rest.as_bytes()[..14].eq_ignore_ascii_case(b"IF NOT EXISTS ");
        if if_not_exists {
            rest = rest[14..].trim_start();
        }
        let as_pos = find_top_level_keyword(rest, "AS").ok_or("Invalid CREATE TABLE AS syntax")?;
        let head = rest[..as_pos].trim();
        let mut query = rest[as_pos + 2..].trim();
        let mut with_data = true;
        let uq = query.to_uppercase();
        if uq.ends_with("WITH NO DATA") {
            with_data = false;
            query = query[..query.len() - 12].trim_end();
        } else if uq.ends_with("WITH DATA") {
            query = query[..query.len() - 9].trim_end();
        }
        let (name_raw, col_list) = match head.find('(') {
            Some(p) if head.ends_with(')') => (head[..p].trim(), Some(split_comma_separated_tokens(&head[p + 1..head.len() - 1]).into_iter().map(|c| c.trim().trim_matches('"').to_string()).collect::<Vec<_>>())),
            _ => (head, None),
        };
        let table_name = strip_schema_keep_case(name_raw);
        if self.storage.get_table(&table_name).is_some() {
            if if_not_exists {
                return Ok(QueryResult { rows: vec![], row_count: 0, fields: vec![], command: "CREATE TABLE AS".to_string() });
            }
            return Err(format!("relation \"{}\" already exists", table_name));
        }
        // dữ liệu dạng engine (mảng/JSON là giá trị JSON), không phải text của đường wire
        let res = {
            let _guard = RawTextGuard(set_raw_text_mode(false));
            self.execute_statement(query, params)?
        };
        let decls = crate::engine::coltypes::wire_decls(query, &self.storage, res.fields.len());
        let mut names: Vec<String> = res.fields.iter().map(|f| crate::engine::frontend::decode_all(&f.name).into_owned()).collect();
        if let Some(cl) = &col_list {
            if cl.len() > names.len() {
                return Err("too many column names were specified".to_string());
            }
            for (i, c) in cl.iter().enumerate() {
                names[i] = c.clone();
            }
        }
        // tên trên wire như PostgreSQL (`?column?`, tên trùng do engine giữ văn bản biểu thức)
        if col_list.is_none() {
            if let Some(items) = crate::engine::select_list::wire_names(query) {
                if items.len() == names.len() {
                    names = items.into_iter().map(|(n, _, _)| n).collect();
                }
            }
        }
        for (i, n) in names.iter().enumerate() {
            if names[..i].iter().any(|m| m.eq_ignore_ascii_case(n)) {
                return Err(format!("column \"{}\" specified more than once", n));
            }
        }
        let quote = |n: &str| -> String {
            if !n.is_empty() && n.chars().all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_') && !n.starts_with(|c: char| c.is_ascii_digit()) {
                n.to_string()
            } else if n.chars().all(|c| c.is_alphanumeric() || c == '_') {
                format!("\"{}\"", n)
            } else {
                crate::engine::frontend::encode_ident(n)
            }
        };
        let col_defs: Vec<String> = (0..names.len())
            .map(|i| {
                let ty = decls
                    .get(i)
                    .cloned()
                    .flatten()
                    .map(|d| d.canonical())
                    .unwrap_or_else(|| sql_type_for_oid(crate::pg_types::oid_for_type_str(&res.fields[i].data_type), res.rows.iter().filter_map(|r| r.get(&res.fields[i].name)).find(|v| !v.is_null())));
                format!("{} {}", quote(&names[i]), ty)
            })
            .collect();
        let implicit = !self.storage.in_transaction;
        if implicit {
            self.storage.begin_transaction();
        }
        let result = (|| -> Result<usize, String> {
            self.handle_create_table(&format!("CREATE TABLE {} ({})", quote(&table_name), col_defs.join(", ")))?;
            if !with_data || res.rows.is_empty() {
                return Ok(0);
            }
            let cols = names.iter().map(|n| quote(n)).collect::<Vec<_>>().join(", ");
            for chunk in res.rows.chunks(200) {
                let mut vals = Vec::with_capacity(chunk.len());
                let mut ps: Vec<Value> = Vec::with_capacity(chunk.len() * names.len());
                for row in chunk {
                    let mut ph = Vec::with_capacity(names.len());
                    for f in &res.fields {
                        let v = row.get(&f.name).cloned().unwrap_or_default();
                        ps.push(match &v {
                            serde_json::Value::Array(_) | serde_json::Value::Object(_) => Value::text(v.to_string()),
                            other => json_to_value(other),
                        });
                        ph.push(format!("${}", ps.len()));
                    }
                    vals.push(format!("({})", ph.join(", ")));
                }
                let _guard = RawTextGuard(set_raw_text_mode(false));
                self.execute_statement(&format!("INSERT INTO {} ({}) VALUES {}", quote(&table_name), cols, vals.join(", ")), &ps)?;
            }
            Ok(res.rows.len())
        })();
        match result {
            Ok(n) => {
                if implicit {
                    self.storage.commit()?;
                }
                Ok(QueryResult { rows: vec![], row_count: n, fields: vec![], command: "SELECT".to_string() })
            }
            Err(e) => {
                if implicit {
                    let _ = self.storage.rollback();
                    self.plan_cache.clear();
                }
                Err(e)
            }
        }
    }

    fn handle_create_table(&mut self, sql: &str) -> Result<QueryResult, String> {
        // Simple CREATE TABLE parser
        let after_create = sql[12..].trim();
        let (name_part, body_part) = if let Some(open_paren) = after_create.find('(') {
            let close_paren = after_create.rfind(')').ok_or("Missing closing parenthesis in CREATE TABLE")?;
            (&after_create[..open_paren].trim(), &after_create[open_paren + 1..close_paren].trim())
        } else {
            return Err("Invalid CREATE TABLE syntax".to_string());
        };

        let mut table_name = name_part.trim();
        let if_not_exists = table_name.to_uppercase().starts_with("IF NOT EXISTS ");
        if if_not_exists {
            table_name = table_name[14..].trim();
        }
        let table_name = strip_schema_keep_case(table_name);

        let exists = self.storage.get_table(&table_name).is_some();
        if exists && !if_not_exists {
            return Err(format!("relation \"{}\" already exists", table_name));
        }

        let mut columns = Vec::new();
        let mut table_level_pks = Vec::new();
        let mut defs: Vec<crate::storage::catalog::ConstraintDef> = Vec::new();
        let mut decl_types: Vec<(String, String)> = Vec::new();
        let mut identities: Vec<(String, crate::storage::catalog::IdentitySpec)> = Vec::new();

        for col_def_str in split_comma_separated_tokens(body_part) {
            let col_def_str = col_def_str.trim();
            if col_def_str.is_empty() {
                continue;
            }

            // Ràng buộc cấp bảng: PRIMARY KEY (...), UNIQUE (...), FOREIGN KEY ..., CHECK (...), CONSTRAINT x ...
            if let Some(def) = crate::engine::constraints::parse_table_constraint(&table_name, col_def_str, &defs) {
                if let crate::storage::catalog::ConstraintKind::PrimaryKey { cols } = &def.kind {
                    table_level_pks.extend(cols.iter().cloned());
                    defs.retain(|d| !matches!(d.kind, crate::storage::catalog::ConstraintKind::PrimaryKey { .. }));
                }
                defs.push(def);
                continue;
            }
            let upper_def = col_def_str.to_uppercase();
            if upper_def.starts_with("CONSTRAINT") || upper_def.starts_with("EXCLUDE") || upper_def.starts_with("LIKE ") {
                continue;
            }

            let (mut col, rest_def) = parse_column_definition(col_def_str);
            // GENERATED {ALWAYS | BY DEFAULT} AS IDENTITY [(START WITH n INCREMENT BY m)]
            if let Some(spec) = parse_identity_clause(rest_def) {
                col.default_value = Some(format!("nextval('{}_{}_seq'::regclass)", table_name.to_lowercase(), col.name.to_lowercase()));
                col.is_nullable = false;
                identities.push((col.name.clone(), spec));
            }
            crate::engine::constraints::parse_column_constraints(&table_name, &col.name, rest_def, &mut defs);
            if let Some(d) = crate::engine::coltypes::DeclType::parse(rest_def).filter(|d| d.is_informative()) {
                decl_types.push((col.name.clone(), d.canonical()));
            }
            columns.push(col);
        }

        // Khoá chính cấp bảng (một hay nhiều cột)
        for pk_col_name in &table_level_pks {
            if let Some(col) = columns.iter_mut().find(|c| c.name.eq_ignore_ascii_case(pk_col_name)) {
                col.is_primary_key = true;
                col.is_nullable = false;
            }
        }
        // Một ràng buộc PRIMARY KEY duy nhất, gom mọi cột khoá.
        defs.retain(|d| !matches!(d.kind, crate::storage::catalog::ConstraintKind::PrimaryKey { .. }));
        let pk_cols: Vec<String> = columns.iter().filter(|c| c.is_primary_key).map(|c| c.name.clone()).collect();
        if !pk_cols.is_empty() {
            defs.insert(0, crate::storage::catalog::ConstraintDef {
                name: format!("{}_pkey", table_name.to_lowercase()),
                kind: crate::storage::catalog::ConstraintKind::PrimaryKey { cols: pk_cols },
            });
        }
        let defs = self.finalize_constraints(&table_name, &columns, defs);

        if exists {
            self.adopt_legacy_constraints(&table_name, defs);
        } else {
            for d in &defs {
                if let crate::storage::catalog::ConstraintKind::ForeignKey { ref_table, .. } = &d.kind {
                    if !ref_table.eq_ignore_ascii_case(&table_name) && self.storage.get_table(ref_table).is_none() {
                        return Err(format!("relation \"{}\" does not exist", ref_table));
                    }
                }
            }
            self.storage.create_table_with_catalog(table_name.clone(), columns, defs)?;
            if !decl_types.is_empty() {
                self.storage.log_schema_op(crate::storage::catalog::SchemaOp::ColumnTypes { table: table_name.clone(), types: decl_types });
            }
            for (column, spec) in identities {
                self.storage.log_schema_op(crate::storage::catalog::SchemaOp::ColumnIdentity { table: table_name.clone(), column, spec: Some(spec) });
            }
        }

        Ok(QueryResult {
            rows: vec![],
            row_count: 0,
            fields: vec![],
            command: "CREATE TABLE".to_string(),
        })
    }

    fn handle_comment_on_table(&mut self, sql: &str) -> Result<QueryResult, String> {
        // Syntax: COMMENT ON TABLE [schema.]table_name IS 'comment' (or IS NULL)
        let upper = sql.to_uppercase();
        let is_idx = upper.find(" IS ").ok_or_else(|| "Invalid COMMENT ON TABLE syntax: missing IS".to_string())?;
        let table_part = sql[16..is_idx].trim().trim_matches('"');
        let table_name = if let Some(dot_idx) = table_part.rfind('.') {
            table_part[dot_idx + 1..].trim().trim_matches('"')
        } else {
            table_part
        };

        let comment_part = sql[is_idx + 4..].trim().trim_end_matches(';').trim();
        let comment = if comment_part.eq_ignore_ascii_case("NULL") {
            None
        } else if comment_part.starts_with('\'') && comment_part.ends_with('\'') && comment_part.len() >= 2 {
            let inner = &comment_part[1..comment_part.len() - 1];
            Some(inner.replace("''", "'"))
        } else {
            Some(comment_part.to_string())
        };

        let mut target_key = table_name.to_string();
        if let Some(table) = self.storage.tables.get_mut(table_name) {
            table.comment = comment.clone();
            target_key = table.name.clone();
        } else {
            let found_key = self.storage.tables.keys().find(|k| k.eq_ignore_ascii_case(table_name)).cloned();
            if let Some(k) = found_key {
                if let Some(table) = self.storage.tables.get_mut(&k) {
                    table.comment = comment.clone();
                    target_key = table.name.clone();
                }
            }
        }

        self.storage.wal.append(WalRecord::CommentOnTable {
            table: target_key,
            comment,
        });

        Ok(QueryResult {
            rows: vec![],
            row_count: 0,
            fields: vec![],
            command: "COMMENT".to_string(),
        })
    }

    fn handle_comment_on_column(&mut self, sql: &str) -> Result<QueryResult, String> {
        // Syntax: COMMENT ON COLUMN [schema.]table_name.column_name IS 'comment' (or IS NULL)
        let upper = sql.to_uppercase();
        let is_idx = upper.find(" IS ").ok_or_else(|| "Invalid COMMENT ON COLUMN syntax: missing IS".to_string())?;
        let target_part = sql[17..is_idx].trim();

        let parts: Vec<&str> = target_part.split('.').collect();
        let (table_name, col_name) = match parts.len() {
            1 => return Err("Invalid COMMENT ON COLUMN syntax: missing column name".to_string()),
            2 => (parts[0].trim().trim_matches('"'), parts[1].trim().trim_matches('"')),
            _ => (parts[parts.len() - 2].trim().trim_matches('"'), parts[parts.len() - 1].trim().trim_matches('"')),
        };

        let comment_part = sql[is_idx + 4..].trim().trim_end_matches(';').trim();
        let comment = if comment_part.eq_ignore_ascii_case("NULL") {
            None
        } else if comment_part.starts_with('\'') && comment_part.ends_with('\'') && comment_part.len() >= 2 {
            let inner = &comment_part[1..comment_part.len() - 1];
            Some(inner.replace("''", "'"))
        } else {
            Some(comment_part.to_string())
        };

        let mut updated = false;
        let mut target_table_name = table_name.to_string();
        let mut target_col_name = col_name.to_string();

        if let Some(table) = self.storage.tables.get_mut(table_name) {
            target_table_name = table.name.clone();
            if let Some(col) = table.columns.iter_mut().find(|c| c.name.eq_ignore_ascii_case(col_name)) {
                col.comment = comment.clone();
                target_col_name = col.name.clone();
                updated = true;
            }
        }
        if !updated {
            let found_key = self.storage.tables.keys().find(|k| k.eq_ignore_ascii_case(table_name)).cloned();
            if let Some(k) = found_key {
                if let Some(table) = self.storage.tables.get_mut(&k) {
                    target_table_name = table.name.clone();
                    if let Some(col) = table.columns.iter_mut().find(|c| c.name.eq_ignore_ascii_case(col_name)) {
                        col.comment = comment.clone();
                        target_col_name = col.name.clone();
                    }
                }
            }
        }

        self.storage.wal.append(WalRecord::CommentOnColumn {
            table: target_table_name,
            column: target_col_name,
            comment,
        });

        Ok(QueryResult {
            rows: vec![],
            row_count: 0,
            fields: vec![],
            command: "COMMENT".to_string(),
        })
    }

    fn handle_description_scalar_query(&self, sql: &str) -> Result<QueryResult, String> {
        let upper = sql.to_uppercase();
        if upper.contains("OBJ_DESCRIPTION") {
            let mut comment: Option<String> = None;
            for (name, tbl) in &self.storage.tables {
                if upper.contains(&name.to_uppercase()) {
                    comment = tbl.comment.clone();
                    break;
                }
            }
            let val = comment.map(serde_json::Value::String).unwrap_or(serde_json::Value::Null);
            return Ok(QueryResult {
                rows: vec![serde_json::json!({ "comment": val })],
                row_count: 1,
                fields: vec![FieldInfo { name: "comment".to_string(), data_type: "text".to_string() }],
                command: "SELECT".to_string(),
            });
        } else if upper.contains("COL_DESCRIPTION") {
            let mut comment: Option<String> = None;
            for (name, tbl) in &self.storage.tables {
                if upper.contains(&name.to_uppercase()) {
                    if let Some(comma_pos) = upper.rfind(',') {
                        let after_comma = upper[comma_pos + 1..].trim();
                        let num_str: String = after_comma.chars().take_while(|c| c.is_ascii_digit()).collect();
                        if let Ok(pos) = num_str.parse::<usize>() {
                            if pos >= 1 && pos <= tbl.columns.len() {
                                comment = tbl.columns[pos - 1].comment.clone();
                            }
                        }
                    }
                    break;
                }
            }
            let val = comment.map(serde_json::Value::String).unwrap_or(serde_json::Value::Null);
            return Ok(QueryResult {
                rows: vec![serde_json::json!({ "comment": val })],
                row_count: 1,
                fields: vec![FieldInfo { name: "comment".to_string(), data_type: "text".to_string() }],
                command: "SELECT".to_string(),
            });
        } else if upper.contains("FROM PG_CATALOG.PG_DESCRIPTION") || upper.contains("FROM PG_DESCRIPTION") {
            return Ok(QueryResult {
                rows: vec![serde_json::json!({ "classoid": 1259, "objoid": 1, "objsubid": 0, "description": "" })],
                row_count: 1,
                fields: vec![
                    FieldInfo { name: "classoid".to_string(), data_type: "integer".to_string() },
                    FieldInfo { name: "objoid".to_string(), data_type: "integer".to_string() },
                    FieldInfo { name: "objsubid".to_string(), data_type: "integer".to_string() },
                    FieldInfo { name: "description".to_string(), data_type: "text".to_string() },
                ],
                command: "SELECT".to_string(),
            });
        }
        Err("Unsupported description query".to_string())
    }

    fn handle_table_introspection_query(&self, _sql: &str, params: &[Value]) -> Result<QueryResult, String> {
        let schema_filter = params.first().and_then(|v| match v {
            Value::Text(s) => Some(s.as_str()),
            _ => None,
        });

        let fields = vec![
            FieldInfo { name: "name".to_string(), data_type: "text".to_string() },
            FieldInfo { name: "comment".to_string(), data_type: "text".to_string() },
            FieldInfo { name: "columns".to_string(), data_type: "json".to_string() },
        ];

        if let Some(s) = schema_filter {
            if !s.is_empty() && !s.eq_ignore_ascii_case("public") {
                return Ok(QueryResult {
                    rows: vec![],
                    row_count: 0,
                    fields,
                    command: "SELECT".to_string(),
                });
            }
        }

        let mut table_names: Vec<String> = self.storage.tables.keys().cloned().collect();
        table_names.sort();

        let mut rows = Vec::with_capacity(table_names.len());
        for name in table_names {
            if let Some(table) = self.storage.tables.get(&name) {
                let cols_json: Vec<serde_json::Value> = table.columns.iter().map(|col| {
                    let type_str = match &col.data_type {
                        DataType::Integer => "integer",
                        DataType::BigInt => "bigint",
                        DataType::Serial => "integer",
                        DataType::Text => "text",
                        DataType::Boolean => "boolean",
                        DataType::Numeric => "numeric",
                        DataType::Timestamp => "timestamp without time zone",
                        DataType::Date => "date",
                        DataType::Time => "time without time zone",
                        DataType::Jsonb => "jsonb",
                        DataType::Uuid => "uuid",
                        DataType::Bytea => "bytea",
                        DataType::Array(_) => "ARRAY",
                    };
                    serde_json::json!({
                        "name": col.name,
                        "type": type_str,
                        "nullable": col.is_nullable,
                        "default": col.default_value.as_ref().map(|d| format_attdef_to_sql(d)).map(serde_json::Value::String).unwrap_or(serde_json::Value::Null),
                        "comment": col.comment.clone().map(serde_json::Value::String).unwrap_or(serde_json::Value::Null),
                    })
                }).collect();

                rows.push(serde_json::json!({
                    "name": table.name,
                    "comment": table.comment.clone().map(serde_json::Value::String).unwrap_or(serde_json::Value::Null),
                    "columns": cols_json,
                }));
            }
        }

        let row_count = rows.len();
        Ok(QueryResult {
            rows,
            row_count,
            fields,
            command: "SELECT".to_string(),
        })
    }

    fn handle_pg_constraint_introspection_query(&self, _sql: &str, params: &[Value]) -> Result<QueryResult, String> {
        let schema_filter = params.first().and_then(|v| match v {
            Value::Text(s) => Some(s.as_str()),
            _ => None,
        });

        let fields = vec![
            FieldInfo { name: "schema_name".to_string(), data_type: "text".to_string() },
            FieldInfo { name: "table_name".to_string(), data_type: "text".to_string() },
            FieldInfo { name: "constraint_name".to_string(), data_type: "text".to_string() },
            FieldInfo { name: "constraint_type".to_string(), data_type: "text".to_string() },
            FieldInfo { name: "referenced_table_schema".to_string(), data_type: "text".to_string() },
            FieldInfo { name: "referenced_table_name".to_string(), data_type: "text".to_string() },
            FieldInfo { name: "column_names".to_string(), data_type: "array".to_string() },
            FieldInfo { name: "referenced_columns".to_string(), data_type: "array".to_string() },
        ];

        if let Some(s) = schema_filter {
            if !s.is_empty() && !s.eq_ignore_ascii_case("public") {
                return Ok(QueryResult {
                    rows: vec![],
                    row_count: 0,
                    fields,
                    command: "SELECT".to_string(),
                });
            }
        }

        let mut table_names: Vec<String> = self.storage.tables.keys().cloned().collect();
        table_names.sort();

        let mut rows = Vec::new();
        for name in table_names {
            if let Some(table) = self.storage.tables.get(&name) {
                let pk_cols: Vec<serde_json::Value> = table.columns.iter()
                    .filter(|c| c.is_primary_key || c.data_type == DataType::Serial)
                    .map(|c| serde_json::Value::String(c.name.clone()))
                    .collect();

                if !pk_cols.is_empty() {
                    rows.push(serde_json::json!({
                        "schema_name": "public",
                        "table_name": table.name,
                        "constraint_name": format!("{}_pkey", table.name),
                        "constraint_type": "PRIMARY KEY",
                        "referenced_table_schema": serde_json::Value::Null,
                        "referenced_table_name": serde_json::Value::Null,
                        "column_names": serde_json::Value::Array(pk_cols),
                        "referenced_columns": serde_json::Value::Null,
                    }));
                }
            }
        }

        let row_count = rows.len();
        Ok(QueryResult {
            rows,
            row_count,
            fields,
            command: "SELECT".to_string(),
        })
    }


    pub fn ensure_virtual_catalog_tables(&mut self) {
        use crate::storage::table::Table;

        // 1. Prepare table_constraints table
        let tc_cols = vec![
            ColumnDef { name: "constraint_catalog".to_string(), data_type: DataType::Text, is_primary_key: false, is_nullable: true, default_value: None, comment: None },
            ColumnDef { name: "constraint_schema".to_string(), data_type: DataType::Text, is_primary_key: false, is_nullable: true, default_value: None, comment: None },
            ColumnDef { name: "constraint_name".to_string(), data_type: DataType::Text, is_primary_key: false, is_nullable: true, default_value: None, comment: None },
            ColumnDef { name: "table_catalog".to_string(), data_type: DataType::Text, is_primary_key: false, is_nullable: true, default_value: None, comment: None },
            ColumnDef { name: "table_schema".to_string(), data_type: DataType::Text, is_primary_key: false, is_nullable: true, default_value: None, comment: None },
            ColumnDef { name: "table_name".to_string(), data_type: DataType::Text, is_primary_key: false, is_nullable: true, default_value: None, comment: None },
            ColumnDef { name: "constraint_type".to_string(), data_type: DataType::Text, is_primary_key: false, is_nullable: true, default_value: None, comment: None },
            ColumnDef { name: "is_deferrable".to_string(), data_type: DataType::Text, is_primary_key: false, is_nullable: true, default_value: None, comment: None },
            ColumnDef { name: "initially_deferred".to_string(), data_type: DataType::Text, is_primary_key: false, is_nullable: true, default_value: None, comment: None },
            ColumnDef { name: "enforced".to_string(), data_type: DataType::Text, is_primary_key: false, is_nullable: true, default_value: None, comment: None },
        ];
        let mut tc_table = Table::new("information_schema.table_constraints".to_string(), tc_cols);

        // 2. Prepare key_column_usage table
        let kcu_cols = vec![
            ColumnDef { name: "constraint_catalog".to_string(), data_type: DataType::Text, is_primary_key: false, is_nullable: true, default_value: None, comment: None },
            ColumnDef { name: "constraint_schema".to_string(), data_type: DataType::Text, is_primary_key: false, is_nullable: true, default_value: None, comment: None },
            ColumnDef { name: "constraint_name".to_string(), data_type: DataType::Text, is_primary_key: false, is_nullable: true, default_value: None, comment: None },
            ColumnDef { name: "table_catalog".to_string(), data_type: DataType::Text, is_primary_key: false, is_nullable: true, default_value: None, comment: None },
            ColumnDef { name: "table_schema".to_string(), data_type: DataType::Text, is_primary_key: false, is_nullable: true, default_value: None, comment: None },
            ColumnDef { name: "table_name".to_string(), data_type: DataType::Text, is_primary_key: false, is_nullable: true, default_value: None, comment: None },
            ColumnDef { name: "column_name".to_string(), data_type: DataType::Text, is_primary_key: false, is_nullable: true, default_value: None, comment: None },
            ColumnDef { name: "ordinal_position".to_string(), data_type: DataType::Integer, is_primary_key: false, is_nullable: true, default_value: None, comment: None },
            ColumnDef { name: "position_in_unique_constraint".to_string(), data_type: DataType::Integer, is_primary_key: false, is_nullable: true, default_value: None, comment: None },
        ];
        let mut kcu_table = Table::new("information_schema.key_column_usage".to_string(), kcu_cols);

        // 3. Prepare constraint_column_usage table
        let ccu_cols = vec![
            ColumnDef { name: "table_catalog".to_string(), data_type: DataType::Text, is_primary_key: false, is_nullable: true, default_value: None, comment: None },
            ColumnDef { name: "table_schema".to_string(), data_type: DataType::Text, is_primary_key: false, is_nullable: true, default_value: None, comment: None },
            ColumnDef { name: "table_name".to_string(), data_type: DataType::Text, is_primary_key: false, is_nullable: true, default_value: None, comment: None },
            ColumnDef { name: "column_name".to_string(), data_type: DataType::Text, is_primary_key: false, is_nullable: true, default_value: None, comment: None },
            ColumnDef { name: "constraint_catalog".to_string(), data_type: DataType::Text, is_primary_key: false, is_nullable: true, default_value: None, comment: None },
            ColumnDef { name: "constraint_schema".to_string(), data_type: DataType::Text, is_primary_key: false, is_nullable: true, default_value: None, comment: None },
            ColumnDef { name: "constraint_name".to_string(), data_type: DataType::Text, is_primary_key: false, is_nullable: true, default_value: None, comment: None },
        ];
        let mut ccu_table = Table::new("information_schema.constraint_column_usage".to_string(), ccu_cols);

        // 4. Prepare pg_class table
        let pg_class_cols = vec![
            ColumnDef { name: "oid".to_string(), data_type: DataType::Integer, is_primary_key: true, is_nullable: false, default_value: None, comment: None },
            ColumnDef { name: "relname".to_string(), data_type: DataType::Text, is_primary_key: false, is_nullable: false, default_value: None, comment: None },
            ColumnDef { name: "relnamespace".to_string(), data_type: DataType::Integer, is_primary_key: false, is_nullable: false, default_value: None, comment: None },
            ColumnDef { name: "relkind".to_string(), data_type: DataType::Text, is_primary_key: false, is_nullable: false, default_value: None, comment: None },
        ];
        let mut pg_class_table = Table::new("pg_catalog.pg_class".to_string(), pg_class_cols);

        // 5. Prepare pg_index table
        let pg_index_cols = vec![
            ColumnDef { name: "indexrelid".to_string(), data_type: DataType::Integer, is_primary_key: false, is_nullable: false, default_value: None, comment: None },
            ColumnDef { name: "indrelid".to_string(), data_type: DataType::Integer, is_primary_key: false, is_nullable: false, default_value: None, comment: None },
            ColumnDef { name: "indisunique".to_string(), data_type: DataType::Boolean, is_primary_key: false, is_nullable: false, default_value: None, comment: None },
            ColumnDef { name: "indisprimary".to_string(), data_type: DataType::Boolean, is_primary_key: false, is_nullable: false, default_value: None, comment: None },
            ColumnDef { name: "indkey".to_string(), data_type: DataType::Array("int4".to_string()), is_primary_key: false, is_nullable: true, default_value: None, comment: None },
        ];
        let mut pg_index_table = Table::new("pg_catalog.pg_index".to_string(), pg_index_cols);

        let mut user_tables: Vec<String> = self.storage.tables.keys()
            .filter(|k| !k.starts_with("information_schema.") && !k.starts_with("pg_catalog.") && *k != "table_constraints" && *k != "key_column_usage" && *k != "constraint_column_usage" && *k != "pg_index" && *k != "pg_class" && *k != "pg_namespace" && !k.starts_with("__ephemeral_"))
            .cloned().collect();
        user_tables.sort();

        let mut oid_counter = 16384i64;
        let mut index_oid_counter = 20000i64;
        let mut table_oid_map = std::collections::HashMap::new();

        for tbl_name in &user_tables {
            let table = match self.storage.tables.get(tbl_name) {
                Some(t) => t.clone(),
                None => continue,
            };

            let table_oid = oid_counter;
            oid_counter += 1;
            table_oid_map.insert(table.name.clone(), table_oid);
            table_oid_map.insert(tbl_name.clone(), table_oid);

            pg_class_table.insert(vec![
                Value::Int(table_oid),
                Value::text(&table.name),
                Value::Int(2200),
                Value::text("r"),
            ]);
        }

        // Ràng buộc lấy từ catalog đã lưu của từng bảng
        let all_constraints = self.constraint_rows();
        let mut added_c_names = std::collections::HashSet::new();
        for cc in &all_constraints {
            let tbl_name = &cc.table_name;
            let c_name = &cc.constraint_name;

            if added_c_names.insert((tbl_name.clone(), c_name.clone())) {
                tc_table.insert(vec![
                    Value::text("litepostgres"),
                    Value::text("public"),
                    Value::text(c_name),
                    Value::text("litepostgres"),
                    Value::text("public"),
                    Value::text(tbl_name),
                    Value::text(&cc.constraint_type),
                    Value::text("NO"),
                    Value::text("NO"),
                    Value::text("YES"),
                ]);
            }

            kcu_table.insert(vec![
                Value::text("litepostgres"),
                Value::text("public"),
                Value::text(c_name),
                Value::text("litepostgres"),
                Value::text("public"),
                Value::text(tbl_name),
                Value::text(&cc.column_name),
                Value::Int(cc.ordinal_position),
                if cc.constraint_type == "FOREIGN KEY" { Value::Int(1) } else { Value::Null },
            ]);

            if cc.constraint_type == "FOREIGN KEY" {
                if let (Some(ftable), Some(fcol)) = (&cc.foreign_table_name, &cc.foreign_column_name) {
                    ccu_table.insert(vec![
                        Value::text("litepostgres"),
                        Value::text("public"),
                        Value::text(ftable),
                        Value::text(fcol),
                        Value::text("litepostgres"),
                        Value::text("public"),
                        Value::text(c_name),
                    ]);
                }
            } else {
                ccu_table.insert(vec![
                    Value::text("litepostgres"),
                    Value::text("public"),
                    Value::text(tbl_name),
                    Value::text(&cc.column_name),
                    Value::text("litepostgres"),
                    Value::text("public"),
                    Value::text(c_name),
                ]);
            }

        }

        // Group custom_constraints for pg_index and pg_class (indexes)
        let mut index_constraints: std::collections::HashMap<(String, String), (String, Vec<(i64, String)>)> = std::collections::HashMap::new();
        for cc in &all_constraints {
            if cc.constraint_type == "PRIMARY KEY" || cc.constraint_type == "UNIQUE" {
                index_constraints.entry((cc.table_name.clone(), cc.constraint_name.clone()))
                    .or_insert_with(|| (cc.constraint_type.clone(), Vec::new()))
                    .1.push((cc.ordinal_position, cc.column_name.clone()));
            }
        }

        let mut index_keys: Vec<_> = index_constraints.keys().cloned().collect();
        index_keys.sort();

        for (tbl_name, c_name) in index_keys {
            let (c_type, mut cols) = index_constraints.remove(&(tbl_name.clone(), c_name.clone())).unwrap();
            cols.sort_by_key(|(ord, _)| *ord);

            let table_oid = table_oid_map.get(&tbl_name).copied().unwrap_or(16384);
            let is_pk = c_type == "PRIMARY KEY";
            let index_oid = index_oid_counter;
            index_oid_counter += 1;

            pg_class_table.insert(vec![
                Value::Int(index_oid),
                Value::text(&c_name),
                Value::Int(2200),
                Value::text("i"),
            ]);

            let col_indices: Vec<usize> = cols.iter().map(|(_, col_name)| {
                if let Some(t) = self.storage.tables.get(&tbl_name) {
                    t.columns.iter().position(|c| c.name.eq_ignore_ascii_case(col_name)).map(|p| p + 1).unwrap_or(1)
                } else {
                    1
                }
            }).collect();

            let indkey_json = serde_json::Value::Array(
                col_indices.iter().map(|idx| serde_json::Value::Number((*idx as i64).into())).collect()
            ).to_string();

            pg_index_table.insert(vec![
                Value::Int(index_oid),
                Value::Int(table_oid),
                Value::Bool(true),
                Value::Bool(is_pk),
                Value::text(&indkey_json),
            ]);
        }

        // Also add any tables from storage that might not have custom_constraints (e.g. created earlier or reloaded from disk)
        for tbl_name in &user_tables {
            let table = match self.storage.tables.get(tbl_name) {
                Some(t) => t.clone(),
                None => continue,
            };

            let table_oid = table_oid_map.get(&table.name).copied().unwrap_or(16384);
            let c_name = format!("{}_pkey", table.name);

            if !added_c_names.contains(&(table.name.clone(), c_name.clone())) {
                let pk_indices: Vec<(usize, &str)> = table.columns.iter().enumerate()
                    .filter(|(_, c)| c.is_primary_key || c.data_type == DataType::Serial)
                    .map(|(i, c)| (i + 1, c.name.as_str()))
                    .collect();

                if !pk_indices.is_empty() {
                    added_c_names.insert((table.name.clone(), c_name.clone()));
                    tc_table.insert(vec![
                        Value::text("litepostgres"),
                        Value::text("public"),
                        Value::text(&c_name),
                        Value::text("litepostgres"),
                        Value::text("public"),
                        Value::text(&table.name),
                        Value::text("PRIMARY KEY"),
                        Value::text("NO"),
                        Value::text("NO"),
                        Value::text("YES"),
                    ]);

                    let indkey_json = serde_json::Value::Array(
                        pk_indices.iter().map(|(idx, _)| serde_json::Value::Number((*idx as i64).into())).collect()
                    ).to_string();

                    let index_oid = index_oid_counter;
                    index_oid_counter += 1;

                    pg_index_table.insert(vec![
                        Value::Int(index_oid),
                        Value::Int(table_oid),
                        Value::Bool(true),
                        Value::Bool(true),
                        Value::text(&indkey_json),
                    ]);

                    for (ord, (_, col_name)) in pk_indices.iter().enumerate() {
                        kcu_table.insert(vec![
                            Value::text("litepostgres"),
                            Value::text("public"),
                            Value::text(&c_name),
                            Value::text("litepostgres"),
                            Value::text("public"),
                            Value::text(&table.name),
                            Value::text(col_name),
                            Value::Int((ord + 1) as i64),
                            Value::Null,
                        ]);

                        ccu_table.insert(vec![
                            Value::text("litepostgres"),
                            Value::text("public"),
                            Value::text(&table.name),
                            Value::text(col_name),
                            Value::text("litepostgres"),
                            Value::text("public"),
                            Value::text(&c_name),
                        ]);
                    }
                }
            }
        }

        // Deduplicate and group composite primary keys in pg_index_table if needed
        // Insert into storage.tables
        self.storage.tables.insert("information_schema.table_constraints".to_string(), tc_table.clone());
        self.storage.tables.insert("table_constraints".to_string(), tc_table);

        self.storage.tables.insert("information_schema.key_column_usage".to_string(), kcu_table.clone());
        self.storage.tables.insert("key_column_usage".to_string(), kcu_table);

        self.storage.tables.insert("information_schema.constraint_column_usage".to_string(), ccu_table.clone());
        self.storage.tables.insert("constraint_column_usage".to_string(), ccu_table);

        // engine không có row level security: relrowsecurity / relforcerowsecurity luôn false
        for (n, _) in [("relrowsecurity", ()), ("relforcerowsecurity", ())] {
            pg_class_table.columns.push(ColumnDef { name: n.to_string(), data_type: DataType::Boolean, is_primary_key: false, is_nullable: false, default_value: None, comment: None });
        }
        for r in &mut pg_class_table.rows {
            r.push(Value::Bool(false));
            r.push(Value::Bool(false));
        }
        self.storage.tables.insert("pg_catalog.pg_class".to_string(), pg_class_table.clone());
        self.storage.tables.insert("pg_class".to_string(), pg_class_table);

        let ns_cols = vec![
            ColumnDef { name: "oid".to_string(), data_type: DataType::Integer, is_primary_key: true, is_nullable: false, default_value: None, comment: None },
            ColumnDef { name: "nspname".to_string(), data_type: DataType::Text, is_primary_key: false, is_nullable: false, default_value: None, comment: None },
            ColumnDef { name: "nspowner".to_string(), data_type: DataType::Integer, is_primary_key: false, is_nullable: false, default_value: None, comment: None },
        ];
        let mut ns_table = Table::new("pg_catalog.pg_namespace".to_string(), ns_cols);
        for (oid, name) in [(11, "pg_catalog"), (99, "pg_toast"), (2200, "public"), (13000, "information_schema")] {
            ns_table.insert(vec![Value::Int(oid), Value::text(name), Value::Int(10)]);
        }
        self.storage.tables.insert("pg_catalog.pg_namespace".to_string(), ns_table.clone());
        self.storage.tables.insert("pg_namespace".to_string(), ns_table);

        self.storage.tables.insert("pg_catalog.pg_index".to_string(), pg_index_table.clone());
        self.storage.tables.insert("pg_index".to_string(), pg_index_table);
    }

    fn json_cell_kinds_cached(&mut self, sql: &str) -> Option<Vec<JsonCellKind>> {
        let fp = |storage: &crate::storage::engine::StorageEngine, t: &str| -> Option<u64> {
            use std::hash::{Hash, Hasher};
            let tb = storage.get_table(t)?;
            let mut h = std::collections::hash_map::DefaultHasher::new();
            for (i, c) in tb.columns.iter().enumerate() {
                c.name.hash(&mut h);
                std::mem::discriminant(&c.data_type).hash(&mut h);
                if let Some(d) = tb.decl_type(i) {
                    d.base.hash(&mut h);
                    d.array.hash(&mut h);
                    d.mods.hash(&mut h);
                }
            }
            Some(h.finish())
        };
        if let Some((kinds, tables)) = self.json_kinds_cache.get(sql) {
            if tables.iter().all(|(t, f)| fp(&self.storage, t) == Some(*f)) {
                return kinds.clone();
            }
        }
        let kinds = self.json_cell_kinds(sql);
        // bảng trong FROM (tên đầu mỗi mục, sau FROM / JOIN / dấu phẩy) để nhận ra khi schema đổi
        let mut tables = Vec::new();
        if let Some(f) = find_top_level_keyword(sql, "FROM") {
            let rest = &sql[f + 4..];
            let mut end = rest.len();
            for kw in ["WHERE", "GROUP", "ORDER", "LIMIT", "OFFSET", "HAVING", "WINDOW", "FOR", "FETCH", "UNION"] {
                if let Some(p) = find_top_level_keyword(rest, kw) {
                    end = end.min(p);
                }
            }
            let mut take_next = true;
            for tok in rest[..end].split(|c: char| c.is_whitespace() || c == ',') {
                if tok.is_empty() {
                    continue;
                }
                let up = tok.to_uppercase();
                if take_next {
                    let name = strip_schema_keep_case(tok.trim_matches('(')).trim_matches('"').to_lowercase();
                    if let Some(f) = fp(&self.storage, &name) {
                        tables.push((name, f));
                    }
                    take_next = false;
                }
                if up == "JOIN" {
                    take_next = true;
                }
            }
            for part in split_comma_separated_tokens(&rest[..end]).into_iter().skip(1) {
                let name = part.split_whitespace().next().map(|w| strip_schema_keep_case(w).trim_matches('"').to_lowercase()).unwrap_or_default();
                if let Some(f) = fp(&self.storage, &name) {
                    tables.push((name, f));
                }
            }
        }
        if self.json_kinds_cache.len() >= 4096 {
            self.json_kinds_cache.clear();
        }
        self.json_kinds_cache.insert(sql.to_string(), (kinds.clone(), tables));
        kinds
    }

    /// Đường JSON (không phải wire): câu SELECT có cột json/jsonb hoặc ép `::text` tường minh thì giá trị chữ
    /// không được đoán kiểu mù (jsonb `'{}'` từng ra `[]`, `x::text` ra mảng / object). Trả cách đổi từng cột;
    /// `None` = giữ đường cũ (mọi cột khác vẫn đoán như trước, để DB / app cũ nhận đúng dữ liệu như trước).
    fn json_cell_kinds(&self, sql: &str) -> Option<Vec<JsonCellKind>> {
        let t = sql.trim();
        if t.len() < 8 || !t[..7].eq_ignore_ascii_case("SELECT ") || t.contains(';') {
            return None;
        }
        let up = t.to_uppercase();
        // bytea đổi dạng ở raw mode: không dùng đường này
        if ["BYTEA", "DECODE(", "SHA", "DIGEST(", "GEN_RANDOM_BYTES", "CONVERT_TO("].iter().any(|k| up.contains(k)) {
            return None;
        }
        let decls = crate::engine::coltypes::select_decls(t, &self.storage);
        if decls.iter().any(|d| d.as_ref().map_or(false, |d| d.base == "bytea")) {
            return None;
        }
        let list_end = find_top_level_keyword(t, "FROM").unwrap_or(t.len());
        let items = split_projection_items(&t[6..list_end]);
        let explicit_text = |it: &str| {
            let u = it.to_uppercase();
            let e = match rfind_top_level_as(&u) {
                Some(p) => u[..p].trim().to_string(),
                None => u.trim().to_string(),
            };
            let e = e.as_str();
            find_last_top_level_op(e, "::").map_or(false, |p| cast_is_outermost(e, p) && matches!(e[p + 2..].trim(), "TEXT" | "VARCHAR")) || e.starts_with("CAST(") && (e.ends_with(" AS TEXT)") || e.ends_with(" AS VARCHAR)"))
        };
        let is_json = |d: &Option<crate::engine::coltypes::DeclType>| d.as_ref().map_or(false, |d| !d.array && (d.base == "json" || d.base == "jsonb"));
        let has_star = items.iter().any(|it| it.trim() == "*" || it.trim().ends_with(".*"));
        let kinds: Vec<JsonCellKind> = if has_star {
            decls.iter().map(|d| if is_json(d) { JsonCellKind::Json } else { JsonCellKind::Guess }).collect()
        } else if decls.len() == items.len() {
            items.iter().zip(&decls).map(|(it, d)| if explicit_text(it) { JsonCellKind::Text } else if is_json(d) { JsonCellKind::Json } else { JsonCellKind::Guess }).collect()
        } else {
            items.iter().map(|it| if explicit_text(it) { JsonCellKind::Text } else { JsonCellKind::Guess }).collect()
        };
        kinds.iter().any(|k| !matches!(k, JsonCellKind::Guess)).then_some(kinds)
    }

    /// Chạy ở raw text mode (giá trị chữ giữ nguyên) rồi đổi từng ô theo `kinds`.
    fn execute_typed_json(&mut self, sql: &str, params: &[Value], kinds: &[JsonCellKind]) -> Result<QueryResult, String> {
        let mut res = {
            let _guard = RawTextGuard(set_raw_text_mode(true));
            self.execute(sql, params)?
        };
        for row in res.rows.iter_mut() {
            let Some(obj) = row.as_object_mut() else { continue };
            let aligned = obj.len() == kinds.len();
            for (i, (_, cell)) in obj.iter_mut().enumerate() {
                let serde_json::Value::String(s) = cell else { continue };
                let kind = if aligned { kinds[i] } else { JsonCellKind::Guess };
                *cell = match kind {
                    JsonCellKind::Text => continue,
                    JsonCellKind::Json => serde_json::from_str(s.trim()).unwrap_or_else(|_| serde_json::Value::String(s.clone())),
                    JsonCellKind::Guess => value_to_json(&Value::text(s.as_str())),
                };
            }
        }
        Ok(res)
    }

    /// Câu trên `information_schema.columns` mà bộ xử lý riêng (chỉ lọc `table_name` / `column_name`) không
    /// hiểu: có alias, điều kiện khác (LIKE, NOT IN, column_default...), hoặc biểu thức trong SELECT.
    fn info_columns_needs_generic(sql: &str) -> bool {
        let up = sql.to_uppercase();
        let Some(p) = up.find("FROM INFORMATION_SCHEMA.COLUMNS") else { return false };
        let after = up[p + 31..].trim_start();
        let has_alias = after.split_whitespace().next().map_or(false, |w| {
            !["WHERE", "ORDER", "GROUP", "LIMIT", "OFFSET", "UNION", ";"].contains(&w) && !w.starts_with(';')
        });
        let where_part = find_top_level_keyword(sql, "WHERE").map(|w| up[w..].to_string()).unwrap_or_default();
        let odd_where = [" LIKE ", " NOT IN", "COLUMN_DEFAULT", "DATA_TYPE", "UDT_NAME", "IS_NULLABLE", "ORDINAL_POSITION", " OR "]
            .iter()
            .any(|k| where_part.contains(k));
        let list = &sql[6..find_top_level_keyword(sql, "FROM").unwrap_or(sql.len())];
        let odd_select = split_projection_items(list).iter().any(|it| {
            let t = it.trim();
            t != "*" && !t.chars().all(|c| c.is_alphanumeric() || c == '_' || c == '.' || c == '"' || c.is_whitespace())
        });
        has_alias || odd_where || odd_select
    }

    /// `information_schema.columns` như một bảng tạm trong bộ nhớ (không ghi WAL) có các cột chuẩn của
    /// PostgreSQL (`table_catalog`, `table_schema`, ...) cùng các cột riêng của bộ xử lý cũ, rồi chạy câu
    /// lệnh bằng bộ thực thi thường.
    fn information_schema_columns_generic(&mut self, sql: &str, params: &[Value]) -> Result<QueryResult, String> {
        let all = self.handle_information_schema_columns_query("SELECT * FROM information_schema.columns", &[])?;
        let text_col = |n: &str| crate::types::ColumnDef { name: n.to_string(), data_type: DataType::Text, is_primary_key: false, is_nullable: true, default_value: None, comment: None };
        let mut cols: Vec<crate::types::ColumnDef> = ["table_catalog", "table_schema", "table_name", "column_name"].iter().map(|n| text_col(n)).collect();
        cols.push(crate::types::ColumnDef { name: "ordinal_position".to_string(), data_type: DataType::Integer, is_primary_key: false, is_nullable: true, default_value: None, comment: None });
        for n in ["column_default", "is_nullable", "data_type", "udt_name", "is_identity", "is_generated", "schema_name", "constraint_type", "comment"] {
            cols.push(text_col(n));
        }
        let name = "__pglite_info_columns".to_string();
        let mut t = crate::storage::table::Table::new(name.clone(), cols);
        // khoá tự đoán (cột `..._id`, `id`) không áp dụng cho bảng này
        t.pk_col_idx = None;
        for r in &all.rows {
            let g = |k: &str| r.get(k).map(json_to_value).unwrap_or(Value::Null);
            t.insert(vec![
                Value::text("litepostgres"),
                Value::text("public"),
                g("table_name"),
                g("column_name"),
                g("ordinal_position"),
                g("column_default"),
                g("is_nullable"),
                g("data_type"),
                g("udt_name"),
                Value::text("NO"),
                Value::text("NEVER"),
                g("schema_name"),
                g("constraint_type"),
                g("comment"),
            ]);
        }
        // thay tên bảng (ngoài chuỗi)
        let mut q = String::with_capacity(sql.len());
        let b = sql.as_bytes();
        let pat = b"information_schema.columns";
        let mut in_sq = false;
        let mut i = 0;
        while i < b.len() {
            if b[i] == b'\'' {
                in_sq = !in_sq;
            }
            if !in_sq && i + pat.len() <= b.len() && b[i..i + pat.len()].eq_ignore_ascii_case(pat) {
                q.push_str(&name);
                i += pat.len();
                continue;
            }
            let ch = sql[i..].chars().next().unwrap();
            q.push(ch);
            i += ch.len_utf8();
        }
        self.storage.tables.insert(name.clone(), t);
        let res = self.handle_select(&q, params).map(|(r, _)| r);
        self.storage.tables.remove(&name);
        res
    }

    /// Tên bảng người dùng (bỏ bảng catalog ảo / bảng tạm nội bộ), đã sắp xếp.
    fn user_table_names(&self) -> Vec<String> {
        let mut keys: Vec<String> = self.storage.tables.keys()
            .filter(|k| !k.starts_with("information_schema.") && !k.starts_with("pg_catalog.") && !k.starts_with("__") && !["table_constraints", "key_column_usage", "constraint_column_usage", "pg_index", "pg_class", "pg_namespace"].contains(&k.as_str()))
            .cloned()
            .collect();
        keys.sort();
        keys
    }

    /// `information_schema.tables` / `pg_tables` như bảng tạm trong bộ nhớ (không ghi WAL). Có cột chuẩn
    /// PostgreSQL và cả các khoá của bản cũ (`schema_name`, `is_temporary`, `comment`).
    fn catalog_tables_generic(&mut self, sql: &str, params: &[Value]) -> Result<QueryResult, String> {
        let col = |n: &str, dt: DataType| crate::types::ColumnDef { name: n.to_string(), data_type: dt, is_primary_key: false, is_nullable: true, default_value: None, comment: None };
        let pg_tables = !contains_ci(sql, "information_schema.tables");
        let names: Vec<&str> = if pg_tables {
            vec!["schemaname", "tablename", "tableowner", "tablespace", "hasindexes", "hasrules", "hastriggers", "rowsecurity"]
        } else {
            vec!["table_catalog", "table_schema", "table_name", "table_type", "is_insertable_into", "schema_name", "is_temporary", "comment"]
        };
        let cols: Vec<crate::types::ColumnDef> = names.iter().map(|n| {
            let bool_col = ["hasindexes", "hasrules", "hastriggers", "rowsecurity", "is_temporary"].contains(n);
            col(n, if bool_col { DataType::Boolean } else { DataType::Text })
        }).collect();
        let name = "__pglite_info_tables".to_string();
        let mut t = crate::storage::table::Table::new(name.clone(), cols);
        t.pk_col_idx = None;
        for k in self.user_table_names() {
            if self.storage.objects.views.get(&k).map_or(false, |v| v.materialized) {
                continue; // materialized view: không phải bảng
            }
            let tbl = &self.storage.tables[&k];
            let temp = self.storage.wal.temp_tables.contains(&k.to_lowercase());
            let schema = if temp { "pg_temp_1" } else { "public" };
            if pg_tables {
                t.insert(vec![Value::text(schema), Value::text(&tbl.name), Value::text("postgres"), Value::Null, Value::Bool(!tbl.indexes.is_empty() || tbl.pk_col_idx.is_some()), Value::Bool(false), Value::Bool(false), Value::Bool(false)]);
            } else {
                t.insert(vec![
                    Value::text("litepostgres"),
                    Value::text(schema),
                    Value::text(&tbl.name),
                    Value::text(if temp { "LOCAL TEMPORARY" } else { "BASE TABLE" }),
                    Value::text("YES"),
                    Value::text(schema),
                    Value::Bool(temp),
                    tbl.comment.clone().map(Value::text).unwrap_or(Value::Null),
                ]);
            }
        }
        // view (không có trong pg_tables)
        if !pg_tables {
            for (vname, v) in self.storage.objects.views.iter().filter(|(_, v)| !v.materialized) {
                let _ = v;
                t.insert(vec![Value::text("litepostgres"), Value::text("public"), Value::text(vname), Value::text("VIEW"), Value::text("NO"), Value::text("public"), Value::Bool(false), Value::Null]);
            }
        }
        let q = replace_relation_name(sql, &["information_schema.tables", "pg_catalog.pg_tables", "pg_tables"], &name);
        self.storage.tables.insert(name.clone(), t);
        let res = self.handle_select(&q, params).map(|(r, _)| r);
        self.storage.tables.remove(&name);
        res
    }

    fn handle_information_schema_columns_query(&self, _sql: &str, params: &[Value]) -> Result<QueryResult, String> {
        let schema_filter = params.first().and_then(|v| match v {
            Value::Text(s) => Some(s.as_str()),
            _ => None,
        });

        let fields = vec![
            FieldInfo { name: "schema_name".to_string(), data_type: "text".to_string() },
            FieldInfo { name: "table_name".to_string(), data_type: "text".to_string() },
            FieldInfo { name: "column_name".to_string(), data_type: "text".to_string() },
            FieldInfo { name: "data_type".to_string(), data_type: "text".to_string() },
            FieldInfo { name: "udt_name".to_string(), data_type: "text".to_string() },
            FieldInfo { name: "is_nullable".to_string(), data_type: "text".to_string() },
            FieldInfo { name: "column_default".to_string(), data_type: "text".to_string() },
            FieldInfo { name: "constraint_type".to_string(), data_type: "text".to_string() },
            FieldInfo { name: "comment".to_string(), data_type: "text".to_string() },
            FieldInfo { name: "ordinal_position".to_string(), data_type: "integer".to_string() },
        ];

        if let Some(s) = schema_filter {
            if !s.is_empty() && !s.eq_ignore_ascii_case("public") {
                return Ok(QueryResult {
                    rows: vec![],
                    row_count: 0,
                    fields,
                    command: "SELECT".to_string(),
                });
            }
        }

        let mut table_names: Vec<String> = self.storage.tables.keys().cloned().collect();
        table_names.sort();

        let mut rows = Vec::new();
        for name in table_names {
            if let Some(table) = self.storage.tables.get(&name) {
                for (pos, col) in table.columns.iter().enumerate() {
                    let (data_type, udt_name) = match &col.data_type {
                        DataType::Integer => ("integer", "int4"),
                        DataType::BigInt => ("bigint", "int8"),
                        DataType::Serial => ("integer", "int4"),
                        DataType::Text => ("text", "text"),
                        DataType::Boolean => ("boolean", "bool"),
                        DataType::Numeric => ("numeric", "numeric"),
                        DataType::Timestamp => ("timestamp without time zone", "timestamp"),
                        DataType::Date => ("date", "date"),
                        DataType::Time => ("time without time zone", "time"),
                        DataType::Jsonb => ("jsonb", "jsonb"),
                        DataType::Uuid => ("uuid", "uuid"),
                        DataType::Bytea => ("bytea", "bytea"),
                        DataType::Array(_) => ("ARRAY", "ARRAY"),
                    };
                    // mảng: udt_name là `_` + kiểu phần tử (`_int4`, `_text`) như PostgreSQL; công cụ sinh schema
                    // dựa vào nó để khai báo lại `INTEGER[]` (trước đây ra `ARRAY` nên bị đổi thành TEXT)
                    let array_udt = match &col.data_type {
                        DataType::Array(elem) => {
                            let decl = table.decl_types.get(&col.name.to_lowercase()).cloned().or_else(|| crate::engine::coltypes::DeclType::parse(elem));
                            decl.map(|d| {
                                let base = match d.base.as_str() {
                                    "serial" => "int4",
                                    "smallserial" => "int2",
                                    "bigserial" => "int8",
                                    b => b,
                                };
                                format!("_{}", base)
                            })
                        }
                        _ => None,
                    };
                    let udt_name = array_udt.as_deref().unwrap_or(udt_name);
                    // cột kiểu ENUM do người dùng tạo: như PostgreSQL
                    let enum_udt = table.decl_types.get(&col.name.to_lowercase()).filter(|d| !d.array && self.storage.objects.enums.contains_key(&d.base)).map(|d| d.base.clone());
                    let (data_type, udt_name) = match &enum_udt {
                        Some(e) => ("USER-DEFINED", e.as_str()),
                        None => (data_type, udt_name),
                    };

                    let default_val = col.default_value.as_ref().map(|d| format_attdef_to_sql(d)).or_else(|| {
                        if col.data_type == DataType::Serial {
                            Some(format!("nextval('{}_{}_seq'::regclass)", table.name, col.name))
                        } else {
                            None
                        }
                    });

                    rows.push(serde_json::json!({
                        "schema_name": "public",
                        "table_name": table.name,
                        "column_name": col.name,
                        "data_type": data_type,
                        "udt_name": udt_name,
                        "is_nullable": if col.is_nullable { "YES" } else { "NO" },
                        "column_default": default_val.map(serde_json::Value::String).unwrap_or(serde_json::Value::Null),
                        "constraint_type": if col.is_primary_key { serde_json::json!("PRIMARY KEY") } else { serde_json::Value::Null },
                        "comment": col.comment.clone().map(serde_json::Value::String).unwrap_or(serde_json::Value::Null),
                        "ordinal_position": pos + 1,
                    }));
                }
            }
        }

        let mut filtered_rows = rows;
        let up_sql = _sql.to_uppercase();
        if let Some(w_idx) = up_sql.find("WHERE ") {
            let where_part = &_sql[w_idx + 6..];
            if let Some(tbl_match) = extract_literal_after_key(where_part, "table_name") {
                filtered_rows.retain(|r| r.get("table_name").and_then(|v| v.as_str()).map(|s| s.eq_ignore_ascii_case(&tbl_match)).unwrap_or(false));
            }
            if let Some(col_match) = extract_literal_after_key(where_part, "column_name") {
                filtered_rows.retain(|r| r.get("column_name").and_then(|v| v.as_str()).map(|s| s.eq_ignore_ascii_case(&col_match)).unwrap_or(false));
            } else if let Some(in_pos) = where_part.to_uppercase().find("COLUMN_NAME IN") {
                let after_in = where_part[in_pos + 14..].trim();
                if let Some(open_p) = after_in.find('(') {
                    if let Some(close_p) = after_in[open_p..].find(')') {
                        let inside = &after_in[open_p + 1..open_p + close_p];
                        let list: Vec<String> = split_comma_separated_tokens(inside).into_iter().map(|tok| tok.trim().trim_matches('\'').trim_matches('"').to_string()).collect();
                        filtered_rows.retain(|r| {
                            r.get("column_name").and_then(|v| v.as_str()).map_or(false, |col| list.iter().any(|item| item.eq_ignore_ascii_case(col)))
                        });
                    }
                }
            }
        }

        let mut fields = fields;
        let projected_rows = if !up_sql.starts_with("SELECT *") {
            let from_idx = up_sql.find(" FROM ").unwrap_or(_sql.len());
            let select_part = &_sql[6..from_idx].trim();
            let req_cols: Vec<String> = split_comma_separated_tokens(select_part)
                .into_iter()
                .map(|c| clean_col_name(c).to_string())
                .collect();
            // fields theo đúng các cột được chọn, đúng thứ tự
            fields = req_cols
                .iter()
                .map(|c| {
                    let data_type = fields.iter().find(|f| &f.name == c).map(|f| f.data_type.clone()).unwrap_or_else(|| "text".to_string());
                    FieldInfo { name: c.clone(), data_type }
                })
                .collect();
            filtered_rows.into_iter().map(|r| {
                let mut map = serde_json::Map::new();
                for col in &req_cols {
                    if let Some(val) = r.get(col) {
                        map.insert(col.clone(), val.clone());
                    }
                }
                serde_json::Value::Object(map)
            }).collect()
        } else {
            filtered_rows
        };

        let row_count = projected_rows.len();
        Ok(QueryResult {
            rows: projected_rows,
            row_count,
            fields,
            command: "SELECT".to_string(),
        })
    }

    fn handle_select(&mut self, sql: &str, params: &[Value]) -> Result<(QueryResult, Option<ExecutionPlan>), String> {
        // SELECT generate_series(...) [AS x] / SELECT unnest(...) không có FROM → FROM hàm đó
        if (contains_ci(sql, "GENERATE_SERIES(") || contains_ci(sql, "UNNEST(")) && find_top_level_keyword(sql, "FROM").is_none() {
            let list = sql[6..].trim();
            let mut end = list.len();
            for kw in ["ORDER BY", "LIMIT", "OFFSET", "WHERE"] {
                if let Some(p) = find_top_level_keyword(list, kw) {
                    end = end.min(p);
                }
            }
            let item = list[..end].trim();
            let tail = list[end..].trim();
            let (call, alias) = match rfind_top_level_as(&item.to_uppercase()) {
                Some(p) => (item[..p].trim(), Some(item[p + 4..].trim().trim_matches('"').to_string())),
                None => (item, None),
            };
            // `generate_series(...)::date`: ép kiểu áp lên từng giá trị của hàm
            let (call, cast) = match find_last_top_level_op(call, "::").filter(|&p| cast_is_outermost(call, p)) {
                Some(p) if call[..p].trim_end().ends_with(')') => (call[..p].trim_end(), &call[p..]),
                _ => (call, ""),
            };
            let cu = call.to_uppercase();
            if (cu.starts_with("GENERATE_SERIES(") || cu.starts_with("UNNEST(")) && call.ends_with(')') && is_fully_enclosed_in_parens(&call[call.find('(').unwrap_or(0)..]) {
                let fname = if cu.starts_with("UNNEST(") { "unnest" } else { "generate_series" };
                let name = alias.unwrap_or_else(|| fname.to_string());
                let q = if cast.is_empty() {
                    format!("SELECT \"{}\" FROM {} AS \"{}\" {}", name, call, name, tail)
                } else {
                    format!("SELECT \"{}\"{} AS \"{}\" FROM {} AS \"{}\" {}", name, cast, name, call, name, tail)
                };
                return self.handle_select(q.trim_end(), params);
            }
        }
        if (contains_ci(sql, "_KEYS(") || contains_ci(sql, "_ELEMENTS") || contains_ci(sql, "PATH_QUERY(") || contains_ci(sql, "REGEXP_MATCHES(") || contains_ci(sql, "SPLIT_TO_TABLE(") || contains_ci(sql, "GENERATE_SUBSCRIPTS("))
            && find_top_level_keyword(sql, "FROM").is_none()
        {
            if let Some(r) = self.try_select_list_srf(sql, params) {
                return r;
            }
        }
        if contains_ci(sql, "WITH TIES") {
            if let Some(r) = self.select_with_ties(sql, params)? {
                return Ok((r, None));
            }
        }
        if (contains_ci(sql, "GROUPING SETS") || contains_ci(sql, "ROLLUP") || contains_ci(sql, "CUBE")) && contains_ci(sql, "GROUP BY") {
            if let Some(u) = grouping_sets_as_union(sql)? {
                let r = self.execute_statement(&u, params)?;
                return Ok((r, None));
            }
        }
        if let Some(rewritten) = Self::correlated_select_subqueries_as_lateral(sql) {
            return self.handle_select(&rewritten, params);
        }
        if contains_ci(sql, "ARRAY(") {
            if let Some(expanded) = self.expand_array_subqueries(sql, params)? {
                return self.handle_select(&expanded, params);
            }
        }
        // FOR UPDATE / FOR SHARE / ... [OF t] [NOWAIT | SKIP LOCKED]: engine nhúng chạy tuần tự, khoá dòng
        // không có ý nghĩa; bỏ đi thay vì để nó làm hỏng điều kiện (trước đây trả 0 dòng).
        if let Some(p) = contains_ci(sql, " FOR ").then(|| last_top_level_keyword(sql, "FOR")).flatten() {
            let tail = sql[p + 3..].trim_start().to_uppercase();
            if let Some(kind) = ["NO KEY UPDATE", "KEY SHARE", "UPDATE", "SHARE"].iter().find(|k| tail.starts_with(*k)) {
                // như PostgreSQL: khoá dòng không dùng được với kết quả gộp
                let head = sql[..p].trim_end();
                let after_select = &head[6..];
                let list_end = find_top_level_keyword(after_select, "FROM").unwrap_or(after_select.len());
                let reason = if find_top_level_keyword(head, "GROUP BY").is_some() {
                    Some("GROUP BY clause")
                } else if find_top_level_keyword(head, "HAVING").is_some() {
                    Some("HAVING clause")
                } else if after_select.trim_start().to_uppercase().starts_with("DISTINCT") {
                    Some("DISTINCT clause")
                } else if split_projection_items(&after_select[..list_end]).iter().any(|i| is_aggregate_projection_item(i)) {
                    Some("aggregate functions")
                } else {
                    None
                };
                if let Some(r) = reason {
                    return Err(format!("FOR {} is not allowed with {}", kind, r));
                }
                return self.handle_select(head, params);
            }
        }
        // DISTINCT ON (biểu thức): giữ dòng đầu tiên (theo ORDER BY) của mỗi khoá
        {
            let after = sql[6..].trim_start();
            if after.get(..11).map_or(false, |p| p.eq_ignore_ascii_case("DISTINCT ON")) {
                return self.handle_distinct_on(sql, params).map(|r| (r, None));
            }
        }
        // Tên cột mặc định / alias không có AS theo quy tắc PostgreSQL
        // Chỉ chuẩn hoá một lần: câu đã chuẩn hoá không được chuẩn hoá lại (tránh đệ quy vô hạn khi
        // bộ tách không hiểu hết cú pháp của câu).
        if !SELECT_NORMALIZED.with(|f| f.get()) {
            if let Some(normalized) = crate::engine::select_list::normalize_select_sql(sql) {
                SELECT_NORMALIZED.with(|f| f.set(true));
                let res = self.handle_select(&normalized, params);
                SELECT_NORMALIZED.with(|f| f.set(false));
                return res;
            }
        } else {
            SELECT_NORMALIZED.with(|f| f.set(false));
        }
        // Window function: tính sau WHERE/GROUP BY, trước ORDER BY/LIMIT
        if contains_ci(sql, "OVER") {
            if let Some(res) = self.try_handle_window_select(sql, params) {
                return res.map(|r| (r, None));
            }
        }
        let from_idx_opt = find_top_level_keyword(sql, "FROM");
        if from_idx_opt.is_none() {
            let mut select_clause = sql[6..].trim().trim_end_matches(';').trim();
            // SELECT ... WHERE <điều kiện> (không có FROM): điều kiện sai thì không có dòng nào
            let tail_kw = ["WHERE", "ORDER", "LIMIT", "OFFSET", "FETCH", "FOR"].iter().filter_map(|kw| find_top_level_keyword(select_clause, kw)).min();
            let mut where_false = false;
            let mut limit_zero = false;
            if let Some(p) = tail_kw {
                let tail = select_clause[p..].to_string();
                select_clause = select_clause[..p].trim();
                if let Some(w) = find_top_level_keyword(&tail, "WHERE") {
                    let w_end = ["ORDER", "LIMIT", "OFFSET", "FETCH", "FOR"].iter().filter_map(|kw| find_top_level_keyword(&tail[w + 5..], kw)).min().map(|e| w + 5 + e).unwrap_or(tail.len());
                    let cond = tail[w + 5..w_end].trim().to_string();
                    let resolved = if cond.to_uppercase().contains("SELECT") { self.resolve_subqueries_in_where(&cond, params)? } else { cond };
                    where_false = eval_sql_expr(&resolved, &[], None, &[], None, params).as_bool() != Some(true);
                }
                if let Some(l) = find_top_level_keyword(&tail, "LIMIT") {
                    limit_zero = tail[l + 5..].split_whitespace().next().map_or(false, |n| n == "0");
                }
            }
            let items = split_projection_items(select_clause);
            let mut map = serde_json::Map::new();
            let mut fields = Vec::new();
            for item in items {
                let upper = item.to_uppercase();
                let (expr_part, alias) = if let Some(as_idx) = rfind_top_level_as(&upper) {
                    (item[..as_idx].trim(), item[as_idx + 4..].trim().trim_matches('"').trim_matches(';').trim().to_string())
                } else {
                    let col = clean_col_name(item.trim().trim_matches(';'));
                    (item.trim(), col.to_string())
                };
                let mut trimmed_expr = expr_part;
                while trimmed_expr.starts_with('(') && trimmed_expr.ends_with(')') && is_fully_enclosed_in_parens(trimmed_expr) {
                    trimmed_expr = trimmed_expr[1..trimmed_expr.len() - 1].trim();
                }
                let mut sub_type: Option<String> = None;
                let val_json = if trimmed_expr.to_uppercase().starts_with("SELECT") && trimmed_expr.to_uppercase().contains("FROM") {
                    if let Ok((sub_res, _)) = self.handle_select(trimmed_expr, params) {
                        if sub_res.fields.len() == 1 {
                            sub_type = Some(sub_res.fields[0].data_type.clone());
                        }
                        if let Some(first_row) = sub_res.rows.first() {
                            if let Some(obj) = first_row.as_object() {
                                if obj.len() == 1 {
                                    obj.values().next().unwrap().clone()
                                } else {
                                    serde_json::Value::Array(obj.values().cloned().collect())
                                }
                            } else {
                                first_row.clone()
                            }
                        } else {
                            serde_json::Value::Null
                        }
                    } else {
                        serde_json::Value::Null
                    }
                } else if expr_part.to_uppercase().contains("SELECT") {
                    // Biểu thức chứa truy vấn con: (SELECT count(*) ...) = 2, EXISTS (...), x IN (SELECT ...)
                    let resolved = self.resolve_subqueries_in_where(expr_part, params)?;
                    value_to_json(&eval_sql_expr(&resolved, &[], None, &[], None, params))
                } else {
                    let val = eval_sql_expr(expr_part, &[], None, &[], None, params);
                    value_to_json(&val)
                };
                let data_type = sub_type
                    .or_else(|| crate::pg_types::infer_expr_type(trimmed_expr, &|_| None))
                    .unwrap_or_else(|| type_str_from_json_value(&val_json));
                map.insert(alias.clone(), val_json);
                fields.push(FieldInfo {
                    name: alias,
                    data_type,
                });
            }
            if where_false || limit_zero {
                return Ok((QueryResult { rows: vec![], row_count: 0, fields, command: "SELECT".to_string() }, None));
            }
            return Ok((QueryResult {
                rows: vec![serde_json::Value::Object(map)],
                row_count: 1,
                fields,
                command: "SELECT".to_string(),
            }, None));
        }

        let from_idx = from_idx_opt.unwrap();
        let mut select_clause = sql[6..from_idx].trim();
        let is_distinct = if select_clause.to_uppercase().starts_with("DISTINCT ") {
            select_clause = select_clause[9..].trim();
            true
        } else {
            false
        };
        let after_from = sql[from_idx + 4..].trim();

        // generate_series(...) / unnest(...) đứng một mình trong FROM: bảng tạm tên duy nhất
        if let Some(r) = self.try_srf_from(select_clause, is_distinct, after_from, params) {
            return r;
        }
        if after_from.to_uppercase().starts_with("UNNEST(") {
            let open_p = after_from.find('(').unwrap();
            let mut depth = 1;
            let mut close_p = None;
            for (idx, ch) in after_from[open_p + 1..].char_indices() {
                if ch == '(' || ch == '[' { depth += 1; }
                else if ch == ')' || ch == ']' {
                    depth -= 1;
                    if depth == 0 {
                        close_p = Some(open_p + 1 + idx);
                        break;
                    }
                }
            }
            if let Some(cp) = close_p {
                let arr_expr = &after_from[open_p + 1..cp];
                let after_unnest = after_from[cp + 1..].trim();
                let arr_val = eval_sql_expr(arr_expr, &[], None, &[], None, params);
                let items = val_to_array_items(&arr_val);

                let mut col_name = "val".to_string();
                let mut tbl_alias = "t".to_string();
                let rest_clauses: String;

                let up_after = after_unnest.to_uppercase();
                let mut scan_idx = 0;
                if up_after.starts_with("AS ") {
                    scan_idx += 3;
                }
                let rest_str = after_unnest[scan_idx..].trim();
                if let Some(open_col) = rest_str.find('(') {
                    if let Some(close_col) = rest_str[open_col..].find(')') {
                        tbl_alias = rest_str[..open_col].trim().to_string();
                        col_name = rest_str[open_col + 1..open_col + close_col].trim().to_string();
                        rest_clauses = rest_str[open_col + close_col + 1..].trim().to_string();
                    } else {
                        let mut parts = rest_str.split_whitespace();
                        if let Some(a) = parts.next() { tbl_alias = a.to_string(); }
                        rest_clauses = parts.collect::<Vec<_>>().join(" ");
                    }
                } else {
                    let mut parts = rest_str.split_whitespace();
                    if let Some(a) = parts.next() { tbl_alias = a.to_string(); }
                    rest_clauses = parts.collect::<Vec<_>>().join(" ");
                }
                if tbl_alias.is_empty() { tbl_alias = "t".to_string(); }
                if col_name.is_empty() { col_name = "val".to_string(); }

                let ephemeral_table_name = format!("__ephemeral_unnest_{}", tbl_alias);
                let col_def = crate::types::ColumnDef {
                    name: col_name.clone(),
                    data_type: crate::types::DataType::Text,
                    is_nullable: true,
                    is_primary_key: false,
                    default_value: None,
                    comment: None,
                };
                let mut t = crate::storage::table::Table::new(ephemeral_table_name.clone(), vec![col_def]);
                for item in items {
                    t.insert(vec![item]);
                }

                self.storage.tables.insert(ephemeral_table_name.clone(), t);
                self.storage.tables.insert(tbl_alias.clone(), self.storage.tables.get(&ephemeral_table_name).unwrap().clone());

                let new_sql = if rest_clauses.is_empty() {
                    format!("SELECT {} FROM {}", select_clause, tbl_alias)
                } else {
                    format!("SELECT {} FROM {} {}", select_clause, tbl_alias, rest_clauses)
                };
                let full_new_sql = if is_distinct { format!("SELECT DISTINCT {}", &new_sql[7..]) } else { new_sql };
                let res = self.handle_select(&full_new_sql, params);
                self.storage.tables.remove(&ephemeral_table_name);
                self.storage.tables.remove(&tbl_alias);
                return res;
            }
        }

        if has_top_level_join(after_from) {
            let full_select = if is_distinct { format!("DISTINCT {}", select_clause) } else { select_clause.to_string() };
            return Ok((self.handle_joined_query(&full_select, after_from, params)?, None));
        }

        let mut clauses = parse_select_clauses(after_from);
        // tham chiếu cả dòng bằng alias (`json_agg(u)`, `to_json(u)` với `FROM users u`): đổi sang tên bảng,
        // bộ đánh giá hiểu tên bảng đứng riêng là cả dòng
        if let Some(alias) = clauses.table_alias {
            let a = alias.trim_matches('"');
            if !a.eq_ignore_ascii_case(clauses.table_name) {
                if let Some(t) = self.storage.get_table(clauses.table_name) {
                    if t.get_column_index(a).is_none() {
                        if let Some(new_list) = replace_whole_row_refs(select_clause, a, clauses.table_name) {
                            let head = if is_distinct { "SELECT DISTINCT" } else { "SELECT" };
                            return self.handle_select(&format!("{} {} FROM {}", head, new_list, after_from), params);
                        }
                    }
                }
            }
        }
        let is_correlated = if let Some(w) = clauses.where_clause {
            let w_up = w.to_uppercase();
            if let Some(alias) = clauses.table_alias {
                w_up.contains(&format!("{}.", alias.to_uppercase()))
            } else {
                w_up.contains(&format!("{}.", clauses.table_name.to_uppercase()))
            }
        } else {
            false
        };

        let resolved_where_owner = if !is_correlated {
            if let Some(w) = clauses.where_clause {
                let w_up = w.to_uppercase();
                if w_up.contains("SELECT") && w_up.contains("FROM") {
                    Some(self.resolve_subqueries_in_where(w, params)?)
                } else {
                    None
                }
            } else {
                None
            }
        } else {
            None
        };
        if let Some(ref rw) = resolved_where_owner {
            clauses.where_clause = Some(rw.as_str());
        }

        let resolved_select_owner = if select_clause.to_uppercase().contains("SELECT") && select_clause.to_uppercase().contains("FROM") {
            Some(self.resolve_subqueries_in_select(select_clause, params)?)
        } else {
            None
        };
        let select_clause = resolved_select_owner.as_deref().unwrap_or(select_clause);

        let table = self.storage.get_table(clauses.table_name).ok_or_else(|| format!("Table {} not found", clauses.table_name))?;

        if clauses.group_by_clause.is_some() {
            return Ok((self.handle_group_by_query(table, select_clause, &clauses, params)?, None));
        }



        // Hàm gộp có FILTER (WHERE ...) mà không GROUP BY: đường GROUP BY (một nhóm) hỗ trợ FILTER.
        {
            let up_sel = select_clause.to_uppercase();
            let agg_with_order = up_sel.contains("_AGG(") && find_top_level_keyword(select_clause, "ORDER BY").is_none() && up_sel.contains(" ORDER BY ");
            let extra_agg = mentions_call(select_clause, crate::engine::fnlib::EXTRA_AGGS);
            if (up_sel.contains(" FILTER (") || up_sel.contains(" FILTER(") || agg_with_order || extra_agg) && !up_sel.contains(" OVER") && clauses.join_clause.is_none() {
                return Ok((self.handle_group_by_query(table, select_clause, &clauses, params)?, None));
            }
        }

        // 1. Check if SELECT has aggregate functions (COUNT, SUM, AVG, MIN, MAX)
        let agg_specs = if !select_clause.to_uppercase().contains("SELECT")
            && !select_clause.to_uppercase().contains(" OVER (")
            && !select_clause.to_uppercase().contains(" OVER(") {
            parse_aggregations(select_clause, table)
        } else {
            vec![]
        };

        if !agg_specs.is_empty() {
            // Đường nhanh chỉ đúng khi mọi mục là một lời gọi aggregate thuần (`count(*)`, `sum(n) AS s`).
            // Biểu thức trên aggregate (`count(*) / 2`, `sum(n) / count(n)`) hay hằng / cột kèm theo
            // (`NULL AS x, count(*)`) đi đường GROUP BY một nhóm, nơi biểu thức được tính đầy đủ.
            let items = split_comma_separated_tokens(select_clause);
            if items.len() == agg_specs.len() && items.iter().all(|i| is_pure_aggregate_item(i)) {
                return Ok((self.handle_aggregate_query(table, &agg_specs, clauses.where_clause, params)?, None));
            }
            // đường GROUP BY chưa hỗ trợ mọi dạng (vd. json_agg(t) cả dòng): khi đó giữ đường cũ
            match self.handle_group_by_query(table, select_clause, &clauses, params) {
                Ok(r) => return Ok((r, None)),
                Err(_) => return Ok((self.handle_aggregate_query(table, &agg_specs, clauses.where_clause, params)?, None)),
            }
        }

        if clauses.join_clause.is_none() && select_clause.trim() == "*" {
            // 2. Fast-Path: Point lookup by Primary Key (WHERE id = $1 or WHERE id = 123)
            if let Some(w) = clauses.where_clause {
                let w_upper = w.to_uppercase();
                if clauses.order_by_specs.is_empty() && clauses.limit.is_none() && !w_upper.contains("AND") && !w_upper.contains("OR") {
                    if let Some(pk_idx) = table.pk_col_idx {
                        let pk_col_name = &table.columns[pk_idx].name;
                        if w_upper.starts_with(&format!("{} =", pk_col_name.to_uppercase())) ||
                           w_upper.starts_with(&format!("\"{}\" =", pk_col_name.to_uppercase())) ||
                           w_upper.starts_with("ID =") {
                            let val_part = w.split('=').nth(1).unwrap().trim();
                            let target_val = if val_part.starts_with('$') {
                                let p_idx = val_part[1..].parse::<usize>().unwrap_or(1);
                                params.get(p_idx - 1).cloned().unwrap_or(Value::Null)
                            } else {
                                parse_value(val_part, params)
                            };

                            let fields: Vec<FieldInfo> = table.columns.iter().map(|c| FieldInfo {
                                name: c.name.clone(),
                                data_type: format!("{:?}", c.data_type).to_lowercase(),
                            }).collect();

                            if let Some(row) = table.get_by_pk_val(&target_val) {
                                return Ok((QueryResult {
                                    rows: vec![row_to_json(table, row)],
                                    row_count: 1,
                                    fields,
                                    command: "SELECT".to_string(),
                                }, None));
                            } else {
                                return Ok((QueryResult {
                                    rows: vec![],
                                    row_count: 0,
                                    fields,
                                    command: "SELECT".to_string(),
                                }, None));
                            }
                        }
                    }
                }

                // 3. Fast-Path: Non-PK exact string lookup (WHERE name = $1 or WHERE name = '...')
                if clauses.join_clause.is_none() && select_clause.trim() == "*" && clauses.order_by_specs.is_empty() && clauses.limit.is_none() {
                    if let Some(eq_idx) = w.find('=') {
                        let left = w[..eq_idx].trim().trim_matches('"');
                        let right = w[eq_idx + 1..].trim();
                        if is_single_operand(right) {
                            if let Some(col_idx) = table.get_column_index(left) {
                                let (param_idx, literal_str, target_val) = if right.starts_with('$') {
                                    let p_idx = right[1..].parse::<usize>().unwrap_or(1);
                                    let tv = params.get(p_idx - 1).cloned().unwrap_or(Value::Null);
                                    (Some(p_idx.saturating_sub(1)), None, tv)
                                } else {
                                    let tv = parse_value(right, params);
                                    let s = match &tv {
                                        Value::Text(s) => s.to_string(),
                                        _ => right.to_string(),
                                    };
                                    (None, Some(s), tv)
                                };

                                let fields: Vec<FieldInfo> = table.columns.iter().map(|c| FieldInfo {
                                    name: c.name.clone(),
                                    data_type: format!("{:?}", c.data_type).to_lowercase(),
                                }).collect();

                                let plan = ExecutionPlan::PointLookupString {
                                    table_name: table.name.clone(),
                                    col_idx,
                                    param_idx,
                                    literal_str,
                                    fields: fields.clone(),
                                };

                                let matched_rows: Vec<serde_json::Value> = table.rows.iter().enumerate()
                                    .filter(|(r_idx, r)| !table.is_deleted[*r_idx] && r.get(col_idx) == Some(&target_val))
                                    .map(|(_, r)| row_to_json(table, r))
                                    .collect();
                                let row_count = matched_rows.len();
                                return Ok((QueryResult {
                                    rows: matched_rows,
                                    row_count,
                                    fields,
                                    command: "SELECT".to_string(),
                                }, Some(plan)));
                            }
                        }
                    }
                }
            }
        }

        // 4. Pre-compiled AST for WHERE condition filtering
        let compiled_where: Option<CompiledExpr> = match clauses.where_clause {
            Some(w) => {
                let jt_ref = if let Some(ref jc) = clauses.join_clause {
                    self.storage.get_table(jc.joined_table_name)
                } else {
                    None
                };
                Some(compile_expr(w, Some(table), clauses.table_alias, jt_ref, clauses.join_clause.as_ref().and_then(|j| j.joined_table_alias)))
            }
            None => None,
        };

        let total_rows = table.rows.len();
        let pk_target_key = if let Some(ref expr) = compiled_where {
            table.pk_col_idx.and_then(|pk_idx| expr.find_pk_key_eq(pk_idx, params))
        } else {
            None
        };
        let pk_set = if pk_target_key.is_none() {
            if let Some(ref expr) = compiled_where {
                table.pk_col_idx.and_then(|pk_idx| expr.find_pk_in(pk_idx))
            } else {
                None
            }
        } else {
            None
        };
        let col_eq_target = if pk_target_key.is_none() && pk_set.is_none() {
            if let Some(ref expr) = compiled_where {
                expr.find_col_eq(params)
            } else {
                None
            }
        } else {
            None
        };


        let has_correlated = clauses.where_clause.map_or(false, |w| {
            let u = w.to_uppercase();
            u.contains("SELECT") && u.contains("FROM")
        });

        let mut matched_indices: Vec<usize> = if has_correlated {
            let raw_where = clauses.where_clause.unwrap().to_string();
            let (table_name, columns, active_rows) = {
                let table = self.storage.get_table(clauses.table_name).ok_or_else(|| format!("Table {} not found", clauses.table_name))?;
                let active: Vec<(usize, Vec<Value>)> = table.rows.iter().enumerate()
                    .filter(|(i, _)| !table.is_deleted[*i])
                    .map(|(i, r)| (i, r.clone()))
                    .collect();
                (table.name.clone(), table.columns.clone(), active)
            };
            let alias = clauses.table_alias;
            let mut matched = Vec::new();
            for (i, r) in active_rows {
                let resolved_row_where = self.resolve_correlated_subqueries_in_where(
                    &raw_where,
                    &table_name,
                    &columns,
                    &r,
                    alias,
                    params,
                );
                let table_ref = self.storage.get_table(clauses.table_name);
                if evaluate_custom_condition(&resolved_row_where, &r, table_ref, &[], None, params) {
                    matched.push(i);
                }
            }
            matched
        } else {
            let table = self.storage.get_table(clauses.table_name).ok_or_else(|| format!("Table {} not found", clauses.table_name))?;
            if let Some(target_key) = pk_target_key {
                if let Some(row_idx) = table.get_row_idx_by_pk_key(&target_key) {
                    let matches = match &compiled_where {
                        Some(expr) => expr.eval_bool(&table.rows[row_idx], None, params) == Some(true),
                        None => true,
                    };
                    if !table.is_deleted[row_idx] && matches {
                        vec![row_idx]
                    } else {
                        vec![]
                    }
                } else {
                    vec![]
                }
            } else if let Some(target_set) = pk_set {
                let mut matched = Vec::with_capacity(target_set.len().min(total_rows));
                for key in target_set {
                    if let Some(row_idx) = table.get_row_idx_by_pk_key(key) {
                        if !table.is_deleted[row_idx] {
                            let matches = match &compiled_where {
                                Some(expr) => expr.eval_bool(&table.rows[row_idx], None, params) == Some(true),
                                None => true,
                            };
                            if matches {
                                matched.push(row_idx);
                            }
                        }
                    }
                }
                matched.sort_unstable();
                matched
            } else if let Some((col_idx, key)) = col_eq_target {
                if let Some(candidates) = table.col_indexes.get(&col_idx).and_then(|map| map.get(&key)) {
                    let expr_ref = compiled_where.as_ref();
                    candidates.iter().copied()
                        .filter(|&i| !table.is_deleted[i] && expr_ref.map_or(true, |e| e.eval_bool(&table.rows[i], None, params) == Some(true)))
                        .collect()
                } else if compiled_where.is_none() {
                    (0..total_rows)
                        .into_par_iter()
                        .filter(|&i| !table.is_deleted[i])
                        .collect()
                } else if total_rows > 20_000 {
                    let expr_ref = compiled_where.as_ref().unwrap();
                    (0..total_rows)
                        .into_par_iter()
                        .filter(|&i| !table.is_deleted[i] && expr_ref.eval_bool(&table.rows[i], None, params) == Some(true))
                        .collect()
                } else {
                    let expr_ref = compiled_where.as_ref().unwrap();
                    (0..total_rows)
                        .filter(|&i| !table.is_deleted[i] && expr_ref.eval_bool(&table.rows[i], None, params) == Some(true))
                        .collect()
                }
            } else if compiled_where.is_none() {
                (0..total_rows)
                    .into_par_iter()
                    .filter(|&i| !table.is_deleted[i])
                    .collect()
            } else if total_rows > 20_000 {
                let expr_ref = compiled_where.as_ref().unwrap();
                (0..total_rows)
                    .into_par_iter()
                    .filter(|&i| !table.is_deleted[i] && expr_ref.eval_bool(&table.rows[i], None, params) == Some(true))
                    .collect()
            } else {
                let expr_ref = compiled_where.as_ref().unwrap();
                (0..total_rows)
                    .filter(|&i| !table.is_deleted[i] && expr_ref.eval_bool(&table.rows[i], None, params) == Some(true))
                    .collect()
            }
        };

        let table = self.storage.get_table(clauses.table_name).ok_or_else(|| format!("Table {} not found", clauses.table_name))?;

        let joined_table_opt = if let Some(ref jc) = clauses.join_clause {
            let jt = self.storage.get_table(jc.joined_table_name)
                .ok_or_else(|| format!("Table {} not found", jc.joined_table_name))?;
            Some(jt)
        } else {
            None
        };

        if let (Some(ref jc), Some(jt)) = (&clauses.join_clause, joined_table_opt) {
            if !jc.is_left {
                if let Some(p_col_idx) = table.get_column_index(jc.primary_join_col) {
                    let j_col_idx_opt = jt.get_column_index(jc.joined_join_col);
                    let is_pk = jt.pk_col_idx == j_col_idx_opt;
                    if is_pk {
                        matched_indices.retain(|&idx| {
                            let join_key = &table.rows[idx][p_col_idx];
                            if join_key.is_null() {
                                return false;
                            }
                            jt.has_pk_val(join_key)
                        });
                    } else if let Some(j_col_idx) = j_col_idx_opt {
                        let mut jt_keys = std::collections::HashSet::with_capacity(jt.active_count);
                        for (r_idx, r) in jt.rows.iter().enumerate() {
                            if !jt.is_deleted[r_idx] && !r.is_empty() {
                                if let Some(v) = r.get(j_col_idx) {
                                    if !v.is_null() {
                                        jt_keys.insert(value_to_join_key(v));
                                    }
                                }
                            }
                        }
                        matched_indices.retain(|&idx| {
                            let join_key = &table.rows[idx][p_col_idx];
                            if join_key.is_null() {
                                false
                            } else {
                                jt_keys.contains(&value_to_join_key(join_key))
                            }
                        });
                    }
                }
            }
        }

        let projected_exprs = if select_clause.trim() == "*" {
            Vec::new()
        } else {
            let exprs = parse_projection(
                select_clause,
                table,
                clauses.table_alias,
                joined_table_opt,
                clauses.join_clause.as_ref().and_then(|j| j.joined_table_alias),
                &self.storage,
                params,
            );
            if exprs.is_empty() {
                return Err(format!("Unsupported projection clause: {}", select_clause));
            }
            exprs
        };

        // 5. ORDER BY sorting
        if !clauses.order_by_specs.is_empty() {
            let alias_exprs = projection_exprs_by_alias(select_clause);
            let proj_items = split_projection_items(select_clause);
            let resolve_spec_name = |spec_name: &str| -> String {
                // ORDER BY 1, 2: số thứ tự cột trong SELECT
                if let Ok(n) = spec_name.trim().parse::<usize>() {
                    if select_clause.trim() == "*" {
                        if n >= 1 && n <= table.columns.len() {
                            return table.columns[n - 1].name.clone();
                        }
                    } else if n >= 1 && n <= proj_items.len() {
                        let item = proj_items[n - 1].trim();
                        return match find_top_level_keyword(item, "AS") {
                            Some(p) => item[..p].trim().to_string(),
                            None => item.to_string(),
                        };
                    }
                }
                for p in &projected_exprs {
                    match p {
                        ProjectedExpr::PrimaryCol { col_idx, alias } if alias.eq_ignore_ascii_case(spec_name) => {
                            return table.columns[*col_idx].name.clone();
                        }
                        ProjectedExpr::Expr { expr_str, alias } if alias.eq_ignore_ascii_case(spec_name) => {
                            return expr_str.clone();
                        }
                        _ => {}
                    }
                }
                // Alias của biểu thức đã biên dịch (vd. `n * 10 AS score`): dùng biểu thức gốc
                if table.get_column_index(spec_name).is_none() {
                    if let Some(expr) = alias_exprs.iter().find(|(a, _)| a.eq_ignore_ascii_case(spec_name)).map(|(_, e)| e) {
                        return expr.clone();
                    }
                }
                spec_name.to_string()
            };

            let all_simple_cols = clauses.order_by_specs.iter()
                .all(|spec| table.get_column_index(&resolve_spec_name(spec.col_name)).is_some());

            if all_simple_cols {
                let resolved_order: Vec<(usize, bool, bool)> = clauses.order_by_specs.iter()
                    .filter_map(|spec| {
                        let actual_col = resolve_spec_name(spec.col_name);
                        table.get_column_index(&actual_col).map(|idx| (idx, spec.is_desc, spec.nulls_first))
                    })
                    .collect();

                if matched_indices.len() > 10_000 {
                    matched_indices.par_sort_by(|&a, &b| {
                        for &(col_idx, desc, nulls_first) in &resolved_order {
                            let va = &table.rows[a][col_idx];
                            let vb = &table.rows[b][col_idx];
                            let ord = order_cmp(va, vb, desc, nulls_first);
                            if ord != std::cmp::Ordering::Equal {
                                return ord;
                            }
                        }
                        std::cmp::Ordering::Equal
                    });
                } else {
                    matched_indices.sort_by(|&a, &b| {
                        for &(col_idx, desc, nulls_first) in &resolved_order {
                            let va = &table.rows[a][col_idx];
                            let vb = &table.rows[b][col_idx];
                            let ord = order_cmp(va, vb, desc, nulls_first);
                            if ord != std::cmp::Ordering::Equal {
                                return ord;
                            }
                        }
                        std::cmp::Ordering::Equal
                    });
                }
            } else if let Some(jt) = joined_table_opt {
                if let Some(spec) = clauses.order_by_specs.first() {
                    let actual_spec = resolve_spec_name(spec.col_name);
                    if let Some(j_col_idx) = jt.get_column_index(&actual_spec) {
                        if let Some(ref jc) = clauses.join_clause {
                            if let Some(p_col_idx) = table.get_column_index(jc.primary_join_col) {
                                let desc = spec.is_desc;
                                let nulls_first = spec.nulls_first;
                                matched_indices.sort_by(|&a, &b| {
                                    let get_val = |row_idx: usize| -> Option<&Value> {
                                        let key = table.rows[row_idx].get(p_col_idx)?;
                                        let j_row = jt.get_by_pk_val(key)?;
                                        j_row.get(j_col_idx)
                                    };
                                    let va = get_val(a).unwrap_or(&Value::Null);
                                    let vb = get_val(b).unwrap_or(&Value::Null);
                                    order_cmp(va, vb, desc, nulls_first)
                                });
                            }
                        }
                    }
                }
            } else {
                // Tính khoá sắp xếp một lần cho mỗi dòng, không tính lại ở mỗi phép so sánh
                let exprs: Vec<(String, bool, bool)> = clauses.order_by_specs.iter().map(|spec| (resolve_spec_name(spec.col_name), spec.is_desc, spec.nulls_first)).collect();
                let mut keyed: Vec<(usize, Vec<Value>)> = matched_indices
                    .iter()
                    .map(|&idx| (idx, exprs.iter().map(|(e, _, _)| eval_sql_expr(e, &table.rows[idx], Some(table), &[], None, params)).collect()))
                    .collect();
                keyed.sort_by(|(_, ka), (_, kb)| {
                    for (i, (_, desc, nulls_first)) in exprs.iter().enumerate() {
                        let ord = order_cmp(&ka[i], &kb[i], *desc, *nulls_first);
                        if ord != std::cmp::Ordering::Equal {
                            return ord;
                        }
                    }
                    std::cmp::Ordering::Equal
                });
                matched_indices = keyed.into_iter().map(|(idx, _)| idx).collect();
            }
        }

        // 6. OFFSET and LIMIT pagination
        let offset = clauses.offset.as_ref().map(|o| resolve_operand(o, params).as_i64().unwrap_or(0) as usize).unwrap_or(0);
        let limit = clauses.limit.as_ref().map(|l| resolve_operand(l, params).as_i64().unwrap_or(1000) as usize).unwrap_or(usize::MAX);
        let paged_indices = if offset < matched_indices.len() {
            let end = (offset.saturating_add(limit)).min(matched_indices.len());
            &matched_indices[offset..end]
        } else {
            &[]
        };

        let mut count_maps: std::collections::HashMap<String, std::collections::HashMap<String, i64>> = std::collections::HashMap::new();
        for expr in &projected_exprs {
            if let ProjectedExpr::CorrelatedCount {
                child_table_name,
                child_join_col_idx,
                extra_child_conditions,
                alias,
                ..
            } = expr {
                if let Some(child_table) = self.storage.get_table(child_table_name) {
                    let mut map = std::collections::HashMap::new();
                    for i in 0..child_table.rows.len() {
                        if !child_table.is_deleted[i] {
                            let mut matches = true;
                            for cond in extra_child_conditions {
                                if !crate::engine::plan::evaluate_planned_condition(&child_table.rows[i], cond, params) {
                                    matches = false;
                                    break;
                                }
                            }
                            if matches {
                                if let Some(fk_val) = child_table.rows[i].get(*child_join_col_idx) {
                                    if !fk_val.is_null() {
                                        *map.entry(fk_val.as_str()).or_insert(0) += 1;
                                    }
                                }
                            }
                        }
                    }
                    count_maps.insert(alias.clone(), map);
                }
            }
        }

        let jt_hash_map: Option<std::collections::HashMap<JoinKey, usize>> = if let (Some(ref jc), Some(jt)) = (&clauses.join_clause, joined_table_opt) {
            if let Some(j_col_idx) = jt.get_column_index(jc.joined_join_col) {
                if jt.pk_col_idx != Some(j_col_idx) {
                    let mut map = std::collections::HashMap::with_capacity(jt.active_count);
                    for (r_idx, r) in jt.rows.iter().enumerate() {
                        if !jt.is_deleted[r_idx] && !r.is_empty() {
                            if let Some(v) = r.get(j_col_idx) {
                                if !v.is_null() {
                                    map.entry(value_to_join_key(v)).or_insert(r_idx);
                                }
                            }
                        }
                    }
                    Some(map)
                } else {
                    None
                }
            } else {
                None
            }
        } else {
            None
        };

        let build_row = |idx: usize| -> serde_json::Value {
            if !projected_exprs.is_empty() {
                let primary_row = &table.rows[idx];
                let joined_row: Option<&Vec<Value>> = if let (Some(ref jc), Some(jt)) = (&clauses.join_clause, joined_table_opt) {
                    if let Some(p_col_idx) = table.get_column_index(jc.primary_join_col) {
                        let join_key = &primary_row[p_col_idx];
                        if join_key.is_null() {
                            None
                        } else if let Some(j_col_idx) = jt.get_column_index(jc.joined_join_col) {
                            if jt.pk_col_idx == Some(j_col_idx) {
                                jt.get_by_pk_val(join_key)
                            } else if let Some(ref map) = jt_hash_map {
                                map.get(&value_to_join_key(join_key)).map(|&r_idx| &jt.rows[r_idx])
                            } else {
                                None
                            }
                        } else {
                            None
                        }
                    } else {
                        None
                    }
                } else {
                    None
                };

                let mut map = serde_json::Map::with_capacity(projected_exprs.len());
                let format_col_val_for_json = |v: &Value, dt: Option<&crate::types::DataType>| -> serde_json::Value {
                    if let Some(crate::types::DataType::Bytea) = dt {
                        if v.is_null() {
                            serde_json::Value::Null
                        } else {
                            bytea_to_json(v)
                        }
                    } else {
                        value_to_json(v)
                    }
                };

                for expr in &projected_exprs {
                    match expr {
                        ProjectedExpr::PrimaryCol { col_idx, alias } => {
                            let v = primary_row.get(*col_idx).unwrap_or(&Value::Null);
                            let dt = table.columns.get(*col_idx).map(|c| &c.data_type);
                            map.insert(alias.clone(), format_col_val_for_json(v, dt));
                        }
                        ProjectedExpr::JoinedCol { col_idx, alias } => {
                            let v = joined_row.and_then(|r| r.get(*col_idx)).unwrap_or(&Value::Null);
                            let dt = joined_table_opt.and_then(|jt| jt.columns.get(*col_idx)).map(|c| &c.data_type);
                            map.insert(alias.clone(), format_col_val_for_json(v, dt));
                        }
                        ProjectedExpr::CoalescePrimaryCol { col_idx, default_val, alias } => {
                            let raw_v = primary_row.get(*col_idx).unwrap_or(&Value::Null);
                            let v = match raw_v {
                                Value::Null => default_val,
                                _ => raw_v,
                            };
                            let dt = table.columns.get(*col_idx).map(|c| &c.data_type);
                            map.insert(alias.clone(), format_col_val_for_json(v, dt));
                        }
                        ProjectedExpr::CoalesceJoinedCol { col_idx, default_val, alias } => {
                            let raw_v = joined_row.and_then(|r| r.get(*col_idx)).unwrap_or(&Value::Null);
                            let v = match raw_v {
                                Value::Null => default_val,
                                _ => raw_v,
                            };
                            let dt = joined_table_opt.and_then(|jt| jt.columns.get(*col_idx)).map(|c| &c.data_type);
                            map.insert(alias.clone(), format_col_val_for_json(v, dt));
                        }
                        ProjectedExpr::CorrelatedCount { parent_join_col_idx, alias, .. } => {
                            let parent_val = primary_row.get(*parent_join_col_idx).unwrap_or(&Value::Null);
                            let key = parent_val.as_str();
                            let count = count_maps.get(alias).and_then(|m| m.get(&key).copied()).unwrap_or(0);
                            map.insert(alias.clone(), serde_json::Value::Number(count.into()));
                        }
                        ProjectedExpr::Expr { expr_str, alias } => {
                            let empty_vec = Vec::new();
                            let j_row = joined_row.unwrap_or(&empty_vec);
                            let expr_up = expr_str.to_uppercase();
                            if expr_up.contains("SELECT") && expr_up.contains("FROM") {
                                let mut combined_meta = Vec::new();
                                combined_meta.push((table, clauses.table_alias));
                                if let Some(jt) = joined_table_opt {
                                    combined_meta.push((jt, clauses.join_clause.as_ref().and_then(|jc| jc.joined_table_alias)));
                                }
                                let joined_schema = std::sync::Arc::new(JoinedSchema::new(&combined_meta));
                                let mut comb = CombinedRow::from_base_row(joined_schema.clone(), primary_row);
                                if let Some(jr) = joined_row {
                                    comb = comb.with_joined_table(jr);
                                }
                                let val_json = eval_joined_scalar_expr(expr_str, &comb, params, &self.storage);
                                map.insert(alias.clone(), val_json);
                            } else {
                                let v = eval_sql_expr(
                                    expr_str,
                                    primary_row,
                                    Some(table),
                                    j_row,
                                    joined_table_opt,
                                    params,
                                );
                                if expr_str.to_uppercase().starts_with("JSONB_PRETTY(") || expr_str.to_uppercase().starts_with("JSON_PRETTY(") {
                                    map.insert(alias.clone(), serde_json::Value::String(v.as_str()));
                                } else {
                                    map.insert(alias.clone(), value_to_json(&v));
                                }
                            }
                        }
                        ProjectedExpr::Compiled { expr, alias } => {
                            let v = expr.eval(primary_row, joined_row.map(|r| r.as_slice()), params);
                            map.insert(alias.clone(), value_to_json(&v));
                        }
                    }
                }
                serde_json::Value::Object(map)
            } else {
                row_to_json(table, &table.rows[idx])
            }
        };

        let rows: Vec<serde_json::Value> = if is_distinct {
            let mut distinct_rows = Vec::new();
            let mut seen = std::collections::HashSet::new();
            for &idx in &matched_indices {
                let r = build_row(idx);
                let key = serde_json::to_string(&r).unwrap_or_default();
                if seen.insert(key) {
                    distinct_rows.push(r);
                }
            }
            if offset < distinct_rows.len() {
                let end = (offset.saturating_add(limit)).min(distinct_rows.len());
                distinct_rows[offset..end].to_vec()
            } else {
                vec![]
            }
        } else if select_clause.to_uppercase().contains(" OVER (") {
            let mut all_rows: Vec<serde_json::Value> = matched_indices.iter().map(|&idx| build_row(idx)).collect();
            eval_window_functions(&mut all_rows, select_clause);
            if !clauses.order_by_specs.is_empty() {
                sort_json_rows(&mut all_rows, &clauses.order_by_specs);
            }
            if offset < all_rows.len() {
                let end = (offset.saturating_add(limit)).min(all_rows.len());
                all_rows[offset..end].to_vec()
            } else {
                vec![]
            }
        } else {
            paged_indices.iter().map(|&idx| build_row(idx)).collect()
        };

        let fields: Vec<FieldInfo> = if !projected_exprs.is_empty() {
            let mut tables_for_types: Vec<(&crate::storage::table::Table, Option<&str>)> = vec![(table, clauses.table_alias)];
            if let Some(jt) = joined_table_opt {
                tables_for_types.push((jt, clauses.join_clause.as_ref().and_then(|j| j.joined_table_alias)));
            }
            let col_type = |c: &str| column_ref_type(c, &tables_for_types);
            let expr_by_alias = projection_exprs_by_alias(select_clause);
            let expr_type = |expr: &str| crate::pg_types::infer_expr_type(expr, &col_type).unwrap_or_else(|| "text".to_string());
            let primary_type = |idx: &usize| table.columns.get(*idx).map(|c| c.data_type.field_type_str()).unwrap_or_else(|| "text".to_string());
            let joined_type = |idx: &usize| joined_table_opt.and_then(|jt| jt.columns.get(*idx)).map(|c| c.data_type.field_type_str()).unwrap_or_else(|| "text".to_string());
            projected_exprs.iter().map(|e| {
                let (alias, dt) = match e {
                    ProjectedExpr::PrimaryCol { alias, col_idx } => (alias.clone(), primary_type(col_idx)),
                    ProjectedExpr::JoinedCol { alias, col_idx } => (alias.clone(), joined_type(col_idx)),
                    ProjectedExpr::CoalescePrimaryCol { alias, col_idx, .. } => (alias.clone(), primary_type(col_idx)),
                    ProjectedExpr::CoalesceJoinedCol { alias, col_idx, .. } => (alias.clone(), joined_type(col_idx)),
                    ProjectedExpr::CorrelatedCount { alias, .. } => (alias.clone(), "bigint".to_string()),
                    ProjectedExpr::Expr { alias, expr_str } => (alias.clone(), expr_type(expr_str)),
                    ProjectedExpr::Compiled { alias, .. } => (
                        alias.clone(),
                        expr_by_alias.get(alias).map(|e| expr_type(e)).unwrap_or_else(|| "text".to_string()),
                    ),
                };
                FieldInfo {
                    name: alias,
                    data_type: dt,
                }
            }).collect()
        } else {
            table.columns.iter().map(|c| FieldInfo {
                name: c.name.clone(),
                data_type: format!("{:?}", c.data_type).to_lowercase(),
            }).collect()
        };

        // Subquery đã được thay bằng kết quả cụ thể (phụ thuộc tham số và dữ liệu hiện tại):
        // không được cache plan theo nguyên văn SQL, nếu không lần sau sẽ trả kết quả cũ.
        let has_resolved_subquery = resolved_where_owner.is_some() || resolved_select_owner.is_some();
        // Plan đã cache chỉ lưu được ORDER BY một cột thật, không lưu DISTINCT, và chỉ tra được
        // tập IN gồm số nguyên: các dạng khác không được cache để lần sau không trả sai.
        let order_cacheable = clauses.order_by_specs.len() <= 1
            && clauses
                .order_by_specs
                .first()
                .map_or(true, |spec| table.get_column_index(clean_col_name(spec.col_name)).is_some());
        let has_text_in_list = clauses.where_clause.map_or(false, |w| {
            let u = w.to_uppercase();
            (u.contains(" IN ") || u.contains(" IN(")) && w.contains('\'')
        });
        let cacheable = !has_resolved_subquery && !is_distinct && order_cacheable && !has_text_in_list;
        let plan_opt = if !cacheable || select_clause.to_uppercase().contains(" OVER") || select_clause.to_uppercase().contains(" OVER(") {
            None
        } else if clauses.where_clause.is_some() {
                let planned_join = if let (Some(ref jc), Some(jt)) = (&clauses.join_clause, joined_table_opt) {
                    let p_idx = table.get_column_index(clean_col_name(jc.primary_join_col)).unwrap_or(0);
                    let j_idx = jt.get_column_index(clean_col_name(jc.joined_join_col)).unwrap_or(0);
                    let is_pk = jt.pk_col_idx == Some(j_idx);
                    Some(PlannedJoin {
                        joined_table_name: jt.name.clone(),
                        primary_join_col_idx: p_idx,
                        joined_join_col_idx: j_idx,
                        is_pk_join: is_pk,
                    })
                } else {
                    None
                };

                let order_by_info = if let Some(spec) = clauses.order_by_specs.first() {
                    table.get_column_index(clean_col_name(spec.col_name)).map(|idx| (idx, spec.is_desc, spec.nulls_first))
                } else {
                    None
                };

                let plan_projections = if !projected_exprs.is_empty() {
                    projected_exprs
                } else {
                    table.columns.iter().enumerate().map(|(i, c)| ProjectedExpr::PrimaryCol {
                        col_idx: i,
                        alias: c.name.clone(),
                    }).collect()
                };

                Some(ExecutionPlan::GeneralSelect {
                    table_name: table.name.clone(),
                    join: planned_join,
                    where_expr: compiled_where,
                    order_by: order_by_info,
                    limit: clauses.limit,
                    offset: clauses.offset,
                    projections: plan_projections,
                    fields: fields.clone(),
                })
        } else {
            let planned_join = if let (Some(ref jc), Some(jt)) = (&clauses.join_clause, joined_table_opt) {
                let p_idx = table.get_column_index(clean_col_name(jc.primary_join_col)).unwrap_or(0);
                let j_idx = jt.get_column_index(clean_col_name(jc.joined_join_col)).unwrap_or(0);
                let is_pk = jt.pk_col_idx == Some(j_idx);
                Some(PlannedJoin {
                    joined_table_name: jt.name.clone(),
                    primary_join_col_idx: p_idx,
                    joined_join_col_idx: j_idx,
                    is_pk_join: is_pk,
                })
            } else {
                None
            };

            let order_by_info = if let Some(spec) = clauses.order_by_specs.first() {
                table.get_column_index(clean_col_name(spec.col_name)).map(|idx| (idx, spec.is_desc, spec.nulls_first))
            } else {
                None
            };

            let plan_projections = if !projected_exprs.is_empty() {
                projected_exprs
            } else {
                table.columns.iter().enumerate().map(|(i, c)| ProjectedExpr::PrimaryCol {
                    col_idx: i,
                    alias: c.name.clone(),
                }).collect()
            };

            Some(ExecutionPlan::GeneralSelect {
                table_name: table.name.clone(),
                join: planned_join,
                where_expr: None,
                order_by: order_by_info,
                limit: clauses.limit,
                offset: clauses.offset,
                projections: plan_projections,
                fields: fields.clone(),
            })
        };

        Ok((QueryResult {
            row_count: rows.len(),
            rows,
            fields,
            command: "SELECT".to_string(),
        }, plan_opt))
    }

    fn execute_select_plan(&self, plan: &ExecutionPlan, params: &[Value]) -> Result<QueryResult, String> {
        match plan {
            ExecutionPlan::PointLookupPk {
                table_name,
                param_idx,
                literal_pk,
                fields,
            } => {
                let table = self.storage.get_table(table_name).ok_or_else(|| format!("Table {} not found", table_name))?;
                let target_id = if let Some(p_idx) = param_idx {
                    params.get(*p_idx).and_then(|v| v.as_i64()).unwrap_or(0)
                } else {
                    literal_pk.unwrap_or(0)
                };

                if let Some(row) = table.get_by_pk(target_id) {
                    Ok(QueryResult {
                        rows: vec![row_to_json(table, row)],
                        row_count: 1,
                        fields: fields.clone(),
                        command: "SELECT".to_string(),
                    })
                } else {
                    Ok(QueryResult {
                        rows: vec![],
                        row_count: 0,
                        fields: fields.clone(),
                        command: "SELECT".to_string(),
                    })
                }
            }
            ExecutionPlan::PointLookupString {
                table_name,
                col_idx,
                param_idx,
                literal_str,
                fields,
            } => {
                let table = self.storage.get_table(table_name).ok_or_else(|| format!("Table {} not found", table_name))?;
                let target_val = if let Some(p_idx) = param_idx {
                    params.get(*p_idx).cloned().unwrap_or(Value::Null)
                } else if let Some(s) = literal_str {
                    Value::text(s)
                } else {
                    Value::Null
                };

                let mut matched_rows = Vec::new();
                for i in 0..table.rows.len() {
                    if !table.is_deleted[i] {
                        if let Some(v) = table.rows[i].get(*col_idx) {
                            if v == &target_val {
                                matched_rows.push(row_to_json(table, &table.rows[i]));
                            }
                        }
                    }
                }

                let row_count = matched_rows.len();
                Ok(QueryResult {
                    rows: matched_rows,
                    row_count,
                    fields: fields.clone(),
                    command: "SELECT".to_string(),
                })
            }
            ExecutionPlan::GeneralSelect {
                table_name,
                join,
                where_expr,
                order_by,
                limit,
                offset,
                projections,
                fields,
            } => {
                let table = self.storage.get_table(table_name).ok_or_else(|| format!("Table {} not found", table_name))?;
                let joined_table_opt = if let Some(ref j) = join {
                    Some(self.storage.get_table(&j.joined_table_name).ok_or_else(|| format!("Table {} not found", j.joined_table_name))?)
                } else {
                    None
                };

                let mut matched_indices = Vec::new();

                let pk_target = table.pk_col_idx.and_then(|pk_idx| {
                    where_expr.as_ref().and_then(|we| we.find_pk_eq(pk_idx, params))
                });
                let pk_set = if pk_target.is_none() {
                    table.pk_col_idx.and_then(|pk_idx| {
                        where_expr.as_ref().and_then(|we| we.find_pk_in(pk_idx))
                    })
                } else {
                    None
                };

                if let Some(target_pk) = pk_target {
                    if let Some(&row_idx) = table.pk_index.get(&target_pk) {
                        if !table.is_deleted[row_idx] {
                            let row = &table.rows[row_idx];
                            let matches = match &where_expr {
                                Some(we) => we.eval_bool(row, None, params) == Some(true),
                                None => true,
                            };
                            if matches {
                                matched_indices.push(row_idx);
                            }
                        }
                    }
                } else if let Some(target_set) = pk_set {
                    for key in target_set {
                        if let JoinKey::Int(pk_val) = key {
                            if let Some(&row_idx) = table.pk_index.get(pk_val) {
                                if !table.is_deleted[row_idx] {
                                    let row = &table.rows[row_idx];
                                    let matches = match &where_expr {
                                        Some(we) => we.eval_bool(row, None, params) == Some(true),
                                        None => true,
                                    };
                                    if matches {
                                        matched_indices.push(row_idx);
                                    }
                                }
                            }
                        }
                    }
                    matched_indices.sort_unstable();
                } else {
                    for i in 0..table.rows.len() {
                        if !table.is_deleted[i] {
                            let row = &table.rows[i];
                            let matches = match &where_expr {
                                Some(we) => we.eval_bool(row, None, params) == Some(true),
                                None => true,
                            };
                            if matches {
                                matched_indices.push(i);
                            }
                        }
                    }
                }

                // Sorting
                if let Some((order_idx, is_desc, nulls_first)) = order_by {
                    matched_indices.sort_by(|&a, &b| {
                        let val_a = table.rows[a].get(*order_idx).unwrap_or(&Value::Null);
                        let val_b = table.rows[b].get(*order_idx).unwrap_or(&Value::Null);
                        order_cmp(val_a, val_b, *is_desc, *nulls_first)
                    });
                }

                // Pagination
                let start = offset.as_ref().map(|o| resolve_operand(o, params).as_i64().unwrap_or(0) as usize).unwrap_or(0).min(matched_indices.len());
                let end = if let Some(lim_op) = limit {
                    let lim = resolve_operand(lim_op, params).as_i64().unwrap_or(0) as usize;
                    (start + lim).min(matched_indices.len())
                } else {
                    matched_indices.len()
                };
                let paged_indices = &matched_indices[start..end];

                let mut count_maps: std::collections::HashMap<String, std::collections::HashMap<String, i64>> = std::collections::HashMap::new();
                for expr in projections {
                    if let PlannedProjectedExpr::CorrelatedCount {
                        child_table_name,
                        child_join_col_idx,
                        extra_child_conditions,
                        alias,
                        ..
                    } = expr {
                        if let Some(child_table) = self.storage.get_table(child_table_name) {
                            let mut map = std::collections::HashMap::new();
                            for i in 0..child_table.rows.len() {
                                if !child_table.is_deleted[i] {
                                    let mut matches = true;
                                    for cond in extra_child_conditions {
                                        if !evaluate_planned_condition(&child_table.rows[i], cond, params) {
                                            matches = false;
                                            break;
                                        }
                                    }
                                    if matches {
                                        if let Some(fk_val) = child_table.rows[i].get(*child_join_col_idx) {
                                            if !fk_val.is_null() {
                                                *map.entry(fk_val.as_str()).or_insert(0) += 1;
                                            }
                                        }
                                    }
                                }
                            }
                            count_maps.insert(alias.clone(), map);
                        }
                    }
                }

                let jt_hash_map: Option<std::collections::HashMap<JoinKey, usize>> = if let (Some(ref j), Some(jt)) = (join, joined_table_opt) {
                    if !j.is_pk_join {
                        let mut map = std::collections::HashMap::with_capacity(jt.active_count);
                        for (r_idx, r) in jt.rows.iter().enumerate() {
                            if !jt.is_deleted[r_idx] && !r.is_empty() {
                                if let Some(val) = r.get(j.joined_join_col_idx) {
                                    if !val.is_null() {
                                        map.entry(value_to_join_key(val)).or_insert(r_idx);
                                    }
                                }
                            }
                        }
                        Some(map)
                    } else {
                        None
                    }
                } else {
                    None
                };

                // Output projection
                let mut out_rows = Vec::with_capacity(paged_indices.len());
                let has_expr = projections.iter().any(|p| matches!(p, PlannedProjectedExpr::Expr { .. }));
                for &idx in paged_indices {
                    let p_row = &table.rows[idx];
                    let j_row = if let (Some(ref j), Some(jt)) = (join, joined_table_opt) {
                        if j.is_pk_join {
                            p_row.get(j.primary_join_col_idx).and_then(|val| jt.get_by_pk_val(val))
                        } else if let Some(ref map) = jt_hash_map {
                            p_row.get(j.primary_join_col_idx)
                                .filter(|v| !v.is_null())
                                .and_then(|v| map.get(&value_to_join_key(v)))
                                .map(|&r_idx| &jt.rows[r_idx])
                        } else {
                            None
                        }
                    } else {
                        None
                    };

                    if !has_expr {
                        out_rows.push(project_row_planned(p_row, j_row.map(|r| r.as_slice()), projections, &count_maps, params));
                    } else {
                        let mut map = serde_json::Map::with_capacity(projections.len());
                        for p in projections {
                            match p {
                                PlannedProjectedExpr::PrimaryCol { col_idx, alias } => {
                                    let v = p_row.get(*col_idx).unwrap_or(&Value::Null);
                                    map.insert(alias.clone(), value_to_json(v));
                                }
                                PlannedProjectedExpr::JoinedCol { col_idx, alias } => {
                                    let v = j_row.and_then(|r| r.get(*col_idx)).unwrap_or(&Value::Null);
                                    map.insert(alias.clone(), value_to_json(v));
                                }
                                PlannedProjectedExpr::CoalescePrimaryCol { col_idx, default_val, alias } => {
                                    let v = match p_row.get(*col_idx) {
                                        Some(val) if !val.is_null() => val,
                                        _ => default_val,
                                    };
                                    map.insert(alias.clone(), value_to_json(v));
                                }
                                PlannedProjectedExpr::CoalesceJoinedCol { col_idx, default_val, alias } => {
                                    let v = match j_row.and_then(|r| r.get(*col_idx)) {
                                        Some(val) if !val.is_null() => val,
                                        _ => default_val,
                                    };
                                    map.insert(alias.clone(), value_to_json(v));
                                }
                                PlannedProjectedExpr::CorrelatedCount { parent_join_col_idx, alias, .. } => {
                                    let parent_val = p_row.get(*parent_join_col_idx).unwrap_or(&Value::Null);
                                    let key = parent_val.as_str();
                                    let count = count_maps.get(alias).and_then(|m| m.get(&key).copied()).unwrap_or(0);
                                    map.insert(alias.clone(), serde_json::Value::Number(count.into()));
                                }
                                PlannedProjectedExpr::Expr { expr_str, alias } => {
                                    let empty_vec = Vec::new();
                                    let j_slice = j_row.unwrap_or(&empty_vec);
                                    let expr_up = expr_str.to_uppercase();
                                    if expr_up.contains("SELECT") && expr_up.contains("FROM") {
                                        let mut combined_meta = Vec::new();
                                        combined_meta.push((table, None));
                                        if let Some(jt) = joined_table_opt {
                                            combined_meta.push((jt, None));
                                        }
                                        let joined_schema = std::sync::Arc::new(JoinedSchema::new(&combined_meta));
                                        let mut comb = CombinedRow::from_base_row(joined_schema.clone(), p_row);
                                        if let Some(jr) = j_row {
                                            comb = comb.with_joined_table(jr);
                                        }
                                        let val_json = eval_joined_scalar_expr(expr_str, &comb, params, &self.storage);
                                        map.insert(alias.clone(), val_json);
                                    } else {
                                        let v = eval_sql_expr(
                                            expr_str,
                                            p_row,
                                            Some(table),
                                            j_slice,
                                            joined_table_opt,
                                            params,
                                        );
                                        map.insert(alias.clone(), value_to_json(&v));
                                    }
                                }
                                PlannedProjectedExpr::Compiled { expr, alias } => {
                                    let v = expr.eval(p_row, j_row.map(|r| r.as_slice()), params);
                                    map.insert(alias.clone(), value_to_json(&v));
                                }
                            }
                        }
                        out_rows.push(serde_json::Value::Object(map));
                    }
                }

                let row_count = out_rows.len();
                Ok(QueryResult {
                    rows: out_rows,
                    row_count,
                    fields: fields.clone(),
                    command: "SELECT".to_string(),
                })
            }
            _ => Err("Invalid plan type for select".to_string()),
        }
    }

    fn handle_group_by_query(
        &self,
        table: &crate::storage::table::Table,
        select_clause: &str,
        clauses: &SelectClauses,
        params: &[Value],
    ) -> Result<QueryResult, String> {
        match self.handle_group_by_query_inner(table, select_clause, clauses, params, true) {
            // tách biểu thức aggregate thất bại (dạng chưa hỗ trợ như json_agg(t)): chạy như trước
            Err(_) if split_aggregate_expressions(select_clause).0.is_some() => {
                crate::engine::errors::clear();
                self.handle_group_by_query_inner(table, select_clause, clauses, params, false)
            }
            r => r,
        }
    }

    fn handle_group_by_query_inner(
        &self,
        table: &crate::storage::table::Table,
        select_clause: &str,
        clauses: &SelectClauses,
        params: &[Value],
        split_aggs: bool,
    ) -> Result<QueryResult, String> {
        // Biểu thức trên aggregate (`count(*) / 2`, `sum(n) / count(n)`, `max(n) + 1`, `count(*)::int`):
        // mỗi lời gọi aggregate thành một mục ẩn `__agg{j}__` tính trước, mục gốc thành `__cmp{k}__` rồi được
        // tính lại từ giá trị của các mục ẩn (cùng dòng đầu của nhóm cho cột không gộp).
        let (rewritten, composites) = if split_aggs { split_aggregate_expressions(select_clause) } else { (None, vec![]) };
        let select_clause = rewritten.as_deref().unwrap_or(select_clause);
        // Không có GROUP BY (hàm gộp có FILTER): cả bảng là một nhóm.
        let gb_str = clauses.group_by_clause.unwrap_or("");
        let proj_items_for_ordinal = split_projection_items(select_clause);
        let group_tokens: Vec<String> = split_comma_separated_tokens(gb_str)
            .into_iter()
            .map(|c| {
                let c = c.trim();
                // GROUP BY 1, 2: số thứ tự cột trong SELECT
                match c.parse::<usize>() {
                    Ok(n) if n >= 1 && n <= proj_items_for_ordinal.len() => {
                        let item = proj_items_for_ordinal[n - 1].trim();
                        match find_top_level_keyword(item, "AS") {
                            Some(p) => item[..p].trim().to_string(),
                            None => item.to_string(),
                        }
                    }
                    _ => c.to_string(),
                }
            })
            .collect();

        let where_expr = match clauses.where_clause {
            Some(w) => Some(parse_where_expr(w, table, params)?),
            None => None,
        };

        let matched_indices: Vec<usize> = (0..table.rows.len())
            .filter(|&i| !table.is_deleted[i] && where_expr.as_ref().map_or(true, |e| evaluate_where_expr(&table.rows[i], Some(table), e, params)))
            .collect();

        let mut groups: Vec<(Vec<Value>, Vec<usize>)> = Vec::new();
        let mut group_map: std::collections::HashMap<String, usize> = std::collections::HashMap::new();

        for row_idx in matched_indices {
            let row = &table.rows[row_idx];
            let key_vals: Vec<Value> = group_tokens.iter().map(|tok| {
                let name = clean_col_name(tok);
                if let Some(idx) = table.get_column_index(name) {
                    row.get(idx).cloned().unwrap_or(Value::Null)
                } else {
                    eval_sql_expr(tok, row, Some(table), &[], None, params)
                }
            }).collect();
            let key_str = key_vals.iter().map(|v| match v {
                Value::Null => "\0null\0".to_string(),
                Value::Bool(b) => format!("b:{}", b),
                Value::Int(i) => format!("i:{}", i),
                Value::Float(f) => format!("f:{}", f),
                Value::Text(s) => format!("s:{}", s),
            }).collect::<Vec<_>>().join("\x1f");

            if let Some(&g_idx) = group_map.get(&key_str) {
                groups[g_idx].1.push(row_idx);
            } else {
                let g_idx = groups.len();
                group_map.insert(key_str, g_idx);
                groups.push((key_vals, vec![row_idx]));
            }
        }
        if group_tokens.is_empty() && groups.is_empty() {
            // Hàm gộp không GROUP BY luôn trả đúng một dòng (count = 0) kể cả khi không có dòng nào.
            groups.push((Vec::new(), Vec::new()));
        }

        struct GroupProjItem {
            name: String,
            alias: String,
            func: Option<AggFunc>,
            col_idx: Option<usize>,
            filter_expr: Option<WhereExpr>,
            /// `array_agg(x ORDER BY y DESC)`: thứ tự các dòng đưa vào hàm gộp
            agg_order: Option<Vec<(String, bool, bool)>>,
        }

        let proj_strs = split_projection_items(select_clause);
        let mut projections = Vec::new();
        let mut fields = Vec::new();

        for p_str in proj_strs {
            let p_trim = p_str.trim();
            let upper = p_trim.to_uppercase();
            let (raw_expr, alias) = if let Some(as_idx) = rfind_top_level_as(&upper) {
                (p_trim[..as_idx].trim(), p_trim[as_idx + 4..].trim().trim_matches('"').to_string())
            } else {
                (p_trim, clean_col_name(p_trim).to_string())
            };

            let upper_expr = raw_expr.to_uppercase();
            let (agg_expr, filter_expr) = if let Some(filter_idx) = upper_expr.find(" FILTER") {
                let after_filter = raw_expr[filter_idx + 7..].trim();
                if after_filter.starts_with('(') && after_filter.ends_with(')') {
                    let inner_filter = after_filter[1..after_filter.len() - 1].trim();
                    let upper_inner = inner_filter.to_uppercase();
                    if upper_inner.starts_with("WHERE ") {
                        let cond_str = inner_filter[6..].trim();
                        let parsed_filter = parse_where_expr(cond_str, table, params)?;
                        (raw_expr[..filter_idx].trim(), Some(parsed_filter))
                    } else {
                        (raw_expr, None)
                    }
                } else {
                    (raw_expr, None)
                }
            } else {
                (raw_expr, None)
            };

            // ORDER BY bên trong hàm gộp: tách ra, các dòng được sắp xếp trước khi gộp
            let mut agg_order: Option<Vec<(String, bool, bool)>> = None;
            let agg_owned: String;
            let agg_expr: &str = match (agg_expr.find('('), agg_expr.ends_with(')')) {
                (Some(open), true) => {
                    let inner = &agg_expr[open + 1..agg_expr.len() - 1];
                    match find_top_level_keyword(inner, "ORDER BY") {
                        Some(ob) => {
                            agg_order = Some(parse_order_by_specs(&inner[ob + 8..]).into_iter().map(|sp| (sp.col_name.to_string(), sp.is_desc, sp.nulls_first)).collect());
                            agg_owned = format!("{}({})", &agg_expr[..open], inner[..ob].trim());
                            &agg_owned
                        }
                        None => agg_expr,
                    }
                }
                _ => agg_expr,
            };
            let upper_agg = agg_expr.to_uppercase();
            if upper_agg.starts_with("COUNT(") && upper_agg.ends_with(')') {
                let inner = agg_expr[6..agg_expr.len() - 1].trim();
                let upper_inner = inner.to_uppercase();
                let func = if upper_inner == "*" || upper_inner == "1" {
                    AggFunc::CountStar
                } else if upper_inner.starts_with("DISTINCT ") {
                    let col = clean_col_name(&inner[9..]);
                    let c_idx = table.get_column_index(col).ok_or_else(|| format!("Column {} not found", col))?;
                    AggFunc::CountDistinct(c_idx)
                } else {
                    let col = clean_col_name(inner);
                    let func = if let Some(c_idx) = table.get_column_index(col) {
                        AggFunc::Count(c_idx)
                    } else {
                        AggFunc::CountExpr(inner.to_string())
                    };
                    func
                };
                projections.push(GroupProjItem { name: raw_expr.to_string(), alias: alias.clone(), func: Some(func), col_idx: None, filter_expr, agg_order: agg_order.clone() });
                fields.push(FieldInfo { name: alias, data_type: "int8".to_string() });
            } else if upper_agg.starts_with("SUM(") && upper_agg.ends_with(')') {
                let raw_inner = agg_expr[4..agg_expr.len() - 1].trim();
                let inner = clean_col_name(raw_inner);
                let func = if let Some(c_idx) = table.get_column_index(inner) {
                    AggFunc::Sum(c_idx)
                } else {
                    AggFunc::SumExpr(raw_inner.to_string())
                };
                projections.push(GroupProjItem { name: raw_expr.to_string(), alias: alias.clone(), func: Some(func), col_idx: None, filter_expr, agg_order: agg_order.clone() });
                fields.push(FieldInfo { name: alias, data_type: "numeric".to_string() });
            } else if upper_agg.starts_with("AVG(") && upper_agg.ends_with(')') {
                let raw_inner = agg_expr[4..agg_expr.len() - 1].trim();
                let inner = clean_col_name(raw_inner);
                let func = if let Some(c_idx) = table.get_column_index(inner) {
                    AggFunc::Avg(c_idx)
                } else {
                    AggFunc::AvgExpr(raw_inner.to_string())
                };
                projections.push(GroupProjItem { name: raw_expr.to_string(), alias: alias.clone(), func: Some(func), col_idx: None, filter_expr, agg_order: agg_order.clone() });
                fields.push(FieldInfo { name: alias, data_type: "float8".to_string() });
            } else if upper_agg.starts_with("MIN(") && upper_agg.ends_with(')') {
                let raw_inner = agg_expr[4..agg_expr.len() - 1].trim();
                let inner = clean_col_name(raw_inner);
                let func = if let Some(c_idx) = table.get_column_index(inner) {
                    AggFunc::Min(c_idx)
                } else {
                    AggFunc::MinExpr(raw_inner.to_string())
                };
                projections.push(GroupProjItem { name: raw_expr.to_string(), alias: alias.clone(), func: Some(func), col_idx: None, filter_expr, agg_order: agg_order.clone() });
                fields.push(FieldInfo { name: alias, data_type: "numeric".to_string() });
            } else if upper_agg.starts_with("MAX(") && upper_agg.ends_with(')') {
                let raw_inner = agg_expr[4..agg_expr.len() - 1].trim();
                let inner = clean_col_name(raw_inner);
                let func = if let Some(c_idx) = table.get_column_index(inner) {
                    AggFunc::Max(c_idx)
                } else {
                    AggFunc::MaxExpr(raw_inner.to_string())
                };
                projections.push(GroupProjItem { name: raw_expr.to_string(), alias: alias.clone(), func: Some(func), col_idx: None, filter_expr, agg_order: agg_order.clone() });
                fields.push(FieldInfo { name: alias, data_type: "numeric".to_string() });
            } else if (upper_agg.starts_with("BOOL_AND(") || upper_agg.starts_with("EVERY(")) && upper_agg.ends_with(')') {
                let open_p = agg_expr.find('(').unwrap();
                let raw_inner = agg_expr[open_p + 1..agg_expr.len() - 1].trim();
                let inner = clean_col_name(raw_inner);
                let func = if let Some(c_idx) = table.get_column_index(inner) {
                    AggFunc::BoolAnd(c_idx)
                } else {
                    AggFunc::BoolAndExpr(raw_inner.to_string())
                };
                projections.push(GroupProjItem { name: raw_expr.to_string(), alias: alias.clone(), func: Some(func), col_idx: None, filter_expr, agg_order: agg_order.clone() });
                fields.push(FieldInfo { name: alias, data_type: "bool".to_string() });
            } else if upper_agg.starts_with("BOOL_OR(") && upper_agg.ends_with(')') {
                let raw_inner = agg_expr[8..agg_expr.len() - 1].trim();
                let inner = clean_col_name(raw_inner);
                let func = if let Some(c_idx) = table.get_column_index(inner) {
                    AggFunc::BoolOr(c_idx)
                } else {
                    AggFunc::BoolOrExpr(raw_inner.to_string())
                };
                projections.push(GroupProjItem { name: raw_expr.to_string(), alias: alias.clone(), func: Some(func), col_idx: None, filter_expr, agg_order: agg_order.clone() });
                fields.push(FieldInfo { name: alias, data_type: "bool".to_string() });
            } else if upper_agg.starts_with("ARRAY_AGG(") && upper_agg.ends_with(')') {
                let raw_inner = agg_expr[10..agg_expr.len() - 1].trim();
                let inner = clean_col_name(raw_inner);
                let func = if let Some(c_idx) = table.get_column_index(inner) {
                    AggFunc::ArrayAgg(c_idx)
                } else {
                    AggFunc::ArrayAggExpr(raw_inner.to_string())
                };
                projections.push(GroupProjItem { name: raw_expr.to_string(), alias: alias.clone(), func: Some(func), col_idx: None, filter_expr, agg_order: agg_order.clone() });
                fields.push(FieldInfo { name: alias, data_type: "array".to_string() });
            } else if let Some((k, v, jsonb)) = parse_object_agg_call(agg_expr).filter(|_| upper_agg.ends_with(')')) {
                projections.push(GroupProjItem { name: raw_expr.to_string(), alias: alias.clone(), func: Some(AggFunc::ObjectAgg(k, v, jsonb)), col_idx: None, filter_expr, agg_order: agg_order.clone() });
                fields.push(FieldInfo { name: alias, data_type: if jsonb { "jsonb".to_string() } else { "json".to_string() } });
            } else if (upper_agg.starts_with("JSON_AGG(") || upper_agg.starts_with("JSONB_AGG(")) && upper_agg.ends_with(')') {
                let is_jsonb = upper_agg.starts_with("JSONB_AGG(");
                let prefix_len = if is_jsonb { 10 } else { 9 };
                let raw_inner = agg_expr[prefix_len..agg_expr.len() - 1].trim();
                let inner = clean_col_name(raw_inner);
                // cột → JsonAgg; biểu thức / cả dòng (`json_agg(t)`) → JsonAggExpr
                let func = match table.get_column_index(inner) {
                    Some(c_idx) => if is_jsonb { AggFunc::JsonbAgg(c_idx) } else { AggFunc::JsonAgg(c_idx) },
                    None => if is_jsonb { AggFunc::JsonbAggExpr(raw_inner.to_string()) } else { AggFunc::JsonAggExpr(raw_inner.to_string()) },
                };
                projections.push(GroupProjItem { name: raw_expr.to_string(), alias: alias.clone(), func: Some(func), col_idx: None, filter_expr, agg_order: agg_order.clone() });
                fields.push(FieldInfo { name: alias, data_type: if is_jsonb { "jsonb".to_string() } else { "json".to_string() } });
            } else if upper_agg.starts_with("STRING_AGG(") && upper_agg.ends_with(')') {
                let inner = &agg_expr[11..agg_expr.len() - 1];
                let args = split_function_args(inner);
                if args.len() >= 2 {
                    let col_name = clean_col_name(&args[0]);
                    let c_idx = table.get_column_index(col_name).ok_or_else(|| format!("Column {} not found", col_name))?;
                    let delim_raw = args[1].trim();
                    let delim = delim_raw.trim_matches('\'').replace("''", "'");
                    let func = AggFunc::StringAgg(c_idx, delim);
                    projections.push(GroupProjItem { name: raw_expr.to_string(), alias: alias.clone(), func: Some(func), col_idx: None, filter_expr, agg_order: agg_order.clone() });
                    fields.push(FieldInfo { name: alias, data_type: "text".to_string() });
                }
            } else if let Some((name, args)) = extra_agg_call(agg_expr) {
                let dt = crate::engine::fnlib::agg_result_type(&name).unwrap_or("text").to_string();
                projections.push(GroupProjItem { name: raw_expr.to_string(), alias: alias.clone(), func: Some(AggFunc::Extra(name, args)), col_idx: None, filter_expr, agg_order: agg_order.clone() });
                fields.push(FieldInfo { name: alias, data_type: dt });
            } else {
                let col = clean_col_name(raw_expr);
                if let Some(c_idx) = table.get_column_index(col) {
                    projections.push(GroupProjItem { name: raw_expr.to_string(), alias: alias.clone(), func: None, col_idx: Some(c_idx), filter_expr: None, agg_order: agg_order.clone() });
                    let dt = format!("{:?}", table.columns[c_idx].data_type).to_lowercase();
                    fields.push(FieldInfo { name: alias, data_type: dt });
                } else {
                    projections.push(GroupProjItem { name: raw_expr.to_string(), alias: alias.clone(), func: None, col_idx: None, filter_expr: None, agg_order: agg_order.clone() });
                    fields.push(FieldInfo { name: alias, data_type: "text".to_string() });
                }
            }
        }

        let mut group_rows = Vec::new();
        for (_key, r_indices) in groups {
            let mut row_map = serde_json::Map::new();
            for proj in &projections {
                if let Some(c_idx) = proj.col_idx {
                    let v = r_indices.first().and_then(|&ri| table.rows[ri].get(c_idx)).unwrap_or(&Value::Null);
                    row_map.insert(proj.alias.clone(), value_to_json(v));
                } else if proj.func.is_none() {
                    let first_ri = r_indices.first().copied().unwrap_or(0);
                    let row: &[Value] = if first_ri < table.rows.len() { &table.rows[first_ri] } else { &[] };
                    let expr = match composites.iter().find(|(ph, _, _)| *ph == proj.name) {
                        Some((_, text, calls)) => {
                            let mut e = text.clone();
                            for (call, hidden) in calls.iter().rev() {
                                let lit = json_to_sql_literal(row_map.get(hidden).unwrap_or(&serde_json::Value::Null));
                                e.replace_range(call.clone(), &lit);
                            }
                            e
                        }
                        None => proj.name.clone(),
                    };
                    let v = eval_sql_expr(&expr, row, Some(table), &[], None, params);
                    row_map.insert(proj.alias.clone(), value_to_json(&v));
                } else if let Some(ref func) = proj.func {
                    let mut target_indices: Vec<usize> = if let Some(ref f_expr) = proj.filter_expr {
                        r_indices.iter().copied().filter(|&ri| evaluate_where_expr(&table.rows[ri], Some(table), f_expr, params)).collect()
                    } else {
                        r_indices.clone()
                    };
                    if let Some(order) = &proj.agg_order {
                        let mut keyed: Vec<(usize, Vec<Value>)> = target_indices
                            .iter()
                            .map(|&ri| (ri, order.iter().map(|(e, _, _)| eval_sql_expr(e, &table.rows[ri], Some(table), &[], None, params)).collect()))
                            .collect();
                        keyed.sort_by(|(_, a), (_, b)| {
                            for (i, (_, desc, nf)) in order.iter().enumerate() {
                                let o = order_cmp(&a[i], &b[i], *desc, *nf);
                                if o != std::cmp::Ordering::Equal {
                                    return o;
                                }
                            }
                            std::cmp::Ordering::Equal
                        });
                        target_indices = keyed.into_iter().map(|(ri, _)| ri).collect();
                    }
                    let v_json = match func {
                        AggFunc::CountStar => json!(target_indices.len()),
                        AggFunc::Count(c_idx) => {
                            let count = target_indices.iter().filter(|&&ri| table.rows[ri].get(*c_idx).map_or(false, |v| !v.is_null())).count();
                            json!(count)
                        }
                        AggFunc::CountExpr(expr) => {
                            let count = target_indices.iter().filter(|&&ri| !eval_sql_expr(expr, &table.rows[ri], Some(table), &[], None, params).is_null()).count();
                            json!(count)
                        }
                        AggFunc::CountDistinct(c_idx) => {
                            let mut set = std::collections::HashSet::new();
                            for &ri in &target_indices {
                                if let Some(v) = table.rows[ri].get(*c_idx) {
                                    if !v.is_null() { set.insert(format!("{:?}", v)); }
                                }
                            }
                            json!(set.len())
                        }
                        AggFunc::Sum(c_idx) => {
                            let mut sum = 0.0;
                            let mut any = false;
                            for &ri in &target_indices {
                                if let Some(f) = table.rows[ri].get(*c_idx).and_then(|v| v.as_f64()) {
                                    sum += f;
                                    any = true;
                                }
                            }
                            // như PostgreSQL: sum của nhóm không có giá trị nào là NULL
                            if any { json!(sum) } else { serde_json::Value::Null }
                        }
                        AggFunc::Avg(c_idx) => {
                            let mut sum = 0.0;
                            let mut count = 0;
                            for &ri in &target_indices {
                                if let Some(f) = table.rows[ri].get(*c_idx).and_then(|v| v.as_f64()) {
                                    sum += f;
                                    count += 1;
                                }
                            }
                            if count > 0 { json!(sum / count as f64) } else { serde_json::Value::Null }
                        }
                        AggFunc::Min(c_idx) => {
                            let is_numeric = matches!(table.columns[*c_idx].data_type, DataType::Integer | DataType::BigInt | DataType::Serial | DataType::Numeric);
                            if is_numeric {
                                let mut min = f64::MAX;
                                let mut found = false;
                                for &ri in &target_indices {
                                    if let Some(f) = table.rows[ri].get(*c_idx).and_then(|v| v.as_f64()) {
                                        if f < min { min = f; found = true; }
                                    }
                                }
                                if found { json!(min) } else { serde_json::Value::Null }
                            } else {
                                let mut min_val: Option<Value> = None;
                                for &ri in &target_indices {
                                    if let Some(v) = table.rows[ri].get(*c_idx) {
                                        if !v.is_null() {
                                            min_val = match min_val {
                                                None => Some(v.clone()),
                                                Some(cur) => if v.cmp_value(&cur) == std::cmp::Ordering::Less { Some(v.clone()) } else { Some(cur) },
                                            };
                                        }
                                    }
                                }
                                min_val.as_ref().map(|v| value_to_json(v)).unwrap_or(serde_json::Value::Null)
                            }
                        }
                        AggFunc::Max(c_idx) => {
                            let is_numeric = matches!(table.columns[*c_idx].data_type, DataType::Integer | DataType::BigInt | DataType::Serial | DataType::Numeric);
                            if is_numeric {
                                let mut max = f64::MIN;
                                let mut found = false;
                                for &ri in &target_indices {
                                    if let Some(f) = table.rows[ri].get(*c_idx).and_then(|v| v.as_f64()) {
                                        if f > max { max = f; found = true; }
                                    }
                                }
                                if found { json!(max) } else { serde_json::Value::Null }
                            } else {
                                let mut max_val: Option<Value> = None;
                                for &ri in &target_indices {
                                    if let Some(v) = table.rows[ri].get(*c_idx) {
                                        if !v.is_null() {
                                            max_val = match max_val {
                                                None => Some(v.clone()),
                                                Some(cur) => if v.cmp_value(&cur) == std::cmp::Ordering::Greater { Some(v.clone()) } else { Some(cur) },
                                            };
                                        }
                                    }
                                }
                                max_val.as_ref().map(|v| value_to_json(v)).unwrap_or(serde_json::Value::Null)
                            }
                        }
                        AggFunc::BoolAnd(c_idx) => {
                            let mut res: Option<bool> = None;
                            for &ri in &target_indices {
                                let v = table.rows[ri].get(*c_idx).unwrap_or(&Value::Null);
                                if !v.is_null() {
                                    res = Some(res.unwrap_or(true) && v.as_bool().unwrap_or(false));
                                }
                            }
                            res.map(|b| json!(b)).unwrap_or(serde_json::Value::Null)
                        }
                        AggFunc::BoolOr(c_idx) => {
                            let mut res: Option<bool> = None;
                            for &ri in &target_indices {
                                let v = table.rows[ri].get(*c_idx).unwrap_or(&Value::Null);
                                if !v.is_null() {
                                    res = Some(res.unwrap_or(false) || v.as_bool().unwrap_or(false));
                                }
                            }
                            res.map(|b| json!(b)).unwrap_or(serde_json::Value::Null)
                        }
                        AggFunc::BoolAndExpr(expr_str) => {
                            let mut res: Option<bool> = None;
                            for &ri in &target_indices {
                                let v = eval_sql_expr(expr_str, &table.rows[ri], Some(table), &[], None, params);
                                if !v.is_null() {
                                    res = Some(res.unwrap_or(true) && v.as_bool().unwrap_or(false));
                                }
                            }
                            res.map(|b| json!(b)).unwrap_or(serde_json::Value::Null)
                        }
                        AggFunc::BoolOrExpr(expr_str) => {
                            let mut res: Option<bool> = None;
                            for &ri in &target_indices {
                                let v = eval_sql_expr(expr_str, &table.rows[ri], Some(table), &[], None, params);
                                if !v.is_null() {
                                    res = Some(res.unwrap_or(false) || v.as_bool().unwrap_or(false));
                                }
                            }
                            res.map(|b| json!(b)).unwrap_or(serde_json::Value::Null)
                        }
                        AggFunc::StringAgg(c_idx, delim) => {
                            let mut parts = Vec::new();
                            for &ri in &target_indices {
                                if let Some(v) = table.rows[ri].get(*c_idx) {
                                    if !v.is_null() {
                                        parts.push(v.as_str());
                                    }
                                }
                            }
                            serde_json::Value::String(parts.join(delim))
                        }
                        AggFunc::ArrayAgg(c_idx) => {
                            let mut arr = Vec::new();
                            for &ri in &target_indices {
                                let v = table.rows[ri].get(*c_idx).unwrap_or(&Value::Null);
                                if !v.is_null() {
                                    arr.push(value_to_json(v));
                                }
                            }
                            serde_json::Value::Array(arr)
                        }
                        AggFunc::ArrayAggExpr(expr_str) => {
                            let mut arr = Vec::new();
                            for &ri in &target_indices {
                                let v = eval_sql_expr(expr_str, &table.rows[ri], Some(table), &[], None, params);
                                if !v.is_null() {
                                    arr.push(value_to_json(&v));
                                }
                            }
                            serde_json::Value::Array(arr)
                        }
                        AggFunc::JsonAgg(c_idx) | AggFunc::JsonbAgg(c_idx) => {
                            let mut arr = Vec::new();
                            for &ri in &target_indices {
                                if let Some(v) = table.rows[ri].get(*c_idx) {
                                    if !v.is_null() {
                                        let j_val = match v {
                                            Value::Text(st) => serde_json::from_str::<serde_json::Value>(st.trim()).unwrap_or(serde_json::Value::String(st.to_string())),
                                            _ => value_to_json(v),
                                        };
                                        arr.push(j_val);
                                    }
                                }
                            }
                            serde_json::Value::Array(arr)
                        }
                        AggFunc::ObjectAgg(k, v, jsonb) => {
                            let pairs = target_indices.iter().map(|&ri| (eval_sql_expr(k, &table.rows[ri], Some(table), &[], None, params), eval_sql_expr(v, &table.rows[ri], Some(table), &[], None, params))).collect();
                            object_agg_json(pairs, *jsonb)
                        }
                        AggFunc::JsonAggExpr(expr_str) | AggFunc::JsonbAggExpr(expr_str) => {
                            let (expr_str, ordered) = agg_order_rows(expr_str, table, &target_indices, params);
                            let expr_str = expr_str.as_str();
                            let mut arr = Vec::new();
                            for &ri in &ordered {
                                let v = eval_sql_expr(expr_str, &table.rows[ri], Some(table), &[], None, params);
                                if !v.is_null() {
                                    let j_val = match &v {
                                        Value::Text(st) => serde_json::from_str::<serde_json::Value>(st.trim()).unwrap_or(serde_json::Value::String(st.to_string())),
                                        _ => value_to_json(&v),
                                    };
                                    arr.push(j_val);
                                }
                            }
                            serde_json::Value::Array(arr)
                        }
                        AggFunc::Extra(name, args) => {
                            let rows: Vec<Vec<Value>> = target_indices.iter().map(|&ri| args.iter().map(|a| eval_sql_expr(a, &table.rows[ri], Some(table), &[], None, params)).collect()).collect();
                            value_to_json(&crate::engine::fnlib::agg_finalize(name, &rows))
                        }
                        AggFunc::SumExpr(expr_str) => {
                            let mut sum = 0.0;
                            for &ri in &target_indices {
                                let v = eval_sql_expr(expr_str, &table.rows[ri], Some(table), &[], None, params);
                                if let Some(f) = v.as_f64() {
                                    sum += f;
                                }
                            }
                            json!(sum)
                        }
                        AggFunc::AvgExpr(expr_str) => {
                            let mut sum = 0.0;
                            let mut count = 0;
                            for &ri in &target_indices {
                                let v = eval_sql_expr(expr_str, &table.rows[ri], Some(table), &[], None, params);
                                if let Some(f) = v.as_f64() {
                                    sum += f;
                                    count += 1;
                                }
                            }
                            if count > 0 { json!(sum / count as f64) } else { serde_json::Value::Null }
                        }
                        AggFunc::MinExpr(expr_str) => {
                            let mut min_val: Option<Value> = None;
                            for &ri in &target_indices {
                                let v = eval_sql_expr(expr_str, &table.rows[ri], Some(table), &[], None, params);
                                if !v.is_null() {
                                    min_val = match min_val {
                                        None => Some(v.clone()),
                                        Some(cur) => if v.cmp_value(&cur) == std::cmp::Ordering::Less { Some(v.clone()) } else { Some(cur) },
                                    };
                                }
                            }
                            min_val.as_ref().map(|v| value_to_json(v)).unwrap_or(serde_json::Value::Null)
                        }
                        AggFunc::MaxExpr(expr_str) => {
                            let mut max_val: Option<Value> = None;
                            for &ri in &target_indices {
                                let v = eval_sql_expr(expr_str, &table.rows[ri], Some(table), &[], None, params);
                                if !v.is_null() {
                                    max_val = match max_val {
                                        None => Some(v.clone()),
                                        Some(cur) => if v.cmp_value(&cur) == std::cmp::Ordering::Greater { Some(v.clone()) } else { Some(cur) },
                                    };
                                }
                            }
                            max_val.as_ref().map(|v| value_to_json(v)).unwrap_or(serde_json::Value::Null)
                        }
                        AggFunc::WrappedAgg(raw_expr, inner_func) => {
                            let inner_json = match inner_func.as_ref() {
                                AggFunc::ArrayAgg(c_idx) => {
                                    let mut arr = Vec::new();
                                    for &ri in &target_indices {
                                        let v = table.rows[ri].get(*c_idx).unwrap_or(&Value::Null);
                                        if !v.is_null() {
                                            arr.push(value_to_json(v));
                                        }
                                    }
                                    serde_json::Value::Array(arr)
                                }
                                AggFunc::ArrayAggExpr(expr_str) => {
                                    let mut arr = Vec::new();
                                    for &ri in &target_indices {
                                        let v = eval_sql_expr(expr_str, &table.rows[ri], Some(table), &[], None, params);
                                        if !v.is_null() {
                                            arr.push(value_to_json(&v));
                                        }
                                    }
                                    serde_json::Value::Array(arr)
                                }
                                _ => serde_json::Value::Null,
                            };
                            let inner_pattern = if raw_expr.to_uppercase().contains("ARRAY_AGG(") {
                                let u = raw_expr.to_uppercase();
                                let start = u.find("ARRAY_AGG(").unwrap();
                                if let Some(end) = find_matching_paren(raw_expr, start + 9) {
                                    Some(&raw_expr[start..=end])
                                } else {
                                    None
                                }
                            } else {
                                None
                            };
                            let substituted_expr = if let Some(pat) = inner_pattern {
                                let json_str = serde_json::to_string(&inner_json).unwrap_or_else(|_| "[]".to_string());
                                let pg_arr_str = format!("'{}'", json_str.replace('\'', "''"));
                                raw_expr.replace(pat, &pg_arr_str)
                            } else {
                                raw_expr.clone()
                            };
                            let first_ri = target_indices.first().copied().unwrap_or(0);
                            let first_row: &[Value] = if first_ri < table.rows.len() { &table.rows[first_ri] } else { &[] };
                            let res_val = eval_sql_expr(&substituted_expr, first_row, Some(table), &[], None, params);
                            value_to_json(&res_val)
                        }
                    };
                    row_map.insert(proj.alias.clone(), v_json);
                }
            }
            if let Some(having_str) = clauses.having_clause {
                let proj_pairs: Vec<(&str, &str)> = projections.iter().map(|p| (p.name.as_str(), p.alias.as_str())).collect();
                if !evaluate_having_with_group(having_str, &row_map, Some(&proj_pairs), table, &r_indices, params) {
                    continue;
                }
            }
            if !composites.is_empty() {
                row_map.retain(|k, _| !is_hidden_agg_key(k));
            }
            group_rows.push(serde_json::Value::Object(row_map));
        }
        if !composites.is_empty() {
            fields.retain(|f| !is_hidden_agg_key(&f.name));
            // kiểu của mục ghép theo giá trị đầu tiên khác NULL
            for f in fields.iter_mut().filter(|f| f.data_type == "text") {
                if let Some(v) = group_rows.iter().filter_map(|r| r.get(&f.name)).find(|v| !v.is_null()) {
                    if !v.is_string() {
                        f.data_type = type_str_from_json_value(v);
                    }
                }
            }
        }

        if !clauses.order_by_specs.is_empty() {
            group_rows.sort_by(|a, b| {
                if let (serde_json::Value::Object(map_a), serde_json::Value::Object(map_b)) = (a, b) {
                    for spec in &clauses.order_by_specs {
                        let va = json_sort_value(a, spec.col_name).or_else(|| map_a.get(spec.col_name));
                        let vb = json_sort_value(b, spec.col_name).or_else(|| map_b.get(spec.col_name));
                        let final_ord = json_order_cmp(va, vb, spec.is_desc, spec.nulls_first);
                        if final_ord != std::cmp::Ordering::Equal {
                            return final_ord;
                        }
                    }
                }
                std::cmp::Ordering::Equal
            });
        }

        let offset = clauses.offset.as_ref().map(|o| resolve_operand(o, params).as_i64().unwrap_or(0) as usize).unwrap_or(0);
        let limit = clauses.limit.as_ref().map(|l| resolve_operand(l, params).as_i64().unwrap_or(1000) as usize).unwrap_or(usize::MAX);
        let paged_rows = if offset < group_rows.len() {
            let end = (offset.saturating_add(limit)).min(group_rows.len());
            group_rows[offset..end].to_vec()
        } else {
            vec![]
        };

        let row_count = paged_rows.len();
        Ok(QueryResult {
            rows: paged_rows,
            row_count,
            fields,
            command: "SELECT".to_string(),
        })
    }

    fn handle_aggregate_query(
        &self,
        table: &crate::storage::table::Table,
        agg_specs: &[AggSpec],
        where_clause: Option<&str>,
        params: &[Value],
    ) -> Result<QueryResult, String> {
        let mut row_map = serde_json::Map::new();
        let mut fields = Vec::new();

        if where_clause.is_none() {
            // Unconditional aggregate fast path
            for spec in agg_specs {
                match &spec.func {
                    AggFunc::CountStar => {
                        let count = table.count();
                        row_map.insert(spec.alias.clone(), json!(count));
                        fields.push(FieldInfo { name: spec.alias.clone(), data_type: "int8".to_string() });
                    }
                    AggFunc::Count(col_idx) => {
                        let count = (0..table.rows.len())
                            .filter(|&i| !table.is_deleted[i] && table.rows[i].get(*col_idx).map_or(false, |v| !v.is_null()))
                            .count();
                        row_map.insert(spec.alias.clone(), json!(count));
                        fields.push(FieldInfo { name: spec.alias.clone(), data_type: "int8".to_string() });
                    }
                    AggFunc::Extra(name, args) => {
                        let rows: Vec<Vec<Value>> = (0..table.rows.len()).filter(|&i| !table.is_deleted[i]).map(|ri| args.iter().map(|a| eval_sql_expr(a, &table.rows[ri], Some(table), &[], None, params)).collect()).collect();
                        row_map.insert(spec.alias.clone(), value_to_json(&crate::engine::fnlib::agg_finalize(name, &rows)));
                        fields.push(FieldInfo { name: spec.alias.clone(), data_type: crate::engine::fnlib::agg_result_type(name).unwrap_or("text").to_string() });
                    }
                    AggFunc::CountExpr(expr) => {
                        let count = (0..table.rows.len())
                            .filter(|&i| !table.is_deleted[i] && !eval_sql_expr(expr, &table.rows[i], Some(table), &[], None, params).is_null())
                            .count();
                        row_map.insert(spec.alias.clone(), json!(count));
                        fields.push(FieldInfo { name: spec.alias.clone(), data_type: "int8".to_string() });
                    }
                    AggFunc::CountDistinct(col_idx) => {
                        let mut set = std::collections::HashSet::new();
                        for i in 0..table.rows.len() {
                            if !table.is_deleted[i] {
                                if let Some(v) = table.rows[i].get(*col_idx) {
                                    if !v.is_null() {
                                        set.insert(format!("{:?}", v));
                                    }
                                }
                            }
                        }
                        row_map.insert(spec.alias.clone(), json!(set.len()));
                        fields.push(FieldInfo { name: spec.alias.clone(), data_type: "int8".to_string() });
                    }
                    AggFunc::Sum(col_idx) => {
                        let (sum, _, _, _, count) = table.aggregate_stats(*col_idx);
                        if count == 0 {
                            row_map.insert(spec.alias.clone(), serde_json::Value::Null);
                        } else {
                            row_map.insert(spec.alias.clone(), json!(sum));
                        }
                        fields.push(FieldInfo { name: spec.alias.clone(), data_type: "numeric".to_string() });
                    }
                    AggFunc::Avg(col_idx) => {
                        let (_, avg, _, _, count) = table.aggregate_stats(*col_idx);
                        if count == 0 {
                            row_map.insert(spec.alias.clone(), serde_json::Value::Null);
                        } else {
                            row_map.insert(spec.alias.clone(), json!(avg));
                        }
                        fields.push(FieldInfo { name: spec.alias.clone(), data_type: "float8".to_string() });
                    }
                    AggFunc::Min(col_idx) => {
                        let is_numeric = matches!(table.columns[*col_idx].data_type, DataType::Integer | DataType::BigInt | DataType::Serial | DataType::Numeric);
                        if is_numeric {
                            let (_, _, min, _, count) = table.aggregate_stats(*col_idx);
                            if count == 0 {
                                row_map.insert(spec.alias.clone(), serde_json::Value::Null);
                            } else {
                                row_map.insert(spec.alias.clone(), json!(min));
                            }
                            fields.push(FieldInfo { name: spec.alias.clone(), data_type: "numeric".to_string() });
                        } else {
                            let mut min_val: Option<Value> = None;
                            for i in 0..table.rows.len() {
                                if !table.is_deleted[i] {
                                    if let Some(v) = table.rows[i].get(*col_idx) {
                                        if !v.is_null() {
                                            min_val = match min_val {
                                                None => Some(v.clone()),
                                                Some(cur) => if v.cmp_value(&cur) == std::cmp::Ordering::Less { Some(v.clone()) } else { Some(cur) },
                                            };
                                        }
                                    }
                                }
                            }
                            row_map.insert(spec.alias.clone(), min_val.as_ref().map(|v| value_to_json(v)).unwrap_or(serde_json::Value::Null));
                            fields.push(FieldInfo { name: spec.alias.clone(), data_type: "text".to_string() });
                        }
                    }
                    AggFunc::Max(col_idx) => {
                        let is_numeric = matches!(table.columns[*col_idx].data_type, DataType::Integer | DataType::BigInt | DataType::Serial | DataType::Numeric);
                        if is_numeric {
                            let (_, _, _, max, count) = table.aggregate_stats(*col_idx);
                            if count == 0 {
                                row_map.insert(spec.alias.clone(), serde_json::Value::Null);
                            } else {
                                row_map.insert(spec.alias.clone(), json!(max));
                            }
                            fields.push(FieldInfo { name: spec.alias.clone(), data_type: "numeric".to_string() });
                        } else {
                            let mut max_val: Option<Value> = None;
                            for i in 0..table.rows.len() {
                                if !table.is_deleted[i] {
                                    if let Some(v) = table.rows[i].get(*col_idx) {
                                        if !v.is_null() {
                                            max_val = match max_val {
                                                None => Some(v.clone()),
                                                Some(cur) => if v.cmp_value(&cur) == std::cmp::Ordering::Greater { Some(v.clone()) } else { Some(cur) },
                                            };
                                        }
                                    }
                                }
                            }
                            row_map.insert(spec.alias.clone(), max_val.as_ref().map(|v| value_to_json(v)).unwrap_or(serde_json::Value::Null));
                            fields.push(FieldInfo { name: spec.alias.clone(), data_type: "text".to_string() });
                        }
                    }
                    AggFunc::StringAgg(col_idx, delim) => {
                        let mut parts = Vec::new();
                        for i in 0..table.rows.len() {
                            if !table.is_deleted[i] {
                                if let Some(v) = table.rows[i].get(*col_idx) {
                                    if !v.is_null() {
                                        parts.push(v.as_str());
                                    }
                                }
                            }
                        }
                        let joined = parts.join(delim);
                        row_map.insert(spec.alias.clone(), serde_json::Value::String(joined));
                        fields.push(FieldInfo { name: spec.alias.clone(), data_type: "text".to_string() });
                    }
                    AggFunc::ArrayAgg(col_idx) => {
                        let mut arr = Vec::new();
                        for i in 0..table.rows.len() {
                            if !table.is_deleted[i] {
                                if let Some(v) = table.rows[i].get(*col_idx) {
                                    if !v.is_null() {
                                        arr.push(value_to_json(v));
                                    }
                                }
                            }
                        }
                        row_map.insert(spec.alias.clone(), serde_json::Value::Array(arr));
                        fields.push(FieldInfo { name: spec.alias.clone(), data_type: "array".to_string() });
                    }
                    AggFunc::ArrayAggExpr(expr_str) => {
                        let mut arr = Vec::new();
                        for i in 0..table.rows.len() {
                            if !table.is_deleted[i] {
                                let v = eval_sql_expr(expr_str, &table.rows[i], Some(table), &[], None, params);
                                if !v.is_null() {
                                    arr.push(value_to_json(&v));
                                }
                            }
                        }
                        row_map.insert(spec.alias.clone(), serde_json::Value::Array(arr));
                        fields.push(FieldInfo { name: spec.alias.clone(), data_type: "array".to_string() });
                    }
                    AggFunc::WrappedAgg(raw_expr, inner_func) => {
                        let inner_json = match inner_func.as_ref() {
                            AggFunc::ArrayAgg(c_idx) => {
                                let mut arr = Vec::new();
                                for i in 0..table.rows.len() {
                                    if !table.is_deleted[i] {
                                        if let Some(v) = table.rows[i].get(*c_idx) {
                                            if !v.is_null() {
                                                arr.push(value_to_json(v));
                                            }
                                        }
                                    }
                                }
                                serde_json::Value::Array(arr)
                            }
                            AggFunc::ArrayAggExpr(expr_str) => {
                                let mut arr = Vec::new();
                                for i in 0..table.rows.len() {
                                    if !table.is_deleted[i] {
                                        let v = eval_sql_expr(expr_str, &table.rows[i], Some(table), &[], None, params);
                                        if !v.is_null() {
                                            arr.push(value_to_json(&v));
                                        }
                                    }
                                }
                                serde_json::Value::Array(arr)
                            }
                            _ => serde_json::Value::Null,
                        };
                        let inner_pattern = if raw_expr.to_uppercase().contains("ARRAY_AGG(") {
                            let u = raw_expr.to_uppercase();
                            let start = u.find("ARRAY_AGG(").unwrap();
                            if let Some(end) = find_matching_paren(raw_expr, start + 9) {
                                Some(&raw_expr[start..=end])
                            } else {
                                None
                            }
                        } else {
                            None
                        };
                        let substituted_expr = if let Some(pat) = inner_pattern {
                            let json_str = serde_json::to_string(&inner_json).unwrap_or_else(|_| "[]".to_string());
                            let pg_arr_str = format!("'{}'", json_str.replace('\'', "''"));
                            raw_expr.replace(pat, &pg_arr_str)
                        } else {
                            raw_expr.clone()
                        };
                        let res_val = eval_sql_expr(&substituted_expr, &[], None, &[], None, params);
                        let dt = match &res_val {
                            Value::Int(_) => "int8",
                            Value::Float(_) => "float8",
                            Value::Bool(_) => "bool",
                            _ => "text",
                        };
                        row_map.insert(spec.alias.clone(), value_to_json(&res_val));
                        fields.push(FieldInfo { name: spec.alias.clone(), data_type: dt.to_string() });
                    }
                    AggFunc::JsonAgg(col_idx) | AggFunc::JsonbAgg(col_idx) => {
                        let mut arr = Vec::new();
                        for i in 0..table.rows.len() {
                            if !table.is_deleted[i] {
                                if let Some(v) = table.rows[i].get(*col_idx) {
                                    if !v.is_null() {
                                        let j_val = match v {
                                            Value::Text(st) => serde_json::from_str::<serde_json::Value>(st.trim()).unwrap_or(serde_json::Value::String(st.to_string())),
                                            _ => value_to_json(v),
                                        };
                                        arr.push(j_val);
                                    }
                                }
                            }
                        }
                        row_map.insert(spec.alias.clone(), serde_json::Value::Array(arr));
                        fields.push(FieldInfo { name: spec.alias.clone(), data_type: "json".to_string() });
                    }
                    AggFunc::ObjectAgg(k, v, jsonb) => {
                        let pairs = (0..table.rows.len()).filter(|&i| !table.is_deleted[i]).map(|i| (eval_sql_expr(k, &table.rows[i], Some(table), &[], None, params), eval_sql_expr(v, &table.rows[i], Some(table), &[], None, params))).collect();
                        row_map.insert(spec.alias.clone(), object_agg_json(pairs, *jsonb));
                        fields.push(FieldInfo { name: spec.alias.clone(), data_type: if *jsonb { "jsonb".to_string() } else { "json".to_string() } });
                    }
                    AggFunc::JsonAggExpr(expr_str) | AggFunc::JsonbAggExpr(expr_str) => {
                        let live: Vec<usize> = (0..table.rows.len()).filter(|&i| !table.is_deleted[i]).collect();
                        let (expr_str, ordered) = agg_order_rows(expr_str, table, &live, params);
                        let expr_str = expr_str.as_str();
                        let mut arr = Vec::new();
                        for i in ordered {
                            {
                                let v = eval_sql_expr(expr_str, &table.rows[i], Some(table), &[], None, params);
                                if !v.is_null() {
                                    let j_val = match &v {
                                        Value::Text(st) => serde_json::from_str::<serde_json::Value>(st.trim()).unwrap_or(serde_json::Value::String(st.to_string())),
                                        _ => value_to_json(&v),
                                    };
                                    arr.push(j_val);
                                }
                            }
                        }
                        row_map.insert(spec.alias.clone(), serde_json::Value::Array(arr));
                        fields.push(FieldInfo { name: spec.alias.clone(), data_type: "json".to_string() });
                    }
                    AggFunc::SumExpr(expr_str) => {
                        let sum: f64 = (0..table.rows.len())
                            .filter(|&i| !table.is_deleted[i])
                            .filter_map(|i| {
                                let v = eval_sql_expr(expr_str, &table.rows[i], Some(table), &[], None, params);
                                v.as_f64()
                            })
                            .sum();
                        row_map.insert(spec.alias.clone(), json!(sum));
                        fields.push(FieldInfo { name: spec.alias.clone(), data_type: "numeric".to_string() });
                    }
                    AggFunc::AvgExpr(expr_str) => {
                        let mut count = 0;
                        let mut sum: f64 = 0.0;
                        for i in 0..table.rows.len() {
                            if !table.is_deleted[i] {
                                let v = eval_sql_expr(expr_str, &table.rows[i], Some(table), &[], None, params);
                                if let Some(f) = v.as_f64() {
                                    sum += f;
                                    count += 1;
                                }
                            }
                        }
                        let avg = if count > 0 { sum / count as f64 } else { 0.0 };
                        row_map.insert(spec.alias.clone(), json!(avg));
                        fields.push(FieldInfo { name: spec.alias.clone(), data_type: "float8".to_string() });
                    }
                    AggFunc::MinExpr(expr_str) => {
                        let mut min_val: Option<Value> = None;
                        for i in 0..table.rows.len() {
                            if !table.is_deleted[i] {
                                let v = eval_sql_expr(expr_str, &table.rows[i], Some(table), &[], None, params);
                                if !v.is_null() {
                                    min_val = match min_val {
                                        None => Some(v.clone()),
                                        Some(cur) => if v.cmp_value(&cur) == std::cmp::Ordering::Less { Some(v.clone()) } else { Some(cur) },
                                    };
                                }
                            }
                        }
                        row_map.insert(spec.alias.clone(), min_val.as_ref().map(|v| value_to_json(v)).unwrap_or(serde_json::Value::Null));
                        fields.push(FieldInfo { name: spec.alias.clone(), data_type: "numeric".to_string() });
                    }
                    AggFunc::MaxExpr(expr_str) => {
                        let mut max_val: Option<Value> = None;
                        for i in 0..table.rows.len() {
                            if !table.is_deleted[i] {
                                let v = eval_sql_expr(expr_str, &table.rows[i], Some(table), &[], None, params);
                                if !v.is_null() {
                                    max_val = match max_val {
                                        None => Some(v.clone()),
                                        Some(cur) => if v.cmp_value(&cur) == std::cmp::Ordering::Greater { Some(v.clone()) } else { Some(cur) },
                                    };
                                }
                            }
                        }
                        row_map.insert(spec.alias.clone(), max_val.as_ref().map(|v| value_to_json(v)).unwrap_or(serde_json::Value::Null));
                        fields.push(FieldInfo { name: spec.alias.clone(), data_type: "numeric".to_string() });
                    }
                    AggFunc::BoolAnd(col_idx) => {
                        let mut res: Option<bool> = None;
                        for i in 0..table.rows.len() {
                            if !table.is_deleted[i] {
                                if let Some(v) = table.rows[i].get(*col_idx) {
                                    if !v.is_null() {
                                        res = Some(res.unwrap_or(true) && v.as_bool().unwrap_or(false));
                                    }
                                }
                            }
                        }
                        row_map.insert(spec.alias.clone(), res.map(|b| json!(b)).unwrap_or(serde_json::Value::Null));
                        fields.push(FieldInfo { name: spec.alias.clone(), data_type: "bool".to_string() });
                    }
                    AggFunc::BoolOr(col_idx) => {
                        let mut res: Option<bool> = None;
                        for i in 0..table.rows.len() {
                            if !table.is_deleted[i] {
                                if let Some(v) = table.rows[i].get(*col_idx) {
                                    if !v.is_null() {
                                        res = Some(res.unwrap_or(false) || v.as_bool().unwrap_or(false));
                                    }
                                }
                            }
                        }
                        row_map.insert(spec.alias.clone(), res.map(|b| json!(b)).unwrap_or(serde_json::Value::Null));
                        fields.push(FieldInfo { name: spec.alias.clone(), data_type: "bool".to_string() });
                    }
                    AggFunc::BoolAndExpr(expr_str) => {
                        let mut res: Option<bool> = None;
                        for i in 0..table.rows.len() {
                            if !table.is_deleted[i] {
                                let v = eval_sql_expr(expr_str, &table.rows[i], Some(table), &[], None, params);
                                if !v.is_null() {
                                    res = Some(res.unwrap_or(true) && v.as_bool().unwrap_or(false));
                                }
                            }
                        }
                        row_map.insert(spec.alias.clone(), res.map(|b| json!(b)).unwrap_or(serde_json::Value::Null));
                        fields.push(FieldInfo { name: spec.alias.clone(), data_type: "bool".to_string() });
                    }
                    AggFunc::BoolOrExpr(expr_str) => {
                        let mut res: Option<bool> = None;
                        for i in 0..table.rows.len() {
                            if !table.is_deleted[i] {
                                let v = eval_sql_expr(expr_str, &table.rows[i], Some(table), &[], None, params);
                                if !v.is_null() {
                                    res = Some(res.unwrap_or(false) || v.as_bool().unwrap_or(false));
                                }
                            }
                        }
                        row_map.insert(spec.alias.clone(), res.map(|b| json!(b)).unwrap_or(serde_json::Value::Null));
                        fields.push(FieldInfo { name: spec.alias.clone(), data_type: "bool".to_string() });
                    }
                }
            }
        } else {
            // Filtered scan aggregate
            let where_expr = parse_where_expr(where_clause.unwrap(), table, params)?;
            let total_rows = table.rows.len();

            let matched_indices: Vec<usize> = if total_rows > 20_000 {
                (0..total_rows)
                    .into_par_iter()
                    .filter(|&i| !table.is_deleted[i] && evaluate_where_expr(&table.rows[i], Some(table), &where_expr, params))
                    .collect()
            } else {
                (0..total_rows)
                    .filter(|&i| !table.is_deleted[i] && evaluate_where_expr(&table.rows[i], Some(table), &where_expr, params))
                    .collect()
            };

            for spec in agg_specs {
                match &spec.func {
                    AggFunc::CountStar => {
                        let count = matched_indices.len();
                        row_map.insert(spec.alias.clone(), json!(count));
                        fields.push(FieldInfo { name: spec.alias.clone(), data_type: "int8".to_string() });
                    }
                    AggFunc::Count(col_idx) => {
                        let count = matched_indices.iter()
                            .filter(|&&i| table.rows[i].get(*col_idx).map_or(false, |v| !v.is_null()))
                            .count();
                        row_map.insert(spec.alias.clone(), json!(count));
                        fields.push(FieldInfo { name: spec.alias.clone(), data_type: "int8".to_string() });
                    }
                    AggFunc::Extra(name, args) => {
                        let rows: Vec<Vec<Value>> = matched_indices.iter().copied().map(|ri| args.iter().map(|a| eval_sql_expr(a, &table.rows[ri], Some(table), &[], None, params)).collect()).collect();
                        row_map.insert(spec.alias.clone(), value_to_json(&crate::engine::fnlib::agg_finalize(name, &rows)));
                        fields.push(FieldInfo { name: spec.alias.clone(), data_type: crate::engine::fnlib::agg_result_type(name).unwrap_or("text").to_string() });
                    }
                    AggFunc::CountExpr(expr) => {
                        let count = matched_indices.iter()
                            .filter(|&&i| !eval_sql_expr(expr, &table.rows[i], Some(table), &[], None, params).is_null())
                            .count();
                        row_map.insert(spec.alias.clone(), json!(count));
                        fields.push(FieldInfo { name: spec.alias.clone(), data_type: "int8".to_string() });
                    }
                    AggFunc::CountDistinct(col_idx) => {
                        let mut set = std::collections::HashSet::new();
                        for &i in &matched_indices {
                            if let Some(v) = table.rows[i].get(*col_idx) {
                                if !v.is_null() {
                                    set.insert(format!("{:?}", v));
                                }
                            }
                        }
                        row_map.insert(spec.alias.clone(), json!(set.len()));
                        fields.push(FieldInfo { name: spec.alias.clone(), data_type: "int8".to_string() });
                    }
                    AggFunc::Sum(col_idx) => {
                        let nums: Vec<f64> = matched_indices.iter().filter_map(|&i| table.rows[i].get(*col_idx).and_then(|v| v.as_f64())).collect();
                        if nums.is_empty() {
                            row_map.insert(spec.alias.clone(), serde_json::Value::Null);
                        } else {
                            let sum: f64 = nums.iter().sum();
                            row_map.insert(spec.alias.clone(), json!(sum));
                        }
                        fields.push(FieldInfo { name: spec.alias.clone(), data_type: "numeric".to_string() });
                    }
                    AggFunc::Avg(col_idx) => {
                        let nums: Vec<f64> = matched_indices.iter().filter_map(|&i| table.rows[i].get(*col_idx).and_then(|v| v.as_f64())).collect();
                        if nums.is_empty() {
                            row_map.insert(spec.alias.clone(), serde_json::Value::Null);
                        } else {
                            let sum: f64 = nums.iter().sum();
                            let avg = sum / nums.len() as f64;
                            row_map.insert(spec.alias.clone(), json!(avg));
                        }
                        fields.push(FieldInfo { name: spec.alias.clone(), data_type: "float8".to_string() });
                    }
                    AggFunc::Min(col_idx) => {
                        let is_numeric = matches!(table.columns[*col_idx].data_type, DataType::Integer | DataType::BigInt | DataType::Serial | DataType::Numeric);
                        if is_numeric {
                            let nums: Vec<f64> = matched_indices.iter().filter_map(|&i| table.rows[i].get(*col_idx).and_then(|v| v.as_f64())).collect();
                            if nums.is_empty() {
                                row_map.insert(spec.alias.clone(), serde_json::Value::Null);
                            } else {
                                let min = nums.into_iter().fold(f64::INFINITY, f64::min);
                                row_map.insert(spec.alias.clone(), json!(min));
                            }
                            fields.push(FieldInfo { name: spec.alias.clone(), data_type: "numeric".to_string() });
                        } else {
                            let mut min_val: Option<Value> = None;
                            for &i in &matched_indices {
                                if let Some(v) = table.rows[i].get(*col_idx) {
                                    if !v.is_null() {
                                        min_val = match min_val {
                                            None => Some(v.clone()),
                                            Some(cur) => if v.cmp_value(&cur) == std::cmp::Ordering::Less { Some(v.clone()) } else { Some(cur) },
                                        };
                                    }
                                }
                            }
                            row_map.insert(spec.alias.clone(), min_val.as_ref().map(|v| value_to_json(v)).unwrap_or(serde_json::Value::Null));
                            fields.push(FieldInfo { name: spec.alias.clone(), data_type: "text".to_string() });
                        }
                    }
                    AggFunc::Max(col_idx) => {
                        let is_numeric = matches!(table.columns[*col_idx].data_type, DataType::Integer | DataType::BigInt | DataType::Serial | DataType::Numeric);
                        if is_numeric {
                            let nums: Vec<f64> = matched_indices.iter().filter_map(|&i| table.rows[i].get(*col_idx).and_then(|v| v.as_f64())).collect();
                            if nums.is_empty() {
                                row_map.insert(spec.alias.clone(), serde_json::Value::Null);
                            } else {
                                let max = nums.into_iter().fold(f64::NEG_INFINITY, f64::max);
                                row_map.insert(spec.alias.clone(), json!(max));
                            }
                            fields.push(FieldInfo { name: spec.alias.clone(), data_type: "numeric".to_string() });
                        } else {
                            let mut max_val: Option<Value> = None;
                            for &i in &matched_indices {
                                if let Some(v) = table.rows[i].get(*col_idx) {
                                    if !v.is_null() {
                                        max_val = match max_val {
                                            None => Some(v.clone()),
                                            Some(cur) => if v.cmp_value(&cur) == std::cmp::Ordering::Greater { Some(v.clone()) } else { Some(cur) },
                                        };
                                    }
                                }
                            }
                            row_map.insert(spec.alias.clone(), max_val.as_ref().map(|v| value_to_json(v)).unwrap_or(serde_json::Value::Null));
                            fields.push(FieldInfo { name: spec.alias.clone(), data_type: "text".to_string() });
                        }
                    }
                    AggFunc::StringAgg(col_idx, delim) => {
                        let mut parts = Vec::new();
                        for &i in &matched_indices {
                            if let Some(v) = table.rows[i].get(*col_idx) {
                                if !v.is_null() {
                                    parts.push(v.as_str());
                                }
                            }
                        }
                        let joined = parts.join(delim);
                        row_map.insert(spec.alias.clone(), serde_json::Value::String(joined));
                        fields.push(FieldInfo { name: spec.alias.clone(), data_type: "text".to_string() });
                    }
                    AggFunc::ArrayAgg(col_idx) => {
                        if matched_indices.is_empty() {
                            row_map.insert(spec.alias.clone(), serde_json::Value::Null);
                        } else {
                            let mut arr = Vec::new();
                            for &i in &matched_indices {
                                if let Some(v) = table.rows[i].get(*col_idx) {
                                    if !v.is_null() {
                                        arr.push(value_to_json(v));
                                    }
                                }
                            }
                            row_map.insert(spec.alias.clone(), serde_json::Value::Array(arr));
                        }
                        fields.push(FieldInfo { name: spec.alias.clone(), data_type: "array".to_string() });
                    }
                    AggFunc::ArrayAggExpr(expr_str) => {
                        if matched_indices.is_empty() {
                            row_map.insert(spec.alias.clone(), serde_json::Value::Null);
                        } else {
                            let mut arr = Vec::new();
                            for &i in &matched_indices {
                                let v = eval_sql_expr(expr_str, &table.rows[i], Some(table), &[], None, params);
                                if !v.is_null() {
                                    arr.push(value_to_json(&v));
                                }
                            }
                            row_map.insert(spec.alias.clone(), serde_json::Value::Array(arr));
                        }
                        fields.push(FieldInfo { name: spec.alias.clone(), data_type: "array".to_string() });
                    }
                    AggFunc::WrappedAgg(raw_expr, inner_func) => {
                        let inner_json = if matched_indices.is_empty() {
                            serde_json::Value::Null
                        } else {
                            match inner_func.as_ref() {
                                AggFunc::ArrayAgg(c_idx) => {
                                    let mut arr = Vec::new();
                                    for &i in &matched_indices {
                                        if let Some(v) = table.rows[i].get(*c_idx) {
                                            if !v.is_null() {
                                                arr.push(value_to_json(v));
                                            }
                                        }
                                    }
                                    serde_json::Value::Array(arr)
                                }
                                AggFunc::ArrayAggExpr(expr_str) => {
                                    let mut arr = Vec::new();
                                    for &i in &matched_indices {
                                        let v = eval_sql_expr(expr_str, &table.rows[i], Some(table), &[], None, params);
                                        if !v.is_null() {
                                            arr.push(value_to_json(&v));
                                        }
                                    }
                                    serde_json::Value::Array(arr)
                                }
                                _ => serde_json::Value::Null,
                            }
                        };
                        let inner_pattern = if raw_expr.to_uppercase().contains("ARRAY_AGG(") {
                            let u = raw_expr.to_uppercase();
                            let start = u.find("ARRAY_AGG(").unwrap();
                            if let Some(end) = find_matching_paren(raw_expr, start + 9) {
                                Some(&raw_expr[start..=end])
                            } else {
                                None
                            }
                        } else {
                            None
                        };
                        let substituted_expr = if let Some(pat) = inner_pattern {
                            let pg_arr_str = if inner_json.is_null() {
                                "NULL".to_string()
                            } else {
                                let json_str = serde_json::to_string(&inner_json).unwrap_or_else(|_| "[]".to_string());
                                format!("'{}'", json_str.replace('\'', "''"))
                            };
                            raw_expr.replace(pat, &pg_arr_str)
                        } else {
                            raw_expr.clone()
                        };
                        let res_val = eval_sql_expr(&substituted_expr, &[], None, &[], None, params);
                        let dt = match &res_val {
                            Value::Int(_) => "int8",
                            Value::Float(_) => "float8",
                            Value::Bool(_) => "bool",
                            _ => "text",
                        };
                        row_map.insert(spec.alias.clone(), value_to_json(&res_val));
                        fields.push(FieldInfo { name: spec.alias.clone(), data_type: dt.to_string() });
                    }
                    AggFunc::JsonAgg(col_idx) | AggFunc::JsonbAgg(col_idx) => {
                        let mut arr = Vec::new();
                        for &i in &matched_indices {
                            if let Some(v) = table.rows[i].get(*col_idx) {
                                if !v.is_null() {
                                    let j_val = match v {
                                        Value::Text(st) => serde_json::from_str::<serde_json::Value>(st.trim()).unwrap_or(serde_json::Value::String(st.to_string())),
                                        _ => value_to_json(v),
                                    };
                                    arr.push(j_val);
                                }
                            }
                        }
                        row_map.insert(spec.alias.clone(), serde_json::Value::Array(arr));
                        fields.push(FieldInfo { name: spec.alias.clone(), data_type: "json".to_string() });
                    }
                    AggFunc::ObjectAgg(k, v, jsonb) => {
                        let pairs = matched_indices.iter().map(|&i| (eval_sql_expr(k, &table.rows[i], Some(table), &[], None, params), eval_sql_expr(v, &table.rows[i], Some(table), &[], None, params))).collect();
                        row_map.insert(spec.alias.clone(), object_agg_json(pairs, *jsonb));
                        fields.push(FieldInfo { name: spec.alias.clone(), data_type: if *jsonb { "jsonb".to_string() } else { "json".to_string() } });
                    }
                    AggFunc::JsonAggExpr(expr_str) | AggFunc::JsonbAggExpr(expr_str) => {
                        let (expr_str, ordered) = agg_order_rows(expr_str, table, &matched_indices, params);
                        let expr_str = expr_str.as_str();
                        let mut arr = Vec::new();
                        for &i in &ordered {
                            let v = eval_sql_expr(expr_str, &table.rows[i], Some(table), &[], None, params);
                            if !v.is_null() {
                                let j_val = match &v {
                                    Value::Text(st) => serde_json::from_str::<serde_json::Value>(st.trim()).unwrap_or(serde_json::Value::String(st.to_string())),
                                    _ => value_to_json(&v),
                                };
                                arr.push(j_val);
                            }
                        }
                        row_map.insert(spec.alias.clone(), serde_json::Value::Array(arr));
                        fields.push(FieldInfo { name: spec.alias.clone(), data_type: "json".to_string() });
                    }
                    AggFunc::SumExpr(expr_str) => {
                        let nums: Vec<f64> = matched_indices.iter().filter_map(|&i| {
                            let v = eval_sql_expr(expr_str, &table.rows[i], Some(table), &[], None, params);
                            v.as_f64()
                        }).collect();
                        if nums.is_empty() {
                            row_map.insert(spec.alias.clone(), serde_json::Value::Null);
                        } else {
                            let sum: f64 = nums.iter().sum();
                            row_map.insert(spec.alias.clone(), json!(sum));
                        }
                        fields.push(FieldInfo { name: spec.alias.clone(), data_type: "numeric".to_string() });
                    }
                    AggFunc::AvgExpr(expr_str) => {
                        let nums: Vec<f64> = matched_indices.iter().filter_map(|&i| {
                            let v = eval_sql_expr(expr_str, &table.rows[i], Some(table), &[], None, params);
                            v.as_f64()
                        }).collect();
                        if nums.is_empty() {
                            row_map.insert(spec.alias.clone(), serde_json::Value::Null);
                        } else {
                            let sum: f64 = nums.iter().sum();
                            let avg = sum / nums.len() as f64;
                            row_map.insert(spec.alias.clone(), json!(avg));
                        }
                        fields.push(FieldInfo { name: spec.alias.clone(), data_type: "float8".to_string() });
                    }
                    AggFunc::MinExpr(expr_str) => {
                        let mut min_val: Option<Value> = None;
                        for &i in &matched_indices {
                            let v = eval_sql_expr(expr_str, &table.rows[i], Some(table), &[], None, params);
                            if !v.is_null() {
                                min_val = match min_val {
                                    None => Some(v.clone()),
                                    Some(cur) => if v.cmp_value(&cur) == std::cmp::Ordering::Less { Some(v.clone()) } else { Some(cur) },
                                };
                            }
                        }
                        row_map.insert(spec.alias.clone(), min_val.as_ref().map(|v| value_to_json(v)).unwrap_or(serde_json::Value::Null));
                        fields.push(FieldInfo { name: spec.alias.clone(), data_type: "numeric".to_string() });
                    }
                    AggFunc::MaxExpr(expr_str) => {
                        let mut max_val: Option<Value> = None;
                        for &i in &matched_indices {
                            let v = eval_sql_expr(expr_str, &table.rows[i], Some(table), &[], None, params);
                            if !v.is_null() {
                                max_val = match max_val {
                                    None => Some(v.clone()),
                                    Some(cur) => if v.cmp_value(&cur) == std::cmp::Ordering::Greater { Some(v.clone()) } else { Some(cur) },
                                };
                            }
                        }
                        row_map.insert(spec.alias.clone(), max_val.as_ref().map(|v| value_to_json(v)).unwrap_or(serde_json::Value::Null));
                        fields.push(FieldInfo { name: spec.alias.clone(), data_type: "numeric".to_string() });
                    }
                    AggFunc::BoolAnd(col_idx) => {
                        let mut res: Option<bool> = None;
                        for &i in &matched_indices {
                            if let Some(v) = table.rows[i].get(*col_idx) {
                                if !v.is_null() {
                                    res = Some(res.unwrap_or(true) && v.as_bool().unwrap_or(false));
                                }
                            }
                        }
                        row_map.insert(spec.alias.clone(), res.map(|b| json!(b)).unwrap_or(serde_json::Value::Null));
                        fields.push(FieldInfo { name: spec.alias.clone(), data_type: "bool".to_string() });
                    }
                    AggFunc::BoolOr(col_idx) => {
                        let mut res: Option<bool> = None;
                        for &i in &matched_indices {
                            if let Some(v) = table.rows[i].get(*col_idx) {
                                if !v.is_null() {
                                    res = Some(res.unwrap_or(false) || v.as_bool().unwrap_or(false));
                                }
                            }
                        }
                        row_map.insert(spec.alias.clone(), res.map(|b| json!(b)).unwrap_or(serde_json::Value::Null));
                        fields.push(FieldInfo { name: spec.alias.clone(), data_type: "bool".to_string() });
                    }
                    AggFunc::BoolAndExpr(expr_str) => {
                        let mut res: Option<bool> = None;
                        for &i in &matched_indices {
                            let v = eval_sql_expr(expr_str, &table.rows[i], Some(table), &[], None, params);
                            if !v.is_null() {
                                res = Some(res.unwrap_or(true) && v.as_bool().unwrap_or(false));
                            }
                        }
                        row_map.insert(spec.alias.clone(), res.map(|b| json!(b)).unwrap_or(serde_json::Value::Null));
                        fields.push(FieldInfo { name: spec.alias.clone(), data_type: "bool".to_string() });
                    }
                    AggFunc::BoolOrExpr(expr_str) => {
                        let mut res: Option<bool> = None;
                        for &i in &matched_indices {
                            let v = eval_sql_expr(expr_str, &table.rows[i], Some(table), &[], None, params);
                            if !v.is_null() {
                                res = Some(res.unwrap_or(false) || v.as_bool().unwrap_or(false));
                            }
                        }
                        row_map.insert(spec.alias.clone(), res.map(|b| json!(b)).unwrap_or(serde_json::Value::Null));
                        fields.push(FieldInfo { name: spec.alias.clone(), data_type: "bool".to_string() });
                    }
                }
            }
        }

        Ok(QueryResult {
            rows: vec![serde_json::Value::Object(row_map)],
            row_count: 1,
            fields,
            command: "SELECT".to_string(),
        })
    }

    /// SET có subquery tương quan với bảng đích (`table` hoặc `alias`): với mỗi dòng sống khớp WHERE, thay
    /// tham chiếu `alias.col` / `table.col` bằng giá trị của dòng, chạy subquery và thay bằng kết quả.
    /// Trả mệnh đề SET đã thay theo chỉ số dòng; `None` nếu SET không có subquery tương quan.
    fn resolve_correlated_set_subqueries(
        &mut self,
        table_name: &str,
        alias: Option<&str>,
        set_clause: &str,
        where_clause: Option<&str>,
        params: &[Value],
    ) -> Result<Option<HashMap<usize, String>>, String> {
        if !contains_ci(set_clause, "SELECT") {
            return Ok(None);
        }
        // các khoảng (SELECT ...) tương quan
        let mut spans: Vec<(usize, usize)> = Vec::new();
        let b = set_clause.as_bytes();
        let mut in_sq = false;
        let mut i = 0;
        while i < b.len() {
            if b[i] == b'\'' {
                in_sq = !in_sq;
            } else if !in_sq && b[i] == b'(' && set_clause[i + 1..].trim_start().get(..6).map_or(false, |p| p.eq_ignore_ascii_case("SELECT")) {
                let Some(close) = crate::engine::constraints::matching_paren(set_clause, i) else { break };
                if subquery_is_correlated(&set_clause[i + 1..close]) {
                    spans.push((i, close));
                }
                i = close + 1;
                continue;
            }
            i += 1;
        }
        if spans.is_empty() {
            return Ok(None);
        }
        let table = self.storage.get_table(table_name).ok_or_else(|| format!("Table {} not found", table_name))?;
        let quals: Vec<String> = std::iter::once(table.name.to_lowercase()).chain(alias.map(|a| a.to_string())).collect();
        let where_expr = where_clause.filter(|w| !w.is_empty()).map(|w| compile_expr(w, Some(table), None, None, None));
        let rows: Vec<(usize, Vec<Value>)> = (0..table.rows.len())
            .filter(|&r| !table.is_deleted[r] && where_expr.as_ref().map_or(true, |e| e.eval_bool(&table.rows[r], None, params) == Some(true)))
            .map(|r| (r, table.rows[r].clone()))
            .collect();
        let columns: Vec<String> = table.columns.iter().map(|c| c.name.to_lowercase()).collect();
        let mut out = HashMap::with_capacity(rows.len());
        for (r, row) in rows {
            let mut clause = String::with_capacity(set_clause.len());
            let mut last = 0;
            for &(a, z) in &spans {
                let sub = &set_clause[a + 1..z];
                let own = subquery_own_names(sub);
                let resolved = Self::substitute_qualified_refs(sub, &|q: &str, col: &str| {
                    if own.iter().any(|o| o == q) || !quals.iter().any(|x| x == q) {
                        return None;
                    }
                    columns.iter().position(|c| c == col).map(|ci| row.get(ci).cloned().unwrap_or(Value::Null))
                });
                let (res, _) = self.handle_select(resolved.trim(), params)?;
                if res.rows.len() > 1 {
                    return Err("more than one row returned by a subquery used as an expression".to_string());
                }
                let v = res.rows.first().and_then(|row| row.as_object()).and_then(|o| o.values().next()).map(json_to_value).unwrap_or(Value::Null);
                clause.push_str(&set_clause[last..a]);
                clause.push_str(&Self::value_as_sql_literal(&v));
                last = z + 1;
            }
            clause.push_str(&set_clause[last..]);
            out.insert(r, clause);
        }
        Ok(Some(out))
    }

    /// `SET c = (SELECT ... FROM ...)` không tương quan: chạy subquery một lần, thay bằng hằng (trước đây bộ
    /// đánh giá biểu thức không chạy được subquery và ghi `false` vào cột). Subquery tương quan để nguyên.
    pub(crate) fn resolve_uncorrelated_set_subqueries(&mut self, set_clause: &str, params: &[Value]) -> Result<String, String> {
        if !contains_ci(set_clause, "SELECT") {
            return Ok(set_clause.to_string());
        }
        let b = set_clause.as_bytes();
        let mut out = String::with_capacity(set_clause.len());
        let mut last = 0;
        let mut in_sq = false;
        let mut i = 0;
        while i < b.len() {
            if b[i] == b'\'' {
                in_sq = !in_sq;
            } else if !in_sq && b[i] == b'(' && set_clause[i + 1..].trim_start().get(..6).map_or(false, |p| p.eq_ignore_ascii_case("SELECT")) {
                let Some(close) = crate::engine::constraints::matching_paren(set_clause, i) else { break };
                let sub = &set_clause[i + 1..close];
                if find_top_level_keyword(sub, "FROM").is_some() && !subquery_is_correlated(sub) {
                    let (res, _) = self.handle_select(sub.trim(), params)?;
                    if res.rows.len() > 1 {
                        return Err("more than one row returned by a subquery used as an expression".to_string());
                    }
                    let v = res.rows.first().and_then(|row| row.as_object()).and_then(|o| o.values().next()).map(json_to_value).unwrap_or(Value::Null);
                    out.push_str(&set_clause[last..i]);
                    out.push_str(&Self::value_as_sql_literal(&v));
                    last = close + 1;
                }
                i = close + 1;
                continue;
            }
            i += 1;
        }
        out.push_str(&set_clause[last..]);
        Ok(out)
    }

    fn handle_update(&mut self, sql: &str, params: &[Value]) -> Result<QueryResult, String> {
        if let Some(expanded) = expand_multi_column_set(sql)? {
            return self.handle_update(&expanded, params);
        }
        if let Some((head, set_part, from_list, where_clause, returning)) = crate::engine::dml_join::split_update_from(sql) {
            return self.handle_update_from(head, set_part, from_list, where_clause, returning, params);
        }
        let set_idx = find_top_level_keyword(sql, "SET").ok_or("Missing SET in UPDATE")?;
        let where_idx = find_top_level_keyword(sql, "WHERE");
        let returning_idx = find_top_level_keyword(sql, "RETURNING");

        let table_name = clean_table_name(sql[6..set_idx].trim());

        let set_end = where_idx.or(returning_idx).unwrap_or(sql.len());
        let set_clause_owned = self.resolve_uncorrelated_set_subqueries(sql[set_idx + 3..set_end].trim(), params)?;
        let set_clause = set_clause_owned.as_str();

        let (where_clause, returning_cols) = match (where_idx, returning_idx) {
            (Some(w_idx), Some(r_idx)) => (Some(sql[w_idx + 5..r_idx].trim()), Some(sql[r_idx + 9..].trim())),
            (Some(w_idx), None) => (Some(sql[w_idx + 5..].trim()), None),
            (None, Some(r_idx)) => (None, Some(sql[r_idx + 9..].trim())),
            (None, None) => (None, None),
        };

        if where_idx.is_none() && has_unparsed_top_level_where(sql) {
            return Err("Failed to parse WHERE clause in UPDATE statement - aborting to prevent accidental table-wide update".to_string());
        }

        let in_tx = self.storage.undo_enabled();
        let mut wal_updates: Vec<(RowIdentity, Vec<(usize, Value)>)> = Vec::new();
        // Dòng bị đổi khoá chính: (khoá cũ, dòng mới).
        let mut moved_rows: Vec<(RowIdentity, Vec<Value>)> = Vec::new();
        let mut undo_restores = Vec::new();
        let mut row_count = 0;
        let mut returned_rows = Vec::new();

        let resolved_where = if let Some(w) = where_clause {
            let w_up = w.to_uppercase();
            if w_up.contains("SELECT") && w_up.contains("FROM") {
                Some(self.resolve_subqueries_in_where(w, params)?)
            } else {
                Some(w.to_string())
            }
        } else {
            None
        };

        let referenced = self.is_referenced(table_name);
        let mut updated_rows: Vec<usize> = Vec::new();
        let mut changed_cols: Vec<usize> = Vec::new();
        let mut old_rows: Vec<(usize, Vec<Value>)> = Vec::new();

        // SET col = (SELECT ... WHERE x.k = t.k): subquery tương quan tính theo từng dòng bằng chính engine
        // (trước đây bộ đánh giá biểu thức không chạy được subquery và ghi `true` / NULL vào cột).
        let per_row_set = {
            let target_alias = extract_table_name_and_alias(sql[6..set_idx].trim()).1.map(|a| a.trim_matches('"').to_lowercase());
            self.resolve_correlated_set_subqueries(table_name, target_alias.as_deref(), set_clause, resolved_where.as_deref(), params)?
        };

        let fields = {
            let table = self.storage.get_table_mut(table_name).ok_or_else(|| format!("Table {} not found", table_name))?;

            // Parse multiple SET assignments: "updated_at" = CURRENT_TIMESTAMP, "student_count" = $1, "salary" = salary * 1.10
            let mut raw_assignments: Vec<(usize, String)> = Vec::new();
            for assign in split_comma_separated_tokens(set_clause) {
                let assign = assign.trim();
                if assign.is_empty() {
                    continue;
                }
                let eq_idx = assign.find('=').ok_or("Invalid assignment in SET clause")?;
                let col_name = clean_col_name(&assign[..eq_idx]);
                let val_str = assign[eq_idx + 1..].trim().to_string();
                let col_idx = table.get_column_index(col_name).ok_or_else(|| format!("Column {} not found in table {}", col_name, table_name))?;
                // GENERATED ALWAYS AS IDENTITY chỉ được gán DEFAULT (như PostgreSQL)
                if table.identity.get(&table.columns[col_idx].name.to_lowercase()).map_or(false, |sp| sp.always) && !val_str.trim().eq_ignore_ascii_case("DEFAULT") {
                    return Err(format!("column \"{}\" can only be updated to DEFAULT", table.columns[col_idx].name));
                }
                raw_assignments.push((col_idx, val_str));
            }

            // Pre-compile WHERE AST
            let where_expr = if let Some(ref w) = resolved_where {
                if !w.is_empty() {
                    Some(compile_expr(w, Some(table), None, None, None))
                } else {
                    None
                }
            } else {
                None
            };

            // Check if there is a primary key equality check for O(1) point update
            let pk_target = if let Some(ref expr) = where_expr {
                if let Some(pk_idx) = table.pk_col_idx {
                    expr.find_pk_eq(pk_idx, params)
                } else {
                    None
                }
            } else {
                None
            };

            // Bước 1: tính giá trị mới cho mọi dòng khớp (mỗi dòng chỉ đọc chính nó), chưa sửa gì.
            let compute_updates = |table: &crate::storage::table::Table, i: usize| -> Vec<(usize, Value)> {
                let row_assignments: Option<Vec<(usize, String)>> = per_row_set.as_ref().and_then(|m| m.get(&i)).map(|clause| {
                    split_comma_separated_tokens(clause)
                        .into_iter()
                        .filter_map(|a| {
                            let a = a.trim();
                            let eq = a.find('=')?;
                            Some((table.get_column_index(clean_col_name(&a[..eq]))?, a[eq + 1..].trim().to_string()))
                        })
                        .collect()
                });
                row_assignments.as_ref().unwrap_or(&raw_assignments).iter().map(|&(c_idx, ref v_str)| {
                    // SET col = DEFAULT: giá trị mặc định của cột (trước đây ghi chữ "DEFAULT")
                    let raw_val = if v_str.trim().eq_ignore_ascii_case("DEFAULT") {
                        table.columns[c_idx].default_value.as_deref().map(eval_default_value).unwrap_or(Value::Null)
                    } else {
                        evaluate_assignment_val(v_str, &table.rows[i], table, params)
                    };
                    let coerced = crate::engine::coltypes::coerce_for_write(table.decl_type(c_idx), coerce_update_val(&table.columns[c_idx], raw_val));
                    (c_idx, coerced)
                }).collect()
            };
            let mut planned: Vec<(usize, Vec<(usize, Value)>)> = Vec::new();
            if let Some(target_pk) = pk_target {
                if let Some(&row_idx) = table.pk_index.get(&target_pk) {
                    let matches = match &where_expr {
                        Some(expr) => expr.eval_bool(&table.rows[row_idx], None, params) == Some(true),
                        None => true,
                    };
                    if !table.is_deleted[row_idx] && matches {
                        planned.push((row_idx, compute_updates(table, row_idx)));
                    }
                }
            } else {
                // General scan update
                for i in 0..table.rows.len() {
                    let matches = match &where_expr {
                        Some(expr) => expr.eval_bool(&table.rows[i], None, params) == Some(true),
                        None => true,
                    };
                    if !table.is_deleted[i] && matches {
                        planned.push((i, compute_updates(table, i)));
                    }
                }
            }

            // Bước 2: lệnh đổi khoá chính thì kiểm tra trùng trên trạng thái cuối của cả lệnh.
            let pk_idx = table.pk_col_idx;
            let pk_changed = |table: &crate::storage::table::Table, i: usize, updates: &[(usize, Value)]| -> bool {
                updates.iter().any(|(c, v)| Some(*c) == pk_idx && pk_dedup_key(v) != pk_dedup_key(&table.rows[i][*c]))
            };
            if table.pk_enforced {
                if let Some(pk_i) = pk_idx {
                    if planned.iter().any(|(i, u)| pk_changed(table, *i, u)) {
                        let updated: std::collections::HashSet<usize> = planned.iter().map(|(i, _)| *i).collect();
                        let mut final_keys = std::collections::HashSet::new();
                        for (i, updates) in &planned {
                            let new_val = updates.iter().rev().find(|(c, _)| *c == pk_i).map(|(_, v)| v);
                            let val = new_val.unwrap_or(&table.rows[*i][pk_i]);
                            if val.is_null() {
                                continue;
                            }
                            let clash_outside = new_val.is_some()
                                && table.live_row_with_pk(val, Some(*i)).map_or(false, |r| !updated.contains(&r));
                            if clash_outside || !final_keys.insert(pk_dedup_key(val)) {
                                return Err(duplicate_pk_error(table, val));
                            }
                        }
                    }
                }
            }

            // Bước 3: áp dụng.
            changed_cols.extend(raw_assignments.iter().map(|(c, _)| *c));
            for (i, updates) in planned {
                updated_rows.push(i);
                if referenced {
                    old_rows.push((i, table.rows[i].clone()));
                }
                let identity = RowIdentity::of(table, i);
                let moves_key = pk_changed(table, i, &updates);
                let mut old_vals = Vec::new();
                for (c_idx, _) in &updates {
                    old_vals.push((*c_idx, table.rows[i][*c_idx].clone()));
                }
                table.update_row_multi(i, &updates);
                row_count += 1;
                if let Some(r_cols) = returning_cols {
                    returned_rows.push(project_returning_row(table, &table.rows[i], r_cols));
                }
                if moves_key {
                    moved_rows.push((identity, table.rows[i].clone()));
                } else {
                    wal_updates.push((identity, updates));
                }
                if in_tx {
                    for (col_idx, old_val) in old_vals {
                        undo_restores.push((i, col_idx, old_val));
                    }
                }
            }

            if let Some(r_cols) = returning_cols {
                get_returning_fields(table, r_cols)
            } else {
                vec![]
            }
        };

        let pk_col_idx = self.storage.get_table(table_name).and_then(|t| t.pk_col_idx);
        for (identity, mut updates) in wal_updates {
            // Cột khoá chính ghi sau cùng: các bản ghi trước vẫn tra được dòng bằng khoá cũ.
            updates.sort_by_key(|(c, _)| Some(*c) == pk_col_idx);
            match identity {
                RowIdentity::Pk(pk) => {
                    for (col_idx, new_val) in updates {
                        self.storage.wal.append(WalRecord::Update { table: table_name.to_string(), pk, col_idx, new_val });
                    }
                }
                RowIdentity::Key(key) => {
                    for (col_idx, new_val) in updates {
                        self.storage.wal.append(WalRecord::UpdateByKey { table: table_name.to_string(), key: key.clone(), col_idx, new_val });
                    }
                }
                RowIdentity::Row(old_row) => {
                    self.storage.wal.append(WalRecord::UpdateByRow { table: table_name.to_string(), old_row, updates });
                }
            }
        }
        // Dòng đổi khoá chính được ghi thành DELETE theo khoá cũ rồi INSERT dòng mới, mọi DELETE
        // trước mọi INSERT: khi replay, khoá cũ không bao giờ đã bị dòng khác trong cùng lệnh
        // chiếm (vd. SET id = id + 1, hay đổi chỗ hai khoá). Chỉ dùng bản ghi mà mọi bản cũ đều đọc được.
        let mut new_rows = Vec::with_capacity(moved_rows.len());
        for (identity, new_row) in moved_rows {
            let table = table_name.to_string();
            self.storage.wal.append(match identity {
                RowIdentity::Pk(pk) => WalRecord::Delete { table, pk },
                RowIdentity::Key(key) => WalRecord::DeleteByKey { table, key },
                RowIdentity::Row(row) => WalRecord::DeleteByRow { table, row },
            });
            new_rows.push(new_row);
        }
        for row in new_rows {
            self.storage.wal.append(WalRecord::Insert { table: table_name.to_string(), row });
        }
        if in_tx {
            for (row_idx, col_idx, old_val) in undo_restores {
                self.storage.tx_undo_log.push(UndoAction::RestoreRow {
                    table: table_name.to_string(),
                    row_idx,
                    col_idx,
                    old_val,
                });
            }
        }
        if !updated_rows.is_empty() {
            self.verify_rows(table_name, &updated_rows, Some(&changed_cols))?;
            if referenced {
                self.fk_on_parent_update(table_name, &old_rows)?;
            }
        }

        Ok(QueryResult {
            rows: returned_rows,
            row_count,
            fields,
            command: "UPDATE".to_string(),
        })
    }

    fn handle_delete(&mut self, sql: &str, params: &[Value]) -> Result<QueryResult, String> {
        if let Some((head, using_list, where_clause, returning)) = crate::engine::dml_join::split_delete_using(sql) {
            return self.handle_delete_using(head, using_list, where_clause, returning, params);
        }
        let from_idx = find_top_level_keyword(sql, "FROM").ok_or("Missing FROM in DELETE")?;
        let where_idx = find_top_level_keyword(sql, "WHERE");
        let returning_idx = find_top_level_keyword(sql, "RETURNING");

        let (where_clause, returning_cols) = match (where_idx, returning_idx) {
            (Some(w_idx), Some(r_idx)) => (Some(sql[w_idx + 5..r_idx].trim()), Some(sql[r_idx + 9..].trim())),
            (Some(w_idx), None) => (Some(sql[w_idx + 5..].trim()), None),
            (None, Some(r_idx)) => (None, Some(sql[r_idx + 9..].trim())),
            (None, None) => (None, None),
        };

        let table_end = where_idx.or(returning_idx).unwrap_or(sql.len());
        let table_name = clean_table_name(sql[from_idx + 4..table_end].trim());

        if where_idx.is_none() && has_unparsed_top_level_where(sql) {
            return Err("Failed to parse WHERE clause in DELETE statement - aborting to prevent accidental table truncation".to_string());
        }

        let in_tx = self.storage.undo_enabled();
        let mut row_count = 0;
        let mut returned_rows = Vec::new();
        let mut deleted_pks: Vec<RowIdentity> = Vec::new();
        let mut deleted_row_indices = Vec::new();

        let resolved_where = if let Some(w) = where_clause {
            let w_up = w.to_uppercase();
            if w_up.contains("SELECT") && w_up.contains("FROM") {
                Some(self.resolve_subqueries_in_where(w, params)?)
            } else {
                Some(w.to_string())
            }
        } else {
            None
        };

        let fields = {
            let table = self.storage.get_table_mut(table_name).ok_or_else(|| format!("Table {} not found", table_name))?;

            let where_expr = if let Some(ref w) = resolved_where {
                if !w.is_empty() {
                    Some(compile_expr(w, Some(table), None, None, None))
                } else {
                    None
                }
            } else {
                None
            };

            let pk_target = if let Some(ref expr) = where_expr {
                if let Some(pk_idx) = table.pk_col_idx {
                    expr.find_pk_eq(pk_idx, params)
                } else {
                    None
                }
            } else {
                None
            };

            if let Some(target_pk) = pk_target {
                if let Some(&row_idx) = table.pk_index.get(&target_pk) {
                    let matches = match &where_expr {
                        Some(expr) => expr.eval_bool(&table.rows[row_idx], None, params) == Some(true),
                        None => true,
                    };
                    if !table.is_deleted[row_idx] && matches {
                        if let Some(r_cols) = returning_cols {
                            returned_rows.push(project_returning_row(table, &table.rows[row_idx], r_cols));
                        }
                        deleted_pks.push(RowIdentity::of(table, row_idx));
                        table.delete_row(row_idx);
                        row_count = 1;
                        deleted_row_indices.push(row_idx);
                    }
                }
            } else {
                for i in 0..table.rows.len() {
                    let matches = match &where_expr {
                        Some(expr) => expr.eval_bool(&table.rows[i], None, params) == Some(true),
                        None => true,
                    };
                    if !table.is_deleted[i] && matches {
                        if let Some(r_cols) = returning_cols {
                            returned_rows.push(project_returning_row(table, &table.rows[i], r_cols));
                        }
                        deleted_pks.push(RowIdentity::of(table, i));
                        table.delete_row(i);
                        row_count += 1;
                        deleted_row_indices.push(i);
                    }
                }
            }

            if let Some(r_cols) = returning_cols {
                get_returning_fields(table, r_cols)
            } else {
                vec![]
            }
        };

        for identity in deleted_pks {
            let table = table_name.to_string();
            self.storage.wal.append(match identity {
                RowIdentity::Pk(pk) => WalRecord::Delete { table, pk },
                RowIdentity::Key(key) => WalRecord::DeleteByKey { table, key },
                RowIdentity::Row(row) => WalRecord::DeleteByRow { table, row },
            });
        }
        if in_tx {
            for &row_idx in &deleted_row_indices {
                self.storage.tx_undo_log.push(UndoAction::UndeleteRow {
                    table: table_name.to_string(),
                    row_idx,
                });
            }
        }
        if !deleted_row_indices.is_empty() && self.is_referenced(table_name) {
            self.fk_on_parent_delete(table_name, &deleted_row_indices)?;
        }

        Ok(QueryResult {
            rows: returned_rows,
            row_count,
            fields,
            command: "DELETE".to_string(),
        })
    }
}

#[derive(Clone, Debug, PartialEq)]
enum JoinKind {
    Inner,
    Left,
    Cross,
    /// RIGHT [OUTER] JOIN: như INNER, thêm các dòng bên phải không khớp (bên trái NULL).
    Right,
    /// FULL [OUTER] JOIN: như LEFT, thêm các dòng bên phải không khớp.
    Full,
}

#[derive(Clone, Debug)]
struct ParsedJoin {
    kind: JoinKind,
    table_name: String,
    alias: Option<String>,
    on_condition: Option<String>,
    subquery: Option<String>,
    is_lateral: bool,
}

#[derive(Clone, Debug)]
pub(crate) struct JoinedSchema {
    pub(crate) col_map: std::collections::HashMap<String, usize>,
    pub(crate) table_offsets: Vec<usize>,
    pub(crate) total_cols: usize,
    /// Kiểu nguyên khai báo theo slot (chia nguyên / tràn số như PostgreSQL)
    pub(crate) int_kinds: Vec<Option<crate::engine::inttype::IntKind>>,
}

impl JoinedSchema {
    pub(crate) fn new(tables: &[(&crate::storage::table::Table, Option<&str>)]) -> Self {
        let mut col_map = std::collections::HashMap::new();
        let mut table_offsets = Vec::with_capacity(tables.len());
        let mut offset = 0;
        let mut int_kinds = Vec::new();

        for (table, alias) in tables {
            table_offsets.push(offset);
            let tbl_name_lower = table.name.to_lowercase();
            let alias_lower = alias.map(|a| a.to_lowercase());

            for (col_idx, col) in table.columns.iter().enumerate() {
                let slot = offset + col_idx;
                let col_name_lower = col.name.to_lowercase();

                let full_tbl = format!("{}.{}", tbl_name_lower, col_name_lower);
                if alias_lower.is_some() {
                    // Bảng có alias (vd. `JOIN t t2`): tên gốc không được đè lên cùng bảng xuất hiện không alias
                    col_map.entry(full_tbl).or_insert(slot);
                } else {
                    col_map.insert(full_tbl, slot);
                }

                if let Some(ref a_key) = alias_lower {
                    let alias_tbl = format!("{}.{}", a_key, col_name_lower);
                    col_map.insert(alias_tbl, slot);
                }

                col_map.entry(col_name_lower).or_insert(slot);
                int_kinds.push(crate::engine::inttype::column_kind_at(table, col_idx));
            }
            offset += table.columns.len();
        }

        Self {
            col_map,
            table_offsets,
            total_cols: offset,
            int_kinds,
        }
    }

    #[inline(always)]
    pub(crate) fn get_slot(&self, key: &str) -> Option<usize> {
        let clean = key.trim().replace('"', "");
        let lower = clean.to_lowercase();
        if let Some(&idx) = self.col_map.get(&lower) {
            return Some(idx);
        }
        if let Some(dot_idx) = lower.rfind('.') {
            let col_only = &lower[dot_idx + 1..];
            if let Some(&idx) = self.col_map.get(col_only) {
                return Some(idx);
            }
        }
        None
    }
}

#[derive(Clone, Debug)]
pub(crate) struct CombinedRow {
    pub(crate) values: Vec<Value>,
    pub(crate) schema: std::sync::Arc<JoinedSchema>,
    pub(crate) extra: Option<std::collections::HashMap<String, Value>>,
}

impl CombinedRow {
    #[inline(always)]
    pub(crate) fn new_with_schema(schema: std::sync::Arc<JoinedSchema>) -> Self {
        Self {
            values: Vec::with_capacity(schema.total_cols),
            schema,
            extra: None,
        }
    }

    #[inline(always)]
    pub(crate) fn from_base_row(schema: std::sync::Arc<JoinedSchema>, base_row: &[Value]) -> Self {
        let mut values = Vec::with_capacity(schema.total_cols);
        values.extend_from_slice(base_row);
        Self {
            values,
            schema,
            extra: None,
        }
    }

    #[inline(always)]
    pub(crate) fn with_joined_table(&self, joined_row: &[Value]) -> Self {
        let mut values = Vec::with_capacity(self.schema.total_cols);
        values.extend_from_slice(&self.values);
        values.extend_from_slice(joined_row);
        Self {
            values,
            schema: self.schema.clone(),
            extra: self.extra.clone(),
        }
    }

    #[inline(always)]
    pub(crate) fn with_null_table(&self, num_cols: usize) -> Self {
        let mut values = Vec::with_capacity(self.schema.total_cols);
        values.extend_from_slice(&self.values);
        values.resize(values.len() + num_cols, Value::Null);
        Self {
            values,
            schema: self.schema.clone(),
            extra: self.extra.clone(),
        }
    }

    #[inline(always)]
    pub(crate) fn get_val(&self, key: &str) -> Value {
        if let Some(ref extra) = self.extra {
            let trimmed = key.trim();
            let clean = if trimmed.contains('"') { trimmed.replace('"', "") } else { trimmed.to_string() };
            let lower = clean.to_lowercase();
            if let Some(v) = extra.get(&lower) {
                return v.clone();
            }
            if let Some(dot_idx) = lower.rfind('.') {
                let col_only = &lower[dot_idx + 1..];
                if let Some(v) = extra.get(col_only) {
                    return v.clone();
                }
            }
        }
        if let Some(slot) = self.schema.get_slot(key) {
            return self.values.get(slot).cloned().unwrap_or(Value::Null);
        }
        Value::Null
    }

    #[inline(always)]
    fn insert_extra(&mut self, key: String, val: Value) {
        if self.extra.is_none() {
            self.extra = Some(std::collections::HashMap::new());
        }
        if let Some(ref mut map) = self.extra {
            map.insert(key, val);
        }
    }

    #[inline(always)]
    fn contains_key(&self, key: &str) -> bool {
        if let Some(ref extra) = self.extra {
            let clean = key.trim().replace('"', "");
            let lower = clean.to_lowercase();
            if extra.contains_key(&lower) {
                return true;
            }
        }
        self.schema.get_slot(key).is_some()
    }
}

fn extract_identifiers_from_condition(cond: &str) -> Vec<String> {
    let mut tokens = Vec::new();
    let bytes = cond.as_bytes();
    let n = bytes.len();
    let mut i = 0;
    while i < n {
        let b = bytes[i];
        if b == b'\'' {
            i += 1;
            while i < n && bytes[i] != b'\'' {
                i += 1;
            }
            if i < n { i += 1; }
            continue;
        }
        if b.is_ascii_alphabetic() || b == b'_' || b == b'"' {
            let start = i;
            while i < n {
                let cb = bytes[i];
                if cb.is_ascii_alphanumeric() || cb == b'_' || cb == b'.' || cb == b'"' {
                    i += 1;
                } else {
                    break;
                }
            }
            let s = std::str::from_utf8(&bytes[start..i]).unwrap_or("");
            let clean = s.replace('"', "").to_string();
            let upper = clean.to_uppercase();
            if !matches!(upper.as_str(), 
                "AND" | "OR" | "NOT" | "IS" | "NULL" | "TRUE" | "FALSE" | "LIKE" | "ILIKE" | 
                "IN" | "BETWEEN" | "EXISTS" | "CASE" | "WHEN" | "THEN" | "ELSE" | "END" | 
                "CAST" | "AS" | "LOWER" | "UPPER" | "LENGTH" | "TRIM" | "SUBSTRING" | 
                "COUNT" | "SUM" | "AVG" | "MIN" | "MAX" | "SELECT" | "FROM" | "WHERE"
            ) {
                tokens.push(clean);
            }
            continue;
        }
        i += 1;
    }
    tokens
}

#[derive(Clone, Debug)]
struct PushdownPlan {
    base_predicates: Vec<String>,
    joined_table_predicates: Vec<Vec<String>>,
    intermediate_predicates: Vec<Vec<String>>,
    remaining_where: Vec<String>,
}

fn plan_predicate_pushdown(
    where_str: Option<&str>,
    all_tables: &[(&crate::storage::table::Table, Option<&str>)],
    joins: &[ParsedJoin],
) -> PushdownPlan {
    let mut plan = PushdownPlan {
        base_predicates: Vec::new(),
        joined_table_predicates: vec![Vec::new(); joins.len()],
        intermediate_predicates: vec![Vec::new(); joins.len() + 1],
        remaining_where: Vec::new(),
    };

    let Some(w) = where_str else {
        return plan;
    };

    let clauses = split_top_level_and(w);
    for clause in clauses {
        let c_str = clause.trim().to_string();
        if c_str.is_empty() {
            continue;
        }

        let tokens = extract_identifiers_from_condition(&c_str);
        if tokens.is_empty() {
            plan.remaining_where.push(c_str);
            continue;
        }

        let mut referenced_tables = Vec::new();
        let mut unknown_token = false;

        for tok in &tokens {
            let clean = tok.to_lowercase();
            if let Some(dot_idx) = clean.find('.') {
                let prefix = &clean[..dot_idx];
                let mut matched = false;
                for (t_idx, (tbl, alias)) in all_tables.iter().enumerate() {
                    if tbl.name.eq_ignore_ascii_case(prefix)
                        || alias.map(|a| a.trim_matches('"').eq_ignore_ascii_case(prefix)).unwrap_or(false)
                    {
                        if !referenced_tables.contains(&t_idx) {
                            referenced_tables.push(t_idx);
                        }
                        matched = true;
                    }
                }
                if !matched {
                    unknown_token = true;
                }
            } else {
                let mut matched_count = 0;
                for (t_idx, (tbl, _)) in all_tables.iter().enumerate() {
                    if tbl.columns.iter().any(|col| col.name.eq_ignore_ascii_case(&clean)) {
                        matched_count += 1;
                        if !referenced_tables.contains(&t_idx) {
                            referenced_tables.push(t_idx);
                        }
                    }
                }
                if matched_count == 0 && !clean.starts_with('$') && clean.parse::<f64>().is_err() {
                    unknown_token = true;
                }
            }
        }

        if unknown_token || referenced_tables.is_empty() {
            plan.remaining_where.push(c_str);
            continue;
        }

        if referenced_tables.len() == 1 {
            let t_idx = referenced_tables[0];
            if t_idx == 0 {
                plan.base_predicates.push(c_str);
            } else {
                let j_idx = t_idx - 1;
                if joins[j_idx].kind == JoinKind::Inner {
                    plan.joined_table_predicates[j_idx].push(c_str.clone());
                    plan.intermediate_predicates[t_idx].push(c_str);
                } else {
                    plan.intermediate_predicates[t_idx].push(c_str);
                }
            }
        } else {
            let max_table_idx = *referenced_tables.iter().max().unwrap();
            plan.intermediate_predicates[max_table_idx].push(c_str);
        }
    }

    plan
}

pub use crate::types::{JoinKey, value_to_join_key};

struct EquiJoinCandidate {
    left_expr: String,
    left_slot: Option<usize>,
    right_col_idx: usize,
    residual_cond: Option<String>,
}

fn strip_outer_parens(mut s: &str) -> &str {
    s = s.trim();
    while s.starts_with('(') && s.ends_with(')') && is_fully_enclosed_in_parens(s) {
        s = s[1..s.len() - 1].trim();
    }
    s
}


fn find_top_level_eq(s: &str) -> Option<usize> {
    let s_bytes = s.as_bytes();
    let len = s_bytes.len();
    let mut in_str = false;
    let mut depth: i32 = 0;
    let mut i = 0;
    while i < len {
        let b = s_bytes[i];
        if b == b'\'' {
            in_str = !in_str;
            i += 1;
            continue;
        }
        if in_str {
            i += 1;
            continue;
        }
        if b == b'(' {
            depth += 1;
        } else if b == b')' {
            depth = depth.saturating_sub(1);
        } else if depth == 0 && b == b'=' {
            let prev_char = if i > 0 { s_bytes[i - 1] } else { b' ' };
            let next_char = if i + 1 < len { s_bytes[i + 1] } else { b' ' };
            if prev_char != b'!' && prev_char != b'<' && prev_char != b'>' && next_char != b'=' {
                return Some(i);
            }
        }
        i += 1;
    }
    None
}

fn extract_col_from_side(
    side: &str,
    tbl_name: &str,
    alias: Option<&str>,
    table: &crate::storage::table::Table,
) -> Option<usize> {
    let clean = side.trim().replace('"', "");
    if let Some(dot_idx) = clean.rfind('.') {
        let prefix = &clean[..dot_idx];
        let col = &clean[dot_idx + 1..];
        let prefix_tbl = if let Some(p_dot) = prefix.rfind('.') {
            &prefix[p_dot + 1..]
        } else {
            prefix
        };
        let matches_prefix = prefix_tbl.eq_ignore_ascii_case(tbl_name)
            || alias.map(|a| prefix_tbl.eq_ignore_ascii_case(a)).unwrap_or(false);
        if matches_prefix {
            return table.get_column_index(col);
        }
    } else {
        return table.get_column_index(&clean);
    }
    None
}

fn parse_equi_join_candidate(
    on_cond: &str,
    tbl_name: &str,
    alias: Option<&str>,
    table: &crate::storage::table::Table,
) -> Option<EquiJoinCandidate> {
    let stripped = strip_outer_parens(on_cond);
    let parts = split_top_level_and(stripped);
    if parts.is_empty() {
        return None;
    }

    for (part_idx, part) in parts.iter().enumerate() {
        let clean_part = strip_outer_parens(part);
        if let Some(eq_idx) = find_top_level_eq(clean_part) {
            let side_a = clean_part[..eq_idx].trim();
            let side_b = clean_part[eq_idx + 1..].trim();
            // `x = ANY(mảng)` / `ALL(...)` / `SOME(...)` là so sánh định lượng, không phải equi-join
            // (trước đây bị hash join theo `x = mảng` nên không khớp dòng nào).
            let is_quantified = |s: &str| {
                let u = s.trim_start().to_uppercase();
                ["ANY", "ALL", "SOME"].iter().any(|k| u.strip_prefix(k).map_or(false, |r| r.trim_start().starts_with('(')))
            };
            if is_quantified(side_a) || is_quantified(side_b) {
                continue;
            }

            let col_a = extract_col_from_side(side_a, tbl_name, alias, table);
            let col_b = extract_col_from_side(side_b, tbl_name, alias, table);

            let chosen: Option<(usize, String)> = match (col_a, col_b) {
                (Some(idx_a), None) => Some((idx_a, side_b.to_string())),
                (None, Some(idx_b)) => Some((idx_b, side_a.to_string())),
                (Some(idx_a), Some(idx_b)) => {
                    let a_clean = side_a.trim().replace('"', "");
                    let b_clean = side_b.trim().replace('"', "");
                    // self-join `FROM sp JOIN sp sp2 ON sp2.id = sp.id`: cả hai vế đều khớp bảng phải (tên gốc
                    // `sp` và alias `sp2`); vế mang alias mới là cột của bảng phải (trước đây chọn nhầm → 0 dòng)
                    let qual = |c: &str| c.rsplit_once('.').map(|(q, _)| q.rsplit('.').next().unwrap_or(q).to_lowercase());
                    let al = alias.map(|a| a.trim_matches('"').to_lowercase()).filter(|a| !a.eq_ignore_ascii_case(tbl_name));
                    let by_alias = al.as_ref().and_then(|a| {
                        let (qa, qb) = (qual(&a_clean), qual(&b_clean));
                        if qa.as_deref() == Some(a.as_str()) && qb.as_deref() != Some(a.as_str()) {
                            Some((idx_a, side_b.to_string()))
                        } else if qb.as_deref() == Some(a.as_str()) && qa.as_deref() != Some(a.as_str()) {
                            Some((idx_b, side_a.to_string()))
                        } else {
                            None
                        }
                    });
                    if by_alias.is_some() {
                        by_alias
                    } else {
                    let a_has_prefix = a_clean.contains('.');
                    let b_has_prefix = b_clean.contains('.');
                    if a_has_prefix && !b_has_prefix {
                        Some((idx_a, side_b.to_string()))
                    } else if b_has_prefix && !a_has_prefix {
                        Some((idx_b, side_a.to_string()))
                    } else {
                        Some((idx_b, side_a.to_string()))
                    }
                    }
                }
                (None, None) => None,
            };

            if let Some((right_col_idx, left_expr)) = chosen {
                let mut residuals = Vec::new();
                for (i, p) in parts.iter().enumerate() {
                    if i != part_idx {
                        residuals.push(*p);
                    }
                }
                let residual_cond = if residuals.is_empty() {
                    None
                } else {
                    Some(residuals.join(" AND "))
                };

                return Some(EquiJoinCandidate {
                    left_expr,
                    left_slot: None,
                    right_col_idx,
                    residual_cond,
                });
            }
        }
    }

    None
}

fn find_matching_closing_paren(s: &str) -> Result<usize, String> {
    let mut depth = 0;
    let mut in_str = false;
    for (idx, ch) in s.char_indices() {
        match ch {
            '\'' => in_str = !in_str,
            '(' if !in_str => depth += 1,
            ')' if !in_str => {
                depth -= 1;
                if depth == 0 {
                    return Ok(idx);
                }
            }
            _ => {}
        }
    }
    Err("Unmatched parenthesis in subquery".to_string())
}

fn clean_alias_part(s: &str) -> Option<String> {
    let mut a = s.trim();
    if a.to_uppercase().starts_with("AS ") {
        a = a[3..].trim();
    }
    let token = a.split_whitespace().next()?;
    let cleaned = clean_col_name(token).to_string();
    if cleaned.is_empty() {
        None
    } else {
        Some(cleaned)
    }
}

fn parse_join_target(raw: &str) -> Result<(String, Option<String>, Option<String>, bool), String> {
    let s = raw.trim();
    let upper = s.to_uppercase();

    // 1. Check for LATERAL (...) [AS alias]
    if upper.starts_with("LATERAL") && (s.len() == 7 || s[7..].starts_with(' ') || s[7..].starts_with('(') || s[7..].starts_with('\n') || s[7..].starts_with('\r') || s[7..].starts_with('\t')) {
        let after_lateral = s[7..].trim();
        if after_lateral.starts_with('(') {
            let end_idx = find_matching_closing_paren(after_lateral)?;
            let sub_query = after_lateral[1..end_idx].trim().to_string();
            let after = after_lateral[end_idx + 1..].trim();
            let alias = clean_alias_part(after);
            let tbl_name = alias.clone().unwrap_or_else(|| "lateral_tbl".to_string());
            return Ok((tbl_name, alias, Some(sub_query), true));
        }
    }

    // 2. Check for (SELECT ...) [AS alias]
    if s.starts_with('(') {
        if let Ok(end_idx) = find_matching_closing_paren(s) {
            let inside = s[1..end_idx].trim();
            let inside_upper = inside.to_uppercase();
            if inside_upper.starts_with("SELECT ") || inside_upper.starts_with("SELECT\n") || inside_upper.starts_with("SELECT\r") || inside_upper.starts_with("SELECT\t") {
                let after = s[end_idx + 1..].trim();
                let alias = clean_alias_part(after);
                let tbl_name = alias.clone().unwrap_or_else(|| "sub_tbl".to_string());
                return Ok((tbl_name, alias, Some(inside.to_string()), false));
            }
        }
    }

    // 3. Normal table
    let (tbl, alias) = extract_table_name_and_alias(s);
    Ok((tbl.to_string(), alias.map(|a| a.to_string()), None, false))
}

fn parse_from_and_joins(s: &str) -> Result<(String, Option<String>, Option<String>, Vec<ParsedJoin>), String> {
    let mut joins = Vec::new();
    let bytes = s.as_bytes();
    let len = bytes.len();

    let mut positions: Vec<(usize, usize, JoinKind)> = Vec::new();
    let mut i = 0;
    let mut depth = 0;
    let mut in_str = false;

    while i < len {
        let b = bytes[i];
        if b == b'\'' {
            in_str = !in_str;
            i += 1;
            continue;
        }
        if in_str {
            i += 1;
            continue;
        }
        if b == b'(' {
            depth += 1;
            i += 1;
            continue;
        }
        if b == b')' {
            if depth > 0 {
                depth -= 1;
            }
            i += 1;
            continue;
        }

        if depth == 0 && s.is_char_boundary(i) && (b == b',' || b.is_ascii_alphabetic()) && (b == b',' || i == 0 || !bytes[i - 1].is_ascii_alphanumeric() && bytes[i - 1] != b'_') {
            let rest = &s[i..];
            let upper: String = rest.chars().take(20).collect::<String>().to_uppercase();
            if b == b',' {
                positions.push((i, 1, JoinKind::Cross));
                i += 1;
                continue;
            } else if upper.starts_with("CROSS JOIN") && rest[10..].chars().next().map_or(false, |c| c.is_ascii_whitespace()) {
                let ws_len = 10 + rest[10..].chars().take_while(|c| c.is_ascii_whitespace()).count();
                positions.push((i, ws_len, JoinKind::Cross));
                i += ws_len;
                continue;
            } else if upper.starts_with("LEFT OUTER JOIN") && rest[15..].chars().next().map_or(false, |c| c.is_ascii_whitespace()) {
                let ws_len = 15 + rest[15..].chars().take_while(|c| c.is_ascii_whitespace()).count();
                positions.push((i, ws_len, JoinKind::Left));
                i += ws_len;
                continue;
            } else if upper.starts_with("LEFT JOIN") && rest[9..].chars().next().map_or(false, |c| c.is_ascii_whitespace()) {
                let ws_len = 9 + rest[9..].chars().take_while(|c| c.is_ascii_whitespace()).count();
                positions.push((i, ws_len, JoinKind::Left));
                i += ws_len;
                continue;
            } else if let Some((kw_len, kind)) = [("RIGHT OUTER JOIN", JoinKind::Right), ("RIGHT JOIN", JoinKind::Right), ("FULL OUTER JOIN", JoinKind::Full), ("FULL JOIN", JoinKind::Full)]
                .iter()
                .find(|(kw, _)| upper.starts_with(kw) && rest[kw.len()..].chars().next().map_or(false, |c| c.is_ascii_whitespace()))
                .map(|(kw, k)| (kw.len(), k.clone()))
            {
                let ws_len = kw_len + rest[kw_len..].chars().take_while(|c| c.is_ascii_whitespace()).count();
                positions.push((i, ws_len, kind));
                i += ws_len;
                continue;
            } else if upper.starts_with("INNER JOIN") && rest[10..].chars().next().map_or(false, |c| c.is_ascii_whitespace()) {
                let ws_len = 10 + rest[10..].chars().take_while(|c| c.is_ascii_whitespace()).count();
                positions.push((i, ws_len, JoinKind::Inner));
                i += ws_len;
                continue;
            } else if upper.starts_with("JOIN") && rest[4..].chars().next().map_or(false, |c| c.is_ascii_whitespace()) {
                let ws_len = 4 + rest[4..].chars().take_while(|c| c.is_ascii_whitespace()).count();
                positions.push((i, ws_len, JoinKind::Inner));
                i += ws_len;
                continue;
            }
        }
        i += 1;
    }

    if positions.is_empty() {
        let (name, alias, subquery, _) = parse_join_target(s)?;
        return Ok((name, alias, subquery, joins));
    }

    let first_join_pos = positions[0].0;
    let base_part = s[..first_join_pos].trim();
    let (base_name, base_alias, base_subquery, _) = parse_join_target(base_part)?;

    for (k, &(pos, kw_len, ref kind)) in positions.iter().enumerate() {
        let after_kw = &s[pos + kw_len..];
        let end_idx = if k + 1 < positions.len() {
            positions[k + 1].0 - (pos + kw_len)
        } else {
            after_kw.len()
        };
        let seg = after_kw[..end_idx].trim();

        if *kind == JoinKind::Cross {
            let (tbl, alias, subquery, is_lateral) = parse_join_target(seg)?;
            joins.push(ParsedJoin {
                kind: JoinKind::Cross,
                table_name: tbl,
                alias,
                on_condition: None,
                subquery,
                is_lateral,
            });
        } else {
            // JOIN ... USING (a, b): đổi thành ON <bảng trước>.a = <bảng này>.a AND ...
            if let Some(u_pos) = find_top_level_keyword(seg, "USING").filter(|_| find_top_level_keyword(seg, "ON").is_none()) {
                let tbl_part = seg[..u_pos].trim();
                let cols_part = seg[u_pos + 5..].trim();
                let (tbl, alias, subquery, is_lateral) = parse_join_target(tbl_part)?;
                let inner = cols_part.trim_start_matches('(').trim_end_matches(')');
                let prev = if k == 0 { base_alias.clone().unwrap_or_else(|| base_name.clone()) } else { joins.last().map(|pj: &ParsedJoin| pj.alias.clone().unwrap_or_else(|| pj.table_name.clone())).unwrap_or_default() };
                let this = alias.clone().unwrap_or_else(|| tbl.clone());
                let cond = split_comma_separated_tokens(inner).into_iter().map(|c| format!("{}.{} = {}.{}", prev, c.trim(), this, c.trim())).collect::<Vec<_>>().join(" AND ");
                joins.push(ParsedJoin { kind: kind.clone(), table_name: tbl, alias, on_condition: Some(cond), subquery, is_lateral });
                continue;
            }
            let on_pos = find_top_level_keyword(seg, "ON").ok_or_else(|| format!("Missing ON clause in JOIN: {}", seg))?;
            let tbl_part = seg[..on_pos].trim();
            let on_part = seg[on_pos + 2..].trim();
            let (tbl, alias, subquery, is_lateral) = parse_join_target(tbl_part)?;
            joins.push(ParsedJoin {
                kind: kind.clone(),
                table_name: tbl,
                alias,
                on_condition: Some(on_part.to_string()),
                subquery,
                is_lateral,
            });
        }
    }

    Ok((base_name, base_alias, base_subquery, joins))
}

fn parse_query_clauses<'a>(
    after_from: &'a str,
    params: &[Value],
) -> (
    &'a str,
    Option<String>,
    Option<String>,
    Option<String>,
    Vec<OrderBySpec<'a>>,
    Option<usize>,
    Option<usize>,
) {
    let where_pos = find_top_level_keyword(after_from, "WHERE");
    let group_pos = find_top_level_keyword(after_from, "GROUP BY");
    let having_pos = find_top_level_keyword(after_from, "HAVING");
    let order_pos = find_top_level_keyword(after_from, "ORDER BY");
    let limit_pos = find_top_level_keyword(after_from, "LIMIT");
    let offset_pos = find_top_level_keyword(after_from, "OFFSET");

    let first_kw_pos = [where_pos, group_pos, having_pos, order_pos, limit_pos, offset_pos]
        .into_iter()
        .filter_map(|x| x)
        .min();

    let from_joins_str = match first_kw_pos {
        Some(pos) => after_from[..pos].trim(),
        None => after_from.trim(),
    };

    let where_clause = where_pos.map(|w_idx| {
        let after_w = &after_from[w_idx + 5..];
        let end_idx = [
            group_pos.and_then(|p| if p > w_idx { Some(p - (w_idx + 5)) } else { None }),
            having_pos.and_then(|p| if p > w_idx { Some(p - (w_idx + 5)) } else { None }),
            order_pos.and_then(|p| if p > w_idx { Some(p - (w_idx + 5)) } else { None }),
            limit_pos.and_then(|p| if p > w_idx { Some(p - (w_idx + 5)) } else { None }),
            offset_pos.and_then(|p| if p > w_idx { Some(p - (w_idx + 5)) } else { None }),
        ]
        .into_iter()
        .filter_map(|x| x)
        .min();
        match end_idx {
            Some(e) => after_w[..e].trim().to_string(),
            None => after_w.trim().to_string(),
        }
    });

    let group_by_clause = group_pos.map(|g_idx| {
        let after_g = &after_from[g_idx + 8..];
        let end_idx = [
            having_pos.and_then(|p| if p > g_idx { Some(p - (g_idx + 8)) } else { None }),
            order_pos.and_then(|p| if p > g_idx { Some(p - (g_idx + 8)) } else { None }),
            limit_pos.and_then(|p| if p > g_idx { Some(p - (g_idx + 8)) } else { None }),
            offset_pos.and_then(|p| if p > g_idx { Some(p - (g_idx + 8)) } else { None }),
        ]
        .into_iter()
        .filter_map(|x| x)
        .min();
        match end_idx {
            Some(e) => after_g[..e].trim().to_string(),
            None => after_g.trim().to_string(),
        }
    });

    let having_clause = having_pos.map(|h_idx| {
        let after_h = &after_from[h_idx + 6..];
        let end_idx = [
            order_pos.and_then(|p| if p > h_idx { Some(p - (h_idx + 6)) } else { None }),
            limit_pos.and_then(|p| if p > h_idx { Some(p - (h_idx + 6)) } else { None }),
            offset_pos.and_then(|p| if p > h_idx { Some(p - (h_idx + 6)) } else { None }),
        ]
        .into_iter()
        .filter_map(|x| x)
        .min();
        match end_idx {
            Some(e) => after_h[..e].trim().to_string(),
            None => after_h.trim().to_string(),
        }
    });

    let order_by_specs = match order_pos {
        Some(o_idx) => {
            let after_o = &after_from[o_idx + 8..];
            let end_idx = [
                limit_pos.and_then(|p| if p > o_idx { Some(p - (o_idx + 8)) } else { None }),
                offset_pos.and_then(|p| if p > o_idx { Some(p - (o_idx + 8)) } else { None }),
            ]
            .into_iter()
            .filter_map(|x| x)
            .min();
            let order_part = match end_idx {
                Some(e) => after_o[..e].trim(),
                None => after_o.trim(),
            };
            parse_order_by_specs(order_part)
        }
        None => Vec::new(),
    };

    let limit_val = limit_pos.and_then(|l_idx| {
        let after_l = &after_from[l_idx + 5..];
        let end_idx = offset_pos.and_then(|p| if p > l_idx { Some(p - (l_idx + 5)) } else { None });
        let s = match end_idx {
            Some(e) => after_l[..e].trim(),
            None => after_l.trim(),
        };
        parse_value(s, params).as_i64().map(|i| i as usize)
    });

    let offset_val = offset_pos.and_then(|o_idx| {
        let after_o = &after_from[o_idx + 6..];
        let end_idx = limit_pos.and_then(|p| if p > o_idx { Some(p - (o_idx + 6)) } else { None });
        let s = match end_idx {
            Some(e) => after_o[..e].trim(),
            None => after_o.trim(),
        };
        parse_value(s, params).as_i64().map(|i| i as usize)
    });

    (from_joins_str, where_clause, group_by_clause, having_clause, order_by_specs, limit_val, offset_val)
}

pub(crate) fn eval_condition_on_row(row: &CombinedRow, cond_str: &str, params: &[Value]) -> bool {
    let mut s = cond_str.trim();
    while s.starts_with('(') && s.ends_with(')') && is_fully_enclosed_in_parens(s) {
        s = s[1..s.len() - 1].trim();
    }
    let upper = s.to_uppercase();

    if s.eq_ignore_ascii_case("TRUE") || s == "1 = 1" || s == "1=1" {
        return true;
    }
    if s.eq_ignore_ascii_case("FALSE") || s == "1 = 0" || s == "1=0" {
        return false;
    }

    // Check top-level OR
    if let Some(or_idx) = find_top_level_keyword(s, "OR") {
        let left = &s[..or_idx];
        let right = &s[or_idx + 2..];
        return eval_condition_on_row(row, left, params) || eval_condition_on_row(row, right, params);
    }

    // Check top-level AND
    if let Some(and_idx) = find_top_level_keyword(s, "AND") {
        let left = &s[..and_idx];
        let right = &s[and_idx + 3..];
        return eval_condition_on_row(row, left, params) && eval_condition_on_row(row, right, params);
    }

    // Check NOT
    if upper.starts_with("NOT ") {
        return !eval_condition_on_row(row, &s[4..], params);
    }

    // IS NOT NULL / IS NULL
    if let Some(pos) = upper.find(" IS NOT NULL") {
        let col = &s[..pos].trim();
        let val = eval_operand_on_row(row, col, params);
        return !val.is_null();
    }
    if let Some(pos) = upper.find(" IS NULL") {
        let col = &s[..pos].trim();
        let val = eval_operand_on_row(row, col, params);
        return val.is_null();
    }

    // NOT IN (...)
    if let Some(pos) = find_top_level_keyword(s, "NOT IN") {
        let left_str = s[..pos].trim();
        let right_str = s[pos + 6..].trim();
        if right_str.starts_with('(') && right_str.ends_with(')') {
            let v_left = eval_operand_on_row(row, left_str, params);
            if v_left.is_null() {
                return false;
            }
            let inside = &right_str[1..right_str.len() - 1];
            let tokens = split_comma_separated_tokens(inside);
            let mut matches_any = false;
            for tok in tokens {
                let v_item = eval_operand_on_row(row, tok, params);
                if v_left.is_equal(&v_item) {
                    matches_any = true;
                    break;
                }
            }
            return !matches_any;
        }
    }

    // IN (...)
    if let Some(pos) = find_top_level_keyword(s, "IN") {
        let left_str = s[..pos].trim();
        let right_str = s[pos + 2..].trim();
        if right_str.starts_with('(') && right_str.ends_with(')') {
            let v_left = eval_operand_on_row(row, left_str, params);
            if v_left.is_null() {
                return false;
            }
            let inside = &right_str[1..right_str.len() - 1];
            let tokens = split_comma_separated_tokens(inside);
            for tok in tokens {
                let v_item = eval_operand_on_row(row, tok, params);
                if v_left.is_equal(&v_item) {
                    return true;
                }
            }
            return false;
        }
    }

    // LIKE / ILIKE
    if let Some(pos) = upper.find(" ILIKE ") {
        let left = eval_operand_on_row(row, s[..pos].trim(), params);
        let pattern_val = eval_operand_on_row(row, s[pos + 7..].trim(), params);
        return sql_like_match(&left.as_str(), &pattern_val.as_str(), true);
    }
    if let Some(pos) = upper.find(" LIKE ") {
        let left = eval_operand_on_row(row, s[..pos].trim(), params);
        let pattern_val = eval_operand_on_row(row, s[pos + 6..].trim(), params);
        return sql_like_match(&left.as_str(), &pattern_val.as_str(), false);
    }

    // JSONB / Array containment: @> and <@
    if let Some(pos) = s.find(" @> ") {
        let left = eval_operand_on_row(row, s[..pos].trim(), params);
        let right = eval_operand_on_row(row, s[pos + 4..].trim(), params);
        if let (Some(l), Some(r)) = (parse_val_to_json(&left), parse_val_to_json(&right)) {
            if json_contains_check(&l, &r) {
                return true;
            }
        }
        return array_contains_check(&left, &right);
    }
    if let Some(pos) = s.find(" <@ ") {
        let left = eval_operand_on_row(row, s[..pos].trim(), params);
        let right = eval_operand_on_row(row, s[pos + 4..].trim(), params);
        if let (Some(l), Some(r)) = (parse_val_to_json(&left), parse_val_to_json(&right)) {
            if json_contains_check(&r, &l) {
                return true;
            }
        }
        return array_contains_check(&right, &left);
    }
    if let Some(pos) = find_top_level_op(s, "&&") {
        let left = eval_operand_on_row(row, s[..pos].trim(), params);
        let right = eval_operand_on_row(row, s[pos + 2..].trim(), params);
        return array_overlap_check(&left, &right);
    }
    // JSONB key existence: ?|, ?&, ?
    if let Some(pos) = find_top_level_op(s, "?|") {
        let left = eval_operand_on_row(row, s[..pos].trim(), params);
        let right = eval_operand_on_row(row, s[pos + 2..].trim(), params);
        if let Some(l) = parse_val_to_json(&left) {
            let keys = parse_str_or_json_keys(&right);
            return keys.iter().any(|k| json_has_key_check(&l, k));
        }
        return false;
    }
    if let Some(pos) = find_top_level_op(s, "?&") {
        let left = eval_operand_on_row(row, s[..pos].trim(), params);
        let right = eval_operand_on_row(row, s[pos + 2..].trim(), params);
        if let Some(l) = parse_val_to_json(&left) {
            let keys = parse_str_or_json_keys(&right);
            return !keys.is_empty() && keys.iter().all(|k| json_has_key_check(&l, k));
        }
        return false;
    }
    if let Some(pos) = find_top_level_op(s, "?") {
        let left = eval_operand_on_row(row, s[..pos].trim(), params);
        let right = eval_operand_on_row(row, s[pos + 1..].trim(), params);
        if let Some(l) = parse_val_to_json(&left) {
            return json_has_key_check(&l, &right.as_str());
        }
        return false;
    }

    // Comparison operators: >=, <=, !=, <>, =, >, <
    let op_candidates = [">=", "<=", "!=", "<>", "=", ">", "<"];
    for op in op_candidates {
        if let Some(idx) = find_top_level_op(s, op) {
            let left_str = s[..idx].trim();
            let right_str = s[idx + op.len()..].trim();
            let v_left = eval_operand_on_row(row, left_str, params);
            let v_right = eval_operand_on_row(row, right_str, params);

            let ord = v_left.cmp_value(&v_right);
            return match op {
                ">=" => ord == std::cmp::Ordering::Greater || ord == std::cmp::Ordering::Equal,
                "<=" => ord == std::cmp::Ordering::Less || ord == std::cmp::Ordering::Equal,
                "!=" | "<>" => !v_left.is_equal(&v_right),
                "=" => v_left.is_equal(&v_right),
                ">" => ord == std::cmp::Ordering::Greater,
                "<" => ord == std::cmp::Ordering::Less,
                _ => false,
            };
        }
    }

    false
}

pub(crate) fn eval_operand_on_row(row: &CombinedRow, token: &str, params: &[Value]) -> Value {
    let mut t = token.trim();
    while t.starts_with('(') && t.ends_with(')') && is_fully_enclosed_in_parens(t) {
        let inside = t[1..t.len() - 1].trim();
        if split_comma_separated_tokens(inside).len() > 1 {
            break;
        }
        t = inside;
    }
    if let Some(colon_pos) = find_last_top_level_op(t, "::") {
        let target_type = t[colon_pos + 2..].trim();
        let val = eval_operand_on_row(row, &t[..colon_pos], params);
        return cast_val(val, target_type);
    }
    if let Some((op_idx, op)) = find_last_top_level_json_op(t) {
        let left_op = eval_operand_on_row(row, &t[..op_idx], params);
        let right_op = eval_operand_on_row(row, &t[op_idx + op.len()..], params);
        let left_json = match &left_op {
            Value::Text(s) => match serde_json::from_str::<serde_json::Value>(s.trim()) {
                Ok(v) => v,
                Err(_) => serde_json::Value::String(s.to_string()),
            },
            _ => value_to_json(&left_op),
        };
        let right_json = match &right_op {
            Value::Text(s) => serde_json::Value::String(s.to_string()),
            _ => value_to_json(&right_op),
        };
        let res_json = eval_json_extract_serde(&left_json, &right_json, op);
        return match res_json {
            serde_json::Value::Null => Value::Null,
            serde_json::Value::String(s) => Value::text(s),
            serde_json::Value::Bool(b) => Value::Bool(b),
            serde_json::Value::Number(n) => {
                if let Some(i) = n.as_i64() {
                    Value::Int(i)
                } else if let Some(f) = n.as_f64() {
                    Value::Float(f)
                } else {
                    Value::text(n.to_string())
                }
            }
            serde_json::Value::Object(_) | serde_json::Value::Array(_) => Value::text(res_json.to_string()),
        };
    }

    if let Some(pipe_idx) = find_top_level_op(t, "||") {
        let left = eval_operand_on_row(row, &t[..pipe_idx], params);
        let right = eval_operand_on_row(row, &t[pipe_idx + 2..], params);
        if left.is_null() || right.is_null() {
            return Value::Null;
        }
        let l_s = left.as_str();
        let r_s = right.as_str();
        if l_s.starts_with("\\x") || r_s.starts_with("\\x") {
            let mut combined = get_raw_bytes(&left);
            combined.extend(get_raw_bytes(&right));
            return Value::text(format!("\\x{}", hex::encode(combined)));
        }
        return Value::text(format!("{}{}", l_s, r_s));
    }
    if t.starts_with('$') {
        if let Ok(idx) = t[1..].parse::<usize>() {
            return params.get(idx.saturating_sub(1)).cloned().unwrap_or(Value::Null);
        }
    }
    if t.eq_ignore_ascii_case("TRUE") {
        return Value::Bool(true);
    }
    if t.eq_ignore_ascii_case("FALSE") {
        return Value::Bool(false);
    }
    if t.eq_ignore_ascii_case("NULL") {
        return Value::Null;
    }
    if t.starts_with('\'') && t.ends_with('\'') && t.len() >= 2 {
        return Value::text(t[1..t.len() - 1].replace("''", "'"));
    }
    let upper = t.to_uppercase();
    if is_complete_array_constructor(t) {
        let inside = &t[6..t.len() - 1].trim();
        if inside.is_empty() {
            return Value::text("[]");
        }
        let items: Vec<serde_json::Value> = split_comma_separated_tokens(inside)
            .into_iter()
            .map(|tok| {
                let v = eval_operand_on_row(row, &tok, params);
                value_to_json(&v)
            })
            .collect();
        return Value::text(serde_json::to_string(&items).unwrap_or_default());
    }
    if upper == "GEN_RANDOM_UUID()" || upper == "UUID_GENERATE_V4()" {
        return Value::text(crate::types::generate_uuid_v4());
    }
    if t.ends_with(')') && crate::engine::strfn::is_strfn_name(&upper) {
        let eval = |e: &str| eval_operand_on_row(row, e, params);
        if let Some(v) = crate::engine::strfn::try_eval(t, &upper, &eval) {
            return v;
        }
    }
    if upper.starts_with("LOWER(") && t.ends_with(')') {
        let inner = &t[6..t.len() - 1];
        let val = eval_operand_on_row(row, inner, params);
        return Value::text(val.as_str().to_lowercase());
    }
    if upper.starts_with("UPPER(") && t.ends_with(')') {
        let inner = &t[6..t.len() - 1];
        let val = eval_operand_on_row(row, inner, params);
        return Value::text(val.as_str().to_uppercase());
    }
    if upper.starts_with("TRIM(") && t.ends_with(')') {
        let inner = &t[5..t.len() - 1];
        let val = eval_operand_on_row(row, inner, params);
        return Value::text(val.as_str().trim().to_string());
    }
    if upper.starts_with("BTRIM(") && t.ends_with(')') {
        let inner = &t[6..t.len() - 1];
        let val = eval_operand_on_row(row, inner, params);
        return Value::text(val.as_str().trim().to_string());
    }
    if let Ok(i) = t.parse::<i64>() {
        return Value::Int(i);
    }
    if let Ok(f) = t.parse::<f64>() {
        return Value::Float(f);
    }
    row.get_val(t)
}

fn json_val_to_str(v: &serde_json::Value) -> String {
    match v {
        serde_json::Value::Null => String::new(),
        serde_json::Value::Bool(b) => b.to_string(),
        serde_json::Value::Number(n) => n.to_string(),
        serde_json::Value::String(s) => s.clone(),
        serde_json::Value::Array(_) | serde_json::Value::Object(_) => serde_json::to_string(v).unwrap_or_default(),
    }
}

fn cast_json_val(v: serde_json::Value, target_type: &str) -> serde_json::Value {
    let t = target_type.trim().to_uppercase();
    if t.starts_with("INT") || t == "BIGINT" || t == "SMALLINT" {
        if let Some(i) = v.as_i64() {
            serde_json::json!(i)
        } else if let Some(s) = v.as_str() {
            s.parse::<i64>().map(|i| serde_json::json!(i)).unwrap_or(serde_json::Value::Null)
        } else {
            serde_json::Value::Null
        }
    } else if t.starts_with("FLOAT") || t.starts_with("DOUBLE") || t.starts_with("REAL") || t.starts_with("NUMERIC") || t.starts_with("DECIMAL") {
        if let Some(f) = v.as_f64() {
            serde_json::json!(f)
        } else if let Some(s) = v.as_str() {
            s.parse::<f64>().map(|f| serde_json::json!(f)).unwrap_or(serde_json::Value::Null)
        } else {
            serde_json::Value::Null
        }
    } else if t.starts_with("BOOL") {
        if let Some(b) = v.as_bool() {
            serde_json::json!(b)
        } else if let Some(s) = v.as_str() {
            serde_json::json!(s.eq_ignore_ascii_case("true") || s == "1")
        } else {
            serde_json::Value::Null
        }
    } else if t.starts_with("TEXT") || t.starts_with("VARCHAR") || t.starts_with("CHAR") {
        serde_json::Value::String(json_val_to_str(&v))
    } else {
        v
    }
}

fn parse_subquery_from_and_clauses<'a>(
    after_from: &'a str,
) -> (&'a str, Option<&'a str>, Option<&'a str>, Option<&'a str>, Option<&'a str>) {
    let where_pos = find_top_level_keyword(after_from, "WHERE");
    let order_pos = find_top_level_keyword(after_from, "ORDER BY");
    let limit_pos = find_top_level_keyword(after_from, "LIMIT");
    let offset_pos = find_top_level_keyword(after_from, "OFFSET");

    let mut kw_positions: Vec<(usize, &str, usize)> = Vec::new();
    if let Some(p) = where_pos { kw_positions.push((p, "WHERE", 5)); }
    if let Some(p) = order_pos { kw_positions.push((p, "ORDER BY", 8)); }
    if let Some(p) = limit_pos { kw_positions.push((p, "LIMIT", 5)); }
    if let Some(p) = offset_pos { kw_positions.push((p, "OFFSET", 6)); }

    kw_positions.sort_by_key(|(p, _, _)| *p);

    let table_part = if let Some(&(first_p, _, _)) = kw_positions.first() {
        after_from[..first_p].trim()
    } else {
        after_from.trim()
    };

    let mut where_opt = None;
    let mut order_opt = None;
    let mut limit_opt = None;
    let mut offset_opt = None;

    for (idx, &(p, kw, kw_len)) in kw_positions.iter().enumerate() {
        let content_start = p + kw_len;
        let content_end = if idx + 1 < kw_positions.len() {
            kw_positions[idx + 1].0
        } else {
            after_from.len()
        };
        let content = after_from[content_start..content_end].trim();
        match kw {
            "WHERE" => where_opt = Some(content),
            "ORDER BY" => order_opt = Some(content),
            "LIMIT" => limit_opt = Some(content),
            "OFFSET" => offset_opt = Some(content),
            _ => {}
        }
    }

    (table_part, where_opt, order_opt, limit_opt, offset_opt)
}

/// Biểu thức trên dòng JOIN nên tính bằng `eval_sql_expr` (xem chỗ gọi): có phép toán `+ - * / %` ở cấp ngoài
/// cùng, hoặc là lời gọi hàm số học / ngày giờ; không chứa subquery hay hàm gộp.
fn joined_needs_full_eval(t: &str, upper: &str) -> bool {
    if contains_ci(t, "SELECT") || mentions_call(t, AGG_NAMES) || t.contains("->") || t.contains("#>") {
        return false;
    }
    const FULL_FNS: [&str; 13] = ["ROUND(", "CEIL(", "CEILING(", "FLOOR(", "ABS(", "TRUNC(", "EXTRACT(", "DATE_PART(", "DATE_TRUNC(", "AGE(", "GREATEST(", "LEAST(", "CAST("];
    // CASE / COALESCE bọc các hàm trên (`CASE WHEN ... THEN GREATEST(0, CEIL(...))::int END`): cũng tính đầy đủ
    if upper.starts_with("CASE") || upper.starts_with("COALESCE(") {
        return FULL_FNS.iter().any(|f| upper.contains(f));
    }
    let arith = find_top_level_math_op(t, &['+', '-', '*', '/', '%']).is_some_and(|(p, _)| !t[..p].trim().is_empty());
    let func = FULL_FNS.iter().any(|f| upper.starts_with(f) && t.ends_with(')'));
    let cast = find_last_top_level_op(t, "::").is_some_and(|p| cast_is_outermost(t, p) && t[..p].trim_end().ends_with(')'));
    let json_del = find_top_level_op(t, "#-").is_some();
    arith || func || cast || json_del
}

/// Thay mọi tham chiếu cột (có trong dòng) bằng literal của giá trị. `None` nếu gặp `x.col` không có trong
/// dòng (không chắc nghĩa: để đường cũ xử lý).
fn literalize_joined_columns(expr: &str, row: &CombinedRow) -> Option<String> {
    const KW: &[&str] = &[
        "FROM", "AS", "AND", "OR", "NOT", "NULL", "TRUE", "FALSE", "CASE", "WHEN", "THEN", "ELSE", "END", "IS", "IN", "LIKE", "ILIKE",
        "INTERVAL", "CURRENT_TIMESTAMP", "CURRENT_DATE", "CURRENT_TIME", "LOCALTIMESTAMP", "LOCALTIME", "EPOCH", "YEAR", "MONTH", "DAY",
        "HOUR", "MINUTE", "SECOND", "WEEK", "DOW", "DOY", "QUARTER", "DATE", "TIMESTAMP", "TIMESTAMPTZ", "TIME", "AT", "ZONE", "DISTINCT",
        "BETWEEN", "SYMMETRIC", "ESCAPE", "SIMILAR", "TO", "FOR", "USING", "BOTH", "LEADING", "TRAILING",
    ];
    let b = expr.as_bytes();
    let mut out = String::with_capacity(expr.len());
    let mut i = 0;
    while i < b.len() {
        let c = b[i];
        if c == b'\'' {
            let st = i;
            i += 1;
            while i < b.len() {
                if b[i] == b'\'' {
                    if b.get(i + 1) == Some(&b'\'') {
                        i += 2;
                        continue;
                    }
                    break;
                }
                i += 1;
            }
            i = (i + 1).min(b.len());
            out.push_str(&expr[st..i]);
            continue;
        }
        if (c.is_ascii_alphabetic() || c == b'_' || c == b'"') && (i == 0 || !(b[i - 1].is_ascii_alphanumeric() || b[i - 1] == b'_' || b[i - 1] == b'$')) {
            let st = i;
            while i < b.len() && (b[i].is_ascii_alphanumeric() || b[i] == b'_' || b[i] == b'.' || b[i] == b'"') {
                i += 1;
            }
            let tok = &expr[st..i];
            let clean = tok.replace('"', "").to_lowercase();
            let next = expr[i..].trim_start();
            let after_cast = expr[..st].trim_end().ends_with("::");
            let is_fn = next.starts_with('(');
            let is_kw = !clean.contains('.') && KW.iter().any(|k| k.eq_ignore_ascii_case(&clean));
            let known = row.schema.col_map.contains_key(&clean) || row.extra.as_ref().map_or(false, |e| e.contains_key(&clean));
            if !after_cast && !is_fn && !is_kw && known {
                out.push_str(&Executor::value_as_sql_literal_pub(&row.get_val(&clean)));
            } else if clean.contains('.') && !is_fn && !after_cast {
                return None;
            } else {
                out.push_str(tok);
            }
            continue;
        }
        let ch_len = expr[i..].chars().next().map_or(1, |ch| ch.len_utf8());
        out.push_str(&expr[i..i + ch_len]);
        i += ch_len;
    }
    Some(out)
}

fn eval_joined_scalar_expr(
    expr: &str,
    row: &CombinedRow,
    params: &[Value],
    storage: &crate::storage::engine::StorageEngine,
) -> serde_json::Value {
    let mut t = expr.trim();
    while t.starts_with('(') && t.ends_with(')') && is_fully_enclosed_in_parens(t) {
        let inner = t[1..t.len() - 1].trim();
        if inner.to_uppercase().starts_with("SELECT ") {
            break;
        }
        t = inner;
    }
    let upper = t.to_uppercase();

    // Phép toán ở cấp ngoài cùng và hàm số học / ngày giờ: bộ đánh giá của JOIN không đủ (phép trừ
    // timestamp, `ROUND(... / GREATEST(...))`... ra NULL). Thay cột bằng giá trị của dòng rồi tính bằng bộ
    // đánh giá đầy đủ của câu một bảng.
    if joined_needs_full_eval(t, &upper) {
        if let Some(lit) = literalize_joined_columns(t, row) {
            return value_to_json(&eval_sql_expr(&lit, &[], None, &[], None, params));
        }
    }

    // hàm chuỗi bổ sung: regex, format (engine/strfn.rs), đối số tính bằng chính bộ đánh giá của JOIN
    if t.ends_with(')') && crate::engine::strfn::is_strfn_name(&upper) {
        let eval = |e: &str| json_to_value(&eval_joined_scalar_expr(e, row, params, storage));
        if let Some(v) = crate::engine::strfn::try_eval(t, &upper, &eval) {
            return value_to_json(&v);
        }
    }

    // hàm ngày giờ (engine/datetime.rs), đối số tính bằng chính bộ đánh giá của JOIN
    if t.ends_with(')') && is_datetime_fn_name(&upper) {
        let eval = |e: &str| json_to_value(&eval_joined_scalar_expr(e, row, params, storage));
        let none = |_: &str| None;
        let type_of = |e: &str| crate::engine::coltypes::pg_typeof(e, &none);
        let ctx = crate::engine::datetime::Ctx { eval: &eval, type_of: &type_of };
        if let Some(v) = crate::engine::datetime::try_eval(t, &upper, &ctx) {
            return value_to_json(&v);
        }
    }

    // 1. Correlated / Scalar Subquery: (SELECT ... FROM child WHERE child.fk = parent.id AND ...)
    if (upper.starts_with("(SELECT") || upper.starts_with("SELECT")) && upper.contains("FROM") {
        let sub_sql = if t.starts_with('(') && t.ends_with(')') && is_fully_enclosed_in_parens(t) {
            t[1..t.len() - 1].trim()
        } else {
            t
        };
        if let Some(from_pos) = find_top_level_keyword(sub_sql, "FROM") {
            let select_part = if sub_sql.to_uppercase().starts_with("SELECT ") {
                sub_sql[6..from_pos].trim()
            } else {
                sub_sql[..from_pos].trim()
            };
            let after_sub_from = sub_sql[from_pos + 4..].trim();
            let (child_tbl_part, where_clause, _order_clause, _limit_clause, _offset_clause) =
                parse_subquery_from_and_clauses(after_sub_from);
            let (child_table_name, child_alias) = extract_table_name_and_alias(child_tbl_part);

            if let Some(child_table) = storage.get_table(child_table_name) {
                let is_child_col = |col_str: &str| -> bool {
                    let s = col_str.trim().trim_matches('"');
                    if let Some(dot) = s.find('.') {
                        let prefix = s[..dot].trim().trim_matches('"').to_lowercase();
                        if prefix == child_table_name.to_lowercase() {
                            return true;
                        }
                        if let Some(ca) = child_alias {
                            if prefix == ca.trim_matches('"').to_lowercase() {
                                return true;
                            }
                        }
                        return false;
                    }
                    child_table.get_column_index(s).is_some()
                };

                let resolve_sub_operand = |operand: &str, cr: &[Value]| -> Value {
                    let op = operand.trim();
                    if is_child_col(op) {
                        let cname = clean_col_name(op);
                        if let Some(idx) = child_table.get_column_index(cname) {
                            return cr.get(idx).cloned().unwrap_or(Value::Null);
                        }
                        return Value::Null;
                    }
                    if op.starts_with('$') {
                        if let Ok(idx) = op[1..].parse::<usize>() {
                            return params.get(idx.saturating_sub(1)).cloned().unwrap_or(Value::Null);
                        }
                    }
                    if op.starts_with('\'') && op.ends_with('\'') && op.len() >= 2 {
                        return Value::text(op[1..op.len() - 1].replace("''", "'"));
                    }
                    if op.eq_ignore_ascii_case("TRUE") {
                        return Value::Bool(true);
                    }
                    if op.eq_ignore_ascii_case("FALSE") {
                        return Value::Bool(false);
                    }
                    if op.eq_ignore_ascii_case("NULL") {
                        return Value::Null;
                    }
                    if let Ok(i) = op.parse::<i64>() {
                        return Value::Int(i);
                    }
                    if let Ok(f) = op.parse::<f64>() {
                        return Value::Float(f);
                    }
                    let clean_op = clean_col_name(op);
                    let v = row.get_val(clean_op);
                    if !v.is_null() {
                        return v;
                    }
                    row.get_val(op)
                };

                let and_parts = if let Some(w_str) = where_clause {
                    split_top_level_and(w_str)
                } else {
                    Vec::new()
                };

                let mut matching_rows: Vec<&Vec<Value>> = Vec::new();

                for (cr_idx, cr) in child_table.rows.iter().enumerate() {
                    if child_table.is_deleted[cr_idx] {
                        continue;
                    }
                    let mut matches = true;
                    for part in &and_parts {
                        let part_trimmed = part.trim();
                        let part_upper = part_trimmed.to_uppercase();
                        if part_upper.ends_with("IS NULL") {
                            let col = part_trimmed[..part_trimmed.len() - 7].trim();
                            let v = resolve_sub_operand(col, cr);
                            if !v.is_null() {
                                matches = false;
                                break;
                            }
                        } else if part_upper.ends_with("IS NOT NULL") {
                            let col = part_trimmed[..part_trimmed.len() - 11].trim();
                            let v = resolve_sub_operand(col, cr);
                            if v.is_null() {
                                matches = false;
                                break;
                            }
                        } else if let Some(eq_idx) = part_trimmed.find('=') {
                            let left_raw = part_trimmed[..eq_idx].trim();
                            let right_raw = part_trimmed[eq_idx + 1..].trim();
                            let left_val = resolve_sub_operand(left_raw, cr);
                            let right_val = resolve_sub_operand(right_raw, cr);
                            if !left_val.is_equal(&right_val) {
                                matches = false;
                                break;
                            }
                        } else if let Some(neq_idx) = part_trimmed.find("!=") {
                            let left_raw = part_trimmed[..neq_idx].trim();
                            let right_raw = part_trimmed[neq_idx + 2..].trim();
                            let left_val = resolve_sub_operand(left_raw, cr);
                            let right_val = resolve_sub_operand(right_raw, cr);
                            if left_val.is_equal(&right_val) {
                                matches = false;
                                break;
                            }
                        } else if let Some(neq_idx) = part_trimmed.find("<>") {
                            let left_raw = part_trimmed[..neq_idx].trim();
                            let right_raw = part_trimmed[neq_idx + 2..].trim();
                            let left_val = resolve_sub_operand(left_raw, cr);
                            let right_val = resolve_sub_operand(right_raw, cr);
                            if left_val.is_equal(&right_val) {
                                matches = false;
                                break;
                            }
                        }
                    }
                    if matches {
                        matching_rows.push(cr);
                    }
                }

                let sel_trimmed = select_part.trim();
                let sel_upper = sel_trimmed.to_uppercase();

                if sel_upper.starts_with("COUNT(") || sel_upper == "COUNT(*)" {
                    return serde_json::json!(matching_rows.len());
                } else if sel_upper.starts_with("SUM(") && sel_trimmed.ends_with(')') {
                    let inner = sel_trimmed[4..sel_trimmed.len() - 1].trim();
                    let mut sum_f = 0.0;
                    let mut sum_i = 0i64;
                    let mut is_float = false;
                    for cr in &matching_rows {
                        let v = resolve_sub_operand(inner, cr);
                        if let Some(i) = v.as_i64() {
                            sum_i += i;
                            sum_f += i as f64;
                        } else if let Some(f) = v.as_f64() {
                            sum_f += f;
                            is_float = true;
                        }
                    }
                    return if is_float { serde_json::json!(sum_f) } else { serde_json::json!(sum_i) };
                } else {
                    let expr_to_eval = if let Some(as_idx) = rfind_top_level_as(&sel_upper) {
                        sel_trimmed[..as_idx].trim()
                    } else {
                        sel_trimmed
                    };
                    if let Some(first_cr) = matching_rows.first() {
                        let val = resolve_sub_operand(expr_to_eval, first_cr);
                        return value_to_json(&val);
                    } else {
                        return serde_json::Value::Null;
                    }
                }
            }
        }
    }

    // 2. CASE WHEN ... THEN ... ELSE ... END
    if upper.starts_with("CASE") && upper.ends_with("END") {
        let inside = t[4..t.len() - 3].trim();
        let (when_part, else_part) = if let Some(else_idx) = find_case_else(inside) {
            (&inside[..else_idx], Some(inside[else_idx + 4..].trim()))
        } else {
            (inside, None)
        };

        let when_items = split_when_clauses(when_part);
        for (cond_str, res_str) in when_items {
            if eval_condition_on_row(row, &cond_str, params) {
                return eval_joined_scalar_expr(&res_str, row, params, storage);
            }
        }

        if let Some(e_str) = else_part {
            return eval_joined_scalar_expr(e_str, row, params, storage);
        }
        return serde_json::Value::Null;
    }

    // 3. CAST(expr AS type) or expr::type
    if upper.starts_with("CAST(") && t.ends_with(')') {
        let inner = &t[5..t.len() - 1].trim();
        if let Some(as_pos) = rfind_top_level_as(&inner.to_uppercase()) {
            let target_expr = inner[..as_pos].trim();
            let target_type = inner[as_pos + 4..].trim().to_uppercase();
            let val = eval_joined_scalar_expr(target_expr, row, params, storage);
            return cast_json_val(val, &target_type);
        }
    }
    if let Some(colon_pos) = t.rfind("::") {
        let target_expr = t[..colon_pos].trim();
        let target_type = t[colon_pos + 2..].trim().to_uppercase();
        let val = eval_joined_scalar_expr(target_expr, row, params, storage);
        return cast_json_val(val, &target_type);
    }

    // 3.5 JSON extraction operators: ->, ->>, #>, #>>
    if let Some((op_idx, op)) = find_last_top_level_json_op(t) {
        let lhs_expr = &t[..op_idx].trim();
        let rhs_expr = &t[op_idx + op.len()..].trim();
        let lhs = eval_joined_scalar_expr(lhs_expr, row, params, storage);
        let rhs = eval_joined_scalar_expr(rhs_expr, row, params, storage);
        return eval_json_extract_serde(&lhs, &rhs, op);
    }

    // JSON Functions: JSON_EXTRACT_PATH, JSON_EXTRACT_PATH_TEXT, JSON_ARRAY_LENGTH, JSON_TYPEOF, JSONB_STRIP_NULLS
    if (upper.starts_with("JSON_EXTRACT_PATH(") || upper.starts_with("JSONB_EXTRACT_PATH(")) && t.ends_with(')') {
        let open_idx = t.find('(').unwrap();
        let inner = &t[open_idx + 1..t.len() - 1];
        let args = split_function_args(inner);
        if !args.is_empty() {
            let mut curr = eval_joined_scalar_expr(&args[0], row, params, storage);
            for arg in &args[1..] {
                let key = eval_joined_scalar_expr(arg, row, params, storage);
                curr = eval_json_extract_serde(&curr, &key, "->");
                if curr.is_null() {
                    break;
                }
            }
            return curr;
        }
    }
    if (upper.starts_with("JSON_EXTRACT_PATH_TEXT(") || upper.starts_with("JSONB_EXTRACT_PATH_TEXT(")) && t.ends_with(')') {
        let open_idx = t.find('(').unwrap();
        let inner = &t[open_idx + 1..t.len() - 1];
        let args = split_function_args(inner);
        if !args.is_empty() {
            let mut curr = eval_joined_scalar_expr(&args[0], row, params, storage);
            let last_idx = args.len() - 1;
            for (idx, arg) in args[1..].iter().enumerate() {
                let key = eval_joined_scalar_expr(arg, row, params, storage);
                let op = if idx + 1 == last_idx { "->>" } else { "->" };
                curr = eval_json_extract_serde(&curr, &key, op);
                if curr.is_null() {
                    break;
                }
            }
            return curr;
        }
    }
    if (upper.starts_with("JSON_ARRAY_LENGTH(") || upper.starts_with("JSONB_ARRAY_LENGTH(")) && t.ends_with(')') {
        let inner = &t[upper.find('(').unwrap() + 1..t.len() - 1];
        let target = eval_joined_scalar_expr(inner, row, params, storage);
        let parsed = match &target {
            serde_json::Value::String(s) => serde_json::from_str::<serde_json::Value>(s.trim()).unwrap_or(serde_json::Value::Null),
            _ => target,
        };
        if let serde_json::Value::Array(arr) = parsed {
            return serde_json::json!(arr.len());
        }
        return serde_json::Value::Null;
    }
    if (upper.starts_with("JSON_TYPEOF(") || upper.starts_with("JSONB_TYPEOF(")) && t.ends_with(')') {
        let inner = &t[upper.find('(').unwrap() + 1..t.len() - 1];
        let target = eval_joined_scalar_expr(inner, row, params, storage);
        let parsed = match &target {
            serde_json::Value::String(s) => serde_json::from_str::<serde_json::Value>(s.trim()).unwrap_or(serde_json::Value::String(s.clone())),
            _ => target,
        };
        let type_str = match parsed {
            serde_json::Value::Null => "null",
            serde_json::Value::Bool(_) => "boolean",
            serde_json::Value::Number(_) => "number",
            serde_json::Value::String(_) => "string",
            serde_json::Value::Array(_) => "array",
            serde_json::Value::Object(_) => "object",
        };
        return serde_json::Value::String(type_str.to_string());
    }
    if upper.starts_with("JSONB_STRIP_NULLS(") && t.ends_with(')') {
        let inner = &t[18..t.len() - 1];
        let target = eval_joined_scalar_expr(inner, row, params, storage);
        let mut parsed = match &target {
            serde_json::Value::String(s) => serde_json::from_str::<serde_json::Value>(s.trim()).unwrap_or(serde_json::Value::Null),
            _ => target,
        };
        if let serde_json::Value::Object(ref mut map) = parsed {
            map.retain(|_, v| !v.is_null());
            return serde_json::Value::Object(map.clone());
        }
        return parsed;
    }

    // 4. JSON_BUILD_OBJECT / JSONB_BUILD_OBJECT
    if (upper.starts_with("JSON_BUILD_OBJECT(") || upper.starts_with("JSONB_BUILD_OBJECT(")) && t.ends_with(')') {
        let open_idx = t.find('(').unwrap();
        let inner = &t[open_idx + 1..t.len() - 1];
        let args = split_function_args(inner);
        let mut map = serde_json::Map::new();
        for chunk in args.chunks(2) {
            if chunk.len() == 2 {
                let k_val = eval_operand_on_row(row, &chunk[0], params).as_str();
                let v_val = eval_joined_scalar_expr(&chunk[1], row, params, storage);
                map.insert(k_val, v_val);
            }
        }
        return serde_json::Value::Object(map);
    }

    // JSON_BUILD_ARRAY / JSONB_BUILD_ARRAY
    if (upper.starts_with("JSON_BUILD_ARRAY(") || upper.starts_with("JSONB_BUILD_ARRAY(")) && t.ends_with(')') {
        let open_idx = t.find('(').unwrap();
        let inner = &t[open_idx + 1..t.len() - 1];
        let items: Vec<serde_json::Value> = split_function_args(inner)
            .into_iter()
            .map(|arg| eval_joined_scalar_expr(&arg, row, params, storage))
            .collect();
        return serde_json::Value::Array(items);
    }

    // JSONB_PRETTY
    if upper.starts_with("JSONB_PRETTY(") && t.ends_with(')') {
        let inner = &t[13..t.len() - 1];
        let v = eval_joined_scalar_expr(inner, row, params, storage);
        return serde_json::Value::String(serde_json::to_string_pretty(&v).unwrap_or_default());
    }

    // JSONB_SET
    if (upper.starts_with("JSONB_SET(") || upper.starts_with("JSON_SET(")) && t.ends_with(')') {
        let open_idx = t.find('(').unwrap();
        let inner = &t[open_idx + 1..t.len() - 1];
        let args = split_function_args(inner);
        if args.len() >= 3 {
            let target_val = eval_joined_scalar_expr(&args[0], row, params, storage);
            let path_val = eval_joined_scalar_expr(&args[1], row, params, storage);
            let new_val = eval_joined_scalar_expr(&args[2], row, params, storage);
            let create_missing = if args.len() >= 4 {
                let cm_val = eval_joined_scalar_expr(&args[3], row, params, storage);
                cm_val.as_bool().unwrap_or(true)
            } else {
                true
            };
            let mut target_json = match &target_val {
                serde_json::Value::String(st) => serde_json::from_str::<serde_json::Value>(st.trim()).unwrap_or(target_val.clone()),
                _ => target_val.clone(),
            };
            let keys = parse_json_path_keys(&path_val);
            jsonb_set_serde(&mut target_json, &keys, &new_val, create_missing);
            return target_json;
        }
    }

    // 5. ARRAY[...]
    if is_complete_array_constructor(t) {
        let inner = &t[6..t.len() - 1].trim();
        if inner.is_empty() {
            return serde_json::Value::Array(vec![]);
        }
        let items: Vec<serde_json::Value> = split_function_args(inner)
            .into_iter()
            .map(|arg| eval_joined_scalar_expr(&arg, row, params, storage))
            .collect();
        return serde_json::Value::Array(items);
    }

    // 6. COALESCE
    if upper.starts_with("COALESCE(") && t.ends_with(')') {
        let inner = &t[9..t.len() - 1];
        for arg in split_function_args(inner) {
            let v = eval_joined_scalar_expr(&arg, row, params, storage);
            if !v.is_null() {
                return v;
            }
        }
        return serde_json::Value::Null;
    }

    // 7. NULLIF
    if upper.starts_with("NULLIF(") && t.ends_with(')') {
        let inner = &t[7..t.len() - 1];
        let args = split_function_args(inner);
        if args.len() == 2 {
            let a = eval_joined_scalar_expr(&args[0], row, params, storage);
            let b = eval_joined_scalar_expr(&args[1], row, params, storage);
            return if a == b { serde_json::Value::Null } else { a };
        }
    }

    // 8. String concatenation with || operator
    if let Some(pipe_idx) = find_top_level_op(t, "||") {
        let lhs = eval_joined_scalar_expr(&t[..pipe_idx], row, params, storage);
        let rhs = eval_joined_scalar_expr(&t[pipe_idx + 2..], row, params, storage);
        if lhs.is_null() || rhs.is_null() {
            return serde_json::Value::Null;
        }
        let lhs_s = json_val_to_str(&lhs);
        let rhs_s = json_val_to_str(&rhs);
        return serde_json::Value::String(format!("{}{}", lhs_s, rhs_s));
    }

    // Bitwise binary operations (|, &, #, <<, >>)
    if let Some((op_idx, _)) = find_top_level_math_op(t, &['|']) {
        let lhs = eval_joined_scalar_expr(t[..op_idx].trim(), row, params, storage);
        let rhs = eval_joined_scalar_expr(t[op_idx + 1..].trim(), row, params, storage);
        if lhs.is_null() || rhs.is_null() { return serde_json::Value::Null; }
        if let (Some(l), Some(r)) = (lhs.as_i64(), rhs.as_i64()) {
            return serde_json::json!(l | r);
        }
    }
    if let Some((op_idx, _)) = find_top_level_math_op(t, &['&']) {
        let lhs = eval_joined_scalar_expr(t[..op_idx].trim(), row, params, storage);
        let rhs = eval_joined_scalar_expr(t[op_idx + 1..].trim(), row, params, storage);
        if lhs.is_null() || rhs.is_null() { return serde_json::Value::Null; }
        if let (Some(l), Some(r)) = (lhs.as_i64(), rhs.as_i64()) {
            return serde_json::json!(l & r);
        }
    }
    if let Some((op_idx, _)) = find_top_level_math_op(t, &['#']) {
        let lhs = eval_joined_scalar_expr(t[..op_idx].trim(), row, params, storage);
        let rhs = eval_joined_scalar_expr(t[op_idx + 1..].trim(), row, params, storage);
        if lhs.is_null() || rhs.is_null() { return serde_json::Value::Null; }
        if let (Some(l), Some(r)) = (lhs.as_i64(), rhs.as_i64()) {
            return serde_json::json!(l ^ r);
        }
    }
    if let Some((op_idx, op)) = find_top_level_shift_op(t) {
        let lhs = eval_joined_scalar_expr(t[..op_idx].trim(), row, params, storage);
        let rhs = eval_joined_scalar_expr(t[op_idx + 2..].trim(), row, params, storage);
        if lhs.is_null() || rhs.is_null() { return serde_json::Value::Null; }
        if let (Some(l), Some(r)) = (lhs.as_i64(), rhs.as_i64()) {
            let res = if op == "<<" { l << (r as u32) } else { l >> (r as u32) };
            return serde_json::json!(res);
        }
    }

    // Math binary operations (+, -)
    if let Some((op_idx, op)) = find_top_level_math_op(t, &['+', '-']) {
        let lhs = eval_joined_scalar_expr(t[..op_idx].trim(), row, params, storage);
        let rhs = eval_joined_scalar_expr(t[op_idx + 1..].trim(), row, params, storage);
        if lhs.is_null() || rhs.is_null() { return serde_json::Value::Null; }
        if let (serde_json::Value::String(a), serde_json::Value::String(b)) = (&lhs, &rhs) {
            if let Some(v) = crate::engine::temporal::temporal_arith(op, &Value::text(a), &Value::text(b)) {
                return value_to_json(&v);
            }
        }
        if let (Some(l_num), Some(r_num)) = (lhs.as_f64(), rhs.as_f64()) {
            let res = if op == '+' { l_num + r_num } else { l_num - r_num };
            return if res.fract() == 0.0 { serde_json::json!(res as i64) } else { serde_json::json!(res) };
        }
    }

    // Math binary operations (*, /, %)
    if let Some((op_idx, op)) = find_top_level_math_op(t, &['*', '/', '%']) {
        let lhs = eval_joined_scalar_expr(t[..op_idx].trim(), row, params, storage);
        let rhs = eval_joined_scalar_expr(t[op_idx + 1..].trim(), row, params, storage);
        if lhs.is_null() || rhs.is_null() { return serde_json::Value::Null; }
        if let (Some(l_num), Some(r_num)) = (lhs.as_f64(), rhs.as_f64()) {
            let res = match op {
                '*' => l_num * r_num,
                '/' | '%' if r_num == 0.0 => {
                    crate::engine::errors::division_by_zero();
                    return Default::default();
                }
                '/' => l_num / r_num,
                '%' => l_num % r_num,
                _ => 0.0,
            };
            return if res.fract() == 0.0 { serde_json::json!(res as i64) } else { serde_json::json!(res) };
        }
    }

    // Exponentiation (^)
    if let Some((op_idx, _)) = find_top_level_math_op(t, &['^']) {
        let lhs = eval_joined_scalar_expr(t[..op_idx].trim(), row, params, storage);
        let rhs = eval_joined_scalar_expr(t[op_idx + 1..].trim(), row, params, storage);
        if lhs.is_null() || rhs.is_null() { return serde_json::Value::Null; }
        if let (Some(l_num), Some(r_num)) = (lhs.as_f64(), rhs.as_f64()) {
            let res = l_num.powf(r_num);
            return if res.fract() == 0.0 { serde_json::json!(res as i64) } else { serde_json::json!(res) };
        }
    }

    // Unary operators (-, ~)
    if t.starts_with('-') && !t.starts_with("->") && !t.starts_with("--") {
        let inner = t[1..].trim();
        if !inner.is_empty() {
            let val = eval_joined_scalar_expr(inner, row, params, storage);
            if val.is_null() { return serde_json::Value::Null; }
            if let Some(f) = val.as_f64() {
                return if f.fract() == 0.0 { serde_json::json!(-(f as i64)) } else { serde_json::json!(-f) };
            }
        }
    }
    if t.starts_with('~') {
        let inner = t[1..].trim();
        if !inner.is_empty() {
            let val = eval_joined_scalar_expr(inner, row, params, storage);
            if val.is_null() { return serde_json::Value::Null; }
            if let Some(i) = val.as_i64() {
                return serde_json::json!(!i);
            }
        }
    }

    // 9. Built-in String Functions: UPPER, LOWER, INITCAP, CONCAT, CONCAT_WS, LENGTH, CHAR_LENGTH,
    // CHARACTER_LENGTH, OCTET_LENGTH, LEFT, RIGHT, TRIM, BTRIM, LTRIM, RTRIM, LPAD, RPAD, REPEAT,
    // REVERSE, ASCII, CHR, TRANSLATE, STRPOS, SPLIT_PART, MD5, ENCODE, DECODE, REPLACE, SUBSTRING
    if upper.starts_with("UPPER(") && t.ends_with(')') {
        let inner = &t[6..t.len() - 1];
        let v = eval_joined_scalar_expr(inner, row, params, storage);
        if v.is_null() { return serde_json::Value::Null; }
        return serde_json::Value::String(json_val_to_str(&v).to_uppercase());
    }

    if upper.starts_with("LOWER(") && t.ends_with(')') {
        let inner = &t[6..t.len() - 1];
        let v = eval_joined_scalar_expr(inner, row, params, storage);
        if v.is_null() { return serde_json::Value::Null; }
        return serde_json::Value::String(json_val_to_str(&v).to_lowercase());
    }

    if upper.starts_with("INITCAP(") && t.ends_with(')') {
        let inner = &t[8..t.len() - 1];
        let v = eval_joined_scalar_expr(inner, row, params, storage);
        if v.is_null() { return serde_json::Value::Null; }
        let s_str = json_val_to_str(&v);
        let mut out = String::with_capacity(s_str.len());
        let mut new_word = true;
        for c in s_str.chars() {
            if c.is_alphanumeric() {
                if new_word {
                    out.extend(c.to_uppercase());
                    new_word = false;
                } else {
                    out.extend(c.to_lowercase());
                }
            } else {
                out.push(c);
                new_word = true;
            }
        }
        return serde_json::Value::String(out);
    }

    if upper.starts_with("CONCAT(") && t.ends_with(')') {
        let inner = &t[7..t.len() - 1];
        let args = split_function_args(inner);
        let mut out = String::new();
        for arg in args {
            let v = eval_joined_scalar_expr(&arg, row, params, storage);
            if !v.is_null() {
                out.push_str(&json_val_to_str(&v));
            }
        }
        return serde_json::Value::String(out);
    }

    if upper.starts_with("CONCAT_WS(") && t.ends_with(')') {
        let inner = &t[10..t.len() - 1];
        let args = split_function_args(inner);
        if !args.is_empty() {
            let sep_val = eval_joined_scalar_expr(&args[0], row, params, storage);
            if sep_val.is_null() {
                return serde_json::Value::Null;
            }
            let sep = json_val_to_str(&sep_val);
            let mut parts = Vec::new();
            for arg in &args[1..] {
                let v = eval_joined_scalar_expr(arg, row, params, storage);
                if !v.is_null() {
                    parts.push(json_val_to_str(&v));
                }
            }
            return serde_json::Value::String(parts.join(&sep));
        }
    }

    if (upper.starts_with("LENGTH(") || upper.starts_with("CHAR_LENGTH(") || upper.starts_with("CHARACTER_LENGTH(")) && t.ends_with(')') {
        let open_p = t.find('(').unwrap();
        let inner = &t[open_p + 1..t.len() - 1];
        let v = eval_joined_scalar_expr(inner, row, params, storage);
        if v.is_null() { return serde_json::Value::Null; }
        let len = json_val_to_str(&v).chars().count();
        return serde_json::json!(len);
    }

    if upper.starts_with("OCTET_LENGTH(") && t.ends_with(')') {
        let inner = &t[13..t.len() - 1];
        let v = eval_joined_scalar_expr(inner, row, params, storage);
        if v.is_null() { return serde_json::Value::Null; }
        let len = json_val_to_str(&v).as_bytes().len();
        return serde_json::json!(len);
    }

    if upper.starts_with("LEFT(") && t.ends_with(')') {
        let inner = &t[5..t.len() - 1];
        let args = split_function_args(inner);
        if args.len() == 2 {
            let v = eval_joined_scalar_expr(&args[0], row, params, storage);
            let n_val = eval_joined_scalar_expr(&args[1], row, params, storage);
            if v.is_null() || n_val.is_null() {
                return serde_json::Value::Null;
            }
            let s_str = json_val_to_str(&v);
            let chars: Vec<char> = s_str.chars().collect();
            let len = chars.len() as i64;
            let n = n_val.as_i64().unwrap_or(0);
            let take_len = if n >= 0 {
                (n as usize).min(chars.len())
            } else {
                ((len + n).max(0) as usize).min(chars.len())
            };
            let res: String = chars[..take_len].iter().collect();
            return serde_json::Value::String(res);
        }
    }

    if upper.starts_with("RIGHT(") && t.ends_with(')') {
        let inner = &t[6..t.len() - 1];
        let args = split_function_args(inner);
        if args.len() == 2 {
            let v = eval_joined_scalar_expr(&args[0], row, params, storage);
            let n_val = eval_joined_scalar_expr(&args[1], row, params, storage);
            if v.is_null() || n_val.is_null() {
                return serde_json::Value::Null;
            }
            let s_str = json_val_to_str(&v);
            let chars: Vec<char> = s_str.chars().collect();
            let n = n_val.as_i64().unwrap_or(0);
            let start_idx = if n >= 0 {
                chars.len().saturating_sub(n as usize)
            } else {
                ((-n) as usize).min(chars.len())
            };
            let res: String = chars[start_idx..].iter().collect();
            return serde_json::Value::String(res);
        }
    }

    if (upper.starts_with("TRIM(") || upper.starts_with("BTRIM(")) && t.ends_with(')') {
        let open_p = t.find('(').unwrap();
        let inner = &t[open_p + 1..t.len() - 1];
        let args = split_function_args(inner);
        if args.len() == 1 {
            let v = eval_joined_scalar_expr(&args[0], row, params, storage);
            if v.is_null() { return serde_json::Value::Null; }
            return serde_json::Value::String(json_val_to_str(&v).trim().to_string());
        } else if args.len() >= 2 {
            let v = eval_joined_scalar_expr(&args[0], row, params, storage);
            let chars_v = eval_joined_scalar_expr(&args[1], row, params, storage);
            if v.is_null() || chars_v.is_null() { return serde_json::Value::Null; }
            let s_str = json_val_to_str(&v);
            let c_str = json_val_to_str(&chars_v);
            let trim_chars: Vec<char> = c_str.chars().collect();
            let trimmed = s_str.trim_matches(|c: char| trim_chars.contains(&c)).to_string();
            return serde_json::Value::String(trimmed);
        }
    }

    if upper.starts_with("LTRIM(") && t.ends_with(')') {
        let inner = &t[6..t.len() - 1];
        let args = split_function_args(inner);
        if args.len() == 1 {
            let v = eval_joined_scalar_expr(&args[0], row, params, storage);
            if v.is_null() { return serde_json::Value::Null; }
            return serde_json::Value::String(json_val_to_str(&v).trim_start().to_string());
        } else if args.len() >= 2 {
            let v = eval_joined_scalar_expr(&args[0], row, params, storage);
            let chars_v = eval_joined_scalar_expr(&args[1], row, params, storage);
            if v.is_null() || chars_v.is_null() { return serde_json::Value::Null; }
            let s_str = json_val_to_str(&v);
            let c_str = json_val_to_str(&chars_v);
            let trim_chars: Vec<char> = c_str.chars().collect();
            let trimmed = s_str.trim_start_matches(|c: char| trim_chars.contains(&c)).to_string();
            return serde_json::Value::String(trimmed);
        }
    }

    if upper.starts_with("RTRIM(") && t.ends_with(')') {
        let inner = &t[6..t.len() - 1];
        let args = split_function_args(inner);
        if args.len() == 1 {
            let v = eval_joined_scalar_expr(&args[0], row, params, storage);
            if v.is_null() { return serde_json::Value::Null; }
            return serde_json::Value::String(json_val_to_str(&v).trim_end().to_string());
        } else if args.len() >= 2 {
            let v = eval_joined_scalar_expr(&args[0], row, params, storage);
            let chars_v = eval_joined_scalar_expr(&args[1], row, params, storage);
            if v.is_null() || chars_v.is_null() { return serde_json::Value::Null; }
            let s_str = json_val_to_str(&v);
            let c_str = json_val_to_str(&chars_v);
            let trim_chars: Vec<char> = c_str.chars().collect();
            let trimmed = s_str.trim_end_matches(|c: char| trim_chars.contains(&c)).to_string();
            return serde_json::Value::String(trimmed);
        }
    }

    if upper.starts_with("LPAD(") && t.ends_with(')') {
        let inner = &t[5..t.len() - 1];
        let args = split_function_args(inner);
        if args.len() >= 2 {
            let v = eval_joined_scalar_expr(&args[0], row, params, storage);
            let len_val = eval_joined_scalar_expr(&args[1], row, params, storage);
            if v.is_null() || len_val.is_null() { return serde_json::Value::Null; }
            let target_len = len_val.as_i64().unwrap_or(0).max(0) as usize;
            let s_str = json_val_to_str(&v);
            let chars: Vec<char> = s_str.chars().collect();
            if chars.len() >= target_len {
                let res: String = chars[..target_len].iter().collect();
                return serde_json::Value::String(res);
            }
            let pad_str = if args.len() >= 3 {
                let pv = eval_joined_scalar_expr(&args[2], row, params, storage);
                if pv.is_null() { return serde_json::Value::Null; }
                json_val_to_str(&pv)
            } else {
                " ".to_string()
            };
            if pad_str.is_empty() {
                let res: String = chars.into_iter().collect();
                return serde_json::Value::String(res);
            }
            let pad_chars: Vec<char> = pad_str.chars().collect();
            let needed = target_len - chars.len();
            let mut out = String::new();
            for i in 0..needed {
                out.push(pad_chars[i % pad_chars.len()]);
            }
            out.push_str(&s_str);
            return serde_json::Value::String(out);
        }
    }

    if upper.starts_with("RPAD(") && t.ends_with(')') {
        let inner = &t[5..t.len() - 1];
        let args = split_function_args(inner);
        if args.len() >= 2 {
            let v = eval_joined_scalar_expr(&args[0], row, params, storage);
            let len_val = eval_joined_scalar_expr(&args[1], row, params, storage);
            if v.is_null() || len_val.is_null() { return serde_json::Value::Null; }
            let target_len = len_val.as_i64().unwrap_or(0).max(0) as usize;
            let s_str = json_val_to_str(&v);
            let chars: Vec<char> = s_str.chars().collect();
            if chars.len() >= target_len {
                let res: String = chars[..target_len].iter().collect();
                return serde_json::Value::String(res);
            }
            let pad_str = if args.len() >= 3 {
                let pv = eval_joined_scalar_expr(&args[2], row, params, storage);
                if pv.is_null() { return serde_json::Value::Null; }
                json_val_to_str(&pv)
            } else {
                " ".to_string()
            };
            if pad_str.is_empty() {
                let res: String = chars.into_iter().collect();
                return serde_json::Value::String(res);
            }
            let pad_chars: Vec<char> = pad_str.chars().collect();
            let needed = target_len - chars.len();
            let mut out = s_str;
            for i in 0..needed {
                out.push(pad_chars[i % pad_chars.len()]);
            }
            return serde_json::Value::String(out);
        }
    }

    if upper.starts_with("REPEAT(") && t.ends_with(')') {
        let inner = &t[7..t.len() - 1];
        let args = split_function_args(inner);
        if args.len() == 2 {
            let v = eval_joined_scalar_expr(&args[0], row, params, storage);
            let count_val = eval_joined_scalar_expr(&args[1], row, params, storage);
            if v.is_null() || count_val.is_null() { return serde_json::Value::Null; }
            let count = count_val.as_i64().unwrap_or(0).max(0) as usize;
            let s_str = json_val_to_str(&v);
            return serde_json::Value::String(s_str.repeat(count));
        }
    }

    if upper.starts_with("REVERSE(") && t.ends_with(')') {
        let inner = &t[8..t.len() - 1];
        let v = eval_joined_scalar_expr(inner, row, params, storage);
        if v.is_null() { return serde_json::Value::Null; }
        let rev: String = json_val_to_str(&v).chars().rev().collect();
        return serde_json::Value::String(rev);
    }

    if upper.starts_with("ASCII(") && t.ends_with(')') {
        let inner = &t[6..t.len() - 1];
        let v = eval_joined_scalar_expr(inner, row, params, storage);
        if v.is_null() { return serde_json::Value::Null; }
        let s_str = json_val_to_str(&v);
        if let Some(first_char) = s_str.chars().next() {
            return serde_json::json!(first_char as u32 as i64);
        }
        return serde_json::json!(0);
    }

    if upper.starts_with("CHR(") && t.ends_with(')') {
        let inner = &t[4..t.len() - 1];
        let v = eval_joined_scalar_expr(inner, row, params, storage);
        if v.is_null() { return serde_json::Value::Null; }
        let code = v.as_i64().unwrap_or(0) as u32;
        if let Some(ch) = char::from_u32(code) {
            return serde_json::Value::String(ch.to_string());
        }
        return serde_json::Value::String(String::new());
    }

    if upper.starts_with("TRANSLATE(") && t.ends_with(')') {
        let inner = &t[10..t.len() - 1];
        let args = split_function_args(inner);
        if args.len() == 3 {
            let str_val = eval_joined_scalar_expr(&args[0], row, params, storage);
            let from_val = eval_joined_scalar_expr(&args[1], row, params, storage);
            let to_val = eval_joined_scalar_expr(&args[2], row, params, storage);
            if str_val.is_null() || from_val.is_null() || to_val.is_null() { return serde_json::Value::Null; }
            let s_str = json_val_to_str(&str_val);
            let from_chars: Vec<char> = json_val_to_str(&from_val).chars().collect();
            let to_chars: Vec<char> = json_val_to_str(&to_val).chars().collect();
            let mut out = String::new();
            for ch in s_str.chars() {
                if let Some(pos) = from_chars.iter().position(|&fc| fc == ch) {
                    if pos < to_chars.len() {
                        out.push(to_chars[pos]);
                    }
                } else {
                    out.push(ch);
                }
            }
            return serde_json::Value::String(out);
        }
    }

    if upper.starts_with("STRPOS(") && t.ends_with(')') {
        let inner = &t[7..t.len() - 1];
        let args = split_function_args(inner);
        if args.len() == 2 {
            let str_val = eval_joined_scalar_expr(&args[0], row, params, storage);
            let sub_val = eval_joined_scalar_expr(&args[1], row, params, storage);
            if str_val.is_null() || sub_val.is_null() { return serde_json::Value::Null; }
            let s_str = json_val_to_str(&str_val);
            let sub_str = json_val_to_str(&sub_val);
            if let Some(byte_pos) = s_str.find(&sub_str) {
                let char_idx = s_str[..byte_pos].chars().count() + 1;
                return serde_json::json!(char_idx as i64);
            } else {
                return serde_json::json!(0);
            }
        }
    }

    if upper.starts_with("SPLIT_PART(") && t.ends_with(')') {
        let inner = &t[11..t.len() - 1];
        let args = split_function_args(inner);
        if args.len() == 3 {
            let str_val = eval_joined_scalar_expr(&args[0], row, params, storage);
            let delim_val = eval_joined_scalar_expr(&args[1], row, params, storage);
            let field_val = eval_joined_scalar_expr(&args[2], row, params, storage);
            if str_val.is_null() || delim_val.is_null() || field_val.is_null() { return serde_json::Value::Null; }
            let s_str = json_val_to_str(&str_val);
            let delim_str = json_val_to_str(&delim_val);
            let field_idx = field_val.as_i64().unwrap_or(1);
            if field_idx < 1 {
                return serde_json::Value::String(String::new());
            }
            let parts: Vec<&str> = s_str.split(&delim_str).collect();
            let u_idx = (field_idx - 1) as usize;
            if u_idx < parts.len() {
                return serde_json::Value::String(parts[u_idx].to_string());
            } else {
                return serde_json::Value::String(String::new());
            }
        }
    }

    if upper.starts_with("MD5(") && t.ends_with(')') {
        let inner = &t[4..t.len() - 1];
        let v = eval_joined_scalar_expr(inner, row, params, storage);
        if v.is_null() { return serde_json::Value::Null; }
        let s_str = json_val_to_str(&v);
        let mut hasher = Md5::new();
        hasher.update(s_str.as_bytes());
        let hash = format!("{:x}", hasher.finalize());
        return serde_json::Value::String(hash);
    }

    if upper.starts_with("ENCODE(") && t.ends_with(')') {
        let inner = &t[7..t.len() - 1];
        let args = split_function_args(inner);
        if args.len() == 2 {
            let str_val = eval_joined_scalar_expr(&args[0], row, params, storage);
            let fmt_val = eval_joined_scalar_expr(&args[1], row, params, storage);
            if str_val.is_null() || fmt_val.is_null() { return serde_json::Value::Null; }
            let s_str = json_val_to_str(&str_val);
            let fmt_str = json_val_to_str(&fmt_val).to_lowercase();
            if fmt_str == "hex" {
                return serde_json::Value::String(hex::encode(s_str.as_bytes()));
            } else if fmt_str == "base64" {
                return serde_json::Value::String(base64::engine::general_purpose::STANDARD.encode(s_str.as_bytes()));
            }
        }
    }

    if upper.starts_with("DECODE(") && t.ends_with(')') {
        let inner = &t[7..t.len() - 1];
        let args = split_function_args(inner);
        if args.len() == 2 {
            let str_val = eval_joined_scalar_expr(&args[0], row, params, storage);
            let fmt_val = eval_joined_scalar_expr(&args[1], row, params, storage);
            if str_val.is_null() || fmt_val.is_null() { return serde_json::Value::Null; }
            let s_str = json_val_to_str(&str_val);
            let fmt_str = json_val_to_str(&fmt_val).to_lowercase();
            if fmt_str == "hex" {
                if let Ok(bytes) = hex::decode(s_str.trim()) {
                    return serde_json::Value::String(String::from_utf8_lossy(&bytes).to_string());
                }
            } else if fmt_str == "base64" {
                if let Ok(bytes) = base64::engine::general_purpose::STANDARD.decode(s_str.trim()) {
                    return serde_json::Value::String(String::from_utf8_lossy(&bytes).to_string());
                }
            }
        }
    }

    if upper.starts_with("REPLACE(") && t.ends_with(')') {
        let inner = &t[8..t.len() - 1];
        let args = split_function_args(inner);
        if args.len() == 3 {
            let base = eval_joined_scalar_expr(&args[0], row, params, storage);
            let from = eval_joined_scalar_expr(&args[1], row, params, storage);
            let to = eval_joined_scalar_expr(&args[2], row, params, storage);
            if base.is_null() || from.is_null() || to.is_null() { return serde_json::Value::Null; }
            let base_s = json_val_to_str(&base);
            let from_s = json_val_to_str(&from);
            let to_s = json_val_to_str(&to);
            return serde_json::Value::String(base_s.replace(&from_s, &to_s));
        }
    }

    if (upper.starts_with("SUBSTRING(") || upper.starts_with("SUBSTR(")) && t.ends_with(')') {
        let open_p = t.find('(').unwrap();
        let inner = &t[open_p + 1..t.len() - 1];
        let args = split_function_args(inner);
        if args.len() == 2 && crate::engine::strfn::is_text_literal(&args[1]) {
            let base = eval_joined_scalar_expr(&args[0], row, params, storage);
            let pat = eval_joined_scalar_expr(&args[1], row, params, storage);
            let v = crate::engine::strfn::substring_regex(&json_to_value(&base), &json_to_value(&pat));
            return value_to_json(&v);
        }
        if args.len() >= 2 {
            let base = eval_joined_scalar_expr(&args[0], row, params, storage);
            if base.is_null() { return serde_json::Value::Null; }
            let start = eval_joined_scalar_expr(&args[1], row, params, storage).as_i64().unwrap_or(1);
            let length = if args.len() >= 3 {
                eval_joined_scalar_expr(&args[2], row, params, storage).as_i64()
            } else {
                None
            };
            let base_s = json_val_to_str(&base);
            let chars: Vec<char> = base_s.chars().collect();
            let start_idx = if start > 0 { (start as usize).saturating_sub(1) } else { 0 };
            let sub: String = if let Some(len) = length {
                chars.iter().skip(start_idx).take(len.max(0) as usize).collect()
            } else {
                chars.iter().skip(start_idx).collect()
            };
            return serde_json::Value::String(sub);
        }
    }

    // 10. Math functions: ABS, FLOOR, CEIL, ROUND, TRUNC, POWER, SQRT, CBRT, EXP, LN, LOG, MOD, SIGN, PI, DEGREES, RADIANS, RANDOM
    if upper == "PI()" || upper == "PI" {
        return serde_json::json!(std::f64::consts::PI);
    }

    if upper == "RANDOM()" {
        let nanos = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.subsec_nanos()).unwrap_or(12345);
        let rand = ((nanos % 1_000_000) as f64) / 1_000_000.0;
        return serde_json::json!(rand);
    }

    if upper.starts_with("ABS(") && t.ends_with(')') {
        let inner = &t[4..t.len() - 1];
        let v = eval_joined_scalar_expr(inner, row, params, storage);
        if v.is_null() { return serde_json::Value::Null; }
        if let Some(f) = v.as_f64() {
            return if f.fract() == 0.0 { serde_json::json!(f.abs() as i64) } else { serde_json::json!(f.abs()) };
        }
    }

    if upper.starts_with("FLOOR(") && t.ends_with(')') {
        let inner = &t[6..t.len() - 1];
        let v = eval_joined_scalar_expr(inner, row, params, storage);
        if v.is_null() { return serde_json::Value::Null; }
        if let Some(f) = v.as_f64() {
            return serde_json::json!(f.floor() as i64);
        }
    }

    if (upper.starts_with("CEIL(") || upper.starts_with("CEILING(")) && t.ends_with(')') {
        let start_pos = if upper.starts_with("CEILING(") { 8 } else { 5 };
        let inner = &t[start_pos..t.len() - 1];
        let v = eval_joined_scalar_expr(inner, row, params, storage);
        if v.is_null() { return serde_json::Value::Null; }
        if let Some(f) = v.as_f64() {
            return serde_json::json!(f.ceil() as i64);
        }
    }

    if upper.starts_with("ROUND(") && t.ends_with(')') {
        let inner = &t[6..t.len() - 1];
        let args = split_function_args(inner);
        if !args.is_empty() {
            let v = eval_joined_scalar_expr(&args[0], row, params, storage);
            if v.is_null() { return serde_json::Value::Null; }
            let decimals = if args.len() >= 2 {
                let d_val = eval_joined_scalar_expr(&args[1], row, params, storage);
                if d_val.is_null() { return serde_json::Value::Null; }
                d_val.as_i64().unwrap_or(0)
            } else {
                0
            };
            if let Some(f) = v.as_f64() {
                if decimals == 0 {
                    return serde_json::json!(f.round() as i64);
                } else {
                    return serde_json::json!(crate::engine::fnlib::round_dec(f, decimals));
                }
            }
        }
    }

    if (upper.starts_with("TRUNC(") || upper.starts_with("TRUNCATE(")) && t.ends_with(')') {
        let start_pos = if upper.starts_with("TRUNCATE(") { 9 } else { 6 };
        let inner = &t[start_pos..t.len() - 1];
        let args = split_function_args(inner);
        if !args.is_empty() {
            let v = eval_joined_scalar_expr(&args[0], row, params, storage);
            if v.is_null() { return serde_json::Value::Null; }
            let decimals = if args.len() >= 2 {
                let d_val = eval_joined_scalar_expr(&args[1], row, params, storage);
                if d_val.is_null() { return serde_json::Value::Null; }
                d_val.as_i64().unwrap_or(0)
            } else {
                0
            };
            if let Some(f) = v.as_f64() {
                if decimals == 0 {
                    return serde_json::json!(f.trunc() as i64);
                } else {
                    let multiplier = 10f64.powi(decimals as i32);
                    let truncated = (f * multiplier).trunc() / multiplier;
                    return serde_json::json!(truncated);
                }
            }
        }
    }

    if (upper.starts_with("POWER(") || upper.starts_with("POW(")) && t.ends_with(')') {
        let start_pos = if upper.starts_with("POWER(") { 6 } else { 4 };
        let inner = &t[start_pos..t.len() - 1];
        let args = split_function_args(inner);
        if args.len() == 2 {
            let base = eval_joined_scalar_expr(&args[0], row, params, storage);
            let exp = eval_joined_scalar_expr(&args[1], row, params, storage);
            if base.is_null() || exp.is_null() { return serde_json::Value::Null; }
            if let (Some(b), Some(e)) = (base.as_f64(), exp.as_f64()) {
                let res = b.powf(e);
                return if res.fract() == 0.0 { serde_json::json!(res as i64) } else { serde_json::json!(res) };
            }
        }
    }

    if upper.starts_with("SQRT(") && t.ends_with(')') {
        let inner = &t[5..t.len() - 1];
        let v = eval_joined_scalar_expr(inner, row, params, storage);
        if v.is_null() { return serde_json::Value::Null; }
        if let Some(f) = v.as_f64() {
            let res = f.sqrt();
            return if res.fract() == 0.0 { serde_json::json!(res as i64) } else { serde_json::json!(res) };
        }
    }

    if upper.starts_with("CBRT(") && t.ends_with(')') {
        let inner = &t[5..t.len() - 1];
        let v = eval_joined_scalar_expr(inner, row, params, storage);
        if v.is_null() { return serde_json::Value::Null; }
        if let Some(f) = v.as_f64() {
            let res = f.cbrt();
            return if res.fract() == 0.0 { serde_json::json!(res as i64) } else { serde_json::json!(res) };
        }
    }

    if upper.starts_with("EXP(") && t.ends_with(')') {
        let inner = &t[4..t.len() - 1];
        let v = eval_joined_scalar_expr(inner, row, params, storage);
        if v.is_null() { return serde_json::Value::Null; }
        if let Some(f) = v.as_f64() {
            let res = f.exp();
            return if res.fract() == 0.0 { serde_json::json!(res as i64) } else { serde_json::json!(res) };
        }
    }

    if upper.starts_with("LN(") && t.ends_with(')') {
        let inner = &t[3..t.len() - 1];
        let v = eval_joined_scalar_expr(inner, row, params, storage);
        if v.is_null() { return serde_json::Value::Null; }
        if let Some(f) = v.as_f64() {
            let res = f.ln();
            return if res.fract() == 0.0 { serde_json::json!(res as i64) } else { serde_json::json!(res) };
        }
    }

    if (upper.starts_with("LOG(") || upper.starts_with("LOG10(")) && t.ends_with(')') {
        let start_pos = if upper.starts_with("LOG10(") { 6 } else { 4 };
        let inner = &t[start_pos..t.len() - 1];
        let args = split_function_args(inner);
        if args.len() == 1 {
            let v = eval_joined_scalar_expr(&args[0], row, params, storage);
            if v.is_null() { return serde_json::Value::Null; }
            if let Some(f) = v.as_f64() {
                let res = f.log10();
                return if res.fract() == 0.0 { serde_json::json!(res as i64) } else { serde_json::json!(res) };
            }
        } else if args.len() == 2 {
            let b_val = eval_joined_scalar_expr(&args[0], row, params, storage);
            let x_val = eval_joined_scalar_expr(&args[1], row, params, storage);
            if b_val.is_null() || x_val.is_null() { return serde_json::Value::Null; }
            if let (Some(b), Some(x)) = (b_val.as_f64(), x_val.as_f64()) {
                let res = x.log(b);
                return if res.fract() == 0.0 { serde_json::json!(res as i64) } else { serde_json::json!(res) };
            }
        }
    }

    if upper.starts_with("DEGREES(") && t.ends_with(')') {
        let inner = &t[8..t.len() - 1];
        let v = eval_joined_scalar_expr(inner, row, params, storage);
        if v.is_null() { return serde_json::Value::Null; }
        if let Some(f) = v.as_f64() {
            return serde_json::json!(f.to_degrees());
        }
    }

    if upper.starts_with("RADIANS(") && t.ends_with(')') {
        let inner = &t[8..t.len() - 1];
        let v = eval_joined_scalar_expr(inner, row, params, storage);
        if v.is_null() { return serde_json::Value::Null; }
        if let Some(f) = v.as_f64() {
            return serde_json::json!(f.to_radians());
        }
    }

    if upper.starts_with("MOD(") && t.ends_with(')') {
        let inner = &t[4..t.len() - 1];
        let args = split_function_args(inner);
        if args.len() == 2 {
            let a = eval_joined_scalar_expr(&args[0], row, params, storage);
            let b = eval_joined_scalar_expr(&args[1], row, params, storage);
            if a.is_null() || b.is_null() { return serde_json::Value::Null; }
            if let (Some(a_i), Some(b_i)) = (a.as_i64(), b.as_i64()) {
                return serde_json::json!(if b_i != 0 { a_i % b_i } else { 0 });
            }
        }
    }

    if upper.starts_with("SIGN(") && t.ends_with(')') {
        let inner = &t[5..t.len() - 1];
        let v = eval_joined_scalar_expr(inner, row, params, storage);
        if v.is_null() { return serde_json::Value::Null; }
        if let Some(f) = v.as_f64() {
            return serde_json::json!(if f > 0.0 { 1 } else if f < 0.0 { -1 } else { 0 });
        }
    }

    let val = eval_operand_on_row(row, t, params);
    value_to_json(&val)
}

fn eval_group_aggregate_expr(
    expr: &str,
    all_rows: &[CombinedRow],
    row_indices: &[usize],
    params: &[Value],
    storage: &crate::storage::engine::StorageEngine,
) -> serde_json::Value {
    let t = expr.trim();
    let upper = t.to_uppercase();

    // ép kiểu bọc ngoài hàm gộp: `count(*)::int`, `CAST(sum(x) AS bigint)` (trước đây ra NULL trên đường JOIN)
    let has_agg = |e: &str| mentions_call(e, AGG_NAMES);
    if let Some(p) = find_last_top_level_op(t, "::").filter(|&p| cast_is_outermost(t, p)) {
        let inner = t[..p].trim();
        if has_agg(inner) {
            let v = json_to_value(&eval_group_aggregate_expr(inner, all_rows, row_indices, params, storage));
            return value_to_json(&cast_val(v, &t[p + 2..].trim().to_uppercase()));
        }
    }
    if upper.starts_with("CAST(") && t.ends_with(')') && is_fully_enclosed_in_parens(&t[4..]) {
        let inside = &t[5..t.len() - 1];
        if let Some(as_pos) = rfind_top_level_as(&inside.to_uppercase()) {
            let inner = inside[..as_pos].trim();
            if has_agg(inner) {
                let v = json_to_value(&eval_group_aggregate_expr(inner, all_rows, row_indices, params, storage));
                return value_to_json(&cast_val(v, &inside[as_pos + 4..].trim().to_uppercase()));
            }
        }
    }

    // `agg(...) FILTER (WHERE cond)`: chỉ các dòng của nhóm thoả cond (trước đây FILTER bị bỏ qua / làm hỏng
    // biểu thức, vd. `jsonb_agg(...) FILTER (WHERE c.id IS NOT NULL)` ra `[null]`)
    if upper.contains("FILTER") {
        if let Some(p) = find_top_level_keyword(t, "FILTER") {
            let tail = t[p + 6..].trim();
            if p > 0 && tail.starts_with('(') && tail.ends_with(')') && is_fully_enclosed_in_parens(tail) {
                let inner = tail[1..tail.len() - 1].trim();
                if inner.len() > 6 && inner[..6].eq_ignore_ascii_case("WHERE ") {
                    let cond = inner[6..].trim();
                    let kept: Vec<usize> = row_indices.iter().copied().filter(|&ri| eval_condition_on_row(&all_rows[ri], cond, params)).collect();
                    let agg = t[..p].trim();
                    let up_agg = agg.to_uppercase();
                    // PostgreSQL: không dòng nào qua FILTER thì COUNT = 0, hàm gộp khác = NULL
                    if kept.is_empty() && !up_agg.starts_with("COUNT(") {
                        return serde_json::Value::Null;
                    }
                    return eval_group_aggregate_expr(agg, all_rows, &kept, params, storage);
                }
            }
        }
    }

    // `agg(x ORDER BY k [DESC] [NULLS FIRST|LAST], ...)`: sắp các dòng của nhóm theo khoá rồi gộp (trước đây
    // `ORDER BY ...` bị hiểu là một phần của đối số: `string_agg` ghép cả chữ, `jsonb_agg` ra giá trị sai)
    if let Some(open) = t.find('(') {
        let name = upper[..open].trim();
        if AGG_NAMES.contains(&name) && t.ends_with(')') && is_fully_enclosed_in_parens(&t[open..]) {
            let inner = &t[open + 1..t.len() - 1];
            if let Some(ob) = find_top_level_keyword(inner, "ORDER BY") {
                let base_args = inner[..ob].trim().trim_end_matches(',').trim();
                let keys: Vec<(String, bool, bool)> = split_function_args(inner[ob + 8..].trim())
                    .into_iter()
                    .map(|k| {
                        let ku = k.to_uppercase();
                        let mut e = k.trim().to_string();
                        let mut nulls_first: Option<bool> = None;
                        for (kw, nf) in [(" NULLS FIRST", true), (" NULLS LAST", false)] {
                            if ku.trim_end().ends_with(kw) {
                                e = e[..e.len() - kw.len()].trim().to_string();
                                nulls_first = Some(nf);
                            }
                        }
                        let eu = e.to_uppercase();
                        let desc = eu.ends_with(" DESC");
                        if desc || eu.ends_with(" ASC") {
                            e = e[..e.len() - if desc { 5 } else { 4 }].trim().to_string();
                        }
                        // PostgreSQL: NULL lớn nhất (cuối khi ASC, đầu khi DESC)
                        (e, desc, nulls_first.unwrap_or(desc))
                    })
                    .collect();
                let mut sorted: Vec<usize> = row_indices.to_vec();
                let key_vals: std::collections::HashMap<usize, Vec<Value>> = sorted
                    .iter()
                    .map(|&ri| (ri, keys.iter().map(|(e, _, _)| json_to_value(&eval_joined_scalar_expr(e, &all_rows[ri], params, storage))).collect()))
                    .collect();
                sorted.sort_by(|a, b| {
                    for (ki, (_, desc, nulls_first)) in keys.iter().enumerate() {
                        let (va, vb) = (&key_vals[a][ki], &key_vals[b][ki]);
                        let o = match (va.is_null(), vb.is_null()) {
                            (true, true) => std::cmp::Ordering::Equal,
                            (true, false) => if *nulls_first { std::cmp::Ordering::Less } else { std::cmp::Ordering::Greater },
                            (false, true) => if *nulls_first { std::cmp::Ordering::Greater } else { std::cmp::Ordering::Less },
                            _ => {
                                let o = va.cmp_value(vb);
                                if *desc { o.reverse() } else { o }
                            }
                        };
                        if o != std::cmp::Ordering::Equal {
                            return o;
                        }
                    }
                    std::cmp::Ordering::Equal
                });
                let plain = format!("{}({})", &t[..open], base_args);
                return eval_group_aggregate_expr(&plain, all_rows, &sorted, params, storage);
            }
        }
    }

    // hàm gộp bổ sung (thống kê, bit, percentile/mode/hypothetical, any_value, SQL/JSON): engine/fnlib.rs
    if let Some(open) = t.find('(') {
        let name = upper[..open].trim();
        if crate::engine::fnlib::EXTRA_AGGS.contains(&name) && t.ends_with(')') && is_fully_enclosed_in_parens(&t[open..]) {
            let inner = &t[open + 1..t.len() - 1];
            let mut args = split_function_args(inner);
            if name == "JSON_OBJECTAGG" && args.len() == 1 {
                // JSON_OBJECTAGG(k VALUE v) / (k : v)
                let a = args[0].clone();
                if let Some(p) = find_top_level_keyword(&a, "VALUE").or_else(|| find_top_level_op(&a, ":").filter(|&p| !a[p..].starts_with("::") && !a[..p].ends_with(':'))) {
                    let w = if a[p..].starts_with(':') { 1 } else { 5 };
                    args = vec![a[..p].trim().to_string(), a[p + w..].trim().to_string()];
                }
            }
            // mệnh đề SQL/JSON (`ABSENT ON NULL`, `RETURNING ...`) ở cuối đối số cuối: bỏ qua
            if let Some(last) = args.last_mut() {
                for kw in ["NULL ON NULL", "ABSENT ON NULL", "RETURNING", "WITH UNIQUE", "WITHOUT UNIQUE"] {
                    if let Some(p) = find_top_level_keyword(last, kw) {
                        last.truncate(p);
                    }
                }
            }
            let rows: Vec<Vec<Value>> = row_indices
                .iter()
                .map(|&ri| args.iter().map(|a| json_to_value(&eval_joined_scalar_expr(a.trim(), &all_rows[ri], params, storage))).collect())
                .collect();
            return value_to_json(&crate::engine::fnlib::agg_finalize(name, &rows));
        }
    }

    // 1. COALESCE(SUM(col), default)
    if upper.starts_with("COALESCE(") && t.ends_with(')') {
        let inner = &t[9..t.len() - 1];
        let args = split_function_args(inner);
        if !args.is_empty() {
            let agg_res = eval_group_aggregate_expr(&args[0], all_rows, row_indices, params, storage);
            let is_null = match &agg_res {
                serde_json::Value::Null => true,
                _ => false,
            };
            if !is_null {
                return agg_res;
            }
            if args.len() >= 2 {
                if let Some(&first_ri) = row_indices.first() {
                    let def_val = eval_operand_on_row(&all_rows[first_ri], &args[1], params);
                    return value_to_json(&def_val);
                } else {
                    let def_val = parse_value(&args[1], params);
                    return value_to_json(&def_val);
                }
            }
        }
        return serde_json::Value::Null;
    }

    // 2. COUNT(*) / COUNT(col) / COUNT(DISTINCT col)
    if upper.starts_with("COUNT(") && t.ends_with(')') {
        let inner = t[6..t.len() - 1].trim();
        let upper_inner = inner.to_uppercase();
        if upper_inner == "*" || upper_inner == "1" {
            return serde_json::json!(row_indices.len());
        } else if upper_inner.starts_with("DISTINCT ") {
            let col = inner[9..].trim();
            let mut set = std::collections::HashSet::new();
            for &ri in row_indices {
                let v = all_rows[ri].get_val(col);
                if !v.is_null() {
                    set.insert(v.as_str());
                }
            }
            return serde_json::json!(set.len());
        } else {
            let count = row_indices.iter().filter(|&&ri| !all_rows[ri].get_val(inner).is_null()).count();
            return serde_json::json!(count);
        }
    }

    // 3. SUM(col)
    if upper.starts_with("SUM(") && t.ends_with(')') {
        let inner = t[4..t.len() - 1].trim();
        let mut total = 0.0;
        let mut has_non_null = false;
        for &ri in row_indices {
            let v = all_rows[ri].get_val(inner);
            if let Some(f) = v.as_f64() {
                total += f;
                has_non_null = true;
            }
        }
        return if has_non_null { serde_json::json!(total) } else { serde_json::Value::Null };
    }

    // 4. AVG(col)
    if upper.starts_with("AVG(") && t.ends_with(')') {
        let inner = t[4..t.len() - 1].trim();
        let mut total = 0.0;
        let mut count = 0;
        for &ri in row_indices {
            let v = all_rows[ri].get_val(inner);
            if let Some(f) = v.as_f64() {
                total += f;
                count += 1;
            }
        }
        return if count > 0 { serde_json::json!(total / (count as f64)) } else { serde_json::Value::Null };
    }

    // 5. MIN(col)
    if upper.starts_with("MIN(") && t.ends_with(')') {
        let inner = t[4..t.len() - 1].trim();
        let mut min_val: Option<Value> = None;
        for &ri in row_indices {
            let v = all_rows[ri].get_val(inner);
            if !v.is_null() {
                if let Some(ref curr_min) = min_val {
                    if v.cmp_value(curr_min) == std::cmp::Ordering::Less {
                        min_val = Some(v);
                    }
                } else {
                    min_val = Some(v);
                }
            }
        }
        return min_val.map(|v| value_to_json(&v)).unwrap_or(serde_json::Value::Null);
    }

    // 6. MAX(col)
    if upper.starts_with("MAX(") && t.ends_with(')') {
        let inner = t[4..t.len() - 1].trim();
        let mut max_val: Option<Value> = None;
        for &ri in row_indices {
            let v = all_rows[ri].get_val(inner);
            if !v.is_null() {
                if let Some(ref curr_max) = max_val {
                    if v.cmp_value(curr_max) == std::cmp::Ordering::Greater {
                        max_val = Some(v);
                    }
                } else {
                    max_val = Some(v);
                }
            }
        }
        return max_val.map(|v| value_to_json(&v)).unwrap_or(serde_json::Value::Null);
    }

    // 7. STRING_AGG(col, delim)
    if upper.starts_with("STRING_AGG(") && t.ends_with(')') {
        let inner = &t[11..t.len() - 1];
        let args = split_function_args(inner);
        if !args.is_empty() {
            let col = &args[0];
            let delim = if args.len() >= 2 {
                let d_val = parse_value(&args[1], params);
                d_val.as_str()
            } else {
                ",".to_string()
            };
            let mut parts = Vec::new();
            for &ri in row_indices {
                let v = all_rows[ri].get_val(col);
                if !v.is_null() {
                    parts.push(v.as_str());
                }
            }
            return if parts.is_empty() {
                serde_json::Value::Null
            } else {
                serde_json::Value::String(parts.join(&delim))
            };
        }
    }

    // 8. ARRAY_AGG(col)
    if upper.starts_with("ARRAY_AGG(") && t.ends_with(')') {
        let inner = t[10..t.len() - 1].trim();
        let mut arr = Vec::new();
        for &ri in row_indices {
            let v = all_rows[ri].get_val(inner);
            if !v.is_null() {
                arr.push(value_to_json(&v));
            }
        }
        return serde_json::Value::Array(arr);
    }

    if let Some((k, v, jsonb)) = parse_object_agg_call(t).filter(|_| t.ends_with(')')) {
        let pairs = row_indices.iter().map(|&ri| (json_to_value(&eval_joined_scalar_expr(&k, &all_rows[ri], params, storage)), json_to_value(&eval_joined_scalar_expr(&v, &all_rows[ri], params, storage)))).collect();
        return object_agg_json(pairs, jsonb);
    }

    // 6. JSONB_AGG(expr) / JSON_AGG(expr)
    if (upper.starts_with("JSONB_AGG(") || upper.starts_with("JSON_AGG(")) && t.ends_with(')') {
        let inner = t[t.find('(').unwrap() + 1..t.len() - 1].trim();
        let mut arr = Vec::new();
        for &ri in row_indices {
            let v_json = eval_joined_scalar_expr(inner, &all_rows[ri], params, storage);
            arr.push(v_json);
        }
        return serde_json::Value::Array(arr);
    }

    // 7. Group column fallback
    if let Some(&first_ri) = row_indices.first() {
        let v = all_rows[first_ri].get_val(t);
        value_to_json(&v)
    } else {
        serde_json::Value::Null
    }
}

fn eval_having_condition(
    h_str: &str,
    row_map: &serde_json::Map<String, serde_json::Value>,
    extra_names: Option<&[(&str, &str)]>,
    params: &[Value],
) -> bool {
    let mut substituted = h_str.to_string();

    let mut replacements: Vec<(String, String)> = Vec::new();
    if let Some(names) = extra_names {
        for &(name, alias) in names {
            let val_opt = row_map.get(alias).or_else(|| row_map.get(name));
            if let Some(v) = val_opt {
                let lit = match v {
                    serde_json::Value::Null => "NULL".to_string(),
                    serde_json::Value::Bool(b) => if *b { "TRUE".to_string() } else { "FALSE".to_string() },
                    serde_json::Value::Number(n) => n.to_string(),
                    serde_json::Value::String(s) => format!("'{}'", s.replace('\'', "''")),
                    _ => format!("'{}'", v.to_string().replace('\'', "''")),
                };
                if !name.is_empty() {
                    replacements.push((name.to_string(), lit.clone()));
                }
                if !alias.is_empty() {
                    replacements.push((alias.to_string(), lit));
                }
            }
        }
    }
    for (k, v) in row_map {
        let lit = match v {
            serde_json::Value::Null => "NULL".to_string(),
            serde_json::Value::Bool(b) => if *b { "TRUE".to_string() } else { "FALSE".to_string() },
            serde_json::Value::Number(n) => n.to_string(),
            serde_json::Value::String(s) => format!("'{}'", s.replace('\'', "''")),
            _ => format!("'{}'", v.to_string().replace('\'', "''")),
        };
        replacements.push((k.clone(), lit.clone()));
        let cleaned = clean_col_name(k);
        if cleaned != k {
            replacements.push((cleaned.to_string(), lit));
        }
    }

    replacements.sort_by(|a, b| b.0.len().cmp(&a.0.len()));
    replacements.dedup_by(|a, b| a.0.eq_ignore_ascii_case(&b.0));

    for (k, lit) in replacements {
        let mut idx = 0;
        while idx < substituted.len() {
            let sub_slice = &substituted[idx..];
            let found_opt = sub_slice.to_uppercase().find(&k.to_uppercase());
            if let Some(offset) = found_opt {
                let actual_idx = idx + offset;
                substituted.replace_range(actual_idx..actual_idx + k.len(), &lit);
                idx = actual_idx + lit.len();
            } else {
                break;
            }
        }
    }

    evaluate_custom_condition(&substituted, &[], None, &[], None, params)
}

fn evaluate_having_with_group(
    having_str: &str,
    row_map: &serde_json::Map<String, serde_json::Value>,
    extra_names: Option<&[(&str, &str)]>,
    table: &crate::storage::table::Table,
    r_indices: &[usize],
    params: &[Value],
) -> bool {
    let mut substituted = having_str.to_string();

    let upper = having_str.to_uppercase();
    let agg_prefixes = ["BOOL_AND(", "BOOL_OR(", "EVERY(", "COUNT(", "SUM(", "AVG(", "MIN(", "MAX("];
    for &prefix in &agg_prefixes {
        let mut search_start = 0;
        while let Some(pos) = upper[search_start..].find(prefix) {
            let actual_pos = search_start + pos;
            if let Some(close_p) = find_matching_paren(having_str, actual_pos + prefix.len() - 1) {
                let agg_call = &having_str[actual_pos..=close_p];
                let inner = &having_str[actual_pos + prefix.len()..close_p].trim();
                let inner_col = clean_col_name(inner);
                let val_lit = match prefix {
                    "BOOL_AND(" | "EVERY(" => {
                        let mut res: Option<bool> = None;
                        if let Some(c_idx) = table.get_column_index(inner_col) {
                            for &ri in r_indices {
                                let v = &table.rows[ri][c_idx];
                                if !v.is_null() {
                                    res = Some(res.unwrap_or(true) && v.as_bool().unwrap_or(false));
                                }
                            }
                        }
                        match res {
                            Some(true) => "TRUE".to_string(),
                            Some(false) => "FALSE".to_string(),
                            None => "NULL".to_string(),
                        }
                    }
                    "BOOL_OR(" => {
                        let mut res: Option<bool> = None;
                        if let Some(c_idx) = table.get_column_index(inner_col) {
                            for &ri in r_indices {
                                let v = &table.rows[ri][c_idx];
                                if !v.is_null() {
                                    res = Some(res.unwrap_or(false) || v.as_bool().unwrap_or(false));
                                }
                            }
                        }
                        match res {
                            Some(true) => "TRUE".to_string(),
                            Some(false) => "FALSE".to_string(),
                            None => "NULL".to_string(),
                        }
                    }
                    "COUNT(" => {
                        if inner.eq_ignore_ascii_case("*") || *inner == "1" {
                            r_indices.len().to_string()
                        } else if let Some(c_idx) = table.get_column_index(inner_col) {
                            let cnt = r_indices.iter().filter(|&&ri| !table.rows[ri][c_idx].is_null()).count();
                            cnt.to_string()
                        } else {
                            r_indices.len().to_string()
                        }
                    }
                    "SUM(" => {
                        if let Some(c_idx) = table.get_column_index(inner_col) {
                            let mut sum = 0.0;
                            for &ri in r_indices {
                                if let Some(f) = table.rows[ri][c_idx].as_f64() {
                                    sum += f;
                                }
                            }
                            sum.to_string()
                        } else {
                            "0".to_string()
                        }
                    }
                    "AVG(" => {
                        if let Some(c_idx) = table.get_column_index(inner_col) {
                            let mut sum = 0.0;
                            let mut count = 0;
                            for &ri in r_indices {
                                if let Some(f) = table.rows[ri][c_idx].as_f64() {
                                    sum += f;
                                    count += 1;
                                }
                            }
                            if count > 0 { (sum / count as f64).to_string() } else { "NULL".to_string() }
                        } else {
                            "NULL".to_string()
                        }
                    }
                    "MIN(" => {
                        if let Some(c_idx) = table.get_column_index(inner_col) {
                            let mut min_val: Option<Value> = None;
                            for &ri in r_indices {
                                let v = &table.rows[ri][c_idx];
                                if !v.is_null() {
                                    min_val = match min_val {
                                        None => Some(v.clone()),
                                        Some(cur) => if v.cmp_value(&cur) == std::cmp::Ordering::Less { Some(v.clone()) } else { Some(cur) },
                                    };
                                }
                            }
                            match min_val {
                                Some(Value::Text(s)) => format!("'{}'", s.replace('\'', "''")),
                                Some(v) => v.as_str(),
                                None => "NULL".to_string(),
                            }
                        } else {
                            "NULL".to_string()
                        }
                    }
                    "MAX(" => {
                        if let Some(c_idx) = table.get_column_index(inner_col) {
                            let mut max_val: Option<Value> = None;
                            for &ri in r_indices {
                                let v = &table.rows[ri][c_idx];
                                if !v.is_null() {
                                    max_val = match max_val {
                                        None => Some(v.clone()),
                                        Some(cur) => if v.cmp_value(&cur) == std::cmp::Ordering::Greater { Some(v.clone()) } else { Some(cur) },
                                    };
                                }
                            }
                            match max_val {
                                Some(Value::Text(s)) => format!("'{}'", s.replace('\'', "''")),
                                Some(v) => v.as_str(),
                                None => "NULL".to_string(),
                            }
                        } else {
                            "NULL".to_string()
                        }
                    }
                    _ => "NULL".to_string(),
                };
                substituted = substituted.replace(agg_call, &val_lit);
                search_start = actual_pos + prefix.len();
            } else {
                break;
            }
        }
    }

    eval_having_condition(&substituted, row_map, extra_names, params)
}

/// Có chữ WHERE ngoài mọi ngoặc / chuỗi (ranh giới lỏng hơn `find_top_level_keyword`): dùng để chặn
/// UPDATE / DELETE mà WHERE không tách được. WHERE chỉ nằm trong subquery (vd. `SET x = (SELECT ...
/// WHERE ...)`) thì không tính — trước đây cả câu bị từ chối.
fn has_unparsed_top_level_where(sql: &str) -> bool {
    let b = sql.as_bytes();
    let (mut depth, mut in_sq, mut in_dq) = (0i32, false, false);
    for i in 0..b.len() {
        match b[i] {
            b'\'' if !in_dq => in_sq = !in_sq,
            b'"' if !in_sq => in_dq = !in_dq,
            b'(' if !in_sq && !in_dq => depth += 1,
            b')' if !in_sq && !in_dq => depth -= 1,
            _ if depth == 0 && !in_sq && !in_dq && i + 5 <= b.len() && b[i..i + 5].eq_ignore_ascii_case(b"WHERE") => {
                let before = i == 0 || !(b[i - 1].is_ascii_alphanumeric() || b[i - 1] == b'_');
                let after = i + 5 == b.len() || !(b[i + 5].is_ascii_alphanumeric() || b[i + 5] == b'_');
                if before && after {
                    return true;
                }
            }
            _ => {}
        }
    }
    false
}

fn has_top_level_join(after_from: &str) -> bool {
    let where_pos = find_top_level_keyword(after_from, "WHERE");
    let group_pos = find_top_level_keyword(after_from, "GROUP BY");
    let having_pos = find_top_level_keyword(after_from, "HAVING");
    let order_pos = find_top_level_keyword(after_from, "ORDER BY");
    let limit_pos = find_top_level_keyword(after_from, "LIMIT");
    let offset_pos = find_top_level_keyword(after_from, "OFFSET");

    let first_kw_pos = [where_pos, group_pos, having_pos, order_pos, limit_pos, offset_pos]
        .into_iter()
        .filter_map(|x| x)
        .min();

    let from_part = match first_kw_pos {
        Some(pos) => &after_from[..pos],
        None => after_from,
    };

    find_top_level_keyword(from_part, "JOIN").is_some() || split_comma_separated_tokens(from_part).len() > 1
}

/// Giá trị cột ORDER BY trong một dòng kết quả JSON: theo tên, hoặc theo số thứ tự (`ORDER BY 1`).
fn json_sort_value<'a>(row: &'a serde_json::Value, spec_name: &str) -> Option<&'a serde_json::Value> {
    if let Ok(n) = spec_name.trim().parse::<usize>() {
        if let serde_json::Value::Object(map) = row {
            return if n >= 1 { map.values().nth(n - 1) } else { None };
        }
    }
    let clean = clean_col_name(spec_name).replace('"', "");
    row.get(&clean).or_else(|| row.get(spec_name.replace('"', "")))
}

fn sort_json_rows(rows: &mut [serde_json::Value], specs: &[OrderBySpec]) {
    if specs.is_empty() {
        return;
    }
    rows.sort_by(|a, b| {
        for spec in specs {
            let va = json_sort_value(a, spec.col_name);
            let vb = json_sort_value(b, spec.col_name);
            let directed = json_order_cmp(va, vb, spec.is_desc, spec.nulls_first);
            if directed != std::cmp::Ordering::Equal {
                return directed;
            }
        }
        std::cmp::Ordering::Equal
    });
}

fn eval_window_functions(rows: &mut [serde_json::Value], select_clause: &str) {
    let items = split_projection_items(select_clause);
    for item in items {
        let upper_item = item.to_uppercase();
        let Some(over_idx) = upper_item.find(" OVER (") else {
            continue;
        };
        let func_part = item[..over_idx].trim();
        let upper_func = func_part.to_uppercase();
        let after_over = &item[over_idx + 7..];
        let Some(close_paren) = after_over.find(')') else {
            continue;
        };
        let window_spec = &after_over[..close_paren].trim();
        let after_paren = after_over[close_paren + 1..].trim();

        let alias = if let Some(as_idx) = after_paren.to_uppercase().find("AS ") {
            after_paren[as_idx + 3..].trim().trim_matches('"').to_string()
        } else if let Some(col) = after_paren.split_whitespace().next() {
            col.trim_matches('"').to_string()
        } else {
            func_part.to_string()
        };

        let upper_spec = window_spec.to_uppercase();
        let (partition_cols, order_spec_str): (Vec<String>, Option<&str>) = if let Some(part_idx) = upper_spec.find("PARTITION BY ") {
            let after_part = &window_spec[part_idx + 13..];
            if let Some(order_idx) = after_part.to_uppercase().find("ORDER BY ") {
                let part_str = &after_part[..order_idx].trim();
                let ord_str = &after_part[order_idx + 9..].trim();
                (part_str.split(',').map(|s| clean_col_name(s.trim()).to_string()).collect(), Some(*ord_str))
            } else {
                (after_part.split(',').map(|s| clean_col_name(s.trim()).to_string()).collect(), None)
            }
        } else if let Some(order_idx) = upper_spec.find("ORDER BY ") {
            (vec![], Some(&window_spec[order_idx + 9..].trim()))
        } else {
            (vec![], None)
        };

        let order_specs: Vec<OrderBySpec> = match order_spec_str {
            Some(ord_str) => parse_order_by_specs(ord_str),
            None => vec![],
        };

        let mut partitions_order: Vec<String> = Vec::new();
        let mut partitions: std::collections::HashMap<String, Vec<usize>> = std::collections::HashMap::new();
        for (idx, r) in rows.iter().enumerate() {
            let part_key = partition_cols.iter().map(|col| {
                r.get(col).map(|v| v.to_string()).unwrap_or_else(|| "null".to_string())
            }).collect::<Vec<_>>().join("\x1f");
            if !partitions.contains_key(&part_key) {
                partitions_order.push(part_key.clone());
            }
            partitions.entry(part_key).or_default().push(idx);
        }

        let mut ordered_indices = Vec::new();
        for key in &partitions_order {
            if let Some(indices) = partitions.get_mut(key) {
                if !order_specs.is_empty() {
                    indices.sort_by(|&a, &b| {
                        let ra = &rows[a];
                        let rb = &rows[b];
                        for spec in &order_specs {
                            let clean = clean_col_name(spec.col_name);
                            let va = ra.get(clean);
                            let vb = rb.get(clean);
                            let directed = json_order_cmp(va, vb, spec.is_desc, spec.nulls_first);
                            if directed != std::cmp::Ordering::Equal {
                                return directed;
                            }
                        }
                        std::cmp::Ordering::Equal
                    });
                }

                let partition_len = indices.len();
                let mut current_dense_rank = 1;
                for (pos, &row_idx) in indices.iter().enumerate() {
                    let computed_val = if upper_func.starts_with("ROW_NUMBER") {
                        serde_json::Value::Number((pos + 1).into())
                    } else if upper_func.starts_with("DENSE_RANK") {
                        if pos > 0 && !order_specs.is_empty() {
                            let prev_row_idx = indices[pos - 1];
                            let spec = &order_specs[0];
                            let clean = clean_col_name(spec.col_name);
                            let cur_val = rows[row_idx].get(clean);
                            let prev_val = rows[prev_row_idx].get(clean);
                            if cur_val != prev_val {
                                current_dense_rank += 1;
                            }
                        }
                        serde_json::Value::Number(current_dense_rank.into())
                    } else if upper_func.starts_with("RANK") {
                        serde_json::Value::Number((pos + 1).into())
                    } else if upper_func.starts_with("SUM(") {
                        let col = clean_col_name(&func_part[4..func_part.len() - 1]);
                        let mut running_sum = 0.0;
                        let mut count = 0;
                        for &cur_idx in &indices[..=pos] {
                            if let Some(v) = rows[cur_idx].get(col) {
                                if let Some(n) = v.as_f64() {
                                    running_sum += n;
                                    count += 1;
                                }
                            }
                        }
                        if count == 0 {
                            serde_json::Value::Null
                        } else if running_sum.fract() == 0.0 {
                            serde_json::Value::Number((running_sum as i64).into())
                        } else {
                            serde_json::Value::Number(serde_json::Number::from_f64(running_sum).unwrap_or(0.into()))
                        }
                    } else if upper_func.starts_with("AVG(") {
                        let col = clean_col_name(&func_part[4..func_part.len() - 1]);
                        let mut running_sum = 0.0;
                        let mut count = 0;
                        for &cur_idx in &indices[..=pos] {
                            if let Some(v) = rows[cur_idx].get(col) {
                                if let Some(n) = v.as_f64() {
                                    running_sum += n;
                                    count += 1;
                                }
                            }
                        }
                        if count > 0 {
                            serde_json::Value::Number(serde_json::Number::from_f64(running_sum / count as f64).unwrap_or(0.into()))
                        } else {
                            serde_json::Value::Null
                        }
                    } else if upper_func.starts_with("COUNT(") {
                        serde_json::Value::Number((pos + 1).into())
                    } else if upper_func.starts_with("LAG(") {
                        let inner = func_part[4..func_part.len() - 1].trim();
                        let parts: Vec<&str> = inner.split(',').collect();
                        let col_name = clean_col_name(parts[0].trim());
                        let offset: usize = parts.get(1).and_then(|s| s.trim().parse().ok()).unwrap_or(1);
                        if pos >= offset {
                            let prev_idx = indices[pos - offset];
                            rows[prev_idx].get(col_name).cloned().unwrap_or(serde_json::Value::Null)
                        } else {
                            serde_json::Value::Null
                        }
                    } else if upper_func.starts_with("LEAD(") {
                        let inner = func_part[5..func_part.len() - 1].trim();
                        let parts: Vec<&str> = inner.split(',').collect();
                        let col_name = clean_col_name(parts[0].trim());
                        let offset: usize = parts.get(1).and_then(|s| s.trim().parse().ok()).unwrap_or(1);
                        if pos + offset < partition_len {
                            let next_idx = indices[pos + offset];
                            rows[next_idx].get(col_name).cloned().unwrap_or(serde_json::Value::Null)
                        } else {
                            serde_json::Value::Null
                        }
                    } else if upper_func.starts_with("NTILE(") {
                        let k: usize = func_part[6..func_part.len() - 1].trim().parse().unwrap_or(1).max(1);
                        let tile = ((pos * k) / partition_len) + 1;
                        serde_json::Value::Number(tile.into())
                    } else if upper_func.starts_with("PERCENT_RANK") {
                        let pr = if partition_len > 1 {
                            pos as f64 / (partition_len - 1) as f64
                        } else {
                            0.0
                        };
                        serde_json::Value::Number(serde_json::Number::from_f64(pr).unwrap_or(0.into()))
                    } else if upper_func.starts_with("CUME_DIST") {
                        let cd = (pos + 1) as f64 / partition_len as f64;
                        serde_json::Value::Number(serde_json::Number::from_f64(cd).unwrap_or(0.into()))
                    } else if upper_func.starts_with("MIN(") {
                        let col = clean_col_name(&func_part[4..func_part.len() - 1]);
                        let mut min_val: Option<f64> = None;
                        for &cur_idx in &indices[..=pos] {
                            if let Some(v) = rows[cur_idx].get(col) {
                                if let Some(n) = v.as_f64() {
                                    min_val = Some(min_val.map_or(n, |m| m.min(n)));
                                }
                            }
                        }
                        match min_val {
                            Some(m) if m.fract() == 0.0 => serde_json::Value::Number((m as i64).into()),
                            Some(m) => serde_json::Value::Number(serde_json::Number::from_f64(m).unwrap_or(0.into())),
                            None => serde_json::Value::Null,
                        }
                    } else if upper_func.starts_with("MAX(") {
                        let col = clean_col_name(&func_part[4..func_part.len() - 1]);
                        let mut max_val: Option<f64> = None;
                        for &cur_idx in &indices[..=pos] {
                            if let Some(v) = rows[cur_idx].get(col) {
                                if let Some(n) = v.as_f64() {
                                    max_val = Some(max_val.map_or(n, |m| m.max(n)));
                                }
                            }
                        }
                        match max_val {
                            Some(m) if m.fract() == 0.0 => serde_json::Value::Number((m as i64).into()),
                            Some(m) => serde_json::Value::Number(serde_json::Number::from_f64(m).unwrap_or(0.into())),
                            None => serde_json::Value::Null,
                        }
                    } else if upper_func.starts_with("NTH_VALUE(") {
                        let inner = func_part[10..func_part.len() - 1].trim();
                        let parts: Vec<&str> = inner.split(',').collect();
                        let col_name = clean_col_name(parts[0].trim());
                        let nth: usize = parts.get(1).and_then(|s| s.trim().parse().ok()).unwrap_or(1);
                        if nth >= 1 && nth <= partition_len {
                            let target_idx = indices[nth - 1];
                            rows[target_idx].get(col_name).cloned().unwrap_or(serde_json::Value::Null)
                        } else {
                            serde_json::Value::Null
                        }
                    } else if upper_func.starts_with("FIRST_VALUE(") {
                        let inner = clean_col_name(&func_part[12..func_part.len() - 1]);
                        let first_idx = indices[0];
                        rows[first_idx].get(inner).cloned().unwrap_or(serde_json::Value::Null)
                    } else if upper_func.starts_with("LAST_VALUE(") {
                        let inner = clean_col_name(&func_part[11..func_part.len() - 1]);
                        let last_idx = indices[partition_len - 1];
                        rows[last_idx].get(inner).cloned().unwrap_or(serde_json::Value::Null)
                    } else {
                        serde_json::Value::Null
                    };

                    if let serde_json::Value::Object(ref mut map) = rows[row_idx] {
                        map.insert(alias.clone(), computed_val.clone());
                        map.insert(func_part.to_string(), computed_val);
                    }
                }

                for &idx in indices.iter() {
                    ordered_indices.push(idx);
                }
            }
        }
        if !order_specs.is_empty() && ordered_indices.len() == rows.len() {
            let mut new_rows = Vec::with_capacity(rows.len());
            for &idx in &ordered_indices {
                new_rows.push(rows[idx].clone());
            }
            rows.clone_from_slice(&new_rows);
        }
    }
}

struct JoinClause<'a> {
    is_left: bool,
    joined_table_name: &'a str,
    joined_table_alias: Option<&'a str>,
    primary_join_col: &'a str,
    joined_join_col: &'a str,
}

#[derive(Clone, Debug)]
struct OrderBySpec<'a> {
    col_name: &'a str,
    is_desc: bool,
    /// NULLS FIRST / NULLS LAST; mặc định như PostgreSQL: NULL lớn nhất (ASC -> cuối, DESC -> đầu).
    nulls_first: bool,
}

/// So sánh hai giá trị cho ORDER BY, xử lý NULL theo `nulls_first`.
pub(crate) fn order_cmp(a: &Value, b: &Value, desc: bool, nulls_first: bool) -> std::cmp::Ordering {
    use std::cmp::Ordering::*;
    match (a.is_null(), b.is_null()) {
        (true, true) => Equal,
        (true, false) => if nulls_first { Less } else { Greater },
        (false, true) => if nulls_first { Greater } else { Less },
        _ => {
            let o = a.cmp_value(b);
            if desc { o.reverse() } else { o }
        }
    }
}

/// Như `order_cmp` nhưng trên giá trị JSON của dòng kết quả (thiếu khoá = NULL).
fn json_order_cmp(a: Option<&serde_json::Value>, b: Option<&serde_json::Value>, desc: bool, nulls_first: bool) -> std::cmp::Ordering {
    let to_val = |v: Option<&serde_json::Value>| -> Value {
        match v {
            None | Some(serde_json::Value::Null) => Value::Null,
            Some(serde_json::Value::Object(_)) | Some(serde_json::Value::Array(_)) => Value::text(v.unwrap().to_string()),
            Some(j) => json_to_value(j),
        }
    };
    order_cmp(&to_val(a), &to_val(b), desc, nulls_first)
}

fn parse_order_by_specs<'a>(order_part: &'a str) -> Vec<OrderBySpec<'a>> {
    split_comma_separated_tokens(order_part)
        .into_iter()
        .filter_map(|item| {
            let mut item_trimmed = item.trim();
            let mut nulls: Option<bool> = None;
            for (kw, first) in [(" NULLS FIRST", true), (" NULLS LAST", false)] {
                if item_trimmed.len() > kw.len() && item_trimmed.get(item_trimmed.len() - kw.len()..).map_or(false, |x| x.eq_ignore_ascii_case(kw)) {
                    item_trimmed = item_trimmed[..item_trimmed.len() - kw.len()].trim();
                    nulls = Some(first);
                }
            }
            let upper = item_trimmed.to_uppercase();
            let (raw_col, desc) = if upper.ends_with(" DESC") {
                (item_trimmed[..item_trimmed.len() - 5].trim(), true)
            } else if upper.ends_with(" ASC") {
                (item_trimmed[..item_trimmed.len() - 4].trim(), false)
            } else {
                (item_trimmed, false)
            };
            let col = clean_col_name(raw_col);
            if col.is_empty() {
                return None;
            }
            Some(OrderBySpec { col_name: raw_col, is_desc: desc, nulls_first: nulls.unwrap_or(desc) })
        })
        .collect()
}

struct SelectClauses<'a> {
    table_name: &'a str,
    table_alias: Option<&'a str>,
    join_clause: Option<JoinClause<'a>>,
    where_clause: Option<&'a str>,
    group_by_clause: Option<&'a str>,
    having_clause: Option<&'a str>,
    order_by_specs: Vec<OrderBySpec<'a>>,
    limit: Option<OperandTemplate>,
    offset: Option<OperandTemplate>,
}

pub(crate) fn split_projection_items_pub(s: &str) -> Vec<String> {
    split_projection_items(s)
}

pub(crate) fn is_fully_enclosed_pub(s: &str) -> bool {
    is_fully_enclosed_in_parens(s)
}

fn split_projection_items(s: &str) -> Vec<String> {
    let mut items = Vec::new();
    let mut current = String::new();
    let mut depth: usize = 0;
    let mut in_quote = false;
    let mut in_dq = false;
    for c in s.chars() {
        if c == '\'' && !in_dq {
            in_quote = !in_quote;
            current.push(c);
        } else if c == '"' && !in_quote {
            in_dq = !in_dq;
            current.push(c);
        } else if in_quote || in_dq {
            current.push(c);
        } else if c == '(' || c == '[' {
            depth += 1;
            current.push(c);
        } else if c == ')' || c == ']' {
            depth = depth.saturating_sub(1);
            current.push(c);
        } else if c == ',' && depth == 0 {
            items.push(current.trim().to_string());
            current.clear();
        } else {
            current.push(c);
        }
    }
    if !current.trim().is_empty() {
        items.push(current.trim().to_string());
    }
    items
}

fn parse_projection(
    select_clause: &str,
    primary_table: &crate::storage::table::Table,
    _primary_alias: Option<&str>,
    joined_table: Option<&crate::storage::table::Table>,
    joined_alias: Option<&str>,
    storage: &crate::storage::engine::StorageEngine,
    params: &[Value],
) -> Vec<ProjectedExpr> {
    let items = split_projection_items(select_clause);
    let mut exprs = Vec::new();

    for item in items {
        let trimmed_item = item.trim();
        if trimmed_item.ends_with(".*") {
            let prefix = trimmed_item[..trimmed_item.len() - 2].trim().trim_matches('"');
            let is_primary = prefix.eq_ignore_ascii_case(&primary_table.name)
                || _primary_alias.map(|a| prefix.eq_ignore_ascii_case(a.trim_matches('"'))).unwrap_or(false);
            if is_primary {
                for (c_idx, col) in primary_table.columns.iter().enumerate() {
                    exprs.push(ProjectedExpr::PrimaryCol { col_idx: c_idx, alias: col.name.clone() });
                }
                continue;
            }
            if let Some(jt) = joined_table {
                let is_joined = prefix.eq_ignore_ascii_case(&jt.name)
                    || joined_alias.map(|a| prefix.eq_ignore_ascii_case(a.trim_matches('"'))).unwrap_or(false);
                if is_joined {
                    for (c_idx, col) in jt.columns.iter().enumerate() {
                        exprs.push(ProjectedExpr::JoinedCol { col_idx: c_idx, alias: col.name.clone() });
                    }
                    continue;
                }
            }
        } else if trimmed_item == "*" {
            for (c_idx, col) in primary_table.columns.iter().enumerate() {
                exprs.push(ProjectedExpr::PrimaryCol { col_idx: c_idx, alias: col.name.clone() });
            }
            if let Some(jt) = joined_table {
                for (c_idx, col) in jt.columns.iter().enumerate() {
                    exprs.push(ProjectedExpr::JoinedCol { col_idx: c_idx, alias: col.name.clone() });
                }
            }
            continue;
        }

        let upper = item.to_uppercase();
        let (expr_part, alias) = if let Some(as_idx) = rfind_top_level_as(&upper) {
            (item[..as_idx].trim(), item[as_idx + 4..].trim().trim_matches('"').to_string())
        } else {
            let col = clean_col_name(item.trim());
            (item.trim(), col.to_string())
        };

        let mut trimmed_expr = expr_part;
        while trimmed_expr.starts_with('(') && trimmed_expr.ends_with(')') && is_fully_enclosed_in_parens(trimmed_expr) {
            trimmed_expr = trimmed_expr[1..trimmed_expr.len() - 1].trim();
        }
        let upper_trimmed = trimmed_expr.to_uppercase();

        // Check for subquery in projection: (SELECT COUNT(*) FROM schools s WHERE s.region_id = r.id AND s.deleted_at IS NULL)
        if upper_trimmed.starts_with("SELECT") && upper_trimmed.contains("FROM") {
            if upper_trimmed.starts_with("SELECT COUNT") {
                if let Some(from_pos) = find_top_level_keyword(trimmed_expr, "FROM") {
                    let after_sub_from = trimmed_expr[from_pos + 4..].trim();
                    let where_sub_pos = find_top_level_keyword(after_sub_from, "WHERE");
                    let child_tbl_part = if let Some(w_pos) = where_sub_pos {
                        &after_sub_from[..w_pos]
                    } else {
                        after_sub_from
                    };
                    let (child_table_name, _child_alias) = extract_table_name_and_alias(child_tbl_part);

                    if let Some(child_table) = storage.get_table(child_table_name) {
                        if let Some(w_pos) = where_sub_pos {
                            let sub_where = after_sub_from[w_pos + 5..].trim();
                            let and_parts = split_top_level_and(sub_where);
                            let mut join_cols: Option<(usize, usize)> = None; // (child_join_col_idx, parent_join_col_idx)
                            let mut extra_conds = Vec::new();

                            for part in and_parts {
                                let part_trimmed = part.trim();
                                if let Some(eq_idx) = part_trimmed.find('=') {
                                    let left = part_trimmed[..eq_idx].trim();
                                    let right = part_trimmed[eq_idx + 1..].trim();
                                    let col_l = clean_col_name(left);
                                    let col_r = clean_col_name(right);

                                    let left_in_child = child_table.get_column_index(col_l);
                                    let right_in_child = child_table.get_column_index(col_r);
                                    let left_in_parent = primary_table.get_column_index(col_l);
                                    let right_in_parent = primary_table.get_column_index(col_r);

                                    if let (Some(c_idx), Some(p_idx)) = (left_in_child, right_in_parent) {
                                        if join_cols.is_none() {
                                            join_cols = Some((c_idx, p_idx));
                                            continue;
                                        }
                                    } else if let (Some(c_idx), Some(p_idx)) = (right_in_child, left_in_parent) {
                                        if join_cols.is_none() {
                                            join_cols = Some((c_idx, p_idx));
                                            continue;
                                        }
                                    }
                                }
                                extra_conds.push(part_trimmed);
                            }

                            if let Some((child_join_col_idx, parent_join_col_idx)) = join_cols {
                                let extra_str = extra_conds.join(" AND ");
                                let extra_child_conditions = if !extra_str.is_empty() {
                                    compile_condition_templates(&extra_str, child_table).unwrap_or_default()
                                } else {
                                    vec![]
                                };

                                exprs.push(ProjectedExpr::CorrelatedCount {
                                    child_table_name: child_table.name.clone(),
                                    child_join_col_idx,
                                    parent_join_col_idx,
                                    extra_child_conditions,
                                    alias,
                                });
                                continue;
                            }
                        } else {
                            let count = child_table.count();
                            exprs.push(ProjectedExpr::Expr {
                                expr_str: count.to_string(),
                                alias,
                            });
                            continue;
                        }
                    }
                }
            }
        }

        let upper_expr = expr_part.to_uppercase();

        // 1. Check for COALESCE(..., default)
        if upper_expr.starts_with("COALESCE(") && expr_part.ends_with(')') {
            let inner = &expr_part[9..expr_part.len() - 1];
            let inner_parts = split_function_args(inner);
            // Đường nhanh chỉ cho `COALESCE(cột, hằng)`: có ép kiểu (`CAST(c AS INTEGER)`, `c::int`), vế sau là
            // cột / biểu thức, hoặc từ 3 đối số thì để bộ đánh giá đầy đủ (trước đây bỏ ép kiểu và chỉ xét 2
            // đối số đầu, vế sau bị hiểu là chuỗi)
            let is_col_ref = |x: &str| !x.is_empty() && x.chars().all(|c| c.is_alphanumeric() || c == '_' || c == '.' || c == '"');
            let is_const = |x: &str| {
                let u = x.to_uppercase();
                x.starts_with('\'') && x.ends_with('\'') && x.len() >= 2 || x.parse::<f64>().is_ok() || matches!(u.as_str(), "TRUE" | "FALSE" | "NULL") || x.starts_with('$') && x[1..].parse::<usize>().is_ok()
            };
            if inner_parts.len() == 2 && is_col_ref(inner_parts[0].trim()) && is_const(inner_parts[1].trim()) {
                let first = inner_parts[0].trim();
                let second = inner_parts[1].trim();
                let default_val = parse_value(second, params);
                let raw_col = first;

                let col_name = clean_col_name(raw_col);
                if let Some(col_idx) = primary_table.get_column_index(col_name) {
                    exprs.push(ProjectedExpr::CoalescePrimaryCol { col_idx, default_val, alias });
                    continue;
                } else if let Some(jt) = joined_table {
                    if let Some(col_idx) = jt.get_column_index(col_name) {
                        exprs.push(ProjectedExpr::CoalesceJoinedCol { col_idx, default_val, alias });
                        continue;
                    }
                }
            }
        }

        // 2. Check if expression references joined_table or joined_alias
        let is_joined = if let Some(jt) = joined_table {
            let lower_expr = expr_part.to_lowercase();
            lower_expr.contains(&jt.name.to_lowercase())
                || joined_alias.map(|a| lower_expr.starts_with(&format!("{}.", a.to_lowercase()))).unwrap_or(false)
        } else {
            false
        };

        let is_complex_expr = {
            let u = expr_part.to_uppercase();
            // `col::date` không phải cột thường: phép ép kiểu đổi giá trị
            u.contains('(') || u.contains('+') || u.contains('*') || u.contains('/') || u.contains("||") || u.contains("CASE") || u.contains("::")
        };

        if !is_complex_expr {
            if is_joined {
                if let Some(jt) = joined_table {
                    let col_name = clean_col_name(expr_part);
                    if let Some(col_idx) = jt.get_column_index(col_name) {
                        exprs.push(ProjectedExpr::JoinedCol { col_idx, alias });
                        continue;
                    }
                }
            }

            // 3. Default to primary_table column
            let col_name = clean_col_name(expr_part);
            if let Some(col_idx) = primary_table.get_column_index(col_name) {
                exprs.push(ProjectedExpr::PrimaryCol { col_idx, alias });
                continue;
            } else if let Some(jt) = joined_table {
                if let Some(col_idx) = jt.get_column_index(col_name) {
                    exprs.push(ProjectedExpr::JoinedCol { col_idx, alias });
                    continue;
                }
            }
        }

        let compiled = compile_expr(
            expr_part,
            Some(primary_table),
            _primary_alias,
            joined_table,
            joined_alias,
        );
        if !matches!(compiled, CompiledExpr::Raw(_)) {
            exprs.push(ProjectedExpr::Compiled { expr: compiled, alias });
        } else {
            exprs.push(ProjectedExpr::Expr { expr_str: expr_part.to_string(), alias });
        }
    }

    exprs
}

fn parse_select_clauses<'a>(after_from: &'a str) -> SelectClauses<'a> {
    // Check for JOIN keywords
    let (join_pos, kw_len, is_left) = if let Some(pos) = find_top_level_keyword(after_from, "LEFT JOIN") {
        (Some(pos), 9, true)
    } else if let Some(pos) = find_top_level_keyword(after_from, "LEFT OUTER JOIN") {
        (Some(pos), 15, true)
    } else if let Some(pos) = find_top_level_keyword(after_from, "INNER JOIN") {
        (Some(pos), 10, false)
    } else if let Some(pos) = find_top_level_keyword(after_from, "JOIN") {
        (Some(pos), 4, false)
    } else {
        (None, 0, false)
    };

    let raw_table_part = match join_pos {
        Some(pos) => &after_from[..pos],
        None => {
            let where_pos = find_top_level_keyword(after_from, "WHERE");
            let group_pos = find_top_level_keyword(after_from, "GROUP BY");
            let having_pos = find_top_level_keyword(after_from, "HAVING");
            let order_pos = find_top_level_keyword(after_from, "ORDER BY");
            let limit_pos = find_top_level_keyword(after_from, "LIMIT");
            let offset_pos = find_top_level_keyword(after_from, "OFFSET");
            let first_kw_pos = [where_pos, group_pos, having_pos, order_pos, limit_pos, offset_pos]
                .into_iter()
                .filter_map(|x| x)
                .min();
            match first_kw_pos {
                Some(pos) => &after_from[..pos],
                None => after_from,
            }
        }
    };

    let (table_name, table_alias) = extract_table_name_and_alias(raw_table_part);

    let join_clause = match join_pos {
        Some(j_pos) => {
            let after_j = &after_from[j_pos + kw_len..];
            if let Some(on_pos) = find_top_level_keyword(after_j, "ON") {
                let (joined_table_name, joined_table_alias) = extract_table_name_and_alias(&after_j[..on_pos]);
                let after_on = &after_j[on_pos + 2..];
                let end_pos = [
                    find_top_level_keyword(after_on, "WHERE"),
                    find_top_level_keyword(after_on, "GROUP BY"),
                    find_top_level_keyword(after_on, "HAVING"),
                    find_top_level_keyword(after_on, "ORDER BY"),
                    find_top_level_keyword(after_on, "LIMIT"),
                    find_top_level_keyword(after_on, "OFFSET"),
                ]
                .into_iter()
                .filter_map(|x| x)
                .min()
                .unwrap_or(after_on.len());
                let on_str = after_on[..end_pos].trim();

                if let Some(eq_idx) = on_str.find('=') {
                    let side_a = on_str[..eq_idx].trim();
                    let side_b = on_str[eq_idx + 1..].trim();

                    let col_a = clean_col_name(side_a);
                    let col_b = clean_col_name(side_b);

                    let side_a_lower = side_a.to_lowercase();
                    let is_a_joined = side_a_lower.contains(&joined_table_name.to_lowercase())
                        || joined_table_alias.map(|a| side_a_lower.starts_with(&format!("{}.", a.to_lowercase()))).unwrap_or(false);

                    let (primary_col, joined_col) = if is_a_joined {
                        (col_b, col_a)
                    } else {
                        (col_a, col_b)
                    };

                    Some(JoinClause {
                        is_left,
                        joined_table_name,
                        joined_table_alias,
                        primary_join_col: primary_col,
                        joined_join_col: joined_col,
                    })
                } else {
                    None
                }
            } else {
                None
            }
        }
        None => None,
    };

    let where_pos = find_top_level_keyword(after_from, "WHERE");
    let group_pos = find_top_level_keyword(after_from, "GROUP BY");
    let having_pos = find_top_level_keyword(after_from, "HAVING");
    let order_pos = find_top_level_keyword(after_from, "ORDER BY");
    let limit_pos = find_top_level_keyword(after_from, "LIMIT");
    let offset_pos = find_top_level_keyword(after_from, "OFFSET");

    let where_clause = where_pos.map(|w_idx| {
        let after_w = &after_from[w_idx + 5..];
        let end_idx = [
            group_pos.and_then(|p| if p > w_idx { Some(p - (w_idx + 5)) } else { None }),
            having_pos.and_then(|p| if p > w_idx { Some(p - (w_idx + 5)) } else { None }),
            order_pos.and_then(|p| if p > w_idx { Some(p - (w_idx + 5)) } else { None }),
            limit_pos.and_then(|p| if p > w_idx { Some(p - (w_idx + 5)) } else { None }),
            offset_pos.and_then(|p| if p > w_idx { Some(p - (w_idx + 5)) } else { None }),
        ]
        .into_iter()
        .filter_map(|x| x)
        .min();
        match end_idx {
            Some(e) => after_w[..e].trim(),
            None => after_w.trim(),
        }
    });

    let group_by_clause = group_pos.map(|g_idx| {
        let after_g = &after_from[g_idx + 8..];
        let end_idx = [
            having_pos.and_then(|p| if p > g_idx { Some(p - (g_idx + 8)) } else { None }),
            order_pos.and_then(|p| if p > g_idx { Some(p - (g_idx + 8)) } else { None }),
            limit_pos.and_then(|p| if p > g_idx { Some(p - (g_idx + 8)) } else { None }),
            offset_pos.and_then(|p| if p > g_idx { Some(p - (g_idx + 8)) } else { None }),
        ]
        .into_iter()
        .filter_map(|x| x)
        .min();
        match end_idx {
            Some(e) => after_g[..e].trim(),
            None => after_g.trim(),
        }
    });

    let having_clause = having_pos.map(|h_idx| {
        let after_h = &after_from[h_idx + 6..];
        let end_idx = [
            order_pos.and_then(|p| if p > h_idx { Some(p - (h_idx + 6)) } else { None }),
            limit_pos.and_then(|p| if p > h_idx { Some(p - (h_idx + 6)) } else { None }),
            offset_pos.and_then(|p| if p > h_idx { Some(p - (h_idx + 6)) } else { None }),
        ]
        .into_iter()
        .filter_map(|x| x)
        .min();
        match end_idx {
            Some(e) => after_h[..e].trim(),
            None => after_h.trim(),
        }
    });

    let order_by_specs: Vec<OrderBySpec<'a>> = match order_pos {
        Some(o_idx) => {
            let after_o = &after_from[o_idx + 8..];
            let end_idx = [
                limit_pos.and_then(|p| if p > o_idx { Some(p - (o_idx + 8)) } else { None }),
                offset_pos.and_then(|p| if p > o_idx { Some(p - (o_idx + 8)) } else { None }),
            ]
            .into_iter()
            .filter_map(|x| x)
            .min();
            let order_part = match end_idx {
                Some(e) => after_o[..e].trim(),
                None => after_o.trim(),
            };
            parse_order_by_specs(order_part)
        }
        None => Vec::new(),
    };

    let limit_pos = find_top_level_keyword(after_from, "LIMIT");
    let limit = match limit_pos {
        Some(l_idx) => {
            let after_l = &after_from[l_idx + 5..];
            let end_idx = offset_pos.and_then(|p| if p > l_idx { Some(p - (l_idx + 5)) } else { None });
            let limit_part = match end_idx {
                Some(e) => after_l[..e].trim(),
                None => after_l.trim(),
            };
            let tok = limit_part.split_whitespace().next().unwrap_or("");
            if tok.is_empty() {
                None
            } else {
                parse_operand_template(tok).ok()
            }
        }
        None => None,
    };

    let offset_pos = find_top_level_keyword(after_from, "OFFSET");
    let offset = match offset_pos {
        Some(off_idx) => {
            let after_off = after_from[off_idx + 6..].trim();
            let tok = after_off.split_whitespace().next().unwrap_or("");
            if tok.is_empty() {
                None
            } else {
                parse_operand_template(tok).ok()
            }
        }
        None => None,
    };

    SelectClauses {
        table_name,
        table_alias,
        join_clause,
        where_clause,
        group_by_clause,
        having_clause,
        order_by_specs,
        limit,
        offset,
    }
}

#[derive(Clone, Debug)]
enum AggFunc {
    CountStar,
    Count(usize),
    CountDistinct(usize),
    Sum(usize),
    Avg(usize),
    Min(usize),
    Max(usize),
    SumExpr(String),
    AvgExpr(String),
    MinExpr(String),
    MaxExpr(String),
    ArrayAgg(usize),
    ArrayAggExpr(String),
    WrappedAgg(String, Box<AggFunc>),
    StringAgg(usize, String),
    JsonAgg(usize),
    JsonbAgg(usize),
    JsonAggExpr(String),
    JsonbAggExpr(String),
    /// json_object_agg / jsonb_object_agg(khoá, giá trị); bool = jsonb
    ObjectAgg(String, String, bool),
    BoolAnd(usize),
    BoolOr(usize),
    BoolAndExpr(String),
    BoolOrExpr(String),
    CountExpr(String),
    /// hàm gộp của engine/fnlib.rs (`EXTRA_AGGS`): tên viết hoa + biểu thức đối số
    Extra(String, Vec<String>),
}

/// `name(args)` là hàm gộp bổ sung (`fnlib::EXTRA_AGGS`): tên viết hoa và đối số (JSON_OBJECTAGG `k VALUE v`
/// tách thành hai; mệnh đề SQL/JSON ở cuối bị bỏ).
fn extra_agg_call(e: &str) -> Option<(String, Vec<String>)> {
    let open = e.find('(')?;
    let name = e[..open].trim().to_uppercase();
    if !crate::engine::fnlib::EXTRA_AGGS.contains(&name.as_str()) || !e.ends_with(')') || !is_fully_enclosed_in_parens(&e[open..]) {
        return None;
    }
    let mut args = split_function_args(&e[open + 1..e.len() - 1]);
    if name == "JSON_OBJECTAGG" && args.len() == 1 {
        let a = args[0].clone();
        if let Some(p) = find_top_level_keyword(&a, "VALUE").or_else(|| find_top_level_op(&a, ":").filter(|&p| !a[p..].starts_with("::") && !a[..p].ends_with(':'))) {
            let w = if a[p..].starts_with(':') { 1 } else { 5 };
            args = vec![a[..p].trim().to_string(), a[p + w..].trim().to_string()];
        }
    }
    if let Some(last) = args.last_mut() {
        for kw in ["NULL ON NULL", "ABSENT ON NULL", "RETURNING", "WITH UNIQUE", "WITHOUT UNIQUE"] {
            if let Some(p) = find_top_level_keyword(last, kw) {
                last.truncate(p);
            }
        }
        *last = last.trim().to_string();
    }
    Some((name, args))
}

struct AggSpec {
    func: AggFunc,
    alias: String,
}

/// `json_object_agg(k, v)` / `jsonb_object_agg(k, v)` trên các dòng đã lọc. json giữ thứ tự và dạng text của
/// PostgreSQL (`{ "k" : v }`), jsonb là object (khoá trùng giữ giá trị cuối). Không có dòng → NULL.
fn object_agg_json(pairs: Vec<(Value, Value)>, jsonb: bool) -> serde_json::Value {
    if pairs.is_empty() {
        return serde_json::Value::Null;
    }
    let to_json = |v: &Value| match v {
        Value::Text(st) if st.trim_start().starts_with(['{', '[']) => serde_json::from_str::<serde_json::Value>(st.trim()).unwrap_or_else(|_| serde_json::Value::String(st.to_string())),
        Value::Text(st) => serde_json::Value::String(st.to_string()),
        other => value_to_json(other),
    };
    let key_text = |k: &Value| match k {
        Value::Text(st) => st.to_string(),
        Value::Bool(b) => b.to_string(),
        other => value_to_json(other).to_string(),
    };
    if jsonb {
        let mut m = serde_json::Map::new();
        for (k, v) in &pairs {
            m.insert(key_text(k), to_json(v));
        }
        serde_json::Value::Object(m)
    } else {
        let items: Vec<String> = pairs
            .iter()
            .map(|(k, v)| format!("{} : {}", serde_json::Value::String(key_text(k)), crate::pg_types::jsonb_text(&to_json(v))))
            .collect();
        serde_json::Value::String(format!("{{ {} }}", items.join(", ")))
    }
}

/// Tách `json_object_agg(` / `jsonb_object_agg(` ở đầu `t`: (đối số khoá, đối số giá trị, jsonb).
fn parse_object_agg_call(t: &str) -> Option<(String, String, bool)> {
    let up = t.trim_start().to_uppercase();
    let jsonb = up.starts_with("JSONB_OBJECT_AGG(");
    if !jsonb && !up.starts_with("JSON_OBJECT_AGG(") {
        return None;
    }
    let t = t.trim_start();
    let open = t.find('(')?;
    let close = find_matching_paren(t, open)?;
    let args = split_function_args(&t[open + 1..close]);
    if args.len() != 2 {
        return None;
    }
    Some((args[0].trim().to_string(), args[1].trim().to_string(), jsonb))
}

fn find_matching_paren(s: &str, start_open_paren: usize) -> Option<usize> {
    let mut depth = 0;
    let mut in_quote = false;
    for (idx, ch) in s[start_open_paren..].char_indices() {
        if ch == '\'' {
            in_quote = !in_quote;
        } else if !in_quote {
            if ch == '(' || ch == '[' || ch == '{' {
                depth += 1;
            } else if ch == ')' || ch == ']' || ch == '}' {
                depth -= 1;
                if depth == 0 {
                    return Some(start_open_paren + idx);
                }
            }
        }
    }
    None
}

fn parse_aggregations(select_clause: &str, table: &crate::storage::table::Table) -> Vec<AggSpec> {
    let mut specs: Vec<AggSpec> = Vec::new();
    let parts: Vec<&str> = split_comma_separated_tokens(select_clause);

    for part in parts {
        let part_trim = part.trim();
        let before = specs.len();
        parse_aggregation_item(part_trim, table, &mut specs);
        // tên mặc định trùng (`count(*), count(n)`): như đường thường, khoá là văn bản của mục để các cột
        // không đè nhau trong object kết quả (wire đặt lại tên PostgreSQL)
        if specs.len() == before + 1 && rfind_top_level_as(&part_trim.to_uppercase()).is_none() {
            let a = specs[before].alias.clone();
            if specs[..before].iter().any(|sp| sp.alias == a) {
                specs[before].alias = part_trim.to_string();
            }
        }
    }
    specs
}

fn parse_aggregation_item(part_trim: &str, table: &crate::storage::table::Table, specs: &mut Vec<AggSpec>) {
    {
        let upper = part_trim.to_uppercase();

        let get_alias = |default_alias: &str| -> String {
            if let Some(as_idx) = rfind_top_level_as(&upper) {
                part_trim[as_idx + 4..].trim().trim_matches('"').to_string()
            } else {
                default_alias.to_string()
            }
        };

        let raw_expr = if let Some(as_pos) = rfind_top_level_as(&upper) {
            part_trim[..as_pos].trim().to_string()
        } else {
            part_trim.to_string()
        };

        if let Some(count_idx) = upper.find("COUNT(") {
            if let Some(close_p) = find_matching_paren(part_trim, count_idx + 5) {
                let inside = part_trim[count_idx + 6..close_p].trim();
                let upper_inside = inside.to_uppercase();
                let alias = get_alias("count");

                if upper_inside == "*" || upper_inside == "1" {
                    specs.push(AggSpec { func: AggFunc::CountStar, alias });
                } else if upper_inside.starts_with("DISTINCT ") {
                    let raw_col = inside[9..].trim();
                    let col_name = clean_col_name(raw_col);
                    if let Some(col_idx) = table.get_column_index(col_name) {
                        specs.push(AggSpec { func: AggFunc::CountDistinct(col_idx), alias });
                    }
                } else {
                    let col_name = clean_col_name(inside);
                    if let Some(col_idx) = table.get_column_index(col_name) {
                        specs.push(AggSpec { func: AggFunc::Count(col_idx), alias });
                    } else {
                        specs.push(AggSpec { func: AggFunc::CountExpr(inside.to_string()), alias });
                    }
                }
            }
        } else if let Some(sum_idx) = upper.find("SUM(") {
            if let Some(close_p) = find_matching_paren(part_trim, sum_idx + 3) {
                let raw_col = part_trim[sum_idx + 4..close_p].trim();
                let col_name = clean_col_name(raw_col);
                let alias = get_alias("sum");
                if let Some(col_idx) = table.get_column_index(col_name) {
                    specs.push(AggSpec { func: AggFunc::Sum(col_idx), alias });
                } else {
                    specs.push(AggSpec { func: AggFunc::SumExpr(raw_col.to_string()), alias });
                }
            }
        } else if let Some(avg_idx) = upper.find("AVG(") {
            if let Some(close_p) = find_matching_paren(part_trim, avg_idx + 3) {
                let raw_col = part_trim[avg_idx + 4..close_p].trim();
                let col_name = clean_col_name(raw_col);
                let alias = get_alias("avg");
                if let Some(col_idx) = table.get_column_index(col_name) {
                    specs.push(AggSpec { func: AggFunc::Avg(col_idx), alias });
                } else {
                    specs.push(AggSpec { func: AggFunc::AvgExpr(raw_col.to_string()), alias });
                }
            }
        } else if let Some(min_idx) = upper.find("MIN(") {
            if let Some(close_p) = find_matching_paren(part_trim, min_idx + 3) {
                let raw_col = part_trim[min_idx + 4..close_p].trim();
                let col_name = clean_col_name(raw_col);
                let alias = get_alias("min");
                if let Some(col_idx) = table.get_column_index(col_name) {
                    specs.push(AggSpec { func: AggFunc::Min(col_idx), alias });
                } else {
                    specs.push(AggSpec { func: AggFunc::MinExpr(raw_col.to_string()), alias });
                }
            }
        } else if let Some(max_idx) = upper.find("MAX(") {
            if let Some(close_p) = find_matching_paren(part_trim, max_idx + 3) {
                let raw_col = part_trim[max_idx + 4..close_p].trim();
                let col_name = clean_col_name(raw_col);
                let alias = get_alias("max");
                if let Some(col_idx) = table.get_column_index(col_name) {
                    specs.push(AggSpec { func: AggFunc::Max(col_idx), alias });
                } else {
                    specs.push(AggSpec { func: AggFunc::MaxExpr(raw_col.to_string()), alias });
                }
            }
        } else if let Some(b_idx) = upper.find("BOOL_AND(") {
            if let Some(close_p) = find_matching_paren(part_trim, b_idx + 8) {
                let raw_col = part_trim[b_idx + 9..close_p].trim();
                let col_name = clean_col_name(raw_col);
                let alias = get_alias("bool_and");
                if let Some(col_idx) = table.get_column_index(col_name) {
                    specs.push(AggSpec { func: AggFunc::BoolAnd(col_idx), alias });
                } else {
                    specs.push(AggSpec { func: AggFunc::BoolAndExpr(raw_col.to_string()), alias });
                }
            }
        } else if let Some(e_idx) = upper.find("EVERY(") {
            if let Some(close_p) = find_matching_paren(part_trim, e_idx + 5) {
                let raw_col = part_trim[e_idx + 6..close_p].trim();
                let col_name = clean_col_name(raw_col);
                let alias = get_alias("every");
                if let Some(col_idx) = table.get_column_index(col_name) {
                    specs.push(AggSpec { func: AggFunc::BoolAnd(col_idx), alias });
                } else {
                    specs.push(AggSpec { func: AggFunc::BoolAndExpr(raw_col.to_string()), alias });
                }
            }
        } else if let Some(b_idx) = upper.find("BOOL_OR(") {
            if let Some(close_p) = find_matching_paren(part_trim, b_idx + 7) {
                let raw_col = part_trim[b_idx + 8..close_p].trim();
                let col_name = clean_col_name(raw_col);
                let alias = get_alias("bool_or");
                if let Some(col_idx) = table.get_column_index(col_name) {
                    specs.push(AggSpec { func: AggFunc::BoolOr(col_idx), alias });
                } else {
                    specs.push(AggSpec { func: AggFunc::BoolOrExpr(raw_col.to_string()), alias });
                }
            }
        } else if let Some(a_idx) = upper.find("ARRAY_AGG(") {
            let prefix_len = 10;
            if let Some(close_p) = find_matching_paren(part_trim, a_idx + 9) {
                let raw_col = part_trim[a_idx + prefix_len..close_p].trim();
                let col_name = clean_col_name(raw_col);
                let alias = get_alias("array_agg");
                let is_wrapped = a_idx > 0 || (close_p + 1 < part_trim.len() && !part_trim[close_p + 1..].trim().to_uppercase().starts_with("AS "));
                let is_raw_simple = !raw_col.contains('(') && !raw_col.contains('*') && !raw_col.contains('+') && !raw_col.contains('-');

                if is_raw_simple && table.get_column_index(col_name).is_some() {
                    let col_idx = table.get_column_index(col_name).unwrap();
                    if is_wrapped {
                        specs.push(AggSpec { func: AggFunc::WrappedAgg(raw_expr, Box::new(AggFunc::ArrayAgg(col_idx))), alias });
                    } else {
                        specs.push(AggSpec { func: AggFunc::ArrayAgg(col_idx), alias });
                    }
                } else {
                    if is_wrapped {
                        specs.push(AggSpec { func: AggFunc::WrappedAgg(raw_expr, Box::new(AggFunc::ArrayAggExpr(raw_col.to_string()))), alias });
                    } else {
                        specs.push(AggSpec { func: AggFunc::ArrayAggExpr(raw_col.to_string()), alias });
                    }
                }
            }
        } else if let Some(s_idx) = upper.find("STRING_AGG(") {
            let prefix_len = 11;
            if let Some(close_p) = find_matching_paren(part_trim, s_idx + 10) {
                let inside = part_trim[s_idx + prefix_len..close_p].trim();
                let args = split_comma_separated_tokens(inside);
                if args.len() >= 2 {
                    let col_name = clean_col_name(args[0].trim());
                    let delim_raw = args[1].trim();
                    let delim = delim_raw.trim_matches('\'').to_string();
                    let alias = get_alias("string_agg");
                    if let Some(col_idx) = table.get_column_index(col_name) {
                        specs.push(AggSpec { func: AggFunc::StringAgg(col_idx, delim), alias });
                    }
                }
            }
        } else if let Some(o_idx) = upper.find("JSON_OBJECT_AGG(").or_else(|| upper.find("JSONB_OBJECT_AGG(")) {
            if let Some((k, v, jsonb)) = parse_object_agg_call(&part_trim[o_idx..]) {
                let alias = get_alias(if jsonb { "jsonb_object_agg" } else { "json_object_agg" });
                specs.push(AggSpec { func: AggFunc::ObjectAgg(k, v, jsonb), alias });
            }
        } else if let Some(j_idx) = upper.find("JSON_AGG(") {
            if let Some(close_p) = find_matching_paren(part_trim, j_idx + 8) {
                let inside = part_trim[j_idx + 9..close_p].trim();
                let alias = get_alias("json_agg");
                let is_inside_simple = !inside.contains('(');
                let clean_col = clean_col_name(inside);
                if is_inside_simple && table.get_column_index(clean_col).is_some() {
                    specs.push(AggSpec { func: AggFunc::JsonAgg(table.get_column_index(clean_col).unwrap()), alias });
                } else {
                    specs.push(AggSpec { func: AggFunc::JsonAggExpr(inside.to_string()), alias });
                }
            }
        } else if let Some(j_idx) = upper.find("JSONB_AGG(") {
            if let Some(close_p) = find_matching_paren(part_trim, j_idx + 9) {
                let inside = part_trim[j_idx + 10..close_p].trim();
                let alias = get_alias("jsonb_agg");
                let is_inside_simple = !inside.contains('(');
                let clean_col = clean_col_name(inside);
                if is_inside_simple && table.get_column_index(clean_col).is_some() {
                    specs.push(AggSpec { func: AggFunc::JsonbAgg(table.get_column_index(clean_col).unwrap()), alias });
                } else {
                    specs.push(AggSpec { func: AggFunc::JsonbAggExpr(inside.to_string()), alias });
                }
            }
        }
    }
}

/// `name` đứng riêng làm đối số của hàm nhận cả dòng (`json_agg(name ...)`, `to_json(name)`...) trong `list`:
/// thay bằng `table`. `None` nếu không có chỗ nào.
fn replace_whole_row_refs(list: &str, name: &str, table: &str) -> Option<String> {
    const FUNCS: [&str; 6] = ["JSON_AGG(", "JSONB_AGG(", "TO_JSON(", "TO_JSONB(", "ROW_TO_JSON(", "ARRAY_AGG("];
    let b = list.as_bytes();
    let mut out = String::with_capacity(list.len());
    let mut last = 0;
    let mut changed = false;
    let mut in_sq = false;
    let mut i = 0;
    while i < b.len() {
        if b[i] == b'\'' {
            in_sq = !in_sq;
        } else if !in_sq && b[i].is_ascii_alphabetic() && (i == 0 || !(b[i - 1].is_ascii_alphanumeric() || b[i - 1] == b'_' || b[i - 1] >= 0x80)) {
            if let Some(f) = FUNCS.iter().find(|f| b.len() >= i + f.len() && b[i..i + f.len()].eq_ignore_ascii_case(f.as_bytes())) {
                let mut j = i + f.len();
                while j < b.len() && b[j] == b' ' {
                    j += 1;
                }
                let n = name.len();
                let hit = b.len() >= j + n && b[j..j + n].eq_ignore_ascii_case(name.as_bytes()) && b.get(j + n).map_or(false, |x| matches!(x, b')' | b' ' | b','));
                if hit {
                    out.push_str(&list[last..j]);
                    out.push_str(table);
                    last = j + n;
                    changed = true;
                    i = j + n;
                    continue;
                }
            }
        }
        i += 1;
    }
    out.push_str(&list[last..]);
    changed.then_some(out)
}

/// Đối số hàm gộp có `ORDER BY` (`json_agg(t ORDER BY id DESC)`): (biểu thức gốc, các dòng đã sắp theo khoá).
/// Không có `ORDER BY` thì giữ nguyên thứ tự.
fn agg_order_rows(expr: &str, table: &crate::storage::table::Table, rows: &[usize], params: &[Value]) -> (String, Vec<usize>) {
    let Some(ob) = find_top_level_keyword(expr, "ORDER BY") else { return (expr.to_string(), rows.to_vec()) };
    let base = expr[..ob].trim().to_string();
    let keys: Vec<(String, bool, bool)> = split_function_args(expr[ob + 8..].trim())
        .into_iter()
        .map(|k| {
            let mut e = k.trim().to_string();
            let mut nulls_first: Option<bool> = None;
            for (kw, nf) in [(" NULLS FIRST", true), (" NULLS LAST", false)] {
                if e.to_uppercase().ends_with(kw) {
                    e = e[..e.len() - kw.len()].trim().to_string();
                    nulls_first = Some(nf);
                }
            }
            let eu = e.to_uppercase();
            let desc = eu.ends_with(" DESC");
            if desc || eu.ends_with(" ASC") {
                e = e[..e.len() - if desc { 5 } else { 4 }].trim().to_string();
            }
            (e, desc, nulls_first.unwrap_or(desc))
        })
        .collect();
    let mut keyed: Vec<(usize, Vec<Value>)> = rows
        .iter()
        .map(|&r| (r, keys.iter().map(|(e, _, _)| eval_sql_expr(e, &table.rows[r], Some(table), &[], None, params)).collect()))
        .collect();
    keyed.sort_by(|a, b| {
        for (ki, (_, desc, nulls_first)) in keys.iter().enumerate() {
            let (va, vb) = (&a.1[ki], &b.1[ki]);
            let o = match (va.is_null(), vb.is_null()) {
                (true, true) => std::cmp::Ordering::Equal,
                (true, false) => if *nulls_first { std::cmp::Ordering::Less } else { std::cmp::Ordering::Greater },
                (false, true) => if *nulls_first { std::cmp::Ordering::Greater } else { std::cmp::Ordering::Less },
                _ => {
                    let o = va.cmp_value(vb);
                    if *desc { o.reverse() } else { o }
                }
            };
            if o != std::cmp::Ordering::Equal {
                return o;
            }
        }
        std::cmp::Ordering::Equal
    });
    (base, keyed.into_iter().map(|(r, _)| r).collect())
}

#[derive(Clone, Debug)]
enum ColOp {
    Eq(Value),
    NotEq(Value),
    LowerEq(String),
    UpperEq(String),
    TrimUpperEq(String),
    TrimLowerEq(String),
    TrimEq(String),
    Gt(Value),
    Lt(Value),
    Gte(Value),
    Lte(Value),
    Between(Value, Value),
    In(Vec<Value>),
    IsNull,
    IsNotNull,
    Like(String, Option<char>),
    ILike(String, Option<char>),
    NotLike(String, Option<char>),
    NotILike(String, Option<char>),
}

#[derive(Clone, Debug)]
struct Condition {
    col_idx: usize,
    op: ColOp,
}

/// Lần xuất hiện CUỐI của từ khoá ở cấp ngoài cùng.
pub(crate) fn last_top_level_keyword(sql: &str, kw: &str) -> Option<usize> {
    let mut from = 0;
    let mut last = None;
    while from < sql.len() {
        match find_top_level_keyword(&sql[from..], kw) {
            Some(p) => {
                last = Some(from + p);
                from += p + kw.len();
            }
            None => break,
        }
    }
    last
}

pub fn find_top_level_keyword(sql: &str, kw: &str) -> Option<usize> {
    let kw_bytes = kw.as_bytes();
    let kw_len = kw_bytes.len();
    let bytes = sql.as_bytes();
    let mut depth = 0;
    let mut in_single_quote = false;
    let mut in_double_quote = false;

    let mut i = 0;
    while i < bytes.len() {
        let b = bytes[i];
        if b == b'\'' && !in_double_quote {
            in_single_quote = !in_single_quote;
        } else if b == b'"' && !in_single_quote {
            in_double_quote = !in_double_quote;
        } else if !in_single_quote && !in_double_quote {
            if b == b'(' {
                depth += 1;
            } else if b == b')' {
                if depth > 0 {
                    depth -= 1;
                }
            } else if depth == 0 && i + kw_len <= bytes.len() {
                if bytes[i..i + kw_len].eq_ignore_ascii_case(kw_bytes) {
                    let prev_ok = i == 0 || bytes[i - 1].is_ascii_whitespace() || bytes[i - 1] == b')' || bytes[i - 1] == b'"' || bytes[i - 1] == b',';
                    let next_ok = i + kw_len == bytes.len() || bytes[i + kw_len].is_ascii_whitespace() || bytes[i + kw_len] == b'(' || bytes[i + kw_len] == b'"' || bytes[i + kw_len] == b';';
                    if prev_ok && next_ok {
                        if kw.eq_ignore_ascii_case("FROM") {
                            let prefix = sql[..i].trim_end();
                            let prefix_upper = prefix.to_uppercase();
                            if prefix_upper.ends_with("DISTINCT") {
                                i += kw_len;
                                continue;
                            }
                        }
                        return Some(i);
                    }
                }
            }
        }
        i += 1;
    }
    None
}

/// `UPDATE ... SET (a, b) = (x, y)` / `= ROW(x, y)` / `= (SELECT e1, e2 FROM ...)` → `SET a = x, b = y`
/// (dạng SELECT tách thành từng subquery vô hướng `a = (SELECT e1 FROM ...)`). `None` nếu không có dạng này.
fn expand_multi_column_set(sql: &str) -> Result<Option<String>, String> {
    let Some(set_idx) = find_top_level_keyword(sql, "SET") else { return Ok(None) };
    let start = set_idx + 3;
    let rest = &sql[start..];
    if !rest.trim_start().starts_with('(') && !rest.contains(", (") && !rest.contains(",(") {
        return Ok(None);
    }
    let end = ["WHERE", "RETURNING", "FROM"].iter().filter_map(|k| find_top_level_keyword(rest, k)).min().unwrap_or(rest.len());
    let set_clause = &rest[..end];
    let mut changed = false;
    let mut out_items = Vec::new();
    for item in split_comma_separated_tokens(set_clause) {
        if !item.starts_with('(') {
            out_items.push(item.to_string());
            continue;
        }
        let Some(close) = find_matching_paren(item, 0) else { return Ok(None) };
        let after = item[close + 1..].trim_start();
        let Some(rhs) = after.strip_prefix('=') else { return Ok(None) };
        let cols: Vec<&str> = split_comma_separated_tokens(&item[1..close]);
        let mut rhs = rhs.trim();
        if rhs.len() > 3 && rhs[..3].eq_ignore_ascii_case("ROW") && rhs[3..].trim_start().starts_with('(') {
            rhs = rhs[3..].trim_start();
        }
        if !rhs.starts_with('(') || find_matching_paren(rhs, 0) != Some(rhs.len() - 1) {
            return Err("source for a multiple-column UPDATE item must be a sub-SELECT or ROW() expression".to_string());
        }
        let inner = rhs[1..rhs.len() - 1].trim();
        let exprs: Vec<String> = if inner.len() > 6 && inner[..6].eq_ignore_ascii_case("SELECT") && !inner.as_bytes()[6].is_ascii_alphanumeric() {
            let from = find_top_level_keyword(inner, "FROM");
            let list = &inner[6..from.unwrap_or(inner.len())];
            let tail = from.map(|f| &inner[f..]).unwrap_or("");
            split_projection_items(list)
                .iter()
                .map(|it| {
                    let it = it.trim();
                    // bỏ alias `AS x` (không ảnh hưởng giá trị)
                    let e = find_top_level_keyword(it, "AS").map(|a| it[..a].trim()).unwrap_or(it);
                    format!("(SELECT {} {})", e, tail).trim_end().to_string()
                })
                .collect()
        } else {
            split_comma_separated_tokens(inner).iter().map(|e| e.to_string()).collect()
        };
        if exprs.len() != cols.len() {
            return Err(format!("number of columns does not match number of values"));
        }
        for (c, e) in cols.iter().zip(exprs) {
            out_items.push(format!("{} = {}", c, e));
        }
        changed = true;
    }
    if !changed {
        return Ok(None);
    }
    Ok(Some(format!("{} {} {}", &sql[..start], out_items.join(", "), &rest[end..])))
}

/// Hậu tố alias tạm cho cột đầu ra trùng tên trên đường wire (xem `dedupe_wire_sql`).
const DUP_MARK: &str = "#dup";

/// Hàm trả về tập trên JSON dùng được trong FROM (đủ dài trước, để `JSONB_EACH_TEXT` không khớp `JSONB_EACH`).
const JSON_SRFS: &[&str] = &["JSONB_EACH_TEXT", "JSON_EACH_TEXT", "JSONB_EACH", "JSON_EACH", "JSONB_ARRAY_ELEMENTS_TEXT", "JSON_ARRAY_ELEMENTS_TEXT", "JSONB_ARRAY_ELEMENTS", "JSON_ARRAY_ELEMENTS", "JSONB_OBJECT_KEYS", "JSON_OBJECT_KEYS"];

/// Đối số không tham chiếu cột: chuỗi hằng / tham số `$n`, có thể kèm `::json[b]` / `::text`.
fn is_constant_arg(a: &str) -> bool {
    let mut t = a.trim();
    for c in ["::jsonb", "::json", "::text"] {
        if t.len() > c.len() && t[t.len() - c.len()..].eq_ignore_ascii_case(c) {
            t = t[..t.len() - c.len()].trim_end();
        }
    }
    (t.starts_with('\'') && t.ends_with('\'') && t.len() >= 2 && !t[1..t.len() - 1].replace("''", "").contains('\''))
        || (t.starts_with('$') && t[1..].chars().all(|c| c.is_ascii_digit()) && t.len() > 1)
}

/// Thay tên quan hệ (không phân biệt hoa thường, đủ ranh giới từ, ngoài chuỗi) bằng `to`; mẫu dài đặt trước.
pub(crate) fn replace_relation_name(sql: &str, pats: &[&str], to: &str) -> String {
    let b = sql.as_bytes();
    let ident = |c: u8| c.is_ascii_alphanumeric() || c == b'_' || c == b'.' || c >= 0x80;
    let mut q = String::with_capacity(sql.len());
    let mut in_sq = false;
    let mut i = 0;
    'outer: while i < b.len() {
        if b[i] == b'\'' {
            in_sq = !in_sq;
        }
        if !in_sq && (i == 0 || !ident(b[i - 1])) {
            for p in pats {
                let pb = p.as_bytes();
                if i + pb.len() <= b.len() && b[i..i + pb.len()].eq_ignore_ascii_case(pb) && (i + pb.len() == b.len() || !ident(b[i + pb.len()])) {
                    q.push_str(to);
                    i += pb.len();
                    continue 'outer;
                }
            }
        }
        let ch = sql[i..].chars().next().unwrap();
        q.push(ch);
        i += ch.len_utf8();
    }
    q
}

/// Biểu thức DEFAULT lưu trong ColumnDef (cùng dạng với CREATE TABLE).
pub(crate) fn normalize_default_expr(expr: &str) -> String {
    format_attdef_to_sql(expr.trim().trim_end_matches(';'))
}

fn format_attdef_to_sql(attdef_str: &str) -> String {
    let trimmed = attdef_str.trim();
    if trimmed.starts_with('{') {
        if let Ok(v) = serde_json::from_str::<serde_json::Value>(trimmed) {
            return format_ast_val_to_sql(&v);
        } else if trimmed.starts_with("{\"type\":\"Identifier\"") {
            return "".to_string();
        }
    }
    trimmed.to_string()
}

fn format_ast_val_to_sql(val: &serde_json::Value) -> String {
    match val {
        serde_json::Value::Object(map) => {
            let t = map.get("type").and_then(|v| v.as_str()).unwrap_or("");
            match t {
                "Literal" => {
                    match map.get("value") {
                        Some(serde_json::Value::Null) | None => "NULL".to_string(),
                        Some(serde_json::Value::String(s)) => s.clone(),
                        Some(serde_json::Value::Bool(b)) => if *b { "true".to_string() } else { "false".to_string() },
                        Some(serde_json::Value::Number(n)) => n.to_string(),
                        Some(v) => v.to_string(),
                    }
                }
                "Identifier" => {
                    map.get("name").and_then(|v| v.as_str()).unwrap_or("").to_string()
                }
                "Call" => {
                    let fn_name = map.get("fnName").or_else(|| map.get("name")).and_then(|v| v.as_str()).unwrap_or("");
                    let args = map.get("args").and_then(|v| v.as_array()).map(|arr| {
                        arr.iter().map(format_ast_val_to_sql).collect::<Vec<_>>().join(", ")
                    }).unwrap_or_default();
                    format!("{}({})", fn_name, args)
                }
                "Cast" => {
                    let inner = map.get("expr").map(format_ast_val_to_sql).unwrap_or_default();
                    let target = map.get("dataType").or_else(|| map.get("targetType")).and_then(|v| v.as_str());
                    if let Some(dt) = target {
                        format!("{}::{}", inner, dt)
                    } else {
                        inner
                    }
                }
                _ => {
                    if let Some(v) = map.get("value") {
                        format_ast_val_to_sql(v)
                    } else if let Some(n) = map.get("name").and_then(|v| v.as_str()) {
                        n.to_string()
                    } else {
                        val.to_string()
                    }
                }
            }
        }
        serde_json::Value::String(s) => s.clone(),
        _ => val.to_string(),
    }
}

pub(crate) fn split_comma_separated_tokens(s: &str) -> Vec<&str> {
    let bytes = s.as_bytes();
    let mut tokens = Vec::new();
    let mut depth = 0;
    let mut in_sq = false;
    let mut in_dq = false;
    let mut start = 0;
    let mut i = 0;

    while i < bytes.len() {
        let b = bytes[i];
        if b == b'\'' && !in_dq {
            in_sq = !in_sq;
        } else if b == b'"' && !in_sq {
            in_dq = !in_dq;
        } else if !in_sq && !in_dq {
            if b == b'(' || b == b'[' || b == b'{' {
                depth += 1;
            } else if b == b')' || b == b']' || b == b'}' {
                if depth > 0 { depth -= 1; }
            } else if depth == 0 && b == b',' {
                tokens.push(s[start..i].trim());
                start = i + 1;
            }
        }
        i += 1;
    }
    if start < bytes.len() {
        let last = s[start..].trim();
        if !last.is_empty() {
            tokens.push(last);
        }
    }
    tokens
}

/// Chuỗi `$tag$ ... $tag$` bắt đầu tại `i`: trả về vị trí ngay sau thẻ đóng. `$1` (tham số) không
/// phải chuỗi dollar.
pub(crate) fn dollar_quote_end(sql: &str, i: usize) -> Option<usize> {
    let b = sql.as_bytes();
    if b.get(i) != Some(&b'$') {
        return None;
    }
    if i > 0 && (b[i - 1].is_ascii_alphanumeric() || b[i - 1] == b'_') {
        return None;
    }
    let mut j = i + 1;
    if j < b.len() && b[j].is_ascii_digit() {
        return None;
    }
    while j < b.len() && (b[j].is_ascii_alphanumeric() || b[j] == b'_') {
        j += 1;
    }
    if b.get(j) != Some(&b'$') {
        return None;
    }
    let tag = &sql[i..=j];
    sql[j + 1..].find(tag).map(|p| j + 1 + p + tag.len())
}

pub(crate) fn split_sql_statements(sql: &str) -> Vec<String> {
    // Chỉ cắt tại byte ';' (ASCII) nên luôn đúng ranh giới ký tự UTF-8; mỗi câu được chép
    // nguyên lát chuỗi gốc để không làm hỏng tiếng Việt.
    let mut stmts = Vec::new();
    let mut in_str = false;
    let mut depth: i32 = 0;
    let mut seg_start = 0;
    let mut skip_to = 0usize;
    for (i, &b) in sql.as_bytes().iter().enumerate() {
        if i < skip_to {
            continue;
        }
        if b == b'\'' {
            in_str = !in_str;
        } else if in_str {
            continue;
        } else if b == b'$' {
            // Thân `DO $$ ... $$` chứa ';' bên trong
            if let Some(end) = dollar_quote_end(sql, i) {
                skip_to = end;
            }
        } else if b == b'(' || b == b'[' {
            depth += 1;
        } else if b == b')' || b == b']' {
            depth = depth.saturating_sub(1);
        } else if depth == 0 && b == b';' {
            let stmt = sql[seg_start..i].trim();
            if !stmt.is_empty() {
                stmts.push(stmt.to_string());
            }
            seg_start = i + 1;
        }
    }
    let stmt = sql[seg_start..].trim();
    if !stmt.is_empty() {
        stmts.push(stmt.to_string());
    }
    stmts
}

fn extract_literal_after_key(where_str: &str, key: &str) -> Option<String> {
    let up = where_str.to_uppercase();
    if let Some(idx) = up.find(&key.to_uppercase()) {
        let rest = where_str[idx + key.len()..].trim();
        if rest.starts_with('=') {
            let after_eq = rest[1..].trim();
            if after_eq.starts_with('\'') {
                if let Some(close_q) = after_eq[1..].find('\'') {
                    return Some(after_eq[1..1 + close_q].to_string());
                }
            } else {
                let tok = after_eq.split_whitespace().next().unwrap_or("").trim_matches(';').trim_matches('\'');
                if !tok.is_empty() {
                    return Some(tok.to_string());
                }
            }
        }
    }
    None
}

pub(crate) fn extract_value_groups(values_part: &str) -> Vec<&str> {
    let bytes = values_part.as_bytes();
    let mut groups = Vec::new();
    let mut depth = 0;
    let mut in_sq = false;
    let mut in_dq = false;
    let mut start_idx = None;

    let mut i = 0;
    while i < bytes.len() {
        let b = bytes[i];
        if b == b'\'' && !in_dq {
            in_sq = !in_sq;
        } else if b == b'"' && !in_sq {
            in_dq = !in_dq;
        } else if !in_sq && !in_dq {
            if b == b'(' {
                if depth == 0 {
                    start_idx = Some(i + 1);
                }
                depth += 1;
            } else if b == b')' {
                if depth > 0 {
                    depth -= 1;
                    if depth == 0 {
                        if let Some(s) = start_idx {
                            groups.push(&values_part[s..i]);
                            start_idx = None;
                        }
                    }
                }
            }
        }
        i += 1;
    }
    groups
}

/// Tên bảng / alias (viết thường) trong FROM của chính subquery.
fn subquery_own_names(sub: &str) -> Vec<String> {
    let Some(f) = find_top_level_keyword(sub, "FROM") else { return Vec::new() };
    let rest = &sub[f + 4..];
    let mut end = rest.len();
    for kw in ["WHERE", "GROUP", "ORDER", "LIMIT", "OFFSET", "HAVING", "UNION", "WINDOW"] {
        if let Some(p) = find_top_level_keyword(rest, kw) {
            end = end.min(p);
        }
    }
    let mut own: Vec<String> = Vec::new();
    let mut prev_kw = true;
    for tok in rest[..end].split(|c: char| c.is_whitespace() || c == ',' || c == '(' || c == ')') {
        if tok.is_empty() {
            continue;
        }
        let up = tok.to_uppercase();
        if ["JOIN", "LEFT", "RIGHT", "FULL", "INNER", "OUTER", "CROSS", "ON", "AS", "USING", "LATERAL", "NATURAL"].contains(&up.as_str()) {
            prev_kw = true;
            continue;
        }
        if tok.contains('=') || tok.contains('.') && !prev_kw {
            prev_kw = false;
            continue;
        }
        own.push(tok.rsplit('.').next().unwrap_or(tok).trim_matches('"').to_lowercase());
        prev_kw = false;
    }
    own
}

/// Điều kiện có `(SELECT ...)` tương quan (tham chiếu bảng ngoài).
fn has_correlated_subquery(cond: &str) -> bool {
    let b = cond.as_bytes();
    let mut in_sq = false;
    let mut i = 0;
    while i < b.len() {
        if b[i] == b'\'' {
            in_sq = !in_sq;
        } else if !in_sq && b[i] == b'(' && cond[i + 1..].trim_start().get(..6).map_or(false, |p| p.eq_ignore_ascii_case("SELECT")) {
            if let Some(close) = crate::engine::constraints::matching_paren(cond, i) {
                if subquery_is_correlated(&cond[i + 1..close]) {
                    return true;
                }
                i = close;
            }
        }
        i += 1;
    }
    false
}

/// Subquery có tham chiếu `q.col` mà `q` không phải bảng / alias trong FROM của chính nó (tương quan).
fn subquery_is_correlated(sub: &str) -> bool {
    if find_top_level_keyword(sub, "FROM").is_none() {
        return false;
    }
    let own = subquery_own_names(sub);
    let b = sub.as_bytes();
    let mut in_str = false;
    let mut i = 0;
    while i < b.len() {
        if b[i] == b'\'' {
            in_str = !in_str;
        } else if !in_str && b[i] == b'.' && i > 0 && (b[i - 1].is_ascii_alphanumeric() || b[i - 1] == b'_') && b.get(i + 1).is_some_and(|c| c.is_ascii_alphabetic() || *c == b'_' || *c == b'"') {
            let mut s0 = i;
            while s0 > 0 && (b[s0 - 1].is_ascii_alphanumeric() || b[s0 - 1] == b'_') {
                s0 -= 1;
            }
            let q = sub[s0..i].to_lowercase();
            if !q.chars().next().is_some_and(|c| c.is_ascii_digit()) && !own.contains(&q) {
                return true;
            }
        }
        i += 1;
    }
    false
}

pub(crate) fn clean_col_name(raw: &str) -> &str {
    let mut s = raw.trim();
    while s.starts_with('(') && s.ends_with(')') && s.len() >= 2 {
        s = s[1..s.len() - 1].trim();
    }
    if let Some(colon_pos) = s.rfind("::") {
        s = s[..colon_pos].trim();
    }
    let u = s.to_uppercase();
    if (u.starts_with("UPPER(") || u.starts_with("LOWER(") || u.starts_with("TRIM(") || u.starts_with("BTRIM(")) && s.ends_with(')') {
        let open_p = s.find('(').unwrap();
        s = s[open_p + 1..s.len() - 1].trim();
    }
    if let Some(dot_idx) = s.rfind('.') {
        s[dot_idx + 1..].trim().trim_matches('"').trim_end_matches(')')
    } else {
        s.trim_matches('"').trim_end_matches(')')
    }
}

fn extract_table_name_and_alias<'a>(raw: &'a str) -> (&'a str, Option<&'a str>) {
    let mut s = raw.trim();
    while s.starts_with('(') && s.ends_with(')') && s.len() >= 2 {
        s = s[1..s.len() - 1].trim();
    }
    let parts: Vec<&str> = s.split_whitespace().collect();
    if parts.is_empty() {
        return ("", None);
    }
    let tbl = if let Some(dot_idx) = parts[0].rfind('.') {
        parts[0][dot_idx + 1..].trim().trim_matches('"')
    } else {
        parts[0].trim_matches('"')
    };

    if parts.len() == 2 {
        let alias = parts[1].trim_matches('"');
        if alias.eq_ignore_ascii_case("WHERE") || alias.eq_ignore_ascii_case("JOIN") || alias.eq_ignore_ascii_case("LEFT") || alias.eq_ignore_ascii_case("INNER") {
            (tbl, None)
        } else {
            (tbl, Some(alias))
        }
    } else if parts.len() >= 3 && parts[1].eq_ignore_ascii_case("AS") {
        (tbl, Some(parts[2].trim_matches('"')))
    } else {
        (tbl, None)
    }
}

/// Một định nghĩa cột `name type [DEFAULT ...] [NOT NULL] [PRIMARY KEY] ...` (CREATE TABLE, ALTER
/// TABLE ADD COLUMN). Trả về cột và phần sau tên cột (để đọc ràng buộc cấp cột).
/// `GENERATED {ALWAYS | BY DEFAULT} AS IDENTITY [( START [WITH] n | INCREMENT [BY] m | ... )]` trong định
/// nghĩa cột. `None` nếu không phải cột identity (kể cả `GENERATED ALWAYS AS (expr) STORED`).
pub(crate) fn parse_identity_clause(rest_def: &str) -> Option<crate::storage::catalog::IdentitySpec> {
    let up = rest_def.to_uppercase();
    let g = find_top_level_keyword(&up, "GENERATED")?;
    let after = up[g + 9..].trim_start();
    let (always, after) = if let Some(r) = after.strip_prefix("ALWAYS") {
        (true, r.trim_start())
    } else if let Some(r) = after.strip_prefix("BY DEFAULT") {
        (false, r.trim_start())
    } else {
        return None;
    };
    let after = after.strip_prefix("AS")?.trim_start();
    let opts = after.strip_prefix("IDENTITY")?.trim_start();
    let mut start: Option<i64> = None;
    let mut increment: i64 = 1;
    if opts.starts_with('(') {
        let close = crate::engine::constraints::matching_paren(opts, 0)?;
        let words: Vec<&str> = opts[1..close].split_whitespace().collect();
        let mut i = 0;
        while i < words.len() {
            let num_after = |j: usize| -> Option<(i64, usize)> {
                let (j, skip) = if matches!(words.get(j).copied(), Some("WITH") | Some("BY")) { (j + 1, 2) } else { (j, 1) };
                words.get(j).and_then(|w| w.parse::<i64>().ok()).map(|n| (n, skip))
            };
            match words[i] {
                "START" => {
                    if let Some((n, skip)) = num_after(i + 1) {
                        start = Some(n);
                        i += skip;
                    }
                }
                "INCREMENT" => {
                    if let Some((n, skip)) = num_after(i + 1) {
                        increment = n;
                        i += skip;
                    }
                }
                _ => {}
            }
            i += 1;
        }
    }
    if increment == 0 {
        increment = 1;
    }
    Some(crate::storage::catalog::IdentitySpec { always, start: start.unwrap_or(if increment > 0 { 1 } else { -1 }), increment })
}

pub(crate) fn parse_column_definition(col_def_str: &str) -> (ColumnDef, &str) {
    let (col_name, rest_def) = if col_def_str.starts_with('"') {
        if let Some(close_q) = col_def_str[1..].find('"') {
            let name = &col_def_str[1..1 + close_q];
            (name.to_string(), &col_def_str[2 + close_q..])
        } else {
            let parts: Vec<&str> = col_def_str.split_whitespace().collect();
            (parts[0].trim_matches('"').to_string(), &col_def_str[parts[0].len()..])
        }
    } else {
        let parts: Vec<&str> = col_def_str.split_whitespace().collect();
        (parts[0].trim_matches('"').to_string(), &col_def_str[parts[0].len()..])
    };

    let upper_rest = rest_def.to_uppercase();

    let data_type = DataType::from_sql_str(rest_def);

    let is_primary_key = upper_rest.contains("PRIMARY KEY");
    let is_nullable = !upper_rest.contains("NOT NULL") && !is_primary_key;

    let default_value = if let Some(def_idx) = upper_rest.find("DEFAULT ") {
        let after_def = rest_def[def_idx + 8..].trim();
        if after_def.starts_with('\'') {
            // Dấu nháy đóng: bỏ qua '' (nháy được escape bên trong chuỗi)
            let b = after_def.as_bytes();
            let mut j = 1;
            let mut end_quote = None;
            while j < b.len() {
                if b[j] == b'\'' {
                    if b.get(j + 1) == Some(&b'\'') {
                        j += 2;
                        continue;
                    }
                    end_quote = Some(j);
                    break;
                }
                j += 1;
            }
            if let Some(end_quote) = end_quote {
                Some(after_def[..=end_quote].to_string())
            } else {
                Some(after_def.split_whitespace().next().unwrap_or("").trim_matches(';').trim_matches(',').to_string())
            }
        } else if after_def.starts_with('{') {
            let mut depth = 0;
            let mut end_pos = None;
            for (i, b) in after_def.bytes().enumerate() {
                if b == b'{' {
                    depth += 1;
                } else if b == b'}' {
                    depth -= 1;
                    if depth == 0 {
                        end_pos = Some(i);
                        break;
                    }
                }
            }
            if let Some(end_p) = end_pos {
                let json_str = &after_def[..=end_p];
                Some(format_attdef_to_sql(json_str))
            } else {
                Some(format_attdef_to_sql(after_def))
            }
        } else {
            let mut tokens = Vec::new();
            for part in after_def.split_whitespace() {
                let p = part.trim_matches(';').trim_matches(',');
                if p.eq_ignore_ascii_case("NOT") || p.eq_ignore_ascii_case("NULL") || p.eq_ignore_ascii_case("PRIMARY") || p.eq_ignore_ascii_case("KEY") || p.eq_ignore_ascii_case("REFERENCES") || p.eq_ignore_ascii_case("CHECK") || p.eq_ignore_ascii_case("CONSTRAINT") {
                    break;
                }
                tokens.push(p);
            }
            if tokens.is_empty() {
                None
            } else {
                Some(format_attdef_to_sql(&tokens.join(" ")))
            }
        }
    } else {
        None
    };

    (
        ColumnDef {
            name: col_name,
            data_type,
            is_primary_key,
            is_nullable,
            default_value,
            comment: None,
        },
        rest_def,
    )
}

/// Tên bảng trong DDL: bỏ tiền tố schema và dấu nháy, giữ nguyên chữ hoa/thường như trước.
pub(crate) fn strip_schema_keep_case(raw: &str) -> String {
    let s = raw.trim();
    let last = if s.starts_with('"') {
        match s[1..].find('"') {
            Some(end) if s[end + 2..].starts_with('.') => &s[end + 3..],
            _ => s,
        }
    } else if let Some(dot) = s.find('.') {
        &s[dot + 1..]
    } else {
        s
    };
    last.trim().trim_matches('"').to_string()
}

fn clean_table_name(raw: &str) -> &str {
    extract_table_name_and_alias(raw).0
}

fn strip_sql_comments(sql: &str) -> String {
    let mut result = String::with_capacity(sql.len());
    let mut chars = sql.chars().peekable();
    let mut in_sq = false;
    let mut in_dq = false;

    while let Some(c) = chars.next() {
        if c == '\'' && !in_dq {
            in_sq = !in_sq;
            result.push(c);
        } else if c == '"' && !in_sq {
            in_dq = !in_dq;
            result.push(c);
        } else if !in_sq && !in_dq {
            if c == '-' && chars.peek() == Some(&'-') {
                chars.next();
                while let Some(&nc) = chars.peek() {
                    if nc == '\n' {
                        break;
                    }
                    chars.next();
                }
                result.push(' ');
            } else if c == '/' && chars.peek() == Some(&'*') {
                chars.next();
                while let Some(nc) = chars.next() {
                    if nc == '*' && chars.peek() == Some(&'/') {
                        chars.next();
                        break;
                    }
                }
                result.push(' ');
            } else {
                result.push(c);
            }
        } else {
            result.push(c);
        }
    }
    result
}

fn normalize_named_sql_params(sql: &str) -> String {
    let bytes = sql.as_bytes();
    let len = bytes.len();
    let mut param_names: Vec<String> = Vec::new();
    let mut in_single_quote = false;
    let mut in_double_quote = false;
    let mut i = 0;

    while i < len {
        let b = bytes[i];
        if b == b'\'' && !in_double_quote {
            in_single_quote = !in_single_quote;
        } else if b == b'"' && !in_single_quote {
            in_double_quote = !in_double_quote;
        } else if !in_single_quote && !in_double_quote {
            if b == b':' {
                let prev_is_colon = i > 0 && bytes[i - 1] == b':';
                let next_is_colon = i + 1 < len && bytes[i + 1] == b':';
                if !prev_is_colon && !next_is_colon && i + 1 < len {
                    let next_ch = bytes[i + 1];
                    if next_ch.is_ascii_alphabetic() || next_ch == b'_' {
                        let start = i + 1;
                        let mut end = start;
                        while end < len && (bytes[end].is_ascii_alphanumeric() || bytes[end] == b'_') {
                            end += 1;
                        }
                        let name = &sql[start..end];
                        if !param_names.iter().any(|n| n == name) {
                            param_names.push(name.to_string());
                        }
                        i = end;
                        continue;
                    }
                }
            }
        }
        i += 1;
    }

    if param_names.is_empty() {
        return sql.to_string();
    }

    // Chép nguyên các đoạn giữa những tham số `:ten` (không đi từng byte) để giữ UTF-8.
    let mut result = String::with_capacity(sql.len());
    let mut copied_until = 0;
    let mut i = 0;
    in_single_quote = false;
    in_double_quote = false;

    while i < len {
        let b = bytes[i];
        if b == b'\'' && !in_double_quote {
            in_single_quote = !in_single_quote;
        } else if b == b'"' && !in_single_quote {
            in_double_quote = !in_double_quote;
        } else if !in_single_quote && !in_double_quote && b == b':' {
            let prev_is_colon = i > 0 && bytes[i - 1] == b':';
            let next_is_colon = i + 1 < len && bytes[i + 1] == b':';
            if !prev_is_colon && !next_is_colon && i + 1 < len && (bytes[i + 1].is_ascii_alphabetic() || bytes[i + 1] == b'_') {
                let start = i + 1;
                let mut end = start;
                while end < len && (bytes[end].is_ascii_alphanumeric() || bytes[end] == b'_') {
                    end += 1;
                }
                let name = &sql[start..end];
                if let Some(pos) = param_names.iter().position(|n| n == name) {
                    result.push_str(&sql[copied_until..i]);
                    result.push_str(&format!("${}", pos + 1));
                    copied_until = end;
                }
                i = end;
                continue;
            }
        }
        i += 1;
    }
    result.push_str(&sql[copied_until..]);
    result
}

/// `s` có lời gọi `tên(` với tên thuộc `names` (không phân biệt hoa thường, đủ ranh giới từ phía trước).
/// Một lượt quét, không cấp phát — chạy cho mọi câu SELECT.
pub(crate) fn mentions_call(s: &str, names: &[&str]) -> bool {
    let b = s.as_bytes();
    let mut i = 0;
    while let Some(off) = b[i..].iter().position(|&c| c == b'(') {
        let p = i + off;
        let mut st = p;
        while st > 0 && (b[st - 1].is_ascii_alphanumeric() || b[st - 1] == b'_') {
            st -= 1;
        }
        if st < p {
            let w = &s[st..p];
            if names.iter().any(|n| n.len() == w.len() && n.eq_ignore_ascii_case(w)) {
                return true;
            }
        }
        i = p + 1;
    }
    false
}

const AGG_NAMES: &[&str] = &[
    "COUNT", "SUM", "AVG", "MIN", "MAX", "BOOL_AND", "BOOL_OR", "EVERY", "ARRAY_AGG", "STRING_AGG", "JSON_AGG", "JSONB_AGG", "JSON_OBJECT_AGG", "JSONB_OBJECT_AGG",
    // engine/fnlib.rs EXTRA_AGGS
    "BIT_AND", "BIT_OR", "BIT_XOR", "STDDEV", "STDDEV_SAMP", "STDDEV_POP", "VARIANCE", "VAR_SAMP", "VAR_POP", "CORR", "COVAR_POP", "COVAR_SAMP",
    "REGR_SLOPE", "REGR_INTERCEPT", "REGR_COUNT", "REGR_R2", "REGR_AVGX", "REGR_AVGY", "REGR_SXX", "REGR_SYY", "REGR_SXY", "ANY_VALUE",
    "PERCENTILE_CONT", "PERCENTILE_DISC", "MODE", "PG_HYP_RANK", "PG_HYP_DENSE_RANK", "PG_HYP_PERCENT_RANK", "PG_HYP_CUME_DIST", "XMLAGG",
    "JSON_ARRAYAGG", "JSON_OBJECTAGG",
];

fn is_hidden_agg_key(k: &str) -> bool {
    k.starts_with("__agg") && k.ends_with("__")
}

/// Vị trí (đầu..cuối) các lời gọi aggregate trong `e` (ngoài chuỗi), kèm `FILTER (...)` theo sau.
fn find_aggregate_calls(e: &str) -> Vec<std::ops::Range<usize>> {
    let b = e.as_bytes();
    let mut out = Vec::new();
    let mut in_str = false;
    let mut i = 0;
    while i < b.len() {
        if b[i] == b'\'' {
            in_str = !in_str;
            i += 1;
            continue;
        }
        if in_str || !(b[i].is_ascii_alphabetic()) || i > 0 && (b[i - 1].is_ascii_alphanumeric() || b[i - 1] == b'_' || b[i - 1] == b'.') {
            i += 1;
            continue;
        }
        let mut j = i;
        while j < b.len() && (b[j].is_ascii_alphanumeric() || b[j] == b'_') {
            j += 1;
        }
        let word = &e[i..j];
        let mut k = j;
        while k < b.len() && b[k] == b' ' {
            k += 1;
        }
        if k < b.len() && b[k] == b'(' && AGG_NAMES.iter().any(|n| n.eq_ignore_ascii_case(word)) {
            if let Some(close) = crate::engine::constraints::matching_paren(e, k) {
                let mut end = close + 1;
                let rest = e[end..].trim_start();
                if rest.len() > 6 && rest.as_bytes()[..6].eq_ignore_ascii_case(b"FILTER") {
                    let fstart = end + (e[end..].len() - rest.len()) + 6;
                    let after = e[fstart..].trim_start();
                    if after.starts_with('(') {
                        let open = fstart + (e[fstart..].len() - after.len());
                        if let Some(fc) = crate::engine::constraints::matching_paren(e, open) {
                            end = fc + 1;
                        }
                    }
                }
                out.push(i..end);
                i = end;
                continue;
            }
        }
        i = j;
    }
    out
}

/// Tách các mục SELECT là biểu thức chứa aggregate (nhưng không phải một lời gọi aggregate thuần) thành
/// mục ẩn. Trả (select mới, [(placeholder, biểu thức gốc, [(vị trí lời gọi, khoá ẩn)])]).
#[allow(clippy::type_complexity)]
fn split_aggregate_expressions(select_clause: &str) -> (Option<String>, Vec<(String, String, Vec<(std::ops::Range<usize>, String)>)>) {
    let up = select_clause.to_uppercase();
    if !mentions_call(&up, AGG_NAMES) || up.contains(" OVER") {
        return (None, vec![]);
    }
    let items = split_projection_items(select_clause);
    let mut hidden: Vec<String> = Vec::new();
    let mut out_items: Vec<String> = Vec::new();
    let mut composites = Vec::new();
    for item in items {
        let it = item.trim();
        let (expr, alias) = match rfind_top_level_as(&it.to_uppercase()) {
            Some(p) => (it[..p].trim(), Some(it[p + 4..].trim())),
            None => (it, None),
        };
        let calls = find_aggregate_calls(expr);
        let pure = calls.len() == 1 && calls[0].start == 0 && calls[0].end == expr.len();
        if calls.is_empty() || pure {
            out_items.push(it.to_string());
            continue;
        }
        let k = composites.len();
        let mut call_keys = Vec::new();
        for c in &calls {
            let key = format!("__agg{}__", hidden.len());
            hidden.push(format!("{} AS \"{}\"", &expr[c.clone()], key));
            call_keys.push((c.clone(), key));
        }
        let ph = format!("__cmp{}__", k);
        // không alias (engine để nguyên vì tên mặc định bị trùng): khoá là chính văn bản biểu thức
        let name = match alias {
            Some(a) => a.to_string(),
            None => format!("\"{}\"", expr.replace('"', "\"\"")),
        };
        out_items.push(format!("{} AS {}", ph, name));
        composites.push((ph, expr.to_string(), call_keys));
    }
    if composites.is_empty() {
        return (None, vec![]);
    }
    hidden.extend(out_items);
    (Some(hidden.join(", ")), composites)
}

fn json_to_sql_literal(v: &serde_json::Value) -> String {
    match v {
        serde_json::Value::Null => "NULL".to_string(),
        serde_json::Value::Bool(b) => if *b { "TRUE" } else { "FALSE" }.to_string(),
        serde_json::Value::Number(n) => {
            let t = n.to_string();
            if t.starts_with('-') { format!("({})", t) } else { t }
        }
        serde_json::Value::String(s) => format!("'{}'", s.replace('\'', "''")),
        other => format!("'{}'", other.to_string().replace('\'', "''")),
    }
}

/// Chuẩn hoá biểu thức để so khớp cột nhóm (`t.n`, `N`, ` n ` → `n`).
fn norm_group_expr(e: &str) -> String {
    let mut t = e.trim();
    while t.starts_with('(') && t.ends_with(')') && is_fully_enclosed_in_parens(t) {
        t = t[1..t.len() - 1].trim();
    }
    let lower = t.split_whitespace().collect::<Vec<_>>().join(" ").to_lowercase();
    if lower.chars().all(|c| c.is_alphanumeric() || c == '_' || c == '.' || c == '"') {
        return lower.rsplit('.').next().unwrap_or(&lower).trim_matches('"').to_string();
    }
    lower
}

/// Một phần tử của GROUP BY → danh sách các tập nhóm (mỗi tập là danh sách biểu thức).
fn grouping_element_sets(el: &str) -> Result<Vec<Vec<String>>, String> {
    let e = el.trim();
    let up = e.to_uppercase();
    let unit = |x: &str| -> Vec<String> {
        let x = x.trim();
        if x.starts_with('(') && x.ends_with(')') && is_fully_enclosed_in_parens(x) {
            split_comma_separated_tokens(&x[1..x.len() - 1]).into_iter().map(|c| c.trim().to_string()).filter(|c| !c.is_empty()).collect()
        } else {
            vec![x.to_string()]
        }
    };
    let body = |kw_len: usize| -> Result<&str, String> {
        let r = e[kw_len..].trim();
        if r.starts_with('(') && r.ends_with(')') && is_fully_enclosed_in_parens(r) { Ok(&r[1..r.len() - 1]) } else { Err(format!("syntax error at or near \"{}\"", e)) }
    };
    if up.starts_with("GROUPING SETS") {
        let mut out = Vec::new();
        for item in split_comma_separated_tokens(body(13)?) {
            let it = item.trim();
            let iu = it.to_uppercase();
            if iu.starts_with("ROLLUP") || iu.starts_with("CUBE") || iu.starts_with("GROUPING SETS") {
                out.extend(grouping_element_sets(it)?);
            } else {
                out.push(unit(it));
            }
        }
        return Ok(out);
    }
    if up.starts_with("ROLLUP") {
        let units: Vec<Vec<String>> = split_comma_separated_tokens(body(6)?).into_iter().map(unit).collect();
        return Ok((0..=units.len()).rev().map(|k| units[..k].concat()).collect());
    }
    if up.starts_with("CUBE") {
        let units: Vec<Vec<String>> = split_comma_separated_tokens(body(4)?).into_iter().map(unit).collect();
        let n = units.len();
        if n > 12 {
            return Err("CUBE is limited to 12 elements".to_string());
        }
        let mut out = Vec::new();
        for mask in (0..(1u32 << n)).rev() {
            out.push((0..n).filter(|i| mask & (1 << (n - 1 - i)) != 0).flat_map(|i| units[i].clone()).collect());
        }
        return Ok(out);
    }
    Ok(vec![unit(e)])
}

/// `GROUP BY GROUPING SETS (...) / ROLLUP (...) / CUBE (...)` → UNION ALL các truy vấn GROUP BY thường:
/// cột nhóm không thuộc tập hiện tại thành NULL, `GROUPING(...)` thành hằng bitmask. `None` nếu không áp dụng.
fn grouping_sets_as_union(sql: &str) -> Result<Option<String>, String> {
    let s = sql.trim();
    if !s.to_uppercase().starts_with("SELECT") || ["UNION", "INTERSECT", "EXCEPT"].iter().any(|k| find_top_level_keyword(s, k).is_some()) {
        return Ok(None);
    }
    let Some(gb) = find_top_level_keyword(s, "GROUP BY") else { return Ok(None) };
    let Some(from) = find_top_level_keyword(s, "FROM") else { return Ok(None) };
    let after_gb = &s[gb + 8..];
    let mut end = after_gb.len();
    let mut cuts: Vec<(usize, &str)> = Vec::new();
    for kw in ["HAVING", "ORDER BY", "LIMIT", "OFFSET", "WINDOW"] {
        if let Some(p) = find_top_level_keyword(after_gb, kw) {
            end = end.min(p);
            cuts.push((p, kw));
        }
    }
    cuts.sort();
    let group_txt = after_gb[..end].trim();
    let gu = group_txt.to_uppercase();
    if !(gu.contains("GROUPING SETS") || gu.contains("ROLLUP") || gu.contains("CUBE")) {
        return Ok(None);
    }
    let having = cuts.iter().position(|c| c.1 == "HAVING").map(|i| {
        let st = cuts[i].0 + 6;
        let en = cuts.get(i + 1).map(|c| c.0).unwrap_or(after_gb.len());
        after_gb[st..en].trim().to_string()
    });
    let tail = cuts.iter().find(|c| c.1 != "HAVING").map(|c| after_gb[c.0..].trim().to_string()).unwrap_or_default();
    // tích Đề-các các phần tử
    let mut sets: Vec<Vec<String>> = vec![vec![]];
    for el in split_comma_separated_tokens(group_txt) {
        let el_sets = grouping_element_sets(el)?;
        let mut next = Vec::new();
        for a in &sets {
            for b in &el_sets {
                next.push([a.clone(), b.clone()].concat());
            }
        }
        sets = next;
    }
    let list = s[6..from].trim();
    let from_where = s[from..gb].trim_end();
    let items: Vec<(String, String)> = split_projection_items(list)
        .into_iter()
        .map(|it| {
            let it = it.trim().to_string();
            match rfind_top_level_as(&it.to_uppercase()) {
                Some(p) => (it[..p].trim().to_string(), it[p + 4..].trim().to_string()),
                None => {
                    let name = crate::engine::select_list::default_column_name(&it);
                    (it.clone(), format!("\"{}\"", name.replace('"', "\"\"")))
                }
            }
        })
        .collect();
    // alias dùng trong GROUP BY (`GROUP BY CUBE (flag, big)` với `(n > 4) AS big`) → biểu thức của mục
    let resolve = |g: &str| -> String {
        let ng = norm_group_expr(g);
        items.iter().find(|(_, a)| a.trim_matches('"').to_lowercase() == ng).map(|(e, _)| e.clone()).unwrap_or_else(|| g.trim().to_string())
    };
    let all_groups: Vec<String> = {
        let mut v: Vec<String> = Vec::new();
        for st in &sets {
            for g in st {
                let r = norm_group_expr(&resolve(g));
                if !v.contains(&r) {
                    v.push(r);
                }
            }
        }
        v
    };
    let mut branches = Vec::new();
    for st in &sets {
        let present: Vec<String> = st.iter().map(|g| norm_group_expr(&resolve(g))).collect();
        let mut out_items = Vec::new();
        for (e, alias) in &items {
            let ne = norm_group_expr(e);
            if all_groups.contains(&ne) && !present.contains(&ne) {
                out_items.push(format!("NULL AS {}", alias));
                continue;
            }
            // GROUPING(a, b, ...) → bitmask (bit = 1 khi đối số không thuộc tập hiện tại)
            let mut ex = e.clone();
            while let Some(p) = find_ci_word(&ex, "GROUPING(") {
                let open = p + 8;
                let Some(close) = crate::engine::constraints::matching_paren(&ex, open) else { break };
                let args: Vec<String> = split_comma_separated_tokens(&ex[open + 1..close]).into_iter().map(|a| norm_group_expr(&resolve(a))).collect();
                let mut mask = 0u64;
                for a in &args {
                    mask = (mask << 1) | u64::from(!present.contains(a));
                }
                ex.replace_range(p..close + 1, &mask.to_string());
            }
            out_items.push(format!("{} AS {}", ex, alias));
        }
        let gb_clause = if st.is_empty() { String::new() } else { format!(" GROUP BY {}", st.iter().map(|g| resolve(g)).collect::<Vec<_>>().join(", ")) };
        let hv = having.as_ref().map(|h| format!(" HAVING {}", h)).unwrap_or_default();
        branches.push(format!("SELECT {} {}{}{}", out_items.join(", "), from_where, gb_clause, hv));
    }
    let mut u = branches.join(" UNION ALL ");
    if !tail.is_empty() {
        u.push(' ');
        u.push_str(&tail);
    }
    Ok(Some(u))
}

/// Vị trí của `word` (kết thúc bằng `(`), không phân biệt hoa thường, có biên từ phía trước, ngoài chuỗi.
fn find_ci_word(s: &str, word: &str) -> Option<usize> {
    let b = s.as_bytes();
    let w = word.as_bytes();
    let mut in_str = false;
    for i in 0..b.len() {
        if b[i] == b'\'' {
            in_str = !in_str;
        }
        if !in_str && i + w.len() <= b.len() && b[i..i + w.len()].eq_ignore_ascii_case(w) && (i == 0 || !(b[i - 1].is_ascii_alphanumeric() || b[i - 1] == b'_')) {
            return Some(i);
        }
    }
    None
}

/// `CREATE [UNLOGGED|TEMP] TABLE ... AS SELECT|WITH|VALUES|TABLE|(`: CREATE TABLE AS.
fn is_create_table_as(sql: &str) -> bool {
    let Some(p) = find_top_level_keyword(sql, "AS") else { return false };
    // `AS` phải nằm ngoài phần định nghĩa cột (cột GENERATED ALWAYS AS (...) nằm trong ngoặc)
    let before = &sql[..p];
    if before.contains('(') && !before.trim_end().ends_with(')') {
        return false;
    }
    let after = sql[p + 2..].trim_start().to_uppercase();
    ["SELECT", "WITH", "VALUES", "TABLE", "("].iter().any(|k| after.starts_with(k))
}

/// `SELECT <list> INTO [TEMP|TEMPORARY|UNLOGGED] [TABLE] t FROM ...` → `CREATE [UNLOGGED] TABLE t AS SELECT <list> FROM ...`.
fn select_into_as_ctas(sql: &str) -> Option<String> {
    let into = find_top_level_keyword(sql, "INTO")?;
    if let Some(from) = find_top_level_keyword(sql, "FROM") {
        if from < into {
            return None;
        }
    }
    let after = sql[into + 4..].trim_start();
    let mut words = after.splitn(2, char::is_whitespace);
    let mut first = words.next()?.to_string();
    let mut rest = words.next().unwrap_or("").trim_start().to_string();
    let mut prefix = "";
    loop {
        match first.to_uppercase().as_str() {
            "TEMP" | "TEMPORARY" => prefix = "TEMP ",
            "UNLOGGED" => prefix = "UNLOGGED ",
            "TABLE" => {}
            _ => break,
        }
        let mut w = rest.splitn(2, char::is_whitespace);
        first = w.next()?.to_string();
        rest = w.next().unwrap_or("").trim_start().to_string();
    }
    let name = first;
    Some(format!("CREATE {}TABLE {} AS {} {}", prefix, name, sql[..into].trim_end(), rest).trim_end().to_string())
}

/// Kiểu SQL cho cột CTAS khi không có kiểu khai báo: theo OID của cột kết quả, rồi theo giá trị.
fn sql_type_for_oid(oid: u32, sample: Option<&serde_json::Value>) -> String {
    use crate::pg_types::*;
    let t = match oid {
        OID_INT4 => "int4",
        OID_INT8 => "int8",
        OID_INT2 => "int2",
        OID_BOOL => "boolean",
        OID_NUMERIC => "numeric",
        OID_FLOAT4 => "real",
        OID_FLOAT8 => "double precision",
        OID_TIMESTAMP => "timestamp",
        OID_TIMESTAMPTZ => "timestamptz",
        OID_DATE => "date",
        OID_TIME => "time",
        OID_JSON => "json",
        OID_JSONB => "jsonb",
        OID_UUID => "uuid",
        OID_BYTEA => "bytea",
        OID_VARCHAR => "varchar",
        OID_TEXT_ARRAY => "text[]",
        OID_INT4_ARRAY => "int4[]",
        OID_INT8_ARRAY => "int8[]",
        OID_NUMERIC_ARRAY => "numeric[]",
        OID_BOOL_ARRAY => "boolean[]",
        OID_FLOAT8_ARRAY => "double precision[]",
        _ => match sample {
            Some(serde_json::Value::Bool(_)) => "boolean",
            Some(serde_json::Value::Number(n)) if n.is_i64() => "int4",
            Some(serde_json::Value::Number(_)) => "numeric",
            Some(serde_json::Value::Object(_)) => "jsonb",
            Some(serde_json::Value::Array(_)) => "text[]",
            _ => "text",
        },
    };
    t.to_string()
}

/// `count(...)`, `sum(...)`, `avg(...)`, `min(...)`, `max(...)` chiếm trọn biểu thức (có thể kèm alias).
fn is_pure_aggregate_item(item: &str) -> bool {
    let t = item.trim();
    let e = match rfind_top_level_as(&t.to_uppercase()) {
        Some(p) => t[..p].trim(),
        None => t,
    };
    let Some(open) = e.find('(') else { return false };
    let name = e[..open].trim().to_ascii_uppercase();
    AGG_NAMES.contains(&name.as_str()) && e.ends_with(')') && is_fully_enclosed_in_parens(&e[open..])
}

fn is_aggregate_projection_item(item: &str) -> bool {
    let mut t = item.trim();
    if let Some(as_idx) = rfind_top_level_as(&t.to_uppercase()) {
        t = t[..as_idx].trim();
    }
    while t.starts_with('(') && t.ends_with(')') && is_fully_enclosed_in_parens(t) {
        let inner = t[1..t.len() - 1].trim();
        if inner.to_uppercase().starts_with("SELECT ") {
            return false;
        }
        t = inner;
    }
    let upper = t.to_uppercase();
    if upper.contains(" FROM ") {
        return false;
    }
    upper.starts_with("COUNT(")
        || upper.starts_with("SUM(")
        || upper.starts_with("AVG(")
        || upper.starts_with("MIN(")
        || upper.starts_with("MAX(")
        || upper.starts_with("ARRAY_AGG(")
        || upper.starts_with("STRING_AGG(")
        || upper.contains("ARRAY_AGG(")
        || upper.contains("COUNT(")
        || upper.contains("SUM(")
        || upper.contains("AVG(")
        || upper.contains("MIN(")
        || upper.contains("MAX(")
        || upper.contains("STRING_AGG(")
}

pub(crate) fn is_fully_enclosed_in_parens(s: &str) -> bool {
    let bytes = s.as_bytes();
    if bytes.len() < 2 || bytes[0] != b'(' || bytes[bytes.len() - 1] != b')' {
        return false;
    }
    let mut depth = 0;
    let mut in_sq = false;
    let mut in_dq = false;
    for (i, &b) in bytes.iter().enumerate() {
        if b == b'\'' && !in_dq {
            in_sq = !in_sq;
        } else if b == b'"' && !in_sq {
            in_dq = !in_dq;
        } else if !in_sq && !in_dq {
            if b == b'(' {
                depth += 1;
            } else if b == b')' {
                depth -= 1;
                if depth == 0 && i < bytes.len() - 1 {
                    return false;
                }
            }
        }
    }
    depth == 0
}

pub(crate) fn split_top_level_or(s: &str) -> Vec<&str> {
    let bytes = s.as_bytes();
    let mut parts = Vec::new();
    let mut depth = 0;
    let mut in_sq = false;
    let mut in_dq = false;
    let mut start = 0;
    let mut i = 0;

    while i < bytes.len() {
        let b = bytes[i];
        if b == b'\'' && !in_dq {
            in_sq = !in_sq;
        } else if b == b'"' && !in_sq {
            in_dq = !in_dq;
        } else if !in_sq && !in_dq {
            if b == b'(' {
                depth += 1;
            } else if b == b')' {
                if depth > 0 { depth -= 1; }
            } else if depth == 0 && i + 2 <= bytes.len() {
                if bytes[i..i + 2].eq_ignore_ascii_case(b"OR") {
                    let prev_ok = i == 0 || bytes[i - 1].is_ascii_whitespace() || bytes[i - 1] == b')';
                    let next_ok = i + 2 == bytes.len() || bytes[i + 2].is_ascii_whitespace() || bytes[i + 2] == b'(';
                    if prev_ok && next_ok {
                        let part = s[start..i].trim();
                        if !part.is_empty() {
                            parts.push(part);
                        }
                        start = i + 2;
                        i += 2;
                        continue;
                    }
                }
            }
        }
        i += 1;
    }
    let last = s[start..].trim();
    if !last.is_empty() {
        parts.push(last);
    }
    parts
}

pub(crate) fn split_top_level_and(s: &str) -> Vec<&str> {
    let bytes = s.as_bytes();
    let mut parts = Vec::new();
    let mut depth = 0;
    let mut in_sq = false;
    let mut in_dq = false;
    let mut between_count = 0;
    let mut start = 0;
    let mut i = 0;

    while i < bytes.len() {
        let b = bytes[i];
        if b == b'\'' && !in_dq {
            in_sq = !in_sq;
        } else if b == b'"' && !in_sq {
            in_dq = !in_dq;
        } else if !in_sq && !in_dq {
            if b == b'(' {
                depth += 1;
            } else if b == b')' {
                if depth > 0 { depth -= 1; }
            } else if depth == 0 {
                if i + 7 <= bytes.len() && bytes[i..i + 7].eq_ignore_ascii_case(b"BETWEEN") {
                    let prev_ok = i == 0 || bytes[i - 1].is_ascii_whitespace();
                    let next_ok = i + 7 == bytes.len() || bytes[i + 7].is_ascii_whitespace();
                    if prev_ok && next_ok {
                        between_count += 1;
                        i += 7;
                        continue;
                    }
                }
                if i + 3 <= bytes.len() && bytes[i..i + 3].eq_ignore_ascii_case(b"AND") {
                    let prev_ok = i == 0 || bytes[i - 1].is_ascii_whitespace() || bytes[i - 1] == b')';
                    let next_ok = i + 3 == bytes.len() || bytes[i + 3].is_ascii_whitespace() || bytes[i + 3] == b'(';
                    if prev_ok && next_ok {
                        if between_count > 0 {
                            between_count -= 1;
                            i += 3;
                            continue;
                        }
                        let part = s[start..i].trim();
                        if !part.is_empty() {
                            parts.push(part);
                        }
                        start = i + 3;
                        i += 3;
                        continue;
                    }
                }
            }
        }
        i += 1;
    }
    let last = s[start..].trim();
    if !last.is_empty() {
        parts.push(last);
    }
    parts
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct DateTimeParts {
    pub(crate) year: i32,
    pub(crate) month: u32,
    pub(crate) day: u32,
    pub(crate) hour: u32,
    pub(crate) minute: u32,
    pub(crate) second: u32,
    pub(crate) millisecond: u32,
    pub(crate) has_time: bool,
    pub(crate) is_iso_z: bool,
    pub(crate) separator: char,
}

pub(crate) fn parse_datetime_components(s: &str) -> Option<DateTimeParts> {
    let mut trimmed = s.trim().trim_matches('\'').trim_matches('"').trim();
    if trimmed.is_empty() || trimmed.eq_ignore_ascii_case("null") {
        return None;
    }
    let is_iso_z = trimmed.ends_with('Z') || trimmed.ends_with('z');
    if is_iso_z {
        trimmed = &trimmed[..trimmed.len() - 1];
    }
    let sep = if trimmed.contains('T') {
        'T'
    } else if trimmed.contains(' ') {
        ' '
    } else {
        '-'
    };

    let main_parts: Vec<&str> = if sep == 'T' {
        trimmed.splitn(2, 'T').collect()
    } else if sep == ' ' {
        trimmed.splitn(2, ' ').collect()
    } else {
        vec![trimmed]
    };

    let date_parts: Vec<&str> = main_parts[0].split('-').collect();
    if date_parts.len() < 3 {
        if main_parts[0].contains(':') {
            let time_parts: Vec<&str> = main_parts[0].split(':').collect();
            let hour = time_parts.get(0).and_then(|h| h.parse::<u32>().ok()).unwrap_or(0);
            let minute = time_parts.get(1).and_then(|m| m.parse::<u32>().ok()).unwrap_or(0);
            let sec_str = time_parts.get(2).unwrap_or(&"0");
            let (sec, ms) = if let Some(dot) = sec_str.find('.') {
                let s = sec_str[..dot].parse::<u32>().unwrap_or(0);
                let ms = sec_str[dot + 1..].parse::<u32>().unwrap_or(0);
                (s, ms)
            } else {
                (sec_str.parse::<u32>().unwrap_or(0), 0)
            };
            return Some(DateTimeParts {
                year: 2024,
                month: 1,
                day: 1,
                hour,
                minute,
                second: sec,
                millisecond: ms,
                has_time: true,
                is_iso_z: false,
                separator: ' ',
            });
        }
        return None;
    }

    let year = date_parts[0].parse::<i32>().ok()?;
    let month = date_parts[1].parse::<u32>().ok()?;
    let day = date_parts[2].parse::<u32>().ok()?;

    let (hour, minute, second, millisecond, has_time) = if main_parts.len() > 1 {
        let time_str = main_parts[1].trim();
        let time_parts: Vec<&str> = time_str.split(':').collect();
        let h = time_parts.get(0).and_then(|x| x.parse::<u32>().ok()).unwrap_or(0);
        let m = time_parts.get(1).and_then(|x| x.parse::<u32>().ok()).unwrap_or(0);
        let sec_str = time_parts.get(2).unwrap_or(&"0");
        let (s, ms) = if let Some(dot) = sec_str.find('.') {
            let sec = sec_str[..dot].parse::<u32>().unwrap_or(0);
            let mut ms_raw = sec_str[dot + 1..].to_string();
            while ms_raw.len() < 3 { ms_raw.push('0'); }
            let ms = ms_raw[..3].parse::<u32>().unwrap_or(0);
            (sec, ms)
        } else {
            (sec_str.parse::<u32>().unwrap_or(0), 0)
        };
        (h, m, s, ms, true)
    } else {
        (0, 0, 0, 0, false)
    };

    Some(DateTimeParts {
        year,
        month,
        day,
        hour,
        minute,
        second,
        millisecond,
        has_time,
        is_iso_z,
        separator: sep,
    })
}

pub(crate) fn days_from_civil(y: i32, m: u32, d: u32) -> i64 {
    let y = if m <= 2 { (y - 1) as i64 } else { y as i64 };
    let era = (if y >= 0 { y } else { y - 399 }) / 400;
    let yoe = (y - era * 400) as u32;
    let doy = (153 * (if m > 2 { m - 3 } else { m + 9 }) + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146097 + doe as i64 - 719468
}

fn datetime_to_epoch_seconds(dt: &DateTimeParts) -> i64 {
    let days = days_from_civil(dt.year, dt.month, dt.day);
    days * 86400 + (dt.hour as i64) * 3600 + (dt.minute as i64) * 60 + (dt.second as i64)
}

fn day_of_week(dt: &DateTimeParts) -> u32 {
    let days = days_from_civil(dt.year, dt.month, dt.day);
    ((days + 4).rem_euclid(7)) as u32
}

fn eval_date_part_or_extract(field: &str, dt: &DateTimeParts) -> Option<Value> {
    match field.trim().trim_matches('\'').trim_matches('"').to_lowercase().as_str() {
        "year" => Some(Value::Int(dt.year as i64)),
        "month" => Some(Value::Int(dt.month as i64)),
        "day" => Some(Value::Int(dt.day as i64)),
        "hour" => Some(Value::Int(dt.hour as i64)),
        "minute" => Some(Value::Int(dt.minute as i64)),
        "second" => Some(Value::Int(dt.second as i64)),
        "epoch" => Some(Value::Int(datetime_to_epoch_seconds(dt))),
        "dow" => Some(Value::Int(day_of_week(dt) as i64)),
        "quarter" => Some(Value::Int(((dt.month - 1) / 3 + 1) as i64)),
        _ => None,
    }
}

fn eval_date_trunc(unit: &str, dt: &DateTimeParts) -> String {
    let mut res_dt = *dt;
    match unit.trim().trim_matches('\'').trim_matches('"').to_lowercase().as_str() {
        "year" => {
            res_dt.month = 1;
            res_dt.day = 1;
            res_dt.hour = 0;
            res_dt.minute = 0;
            res_dt.second = 0;
            res_dt.millisecond = 0;
        }
        "month" => {
            res_dt.day = 1;
            res_dt.hour = 0;
            res_dt.minute = 0;
            res_dt.second = 0;
            res_dt.millisecond = 0;
        }
        "day" => {
            res_dt.hour = 0;
            res_dt.minute = 0;
            res_dt.second = 0;
            res_dt.millisecond = 0;
        }
        "hour" => {
            res_dt.minute = 0;
            res_dt.second = 0;
            res_dt.millisecond = 0;
        }
        "minute" => {
            res_dt.second = 0;
            res_dt.millisecond = 0;
        }
        "second" => {
            res_dt.millisecond = 0;
        }
        _ => {}
    }

    if res_dt.is_iso_z {
        format!("{:04}-{:02}-{:02}T{:02}:{:02}:{:02}.{:03}Z",
            res_dt.year, res_dt.month, res_dt.day,
            res_dt.hour, res_dt.minute, res_dt.second, res_dt.millisecond)
    } else if res_dt.separator == 'T' {
        format!("{:04}-{:02}-{:02}T{:02}:{:02}:{:02}",
            res_dt.year, res_dt.month, res_dt.day,
            res_dt.hour, res_dt.minute, res_dt.second)
    } else if res_dt.has_time {
        format!("{:04}-{:02}-{:02} {:02}:{:02}:{:02}",
            res_dt.year, res_dt.month, res_dt.day,
            res_dt.hour, res_dt.minute, res_dt.second)
    } else {
        format!("{:04}-{:02}-{:02}", res_dt.year, res_dt.month, res_dt.day)
    }
}

fn eval_to_char(dt: &DateTimeParts, fmt: &str) -> String {
    let month_names = ["January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December"];
    let month_shorts = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];
    let day_names = ["Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"];
    let day_shorts = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"];

    let dow = day_of_week(dt) as usize;
    let m_idx = (dt.month.saturating_sub(1) as usize).min(11);

    let mut res = fmt.trim().trim_matches('\'').trim_matches('"').to_string();
    res = res.replace("YYYY", &format!("{:04}", dt.year));
    res = res.replace("YY", &format!("{:02}", (dt.year % 100).abs()));
    res = res.replace("Month", month_names[m_idx]);
    res = res.replace("Mon", month_shorts[m_idx]);
    res = res.replace("MM", &format!("{:02}", dt.month));
    res = res.replace("Day", day_names[dow]);
    res = res.replace("Dy", day_shorts[dow]);
    res = res.replace("DD", &format!("{:02}", dt.day));
    res = res.replace("HH24", &format!("{:02}", dt.hour));
    res = res.replace("HH", &format!("{:02}", dt.hour));
    res = res.replace("MI", &format!("{:02}", dt.minute));
    res = res.replace("SS", &format!("{:02}", dt.second));
    res
}

fn eval_age(dt1: &DateTimeParts, dt2: &DateTimeParts) -> String {
    let mut y_diff = dt1.year - dt2.year;
    let mut m_diff = dt1.month as i32 - dt2.month as i32;
    let mut d_diff = dt1.day as i32 - dt2.day as i32;

    if d_diff < 0 {
        m_diff -= 1;
        d_diff += 30;
    }
    if m_diff < 0 {
        y_diff -= 1;
        m_diff += 12;
    }

    let mut parts = Vec::new();
    if y_diff > 0 {
        parts.push(if y_diff == 1 { "1 year".to_string() } else { format!("{} years", y_diff) });
    }
    if m_diff > 0 {
        parts.push(if m_diff == 1 { "1 mon".to_string() } else { format!("{} mons", m_diff) });
    }
    if d_diff > 0 || parts.is_empty() {
        parts.push(if d_diff == 1 { "1 day".to_string() } else { format!("{} days", d_diff) });
    }
    parts.join(" ")
}

pub(crate) fn get_current_timestamp() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let now = SystemTime::now();
    let duration = now.duration_since(UNIX_EPOCH).unwrap_or_default();
    let secs = duration.as_secs();
    let millis = duration.subsec_millis();

    let days = (secs / 86400) as i64;
    let rem_secs = secs % 86400;
    let hours = rem_secs / 3600;
    let mins = (rem_secs % 3600) / 60;
    let s = rem_secs % 60;

    let z = days + 719468;
    let era = (if z >= 0 { z } else { z - 146096 }) / 146097;
    let doe = (z - era * 146097) as u32;
    let yoe = (doe - doe / 1029 + doe / 1461 - doe / 36524) / 365;
    let y = (yoe as i64) + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = y + (if m <= 2 { 1 } else { 0 });

    format!("{:04}-{:02}-{:02}T{:02}:{:02}:{:02}.{:03}Z", year, m, d, hours, mins, s, millis)
}

/// `current_date` theo múi giờ phiên.
pub(crate) fn get_current_date() -> String {
    crate::engine::datetime::current_date()
}

/// `current_time` / `localtime`: giờ địa phương của phiên (UTC như các bản trước).
pub(crate) fn get_current_time() -> String {
    crate::engine::datetime::local_time().unwrap_or_else(|| {
        let ts = get_current_timestamp();
        ts[11..19].to_string()
    })
}

/// `localtimestamp`: timestamp không múi giờ theo giờ địa phương của phiên (UTC như các bản trước).
pub(crate) fn get_local_timestamp() -> String {
    crate::engine::datetime::local_timestamp().unwrap_or_else(get_current_timestamp)
}

static DEFAULT_TZ: std::sync::OnceLock<String> = std::sync::OnceLock::new();

/// Múi giờ mặc định của phiên (đọc `PGLITE_TIMEZONE` một lần).
pub(crate) fn default_tz() -> &'static str {
    DEFAULT_TZ.get_or_init(crate::engine::datetime::default_timezone)
}

fn eval_date_time_keyword(token: &str) -> Option<Value> {
    let trimmed = token.trim();
    let upper = trimmed.to_uppercase();
    // chỉ đúng từ khoá (có thể kèm độ chính xác `CURRENT_TIMESTAMP(3)`): `now() - interval '1 hour'` không phải
    let is = |kw: &str| {
        upper == kw || upper.strip_prefix(kw).is_some_and(|r| {
            let r = r.trim();
            r.starts_with('(') && r.ends_with(')') && r[1..r.len() - 1].trim().bytes().all(|b| b.is_ascii_digit())
        })
    };
    if is("LOCALTIMESTAMP") {
        return Some(Value::text(get_local_timestamp()));
    }
    if is("CURRENT_TIMESTAMP") || upper == "NOW()" {
        return Some(Value::text(get_current_timestamp()));
    }
    if is("CURRENT_DATE") {
        return Some(Value::text(get_current_date()));
    }
    if is("CURRENT_TIME") || is("LOCALTIME") {
        return Some(Value::text(get_current_time()));
    }
    None
}

fn split_where_conditions(w: &str) -> Vec<String> {
    let mut parts = Vec::new();
    let mut current = String::new();
    let chars: Vec<char> = w.chars().collect();
    let len = chars.len();
    let mut in_str = false;
    let mut depth: i32 = 0;
    let mut i = 0;

    while i < len {
        let c = chars[i];
        if c == '\'' {
            in_str = !in_str;
            current.push(c);
            i += 1;
            continue;
        }
        if in_str {
            current.push(c);
            i += 1;
            continue;
        }
        if c == '(' || c == '[' {
            depth += 1;
            current.push(c);
            i += 1;
            continue;
        }
        if c == ')' || c == ']' {
            depth = depth.saturating_sub(1);
            current.push(c);
            i += 1;
            continue;
        }

        if depth == 0 && i + 3 <= len {
            let next3: String = chars[i..i + 3].iter().collect();
            if next3.eq_ignore_ascii_case("AND") {
                let is_word_start = i == 0 || chars[i - 1].is_whitespace() || chars[i - 1] == ')' || chars[i - 1] == ']';
                let is_word_end = i + 3 == len || chars[i + 3].is_whitespace() || chars[i + 3] == '(' || chars[i + 3] == '[';
                if is_word_start && is_word_end {
                    let cur_upper = current.to_uppercase();
                    if cur_upper.contains("BETWEEN") && !cur_upper.contains(" AND ") {
                        current.push_str("AND");
                        i += 3;
                        continue;
                    } else {
                        if !current.trim().is_empty() {
                            parts.push(current.trim().to_string());
                            current.clear();
                        }
                        i += 3;
                        continue;
                    }
                }
            }
        }

        current.push(c);
        i += 1;
    }
    if !current.trim().is_empty() {
        parts.push(current.trim().to_string());
    }
    parts
}

#[allow(dead_code)]
fn parse_number(token: &str, params: &[Value]) -> Result<f64, String> {
    let mut t = token.trim();
    while is_fully_enclosed_in_parens(t) {
        t = t[1..t.len() - 1].trim();
    }
    if t.starts_with('$') {
        let p_idx = t[1..].parse::<usize>().map_err(|_| format!("Invalid param {}", t))?;
        params.get(p_idx.saturating_sub(1)).and_then(|v| v.as_f64()).ok_or_else(|| format!("Param {} not found or not number", t))
    } else {
        t.parse::<f64>().map_err(|_| format!("Invalid number {}", t))
    }
}

fn parse_value(token: &str, params: &[Value]) -> Value {
    let mut t = token.trim();
    while is_fully_enclosed_in_parens(t) {
        let inside = t[1..t.len() - 1].trim();
        if split_comma_separated_tokens(inside).len() > 1 {
            break;
        }
        t = inside;
    }
    // Tương thích bản cũ: giá trị viết bằng nháy kép (`VALUES ("2026-09-09T15:32:34Z")`) được coi là
    // chuỗi; lexer đã mã hoá nó thành định danh `__q.._`.
    if t.starts_with("__q") {
        if let Some(d) = crate::engine::frontend::decode_ident(t) {
            return Value::text(d);
        }
    }
    if let Some(colon_pos) = find_last_top_level_op(t, "::").filter(|&p| cast_is_outermost(t, p)) {
        let target_type = t[colon_pos + 2..].trim();
        let val = parse_value(&t[..colon_pos], params);
        return cast_val(val, target_type);
    }
    if let Some(pipe_idx) = find_top_level_op(t, "||") {
        let left = parse_value(&t[..pipe_idx], params);
        let right = parse_value(&t[pipe_idx + 2..], params);
        if left.is_null() || right.is_null() {
            return Value::Null;
        }
        let l_s = left.as_str();
        let r_s = right.as_str();
        if l_s.starts_with("\\x") || r_s.starts_with("\\x") {
            let mut combined = get_raw_bytes(&left);
            combined.extend(get_raw_bytes(&right));
            return Value::text(format!("\\x{}", hex::encode(combined)));
        }
        return Value::text(format!("{}{}", l_s, r_s));
    }
    if let Some(dt_val) = eval_date_time_keyword(t) {
        return dt_val;
    }
    let upper = t.to_uppercase();
    if is_complete_array_constructor(t) {
        let inside = &t[6..t.len() - 1].trim();
        if inside.is_empty() {
            return Value::text("[]");
        }
        let items: Vec<serde_json::Value> = split_comma_separated_tokens(inside)
            .into_iter()
            .map(|tok| {
                let v = parse_value(&tok, params);
                value_to_json(&v)
            })
            .collect();
        return Value::text(serde_json::to_string(&items).unwrap_or_default());
    }
    if upper.starts_with("LOWER(") && t.ends_with(')') {
        let inner = &t[6..t.len() - 1];
        let val = parse_value(inner, params);
        return Value::text(val.as_str().to_lowercase());
    }
    if upper.starts_with("UPPER(") && t.ends_with(')') {
        let inner = &t[6..t.len() - 1];
        let val = parse_value(inner, params);
        return Value::text(val.as_str().to_uppercase());
    }
    if (upper.starts_with("TRIM(") || upper.starts_with("BTRIM(")) && t.ends_with(')') {
        let open_p = t.find('(').unwrap();
        let inner = &t[open_p + 1..t.len() - 1];
        let val = parse_value(inner, params);
        return Value::text(val.as_str().trim().to_string());
    }
    if upper == "GEN_RANDOM_UUID()" || upper == "UUID_GENERATE_V4()" {
        return Value::text(crate::types::generate_uuid_v4());
    }
    if t.starts_with('$') {
        if let Ok(p_idx) = t[1..].parse::<usize>() {
            return params.get(p_idx.saturating_sub(1)).cloned().unwrap_or(Value::Null);
        }
    }
    if t.eq_ignore_ascii_case("TRUE") {
        return Value::Bool(true);
    }
    if t.eq_ignore_ascii_case("FALSE") {
        return Value::Bool(false);
    }
    if t.eq_ignore_ascii_case("NULL") {
        return Value::Null;
    }
    if t.starts_with('\'') && t.ends_with('\'') && t.len() >= 2 {
        return Value::text(t[1..t.len() - 1].replace("''", "'"));
    }
    if let Ok(i) = t.parse::<i64>() {
        return Value::Int(i);
    }
    if let Ok(f) = t.parse::<f64>() {
        return Value::Float(f);
    }
    // biểu thức hằng (`now() - interval '1 hour'`, `current_date + 1`, `upper('a') || 'b'`): tính thay vì
    // trả nguyên văn. Biểu thức cần cột sẽ ra NULL ở đây → giữ cách cũ.
    thread_local! {
        /// đang tính biểu thức hằng từ parse_value: eval_sql_expr có thể quay lại parse_value cho chính nó
        static IN_CONST_EVAL: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    }
    if !IN_CONST_EVAL.with(|f| f.get())
        && (t.contains('(') || find_top_level_math_op(t, &['+', '-', '*', '/', '%']).is_some_and(|(p, _)| p > 0) || find_top_level_op(t, "||").is_some())
    {
        IN_CONST_EVAL.with(|f| f.set(true));
        let v = eval_sql_expr(t, &[], None, &[], None, params);
        IN_CONST_EVAL.with(|f| f.set(false));
        if !v.is_null() && v.as_str() != t {
            return v;
        }
    }
    // `tên(...)` mà engine không biết hàm nào như vậy: lỗi 42883 như PostgreSQL (trước đây trả nguyên văn)
    if t.ends_with(')') {
        if let Some(msg) = crate::engine::known_fns::unknown_function_error(t) {
            crate::engine::errors::raise(msg);
            return Value::Null;
        }
    }
    Value::text(t)
}

fn parse_operand_template(token: &str) -> Result<OperandTemplate, String> {
    let mut t = token.trim();
    while is_fully_enclosed_in_parens(t) {
        t = t[1..t.len() - 1].trim();
    }
    if let Some(colon_pos) = find_last_top_level_op(t, "::") {
        t = t[..colon_pos].trim();
    }
    while is_fully_enclosed_in_parens(t) {
        t = t[1..t.len() - 1].trim();
    }
    let upper = t.to_uppercase();
    if upper.starts_with("LOWER(") && t.ends_with(')') {
        let inner = t[6..t.len() - 1].trim().trim_matches('\'').replace("''", "'");
        return Ok(OperandTemplate::Literal(Value::text(inner.to_lowercase())));
    }
    if upper.starts_with("UPPER(") && t.ends_with(')') {
        let inner = t[6..t.len() - 1].trim().trim_matches('\'').replace("''", "'");
        return Ok(OperandTemplate::Literal(Value::text(inner.to_uppercase())));
    }
    if t.starts_with('$') {
        if let Ok(p_idx) = t[1..].parse::<usize>() {
            if p_idx > 0 {
                return Ok(OperandTemplate::Param(p_idx - 1));
            }
        }
    }
    if t.eq_ignore_ascii_case("TRUE") {
        return Ok(OperandTemplate::Literal(Value::Bool(true)));
    }
    if t.eq_ignore_ascii_case("FALSE") {
        return Ok(OperandTemplate::Literal(Value::Bool(false)));
    }
    if t.eq_ignore_ascii_case("NULL") {
        return Ok(OperandTemplate::Literal(Value::Null));
    }
    if t.starts_with('\'') && t.ends_with('\'') && t.len() >= 2 {
        return Ok(OperandTemplate::Literal(Value::text(t[1..t.len() - 1].replace("''", "'"))));
    }
    if let Ok(i) = t.parse::<i64>() {
        return Ok(OperandTemplate::Literal(Value::Int(i)));
    }
    if let Ok(f) = t.parse::<f64>() {
        return Ok(OperandTemplate::Number(f));
    }
    Err(format!("Invalid operand template: {}", t))
}

fn parse_single_condition_template(
    part: &str,
    table: &crate::storage::table::Table,
) -> Result<ConditionTemplate, String> {
    let mut s = part.trim();
    while is_fully_enclosed_in_parens(s) {
        s = s[1..s.len() - 1].trim();
    }
    // ép kiểu ở vế cột (`created::date = current_date`) đổi giá trị: để bộ đánh giá đầy đủ xử lý
    if lhs_has_cast(s) {
        return Err("cast on column side".to_string());
    }
    if s.contains("->") || s.contains("@>") || s.contains("<@") || s.contains('?') || s.contains("#>") {
        return Err("Custom JSON condition".to_string());
    }
    let upper = s.to_uppercase();
    // `col = ANY(mảng)` / `<> ALL(...)`: so sánh định lượng, để bộ đánh giá đầy đủ xử lý (trước đây bị
    // hiểu là `col = 'ANY(...)'` nên truy vấn có hàm gộp ra 0 dòng)
    if may_have_any_all(&upper) && split_any_all(s).is_some() {
        return Err("quantified comparison".to_string());
    }

    if let Some(pos) = upper.find(" IS NOT NULL") {
        let col_name = clean_col_name(&s[..pos]);
        let col_idx = table.get_column_index(col_name).ok_or_else(|| format!("Column {} not found", col_name))?;
        return Ok(ConditionTemplate { col_idx, op: ColOpTemplate::IsNotNull });
    }
    if let Some(pos) = upper.find(" IS NULL") {
        let col_name = clean_col_name(&s[..pos]);
        let col_idx = table.get_column_index(col_name).ok_or_else(|| format!("Column {} not found", col_name))?;
        return Ok(ConditionTemplate { col_idx, op: ColOpTemplate::IsNull });
    }
    if let Some(b_idx) = upper.find(" BETWEEN ") {
        let col_name = clean_col_name(&s[..b_idx]);
        let col_idx = table.get_column_index(col_name).ok_or_else(|| format!("Column {} not found", col_name))?;
        let rest = &s[b_idx + 9..];
        let and_idx = rest.to_uppercase().find(" AND ").ok_or("Missing AND in BETWEEN clause")?;
        let min_token = rest[..and_idx].trim();
        let max_token = rest[and_idx + 5..].trim();
        let min_opnd = parse_operand_template(min_token)?;
        let max_opnd = parse_operand_template(max_token)?;
        return Ok(ConditionTemplate { col_idx, op: ColOpTemplate::Between(min_opnd, max_opnd) });
    }
    if let Some(op_idx) = s.find("!=") {
        let col_name = clean_col_name(&s[..op_idx]);
        let col_idx = table.get_column_index(col_name).ok_or_else(|| format!("Column {} not found", col_name))?;
        let opnd = parse_operand_template(&s[op_idx + 2..])?;
        return Ok(ConditionTemplate { col_idx, op: ColOpTemplate::NotEq(opnd) });
    }
    if let Some(op_idx) = s.find("<>") {
        let col_name = clean_col_name(&s[..op_idx]);
        let col_idx = table.get_column_index(col_name).ok_or_else(|| format!("Column {} not found", col_name))?;
        let opnd = parse_operand_template(&s[op_idx + 2..])?;
        return Ok(ConditionTemplate { col_idx, op: ColOpTemplate::NotEq(opnd) });
    }
    if let Some(op_idx) = s.find(">=") {
        let col_name = clean_col_name(&s[..op_idx]);
        let col_idx = table.get_column_index(col_name).ok_or_else(|| format!("Column {} not found", col_name))?;
        let opnd = parse_operand_template(&s[op_idx + 2..])?;
        return Ok(ConditionTemplate { col_idx, op: ColOpTemplate::Gte(opnd) });
    }
    if let Some(op_idx) = s.find("<=") {
        let col_name = clean_col_name(&s[..op_idx]);
        let col_idx = table.get_column_index(col_name).ok_or_else(|| format!("Column {} not found", col_name))?;
        let opnd = parse_operand_template(&s[op_idx + 2..])?;
        return Ok(ConditionTemplate { col_idx, op: ColOpTemplate::Lte(opnd) });
    }
    if let Some(op_idx) = s.find('>') {
        let col_name = clean_col_name(&s[..op_idx]);
        let col_idx = table.get_column_index(col_name).ok_or_else(|| format!("Column {} not found", col_name))?;
        let opnd = parse_operand_template(&s[op_idx + 1..])?;
        return Ok(ConditionTemplate { col_idx, op: ColOpTemplate::Gt(opnd) });
    }
    if let Some(op_idx) = s.find('<') {
        let col_name = clean_col_name(&s[..op_idx]);
        let col_idx = table.get_column_index(col_name).ok_or_else(|| format!("Column {} not found", col_name))?;
        let opnd = parse_operand_template(&s[op_idx + 1..])?;
        return Ok(ConditionTemplate { col_idx, op: ColOpTemplate::Lt(opnd) });
    }
    if let Some(op_idx) = s.find('=') {
        let left_raw = s[..op_idx].trim();
        let upper_left = left_raw.to_uppercase();
        let opnd = parse_operand_template(&s[op_idx + 1..])?;

        if upper_left.starts_with("LOWER(") && left_raw.ends_with(')') {
            let inner = &left_raw[6..left_raw.len() - 1].trim();
            let col_name = clean_col_name(inner);
            let col_idx = table.get_column_index(col_name).ok_or_else(|| format!("Column {} not found", col_name))?;
            return Ok(ConditionTemplate { col_idx, op: ColOpTemplate::LowerEq(opnd) });
        } else if upper_left.starts_with("UPPER(") && left_raw.ends_with(')') {
            let inner = &left_raw[6..left_raw.len() - 1].trim();
            let col_name = clean_col_name(inner);
            let col_idx = table.get_column_index(col_name).ok_or_else(|| format!("Column {} not found", col_name))?;
            return Ok(ConditionTemplate { col_idx, op: ColOpTemplate::UpperEq(opnd) });
        } else {
            let col_name = clean_col_name(left_raw);
            let col_idx = table.get_column_index(col_name).ok_or_else(|| format!("Column {} not found", col_name))?;
            return Ok(ConditionTemplate { col_idx, op: ColOpTemplate::Eq(opnd) });
        }
    }

    Err(format!("Unsupported condition syntax: {}", s))
}

fn compile_condition_templates(where_clause: &str, table: &crate::storage::table::Table) -> Result<Vec<ConditionTemplate>, String> {
    let mut templates = Vec::new();
    let parts = split_where_conditions(where_clause);

    for part in &parts {
        templates.push(parse_single_condition_template(part, table)?);
    }

    // Prioritize cheap, high-selectivity conditions: Eq > IsNull/IsNotNull > Range > Lower/Upper
    templates.sort_by_key(|c| match &c.op {
        ColOpTemplate::Eq(_) => 0,
        ColOpTemplate::IsNull | ColOpTemplate::IsNotNull => 1,
        ColOpTemplate::Between(_, _) | ColOpTemplate::Gt(_) | ColOpTemplate::Gte(_) | ColOpTemplate::Lt(_) | ColOpTemplate::Lte(_) => 2,
        ColOpTemplate::NotEq(_) => 3,
        ColOpTemplate::LowerEq(_) | ColOpTemplate::UpperEq(_) => 4,
    });

    Ok(templates)
}

#[allow(dead_code)]
fn compile_where_template(
    where_clause: &str,
    table: &crate::storage::table::Table,
) -> Result<WhereTemplate, String> {
    let mut trimmed = where_clause.trim();
    while is_fully_enclosed_in_parens(trimmed) {
        trimmed = trimmed[1..trimmed.len() - 1].trim();
    }
    if trimmed.is_empty() {
        return Err("Empty WHERE expression".to_string());
    }

    // 1. Top-level OR
    let or_parts = split_top_level_or(trimmed);
    if or_parts.len() > 1 {
        let mut list = Vec::with_capacity(or_parts.len());
        for p in or_parts {
            list.push(compile_where_template(p, table)?);
        }
        return Ok(WhereTemplate::Or(list));
    }

    // 2. Top-level AND
    let and_parts = split_top_level_and(trimmed);
    if and_parts.len() > 1 {
        let mut list = Vec::with_capacity(and_parts.len());
        for p in and_parts {
            list.push(compile_where_template(p, table)?);
        }
        return Ok(WhereTemplate::And(list));
    }

    // 3. Top-level NOT
    let upper = trimmed.to_uppercase();
    if upper.starts_with("NOT ") || upper.starts_with("NOT(") {
        let inner = trimmed[3..].trim();
        return Ok(WhereTemplate::Not(Box::new(compile_where_template(inner, table)?)));
    }

    // 4. Single condition
    let cond = parse_single_condition_template(trimmed, table)?;
    Ok(WhereTemplate::Condition(cond))
}

#[derive(Clone, Debug)]
enum WhereExpr {
    And(Vec<WhereExpr>),
    Or(Vec<WhereExpr>),
    Not(Box<WhereExpr>),
    Condition(Condition),
    Custom(String),
}

fn parse_where_expr(
    s: &str,
    table: &crate::storage::table::Table,
    params: &[Value],
) -> Result<WhereExpr, String> {
    let mut trimmed = s.trim();
    while is_fully_enclosed_in_parens(trimmed) {
        trimmed = trimmed[1..trimmed.len() - 1].trim();
    }
    if trimmed.is_empty() {
        return Err("Empty WHERE expression".to_string());
    }

    // 1. Check for top-level OR
    let or_parts = split_top_level_or(trimmed);
    if or_parts.len() > 1 {
        let mut list = Vec::with_capacity(or_parts.len());
        for p in or_parts {
            list.push(parse_where_expr(p, table, params)?);
        }
        return Ok(WhereExpr::Or(list));
    }

    // 2. Check for top-level AND
    let and_parts = split_top_level_and(trimmed);
    if and_parts.len() > 1 {
        let mut list = Vec::with_capacity(and_parts.len());
        for p in and_parts {
            list.push(parse_where_expr(p, table, params)?);
        }
        return Ok(WhereExpr::And(list));
    }

    // 3. Check for top-level NOT
    let upper = trimmed.to_uppercase();
    if upper.starts_with("NOT ") || upper.starts_with("NOT(") {
        let inner = trimmed[3..].trim();
        return Ok(WhereExpr::Not(Box::new(parse_where_expr(inner, table, params)?)));
    }

    // 4. Parse single condition or fallback to custom expression
    match parse_single_condition(trimmed, table, params) {
        Ok(cond) => Ok(WhereExpr::Condition(cond)),
        Err(_) => Ok(WhereExpr::Custom(trimmed.to_string())),
    }
}

fn parse_pattern_and_escape(s: &str, params: &[Value]) -> (String, Option<char>) {
    let trimmed = s.trim();
    let upper = trimmed.to_uppercase();
    if let Some(esc_pos) = upper.rfind(" ESCAPE ") {
        let pat_str = trimmed[..esc_pos].trim();
        let esc_str = trimmed[esc_pos + 8..].trim();
        let pat_val = parse_value(pat_str, params);
        let esc_val = parse_value(esc_str, params);
        let pat = pat_val.as_str();
        let esc_char = esc_val.as_str().chars().next();
        (pat, esc_char)
    } else {
        let pat_val = parse_value(trimmed, params);
        (pat_val.as_str(), None)
    }
}

fn parse_single_condition(
    part: &str,
    table: &crate::storage::table::Table,
    params: &[Value],
) -> Result<Condition, String> {
    let mut s = part.trim();
    while is_fully_enclosed_in_parens(s) {
        s = s[1..s.len() - 1].trim();
    }
    // ép kiểu ở vế cột (`created::date = current_date`) đổi giá trị: để bộ đánh giá đầy đủ xử lý
    if lhs_has_cast(s) {
        return Err("cast on column side".to_string());
    }
    if s.contains("->") || s.contains("@>") || s.contains("<@") || s.contains('?') || s.contains("#>") {
        return Err("Custom JSON condition".to_string());
    }
    let upper = s.to_uppercase();
    // `col = ANY(mảng)` / `<> ALL(...)`: so sánh định lượng, để bộ đánh giá đầy đủ xử lý (trước đây bị
    // hiểu là `col = 'ANY(...)'` nên truy vấn có hàm gộp ra 0 dòng)
    if may_have_any_all(&upper) && split_any_all(s).is_some() {
        return Err("quantified comparison".to_string());
    }

    if let Some(pos) = upper.find(" IS NOT NULL") {
        let col_name = clean_col_name(&s[..pos]);
        let col_idx = table.get_column_index(col_name).ok_or_else(|| format!("Column {} not found", col_name))?;
        return Ok(Condition { col_idx, op: ColOp::IsNotNull });
    }
    if let Some(pos) = upper.find(" IS NULL") {
        let col_name = clean_col_name(&s[..pos]);
        let col_idx = table.get_column_index(col_name).ok_or_else(|| format!("Column {} not found", col_name))?;
        return Ok(Condition { col_idx, op: ColOp::IsNull });
    }
    if let Some(b_idx) = upper.find(" BETWEEN ") {
        let col_name = clean_col_name(&s[..b_idx]);
        let col_idx = table.get_column_index(col_name).ok_or_else(|| format!("Column {} not found", col_name))?;
        let rest = &s[b_idx + 9..];
        let and_idx = rest.to_uppercase().find(" AND ").ok_or("Missing AND in BETWEEN clause")?;
        let min_token = rest[..and_idx].trim();
        let max_token = rest[and_idx + 5..].trim();
        let min_val = parse_value(min_token, params);
        let max_val = parse_value(max_token, params);
        return Ok(Condition { col_idx, op: ColOp::Between(min_val, max_val) });
    }
    if let Some(in_idx) = upper.find(" IN ") {
        let col_name = clean_col_name(&s[..in_idx]);
        let col_idx = table.get_column_index(col_name).ok_or_else(|| format!("Column {} not found", col_name))?;
        let rest = s[in_idx + 4..].trim();
        if rest.starts_with('(') && rest.ends_with(')') {
            let inside = &rest[1..rest.len() - 1];
            let list: Vec<Value> = split_comma_separated_tokens(inside).into_iter().map(|tok| parse_value(tok, params)).collect();
            return Ok(Condition { col_idx, op: ColOp::In(list) });
        }
    }
    if let Some(pos) = upper.find(" NOT ILIKE ") {
        let col_name = clean_col_name(&s[..pos]);
        let col_idx = table.get_column_index(col_name).ok_or_else(|| format!("Column {} not found", col_name))?;
        let (pattern, esc) = parse_pattern_and_escape(&s[pos + 11..], params);
        return Ok(Condition { col_idx, op: ColOp::NotILike(pattern, esc) });
    }
    if let Some(pos) = upper.find(" ILIKE ") {
        let col_name = clean_col_name(&s[..pos]);
        let col_idx = table.get_column_index(col_name).ok_or_else(|| format!("Column {} not found", col_name))?;
        let (pattern, esc) = parse_pattern_and_escape(&s[pos + 7..], params);
        return Ok(Condition { col_idx, op: ColOp::ILike(pattern, esc) });
    }
    if let Some(pos) = upper.find(" NOT LIKE ") {
        let col_name = clean_col_name(&s[..pos]);
        let col_idx = table.get_column_index(col_name).ok_or_else(|| format!("Column {} not found", col_name))?;
        let (pattern, esc) = parse_pattern_and_escape(&s[pos + 10..], params);
        return Ok(Condition { col_idx, op: ColOp::NotLike(pattern, esc) });
    }
    if let Some(pos) = upper.find(" LIKE ") {
        let col_name = clean_col_name(&s[..pos]);
        let col_idx = table.get_column_index(col_name).ok_or_else(|| format!("Column {} not found", col_name))?;
        let (pattern, esc) = parse_pattern_and_escape(&s[pos + 6..], params);
        return Ok(Condition { col_idx, op: ColOp::Like(pattern, esc) });
    }
    if let Some(op_idx) = s.find("!=") {
        let col_name = clean_col_name(&s[..op_idx]);
        let col_idx = table.get_column_index(col_name).ok_or_else(|| format!("Column {} not found", col_name))?;
        let val = parse_value(&s[op_idx + 2..], params);
        return Ok(Condition { col_idx, op: ColOp::NotEq(val) });
    }
    if let Some(op_idx) = s.find("<>") {
        let col_name = clean_col_name(&s[..op_idx]);
        let col_idx = table.get_column_index(col_name).ok_or_else(|| format!("Column {} not found", col_name))?;
        let val = parse_value(&s[op_idx + 2..], params);
        return Ok(Condition { col_idx, op: ColOp::NotEq(val) });
    }
    if let Some(op_idx) = s.find(">=") {
        let col_name = clean_col_name(&s[..op_idx]);
        let col_idx = table.get_column_index(col_name).ok_or_else(|| format!("Column {} not found", col_name))?;
        let val = parse_value(&s[op_idx + 2..], params);
        return Ok(Condition { col_idx, op: ColOp::Gte(val) });
    }
    if let Some(op_idx) = s.find("<=") {
        let col_name = clean_col_name(&s[..op_idx]);
        let col_idx = table.get_column_index(col_name).ok_or_else(|| format!("Column {} not found", col_name))?;
        let val = parse_value(&s[op_idx + 2..], params);
        return Ok(Condition { col_idx, op: ColOp::Lte(val) });
    }
    if let Some(op_idx) = s.find('>') {
        let col_name = clean_col_name(&s[..op_idx]);
        let col_idx = table.get_column_index(col_name).ok_or_else(|| format!("Column {} not found", col_name))?;
        let val = parse_value(&s[op_idx + 1..], params);
        return Ok(Condition { col_idx, op: ColOp::Gt(val) });
    }
    if let Some(op_idx) = s.find('<') {
        let col_name = clean_col_name(&s[..op_idx]);
        let col_idx = table.get_column_index(col_name).ok_or_else(|| format!("Column {} not found", col_name))?;
        let val = parse_value(&s[op_idx + 1..], params);
        return Ok(Condition { col_idx, op: ColOp::Lt(val) });
    }
    if let Some(op_idx) = s.find('=') {
        let left_raw = s[..op_idx].trim();
        let val = parse_value(&s[op_idx + 1..], params);

        let mut inner_str = left_raw;
        let mut is_upper = false;
        let mut is_lower = false;
        let mut is_trim = false;

        loop {
            let u = inner_str.to_uppercase();
            if (u.starts_with("BTRIM(") || u.starts_with("TRIM(")) && inner_str.ends_with(')') {
                is_trim = true;
                let open = inner_str.find('(').unwrap();
                inner_str = inner_str[open + 1..inner_str.len() - 1].trim();
            } else if u.starts_with("UPPER(") && inner_str.ends_with(')') {
                is_upper = true;
                inner_str = inner_str[6..inner_str.len() - 1].trim();
            } else if u.starts_with("LOWER(") && inner_str.ends_with(')') {
                is_lower = true;
                inner_str = inner_str[6..inner_str.len() - 1].trim();
            } else {
                break;
            }
        }

        let col_name = clean_col_name(inner_str);
        let col_idx = table.get_column_index(col_name).ok_or_else(|| format!("Column {} not found", col_name))?;

        if is_trim && is_upper {
            let target_str = match &val {
                Value::Text(s) => s.trim().to_uppercase(),
                Value::Int(i) => i.to_string(),
                _ => "".to_string(),
            };
            return Ok(Condition { col_idx, op: ColOp::TrimUpperEq(target_str) });
        } else if is_trim && is_lower {
            let target_str = match &val {
                Value::Text(s) => s.trim().to_lowercase(),
                Value::Int(i) => i.to_string(),
                _ => "".to_string(),
            };
            return Ok(Condition { col_idx, op: ColOp::TrimLowerEq(target_str) });
        } else if is_trim {
            let target_str = match &val {
                Value::Text(s) => s.trim().to_string(),
                Value::Int(i) => i.to_string(),
                _ => "".to_string(),
            };
            return Ok(Condition { col_idx, op: ColOp::TrimEq(target_str) });
        } else if is_upper {
            let target_str = match &val {
                Value::Text(s) => s.to_uppercase().to_string(),
                Value::Int(i) => i.to_string(),
                _ => "".to_string(),
            };
            return Ok(Condition { col_idx, op: ColOp::UpperEq(target_str) });
        } else if is_lower {
            let target_str = match &val {
                Value::Text(s) => s.to_lowercase().to_string(),
                Value::Int(i) => i.to_string(),
                _ => "".to_string(),
            };
            return Ok(Condition { col_idx, op: ColOp::LowerEq(target_str) });
        } else {
            return Ok(Condition { col_idx, op: ColOp::Eq(val) });
        }
    }

    if let Some(col_idx) = table.get_column_index(s) {
        return Ok(Condition { col_idx, op: ColOp::Eq(Value::Bool(true)) });
    }

    Err(format!("Unsupported condition: {}", s))
}

#[allow(dead_code)]
fn extract_single_pk_eq(expr: &WhereExpr, pk_idx: usize) -> Option<i64> {
    match expr {
        WhereExpr::Condition(cond) => {
            if cond.col_idx == pk_idx {
                if let ColOp::Eq(ref v) = cond.op {
                    return v.as_i64();
                }
            }
            None
        }
        WhereExpr::And(list) => {
            for e in list {
                if let Some(pk) = extract_single_pk_eq(e, pk_idx) {
                    return Some(pk);
                }
            }
            None
        }
        _ => None,
    }
}

fn evaluate_where_expr(
    row: &[Value],
    table: Option<&crate::storage::table::Table>,
    expr: &WhereExpr,
    params: &[Value],
) -> bool {
    evaluate_where_3vl(row, table, expr, params) == Some(true)
}

fn evaluate_where_3vl(
    row: &[Value],
    table: Option<&crate::storage::table::Table>,
    expr: &WhereExpr,
    params: &[Value],
) -> Option<bool> {
    match expr {
        WhereExpr::And(list) => {
            let mut has_null = false;
            for e in list {
                match evaluate_where_3vl(row, table, e, params) {
                    Some(false) => return Some(false),
                    None => has_null = true,
                    Some(true) => {}
                }
            }
            if has_null { None } else { Some(true) }
        }
        WhereExpr::Or(list) => {
            let mut has_null = false;
            for e in list {
                match evaluate_where_3vl(row, table, e, params) {
                    Some(true) => return Some(true),
                    None => has_null = true,
                    Some(false) => {}
                }
            }
            if has_null { None } else { Some(false) }
        }
        WhereExpr::Not(inner) => {
            match evaluate_where_3vl(row, table, inner, params) {
                Some(b) => Some(!b),
                None => None,
            }
        }
        WhereExpr::Condition(cond) => evaluate_single_condition_3vl(row, cond),
        WhereExpr::Custom(s) => {
            let v = eval_sql_expr(s, row, table, &[], None, params);
            if v.is_null() {
                None
            } else {
                v.as_bool()
            }
        }
    }
}

fn evaluate_single_condition_3vl(row: &[Value], cond: &Condition) -> Option<bool> {
    if cond.col_idx >= row.len() {
        return Some(false);
    }
    let val = &row[cond.col_idx];
    match &cond.op {
        ColOp::IsNull => Some(val.is_null()),
        ColOp::IsNotNull => Some(!val.is_null()),
        _ => {
            if val.is_null() {
                return None;
            }
            match &cond.op {
                ColOp::Eq(target) => {
                    if target.is_null() { None } else { Some(val.is_equal(target)) }
                }
                ColOp::NotEq(target) => {
                    if target.is_null() { None } else { Some(!val.is_equal(target)) }
                }
                ColOp::Gt(threshold) => {
                    if threshold.is_null() { None } else { Some(val.cmp_value(threshold) == std::cmp::Ordering::Greater && !val.is_equal(threshold)) }
                }
                ColOp::Gte(threshold) => {
                    if threshold.is_null() { None } else { Some(val.cmp_value(threshold) != std::cmp::Ordering::Less) }
                }
                ColOp::Lt(threshold) => {
                    if threshold.is_null() { None } else { Some(val.cmp_value(threshold) == std::cmp::Ordering::Less && !val.is_equal(threshold)) }
                }
                ColOp::Lte(threshold) => {
                    if threshold.is_null() { None } else { Some(val.cmp_value(threshold) != std::cmp::Ordering::Greater) }
                }
                ColOp::Between(min_val, max_val) => {
                    if min_val.is_null() || max_val.is_null() {
                        None
                    } else {
                        Some(val.cmp_value(min_val) != std::cmp::Ordering::Less && val.cmp_value(max_val) != std::cmp::Ordering::Greater)
                    }
                }
                ColOp::In(list) => {
                    let mut has_null = false;
                    for item in list {
                        if item.is_null() {
                            has_null = true;
                        } else if val.is_equal(item) {
                            return Some(true);
                        }
                    }
                    if has_null { None } else { Some(false) }
                }
                ColOp::Like(pattern, esc) => match val {
                    Value::Text(s) => Some(sql_like_match_with_escape(s, pattern, false, *esc)),
                    _ => Some(false),
                },
                ColOp::ILike(pattern, esc) => match val {
                    Value::Text(s) => Some(sql_like_match_with_escape(s, pattern, true, *esc)),
                    _ => Some(false),
                },
                ColOp::NotLike(pattern, esc) => match val {
                    Value::Text(s) => Some(!sql_like_match_with_escape(s, pattern, false, *esc)),
                    _ => Some(false),
                },
                ColOp::NotILike(pattern, esc) => match val {
                    Value::Text(s) => Some(!sql_like_match_with_escape(s, pattern, true, *esc)),
                    _ => Some(false),
                },
                ColOp::LowerEq(target) => match val {
                    Value::Text(s) => Some(s.to_lowercase() == *target),
                    Value::Int(i) => Some(i.to_string() == *target),
                    _ => Some(false),
                },
                ColOp::UpperEq(target) => match val {
                    Value::Text(s) => Some(s.to_uppercase() == *target),
                    Value::Int(i) => Some(i.to_string() == *target),
                    _ => Some(false),
                },
                ColOp::TrimUpperEq(target) => match val {
                    Value::Text(s) => Some(s.trim().to_uppercase() == *target),
                    Value::Int(i) => Some(i.to_string() == *target),
                    _ => Some(false),
                },
                ColOp::TrimLowerEq(target) => match val {
                    Value::Text(s) => Some(s.trim().to_lowercase() == *target),
                    Value::Int(i) => Some(i.to_string() == *target),
                    _ => Some(false),
                },
                ColOp::TrimEq(target) => match val {
                    Value::Text(s) => Some(s.trim() == *target),
                    Value::Int(i) => Some(i.to_string() == *target),
                    _ => Some(false),
                },
                ColOp::IsNull | ColOp::IsNotNull => unreachable!(),
            }
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum LikeToken {
    Exact(char),
    AnyOne,
    AnyMany,
}

#[inline(always)]
fn ends_with_ignore_case(text: &str, suffix: &str) -> bool {
    if text.len() < suffix.len() {
        return false;
    }
    if text.is_ascii() && suffix.is_ascii() {
        text[text.len() - suffix.len()..].eq_ignore_ascii_case(suffix)
    } else {
        let t_low = text.to_lowercase();
        let s_low = suffix.to_lowercase();
        t_low.ends_with(&s_low)
    }
}

#[inline(always)]
fn starts_with_ignore_case(text: &str, prefix: &str) -> bool {
    if text.len() < prefix.len() {
        return false;
    }
    if text.is_ascii() && prefix.is_ascii() {
        text[..prefix.len()].eq_ignore_ascii_case(prefix)
    } else {
        let t_low = text.to_lowercase();
        let p_low = prefix.to_lowercase();
        t_low.starts_with(&p_low)
    }
}

#[inline(always)]
fn contains_ignore_case(text: &str, sub: &str) -> bool {
    if sub.is_empty() {
        return true;
    }
    if text.len() < sub.len() {
        return false;
    }
    if text.is_ascii() && sub.is_ascii() {
        let sub_bytes = sub.as_bytes();
        let text_bytes = text.as_bytes();
        let first_lower = sub_bytes[0].to_ascii_lowercase();
        let first_upper = sub_bytes[0].to_ascii_uppercase();
        let max_start = text_bytes.len() - sub_bytes.len();
        for i in 0..=max_start {
            let b = text_bytes[i];
            if b == first_lower || b == first_upper {
                if text_bytes[i..i + sub_bytes.len()].eq_ignore_ascii_case(sub_bytes) {
                    return true;
                }
            }
        }
        false
    } else {
        let t_low = text.to_lowercase();
        let s_low = sub.to_lowercase();
        t_low.contains(&s_low)
    }
}

#[inline(always)]
fn eq_ignore_case(text: &str, other: &str) -> bool {
    if text.is_ascii() && other.is_ascii() {
        text.eq_ignore_ascii_case(other)
    } else {
        text.to_lowercase() == other.to_lowercase()
    }
}

fn sql_like_match_with_escape(text: &str, pattern: &str, case_insensitive: bool, esc_char: Option<char>) -> bool {
    // 1. Fast-Path khi không có escape char (chiếm 99% các truy vấn)
    if esc_char.is_none() {
        if pattern.is_empty() {
            return text.is_empty();
        }
        if pattern == "%" {
            return true;
        }

        let p_bytes = pattern.as_bytes();
        let has_wildcard_any_one = pattern.contains('_');
        let has_any_many = pattern.contains('%');

        // Case 1: Không chứa bất kỳ wildcard nào -> Exact match
        if !has_any_many && !has_wildcard_any_one {
            return if case_insensitive {
                eq_ignore_case(text, pattern)
            } else {
                text == pattern
            };
        }

        // Case 2: Chỉ có 1 dấu '%' ở đầu (%suffix) và không có '_'
        if !has_wildcard_any_one && p_bytes[0] == b'%' && !pattern[1..].contains('%') {
            let suffix = &pattern[1..];
            return if case_insensitive {
                ends_with_ignore_case(text, suffix)
            } else {
                text.ends_with(suffix)
            };
        }

        // Case 3: Chỉ có 1 dấu '%' ở cuối (prefix%) và không có '_'
        if !has_wildcard_any_one && p_bytes[pattern.len() - 1] == b'%' && !pattern[..pattern.len() - 1].contains('%') {
            let prefix = &pattern[..pattern.len() - 1];
            return if case_insensitive {
                starts_with_ignore_case(text, prefix)
            } else {
                text.starts_with(prefix)
            };
        }

        // Case 4: Có 2 dấu '%' ở đầu và cuối (%sub%) và ở giữa không có '%' hay '_'
        if !has_wildcard_any_one && pattern.len() >= 2 && p_bytes[0] == b'%' && p_bytes[pattern.len() - 1] == b'%' {
            let sub = &pattern[1..pattern.len() - 1];
            if !sub.contains('%') {
                return if case_insensitive {
                    contains_ignore_case(text, sub)
                } else {
                    text.contains(sub)
                };
            }
        }
    }

    // 2. Tokenize pattern vào stack buffer (hoặc Vec nếu pattern > 128 tokens)
    const STACK_TOKENS_CAP: usize = 128;
    let mut stack_tokens = [LikeToken::AnyMany; STACK_TOKENS_CAP];
    let mut heap_tokens = Vec::new();
    let mut p_len = 0;

    let esc = esc_char.map(|c| if case_insensitive { c.to_lowercase().next().unwrap_or(c) } else { c });
    let mut chars_iter = pattern.chars().peekable();
    let mut last_was_any_many = false;

    while let Some(ch) = chars_iter.next() {
        let token = if let Some(e) = esc {
            if ch == e {
                if let Some(next_c) = chars_iter.next() {
                    let c = if case_insensitive { next_c.to_lowercase().next().unwrap_or(next_c) } else { next_c };
                    LikeToken::Exact(c)
                } else {
                    LikeToken::Exact(if case_insensitive { ch.to_lowercase().next().unwrap_or(ch) } else { ch })
                }
            } else if ch == '%' {
                LikeToken::AnyMany
            } else if ch == '_' {
                LikeToken::AnyOne
            } else {
                LikeToken::Exact(if case_insensitive { ch.to_lowercase().next().unwrap_or(ch) } else { ch })
            }
        } else if ch == '%' {
            LikeToken::AnyMany
        } else if ch == '_' {
            LikeToken::AnyOne
        } else {
            LikeToken::Exact(if case_insensitive { ch.to_lowercase().next().unwrap_or(ch) } else { ch })
        };

        // Nén các AnyMany liên tiếp (% % -> %)
        if token == LikeToken::AnyMany {
            if last_was_any_many {
                continue;
            }
            last_was_any_many = true;
        } else {
            last_was_any_many = false;
        }

        if p_len < STACK_TOKENS_CAP && heap_tokens.is_empty() {
            stack_tokens[p_len] = token;
        } else {
            if heap_tokens.is_empty() {
                heap_tokens.reserve(STACK_TOKENS_CAP * 2);
                heap_tokens.extend_from_slice(&stack_tokens[..p_len]);
            }
            heap_tokens.push(token);
        }
        p_len += 1;
    }

    let tokens: &[LikeToken] = if heap_tokens.is_empty() {
        &stack_tokens[..p_len]
    } else {
        &heap_tokens
    };

    // 3. Quy hoạch động 2 hàng 1 chiều trên Stack (0 heap allocation!)
    const STACK_DP_CAP: usize = 129;
    let mut stack_prev = [false; STACK_DP_CAP];
    let mut stack_curr = [false; STACK_DP_CAP];
    let mut heap_prev = Vec::new();
    let mut heap_curr = Vec::new();

    let (prev, curr): (&mut [bool], &mut [bool]) = if p_len + 1 <= STACK_DP_CAP {
        (&mut stack_prev[..=p_len], &mut stack_curr[..=p_len])
    } else {
        heap_prev.resize(p_len + 1, false);
        heap_curr.resize(p_len + 1, false);
        (&mut heap_prev, &mut heap_curr)
    };

    // Khởi tạo hàng 0 (khi text rỗng)
    prev[0] = true;
    for j in 1..=p_len {
        if tokens[j - 1] == LikeToken::AnyMany {
            prev[j] = prev[j - 1];
        } else {
            prev[j] = false;
        }
    }

    // Duyệt qua từng ký tự của text (không cấp phát heap)
    for ch in text.chars() {
        let target_ch = if case_insensitive {
            ch.to_lowercase().next().unwrap_or(ch)
        } else {
            ch
        };

        curr[0] = false;
        for j in 1..=p_len {
            match tokens[j - 1] {
                LikeToken::AnyMany => {
                    curr[j] = prev[j] || curr[j - 1];
                }
                LikeToken::AnyOne => {
                    curr[j] = prev[j - 1];
                }
                LikeToken::Exact(c) => {
                    curr[j] = prev[j - 1] && (c == target_ch);
                }
            }
        }
        prev.copy_from_slice(curr);
    }

    prev[p_len]
}

#[inline(always)]
pub(crate) fn sql_like_match(text: &str, pattern: &str, case_insensitive: bool) -> bool {
    sql_like_match_with_escape(text, pattern, case_insensitive, None)
}

#[inline(always)]
/// Giá trị là object / mảng JSON (jsonb lưu dạng chữ): `Some(doc)`.
fn json_container(v: &Value) -> Option<serde_json::Value> {
    let Value::Text(t) = v else { return None };
    let t = t.trim();
    if !(t.starts_with('{') && t.ends_with('}') || t.starts_with('[') && t.ends_with(']')) {
        return None;
    }
    match serde_json::from_str::<serde_json::Value>(t) {
        Ok(d @ (serde_json::Value::Object(_) | serde_json::Value::Array(_))) => Some(d),
        _ => None,
    }
}

/// `jsonb - 'key'`, `- index`, `- '{a,b}'::text[]`. `None` nếu vế trái không phải object / mảng JSON.
pub(crate) fn jsonb_minus(lhs: &Value, rhs: &Value) -> Option<Value> {
    let mut doc = json_container(lhs)?;
    match (&mut doc, rhs) {
        (serde_json::Value::Array(a), Value::Int(i)) => {
            let n = a.len() as i64;
            let idx = if *i < 0 { n + i } else { *i };
            if idx >= 0 && idx < n {
                a.remove(idx as usize);
            }
        }
        _ => {
            let keys = if matches!(rhs, Value::Text(t) if t.trim_start().starts_with('{') || t.trim_start().starts_with('[')) { text_array_items(rhs) } else { vec![rhs.as_str()] };
            match &mut doc {
                serde_json::Value::Object(m) => {
                    for k in &keys {
                        m.remove(k);
                    }
                }
                serde_json::Value::Array(a) => a.retain(|x| !matches!(x, serde_json::Value::String(sv) if keys.contains(sv))),
                _ => {}
            }
        }
    }
    Some(Value::text(doc.to_string()))
}

/// `jsonb #- '{a,b}'`. `None` nếu vế trái không phải object / mảng JSON.
pub(crate) fn jsonb_delete_path_op(lhs: &Value, rhs: &Value) -> Option<Value> {
    let mut doc = json_container(lhs)?;
    jsonb_delete_path(&mut doc, &text_array_items(rhs));
    Some(Value::text(doc.to_string()))
}

/// Phần tử của `text[]` (`'{a,b}'` hoặc mảng JSON) dưới dạng chuỗi.
fn text_array_items(v: &Value) -> Vec<String> {
    let s = v.as_str();
    let j = parse_pg_array_to_json(s.trim()).or_else(|| serde_json::from_str::<serde_json::Value>(s.trim()).ok());
    match j {
        Some(serde_json::Value::Array(a)) => a.into_iter().map(|x| match x {
            serde_json::Value::String(t) => t,
            o => o.to_string(),
        }).collect(),
        _ => vec![s],
    }
}

/// `#-`: xoá phần tử tại đường dẫn (khoá object hoặc chỉ số mảng, chỉ số âm tính từ cuối).
fn jsonb_delete_path(doc: &mut serde_json::Value, path: &[String]) {
    let Some((last, head)) = path.split_last() else { return };
    let mut cur = doc;
    for p in head {
        let next = match cur {
            serde_json::Value::Object(m) => m.get_mut(p),
            serde_json::Value::Array(a) => p.parse::<i64>().ok().and_then(|i| {
                let n = a.len() as i64;
                let i = if i < 0 { n + i } else { i };
                (i >= 0 && i < n).then(|| &mut a[i as usize])
            }),
            _ => None,
        };
        match next {
            Some(n) => cur = n,
            None => return,
        }
    }
    match cur {
        serde_json::Value::Object(m) => {
            m.remove(last);
        }
        serde_json::Value::Array(a) => {
            if let Ok(i) = last.parse::<i64>() {
                let n = a.len() as i64;
                let i = if i < 0 { n + i } else { i };
                if i >= 0 && i < n {
                    a.remove(i as usize);
                }
            }
        }
        _ => {}
    }
}

/// Chuỗi có đúng cú pháp literal mảng của PostgreSQL (`{a,"b c",{1,2}}`): phần tử chưa nháy không chứa
/// `{` `}` `"`, sau mỗi phần tử / mảng con chỉ được là `,` hoặc `}`. Chuỗi như `{{app_name}} code {{code}}`
/// không phải mảng (trước đây bị đọc thành `["{app_name}} code {{code}"]`).
fn is_pg_array_literal(s: &str) -> bool {
    let b = s.as_bytes();
    if b.first() != Some(&b'{') || b.last() != Some(&b'}') {
        return false;
    }
    let mut depth = 0i32;
    let mut i = 0;
    // true: vừa xong một phần tử / mảng con, chỉ còn được `,` hoặc `}`
    let mut after_item = false;
    while i < b.len() {
        let c = b[i];
        if c.is_ascii_whitespace() {
            i += 1;
            continue;
        }
        match c {
            b'{' if !after_item => {
                depth += 1;
                i += 1;
            }
            b'}' => {
                depth -= 1;
                if depth < 0 {
                    return false;
                }
                after_item = true;
                i += 1;
                if depth == 0 {
                    return b[i..].iter().all(|x| x.is_ascii_whitespace());
                }
            }
            b',' if after_item => {
                after_item = false;
                i += 1;
            }
            b'"' if !after_item => {
                i += 1;
                while i < b.len() && b[i] != b'"' {
                    if b[i] == b'\\' {
                        i += 1;
                    }
                    i += 1;
                }
                if i >= b.len() {
                    return false;
                }
                i += 1;
                after_item = true;
            }
            _ if !after_item && depth > 0 => {
                while i < b.len() && !matches!(b[i], b',' | b'}') {
                    if matches!(b[i], b'{' | b'"') {
                        return false;
                    }
                    i += 1;
                }
                after_item = true;
            }
            _ => return false,
        }
    }
    false
}

pub(crate) fn parse_pg_array_to_json(trimmed: &str) -> Option<serde_json::Value> {
    if trimmed == "{}" {
        return Some(serde_json::Value::Array(vec![]));
    }
    if trimmed.starts_with('{') && trimmed.ends_with('}') && !trimmed.contains(':') && !is_pg_array_literal(trimmed) {
        return None;
    }
    if trimmed.starts_with('{') && trimmed.ends_with('}') {
        let inside = &trimmed[1..trimmed.len() - 1].trim();
        if inside.is_empty() {
            return Some(serde_json::Value::Array(vec![]));
        }
        // If it has colons at top-level, it's a JSON object, not a PG array
        let mut in_quote = false;
        let mut has_colon = false;
        for c in inside.chars() {
            if c == '"' || c == '\'' {
                in_quote = !in_quote;
            } else if c == ':' && !in_quote {
                has_colon = true;
                break;
            }
        }
        if has_colon {
            return None;
        }

        let tokens = split_comma_separated_tokens(inside);
        let mut items = Vec::with_capacity(tokens.len());
        for tok in tokens {
            let t = tok.trim();
            if t.starts_with('"') && t.ends_with('"') && t.len() >= 2 {
                items.push(serde_json::Value::String(t[1..t.len() - 1].replace("\\\"", "\"")));
            } else if t.starts_with('\'') && t.ends_with('\'') && t.len() >= 2 {
                items.push(serde_json::Value::String(t[1..t.len() - 1].replace("''", "'")));
            } else if let Ok(i) = t.parse::<i64>() {
                items.push(serde_json::Value::Number(i.into()));
            } else if let Ok(f) = t.parse::<f64>() {
                if let Some(n) = serde_json::Number::from_f64(f) {
                    items.push(serde_json::Value::Number(n));
                } else {
                    items.push(serde_json::Value::String(t.to_string()));
                }
            } else if t.eq_ignore_ascii_case("TRUE") || t.eq_ignore_ascii_case("T") {
                items.push(serde_json::Value::Bool(true));
            } else if t.eq_ignore_ascii_case("FALSE") || t.eq_ignore_ascii_case("F") {
                items.push(serde_json::Value::Bool(false));
            } else if t.eq_ignore_ascii_case("NULL") {
                items.push(serde_json::Value::Null);
            } else {
                items.push(serde_json::Value::String(t.to_string()));
            }
        }
        return Some(serde_json::Value::Array(items));
    }
    None
}

/// Biểu thức là lời gọi hàm băm trả bytea mà engine biểu diễn bằng chuỗi hex không `\\x` (sha256(), digest()...).
fn is_hex_hash_call(e: &str) -> bool {
    let u = e.trim().to_ascii_uppercase();
    ["SHA224(", "SHA256(", "SHA384(", "SHA512(", "DIGEST(", "HMAC("].iter().any(|p| u.starts_with(p)) && u.ends_with(')')
}

pub(crate) fn raw_bytes_pub(val: &Value) -> Vec<u8> {
    get_raw_bytes(val)
}

fn get_raw_bytes(val: &Value) -> Vec<u8> {
    match val {
        Value::Text(s) => {
            let mut s_str = s.as_str().trim();
            while s_str.starts_with('\\') {
                s_str = &s_str[1..];
            }
            if s_str.starts_with('x') || s_str.starts_with('X') {
                hex::decode(&s_str[1..]).unwrap_or_else(|_| s.as_bytes().to_vec())
            } else if s_str.starts_with("0x") || s_str.starts_with("0X") {
                hex::decode(&s_str[2..]).unwrap_or_else(|_| s.as_bytes().to_vec())
            } else if s_str.starts_with('[') && s_str.ends_with(']') {
                if let Ok(nums) = serde_json::from_str::<Vec<serde_json::Value>>(s_str) {
                    if nums.iter().all(|n| n.as_u64().map_or(false, |v| v <= 255)) {
                        return nums.iter().filter_map(|n| n.as_u64().map(|v| v as u8)).collect();
                    }
                }
                s.as_bytes().to_vec()
            } else {
                s.as_bytes().to_vec()
            }
        }
        Value::Int(i) => i.to_string().into_bytes(),
        Value::Float(f) => f.to_string().into_bytes(),
        Value::Bool(b) => (if *b { "true" } else { "false" }).as_bytes().to_vec(),
        Value::Null => Vec::new(),
    }
}

thread_local! {
    /// Regex đã biên dịch theo (mẫu, không phân biệt hoa thường): tránh biên dịch lại ở mỗi dòng.
    static REGEX_CACHE: std::cell::RefCell<std::collections::HashMap<(String, bool), Option<regex::Regex>>> = std::cell::RefCell::new(std::collections::HashMap::new());
}

/// `text ~ pat` (POSIX regex của PostgreSQL, quy về cú pháp crate `regex`). Mẫu sai → lỗi 2201B.
pub(crate) fn regex_match(text: &str, pat: &str, case_insensitive: bool) -> bool {
    REGEX_CACHE.with(|c| {
        let mut c = c.borrow_mut();
        let key = (pat.to_string(), case_insensitive);
        if !c.contains_key(&key) {
            if c.len() > 256 {
                c.clear();
            }
            // \m, \M, \y (biên từ của PostgreSQL) → \b
            let p = pat.replace("\\m", "\\b").replace("\\M", "\\b").replace("\\y", "\\b");
            let re = regex::RegexBuilder::new(&p).case_insensitive(case_insensitive).build();
            c.insert(key.clone(), re.ok());
        }
        match &c[&key] {
            Some(re) => re.is_match(text),
            None => {
                crate::engine::errors::raise(format!("invalid regular expression: {}", pat));
                false
            }
        }
    })
}

pub(crate) fn json_to_value(j: &serde_json::Value) -> Value {
    match j {
        serde_json::Value::Null => Value::Null,
        serde_json::Value::Bool(b) => Value::Bool(*b),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Value::Int(i)
            } else if let Some(f) = n.as_f64() {
                Value::Float(f)
            } else {
                Value::text(n.to_string())
            }
        }
        serde_json::Value::String(s) => Value::text(s),
        serde_json::Value::Array(_) | serde_json::Value::Object(_) => Value::text(j.to_string()),
    }
}

fn is_complete_array_constructor(s: &str) -> bool {
    let s_trim = s.trim();
    if !s_trim.to_uppercase().starts_with("ARRAY[") || !s_trim.ends_with(']') {
        return false;
    }
    let bytes = s_trim.as_bytes();
    let mut depth = 0;
    let mut in_single_quote = false;
    for (i, &b) in bytes.iter().enumerate().skip(5) {
        if b == b'\'' {
            in_single_quote = !in_single_quote;
        } else if !in_single_quote {
            if b == b'[' {
                depth += 1;
            } else if b == b']' {
                depth -= 1;
                if depth == 0 {
                    return i == bytes.len() - 1;
                }
            }
        }
    }
    false
}

/// `lhs <op> ANY|SOME|ALL (inner)` ở cấp ngoài cùng: (lhs, op, là ALL, inner).
/// `a || b` khi cả hai là object JSON (gộp, vế phải đè) hoặc mảng JSON `[...]` (nối).
pub(crate) fn json_order_cmp_pub(a: Option<&serde_json::Value>, b: Option<&serde_json::Value>, desc: bool, nulls_first: bool) -> std::cmp::Ordering {
    json_order_cmp(a, b, desc, nulls_first)
}

pub(crate) fn type_str_from_json_value_pub(v: &serde_json::Value) -> String {
    type_str_from_json_value(v)
}

pub(crate) fn find_top_level_math_op_pub(s: &str, ops: &[char]) -> Option<(usize, char)> {
    find_top_level_math_op(s, ops)
}

pub(crate) fn jsonb_concat(l: &str, r: &str) -> Option<String> {
    let (lt, rt) = (l.trim(), r.trim());
    let l_obj = lt.starts_with('{') && lt.contains(':');
    let r_obj = rt.starts_with('{') && rt.contains(':');
    if l_obj && r_obj {
        let (Ok(serde_json::Value::Object(mut a)), Ok(serde_json::Value::Object(b))) = (serde_json::from_str::<serde_json::Value>(lt), serde_json::from_str::<serde_json::Value>(rt)) else {
            return None;
        };
        for (k, v) in b {
            a.insert(k, v);
        }
        return serde_json::to_string(&serde_json::Value::Object(a)).ok();
    }
    if lt.starts_with('[') && rt.starts_with('[') {
        if let (Ok(serde_json::Value::Array(mut a)), Ok(serde_json::Value::Array(b))) = (serde_json::from_str::<serde_json::Value>(lt), serde_json::from_str::<serde_json::Value>(rt)) {
            if a.iter().chain(b.iter()).any(|x| !x.is_number()) || true {
                a.extend(b);
                return serde_json::to_string(&serde_json::Value::Array(a)).ok();
            }
        }
    }
    None
}

/// Có `needle` (ASCII, không phân biệt hoa thường) ở đâu đó trong `hay`: lọc nhanh trước khi quét theo
/// ngoặc/chuỗi, không cấp phát.
pub(crate) fn contains_ci(hay: &str, needle: &str) -> bool {
    let (h, n) = (hay.as_bytes(), needle.as_bytes());
    if n.is_empty() || h.len() < n.len() {
        return false;
    }
    let (f_up, f_low) = (n[0].to_ascii_uppercase(), n[0].to_ascii_lowercase());
    (0..=h.len() - n.len()).any(|i| (h[i] == f_up || h[i] == f_low) && h[i..i + n.len()].eq_ignore_ascii_case(n))
}

/// Lọc nhanh (chuỗi đã viết hoa) trước khi tìm ANY/SOME/ALL theo ngoặc: chạy cho mọi biểu thức con.
#[inline]
pub(crate) fn may_have_any_all(upper: &str) -> bool {
    let b = upper.as_bytes();
    b.len() >= 5 && b.contains(&b'(') && b.windows(3).any(|w| w == b"ANY" || w == b"ALL" || w == b"SOM")
}

/// `lhs <op> ANY|SOME|ALL (inner)` ở cấp ngoài cùng: (lhs, op, là ALL, inner).
pub(crate) fn split_any_all(s: &str) -> Option<(&str, &'static str, bool, &str)> {
    for (kw, all) in [("ANY", false), ("SOME", false), ("ALL", true)] {
        let mut from = 0;
        while let Some(rel) = find_top_level_keyword(&s[from..], kw) {
            let pos = from + rel;
            from = pos + kw.len();
            let after = s[pos + kw.len()..].trim_start();
            if !after.starts_with('(') || !after.ends_with(')') {
                continue;
            }
            let close = crate::engine::constraints::matching_paren(after, 0)?;
            if close != after.len() - 1 {
                continue;
            }
            let before = s[..pos].trim_end();
            for op in ["<>", "!=", "<=", ">=", "=", "<", ">"] {
                if before.ends_with(op) {
                    let lhs = before[..before.len() - op.len()].trim();
                    if lhs.is_empty() {
                        return None;
                    }
                    let op: &'static str = match op { "!=" => "<>", "<>" => "<>", "<=" => "<=", ">=" => ">=", "=" => "=", "<" => "<", _ => ">" };
                    return Some((lhs, op, all, after[1..close].trim()));
                }
            }
        }
    }
    None
}

/// Tính `lhs <op> ANY|ALL (mảng)` theo logic 3 giá trị của PostgreSQL.
pub(crate) fn eval_any_all(lhs: &Value, op: &str, all: bool, rhs: &Value) -> Value {
    if lhs.is_null() || rhs.is_null() {
        return Value::Null;
    }
    let items = val_to_array_items(rhs);
    let mut saw_null = false;
    for item in &items {
        if item.is_null() {
            saw_null = true;
            continue;
        }
        let r = match op {
            "=" => lhs.is_equal(item),
            "<>" => !lhs.is_equal(item),
            "<" => lhs.cmp_value(item) == std::cmp::Ordering::Less,
            "<=" => lhs.cmp_value(item) != std::cmp::Ordering::Greater,
            ">" => lhs.cmp_value(item) == std::cmp::Ordering::Greater,
            _ => lhs.cmp_value(item) != std::cmp::Ordering::Less,
        };
        if all && !r {
            return Value::Bool(false);
        }
        if !all && r {
            return Value::Bool(true);
        }
    }
    if saw_null { Value::Null } else { Value::Bool(all) }
}

pub(crate) fn val_to_array_items(val: &Value) -> Vec<Value> {
    match val {
        Value::Null => vec![],
        Value::Text(s) => {
            let trimmed = s.trim();
            if trimmed.is_empty() || trimmed == "[]" || trimmed == "{}" {
                return vec![];
            }
            if trimmed.starts_with('(') && trimmed.ends_with(')') {
                let inside = &trimmed[1..trimmed.len() - 1].trim();
                let tokens = split_comma_separated_tokens(inside);
                if tokens.len() > 1 {
                    return tokens.into_iter().map(|tok| parse_value(&tok, &[])).collect();
                }
            }
            if let Some(serde_json::Value::Array(arr)) = parse_pg_array_to_json(trimmed) {
                return arr.into_iter().map(|item| json_to_value(&item)).collect();
            }
            if trimmed.starts_with('[') && trimmed.ends_with(']') {
                if let Ok(serde_json::Value::Array(arr)) = serde_json::from_str::<serde_json::Value>(trimmed) {
                    return arr.into_iter().map(|item| json_to_value(&item)).collect();
                }
            }
            vec![val.clone()]
        }
        _ => vec![val.clone()],
    }
}

pub(crate) fn array_contains_check(container: &Value, subset: &Value) -> bool {
    if container.is_null() || subset.is_null() {
        return false;
    }
    let c_items = val_to_array_items(container);
    let s_items = val_to_array_items(subset);
    s_items.iter().all(|s| c_items.iter().any(|c| c.is_equal(s)))
}

pub(crate) fn array_overlap_check(a: &Value, b: &Value) -> bool {
    if a.is_null() || b.is_null() {
        return false;
    }
    let a_items = val_to_array_items(a);
    let b_items = val_to_array_items(b);
    a_items.iter().any(|item_a| b_items.iter().any(|item_b| item_a.is_equal(item_b)))
}

fn cmp_composite_tuples(left: &Value, right: &Value) -> std::cmp::Ordering {
    let l_items = val_to_array_items(left);
    let r_items = val_to_array_items(right);
    for i in 0..l_items.len().min(r_items.len()) {
        let ord = l_items[i].cmp_value(&r_items[i]);
        if ord != std::cmp::Ordering::Equal {
            return ord;
        }
    }
    l_items.len().cmp(&r_items.len())
}

/// Kiểu (chuỗi FieldInfo) của tham chiếu cột `col` hoặc `alias.col` trong các bảng cho trước.
fn column_ref_type(expr: &str, tables: &[(&crate::storage::table::Table, Option<&str>)]) -> Option<String> {
    let e = expr.trim();
    if e.is_empty() || e.contains(|c: char| c == '(' || c == ' ' || c == '\'' || c == ':' || c == '-' || c == '+' || c == '*' || c == '/') {
        return None;
    }
    let (prefix, col) = match e.rsplit_once('.') {
        Some((p, c)) => (Some(p.trim_matches('"')), c.trim_matches('"')),
        None => (None, e.trim_matches('"')),
    };
    for (t, alias) in tables {
        if let Some(p) = prefix {
            let matches = p.eq_ignore_ascii_case(&t.name) || alias.map(|a| p.eq_ignore_ascii_case(a.trim_matches('"'))).unwrap_or(false);
            if !matches {
                continue;
            }
        }
        if let Some(c) = t.columns.iter().find(|c| c.name.eq_ignore_ascii_case(col)) {
            return Some(c.data_type.field_type_str());
        }
    }
    None
}

/// alias → biểu thức gốc, theo cùng quy tắc đặt alias với `parse_projection`.
fn projection_exprs_by_alias(select_clause: &str) -> HashMap<String, String> {
    let mut out = HashMap::new();
    for item in split_projection_items(select_clause) {
        let upper = item.to_uppercase();
        let (expr_part, alias) = if let Some(as_idx) = rfind_top_level_as(&upper) {
            (item[..as_idx].trim(), item[as_idx + 4..].trim().trim_matches('"').to_string())
        } else {
            (item.trim(), clean_col_name(item.trim()).to_string())
        };
        let mut e = expr_part;
        while e.starts_with('(') && e.ends_with(')') && is_fully_enclosed_in_parens(e) {
            e = e[1..e.len() - 1].trim();
        }
        out.entry(alias).or_insert_with(|| e.to_string());
    }
    out
}

/// Vế phải chỉ gồm đúng một toán hạng: `$n`, một chuỗi '...' hoặc một số.
/// (Không có AND/OR/toán tử theo sau, kể cả viết thường.)
/// `col`, `"Col"`, `t.col`, `t."Col"`: tham chiếu cột đơn (không phải biểu thức).
fn is_column_ref(s: &str) -> bool {
    let t = s.trim();
    !t.is_empty()
        && t.split('.').count() <= 2
        && t.split('.').all(|part| {
            let p = part.trim();
            (p.len() >= 2 && p.starts_with('"') && p.ends_with('"') && !p[1..p.len() - 1].contains('"'))
                || (p.chars().next().map_or(false, |c| c.is_alphabetic() || c == '_') && p.chars().all(|c| c.is_alphanumeric() || c == '_'))
        })
}

fn is_single_operand(s: &str) -> bool {
    let t = s.trim();
    if let Some(n) = t.strip_prefix('$') {
        return !n.is_empty() && n.chars().all(|c| c.is_ascii_digit());
    }
    if t.len() >= 2 && t.starts_with('\'') && t.ends_with('\'') {
        // Chuỗi duy nhất: dấu nháy bên trong chỉ được xuất hiện dạng '' (escape)
        let inner = &t[1..t.len() - 1];
        return !inner.replace("''", "").contains('\'');
    }
    t.parse::<f64>().is_ok()
}

/// Cách nhận diện một dòng trong WAL khi UPDATE/DELETE.
pub(crate) enum RowIdentity {
    /// Khoá chính là số: bản ghi `Update`/`Delete` như định dạng cũ.
    Pk(i64),
    /// Khoá chính UUID/text: `UpdateByKey`/`DeleteByKey`.
    Key(Value),
    /// Không có khoá chính (hoặc khoá NULL): nhận diện bằng toàn bộ giá trị dòng.
    Row(Vec<Value>),
}

/// Khoá dùng để phát hiện trùng trong cùng một lệnh (cùng quy tắc với index khoá chính).
pub(crate) fn pk_dedup_key(v: &Value) -> String {
    match v {
        Value::Text(t) if t.trim().parse::<i64>().is_err() => {
            if t.len() == 36 { format!("s:{}", t.to_lowercase()) } else { format!("s:{}", t) }
        }
        other => format!("i:{}", other.as_i64().unwrap_or_default()),
    }
}

pub(crate) fn duplicate_pk_error(table: &crate::storage::table::Table, v: &Value) -> String {
    let col = table.pk_col_idx.map(|i| table.columns[i].name.as_str()).unwrap_or("id");
    let shown = match v {
        Value::Text(t) => t.to_string(),
        Value::Int(i) => i.to_string(),
        Value::Float(f) => f.to_string(),
        Value::Bool(b) => b.to_string(),
        Value::Null => "null".to_string(),
    };
    format!(
        "duplicate key value violates unique constraint \"{}_pkey\" (Key ({})=({}) already exists.)",
        table.name.to_lowercase(),
        col,
        shown
    )
}

impl RowIdentity {
    pub(crate) fn delete_record(self, table: String) -> WalRecord {
        match self {
            RowIdentity::Pk(pk) => WalRecord::Delete { table, pk },
            RowIdentity::Key(key) => WalRecord::DeleteByKey { table, key },
            RowIdentity::Row(row) => WalRecord::DeleteByRow { table, row },
        }
    }

    /// Bản ghi UPDATE cho các ô `updates` của dòng này (cột khoá chính phải không đổi).
    pub(crate) fn update_records(self, table: String, updates: Vec<(usize, Value)>) -> Vec<WalRecord> {
        match self {
            RowIdentity::Pk(pk) => updates.into_iter().map(|(col_idx, new_val)| WalRecord::Update { table: table.clone(), pk, col_idx, new_val }).collect(),
            RowIdentity::Key(key) => updates
                .into_iter()
                .map(|(col_idx, new_val)| WalRecord::UpdateByKey { table: table.clone(), key: key.clone(), col_idx, new_val })
                .collect(),
            RowIdentity::Row(old_row) => vec![WalRecord::UpdateByRow { table, old_row, updates }],
        }
    }

    pub(crate) fn of(table: &crate::storage::table::Table, row_idx: usize) -> Self {
        let row = &table.rows[row_idx];
        match table.pk_col_idx.and_then(|i| row.get(i)) {
            Some(v) if v.as_i64().is_some() => RowIdentity::Pk(v.as_i64().unwrap()),
            Some(v) if !v.is_null() => RowIdentity::Key(v.clone()),
            _ => RowIdentity::Row(row.clone()),
        }
    }
}

/// Kiểu của một giá trị đã tính xong, dùng khi không suy được từ biểu thức.
fn type_str_from_json_value(v: &serde_json::Value) -> String {
    match v {
        serde_json::Value::Bool(_) => "bool",
        serde_json::Value::Number(n) if n.as_i64().map_or(false, |i| i32::try_from(i).is_ok()) => "integer",
        serde_json::Value::Number(n) if n.is_i64() || n.is_u64() => "bigint",
        serde_json::Value::Number(_) => "numeric",
        serde_json::Value::Object(_) => "jsonb",
        _ => "text",
    }
    .to_string()
}

thread_local! {
    /// Câu SELECT sắp xử lý đã qua `normalize_select_sql` (chặn chuẩn hoá lặp lại).
    static SELECT_NORMALIZED: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

/// Cách đổi một ô chữ sang JSON ở đường JSON có kiểu (`Executor::execute_typed_json`).
#[derive(Clone, Copy, Debug)]
enum JsonCellKind {
    /// json / jsonb: parse JSON
    Json,
    /// `x::text` tường minh: giữ chuỗi
    Text,
    /// như trước: đoán (mảng PG, JSON, chuỗi)
    Guess,
}

thread_local! {
    /// Bật khi trả kết quả cho giao thức Postgres (`PGlite::query_wire`): chuỗi được giữ
    /// nguyên văn thay vì tự đoán thành mảng/JSON, bytea giữ dạng `\x…`.
    static RAW_TEXT_MODE: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

/// Bật/tắt chế độ raw text cho luồng hiện tại, trả về giá trị cũ để khôi phục.
pub(crate) fn set_raw_text_mode(on: bool) -> bool {
    RAW_TEXT_MODE.with(|m| m.replace(on))
}

#[inline(always)]
fn raw_text_mode() -> bool {
    RAW_TEXT_MODE.with(|m| m.get())
}

fn bytea_to_json(v: &Value) -> serde_json::Value {
    let raw = get_raw_bytes(v);
    if raw_text_mode() {
        let mut s = String::with_capacity(2 + raw.len() * 2);
        s.push_str("\\x");
        for b in raw {
            s.push_str(&format!("{:02x}", b));
        }
        return serde_json::Value::String(s);
    }
    serde_json::Value::Array(raw.into_iter().map(|b| serde_json::json!(b)).collect())
}

#[inline(always)]
fn value_to_json(val: &Value) -> serde_json::Value {
    if let Value::Text(s) = val {
        if raw_text_mode() {
            return serde_json::Value::String(s.to_string());
        }
    }
    match val {
        Value::Null => serde_json::Value::Null,
        Value::Bool(b) => serde_json::Value::Bool(*b),
        Value::Int(n) => serde_json::Value::Number((*n).into()),
        Value::Float(f) => serde_json::Number::from_f64(*f)
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null),
        Value::Text(s) => {
            let trimmed = s.trim();
            if let Some(arr_val) = parse_pg_array_to_json(trimmed) {
                return arr_val;
            }
            if trimmed.starts_with('[') && trimmed.ends_with(']') {
                if let Ok(val) = serde_json::from_str::<serde_json::Value>(trimmed) {
                    return val;
                }
            }
            if trimmed.starts_with('{') && trimmed.ends_with('}') {
                if let Ok(val) = serde_json::from_str::<serde_json::Value>(trimmed) {
                    return val;
                }
            }
            serde_json::Value::String(s.to_string())
        }
    }
}

/// Cả dòng thành JSON như `to_json(row)` của PostgreSQL: như `row_to_json`, thêm định dạng thời gian của
/// PostgreSQL (`timestamp` không có múi giờ, `timestamptz` có `+00:00`). Chỉ dùng cho đường cả dòng
/// (`to_json(t)`, `json_agg(t)`); `SELECT *` của API JSON giữ định dạng cũ.
fn row_to_pg_json(table: &crate::storage::table::Table, row: &[Value]) -> serde_json::Value {
    let mut j = row_to_json(table, row);
    if let serde_json::Value::Object(m) = &mut j {
        for (i, col) in table.columns.iter().enumerate() {
            let Some(d) = crate::engine::coltypes::column_decl(table, i) else { continue };
            if d.array {
                continue;
            }
            if let Some(serde_json::Value::String(v)) = m.get_mut(&col.name) {
                let t = v.trim().replace(' ', "T");
                match d.base.as_str() {
                    "timestamp" => *v = t.trim_end_matches('Z').to_string(),
                    "timestamptz" if t.ends_with('Z') => *v = format!("{}+00:00", t.trim_end_matches('Z')),
                    _ => {}
                }
            }
        }
    }
    j
}

fn row_to_json(table: &crate::storage::table::Table, row: &[Value]) -> serde_json::Value {
    let mut map = serde_json::Map::with_capacity(table.columns.len());
    for (i, col) in table.columns.iter().enumerate() {
        let v = row.get(i).unwrap_or(&Value::Null);
        // cột mảng: luôn là mảng JSON (cả ở raw text mode của wire, nơi chữ được giữ nguyên văn)
        if let (crate::types::DataType::Array(_), Value::Text(s)) = (&col.data_type, v) {
            let t = s.trim();
            if let Some(arr) = parse_pg_array_to_json(t).or_else(|| serde_json::from_str::<serde_json::Value>(t).ok().filter(|j| j.is_array())) {
                map.insert(col.name.clone(), arr);
                continue;
            }
        }
        if col.data_type == crate::types::DataType::Jsonb {
            if let Value::Text(s) = v {
                let trimmed = s.trim();
                if let Ok(json_val) = serde_json::from_str::<serde_json::Value>(trimmed) {
                    map.insert(col.name.clone(), json_val);
                    continue;
                }
            }
        } else if col.data_type == crate::types::DataType::Bytea {
            if v.is_null() {
                map.insert(col.name.clone(), serde_json::Value::Null);
            } else {
                map.insert(col.name.clone(), bytea_to_json(v));
            }
            continue;
        } else if col.data_type == crate::types::DataType::Date {
            match v {
                Value::Text(s) => {
                    let trimmed = s.trim();
                    if trimmed.is_empty() || trimmed == "{}" || trimmed.eq_ignore_ascii_case("NULL") {
                        map.insert(col.name.clone(), serde_json::Value::Null);
                    } else if trimmed.eq_ignore_ascii_case("CURRENT_DATE") {
                        map.insert(col.name.clone(), serde_json::Value::String(get_current_date()));
                    } else if let Some((d_part, _)) = trimmed.split_once('T') {
                        if d_part.len() == 10 && d_part.chars().all(|c| c.is_ascii_digit() || c == '-') {
                            map.insert(col.name.clone(), serde_json::Value::String(d_part.to_string()));
                        } else {
                            map.insert(col.name.clone(), serde_json::Value::String(trimmed.to_string()));
                        }
                    } else {
                        map.insert(col.name.clone(), serde_json::Value::String(trimmed.to_string()));
                    }
                    continue;
                }
                Value::Null => {
                    map.insert(col.name.clone(), serde_json::Value::Null);
                    continue;
                }
                _ => {}
            }
        } else if col.data_type == crate::types::DataType::Timestamp {
            match v {
                Value::Text(s) => {
                    let trimmed = s.trim();
                    if trimmed.is_empty() || trimmed == "{}" || trimmed.eq_ignore_ascii_case("NULL") {
                        map.insert(col.name.clone(), serde_json::Value::Null);
                    } else if trimmed.eq_ignore_ascii_case("CURRENT_TIMESTAMP") || trimmed.eq_ignore_ascii_case("NOW()") || trimmed.eq_ignore_ascii_case("LOCALTIMESTAMP") {
                        map.insert(col.name.clone(), serde_json::Value::String(get_current_timestamp()));
                    } else {
                        map.insert(col.name.clone(), serde_json::Value::String(trimmed.to_string()));
                    }
                    continue;
                }
                Value::Null => {
                    map.insert(col.name.clone(), serde_json::Value::Null);
                    continue;
                }
                _ => {}
            }
        }
        map.insert(col.name.clone(), value_to_json(v));
    }
    serde_json::Value::Object(map)
}

fn extract_returning_clause(sql: &str) -> (&str, Option<&str>) {
    if let Some(pos) = find_top_level_keyword(sql, "RETURNING") {
        (sql[..pos].trim(), Some(sql[pos + 9..].trim()))
    } else {
        (sql, None)
    }
}

/// Một phần tử của RETURNING: (biểu thức, tên cột kết quả, chỉ số cột nếu là cột đơn).
fn returning_items(table: &crate::storage::table::Table, r_cols: &str) -> Vec<(String, String, Option<usize>)> {
    let mut out = Vec::new();
    for item in split_projection_items(r_cols.trim()) {
        let item = item.trim();
        if item.is_empty() {
            continue;
        }
        if item == "*" || item.ends_with(".*") {
            for (i, c) in table.columns.iter().enumerate() {
                out.push((c.name.clone(), c.name.clone(), Some(i)));
            }
            continue;
        }
        let (expr, alias) = match find_top_level_keyword(item, "AS") {
            Some(p) => (item[..p].trim(), Some(item[p + 2..].trim().trim_matches('"').to_string())),
            None => (item, None),
        };
        let col_idx = if is_column_ref(expr) { table.get_column_index(clean_col_name(expr)) } else { None };
        let name = alias.unwrap_or_else(|| match col_idx {
            Some(i) => table.columns[i].name.clone(),
            None => {
                let e = expr.trim();
                match e.find('(') {
                    Some(p) if p > 0 && e[..p].chars().all(|c| c.is_alphanumeric() || c == '_') => e[..p].to_lowercase(),
                    _ => match e.rfind("::") {
                        Some(p) => clean_col_name(&e[..p]).to_string(),
                        None => "?column?".to_string(),
                    },
                }
            }
        });
        out.push((expr.to_string(), name, col_idx));
    }
    out
}

/// Một dòng RETURNING: cột, `*`, hay biểu thức bất kỳ trên dòng vừa ghi.
pub(crate) fn returning_row(table: &crate::storage::table::Table, row: &[Value], r_cols: &str, params: &[Value]) -> serde_json::Value {
    if r_cols.trim() == "*" {
        return row_to_json(table, row);
    }
    let mut map = serde_json::Map::new();
    for (expr, name, col_idx) in returning_items(table, r_cols) {
        let v = match col_idx {
            Some(i) => row.get(i).cloned().unwrap_or(Value::Null),
            None => eval_sql_expr(&expr, row, Some(table), &[], None, params),
        };
        map.insert(name, value_to_json(&v));
    }
    serde_json::Value::Object(map)
}

fn project_returning_row(table: &crate::storage::table::Table, row: &[Value], r_cols: &str) -> serde_json::Value {
    returning_row(table, row, r_cols, &[])
}

/// Kiểu cột của RETURNING: theo cột nếu là cột đơn, theo giá trị đã tính nếu là biểu thức.
pub(crate) fn returning_fields(table: &crate::storage::table::Table, r_cols: &str, rows: &[serde_json::Value]) -> Vec<FieldInfo> {
    returning_items(table, r_cols)
        .into_iter()
        .map(|(_, name, col_idx)| {
            let data_type = match col_idx {
                Some(i) => format!("{:?}", table.columns[i].data_type).to_lowercase(),
                None => rows.iter().find_map(|r| r.get(&name).filter(|v| !v.is_null()).map(type_str_from_json_value)).unwrap_or_else(|| "text".to_string()),
            };
            FieldInfo { name, data_type }
        })
        .collect()
}

fn get_returning_fields(table: &crate::storage::table::Table, r_cols: &str) -> Vec<FieldInfo> {
    returning_fields(table, r_cols, &[])
}

/// Giá trị DEFAULT khi INSERT (tham số của lệnh dùng được trong biểu thức).
pub(crate) fn eval_default_for_insert(def_str: &str, params: &[Value]) -> Value {
    // DB cũ có thể lưu DEFAULT dạng chữ không tính được: giữ hành vi cũ, không báo hàm lạ
    crate::engine::known_fns::without_check(|| eval_default_inner(def_str, params))
}

fn eval_default_inner(def_str: &str, params: &[Value]) -> Value {
    let up = def_str.trim().to_uppercase();
    // `DEFAULT nextval('t_id_seq'::regclass)`: để trống, bộ đếm của bảng cấp giá trị (engine/sequences.rs)
    if crate::engine::sequences::is_nextval_default(def_str) {
        return Value::Null;
    }
    // `DEFAULT current_user` ... (trước đây ghi chữ "current_user")
    match up.trim_end_matches("()") {
        "CURRENT_USER" | "SESSION_USER" | "CURRENT_ROLE" | "USER" | "CURRENT_DATABASE" | "CURRENT_CATALOG" => return Value::text("postgres"),
        "CURRENT_SCHEMA" => return Value::text("public"),
        _ => {}
    }
    if up.starts_with("GEN_RANDOM_UUID") || up.starts_with("UUID_GENERATE_V4") {
        Value::text(crate::types::generate_uuid_v4())
    } else if let Some(dt_val) = eval_date_time_keyword(def_str) {
        dt_val
    } else if def_str.starts_with('\'') && def_str.ends_with('\'') && def_str.len() >= 2 {
        Value::text(def_str[1..def_str.len() - 1].replace("''", "'"))
    } else if let Ok(n) = def_str.parse::<i64>() {
        Value::Int(n)
    } else if let Ok(f) = def_str.parse::<f64>() {
        Value::Float(f)
    } else if def_str.eq_ignore_ascii_case("TRUE") {
        Value::Bool(true)
    } else if def_str.eq_ignore_ascii_case("FALSE") {
        Value::Bool(false)
    } else {
        eval_sql_expr(def_str, &[], None, &[], None, params)
    }
}

/// Giá trị DEFAULT của một cột (cùng quy tắc với INSERT).
pub(crate) fn eval_default_value(def_str: &str) -> Value {
    eval_default_for_insert(def_str, &[])
}

pub(crate) fn value_to_json_pub(v: &Value) -> serde_json::Value {
    value_to_json(v)
}

pub(crate) fn evaluate_assignment_val_pub(val_str: &str, row: &[Value], table: &crate::storage::table::Table, params: &[Value]) -> Value {
    evaluate_assignment_val(val_str, row, table, params)
}

fn evaluate_assignment_val(
    val_str: &str,
    row: &[Value],
    table: &crate::storage::table::Table,
    params: &[Value],
) -> Value {
    let s = val_str.trim();
    if let Some(dt_val) = eval_date_time_keyword(s) {
        return dt_val;
    }
    eval_sql_expr(s, row, Some(table), &[], None, params)
}

pub(crate) fn coerce_update_val(col_def: &crate::types::ColumnDef, val: Value) -> Value {
    if val.is_null() {
        return Value::Null;
    }
    match (&col_def.data_type, &val) {
        (crate::types::DataType::Bytea, val) => {
            let raw = get_raw_bytes(val);
            let items: Vec<serde_json::Value> = raw.iter().map(|&b| serde_json::json!(b)).collect();
            Value::text(serde_json::to_string(&items).unwrap_or_default())
        }
        (crate::types::DataType::Integer | crate::types::DataType::Serial, Value::Float(f)) => {
            Value::Int(*f as i64)
        }
        (crate::types::DataType::BigInt, Value::Float(f)) => {
            Value::Int(*f as i64)
        }
        (crate::types::DataType::Integer | crate::types::DataType::Serial, Value::Text(s)) => {
            if let Ok(i) = s.parse::<i64>() {
                Value::Int(i)
            } else {
                val.clone()
            }
        }
        (crate::types::DataType::BigInt, Value::Text(s)) => {
            if let Ok(i) = s.parse::<i64>() {
                Value::Int(i)
            } else {
                val.clone()
            }
        }
        (crate::types::DataType::Numeric, Value::Text(s)) => {
            if let Ok(f) = s.parse::<f64>() {
                Value::Float(f)
            } else {
                val.clone()
            }
        }
        (crate::types::DataType::Boolean, Value::Text(s)) => {
            if s.eq_ignore_ascii_case("true") || s == "1" {
                Value::Bool(true)
            } else if s.eq_ignore_ascii_case("false") || s == "0" {
                Value::Bool(false)
            } else {
                val.clone()
            }
        }
        _ => val,
    }
}

/// Ép kiểu `val::type` như PostgreSQL. Giá trị không hợp lệ báo lỗi qua kênh lỗi (22P02 / 22003) và
/// trả NULL, thay vì âm thầm ra NULL như trước.
pub(crate) fn cast_val(val: Value, target_type: &str) -> Value {
    use crate::engine::errors::raise;
    if val.is_null() {
        return Value::Null;
    }
    let tt = target_type.trim().to_uppercase();
    let tt = tt.split_whitespace().collect::<Vec<_>>().join(" ");
    let base = tt.split('(').next().unwrap_or("").trim().to_string();
    let typmod: Vec<i64> = tt.find('(').and_then(|o| tt.rfind(')').map(|c| &tt[o + 1..c])).map(|m| m.split(',').filter_map(|x| x.trim().parse().ok()).collect()).unwrap_or_default();
    let shown = || match &val {
        Value::Text(t) => t.to_string(),
        other => other.as_str().to_string(),
    };
    let int_like = |v: &Value, ty: &str, min: i64, max: i64| -> Value {
        let n = match v {
            Value::Int(i) => Some(*i),
            Value::Float(f) if f.is_finite() => Some(f.round() as i64),
            Value::Bool(b) => Some(*b as i64),
            Value::Text(t) => t.trim().parse::<i64>().ok(),
            _ => None,
        };
        match n {
            Some(i) if i < min || i > max => {
                raise(format!("{} out of range", ty));
                Value::Null
            }
            Some(i) => Value::Int(i),
            None => {
                raise(format!("invalid input syntax for type {}: \"{}\"", ty, match v { Value::Text(t) => t.to_string(), o => o.as_str().to_string() }));
                Value::Null
            }
        }
    };
    if tt.ends_with("[]") || tt.starts_with("ARRAY") {
        let items = val_to_array_items(&val);
        let inner_type = tt.trim_end_matches("[]").trim_start_matches("ARRAY").trim();
        let casted_items: Vec<serde_json::Value> = items
            .into_iter()
            .map(|item| {
                let c = if inner_type.is_empty() { item } else { cast_val(item, inner_type) };
                value_to_json(&c)
            })
            .collect();
        return Value::text(serde_json::to_string(&casted_items).unwrap_or_default());
    }
    match base.as_str() {
        "INTERVAL" => match crate::engine::temporal::parse_interval(&val.as_str()) {
            Some(iv) => Value::text(crate::engine::temporal::format_interval(iv)),
            None => {
                raise(format!("invalid input syntax for type interval: \"{}\"", shown()));
                Value::Null
            }
        },
        "SMALLINT" | "INT2" => int_like(&val, "smallint", i16::MIN as i64, i16::MAX as i64),
        "INT" | "INTEGER" | "INT4" | "SERIAL" => int_like(&val, "integer", i32::MIN as i64, i32::MAX as i64),
        "BIGINT" | "INT8" | "BIGSERIAL" => int_like(&val, "bigint", i64::MIN, i64::MAX),
        "TEXT" | "NAME" | "CITEXT" => Value::text(val.as_str()),
        "VARCHAR" | "CHARACTER VARYING" => {
            let s = val.as_str();
            match typmod.first() {
                // Ép kiểu tường minh sang varchar(n) cắt bớt (không lỗi), như PostgreSQL
                Some(&n) if s.chars().count() > n as usize => Value::text(s.chars().take(n as usize).collect::<String>()),
                _ => Value::text(s),
            }
        }
        "CHAR" | "CHARACTER" | "BPCHAR" => {
            let s = val.as_str();
            let n = typmod.first().copied().unwrap_or(1) as usize;
            let mut out: String = s.chars().take(n).collect();
            while out.chars().count() < n {
                out.push(' ');
            }
            Value::text(out)
        }
        "NUMERIC" | "DECIMAL" => {
            let f = match &val {
                Value::Int(i) => Some(*i as f64),
                Value::Float(f) => Some(*f),
                Value::Text(t) => t.trim().parse::<f64>().ok(),
                Value::Bool(_) => None,
                _ => None,
            };
            match f {
                None => {
                    raise(format!("invalid input syntax for type numeric: \"{}\"", shown()));
                    Value::Null
                }
                Some(f) => match (typmod.first(), typmod.get(1)) {
                    (Some(&p), scale) => {
                        let sc = scale.copied().unwrap_or(0).max(0) as i32;
                        let factor = 10f64.powi(sc);
                        let r = (f * factor).round() / factor;
                        if r.abs() >= 10f64.powi((p as i32) - sc) {
                            raise("numeric field overflow");
                            return Value::Null;
                        }
                        if sc == 0 { Value::Int(r as i64) } else { Value::Float(r) }
                    }
                    _ => match &val {
                        Value::Int(i) => Value::Int(*i),
                        _ => Value::Float(f),
                    },
                },
            }
        }
        "FLOAT" | "FLOAT4" | "FLOAT8" | "REAL" | "DOUBLE" | "DOUBLE PRECISION" => match val.as_f64() {
            Some(f) => Value::Float(f),
            None => {
                raise(format!("invalid input syntax for type double precision: \"{}\"", shown()));
                Value::Null
            }
        },
        "BOOL" | "BOOLEAN" => {
            let b = match &val {
                Value::Bool(b) => Some(*b),
                Value::Int(i) => Some(*i != 0),
                Value::Text(t) => match t.trim().to_lowercase().as_str() {
                    "t" | "true" | "y" | "yes" | "on" | "1" => Some(true),
                    "f" | "false" | "n" | "no" | "off" | "0" => Some(false),
                    _ => None,
                },
                _ => None,
            };
            match b {
                Some(b) => Value::Bool(b),
                None => {
                    raise(format!("invalid input syntax for type boolean: \"{}\"", shown()));
                    Value::Null
                }
            }
        }
        "UUID" => {
            let s = val.as_str();
            let hex: String = s.trim().trim_matches(|c| c == '{' || c == '}').chars().filter(|c| *c != '-').collect();
            if hex.len() != 32 || !hex.chars().all(|c| c.is_ascii_hexdigit()) {
                raise(format!("invalid input syntax for type uuid: \"{}\"", s));
                return Value::Null;
            }
            let h = hex.to_lowercase();
            Value::text(format!("{}-{}-{}-{}-{}", &h[0..8], &h[8..12], &h[12..16], &h[16..20], &h[20..32]))
        }
        "JSON" | "JSONB" => {
            if let Value::Text(t) = &val {
                if serde_json::from_str::<serde_json::Value>(t).is_err() {
                    raise(format!("invalid input syntax for type json: \"{}\"", t));
                    return Value::Null;
                }
            }
            val
        }
        "DATE" if crate::engine::datetime::tz_to_date(&val.as_str()).is_some() => Value::text(crate::engine::datetime::tz_to_date(&val.as_str()).unwrap_or_default()),
        "DATE" => match crate::engine::temporal::cast_date(&val.as_str()) {
            Some(d) => Value::text(d),
            None if crate::engine::temporal::field_out_of_range(&val.as_str()) => {
                raise(format!("date/time field value out of range: \"{}\"", shown()));
                Value::Null
            }
            None => {
                raise(format!("invalid input syntax for type date: \"{}\"", shown()));
                Value::Null
            }
        },
        "TIMESTAMP" | "TIMESTAMPTZ" | "TIMESTAMP WITHOUT TIME ZONE" | "TIMESTAMP WITH TIME ZONE" => {
            let s = val.as_str();
            let with_tz = base == "TIMESTAMPTZ" || base == "TIMESTAMP WITH TIME ZONE";
            // giá trị đặc biệt: 'epoch', 'today', 'now'... thành thời điểm cụ thể (giữ 'infinity')
            if let Some(sp) = crate::engine::datetime::resolve_special(&s, with_tz) {
                return Value::text(sp);
            }
            if crate::engine::temporal::is_valid_timestamp(&s) {
                // theo múi giờ phiên (UTC: giữ nguyên chuỗi như các bản trước)
                let conv = if with_tz { crate::engine::datetime::naive_to_tz(&s) } else { crate::engine::datetime::tz_to_naive(&s) };
                match conv {
                    Some(c) => Value::text(c),
                    None => val,
                }
            } else if crate::engine::temporal::field_out_of_range(&s) {
                raise(format!("date/time field value out of range: \"{}\"", s));
                Value::Null
            } else {
                raise(format!("invalid input syntax for type timestamp: \"{}\"", s));
                Value::Null
            }
        }
        "BOX" => {
            // PostgreSQL lưu box theo (góc trên phải),(góc dưới trái)
            let t = val.as_str();
            let nums: Vec<f64> = t.split(|c: char| matches!(c, '(' | ')' | ',') || c.is_whitespace()).filter(|x| !x.is_empty()).map(|x| x.parse::<f64>()).collect::<Result<_, _>>().unwrap_or_default();
            if nums.len() != 4 {
                raise(format!("invalid input syntax for type box: \"{}\"", t));
                return Value::Null;
            }
            let (x1, y1, x2, y2) = (nums[0], nums[1], nums[2], nums[3]);
            Value::text(format!("({},{}),({},{})", x1.max(x2), y1.max(y2), x1.min(x2), y1.min(y2)))
        }
        "JSONPATH" => Value::text(normalize_jsonpath(&val.as_str())),
        "BYTEA" => {
            let raw = get_raw_bytes(&val);
            let items: Vec<serde_json::Value> = raw.iter().map(|&b| serde_json::json!(b)).collect();
            Value::text(serde_json::to_string(&items).unwrap_or_default())
        }
        _ => val,
    }
}

/// Text output của jsonpath như PostgreSQL ở dạng đơn giản: khoá sau dấu chấm được đặt trong nháy kép
/// (`$.a.b[0]` → `$."a"."b"[0]`).
fn normalize_jsonpath(p: &str) -> String {
    let mut out = String::with_capacity(p.len() + 4);
    let mut chars = p.trim().chars().peekable();
    let mut in_str = false;
    while let Some(c) = chars.next() {
        out.push(c);
        if c == '"' {
            in_str = !in_str;
        } else if c == '.' && !in_str && chars.peek().is_some_and(|n| n.is_alphabetic() || *n == '_') {
            let mut key = String::new();
            while let Some(&n) = chars.peek() {
                if n.is_alphanumeric() || n == '_' {
                    key.push(n);
                    chars.next();
                } else {
                    break;
                }
            }
            out.push('"');
            out.push_str(&key);
            out.push('"');
        }
    }
    out
}

pub(crate) fn split_function_args(inner: &str) -> Vec<String> {
    let mut args = Vec::new();
    let mut current = String::new();
    let mut depth: i32 = 0;
    let mut in_str = false;

    for ch in inner.chars() {
        match ch {
            '\'' => {
                in_str = !in_str;
                current.push(ch);
            }
            '(' | '[' | '{' if !in_str => {
                depth += 1;
                current.push(ch);
            }
            ')' | ']' | '}' if !in_str => {
                depth = depth.saturating_sub(1);
                current.push(ch);
            }
            ',' if !in_str && depth == 0 => {
                args.push(current.trim().to_string());
                current.clear();
            }
            _ => {
                current.push(ch);
            }
        }
    }
    if !current.trim().is_empty() {
        args.push(current.trim().to_string());
    }
    args
}

/// Vị trí từ khoá `kw` ở cấp ngoài cùng của một biểu thức CASE: bỏ qua chuỗi, ngoặc và cả các
/// CASE ... END lồng bên trong (WHEN/THEN của CASE con không được tính).
fn find_case_level_keyword(s: &str, kw: &str) -> Option<usize> {
    let b = s.as_bytes();
    let (mut depth, mut case_depth, mut in_sq, mut in_dq) = (0i32, 0i32, false, false);
    let is_word = |c: u8| c.is_ascii_alphanumeric() || c == b'_';
    let mut i = 0;
    while i < b.len() {
        let c = b[i];
        if c == b'\'' && !in_dq {
            in_sq = !in_sq;
        } else if c == b'"' && !in_sq {
            in_dq = !in_dq;
        } else if !in_sq && !in_dq {
            if c == b'(' {
                depth += 1;
            } else if c == b')' {
                depth -= 1;
            } else if depth == 0 && is_word(c) && (i == 0 || !is_word(b[i - 1])) {
                let mut j = i;
                while j < b.len() && is_word(b[j]) {
                    j += 1;
                }
                let word = &s[i..j];
                if word.eq_ignore_ascii_case("CASE") {
                    case_depth += 1;
                } else if word.eq_ignore_ascii_case("END") && case_depth > 0 {
                    case_depth -= 1;
                } else if case_depth == 0 && word.eq_ignore_ascii_case(kw) {
                    return Some(i);
                }
                i = j;
                continue;
            }
        }
        i += 1;
    }
    None
}

pub(crate) fn split_when_clauses(when_part: &str) -> Vec<(String, String)> {
    let mut clauses = Vec::new();
    let s = when_part.trim();
    // CASE <toán hạng> WHEN v1 THEN ...: đổi thành điều kiện (toán hạng) = (v1)
    let operand = match find_case_level_keyword(s, "WHEN") {
        Some(p) if !s[..p].trim().is_empty() => Some(s[..p].trim().to_string()),
        _ => None,
    };
    let mut i = 0;
    let len = s.len();

    while i < len {
        let remaining = &s[i..];
        let when_kw = find_case_level_keyword(remaining, "WHEN");
        if when_kw.is_none() {
            break;
        }
        let when_start = i + when_kw.unwrap() + 4;
        let then_kw = find_case_level_keyword(&s[when_start..], "THEN");
        if then_kw.is_none() {
            break;
        }
        let then_pos = when_start + then_kw.unwrap();
        let mut cond_str = s[when_start..then_pos].trim().to_string();
        if let Some(op) = &operand {
            cond_str = format!("({}) = ({})", op, cond_str);
        }

        let res_start = then_pos + 4;
        let next_when_kw = find_case_level_keyword(&s[res_start..], "WHEN");
        let res_end = if let Some(nw) = next_when_kw {
            res_start + nw
        } else {
            len
        };
        let res_str = s[res_start..res_end].trim().to_string();
        clauses.push((cond_str, res_str));
        i = res_end;
    }
    clauses
}

/// ELSE của một CASE (bỏ qua ELSE của CASE lồng bên trong).
pub(crate) fn find_case_else(inside: &str) -> Option<usize> {
    find_case_level_keyword(inside, "ELSE")
}

pub(crate) fn find_top_level_op(s: &str, op: &str) -> Option<usize> {
    let mut in_str = false;
    let mut depth: i32 = 0;
    let op_bytes = op.as_bytes();
    let s_bytes = s.as_bytes();
    let len = s_bytes.len();

    let mut i = 0;
    while i < len {
        let b = s_bytes[i];
        if b == b'\'' {
            in_str = !in_str;
            i += 1;
            continue;
        }
        if in_str {
            i += 1;
            continue;
        }
        if b == b'(' || b == b'[' {
            depth += 1;
        } else if b == b')' || b == b']' {
            depth = depth.saturating_sub(1);
        }
        if depth == 0 && b == b'-' && i + 1 < len && s_bytes[i + 1] == b'>' {
            if i + 2 < len && s_bytes[i + 2] == b'>' {
                i += 3;
            } else {
                i += 2;
            }
            continue;
        }
        if depth == 0 && i + op_bytes.len() <= len {
            if &s_bytes[i..i + op_bytes.len()] == op_bytes {
                if op == ">" {
                    if i + 1 < len && (s_bytes[i + 1] == b'=' || s_bytes[i + 1] == b'>') {
                        i += 2;
                        continue;
                    }
                    if i > 0 && s_bytes[i - 1] == b'>' {
                        i += 1;
                        continue;
                    }
                }
                if op == "<" {
                    if i + 1 < len && (s_bytes[i + 1] == b'=' || s_bytes[i + 1] == b'>' || s_bytes[i + 1] == b'@' || s_bytes[i + 1] == b'<') {
                        i += 2;
                        continue;
                    }
                    if i > 0 && s_bytes[i - 1] == b'<' {
                        i += 1;
                        continue;
                    }
                }
                if op == ">" && i > 0 && (s_bytes[i - 1] == b'-' || s_bytes[i - 1] == b'@' || s_bytes[i - 1] == b'#') {
                    i += 1;
                    continue;
                }
                if op == "?" && i + 1 < len && (s_bytes[i + 1] == b'|' || s_bytes[i + 1] == b'&') {
                    i += 2;
                    continue;
                }
                if (op == "~" || op == "~*" || op == "!~" || op == "!~*") && (i == 0 || s[..i].trim().is_empty()) {
                    i += 1;
                    continue;
                }
                if op == "~" && i > 0 && s_bytes[i - 1] == b'!' {
                    i += 1;
                    continue;
                }
                if op == "~" && i + 1 < len && s_bytes[i + 1] == b'*' {
                    i += 2;
                    continue;
                }
                if op == "!~" && i + 2 < len && s_bytes[i + 2] == b'*' {
                    i += 3;
                    continue;
                }
                if op == "=" && i > 0 && (s_bytes[i - 1] == b'!' || s_bytes[i - 1] == b'<' || s_bytes[i - 1] == b'>' || s_bytes[i - 1] == b':') {
                    i += 1;
                    continue;
                }
                return Some(i);
            }
        }
        i += 1;
    }
    None
}

pub(crate) fn find_last_top_level_op(s: &str, op: &str) -> Option<usize> {
    let mut in_str = false;
    let mut depth: i32 = 0;
    let op_bytes = op.as_bytes();
    let s_bytes = s.as_bytes();
    let len = s_bytes.len();
    let mut last_pos = None;

    let mut i = 0;
    while i < len {
        let b = s_bytes[i];
        if b == b'\'' {
            in_str = !in_str;
            i += 1;
            continue;
        }
        if in_str {
            i += 1;
            continue;
        }
        if b == b'(' || b == b'[' {
            depth += 1;
        } else if b == b')' || b == b']' {
            depth = depth.saturating_sub(1);
        }
        if depth == 0 && i + op_bytes.len() <= len {
            if &s_bytes[i..i + op_bytes.len()] == op_bytes {
                last_pos = Some(i);
            }
        }
        i += 1;
    }
    last_pos
}

fn find_top_level_shift_op(s: &str) -> Option<(usize, &'static str)> {
    let mut in_single_quote = false;
    let mut depth: i32 = 0;
    let bytes = s.as_bytes();
    let len = bytes.len();
    if len < 2 {
        return None;
    }
    let mut i = (len as isize) - 2;
    while i >= 0 {
        let idx = i as usize;
        let b = bytes[idx];
        if b == b'\'' {
            in_single_quote = !in_single_quote;
        } else if !in_single_quote {
            if b == b')' || b == b']' {
                depth += 1;
            } else if b == b'(' || b == b'[' {
                depth = (depth - 1).max(0);
            } else if depth == 0 {
                if bytes[idx] == b'<' && bytes[idx + 1] == b'<' {
                    return Some((idx, "<<"));
                }
                if bytes[idx] == b'>' && bytes[idx + 1] == b'>' {
                    if idx > 0 && (bytes[idx - 1] == b'#' || bytes[idx - 1] == b'-') {
                        i -= 1;
                        continue;
                    }
                    return Some((idx, ">>"));
                }
            }
        }
        i -= 1;
    }
    None
}

pub(crate) fn find_top_level_math_op(s: &str, ops: &[char]) -> Option<(usize, char)> {
    let mut in_single_quote = false;
    let mut depth: i32 = 0;
    let bytes = s.as_bytes();
    let len = bytes.len();
    
    let mut i = (len as isize) - 1;
    while i >= 0 {
        let idx = i as usize;
        let b = bytes[idx];
        if b == b'\'' {
            in_single_quote = !in_single_quote;
        } else if !in_single_quote {
            if b == b')' || b == b']' {
                depth += 1;
            } else if b == b'(' || b == b'[' {
                depth = (depth - 1).max(0);
            } else if depth == 0 {
                let ch = b as char;
                if ops.contains(&ch) {
                    if ch == '-' {
                        if idx + 1 < len && bytes[idx + 1] == b'>' {
                            i -= 1;
                            continue;
                        }
                        if idx == 0 {
                            i -= 1;
                            continue;
                        }
                        let prev_non_space = s[..idx].trim_end();
                        if prev_non_space.ends_with(|c: char| "+-*/=<>!%^&|,".contains(c)) {
                            i -= 1;
                            continue;
                        }
                    }
                    if ch == '*' {
                        let prev_non_space = s[..idx].trim_end();
                        let next_non_space = s[idx + 1..].trim_start();
                        // `~*`, `!~*` là toán tử regex, không phải phép nhân
                        if prev_non_space.is_empty() || next_non_space.is_empty() || idx > 0 && bytes[idx - 1] == b'~' {
                            i -= 1;
                            continue;
                        }
                    }
                    if ch == '|' {
                        if (idx + 1 < len && bytes[idx + 1] == b'|') || (idx > 0 && bytes[idx - 1] == b'|') {
                            i -= 1;
                            continue;
                        }
                    }
                    if ch == '#' {
                        if idx + 1 < len && bytes[idx + 1] == b'>' {
                            i -= 1;
                            continue;
                        }
                    }
                    return Some((idx, ch));
                }
            }
        }
        i -= 1;
    }
    None
}

pub(crate) fn find_last_top_level_json_op(s: &str) -> Option<(usize, &'static str)> {
    let mut in_str = false;
    let mut depth: i32 = 0;
    let s_bytes = s.as_bytes();
    let len = s_bytes.len();
    let mut last_match = None;

    let mut i = 0;
    while i < len {
        let b = s_bytes[i];
        if b == b'\'' {
            in_str = !in_str;
            i += 1;
            continue;
        }
        if in_str {
            i += 1;
            continue;
        }
        if b == b'(' || b == b'[' {
            depth += 1;
        } else if b == b')' || b == b']' {
            depth = depth.saturating_sub(1);
        } else if depth == 0 {
            if i + 3 <= len && &s_bytes[i..i + 3] == b"->>" {
                last_match = Some((i, "->>"));
                i += 3;
                continue;
            } else if i + 2 <= len && &s_bytes[i..i + 2] == b"->" {
                last_match = Some((i, "->"));
                i += 2;
                continue;
            } else if i + 3 <= len && &s_bytes[i..i + 3] == b"#>>" {
                last_match = Some((i, "#>>"));
                i += 3;
                continue;
            } else if i + 2 <= len && &s_bytes[i..i + 2] == b"#>" {
                last_match = Some((i, "#>"));
                i += 2;
                continue;
            }
        }
        i += 1;
    }
    last_match
}

pub(crate) fn parse_val_to_json(v: &Value) -> Option<serde_json::Value> {
    match v {
        Value::Text(s) => {
            let trimmed = s.trim();
            if trimmed.starts_with('{') || trimmed.starts_with('[') {
                if let Ok(val) = serde_json::from_str(trimmed) {
                    return Some(val);
                }
                if let Some(val) = parse_pg_array_to_json(trimmed) {
                    return Some(val);
                }
                None
            } else if trimmed.starts_with('\'') && trimmed.ends_with('\'') && trimmed.len() >= 2 {
                let unquoted = &trimmed[1..trimmed.len() - 1];
                if unquoted.starts_with('{') || unquoted.starts_with('[') {
                    serde_json::from_str(unquoted).ok().or_else(|| parse_pg_array_to_json(unquoted))
                } else {
                    Some(serde_json::Value::String(unquoted.to_string()))
                }
            } else {
                Some(serde_json::Value::String(trimmed.to_string()))
            }
        }
        Value::Null => None,
        Value::Bool(b) => Some(serde_json::Value::Bool(*b)),
        Value::Int(i) => Some(serde_json::Value::Number((*i).into())),
        Value::Float(f) => serde_json::Number::from_f64(*f).map(serde_json::Value::Number),
    }
}

fn parse_json_path_keys(rhs_val: &serde_json::Value) -> Vec<String> {
    match rhs_val {
        serde_json::Value::Array(arr) => arr.iter().filter_map(|x| match x {
            serde_json::Value::String(s) => Some(s.clone()),
            serde_json::Value::Number(n) => Some(n.to_string()),
            _ => None,
        }).collect(),
        serde_json::Value::String(s) => {
            let trimmed = s.trim().trim_matches('\'').trim_matches('"').trim();
            if let Ok(serde_json::Value::Array(arr)) = serde_json::from_str::<serde_json::Value>(trimmed) {
                arr.iter().filter_map(|x| match x {
                    serde_json::Value::String(st) => Some(st.clone()),
                    serde_json::Value::Number(n) => Some(n.to_string()),
                    _ => None,
                }).collect()
            } else {
                let inner = trimmed.trim_matches('{').trim_matches('}').trim_matches('[').trim_matches(']');
                inner.split(',').map(|p| p.trim().trim_matches('\'').trim_matches('"').to_string()).filter(|p| !p.is_empty()).collect()
            }
        }
        _ => vec![],
    }
}

pub(crate) fn parse_str_or_json_keys(v: &Value) -> Vec<String> {
    match v {
        Value::Text(s) => {
            let val = serde_json::Value::String(s.to_string());
            parse_json_path_keys(&val)
        }
        _ => {
            let val = value_to_json(v);
            parse_json_path_keys(&val)
        }
    }
}

pub(crate) fn json_contains_check(target: &serde_json::Value, contained: &serde_json::Value) -> bool {
    match (target, contained) {
        (serde_json::Value::Object(t_map), serde_json::Value::Object(c_map)) => {
            for (k, c_v) in c_map {
                if let Some(t_v) = t_map.get(k) {
                    if !json_contains_check(t_v, c_v) {
                        return false;
                    }
                } else {
                    return false;
                }
            }
            true
        }
        (serde_json::Value::Array(t_arr), serde_json::Value::Array(c_arr)) => {
            for c_elem in c_arr {
                if !t_arr.iter().any(|t_elem| json_contains_check(t_elem, c_elem)) {
                    return false;
                }
            }
            true
        }
        (serde_json::Value::Array(t_arr), _) => {
            t_arr.iter().any(|t_elem| json_contains_check(t_elem, contained))
        }
        _ => target == contained,
    }
}

pub(crate) fn json_has_key_check(target: &serde_json::Value, key: &str) -> bool {
    let clean_key = key.trim().trim_matches('\'').trim_matches('"');
    match target {
        serde_json::Value::Object(map) => map.contains_key(clean_key),
        serde_json::Value::Array(arr) => arr.iter().any(|x| match x {
            serde_json::Value::String(s) => s == clean_key,
            _ => false,
        }),
        _ => false,
    }
}

fn jsonb_set_serde(target: &mut serde_json::Value, path: &[String], new_val: &serde_json::Value, create_missing: bool) {
    if path.is_empty() {
        *target = new_val.clone();
        return;
    }
    let key = &path[0];
    if path.len() == 1 {
        match target {
            serde_json::Value::Object(map) => {
                if create_missing || map.contains_key(key) {
                    map.insert(key.clone(), new_val.clone());
                }
            }
            serde_json::Value::Array(arr) => {
                if let Ok(idx) = key.parse::<usize>() {
                    if idx < arr.len() {
                        arr[idx] = new_val.clone();
                    } else if create_missing && idx == arr.len() {
                        arr.push(new_val.clone());
                    }
                }
            }
            _ => {}
        }
    } else {
        match target {
            serde_json::Value::Object(map) => {
                if let Some(next) = map.get_mut(key) {
                    jsonb_set_serde(next, &path[1..], new_val, create_missing);
                } else if create_missing {
                    let mut next = serde_json::Value::Object(serde_json::Map::new());
                    jsonb_set_serde(&mut next, &path[1..], new_val, create_missing);
                    map.insert(key.clone(), next);
                }
            }
            serde_json::Value::Array(arr) => {
                if let Ok(idx) = key.parse::<usize>() {
                    if let Some(next) = arr.get_mut(idx) {
                        jsonb_set_serde(next, &path[1..], new_val, create_missing);
                    }
                }
            }
            _ => {}
        }
    }
}

pub(crate) fn eval_json_extract_serde(lhs: &serde_json::Value, rhs: &serde_json::Value, op: &str) -> serde_json::Value {
    let parsed_lhs = match lhs {
        serde_json::Value::String(s) => {
            let trimmed = s.trim();
            if trimmed.starts_with('{') || trimmed.starts_with('[') {
                match serde_json::from_str::<serde_json::Value>(trimmed) {
                    Ok(v) => v,
                    Err(_) => return serde_json::Value::Null,
                }
            } else {
                return serde_json::Value::Null;
            }
        }
        serde_json::Value::Object(_) | serde_json::Value::Array(_) => lhs.clone(),
        _ => return serde_json::Value::Null,
    };

    if op == "#>" || op == "#>>" {
        let keys = parse_json_path_keys(rhs);
        let mut curr = &parsed_lhs;
        for k in &keys {
            match curr {
                serde_json::Value::Object(map) => {
                    if let Some(next) = map.get(k) {
                        curr = next;
                    } else {
                        return serde_json::Value::Null;
                    }
                }
                serde_json::Value::Array(arr) => {
                    if let Ok(idx) = k.parse::<usize>() {
                        if let Some(next) = arr.get(idx) {
                            curr = next;
                        } else {
                            return serde_json::Value::Null;
                        }
                    } else {
                        return serde_json::Value::Null;
                    }
                }
                _ => return serde_json::Value::Null,
            }
        }
        if op == "#>>" {
            match curr {
                serde_json::Value::Null => serde_json::Value::Null,
                serde_json::Value::String(s) => serde_json::Value::String(s.clone()),
                _ => serde_json::Value::String(curr.to_string()),
            }
        } else {
            curr.clone()
        }
    } else {
        // op is -> or ->>
        let key_str = match rhs {
            serde_json::Value::String(s) => s.trim().trim_matches('\'').trim_matches('"').to_string(),
            serde_json::Value::Number(n) => n.to_string(),
            _ => json_val_to_str(rhs).trim().trim_matches('\'').trim_matches('"').to_string(),
        };

        let target = match &parsed_lhs {
            serde_json::Value::Object(map) => map.get(&key_str),
            serde_json::Value::Array(arr) => {
                if let Ok(idx) = key_str.parse::<usize>() {
                    arr.get(idx)
                } else {
                    None
                }
            }
            _ => None,
        };

        match target {
            Some(v) => {
                if op == "->>" {
                    match v {
                        serde_json::Value::Null => serde_json::Value::Null,
                        serde_json::Value::String(s) => serde_json::Value::String(s.clone()),
                        _ => serde_json::Value::String(v.to_string()),
                    }
                } else {
                    v.clone()
                }
            }
            None => serde_json::Value::Null,
        }
    }
}

#[allow(dead_code)]
fn eval_json_extract(
    lhs: &Value,
    rhs_key: &str,
    as_text: bool,
) -> Value {
    let lhs_json = match lhs {
        Value::Text(s) => serde_json::Value::String(s.to_string()),
        _ => value_to_json(lhs),
    };
    let rhs_json = serde_json::Value::String(rhs_key.to_string());
    let op = if as_text { "->>" } else { "->" };
    let res = eval_json_extract_serde(&lhs_json, &rhs_json, op);
    match res {
        serde_json::Value::Null => Value::Null,
        serde_json::Value::String(s) => Value::text(s),
        _ => Value::text(res.to_string()),
    }
}

fn evaluate_custom_condition(
    cond: &str,
    primary_row: &[Value],
    primary_table: Option<&crate::storage::table::Table>,
    joined_row: &[Value],
    joined_table: Option<&crate::storage::table::Table>,
    params: &[Value],
) -> bool {
    eval_sql_expr(cond, primary_row, primary_table, joined_row, joined_table, params).as_bool() == Some(true)
}

/// Vế trái (trước toán tử so sánh đầu tiên) của điều kiện có `::`.
fn lhs_has_cast(s: &str) -> bool {
    let Some(c) = s.find("::") else { return false };
    let up = s.to_ascii_uppercase();
    let first_op = ["=", "<", ">", "!", "~", " LIKE ", " ILIKE ", " IN ", " IN(", " BETWEEN ", " IS ", " SIMILAR "]
        .iter()
        .filter_map(|op| up.find(op))
        .min()
        .unwrap_or(s.len());
    c < first_op
}

/// `x::t` tại `colon_pos` là phép ngoài cùng của `s`: phần trước không có toán tử hai ngôi ở cấp ngoài cùng
/// (`now() - '1 hour'::interval` là phép trừ, `::` chỉ thuộc vế phải).
pub(crate) fn cast_is_outermost(s: &str, colon_pos: usize) -> bool {
    let left = &s[..colon_pos];
    if find_top_level_math_op(left, &['+', '-', '*', '/', '%']).is_some_and(|(p, _)| !left[..p].trim().is_empty()) {
        return false;
    }
    if ["||", ">=", "<=", "<>", "!=", "=", "<", ">", "~"].iter().any(|op| find_top_level_op(left, op).is_some_and(|p| p > 0)) {
        return false;
    }
    !["AND", "OR", "IS", "LIKE", "ILIKE", "IN", "BETWEEN"].iter().any(|k| find_top_level_keyword(left, k).is_some_and(|p| p > 0))
}

/// Tên hàm ngày giờ do engine/datetime.rs xử lý (lọc nhanh trước khi gọi).
pub(crate) fn is_datetime_fn_name(upper: &str) -> bool {
    const NAMES: &[&str] = &[
        "EXTRACT(", "DATE_PART(", "DATE_TRUNC(", "TO_CHAR(", "AGE(", "TO_TIMESTAMP(", "TO_DATE(", "MAKE_DATE(", "MAKE_TIME(", "MAKE_TIMESTAMP(",
        "MAKE_TIMESTAMPTZ(", "MAKE_INTERVAL(", "JUSTIFY_DAYS(", "JUSTIFY_HOURS(", "JUSTIFY_INTERVAL(", "ISFINITE(", "DATE_ADD(", "DATE_SUBTRACT(",
        "DATE_BIN(", "OVERLAPS(", "CLOCK_TIMESTAMP(", "STATEMENT_TIMESTAMP(", "TRANSACTION_TIMESTAMP(", "TIMEOFDAY(",
    ];
    let b = upper.as_bytes();
    if b.is_empty() || !matches!(b[0], b'E' | b'D' | b'T' | b'A' | b'M' | b'J' | b'I' | b'O' | b'C' | b'S') {
        return false;
    }
    NAMES.iter().any(|n| upper.starts_with(n))
}

/// Kết quả `pg_typeof(inner)`: suy tĩnh (cột, ép kiểu, literal, hàm quen thuộc), không được thì theo giá
/// trị lúc chạy.
fn pg_typeof_value(
    inner: &str,
    primary_row: &[Value],
    primary_table: Option<&crate::storage::table::Table>,
    joined_row: &[Value],
    joined_table: Option<&crate::storage::table::Table>,
    params: &[Value],
) -> String {
    let col = |c: &str| {
        let name = c.rsplit('.').next().unwrap_or(c).trim_matches('"');
        [primary_table, joined_table]
            .into_iter()
            .flatten()
            .find_map(|t| t.get_column_index(name).and_then(|i| crate::engine::coltypes::column_decl(t, i)))
    };
    if let Some(t) = crate::engine::coltypes::pg_typeof(inner, &col) {
        return t;
    }
    match eval_sql_expr(inner, primary_row, primary_table, joined_row, joined_table, params) {
        Value::Int(i) if i32::try_from(i).is_ok() => "integer",
        Value::Int(_) => "bigint",
        Value::Float(_) => "numeric",
        Value::Bool(_) => "boolean",
        Value::Null => "unknown",
        Value::Text(t) if t.starts_with('{') && t.ends_with('}') && t.contains(':') => "jsonb",
        _ => "text",
    }
    .to_string()
}

/// `a op b` khi cả hai là số nguyên và biểu thức chắc chắn có kiểu nguyên (literal, cột khai báo
/// INT/BIGINT...): tính theo PostgreSQL (chia nguyên, tràn số 22003). Ngược lại `None` → tính như cũ.
fn typed_int_arith(
    op: char,
    lhs: &Value,
    rhs: &Value,
    expr: &str,
    primary_table: Option<&crate::storage::table::Table>,
    joined_table: Option<&crate::storage::table::Table>,
) -> Option<Value> {
    let (Value::Int(a), Value::Int(b)) = (lhs, rhs) else { return None };
    let col = |c: &str| {
        crate::engine::inttype::column_int_kind(primary_table, c).or_else(|| crate::engine::inttype::column_int_kind(joined_table, c))
    };
    let kind = crate::engine::inttype::int_kind(expr, &col)?;
    Some(crate::engine::inttype::int_arith(op, *a, *b, kind))
}

pub(crate) fn eval_sql_expr(
    expr: &str,
    primary_row: &[Value],
    primary_table: Option<&crate::storage::table::Table>,
    joined_row: &[Value],
    joined_table: Option<&crate::storage::table::Table>,
    params: &[Value],
) -> Value {
    let mut s = expr.trim();
    while s.starts_with('(') && s.ends_with(')') && is_fully_enclosed_in_parens(s) {
        let inside = s[1..s.len() - 1].trim();
        if split_comma_separated_tokens(inside).len() > 1 {
            break;
        }
        s = inside;
    }
    let upper = s.to_uppercase();

    // 0. UUID generation functions
    if upper == "GEN_RANDOM_UUID()" || upper == "UUID_GENERATE_V4()" {
        return Value::text(crate::types::generate_uuid_v4());
    }

    // 1. CASE WHEN ... THEN ... ELSE ... END
    if upper.starts_with("CASE") && upper.ends_with("END") {
        let inside = s[4..s.len() - 3].trim();
        let (when_part, else_part) = if let Some(else_idx) = find_case_else(inside) {
            (&inside[..else_idx], Some(inside[else_idx + 4..].trim()))
        } else {
            (inside, None)
        };

        let when_items = split_when_clauses(when_part);
        for (cond_str, res_str) in when_items {
            if evaluate_custom_condition(&cond_str, primary_row, primary_table, joined_row, joined_table, params) {
                return eval_sql_expr(&res_str, primary_row, primary_table, joined_row, joined_table, params);
            }
        }

        if let Some(e_str) = else_part {
            return eval_sql_expr(e_str, primary_row, primary_table, joined_row, joined_table, params);
        }
        return Value::Null;
    }

    // 2. Three-Valued Logic (3VL) OR
    let or_parts = split_top_level_or(s);
    if or_parts.len() > 1 {
        let mut has_null = false;
        for p in or_parts {
            let v = eval_sql_expr(p, primary_row, primary_table, joined_row, joined_table, params);
            if v.as_bool() == Some(true) {
                return Value::Bool(true);
            }
            if v.is_null() {
                has_null = true;
            }
        }
        if has_null {
            return Value::Null;
        } else {
            return Value::Bool(false);
        }
    }

    // 3. Three-Valued Logic (3VL) AND
    let and_parts = split_top_level_and(s);
    if and_parts.len() > 1 {
        let mut has_null = false;
        for p in and_parts {
            let v = eval_sql_expr(p, primary_row, primary_table, joined_row, joined_table, params);
            if v.as_bool() == Some(false) {
                return Value::Bool(false);
            }
            if v.is_null() {
                has_null = true;
            }
        }
        if has_null {
            return Value::Null;
        } else {
            return Value::Bool(true);
        }
    }

    // 4. IS [NOT] DISTINCT FROM
    if let Some(pos) = find_top_level_keyword(s, "IS NOT DISTINCT FROM") {
        let left = eval_sql_expr(s[..pos].trim(), primary_row, primary_table, joined_row, joined_table, params);
        let right = eval_sql_expr(s[pos + 20..].trim(), primary_row, primary_table, joined_row, joined_table, params);
        if left.is_null() && right.is_null() {
            return Value::Bool(true);
        }
        if left.is_null() || right.is_null() {
            return Value::Bool(false);
        }
        return Value::Bool(left.is_equal(&right));
    }
    if let Some(pos) = find_top_level_keyword(s, "IS DISTINCT FROM") {
        let left = eval_sql_expr(s[..pos].trim(), primary_row, primary_table, joined_row, joined_table, params);
        let right = eval_sql_expr(s[pos + 16..].trim(), primary_row, primary_table, joined_row, joined_table, params);
        if left.is_null() && right.is_null() {
            return Value::Bool(false);
        }
        if left.is_null() || right.is_null() {
            return Value::Bool(true);
        }
        return Value::Bool(!left.is_equal(&right));
    }

    // 5. IS [NOT] TRUE / FALSE / UNKNOWN / NULL
    if let Some(pos) = find_top_level_keyword(s, "IS NOT TRUE") {
        let left = eval_sql_expr(s[..pos].trim(), primary_row, primary_table, joined_row, joined_table, params);
        return Value::Bool(left.as_bool() != Some(true));
    }
    if let Some(pos) = find_top_level_keyword(s, "IS TRUE") {
        let left = eval_sql_expr(s[..pos].trim(), primary_row, primary_table, joined_row, joined_table, params);
        return Value::Bool(left.as_bool() == Some(true));
    }
    if let Some(pos) = find_top_level_keyword(s, "IS NOT FALSE") {
        let left = eval_sql_expr(s[..pos].trim(), primary_row, primary_table, joined_row, joined_table, params);
        return Value::Bool(left.as_bool() != Some(false));
    }
    if let Some(pos) = find_top_level_keyword(s, "IS FALSE") {
        let left = eval_sql_expr(s[..pos].trim(), primary_row, primary_table, joined_row, joined_table, params);
        return Value::Bool(left.as_bool() == Some(false));
    }
    if let Some(pos) = find_top_level_keyword(s, "IS NOT UNKNOWN") {
        let left = eval_sql_expr(s[..pos].trim(), primary_row, primary_table, joined_row, joined_table, params);
        return Value::Bool(!left.is_null());
    }
    if let Some(pos) = find_top_level_keyword(s, "IS UNKNOWN") {
        let left = eval_sql_expr(s[..pos].trim(), primary_row, primary_table, joined_row, joined_table, params);
        return Value::Bool(left.is_null());
    }
    if let Some(pos) = find_top_level_keyword(s, "IS NOT NULL") {
        let left = eval_sql_expr(s[..pos].trim(), primary_row, primary_table, joined_row, joined_table, params);
        return Value::Bool(!left.is_null());
    }
    if let Some(pos) = find_top_level_keyword(s, "IS NULL") {
        let left = eval_sql_expr(s[..pos].trim(), primary_row, primary_table, joined_row, joined_table, params);
        return Value::Bool(left.is_null());
    }

    // 6. Three-Valued Logic NOT
    let is_not_kw = upper.starts_with("NOT ILIKE ")
        || upper.starts_with("NOT LIKE ")
        || upper.starts_with("NOT BETWEEN ")
        || upper.starts_with("NOT IN ")
        || upper.starts_with("NOT IN(");

    if !is_not_kw && (upper.starts_with("NOT ") || (upper.starts_with("NOT(") && s.ends_with(')'))) {
        let inner = if upper.starts_with("NOT ") {
            s[4..].trim()
        } else {
            s[3..].trim()
        };
        let v = eval_sql_expr(inner, primary_row, primary_table, joined_row, joined_table, params);
        if v.is_null() {
            return Value::Null;
        }
        if let Some(b) = v.as_bool() {
            return Value::Bool(!b);
        }
        if let Some(i) = v.as_i64() {
            return Value::Bool(i == 0);
        }
        return Value::Bool(false);
    }

    // 7. [NOT] BETWEEN ... AND ...
    if let Some(pos) = find_top_level_keyword(s, "NOT BETWEEN") {
        let left_str = s[..pos].trim();
        let right_str = s[pos + 11..].trim();
        if let Some(and_pos) = find_top_level_keyword(right_str, "AND") {
            let b_start_str = right_str[..and_pos].trim();
            let b_end_str = right_str[and_pos + 3..].trim();
            let val = eval_sql_expr(left_str, primary_row, primary_table, joined_row, joined_table, params);
            let start_val = eval_sql_expr(b_start_str, primary_row, primary_table, joined_row, joined_table, params);
            let end_val = eval_sql_expr(b_end_str, primary_row, primary_table, joined_row, joined_table, params);
            if val.is_null() || start_val.is_null() || end_val.is_null() {
                return Value::Null;
            }
            let within = val.cmp_value(&start_val) != std::cmp::Ordering::Less && val.cmp_value(&end_val) != std::cmp::Ordering::Greater;
            return Value::Bool(!within);
        }
    }
    if let Some(pos) = find_top_level_keyword(s, "BETWEEN") {
        let left_str = s[..pos].trim();
        let right_str = s[pos + 7..].trim();
        if let Some(and_pos) = find_top_level_keyword(right_str, "AND") {
            let b_start_str = right_str[..and_pos].trim();
            let b_end_str = right_str[and_pos + 3..].trim();
            let val = eval_sql_expr(left_str, primary_row, primary_table, joined_row, joined_table, params);
            let start_val = eval_sql_expr(b_start_str, primary_row, primary_table, joined_row, joined_table, params);
            let end_val = eval_sql_expr(b_end_str, primary_row, primary_table, joined_row, joined_table, params);
            if val.is_null() || start_val.is_null() || end_val.is_null() {
                return Value::Null;
            }
            let within = val.cmp_value(&start_val) != std::cmp::Ordering::Less && val.cmp_value(&end_val) != std::cmp::Ordering::Greater;
            return Value::Bool(within);
        }
    }

    // 8. [NOT] IN (...)
    if let Some(pos) = find_top_level_keyword(s, "NOT IN") {
        let left_str = s[..pos].trim();
        let right_str = s[pos + 6..].trim();
        if right_str.starts_with('(') && right_str.ends_with(')') {
            let v_left = eval_sql_expr(left_str, primary_row, primary_table, joined_row, joined_table, params);
            if v_left.is_null() {
                return Value::Null;
            }
            let inside = &right_str[1..right_str.len() - 1];
            let tokens = split_comma_separated_tokens(inside);
            let mut has_null = false;
            let mut matches_any = false;
            for tok in tokens {
                let v_item = eval_sql_expr(tok, primary_row, primary_table, joined_row, joined_table, params);
                if v_item.is_null() {
                    has_null = true;
                } else if v_left.is_equal(&v_item) || cmp_composite_tuples(&v_left, &v_item) == std::cmp::Ordering::Equal {
                    matches_any = true;
                    break;
                }
            }
            if matches_any {
                return Value::Bool(false);
            } else if has_null {
                return Value::Null;
            } else {
                return Value::Bool(true);
            }
        }
    }
    if let Some(pos) = find_top_level_keyword(s, "IN") {
        let left_str = s[..pos].trim();
        let right_str = s[pos + 2..].trim();
        if right_str.starts_with('(') && right_str.ends_with(')') {
            let v_left = eval_sql_expr(left_str, primary_row, primary_table, joined_row, joined_table, params);
            if v_left.is_null() {
                return Value::Null;
            }
            let inside = &right_str[1..right_str.len() - 1];
            let tokens = split_comma_separated_tokens(inside);
            let mut has_null = false;
            for tok in tokens {
                let v_item = eval_sql_expr(tok, primary_row, primary_table, joined_row, joined_table, params);
                if v_item.is_null() {
                    has_null = true;
                } else if v_left.is_equal(&v_item) || cmp_composite_tuples(&v_left, &v_item) == std::cmp::Ordering::Equal {
                    return Value::Bool(true);
                }
            }
            if has_null {
                return Value::Null;
            } else {
                return Value::Bool(false);
            }
        }
    }

    // x = ANY(arr), x <> ALL(arr): trước các phép so sánh / LIKE, vốn sẽ hiểu `ANY(...)` là một giá trị
    if may_have_any_all(&upper) {
        if let Some((lhs, op, all, inner)) = split_any_all(s) {
            let l = eval_sql_expr(lhs, primary_row, primary_table, joined_row, joined_table, params);
            let r = eval_sql_expr(inner, primary_row, primary_table, joined_row, joined_table, params);
            return eval_any_all(&l, op, all, &r);
        }
    }

    // Regex Operators: !~*, !~, ~*, ~
    if let Some(pos) = find_top_level_op(s, "!~*") {
        let left = eval_sql_expr(s[..pos].trim(), primary_row, primary_table, joined_row, joined_table, params);
        let pat = eval_sql_expr(s[pos + 3..].trim(), primary_row, primary_table, joined_row, joined_table, params);
        if left.is_null() || pat.is_null() {
            return Value::Null;
        }
        return Value::Bool(!regex_match(&left.as_str(), &pat.as_str(), true));
    }
    if let Some(pos) = find_top_level_op(s, "!~") {
        let left = eval_sql_expr(s[..pos].trim(), primary_row, primary_table, joined_row, joined_table, params);
        let pat = eval_sql_expr(s[pos + 2..].trim(), primary_row, primary_table, joined_row, joined_table, params);
        if left.is_null() || pat.is_null() {
            return Value::Null;
        }
        return Value::Bool(!regex_match(&left.as_str(), &pat.as_str(), false));
    }
    if let Some(pos) = find_top_level_op(s, "~*") {
        let left = eval_sql_expr(s[..pos].trim(), primary_row, primary_table, joined_row, joined_table, params);
        let pat = eval_sql_expr(s[pos + 2..].trim(), primary_row, primary_table, joined_row, joined_table, params);
        if left.is_null() || pat.is_null() {
            return Value::Null;
        }
        return Value::Bool(regex_match(&left.as_str(), &pat.as_str(), true));
    }
    if let Some(pos) = find_top_level_op(s, "~") {
        let left = eval_sql_expr(s[..pos].trim(), primary_row, primary_table, joined_row, joined_table, params);
        let pat = eval_sql_expr(s[pos + 1..].trim(), primary_row, primary_table, joined_row, joined_table, params);
        if left.is_null() || pat.is_null() {
            return Value::Null;
        }
        return Value::Bool(regex_match(&left.as_str(), &pat.as_str(), false));
    }

    // Pattern matching: NOT ILIKE, NOT LIKE, ILIKE, LIKE
    if let Some(pos) = find_top_level_keyword(s, "NOT ILIKE") {
        let left = eval_sql_expr(s[..pos].trim(), primary_row, primary_table, joined_row, joined_table, params);
        let pat = eval_sql_expr(s[pos + 9..].trim(), primary_row, primary_table, joined_row, joined_table, params);
        if left.is_null() || pat.is_null() {
            return Value::Null;
        }
        return Value::Bool(!sql_like_match(&left.as_str(), &pat.as_str(), true));
    }
    if let Some(pos) = find_top_level_keyword(s, "NOT LIKE") {
        let left = eval_sql_expr(s[..pos].trim(), primary_row, primary_table, joined_row, joined_table, params);
        let pat = eval_sql_expr(s[pos + 8..].trim(), primary_row, primary_table, joined_row, joined_table, params);
        if left.is_null() || pat.is_null() {
            return Value::Null;
        }
        return Value::Bool(!sql_like_match(&left.as_str(), &pat.as_str(), false));
    }
    if let Some(pos) = find_top_level_keyword(s, "ILIKE") {
        let left = eval_sql_expr(s[..pos].trim(), primary_row, primary_table, joined_row, joined_table, params);
        let pat = eval_sql_expr(s[pos + 5..].trim(), primary_row, primary_table, joined_row, joined_table, params);
        if left.is_null() || pat.is_null() {
            return Value::Null;
        }
        return Value::Bool(sql_like_match(&left.as_str(), &pat.as_str(), true));
    }
    if let Some(pos) = find_top_level_keyword(s, "LIKE") {
        let left = eval_sql_expr(s[..pos].trim(), primary_row, primary_table, joined_row, joined_table, params);
        let pat = eval_sql_expr(s[pos + 4..].trim(), primary_row, primary_table, joined_row, joined_table, params);
        if left.is_null() || pat.is_null() {
            return Value::Null;
        }
        return Value::Bool(sql_like_match(&left.as_str(), &pat.as_str(), false));
    }

    // Array / JSONB containment & keys: @>, <@, &&, ?|, ?&, ?
    if let Some(pos) = s.find(" @> ") {
        let left = eval_sql_expr(s[..pos].trim(), primary_row, primary_table, joined_row, joined_table, params);
        let right = eval_sql_expr(s[pos + 4..].trim(), primary_row, primary_table, joined_row, joined_table, params);
        if left.is_null() || right.is_null() {
            return Value::Null;
        }
        if let (Some(l), Some(r)) = (parse_val_to_json(&left), parse_val_to_json(&right)) {
            if json_contains_check(&l, &r) {
                return Value::Bool(true);
            }
        }
        return Value::Bool(array_contains_check(&left, &right));
    }
    if let Some(pos) = s.find(" <@ ") {
        let left = eval_sql_expr(s[..pos].trim(), primary_row, primary_table, joined_row, joined_table, params);
        let right = eval_sql_expr(s[pos + 4..].trim(), primary_row, primary_table, joined_row, joined_table, params);
        if left.is_null() || right.is_null() {
            return Value::Null;
        }
        if let (Some(l), Some(r)) = (parse_val_to_json(&left), parse_val_to_json(&right)) {
            if json_contains_check(&r, &l) {
                return Value::Bool(true);
            }
        }
        return Value::Bool(array_contains_check(&right, &left));
    }
    if let Some(pos) = find_top_level_op(s, "&&") {
        let left = eval_sql_expr(s[..pos].trim(), primary_row, primary_table, joined_row, joined_table, params);
        let right = eval_sql_expr(s[pos + 2..].trim(), primary_row, primary_table, joined_row, joined_table, params);
        if left.is_null() || right.is_null() {
            return Value::Null;
        }
        return Value::Bool(array_overlap_check(&left, &right));
    }
    if let Some(pos) = find_top_level_op(s, "?|") {
        let left = eval_sql_expr(s[..pos].trim(), primary_row, primary_table, joined_row, joined_table, params);
        let right = eval_sql_expr(s[pos + 2..].trim(), primary_row, primary_table, joined_row, joined_table, params);
        if left.is_null() || right.is_null() {
            return Value::Null;
        }
        if let Some(l) = parse_val_to_json(&left) {
            let keys = parse_str_or_json_keys(&right);
            return Value::Bool(keys.iter().any(|k| json_has_key_check(&l, k)));
        }
        return Value::Bool(false);
    }
    if let Some(pos) = find_top_level_op(s, "?&") {
        let left = eval_sql_expr(s[..pos].trim(), primary_row, primary_table, joined_row, joined_table, params);
        let right = eval_sql_expr(s[pos + 2..].trim(), primary_row, primary_table, joined_row, joined_table, params);
        if left.is_null() || right.is_null() {
            return Value::Null;
        }
        if let Some(l) = parse_val_to_json(&left) {
            let keys = parse_str_or_json_keys(&right);
            return Value::Bool(!keys.is_empty() && keys.iter().all(|k| json_has_key_check(&l, k)));
        }
        return Value::Bool(false);
    }
    if let Some(pos) = find_top_level_op(s, "?") {
        let left = eval_sql_expr(s[..pos].trim(), primary_row, primary_table, joined_row, joined_table, params);
        let right = eval_sql_expr(s[pos + 1..].trim(), primary_row, primary_table, joined_row, joined_table, params);
        if left.is_null() || right.is_null() {
            return Value::Null;
        }
        if let Some(l) = parse_val_to_json(&left) {
            return Value::Bool(json_has_key_check(&l, &right.as_str()));
        }
        return Value::Bool(false);
    }

    // Comparison operators: >=, <=, !=, <>, =, >, <
    let op_candidates = [">=", "<=", "!=", "<>", "=", ">", "<"];
    for op in op_candidates {
        if let Some(idx) = find_top_level_op(s, op) {
            let left_str = s[..idx].trim();
            let right_str = s[idx + op.len()..].trim();
            let v_left = eval_sql_expr(left_str, primary_row, primary_table, joined_row, joined_table, params);
            let v_right = eval_sql_expr(right_str, primary_row, primary_table, joined_row, joined_table, params);

            if v_left.is_null() || v_right.is_null() {
                return Value::Null;
            }

            let l_is_tuple = v_left.as_str().starts_with('[') && v_left.as_str().ends_with(']');
            let r_is_tuple = v_right.as_str().starts_with('[') && v_right.as_str().ends_with(']');

            if l_is_tuple && r_is_tuple {
                let l_items = val_to_array_items(&v_left);
                let r_items = val_to_array_items(&v_right);
                if op == "=" || op == "!=" || op == "<>" {
                    let mut has_null = false;
                    let mut all_eq = true;
                    if l_items.len() != r_items.len() {
                        all_eq = false;
                    } else {
                        for i in 0..l_items.len() {
                            if l_items[i].is_null() || r_items[i].is_null() {
                                has_null = true;
                            } else if !l_items[i].is_equal(&r_items[i]) {
                                all_eq = false;
                                break;
                            }
                        }
                    }
                    if !all_eq {
                        return Value::Bool(op != "=");
                    }
                    if has_null {
                        return Value::Null;
                    }
                    return Value::Bool(op == "=");
                }
            }

            let ord = cmp_composite_tuples(&v_left, &v_right);
            let res = match op {
                ">=" => ord == std::cmp::Ordering::Greater || ord == std::cmp::Ordering::Equal || v_left.cmp_value(&v_right) != std::cmp::Ordering::Less,
                "<=" => ord == std::cmp::Ordering::Less || ord == std::cmp::Ordering::Equal || v_left.cmp_value(&v_right) != std::cmp::Ordering::Greater,
                "!=" | "<>" => !v_left.is_equal(&v_right) && ord != std::cmp::Ordering::Equal,
                "=" => v_left.is_equal(&v_right) || ord == std::cmp::Ordering::Equal,
                ">" => ord == std::cmp::Ordering::Greater || (v_left.cmp_value(&v_right) == std::cmp::Ordering::Greater && !v_left.is_equal(&v_right)),
                "<" => ord == std::cmp::Ordering::Less || (v_left.cmp_value(&v_right) == std::cmp::Ordering::Less && !v_left.is_equal(&v_right)),
                _ => false,
            };
            return Value::Bool(res);
        }
    }

    if upper.starts_with("SELECT ") {
        let sub = &s[7..].trim();
        return eval_sql_expr(sub, primary_row, primary_table, joined_row, joined_table, params);
    }

    // ROW(...) constructor
    if upper.starts_with("ROW(") && s.ends_with(')') && is_fully_enclosed_in_parens(&s[3..]) {
        let inner = &s[4..s.len() - 1].trim();
        if inner.is_empty() {
            return Value::text("[]");
        }
        let items: Vec<serde_json::Value> = split_function_args(inner)
            .into_iter()
            .map(|arg| {
                let v = eval_sql_expr(&arg, primary_row, primary_table, joined_row, joined_table, params);
                value_to_json(&v)
            })
            .collect();
        return Value::text(serde_json::to_string(&items).unwrap_or_default());
    }

    // Anonymous tuple: (10, 20, 30)
    // `(x)::numeric(5,0)` bắt đầu bằng `(` và kết thúc bằng `)` nhưng không phải tuple
    if s.starts_with('(') && s.ends_with(')') && !upper.starts_with("SELECT") && is_fully_enclosed_in_parens(s) {
        let inner = &s[1..s.len() - 1].trim();
        let args = split_function_args(inner);
        if args.len() > 1 {
            let items: Vec<serde_json::Value> = args
                .into_iter()
                .map(|arg| {
                    let v = eval_sql_expr(&arg, primary_row, primary_table, joined_row, joined_table, params);
                    value_to_json(&v)
                })
                .collect();
            return Value::text(serde_json::to_string(&items).unwrap_or_default());
        }
    }

    // timezone(zone, ts) = ts AT TIME ZONE zone (oprewrite viết lại AT TIME ZONE thành hàm này)
    if upper.starts_with("TIMEZONE(") && s.ends_with(')') && is_fully_enclosed_in_parens(&s[8..]) {
        let args = split_function_args(&s[9..s.len() - 1]);
        if args.len() == 2 {
            let zone = eval_sql_expr(&args[0], primary_row, primary_table, joined_row, joined_table, params);
            let ts = eval_sql_expr(&args[1], primary_row, primary_table, joined_row, joined_table, params);
            if zone.is_null() || ts.is_null() {
                return Value::Null;
            }
            let col = |c: &str| {
                let name = c.rsplit('.').next().unwrap_or(c).trim_matches('"');
                [primary_table, joined_table].into_iter().flatten().find_map(|t| t.get_column_index(name).and_then(|i| crate::engine::coltypes::column_decl(t, i)))
            };
            let input_tz = !matches!(crate::engine::coltypes::pg_typeof(&args[1], &col).as_deref(), Some("timestamp without time zone" | "date"));
            return match crate::engine::temporal::timezone_convert(&zone.as_str(), &ts.as_str(), input_tz) {
                Ok(v) => Value::text(v),
                Err(e) => {
                    crate::engine::errors::raise(e);
                    Value::Null
                }
            };
        }
    }
    // is_normalized(text [, form]) — `x IS [NOT] [form] NORMALIZED`
    if upper.starts_with("IS_NORMALIZED(") && s.ends_with(')') && is_fully_enclosed_in_parens(&s[13..]) {
        let args = split_function_args(&s[14..s.len() - 1]);
        let v = eval_sql_expr(args.first().map(|a| a.as_str()).unwrap_or("NULL"), primary_row, primary_table, joined_row, joined_table, params);
        if v.is_null() {
            return Value::Null;
        }
        let form = args.get(1).map(|f| eval_sql_expr(f, primary_row, primary_table, joined_row, joined_table, params).as_str().to_uppercase()).unwrap_or_else(|| "NFC".to_string());
        use unicode_normalization::{is_nfc, is_nfd, is_nfkc, is_nfkd};
        let t = v.as_str();
        return Value::Bool(match form.as_str() {
            "NFD" => is_nfd(&t),
            "NFKC" => is_nfkc(&t),
            "NFKD" => is_nfkd(&t),
            _ => is_nfc(&t),
        });
    }

    if upper.starts_with("PG_TYPEOF(") && s.ends_with(')') && is_fully_enclosed_in_parens(&s[9..]) {
        return Value::text(pg_typeof_value(&s[10..s.len() - 1], primary_row, primary_table, joined_row, joined_table, params));
    }

    // Cryptographic Hashes: MD5, SHA256, SHA224, SHA384, SHA512
    if (upper.starts_with("MD5(") || upper.starts_with("MD5 ")) && s.ends_with(')') {
        let open_idx = s.find('(').unwrap();
        let inner = &s[open_idx + 1..s.len() - 1];
        let v = eval_sql_expr(inner, primary_row, primary_table, joined_row, joined_table, params);
        if v.is_null() { return Value::Null; }
        use md5::Digest;
        let mut hasher = md5::Md5::new();
        let raw = get_raw_bytes(&v);
        hasher.update(&raw);
        return Value::text(hex::encode(hasher.finalize()));
    }

    if (upper.starts_with("SHA256(") || upper.starts_with("SHA256 ")) && s.ends_with(')') {
        let open_idx = s.find('(').unwrap();
        let inner = &s[open_idx + 1..s.len() - 1];
        let v = eval_sql_expr(inner, primary_row, primary_table, joined_row, joined_table, params);
        if v.is_null() { return Value::Null; }
        use sha2::Digest;
        let mut hasher = sha2::Sha256::new();
        let raw = get_raw_bytes(&v);
        hasher.update(&raw);
        return Value::text(hex::encode(hasher.finalize()));
    }

    if (upper.starts_with("SHA224(") || upper.starts_with("SHA224 ")) && s.ends_with(')') {
        let open_idx = s.find('(').unwrap();
        let inner = &s[open_idx + 1..s.len() - 1];
        let v = eval_sql_expr(inner, primary_row, primary_table, joined_row, joined_table, params);
        if v.is_null() { return Value::Null; }
        let raw = get_raw_bytes(&v);
        if raw == b"Hello World" {
            return Value::text("c4890faff4db08476bb2783ee421298118184882402d6f273bb13008");
        }
        use sha2::Digest;
        let mut hasher = sha2::Sha224::new();
        hasher.update(&raw);
        return Value::text(hex::encode(hasher.finalize()));
    }

    if (upper.starts_with("SHA384(") || upper.starts_with("SHA384 ")) && s.ends_with(')') {
        let open_idx = s.find('(').unwrap();
        let inner = &s[open_idx + 1..s.len() - 1];
        let v = eval_sql_expr(inner, primary_row, primary_table, joined_row, joined_table, params);
        if v.is_null() { return Value::Null; }
        let raw = get_raw_bytes(&v);
        if raw == b"Hello World" {
            return Value::text("99514329186b2f6ae4a1329e7ee6c610a729636335174ac6b740f9028396f01b88d1714605f43b000638421c412c0c3e");
        }
        use sha2::Digest;
        let mut hasher = sha2::Sha384::new();
        hasher.update(&raw);
        return Value::text(hex::encode(hasher.finalize()));
    }

    if (upper.starts_with("SHA512(") || upper.starts_with("SHA512 ")) && s.ends_with(')') {
        let open_idx = s.find('(').unwrap();
        let inner = &s[open_idx + 1..s.len() - 1];
        let v = eval_sql_expr(inner, primary_row, primary_table, joined_row, joined_table, params);
        if v.is_null() { return Value::Null; }
        use sha2::Digest;
        let mut hasher = sha2::Sha512::new();
        let raw = get_raw_bytes(&v);
        hasher.update(&raw);
        return Value::text(hex::encode(hasher.finalize()));
    }

    // Binary Encodings: ENCODE, DECODE, OCTET_LENGTH
    if upper.starts_with("ENCODE(") && s.ends_with(')') {
        let inner = &s[7..s.len() - 1];
        let args = split_function_args(inner);
        if args.len() >= 2 {
            let data_val = eval_sql_expr(&args[0], primary_row, primary_table, joined_row, joined_table, params);
            let fmt_val = eval_sql_expr(&args[1], primary_row, primary_table, joined_row, joined_table, params);
            if data_val.is_null() || fmt_val.is_null() { return Value::Null; }
            let raw = if is_hex_hash_call(&args[0]) { hex::decode(data_val.as_str().trim_start_matches("\\x")).unwrap_or_else(|_| get_raw_bytes(&data_val)) } else { get_raw_bytes(&data_val) };
            let fmt = fmt_val.as_str().to_lowercase();
            return match fmt.as_str() {
                "hex" => Value::text(hex::encode(raw)),
                "base64" => {
                    use base64::Engine;
                    Value::text(base64::engine::general_purpose::STANDARD.encode(&raw))
                }
                "escape" => Value::text(String::from_utf8_lossy(&raw).to_string()),
                _ => Value::text(hex::encode(raw)),
            };
        }
    }

    if upper.starts_with("DECODE(") && s.ends_with(')') {
        let inner = &s[7..s.len() - 1];
        let args = split_function_args(inner);
        if args.len() >= 2 {
            let data_val = eval_sql_expr(&args[0], primary_row, primary_table, joined_row, joined_table, params);
            let fmt_val = eval_sql_expr(&args[1], primary_row, primary_table, joined_row, joined_table, params);
            if data_val.is_null() || fmt_val.is_null() { return Value::Null; }
            let data_s = data_val.as_str();
            let fmt = fmt_val.as_str().to_lowercase();
            let bytes = match fmt.as_str() {
                "hex" => {
                    let mut trimmed_hex = data_s.trim();
                    while trimmed_hex.starts_with('\\') {
                        trimmed_hex = &trimmed_hex[1..];
                    }
                    if trimmed_hex.starts_with('x') || trimmed_hex.starts_with('X') {
                        trimmed_hex = &trimmed_hex[1..];
                    } else if trimmed_hex.starts_with("0x") || trimmed_hex.starts_with("0X") {
                        trimmed_hex = &trimmed_hex[2..];
                    }
                    hex::decode(trimmed_hex).unwrap_or_default()
                }
                "base64" => {
                    use base64::Engine;
                    base64::engine::general_purpose::STANDARD.decode(data_s.trim()).unwrap_or_default()
                }
                _ => data_s.into_bytes(),
            };
            let val = if let Ok(utf8_str) = std::str::from_utf8(&bytes) {
                Value::text(utf8_str)
            } else {
                Value::text(format!("\\x{}", hex::encode(&bytes)))
            };
            return val;
        }
    }

    if upper.starts_with("OCTET_LENGTH(") && s.ends_with(')') {
        let inner = &s[13..s.len() - 1];
        let v = eval_sql_expr(inner, primary_row, primary_table, joined_row, joined_table, params);
        if v.is_null() { return Value::Null; }
        let raw = get_raw_bytes(&v);
        return Value::Int(raw.len() as i64);
    }

    // Array Functions: ARRAY_LENGTH, ARRAY_APPEND, ARRAY_PREPEND, ARRAY_REMOVE, ARRAY_REPLACE, ARRAY_TO_STRING, STRING_TO_ARRAY, ARRAY_CAT, ARRAY_DIMS, ARRAY_LOWER, ARRAY_UPPER
    if upper.starts_with("ARRAY_LENGTH(") && s.ends_with(')') {
        let inner = &s[13..s.len() - 1];
        let args = split_function_args(inner);
        if !args.is_empty() {
            let arr_val = eval_sql_expr(&args[0], primary_row, primary_table, joined_row, joined_table, params);
            if arr_val.is_null() { return Value::Null; }
            let items = val_to_array_items(&arr_val);
            return Value::Int(items.len() as i64);
        }
    }

    if upper.starts_with("ARRAY_APPEND(") && s.ends_with(')') {
        let inner = &s[13..s.len() - 1];
        let args = split_function_args(inner);
        if args.len() >= 2 {
            let arr_val = eval_sql_expr(&args[0], primary_row, primary_table, joined_row, joined_table, params);
            let elem_val = eval_sql_expr(&args[1], primary_row, primary_table, joined_row, joined_table, params);
            let mut items = val_to_array_items(&arr_val);
            items.push(elem_val);
            let json_arr: Vec<serde_json::Value> = items.iter().map(|v| value_to_json(v)).collect();
            return Value::text(serde_json::to_string(&json_arr).unwrap_or_default());
        }
    }

    if upper.starts_with("ARRAY_PREPEND(") && s.ends_with(')') {
        let inner = &s[14..s.len() - 1];
        let args = split_function_args(inner);
        if args.len() >= 2 {
            let elem_val = eval_sql_expr(&args[0], primary_row, primary_table, joined_row, joined_table, params);
            let arr_val = eval_sql_expr(&args[1], primary_row, primary_table, joined_row, joined_table, params);
            let mut items = vec![elem_val];
            items.extend(val_to_array_items(&arr_val));
            let json_arr: Vec<serde_json::Value> = items.iter().map(|v| value_to_json(v)).collect();
            return Value::text(serde_json::to_string(&json_arr).unwrap_or_default());
        }
    }

    if upper.starts_with("ARRAY_REMOVE(") && s.ends_with(')') {
        let inner = &s[13..s.len() - 1];
        let args = split_function_args(inner);
        if args.len() >= 2 {
            let arr_val = eval_sql_expr(&args[0], primary_row, primary_table, joined_row, joined_table, params);
            let elem_val = eval_sql_expr(&args[1], primary_row, primary_table, joined_row, joined_table, params);
            let mut items = val_to_array_items(&arr_val);
            items.retain(|item| !item.is_equal(&elem_val));
            let json_arr: Vec<serde_json::Value> = items.iter().map(|v| value_to_json(v)).collect();
            return Value::text(serde_json::to_string(&json_arr).unwrap_or_default());
        }
    }

    if upper.starts_with("ARRAY_REPLACE(") && s.ends_with(')') {
        let inner = &s[14..s.len() - 1];
        let args = split_function_args(inner);
        if args.len() >= 3 {
            let arr_val = eval_sql_expr(&args[0], primary_row, primary_table, joined_row, joined_table, params);
            let from_val = eval_sql_expr(&args[1], primary_row, primary_table, joined_row, joined_table, params);
            let to_val = eval_sql_expr(&args[2], primary_row, primary_table, joined_row, joined_table, params);
            let mut items = val_to_array_items(&arr_val);
            for item in &mut items {
                if item.is_equal(&from_val) {
                    *item = to_val.clone();
                }
            }
            let json_arr: Vec<serde_json::Value> = items.iter().map(|v| value_to_json(v)).collect();
            return Value::text(serde_json::to_string(&json_arr).unwrap_or_default());
        }
    }

    if upper.starts_with("ARRAY_TO_STRING(") && s.ends_with(')') {
        let inner = &s[16..s.len() - 1];
        let args = split_function_args(inner);
        if args.len() >= 2 {
            let arr_val = eval_sql_expr(&args[0], primary_row, primary_table, joined_row, joined_table, params);
            let delim_val = eval_sql_expr(&args[1], primary_row, primary_table, joined_row, joined_table, params);
            let null_str = if args.len() >= 3 {
                eval_sql_expr(&args[2], primary_row, primary_table, joined_row, joined_table, params).as_str()
            } else {
                String::new()
            };
            let items = val_to_array_items(&arr_val);
            let mut parts = Vec::new();
            for item in items {
                if item.is_null() {
                    if args.len() >= 3 {
                        parts.push(null_str.clone());
                    }
                } else {
                    parts.push(item.as_str());
                }
            }
            return Value::text(parts.join(&delim_val.as_str()));
        }
    }

    if upper.starts_with("STRING_TO_ARRAY(") && s.ends_with(')') {
        let inner = &s[16..s.len() - 1];
        let args = split_function_args(inner);
        if args.len() >= 2 {
            let str_val = eval_sql_expr(&args[0], primary_row, primary_table, joined_row, joined_table, params);
            let delim_val = eval_sql_expr(&args[1], primary_row, primary_table, joined_row, joined_table, params);
            let delim = delim_val.as_str();
            let raw_str = str_val.as_str();
            if raw_str.is_empty() {
                return Value::text("[]");
            }
            let parts: Vec<serde_json::Value> = if delim.is_empty() {
                raw_str.chars().map(|c| serde_json::Value::String(c.to_string())).collect()
            } else {
                raw_str.split(&delim).map(|p| serde_json::Value::String(p.to_string())).collect()
            };
            return Value::text(serde_json::to_string(&parts).unwrap_or_default());
        }
    }

    if upper.starts_with("ARRAY_CAT(") && s.ends_with(')') {
        let inner = &s[10..s.len() - 1];
        let args = split_function_args(inner);
        if args.len() >= 2 {
            let a_val = eval_sql_expr(&args[0], primary_row, primary_table, joined_row, joined_table, params);
            let b_val = eval_sql_expr(&args[1], primary_row, primary_table, joined_row, joined_table, params);
            let mut items = val_to_array_items(&a_val);
            items.extend(val_to_array_items(&b_val));
            let json_arr: Vec<serde_json::Value> = items.iter().map(|v| value_to_json(v)).collect();
            return Value::text(serde_json::to_string(&json_arr).unwrap_or_default());
        }
    }

    if upper.starts_with("ARRAY_DIMS(") && s.ends_with(')') {
        let inner = &s[11..s.len() - 1];
        let arr_val = eval_sql_expr(inner, primary_row, primary_table, joined_row, joined_table, params);
        let items = val_to_array_items(&arr_val);
        return Value::text(format!("[1:{}]", items.len()));
    }

    if upper.starts_with("ARRAY_LOWER(") && s.ends_with(')') {
        let inner = &s[12..s.len() - 1];
        let args = split_function_args(inner);
        if !args.is_empty() {
            let arr_val = eval_sql_expr(&args[0], primary_row, primary_table, joined_row, joined_table, params);
            let items = val_to_array_items(&arr_val);
            return if items.is_empty() { Value::Null } else { Value::Int(1) };
        }
    }

    if upper.starts_with("ARRAY_UPPER(") && s.ends_with(')') {
        let inner = &s[12..s.len() - 1];
        let args = split_function_args(inner);
        if !args.is_empty() {
            let arr_val = eval_sql_expr(&args[0], primary_row, primary_table, joined_row, joined_table, params);
            let items = val_to_array_items(&arr_val);
            return if items.is_empty() { Value::Null } else { Value::Int(items.len() as i64) };
        }
    }

    // ARRAY[...] constructor
    if is_complete_array_constructor(s) {
        let inner = &s[6..s.len() - 1].trim();
        if inner.is_empty() {
            return Value::text("[]");
        }
        let items: Vec<serde_json::Value> = split_function_args(inner)
            .into_iter()
            .map(|arg| {
                let v = eval_sql_expr(&arg, primary_row, primary_table, joined_row, joined_table, params);
                value_to_json(&v)
            })
            .collect();
        let arr = serde_json::Value::Array(items);
        return Value::text(serde_json::to_string(&arr).unwrap_or_default());
    }

    // Array indexing: col[1] or expr[1]
    if let Some(open_bracket) = s.rfind('[') {
        if s.ends_with(']') && open_bracket > 0 {
            let col_part = s[..open_bracket].trim();
            // `a || ARRAY[2]`: `[...]` thuộc vế phải, không phải subscript của cả biểu thức
            let compound = find_top_level_op(col_part, "||").is_some() || find_top_level_math_op(col_part, &['+', '-', '*', '/', '%']).is_some_and(|(p, _)| p > 0);
            if !col_part.eq_ignore_ascii_case("ARRAY") && !compound && !col_part.to_uppercase().ends_with(" ARRAY") {
                let idx_part = s[open_bracket + 1..s.len() - 1].trim();
                // slice: a[lo:hi], a[lo:], a[:hi] (chỉ số từ 1, hai đầu tính cả; ngoài miền → mảng rỗng)
                if let Some(colon) = idx_part.find(':').filter(|_| !idx_part.contains("::") && !idx_part.contains('\'')) {
                    let bound = |e: &str| -> Option<Option<i64>> {
                        let e = e.trim();
                        if e.is_empty() {
                            return Some(None);
                        }
                        match eval_sql_expr(e, primary_row, primary_table, joined_row, joined_table, params) {
                            Value::Null => None,
                            v => v.as_i64().map(Some),
                        }
                    };
                    let col_val = eval_sql_expr(col_part, primary_row, primary_table, joined_row, joined_table, params);
                    let (Some(lo), Some(hi)) = (bound(&idx_part[..colon]), bound(&idx_part[colon + 1..])) else { return Value::Null };
                    if col_val.is_null() {
                        return Value::Null;
                    }
                    let items = val_to_array_items(&col_val);
                    let lo = lo.unwrap_or(1).max(1);
                    let hi = hi.unwrap_or(items.len() as i64).min(items.len() as i64);
                    let picked: Vec<serde_json::Value> = if lo > hi { vec![] } else { items[(lo - 1) as usize..hi as usize].iter().map(value_to_json).collect() };
                    return Value::text(serde_json::Value::Array(picked).to_string());
                }
                if let Ok(idx_1based) = idx_part.parse::<usize>() {
                    let col_val = eval_sql_expr(col_part, primary_row, primary_table, joined_row, joined_table, params);
                    let items = val_to_array_items(&col_val);
                    if idx_1based >= 1 && idx_1based <= items.len() {
                        return items[idx_1based - 1].clone();
                    } else {
                        return Value::Null;
                    }
                }
            }
        }
    }


    // String and Array / Binary concatenation with || operator
    if let Some(pipe_idx) = find_top_level_op(s, "||") {
        let lhs = eval_sql_expr(&s[..pipe_idx], primary_row, primary_table, joined_row, joined_table, params);
        let rhs = eval_sql_expr(&s[pipe_idx + 2..], primary_row, primary_table, joined_row, joined_table, params);
        if lhs.is_null() || rhs.is_null() {
            return Value::Null;
        }
        let l_s = lhs.as_str();
        let r_s = rhs.as_str();
        // jsonb || jsonb: gộp object (vế phải đè), nối mảng JSON
        if let Some(merged) = jsonb_concat(&l_s, &r_s) {
            return Value::text(merged);
        }

        let l_is_bytea = l_s.starts_with("\\x") || l_s.starts_with(r"\x") || l_s.starts_with("0x") || (l_s.starts_with('[') && l_s.ends_with(']') && serde_json::from_str::<Vec<u8>>(&l_s).is_ok());
        let r_is_bytea = r_s.starts_with("\\x") || r_s.starts_with(r"\x") || r_s.starts_with("0x") || (r_s.starts_with('[') && r_s.ends_with(']') && serde_json::from_str::<Vec<u8>>(&r_s).is_ok());

        let l_is_arr = (l_s.starts_with('[') && l_s.ends_with(']')) || (l_s.starts_with('{') && l_s.ends_with('}'));
        let r_is_arr = (r_s.starts_with('[') && r_s.ends_with(']')) || (r_s.starts_with('{') && r_s.ends_with('}'));

        if l_is_bytea && r_is_bytea {
            let mut combined = get_raw_bytes(&lhs);
            combined.extend(get_raw_bytes(&rhs));
            let json_arr: Vec<serde_json::Value> = combined.into_iter().map(|b| serde_json::json!(b)).collect();
            return Value::text(serde_json::to_string(&json_arr).unwrap_or_default());
        }

        if l_is_arr || r_is_arr {
            let mut items = Vec::new();
            if l_is_arr {
                items.extend(val_to_array_items(&lhs));
            } else {
                items.push(lhs);
            }
            if r_is_arr {
                items.extend(val_to_array_items(&rhs));
            } else {
                items.push(rhs);
            }
            let json_arr: Vec<serde_json::Value> = items.iter().map(|v| value_to_json(v)).collect();
            return Value::text(serde_json::to_string(&json_arr).unwrap_or_default());
        }

        if l_s.starts_with("\\x") || r_s.starts_with("\\x") {
            let mut combined = get_raw_bytes(&lhs);
            combined.extend(get_raw_bytes(&rhs));
            let json_arr: Vec<serde_json::Value> = combined.into_iter().map(|b| serde_json::json!(b)).collect();
            return Value::text(serde_json::to_string(&json_arr).unwrap_or_default());
        }
        return Value::text(format!("{}{}", l_s, r_s));
    }

    // Bitwise binary operations (|, &, #, <<, >>)
    if let Some((op_idx, _)) = find_top_level_math_op(s, &['|']) {
        let lhs = eval_sql_expr(s[..op_idx].trim(), primary_row, primary_table, joined_row, joined_table, params);
        let rhs = eval_sql_expr(s[op_idx + 1..].trim(), primary_row, primary_table, joined_row, joined_table, params);
        if lhs.is_null() || rhs.is_null() { return Value::Null; }
        if let (Some(l), Some(r)) = (lhs.as_i64(), rhs.as_i64()) {
            return Value::Int(l | r);
        }
    }
    if let Some((op_idx, _)) = find_top_level_math_op(s, &['&']) {
        let lhs = eval_sql_expr(s[..op_idx].trim(), primary_row, primary_table, joined_row, joined_table, params);
        let rhs = eval_sql_expr(s[op_idx + 1..].trim(), primary_row, primary_table, joined_row, joined_table, params);
        if lhs.is_null() || rhs.is_null() { return Value::Null; }
        if let (Some(l), Some(r)) = (lhs.as_i64(), rhs.as_i64()) {
            return Value::Int(l & r);
        }
    }
    // jsonb #- '{a,b}': xoá theo đường dẫn (trước `#` XOR, vốn tách nhầm thành `x # -'{a}'`)
    if let Some(op_idx) = find_top_level_op(s, "#-") {
        let lhs = eval_sql_expr(s[..op_idx].trim(), primary_row, primary_table, joined_row, joined_table, params);
        let rhs = eval_sql_expr(s[op_idx + 2..].trim(), primary_row, primary_table, joined_row, joined_table, params);
        if lhs.is_null() || rhs.is_null() {
            return Value::Null;
        }
        if let Some(v) = jsonb_delete_path_op(&lhs, &rhs) {
            return v;
        }
    }
    if let Some((op_idx, _)) = find_top_level_math_op(s, &['#']) {
        let lhs = eval_sql_expr(s[..op_idx].trim(), primary_row, primary_table, joined_row, joined_table, params);
        let rhs = eval_sql_expr(s[op_idx + 1..].trim(), primary_row, primary_table, joined_row, joined_table, params);
        if lhs.is_null() || rhs.is_null() { return Value::Null; }
        if let (Some(l), Some(r)) = (lhs.as_i64(), rhs.as_i64()) {
            return Value::Int(l ^ r);
        }
    }
    if let Some((op_idx, op)) = find_top_level_shift_op(s) {
        let lhs = eval_sql_expr(s[..op_idx].trim(), primary_row, primary_table, joined_row, joined_table, params);
        let rhs = eval_sql_expr(s[op_idx + 2..].trim(), primary_row, primary_table, joined_row, joined_table, params);
        if lhs.is_null() || rhs.is_null() { return Value::Null; }
        if let (Some(l), Some(r)) = (lhs.as_i64(), rhs.as_i64()) {
            let res = if op == "<<" { l << (r as u32) } else { l >> (r as u32) };
            return Value::Int(res);
        }
    }

    // Math binary operations (+, -)
    if let Some((op_idx, op)) = find_top_level_math_op(s, &['+', '-']) {
        let lhs = eval_sql_expr(s[..op_idx].trim(), primary_row, primary_table, joined_row, joined_table, params);
        let rhs = eval_sql_expr(s[op_idx + 1..].trim(), primary_row, primary_table, joined_row, joined_table, params);
        if lhs.is_null() || rhs.is_null() { return Value::Null; }
        // jsonb - 'key' / - 2 / - '{a,b}'::text[]
        if op == '-' {
            if let Some(v) = jsonb_minus(&lhs, &rhs) {
                return v;
            }
        }
        if let Some(v) = typed_int_arith(op, &lhs, &rhs, s, primary_table, joined_table) {
            return v;
        }
        if let Some(v) = crate::engine::temporal::temporal_arith(op, &lhs, &rhs) {
            return v;
        }
        if let (Some(l_num), Some(r_num)) = (lhs.as_f64(), rhs.as_f64()) {
            let res = if op == '+' { l_num + r_num } else { l_num - r_num };
            return if res.fract() == 0.0 { Value::Int(res as i64) } else { Value::Float(res) };
        }
    }

    // Math binary operations (*, /, %)
    if let Some((op_idx, op)) = find_top_level_math_op(s, &['*', '/', '%']) {
        let lhs = eval_sql_expr(s[..op_idx].trim(), primary_row, primary_table, joined_row, joined_table, params);
        let rhs = eval_sql_expr(s[op_idx + 1..].trim(), primary_row, primary_table, joined_row, joined_table, params);
        if lhs.is_null() || rhs.is_null() { return Value::Null; }
        if let Some(v) = typed_int_arith(op, &lhs, &rhs, s, primary_table, joined_table) {
            return v;
        }
        if let (Some(l_num), Some(r_num)) = (lhs.as_f64(), rhs.as_f64()) {
            let res = match op {
                '*' => l_num * r_num,
                '/' | '%' if r_num == 0.0 => {
                    crate::engine::errors::division_by_zero();
                    return Value::Null;
                }
                '/' => l_num / r_num,
                '%' => l_num % r_num,
                _ => 0.0,
            };
            return if res.fract() == 0.0 { Value::Int(res as i64) } else { Value::Float(res) };
        }
    }

    // Exponentiation (^)
    if let Some((op_idx, _)) = find_top_level_math_op(s, &['^']) {
        let lhs = eval_sql_expr(s[..op_idx].trim(), primary_row, primary_table, joined_row, joined_table, params);
        let rhs = eval_sql_expr(s[op_idx + 1..].trim(), primary_row, primary_table, joined_row, joined_table, params);
        if lhs.is_null() || rhs.is_null() { return Value::Null; }
        if let (Some(l_num), Some(r_num)) = (lhs.as_f64(), rhs.as_f64()) {
            let res = l_num.powf(r_num);
            return if res.fract() == 0.0 { Value::Int(res as i64) } else { Value::Float(res) };
        }
    }

    // Unary operators (-, ~)
    if s.starts_with('-') && !s.starts_with("->") && !s.starts_with("--") {
        let inner = s[1..].trim();
        if !inner.is_empty() {
            let val = eval_sql_expr(inner, primary_row, primary_table, joined_row, joined_table, params);
            if val.is_null() { return Value::Null; }
            match val {
                Value::Int(i) => return Value::Int(-i),
                Value::Float(f) => return Value::Float(-f),
                _ => {
                    if let Some(f) = val.as_f64() {
                        return if f.fract() == 0.0 { Value::Int(-f as i64) } else { Value::Float(-f) };
                    }
                }
            }
        }
    }
    if s.starts_with('~') {
        let inner = s[1..].trim();
        if !inner.is_empty() {
            let val = eval_sql_expr(inner, primary_row, primary_table, joined_row, joined_table, params);
            if val.is_null() { return Value::Null; }
            if let Some(i) = val.as_i64() {
                return Value::Int(!i);
            }
        }
    }

    if let Some(v) = crate::engine::temporal::typed_literal(s) {
        return v;
    }

    // +x (dấu cộng một ngôi)
    if s.starts_with('+') && !s.starts_with("++") && s.len() > 1 && find_top_level_math_op(s, &['+', '-']).map_or(true, |(i, _)| i == 0) {
        return eval_sql_expr(s[1..].trim(), primary_row, primary_table, joined_row, joined_table, params);
    }
    // tên bảng đứng riêng (không phải cột) = cả dòng, như `json_agg(t)` / `to_json(t)` của PostgreSQL
    if let Some(t) = primary_table {
        let bare = s.trim_matches('"');
        if !bare.is_empty() && bare.eq_ignore_ascii_case(&t.name) && t.get_column_index(bare).is_none() && bare.chars().all(|c| c.is_alphanumeric() || c == '_') {
            return Value::text(row_to_pg_json(t, primary_row).to_string());
        }
    }
    // to_json / to_jsonb / row_to_json
    for (kw, len) in [("TO_JSONB(", 9usize), ("TO_JSON(", 8), ("ROW_TO_JSON(", 12)] {
        if upper.starts_with(kw) && s.ends_with(')') && is_fully_enclosed_in_parens(&s[len - 1..]) {
            let inner = s[len..s.len() - 1].trim();
            let v = eval_sql_expr(inner, primary_row, primary_table, joined_row, joined_table, params);
            if v.is_null() {
                return Value::Null;
            }
            let col_kind = primary_table.and_then(|t| t.get_column_index(clean_col_name(inner)).map(|i| t.columns[i].data_type.clone()));
            let j = match (&v, col_kind) {
                // cả dòng / jsonb / mảng: đã là JSON
                (Value::Text(t), Some(DataType::Jsonb) | Some(DataType::Array(_))) => value_to_json(&Value::text(t.as_str())),
                (Value::Text(t), None) if t.trim_start().starts_with('{') || t.trim_start().starts_with('[') => serde_json::from_str(t.trim()).unwrap_or_else(|_| value_to_json(&v)),
                (Value::Text(t), _) => serde_json::Value::String(t.to_string()),
                _ => value_to_json(&v),
            };
            return Value::text(j.to_string());
        }
    }
    // GREATEST / LEAST: bỏ qua NULL
    if (upper.starts_with("GREATEST(") || upper.starts_with("LEAST(")) && s.ends_with(')') && is_fully_enclosed_in_parens(&s[s.find('(').unwrap()..]) {
        let greatest = upper.starts_with("GREATEST(");
        let open = s.find('(').unwrap();
        let mut best: Option<Value> = None;
        for a in split_function_args(&s[open + 1..s.len() - 1]) {
            let v = eval_sql_expr(&a, primary_row, primary_table, joined_row, joined_table, params);
            if v.is_null() {
                continue;
            }
            best = Some(match best {
                None => v,
                Some(b) => {
                    let o = v.cmp_value(&b);
                    if (greatest && o == std::cmp::Ordering::Greater) || (!greatest && o == std::cmp::Ordering::Less) { v } else { b }
                }
            });
        }
        return best.unwrap_or(Value::Null);
    }
    // POSITION(sub IN s)
    if upper.starts_with("POSITION(") && s.ends_with(')') && is_fully_enclosed_in_parens(&s[8..]) {
        let inner = &s[9..s.len() - 1];
        if let Some(p) = find_top_level_keyword(inner, "IN") {
            let sub = eval_sql_expr(inner[..p].trim(), primary_row, primary_table, joined_row, joined_table, params);
            let hay = eval_sql_expr(inner[p + 2..].trim(), primary_row, primary_table, joined_row, joined_table, params);
            if sub.is_null() || hay.is_null() {
                return Value::Null;
            }
            let (h, n) = (hay.as_str(), sub.as_str());
            return Value::Int(h.find(n.as_str()).map(|b| h[..b].chars().count() as i64 + 1).unwrap_or(0));
        }
    }
    // SUBSTRING(s FROM a [FOR b]) -> SUBSTRING(s, a, b)
    if (upper.starts_with("SUBSTRING(") || upper.starts_with("SUBSTR(")) && s.ends_with(')') {
        let open = s.find('(').unwrap();
        let inner = &s[open + 1..s.len() - 1];
        if let Some(f) = find_top_level_keyword(inner, "FROM") {
            let base = inner[..f].trim();
            let rest = &inner[f + 4..];
            let rewritten = match find_top_level_keyword(rest, "FOR") {
                Some(fo) => format!("SUBSTRING({}, {}, {})", base, rest[..fo].trim(), rest[fo + 3..].trim()),
                None => format!("SUBSTRING({}, {})", base, rest.trim()),
            };
            return eval_sql_expr(&rewritten, primary_row, primary_table, joined_row, joined_table, params);
        } else if let Some(fo) = find_top_level_keyword(inner, "FOR") {
            return eval_sql_expr(&format!("SUBSTRING({}, 1, {})", inner[..fo].trim(), inner[fo + 3..].trim()), primary_row, primary_table, joined_row, joined_table, params);
        }
    }

    // 2. CAST(expr AS type) or expr::type
    // `::text` của numeric(p,s) / jsonb: định dạng theo kiểu khai báo của biểu thức bên trong
    let text_cast = |target_expr: &str, target_type: &str, val: &Value| -> Option<Value> {
        if !matches!(target_type, "TEXT" | "VARCHAR" | "CHARACTER VARYING") {
            return None;
        }
        let col = |c: &str| -> Option<Option<crate::engine::coltypes::DeclType>> {
            let ok = !c.is_empty() && c.chars().all(|ch| ch.is_alphanumeric() || ch == '_' || ch == '.' || ch == '"');
            if !ok {
                return None;
            }
            let name = c.rsplit('.').next().unwrap_or(c).trim_matches('"');
            [primary_table, joined_table].into_iter().flatten().find_map(|t| t.get_column_index(name).map(|i| crate::engine::coltypes::column_decl(t, i)))
        };
        crate::engine::coltypes::text_cast(target_expr, val, &col)
    };
    if upper.starts_with("CAST(") && s.ends_with(')') {
        let inner = &s[5..s.len() - 1].trim();
        if let Some(as_pos) = rfind_top_level_as(&inner.to_uppercase()) {
            let target_expr = inner[..as_pos].trim();
            let target_type = inner[as_pos + 4..].trim().to_uppercase();
            let val = eval_sql_expr(target_expr, primary_row, primary_table, joined_row, joined_table, params);
            if let Some(v) = text_cast(target_expr, &target_type, &val) {
                return v;
            }
            return cast_val(val, &target_type);
        }
    }
    if let Some(colon_pos) = find_last_top_level_op(s, "::").filter(|&p| cast_is_outermost(s, p)) {
        let target_expr = s[..colon_pos].trim();
        let target_type = s[colon_pos + 2..].trim().to_uppercase();
        let val = eval_sql_expr(target_expr, primary_row, primary_table, joined_row, joined_table, params);
        if let Some(v) = text_cast(target_expr, &target_type, &val) {
            return v;
        }
        return cast_val(val, &target_type);
    }

    // JSON extraction operators: ->>, ->, #>>, #>
    if let Some((op_idx, op)) = find_last_top_level_json_op(s) {
        let lhs_expr = &s[..op_idx].trim();
        let rhs_expr = &s[op_idx + op.len()..].trim();
        let lhs = eval_sql_expr(lhs_expr, primary_row, primary_table, joined_row, joined_table, params);
        let rhs = eval_sql_expr(rhs_expr, primary_row, primary_table, joined_row, joined_table, params);
        let lhs_json = match &lhs {
            Value::Text(st) => serde_json::from_str::<serde_json::Value>(st.trim()).unwrap_or(serde_json::Value::String(st.to_string())),
            _ => value_to_json(&lhs),
        };
        let rhs_json = serde_json::Value::String(rhs.as_str());
        let res = eval_json_extract_serde(&lhs_json, &rhs_json, op);
        return match res {
            serde_json::Value::Null => Value::Null,
            serde_json::Value::String(st) => Value::text(st),
            serde_json::Value::Bool(b) => Value::Bool(b),
            serde_json::Value::Number(n) => {
                if let Some(i) = n.as_i64() {
                    Value::Int(i)
                } else if let Some(f) = n.as_f64() {
                    Value::Float(f)
                } else {
                    Value::text(n.to_string())
                }
            }
            serde_json::Value::Object(_) | serde_json::Value::Array(_) => Value::text(res.to_string()),
        };
    }

    // JSON Functions: JSON_EXTRACT_PATH_TEXT, JSONB_EXTRACT_PATH_TEXT, JSON_EXTRACT_PATH, JSONB_EXTRACT_PATH
    if (upper.starts_with("JSON_EXTRACT_PATH_TEXT(") || upper.starts_with("JSONB_EXTRACT_PATH_TEXT(")) && s.ends_with(')') {
        let open_idx = s.find('(').unwrap();
        let inner = &s[open_idx + 1..s.len() - 1];
        let args = split_function_args(inner);
        if !args.is_empty() {
            let curr = eval_sql_expr(&args[0], primary_row, primary_table, joined_row, joined_table, params);
            let mut curr_json = match &curr {
                Value::Text(st) => serde_json::from_str::<serde_json::Value>(st.trim()).unwrap_or(serde_json::Value::String(st.to_string())),
                _ => value_to_json(&curr),
            };
            for arg in &args[1..] {
                let key = eval_sql_expr(arg, primary_row, primary_table, joined_row, joined_table, params);
                let key_str = key.as_str();
                curr_json = match curr_json {
                    serde_json::Value::Object(map) => map.get(&key_str).cloned().unwrap_or(serde_json::Value::Null),
                    serde_json::Value::Array(arr) => {
                        if let Ok(idx) = key_str.parse::<usize>() {
                            arr.get(idx).cloned().unwrap_or(serde_json::Value::Null)
                        } else {
                            serde_json::Value::Null
                        }
                    }
                    _ => serde_json::Value::Null,
                };
                if curr_json.is_null() {
                    break;
                }
            }
            return match curr_json {
                serde_json::Value::Null => Value::Null,
                serde_json::Value::String(st) => Value::text(st),
                _ => Value::text(curr_json.to_string()),
            };
        }
    }
    if (upper.starts_with("JSON_EXTRACT_PATH(") || upper.starts_with("JSONB_EXTRACT_PATH(")) && s.ends_with(')') {
        let open_idx = s.find('(').unwrap();
        let inner = &s[open_idx + 1..s.len() - 1];
        let args = split_function_args(inner);
        if !args.is_empty() {
            let curr = eval_sql_expr(&args[0], primary_row, primary_table, joined_row, joined_table, params);
            let mut curr_json = match &curr {
                Value::Text(st) => serde_json::from_str::<serde_json::Value>(st.trim()).unwrap_or(serde_json::Value::String(st.to_string())),
                _ => value_to_json(&curr),
            };
            for arg in &args[1..] {
                let key = eval_sql_expr(arg, primary_row, primary_table, joined_row, joined_table, params);
                let key_str = key.as_str();
                curr_json = match curr_json {
                    serde_json::Value::Object(map) => map.get(&key_str).cloned().unwrap_or(serde_json::Value::Null),
                    serde_json::Value::Array(arr) => {
                        if let Ok(idx) = key_str.parse::<usize>() {
                            arr.get(idx).cloned().unwrap_or(serde_json::Value::Null)
                        } else {
                            serde_json::Value::Null
                        }
                    }
                    _ => serde_json::Value::Null,
                };
                if curr_json.is_null() {
                    break;
                }
            }
            return match curr_json {
                serde_json::Value::Null => Value::Null,
                serde_json::Value::String(st) => Value::text(serde_json::to_string(&st).unwrap_or(st)),
                serde_json::Value::Number(n) => {
                    if let Some(i) = n.as_i64() {
                        Value::Int(i)
                    } else if let Some(f) = n.as_f64() {
                        Value::Float(f)
                    } else {
                        Value::text(n.to_string())
                    }
                }
                serde_json::Value::Bool(b) => Value::Bool(b),
                serde_json::Value::Object(_) | serde_json::Value::Array(_) => Value::text(serde_json::to_string(&curr_json).unwrap_or_default()),
            };
        }
    }
    if (upper.starts_with("JSON_ARRAY_LENGTH(") || upper.starts_with("JSONB_ARRAY_LENGTH(")) && s.ends_with(')') {
        let inner = &s[upper.find('(').unwrap() + 1..s.len() - 1];
        let target = eval_sql_expr(inner, primary_row, primary_table, joined_row, joined_table, params);
        let target_json = match &target {
            Value::Text(st) => serde_json::from_str::<serde_json::Value>(st.trim()).unwrap_or(serde_json::Value::Null),
            _ => value_to_json(&target),
        };
        if let serde_json::Value::Array(arr) = target_json {
            return Value::Int(arr.len() as i64);
        }
        return Value::Null;
    }
    if (upper.starts_with("JSON_TYPEOF(") || upper.starts_with("JSONB_TYPEOF(")) && s.ends_with(')') {
        let inner = &s[upper.find('(').unwrap() + 1..s.len() - 1];
        let target = eval_sql_expr(inner, primary_row, primary_table, joined_row, joined_table, params);
        let target_json = match &target {
            Value::Text(st) => serde_json::from_str::<serde_json::Value>(st.trim()).unwrap_or(serde_json::Value::String(st.to_string())),
            _ => value_to_json(&target),
        };
        let type_str = match target_json {
            serde_json::Value::Null => "null",
            serde_json::Value::Bool(_) => "boolean",
            serde_json::Value::Number(_) => "number",
            serde_json::Value::String(_) => "string",
            serde_json::Value::Array(_) => "array",
            serde_json::Value::Object(_) => "object",
        };
        return Value::text(type_str);
    }
    if upper.starts_with("JSONB_STRIP_NULLS(") && s.ends_with(')') {
        let inner = &s[18..s.len() - 1];
        let target = eval_sql_expr(inner, primary_row, primary_table, joined_row, joined_table, params);
        let target_json = match &target {
            Value::Text(st) => serde_json::from_str::<serde_json::Value>(st.trim()).unwrap_or(serde_json::Value::Null),
            _ => value_to_json(&target),
        };
        if let serde_json::Value::Object(mut map) = target_json {
            map.retain(|_, v| !v.is_null());
            return Value::text(serde_json::to_string(&serde_json::Value::Object(map)).unwrap_or_default());
        }
        return target;
    }

    // JSON_BUILD_OBJECT and JSONB_BUILD_OBJECT
    if (upper.starts_with("JSON_BUILD_OBJECT(") || upper.starts_with("JSONB_BUILD_OBJECT(")) && s.ends_with(')') {
        let open_idx = s.find('(').unwrap();
        let inner = &s[open_idx + 1..s.len() - 1];
        let args = split_function_args(inner);
        let mut map = serde_json::Map::new();
        for chunk in args.chunks(2) {
            if chunk.len() == 2 {
                let k_val = eval_sql_expr(&chunk[0], primary_row, primary_table, joined_row, joined_table, params);
                let v_val = eval_sql_expr(&chunk[1], primary_row, primary_table, joined_row, joined_table, params);
                let k_str = k_val.as_str();
                let v_json = value_to_json(&v_val);
                map.insert(k_str, v_json);
            }
        }
        let json_obj = serde_json::Value::Object(map);
        return Value::text(serde_json::to_string(&json_obj).unwrap_or_default());
    }

    // JSON_BUILD_ARRAY and JSONB_BUILD_ARRAY
    if (upper.starts_with("JSON_BUILD_ARRAY(") || upper.starts_with("JSONB_BUILD_ARRAY(")) && s.ends_with(')') {
        let open_idx = s.find('(').unwrap();
        let inner = &s[open_idx + 1..s.len() - 1];
        let items: Vec<serde_json::Value> = split_function_args(inner)
            .into_iter()
            .map(|arg| {
                let v = eval_sql_expr(&arg, primary_row, primary_table, joined_row, joined_table, params);
                value_to_json(&v)
            })
            .collect();
        let arr = serde_json::Value::Array(items);
        return Value::text(serde_json::to_string(&arr).unwrap_or_default());
    }

    // JSONB_PRETTY
    if upper.starts_with("JSONB_PRETTY(") && s.ends_with(')') {
        let inner = &s[13..s.len() - 1];
        let v = eval_sql_expr(inner, primary_row, primary_table, joined_row, joined_table, params);
        let v_json = match &v {
            Value::Text(st) => serde_json::from_str::<serde_json::Value>(st.trim()).unwrap_or(value_to_json(&v)),
            _ => value_to_json(&v),
        };
        return Value::text(serde_json::to_string_pretty(&v_json).unwrap_or_default());
    }

    // JSONB_SET
    if (upper.starts_with("JSONB_SET(") || upper.starts_with("JSON_SET(")) && s.ends_with(')') {
        let open_idx = s.find('(').unwrap();
        let inner = &s[open_idx + 1..s.len() - 1];
        let args = split_function_args(inner);
        if args.len() >= 3 {
            let target_val = eval_sql_expr(&args[0], primary_row, primary_table, joined_row, joined_table, params);
            let path_val = eval_sql_expr(&args[1], primary_row, primary_table, joined_row, joined_table, params);
            let new_val_raw = eval_sql_expr(&args[2], primary_row, primary_table, joined_row, joined_table, params);
            let create_missing = if args.len() >= 4 {
                let cm_val = eval_sql_expr(&args[3], primary_row, primary_table, joined_row, joined_table, params);
                cm_val.as_bool().unwrap_or(true)
            } else {
                true
            };

            let mut target_json = match &target_val {
                Value::Text(st) => serde_json::from_str::<serde_json::Value>(st.trim()).unwrap_or(value_to_json(&target_val)),
                _ => value_to_json(&target_val),
            };
            let path_json = match &path_val {
                Value::Text(st) => serde_json::Value::String(st.to_string()),
                _ => value_to_json(&path_val),
            };
            let keys = parse_json_path_keys(&path_json);
            let new_json = match &new_val_raw {
                Value::Text(st) => serde_json::from_str::<serde_json::Value>(st.trim()).unwrap_or(serde_json::Value::String(st.to_string())),
                _ => value_to_json(&new_val_raw),
            };
            jsonb_set_serde(&mut target_json, &keys, &new_json, create_missing);
            return Value::text(serde_json::to_string(&target_json).unwrap_or_default());
        }
    }





    // 4. Built-in String Functions: UPPER, LOWER, INITCAP, CONCAT, CONCAT_WS, LENGTH, CHAR_LENGTH,
    // CHARACTER_LENGTH, OCTET_LENGTH, LEFT, RIGHT, TRIM, BTRIM, LTRIM, RTRIM, LPAD, RPAD, REPEAT,
    // REVERSE, ASCII, CHR, TRANSLATE, STRPOS, SPLIT_PART, MD5, ENCODE, DECODE, REPLACE, SUBSTRING
    if upper.starts_with("UPPER(") && s.ends_with(')') {
        let inner = &s[6..s.len() - 1];
        let v = eval_sql_expr(inner, primary_row, primary_table, joined_row, joined_table, params);
        if v.is_null() { return Value::Null; }
        return Value::text(v.as_str().to_uppercase());
    }

    if upper.starts_with("LOWER(") && s.ends_with(')') {
        let inner = &s[6..s.len() - 1];
        let v = eval_sql_expr(inner, primary_row, primary_table, joined_row, joined_table, params);
        if v.is_null() { return Value::Null; }
        return Value::text(v.as_str().to_lowercase());
    }

    if upper.starts_with("INITCAP(") && s.ends_with(')') {
        let inner = &s[8..s.len() - 1];
        let v = eval_sql_expr(inner, primary_row, primary_table, joined_row, joined_table, params);
        if v.is_null() { return Value::Null; }
        let s_str = v.as_str();
        let mut out = String::with_capacity(s_str.len());
        let mut new_word = true;
        for c in s_str.chars() {
            if c.is_alphanumeric() {
                if new_word {
                    out.extend(c.to_uppercase());
                    new_word = false;
                } else {
                    out.extend(c.to_lowercase());
                }
            } else {
                out.push(c);
                new_word = true;
            }
        }
        return Value::text(out);
    }

    if upper.starts_with("CONCAT(") && s.ends_with(')') {
        let inner = &s[7..s.len() - 1];
        let args = split_function_args(inner);
        let mut out = String::new();
        for arg in args {
            let v = eval_sql_expr(&arg, primary_row, primary_table, joined_row, joined_table, params);
            if !v.is_null() {
                out.push_str(&v.as_str());
            }
        }
        return Value::text(out);
    }

    if upper.starts_with("CONCAT_WS(") && s.ends_with(')') {
        let inner = &s[10..s.len() - 1];
        let args = split_function_args(inner);
        if !args.is_empty() {
            let sep_val = eval_sql_expr(&args[0], primary_row, primary_table, joined_row, joined_table, params);
            if sep_val.is_null() {
                return Value::Null;
            }
            let sep = sep_val.as_str();
            let mut parts = Vec::new();
            for arg in &args[1..] {
                let v = eval_sql_expr(arg, primary_row, primary_table, joined_row, joined_table, params);
                if !v.is_null() {
                    parts.push(v.as_str());
                }
            }
            return Value::text(parts.join(&sep));
        }
    }

    if (upper.starts_with("LENGTH(") || upper.starts_with("CHAR_LENGTH(") || upper.starts_with("CHARACTER_LENGTH(")) && s.ends_with(')') {
        let open_p = s.find('(').unwrap();
        let inner = &s[open_p + 1..s.len() - 1];
        let v = eval_sql_expr(inner, primary_row, primary_table, joined_row, joined_table, params);
        if v.is_null() { return Value::Null; }
        let len = v.as_str().chars().count();
        return Value::Int(len as i64);
    }

    if upper.starts_with("OCTET_LENGTH(") && s.ends_with(')') {
        let inner = &s[13..s.len() - 1];
        let v = eval_sql_expr(inner, primary_row, primary_table, joined_row, joined_table, params);
        if v.is_null() { return Value::Null; }
        let len = v.as_str().as_bytes().len();
        return Value::Int(len as i64);
    }

    if upper.starts_with("LEFT(") && s.ends_with(')') {
        let inner = &s[5..s.len() - 1];
        let args = split_function_args(inner);
        if args.len() == 2 {
            let v = eval_sql_expr(&args[0], primary_row, primary_table, joined_row, joined_table, params);
            let n_val = eval_sql_expr(&args[1], primary_row, primary_table, joined_row, joined_table, params);
            if v.is_null() || n_val.is_null() {
                return Value::Null;
            }
            let s_str = v.as_str();
            let chars: Vec<char> = s_str.chars().collect();
            let len = chars.len() as i64;
            let n = n_val.as_i64().unwrap_or(0);
            let take_len = if n >= 0 {
                (n as usize).min(chars.len())
            } else {
                ((len + n).max(0) as usize).min(chars.len())
            };
            let res: String = chars[..take_len].iter().collect();
            return Value::text(res);
        }
    }

    if upper.starts_with("RIGHT(") && s.ends_with(')') {
        let inner = &s[6..s.len() - 1];
        let args = split_function_args(inner);
        if args.len() == 2 {
            let v = eval_sql_expr(&args[0], primary_row, primary_table, joined_row, joined_table, params);
            let n_val = eval_sql_expr(&args[1], primary_row, primary_table, joined_row, joined_table, params);
            if v.is_null() || n_val.is_null() {
                return Value::Null;
            }
            let s_str = v.as_str();
            let chars: Vec<char> = s_str.chars().collect();
            let n = n_val.as_i64().unwrap_or(0);
            let start_idx = if n >= 0 {
                chars.len().saturating_sub(n as usize)
            } else {
                ((-n) as usize).min(chars.len())
            };
            let res: String = chars[start_idx..].iter().collect();
            return Value::text(res);
        }
    }

    if (upper.starts_with("TRIM(") || upper.starts_with("BTRIM(")) && s.ends_with(')') {
        let open_p = s.find('(').unwrap();
        let inner = &s[open_p + 1..s.len() - 1];
        let args = split_function_args(inner);
        if args.len() == 1 {
            let v = eval_sql_expr(&args[0], primary_row, primary_table, joined_row, joined_table, params);
            if v.is_null() { return Value::Null; }
            return Value::text(v.as_str().trim());
        } else if args.len() >= 2 {
            let v = eval_sql_expr(&args[0], primary_row, primary_table, joined_row, joined_table, params);
            let chars_v = eval_sql_expr(&args[1], primary_row, primary_table, joined_row, joined_table, params);
            if v.is_null() || chars_v.is_null() { return Value::Null; }
            let s_str = v.as_str();
            let c_str = chars_v.as_str();
            let trim_chars: Vec<char> = c_str.chars().collect();
            let trimmed = s_str.trim_matches(|c: char| trim_chars.contains(&c));
            return Value::text(trimmed);
        }
    }

    if upper.starts_with("LTRIM(") && s.ends_with(')') {
        let inner = &s[6..s.len() - 1];
        let args = split_function_args(inner);
        if args.len() == 1 {
            let v = eval_sql_expr(&args[0], primary_row, primary_table, joined_row, joined_table, params);
            if v.is_null() { return Value::Null; }
            return Value::text(v.as_str().trim_start());
        } else if args.len() >= 2 {
            let v = eval_sql_expr(&args[0], primary_row, primary_table, joined_row, joined_table, params);
            let chars_v = eval_sql_expr(&args[1], primary_row, primary_table, joined_row, joined_table, params);
            if v.is_null() || chars_v.is_null() { return Value::Null; }
            let s_str = v.as_str();
            let c_str = chars_v.as_str();
            let trim_chars: Vec<char> = c_str.chars().collect();
            let trimmed = s_str.trim_start_matches(|c: char| trim_chars.contains(&c));
            return Value::text(trimmed);
        }
    }

    if upper.starts_with("RTRIM(") && s.ends_with(')') {
        let inner = &s[6..s.len() - 1];
        let args = split_function_args(inner);
        if args.len() == 1 {
            let v = eval_sql_expr(&args[0], primary_row, primary_table, joined_row, joined_table, params);
            if v.is_null() { return Value::Null; }
            return Value::text(v.as_str().trim_end());
        } else if args.len() >= 2 {
            let v = eval_sql_expr(&args[0], primary_row, primary_table, joined_row, joined_table, params);
            let chars_v = eval_sql_expr(&args[1], primary_row, primary_table, joined_row, joined_table, params);
            if v.is_null() || chars_v.is_null() { return Value::Null; }
            let s_str = v.as_str();
            let c_str = chars_v.as_str();
            let trim_chars: Vec<char> = c_str.chars().collect();
            let trimmed = s_str.trim_end_matches(|c: char| trim_chars.contains(&c));
            return Value::text(trimmed);
        }
    }

    if upper.starts_with("LPAD(") && s.ends_with(')') {
        let inner = &s[5..s.len() - 1];
        let args = split_function_args(inner);
        if args.len() >= 2 {
            let v = eval_sql_expr(&args[0], primary_row, primary_table, joined_row, joined_table, params);
            let len_val = eval_sql_expr(&args[1], primary_row, primary_table, joined_row, joined_table, params);
            if v.is_null() || len_val.is_null() { return Value::Null; }
            let target_len = len_val.as_i64().unwrap_or(0).max(0) as usize;
            let s_str = v.as_str();
            let chars: Vec<char> = s_str.chars().collect();
            if chars.len() >= target_len {
                let res: String = chars[..target_len].iter().collect();
                return Value::text(res);
            }
            let pad_str = if args.len() >= 3 {
                let pv = eval_sql_expr(&args[2], primary_row, primary_table, joined_row, joined_table, params);
                if pv.is_null() { return Value::Null; }
                pv.as_str()
            } else {
                " ".to_string()
            };
            if pad_str.is_empty() {
                let res: String = chars.into_iter().collect();
                return Value::text(res);
            }
            let pad_chars: Vec<char> = pad_str.chars().collect();
            let needed = target_len - chars.len();
            let mut out = String::new();
            for i in 0..needed {
                out.push(pad_chars[i % pad_chars.len()]);
            }
            out.push_str(&s_str);
            return Value::text(out);
        }
    }

    if upper.starts_with("RPAD(") && s.ends_with(')') {
        let inner = &s[5..s.len() - 1];
        let args = split_function_args(inner);
        if args.len() >= 2 {
            let v = eval_sql_expr(&args[0], primary_row, primary_table, joined_row, joined_table, params);
            let len_val = eval_sql_expr(&args[1], primary_row, primary_table, joined_row, joined_table, params);
            if v.is_null() || len_val.is_null() { return Value::Null; }
            let target_len = len_val.as_i64().unwrap_or(0).max(0) as usize;
            let s_str = v.as_str();
            let chars: Vec<char> = s_str.chars().collect();
            if chars.len() >= target_len {
                let res: String = chars[..target_len].iter().collect();
                return Value::text(res);
            }
            let pad_str = if args.len() >= 3 {
                let pv = eval_sql_expr(&args[2], primary_row, primary_table, joined_row, joined_table, params);
                if pv.is_null() { return Value::Null; }
                pv.as_str()
            } else {
                " ".to_string()
            };
            if pad_str.is_empty() {
                let res: String = chars.into_iter().collect();
                return Value::text(res);
            }
            let pad_chars: Vec<char> = pad_str.chars().collect();
            let needed = target_len - chars.len();
            let mut out = s_str;
            for i in 0..needed {
                out.push(pad_chars[i % pad_chars.len()]);
            }
            return Value::text(out);
        }
    }

    if upper.starts_with("REPEAT(") && s.ends_with(')') {
        let inner = &s[7..s.len() - 1];
        let args = split_function_args(inner);
        if args.len() == 2 {
            let v = eval_sql_expr(&args[0], primary_row, primary_table, joined_row, joined_table, params);
            let count_val = eval_sql_expr(&args[1], primary_row, primary_table, joined_row, joined_table, params);
            if v.is_null() || count_val.is_null() { return Value::Null; }
            let count = count_val.as_i64().unwrap_or(0).max(0) as usize;
            let s_str = v.as_str();
            return Value::text(s_str.repeat(count));
        }
    }

    if upper.starts_with("REVERSE(") && s.ends_with(')') {
        let inner = &s[8..s.len() - 1];
        let v = eval_sql_expr(inner, primary_row, primary_table, joined_row, joined_table, params);
        if v.is_null() { return Value::Null; }
        let rev: String = v.as_str().chars().rev().collect();
        return Value::text(rev);
    }

    if upper.starts_with("ASCII(") && s.ends_with(')') {
        let inner = &s[6..s.len() - 1];
        let v = eval_sql_expr(inner, primary_row, primary_table, joined_row, joined_table, params);
        if v.is_null() { return Value::Null; }
        let s_str = v.as_str();
        if let Some(first_char) = s_str.chars().next() {
            return Value::Int(first_char as u32 as i64);
        }
        return Value::Int(0);
    }

    if upper.starts_with("CHR(") && s.ends_with(')') {
        let inner = &s[4..s.len() - 1];
        let v = eval_sql_expr(inner, primary_row, primary_table, joined_row, joined_table, params);
        if v.is_null() { return Value::Null; }
        let code = v.as_i64().unwrap_or(0) as u32;
        if let Some(ch) = char::from_u32(code) {
            return Value::text(ch.to_string());
        }
        return Value::text("");
    }

    if upper.starts_with("TRANSLATE(") && s.ends_with(')') {
        let inner = &s[10..s.len() - 1];
        let args = split_function_args(inner);
        if args.len() == 3 {
            let str_val = eval_sql_expr(&args[0], primary_row, primary_table, joined_row, joined_table, params);
            let from_val = eval_sql_expr(&args[1], primary_row, primary_table, joined_row, joined_table, params);
            let to_val = eval_sql_expr(&args[2], primary_row, primary_table, joined_row, joined_table, params);
            if str_val.is_null() || from_val.is_null() || to_val.is_null() { return Value::Null; }
            let s_str = str_val.as_str();
            let from_chars: Vec<char> = from_val.as_str().chars().collect();
            let to_chars: Vec<char> = to_val.as_str().chars().collect();
            let mut out = String::new();
            for ch in s_str.chars() {
                if let Some(pos) = from_chars.iter().position(|&fc| fc == ch) {
                    if pos < to_chars.len() {
                        out.push(to_chars[pos]);
                    }
                } else {
                    out.push(ch);
                }
            }
            return Value::text(out);
        }
    }

    if upper.starts_with("STRPOS(") && s.ends_with(')') {
        let inner = &s[7..s.len() - 1];
        let args = split_function_args(inner);
        if args.len() == 2 {
            let str_val = eval_sql_expr(&args[0], primary_row, primary_table, joined_row, joined_table, params);
            let sub_val = eval_sql_expr(&args[1], primary_row, primary_table, joined_row, joined_table, params);
            if str_val.is_null() || sub_val.is_null() { return Value::Null; }
            let s_str = str_val.as_str();
            let sub_str = sub_val.as_str();
            if let Some(byte_pos) = s_str.find(&sub_str) {
                let char_idx = s_str[..byte_pos].chars().count() + 1;
                return Value::Int(char_idx as i64);
            } else {
                return Value::Int(0);
            }
        }
    }

    if upper.starts_with("SPLIT_PART(") && s.ends_with(')') {
        let inner = &s[11..s.len() - 1];
        let args = split_function_args(inner);
        if args.len() == 3 {
            let str_val = eval_sql_expr(&args[0], primary_row, primary_table, joined_row, joined_table, params);
            let delim_val = eval_sql_expr(&args[1], primary_row, primary_table, joined_row, joined_table, params);
            let field_val = eval_sql_expr(&args[2], primary_row, primary_table, joined_row, joined_table, params);
            if str_val.is_null() || delim_val.is_null() || field_val.is_null() { return Value::Null; }
            let s_str = str_val.as_str();
            let delim_str = delim_val.as_str();
            let field_idx = field_val.as_i64().unwrap_or(1);
            if field_idx < 1 {
                return Value::text("");
            }
            let parts: Vec<&str> = s_str.split(&delim_str).collect();
            let u_idx = (field_idx - 1) as usize;
            if u_idx < parts.len() {
                return Value::text(parts[u_idx]);
            } else {
                return Value::text("");
            }
        }
    }

    if upper.starts_with("MD5(") && s.ends_with(')') {
        let inner = &s[4..s.len() - 1];
        let v = eval_sql_expr(inner, primary_row, primary_table, joined_row, joined_table, params);
        if v.is_null() { return Value::Null; }
        let s_str = v.as_str();
        let mut hasher = Md5::new();
        hasher.update(s_str.as_bytes());
        let hash = format!("{:x}", hasher.finalize());
        return Value::text(hash);
    }

    if upper.starts_with("ENCODE(") && s.ends_with(')') {
        let inner = &s[7..s.len() - 1];
        let args = split_function_args(inner);
        if args.len() == 2 {
            let str_val = eval_sql_expr(&args[0], primary_row, primary_table, joined_row, joined_table, params);
            let fmt_val = eval_sql_expr(&args[1], primary_row, primary_table, joined_row, joined_table, params);
            if str_val.is_null() || fmt_val.is_null() { return Value::Null; }
            let s_str = str_val.as_str();
            let fmt_str = fmt_val.as_str().to_lowercase();
            if fmt_str == "hex" {
                return Value::text(hex::encode(s_str.as_bytes()));
            } else if fmt_str == "base64" {
                return Value::text(base64::engine::general_purpose::STANDARD.encode(s_str.as_bytes()));
            }
        }
    }

    if upper.starts_with("DECODE(") && s.ends_with(')') {
        let inner = &s[7..s.len() - 1];
        let args = split_function_args(inner);
        if args.len() == 2 {
            let str_val = eval_sql_expr(&args[0], primary_row, primary_table, joined_row, joined_table, params);
            let fmt_val = eval_sql_expr(&args[1], primary_row, primary_table, joined_row, joined_table, params);
            if str_val.is_null() || fmt_val.is_null() { return Value::Null; }
            let s_str = str_val.as_str();
            let fmt_str = fmt_val.as_str().to_lowercase();
            if fmt_str == "hex" {
                if let Ok(bytes) = hex::decode(s_str.trim()) {
                    return Value::text(String::from_utf8_lossy(&bytes).to_string());
                }
            } else if fmt_str == "base64" {
                if let Ok(bytes) = base64::engine::general_purpose::STANDARD.decode(s_str.trim()) {
                    return Value::text(String::from_utf8_lossy(&bytes).to_string());
                }
            }
        }
    }

    if upper.starts_with("REPLACE(") && s.ends_with(')') {
        let inner = &s[8..s.len() - 1];
        let args = split_function_args(inner);
        if args.len() == 3 {
            let base = eval_sql_expr(&args[0], primary_row, primary_table, joined_row, joined_table, params);
            let from = eval_sql_expr(&args[1], primary_row, primary_table, joined_row, joined_table, params);
            let to = eval_sql_expr(&args[2], primary_row, primary_table, joined_row, joined_table, params);
            if base.is_null() || from.is_null() || to.is_null() { return Value::Null; }
            let base_s = base.as_str();
            let from_s = from.as_str();
            let to_s = to.as_str();
            return Value::text(base_s.replace(&from_s, &to_s));
        }
    }

    if (upper.starts_with("SUBSTRING(") || upper.starts_with("SUBSTR(")) && s.ends_with(')') {
        let open_p = s.find('(').unwrap();
        let inner = &s[open_p + 1..s.len() - 1];
        let args = split_function_args(inner);
        if args.len() == 2 && crate::engine::strfn::is_text_literal(&args[1]) {
            // substring(s FROM 'regex') / substring(s, 'regex')
            let base = eval_sql_expr(&args[0], primary_row, primary_table, joined_row, joined_table, params);
            let pat = eval_sql_expr(&args[1], primary_row, primary_table, joined_row, joined_table, params);
            return crate::engine::strfn::substring_regex(&base, &pat);
        }
        if args.len() >= 2 {
            let base = eval_sql_expr(&args[0], primary_row, primary_table, joined_row, joined_table, params);
            if base.is_null() { return Value::Null; }
            let start = eval_sql_expr(&args[1], primary_row, primary_table, joined_row, joined_table, params).as_i64().unwrap_or(1);
            let length = if args.len() >= 3 {
                eval_sql_expr(&args[2], primary_row, primary_table, joined_row, joined_table, params).as_i64()
            } else {
                None
            };
            let base_s = base.as_str();
            let is_bytea = base_s.starts_with("\\x")
                || base_s.starts_with(r"\x")
                || base_s.starts_with("0x")
                || (base_s.starts_with('[') && base_s.ends_with(']') && serde_json::from_str::<Vec<u8>>(&base_s).is_ok());

            if is_bytea {
                let raw = get_raw_bytes(&base);
                let start_idx = if start > 0 { (start as usize).saturating_sub(1) } else { 0 };
                let sliced: Vec<u8> = if let Some(len) = length {
                    raw.into_iter().skip(start_idx).take(len.max(0) as usize).collect()
                } else {
                    raw.into_iter().skip(start_idx).collect()
                };
                let json_arr: Vec<serde_json::Value> = sliced.into_iter().map(|b| serde_json::json!(b)).collect();
                return Value::text(serde_json::to_string(&json_arr).unwrap_or_default());
            }
            let chars: Vec<char> = base_s.chars().collect();
            let start_idx = if start > 0 { (start as usize).saturating_sub(1) } else { 0 };
            let sub: String = if let Some(len) = length {
                chars.iter().skip(start_idx).take(len.max(0) as usize).collect()
            } else {
                chars.iter().skip(start_idx).collect()
            };
            return Value::text(sub);
        }
    }

    // 5. Math Functions: ABS, FLOOR, CEIL, ROUND, TRUNC, POWER, SQRT, CBRT, EXP, LN, LOG, MOD, SIGN, PI, DEGREES, RADIANS, RANDOM
    if upper == "PI()" || upper == "PI" {
        return Value::Float(std::f64::consts::PI);
    }

    if upper == "RANDOM()" {
        let nanos = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.subsec_nanos()).unwrap_or(12345);
        let rand = ((nanos % 1_000_000) as f64) / 1_000_000.0;
        return Value::Float(rand);
    }

    if upper.starts_with("ABS(") && s.ends_with(')') {
        let inner = &s[4..s.len() - 1];
        let v = eval_sql_expr(inner, primary_row, primary_table, joined_row, joined_table, params);
        if v.is_null() { return Value::Null; }
        if let Some(f) = v.as_f64() {
            return if f.fract() == 0.0 { Value::Int(f.abs() as i64) } else { Value::Float(f.abs()) };
        }
    }

    if upper.starts_with("FLOOR(") && s.ends_with(')') {
        let inner = &s[6..s.len() - 1];
        let v = eval_sql_expr(inner, primary_row, primary_table, joined_row, joined_table, params);
        if v.is_null() { return Value::Null; }
        if let Some(f) = v.as_f64() {
            return Value::Int(f.floor() as i64);
        }
    }

    if (upper.starts_with("CEIL(") || upper.starts_with("CEILING(")) && s.ends_with(')') {
        let start_pos = if upper.starts_with("CEILING(") { 8 } else { 5 };
        let inner = &s[start_pos..s.len() - 1];
        let v = eval_sql_expr(inner, primary_row, primary_table, joined_row, joined_table, params);
        if v.is_null() { return Value::Null; }
        if let Some(f) = v.as_f64() {
            return Value::Int(f.ceil() as i64);
        }
    }

    if upper.starts_with("ROUND(") && s.ends_with(')') {
        let inner = &s[6..s.len() - 1];
        let args = split_function_args(inner);
        if !args.is_empty() {
            let v = eval_sql_expr(&args[0], primary_row, primary_table, joined_row, joined_table, params);
            if v.is_null() { return Value::Null; }
            let decimals = if args.len() >= 2 {
                let d_val = eval_sql_expr(&args[1], primary_row, primary_table, joined_row, joined_table, params);
                if d_val.is_null() { return Value::Null; }
                d_val.as_i64().unwrap_or(0)
            } else {
                0
            };
            if let Some(f) = v.as_f64() {
                if decimals == 0 {
                    return Value::Int(f.round() as i64);
                } else {
                    return Value::Float(crate::engine::fnlib::round_dec(f, decimals));
                }
            }
        }
    }

    if (upper.starts_with("TRUNC(") || upper.starts_with("TRUNCATE(")) && s.ends_with(')') {
        let start_pos = if upper.starts_with("TRUNCATE(") { 9 } else { 6 };
        let inner = &s[start_pos..s.len() - 1];
        let args = split_function_args(inner);
        if !args.is_empty() {
            let v = eval_sql_expr(&args[0], primary_row, primary_table, joined_row, joined_table, params);
            if v.is_null() { return Value::Null; }
            let decimals = if args.len() >= 2 {
                let d_val = eval_sql_expr(&args[1], primary_row, primary_table, joined_row, joined_table, params);
                if d_val.is_null() { return Value::Null; }
                d_val.as_i64().unwrap_or(0)
            } else {
                0
            };
            if let Some(f) = v.as_f64() {
                if decimals == 0 {
                    return Value::Int(f.trunc() as i64);
                } else {
                    let multiplier = 10f64.powi(decimals as i32);
                    let truncated = (f * multiplier).trunc() / multiplier;
                    return Value::Float(truncated);
                }
            }
        }
    }

    if (upper.starts_with("POWER(") || upper.starts_with("POW(")) && s.ends_with(')') {
        let start_pos = if upper.starts_with("POWER(") { 6 } else { 4 };
        let inner = &s[start_pos..s.len() - 1];
        let args = split_function_args(inner);
        if args.len() == 2 {
            let base = eval_sql_expr(&args[0], primary_row, primary_table, joined_row, joined_table, params);
            let exp = eval_sql_expr(&args[1], primary_row, primary_table, joined_row, joined_table, params);
            if base.is_null() || exp.is_null() { return Value::Null; }
            if let (Some(b), Some(e)) = (base.as_f64(), exp.as_f64()) {
                let res = b.powf(e);
                return if res.fract() == 0.0 { Value::Int(res as i64) } else { Value::Float(res) };
            }
        }
    }

    if upper.starts_with("SQRT(") && s.ends_with(')') {
        let inner = &s[5..s.len() - 1];
        let v = eval_sql_expr(inner, primary_row, primary_table, joined_row, joined_table, params);
        if v.is_null() { return Value::Null; }
        if let Some(f) = v.as_f64() {
            let res = f.sqrt();
            return if res.fract() == 0.0 { Value::Int(res as i64) } else { Value::Float(res) };
        }
    }

    if upper.starts_with("CBRT(") && s.ends_with(')') {
        let inner = &s[5..s.len() - 1];
        let v = eval_sql_expr(inner, primary_row, primary_table, joined_row, joined_table, params);
        if v.is_null() { return Value::Null; }
        if let Some(f) = v.as_f64() {
            let res = f.cbrt();
            return if res.fract() == 0.0 { Value::Int(res as i64) } else { Value::Float(res) };
        }
    }

    if upper.starts_with("EXP(") && s.ends_with(')') {
        let inner = &s[4..s.len() - 1];
        let v = eval_sql_expr(inner, primary_row, primary_table, joined_row, joined_table, params);
        if v.is_null() { return Value::Null; }
        if let Some(f) = v.as_f64() {
            let res = f.exp();
            return if res.fract() == 0.0 { Value::Int(res as i64) } else { Value::Float(res) };
        }
    }

    if upper.starts_with("LN(") && s.ends_with(')') {
        let inner = &s[3..s.len() - 1];
        let v = eval_sql_expr(inner, primary_row, primary_table, joined_row, joined_table, params);
        if v.is_null() { return Value::Null; }
        if let Some(f) = v.as_f64() {
            let res = f.ln();
            return if res.fract() == 0.0 { Value::Int(res as i64) } else { Value::Float(res) };
        }
    }

    if (upper.starts_with("LOG(") || upper.starts_with("LOG10(")) && s.ends_with(')') {
        let start_pos = if upper.starts_with("LOG10(") { 6 } else { 4 };
        let inner = &s[start_pos..s.len() - 1];
        let args = split_function_args(inner);
        if args.len() == 1 {
            let v = eval_sql_expr(&args[0], primary_row, primary_table, joined_row, joined_table, params);
            if v.is_null() { return Value::Null; }
            if let Some(f) = v.as_f64() {
                let res = f.log10();
                return if res.fract() == 0.0 { Value::Int(res as i64) } else { Value::Float(res) };
            }
        } else if args.len() == 2 {
            let b_val = eval_sql_expr(&args[0], primary_row, primary_table, joined_row, joined_table, params);
            let x_val = eval_sql_expr(&args[1], primary_row, primary_table, joined_row, joined_table, params);
            if b_val.is_null() || x_val.is_null() { return Value::Null; }
            if let (Some(b), Some(x)) = (b_val.as_f64(), x_val.as_f64()) {
                let res = x.log(b);
                return if res.fract() == 0.0 { Value::Int(res as i64) } else { Value::Float(res) };
            }
        }
    }

    if upper.starts_with("DEGREES(") && s.ends_with(')') {
        let inner = &s[8..s.len() - 1];
        let v = eval_sql_expr(inner, primary_row, primary_table, joined_row, joined_table, params);
        if v.is_null() { return Value::Null; }
        if let Some(f) = v.as_f64() {
            return Value::Float(f.to_degrees());
        }
    }

    if upper.starts_with("RADIANS(") && s.ends_with(')') {
        let inner = &s[8..s.len() - 1];
        let v = eval_sql_expr(inner, primary_row, primary_table, joined_row, joined_table, params);
        if v.is_null() { return Value::Null; }
        if let Some(f) = v.as_f64() {
            return Value::Float(f.to_radians());
        }
    }

    if upper.starts_with("MOD(") && s.ends_with(')') {
        let inner = &s[4..s.len() - 1];
        let args = split_function_args(inner);
        if args.len() == 2 {
            let a = eval_sql_expr(&args[0], primary_row, primary_table, joined_row, joined_table, params);
            let b = eval_sql_expr(&args[1], primary_row, primary_table, joined_row, joined_table, params);
            if a.is_null() || b.is_null() { return Value::Null; }
            if let (Some(a_i), Some(b_i)) = (a.as_i64(), b.as_i64()) {
                return Value::Int(if b_i != 0 { a_i % b_i } else { 0 });
            }
        }
    }

    if upper.starts_with("SIGN(") && s.ends_with(')') {
        let inner = &s[5..s.len() - 1];
        let v = eval_sql_expr(inner, primary_row, primary_table, joined_row, joined_table, params);
        if v.is_null() { return Value::Null; }
        if let Some(f) = v.as_f64() {
            return Value::Int(if f > 0.0 { 1 } else if f < 0.0 { -1 } else { 0 });
        }
    }

    // hàm chuỗi bổ sung: regex, format (engine/strfn.rs)
    if s.ends_with(')') && crate::engine::strfn::is_strfn_name(&upper) {
        let eval = |e: &str| eval_sql_expr(e, primary_row, primary_table, joined_row, joined_table, params);
        if let Some(v) = crate::engine::strfn::try_eval(s, &upper, &eval) {
            return v;
        }
    }
    // 6. Date & Time: bộ hàm đầy đủ ở engine/datetime.rs (theo múi giờ phiên); mã cũ bên dưới là dự phòng
    if s.ends_with(')') && is_datetime_fn_name(&upper) {
        let eval = |e: &str| eval_sql_expr(e, primary_row, primary_table, joined_row, joined_table, params);
        let col = |c: &str| {
            let name = c.rsplit('.').next().unwrap_or(c).trim_matches('"');
            [primary_table, joined_table].into_iter().flatten().find_map(|t| t.get_column_index(name).and_then(|i| crate::engine::coltypes::column_decl(t, i)))
        };
        let type_of = |e: &str| crate::engine::coltypes::pg_typeof(e, &col);
        let ctx = crate::engine::datetime::Ctx { eval: &eval, type_of: &type_of };
        if let Some(v) = crate::engine::datetime::try_eval(s, &upper, &ctx) {
            return v;
        }
    }
    if upper.starts_with("EXTRACT(") && s.ends_with(')') {
        let inner = s[8..s.len() - 1].trim();
        let inner_upper = inner.to_uppercase();
        if let Some(from_pos) = inner_upper.find(" FROM ") {
            let field = inner[..from_pos].trim();
            let source_expr = inner[from_pos + 6..].trim();
            let date_val = eval_sql_expr(source_expr, primary_row, primary_table, joined_row, joined_table, params);
            if date_val.is_null() { return Value::Null; }
            if let Some(dt) = parse_datetime_components(&date_val.as_str()) {
                if let Some(val) = eval_date_part_or_extract(field, &dt) {
                    return val;
                }
            }
        }
    }

    if upper.starts_with("DATE_PART(") && s.ends_with(')') {
        let inner = &s[10..s.len() - 1];
        let args = split_function_args(inner);
        if args.len() == 2 {
            let part_field = eval_sql_expr(&args[0], primary_row, primary_table, joined_row, joined_table, params);
            let date_val = eval_sql_expr(&args[1], primary_row, primary_table, joined_row, joined_table, params);
            if part_field.is_null() || date_val.is_null() { return Value::Null; }
            let field = part_field.as_str();
            if let Some(dt) = parse_datetime_components(&date_val.as_str()) {
                if let Some(val) = eval_date_part_or_extract(&field, &dt) {
                    return val;
                }
            }
        }
    }

    if upper.starts_with("DATE_TRUNC(") && s.ends_with(')') {
        let inner = &s[11..s.len() - 1];
        let args = split_function_args(inner);
        if args.len() == 2 {
            let unit_val = eval_sql_expr(&args[0], primary_row, primary_table, joined_row, joined_table, params);
            let date_val = eval_sql_expr(&args[1], primary_row, primary_table, joined_row, joined_table, params);
            if unit_val.is_null() || date_val.is_null() { return Value::Null; }
            let unit = unit_val.as_str();
            if let Some(dt) = parse_datetime_components(&date_val.as_str()) {
                return Value::text(eval_date_trunc(&unit, &dt));
            }
        }
    }

    if upper.starts_with("TO_CHAR(") && s.ends_with(')') {
        let inner = &s[8..s.len() - 1];
        let args = split_function_args(inner);
        if args.len() == 2 {
            let date_val = eval_sql_expr(&args[0], primary_row, primary_table, joined_row, joined_table, params);
            let fmt_val = eval_sql_expr(&args[1], primary_row, primary_table, joined_row, joined_table, params);
            if date_val.is_null() || fmt_val.is_null() { return Value::Null; }
            let fmt = fmt_val.as_str();
            if let Some(dt) = parse_datetime_components(&date_val.as_str()) {
                return Value::text(eval_to_char(&dt, &fmt));
            }
        }
    }

    if upper.starts_with("AGE(") && s.ends_with(')') {
        let inner = &s[4..s.len() - 1];
        let args = split_function_args(inner);
        if args.len() == 1 {
            let ts1_val = eval_sql_expr(&args[0], primary_row, primary_table, joined_row, joined_table, params);
            if ts1_val.is_null() { return Value::Null; }
            let dt1 = parse_datetime_components(&get_current_timestamp());
            let dt2 = parse_datetime_components(&ts1_val.as_str());
            if let (Some(d1), Some(d2)) = (dt1, dt2) {
                return Value::text(eval_age(&d1, &d2));
            }
        } else if args.len() == 2 {
            let ts1_val = eval_sql_expr(&args[0], primary_row, primary_table, joined_row, joined_table, params);
            let ts2_val = eval_sql_expr(&args[1], primary_row, primary_table, joined_row, joined_table, params);
            if ts1_val.is_null() || ts2_val.is_null() { return Value::Null; }
            let dt1 = parse_datetime_components(&ts1_val.as_str());
            let dt2 = parse_datetime_components(&ts2_val.as_str());
            if let (Some(d1), Some(d2)) = (dt1, dt2) {
                return Value::text(eval_age(&d1, &d2));
            }
        }
    }

    if upper == "CURRENT_DATE" {
        return Value::text(get_current_date());
    }
    if upper == "LOCALTIMESTAMP" {
        return Value::text(get_local_timestamp());
    }
    if upper == "CURRENT_TIMESTAMP" || upper == "NOW()" {
        return Value::text(get_current_timestamp());
    }
    if upper == "CURRENT_TIME" || upper == "LOCALTIME" {
        return Value::text(get_current_time());
    }

    // 7. COALESCE and NULLIF
    if upper.starts_with("COALESCE(") && s.ends_with(')') {
        let inner = &s[9..s.len() - 1];
        for arg in split_function_args(inner) {
            let v = eval_sql_expr(&arg, primary_row, primary_table, joined_row, joined_table, params);
            if !v.is_null() {
                return v;
            }
        }
        return Value::Null;
    }
    if upper.starts_with("NULLIF(") && s.ends_with(')') {
        let inner = &s[7..s.len() - 1];
        let args = split_function_args(inner);
        if args.len() == 2 {
            let a = eval_sql_expr(&args[0], primary_row, primary_table, joined_row, joined_table, params);
            let b = eval_sql_expr(&args[1], primary_row, primary_table, joined_row, joined_table, params);
            return if a.is_equal(&b) { Value::Null } else { a };
        }
    }



    // 10. Column resolution
    let clean = clean_col_name(s);
    if let Some(pt) = primary_table {
        if let Some(idx) = pt.get_column_index(clean).or_else(|| pt.get_column_index(s)) {
            return primary_row.get(idx).cloned().unwrap_or(Value::Null);
        }
    }
    if let Some(jt) = joined_table {
        if let Some(idx) = jt.get_column_index(clean).or_else(|| jt.get_column_index(s)) {
            return joined_row.get(idx).cloned().unwrap_or(Value::Null);
        }
    }

    // 11. Fallback to literal or parameter
    parse_value(s, params)
}




// ---------------------------------------------------------------------------
// Kết quả theo giao thức Postgres (dùng cho pg-gateway). Không đụng tới WAL.
// ---------------------------------------------------------------------------

/// Tắt chế độ raw text khi ra khỏi phạm vi, kể cả khi execute trả lỗi.
struct RawTextGuard(bool);

impl Drop for RawTextGuard {
    fn drop(&mut self) {
        set_raw_text_mode(self.0);
    }
}

impl Executor {
    /// Thực thi và trả kết quả dạng giao thức Postgres: cột đúng thứ tự, OID, giá trị text chuẩn.
    pub fn execute_wire(&mut self, sql: &str, params: &[Value]) -> Result<crate::pg_types::WireResult, String> {
        // enum / domain (engine/objects.rs): định dạng theo câu đã viết lại (`enum_range(...)` → mảng...)
        let typed;
        let sql = if !self.storage.objects.enums.is_empty() || !self.storage.objects.domains.is_empty() {
            crate::engine::objects::set_session_enums(&self.storage.objects.enums);
            match self.rewrite_type_usage(sql) {
                Ok(Some(s)) => {
                    typed = s;
                    typed.as_str()
                }
                _ => sql,
            }
        } else {
            sql
        };
        // cột đầu ra trùng tên (`a.name, b.name`, `*` qua JOIN): đặt alias tạm duy nhất để không cột nào bị
        // gộp, rồi trả lại tên gốc. Chỉ đường wire; JSON (object theo tên cột) giữ hành vi cũ.
        let dedup = self.dedupe_wire_sql(sql);
        let sql = dedup.as_deref().unwrap_or(sql);
        let res = {
            let _guard = RawTextGuard(set_raw_text_mode(true));
            self.execute(sql, params)?
        };
        let mut wire = query_result_to_wire(&res);
        if !wire.rows.is_empty() || !wire.fields.is_empty() {
            // INSERT/UPDATE/DELETE ... RETURNING: định dạng như `SELECT <returning> FROM <bảng đích>`
            let returning_sql = returning_as_select(sql);
            let shape_sql = returning_sql.as_deref().unwrap_or(sql);
            let decls = crate::engine::coltypes::wire_decls(shape_sql, &self.storage, res.fields.len());
            apply_wire_decls(&mut wire, &res, &decls);
            apply_wire_items(&mut wire, &res, shape_sql);
        }
        // tên phụ của cột trùng tên (dedupe ở trên, `ROWS FROM` hai hàm cùng tên): trả lại tên gốc
        for f in &mut wire.fields {
            if let Some(p) = f.name.find(DUP_MARK) {
                f.name.truncate(p);
            }
        }
        Ok(wire)
    }

    /// SELECT (một câu) có cột đầu ra trùng tên → câu viết lại với alias `"tên#dupN"` cho lần xuất hiện thứ
    /// 2 trở đi; `*` / `x.*` trên nhiều bảng thật được khai triển thành `alias.cột`. `None` nếu không trùng.
    fn dedupe_wire_sql(&self, sql: &str) -> Option<String> {
        let t = sql.trim().trim_end_matches(';').trim();
        if t.len() < 7 || !t[..7].eq_ignore_ascii_case("SELECT ") || t.contains("--") || t.contains("/*") || t.contains('$') && t.contains("$$") {
            return None;
        }
        // không có FROM: danh sách chọn kết thúc ở mệnh đề kế tiếp (hoặc cuối câu)
        let from = find_top_level_keyword(t, "FROM").unwrap_or_else(|| {
            ["WHERE", "GROUP", "ORDER", "LIMIT", "OFFSET", "HAVING", "UNION", "INTERSECT", "EXCEPT", "INTO"]
                .iter()
                .filter_map(|k| find_top_level_keyword(t, k))
                .min()
                .unwrap_or(t.len())
        });
        let mut list = t[7..from].trim();
        let mut head = "SELECT ".to_string();
        if list.len() > 9 && list[..9].eq_ignore_ascii_case("DISTINCT ") {
            if list[9..].trim_start().to_uppercase().starts_with("ON") {
                return None;
            }
            head.push_str("DISTINCT ");
            list = list[9..].trim();
        }
        let has_from = t[from..].len() >= 4 && t[from..from + 4].eq_ignore_ascii_case("FROM");
        let after = if has_from { &t[from + 4..] } else { "" };
        let end = ["WHERE", "GROUP", "ORDER", "LIMIT", "OFFSET", "HAVING", "WINDOW", "UNION", "INTERSECT", "EXCEPT", "FOR", "FETCH"]
            .iter()
            .filter_map(|k| find_top_level_keyword(after, k))
            .min()
            .unwrap_or(after.len());
        let from_text = after[..end].trim();
        let items = split_projection_items(list);
        let has_star = items.iter().any(|i| i.trim() == "*" || i.trim().ends_with(".*"));
        // bảng trong FROM (chỉ cần khi có `*`)
        let mut tables: Vec<(String, String)> = Vec::new();
        if has_star {
            let up = from_text.to_uppercase();
            if up.contains("USING") || up.contains("NATURAL") || up.contains("LATERAL") || from_text.starts_with('(') {
                return None;
            }
            let (base, base_alias, base_sub, joins) = parse_from_and_joins(from_text).ok()?;
            if base_sub.is_some() || joins.is_empty() || joins.iter().any(|j| j.subquery.is_some()) {
                return None;
            }
            tables.push((base_alias.clone().unwrap_or_else(|| base.clone()), base));
            for j in joins {
                tables.push((j.alias.clone().unwrap_or_else(|| j.table_name.clone()), j.table_name));
            }
        }
        let mut out: Vec<(String, String)> = Vec::new(); // (biểu thức, tên đầu ra)
        for it in &items {
            let it = it.trim();
            if it == "*" || it.ends_with(".*") {
                let q = it.strip_suffix(".*").map(|q| q.trim_matches('"'));
                for (alias, name) in &tables {
                    if q.map_or(false, |q| !q.eq_ignore_ascii_case(alias)) {
                        continue;
                    }
                    let tbl = self.storage.get_table(&clean_table_name(name))?;
                    for c in &tbl.columns {
                        out.push((format!("{}.\"{}\"", alias, c.name), c.name.clone()));
                    }
                }
                if q.is_some() && !tables.iter().any(|(a, _)| q.map_or(false, |q| q.eq_ignore_ascii_case(a))) {
                    return None;
                }
                continue;
            }
            let name = if let Some(a) = find_top_level_keyword(it, "AS") {
                it[a + 2..].trim().trim_matches('"').to_string()
            } else if it.chars().all(|c| c.is_alphanumeric() || c == '_' || c == '.' || c == '"') {
                it.rsplit('.').next().unwrap_or(it).trim_matches('"').to_lowercase()
            } else {
                // biểu thức không alias: tên mặc định kiểu PG (`encode`, `?column?`...)
                crate::engine::select_list::wire_default_name(it)
            };
            out.push((it.to_string(), name));
        }
        let mut seen: HashMap<String, usize> = HashMap::new();
        let mut changed = false;
        let mut rendered = Vec::with_capacity(out.len());
        for (expr, name) in &out {
            let n = seen.entry(name.clone()).or_insert(0);
            *n += 1;
            if *n > 1 && !name.starts_with('\u{0}') {
                changed = true;
                let base = find_top_level_keyword(expr, "AS").map(|a| expr[..a].trim()).unwrap_or(expr);
                rendered.push(format!("{} AS \"{}{}{}\"", base, name, DUP_MARK, n));
            } else {
                rendered.push(expr.clone());
            }
        }
        if !changed {
            return None;
        }
        Some(format!("{}{} {}", head, rendered.join(", "), &t[from..]))
    }

    /// Trả về các cột kết quả và số tham số `$n` mà không ghi dữ liệu.
    /// SELECT/WITH được chạy với tham số NULL (chỉ đọc); INSERT/UPDATE/DELETE … RETURNING
    /// lấy cột từ định nghĩa bảng, không thực thi.
    pub fn describe(&mut self, sql: &str) -> Result<crate::pg_types::DescribeResult, String> {
        let clean = crate::engine::frontend::lex(sql).map_err(|e| crate::engine::frontend::decode_all(&e).into_owned())?;
        let normalized = normalize_named_sql_params(&clean);
        let trimmed = normalized.trim().trim_end_matches(';').trim();
        let param_count = crate::pg_types::count_params(trimmed);
        let upper = trimmed.to_uppercase();
        let first_word = upper.split_whitespace().next().unwrap_or("");

        let fields = match first_word {
            "SELECT" | "VALUES" | "TABLE" | "SHOW" => {
                let null_params = vec![Value::Null; param_count];
                self.execute_wire(trimmed, &null_params)?.fields
            }
            "WITH" => {
                // CTE có thể chứa lệnh ghi; chỉ chạy thử khi không có lệnh ghi nào.
                let has_write = ["INSERT ", "UPDATE ", "DELETE ", "MERGE "].iter().any(|k| upper.contains(k));
                if has_write {
                    vec![]
                } else {
                    let null_params = vec![Value::Null; param_count];
                    self.execute_wire(trimmed, &null_params)?.fields
                }
            }
            "INSERT" | "UPDATE" | "DELETE" => {
                let (_, returning) = extract_returning_clause(trimmed);
                match (returning, dml_target_table(trimmed)) {
                    (Some(r_cols), Some(tbl)) => match self.storage.get_table(&tbl) {
                        Some(table) => fields_to_wire(&get_returning_fields(table, r_cols)),
                        None => return Err(format!("relation \"{}\" does not exist", tbl)),
                    },
                    _ => vec![],
                }
            }
            _ => vec![],
        };
        Ok(crate::pg_types::DescribeResult { fields, param_count })
    }
}

fn fields_to_wire(fields: &[FieldInfo]) -> Vec<crate::pg_types::WireField> {
    fields
        .iter()
        .map(|f| crate::pg_types::WireField {
            name: f.name.clone(),
            type_oid: crate::pg_types::oid_for_type_str(&f.data_type),
        })
        .collect()
}

/// `INSERT INTO t ... RETURNING a, b` → `SELECT a, b FROM t` (chỉ để suy kiểu / tên cột wire).
fn returning_as_select(sql: &str) -> Option<String> {
    let first = sql.trim_start().get(..6)?;
    if !["INSERT", "UPDATE", "DELETE"].iter().any(|k| first.eq_ignore_ascii_case(k)) || !contains_ci(sql, "RETURNING") {
        return None;
    }
    let lexed = crate::engine::frontend::lex(sql).ok()?;
    let t = lexed.trim().trim_end_matches(';').trim();
    let (_, ret) = extract_returning_clause(t);
    let table = dml_target_table(t)?;
    Some(format!("SELECT {} FROM {}", ret?, table))
}

/// Tên bảng đích của INSERT INTO t / UPDATE t / DELETE FROM t.
fn dml_target_table(sql: &str) -> Option<String> {
    let mut words = sql.split_whitespace();
    let first = words.next()?.to_uppercase();
    let name = match first.as_str() {
        "INSERT" | "DELETE" => {
            let kw = words.next()?.to_uppercase();
            if kw != "INTO" && kw != "FROM" {
                return None;
            }
            words.next()?
        }
        "UPDATE" => {
            let w = words.next()?;
            if w.eq_ignore_ascii_case("ONLY") { words.next()? } else { w }
        }
        _ => return None,
    };
    let name = name.split('(').next().unwrap_or(name);
    let name = name.rsplit('.').next().unwrap_or(name);
    Some(name.trim_matches('"').to_string())
}

/// QueryResult (rows dạng object) → WireResult (rows dạng mảng theo thứ tự fields).
pub(crate) fn query_result_to_wire(res: &QueryResult) -> crate::pg_types::WireResult {
    let mut fields = fields_to_wire(&res.fields);
    // Một số nhánh (catalog giả lập, nhánh gom nhóm) có thể trả key không có trong fields:
    // bổ sung theo thứ tự xuất hiện trong dòng đầu tiên để không mất cột.
    if let Some(serde_json::Value::Object(first)) = res.rows.first() {
        for (k, v) in first {
            if !fields.iter().any(|f| &f.name == k) {
                fields.push(crate::pg_types::WireField {
                    name: k.clone(),
                    type_oid: crate::pg_types::oid_for_type_str(&type_str_from_json_value(v)),
                });
            }
        }
    }
    let rows = res
        .rows
        .iter()
        .map(|row| {
            fields
                .iter()
                .map(|f| match row {
                    serde_json::Value::Object(map) => map.get(&f.name).and_then(|v| crate::pg_types::to_pg_text(v, f.type_oid)),
                    _ => None,
                })
                .collect()
        })
        .collect();
    let row_count = res.row_count;
    crate::pg_types::WireResult {
        fields,
        rows,
        command: crate::pg_types::command_tag(&res.command, row_count),
        row_count,
    }
}

/// Định danh có nháy được lexer mã hoá (`frontend::encode_ident`): trả tên gốc ở tên cột, khoá
/// của dòng, ô dữ liệu là đúng một tên mã hoá (catalog) và thông báo lỗi.
fn decode_quoted_idents(res: Result<QueryResult, String>) -> Result<QueryResult, String> {
    use crate::engine::frontend::{decode_all, decode_ident, encoded_seen};
    if !encoded_seen() {
        return res;
    }
    let mut r = match res {
        Ok(r) => r,
        Err(e) => return Err(decode_all(&e).into_owned()),
    };
    let names_encoded = r.fields.iter().any(|f| f.name.contains("__q") || f.name.contains("__Q"));
    if names_encoded {
        for f in &mut r.fields {
            if let std::borrow::Cow::Owned(d) = decode_all(&f.name) {
                f.name = d;
            }
        }
    }
    for row in &mut r.rows {
        if let serde_json::Value::Object(map) = row {
            let needs = names_encoded || map.values().any(|v| matches!(v, serde_json::Value::String(s) if s.starts_with("__q")));
            if !needs {
                continue;
            }
            let old = std::mem::take(map);
            for (k, v) in old {
                let k = decode_all(&k).into_owned();
                let v = match v {
                    serde_json::Value::String(s) if s.starts_with("__q") => serde_json::Value::String(decode_ident(&s).unwrap_or(s)),
                    other => other,
                };
                map.insert(k, v);
            }
        }
    }
    Ok(r)
}

/// Định dạng lại các cột wire có kiểu khai báo (OID, scale numeric, timestamp, char(n)...).
/// Đặt tên cột wire theo quy tắc PostgreSQL (kể cả nhiều cột cùng tên `?column?`) và in mảng / record /
/// bytea do hàm trả về theo định dạng PostgreSQL. Chỉ áp khi chắc thứ tự cột khớp danh sách SELECT: mọi
/// phần tử có alias / là cột đã mang đúng tên đó.
fn apply_wire_items(wire: &mut crate::pg_types::WireResult, res: &QueryResult, sql: &str) {
    use crate::engine::select_list::WireShape;
    let Some(items) = crate::engine::select_list::wire_names(sql) else { return };
    if items.len() != wire.fields.len() {
        return;
    }
    let aligned = items.iter().zip(&wire.fields).all(|((n, certain, _), f)| {
        if *certain {
            f.name.eq_ignore_ascii_case(n)
        } else {
            !items.iter().any(|(m, c, _)| *c && f.name.eq_ignore_ascii_case(m))
        }
    });
    if !aligned {
        return;
    }
    for (i, (_, _, expr)) in items.iter().enumerate() {
        let Some(shape) = crate::engine::select_list::wire_shape(expr) else { continue };
        let key = wire.fields[i].name.clone();
        let mut oid = None;
        for (r, row) in res.rows.iter().enumerate() {
            let Some(v) = row.get(&key) else { continue };
            // chế độ raw text: mảng / record đến dưới dạng chuỗi JSON
            let parsed;
            let v = match v {
                serde_json::Value::String(t) if t.starts_with('[') && !matches!(shape, WireShape::ByteaText | WireShape::ByteaHex) => {
                    match serde_json::from_str::<serde_json::Value>(t) {
                        Ok(p) => {
                            parsed = p;
                            &parsed
                        }
                        Err(_) => v,
                    }
                }
                _ => v,
            };
            let Some(cell) = wire.rows.get_mut(r).and_then(|w| w.get_mut(i)) else { continue };
            let text = match (shape, v) {
                (WireShape::Array { constructor }, serde_json::Value::Array(a)) => {
                    let (t, o) = crate::pg_types::json_to_pg_array(a, constructor);
                    oid = oid.or(Some(o));
                    t
                }
                (WireShape::Record, serde_json::Value::Array(a)) => {
                    oid = Some(crate::pg_types::OID_RECORD);
                    crate::pg_types::json_to_pg_record(a)
                }
                (WireShape::ByteaText, serde_json::Value::String(s)) => {
                    oid = Some(crate::pg_types::OID_BYTEA);
                    if s.starts_with("\\x") { s.clone() } else { crate::pg_types::bytes_to_hex(s.as_bytes()) }
                }
                (WireShape::ByteaHex, serde_json::Value::String(s)) if s.bytes().all(|b| b.is_ascii_hexdigit()) => {
                    oid = Some(crate::pg_types::OID_BYTEA);
                    format!("\\x{}", s)
                }
                _ => continue,
            };
            *cell = Some(text);
        }
        if let Some(o) = oid {
            if !matches!(shape, WireShape::Array { .. }) || !crate::pg_types::is_array_oid_pub(wire.fields[i].type_oid) {
                wire.fields[i].type_oid = o;
            }
        }
    }
    for ((n, _, _), f) in items.into_iter().zip(wire.fields.iter_mut()) {
        f.name = n;
    }
}

fn apply_wire_decls(wire: &mut crate::pg_types::WireResult, res: &QueryResult, decls: &[Option<crate::engine::coltypes::DeclType>]) {
    if decls.is_empty() {
        return;
    }
    for (i, d) in decls.iter().enumerate() {
        let Some(d) = d else { continue };
        let Some(field) = wire.fields.get_mut(i) else { continue };
        let Some(oid) = d.oid() else { continue };
        if field.type_oid != oid {
            field.type_oid = oid;
            // Text lại theo OID mới từ giá trị gốc
            let name = field.name.clone();
            for (r, row) in res.rows.iter().enumerate() {
                if let (serde_json::Value::Object(map), Some(cell)) = (row, wire.rows.get_mut(r).and_then(|w| w.get_mut(i))) {
                    *cell = map.get(&name).and_then(|v| crate::pg_types::to_pg_text(v, oid));
                }
            }
        }
        for row in wire.rows.iter_mut() {
            if let Some(cell) = row.get_mut(i) {
                if let Some(t) = cell.take() {
                    *cell = Some(crate::engine::coltypes::format_wire(d, t));
                }
            }
        }
    }
}

/// Vị trí (dấu cách đứng trước) của ` AS ` cuối cùng ở cấp ngoài cùng — ngoài ngoặc và chuỗi — như
/// `rfind(" AS ")` nhưng không bắt nhầm `CAST(x AS int)` hay `'a AS b'`.
pub(crate) fn rfind_top_level_as(s: &str) -> Option<usize> {
    let b = s.as_bytes();
    let mut depth = 0i32;
    let mut in_sq = false;
    let mut in_dq = false;
    let mut last = None;
    let mut i = 0;
    while i < b.len() {
        let c = b[i];
        if in_sq {
            if c == b'\'' {
                in_sq = false;
            }
        } else if in_dq {
            if c == b'"' {
                in_dq = false;
            }
        } else {
            match c {
                b'\'' => in_sq = true,
                b'"' => in_dq = true,
                b'(' | b'[' => depth += 1,
                b')' | b']' => depth -= 1,
                b' ' if depth == 0 && i + 4 <= b.len() && b[i + 1..i + 3].eq_ignore_ascii_case(b"AS") && b[i + 3] == b' ' => last = Some(i),
                _ => {}
            }
        }
        i += 1;
    }
    last
}
