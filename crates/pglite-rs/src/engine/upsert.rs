//! INSERT: `VALUES` / `SELECT` / `DEFAULT VALUES`, `ON CONFLICT ... DO NOTHING | DO UPDATE SET ...
//! [WHERE ...]` với `EXCLUDED.col`, và `RETURNING` (cột, `*`, biểu thức).
//!
//! Mọi thay đổi được ghi bằng các bản ghi WAL cũ (`Insert`, `Update*`, `Delete*`), nên binary cũ đọc
//! được dữ liệu của upsert. Lỗi giữa chừng thì `end_statement` hoàn tác cả lệnh.
use super::constraints::{matching_paren, strip_schema};
use super::executor::{
    coerce_update_val, duplicate_pk_error, eval_default_for_insert, eval_sql_expr, extract_value_groups, find_top_level_keyword, pk_dedup_key,
    returning_fields, returning_row, split_comma_separated_tokens, Executor,
};
use crate::storage::engine::UndoAction;
use crate::types::{QueryResult, Value};

enum ConflictTarget {
    /// `ON CONFLICT DO ...`: mọi ràng buộc UNIQUE / khoá chính.
    Any,
    Columns(Vec<String>),
    Constraint(String),
}

enum ConflictAction {
    Nothing,
    Update { assignments: Vec<(String, String)>, where_clause: Option<String> },
}

struct OnConflict {
    target: ConflictTarget,
    action: ConflictAction,
}

/// Vị trí `ON CONFLICT` ở cấp ngoài cùng (không nằm trong chuỗi / ngoặc).
fn find_on_conflict(s: &str) -> Option<usize> {
    let mut from = 0;
    while let Some(rel) = find_top_level_keyword(&s[from..], "CONFLICT") {
        let pos = from + rel;
        let before = s[..pos].trim_end();
        if before.len() >= 2 && before.get(before.len() - 2..).map_or(false, |x| x.eq_ignore_ascii_case("ON")) {
            let on_pos = before.len() - 2;
            if on_pos == 0 || !s.as_bytes()[on_pos - 1].is_ascii_alphanumeric() {
                return Some(on_pos);
            }
        }
        from = pos + 8;
    }
    None
}

fn parse_on_conflict(clause: &str) -> Result<OnConflict, String> {
    // clause bắt đầu bằng "ON CONFLICT"
    let mut rest = clause.trim()[2..].trim_start()[8..].trim_start();
    let mut target = ConflictTarget::Any;
    if rest.starts_with('(') {
        let close = matching_paren(rest, 0).ok_or("Missing ')' in ON CONFLICT")?;
        let cols = split_comma_separated_tokens(&rest[1..close])
            .into_iter()
            .map(|c| {
                let first = c.split_whitespace().next().unwrap_or(c);
                strip_schema(first)
            })
            .collect();
        target = ConflictTarget::Columns(cols);
        rest = rest[close + 1..].trim_start();
        // ON CONFLICT (cols) WHERE pred: điều kiện chọn index một phần, bỏ qua (chọn theo cột)
        if rest.len() > 5 && rest.get(..5).map_or(false, |p| p.eq_ignore_ascii_case("WHERE")) {
            let do_pos = find_top_level_keyword(rest, "DO").ok_or("Missing DO in ON CONFLICT")?;
            rest = rest[do_pos..].trim_start();
        }
    } else if rest.len() > 13 && rest.get(..13).map_or(false, |p| p.eq_ignore_ascii_case("ON CONSTRAINT")) {
        let after = rest[13..].trim_start();
        let end = after.find(char::is_whitespace).unwrap_or(after.len());
        target = ConflictTarget::Constraint(strip_schema(&after[..end]));
        rest = after[end..].trim_start();
    }
    if !(rest.len() >= 2 && rest.get(..2).map_or(false, |p| p.eq_ignore_ascii_case("DO"))) {
        return Err("syntax error at or near ON CONFLICT: expected DO NOTHING or DO UPDATE".to_string());
    }
    rest = rest[2..].trim_start();
    let up = rest.to_uppercase();
    if up.starts_with("NOTHING") {
        return Ok(OnConflict { target, action: ConflictAction::Nothing });
    }
    if !up.starts_with("UPDATE") {
        return Err("syntax error in ON CONFLICT: expected DO NOTHING or DO UPDATE".to_string());
    }
    if matches!(target, ConflictTarget::Any) {
        return Err("ON CONFLICT DO UPDATE requires inference specification or constraint name".to_string());
    }
    rest = rest[6..].trim_start();
    if !(rest.len() >= 3 && rest.get(..3).map_or(false, |p| p.eq_ignore_ascii_case("SET"))) {
        return Err("syntax error in ON CONFLICT DO UPDATE: expected SET".to_string());
    }
    rest = rest[3..].trim_start();
    let (set_part, where_clause) = match find_top_level_keyword(rest, "WHERE") {
        Some(w) => (&rest[..w], Some(rest[w + 5..].trim().to_string())),
        None => (rest, None),
    };
    let mut assignments = Vec::new();
    for a in split_comma_separated_tokens(set_part) {
        let eq = find_assign_eq(a).ok_or("Invalid assignment in ON CONFLICT DO UPDATE SET")?;
        let col = a[..eq].trim();
        let col = col.rsplit('.').next().unwrap_or(col);
        assignments.push((strip_schema(col), a[eq + 1..].trim().to_string()));
    }
    Ok(OnConflict { target, action: ConflictAction::Update { assignments, where_clause } })
}

/// `=` của phép gán (bỏ qua `=` nằm trong chuỗi hay ngoặc, và các toán tử `<=`, `>=`, `!=`).
fn find_assign_eq(s: &str) -> Option<usize> {
    let b = s.as_bytes();
    let (mut depth, mut sq, mut dq) = (0i32, false, false);
    for i in 0..b.len() {
        match b[i] {
            b'\'' if !dq => sq = !sq,
            b'"' if !sq => dq = !dq,
            b'(' if !sq && !dq => depth += 1,
            b')' if !sq && !dq => depth -= 1,
            b'=' if !sq && !dq && depth == 0 => {
                let prev = if i > 0 { b[i - 1] } else { b' ' };
                if prev != b'<' && prev != b'>' && prev != b'!' {
                    return Some(i);
                }
            }
            _ => {}
        }
    }
    None
}

/// Thay `EXCLUDED.col` bằng tham số `$n` trỏ vào giá trị của dòng định chèn.
fn substitute_excluded(expr: &str, columns: &[crate::types::ColumnDef], base: usize) -> String {
    let bytes = expr.as_bytes();
    let mut out = String::with_capacity(expr.len());
    let (mut i, mut sq) = (0usize, false);
    while i < bytes.len() {
        let c = bytes[i];
        if c == b'\'' {
            sq = !sq;
            out.push('\'');
            i += 1;
            continue;
        }
        if !sq && i + 9 <= bytes.len() && expr.get(i..i + 9).map_or(false, |x| x.eq_ignore_ascii_case("excluded.")) && (i == 0 || !(ident_byte(bytes[i - 1]) || bytes[i - 1] == b'.')) {
            let start = i + 9;
            let (name, end) = if bytes.get(start) == Some(&b'"') {
                let close = expr[start + 1..].find('"').map(|p| start + 1 + p).unwrap_or(bytes.len() - 1);
                (expr[start + 1..close].to_string(), close + 1)
            } else {
                let end = (start..bytes.len()).find(|&j| !ident_byte(bytes[j])).unwrap_or(bytes.len());
                (expr[start..end].to_string(), end)
            };
            if let Some(idx) = columns.iter().position(|c| c.name.eq_ignore_ascii_case(&name)) {
                out.push_str(&format!("${}", base + idx + 1));
                i = end;
                continue;
            }
        }
        // giữ nguyên ký tự (UTF-8 an toàn: chép cả chuỗi byte của ký tự)
        let ch_len = utf8_len(c);
        out.push_str(&expr[i..i + ch_len]);
        i += ch_len;
    }
    out
}

/// Byte thuộc một định danh: chữ, số, `_`, hoặc byte của ký tự không phải ASCII (tên cột tiếng Việt).
fn ident_byte(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'_' || b >= 0x80
}

fn utf8_len(b: u8) -> usize {
    match b {
        0x00..=0x7F => 1,
        0xC0..=0xDF => 2,
        0xE0..=0xEF => 3,
        _ => 4,
    }
}

/// Giá trị của một phần tử trong `VALUES (...)`.
fn eval_value_token(token: &str, params: &[Value], param_cursor: &mut usize, default: Option<&String>) -> Value {
    let token = token.trim();
    if let Some(num) = token.strip_prefix('$') {
        if let Ok(p_num) = num.parse::<usize>() {
            return if p_num >= 1 && p_num <= params.len() {
                params[p_num - 1].clone()
            } else if *param_cursor < params.len() {
                let v = params[*param_cursor].clone();
                *param_cursor += 1;
                v
            } else {
                Value::Null
            };
        }
        // `$1::jsonb`, `$2::int`...: qua bộ đánh giá biểu thức (có ép kiểu)
        return eval_sql_expr(token, &[], None, &[], None, params);
    }
    if token.eq_ignore_ascii_case("DEFAULT") {
        return default.map(|d| eval_default_for_insert(d, params)).unwrap_or(Value::Null);
    }
    if token.eq_ignore_ascii_case("NULL") {
        return Value::Null;
    }
    if token.eq_ignore_ascii_case("TRUE") {
        return Value::Bool(true);
    }
    if token.eq_ignore_ascii_case("FALSE") {
        return Value::Bool(false);
    }
    if token.len() >= 2 && token.starts_with('\'') && token.ends_with('\'') && !token[1..token.len() - 1].replace("''", "").contains('\'') {
        return Value::text(token[1..token.len() - 1].replace("''", "'"));
    }
    if let Ok(i) = token.parse::<i64>() {
        return Value::Int(i);
    }
    if let Ok(f) = token.parse::<f64>() {
        return Value::Float(f);
    }
    eval_sql_expr(token, &[], None, &[], None, params)
}

impl Executor {
    pub(crate) fn handle_insert(&mut self, sql: &str, params: &[Value]) -> Result<QueryResult, String> {
        // RETURNING ở cấp ngoài cùng (sau ON CONFLICT nếu có)
        let (body, returning) = match find_top_level_keyword(sql, "RETURNING") {
            Some(p) => (sql[..p].trim(), Some(sql[p + 9..].trim().trim_end_matches(';').trim())),
            None => (sql.trim().trim_end_matches(';').trim(), None),
        };
        let (body, on_conflict) = match find_on_conflict(body) {
            Some(p) => (body[..p].trim(), Some(parse_on_conflict(&body[p..])?)),
            None => (body, None),
        };

        let into = find_top_level_keyword(body, "INTO").ok_or("Missing INTO in INSERT")?;
        let after_into = body[into + 4..].trim_start();
        // Nguồn dữ liệu: VALUES / SELECT / WITH / DEFAULT VALUES
        let src_pos = ["VALUES", "SELECT", "WITH", "DEFAULT", "OVERRIDING"]
            .iter()
            .filter_map(|kw| find_top_level_keyword(after_into, kw))
            .min()
            .ok_or("Missing VALUES clause in INSERT")?;
        let head = after_into[..src_pos].trim();
        let mut source = after_into[src_pos..].trim();
        if source.len() > 10 && source.get(..10).map_or(false, |p| p.eq_ignore_ascii_case("OVERRIDING")) {
            // OVERRIDING SYSTEM|USER VALUE: bỏ qua
            let v = find_top_level_keyword(source, "VALUE").map(|p| p + 5).unwrap_or(source.len());
            source = source[v..].trim();
        }
        let (table_part, target_cols) = match head.find('(') {
            Some(open) => {
                let close = matching_paren(head, open).ok_or("Missing closing parenthesis for columns")?;
                let cols: Vec<String> = split_comma_separated_tokens(&head[open + 1..close]).into_iter().map(|c| c.trim().trim_matches('"').to_string()).collect();
                (head[..open].trim(), Some(cols))
            }
            None => (head, None),
        };
        // bỏ alias: "t AS x" / "t x"
        let table_name = {
            let first = table_part.split_whitespace().next().unwrap_or("");
            super::executor::strip_schema_keep_case(first)
        };

        let (col_indices, col_defs, clean_table_name) = {
            let table = self.storage.get_table(&table_name).ok_or_else(|| format!("Table {} not found", table_name))?;
            let indices: Vec<usize> = match &target_cols {
                Some(cols) => cols
                    .iter()
                    .map(|c| table.get_column_index(c).ok_or_else(|| format!("Column {} not found in {}", c, table_name)))
                    .collect::<Result<_, _>>()?,
                None => (0..table.columns.len()).collect(),
            };
            (indices, table.columns.clone(), table.name.to_lowercase())
        };

        // 1. Dựng mọi dòng định chèn (chưa sửa gì)
        let defaults_row = |params: &[Value]| -> Vec<Value> {
            col_defs.iter().map(|c| c.default_value.as_ref().map(|d| eval_default_for_insert(d, params)).unwrap_or(Value::Null)).collect()
        };
        let mut built: Vec<Vec<Value>> = Vec::new();
        let up_src = source.to_uppercase();
        if up_src.starts_with("DEFAULT VALUES") {
            built.push(defaults_row(params));
        } else if up_src.starts_with("VALUES") {
            let mut param_cursor = 0;
            for group in extract_value_groups(&source[6..]) {
                let mut row = defaults_row(params);
                let tokens = split_comma_separated_tokens(group);
                if tokens.len() > col_indices.len() {
                    return Err("INSERT has more expressions than target columns".to_string());
                }
                for (ptr, token) in tokens.into_iter().enumerate() {
                    let Some(&target_idx) = col_indices.get(ptr) else { break };
                    let val = eval_value_token(token, params, &mut param_cursor, col_defs[target_idx].default_value.as_ref());
                    row[target_idx] = coerce_update_val(&col_defs[target_idx], val);
                }
                built.push(row);
            }
        } else {
            // INSERT ... SELECT / WITH ... SELECT
            let res = self.execute_statement(source, params)?;
            for r in res.rows {
                let mut row = defaults_row(params);
                if let serde_json::Value::Object(map) = r {
                    for (ptr, (_, v)) in map.into_iter().enumerate() {
                        let Some(&target_idx) = col_indices.get(ptr) else { break };
                        let val = super::executor::json_to_value(&v);
                        let val = match (&v, &col_defs[target_idx].data_type) {
                            (serde_json::Value::Object(_) | serde_json::Value::Array(_), _) => Value::text(v.to_string()),
                            _ => val,
                        };
                        row[target_idx] = coerce_update_val(&col_defs[target_idx], val);
                    }
                }
                built.push(row);
            }
        }

        let upsert = on_conflict.is_some();
        let referenced = on_conflict.as_ref().map_or(false, |oc| matches!(oc.action, ConflictAction::Update { .. })) && self.is_referenced(&clean_table_name);
        let undo = self.storage.undo_enabled();
        let mut returned_rows = Vec::new();
        let mut inserted: Vec<usize> = Vec::new();
        let mut updated: Vec<usize> = Vec::new();
        let mut changed_cols: Vec<usize> = Vec::new();
        let mut old_rows: Vec<(usize, Vec<Value>)> = Vec::new();

        match on_conflict {
            None => {
                // Đường nhanh như trước: kiểm tra khoá chính trùng cho cả lệnh trước khi chèn.
                let table = self.storage.tables.get_mut(&clean_table_name).unwrap();
                if table.pk_enforced {
                    if let Some(pk_idx) = table.pk_col_idx {
                        let mut seen = std::collections::HashSet::new();
                        for row in &built {
                            let v = &row[pk_idx];
                            if v.is_null() || matches!(v, Value::Text(t) if t.trim().is_empty()) {
                                continue;
                            }
                            if table.live_row_with_pk(v, None).is_some() || !seen.insert(pk_dedup_key(v)) {
                                return Err(duplicate_pk_error(table, v));
                            }
                        }
                    }
                }
                let count = built.len();
                table.rows.reserve(count);
                table.is_deleted.reserve(count);
                let has_wal = self.storage.wal.writer.is_some();
                let wal = &mut self.storage.wal;
                let first = table.rows.len();
                for row in built {
                    table.insert(row);
                    let last_row = table.rows.last().unwrap();
                    if let Some(r) = returning {
                        returned_rows.push(returning_row(table, last_row, r, params));
                    }
                    if has_wal {
                        wal.append_ref(crate::storage::wal::WalRecordRef::Insert { table: &clean_table_name, row: last_row });
                    }
                }
                inserted.extend(first..first + count);
                if undo && count > 0 {
                    self.storage.tx_undo_log.push(UndoAction::DeleteInsertedRows { table: clean_table_name.clone(), count });
                }
            }
            Some(oc) => {
                // Upsert: xử lý tuần tự, dòng sau thấy dòng trước (như PostgreSQL).
                let arbiters = self.conflict_arbiters(&clean_table_name, &oc.target)?;
                let assignments: Vec<(usize, String)> = match &oc.action {
                    ConflictAction::Update { assignments, .. } => assignments
                        .iter()
                        .map(|(c, e)| {
                            let idx = col_defs.iter().position(|d| d.name.eq_ignore_ascii_case(c)).ok_or_else(|| format!("column \"{}\" of relation \"{}\" does not exist", c, table_name))?;
                            Ok((idx, substitute_excluded(e, &col_defs, params.len())))
                        })
                        .collect::<Result<_, String>>()?,
                    ConflictAction::Nothing => Vec::new(),
                };
                changed_cols.extend(assignments.iter().map(|(c, _)| *c));
                let where_sub = match &oc.action {
                    ConflictAction::Update { where_clause: Some(w), .. } => Some(substitute_excluded(w, &col_defs, params.len())),
                    _ => None,
                };
                let mut affected: std::collections::HashSet<usize> = std::collections::HashSet::new();
                for row in built {
                    let conflict = self.find_conflict(&clean_table_name, &arbiters, &row);
                    match conflict {
                        None => {
                            let (idx, ret) = {
                                let table = self.storage.tables.get_mut(&clean_table_name).unwrap();
                                table.insert(row);
                                let idx = table.rows.len() - 1;
                                let ret = returning.map(|r| returning_row(table, &table.rows[idx], r, params));
                                (idx, ret)
                            };
                            let row_ref = self.storage.tables[&clean_table_name].rows[idx].clone();
                            self.storage.wal.append(crate::storage::wal::WalRecord::Insert { table: clean_table_name.clone(), row: row_ref });
                            if undo {
                                self.storage.tx_undo_log.push(UndoAction::DeleteInsertedRows { table: clean_table_name.clone(), count: 1 });
                            }
                            // Ràng buộc khác (không phải arbiter) vẫn phải đúng, kiểm tra ngay để dòng sau
                            // không dựa vào một dòng sai.
                            self.verify_rows(&clean_table_name, &[idx], None)?;
                            affected.insert(idx);
                            inserted.push(idx);
                            if let Some(r) = ret {
                                returned_rows.push(r);
                            }
                        }
                        Some(existing) => {
                            if assignments.is_empty() {
                                continue; // DO NOTHING
                            }
                            if affected.contains(&existing) {
                                return Err("ON CONFLICT DO UPDATE command cannot affect row a second time (Ensure that no rows proposed for insertion within the same command have duplicate constrained values.)".to_string());
                            }
                            let mut ext_params = params.to_vec();
                            ext_params.extend(row.iter().cloned());
                            let (updates, pass) = {
                                let table = &self.storage.tables[&clean_table_name];
                                let cur = &table.rows[existing];
                                let pass = match &where_sub {
                                    Some(w) => crate::engine::expr::compile_expr(w, Some(table), None, None, None).eval_bool(cur, None, &ext_params) == Some(true),
                                    None => true,
                                };
                                let updates: Vec<(usize, Value)> = assignments
                                    .iter()
                                    .map(|(c, e)| {
                                        let v = super::executor::evaluate_assignment_val_pub(e, cur, table, &ext_params);
                                        (*c, coerce_update_val(&table.columns[*c], v))
                                    })
                                    .collect();
                                (updates, pass)
                            };
                            if !pass {
                                continue;
                            }
                            if referenced {
                                old_rows.push((existing, self.storage.tables[&clean_table_name].rows[existing].clone()));
                            }
                            // Khoá chính đổi thành khoá đã có: lỗi như UPDATE thường
                            {
                                let table = &self.storage.tables[&clean_table_name];
                                if table.pk_enforced {
                                    if let Some(pk) = table.pk_col_idx {
                                        if let Some((_, v)) = updates.iter().find(|(c, _)| *c == pk) {
                                            if table.live_row_with_pk(v, Some(existing)).is_some() {
                                                return Err(duplicate_pk_error(table, v));
                                            }
                                        }
                                    }
                                }
                            }
                            self.update_row_logged(&clean_table_name, existing, updates);
                            self.verify_rows(&clean_table_name, &[existing], Some(&changed_cols))?;
                            affected.insert(existing);
                            updated.push(existing);
                            if let Some(r) = returning {
                                let table = &self.storage.tables[&clean_table_name];
                                returned_rows.push(returning_row(table, &table.rows[existing], r, params));
                            }
                        }
                    }
                }
            }
        }

        if !inserted.is_empty() && !upsert {
            self.verify_rows(&clean_table_name, &inserted, None)?;
        }
        if referenced && !old_rows.is_empty() {
            self.fk_on_parent_update(&clean_table_name, &old_rows)?;
        }

        let fields = match returning {
            Some(r) => returning_fields(&self.storage.tables[&clean_table_name], r, &returned_rows),
            None => vec![],
        };
        Ok(QueryResult {
            row_count: inserted.len() + updated.len(),
            rows: returned_rows,
            fields,
            command: "INSERT".to_string(),
        })
    }

    /// Các index dùng để phát hiện xung đột cho `ON CONFLICT`.
    fn conflict_arbiters(&self, table: &str, target: &ConflictTarget) -> Result<Vec<Arbiter>, String> {
        let t = &self.storage.tables[table];
        let pk = t.pk_col_idx.filter(|_| t.pk_enforced || t.constraints.is_empty());
        let mut out = Vec::new();
        match target {
            ConflictTarget::Any => {
                if let Some(p) = pk {
                    out.push(Arbiter::Pk(p));
                }
                out.extend((0..t.uniq.len()).map(Arbiter::Unique));
            }
            ConflictTarget::Constraint(name) => {
                if name.eq_ignore_ascii_case(&format!("{}_pkey", t.name.to_lowercase())) {
                    if let Some(p) = t.pk_col_idx {
                        out.push(Arbiter::Pk(p));
                    }
                }
                out.extend(t.uniq.iter().enumerate().filter(|(_, u)| u.name.eq_ignore_ascii_case(name)).map(|(i, _)| Arbiter::Unique(i)));
                if out.is_empty() {
                    return Err(format!("constraint \"{}\" for table \"{}\" does not exist", name, t.name.to_lowercase()));
                }
            }
            ConflictTarget::Columns(cols) => {
                let idx: Vec<usize> = cols
                    .iter()
                    .map(|c| t.get_column_index(c).ok_or_else(|| format!("column \"{}\" does not exist", c)))
                    .collect::<Result<_, _>>()?;
                if idx.len() == 1 && t.pk_col_idx == Some(idx[0]) {
                    out.push(Arbiter::Pk(idx[0]));
                }
                for (i, u) in t.uniq.iter().enumerate() {
                    if u.cols.len() == idx.len() && u.cols.iter().all(|c| idx.contains(c)) {
                        out.push(Arbiter::Unique(i));
                    }
                }
                if out.is_empty() {
                    // Bảng do bản cũ tạo (UNIQUE chưa được lưu) hoặc cột không có index UNIQUE: vẫn upsert
                    // theo đúng các cột đó bằng cách tìm dòng trùng giá trị, thay vì làm hỏng lệnh của app.
                    out.push(Arbiter::Scan(idx));
                }
            }
        }
        Ok(out)
    }

    fn find_conflict(&self, table: &str, arbiters: &[Arbiter], row: &[Value]) -> Option<usize> {
        let t = &self.storage.tables[table];
        for a in arbiters {
            let hit = match a {
                Arbiter::Pk(p) => {
                    let v = &row[*p];
                    if v.is_null() || matches!(v, Value::Text(s) if s.trim().is_empty()) {
                        None
                    } else {
                        t.live_row_with_pk(v, None)
                    }
                }
                Arbiter::Unique(i) => t.unique_conflict(*i, row, None),
                Arbiter::Scan(cols) => {
                    let parts: Option<Vec<_>> = cols.iter().map(|&c| crate::storage::catalog::ukey_part(&row[c])).collect();
                    parts.and_then(|key| {
                        (0..t.rows.len()).find(|&r| {
                            !t.is_deleted[r] && cols.iter().zip(&key).all(|(&c, k)| crate::storage::catalog::ukey_part(&t.rows[r][c]).as_ref() == Some(k))
                        })
                    })
                }
            };
            if hit.is_some() {
                return hit;
            }
        }
        None
    }
}

enum Arbiter {
    Pk(usize),
    Unique(usize),
    Scan(Vec<usize>),
}
