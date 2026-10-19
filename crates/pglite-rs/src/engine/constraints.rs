//! Ràng buộc: đọc từ DDL, kiểm tra khi ghi (NOT NULL, UNIQUE, CHECK, FOREIGN KEY) và hành động khoá
//! ngoại (ON DELETE/UPDATE CASCADE, SET NULL, SET DEFAULT, RESTRICT, NO ACTION).
//!
//! Kiểm tra chạy SAU khi dòng đã được ghi vào bảng (và WAL): nếu vi phạm thì trả lỗi, và
//! `StorageEngine::end_statement` hoàn tác toàn bộ lệnh (bộ nhớ + WAL). Không bao giờ chạy lúc replay.
use super::executor::{find_top_level_keyword, split_comma_separated_tokens, Executor};
use crate::storage::catalog::{ConstraintDef, ConstraintKind, FkAction, IndexDef};
use crate::storage::engine::UndoAction;
use crate::storage::table::Table;
use crate::storage::wal::WalRecord;
use crate::types::{ColumnDef, Value};

fn unquote(s: &str) -> String {
    let s = s.trim();
    if s.len() >= 2 && s.starts_with('"') && s.ends_with('"') {
        s[1..s.len() - 1].to_string()
    } else {
        s.to_lowercase()
    }
}

/// Bỏ tiền tố schema: `public.users` -> `users`.
pub(crate) fn strip_schema(name: &str) -> String {
    let n = name.trim();
    let last = match n.rfind('.') {
        Some(i) if !n[i..].contains('"') || n[i + 1..].starts_with('"') => &n[i + 1..],
        _ => n,
    };
    unquote(last)
}

/// `(a, "B", c)` -> ["a", "B", "c"]. `s` bắt đầu bằng `(`.
fn parse_col_list(s: &str) -> Option<(Vec<String>, usize)> {
    let s_trim = s.trim_start();
    let offset = s.len() - s_trim.len();
    if !s_trim.starts_with('(') {
        return None;
    }
    let close = matching_paren(s_trim, 0)?;
    let cols = split_comma_separated_tokens(&s_trim[1..close]).into_iter().map(|c| unquote(c.split_whitespace().next().unwrap_or(c))).collect();
    Some((cols, offset + close + 1))
}

pub(crate) fn matching_paren(s: &str, open: usize) -> Option<usize> {
    let bytes = s.as_bytes();
    let mut depth = 0i32;
    let mut in_sq = false;
    let mut in_dq = false;
    for (i, &b) in bytes.iter().enumerate().skip(open) {
        match b {
            b'\'' if !in_dq => in_sq = !in_sq,
            b'"' if !in_sq => in_dq = !in_dq,
            b'(' if !in_sq && !in_dq => depth += 1,
            b')' if !in_sq && !in_dq => {
                depth -= 1;
                if depth == 0 {
                    return Some(i);
                }
            }
            _ => {}
        }
    }
    None
}

/// Đọc `REFERENCES t [(cols)] [MATCH x] [ON DELETE a] [ON UPDATE b] ...`. `s` là phần sau từ khoá.
fn parse_references(s: &str) -> (String, Vec<String>, FkAction, FkAction) {
    let s = s.trim();
    let name_end = s.find(|c: char| c == '(' || c.is_whitespace()).unwrap_or(s.len());
    let ref_table = strip_schema(&s[..name_end]);
    let mut rest = &s[name_end..];
    let mut ref_cols = Vec::new();
    if let Some((cols, used)) = parse_col_list(rest) {
        ref_cols = cols;
        rest = &rest[used..];
    }
    let words: Vec<String> = rest.split_whitespace().map(|w| w.trim_matches(|c| c == ',' || c == ';').to_uppercase()).collect();
    let mut on_delete = FkAction::NoAction;
    let mut on_update = FkAction::NoAction;
    let mut i = 0;
    while i < words.len() {
        if words[i] == "ON" && i + 1 < words.len() {
            let target = words[i + 1].clone();
            let (action, used) = match (words.get(i + 2).map(|s| s.as_str()), words.get(i + 3).map(|s| s.as_str())) {
                (Some("SET"), Some(w2)) | (Some("NO"), Some(w2)) => (FkAction::parse(&format!("{} {}", words[i + 2], w2)), 4),
                (Some(w), _) => (FkAction::parse(w), 3),
                _ => (None, 2),
            };
            if let Some(a) = action {
                if target == "DELETE" {
                    on_delete = a;
                } else if target == "UPDATE" {
                    on_update = a;
                }
            }
            i += used;
        } else {
            i += 1;
        }
    }
    (ref_table, ref_cols, on_delete, on_update)
}

/// Phần `CHECK (...)` bắt đầu tại `pos` (vị trí từ CHECK): trả về biểu thức bên trong.
fn parse_check_at(s: &str, pos: usize) -> Option<String> {
    let after = &s[pos + 5..];
    let open = after.find('(')?;
    if !after[..open].trim().is_empty() {
        return None;
    }
    let close = matching_paren(after, open)?;
    Some(after[open + 1..close].trim().to_string())
}

fn unique_name(base: String, taken: &[ConstraintDef]) -> String {
    if !taken.iter().any(|c| c.name.eq_ignore_ascii_case(&base)) {
        return base;
    }
    (1..).map(|n| format!("{base}{n}")).find(|n| !taken.iter().any(|c| c.name.eq_ignore_ascii_case(n))).unwrap()
}

/// `CONSTRAINT name ...` ở đầu: (Some(name), phần còn lại).
fn split_constraint_name(def: &str) -> (Option<String>, &str) {
    let t = def.trim_start();
    if t.len() > 11 && t.get(..10).map_or(false, |p| p.eq_ignore_ascii_case("CONSTRAINT")) && t.as_bytes()[10].is_ascii_whitespace() {
        let rest = t[10..].trim_start();
        let end = if rest.starts_with('"') {
            rest[1..].find('"').map(|i| i + 2).unwrap_or(rest.len())
        } else {
            rest.find(char::is_whitespace).unwrap_or(rest.len())
        };
        return (Some(unquote(&rest[..end])), rest[end..].trim_start());
    }
    (None, t)
}

fn starts_with_kw(s: &str, kw: &str) -> bool {
    s.len() >= kw.len() && s.get(..kw.len()).map_or(false, |p| p.eq_ignore_ascii_case(kw)) && s[kw.len()..].chars().next().map_or(true, |c| !c.is_alphanumeric() && c != '_')
}

/// Ràng buộc cấp bảng (một phần tử trong danh sách của CREATE TABLE, hoặc sau ALTER TABLE ADD).
/// `None` nếu `def` không phải ràng buộc cấp bảng (tức là định nghĩa cột).
pub(crate) fn parse_table_constraint(table: &str, def: &str, taken: &[ConstraintDef]) -> Option<ConstraintDef> {
    let (name, body) = split_constraint_name(def);
    let t = table.to_lowercase();
    if starts_with_kw(body, "PRIMARY KEY") {
        let (cols, _) = parse_col_list(&body[11..])?;
        return Some(ConstraintDef { name: name.unwrap_or_else(|| format!("{t}_pkey")), kind: ConstraintKind::PrimaryKey { cols } });
    }
    if starts_with_kw(body, "UNIQUE") {
        let mut rest = body[6..].trim_start();
        if starts_with_kw(rest, "NULLS") {
            // UNIQUE NULLS [NOT] DISTINCT (...): coi như UNIQUE thường
            rest = rest.find('(').map(|i| &rest[i..]).unwrap_or(rest);
        }
        let (cols, _) = parse_col_list(rest)?;
        let n = name.unwrap_or_else(|| unique_name(format!("{t}_{}_key", cols.join("_")), taken));
        return Some(ConstraintDef { name: n, kind: ConstraintKind::Unique { cols } });
    }
    if starts_with_kw(body, "FOREIGN KEY") {
        let (cols, used) = parse_col_list(&body[11..])?;
        let after = &body[11 + used..];
        let r = find_top_level_keyword(after, "REFERENCES")?;
        let (ref_table, mut ref_cols, on_delete, on_update) = parse_references(&after[r + 10..]);
        if ref_cols.is_empty() {
            ref_cols = cols.clone(); // sẽ được thay bằng khoá chính của bảng cha khi đăng ký
        }
        let n = name.unwrap_or_else(|| unique_name(format!("{t}_{}_fkey", cols.join("_")), taken));
        return Some(ConstraintDef { name: n, kind: ConstraintKind::ForeignKey { cols, ref_table, ref_cols, on_delete, on_update } });
    }
    if starts_with_kw(body, "CHECK") {
        let expr = parse_check_at(body, 0)?;
        let n = name.unwrap_or_else(|| unique_name(format!("{t}_check"), taken));
        return Some(ConstraintDef { name: n, kind: ConstraintKind::Check { expr } });
    }
    if starts_with_kw(body, "EXCLUDE") {
        return None;
    }
    None
}

/// Ràng buộc cấp cột trong định nghĩa `col type ...` (UNIQUE, REFERENCES, CHECK, PRIMARY KEY).
pub(crate) fn parse_column_constraints(table: &str, col: &str, rest: &str, taken: &mut Vec<ConstraintDef>) {
    let t = table.to_lowercase();
    let c = col.to_lowercase();
    let up = rest.to_uppercase();
    // Chỉ xét phần ngoài chuỗi/ngoặc: dùng find_top_level_keyword.
    if find_top_level_keyword(rest, "PRIMARY").is_some() && up.contains("PRIMARY KEY") {
        let def = ConstraintDef { name: format!("{t}_pkey"), kind: ConstraintKind::PrimaryKey { cols: vec![col.to_string()] } };
        taken.push(def);
    }
    if let Some(pos) = find_top_level_keyword(rest, "UNIQUE") {
        let _ = pos;
        let name = unique_name(format!("{t}_{c}_key"), taken);
        taken.push(ConstraintDef { name, kind: ConstraintKind::Unique { cols: vec![col.to_string()] } });
    }
    if let Some(pos) = find_top_level_keyword(rest, "REFERENCES") {
        let (ref_table, ref_cols, on_delete, on_update) = parse_references(&rest[pos + 10..]);
        let name = unique_name(format!("{t}_{c}_fkey"), taken);
        let ref_cols = if ref_cols.is_empty() { vec![String::new()] } else { ref_cols };
        taken.push(ConstraintDef { name, kind: ConstraintKind::ForeignKey { cols: vec![col.to_string()], ref_table, ref_cols, on_delete, on_update } });
    }
    let mut search_from = 0;
    while let Some(rel) = find_top_level_keyword(&rest[search_from..], "CHECK") {
        let pos = search_from + rel;
        if let Some(expr) = parse_check_at(rest, pos) {
            let name = unique_name(format!("{t}_{c}_check"), taken);
            taken.push(ConstraintDef { name, kind: ConstraintKind::Check { expr } });
        }
        search_from = pos + 5;
    }
}

/// Định nghĩa `CREATE [UNIQUE] INDEX` đã đọc.
pub(crate) struct ParsedIndex {
    pub name: String,
    pub table: String,
    pub def: IndexDef,
    pub if_not_exists: bool,
}

/// `CREATE [UNIQUE] INDEX [CONCURRENTLY] [IF NOT EXISTS] [name] ON [ONLY] t [USING m] (cols) [INCLUDE (...)] [WHERE p]`
pub(crate) fn parse_create_index(sql: &str) -> Result<ParsedIndex, String> {
    let mut rest = sql.trim().trim_end_matches(';').trim();
    rest = rest[6..].trim_start(); // CREATE
    let unique = starts_with_kw(rest, "UNIQUE");
    if unique {
        rest = rest[6..].trim_start();
    }
    if !starts_with_kw(rest, "INDEX") {
        return Err("Invalid CREATE INDEX syntax".to_string());
    }
    rest = rest[5..].trim_start();
    if starts_with_kw(rest, "CONCURRENTLY") {
        rest = rest[12..].trim_start();
    }
    let if_not_exists = starts_with_kw(rest, "IF NOT EXISTS");
    if if_not_exists {
        rest = rest[13..].trim_start();
    }
    let on_pos = find_top_level_keyword(rest, "ON").ok_or("Missing ON in CREATE INDEX")?;
    let name_part = rest[..on_pos].trim();
    let mut after_on = rest[on_pos + 2..].trim_start();
    if starts_with_kw(after_on, "ONLY") {
        after_on = after_on[4..].trim_start();
    }
    let open = after_on.find('(').ok_or("Missing column list in CREATE INDEX")?;
    let mut table_part = after_on[..open].trim();
    if let Some(u) = find_top_level_keyword(table_part, "USING") {
        table_part = table_part[..u].trim();
    }
    let table = strip_schema(table_part);
    let close = matching_paren(after_on, open).ok_or("Missing closing parenthesis in CREATE INDEX")?;
    let cols: Vec<String> = split_comma_separated_tokens(&after_on[open + 1..close])
        .into_iter()
        .map(|c| {
            // bỏ ASC/DESC/NULLS ..., opclass; biểu thức giữ nguyên văn
            let c = c.trim();
            let first = c.split_whitespace().next().unwrap_or(c);
            if first.contains('(') { c.to_string() } else { unquote(first) }
        })
        .collect();
    let tail = &after_on[close + 1..];
    let predicate = find_top_level_keyword(tail, "WHERE").map(|w| tail[w + 5..].trim().to_string()).filter(|p| !p.is_empty());
    let name = if name_part.is_empty() {
        format!("{}_{}_{}", table, cols.join("_").replace(|ch: char| !ch.is_alphanumeric() && ch != '_', ""), if unique { "key" } else { "idx" })
    } else {
        unquote(name_part)
    };
    Ok(ParsedIndex { name: name.clone(), table, def: IndexDef { name, cols, unique, predicate }, if_not_exists })
}

fn show_val(v: &Value) -> String {
    match v {
        Value::Text(t) => t.to_string(),
        Value::Int(i) => i.to_string(),
        Value::Float(f) => f.to_string(),
        Value::Bool(b) => b.to_string(),
        Value::Null => "null".to_string(),
    }
}

fn key_detail(table: &Table, cols: &[usize], row: &[Value]) -> String {
    let names: Vec<&str> = cols.iter().map(|&c| table.columns[c].name.as_str()).collect();
    let vals: Vec<String> = cols.iter().map(|&c| show_val(&row[c])).collect();
    format!("({})=({})", names.join(", "), vals.join(", "))
}

/// Hai giá trị khoá ngoại có bằng nhau không (so như khoá UNIQUE: '0123' khác 123).
fn fk_eq(a: &Value, b: &Value) -> bool {
    match (crate::storage::catalog::ukey_part(a), crate::storage::catalog::ukey_part(b)) {
        (Some(x), Some(y)) => {
            x == y || {
                // khoá ngoại INT trỏ khoá text dạng số (và ngược lại) vẫn khớp như trước
                use crate::storage::catalog::UKeyPart::*;
                match (&x, &y) {
                    (Int(i), Text(t)) | (Text(t), Int(i)) => t.trim().parse::<i64>().ok() == Some(*i),
                    _ => false,
                }
            }
        }
        _ => false,
    }
}

/// Khoá ngoại đã được giải tên cột thành chỉ số.
#[derive(Clone)]
struct ResolvedFk {
    name: String,
    child: String,
    child_cols: Vec<usize>,
    parent: String,
    parent_cols: Vec<usize>,
    on_delete: FkAction,
    on_update: FkAction,
}

impl Executor {
    /// Hoàn thiện danh sách ràng buộc trước khi lưu: khoá ngoại không ghi cột đích thì trỏ khoá chính
    /// của bảng cha (như PostgreSQL).
    pub(crate) fn finalize_constraints(&self, table: &str, columns: &[ColumnDef], mut defs: Vec<ConstraintDef>) -> Vec<ConstraintDef> {
        for d in &mut defs {
            if let ConstraintKind::ForeignKey { cols, ref_table, ref_cols, .. } = &mut d.kind {
                let needs_pk = ref_cols.iter().all(|c| c.is_empty()) || ref_cols.len() != cols.len();
                if needs_pk {
                    let parent_pk: Vec<String> = if ref_table.eq_ignore_ascii_case(table) {
                        columns.iter().filter(|c| c.is_primary_key).map(|c| c.name.clone()).collect()
                    } else {
                        self.storage
                            .get_table(ref_table)
                            .map(|p| match p.constraints.iter().find_map(|c| match &c.kind {
                                ConstraintKind::PrimaryKey { cols } => Some(cols.clone()),
                                _ => None,
                            }) {
                                Some(cols) => cols,
                                None => p.pk_col_idx.map(|i| vec![p.columns[i].name.clone()]).unwrap_or_default(),
                            })
                            .unwrap_or_default()
                    };
                    *ref_cols = if parent_pk.len() == cols.len() { parent_pk } else { vec!["id".to_string(); cols.len()] };
                }
            }
        }
        defs
    }

    /// Ràng buộc của mọi bảng cho information_schema / pg_catalog: một dòng mỗi cột của ràng buộc.
    pub(crate) fn constraint_rows(&self) -> Vec<super::executor::TableConstraintDef> {
        let mut names: Vec<&String> = self.storage.tables.keys().collect();
        names.sort();
        let mut out = Vec::new();
        for key in names {
            let t = &self.storage.tables[key];
            for c in &t.constraints {
                let (cols, foreign): (&[String], Option<(&String, &Vec<String>)>) = match &c.kind {
                    ConstraintKind::PrimaryKey { cols } | ConstraintKind::Unique { cols } => (cols, None),
                    ConstraintKind::ForeignKey { cols, ref_table, ref_cols, .. } => (cols, Some((ref_table, ref_cols))),
                    ConstraintKind::Check { .. } => continue,
                };
                for (i, col) in cols.iter().enumerate() {
                    let col_name = t.get_column_index(col).map(|ci| t.columns[ci].name.clone()).unwrap_or_else(|| col.clone());
                    out.push(super::executor::TableConstraintDef {
                        table_name: t.name.clone(),
                        constraint_name: c.name.clone(),
                        constraint_type: c.type_name().to_string(),
                        column_name: col_name,
                        ordinal_position: (i + 1) as i64,
                        foreign_table_name: foreign.map(|(rt, _)| rt.clone()),
                        foreign_column_name: foreign.and_then(|(_, rc)| rc.get(i).cloned()),
                    });
                }
            }
        }
        out
    }

    /// Mọi khoá ngoại trỏ TỚI bảng `parent`.
    fn referencing_fks(&self, parent: &str) -> Vec<ResolvedFk> {
        let mut out = Vec::new();
        let Some(p) = self.storage.get_table(parent) else {
            return out;
        };
        for (child_key, child) in &self.storage.tables {
            for c in &child.constraints {
                if let ConstraintKind::ForeignKey { cols, ref_table, ref_cols, on_delete, on_update } = &c.kind {
                    if !ref_table.eq_ignore_ascii_case(parent) {
                        continue;
                    }
                    let child_cols: Option<Vec<usize>> = cols.iter().map(|n| child.get_column_index(n)).collect();
                    let parent_cols: Option<Vec<usize>> = ref_cols.iter().map(|n| p.get_column_index(n)).collect();
                    if let (Some(child_cols), Some(parent_cols)) = (child_cols, parent_cols) {
                        out.push(ResolvedFk {
                            name: c.name.clone(),
                            child: child_key.clone(),
                            child_cols,
                            parent: parent.to_lowercase(),
                            parent_cols,
                            on_delete: *on_delete,
                            on_update: *on_update,
                        });
                    }
                }
            }
        }
        out.sort_by(|a, b| a.child.cmp(&b.child).then(a.name.cmp(&b.name)));
        out
    }

    /// Bảng có bị khoá ngoại nào trỏ tới không (đường nhanh cho DELETE/UPDATE).
    pub(crate) fn is_referenced(&self, parent: &str) -> bool {
        self.storage.tables.values().any(|t| {
            t.constraints.iter().any(|c| matches!(&c.kind, ConstraintKind::ForeignKey { ref_table, .. } if ref_table.eq_ignore_ascii_case(parent)))
        })
    }

    /// Có dòng sống nào trong `table` mang đúng `vals` ở các cột `cols` không (khác `except`).
    fn find_rows_with(&self, table: &str, cols: &[usize], vals: &[Value], except: Option<usize>, limit: usize) -> Vec<usize> {
        let Some(t) = self.storage.get_table(table) else {
            return Vec::new();
        };
        let matches = |r: usize| r < t.rows.len() && Some(r) != except && !t.is_deleted[r] && cols.iter().zip(vals).all(|(&c, v)| fk_eq(&t.rows[r][c], v));
        // 1. Khoá chính một cột
        if cols.len() == 1 && t.pk_col_idx == Some(cols[0]) && !t.has_dup_pk {
            if let Some(r) = t.live_row_with_pk(&vals[0], except) {
                if matches(r) {
                    return vec![r];
                }
            }
            // khoá chính số được lưu dạng text hay ngược lại: rơi xuống quét
        }
        // 2. Index UNIQUE đúng các cột này
        for (ui, u) in t.uniq.iter().enumerate() {
            if u.predicate.is_none() && u.cols.len() == cols.len() && u.cols.iter().all(|c| cols.contains(c)) {
                let mut probe = vec![Value::Null; t.columns.len()];
                for (&c, v) in cols.iter().zip(vals) {
                    probe[c] = v.clone();
                }
                if let Some(r) = t.unique_conflict(ui, &probe, except) {
                    if matches(r) {
                        return vec![r];
                    }
                }
                break;
            }
        }
        // 3. Index phụ một cột
        if cols.len() == 1 {
            if let Some(map) = t.col_indexes.get(&cols[0]) {
                let key = crate::types::value_to_join_key(&vals[0]);
                return map.get(&key).map(|l| l.iter().copied().filter(|&r| matches(r)).take(limit).collect()).unwrap_or_default();
            }
        }
        // 4. Quét
        (0..t.rows.len()).filter(|&r| matches(r)).take(limit).collect()
    }

    /// Kiểm tra các dòng vừa ghi của `table`. `changed`: cột đã đổi (UPDATE); `None` = dòng mới (INSERT).
    pub(crate) fn verify_rows(&self, table: &str, rows: &[usize], changed: Option<&[usize]>) -> Result<(), String> {
        let Some(t) = self.storage.get_table(table) else {
            return Ok(());
        };
        // Kiểu dữ liệu: chuỗi không đọc được thành số/bool/uuid/ngày giờ thì lỗi như PostgreSQL (trước
        // đây 'abc' được lưu thẳng vào cột INT).
        for &r in rows {
            if r >= t.rows.len() || t.is_deleted[r] {
                continue;
            }
            for (c, col) in t.columns.iter().enumerate() {
                if changed.map_or(false, |ch| !ch.contains(&c)) {
                    continue;
                }
                if let Some(e) = type_error(col, &t.rows[r][c]) {
                    return Err(e);
                }
            }
        }
        let not_null: Vec<usize> = t
            .columns
            .iter()
            .enumerate()
            .filter(|(i, c)| !c.is_nullable && changed.map_or(true, |ch| ch.contains(i)))
            .map(|(i, _)| i)
            .collect();
        if not_null.is_empty() && t.uniq.is_empty() && t.constraints.is_empty() {
            return Ok(());
        }
        let touches = |cols: &[usize]| changed.map_or(true, |ch| cols.iter().any(|c| ch.contains(c)));
        let checks: Vec<(String, crate::engine::expr::CompiledExpr)> = t
            .constraints
            .iter()
            .filter_map(|c| match &c.kind {
                ConstraintKind::Check { expr } => Some((c.name.clone(), crate::engine::expr::compile_expr(expr, Some(t), None, None, None))),
                _ => None,
            })
            .collect();
        let fks: Vec<(String, Vec<usize>, String, Vec<usize>)> = t
            .constraints
            .iter()
            .filter_map(|c| match &c.kind {
                ConstraintKind::ForeignKey { cols, ref_table, ref_cols, .. } => {
                    let child: Vec<usize> = cols.iter().map(|n| t.get_column_index(n)).collect::<Option<_>>()?;
                    let parent = self.storage.get_table(ref_table)?;
                    let pcols: Vec<usize> = ref_cols.iter().map(|n| parent.get_column_index(n)).collect::<Option<_>>()?;
                    touches(&child).then(|| (c.name.clone(), child, ref_table.to_lowercase(), pcols))
                }
                _ => None,
            })
            .collect();
        for &r in rows {
            if r >= t.rows.len() || t.is_deleted[r] {
                continue;
            }
            let row = &t.rows[r];
            for &c in &not_null {
                if row[c].is_null() {
                    return Err(format!(
                        "null value in column \"{}\" of relation \"{}\" violates not-null constraint",
                        t.columns[c].name,
                        t.name.to_lowercase()
                    ));
                }
            }
            for (ui, u) in t.uniq.iter().enumerate() {
                if !touches(&u.cols) && u.predicate.is_none() {
                    continue;
                }
                if let Some(k) = u.key(row) {
                    if u.map.get(&k).map_or(false, |l| l.iter().any(|&o| o != r)) {
                        let _ = ui;
                        return Err(format!(
                            "duplicate key value violates unique constraint \"{}\" (Key {} already exists.)",
                            u.name,
                            key_detail(t, &u.cols, row)
                        ));
                    }
                }
            }
            for (name, expr) in &checks {
                if expr.eval_bool(row, None, &[]) == Some(false) {
                    return Err(format!("new row for relation \"{}\" violates check constraint \"{}\"", t.name.to_lowercase(), name));
                }
            }
            for (name, child_cols, parent, parent_cols) in &fks {
                let vals: Vec<Value> = child_cols.iter().map(|&c| row[c].clone()).collect();
                if vals.iter().any(|v| v.is_null()) {
                    continue; // MATCH SIMPLE: có NULL thì không kiểm tra
                }
                if self.find_rows_with(parent, parent_cols, &vals, None, 1).is_empty() {
                    return Err(format!(
                        "insert or update on table \"{}\" violates foreign key constraint \"{}\" (Key {} is not present in table \"{}\".)",
                        t.name.to_lowercase(),
                        name,
                        key_detail(t, child_cols, row),
                        parent
                    ));
                }
            }
        }
        Ok(())
    }

    /// Xoá một dòng, ghi WAL và undo như DELETE thường.
    pub(crate) fn delete_row_logged(&mut self, table: &str, row_idx: usize) -> bool {
        let clean = table.to_lowercase();
        let identity = {
            let Some(t) = self.storage.get_table_mut(&clean) else {
                return false;
            };
            if row_idx >= t.rows.len() || t.is_deleted[row_idx] {
                return false;
            }
            let id = super::executor::RowIdentity::of(t, row_idx);
            t.delete_row(row_idx);
            id
        };
        self.storage.wal.append(identity.delete_record(clean.clone()));
        if self.storage.undo_enabled() {
            self.storage.tx_undo_log.push(UndoAction::UndeleteRow { table: clean, row_idx });
        }
        true
    }

    /// Sửa các ô của một dòng, ghi WAL và undo như UPDATE thường (đổi khoá chính = DELETE + INSERT).
    pub(crate) fn update_row_logged(&mut self, table: &str, row_idx: usize, updates: Vec<(usize, Value)>) {
        let clean = table.to_lowercase();
        let undo = self.storage.undo_enabled();
        let (identity, moves_key, old_vals, new_row) = {
            let Some(t) = self.storage.get_table_mut(&clean) else {
                return;
            };
            if row_idx >= t.rows.len() || t.is_deleted[row_idx] {
                return;
            }
            let identity = super::executor::RowIdentity::of(t, row_idx);
            let moves_key = updates.iter().any(|(c, v)| Some(*c) == t.pk_col_idx && !fk_eq(v, &t.rows[row_idx][*c]));
            let old_vals: Vec<(usize, Value)> = updates.iter().map(|(c, _)| (*c, t.rows[row_idx][*c].clone())).collect();
            t.update_row_multi(row_idx, &updates);
            (identity, moves_key, old_vals, t.rows[row_idx].clone())
        };
        if moves_key {
            self.storage.wal.append(identity.delete_record(clean.clone()));
            self.storage.wal.append(WalRecord::Insert { table: clean.clone(), row: new_row });
        } else {
            for rec in identity.update_records(clean.clone(), updates) {
                self.storage.wal.append(rec);
            }
        }
        if undo {
            for (col_idx, old_val) in old_vals.into_iter().rev() {
                self.storage.tx_undo_log.push(UndoAction::RestoreRow { table: clean.clone(), row_idx, col_idx, old_val });
            }
        }
    }

    /// Hành động khoá ngoại sau khi các dòng `rows` của `parent` đã bị xoá (giá trị vẫn còn trong
    /// `rows`, chỉ bị đánh dấu xoá).
    pub(crate) fn fk_on_parent_delete(&mut self, parent: &str, rows: &[usize]) -> Result<(), String> {
        self.fk_on_parent_delete_depth(parent, rows, 0)
    }

    fn fk_on_parent_delete_depth(&mut self, parent: &str, rows: &[usize], depth: usize) -> Result<(), String> {
        if rows.is_empty() {
            return Ok(());
        }
        if depth > 64 {
            return Err("foreign key cascade is nested too deeply".to_string());
        }
        let fks = self.referencing_fks(parent);
        if fks.is_empty() {
            return Ok(());
        }
        let mut deferred: Vec<(ResolvedFk, Vec<Value>)> = Vec::new();
        for fk in fks {
            let keys: Vec<Vec<Value>> = {
                let p = self.storage.get_table(parent).unwrap();
                rows.iter().map(|&r| fk.parent_cols.iter().map(|&c| p.rows[r][c].clone()).collect::<Vec<Value>>()).filter(|k| k.iter().all(|v| !v.is_null())).collect()
            };
            for key in keys {
                // Bảng cha vẫn còn dòng khác mang khoá này (dữ liệu cũ trùng khoá): không có gì bị mồ côi.
                if !self.find_rows_with(parent, &fk.parent_cols, &key, None, 1).is_empty() {
                    continue;
                }
                let children = self.find_rows_with(&fk.child, &fk.child_cols, &key, None, usize::MAX);
                if children.is_empty() {
                    continue;
                }
                match fk.on_delete {
                    FkAction::Cascade => {
                        let mut deleted = Vec::new();
                        for r in children {
                            if self.delete_row_logged(&fk.child, r) {
                                deleted.push(r);
                            }
                        }
                        let child = fk.child.clone();
                        self.fk_on_parent_delete_depth(&child, &deleted, depth + 1)?;
                    }
                    FkAction::SetNull | FkAction::SetDefault => {
                        self.fk_set_children(&fk, &children, fk.on_delete)?;
                    }
                    FkAction::Restrict => return Err(still_referenced(&fk, &key, self)),
                    FkAction::NoAction => deferred.push((fk.clone(), key)),
                }
            }
            if !deferred.is_empty() {
                // NO ACTION: chỉ lỗi nếu sau mọi cascade vẫn còn dòng con trỏ tới khoá đã mất.
                let pending = std::mem::take(&mut deferred);
                for (fk, key) in pending {
                    if !self.find_rows_with(&fk.child, &fk.child_cols, &key, None, 1).is_empty() {
                        return Err(still_referenced(&fk, &key, self));
                    }
                }
            }
        }
        Ok(())
    }

    fn fk_set_children(&mut self, fk: &ResolvedFk, children: &[usize], action: FkAction) -> Result<(), String> {
        let defaults: Vec<Value> = {
            let t = self.storage.get_table(&fk.child).unwrap();
            fk.child_cols
                .iter()
                .map(|&c| match (action, &t.columns[c].default_value) {
                    (FkAction::SetDefault, Some(d)) => super::executor::eval_default_value(d),
                    _ => Value::Null,
                })
                .collect()
        };
        for &r in children {
            let updates: Vec<(usize, Value)> = fk.child_cols.iter().copied().zip(defaults.iter().cloned()).collect();
            self.update_row_logged(&fk.child, r, updates);
        }
        let child = fk.child.clone();
        let cols = fk.child_cols.clone();
        self.verify_rows(&child, children, Some(&cols))
    }

    /// Hành động khoá ngoại khi UPDATE đổi khoá được tham chiếu. `old_rows`: (dòng, giá trị cũ).
    pub(crate) fn fk_on_parent_update(&mut self, parent: &str, old_rows: &[(usize, Vec<Value>)]) -> Result<(), String> {
        let fks = self.referencing_fks(parent);
        for fk in fks {
            for (r, old) in old_rows {
                let old_key: Vec<Value> = fk.parent_cols.iter().map(|&c| old[c].clone()).collect();
                let new_key: Vec<Value> = {
                    let p = self.storage.get_table(parent).unwrap();
                    fk.parent_cols.iter().map(|&c| p.rows[*r][c].clone()).collect()
                };
                if old_key.iter().any(|v| v.is_null()) || old_key.iter().zip(&new_key).all(|(a, b)| fk_eq(a, b)) {
                    continue;
                }
                if !self.find_rows_with(parent, &fk.parent_cols, &old_key, None, 1).is_empty() {
                    continue;
                }
                let children = self.find_rows_with(&fk.child, &fk.child_cols, &old_key, None, usize::MAX);
                if children.is_empty() {
                    continue;
                }
                match fk.on_update {
                    FkAction::Cascade => {
                        for &c in &children {
                            let updates: Vec<(usize, Value)> = fk.child_cols.iter().copied().zip(new_key.iter().cloned()).collect();
                            self.update_row_logged(&fk.child, c, updates);
                        }
                    }
                    FkAction::SetNull | FkAction::SetDefault => self.fk_set_children(&fk, &children, fk.on_update)?,
                    FkAction::Restrict | FkAction::NoAction => return Err(still_referenced(&fk, &old_key, self)),
                }
            }
        }
        Ok(())
    }

    /// Dữ liệu hiện có có thoả ràng buộc `def` của bảng `table` không (trước khi ADD CONSTRAINT,
    /// CREATE UNIQUE INDEX, hay khi nhận lại ràng buộc cho bảng cũ). `Err` mô tả vi phạm đầu tiên.
    pub(crate) fn validate_existing(&self, table: &str, def: &ConstraintDef) -> Result<(), String> {
        let t = self.storage.get_table(table).ok_or_else(|| format!("relation \"{}\" does not exist", table))?;
        let live: Vec<usize> = (0..t.rows.len()).filter(|&r| !t.is_deleted[r]).collect();
        match &def.kind {
            ConstraintKind::PrimaryKey { cols } | ConstraintKind::Unique { cols } => {
                let idx: Vec<usize> = cols
                    .iter()
                    .map(|c| t.get_column_index(c).ok_or_else(|| format!("column \"{}\" named in key does not exist", c)))
                    .collect::<Result<_, _>>()?;
                let mut seen = std::collections::HashSet::new();
                for &r in &live {
                    let row = &t.rows[r];
                    if matches!(def.kind, ConstraintKind::PrimaryKey { .. }) {
                        if let Some(&c) = idx.iter().find(|&&c| row[c].is_null()) {
                            return Err(format!("column \"{}\" of relation \"{}\" contains null values", t.columns[c].name, t.name.to_lowercase()));
                        }
                    }
                    let key: Option<Vec<_>> = idx.iter().map(|&c| crate::storage::catalog::ukey_part(&row[c])).collect();
                    if let Some(k) = key {
                        if !seen.insert(k) {
                            return Err(format!(
                                "could not create unique index \"{}\" (Key {} is duplicated.)",
                                def.name,
                                key_detail(t, &idx, row)
                            ));
                        }
                    }
                }
            }
            ConstraintKind::Check { expr } => {
                let compiled = crate::engine::expr::compile_expr(expr, Some(t), None, None, None);
                if live.iter().any(|&r| compiled.eval_bool(&t.rows[r], None, &[]) == Some(false)) {
                    return Err(format!("check constraint \"{}\" of relation \"{}\" is violated by some row", def.name, t.name.to_lowercase()));
                }
            }
            ConstraintKind::ForeignKey { cols, ref_table, ref_cols, .. } => {
                let child: Vec<usize> = cols
                    .iter()
                    .map(|c| t.get_column_index(c).ok_or_else(|| format!("column \"{}\" referenced in foreign key constraint does not exist", c)))
                    .collect::<Result<_, _>>()?;
                let parent = self.storage.get_table(ref_table).ok_or_else(|| format!("relation \"{}\" does not exist", ref_table))?;
                let pcols: Vec<usize> = ref_cols
                    .iter()
                    .map(|c| parent.get_column_index(c).ok_or_else(|| format!("column \"{}\" referenced in foreign key constraint does not exist", c)))
                    .collect::<Result<_, _>>()?;
                for &r in &live {
                    let vals: Vec<Value> = child.iter().map(|&c| t.rows[r][c].clone()).collect();
                    if vals.iter().any(|v| v.is_null()) {
                        continue;
                    }
                    if self.find_rows_with(ref_table, &pcols, &vals, None, 1).is_empty() {
                        return Err(format!(
                            "insert or update on table \"{}\" violates foreign key constraint \"{}\" (Key {} is not present in table \"{}\".)",
                            t.name.to_lowercase(),
                            def.name,
                            key_detail(t, &child, &t.rows[r]),
                            ref_table.to_lowercase()
                        ));
                    }
                }
            }
        }
        Ok(())
    }

    /// Bảng do bản cũ tạo (catalog không được lưu) gặp lại `CREATE TABLE IF NOT EXISTS`: nhận lại các
    /// ràng buộc trong câu lệnh mà dữ liệu hiện có đã thoả. Ràng buộc bị dữ liệu cũ vi phạm thì bỏ qua
    /// và cảnh báo, không làm lỗi lệnh (app vẫn khởi động được như trước).
    pub(crate) fn adopt_legacy_constraints(&mut self, table: &str, defs: Vec<ConstraintDef>) {
        let tracked = self.storage.get_table(table).map_or(true, |t| t.catalog_tracked);
        if tracked {
            return;
        }
        let mut adopted: Vec<ConstraintDef> = Vec::new();
        for d in defs {
            // PRIMARY KEY một cột: bảng cũ đã có pk_index (ép hay không theo cờ đã lưu); chỉ ghi catalog.
            let single_pk = matches!(&d.kind, ConstraintKind::PrimaryKey { cols } if cols.len() == 1);
            let ok = if single_pk {
                Ok(())
            } else {
                // Kiểm tra trên catalog tạm (FK tự trỏ cần ràng buộc của chính bảng)
                self.validate_existing(table, &d)
            };
            match ok {
                Ok(()) => adopted.push(d),
                Err(e) => eprintln!(
                    "[pglite-rs] WARNING: existing data in \"{}\" violates constraint \"{}\"; it is not enforced: {}",
                    table, d.name, e
                ),
            }
        }
        let indexes = self.storage.get_table(table).map(|t| t.indexes.clone()).unwrap_or_default();
        self.storage.log_schema_op(crate::storage::catalog::SchemaOp::TableCatalog { table: table.to_string(), constraints: adopted, indexes, legacy: true });
    }

    /// Dữ liệu hiện có có trùng khoá trên một `CREATE UNIQUE INDEX` (có thể một phần) không.
    pub(crate) fn validate_unique_index(&self, table: &str, def: &IndexDef) -> Result<(), String> {
        let t = self.storage.get_table(table).ok_or_else(|| format!("relation \"{}\" does not exist", table))?;
        let Some(cols) = def.cols.iter().map(|c| t.get_column_index(c)).collect::<Option<Vec<usize>>>() else {
            return Ok(()); // index trên biểu thức: chỉ lưu, không ép
        };
        let predicate = def.predicate.as_deref().map(|p| crate::engine::expr::compile_expr(p, Some(t), None, None, None));
        let probe = crate::storage::catalog::UniqueIdx { name: def.name.clone(), cols: cols.clone(), predicate, map: Default::default() };
        let mut seen = std::collections::HashSet::new();
        for r in 0..t.rows.len() {
            if t.is_deleted[r] {
                continue;
            }
            if let Some(k) = probe.key(&t.rows[r]) {
                if !seen.insert(k) {
                    return Err(format!("could not create unique index \"{}\" (Key {} is duplicated.)", def.name, key_detail(t, &cols, &t.rows[r])));
                }
            }
        }
        Ok(())
    }

    /// Kiểm tra NOT NULL trên dữ liệu hiện có (ALTER COLUMN SET NOT NULL).
    pub(crate) fn validate_not_null(&self, table: &str, col: usize) -> Result<(), String> {
        let t = self.storage.get_table(table).ok_or_else(|| format!("relation \"{}\" does not exist", table))?;
        if (0..t.rows.len()).any(|r| !t.is_deleted[r] && t.rows[r][col].is_null()) {
            return Err(format!("column \"{}\" of relation \"{}\" contains null values", t.columns[col].name, t.name.to_lowercase()));
        }
        Ok(())
    }
}

/// Lỗi kiểu khi giá trị dạng chuỗi không hợp lệ với kiểu cột.
fn type_error(col: &ColumnDef, v: &Value) -> Option<String> {
    use crate::types::DataType;
    if let (DataType::Integer, Value::Int(i)) = (&col.data_type, v) {
        if *i < i32::MIN as i64 || *i > i32::MAX as i64 {
            return Some("integer out of range".to_string());
        }
    }
    let Value::Text(t) = v else { return None };
    let s = t.trim();
    let bad = |ty: &str| Some(format!("invalid input syntax for type {}: \"{}\"", ty, t));
    match col.data_type {
        DataType::Integer | DataType::Serial | DataType::BigInt => {
            if s.parse::<i64>().is_err() && !s.parse::<f64>().map_or(false, |f| f.fract() == 0.0) {
                return bad(if col.data_type == DataType::BigInt { "bigint" } else { "integer" });
            }
        }
        DataType::Numeric => {
            if s.parse::<f64>().is_err() && !matches!(s.to_lowercase().as_str(), "nan" | "infinity" | "-infinity") {
                return bad("numeric");
            }
        }
        DataType::Boolean => {
            if !matches!(s.to_lowercase().as_str(), "t" | "true" | "f" | "false" | "1" | "0" | "y" | "yes" | "n" | "no" | "on" | "off") {
                return bad("boolean");
            }
        }
        DataType::Uuid => {
            let hex: String = s.trim_matches(|c| c == '{' || c == '}').chars().filter(|c| *c != '-').collect();
            if hex.len() != 32 || !hex.chars().all(|c| c.is_ascii_hexdigit()) {
                return bad("uuid");
            }
        }
        DataType::Date | DataType::Timestamp => {
            let special = matches!(s.to_lowercase().as_str(), "infinity" | "-infinity" | "now" | "today" | "epoch" | "tomorrow" | "yesterday");
            if !special && super::executor::parse_datetime_components(s).is_none() {
                return bad(if col.data_type == DataType::Date { "date" } else { "timestamp" });
            }
        }
        _ => {}
    }
    None
}

fn still_referenced(fk: &ResolvedFk, key: &[Value], ex: &Executor) -> String {
    let detail = ex
        .storage
        .get_table(&fk.parent)
        .map(|p| {
            let names: Vec<&str> = fk.parent_cols.iter().map(|&c| p.columns[c].name.as_str()).collect();
            let vals: Vec<String> = key.iter().map(show_val).collect();
            format!("({})=({})", names.join(", "), vals.join(", "))
        })
        .unwrap_or_default();
    format!(
        "update or delete on table \"{}\" violates foreign key constraint \"{}\" on table \"{}\" (Key {} is still referenced from table \"{}\".)",
        fk.parent, fk.name, fk.child, detail, fk.child
    )
}
