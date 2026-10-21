//! Kiểm tra tên cột trước khi chạy SELECT (như PostgreSQL báo `column "x" does not exist`, 42703),
//! thay vì để bộ đánh giá coi định danh lạ là chuỗi. Cố ý thận trọng để không báo nhầm:
//! - chỉ câu SELECT cấp cao nhất, FROM đúng một bảng thật (không JOIN, subquery, CTE, UNION...);
//! - chỉ xét định danh ở mức ngoặc 0 của danh sách SELECT (mục là một định danh), WHERE và ORDER BY;
//!   bên trong lời gọi hàm (EXTRACT(year FROM ..), TRIM(BOTH ..)...) không xét.
use crate::storage::engine::StorageEngine;

const KEYWORDS: &[&str] = &[
    "AND", "OR", "NOT", "IS", "NULL", "TRUE", "FALSE", "IN", "LIKE", "ILIKE", "BETWEEN", "SIMILAR", "TO", "ESCAPE", "DISTINCT", "FROM",
    "UNKNOWN", "ISNULL", "NOTNULL", "CASE", "WHEN", "THEN", "ELSE", "END", "ANY", "ALL", "SOME", "EXISTS", "COLLATE", "AT", "TIME", "ZONE",
    "INTERVAL", "DATE", "TIMESTAMP", "TIMESTAMPTZ", "CURRENT_DATE", "CURRENT_TIME", "CURRENT_TIMESTAMP", "LOCALTIME", "LOCALTIMESTAMP",
    "CURRENT_USER", "SESSION_USER", "USER", "CURRENT_SCHEMA", "CURRENT_CATALOG", "CURRENT_ROLE", "ARRAY", "ROW", "SYMMETRIC", "ASYMMETRIC",
    "OVERLAPS", "DEFAULT", "WITH", "WITHOUT", "ASC", "DESC", "NULLS", "FIRST", "LAST", "USING", "AS", "OPERATOR", "VARIADIC", "SELECT",
    "VALUES", "ON", "BY", "UNIQUE", "OF", "NORMALIZED", "NFC", "NFD", "NFKC", "NFKD", "JSON", "SCALAR", "OBJECT", "KEYS", "DOCUMENT",
];

fn is_ident_char(c: char) -> bool {
    c.is_alphanumeric() || c == '_' || c == '$'
}

/// Định danh ở mức ngoặc 0 của `clause`: (vị trí, định danh có thể có tiền tố `a.`), bỏ qua chuỗi,
/// số, tham số, tên hàm (theo sau là `(`), kiểu sau `::`, từ khoá và tên kiểu trước literal.
fn top_level_idents(clause: &str) -> Vec<String> {
    let chars: Vec<char> = clause.chars().collect();
    let n = chars.len();
    let mut out = Vec::new();
    let mut depth = 0i32;
    let mut i = 0;
    while i < n {
        let c = chars[i];
        match c {
            '\'' => {
                i += 1;
                while i < n {
                    if chars[i] == '\'' {
                        if i + 1 < n && chars[i + 1] == '\'' {
                            i += 2;
                            continue;
                        }
                        break;
                    }
                    i += 1;
                }
                i += 1;
            }
            '(' | '[' => {
                depth += 1;
                i += 1;
            }
            ')' | ']' => {
                depth -= 1;
                i += 1;
            }
            '$' => {
                i += 1;
                while i < n && is_ident_char(chars[i]) {
                    i += 1;
                }
            }
            _ if c.is_ascii_digit() => {
                while i < n && (is_ident_char(chars[i]) || chars[i] == '.') {
                    i += 1;
                }
            }
            _ if c == '"' || c.is_alphabetic() || c == '_' => {
                let start = i;
                // định danh, có thể "a"."b" / a.b
                loop {
                    if i < n && chars[i] == '"' {
                        i += 1;
                        while i < n && chars[i] != '"' {
                            i += 1;
                        }
                        i += 1;
                    } else {
                        while i < n && is_ident_char(chars[i]) {
                            i += 1;
                        }
                    }
                    if i + 1 < n && chars[i] == '.' && (chars[i + 1] == '"' || chars[i + 1].is_alphabetic() || chars[i + 1] == '_') {
                        i += 1;
                        continue;
                    }
                    break;
                }
                let word: String = chars[start..i.min(n)].iter().collect();
                let mut k = i;
                while k < n && chars[k].is_whitespace() {
                    k += 1;
                }
                let next = chars.get(k).copied();
                let prev_cast = clause[..chars[..start].iter().map(|c| c.len_utf8()).sum::<usize>()].trim_end().ends_with("::");
                let star = i < n && chars[i] == '.'; // t.*
                // `(x).trường`: truy cập trường của composite, không phải tên cột
                let field_access = start >= 2 && chars[start - 1] == '.' && chars[start - 2] == ')';
                if depth == 0 && !prev_cast && !star && !field_access && next != Some('(') && next != Some('\'') && !KEYWORDS.contains(&word.to_uppercase().as_str()) {
                    out.push(word);
                }
            }
            _ => i += 1,
        }
    }
    out
}

fn unquote(s: &str) -> String {
    s.trim_matches('"').to_string()
}

/// Bỏ alias khỏi một mục SELECT: `expr AS a` / `expr a`.
fn strip_alias(item: &str) -> (&str, Option<String>) {
    let t = item.trim();
    if let Some(p) = crate::engine::executor::find_top_level_keyword(t, "AS") {
        return (t[..p].trim(), Some(unquote(t[p + 2..].trim())));
    }
    (t, None)
}

/// Kết quả kiểm tra hợp lệ, để cache theo câu SQL.
pub(crate) enum Checked {
    /// Câu lệnh không thuộc diện kiểm tra (chỉ phụ thuộc vào text): luôn bỏ qua.
    NotApplicable,
    /// Đã kiểm tra trên bảng này: còn đúng khi danh sách cột của bảng không đổi.
    Table(String),
    /// Bảng chưa tồn tại: không cache.
    NoTable,
}

/// Dấu vân tay danh sách cột của bảng (đổi khi thêm/xoá/đổi tên cột).
pub(crate) fn columns_fingerprint(storage: &StorageEngine, table: &str) -> Option<u64> {
    use std::hash::{Hash, Hasher};
    let t = storage.get_table(table)?;
    let mut h = std::collections::hash_map::DefaultHasher::new();
    for c in &t.columns {
        c.name.hash(&mut h);
    }
    Some(h.finish())
}

/// `Err(column "x" does not exist)` nếu câu SELECT đơn giản dùng cột không có trong bảng.
pub(crate) fn check_select_columns(sql: &str, storage: &StorageEngine) -> Result<Checked, String> {
    use crate::engine::executor::find_top_level_keyword;
    let s = sql.trim().trim_end_matches(';').trim();
    let upper = s.to_uppercase();
    if !upper.starts_with("SELECT ") || upper.matches("SELECT").count() != 1 {
        return Ok(Checked::NotApplicable);
    }
    for kw in [" JOIN ", " UNION ", " INTERSECT ", " EXCEPT ", " LATERAL ", " OVER", " INTO ", " GROUP BY", " HAVING ", " WINDOW ", " FETCH ", "TABLESAMPLE", " ONLY "] {
        if upper.contains(kw) {
            return Ok(Checked::NotApplicable);
        }
    }
    let Some(from_pos) = find_top_level_keyword(s, "FROM") else { return Ok(Checked::NotApplicable) };
    let list = &s[6..from_pos];
    let rest = &s[from_pos + 4..];
    let clause_kw = ["WHERE", "ORDER", "LIMIT", "OFFSET", "FOR"];
    let mut cuts: Vec<(usize, &str)> = clause_kw.iter().filter_map(|k| find_top_level_keyword(rest, k).map(|p| (p, *k))).collect();
    cuts.sort();
    let from_end = cuts.first().map(|c| c.0).unwrap_or(rest.len());
    let from = rest[..from_end].trim();
    if from.contains(',') || from.contains('(') {
        return Ok(Checked::NotApplicable);
    }
    let mut parts = from.split_whitespace();
    let Some(tname) = parts.next() else { return Ok(Checked::NotApplicable) };
    let alias = match (parts.next(), parts.next()) {
        (None, _) => None,
        (Some(a), None) if !a.eq_ignore_ascii_case("AS") => Some(unquote(a)),
        (Some(a), Some(b)) if a.eq_ignore_ascii_case("AS") && parts.next().is_none() => Some(unquote(b)),
        _ => return Ok(Checked::NotApplicable),
    };
    let tname_clean = unquote(tname);
    let short = tname_clean.rsplit('.').next().unwrap_or(&tname_clean).to_string();
    if tname_clean.contains('.') && !tname_clean.to_lowercase().starts_with("public.") {
        return Ok(Checked::NotApplicable); // schema khác (information_schema, pg_catalog...)
    }
    let Some(table) = storage.get_table(&short) else { return Ok(Checked::NoTable) };
    let clause = |k: &str| -> Option<&str> {
        let idx = cuts.iter().position(|c| c.1 == k)?;
        let start = cuts[idx].0 + k.len();
        let end = cuts.get(idx + 1).map(|c| c.0).unwrap_or(rest.len());
        Some(&rest[start..end])
    };

    let mut select_aliases: Vec<String> = Vec::new();
    let mut to_check: Vec<String> = Vec::new();
    for item in crate::engine::executor::split_comma_separated_tokens(list) {
        let (expr, al) = strip_alias(item);
        if let Some(a) = &al {
            select_aliases.push(a.to_lowercase());
        }
        let e = expr.trim();
        let e = e.strip_prefix("DISTINCT ").or_else(|| e.strip_prefix("distinct ")).unwrap_or(e).trim();
        // `(col).field`: chưa có kiểu composite nên cột nào cũng không phải composite (42809 như PostgreSQL)
        if let Some(rest) = e.strip_prefix('(') {
            if let Some((inner, field)) = rest.split_once(").") {
                let (inner, field) = (unquote(inner.trim()), field.trim());
                if !field.is_empty() && field.chars().all(|c| is_ident_char(c)) {
                    if let Some(i) = table.get_column_index(&inner) {
                        let decl = crate::engine::coltypes::column_decl(table, i);
                        if decl.as_ref().is_some_and(|d| crate::engine::composite::fields_of(&d.base).is_some()) {
                            continue;
                        }
                        let ty = decl.map(|d| d.sql_name()).unwrap_or_else(|| "text".to_string());
                        return Err(format!("column notation .{} applied to type {}, which is not a composite type", unquote(field), ty));
                    }
                }
            }
        }
        let idents = top_level_idents(e);
        // chỉ mục là đúng một định danh (`nope`, `t.nope`); `expr alias` có alias ngầm thì bỏ qua
        if idents.len() == 1 && idents[0].len() == e.len() {
            to_check.push(idents[0].clone());
        } else if al.is_none() && idents.len() == 1 && !e.contains(char::is_whitespace) {
            to_check.push(idents[0].clone());
        }
    }
    if let Some(w) = clause("WHERE") {
        to_check.extend(top_level_idents(w).into_iter().filter(|i| !select_aliases.contains(&i.to_lowercase())));
    }
    if let Some(o) = clause("ORDER") {
        let o = o.trim_start();
        let o = o.get(..2).filter(|p| p.eq_ignore_ascii_case("BY")).map(|_| &o[2..]).unwrap_or(o);
        to_check.extend(top_level_idents(o).into_iter().filter(|i| !select_aliases.contains(&unquote(i).to_lowercase())));
    }

    let qualifiers: Vec<String> = [Some(short.to_lowercase()), alias.as_ref().map(|a| a.to_lowercase())].into_iter().flatten().collect();
    for ident in to_check {
        let (qual, col) = match ident.rfind('.') {
            Some(p) => (Some(unquote(&ident[..p])), unquote(&ident[p + 1..])),
            None => (None, unquote(&ident)),
        };
        if let Some(q) = &qual {
            let q_short = q.rsplit('.').next().unwrap_or(q).trim_matches('"').to_lowercase();
            if !qualifiers.contains(&q_short) {
                return Ok(Checked::NotApplicable); // tham chiếu lạ (bảng khác...): để engine tự xử lý
            }
        }
        if table.get_column_index(&col).is_none() {
            let shown = crate::engine::frontend::decode_all(&col).into_owned();
            return Err(match qual {
                Some(q) => format!("column {}.{} does not exist", crate::engine::frontend::decode_all(&q), shown),
                None => format!("column \"{}\" does not exist", shown),
            });
        }
    }
    Ok(Checked::Table(short))
}
