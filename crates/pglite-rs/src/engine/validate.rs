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

// ---------------------------------------------------------------------------------------------------------------
// Tham chiếu `alias.cột` ở mọi mức (JOIN, subquery, câu DML): alias gắn chắc chắn với một bảng thật mà bảng
// không có cột đó → 42703 như PostgreSQL, thay vì để bộ đánh giá coi là NULL. Alias không chắc chắn (CTE,
// subquery, hàm, cùng tên trỏ tới nhiều bảng) thì bỏ qua.

#[derive(Debug, Clone, PartialEq)]
enum Tok {
    Word(String),
    Dot,
    LParen,
    RParen,
    Comma,
    Other,
}

/// Tách token; `None` khi có chuỗi dollar ($$...$$) — không xét.
fn tokens(sql: &str) -> Option<Vec<Tok>> {
    let b = sql.as_bytes();
    let n = b.len();
    let mut out = Vec::new();
    let mut i = 0;
    while i < n {
        let c = b[i];
        match c {
            b'\'' => {
                i += 1;
                while i < n {
                    if b[i] == b'\'' {
                        if i + 1 < n && b[i + 1] == b'\'' {
                            i += 2;
                            continue;
                        }
                        break;
                    }
                    if b[i] == b'\\' {
                        i += 1;
                    }
                    i += 1;
                }
                i += 1;
                out.push(Tok::Other);
            }
            b'-' if i + 1 < n && b[i + 1] == b'-' => {
                while i < n && b[i] != b'\n' {
                    i += 1;
                }
            }
            b'/' if i + 1 < n && b[i + 1] == b'*' => {
                i += 2;
                while i + 1 < n && !(b[i] == b'*' && b[i + 1] == b'/') {
                    i += 1;
                }
                i += 2;
            }
            b'"' => {
                let st = i;
                i += 1;
                while i < n && b[i] != b'"' {
                    i += 1;
                }
                i += 1;
                out.push(Tok::Word(sql[st..i.min(n)].to_string()));
            }
            b'$' => {
                if i + 1 < n && b[i + 1].is_ascii_digit() {
                    i += 1;
                    while i < n && b[i].is_ascii_digit() {
                        i += 1;
                    }
                    out.push(Tok::Other);
                } else {
                    return None;
                }
            }
            b'.' => {
                // số thập phân `.5`
                if i + 1 < n && b[i + 1].is_ascii_digit() && !matches!(out.last(), Some(Tok::Word(_)) | Some(Tok::RParen)) {
                    i += 1;
                    while i < n && b[i].is_ascii_alphanumeric() {
                        i += 1;
                    }
                    out.push(Tok::Other);
                } else {
                    out.push(Tok::Dot);
                    i += 1;
                }
            }
            b'(' => {
                out.push(Tok::LParen);
                i += 1;
            }
            b')' => {
                out.push(Tok::RParen);
                i += 1;
            }
            b',' => {
                out.push(Tok::Comma);
                i += 1;
            }
            _ if c.is_ascii_digit() => {
                while i < n && (b[i].is_ascii_alphanumeric() || b[i] == b'.' && i + 1 < n && b[i + 1].is_ascii_digit()) {
                    i += 1;
                }
                out.push(Tok::Other);
            }
            _ if c.is_ascii_alphabetic() || c == b'_' || c >= 0x80 => {
                let st = i;
                while i < n && (b[i].is_ascii_alphanumeric() || b[i] == b'_' || b[i] == b'$' || b[i] >= 0x80) {
                    i += 1;
                }
                out.push(Tok::Word(sql[st..i].to_string()));
            }
            b':' if i + 1 < n && b[i + 1] == b':' => {
                // `::kiểu` — bỏ tên kiểu (có thể có dạng a.b)
                i += 2;
                out.push(Tok::Other);
                while i < n && b[i] == b' ' {
                    i += 1;
                }
                while i < n && (b[i].is_ascii_alphanumeric() || b[i] == b'_' || b[i] == b'.' || b[i] == b'"') {
                    i += 1;
                }
            }
            _ if c.is_ascii_whitespace() => i += 1,
            _ => {
                out.push(Tok::Other);
                i += 1;
            }
        }
    }
    Some(out)
}

const STOP_ALIAS: &[&str] = &[
    "WHERE", "JOIN", "ON", "LEFT", "RIGHT", "INNER", "OUTER", "FULL", "CROSS", "NATURAL", "GROUP", "ORDER", "LIMIT", "OFFSET", "SET",
    "USING", "UNION", "INTERSECT", "EXCEPT", "HAVING", "WINDOW", "FETCH", "FOR", "RETURNING", "VALUES", "SELECT", "DEFAULT", "LATERAL",
    "TABLESAMPLE", "ONLY", "WITH", "OVERRIDING", "DO", "CONFLICT", "AS", "AND", "OR", "NOT", "INTO", "FROM", "THEN", "WHEN", "ELSE", "END",
];

fn up(w: &str) -> String {
    w.to_ascii_uppercase()
}

/// Tên bảng ngắn (bỏ `public.`); `None` nếu thuộc schema khác.
fn table_short(parts: &[String]) -> Option<String> {
    match parts {
        [t] => Some(unquote(t).to_lowercase()),
        [s, t] if unquote(s).eq_ignore_ascii_case("public") => Some(unquote(t).to_lowercase()),
        _ => None,
    }
}

/// `Ok(Some(bảng đã xét))` khi không có lỗi; `Ok(None)` khi câu không thuộc diện xét.
pub(crate) fn check_qualified_refs(sql: &str, storage: &StorageEngine) -> Result<Option<Vec<String>>, String> {
    use std::collections::HashMap;
    let Some(t) = tokens(sql) else { return Ok(None) };
    let first = match t.first() {
        Some(Tok::Word(w)) => up(w),
        _ => return Ok(None),
    };
    if !matches!(first.as_str(), "SELECT" | "WITH" | "UPDATE" | "DELETE" | "INSERT") {
        return Ok(None);
    }
    if !t.windows(3).any(|w| matches!(w, [Tok::Word(_), Tok::Dot, Tok::Word(_)])) {
        return Ok(None);
    }
    let word = |k: usize| -> Option<&str> {
        match t.get(k) {
            Some(Tok::Word(w)) => Some(w.as_str()),
            _ => None,
        }
    };
    let is_kw = |k: usize, s: &str| word(k).is_some_and(|w| w.eq_ignore_ascii_case(s));

    // tên CTE: `name AS (`, `name AS [NOT] MATERIALIZED (`, `name (cols) AS (`
    let mut ctes: Vec<String> = Vec::new();
    for k in 0..t.len() {
        if let Some(w) = word(k) {
            let mut j = k + 1;
            if t.get(j) == Some(&Tok::LParen) {
                while j < t.len() && t[j] != Tok::RParen {
                    j += 1;
                }
                j += 1;
            }
            if is_kw(j, "AS") {
                let mut m = j + 1;
                if is_kw(m, "NOT") {
                    m += 1;
                }
                if is_kw(m, "MATERIALIZED") {
                    m += 1;
                }
                if t.get(m) == Some(&Tok::LParen) {
                    ctes.push(unquote(w).to_lowercase());
                }
            }
        }
    }

    // gắn alias: Some(bảng) hoặc None (không chắc chắn)
    let mut bind: HashMap<String, Option<String>> = HashMap::new();
    let add = |bind: &mut HashMap<String, Option<String>>, q: String, target: Option<String>| {
        let e = bind.entry(q).or_insert_with(|| target.clone());
        if *e != target {
            *e = None;
        }
    };
    // mức ngoặc nào là một câu truy vấn (FROM ở đó là mệnh đề FROM thật)
    let mut query_level: Vec<bool> = vec![true];
    let mut k = 0;
    while k < t.len() {
        match &t[k] {
            Tok::LParen => {
                let q = word(k + 1).is_some_and(|w| matches!(up(w).as_str(), "SELECT" | "WITH" | "VALUES"));
                query_level.push(q);
                k += 1;
                continue;
            }
            Tok::RParen => {
                if query_level.len() > 1 {
                    query_level.pop();
                }
                k += 1;
                continue;
            }
            _ => {}
        }
        let Some(w) = word(k) else {
            k += 1;
            continue;
        };
        let u = up(w);
        let at_query = *query_level.last().unwrap_or(&false);
        let rel_kw = match u.as_str() {
            "FROM" => at_query && !(k > 0 && is_kw(k - 1, "DISTINCT")),
            "JOIN" | "UPDATE" | "INTO" => at_query,
            "USING" => at_query && word(k + 1).is_some(),
            _ => false,
        };
        if !rel_kw {
            k += 1;
            continue;
        }
        // danh sách quan hệ: rel [, rel ...]
        let mut j = k + 1;
        loop {
            while is_kw(j, "ONLY") || is_kw(j, "LATERAL") {
                j += 1;
            }
            let target: Option<String>;
            if t.get(j) == Some(&Tok::LParen) {
                // (subquery) / (a JOIN b): không gắn
                let mut depth = 0;
                while j < t.len() {
                    match t[j] {
                        Tok::LParen => depth += 1,
                        Tok::RParen => {
                            depth -= 1;
                            if depth == 0 {
                                break;
                            }
                        }
                        _ => {}
                    }
                    j += 1;
                }
                j += 1;
                target = None;
            } else if let Some(_) = word(j) {
                let mut parts = vec![word(j).unwrap().to_string()];
                j += 1;
                while t.get(j) == Some(&Tok::Dot) && word(j + 1).is_some() {
                    parts.push(word(j + 1).unwrap().to_string());
                    j += 2;
                }
                if t.get(j) == Some(&Tok::LParen) {
                    // hàm trả về tập
                    let mut depth = 0;
                    while j < t.len() {
                        match t[j] {
                            Tok::LParen => depth += 1,
                            Tok::RParen => {
                                depth -= 1;
                                if depth == 0 {
                                    break;
                                }
                            }
                            _ => {}
                        }
                        j += 1;
                    }
                    j += 1;
                    target = None;
                } else {
                    let short = table_short(&parts);
                    target = short.as_ref().filter(|s| !ctes.contains(s) && storage.get_table(s).is_some()).cloned();
                    if let Some(s) = &short {
                        // tên bảng không có alias cũng là bộ định danh
                        let has_alias = is_kw(j, "AS") || word(j).is_some_and(|w| !STOP_ALIAS.contains(&up(w).as_str()));
                        if !has_alias {
                            add(&mut bind, s.clone(), target.clone());
                        }
                    }
                }
            } else {
                break;
            }
            // alias
            if is_kw(j, "AS") {
                j += 1;
            }
            if let Some(a) = word(j).filter(|w| !STOP_ALIAS.contains(&up(w).as_str())) {
                let a = unquote(a).to_lowercase();
                j += 1;
                // alias kèm danh sách cột đổi tên: không gắn
                let renamed = t.get(j) == Some(&Tok::LParen) && !is_kw(k, "INTO");
                add(&mut bind, a, if renamed { None } else { target.clone() });
            }
            if t.get(j) == Some(&Tok::Comma) && is_kw(k, "FROM") || t.get(j) == Some(&Tok::Comma) && is_kw(k, "USING") {
                j += 1;
                continue;
            }
            break;
        }
        // vòng ngoài vẫn quét tiếp từng token (subquery trong FROM có FROM/JOIN riêng)
        k += 1;
    }

    // tham chiếu q.col
    let mut used: Vec<String> = Vec::new();
    for k in 0..t.len().saturating_sub(2) {
        let (Tok::Word(q), Tok::Dot, Tok::Word(c)) = (&t[k], &t[k + 1], &t[k + 2]) else { continue };
        if k > 0 && matches!(t[k - 1], Tok::Dot | Tok::RParen) {
            continue;
        }
        if t.get(k + 3) == Some(&Tok::LParen) || t.get(k + 3) == Some(&Tok::Dot) {
            continue;
        }
        let ql = unquote(q).to_lowercase();
        let Some(Some(table)) = bind.get(&ql) else { continue };
        let Some(tbl) = storage.get_table(table) else { continue };
        if !used.contains(table) {
            used.push(table.clone());
        }
        let col = unquote(c);
        if matches!(col.to_lowercase().as_str(), "ctid" | "xmin" | "xmax" | "cmin" | "cmax" | "oid" | "tableoid") {
            continue;
        }
        if tbl.get_column_index(&col).is_none() {
            return Err(format!(
                "column {}.{} does not exist",
                crate::engine::frontend::decode_all(&unquote(q)),
                crate::engine::frontend::decode_all(&col)
            ));
        }
    }
    Ok(Some(used))
}
