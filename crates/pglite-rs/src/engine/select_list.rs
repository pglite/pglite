//! Chuẩn hoá danh sách SELECT trước khi thực thi: mọi phần tử không phải cột đơn được gán alias
//! tường minh theo quy tắc đặt tên của PostgreSQL (`count(*)` -> `count`, `'5'::int` -> `int4`,
//! `n::text` -> `n`, biểu thức khác -> `?column?`), và alias không có `AS` (`SELECT n "Num"`,
//! `count(*) total`) được nhận ra. Nhờ vậy mọi đường thực thi phía sau thấy cùng một tên cột.
//!
//! Kết quả là object JSON nên hai cột trùng tên sẽ đè nhau: tên mặc định nào đã có thì giữ nguyên
//! cách đặt tên cũ cho phần tử đó, để không mất dữ liệu.
use super::executor::{find_last_top_level_op, find_top_level_keyword, split_projection_items_pub};

const KEYWORDS: &[&str] = &[
    "AND", "OR", "NOT", "IS", "IN", "LIKE", "ILIKE", "BETWEEN", "THEN", "ELSE", "WHEN", "CASE", "END", "SELECT", "DISTINCT", "AS", "ON",
    "BY", "SIMILAR", "TO", "ESCAPE", "OVER", "FILTER", "WHERE", "FROM", "INTERVAL", "SYMMETRIC", "ANY", "ALL", "SOME", "COLLATE", "AT",
    "ZONE", "TIME", "WITH", "WITHOUT", "NULL", "TRUE", "FALSE", "UNKNOWN", "ASC", "DESC", "NULLS", "FIRST", "LAST", "DATE", "TIMESTAMP",
    "ROW", "ARRAY", "EXISTS", "CURRENT_DATE", "CURRENT_TIME", "CURRENT_TIMESTAMP", "LOCALTIME", "LOCALTIMESTAMP", "DEFAULT", "ISNULL",
    "NOTNULL", "NORMALIZED", "OPERATOR", "UNION", "INTERSECT", "EXCEPT", "LIMIT", "OFFSET", "FETCH", "FOR", "GROUP", "HAVING", "ORDER",
    "WINDOW", "INTO", "VARYING", "PRECISION", "ZONE",
];

fn is_keyword(w: &str) -> bool {
    KEYWORDS.iter().any(|k| k.eq_ignore_ascii_case(w))
}

fn is_ident_char(c: char) -> bool {
    c.is_alphanumeric() || c == '_' || c == '$'
}

/// `col`, `"Col"`, `t.col`, `t."Col"`, `s.t.col`
fn is_column_ref(s: &str) -> bool {
    let t = s.trim();
    !t.is_empty()
        && t.split('.').count() <= 3
        && t.split('.').all(|p| {
            let p = p.trim();
            (p.len() >= 2 && p.starts_with('"') && p.ends_with('"') && !p[1..p.len() - 1].contains('"'))
                || (p.chars().next().map_or(false, |c| c.is_alphabetic() || c == '_') && p.chars().all(|c| c.is_alphanumeric() || c == '_') && !is_keyword(p))
        })
}

/// Tên kiểu PostgreSQL trả về làm tên cột cho `'x'::type`.
fn type_column_name(t: &str) -> String {
    let base = t.trim().trim_end_matches("[]").split('(').next().unwrap_or("").trim().to_lowercase();
    let base = base.split_whitespace().collect::<Vec<_>>().join(" ");
    let is_array = t.trim().ends_with("[]");
    let name = match base.as_str() {
        "int" | "integer" | "int4" => "int4",
        "bigint" | "int8" => "int8",
        "smallint" | "int2" => "int2",
        "real" | "float4" => "float4",
        "double precision" | "float8" | "float" => "float8",
        "numeric" | "decimal" => "numeric",
        "boolean" | "bool" => "bool",
        "character varying" | "varchar" => "varchar",
        "character" | "char" | "bpchar" => "bpchar",
        "timestamp without time zone" | "timestamp" => "timestamp",
        "timestamp with time zone" | "timestamptz" => "timestamptz",
        "time without time zone" | "time" => "time",
        "time with time zone" | "timetz" => "timetz",
        other => return if is_array { other.to_string() } else { other.to_string() },
    };
    name.to_string()
}

/// Tên cột mặc định PostgreSQL đặt cho một biểu thức trong SELECT.
pub(crate) fn default_column_name(expr: &str) -> String {
    let mut s = expr.trim();
    while s.starts_with('(') && s.ends_with(')') && super::executor::is_fully_enclosed_pub(s) {
        let inner = s[1..s.len() - 1].trim();
        if super::executor::split_comma_separated_tokens(inner).len() > 1 {
            return "row".to_string();
        }
        s = inner;
    }
    if is_column_ref(s) {
        let last = s.rsplit('.').next().unwrap_or(s).trim();
        return if last.starts_with('"') { last.trim_matches('"').to_string() } else { last.to_lowercase() };
    }
    let up = s.to_uppercase();
    // literal có kiểu: INTERVAL '1 day', DATE '...', int '5'
    for (kw, name) in [("INTERVAL ", "interval"), ("TIMESTAMPTZ ", "timestamptz"), ("TIMESTAMP ", "timestamp"), ("DATE ", "date"), ("TIME ", "time"), ("INT ", "int4"), ("INTEGER ", "int4"), ("BIGINT ", "int8"), ("TEXT ", "text"), ("NUMERIC ", "numeric"), ("BOOLEAN ", "bool"), ("JSONB ", "jsonb"), ("UUID ", "uuid")] {
        if up.starts_with(kw) && s[kw.len()..].trim_start().starts_with('\'') && s.ends_with('\'') {
            return name.to_string();
        }
    }
    if let Some(p) = find_last_top_level_op(s, "::") {
        let inner = default_column_name(&s[..p]);
        return if inner == "?column?" { type_column_name(&s[p + 2..]) } else { inner };
    }
    if up.starts_with("CAST(") && s.ends_with(')') {
        let inside = &s[5..s.len() - 1];
        if let Some(a) = find_top_level_keyword(inside, "AS") {
            let inner = default_column_name(&inside[..a]);
            return if inner == "?column?" { type_column_name(&inside[a + 2..]) } else { inner };
        }
    }
    if up.starts_with("CASE") && up.ends_with("END") {
        return "case".to_string();
    }
    if up.starts_with("ARRAY[") || up.starts_with("ARRAY(") {
        return "array".to_string();
    }
    if up.starts_with("EXISTS") && s[6..].trim_start().starts_with('(') {
        return "exists".to_string();
    }
    if up.starts_with("NOT EXISTS") {
        return "?column?".to_string();
    }
    for kw in ["CURRENT_TIMESTAMP", "CURRENT_DATE", "CURRENT_TIME", "LOCALTIMESTAMP", "LOCALTIME", "CURRENT_USER", "SESSION_USER", "CURRENT_SCHEMA", "CURRENT_CATALOG", "USER"] {
        if up == kw {
            return kw.to_lowercase();
        }
    }
    // lời gọi hàm chiếm trọn biểu thức: f(...) [FILTER (...)] [OVER (...)]
    if let Some(open) = s.find('(') {
        let name = s[..open].trim();
        if !name.is_empty() && name.chars().all(|c| is_ident_char(c) || c == '.') && !is_keyword(name.rsplit('.').next().unwrap_or(name)) || matches!(name.to_uppercase().as_str(), "COALESCE" | "NULLIF" | "GREATEST" | "LEAST" | "EXTRACT" | "POSITION" | "SUBSTRING" | "OVERLAY" | "TRIM") {
            if let Some(close) = super::constraints::matching_paren(s, open) {
                let tail = s[close + 1..].trim().to_uppercase();
                if tail.is_empty() || tail.starts_with("OVER") || tail.starts_with("FILTER") || tail.starts_with("WITHIN GROUP") {
                    return name.rsplit('.').next().unwrap_or(name).to_lowercase();
                }
            }
        }
    }
    "?column?".to_string()
}

/// Alias không có `AS` ở cuối phần tử: (biểu thức, alias).
fn implicit_alias(item: &str) -> Option<(&str, String)> {
    let t = item.trim_end();
    if t.ends_with('"') {
        let open = t[..t.len() - 1].rfind('"')?;
        let before = &t[..open];
        if before.ends_with(char::is_whitespace) && !before.trim().is_empty() && !before.trim_end().ends_with('.') {
            let expr = before.trim_end();
            if alias_can_follow(expr) {
                return Some((expr, t[open + 1..t.len() - 1].to_string()));
            }
        }
        return None;
    }
    let start = t.rfind(|c: char| !is_ident_char(c)).map(|p| p + t[p..].chars().next().unwrap().len_utf8())?;
    let word = &t[start..];
    if word.is_empty() || is_keyword(word) || word.chars().next().map_or(true, |c| c.is_ascii_digit() || c == '$') {
        return None;
    }
    let before = &t[..start];
    if !before.ends_with(char::is_whitespace) {
        return None;
    }
    let expr = before.trim_end();
    if expr.is_empty() || !alias_can_follow(expr) {
        return None;
    }
    Some((expr, word.to_lowercase()))
}

/// Biểu thức `expr` kết thúc ở chỗ một alias có thể đứng sau (không phải giữa một toán tử).
fn alias_can_follow(expr: &str) -> bool {
    let last = expr.chars().last().unwrap_or(' ');
    if !(is_ident_char(last) || last == ')' || last == '\'' || last == '"' || last == ']' || last == '*') {
        return false;
    }
    if last == '*' {
        return false;
    }
    // từ cuối của biểu thức không được là từ khoá (vd. `x IS NOT` + `NULL` đã loại ở trên)
    let w_start = expr.rfind(|c: char| !is_ident_char(c)).map(|p| p + 1).unwrap_or(0);
    let w = &expr[w_start..];
    if !w.is_empty() && is_keyword(w) && !matches!(w.to_uppercase().as_str(), "END" | "NULL" | "TRUE" | "FALSE" | "CURRENT_TIMESTAMP" | "CURRENT_DATE" | "CURRENT_TIME" | "LOCALTIMESTAMP" | "LOCALTIME") {
        return false;
    }
    // cả biểu thức phải "đóng": số ngoặc cân bằng và không ở trong chuỗi
    let mut depth = 0i32;
    let mut sq = false;
    for c in expr.chars() {
        match c {
            '\'' => sq = !sq,
            '(' if !sq => depth += 1,
            ')' if !sq => depth -= 1,
            _ => {}
        }
    }
    depth == 0 && !sq
}

fn quote_alias(a: &str) -> String {
    format!("\"{}\"", a.replace('"', "\"\""))
}

/// Vị trí cuối danh sách SELECT (FROM, hoặc WHERE/ORDER BY/... khi không có FROM).
fn select_list_end(sql: &str) -> usize {
    if let Some(p) = find_top_level_keyword(sql, "FROM") {
        return p;
    }
    ["WHERE", "GROUP", "HAVING", "ORDER", "LIMIT", "OFFSET", "FETCH", "FOR", "WINDOW", "INTO"]
        .iter()
        .filter_map(|kw| find_top_level_keyword(sql, kw))
        .min()
        .unwrap_or(sql.len())
}

/// Viết lại `SELECT ...` với alias tường minh. Trả về `None` nếu không cần đổi gì.
pub(crate) fn normalize_select_sql(sql: &str) -> Option<String> {
    let t = sql.trim();
    if t.len() < 7 || !t[..6].eq_ignore_ascii_case("SELECT") {
        return None;
    }
    // SELECT * FROM ...: không có gì để đặt tên (đường nóng của tra cứu theo khoá)
    let after_kw = t[6..].trim_start();
    if after_kw.starts_with('*') && after_kw[1..].starts_with(char::is_whitespace) && !after_kw[1..].trim_start().starts_with(',') {
        return None;
    }
    let end = select_list_end(t);
    if end <= 6 {
        return None;
    }
    let mut list_start = 6;
    let after = &t[6..end];
    let a_trim = after.trim_start();
    let offset = 6 + (after.len() - a_trim.len());
    let up = a_trim.to_uppercase();
    if up.starts_with("DISTINCT ON") {
        let open = a_trim.find('(')?;
        let close = super::constraints::matching_paren(a_trim, open)?;
        list_start = offset + close + 1;
    } else if up.starts_with("DISTINCT ") || up.starts_with("DISTINCT\n") {
        list_start = offset + 8;
    } else if up.starts_with("ALL ") {
        list_start = offset + 3;
    }
    let list = &t[list_start..end];
    let items = split_projection_items_pub(list);
    let mut used: Vec<String> = Vec::new();
    let mut changed = false;
    let mut out_items = Vec::with_capacity(items.len());
    for item in &items {
        let it = item.trim();
        if it == "*" || it.ends_with(".*") {
            out_items.push(it.to_string());
            continue;
        }
        if let Some(p) = find_top_level_keyword(it, "AS") {
            used.push(it[p + 2..].trim().trim_matches('"').to_lowercase());
            out_items.push(it.to_string());
            continue;
        }
        if let Some((expr, alias)) = implicit_alias(it) {
            if !is_column_ref(it) {
                used.push(alias.to_lowercase());
                out_items.push(format!("{} AS {}", expr, quote_alias(&alias)));
                changed = true;
                continue;
            }
        }
        if is_column_ref(it) {
            used.push(default_column_name(it).to_lowercase());
            out_items.push(it.to_string());
            continue;
        }
        let name = default_column_name(it);
        if used.iter().any(|u| u == &name.to_lowercase()) || items.iter().filter(|o| find_top_level_keyword(o, "AS").is_none() && implicit_alias(o).is_none() && !is_column_ref(o) && default_column_name(o) == name).count() > 1 {
            // tên trùng: giữ cách đặt tên cũ để hai cột không đè nhau trong object kết quả
            out_items.push(it.to_string());
            continue;
        }
        used.push(name.to_lowercase());
        out_items.push(format!("{} AS {}", it, quote_alias(&name)));
        changed = true;
    }
    if !changed {
        return None;
    }
    Some(format!("{} {} {}", t[..list_start].trim_end(), out_items.join(", "), &t[end..]).trim_end().to_string())
}
