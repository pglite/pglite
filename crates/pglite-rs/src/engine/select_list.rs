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
    "BY", "SIMILAR", "TO", "ESCAPE", "OVER", "FILTER", "WHERE", "FROM", "INTERVAL", "SYMMETRIC", "ANY", "ALL", "SOME", "COLLATE",
    "TIME", "WITH", "WITHOUT", "NULL", "TRUE", "FALSE", "UNKNOWN", "ASC", "DESC", "NULLS", "FIRST", "LAST", "DATE", "TIMESTAMP",
    "ROW", "ARRAY", "EXISTS", "CURRENT_DATE", "CURRENT_TIME", "CURRENT_TIMESTAMP", "LOCALTIME", "LOCALTIMESTAMP", "DEFAULT", "ISNULL",
    "NOTNULL", "NORMALIZED", "OPERATOR", "UNION", "INTERSECT", "EXCEPT", "LIMIT", "OFFSET", "FETCH", "FOR", "GROUP", "HAVING", "ORDER",
    "WINDOW", "INTO", "VARYING", "PRECISION",
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
        "bit varying" | "varbit" => "varbit",
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
    // subscript / slice: tên của biểu thức gốc (`tags[1:2]` → `tags`)
    if s.ends_with(']') && !s.to_uppercase().starts_with("ARRAY[") {
        if let Some(open) = s.rfind('[') {
            let base = s[..open].trim();
            if !base.is_empty() && super::constraints::matching_bracket_ok(&s[open..]) {
                return default_column_name(base);
            }
        }
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
    // phép toán hai ngôi ở cấp ngoài cùng (`created - '1 hour'::interval`): ép kiểu chỉ thuộc vế phải
    let case_expr = up.starts_with("CASE") && up.ends_with("END");
    if !case_expr
        && (super::executor::find_top_level_math_op(s, &['+', '-', '*', '/', '%']).is_some_and(|(p, _)| !s[..p].trim().is_empty())
            || ["||", "#>", "=", "<", ">", "@>", "<@", "~", "&&"].iter().any(|op| super::executor::find_top_level_op(s, op).is_some_and(|p| p > 0))
            || ["LIKE", "ILIKE", "IN", "BETWEEN", "IS", "AND", "OR", "SIMILAR"].iter().any(|k| find_top_level_keyword(s, k).is_some_and(|p| p > 0)))
    {
        return "?column?".to_string();
    }
    if find_last_top_level_op(s, "::").is_some() || up.starts_with("CAST(") && s.ends_with(')') {
        if let Some((name, _)) = cast_name(s) {
            return name;
        }
    }
    if up.starts_with("CASE") && up.ends_with("END") {
        return "case".to_string();
    }
    if up.starts_with("ROW(") && s.ends_with(')') {
        return "row".to_string();
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
                    let n = name.rsplit('.').next().unwrap_or(name).to_lowercase();
                    // hàm gộp hypothetical-set sau khi viết lại `rank(x) WITHIN GROUP (...)`
                    return n.strip_prefix("pg_hyp_").map(str::to_string).unwrap_or(n);
                }
            }
        }
    }
    "?column?".to_string()
}

/// Tên cột của một ép kiểu theo FigureColname của PostgreSQL: (tên, độ mạnh). Cột / hàm bên trong
/// (độ mạnh 2) thắng tên kiểu (`n::numeric::text` → `n`); ép kiểu lồng của hằng thì kiểu ngoài cùng
/// thắng (`'1'::bit(1)::varbit` → `varbit`).
fn cast_name(s: &str) -> Option<(String, u8)> {
    let mut s = s.trim();
    while s.starts_with('(') && s.ends_with(')') && super::executor::is_fully_enclosed_pub(s) {
        s = s[1..s.len() - 1].trim();
    }
    let (arg, ty) = if let Some(p) = find_last_top_level_op(s, "::") {
        (&s[..p], &s[p + 2..])
    } else if s.len() > 5 && s.as_bytes()[..5].eq_ignore_ascii_case(b"CAST(") && s.ends_with(')') {
        let inside = &s[5..s.len() - 1];
        let a = find_top_level_keyword(inside, "AS")?;
        (&inside[..a], &inside[a + 2..])
    } else {
        let n = default_column_name(s);
        let strength = if n == "?column?" {
            0
        } else if s.ends_with('\'') {
            1 // literal có kiểu: INTERVAL '1 day'
        } else {
            2
        };
        return Some((n, strength));
    };
    let (inner, strength) = cast_name(arg)?;
    Some(if strength > 1 { (inner, strength) } else { (type_column_name(ty), 1) })
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
    // `'x'::timestamp a`, `x::double precision v`: từ cuối là tên kiểu sau `::`
    let after_cast = find_last_top_level_op(expr, "::").is_some_and(|p| {
        expr[p + 2..].chars().all(|c| c.is_alphanumeric() || c == ' ' || c == '_' || c == '(' || c == ')' || c == ',' || c == '[' || c == ']')
    });
    if !w.is_empty() && is_keyword(w) && !after_cast && !matches!(w.to_uppercase().as_str(), "END" | "NULL" | "TRUE" | "FALSE" | "CURRENT_TIMESTAMP" | "CURRENT_DATE" | "CURRENT_TIME" | "LOCALTIMESTAMP" | "LOCALTIME") {
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
    if t.len() < 7 || !t.as_bytes()[..6].eq_ignore_ascii_case(b"SELECT") {
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

/// Tên mặc định PostgreSQL của một biểu thức trên đường wire: như `default_column_name` sau khi viết lại
/// cú pháp (`trim(LEADING ...)` → `ltrim`), và `trim(x)` → `btrim` (object JSON giữ tên cũ `trim`).
pub(crate) fn wire_default_name(expr: &str) -> String {
    let rewritten = super::oprewrite::rewrite(expr);
    let n = default_column_name(rewritten.as_deref().unwrap_or(expr));
    if n == "trim" { "btrim".into() } else { n }
}

/// Tên cột PostgreSQL cho từng phần tử của danh sách SELECT (đường wire): alias nếu có, ngược lại tên
/// mặc định (`?column?`, `int4`, `count`...) — kể cả khi nhiều cột trùng tên, điều mà kết quả dạng object
/// không giữ được. `None` khi không chắc (có `*`, không phải SELECT, nhiều câu lệnh...). Cờ đi kèm:
/// tên chắc chắn đã là tên cột engine trả về (alias, tham chiếu cột) — dùng để kiểm tra khớp vị trí.
pub(crate) fn wire_names(sql: &str) -> Option<Vec<(String, bool, String)>> {
    let lexed = super::frontend::lex(sql).ok()?;
    let mut t = lexed.trim().trim_end_matches(';').trim();
    if t.len() < 7 || !t.as_bytes()[..6].eq_ignore_ascii_case(b"SELECT") || !t.as_bytes()[6].is_ascii_whitespace() || t.contains(';') {
        return None;
    }
    for kw in ["UNION", "INTERSECT", "EXCEPT"] {
        if let Some(p) = find_top_level_keyword(t, kw) {
            t = t[..p].trim_end();
        }
    }
    let end = select_list_end(t);
    let mut list = t.get(6..end)?.trim_start();
    let up = list.to_uppercase();
    if up.starts_with("DISTINCT ON") {
        let open = list.find('(')?;
        let close = super::constraints::matching_paren(list, open)?;
        list = &list[close + 1..];
    } else if up.starts_with("DISTINCT ") {
        list = &list[9..];
    } else if up.starts_with("ALL ") {
        list = &list[4..];
    }
    let mut out = Vec::new();
    for item in split_projection_items_pub(list) {
        let it = item.trim();
        if it.is_empty() || it == "*" || it.ends_with(".*") {
            return None;
        }
        let (name, certain, expr) = if let Some(p) = find_top_level_keyword(it, "AS") {
            let a = it[p + 2..].trim();
            (if a.starts_with('"') { a.trim_matches('"').replace("\"\"", "\"") } else { a.to_lowercase() }, true, it[..p].trim())
        } else if let (Some((expr, alias)), false) = (implicit_alias(it), is_column_ref(it)) {
            (alias, true, expr)
        } else {
            (wire_default_name(it), is_column_ref(it), it)
        };
        out.push((super::frontend::decode_all(&name).into_owned(), certain, expr.to_string()));
    }
    Some(out)
}

/// Loại giá trị đặc biệt của một biểu thức SELECT mà engine trả dạng JSON/chuỗi nhưng PostgreSQL in
/// theo kiểu riêng (đường wire).
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) enum WireShape {
    /// mảng: `ARRAY[...]`, `string_to_array(...)`, `array_append(...)`...
    Array { constructor: bool },
    /// `ROW(...)`, `(a, b)`
    Record,
    /// bytea từ `decode(...)` (engine trả chuỗi byte)
    ByteaText,
    /// bytea từ `sha256(...)`... (engine trả hex)
    ByteaHex,
}

pub(crate) fn wire_shape(expr: &str) -> Option<WireShape> {
    let mut s = expr.trim();
    while s.starts_with('(') && s.ends_with(')') && super::executor::is_fully_enclosed_pub(s) {
        let inner = s[1..s.len() - 1].trim();
        if super::executor::split_comma_separated_tokens(inner).len() > 1 {
            return Some(WireShape::Record);
        }
        s = inner;
    }
    let up = s.to_uppercase();
    // nối mảng: `0 || ARRAY[1]`, `arr || 3`
    if let Some(p) = super::executor::find_top_level_op(s, "||") {
        let (l, r) = (&s[..p], &s[p + 2..]);
        if [l, r].iter().any(|x| matches!(wire_shape(x), Some(WireShape::Array { .. }))) {
            return Some(WireShape::Array { constructor: false });
        }
    }
    if up.starts_with("ARRAY[") && s.ends_with(']') {
        return Some(WireShape::Array { constructor: true });
    }
    // slice a[1:2]
    if s.ends_with(']') {
        if let Some(open) = s.rfind('[') {
            let inner = &s[open + 1..s.len() - 1];
            if inner.contains(':') && !inner.contains("::") {
                return Some(WireShape::Array { constructor: false });
            }
        }
    }
    let open = s.find('(')?;
    // percentile_cont/disc(ARRAY[...]) WITHIN GROUP (...): mảng kết quả
    if up.starts_with("PERCENTILE_") && s[open + 1..].trim_start().to_uppercase().starts_with("ARRAY[") {
        return Some(WireShape::Array { constructor: false });
    }
    if !s.ends_with(')') || super::constraints::matching_paren(s, open) != Some(s.len() - 1) {
        return None;
    }
    match up[..open].trim() {
        "ROW" => Some(WireShape::Record),
        "ARRAY" => Some(WireShape::Array { constructor: true }),
        "STRING_TO_ARRAY" | "REGEXP_SPLIT_TO_ARRAY" | "REGEXP_MATCH" | "ARRAY_APPEND" | "ARRAY_PREPEND" | "ARRAY_CAT" | "ARRAY_REMOVE"
        | "ARRAY_REPLACE" | "ARRAY_AGG" | "ARRAY_FILL" | "ARRAY_POSITIONS" | "TRIM_ARRAY" | "ARRAY_REVERSE" | "ARRAY_SORT"
        | "ARRAY_SHUFFLE" | "ARRAY_SAMPLE" => Some(WireShape::Array { constructor: false }),
        "DECODE" => Some(WireShape::ByteaText),
        "SHA224" | "SHA256" | "SHA384" | "SHA512" | "DIGEST" | "HMAC" => Some(WireShape::ByteaHex),
        _ => None,
    }
}
