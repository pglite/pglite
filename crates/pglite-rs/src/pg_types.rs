//! Ánh xạ kiểu của engine sang kiểu Postgres (OID) và định dạng text chuẩn Postgres.
//!
//! Chỉ dùng ở tầng kết quả truy vấn: không có gì ở đây được ghi xuống WAL,
//! nên định dạng file database không thay đổi.

use crate::types::DataType;
use serde::{Deserialize, Serialize};

pub const OID_BOOL: u32 = 16;
pub const OID_BYTEA: u32 = 17;
pub const OID_INT8: u32 = 20;
pub const OID_INT2: u32 = 21;
pub const OID_INT4: u32 = 23;
pub const OID_TEXT: u32 = 25;
pub const OID_JSON: u32 = 114;
pub const OID_FLOAT4: u32 = 700;
pub const OID_FLOAT8: u32 = 701;
pub const OID_BOOL_ARRAY: u32 = 1000;
pub const OID_INT2_ARRAY: u32 = 1005;
pub const OID_INT4_ARRAY: u32 = 1007;
pub const OID_TEXT_ARRAY: u32 = 1009;
pub const OID_INT8_ARRAY: u32 = 1016;
pub const OID_FLOAT8_ARRAY: u32 = 1022;
pub const OID_VARCHAR: u32 = 1043;
pub const OID_DATE: u32 = 1082;
pub const OID_TIME: u32 = 1083;
pub const OID_TIMESTAMP: u32 = 1114;
pub const OID_TIMESTAMP_ARRAY: u32 = 1115;
pub const OID_DATE_ARRAY: u32 = 1182;
pub const OID_TIMESTAMPTZ: u32 = 1184;
pub const OID_TIMESTAMPTZ_ARRAY: u32 = 1185;
pub const OID_NUMERIC_ARRAY: u32 = 1231;
pub const OID_NUMERIC: u32 = 1700;
pub const OID_UUID: u32 = 2950;
pub const OID_UUID_ARRAY: u32 = 2951;
pub const OID_JSONB: u32 = 3802;
pub const OID_JSONB_ARRAY: u32 = 3807;

/// Một cột kết quả theo giao thức Postgres.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WireField {
    pub name: String,
    #[serde(rename = "dataTypeID")]
    pub type_oid: u32,
}

/// Kết quả truy vấn dạng giao thức Postgres: cột theo đúng thứ tự SELECT,
/// mỗi dòng là mảng giá trị text chuẩn Postgres (`None` = NULL).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WireResult {
    pub fields: Vec<WireField>,
    pub rows: Vec<Vec<Option<String>>>,
    /// Tag cho CommandComplete, ví dụ `SELECT 2`, `INSERT 0 1`, `UPDATE 3`.
    pub command: String,
    #[serde(rename = "rowCount")]
    pub row_count: usize,
}

/// Kết quả describe: cột trả về và số tham số `$n` của câu lệnh.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DescribeResult {
    pub fields: Vec<WireField>,
    #[serde(rename = "paramCount")]
    pub param_count: usize,
}

impl DataType {
    /// OID Postgres gần đúng nhất cho kiểu engine đang lưu.
    /// TIMESTAMP và TIMESTAMPTZ được lưu chung nên đều báo là timestamptz (giá trị coi là UTC).
    pub fn pg_oid(&self) -> u32 {
        match self {
            DataType::Integer | DataType::Serial => OID_INT4,
            DataType::BigInt => OID_INT8,
            DataType::Text => OID_TEXT,
            DataType::Boolean => OID_BOOL,
            DataType::Numeric => OID_NUMERIC,
            DataType::Timestamp => OID_TIMESTAMPTZ,
            DataType::Date => OID_DATE,
            DataType::Time => OID_TIME,
            DataType::Jsonb => OID_JSONB,
            DataType::Uuid => OID_UUID,
            DataType::Bytea => OID_BYTEA,
            DataType::Array(elem) => array_oid_for_elem(elem),
        }
    }

    /// Chuỗi kiểu dùng trong `FieldInfo::data_type` (giống cách executor đang sinh).
    pub fn field_type_str(&self) -> String {
        format!("{:?}", self).to_lowercase()
    }
}

fn array_oid_for_elem(elem: &str) -> u32 {
    let e = elem.trim().trim_matches('"').to_uppercase();
    let base = e.split(|c: char| c == '(' || c.is_whitespace()).next().unwrap_or("");
    match base {
        "INT" | "INTEGER" | "INT4" | "SERIAL" => OID_INT4_ARRAY,
        "SMALLINT" | "INT2" => OID_INT2_ARRAY,
        "BIGINT" | "INT8" | "BIGSERIAL" => OID_INT8_ARRAY,
        "BOOL" | "BOOLEAN" => OID_BOOL_ARRAY,
        "NUMERIC" | "DECIMAL" => OID_NUMERIC_ARRAY,
        "FLOAT" | "FLOAT8" | "DOUBLE" | "REAL" | "FLOAT4" => OID_FLOAT8_ARRAY,
        "UUID" => OID_UUID_ARRAY,
        "DATE" => OID_DATE_ARRAY,
        "TIMESTAMP" | "TIMESTAMPTZ" => OID_TIMESTAMPTZ_ARRAY,
        "JSONB" | "JSON" => OID_JSONB_ARRAY,
        _ => OID_TEXT_ARRAY,
    }
}

/// OID cho chuỗi kiểu trong `FieldInfo::data_type`. Executor sinh ra hai nhóm chuỗi:
/// dạng Debug-lowercase của `DataType` (`integer`, `array("INT")`…) và literal kiểu
/// Postgres (`int8`, `float8`, `bool`, `json`…). Không nhận ra thì trả text.
pub fn oid_for_type_str(s: &str) -> u32 {
    let t = s.trim().to_lowercase();
    if let Some(inner) = t.strip_prefix("array(").and_then(|r| r.strip_suffix(')')) {
        return array_oid_for_elem(inner);
    }
    if let Some(elem) = t.strip_suffix("[]") {
        return array_oid_for_elem(elem);
    }
    match t.as_str() {
        "integer" | "int" | "int4" | "serial" => OID_INT4,
        "smallint" | "int2" => OID_INT2,
        "bigint" | "int8" | "bigserial" => OID_INT8,
        "text" | "string" => OID_TEXT,
        "varchar" | "character varying" => OID_VARCHAR,
        "boolean" | "bool" => OID_BOOL,
        "numeric" | "decimal" => OID_NUMERIC,
        "float8" | "double precision" | "float" => OID_FLOAT8,
        "float4" | "real" => OID_FLOAT4,
        "timestamp" | "timestamptz" | "timestamp with time zone" => OID_TIMESTAMPTZ,
        "timestamp without time zone" => OID_TIMESTAMP,
        "date" => OID_DATE,
        "time" => OID_TIME,
        "json" => OID_JSON,
        "jsonb" => OID_JSONB,
        "uuid" => OID_UUID,
        "bytea" => OID_BYTEA,
        "array" => OID_TEXT_ARRAY,
        _ => OID_TEXT,
    }
}

fn is_array_oid(oid: u32) -> bool {
    matches!(
        oid,
        OID_BOOL_ARRAY
            | OID_INT2_ARRAY
            | OID_INT4_ARRAY
            | OID_TEXT_ARRAY
            | OID_INT8_ARRAY
            | OID_FLOAT8_ARRAY
            | OID_TIMESTAMP_ARRAY
            | OID_DATE_ARRAY
            | OID_TIMESTAMPTZ_ARRAY
            | OID_NUMERIC_ARRAY
            | OID_UUID_ARRAY
            | OID_JSONB_ARRAY
    )
}

/// Chuyển một giá trị JSON của engine sang định dạng text chuẩn Postgres theo OID cột.
pub fn to_pg_text(v: &serde_json::Value, oid: u32) -> Option<String> {
    use serde_json::Value as J;
    match v {
        J::Null => None,
        J::Bool(b) => Some(if *b { "t" } else { "f" }.to_string()),
        J::Number(n) => {
            if oid == OID_BOOL {
                return Some(if n.as_f64().unwrap_or(0.0) != 0.0 { "t" } else { "f" }.to_string());
            }
            Some(n.to_string())
        }
        J::String(s) => Some(string_to_pg_text(s, oid)),
        J::Array(items) => {
            if oid == OID_JSON || oid == OID_JSONB {
                Some(v.to_string())
            } else if oid == OID_BYTEA {
                let bytes: Option<Vec<u8>> = items.iter().map(|x| x.as_u64().and_then(|n| u8::try_from(n).ok())).collect();
                match bytes {
                    Some(b) => Some(bytes_to_hex(&b)),
                    None => Some(v.to_string()),
                }
            } else if is_array_oid(oid) {
                Some(json_array_to_pg_array(items))
            } else {
                Some(v.to_string())
            }
        }
        J::Object(_) => Some(v.to_string()),
    }
}

fn string_to_pg_text(s: &str, oid: u32) -> String {
    match oid {
        OID_BOOL => match s.trim().to_lowercase().as_str() {
            "true" | "t" | "1" | "yes" | "on" => "t".to_string(),
            "false" | "f" | "0" | "no" | "off" => "f".to_string(),
            _ => s.to_string(),
        },
        OID_BYTEA => {
            // Engine lưu bytea dạng `\x..`, `0x..` hoặc danh sách JSON `[1,2]`
            let t = s.trim();
            if let Some(hex) = t.strip_prefix("0x") {
                return format!("\\x{}", hex.to_lowercase());
            }
            if t.starts_with('[') {
                if let Ok(items) = serde_json::from_str::<Vec<u8>>(t) {
                    return bytes_to_hex(&items);
                }
            }
            t.to_string()
        }
        OID_TIMESTAMPTZ => normalize_timestamptz(s),
        OID_TIMESTAMP => s.trim().replacen('T', " ", 1),
        OID_DATE => {
            let t = s.trim();
            match t.split_once(|c| c == 'T' || c == ' ') {
                Some((d, _)) if d.len() == 10 => d.to_string(),
                _ => t.to_string(),
            }
        }
        _ if is_array_oid(oid) => {
            let t = s.trim();
            if t.starts_with('[') {
                if let Ok(serde_json::Value::Array(items)) = serde_json::from_str::<serde_json::Value>(t) {
                    return json_array_to_pg_array(&items);
                }
            }
            s.to_string()
        }
        _ => s.to_string(),
    }
}

/// `2026-10-12 08:00:00` → `2026-10-12 08:00:00+00`; `...Z` → `...+00`; giữ nguyên offset có sẵn.
pub fn normalize_timestamptz(s: &str) -> String {
    let t = s.trim();
    if t.len() < 10 || !t.as_bytes()[0].is_ascii_digit() {
        return t.to_string();
    }
    if t.len() == 10 {
        return format!("{} 00:00:00+00", t);
    }
    let mut out = t.replacen('T', " ", 1);
    if let Some(stripped) = out.strip_suffix('Z').or_else(|| out.strip_suffix('z')) {
        return format!("{}+00", stripped);
    }
    // Offset nằm sau phần giờ (sau ký tự thứ 10): dạng +07, +07:00, -05:30
    let tail = &out[10..];
    if tail.contains('+') || tail.contains('-') {
        return out;
    }
    if out.to_uppercase().ends_with(" UTC") {
        out.truncate(out.len() - 4);
    }
    out.push_str("+00");
    out
}

fn bytes_to_hex(b: &[u8]) -> String {
    let mut s = String::with_capacity(2 + b.len() * 2);
    s.push_str("\\x");
    for byte in b {
        s.push_str(&format!("{:02x}", byte));
    }
    s
}

/// `["a","b c",null,1]` → `{a,"b c",NULL,1}`
fn json_array_to_pg_array(items: &[serde_json::Value]) -> String {
    use serde_json::Value as J;
    let mut out = String::from("{");
    for (i, item) in items.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        match item {
            J::Null => out.push_str("NULL"),
            J::Bool(b) => out.push_str(if *b { "t" } else { "f" }),
            J::Number(n) => out.push_str(&n.to_string()),
            J::Array(inner) => out.push_str(&json_array_to_pg_array(inner)),
            J::String(s) => push_pg_array_elem(&mut out, s),
            J::Object(_) => push_pg_array_elem(&mut out, &item.to_string()),
        }
    }
    out.push('}');
    out
}

fn push_pg_array_elem(out: &mut String, s: &str) {
    let needs_quote = s.is_empty()
        || s.eq_ignore_ascii_case("NULL")
        || s.chars().any(|c| matches!(c, ',' | '{' | '}' | '"' | '\\') || c.is_whitespace());
    if needs_quote {
        out.push('"');
        for c in s.chars() {
            if c == '"' || c == '\\' {
                out.push('\\');
            }
            out.push(c);
        }
        out.push('"');
    } else {
        out.push_str(s);
    }
}

/// Tag CommandComplete theo chuẩn Postgres từ `QueryResult::command` của engine.
pub fn command_tag(command: &str, row_count: usize) -> String {
    let c = command.trim().to_uppercase();
    match c.as_str() {
        "INSERT" => format!("INSERT 0 {}", row_count),
        "UPDATE" | "DELETE" | "SELECT" | "MERGE" | "FETCH" | "MOVE" | "COPY" => format!("{} {}", c, row_count),
        "" => format!("SELECT {}", row_count),
        _ => c,
    }
}

/// Số tham số `$n` lớn nhất trong câu SQL (bỏ qua chuỗi '...' và định danh "...").
pub fn count_params(sql: &str) -> usize {
    let b = sql.as_bytes();
    let mut max = 0usize;
    let mut i = 0;
    let mut in_str = false;
    let mut in_ident = false;
    while i < b.len() {
        let c = b[i];
        if in_str {
            if c == b'\'' {
                in_str = false;
            }
        } else if in_ident {
            if c == b'"' {
                in_ident = false;
            }
        } else if c == b'\'' {
            in_str = true;
        } else if c == b'"' {
            in_ident = true;
        } else if c == b'$' {
            let start = i + 1;
            let mut j = start;
            while j < b.len() && b[j].is_ascii_digit() {
                j += 1;
            }
            if j > start {
                if let Ok(n) = sql[start..j].parse::<usize>() {
                    max = max.max(n);
                }
                i = j;
                continue;
            }
        }
        i += 1;
    }
    max
}

/// Đổi tên kiểu SQL (trong `::type` hay `CAST(x AS type)`) sang chuỗi kiểu của FieldInfo.
pub fn type_str_for_sql_type(sql_type: &str) -> String {
    let t = sql_type.trim().trim_matches('"').to_lowercase();
    let base = t.split(|c: char| c == '(').next().unwrap_or("").trim();
    match base {
        "json" => "json".to_string(),
        "smallint" | "int2" => "int2".to_string(),
        "float8" | "double precision" | "float" => "float8".to_string(),
        "float4" | "real" => "float4".to_string(),
        "varchar" | "character varying" | "char" | "character" | "bpchar" | "name" => "text".to_string(),
        "timestamp without time zone" => "timestamp".to_string(),
        _ => DataType::from_sql_str(&t).field_type_str(),
    }
}

/// Suy kiểu cho một biểu thức trong SELECT. `col_type` tra kiểu của tham chiếu cột
/// (`col` hoặc `alias.col`). Trả `None` nếu không suy được (khi đó dùng text).
pub fn infer_expr_type(expr: &str, col_type: &dyn Fn(&str) -> Option<String>) -> Option<String> {
    let e = expr.trim();
    if e.is_empty() {
        return None;
    }
    // Cột đơn giản
    if let Some(t) = col_type(e) {
        return Some(t);
    }
    // Ép kiểu hậu tố: expr::type (lấy dấu :: cuối cùng ở cấp ngoài cùng)
    if let Some(pos) = rfind_top_level(e, "::") {
        let ty = e[pos + 2..].trim();
        if !ty.is_empty() && ty.chars().all(|c| c.is_ascii_alphanumeric() || c == '_' || c == ' ' || c == '(' || c == ')' || c == ',' || c == '[' || c == ']') {
            return Some(type_str_for_sql_type(ty));
        }
    }
    let upper = e.to_uppercase();
    // Literal
    if e.starts_with('\'') && e.ends_with('\'') {
        return Some("text".to_string());
    }
    if upper == "TRUE" || upper == "FALSE" {
        return Some("bool".to_string());
    }
    if let Ok(n) = e.parse::<i64>() {
        return Some(if i32::try_from(n).is_ok() { "integer" } else { "bigint" }.to_string());
    }
    if e.parse::<f64>().is_ok() {
        return Some("numeric".to_string());
    }
    match upper.as_str() {
        "NOW()" | "CURRENT_TIMESTAMP" | "LOCALTIMESTAMP" | "TRANSACTION_TIMESTAMP()" | "STATEMENT_TIMESTAMP()" | "CLOCK_TIMESTAMP()" => {
            return Some("timestamp".to_string())
        }
        "CURRENT_DATE" => return Some("date".to_string()),
        "CURRENT_TIME" | "LOCALTIME" => return Some("time".to_string()),
        "CURRENT_USER" | "SESSION_USER" | "USER" | "CURRENT_SCHEMA" | "CURRENT_SCHEMA()" | "CURRENT_DATABASE()" | "VERSION()" => {
            return Some("text".to_string())
        }
        _ => {}
    }
    // CAST(x AS type)
    if upper.starts_with("CAST(") && e.ends_with(')') {
        let inner = &e[5..e.len() - 1];
        if let Some(pos) = rfind_top_level(&inner.to_uppercase(), " AS ") {
            return Some(type_str_for_sql_type(&inner[pos + 4..]));
        }
    }
    // Gọi hàm: NAME(args) [OVER (...)]
    let paren = e.find('(')?;
    let fname = upper[..paren].trim();
    if fname.is_empty() || !fname.chars().all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '.') {
        return None;
    }
    let fname = fname.rsplit('.').next().unwrap_or(fname);
    let args = matching_paren_body(e, paren);
    let first_arg = args.map(|a| split_top_level_commas(a).into_iter().next().unwrap_or("").to_string());
    let t = match fname {
        "COUNT" | "ROW_NUMBER" | "RANK" | "DENSE_RANK" | "NTILE" => "int8",
        "SUM" => {
            // SUM của integer/bigint ra bigint/numeric trong Postgres; SUM(float) ra float8
            match first_arg.as_deref().and_then(|a| infer_expr_type(a, col_type)).as_deref() {
                Some("integer") | Some("int2") | Some("serial") => "int8",
                Some("float8") | Some("float4") => "float8",
                _ => "numeric",
            }
        }
        "AVG" => match first_arg.as_deref().and_then(|a| infer_expr_type(a, col_type)).as_deref() {
            Some("float8") | Some("float4") => "float8",
            _ => "numeric",
        },
        "MIN" | "MAX" | "COALESCE" | "NULLIF" | "GREATEST" | "LEAST" | "LAG" | "LEAD" | "FIRST_VALUE" | "LAST_VALUE" | "ABS" | "ROUND" | "CEIL" | "CEILING" | "FLOOR" | "TRUNC" => {
            return first_arg.as_deref().and_then(|a| infer_expr_type(a, col_type)).map(|t| {
                if matches!(fname, "ROUND" | "TRUNC") && t != "numeric" && t != "float8" {
                    "numeric".to_string()
                } else {
                    t
                }
            });
        }
        "LENGTH" | "CHAR_LENGTH" | "CHARACTER_LENGTH" | "OCTET_LENGTH" | "POSITION" | "STRPOS" | "ARRAY_LENGTH" | "CARDINALITY" | "JSONB_ARRAY_LENGTH" | "JSON_ARRAY_LENGTH" => "integer",
        "EXTRACT" | "DATE_PART" => "numeric",
        "RANDOM" | "PI" | "SQRT" | "POWER" | "POW" | "LN" | "LOG" | "EXP" | "PERCENT_RANK" | "CUME_DIST" => "float8",
        "BOOL_AND" | "BOOL_OR" | "EVERY" | "EXISTS" => "bool",
        "STRING_AGG" | "CONCAT" | "CONCAT_WS" | "UPPER" | "LOWER" | "TRIM" | "LTRIM" | "RTRIM" | "BTRIM" | "SUBSTRING" | "SUBSTR" | "LEFT" | "RIGHT" | "REPLACE" | "TO_CHAR" | "LPAD" | "RPAD" | "INITCAP" | "MD5" | "REPEAT" | "REVERSE" | "SPLIT_PART" | "FORMAT" | "QUOTE_IDENT" | "QUOTE_LITERAL" | "ENCODE" | "OBJ_DESCRIPTION" | "COL_DESCRIPTION" | "FORMAT_TYPE" => "text",
        "JSON_AGG" | "JSON_BUILD_OBJECT" | "JSON_BUILD_ARRAY" | "JSON_OBJECT_AGG" | "TO_JSON" | "ROW_TO_JSON" => "json",
        "JSONB_AGG" | "JSONB_BUILD_OBJECT" | "JSONB_BUILD_ARRAY" | "JSONB_OBJECT_AGG" | "TO_JSONB" | "JSONB_SET" | "JSONB_STRIP_NULLS" => "jsonb",
        "ARRAY_AGG" => {
            return Some(match first_arg.as_deref().and_then(|a| infer_expr_type(a, col_type)) {
                Some(t) => format!("array(\"{}\")", elem_name_for_type_str(&t)),
                None => "array".to_string(),
            })
        }
        "NOW" | "TRANSACTION_TIMESTAMP" | "STATEMENT_TIMESTAMP" | "CLOCK_TIMESTAMP" | "TO_TIMESTAMP" | "DATE_TRUNC" => "timestamp",
        "TO_DATE" => "date",
        "GEN_RANDOM_UUID" | "UUID_GENERATE_V4" => "uuid",
        "TO_NUMBER" => "numeric",
        _ => return None,
    };
    Some(t.to_string())
}

fn elem_name_for_type_str(t: &str) -> &'static str {
    match oid_for_type_str(t) {
        OID_INT4 | OID_INT2 => "INT",
        OID_INT8 => "BIGINT",
        OID_BOOL => "BOOLEAN",
        OID_NUMERIC => "NUMERIC",
        OID_FLOAT8 | OID_FLOAT4 => "FLOAT8",
        OID_UUID => "UUID",
        OID_DATE => "DATE",
        OID_TIMESTAMPTZ | OID_TIMESTAMP => "TIMESTAMPTZ",
        OID_JSONB | OID_JSON => "JSONB",
        _ => "TEXT",
    }
}

fn matching_paren_body(s: &str, open: usize) -> Option<&str> {
    let b = s.as_bytes();
    let mut depth = 0i32;
    let mut in_str = false;
    for (i, &c) in b.iter().enumerate().skip(open) {
        if in_str {
            if c == b'\'' {
                in_str = false;
            }
            continue;
        }
        match c {
            b'\'' => in_str = true,
            b'(' => depth += 1,
            b')' => {
                depth -= 1;
                if depth == 0 {
                    return Some(&s[open + 1..i]);
                }
            }
            _ => {}
        }
    }
    None
}

fn split_top_level_commas(s: &str) -> Vec<&str> {
    let b = s.as_bytes();
    let mut out = Vec::new();
    let mut depth = 0i32;
    let mut in_str = false;
    let mut start = 0;
    for (i, &c) in b.iter().enumerate() {
        if in_str {
            if c == b'\'' {
                in_str = false;
            }
            continue;
        }
        match c {
            b'\'' => in_str = true,
            b'(' => depth += 1,
            b')' => depth -= 1,
            b',' if depth == 0 => {
                out.push(s[start..i].trim());
                start = i + 1;
            }
            _ => {}
        }
    }
    out.push(s[start..].trim());
    out
}

/// Vị trí xuất hiện cuối cùng của `needle` ở cấp ngoài cùng (ngoài ngoặc và chuỗi).
fn rfind_top_level(s: &str, needle: &str) -> Option<usize> {
    let b = s.as_bytes();
    let n = needle.as_bytes();
    let mut depth = 0i32;
    let mut in_str = false;
    let mut found = None;
    let mut i = 0;
    while i < b.len() {
        let c = b[i];
        if in_str {
            if c == b'\'' {
                in_str = false;
            }
        } else {
            match c {
                b'\'' => in_str = true,
                b'(' => depth += 1,
                b')' => depth -= 1,
                _ => {
                    if depth == 0 && b[i..].starts_with(n) {
                        found = Some(i);
                    }
                }
            }
        }
        i += 1;
    }
    found
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn oid_mapping_covers_executor_type_strings() {
        assert_eq!(oid_for_type_str("integer"), OID_INT4);
        assert_eq!(oid_for_type_str("bigint"), OID_INT8);
        assert_eq!(oid_for_type_str("serial"), OID_INT4);
        assert_eq!(oid_for_type_str("boolean"), OID_BOOL);
        assert_eq!(oid_for_type_str("timestamp"), OID_TIMESTAMPTZ);
        assert_eq!(oid_for_type_str("jsonb"), OID_JSONB);
        assert_eq!(oid_for_type_str("json"), OID_JSON);
        assert_eq!(oid_for_type_str("array(\"INT\")"), OID_INT4_ARRAY);
        assert_eq!(oid_for_type_str("array(\"TEXT\")"), OID_TEXT_ARRAY);
        assert_eq!(oid_for_type_str("int8"), OID_INT8);
        assert_eq!(oid_for_type_str("float8"), OID_FLOAT8);
        assert_eq!(oid_for_type_str("something-else"), OID_TEXT);
        assert_eq!(oid_for_type_str(&DataType::Array("INT".into()).field_type_str()), OID_INT4_ARRAY);
    }

    #[test]
    fn pg_text_formatting() {
        assert_eq!(to_pg_text(&json!(true), OID_BOOL).as_deref(), Some("t"));
        assert_eq!(to_pg_text(&json!(null), OID_TEXT), None);
        assert_eq!(to_pg_text(&json!(4.5), OID_NUMERIC).as_deref(), Some("4.5"));
        assert_eq!(to_pg_text(&json!(["x", "y z", null]), OID_TEXT_ARRAY).as_deref(), Some("{x,\"y z\",NULL}"));
        assert_eq!(to_pg_text(&json!("[1,2]"), OID_INT4_ARRAY).as_deref(), Some("{1,2}"));
        assert_eq!(to_pg_text(&json!("[7831,7832]"), OID_TEXT).as_deref(), Some("[7831,7832]"));
        assert_eq!(to_pg_text(&json!({"a": 1}), OID_JSONB).as_deref(), Some("{\"a\":1}"));
        assert_eq!(to_pg_text(&json!([1, 2]), OID_BYTEA).as_deref(), Some("\\x0102"));
        assert_eq!(to_pg_text(&json!("2026-10-12 08:00:00"), OID_TIMESTAMPTZ).as_deref(), Some("2026-10-12 08:00:00+00"));
        assert_eq!(to_pg_text(&json!("2026-10-12T08:00:00Z"), OID_TIMESTAMPTZ).as_deref(), Some("2026-10-12 08:00:00+00"));
        assert_eq!(to_pg_text(&json!("2026-10-12T08:00:00.123+07:00"), OID_TIMESTAMPTZ).as_deref(), Some("2026-10-12 08:00:00.123+07:00"));
        assert_eq!(to_pg_text(&json!("2026-10-12T08:00:00"), OID_DATE).as_deref(), Some("2026-10-12"));
    }

    #[test]
    fn tags_and_params() {
        assert_eq!(command_tag("INSERT", 1), "INSERT 0 1");
        assert_eq!(command_tag("SELECT", 3), "SELECT 3");
        assert_eq!(command_tag("CREATE TABLE", 0), "CREATE TABLE");
        assert_eq!(count_params("SELECT * FROM t WHERE a = $1 AND b = $12 AND c = '$5'"), 12);
    }

    #[test]
    fn expression_types() {
        let cols = |c: &str| if c == "age" || c == "u.age" { Some("integer".to_string()) } else { None };
        assert_eq!(infer_expr_type("COUNT(*)", &cols).as_deref(), Some("int8"));
        assert_eq!(infer_expr_type("u.age", &cols).as_deref(), Some("integer"));
        assert_eq!(infer_expr_type("MAX(age)", &cols).as_deref(), Some("integer"));
        assert_eq!(infer_expr_type("SUM(age)", &cols).as_deref(), Some("int8"));
        assert_eq!(infer_expr_type("COALESCE(age, 0)", &cols).as_deref(), Some("integer"));
        assert_eq!(infer_expr_type("price::int", &cols).as_deref(), Some("integer"));
        assert_eq!(infer_expr_type("x::json", &cols).as_deref(), Some("json"));
        assert_eq!(infer_expr_type("CAST(x AS BIGINT)", &cols).as_deref(), Some("bigint"));
        assert_eq!(infer_expr_type("NOW()", &cols).as_deref(), Some("timestamp"));
        assert_eq!(infer_expr_type("to_char(d, 'YYYY')", &cols).as_deref(), Some("text"));
        assert_eq!(infer_expr_type("a || b", &cols), None);
    }
}


/// Mã SQLSTATE của PostgreSQL cho một thông báo lỗi của engine (để driver/ORM nhận biết loại lỗi,
/// vd. bắt `unique_violation`). `XX000` nếu không nhận ra.
pub fn sqlstate(message: &str) -> &'static str {
    let m = message.to_lowercase();
    let has = |s: &str| m.contains(s);
    if has("duplicate key value violates unique constraint") || has("could not create unique index") {
        "23505"
    } else if has("violates foreign key constraint") {
        "23503"
    } else if has("violates not-null constraint") || (has("contains null values") && has("column")) {
        "23502"
    } else if has("violates check constraint") || has("is violated by some row") {
        "23514"
    } else if has("cannot affect row a second time") {
        "21000"
    } else if has("invalid input syntax for type") {
        "22P02"
    } else if has("division by zero") {
        "22012"
    } else if has("savepoint") && has("does not exist") {
        "3B001"
    } else if has("can only be used in transaction blocks") {
        "25P01"
    } else if has("relation") && has("already exists") {
        "42P07"
    } else if has("column") && has("already exists") {
        "42701"
    } else if has("constraint") && has("already exists") {
        "42710"
    } else if (has("column") && has("does not exist")) || has("not found in") {
        "42703"
    } else if has("constraint") && has("does not exist") {
        "42704"
    } else if (has("relation") || has("table")) && (has("does not exist") || has("not found")) {
        "42P01"
    } else if has("other objects depend on it") || has("cannot truncate a table referenced") {
        "2BP01"
    } else if has("syntax error") || has("unsupported") {
        "42601"
    } else if has("must have the same number of columns") || has("more expressions than target columns") {
        "42601"
    } else if has("wal write failed") {
        "58030"
    } else {
        "XX000"
    }
}
