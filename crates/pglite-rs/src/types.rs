use compact_str::CompactString;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum DataType {
    Integer,
    BigInt,
    Serial,
    Text,
    Boolean,
    Numeric,
    Timestamp,
    Date,
    Time,
    Jsonb,
    Uuid,
    Bytea,
    Array(String),
}

impl DataType {
    pub fn from_sql_str(type_def: &str) -> Self {
        let cleaned = type_def.trim().trim_matches('"');
        let upper = cleaned.to_uppercase();
        let first_word = upper.split_whitespace().next().unwrap_or("");
        let base_type = first_word.split('(').next().unwrap_or(first_word).trim_matches(';');

        if upper.ends_with("[]") || upper.starts_with("ARRAY") {
            let elem = upper.trim_end_matches("[]").trim();
            return DataType::Array(elem.to_string());
        }

        match base_type {
            "UUID" => DataType::Uuid,
            "BYTEA" | "BLOB" | "BINARY" => DataType::Bytea,
            "SERIAL" | "BIGSERIAL" | "SMALLSERIAL" => DataType::Serial,
            "BIGINT" | "INT8" => DataType::BigInt,
            "INT" | "INTEGER" | "INT4" | "SMALLINT" | "INT2" => DataType::Integer,
            "BOOL" | "BOOLEAN" => DataType::Boolean,
            "NUMERIC" | "DECIMAL" | "FLOAT" | "FLOAT4" | "FLOAT8" | "DOUBLE" | "REAL" | "MONEY" => DataType::Numeric,
            "TIMESTAMP" | "TIMESTAMPTZ" => DataType::Timestamp,
            "DATE" => DataType::Date,
            "TIME" | "TIMETZ" => DataType::Time,
            "JSONB" | "JSON" => DataType::Jsonb,
            _ => {
                if upper.starts_with("UUID") {
                    DataType::Uuid
                } else if upper.starts_with("BYTEA") {
                    DataType::Bytea
                } else if upper.starts_with("TIMESTAMP") {
                    DataType::Timestamp
                } else if upper.starts_with("TIME ") || upper.starts_with("TIME(") {
                    DataType::Time
                } else if upper.starts_with("DATE") {
                    DataType::Date
                } else if upper.starts_with("JSONB") || upper.starts_with("JSON") {
                    DataType::Jsonb
                } else if upper.starts_with("DOUBLE") {
                    DataType::Numeric
                } else {
                    DataType::Text
                }
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ColumnDef {
    pub name: String,
    pub data_type: DataType,
    pub is_primary_key: bool,
    pub is_nullable: bool,
    pub default_value: Option<String>,
    #[serde(default)]
    pub comment: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Value {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    Text(CompactString),
}

impl Value {
    pub fn text(s: impl AsRef<str>) -> Self {
        Value::Text(CompactString::new(s.as_ref()))
    }
    pub fn as_i64(&self) -> Option<i64> {
        match self {
            Value::Int(i) => Some(*i),
            Value::Float(f) => Some(*f as i64),
            Value::Bool(b) => Some(if *b { 1 } else { 0 }),
            Value::Text(s) => s.trim().parse::<i64>().ok(),
            _ => None,
        }
    }

    pub fn as_bool(&self) -> Option<bool> {
        match self {
            Value::Bool(b) => Some(*b),
            Value::Int(i) => Some(*i != 0),
            Value::Float(f) => Some(*f != 0.0),
            Value::Text(s) => match s.trim().to_lowercase().as_str() {
                "true" | "t" | "1" | "yes" | "y" | "on" => Some(true),
                "false" | "f" | "0" | "no" | "n" | "off" => Some(false),
                _ => None,
            },
            _ => None,
        }
    }

    pub fn as_f64(&self) -> Option<f64> {
        match self {
            Value::Int(i) => Some(*i as f64),
            Value::Float(f) => Some(*f),
            Value::Bool(b) => Some(if *b { 1.0 } else { 0.0 }),
            Value::Text(s) => s.trim().parse::<f64>().ok(),
            _ => None,
        }
    }

    pub fn as_text(&self) -> Option<&str> {
        match self {
            Value::Text(s) => Some(s.as_str()),
            _ => None,
        }
    }

    pub fn is_text(&self) -> bool {
        matches!(self, Value::Text(_))
    }

    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }

    pub fn is_equal(&self, other: &Value) -> bool {
        match (self, other) {
            (Value::Null, Value::Null) => true,
            (Value::Bool(a), Value::Bool(b)) => a == b,
            (Value::Int(a), Value::Int(b)) => a == b,
            (Value::Float(a), Value::Float(b)) => (a - b).abs() <= 1e-6,
            (Value::Int(a), Value::Float(b)) => (*a as f64 - b).abs() <= 1e-6,
            (Value::Float(a), Value::Int(b)) => (a - *b as f64).abs() <= 1e-6,
            (Value::Text(a), Value::Text(b)) => {
                if a == b {
                    true
                } else if (a.len() == 36 || b.len() == 36) && a.eq_ignore_ascii_case(b) {
                    true
                } else if let (Some(ba), Some(bb)) = (extract_bytes_from_str(a), extract_bytes_from_str(b)) {
                    ba == bb
                } else if a.eq_ignore_ascii_case(b) && (a.starts_with("\\x") || a.starts_with("\\X") || a.starts_with(r"\x")) {
                    true
                } else {
                    false
                }
            }
            (Value::Int(a), Value::Text(b)) | (Value::Text(b), Value::Int(a)) => {
                if let Ok(i) = b.parse::<i64>() {
                    *a == i
                } else if let Ok(f) = b.parse::<f64>() {
                    (*a as f64 - f).abs() <= 1e-6
                } else {
                    false
                }
            }
            (Value::Float(a), Value::Text(b)) | (Value::Text(b), Value::Float(a)) => {
                if let Ok(f) = b.parse::<f64>() {
                    (*a - f).abs() <= 1e-6
                } else {
                    false
                }
            }
            (Value::Bool(a), Value::Text(b)) | (Value::Text(b), Value::Bool(a)) => {
                if let Some(b_val) = Value::Text(b.clone()).as_bool() {
                    *a == b_val
                } else {
                    false
                }
            }
            (Value::Bool(a), Value::Int(b)) | (Value::Int(b), Value::Bool(a)) => {
                (*a && *b != 0) || (!*a && *b == 0)
            }
            _ => false,
        }
    }

    pub fn cmp_value(&self, other: &Value) -> std::cmp::Ordering {
        match (self, other) {
            (Value::Int(a), Value::Int(b)) => a.cmp(b),
            (Value::Float(a), Value::Float(b)) => a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal),
            (Value::Int(a), Value::Float(b)) => (*a as f64).partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal),
            (Value::Float(a), Value::Int(b)) => a.partial_cmp(&(*b as f64)).unwrap_or(std::cmp::Ordering::Equal),
            (Value::Text(a), Value::Text(b)) => {
                if let (Some(ba), Some(bb)) = (extract_bytes_from_str(a), extract_bytes_from_str(b)) {
                    ba.cmp(&bb)
                } else if a.len() == 36 && b.len() == 36 {
                    a.to_lowercase().cmp(&b.to_lowercase())
                } else {
                    a.cmp(b)
                }
            }
            (Value::Bool(a), Value::Bool(b)) => a.cmp(b),
            (Value::Int(a), Value::Text(b)) => {
                if let Ok(i) = b.parse::<i64>() {
                    a.cmp(&i)
                } else if let Ok(f) = b.parse::<f64>() {
                    (*a as f64).partial_cmp(&f).unwrap_or(std::cmp::Ordering::Equal)
                } else {
                    self.as_str().cmp(&other.as_str())
                }
            }
            (Value::Text(a), Value::Int(b)) => {
                if let Ok(i) = a.parse::<i64>() {
                    i.cmp(b)
                } else if let Ok(f) = a.parse::<f64>() {
                    f.partial_cmp(&(*b as f64)).unwrap_or(std::cmp::Ordering::Equal)
                } else {
                    self.as_str().cmp(&other.as_str())
                }
            }
            (Value::Float(a), Value::Text(b)) => {
                if let Ok(f) = b.parse::<f64>() {
                    a.partial_cmp(&f).unwrap_or(std::cmp::Ordering::Equal)
                } else {
                    self.as_str().cmp(&other.as_str())
                }
            }
            (Value::Text(a), Value::Float(b)) => {
                if let Ok(f) = a.parse::<f64>() {
                    f.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal)
                } else {
                    self.as_str().cmp(&other.as_str())
                }
            }
            (Value::Null, Value::Null) => std::cmp::Ordering::Equal,
            (Value::Null, _) => std::cmp::Ordering::Less,
            (_, Value::Null) => std::cmp::Ordering::Greater,
            _ => self.as_str().cmp(&other.as_str()),
        }
    }

    pub fn as_str(&self) -> String {
        match self {
            Value::Null => "null".to_string(),
            Value::Bool(b) => b.to_string(),
            Value::Int(i) => i.to_string(),
            Value::Float(f) => f.to_string(),
            Value::Text(s) => s.to_string(),
        }
    }

    #[inline]
    pub fn write_json(&self, buf: &mut String) {
        match self {
            Value::Null => buf.push_str("null"),
            Value::Bool(b) => buf.push_str(if *b { "true" } else { "false" }),
            Value::Int(i) => {
                use std::fmt::Write;
                let _ = write!(buf, "{}", i);
            }
            Value::Float(f) => {
                use std::fmt::Write;
                if f.is_nan() || f.is_infinite() {
                    buf.push_str("null");
                } else {
                    let _ = write!(buf, "{}", f);
                }
            }
            Value::Text(s) => {
                write_json_str(s, buf);
            }
        }
    }
}

#[inline]
pub fn write_json_str(s: &str, buf: &mut String) {
    buf.push('"');
    let mut last = 0;
    for (i, b) in s.bytes().enumerate() {
        let escape = match b {
            b'"' => "\\\"",
            b'\\' => "\\\\",
            b'\n' => "\\n",
            b'\r' => "\\r",
            b'\t' => "\\t",
            0x08 => "\\b",
            0x0c => "\\f",
            _ => continue,
        };
        if last < i {
            buf.push_str(&s[last..i]);
        }
        buf.push_str(escape);
        last = i + 1;
    }
    if last < s.len() {
        buf.push_str(&s[last..]);
    }
    buf.push('"');
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FieldInfo {
    pub name: String,
    pub data_type: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryResult {
    pub rows: Vec<serde_json::Value>,
    #[serde(rename = "rowCount")]
    pub row_count: usize,
    pub fields: Vec<FieldInfo>,
    pub command: String,
}

impl QueryResult {
    pub fn to_json_string(&self) -> String {
        serde_json::to_string(self).unwrap_or_else(|_| "{}".to_string())
    }

    pub fn rows_to_json_string(&self) -> String {
        serde_json::to_string(&self.rows).unwrap_or_else(|_| "[]".to_string())
    }
}

fn extract_bytes_from_str(s: &str) -> Option<Vec<u8>> {
    let mut s_str = s.trim();
    while s_str.starts_with('\\') {
        s_str = &s_str[1..];
    }
    if s_str.starts_with('x') || s_str.starts_with('X') {
        if let Ok(b) = hex::decode(&s_str[1..]) {
            return Some(b);
        }
    } else if s_str.starts_with("0x") || s_str.starts_with("0X") {
        if let Ok(b) = hex::decode(&s_str[2..]) {
            return Some(b);
        }
    } else if s_str.starts_with('[') && s_str.ends_with(']') {
        if let Ok(nums) = serde_json::from_str::<Vec<serde_json::Value>>(s_str) {
            if !nums.is_empty() && nums.iter().all(|n| n.as_u64().map_or(false, |v| v <= 255)) {
                return Some(nums.iter().filter_map(|n| n.as_u64().map(|v| v as u8)).collect());
            }
        }
    }
    None
}

#[inline]
pub fn generate_uuid_v4() -> String {
    uuid::Uuid::new_v4().to_string()
}

// ---------------------------------------------------------------------------
// Ergonomic Type Conversions for Native Rust
// ---------------------------------------------------------------------------

impl From<i64> for Value {
    #[inline]
    fn from(v: i64) -> Self {
        Value::Int(v)
    }
}

impl From<i32> for Value {
    #[inline]
    fn from(v: i32) -> Self {
        Value::Int(v as i64)
    }
}

impl From<i16> for Value {
    #[inline]
    fn from(v: i16) -> Self {
        Value::Int(v as i64)
    }
}

impl From<i8> for Value {
    #[inline]
    fn from(v: i8) -> Self {
        Value::Int(v as i64)
    }
}

impl From<isize> for Value {
    #[inline]
    fn from(v: isize) -> Self {
        Value::Int(v as i64)
    }
}

impl From<u32> for Value {
    #[inline]
    fn from(v: u32) -> Self {
        Value::Int(v as i64)
    }
}

impl From<u16> for Value {
    #[inline]
    fn from(v: u16) -> Self {
        Value::Int(v as i64)
    }
}

impl From<u8> for Value {
    #[inline]
    fn from(v: u8) -> Self {
        Value::Int(v as i64)
    }
}

impl From<usize> for Value {
    #[inline]
    fn from(v: usize) -> Self {
        Value::Int(v as i64)
    }
}

impl From<f64> for Value {
    #[inline]
    fn from(v: f64) -> Self {
        Value::Float(v)
    }
}

impl From<f32> for Value {
    #[inline]
    fn from(v: f32) -> Self {
        Value::Float(v as f64)
    }
}

impl From<bool> for Value {
    #[inline]
    fn from(v: bool) -> Self {
        Value::Bool(v)
    }
}

impl From<&str> for Value {
    #[inline]
    fn from(v: &str) -> Self {
        Value::text(v)
    }
}

impl From<&String> for Value {
    #[inline]
    fn from(v: &String) -> Self {
        Value::text(v.as_str())
    }
}

impl From<String> for Value {
    #[inline]
    fn from(v: String) -> Self {
        Value::Text(CompactString::new(v))
    }
}

impl From<CompactString> for Value {
    #[inline]
    fn from(v: CompactString) -> Self {
        Value::Text(v)
    }
}

impl<T> From<Option<T>> for Value
where
    T: Into<Value>,
{
    #[inline]
    fn from(v: Option<T>) -> Self {
        match v {
            Some(x) => x.into(),
            None => Value::Null,
        }
    }
}

#[macro_export]
macro_rules! params {
    () => {
        Vec::<$crate::types::Value>::new()
    };
    ($($val:expr),* $(,)?) => {
        vec![$($crate::types::Value::from($val)),*]
    };
}

