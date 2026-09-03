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
    Jsonb,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ColumnDef {
    pub name: String,
    pub data_type: DataType,
    pub is_primary_key: bool,
    pub is_nullable: bool,
    pub default_value: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Value {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    Text(String),
}

impl Value {
    pub fn as_i64(&self) -> Option<i64> {
        match self {
            Value::Int(i) => Some(*i),
            Value::Float(f) => Some(*f as i64),
            Value::Text(s) => s.parse::<i64>().ok(),
            _ => None,
        }
    }

    pub fn as_bool(&self) -> Option<bool> {
        match self {
            Value::Bool(b) => Some(*b),
            Value::Int(i) => Some(*i != 0),
            Value::Text(s) => match s.to_lowercase().as_str() {
                "true" | "t" | "1" => Some(true),
                "false" | "f" | "0" => Some(false),
                _ => None,
            },
            _ => None,
        }
    }

    pub fn as_f64(&self) -> Option<f64> {
        match self {
            Value::Int(i) => Some(*i as f64),
            Value::Float(f) => Some(*f),
            Value::Text(s) => s.parse::<f64>().ok(),
            _ => None,
        }
    }

    pub fn as_text(&self) -> Option<&str> {
        match self {
            Value::Text(s) => Some(s.as_str()),
            _ => None,
        }
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
            (Value::Text(a), Value::Text(b)) => a == b,
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
            _ => false,
        }
    }

    pub fn cmp_value(&self, other: &Value) -> std::cmp::Ordering {
        match (self, other) {
            (Value::Int(a), Value::Int(b)) => a.cmp(b),
            (Value::Float(a), Value::Float(b)) => a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal),
            (Value::Int(a), Value::Float(b)) => (*a as f64).partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal),
            (Value::Float(a), Value::Int(b)) => a.partial_cmp(&(*b as f64)).unwrap_or(std::cmp::Ordering::Equal),
            (Value::Text(a), Value::Text(b)) => a.cmp(b),
            (Value::Bool(a), Value::Bool(b)) => a.cmp(b),
            (Value::Null, Value::Null) => std::cmp::Ordering::Equal,
            (Value::Null, _) => std::cmp::Ordering::Less,
            (_, Value::Null) => std::cmp::Ordering::Greater,
            _ => std::cmp::Ordering::Equal,
        }
    }

    pub fn as_str(&self) -> String {
        match self {
            Value::Null => "null".to_string(),
            Value::Bool(b) => b.to_string(),
            Value::Int(i) => i.to_string(),
            Value::Float(f) => f.to_string(),
            Value::Text(s) => s.clone(),
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
