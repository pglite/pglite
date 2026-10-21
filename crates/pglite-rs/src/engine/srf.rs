//! Hàm trả tập (set-returning function) bổ sung, dùng chung cho `FROM f(...)` và danh sách `SELECT f(...)`:
//! `jsonb_object_keys`, `jsonb_array_elements[_text]`, `jsonb_path_query`, `regexp_matches`,
//! `regexp_split_to_table`, `generate_subscripts`, `jsonb_to_record[set]`, `jsonb_populate_record[set]`
//! (và các bản `json_`). `generate_series` / `unnest` / `jsonb_each` vẫn ở đường cũ của executor.
use crate::types::{DataType, Value};

/// Kết quả: các cột (tên, kiểu lưu, tên kiểu PG cho wire nếu khác mặc định) và các dòng.
pub(crate) struct SrfOut {
    pub cols: Vec<(String, DataType, Option<&'static str>)>,
    pub rows: Vec<Vec<Value>>,
}

/// Tên (viết hoa) có phải hàm trả tập của module. `select_list` = chỉ các hàm một cột (dùng được trong SELECT).
pub(crate) fn is_srf(name: &str, select_list: bool) -> bool {
    matches!(
        name,
        "JSONB_OBJECT_KEYS" | "JSON_OBJECT_KEYS" | "JSONB_ARRAY_ELEMENTS" | "JSON_ARRAY_ELEMENTS" | "JSONB_ARRAY_ELEMENTS_TEXT" | "JSON_ARRAY_ELEMENTS_TEXT"
            | "JSONB_PATH_QUERY" | "REGEXP_MATCHES" | "REGEXP_SPLIT_TO_TABLE" | "GENERATE_SUBSCRIPTS"
    ) || !select_list && matches!(name, "JSONB_TO_RECORD" | "JSON_TO_RECORD" | "JSONB_TO_RECORDSET" | "JSON_TO_RECORDSET" | "JSONB_POPULATE_RECORD" | "JSON_POPULATE_RECORD" | "JSONB_POPULATE_RECORDSET" | "JSON_POPULATE_RECORDSET")
}

/// Hàm cần danh sách cột (`AS x(a int, ...)` hoặc kiểu bảng ở đối số đầu).
pub(crate) fn needs_record_columns(name: &str) -> bool {
    name.contains("_TO_RECORD") || name.contains("_POPULATE_RECORD")
}

fn json_arg(v: &Value) -> Result<Option<serde_json::Value>, String> {
    match v {
        Value::Null => Ok(None),
        Value::Text(t) => serde_json::from_str(t.trim()).map(Some).map_err(|_| format!("invalid input syntax for type json")),
        other => Ok(Some(super::executor::value_to_json_pub(other))),
    }
}

/// Ô của cột kiểu `dt` từ một giá trị JSON (như `jsonb_to_record`).
fn json_cell(j: &serde_json::Value, dt: &DataType) -> Result<Value, String> {
    use serde_json::Value as J;
    Ok(match (j, dt) {
        (J::Null, _) => Value::Null,
        (_, DataType::Jsonb) => Value::text(j.to_string()),
        (J::Number(n), DataType::Integer | DataType::BigInt | DataType::Serial) => match n.as_i64() {
            Some(i) => Value::Int(i),
            None => return Err(format!("invalid input syntax for type integer: \"{}\"", n)),
        },
        (J::String(s), DataType::Integer | DataType::BigInt | DataType::Serial) => match s.trim().parse::<i64>() {
            Ok(i) => Value::Int(i),
            Err(_) => return Err(format!("invalid input syntax for type integer: \"{}\"", s)),
        },
        (J::Number(n), DataType::Numeric) => n.as_i64().map(Value::Int).unwrap_or_else(|| Value::Float(n.as_f64().unwrap_or(0.0))),
        (J::String(s), DataType::Numeric) => match s.trim().parse::<f64>() {
            Ok(f) => Value::Float(f),
            Err(_) => return Err(format!("invalid input syntax for type numeric: \"{}\"", s)),
        },
        (J::Bool(b), _) => Value::Bool(*b),
        (J::String(s), DataType::Boolean) => match s.to_ascii_lowercase().as_str() {
            "t" | "true" | "yes" | "on" | "1" => Value::Bool(true),
            "f" | "false" | "no" | "off" | "0" => Value::Bool(false),
            _ => return Err(format!("invalid input syntax for type boolean: \"{}\"", s)),
        },
        (J::String(s), _) => Value::text(s),
        (other, _) => Value::text(other.to_string()),
    })
}

/// Tính hàm `name` (viết hoa) với đối số đã tính. `record_cols` = cột khai báo (tên, kiểu SQL) cho các hàm record.
pub(crate) fn eval(name: &str, args: &[Value], record_cols: &[(String, String)]) -> Result<SrfOut, String> {
    let lname = name.to_ascii_lowercase();
    let one = |dt: DataType, pg: Option<&'static str>, rows: Vec<Value>| SrfOut { cols: vec![(lname.clone(), dt, pg)], rows: rows.into_iter().map(|v| vec![v]).collect() };
    let arg = |i: usize| args.get(i).cloned().unwrap_or(Value::Null);
    let flags = |i: usize| match args.get(i) {
        Some(v) if !v.is_null() => v.as_str(),
        _ => String::new(),
    };
    match name {
        "JSONB_OBJECT_KEYS" | "JSON_OBJECT_KEYS" => {
            let Some(j) = json_arg(&arg(0))? else { return Ok(one(DataType::Text, None, vec![])) };
            let serde_json::Value::Object(m) = j else {
                return Err(format!("cannot call {} on a{}", lname, if j.is_array() { "n array" } else { " scalar" }));
            };
            let mut keys: Vec<String> = m.keys().cloned().collect();
            if name.starts_with("JSONB") {
                keys.sort_by(|a, b| a.len().cmp(&b.len()).then(a.cmp(b)));
            }
            Ok(one(DataType::Text, None, keys.into_iter().map(Value::text).collect()))
        }
        "JSONB_ARRAY_ELEMENTS" | "JSON_ARRAY_ELEMENTS" | "JSONB_ARRAY_ELEMENTS_TEXT" | "JSON_ARRAY_ELEMENTS_TEXT" => {
            let as_text = name.ends_with("_TEXT");
            let mut out = SrfOut { cols: vec![("value".into(), if as_text { DataType::Text } else { DataType::Jsonb }, None)], rows: vec![] };
            let Some(j) = json_arg(&arg(0))? else { return Ok(out) };
            let serde_json::Value::Array(a) = j else {
                return Err(if j.is_object() { "cannot extract elements from an object".into() } else { "cannot extract elements from a scalar".into() });
            };
            out.rows = a
                .into_iter()
                .map(|x| {
                    vec![match x {
                        serde_json::Value::Null if as_text => Value::Null,
                        serde_json::Value::String(s) if as_text => Value::text(s),
                        other => Value::text(other.to_string()),
                    }]
                })
                .collect();
            Ok(out)
        }
        "JSONB_PATH_QUERY" => {
            let (t, p) = (arg(0), arg(1));
            if t.is_null() || p.is_null() {
                return Ok(one(DataType::Jsonb, None, vec![]));
            }
            let target = json_arg(&t)?.unwrap_or(serde_json::Value::Null);
            let path = super::jsonpath::compiled(&p.as_str())?;
            let vars = match json_arg(&arg(2))? {
                None => serde_json::json!({}),
                Some(o @ serde_json::Value::Object(_)) => o,
                Some(_) => return Err("\"vars\" argument is not an object".into()),
            };
            let silent = arg(3).as_bool().unwrap_or(false);
            let items = match path.query(&target, &vars) {
                Ok(v) => v,
                Err(_) if silent => vec![],
                Err(e) => return Err(e),
            };
            Ok(one(DataType::Jsonb, None, items.iter().map(|j| Value::text(j.to_string())).collect()))
        }
        "REGEXP_MATCHES" => {
            let (s, p) = (arg(0), arg(1));
            if s.is_null() || p.is_null() {
                return Ok(one(DataType::Array("TEXT".into()), Some("text[]"), vec![]));
            }
            let rows = super::strfn::regexp_matches_rows(&s.as_str(), &p.as_str(), &flags(2))?;
            Ok(one(DataType::Array("TEXT".into()), Some("text[]"), rows))
        }
        "REGEXP_SPLIT_TO_TABLE" => {
            let (s, p) = (arg(0), arg(1));
            if s.is_null() || p.is_null() {
                return Ok(one(DataType::Text, None, vec![]));
            }
            Ok(one(DataType::Text, None, super::strfn::regexp_split_rows(&s.as_str(), &p.as_str(), &flags(2))?))
        }
        "GENERATE_SUBSCRIPTS" => {
            let a = arg(0);
            let dim = arg(1).as_i64().unwrap_or(1);
            let reverse = arg(2).as_bool().unwrap_or(false);
            let items = super::executor::val_to_array_items(&a);
            // chiều `dim` (1-based): độ dài của mảng ở mức lồng tương ứng
            let mut len = if a.is_null() { 0 } else { items.len() as i64 };
            let mut cur = items;
            for _ in 1..dim {
                cur = cur.first().map(super::executor::val_to_array_items).unwrap_or_default();
                len = cur.len() as i64;
            }
            if dim < 1 {
                len = 0;
            }
            let mut v: Vec<Value> = (1..=len).map(Value::Int).collect();
            if reverse {
                v.reverse();
            }
            Ok(one(DataType::Integer, None, v))
        }
        _ if needs_record_columns(name) => {
            let populate = name.contains("POPULATE");
            let json = if populate { arg(1) } else { arg(0) };
            let types: Vec<DataType> = record_cols.iter().map(|(_, t)| DataType::from_sql_str(t)).collect();
            let cols = record_cols.iter().zip(&types).map(|((n, _), dt)| (n.clone(), dt.clone(), None)).collect();
            let mut out = SrfOut { cols, rows: vec![] };
            let Some(j) = json_arg(&json)? else {
                if !name.ends_with("SET") {
                    out.rows.push(vec![Value::Null; record_cols.len()]);
                }
                return Ok(out);
            };
            let objs: Vec<serde_json::Value> = if name.ends_with("SET") {
                match j {
                    serde_json::Value::Array(a) => a,
                    _ => return Err("cannot call ".to_string() + &lname + " on a non-array"),
                }
            } else {
                vec![j]
            };
            for o in objs {
                let serde_json::Value::Object(m) = o else {
                    return Err(format!("cannot call {} on a{}", lname, " scalar"));
                };
                let mut row = Vec::with_capacity(record_cols.len());
                for ((n, _), dt) in record_cols.iter().zip(&types) {
                    row.push(match m.get(n) {
                        Some(v) => json_cell(v, dt)?,
                        None => Value::Null,
                    });
                }
                out.rows.push(row);
            }
            Ok(out)
        }
        _ => Err(format!("function {}() does not exist", lname)),
    }
}

/// `JSON_TABLE(ctx, 'path' [AS tên] [PASSING v AS x, ...] COLUMNS (cột kiểu [PATH 'p'] | cột FOR ORDINALITY |
/// cột kiểu EXISTS [PATH 'p'], ...))`: mỗi phần tử khớp `path` thành một dòng. `NESTED PATH` chưa hỗ trợ.
pub(crate) fn json_table(inner: &str, eval: &dyn Fn(&str) -> Value) -> Result<SrfOut, String> {
    use super::executor::{find_top_level_keyword as kw, split_function_args};
    let cols_at = kw(inner, "COLUMNS").ok_or("syntax error at or near \")\"")?;
    let head = &inner[..cols_at];
    let after = inner[cols_at + 7..].trim_start();
    if !after.starts_with('(') {
        return Err("syntax error in JSON_TABLE".into());
    }
    let close = super::constraints::matching_paren(after, 0).ok_or("syntax error in JSON_TABLE")?;
    let col_src = &after[1..close];
    if kw(col_src, "NESTED").is_some() {
        return Err("JSON_TABLE NESTED PATH is not supported".into());
    }
    let parts = split_function_args(head);
    let ctx = eval(parts.first().ok_or("syntax error in JSON_TABLE")?.trim());
    let rest = parts[1..].join(", ");
    let mut vars = serde_json::Map::new();
    let path_end = ["AS", "PASSING"].iter().filter_map(|k| kw(&rest, k)).min().unwrap_or(rest.len());
    let path_src = eval(rest[..path_end].trim());
    if let Some(p) = kw(&rest, "PASSING") {
        for item in split_function_args(&rest[p + 7..]) {
            if let Some(a) = kw(&item, "AS") {
                let v = eval(item[..a].trim());
                vars.insert(item[a + 2..].trim().trim_matches('"').to_string(), json_arg(&v)?.unwrap_or(serde_json::Value::Null));
            }
        }
    }
    let vars = serde_json::Value::Object(vars);
    // cột
    struct Col {
        name: String,
        dt: DataType,
        ordinality: bool,
        exists: bool,
        path: super::jsonpath::JsonPath,
    }
    let mut cols = Vec::new();
    for c in split_function_args(col_src) {
        let c = c.trim();
        let mut words = c.splitn(2, char::is_whitespace);
        let name = words.next().unwrap_or("").trim_matches('"').to_string();
        let tail = words.next().unwrap_or("").trim();
        let tu = tail.to_ascii_uppercase();
        if tu == "FOR ORDINALITY" {
            cols.push(Col { name, dt: DataType::BigInt, ordinality: true, exists: false, path: super::jsonpath::compiled("$")? });
            continue;
        }
        let path_kw = kw(tail, "PATH");
        let exists = kw(tail, "EXISTS").is_some();
        let type_end = [kw(tail, "PATH"), kw(tail, "EXISTS"), kw(tail, "FORMAT"), kw(tail, "DEFAULT"), kw(tail, "NULL"), kw(tail, "ERROR"), kw(tail, "WITH"), kw(tail, "WITHOUT"), kw(tail, "KEEP"), kw(tail, "OMIT")]
            .into_iter()
            .flatten()
            .min()
            .unwrap_or(tail.len());
        let dt = DataType::from_sql_str(tail[..type_end].trim());
        let psrc = match path_kw {
            Some(p) => {
                let lit = tail[p + 4..].trim_start();
                let end = lit[1..].find('\'').map(|e| e + 2).unwrap_or(lit.len());
                lit[1..end - 1].replace("''", "'")
            }
            None => format!("$.\"{}\"", name),
        };
        cols.push(Col { name, dt, ordinality: false, exists, path: super::jsonpath::compiled(&psrc)? });
    }
    let out_cols = cols.iter().map(|c| (c.name.clone(), if c.exists { DataType::Boolean } else { c.dt.clone() }, None)).collect();
    let mut out = SrfOut { cols: out_cols, rows: vec![] };
    let (Some(target), false) = (json_arg(&ctx)?, path_src.is_null()) else { return Ok(out) };
    let path = super::jsonpath::compiled(&path_src.as_str())?;
    for (i, item) in path.query(&target, &vars)?.iter().enumerate() {
        let mut row = Vec::with_capacity(cols.len());
        for c in &cols {
            if c.ordinality {
                row.push(Value::Int(i as i64 + 1));
                continue;
            }
            // đường dẫn cột tính trên phần tử hiện tại (`$` = phần tử)
            let found = c.path.query(item, &vars).unwrap_or_default();
            if c.exists {
                row.push(Value::Bool(!found.is_empty()));
                continue;
            }
            row.push(match found.as_slice() {
                [] => Value::Null,
                [one] => json_cell(one, &c.dt).unwrap_or(Value::Null),
                many => Value::text(serde_json::Value::Array(many.to_vec()).to_string()),
            });
        }
        out.rows.push(row);
    }
    Ok(out)
}
