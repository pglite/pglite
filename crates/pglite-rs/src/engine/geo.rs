//! Hình học và XML tối thiểu (đợt 15, P3): `point(x, y)`, toán tử khoảng cách `<->` (điểm `(x,y)`, và vector
//! `[1,2,3]` theo khoảng cách Euclid như pgvector). Các hàm hình học / XML khác báo `0A000` rõ ràng thay vì trả
//! nguyên văn biểu thức như trước.
use crate::types::Value;

pub(crate) fn is_geo_fn(name: &str) -> bool {
    matches!(name, "POINT" | "PG_DISTANCE" | "PG_XMLELEMENT") || unsupported(name).is_some()
}

fn unsupported(name: &str) -> Option<&'static str> {
    Some(match name {
        "CIRCLE" | "LSEG" | "LINE" | "PATH" | "POLYGON" | "AREA" | "CENTER" | "DIAMETER" | "RADIUS" | "NPOINTS" | "PCLOSE" | "POPEN" => "geometric types",
        "XMLFOREST" | "XMLPARSE" | "XMLSERIALIZE" | "XPATH" | "XPATH_EXISTS" | "XMLCONCAT" | "XMLROOT" | "XMLPI" | "XMLCOMMENT"
        | "XML_IS_WELL_FORMED" | "XML_IS_WELL_FORMED_DOCUMENT" | "XML_IS_WELL_FORMED_CONTENT" | "XMLEXISTS" | "TABLE_TO_XML" | "QUERY_TO_XML" | "XMLTABLE" => "XML functions",
        _ => return None,
    })
}

pub(crate) fn geo_fn_type(name: &str) -> Option<&'static str> {
    Some(match name {
        "POINT" => "point",
        "PG_DISTANCE" => "double precision",
        "PG_XMLELEMENT" => "xml",
        _ => return None,
    })
}

fn num(v: &Value) -> Option<f64> {
    match v {
        Value::Int(i) => Some(*i as f64),
        Value::Float(f) => Some(*f),
        Value::Text(t) => t.trim().parse().ok(),
        _ => None,
    }
}

/// `(x,y)` hoặc `[a,b,...]` → toạ độ.
fn coords(v: &Value) -> Option<Vec<f64>> {
    let Value::Text(t) = v else { return None };
    let t = t.trim();
    let inner = t.strip_prefix('(').and_then(|x| x.strip_suffix(')')).or_else(|| t.strip_prefix('[').and_then(|x| x.strip_suffix(']')))?;
    inner.split(',').map(|p| p.trim().parse::<f64>().ok()).collect()
}

pub(crate) fn call_geo_fn(name: &str, args: &[Value]) -> Option<Value> {
    if let Some(what) = unsupported(name) {
        super::errors::raise(format!("feature not supported: {} are not supported ({}())", what, name.to_lowercase()));
        return Some(Value::Null);
    }
    if name == "PG_XMLELEMENT" {
        // xmlelement(name tên, nội dung...): nội dung NULL bị bỏ qua, ký tự đặc biệt được thoát
        let Some(Value::Text(tag)) = args.first() else { return Some(Value::Null) };
        let mut body = String::new();
        for a in &args[1..] {
            let t = match a {
                Value::Null => continue,
                Value::Text(t) => t.to_string(),
                Value::Int(i) => i.to_string(),
                Value::Float(f) => crate::pg_types::float8_text(*f),
                Value::Bool(b) => b.to_string(),
            };
            body.push_str(&t.replace('&', "&amp;").replace('<', "&lt;").replace('>', "&gt;"));
        }
        return Some(Value::text(if body.is_empty() { format!("<{}/>", tag) } else { format!("<{0}>{1}</{0}>", tag, body) }));
    }
    if args.iter().any(|a| a.is_null()) {
        return Some(Value::Null);
    }
    match name {
        "POINT" => {
            let (x, y) = (num(args.first()?)?, num(args.get(1)?)?);
            Some(Value::text(format!("({},{})", crate::pg_types::float8_text(x), crate::pg_types::float8_text(y))))
        }
        "PG_DISTANCE" => {
            let (a, b) = (coords(args.first()?), coords(args.get(1)?));
            match (a, b) {
                (Some(a), Some(b)) if a.len() == b.len() && !a.is_empty() => Some(Value::Float(a.iter().zip(&b).map(|(x, y)| (x - y) * (x - y)).sum::<f64>().sqrt())),
                (Some(a), Some(b)) if a.len() != b.len() => {
                    super::errors::raise(format!("different vector dimensions {} and {}", a.len(), b.len()));
                    Some(Value::Null)
                }
                _ => {
                    super::errors::raise("feature not supported: operator <-> is only supported for points and vectors".to_string());
                    Some(Value::Null)
                }
            }
        }
        _ => None,
    }
}
