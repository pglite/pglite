//! Kiểu composite (đợt 15): `CREATE TYPE t AS (a int, b text)`, `ROW(...)::t`, `'(1,x)'::t`, cột kiểu `t`,
//! truy cập trường `(x).a`, `DROP TYPE`.
//!
//! Định nghĩa lưu bằng `SchemaOp::ObjectMeta { kind: "composite", name, value: JSON [[tên, kiểu], ...] }` (bản cũ
//! bỏ qua op này; cột kiểu composite vẫn là cột văn bản bình thường). Giá trị lưu dạng văn bản record của PG
//! (`(1,x)`, `(,"a b")`).
use crate::types::Value;
use std::cell::RefCell;
use std::collections::BTreeMap;

pub(crate) type Fields = Vec<(String, String)>;

thread_local! {
    /// Kiểu composite của DB đang chạy (đặt ở đầu mỗi câu lệnh, như enum).
    static TYPES: RefCell<BTreeMap<String, Fields>> = const { RefCell::new(BTreeMap::new()) };
}

/// Đồng bộ danh sách kiểu composite từ `ObjectCatalog.extra` (rẻ khi không có kiểu nào).
pub(crate) fn set_session_types(extra: &BTreeMap<(String, String), String>) {
    let mut found: BTreeMap<String, Fields> = BTreeMap::new();
    for ((kind, name), json) in extra.range(("composite".to_string(), String::new())..) {
        if kind != "composite" {
            break;
        }
        if let Ok(f) = serde_json::from_str::<Fields>(json) {
            found.insert(name.clone(), f);
        }
    }
    TYPES.with(|t| {
        let mut t = t.borrow_mut();
        if *t != found {
            *t = found;
        }
    });
}

pub(crate) fn has_types() -> bool {
    TYPES.with(|t| !t.borrow().is_empty())
}

pub(crate) fn fields_of(name: &str) -> Option<Fields> {
    TYPES.with(|t| t.borrow().get(&name.trim().trim_matches('"').to_lowercase()).cloned())
}

/// Phần tử của văn bản record `(a,b,"c d")`; `None` cho trường rỗng không nháy (NULL).
pub(crate) fn parse_record(s: &str) -> Result<Vec<Option<String>>, String> {
    let t = s.trim();
    if !(t.starts_with('(') && t.ends_with(')')) {
        return Err(format!("malformed record literal: \"{}\"", t));
    }
    let inner = &t[1..t.len() - 1];
    let mut out = Vec::new();
    let mut cur = String::new();
    let mut quoted = false;
    let mut any_quote = false;
    let mut it = inner.chars().peekable();
    while let Some(c) = it.next() {
        match c {
            '"' if quoted && it.peek() == Some(&'"') => {
                cur.push('"');
                it.next();
            }
            '"' => {
                quoted = !quoted;
                any_quote = true;
            }
            '\\' => {
                if let Some(n) = it.next() {
                    cur.push(n);
                }
            }
            ',' if !quoted => {
                out.push(if cur.is_empty() && !any_quote { None } else { Some(std::mem::take(&mut cur)) });
                cur.clear();
                any_quote = false;
            }
            c => cur.push(c),
        }
    }
    out.push(if cur.is_empty() && !any_quote { None } else { Some(cur) });
    Ok(out)
}

fn quote_field(s: &str) -> String {
    if s.is_empty() || s.chars().any(|c| matches!(c, '"' | '\\' | '(' | ')' | ',') || c.is_whitespace()) {
        format!("\"{}\"", s.replace('\\', "\\\\").replace('"', "\"\""))
    } else {
        s.to_string()
    }
}

pub(crate) fn record_text(vals: &[Option<String>]) -> String {
    format!("({})", vals.iter().map(|v| v.as_deref().map(quote_field).unwrap_or_default()).collect::<Vec<_>>().join(","))
}

fn json_scalar_text(v: &serde_json::Value) -> Option<String> {
    match v {
        serde_json::Value::Null => None,
        serde_json::Value::String(s) => Some(s.clone()),
        serde_json::Value::Bool(b) => Some(if *b { "t".into() } else { "f".into() }),
        other => Some(other.to_string()),
    }
}

/// Giá trị của trường theo kiểu khai báo của nó.
fn typed_field(v: Option<&str>, ty: &str) -> Value {
    let Some(v) = v else { return Value::Null };
    let t = ty.trim().to_ascii_lowercase();
    match t.as_str() {
        "int" | "integer" | "int4" | "int2" | "smallint" | "int8" | "bigint" | "serial" | "bigserial" => v.trim().parse::<i64>().map(Value::Int).unwrap_or_else(|_| Value::text(v)),
        "bool" | "boolean" => Value::Bool(matches!(v.trim().to_ascii_lowercase().as_str(), "t" | "true" | "1" | "yes" | "on")),
        "float8" | "float4" | "real" | "double precision" => v.trim().parse::<f64>().map(Value::Float).unwrap_or_else(|_| Value::text(v)),
        _ => Value::text(v),
    }
}

/// Chuẩn hoá một giá trị về văn bản record của kiểu `ty` (từ `ROW(...)` — mảng JSON — hoặc văn bản record).
pub(crate) fn coerce(v: &Value, ty: &str) -> Result<Value, String> {
    let Some(fields) = fields_of(ty) else { return Ok(v.clone()) };
    let text = match v {
        Value::Null => return Ok(Value::Null),
        Value::Text(t) => t.to_string(),
        other => return Err(format!("cannot cast type {:?} to {}", other, ty)),
    };
    let t = text.trim();
    let vals: Vec<Option<String>> = if t.starts_with('[') {
        let arr: Vec<serde_json::Value> = serde_json::from_str(t).map_err(|_| format!("malformed record literal: \"{}\"", t))?;
        arr.iter().map(json_scalar_text).collect()
    } else {
        parse_record(t)?
    };
    if vals.len() > fields.len() {
        return Err(format!("malformed record literal: \"{}\"", t));
    }
    if vals.len() < fields.len() && t.starts_with('[') {
        return Err(format!("cannot cast type record to {}", ty));
    }
    if vals.len() < fields.len() {
        return Err(format!("malformed record literal: \"{}\"", t));
    }
    // kiểm tra kiểu từng trường số / bool
    let mut out = Vec::with_capacity(vals.len());
    for (v, (_, fty)) in vals.iter().zip(&fields) {
        let f = fty.to_ascii_lowercase();
        if let Some(s) = v {
            if matches!(f.as_str(), "int" | "integer" | "int4" | "int2" | "smallint" | "int8" | "bigint") && s.trim().parse::<i64>().is_err() {
                return Err(format!("invalid input syntax for type integer: \"{}\"", s));
            }
        }
        out.push(v.as_ref().map(|s| match f.as_str() {
            "bool" | "boolean" => if matches!(s.trim().to_ascii_lowercase().as_str(), "t" | "true" | "1" | "yes" | "on") { "t".into() } else { "f".into() },
            _ => s.clone(),
        }));
    }
    Ok(Value::text(record_text(&out)))
}

pub(crate) fn is_composite_fn(name: &str) -> bool {
    matches!(name, "PG_RECORD_IN" | "PG_RECORD_FIELD")
}

pub(crate) fn call_composite_fn(name: &str, args: &[Value]) -> Option<Value> {
    let ty = match args.get(if name == "PG_RECORD_IN" { 1 } else { 2 })? {
        Value::Text(t) => t.to_string(),
        _ => return Some(Value::Null),
    };
    match name {
        "PG_RECORD_IN" => match coerce(args.first()?, &ty) {
            Ok(v) => Some(v),
            Err(e) => {
                super::errors::raise(e);
                Some(Value::Null)
            }
        },
        _ => {
            let field = match args.get(1)? {
                Value::Text(t) => t.to_string(),
                _ => return Some(Value::Null),
            };
            let fields = fields_of(&ty)?;
            let Some(idx) = fields.iter().position(|(n, _)| n.eq_ignore_ascii_case(&field)) else {
                super::errors::raise(format!("could not identify column \"{}\" in record data type", field));
                return Some(Value::Null);
            };
            let rec = match coerce(args.first()?, &ty) {
                Ok(Value::Text(t)) => t.to_string(),
                Ok(_) => return Some(Value::Null),
                Err(e) => {
                    super::errors::raise(e);
                    return Some(Value::Null);
                }
            };
            let vals = parse_record(&rec).ok()?;
            Some(typed_field(vals.get(idx).and_then(|v| v.as_deref()), &fields[idx].1))
        }
    }
}

/// Kiểu PG (cho wire) của `(x).field`.
pub(crate) fn field_type(ty: &str, field: &str) -> Option<String> {
    fields_of(ty)?.into_iter().find(|(n, _)| n.eq_ignore_ascii_case(field)).map(|(_, t)| t)
}

/// Viết lại `X::kiểu_composite` → `pg_record_in(X, 'kiểu')` và `(X).trường` → `pg_record_field(X, 'trường', 'kiểu')`.
/// `col_type` trả kiểu composite của một cột (theo tên). `None` nếu không đổi gì.
pub(crate) fn rewrite(sql: &str, col_type: &dyn Fn(&str) -> Option<String>) -> Option<String> {
    use super::oprewrite::{code_mask, primary_start};
    let names: Vec<String> = TYPES.with(|t| t.borrow().keys().cloned().collect());
    if names.is_empty() {
        return None;
    }
    let mut s = sql.to_string();
    let mut changed = false;
    // 1. ép kiểu
    loop {
        let m = code_mask(&s);
        let lower = s.to_ascii_lowercase();
        let mut found: Option<(usize, usize, String)> = None;
        for n in &names {
            let pat = format!("::{}", n);
            let mut from = 0;
            while let Some(p) = lower[from..].find(&pat).map(|p| p + from) {
                from = p + pat.len();
                let end = p + pat.len();
                if !m[p] || lower.as_bytes().get(end).is_some_and(|c| c.is_ascii_alphanumeric() || *c == b'_') {
                    continue;
                }
                if found.as_ref().map_or(true, |(fp, _, _)| p < *fp) {
                    found = Some((p, end, n.clone()));
                }
                break;
            }
        }
        let Some((p, end, ty)) = found else { break };
        let Some(a) = primary_start(&s, &m, p) else { break };
        let rep = format!("pg_record_in({}, '{}')", &s[a..p], ty);
        s.replace_range(a..end, &rep);
        changed = true;
    }
    // 2. (X).field
    let mut from = 0;
    loop {
        let m = code_mask(&s);
        let b = s.as_bytes();
        let Some(p) = (from..b.len().saturating_sub(1)).find(|&i| m[i] && b[i] == b')' && b[i + 1] == b'.') else { break };
        from = p + 2;
        // trường: định danh ngay sau dấu chấm, không phải lời gọi hàm / `*`
        let fs = p + 2;
        let mut fe = fs;
        while fe < b.len() && (b[fe].is_ascii_alphanumeric() || b[fe] == b'_') {
            fe += 1;
        }
        if fe == fs || b.get(fe) == Some(&b'(') {
            continue;
        }
        // ngoặc mở tương ứng
        let mut depth = 0i32;
        let mut k = p as isize;
        while k >= 0 {
            let c = b[k as usize];
            if m[k as usize] {
                if c == b')' {
                    depth += 1;
                } else if c == b'(' {
                    depth -= 1;
                    if depth == 0 {
                        break;
                    }
                }
            }
            k -= 1;
        }
        if k < 0 {
            continue;
        }
        let open = k as usize;
        // `f(x).y` là lời gọi hàm: chỉ nhận `(X).y` đứng riêng
        if open > 0 && (b[open - 1].is_ascii_alphanumeric() || b[open - 1] == b'_') {
            continue;
        }
        let inner = s[open + 1..p].trim().to_string();
        let il = inner.to_ascii_lowercase();
        let ty = if il.starts_with("pg_record_in(") {
            il.rsplit('\'').nth(1).map(|t| t.to_string())
        } else if inner.chars().all(|c| c.is_alphanumeric() || c == '_' || c == '.' || c == '"') {
            col_type(inner.rsplit('.').next().unwrap_or(&inner).trim_matches('"'))
        } else {
            None
        };
        let Some(ty) = ty else { continue };
        let field = s[fs..fe].to_string();
        // mục trần của danh sách SELECT: đặt tên cột như PG (tên trường)
        let after = s[fe..].trim_start().to_ascii_lowercase();
        let no_alias = after.is_empty() || after.starts_with(',') || after.starts_with("from ") || after.starts_with(';');
        let alias = if no_alias && super::catalog_views::bare_select_item(&s[..open], &after) { format!(" AS \"{}\"", field.to_lowercase()) } else { String::new() };
        let rep = format!("pg_record_field({}, '{}', '{}'){}", inner, field.to_lowercase(), ty, alias);
        s.replace_range(open..fe, &rep);
        from = open + rep.len();
        changed = true;
    }
    changed.then_some(s)
}
