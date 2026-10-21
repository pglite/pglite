//! Thư viện hàm vô hướng dùng chung cho mọi bộ đánh giá (`eval_sql_expr`, đường JOIN, `eval_operand_on_row`,
//! biểu thức biên dịch). Được gọi qua `strfn` — mỗi hàm chỉ cài một lần ở đây thay vì chép vào từng evaluator.
//!
//! - Toán: lượng giác (`sin`... `atan2`, dạng độ `sind`..., hyperbolic), `div`, `gcd`, `lcm`, `factorial`,
//!   `width_bucket`, `scale`, `trim_scale`, `min_scale`, `setseed`, `random([min, max])`.
//! - Chuỗi: `bit_length`, `casefold`, `normalize`, `quote_ident`, `quote_literal`, `quote_nullable`,
//!   `starts_with`, `to_hex`, `to_char(số, mẫu)`, `to_number`.
//! - Mảng: `cardinality`, `array_ndims`, `array_position(s)`, `array_fill`, `trim_array`, `array_reverse`, `array_sort`.
//! - Hệ thống: `pg_backend_pid`, `txid_current`, `pg_current_xact_id`, `pg_sleep`, `has_*_privilege`, `pg_has_role`,
//!   `pg_size_pretty`, `pg_column_size`, `uuidv4`, `uuidv7`.
//!
//! Quy ước: tên hàm đã viết hoa; đối số đã được tính; `None` = không xử lý (để nhánh khác của evaluator thử).
use crate::types::Value;
use std::cell::Cell;

/// Hàm do module này sở hữu hoàn toàn (đường biên dịch được phép chuyển thành `FuncCall`).
pub(crate) fn is_known(name: &str) -> bool {
    matches!(
        name,
        "SIN" | "COS" | "TAN" | "COT" | "ASIN" | "ACOS" | "ATAN" | "ATAN2" | "SIND" | "COSD" | "TAND" | "COTD" | "ASIND" | "ACOSD" | "ATAND"
            | "ATAN2D" | "SINH" | "COSH" | "TANH" | "ASINH" | "ACOSH" | "ATANH" | "DIV" | "GCD" | "LCM" | "FACTORIAL" | "WIDTH_BUCKET"
            | "SCALE" | "TRIM_SCALE" | "MIN_SCALE" | "SETSEED" | "BIT_LENGTH" | "CASEFOLD" | "NORMALIZE" | "QUOTE_IDENT" | "QUOTE_LITERAL"
            | "QUOTE_NULLABLE" | "STARTS_WITH" | "TO_HEX" | "TO_NUMBER" | "CARDINALITY" | "ARRAY_NDIMS" | "ARRAY_POSITION"
            | "ARRAY_POSITIONS" | "ARRAY_FILL" | "TRIM_ARRAY" | "ARRAY_REVERSE" | "ARRAY_SORT" | "PG_BACKEND_PID" | "TXID_CURRENT"
            | "PG_CURRENT_XACT_ID" | "PG_SLEEP" | "HAS_TABLE_PRIVILEGE" | "HAS_SCHEMA_PRIVILEGE" | "HAS_DATABASE_PRIVILEGE"
            | "HAS_COLUMN_PRIVILEGE" | "HAS_SEQUENCE_PRIVILEGE" | "HAS_FUNCTION_PRIVILEGE" | "PG_HAS_ROLE" | "PG_SIZE_PRETTY"
            | "PG_COLUMN_SIZE" | "UUIDV4" | "UUIDV7" | "OVERLAY" | "JSONB_INSERT" | "JSONB_SET_LAX" | "JSONB_PATH_EXISTS"
            | "JSONB_PATH_MATCH" | "JSONB_PATH_QUERY_ARRAY" | "JSONB_PATH_QUERY_FIRST" | "PG_IS_JSON" | "PG_ATAT" | "DIGEST" | "HMAC"
            | "CRYPT" | "GEN_SALT" | "UNACCENT" | "CONVERT_FROM" | "CONVERT_TO" | "PG_ADVISORY_LOCK" | "PG_ADVISORY_XACT_LOCK"
            | "PG_ADVISORY_LOCK_SHARED" | "PG_ADVISORY_XACT_LOCK_SHARED" | "PG_ADVISORY_UNLOCK" | "PG_ADVISORY_UNLOCK_SHARED"
            | "PG_ADVISORY_UNLOCK_ALL" | "PG_TRY_ADVISORY_LOCK" | "PG_TRY_ADVISORY_XACT_LOCK" | "PG_TRY_ADVISORY_LOCK_SHARED"
            | "PG_TRY_ADVISORY_XACT_LOCK_SHARED" | "PG_TOTAL_RELATION_SIZE" | "PG_RELATION_SIZE" | "PG_TABLE_SIZE" | "PG_INDEXES_SIZE"
            | "PG_DATABASE_SIZE" | "PG_POSTMASTER_START_TIME" | "PG_CONF_LOAD_TIME" | "INET_SERVER_ADDR" | "INET_CLIENT_ADDR"
            | "INET_SERVER_PORT" | "INET_CLIENT_PORT" | "PG_NOTIFY"
    )
}

/// Hàm chỉ xử lý một phần ở đây (vd. `to_char` với đối số số; dạng ngày giờ ở `datetime.rs`): chỉ thử ở các
/// evaluator dạng chuỗi, không được biên dịch thành `FuncCall`.
pub(crate) fn is_partial(name: &str) -> bool {
    matches!(name, "TO_CHAR" | "RANDOM" | "JSON_VALUE" | "JSON_QUERY" | "JSON_EXISTS" | "JSON_OBJECT")
}

/// Kiểu kết quả (tên kiểu PG) cho đường wire.
pub(crate) fn result_type(name: &str) -> Option<&'static str> {
    Some(match name {
        "SIN" | "COS" | "TAN" | "COT" | "ASIN" | "ACOS" | "ATAN" | "ATAN2" | "SIND" | "COSD" | "TAND" | "COTD" | "ASIND" | "ACOSD" | "ATAND"
        | "ATAN2D" | "SINH" | "COSH" | "TANH" | "ASINH" | "ACOSH" | "ATANH" | "RANDOM" => "double precision",
        "FACTORIAL" | "TRIM_SCALE" | "TO_NUMBER" => "numeric",
        "SCALE" | "MIN_SCALE" | "WIDTH_BUCKET" | "BIT_LENGTH" | "CARDINALITY" | "ARRAY_NDIMS" | "ARRAY_POSITION" | "PG_BACKEND_PID"
        | "PG_COLUMN_SIZE" => "integer",
        "TXID_CURRENT" => "bigint",
        "PG_CURRENT_XACT_ID" => "xid8",
        "SETSEED" | "PG_SLEEP" => "void",
        "CASEFOLD" | "NORMALIZE" | "OVERLAY" | "QUOTE_IDENT" | "QUOTE_LITERAL" | "QUOTE_NULLABLE" | "TO_HEX" | "PG_SIZE_PRETTY" => "text",
        "STARTS_WITH" | "HAS_TABLE_PRIVILEGE" | "HAS_SCHEMA_PRIVILEGE" | "HAS_DATABASE_PRIVILEGE" | "HAS_COLUMN_PRIVILEGE"
        | "HAS_SEQUENCE_PRIVILEGE" | "HAS_FUNCTION_PRIVILEGE" | "PG_HAS_ROLE" => "boolean",
        "ARRAY_POSITIONS" => "integer[]",
        "JSONB_INSERT" | "JSONB_SET_LAX" | "JSONB_PATH_QUERY_ARRAY" | "JSONB_PATH_QUERY_FIRST" | "JSON_QUERY" => "jsonb",
        "JSONB_PATH_EXISTS" | "JSONB_PATH_MATCH" | "JSON_EXISTS" | "PG_IS_JSON" | "PG_ATAT" => "boolean",
        "JSON_VALUE" | "CRYPT" | "GEN_SALT" | "UNACCENT" | "CONVERT_FROM" => "text",
        "DIGEST" | "HMAC" | "CONVERT_TO" => "bytea",
        "JSON_OBJECT" => "json",
        "UUIDV4" | "UUIDV7" => "uuid",
        "PG_ADVISORY_LOCK" | "PG_ADVISORY_XACT_LOCK" | "PG_ADVISORY_LOCK_SHARED" | "PG_ADVISORY_XACT_LOCK_SHARED" | "PG_ADVISORY_UNLOCK_ALL"
        | "PG_NOTIFY" => "void",
        "PG_ADVISORY_UNLOCK" | "PG_ADVISORY_UNLOCK_SHARED" | "PG_TRY_ADVISORY_LOCK" | "PG_TRY_ADVISORY_XACT_LOCK" | "PG_TRY_ADVISORY_LOCK_SHARED"
        | "PG_TRY_ADVISORY_XACT_LOCK_SHARED" => "boolean",
        "PG_TOTAL_RELATION_SIZE" | "PG_RELATION_SIZE" | "PG_TABLE_SIZE" | "PG_INDEXES_SIZE" | "PG_DATABASE_SIZE" => "bigint",
        "PG_POSTMASTER_START_TIME" | "PG_CONF_LOAD_TIME" => "timestamp with time zone",
        "INET_SERVER_ADDR" | "INET_CLIENT_ADDR" => "inet",
        "INET_SERVER_PORT" | "INET_CLIENT_PORT" => "integer",
        _ => return None,
    })
}

thread_local! {
    /// Trạng thái bộ sinh số ngẫu nhiên của phiên (`setseed`): `None` = chưa đặt hạt giống, dùng nguồn ngẫu nhiên hệ thống.
    static SEED: Cell<Option<u64>> = const { Cell::new(None) };
}

/// Số ngẫu nhiên trong [0, 1). Sau `setseed` dãy số lặp lại được (xorshift64*), như PG trong cùng phiên.
pub(crate) fn random_f64() -> f64 {
    let bits = SEED.with(|s| match s.get() {
        Some(mut x) => {
            x ^= x >> 12;
            x ^= x << 25;
            x ^= x >> 27;
            s.set(Some(x));
            x.wrapping_mul(0x2545_F491_4F6C_DD1D)
        }
        None => {
            let u = uuid::Uuid::new_v4();
            u64::from_le_bytes(u.as_bytes()[..8].try_into().unwrap())
        }
    });
    (bits >> 11) as f64 / (1u64 << 53) as f64
}

fn fail(msg: impl Into<String>) -> Option<Value> {
    super::errors::raise(msg);
    Some(Value::Null)
}

fn num(v: &Value) -> Option<f64> {
    match v {
        Value::Int(i) => Some(*i as f64),
        Value::Float(f) => Some(*f),
        Value::Text(s) => s.trim().parse().ok(),
        Value::Bool(_) | Value::Null => None,
    }
}

fn int(v: &Value) -> Option<i64> {
    match v {
        Value::Int(i) => Some(*i),
        Value::Float(f) if f.fract() == 0.0 => Some(*f as i64),
        Value::Text(s) => s.trim().parse().ok(),
        _ => None,
    }
}

/// Chuỗi thập phân của một số như PG in ra (`1.50` giữ nguyên nếu là text, float in dạng ngắn nhất).
fn decimal_text(v: &Value) -> Option<String> {
    match v {
        Value::Int(i) => Some(i.to_string()),
        Value::Float(f) if f.is_finite() => Some(format!("{}", f)),
        Value::Text(s) => {
            let t = s.trim();
            t.parse::<f64>().ok().map(|_| t.to_string())
        }
        _ => None,
    }
}

/// Giá trị numeric từ chuỗi thập phân: số nguyên → `Int`, còn lại `Float`.
fn numeric_value(s: &str) -> Value {
    if !s.contains(['.', 'e', 'E']) {
        if let Ok(i) = s.parse::<i64>() {
            return Value::Int(i);
        }
        return Value::text(s);
    }
    s.parse::<f64>().map(Value::Float).unwrap_or_else(|_| Value::text(s))
}

/// Giá trị vô hướng → JSON (không phụ thuộc chế độ raw text của đường wire).
fn json_of(v: &Value) -> serde_json::Value {
    match v {
        Value::Null => serde_json::Value::Null,
        Value::Bool(b) => serde_json::Value::Bool(*b),
        Value::Int(i) => serde_json::Value::from(*i),
        Value::Float(f) => serde_json::Number::from_f64(*f).map_or(serde_json::Value::Null, serde_json::Value::Number),
        Value::Text(s) => as_array(v).map_or_else(|| serde_json::Value::String(s.to_string()), serde_json::Value::Array),
    }
}

fn array_value(items: Vec<serde_json::Value>) -> Value {
    Value::text(serde_json::Value::Array(items).to_string())
}

/// Mảng dạng JSON (`None` nếu không phải mảng).
fn as_array(v: &Value) -> Option<Vec<serde_json::Value>> {
    let Value::Text(s) = v else { return None };
    let t = s.trim();
    let parsed = if t.starts_with('[') { serde_json::from_str(t).ok() } else { super::executor::parse_pg_array_to_json(t) };
    // mảng nhiều chiều dạng PG `{{1,2},{3,4}}`: phần tử con còn là chuỗi `{...}`
    fn nest(j: serde_json::Value) -> serde_json::Value {
        match j {
            serde_json::Value::String(s) if s.starts_with('{') && s.ends_with('}') => match super::executor::parse_pg_array_to_json(&s) {
                Some(serde_json::Value::Array(a)) => serde_json::Value::Array(a.into_iter().map(nest).collect()),
                _ => serde_json::Value::String(s),
            },
            // chế độ raw text của đường wire: mảng con của ARRAY[[...]] là chuỗi JSON `[1,2]`
            serde_json::Value::String(s) if s.starts_with('[') && s.ends_with(']') => match serde_json::from_str::<serde_json::Value>(&s) {
                Ok(serde_json::Value::Array(a)) => serde_json::Value::Array(a.into_iter().map(nest).collect()),
                _ => serde_json::Value::String(s),
            },
            other => other,
        }
    }
    match parsed {
        Some(serde_json::Value::Array(a)) => Some(a.into_iter().map(nest).collect()),
        _ => None,
    }
}

fn json_cmp(a: &serde_json::Value, b: &serde_json::Value) -> std::cmp::Ordering {
    use serde_json::Value as J;
    match (a, b) {
        (J::Null, J::Null) => std::cmp::Ordering::Equal,
        (J::Null, _) => std::cmp::Ordering::Greater,
        (_, J::Null) => std::cmp::Ordering::Less,
        (J::Number(x), J::Number(y)) => x.as_f64().unwrap_or(0.0).partial_cmp(&y.as_f64().unwrap_or(0.0)).unwrap_or(std::cmp::Ordering::Equal),
        (J::String(x), J::String(y)) => x.cmp(y),
        (J::Bool(x), J::Bool(y)) => x.cmp(y),
        (J::Array(x), J::Array(y)) => {
            for (p, q) in x.iter().zip(y) {
                let o = json_cmp(p, q);
                if o != std::cmp::Ordering::Equal {
                    return o;
                }
            }
            x.len().cmp(&y.len())
        }
        _ => a.to_string().cmp(&b.to_string()),
    }
}

/// So khớp phần tử mảng với giá trị (`IS NOT DISTINCT FROM`, số so theo giá trị).
fn json_matches(item: &serde_json::Value, v: &Value) -> bool {
    match (item, v) {
        (serde_json::Value::Null, Value::Null) => true,
        (serde_json::Value::Null, _) | (_, Value::Null) => false,
        (serde_json::Value::Number(n), _) => num(v).is_some_and(|x| n.as_f64() == Some(x)),
        (serde_json::Value::String(s), _) => *s == v.as_str(),
        (serde_json::Value::Bool(b), _) => v.as_bool() == Some(*b),
        _ => *item == json_of(v),
    }
}

fn depth(v: &serde_json::Value) -> usize {
    match v {
        serde_json::Value::Array(a) => 1 + a.first().map_or(0, depth),
        _ => 0,
    }
}

fn leaf_count(v: &serde_json::Value) -> usize {
    match v {
        serde_json::Value::Array(a) => a.iter().map(leaf_count).sum(),
        _ => 1,
    }
}

/// n! dạng chuỗi thập phân (nhân số lớn theo cơ số 10^9).
fn factorial(n: u64) -> String {
    let mut digits: Vec<u64> = vec![1];
    for k in 2..=n {
        let mut carry = 0u64;
        for d in digits.iter_mut() {
            let x = *d * k + carry;
            *d = x % 1_000_000_000;
            carry = x / 1_000_000_000;
        }
        while carry > 0 {
            digits.push(carry % 1_000_000_000);
            carry /= 1_000_000_000;
        }
    }
    let mut s = digits.last().unwrap().to_string();
    for d in digits.iter().rev().skip(1) {
        s.push_str(&format!("{:09}", d));
    }
    s
}

fn gcd(mut a: i128, mut b: i128) -> i128 {
    a = a.abs();
    b = b.abs();
    while b != 0 {
        (a, b) = (b, a % b);
    }
    a
}

/// `to_char(số, mẫu)`: các mẫu `9 0 . D , G FM S MI PL SG PR` và chuỗi trong nháy kép.
pub(crate) fn to_char_number(v: f64, fmt: &str) -> String {
    #[derive(Clone, Copy, PartialEq)]
    enum T {
        Nine,
        Zero,
        Point,
        Group,
        Sign,
        Minus,
        Plus,
        Pr,
        Lit(char),
    }
    let mut fm = false;
    let mut toks: Vec<T> = Vec::new();
    let chars: Vec<char> = fmt.chars().collect();
    let mut i = 0;
    while i < chars.len() {
        let rest: String = chars[i..].iter().take(2).collect::<String>().to_ascii_uppercase();
        match chars[i] {
            '9' => toks.push(T::Nine),
            '0' => toks.push(T::Zero),
            '.' => toks.push(T::Point),
            ',' => toks.push(T::Group),
            'D' | 'd' => toks.push(T::Point),
            'G' | 'g' => toks.push(T::Group),
            'S' | 's' if rest != "SG" => toks.push(T::Sign),
            '"' => {
                i += 1;
                while i < chars.len() && chars[i] != '"' {
                    toks.push(T::Lit(chars[i]));
                    i += 1;
                }
            }
            _ if rest == "FM" => {
                fm = true;
                i += 1;
            }
            _ if rest == "MI" => {
                toks.push(T::Minus);
                i += 1;
            }
            _ if rest == "PL" => {
                toks.push(T::Plus);
                i += 1;
            }
            _ if rest == "SG" => {
                toks.push(T::Sign);
                i += 1;
            }
            _ if rest == "PR" => {
                toks.push(T::Pr);
                i += 1;
            }
            c => toks.push(T::Lit(c)),
        }
        i += 1;
    }
    let point = toks.iter().position(|t| *t == T::Point);
    let int_slots = toks[..point.unwrap_or(toks.len())].iter().filter(|t| matches!(t, T::Nine | T::Zero)).count();
    let frac_slots = point.map_or(0, |p| toks[p + 1..].iter().filter(|t| matches!(t, T::Nine | T::Zero)).count());
    let neg = v < 0.0;
    let s = format!("{:.*}", frac_slots, v.abs());
    let (ip, fp) = s.split_once('.').unwrap_or((&s, ""));
    let mut int_digits: Vec<char> = if ip == "0" && frac_slots > 0 { Vec::new() } else { ip.chars().collect() };
    let overflow = int_digits.len() > int_slots;
    if int_slots == 0 {
        int_digits.clear();
    }
    let explicit_sign = toks.iter().any(|t| matches!(t, T::Sign | T::Minus | T::Plus | T::Pr));
    let mut out = String::new();
    let mut slot = 0usize;
    let mut started = false;
    let first_zero = toks.iter().take_while(|t| **t != T::Point).filter(|t| matches!(t, T::Nine | T::Zero)).position(|t| *t == T::Zero);
    let lead = int_slots.saturating_sub(int_digits.len());
    let mut frac_iter = fp.chars();
    let mut after_point = false;
    let mut pending_sign = !explicit_sign;
    let push_sign = |out: &mut String| out.push(if neg { '-' } else if fm { '\0' } else { ' ' });
    for t in &toks {
        match *t {
            T::Nine | T::Zero if !after_point => {
                let ch = if overflow {
                    Some('#')
                } else if slot < lead {
                    if first_zero.is_some_and(|z| slot >= z) { Some('0') } else { None }
                } else {
                    Some(int_digits[slot - lead])
                };
                match ch {
                    Some(c) => {
                        if pending_sign {
                            push_sign(&mut out);
                            pending_sign = false;
                        }
                        started = true;
                        out.push(c);
                    }
                    None if !fm => out.push(' '),
                    None => {}
                }
                slot += 1;
            }
            T::Nine | T::Zero => {
                out.push(if overflow { '#' } else { frac_iter.next().unwrap_or('0') });
            }
            T::Point => {
                if pending_sign {
                    push_sign(&mut out);
                    pending_sign = false;
                }
                after_point = true;
                out.push(if overflow { '#' } else { '.' });
            }
            T::Group => {
                if started || after_point {
                    out.push(',');
                } else if !fm {
                    out.push(' ');
                }
            }
            T::Sign => out.push(if neg { '-' } else { '+' }),
            T::Minus => out.push(if neg { '-' } else if fm { '\0' } else { ' ' }),
            T::Plus => out.push(if neg { '\0' } else { '+' }),
            T::Pr => {}
            T::Lit(c) => out.push(c),
        }
    }
    if pending_sign {
        out.insert(0, if neg { '-' } else { ' ' });
        if fm && !neg {
            out.remove(0);
        }
    }
    let mut out: String = out.chars().filter(|c| *c != '\0').collect();
    if fm && frac_slots > 0 {
        // FM: bỏ số 0 thừa của các ô `9` cuối phần thập phân
        let nines_tail = toks.iter().rev().take_while(|t| matches!(t, T::Nine | T::Lit(_))).filter(|t| **t == T::Nine).count();
        let mut removable = nines_tail;
        while removable > 0 && out.ends_with('0') {
            out.pop();
            removable -= 1;
        }
    }
    if toks.contains(&T::Pr) && neg {
        out = format!("<{}>", out.trim_start_matches('-').trim());
    } else if toks.contains(&T::Pr) && !fm {
        out = format!(" {} ", out);
    }
    out
}

/// `to_number(text, mẫu)`: bỏ ký tự nhóm/chữ, giữ chữ số, dấu thập phân và dấu âm (`-`, `<...>`, `MI`).
fn to_number(s: &str, fmt: &str) -> Option<Value> {
    let up = fmt.to_ascii_uppercase();
    let dec = if up.contains('D') { '.' } else { '.' };
    let neg = s.contains('-') || (s.trim_start().starts_with('<') && s.trim_end().ends_with('>'));
    let frac_slots = up.find(['.', 'D']).map(|p| up[p + 1..].chars().filter(|c| *c == '9' || *c == '0').count());
    let mut digits = String::new();
    let mut seen_point = false;
    let mut frac = 0usize;
    for c in s.chars() {
        if c.is_ascii_digit() {
            if seen_point {
                if frac_slots.is_some_and(|f| frac >= f) {
                    continue;
                }
                frac += 1;
            }
            digits.push(c);
        } else if c == dec && !seen_point && frac_slots.is_some() {
            seen_point = true;
            digits.push('.');
        }
    }
    if digits.is_empty() || digits == "." {
        return fail(format!("invalid input syntax for type numeric: \"{}\"", s));
    }
    let text = if neg { format!("-{}", digits) } else { digits };
    Some(numeric_value(text.trim_end_matches('.')))
}

fn size_pretty(bytes: f64) -> String {
    // giống pg_size_pretty: đổi đơn vị khi >= 10 * 1024 (làm tròn nửa lên)
    let units = ["bytes", "kB", "MB", "GB", "TB", "PB"];
    let mut v = bytes;
    let mut u = 0;
    while u + 1 < units.len() && v.abs() >= 10.0 * 1024.0 - 0.5 {
        v /= 1024.0;
        u += 1;
    }
    let r = if u == 0 { v } else { v.round() };
    format!("{} {}", r as i64, units[u])
}

/// Gọi hàm `name` (viết hoa) với đối số đã tính.
pub(crate) fn call(name: &str, args: &[Value]) -> Option<Value> {
    // hàm không phụ thuộc NULL ở đối số
    match name {
        "PG_BACKEND_PID" => return Some(Value::Int(std::process::id() as i64)),
        "TXID_CURRENT" | "PG_CURRENT_XACT_ID" => return Some(Value::Int(1000)),
        "UUIDV4" => return Some(Value::text(uuid::Uuid::new_v4().to_string())),
        "UUIDV7" => return Some(Value::text(uuid_v7())),
        // engine nhúng một tiến trình: khoá advisory luôn lấy được ngay
        "PG_ADVISORY_LOCK" | "PG_ADVISORY_XACT_LOCK" | "PG_ADVISORY_LOCK_SHARED" | "PG_ADVISORY_XACT_LOCK_SHARED" | "PG_ADVISORY_UNLOCK_ALL" => {
            return Some(Value::text(""));
        }
        "PG_ADVISORY_UNLOCK" | "PG_ADVISORY_UNLOCK_SHARED" | "PG_TRY_ADVISORY_LOCK" | "PG_TRY_ADVISORY_XACT_LOCK" | "PG_TRY_ADVISORY_LOCK_SHARED"
        | "PG_TRY_ADVISORY_XACT_LOCK_SHARED" => return Some(Value::Bool(true)),
        // kích thước: engine không chia trang như PG; trả một trang (8 kB) cho quan hệ, như bảng nhỏ trong PG
        "PG_TOTAL_RELATION_SIZE" | "PG_RELATION_SIZE" | "PG_TABLE_SIZE" | "PG_INDEXES_SIZE" | "PG_DATABASE_SIZE" => {
            return Some(if args.first().is_some_and(|a| a.is_null()) { Value::Null } else { Value::Int(8192) });
        }
        "PG_POSTMASTER_START_TIME" | "PG_CONF_LOAD_TIME" => {
            static START: std::sync::OnceLock<String> = std::sync::OnceLock::new();
            let t = START.get_or_init(super::executor::get_current_timestamp);
            return Some(Value::text(t));
        }
        // kết nối nhúng (không qua TCP): PG trả NULL với kết nối unix socket
        "INET_SERVER_ADDR" | "INET_CLIENT_ADDR" | "INET_SERVER_PORT" | "INET_CLIENT_PORT" => return Some(Value::Null),
        "PG_NOTIFY" => return Some(Value::text("")),
        "QUOTE_NULLABLE" => {
            let v = args.first()?;
            return Some(Value::text(if v.is_null() { "NULL".to_string() } else { super::strfn::quote_literal_text(&v.as_str()) }));
        }
        "HAS_TABLE_PRIVILEGE" | "HAS_SCHEMA_PRIVILEGE" | "HAS_DATABASE_PRIVILEGE" | "HAS_COLUMN_PRIVILEGE" | "HAS_SEQUENCE_PRIVILEGE"
        | "HAS_FUNCTION_PRIVILEGE" | "PG_HAS_ROLE" => {
            return Some(if args.iter().any(|a| a.is_null()) { Value::Null } else { Value::Bool(true) });
        }
        "RANDOM" => {
            return match args.len() {
                0 => Some(Value::Float(random_f64())),
                2 => {
                    if args.iter().any(|a| a.is_null()) {
                        return Some(Value::Null);
                    }
                    match (int(&args[0]), int(&args[1]), &args[0], &args[1]) {
                        (Some(lo), Some(hi), Value::Int(_), Value::Int(_)) => {
                            if lo > hi {
                                return fail("lower bound must be less than or equal to upper bound");
                            }
                            let span = (hi as i128 - lo as i128 + 1) as f64;
                            Some(Value::Int(lo + (random_f64() * span).floor() as i64))
                        }
                        _ => {
                            let (lo, hi) = (num(&args[0])?, num(&args[1])?);
                            if lo > hi {
                                return fail("lower bound must be less than or equal to upper bound");
                            }
                            Some(Value::Float(lo + random_f64() * (hi - lo)))
                        }
                    }
                }
                _ => None,
            };
        }
        "SETSEED" => {
            let s = args.first().and_then(num)?;
            if !(-1.0..=1.0).contains(&s) {
                return fail(format!("setseed parameter {} is out of allowed range [-1,1]", s));
            }
            SEED.with(|c| c.set(Some(((s + 1.0) * 1e15) as u64 | 1)));
            return Some(Value::text(""));
        }
        "PG_SLEEP" => {
            let secs = args.first().and_then(num).unwrap_or(0.0);
            if secs > 0.0 {
                #[cfg(not(target_arch = "wasm32"))]
                std::thread::sleep(std::time::Duration::from_secs_f64(secs.min(3600.0)));
            }
            return Some(Value::text(""));
        }
        _ => {}
    }
    if let Some(r) = json_fn(name, args) {
        return r;
    }
    if args.iter().any(|a| a.is_null()) && name != "ARRAY_POSITION" && name != "ARRAY_POSITIONS" && name != "ARRAY_FILL" {
        return Some(Value::Null);
    }
    let a0 = args.first();
    let f0 = || a0.and_then(num);
    let trig = |f: fn(f64) -> f64| -> Option<Value> {
        let x = f0()?;
        let r = f(x);
        if r.is_nan() && !x.is_nan() {
            return fail("input is out of range");
        }
        Some(Value::Float(clean(r)))
    };
    let deg = |f: fn(f64) -> f64| -> Option<Value> { Some(Value::Float(clean(f(f0()?.to_radians())))) };
    let adeg = |f: fn(f64) -> f64| -> Option<Value> {
        let x = f0()?;
        let r = f(x);
        if r.is_nan() {
            return fail("input is out of range");
        }
        Some(Value::Float(clean(r.to_degrees())))
    };
    match name {
        "SIN" => trig(f64::sin),
        "COS" => trig(f64::cos),
        "TAN" => trig(f64::tan),
        "COT" => trig(|x| 1.0 / x.tan()),
        "ASIN" => trig(f64::asin),
        "ACOS" => trig(f64::acos),
        "ATAN" => trig(f64::atan),
        "SINH" => trig(f64::sinh),
        "COSH" => trig(f64::cosh),
        "TANH" => trig(f64::tanh),
        "ASINH" => trig(f64::asinh),
        "ACOSH" => trig(f64::acosh),
        "ATANH" => trig(f64::atanh),
        "SIND" => deg(f64::sin),
        "COSD" => deg(f64::cos),
        "TAND" => deg(f64::tan),
        "COTD" => deg(|x| 1.0 / x.tan()),
        "ASIND" => adeg(f64::asin),
        "ACOSD" => adeg(f64::acos),
        "ATAND" => adeg(f64::atan),
        "ATAN2" | "ATAN2D" => {
            let (y, x) = (f0()?, args.get(1).and_then(num)?);
            let r = y.atan2(x);
            Some(Value::Float(clean(if name == "ATAN2D" { r.to_degrees() } else { r })))
        }
        "DIV" => {
            let (a, b) = (args.first()?, args.get(1)?);
            if let (Value::Int(x), Value::Int(y)) = (a, b) {
                if *y == 0 {
                    super::errors::division_by_zero();
                    return Some(Value::Null);
                }
                return Some(Value::Int(x.wrapping_div(*y)));
            }
            let (x, y) = (num(a)?, num(b)?);
            if y == 0.0 {
                super::errors::division_by_zero();
                return Some(Value::Null);
            }
            let q = (x / y).trunc();
            Some(if q.abs() < 9.0e15 { Value::Int(q as i64) } else { Value::Float(q) })
        }
        "GCD" | "LCM" => {
            let (a, b) = (int(args.first()?)? as i128, int(args.get(1)?)? as i128);
            let g = gcd(a, b);
            let r = if name == "GCD" { g } else if g == 0 { 0 } else { (a / g * b).abs() };
            match i64::try_from(r) {
                Ok(v) => Some(Value::Int(v)),
                Err(_) => fail("bigint out of range"),
            }
        }
        "FACTORIAL" => {
            let n = int(a0?)?;
            if n < 0 {
                return fail("factorial of a negative number is undefined");
            }
            if n > 1000 {
                return fail("value overflows numeric format");
            }
            let s = factorial(n as u64);
            Some(s.parse::<i64>().map(Value::Int).unwrap_or_else(|_| Value::text(s)))
        }
        "WIDTH_BUCKET" => {
            let op = f0()?;
            if args.len() == 2 {
                // width_bucket(op, mảng ngưỡng đã sắp)
                let th = as_array(&args[1])?;
                let n = th.iter().filter(|t| t.as_f64().is_some_and(|x| x <= op)).count();
                return Some(Value::Int(n as i64));
            }
            let (lo, hi, cnt) = (args.get(1).and_then(num)?, args.get(2).and_then(num)?, int(args.get(3)?)?);
            if cnt <= 0 {
                return fail("count must be greater than zero");
            }
            if lo == hi {
                return fail("lower bound cannot equal upper bound");
            }
            let b = if lo < hi {
                if op < lo { 0 } else if op >= hi { cnt + 1 } else { ((op - lo) / (hi - lo) * cnt as f64).floor() as i64 + 1 }
            } else if op > lo {
                0
            } else if op <= hi {
                cnt + 1
            } else {
                ((lo - op) / (lo - hi) * cnt as f64).floor() as i64 + 1
            };
            Some(Value::Int(b))
        }
        "SCALE" | "MIN_SCALE" | "TRIM_SCALE" => {
            let t = decimal_text(a0?)?;
            let frac = t.split_once('.').map_or("", |(_, f)| f);
            match name {
                "SCALE" => Some(Value::Int(frac.len() as i64)),
                "MIN_SCALE" => Some(Value::Int(frac.trim_end_matches('0').len() as i64)),
                _ => {
                    let trimmed = if t.contains('.') { t.trim_end_matches('0').trim_end_matches('.').to_string() } else { t };
                    Some(numeric_value(&trimmed))
                }
            }
        }
        "BIT_LENGTH" => Some(Value::Int(a0?.as_str().len() as i64 * 8)),
        "DIGEST" | "HMAC" => {
            // pgcrypto; kết quả bytea dạng hex như sha256() của engine
            let data = super::executor::raw_bytes_pub(a0?);
            let (key, algo) = if name == "HMAC" { (Some(super::executor::raw_bytes_pub(args.get(1)?)), args.get(2)?.as_str()) } else { (None, args.get(1)?.as_str()) };
            match crypto_digest(&algo.to_ascii_lowercase(), &data, key.as_deref()) {
                Some(d) => Some(Value::text(hex::encode(d))),
                None => fail(format!("Cannot use \"{}\": No such hash algorithm", algo)),
            }
        }
        "GEN_SALT" => {
            let kind = a0?.as_str().to_ascii_lowercase();
            let rounds = args.get(1).and_then(int);
            match gen_salt(&kind, rounds) {
                Ok(s) => Some(Value::text(s)),
                Err(e) => fail(e),
            }
        }
        "CRYPT" => match crypt(&a0?.as_str(), &args.get(1)?.as_str()) {
            Ok(s) => Some(Value::text(s)),
            Err(e) => fail(e),
        },
        "UNACCENT" => {
            // như từ điển unaccent mặc định: bỏ dấu (tách NFD, bỏ dấu kết hợp) + các chữ riêng (đ, ø, ß...)
            use unicode_normalization::UnicodeNormalization;
            let s = args.last()?.as_str();
            let out: String = s
                .nfd()
                .filter(|c| !unicode_normalization::char::is_combining_mark(*c))
                .flat_map(|c| match c {
                    'đ' => "d".chars().collect::<Vec<_>>(),
                    'Đ' => "D".chars().collect(),
                    'ø' => "o".chars().collect(),
                    'Ø' => "O".chars().collect(),
                    'ł' => "l".chars().collect(),
                    'Ł' => "L".chars().collect(),
                    'ß' => "ss".chars().collect(),
                    'æ' => "ae".chars().collect(),
                    'Æ' => "AE".chars().collect(),
                    'œ' => "oe".chars().collect(),
                    'Œ' => "OE".chars().collect(),
                    c => vec![c],
                })
                .collect();
            Some(Value::text(out.nfc().collect::<String>()))
        }
        "CONVERT_FROM" => {
            let bytes = super::executor::raw_bytes_pub(a0?);
            match String::from_utf8(bytes) {
                Ok(s) => Some(Value::text(s)),
                Err(_) => fail("invalid byte sequence for encoding \"UTF8\""),
            }
        }
        "CONVERT_TO" => Some(Value::text(format!("\\x{}", hex::encode(a0?.as_str().as_bytes())))),
        "CASEFOLD" => Some(Value::text(a0?.as_str().to_lowercase())),
        "OVERLAY" => {
            // overlay(s, r, start [, count]): thay `count` ký tự từ `start` (mặc định count = length(r))
            let src: Vec<char> = a0?.as_str().chars().collect();
            let rep = args.get(1)?.as_str();
            let start = int(args.get(2)?)?;
            let count = match args.get(3) {
                Some(c) => int(c)?,
                None => rep.chars().count() as i64,
            };
            if start < 1 || count < 0 {
                return fail("negative substring length not allowed");
            }
            let a = ((start - 1) as usize).min(src.len());
            let b = (a + count as usize).min(src.len());
            let mut out: String = src[..a].iter().collect();
            out.push_str(&rep);
            out.extend(&src[b..]);
            Some(Value::text(out))
        }
        "NORMALIZE" => {
            use unicode_normalization::UnicodeNormalization;
            let s = a0?.as_str();
            let form = args.get(1).map(|f| f.as_str().trim().to_ascii_uppercase()).unwrap_or_else(|| "NFC".into());
            Some(Value::text(match form.as_str() {
                "NFC" => s.nfc().collect::<String>(),
                "NFD" => s.nfd().collect(),
                "NFKC" => s.nfkc().collect(),
                "NFKD" => s.nfkd().collect(),
                _ => return fail(format!("invalid normalization form: {}", form)),
            }))
        }
        "QUOTE_IDENT" => Some(Value::text(super::strfn::quote_ident(&a0?.as_str()))),
        "QUOTE_LITERAL" => Some(Value::text(super::strfn::quote_literal_text(&a0?.as_str()))),
        "STARTS_WITH" => Some(Value::Bool(a0?.as_str().starts_with(&args.get(1)?.as_str()))),
        "TO_HEX" => {
            let n = int(a0?)?;
            Some(Value::text(match i32::try_from(n) {
                Ok(v) => format!("{:x}", v as u32),
                Err(_) => format!("{:x}", n as u64),
            }))
        }
        "TO_CHAR" => {
            // chỉ dạng số; ngày giờ để datetime.rs xử lý
            let v = a0?;
            let fmt = args.get(1)?.as_str();
            let x = match v {
                Value::Int(i) => *i as f64,
                Value::Float(f) => *f,
                _ => return None,
            };
            Some(Value::text(to_char_number(x, &fmt)))
        }
        "TO_NUMBER" => to_number(&a0?.as_str(), &args.get(1).map(|f| f.as_str()).unwrap_or_default()),
        "PG_SIZE_PRETTY" => Some(Value::text(size_pretty(f0()?))),
        "PG_COLUMN_SIZE" => Some(Value::Int(match a0? {
            Value::Int(i) if i32::try_from(*i).is_ok() => 4,
            Value::Int(_) | Value::Float(_) => 8,
            Value::Bool(_) => 1,
            v => v.as_str().len() as i64 + 1,
        })),
        "CARDINALITY" => Some(Value::Int(as_array(a0?).map_or(0, |a| a.iter().map(leaf_count).sum::<usize>()) as i64)),
        "ARRAY_NDIMS" => {
            let a = as_array(a0?)?;
            Some(if a.is_empty() { Value::Null } else { Value::Int(depth(&serde_json::Value::Array(a)) as i64) })
        }
        "ARRAY_POSITION" | "ARRAY_POSITIONS" => {
            if a0?.is_null() {
                return Some(Value::Null);
            }
            let a = as_array(a0?)?;
            let target = args.get(1)?;
            let start = args.get(2).and_then(int).unwrap_or(1).max(1) as usize;
            let mut hits = a.iter().enumerate().skip(start - 1).filter(|(_, it)| json_matches(it, target)).map(|(i, _)| i as i64 + 1);
            if name == "ARRAY_POSITION" {
                Some(hits.next().map_or(Value::Null, Value::Int))
            } else {
                Some(array_value(hits.map(serde_json::Value::from).collect()))
            }
        }
        "ARRAY_FILL" => {
            let fill = json_of(args.first()?);
            let dims = as_array(args.get(1)?)?;
            if dims.iter().any(|d| d.is_null()) {
                return fail("dimension values cannot be null");
            }
            let dims: Vec<usize> = dims.iter().map(|d| d.as_i64().unwrap_or(0).max(0) as usize).collect();
            fn build(fill: &serde_json::Value, dims: &[usize]) -> serde_json::Value {
                match dims.split_first() {
                    None => fill.clone(),
                    Some((n, rest)) => serde_json::Value::Array((0..*n).map(|_| build(fill, rest)).collect()),
                }
            }
            if dims.contains(&0) {
                return Some(array_value(vec![]));
            }
            Some(Value::text(build(&fill, &dims).to_string()))
        }
        "TRIM_ARRAY" => {
            let mut a = as_array(a0?)?;
            let n = int(args.get(1)?)?;
            if n < 0 || n as usize > a.len() {
                return fail(format!("number of elements to trim must be between 0 and {}", a.len()));
            }
            a.truncate(a.len() - n as usize);
            Some(array_value(a))
        }
        "ARRAY_REVERSE" => {
            let mut a = as_array(a0?)?;
            a.reverse();
            Some(array_value(a))
        }
        "ARRAY_SORT" => {
            let mut a = as_array(a0?)?;
            let desc = args.get(1).and_then(|v| v.as_bool()).unwrap_or(false);
            let nulls_first = args.get(2).and_then(|v| v.as_bool()).unwrap_or(desc);
            a.sort_by(|x, y| {
                let o = match (x.is_null(), y.is_null()) {
                    (true, true) => std::cmp::Ordering::Equal,
                    (true, false) => if nulls_first { std::cmp::Ordering::Less } else { std::cmp::Ordering::Greater },
                    (false, true) => if nulls_first { std::cmp::Ordering::Greater } else { std::cmp::Ordering::Less },
                    _ => {
                        let o = json_cmp(x, y);
                        if desc { o.reverse() } else { o }
                    }
                };
                o
            });
            Some(array_value(a))
        }
        _ => None,
    }
}

/// Giá trị jsonb của một đối số (chuỗi JSON, hoặc số/bool) — `None` nếu không phải JSON hợp lệ.
fn jsonb_arg(v: &Value) -> Option<serde_json::Value> {
    match v {
        Value::Null => None,
        Value::Text(t) => serde_json::from_str(t.trim()).ok(),
        other => Some(json_of(other)),
    }
}

/// Đường dẫn `text[]` (`'{a,0}'`) → các phần tử.
fn text_path(v: &Value) -> Vec<String> {
    match as_array(v) {
        Some(a) => a.iter().map(|x| x.as_str().map(str::to_string).unwrap_or_else(|| x.to_string())).collect(),
        None => vec![v.as_str()],
    }
}

fn jsonb_out(j: &serde_json::Value) -> Value {
    Value::text(j.to_string())
}

/// Biến của jsonpath (`vars` jsonb) — mặc định object rỗng.
fn path_vars(v: Option<&Value>) -> Result<serde_json::Value, String> {
    match v {
        None | Some(Value::Null) => Ok(serde_json::json!({})),
        Some(x) => match jsonb_arg(x) {
            Some(o @ serde_json::Value::Object(_)) => Ok(o),
            _ => Err("\"vars\" argument is not an object".into()),
        },
    }
}

/// Hàm JSON có xử lý NULL riêng. `None` = không phải hàm JSON của module.
fn json_fn(name: &str, args: &[Value]) -> Option<Option<Value>> {
    let r = match name {
        "JSONB_SET_LAX" => {
            // jsonb_set_lax(target, path, new, create_if_missing = true, null_value_treatment = 'use_json_null')
            if args.len() < 3 || args[0].is_null() || args[1].is_null() {
                return Some(Some(Value::Null));
            }
            let treatment = args.get(4).map(|v| v.as_str().to_ascii_lowercase()).unwrap_or_else(|| "use_json_null".into());
            let create = args.get(3).and_then(|v| v.as_bool()).unwrap_or(true);
            let mut target = jsonb_arg(&args[0])?;
            let path = text_path(&args[1]);
            if args[2].is_null() {
                match treatment.as_str() {
                    "raise_exception" => return Some(fail("JSON value must not be null")),
                    "return_target" => return Some(Some(jsonb_out(&target))),
                    "delete_key" => {
                        delete_path(&mut target, &path);
                        return Some(Some(jsonb_out(&target)));
                    }
                    "use_json_null" => {}
                    _ => return Some(fail("null_value_treatment must be \"delete_key\", \"return_target\", \"use_json_null\", or \"raise_exception\"")),
                }
            }
            let newv = if args[2].is_null() { serde_json::Value::Null } else { jsonb_arg(&args[2])? };
            set_path(&mut target, &path, newv, create, false);
            Some(jsonb_out(&target))
        }
        "JSONB_INSERT" => {
            if args.len() < 3 || args.iter().take(3).any(|a| a.is_null()) {
                return Some(Some(Value::Null));
            }
            let mut target = jsonb_arg(&args[0])?;
            let path = text_path(&args[1]);
            let newv = jsonb_arg(&args[2])?;
            let after = args.get(3).and_then(|v| v.as_bool()).unwrap_or(false);
            if let Err(e) = insert_path(&mut target, &path, newv, after) {
                return Some(fail(e));
            }
            Some(jsonb_out(&target))
        }
        "JSONB_PATH_EXISTS" | "JSONB_PATH_MATCH" | "JSONB_PATH_QUERY_ARRAY" | "JSONB_PATH_QUERY_FIRST" | "PG_ATAT" => {
            if args.len() < 2 || args[0].is_null() || args[1].is_null() {
                return Some(Some(Value::Null));
            }
            let silent = args.get(3).and_then(|v| v.as_bool()).unwrap_or(false) || name == "PG_ATAT";
            let target = jsonb_arg(&args[0])?;
            let path = match super::jsonpath::compiled(&args[1].as_str()) {
                Ok(p) => p,
                Err(e) => return Some(fail(e)),
            };
            let vars = match path_vars(args.get(2)) {
                Ok(v) => v,
                Err(e) => return Some(fail(e)),
            };
            let res = match name {
                "JSONB_PATH_MATCH" | "PG_ATAT" => path.matches(&target, &vars).map(|b| b.map_or(Value::Null, Value::Bool)),
                _ => path.query(&target, &vars).map(|v| match name {
                    "JSONB_PATH_EXISTS" => Value::Bool(!v.is_empty()),
                    "JSONB_PATH_QUERY_ARRAY" => jsonb_out(&serde_json::Value::Array(v)),
                    _ => v.first().map_or(Value::Null, jsonb_out),
                }),
            };
            match res {
                Ok(v) => Some(v),
                Err(_) if silent => Some(Value::Null),
                Err(e) => return Some(fail(e)),
            }
        }
        "PG_IS_JSON" => {
            // pg_is_json(x, kind, unique_keys): `x IS JSON [VALUE|SCALAR|ARRAY|OBJECT] [WITH UNIQUE KEYS]`
            let v = args.first()?;
            if v.is_null() {
                return Some(Some(Value::Null));
            }
            let kind = args.get(1).map(|k| k.as_str().to_ascii_uppercase()).unwrap_or_else(|| "VALUE".into());
            let unique = args.get(2).and_then(|u| u.as_bool()).unwrap_or(false);
            let text = v.as_str();
            let parsed: Option<serde_json::Value> = match v {
                Value::Text(_) => serde_json::from_str(text.trim()).ok(),
                other => Some(json_of(other)),
            };
            let ok = match parsed {
                None => false,
                Some(j) => {
                    let kind_ok = match kind.as_str() {
                        "OBJECT" => j.is_object(),
                        "ARRAY" => j.is_array(),
                        "SCALAR" => !j.is_object() && !j.is_array(),
                        _ => true,
                    };
                    kind_ok && (!unique || !has_duplicate_keys(&text))
                }
            };
            Some(Value::Bool(ok))
        }
        _ => return None,
    };
    Some(r)
}

/// Khoá trùng trong một object JSON (serde gộp khoá trùng nên kiểm trên văn bản gốc).
fn has_duplicate_keys(text: &str) -> bool {
    struct Dup(bool);
    impl<'de> serde::de::Visitor<'de> for Dup {
        type Value = bool;
        fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
            f.write_str("json")
        }
        fn visit_map<A: serde::de::MapAccess<'de>>(self, mut m: A) -> Result<bool, A::Error> {
            let mut seen = std::collections::HashSet::new();
            let mut dup = self.0;
            while let Some(k) = m.next_key::<String>()? {
                dup |= !seen.insert(k);
                dup |= m.next_value_seed(DupSeed)?;
            }
            Ok(dup)
        }
        fn visit_seq<A: serde::de::SeqAccess<'de>>(self, mut s: A) -> Result<bool, A::Error> {
            let mut dup = self.0;
            while let Some(d) = s.next_element_seed(DupSeed)? {
                dup |= d;
            }
            Ok(dup)
        }
        fn visit_bool<E>(self, _: bool) -> Result<bool, E> {
            Ok(false)
        }
        fn visit_i64<E>(self, _: i64) -> Result<bool, E> {
            Ok(false)
        }
        fn visit_u64<E>(self, _: u64) -> Result<bool, E> {
            Ok(false)
        }
        fn visit_f64<E>(self, _: f64) -> Result<bool, E> {
            Ok(false)
        }
        fn visit_str<E>(self, _: &str) -> Result<bool, E> {
            Ok(false)
        }
        fn visit_unit<E>(self) -> Result<bool, E> {
            Ok(false)
        }
    }
    struct DupSeed;
    impl<'de> serde::de::DeserializeSeed<'de> for DupSeed {
        type Value = bool;
        fn deserialize<D: serde::Deserializer<'de>>(self, d: D) -> Result<bool, D::Error> {
            d.deserialize_any(Dup(false))
        }
    }
    let mut de = serde_json::Deserializer::from_str(text.trim());
    serde::de::DeserializeSeed::deserialize(DupSeed, &mut de).unwrap_or(false)
}

/// Chỉ số mảng kiểu PG (âm = đếm từ cuối). `None` nếu không phải số.
fn array_index(seg: &str, len: usize) -> Option<i64> {
    let i: i64 = seg.trim().parse().ok()?;
    Some(if i < 0 { len as i64 + i } else { i })
}

fn set_path(target: &mut serde_json::Value, path: &[String], newv: serde_json::Value, create: bool, _insert: bool) {
    let Some((head, rest)) = path.split_first() else {
        *target = newv;
        return;
    };
    match target {
        serde_json::Value::Object(m) => {
            if rest.is_empty() {
                if m.contains_key(head) || create {
                    m.insert(head.clone(), newv);
                }
            } else if let Some(child) = m.get_mut(head) {
                set_path(child, rest, newv, create, false);
            }
        }
        serde_json::Value::Array(a) => {
            let Some(i) = array_index(head, a.len()) else { return };
            if rest.is_empty() {
                if i >= 0 && (i as usize) < a.len() {
                    a[i as usize] = newv;
                } else if create {
                    if i < 0 { a.insert(0, newv) } else { a.push(newv) }
                }
            } else if i >= 0 && (i as usize) < a.len() {
                set_path(&mut a[i as usize], rest, newv, create, false);
            }
        }
        _ => {}
    }
}

fn delete_path(target: &mut serde_json::Value, path: &[String]) {
    let Some((head, rest)) = path.split_first() else { return };
    match target {
        serde_json::Value::Object(m) => {
            if rest.is_empty() {
                m.shift_remove(head);
            } else if let Some(c) = m.get_mut(head) {
                delete_path(c, rest);
            }
        }
        serde_json::Value::Array(a) => {
            if let Some(i) = array_index(head, a.len()).filter(|i| *i >= 0 && (*i as usize) < a.len()) {
                if rest.is_empty() {
                    a.remove(i as usize);
                } else {
                    delete_path(&mut a[i as usize], rest);
                }
            }
        }
        _ => {}
    }
}

/// `jsonb_insert`: chèn vào mảng trước (hoặc sau) vị trí; với object chỉ thêm khoá mới.
fn insert_path(target: &mut serde_json::Value, path: &[String], newv: serde_json::Value, after: bool) -> Result<(), String> {
    let Some((head, rest)) = path.split_first() else { return Ok(()) };
    match target {
        serde_json::Value::Object(m) => {
            if rest.is_empty() {
                if m.contains_key(head) {
                    return Err("cannot replace existing key".into());
                }
                m.insert(head.clone(), newv);
                Ok(())
            } else if let Some(c) = m.get_mut(head) {
                insert_path(c, rest, newv, after)
            } else {
                Ok(())
            }
        }
        serde_json::Value::Array(a) => {
            let Some(i) = array_index(head, a.len()) else {
                return Err(format!("path element at position {} is not an integer: \"{}\"", path.len() - rest.len(), head));
            };
            if rest.is_empty() {
                let pos = if i < 0 { 0 } else if i as usize >= a.len() { a.len() } else { i as usize + usize::from(after) };
                a.insert(pos.min(a.len()), newv);
                Ok(())
            } else if i >= 0 && (i as usize) < a.len() {
                insert_path(&mut a[i as usize], rest, newv, after)
            } else {
                Ok(())
            }
        }
        _ => Ok(()),
    }
}

/// SQL/JSON `JSON_VALUE` / `JSON_QUERY` / `JSON_EXISTS` / `json_object(...)`: cần đối số dạng thô (mệnh đề
/// `PASSING`, `RETURNING`, `ON ERROR`, cú pháp `'k': v` / `'k' VALUE v`). `eval` tính một biểu thức.
pub(crate) fn sql_json(name: &str, raw: &[String], eval: &dyn Fn(&str) -> Value) -> Option<Value> {
    use super::executor::find_top_level_keyword as kw;
    if name == "JSON_OBJECT" {
        return json_object(raw, eval);
    }
    if raw.len() < 2 {
        return None;
    }
    let ctx = eval(&raw[0]);
    // tách mệnh đề sau biểu thức path
    let tail_src = raw[1..].join(", ");
    let clause_kws = ["PASSING", "RETURNING", "WITH", "WITHOUT", "KEEP", "OMIT", "NULL", "ERROR", "EMPTY", "DEFAULT", "TRUE", "FALSE", "UNKNOWN"];
    let cut = clause_kws.iter().filter_map(|k| kw(&tail_src, k)).min().unwrap_or(tail_src.len());
    let path_expr = tail_src[..cut].trim().trim_end_matches(',').trim();
    let clauses = &tail_src[cut..];
    let cu = clauses.to_ascii_uppercase();
    let path_src = eval(path_expr);
    if ctx.is_null() || path_src.is_null() {
        return Some(Value::Null);
    }
    // PASSING v AS name, ...
    let mut vars = serde_json::Map::new();
    if let Some(p) = kw(clauses, "PASSING") {
        let after = &clauses[p + 7..];
        let end = ["RETURNING", "WITH", "WITHOUT", "KEEP", "OMIT", "NULL", "ERROR", "EMPTY", "DEFAULT", "TRUE", "FALSE", "UNKNOWN"].iter().filter_map(|k| kw(after, k)).min().unwrap_or(after.len());
        for item in super::executor::split_function_args(&after[..end]) {
            if let Some(a) = kw(&item, "AS") {
                let v = eval(item[..a].trim());
                vars.insert(item[a + 2..].trim().trim_matches('"').to_string(), jsonb_arg(&v).unwrap_or_else(|| json_of(&v)));
            }
        }
    }
    let returning = kw(clauses, "RETURNING").map(|p| {
        let r = clauses[p + 9..].trim_start();
        let end = ["WITH", "WITHOUT", "KEEP", "OMIT", "NULL", "ERROR", "EMPTY", "DEFAULT", "TRUE", "FALSE", "UNKNOWN", "FORMAT"].iter().filter_map(|k| kw(r, k)).min().unwrap_or(r.len());
        r[..end].trim().to_string()
    });
    let error_mode = if cu.contains("ERROR ON ERROR") { "error" } else { "null" };
    let target = match jsonb_arg(&ctx) {
        Some(t) => t,
        None => return fail(format!("invalid input syntax for type json")),
    };
    let path = match super::jsonpath::compiled(&path_src.as_str()) {
        Ok(p) => p,
        Err(e) => return fail(e),
    };
    let on_err = |e: String| -> Option<Value> {
        if name == "JSON_EXISTS" {
            if cu.contains("TRUE ON ERROR") {
                return Some(Value::Bool(true));
            }
            if cu.contains("UNKNOWN ON ERROR") {
                return Some(Value::Null);
            }
            if cu.contains("ERROR ON ERROR") {
                return fail(e);
            }
            return Some(Value::Bool(false));
        }
        if error_mode == "error" { fail(e) } else { Some(Value::Null) }
    };
    let items = match path.query(&target, &serde_json::Value::Object(vars)) {
        Ok(v) => v,
        Err(e) => return on_err(e),
    };
    let cast = |v: Value| -> Option<Value> {
        match &returning {
            Some(t) if !t.is_empty() => {
                let lit = match &v {
                    Value::Null => return Some(Value::Null),
                    other => format!("'{}'::{}", other.as_str().replace('\'', "''"), t),
                };
                Some(eval(&lit))
            }
            _ => Some(v),
        }
    };
    match name {
        "JSON_EXISTS" => Some(Value::Bool(!items.is_empty())),
        "JSON_VALUE" => match items.as_slice() {
            [] => Some(Value::Null),
            [one] => match one {
                serde_json::Value::Null => Some(Value::Null),
                serde_json::Value::String(st) => cast(Value::text(st)),
                serde_json::Value::Object(_) | serde_json::Value::Array(_) => on_err("JSON path expression in JSON_VALUE should return single scalar item".into()),
                other => cast(Value::text(other.to_string())),
            },
            _ => on_err("JSON path expression in JSON_VALUE should return single scalar item".into()),
        },
        _ => {
            let wrap_uncond = cu.contains("WITH WRAPPER") || cu.contains("WITH ARRAY WRAPPER") || cu.contains("WITH UNCONDITIONAL");
            let wrap_cond = cu.contains("WITH CONDITIONAL");
            let out = if wrap_uncond || (wrap_cond && (items.len() != 1 || !(items[0].is_array() || items[0].is_object()))) {
                Some(serde_json::Value::Array(items))
            } else {
                match items.len() {
                    0 => None,
                    1 => Some(items.into_iter().next().unwrap()),
                    _ => return on_err("JSON path expression in JSON_QUERY should return single item without wrapper".into()),
                }
            };
            match out {
                None => Some(Value::Null),
                Some(serde_json::Value::String(st)) if cu.contains("OMIT QUOTES") => cast(Value::text(st)),
                Some(j) => cast(jsonb_out(&j)),
            }
        }
    }
}

/// `json_object(text[])`, `json_object(keys[], values[])` và SQL/JSON `json_object('k': v, 'k2' VALUE v2 ...)`.
/// Kết quả là kiểu `json` với định dạng của PostgreSQL (`{"a" : 1}`).
fn json_object(raw: &[String], eval: &dyn Fn(&str) -> Value) -> Option<Value> {
    use super::executor::find_top_level_keyword as kw;
    let quote = |s: &str| serde_json::Value::String(s.to_string()).to_string();
    let sql_json_syntax = raw.iter().any(|a| kw(a, "VALUE").is_some() || super::executor::find_top_level_op(a, ":").is_some_and(|p| p > 0 && !a[p..].starts_with("::") && !a[..p].ends_with(':')))
        || raw.is_empty();
    if sql_json_syntax {
        let mut parts = Vec::new();
        for a in raw {
            let up = a.trim().to_ascii_uppercase();
            if up.starts_with("NULL ON NULL") || up.starts_with("ABSENT ON NULL") || up.starts_with("RETURNING") || up.starts_with("WITH") || up.starts_with("WITHOUT") {
                continue;
            }
            let (k, v) = if let Some(p) = kw(a, "VALUE") {
                (&a[..p], &a[p + 5..])
            } else {
                let p = super::executor::find_top_level_op(a, ":")?;
                (&a[..p], &a[p + 1..])
            };
            let key = eval(k.trim());
            if key.is_null() {
                return fail("null value not allowed for object key");
            }
            let val = eval(v.trim());
            let vj = match &val {
                Value::Null => "null".to_string(),
                Value::Text(t) if v.trim().to_ascii_lowercase().contains("::json") => t.to_string(),
                Value::Text(t) => quote(t),
                other => json_of(other).to_string(),
            };
            parts.push(format!("{} : {}", quote(&key.as_str()), vj));
        }
        return Some(Value::text(format!("{{{}}}", parts.join(", "))));
    }
    let args: Vec<Value> = raw.iter().map(|a| eval(a)).collect();
    if args.iter().any(|a| a.is_null()) {
        return Some(Value::Null);
    }
    let flat = |v: &Value| -> Vec<serde_json::Value> {
        let mut out = Vec::new();
        fn walk(j: &serde_json::Value, out: &mut Vec<serde_json::Value>) {
            match j {
                serde_json::Value::Array(a) => a.iter().for_each(|x| walk(x, out)),
                other => out.push(other.clone()),
            }
        }
        if let Some(a) = as_array(v) {
            a.iter().for_each(|x| walk(x, &mut out));
        }
        out
    };
    let text = |j: &serde_json::Value| -> Option<String> {
        match j {
            serde_json::Value::Null => None,
            serde_json::Value::String(s) => Some(s.clone()),
            other => Some(other.to_string()),
        }
    };
    let (keys, vals): (Vec<serde_json::Value>, Vec<serde_json::Value>) = match args.as_slice() {
        [one] => {
            let items = flat(one);
            if items.len() % 2 != 0 {
                return fail("array must have even number of elements");
            }
            (items.iter().step_by(2).cloned().collect(), items.iter().skip(1).step_by(2).cloned().collect())
        }
        [k, v] => {
            let (k, v) = (flat(k), flat(v));
            if k.len() != v.len() {
                return fail("mismatched array dimensions");
            }
            (k, v)
        }
        _ => return None,
    };
    let mut parts = Vec::new();
    for (k, v) in keys.iter().zip(&vals) {
        let Some(k) = text(k) else { return fail("null value not allowed for object key") };
        parts.push(format!("{} : {}", quote(&k), text(v).map_or("null".to_string(), |s| quote(&s))));
    }
    Some(Value::text(format!("{{{}}}", parts.join(", "))))
}

/// Băm theo tên thuật toán của pgcrypto (`md5`, `sha1`, `sha224`, `sha256`, `sha384`, `sha512`); có khoá = HMAC.
fn crypto_digest(algo: &str, data: &[u8], key: Option<&[u8]>) -> Option<Vec<u8>> {
    use hmac::Mac;
    use sha2::Digest;
    macro_rules! go {
        ($h:ty) => {
            match key {
                None => Some(<$h>::digest(data).to_vec()),
                Some(k) => {
                    let mut m = <hmac::Hmac<$h> as Mac>::new_from_slice(k).ok()?;
                    m.update(data);
                    Some(m.finalize().into_bytes().to_vec())
                }
            }
        };
    }
    match algo {
        "md5" => go!(md5::Md5),
        "sha1" => go!(sha1::Sha1),
        "sha224" => go!(sha2::Sha224),
        "sha256" => go!(sha2::Sha256),
        "sha384" => go!(sha2::Sha384),
        "sha512" => go!(sha2::Sha512),
        _ => None,
    }
}

fn random_bytes<const N: usize>() -> [u8; N] {
    let mut out = [0u8; N];
    for chunk in out.chunks_mut(16) {
        let u = uuid::Uuid::new_v4();
        chunk.copy_from_slice(&u.as_bytes()[..chunk.len()]);
    }
    out
}

const CRYPT64: &[u8] = b"./0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";

/// `gen_salt('bf' [, rounds])` (mặc định 6 như pgcrypto) và `gen_salt('md5')`.
fn gen_salt(kind: &str, rounds: Option<i64>) -> Result<String, String> {
    use base64::Engine;
    match kind {
        "bf" => {
            let r = rounds.unwrap_or(6);
            if !(4..=31).contains(&r) {
                return Err("gen_salt: Incorrect number of rounds".into());
            }
            Ok(format!("$2a${:02}${}", r, bcrypt::BASE_64.encode(random_bytes::<16>())))
        }
        "md5" => {
            let b = random_bytes::<8>();
            Ok(format!("$1${}", b.iter().map(|x| CRYPT64[(*x & 63) as usize] as char).collect::<String>()))
        }
        _ => Err(format!("gen_salt: Unknown salt algorithm")),
    }
}

/// `crypt(mật khẩu, salt)`: bcrypt (`$2a$`/`$2b$`/`$2y$`) và md5-crypt (`$1$`). Salt có thể là một hash cũ
/// (so khớp mật khẩu bằng `crypt(pw, hash) = hash`).
fn crypt(pw: &str, salt: &str) -> Result<String, String> {
    use base64::Engine;
    if let Some(rest) = salt.strip_prefix("$2a$").or_else(|| salt.strip_prefix("$2b$")).or_else(|| salt.strip_prefix("$2y$")).or_else(|| salt.strip_prefix("$2x$")) {
        let ver = &salt[..4];
        let cost: u32 = rest.get(..2).and_then(|c| c.parse().ok()).ok_or("invalid salt")?;
        let salt_b64 = rest.get(3..25).ok_or("invalid salt")?;
        let raw = bcrypt::BASE_64.decode(salt_b64).map_err(|_| "invalid salt".to_string())?;
        let salt16: [u8; 16] = raw.get(..16).and_then(|r| r.try_into().ok()).ok_or("invalid salt")?;
        if !(4..=31).contains(&cost) {
            return Err("invalid salt".into());
        }
        let mut pwb: Vec<u8> = pw.as_bytes().iter().copied().take(72).collect();
        if pwb.len() < 72 {
            pwb.push(0);
        }
        let out = bcrypt::bcrypt(cost, salt16, &pwb);
        return Ok(format!("{}{:02}${}{}", ver, cost, salt_b64, bcrypt::BASE_64.encode(&out[..23])));
    }
    if let Some(rest) = salt.strip_prefix("$1$") {
        let s = rest.split('$').next().unwrap_or("");
        let s = &s[..s.len().min(8)];
        return Ok(md5_crypt(pw.as_bytes(), s.as_bytes()));
    }
    Err("invalid salt".into())
}

/// MD5-crypt (`$1$salt$hash`) theo thuật toán của FreeBSD (như pgcrypto).
fn md5_crypt(pw: &[u8], salt: &[u8]) -> String {
    use md5::{Digest, Md5};
    let mut alt = Md5::new();
    alt.update(pw);
    alt.update(salt);
    alt.update(pw);
    let alt = alt.finalize();
    let mut ctx = Md5::new();
    ctx.update(pw);
    ctx.update(b"$1$");
    ctx.update(salt);
    let mut n = pw.len();
    while n > 0 {
        ctx.update(&alt[..n.min(16)]);
        n = n.saturating_sub(16);
    }
    let mut i = pw.len();
    while i > 0 {
        if i & 1 == 1 {
            ctx.update([0u8]);
        } else {
            ctx.update(&pw[..1.min(pw.len())]);
        }
        i >>= 1;
    }
    let mut fin = ctx.finalize();
    for r in 0..1000 {
        let mut c = Md5::new();
        if r & 1 == 1 {
            c.update(pw);
        } else {
            c.update(fin);
        }
        if r % 3 != 0 {
            c.update(salt);
        }
        if r % 7 != 0 {
            c.update(pw);
        }
        if r & 1 == 1 {
            c.update(fin);
        } else {
            c.update(pw);
        }
        fin = c.finalize();
    }
    let mut out = format!("$1${}$", String::from_utf8_lossy(salt));
    let enc = |v: u32, n: usize, out: &mut String| {
        let mut v = v;
        for _ in 0..n {
            out.push(CRYPT64[(v & 0x3f) as usize] as char);
            v >>= 6;
        }
    };
    for (a, b, c) in [(0, 6, 12), (1, 7, 13), (2, 8, 14), (3, 9, 15), (4, 10, 5)] {
        enc(((fin[a] as u32) << 16) | ((fin[b] as u32) << 8) | fin[c] as u32, 4, &mut out);
    }
    enc(fin[11] as u32, 2, &mut out);
    out
}

/// `round(x, d)` kiểu numeric (nửa ra xa 0) trên chuỗi thập phân ngắn nhất của `x`, tránh sai số nhị phân
/// (`round(1.005, 2)` = 1.01 như PostgreSQL, không phải 1.00). `d < 0` làm tròn phần nguyên.
pub(crate) fn round_dec(x: f64, d: i64) -> f64 {
    if !x.is_finite() {
        return x;
    }
    if d < 0 {
        let m = 10f64.powi((-d) as i32);
        return (x / m).round() * m;
    }
    let text = format!("{}", x);
    match super::coltypes::round_decimal_str(&text, d as usize).and_then(|r| r.parse::<f64>().ok()) {
        Some(r) => r,
        None => {
            let m = 10f64.powi(d as i32);
            (x * m).round() / m
        }
    }
}

/// Bỏ sai số dấu phẩy động rất nhỏ quanh 0 (`sin(pi())` vẫn như PG, nhưng `-0` → `0`).
fn clean(x: f64) -> f64 {
    if x == 0.0 { 0.0 } else { x }
}

/// UUID v7 (RFC 9562): 48 bit mili giây Unix + ngẫu nhiên.
fn uuid_v7() -> String {
    let ms = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_millis() as u64).unwrap_or(0);
    let mut b = *uuid::Uuid::new_v4().as_bytes();
    b[..6].copy_from_slice(&ms.to_be_bytes()[2..]);
    b[6] = (b[6] & 0x0f) | 0x70;
    b[8] = (b[8] & 0x3f) | 0x80;
    uuid::Uuid::from_bytes(b).to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn to_char_numbers() {
        assert_eq!(to_char_number(1234.5, "FM9,999.00"), "1,234.50");
        assert_eq!(to_char_number(1234.5, "9,999.00"), " 1,234.50");
        assert_eq!(to_char_number(-12.0, "999"), " -12");
        assert_eq!(to_char_number(12.0, "999"), "  12");
        assert_eq!(to_char_number(5.0, "000"), " 005");
        assert_eq!(to_char_number(1.0, "FM9.99"), "1.");
        assert_eq!(to_char_number(12345.0, "999"), " ###");
    }
    #[test]
    fn factorial_big() {
        assert_eq!(factorial(5), "120");
        assert_eq!(factorial(25), "15511210043330985984000000");
    }
}

/// Hàm gộp bổ sung (tính trên đường GROUP BY; không GROUP BY thì cả bảng là một nhóm). Dạng `WITHIN GROUP`
/// được `oprewrite` đổi thành đối số thường: `percentile_cont(f, khoá, 'asc'|'desc')`, `mode(khoá, dir)`,
/// `pg_hyp_rank(x, khoá, dir)` (và `_dense_rank`, `_percent_rank`, `_cume_dist`).
pub(crate) const EXTRA_AGGS: &[&str] = &[
    "BIT_AND", "BIT_OR", "BIT_XOR", "STDDEV", "STDDEV_SAMP", "STDDEV_POP", "VARIANCE", "VAR_SAMP", "VAR_POP", "CORR", "COVAR_POP", "COVAR_SAMP",
    "REGR_SLOPE", "REGR_INTERCEPT", "REGR_COUNT", "REGR_R2", "REGR_AVGX", "REGR_AVGY", "REGR_SXX", "REGR_SYY", "REGR_SXY", "ANY_VALUE",
    "PERCENTILE_CONT", "PERCENTILE_DISC", "MODE", "PG_HYP_RANK", "PG_HYP_DENSE_RANK", "PG_HYP_PERCENT_RANK", "PG_HYP_CUME_DIST", "XMLAGG",
    "JSON_ARRAYAGG", "JSON_OBJECTAGG",
];

/// Kiểu kết quả (PG) của hàm gộp bổ sung; `None` = theo kiểu đối số.
pub(crate) fn agg_result_type(name: &str) -> Option<&'static str> {
    Some(match name {
        "STDDEV" | "STDDEV_SAMP" | "STDDEV_POP" | "VARIANCE" | "VAR_SAMP" | "VAR_POP" => "numeric",
        "CORR" | "COVAR_POP" | "COVAR_SAMP" | "REGR_SLOPE" | "REGR_INTERCEPT" | "REGR_R2" | "REGR_AVGX" | "REGR_AVGY" | "REGR_SXX" | "REGR_SYY"
        | "REGR_SXY" | "PERCENTILE_CONT" | "PG_HYP_PERCENT_RANK" | "PG_HYP_CUME_DIST" => "double precision",
        "REGR_COUNT" | "PG_HYP_RANK" | "PG_HYP_DENSE_RANK" => "bigint",
        "XMLAGG" => "xml",
        "JSON_ARRAYAGG" | "JSON_OBJECTAGG" => "json",
        _ => return None,
    })
}

/// Kết thúc hàm gộp: `rows` = giá trị đối số của từng dòng trong nhóm (đã qua FILTER, đã sắp nếu có ORDER BY).
pub(crate) fn agg_finalize(name: &str, rows: &[Vec<Value>]) -> Value {
    let col = |i: usize| rows.iter().filter_map(move |r| r.get(i)).filter(|v| !v.is_null());
    let nums = |i: usize| col(i).filter_map(num).collect::<Vec<f64>>();
    let numeric = |f: f64| if f.is_finite() { Value::Float(f) } else { Value::Null };
    match name {
        "BIT_AND" | "BIT_OR" | "BIT_XOR" => {
            let mut it = col(0).filter_map(int);
            let Some(first) = it.next() else { return Value::Null };
            Value::Int(it.fold(first, |a, b| match name {
                "BIT_AND" => a & b,
                "BIT_OR" => a | b,
                _ => a ^ b,
            }))
        }
        "STDDEV" | "STDDEV_SAMP" | "STDDEV_POP" | "VARIANCE" | "VAR_SAMP" | "VAR_POP" => {
            let v = nums(0);
            let n = v.len() as f64;
            let pop = name.ends_with("_POP");
            if v.is_empty() || (!pop && v.len() < 2) {
                return Value::Null;
            }
            let mean = v.iter().sum::<f64>() / n;
            let ss: f64 = v.iter().map(|x| (x - mean) * (x - mean)).sum();
            let var = ss / if pop { n } else { n - 1.0 };
            numeric(if name.starts_with("STDDEV") { var.sqrt() } else { var })
        }
        "CORR" | "COVAR_POP" | "COVAR_SAMP" | "REGR_SLOPE" | "REGR_INTERCEPT" | "REGR_COUNT" | "REGR_R2" | "REGR_AVGX" | "REGR_AVGY" | "REGR_SXX"
        | "REGR_SYY" | "REGR_SXY" => {
            // (Y, X): chỉ các cặp không NULL
            let pairs: Vec<(f64, f64)> = rows.iter().filter_map(|r| Some((num(r.first()?)?, num(r.get(1)?)?))).collect();
            let n = pairs.len() as f64;
            if name == "REGR_COUNT" {
                return Value::Int(pairs.len() as i64);
            }
            if pairs.is_empty() {
                return Value::Null;
            }
            let (sy, sx) = pairs.iter().fold((0.0, 0.0), |a, p| (a.0 + p.0, a.1 + p.1));
            let (my, mx) = (sy / n, sx / n);
            let sxx: f64 = pairs.iter().map(|p| (p.1 - mx).powi(2)).sum();
            let syy: f64 = pairs.iter().map(|p| (p.0 - my).powi(2)).sum();
            let sxy: f64 = pairs.iter().map(|p| (p.1 - mx) * (p.0 - my)).sum();
            match name {
                "COVAR_POP" => Value::Float(sxy / n),
                "COVAR_SAMP" => if n < 2.0 { Value::Null } else { Value::Float(sxy / (n - 1.0)) },
                "CORR" => if sxx == 0.0 || syy == 0.0 { Value::Null } else { Value::Float(sxy / (sxx * syy).sqrt()) },
                "REGR_SLOPE" => if sxx == 0.0 { Value::Null } else { Value::Float(sxy / sxx) },
                "REGR_INTERCEPT" => if sxx == 0.0 { Value::Null } else { Value::Float(my - sxy / sxx * mx) },
                "REGR_R2" => {
                    if sxx == 0.0 {
                        Value::Null
                    } else if syy == 0.0 {
                        Value::Float(1.0)
                    } else {
                        Value::Float(sxy * sxy / (sxx * syy))
                    }
                }
                "REGR_AVGX" => Value::Float(mx),
                "REGR_AVGY" => Value::Float(my),
                "REGR_SXX" => Value::Float(sxx),
                "REGR_SYY" => Value::Float(syy),
                _ => Value::Float(sxy),
            }
        }
        "ANY_VALUE" => col(0).next().cloned().unwrap_or(Value::Null),
        "XMLAGG" => {
            let parts: Vec<String> = col(0).map(|v| v.as_str()).collect();
            if parts.is_empty() { Value::Null } else { Value::text(parts.concat()) }
        }
        "JSON_ARRAYAGG" => {
            let items: Vec<String> = col(0).map(|v| json_of(v).to_string()).collect();
            if rows.is_empty() { Value::Null } else { Value::text(format!("[{}]", items.join(", "))) }
        }
        "JSON_OBJECTAGG" => {
            if rows.is_empty() {
                return Value::Null;
            }
            let mut parts = Vec::new();
            for r in rows {
                let (Some(k), Some(v)) = (r.first(), r.get(1)) else { continue };
                if k.is_null() {
                    super::errors::raise("null value not allowed for object key");
                    return Value::Null;
                }
                parts.push(format!("{} : {}", serde_json::Value::String(k.as_str()).to_string(), json_of(v)));
            }
            Value::text(format!("{{ {} }}", parts.join(", ")))
        }
        "PERCENTILE_CONT" | "PERCENTILE_DISC" | "MODE" | "PG_HYP_RANK" | "PG_HYP_DENSE_RANK" | "PG_HYP_PERCENT_RANK" | "PG_HYP_CUME_DIST" => {
            // vị trí của khoá sắp và hướng sắp
            let (key_i, dir_i) = if name == "MODE" { (0, 1) } else { (1, 2) };
            let desc = rows.first().and_then(|r| r.get(dir_i)).is_some_and(|d| d.as_str().eq_ignore_ascii_case("desc"));
            let mut keys: Vec<Value> = col(key_i).cloned().collect();
            keys.sort_by(|a, b| if desc { b.cmp_value(a) } else { a.cmp_value(b) });
            let direct = rows.first().and_then(|r| r.first()).cloned().unwrap_or(Value::Null);
            match name {
                "MODE" => {
                    let mut best: Option<(&Value, usize)> = None;
                    let mut i = 0;
                    while i < keys.len() {
                        let mut j = i;
                        while j < keys.len() && keys[j].is_equal(&keys[i]) {
                            j += 1;
                        }
                        if best.map_or(true, |b| j - i > b.1) {
                            best = Some((&keys[i], j - i));
                        }
                        i = j;
                    }
                    best.map_or(Value::Null, |b| b.0.clone())
                }
                "PERCENTILE_CONT" | "PERCENTILE_DISC" => {
                    if keys.is_empty() || direct.is_null() {
                        return Value::Null;
                    }
                    let one = |f: f64| -> Option<Value> {
                        if !(0.0..=1.0).contains(&f) {
                            super::errors::raise(format!("percentile value {} is not between 0 and 1", f));
                            return None;
                        }
                        let n = keys.len();
                        if name == "PERCENTILE_DISC" {
                            let idx = ((f * n as f64).ceil() as usize).max(1) - 1;
                            return Some(keys[idx.min(n - 1)].clone());
                        }
                        let pos = f * (n - 1) as f64;
                        let (lo, hi) = (pos.floor() as usize, pos.ceil() as usize);
                        let (a, b) = (num(&keys[lo])?, num(&keys[hi])?);
                        Some(Value::Float(clean(a + (b - a) * (pos - lo as f64))))
                    };
                    match as_array(&direct) {
                        Some(fs) => array_value(fs.iter().map(|f| f.as_f64().and_then(one).map_or(serde_json::Value::Null, |v| json_of(&v))).collect()),
                        None => num(&direct).and_then(one).unwrap_or(Value::Null),
                    }
                }
                _ => {
                    // hàng giả định: x được chèn vào nhóm đã sắp
                    let before = |k: &Value| if desc { k.cmp_value(&direct) == std::cmp::Ordering::Greater } else { k.cmp_value(&direct) == std::cmp::Ordering::Less };
                    let n = keys.len();
                    let lt = keys.iter().filter(|k| before(k)).count();
                    match name {
                        "PG_HYP_RANK" => Value::Int(lt as i64 + 1),
                        "PG_HYP_DENSE_RANK" => {
                            let mut d: Vec<&Value> = keys.iter().filter(|k| before(k)).collect();
                            d.dedup_by(|a, b| a.is_equal(b));
                            Value::Int(d.len() as i64 + 1)
                        }
                        "PG_HYP_PERCENT_RANK" => Value::Float(if n == 0 { 0.0 } else { lt as f64 / n as f64 }),
                        _ => {
                            let le = keys.iter().filter(|k| before(k) || k.is_equal(&direct)).count();
                            Value::Float((le + 1) as f64 / (n + 1) as f64)
                        }
                    }
                }
            }
        }
        _ => Value::Null,
    }
}
