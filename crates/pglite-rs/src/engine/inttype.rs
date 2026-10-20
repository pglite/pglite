//! Suy kiểu số nguyên tĩnh của một biểu thức (int2 / int4 / int8) để phép tính theo đúng PostgreSQL:
//! `7 / 2` = 3 (chia nguyên, làm tròn về 0), `2147483647 + 1` → lỗi 22003. `Value` không phân biệt
//! numeric với số nguyên (`2.00` có thể thành `Int(2)`), nên chỉ coi là số nguyên khi CHẮC CHẮN: literal
//! nguyên, cột có kiểu khai báo nguyên (`SchemaOp::ColumnTypes`; bảng cũ không có → hành vi như cũ),
//! ép kiểu `::int`, `count(...)`, và phép + - * / % giữa chúng. Còn lại trả `None` → chia thực như cũ.
use crate::types::Value;
use super::executor::{find_last_top_level_op, find_top_level_keyword, find_top_level_math_op, is_fully_enclosed_in_parens};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum IntKind {
    I2,
    I4,
    I8,
}

impl IntKind {
    pub fn from_type_name(t: &str) -> Option<IntKind> {
        let t = t.trim();
        if t.ends_with(']') {
            return None;
        }
        match t.to_ascii_lowercase().as_str() {
            "int2" | "smallint" | "smallserial" => Some(IntKind::I2),
            "int" | "int4" | "integer" | "serial" => Some(IntKind::I4),
            "int8" | "bigint" | "bigserial" => Some(IntKind::I8),
            _ => None,
        }
    }

    fn range(self) -> (i64, i64, &'static str) {
        match self {
            IntKind::I2 => (i16::MIN as i64, i16::MAX as i64, "smallint out of range"),
            IntKind::I4 => (i32::MIN as i64, i32::MAX as i64, "integer out of range"),
            IntKind::I8 => (i64::MIN, i64::MAX, "bigint out of range"),
        }
    }
}

/// Kiểu nguyên của biểu thức; `col` trả kiểu nguyên của một tham chiếu cột (`n`, `t.n`).
pub fn int_kind(expr: &str, col: &dyn Fn(&str) -> Option<IntKind>) -> Option<IntKind> {
    let mut s = expr.trim();
    while s.starts_with('(') && s.ends_with(')') && is_fully_enclosed_in_parens(s) {
        s = s[1..s.len() - 1].trim();
    }
    if s.is_empty() {
        return None;
    }
    let b = s.as_bytes();
    if b.iter().all(|c| c.is_ascii_digit()) {
        return match s.parse::<i64>() {
            Ok(v) if i32::try_from(v).is_ok() => Some(IntKind::I4),
            Ok(_) => Some(IntKind::I8),
            Err(_) => None,
        };
    }
    if let Some((p, _)) = find_top_level_math_op(s, &['+', '-']).or_else(|| find_top_level_math_op(s, &['*', '/', '%'])) {
        let l = s[..p].trim();
        if !l.is_empty() {
            let (a, c) = (int_kind(l, col)?, int_kind(&s[p + 1..], col)?);
            return Some(a.max(c));
        }
    }
    if let Some(p) = find_last_top_level_op(s, "::") {
        return IntKind::from_type_name(&s[p + 2..]);
    }
    if let Some(rest) = s.strip_prefix('-') {
        return int_kind(rest, col);
    }
    if b[0].is_ascii_alphabetic() || b[0] == b'_' || b[0] == b'"' {
        if s.bytes().all(|c| c.is_ascii_alphanumeric() || c == b'_' || c == b'.' || c == b'"') {
            return col(s);
        }
        if let Some(open) = s.find('(') {
            let name = s[..open].trim();
            if s.ends_with(')') && is_fully_enclosed_in_parens(&s[open..]) {
                if name.eq_ignore_ascii_case("count") {
                    return Some(IntKind::I8);
                }
                if name.eq_ignore_ascii_case("cast") {
                    let inner = &s[open + 1..s.len() - 1];
                    return find_top_level_keyword(inner, "AS").and_then(|a| IntKind::from_type_name(&inner[a + 2..]));
                }
            }
        }
    }
    None
}

/// Kiểu nguyên của cột `name` (có thể kèm tiền tố bảng) trong `table`, theo kiểu khai báo.
pub fn column_int_kind(table: Option<&crate::storage::table::Table>, name: &str) -> Option<IntKind> {
    let t = table?;
    let c = name.rsplit('.').next().unwrap_or(name).trim_matches('"');
    let idx = t.get_column_index(c)?;
    column_kind_at(t, idx)
}

/// Kiểu nguyên của cột thứ `idx`: theo kiểu khai báo; bảng không lưu kiểu khai báo (INT, bảng cũ) thì
/// theo `DataType` (INT/INTEGER/SMALLINT → int4, BIGINT → int8, SERIAL các loại → int8 cho an toàn).
pub fn column_kind_at(t: &crate::storage::table::Table, idx: usize) -> Option<IntKind> {
    if let Some(d) = t.decl_type(idx) {
        return if d.array { None } else { IntKind::from_type_name(&d.base) };
    }
    match t.columns.get(idx)?.data_type {
        crate::types::DataType::Integer => Some(IntKind::I4),
        crate::types::DataType::BigInt | crate::types::DataType::Serial => Some(IntKind::I8),
        _ => None,
    }
}

/// Phép tính số nguyên theo kiểu `kind`: chia làm tròn về 0, tràn số báo 22003.
pub fn int_arith(op: char, a: i64, b: i64, kind: IntKind) -> Value {
    let (mut lo, mut hi, mut msg) = kind.range();
    if a < lo || a > hi || b < lo || b > hi {
        // giá trị cũ lớn hơn kiểu khai báo (DB do bản cũ ghi): tính theo int8 thay vì báo lỗi
        (lo, hi, msg) = IntKind::I8.range();
    }
    let r = match op {
        '+' => a.checked_add(b),
        '-' => a.checked_sub(b),
        '*' => a.checked_mul(b),
        '/' | '%' if b == 0 => {
            super::errors::division_by_zero();
            return Value::Null;
        }
        '/' => a.checked_div(b),
        '%' => Some(a.checked_rem(b).unwrap_or(0)),
        _ => None,
    };
    match r {
        Some(v) if v >= lo && v <= hi => Value::Int(v),
        _ => {
            super::errors::raise(msg);
            Value::Null
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn k(s: &str) -> Option<IntKind> {
        int_kind(s, &|c| if c == "n" { Some(IntKind::I4) } else { None })
    }

    #[test]
    fn kinds() {
        assert_eq!(k("7"), Some(IntKind::I4));
        assert_eq!(k("3000000000"), Some(IntKind::I8));
        assert_eq!(k("7.0"), None);
        assert_eq!(k("(n + 1) * 2"), Some(IntKind::I4));
        assert_eq!(k("n / price"), None);
        assert_eq!(k("-7"), Some(IntKind::I4));
        assert_eq!(k("'7'::bigint"), Some(IntKind::I8));
        assert_eq!(k("count(*)"), Some(IntKind::I8));
        assert_eq!(k("CAST(x AS int)"), Some(IntKind::I4));
        assert_eq!(k("1.5::int"), Some(IntKind::I4));
        assert_eq!(k("n::numeric / 2"), None);
        assert_eq!(k("n::int8 * 2"), Some(IntKind::I8));
    }

    #[test]
    fn arith() {
        assert_eq!(int_arith('/', 7, 2, IntKind::I4), Value::Int(3));
        assert_eq!(int_arith('/', -7, 2, IntKind::I4), Value::Int(-3));
        super::super::errors::clear();
        assert_eq!(int_arith('+', 2147483647, 1, IntKind::I4), Value::Null);
        assert_eq!(super::super::errors::take().as_deref(), Some("integer out of range"));
        assert_eq!(int_arith('+', 2147483647, 1, IntKind::I8), Value::Int(2147483648));
    }
}
