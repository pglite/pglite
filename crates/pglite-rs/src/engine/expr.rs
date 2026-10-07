use crate::types::Value;
use md5::{Digest as Md5Digest, Md5};
use sha2::Sha256;

#[derive(Clone, Debug)]
pub enum CompiledExpr {
    Col(usize),
    JoinedCol(usize),
    Param(usize),
    Literal(Value),
    Null,
    Now,
    CurrentDate,
    CurrentTime,
    GenRandomUuid,

    // Unary
    Not(Box<CompiledExpr>),
    Neg(Box<CompiledExpr>),
    BitNot(Box<CompiledExpr>),
    IsNull(Box<CompiledExpr>),
    IsNotNull(Box<CompiledExpr>),
    IsTrue(Box<CompiledExpr>),
    IsNotTrue(Box<CompiledExpr>),
    IsFalse(Box<CompiledExpr>),
    IsNotFalse(Box<CompiledExpr>),
    Lower(Box<CompiledExpr>),
    Upper(Box<CompiledExpr>),
    Trim(Box<CompiledExpr>),
    Cast {
        expr: Box<CompiledExpr>,
        target_type: String,
    },

    // Binary Arithmetic & Bitwise
    Add(Box<CompiledExpr>, Box<CompiledExpr>),
    Sub(Box<CompiledExpr>, Box<CompiledExpr>),
    Mul(Box<CompiledExpr>, Box<CompiledExpr>),
    Div(Box<CompiledExpr>, Box<CompiledExpr>),
    Mod(Box<CompiledExpr>, Box<CompiledExpr>),
    Pow(Box<CompiledExpr>, Box<CompiledExpr>),
    BitAnd(Box<CompiledExpr>, Box<CompiledExpr>),
    BitOr(Box<CompiledExpr>, Box<CompiledExpr>),
    BitXor(Box<CompiledExpr>, Box<CompiledExpr>),
    Shl(Box<CompiledExpr>, Box<CompiledExpr>),
    Shr(Box<CompiledExpr>, Box<CompiledExpr>),
    Concat(Box<CompiledExpr>, Box<CompiledExpr>),

    // Comparisons
    Eq(Box<CompiledExpr>, Box<CompiledExpr>),
    NotEq(Box<CompiledExpr>, Box<CompiledExpr>),
    Gt(Box<CompiledExpr>, Box<CompiledExpr>),
    Gte(Box<CompiledExpr>, Box<CompiledExpr>),
    Lt(Box<CompiledExpr>, Box<CompiledExpr>),
    Lte(Box<CompiledExpr>, Box<CompiledExpr>),
    IsDistinctFrom(Box<CompiledExpr>, Box<CompiledExpr>),
    IsNotDistinctFrom(Box<CompiledExpr>, Box<CompiledExpr>),

    // String / Pattern Matching
    Like {
        expr: Box<CompiledExpr>,
        pattern: Box<CompiledExpr>,
        case_insensitive: bool,
        negated: bool,
    },
    Regex {
        expr: Box<CompiledExpr>,
        pattern: String,
        case_insensitive: bool,
        negated: bool,
    },

    // Multi-way Logic
    And(Vec<CompiledExpr>),
    Or(Vec<CompiledExpr>),
    Between {
        expr: Box<CompiledExpr>,
        low: Box<CompiledExpr>,
        high: Box<CompiledExpr>,
        negated: bool,
    },
    In {
        expr: Box<CompiledExpr>,
        list: Vec<CompiledExpr>,
        negated: bool,
    },
    Case {
        when_branches: Vec<(CompiledExpr, CompiledExpr)>,
        else_branch: Option<Box<CompiledExpr>>,
    },
    Coalesce(Vec<CompiledExpr>),
    NullIf(Box<CompiledExpr>, Box<CompiledExpr>),

    // JSON Operators
    JsonExtract {
        lhs: Box<CompiledExpr>,
        rhs: Box<CompiledExpr>,
        as_text: bool,
    },
    JsonExtractPath {
        lhs: Box<CompiledExpr>,
        path: Box<CompiledExpr>,
        as_text: bool,
    },
    JsonContains(Box<CompiledExpr>, Box<CompiledExpr>),
    JsonContained(Box<CompiledExpr>, Box<CompiledExpr>),
    JsonHasKey(Box<CompiledExpr>, Box<CompiledExpr>),
    JsonHasAnyKey(Box<CompiledExpr>, Box<CompiledExpr>),
    JsonHasAllKeys(Box<CompiledExpr>, Box<CompiledExpr>),
    ArrayOverlap(Box<CompiledExpr>, Box<CompiledExpr>),

    // Function Calls
    FuncCall {
        name: String,
        args: Vec<CompiledExpr>,
    },

    // Fallback for subqueries or raw expressions
    Raw(String),
}

impl CompiledExpr {
    #[inline(always)]
    pub fn eval_bool(&self, primary_row: &[Value], joined_row: Option<&[Value]>, params: &[Value]) -> Option<bool> {
        match self {
            CompiledExpr::And(branches) => {
                let mut has_null = false;
                for b in branches {
                    match b.eval_bool(primary_row, joined_row, params) {
                        Some(false) => return Some(false),
                        None => has_null = true,
                        Some(true) => {}
                    }
                }
                if has_null { None } else { Some(true) }
            }
            CompiledExpr::Or(branches) => {
                let mut has_null = false;
                for b in branches {
                    match b.eval_bool(primary_row, joined_row, params) {
                        Some(true) => return Some(true),
                        None => has_null = true,
                        Some(false) => {}
                    }
                }
                if has_null { None } else { Some(false) }
            }
            CompiledExpr::Not(inner) => {
                inner.eval_bool(primary_row, joined_row, params).map(|b| !b)
            }
            CompiledExpr::IsNull(inner) => {
                let v = inner.eval(primary_row, joined_row, params);
                Some(v.is_null())
            }
            CompiledExpr::IsNotNull(inner) => {
                let v = inner.eval(primary_row, joined_row, params);
                Some(!v.is_null())
            }
            CompiledExpr::Eq(l, r) => {
                let vl = l.eval(primary_row, joined_row, params);
                let vr = r.eval(primary_row, joined_row, params);
                if vl.is_null() || vr.is_null() {
                    None
                } else {
                    Some(vl.is_equal(&vr))
                }
            }
            CompiledExpr::NotEq(l, r) => {
                let vl = l.eval(primary_row, joined_row, params);
                let vr = r.eval(primary_row, joined_row, params);
                if vl.is_null() || vr.is_null() {
                    None
                } else {
                    Some(!vl.is_equal(&vr))
                }
            }
            CompiledExpr::Gt(l, r) => {
                let vl = l.eval(primary_row, joined_row, params);
                let vr = r.eval(primary_row, joined_row, params);
                if vl.is_null() || vr.is_null() {
                    None
                } else {
                    Some(vl.cmp_value(&vr) == std::cmp::Ordering::Greater && !vl.is_equal(&vr))
                }
            }
            CompiledExpr::Gte(l, r) => {
                let vl = l.eval(primary_row, joined_row, params);
                let vr = r.eval(primary_row, joined_row, params);
                if vl.is_null() || vr.is_null() {
                    None
                } else {
                    Some(vl.cmp_value(&vr) != std::cmp::Ordering::Less)
                }
            }
            CompiledExpr::Lt(l, r) => {
                let vl = l.eval(primary_row, joined_row, params);
                let vr = r.eval(primary_row, joined_row, params);
                if vl.is_null() || vr.is_null() {
                    None
                } else {
                    Some(vl.cmp_value(&vr) == std::cmp::Ordering::Less && !vl.is_equal(&vr))
                }
            }
            CompiledExpr::Lte(l, r) => {
                let vl = l.eval(primary_row, joined_row, params);
                let vr = r.eval(primary_row, joined_row, params);
                if vl.is_null() || vr.is_null() {
                    None
                } else {
                    Some(vl.cmp_value(&vr) != std::cmp::Ordering::Greater)
                }
            }
            _ => {
                let val = self.eval(primary_row, joined_row, params);
                val.as_bool()
            }
        }
    }

    pub fn eval(&self, primary_row: &[Value], joined_row: Option<&[Value]>, params: &[Value]) -> Value {
        match self {
            CompiledExpr::Col(idx) => primary_row.get(*idx).cloned().unwrap_or(Value::Null),
            CompiledExpr::JoinedCol(idx) => {
                joined_row.and_then(|r| r.get(*idx)).cloned().unwrap_or(Value::Null)
            }
            CompiledExpr::Param(idx) => params.get(*idx).cloned().unwrap_or(Value::Null),
            CompiledExpr::Literal(val) => val.clone(),
            CompiledExpr::Null => Value::Null,
            CompiledExpr::Now => Value::text(crate::engine::executor::get_current_timestamp()),
            CompiledExpr::CurrentDate => Value::text(crate::engine::executor::get_current_date()),
            CompiledExpr::CurrentTime => Value::text(crate::engine::executor::get_current_time()),
            CompiledExpr::GenRandomUuid => Value::text(crate::types::generate_uuid_v4()),

            CompiledExpr::Not(inner) => {
                match inner.eval(primary_row, joined_row, params) {
                    Value::Null => Value::Null,
                    v => Value::Bool(!v.as_bool().unwrap_or(false)),
                }
            }
            CompiledExpr::Neg(inner) => {
                let v = inner.eval(primary_row, joined_row, params);
                match v {
                    Value::Int(i) => Value::Int(-i),
                    Value::Float(f) => Value::Float(-f),
                    _ => Value::Null,
                }
            }
            CompiledExpr::BitNot(inner) => {
                let v = inner.eval(primary_row, joined_row, params);
                match v.as_i64() {
                    Some(i) => Value::Int(!i),
                    None => Value::Null,
                }
            }
            CompiledExpr::IsNull(inner) => {
                let v = inner.eval(primary_row, joined_row, params);
                Value::Bool(v.is_null())
            }
            CompiledExpr::IsNotNull(inner) => {
                let v = inner.eval(primary_row, joined_row, params);
                Value::Bool(!v.is_null())
            }
            CompiledExpr::IsTrue(inner) => {
                let v = inner.eval(primary_row, joined_row, params);
                Value::Bool(v.as_bool() == Some(true))
            }
            CompiledExpr::IsNotTrue(inner) => {
                let v = inner.eval(primary_row, joined_row, params);
                Value::Bool(v.as_bool() != Some(true))
            }
            CompiledExpr::IsFalse(inner) => {
                let v = inner.eval(primary_row, joined_row, params);
                Value::Bool(v.as_bool() == Some(false))
            }
            CompiledExpr::IsNotFalse(inner) => {
                let v = inner.eval(primary_row, joined_row, params);
                Value::Bool(v.as_bool() != Some(false))
            }
            CompiledExpr::Lower(inner) => {
                let v = inner.eval(primary_row, joined_row, params);
                if v.is_null() { Value::Null } else { Value::text(v.as_str().to_lowercase()) }
            }
            CompiledExpr::Upper(inner) => {
                let v = inner.eval(primary_row, joined_row, params);
                if v.is_null() { Value::Null } else { Value::text(v.as_str().to_uppercase()) }
            }
            CompiledExpr::Trim(inner) => {
                let v = inner.eval(primary_row, joined_row, params);
                if v.is_null() { Value::Null } else { Value::text(v.as_str().trim().to_string()) }
            }
            CompiledExpr::Cast { expr, target_type } => {
                let v = expr.eval(primary_row, joined_row, params);
                crate::engine::executor::cast_val(v, target_type)
            }

            CompiledExpr::Add(l, r) => {
                let vl = l.eval(primary_row, joined_row, params);
                let vr = r.eval(primary_row, joined_row, params);
                match (vl.as_f64(), vr.as_f64()) {
                    (Some(a), Some(b)) => {
                        let res = a + b;
                        if res.fract() == 0.0 { Value::Int(res as i64) } else { Value::Float(res) }
                    }
                    _ => Value::Null,
                }
            }
            CompiledExpr::Sub(l, r) => {
                let vl = l.eval(primary_row, joined_row, params);
                let vr = r.eval(primary_row, joined_row, params);
                match (vl.as_f64(), vr.as_f64()) {
                    (Some(a), Some(b)) => {
                        let res = a - b;
                        if res.fract() == 0.0 { Value::Int(res as i64) } else { Value::Float(res) }
                    }
                    _ => Value::Null,
                }
            }
            CompiledExpr::Mul(l, r) => {
                let vl = l.eval(primary_row, joined_row, params);
                let vr = r.eval(primary_row, joined_row, params);
                match (vl.as_f64(), vr.as_f64()) {
                    (Some(a), Some(b)) => {
                        let res = a * b;
                        if res.fract() == 0.0 { Value::Int(res as i64) } else { Value::Float(res) }
                    }
                    _ => Value::Null,
                }
            }
            CompiledExpr::Div(l, r) => {
                let vl = l.eval(primary_row, joined_row, params);
                let vr = r.eval(primary_row, joined_row, params);
                match (vl.as_f64(), vr.as_f64()) {
                    (Some(a), Some(b)) if b != 0.0 => {
                        let res = a / b;
                        if res.fract() == 0.0 { Value::Int(res as i64) } else { Value::Float(res) }
                    }
                    _ => Value::Null,
                }
            }
            CompiledExpr::Mod(l, r) => {
                let vl = l.eval(primary_row, joined_row, params);
                let vr = r.eval(primary_row, joined_row, params);
                match (vl.as_i64(), vr.as_i64()) {
                    (Some(a), Some(b)) if b != 0 => Value::Int(a % b),
                    _ => Value::Null,
                }
            }
            CompiledExpr::Pow(l, r) => {
                let vl = l.eval(primary_row, joined_row, params);
                let vr = r.eval(primary_row, joined_row, params);
                match (vl.as_f64(), vr.as_f64()) {
                    (Some(a), Some(b)) => {
                        let res = a.powf(b);
                        if res.fract() == 0.0 { Value::Int(res as i64) } else { Value::Float(res) }
                    }
                    _ => Value::Null,
                }
            }
            CompiledExpr::BitAnd(l, r) => {
                let vl = l.eval(primary_row, joined_row, params);
                let vr = r.eval(primary_row, joined_row, params);
                match (vl.as_i64(), vr.as_i64()) {
                    (Some(a), Some(b)) => Value::Int(a & b),
                    _ => Value::Null,
                }
            }
            CompiledExpr::BitOr(l, r) => {
                let vl = l.eval(primary_row, joined_row, params);
                let vr = r.eval(primary_row, joined_row, params);
                match (vl.as_i64(), vr.as_i64()) {
                    (Some(a), Some(b)) => Value::Int(a | b),
                    _ => Value::Null,
                }
            }
            CompiledExpr::BitXor(l, r) => {
                let vl = l.eval(primary_row, joined_row, params);
                let vr = r.eval(primary_row, joined_row, params);
                match (vl.as_i64(), vr.as_i64()) {
                    (Some(a), Some(b)) => Value::Int(a ^ b),
                    _ => Value::Null,
                }
            }
            CompiledExpr::Shl(l, r) => {
                let vl = l.eval(primary_row, joined_row, params);
                let vr = r.eval(primary_row, joined_row, params);
                match (vl.as_i64(), vr.as_i64()) {
                    (Some(a), Some(b)) => Value::Int(a << (b as u32)),
                    _ => Value::Null,
                }
            }
            CompiledExpr::Shr(l, r) => {
                let vl = l.eval(primary_row, joined_row, params);
                let vr = r.eval(primary_row, joined_row, params);
                match (vl.as_i64(), vr.as_i64()) {
                    (Some(a), Some(b)) => Value::Int(a >> (b as u32)),
                    _ => Value::Null,
                }
            }
            CompiledExpr::Concat(l, r) => {
                let vl = l.eval(primary_row, joined_row, params);
                let vr = r.eval(primary_row, joined_row, params);
                if vl.is_null() || vr.is_null() {
                    Value::Null
                } else {
                    Value::text(format!("{}{}", vl.as_str(), vr.as_str()))
                }
            }

            CompiledExpr::Eq(l, r) => {
                let vl = l.eval(primary_row, joined_row, params);
                let vr = r.eval(primary_row, joined_row, params);
                if vl.is_null() || vr.is_null() { Value::Null } else { Value::Bool(vl.is_equal(&vr)) }
            }
            CompiledExpr::NotEq(l, r) => {
                let vl = l.eval(primary_row, joined_row, params);
                let vr = r.eval(primary_row, joined_row, params);
                if vl.is_null() || vr.is_null() { Value::Null } else { Value::Bool(!vl.is_equal(&vr)) }
            }
            CompiledExpr::Gt(l, r) => {
                let vl = l.eval(primary_row, joined_row, params);
                let vr = r.eval(primary_row, joined_row, params);
                if vl.is_null() || vr.is_null() { Value::Null } else { Value::Bool(vl.cmp_value(&vr) == std::cmp::Ordering::Greater && !vl.is_equal(&vr)) }
            }
            CompiledExpr::Gte(l, r) => {
                let vl = l.eval(primary_row, joined_row, params);
                let vr = r.eval(primary_row, joined_row, params);
                if vl.is_null() || vr.is_null() { Value::Null } else { Value::Bool(vl.cmp_value(&vr) != std::cmp::Ordering::Less) }
            }
            CompiledExpr::Lt(l, r) => {
                let vl = l.eval(primary_row, joined_row, params);
                let vr = r.eval(primary_row, joined_row, params);
                if vl.is_null() || vr.is_null() { Value::Null } else { Value::Bool(vl.cmp_value(&vr) == std::cmp::Ordering::Less && !vl.is_equal(&vr)) }
            }
            CompiledExpr::Lte(l, r) => {
                let vl = l.eval(primary_row, joined_row, params);
                let vr = r.eval(primary_row, joined_row, params);
                if vl.is_null() || vr.is_null() { Value::Null } else { Value::Bool(vl.cmp_value(&vr) != std::cmp::Ordering::Greater) }
            }
            CompiledExpr::IsDistinctFrom(l, r) => {
                let vl = l.eval(primary_row, joined_row, params);
                let vr = r.eval(primary_row, joined_row, params);
                if vl.is_null() && vr.is_null() {
                    Value::Bool(false)
                } else if vl.is_null() || vr.is_null() {
                    Value::Bool(true)
                } else {
                    Value::Bool(!vl.is_equal(&vr))
                }
            }
            CompiledExpr::IsNotDistinctFrom(l, r) => {
                let vl = l.eval(primary_row, joined_row, params);
                let vr = r.eval(primary_row, joined_row, params);
                if vl.is_null() && vr.is_null() {
                    Value::Bool(true)
                } else if vl.is_null() || vr.is_null() {
                    Value::Bool(false)
                } else {
                    Value::Bool(vl.is_equal(&vr))
                }
            }

            CompiledExpr::Like { expr, pattern, case_insensitive, negated } => {
                let text_val = expr.eval(primary_row, joined_row, params);
                let pat_val = pattern.eval(primary_row, joined_row, params);
                if text_val.is_null() || pat_val.is_null() {
                    Value::Null
                } else {
                    let m = crate::engine::executor::sql_like_match(&text_val.as_str(), &pat_val.as_str(), *case_insensitive);
                    Value::Bool(if *negated { !m } else { m })
                }
            }
            CompiledExpr::Regex { expr, pattern, case_insensitive, negated } => {
                let text_val = expr.eval(primary_row, joined_row, params);
                if text_val.is_null() {
                    Value::Null
                } else {
                    let m = crate::engine::executor::regex_match(&text_val.as_str(), pattern, *case_insensitive);
                    Value::Bool(if *negated { !m } else { m })
                }
            }

            CompiledExpr::And(branches) => {
                let mut has_null = false;
                for b in branches {
                    let v = b.eval(primary_row, joined_row, params);
                    if v.as_bool() == Some(false) {
                        return Value::Bool(false);
                    }
                    if v.is_null() {
                        has_null = true;
                    }
                }
                if has_null { Value::Null } else { Value::Bool(true) }
            }
            CompiledExpr::Or(branches) => {
                let mut has_null = false;
                for b in branches {
                    let v = b.eval(primary_row, joined_row, params);
                    if v.as_bool() == Some(true) {
                        return Value::Bool(true);
                    }
                    if v.is_null() {
                        has_null = true;
                    }
                }
                if has_null { Value::Null } else { Value::Bool(false) }
            }
            CompiledExpr::Between { expr, low, high, negated } => {
                let v = expr.eval(primary_row, joined_row, params);
                let l = low.eval(primary_row, joined_row, params);
                let h = high.eval(primary_row, joined_row, params);
                if v.is_null() || l.is_null() || h.is_null() {
                    Value::Null
                } else {
                    let within = v.cmp_value(&l) != std::cmp::Ordering::Less && v.cmp_value(&h) != std::cmp::Ordering::Greater;
                    Value::Bool(if *negated { !within } else { within })
                }
            }
            CompiledExpr::In { expr, list, negated } => {
                let v = expr.eval(primary_row, joined_row, params);
                if v.is_null() {
                    return Value::Null;
                }
                let mut has_null = false;
                for item in list {
                    let it = item.eval(primary_row, joined_row, params);
                    if it.is_null() {
                        has_null = true;
                    } else if v.is_equal(&it) {
                        return Value::Bool(!*negated);
                    } else {
                        let items = crate::engine::executor::val_to_array_items(&it);
                        if (items.len() > 1 || (items.len() == 1 && !items[0].is_equal(&it)))
                            && items.iter().any(|item| v.is_equal(item))
                        {
                            return Value::Bool(!*negated);
                        }
                    }
                }
                if has_null {
                    Value::Null
                } else {
                    Value::Bool(*negated)
                }
            }
            CompiledExpr::Case { when_branches, else_branch } => {
                for (cond, result) in when_branches {
                    if cond.eval_bool(primary_row, joined_row, params) == Some(true) {
                        return result.eval(primary_row, joined_row, params);
                    }
                }
                if let Some(eb) = else_branch {
                    eb.eval(primary_row, joined_row, params)
                } else {
                    Value::Null
                }
            }
            CompiledExpr::Coalesce(args) => {
                for arg in args {
                    let v = arg.eval(primary_row, joined_row, params);
                    if !v.is_null() {
                        return v;
                    }
                }
                Value::Null
            }
            CompiledExpr::NullIf(a, b) => {
                let va = a.eval(primary_row, joined_row, params);
                let vb = b.eval(primary_row, joined_row, params);
                if va.is_equal(&vb) { Value::Null } else { va }
            }

            CompiledExpr::JsonExtract { lhs, rhs, as_text } => {
                let vl = lhs.eval(primary_row, joined_row, params);
                let vr = rhs.eval(primary_row, joined_row, params);
                let lhs_json = crate::engine::executor::parse_val_to_json(&vl).unwrap_or(serde_json::Value::Null);
                let rhs_json = serde_json::Value::String(vr.as_str());
                let op = if *as_text { "->>" } else { "->" };
                let res = crate::engine::executor::eval_json_extract_serde(&lhs_json, &rhs_json, op);
                crate::engine::executor::json_to_value(&res)
            }
            CompiledExpr::JsonExtractPath { lhs, path, as_text } => {
                let vl = lhs.eval(primary_row, joined_row, params);
                let vp = path.eval(primary_row, joined_row, params);
                let lhs_json = crate::engine::executor::parse_val_to_json(&vl).unwrap_or(serde_json::Value::Null);
                let path_json = serde_json::Value::String(vp.as_str());
                let op = if *as_text { "#>>" } else { "#>" };
                let res = crate::engine::executor::eval_json_extract_serde(&lhs_json, &path_json, op);
                crate::engine::executor::json_to_value(&res)
            }
            CompiledExpr::JsonContains(l, r) => {
                let vl = l.eval(primary_row, joined_row, params);
                let vr = r.eval(primary_row, joined_row, params);
                if let (Some(lj), Some(rj)) = (crate::engine::executor::parse_val_to_json(&vl), crate::engine::executor::parse_val_to_json(&vr)) {
                    Value::Bool(crate::engine::executor::json_contains_check(&lj, &rj))
                } else {
                    Value::Bool(crate::engine::executor::array_contains_check(&vl, &vr))
                }
            }
            CompiledExpr::JsonContained(l, r) => {
                let vl = l.eval(primary_row, joined_row, params);
                let vr = r.eval(primary_row, joined_row, params);
                if let (Some(lj), Some(rj)) = (crate::engine::executor::parse_val_to_json(&vl), crate::engine::executor::parse_val_to_json(&vr)) {
                    Value::Bool(crate::engine::executor::json_contains_check(&rj, &lj))
                } else {
                    Value::Bool(crate::engine::executor::array_contains_check(&vr, &vl))
                }
            }
            CompiledExpr::JsonHasKey(l, r) => {
                let vl = l.eval(primary_row, joined_row, params);
                let vr = r.eval(primary_row, joined_row, params);
                if let Some(lj) = crate::engine::executor::parse_val_to_json(&vl) {
                    Value::Bool(crate::engine::executor::json_has_key_check(&lj, &vr.as_str()))
                } else {
                    Value::Bool(false)
                }
            }
            CompiledExpr::JsonHasAnyKey(l, r) => {
                let vl = l.eval(primary_row, joined_row, params);
                let vr = r.eval(primary_row, joined_row, params);
                if let Some(lj) = crate::engine::executor::parse_val_to_json(&vl) {
                    let keys = crate::engine::executor::parse_str_or_json_keys(&vr);
                    Value::Bool(keys.iter().any(|k| crate::engine::executor::json_has_key_check(&lj, k)))
                } else {
                    Value::Bool(false)
                }
            }
            CompiledExpr::JsonHasAllKeys(l, r) => {
                let vl = l.eval(primary_row, joined_row, params);
                let vr = r.eval(primary_row, joined_row, params);
                if let Some(lj) = crate::engine::executor::parse_val_to_json(&vl) {
                    let keys = crate::engine::executor::parse_str_or_json_keys(&vr);
                    Value::Bool(!keys.is_empty() && keys.iter().all(|k| crate::engine::executor::json_has_key_check(&lj, k)))
                } else {
                    Value::Bool(false)
                }
            }
            CompiledExpr::ArrayOverlap(l, r) => {
                let vl = l.eval(primary_row, joined_row, params);
                let vr = r.eval(primary_row, joined_row, params);
                Value::Bool(crate::engine::executor::array_overlap_check(&vl, &vr))
            }

            CompiledExpr::FuncCall { name, args } => {
                let evaluated_args: Vec<Value> = args.iter().map(|a| a.eval(primary_row, joined_row, params)).collect();
                eval_func_call(name, &evaluated_args)
            }

            CompiledExpr::Raw(s) => {
                crate::engine::executor::eval_sql_expr(s, primary_row, None, &[], None, params)
            }
        }
    }

    #[inline(always)]
    pub(crate) fn eval_joined(&self, row: &crate::engine::executor::CombinedRow, params: &[Value]) -> Option<bool> {
        let v = self.eval_on_combined(row, params);
        v.as_bool()
    }

    pub(crate) fn eval_on_combined(&self, row: &crate::engine::executor::CombinedRow, params: &[Value]) -> Value {
        match self {
            CompiledExpr::Col(slot) | CompiledExpr::JoinedCol(slot) => {
                row.values.get(*slot).cloned().unwrap_or(Value::Null)
            }
            CompiledExpr::And(branches) => {
                let mut has_null = false;
                for b in branches {
                    let v = b.eval_on_combined(row, params);
                    if v.as_bool() == Some(false) {
                        return Value::Bool(false);
                    }
                    if v.is_null() {
                        has_null = true;
                    }
                }
                if has_null { Value::Null } else { Value::Bool(true) }
            }
            CompiledExpr::Or(branches) => {
                let mut has_null = false;
                for b in branches {
                    let v = b.eval_on_combined(row, params);
                    if v.as_bool() == Some(true) {
                        return Value::Bool(true);
                    }
                    if v.is_null() {
                        has_null = true;
                    }
                }
                if has_null { Value::Null } else { Value::Bool(false) }
            }
            CompiledExpr::Not(inner) => {
                match inner.eval_on_combined(row, params) {
                    Value::Null => Value::Null,
                    v => Value::Bool(!v.as_bool().unwrap_or(false)),
                }
            }
            CompiledExpr::Eq(l, r) => {
                let vl = l.eval_on_combined(row, params);
                let vr = r.eval_on_combined(row, params);
                if vl.is_null() || vr.is_null() { Value::Null } else { Value::Bool(vl.is_equal(&vr)) }
            }
            CompiledExpr::NotEq(l, r) => {
                let vl = l.eval_on_combined(row, params);
                let vr = r.eval_on_combined(row, params);
                if vl.is_null() || vr.is_null() { Value::Null } else { Value::Bool(!vl.is_equal(&vr)) }
            }
            CompiledExpr::Gt(l, r) => {
                let vl = l.eval_on_combined(row, params);
                let vr = r.eval_on_combined(row, params);
                if vl.is_null() || vr.is_null() { Value::Null } else { Value::Bool(vl.cmp_value(&vr) == std::cmp::Ordering::Greater && !vl.is_equal(&vr)) }
            }
            CompiledExpr::Gte(l, r) => {
                let vl = l.eval_on_combined(row, params);
                let vr = r.eval_on_combined(row, params);
                if vl.is_null() || vr.is_null() { Value::Null } else { Value::Bool(vl.cmp_value(&vr) != std::cmp::Ordering::Less) }
            }
            CompiledExpr::Lt(l, r) => {
                let vl = l.eval_on_combined(row, params);
                let vr = r.eval_on_combined(row, params);
                if vl.is_null() || vr.is_null() { Value::Null } else { Value::Bool(vl.cmp_value(&vr) == std::cmp::Ordering::Less && !vl.is_equal(&vr)) }
            }
            CompiledExpr::Lte(l, r) => {
                let vl = l.eval_on_combined(row, params);
                let vr = r.eval_on_combined(row, params);
                if vl.is_null() || vr.is_null() { Value::Null } else { Value::Bool(vl.cmp_value(&vr) != std::cmp::Ordering::Greater) }
            }
            CompiledExpr::IsNull(inner) => {
                let v = inner.eval_on_combined(row, params);
                Value::Bool(v.is_null())
            }
            CompiledExpr::IsNotNull(inner) => {
                let v = inner.eval_on_combined(row, params);
                Value::Bool(!v.is_null())
            }
            CompiledExpr::Raw(s) => {
                Value::Bool(crate::engine::executor::eval_condition_on_row(row, s, params))
            }
            _ => self.eval(&row.values, None, params),
        }
    }

    pub fn eval_const(&self, params: &[Value]) -> Option<Value> {
        match self {
            CompiledExpr::Literal(v) => Some(v.clone()),
            CompiledExpr::Null => Some(Value::Null),
            CompiledExpr::Param(idx) => params.get(*idx).cloned(),
            _ => None,
        }
    }

    pub fn find_pk_eq(&self, pk_col_idx: usize, params: &[Value]) -> Option<i64> {
        match self {
            CompiledExpr::Eq(lhs, rhs) => {
                match (&**lhs, &**rhs) {
                    (CompiledExpr::Col(idx), val_expr) if *idx == pk_col_idx => {
                        val_expr.eval_const(params).and_then(|v| v.as_i64())
                    }
                    (val_expr, CompiledExpr::Col(idx)) if *idx == pk_col_idx => {
                        val_expr.eval_const(params).and_then(|v| v.as_i64())
                    }
                    _ => None,
                }
            }
            CompiledExpr::And(branches) => {
                for b in branches {
                    if let Some(target) = b.find_pk_eq(pk_col_idx, params) {
                        return Some(target);
                    }
                }
                None
            }
            _ => None,
        }
    }
}

pub fn compile_expr(
    sql_expr: &str,
    table: Option<&crate::storage::table::Table>,
    table_alias: Option<&str>,
    joined_table: Option<&crate::storage::table::Table>,
    joined_alias: Option<&str>,
) -> CompiledExpr {
    let mut s = sql_expr.trim();
    while s.starts_with('(') && s.ends_with(')') && is_enclosed(s) {
        let inside = s[1..s.len() - 1].trim();
        if crate::engine::executor::split_comma_separated_tokens(inside).len() > 1 {
            break;
        }
        s = inside;
    }

    let upper = s.to_uppercase();

    // 1. CASE WHEN ... THEN ... ELSE ... END
    if upper.starts_with("CASE") && upper.ends_with("END") {
        let inside = s[4..s.len() - 3].trim();
        let (when_part, else_part) = if let Some(else_idx) = crate::engine::executor::find_top_level_keyword(inside, "ELSE") {
            (&inside[..else_idx], Some(inside[else_idx + 4..].trim()))
        } else {
            (inside, None)
        };

        let raw_branches = crate::engine::executor::split_when_clauses(when_part);
        let when_branches = raw_branches.into_iter().map(|(c, r)| {
            (
                compile_expr(&c, table, table_alias, joined_table, joined_alias),
                compile_expr(&r, table, table_alias, joined_table, joined_alias),
            )
        }).collect();

        let else_branch = else_part.map(|e| Box::new(compile_expr(e, table, table_alias, joined_table, joined_alias)));
        return CompiledExpr::Case { when_branches, else_branch };
    }

    // 2. OR
    let or_parts = crate::engine::executor::split_top_level_or(s);
    if or_parts.len() > 1 {
        let branches = or_parts.into_iter().map(|p| compile_expr(p, table, table_alias, joined_table, joined_alias)).collect();
        return CompiledExpr::Or(branches);
    }

    // 3. AND
    let and_parts = crate::engine::executor::split_top_level_and(s);
    if and_parts.len() > 1 {
        let branches = and_parts.into_iter().map(|p| compile_expr(p, table, table_alias, joined_table, joined_alias)).collect();
        return CompiledExpr::And(branches);
    }

    // 4. IS NOT DISTINCT FROM / IS DISTINCT FROM
    if let Some(pos) = crate::engine::executor::find_top_level_keyword(s, "IS NOT DISTINCT FROM") {
        let l = compile_expr(&s[..pos], table, table_alias, joined_table, joined_alias);
        let r = compile_expr(&s[pos + 20..], table, table_alias, joined_table, joined_alias);
        return CompiledExpr::IsNotDistinctFrom(Box::new(l), Box::new(r));
    }
    if let Some(pos) = crate::engine::executor::find_top_level_keyword(s, "IS DISTINCT FROM") {
        let l = compile_expr(&s[..pos], table, table_alias, joined_table, joined_alias);
        let r = compile_expr(&s[pos + 16..], table, table_alias, joined_table, joined_alias);
        return CompiledExpr::IsDistinctFrom(Box::new(l), Box::new(r));
    }

    // 5. IS [NOT] TRUE/FALSE/NULL
    if let Some(pos) = crate::engine::executor::find_top_level_keyword(s, "IS NOT TRUE") {
        return CompiledExpr::IsNotTrue(Box::new(compile_expr(&s[..pos], table, table_alias, joined_table, joined_alias)));
    }
    if let Some(pos) = crate::engine::executor::find_top_level_keyword(s, "IS TRUE") {
        return CompiledExpr::IsTrue(Box::new(compile_expr(&s[..pos], table, table_alias, joined_table, joined_alias)));
    }
    if let Some(pos) = crate::engine::executor::find_top_level_keyword(s, "IS NOT FALSE") {
        return CompiledExpr::IsNotFalse(Box::new(compile_expr(&s[..pos], table, table_alias, joined_table, joined_alias)));
    }
    if let Some(pos) = crate::engine::executor::find_top_level_keyword(s, "IS FALSE") {
        return CompiledExpr::IsFalse(Box::new(compile_expr(&s[..pos], table, table_alias, joined_table, joined_alias)));
    }
    if let Some(pos) = crate::engine::executor::find_top_level_keyword(s, "IS NOT NULL") {
        return CompiledExpr::IsNotNull(Box::new(compile_expr(&s[..pos], table, table_alias, joined_table, joined_alias)));
    }
    if let Some(pos) = crate::engine::executor::find_top_level_keyword(s, "IS NULL") {
        return CompiledExpr::IsNull(Box::new(compile_expr(&s[..pos], table, table_alias, joined_table, joined_alias)));
    }

    // 6. NOT
    let is_not_kw = upper.starts_with("NOT ILIKE ")
        || upper.starts_with("NOT LIKE ")
        || upper.starts_with("NOT BETWEEN ")
        || upper.starts_with("NOT IN ")
        || upper.starts_with("NOT IN(");

    if !is_not_kw && (upper.starts_with("NOT ") || (upper.starts_with("NOT(") && s.ends_with(')'))) {
        let inner = if upper.starts_with("NOT ") { s[4..].trim() } else { s[3..].trim() };
        return CompiledExpr::Not(Box::new(compile_expr(inner, table, table_alias, joined_table, joined_alias)));
    }

    // 7. BETWEEN
    if let Some(pos) = crate::engine::executor::find_top_level_keyword(s, "NOT BETWEEN") {
        let left_str = s[..pos].trim();
        let right_str = s[pos + 11..].trim();
        if let Some(and_pos) = crate::engine::executor::find_top_level_keyword(right_str, "AND") {
            let low = compile_expr(right_str[..and_pos].trim(), table, table_alias, joined_table, joined_alias);
            let high = compile_expr(right_str[and_pos + 3..].trim(), table, table_alias, joined_table, joined_alias);
            let expr = compile_expr(left_str, table, table_alias, joined_table, joined_alias);
            return CompiledExpr::Between { expr: Box::new(expr), low: Box::new(low), high: Box::new(high), negated: true };
        }
    }
    if let Some(pos) = crate::engine::executor::find_top_level_keyword(s, "BETWEEN") {
        let left_str = s[..pos].trim();
        let right_str = s[pos + 7..].trim();
        if let Some(and_pos) = crate::engine::executor::find_top_level_keyword(right_str, "AND") {
            let low = compile_expr(right_str[..and_pos].trim(), table, table_alias, joined_table, joined_alias);
            let high = compile_expr(right_str[and_pos + 3..].trim(), table, table_alias, joined_table, joined_alias);
            let expr = compile_expr(left_str, table, table_alias, joined_table, joined_alias);
            return CompiledExpr::Between { expr: Box::new(expr), low: Box::new(low), high: Box::new(high), negated: false };
        }
    }

    // 8. IN (...)
    if let Some(pos) = crate::engine::executor::find_top_level_keyword(s, "NOT IN") {
        let left_str = s[..pos].trim();
        let right_str = s[pos + 6..].trim();
        if right_str.starts_with('(') && right_str.ends_with(')') {
            let inside = &right_str[1..right_str.len() - 1];
            let list = crate::engine::executor::split_comma_separated_tokens(inside)
                .into_iter()
                .map(|tok| compile_expr(tok, table, table_alias, joined_table, joined_alias))
                .collect();
            let expr = compile_expr(left_str, table, table_alias, joined_table, joined_alias);
            return CompiledExpr::In { expr: Box::new(expr), list, negated: true };
        }
    }
    if let Some(pos) = crate::engine::executor::find_top_level_keyword(s, "IN") {
        let left_str = s[..pos].trim();
        let right_str = s[pos + 2..].trim();
        if right_str.starts_with('(') && right_str.ends_with(')') {
            let inside = &right_str[1..right_str.len() - 1];
            let list = crate::engine::executor::split_comma_separated_tokens(inside)
                .into_iter()
                .map(|tok| compile_expr(tok, table, table_alias, joined_table, joined_alias))
                .collect();
            let expr = compile_expr(left_str, table, table_alias, joined_table, joined_alias);
            return CompiledExpr::In { expr: Box::new(expr), list, negated: false };
        }
    }

    // 9. LIKE / ILIKE / REGEX
    if let Some(pos) = crate::engine::executor::find_top_level_keyword(s, "NOT ILIKE") {
        let l = compile_expr(&s[..pos], table, table_alias, joined_table, joined_alias);
        let r = compile_expr(&s[pos + 9..], table, table_alias, joined_table, joined_alias);
        return CompiledExpr::Like { expr: Box::new(l), pattern: Box::new(r), case_insensitive: true, negated: true };
    }
    if let Some(pos) = crate::engine::executor::find_top_level_keyword(s, "NOT LIKE") {
        let l = compile_expr(&s[..pos], table, table_alias, joined_table, joined_alias);
        let r = compile_expr(&s[pos + 8..], table, table_alias, joined_table, joined_alias);
        return CompiledExpr::Like { expr: Box::new(l), pattern: Box::new(r), case_insensitive: false, negated: true };
    }
    if let Some(pos) = crate::engine::executor::find_top_level_keyword(s, "ILIKE") {
        let l = compile_expr(&s[..pos], table, table_alias, joined_table, joined_alias);
        let r = compile_expr(&s[pos + 5..], table, table_alias, joined_table, joined_alias);
        return CompiledExpr::Like { expr: Box::new(l), pattern: Box::new(r), case_insensitive: true, negated: false };
    }
    if let Some(pos) = crate::engine::executor::find_top_level_keyword(s, "LIKE") {
        let l = compile_expr(&s[..pos], table, table_alias, joined_table, joined_alias);
        let r = compile_expr(&s[pos + 4..], table, table_alias, joined_table, joined_alias);
        return CompiledExpr::Like { expr: Box::new(l), pattern: Box::new(r), case_insensitive: false, negated: false };
    }

    // 10. JSON & Array containment
    if let Some(pos) = s.find(" @> ") {
        let l = compile_expr(&s[..pos], table, table_alias, joined_table, joined_alias);
        let r = compile_expr(&s[pos + 4..], table, table_alias, joined_table, joined_alias);
        return CompiledExpr::JsonContains(Box::new(l), Box::new(r));
    }
    if let Some(pos) = s.find(" <@ ") {
        let l = compile_expr(&s[..pos], table, table_alias, joined_table, joined_alias);
        let r = compile_expr(&s[pos + 4..], table, table_alias, joined_table, joined_alias);
        return CompiledExpr::JsonContained(Box::new(l), Box::new(r));
    }
    if let Some(pos) = crate::engine::executor::find_top_level_op(s, "&&") {
        let l = compile_expr(&s[..pos], table, table_alias, joined_table, joined_alias);
        let r = compile_expr(&s[pos + 2..], table, table_alias, joined_table, joined_alias);
        return CompiledExpr::ArrayOverlap(Box::new(l), Box::new(r));
    }
    if let Some(pos) = crate::engine::executor::find_top_level_op(s, "?|") {
        let l = compile_expr(&s[..pos], table, table_alias, joined_table, joined_alias);
        let r = compile_expr(&s[pos + 2..], table, table_alias, joined_table, joined_alias);
        return CompiledExpr::JsonHasAnyKey(Box::new(l), Box::new(r));
    }
    if let Some(pos) = crate::engine::executor::find_top_level_op(s, "?&") {
        let l = compile_expr(&s[..pos], table, table_alias, joined_table, joined_alias);
        let r = compile_expr(&s[pos + 2..], table, table_alias, joined_table, joined_alias);
        return CompiledExpr::JsonHasAllKeys(Box::new(l), Box::new(r));
    }
    if let Some(pos) = crate::engine::executor::find_top_level_op(s, "?") {
        let l = compile_expr(&s[..pos], table, table_alias, joined_table, joined_alias);
        let r = compile_expr(&s[pos + 1..], table, table_alias, joined_table, joined_alias);
        return CompiledExpr::JsonHasKey(Box::new(l), Box::new(r));
    }

    // 11. Comparison operators
    for op in &[">=", "<=", "!=", "<>", "=", ">", "<"] {
        if let Some(idx) = crate::engine::executor::find_top_level_op(s, op) {
            let l = compile_expr(&s[..idx], table, table_alias, joined_table, joined_alias);
            let r = compile_expr(&s[idx + op.len()..], table, table_alias, joined_table, joined_alias);
            return match *op {
                "=" => CompiledExpr::Eq(Box::new(l), Box::new(r)),
                "!=" | "<>" => CompiledExpr::NotEq(Box::new(l), Box::new(r)),
                ">=" => CompiledExpr::Gte(Box::new(l), Box::new(r)),
                "<=" => CompiledExpr::Lte(Box::new(l), Box::new(r)),
                ">" => CompiledExpr::Gt(Box::new(l), Box::new(r)),
                "<" => CompiledExpr::Lt(Box::new(l), Box::new(r)),
                _ => unreachable!(),
            };
        }
    }

    // 12. Concat (||)
    if let Some(pipe_idx) = crate::engine::executor::find_top_level_op(s, "||") {
        let l = compile_expr(&s[..pipe_idx], table, table_alias, joined_table, joined_alias);
        let r = compile_expr(&s[pipe_idx + 2..], table, table_alias, joined_table, joined_alias);
        return CompiledExpr::Concat(Box::new(l), Box::new(r));
    }

    // 13. Arithmetic (+, -, *, /, %)
    if let Some((op_idx, op)) = crate::engine::executor::find_top_level_math_op(s, &['+', '-']) {
        let l = compile_expr(&s[..op_idx], table, table_alias, joined_table, joined_alias);
        let r = compile_expr(&s[op_idx + 1..], table, table_alias, joined_table, joined_alias);
        return if op == '+' { CompiledExpr::Add(Box::new(l), Box::new(r)) } else { CompiledExpr::Sub(Box::new(l), Box::new(r)) };
    }
    if let Some((op_idx, op)) = crate::engine::executor::find_top_level_math_op(s, &['*', '/', '%']) {
        let l = compile_expr(&s[..op_idx], table, table_alias, joined_table, joined_alias);
        let r = compile_expr(&s[op_idx + 1..], table, table_alias, joined_table, joined_alias);
        return match op {
            '*' => CompiledExpr::Mul(Box::new(l), Box::new(r)),
            '/' => CompiledExpr::Div(Box::new(l), Box::new(r)),
            '%' => CompiledExpr::Mod(Box::new(l), Box::new(r)),
            _ => unreachable!(),
        };
    }

    // 14. Cast (::)
    if let Some(colon_pos) = crate::engine::executor::find_last_top_level_op(s, "::") {
        let target_expr = compile_expr(&s[..colon_pos], table, table_alias, joined_table, joined_alias);
        let target_type = s[colon_pos + 2..].trim().to_uppercase();
        return CompiledExpr::Cast { expr: Box::new(target_expr), target_type };
    }

    // 15. JSON extract operators (->, ->>, #>, #>>)
    if let Some((op_idx, op)) = crate::engine::executor::find_last_top_level_json_op(s) {
        let l = compile_expr(&s[..op_idx], table, table_alias, joined_table, joined_alias);
        let r = compile_expr(&s[op_idx + op.len()..], table, table_alias, joined_table, joined_alias);
        return match op {
            "->" => CompiledExpr::JsonExtract { lhs: Box::new(l), rhs: Box::new(r), as_text: false },
            "->>" => CompiledExpr::JsonExtract { lhs: Box::new(l), rhs: Box::new(r), as_text: true },
            "#>" => CompiledExpr::JsonExtractPath { lhs: Box::new(l), path: Box::new(r), as_text: false },
            "#>>" => CompiledExpr::JsonExtractPath { lhs: Box::new(l), path: Box::new(r), as_text: true },
            _ => CompiledExpr::Raw(s.to_string()),
        };
    }

    // 16. COALESCE / NULLIF
    if upper.starts_with("COALESCE(") && s.ends_with(')') {
        let inner = &s[9..s.len() - 1];
        let args = crate::engine::executor::split_function_args(inner)
            .into_iter()
            .map(|a| compile_expr(&a, table, table_alias, joined_table, joined_alias))
            .collect();
        return CompiledExpr::Coalesce(args);
    }
    if upper.starts_with("NULLIF(") && s.ends_with(')') {
        let inner = &s[7..s.len() - 1];
        let args = crate::engine::executor::split_function_args(inner);
        if args.len() == 2 {
            let a = compile_expr(&args[0], table, table_alias, joined_table, joined_alias);
            let b = compile_expr(&args[1], table, table_alias, joined_table, joined_alias);
            return CompiledExpr::NullIf(Box::new(a), Box::new(b));
        }
    }

    // 17. Constant functions
    if upper == "CURRENT_TIMESTAMP" || upper == "NOW()" || upper == "LOCALTIMESTAMP" {
        return CompiledExpr::Now;
    }
    if upper == "CURRENT_DATE" {
        return CompiledExpr::CurrentDate;
    }
    if upper == "CURRENT_TIME" || upper == "LOCALTIME" {
        return CompiledExpr::CurrentTime;
    }
    if upper == "GEN_RANDOM_UUID()" || upper == "UUID_GENERATE_V4()" {
        return CompiledExpr::GenRandomUuid;
    }

    // 18. Functions (LOWER, UPPER, TRIM, etc.)
    if upper.starts_with("LOWER(") && s.ends_with(')') {
        let inner = &s[6..s.len() - 1];
        return CompiledExpr::Lower(Box::new(compile_expr(inner, table, table_alias, joined_table, joined_alias)));
    }
    if upper.starts_with("UPPER(") && s.ends_with(')') {
        let inner = &s[6..s.len() - 1];
        return CompiledExpr::Upper(Box::new(compile_expr(inner, table, table_alias, joined_table, joined_alias)));
    }
    if (upper.starts_with("TRIM(") || upper.starts_with("BTRIM(")) && s.ends_with(')') {
        let open_p = s.find('(').unwrap();
        let inner = &s[open_p + 1..s.len() - 1];
        return CompiledExpr::Trim(Box::new(compile_expr(inner, table, table_alias, joined_table, joined_alias)));
    }

    // Generic FuncCall
    if let Some(open_p) = s.find('(') {
        if s.ends_with(')') {
            let fn_name = s[..open_p].trim().to_uppercase();
            let inner = &s[open_p + 1..s.len() - 1];
            let args = crate::engine::executor::split_function_args(inner)
                .into_iter()
                .map(|a| compile_expr(&a, table, table_alias, joined_table, joined_alias))
                .collect();
            return CompiledExpr::FuncCall { name: fn_name, args };
        }
    }

    // 19. Parameter ($1, $2, ...)
    if s.starts_with('$') {
        if let Ok(idx) = s[1..].parse::<usize>() {
            return CompiledExpr::Param(idx.saturating_sub(1));
        }
    }

    // 20. Literals
    if s.eq_ignore_ascii_case("TRUE") {
        return CompiledExpr::Literal(Value::Bool(true));
    }
    if s.eq_ignore_ascii_case("FALSE") {
        return CompiledExpr::Literal(Value::Bool(false));
    }
    if s.eq_ignore_ascii_case("NULL") {
        return CompiledExpr::Null;
    }
    if s.starts_with('\'') && s.ends_with('\'') && s.len() >= 2 {
        return CompiledExpr::Literal(Value::text(s[1..s.len() - 1].replace("''", "'")));
    }
    if let Ok(i) = s.parse::<i64>() {
        return CompiledExpr::Literal(Value::Int(i));
    }
    if let Ok(f) = s.parse::<f64>() {
        return CompiledExpr::Literal(Value::Float(f));
    }

    // 21. Column lookups
    let clean = crate::engine::executor::clean_col_name(s);
    if let Some(tbl) = table {
        let is_joined = if let Some(dot_idx) = s.find('.') {
            let prefix = s[..dot_idx].trim().trim_matches('"');
            if let Some(jt) = joined_table {
                prefix.eq_ignore_ascii_case(&jt.name)
                    || joined_alias.map(|a| prefix.eq_ignore_ascii_case(a.trim_matches('"'))).unwrap_or(false)
            } else {
                false
            }
        } else {
            false
        };

        if !is_joined {
            if let Some(col_idx) = tbl.get_column_index(clean).or_else(|| tbl.get_column_index(s)) {
                return CompiledExpr::Col(col_idx);
            }
        }
    }

    if let Some(jt) = joined_table {
        if let Some(col_idx) = jt.get_column_index(clean).or_else(|| jt.get_column_index(s)) {
            return CompiledExpr::JoinedCol(col_idx);
        }
    }

    CompiledExpr::Raw(s.to_string())
}

pub(crate) fn compile_expr_for_joined(
    sql_expr: &str,
    joined_schema: &crate::engine::executor::JoinedSchema,
) -> CompiledExpr {
    let mut s = sql_expr.trim();
    while s.starts_with('(') && s.ends_with(')') && is_enclosed(s) {
        let inside = s[1..s.len() - 1].trim();
        if crate::engine::executor::split_comma_separated_tokens(inside).len() > 1 {
            break;
        }
        s = inside;
    }

    let upper = s.to_uppercase();

    // CASE WHEN ... THEN ... ELSE ... END
    if upper.starts_with("CASE") && upper.ends_with("END") {
        let inside = s[4..s.len() - 3].trim();
        let (when_part, else_part) = if let Some(else_idx) = crate::engine::executor::find_top_level_keyword(inside, "ELSE") {
            (&inside[..else_idx], Some(inside[else_idx + 4..].trim()))
        } else {
            (inside, None)
        };
        let raw_branches = crate::engine::executor::split_when_clauses(when_part);
        let when_branches = raw_branches.into_iter().map(|(c, r)| {
            (
                compile_expr_for_joined(&c, joined_schema),
                compile_expr_for_joined(&r, joined_schema),
            )
        }).collect();
        let else_branch = else_part.map(|e| Box::new(compile_expr_for_joined(e, joined_schema)));
        return CompiledExpr::Case { when_branches, else_branch };
    }

    // OR
    let or_parts = crate::engine::executor::split_top_level_or(s);
    if or_parts.len() > 1 {
        let branches = or_parts.into_iter().map(|p| compile_expr_for_joined(p, joined_schema)).collect();
        return CompiledExpr::Or(branches);
    }

    // AND
    let and_parts = crate::engine::executor::split_top_level_and(s);
    if and_parts.len() > 1 {
        let branches = and_parts.into_iter().map(|p| compile_expr_for_joined(p, joined_schema)).collect();
        return CompiledExpr::And(branches);
    }

    // NOT
    if upper.starts_with("NOT ") {
        return CompiledExpr::Not(Box::new(compile_expr_for_joined(&s[4..], joined_schema)));
    }

    // BETWEEN / NOT BETWEEN
    if let Some(pos) = crate::engine::executor::find_top_level_keyword(s, "NOT BETWEEN") {
        let left_str = s[..pos].trim();
        let right_str = s[pos + 11..].trim();
        if let Some(and_pos) = crate::engine::executor::find_top_level_keyword(right_str, "AND") {
            let low = compile_expr_for_joined(right_str[..and_pos].trim(), joined_schema);
            let high = compile_expr_for_joined(right_str[and_pos + 3..].trim(), joined_schema);
            let expr = compile_expr_for_joined(left_str, joined_schema);
            return CompiledExpr::Between { expr: Box::new(expr), low: Box::new(low), high: Box::new(high), negated: true };
        }
    }
    if let Some(pos) = crate::engine::executor::find_top_level_keyword(s, "BETWEEN") {
        let left_str = s[..pos].trim();
        let right_str = s[pos + 7..].trim();
        if let Some(and_pos) = crate::engine::executor::find_top_level_keyword(right_str, "AND") {
            let low = compile_expr_for_joined(right_str[..and_pos].trim(), joined_schema);
            let high = compile_expr_for_joined(right_str[and_pos + 3..].trim(), joined_schema);
            let expr = compile_expr_for_joined(left_str, joined_schema);
            return CompiledExpr::Between { expr: Box::new(expr), low: Box::new(low), high: Box::new(high), negated: false };
        }
    }

    // IN / NOT IN
    if let Some(pos) = crate::engine::executor::find_top_level_keyword(s, "NOT IN") {
        let left_str = s[..pos].trim();
        let right_str = s[pos + 6..].trim();
        if right_str.starts_with('(') && right_str.ends_with(')') {
            let inside = &right_str[1..right_str.len() - 1];
            let list = crate::engine::executor::split_comma_separated_tokens(inside)
                .into_iter()
                .map(|tok| compile_expr_for_joined(tok, joined_schema))
                .collect();
            let expr = compile_expr_for_joined(left_str, joined_schema);
            return CompiledExpr::In { expr: Box::new(expr), list, negated: true };
        }
    }
    if let Some(pos) = crate::engine::executor::find_top_level_keyword(s, "IN") {
        let left_str = s[..pos].trim();
        let right_str = s[pos + 2..].trim();
        if right_str.starts_with('(') && right_str.ends_with(')') {
            let inside = &right_str[1..right_str.len() - 1];
            let list = crate::engine::executor::split_comma_separated_tokens(inside)
                .into_iter()
                .map(|tok| compile_expr_for_joined(tok, joined_schema))
                .collect();
            let expr = compile_expr_for_joined(left_str, joined_schema);
            return CompiledExpr::In { expr: Box::new(expr), list, negated: false };
        }
    }

    // LIKE / ILIKE
    if let Some(pos) = crate::engine::executor::find_top_level_keyword(s, "NOT ILIKE") {
        let l = compile_expr_for_joined(&s[..pos], joined_schema);
        let r = compile_expr_for_joined(&s[pos + 9..], joined_schema);
        return CompiledExpr::Like { expr: Box::new(l), pattern: Box::new(r), case_insensitive: true, negated: true };
    }
    if let Some(pos) = crate::engine::executor::find_top_level_keyword(s, "ILIKE") {
        let l = compile_expr_for_joined(&s[..pos], joined_schema);
        let r = compile_expr_for_joined(&s[pos + 5..], joined_schema);
        return CompiledExpr::Like { expr: Box::new(l), pattern: Box::new(r), case_insensitive: true, negated: false };
    }

    // IS NULL / IS NOT NULL
    if let Some(pos) = crate::engine::executor::find_top_level_keyword(s, "IS NOT NULL") {
        return CompiledExpr::IsNotNull(Box::new(compile_expr_for_joined(&s[..pos], joined_schema)));
    }
    if let Some(pos) = crate::engine::executor::find_top_level_keyword(s, "IS NULL") {
        return CompiledExpr::IsNull(Box::new(compile_expr_for_joined(&s[..pos], joined_schema)));
    }

    // JSON / Array containment
    if let Some(pos) = s.find(" @> ") {
        let l = compile_expr_for_joined(&s[..pos], joined_schema);
        let r = compile_expr_for_joined(&s[pos + 4..], joined_schema);
        return CompiledExpr::JsonContains(Box::new(l), Box::new(r));
    }
    if let Some(pos) = s.find(" <@ ") {
        let l = compile_expr_for_joined(&s[..pos], joined_schema);
        let r = compile_expr_for_joined(&s[pos + 4..], joined_schema);
        return CompiledExpr::JsonContained(Box::new(l), Box::new(r));
    }
    if let Some(pos) = crate::engine::executor::find_top_level_op(s, "&&") {
        let l = compile_expr_for_joined(&s[..pos], joined_schema);
        let r = compile_expr_for_joined(&s[pos + 2..], joined_schema);
        return CompiledExpr::ArrayOverlap(Box::new(l), Box::new(r));
    }
    if let Some(pos) = crate::engine::executor::find_top_level_op(s, "?|") {
        let l = compile_expr_for_joined(&s[..pos], joined_schema);
        let r = compile_expr_for_joined(&s[pos + 2..], joined_schema);
        return CompiledExpr::JsonHasAnyKey(Box::new(l), Box::new(r));
    }
    if let Some(pos) = crate::engine::executor::find_top_level_op(s, "?&") {
        let l = compile_expr_for_joined(&s[..pos], joined_schema);
        let r = compile_expr_for_joined(&s[pos + 2..], joined_schema);
        return CompiledExpr::JsonHasAllKeys(Box::new(l), Box::new(r));
    }
    if let Some(pos) = crate::engine::executor::find_top_level_op(s, "?") {
        let l = compile_expr_for_joined(&s[..pos], joined_schema);
        let r = compile_expr_for_joined(&s[pos + 1..], joined_schema);
        return CompiledExpr::JsonHasKey(Box::new(l), Box::new(r));
    }

    // Concat (||)
    if let Some(pipe_idx) = crate::engine::executor::find_top_level_op(s, "||") {
        let l = compile_expr_for_joined(&s[..pipe_idx], joined_schema);
        let r = compile_expr_for_joined(&s[pipe_idx + 2..], joined_schema);
        return CompiledExpr::Concat(Box::new(l), Box::new(r));
    }

    // Arithmetic (+, -, *, /, %)
    if let Some((op_idx, op)) = crate::engine::executor::find_top_level_math_op(s, &['+', '-']) {
        let l = compile_expr_for_joined(&s[..op_idx], joined_schema);
        let r = compile_expr_for_joined(&s[op_idx + 1..], joined_schema);
        return if op == '+' { CompiledExpr::Add(Box::new(l), Box::new(r)) } else { CompiledExpr::Sub(Box::new(l), Box::new(r)) };
    }
    if let Some((op_idx, op)) = crate::engine::executor::find_top_level_math_op(s, &['*', '/', '%']) {
        let l = compile_expr_for_joined(&s[..op_idx], joined_schema);
        let r = compile_expr_for_joined(&s[op_idx + 1..], joined_schema);
        return match op {
            '*' => CompiledExpr::Mul(Box::new(l), Box::new(r)),
            '/' => CompiledExpr::Div(Box::new(l), Box::new(r)),
            '%' => CompiledExpr::Mod(Box::new(l), Box::new(r)),
            _ => unreachable!(),
        };
    }

    // Cast (::)
    if let Some(colon_pos) = crate::engine::executor::find_last_top_level_op(s, "::") {
        let target_expr = compile_expr_for_joined(&s[..colon_pos], joined_schema);
        let target_type = s[colon_pos + 2..].trim().to_uppercase();
        return CompiledExpr::Cast { expr: Box::new(target_expr), target_type };
    }

    // Comparisons
    for op in &[">=", "<=", "!=", "<>", "=", ">", "<"] {
        if let Some(idx) = crate::engine::executor::find_top_level_op(s, op) {
            let l = compile_expr_for_joined(&s[..idx], joined_schema);
            let r = compile_expr_for_joined(&s[idx + op.len()..], joined_schema);
            return match *op {
                "=" => CompiledExpr::Eq(Box::new(l), Box::new(r)),
                "!=" | "<>" => CompiledExpr::NotEq(Box::new(l), Box::new(r)),
                ">=" => CompiledExpr::Gte(Box::new(l), Box::new(r)),
                "<=" => CompiledExpr::Lte(Box::new(l), Box::new(r)),
                ">" => CompiledExpr::Gt(Box::new(l), Box::new(r)),
                "<" => CompiledExpr::Lt(Box::new(l), Box::new(r)),
                _ => unreachable!(),
            };
        }
    }

    // JSON extract operators (->, ->>, #>, #>>)
    if let Some((op_idx, op)) = crate::engine::executor::find_last_top_level_json_op(s) {
        let l = compile_expr_for_joined(&s[..op_idx], joined_schema);
        let r = compile_expr_for_joined(&s[op_idx + op.len()..], joined_schema);
        return match op {
            "->" => CompiledExpr::JsonExtract { lhs: Box::new(l), rhs: Box::new(r), as_text: false },
            "->>" => CompiledExpr::JsonExtract { lhs: Box::new(l), rhs: Box::new(r), as_text: true },
            "#>" => CompiledExpr::JsonExtractPath { lhs: Box::new(l), path: Box::new(r), as_text: false },
            "#>>" => CompiledExpr::JsonExtractPath { lhs: Box::new(l), path: Box::new(r), as_text: true },
            _ => CompiledExpr::Raw(s.to_string()),
        };
    }

    // COALESCE / NULLIF
    if upper.starts_with("COALESCE(") && s.ends_with(')') {
        let inner = &s[9..s.len() - 1];
        let args = crate::engine::executor::split_function_args(inner)
            .into_iter()
            .map(|a| compile_expr_for_joined(&a, joined_schema))
            .collect();
        return CompiledExpr::Coalesce(args);
    }
    if upper.starts_with("NULLIF(") && s.ends_with(')') {
        let inner = &s[7..s.len() - 1];
        let args = crate::engine::executor::split_function_args(inner);
        if args.len() == 2 {
            let a = compile_expr_for_joined(&args[0], joined_schema);
            let b = compile_expr_for_joined(&args[1], joined_schema);
            return CompiledExpr::NullIf(Box::new(a), Box::new(b));
        }
    }

    // Functions (LOWER, UPPER, TRIM)
    if upper.starts_with("LOWER(") && s.ends_with(')') {
        let inner = &s[6..s.len() - 1];
        return CompiledExpr::Lower(Box::new(compile_expr_for_joined(inner, joined_schema)));
    }
    if upper.starts_with("UPPER(") && s.ends_with(')') {
        let inner = &s[6..s.len() - 1];
        return CompiledExpr::Upper(Box::new(compile_expr_for_joined(inner, joined_schema)));
    }
    if (upper.starts_with("TRIM(") || upper.starts_with("BTRIM(")) && s.ends_with(')') {
        let open_p = s.find('(').unwrap();
        let inner = &s[open_p + 1..s.len() - 1];
        return CompiledExpr::Trim(Box::new(compile_expr_for_joined(inner, joined_schema)));
    }
    if upper == "CURRENT_TIMESTAMP" || upper == "NOW()" || upper == "LOCALTIMESTAMP" {
        return CompiledExpr::Now;
    }

    // Parameters
    if s.starts_with('$') {
        if let Ok(idx) = s[1..].parse::<usize>() {
            return CompiledExpr::Param(idx.saturating_sub(1));
        }
    }

    // Literals
    if s.eq_ignore_ascii_case("TRUE") {
        return CompiledExpr::Literal(Value::Bool(true));
    }
    if s.eq_ignore_ascii_case("FALSE") {
        return CompiledExpr::Literal(Value::Bool(false));
    }
    if s.eq_ignore_ascii_case("NULL") {
        return CompiledExpr::Null;
    }
    if s.starts_with('\'') && s.ends_with('\'') && s.len() >= 2 {
        return CompiledExpr::Literal(Value::text(s[1..s.len() - 1].replace("''", "'")));
    }
    if let Ok(i) = s.parse::<i64>() {
        return CompiledExpr::Literal(Value::Int(i));
    }
    if let Ok(f) = s.parse::<f64>() {
        return CompiledExpr::Literal(Value::Float(f));
    }

    // Slot in joined schema
    let clean_all = s.trim().replace('"', "");
    if let Some(slot) = joined_schema.get_slot(&clean_all) {
        return CompiledExpr::Col(slot);
    }
    if let Some(slot) = joined_schema.get_slot(&crate::engine::executor::clean_col_name(s).replace('"', "")) {
        return CompiledExpr::Col(slot);
    }

    CompiledExpr::Raw(s.to_string())
}

fn is_enclosed(s: &str) -> bool {
    let bytes = s.as_bytes();
    if bytes.len() < 2 || bytes[0] != b'(' || bytes[bytes.len() - 1] != b')' {
        return false;
    }
    let mut depth = 0;
    let mut in_str = false;
    for (i, &b) in bytes.iter().enumerate() {
        if b == b'\'' {
            in_str = !in_str;
        } else if !in_str {
            if b == b'(' {
                depth += 1;
            } else if b == b')' {
                depth -= 1;
                if depth == 0 && i < bytes.len() - 1 {
                    return false;
                }
            }
        }
    }
    depth == 0
}

fn eval_func_call(name: &str, args: &[Value]) -> Value {
    match name {
        "ABS" => {
            args.first().and_then(|v| v.as_f64()).map(|f| {
                let abs = f.abs();
                if abs.fract() == 0.0 { Value::Int(abs as i64) } else { Value::Float(abs) }
            }).unwrap_or(Value::Null)
        }
        "FLOOR" => {
            args.first().and_then(|v| v.as_f64()).map(|f| Value::Int(f.floor() as i64)).unwrap_or(Value::Null)
        }
        "CEIL" | "CEILING" => {
            args.first().and_then(|v| v.as_f64()).map(|f| Value::Int(f.ceil() as i64)).unwrap_or(Value::Null)
        }
        "ROUND" => {
            if let Some(f) = args.first().and_then(|v| v.as_f64()) {
                let decimals = args.get(1).and_then(|v| v.as_i64()).unwrap_or(0);
                if decimals == 0 {
                    Value::Int(f.round() as i64)
                } else {
                    let multiplier = 10f64.powi(decimals as i32);
                    Value::Float((f * multiplier).round() / multiplier)
                }
            } else {
                Value::Null
            }
        }
        "MD5" => {
            if let Some(v) = args.first() {
                if v.is_null() { return Value::Null; }
                let mut hasher = Md5::new();
                hasher.update(v.as_str().as_bytes());
                Value::text(format!("{:x}", hasher.finalize()))
            } else {
                Value::Null
            }
        }
        "SHA256" => {
            if let Some(v) = args.first() {
                if v.is_null() { return Value::Null; }
                let mut hasher = Sha256::new();
                hasher.update(v.as_str().as_bytes());
                Value::text(hex::encode(hasher.finalize()))
            } else {
                Value::Null
            }
        }
        _ => Value::Null,
    }
}