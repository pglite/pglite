//! Kiểu range / multirange (đợt 15): `int4range`, `int8range`, `numrange`, `daterange`, `tsrange`, `tstzrange` và
//! các multirange tương ứng.
//!
//! Giá trị được lưu như văn bản chuẩn của PostgreSQL (`[1,5)`, `empty`, `{[1,3),[5,7)}`): range rời rạc (số nguyên,
//! ngày) được chuẩn hoá về dạng `[a,b)` như PG. Toán tử (`@>`, `<@`, `&&`, `<<`, `>>`, `&<`, `&>`, `-|-`, `+`, `*`,
//! `-`, `=`) chỉ được viết lại thành hàm `pg_range_*` khi toán hạng chắc chắn là range (hàm dựng, ép kiểu
//! `::xxxrange`, hoặc cột khai báo kiểu range) — `@>` / `&&` của mảng và jsonb không bị đụng tới.
use crate::types::Value;
use std::cmp::Ordering;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum Kind {
    Int4,
    Int8,
    Num,
    Date,
    Ts,
    Tstz,
}

impl Kind {
    /// (loại, là multirange) theo tên kiểu.
    pub(crate) fn from_name(n: &str) -> Option<(Kind, bool)> {
        let n = n.trim().trim_matches('"').to_ascii_lowercase();
        let (base, multi) = match n.strip_suffix("multirange") {
            Some(b) => (b.to_string(), true),
            None => (n.strip_suffix("range")?.to_string(), false),
        };
        let k = match base.as_str() {
            "int4" => Kind::Int4,
            "int8" => Kind::Int8,
            "num" => Kind::Num,
            "date" => Kind::Date,
            "ts" => Kind::Ts,
            "tstz" => Kind::Tstz,
            _ => return None,
        };
        Some((k, multi))
    }
    fn discrete(self) -> bool {
        matches!(self, Kind::Int4 | Kind::Int8 | Kind::Date)
    }
    pub(crate) fn type_name(self, multi: bool) -> String {
        let b = match self {
            Kind::Int4 => "int4",
            Kind::Int8 => "int8",
            Kind::Num => "num",
            Kind::Date => "date",
            Kind::Ts => "ts",
            Kind::Tstz => "tstz",
        };
        format!("{}{}", b, if multi { "multirange" } else { "range" })
    }
    /// Kiểu PG của phần tử (cho `lower()` / `upper()` trên đường wire).
    pub(crate) fn elem_type(self) -> &'static str {
        match self {
            Kind::Int4 => "integer",
            Kind::Int8 => "bigint",
            Kind::Num => "numeric",
            Kind::Date => "date",
            Kind::Ts => "timestamp without time zone",
            Kind::Tstz => "timestamp with time zone",
        }
    }
}

#[derive(Clone, Debug)]
enum B {
    I(i64),
    /// số và văn bản gốc (in lại đúng như nhập)
    N(f64, String),
    /// micro giây
    T(i64),
}

impl B {
    fn cmp(&self, o: &B) -> Ordering {
        match (self, o) {
            (B::I(a), B::I(b)) => a.cmp(b),
            (B::N(a, _), B::N(b, _)) => a.partial_cmp(b).unwrap_or(Ordering::Equal),
            (B::T(a), B::T(b)) => a.cmp(b),
            _ => Ordering::Equal,
        }
    }
    fn text(&self, k: Kind) -> String {
        match self {
            B::I(v) => v.to_string(),
            B::N(_, t) => t.clone(),
            B::T(us) => super::datetime::fmt_range_point(*us, match k {
                Kind::Date => 0,
                Kind::Ts => 1,
                _ => 2,
            }),
        }
    }
    fn value(&self, k: Kind) -> Value {
        match self {
            B::I(v) => Value::Int(*v),
            _ => Value::text(self.text(k)),
        }
    }
    fn step(&self, k: Kind) -> B {
        match self {
            B::I(v) => B::I(v.saturating_add(1)),
            B::T(us) if k == Kind::Date && *us != i64::MAX && *us != i64::MIN => B::T(us + super::datetime::DAY_MICROS),
            other => other.clone(),
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct Range {
    kind: Kind,
    empty: bool,
    lo: Option<B>,
    hi: Option<B>,
    lo_inc: bool,
    hi_inc: bool,
}

fn parse_bound(s: &str, k: Kind) -> Result<B, String> {
    let t = s.trim();
    let bad = || match k {
        Kind::Int4 => format!("invalid input syntax for type integer: \"{}\"", t),
        Kind::Int8 => format!("invalid input syntax for type bigint: \"{}\"", t),
        Kind::Num => format!("invalid input syntax for type numeric: \"{}\"", t),
        Kind::Date => format!("invalid input syntax for type date: \"{}\"", t),
        Kind::Ts => format!("invalid input syntax for type timestamp: \"{}\"", t),
        Kind::Tstz => format!("invalid input syntax for type timestamp with time zone: \"{}\"", t),
    };
    match k {
        Kind::Int4 | Kind::Int8 => {
            let v: i64 = t.parse().map_err(|_| bad())?;
            if k == Kind::Int4 && (v > i32::MAX as i64 || v < i32::MIN as i64) {
                return Err(format!("value \"{}\" is out of range for type integer", t));
            }
            Ok(B::I(v))
        }
        Kind::Num => {
            let v: f64 = t.parse().map_err(|_| bad())?;
            Ok(B::N(v, t.to_string()))
        }
        Kind::Date => {
            let us = super::datetime::range_point(t, false).ok_or_else(bad)?;
            // ngày: bỏ phần giờ
            Ok(B::T(if us == i64::MAX || us == i64::MIN { us } else { us.div_euclid(super::datetime::DAY_MICROS) * super::datetime::DAY_MICROS }))
        }
        Kind::Ts => Ok(B::T(super::datetime::range_point(t, false).ok_or_else(bad)?)),
        Kind::Tstz => Ok(B::T(super::datetime::range_point(t, true).ok_or_else(bad)?)),
    }
}

fn quote_bound(s: &str) -> String {
    if s.is_empty() || s.chars().any(|c| matches!(c, ' ' | ',' | '"' | '\\' | '(' | ')' | '[' | ']')) {
        format!("\"{}\"", s.replace('\\', "\\\\").replace('"', "\\\""))
    } else {
        s.to_string()
    }
}

impl Range {
    fn empty(kind: Kind) -> Range {
        Range { kind, empty: true, lo: None, hi: None, lo_inc: false, hi_inc: false }
    }

    /// Dựng range và chuẩn hoá như `make_range` của PG.
    fn make(kind: Kind, lo: Option<B>, hi: Option<B>, lo_inc: bool, hi_inc: bool) -> Result<Range, String> {
        if let (Some(a), Some(b)) = (&lo, &hi) {
            if a.cmp(b) == Ordering::Greater {
                return Err("range lower bound must be less than or equal to range upper bound".to_string());
            }
        }
        let mut r = Range { kind, empty: false, lo_inc: lo_inc && lo.is_some(), hi_inc: hi_inc && hi.is_some(), lo, hi };
        if kind.discrete() {
            if let Some(l) = &r.lo {
                if !r.lo_inc {
                    r.lo = Some(l.step(kind));
                    r.lo_inc = true;
                }
            }
            if let Some(h) = &r.hi {
                if r.hi_inc {
                    r.hi = Some(h.step(kind));
                    r.hi_inc = false;
                }
            }
        }
        if let (Some(a), Some(b)) = (&r.lo, &r.hi) {
            match a.cmp(b) {
                Ordering::Greater => return Ok(Range::empty(kind)),
                Ordering::Equal if !(r.lo_inc && r.hi_inc) => return Ok(Range::empty(kind)),
                _ => {}
            }
        }
        Ok(r)
    }

    pub(crate) fn parse(s: &str, kind: Kind) -> Result<Range, String> {
        let t = s.trim();
        if t.eq_ignore_ascii_case("empty") {
            return Ok(Range::empty(kind));
        }
        let malformed = || format!("malformed range literal: \"{}\"", t);
        let lo_inc = match t.chars().next() {
            Some('[') => true,
            Some('(') => false,
            _ => return Err(malformed()),
        };
        let hi_inc = match t.chars().last() {
            Some(']') => true,
            Some(')') => false,
            _ => return Err(malformed()),
        };
        let inner = &t[1..t.len() - 1];
        // tách tại dấu phẩy ngoài nháy kép
        let mut parts = Vec::new();
        let mut cur = String::new();
        let mut quoted = false;
        let mut was_quoted = false;
        let mut it = inner.chars().peekable();
        while let Some(c) = it.next() {
            match c {
                '"' if quoted && it.peek() == Some(&'"') => {
                    cur.push('"');
                    it.next();
                }
                '"' => {
                    quoted = !quoted;
                    was_quoted = true;
                }
                '\\' => {
                    if let Some(n) = it.next() {
                        cur.push(n);
                    }
                }
                ',' if !quoted => {
                    parts.push((std::mem::take(&mut cur), was_quoted));
                    was_quoted = false;
                }
                c => cur.push(c),
            }
        }
        parts.push((cur, was_quoted));
        if parts.len() != 2 {
            return Err(malformed());
        }
        let bound = |p: &(String, bool)| -> Result<Option<B>, String> {
            if p.0.trim().is_empty() && !p.1 {
                Ok(None)
            } else {
                parse_bound(&p.0, kind).map(Some)
            }
        };
        Range::make(kind, bound(&parts[0])?, bound(&parts[1])?, lo_inc, hi_inc)
    }

    pub(crate) fn text(&self) -> String {
        if self.empty {
            return "empty".to_string();
        }
        format!(
            "{}{},{}{}",
            if self.lo_inc { '[' } else { '(' },
            self.lo.as_ref().map(|b| quote_bound(&b.text(self.kind))).unwrap_or_default(),
            self.hi.as_ref().map(|b| quote_bound(&b.text(self.kind))).unwrap_or_default(),
            if self.hi_inc { ']' } else { ')' }
        )
    }

    /// So sánh biên dưới (None = -∞; cùng giá trị: biên đóng đứng trước).
    fn cmp_lower(&self, o: &Range) -> Ordering {
        match (&self.lo, &o.lo) {
            (None, None) => Ordering::Equal,
            (None, _) => Ordering::Less,
            (_, None) => Ordering::Greater,
            (Some(a), Some(b)) => a.cmp(b).then_with(|| o.lo_inc.cmp(&self.lo_inc)),
        }
    }
    /// So sánh biên trên (None = +∞; cùng giá trị: biên mở đứng trước).
    fn cmp_upper(&self, o: &Range) -> Ordering {
        match (&self.hi, &o.hi) {
            (None, None) => Ordering::Equal,
            (None, _) => Ordering::Greater,
            (_, None) => Ordering::Less,
            (Some(a), Some(b)) => a.cmp(b).then_with(|| self.hi_inc.cmp(&o.hi_inc)),
        }
    }
}

/// Biên dưới `lo` không vượt biên trên `hi` (hai range có điểm chung ở chỗ này).
fn lower_le_upper(lo: &Option<B>, lo_inc: bool, hi: &Option<B>, hi_inc: bool) -> bool {
    match (lo, hi) {
        (Some(l), Some(h)) => match l.cmp(h) {
            Ordering::Less => true,
            Ordering::Equal => lo_inc && hi_inc,
            Ordering::Greater => false,
        },
        _ => true,
    }
}

fn overlaps(a: &Range, b: &Range) -> bool {
    !a.empty && !b.empty && lower_le_upper(&a.lo, a.lo_inc, &b.hi, b.hi_inc) && lower_le_upper(&b.lo, b.lo_inc, &a.hi, a.hi_inc)
}

fn adjacent(a: &Range, b: &Range) -> bool {
    if a.empty || b.empty {
        return false;
    }
    let touch = |h: &Option<B>, h_inc: bool, l: &Option<B>, l_inc: bool| match (h, l) {
        (Some(h), Some(l)) => h.cmp(l) == Ordering::Equal && h_inc != l_inc,
        _ => false,
    };
    touch(&a.hi, a.hi_inc, &b.lo, b.lo_inc) || touch(&b.hi, b.hi_inc, &a.lo, a.lo_inc)
}

fn contains_elem(r: &Range, e: &B) -> bool {
    if r.empty {
        return false;
    }
    let lo_ok = match &r.lo {
        None => true,
        Some(l) => match l.cmp(e) {
            Ordering::Less => true,
            Ordering::Equal => r.lo_inc,
            Ordering::Greater => false,
        },
    };
    let hi_ok = match &r.hi {
        None => true,
        Some(h) => match e.cmp(h) {
            Ordering::Less => true,
            Ordering::Equal => r.hi_inc,
            Ordering::Greater => false,
        },
    };
    lo_ok && hi_ok
}

fn contains_range(a: &Range, b: &Range) -> bool {
    if b.empty {
        return true;
    }
    if a.empty {
        return false;
    }
    a.cmp_lower(b) != Ordering::Greater && a.cmp_upper(b) != Ordering::Less
}

fn union(a: &Range, b: &Range, strict: bool) -> Result<Range, String> {
    if a.empty {
        return Ok(b.clone());
    }
    if b.empty {
        return Ok(a.clone());
    }
    if strict && !overlaps(a, b) && !adjacent(a, b) {
        return Err("result of range union would not be contiguous".to_string());
    }
    let (lo, lo_inc) = if a.cmp_lower(b) != Ordering::Greater { (a.lo.clone(), a.lo_inc) } else { (b.lo.clone(), b.lo_inc) };
    let (hi, hi_inc) = if a.cmp_upper(b) != Ordering::Less { (a.hi.clone(), a.hi_inc) } else { (b.hi.clone(), b.hi_inc) };
    Range::make(a.kind, lo, hi, lo_inc, hi_inc)
}

fn intersect(a: &Range, b: &Range) -> Result<Range, String> {
    if !overlaps(a, b) {
        return Ok(Range::empty(a.kind));
    }
    let (lo, lo_inc) = if a.cmp_lower(b) == Ordering::Greater { (a.lo.clone(), a.lo_inc) } else { (b.lo.clone(), b.lo_inc) };
    let (hi, hi_inc) = if a.cmp_upper(b) == Ordering::Less { (a.hi.clone(), a.hi_inc) } else { (b.hi.clone(), b.hi_inc) };
    Range::make(a.kind, lo, hi, lo_inc, hi_inc)
}

fn minus(a: &Range, b: &Range) -> Result<Range, String> {
    if a.empty || b.empty || !overlaps(a, b) {
        return Ok(a.clone());
    }
    let l = a.cmp_lower(b);
    let u = a.cmp_upper(b);
    if l == Ordering::Less && u == Ordering::Greater {
        return Err("result of range difference would not be contiguous".to_string());
    }
    if l != Ordering::Less && u != Ordering::Greater {
        return Ok(Range::empty(a.kind));
    }
    if l == Ordering::Less {
        Range::make(a.kind, a.lo.clone(), b.lo.clone(), a.lo_inc, !b.lo_inc)
    } else {
        Range::make(a.kind, b.hi.clone(), a.hi.clone(), !b.hi_inc, a.hi_inc)
    }
}

// ---------------------------------------------------------------------------------------------------------------
// Multirange

fn parse_multi(s: &str, kind: Kind) -> Result<Vec<Range>, String> {
    let t = s.trim();
    if !(t.starts_with('{') && t.ends_with('}')) {
        return Err(format!("malformed multirange literal: \"{}\"", t));
    }
    let inner = &t[1..t.len() - 1];
    let mut out = Vec::new();
    let mut start = None;
    let mut quoted = false;
    for (i, c) in inner.char_indices() {
        match c {
            '"' => quoted = !quoted,
            '[' | '(' if !quoted && start.is_none() => start = Some(i),
            ']' | ')' if !quoted => {
                if let Some(st) = start.take() {
                    out.push(Range::parse(&inner[st..=i], kind)?);
                }
            }
            _ => {}
        }
    }
    for w in inner.split(',') {
        if w.trim().eq_ignore_ascii_case("empty") {
            continue;
        }
    }
    Ok(normalize_multi(out, kind))
}

/// Bỏ range rỗng, sắp theo biên dưới, gộp các range chồng / kề nhau.
fn normalize_multi(mut v: Vec<Range>, kind: Kind) -> Vec<Range> {
    v.retain(|r| !r.empty);
    v.sort_by(|a, b| a.cmp_lower(b));
    let mut out: Vec<Range> = Vec::new();
    for r in v {
        if let Some(last) = out.last_mut() {
            if overlaps(last, &r) || adjacent(last, &r) {
                if let Ok(u) = union(last, &r, false) {
                    *last = u;
                    continue;
                }
            }
        }
        out.push(r);
    }
    let _ = kind;
    out
}

fn multi_text(v: &[Range]) -> String {
    format!("{{{}}}", v.iter().map(|r| r.text()).collect::<Vec<_>>().join(","))
}

// ---------------------------------------------------------------------------------------------------------------
// Hàm (gọi từ fnlib)

fn text_of(v: &Value) -> Option<String> {
    match v {
        Value::Null => None,
        Value::Text(t) => Some(t.to_string()),
        Value::Int(i) => Some(i.to_string()),
        Value::Float(f) => Some(f.to_string()),
        Value::Bool(b) => Some(b.to_string()),
    }
}

/// Loại range đoán từ văn bản của giá trị (khi hàm không mang tên kiểu): số nguyên → int8, số → num,
/// ngày → date, còn lại timestamp.
fn guess_kind(r: &str) -> Kind {
    let inner = r.trim().trim_start_matches(['[', '(', '{']).trim_end_matches([']', ')', '}']);
    let first = inner.split(',').map(|p| p.trim().trim_matches('"')).find(|p| !p.is_empty()).unwrap_or("");
    let t = r.trim();
    // range số nguyên đã chuẩn hoá luôn có dạng `[a,b)` / `(,b)`: biên `]` hoặc `(a` là range liên tục
    let continuous = t.ends_with(']') || (t.starts_with('(') && !t[1..].trim_start().starts_with(','));
    if first.parse::<i64>().is_ok() && !continuous {
        Kind::Int8
    } else if first.parse::<f64>().is_ok() {
        Kind::Num
    } else if first.len() == 10 && first.as_bytes().get(4) == Some(&b'-') {
        Kind::Date
    } else if first.contains('+') || first.ends_with('Z') || first.rfind('-').is_some_and(|p| p > 10) {
        Kind::Tstz
    } else {
        Kind::Ts
    }
}

fn range_arg(v: &Value, kind: Option<Kind>) -> Result<Option<Range>, String> {
    let Some(t) = text_of(v) else { return Ok(None) };
    let k = kind.unwrap_or_else(|| guess_kind(&t));
    Range::parse(&t, k).map(Some)
}

pub(crate) fn is_range_fn(name: &str) -> bool {
    if !name.contains("RANGE") && !matches!(name, "ISEMPTY" | "LOWER_INC" | "UPPER_INC" | "LOWER_INF" | "UPPER_INF") {
        return false;
    }
    matches!(
        name,
        "INT4RANGE" | "INT8RANGE" | "NUMRANGE" | "DATERANGE" | "TSRANGE" | "TSTZRANGE" | "INT4MULTIRANGE" | "INT8MULTIRANGE" | "NUMMULTIRANGE"
            | "DATEMULTIRANGE" | "TSMULTIRANGE" | "TSTZMULTIRANGE" | "ISEMPTY" | "LOWER_INC" | "UPPER_INC" | "LOWER_INF" | "UPPER_INF" | "RANGE_MERGE"
            | "PG_RANGE_IN" | "PG_RANGE_OP" | "PG_RANGE_SET" | "PG_RANGE_FN"
    )
}

pub(crate) fn range_fn_type(name: &str) -> Option<&'static str> {
    Some(match name {
        "INT4RANGE" => "int4range",
        "INT8RANGE" => "int8range",
        "NUMRANGE" => "numrange",
        "DATERANGE" => "daterange",
        "TSRANGE" => "tsrange",
        "TSTZRANGE" => "tstzrange",
        "INT4MULTIRANGE" => "int4multirange",
        "INT8MULTIRANGE" => "int8multirange",
        "NUMMULTIRANGE" => "nummultirange",
        "DATEMULTIRANGE" => "datemultirange",
        "TSMULTIRANGE" => "tsmultirange",
        "TSTZMULTIRANGE" => "tstzmultirange",
        "ISEMPTY" | "LOWER_INC" | "UPPER_INC" | "LOWER_INF" | "UPPER_INF" | "PG_RANGE_OP" => "boolean",
        _ => return None,
    })
}

fn fail(e: String) -> Option<Value> {
    super::errors::raise(e);
    Some(Value::Null)
}

pub(crate) fn call_range_fn(name: &str, args: &[Value]) -> Option<Value> {
    let lname = name.to_ascii_lowercase();
    if let Some((kind, multi)) = Kind::from_name(&lname) {
        if multi {
            // int4multirange(r1, r2, ...)
            let mut v = Vec::new();
            for a in args {
                match range_arg(a, Some(kind)) {
                    Ok(Some(r)) => v.push(r),
                    Ok(None) => return Some(Value::Null),
                    Err(e) => return fail(e),
                }
            }
            return Some(Value::text(multi_text(&normalize_multi(v, kind))));
        }
        // int4range(lo, hi [, '[)'])
        let bounds = args.get(2).and_then(text_of).unwrap_or_else(|| "[)".to_string());
        if bounds.len() != 2 || !"[(".contains(&bounds[..1]) || !")]".contains(&bounds[1..]) {
            return fail("invalid range bound flags".to_string());
        }
        let b = |i: usize| -> Result<Option<B>, String> {
            match args.get(i).and_then(text_of) {
                None => Ok(None),
                Some(t) => parse_bound(&t, kind).map(Some),
            }
        };
        let r = match (b(0), b(1)) {
            (Ok(lo), Ok(hi)) => Range::make(kind, lo, hi, bounds.starts_with('['), bounds.ends_with(']')),
            (Err(e), _) | (_, Err(e)) => Err(e),
        };
        return Some(match r {
            Ok(r) => Value::text(r.text()),
            Err(e) => return fail(e),
        });
    }
    match name {
        // 'văn bản'::xxxrange
        "PG_RANGE_IN" => {
            let t = text_of(args.first()?)?;
            let ty = text_of(args.get(1)?)?;
            let (kind, multi) = Kind::from_name(&ty)?;
            Some(if multi {
                match parse_multi(&t, kind) {
                    Ok(v) => Value::text(multi_text(&v)),
                    Err(e) => return fail(e),
                }
            } else {
                match Range::parse(&t, kind) {
                    Ok(r) => Value::text(r.text()),
                    Err(e) => return fail(e),
                }
            })
        }
        "ISEMPTY" | "LOWER_INC" | "UPPER_INC" | "LOWER_INF" | "UPPER_INF" => {
            let r = match range_arg(args.first()?, None) {
                Ok(Some(r)) => r,
                Ok(None) => return Some(Value::Null),
                Err(e) => return fail(e),
            };
            Some(Value::Bool(match name {
                "ISEMPTY" => r.empty,
                "LOWER_INC" => !r.empty && r.lo_inc,
                "UPPER_INC" => !r.empty && r.hi_inc,
                "LOWER_INF" => !r.empty && r.lo.is_none(),
                _ => !r.empty && r.hi.is_none(),
            }))
        }
        "RANGE_MERGE" => {
            let (Ok(Some(a)), Some(bv)) = (range_arg(args.first()?, None), args.get(1)) else { return Some(Value::Null) };
            let Ok(Some(b)) = range_arg(bv, Some(a.kind)) else { return Some(Value::Null) };
            match union(&a, &b, false) {
                Ok(r) => Some(Value::text(r.text())),
                Err(e) => fail(e),
            }
        }
        // pg_range_fn('lower' | 'upper', r)
        "PG_RANGE_FN" => {
            let f = text_of(args.first()?)?;
            let r = match range_arg(args.get(1)?, None) {
                Ok(Some(r)) => r,
                _ => return Some(Value::Null),
            };
            if r.empty {
                return Some(Value::Null);
            }
            let b = if f == "lower" { &r.lo } else { &r.hi };
            Some(b.as_ref().map(|b| b.value(r.kind)).unwrap_or(Value::Null))
        }
        // pg_range_op('@>', a, b) → boolean; pg_range_set('+', a, b) → range
        "PG_RANGE_OP" | "PG_RANGE_SET" => {
            let op = text_of(args.first()?)?;
            let (av, bv) = (args.get(1)?, args.get(2)?);
            if av.is_null() || bv.is_null() {
                return Some(Value::Null);
            }
            let at = text_of(av)?;
            let bt = text_of(bv)?;
            let a_multi = at.trim_start().starts_with('{');
            let b_multi = bt.trim_start().starts_with('{');
            let is_range = |t: &str| {
                let t = t.trim();
                t.eq_ignore_ascii_case("empty") || (t.starts_with(['[', '(']) && t.ends_with([']', ')']) && t.contains(','))
            };
            // multirange: đưa về danh sách range
            if a_multi || b_multi {
                let kind = guess_kind(if a_multi { &at } else { &bt });
                let ma = if a_multi { parse_multi(&at, kind) } else { Ok(vec![]) };
                let mb = if b_multi { parse_multi(&bt, kind) } else { Ok(vec![]) };
                let (Ok(ma), Ok(mb)) = (ma, mb) else { return Some(Value::Null) };
                let res = match op.as_str() {
                    "@>" if a_multi && !b_multi && is_range(&bt) => Range::parse(&bt, kind).ok().map(|r| ma.iter().any(|x| contains_range(x, &r))),
                    "@>" if a_multi && !b_multi => parse_bound(&bt, kind).ok().map(|e| ma.iter().any(|x| contains_elem(x, &e))),
                    "<@" if b_multi && !a_multi && is_range(&at) => Range::parse(&at, kind).ok().map(|r| mb.iter().any(|x| contains_range(x, &r))),
                    "<@" if b_multi && !a_multi => parse_bound(&at, kind).ok().map(|e| mb.iter().any(|x| contains_elem(x, &e))),
                    "&&" => {
                        let la = if a_multi { ma.clone() } else { Range::parse(&at, kind).map(|r| vec![r]).unwrap_or_default() };
                        let lb = if b_multi { mb.clone() } else { Range::parse(&bt, kind).map(|r| vec![r]).unwrap_or_default() };
                        Some(la.iter().any(|x| lb.iter().any(|y| overlaps(x, y))))
                    }
                    "=" => Some(multi_text(&ma) == multi_text(&mb)),
                    "<>" => Some(multi_text(&ma) != multi_text(&mb)),
                    _ => None,
                };
                return Some(res.map(Value::Bool).unwrap_or(Value::Null));
            }
            let kind = guess_kind(if is_range(&at) { &at } else { &bt });
            let ra = if is_range(&at) { Range::parse(&at, kind).ok() } else { None };
            let rb = if is_range(&bt) { Range::parse(&bt, kind).ok() } else { None };
            if name == "PG_RANGE_SET" {
                let (Some(a), Some(b)) = (ra, rb) else { return Some(Value::Null) };
                let r = match op.as_str() {
                    "+" => union(&a, &b, true),
                    "*" => intersect(&a, &b),
                    "-" => minus(&a, &b),
                    _ => return Some(Value::Null),
                };
                return match r {
                    Ok(r) => Some(Value::text(r.text())),
                    Err(e) => fail(e),
                };
            }
            let res = match (op.as_str(), &ra, &rb) {
                ("@>", Some(a), Some(b)) => contains_range(a, b),
                ("@>", Some(a), None) => match parse_bound(&bt, a.kind) {
                    Ok(e) => contains_elem(a, &e),
                    Err(e) => return fail(e),
                },
                ("<@", Some(a), Some(b)) => contains_range(b, a),
                ("<@", None, Some(b)) => match parse_bound(&at, b.kind) {
                    Ok(e) => contains_elem(b, &e),
                    Err(e) => return fail(e),
                },
                ("&&", Some(a), Some(b)) => overlaps(a, b),
                ("<<", Some(a), Some(b)) => !a.empty && !b.empty && !lower_le_upper(&b.lo, b.lo_inc, &a.hi, a.hi_inc),
                (">>", Some(a), Some(b)) => !a.empty && !b.empty && !lower_le_upper(&a.lo, a.lo_inc, &b.hi, b.hi_inc),
                ("&<", Some(a), Some(b)) => !a.empty && !b.empty && a.cmp_upper(b) != Ordering::Greater,
                ("&>", Some(a), Some(b)) => !a.empty && !b.empty && a.cmp_lower(b) != Ordering::Less,
                ("-|-", Some(a), Some(b)) => adjacent(a, b),
                ("=", Some(a), Some(b)) => a.text() == b.text(),
                ("<>", Some(a), Some(b)) => a.text() != b.text(),
                _ => return Some(Value::Null),
            };
            Some(Value::Bool(res))
        }
        _ => None,
    }
}

// ---------------------------------------------------------------------------------------------------------------
// Viết lại cú pháp

const RANGE_TYPES: &[&str] = &[
    "int4range", "int8range", "numrange", "daterange", "tsrange", "tstzrange", "int4multirange", "int8multirange", "nummultirange", "datemultirange",
    "tsmultirange", "tstzmultirange",
];

/// Câu có thể dùng range: nhắc tên kiểu range, hoặc có cột range (`has_range_cols`) và một toán tử / hàm range.
pub(crate) fn may_need(sql: &str, has_range_cols: bool) -> bool {
    if super::executor::contains_ci(sql, "range") {
        return true;
    }
    has_range_cols && ["@>", "<@", "&&", "-|-", "<<", ">>", "&<", "&>", "+", "*", "-"].iter().any(|p| sql.contains(p)) || has_range_cols && (super::executor::contains_ci(sql, "lower(") || super::executor::contains_ci(sql, "upper("))
}

/// Toán hạng (văn bản) chắc chắn là range.
fn is_range_operand(e: &str, is_range_col: &dyn Fn(&str) -> bool) -> bool {
    let t = e.trim();
    let l = t.to_ascii_lowercase();
    if l.starts_with("pg_range_in(") || l.starts_with("pg_range_set(") {
        return true;
    }
    if let Some(p) = l.find('(') {
        let name = l[..p].trim();
        if RANGE_TYPES.contains(&name) || name == "range_merge" {
            return true;
        }
    }
    if t.starts_with('(') && super::constraints::matching_paren(t, 0) == Some(t.len() - 1) {
        return is_range_operand(&t[1..t.len() - 1], is_range_col);
    }
    if t.chars().all(|c| c.is_alphanumeric() || c == '_' || c == '.' || c == '"') {
        let col = t.rsplit('.').next().unwrap_or(t).trim_matches('"');
        return is_range_col(col);
    }
    false
}

/// Viết lại ép kiểu `x::int4range`, toán tử và `lower()` / `upper()` trên range. `None` nếu không đổi gì.
pub(crate) fn rewrite(sql: &str, is_range_col: &dyn Fn(&str) -> bool) -> Option<String> {
    use super::oprewrite::{code_mask, primary_end, primary_start};
    let mut s = sql.to_string();
    let mut changed = false;
    // 1. ép kiểu `X::xxxrange` → pg_range_in(X, 'xxxrange')
    loop {
        let m = code_mask(&s);
        let lower = s.to_ascii_lowercase();
        let mut found = None;
        for ty in RANGE_TYPES {
            let pat = format!("::{}", ty);
            let mut from = 0;
            while let Some(p) = lower[from..].find(&pat).map(|p| p + from) {
                from = p + pat.len();
                let end = p + pat.len();
                if !m[p] || lower.as_bytes().get(end).is_some_and(|c| c.is_ascii_alphanumeric() || *c == b'_') {
                    continue;
                }
                if found.map_or(true, |(fp, _, _)| p < fp) {
                    found = Some((p, end, *ty));
                }
                break;
            }
        }
        let Some((p, end, ty)) = found else { break };
        let Some(a) = primary_start(&s, &m, p) else { break };
        let rep = format!("pg_range_in({}, '{}')", &s[a..p], ty);
        s.replace_range(a..end, &rep);
        changed = true;
    }
    // 2. lower(X) / upper(X) với X là range
    for f in ["lower", "upper"] {
        let mut from = 0;
        loop {
            let m = code_mask(&s);
            let lower = s.to_ascii_lowercase();
            let Some(p) = lower[from..].find(&format!("{}(", f)).map(|p| p + from) else { break };
            from = p + f.len() + 1;
            if !m[p] || (p > 0 && (lower.as_bytes()[p - 1].is_ascii_alphanumeric() || lower.as_bytes()[p - 1] == b'_' || lower.as_bytes()[p - 1] == b'.')) {
                continue;
            }
            let open = p + f.len();
            let Some(close) = super::constraints::matching_paren(&s, open) else { continue };
            let arg = s[open + 1..close].to_string();
            if is_range_operand(&arg, is_range_col) {
                let rep = format!("pg_range_fn('{}', {})", f, arg);
                s.replace_range(p..close + 1, &rep);
                changed = true;
                from = p + rep.len();
            }
        }
    }
    // 3. toán tử hai ngôi
    // `=` / `<>` không cần: hai range đã chuẩn hoá so sánh như văn bản
    const OPS: &[&str] = &["-|-", "@>", "<@", "&&", "<<", ">>", "&<", "&>", "+", "*", "-"];
    let mut guard = 0;
    'outer: loop {
        guard += 1;
        if guard > 200 {
            break;
        }
        let m = code_mask(&s);
        let b = s.as_bytes();
        let mut i = 0;
        while i < b.len() {
            if !m[i] {
                i += 1;
                continue;
            }
            let Some(op) = OPS.iter().find(|op| s[i..].starts_with(**op)) else {
                i += 1;
                continue;
            };
            let opl = op.len();
            // không phải một phần của toán tử dài hơn (`<=`, `>=`, `->`, `->>`, `||`, `::`)
            let prev = if i > 0 { b[i - 1] } else { b' ' };
            let next = b.get(i + opl).copied().unwrap_or(b' ');
            let single = opl == 1;
            if single && (matches!(prev, b'<' | b'>' | b'!' | b'=' | b'-' | b'|' | b'&' | b'@' | b'*' | b'+' | b'#') || matches!(next, b'>' | b'=' | b'<' | b'|' | b'-' | b'@' | b'&' | b'*')) {
                i += 1;
                continue;
            }
            if (*op == "<<" || *op == ">>") && (next == b'=' || prev == b'<' || prev == b'>') {
                i += 2;
                continue;
            }
            let (Some(a), Some(e)) = (primary_start(&s, &m, i), primary_end(&s, &m, i + opl)) else {
                i += opl;
                continue;
            };
            let left = s[a..i].trim().to_string();
            let right = s[i + opl..e].trim().to_string();
            let lr = is_range_operand(&left, is_range_col);
            let rr = is_range_operand(&right, is_range_col);
            let ok = match *op {
                "@>" => lr,
                "<@" => rr,
                "+" | "*" | "-" => lr && rr,
                _ => lr || rr,
            };
            if !ok {
                i += opl;
                continue;
            }
            let func = if matches!(*op, "+" | "*" | "-") { "pg_range_set" } else { "pg_range_op" };
            let rep = format!("{}('{}', {}, {})", func, op, left, right);
            s.replace_range(a..e, &rep);
            changed = true;
            continue 'outer;
        }
        break;
    }
    changed.then_some(s)
}
