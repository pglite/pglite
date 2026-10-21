//! SQL/JSON path (`jsonpath`) của PostgreSQL: dùng cho `jsonb_path_query[_array|_first]`, `jsonb_path_exists`,
//! `jsonb_path_match`, toán tử `@?` / `@@` và `JSON_VALUE` / `JSON_QUERY` / `JSON_EXISTS`.
//!
//! Hỗ trợ: `lax` (mặc định) / `strict`, `$`, `@`, `$biến`, `.key`, `."key"`, `.*`, `[n]`, `[a to b]`, `[last]`,
//! `[*]`, `.**` không hỗ trợ; bộ lọc `?(...)` với `== != <> < <= > >=`, `&&`, `||`, `!`, `exists(...)`,
//! `like_regex "..." [flag "..."]`, `starts with`, `is unknown`; số học `+ - * / %` và dấu âm; phương thức
//! `size() type() double() number() abs() floor() ceiling() keyvalue() string() boolean()`.
//! Chế độ lax tự "mở" mảng khi truy cập khoá và bỏ qua khoá thiếu; strict báo lỗi.
use serde_json::Value as J;

#[derive(Debug, Clone)]
enum Step {
    Key(String),
    AnyKey,
    Index(Vec<(Expr, Option<Expr>)>),
    AnyIndex,
    Filter(Box<Pred>),
    Method(String),
}

#[derive(Debug, Clone)]
enum Base {
    Root,
    Current,
    Var(String),
    Lit(J),
    Last,
}

#[derive(Debug, Clone)]
enum Expr {
    Path(Base, Vec<Step>),
    Bin(char, Box<Expr>, Box<Expr>),
    Neg(Box<Expr>),
    /// biểu thức điều kiện dùng như giá trị (`$.a == 1` ở mức ngoài cùng)
    Pred(Box<Pred>),
}

#[derive(Debug, Clone)]
enum Pred {
    Cmp(String, Expr, Expr),
    And(Box<Pred>, Box<Pred>),
    Or(Box<Pred>, Box<Pred>),
    Not(Box<Pred>),
    Exists(Expr),
    LikeRegex(Expr, String, String),
    StartsWith(Expr, Expr),
    IsUnknown(Box<Pred>),
}

/// Đường dẫn đã biên dịch.
#[derive(Debug, Clone)]
pub(crate) struct JsonPath {
    strict: bool,
    expr: Expr,
}

struct P<'a> {
    s: &'a [u8],
    src: &'a str,
    i: usize,
}

impl<'a> P<'a> {
    fn ws(&mut self) {
        while self.i < self.s.len() && self.s[self.i].is_ascii_whitespace() {
            self.i += 1;
        }
    }
    fn peek(&mut self) -> Option<u8> {
        self.ws();
        self.s.get(self.i).copied()
    }
    fn eat(&mut self, t: &str) -> bool {
        self.ws();
        if self.src[self.i..].starts_with(t) {
            self.i += t.len();
            true
        } else {
            false
        }
    }
    fn eat_kw(&mut self, kw: &str) -> bool {
        self.ws();
        let r = &self.src[self.i..];
        if r.len() >= kw.len() && r[..kw.len()].eq_ignore_ascii_case(kw) && r[kw.len()..].chars().next().map_or(true, |c| !(c.is_alphanumeric() || c == '_')) {
            self.i += kw.len();
            true
        } else {
            false
        }
    }
    fn ident(&mut self) -> Option<String> {
        self.ws();
        let st = self.i;
        while self.i < self.s.len() && (self.s[self.i].is_ascii_alphanumeric() || self.s[self.i] == b'_' || self.s[self.i] >= 0x80) {
            self.i += 1;
        }
        (self.i > st).then(|| self.src[st..self.i].to_string())
    }
    fn string(&mut self) -> Result<String, String> {
        self.ws();
        if self.s.get(self.i) != Some(&b'"') {
            return Err("syntax error in jsonpath: expected string".into());
        }
        self.i += 1;
        let mut out = String::new();
        let mut chars = self.src[self.i..].char_indices();
        while let Some((k, c)) = chars.next() {
            match c {
                '"' => {
                    self.i += k + 1;
                    return Ok(out);
                }
                '\\' => match chars.next() {
                    Some((_, 'n')) => out.push('\n'),
                    Some((_, 't')) => out.push('\t'),
                    Some((_, 'r')) => out.push('\r'),
                    Some((_, 'u')) => {
                        let h: String = (0..4).filter_map(|_| chars.next().map(|x| x.1)).collect();
                        out.push(u32::from_str_radix(&h, 16).ok().and_then(char::from_u32).unwrap_or('\u{fffd}'));
                    }
                    Some((_, o)) => out.push(o),
                    None => break,
                },
                c => out.push(c),
            }
        }
        Err("syntax error in jsonpath: unterminated string".into())
    }
    fn number(&mut self) -> Option<J> {
        self.ws();
        let st = self.i;
        while self.i < self.s.len() && (self.s[self.i].is_ascii_digit() || self.s[self.i] == b'.' || self.s[self.i] == b'e' || self.s[self.i] == b'E' || ((self.s[self.i] == b'+' || self.s[self.i] == b'-') && self.i > st && matches!(self.s[self.i - 1], b'e' | b'E'))) {
            // `1.a` không phải số: dừng trước dấu chấm theo sau là chữ
            if self.s[self.i] == b'.' && self.s.get(self.i + 1).is_some_and(|c| c.is_ascii_alphabetic()) {
                break;
            }
            self.i += 1;
        }
        let t = &self.src[st..self.i];
        if t.is_empty() {
            return None;
        }
        if let Ok(i) = t.parse::<i64>() {
            return Some(J::from(i));
        }
        t.parse::<f64>().ok().and_then(serde_json::Number::from_f64).map(J::Number)
    }

    fn pred(&mut self) -> Result<Pred, String> {
        let mut l = self.pred_and()?;
        while self.eat("||") {
            let r = self.pred_and()?;
            l = Pred::Or(Box::new(l), Box::new(r));
        }
        Ok(l)
    }
    fn pred_and(&mut self) -> Result<Pred, String> {
        let mut l = self.pred_not()?;
        while self.eat("&&") {
            let r = self.pred_not()?;
            l = Pred::And(Box::new(l), Box::new(r));
        }
        Ok(l)
    }
    fn pred_not(&mut self) -> Result<Pred, String> {
        if self.peek() == Some(b'!') && self.s.get(self.i + 1) != Some(&b'=') {
            self.i += 1;
            return Ok(Pred::Not(Box::new(self.pred_not()?)));
        }
        self.pred_atom()
    }
    fn pred_atom(&mut self) -> Result<Pred, String> {
        let save = self.i;
        if self.peek() == Some(b'(') {
            self.i += 1;
            if let Ok(p) = self.pred() {
                if self.eat(")") {
                    return self.pred_suffix(p);
                }
            }
            self.i = save;
        }
        if self.eat_kw("exists") {
            if !self.eat("(") {
                return Err("syntax error in jsonpath: expected ( after exists".into());
            }
            let e = self.expr()?;
            if !self.eat(")") {
                return Err("syntax error in jsonpath: expected )".into());
            }
            return self.pred_suffix(Pred::Exists(e));
        }
        let l = self.expr()?;
        for op in ["==", "!=", "<>", "<=", ">=", "<", ">"] {
            if self.eat(op) {
                let r = self.expr()?;
                let op = if op == "<>" { "!=" } else { op };
                return self.pred_suffix(Pred::Cmp(op.to_string(), l, r));
            }
        }
        if self.eat_kw("like_regex") {
            let pat = self.string()?;
            let flags = if self.eat_kw("flag") { self.string()? } else { String::new() };
            return Ok(Pred::LikeRegex(l, pat, flags));
        }
        if self.eat_kw("starts") {
            if !self.eat_kw("with") {
                return Err("syntax error in jsonpath: expected WITH".into());
            }
            let r = self.expr()?;
            return Ok(Pred::StartsWith(l, r));
        }
        Err("syntax error in jsonpath: expected predicate".into())
    }
    fn pred_suffix(&mut self, p: Pred) -> Result<Pred, String> {
        let save = self.i;
        if self.eat_kw("is") && self.eat_kw("unknown") {
            return Ok(Pred::IsUnknown(Box::new(p)));
        }
        self.i = save;
        Ok(p)
    }

    fn expr(&mut self) -> Result<Expr, String> {
        let mut l = self.term()?;
        loop {
            self.ws();
            match self.s.get(self.i) {
                Some(b'+') => {
                    self.i += 1;
                    let r = self.term()?;
                    l = Expr::Bin('+', Box::new(l), Box::new(r));
                }
                Some(b'-') => {
                    self.i += 1;
                    let r = self.term()?;
                    l = Expr::Bin('-', Box::new(l), Box::new(r));
                }
                _ => return Ok(l),
            }
        }
    }
    fn term(&mut self) -> Result<Expr, String> {
        let mut l = self.unary()?;
        loop {
            self.ws();
            match self.s.get(self.i) {
                Some(c @ (b'*' | b'/' | b'%')) => {
                    let c = *c as char;
                    self.i += 1;
                    let r = self.unary()?;
                    l = Expr::Bin(c, Box::new(l), Box::new(r));
                }
                _ => return Ok(l),
            }
        }
    }
    fn unary(&mut self) -> Result<Expr, String> {
        if self.peek() == Some(b'-') {
            self.i += 1;
            return Ok(Expr::Neg(Box::new(self.unary()?)));
        }
        if self.peek() == Some(b'+') {
            self.i += 1;
            return self.unary();
        }
        self.primary()
    }
    fn primary(&mut self) -> Result<Expr, String> {
        let c = self.peek().ok_or("syntax error in jsonpath: unexpected end")?;
        let base = match c {
            b'$' => {
                self.i += 1;
                match self.s.get(self.i) {
                    Some(c) if c.is_ascii_alphanumeric() || *c == b'_' => Base::Var(self.ident().unwrap_or_default()),
                    Some(b'"') => Base::Var(self.string()?),
                    _ => Base::Root,
                }
            }
            b'@' => {
                self.i += 1;
                Base::Current
            }
            b'"' => Base::Lit(J::String(self.string()?)),
            b'(' => {
                self.i += 1;
                let e = self.expr()?;
                if !self.eat(")") {
                    return Err("syntax error in jsonpath: expected )".into());
                }
                return self.steps_on(e);
            }
            c if c.is_ascii_digit() || c == b'.' => Base::Lit(self.number().ok_or("syntax error in jsonpath: bad number")?),
            _ => {
                if self.eat_kw("true") {
                    Base::Lit(J::Bool(true))
                } else if self.eat_kw("false") {
                    Base::Lit(J::Bool(false))
                } else if self.eat_kw("null") {
                    Base::Lit(J::Null)
                } else if self.eat_kw("last") {
                    Base::Last
                } else {
                    return Err(format!("syntax error in jsonpath at or near \"{}\"", &self.src[self.i..].chars().take(10).collect::<String>()));
                }
            }
        };
        let steps = self.steps()?;
        Ok(Expr::Path(base, steps))
    }
    fn steps_on(&mut self, e: Expr) -> Result<Expr, String> {
        let steps = self.steps()?;
        if steps.is_empty() {
            return Ok(e);
        }
        // (expr).key: chỉ áp được khi expr là đường dẫn
        match e {
            Expr::Path(b, mut s) => {
                s.extend(steps);
                Ok(Expr::Path(b, s))
            }
            _ => Err("syntax error in jsonpath".into()),
        }
    }
    fn steps(&mut self) -> Result<Vec<Step>, String> {
        let mut out = Vec::new();
        loop {
            self.ws();
            match self.s.get(self.i) {
                Some(b'.') => {
                    self.i += 1;
                    if self.s.get(self.i) == Some(&b'*') {
                        self.i += 1;
                        if self.s.get(self.i) == Some(&b'*') {
                            return Err("jsonpath .** is not supported".into());
                        }
                        out.push(Step::AnyKey);
                    } else if self.s.get(self.i) == Some(&b'"') {
                        out.push(Step::Key(self.string()?));
                    } else {
                        let name = self.ident().ok_or("syntax error in jsonpath: expected key")?;
                        if self.s.get(self.i) == Some(&b'(') {
                            self.i += 1;
                            if !self.eat(")") {
                                return Err(format!("jsonpath method {}() with arguments is not supported", name));
                            }
                            out.push(Step::Method(name.to_ascii_lowercase()));
                        } else {
                            out.push(Step::Key(name));
                        }
                    }
                }
                Some(b'[') => {
                    self.i += 1;
                    if self.eat("*") {
                        if !self.eat("]") {
                            return Err("syntax error in jsonpath: expected ]".into());
                        }
                        out.push(Step::AnyIndex);
                        continue;
                    }
                    let mut subs = Vec::new();
                    loop {
                        let a = self.expr()?;
                        let b = if self.eat_kw("to") { Some(self.expr()?) } else { None };
                        subs.push((a, b));
                        if self.eat(",") {
                            continue;
                        }
                        if self.eat("]") {
                            break;
                        }
                        return Err("syntax error in jsonpath: expected ]".into());
                    }
                    out.push(Step::Index(subs));
                }
                Some(b'?') => {
                    self.i += 1;
                    if !self.eat("(") {
                        return Err("syntax error in jsonpath: expected ( after ?".into());
                    }
                    let p = self.pred()?;
                    if !self.eat(")") {
                        return Err("syntax error in jsonpath: expected )".into());
                    }
                    out.push(Step::Filter(Box::new(p)));
                }
                _ => return Ok(out),
            }
        }
    }
}

/// Biên dịch một jsonpath.
pub(crate) fn parse(src: &str) -> Result<JsonPath, String> {
    let mut p = P { s: src.as_bytes(), src, i: 0 };
    let mut strict = false;
    if p.eat_kw("strict") {
        strict = true;
    } else {
        p.eat_kw("lax");
    }
    // biểu thức điều kiện ở mức ngoài cùng (`$.a == 1`) hay một đường dẫn / biểu thức số học
    let save = p.i;
    let expr = match p.pred() {
        Ok(pr) if {
            p.ws();
            p.i == p.s.len()
        } =>
        {
            Expr::Pred(Box::new(pr))
        }
        _ => {
            p.i = save;
            p.expr()?
        }
    };
    p.ws();
    if p.i != p.s.len() {
        return Err(format!("syntax error in jsonpath at or near \"{}\"", &src[p.i..]));
    }
    Ok(JsonPath { strict, expr })
}

struct Ctx<'a> {
    root: &'a J,
    vars: &'a J,
    strict: bool,
}

type R<T> = Result<T, String>;

fn type_name(v: &J) -> &'static str {
    match v {
        J::Null => "null",
        J::Bool(_) => "boolean",
        J::Number(_) => "number",
        J::String(_) => "string",
        J::Array(_) => "array",
        J::Object(_) => "object",
    }
}

impl<'a> Ctx<'a> {
    fn eval(&self, e: &Expr, cur: &J, last: Option<usize>) -> R<Vec<J>> {
        match e {
            Expr::Path(base, steps) => {
                let start = match base {
                    Base::Root => vec![self.root.clone()],
                    Base::Current => vec![cur.clone()],
                    Base::Var(n) => match self.vars.get(n) {
                        Some(v) => vec![v.clone()],
                        None => return Err(format!("could not find jsonpath variable \"{}\"", n)),
                    },
                    Base::Lit(v) => vec![v.clone()],
                    Base::Last => match last {
                        Some(n) => vec![J::from(n as i64 - 1)],
                        None => return Err("LAST is allowed only in array subscripts".into()),
                    },
                };
                let mut items = start;
                for st in steps {
                    let mut next = Vec::new();
                    for it in &items {
                        self.step(st, it, &mut next)?;
                    }
                    items = next;
                }
                Ok(items)
            }
            Expr::Neg(x) => self
                .eval(x, cur, last)?
                .into_iter()
                .map(|v| match v.as_f64() {
                    Some(f) => Ok(num(-f)),
                    None => Err("operand of unary jsonpath operator - is not a numeric value".into()),
                })
                .collect(),
            Expr::Bin(op, a, b) => {
                let (x, y) = (self.eval(a, cur, last)?, self.eval(b, cur, last)?);
                let (Some(x), Some(y)) = (single_num(&x), single_num(&y)) else {
                    return Err(format!("left operand of jsonpath operator {} is not a single numeric value", op));
                };
                let r = match op {
                    '+' => x + y,
                    '-' => x - y,
                    '*' => x * y,
                    '/' => {
                        if y == 0.0 {
                            return Err("division by zero".into());
                        }
                        x / y
                    }
                    _ => {
                        if y == 0.0 {
                            return Err("division by zero".into());
                        }
                        x % y
                    }
                };
                Ok(vec![num(r)])
            }
            Expr::Pred(p) => Ok(vec![match self.pred(p, cur)? {
                Some(b) => J::Bool(b),
                None => J::Null,
            }]),
        }
    }

    fn step(&self, st: &Step, it: &J, out: &mut Vec<J>) -> R<()> {
        match st {
            Step::Key(k) => match it {
                J::Object(m) => match m.get(k) {
                    Some(v) => out.push(v.clone()),
                    None if self.strict => return Err(format!("JSON object does not contain key \"{}\"", k)),
                    None => {}
                },
                J::Array(a) if !self.strict => {
                    for x in a {
                        if let J::Object(m) = x {
                            if let Some(v) = m.get(k) {
                                out.push(v.clone());
                            }
                        }
                    }
                }
                _ if self.strict => return Err("jsonpath member accessor can only be applied to an object".into()),
                _ => {}
            },
            Step::AnyKey => match it {
                J::Object(m) => out.extend(m.values().cloned()),
                J::Array(a) if !self.strict => {
                    for x in a {
                        if let J::Object(m) = x {
                            out.extend(m.values().cloned());
                        }
                    }
                }
                _ if self.strict => return Err("jsonpath wildcard member accessor can only be applied to an object".into()),
                _ => {}
            },
            Step::AnyIndex => match it {
                J::Array(a) => out.extend(a.iter().cloned()),
                _ if self.strict => return Err("jsonpath wildcard array accessor can only be applied to an array".into()),
                other => out.push(other.clone()),
            },
            Step::Index(subs) => {
                let arr: Vec<J> = match it {
                    J::Array(a) => a.clone(),
                    _ if self.strict => return Err("jsonpath array accessor can only be applied to an array".into()),
                    other => vec![other.clone()],
                };
                let n = arr.len();
                for (a, b) in subs {
                    let ia = self.index(a, it, n)?;
                    let ib = match b {
                        Some(b) => self.index(b, it, n)?,
                        None => ia,
                    };
                    for k in ia..=ib {
                        if k >= 0 && (k as usize) < n {
                            out.push(arr[k as usize].clone());
                        } else if self.strict {
                            return Err("jsonpath array subscript is out of bounds".into());
                        }
                    }
                }
            }
            Step::Filter(p) => {
                let cands: Vec<J> = match it {
                    J::Array(a) if !self.strict => a.clone(),
                    other => vec![other.clone()],
                };
                for c in cands {
                    if self.pred(p, &c)? == Some(true) {
                        out.push(c);
                    }
                }
            }
            Step::Method(m) => {
                let items: Vec<J> = match (it, m.as_str()) {
                    (J::Array(a), "size" | "type") => {
                        let _ = a;
                        vec![it.clone()]
                    }
                    (J::Array(a), _) if !self.strict => a.clone(),
                    _ => vec![it.clone()],
                };
                for x in items {
                    out.push(self.method(m, &x)?);
                }
            }
        }
        Ok(())
    }

    fn index(&self, e: &Expr, cur: &J, n: usize) -> R<i64> {
        let v = self.eval(e, cur, Some(n))?;
        match single_num(&v) {
            Some(f) => Ok(f.trunc() as i64),
            None => Err("jsonpath array subscript is not a single numeric value".into()),
        }
    }

    fn method(&self, m: &str, x: &J) -> R<J> {
        Ok(match m {
            "size" => J::from(match x {
                J::Array(a) => a.len() as i64,
                _ if self.strict => return Err("jsonpath item method .size() can only be applied to an array".into()),
                _ => 1,
            }),
            "type" => J::String(type_name(x).into()),
            "double" | "number" | "decimal" => match x {
                J::Number(_) => x.clone(),
                J::String(s) => num(s.trim().parse::<f64>().map_err(|_| format!("argument \"{}\" of jsonpath item method .{}() is invalid for type double precision", s, m))?),
                _ => return Err(format!("jsonpath item method .{}() can only be applied to a string or numeric value", m)),
            },
            "abs" | "floor" | "ceiling" => {
                let f = x.as_f64().ok_or_else(|| format!("jsonpath item method .{}() can only be applied to a numeric value", m))?;
                num(match m {
                    "abs" => f.abs(),
                    "floor" => f.floor(),
                    _ => f.ceil(),
                })
            }
            "string" => J::String(match x {
                J::String(s) => s.clone(),
                J::Number(n) => n.to_string(),
                J::Bool(b) => b.to_string(),
                _ => return Err("jsonpath item method .string() can only be applied to a bool, string, numeric, or datetime value".into()),
            }),
            "boolean" => match x {
                J::Bool(_) => x.clone(),
                J::Number(n) => J::Bool(n.as_f64() != Some(0.0)),
                J::String(s) => match s.to_ascii_lowercase().as_str() {
                    "t" | "true" | "y" | "yes" | "on" | "1" => J::Bool(true),
                    "f" | "false" | "n" | "no" | "off" | "0" => J::Bool(false),
                    _ => return Err(format!("argument \"{}\" of jsonpath item method .boolean() is invalid for type boolean", s)),
                },
                _ => return Err("jsonpath item method .boolean() can only be applied to a bool, string, or numeric value".into()),
            },
            "keyvalue" => match x {
                J::Object(o) => {
                    return Ok(J::Array(
                        o.iter().map(|(k, v)| serde_json::json!({"key": k, "value": v, "id": 0})).collect(),
                    ))
                }
                _ => return Err("jsonpath item method .keyvalue() can only be applied to an object".into()),
            },
            other => return Err(format!("jsonpath item method .{}() is not supported", other)),
        })
    }

    /// Kết quả ba trị: `None` = unknown (lỗi bị nuốt trong bộ lọc, khác kiểu khi so sánh).
    fn pred(&self, p: &Pred, cur: &J) -> R<Option<bool>> {
        Ok(match p {
            Pred::And(a, b) => match self.pred(a, cur)? {
                Some(false) => Some(false),
                x => match (x, self.pred(b, cur)?) {
                    (_, Some(false)) => Some(false),
                    (Some(true), Some(true)) => Some(true),
                    _ => None,
                },
            },
            Pred::Or(a, b) => match self.pred(a, cur)? {
                Some(true) => Some(true),
                x => match (x, self.pred(b, cur)?) {
                    (_, Some(true)) => Some(true),
                    (Some(false), Some(false)) => Some(false),
                    _ => None,
                },
            },
            Pred::Not(a) => self.pred(a, cur)?.map(|b| !b),
            Pred::IsUnknown(a) => Some(self.pred(a, cur)?.is_none()),
            Pred::Exists(e) => match self.eval(e, cur, None) {
                Ok(v) => Some(!v.is_empty()),
                Err(_) => None,
            },
            Pred::Cmp(op, a, b) => {
                let (Ok(x), Ok(y)) = (self.eval(a, cur, None), self.eval(b, cur, None)) else { return Ok(None) };
                let mut unknown = false;
                for l in &x {
                    for r in &y {
                        match cmp(op, l, r) {
                            Some(true) => return Ok(Some(true)),
                            None => unknown = true,
                            _ => {}
                        }
                    }
                }
                if unknown { None } else { Some(false) }
            }
            Pred::LikeRegex(e, pat, flags) => {
                let Ok(x) = self.eval(e, cur, None) else { return Ok(None) };
                let mut b = regex::RegexBuilder::new(pat);
                b.case_insensitive(flags.contains('i')).dot_matches_new_line(flags.contains('s')).multi_line(flags.contains('m'));
                let re = b.build().map_err(|e| format!("invalid regular expression: {}", e))?;
                let mut unknown = false;
                for v in &x {
                    match v {
                        J::String(s) if re.is_match(s) => return Ok(Some(true)),
                        J::String(_) => {}
                        _ => unknown = true,
                    }
                }
                if unknown { None } else { Some(false) }
            }
            Pred::StartsWith(e, p) => {
                let (Ok(x), Ok(y)) = (self.eval(e, cur, None), self.eval(p, cur, None)) else { return Ok(None) };
                let Some(J::String(prefix)) = y.first() else { return Ok(None) };
                let mut unknown = false;
                for v in &x {
                    match v {
                        J::String(s) if s.starts_with(prefix.as_str()) => return Ok(Some(true)),
                        J::String(_) => {}
                        _ => unknown = true,
                    }
                }
                if unknown { None } else { Some(false) }
            }
        })
    }
}

fn num(f: f64) -> J {
    if f.fract() == 0.0 && f.abs() < 9.0e15 {
        J::from(f as i64)
    } else {
        serde_json::Number::from_f64(f).map_or(J::Null, J::Number)
    }
}

fn single_num(v: &[J]) -> Option<f64> {
    match v {
        [x] => x.as_f64(),
        _ => None,
    }
}

fn cmp(op: &str, l: &J, r: &J) -> Option<bool> {
    use std::cmp::Ordering::*;
    let ord = match (l, r) {
        (J::Null, J::Null) => Equal,
        (J::Null, _) | (_, J::Null) => {
            return match op {
                "==" => Some(false),
                "!=" => Some(true),
                _ => None,
            }
        }
        (J::Number(a), J::Number(b)) => a.as_f64()?.partial_cmp(&b.as_f64()?)?,
        (J::String(a), J::String(b)) => a.cmp(b),
        (J::Bool(a), J::Bool(b)) => a.cmp(b),
        _ => return None,
    };
    Some(match op {
        "==" => ord == Equal,
        "!=" => ord != Equal,
        "<" => ord == Less,
        "<=" => ord != Greater,
        ">" => ord == Greater,
        _ => ord != Less,
    })
}

impl JsonPath {
    /// Mọi giá trị khớp đường dẫn.
    pub(crate) fn query(&self, target: &J, vars: &J) -> R<Vec<J>> {
        let ctx = Ctx { root: target, vars, strict: self.strict };
        ctx.eval(&self.expr, target, None)
    }

    /// `jsonb_path_match` / `@@`: kết quả phải là một giá trị boolean (hoặc null = unknown).
    pub(crate) fn matches(&self, target: &J, vars: &J) -> R<Option<bool>> {
        let v = self.query(target, vars)?;
        match v.as_slice() {
            [J::Bool(b)] => Ok(Some(*b)),
            [J::Null] => Ok(None),
            _ => Err("single boolean result is expected".into()),
        }
    }
}

thread_local! {
    static CACHE: std::cell::RefCell<std::collections::HashMap<String, Result<JsonPath, String>>> = std::cell::RefCell::new(std::collections::HashMap::new());
}

/// Biên dịch có cache theo luồng (cùng một đường dẫn được dùng cho mọi dòng của truy vấn).
pub(crate) fn compiled(src: &str) -> Result<JsonPath, String> {
    CACHE.with(|c| {
        let mut c = c.borrow_mut();
        if let Some(r) = c.get(src) {
            return r.clone();
        }
        if c.len() > 256 {
            c.clear();
        }
        let r = parse(src);
        c.insert(src.to_string(), r.clone());
        r
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    fn q(p: &str, t: J) -> Vec<J> {
        compiled(p).unwrap().query(&t, &json!({})).unwrap()
    }
    #[test]
    fn basics() {
        assert_eq!(q("$.a[*]", json!({"a": [1, 2]})), vec![json!(1), json!(2)]);
        assert_eq!(q("$.a", json!({"a": 1})), vec![json!(1)]);
        assert_eq!(q("$.a == 1", json!({"a": 1})), vec![json!(true)]);
        assert_eq!(q("$.a[*] ? (@ > 1)", json!({"a": [1, 2, 3]})), vec![json!(2), json!(3)]);
        assert_eq!(q("$.a.size()", json!({"a": [1, 2, 3]})), vec![json!(3)]);
        assert_eq!(q("$.a[last]", json!({"a": [1, 2, 3]})), vec![json!(3)]);
        assert_eq!(q("$.a[0 to 1]", json!({"a": [1, 2, 3]})), vec![json!(1), json!(2)]);
        assert_eq!(q("$.items[*] ? (@.price > 10 && @.name starts with \"b\").name", json!({"items": [{"name": "bike", "price": 20}, {"name": "ball", "price": 5}]})), vec![json!("bike")]);
        assert_eq!(q("$.a + 1", json!({"a": 2})), vec![json!(3)]);
        assert!(q("$.b", json!({"a": 1})).is_empty());
        assert!(compiled("strict $.b").unwrap().query(&json!({"a": 1}), &json!({})).is_err());
    }
}
