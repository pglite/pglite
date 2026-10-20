//! Hàm chuỗi bổ sung của PostgreSQL. Trước đây các hàm này không được nhận ra và trả lại nguyên văn câu chữ.
//!
//! - Biểu thức chính quy: `regexp_replace`, `regexp_match`, `regexp_matches` (không cờ `g`),
//!   `regexp_split_to_array`, `regexp_count`, `regexp_like`, `regexp_substr`. Mẫu POSIX của PostgreSQL
//!   được quy về crate `regex` (như toán tử `~`); cờ theo bảng 9.24 của tài liệu PostgreSQL.
//! - `format(fmt, ...)`: `%s`, `%I`, `%L`, `%%`, vị trí `%n$`, độ rộng `%-10s` / `%*s`.
//! - `greatest(...)` / `least(...)` (bỏ qua NULL): cùng một cài đặt cho mọi bộ đánh giá, kể cả đường JOIN
//!   (trước đây ở JOIN ra NULL hoặc đối số đầu).
use crate::types::Value;
use std::cell::RefCell;
use std::collections::HashMap;

thread_local! {
    static CACHE: RefCell<HashMap<(String, String), Option<regex::Regex>>> = RefCell::new(HashMap::new());
}

/// Tên hàm (đã viết hoa, kèm `(`) do module này xử lý: lọc nhanh trước khi gọi `try_eval`.
pub(crate) fn is_strfn_name(upper: &str) -> bool {
    upper.starts_with("REGEXP_") || upper.starts_with("FORMAT(") || upper.starts_with("GREATEST(") || upper.starts_with("LEAST(")
}

/// Tên hàm (viết hoa, không kèm ngoặc) có trong module.
pub(crate) fn is_known(name: &str) -> bool {
    matches!(
        name,
        "REGEXP_REPLACE" | "REGEXP_MATCH" | "REGEXP_MATCHES" | "REGEXP_SPLIT_TO_ARRAY" | "REGEXP_COUNT" | "REGEXP_LIKE" | "REGEXP_SUBSTR" | "FORMAT" | "GREATEST" | "LEAST"
    )
}

/// `quote_ident`: nháy kép khi không phải tên thường viết thường.
pub(crate) fn quote_ident(s: &str) -> String {
    let plain = !s.is_empty()
        && s.chars().next().map_or(false, |c| c.is_ascii_lowercase() || c == '_')
        && s.chars().all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_');
    // từ khoá dành riêng của PostgreSQL cũng phải nháy (`quote_ident('user')` = `"user"`)
    const RESERVED: &[&str] = &[
        "all", "analyse", "analyze", "and", "any", "array", "as", "asc", "asymmetric", "both", "case", "cast", "check", "collate", "column",
        "constraint", "create", "current_catalog", "current_date", "current_role", "current_time", "current_timestamp", "current_user",
        "default", "deferrable", "desc", "distinct", "do", "else", "end", "except", "false", "fetch", "for", "foreign", "from", "grant",
        "group", "having", "in", "initially", "intersect", "into", "lateral", "leading", "limit", "localtime", "localtimestamp", "not",
        "null", "offset", "on", "only", "or", "order", "placing", "primary", "references", "returning", "select", "session_user", "some",
        "symmetric", "table", "then", "to", "trailing", "true", "union", "unique", "user", "using", "variadic", "when", "where", "window", "with",
    ];
    if plain && !RESERVED.contains(&s) { s.to_string() } else { format!("\"{}\"", s.replace('"', "\"\"")) }
}

/// `quote_literal` / `%L`: NULL → `NULL`, chuỗi có `\\` dùng dạng `E'...'`.
fn quote_literal_or_null(v: &Value) -> String {
    if v.is_null() {
        return "NULL".to_string();
    }
    let s = v.as_str();
    let body = s.replace('\'', "''");
    if s.contains('\\') { format!("E'{}'", body.replace('\\', "\\\\")) } else { format!("'{}'", body) }
}

/// `format(fmt, args...)` của PostgreSQL.
fn pg_format(args: &[Value]) -> Result<Value, String> {
    let Some(fmt) = args.first() else { return Ok(Value::Null) };
    if fmt.is_null() {
        return Ok(Value::Null);
    }
    let fmt = fmt.as_str();
    let rest = &args[1..];
    let mut out = String::new();
    let chars: Vec<char> = fmt.chars().collect();
    let mut next_arg = 0usize;
    let mut i = 0;
    while i < chars.len() {
        if chars[i] != '%' {
            out.push(chars[i]);
            i += 1;
            continue;
        }
        i += 1;
        if i >= chars.len() {
            return Err("unterminated format() type specifier".to_string());
        }
        if chars[i] == '%' {
            out.push('%');
            i += 1;
            continue;
        }
        // %[n$][-][width|*[m$]]type
        let read_num = |i: &mut usize| -> Option<usize> {
            let st = *i;
            while *i < chars.len() && chars[*i].is_ascii_digit() {
                *i += 1;
            }
            (st < *i).then(|| chars[st..*i].iter().collect::<String>().parse().unwrap_or(0))
        };
        let save = i;
        let mut pos: Option<usize> = None;
        if let Some(n) = read_num(&mut i) {
            if i < chars.len() && chars[i] == '$' {
                pos = Some(n);
                i += 1;
            } else {
                i = save;
            }
        }
        let mut left = false;
        if i < chars.len() && chars[i] == '-' {
            left = true;
            i += 1;
        }
        let mut width: Option<i64> = None;
        if i < chars.len() && chars[i] == '*' {
            i += 1;
            let wv = rest.get(next_arg).cloned().unwrap_or(Value::Null);
            next_arg += 1;
            width = wv.as_i64();
        } else if let Some(w) = read_num(&mut i) {
            width = Some(w as i64);
        }
        if i >= chars.len() {
            return Err("unterminated format() type specifier".to_string());
        }
        let kind = chars[i];
        i += 1;
        let idx = match pos {
            Some(0) => return Err("format specifies argument 0, but arguments are numbered from 1".to_string()),
            Some(n) => n - 1,
            None => next_arg,
        };
        next_arg = idx + 1;
        let v = rest.get(idx).cloned().ok_or_else(|| "too few arguments for format()".to_string())?;
        let piece = match kind {
            's' => if v.is_null() { String::new() } else { v.as_str() },
            'I' => {
                if v.is_null() {
                    return Err("null values cannot be formatted as an SQL identifier".to_string());
                }
                quote_ident(&v.as_str())
            }
            'L' => quote_literal_or_null(&v),
            other => return Err(format!("unrecognized format() type specifier \"{}\"", other)),
        };
        let w = width.unwrap_or(0);
        let (w, left) = if w < 0 { (-w as usize, true) } else { (w as usize, left) };
        let len = piece.chars().count();
        if len < w {
            let pad = " ".repeat(w - len);
            if left { out.push_str(&piece); out.push_str(&pad); } else { out.push_str(&pad); out.push_str(&piece); }
        } else {
            out.push_str(&piece);
        }
    }
    Ok(Value::text(out))
}

struct Flags {
    global: bool,
    /// cờ cho crate `regex`, đã chuẩn hoá (để làm khoá cache)
    key: String,
}

fn parse_flags(flags: &str) -> Result<Flags, String> {
    let (mut global, mut ci, mut dot_nl, mut multi_line, mut expanded, mut quote) = (false, false, true, false, false, false);
    for c in flags.chars() {
        match c {
            'g' => global = true,
            'i' => ci = true,
            'c' => ci = false,
            // nhạy dòng: `.` và `[^...]` không khớp xuống dòng, `^` `$` khớp đầu / cuối dòng
            'n' | 'm' => {
                dot_nl = false;
                multi_line = true;
            }
            's' => {
                dot_nl = true;
                multi_line = false;
            }
            'p' => {
                dot_nl = false;
                multi_line = false;
            }
            'w' => {
                dot_nl = true;
                multi_line = true;
            }
            'x' => expanded = true,
            't' => expanded = false,
            'q' => quote = true,
            'b' | 'e' => {}
            other => return Err(format!("invalid regular expression option: \"{}\"", other)),
        }
    }
    let key = format!("{}{}{}{}{}", ci as u8, dot_nl as u8, multi_line as u8, expanded as u8, quote as u8);
    Ok(Flags { global, key })
}

fn compile(pat: &str, f: &Flags) -> Option<regex::Regex> {
    CACHE.with(|c| {
        let mut c = c.borrow_mut();
        let key = (pat.to_string(), f.key.clone());
        if let Some(r) = c.get(&key) {
            return r.clone();
        }
        if c.len() > 256 {
            c.clear();
        }
        let b = f.key.as_bytes();
        let on = |i: usize| b[i] == b'1';
        let p = if on(4) {
            regex::escape(pat)
        } else {
            // \m, \M, \y (biên từ của PostgreSQL) → \b, như toán tử `~`
            pat.replace("\\m", "\\b").replace("\\M", "\\b").replace("\\y", "\\b")
        };
        let re = regex::RegexBuilder::new(&p)
            .case_insensitive(on(0))
            .dot_matches_new_line(on(1))
            .multi_line(on(2))
            .ignore_whitespace(on(3))
            .build()
            .ok();
        c.insert(key, re.clone());
        re
    })
}

/// Chuỗi thay thế của PostgreSQL: `\1`..`\9` là nhóm, `\&` là cả đoạn khớp, `\\` là dấu `\`.
fn expand_replacement(caps: &regex::Captures, repl: &str, out: &mut String) {
    let mut it = repl.chars().peekable();
    while let Some(c) = it.next() {
        if c != '\\' {
            out.push(c);
            continue;
        }
        match it.peek().copied() {
            Some(d @ '1'..='9') => {
                it.next();
                if let Some(m) = caps.get(d as usize - '0' as usize) {
                    out.push_str(m.as_str());
                }
            }
            Some('&') => {
                it.next();
                out.push_str(caps.get(0).map_or("", |m| m.as_str()));
            }
            Some('\\') => {
                it.next();
                out.push('\\');
            }
            _ => out.push('\\'),
        }
    }
}

/// Mảng text kiểu PostgreSQL (được lưu dạng JSON như `string_to_array`).
fn text_array(items: Vec<Option<String>>) -> Value {
    let arr: Vec<serde_json::Value> = items.into_iter().map(|s| s.map_or(serde_json::Value::Null, serde_json::Value::String)).collect();
    Value::text(serde_json::to_string(&arr).unwrap_or_default())
}

/// Vị trí byte của ký tự thứ `start` (1-based) trong `s`.
fn char_start(s: &str, start: i64) -> usize {
    s.char_indices().nth((start - 1).max(0) as usize).map_or(s.len(), |(i, _)| i)
}

fn captures_of(caps: &regex::Captures) -> Vec<Option<String>> {
    if caps.len() > 1 {
        (1..caps.len()).map(|i| caps.get(i).map(|m| m.as_str().to_string())).collect()
    } else {
        vec![caps.get(0).map(|m| m.as_str().to_string())]
    }
}

/// `regexp_split_to_array`: khớp rỗng ở đầu / cuối chuỗi hoặc ngay sau một khớp trước bị bỏ qua.
fn split(re: &regex::Regex, s: &str) -> Vec<Option<String>> {
    let mut out = Vec::new();
    let mut last = 0;
    let mut prev_end: Option<usize> = None;
    for m in re.find_iter(s) {
        if m.start() == m.end() && (m.start() == 0 || m.start() == s.len() || prev_end == Some(m.start())) {
            continue;
        }
        out.push(Some(s[last..m.start()].to_string()));
        last = m.end();
        prev_end = Some(m.end());
    }
    out.push(Some(s[last..].to_string()));
    out
}

/// Đánh giá `name(args)` nếu là hàm regex. `eval` tính một đối số. `None` = không phải hàm của module.
pub(crate) fn try_eval(s: &str, upper: &str, eval: &dyn Fn(&str) -> Value) -> Option<Value> {
    let open = s.find('(')?;
    if !s.ends_with(')') || !super::executor::is_fully_enclosed_in_parens(&s[open..]) {
        return None;
    }
    let name = upper[..open].trim();
    if !is_known(name) {
        return None;
    }
    let args: Vec<Value> = super::executor::split_function_args(&s[open + 1..s.len() - 1]).iter().map(|a| eval(a)).collect();
    call(name, &args)
}

/// Gọi hàm `name` (viết hoa) với đối số đã tính. `None` = không phải hàm / sai số đối số.
pub(crate) fn call(name: &str, args: &[Value]) -> Option<Value> {
    if name == "GREATEST" || name == "LEAST" {
        let greatest = name == "GREATEST";
        let mut best: Option<Value> = None;
        for v in args.iter().filter(|v| !v.is_null()) {
            best = Some(match best {
                None => v.clone(),
                Some(b) => {
                    let o = v.cmp_value(&b);
                    if (greatest && o == std::cmp::Ordering::Greater) || (!greatest && o == std::cmp::Ordering::Less) { v.clone() } else { b }
                }
            });
        }
        return Some(best.unwrap_or(Value::Null));
    }
    if name == "FORMAT" {
        return Some(pg_format(args).unwrap_or_else(|e| {
            super::errors::raise(e);
            Value::Null
        }));
    }
    if args.len() < 2 {
        return None;
    }
    if args.iter().any(|v| v.is_null()) {
        return Some(Value::Null);
    }
    let text = args[0].as_str();
    let pat = args[1].as_str();
    let int_arg = |i: usize| -> Option<i64> { args.get(i).and_then(|v| v.as_i64().or_else(|| v.as_str().trim().parse().ok())) };
    let fail = |msg: String| {
        super::errors::raise(msg);
        Some(Value::Null)
    };
    let flags_at = |i: usize| -> Result<Flags, String> { parse_flags(&args.get(i).map(|v| v.as_str()).unwrap_or_default()) };
    let get_re = |f: &Flags| compile(&pat, f);

    match name {
        "REGEXP_REPLACE" => {
            if args.len() < 3 {
                return None;
            }
            let repl = args[2].as_str();
            // (src, pat, repl [, flags]) hoặc (src, pat, repl, start [, N [, flags]])
            let start_form = args.len() >= 5 || args.len() == 4 && matches!(args[3], Value::Int(_));
            let (start, nth, flags) = if start_form {
                let start = int_arg(3).unwrap_or(1);
                if start < 1 {
                    return fail(format!("invalid value for parameter \"start\": {}", start));
                }
                let nth = if args.len() >= 5 { int_arg(4).unwrap_or(0) } else { 1 };
                if nth < 0 {
                    return fail(format!("invalid value for parameter \"n\": {}", nth));
                }
                let flags = match flags_at(5) {
                    Ok(f) => f,
                    Err(e) => return fail(e),
                };
                // N = 0 thay mọi lần khớp; thiếu N (chỉ có start) thì thay lần đầu, trừ khi có cờ g
                let nth = if args.len() >= 5 { nth } else if flags.global { 0 } else { 1 };
                (start, nth, flags)
            } else {
                let flags = match flags_at(3) {
                    Ok(f) => f,
                    Err(e) => return fail(e),
                };
                let nth = if flags.global { 0 } else { 1 };
                (1, nth, flags)
            };
            let Some(re) = get_re(&flags) else { return fail(format!("invalid regular expression: {}", pat)) };
            let from = char_start(&text, start);
            let mut out = String::with_capacity(text.len());
            out.push_str(&text[..from]);
            let mut last = from;
            for (k, caps) in re.captures_iter(&text[from..]).enumerate() {
                let m = caps.get(0).unwrap();
                if nth != 0 && k as i64 + 1 != nth {
                    continue;
                }
                out.push_str(&text[last..from + m.start()]);
                expand_replacement(&caps, &repl, &mut out);
                last = from + m.end();
                if nth != 0 {
                    break;
                }
            }
            out.push_str(&text[last..]);
            Some(Value::text(out))
        }
        "REGEXP_MATCH" | "REGEXP_MATCHES" => {
            let flags = match flags_at(2) {
                Ok(f) => f,
                Err(e) => return fail(e),
            };
            if flags.global {
                return if name == "REGEXP_MATCH" {
                    fail("regexp_match() does not support the \"global\" option".to_string())
                } else {
                    // nhiều dòng (hàm trả tập) chưa hỗ trợ: báo lỗi thay vì trả sai
                    fail("regexp_matches() with the \"global\" option is not supported".to_string())
                };
            }
            let Some(re) = get_re(&flags) else { return fail(format!("invalid regular expression: {}", pat)) };
            Some(match re.captures(&text) {
                Some(caps) => text_array(captures_of(&caps)),
                None => Value::Null,
            })
        }
        "REGEXP_SPLIT_TO_ARRAY" => {
            let flags = match flags_at(2) {
                Ok(f) => f,
                Err(e) => return fail(e),
            };
            if flags.global {
                return fail("regexp_split_to_array() does not support the \"global\" option".to_string());
            }
            let Some(re) = get_re(&flags) else { return fail(format!("invalid regular expression: {}", pat)) };
            Some(text_array(split(&re, &text)))
        }
        "REGEXP_COUNT" => {
            let start = int_arg(2).unwrap_or(1);
            if start < 1 {
                return fail(format!("invalid value for parameter \"start\": {}", start));
            }
            let flags = match flags_at(3) {
                Ok(f) => f,
                Err(e) => return fail(e),
            };
            let Some(re) = get_re(&flags) else { return fail(format!("invalid regular expression: {}", pat)) };
            Some(Value::Int(re.find_iter(&text[char_start(&text, start)..]).count() as i64))
        }
        "REGEXP_LIKE" => {
            let flags = match flags_at(2) {
                Ok(f) => f,
                Err(e) => return fail(e),
            };
            if flags.global {
                return fail("regexp_like() does not support the \"global\" option".to_string());
            }
            let Some(re) = get_re(&flags) else { return fail(format!("invalid regular expression: {}", pat)) };
            Some(Value::Bool(re.is_match(&text)))
        }
        "REGEXP_SUBSTR" => {
            let start = int_arg(2).unwrap_or(1);
            let nth = int_arg(3).unwrap_or(1);
            if start < 1 || nth < 1 {
                return fail(format!("invalid value for parameter \"{}\": {}", if start < 1 { "start" } else { "n" }, if start < 1 { start } else { nth }));
            }
            let flags = match flags_at(4) {
                Ok(f) => f,
                Err(e) => return fail(e),
            };
            let sub = int_arg(5).unwrap_or(0).max(0) as usize;
            let Some(re) = get_re(&flags) else { return fail(format!("invalid regular expression: {}", pat)) };
            let from = char_start(&text, start);
            let found = re.captures_iter(&text[from..]).nth(nth as usize - 1).and_then(|c| c.get(sub).map(|m| m.as_str().to_string()));
            Some(found.map_or(Value::Null, Value::text))
        }
        _ => None,
    }
}
