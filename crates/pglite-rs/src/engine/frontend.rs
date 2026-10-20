//! Lexer chuẩn hoá câu lệnh trước khi phần còn lại của engine phân tích (engine chỉ hiểu chuỗi
//! `'...'` chuẩn với `''`):
//! - bỏ comment `--` và `/* ... */` (lồng nhau được, như PostgreSQL);
//! - `E'..'` (escape \n \t \' \x41 é ...), `U&'..' [UESCAPE 'c']`, `$tag$..$tag$` → `'..'`;
//!   thân `$$..$$` của DO / CREATE FUNCTION|PROCEDURE giữ nguyên (trình thông dịch tự xử lý);
//! - `B'101'`, `X'1F'` → `'101'::BIT(3)`;
//! - `0x1F`, `0o17`, `0b101`, `1_000` → số thập phân;
//! - chuỗi liền kề cách nhau bởi xuống dòng được nối (`'a'\n'b'` = `'ab'`), cùng dòng là lỗi 42601;
//! - định danh có nháy chứa ký tự mà engine không xử lý được (dấu cách, `-`, `.`, ...) được mã hoá
//!   thành `__q<hex>_` (thuận nghịch); tên cột, thông báo lỗi trả ra được giải mã lại. Tra cứu
//!   bảng/cột thử thêm tên đã giải mã nên DB cũ lưu tên gốc vẫn dùng được.
use std::sync::atomic::{AtomicBool, Ordering};

/// Đã từng gặp định danh mã hoá (lexer mã hoá hoặc tra cứu theo tên mã hoá): bật giải mã ô dữ liệu.
static ENCODED_SEEN: AtomicBool = AtomicBool::new(false);

pub(crate) fn encoded_seen() -> bool {
    ENCODED_SEEN.load(Ordering::Relaxed)
}

fn is_ident_start(c: char) -> bool {
    c.is_alphabetic() || c == '_'
}

fn is_ident_char(c: char) -> bool {
    c.is_alphanumeric() || c == '_' || c == '$'
}

/// Định danh có nháy mà engine dùng trực tiếp được (chữ, số, `_`, `$`).
fn ident_is_plain(s: &str) -> bool {
    !s.is_empty() && s.chars().all(is_ident_char)
}

pub(crate) fn encode_ident(s: &str) -> String {
    ENCODED_SEEN.store(true, Ordering::Relaxed);
    let mut out = String::with_capacity(4 + s.len() * 2);
    out.push_str("__q");
    out.push_str(&hex::encode(s.as_bytes()));
    out.push('_');
    out
}

/// `__q<hex>_` → tên gốc (không phân biệt hoa thường của chữ số hex).
pub(crate) fn decode_ident(s: &str) -> Option<String> {
    let b = s.as_bytes();
    if b.len() < 6 || !b[..3].eq_ignore_ascii_case(b"__q") || b[b.len() - 1] != b'_' {
        return None;
    }
    let hex_part = &s[3..s.len() - 1];
    if hex_part.len() % 2 != 0 || !hex_part.bytes().all(|c| c.is_ascii_hexdigit()) {
        return None;
    }
    let bytes = hex::decode(hex_part.to_ascii_lowercase()).ok()?;
    String::from_utf8(bytes).ok()
}

/// Thay mọi định danh mã hoá nằm trong `s` (tên cột dạng biểu thức, thông báo lỗi) bằng tên gốc.
pub(crate) fn decode_all(s: &str) -> std::borrow::Cow<'_, str> {
    if !s.contains("__q") && !s.contains("__Q") {
        return std::borrow::Cow::Borrowed(s);
    }
    let mut out = String::with_capacity(s.len());
    let mut rest = s;
    while let Some(pos) = rest.find("__q").or_else(|| rest.find("__Q")) {
        out.push_str(&rest[..pos]);
        let tail = &rest[pos..];
        let end = tail[3..].find(|c: char| !c.is_ascii_hexdigit()).map(|e| e + 3).unwrap_or(tail.len());
        if tail[end..].starts_with('_') {
            if let Some(d) = decode_ident(&tail[..end + 1]) {
                out.push_str(&d);
                rest = &tail[end + 1..];
                continue;
            }
        }
        out.push_str("__q");
        rest = &tail[3..];
    }
    out.push_str(rest);
    std::borrow::Cow::Owned(out)
}

fn push_std_literal(out: &mut String, content: &str) {
    out.push('\'');
    for c in content.chars() {
        if c == '\'' {
            out.push('\'');
        }
        out.push(c);
    }
    out.push('\'');
}

fn syntax_error_near(tok: &str) -> String {
    format!("syntax error at or near \"{}\"", tok)
}

/// Đọc chuỗi `'...'` chuẩn bắt đầu ở `i` (ký tự `'`). Trả (nội dung đã bỏ `''`, vị trí sau dấu đóng).
fn read_std_string(chars: &[char], i: usize) -> Option<(String, usize)> {
    let mut j = i + 1;
    let mut s = String::new();
    while j < chars.len() {
        if chars[j] == '\'' {
            if j + 1 < chars.len() && chars[j + 1] == '\'' {
                s.push('\'');
                j += 2;
                continue;
            }
            return Some((s, j + 1));
        }
        s.push(chars[j]);
        j += 1;
    }
    None
}

/// Nội dung `E'...'` bắt đầu ở `i` (ký tự `'`).
fn read_escape_string(chars: &[char], i: usize) -> Result<Option<(String, usize)>, String> {
    let mut j = i + 1;
    let mut s = String::new();
    let hex_val = |c: char| c.to_digit(16);
    while j < chars.len() {
        let c = chars[j];
        if c == '\'' {
            if j + 1 < chars.len() && chars[j + 1] == '\'' {
                s.push('\'');
                j += 2;
                continue;
            }
            return Ok(Some((s, j + 1)));
        }
        if c == '\\' && j + 1 < chars.len() {
            let n = chars[j + 1];
            j += 2;
            match n {
                'b' => s.push('\u{8}'),
                'f' => s.push('\u{c}'),
                'n' => s.push('\n'),
                'r' => s.push('\r'),
                't' => s.push('\t'),
                '0'..='7' => {
                    let mut v = n.to_digit(8).unwrap();
                    let mut k = 0;
                    while k < 2 && j < chars.len() && chars[j].is_digit(8) {
                        v = v * 8 + chars[j].to_digit(8).unwrap();
                        j += 1;
                        k += 1;
                    }
                    if v == 0 {
                        return Err("invalid byte sequence for encoding \"UTF8\": 0x00".to_string());
                    }
                    s.push(char::from_u32(v & 0xFF).unwrap_or('\u{FFFD}'));
                }
                'x' if j < chars.len() && hex_val(chars[j]).is_some() => {
                    let mut v = hex_val(chars[j]).unwrap();
                    j += 1;
                    if j < chars.len() {
                        if let Some(d) = hex_val(chars[j]) {
                            v = v * 16 + d;
                            j += 1;
                        }
                    }
                    if v == 0 {
                        return Err("invalid byte sequence for encoding \"UTF8\": 0x00".to_string());
                    }
                    s.push(char::from_u32(v).unwrap_or('\u{FFFD}'));
                }
                'u' | 'U' => {
                    let len = if n == 'u' { 4 } else { 8 };
                    if j + len > chars.len() || !chars[j..j + len].iter().all(|c| c.is_ascii_hexdigit()) {
                        return Err("invalid Unicode escape".to_string());
                    }
                    let v = u32::from_str_radix(&chars[j..j + len].iter().collect::<String>(), 16).unwrap();
                    j += len;
                    match char::from_u32(v) {
                        Some(ch) if v != 0 => s.push(ch),
                        _ => return Err("invalid Unicode escape value".to_string()),
                    }
                }
                other => s.push(other),
            }
            continue;
        }
        s.push(c);
        j += 1;
    }
    Ok(None)
}

/// Giải mã nội dung `U&'...'` với ký tự escape `esc`.
fn decode_unicode_escapes(raw: &str, esc: char) -> Result<String, String> {
    let chars: Vec<char> = raw.chars().collect();
    let mut s = String::new();
    let mut j = 0;
    while j < chars.len() {
        let c = chars[j];
        if c != esc {
            s.push(c);
            j += 1;
            continue;
        }
        if j + 1 < chars.len() && chars[j + 1] == esc {
            s.push(esc);
            j += 2;
            continue;
        }
        let (start, len) = if j + 1 < chars.len() && chars[j + 1] == '+' { (j + 2, 6) } else { (j + 1, 4) };
        if start + len > chars.len() || !chars[start..start + len].iter().all(|c| c.is_ascii_hexdigit()) {
            return Err("invalid Unicode escape".to_string());
        }
        let v = u32::from_str_radix(&chars[start..start + len].iter().collect::<String>(), 16).unwrap();
        match char::from_u32(v) {
            Some(ch) if v != 0 => s.push(ch),
            _ => return Err("invalid Unicode escape value".to_string()),
        }
        j = start + len;
    }
    Ok(s)
}

fn skip_ws(chars: &[char], mut j: usize) -> (usize, bool) {
    let mut newline = false;
    while j < chars.len() && chars[j].is_whitespace() {
        if chars[j] == '\n' || chars[j] == '\r' {
            newline = true;
        }
        j += 1;
    }
    (j, newline)
}

/// Thân `$$` của câu lệnh này phải giữ nguyên (DO, CREATE FUNCTION/PROCEDURE).
fn keeps_dollar_body(stmt_prefix: &str) -> bool {
    let up = stmt_prefix.trim_start().to_uppercase();
    if up.starts_with("DO") {
        return true;
    }
    up.starts_with("CREATE") && (up.contains("FUNCTION") || up.contains("PROCEDURE"))
}

/// Có cần chạy lexer không: chỉ khi câu lệnh có cú pháp mà engine không tự hiểu.
fn needs_lex(sql: &str) -> bool {
    let b = sql.as_bytes();
    for i in 0..b.len() {
        match b[i] {
            b'-' if b.get(i + 1) == Some(&b'-') => return true,
            b'/' if b.get(i + 1) == Some(&b'*') => return true,
            b'$' if b.get(i + 1).is_some_and(|c| !c.is_ascii_digit()) => return true,
            b'"' | b'\\' | b'&' => return true,
            b'\'' => {
                // tiền tố E/U/B/X hoặc chuỗi liền kề
                if i > 0 && matches!(b[i - 1], b'e' | b'E' | b'b' | b'B' | b'x' | b'X') {
                    return true;
                }
                // literal có kiểu: int '5', DATE '...'
                let mut k = i;
                while k > 0 && b[k - 1] == b' ' {
                    k -= 1;
                }
                let mut w = k;
                while w > 0 && b[w - 1].is_ascii_alphabetic() {
                    w -= 1;
                }
                if k < i && w < k && (w == 0 || !is_ident_char(b[w - 1] as char)) && typed_literal_type(&sql[w..k]).is_some() {
                    return true;
                }
            }
            b'0' if matches!(b.get(i + 1), Some(b'x' | b'X' | b'o' | b'O' | b'b' | b'B')) => return true,
            b'_' if i > 0 && b[i - 1].is_ascii_digit() && b.get(i + 1).is_some_and(|c| c.is_ascii_digit()) => return true,
            _ => {}
        }
    }
    // chuỗi liền kề: '...' <khoảng trắng> '...'
    adjacent_literals_possible(b)
}

fn adjacent_literals_possible(b: &[u8]) -> bool {
    let mut in_q = false;
    let mut i = 0;
    while i < b.len() {
        if b[i] == b'\'' {
            if in_q && b.get(i + 1) == Some(&b'\'') {
                i += 2;
                continue;
            }
            if in_q {
                let mut j = i + 1;
                while j < b.len() && b[j].is_ascii_whitespace() {
                    j += 1;
                }
                if j > i + 1 && b.get(j) == Some(&b'\'') {
                    return true;
                }
            }
            in_q = !in_q;
        }
        i += 1;
    }
    in_q // chuỗi chưa đóng: để lexer báo lỗi
}

/// Tên kiểu dùng được trong literal có kiểu (`int '5'`) → tên kiểu chuẩn cho `::`.
fn typed_literal_type(word: &str) -> Option<&'static str> {
    Some(match word.to_uppercase().as_str() {
        "INT" | "INTEGER" | "INT4" => "int4",
        "BIGINT" | "INT8" => "int8",
        "SMALLINT" | "INT2" => "int2",
        "NUMERIC" | "DECIMAL" => "numeric",
        "REAL" | "FLOAT4" => "float4",
        "FLOAT8" => "float8",
        "TEXT" => "text",
        "VARCHAR" => "varchar",
        "BOOLEAN" | "BOOL" => "bool",
        "JSON" => "json",
        "JSONB" => "jsonb",
        "UUID" => "uuid",
        "BYTEA" => "bytea",
        "DATE" => "date",
        "TIME" => "time",
        "TIMETZ" => "timetz",
        "TIMESTAMP" => "timestamp",
        "TIMESTAMPTZ" => "timestamptz",
        "INTERVAL" => "interval",
        "MONEY" => "money",
        "INET" => "inet",
        "CIDR" => "cidr",
        _ => return None,
    })
}

/// Trường sau `INTERVAL '..'`: `DAY`, `HOUR TO MINUTE`... Trả (vị trí sau trường, đơn vị đầu).
fn interval_field(chars: &[char], from: usize) -> Option<(usize, &'static str)> {
    const UNITS: &[(&str, &str)] = &[("YEAR", "years"), ("MONTH", "months"), ("DAY", "days"), ("HOUR", "hours"), ("MINUTE", "minutes"), ("SECOND", "seconds")];
    let word_at = |p: usize| -> Option<(usize, String)> {
        let (k, _) = skip_ws(chars, p);
        let mut e = k;
        while e < chars.len() && chars[e].is_alphabetic() {
            e += 1;
        }
        (e > k).then(|| (e, chars[k..e].iter().collect::<String>().to_uppercase()))
    };
    let (e1, w1) = word_at(from)?;
    let unit = UNITS.iter().find(|(u, _)| *u == w1)?.1;
    let mut end = e1;
    if let Some((e2, w2)) = word_at(e1) {
        if w2 == "TO" {
            if let Some((e3, w3)) = word_at(e2) {
                if UNITS.iter().any(|(u, _)| *u == w3) {
                    end = e3;
                }
            }
        }
    }
    Some((end, unit))
}

/// Chuẩn hoá `sql` (xem đầu file). Lỗi cú pháp trả `Err` theo đúng thông báo PostgreSQL.
pub(crate) fn lex(sql: &str) -> Result<String, String> {
    if !needs_lex(sql) {
        return Ok(sql.to_string());
    }
    let chars: Vec<char> = sql.chars().collect();
    let n = chars.len();
    let mut out = String::with_capacity(sql.len());
    let mut stmt_start = 0usize; // vị trí trong `out` của đầu câu lệnh hiện tại
    let mut depth = 0i32;
    let mut i = 0;
    while i < n {
        let c = chars[i];
        let prev_ident = i > 0 && is_ident_char(chars[i - 1]);
        // comment
        if c == '-' && i + 1 < n && chars[i + 1] == '-' {
            while i < n && chars[i] != '\n' {
                i += 1;
            }
            out.push(' ');
            continue;
        }
        if c == '/' && i + 1 < n && chars[i + 1] == '*' {
            let mut level = 0;
            while i < n {
                if chars[i] == '/' && i + 1 < n && chars[i + 1] == '*' {
                    level += 1;
                    i += 2;
                } else if chars[i] == '*' && i + 1 < n && chars[i + 1] == '/' {
                    level -= 1;
                    i += 2;
                    if level == 0 {
                        break;
                    }
                } else {
                    i += 1;
                }
            }
            if level != 0 {
                return Err("unterminated /* comment at or near \"/*\"".to_string());
            }
            out.push(' ');
            continue;
        }
        // chuỗi chuẩn (kèm nối chuỗi liền kề)
        if c == '\'' {
            let Some((mut content, mut j)) = read_std_string(&chars, i) else {
                return Err("unterminated quoted string at or near \"'\"".to_string());
            };
            loop {
                let (k, newline) = skip_ws(&chars, j);
                if k > j && k < n && chars[k] == '\'' {
                    let Some((more, next)) = read_std_string(&chars, k) else { break };
                    if !newline {
                        let tok: String = chars[k..next].iter().collect();
                        return Err(syntax_error_near(&tok));
                    }
                    content.push_str(&more);
                    j = next;
                    continue;
                }
                break;
            }
            push_std_literal(&mut out, &content);
            i = j;
            continue;
        }
        // định danh có nháy
        if c == '"' {
            let mut j = i + 1;
            let mut name = String::new();
            let mut closed = false;
            while j < n {
                if chars[j] == '"' {
                    if j + 1 < n && chars[j + 1] == '"' {
                        name.push('"');
                        j += 2;
                        continue;
                    }
                    closed = true;
                    j += 1;
                    break;
                }
                name.push(chars[j]);
                j += 1;
            }
            if !closed {
                return Err("unterminated quoted identifier at or near \"\"\"".to_string());
            }
            if name.is_empty() {
                return Err("zero-length delimited identifier at or near \"\"\"\"".to_string());
            }
            // Tên extension ("uuid-ossp") là tên đối tượng ngoài, không phải bảng/cột
            if ident_is_plain(&name) || out[stmt_start..].to_ascii_uppercase().contains("EXTENSION") {
                out.push('"');
                out.push_str(&name);
                out.push('"');
            } else {
                out.push_str(&encode_ident(&name));
            }
            i = j;
            continue;
        }
        // tham số $1 / chuỗi $tag$
        if c == '$' && !prev_ident {
            if i + 1 < n && chars[i + 1].is_ascii_digit() {
                out.push(c);
                i += 1;
                while i < n && chars[i].is_ascii_digit() {
                    out.push(chars[i]);
                    i += 1;
                }
                continue;
            }
            let mut j = i + 1;
            while j < n && (chars[j].is_alphanumeric() || chars[j] == '_') {
                j += 1;
            }
            let tag_ok = j < n && chars[j] == '$' && (j == i + 1 || is_ident_start(chars[i + 1]));
            if tag_ok {
                let tag: Vec<char> = chars[i..=j].to_vec();
                let body_start = j + 1;
                let mut k = body_start;
                let mut end = None;
                while k + tag.len() <= n {
                    if chars[k] == '$' && chars[k..k + tag.len()] == tag[..] {
                        end = Some(k);
                        break;
                    }
                    k += 1;
                }
                let Some(end) = end else {
                    return Err("unterminated dollar-quoted string at or near \"".to_string() + &tag.iter().collect::<String>() + "\"");
                };
                let after = end + tag.len();
                if keeps_dollar_body(&out[stmt_start..]) {
                    out.extend(chars[i..after].iter());
                } else {
                    let body: String = chars[body_start..end].iter().collect();
                    push_std_literal(&mut out, &body);
                }
                i = after;
                continue;
            }
            out.push(c);
            i += 1;
            continue;
        }
        // định danh / từ khoá, kèm tiền tố chuỗi E'' U&'' B'' X''
        if is_ident_start(c) && !prev_ident {
            let mut j = i;
            while j < n && is_ident_char(chars[j]) {
                j += 1;
            }
            let word: String = chars[i..j].iter().collect();
            if j < n && chars[j] == '\'' && word.len() == 1 {
                match word.as_str() {
                    "E" | "e" => {
                        let Some((content, next)) = read_escape_string(&chars, j)? else {
                            return Err("unterminated quoted string at or near \"E'\"".to_string());
                        };
                        push_std_literal(&mut out, &content);
                        i = next;
                        continue;
                    }
                    "B" | "b" | "X" | "x" => {
                        let Some((content, next)) = read_std_string(&chars, j) else {
                            return Err("unterminated bit string literal".to_string());
                        };
                        let bits = if word.eq_ignore_ascii_case("b") {
                            if let Some(bad) = content.chars().find(|c| *c != '0' && *c != '1') {
                                return Err(format!("\"{}\" is not a valid binary digit", bad));
                            }
                            content
                        } else {
                            let mut s = String::new();
                            for ch in content.chars() {
                                let Some(d) = ch.to_digit(16) else {
                                    return Err(format!("\"{}\" is not a valid hexadecimal digit", ch));
                                };
                                s.push_str(&format!("{:04b}", d));
                            }
                            s
                        };
                        let len = bits.len();
                        push_std_literal(&mut out, &bits);
                        out.push_str(&format!("::BIT({})", len.max(1)));
                        i = next;
                        continue;
                    }
                    _ => {}
                }
            }
            if (word == "U" || word == "u") && j + 1 < n && chars[j] == '&' && chars[j + 1] == '\'' {
                let Some((raw, mut next)) = read_std_string(&chars, j + 1) else {
                    return Err("unterminated quoted string at or near \"U&'\"".to_string());
                };
                let mut esc = '\\';
                let (k, _) = skip_ws(&chars, next);
                let kw: String = chars[k..(k + 7).min(n)].iter().collect();
                if kw.eq_ignore_ascii_case("UESCAPE") {
                    let (q, _) = skip_ws(&chars, k + 7);
                    if let Some((e, after)) = (q < n && chars[q] == '\'').then(|| read_std_string(&chars, q)).flatten() {
                        let mut ec = e.chars();
                        match (ec.next(), ec.next()) {
                            (Some(ch), None) if !ch.is_ascii_hexdigit() && !"+'\" ".contains(ch) => esc = ch,
                            _ => return Err("invalid Unicode escape character".to_string()),
                        }
                        next = after;
                    }
                }
                push_std_literal(&mut out, &decode_unicode_escapes(&raw, esc)?);
                i = next;
                continue;
            }
            // literal có kiểu: int '5', DATE '2024-01-01', INTERVAL '1' DAY → '...'::kiểu
            if let Some(ty) = typed_literal_type(&word) {
                let (k, newline) = skip_ws(&chars, j);
                let prev_word = out.trim_end().rsplit(|c: char| !is_ident_char(c)).next().unwrap_or("").to_uppercase();
                if k < n && chars[k] == '\'' && !newline && !matches!(prev_word.as_str(), "AT" | "WITH" | "WITHOUT") {
                    if let Some((mut content, next)) = read_std_string(&chars, k) {
                        let mut after = next;
                        if ty == "interval" {
                            if let Some((unit_len, unit)) = interval_field(&chars, next) {
                                if content.trim().parse::<f64>().is_ok() {
                                    content = format!("{} {}", content.trim(), unit);
                                }
                                after = unit_len;
                            }
                        }
                        push_std_literal(&mut out, &content);
                        out.push_str("::");
                        out.push_str(ty);
                        i = after;
                        continue;
                    }
                }
            }
            out.push_str(&word);
            i = j;
            continue;
        }
        // số: 0x.. 0o.. 0b.., gạch dưới giữa các chữ số
        if c.is_ascii_digit() && !prev_ident && !(i > 0 && chars[i - 1] == '.') {
            if c == '0' && i + 2 < n && matches!(chars[i + 1], 'x' | 'X' | 'o' | 'O' | 'b' | 'B') {
                let radix = match chars[i + 1] {
                    'x' | 'X' => 16,
                    'o' | 'O' => 8,
                    _ => 2,
                };
                let mut j = i + 2;
                let mut digits = String::new();
                while j < n && (chars[j].is_digit(radix) || (chars[j] == '_' && j + 1 < n && chars[j + 1].is_digit(radix))) {
                    if chars[j] != '_' {
                        digits.push(chars[j]);
                    }
                    j += 1;
                }
                if !digits.is_empty() && !(j < n && is_ident_char(chars[j])) {
                    match i128::from_str_radix(&digits, radix) {
                        Ok(v) => out.push_str(&v.to_string()),
                        Err(_) => return Err(format!("value \"{}\" is out of range for type bigint", chars[i..j].iter().collect::<String>())),
                    }
                    i = j;
                    continue;
                }
                if j < n && is_ident_char(chars[j]) {
                    let mut e = j;
                    while e < n && is_ident_char(chars[e]) {
                        e += 1;
                    }
                    let tok: String = chars[i..e].iter().collect();
                    return Err(format!("trailing junk after numeric literal at or near \"{}\"", tok));
                }
            }
            let mut j = i;
            while j < n && (chars[j].is_ascii_digit() || chars[j] == '.' || (chars[j] == '_' && j + 1 < n && chars[j + 1].is_ascii_digit() && chars[j - 1].is_ascii_digit())) {
                if chars[j] == '.' && j + 1 < n && chars[j + 1] == '.' {
                    break; // a..b (FOR i IN 1..10)
                }
                if chars[j] != '_' {
                    out.push(chars[j]);
                }
                j += 1;
            }
            i = j;
            continue;
        }
        match c {
            '(' => depth += 1,
            ')' => depth -= 1,
            ';' if depth <= 0 => {
                out.push(c);
                stmt_start = out.len();
                i += 1;
                continue;
            }
            _ => {}
        }
        out.push(c);
        i += 1;
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn literals() {
        assert_eq!(lex(r"SELECT E'tab\there', E'q\'x'").unwrap(), "SELECT 'tab\there', 'q''x'");
        assert_eq!(lex("SELECT $q$it's$q$, $$a$$").unwrap(), "SELECT 'it''s', 'a'");
        assert_eq!(lex("SELECT 'a'\n'b'").unwrap(), "SELECT 'ab'");
        assert!(lex("SELECT 'a' 'b'").unwrap_err().contains("syntax error at or near \"'b'\""));
        assert_eq!(lex("SELECT 0x1F, 0o17, 0b101, 1_000_000, 1.5").unwrap(), "SELECT 31, 15, 5, 1000000, 1.5");
        assert_eq!(lex("SELECT 1 /* a /* b */ c */ + 2 -- x").unwrap(), "SELECT 1   + 2  ");
        assert_eq!(lex(r"SELECT U&'d\0061t\+000061'").unwrap(), "SELECT 'data'");
        assert_eq!(lex("SELECT U&'d!0061t' UESCAPE '!'").unwrap(), "SELECT 'dat'");
        assert_eq!(lex("SELECT B'101', X'1F'").unwrap(), "SELECT '101'::BIT(3), '00011111'::BIT(8)");
        assert_eq!(lex("SELECT $1, t1_2, a$b FROM t WHERE x = $2").unwrap(), "SELECT $1, t1_2, a$b FROM t WHERE x = $2");
        assert_eq!(lex("DO $$ BEGIN RAISE NOTICE 'x--y'; END $$").unwrap(), "DO $$ BEGIN RAISE NOTICE 'x--y'; END $$");
        assert_eq!(lex("SELECT 1; DO $$ x $$").unwrap(), "SELECT 1; DO $$ x $$");
        assert_eq!(lex("SELECT 'it''s' 'x'").is_err(), true);
        assert_eq!(lex("SELECT int '5', DATE '2024-01-01', INTERVAL '1' DAY, INTERVAL '2 hours'").unwrap(), "SELECT '5'::int4, '2024-01-01'::date, '1 days'::interval, '2 hours'::interval");
        assert_eq!(lex("SELECT now() AT TIME ZONE 'UTC'").unwrap(), "SELECT now() AT TIME ZONE 'UTC'");
    }

    #[test]
    fn quoted_identifiers_round_trip() {
        let l = lex("SELECT \"Some Col\", \"createdAt\", \"Tên\" FROM \"Mixed Case\"").unwrap();
        assert!(l.contains("\"createdAt\"") && l.contains("\"Tên\"") && !l.contains("Some Col"));
        assert_eq!(decode_all(&l), "SELECT Some Col, \"createdAt\", \"Tên\" FROM Mixed Case");
        assert_eq!(decode_ident(&encode_ident("a\"b")).unwrap(), "a\"b");
        assert!(lex("SELECT \"\"").is_err());
        assert_eq!(lex("CREATE EXTENSION IF NOT EXISTS \"uuid-ossp\"").unwrap(), "CREATE EXTENSION IF NOT EXISTS \"uuid-ossp\"");
    }
}
