//! Viết lại một số toán tử PostgreSQL thành dạng engine đã hiểu, ngay sau lexer (nên mọi đường thực
//! thi, đặt tên cột wire và suy kiểu đều thấy cùng một câu):
//! - `x ISNULL` / `x NOTNULL` → `x IS NULL` / `x IS NOT NULL`;
//! - `BETWEEN SYMMETRIC a AND b` → `BETWEEN LEAST(a, b) AND GREATEST(a, b)`;
//! - toán tử tiền tố `|/ x`, `||/ x`, `@ x` → `(sqrt(x) + 0)`, `(cbrt(x) + 0)`, `(abs(x) + 0)` (`+ 0` giữ tên
//!   cột `?column?` như PostgreSQL);
//! - `x [NOT] SIMILAR TO 'mẫu' [ESCAPE 'c']` → `x ~ '^(?:regex)$'` / `x !~ ...`;
//! - `x AT TIME ZONE z` → `timezone(z, x)` (cùng hàm PostgreSQL dùng; tên cột `timezone`);
//! - `x IS [NOT] [NFC|NFD|NFKC|NFKD] NORMALIZED` → `[NOT] is_normalized(x, 'NFC')`.
//! Chuỗi `'...'`, định danh `"..."` và thân `$$...$$` không bị đụng tới.

/// Lọc nhanh, một lượt qua câu (chạy cho mọi câu lệnh): có `@` tiền tố, `|/`, hoặc một trong các từ
/// khoá ISNULL, NOTNULL, SYMMETRIC, SIMILAR, ZONE, NORMALIZED.
fn may_need(sql: &str) -> bool {
    const WORDS: [&[u8]; 6] = [b"ISNULL", b"NOTNULL", b"SYMMETRIC", b"SIMILAR", b"ZONE", b"NORMALIZED"];
    let b = sql.as_bytes();
    let mut i = 0;
    while i < b.len() {
        let c = b[i];
        if c == b'@' && !matches!(b.get(i + 1), Some(b'>' | b'@' | b'?')) || c == b'|' && b.get(i + 1) == Some(&b'/') {
            return true;
        }
        if c.is_ascii_alphabetic() {
            let start = i;
            while i < b.len() && (b[i].is_ascii_alphanumeric() || b[i] == b'_') {
                i += 1;
            }
            let w = &b[start..i];
            if (4..=10).contains(&w.len()) && WORDS.iter().any(|k| k.eq_ignore_ascii_case(w)) {
                return true;
            }
            continue;
        }
        i += 1;
    }
    false
}

/// Mặt nạ "là code" theo byte: false trong chuỗi, định danh có nháy, thân `$$`.
fn code_mask(s: &str) -> Vec<bool> {
    let b = s.as_bytes();
    let mut m = vec![true; b.len()];
    let mut i = 0;
    while i < b.len() {
        match b[i] {
            b'\'' | b'"' => {
                let q = b[i];
                let start = i;
                i += 1;
                while i < b.len() {
                    if b[i] == q {
                        if b.get(i + 1) == Some(&q) {
                            i += 2;
                            continue;
                        }
                        break;
                    }
                    i += 1;
                }
                let end = (i + 1).min(b.len());
                m[start..end].iter_mut().for_each(|x| *x = false);
                i = end;
            }
            b'$' if b.get(i + 1) == Some(&b'$') => {
                let start = i;
                match s[i + 2..].find("$$") {
                    Some(e) => {
                        let end = i + 2 + e + 2;
                        m[start..end].iter_mut().for_each(|x| *x = false);
                        i = end;
                    }
                    None => {
                        m[start..].iter_mut().for_each(|x| *x = false);
                        i = b.len();
                    }
                }
            }
            _ => i += 1,
        }
    }
    m
}

fn is_word(c: u8) -> bool {
    c.is_ascii_alphanumeric() || c == b'_'
}

/// Vị trí đầu tiên của từ khoá `kw` (ASCII, không phân biệt hoa thường) trong code, có biên từ.
fn find_kw(s: &str, m: &[bool], kw: &str, from: usize) -> Option<usize> {
    let b = s.as_bytes();
    let k = kw.as_bytes();
    (from..b.len().saturating_sub(k.len() - 1)).find(|&i| {
        m[i] && b[i..i + k.len()].eq_ignore_ascii_case(k) && (i == 0 || !is_word(b[i - 1])) && b.get(i + k.len()).map_or(true, |c| !is_word(*c))
    })
}

/// Toán hạng "primary" bắt đầu tại `i` (sau khoảng trắng): số, định danh (có dấu chấm) kèm `(...)`,
/// `[...]`, ngoặc `(...)`, chuỗi `'...'`, có thể có dấu `-`/`+` đơn và chuỗi ép kiểu `::type`. Trả vị trí kết thúc.
fn primary_end(s: &str, m: &[bool], mut i: usize) -> Option<usize> {
    let b = s.as_bytes();
    while i < b.len() && b[i].is_ascii_whitespace() {
        i += 1;
    }
    if i < b.len() && (b[i] == b'-' || b[i] == b'+') {
        i += 1;
        while i < b.len() && b[i].is_ascii_whitespace() {
            i += 1;
        }
    }
    let start = i;
    if i >= b.len() {
        return None;
    }
    if b[i] == b'(' {
        i = super::constraints::matching_paren(s, i)? + 1;
    } else if !m[i] {
        while i < b.len() && !m[i] {
            i += 1;
        }
    } else if is_word(b[i]) || b[i] == b'$' {
        while i < b.len() && (is_word(b[i]) || b[i] == b'.' || b[i] == b'$') {
            i += 1;
        }
        if i < b.len() && b[i] == b'(' {
            i = super::constraints::matching_paren(s, i)? + 1;
        }
    } else {
        return None;
    }
    // subscript và ép kiểu
    loop {
        if i < b.len() && b[i] == b'[' {
            i = s[i..].find(']').map(|e| i + e + 1)?;
            continue;
        }
        if i + 1 < b.len() && b[i] == b':' && b[i + 1] == b':' {
            i += 2;
            while i < b.len() && (is_word(b[i]) || b[i] == b'[' || b[i] == b']') {
                i += 1;
            }
            if i < b.len() && b[i] == b'(' {
                i = super::constraints::matching_paren(s, i)? + 1;
            }
            continue;
        }
        break;
    }
    (i > start).then_some(i)
}

/// Toán hạng primary kết thúc ngay trước `end` (bỏ khoảng trắng): trả vị trí bắt đầu.
fn primary_start(s: &str, m: &[bool], end: usize) -> Option<usize> {
    let b = s.as_bytes();
    let mut j = end;
    while j > 0 && b[j - 1].is_ascii_whitespace() {
        j -= 1;
    }
    if j == 0 {
        return None;
    }
    loop {
        let start = if b[j - 1] == b')' || b[j - 1] == b']' {
            let close = j - 1;
            let (open_c, close_c) = if b[close] == b')' { (b'(', b')') } else { (b'[', b']') };
            let mut depth = 0i32;
            let mut k = close as isize;
            while k >= 0 {
                let c = b[k as usize];
                if m[k as usize] {
                    if c == close_c {
                        depth += 1;
                    } else if c == open_c {
                        depth -= 1;
                        if depth == 0 {
                            break;
                        }
                    }
                }
                k -= 1;
            }
            if k < 0 {
                return None;
            }
            let mut st = k as usize;
            while st > 0 && (is_word(b[st - 1]) || b[st - 1] == b'.') {
                st -= 1;
            }
            st
        } else if !m[j - 1] {
            let mut st = j - 1;
            while st > 0 && !m[st - 1] {
                st -= 1;
            }
            st
        } else if is_word(b[j - 1]) {
            let mut st = j - 1;
            while st > 0 && (is_word(b[st - 1]) || b[st - 1] == b'.' || b[st - 1] == b'$') {
                st -= 1;
            }
            st
        } else {
            return None;
        };
        // còn ép kiểu phía trước: `'..'::timestamp`, `x::timestamp with time zone`
        let mut p = start;
        let word = s[start..j].to_ascii_lowercase();
        // tên kiểu nhiều từ: `with time zone`, `without time zone`, `double precision`, `character varying`
        let prev_words = |pos: usize, n: usize| -> Option<(usize, String)> {
            let mut q = pos;
            let mut words = Vec::new();
            for _ in 0..n {
                while q > 0 && b[q - 1].is_ascii_whitespace() {
                    q -= 1;
                }
                let e = q;
                while q > 0 && is_word(b[q - 1]) {
                    q -= 1;
                }
                if q == e {
                    return None;
                }
                words.insert(0, s[q..e].to_ascii_lowercase());
            }
            Some((q, words.join(" ")))
        };
        let multi: &[(&str, usize, &[&str])] = &[("zone", 3, &["timestamp with time", "timestamp without time", "time with time", "time without time"]), ("precision", 1, &["double"]), ("varying", 1, &["character", "bit"])];
        for (w, n, pre) in multi {
            if word == *w {
                if let Some((q, ws)) = prev_words(start, *n) {
                    if pre.contains(&ws.as_str()) {
                        p = q;
                    }
                }
            }
        }
        let before = s[..p].trim_end();
        if before.ends_with("::") {
            j = before.len() - 2;
            continue;
        }
        return Some(start.min(p));
    }
}

/// Mẫu SIMILAR TO → regex (neo hai đầu).
fn similar_to_regex(pat: &str, esc: Option<char>) -> String {
    let mut out = String::from("^(?:");
    let mut chars = pat.chars().peekable();
    let mut in_class = false;
    while let Some(c) = chars.next() {
        if Some(c) == esc {
            if let Some(n) = chars.next() {
                out.push_str(&regex::escape(&n.to_string()));
            }
            continue;
        }
        if in_class {
            if c == ']' {
                in_class = false;
            }
            if c == '\\' {
                out.push('\\');
            }
            out.push(c);
            continue;
        }
        match c {
            '%' => out.push_str(".*"),
            '_' => out.push('.'),
            '[' => {
                in_class = true;
                out.push('[');
            }
            '|' | '*' | '+' | '?' | '{' | '}' | '(' | ')' => out.push(c),
            _ => out.push_str(&regex::escape(&c.to_string())),
        }
    }
    out.push_str(")$");
    out
}

fn sql_quote(s: &str) -> String {
    format!("'{}'", s.replace('\'', "''"))
}

/// Chuỗi `'...'` bắt đầu tại `i` (sau khoảng trắng): (nội dung, vị trí kết thúc).
fn string_literal_at(s: &str, i: usize) -> Option<(String, usize)> {
    let b = s.as_bytes();
    let mut i = i;
    while i < b.len() && b[i].is_ascii_whitespace() {
        i += 1;
    }
    if b.get(i) != Some(&b'\'') {
        return None;
    }
    let mut out = String::new();
    let mut j = i + 1;
    let rest = &s[j..];
    let mut it = rest.char_indices().peekable();
    while let Some((k, c)) = it.next() {
        if c == '\'' {
            if let Some(&(_, '\'')) = it.peek() {
                it.next();
                out.push('\'');
                continue;
            }
            j += k + 1;
            return Some((out, j));
        }
        out.push(c);
    }
    None
}

pub(crate) fn rewrite(sql: &str) -> Option<String> {
    if !may_need(sql) {
        return None;
    }
    let mut s = sql.to_string();
    let mut changed = false;

    // ISNULL / NOTNULL
    for (kw, rep) in [("ISNULL", "IS NULL"), ("NOTNULL", "IS NOT NULL")] {
        let mut from = 0;
        loop {
            let m = code_mask(&s);
            let Some(p) = find_kw(&s, &m, kw, from) else { break };
            s.replace_range(p..p + kw.len(), rep);
            from = p + rep.len();
            changed = true;
        }
    }

    // BETWEEN SYMMETRIC a AND b
    loop {
        let m = code_mask(&s);
        let Some(p) = find_kw(&s, &m, "SYMMETRIC", 0) else { break };
        let a_start = p + "SYMMETRIC".len();
        let Some(and) = find_kw(&s, &m, "AND", a_start) else { break };
        let b_start = and + 3;
        // vế b kết thúc ở `,` / `)` cùng cấp hoặc từ khoá mệnh đề kế tiếp
        let bytes = s.as_bytes();
        let mut depth = 0i32;
        let mut end = bytes.len();
        let stops = ["AND", "OR", "THEN", "WHEN", "ELSE", "END", "AS", "FROM", "WHERE", "GROUP", "ORDER", "LIMIT", "OFFSET", "HAVING", "UNION", "IS"];
        let mut k = b_start;
        while k < bytes.len() {
            if m[k] {
                match bytes[k] {
                    b'(' => depth += 1,
                    b')' if depth == 0 => {
                        end = k;
                        break;
                    }
                    b')' => depth -= 1,
                    b',' | b';' if depth == 0 => {
                        end = k;
                        break;
                    }
                    _ => {}
                }
                if depth == 0 && (k == 0 || !is_word(bytes[k - 1])) && stops.iter().any(|w| {
                    bytes[k..].len() >= w.len() && bytes[k..k + w.len()].eq_ignore_ascii_case(w.as_bytes()) && bytes.get(k + w.len()).map_or(true, |c| !is_word(*c))
                }) {
                    end = k;
                    break;
                }
            }
            k += 1;
        }
        let a = s[a_start..and].trim().to_string();
        let bv = s[b_start..end].trim().to_string();
        let rep = format!("LEAST({a}, {bv}) AND GREATEST({a}, {bv}) ");
        s.replace_range(p..end, &rep);
        changed = true;
    }

    // |/ x, ||/ x, @ x
    loop {
        let m = code_mask(&s);
        let b = s.as_bytes();
        let mut hit = None;
        for i in 0..b.len() {
            if !m[i] {
                continue;
            }
            let (op_len, func) = if b[i..].starts_with(b"||/") {
                (3, "cbrt")
            } else if b[i..].starts_with(b"|/") && (i == 0 || b[i - 1] != b'|') {
                (2, "sqrt")
            } else if b[i] == b'@' && !matches!(b.get(i + 1), Some(b'>' | b'@' | b'?')) && (i == 0 || !matches!(b[i - 1], b'<' | b'@')) {
                (1, "abs")
            } else {
                continue;
            };
            // phải ở vị trí tiền tố: trước đó là đầu câu, `(`, `,`, toán tử hoặc từ khoá
            let prev = s[..i].trim_end();
            let prefix_pos = prev.is_empty()
                || prev.ends_with(|c: char| "(,=<>+-*/%^&|!~".contains(c))
                || {
                    let w = prev.rsplit(|c: char| !(c.is_alphanumeric() || c == '_')).next().unwrap_or("").to_uppercase();
                    ["SELECT", "WHERE", "AND", "OR", "NOT", "THEN", "ELSE", "WHEN", "ON", "BY", "HAVING", "RETURNING", "SET", "VALUES"].contains(&w.as_str())
                };
            if !prefix_pos {
                continue;
            }
            if let Some(e) = primary_end(&s, &m, i + op_len) {
                hit = Some((i, op_len, func, e));
                break;
            }
        }
        let Some((i, op_len, func, e)) = hit else { break };
        let operand = s[i + op_len..e].trim().to_string();
        s.replace_range(i..e, &format!("({}({}) + 0)", func, operand));
        changed = true;
    }

    // [NOT] SIMILAR TO 'pat' [ESCAPE 'c']
    let mut from = 0;
    loop {
        let m = code_mask(&s);
        let Some(p) = find_kw(&s, &m, "SIMILAR", from) else { break };
        let after = &s[p + 7..];
        let to_rel = after.len() - after.trim_start().len();
        let to_pos = p + 7 + to_rel;
        if !s[to_pos..].to_uppercase().starts_with("TO") {
            from = p + 7;
            continue;
        }
        let Some((pat, mut end)) = string_literal_at(&s, to_pos + 2) else {
            from = p + 7;
            continue;
        };
        let mut esc = Some('\\');
        let tail = s[end..].trim_start();
        if tail.len() >= 6 && tail.as_bytes()[..6].eq_ignore_ascii_case(b"ESCAPE") {
            let esc_pos = end + (s[end..].len() - tail.len()) + 6;
            if let Some((e, e_end)) = string_literal_at(&s, esc_pos) {
                esc = e.chars().next();
                end = e_end;
            }
        }
        // NOT ngay trước SIMILAR
        let before = s[..p].trim_end();
        let (start, op) = if before.len() >= 3 && before.as_bytes()[before.len() - 3..].eq_ignore_ascii_case(b"NOT") && !before[..before.len() - 3].ends_with(|c: char| c.is_alphanumeric() || c == '_') {
            (before.len() - 3, "!~")
        } else {
            (p, "~")
        };
        let rep = format!("{} {} ", op, sql_quote(&similar_to_regex(&pat, esc)));
        s.replace_range(start..end, &rep);
        from = start + rep.len();
        changed = true;
    }

    // x AT TIME ZONE z → timezone(z, x); chuỗi `AT TIME ZONE` nối tiếp được xử lý từ trái sang phải
    loop {
        let m = code_mask(&s);
        let mut from = 0;
        let mut found = None;
        while let Some(q) = find_kw(&s, &m, "AT", from) {
            if time_zone_end(&s, q + 2).is_some() {
                found = Some(q);
                break;
            }
            from = q + 2;
        }
        let Some(q) = found else { break };
        if !rewrite_at_time_zone(&mut s, &m, q) {
            break;
        }
        changed = true;
    }

    // x IS [NOT] [form] NORMALIZED
    loop {
        let m = code_mask(&s);
        let Some(p) = find_kw(&s, &m, "NORMALIZED", 0) else { break };
        let before = s[..p].trim_end();
        let mut form = "NFC";
        let mut cut = before.len();
        for f in ["NFKC", "NFKD", "NFC", "NFD"] {
            if before.len() >= f.len() && before.as_bytes()[before.len() - f.len()..].eq_ignore_ascii_case(f.as_bytes()) {
                form = f;
                cut = before.len() - f.len();
                break;
            }
        }
        let b2 = s[..cut].trim_end();
        let (neg, b3) = if b2.len() >= 3 && b2.as_bytes()[b2.len() - 3..].eq_ignore_ascii_case(b"NOT") { (true, s[..b2.len() - 3].trim_end()) } else { (false, b2) };
        if !(b3.len() >= 2 && b3.as_bytes()[b3.len() - 2..].eq_ignore_ascii_case(b"IS")) {
            break;
        }
        let is_pos = b3.len() - 2;
        let Some(xs) = primary_start(&s, &m, is_pos) else { break };
        let x = s[xs..is_pos].trim().to_string();
        let rep = format!("{}is_normalized({}, '{}')", if neg { "NOT " } else { "" }, x, form);
        s.replace_range(xs..p + "NORMALIZED".len(), &rep);
        changed = true;
    }

    changed.then_some(s)
}

/// Sau `AT` tại `i`: khoảng trắng, `TIME`, khoảng trắng, `ZONE` → vị trí ngay sau `ZONE`.
fn time_zone_end(s: &str, i: usize) -> Option<usize> {
    let b = s.as_bytes();
    let mut j = i;
    let mut word = |w: &[u8]| -> bool {
        let ws = j;
        while j < b.len() && b[j].is_ascii_whitespace() {
            j += 1;
        }
        if j == ws || b.len() < j + w.len() || !b[j..j + w.len()].eq_ignore_ascii_case(w) || b.get(j + w.len()).is_some_and(|c| is_word(*c)) {
            return false;
        }
        j += w.len();
        true
    };
    (word(b"TIME") && word(b"ZONE")).then_some(j)
}

fn rewrite_at_time_zone(s: &mut String, m: &[bool], at: usize) -> bool {
    let Some(xs) = primary_start(s, m, at) else { return false };
    let Some(zpos) = time_zone_end(s, at + 2) else { return false };
    let Some(ze) = primary_end(s, m, zpos) else { return false };
    let x = s[xs..at].trim().to_string();
    let z = s[zpos..ze].trim().to_string();
    s.replace_range(xs..ze, &format!("timezone({}, {})", z, x));
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    fn r(s: &str) -> String {
        rewrite(s).unwrap_or_else(|| s.to_string())
    }

    #[test]
    fn rewrites() {
        assert_eq!(r("SELECT n ISNULL, n NOTNULL FROM t"), "SELECT n IS NULL, n IS NOT NULL FROM t");
        assert_eq!(r("SELECT 'ISNULL' AS x"), "SELECT 'ISNULL' AS x");
        assert_eq!(r("SELECT 3 BETWEEN SYMMETRIC 5 AND 1 AS a"), "SELECT 3 BETWEEN LEAST(5, 1) AND GREATEST(5, 1) AS a");
        assert_eq!(r("SELECT |/ 25, ||/ 27, @ -4"), "SELECT (sqrt(25) + 0), (cbrt(27) + 0), (abs(-4) + 0)");
        assert_eq!(r("SELECT meta @> '{}' FROM t"), "SELECT meta @> '{}' FROM t");
        assert_eq!(r("SELECT 'abc' NOT SIMILAR TO 'x%'"), "SELECT 'abc' !~ '^(?:x.*)$' ");
        assert_eq!(r("SELECT now() AT TIME ZONE 'UTC'"), "SELECT timezone('UTC', now())");
        assert_eq!(
            r("SELECT '2024-01-01'::timestamp AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Ho_Chi_Minh' AS a"),
            "SELECT timezone('Asia/Ho_Chi_Minh', timezone('UTC', '2024-01-01'::timestamp)) AS a"
        );
        assert_eq!(r("SELECT x::timestamp with time zone AT TIME ZONE 'UTC'"), "SELECT timezone('UTC', x::timestamp with time zone)");
        assert_eq!(r("SELECT 'a' IS NORMALIZED AS a, 'b' IS NOT NFKC NORMALIZED"), "SELECT is_normalized('a', 'NFC') AS a, NOT is_normalized('b', 'NFKC')");
        assert_eq!(similar_to_regex("(a|b)%", Some('\\')), "^(?:(a|b).*)$");
        assert_eq!(similar_to_regex("a.b_", Some('\\')), "^(?:a\\.b.)$");
    }
}
