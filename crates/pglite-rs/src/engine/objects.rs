//! Đối tượng không phải bảng: VIEW, MATERIALIZED VIEW, SCHEMA, SEQUENCE độc lập, kiểu ENUM.
//!
//! Chúng được lưu bằng các `SchemaOp` mới thêm cuối enum (bản cũ bỏ qua) và dựng lại vào
//! `StorageEngine::objects` khi mở DB; DDL theo giao dịch như bảng (`UndoAction::RestoreObjects`).
//! - VIEW: lưu câu truy vấn. Câu lệnh nhắc tới view được mở rộng thành CTE đặt trước
//!   (`WITH v AS (...) <câu lệnh>`), nên view dùng được ở mọi chỗ CTE dùng được: JOIN, self-join,
//!   subquery, `INSERT ... SELECT`, `UPDATE ... FROM`. View lồng nhau được thêm theo thứ tự phụ thuộc.
//!   Materialized view là bảng thường cộng câu truy vấn để `REFRESH`.
//! - SCHEMA: chỉ là tên. Bảng `app.x` vẫn dùng chung không gian tên với `public` như trước.
//! - SEQUENCE độc lập: thuộc tính + giá trị hiện tại (ghi mỗi lần cấp số; ROLLBACK không trả lại số).
//! - ENUM: danh sách nhãn. Giá trị ghi vào cột kiểu enum phải là nhãn đã khai báo, `x::mood` kiểm tra
//!   nhãn, `ORDER BY cột_enum` sắp theo thứ tự khai báo.
use super::executor::{find_top_level_keyword, split_comma_separated_tokens, strip_schema_keep_case, Executor};
use crate::storage::catalog::{DomainDef, SchemaOp, SequenceSpec, SequenceState, ViewDef};
use crate::types::{QueryResult, Value};
use std::cell::RefCell;
use std::collections::BTreeMap;

thread_local! {
    /// Nhãn của các kiểu ENUM của DB đang chạy câu lệnh (đặt ở đầu mỗi `Executor::execute`), cho
    /// `coltypes::coerce_for_write` — hàm đó không có quyền truy cập catalog.
    static ENUMS: RefCell<BTreeMap<String, Vec<String>>> = const { RefCell::new(BTreeMap::new()) };
}

/// Đặt danh sách enum cho câu lệnh sắp chạy (rẻ khi DB không có enum).
pub(crate) fn set_session_enums(enums: &BTreeMap<String, Vec<String>>) {
    ENUMS.with(|e| {
        let mut e = e.borrow_mut();
        if e.is_empty() && enums.is_empty() {
            return;
        }
        if *e != *enums {
            *e = enums.clone();
        }
    });
}

/// `Some(true/false)`: `type_name` là enum và có / không có nhãn `label`; `None`: không phải enum.
pub(crate) fn enum_has_label(type_name: &str, label: &str) -> Option<bool> {
    ENUMS.with(|e| e.borrow().get(type_name).map(|l| l.iter().any(|x| x == label)))
}

fn done(cmd: &str) -> QueryResult {
    QueryResult { rows: vec![], row_count: 0, fields: vec![], command: cmd.to_string() }
}

/// Tên đối tượng: bỏ schema (`public.`, `app.`) và ngoặc kép, viết thường.
pub(crate) fn object_key(raw: &str) -> String {
    strip_schema_keep_case(raw.trim().trim_end_matches(';')).to_lowercase()
}

fn is_ident_byte(c: u8) -> bool {
    c.is_ascii_alphanumeric() || c == b'_' || c == b'$' || c >= 0x80
}

/// Các định danh nằm ngoài chuỗi / comment / dollar-quote: (đầu, cuối, tên không có ngoặc kép).
fn ident_words(sql: &str) -> Vec<(usize, usize, String)> {
    let b = sql.as_bytes();
    let mut out = Vec::new();
    let mut i = 0;
    while i < b.len() {
        let c = b[i];
        if c == b'\'' {
            let esc = i > 0 && (b[i - 1] == b'E' || b[i - 1] == b'e') && (i < 2 || !is_ident_byte(b[i - 2]));
            i += 1;
            while i < b.len() {
                if esc && b[i] == b'\\' {
                    i += 2;
                    continue;
                }
                if b[i] == b'\'' {
                    if b.get(i + 1) == Some(&b'\'') {
                        i += 2;
                        continue;
                    }
                    break;
                }
                i += 1;
            }
            i += 1;
        } else if c == b'-' && b.get(i + 1) == Some(&b'-') {
            while i < b.len() && b[i] != b'\n' {
                i += 1;
            }
        } else if c == b'/' && b.get(i + 1) == Some(&b'*') {
            let mut depth = 0;
            while i < b.len() {
                if b[i] == b'/' && b.get(i + 1) == Some(&b'*') {
                    depth += 1;
                    i += 2;
                } else if b[i] == b'*' && b.get(i + 1) == Some(&b'/') {
                    depth -= 1;
                    i += 2;
                    if depth == 0 {
                        break;
                    }
                } else {
                    i += 1;
                }
            }
        } else if c == b'$' && (i == 0 || !is_ident_byte(b[i - 1])) && !b.get(i + 1).map_or(false, |x| x.is_ascii_digit()) {
            // $tag$ ... $tag$ (tham số $1 thì bỏ qua)
            let tag_end = b[i + 1..].iter().position(|&x| !(x.is_ascii_alphanumeric() || x == b'_')).map(|p| i + 1 + p);
            match tag_end {
                Some(e) if b[e] == b'$' => {
                    let tag = &sql[i..=e];
                    i = match sql[e + 1..].find(tag) {
                        Some(p) => e + 1 + p + tag.len(),
                        None => b.len(),
                    };
                }
                _ => i += 1,
            }
        } else if c == b'"' {
            let end = sql[i + 1..].find('"').map_or(b.len(), |p| i + 1 + p);
            out.push((i, (end + 1).min(b.len()), sql[i + 1..end].to_string()));
            i = end + 1;
        } else if (c.is_ascii_alphabetic() || c == b'_' || c >= 0x80) && (i == 0 || !is_ident_byte(b[i - 1])) {
            let s = i;
            while i < b.len() && is_ident_byte(b[i]) {
                i += 1;
            }
            out.push((s, i, sql[s..i].to_string()));
        } else {
            i += 1;
        }
    }
    out
}

/// Tên các CTE mà chính câu lệnh định nghĩa (`name [(cols)] AS [NOT] [MATERIALIZED] (`): chúng che view cùng tên.
fn statement_cte_names(sql: &str, words: &[(usize, usize, String)]) -> Vec<String> {
    let b = sql.as_bytes();
    let skip_ws = |mut p: usize| {
        while p < b.len() && b[p].is_ascii_whitespace() {
            p += 1;
        }
        p
    };
    let mut out = Vec::new();
    for (_, end, name) in words {
        let mut p = skip_ws(*end);
        if p < b.len() && b[p] == b'(' {
            match crate::engine::constraints::matching_paren(sql, p) {
                Some(close) => p = skip_ws(close + 1),
                None => continue,
            }
        }
        if !sql[p..].get(..2).map_or(false, |w| w.eq_ignore_ascii_case("AS")) || b.get(p + 2).map_or(true, |&x| is_ident_byte(x)) {
            continue;
        }
        p = skip_ws(p + 2);
        for kw in ["NOT", "MATERIALIZED"] {
            if sql[p..].get(..kw.len()).map_or(false, |w| w.eq_ignore_ascii_case(kw)) && b.get(p + kw.len()).map_or(false, |&x| !is_ident_byte(x)) {
                p = skip_ws(p + kw.len());
            }
        }
        if b.get(p) == Some(&b'(') {
            out.push(name.to_lowercase());
        }
    }
    out
}

/// Lý do không dùng được view làm đích của lệnh ghi (view chỉ đọc).
fn dml_target(words: &[(usize, usize, String)]) -> Option<(&'static str, &str)> {
    let w = |i: usize| words.get(i).map(|x| x.2.as_str()).unwrap_or("");
    let up0 = w(0).to_uppercase();
    match up0.as_str() {
        "INSERT" if w(1).eq_ignore_ascii_case("INTO") => Some(("cannot insert into view", w(2))),
        "UPDATE" => Some(("cannot update view", if w(1).eq_ignore_ascii_case("ONLY") { w(2) } else { w(1) })),
        "DELETE" if w(1).eq_ignore_ascii_case("FROM") => Some(("cannot delete from view", if w(2).eq_ignore_ascii_case("ONLY") { w(3) } else { w(2) })),
        _ => None,
    }
}

/// Thuộc tính sequence đọc từ `CREATE` / `ALTER SEQUENCE`.
#[derive(Default)]
struct SeqOptions {
    type_bounds: Option<(i64, i64)>,
    increment: Option<i64>,
    /// `Some(None)` = `NO MINVALUE` (về mặc định).
    min: Option<Option<i64>>,
    max: Option<Option<i64>>,
    start: Option<i64>,
    /// `Some(None)` = `RESTART` không có giá trị (về START).
    restart: Option<Option<i64>>,
    cycle: Option<bool>,
    rename: Option<String>,
}

fn parse_seq_options(s: &str) -> Result<SeqOptions, String> {
    let toks: Vec<&str> = s.split_whitespace().collect();
    let mut o = SeqOptions::default();
    let mut i = 0;
    let up = |i: usize| toks.get(i).map(|t| t.to_uppercase()).unwrap_or_default();
    let num = |i: &mut usize| -> Result<i64, String> {
        let t = toks.get(*i).copied().unwrap_or("");
        let n = t.replace('_', "").parse::<i64>().map_err(|_| format!("syntax error at or near \"{}\"", t))?;
        *i += 1;
        Ok(n)
    };
    while i < toks.len() {
        let w = up(i);
        i += 1;
        match w.as_str() {
            "AS" => {
                let t = up(i);
                i += 1;
                o.type_bounds = Some(match t.as_str() {
                    "SMALLINT" | "INT2" => (i16::MIN as i64, i16::MAX as i64),
                    "INTEGER" | "INT" | "INT4" => (i32::MIN as i64, i32::MAX as i64),
                    "BIGINT" | "INT8" => (i64::MIN, i64::MAX),
                    _ => return Err("sequence type must be smallint, integer, or bigint".to_string()),
                });
            }
            "INCREMENT" => {
                if up(i) == "BY" {
                    i += 1;
                }
                o.increment = Some(num(&mut i)?);
            }
            "MINVALUE" => o.min = Some(Some(num(&mut i)?)),
            "MAXVALUE" => o.max = Some(Some(num(&mut i)?)),
            "START" => {
                if up(i) == "WITH" {
                    i += 1;
                }
                o.start = Some(num(&mut i)?);
            }
            "RESTART" => {
                if up(i) == "WITH" {
                    i += 1;
                }
                o.restart = Some(match toks.get(i).and_then(|t| t.replace('_', "").parse::<i64>().ok()) {
                    Some(n) => {
                        i += 1;
                        Some(n)
                    }
                    None => None,
                });
            }
            "CACHE" => {
                num(&mut i)?;
            }
            "CYCLE" => o.cycle = Some(true),
            "NO" => {
                let n = up(i);
                i += 1;
                match n.as_str() {
                    "MINVALUE" => o.min = Some(None),
                    "MAXVALUE" => o.max = Some(None),
                    "CYCLE" => o.cycle = Some(false),
                    _ => return Err(format!("syntax error at or near \"{}\"", toks.get(i - 1).copied().unwrap_or(""))),
                }
            }
            // OWNED BY t.c | NONE, OWNER TO r, SET SCHEMA s: không ảnh hưởng gì ở đây
            "OWNED" | "OWNER" => i += 2,
            "SET" => {
                let n = up(i);
                if n == "LOGGED" || n == "UNLOGGED" {
                    i += 1;
                } else {
                    i += 2;
                }
            }
            "LOGGED" | "UNLOGGED" => {}
            "RENAME" if up(i) == "TO" => {
                o.rename = toks.get(i + 1).map(|t| object_key(t));
                i += 2;
            }
            _ => return Err(format!("syntax error at or near \"{}\"", toks[i - 1])),
        }
    }
    Ok(o)
}

/// Thuộc tính đầy đủ của sequence: mặc định như PostgreSQL, rồi kiểm tra miền giá trị.
fn resolve_seq_spec(o: &SeqOptions, old: Option<&SequenceSpec>) -> Result<SequenceSpec, String> {
    let increment = o.increment.or(old.map(|s| s.increment)).unwrap_or(1);
    if increment == 0 {
        return Err("INCREMENT must not be zero".to_string());
    }
    let (tmin, tmax) = o.type_bounds.unwrap_or((i64::MIN, i64::MAX));
    let def_min = if increment > 0 { 1 } else { tmin };
    let def_max = if increment > 0 { tmax } else { -1 };
    let min = match o.min {
        Some(Some(v)) => v,
        Some(None) => def_min,
        None => old.map_or(def_min, |s| s.min),
    };
    let max = match o.max {
        Some(Some(v)) => v,
        Some(None) => def_max,
        None => old.map_or(def_max, |s| s.max),
    };
    if min >= max {
        return Err(format!("MINVALUE ({}) must be less than MAXVALUE ({})", min, max));
    }
    let start = o.start.or(old.map(|s| s.start)).unwrap_or(if increment > 0 { min } else { max });
    if start < min {
        return Err(format!("START value ({}) cannot be less than MINVALUE ({})", start, min));
    }
    if start > max {
        return Err(format!("START value ({}) cannot be greater than MAXVALUE ({})", start, max));
    }
    Ok(SequenceSpec { start, increment, min, max, cycle: o.cycle.or(old.map(|s| s.cycle)).unwrap_or(false) })
}

/// Bỏ `IF EXISTS` và `CASCADE` / `RESTRICT` ở cuối: (danh sách tên, if_exists, cascade).
fn parse_drop_list(rest: &str) -> (Vec<String>, bool, bool) {
    let mut rest = rest.trim().trim_end_matches(';').trim();
    let if_exists = rest.len() > 10 && rest[..10].eq_ignore_ascii_case("IF EXISTS ");
    if if_exists {
        rest = rest[10..].trim();
    }
    let mut cascade = false;
    for kw in [" CASCADE", " RESTRICT"] {
        if rest.len() > kw.len() && rest[rest.len() - kw.len()..].eq_ignore_ascii_case(kw) {
            cascade = kw == " CASCADE";
            rest = rest[..rest.len() - kw.len()].trim();
        }
    }
    (split_comma_separated_tokens(rest).into_iter().map(object_key).filter(|n| !n.is_empty()).collect(), if_exists, cascade)
}

/// Bỏ một tiền tố từ khoá (không phân biệt hoa thường) nếu có, theo sau là khoảng trắng.
fn strip_kw<'a>(s: &'a str, kw: &str) -> Option<&'a str> {
    let s = s.trim_start();
    (s.len() > kw.len() && s[..kw.len()].eq_ignore_ascii_case(kw) && s.as_bytes()[kw.len()].is_ascii_whitespace()).then(|| s[kw.len()..].trim_start())
}

/// Chuỗi nhãn `'a'` → `a`.
fn string_literal(s: &str) -> Option<String> {
    let s = s.trim();
    (s.len() >= 2 && s.starts_with('\'') && s.ends_with('\'')).then(|| s[1..s.len() - 1].replace("''", "'"))
}

/// Phần còn lại sau `n` từ đầu tiên.
fn after_words(sql: &str, n: usize) -> &str {
    let mut rest = sql.trim_start();
    for _ in 0..n {
        let end = rest.find(char::is_whitespace).unwrap_or(rest.len());
        rest = rest[end..].trim_start();
    }
    rest
}

/// `x::dom` / `CAST(x AS dom)` → kiểu gốc của domain.
fn rewrite_domain_casts(sql: &str, domains: &BTreeMap<String, DomainDef>) -> Option<String> {
    let words = ident_words(sql);
    let mut out = sql.to_string();
    let mut changed = false;
    for (k, (s, e, w)) in words.iter().enumerate().rev() {
        let Some(d) = domains.get(&w.to_lowercase()) else { continue };
        let before = sql[..*s].trim_end();
        let in_cast = before.ends_with("::") || (k > 0 && words[k - 1].2.eq_ignore_ascii_case("AS") && sql[*e..].trim_start().starts_with(')'));
        if in_cast {
            out.replace_range(*s..*e, &d.base);
            changed = true;
        }
    }
    changed.then_some(out)
}

/// `t.cột` → `bảng.cột` trong điều kiện của view (alias của bảng gốc).
fn replace_qualifier(cond: &str, alias: &str, base: &str) -> String {
    let words = ident_words(cond);
    let mut out = cond.to_string();
    for (s, e, w) in words.iter().rev() {
        if w.eq_ignore_ascii_case(alias) && cond.as_bytes().get(*e) == Some(&b'.') {
            out.replace_range(*s..*e, base);
        }
    }
    out
}

/// `enum_range(NULL::mood)`, `enum_first(...)`, `enum_last(...)`, `enum_range('a'::mood, 'b'::mood)`.
fn rewrite_enum_functions(sql: &str, enums: &BTreeMap<String, Vec<String>>) -> Result<Option<String>, String> {
    let words = ident_words(sql);
    for (s, e, w) in &words {
        let f = w.to_lowercase();
        if !matches!(f.as_str(), "enum_range" | "enum_first" | "enum_last") || sql.as_bytes().get(*e) != Some(&b'(') {
            continue;
        }
        let Some(close) = crate::engine::constraints::matching_paren(sql, *e) else { continue };
        let args = split_comma_separated_tokens(&sql[e + 1..close]);
        let mut ty = None;
        let mut vals: Vec<Option<String>> = Vec::new();
        for a in &args {
            let a = a.trim();
            let Some(p) = a.rfind("::") else { return Ok(None) };
            let t = a[p + 2..].trim().to_lowercase();
            if !enums.contains_key(&t) {
                return Ok(None);
            }
            ty = Some(t);
            let v = a[..p].trim();
            vals.push(if v.eq_ignore_ascii_case("NULL") { None } else { string_literal(v) });
        }
        let Some(ty) = ty else { return Ok(None) };
        let labels = &enums[&ty];
        let lit = |l: &str| sql_quote(l);
        let repl = match (f.as_str(), vals.as_slice()) {
            ("enum_first", [_]) => lit(labels.first().map(|x| x.as_str()).unwrap_or("")),
            ("enum_last", [_]) => lit(labels.last().map(|x| x.as_str()).unwrap_or("")),
            ("enum_range", [_]) => format!("ARRAY[{}]", labels.iter().map(|l| lit(l)).collect::<Vec<_>>().join(",")),
            ("enum_range", [a, b]) => {
                let idx = |v: &Option<String>, def: usize| -> Result<usize, String> {
                    match v {
                        None => Ok(def),
                        Some(x) => labels.iter().position(|l| l == x).ok_or_else(|| format!("invalid input value for enum {}: \"{}\"", ty, x)),
                    }
                };
                let (i, j) = (idx(a, 0)?, idx(b, labels.len().saturating_sub(1))?);
                let sel: Vec<String> = if i <= j { labels[i..=j].iter().map(|l| lit(l)).collect() } else { vec![] };
                if sel.is_empty() {
                    "'{}'::text[]".to_string()
                } else {
                    format!("ARRAY[{}]", sel.join(","))
                }
            }
            _ => return Ok(None),
        };
        // cả mục SELECT không alias: giữ tên cột của PostgreSQL
        let before = sql[..*s].trim_end().to_uppercase();
        let after = sql[close + 1..].trim_start();
        let aw = after.split(|c: char| !c.is_ascii_alphanumeric()).next().unwrap_or("").to_uppercase();
        let whole = (before.ends_with(',') || before.ends_with("SELECT")) && (after.is_empty() || after.starts_with([',', ';']) || aw == "FROM");
        let repl = if whole { format!("{} AS {}", repl, f) } else { repl };
        let out = format!("{}{}{}", &sql[..*s], repl, &sql[close + 1..]);
        return Ok(Some(rewrite_enum_functions(&out, enums)?.unwrap_or(out)));
    }
    Ok(None)
}

/// Thứ hạng (1, 2, ...) của giá trị enum theo thứ tự khai báo.
fn enum_rank_expr(col: &str, labels: &[String]) -> String {
    let arms: Vec<String> = labels.iter().enumerate().map(|(i, l)| format!("WHEN {} THEN {}", sql_quote(l), i + 1)).collect();
    format!("CASE {} {} END", col, arms.join(" "))
}

fn sql_quote(s: &str) -> String {
    format!("'{}'", s.replace('\'', "''"))
}

impl Executor {
    /// Có đối tượng nào cần xét khi chạy câu lệnh (view thường, sequence độc lập, enum).
    pub(crate) fn has_expandable_objects(&self) -> bool {
        let o = &self.storage.objects;
        !o.sequences.is_empty() || !o.enums.is_empty() || !o.domains.is_empty() || o.views.values().any(|v| !v.materialized)
    }

    /// Mở rộng câu lệnh: thêm CTE cho view / sequence được nhắc tới, ép kiểu `::enum`, `ORDER BY` cột
    /// enum. `None` = không đổi. Chỉ áp dụng cho một câu SELECT / WITH / INSERT / UPDATE / DELETE.
    pub(crate) fn expand_objects(&self, sql: &str) -> Result<Option<String>, String> {
        let words = ident_words(sql);
        let Some(lead) = words.first().map(|w| w.2.to_uppercase()) else { return Ok(None) };
        if !["SELECT", "WITH", "INSERT", "UPDATE", "DELETE", "VALUES"].contains(&lead.as_str()) {
            return Ok(None);
        }
        if super::executor::split_sql_statements(sql).len() > 1 {
            return Ok(None); // từng câu được mở rộng khi chạy riêng
        }
        let objs = &self.storage.objects;
        let (out, changed) = match self.rewrite_type_usage(sql)? {
            Some(s) => (s, true),
            None => (sql.to_string(), false),
        };
        let words = if changed { ident_words(&out) } else { words };
        let ctes = statement_cte_names(&out, &words);
        let shadowed = |name: &str| ctes.iter().any(|c| c == name) || self.storage.get_table(name).is_some();
        let mut needed: Vec<String> = Vec::new();
        let mut seqs: Vec<String> = Vec::new();
        for (k, (start, _, w)) in words.iter().enumerate() {
            let key = w.to_lowercase();
            if objs.views.get(&key).map_or(false, |v| !v.materialized) && !shadowed(&key) {
                if !needed.contains(&key) {
                    needed.push(key);
                }
            } else if objs.sequences.contains_key(&key) && !shadowed(&key) && !seqs.contains(&key) {
                // sequence chỉ khi đứng sau FROM / JOIN (có thể kèm schema)
                let qualified = *start > 0 && out.as_bytes()[start - 1] == b'.';
                let prev = if qualified { k.checked_sub(2) } else { k.checked_sub(1) };
                if prev.and_then(|p| words.get(p)).map_or(false, |p| p.2.eq_ignore_ascii_case("FROM") || p.2.eq_ignore_ascii_case("JOIN")) {
                    seqs.push(key);
                }
            }
        }
        if needed.is_empty() && seqs.is_empty() {
            return Ok(changed.then_some(out));
        }
        // view lồng nhau: phụ thuộc đứng trước
        let mut ordered: Vec<String> = Vec::new();
        fn visit(name: &str, views: &BTreeMap<String, ViewDef>, ordered: &mut Vec<String>, stack: &mut Vec<String>, skip: &dyn Fn(&str) -> bool) -> Result<(), String> {
            if ordered.iter().any(|o| o == name) {
                return Ok(());
            }
            if stack.iter().any(|s| s == name) || stack.len() > 32 {
                return Err(format!("infinite recursion detected in rules for relation \"{}\"", name));
            }
            stack.push(name.to_string());
            let q = &views[name].query;
            let inner = ident_words(q);
            let own = statement_cte_names(q, &inner);
            for (_, _, w) in &inner {
                let k = w.to_lowercase();
                if k != name && views.get(&k).map_or(false, |v| !v.materialized) && !own.contains(&k) && !skip(&k) {
                    visit(&k, views, ordered, stack, skip)?;
                } else if k == name && !own.contains(&k) {
                    return Err(format!("infinite recursion detected in rules for relation \"{}\"", name));
                }
            }
            stack.pop();
            ordered.push(name.to_string());
            Ok(())
        }
        let skip = |k: &str| self.storage.get_table(k).is_some();
        for n in &needed {
            visit(n, &objs.views, &mut ordered, &mut Vec::new(), &skip)?;
        }
        let mut defs: Vec<String> = Vec::new();
        for n in &ordered {
            let v = &objs.views[n];
            let cols = if v.columns.is_empty() { String::new() } else { format!(" ({})", v.columns.join(", ")) };
            defs.push(format!("{}{} AS ({})", n, cols, v.query));
        }
        for n in &seqs {
            let st = &objs.sequences[n];
            defs.push(format!("{} AS (SELECT {} AS last_value, 0 AS log_cnt, {} AS is_called)", n, st.last, if st.called { "true" } else { "false" }));
        }
        let words = ident_words(&out);
        let text = if lead == "WITH" {
            let at = if words.get(1).map_or(false, |w| w.2.eq_ignore_ascii_case("RECURSIVE")) { words[1].1 } else { words[0].1 };
            format!("{} {},{}", &out[..at], defs.join(", "), &out[at..])
        } else {
            format!("WITH {} {}", defs.join(", "), out)
        };
        Ok(Some(text))
    }

    /// Phần kiểu của `expand_objects`: ép kiểu domain / enum, hàm enum, `ORDER BY` / so sánh cột enum.
    /// Đường wire gọi riêng để định dạng kết quả theo câu đã viết lại.
    pub(crate) fn rewrite_type_usage(&self, sql: &str) -> Result<Option<String>, String> {
        let objs = &self.storage.objects;
        let mut out = sql.to_string();
        let mut changed = false;
        if !objs.domains.is_empty() {
            if let Some(s) = rewrite_domain_casts(&out, &objs.domains) {
                out = s;
                changed = true;
            }
        }
        if !objs.enums.is_empty() {
            if let Some(s) = self.rewrite_enum_usage(&out)? {
                out = s;
                changed = true;
            }
        }
        Ok(changed.then_some(out))
    }

    /// `INSERT` / `UPDATE` / `DELETE` nhắm vào một view: view đơn giản (`SELECT cột FROM một_bảng [WHERE đk]`)
    /// được ghi thẳng vào bảng gốc như view tự cập nhật của PostgreSQL (UPDATE / DELETE chỉ đụng dòng thấy
    /// được qua view). `WITH CHECK OPTION`: dòng INSERT ... VALUES phải thoả điều kiện của view.
    /// `None` = câu lệnh không ghi vào view.
    pub(crate) fn view_dml(&mut self, sql: &str, params: &[Value]) -> Option<Result<QueryResult, String>> {
        let words = ident_words(sql);
        let (msg, target) = dml_target(&words)?;
        let key = target.to_lowercase();
        let view = self.storage.objects.views.get(&key).filter(|v| !v.materialized)?.clone();
        if self.storage.get_table(&key).is_some() || statement_cte_names(sql, &words).contains(&key) {
            return None;
        }
        Some(self.run_view_dml(sql, &words, msg, &key, &view, params))
    }

    fn run_view_dml(&mut self, sql: &str, words: &[(usize, usize, String)], msg: &str, key: &str, view: &ViewDef, params: &[Value]) -> Result<QueryResult, String> {
        let not_updatable = || format!("{} \"{}\"", msg, key);
        // view đơn giản: SELECT <cột | *> FROM <bảng> [WHERE <đk>]
        let q = view.query.trim();
        let qu = q.to_uppercase();
        if !qu.starts_with("SELECT ") || ["GROUP BY", "HAVING", "LIMIT", "OFFSET", "UNION", "INTERSECT", "EXCEPT", "JOIN", "DISTINCT", "FETCH", "WINDOW"].iter().any(|kw| find_top_level_keyword(q, kw).is_some()) {
            return Err(not_updatable());
        }
        let from = find_top_level_keyword(q, "FROM").ok_or_else(not_updatable)?;
        let where_pos = find_top_level_keyword(q, "WHERE");
        let from_part = q[from + 4..where_pos.unwrap_or(q.len())].trim();
        let from_words: Vec<&str> = from_part.split_whitespace().collect();
        if from_part.contains(',') || from_part.contains('(') || from_words.is_empty() || from_words.len() > 3 {
            return Err(not_updatable());
        }
        let base = object_key(from_words[0]);
        let alias = match from_words.as_slice() {
            [_, a] => Some(a.to_lowercase()),
            [_, kw, a] if kw.eq_ignore_ascii_case("AS") => Some(a.to_lowercase()),
            [_] => None,
            _ => return Err(not_updatable()),
        };
        let Some(base_cols) = self.storage.get_table(&base).map(|t| t.columns.iter().map(|c| c.name.to_lowercase()).collect::<Vec<_>>()) else {
            return Err(not_updatable());
        };
        // cột của view → cột của bảng
        let strip_alias = |c: &str| -> String {
            let c = c.trim().to_lowercase();
            match (&alias, c.split_once('.')) {
                (Some(a), Some((q, rest))) if q == a || q == base => rest.to_string(),
                (None, Some((q, rest))) if q == base => rest.to_string(),
                _ => c,
            }
        };
        let mut map: Vec<(String, String)> = Vec::new();
        for item in split_comma_separated_tokens(q[6..from].trim()) {
            let item = item.trim();
            if item == "*" {
                map.extend(base_cols.iter().map(|c| (c.clone(), c.clone())));
                continue;
            }
            let parts: Vec<&str> = item.split_whitespace().collect();
            let (src, out) = match parts.as_slice() {
                [c] => (strip_alias(c), strip_alias(c)),
                [c, a] => (strip_alias(c), a.trim_matches('"').to_lowercase()),
                [c, kw, a] if kw.eq_ignore_ascii_case("AS") => (strip_alias(c), a.trim_matches('"').to_lowercase()),
                _ => return Err(not_updatable()),
            };
            if !base_cols.contains(&src) {
                return Err(not_updatable());
            }
            map.push((out, src));
        }
        if !view.columns.is_empty() {
            for (i, c) in view.columns.iter().enumerate() {
                if let Some(m) = map.get_mut(i) {
                    m.0 = c.clone();
                }
            }
        }
        let view_cond = where_pos.map(|w| {
            let c = q[w + 5..].trim().to_string();
            match &alias {
                Some(a) => replace_qualifier(&c, a, &base),
                None => c,
            }
        });
        // đổi tên cột của view → cột bảng, và tên view → tên bảng, trong câu lệnh của người dùng
        let mut edits: Vec<(usize, usize, String)> = Vec::new();
        let (_, target_word_idx) = words.iter().enumerate().skip(1).find(|(_, w)| w.2.eq_ignore_ascii_case(key)).map(|(i, w)| (w.clone(), i)).ok_or_else(not_updatable)?;
        let tw = &words[target_word_idx];
        let target_start = tw.0;
        edits.push((tw.0, tw.1, base.clone()));
        for (i, (s0, e0, w)) in words.iter().enumerate() {
            if i == target_word_idx {
                continue;
            }
            let lw = w.to_lowercase();
            if lw == key && sql.as_bytes().get(*e0) == Some(&b'.') {
                edits.push((*s0, *e0, base.clone()));
                continue;
            }
            if let Some((_, src)) = map.iter().find(|(o, s)| *o == lw && o != s) {
                edits.push((*s0, *e0, src.clone()));
            }
        }
        edits.sort_by_key(|e| std::cmp::Reverse(e.0));
        let mut stmt = sql.to_string();
        for (s0, e0, r) in edits {
            stmt.replace_range(s0..e0, &r);
        }
        let up = stmt.trim_start().to_uppercase();
        if up.starts_with("INSERT") {
            // không liệt kê cột: theo thứ tự cột của view
            // không có sửa đổi nào trước tên đích (INSERT INTO <view>)
            let after_target = target_start + base.len();
            if !stmt[after_target..].trim_start().starts_with('(') {
                let cols: Vec<&str> = map.iter().map(|(_, s)| s.as_str()).collect();
                stmt = format!("{} ({}){}", &stmt[..after_target], cols.join(", "), &stmt[after_target..]);
            }
            if view.check_option {
                if let Some(c) = &view_cond {
                    self.check_view_insert(&stmt, after_target, c, key, params)?;
                }
            }
        } else if let Some(c) = &view_cond {
            // UPDATE / DELETE chỉ đụng dòng thấy được qua view
            let tail_kw = find_top_level_keyword(&stmt, "RETURNING");
            let (body, tail) = match tail_kw {
                Some(p) => (stmt[..p].to_string(), stmt[p..].to_string()),
                None => (stmt.clone(), String::new()),
            };
            stmt = match find_top_level_keyword(&body, "WHERE") {
                Some(w) => format!("{}WHERE ({}) AND ({}) {}", &body[..w], c, body[w + 5..].trim(), tail),
                None => format!("{} WHERE {} {}", body.trim_end(), c, tail),
            };
        }
        self.execute(stmt.trim(), params)
    }

    /// `WITH CHECK OPTION` cho `INSERT INTO bảng (cột) VALUES ...`: mọi dòng phải làm điều kiện của view
    /// đúng (NULL cũng là vi phạm). Điều kiện dùng cột không có trong danh sách thì không kiểm tra được ở đây.
    fn check_view_insert(&mut self, stmt: &str, after_target: usize, cond: &str, view: &str, params: &[Value]) -> Result<(), String> {
        let rest = stmt[after_target..].trim_start();
        let Some(close) = crate::engine::constraints::matching_paren(rest, 0) else { return Ok(()) };
        let cols: Vec<String> = split_comma_separated_tokens(&rest[1..close]).into_iter().map(|c| c.trim().trim_matches('"').to_lowercase()).collect();
        let values = rest[close + 1..].trim_start();
        if !values.get(..6).map_or(false, |v| v.eq_ignore_ascii_case("VALUES")) {
            return Ok(());
        }
        let vend = ["RETURNING", "ON CONFLICT"].iter().filter_map(|kw| find_top_level_keyword(values, kw)).min().unwrap_or(values.len());
        let referenced: Vec<String> = ident_words(cond).into_iter().map(|w| w.2.to_lowercase()).collect();
        let base_cols: Vec<String> = self.storage.tables.values().flat_map(|t| t.columns.iter().map(|c| c.name.to_lowercase())).collect();
        if referenced.iter().any(|w| base_cols.contains(w) && !cols.contains(w)) {
            return Ok(());
        }
        let probe = format!("SELECT count(*) AS c FROM ({}) AS __pglite_check({}) WHERE ({}) IS NOT TRUE", values[..vend].trim(), cols.join(", "), cond);
        let r = self.execute(&probe, params)?;
        let bad = r.rows.first().and_then(|row| row.get("c")).and_then(|v| v.as_i64()).unwrap_or(0);
        if bad > 0 {
            return Err(format!("new row violates check option for view \"{}\"", view));
        }
        Ok(())
    }

    /// `'x'::mood` / `CAST('x' AS mood)`: kiểm tra nhãn rồi coi như text; `ORDER BY cột_enum` → theo thứ tự nhãn.
    fn rewrite_enum_usage(&self, sql: &str) -> Result<Option<String>, String> {
        let enums = &self.storage.objects.enums;
        // enum_range / enum_first / enum_last(NULL::mood): hằng số
        if let Some(s) = rewrite_enum_functions(sql, enums)? {
            return Ok(Some(self.rewrite_enum_usage(&s)?.unwrap_or(s)));
        }
        let words = ident_words(sql);
        let b = sql.as_bytes();
        let mut edits: Vec<(usize, usize, String)> = Vec::new();
        for (k, (start, end, w)) in words.iter().enumerate() {
            let key = w.to_lowercase();
            let Some(labels) = enums.get(&key) else { continue };
            let before = sql[..*start].trim_end();
            let is_cast = before.ends_with("::") || (k > 0 && words[k - 1].2.eq_ignore_ascii_case("AS") && before.len() >= 2 && {
                // AS bên trong CAST(...)
                let mut depth = 0i32;
                let mut found = false;
                for j in (0..words[k - 1].0).rev() {
                    match b[j] {
                        b')' => depth += 1,
                        b'(' if depth == 0 => {
                            found = sql[..j].trim_end().to_uppercase().ends_with("CAST");
                            break;
                        }
                        b'(' => depth -= 1,
                        _ => {}
                    }
                }
                found
            });
            if !is_cast {
                continue;
            }
            let mut alias = false;
            // cả mục SELECT không alias: PostgreSQL đặt tên cột là tên kiểu
            let select_item_start = |pos: usize| {
                let p = sql[..pos].trim_end().to_uppercase();
                p.ends_with(',') || p.ends_with("SELECT") || p.ends_with("DISTINCT")
            };
            let select_item_end = |pos: usize| {
                let n = sql[pos..].trim_start();
                let nw = n.split(|c: char| !c.is_ascii_alphanumeric()).next().unwrap_or("").to_uppercase();
                n.is_empty() || n.starts_with([',', ';']) || nw == "FROM"
            };
            if !before.ends_with("::") {
                // CAST(x AS mood)
                let close = sql[*end..].trim_start();
                if close.starts_with(')') {
                    let close_pos = sql.len() - close.len();
                    let open = sql[..words[k - 1].0].rfind('(').unwrap_or(0);
                    let cast_kw = sql[..open].trim_end();
                    let cast_start = cast_kw.len().saturating_sub(4);
                    if select_item_start(cast_start) && select_item_end(close_pos + 1) {
                        edits.push((close_pos + 1, close_pos + 1, format!(" AS {}", key)));
                    }
                }
            }
            if before.ends_with("::") {
                let operand_end = before.len() - 2;
                let operand = sql[..operand_end].trim_end();
                let operand_start = if operand.ends_with('\'') {
                    operand.as_bytes()[..operand.len() - 1].iter().rposition(|&c| c == b'\'').unwrap_or(0)
                } else {
                    words[..k].iter().rev().find(|w| w.1 <= operand.len()).map_or(0, |w| {
                        // bảng.cột
                        let q = words.iter().rev().find(|x| x.1 + 1 == w.0 && b[w.0 - 1] == b'.');
                        q.map_or(w.0, |x| x.0)
                    })
                };
                alias = select_item_start(operand_start) && select_item_end(*end);
            }
            if before.ends_with("::") {
                let lit_end = before.len() - 2;
                let lit = sql[..lit_end].trim_end();
                if lit.ends_with('\'') {
                    // tìm đầu của chuỗi '...'
                    let bytes = lit.as_bytes();
                    let mut j = bytes.len() - 1;
                    let mut open = None;
                    while j > 0 {
                        j -= 1;
                        if bytes[j] == b'\'' {
                            if j > 0 && bytes[j - 1] == b'\'' {
                                j -= 1;
                                continue;
                            }
                            open = Some(j);
                            break;
                        }
                    }
                    if let Some(o) = open {
                        if let Some(label) = string_literal(&lit[o..]) {
                            if !labels.contains(&label) {
                                return Err(format!("invalid input value for enum {}: \"{}\"", key, label));
                            }
                        }
                    }
                }
            }
            edits.push((*start, *end, if alias { format!("text AS {}", key) } else { "text".to_string() }));
        }
        // ORDER BY <cột enum> (không áp cho phép tập hợp: ORDER BY ở đó theo cột kết quả)
        let has_setop = words.iter().any(|w| ["UNION", "INTERSECT", "EXCEPT"].contains(&w.2.to_uppercase().as_str()));
        if !has_setop {
            let enum_cols = self.enum_columns_in(&words);
            if !enum_cols.is_empty() {
                for k in 0..words.len().saturating_sub(1) {
                    if !(words[k].2.eq_ignore_ascii_case("ORDER") && words[k + 1].2.eq_ignore_ascii_case("BY")) {
                        continue;
                    }
                    // mỗi mục: tên cột (có thể kèm bảng) đứng ngay sau ORDER BY hoặc dấu phẩy
                    let mut j = k + 2;
                    while j < words.len() {
                        let start = words[j].0;
                        let qualified = b.get(words[j].1) == Some(&b'.') && words.get(j + 1).map_or(false, |n| n.0 == words[j].1 + 1);
                        let (col, end) = if qualified { (words[j + 1].2.to_lowercase(), words[j + 1].1) } else { (words[j].2.to_lowercase(), words[j].1) };
                        let before = sql[..start].trim_end();
                        if !(before.ends_with(',') || j == k + 2) {
                            break;
                        }
                        let next = sql[end..].trim_start();
                        let next_word = next.split(|c: char| !c.is_ascii_alphanumeric()).next().unwrap_or("").to_uppercase();
                        let whole_item = next.is_empty()
                            || next.starts_with([',', ')', ';'])
                            || ["ASC", "DESC", "NULLS", "LIMIT", "OFFSET", "FETCH", "FOR", "ROWS", "RANGE", "GROUPS", "RETURNING"].contains(&next_word.as_str());
                        if whole_item {
                            if let Some(labels) = enum_cols.get(&col) {
                                edits.push((start, end, enum_rank_expr(&sql[start..end], labels)));
                            }
                        }
                        // sang mục kế tiếp: dấu phẩy ở cùng cấp ngoặc, trước khi gặp ')' đóng mệnh đề
                        let mut depth = 0i32;
                        let mut p = end;
                        let mut comma = None;
                        while p < b.len() {
                            match b[p] {
                                b'(' => depth += 1,
                                b')' if depth == 0 => break,
                                b')' => depth -= 1,
                                b',' if depth == 0 => {
                                    comma = Some(p);
                                    break;
                                }
                                b'\'' => {
                                    p += 1;
                                    while p < b.len() && b[p] != b'\'' {
                                        p += 1;
                                    }
                                }
                                _ => {}
                            }
                            p += 1;
                        }
                        let Some(comma) = comma else { break };
                        if ["LIMIT", "OFFSET", "FETCH", "FOR"].iter().any(|kw| find_top_level_keyword(&sql[end..comma], kw).is_some()) {
                            break;
                        }
                        match words.iter().position(|w| w.0 > comma) {
                            Some(n) => j = n,
                            None => break,
                        }
                    }
                }
            }
        }
        // cột_enum < > <= >= 'nhãn': so theo thứ tự khai báo
        let enum_cols = self.enum_columns_in(&words);
        if !enum_cols.is_empty() {
            for (j, (start, end, w)) in words.iter().enumerate() {
                let Some(labels) = enum_cols.get(&w.to_lowercase()) else { continue };
                // bỏ qua phần `bảng.` phía trước nếu có
                let col_start = if *start > 0 && b[start - 1] == b'.' && j > 0 { words[j - 1].0 } else { *start };
                if b.get(*end) == Some(&b'.') {
                    continue;
                }
                let rest = &sql[*end..];
                let trimmed = rest.trim_start();
                let op = ["<=", ">=", "<", ">"].into_iter().find(|o| trimmed.starts_with(o) && !trimmed.starts_with("<>") && !trimmed.starts_with("<@") && !trimmed.starts_with(">>") && !trimmed.starts_with("<<"));
                let Some(op) = op else { continue };
                let after_op = trimmed[op.len()..].trim_start();
                if !after_op.starts_with('\'') {
                    continue;
                }
                let ab = after_op.as_bytes();
                let mut q = 1;
                while q < ab.len() {
                    if ab[q] == b'\'' {
                        if ab.get(q + 1) == Some(&b'\'') {
                            q += 2;
                            continue;
                        }
                        break;
                    }
                    q += 1;
                }
                let lit = &after_op[..(q + 1).min(after_op.len())];
                let Some(label) = string_literal(lit) else { continue };
                let rank = labels.iter().position(|l| *l == label).ok_or_else(|| format!("invalid input value for enum {}: \"{}\"", self.enum_type_of_column(&w.to_lowercase()).unwrap_or_default(), label))?;
                let lit_end = sql.len() - after_op.len() + lit.len();
                edits.push((col_start, lit_end, format!("{} {} {}", enum_rank_expr(&sql[col_start..*end], labels), op, rank + 1)));
            }
        }
        if edits.is_empty() {
            return Ok(None);
        }
        edits.sort_by_key(|e| std::cmp::Reverse(e.0));
        let mut out = sql.to_string();
        let mut last_start = usize::MAX;
        for (s, e, r) in edits {
            if e > last_start {
                continue; // chồng lấn
            }
            out.replace_range(s..e, &r);
            last_start = s;
        }
        Ok(Some(out))
    }

    /// Tên kiểu enum của một cột (bảng bất kỳ), cho thông báo lỗi.
    fn enum_type_of_column(&self, col: &str) -> Option<String> {
        self.storage.tables.values().find_map(|t| t.decl_types.get(col).filter(|d| self.storage.objects.enums.contains_key(&d.base)).map(|d| d.base.clone()))
    }

    /// Cột kiểu enum của các bảng được nhắc tới trong câu lệnh: tên cột → nhãn. Tên trùng ở hai bảng
    /// với hai enum khác nhau thì bỏ (không biết chọn bên nào).
    fn enum_columns_in(&self, words: &[(usize, usize, String)]) -> BTreeMap<String, Vec<String>> {
        let enums = &self.storage.objects.enums;
        let mut out: BTreeMap<String, Vec<String>> = BTreeMap::new();
        let mut conflict: Vec<String> = Vec::new();
        let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();
        for (_, _, w) in words {
            let key = w.to_lowercase();
            if !seen.insert(key.clone()) {
                continue;
            }
            let Some(t) = self.storage.get_table(&key) else { continue };
            for c in &t.columns {
                let Some(d) = t.decl_types.get(&c.name.to_lowercase()) else { continue };
                if d.array {
                    continue;
                }
                if let Some(labels) = enums.get(&d.base) {
                    let cn = c.name.to_lowercase();
                    match out.get(&cn) {
                        Some(l) if l != labels => conflict.push(cn),
                        _ => {
                            out.insert(cn, labels.clone());
                        }
                    }
                }
            }
        }
        for c in conflict {
            out.remove(&c);
        }
        out
    }

    /// CREATE / ALTER / DROP của view, schema, sequence, type; `DROP TABLE` / `CREATE TABLE` đụng tới
    /// view. `None` = không phải câu thuộc nhóm này.
    pub(crate) fn handle_object_ddl(&mut self, sql: &str, upper: &str, params: &[Value]) -> Option<Result<QueryResult, String>> {
        let first = upper.split_whitespace().next().unwrap_or("");
        if !matches!(first, "CREATE" | "ALTER" | "DROP" | "REFRESH") {
            return None;
        }
        let words: Vec<&str> = upper.split_whitespace().take(6).collect();
        let w = |i: usize| words.get(i).copied().unwrap_or("");
        let res = match first {
            "CREATE" => {
                let mut i = 1;
                let or_replace = w(1) == "OR" && w(2) == "REPLACE";
                if or_replace {
                    i = 3;
                }
                let temp = matches!(w(i), "TEMP" | "TEMPORARY");
                if temp {
                    i += 1;
                }
                match w(i) {
                    "VIEW" | "RECURSIVE" => self.create_view(sql, or_replace, temp, false, params),
                    "MATERIALIZED" if w(i + 1) == "VIEW" => self.create_view(sql, or_replace, temp, true, params),
                    "SEQUENCE" if !or_replace => self.create_sequence(sql),
                    "SCHEMA" if !or_replace && !temp => self.create_schema(sql),
                    "TYPE" if !or_replace && !temp => self.create_type(sql),
                    "DOMAIN" if !or_replace && !temp => self.create_domain(sql),
                    "TABLE" | "UNLOGGED" if !or_replace => {
                        if let Some(e) = self.check_create_table_name(sql) {
                            return Some(Err(e));
                        }
                        let rewritten = self.rewrite_domain_create_table(sql)?;
                        return Some(self.execute_statement(&rewritten, params));
                    }
                    _ => return None,
                }
            }
            "ALTER" => match w(1) {
                "SEQUENCE" => self.alter_sequence(sql),
                "TYPE" => self.alter_type(sql),
                "VIEW" => self.alter_view(sql),
                "MATERIALIZED" if w(2) == "VIEW" => Ok(done("ALTER MATERIALIZED VIEW")),
                "SCHEMA" => self.alter_schema(sql),
                "DOMAIN" => Err("ALTER DOMAIN is not supported".to_string()),
                "TABLE" if !self.storage.objects.domains.is_empty() => {
                    let rewritten = self.rewrite_domain_alter_table(sql)?;
                    return Some(self.execute_statement(&rewritten, params));
                }
                _ => return None,
            },
            "DROP" => match w(1) {
                "VIEW" => self.drop_views(after_words(sql, 2), false),
                "MATERIALIZED" if w(2) == "VIEW" => self.drop_views(after_words(sql, 3), true),
                "SEQUENCE" => self.drop_sequences(after_words(sql, 2)),
                "SCHEMA" => self.drop_schemas(after_words(sql, 2)),
                "TYPE" => self.drop_types(after_words(sql, 2)),
                "DOMAIN" => self.drop_domains(after_words(sql, 2)),
                "TABLE" => return self.drop_table_view_deps(after_words(sql, 2)).map(Err),
                _ => return None,
            },
            _ => {
                if w(1) == "MATERIALIZED" && w(2) == "VIEW" {
                    self.refresh_materialized_view(sql, params)
                } else {
                    return None;
                }
            }
        };
        if res.is_ok() {
            self.plan_cache.clear();
        }
        Some(res)
    }

    /// Tên đã được dùng bởi bảng / view / sequence (cùng không gian tên quan hệ như PostgreSQL).
    fn relation_exists(&self, key: &str) -> bool {
        self.storage.get_table(key).is_some() || self.storage.objects.views.contains_key(key) || self.storage.objects.sequences.contains_key(key)
    }

    /// `CREATE TABLE v (...)` khi `v` là view / sequence: lỗi như PostgreSQL. `None` = để CREATE TABLE chạy tiếp.
    fn check_create_table_name(&self, sql: &str) -> Option<String> {
        let up = sql.to_uppercase();
        let p = up.find("TABLE")? + 5;
        let mut rest = sql[p..].trim_start();
        let ine = strip_kw(rest, "IF NOT EXISTS").is_some() || rest.to_uppercase().starts_with("IF NOT EXISTS");
        if let Some(r) = rest.get(..13).filter(|x| x.eq_ignore_ascii_case("IF NOT EXISTS")) {
            let _ = r;
            rest = rest[13..].trim_start();
        }
        let end = rest.find(|c: char| c.is_whitespace() || c == '(').unwrap_or(rest.len());
        let key = object_key(&rest[..end]);
        let o = &self.storage.objects;
        if (o.views.get(&key).map_or(false, |v| !v.materialized) || o.sequences.contains_key(&key)) && !ine {
            return Some(format!("relation \"{}\" already exists", key));
        }
        None
    }

    // ---------------------------------------------------------------- VIEW

    fn create_view(&mut self, sql: &str, or_replace: bool, temp: bool, materialized: bool, params: &[Value]) -> Result<QueryResult, String> {
        let up = sql.to_uppercase();
        let recursive = find_top_level_keyword(sql, "AS").map_or(false, |p| up[..p].split_whitespace().any(|w| w == "RECURSIVE"));
        let view_kw = up.find("VIEW").ok_or("syntax error at or near \"VIEW\"")? + 4;
        let as_pos = find_top_level_keyword(&sql[view_kw..], "AS").map(|p| view_kw + p).ok_or("syntax error: CREATE VIEW requires AS")?;
        let mut head = sql[view_kw..as_pos].trim();
        let mut if_not_exists = false;
        if head.len() > 14 && head[..14].eq_ignore_ascii_case("IF NOT EXISTS ") {
            if_not_exists = true;
            head = head[14..].trim();
        }
        // WITH (options) trước AS: bỏ qua
        if let Some(p) = find_top_level_keyword(head, "WITH") {
            head = head[..p].trim();
        }
        let (name_raw, cols_raw) = match head.find('(') {
            Some(p) => (&head[..p], Some(head[p + 1..head.rfind(')').unwrap_or(head.len())].trim())),
            None => (head, None),
        };
        let name = object_key(name_raw);
        if name.is_empty() {
            return Err("syntax error at or near \"AS\"".to_string());
        }
        let columns: Vec<String> = cols_raw.map(|c| split_comma_separated_tokens(c).into_iter().map(object_key).collect()).unwrap_or_default();
        let mut query = sql[as_pos + 2..].trim().trim_end_matches(';').trim().to_string();
        // WITH [CASCADED | LOCAL] CHECK OPTION: view không cập nhật được nên không có gì để kiểm tra
        let qup = query.to_uppercase();
        let mut check_option = false;
        for tail in ["WITH CASCADED CHECK OPTION", "WITH LOCAL CHECK OPTION", "WITH CHECK OPTION", "WITH NO DATA", "WITH DATA"] {
            if qup.trim_end().ends_with(tail) {
                query = query[..query.len() - tail.len()].trim().to_string();
                check_option = tail.ends_with("CHECK OPTION");
                break;
            }
        }
        // CREATE RECURSIVE VIEW v (cột) AS q  ≡  CREATE VIEW v AS WITH RECURSIVE v (cột) AS (q) SELECT cột FROM v
        if recursive {
            if columns.is_empty() {
                return Err("syntax error: CREATE RECURSIVE VIEW requires a column list".to_string());
            }
            query = format!("WITH RECURSIVE {n} ({c}) AS ({q}) SELECT {c} FROM {n}", n = name, c = columns.join(", "), q = query);
        }
        let no_data = materialized && qup.trim_end().ends_with("WITH NO DATA");
        let exists_view = self.storage.objects.views.get(&name).cloned();
        if let Some(v) = &exists_view {
            if v.materialized != materialized || !or_replace {
                if if_not_exists {
                    return Ok(done(if materialized { "CREATE MATERIALIZED VIEW" } else { "CREATE VIEW" }));
                }
                return Err(format!("relation \"{}\" already exists", name));
            }
        } else if self.relation_exists(&name) {
            if if_not_exists {
                return Ok(done("CREATE MATERIALIZED VIEW"));
            }
            return Err(format!("relation \"{}\" already exists", name));
        }
        // kiểm tra câu truy vấn (bảng / cột tồn tại) và số cột của danh sách tên
        let probe = self.execute(&query, params)?;
        if !columns.is_empty() && !probe.fields.is_empty() && columns.len() > probe.fields.len() {
            return Err("CREATE VIEW specifies more column names than columns".to_string());
        }
        let def = ViewDef { query: query.clone(), columns: columns.clone(), materialized, check_option };
        if materialized {
            let col_list = if columns.is_empty() { String::new() } else { format!(" ({})", columns.join(", ")) };
            let ctas = format!("CREATE TABLE {}{} AS {}{}", name, col_list, query, if no_data { " WITH NO DATA" } else { "" });
            self.execute(&ctas, params)?;
            self.storage.log_schema_op(SchemaOp::View { name, def: Some(def) });
            return Ok(done("CREATE MATERIALIZED VIEW"));
        }
        let op = SchemaOp::View { name, def: Some(def) };
        if temp {
            self.storage.apply_object_op_unlogged(op);
        } else {
            self.storage.log_schema_op(op);
        }
        Ok(done("CREATE VIEW"))
    }

    /// View (thường) dùng tới quan hệ `key` (bảng / view / sequence).
    fn views_depending_on(&self, key: &str) -> Vec<String> {
        self.storage
            .objects
            .views
            .iter()
            .filter(|(n, v)| n.as_str() != key && ident_words(&v.query).iter().any(|(_, _, w)| w.eq_ignore_ascii_case(key)))
            .map(|(n, _)| n.clone())
            .collect()
    }

    /// Các view phụ thuộc (bắc cầu) vào một trong `keys`, chưa nằm trong `keys`.
    fn dependent_views(&self, keys: &[String]) -> Vec<String> {
        let mut out: Vec<String> = Vec::new();
        let mut frontier: Vec<String> = keys.to_vec();
        while let Some(k) = frontier.pop() {
            for d in self.views_depending_on(&k) {
                if !keys.contains(&d) && !out.contains(&d) {
                    out.push(d.clone());
                    frontier.push(d);
                }
            }
        }
        out
    }

    fn drop_view_objects(&mut self, names: &[String]) -> Result<(), String> {
        for n in names {
            let mat = self.storage.objects.views.get(n).map_or(false, |v| v.materialized);
            if mat && self.storage.get_table(n).is_some() {
                self.storage.drop_table(n);
            }
            if self.storage.wal.temp_tables.contains(n) || !self.view_is_logged(n) {
                self.storage.apply_object_op_unlogged(SchemaOp::View { name: n.clone(), def: None });
            } else {
                self.storage.log_schema_op(SchemaOp::View { name: n.clone(), def: None });
            }
        }
        Ok(())
    }

    /// View tạm không có trong WAL. Không lưu cờ riêng: view tạm là view không có trong DB khi mở lại,
    /// nên ghi `DROP` của nó vào WAL cũng vô hại — luôn ghi cho đơn giản.
    fn view_is_logged(&self, _name: &str) -> bool {
        true
    }

    fn drop_views(&mut self, rest: &str, materialized: bool) -> Result<QueryResult, String> {
        let (names, if_exists, cascade) = parse_drop_list(rest);
        let tag = if materialized { "DROP MATERIALIZED VIEW" } else { "DROP VIEW" };
        let mut targets = Vec::new();
        for n in names {
            match self.storage.objects.views.get(&n) {
                Some(v) if v.materialized == materialized => targets.push(n),
                Some(_) => return Err(format!("\"{}\" is not a{} view", n, if materialized { " materialized" } else { "" })),
                None if self.storage.get_table(&n).is_some() => return Err(format!("\"{}\" is not a{} view", n, if materialized { " materialized" } else { "" })),
                None if if_exists => {}
                None => return Err(format!("{} \"{}\" does not exist", if materialized { "materialized view" } else { "view" }, n)),
            }
        }
        let deps = self.dependent_views(&targets);
        if !deps.is_empty() {
            if !cascade {
                return Err(format!("cannot drop view {} because other objects depend on it (view {} depends on view {})", targets[0], deps[0], targets[0]));
            }
            self.drop_view_objects(&deps)?;
        }
        self.drop_view_objects(&targets)?;
        Ok(done(tag))
    }

    /// `DROP TABLE`: tên là view → lỗi; view dùng bảng → lỗi trừ khi CASCADE (khi đó xoá view). `None` =
    /// để `handle_drop_table` chạy tiếp.
    fn drop_table_view_deps(&mut self, rest: &str) -> Option<String> {
        if self.storage.objects.views.is_empty() {
            return None;
        }
        let (names, _, cascade) = parse_drop_list(rest);
        for n in &names {
            if self.storage.objects.views.get(n).map_or(false, |v| !v.materialized) {
                return Some(format!("\"{}\" is not a table", n));
            }
        }
        let names: Vec<String> = names.into_iter().filter(|n| self.storage.get_table(n).is_some()).collect();
        let deps = self.dependent_views(&names);
        if deps.is_empty() {
            return None;
        }
        if !cascade {
            return Some(format!("cannot drop table {} because other objects depend on it (view {} depends on table {})", names[0], deps[0], names[0]));
        }
        self.drop_view_objects(&deps).err()
    }

    fn alter_view(&mut self, sql: &str) -> Result<QueryResult, String> {
        // ALTER VIEW [IF EXISTS] v RENAME TO w | OWNER TO r | SET SCHEMA s | ALTER COLUMN ...
        let mut rest = after_words(sql, 2);
        let if_exists = strip_kw(rest, "IF EXISTS").is_some();
        if let Some(r) = strip_kw(rest, "IF EXISTS") {
            rest = r;
        }
        let name_end = rest.find(char::is_whitespace).unwrap_or(rest.len());
        let name = object_key(&rest[..name_end]);
        let tail = rest[name_end..].trim();
        let Some(def) = self.storage.objects.views.get(&name).cloned() else {
            if if_exists {
                return Ok(done("ALTER VIEW"));
            }
            return Err(format!("relation \"{}\" does not exist", name));
        };
        if let Some(r) = strip_kw(tail, "RENAME").and_then(|r| strip_kw(r, "TO")) {
            let new = object_key(r);
            if self.relation_exists(&new) {
                return Err(format!("relation \"{}\" already exists", new));
            }
            self.storage.log_schema_op(SchemaOp::View { name: name.clone(), def: None });
            self.storage.log_schema_op(SchemaOp::View { name: new, def: Some(def) });
        }
        Ok(done("ALTER VIEW"))
    }

    fn refresh_materialized_view(&mut self, sql: &str, params: &[Value]) -> Result<QueryResult, String> {
        let up = sql.to_uppercase();
        let mut rest = sql[up.find("VIEW").unwrap_or(0) + 4..].trim();
        if let Some(r) = strip_kw(rest, "CONCURRENTLY") {
            rest = r;
        }
        let mut with_data = true;
        let ru = rest.to_uppercase();
        if ru.trim_end().ends_with("WITH NO DATA") {
            with_data = false;
            rest = rest[..ru.rfind("WITH NO DATA").unwrap_or(rest.len())].trim();
        } else if ru.trim_end().ends_with("WITH DATA") {
            rest = rest[..ru.rfind("WITH DATA").unwrap_or(rest.len())].trim();
        }
        let name = object_key(rest);
        let def = match self.storage.objects.views.get(&name) {
            Some(v) if v.materialized => v.clone(),
            Some(_) => return Err(format!("\"{}\" is not a materialized view", name)),
            None => return Err(format!("relation \"{}\" does not exist", name)),
        };
        self.execute(&format!("DELETE FROM {}", name), params)?;
        if with_data {
            let cols = self.storage.get_table(&name).map(|t| t.columns.iter().map(|c| c.name.clone()).collect::<Vec<_>>().join(", ")).unwrap_or_default();
            self.execute(&format!("INSERT INTO {} ({}) SELECT * FROM ({}) AS __pglite_refresh", name, cols, def.query), params)?;
        }
        Ok(done("REFRESH MATERIALIZED VIEW"))
    }

    // ---------------------------------------------------------------- SCHEMA

    fn create_schema(&mut self, sql: &str) -> Result<QueryResult, String> {
        let mut rest = after_words(sql, 2).trim_end_matches(';').trim();
        let if_not_exists = strip_kw(rest, "IF NOT EXISTS").is_some();
        if let Some(r) = strip_kw(rest, "IF NOT EXISTS") {
            rest = r;
        }
        let toks: Vec<&str> = rest.split_whitespace().collect();
        // CREATE SCHEMA AUTHORIZATION r → schema tên r; CREATE SCHEMA s AUTHORIZATION r
        let name = if toks.first().map_or(false, |t| t.eq_ignore_ascii_case("AUTHORIZATION")) {
            toks.get(1).map(|t| object_key(t)).unwrap_or_default()
        } else {
            if toks.len() > 1 && !toks[1].eq_ignore_ascii_case("AUTHORIZATION") {
                return Err("CREATE SCHEMA with schema elements is not supported".to_string());
            }
            toks.first().map(|t| object_key(t)).unwrap_or_default()
        };
        if name.is_empty() {
            return Err("syntax error at end of input".to_string());
        }
        let o = &self.storage.objects;
        let exists = o.schemas.contains(&name) || (name == "public" && !o.public_dropped) || ["pg_catalog", "information_schema", "pg_toast"].contains(&name.as_str());
        if exists {
            if if_not_exists {
                return Ok(done("CREATE SCHEMA"));
            }
            return Err(format!("schema \"{}\" already exists", name));
        }
        self.storage.log_schema_op(SchemaOp::Schema { name, create: true });
        Ok(done("CREATE SCHEMA"))
    }

    fn alter_schema(&mut self, sql: &str) -> Result<QueryResult, String> {
        let rest = after_words(sql, 2);
        let name_end = rest.find(char::is_whitespace).unwrap_or(rest.len());
        let name = object_key(&rest[..name_end]);
        if !self.storage.objects.schemas.contains(&name) && name != "public" {
            return Err(format!("schema \"{}\" does not exist", name));
        }
        if let Some(r) = strip_kw(rest[name_end..].trim(), "RENAME").and_then(|r| strip_kw(r, "TO")) {
            let new = object_key(r);
            if name == "public" {
                return Err("renaming schema public is not supported".to_string());
            }
            if self.storage.objects.schemas.contains(&new) || new == "public" {
                return Err(format!("schema \"{}\" already exists", new));
            }
            self.storage.log_schema_op(SchemaOp::Schema { name, create: false });
            self.storage.log_schema_op(SchemaOp::Schema { name: new, create: true });
        }
        Ok(done("ALTER SCHEMA"))
    }

    fn drop_schemas(&mut self, rest: &str) -> Result<QueryResult, String> {
        let (names, if_exists, cascade) = parse_drop_list(rest);
        for n in &names {
            let exists = self.storage.objects.schemas.contains(n) || (n == "public" && !self.storage.objects.public_dropped);
            if !exists && !if_exists {
                return Err(format!("schema \"{}\" does not exist", n));
            }
            if ["pg_catalog", "information_schema"].contains(&n.as_str()) {
                return Err(format!("cannot drop schema {} because it is required by the database system", n));
            }
        }
        for n in names {
            if n == "public" {
                if self.storage.objects.public_dropped {
                    continue;
                }
                // `DROP SCHEMA public CASCADE` (hay dùng để làm sạch DB): xoá mọi bảng, view, sequence, enum
                let tables: Vec<String> = self.storage.tables.keys().filter(|k| !self.storage.wal.temp_tables.contains(*k) && !k.starts_with("__")).cloned().collect();
                let o = &self.storage.objects;
                let has_objects = !tables.is_empty() || !o.views.is_empty() || !o.sequences.is_empty() || !o.enums.is_empty();
                if has_objects && !cascade {
                    return Err("cannot drop schema public because other objects depend on it".to_string());
                }
                let views: Vec<String> = o.views.keys().cloned().collect();
                let seqs: Vec<String> = o.sequences.keys().cloned().collect();
                let enums: Vec<String> = o.enums.keys().cloned().collect();
                self.drop_view_objects(&views)?;
                for t in tables {
                    if self.storage.get_table(&t).is_some() {
                        self.storage.drop_table(&t);
                    }
                }
                for s in seqs {
                    self.storage.log_schema_op(SchemaOp::Sequence { name: s, spec: None });
                }
                for e in enums {
                    self.storage.log_schema_op(SchemaOp::EnumType { name: e, labels: None });
                }
                self.custom_constraints.clear();
            }
            if self.storage.objects.schemas.contains(&n) || n == "public" {
                self.storage.log_schema_op(SchemaOp::Schema { name: n, create: false });
            }
        }
        Ok(done("DROP SCHEMA"))
    }

    // ---------------------------------------------------------------- SEQUENCE

    fn create_sequence(&mut self, sql: &str) -> Result<QueryResult, String> {
        let up = sql.to_uppercase();
        let mut rest = sql[up.find("SEQUENCE").unwrap_or(0) + 8..].trim().trim_end_matches(';').trim();
        let if_not_exists = strip_kw(rest, "IF NOT EXISTS").is_some();
        if let Some(r) = strip_kw(rest, "IF NOT EXISTS") {
            rest = r;
        }
        let name_end = rest.find(char::is_whitespace).unwrap_or(rest.len());
        let name = object_key(&rest[..name_end]);
        if name.is_empty() {
            return Err("syntax error at end of input".to_string());
        }
        let opts = parse_seq_options(&rest[name_end..])?;
        if self.relation_exists(&name) || self.find_owned_sequence_key(&name) {
            if if_not_exists {
                return Ok(done("CREATE SEQUENCE"));
            }
            return Err(format!("relation \"{}\" already exists", name));
        }
        let spec = resolve_seq_spec(&opts, None)?;
        self.storage.log_schema_op(SchemaOp::Sequence { name, spec: Some(spec) });
        Ok(done("CREATE SEQUENCE"))
    }

    fn alter_sequence(&mut self, sql: &str) -> Result<QueryResult, String> {
        let mut rest = after_words(sql, 2).trim_end_matches(';').trim();
        let if_exists = strip_kw(rest, "IF EXISTS").is_some();
        if let Some(r) = strip_kw(rest, "IF EXISTS") {
            rest = r;
        }
        let name_end = rest.find(char::is_whitespace).unwrap_or(rest.len());
        let name = object_key(&rest[..name_end]);
        let opts = parse_seq_options(&rest[name_end..])?;
        let Some(st) = self.storage.objects.sequences.get(&name).cloned() else {
            // sequence gắn với cột (SERIAL / IDENTITY / DEFAULT nextval): RESTART = setval(..., false)
            if self.find_owned_sequence_key(&name) {
                if let Some(r) = opts.restart {
                    let v = r.unwrap_or(1);
                    self.execute(&format!("SELECT setval('{}', {}, false)", name, v), &[])?;
                }
                return Ok(done("ALTER SEQUENCE"));
            }
            if if_exists {
                return Ok(done("ALTER SEQUENCE"));
            }
            return Err(format!("relation \"{}\" does not exist", name));
        };
        if let Some(new) = &opts.rename {
            if self.relation_exists(new) {
                return Err(format!("relation \"{}\" already exists", new));
            }
            self.storage.log_schema_op(SchemaOp::Sequence { name: name.clone(), spec: None });
            self.storage.log_schema_op(SchemaOp::Sequence { name: new.clone(), spec: Some(st.spec) });
            self.storage.log_sequence_value(new, st.last, st.called);
            return Ok(done("ALTER SEQUENCE"));
        }
        let spec = resolve_seq_spec(&opts, Some(&st.spec))?;
        if spec != st.spec {
            self.storage.log_schema_op(SchemaOp::Sequence { name: name.clone(), spec: Some(spec) });
        }
        if let Some(r) = opts.restart {
            let v = r.unwrap_or(spec.start);
            if v < spec.min {
                return Err(format!("RESTART value ({}) cannot be less than MINVALUE ({})", v, spec.min));
            }
            if v > spec.max {
                return Err(format!("RESTART value ({}) cannot be greater than MAXVALUE ({})", v, spec.max));
            }
            self.storage.log_sequence_value(&name, v, false);
        }
        Ok(done("ALTER SEQUENCE"))
    }

    fn drop_sequences(&mut self, rest: &str) -> Result<QueryResult, String> {
        let (names, if_exists, cascade) = parse_drop_list(rest);
        let mut targets = Vec::new();
        for n in names {
            if self.storage.objects.sequences.contains_key(&n) {
                targets.push(n);
            } else if self.find_owned_sequence_key(&n) {
                return Err(format!("cannot drop sequence {} because other objects depend on it (column default depends on sequence {})", n, n));
            } else if !if_exists {
                return Err(format!("sequence \"{}\" does not exist", n));
            }
        }
        for n in &targets {
            // cột có DEFAULT nextval('n'): lỗi, hoặc bỏ DEFAULT khi CASCADE
            let users: Vec<(String, String)> = self
                .storage
                .tables
                .iter()
                .flat_map(|(tk, t)| {
                    t.columns
                        .iter()
                        .filter(|c| c.default_value.as_deref().map_or(false, |d| crate::engine::sequences::is_nextval_default(d) && crate::engine::sequences::default_seq_name(d).as_deref() == Some(n.as_str())))
                        .map(move |c| (tk.clone(), c.name.clone()))
                })
                .collect();
            if !users.is_empty() {
                if !cascade {
                    return Err(format!("cannot drop sequence {} because other objects depend on it (default value for column {} of table {} depends on sequence {})", n, users[0].1, users[0].0, n));
                }
                for (t, c) in users {
                    self.execute(&format!("ALTER TABLE {} ALTER COLUMN {} DROP DEFAULT", t, c), &[])?;
                }
            }
            let deps = self.dependent_views(std::slice::from_ref(n));
            if !deps.is_empty() {
                if !cascade {
                    return Err(format!("cannot drop sequence {} because other objects depend on it (view {} depends on sequence {})", n, deps[0], n));
                }
                self.drop_view_objects(&deps)?;
            }
            self.storage.log_schema_op(SchemaOp::Sequence { name: n.clone(), spec: None });
        }
        Ok(done("DROP SEQUENCE"))
    }

    /// `nextval` / `setval` / `currval` trên sequence độc lập; `None` nếu `key` không phải sequence độc lập.
    pub(crate) fn standalone_sequence_call(&mut self, func: &str, key: &str, args: &[Value]) -> Option<Result<i64, String>> {
        let st: SequenceState = self.storage.objects.sequences.get(key)?.clone();
        let sp = st.spec;
        Some(match func {
            "NEXTVAL" => self.standalone_nextval(key),
            "SETVAL" => {
                let v = match args.get(1).and_then(|x| x.as_i64().or_else(|| x.as_str().trim().parse().ok())) {
                    Some(v) => v,
                    None => return Some(Err("setval: value must be an integer".to_string())),
                };
                let is_called = args.get(2).map_or(true, |x| x.as_bool().unwrap_or(true));
                if v < sp.min || v > sp.max {
                    return Some(Err(format!("setval: value {} is out of bounds for sequence \"{}\" ({}..{})", v, key, sp.min, sp.max)));
                }
                self.storage.log_sequence_value(key, v, is_called);
                Ok(v)
            }
            _ => Err(format!("unsupported sequence function {}", func)),
        })
    }

    /// Cấp số kế tiếp của sequence độc lập (ghi WAL). Lỗi 2200H khi hết miền mà không có CYCLE.
    pub(crate) fn standalone_nextval(&mut self, key: &str) -> Result<i64, String> {
        let st = self.storage.objects.sequences.get(key).cloned().ok_or_else(|| format!("relation \"{}\" does not exist", key))?;
        let sp = st.spec;
        let v = if !st.called {
            st.last
        } else {
            match st.last.checked_add(sp.increment) {
                Some(n) if n >= sp.min && n <= sp.max => n,
                _ if sp.cycle => {
                    if sp.increment > 0 {
                        sp.min
                    } else {
                        sp.max
                    }
                }
                _ => {
                    return Err(if sp.increment > 0 {
                        format!("nextval: reached maximum value of sequence \"{}\" ({})", key, sp.max)
                    } else {
                        format!("nextval: reached minimum value of sequence \"{}\" ({})", key, sp.min)
                    })
                }
            }
        };
        self.storage.log_sequence_value(key, v, true);
        Ok(v)
    }

    /// Có cột nào sở hữu sequence tên `key` (SERIAL / IDENTITY / `DEFAULT nextval`).
    fn find_owned_sequence_key(&self, key: &str) -> bool {
        self.find_owned_sequence_pub(key).is_some()
    }

    // ---------------------------------------------------------------- TYPE (ENUM)

    fn create_type(&mut self, sql: &str) -> Result<QueryResult, String> {
        let rest = after_words(sql, 2).trim_end_matches(';').trim();
        let as_pos = find_top_level_keyword(rest, "AS");
        let Some(as_pos) = as_pos else {
            return Err("CREATE TYPE without AS (shell / base types) is not supported".to_string());
        };
        let name = object_key(&rest[..as_pos]);
        let body = rest[as_pos + 2..].trim();
        let Some(list) = strip_kw(body, "ENUM").or_else(|| body.get(..4).filter(|p| p.eq_ignore_ascii_case("ENUM")).map(|_| body[4..].trim_start())) else {
            let kind = if body.starts_with('(') { "composite types" } else if strip_kw(body, "RANGE").is_some() { "range types" } else { "this kind of type" };
            return Err(format!("CREATE TYPE for {} is not supported", kind));
        };
        let inner = list.trim();
        if !(inner.starts_with('(') && inner.ends_with(')')) {
            return Err("syntax error in CREATE TYPE ... AS ENUM".to_string());
        }
        let mut labels: Vec<String> = Vec::new();
        for item in split_comma_separated_tokens(&inner[1..inner.len() - 1]) {
            if item.trim().is_empty() {
                continue;
            }
            let l = string_literal(item).ok_or_else(|| format!("syntax error at or near \"{}\"", item.trim()))?;
            if l.len() > 63 {
                return Err(format!("invalid enum label \"{}\" (Labels must be 63 bytes or less.)", l));
            }
            if labels.contains(&l) {
                return Err(format!("enum label \"{}\" used more than once", l));
            }
            labels.push(l);
        }
        if self.type_name_taken(&name) {
            return Err(format!("type \"{}\" already exists", name));
        }
        self.storage.log_schema_op(SchemaOp::EnumType { name, labels: Some(labels) });
        Ok(done("CREATE TYPE"))
    }

    fn alter_type(&mut self, sql: &str) -> Result<QueryResult, String> {
        let rest = after_words(sql, 2).trim_end_matches(';').trim();
        let name_end = rest.find(char::is_whitespace).unwrap_or(rest.len());
        let name = object_key(&rest[..name_end]);
        let tail = rest[name_end..].trim();
        let Some(mut labels) = self.storage.objects.enums.get(&name).cloned() else {
            return Err(format!("type \"{}\" does not exist", name));
        };
        if let Some(mut r) = strip_kw(tail, "ADD").and_then(|r| strip_kw(r, "VALUE")) {
            let ine = strip_kw(r, "IF NOT EXISTS").is_some();
            if let Some(x) = strip_kw(r, "IF NOT EXISTS") {
                r = x;
            }
            let lit_end = r.find(|c: char| c == '\'').and_then(|s| {
                let b = r.as_bytes();
                let mut i = s + 1;
                while i < b.len() {
                    if b[i] == b'\'' {
                        if b.get(i + 1) == Some(&b'\'') {
                            i += 2;
                            continue;
                        }
                        return Some(i + 1);
                    }
                    i += 1;
                }
                None
            });
            let lit_end = lit_end.ok_or("syntax error in ALTER TYPE ... ADD VALUE")?;
            let label = string_literal(&r[..lit_end]).ok_or("syntax error in ALTER TYPE ... ADD VALUE")?;
            if labels.contains(&label) {
                if ine {
                    return Ok(done("ALTER TYPE"));
                }
                return Err(format!("enum label \"{}\" already exists", label));
            }
            let pos_part = r[lit_end..].trim();
            let pos = if let Some(x) = strip_kw(pos_part, "BEFORE").or_else(|| strip_kw(pos_part, "AFTER")) {
                let other = string_literal(x).ok_or("syntax error in ALTER TYPE ... ADD VALUE")?;
                let idx = labels.iter().position(|l| *l == other).ok_or_else(|| format!("\"{}\" is not an existing enum label", other))?;
                if pos_part.to_uppercase().starts_with("BEFORE") {
                    idx
                } else {
                    idx + 1
                }
            } else {
                labels.len()
            };
            labels.insert(pos, label);
        } else if let Some(r) = strip_kw(tail, "RENAME").and_then(|r| strip_kw(r, "VALUE")) {
            let to = find_top_level_keyword(r, "TO").ok_or("syntax error in ALTER TYPE ... RENAME VALUE")?;
            let old = string_literal(&r[..to]).ok_or("syntax error in ALTER TYPE ... RENAME VALUE")?;
            let new = string_literal(&r[to + 2..]).ok_or("syntax error in ALTER TYPE ... RENAME VALUE")?;
            let idx = labels.iter().position(|l| *l == old).ok_or_else(|| format!("\"{}\" is not an existing enum label", old))?;
            if labels.contains(&new) {
                return Err(format!("enum label \"{}\" already exists", new));
            }
            labels[idx] = new.clone();
            // dữ liệu đang lưu nhãn cũ: đổi theo (PostgreSQL lưu OID của nhãn nên tự đổi)
            self.storage.log_schema_op(SchemaOp::EnumType { name: name.clone(), labels: Some(labels) });
            set_session_enums(&self.storage.objects.enums);
            for (t, c) in self.enum_typed_columns(&name) {
                self.execute(&format!("UPDATE {} SET {} = {} WHERE {} = {}", t, c, sql_quote(&new), c, sql_quote(&old)), &[])?;
            }
            return Ok(done("ALTER TYPE"));
        } else if strip_kw(tail, "OWNER").is_some() || strip_kw(tail, "SET").is_some() {
            return Ok(done("ALTER TYPE"));
        } else {
            return Err("this form of ALTER TYPE is not supported".to_string());
        }
        self.storage.log_schema_op(SchemaOp::EnumType { name, labels: Some(labels) });
        Ok(done("ALTER TYPE"))
    }

    /// Tên kiểu đã dùng: enum, domain, hoặc kiểu dựng sẵn.
    fn type_name_taken(&self, name: &str) -> bool {
        let o = &self.storage.objects;
        o.enums.contains_key(name) || o.domains.contains_key(name) || crate::engine::coltypes::DeclType::parse(name).map_or(false, |d| d.oid().is_some())
    }

    // ---------------------------------------------------------------- DOMAIN

    fn create_domain(&mut self, sql: &str) -> Result<QueryResult, String> {
        let rest = after_words(sql, 2).trim_end_matches(';').trim();
        let name_end = rest.find(char::is_whitespace).ok_or("syntax error at end of input")?;
        let name = object_key(&rest[..name_end]);
        let mut body = rest[name_end..].trim_start();
        if let Some(r) = strip_kw(body, "AS") {
            body = r;
        }
        let type_end = ["DEFAULT", "NOT NULL", "NULL", "CHECK", "CONSTRAINT", "COLLATE"].iter().filter_map(|kw| find_top_level_keyword(body, kw)).min().unwrap_or(body.len());
        let base = body[..type_end].trim().to_string();
        if base.is_empty() {
            return Err("syntax error in CREATE DOMAIN".to_string());
        }
        let mut def = DomainDef { base, not_null: false, default: None, checks: Vec::new() };
        let mut c = body[type_end..].trim();
        while !c.is_empty() {
            let cu = c.to_uppercase();
            if let Some(r) = strip_kw(c, "CONSTRAINT") {
                let e = r.find(char::is_whitespace).unwrap_or(r.len());
                c = r[e..].trim_start();
            } else if cu.starts_with("NOT NULL") {
                def.not_null = true;
                c = c[8..].trim_start();
            } else if cu.starts_with("NULL") {
                c = c[4..].trim_start();
            } else if cu.starts_with("CHECK") {
                let open = c.find('(').ok_or("syntax error at or near \"CHECK\"")?;
                let close = crate::engine::constraints::matching_paren(c, open).ok_or("syntax error at or near \"CHECK\"")?;
                def.checks.push(c[open + 1..close].trim().to_string());
                c = c[close + 1..].trim_start();
            } else if let Some(r) = strip_kw(c, "COLLATE") {
                let e = r.find(char::is_whitespace).unwrap_or(r.len());
                c = r[e..].trim_start();
            } else if let Some(r) = strip_kw(c, "DEFAULT") {
                let e = ["CONSTRAINT", "NOT NULL", "CHECK", "COLLATE"].iter().filter_map(|kw| find_top_level_keyword(r, kw)).min().unwrap_or(r.len());
                def.default = Some(r[..e].trim().to_string());
                c = r[e..].trim_start();
            } else {
                return Err(format!("syntax error at or near \"{}\"", c.split_whitespace().next().unwrap_or("")));
            }
        }
        if self.type_name_taken(&name) {
            return Err(format!("type \"{}\" already exists", name));
        }
        self.storage.log_schema_op(SchemaOp::Domain { name, def: Some(def) });
        Ok(done("CREATE DOMAIN"))
    }

    fn drop_domains(&mut self, rest: &str) -> Result<QueryResult, String> {
        let (names, if_exists, _) = parse_drop_list(rest);
        for n in &names {
            if !self.storage.objects.domains.contains_key(n) && !if_exists {
                return Err(format!("type \"{}\" does not exist", n));
            }
        }
        for n in names {
            if self.storage.objects.domains.contains_key(&n) {
                self.storage.log_schema_op(SchemaOp::Domain { name: n, def: None });
            }
        }
        Ok(done("DROP DOMAIN"))
    }

    /// Định nghĩa cột `c dom ...` → `c <kiểu gốc> ... [NOT NULL] [DEFAULT ...] CHECK (<đk với VALUE = c>)`.
    fn rewrite_domain_coldef(&self, def: &str) -> Option<String> {
        let d = def.trim();
        let name_end = if d.starts_with('"') { d[1..].find('"').map(|p| p + 2)? } else { d.find(char::is_whitespace)? };
        let rest = d[name_end..].trim_start();
        let ty_end = rest.find(|c: char| c.is_whitespace() || c == ',').unwrap_or(rest.len());
        let ty_word = &rest[..ty_end];
        if ty_word.ends_with(']') {
            return None;
        }
        let dom = self.storage.objects.domains.get(&object_key(ty_word))?;
        let col = &d[..name_end];
        let tail = &rest[ty_end..];
        let tu = tail.to_uppercase();
        let mut out = format!("{} {}{}", col, dom.base, tail);
        if dom.not_null && !tu.contains("NOT NULL") {
            out.push_str(" NOT NULL");
        }
        if let Some(dv) = &dom.default {
            if find_top_level_keyword(tail, "DEFAULT").is_none() {
                out.push_str(&format!(" DEFAULT {}", dv));
            }
        }
        for c in &dom.checks {
            let mut e = c.clone();
            for (s0, e0, w) in ident_words(c).iter().rev() {
                if w.eq_ignore_ascii_case("VALUE") {
                    e.replace_range(*s0..*e0, col);
                }
            }
            out.push_str(&format!(" CHECK ({})", e));
        }
        Some(out)
    }

    /// `CREATE TABLE` có cột kiểu domain; `None` = không có gì để đổi.
    fn rewrite_domain_create_table(&self, sql: &str) -> Option<String> {
        if self.storage.objects.domains.is_empty() || find_top_level_keyword(sql, "AS").is_some() {
            return None;
        }
        let open = sql.find('(')?;
        let close = crate::engine::constraints::matching_paren(sql, open)?;
        let mut changed = false;
        let items: Vec<String> = split_comma_separated_tokens(&sql[open + 1..close])
            .into_iter()
            .map(|it| match self.rewrite_domain_coldef(it) {
                Some(r) => {
                    changed = true;
                    r
                }
                None => it.trim().to_string(),
            })
            .collect();
        changed.then(|| format!("{}({}){}", &sql[..open], items.join(", "), &sql[close + 1..]))
    }

    /// `ALTER TABLE t ADD [COLUMN] [IF NOT EXISTS] c dom ...`.
    fn rewrite_domain_alter_table(&self, sql: &str) -> Option<String> {
        let words = ident_words(sql);
        let mut changed = false;
        let mut out = sql.to_string();
        let mut i = words.len();
        while i > 0 {
            i -= 1;
            if !words[i].2.eq_ignore_ascii_case("ADD") {
                continue;
            }
            let mut j = i + 1;
            if words.get(j).map_or(false, |w| w.2.eq_ignore_ascii_case("COLUMN")) {
                j += 1;
            }
            if words.get(j).map_or(false, |w| w.2.eq_ignore_ascii_case("IF")) {
                j += 3;
            }
            let Some(start) = words.get(j).map(|w| w.0) else { continue };
            let end = {
                let tail = &sql[start..];
                let mut depth = 0i32;
                let mut e = tail.len();
                for (k, ch) in tail.char_indices() {
                    match ch {
                        '(' => depth += 1,
                        ')' => depth -= 1,
                        ',' | ';' if depth == 0 => {
                            e = k;
                            break;
                        }
                        _ => {}
                    }
                }
                start + e
            };
            if let Some(r) = self.rewrite_domain_coldef(&sql[start..end]) {
                out.replace_range(start..end, &r);
                changed = true;
            }
        }
        changed.then_some(out)
    }

    /// (bảng, cột) có kiểu enum `name` (kể cả mảng).
    fn enum_typed_columns(&self, name: &str) -> Vec<(String, String)> {
        let mut out = Vec::new();
        let mut keys: Vec<&String> = self.storage.tables.keys().collect();
        keys.sort();
        for k in keys {
            let t = &self.storage.tables[k];
            for c in &t.columns {
                if t.decl_types.get(&c.name.to_lowercase()).map_or(false, |d| d.base == name) {
                    out.push((k.clone(), c.name.clone()));
                }
            }
        }
        out
    }

    fn drop_types(&mut self, rest: &str) -> Result<QueryResult, String> {
        let (names, if_exists, cascade) = parse_drop_list(rest);
        for n in &names {
            if !self.storage.objects.enums.contains_key(n) {
                if if_exists {
                    continue;
                }
                return Err(format!("type \"{}\" does not exist", n));
            }
            let users = self.enum_typed_columns(n);
            if !users.is_empty() {
                if !cascade {
                    return Err(format!("cannot drop type {} because other objects depend on it (column {} of table {} depends on type {})", n, users[0].1, users[0].0, n));
                }
                for (t, c) in users {
                    self.execute(&format!("ALTER TABLE {} DROP COLUMN {}", t, c), &[])?;
                }
            }
        }
        for n in names {
            if self.storage.objects.enums.contains_key(&n) {
                self.storage.log_schema_op(SchemaOp::EnumType { name: n, labels: None });
            }
        }
        Ok(done("DROP TYPE"))
    }

    /// SELECT trên catalog của đối tượng (`information_schema.views`, `pg_sequences`, `pg_enum`,
    /// `information_schema.schemata`...): dựng bảng tạm trong bộ nhớ (không ghi WAL) rồi chạy câu lệnh
    /// như bình thường, nên WHERE / JOIN / ORDER BY / hàm gộp đều dùng được. `None` = không nhắc tới.
    pub(crate) fn object_catalog_select(&mut self, sql: &str, params: &[Value]) -> Option<Result<QueryResult, String>> {
        const RELS: [(&str, &str); 14] = [
            ("information_schema.views", "information_schema.views"),
            ("information_schema.sequences", "information_schema.sequences"),
            ("information_schema.schemata", "information_schema.schemata"),
            ("pg_catalog.pg_views", "pg_views"),
            ("pg_catalog.pg_matviews", "pg_matviews"),
            ("pg_catalog.pg_sequences", "pg_sequences"),
            ("pg_catalog.pg_enum", "pg_enum"),
            ("public.pg_views", "pg_views"),
            ("public.pg_enum", "pg_enum"),
            ("pg_views", "pg_views"),
            ("pg_matviews", "pg_matviews"),
            ("pg_sequences", "pg_sequences"),
            ("pg_enum", "pg_enum"),
            ("schemata", ""),
        ];
        let lower = sql.to_lowercase();
        let b = lower.as_bytes();
        let occurs = |pat: &str| {
            let mut from = 0;
            while let Some(p) = lower[from..].find(pat) {
                let s = from + p;
                let e = s + pat.len();
                let left_ok = s == 0 || !(is_ident_byte(b[s - 1]) || b[s - 1] == b'.');
                let right_ok = e == b.len() || !(is_ident_byte(b[e]) || b[e] == b'.');
                if left_ok && right_ok {
                    return true;
                }
                from = e;
            }
            false
        };
        let mut found: Vec<(&str, &str)> = Vec::new();
        for (pat, rel) in RELS {
            if !rel.is_empty() && occurs(pat) && !found.iter().any(|(p, _)| p.contains(pat)) {
                found.push((pat, rel));
            }
        }
        if found.is_empty() {
            return None;
        }
        let mut q = sql.to_string();
        let mut created: Vec<String> = Vec::new();
        for (i, (pat, rel)) in found.iter().enumerate() {
            let Some((cols, rows)) = self.object_catalog_rows(rel) else { continue };
            let name = format!("__pglite_objcat_{}", i);
            let defs: Vec<crate::types::ColumnDef> = cols
                .iter()
                .enumerate()
                .map(|(ci, c)| {
                    let sample = rows.iter().map(|r| &r[ci]).find(|v| !v.is_null());
                    let dt = match sample {
                        Some(Value::Int(_)) => crate::types::DataType::BigInt,
                        Some(Value::Float(_)) => crate::types::DataType::Numeric,
                        Some(Value::Bool(_)) => crate::types::DataType::Boolean,
                        _ => crate::types::DataType::Text,
                    };
                    crate::types::ColumnDef { name: c.to_string(), data_type: dt, is_primary_key: false, is_nullable: true, default_value: None, comment: None }
                })
                .collect();
            let mut t = crate::storage::table::Table::new(name.clone(), defs);
            t.pk_col_idx = None;
            for r in rows {
                t.insert(r);
            }
            q = super::executor::replace_relation_name(&q, &[pat], &name);
            self.storage.tables.insert(name.clone(), t);
            created.push(name);
        }
        if created.is_empty() {
            return None;
        }
        let res = self.execute_statement(&q, params);
        for n in created {
            self.storage.tables.remove(&n);
        }
        self.plan_cache.retain(|k, _| !k.contains("__pglite_objcat_"));
        Some(res)
    }

    /// `information_schema.views` / `pg_views` / `pg_matviews` / `information_schema.sequences` /
    /// `pg_sequences` / `pg_enum` / `information_schema.schemata`: dữ liệu (tên bảng ảo, cột, dòng).
    pub(crate) fn object_catalog_rows(&self, relation: &str) -> Option<(Vec<&'static str>, Vec<Vec<Value>>)> {
        let o = &self.storage.objects;
        let t = |s: &str| Value::text(s);
        match relation {
            "information_schema.views" => Some((
                vec!["table_catalog", "table_schema", "table_name", "view_definition", "check_option", "is_updatable", "is_insertable_into", "is_trigger_updatable", "is_trigger_deletable", "is_trigger_insertable_into"],
                o.views
                    .iter()
                    .filter(|(_, v)| !v.materialized)
                    .map(|(n, v)| vec![t("postgres"), t("public"), t(n), t(&v.query), t("NONE"), t("NO"), t("NO"), t("NO"), t("NO"), t("NO")])
                    .collect(),
            )),
            "pg_views" => Some((
                vec!["schemaname", "viewname", "viewowner", "definition"],
                o.views.iter().filter(|(_, v)| !v.materialized).map(|(n, v)| vec![t("public"), t(n), t("postgres"), t(&v.query)]).collect(),
            )),
            "pg_matviews" => Some((
                vec!["schemaname", "matviewname", "matviewowner", "tablespace", "hasindexes", "ispopulated", "definition"],
                o.views.iter().filter(|(_, v)| v.materialized).map(|(n, v)| vec![t("public"), t(n), t("postgres"), Value::Null, Value::Bool(false), Value::Bool(true), t(&v.query)]).collect(),
            )),
            "information_schema.sequences" => Some((
                vec!["sequence_catalog", "sequence_schema", "sequence_name", "data_type", "numeric_precision", "numeric_precision_radix", "numeric_scale", "start_value", "minimum_value", "maximum_value", "increment", "cycle_option"],
                o.sequences
                    .iter()
                    .map(|(n, s)| {
                        let sp = s.spec;
                        vec![t("postgres"), t("public"), t(n), t("bigint"), Value::Int(64), Value::Int(2), Value::Int(0), t(&sp.start.to_string()), t(&sp.min.to_string()), t(&sp.max.to_string()), t(&sp.increment.to_string()), t(if sp.cycle { "YES" } else { "NO" })]
                    })
                    .collect(),
            )),
            "pg_sequences" => Some((
                vec!["schemaname", "sequencename", "sequenceowner", "data_type", "start_value", "min_value", "max_value", "increment_by", "cycle", "cache_size", "last_value"],
                o.sequences
                    .iter()
                    .map(|(n, s)| {
                        let sp = s.spec;
                        vec![t("public"), t(n), t("postgres"), t("bigint"), Value::Int(sp.start), Value::Int(sp.min), Value::Int(sp.max), Value::Int(sp.increment), Value::Bool(sp.cycle), Value::Int(1), if s.called { Value::Int(s.last) } else { Value::Null }]
                    })
                    .collect(),
            )),
            "pg_enum" => Some((
                vec!["oid", "enumtypid", "enumsortorder", "enumlabel"],
                o.enums
                    .values()
                    .enumerate()
                    .flat_map(|(ti, labels)| {
                        labels.iter().enumerate().map(move |(i, l)| vec![Value::Int(91000 + (ti * 100 + i) as i64), Value::Int(90000 + ti as i64), Value::Float((i + 1) as f64), Value::text(l)])
                    })
                    .collect(),
            )),
            "information_schema.schemata" => {
                let mut names: Vec<String> = vec!["information_schema".into(), "pg_catalog".into(), "pg_toast".into()];
                if !o.public_dropped {
                    names.push("public".into());
                }
                names.extend(o.schemas.iter().cloned());
                names.sort();
                Some((
                    vec!["catalog_name", "schema_name", "schema_owner", "default_character_set_catalog", "default_character_set_schema", "default_character_set_name", "sql_path"],
                    names.iter().map(|n| vec![t("postgres"), t(n), t(if n == "public" { "pg_database_owner" } else { "postgres" }), Value::Null, Value::Null, Value::Null, Value::Null]).collect(),
                ))
            }
            _ => None,
        }
    }
}
