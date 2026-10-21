//! `DO $$ ... $$`: bộ con PL/pgSQL đủ cho script migration.
//!
//! Hỗ trợ: `[DECLARE]` biến (`x int := 0`, `r record`, `CONSTANT`, `DEFAULT`), khối `BEGIN ... [EXCEPTION
//! WHEN ... THEN ...] END` lồng nhau (trong handler có `SQLERRM`, `SQLSTATE`), gán `x := biểu thức`,
//! `SELECT ... INTO [STRICT] x, y`, `... RETURNING ... INTO x`, `IF ... THEN ... [ELSIF ...] [ELSE ...]
//! END IF`, vòng lặp `LOOP`, `WHILE`, `FOR i IN [REVERSE] a..b [BY n]`, `FOR r IN <query> | EXECUTE <expr>
//! [USING ...]`, `FOREACH x IN ARRAY <expr>`, `EXIT` / `CONTINUE [WHEN ...]`, `PERFORM`, `EXECUTE <expr>
//! [INTO ...] [USING ...]`, `GET DIAGNOSTICS x = ROW_COUNT`, `RAISE NOTICE|...|EXCEPTION 'fmt %', args`,
//! `NULL`, `RETURN`, và câu SQL thường. Biểu thức / điều kiện được tính bằng chính engine
//! (`SELECT (<biểu thức>)`) sau khi thay biến bằng giá trị.
//!
//! Biến khai báo nhưng chưa từng được gán mà trùng tên cột của một bảng thì không bị thay (giữ hành vi các
//! bản trước, khi khai báo bị bỏ qua).
//!
//! Cả khối chạy như một đơn vị: ngoài giao dịch thì được bọc trong giao dịch ngầm (lỗi -> ROLLBACK).
//! `EXCEPTION` dùng savepoint như PostgreSQL (thay đổi dữ liệu của khối lỗi bị huỷ).
use super::executor::{dollar_quote_end, find_top_level_keyword, json_to_value, split_comma_separated_tokens, Executor};
use crate::types::{QueryResult, Value};
use std::collections::HashMap;

#[derive(Debug)]
enum Stmt {
    Sql(String),
    Assign { target: String, expr: String },
    /// `SELECT ... INTO x` / `INSERT ... RETURNING ... INTO x`: câu đã bỏ phần INTO
    SqlInto { sql: String, targets: Vec<String>, strict: bool, orig: String },
    Execute { expr: String, into: Vec<String>, strict: bool, using: Vec<String> },
    If { branches: Vec<(String, Vec<Stmt>)>, otherwise: Vec<Stmt> },
    Block(Block),
    Loop(Vec<Stmt>),
    While { cond: String, body: Vec<Stmt> },
    ForRange { var: String, from: String, to: String, by: Option<String>, reverse: bool, body: Vec<Stmt> },
    ForQuery { targets: Vec<String>, query: String, dynamic: bool, using: Vec<String>, body: Vec<Stmt> },
    ForEach { var: String, array: String, body: Vec<Stmt> },
    Exit { when: Option<String> },
    Continue { when: Option<String> },
    GetDiagnostics { target: String },
    Raise { level: String, message: String, args: Vec<String>, errcode: Option<String> },
    Null,
    /// `RETURN [biểu thức]` (hàm / trigger); DO chỉ dùng dạng không biểu thức
    Return(Option<String>),
    /// `RETURN NEXT [biểu thức]` (hàm SETOF)
    ReturnNext(Option<String>),
    /// `RETURN QUERY câu_lệnh` / `RETURN QUERY EXECUTE biểu_thức [USING ...]`
    ReturnQuery { sql: String, dynamic: bool, using: Vec<String> },
}

#[derive(Debug)]
struct Decl {
    name: String,
    ty: String,
    init: Option<String>,
}

#[derive(Debug)]
pub(crate) struct Block {
    decls: Vec<Decl>,
    body: Vec<Stmt>,
    /// (điều kiện như `others`, `duplicate_object`; các lệnh xử lý)
    handlers: Vec<(Vec<String>, Vec<Stmt>)>,
}

struct Parser<'a> {
    s: &'a str,
    pos: usize,
}

fn is_ident_byte(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'_' || b >= 0x80
}

fn is_ident(s: &str) -> bool {
    let s = s.trim();
    !s.is_empty() && s.bytes().next().map_or(false, |b| b.is_ascii_alphabetic() || b == b'_' || b >= 0x80) && s.bytes().all(is_ident_byte)
}

impl<'a> Parser<'a> {
    fn skip_ws(&mut self) {
        let b = self.s.as_bytes();
        loop {
            while self.pos < b.len() && b[self.pos].is_ascii_whitespace() {
                self.pos += 1;
            }
            if self.s[self.pos..].starts_with("--") {
                while self.pos < b.len() && b[self.pos] != b'\n' {
                    self.pos += 1;
                }
                continue;
            }
            if self.s[self.pos..].starts_with("/*") {
                self.pos = self.s[self.pos..].find("*/").map(|e| self.pos + e + 2).unwrap_or(b.len());
                continue;
            }
            // nhãn `<<tên>>` trước khối / vòng lặp: bỏ qua
            if self.s[self.pos..].starts_with("<<") {
                if let Some(e) = self.s[self.pos..].find(">>") {
                    self.pos += e + 2;
                    continue;
                }
            }
            break;
        }
    }

    /// Từ khoá `kw` ở vị trí hiện tại (đủ cả từ).
    fn peek_kw(&mut self, kw: &str) -> bool {
        self.skip_ws();
        let rest = &self.s[self.pos..];
        rest.len() >= kw.len() && rest.get(..kw.len()).map_or(false, |p| p.eq_ignore_ascii_case(kw)) && rest.as_bytes().get(kw.len()).map_or(true, |&b| !is_ident_byte(b))
    }

    fn eat_kw(&mut self, kw: &str) -> bool {
        if self.peek_kw(kw) {
            self.pos += kw.len();
            true
        } else {
            false
        }
    }

    fn expect_kw(&mut self, kw: &str) -> Result<(), String> {
        if self.eat_kw(kw) {
            Ok(())
        } else {
            Err(format!("syntax error in DO block: expected {} near \"{}\"", kw, snippet(&self.s[self.pos..])))
        }
    }

    fn eat_semicolon(&mut self) {
        self.skip_ws();
        if self.s[self.pos..].starts_with(';') {
            self.pos += 1;
        }
    }

    /// Đọc tới `;` hoặc từ khoá dừng (`stop`) ở cấp ngoài cùng; bỏ qua chuỗi, ngoặc, `$$`.
    fn read_until(&mut self, stop: &[&str]) -> String {
        self.skip_ws();
        let b = self.s.as_bytes();
        let start = self.pos;
        let mut depth = 0i32;
        let mut i = self.pos;
        while i < b.len() {
            let c = b[i];
            if c == b'\'' || c == b'"' {
                let q = c;
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
                i += 1;
                continue;
            }
            if c == b'$' {
                if let Some(end) = dollar_quote_end(self.s, i) {
                    i = end;
                    continue;
                }
            }
            if c == b'(' {
                depth += 1;
            } else if c == b')' {
                depth -= 1;
            } else if depth == 0 {
                if c == b';' {
                    break;
                }
                let word_start = i == 0 || !is_ident_byte(b[i - 1]);
                if word_start && !stop.is_empty() {
                    let rest = &self.s[i..];
                    if stop.iter().any(|kw| rest.len() >= kw.len() && rest.get(..kw.len()).map_or(false, |p| p.eq_ignore_ascii_case(kw)) && rest.as_bytes().get(kw.len()).map_or(true, |&x| !is_ident_byte(x))) {
                        break;
                    }
                }
            }
            i += 1;
        }
        self.pos = i.min(b.len());
        self.s[start..self.pos].trim().to_string()
    }

    /// `END LOOP [nhãn];`
    fn end_loop(&mut self) -> Result<(), String> {
        self.expect_kw("END")?;
        self.expect_kw("LOOP")?;
        let _label = self.read_until(&[]);
        self.eat_semicolon();
        Ok(())
    }

    /// `[nhãn] [WHEN điều kiện];` sau EXIT / CONTINUE
    fn exit_tail(&mut self) -> Option<String> {
        let rest = self.read_until(&[]);
        self.eat_semicolon();
        let up = rest.to_uppercase();
        find_top_level_keyword(&rest, "WHEN").map(|p| rest[p + 4..].trim().to_string()).filter(|c| !c.is_empty()).or_else(|| up.starts_with("WHEN").then(|| rest[4..].trim().to_string()))
    }

    fn parse_block(&mut self) -> Result<Block, String> {
        let mut decls = Vec::new();
        if self.eat_kw("DECLARE") {
            while !self.peek_kw("BEGIN") && self.pos < self.s.len() {
                let decl = self.read_until(&["BEGIN"]);
                if decl.is_empty() && !self.s[self.pos..].starts_with(';') {
                    break;
                }
                self.eat_semicolon();
                if let Some(d) = parse_decl(&decl) {
                    decls.push(d);
                }
            }
        }
        self.expect_kw("BEGIN")?;
        let body = self.parse_stmts(&["EXCEPTION", "END"])?;
        let mut handlers = Vec::new();
        if self.eat_kw("EXCEPTION") {
            while self.eat_kw("WHEN") {
                let cond = self.read_until(&["THEN"]);
                self.expect_kw("THEN")?;
                let conds = cond.split(|c: char| c.is_whitespace()).filter(|w| !w.is_empty() && !w.eq_ignore_ascii_case("OR")).map(|w| w.to_lowercase()).collect();
                let stmts = self.parse_stmts(&["WHEN", "END"])?;
                handlers.push((conds, stmts));
            }
        }
        self.expect_kw("END")?;
        // nhãn khối tuỳ chọn
        let _label = self.read_until(&[]);
        self.eat_semicolon();
        Ok(Block { decls, body, handlers })
    }

    fn parse_stmts(&mut self, stop: &[&str]) -> Result<Vec<Stmt>, String> {
        let mut out = Vec::new();
        loop {
            self.skip_ws();
            if self.pos >= self.s.len() {
                return Err("syntax error in DO block: unexpected end of block".to_string());
            }
            if stop.iter().any(|kw| self.peek_kw(kw)) {
                return Ok(out);
            }
            if self.s[self.pos..].starts_with(';') {
                self.pos += 1;
                continue;
            }
            if self.peek_kw("IF") {
                self.pos += 2;
                let mut branches = Vec::new();
                let cond = self.read_until(&["THEN"]);
                self.expect_kw("THEN")?;
                let stmts = self.parse_stmts(&["ELSIF", "ELSEIF", "ELSE", "END"])?;
                branches.push((cond, stmts));
                let mut otherwise = Vec::new();
                loop {
                    if self.eat_kw("ELSIF") || self.eat_kw("ELSEIF") {
                        let cond = self.read_until(&["THEN"]);
                        self.expect_kw("THEN")?;
                        let stmts = self.parse_stmts(&["ELSIF", "ELSEIF", "ELSE", "END"])?;
                        branches.push((cond, stmts));
                    } else if self.eat_kw("ELSE") {
                        otherwise = self.parse_stmts(&["END"])?;
                    } else {
                        break;
                    }
                }
                self.expect_kw("END")?;
                self.expect_kw("IF")?;
                self.eat_semicolon();
                out.push(Stmt::If { branches, otherwise });
                continue;
            }
            if self.peek_kw("BEGIN") || self.peek_kw("DECLARE") {
                // BEGIN đứng đầu câu trong thân khối là khối lồng nhau (không phải giao dịch)
                out.push(Stmt::Block(self.parse_block()?));
                continue;
            }
            if self.eat_kw("LOOP") {
                let body = self.parse_stmts(&["END"])?;
                self.end_loop()?;
                out.push(Stmt::Loop(body));
                continue;
            }
            if self.eat_kw("WHILE") {
                let cond = self.read_until(&["LOOP"]);
                self.expect_kw("LOOP")?;
                let body = self.parse_stmts(&["END"])?;
                self.end_loop()?;
                out.push(Stmt::While { cond, body });
                continue;
            }
            if self.eat_kw("FOREACH") {
                let var = self.read_until(&["IN"]);
                self.expect_kw("IN")?;
                self.expect_kw("ARRAY")?;
                let array = self.read_until(&["LOOP"]);
                self.expect_kw("LOOP")?;
                let body = self.parse_stmts(&["END"])?;
                self.end_loop()?;
                out.push(Stmt::ForEach { var: var.to_lowercase(), array, body });
                continue;
            }
            if self.eat_kw("FOR") {
                let vars = self.read_until(&["IN"]);
                self.expect_kw("IN")?;
                let targets: Vec<String> = vars.split(',').map(|v| v.trim().to_lowercase()).collect();
                if self.eat_kw("EXECUTE") {
                    let expr = self.read_until(&["USING", "LOOP"]);
                    let using = if self.eat_kw("USING") { split_args(&self.read_until(&["LOOP"])) } else { Vec::new() };
                    self.expect_kw("LOOP")?;
                    let body = self.parse_stmts(&["END"])?;
                    self.end_loop()?;
                    out.push(Stmt::ForQuery { targets, query: expr, dynamic: true, using, body });
                    continue;
                }
                let reverse = self.eat_kw("REVERSE");
                let spec = self.read_until(&["LOOP"]);
                self.expect_kw("LOOP")?;
                let body = self.parse_stmts(&["END"])?;
                self.end_loop()?;
                if let Some(dd) = find_range_dots(&spec) {
                    let from = spec[..dd].trim().to_string();
                    let rest = spec[dd + 2..].trim();
                    let (to, by) = match find_top_level_keyword(rest, "BY") {
                        Some(p) => (rest[..p].trim().to_string(), Some(rest[p + 2..].trim().to_string())),
                        None => (rest.to_string(), None),
                    };
                    out.push(Stmt::ForRange { var: targets[0].clone(), from, to, by, reverse, body });
                } else {
                    out.push(Stmt::ForQuery { targets, query: spec, dynamic: false, using: Vec::new(), body });
                }
                continue;
            }
            if self.eat_kw("EXIT") {
                out.push(Stmt::Exit { when: self.exit_tail() });
                continue;
            }
            if self.eat_kw("CONTINUE") {
                out.push(Stmt::Continue { when: self.exit_tail() });
                continue;
            }
            if self.peek_kw("GET") {
                let save = self.pos;
                self.eat_kw("GET");
                if self.eat_kw("DIAGNOSTICS") || (self.eat_kw("CURRENT") && self.eat_kw("DIAGNOSTICS")) {
                    let item = self.read_until(&[]);
                    self.eat_semicolon();
                    let target = item.split(|c| c == '=' || c == ':').next().unwrap_or("").trim().to_lowercase();
                    if !item.to_uppercase().contains("ROW_COUNT") {
                        return Err(format!("GET DIAGNOSTICS item is not supported in DO blocks: {}", item));
                    }
                    out.push(Stmt::GetDiagnostics { target });
                    continue;
                }
                self.pos = save;
            }
            if self.eat_kw("RAISE") {
                let rest = self.read_until(&[]);
                self.eat_semicolon();
                let mut words = rest.splitn(2, char::is_whitespace);
                let first = words.next().unwrap_or("").to_uppercase();
                let (level, body) = if ["NOTICE", "INFO", "WARNING", "LOG", "DEBUG", "EXCEPTION"].contains(&first.as_str()) {
                    (first, words.next().unwrap_or("").trim().to_string())
                } else {
                    ("EXCEPTION".to_string(), rest.clone())
                };
                // `USING ERRCODE = '22023' | 'tên_điều_kiện', MESSAGE = ...`
                let (body, errcode) = match find_top_level_keyword(&body, "USING") {
                    Some(p) => {
                        let opts = body[p + 5..].to_string();
                        let mut code = None;
                        for o in split_args(&opts) {
                            if let Some(eq) = o.find('=') {
                                if o[..eq].trim().eq_ignore_ascii_case("ERRCODE") {
                                    code = string_literal(o[eq + 1..].trim()).or_else(|| Some(o[eq + 1..].trim().to_string()));
                                }
                            }
                        }
                        (body[..p].trim().to_string(), code)
                    }
                    None => (body, None),
                };
                let parts = split_args(&body);
                let message = parts.first().map(|m| raise_message(m)).unwrap_or_default();
                out.push(Stmt::Raise { level, message, args: parts.into_iter().skip(1).collect(), errcode });
                continue;
            }
            if self.eat_kw("NULL") {
                self.eat_semicolon();
                out.push(Stmt::Null);
                continue;
            }
            if self.eat_kw("RETURN") {
                if self.eat_kw("NEXT") {
                    let e = self.read_until(&[]);
                    self.eat_semicolon();
                    out.push(Stmt::ReturnNext((!e.trim().is_empty()).then(|| e.trim().to_string())));
                    continue;
                }
                if self.eat_kw("QUERY") {
                    if self.eat_kw("EXECUTE") {
                        let expr = self.read_until(&["USING"]);
                        let using = if self.eat_kw("USING") { split_args(&self.read_until(&[])) } else { Vec::new() };
                        self.eat_semicolon();
                        out.push(Stmt::ReturnQuery { sql: expr, dynamic: true, using });
                    } else {
                        let q = self.read_until(&[]);
                        self.eat_semicolon();
                        out.push(Stmt::ReturnQuery { sql: q, dynamic: false, using: Vec::new() });
                    }
                    continue;
                }
                let e = self.read_until(&[]);
                self.eat_semicolon();
                out.push(Stmt::Return((!e.trim().is_empty()).then(|| e.trim().to_string())));
                continue;
            }
            if self.eat_kw("PERFORM") {
                let expr = self.read_until(&[]);
                self.eat_semicolon();
                out.push(Stmt::Sql(format!("SELECT {}", expr)));
                continue;
            }
            if self.eat_kw("EXECUTE") {
                let expr = self.read_until(&["INTO", "USING"]);
                let mut into = Vec::new();
                let mut strict = false;
                let mut using = Vec::new();
                loop {
                    if self.eat_kw("INTO") {
                        strict = self.eat_kw("STRICT");
                        into = self.read_until(&["USING"]).split(',').map(|t| t.trim().to_lowercase()).collect();
                    } else if self.eat_kw("USING") {
                        using = split_args(&self.read_until(&["INTO"]));
                    } else {
                        break;
                    }
                }
                self.eat_semicolon();
                out.push(Stmt::Execute { expr, into, strict, using });
                continue;
            }
            let sql = self.read_until(&[]);
            self.eat_semicolon();
            if sql.is_empty() {
                continue;
            }
            // gán: `x := biểu thức` / `x = biểu thức`
            if let Some(p) = find_assign(&sql) {
                let target = sql[..p].trim();
                if is_ident(target) || target.split('.').all(is_ident) {
                    let op_len = if sql[p..].starts_with(":=") { 2 } else { 1 };
                    out.push(Stmt::Assign { target: target.to_lowercase(), expr: sql[p + op_len..].trim().to_string() });
                    continue;
                }
            }
            if let Some((stripped, targets, strict)) = split_into(&sql) {
                out.push(Stmt::SqlInto { sql: stripped, targets, strict, orig: sql });
                continue;
            }
            out.push(Stmt::Sql(sql));
        }
    }
}

/// `x int := 0`, `r record`, `msg text DEFAULT ''`, `n CONSTANT int = 5`.
fn parse_decl(decl: &str) -> Option<Decl> {
    let d = decl.trim();
    if d.is_empty() {
        return None;
    }
    let name_end = d.find(|c: char| c.is_whitespace()).unwrap_or(d.len());
    let name = d[..name_end].trim_matches('"').to_lowercase();
    if !is_ident(&name) {
        return None;
    }
    let rest = d[name_end..].trim();
    let (ty, init) = if let Some(p) = find_top_level_op(rest, ":=") {
        (rest[..p].trim(), Some(rest[p + 2..].trim().to_string()))
    } else if let Some(p) = find_top_level_keyword(rest, "DEFAULT") {
        (rest[..p].trim(), Some(rest[p + 7..].trim().to_string()))
    } else if let Some(p) = find_top_level_op(rest, "=") {
        (rest[..p].trim(), Some(rest[p + 1..].trim().to_string()))
    } else {
        (rest, None)
    };
    let mut ty = ty.to_string();
    for kw in ["CONSTANT ", "constant "] {
        if let Some(t) = ty.strip_prefix(kw) {
            ty = t.trim().to_string();
        }
    }
    let ty = match find_top_level_keyword(&ty, "NOT NULL") {
        Some(p) => ty[..p].trim().to_string(),
        None => ty,
    };
    Some(Decl { name, ty: ty.to_lowercase(), init })
}

/// Vị trí của `op` ở cấp ngoài cùng (ngoài chuỗi / ngoặc).
fn find_top_level_op(s: &str, op: &str) -> Option<usize> {
    let b = s.as_bytes();
    let (mut depth, mut in_sq, mut in_dq) = (0i32, false, false);
    let mut i = 0;
    while i < b.len() {
        match b[i] {
            b'\'' if !in_dq => in_sq = !in_sq,
            b'"' if !in_sq => in_dq = !in_dq,
            b'(' | b'[' if !in_sq && !in_dq => depth += 1,
            b')' | b']' if !in_sq && !in_dq => depth -= 1,
            _ if !in_sq && !in_dq && depth == 0 && b[i..].starts_with(op.as_bytes()) => {
                // `=` không phải một phần của `:=`, `<=`, `>=`, `<>`, `!=`, `==`
                if op == "=" && (i > 0 && matches!(b[i - 1], b':' | b'<' | b'>' | b'!' | b'=') || b.get(i + 1) == Some(&b'=')) {
                    i += 1;
                    continue;
                }
                return Some(i);
            }
            _ => {}
        }
        i += 1;
    }
    None
}

/// Phép gán ở đầu câu: `ident :=` hoặc `ident =` (vế trái là một tên).
fn find_assign(sql: &str) -> Option<usize> {
    let p = find_top_level_op(sql, ":=").or_else(|| find_top_level_op(sql, "="))?;
    let lhs = sql[..p].trim();
    (!lhs.is_empty() && (is_ident(lhs) || lhs.split('.').all(is_ident))).then_some(p)
}

/// `a .. b` của FOR số nguyên (ngoài chuỗi / ngoặc).
fn find_range_dots(s: &str) -> Option<usize> {
    let p = find_top_level_op(s, "..")?;
    (p > 0).then_some(p)
}

/// Danh sách đối số cách nhau bởi dấu phẩy ở cấp ngoài cùng.
fn split_args(s: &str) -> Vec<String> {
    if s.trim().is_empty() {
        return Vec::new();
    }
    split_comma_separated_tokens(s).into_iter().map(|t| t.trim().to_string()).collect()
}

/// `SELECT a, b INTO [STRICT] x, y FROM ...` hoặc `INSERT/UPDATE/DELETE ... RETURNING a INTO x`:
/// (câu không có INTO, đích, STRICT). `SELECT ... INTO bảng` (tạo bảng) không thuộc diện: đích phải là
/// tên đơn và câu SELECT phải có FROM sau INTO hoặc không có FROM.
fn split_into(sql: &str) -> Option<(String, Vec<String>, bool)> {
    let up = sql.trim_start().to_uppercase();
    let p = find_top_level_keyword(sql, "INTO")?;
    if up.starts_with("SELECT") || up.starts_with("WITH") {
        // vị trí INTO phải nằm trước FROM (danh sách SELECT)
        let from = find_top_level_keyword(sql, "FROM");
        if from.map_or(false, |f| f < p) {
            return None;
        }
    } else if up.starts_with("INSERT") || up.starts_with("UPDATE") || up.starts_with("DELETE") {
        let r = find_top_level_keyword(sql, "RETURNING")?;
        if r > p {
            return None;
        }
    } else {
        return None;
    }
    let after = sql[p + 4..].trim_start();
    let (strict, after) = match after.get(..7) {
        Some(w) if w.eq_ignore_ascii_case("STRICT ") => (true, after[7..].trim_start()),
        _ => (false, after),
    };
    // đích kết thúc ở FROM / WHERE / ... hoặc cuối câu
    let mut end = after.len();
    for kw in ["FROM", "WHERE", "GROUP", "ORDER", "LIMIT", "UNION", "HAVING", "OFFSET"] {
        if let Some(q) = find_top_level_keyword(after, kw) {
            end = end.min(q);
        }
    }
    let targets: Vec<String> = after[..end].split(',').map(|t| t.trim().to_lowercase()).collect();
    if targets.is_empty() || !targets.iter().all(|t| is_ident(t) || t.split('.').all(is_ident)) {
        return None;
    }
    let stripped = format!("{} {}", sql[..p].trim_end(), after[end..].trim_start());
    Some((stripped.trim().to_string(), targets, strict))
}

fn snippet(s: &str) -> String {
    s.chars().take(40).collect()
}

/// `'abc'` hoặc `$x$abc$x$` -> nội dung.
fn string_literal(s: &str) -> Option<String> {
    let t = s.trim();
    if t.len() >= 2 && t.starts_with('\'') && t.ends_with('\'') {
        return Some(t[1..t.len() - 1].replace("''", "'"));
    }
    if t.starts_with('$') {
        let tag_end = t[1..].find('$')? + 2;
        let tag = &t[..tag_end];
        if t.len() >= 2 * tag.len() && t.ends_with(tag) {
            return Some(t[tag.len()..t.len() - tag.len()].to_string());
        }
    }
    None
}

/// Mã SQLSTATE của một tên điều kiện PL/pgSQL (phụ lục A của PostgreSQL), các tên thường gặp.
fn condition_sqlstate(cond: &str) -> Option<&'static str> {
    Some(match cond {
        "unique_violation" => "23505",
        "foreign_key_violation" => "23503",
        "not_null_violation" => "23502",
        "check_violation" => "23514",
        "restrict_violation" => "23001",
        "exclusion_violation" => "23P01",
        "division_by_zero" => "22012",
        "numeric_value_out_of_range" => "22003",
        "invalid_text_representation" => "22P02",
        "invalid_parameter_value" => "22023",
        "string_data_right_truncation" => "22001",
        "datetime_field_overflow" => "22008",
        "raise_exception" => "P0001",
        "no_data_found" => "P0002",
        "too_many_rows" => "P0003",
        "undefined_table" => "42P01",
        "undefined_column" => "42703",
        "undefined_function" => "42883",
        "duplicate_table" => "42P07",
        "duplicate_object" => "42710",
        "syntax_error" => "42601",
        "feature_not_supported" => "0A000",
        "insufficient_privilege" => "42501",
        "serialization_failure" => "40001",
        "deadlock_detected" => "40P01",
        "object_not_in_prerequisite_state" => "55000",
        _ => return None,
    })
}

/// Tên điều kiện → mã (cho `RAISE ... USING ERRCODE = 'unique_violation'`); mã 5 ký tự giữ nguyên.
pub(crate) fn errcode_of(code: &str) -> String {
    let c = code.trim().trim_matches('\'');
    if c.len() == 5 && c.chars().all(|ch| ch.is_ascii_alphanumeric()) {
        return c.to_ascii_uppercase();
    }
    condition_sqlstate(&c.to_ascii_lowercase()).unwrap_or("P0001").to_string()
}

/// `'Bảng % đã tồn tại'` -> chuỗi định dạng.
fn raise_message(s: &str) -> String {
    string_literal(s).unwrap_or_else(|| s.trim().to_string())
}

/// Lỗi `msg` có khớp điều kiện `WHEN cond` của PL/pgSQL không.
fn error_matches(cond: &str, msg: &str) -> bool {
    let m = msg.to_lowercase();
    // `WHEN SQLSTATE '22023'` và tên điều kiện có mã SQLSTATE: so theo mã của lỗi
    let state = crate::pg_types::sqlstate(msg);
    if let Some(code) = cond.strip_prefix("sqlstate").map(|c| c.trim().trim_matches('\'')) {
        return state.eq_ignore_ascii_case(code);
    }
    if let Some(code) = condition_sqlstate(cond) {
        if state == code {
            return true;
        }
    }
    match cond {
        "others" => true,
        "duplicate_object" | "duplicate_table" | "duplicate_column" | "duplicate_schema" | "duplicate_function" => m.contains("already exists"),
        "undefined_object" | "undefined_table" | "undefined_column" | "undefined_function" | "invalid_schema_name" => m.contains("does not exist") || m.contains("not found"),
        "unique_violation" => m.contains("duplicate key"),
        "foreign_key_violation" => m.contains("foreign key"),
        "not_null_violation" => m.contains("not-null"),
        "check_violation" => m.contains("check constraint"),
        "raise_exception" => m.starts_with("raise:") || m.starts_with("raise[p0001]"),
        "feature_not_supported" => m.contains("unsupported") || m.contains("not supported"),
        "invalid_text_representation" | "data_exception" => m.contains("invalid input syntax"),
        "no_data_found" => m.contains("query returned no rows"),
        "too_many_rows" => m.contains("query returned more than one row"),
        "division_by_zero" => m.contains("division by zero"),
        _ => false,
    }
}

/// Tách thân `$tag$ ... $tag$` (và `LANGUAGE plpgsql` trước/sau) của lệnh DO.
fn do_body(sql: &str) -> Result<String, String> {
    let rest = sql.trim().trim_end_matches(';').trim()[2..].trim();
    let rest = strip_language(rest);
    let lit = rest.trim();
    let start = lit.find('$').ok_or("DO requires a dollar-quoted body")?;
    let end = dollar_quote_end(lit, start).ok_or("unterminated dollar-quoted string in DO")?;
    let tail = strip_language(lit[end..].trim());
    if !tail.trim().is_empty() {
        return Err(format!("syntax error at end of DO statement: {}", tail));
    }
    let tag_end = lit[start + 1..].find('$').unwrap() + start + 2;
    let tag_len = tag_end - start;
    let lang_ok = !(lit[..start].trim().len() > 0 && !lit[..start].trim().to_lowercase().contains("plpgsql"));
    if !lang_ok {
        return Err("only LANGUAGE plpgsql is supported in DO".to_string());
    }
    Ok(lit[tag_end..end - tag_len].to_string())
}

fn strip_language(s: &str) -> &str {
    let t = s.trim();
    let up = t.to_uppercase();
    if up.starts_with("LANGUAGE ") {
        let after = t[9..].trim_start();
        let w = after.find(char::is_whitespace).unwrap_or(after.len());
        return &after[w..];
    }
    if let Some(p) = up.rfind(" LANGUAGE ") {
        if up[p + 10..].trim().split_whitespace().count() == 1 {
            return &t[..p];
        }
    }
    t
}

enum Flow {
    Continue,
    Return,
    /// EXIT: thoát vòng lặp gần nhất
    Exit,
    /// CONTINUE: sang lượt kế của vòng lặp gần nhất
    Next,
}

#[derive(Clone, Debug)]
enum VarVal {
    Scalar(Value),
    Record(Option<serde_json::Map<String, serde_json::Value>>),
}

#[derive(Clone, Debug)]
struct Var {
    val: VarVal,
    ty: String,
    /// đã từng được gán / khởi tạo (biến chưa gán trùng tên cột thì không thay, xem đầu module)
    set: bool,
}

/// Biến của các khối đang chạy (trong cùng trước).
#[derive(Default)]
pub(crate) struct Env {
    scopes: Vec<HashMap<String, Var>>,
    row_count: i64,
    /// giá trị của `RETURN biểu_thức` (hàm vô hướng) / bản ghi (`RETURN NEW`)
    pub(crate) ret: Option<Value>,
    pub(crate) ret_record: Option<Option<serde_json::Map<String, serde_json::Value>>>,
    /// các dòng của `RETURN NEXT` / `RETURN QUERY` (hàm SETOF / TABLE)
    pub(crate) ret_rows: Vec<serde_json::Value>,
    /// `FOUND`
    found: bool,
    /// giá trị cuối của các tham số OUT / INOUT (hàm có OUT không RETURN biểu thức)
    pub(crate) out_values: serde_json::Map<String, serde_json::Value>,
}

impl Env {
    fn get(&self, name: &str) -> Option<&Var> {
        self.scopes.iter().rev().find_map(|s| s.get(name))
    }
    fn get_mut(&mut self, name: &str) -> Option<&mut Var> {
        self.scopes.iter_mut().rev().find_map(|s| s.get_mut(name))
    }
}

fn literal(v: &Value) -> String {
    match v {
        Value::Null => "NULL".to_string(),
        Value::Int(i) => i.to_string(),
        Value::Float(f) => f.to_string(),
        Value::Bool(b) => (if *b { "TRUE" } else { "FALSE" }).to_string(),
        Value::Text(s) => format!("'{}'", s.replace('\'', "''")),
    }
}

/// Ép giá trị theo kiểu khai báo của biến (các kiểu vô hướng thường gặp).
fn coerce_to_decl(v: Value, ty: &str) -> Value {
    if v.is_null() {
        return v;
    }
    let base = ty.split(|c: char| c == '(' || c.is_whitespace()).next().unwrap_or("");
    match base {
        "int" | "integer" | "int4" | "smallint" | "int2" | "bigint" | "int8" | "numeric" | "decimal" | "real" | "float4" | "float8" | "double" | "boolean" | "bool" | "date" | "timestamp" | "timestamptz" => {
            super::executor::cast_val(v, &ty.to_uppercase())
        }
        "text" | "varchar" | "character" | "char" | "name" => Value::text(v.as_str()),
        _ => v,
    }
}

/// Tên cột của mọi bảng (viết thường): để không thay biến chưa gán trùng tên cột.
fn column_names(ex: &Executor) -> std::collections::HashSet<String> {
    ex.storage.tables.values().flat_map(|t| t.columns.iter().map(|c| c.name.to_lowercase())).collect()
}

thread_local! {
    /// Thân hàm PL/pgSQL đã parse, theo nội dung thân (gọi hàm nhiều lần không parse lại).
    static BODY_CACHE: std::cell::RefCell<HashMap<String, std::rc::Rc<Block>>> = std::cell::RefCell::new(HashMap::new());
}

fn parse_body(body: &str) -> Result<std::rc::Rc<Block>, String> {
    if let Some(b) = BODY_CACHE.with(|c| c.borrow().get(body).cloned()) {
        return Ok(b);
    }
    let mut p = Parser { s: body, pos: 0 };
    let block = p.parse_block()?;
    p.skip_ws();
    if p.pos < body.len() {
        return Err(format!("syntax error in function body near \"{}\"", snippet(&body[p.pos..])));
    }
    let rc = std::rc::Rc::new(block);
    BODY_CACHE.with(|c| {
        let mut c = c.borrow_mut();
        if c.len() > 512 {
            c.clear();
        }
        c.insert(body.to_string(), rc.clone());
    });
    Ok(rc)
}

impl Executor {
    /// Kiểm tra cú pháp thân PL/pgSQL (lúc CREATE FUNCTION).
    pub(crate) fn plpgsql_check(&mut self, body: &str) -> Result<(), String> {
        parse_body(body).map(|_| ())
    }

    /// Chạy thân hàm PL/pgSQL với các biến (tên, giá trị, kiểu) và bản ghi (NEW / OLD của trigger).
    pub(crate) fn run_plpgsql_body(&mut self, body: &str, vars: Vec<(String, Value, String)>, records: Vec<(String, Option<serde_json::Map<String, serde_json::Value>>)>, null_records: &[String]) -> Result<Env, String> {
        let block = parse_body(body)?;
        let mut env = Env::default();
        let mut scope = HashMap::new();
        for (n, v, ty) in vars {
            let set = true;
            scope.insert(n.to_lowercase(), Var { val: VarVal::Scalar(v), ty, set });
        }
        for (n, r) in records {
            // `set = false`: bản ghi NULL của trigger (RETURN nó = RETURN NULL)
            let set = !null_records.contains(&n);
            scope.insert(n, Var { val: VarVal::Record(r), ty: "record".into(), set });
        }
        env.scopes.push(scope);
        let flow = self.run_block(&mut env, &block, 0)?;
        let _ = flow;
        // tham số OUT: giá trị cuối
        let names: Vec<String> = match env.get("__out_params") {
            Some(Var { val: VarVal::Scalar(Value::Text(t)), .. }) => t.split(',').filter(|n| !n.is_empty()).map(|n| n.to_string()).collect(),
            _ => Vec::new(),
        };
        for n in names {
            if let Some(Var { val: VarVal::Scalar(v), .. }) = env.get(&n) {
                let j = super::executor::value_to_json_pub(v);
                env.out_values.insert(n, j);
            }
        }
        Ok(env)
    }

    /// Thay tham số của hàm SQL (theo tên, cả `tên_hàm.tham_số`) bằng hằng. Tên trùng cột của một bảng thì cột
    /// được ưu tiên như PostgreSQL (`set = false`).
    pub(crate) fn subst_routine_params(&self, sql: &str, fname: &str, named: &[(String, Value)], set: bool) -> Result<String, String> {
        if named.is_empty() {
            return Ok(sql.to_string());
        }
        // `tên_hàm.tham_số` luôn là tham số
        let mut s = sql.to_string();
        for (n, v) in named {
            let q = format!("{}.{}", fname, n);
            let mut out = String::new();
            let mut rest = s.as_str();
            while let Some(p) = rest.to_lowercase().find(&q) {
                out.push_str(&rest[..p]);
                out.push_str(&literal(v));
                rest = &rest[p + q.len()..];
            }
            out.push_str(rest);
            s = out;
        }
        let mut env = Env::default();
        let mut scope = HashMap::new();
        for (n, v) in named {
            scope.insert(n.to_lowercase(), Var { val: VarVal::Scalar(v.clone()), ty: String::new(), set });
        }
        env.scopes.push(scope);
        self.do_subst(&env, &s)
    }

    pub(crate) fn handle_do_block(&mut self, sql: &str) -> Result<QueryResult, String> {
        let body = do_body(sql)?;
        let mut p = Parser { s: &body, pos: 0 };
        let block = p.parse_block()?;
        p.skip_ws();
        if p.pos < body.len() {
            return Err(format!("syntax error in DO block near \"{}\"", snippet(&body[p.pos..])));
        }
        let implicit_tx = !self.storage.in_transaction;
        if implicit_tx {
            self.storage.begin_transaction();
        }
        let mut env = Env::default();
        let res = self.run_block(&mut env, &block, 0);
        if implicit_tx {
            match &res {
                Ok(_) => self.storage.commit()?,
                Err(_) => {
                    let _ = self.storage.rollback();
                }
            }
        }
        res.map(|_| QueryResult { rows: vec![], row_count: 0, fields: vec![], command: "DO".to_string() })
    }

    /// Thay biến (và `r.cột` của biến record) trong `sql` bằng literal. Bỏ qua chuỗi, tên trong ngoặc
    /// kép, `$$`, tên đứng sau `.` hoặc `AS`, và lời gọi hàm `tên(`.
    fn do_subst(&self, env: &Env, sql: &str) -> Result<String, String> {
        if env.scopes.iter().all(|s| s.is_empty()) {
            return Ok(sql.to_string());
        }
        let mut cols: Option<std::collections::HashSet<String>> = None;
        let b = sql.as_bytes();
        let mut out = String::with_capacity(sql.len());
        let mut i = 0;
        let mut last_word = String::new();
        while i < b.len() {
            let c = b[i];
            if c == b'\'' || c == b'"' {
                let q = c;
                let st = i;
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
                i = (i + 1).min(b.len());
                out.push_str(&sql[st..i]);
                last_word.clear();
                continue;
            }
            if c == b'$' {
                if let Some(end) = dollar_quote_end(sql, i) {
                    out.push_str(&sql[i..end]);
                    i = end;
                    continue;
                }
            }
            if c == b'-' && b.get(i + 1) == Some(&b'-') {
                let e = sql[i..].find('\n').map_or(b.len(), |e| i + e);
                out.push_str(&sql[i..e]);
                i = e;
                continue;
            }
            if (c.is_ascii_alphabetic() || c == b'_' || c >= 0x80) && (i == 0 || !is_ident_byte(b[i - 1]) && b[i - 1] != b'$') {
                let st = i;
                while i < b.len() && is_ident_byte(b[i]) {
                    i += 1;
                }
                let word = &sql[st..i];
                let lw = word.to_lowercase();
                let prev = sql[..st].trim_end();
                let after_dot = prev.ends_with('.');
                let after_as = last_word.eq_ignore_ascii_case("AS");
                let next = sql[i..].trim_start();
                let mut replaced = false;
                if !after_dot && !after_as && !next.starts_with('(') {
                    if let Some(var) = env.get(&lw) {
                        match &var.val {
                            VarVal::Record(rec) if next.starts_with('.') => {
                                let skip = sql[i..].len() - next.len() + 1;
                                let fst = i + skip;
                                let mut fe = fst;
                                while fe < b.len() && is_ident_byte(b[fe]) {
                                    fe += 1;
                                }
                                let field = sql[fst..fe].to_lowercase();
                                let rec = rec.as_ref().ok_or_else(|| format!("record \"{}\" is not assigned yet", lw))?;
                                let v = rec
                                    .iter()
                                    .find(|(k, _)| k.to_lowercase() == field)
                                    .map(|(_, v)| json_to_value(v))
                                    .ok_or_else(|| format!("record \"{}\" has no field \"{}\"", lw, field))?;
                                out.push_str(&literal(&v));
                                i = fe;
                                replaced = true;
                            }
                            VarVal::Scalar(v) if !next.starts_with('.') => {
                                let keep = !var.set && cols.get_or_insert_with(|| column_names(self)).contains(&lw);
                                if !keep {
                                    out.push_str(&literal(v));
                                    replaced = true;
                                }
                            }
                            _ => {}
                        }
                    }
                }
                if !replaced {
                    out.push_str(word);
                }
                last_word = word.to_string();
                continue;
            }
            if !c.is_ascii_whitespace() {
                last_word.clear();
            }
            let ch_len = sql[i..].chars().next().map_or(1, |ch| ch.len_utf8());
            out.push_str(&sql[i..i + ch_len]);
            i += ch_len;
        }
        Ok(out)
    }

    fn do_exec(&mut self, env: &mut Env, sql: &str, params: &[Value]) -> Result<QueryResult, String> {
        let up = sql.trim_start().to_uppercase();
        if up.starts_with("COMMIT") || up.starts_with("ROLLBACK") || up.starts_with("START TRANSACTION") {
            return Err("transaction control is not supported inside DO blocks".to_string());
        }
        let s = self.do_subst(env, sql)?;
        let r = self.do_run(&s, params)?;
        env.row_count = r.row_count as i64;
        Ok(r)
    }

    /// `execute_statement` kèm lỗi lúc tính biểu thức (chia cho 0...) của chính câu đó, để `EXCEPTION`
    /// bắt được ngay (bình thường lỗi này chỉ được lấy ra khi cả lệnh DO kết thúc).
    fn do_run(&mut self, sql: &str, params: &[Value]) -> Result<QueryResult, String> {
        let r = self.execute_statement(sql, params);
        if let Some(e) = crate::engine::errors::take() {
            return Err(e);
        }
        r
    }

    /// Giá trị của biểu thức (sau khi thay biến).
    fn do_eval(&mut self, env: &mut Env, expr: &str) -> Result<Value, String> {
        let s = self.do_subst(env, expr)?;
        let res = self.do_run(&format!("SELECT ({}) AS __v", s), &[])?;
        Ok(res.rows.first().and_then(|r| r.as_object().and_then(|o| o.values().next().map(json_to_value))).unwrap_or(Value::Null))
    }

    fn do_assign(&mut self, env: &mut Env, target: &str, v: Value) -> Result<(), String> {
        // `rec.cột := v` (NEW.cột trong trigger)
        if let (None, Some((rec, field))) = (env.get(target), target.split_once('.')) {
            let var = env.get_mut(rec).ok_or_else(|| format!("\"{}\" is not a known variable", target))?;
            let VarVal::Record(r) = &mut var.val else { return Err(format!("\"{}\" is not a record variable", rec)) };
            let map = r.get_or_insert_with(Default::default);
            let key = map.keys().find(|k| k.eq_ignore_ascii_case(field)).cloned().ok_or_else(|| format!("record \"{}\" has no field \"{}\"", rec, field))?;
            map.insert(key, super::executor::value_to_json_pub(&v));
            var.set = true;
            return Ok(());
        }
        let var = env.get_mut(target).ok_or_else(|| format!("\"{}\" is not a known variable", target))?;
        var.val = match &var.val {
            VarVal::Record(_) => return Err(format!("cannot assign a scalar to record variable \"{}\"", target)),
            VarVal::Scalar(_) => VarVal::Scalar(coerce_to_decl(v, &var.ty)),
        };
        var.set = true;
        Ok(())
    }

    /// Gán dòng đầu của kết quả cho các đích INTO (một biến record nhận cả dòng).
    fn do_assign_row(&mut self, env: &mut Env, targets: &[String], res: &QueryResult, strict: bool) -> Result<(), String> {
        if strict && res.rows.len() != 1 {
            return Err(if res.rows.is_empty() { "query returned no rows".to_string() } else { "query returned more than one row".to_string() });
        }
        let row = res.rows.first().and_then(|r| r.as_object()).cloned();
        if targets.len() == 1 {
            if let Some(var) = env.get_mut(&targets[0]) {
                if let VarVal::Record(_) = var.val {
                    var.val = VarVal::Record(row);
                    var.set = true;
                    return Ok(());
                }
            }
        }
        let vals: Vec<Value> = match &row {
            Some(o) => o.values().map(json_to_value).collect(),
            None => Vec::new(),
        };
        for (k, t) in targets.iter().enumerate() {
            self.do_assign(env, t, vals.get(k).cloned().unwrap_or(Value::Null))?;
        }
        Ok(())
    }

    fn run_block(&mut self, env: &mut Env, block: &Block, depth: usize) -> Result<Flow, String> {
        env.scopes.push(HashMap::new());
        let res = self.run_block_inner(env, block, depth);
        env.scopes.pop();
        res
    }

    fn run_block_inner(&mut self, env: &mut Env, block: &Block, depth: usize) -> Result<Flow, String> {
        for d in &block.decls {
            let is_record = d.ty == "record" || d.ty.ends_with("%rowtype");
            let val = if is_record { VarVal::Record(None) } else { VarVal::Scalar(Value::Null) };
            env.scopes.last_mut().unwrap().insert(d.name.clone(), Var { val, ty: d.ty.clone(), set: false });
            if let (Some(init), false) = (&d.init, is_record) {
                let v = self.do_eval(env, init)?;
                self.do_assign(env, &d.name, v)?;
            }
        }
        if block.handlers.is_empty() {
            return self.run_stmts(env, &block.body, depth);
        }
        // Khối có EXCEPTION: savepoint để huỷ thay đổi của khối khi có lỗi được bắt.
        let sp = format!("__pglite_do_{}", depth);
        self.storage.savepoint(&sp);
        match self.run_stmts(env, &block.body, depth) {
            Ok(f) => {
                self.storage.release_savepoint(&sp);
                Ok(f)
            }
            Err(e) => {
                let handler = block.handlers.iter().find(|(conds, _)| conds.iter().any(|c| error_matches(c, &e)));
                match handler {
                    Some((_, stmts)) => {
                        self.storage.rollback_to_savepoint(&sp);
                        self.storage.release_savepoint(&sp);
                        self.plan_cache.clear();
                        crate::engine::errors::clear();
                        let msg = e.strip_prefix("raise: ").unwrap_or(&e).to_string();
                        let mut scope = HashMap::new();
                        scope.insert("sqlstate".to_string(), Var { val: VarVal::Scalar(Value::text(crate::pg_types::sqlstate(&e))), ty: "text".into(), set: true });
                        scope.insert("sqlerrm".to_string(), Var { val: VarVal::Scalar(Value::text(msg)), ty: "text".into(), set: true });
                        env.scopes.push(scope);
                        let r = self.run_stmts(env, stmts, depth);
                        env.scopes.pop();
                        r
                    }
                    None => Err(e),
                }
            }
        }
    }

    /// Chạy thân vòng lặp một lượt: `Some(flow)` nếu phải dừng vòng lặp và trả `flow` ra ngoài.
    fn run_loop_body(&mut self, env: &mut Env, body: &[Stmt], depth: usize) -> Result<Option<Flow>, String> {
        match self.run_stmts(env, body, depth + 1)? {
            Flow::Return => Ok(Some(Flow::Return)),
            Flow::Exit => Ok(Some(Flow::Continue)),
            Flow::Next | Flow::Continue => Ok(None),
        }
    }

    fn run_stmts(&mut self, env: &mut Env, stmts: &[Stmt], depth: usize) -> Result<Flow, String> {
        if depth > 32 {
            return Err("DO block nesting is too deep".to_string());
        }
        const MAX_ITER: usize = 10_000_000;
        for st in stmts {
            match st {
                Stmt::Sql(sql) => {
                    self.do_exec(env, sql, &[])?;
                }
                Stmt::Assign { target, expr } => {
                    let v = self.do_eval(env, expr)?;
                    self.do_assign(env, target, v)?;
                }
                Stmt::SqlInto { sql, targets, strict, orig } => {
                    // đích không phải biến: `SELECT ... INTO bảng FROM ...` (tạo bảng) như trước
                    if !targets.iter().all(|t| env.get(t).is_some()) {
                        self.do_exec(env, orig, &[])?;
                        continue;
                    }
                    let res = self.do_exec(env, sql, &[])?;
                    self.do_assign_row(env, targets, &res, *strict)?;
                }
                Stmt::Execute { expr, into, strict, using } => {
                    let q = self.do_eval(env, expr)?;
                    if q.is_null() {
                        return Err("query string argument of EXECUTE is null".to_string());
                    }
                    let mut params = Vec::with_capacity(using.len());
                    for u in using {
                        params.push(self.do_eval(env, u)?);
                    }
                    let res = self.do_run(&q.as_str(), &params)?;
                    env.row_count = res.row_count as i64;
                    if !into.is_empty() {
                        self.do_assign_row(env, into, &res, *strict)?;
                    }
                }
                Stmt::If { branches, otherwise } => {
                    let mut taken = false;
                    for (cond, body) in branches {
                        if self.eval_do_condition(env, cond)? {
                            taken = true;
                            match self.run_stmts(env, body, depth + 1)? {
                                Flow::Continue => {}
                                f => return Ok(f),
                            }
                            break;
                        }
                    }
                    if !taken {
                        match self.run_stmts(env, otherwise, depth + 1)? {
                            Flow::Continue => {}
                            f => return Ok(f),
                        }
                    }
                }
                Stmt::Block(b) => match self.run_block(env, b, depth + 1)? {
                    Flow::Continue => {}
                    f => return Ok(f),
                },
                Stmt::Loop(body) => {
                    let mut n = 0;
                    loop {
                        n += 1;
                        if n > MAX_ITER {
                            return Err("LOOP exceeded the iteration limit in DO block".to_string());
                        }
                        if let Some(f) = self.run_loop_body(env, body, depth)? {
                            if let Flow::Return = f {
                                return Ok(Flow::Return);
                            }
                            break;
                        }
                    }
                }
                Stmt::While { cond, body } => {
                    let mut n = 0;
                    while self.eval_do_condition(env, cond)? {
                        n += 1;
                        if n > MAX_ITER {
                            return Err("WHILE exceeded the iteration limit in DO block".to_string());
                        }
                        if let Some(f) = self.run_loop_body(env, body, depth)? {
                            if let Flow::Return = f {
                                return Ok(Flow::Return);
                            }
                            break;
                        }
                    }
                }
                Stmt::ForRange { var, from, to, by, reverse, body } => {
                    let as_int = |v: Value, what: &str| v.as_i64().or_else(|| v.as_str().trim().parse().ok()).ok_or_else(|| format!("{} of integer FOR loop must be an integer", what));
                    let a = as_int(self.do_eval(env, from)?, "lower bound")?;
                    let z = as_int(self.do_eval(env, to)?, "upper bound")?;
                    let step = match by {
                        Some(b) => as_int(self.do_eval(env, b)?, "BY value")?,
                        None => 1,
                    };
                    if step <= 0 {
                        return Err("BY value of FOR loop must be greater than zero".to_string());
                    }
                    env.scopes.push(HashMap::new());
                    let mut i = a;
                    let mut flow = Flow::Continue;
                    while if *reverse { i >= z } else { i <= z } {
                        env.scopes.last_mut().unwrap().insert(var.clone(), Var { val: VarVal::Scalar(Value::Int(i)), ty: "integer".into(), set: true });
                        match self.run_loop_body(env, body, depth) {
                            Ok(None) => {}
                            Ok(Some(f)) => {
                                flow = f;
                                break;
                            }
                            Err(e) => {
                                env.scopes.pop();
                                return Err(e);
                            }
                        }
                        i = if *reverse { i - step } else { i + step };
                    }
                    env.scopes.pop();
                    if let Flow::Return = flow {
                        return Ok(Flow::Return);
                    }
                }
                Stmt::ForQuery { targets, query, dynamic, using, body } => {
                    let res = if *dynamic {
                        let q = self.do_eval(env, query)?;
                        let mut params = Vec::with_capacity(using.len());
                        for u in using {
                            params.push(self.do_eval(env, u)?);
                        }
                        self.do_run(&q.as_str(), &params)?
                    } else {
                        self.do_exec(env, query, &[])?
                    };
                    // biến vòng lặp chưa khai báo: tạo record cục bộ (PL/pgSQL đòi khai báo, ở đây dễ dãi hơn)
                    env.scopes.push(HashMap::new());
                    for t in targets {
                        if env.get(t).is_none() {
                            let val = if targets.len() == 1 { VarVal::Record(None) } else { VarVal::Scalar(Value::Null) };
                            env.scopes.last_mut().unwrap().insert(t.clone(), Var { val, ty: if targets.len() == 1 { "record".into() } else { String::new() }, set: true });
                        }
                    }
                    let mut flow = Flow::Continue;
                    for row in &res.rows {
                        let one = QueryResult { rows: vec![row.clone()], row_count: 1, fields: res.fields.clone(), command: String::new() };
                        let step = self.do_assign_row(env, targets, &one, false).and_then(|_| self.run_loop_body(env, body, depth));
                        match step {
                            Ok(None) => {}
                            Ok(Some(f)) => {
                                flow = f;
                                break;
                            }
                            Err(e) => {
                                env.scopes.pop();
                                return Err(e);
                            }
                        }
                    }
                    env.scopes.pop();
                    if let Flow::Return = flow {
                        return Ok(Flow::Return);
                    }
                }
                Stmt::ForEach { var, array, body } => {
                    let arr = self.do_eval(env, array)?;
                    if arr.is_null() {
                        return Err("FOREACH expression must not be null".to_string());
                    }
                    let items: Vec<Value> = match serde_json::from_str::<serde_json::Value>(&arr.as_str()) {
                        Ok(serde_json::Value::Array(a)) => a.iter().map(json_to_value).collect(),
                        _ => {
                            // dạng `{a,b}`
                            let t = arr.as_str();
                            let inner = t.trim().trim_start_matches('{').trim_end_matches('}');
                            if inner.is_empty() { Vec::new() } else { inner.split(',').map(|x| Value::text(x.trim().trim_matches('"'))).collect() }
                        }
                    };
                    let mut flow = Flow::Continue;
                    for it in items {
                        self.do_assign(env, var, it)?;
                        if let Some(f) = self.run_loop_body(env, body, depth)? {
                            flow = f;
                            break;
                        }
                    }
                    if let Flow::Return = flow {
                        return Ok(Flow::Return);
                    }
                }
                Stmt::Exit { when } => {
                    if when.as_ref().map_or(Ok(true), |c| self.eval_do_condition(env, c))? {
                        return Ok(Flow::Exit);
                    }
                }
                Stmt::Continue { when } => {
                    if when.as_ref().map_or(Ok(true), |c| self.eval_do_condition(env, c))? {
                        return Ok(Flow::Next);
                    }
                }
                Stmt::GetDiagnostics { target } => {
                    let n = env.row_count;
                    self.do_assign(env, target, Value::Int(n))?;
                }
                Stmt::Raise { level, message, args, errcode } => {
                    let mut vals = Vec::with_capacity(args.len());
                    for a in args {
                        vals.push(self.do_eval(env, a)?);
                    }
                    let mut text = String::new();
                    let mut it = vals.into_iter();
                    let mut chars = message.chars().peekable();
                    while let Some(c) = chars.next() {
                        if c == '%' {
                            if chars.peek() == Some(&'%') {
                                chars.next();
                                text.push('%');
                            } else {
                                match it.next() {
                                    Some(v) if v.is_null() => text.push_str("<NULL>"),
                                    Some(v) => text.push_str(&v.as_str()),
                                    None => return Err("too few parameters specified for RAISE".to_string()),
                                }
                            }
                        } else {
                            text.push(c);
                        }
                    }
                    if level == "EXCEPTION" {
                        return Err(match errcode {
                            Some(c) => format!("raise[{}]: {}", errcode_of(c), text),
                            None => format!("raise: {}", text),
                        });
                    }
                    eprintln!("[pglite-rs] {}: {}", level, text);
                }
                Stmt::Null => {}
                Stmt::Return(None) => return Ok(Flow::Return),
                Stmt::Return(Some(e)) => {
                    // RETURN NEW / RETURN OLD / RETURN r: bản ghi; NULL trong trigger = bỏ dòng
                    let name = e.trim().to_lowercase();
                    if let Some(Var { val: VarVal::Record(r), set, .. }) = env.get(&name) {
                        env.ret_record = Some(if *set { r.clone() } else { None });
                    } else if name == "null" {
                        env.ret_record = Some(None);
                        env.ret = Some(Value::Null);
                    } else {
                        let v = self.do_eval(env, e)?;
                        env.ret = Some(v);
                    }
                    return Ok(Flow::Return);
                }
                Stmt::ReturnNext(e) => {
                    let row = match e {
                        Some(e) => {
                            let name = e.trim().to_lowercase();
                            match env.get(&name) {
                                Some(Var { val: VarVal::Record(Some(r)), .. }) => serde_json::Value::Object(r.clone()),
                                _ => {
                                    let v = self.do_eval(env, e)?;
                                    serde_json::json!({ "__v": super::executor::value_to_json_pub(&v) })
                                }
                            }
                        }
                        // RETURN NEXT không biểu thức: giá trị hiện tại của các tham số OUT
                        None => {
                            let mut m = serde_json::Map::new();
                            if let Some(outs) = env.get("__out_params") {
                                if let VarVal::Scalar(Value::Text(names)) = &outs.val {
                                    for n in names.split(',').filter(|n| !n.is_empty()) {
                                        if let Some(Var { val: VarVal::Scalar(v), .. }) = env.get(n) {
                                            m.insert(n.to_string(), super::executor::value_to_json_pub(v));
                                        }
                                    }
                                }
                            }
                            serde_json::Value::Object(m)
                        }
                    };
                    env.ret_rows.push(row);
                }
                Stmt::ReturnQuery { sql, dynamic, using } => {
                    let res = if *dynamic {
                        let q = self.do_eval(env, sql)?;
                        let mut params = Vec::with_capacity(using.len());
                        for u in using {
                            params.push(self.do_eval(env, u)?);
                        }
                        self.do_run(&q.as_str(), &params)?
                    } else {
                        self.do_exec(env, sql, &[])?
                    };
                    env.ret_rows.extend(res.rows.into_iter());
                }
            }
        }
        Ok(Flow::Continue)
    }

    fn eval_do_condition(&mut self, env: &mut Env, cond: &str) -> Result<bool, String> {
        let s = self.do_subst(env, cond)?;
        let res = self.do_run(&format!("SELECT ({}) AS __cond", s), &[])?;
        let v = res.rows.first().and_then(|r| r.as_object().and_then(|o| o.values().next().cloned())).unwrap_or(serde_json::Value::Null);
        Ok(match v {
            serde_json::Value::Bool(b) => b,
            serde_json::Value::Number(n) => n.as_f64().map_or(false, |f| f != 0.0),
            serde_json::Value::String(s) => matches!(s.to_lowercase().as_str(), "t" | "true" | "1" | "yes"),
            _ => false,
        })
    }
}

/// Giá trị mặc định cho SHOW (giống PostgreSQL mới cài).
fn default_setting(name: &str) -> Option<&'static str> {
    Some(match name {
        "search_path" => "\"$user\", public",
        "timezone" => "UTC",
        "client_encoding" | "server_encoding" => "UTF8",
        "server_version" => "16.0",
        "server_version_num" => "160000",
        "standard_conforming_strings" => "on",
        "datestyle" => "ISO, MDY",
        "intervalstyle" => "postgres",
        "transaction_isolation" | "default_transaction_isolation" => "read committed",
        "application_name" => "",
        "integer_datetimes" => "on",
        "max_identifier_length" => "63",
        "lc_messages" | "lc_collate" | "lc_ctype" => "C",
        "statement_timeout" | "lock_timeout" | "idle_in_transaction_session_timeout" => "0",
        _ => return None,
    })
}

impl Executor {
    /// Giá trị hiện tại của tham số `name` (viết thường) như SHOW / `current_setting`.
    pub(crate) fn setting_value(&self, name: &str) -> Option<String> {
        let name = if name == "time zone" { "timezone" } else { name };
        self.settings.get(name).cloned().or_else(|| {
            if name == "timezone" { Some(crate::engine::executor::default_tz().to_string()) } else { default_setting(name).map(|v| v.to_string()) }
        })
    }

    /// `current_setting('name' [, missing_ok])` và `set_config('name', 'value', is_local)` với đối số là
    /// literal: thay bằng giá trị trước khi chạy câu lệnh (trước đây trả nguyên văn câu chữ). `None` nếu không
    /// đổi gì.
    pub(crate) fn resolve_setting_calls(&mut self, sql: &str) -> Result<Option<String>, String> {
        let mut out = sql.to_string();
        let mut changed = false;
        let mut from = 0;
        loop {
            let b = out.as_bytes();
            let mut found: Option<(usize, usize, bool)> = None;
            let mut in_sq = false;
            let mut i = from;
            while i < b.len() {
                if b[i] == b'\'' {
                    in_sq = !in_sq;
                } else if !in_sq && (i == 0 || !(b[i - 1].is_ascii_alphanumeric() || b[i - 1] == b'_')) {
                    for (kw, is_set) in [("CURRENT_SETTING(", false), ("SET_CONFIG(", true)] {
                        if b.len() >= i + kw.len() && b[i..i + kw.len()].eq_ignore_ascii_case(kw.as_bytes()) {
                            found = Some((i, i + kw.len() - 1, is_set));
                            break;
                        }
                    }
                    if found.is_some() {
                        break;
                    }
                }
                i += 1;
            }
            let Some((start, open, is_set)) = found else { break };
            let Some(close) = crate::engine::constraints::matching_paren(&out, open) else { break };
            let args: Vec<String> = crate::engine::executor::split_function_args(&out[open + 1..close]).into_iter().map(|a| a.trim().to_string()).collect();
            let lits: Option<Vec<String>> = args.iter().map(|a| string_literal(a).or_else(|| matches!(a.to_lowercase().as_str(), "true" | "false").then(|| a.to_lowercase()))).collect();
            let Some(lits) = lits else {
                from = close;
                continue;
            };
            let name = lits.first().map(|n| n.to_lowercase()).unwrap_or_default();
            let value = if is_set {
                if lits.len() != 3 {
                    from = close;
                    continue;
                }
                let v = lits[1].clone();
                if name == "timezone" || name == "time zone" {
                    crate::engine::datetime::set_session_tz(&v);
                    self.settings.insert("timezone".to_string(), v.clone());
                } else {
                    self.settings.insert(name.clone(), v.clone());
                }
                Some(v)
            } else {
                let missing_ok = lits.get(1).map_or(false, |m| m == "true");
                match self.setting_value(&name) {
                    Some(v) => Some(v),
                    None if missing_ok => None,
                    None => return Err(format!("unrecognized configuration parameter \"{}\"", lits[0])),
                }
            };
            let lit = match value {
                Some(v) => format!("'{}'", v.replace('\'', "''")),
                None => "NULL".to_string(),
            };
            // cả mục SELECT không alias: tên cột như PostgreSQL
            let before = out[..start].trim_end().to_uppercase();
            let after = out[close + 1..].trim_start().to_uppercase();
            let whole_item = (before.ends_with(',') || before.ends_with("SELECT")) && (after.is_empty() || after.starts_with(',') || after.starts_with("FROM "));
            let lit = if whole_item { format!("{} AS \"{}\"", lit, if is_set { "set_config" } else { "current_setting" }) } else { lit };
            out = format!("{}{}{}", &out[..start], lit, &out[close + 1..]);
            from = start + lit.len();
            changed = true;
        }
        Ok(changed.then_some(out))
    }

    /// Hàm / từ khoá thông tin phiên trong câu DML: `version()`, `current_database()`, `current_catalog`,
    /// `current_schema[()]`, `current_schemas(bool)`, `current_user`, `session_user`, `current_role`.
    /// Thay bằng literal trước khi chạy (trước đây trả nguyên văn câu chữ). `None` nếu không đổi gì.
    pub(crate) fn resolve_session_info(&self, sql: &str) -> Option<String> {
        const NAMES: [&str; 8] = ["VERSION", "CURRENT_DATABASE", "CURRENT_CATALOG", "CURRENT_SCHEMAS", "CURRENT_SCHEMA", "CURRENT_USER", "SESSION_USER", "CURRENT_ROLE"];
        let up = sql.trim_start().to_uppercase();
        if !["SELECT", "INSERT", "UPDATE", "DELETE", "WITH", "VALUES"].iter().any(|k| up.starts_with(k)) {
            return None;
        }
        let b = sql.as_bytes();
        let mut out = String::with_capacity(sql.len());
        let mut last = 0;
        let mut changed = false;
        let mut in_sq = false;
        let mut in_dq = false;
        let mut i = 0;
        while i < b.len() {
            let c = b[i];
            if c == b'\'' && !in_dq {
                in_sq = !in_sq;
            } else if c == b'"' && !in_sq {
                in_dq = !in_dq;
            } else if !in_sq && !in_dq && c.is_ascii_alphabetic() && (i == 0 || !(b[i - 1].is_ascii_alphanumeric() || b[i - 1] == b'_' || b[i - 1] == b'.' || b[i - 1] >= 0x80)) {
                let seg = &sql[i..];
                let hit = |n: &&&str| b.len() >= i + n.len() && b[i..i + n.len()].eq_ignore_ascii_case(n.as_bytes()) && b.get(i + n.len()).map_or(true, |x| !(x.is_ascii_alphanumeric() || *x == b'_'));
                if let Some(name) = NAMES.iter().find(hit) {
                    let after = seg[name.len()..].trim_start();
                    let ws = seg[name.len()..].len() - after.len();
                    let (value, consumed) = match *name {
                        "VERSION" | "CURRENT_DATABASE" if after.starts_with('(') && after[1..].trim_start().starts_with(')') => {
                            let close = name.len() + ws + after.find(')').unwrap() + 1;
                            let v = if *name == "VERSION" { format!("PostgreSQL {} (pglite-rs)", self.setting_value("server_version").unwrap_or_default()) } else { "postgres".to_string() };
                            (v, close)
                        }
                        "CURRENT_SCHEMAS" if after.starts_with('(') => {
                            let Some(cl) = after.find(')') else { i += 1; continue };
                            let arg = after[1..cl].trim().to_lowercase();
                            let v = if arg == "true" { "{pg_catalog,public}" } else { "{public}" };
                            (v.to_string(), name.len() + ws + cl + 1)
                        }
                        "CURRENT_SCHEMA" => {
                            if after.starts_with('(') && after[1..].trim_start().starts_with(')') {
                                ("public".to_string(), name.len() + ws + after.find(')').unwrap() + 1)
                            } else if after.starts_with('(') {
                                i += 1;
                                continue;
                            } else {
                                ("public".to_string(), name.len())
                            }
                        }
                        "CURRENT_CATALOG" if !after.starts_with('(') => ("postgres".to_string(), name.len()),
                        "CURRENT_USER" | "SESSION_USER" | "CURRENT_ROLE" if !after.starts_with('(') => ("postgres".to_string(), name.len()),
                        _ => {
                            i += 1;
                            continue;
                        }
                    };
                    // cả mục SELECT không alias: tên cột như PostgreSQL
                    let before = sql[..i].trim_end().to_uppercase();
                    let tail = sql[i + consumed..].trim_start().to_uppercase();
                    let whole_item = (before.ends_with(',') || before.ends_with("SELECT")) && (tail.is_empty() || tail.starts_with(',') || tail.starts_with("FROM ") || tail.starts_with("FROM\n"));
                    let lit = format!("'{}'", value.replace('\'', "''"));
                    out.push_str(&sql[last..i]);
                    if whole_item {
                        out.push_str(&format!("{} AS \"{}\"", lit, name.to_lowercase()));
                    } else {
                        out.push_str(&lit);
                    }
                    i += consumed;
                    last = i;
                    changed = true;
                    continue;
                }
            }
            i += 1;
        }
        out.push_str(&sql[last..]);
        changed.then_some(out)
    }

    /// CREATE/DROP EXTENSION, SET, SHOW, RESET, DISCARD... `None` nếu không phải các lệnh này.
    pub(crate) fn handle_session_statement(&mut self, sql: &str, upper: &str) -> Option<Result<QueryResult, String>> {
        let ok = |cmd: &str| Some(Ok(QueryResult { rows: vec![], row_count: 0, fields: vec![], command: cmd.to_string() }));
        if upper.starts_with("CREATE EXTENSION") {
            let rest = sql[16..].trim();
            let rest = if rest.to_uppercase().starts_with("IF NOT EXISTS") { rest[13..].trim() } else { rest };
            let name = rest.split_whitespace().next().unwrap_or("").trim_matches('"').to_lowercase();
            // Các hàm của những extension này đã có sẵn trong engine (gen_random_uuid, uuid_generate_v4...)
            return if ["pgcrypto", "uuid-ossp", "plpgsql", "citext", "pg_trgm", "btree_gin", "btree_gist", "unaccent", "hstore"].contains(&name.as_str()) {
                ok("CREATE EXTENSION")
            } else {
                Some(Err(format!("extension \"{}\" is not available", name)))
            };
        }
        if upper.starts_with("DROP EXTENSION") || upper.starts_with("COMMENT ON EXTENSION") || upper.starts_with("DISCARD") || upper.starts_with("LISTEN") || upper.starts_with("UNLISTEN") || upper.starts_with("NOTIFY") {
            return ok(upper.split_whitespace().next().unwrap_or(""));
        }
        if upper.starts_with("SET ") {
            let mut rest = sql[4..].trim();
            for kw in ["SESSION ", "LOCAL "] {
                if rest.to_uppercase().starts_with(kw) {
                    rest = rest[kw.len()..].trim();
                }
            }
            let up = rest.to_uppercase();
            if up.starts_with("TRANSACTION") || up.starts_with("CHARACTERISTICS") || up.starts_with("CONSTRAINTS") || up.starts_with("ROLE") || up.starts_with("SESSION AUTHORIZATION") {
                return ok("SET");
            }
            let (name, value) = if up.starts_with("TIME ZONE") {
                ("timezone".to_string(), rest[9..].trim().to_string())
            } else if up.starts_with("NAMES") {
                ("client_encoding".to_string(), rest[5..].trim().to_string())
            } else {
                let split = rest.find(|c: char| c == '=' || c.is_whitespace()).unwrap_or(rest.len());
                let name = rest[..split].trim().to_lowercase();
                let mut v = rest[split..].trim();
                if v.starts_with('=') {
                    v = v[1..].trim();
                } else if v.to_uppercase().starts_with("TO ") {
                    v = v[3..].trim();
                }
                (name, v.to_string())
            };
            let mut value = value.split(',').map(|p| p.trim().trim_matches('\'').to_string()).collect::<Vec<_>>().join(", ");
            if name == "timezone" {
                let raw = value.trim().to_string();
                if raw.eq_ignore_ascii_case("LOCAL") || raw.eq_ignore_ascii_case("DEFAULT") {
                    self.settings.remove("timezone");
                    return ok("SET");
                }
                // số (`SET TIME ZONE 7`) hoặc độ lệch ISO (`'+07'`, `'-05:30'`): giờ phía đông UTC như PostgreSQL
                // (khác `AT TIME ZONE '+07'`, nơi chuỗi được hiểu theo POSIX)
                let iso = raw.strip_prefix('+').or_else(|| raw.strip_prefix('-')).is_some_and(|b| {
                    let (h, m) = b.split_once(':').unwrap_or((b, "0"));
                    !h.is_empty() && h.len() <= 2 && h.bytes().all(|c| c.is_ascii_digit()) && m.bytes().all(|c| c.is_ascii_digit())
                });
                if iso {
                    let neg = raw.starts_with('-');
                    let (h, m) = raw[1..].split_once(':').unwrap_or((&raw[1..], "0"));
                    let secs = (h.parse::<i64>().unwrap_or(0) * 3600 + m.parse::<i64>().unwrap_or(0) * 60) * if neg { -1 } else { 1 };
                    value = format!("UTC{}{:02}:{:02}", if secs >= 0 { '-' } else { '+' }, secs.abs() / 3600, (secs.abs() % 3600) / 60);
                } else if !rest.contains('\'') {
                    if let Ok(h) = raw.parse::<f64>() {
                        let secs = (h * 3600.0).round() as i64;
                        let (hh, mm) = (secs.abs() / 3600, (secs.abs() % 3600) / 60);
                        value = format!("UTC{}{:02}:{:02}", if secs >= 0 { '-' } else { '+' }, hh, mm);
                    }
                }
                if !crate::engine::datetime::valid_zone(&value) {
                    return Some(Err(format!("invalid value for parameter \"TimeZone\": \"{}\"", raw)));
                }
            }
            if value.eq_ignore_ascii_case("DEFAULT") {
                self.settings.remove(&name);
            } else {
                self.settings.insert(name, value);
            }
            return ok("SET");
        }
        if upper.starts_with("RESET ") {
            let name = sql[6..].trim().to_lowercase();
            if name == "all" {
                self.settings.clear();
            } else {
                self.settings.remove(&name);
            }
            return ok("RESET");
        }
        if upper.starts_with("SHOW ") {
            let name = sql[5..].trim().trim_matches('"').to_lowercase();
            let name = if name == "time zone" { "timezone".to_string() } else { name };
            if name == "all" {
                return Some(Err("SHOW ALL is not supported".to_string()));
            }
            let value = self.setting_value(&name);
            return Some(match value {
                Some(v) => Ok(QueryResult {
                    rows: vec![serde_json::json!({ name.clone(): v })],
                    row_count: 1,
                    fields: vec![crate::types::FieldInfo { name, data_type: "text".to_string() }],
                    command: "SHOW".to_string(),
                }),
                None => Err(format!("unrecognized configuration parameter \"{}\"", name)),
            });
        }
        None
    }
}
