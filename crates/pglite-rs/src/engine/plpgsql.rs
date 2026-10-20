//! `DO $$ ... $$`: bộ con PL/pgSQL đủ cho script migration.
//!
//! Hỗ trợ: `[DECLARE]` (bỏ qua khai báo không dùng), khối `BEGIN ... [EXCEPTION WHEN ... THEN ...] END`
//! lồng nhau, `IF ... THEN ... [ELSIF ...] [ELSE ...] END IF`, `PERFORM`, `RAISE NOTICE|INFO|WARNING|
//! EXCEPTION`, `NULL`, `RETURN`, `EXECUTE '...'`, và câu SQL thường. Điều kiện (`IF`) được tính bằng
//! chính engine: `SELECT (<điều kiện>)`.
//!
//! Cả khối chạy như một đơn vị: ngoài giao dịch thì được bọc trong giao dịch ngầm (lỗi -> ROLLBACK).
//! `EXCEPTION` dùng savepoint như PostgreSQL (thay đổi dữ liệu của khối lỗi bị huỷ).
use super::executor::{dollar_quote_end, Executor};
use crate::types::QueryResult;

#[derive(Debug)]
enum Stmt {
    Sql(String),
    If { branches: Vec<(String, Vec<Stmt>)>, otherwise: Vec<Stmt> },
    Block(Block),
    Raise { level: String, message: String },
    Null,
    Return,
}

#[derive(Debug)]
struct Block {
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

    fn parse_block(&mut self) -> Result<Block, String> {
        if self.eat_kw("DECLARE") {
            // Khai báo biến: chỉ cho phép bỏ qua (không có biến nào được dùng trong bộ con này)
            while !self.peek_kw("BEGIN") && self.pos < self.s.len() {
                let decl = self.read_until(&["BEGIN"]);
                if decl.is_empty() && !self.s[self.pos..].starts_with(';') {
                    break;
                }
                self.eat_semicolon();
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
        self.skip_ws();
        self.eat_semicolon();
        Ok(Block { body, handlers })
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
            if self.eat_kw("RAISE") {
                let rest = self.read_until(&[]);
                self.eat_semicolon();
                let mut words = rest.splitn(2, char::is_whitespace);
                let first = words.next().unwrap_or("").to_uppercase();
                let (level, msg) = if ["NOTICE", "INFO", "WARNING", "LOG", "DEBUG", "EXCEPTION"].contains(&first.as_str()) {
                    (first, words.next().unwrap_or("").trim().to_string())
                } else {
                    ("EXCEPTION".to_string(), rest.clone())
                };
                out.push(Stmt::Raise { level, message: raise_message(&msg) });
                continue;
            }
            if self.eat_kw("NULL") {
                self.eat_semicolon();
                out.push(Stmt::Null);
                continue;
            }
            if self.eat_kw("RETURN") {
                let _ = self.read_until(&[]);
                self.eat_semicolon();
                out.push(Stmt::Return);
                continue;
            }
            if self.eat_kw("PERFORM") {
                let expr = self.read_until(&[]);
                self.eat_semicolon();
                out.push(Stmt::Sql(format!("SELECT {}", expr)));
                continue;
            }
            if self.eat_kw("EXECUTE") {
                let expr = self.read_until(&[]);
                self.eat_semicolon();
                let lit = string_literal(&expr).ok_or_else(|| format!("EXECUTE in DO block only supports a string literal: {}", expr))?;
                out.push(Stmt::Sql(lit));
                continue;
            }
            let sql = self.read_until(&[]);
            self.eat_semicolon();
            if sql.contains(":=") && !sql.to_uppercase().starts_with("SELECT") {
                return Err(format!("variable assignment is not supported in DO blocks: {}", sql));
            }
            if !sql.is_empty() {
                out.push(Stmt::Sql(sql));
            }
        }
    }
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

/// `'Bảng % đã tồn tại', x` -> chỉ lấy chuỗi định dạng.
fn raise_message(s: &str) -> String {
    let t = s.trim();
    if t.starts_with('\'') {
        let b = t.as_bytes();
        let mut i = 1;
        while i < b.len() {
            if b[i] == b'\'' {
                if b.get(i + 1) == Some(&b'\'') {
                    i += 2;
                    continue;
                }
                return t[1..i].replace("''", "'");
            }
            i += 1;
        }
    }
    t.to_string()
}

/// Lỗi `msg` có khớp điều kiện `WHEN cond` của PL/pgSQL không.
fn error_matches(cond: &str, msg: &str) -> bool {
    let m = msg.to_lowercase();
    match cond {
        "others" => true,
        "duplicate_object" | "duplicate_table" | "duplicate_column" | "duplicate_schema" | "duplicate_function" => m.contains("already exists"),
        "undefined_object" | "undefined_table" | "undefined_column" | "undefined_function" | "invalid_schema_name" => m.contains("does not exist") || m.contains("not found"),
        "unique_violation" => m.contains("duplicate key"),
        "foreign_key_violation" => m.contains("foreign key"),
        "not_null_violation" => m.contains("not-null"),
        "check_violation" => m.contains("check constraint"),
        "raise_exception" => m.starts_with("raise:"),
        "feature_not_supported" => m.contains("unsupported") || m.contains("not supported"),
        "invalid_text_representation" | "data_exception" => m.contains("invalid input syntax"),
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
}

impl Executor {
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
        let res = self.run_block(&block, 0);
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

    fn run_block(&mut self, block: &Block, depth: usize) -> Result<Flow, String> {
        if block.handlers.is_empty() {
            return self.run_stmts(&block.body, depth);
        }
        // Khối có EXCEPTION: savepoint để huỷ thay đổi của khối khi có lỗi được bắt.
        let sp = format!("__pglite_do_{}", depth);
        self.storage.savepoint(&sp);
        match self.run_stmts(&block.body, depth) {
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
                        self.run_stmts(stmts, depth)
                    }
                    None => Err(e),
                }
            }
        }
    }

    fn run_stmts(&mut self, stmts: &[Stmt], depth: usize) -> Result<Flow, String> {
        if depth > 32 {
            return Err("DO block nesting is too deep".to_string());
        }
        for st in stmts {
            match st {
                Stmt::Sql(sql) => {
                    let up = sql.trim_start().to_uppercase();
                    if up.starts_with("COMMIT") || up.starts_with("ROLLBACK") || up.starts_with("START TRANSACTION") {
                        return Err("transaction control is not supported inside DO blocks".to_string());
                    }
                    self.execute_statement(sql, &[])?;
                }
                Stmt::If { branches, otherwise } => {
                    let mut taken = false;
                    for (cond, body) in branches {
                        if self.eval_do_condition(cond)? {
                            taken = true;
                            if let Flow::Return = self.run_stmts(body, depth + 1)? {
                                return Ok(Flow::Return);
                            }
                            break;
                        }
                    }
                    if !taken {
                        if let Flow::Return = self.run_stmts(otherwise, depth + 1)? {
                            return Ok(Flow::Return);
                        }
                    }
                }
                Stmt::Block(b) => {
                    if let Flow::Return = self.run_block(b, depth + 1)? {
                        return Ok(Flow::Return);
                    }
                }
                Stmt::Raise { level, message } => {
                    if level == "EXCEPTION" {
                        return Err(format!("raise: {}", message));
                    }
                    eprintln!("[pglite-rs] {}: {}", level, message);
                }
                Stmt::Null => {}
                Stmt::Return => return Ok(Flow::Return),
            }
        }
        Ok(Flow::Continue)
    }

    fn eval_do_condition(&mut self, cond: &str) -> Result<bool, String> {
        let res = self.execute_statement(&format!("SELECT ({}) AS __cond", cond), &[])?;
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
            let value = self.settings.get(&name).cloned().or_else(|| {
                if name == "timezone" { Some(crate::engine::executor::default_tz().to_string()) } else { default_setting(&name).map(|v| v.to_string()) }
            });
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
