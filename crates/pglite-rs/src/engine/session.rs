//! Trạng thái phiên của các lệnh tiện ích (đợt 13): cursor `DECLARE / FETCH / MOVE / CLOSE` (+ `WHERE CURRENT OF`),
//! giao dịch `READ ONLY`, `SET LOCAL`, `LISTEN / UNLISTEN / NOTIFY / pg_notify`, `LOCK TABLE`, `SHOW ALL`,
//! `PREPARE TRANSACTION`, và vùng đệm của `COPY ... FROM STDIN / TO STDOUT` (engine/copy.rs).
//!
//! Mọi thứ ở đây chỉ sống trong bộ nhớ của phiên, không ghi gì xuống WAL.
use super::executor::{split_comma_separated_tokens, strip_schema_keep_case, Executor};
use crate::types::{FieldInfo, QueryResult, Value};
use std::collections::{BTreeSet, HashMap};

/// Cursor đã materialize kết quả lúc DECLARE (như cursor `INSENSITIVE` của PG).
struct Cursor {
    res: QueryResult,
    /// 0 = trước dòng đầu, k = đang ở dòng k (1-based), len + 1 = sau dòng cuối.
    pos: usize,
    hold: bool,
    /// Bảng nguồn nếu truy vấn là `SELECT ... FROM một_bảng` (cho `WHERE CURRENT OF`).
    table: Option<String>,
}

#[derive(Default)]
pub struct SessionState {
    /// Dữ liệu chờ cho `COPY ... FROM STDIN` (đưa vào bởi `PGlite::copy_from`).
    pub(crate) copy_in: Option<String>,
    /// Kết quả của `COPY ... TO STDOUT` gần nhất (lấy bởi `PGlite::copy_to`).
    pub(crate) copy_out: Option<String>,
    cursors: HashMap<String, Cursor>,
    /// Giao dịch hiện tại là READ ONLY.
    pub(crate) read_only: bool,
    /// `SET LOCAL`: giá trị trước đó của các tham số, trả lại khi giao dịch kết thúc.
    local_saved: Vec<(String, Option<String>)>,
    /// NOTIFY trong giao dịch: chỉ gửi khi COMMIT.
    pending_notify: Vec<(String, String)>,
    /// Thông báo đã gửi tới kênh phiên đang LISTEN, chờ ứng dụng lấy (`PGlite::take_notifications`).
    pub(crate) notifications: Vec<(String, String)>,
    listening: BTreeSet<String>,
}

impl SessionState {
    pub(crate) fn has_cursors(&self) -> bool {
        !self.cursors.is_empty()
    }
}

fn ok(cmd: &str) -> Option<Result<QueryResult, String>> {
    Some(Ok(QueryResult { rows: vec![], row_count: 0, fields: vec![], command: cmd.to_string() }))
}

fn ident(s: &str) -> String {
    let t = s.trim();
    if t.len() >= 2 && t.starts_with('"') && t.ends_with('"') {
        t[1..t.len() - 1].to_string()
    } else {
        t.to_lowercase()
    }
}

fn str_lit(s: &str) -> Option<String> {
    let t = s.trim();
    (t.len() >= 2 && t.starts_with('\'') && t.ends_with('\'')).then(|| t[1..t.len() - 1].replace("''", "'"))
}

/// Tên canonical của tham số như PG in ở cột SHOW (`timezone` → `TimeZone`).
pub(crate) fn canonical_setting_name(name: &str) -> Option<&'static str> {
    super::catalog_views::setting_row(name).and_then(|r| r[0].as_deref())
}

/// Giá trị mặc định của PG cho tham số không được engine đặt riêng.
pub(crate) fn pg_default_setting(name: &str) -> Option<String> {
    super::catalog_views::setting_row(name).map(|r| super::catalog_views::show_value(r))
}

/// Lệnh ghi bị chặn trong giao dịch READ ONLY: tên lệnh như thông báo của PG, `None` = lệnh đọc.
fn write_command_name(upper: &str) -> Option<String> {
    let w: Vec<&str> = upper.split_whitespace().take(4).collect();
    let first = *w.first()?;
    Some(match first {
        "INSERT" | "UPDATE" | "DELETE" | "MERGE" => first.to_string(),
        "TRUNCATE" => "TRUNCATE TABLE".to_string(),
        "COPY" if upper.contains(" FROM ") => "COPY FROM".to_string(),
        "CREATE" | "DROP" | "ALTER" => {
            let mut i = 1;
            while i < w.len() && matches!(w[i], "OR" | "REPLACE" | "UNIQUE" | "TEMP" | "TEMPORARY" | "UNLOGGED" | "MATERIALIZED") {
                i += 1;
            }
            match w.get(i) {
                Some(obj) => format!("{} {}", first, obj.trim_end_matches('(')),
                None => first.to_string(),
            }
        }
        "COMMENT" => "COMMENT".to_string(),
        "REINDEX" | "CLUSTER" | "REFRESH" => first.to_string(),
        _ => return None,
    })
}

impl Executor {
    /// Các lệnh của module này; `None` = không phải lệnh ở đây.
    pub(crate) fn handle_session_extra(&mut self, sql: &str, upper: &str, params: &[Value]) -> Option<Result<QueryResult, String>> {
        let first = upper.split(|c: char| c.is_whitespace() || c == '(').next().unwrap_or("");
        match first {
            "DECLARE" => Some(self.declare_cursor(sql, upper, params)),
            "FETCH" | "MOVE" => Some(self.fetch_cursor(sql, first == "MOVE")),
            "CLOSE" => {
                let name = sql[5..].trim();
                if name.eq_ignore_ascii_case("ALL") {
                    self.session.cursors.clear();
                } else if self.session.cursors.remove(&ident(name)).is_none() {
                    return Some(Err(format!("cursor \"{}\" does not exist", ident(name))));
                }
                ok("CLOSE CURSOR")
            }
            "LISTEN" => {
                self.session.listening.insert(ident(&sql[6..]));
                ok("LISTEN")
            }
            "UNLISTEN" => {
                let ch = sql[8..].trim();
                if ch == "*" {
                    self.session.listening.clear();
                } else {
                    self.session.listening.remove(&ident(ch));
                }
                ok("UNLISTEN")
            }
            "NOTIFY" => {
                let rest = sql[6..].trim();
                let (ch, payload) = match rest.find(',') {
                    Some(p) => (&rest[..p], str_lit(&rest[p + 1..]).unwrap_or_default()),
                    None => (rest, String::new()),
                };
                self.notify(ident(ch), payload);
                ok("NOTIFY")
            }
            "LOCK" => {
                if !self.storage.in_transaction {
                    return Some(Err("LOCK TABLE can only be used in transaction blocks".to_string()));
                }
                let mut rest = sql[4..].trim();
                if rest.len() >= 6 && rest[..6].eq_ignore_ascii_case("TABLE ") {
                    rest = rest[6..].trim();
                }
                if rest.len() >= 5 && rest[..5].eq_ignore_ascii_case("ONLY ") {
                    rest = rest[5..].trim();
                }
                let ru = rest.to_ascii_uppercase();
                let end = [" IN ", " NOWAIT"].iter().filter_map(|k| ru.find(k)).min().unwrap_or(rest.len());
                for t in split_comma_separated_tokens(&rest[..end]) {
                    let name = strip_schema_keep_case(t.trim().trim_end_matches('*').trim());
                    if self.storage.get_table(&name).is_none() && self.storage.get_table(&name.to_lowercase()).is_none() {
                        return Some(Err(format!("relation \"{}\" does not exist", name)));
                    }
                }
                // engine một tiến trình, một phiên ghi: khoá luôn có ngay
                ok("LOCK TABLE")
            }
            "PREPARE" if upper.starts_with("PREPARE TRANSACTION") => {
                if !self.storage.in_transaction {
                    // PG: WARNING there is no transaction in progress
                    return ok("ROLLBACK");
                }
                let _ = self.storage.rollback();
                self.plan_cache.clear();
                self.clear_deferred_state();
                self.finish_tx(false);
                Some(Err("prepared transactions are disabled".to_string()))
            }
            "COMMIT" | "ROLLBACK" if upper.contains(" PREPARED") => {
                let id = str_lit(&sql[upper.find(" PREPARED").unwrap() + 9..]).unwrap_or_default();
                Some(Err(format!("prepared transaction with identifier \"{}\" does not exist", id)))
            }
            "SHOW" if upper[4..].trim() == "ALL" => Some(Ok(self.show_all())),
            _ => None,
        }
    }

    /// Chặn lệnh ghi trong giao dịch READ ONLY (25006). Chỉ ở câu cấp cao nhất (câu nội bộ của engine không bị chặn).
    pub(crate) fn read_only_gate(&self, upper: &str) -> Result<(), String> {
        if !self.session.read_only || self.storage.stmt_depth > 1 || !self.storage.in_transaction {
            return Ok(());
        }
        match write_command_name(upper) {
            Some(cmd) => Err(format!("cannot execute {} in a read-only transaction", cmd)),
            None => Ok(()),
        }
    }

    /// `BEGIN / START TRANSACTION / SET TRANSACTION ...`: chế độ READ ONLY / READ WRITE nếu có.
    pub(crate) fn apply_tx_mode(&mut self, upper: &str) {
        if upper.contains("READ ONLY") {
            self.session.read_only = true;
        } else if upper.contains("READ WRITE") {
            self.session.read_only = false;
        } else if upper.starts_with("BEGIN") || upper.starts_with("START") {
            self.session.read_only = self.setting_value("default_transaction_read_only").is_some_and(|v| v == "on");
        }
    }

    /// `SET LOCAL`: nhớ giá trị cũ để trả lại khi giao dịch kết thúc.
    pub(crate) fn remember_local_setting(&mut self, name: &str) {
        if !self.session.local_saved.iter().any(|(n, _)| n == name) {
            let old = self.settings.get(name).cloned();
            self.session.local_saved.push((name.to_string(), old));
        }
    }

    /// Giao dịch kết thúc (COMMIT = `true`, ROLLBACK = `false`).
    pub(crate) fn finish_tx(&mut self, committed: bool) {
        let s = &mut self.session;
        s.read_only = false;
        s.cursors.retain(|_, c| c.hold && committed);
        for (name, old) in s.local_saved.drain(..).rev() {
            match old {
                Some(v) => self.settings.insert(name, v),
                None => self.settings.remove(&name),
            };
        }
        let pending = std::mem::take(&mut self.session.pending_notify);
        if committed {
            for (ch, p) in pending {
                if self.session.listening.contains(&ch) {
                    self.session.notifications.push((ch, p));
                }
            }
        }
    }

    pub(crate) fn notify(&mut self, channel: String, payload: String) {
        if self.storage.in_transaction {
            // PG gộp các thông báo trùng (kênh, nội dung) trong cùng giao dịch
            if !self.session.pending_notify.iter().any(|(c, p)| *c == channel && *p == payload) {
                self.session.pending_notify.push((channel, payload));
            }
        } else if self.session.listening.contains(&channel) {
            self.session.notifications.push((channel, payload));
        }
    }

    /// Lấy (và xoá) các thông báo đã nhận.
    pub fn take_notifications(&mut self) -> Vec<(String, String)> {
        std::mem::take(&mut self.session.notifications)
    }

    /// `pg_notify('kênh', 'nội dung')` với đối số literal: gửi thông báo và thay lời gọi bằng `''` (void).
    pub(crate) fn resolve_notify_calls(&mut self, sql: &str) -> Option<String> {
        let lower = sql.to_ascii_lowercase();
        let mut out = String::with_capacity(sql.len());
        let mut last = 0;
        let mut from = 0;
        while let Some(p) = lower[from..].find("pg_notify(").map(|p| p + from) {
            from = p + 10;
            if p > 0 && (lower.as_bytes()[p - 1].is_ascii_alphanumeric() || lower.as_bytes()[p - 1] == b'_') {
                continue;
            }
            // trong chuỗi '...': bỏ qua
            if sql[..p].bytes().filter(|&b| b == b'\'').count() % 2 == 1 {
                continue;
            }
            let close = super::constraints::matching_paren(sql, p + 9)?;
            let args = split_comma_separated_tokens(&sql[p + 10..close]);
            if args.len() != 2 {
                return None;
            }
            let ch = str_lit(args[0])?;
            let payload = if args[1].trim().eq_ignore_ascii_case("NULL") { String::new() } else { str_lit(args[1])? };
            self.notify(ch, payload);
            out.push_str(&sql[last..p]);
            out.push_str("''");
            last = close + 1;
            from = close + 1;
        }
        if last == 0 {
            return None;
        }
        out.push_str(&sql[last..]);
        Some(out)
    }

    fn show_all(&self) -> QueryResult {
        let rows: Vec<serde_json::Value> = super::catalog_views::settings_rows()
            .iter()
            .map(|r| {
                let name = r[0].clone().unwrap_or_default();
                let lname = name.to_lowercase();
                let engine_default = if lname == "timezone" { Some(super::executor::default_tz().to_string()) } else { super::plpgsql::default_setting(&lname).map(|v| v.to_string()) };
                let cur = self.settings.get(&lname).cloned().or(engine_default).unwrap_or_else(|| super::catalog_views::show_value(r));
                serde_json::json!({ "name": name, "setting": cur, "description": r[4] })
            })
            .collect();
        let fields = ["name", "setting", "description"].iter().map(|n| FieldInfo { name: n.to_string(), data_type: "text".to_string() }).collect();
        QueryResult { row_count: rows.len(), rows, fields, command: "SHOW".to_string() }
    }

    /// `DECLARE tên [BINARY] [ASENSITIVE | INSENSITIVE] [[NO] SCROLL] CURSOR [{WITH | WITHOUT} HOLD] FOR truy_vấn`
    fn declare_cursor(&mut self, sql: &str, upper: &str, params: &[Value]) -> Result<QueryResult, String> {
        let cpos = upper.find(" CURSOR").ok_or_else(|| "syntax error at or near \"DECLARE\"".to_string())?;
        let for_pos = super::executor::find_top_level_keyword(&sql[cpos..], "FOR").map(|p| p + cpos).ok_or("syntax error at end of input")?;
        let head = &upper[7..cpos];
        let name = ident(sql[7..cpos].split_whitespace().next().unwrap_or(""));
        let opts = &upper[cpos + 7..for_pos];
        let hold = opts.contains("WITH HOLD");
        if head.contains("BINARY") {
            return Err("binary cursors are not supported".to_string());
        }
        if !hold && !self.storage.in_transaction {
            return Err("DECLARE CURSOR can only be used in transaction blocks".to_string());
        }
        if self.session.cursors.contains_key(&name) {
            return Err(format!("cursor \"{}\" already exists", name));
        }
        let query = sql[for_pos + 3..].trim();
        let qu = query.to_ascii_uppercase();
        if !(qu.starts_with("SELECT") || qu.starts_with("WITH") || qu.starts_with("VALUES") || qu.starts_with("TABLE") || qu.starts_with('(')) {
            return Err(format!("syntax error at or near \"{}\"", query.split_whitespace().next().unwrap_or("")));
        }
        let res = self.execute_statement(query, params)?;
        // bảng nguồn đơn (cho WHERE CURRENT OF)
        let table = super::executor::find_top_level_keyword(query, "FROM").and_then(|f| {
            let after = query[f + 4..].trim();
            let end = ["WHERE", "ORDER", "LIMIT", "OFFSET", "FOR", "GROUP"].iter().filter_map(|k| super::executor::find_top_level_keyword(after, k)).min().unwrap_or(after.len());
            let t = after[..end].trim();
            let name = t.split_whitespace().next()?;
            (!t.contains(',') && !t.to_ascii_uppercase().contains(" JOIN ") && !t.starts_with('(')).then(|| strip_schema_keep_case(name).to_lowercase())
        });
        self.session.cursors.insert(name, Cursor { res, pos: 0, hold, table });
        Ok(QueryResult { rows: vec![], row_count: 0, fields: vec![], command: "DECLARE CURSOR".to_string() })
    }

    /// `FETCH / MOVE [hướng] [FROM | IN] tên`.
    fn fetch_cursor(&mut self, sql: &str, is_move: bool) -> Result<QueryResult, String> {
        let words: Vec<&str> = sql.split_whitespace().skip(1).collect();
        let name_word = *words.last().ok_or("syntax error at end of input")?;
        let name = ident(name_word);
        let mut dir: Vec<String> = words[..words.len() - 1].iter().map(|w| w.to_ascii_uppercase()).collect();
        if matches!(dir.last().map(|s| s.as_str()), Some("FROM") | Some("IN")) {
            dir.pop();
        }
        let cur = self.session.cursors.get_mut(&name).ok_or_else(|| format!("cursor \"{}\" does not exist", name))?;
        let len = cur.res.rows.len();
        let num = |s: Option<&String>| -> Result<i64, String> {
            match s {
                None => Ok(1),
                Some(x) if x == "ALL" => Ok(i64::MAX),
                Some(x) => x.parse::<i64>().map_err(|_| format!("syntax error at or near \"{}\"", x)),
            }
        };
        // (dòng 1-based theo thứ tự trả về, vị trí mới)
        let mut picked: Vec<usize> = Vec::new();
        let forward = |pos: usize, n: i64, picked: &mut Vec<usize>| -> usize {
            let mut p = pos;
            let mut k = 0i64;
            while k < n && p <= len {
                p += 1;
                if p > len {
                    break;
                }
                picked.push(p);
                k += 1;
            }
            p.min(len + 1)
        };
        let backward = |pos: usize, n: i64, picked: &mut Vec<usize>| -> usize {
            let mut p = pos;
            let mut k = 0i64;
            while k < n && p > 0 {
                p -= 1;
                if p == 0 {
                    break;
                }
                picked.push(p);
                k += 1;
            }
            p
        };
        let absolute = |k: i64, picked: &mut Vec<usize>| -> usize {
            let p = if k >= 0 { (k as usize).min(len + 1) } else if (-k) as usize > len { 0 } else { len + 1 - (-k) as usize };
            if (1..=len).contains(&p) {
                picked.push(p);
            }
            p
        };
        let d0 = dir.first().map(|s| s.as_str());
        let new_pos = match d0 {
            None | Some("NEXT") => forward(cur.pos, 1, &mut picked),
            Some("PRIOR") => backward(cur.pos, 1, &mut picked),
            Some("FIRST") => absolute(1, &mut picked),
            Some("LAST") => absolute(-1, &mut picked),
            Some("ABSOLUTE") => absolute(num(dir.get(1))?, &mut picked),
            Some("RELATIVE") => {
                let k = num(dir.get(1))?;
                if k == 0 {
                    if (1..=len).contains(&cur.pos) {
                        picked.push(cur.pos);
                    }
                    cur.pos
                } else if k > 0 {
                    let target = cur.pos as i64 + k;
                    absolute(target.min(len as i64 + 1), &mut picked)
                } else {
                    let target = cur.pos as i64 + k;
                    if target <= 0 {
                        0
                    } else {
                        absolute(target, &mut picked)
                    }
                }
            }
            Some("ALL") => forward(cur.pos, i64::MAX, &mut picked),
            Some("FORWARD") => forward(cur.pos, num(dir.get(1))?, &mut picked),
            Some("BACKWARD") => backward(cur.pos, num(dir.get(1))?, &mut picked),
            Some(n) if n.parse::<i64>().is_ok() => {
                let k: i64 = n.parse().unwrap();
                if k >= 0 {
                    forward(cur.pos, k, &mut picked)
                } else {
                    backward(cur.pos, -k, &mut picked)
                }
            }
            Some(other) => return Err(format!("syntax error at or near \"{}\"", other)),
        };
        cur.pos = new_pos;
        let n = picked.len();
        if is_move {
            return Ok(QueryResult { rows: vec![], row_count: n, fields: vec![], command: "MOVE".to_string() });
        }
        let rows = picked.into_iter().map(|i| cur.res.rows[i - 1].clone()).collect();
        Ok(QueryResult { rows, row_count: n, fields: cur.res.fields.clone(), command: "FETCH".to_string() })
    }

    /// `UPDATE / DELETE ... WHERE CURRENT OF tên`: thay bằng điều kiện khớp dòng hiện tại của cursor (khoá chính
    /// nếu có, không thì mọi cột của dòng). `None` nếu câu không có `CURRENT OF`.
    pub(crate) fn resolve_current_of(&self, sql: &str) -> Option<Result<String, String>> {
        let up = sql.to_ascii_uppercase();
        let p = up.find("WHERE CURRENT OF ")?;
        let name = ident(sql[p + 17..].split_whitespace().next().unwrap_or(""));
        let Some(cur) = self.session.cursors.get(&name) else {
            return Some(Err(format!("cursor \"{}\" does not exist", name)));
        };
        let Some(tname) = &cur.table else {
            return Some(Err(format!("cursor \"{}\" is not a simply updatable scan of table", name)));
        };
        if cur.pos == 0 || cur.pos > cur.res.rows.len() {
            return Some(Err(format!("cursor \"{}\" is not positioned on a row", name)));
        }
        let Some(serde_json::Value::Object(row)) = cur.res.rows.get(cur.pos - 1) else { return None };
        let table = self.storage.get_table(tname)?;
        let pk: Vec<String> = table.columns.iter().filter(|c| c.is_primary_key).map(|c| c.name.clone()).collect();
        let cols: Vec<String> = if !pk.is_empty() && pk.iter().all(|c| row.contains_key(c)) {
            pk
        } else {
            table.columns.iter().map(|c| c.name.clone()).filter(|c| row.contains_key(c)).collect()
        };
        if cols.is_empty() {
            return Some(Err(format!("cursor \"{}\" is not a simply updatable scan of table \"{}\"", name, tname)));
        }
        let conds: Vec<String> = cols
            .iter()
            .map(|c| match &row[c] {
                serde_json::Value::Null => format!("{} IS NULL", c),
                serde_json::Value::String(s) => format!("{} = '{}'", c, s.replace('\'', "''")),
                serde_json::Value::Bool(b) => format!("{} = {}", c, b),
                serde_json::Value::Number(n) => format!("{} = {}", c, n),
                other => format!("{} = '{}'", c, other.to_string().replace('\'', "''")),
            })
            .collect();
        let tail_start = p + 17 + sql[p + 17..].find(|c: char| !c.is_whitespace()).unwrap_or(0);
        let tail = &sql[tail_start..];
        let after_name = tail.find(char::is_whitespace).map(|i| &tail[i..]).unwrap_or("");
        Some(Ok(format!("{}WHERE {}{}", &sql[..p], conds.join(" AND "), after_name)))
    }
}
