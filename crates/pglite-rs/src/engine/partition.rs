//! Bảng phân vùng (đợt 15b): `CREATE TABLE ... PARTITION BY RANGE | LIST | HASH (khoá)`,
//! `CREATE TABLE con PARTITION OF cha FOR VALUES FROM (..) TO (..) | IN (..) | WITH (MODULUS m, REMAINDER r) | DEFAULT`
//! (lồng nhiều cấp), `ALTER TABLE cha ATTACH | DETACH PARTITION con`.
//!
//! Bảng cha là một bảng thật nhưng không giữ dòng nào: `INSERT INTO cha` chạy như thường (DEFAULT, SERIAL,
//! RETURNING...), rồi các dòng mới được chuyển sang partition khớp (dòng không khớp → lỗi `23514` như PG, cả câu
//! bị huỷ). `INSERT` thẳng vào partition cũng qua đường này để từ chối dòng nằm ngoài khoảng của nó. Đọc từ bảng
//! cha (`FROM cha`, `JOIN cha`) được thay bằng `(SELECT ... UNION ALL ...)` các partition lá; `UPDATE` / `DELETE`
//! / `TRUNCATE` chạy trên từng partition, `UPDATE` làm khoá đổi partition thì dòng được chuyển như PG.
//! Metadata lưu bằng `SchemaOp::TableMeta` (`partition_by`, `partition_of`): bản cũ bỏ qua và vẫn thấy dữ liệu
//! trong từng partition như bảng thường. Phân vùng HASH dùng hàm băm riêng của engine (khác PG).
use super::executor::{contains_ci, find_top_level_keyword, split_comma_separated_tokens, strip_schema_keep_case, Executor};
use crate::storage::catalog::SchemaOp;
use crate::types::{QueryResult, Value};
use serde::{Deserialize, Serialize};
use std::cell::Cell;

#[derive(Serialize, Deserialize, Clone, Debug)]
pub(crate) struct PartBy {
    /// range | list | hash
    pub strategy: String,
    pub keys: Vec<String>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub(crate) struct PartOf {
    pub parent: String,
    /// `FROM (..) TO (..)`, `IN (..)`, `WITH (..)`, `DEFAULT` — như khai báo
    pub bound: String,
}

thread_local! {
    /// Đang chạy câu nội bộ của module này: không xử lý partition lần nữa.
    static BUSY: Cell<u32> = const { Cell::new(0) };
}

struct Busy;
impl Busy {
    fn new() -> Busy {
        BUSY.with(|b| b.set(b.get() + 1));
        Busy
    }
}
impl Drop for Busy {
    fn drop(&mut self) {
        BUSY.with(|b| b.set(b.get() - 1));
    }
}

fn ok(cmd: &str, n: usize) -> QueryResult {
    QueryResult { rows: vec![], row_count: n, fields: vec![], command: cmd.to_string() }
}

fn word_at(s: &str, kw: &str) -> bool {
    s.len() >= kw.len() && s[..kw.len()].eq_ignore_ascii_case(kw) && s[kw.len()..].chars().next().map_or(true, |c| !(c.is_alphanumeric() || c == '_'))
}

/// Tên bảng ở đầu `s` (có thể kèm schema / nháy) và phần còn lại.
fn take_name(s: &str) -> (String, &str) {
    let t = s.trim_start();
    let end = if t.starts_with('"') {
        t[1..].find('"').map(|p| p + 2).unwrap_or(t.len())
    } else {
        t.find(|c: char| c.is_whitespace() || c == '(' || c == ';' || c == ',').unwrap_or(t.len())
    };
    let mut end = end;
    // schema.tên
    if t[end..].starts_with('.') {
        let rest = &t[end + 1..];
        let e2 = rest.find(|c: char| c.is_whitespace() || c == '(' || c == ';' || c == ',').unwrap_or(rest.len());
        end += 1 + e2;
    }
    (strip_schema_keep_case(&t[..end]).to_lowercase(), &t[end..])
}

/// Hàm băm cho phân vùng HASH (FNV-1a trên văn bản của các khoá).
pub(crate) fn partition_hash(vals: &[Value]) -> u64 {
    let mut h: u64 = 0xcbf29ce484222325;
    for v in vals {
        let t = match v {
            Value::Null => continue,
            Value::Int(i) => i.to_string(),
            Value::Float(f) => f.to_string(),
            Value::Bool(b) => b.to_string(),
            Value::Text(t) => t.to_string(),
        };
        for b in t.bytes().chain(std::iter::once(0u8)) {
            h ^= b as u64;
            h = h.wrapping_mul(0x100000001b3);
        }
    }
    h
}

pub(crate) fn call_hash_fn(args: &[Value]) -> Option<Value> {
    let m = args.first()?.as_i64()?;
    let r = args.get(1)?.as_i64()?;
    if m <= 0 {
        return Some(Value::Bool(false));
    }
    Some(Value::Bool((partition_hash(&args[2..]) % m as u64) as i64 == r))
}

/// Biểu thức SQL đúng khi dòng thuộc khoảng `bound` (không dùng cho DEFAULT).
fn bound_predicate(by: &PartBy, bound: &str) -> Result<String, String> {
    let b = bound.trim();
    let bu = b.to_ascii_uppercase();
    let keys = &by.keys;
    let row = |k: &[String]| if k.len() == 1 { k[0].clone() } else { format!("({})", k.join(", ")) };
    if bu.starts_with("FROM") {
        let to = find_top_level_keyword(b, "TO").ok_or("syntax error at or near \"FROM\"")?;
        let lo = b[4..to].trim().trim_start_matches('(').trim_end_matches(')').to_string();
        let hi = b[to + 2..].trim().trim_start_matches('(').trim_end_matches(')').to_string();
        let lo_v: Vec<&str> = split_comma_separated_tokens(&lo);
        let hi_v: Vec<&str> = split_comma_separated_tokens(&hi);
        if lo_v.len() != keys.len() || hi_v.len() != keys.len() {
            return Err("FROM must specify exactly one value per partitioning column".to_string());
        }
        let mut conds = Vec::new();
        // một cột: so sánh trực tiếp; nhiều cột: so sánh từng phần như so sánh dòng
        if keys.len() == 1 {
            if !lo_v[0].trim().eq_ignore_ascii_case("MINVALUE") {
                conds.push(format!("{} >= {}", keys[0], lo_v[0].trim()));
            }
            if !hi_v[0].trim().eq_ignore_ascii_case("MAXVALUE") {
                conds.push(format!("{} < {}", keys[0], hi_v[0].trim()));
            }
        } else {
            let lex = |vals: &[&str], ge: bool| -> String {
                // (k1, k2) >= (a, b)  ≡  k1 > a OR (k1 = a AND k2 >= b)
                let mut parts = Vec::new();
                for i in 0..keys.len() {
                    if vals[i].trim().eq_ignore_ascii_case("MINVALUE") || vals[i].trim().eq_ignore_ascii_case("MAXVALUE") {
                        break;
                    }
                    let eqs: Vec<String> = (0..i).map(|j| format!("{} = {}", keys[j], vals[j].trim())).collect();
                    let last = i + 1 == keys.len();
                    let op = match (ge, last) {
                        (true, true) => ">=",
                        (false, true) => "<",
                        (true, false) => ">",
                        (false, false) => "<",
                    };
                    let mut c = eqs.clone();
                    c.push(format!("{} {} {}", keys[i], op, vals[i].trim()));
                    parts.push(format!("({})", c.join(" AND ")));
                }
                if parts.is_empty() { "TRUE".to_string() } else { format!("({})", parts.join(" OR ")) }
            };
            conds.push(lex(&lo_v, true));
            conds.push(lex(&hi_v, false));
        }
        let _ = row;
        Ok(if conds.is_empty() { "TRUE".to_string() } else { format!("({})", conds.join(" AND ")) })
    } else if bu.starts_with("IN") {
        let inner = b[2..].trim().trim_start_matches('(').trim_end_matches(')');
        let vals: Vec<&str> = split_comma_separated_tokens(inner);
        let null = vals.iter().any(|v| v.trim().eq_ignore_ascii_case("NULL"));
        let rest: Vec<&str> = vals.iter().copied().filter(|v| !v.trim().eq_ignore_ascii_case("NULL")).collect();
        let mut c = Vec::new();
        if !rest.is_empty() {
            c.push(format!("{} IN ({})", keys[0], rest.join(", ")));
        }
        if null {
            c.push(format!("{} IS NULL", keys[0]));
        }
        Ok(if c.is_empty() { "FALSE".to_string() } else { format!("({})", c.join(" OR ")) })
    } else if bu.starts_with("WITH") {
        let inner = b[4..].trim().trim_start_matches('(').trim_end_matches(')');
        let mut m = None;
        let mut r = None;
        for p in split_comma_separated_tokens(inner) {
            let w: Vec<&str> = p.split_whitespace().collect();
            match (w.first().map(|x| x.to_ascii_uppercase()), w.get(1)) {
                (Some(k), Some(v)) if k == "MODULUS" => m = v.parse::<i64>().ok(),
                (Some(k), Some(v)) if k == "REMAINDER" => r = v.parse::<i64>().ok(),
                _ => {}
            }
        }
        let (Some(m), Some(r)) = (m, r) else { return Err("invalid bound specification for a hash partition".to_string()) };
        if r >= m {
            return Err(format!("remainder for hash partition must be less than modulus"));
        }
        Ok(format!("pg_partition_hash({}, {}, {})", m, r, keys.join(", ")))
    } else {
        Err(format!("syntax error at or near \"{}\"", b.split_whitespace().next().unwrap_or("")))
    }
}

impl Executor {
    fn part_by(&self, table: &str) -> Option<PartBy> {
        self.storage.get_table(table)?.meta.get("partition_by").and_then(|j| serde_json::from_str(j).ok())
    }
    fn part_of(&self, table: &str) -> Option<PartOf> {
        self.storage.get_table(table)?.meta.get("partition_of").and_then(|j| serde_json::from_str(j).ok())
    }
    /// Partition trực tiếp của `parent`: (tên, khoảng).
    fn partitions(&self, parent: &str) -> Vec<(String, String)> {
        let mut v: Vec<(String, String)> = self
            .storage
            .tables
            .iter()
            .filter_map(|(k, t)| {
                let po: PartOf = serde_json::from_str(t.meta.get("partition_of")?).ok()?;
                (po.parent == parent).then(|| (k.clone(), po.bound))
            })
            .collect();
        v.sort();
        v
    }
    /// Partition lá (không tự phân vùng) dưới `parent`.
    fn leaf_partitions(&self, parent: &str) -> Vec<String> {
        let mut out = Vec::new();
        for (c, _) in self.partitions(parent) {
            if self.part_by(&c).is_some() {
                out.extend(self.leaf_partitions(&c));
            } else {
                out.push(c);
            }
        }
        out
    }

    /// Có bảng phân vùng không (cache theo DDL như range).
    pub(crate) fn has_partitions(&self) -> bool {
        let key = (self.storage.wal.ddl_appended, self.storage.tables.len());
        if let Some((d, n, v)) = self.part_cache.get() {
            if (d, n) == key {
                return v;
            }
        }
        let v = self.storage.tables.values().any(|t| t.meta.contains_key("partition_by"));
        self.part_cache.set(Some((key.0, key.1, v)));
        v
    }

    fn col_list(&self, table: &str) -> String {
        self.storage.get_table(table).map(|t| t.columns.iter().map(|c| format!("\"{}\"", c.name)).collect::<Vec<_>>().join(", ")).unwrap_or_else(|| "*".to_string())
    }

    /// DDL và DML của partition. `None` = câu không thuộc diện.
    pub(crate) fn handle_partitioning(&mut self, sql: &str, upper: &str, params: &[Value]) -> Option<Result<QueryResult, String>> {
        if BUSY.with(|b| b.get()) > 0 {
            return None;
        }
        if upper.starts_with("CREATE") && (contains_ci(sql, " PARTITION BY ") || contains_ci(sql, " PARTITION OF ")) && find_top_level_keyword(sql, "TABLE").is_some() {
            return Some(self.create_partitioned(sql));
        }
        if upper.starts_with("ALTER TABLE") && (contains_ci(sql, " ATTACH PARTITION ") || contains_ci(sql, " DETACH PARTITION ")) {
            return Some(self.attach_detach(sql));
        }
        if !self.has_partitions() {
            return None;
        }
        let first = upper.split_whitespace().next().unwrap_or("");
        match first {
            "INSERT" => {
                let rest = sql.trim_start()[6..].trim_start();
                if !word_at(rest, "INTO") {
                    return None;
                }
                let (target, _) = take_name(&rest[4..]);
                if self.part_by(&target).is_some() || self.part_of(&target).is_some() {
                    return Some(self.insert_routed(sql, &target, params));
                }
            }
            "UPDATE" | "DELETE" => {
                let rest = sql.trim_start()[first.len()..].trim_start();
                let rest = if first == "DELETE" {
                    if !word_at(rest, "FROM") {
                        return None;
                    }
                    &rest[4..]
                } else {
                    rest
                };
                let rest = if word_at(rest.trim_start(), "ONLY") { &rest.trim_start()[4..] } else { rest };
                let (target, _) = take_name(rest);
                if self.part_by(&target).is_some() {
                    return Some(self.dml_on_partitions(sql, first, &target, params));
                }
                if first == "UPDATE" && self.part_of(&target).is_some() {
                    return Some(self.update_partition_checked(sql, &target, params));
                }
            }
            "TRUNCATE" => {
                let rest = sql.trim_start()[8..].trim_start();
                let rest = if word_at(rest, "TABLE") { &rest[5..] } else { rest };
                let (target, _) = take_name(rest);
                if self.part_by(&target).is_some() {
                    let _b = Busy::new();
                    let mut n = 0;
                    for leaf in self.leaf_partitions(&target) {
                        let q = format!("TRUNCATE {}", leaf);
                        if let Err(e) = self.execute_statement(&q, params) {
                            return Some(Err(e));
                        }
                        n += 1;
                    }
                    let _ = n;
                    return Some(Ok(ok("TRUNCATE TABLE", 0)));
                }
            }
            "DROP" if upper.starts_with("DROP TABLE") => {
                // xoá bảng cha thì xoá luôn các partition (như PG)
                let rest = sql.trim_start()[10..].trim_start();
                let rest = if rest.len() > 9 && rest[..9].eq_ignore_ascii_case("IF EXISTS") { &rest[9..] } else { rest };
                let names: Vec<String> = split_comma_separated_tokens(rest.split_whitespace().take_while(|w| !w.eq_ignore_ascii_case("CASCADE") && !w.eq_ignore_ascii_case("RESTRICT")).collect::<Vec<_>>().join(" ").as_str()).iter().map(|n| take_name(n).0).collect();
                let parents: Vec<String> = names.iter().filter(|n| self.part_by(n).is_some()).cloned().collect();
                if parents.is_empty() {
                    return None;
                }
                let _b = Busy::new();
                for p in &parents {
                    for (c, _) in self.all_descendants(p) {
                        if let Err(e) = self.execute_statement(&format!("DROP TABLE IF EXISTS {}", c), params) {
                            return Some(Err(e));
                        }
                    }
                }
                return Some(self.execute_statement(sql, params));
            }
            _ => {}
        }
        None
    }

    fn all_descendants(&self, parent: &str) -> Vec<(String, String)> {
        let mut out = Vec::new();
        for (c, b) in self.partitions(parent) {
            out.extend(self.all_descendants(&c));
            out.push((c, b));
        }
        out
    }

    /// Thay `FROM cha` / `JOIN cha` bằng bảng tạm `__part_cha` (dựng ở `partition_reads`). `None` = không đổi.
    fn rewrite_partition_reads(&self, sql: &str) -> Option<String> {
        if !self.has_partitions() {
            return None;
        }
        let parents: Vec<String> = self.storage.tables.iter().filter(|(_, t)| t.meta.contains_key("partition_by")).map(|(k, _)| k.clone()).collect();
        if !parents.iter().any(|p| contains_ci(sql, p)) {
            return None;
        }
        let m = super::oprewrite::code_mask(sql);
        let b = sql.as_bytes();
        let mut out = String::with_capacity(sql.len() + 64);
        let mut last = 0;
        let mut i = 0;
        let mut changed = false;
        let is_id = |c: u8| c.is_ascii_alphanumeric() || c == b'_';
        while i < b.len() {
            if !m[i] || !is_id(b[i]) || (i > 0 && (is_id(b[i - 1]) || b[i - 1] == b'.' || b[i - 1] == b'"')) {
                i += 1;
                continue;
            }
            let st = i;
            while i < b.len() && is_id(b[i]) {
                i += 1;
            }
            let word = sql[st..i].to_ascii_lowercase();
            if !parents.contains(&word) || b.get(i) == Some(&b'.') || b.get(i) == Some(&b'(') {
                continue;
            }
            // phải đứng sau FROM / JOIN / dấu phẩy trong FROM (bỏ `public.`)
            let before = sql[..st].trim_end();
            let before = before.strip_suffix("public.").map(|x| x.trim_end()).unwrap_or(before);
            let bu = before.to_ascii_uppercase();
            let after_kw = bu.ends_with("FROM") || bu.ends_with("JOIN") || bu.ends_with("ONLY") || (before.ends_with(',') && find_top_level_keyword(sql, "FROM").is_some_and(|f| f < st));
            let into = bu.ends_with("INTO") || bu.ends_with("UPDATE") || bu.ends_with("TABLE");
            if !after_kw || into {
                continue;
            }
            let union = format!("__part_{}", word);
            // alias: giữ alias có sẵn, không có thì dùng tên bảng
            let rest = sql[i..].trim_start();
            let ru = rest.to_ascii_uppercase();
            let has_alias = word_at(&ru, "AS")
                || (!rest.is_empty()
                    && rest.chars().next().is_some_and(|c| c.is_alphabetic() || c == '_' || c == '"')
                    && !["WHERE", "JOIN", "LEFT", "RIGHT", "INNER", "FULL", "CROSS", "ON", "USING", "GROUP", "ORDER", "LIMIT", "OFFSET", "UNION", "EXCEPT", "INTERSECT", "HAVING", "WINDOW", "FOR", "NATURAL", "RETURNING", "SET", "FETCH"].iter().any(|k| word_at(&ru, k)));
            let start = if sql[..st].trim_end().ends_with("public.") { sql[..st].trim_end().len() - 7 } else { st };
            out.push_str(&sql[last..start]);
            out.push_str(&union);
            if !has_alias {
                out.push_str(&format!(" AS {}", word));
            }
            last = i;
            changed = true;
        }
        if !changed {
            return None;
        }
        out.push_str(&sql[last..]);
        Some(out)
    }

    fn create_partitioned(&mut self, sql: &str) -> Result<QueryResult, String> {
        let t = sql.trim().trim_end_matches(';').trim();
        let tpos = find_top_level_keyword(t, "TABLE").unwrap();
        let mut rest = t[tpos + 5..].trim_start();
        let if_not_exists = rest.len() > 13 && rest[..13].eq_ignore_ascii_case("IF NOT EXISTS");
        if if_not_exists {
            rest = rest[13..].trim_start();
        }
        let (name, after) = take_name(rest);
        if self.storage.get_table(&name).is_some() {
            if if_not_exists {
                return Ok(ok("CREATE TABLE", 0));
            }
            return Err(format!("relation \"{}\" already exists", name));
        }
        // PARTITION BY ở cuối (cả bảng thường lẫn partition lồng)
        let (head, by) = match find_top_level_keyword(t, "PARTITION BY") {
            Some(p) if !t[p..].to_ascii_uppercase().starts_with("PARTITION OF") => (t[..p].trim_end(), Some(t[p + 12..].trim())),
            _ => (t, None),
        };
        let part_by = match by {
            Some(b) => {
                let bu = b.to_ascii_uppercase();
                let strategy = ["RANGE", "LIST", "HASH"].iter().find(|s| bu.starts_with(**s)).ok_or_else(|| format!("unrecognized partitioning strategy \"{}\"", b.split_whitespace().next().unwrap_or("")))?;
                let open = b.find('(').ok_or("syntax error at end of input")?;
                let close = super::constraints::matching_paren(b, open).ok_or("syntax error at end of input")?;
                let keys: Vec<String> = split_comma_separated_tokens(&b[open + 1..close]).iter().map(|k| k.trim().to_string()).collect();
                if *strategy == "LIST" && keys.len() != 1 {
                    return Err("cannot use \"list\" partition strategy with more than one column".to_string());
                }
                Some(PartBy { strategy: strategy.to_ascii_lowercase(), keys })
            }
            None => None,
        };
        let _b = Busy::new();
        let au = after.trim_start().to_ascii_uppercase();
        if au.starts_with("PARTITION OF") {
            let after_of = &after.trim_start()[12..];
            let (parent, bound_rest) = take_name(after_of);
            let pby = self.part_by(&parent).ok_or_else(|| {
                if self.storage.get_table(&parent).is_some() {
                    format!("\"{}\" is not partitioned", parent)
                } else {
                    format!("relation \"{}\" does not exist", parent)
                }
            })?;
            // (ràng buộc thêm) tuỳ chọn trước FOR VALUES
            let br = bound_rest.trim_start();
            let (extra, br) = if br.starts_with('(') {
                let c = super::constraints::matching_paren(br, 0).ok_or("syntax error")?;
                (Some(br[1..c].to_string()), br[c + 1..].trim_start())
            } else {
                (None, br)
            };
            let bound = if br.to_ascii_uppercase().starts_with("DEFAULT") {
                "DEFAULT".to_string()
            } else if br.len() >= 10 && br[..10].eq_ignore_ascii_case("FOR VALUES") {
                let bu = br[10..].trim();
                let bound_end = find_top_level_keyword(bu, "PARTITION BY").unwrap_or(bu.len());
                bu[..bound_end].trim().to_string()
            } else {
                return Err("syntax error at or near \"PARTITION\"".to_string());
            };
            self.check_bound(&parent, &pby, &name, &bound)?;
            let extra = extra.map(|e| format!(", {}", e)).unwrap_or_default();
            self.execute_statement(&format!("CREATE TABLE {} (LIKE {} INCLUDING ALL{})", name, parent, extra), &[])?;
            let po = PartOf { parent: parent.clone(), bound };
            self.storage.log_schema_op(SchemaOp::TableMeta { table: name.clone(), key: "partition_of".into(), value: Some(serde_json::to_string(&po).unwrap_or_default()) });
        } else {
            self.execute_statement(head, &[])?;
        }
        if let Some(pb) = part_by {
            for k in &pb.keys {
                let col = k.trim_matches('"');
                if col.chars().all(|c| c.is_alphanumeric() || c == '_') && self.storage.get_table(&name).is_some_and(|t| t.get_column_index(col).is_none()) {
                    return Err(format!("column \"{}\" named in partition key does not exist", col));
                }
            }
            self.storage.log_schema_op(SchemaOp::TableMeta { table: name.clone(), key: "partition_by".into(), value: Some(serde_json::to_string(&pb).unwrap_or_default()) });
        }
        self.plan_cache.clear();
        Ok(ok("CREATE TABLE", 0))
    }

    /// Khoảng mới không được chồng partition đã có (RANGE / LIST), có tối đa một DEFAULT.
    fn check_bound(&mut self, parent: &str, by: &PartBy, name: &str, bound: &str) -> Result<(), String> {
        let existing = self.partitions(parent);
        if bound.eq_ignore_ascii_case("DEFAULT") {
            if let Some((d, _)) = existing.iter().find(|(_, b)| b.eq_ignore_ascii_case("DEFAULT")) {
                return Err(format!("partition \"{}\" conflicts with existing default partition \"{}\"", name, d));
            }
            return Ok(());
        }
        let bu = bound.to_ascii_uppercase();
        let want = match by.strategy.as_str() {
            "range" => "FROM",
            "list" => "IN",
            _ => "WITH",
        };
        if !bu.starts_with(want) {
            return Err(format!("invalid bound specification for a {} partition", by.strategy));
        }
        let pred = bound_predicate(by, bound)?;
        if by.strategy == "range" && by.keys.len() == 1 {
            let to = find_top_level_keyword(bound, "TO").unwrap();
            let lo = bound[4..to].trim().trim_start_matches('(').trim_end_matches(')').trim().to_string();
            let hi = bound[to + 2..].trim().trim_start_matches('(').trim_end_matches(')').trim().to_string();
            let minmax = |v: &str| v.eq_ignore_ascii_case("MINVALUE") || v.eq_ignore_ascii_case("MAXVALUE");
            if !minmax(&lo) && !minmax(&hi) {
                let r = self.execute_statement(&format!("SELECT ({}) >= ({}) AS e", lo, hi), &[])?;
                if r.rows.first().and_then(|x| x.get("e")).and_then(|v| v.as_bool()) == Some(true) {
                    return Err(format!("empty range bound specified for partition \"{}\"", name));
                }
            }
            for (other, ob) in &existing {
                let obu = ob.to_ascii_uppercase();
                if !obu.starts_with("FROM") {
                    continue;
                }
                let oto = find_top_level_keyword(ob, "TO").unwrap();
                let olo = ob[4..oto].trim().trim_start_matches('(').trim_end_matches(')').trim().to_string();
                let ohi = ob[oto + 2..].trim().trim_start_matches('(').trim_end_matches(')').trim().to_string();
                // chồng nhau ⟺ lo < ohi AND olo < hi (MINVALUE / MAXVALUE luôn thoả)
                let c1 = if minmax(&lo) || minmax(&ohi) { "TRUE".to_string() } else { format!("({}) < ({})", lo, ohi) };
                let c2 = if minmax(&olo) || minmax(&hi) { "TRUE".to_string() } else { format!("({}) < ({})", olo, hi) };
                let r = self.execute_statement(&format!("SELECT {} AND {} AS o", c1, c2), &[])?;
                if r.rows.first().and_then(|x| x.get("o")).and_then(|v| v.as_bool()) == Some(true) {
                    return Err(format!("partition \"{}\" would overlap partition \"{}\"", name, other));
                }
            }
        } else if by.strategy == "list" {
            let inner = bound[2..].trim().trim_start_matches('(').trim_end_matches(')').to_string();
            let mine: Vec<String> = split_comma_separated_tokens(&inner).iter().map(|v| v.trim().to_lowercase()).collect();
            for (other, ob) in &existing {
                if !ob.to_ascii_uppercase().starts_with("IN") {
                    continue;
                }
                let oi = ob[2..].trim().trim_start_matches('(').trim_end_matches(')').to_string();
                if split_comma_separated_tokens(&oi).iter().any(|v| mine.contains(&v.trim().to_lowercase())) {
                    return Err(format!("partition \"{}\" would overlap partition \"{}\"", name, other));
                }
            }
        }
        let _ = pred;
        Ok(())
    }

    fn attach_detach(&mut self, sql: &str) -> Result<QueryResult, String> {
        let t = sql.trim().trim_end_matches(';').trim();
        let rest = t[11..].trim_start();
        let (parent, after) = take_name(rest);
        let au = after.trim_start().to_ascii_uppercase();
        let pby = self.part_by(&parent).ok_or_else(|| format!("table \"{}\" is not partitioned", parent))?;
        let _b = Busy::new();
        if au.starts_with("DETACH PARTITION") {
            let (child, _) = take_name(&after.trim_start()[16..]);
            match self.part_of(&child) {
                Some(po) if po.parent == parent => {}
                _ => return Err(format!("relation \"{}\" is not a partition of relation \"{}\"", child, parent)),
            }
            self.storage.log_schema_op(SchemaOp::TableMeta { table: child, key: "partition_of".into(), value: None });
            self.plan_cache.clear();
            return Ok(ok("ALTER TABLE", 0));
        }
        let (child, after_child) = take_name(&after.trim_start()[16..]);
        if self.storage.get_table(&child).is_none() {
            return Err(format!("relation \"{}\" does not exist", child));
        }
        if self.part_of(&child).is_some() {
            return Err(format!("\"{}\" is already a partition", child));
        }
        let br = after_child.trim_start();
        let bound = if br.to_ascii_uppercase().starts_with("DEFAULT") {
            "DEFAULT".to_string()
        } else if br.len() >= 10 && br[..10].eq_ignore_ascii_case("FOR VALUES") {
            br[10..].trim().to_string()
        } else {
            return Err("syntax error at or near \"ATTACH\"".to_string());
        };
        self.check_bound(&parent, &pby, &child, &bound)?;
        // dòng có sẵn phải thuộc khoảng
        if !bound.eq_ignore_ascii_case("DEFAULT") {
            let pred = bound_predicate(&pby, &bound)?;
            let r = self.execute_statement(&format!("SELECT count(*) AS n FROM {} WHERE ({}) IS NOT TRUE", child, pred), &[])?;
            if r.rows.first().and_then(|x| x.get("n")).and_then(|v| v.as_i64()).unwrap_or(0) > 0 {
                return Err(format!("partition constraint of relation \"{}\" is violated by some row", child));
            }
        }
        let po = PartOf { parent, bound };
        self.storage.log_schema_op(SchemaOp::TableMeta { table: child, key: "partition_of".into(), value: Some(serde_json::to_string(&po).unwrap_or_default()) });
        self.plan_cache.clear();
        Ok(ok("ALTER TABLE", 0))
    }

    /// Chuyển mọi dòng đang nằm trong `staging` (bảng cha, hoặc bảng cha của partition đích) sang partition khớp
    /// dưới `parent`. `only`: chỉ được vào partition này (INSERT thẳng vào partition).
    fn route_rows(&mut self, staging: &str, parent: &str, only: Option<&str>, params: &[Value]) -> Result<(), String> {
        let by = self.part_by(parent).ok_or_else(|| format!("\"{}\" is not partitioned", parent))?;
        let cols = self.col_list(staging);
        let parts = self.partitions(parent);
        let mut default: Option<String> = None;
        for (child, bound) in &parts {
            if bound.eq_ignore_ascii_case("DEFAULT") {
                default = Some(child.clone());
                continue;
            }
            let pred = bound_predicate(&by, bound)?;
            if only.is_some_and(|o| o != child && !self.is_ancestor_of(child, o)) {
                continue;
            }
            self.move_rows(staging, child, &format!("({}) IS TRUE", pred), &cols, only, params)?;
        }
        if let Some(d) = default {
            if only.map_or(true, |o| o == d || self.is_ancestor_of(&d, o)) {
                self.move_rows(staging, &d, "TRUE", &cols, only, params)?;
            }
        }
        // còn dòng → không có partition nào nhận
        let r = self.execute_statement(&format!("SELECT count(*) AS n FROM {}", staging), &[])?;
        if r.rows.first().and_then(|x| x.get("n")).and_then(|v| v.as_i64()).unwrap_or(0) > 0 {
            return Err(match only {
                Some(o) => format!("new row for relation \"{}\" violates partition constraint", o),
                None => format!("no partition of relation \"{}\" found for row", parent),
            });
        }
        Ok(())
    }

    fn is_ancestor_of(&self, a: &str, b: &str) -> bool {
        let mut cur = b.to_string();
        while let Some(po) = self.part_of(&cur) {
            if po.parent == a {
                return true;
            }
            cur = po.parent;
        }
        false
    }

    fn move_rows(&mut self, staging: &str, child: &str, cond: &str, cols: &str, only: Option<&str>, params: &[Value]) -> Result<(), String> {
        let count = self.execute_statement(&format!("SELECT count(*) AS n FROM {} WHERE {}", staging, cond), &[])?;
        if count.rows.first().and_then(|x| x.get("n")).and_then(|v| v.as_i64()).unwrap_or(0) == 0 {
            return Ok(());
        }
        if self.part_by(child).is_some() {
            // partition lồng: chuyển xuống bảng con rồi định tuyến tiếp
            self.execute_statement(&format!("INSERT INTO {} ({}) SELECT {} FROM {} WHERE {}", child, cols, cols, staging, cond), params)?;
            self.execute_statement(&format!("DELETE FROM {} WHERE {}", staging, cond), &[])?;
            return self.route_rows(child, child, only, params);
        }
        self.execute_statement(&format!("INSERT INTO {} ({}) SELECT {} FROM {} WHERE {}", child, cols, cols, staging, cond), params)?;
        self.execute_statement(&format!("DELETE FROM {} WHERE {}", staging, cond), &[])?;
        Ok(())
    }

    fn insert_routed(&mut self, sql: &str, target: &str, params: &[Value]) -> Result<QueryResult, String> {
        let _b = Busy::new();
        let (staging, parent, only) = if self.part_by(target).is_some() {
            (target.to_string(), target.to_string(), None)
        } else {
            // INSERT thẳng vào partition: ghi tạm vào bảng cha của nó rồi chỉ cho vào partition này
            let po = self.part_of(target).unwrap();
            (po.parent.clone(), po.parent.clone(), Some(target.to_string()))
        };
        let q = match &only {
            None => sql.to_string(),
            Some(_) => {
                let p = sql.to_ascii_uppercase().find(&target.to_ascii_uppercase()).unwrap_or(0);
                format!("{}{}{}", &sql[..p], staging, &sql[p + target.len()..])
            }
        };
        let res = self.run_with_partition_reads(&q, params)?;
        // gốc phân vùng để định tuyến: nếu bảng cha của partition đích lại là partition, vẫn chỉ định tuyến từ nó
        self.route_rows(&staging, &parent, only.as_deref(), params)?;
        Ok(res)
    }

    /// Chạy câu có đọc từ bảng cha: dựng bảng tạm `__part_cha` (chép dòng của các partition lá, không ghi WAL).
    fn run_with_partition_reads(&mut self, sql: &str, params: &[Value]) -> Result<QueryResult, String> {
        let Some(q) = self.rewrite_partition_reads(sql) else { return self.execute_statement(sql, params) };
        let parents: Vec<String> = self.storage.tables.iter().filter(|(_, t)| t.meta.contains_key("partition_by")).map(|(k, _)| k.clone()).collect();
        let mut made = Vec::new();
        for p in parents {
            let tmp = format!("__part_{}", p);
            if !contains_ci(&q, &tmp) || self.storage.tables.contains_key(&tmp) {
                continue;
            }
            let Some(pt) = self.storage.get_table(&p) else { continue };
            let mut t = crate::storage::table::Table::new(tmp.clone(), pt.columns.clone());
            t.decl_types = pt.decl_types.clone();
            t.pk_col_idx = None;
            let names: Vec<String> = pt.columns.iter().map(|c| c.name.to_lowercase()).collect();
            for leaf in self.leaf_partitions(&p) {
                let Some(lt) = self.storage.get_table(&leaf) else { continue };
                let map: Vec<Option<usize>> = names.iter().map(|n| lt.columns.iter().position(|c| c.name.to_lowercase() == *n)).collect();
                for (ri, row) in lt.rows.iter().enumerate() {
                    if lt.is_deleted.get(ri).copied().unwrap_or(false) {
                        continue;
                    }
                    t.insert(map.iter().map(|m| m.and_then(|i| row.get(i).cloned()).unwrap_or(Value::Null)).collect());
                }
            }
            self.storage.tables.insert(tmp.clone(), t);
            made.push(tmp);
        }
        let r = self.execute_statement(&q, params);
        for m in made {
            self.storage.tables.remove(&m);
        }
        r
    }

    /// Câu đọc từ bảng phân vùng (SELECT, INSERT ... SELECT, UPDATE ... FROM...). `None` = không thuộc diện.
    pub(crate) fn partition_reads(&mut self, sql: &str, params: &[Value]) -> Option<Result<QueryResult, String>> {
        if BUSY.with(|b| b.get()) > 0 || !self.has_partitions() {
            return None;
        }
        self.rewrite_partition_reads(sql)?;
        let _b = Busy::new();
        Some(self.run_with_partition_reads(sql, params))
    }

    /// UPDATE / DELETE trên bảng cha: chạy trên từng partition lá; UPDATE đổi partition thì chuyển dòng.
    fn dml_on_partitions(&mut self, sql: &str, verb: &str, target: &str, params: &[Value]) -> Result<QueryResult, String> {
        let _b = Busy::new();
        let leaves = self.leaf_partitions(target);
        let mut total = 0usize;
        let mut rows = Vec::new();
        let mut fields = Vec::new();
        let up = sql.to_ascii_uppercase();
        let tpos = up.find(&target.to_ascii_uppercase()).unwrap_or(0);
        for leaf in &leaves {
            let q = format!("{}{}{}", &sql[..tpos], leaf, &sql[tpos + target.len()..]);
            // tham chiếu `cha.cột` trong câu: alias về bảng cha
            let q = if find_top_level_keyword(&q[tpos..], "AS").is_none() && contains_ci(&q, &format!("{}.", target)) {
                let lp = tpos + leaf.len();
                format!("{} AS {}{}", &q[..lp], target, &q[lp..])
            } else {
                q
            };
            let r = self.run_with_partition_reads(&q, params)?;
            total += r.row_count;
            if fields.is_empty() {
                fields = r.fields.clone();
            }
            rows.extend(r.rows);
        }
        if verb == "UPDATE" {
            // dòng không còn thuộc partition của nó: chuyển về bảng cha rồi định tuyến lại
            let cols = self.col_list(target);
            let mut moved = false;
            for leaf in &leaves {
                let pred = self.full_predicate(leaf)?;
                let Some(pred) = pred else { continue };
                let cond = format!("({}) IS NOT TRUE", pred);
                let r = self.execute_statement(&format!("SELECT count(*) AS n FROM {} WHERE {}", leaf, cond), &[])?;
                if r.rows.first().and_then(|x| x.get("n")).and_then(|v| v.as_i64()).unwrap_or(0) > 0 {
                    self.execute_statement(&format!("INSERT INTO {} ({}) SELECT {} FROM {} WHERE {}", target, cols, cols, leaf, cond), &[])?;
                    self.execute_statement(&format!("DELETE FROM {} WHERE {}", leaf, cond), &[])?;
                    moved = true;
                }
            }
            if moved {
                self.route_rows(target, target, None, params)?;
            }
        }
        Ok(QueryResult { row_count: total, rows, fields, command: verb.to_string() })
    }

    /// Điều kiện đầy đủ (mọi cấp tổ tiên) của một partition; `None` = DEFAULT ở cấp nào đó (không kiểm được rẻ).
    fn full_predicate(&self, leaf: &str) -> Result<Option<String>, String> {
        let mut conds = Vec::new();
        let mut cur = leaf.to_string();
        while let Some(po) = self.part_of(&cur) {
            if po.bound.eq_ignore_ascii_case("DEFAULT") {
                return Ok(None);
            }
            let by = self.part_by(&po.parent).ok_or("partition parent is not partitioned")?;
            conds.push(bound_predicate(&by, &po.bound)?);
            cur = po.parent;
        }
        Ok((!conds.is_empty()).then(|| conds.join(" AND ")))
    }

    /// UPDATE thẳng vào một partition: dòng ra khỏi khoảng → lỗi như PG.
    fn update_partition_checked(&mut self, sql: &str, target: &str, params: &[Value]) -> Result<QueryResult, String> {
        let _b = Busy::new();
        let r = self.run_with_partition_reads(sql, params)?;
        if let Some(pred) = self.full_predicate(target)? {
            let c = self.execute_statement(&format!("SELECT count(*) AS n FROM {} WHERE ({}) IS NOT TRUE", target, pred), &[])?;
            if c.rows.first().and_then(|x| x.get("n")).and_then(|v| v.as_i64()).unwrap_or(0) > 0 {
                return Err(format!("new row for relation \"{}\" violates partition constraint", target));
            }
        }
        Ok(r)
    }
}
