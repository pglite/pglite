//! Trigger: `CREATE [OR REPLACE] TRIGGER tên {BEFORE | AFTER} {INSERT | UPDATE [OF cột, ...] | DELETE | TRUNCATE}
//! [OR ...] ON bảng [FOR [EACH] {ROW | STATEMENT}] [WHEN (điều kiện)] EXECUTE {FUNCTION | PROCEDURE} hàm(đối số)`,
//! `DROP TRIGGER`, `ALTER TABLE ... {ENABLE | DISABLE} TRIGGER`.
//!
//! Định nghĩa lưu trong thuộc tính phụ của bảng (`TableMeta`, khoá `trigger:<tên>`, JSON): đi theo bảng khi đổi
//! tên / xoá / TRUNCATE, bản cũ bỏ qua. Hàm trigger là hàm PL/pgSQL `RETURNS trigger` (engine/routines.rs) với
//! `NEW`, `OLD`, `TG_OP`, `TG_NAME`, `TG_TABLE_NAME`, `TG_WHEN`, `TG_LEVEL`, `TG_ARGV`.
//!
//! Thứ tự như PostgreSQL: trigger cùng loại chạy theo tên; BEFORE ROW có thể sửa NEW hoặc bỏ dòng (`RETURN NULL`);
//! AFTER ROW chạy sau khi lệnh đã ghi xong mọi dòng (trước AFTER STATEMENT). Bảng không có trigger đi đường cũ
//! (chỉ một lần tra `meta`), nên không tốn thêm gì.
use super::executor::{find_top_level_keyword, json_to_value, split_comma_separated_tokens, split_function_args, Executor};
use super::routines::TriggerCall;
use crate::types::{QueryResult, Value};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub(crate) struct TriggerDef {
    pub name: String,
    /// `before` | `after` | `instead`
    pub timing: String,
    /// `insert` | `update` | `delete` | `truncate`
    pub events: Vec<String>,
    pub update_cols: Vec<String>,
    pub row: bool,
    pub when: Option<String>,
    pub function: String,
    pub args: Vec<String>,
    pub enabled: bool,
}

fn row_map(cols: &[crate::types::ColumnDef], row: &[Value]) -> serde_json::Map<String, serde_json::Value> {
    cols.iter().zip(row).map(|(c, v)| (c.name.clone(), super::executor::value_to_json_pub(v))).collect()
}

fn literal(v: &Value) -> String {
    match v {
        Value::Null => "NULL".into(),
        Value::Bool(b) => if *b { "TRUE" } else { "FALSE" }.into(),
        Value::Int(i) => if *i < 0 { format!("({})", i) } else { i.to_string() },
        Value::Float(f) => if *f < 0.0 { format!("({})", f) } else { format!("{}", f) },
        Value::Text(t) => format!("'{}'", t.replace('\'', "''")),
    }
}

/// Thay `NEW.cột` / `OLD.cột` trong điều kiện WHEN bằng hằng.
fn subst_records(cond: &str, cols: &[crate::types::ColumnDef], old: Option<&[Value]>, new: Option<&[Value]>) -> String {
    let mut out = cond.to_string();
    for (rec, vals) in [("new", new), ("old", old)] {
        for (i, c) in cols.iter().enumerate() {
            let pat = format!("{}.{}", rec, c.name.to_lowercase());
            let v = vals.and_then(|r| r.get(i)).cloned().unwrap_or(Value::Null);
            let mut res = String::new();
            let mut rest = out.as_str();
            loop {
                let low = rest.to_lowercase();
                let Some(p) = low.find(&pat) else { break };
                let before_ok = p == 0 || !rest.as_bytes()[p - 1].is_ascii_alphanumeric() && rest.as_bytes()[p - 1] != b'_';
                let after_ok = rest.as_bytes().get(p + pat.len()).map_or(true, |c| !c.is_ascii_alphanumeric() && *c != b'_');
                res.push_str(&rest[..p]);
                if before_ok && after_ok {
                    res.push_str(&literal(&v));
                } else {
                    res.push_str(&rest[p..p + pat.len()]);
                }
                rest = &rest[p + pat.len()..];
            }
            res.push_str(rest);
            out = res;
        }
    }
    out
}

impl Executor {
    /// Trigger đang bật của bảng (theo tên).
    pub(crate) fn table_triggers(&self, table: &str) -> Vec<TriggerDef> {
        let Some(t) = self.storage.get_table(table) else { return Vec::new() };
        if t.meta.is_empty() {
            return Vec::new();
        }
        t.meta.range("trigger:".to_string()..).take_while(|(k, _)| k.starts_with("trigger:")).filter_map(|(_, v)| serde_json::from_str::<TriggerDef>(v).ok()).filter(|d| d.enabled).collect()
    }

    /// Bảng có trigger cấp dòng cho sự kiện này.
    pub(crate) fn has_row_triggers(&self, table: &str, event: &str) -> bool {
        self.storage.get_table(table).is_some_and(|t| !t.meta.is_empty()) && self.table_triggers(table).iter().any(|d| d.row && d.events.iter().any(|e| e == event))
    }

    /// Chạy trigger cấp câu lệnh.
    pub(crate) fn fire_statement_triggers(&mut self, table: &str, timing: &str, event: &str) -> Result<(), String> {
        let defs: Vec<TriggerDef> = self.table_triggers(table).into_iter().filter(|d| !d.row && d.timing == timing && d.events.iter().any(|e| e == event)).collect();
        for d in defs {
            self.run_trigger(&d, table, event, None, None)?;
        }
        Ok(())
    }

    /// Chạy trigger cấp dòng `timing` cho một dòng. BEFORE: trả dòng mới (có thể đã sửa) hoặc `None` = bỏ dòng.
    pub(crate) fn fire_row_triggers(&mut self, table: &str, timing: &str, event: &str, old: Option<&[Value]>, new: Option<Vec<Value>>, set_cols: &[String]) -> Result<Option<Vec<Value>>, String> {
        let defs: Vec<TriggerDef> = self.table_triggers(table).into_iter().filter(|d| d.row && d.timing == timing && d.events.iter().any(|e| e == event)).collect();
        let mut cur = new;
        for d in defs {
            if event == "update" && !d.update_cols.is_empty() && !d.update_cols.iter().any(|c| set_cols.iter().any(|s| s.eq_ignore_ascii_case(c))) {
                continue;
            }
            if let Some(w) = &d.when {
                let cols = self.storage.get_table(table).map(|t| t.columns.clone()).unwrap_or_default();
                let cond = subst_records(w, &cols, old, cur.as_deref());
                let r = self.execute_statement(&format!("SELECT ({}) AS __w", cond), &[])?;
                if r.rows.first().and_then(|r| r.get("__w")).and_then(|v| v.as_bool()) != Some(true) {
                    continue;
                }
            }
            let out = self.run_trigger(&d, table, event, old, cur.as_deref())?;
            if timing == "before" && event != "delete" {
                match out {
                    // RETURN NULL: bỏ dòng (và các trigger sau)
                    Some(None) => return Ok(None),
                    Some(Some(rec)) => {
                        let t = self.storage.get_table(table).ok_or("table vanished")?;
                        let row: Vec<Value> = t
                            .columns
                            .iter()
                            .enumerate()
                            .map(|(i, c)| {
                                let v = rec.iter().find(|(k, _)| k.eq_ignore_ascii_case(&c.name)).map(|(_, v)| json_to_value(v)).unwrap_or(Value::Null);
                                crate::engine::coltypes::coerce_for_write(t.decl_type(i), super::executor::coerce_update_val(c, v))
                            })
                            .collect();
                        cur = Some(row);
                    }
                    None => {}
                }
            } else if timing == "before" && event == "delete" && matches!(out, Some(None)) {
                return Ok(None);
            }
        }
        Ok(cur.or_else(|| old.map(|o| o.to_vec())))
    }

    /// Gọi hàm của trigger; `Some(None)` = hàm trả NULL.
    fn run_trigger(&mut self, d: &TriggerDef, table: &str, event: &str, old: Option<&[Value]>, new: Option<&[Value]>) -> Result<Option<Option<serde_json::Map<String, serde_json::Value>>>, String> {
        let cols = self.storage.get_table(table).map(|t| t.columns.clone()).unwrap_or_default();
        let defs = self.routines_named(&d.function);
        let def = defs.into_iter().find(|r| r.returns == "trigger").ok_or_else(|| format!("function {}() does not exist", d.function))?;
        let call = TriggerCall {
            new: new.map(|r| row_map(&cols, r)),
            old: old.map(|r| row_map(&cols, r)),
            op: event.to_uppercase(),
            name: d.name.clone(),
            table: table.to_lowercase(),
            when: d.timing.to_uppercase(),
            level: if d.row { "ROW".into() } else { "STATEMENT".into() },
            argv: d.args.clone(),
            null_record: cols.iter().map(|c| (c.name.clone(), serde_json::Value::Null)).collect(),
        };
        let out = self.call_routine(&def, &[], Some(call))?;
        Ok(out.record)
    }

    /// `CREATE [OR REPLACE] [CONSTRAINT] TRIGGER ...`.
    pub(crate) fn create_trigger(&mut self, sql: &str) -> Result<QueryResult, String> {
        let t = sql.trim().trim_end_matches(';').trim();
        let up = t.to_ascii_uppercase();
        let tp = find_top_level_keyword(t, "TRIGGER").ok_or("syntax error at or near \"TRIGGER\"")?;
        let or_replace = up[..tp].contains("OR REPLACE");
        let rest = t[tp + 7..].trim_start();
        let name_len = rest.find(char::is_whitespace).unwrap_or(rest.len());
        let name = rest[..name_len].trim_matches('"').to_lowercase();
        let rest = rest[name_len..].trim_start();
        let ru = rest.to_ascii_uppercase();
        let (timing, rest) = if ru.starts_with("BEFORE") {
            ("before", &rest[6..])
        } else if ru.starts_with("AFTER") {
            ("after", &rest[5..])
        } else if ru.starts_with("INSTEAD OF") {
            ("instead", &rest[10..])
        } else {
            return Err("syntax error: expected BEFORE, AFTER or INSTEAD OF".into());
        };
        let on = find_top_level_keyword(rest, "ON").ok_or("syntax error: expected ON table")?;
        let mut events = Vec::new();
        let mut update_cols = Vec::new();
        for ev in rest[..on].split(" OR ").flat_map(|x| x.split(" or ")) {
            let e = ev.trim();
            let eu = e.to_ascii_uppercase();
            if eu.starts_with("UPDATE") {
                events.push("update".to_string());
                if let Some(p) = find_top_level_keyword(e, "OF") {
                    update_cols = split_comma_separated_tokens(&e[p + 2..]).into_iter().map(|c| c.trim().trim_matches('"').to_lowercase()).collect();
                }
            } else if eu == "INSERT" || eu == "DELETE" || eu == "TRUNCATE" {
                events.push(eu.to_lowercase());
            } else if !e.is_empty() {
                return Err(format!("syntax error at or near \"{}\"", e));
            }
        }
        let after_on = rest[on + 2..].trim_start();
        let tlen = after_on.find(char::is_whitespace).unwrap_or(after_on.len());
        let table = super::executor::strip_schema_keep_case(after_on[..tlen].trim()).to_lowercase();
        let tail = &after_on[tlen..];
        let tu = tail.to_ascii_uppercase();
        if self.storage.get_table(&table).is_none() {
            return Err(format!("relation \"{}\" does not exist", table));
        }
        let row = tu.contains("FOR EACH ROW") || tu.contains("FOR ROW");
        let when = find_top_level_keyword(tail, "WHEN").and_then(|p| {
            let w = tail[p + 4..].trim_start();
            let c = super::constraints::matching_paren(w, 0)?;
            w.starts_with('(').then(|| w[1..c].trim().to_string())
        });
        let ex = find_top_level_keyword(tail, "EXECUTE").ok_or("syntax error: expected EXECUTE FUNCTION")?;
        let fcall = tail[ex + 7..].trim_start();
        let fcall = fcall.strip_prefix("FUNCTION").or_else(|| fcall.strip_prefix("PROCEDURE")).or_else(|| fcall.strip_prefix("function")).or_else(|| fcall.strip_prefix("procedure")).unwrap_or(fcall).trim();
        let open = fcall.find('(').ok_or("syntax error: expected function call")?;
        let close = super::constraints::matching_paren(fcall, open).ok_or("syntax error: unbalanced parentheses")?;
        let function = super::executor::strip_schema_keep_case(fcall[..open].trim()).trim_matches('"').to_lowercase();
        let args: Vec<String> = split_function_args(&fcall[open + 1..close]).into_iter().filter(|a| !a.trim().is_empty()).map(|a| a.trim().trim_matches('\'').to_string()).collect();
        if !self.routines_named(&function).iter().any(|r| r.returns == "trigger") {
            return Err(format!("function {}() does not exist", function));
        }
        if timing == "instead" {
            return Err("INSTEAD OF triggers on views are not supported".into());
        }
        let key = format!("trigger:{}", name);
        if self.storage.get_table(&table).is_some_and(|t| t.meta.contains_key(&key)) && !or_replace {
            return Err(format!("trigger \"{}\" for relation \"{}\" already exists", name, table));
        }
        let def = TriggerDef { name, timing: timing.into(), events, update_cols, row, when, function, args, enabled: true };
        let json = serde_json::to_string(&def).map_err(|e| e.to_string())?;
        self.storage.log_schema_op(crate::storage::catalog::SchemaOp::TableMeta { table, key, value: Some(json) });
        self.plan_cache.clear();
        Ok(QueryResult { rows: vec![], row_count: 0, fields: vec![], command: "CREATE TRIGGER".to_string() })
    }

    /// `DROP TRIGGER [IF EXISTS] tên ON bảng [CASCADE | RESTRICT]`.
    pub(crate) fn drop_trigger(&mut self, sql: &str) -> Result<QueryResult, String> {
        let t = sql.trim().trim_end_matches(';').trim();
        let mut rest = t[12..].trim_start();
        let if_exists = rest.to_ascii_uppercase().starts_with("IF EXISTS ");
        if if_exists {
            rest = rest[10..].trim_start();
        }
        let on = find_top_level_keyword(rest, "ON").ok_or("syntax error: expected ON table")?;
        let name = rest[..on].trim().trim_matches('"').to_lowercase();
        let table = super::executor::strip_schema_keep_case(rest[on + 2..].split_whitespace().next().unwrap_or("")).to_lowercase();
        let key = format!("trigger:{}", name);
        let exists = self.storage.get_table(&table).is_some_and(|t| t.meta.contains_key(&key));
        if !exists {
            if if_exists {
                return Ok(QueryResult { rows: vec![], row_count: 0, fields: vec![], command: "DROP TRIGGER".to_string() });
            }
            return Err(format!("trigger \"{}\" for table \"{}\" does not exist", name, table));
        }
        self.storage.log_schema_op(crate::storage::catalog::SchemaOp::TableMeta { table, key, value: None });
        Ok(QueryResult { rows: vec![], row_count: 0, fields: vec![], command: "DROP TRIGGER".to_string() })
    }

    /// `ALTER TABLE t {ENABLE | DISABLE} TRIGGER {tên | ALL | USER}`.
    pub(crate) fn alter_trigger_state(&mut self, table: &str, which: &str, enable: bool) -> Result<(), String> {
        let w = which.trim().trim_matches('"').to_lowercase();
        let defs: Vec<(String, TriggerDef)> = self
            .storage
            .get_table(table)
            .map(|t| t.meta.iter().filter(|(k, _)| k.starts_with("trigger:")).filter_map(|(k, v)| serde_json::from_str::<TriggerDef>(v).ok().map(|d| (k.clone(), d))).collect())
            .unwrap_or_default();
        let mut found = false;
        for (k, mut d) in defs {
            if w == "all" || w == "user" || d.name == w {
                found = true;
                d.enabled = enable;
                let json = serde_json::to_string(&d).map_err(|e| e.to_string())?;
                self.storage.log_schema_op(crate::storage::catalog::SchemaOp::TableMeta { table: table.to_string(), key: k, value: Some(json) });
            }
        }
        if !found && w != "all" && w != "user" {
            return Err(format!("trigger \"{}\" for table \"{}\" does not exist", w, table));
        }
        Ok(())
    }

    /// `UPDATE` trên bảng có trigger cấp dòng: tìm dòng và giá trị mới bằng SELECT, chạy BEFORE / AFTER ROW
    /// quanh từng dòng, ghi bằng `update_row_logged` (WAL, undo, generated như UPDATE thường).
    pub(crate) fn update_with_triggers(&mut self, table_part: &str, set_part: &str, from_list: Option<&str>, where_clause: Option<&str>, returning: Option<&str>, params: &[Value]) -> Result<QueryResult, String> {
        let (table, alias) = table_and_alias(table_part);
        let key = table.to_lowercase();
        let cols = self.storage.get_table(&key).ok_or_else(|| format!("relation \"{}\" does not exist", table))?.columns.clone();
        let decls = self.storage.get_table(&key).map(|t| t.decl_types_vec()).unwrap_or_default();
        let set_part = self.resolve_uncorrelated_set_subqueries(set_part, params)?;
        let mut assigns: Vec<(usize, Option<String>)> = Vec::new();
        let mut items: Vec<String> = cols.iter().enumerate().map(|(i, c)| format!("{}.\"{}\" AS \"__t{}\"", alias, c.name, i)).collect();
        let mut set_cols = Vec::new();
        for (j, a) in split_comma_separated_tokens(&set_part).into_iter().enumerate() {
            let eq = a.find('=').ok_or("Invalid assignment in SET clause")?;
            let col = a[..eq].trim();
            let col = col.rsplit('.').next().unwrap_or(col).trim().trim_matches('"');
            let idx = cols.iter().position(|c| c.name.eq_ignore_ascii_case(col)).ok_or_else(|| format!("column \"{}\" of relation \"{}\" does not exist", col, table))?;
            set_cols.push(cols[idx].name.clone());
            let expr = a[eq + 1..].trim();
            if expr.eq_ignore_ascii_case("DEFAULT") {
                assigns.push((idx, None));
            } else {
                items.push(format!("({}) AS \"__v{}\"", expr, j));
                assigns.push((idx, Some(format!("__v{}", j))));
            }
        }
        let target = if alias.eq_ignore_ascii_case(&table) { table.clone() } else { format!("{} {}", table, alias) };
        let mut q = format!("SELECT {} FROM {}", items.join(", "), target);
        if let Some(f) = from_list {
            q.push_str(", ");
            q.push_str(f);
        }
        if let Some(w) = where_clause {
            q.push_str(" WHERE ");
            q.push_str(w);
        }
        let res = self.execute_statement(&q, params)?;
        let targets = self.locate_rows_by_values(&key, &res.rows);
        self.fire_statement_triggers(&key, "before", "update")?;
        let referenced = self.is_referenced(&key);
        let mut updated = Vec::new();
        let mut old_rows = Vec::new();
        let mut after: Vec<(Vec<Value>, Vec<Value>)> = Vec::new();
        let mut returned = Vec::new();
        for (row_idx, obj) in targets {
            let old = self.storage.tables[&key].rows[row_idx].clone();
            let mut new = old.clone();
            for (c, k) in &assigns {
                let v = match k {
                    Some(k) => {
                        let j = obj.get(k).unwrap_or(&serde_json::Value::Null);
                        match j {
                            serde_json::Value::Object(_) | serde_json::Value::Array(_) => Value::text(j.to_string()),
                            other => json_to_value(other),
                        }
                    }
                    None => cols[*c].default_value.as_deref().map(super::executor::eval_default_value).unwrap_or(Value::Null),
                };
                new[*c] = crate::engine::coltypes::coerce_for_write(decls.get(*c).and_then(|d| d.as_ref()), super::executor::coerce_update_val(&cols[*c], v));
            }
            let Some(new) = self.fire_row_triggers(&key, "before", "update", Some(&old), Some(new), &set_cols)? else { continue };
            let updates: Vec<(usize, Value)> = new.iter().enumerate().filter(|(i, v)| old.get(*i) != Some(v)).map(|(i, v)| (i, v.clone())).collect();
            if referenced {
                old_rows.push((row_idx, old.clone()));
            }
            self.update_row_logged(&key, row_idx, updates);
            updated.push(row_idx);
            let final_row = self.storage.tables[&key].rows[row_idx].clone();
            if let Some(r) = returning {
                let t = &self.storage.tables[&key];
                returned.push(super::executor::returning_row(t, &final_row, r, params));
            }
            after.push((old, final_row));
        }
        if !updated.is_empty() {
            let all: Vec<usize> = (0..cols.len()).collect();
            self.verify_rows(&key, &updated, Some(&all))?;
            if referenced {
                self.fk_on_parent_update(&key, &old_rows)?;
            }
        }
        for (old, new) in after {
            self.fire_row_triggers(&key, "after", "update", Some(&old), Some(new), &set_cols)?;
        }
        self.fire_statement_triggers(&key, "after", "update")?;
        let fields = returning.map(|r| super::executor::returning_fields(&self.storage.tables[&key], r, &returned)).unwrap_or_default();
        Ok(QueryResult { row_count: updated.len(), rows: returned, fields, command: "UPDATE".to_string() })
    }

    /// `DELETE` trên bảng có trigger cấp dòng.
    pub(crate) fn delete_with_triggers(&mut self, table_part: &str, using_list: Option<&str>, where_clause: Option<&str>, returning: Option<&str>, params: &[Value]) -> Result<QueryResult, String> {
        let (table, alias) = table_and_alias(table_part);
        let key = table.to_lowercase();
        let cols = self.storage.get_table(&key).ok_or_else(|| format!("relation \"{}\" does not exist", table))?.columns.clone();
        let items: Vec<String> = cols.iter().enumerate().map(|(i, c)| format!("{}.\"{}\" AS \"__t{}\"", alias, c.name, i)).collect();
        let target = if alias.eq_ignore_ascii_case(&table) { table.clone() } else { format!("{} {}", table, alias) };
        let mut q = format!("SELECT {} FROM {}", items.join(", "), target);
        if let Some(u) = using_list {
            q.push_str(", ");
            q.push_str(u);
        }
        if let Some(w) = where_clause {
            q.push_str(" WHERE ");
            q.push_str(w);
        }
        let res = self.execute_statement(&q, params)?;
        let targets = self.locate_rows_by_values(&key, &res.rows);
        self.fire_statement_triggers(&key, "before", "delete")?;
        let mut deleted = Vec::new();
        let mut after = Vec::new();
        let mut returned = Vec::new();
        for (row_idx, _) in targets {
            let old = self.storage.tables[&key].rows[row_idx].clone();
            if self.fire_row_triggers(&key, "before", "delete", Some(&old), None, &[])?.is_none() {
                continue;
            }
            if let Some(r) = returning {
                let t = &self.storage.tables[&key];
                returned.push(super::executor::returning_row(t, &old, r, params));
            }
            if self.delete_row_logged(&key, row_idx) {
                deleted.push(row_idx);
                after.push(old);
            }
        }
        if !deleted.is_empty() && self.is_referenced(&key) {
            self.fk_on_parent_delete(&key, &deleted)?;
        }
        for old in after {
            self.fire_row_triggers(&key, "after", "delete", Some(&old), None, &[])?;
        }
        self.fire_statement_triggers(&key, "after", "delete")?;
        let fields = returning.map(|r| super::executor::returning_fields(&self.storage.tables[&key], r, &returned)).unwrap_or_default();
        Ok(QueryResult { row_count: deleted.len(), rows: returned, fields, command: "DELETE".to_string() })
    }

    /// Dòng sống của bảng ứng với các dòng kết quả (`__t{i}`): theo khoá chính, không thì theo toàn bộ giá trị.
    fn locate_rows_by_values(&self, table: &str, rows: &[serde_json::Value]) -> Vec<(usize, serde_json::Map<String, serde_json::Value>)> {
        let t = &self.storage.tables[table];
        let mut out = Vec::new();
        let mut taken = std::collections::HashSet::new();
        for r in rows {
            let Some(obj) = r.as_object() else { continue };
            let vals: Vec<serde_json::Value> = (0..t.columns.len()).map(|i| obj.get(&format!("__t{}", i)).cloned().unwrap_or(serde_json::Value::Null)).collect();
            let found = match t.pk_col_idx.filter(|_| !t.has_dup_pk) {
                Some(pk) if !vals[pk].is_null() => t.live_row_with_pk(&json_to_value(&vals[pk]), None),
                _ => (0..t.rows.len()).find(|&i| !t.is_deleted[i] && !taken.contains(&i) && t.rows[i].iter().zip(&vals).all(|(cell, j)| super::executor::value_to_json_pub(cell) == *j)),
            };
            if let Some(idx) = found {
                if taken.insert(idx) {
                    out.push((idx, obj.clone()));
                }
            }
        }
        out
    }
}

/// `t`, `t x`, `t AS x` -> (bảng, alias)
fn table_and_alias(s: &str) -> (String, String) {
    let parts: Vec<&str> = s.split_whitespace().collect();
    let table = super::executor::strip_schema_keep_case(parts.first().copied().unwrap_or(""));
    let alias = match parts.as_slice() {
        [_, a] => a.trim_matches('"').to_string(),
        [_, as_kw, a] if as_kw.eq_ignore_ascii_case("AS") => a.trim_matches('"').to_string(),
        _ => table.clone(),
    };
    (table, alias)
}
