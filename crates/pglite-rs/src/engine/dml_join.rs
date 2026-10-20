//! `UPDATE t SET ... FROM ... WHERE ...` và `DELETE FROM t USING ... WHERE ...`.
//!
//! Các cặp dòng khớp được tìm bằng chính bộ máy SELECT (JOIN qua danh sách FROM/USING), rồi ánh xạ
//! về dòng của bảng đích theo khoá chính (hoặc toàn bộ giá trị dòng nếu bảng không có khoá). Mọi
//! thay đổi đi qua `update_row_logged` / `delete_row_logged`: WAL, undo và ràng buộc như UPDATE/DELETE
//! thường. Trước đây phần `FROM ...` bị nối vào biểu thức SET và ghi chuỗi rác vào mọi dòng.
use super::executor::{coerce_update_val, eval_default_value, find_top_level_keyword, json_to_value, returning_fields, returning_row, split_comma_separated_tokens, strip_schema_keep_case, Executor};
use crate::types::{QueryResult, Value};

fn json_cell(v: &serde_json::Value) -> Value {
    match v {
        serde_json::Value::Object(_) | serde_json::Value::Array(_) => Value::text(v.to_string()),
        other => json_to_value(other),
    }
}

/// `t`, `t x`, `t AS x` -> (bảng, alias dùng để tham chiếu)
fn table_and_alias(s: &str) -> (String, String) {
    let parts: Vec<&str> = s.split_whitespace().collect();
    let table = strip_schema_keep_case(parts.first().copied().unwrap_or(""));
    let alias = match parts.as_slice() {
        [_, a] => a.trim_matches('"').to_string(),
        [_, as_kw, a] if as_kw.eq_ignore_ascii_case("AS") => a.trim_matches('"').to_string(),
        _ => table.clone(),
    };
    (table, alias)
}

impl Executor {
    /// Dòng (còn sống) của bảng đích ứng với một dòng kết quả SELECT (cột `__t{i}`).
    fn locate_target_rows(&self, table: &str, rows: &[serde_json::Value]) -> Vec<(usize, serde_json::Map<String, serde_json::Value>)> {
        let t = &self.storage.tables[&table.to_lowercase()];
        let mut out = Vec::new();
        let mut taken = std::collections::HashSet::new();
        for r in rows {
            let Some(obj) = r.as_object() else { continue };
            let vals: Vec<serde_json::Value> = (0..t.columns.len()).map(|i| obj.get(&format!("__t{}", i)).cloned().unwrap_or(serde_json::Value::Null)).collect();
            let found = match t.pk_col_idx.filter(|_| !t.has_dup_pk) {
                Some(pk) if !vals[pk].is_null() => t.live_row_with_pk(&json_cell(&vals[pk]), None),
                _ => (0..t.rows.len()).find(|&i| {
                    !t.is_deleted[i] && !taken.contains(&i) && t.rows[i].iter().zip(&vals).all(|(cell, j)| super::executor::value_to_json_pub(cell) == *j)
                }),
            };
            if let Some(idx) = found {
                // Một dòng đích khớp nhiều dòng nguồn: chỉ dùng lần khớp đầu (như PostgreSQL)
                if taken.insert(idx) {
                    out.push((idx, obj.clone()));
                }
            }
        }
        out
    }

    fn target_select(&self, table: &str, alias: &str, extra: &[String], from_list: &str, where_clause: Option<&str>) -> String {
        let t = &self.storage.tables[&table.to_lowercase()];
        let mut items: Vec<String> = t.columns.iter().enumerate().map(|(i, c)| format!("{}.\"{}\" AS \"__t{}\"", alias, c.name, i)).collect();
        items.extend(extra.iter().cloned());
        let target = if alias.eq_ignore_ascii_case(table) { table.to_string() } else { format!("{} {}", table, alias) };
        let mut sql = format!("SELECT {} FROM {}, {}", items.join(", "), target, from_list);
        if let Some(w) = where_clause {
            sql.push_str(" WHERE ");
            sql.push_str(w);
        }
        sql
    }

    /// `head` = phần giữa UPDATE và SET; `set_part` = các phép gán; `from_list`, `where`, `returning`.
    pub(crate) fn handle_update_from(&mut self, head: &str, set_part: &str, from_list: &str, where_clause: Option<&str>, returning: Option<&str>, params: &[Value]) -> Result<QueryResult, String> {
        let (table, alias) = table_and_alias(head);
        let key = table.to_lowercase();
        let cols = self.storage.get_table(&key).ok_or_else(|| format!("Table {} not found", table))?.columns.clone();
        let decls = self.storage.get_table(&key).map(|t| t.decl_types_vec()).unwrap_or_default();
        let mut assigns: Vec<(usize, Option<String>)> = Vec::new();
        let mut extra = Vec::new();
        for (j, a) in split_comma_separated_tokens(set_part).into_iter().enumerate() {
            let eq = a.find('=').ok_or("Invalid assignment in SET clause")?;
            let col = a[..eq].trim();
            let col = col.rsplit('.').next().unwrap_or(col).trim().trim_matches('"');
            let idx = cols.iter().position(|c| c.name.eq_ignore_ascii_case(col)).ok_or_else(|| format!("column \"{}\" of relation \"{}\" does not exist", col, table))?;
            let expr = a[eq + 1..].trim();
            if expr.eq_ignore_ascii_case("DEFAULT") {
                assigns.push((idx, None));
            } else {
                extra.push(format!("({}) AS \"__v{}\"", expr, j));
                assigns.push((idx, Some(format!("__v{}", j))));
            }
        }
        let select = self.target_select(&table, &alias, &extra, from_list, where_clause);
        let res = self.execute_statement(&select, params)?;
        let targets = self.locate_target_rows(&table, &res.rows);
        let referenced = self.is_referenced(&key);
        let mut updated = Vec::new();
        let mut old_rows = Vec::new();
        let mut returned = Vec::new();
        for (row_idx, obj) in targets {
            let updates: Vec<(usize, Value)> = assigns
                .iter()
                .map(|(c, k)| {
                    let v = match k {
                        Some(k) => json_cell(obj.get(k).unwrap_or(&serde_json::Value::Null)),
                        None => cols[*c].default_value.as_deref().map(eval_default_value).unwrap_or(Value::Null),
                    };
                    (*c, crate::engine::coltypes::coerce_for_write(decls.get(*c).and_then(|d| d.as_ref()), coerce_update_val(&cols[*c], v)))
                })
                .collect();
            if referenced {
                old_rows.push((row_idx, self.storage.tables[&key].rows[row_idx].clone()));
            }
            self.update_row_logged(&key, row_idx, updates);
            updated.push(row_idx);
            if let Some(r) = returning {
                let t = &self.storage.tables[&key];
                returned.push(returning_row(t, &t.rows[row_idx], r, params));
            }
        }
        let changed: Vec<usize> = assigns.iter().map(|(c, _)| *c).collect();
        if !updated.is_empty() {
            self.verify_rows(&key, &updated, Some(&changed))?;
            if referenced {
                self.fk_on_parent_update(&key, &old_rows)?;
            }
        }
        let fields = returning.map(|r| returning_fields(&self.storage.tables[&key], r, &returned)).unwrap_or_default();
        Ok(QueryResult { row_count: updated.len(), rows: returned, fields, command: "UPDATE".to_string() })
    }

    /// `head` = phần sau `DELETE FROM` tới `USING`.
    pub(crate) fn handle_delete_using(&mut self, head: &str, using_list: &str, where_clause: Option<&str>, returning: Option<&str>, params: &[Value]) -> Result<QueryResult, String> {
        let (table, alias) = table_and_alias(head);
        let key = table.to_lowercase();
        if self.storage.get_table(&key).is_none() {
            return Err(format!("Table {} not found", table));
        }
        let select = self.target_select(&table, &alias, &[], using_list, where_clause);
        let res = self.execute_statement(&select, params)?;
        let targets = self.locate_target_rows(&table, &res.rows);
        let mut deleted = Vec::new();
        let mut returned = Vec::new();
        for (row_idx, _) in targets {
            if let Some(r) = returning {
                let t = &self.storage.tables[&key];
                returned.push(returning_row(t, &t.rows[row_idx], r, params));
            }
            if self.delete_row_logged(&key, row_idx) {
                deleted.push(row_idx);
            }
        }
        if !deleted.is_empty() && self.is_referenced(&key) {
            self.fk_on_parent_delete(&key, &deleted)?;
        }
        let fields = returning.map(|r| returning_fields(&self.storage.tables[&key], r, &returned)).unwrap_or_default();
        Ok(QueryResult { row_count: deleted.len(), rows: returned, fields, command: "DELETE".to_string() })
    }
}

/// Tách `UPDATE` thành (head, set, from, where, returning) nếu có `FROM` ở cấp ngoài cùng sau SET.
pub(crate) fn split_update_from(sql: &str) -> Option<(&str, &str, &str, Option<&str>, Option<&str>)> {
    let set_idx = find_top_level_keyword(sql, "SET")?;
    let after_set = &sql[set_idx + 3..];
    let from_rel = find_top_level_keyword(after_set, "FROM")?;
    let set_part = &after_set[..from_rel];
    let rest = &after_set[from_rel + 4..];
    let ret = find_top_level_keyword(rest, "RETURNING");
    let body = &rest[..ret.unwrap_or(rest.len())];
    let (from_list, where_clause) = match find_top_level_keyword(body, "WHERE") {
        Some(w) => (&body[..w], Some(body[w + 5..].trim())),
        None => (body, None),
    };
    Some((sql[6..set_idx].trim(), set_part.trim(), from_list.trim(), where_clause, ret.map(|r| rest[r + 9..].trim())))
}

/// Tách `DELETE FROM t USING ...` thành (head, using, where, returning).
pub(crate) fn split_delete_using(sql: &str) -> Option<(&str, &str, Option<&str>, Option<&str>)> {
    let from = find_top_level_keyword(sql, "FROM")?;
    let after = &sql[from + 4..];
    let using = find_top_level_keyword(after, "USING")?;
    let rest = &after[using + 5..];
    let ret = find_top_level_keyword(rest, "RETURNING");
    let body = &rest[..ret.unwrap_or(rest.len())];
    let (list, where_clause) = match find_top_level_keyword(body, "WHERE") {
        Some(w) => (&body[..w], Some(body[w + 5..].trim())),
        None => (body, None),
    };
    Some((after[..using].trim(), list.trim(), where_clause, ret.map(|r| rest[r + 9..].trim())))
}
