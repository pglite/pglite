//! `MERGE INTO đích [AS a] USING nguồn [AS s] ON điều_kiện WHEN ... THEN ... [RETURNING ...]` (PostgreSQL 15–17):
//! `WHEN MATCHED [AND c] THEN UPDATE SET ... | DELETE | DO NOTHING`,
//! `WHEN NOT MATCHED [BY TARGET] [AND c] THEN INSERT [(cột)] VALUES (...) | INSERT DEFAULT VALUES | DO NOTHING`,
//! `WHEN NOT MATCHED BY SOURCE [AND c] THEN UPDATE SET ... | DELETE | DO NOTHING`, `merge_action()` trong RETURNING.
//!
//! Cách làm: nguồn được tính một lần thành bảng tạm (kèm số thứ tự dòng `__rn`); cặp dòng khớp, điều kiện của
//! từng nhánh và giá trị SET được tính bằng chính bộ máy SELECT (`đích JOIN nguồn ON ...`). Ghi dữ liệu đi qua
//! `update_row_logged` / `delete_row_logged` / một câu INSERT nhiều dòng như UPDATE / DELETE / INSERT thường
//! (WAL, undo, ràng buộc, khoá ngoại). Một dòng đích bị UPDATE/DELETE hai lần → lỗi 21000 như PostgreSQL.
use super::executor::{coerce_update_val, eval_default_value, find_top_level_keyword, json_to_value, returning_fields, returning_row, split_comma_separated_tokens, Executor};
use crate::types::{QueryResult, Value};

#[derive(Debug, Clone, Copy, PartialEq)]
enum When {
    Matched,
    NotMatchedTarget,
    NotMatchedSource,
}

#[derive(Debug, Clone)]
enum Action {
    Update(Vec<(String, String)>),
    Delete,
    Insert(Option<Vec<String>>, Option<Vec<String>>),
    Nothing,
}

#[derive(Debug, Clone)]
struct Clause {
    when: When,
    cond: Option<String>,
    action: Action,
}

fn json_cell(v: &serde_json::Value) -> Value {
    match v {
        serde_json::Value::Object(_) | serde_json::Value::Array(_) => Value::text(v.to_string()),
        other => json_to_value(other),
    }
}

fn sql_literal(v: &serde_json::Value) -> String {
    match v {
        serde_json::Value::Null => "NULL".into(),
        serde_json::Value::Bool(b) => if *b { "TRUE" } else { "FALSE" }.into(),
        serde_json::Value::Number(n) => {
            let t = n.to_string();
            if t.starts_with('-') { format!("({})", t) } else { t }
        }
        serde_json::Value::String(s) => format!("'{}'", s.replace('\'', "''")),
        other => format!("'{}'", other.to_string().replace('\'', "''")),
    }
}

/// `x [AS] alias` → (x, alias)
fn split_alias(s: &str) -> (String, Option<String>) {
    let s = s.trim();
    if s.starts_with('(') {
        let close = super::constraints::matching_paren(s, 0).unwrap_or(s.len() - 1);
        let rest = s[close + 1..].trim();
        let rest = rest.strip_prefix("AS ").or_else(|| rest.strip_prefix("as ")).unwrap_or(rest).trim();
        return (s[..close + 1].to_string(), (!rest.is_empty()).then(|| rest.to_string()));
    }
    let parts: Vec<&str> = s.split_whitespace().collect();
    match parts.as_slice() {
        [t] => (t.to_string(), None),
        [t, a] => (t.to_string(), Some(a.trim_matches('"').to_string())),
        [t, as_kw, a] if as_kw.eq_ignore_ascii_case("AS") => (t.to_string(), Some(a.trim_matches('"').to_string())),
        _ => (s.to_string(), None),
    }
}

/// Đọc các nhánh `WHEN ...`.
fn parse_clauses(s: &str) -> Result<Vec<Clause>, String> {
    let mut out = Vec::new();
    let mut rest = s.trim();
    while !rest.is_empty() {
        let up = rest.to_ascii_uppercase();
        if !up.starts_with("WHEN ") {
            return Err(format!("syntax error at or near \"{}\"", rest.split_whitespace().next().unwrap_or("")));
        }
        let body = &rest[5..];
        let then = find_top_level_keyword(body, "THEN").ok_or("syntax error at or near \"WHEN\"")?;
        let head = body[..then].trim();
        let hu = head.to_ascii_uppercase();
        let (when, after) = if hu.starts_with("NOT MATCHED BY SOURCE") {
            (When::NotMatchedSource, &head[21..])
        } else if hu.starts_with("NOT MATCHED BY TARGET") {
            (When::NotMatchedTarget, &head[21..])
        } else if hu.starts_with("NOT MATCHED") {
            (When::NotMatchedTarget, &head[11..])
        } else if hu.starts_with("MATCHED") {
            (When::Matched, &head[7..])
        } else {
            return Err(format!("syntax error at or near \"{}\"", head));
        };
        let after = after.trim();
        let cond = if after.to_ascii_uppercase().starts_with("AND ") { Some(after[4..].trim().to_string()) } else { None };
        let tail = &body[then + 4..];
        let next = find_top_level_keyword(tail, "WHEN").unwrap_or(tail.len());
        let act = tail[..next].trim();
        let au = act.to_ascii_uppercase();
        let action = if au.starts_with("UPDATE SET ") {
            let mut sets = Vec::new();
            for a in split_comma_separated_tokens(&act[11..]) {
                let eq = a.find('=').ok_or("syntax error in MERGE UPDATE SET")?;
                let col = a[..eq].trim();
                sets.push((col.rsplit('.').next().unwrap_or(col).trim().trim_matches('"').to_string(), a[eq + 1..].trim().to_string()));
            }
            Action::Update(sets)
        } else if au == "DELETE" {
            Action::Delete
        } else if au == "DO NOTHING" {
            Action::Nothing
        } else if au.starts_with("INSERT") {
            let mut r = act[6..].trim();
            let mut cols = None;
            if r.starts_with('(') {
                let c = super::constraints::matching_paren(r, 0).ok_or("syntax error in MERGE INSERT")?;
                cols = Some(split_comma_separated_tokens(&r[1..c]).into_iter().map(|x| x.trim().trim_matches('"').to_string()).collect());
                r = r[c + 1..].trim();
            }
            let ru = r.to_ascii_uppercase();
            if ru.starts_with("OVERRIDING") {
                let v = find_top_level_keyword(r, "VALUE").map(|p| p + 5).unwrap_or(r.len());
                r = r[v..].trim();
            }
            let ru = r.to_ascii_uppercase();
            let vals = if ru == "DEFAULT VALUES" {
                None
            } else if ru.starts_with("VALUES") {
                let v = r[6..].trim();
                if !v.starts_with('(') {
                    return Err("syntax error in MERGE INSERT".into());
                }
                let c = super::constraints::matching_paren(v, 0).ok_or("syntax error in MERGE INSERT")?;
                Some(split_comma_separated_tokens(&v[1..c]).into_iter().map(|x| x.trim().to_string()).collect())
            } else {
                return Err("syntax error in MERGE INSERT".into());
            };
            Action::Insert(cols, vals)
        } else {
            return Err(format!("syntax error at or near \"{}\"", act.split_whitespace().next().unwrap_or("")));
        };
        match (&when, &action) {
            (When::NotMatchedTarget, Action::Update(_) | Action::Delete) => return Err("syntax error: UPDATE/DELETE not allowed in WHEN NOT MATCHED".into()),
            (When::Matched | When::NotMatchedSource, Action::Insert(..)) => return Err("syntax error: INSERT not allowed in WHEN MATCHED".into()),
            _ => {}
        }
        out.push(Clause { when, cond, action });
        rest = tail[next..].trim();
    }
    Ok(out)
}

/// Thay `merge_action()` trong RETURNING bằng hằng (mục đứng riêng không alias giữ tên cột `merge_action`).
fn returning_for(ret: &str, action: &str) -> String {
    let items: Vec<String> = split_comma_separated_tokens(ret)
        .into_iter()
        .map(|it| if it.trim().eq_ignore_ascii_case("merge_action()") { format!("'{}' AS merge_action", action) } else { replace_merge_action(&it, action) })
        .collect();
    items.join(", ")
}

fn replace_merge_action(ret: &str, action: &str) -> String {
    let mut out = String::with_capacity(ret.len());
    let low = ret.to_ascii_lowercase();
    let mut i = 0;
    while let Some(p) = low[i..].find("merge_action()") {
        out.push_str(&ret[i..i + p]);
        out.push_str(&format!("'{}'", action));
        i += p + "merge_action()".len();
    }
    out.push_str(&ret[i..]);
    out
}

impl Executor {
    pub(crate) fn handle_merge(&mut self, sql: &str, params: &[Value]) -> Result<QueryResult, String> {
        let body = sql.trim().trim_end_matches(';');
        let into = find_top_level_keyword(body, "INTO").ok_or("syntax error at or near \"MERGE\"")?;
        let using = find_top_level_keyword(body, "USING").ok_or("syntax error: MERGE requires USING")?;
        let after_using = &body[using + 5..];
        let on = find_top_level_keyword(after_using, "ON").ok_or("syntax error: MERGE requires ON")?;
        let after_on = &after_using[on + 2..];
        let first_when = find_top_level_keyword(after_on, "WHEN").ok_or("syntax error: MERGE requires WHEN")?;
        let cond = after_on[..first_when].trim().to_string();
        let mut clauses_src = &after_on[first_when..];
        let mut returning: Option<String> = None;
        if let Some(r) = find_top_level_keyword(clauses_src, "RETURNING") {
            returning = Some(clauses_src[r + 9..].trim().to_string());
            clauses_src = &clauses_src[..r];
        }
        let clauses = parse_clauses(clauses_src)?;

        let (target, talias) = split_alias(&body[into + 4..using]);
        let target = super::executor::strip_schema_keep_case(&target);
        let key = target.to_lowercase();
        let talias = talias.unwrap_or_else(|| target.clone());
        let (cols, decls) = {
            let t = self.storage.get_table(&key).ok_or_else(|| format!("relation \"{}\" does not exist", target))?;
            (t.columns.clone(), t.decl_types_vec())
        };
        let (src_text, salias) = split_alias(&after_using[..on]);
        let salias = salias.unwrap_or_else(|| src_text.rsplit('.').next().unwrap_or(&src_text).trim_matches('"').to_string());

        // 1. nguồn → bảng tạm (cột của nguồn + __rn)
        let src_sql = if src_text.starts_with('(') { format!("SELECT * FROM {} AS {}", src_text, salias) } else { format!("SELECT * FROM {}", src_text) };
        let src = self.execute_statement(&src_sql, params)?;
        static SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let tmp = format!("__merge_src_{}", SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed));
        let mut defs: Vec<crate::types::ColumnDef> = src
            .fields
            .iter()
            .map(|f| crate::types::ColumnDef { name: f.name.clone(), data_type: crate::types::DataType::from_sql_str(&f.data_type), is_primary_key: false, is_nullable: true, default_value: None, comment: None })
            .collect();
        defs.push(crate::types::ColumnDef { name: "__rn".into(), data_type: crate::types::DataType::BigInt, is_primary_key: false, is_nullable: true, default_value: None, comment: None });
        let mut st = crate::storage::table::Table::new(tmp.clone(), defs);
        st.pk_col_idx = None;
        for (i, r) in src.rows.iter().enumerate() {
            let mut row: Vec<Value> = src.fields.iter().map(|f| r.get(&f.name).map(json_cell).unwrap_or(Value::Null)).collect();
            row.push(Value::Int(i as i64));
            st.insert(row);
        }
        self.storage.tables.insert(tmp.clone(), st);
        let res = self.merge_with_source(&key, &target, &talias, &tmp, &salias, src.rows.len(), &cond, &clauses, returning.as_deref(), &cols, &decls, params);
        self.storage.tables.remove(&tmp);
        res
    }

    #[allow(clippy::too_many_arguments)]
    fn merge_with_source(
        &mut self,
        key: &str,
        target: &str,
        talias: &str,
        tmp: &str,
        salias: &str,
        nsrc: usize,
        cond: &str,
        clauses: &[Clause],
        returning: Option<&str>,
        cols: &[crate::types::ColumnDef],
        decls: &[Option<crate::engine::coltypes::DeclType>],
        params: &[Value],
    ) -> Result<QueryResult, String> {
        let tref = if talias.eq_ignore_ascii_case(target) { target.to_string() } else { format!("{} {}", target, talias) };
        let sref = format!("{} {}", tmp, salias);
        // biểu thức cần tính cho các nhánh loại `w`: điều kiện (`__c{k}`) và giá trị SET / INSERT (`__v{k}_{j}`)
        let extras_for = |w: When| -> Vec<String> {
            let mut extra = Vec::new();
            for (k, c) in clauses.iter().enumerate().filter(|(_, c)| c.when == w) {
                if let Some(cd) = &c.cond {
                    extra.push(format!("(CASE WHEN {} THEN 1 ELSE 0 END) AS \"__c{}\"", cd, k));
                }
                let vals: Vec<&String> = match &c.action {
                    Action::Update(sets) => sets.iter().map(|(_, e)| e).collect(),
                    Action::Insert(_, Some(vals)) => vals.iter().collect(),
                    _ => vec![],
                };
                for (j, e) in vals.into_iter().enumerate() {
                    if !e.eq_ignore_ascii_case("DEFAULT") {
                        extra.push(format!("({}) AS \"__v{}_{}\"", e, k, j));
                    }
                }
            }
            extra
        };
        let tcols: Vec<String> = cols.iter().enumerate().map(|(i, c)| format!("{}.\"{}\" AS \"__t{}\"", talias, c.name, i)).collect();
        let has = |w: When| clauses.iter().any(|c| c.when == w);
        let pick = |obj: &serde_json::Map<String, serde_json::Value>, w: When| -> Option<usize> {
            clauses.iter().enumerate().find(|(k, c)| c.when == w && (c.cond.is_none() || obj.get(&format!("__c{}", k)).and_then(|v| v.as_i64()) == Some(1))).map(|(k, _)| k)
        };
        // dòng đích còn sống ứng với các giá trị `__t{i}` (khoá chính, không thì toàn bộ dòng)
        let locate = |this: &Self, obj: &serde_json::Map<String, serde_json::Value>| -> Option<usize> {
            let t = &this.storage.tables[key];
            let vals: Vec<serde_json::Value> = (0..t.columns.len()).map(|i| obj.get(&format!("__t{}", i)).cloned().unwrap_or(serde_json::Value::Null)).collect();
            match t.pk_col_idx.filter(|_| !t.has_dup_pk) {
                Some(pk) if !vals[pk].is_null() => t.live_row_with_pk(&json_cell(&vals[pk]), None),
                _ => (0..t.rows.len()).find(|&i| !t.is_deleted[i] && t.rows[i].iter().zip(&vals).all(|(cell, j)| super::executor::value_to_json_pub(cell) == *j)),
            }
        };

        // 2. cặp khớp
        let mut matched_src = vec![false; nsrc];
        let mut matched_tgt: std::collections::HashSet<usize> = std::collections::HashSet::new();
        // (nhánh, dòng đích, đối tượng kết quả)
        let mut plan: Vec<(usize, Option<usize>, serde_json::Map<String, serde_json::Value>)> = Vec::new();
        {
            let mut items = tcols.clone();
            items.push(format!("{}.\"__rn\" AS \"__rn\"", salias));
            items.extend(extras_for(When::Matched));
            let q = format!("SELECT {} FROM {} JOIN {} ON {}", items.join(", "), tref, sref, cond);
            let res = self.execute_statement(&q, params)?;
            let mut touched: std::collections::HashSet<usize> = std::collections::HashSet::new();
            for r in &res.rows {
                let Some(obj) = r.as_object() else { continue };
                if let Some(rn) = obj.get("__rn").and_then(|v| v.as_u64()) {
                    if let Some(m) = matched_src.get_mut(rn as usize) {
                        *m = true;
                    }
                }
                let Some(row_idx) = locate(self, obj) else { continue };
                matched_tgt.insert(row_idx);
                if let Some(k) = pick(obj, When::Matched) {
                    if !matches!(clauses[k].action, Action::Nothing) && !touched.insert(row_idx) {
                        return Err("MERGE command cannot affect row a second time".into());
                    }
                    plan.push((k, Some(row_idx), obj.clone()));
                }
            }
        }
        // 3. dòng nguồn không khớp → INSERT
        if has(When::NotMatchedTarget) && matched_src.iter().any(|m| !m) {
            let mut items = vec![format!("{}.\"__rn\" AS \"__rn\"", salias)];
            items.extend(extras_for(When::NotMatchedTarget));
            let q = format!("SELECT {} FROM {}", items.join(", "), sref);
            let res = self.execute_statement(&q, params)?;
            for r in &res.rows {
                let Some(obj) = r.as_object() else { continue };
                let rn = obj.get("__rn").and_then(|v| v.as_u64()).unwrap_or(0) as usize;
                if matched_src.get(rn).copied().unwrap_or(true) {
                    continue;
                }
                if let Some(k) = pick(obj, When::NotMatchedTarget) {
                    plan.push((k, None, obj.clone()));
                }
            }
        }
        // 4. dòng đích không khớp nguồn nào
        if has(When::NotMatchedSource) {
            let mut items = tcols.clone();
            items.extend(extras_for(When::NotMatchedSource));
            let q = format!("SELECT {} FROM {}", items.join(", "), tref);
            let res = self.execute_statement(&q, params)?;
            for r in &res.rows {
                let Some(obj) = r.as_object() else { continue };
                let Some(row_idx) = locate(self, obj) else { continue };
                if matched_tgt.contains(&row_idx) {
                    continue;
                }
                if let Some(k) = pick(obj, When::NotMatchedSource) {
                    plan.push((k, Some(row_idx), obj.clone()));
                }
            }
        }

        // 5. thực hiện
        let referenced = self.is_referenced(key);
        let mut updated = Vec::new();
        let mut changed_cols: Vec<usize> = Vec::new();
        let mut old_rows = Vec::new();
        let mut deleted = Vec::new();
        let mut returned: Vec<serde_json::Value> = Vec::new();
        let mut inserts: Vec<(Option<Vec<String>>, Vec<String>)> = Vec::new();
        let mut count = 0usize;
        for (k, row_idx, obj) in &plan {
            match &clauses[*k].action {
                Action::Nothing => {}
                Action::Update(sets) => {
                    let row_idx = row_idx.ok_or("internal MERGE error")?;
                    let mut updates = Vec::new();
                    for (j, (col, e)) in sets.iter().enumerate() {
                        let ci = cols.iter().position(|c| c.name.eq_ignore_ascii_case(col)).ok_or_else(|| format!("column \"{}\" of relation \"{}\" does not exist", col, target))?;
                        let v = if e.eq_ignore_ascii_case("DEFAULT") {
                            cols[ci].default_value.as_deref().map(eval_default_value).unwrap_or(Value::Null)
                        } else {
                            json_cell(obj.get(&format!("__v{}_{}", k, j)).unwrap_or(&serde_json::Value::Null))
                        };
                        if !changed_cols.contains(&ci) {
                            changed_cols.push(ci);
                        }
                        updates.push((ci, crate::engine::coltypes::coerce_for_write(decls.get(ci).and_then(|d| d.as_ref()), coerce_update_val(&cols[ci], v))));
                    }
                    if referenced {
                        old_rows.push((row_idx, self.storage.tables[key].rows[row_idx].clone()));
                    }
                    self.update_row_logged(key, row_idx, updates);
                    updated.push(row_idx);
                    count += 1;
                    if let Some(r) = returning {
                        let t = &self.storage.tables[key];
                        returned.push(returning_row(t, &t.rows[row_idx], &returning_for(r, "UPDATE"), params));
                    }
                }
                Action::Delete => {
                    let row_idx = row_idx.ok_or("internal MERGE error")?;
                    if let Some(r) = returning {
                        let t = &self.storage.tables[key];
                        returned.push(returning_row(t, &t.rows[row_idx], &returning_for(r, "DELETE"), params));
                    }
                    if self.delete_row_logged(key, row_idx) {
                        deleted.push(row_idx);
                        count += 1;
                    }
                }
                Action::Insert(icols, vals) => {
                    let lits: Vec<String> = match vals {
                        None => vec![],
                        Some(vs) => vs
                            .iter()
                            .enumerate()
                            .map(|(j, e)| if e.eq_ignore_ascii_case("DEFAULT") { "DEFAULT".to_string() } else { sql_literal(obj.get(&format!("__v{}_{}", k, j)).unwrap_or(&serde_json::Value::Null)) })
                            .collect(),
                    };
                    inserts.push((icols.clone(), lits));
                }
            }
        }
        if !updated.is_empty() {
            self.verify_rows(key, &updated, Some(&changed_cols))?;
            if referenced {
                self.fk_on_parent_update(key, &old_rows)?;
            }
        }
        if !deleted.is_empty() && referenced {
            self.fk_on_parent_delete(key, &deleted)?;
        }
        // INSERT: gom theo danh sách cột, một câu nhiều dòng
        let mut groups: Vec<(Option<Vec<String>>, Vec<Vec<String>>)> = Vec::new();
        for (c, l) in inserts {
            match groups.iter_mut().find(|g| g.0 == c && g.1.first().map(|x| x.is_empty()) == Some(l.is_empty())) {
                Some(g) => g.1.push(l),
                None => groups.push((c, vec![l])),
            }
        }
        for (c, rows) in groups {
            let cols_sql = c.map(|c| format!(" ({})", c.iter().map(|x| format!("\"{}\"", x)).collect::<Vec<_>>().join(", "))).unwrap_or_default();
            let ret = returning.map(|r| format!(" RETURNING {}", returning_for(r, "INSERT"))).unwrap_or_default();
            let q = if rows.first().is_some_and(|r| r.is_empty()) {
                // INSERT DEFAULT VALUES cho từng dòng
                let mut last = None;
                for _ in &rows {
                    last = Some(self.execute_statement(&format!("INSERT INTO {} DEFAULT VALUES{}", target, ret), params)?);
                    if let Some(r) = &last {
                        returned.extend(r.rows.iter().cloned());
                    }
                }
                count += rows.len();
                let _ = last;
                continue;
            } else {
                format!("INSERT INTO {}{} VALUES {}{}", target, cols_sql, rows.iter().map(|r| format!("({})", r.join(", "))).collect::<Vec<_>>().join(", "), ret)
            };
            let r = self.execute_statement(&q, params)?;
            count += rows.len();
            returned.extend(r.rows.into_iter());
        }
        let fields = returning.map(|r| returning_fields(&self.storage.tables[key], &returning_for(r, "UPDATE"), &returned)).unwrap_or_default();
        Ok(QueryResult { row_count: count, rows: returned, fields, command: "MERGE".to_string() })
    }
}
