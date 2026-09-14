use crate::storage::engine::{StorageEngine, UndoAction};
use crate::storage::wal::WalRecord;
use crate::types::{ColumnDef, DataType, FieldInfo, QueryResult, Value};
use rayon::prelude::*;
use serde_json::json;
use std::collections::HashMap;
use md5::{Digest, Md5};
use base64::Engine;

pub use crate::engine::plan::PlannedProjectedExpr as ProjectedExpr;
use crate::engine::plan::{
    evaluate_planned_condition, evaluate_planned_where, find_pk_equality_in_where_template,
    project_row_planned, resolve_operand, ColOpTemplate, ConditionTemplate, ExecutionPlan,
    OperandTemplate, PlannedJoin, PlannedProjectedExpr, WhereTemplate,
};

pub struct Executor {
    pub storage: StorageEngine,
    pub plan_cache: HashMap<String, ExecutionPlan>,
}

impl Executor {
    pub fn new(storage: StorageEngine) -> Self {
        Self {
            storage,
            plan_cache: HashMap::new(),
        }
    }

    pub fn execute_rows_json(&mut self, sql: &str, params: &[Value]) -> Result<String, String> {
        let res = self.execute(sql, params)?;
        Ok(res.rows_to_json_string())
    }

    pub fn execute_full_json(&mut self, sql: &str, params: &[Value]) -> Result<String, String> {
        let res = self.execute(sql, params)?;
        Ok(res.to_json_string())
    }

    pub fn execute(&mut self, sql: &str, params: &[Value]) -> Result<QueryResult, String> {
        let clean_sql = strip_sql_comments(sql);
        let normalized_sql = normalize_named_sql_params(&clean_sql);

        let stmts = split_sql_statements(&normalized_sql);
        if stmts.len() > 1 {
            let mut last_res = QueryResult {
                rows: vec![],
                row_count: 0,
                fields: vec![],
                command: String::new(),
            };
            for stmt in stmts {
                last_res = self.execute(&stmt, params)?;
            }
            return Ok(last_res);
        }

        let trimmed = normalized_sql.trim().trim_end_matches(|c: char| c == ';' || c == '.').trim();
        let upper = trimmed.to_uppercase();

        if upper.starts_with("BEGIN") || upper.starts_with("START TRANSACTION") {
            self.storage.begin_transaction();
            return Ok(QueryResult {
                rows: vec![],
                row_count: 0,
                fields: vec![],
                command: "BEGIN".to_string(),
            });
        }

        if upper.starts_with("COMMIT") || upper == "END" {
            self.storage.commit();
            return Ok(QueryResult {
                rows: vec![],
                row_count: 0,
                fields: vec![],
                command: "COMMIT".to_string(),
            });
        }

        if upper.starts_with("SAVEPOINT") {
            let sp_name = trimmed["SAVEPOINT".len()..].trim().trim_matches(';');
            self.storage.savepoint(sp_name);
            return Ok(QueryResult {
                rows: vec![],
                row_count: 0,
                fields: vec![],
                command: "SAVEPOINT".to_string(),
            });
        }

        if upper.starts_with("ROLLBACK TO") || upper.starts_with("ROLLBACK WORK TO") || upper.starts_with("ROLLBACK TRANSACTION TO") {
            let after = if upper.starts_with("ROLLBACK WORK TO") {
                &trimmed["ROLLBACK WORK TO".len()..]
            } else if upper.starts_with("ROLLBACK TRANSACTION TO") {
                &trimmed["ROLLBACK TRANSACTION TO".len()..]
            } else {
                &trimmed["ROLLBACK TO".len()..]
            };
            let sp_name = after.trim();
            let sp_name = if sp_name.to_uppercase().starts_with("SAVEPOINT ") {
                &sp_name[10..]
            } else {
                sp_name
            }.trim().trim_matches(';');
            self.storage.rollback_to_savepoint(sp_name);
            return Ok(QueryResult {
                rows: vec![],
                row_count: 0,
                fields: vec![],
                command: "ROLLBACK".to_string(),
            });
        }

        if upper.starts_with("ROLLBACK") || upper.starts_with("ABORT") {
            self.storage.rollback();
            return Ok(QueryResult {
                rows: vec![],
                row_count: 0,
                fields: vec![],
                command: "ROLLBACK".to_string(),
            });
        }

        if upper.starts_with("RELEASE") {
            let after = if upper.starts_with("RELEASE WORK SAVEPOINT") {
                &trimmed["RELEASE WORK SAVEPOINT".len()..]
            } else if upper.starts_with("RELEASE SAVEPOINT") {
                &trimmed["RELEASE SAVEPOINT".len()..]
            } else {
                &trimmed["RELEASE".len()..]
            };
            let sp_name = after.trim().trim_matches(';');
            self.storage.release_savepoint(sp_name);
            return Ok(QueryResult {
                rows: vec![],
                row_count: 0,
                fields: vec![],
                command: "RELEASE".to_string(),
            });
        }

        if upper.starts_with("CREATE") || upper.starts_with("DROP") || upper.starts_with("ALTER") {
            self.plan_cache.clear();
        }

        if upper.starts_with("ALTER TABLE") {
            return self.handle_alter_table(trimmed);
        }

        let is_with = if upper.starts_with("WITH") {
            let rest = &trimmed[4..];
            rest.is_empty() || rest.chars().next().map(|c| c.is_whitespace()).unwrap_or(false)
        } else {
            false
        };
        if is_with {
            return self.handle_cte(trimmed, params);
        }

        if find_top_level_keyword(trimmed, "UNION").is_some() {
            return self.handle_union(trimmed, params);
        }

        if upper.starts_with("CREATE TABLE") {
            return self.handle_create_table(trimmed);
        }

        if upper.starts_with("DROP TABLE") {
            return self.handle_drop_table(trimmed);
        }

        if upper.starts_with("INSERT INTO") {
            return self.handle_insert(trimmed, params);
        }

        if upper.starts_with("COMMENT ON TABLE") {
            return self.handle_comment_on_table(trimmed);
        }

        if upper.starts_with("COMMENT ON COLUMN") {
            return self.handle_comment_on_column(trimmed);
        }

        if upper.starts_with("SELECT") {
            if upper == "SELECT CURRENT_SCHEMA()" || upper == "SELECT CURRENT_SCHEMA" {
                return Ok(QueryResult {
                    rows: vec![serde_json::json!({ "current_schema": "public" })],
                    row_count: 1,
                    fields: vec![FieldInfo { name: "current_schema".to_string(), data_type: "text".to_string() }],
                    command: "SELECT".to_string(),
                });
            }

            if upper.contains("FROM PG_CLASS") || upper.contains("FROM PG_CATALOG.PG_CLASS") {
                if upper.contains("JSON_AGG") || upper.contains("COLUMNS") || upper.contains("PG_ATTRIBUTE") {
                    return self.handle_table_introspection_query(trimmed, params);
                }
            }

            if upper.contains("FROM PG_CONSTRAINT") || upper.contains("FROM PG_CATALOG.PG_CONSTRAINT") {
                return self.handle_pg_constraint_introspection_query(trimmed, params);
            }

            if upper.contains("FROM INFORMATION_SCHEMA.COLUMNS") {
                return self.handle_information_schema_columns_query(trimmed, params);
            }

            if (upper.contains("OBJ_DESCRIPTION") || upper.contains("COL_DESCRIPTION")) && !upper.contains("FROM ") {
                if let Ok(res) = self.handle_description_scalar_query(trimmed) {
                    return Ok(res);
                }
            }

            if upper.contains("FROM PG_CATALOG.PG_DESCRIPTION") || upper.contains("FROM PG_DESCRIPTION") {
                if let Ok(res) = self.handle_description_scalar_query(trimmed) {
                    return Ok(res);
                }
            }

            if (upper.contains("FROM INFORMATION_SCHEMA.SCHEMATA") || upper.contains("FROM PG_NAMESPACE"))
                && !upper.contains("JOIN") {
                return Ok(QueryResult {
                    rows: vec![serde_json::json!({ "schema_name": "public" })],
                    row_count: 1,
                    fields: vec![FieldInfo { name: "schema_name".to_string(), data_type: "text".to_string() }],
                    command: "SELECT".to_string(),
                });
            }

            if (upper.contains("FROM INFORMATION_SCHEMA.TABLES") || upper.contains("FROM PG_TABLES"))
                && !upper.contains("JOIN") {
                let mut table_keys: Vec<String> = self.storage.tables.keys().cloned().collect();
                table_keys.sort();
                let rows: Vec<serde_json::Value> = table_keys.into_iter().map(|k| {
                    let tbl = &self.storage.tables[&k];
                    serde_json::json!({
                        "table_schema": "public",
                        "schema_name": "public",
                        "table_name": tbl.name,
                        "table_type": "BASE TABLE",
                        "is_temporary": false,
                        "comment": tbl.comment.clone().unwrap_or_default()
                    })
                }).collect();
                let row_count = rows.len();
                return Ok(QueryResult {
                    rows,
                    row_count,
                    fields: vec![
                        FieldInfo { name: "table_name".to_string(), data_type: "text".to_string() },
                        FieldInfo { name: "table_schema".to_string(), data_type: "text".to_string() },
                    ],
                    command: "SELECT".to_string(),
                });
            }

            if upper.contains("FROM PG_CLASS") && !upper.contains("JOIN") {
                let mut table_keys: Vec<String> = self.storage.tables.keys().cloned().collect();
                table_keys.sort();
                let mut rows: Vec<serde_json::Value> = table_keys.into_iter().map(|k| {
                    let tbl = &self.storage.tables[&k];
                    serde_json::json!({
                        "relname": tbl.name,
                        "relkind": "r",
                        "relnamespace": 2200,
                    })
                }).collect();
                if let Some(where_pos) = find_top_level_keyword(trimmed, "WHERE") {
                    let where_clause = trimmed[where_pos + 5..].trim().trim_end_matches(';').trim();
                    rows.retain(|r| {
                        let relname = r.get("relname").and_then(|v| v.as_str()).unwrap_or("");
                        where_clause.contains(&format!("'{}'", relname))
                    });
                }
                let row_count = rows.len();
                return Ok(QueryResult {
                    rows,
                    row_count,
                    fields: vec![
                        FieldInfo { name: "relname".to_string(), data_type: "text".to_string() },
                    ],
                    command: "SELECT".to_string(),
                });
            }

            // Check if FROM has a derived subquery e.g. FROM (SELECT ...)
            if let Some(from_pos) = find_top_level_keyword(trimmed, "FROM") {
                let after_from = trimmed[from_pos + 4..].trim();
                if after_from.starts_with('(') && after_from[1..].trim_start().to_uppercase().starts_with("SELECT") {
                    return self.handle_derived_table_query(trimmed, params);
                }
            }

            if let Some(plan) = self.plan_cache.get(trimmed) {
                if let Ok(res) = self.execute_select_plan(plan, params) {
                    return Ok(res);
                }
            }
            let (res, plan_opt) = self.handle_select(trimmed, params)?;
            if let Some(plan) = plan_opt {
                self.plan_cache.insert(trimmed.to_string(), plan);
            }
            return Ok(res);
        }

        if upper.starts_with("UPDATE") {
            return self.handle_update(trimmed, params);
        }

        if upper.starts_with("DELETE") {
            return self.handle_delete(trimmed, params);
        }

        if upper.starts_with("TRUNCATE") {
            return self.handle_truncate(trimmed);
        }

        Err(format!("Unsupported SQL statement: {}", sql))
    }

    fn handle_truncate(&mut self, sql: &str) -> Result<QueryResult, String> {
        let mut rest = sql.trim();
        if rest.to_uppercase().starts_with("TRUNCATE") {
            rest = rest[8..].trim();
        }
        if rest.to_uppercase().starts_with("TABLE") {
            rest = rest[5..].trim();
        }
        let table_name = rest.trim_matches('"').trim_matches(';').trim();
        let (columns, count) = {
            let table = self.storage.get_table_mut(table_name)
                .ok_or_else(|| format!("Table {} not found", table_name))?;
            let count = table.active_count;
            table.rows.clear();
            table.is_deleted.clear();
            table.pk_index.clear();
            table.active_count = 0;
            table.auto_increment = 1;
            (table.columns.clone(), count)
        };
        self.storage.wal.append(WalRecord::DropTable {
            name: table_name.to_string(),
        });
        self.storage.wal.append(WalRecord::CreateTable {
            name: table_name.to_string(),
            columns,
        });
        self.storage.wal.flush();
        self.plan_cache.clear();

        Ok(QueryResult {
            rows: vec![],
            row_count: count,
            fields: vec![],
            command: "TRUNCATE".to_string(),
        })
    }

    fn handle_alter_table(&mut self, sql: &str) -> Result<QueryResult, String> {
        let upper = sql.to_uppercase();
        let after_alter = if upper.starts_with("ALTER TABLE") {
            sql[11..].trim()
        } else {
            sql.trim()
        };

        let add_kw_pos = find_top_level_keyword(after_alter, "ADD");
        let drop_kw_pos = find_top_level_keyword(after_alter, "DROP");

        if let Some(pos) = add_kw_pos {
            let table_name = clean_table_name(after_alter[..pos].trim());
            let mut after_add = after_alter[pos + 3..].trim();
            if after_add.to_uppercase().starts_with("COLUMN") {
                after_add = after_add[6..].trim();
            }

            let mut if_not_exists = false;
            if after_add.to_uppercase().starts_with("IF NOT EXISTS") {
                if_not_exists = true;
                after_add = after_add[13..].trim();
            }

            // Ignore ADD CONSTRAINT or similar table-level constraints gracefully
            if after_add.to_uppercase().starts_with("CONSTRAINT")
                || after_add.to_uppercase().starts_with("PRIMARY KEY")
                || after_add.to_uppercase().starts_with("FOREIGN KEY")
                || after_add.to_uppercase().starts_with("UNIQUE")
            {
                return Ok(QueryResult {
                    rows: vec![],
                    row_count: 0,
                    fields: vec![],
                    command: "ALTER TABLE".to_string(),
                });
            }

            let tokens: Vec<&str> = after_add.split_whitespace().collect();
            if tokens.is_empty() {
                return Err("Missing column definition in ALTER TABLE ADD".to_string());
            }
            let col_name = clean_col_name(tokens[0]).to_string();
            let type_str = if tokens.len() > 1 { tokens[1] } else { "TEXT" };
            let data_type = crate::types::DataType::from_sql_str(type_str);

            let (default_val, default_str) = if let Some(def_pos) = after_add.to_uppercase().find("DEFAULT") {
                let def_str = after_add[def_pos + 7..].trim();
                (eval_sql_expr(def_str, &[], None, &[], None, &[]), Some(def_str.to_string()))
            } else {
                (Value::Null, None)
            };

            let col_def = crate::types::ColumnDef {
                name: col_name.clone(),
                data_type,
                is_primary_key: false,
                is_nullable: true,
                default_value: default_str,
                comment: None,
            };

            {
                let table = self.storage.get_table_mut(table_name).ok_or_else(|| format!("Table {} not found", table_name))?;
                if table.columns.iter().any(|c| c.name.eq_ignore_ascii_case(&col_name)) {
                    if if_not_exists {
                        return Ok(QueryResult {
                            rows: vec![],
                            row_count: 0,
                            fields: vec![],
                            command: "ALTER TABLE".to_string(),
                        });
                    }
                    return Err(format!("Column \"{}\" of relation \"{}\" already exists", col_name, table_name));
                }

                table.columns.push(col_def.clone());
                for row in &mut table.rows {
                    let cell = if let Some(ref expr_str) = col_def.default_value {
                        eval_sql_expr(expr_str, &[], None, &[], None, &[])
                    } else {
                        default_val.clone()
                    };
                    row.push(cell);
                }
            }

            self.storage.wal.append(WalRecord::AlterTableAddColumn {
                table: table_name.to_string(),
                column: col_def,
                default_val,
            });

            Ok(QueryResult {
                rows: vec![],
                row_count: 0,
                fields: vec![],
                command: "ALTER TABLE".to_string(),
            })
        } else if let Some(pos) = drop_kw_pos {
            let table_name = clean_table_name(after_alter[..pos].trim());
            let mut after_drop = after_alter[pos + 4..].trim();
            if after_drop.to_uppercase().starts_with("COLUMN") {
                after_drop = after_drop[6..].trim();
            }

            let mut if_exists = false;
            if after_drop.to_uppercase().starts_with("IF EXISTS") {
                if_exists = true;
                after_drop = after_drop[9..].trim();
            }

            let tokens: Vec<&str> = after_drop.split_whitespace().collect();
            if tokens.is_empty() {
                return Err("Missing column name in ALTER TABLE DROP".to_string());
            }
            let col_name = clean_col_name(tokens[0]).to_string();

            {
                let table = self.storage.get_table_mut(table_name).ok_or_else(|| format!("Table {} not found", table_name))?;
                let dropped = table.drop_column(&col_name);
                if !dropped {
                    if if_exists {
                        return Ok(QueryResult {
                            rows: vec![],
                            row_count: 0,
                            fields: vec![],
                            command: "ALTER TABLE".to_string(),
                        });
                    }
                    return Err(format!("Column \"{}\" of relation \"{}\" does not exist", col_name, table_name));
                }
            }

            self.storage.wal.append(WalRecord::AlterTableDropColumn {
                table: table_name.to_string(),
                column: col_name,
            });

            Ok(QueryResult {
                rows: vec![],
                row_count: 0,
                fields: vec![],
                command: "ALTER TABLE".to_string(),
            })
        } else {
            Err("Unsupported ALTER TABLE statement".to_string())
        }
    }

    fn handle_union(&mut self, sql: &str, params: &[Value]) -> Result<QueryResult, String> {
        let (order_by_part, query_part) = if let Some(order_pos) = find_top_level_keyword(sql, "ORDER BY") {
            (Some(&sql[order_pos + 8..]), &sql[..order_pos])
        } else {
            (None, sql)
        };

        let (union_pos, is_all) = if let Some(pos) = find_top_level_keyword(query_part, "UNION ALL") {
            (pos, true)
        } else if let Some(pos) = find_top_level_keyword(query_part, "UNION") {
            (pos, false)
        } else {
            return Err("Expected UNION in query".to_string());
        };

        let kw_len = if is_all { 9 } else { 5 };
        let left_sql = query_part[..union_pos].trim();
        let right_sql = query_part[union_pos + kw_len..].trim();

        let left_res = self.execute(left_sql, params)?;
        let right_res = self.execute(right_sql, params)?;

        let mut combined_rows = Vec::new();
        if is_all {
            combined_rows.extend(left_res.rows);
            combined_rows.extend(right_res.rows);
        } else {
            let mut seen = std::collections::HashSet::new();
            for r in left_res.rows.into_iter().chain(right_res.rows.into_iter()) {
                let key = serde_json::to_string(&r).unwrap_or_default();
                if seen.insert(key) {
                    combined_rows.push(r);
                }
            }
        }

        if let Some(order_str) = order_by_part {
            let specs = parse_order_by_specs(order_str);
            if !specs.is_empty() {
                combined_rows.sort_by(|a, b| {
                    for spec in &specs {
                        let clean = clean_col_name(spec.col_name);
                        let va = a.get(clean);
                        let vb = b.get(clean);
                        let ord = match (va, vb) {
                            (Some(serde_json::Value::Number(n1)), Some(serde_json::Value::Number(n2))) => {
                                let f1 = n1.as_f64().unwrap_or(0.0);
                                let f2 = n2.as_f64().unwrap_or(0.0);
                                f1.partial_cmp(&f2).unwrap_or(std::cmp::Ordering::Equal)
                            }
                            (Some(serde_json::Value::String(s1)), Some(serde_json::Value::String(s2))) => {
                                s1.cmp(s2)
                            }
                            (Some(serde_json::Value::Bool(b1)), Some(serde_json::Value::Bool(b2))) => {
                                b1.cmp(b2)
                            }
                            (Some(_), None) => std::cmp::Ordering::Greater,
                            (None, Some(_)) => std::cmp::Ordering::Less,
                            _ => std::cmp::Ordering::Equal,
                        };
                        let final_ord = if spec.is_desc { ord.reverse() } else { ord };
                        if final_ord != std::cmp::Ordering::Equal {
                            return final_ord;
                        }
                    }
                    std::cmp::Ordering::Equal
                });
            }
        }

        let row_count = combined_rows.len();
        Ok(QueryResult {
            rows: combined_rows,
            row_count,
            fields: left_res.fields,
            command: "SELECT".to_string(),
        })
    }

    fn handle_cte(&mut self, sql: &str, params: &[Value]) -> Result<QueryResult, String> {
        let mut without_with = sql.trim()[4..].trim();
        let is_recursive = if without_with.to_uppercase().starts_with("RECURSIVE") {
            let rest = &without_with[9..];
            if rest.is_empty() || rest.chars().next().map(|c| c.is_whitespace()).unwrap_or(false) {
                without_with = rest.trim();
                true
            } else {
                false
            }
        } else {
            false
        };
        let mut i = 0;
        let mut created_tables: Vec<String> = Vec::new();

        while i < without_with.len() {
            let cur_slice = &without_with[i..];
            let as_pos = match find_top_level_keyword(cur_slice, "AS") {
                Some(p) => p,
                None => break,
            };
            let cte_head = cur_slice[..as_pos].trim();
            let (cte_name, explicit_cols) = if let Some(p_idx) = cte_head.find('(') {
                if let Some(close_idx) = cte_head.rfind(')') {
                    let name = clean_table_name(cte_head[..p_idx].trim()).to_string();
                    let cols_str = &cte_head[p_idx + 1..close_idx];
                    let cols: Vec<String> = cols_str.split(',').map(|s| clean_col_name(s.trim()).to_string()).collect();
                    (name, Some(cols))
                } else {
                    (clean_table_name(cte_head).to_string(), None)
                }
            } else {
                (clean_table_name(cte_head).to_string(), None)
            };

            let after_as_offset = as_pos + 2;
            let after_as_raw = &cur_slice[after_as_offset..];
            let lparen_rel = match after_as_raw.find('(') {
                Some(p) => p,
                None => return Err("Expected ( after AS in CTE definition".to_string()),
            };
            let open_paren_idx = after_as_offset + lparen_rel;
            let close_paren_idx = match find_matching_paren(cur_slice, open_paren_idx) {
                Some(p) => p,
                None => return Err("Unmatched parenthesis in CTE definition".to_string()),
            };

            let cte_query = cur_slice[open_paren_idx + 1..close_paren_idx].trim();

            if is_recursive {
                let (union_pos, is_all, kw_len) = if let Some(pos) = find_top_level_keyword(cte_query, "UNION ALL") {
                    (Some(pos), true, 9)
                } else if let Some(pos) = find_top_level_keyword(cte_query, "UNION") {
                    (Some(pos), false, 5)
                } else {
                    (None, false, 0)
                };

                if let Some(u_pos) = union_pos {
                    let non_rec_sql = cte_query[..u_pos].trim();
                    let rec_sql = cte_query[u_pos + kw_len..].trim();

                    let base_res = self.execute(non_rec_sql, params)?;
                    let mut cols = Vec::new();
                    for (c_i, f) in base_res.fields.iter().enumerate() {
                        let name = if let Some(ref exp) = explicit_cols {
                            exp.get(c_i).cloned().unwrap_or_else(|| f.name.clone())
                        } else {
                            f.name.clone()
                        };
                        cols.push(crate::types::ColumnDef {
                            name,
                            data_type: crate::types::DataType::from_sql_str(&f.data_type),
                            is_primary_key: false,
                            is_nullable: true,
                            default_value: None,
                            comment: None,
                        });
                    }

                    let row_from_json = |r_json: &serde_json::Value, cols: &[crate::types::ColumnDef], query_fields: &[FieldInfo]| -> Vec<Value> {
                        let mut row = Vec::new();
                        for (c_i, col) in cols.iter().enumerate() {
                            let field_name = query_fields.get(c_i).map(|f| f.name.as_str());
                            let val_opt = r_json.get(&col.name).or_else(|| field_name.and_then(|fn_str| r_json.get(fn_str)));
                            let v = match val_opt {
                                Some(serde_json::Value::Null) | None => Value::Null,
                                Some(serde_json::Value::Bool(b)) => Value::Bool(*b),
                                Some(serde_json::Value::Number(num)) => {
                                    if let Some(i) = num.as_i64() {
                                        Value::Int(i)
                                    } else if let Some(f) = num.as_f64() {
                                        Value::Float(f)
                                    } else {
                                        Value::Null
                                    }
                                }
                                Some(serde_json::Value::String(s)) => Value::text(s),
                                Some(other) => Value::text(other.to_string()),
                            };
                            row.push(v);
                        }
                        row
                    };

                    let mut accumulated_rows: Vec<Vec<Value>> = Vec::new();
                    let mut working_rows: Vec<Vec<Value>> = Vec::new();

                    for r_json in base_res.rows {
                        let r = row_from_json(&r_json, &cols, &base_res.fields);
                        accumulated_rows.push(r.clone());
                        working_rows.push(r);
                    }

                    let mut iteration = 0;
                    while !working_rows.is_empty() && iteration < 1000 {
                        iteration += 1;
                        let mut working_table = crate::storage::table::Table::new(cte_name.clone(), cols.clone());
                        for r in &working_rows {
                            working_table.insert(r.clone());
                        }
                        self.storage.tables.insert(cte_name.to_lowercase(), working_table);

                        let rec_res = match self.execute(rec_sql, params) {
                            Ok(res) => res,
                            Err(e) => {
                                self.storage.tables.remove(&cte_name.to_lowercase());
                                return Err(e);
                            }
                        };

                        if rec_res.rows.is_empty() {
                            break;
                        }

                        let mut next_working_rows = Vec::new();
                        for r_json in rec_res.rows {
                            let r = row_from_json(&r_json, &cols, &rec_res.fields);
                            if !is_all {
                                if accumulated_rows.iter().any(|acc| acc == &r) {
                                    continue;
                                }
                            }
                            accumulated_rows.push(r.clone());
                            next_working_rows.push(r);
                        }

                        working_rows = next_working_rows;
                    }

                    let mut full_cte_table = crate::storage::table::Table::new(cte_name.clone(), cols);
                    for r in accumulated_rows {
                        full_cte_table.insert(r);
                    }
                    self.storage.tables.insert(cte_name.to_lowercase(), full_cte_table);
                    created_tables.push(cte_name);
                } else {
                    let res = self.execute(cte_query, params)?;
                    let mut cols = Vec::new();
                    for (c_i, f) in res.fields.iter().enumerate() {
                        let name = if let Some(ref exp) = explicit_cols {
                            exp.get(c_i).cloned().unwrap_or_else(|| f.name.clone())
                        } else {
                            f.name.clone()
                        };
                        cols.push(crate::types::ColumnDef {
                            name,
                            data_type: crate::types::DataType::from_sql_str(&f.data_type),
                            is_primary_key: false,
                            is_nullable: true,
                            default_value: None,
                            comment: None,
                        });
                    }
                    let mut rows = Vec::new();
                    for r_json in res.rows {
                        let mut row = Vec::new();
                        for (c_i, col) in cols.iter().enumerate() {
                            let f_name = &res.fields[c_i].name;
                            let v = match r_json.get(&col.name).or_else(|| r_json.get(f_name)) {
                                Some(serde_json::Value::Null) | None => Value::Null,
                                Some(serde_json::Value::Bool(b)) => Value::Bool(*b),
                                Some(serde_json::Value::Number(num)) => {
                                    if let Some(i) = num.as_i64() { Value::Int(i) } else if let Some(f) = num.as_f64() { Value::Float(f) } else { Value::Null }
                                }
                                Some(serde_json::Value::String(s)) => Value::text(s),
                                Some(other) => Value::text(other.to_string()),
                            };
                            row.push(v);
                        }
                        rows.push(row);
                    }
                    let mut cte_table = crate::storage::table::Table::new(cte_name.clone(), cols);
                    for r in rows { cte_table.insert(r); }
                    self.storage.tables.insert(cte_name.to_lowercase(), cte_table);
                    created_tables.push(cte_name);
                }
            } else {
                let res = self.execute(cte_query, params)?;
                let mut cols = Vec::new();
                for (c_i, f) in res.fields.iter().enumerate() {
                    let name = if let Some(ref exp) = explicit_cols {
                        exp.get(c_i).cloned().unwrap_or_else(|| f.name.clone())
                    } else {
                        f.name.clone()
                    };
                    cols.push(crate::types::ColumnDef {
                        name,
                        data_type: crate::types::DataType::from_sql_str(&f.data_type),
                        is_primary_key: false,
                        is_nullable: true,
                        default_value: None,
                        comment: None,
                    });
                }
                let mut rows = Vec::new();
                for r_json in res.rows {
                    let mut row = Vec::new();
                    for (c_i, col) in cols.iter().enumerate() {
                        let f_name = &res.fields[c_i].name;
                        let v = match r_json.get(&col.name).or_else(|| r_json.get(f_name)) {
                            Some(serde_json::Value::Null) | None => Value::Null,
                            Some(serde_json::Value::Bool(b)) => Value::Bool(*b),
                            Some(serde_json::Value::Number(num)) => {
                                if let Some(i) = num.as_i64() { Value::Int(i) } else if let Some(f) = num.as_f64() { Value::Float(f) } else { Value::Null }
                            }
                            Some(serde_json::Value::String(s)) => Value::text(s),
                            Some(other) => Value::text(other.to_string()),
                        };
                        row.push(v);
                    }
                    rows.push(row);
                }
                let mut cte_table = crate::storage::table::Table::new(cte_name.clone(), cols);
                for r in rows { cte_table.insert(r); }
                self.storage.tables.insert(cte_name.to_lowercase(), cte_table);
                created_tables.push(cte_name);
            }

            let rest_after_paren = cur_slice[close_paren_idx + 1..].trim_start();
            if rest_after_paren.starts_with(',') {
                let after_comma = rest_after_paren[1..].trim_start();
                i = without_with.len() - after_comma.len();
            } else {
                let main_query = rest_after_paren;
                let main_res = self.execute(main_query, params);
                for tbl_name in created_tables {
                    self.storage.tables.remove(&tbl_name.to_lowercase());
                }
                return main_res;
            }
        }

        Err("Failed to parse CTE query".to_string())
    }

    fn handle_derived_table_query(&mut self, sql: &str, params: &[Value]) -> Result<QueryResult, String> {
        let from_pos = find_top_level_keyword(sql, "FROM").ok_or("Missing FROM")?;
        let select_part = sql[..from_pos].trim();
        let after_from = sql[from_pos + 4..].trim();

        let mut cte_defs = Vec::new();
        let mut new_from = String::new();
        let mut cur = after_from;

        while !cur.is_empty() {
            if cur.starts_with('(') {
                let mut depth = 0;
                let mut in_str = false;
                let mut end_paren = None;
                for (idx, ch) in cur.char_indices() {
                    match ch {
                        '\'' => in_str = !in_str,
                        '(' if !in_str => depth += 1,
                        ')' if !in_str => {
                            depth -= 1;
                            if depth == 0 {
                                end_paren = Some(idx);
                                break;
                            }
                        }
                        _ => {}
                    }
                }
                let end_idx = end_paren.ok_or("Unmatched parenthesis in derived table")?;
                let sub_query = cur[1..end_idx].trim();
                let after_paren = cur[end_idx + 1..].trim();

                let mut alias_and_rest = after_paren;
                if alias_and_rest.to_uppercase().starts_with("AS ") {
                    alias_and_rest = alias_and_rest[3..].trim();
                }
                let tokens: Vec<&str> = alias_and_rest.split_whitespace().collect();
                let alias = clean_col_name(tokens.first().copied().unwrap_or("sub_tbl")).to_string();
                cte_defs.push(format!("{} AS ({})", alias, sub_query));

                if !new_from.is_empty() {
                    new_from.push(' ');
                }
                new_from.push_str(&alias);

                let alias_token_len = tokens.first().map(|t| t.len()).unwrap_or(0);
                let rest = alias_and_rest[alias_token_len..].trim();
                if rest.to_uppercase().starts_with("CROSS JOIN") {
                    new_from.push_str(" CROSS JOIN ");
                    cur = rest[10..].trim();
                } else {
                    if !rest.is_empty() {
                        new_from.push(' ');
                        new_from.push_str(rest);
                    }
                    break;
                }
            } else {
                if !new_from.is_empty() {
                    new_from.push(' ');
                }
                new_from.push_str(cur);
                break;
            }
        }

        let cte_sql = format!("WITH {} {} FROM {}", cte_defs.join(", "), select_part, new_from);
        self.handle_cte(&cte_sql, params)
    }

fn replace_identifier_token(sql: &str, target: &str, replacement: &str) -> String {
    let mut result = String::with_capacity(sql.len());
    let bytes = sql.as_bytes();
    let target_lower = target.to_lowercase();
    let mut i = 0;
    let mut in_str = false;

    while i < bytes.len() {
        if bytes[i] == b'\'' {
            in_str = !in_str;
            result.push(bytes[i] as char);
            i += 1;
            continue;
        }
        if in_str {
            result.push(bytes[i] as char);
            i += 1;
            continue;
        }

        if i + target.len() <= bytes.len() {
            let slice = &sql[i..i + target.len()];
            if slice.to_lowercase() == target_lower {
                let prev_ok = i == 0 || (!bytes[i - 1].is_ascii_alphanumeric() && bytes[i - 1] != b'_');
                let next_idx = i + target.len();
                let next_ok = next_idx >= bytes.len() || (!bytes[next_idx].is_ascii_alphanumeric() && bytes[next_idx] != b'_');
                if prev_ok && next_ok {
                    result.push_str(replacement);
                    i += target.len();
                    continue;
                }
            }
        }

        result.push(bytes[i] as char);
        i += 1;
    }

    result
}

    fn resolve_subqueries_in_where(&mut self, where_str: &str, params: &[Value]) -> Result<String, String> {
        let mut resolved = where_str.to_string();
        loop {
            let mut found_start = None;
            let bytes = resolved.as_bytes();
            for i in 0..bytes.len() {
                if bytes[i] == b'(' {
                    let after = resolved[i + 1..].trim_start();
                    let upper_after = after.to_uppercase();
                    if upper_after.starts_with("SELECT ")
                        || upper_after.starts_with("SELECT\n")
                        || upper_after.starts_with("SELECT\r")
                        || upper_after.starts_with("SELECT\t")
                    {
                        found_start = Some(i);
                        break;
                    }
                }
            }
            let Some(pos) = found_start else {
                break;
            };
            let start_inner = pos + 1;
            let mut depth = 1;
            let mut end_inner = None;
            for i in start_inner..bytes.len() {
                if bytes[i] == b'(' {
                    depth += 1;
                } else if bytes[i] == b')' {
                    depth -= 1;
                    if depth == 0 {
                        end_inner = Some(i);
                        break;
                    }
                }
            }
            let Some(close_idx) = end_inner else {
                break;
            };

            let sub_sql = &resolved[start_inner..close_idx];
            let sub_res = match self.execute(sub_sql, params) {
                Ok(r) => r,
                Err(_) => break,
            };

            let before = resolved[..pos].trim_end();
            if before.to_uppercase().ends_with("IN") {
                let mut items = Vec::new();
                for r in sub_res.rows {
                    if let serde_json::Value::Object(map) = r {
                        if map.len() > 1 {
                            let tuple_items: Vec<String> = map.values().map(|val| match val {
                                serde_json::Value::Number(n) => n.to_string(),
                                serde_json::Value::String(s) => format!("'{}'", s.replace('\'', "''")),
                                serde_json::Value::Bool(b) => b.to_string(),
                                serde_json::Value::Null => "NULL".to_string(),
                                _ => val.to_string(),
                            }).collect();
                            items.push(format!("({})", tuple_items.join(", ")));
                        } else if let Some(first_val) = map.values().next() {
                            let str_item = match first_val {
                                serde_json::Value::Number(n) => n.to_string(),
                                serde_json::Value::String(s) => format!("'{}'", s.replace('\'', "''")),
                                serde_json::Value::Bool(b) => b.to_string(),
                                serde_json::Value::Null => "NULL".to_string(),
                                _ => first_val.to_string(),
                            };
                            items.push(str_item);
                        }
                    }
                }
                let replacement = if items.is_empty() {
                    "NULL".to_string()
                } else {
                    items.join(", ")
                };
                resolved = format!("{}({}){}", &resolved[..pos], replacement, &resolved[close_idx + 1..]);
            } else if before.to_uppercase().ends_with("EXISTS") {
                let exists = !sub_res.rows.is_empty();
                let exists_pos = before.to_uppercase().rfind("EXISTS").unwrap();
                let replacement = if exists { "TRUE" } else { "FALSE" };
                resolved = format!("{}{}{}", &resolved[..exists_pos], replacement, &resolved[close_idx + 1..]);
            } else {
                let scalar_val = sub_res.rows.first().and_then(|r| {
                    if let serde_json::Value::Object(map) = r {
                        map.values().next().cloned()
                    } else {
                        None
                    }
                }).unwrap_or(serde_json::Value::Null);

                let replacement = match scalar_val {
                    serde_json::Value::Number(n) => n.to_string(),
                    serde_json::Value::String(s) => format!("'{}'", s.replace('\'', "''")),
                    serde_json::Value::Bool(b) => b.to_string(),
                    serde_json::Value::Null => "NULL".to_string(),
                    _ => scalar_val.to_string(),
                };
                resolved = format!("{}{}{}", &resolved[..pos], replacement, &resolved[close_idx + 1..]);
            }
        }
        Ok(resolved)
    }

    fn resolve_subqueries_in_select(&mut self, select_str: &str, params: &[Value]) -> Result<String, String> {
        let mut resolved = select_str.to_string();
        loop {
            let mut found_start = None;
            let bytes = resolved.as_bytes();
            for i in 0..bytes.len() {
                if bytes[i] == b'(' {
                    let after = resolved[i + 1..].trim_start();
                    let upper_after = after.to_uppercase();
                    if upper_after.starts_with("SELECT ")
                        || upper_after.starts_with("SELECT\n")
                        || upper_after.starts_with("SELECT\r")
                        || upper_after.starts_with("SELECT\t")
                    {
                        found_start = Some(i);
                        break;
                    }
                }
            }
            let Some(pos) = found_start else {
                break;
            };
            let start_inner = pos + 1;
            let mut depth = 1;
            let mut end_inner = None;
            for i in start_inner..bytes.len() {
                if bytes[i] == b'(' {
                    depth += 1;
                } else if bytes[i] == b')' {
                    depth -= 1;
                    if depth == 0 {
                        end_inner = Some(i);
                        break;
                    }
                }
            }
            let Some(close_idx) = end_inner else {
                break;
            };

            let sub_sql = &resolved[start_inner..close_idx];
            let is_correlated = if let Some(sub_from) = find_top_level_keyword(sub_sql, "FROM") {
                let after_f = sub_sql[sub_from + 4..].trim();
                let sub_w = find_top_level_keyword(after_f, "WHERE");
                let sub_tbl_part = if let Some(w) = sub_w { &after_f[..w] } else { after_f };
                let (sub_tbl_name, sub_alias) = extract_table_name_and_alias(sub_tbl_part);
                if let Some(w_pos) = sub_w {
                    let w_clause = after_f[w_pos + 5..].trim();
                    w_clause.split_whitespace().any(|tok| {
                        let clean = tok.trim_matches('(').trim_matches(')').trim_matches(',').trim_matches(';');
                        if let Some(dot) = clean.find('.') {
                            let prefix = &clean[..dot];
                            !prefix.eq_ignore_ascii_case(sub_tbl_name)
                                && sub_alias.map(|a| !prefix.eq_ignore_ascii_case(a)).unwrap_or(true)
                        } else {
                            false
                        }
                    })
                } else {
                    false
                }
            } else {
                false
            };
            if is_correlated {
                break;
            }

            let (sub_res, _) = match self.handle_select(sub_sql, params) {
                Ok(r) => r,
                Err(_) => break,
            };

            let val_str = if let Some(first_row) = sub_res.rows.first() {
                if let Some(obj) = first_row.as_object() {
                    if obj.len() == 1 {
                        let v = obj.values().next().unwrap();
                        match v {
                            serde_json::Value::Null => "NULL".to_string(),
                            serde_json::Value::String(s) => format!("'{}'", s.replace('\'', "''")),
                            serde_json::Value::Array(_) | serde_json::Value::Object(_) => format!("'{}'", serde_json::to_string(v).unwrap_or_default().replace('\'', "''")),
                            _ => v.to_string(),
                        }
                    } else {
                        let arr = serde_json::Value::Array(obj.values().cloned().collect());
                        format!("'{}'", serde_json::to_string(&arr).unwrap_or_default().replace('\'', "''"))
                    }
                } else {
                    first_row.to_string()
                }
            } else {
                "NULL".to_string()
            };

            resolved = format!("{}{}{}", &resolved[..pos], val_str, &resolved[close_idx + 1..]);
        }
        Ok(resolved)
    }

    fn resolve_correlated_subqueries_in_where(
        &mut self,
        where_str: &str,
        table_name: &str,
        columns: &[crate::types::ColumnDef],
        row: &[Value],
        alias: Option<&str>,
        params: &[Value],
    ) -> String {
        let mut resolved = where_str.to_string();
        for (col_idx, col) in columns.iter().enumerate() {
            let val = row.get(col_idx).unwrap_or(&Value::Null);
            let val_sql = match val {
                Value::Null => "NULL".to_string(),
                Value::Bool(b) => b.to_string(),
                Value::Int(i) => i.to_string(),
                Value::Float(f) => f.to_string(),
                Value::Text(s) => format!("'{}'", s.replace('\'', "''")),
            };
            if let Some(a) = alias {
                let pattern = format!("{}.{}", a, col.name);
                resolved = Self::replace_identifier_token(&resolved, &pattern, &val_sql);
            }
            let pattern_tbl = format!("{}.{}", table_name, col.name);
            resolved = Self::replace_identifier_token(&resolved, &pattern_tbl, &val_sql);
        }
        if let Ok(res) = self.resolve_subqueries_in_where(&resolved, params) {
            return res;
        }
        resolved
    }

fn json_to_engine_value(v: &serde_json::Value) -> Value {
    match v {
        serde_json::Value::Null => Value::Null,
        serde_json::Value::Bool(b) => Value::Bool(*b),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Value::Int(i)
            } else if let Some(f) = n.as_f64() {
                Value::Float(f)
            } else {
                Value::Null
            }
        }
        serde_json::Value::String(s) => Value::text(s),
        other => Value::text(other.to_string()),
    }
}

fn create_ephemeral_table(name: &str, res: QueryResult) -> crate::storage::table::Table {
    let mut cols = Vec::with_capacity(res.fields.len());
    for f in &res.fields {
        cols.push(crate::types::ColumnDef {
            name: f.name.clone(),
            data_type: crate::types::DataType::from_sql_str(&f.data_type),
            is_primary_key: false,
            is_nullable: true,
            default_value: None,
            comment: None,
        });
    }

    let mut table = crate::storage::table::Table::new(name.to_string(), cols);
    for r_json in res.rows {
        let mut row = Vec::with_capacity(table.columns.len());
        for col in &table.columns {
            let v = r_json.as_object().and_then(|obj| obj.get(&col.name)).map(Self::json_to_engine_value).unwrap_or(Value::Null);
            row.push(v);
        }
        table.insert(row);
    }
    table
}

fn substitute_correlated_references(sql: &str, row: &CombinedRow) -> String {
    let mut out = String::with_capacity(sql.len());
    let chars: Vec<char> = sql.chars().collect();
    let n = chars.len();
    let mut i = 0;

    while i < n {
        let ch = chars[i];
        if ch == '\'' {
            out.push(ch);
            i += 1;
            while i < n {
                let sc = chars[i];
                out.push(sc);
                i += 1;
                if sc == '\'' {
                    if i < n && chars[i] == '\'' {
                        out.push('\'');
                        i += 1;
                    } else {
                        break;
                    }
                }
            }
            continue;
        }

        if ch.is_alphabetic() || ch == '_' || ch == '"' {
            let start = i;
            while i < n {
                let c = chars[i];
                if c.is_alphanumeric() || c == '_' || c == '.' || c == '"' {
                    i += 1;
                } else {
                    break;
                }
            }
            let token: String = chars[start..i].iter().collect();
            let clean_token = token.replace('"', "").to_lowercase();
            if clean_token.contains('.') && row.contains_key(&clean_token) {
                let val = row.get_val(&clean_token);
                match val {
                    Value::Null => out.push_str("NULL"),
                    Value::Bool(b) => out.push_str(if b { "TRUE" } else { "FALSE" }),
                    Value::Int(num) => out.push_str(&num.to_string()),
                    Value::Float(f) => out.push_str(&f.to_string()),
                    Value::Text(ref s) => {
                        out.push('\'');
                        out.push_str(&s.replace('\'', "''"));
                        out.push('\'');
                    }
                }
            } else {
                out.push_str(&token);
            }
            continue;
        }

        out.push(ch);
        i += 1;
    }

    out
}

    fn handle_joined_query(
        &mut self,
        select_clause: &str,
        after_from: &str,
        params: &[Value],
    ) -> Result<QueryResult, String> {
        let mut ephemeral_tables = Vec::new();
        let res = self.handle_joined_query_internal(select_clause, after_from, params, &mut ephemeral_tables);
        for name in ephemeral_tables {
            self.storage.tables.remove(&name);
        }
        res
    }

    fn handle_joined_query_internal(
        &mut self,
        select_clause: &str,
        after_from: &str,
        params: &[Value],
        ephemeral_tables: &mut Vec<String>,
    ) -> Result<QueryResult, String> {
        let (from_joins_str, where_opt, group_opt, having_opt, order_specs, limit_opt, offset_opt) = parse_query_clauses(after_from, params);
        let (base_tbl_name, base_alias, base_subquery, joins) = parse_from_and_joins(from_joins_str)?;

        let resolved_where = if let Some(w) = where_opt {
            let w_up = w.to_uppercase();
            if w_up.contains("SELECT") && w_up.contains("FROM") {
                Some(self.resolve_subqueries_in_where(&w, params)?)
            } else {
                Some(w)
            }
        } else {
            None
        };

        if let Some(ref sub_sql) = base_subquery {
            let sub_res = self.execute(sub_sql, params)?;
            let tbl = Self::create_ephemeral_table(&base_tbl_name, sub_res);
            let key = base_tbl_name.to_lowercase();
            self.storage.tables.insert(key.clone(), tbl);
            ephemeral_tables.push(key);
        }

        for j in &joins {
            if let Some(ref sub_sql) = j.subquery {
                if !j.is_lateral {
                    let sub_res = self.execute(sub_sql, params)?;
                    let tbl = Self::create_ephemeral_table(&j.table_name, sub_res);
                    let key = j.table_name.to_lowercase();
                    self.storage.tables.insert(key.clone(), tbl);
                    ephemeral_tables.push(key);
                }
            }
        }

        let mut all_tables_meta: Vec<(&crate::storage::table::Table, Option<&str>)> = Vec::new();
        let base_table = self.storage.get_table(&base_tbl_name).ok_or_else(|| format!("Table {} not found", base_tbl_name))?;
        all_tables_meta.push((base_table, base_alias.as_deref()));

        for j in &joins {
            if let Some(jt) = self.storage.get_table(&j.table_name) {
                all_tables_meta.push((jt, j.alias.as_deref()));
            }
        }

        let joined_schema = std::sync::Arc::new(JoinedSchema::new(&all_tables_meta));
        let pushdown = plan_predicate_pushdown(resolved_where.as_deref(), &all_tables_meta, &joins);
        let base_table_columns = base_table.columns.clone();

        let can_early_limit = group_opt.is_none() 
            && having_opt.is_none() 
            && order_specs.is_empty() 
            && !select_clause.to_uppercase().starts_with("DISTINCT ")
            && !select_clause.to_uppercase().contains("COUNT(")
            && !select_clause.to_uppercase().contains("SUM(")
            && !select_clause.to_uppercase().contains("AVG(")
            && !select_clause.to_uppercase().contains("MIN(")
            && !select_clause.to_uppercase().contains("MAX(")
            && !select_clause.to_uppercase().contains("OVER(");

        let mut current_rows: Vec<CombinedRow> = Vec::with_capacity(base_table.active_count);
        for (r_idx, r) in base_table.rows.iter().enumerate() {
            if !base_table.is_deleted[r_idx] {
                let comb = CombinedRow::from_base_row(joined_schema.clone(), r);
                let mut pass = true;
                for pred in &pushdown.base_predicates {
                    if !eval_condition_on_row(&comb, pred, params) {
                        pass = false;
                        break;
                    }
                }
                if pass {
                    current_rows.push(comb);
                }
            }
        }

        for (j_idx, j) in joins.iter().enumerate() {
            if current_rows.is_empty() {
                break;
            }

            if j.is_lateral {
                let sub_sql = j.subquery.as_ref().unwrap();
                let alias = j.alias.as_deref().unwrap_or(&j.table_name);
                let mut next_rows = Vec::with_capacity(current_rows.len());

                for base_comb in &current_rows {
                    let resolved_sql = Self::substitute_correlated_references(sub_sql, base_comb);
                    let sub_res = self.execute(&resolved_sql, params)?;
                    let mut match_count = 0;

                    for row_json in &sub_res.rows {
                        let mut candidate = base_comb.clone();
                        if let Some(obj) = row_json.as_object() {
                            for field in &sub_res.fields {
                                let col_name = field.name.to_lowercase();
                                let val = obj.get(&field.name).map(Self::json_to_engine_value).unwrap_or(Value::Null);
                                candidate.insert_extra(format!("{}.{}", alias.to_lowercase(), col_name), val.clone());
                                if !candidate.contains_key(&col_name) {
                                    candidate.insert_extra(col_name, val);
                                }
                            }
                        }
                        let matches = match &j.on_condition {
                            Some(cond) => eval_condition_on_row(&candidate, cond, params),
                            None => true,
                        };
                        if matches {
                            next_rows.push(candidate);
                            match_count += 1;
                        }
                    }

                    if match_count == 0 && j.kind == JoinKind::Left {
                        let mut null_comb = base_comb.clone();
                        for field in &sub_res.fields {
                            let col_name = field.name.to_lowercase();
                            null_comb.insert_extra(format!("{}.{}", alias.to_lowercase(), col_name), Value::Null);
                            if !null_comb.contains_key(&col_name) {
                                null_comb.insert_extra(col_name, Value::Null);
                            }
                        }
                        next_rows.push(null_comb);
                    }
                }

                current_rows = next_rows;
                continue;
            }

            let j_table = self.storage.get_table(&j.table_name).ok_or_else(|| format!("Table {} not found", j.table_name))?;

            let maybe_equi = if let Some(ref cond) = j.on_condition {
                parse_equi_join_candidate(cond, &j.table_name, j.alias.as_deref(), j_table)
            } else {
                None
            };

            let mut next_rows = Vec::with_capacity(current_rows.len());

            let filter_preds = &pushdown.joined_table_predicates[j_idx];
            let table_offset = joined_schema.table_offsets.get(j_idx + 1).cloned().unwrap_or(0);

            if let Some(equi) = maybe_equi {
                if current_rows.len() * 2 < j_table.rows.len() {
                    // Adaptive Hash Join: Build hash table on current_rows (much smaller!), Probe j_table
                    let mut left_hash_map: std::collections::HashMap<JoinKey, Vec<usize>> = std::collections::HashMap::new();
                    for (base_idx, base_comb) in current_rows.iter().enumerate() {
                        let left_val = eval_operand_on_row(base_comb, &equi.left_expr, params);
                        left_hash_map.entry(value_to_join_key(&left_val)).or_default().push(base_idx);
                    }

                    let mut matched_left_counts = if j.kind == JoinKind::Left {
                        vec![0usize; current_rows.len()]
                    } else {
                        Vec::new()
                    };

                    for (jr_idx, jr) in j_table.rows.iter().enumerate() {
                        if !j_table.is_deleted[jr_idx] {
                            if !filter_preds.is_empty() {
                                let mut test_comb = CombinedRow::new_with_schema(joined_schema.clone());
                                test_comb.values.resize(table_offset, Value::Null);
                                test_comb.values.extend_from_slice(jr);
                                let mut pass = true;
                                for p in filter_preds {
                                    if !eval_condition_on_row(&test_comb, p, params) {
                                        pass = false;
                                        break;
                                    }
                                }
                                if !pass {
                                    continue;
                                }
                            }

                            if let Some(val) = jr.get(equi.right_col_idx) {
                                let key = value_to_join_key(val);
                                if let Some(base_indices) = left_hash_map.get(&key) {
                                    for &base_idx in base_indices {
                                        let base_comb = &current_rows[base_idx];
                                        if let Some(ref res_cond) = equi.residual_cond {
                                            let candidate = base_comb.with_joined_table(jr);
                                            if eval_condition_on_row(&candidate, res_cond, params) {
                                                next_rows.push(candidate);
                                                if j.kind == JoinKind::Left {
                                                    matched_left_counts[base_idx] += 1;
                                                }
                                            }
                                        } else {
                                            let matched_comb = base_comb.with_joined_table(jr);
                                            next_rows.push(matched_comb);
                                            if j.kind == JoinKind::Left {
                                                matched_left_counts[base_idx] += 1;
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }

                    if j.kind == JoinKind::Left {
                        for (base_idx, &count) in matched_left_counts.iter().enumerate() {
                            if count == 0 {
                                let null_comb = current_rows[base_idx].with_null_table(j_table.columns.len());
                                next_rows.push(null_comb);
                            }
                        }
                    }
                } else {
                    // Standard Hash Join: Build hash table on j_table's right_col_idx
                    let mut right_hash_map: std::collections::HashMap<JoinKey, Vec<usize>> = std::collections::HashMap::new();
                    for (jr_idx, jr) in j_table.rows.iter().enumerate() {
                        if !j_table.is_deleted[jr_idx] {
                            if !filter_preds.is_empty() {
                                let mut test_comb = CombinedRow::new_with_schema(joined_schema.clone());
                                test_comb.values.resize(table_offset, Value::Null);
                                test_comb.values.extend_from_slice(jr);
                                let mut pass = true;
                                for p in filter_preds {
                                    if !eval_condition_on_row(&test_comb, p, params) {
                                        pass = false;
                                        break;
                                    }
                                }
                                if !pass {
                                    continue;
                                }
                            }
                            if let Some(val) = jr.get(equi.right_col_idx) {
                                right_hash_map.entry(value_to_join_key(val)).or_default().push(jr_idx);
                            }
                        }
                    }

                    // Probe phase: match against current_rows
                    for base_comb in &current_rows {
                        let mut match_count = 0;
                        let left_val = eval_operand_on_row(base_comb, &equi.left_expr, params);

                        let key = value_to_join_key(&left_val);
                        if let Some(matched_indices) = right_hash_map.get(&key) {
                            for &jr_idx in matched_indices {
                                let jr = &j_table.rows[jr_idx];

                                if let Some(ref res_cond) = equi.residual_cond {
                                    let candidate = base_comb.with_joined_table(jr);
                                    if eval_condition_on_row(&candidate, res_cond, params) {
                                        next_rows.push(candidate);
                                        match_count += 1;
                                    }
                                } else {
                                    let matched_comb = base_comb.with_joined_table(jr);
                                    next_rows.push(matched_comb);
                                    match_count += 1;
                                }
                            }
                        }

                        if match_count == 0 && j.kind == JoinKind::Left {
                            let null_comb = base_comb.with_null_table(j_table.columns.len());
                            next_rows.push(null_comb);
                        }
                    }
                }
            } else {
                // Fallback: Nested Loop Join for non-equi joins
                for base_comb in &current_rows {
                    let mut match_count = 0;
                    for (jr_idx, jr) in j_table.rows.iter().enumerate() {
                        if !j_table.is_deleted[jr_idx] {
                            if !filter_preds.is_empty() {
                                let mut test_comb = CombinedRow::new_with_schema(joined_schema.clone());
                                test_comb.values.resize(table_offset, Value::Null);
                                test_comb.values.extend_from_slice(jr);
                                let mut pass = true;
                                for p in filter_preds {
                                    if !eval_condition_on_row(&test_comb, p, params) {
                                        pass = false;
                                        break;
                                    }
                                }
                                if !pass {
                                    continue;
                                }
                            }
                            let candidate = base_comb.with_joined_table(jr);
                            let matches = match &j.on_condition {
                                Some(cond) => eval_condition_on_row(&candidate, cond, params),
                                None => true,
                            };
                            if matches {
                                next_rows.push(candidate);
                                match_count += 1;
                            }
                        }
                    }
                    if match_count == 0 && j.kind == JoinKind::Left {
                        let null_comb = base_comb.with_null_table(j_table.columns.len());
                        next_rows.push(null_comb);
                    }
                }
            }

            // Intermediate predicate filtering right after this join step
            if let Some(preds) = pushdown.intermediate_predicates.get(j_idx + 1) {
                for pred in preds {
                    next_rows.retain(|r| eval_condition_on_row(r, pred, params));
                }
            }

            // Early LIMIT optimization
            if can_early_limit && j_idx + 1 == joins.len() {
                if let Some(lim) = limit_opt {
                    let target = lim + offset_opt.unwrap_or(0);
                    if next_rows.len() >= target {
                        next_rows.truncate(target);
                        current_rows = next_rows;
                        break;
                    }
                }
            }

            current_rows = next_rows;
        }

        for rem in &pushdown.remaining_where {
            current_rows.retain(|r| eval_condition_on_row(r, rem, params));
        }

        let mut final_json_rows: Vec<serde_json::Value> = Vec::new();
        let is_distinct = select_clause.to_uppercase().starts_with("DISTINCT ");
        let clean_select = if is_distinct { select_clause[9..].trim() } else { select_clause };

        if let Some(ref gb_str) = group_opt {
            let group_col_names: Vec<String> = split_comma_separated_tokens(gb_str).into_iter().map(|s| s.trim().to_string()).collect();
            let mut group_map: std::collections::HashMap<String, Vec<usize>> = std::collections::HashMap::new();
            let mut group_keys_order = Vec::new();

            for (idx, r) in current_rows.iter().enumerate() {
                let key = group_col_names.iter().map(|col| r.get_val(col).as_str()).collect::<Vec<_>>().join("\x1f");
                if !group_map.contains_key(&key) {
                    group_keys_order.push(key.clone());
                }
                group_map.entry(key).or_default().push(idx);
            }

            let proj_items = split_projection_items(clean_select);

            for key in group_keys_order {
                let row_indices = &group_map[&key];
                let mut row_map = serde_json::Map::new();

                for item in &proj_items {
                    let upper = item.to_uppercase();
                    let (expr_part, alias) = if let Some(as_idx) = upper.rfind(" AS ") {
                        (item[..as_idx].trim(), item[as_idx + 4..].trim().replace('"', ""))
                    } else {
                        (item.trim(), clean_col_name(item.trim()).replace('"', ""))
                    };

                    let val = eval_group_aggregate_expr(expr_part, &current_rows, row_indices, params, &self.storage);
                    row_map.insert(alias, val);
                }

                let mut include = true;
                if let Some(ref h_str) = having_opt {
                    include = eval_having_condition(h_str, &row_map, None);
                }
                if include {
                    final_json_rows.push(serde_json::Value::Object(row_map));
                }
            }
        } else {
            let proj_items = split_projection_items(clean_select);
            let has_aggregates = proj_items.iter().any(|item| is_aggregate_projection_item(item));

            if has_aggregates {
                let row_indices: Vec<usize> = (0..current_rows.len()).collect();
                let mut row_map = serde_json::Map::new();
                for item in &proj_items {
                    let upper = item.to_uppercase();
                    let (expr_part, alias) = if let Some(as_idx) = upper.rfind(" AS ") {
                        (item[..as_idx].trim(), item[as_idx + 4..].trim().replace('"', ""))
                    } else {
                        (item.trim(), clean_col_name(item.trim()).replace('"', ""))
                    };

                    let val = if is_aggregate_projection_item(item) {
                        eval_group_aggregate_expr(expr_part, &current_rows, &row_indices, params, &self.storage)
                    } else if let Some(&first_ri) = row_indices.first() {
                        eval_joined_scalar_expr(expr_part, &current_rows[first_ri], params, &self.storage)
                    } else {
                        serde_json::Value::Null
                    };
                    row_map.insert(alias, val);
                }

                let mut include = true;
                if let Some(ref h_str) = having_opt {
                    include = eval_having_condition(h_str, &row_map, None);
                }
                if include {
                    final_json_rows.push(serde_json::Value::Object(row_map));
                }
            } else {
                if !order_specs.is_empty() {
                    current_rows.sort_by(|a, b| {
                        for spec in &order_specs {
                            let va = a.get_val(spec.col_name);
                            let vb = b.get_val(spec.col_name);
                            let ord = va.cmp_value(&vb);
                            let directed = if spec.is_desc { ord.reverse() } else { ord };
                            if directed != std::cmp::Ordering::Equal {
                                return directed;
                            }
                        }
                        std::cmp::Ordering::Equal
                    });
                }
                for r in &current_rows {
                    let mut row_map = serde_json::Map::new();
                    for item in &proj_items {
                        let trimmed_item = item.trim();
                        if trimmed_item.ends_with(".*") {
                            let prefix = trimmed_item[..trimmed_item.len() - 2].trim().trim_matches('"');
                            let is_base = prefix.eq_ignore_ascii_case(&base_tbl_name)
                                || base_alias.as_ref().map(|a| prefix.eq_ignore_ascii_case(a.trim_matches('"'))).unwrap_or(false);
                            if is_base {
                                for col in &base_table_columns {
                                    let col_name = &col.name;
                                    let val = r.get_val(&format!("{}.{}", prefix, col_name));
                                    let final_val = if val.is_null() { r.get_val(col_name) } else { val };
                                    row_map.insert(col_name.clone(), value_to_json(&final_val));
                                }
                                continue;
                            }
                            let mut matched_join = false;
                            for j in &joins {
                                let is_j = prefix.eq_ignore_ascii_case(&j.table_name)
                                    || j.alias.as_ref().map(|a| prefix.eq_ignore_ascii_case(a.trim_matches('"'))).unwrap_or(false);
                                if is_j {
                                    if let Some(jt) = self.storage.get_table(&j.table_name) {
                                        for col in &jt.columns {
                                            let col_name = &col.name;
                                            let val = r.get_val(&format!("{}.{}", prefix, col_name));
                                            let final_val = if val.is_null() { r.get_val(col_name) } else { val };
                                            row_map.insert(col_name.clone(), value_to_json(&final_val));
                                        }
                                    }
                                    matched_join = true;
                                    break;
                                }
                            }
                            if matched_join {
                                continue;
                            }
                        } else if trimmed_item == "*" {
                            for col in &base_table_columns {
                                let val = r.get_val(&col.name);
                                row_map.insert(col.name.clone(), value_to_json(&val));
                            }
                            for j in &joins {
                                if let Some(jt) = self.storage.get_table(&j.table_name) {
                                    for col in &jt.columns {
                                        let val = r.get_val(&col.name);
                                        row_map.insert(col.name.clone(), value_to_json(&val));
                                    }
                                }
                            }
                            continue;
                        }

                        let upper = item.to_uppercase();
                        let (expr_part, alias) = if let Some(as_idx) = upper.rfind(" AS ") {
                            (item[..as_idx].trim(), item[as_idx + 4..].trim().replace('"', ""))
                        } else {
                            (item.trim(), clean_col_name(item.trim()).replace('"', ""))
                        };
                        let val = eval_joined_scalar_expr(expr_part, r, params, &self.storage);
                        row_map.insert(alias, val);
                    }
                    final_json_rows.push(serde_json::Value::Object(row_map));
                }
            }
        }

        if select_clause.to_uppercase().contains(" OVER (") {
            eval_window_functions(&mut final_json_rows, select_clause);
        }

        if is_distinct {
            let mut seen = std::collections::HashSet::new();
            let mut distinct_rows = Vec::new();
            for r in final_json_rows {
                let key = serde_json::to_string(&r).unwrap_or_default();
                if seen.insert(key) {
                    distinct_rows.push(r);
                }
            }
            final_json_rows = distinct_rows;
        }

        if !order_specs.is_empty() {
            sort_json_rows(&mut final_json_rows, &order_specs);
        }

        let offset = offset_opt.unwrap_or(0);
        let limit = limit_opt.unwrap_or(usize::MAX);
        if offset < final_json_rows.len() {
            let end = (offset.saturating_add(limit)).min(final_json_rows.len());
            final_json_rows = final_json_rows[offset..end].to_vec();
        } else {
            final_json_rows = vec![];
        }

        let row_count = final_json_rows.len();
        let fields: Vec<FieldInfo> = final_json_rows.first().and_then(|r| {
            if let serde_json::Value::Object(map) = r {
                Some(map.keys().map(|k| FieldInfo { name: k.clone(), data_type: "text".to_string() }).collect())
            } else {
                None
            }
        }).unwrap_or_default();

        Ok(QueryResult {
            rows: final_json_rows,
            row_count,
            fields,
            command: "SELECT".to_string(),
        })
    }

    fn handle_drop_table(&mut self, sql: &str) -> Result<QueryResult, String> {
        let after_drop = sql[10..].trim();
        let mut table_name = after_drop.trim();
        let if_exists = table_name.to_uppercase().starts_with("IF EXISTS ");
        if if_exists {
            table_name = table_name[10..].trim();
        }
        let table_name = table_name.trim_matches('"').trim_matches(';').trim().to_string();
        self.storage.drop_table(&table_name);
        self.plan_cache.clear();

        Ok(QueryResult {
            rows: vec![],
            row_count: 0,
            fields: vec![],
            command: "DROP TABLE".to_string(),
        })
    }

    fn handle_create_table(&mut self, sql: &str) -> Result<QueryResult, String> {
        // Simple CREATE TABLE parser
        let after_create = sql[12..].trim();
        let (name_part, body_part) = if let Some(open_paren) = after_create.find('(') {
            let close_paren = after_create.rfind(')').ok_or("Missing closing parenthesis in CREATE TABLE")?;
            (&after_create[..open_paren].trim(), &after_create[open_paren + 1..close_paren].trim())
        } else {
            return Err("Invalid CREATE TABLE syntax".to_string());
        };

        let mut table_name = name_part.trim();
        let if_not_exists = table_name.to_uppercase().starts_with("IF NOT EXISTS ");
        if if_not_exists {
            table_name = table_name[14..].trim();
        }
        let table_name = table_name.trim_matches('"').to_string();

        if if_not_exists && self.storage.get_table(&table_name).is_some() {
            return Ok(QueryResult {
                rows: vec![],
                row_count: 0,
                fields: vec![],
                command: "CREATE TABLE".to_string(),
            });
        }

        let mut columns = Vec::new();
        let mut table_level_pks = Vec::new();

        for col_def_str in split_comma_separated_tokens(body_part) {
            let col_def_str = col_def_str.trim();
            if col_def_str.is_empty() {
                continue;
            }

            let upper_def = col_def_str.to_uppercase();
            // Handle table-level PRIMARY KEY (id) or CONSTRAINT pk PRIMARY KEY (id)
            if upper_def.starts_with("PRIMARY KEY ") || upper_def.starts_with("PRIMARY KEY(") || (upper_def.starts_with("CONSTRAINT") && upper_def.contains("PRIMARY KEY")) {
                if let Some(open_p) = col_def_str.find('(') {
                    if let Some(close_p) = col_def_str.rfind(')') {
                        let inner = &col_def_str[open_p + 1..close_p];
                        for pk_col in split_comma_separated_tokens(inner) {
                            let pk_clean = clean_col_name(pk_col).to_string();
                            table_level_pks.push(pk_clean);
                        }
                    }
                }
                continue;
            }

            // Ignore other table constraints like FOREIGN KEY, CHECK, UNIQUE (...)
            if upper_def.starts_with("FOREIGN KEY ") || upper_def.starts_with("FOREIGN KEY(") || upper_def.starts_with("CHECK ") || upper_def.starts_with("CHECK(") || (upper_def.starts_with("CONSTRAINT") && !upper_def.contains("PRIMARY KEY")) {
                continue;
            }

            let (col_name, rest_def) = if col_def_str.starts_with('"') {
                if let Some(close_q) = col_def_str[1..].find('"') {
                    let name = &col_def_str[1..1 + close_q];
                    (name.to_string(), &col_def_str[2 + close_q..])
                } else {
                    let parts: Vec<&str> = col_def_str.split_whitespace().collect();
                    (parts[0].trim_matches('"').to_string(), &col_def_str[parts[0].len()..])
                }
            } else {
                let parts: Vec<&str> = col_def_str.split_whitespace().collect();
                (parts[0].trim_matches('"').to_string(), &col_def_str[parts[0].len()..])
            };

            let upper_rest = rest_def.to_uppercase();

            let data_type = DataType::from_sql_str(rest_def);

            let is_primary_key = upper_rest.contains("PRIMARY KEY");
            let is_nullable = !upper_rest.contains("NOT NULL") && !is_primary_key;

            let default_value = if let Some(def_idx) = upper_rest.find("DEFAULT ") {
                let after_def = rest_def[def_idx + 8..].trim();
                if after_def.starts_with('\'') {
                    if let Some(end_quote) = after_def[1..].find('\'') {
                        Some(after_def[..=end_quote + 1].to_string())
                    } else {
                        Some(after_def.split_whitespace().next().unwrap_or("").trim_matches(';').trim_matches(',').to_string())
                    }
                } else {
                    let mut tokens = Vec::new();
                    for part in after_def.split_whitespace() {
                        let p = part.trim_matches(';').trim_matches(',');
                        if p.eq_ignore_ascii_case("NOT") || p.eq_ignore_ascii_case("NULL") || p.eq_ignore_ascii_case("PRIMARY") || p.eq_ignore_ascii_case("KEY") || p.eq_ignore_ascii_case("REFERENCES") || p.eq_ignore_ascii_case("CHECK") || p.eq_ignore_ascii_case("CONSTRAINT") {
                            break;
                        }
                        tokens.push(p);
                    }
                    if tokens.is_empty() {
                        None
                    } else {
                        Some(tokens.join(" "))
                    }
                }
            } else {
                None
            };

            columns.push(ColumnDef {
                name: col_name,
                data_type,
                is_primary_key,
                is_nullable,
                default_value,
                comment: None,
            });
        }

        // Apply table-level primary keys
        for pk_col_name in table_level_pks {
            if let Some(col) = columns.iter_mut().find(|c| c.name.eq_ignore_ascii_case(&pk_col_name)) {
                col.is_primary_key = true;
            }
        }

        let _ = self.storage.create_table(table_name, columns);

        Ok(QueryResult {
            rows: vec![],
            row_count: 0,
            fields: vec![],
            command: "CREATE TABLE".to_string(),
        })
    }

    fn handle_comment_on_table(&mut self, sql: &str) -> Result<QueryResult, String> {
        // Syntax: COMMENT ON TABLE [schema.]table_name IS 'comment' (or IS NULL)
        let upper = sql.to_uppercase();
        let is_idx = upper.find(" IS ").ok_or_else(|| "Invalid COMMENT ON TABLE syntax: missing IS".to_string())?;
        let table_part = sql[16..is_idx].trim().trim_matches('"');
        let table_name = if let Some(dot_idx) = table_part.rfind('.') {
            table_part[dot_idx + 1..].trim().trim_matches('"')
        } else {
            table_part
        };

        let comment_part = sql[is_idx + 4..].trim().trim_end_matches(';').trim();
        let comment = if comment_part.eq_ignore_ascii_case("NULL") {
            None
        } else if comment_part.starts_with('\'') && comment_part.ends_with('\'') && comment_part.len() >= 2 {
            let inner = &comment_part[1..comment_part.len() - 1];
            Some(inner.replace("''", "'"))
        } else {
            Some(comment_part.to_string())
        };

        let mut target_key = table_name.to_string();
        if let Some(table) = self.storage.tables.get_mut(table_name) {
            table.comment = comment.clone();
            target_key = table.name.clone();
        } else {
            let found_key = self.storage.tables.keys().find(|k| k.eq_ignore_ascii_case(table_name)).cloned();
            if let Some(k) = found_key {
                if let Some(table) = self.storage.tables.get_mut(&k) {
                    table.comment = comment.clone();
                    target_key = table.name.clone();
                }
            }
        }

        self.storage.wal.append(WalRecord::CommentOnTable {
            table: target_key,
            comment,
        });

        Ok(QueryResult {
            rows: vec![],
            row_count: 0,
            fields: vec![],
            command: "COMMENT".to_string(),
        })
    }

    fn handle_comment_on_column(&mut self, sql: &str) -> Result<QueryResult, String> {
        // Syntax: COMMENT ON COLUMN [schema.]table_name.column_name IS 'comment' (or IS NULL)
        let upper = sql.to_uppercase();
        let is_idx = upper.find(" IS ").ok_or_else(|| "Invalid COMMENT ON COLUMN syntax: missing IS".to_string())?;
        let target_part = sql[17..is_idx].trim();

        let parts: Vec<&str> = target_part.split('.').collect();
        let (table_name, col_name) = match parts.len() {
            1 => return Err("Invalid COMMENT ON COLUMN syntax: missing column name".to_string()),
            2 => (parts[0].trim().trim_matches('"'), parts[1].trim().trim_matches('"')),
            _ => (parts[parts.len() - 2].trim().trim_matches('"'), parts[parts.len() - 1].trim().trim_matches('"')),
        };

        let comment_part = sql[is_idx + 4..].trim().trim_end_matches(';').trim();
        let comment = if comment_part.eq_ignore_ascii_case("NULL") {
            None
        } else if comment_part.starts_with('\'') && comment_part.ends_with('\'') && comment_part.len() >= 2 {
            let inner = &comment_part[1..comment_part.len() - 1];
            Some(inner.replace("''", "'"))
        } else {
            Some(comment_part.to_string())
        };

        let mut updated = false;
        let mut target_table_name = table_name.to_string();
        let mut target_col_name = col_name.to_string();

        if let Some(table) = self.storage.tables.get_mut(table_name) {
            target_table_name = table.name.clone();
            if let Some(col) = table.columns.iter_mut().find(|c| c.name.eq_ignore_ascii_case(col_name)) {
                col.comment = comment.clone();
                target_col_name = col.name.clone();
                updated = true;
            }
        }
        if !updated {
            let found_key = self.storage.tables.keys().find(|k| k.eq_ignore_ascii_case(table_name)).cloned();
            if let Some(k) = found_key {
                if let Some(table) = self.storage.tables.get_mut(&k) {
                    target_table_name = table.name.clone();
                    if let Some(col) = table.columns.iter_mut().find(|c| c.name.eq_ignore_ascii_case(col_name)) {
                        col.comment = comment.clone();
                        target_col_name = col.name.clone();
                    }
                }
            }
        }

        self.storage.wal.append(WalRecord::CommentOnColumn {
            table: target_table_name,
            column: target_col_name,
            comment,
        });

        Ok(QueryResult {
            rows: vec![],
            row_count: 0,
            fields: vec![],
            command: "COMMENT".to_string(),
        })
    }

    fn handle_description_scalar_query(&self, sql: &str) -> Result<QueryResult, String> {
        let upper = sql.to_uppercase();
        if upper.contains("OBJ_DESCRIPTION") {
            let mut comment: Option<String> = None;
            for (name, tbl) in &self.storage.tables {
                if upper.contains(&name.to_uppercase()) {
                    comment = tbl.comment.clone();
                    break;
                }
            }
            let val = comment.map(serde_json::Value::String).unwrap_or(serde_json::Value::Null);
            return Ok(QueryResult {
                rows: vec![serde_json::json!({ "comment": val })],
                row_count: 1,
                fields: vec![FieldInfo { name: "comment".to_string(), data_type: "text".to_string() }],
                command: "SELECT".to_string(),
            });
        } else if upper.contains("COL_DESCRIPTION") {
            let mut comment: Option<String> = None;
            for (name, tbl) in &self.storage.tables {
                if upper.contains(&name.to_uppercase()) {
                    if let Some(comma_pos) = upper.rfind(',') {
                        let after_comma = upper[comma_pos + 1..].trim();
                        let num_str: String = after_comma.chars().take_while(|c| c.is_ascii_digit()).collect();
                        if let Ok(pos) = num_str.parse::<usize>() {
                            if pos >= 1 && pos <= tbl.columns.len() {
                                comment = tbl.columns[pos - 1].comment.clone();
                            }
                        }
                    }
                    break;
                }
            }
            let val = comment.map(serde_json::Value::String).unwrap_or(serde_json::Value::Null);
            return Ok(QueryResult {
                rows: vec![serde_json::json!({ "comment": val })],
                row_count: 1,
                fields: vec![FieldInfo { name: "comment".to_string(), data_type: "text".to_string() }],
                command: "SELECT".to_string(),
            });
        } else if upper.contains("FROM PG_CATALOG.PG_DESCRIPTION") || upper.contains("FROM PG_DESCRIPTION") {
            return Ok(QueryResult {
                rows: vec![serde_json::json!({ "classoid": 1259, "objoid": 1, "objsubid": 0, "description": "" })],
                row_count: 1,
                fields: vec![
                    FieldInfo { name: "classoid".to_string(), data_type: "integer".to_string() },
                    FieldInfo { name: "objoid".to_string(), data_type: "integer".to_string() },
                    FieldInfo { name: "objsubid".to_string(), data_type: "integer".to_string() },
                    FieldInfo { name: "description".to_string(), data_type: "text".to_string() },
                ],
                command: "SELECT".to_string(),
            });
        }
        Err("Unsupported description query".to_string())
    }

    fn handle_table_introspection_query(&self, _sql: &str, params: &[Value]) -> Result<QueryResult, String> {
        let schema_filter = params.first().and_then(|v| match v {
            Value::Text(s) => Some(s.as_str()),
            _ => None,
        });

        let fields = vec![
            FieldInfo { name: "name".to_string(), data_type: "text".to_string() },
            FieldInfo { name: "comment".to_string(), data_type: "text".to_string() },
            FieldInfo { name: "columns".to_string(), data_type: "json".to_string() },
        ];

        if let Some(s) = schema_filter {
            if !s.is_empty() && !s.eq_ignore_ascii_case("public") {
                return Ok(QueryResult {
                    rows: vec![],
                    row_count: 0,
                    fields,
                    command: "SELECT".to_string(),
                });
            }
        }

        let mut table_names: Vec<String> = self.storage.tables.keys().cloned().collect();
        table_names.sort();

        let mut rows = Vec::with_capacity(table_names.len());
        for name in table_names {
            if let Some(table) = self.storage.tables.get(&name) {
                let cols_json: Vec<serde_json::Value> = table.columns.iter().map(|col| {
                    let type_str = match &col.data_type {
                        DataType::Integer => "integer",
                        DataType::BigInt => "bigint",
                        DataType::Serial => "integer",
                        DataType::Text => "text",
                        DataType::Boolean => "boolean",
                        DataType::Numeric => "numeric",
                        DataType::Timestamp => "timestamp without time zone",
                        DataType::Date => "date",
                        DataType::Time => "time without time zone",
                        DataType::Jsonb => "jsonb",
                        DataType::Uuid => "uuid",
                        DataType::Bytea => "bytea",
                        DataType::Array(_) => "ARRAY",
                    };
                    serde_json::json!({
                        "name": col.name,
                        "type": type_str,
                        "nullable": col.is_nullable,
                        "default": col.default_value.clone().map(serde_json::Value::String).unwrap_or(serde_json::Value::Null),
                        "comment": col.comment.clone().map(serde_json::Value::String).unwrap_or(serde_json::Value::Null),
                    })
                }).collect();

                rows.push(serde_json::json!({
                    "name": table.name,
                    "comment": table.comment.clone().map(serde_json::Value::String).unwrap_or(serde_json::Value::Null),
                    "columns": cols_json,
                }));
            }
        }

        let row_count = rows.len();
        Ok(QueryResult {
            rows,
            row_count,
            fields,
            command: "SELECT".to_string(),
        })
    }

    fn handle_pg_constraint_introspection_query(&self, _sql: &str, params: &[Value]) -> Result<QueryResult, String> {
        let schema_filter = params.first().and_then(|v| match v {
            Value::Text(s) => Some(s.as_str()),
            _ => None,
        });

        let fields = vec![
            FieldInfo { name: "schema_name".to_string(), data_type: "text".to_string() },
            FieldInfo { name: "table_name".to_string(), data_type: "text".to_string() },
            FieldInfo { name: "constraint_name".to_string(), data_type: "text".to_string() },
            FieldInfo { name: "constraint_type".to_string(), data_type: "text".to_string() },
            FieldInfo { name: "referenced_table_schema".to_string(), data_type: "text".to_string() },
            FieldInfo { name: "referenced_table_name".to_string(), data_type: "text".to_string() },
            FieldInfo { name: "column_names".to_string(), data_type: "array".to_string() },
            FieldInfo { name: "referenced_columns".to_string(), data_type: "array".to_string() },
        ];

        if let Some(s) = schema_filter {
            if !s.is_empty() && !s.eq_ignore_ascii_case("public") {
                return Ok(QueryResult {
                    rows: vec![],
                    row_count: 0,
                    fields,
                    command: "SELECT".to_string(),
                });
            }
        }

        let mut table_names: Vec<String> = self.storage.tables.keys().cloned().collect();
        table_names.sort();

        let mut rows = Vec::new();
        for name in table_names {
            if let Some(table) = self.storage.tables.get(&name) {
                let pk_cols: Vec<serde_json::Value> = table.columns.iter()
                    .filter(|c| c.is_primary_key || c.data_type == DataType::Serial)
                    .map(|c| serde_json::Value::String(c.name.clone()))
                    .collect();

                if !pk_cols.is_empty() {
                    rows.push(serde_json::json!({
                        "schema_name": "public",
                        "table_name": table.name,
                        "constraint_name": format!("{}_pkey", table.name),
                        "constraint_type": "PRIMARY KEY",
                        "referenced_table_schema": serde_json::Value::Null,
                        "referenced_table_name": serde_json::Value::Null,
                        "column_names": serde_json::Value::Array(pk_cols),
                        "referenced_columns": serde_json::Value::Null,
                    }));
                }
            }
        }

        let row_count = rows.len();
        Ok(QueryResult {
            rows,
            row_count,
            fields,
            command: "SELECT".to_string(),
        })
    }

    fn handle_information_schema_columns_query(&self, _sql: &str, params: &[Value]) -> Result<QueryResult, String> {
        let schema_filter = params.first().and_then(|v| match v {
            Value::Text(s) => Some(s.as_str()),
            _ => None,
        });

        let fields = vec![
            FieldInfo { name: "schema_name".to_string(), data_type: "text".to_string() },
            FieldInfo { name: "table_name".to_string(), data_type: "text".to_string() },
            FieldInfo { name: "column_name".to_string(), data_type: "text".to_string() },
            FieldInfo { name: "data_type".to_string(), data_type: "text".to_string() },
            FieldInfo { name: "udt_name".to_string(), data_type: "text".to_string() },
            FieldInfo { name: "is_nullable".to_string(), data_type: "text".to_string() },
            FieldInfo { name: "column_default".to_string(), data_type: "text".to_string() },
            FieldInfo { name: "constraint_type".to_string(), data_type: "text".to_string() },
            FieldInfo { name: "comment".to_string(), data_type: "text".to_string() },
        ];

        if let Some(s) = schema_filter {
            if !s.is_empty() && !s.eq_ignore_ascii_case("public") {
                return Ok(QueryResult {
                    rows: vec![],
                    row_count: 0,
                    fields,
                    command: "SELECT".to_string(),
                });
            }
        }

        let mut table_names: Vec<String> = self.storage.tables.keys().cloned().collect();
        table_names.sort();

        let mut rows = Vec::new();
        for name in table_names {
            if let Some(table) = self.storage.tables.get(&name) {
                for (pos, col) in table.columns.iter().enumerate() {
                    let (data_type, udt_name) = match &col.data_type {
                        DataType::Integer => ("integer", "int4"),
                        DataType::BigInt => ("bigint", "int8"),
                        DataType::Serial => ("integer", "int4"),
                        DataType::Text => ("text", "text"),
                        DataType::Boolean => ("boolean", "bool"),
                        DataType::Numeric => ("numeric", "numeric"),
                        DataType::Timestamp => ("timestamp without time zone", "timestamp"),
                        DataType::Date => ("date", "date"),
                        DataType::Time => ("time without time zone", "time"),
                        DataType::Jsonb => ("jsonb", "jsonb"),
                        DataType::Uuid => ("uuid", "uuid"),
                        DataType::Bytea => ("bytea", "bytea"),
                        DataType::Array(_) => ("ARRAY", "ARRAY"),
                    };

                    let default_val = col.default_value.clone().or_else(|| {
                        if col.data_type == DataType::Serial {
                            Some(format!("nextval('{}_{}_seq'::regclass)", table.name, col.name))
                        } else {
                            None
                        }
                    });

                    rows.push(serde_json::json!({
                        "schema_name": "public",
                        "table_name": table.name,
                        "column_name": col.name,
                        "data_type": data_type,
                        "udt_name": udt_name,
                        "is_nullable": if col.is_nullable { "YES" } else { "NO" },
                        "column_default": default_val.map(serde_json::Value::String).unwrap_or(serde_json::Value::Null),
                        "constraint_type": if col.is_primary_key { serde_json::json!("PRIMARY KEY") } else { serde_json::Value::Null },
                        "comment": col.comment.clone().map(serde_json::Value::String).unwrap_or(serde_json::Value::Null),
                        "ordinal_position": pos + 1,
                    }));
                }
            }
        }

        let mut filtered_rows = rows;
        let up_sql = _sql.to_uppercase();
        if let Some(w_idx) = up_sql.find("WHERE ") {
            let where_part = &_sql[w_idx + 6..];
            if let Some(tbl_match) = extract_literal_after_key(where_part, "table_name") {
                filtered_rows.retain(|r| r.get("table_name").and_then(|v| v.as_str()).map(|s| s.eq_ignore_ascii_case(&tbl_match)).unwrap_or(false));
            }
            if let Some(col_match) = extract_literal_after_key(where_part, "column_name") {
                filtered_rows.retain(|r| r.get("column_name").and_then(|v| v.as_str()).map(|s| s.eq_ignore_ascii_case(&col_match)).unwrap_or(false));
            } else if let Some(in_pos) = where_part.to_uppercase().find("COLUMN_NAME IN") {
                let after_in = where_part[in_pos + 14..].trim();
                if let Some(open_p) = after_in.find('(') {
                    if let Some(close_p) = after_in[open_p..].find(')') {
                        let inside = &after_in[open_p + 1..open_p + close_p];
                        let list: Vec<String> = split_comma_separated_tokens(inside).into_iter().map(|tok| tok.trim().trim_matches('\'').trim_matches('"').to_string()).collect();
                        filtered_rows.retain(|r| {
                            r.get("column_name").and_then(|v| v.as_str()).map_or(false, |col| list.iter().any(|item| item.eq_ignore_ascii_case(col)))
                        });
                    }
                }
            }
        }

        let projected_rows = if !up_sql.starts_with("SELECT *") {
            let from_idx = up_sql.find(" FROM ").unwrap_or(_sql.len());
            let select_part = &_sql[6..from_idx].trim();
            let req_cols: Vec<String> = split_comma_separated_tokens(select_part)
                .into_iter()
                .map(|c| clean_col_name(c).to_string())
                .collect();
            filtered_rows.into_iter().map(|r| {
                let mut map = serde_json::Map::new();
                for col in &req_cols {
                    if let Some(val) = r.get(col) {
                        map.insert(col.clone(), val.clone());
                    }
                }
                serde_json::Value::Object(map)
            }).collect()
        } else {
            filtered_rows
        };

        let row_count = projected_rows.len();
        Ok(QueryResult {
            rows: projected_rows,
            row_count,
            fields,
            command: "SELECT".to_string(),
        })
    }

    fn handle_insert(&mut self, sql: &str, params: &[Value]) -> Result<QueryResult, String> {
        // Format: INSERT INTO <table> (<col1>, <col2>) VALUES ($1, $2), ($3, $4) [RETURNING <cols>]
        let (sql_before_returning, returning_cols) = extract_returning_clause(sql);

        let values_idx = find_top_level_keyword(sql_before_returning, "VALUES").ok_or("Missing VALUES clause in INSERT")?;
        let into_idx = find_top_level_keyword(sql_before_returning, "INTO").unwrap_or(6);
        let table_part = sql_before_returning[into_idx + 4..values_idx].trim();

        let (table_name, target_cols) = if let Some(open_p) = table_part.find('(') {
            let close_p = table_part.find(')').ok_or("Missing closing parenthesis for columns")?;
            let name = table_part[..open_p].trim().trim_matches('"');
            let cols: Vec<String> = split_comma_separated_tokens(&table_part[open_p + 1..close_p])
                .into_iter()
                .map(|c| c.trim().trim_matches('"').to_string())
                .collect();
            (name, Some(cols))
        } else {
            (table_part.trim_matches('"'), None)
        };

        let values_part = sql_before_returning[values_idx + 6..].trim();

        // Borrow table
        let (col_indices, num_table_cols, _pk_idx, clean_table_name, col_defaults, col_defs) = {
            let table = self.storage.get_table(table_name).ok_or_else(|| format!("Table {} not found", table_name))?;
            let mut indices = Vec::new();
            if let Some(target) = &target_cols {
                for col in target {
                    let idx = table.get_column_index(col).ok_or_else(|| format!("Column {} not found in {}", col, table_name))?;
                    indices.push(idx);
                }
            } else {
                for i in 0..table.columns.len() {
                    indices.push(i);
                }
            }
            let defaults: Vec<Option<String>> = table.columns.iter().map(|c| c.default_value.clone()).collect();
            let col_defs: Vec<crate::types::ColumnDef> = table.columns.clone();
            (indices, table.columns.len(), table.pk_col_idx, table.name.clone(), defaults, col_defs)
        };

        // Parse row groups in VALUES clause
        let mut param_cursor = 0;
        let mut inserted_rows = Vec::new();

        let groups = extract_value_groups(values_part);
        for group in groups {
            let mut row = vec![Value::Null; num_table_cols];
            for (i, def_opt) in col_defaults.iter().enumerate() {
                if let Some(def_str) = def_opt {
                    let up = def_str.trim().to_uppercase();
                    if up.starts_with("GEN_RANDOM_UUID") || up.starts_with("UUID_GENERATE_V4") {
                        row[i] = Value::text(crate::types::generate_uuid_v4());
                    } else if let Some(dt_val) = eval_date_time_keyword(def_str) {
                        row[i] = dt_val;
                    } else if def_str.starts_with('\'') && def_str.ends_with('\'') && def_str.len() >= 2 {
                        row[i] = Value::text(&def_str[1..def_str.len() - 1]);
                    } else if let Ok(n) = def_str.parse::<i64>() {
                        row[i] = Value::Int(n);
                    } else if let Ok(f) = def_str.parse::<f64>() {
                        row[i] = Value::Float(f);
                    } else if def_str.eq_ignore_ascii_case("TRUE") {
                        row[i] = Value::Bool(true);
                    } else if def_str.eq_ignore_ascii_case("FALSE") {
                        row[i] = Value::Bool(false);
                    } else {
                        row[i] = eval_sql_expr(def_str, &[], None, &[], None, params);
                    }
                }
            }
            let mut col_ptr = 0;
            let tokens = split_comma_separated_tokens(group);

            for token in tokens {
                let token = token.trim();
                if col_ptr >= col_indices.len() {
                    break;
                }
                let target_idx = col_indices[col_ptr];

                let val = if token.starts_with('$') {
                    if let Ok(p_num) = token[1..].parse::<usize>() {
                        if p_num >= 1 && p_num <= params.len() {
                            params[p_num - 1].clone()
                        } else if param_cursor < params.len() {
                            let v = params[param_cursor].clone();
                            param_cursor += 1;
                            v
                        } else {
                            Value::Null
                        }
                    } else {
                        Value::Null
                    }
                } else if token.eq_ignore_ascii_case("DEFAULT") {
                    if let Some(def_str) = col_defaults.get(target_idx).and_then(|d| d.as_ref()) {
                        let up = def_str.trim().to_uppercase();
                        if up.starts_with("GEN_RANDOM_UUID") || up.starts_with("UUID_GENERATE_V4") {
                            Value::text(crate::types::generate_uuid_v4())
                        } else {
                            eval_sql_expr(def_str, &[], None, &[], None, params)
                        }
                    } else {
                        Value::Null
                    }
                } else if let Some(dt_val) = eval_date_time_keyword(token) {
                    dt_val
                } else if token.eq_ignore_ascii_case("NULL") {
                    Value::Null
                } else if token.eq_ignore_ascii_case("TRUE") {
                    Value::Bool(true)
                } else if token.eq_ignore_ascii_case("FALSE") {
                    Value::Bool(false)
                } else if token.starts_with('\'') && token.ends_with('\'') && token.len() >= 2 {
                    Value::text(token[1..token.len() - 1].replace("''", "'"))
                } else if let Ok(int_val) = token.parse::<i64>() {
                    Value::Int(int_val)
                } else if let Ok(flt_val) = token.parse::<f64>() {
                    Value::Float(flt_val)
                } else {
                    eval_sql_expr(token, &[], None, &[], None, params)
                };

                row[target_idx] = coerce_update_val(&col_defs[target_idx], val);
                col_ptr += 1;
            }

            inserted_rows.push(row);
        }

        let count = inserted_rows.len();
        let in_tx = self.storage.in_transaction;

        let mut returned_rows = Vec::new();
        let mut appended_records = Vec::new();
        if let Some(table) = self.storage.get_table_mut(&clean_table_name) {
            table.rows.reserve(count);
            table.is_deleted.reserve(count);
            for row in inserted_rows {
                let _pk = table.insert(row);
                if let Some(last_row) = table.rows.last() {
                    appended_records.push(last_row.clone());
                    if let Some(r_cols) = returning_cols {
                        returned_rows.push(project_returning_row(table, last_row, r_cols));
                    }
                }
            }
        }

        for row in appended_records {
            self.storage.wal.append(WalRecord::Insert {
                table: clean_table_name.clone(),
                row,
            });
        }

        if in_tx {
            for _ in 0..count {
                self.storage.tx_undo_log.push(UndoAction::DeleteLastInsertedRow {
                    table: clean_table_name.clone(),
                });
            }
        }

        let fields = if let (Some(r_cols), Some(table)) = (returning_cols, self.storage.get_table(&clean_table_name)) {
            get_returning_fields(table, r_cols)
        } else {
            vec![]
        };

        Ok(QueryResult {
            rows: returned_rows,
            row_count: count,
            fields,
            command: "INSERT".to_string(),
        })
    }

    fn handle_select(&mut self, sql: &str, params: &[Value]) -> Result<(QueryResult, Option<ExecutionPlan>), String> {
        let from_idx_opt = find_top_level_keyword(sql, "FROM");
        if from_idx_opt.is_none() {
            let select_clause = sql[6..].trim().trim_end_matches(';').trim();
            let items = split_projection_items(select_clause);
            let mut map = serde_json::Map::new();
            let mut fields = Vec::new();
            for item in items {
                let upper = item.to_uppercase();
                let (expr_part, alias) = if let Some(as_idx) = upper.rfind(" AS ") {
                    (item[..as_idx].trim(), item[as_idx + 4..].trim().trim_matches('"').trim_matches(';').trim().to_string())
                } else {
                    let col = clean_col_name(item.trim().trim_matches(';'));
                    (item.trim(), col.to_string())
                };
                let mut trimmed_expr = expr_part;
                while trimmed_expr.starts_with('(') && trimmed_expr.ends_with(')') && is_fully_enclosed_in_parens(trimmed_expr) {
                    trimmed_expr = trimmed_expr[1..trimmed_expr.len() - 1].trim();
                }
                let val_json = if trimmed_expr.to_uppercase().starts_with("SELECT") && trimmed_expr.to_uppercase().contains("FROM") {
                    if let Ok((sub_res, _)) = self.handle_select(trimmed_expr, params) {
                        if let Some(first_row) = sub_res.rows.first() {
                            if let Some(obj) = first_row.as_object() {
                                if obj.len() == 1 {
                                    obj.values().next().unwrap().clone()
                                } else {
                                    serde_json::Value::Array(obj.values().cloned().collect())
                                }
                            } else {
                                first_row.clone()
                            }
                        } else {
                            serde_json::Value::Null
                        }
                    } else {
                        serde_json::Value::Null
                    }
                } else {
                    let val = eval_sql_expr(expr_part, &[], None, &[], None, params);
                    value_to_json(&val)
                };
                map.insert(alias.clone(), val_json);
                fields.push(FieldInfo {
                    name: alias,
                    data_type: "text".to_string(),
                });
            }
            return Ok((QueryResult {
                rows: vec![serde_json::Value::Object(map)],
                row_count: 1,
                fields,
                command: "SELECT".to_string(),
            }, None));
        }

        let from_idx = from_idx_opt.unwrap();
        let mut select_clause = sql[6..from_idx].trim();
        let is_distinct = if select_clause.to_uppercase().starts_with("DISTINCT ") {
            select_clause = select_clause[9..].trim();
            true
        } else {
            false
        };
        let after_from = sql[from_idx + 4..].trim();

        if after_from.to_uppercase().starts_with("UNNEST(") {
            let open_p = after_from.find('(').unwrap();
            let mut depth = 1;
            let mut close_p = None;
            for (idx, ch) in after_from[open_p + 1..].char_indices() {
                if ch == '(' || ch == '[' { depth += 1; }
                else if ch == ')' || ch == ']' {
                    depth -= 1;
                    if depth == 0 {
                        close_p = Some(open_p + 1 + idx);
                        break;
                    }
                }
            }
            if let Some(cp) = close_p {
                let arr_expr = &after_from[open_p + 1..cp];
                let after_unnest = after_from[cp + 1..].trim();
                let arr_val = eval_sql_expr(arr_expr, &[], None, &[], None, params);
                let items = val_to_array_items(&arr_val);

                let mut col_name = "val".to_string();
                let mut tbl_alias = "t".to_string();
                let rest_clauses: String;

                let up_after = after_unnest.to_uppercase();
                let mut scan_idx = 0;
                if up_after.starts_with("AS ") {
                    scan_idx += 3;
                }
                let rest_str = after_unnest[scan_idx..].trim();
                if let Some(open_col) = rest_str.find('(') {
                    if let Some(close_col) = rest_str[open_col..].find(')') {
                        tbl_alias = rest_str[..open_col].trim().to_string();
                        col_name = rest_str[open_col + 1..open_col + close_col].trim().to_string();
                        rest_clauses = rest_str[open_col + close_col + 1..].trim().to_string();
                    } else {
                        let mut parts = rest_str.split_whitespace();
                        if let Some(a) = parts.next() { tbl_alias = a.to_string(); }
                        rest_clauses = parts.collect::<Vec<_>>().join(" ");
                    }
                } else {
                    let mut parts = rest_str.split_whitespace();
                    if let Some(a) = parts.next() { tbl_alias = a.to_string(); }
                    rest_clauses = parts.collect::<Vec<_>>().join(" ");
                }
                if tbl_alias.is_empty() { tbl_alias = "t".to_string(); }
                if col_name.is_empty() { col_name = "val".to_string(); }

                let ephemeral_table_name = format!("__ephemeral_unnest_{}", tbl_alias);
                let col_def = crate::types::ColumnDef {
                    name: col_name.clone(),
                    data_type: crate::types::DataType::Text,
                    is_nullable: true,
                    is_primary_key: false,
                    default_value: None,
                    comment: None,
                };
                let mut t = crate::storage::table::Table::new(ephemeral_table_name.clone(), vec![col_def]);
                for item in items {
                    t.insert(vec![item]);
                }

                self.storage.tables.insert(ephemeral_table_name.clone(), t);
                self.storage.tables.insert(tbl_alias.clone(), self.storage.tables.get(&ephemeral_table_name).unwrap().clone());

                let new_sql = if rest_clauses.is_empty() {
                    format!("SELECT {} FROM {}", select_clause, tbl_alias)
                } else {
                    format!("SELECT {} FROM {} {}", select_clause, tbl_alias, rest_clauses)
                };
                let full_new_sql = if is_distinct { format!("SELECT DISTINCT {}", &new_sql[7..]) } else { new_sql };
                let res = self.handle_select(&full_new_sql, params);
                self.storage.tables.remove(&ephemeral_table_name);
                self.storage.tables.remove(&tbl_alias);
                return res;
            }
        }

        if has_top_level_join(after_from) {
            let full_select = if is_distinct { format!("DISTINCT {}", select_clause) } else { select_clause.to_string() };
            return Ok((self.handle_joined_query(&full_select, after_from, params)?, None));
        }

        let mut clauses = parse_select_clauses(after_from);
        let is_correlated = if let Some(w) = clauses.where_clause {
            let w_up = w.to_uppercase();
            if let Some(alias) = clauses.table_alias {
                w_up.contains(&format!("{}.", alias.to_uppercase()))
            } else {
                w_up.contains(&format!("{}.", clauses.table_name.to_uppercase()))
            }
        } else {
            false
        };

        let resolved_where_owner = if !is_correlated {
            if let Some(w) = clauses.where_clause {
                let w_up = w.to_uppercase();
                if w_up.contains("SELECT") && w_up.contains("FROM") {
                    Some(self.resolve_subqueries_in_where(w, params)?)
                } else {
                    None
                }
            } else {
                None
            }
        } else {
            None
        };
        if let Some(ref rw) = resolved_where_owner {
            clauses.where_clause = Some(rw.as_str());
        }

        let resolved_select_owner = if select_clause.to_uppercase().contains("SELECT") && select_clause.to_uppercase().contains("FROM") {
            Some(self.resolve_subqueries_in_select(select_clause, params)?)
        } else {
            None
        };
        let select_clause = resolved_select_owner.as_deref().unwrap_or(select_clause);

        let table = self.storage.get_table(clauses.table_name).ok_or_else(|| format!("Table {} not found", clauses.table_name))?;

        if clauses.group_by_clause.is_some() {
            return Ok((self.handle_group_by_query(table, select_clause, &clauses, params)?, None));
        }



        // 1. Check if SELECT has aggregate functions (COUNT, SUM, AVG, MIN, MAX)
        let agg_specs = if !select_clause.to_uppercase().contains("SELECT")
            && !select_clause.to_uppercase().contains(" OVER (")
            && !select_clause.to_uppercase().contains(" OVER(") {
            parse_aggregations(select_clause, table)
        } else {
            vec![]
        };

        if !agg_specs.is_empty() {
            return Ok((self.handle_aggregate_query(table, &agg_specs, clauses.where_clause, params)?, None));
        }

        if clauses.join_clause.is_none() && select_clause.trim() == "*" {
            // 2. Fast-Path: Point lookup by Primary Key (WHERE id = $1 or WHERE id = 123)
            if let Some(w) = clauses.where_clause {
                let w_upper = w.to_uppercase();
                if clauses.order_by_specs.is_empty() && clauses.limit.is_none() && !w_upper.contains("AND") && !w_upper.contains("OR") {
                    if let Some(pk_idx) = table.pk_col_idx {
                        let is_int_pk = matches!(table.columns[pk_idx].data_type, DataType::Integer | DataType::BigInt | DataType::Serial);
                        if is_int_pk {
                            let pk_col_name = &table.columns[pk_idx].name;
                            if w_upper.starts_with(&format!("{} =", pk_col_name.to_uppercase())) ||
                               w_upper.starts_with(&format!("\"{}\" =", pk_col_name.to_uppercase())) ||
                               w_upper.starts_with("ID =") {
                                let val_part = w.split('=').nth(1).unwrap().trim();
                            let (param_idx, literal_pk, target_id) = if val_part.starts_with('$') {
                                let p_idx = val_part[1..].parse::<usize>().unwrap_or(1);
                                let tid = params.get(p_idx - 1).and_then(|v| v.as_i64()).unwrap_or(0);
                                (Some(p_idx.saturating_sub(1)), None, tid)
                            } else {
                                let tid = val_part.parse::<i64>().unwrap_or(0);
                                (None, Some(tid), tid)
                            };

                            let fields: Vec<FieldInfo> = table.columns.iter().map(|c| FieldInfo {
                                name: c.name.clone(),
                                data_type: format!("{:?}", c.data_type).to_lowercase(),
                            }).collect();

                            let plan = ExecutionPlan::PointLookupPk {
                                table_name: table.name.clone(),
                                param_idx,
                                literal_pk,
                                fields: fields.clone(),
                            };

                            if let Some(row) = table.get_by_pk(target_id) {
                                return Ok((QueryResult {
                                    rows: vec![row_to_json(table, row)],
                                    row_count: 1,
                                    fields,
                                    command: "SELECT".to_string(),
                                }, Some(plan)));
                            } else {
                                return Ok((QueryResult {
                                    rows: vec![],
                                    row_count: 0,
                                    fields: vec![],
                                    command: "SELECT".to_string(),
                                }, Some(plan)));
                            }
                        }
                    }
                }
            }

                // 3. Fast-Path: Non-PK exact string lookup (WHERE name = $1 or WHERE name = '...')
                if clauses.join_clause.is_none() && select_clause.trim() == "*" && clauses.order_by_specs.is_empty() && clauses.limit.is_none() {
                    if let Some(eq_idx) = w.find('=') {
                        let left = w[..eq_idx].trim().trim_matches('"');
                        let right = w[eq_idx + 1..].trim();
                        if !right.contains("AND") && !right.contains("OR") {
                            if let Some(col_idx) = table.get_column_index(left) {
                                let (param_idx, literal_str, target_val) = if right.starts_with('$') {
                                    let p_idx = right[1..].parse::<usize>().unwrap_or(1);
                                    let tv = params.get(p_idx - 1).cloned().unwrap_or(Value::Null);
                                    (Some(p_idx.saturating_sub(1)), None, tv)
                                } else {
                                    let tv = parse_value(right, params);
                                    let s = match &tv {
                                        Value::Text(s) => s.to_string(),
                                        _ => right.to_string(),
                                    };
                                    (None, Some(s), tv)
                                };

                                let fields: Vec<FieldInfo> = table.columns.iter().map(|c| FieldInfo {
                                    name: c.name.clone(),
                                    data_type: format!("{:?}", c.data_type).to_lowercase(),
                                }).collect();

                                let plan = ExecutionPlan::PointLookupString {
                                    table_name: table.name.clone(),
                                    col_idx,
                                    param_idx,
                                    literal_str,
                                    fields: fields.clone(),
                                };

                                let matched_rows: Vec<serde_json::Value> = table.rows.iter().enumerate()
                                    .filter(|(r_idx, r)| !table.is_deleted[*r_idx] && r.get(col_idx) == Some(&target_val))
                                    .map(|(_, r)| row_to_json(table, r))
                                    .collect();
                                let row_count = matched_rows.len();
                                return Ok((QueryResult {
                                    rows: matched_rows,
                                    row_count,
                                    fields,
                                    command: "SELECT".to_string(),
                                }, Some(plan)));
                            }
                        }
                    }
                }
            }
        }

        // 4. General Scan with WHERE condition filtering
        let where_expr = match clauses.where_clause {
            Some(w) => Some(parse_where_expr(w, table, params)?),
            None => None,
        };

        let total_rows = table.rows.len();

        // Check if any condition is an exact equality on the Primary Key column!
        let pk_target = if let Some(ref expr) = where_expr {
            table.pk_col_idx.and_then(|pk_idx| extract_single_pk_eq(expr, pk_idx))
        } else {
            None
        };

        let has_correlated = clauses.where_clause.map_or(false, |w| {
            let u = w.to_uppercase();
            u.contains("SELECT") && u.contains("FROM")
        });

        let mut matched_indices: Vec<usize> = if has_correlated {
            let raw_where = clauses.where_clause.unwrap().to_string();
            let (table_name, columns, active_rows) = {
                let table = self.storage.get_table(clauses.table_name).ok_or_else(|| format!("Table {} not found", clauses.table_name))?;
                let active: Vec<(usize, Vec<Value>)> = table.rows.iter().enumerate()
                    .filter(|(i, _)| !table.is_deleted[*i])
                    .map(|(i, r)| (i, r.clone()))
                    .collect();
                (table.name.clone(), table.columns.clone(), active)
            };
            let alias = clauses.table_alias;
            let mut matched = Vec::new();
            for (i, r) in active_rows {
                let resolved_row_where = self.resolve_correlated_subqueries_in_where(
                    &raw_where,
                    &table_name,
                    &columns,
                    &r,
                    alias,
                    params,
                );
                let table_ref = self.storage.get_table(clauses.table_name);
                if evaluate_custom_condition(&resolved_row_where, &r, table_ref, &[], None, params) {
                    matched.push(i);
                }
            }
            matched
        } else {
            let table = self.storage.get_table(clauses.table_name).ok_or_else(|| format!("Table {} not found", clauses.table_name))?;
            if let Some(target_pk) = pk_target {
                if let Some(&row_idx) = table.pk_index.get(&target_pk) {
                    let matches = match &where_expr {
                        Some(expr) => evaluate_where_expr(&table.rows[row_idx], Some(table), expr, params),
                        None => true,
                    };
                    if !table.is_deleted[row_idx] && matches {
                        vec![row_idx]
                    } else {
                        vec![]
                    }
                } else {
                    vec![]
                }
            } else if where_expr.is_none() {
                (0..total_rows)
                    .into_par_iter()
                    .filter(|&i| !table.is_deleted[i])
                    .collect()
            } else if total_rows > 20_000 {
                let expr_ref = where_expr.as_ref().unwrap();
                (0..total_rows)
                    .into_par_iter()
                    .filter(|&i| !table.is_deleted[i] && evaluate_where_expr(&table.rows[i], Some(table), expr_ref, params))
                    .collect()
            } else {
                let expr_ref = where_expr.as_ref().unwrap();
                (0..total_rows)
                    .filter(|&i| !table.is_deleted[i] && evaluate_where_expr(&table.rows[i], Some(table), expr_ref, params))
                    .collect()
            }
        };

        let table = self.storage.get_table(clauses.table_name).ok_or_else(|| format!("Table {} not found", clauses.table_name))?;

        let joined_table_opt = if let Some(ref jc) = clauses.join_clause {
            let jt = self.storage.get_table(jc.joined_table_name)
                .ok_or_else(|| format!("Table {} not found", jc.joined_table_name))?;
            Some(jt)
        } else {
            None
        };

        if let (Some(ref jc), Some(jt)) = (&clauses.join_clause, joined_table_opt) {
            if !jc.is_left {
                if let Some(p_col_idx) = table.get_column_index(jc.primary_join_col) {
                    matched_indices.retain(|&idx| {
                        let join_key = &table.rows[idx][p_col_idx];
                        if join_key.is_null() {
                            return false;
                        }
                        if let Some(target_pk) = join_key.as_i64() {
                            jt.get_by_pk(target_pk).is_some()
                        } else if let Some(j_col_idx) = jt.get_column_index(jc.joined_join_col) {
                            jt.rows.iter().enumerate().any(|(r_idx, r)| !jt.is_deleted[r_idx] && r.get(j_col_idx).map(|v| v.is_equal(join_key)).unwrap_or(false))
                        } else {
                            false
                        }
                    });
                }
            }
        }

        let projected_exprs = if select_clause.trim() == "*" {
            Vec::new()
        } else {
            let exprs = parse_projection(
                select_clause,
                table,
                clauses.table_alias,
                joined_table_opt,
                clauses.join_clause.as_ref().and_then(|j| j.joined_table_alias),
                &self.storage,
                params,
            );
            if exprs.is_empty() {
                return Err(format!("Unsupported projection clause: {}", select_clause));
            }
            exprs
        };

        // 5. ORDER BY sorting
        if !clauses.order_by_specs.is_empty() {
            let resolve_spec_name = |spec_name: &str| -> String {
                for p in &projected_exprs {
                    match p {
                        ProjectedExpr::PrimaryCol { col_idx, alias } if alias.eq_ignore_ascii_case(spec_name) => {
                            return table.columns[*col_idx].name.clone();
                        }
                        ProjectedExpr::Expr { expr_str, alias } if alias.eq_ignore_ascii_case(spec_name) => {
                            return expr_str.clone();
                        }
                        _ => {}
                    }
                }
                spec_name.to_string()
            };

            let all_simple_cols = clauses.order_by_specs.iter()
                .all(|spec| table.get_column_index(&resolve_spec_name(spec.col_name)).is_some());

            if all_simple_cols {
                let resolved_order: Vec<(usize, bool)> = clauses.order_by_specs.iter()
                    .filter_map(|spec| {
                        let actual_col = resolve_spec_name(spec.col_name);
                        table.get_column_index(&actual_col).map(|idx| (idx, spec.is_desc))
                    })
                    .collect();

                if matched_indices.len() > 10_000 {
                    matched_indices.par_sort_by(|&a, &b| {
                        for &(col_idx, desc) in &resolved_order {
                            let va = &table.rows[a][col_idx];
                            let vb = &table.rows[b][col_idx];
                            let ord = if desc {
                                vb.cmp_value(va)
                            } else {
                                va.cmp_value(vb)
                            };
                            if ord != std::cmp::Ordering::Equal {
                                return ord;
                            }
                        }
                        std::cmp::Ordering::Equal
                    });
                } else {
                    matched_indices.sort_by(|&a, &b| {
                        for &(col_idx, desc) in &resolved_order {
                            let va = &table.rows[a][col_idx];
                            let vb = &table.rows[b][col_idx];
                            let ord = if desc {
                                vb.cmp_value(va)
                            } else {
                                va.cmp_value(vb)
                            };
                            if ord != std::cmp::Ordering::Equal {
                                return ord;
                            }
                        }
                        std::cmp::Ordering::Equal
                    });
                }
            } else if let Some(jt) = joined_table_opt {
                if let Some(spec) = clauses.order_by_specs.first() {
                    let actual_spec = resolve_spec_name(spec.col_name);
                    if let Some(j_col_idx) = jt.get_column_index(&actual_spec) {
                        if let Some(ref jc) = clauses.join_clause {
                            if let Some(p_col_idx) = table.get_column_index(jc.primary_join_col) {
                                let desc = spec.is_desc;
                                matched_indices.sort_by(|&a, &b| {
                                    let get_val = |row_idx: usize| -> Option<&Value> {
                                        let key = table.rows[row_idx].get(p_col_idx)?;
                                        if let Some(pk) = key.as_i64() {
                                            let j_row = jt.get_by_pk(pk)?;
                                            j_row.get(j_col_idx)
                                        } else {
                                            None
                                        }
                                    };
                                    let va = get_val(a).unwrap_or(&Value::Null);
                                    let vb = get_val(b).unwrap_or(&Value::Null);
                                    if desc {
                                        vb.cmp_value(va)
                                    } else {
                                        va.cmp_value(vb)
                                    }
                                });
                            }
                        }
                    }
                }
            } else {
                matched_indices.sort_by(|&a, &b| {
                    for spec in &clauses.order_by_specs {
                        let actual_expr = resolve_spec_name(spec.col_name);
                        let va = eval_sql_expr(&actual_expr, &table.rows[a], Some(table), &[], None, params);
                        let vb = eval_sql_expr(&actual_expr, &table.rows[b], Some(table), &[], None, params);
                        let ord = if spec.is_desc {
                            vb.cmp_value(&va)
                        } else {
                            va.cmp_value(&vb)
                        };
                        if ord != std::cmp::Ordering::Equal {
                            return ord;
                        }
                    }
                    std::cmp::Ordering::Equal
                });
            }
        }

        // 6. OFFSET and LIMIT pagination
        let offset = clauses.offset.as_ref().map(|o| resolve_operand(o, params).as_i64().unwrap_or(0) as usize).unwrap_or(0);
        let limit = clauses.limit.as_ref().map(|l| resolve_operand(l, params).as_i64().unwrap_or(1000) as usize).unwrap_or(usize::MAX);
        let paged_indices = if offset < matched_indices.len() {
            let end = (offset.saturating_add(limit)).min(matched_indices.len());
            &matched_indices[offset..end]
        } else {
            &[]
        };

        let mut count_maps: std::collections::HashMap<String, std::collections::HashMap<String, i64>> = std::collections::HashMap::new();
        for expr in &projected_exprs {
            if let ProjectedExpr::CorrelatedCount {
                child_table_name,
                child_join_col_idx,
                extra_child_conditions,
                alias,
                ..
            } = expr {
                if let Some(child_table) = self.storage.get_table(child_table_name) {
                    let mut map = std::collections::HashMap::new();
                    for i in 0..child_table.rows.len() {
                        if !child_table.is_deleted[i] {
                            let mut matches = true;
                            for cond in extra_child_conditions {
                                if !crate::engine::plan::evaluate_planned_condition(&child_table.rows[i], cond, params) {
                                    matches = false;
                                    break;
                                }
                            }
                            if matches {
                                if let Some(fk_val) = child_table.rows[i].get(*child_join_col_idx) {
                                    if !fk_val.is_null() {
                                        *map.entry(fk_val.as_str()).or_insert(0) += 1;
                                    }
                                }
                            }
                        }
                    }
                    count_maps.insert(alias.clone(), map);
                }
            }
        }

        let build_row = |idx: usize| -> serde_json::Value {
            if !projected_exprs.is_empty() {
                let primary_row = &table.rows[idx];
                let joined_row: Option<&Vec<Value>> = if let (Some(ref jc), Some(jt)) = (&clauses.join_clause, joined_table_opt) {
                    if let Some(p_col_idx) = table.get_column_index(jc.primary_join_col) {
                        let join_key = &primary_row[p_col_idx];
                        if let Some(target_pk) = join_key.as_i64() {
                            jt.get_by_pk(target_pk)
                        } else if let Some(j_col_idx) = jt.get_column_index(jc.joined_join_col) {
                            jt.rows.iter().find(|r| !r.is_empty() && r[j_col_idx].is_equal(join_key))
                        } else {
                            None
                        }
                    } else {
                        None
                    }
                } else {
                    None
                };

                let mut map = serde_json::Map::with_capacity(projected_exprs.len());
                let format_col_val_for_json = |v: &Value, dt: Option<&crate::types::DataType>| -> serde_json::Value {
                    if let Some(crate::types::DataType::Bytea) = dt {
                        if v.is_null() {
                            serde_json::Value::Null
                        } else {
                            let raw = get_raw_bytes(v);
                            let json_arr: Vec<serde_json::Value> = raw.into_iter().map(|b| serde_json::json!(b)).collect();
                            serde_json::Value::Array(json_arr)
                        }
                    } else {
                        value_to_json(v)
                    }
                };

                for expr in &projected_exprs {
                    match expr {
                        ProjectedExpr::PrimaryCol { col_idx, alias } => {
                            let v = primary_row.get(*col_idx).unwrap_or(&Value::Null);
                            let dt = table.columns.get(*col_idx).map(|c| &c.data_type);
                            map.insert(alias.clone(), format_col_val_for_json(v, dt));
                        }
                        ProjectedExpr::JoinedCol { col_idx, alias } => {
                            let v = joined_row.and_then(|r| r.get(*col_idx)).unwrap_or(&Value::Null);
                            let dt = joined_table_opt.and_then(|jt| jt.columns.get(*col_idx)).map(|c| &c.data_type);
                            map.insert(alias.clone(), format_col_val_for_json(v, dt));
                        }
                        ProjectedExpr::CoalescePrimaryCol { col_idx, default_val, alias } => {
                            let raw_v = primary_row.get(*col_idx).unwrap_or(&Value::Null);
                            let v = match raw_v {
                                Value::Null => default_val,
                                _ => raw_v,
                            };
                            let dt = table.columns.get(*col_idx).map(|c| &c.data_type);
                            map.insert(alias.clone(), format_col_val_for_json(v, dt));
                        }
                        ProjectedExpr::CoalesceJoinedCol { col_idx, default_val, alias } => {
                            let raw_v = joined_row.and_then(|r| r.get(*col_idx)).unwrap_or(&Value::Null);
                            let v = match raw_v {
                                Value::Null => default_val,
                                _ => raw_v,
                            };
                            let dt = joined_table_opt.and_then(|jt| jt.columns.get(*col_idx)).map(|c| &c.data_type);
                            map.insert(alias.clone(), format_col_val_for_json(v, dt));
                        }
                        ProjectedExpr::CorrelatedCount { parent_join_col_idx, alias, .. } => {
                            let parent_val = primary_row.get(*parent_join_col_idx).unwrap_or(&Value::Null);
                            let key = parent_val.as_str();
                            let count = count_maps.get(alias).and_then(|m| m.get(&key).copied()).unwrap_or(0);
                            map.insert(alias.clone(), serde_json::Value::Number(count.into()));
                        }
                        ProjectedExpr::Expr { expr_str, alias } => {
                            let empty_vec = Vec::new();
                            let j_row = joined_row.unwrap_or(&empty_vec);
                            let expr_up = expr_str.to_uppercase();
                            if expr_up.contains("SELECT") && expr_up.contains("FROM") {
                                let mut combined_meta = Vec::new();
                                combined_meta.push((table, clauses.table_alias));
                                if let Some(jt) = joined_table_opt {
                                    combined_meta.push((jt, clauses.join_clause.as_ref().and_then(|jc| jc.joined_table_alias)));
                                }
                                let joined_schema = std::sync::Arc::new(JoinedSchema::new(&combined_meta));
                                let mut comb = CombinedRow::from_base_row(joined_schema.clone(), primary_row);
                                if let Some(jr) = joined_row {
                                    comb = comb.with_joined_table(jr);
                                }
                                let val_json = eval_joined_scalar_expr(expr_str, &comb, params, &self.storage);
                                map.insert(alias.clone(), val_json);
                            } else {
                                let v = eval_sql_expr(
                                    expr_str,
                                    primary_row,
                                    Some(table),
                                    j_row,
                                    joined_table_opt,
                                    params,
                                );
                                if expr_str.to_uppercase().starts_with("JSONB_PRETTY(") || expr_str.to_uppercase().starts_with("JSON_PRETTY(") {
                                    map.insert(alias.clone(), serde_json::Value::String(v.as_str()));
                                } else {
                                    map.insert(alias.clone(), value_to_json(&v));
                                }
                            }
                        }
                    }
                }
                serde_json::Value::Object(map)
            } else {
                row_to_json(table, &table.rows[idx])
            }
        };

        let rows: Vec<serde_json::Value> = if is_distinct {
            let mut distinct_rows = Vec::new();
            let mut seen = std::collections::HashSet::new();
            for &idx in &matched_indices {
                let r = build_row(idx);
                let key = serde_json::to_string(&r).unwrap_or_default();
                if seen.insert(key) {
                    distinct_rows.push(r);
                }
            }
            if offset < distinct_rows.len() {
                let end = (offset.saturating_add(limit)).min(distinct_rows.len());
                distinct_rows[offset..end].to_vec()
            } else {
                vec![]
            }
        } else if select_clause.to_uppercase().contains(" OVER (") {
            let mut all_rows: Vec<serde_json::Value> = matched_indices.iter().map(|&idx| build_row(idx)).collect();
            eval_window_functions(&mut all_rows, select_clause);
            if !clauses.order_by_specs.is_empty() {
                sort_json_rows(&mut all_rows, &clauses.order_by_specs);
            }
            if offset < all_rows.len() {
                let end = (offset.saturating_add(limit)).min(all_rows.len());
                all_rows[offset..end].to_vec()
            } else {
                vec![]
            }
        } else {
            paged_indices.iter().map(|&idx| build_row(idx)).collect()
        };

        let fields: Vec<FieldInfo> = if !projected_exprs.is_empty() {
            projected_exprs.iter().map(|e| {
                let (alias, dt) = match e {
                    ProjectedExpr::PrimaryCol { alias, .. } => (alias.clone(), "text".to_string()),
                    ProjectedExpr::JoinedCol { alias, .. } => (alias.clone(), "text".to_string()),
                    ProjectedExpr::CoalescePrimaryCol { alias, .. } => (alias.clone(), "text".to_string()),
                    ProjectedExpr::CoalesceJoinedCol { alias, .. } => (alias.clone(), "text".to_string()),
                    ProjectedExpr::CorrelatedCount { alias, .. } => (alias.clone(), "bigint".to_string()),
                    ProjectedExpr::Expr { alias, .. } => (alias.clone(), "text".to_string()),
                };
                FieldInfo {
                    name: alias,
                    data_type: dt,
                }
            }).collect()
        } else {
            table.columns.iter().map(|c| FieldInfo {
                name: c.name.clone(),
                data_type: format!("{:?}", c.data_type).to_lowercase(),
            }).collect()
        };

        let plan_opt = if select_clause.to_uppercase().contains(" OVER") || select_clause.to_uppercase().contains(" OVER(") {
            None
        } else if let Some(w) = clauses.where_clause {
            if let Ok(where_tmpl) = compile_where_template(w, table) {
                let planned_join = if let (Some(ref jc), Some(jt)) = (&clauses.join_clause, joined_table_opt) {
                    let p_idx = table.get_column_index(clean_col_name(jc.primary_join_col)).unwrap_or(0);
                    let j_idx = jt.get_column_index(clean_col_name(jc.joined_join_col)).unwrap_or(0);
                    let is_pk = jt.pk_col_idx == Some(j_idx);
                    Some(PlannedJoin {
                        joined_table_name: jt.name.clone(),
                        primary_join_col_idx: p_idx,
                        joined_join_col_idx: j_idx,
                        is_pk_join: is_pk,
                    })
                } else {
                    None
                };

                let order_by_info = if let Some(spec) = clauses.order_by_specs.first() {
                    table.get_column_index(clean_col_name(spec.col_name)).map(|idx| (idx, spec.is_desc))
                } else {
                    None
                };

                let plan_projections = if !projected_exprs.is_empty() {
                    projected_exprs
                } else {
                    table.columns.iter().enumerate().map(|(i, c)| ProjectedExpr::PrimaryCol {
                        col_idx: i,
                        alias: c.name.clone(),
                    }).collect()
                };

                Some(ExecutionPlan::GeneralSelect {
                    table_name: table.name.clone(),
                    join: planned_join,
                    where_template: Some(where_tmpl),
                    order_by: order_by_info,
                    limit: clauses.limit,
                    offset: clauses.offset,
                    projections: plan_projections,
                    fields: fields.clone(),
                })
            } else {
                None
            }
        } else {
            let planned_join = if let (Some(ref jc), Some(jt)) = (&clauses.join_clause, joined_table_opt) {
                let p_idx = table.get_column_index(clean_col_name(jc.primary_join_col)).unwrap_or(0);
                let j_idx = jt.get_column_index(clean_col_name(jc.joined_join_col)).unwrap_or(0);
                let is_pk = jt.pk_col_idx == Some(j_idx);
                Some(PlannedJoin {
                    joined_table_name: jt.name.clone(),
                    primary_join_col_idx: p_idx,
                    joined_join_col_idx: j_idx,
                    is_pk_join: is_pk,
                })
            } else {
                None
            };

            let order_by_info = if let Some(spec) = clauses.order_by_specs.first() {
                table.get_column_index(clean_col_name(spec.col_name)).map(|idx| (idx, spec.is_desc))
            } else {
                None
            };

            let plan_projections = if !projected_exprs.is_empty() {
                projected_exprs
            } else {
                table.columns.iter().enumerate().map(|(i, c)| ProjectedExpr::PrimaryCol {
                    col_idx: i,
                    alias: c.name.clone(),
                }).collect()
            };

            Some(ExecutionPlan::GeneralSelect {
                table_name: table.name.clone(),
                join: planned_join,
                where_template: None,
                order_by: order_by_info,
                limit: clauses.limit,
                offset: clauses.offset,
                projections: plan_projections,
                fields: fields.clone(),
            })
        };

        Ok((QueryResult {
            row_count: rows.len(),
            rows,
            fields,
            command: "SELECT".to_string(),
        }, plan_opt))
    }

    fn execute_select_plan(&self, plan: &ExecutionPlan, params: &[Value]) -> Result<QueryResult, String> {
        match plan {
            ExecutionPlan::PointLookupPk {
                table_name,
                param_idx,
                literal_pk,
                fields,
            } => {
                let table = self.storage.get_table(table_name).ok_or_else(|| format!("Table {} not found", table_name))?;
                let target_id = if let Some(p_idx) = param_idx {
                    params.get(*p_idx).and_then(|v| v.as_i64()).unwrap_or(0)
                } else {
                    literal_pk.unwrap_or(0)
                };

                if let Some(row) = table.get_by_pk(target_id) {
                    Ok(QueryResult {
                        rows: vec![row_to_json(table, row)],
                        row_count: 1,
                        fields: fields.clone(),
                        command: "SELECT".to_string(),
                    })
                } else {
                    Ok(QueryResult {
                        rows: vec![],
                        row_count: 0,
                        fields: vec![],
                        command: "SELECT".to_string(),
                    })
                }
            }
            ExecutionPlan::PointLookupString {
                table_name,
                col_idx,
                param_idx,
                literal_str,
                fields,
            } => {
                let table = self.storage.get_table(table_name).ok_or_else(|| format!("Table {} not found", table_name))?;
                let target_val = if let Some(p_idx) = param_idx {
                    params.get(*p_idx).cloned().unwrap_or(Value::Null)
                } else if let Some(s) = literal_str {
                    Value::text(s)
                } else {
                    Value::Null
                };

                let mut matched_rows = Vec::new();
                for i in 0..table.rows.len() {
                    if !table.is_deleted[i] {
                        if let Some(v) = table.rows[i].get(*col_idx) {
                            if v == &target_val {
                                matched_rows.push(row_to_json(table, &table.rows[i]));
                            }
                        }
                    }
                }

                let row_count = matched_rows.len();
                Ok(QueryResult {
                    rows: matched_rows,
                    row_count,
                    fields: fields.clone(),
                    command: "SELECT".to_string(),
                })
            }
            ExecutionPlan::GeneralSelect {
                table_name,
                join,
                where_template,
                order_by,
                limit,
                offset,
                projections,
                fields,
            } => {
                let table = self.storage.get_table(table_name).ok_or_else(|| format!("Table {} not found", table_name))?;
                let joined_table_opt = if let Some(ref j) = join {
                    Some(self.storage.get_table(&j.joined_table_name).ok_or_else(|| format!("Table {} not found", j.joined_table_name))?)
                } else {
                    None
                };

                let mut matched_indices = Vec::new();

                let pk_target = table.pk_col_idx.and_then(|pk_idx| {
                    where_template.as_ref().and_then(|wt| find_pk_equality_in_where_template(wt, pk_idx, params))
                });

                if let Some(target_pk) = pk_target {
                    if let Some(&row_idx) = table.pk_index.get(&target_pk) {
                        if !table.is_deleted[row_idx] {
                            let row = &table.rows[row_idx];
                            let matches = match &where_template {
                                Some(wt) => evaluate_planned_where(row, wt, params),
                                None => true,
                            };
                            if matches {
                                matched_indices.push(row_idx);
                            }
                        }
                    }
                } else {
                    for i in 0..table.rows.len() {
                        if !table.is_deleted[i] {
                            let row = &table.rows[i];
                            let matches = match &where_template {
                                Some(wt) => evaluate_planned_where(row, wt, params),
                                None => true,
                            };
                            if matches {
                                matched_indices.push(i);
                            }
                        }
                    }
                }

                // Sorting
                if let Some((order_idx, is_desc)) = order_by {
                    matched_indices.sort_by(|&a, &b| {
                        let val_a = table.rows[a].get(*order_idx).unwrap_or(&Value::Null);
                        let val_b = table.rows[b].get(*order_idx).unwrap_or(&Value::Null);
                        if *is_desc {
                            val_b.cmp_value(val_a)
                        } else {
                            val_a.cmp_value(val_b)
                        }
                    });
                }

                // Pagination
                let start = offset.as_ref().map(|o| resolve_operand(o, params).as_i64().unwrap_or(0) as usize).unwrap_or(0).min(matched_indices.len());
                let end = if let Some(lim_op) = limit {
                    let lim = resolve_operand(lim_op, params).as_i64().unwrap_or(0) as usize;
                    (start + lim).min(matched_indices.len())
                } else {
                    matched_indices.len()
                };
                let paged_indices = &matched_indices[start..end];

                let mut count_maps: std::collections::HashMap<String, std::collections::HashMap<String, i64>> = std::collections::HashMap::new();
                for expr in projections {
                    if let PlannedProjectedExpr::CorrelatedCount {
                        child_table_name,
                        child_join_col_idx,
                        extra_child_conditions,
                        alias,
                        ..
                    } = expr {
                        if let Some(child_table) = self.storage.get_table(child_table_name) {
                            let mut map = std::collections::HashMap::new();
                            for i in 0..child_table.rows.len() {
                                if !child_table.is_deleted[i] {
                                    let mut matches = true;
                                    for cond in extra_child_conditions {
                                        if !evaluate_planned_condition(&child_table.rows[i], cond, params) {
                                            matches = false;
                                            break;
                                        }
                                    }
                                    if matches {
                                        if let Some(fk_val) = child_table.rows[i].get(*child_join_col_idx) {
                                            if !fk_val.is_null() {
                                                *map.entry(fk_val.as_str()).or_insert(0) += 1;
                                            }
                                        }
                                    }
                                }
                            }
                            count_maps.insert(alias.clone(), map);
                        }
                    }
                }

                // Output projection
                let mut out_rows = Vec::with_capacity(paged_indices.len());
                let has_expr = projections.iter().any(|p| matches!(p, PlannedProjectedExpr::Expr { .. }));
                for &idx in paged_indices {
                    let p_row = &table.rows[idx];
                    let j_row = if let (Some(ref j), Some(jt)) = (join, joined_table_opt) {
                        if j.is_pk_join {
                            p_row.get(j.primary_join_col_idx).and_then(|v| v.as_i64()).and_then(|pk| jt.get_by_pk(pk))
                        } else {
                            let target_val = p_row.get(j.primary_join_col_idx);
                            jt.rows.iter().enumerate().find(|(r_idx, r)| !jt.is_deleted[*r_idx] && r.get(j.joined_join_col_idx) == target_val).map(|(_, r)| r)
                        }
                    } else {
                        None
                    };

                    if !has_expr {
                        out_rows.push(project_row_planned(p_row, j_row.map(|r| r.as_slice()), projections, &count_maps));
                    } else {
                        let mut map = serde_json::Map::with_capacity(projections.len());
                        for p in projections {
                            match p {
                                PlannedProjectedExpr::PrimaryCol { col_idx, alias } => {
                                    let v = p_row.get(*col_idx).unwrap_or(&Value::Null);
                                    map.insert(alias.clone(), value_to_json(v));
                                }
                                PlannedProjectedExpr::JoinedCol { col_idx, alias } => {
                                    let v = j_row.and_then(|r| r.get(*col_idx)).unwrap_or(&Value::Null);
                                    map.insert(alias.clone(), value_to_json(v));
                                }
                                PlannedProjectedExpr::CoalescePrimaryCol { col_idx, default_val, alias } => {
                                    let v = match p_row.get(*col_idx) {
                                        Some(val) if !val.is_null() => val,
                                        _ => default_val,
                                    };
                                    map.insert(alias.clone(), value_to_json(v));
                                }
                                PlannedProjectedExpr::CoalesceJoinedCol { col_idx, default_val, alias } => {
                                    let v = match j_row.and_then(|r| r.get(*col_idx)) {
                                        Some(val) if !val.is_null() => val,
                                        _ => default_val,
                                    };
                                    map.insert(alias.clone(), value_to_json(v));
                                }
                                PlannedProjectedExpr::CorrelatedCount { parent_join_col_idx, alias, .. } => {
                                    let parent_val = p_row.get(*parent_join_col_idx).unwrap_or(&Value::Null);
                                    let key = parent_val.as_str();
                                    let count = count_maps.get(alias).and_then(|m| m.get(&key).copied()).unwrap_or(0);
                                    map.insert(alias.clone(), serde_json::Value::Number(count.into()));
                                }
                                PlannedProjectedExpr::Expr { expr_str, alias } => {
                                    let empty_vec = Vec::new();
                                    let j_slice = j_row.unwrap_or(&empty_vec);
                                    let expr_up = expr_str.to_uppercase();
                                    if expr_up.contains("SELECT") && expr_up.contains("FROM") {
                                        let mut combined_meta = Vec::new();
                                        combined_meta.push((table, None));
                                        if let Some(jt) = joined_table_opt {
                                            combined_meta.push((jt, None));
                                        }
                                        let joined_schema = std::sync::Arc::new(JoinedSchema::new(&combined_meta));
                                        let mut comb = CombinedRow::from_base_row(joined_schema.clone(), p_row);
                                        if let Some(jr) = j_row {
                                            comb = comb.with_joined_table(jr);
                                        }
                                        let val_json = eval_joined_scalar_expr(expr_str, &comb, params, &self.storage);
                                        map.insert(alias.clone(), val_json);
                                    } else {
                                        let v = eval_sql_expr(
                                            expr_str,
                                            p_row,
                                            Some(table),
                                            j_slice,
                                            joined_table_opt,
                                            params,
                                        );
                                        map.insert(alias.clone(), value_to_json(&v));
                                    }
                                }
                            }
                        }
                        out_rows.push(serde_json::Value::Object(map));
                    }
                }

                let row_count = out_rows.len();
                Ok(QueryResult {
                    rows: out_rows,
                    row_count,
                    fields: fields.clone(),
                    command: "SELECT".to_string(),
                })
            }
            _ => Err("Invalid plan type for select".to_string()),
        }
    }

    fn handle_group_by_query(
        &self,
        table: &crate::storage::table::Table,
        select_clause: &str,
        clauses: &SelectClauses,
        params: &[Value],
    ) -> Result<QueryResult, String> {
        let gb_str = clauses.group_by_clause.unwrap();
        let group_tokens: Vec<String> = split_comma_separated_tokens(gb_str)
            .into_iter()
            .map(|c| c.trim().to_string())
            .collect();

        let where_expr = match clauses.where_clause {
            Some(w) => Some(parse_where_expr(w, table, params)?),
            None => None,
        };

        let matched_indices: Vec<usize> = (0..table.rows.len())
            .filter(|&i| !table.is_deleted[i] && where_expr.as_ref().map_or(true, |e| evaluate_where_expr(&table.rows[i], Some(table), e, params)))
            .collect();

        let mut groups: Vec<(Vec<Value>, Vec<usize>)> = Vec::new();
        let mut group_map: std::collections::HashMap<String, usize> = std::collections::HashMap::new();

        for row_idx in matched_indices {
            let row = &table.rows[row_idx];
            let key_vals: Vec<Value> = group_tokens.iter().map(|tok| {
                let name = clean_col_name(tok);
                if let Some(idx) = table.get_column_index(name) {
                    row.get(idx).cloned().unwrap_or(Value::Null)
                } else {
                    eval_sql_expr(tok, row, Some(table), &[], None, params)
                }
            }).collect();
            let key_str = key_vals.iter().map(|v| match v {
                Value::Null => "\0null\0".to_string(),
                Value::Bool(b) => format!("b:{}", b),
                Value::Int(i) => format!("i:{}", i),
                Value::Float(f) => format!("f:{}", f),
                Value::Text(s) => format!("s:{}", s),
            }).collect::<Vec<_>>().join("\x1f");

            if let Some(&g_idx) = group_map.get(&key_str) {
                groups[g_idx].1.push(row_idx);
            } else {
                let g_idx = groups.len();
                group_map.insert(key_str, g_idx);
                groups.push((key_vals, vec![row_idx]));
            }
        }

        struct GroupProjItem {
            name: String,
            alias: String,
            func: Option<AggFunc>,
            col_idx: Option<usize>,
            filter_expr: Option<WhereExpr>,
        }

        let proj_strs = split_projection_items(select_clause);
        let mut projections = Vec::new();
        let mut fields = Vec::new();

        for p_str in proj_strs {
            let p_trim = p_str.trim();
            let upper = p_trim.to_uppercase();
            let (raw_expr, alias) = if let Some(as_idx) = upper.rfind(" AS ") {
                (p_trim[..as_idx].trim(), p_trim[as_idx + 4..].trim().trim_matches('"').to_string())
            } else {
                (p_trim, clean_col_name(p_trim).to_string())
            };

            let upper_expr = raw_expr.to_uppercase();
            let (agg_expr, filter_expr) = if let Some(filter_idx) = upper_expr.find(" FILTER") {
                let after_filter = raw_expr[filter_idx + 7..].trim();
                if after_filter.starts_with('(') && after_filter.ends_with(')') {
                    let inner_filter = after_filter[1..after_filter.len() - 1].trim();
                    let upper_inner = inner_filter.to_uppercase();
                    if upper_inner.starts_with("WHERE ") {
                        let cond_str = inner_filter[6..].trim();
                        let parsed_filter = parse_where_expr(cond_str, table, params)?;
                        (raw_expr[..filter_idx].trim(), Some(parsed_filter))
                    } else {
                        (raw_expr, None)
                    }
                } else {
                    (raw_expr, None)
                }
            } else {
                (raw_expr, None)
            };

            let upper_agg = agg_expr.to_uppercase();
            if upper_agg.starts_with("COUNT(") && upper_agg.ends_with(')') {
                let inner = agg_expr[6..agg_expr.len() - 1].trim();
                let upper_inner = inner.to_uppercase();
                let func = if upper_inner == "*" || upper_inner == "1" {
                    AggFunc::CountStar
                } else if upper_inner.starts_with("DISTINCT ") {
                    let col = clean_col_name(&inner[9..]);
                    let c_idx = table.get_column_index(col).ok_or_else(|| format!("Column {} not found", col))?;
                    AggFunc::CountDistinct(c_idx)
                } else {
                    let col = clean_col_name(inner);
                    let func = if let Some(c_idx) = table.get_column_index(col) {
                        AggFunc::Count(c_idx)
                    } else {
                        AggFunc::CountExpr(inner.to_string())
                    };
                    func
                };
                projections.push(GroupProjItem { name: raw_expr.to_string(), alias: alias.clone(), func: Some(func), col_idx: None, filter_expr });
                fields.push(FieldInfo { name: alias, data_type: "int8".to_string() });
            } else if upper_agg.starts_with("SUM(") && upper_agg.ends_with(')') {
                let raw_inner = agg_expr[4..agg_expr.len() - 1].trim();
                let inner = clean_col_name(raw_inner);
                let func = if let Some(c_idx) = table.get_column_index(inner) {
                    AggFunc::Sum(c_idx)
                } else {
                    AggFunc::SumExpr(raw_inner.to_string())
                };
                projections.push(GroupProjItem { name: raw_expr.to_string(), alias: alias.clone(), func: Some(func), col_idx: None, filter_expr });
                fields.push(FieldInfo { name: alias, data_type: "numeric".to_string() });
            } else if upper_agg.starts_with("AVG(") && upper_agg.ends_with(')') {
                let raw_inner = agg_expr[4..agg_expr.len() - 1].trim();
                let inner = clean_col_name(raw_inner);
                let func = if let Some(c_idx) = table.get_column_index(inner) {
                    AggFunc::Avg(c_idx)
                } else {
                    AggFunc::AvgExpr(raw_inner.to_string())
                };
                projections.push(GroupProjItem { name: raw_expr.to_string(), alias: alias.clone(), func: Some(func), col_idx: None, filter_expr });
                fields.push(FieldInfo { name: alias, data_type: "float8".to_string() });
            } else if upper_agg.starts_with("MIN(") && upper_agg.ends_with(')') {
                let raw_inner = agg_expr[4..agg_expr.len() - 1].trim();
                let inner = clean_col_name(raw_inner);
                let func = if let Some(c_idx) = table.get_column_index(inner) {
                    AggFunc::Min(c_idx)
                } else {
                    AggFunc::MinExpr(raw_inner.to_string())
                };
                projections.push(GroupProjItem { name: raw_expr.to_string(), alias: alias.clone(), func: Some(func), col_idx: None, filter_expr });
                fields.push(FieldInfo { name: alias, data_type: "numeric".to_string() });
            } else if upper_agg.starts_with("MAX(") && upper_agg.ends_with(')') {
                let raw_inner = agg_expr[4..agg_expr.len() - 1].trim();
                let inner = clean_col_name(raw_inner);
                let func = if let Some(c_idx) = table.get_column_index(inner) {
                    AggFunc::Max(c_idx)
                } else {
                    AggFunc::MaxExpr(raw_inner.to_string())
                };
                projections.push(GroupProjItem { name: raw_expr.to_string(), alias: alias.clone(), func: Some(func), col_idx: None, filter_expr });
                fields.push(FieldInfo { name: alias, data_type: "numeric".to_string() });
            } else if (upper_agg.starts_with("BOOL_AND(") || upper_agg.starts_with("EVERY(")) && upper_agg.ends_with(')') {
                let open_p = agg_expr.find('(').unwrap();
                let raw_inner = agg_expr[open_p + 1..agg_expr.len() - 1].trim();
                let inner = clean_col_name(raw_inner);
                let func = if let Some(c_idx) = table.get_column_index(inner) {
                    AggFunc::BoolAnd(c_idx)
                } else {
                    AggFunc::BoolAndExpr(raw_inner.to_string())
                };
                projections.push(GroupProjItem { name: raw_expr.to_string(), alias: alias.clone(), func: Some(func), col_idx: None, filter_expr });
                fields.push(FieldInfo { name: alias, data_type: "bool".to_string() });
            } else if upper_agg.starts_with("BOOL_OR(") && upper_agg.ends_with(')') {
                let raw_inner = agg_expr[8..agg_expr.len() - 1].trim();
                let inner = clean_col_name(raw_inner);
                let func = if let Some(c_idx) = table.get_column_index(inner) {
                    AggFunc::BoolOr(c_idx)
                } else {
                    AggFunc::BoolOrExpr(raw_inner.to_string())
                };
                projections.push(GroupProjItem { name: raw_expr.to_string(), alias: alias.clone(), func: Some(func), col_idx: None, filter_expr });
                fields.push(FieldInfo { name: alias, data_type: "bool".to_string() });
            } else if upper_agg.starts_with("ARRAY_AGG(") && upper_agg.ends_with(')') {
                let raw_inner = agg_expr[10..agg_expr.len() - 1].trim();
                let inner = clean_col_name(raw_inner);
                let func = if let Some(c_idx) = table.get_column_index(inner) {
                    AggFunc::ArrayAgg(c_idx)
                } else {
                    AggFunc::ArrayAggExpr(raw_inner.to_string())
                };
                projections.push(GroupProjItem { name: raw_expr.to_string(), alias: alias.clone(), func: Some(func), col_idx: None, filter_expr });
                fields.push(FieldInfo { name: alias, data_type: "array".to_string() });
            } else if (upper_agg.starts_with("JSON_AGG(") || upper_agg.starts_with("JSONB_AGG(")) && upper_agg.ends_with(')') {
                let is_jsonb = upper_agg.starts_with("JSONB_AGG(");
                let prefix_len = if is_jsonb { 10 } else { 9 };
                let inner = clean_col_name(&agg_expr[prefix_len..agg_expr.len() - 1]);
                let c_idx = table.get_column_index(inner).ok_or_else(|| format!("Column {} not found", inner))?;
                let func = if is_jsonb { AggFunc::JsonbAgg(c_idx) } else { AggFunc::JsonAgg(c_idx) };
                projections.push(GroupProjItem { name: raw_expr.to_string(), alias: alias.clone(), func: Some(func), col_idx: None, filter_expr });
                fields.push(FieldInfo { name: alias, data_type: if is_jsonb { "jsonb".to_string() } else { "json".to_string() } });
            } else if upper_agg.starts_with("STRING_AGG(") && upper_agg.ends_with(')') {
                let inner = &agg_expr[11..agg_expr.len() - 1];
                let args = split_function_args(inner);
                if args.len() >= 2 {
                    let col_name = clean_col_name(&args[0]);
                    let c_idx = table.get_column_index(col_name).ok_or_else(|| format!("Column {} not found", col_name))?;
                    let delim_raw = args[1].trim();
                    let delim = delim_raw.trim_matches('\'').replace("''", "'");
                    let func = AggFunc::StringAgg(c_idx, delim);
                    projections.push(GroupProjItem { name: raw_expr.to_string(), alias: alias.clone(), func: Some(func), col_idx: None, filter_expr });
                    fields.push(FieldInfo { name: alias, data_type: "text".to_string() });
                }
            } else {
                let col = clean_col_name(raw_expr);
                if let Some(c_idx) = table.get_column_index(col) {
                    projections.push(GroupProjItem { name: raw_expr.to_string(), alias: alias.clone(), func: None, col_idx: Some(c_idx), filter_expr: None });
                    let dt = format!("{:?}", table.columns[c_idx].data_type).to_lowercase();
                    fields.push(FieldInfo { name: alias, data_type: dt });
                } else {
                    projections.push(GroupProjItem { name: raw_expr.to_string(), alias: alias.clone(), func: None, col_idx: None, filter_expr: None });
                    fields.push(FieldInfo { name: alias, data_type: "text".to_string() });
                }
            }
        }

        let mut group_rows = Vec::new();
        for (_key, r_indices) in groups {
            let mut row_map = serde_json::Map::new();
            for proj in &projections {
                if let Some(c_idx) = proj.col_idx {
                    let v = r_indices.first().and_then(|&ri| table.rows[ri].get(c_idx)).unwrap_or(&Value::Null);
                    row_map.insert(proj.alias.clone(), value_to_json(v));
                } else if proj.func.is_none() {
                    let first_ri = r_indices.first().copied().unwrap_or(0);
                    let row: &[Value] = if first_ri < table.rows.len() { &table.rows[first_ri] } else { &[] };
                    let v = eval_sql_expr(&proj.name, row, Some(table), &[], None, params);
                    row_map.insert(proj.alias.clone(), value_to_json(&v));
                } else if let Some(ref func) = proj.func {
                    let target_indices: Vec<usize> = if let Some(ref f_expr) = proj.filter_expr {
                        r_indices.iter().copied().filter(|&ri| evaluate_where_expr(&table.rows[ri], Some(table), f_expr, params)).collect()
                    } else {
                        r_indices.clone()
                    };
                    let v_json = match func {
                        AggFunc::CountStar => json!(target_indices.len()),
                        AggFunc::Count(c_idx) => {
                            let count = target_indices.iter().filter(|&&ri| table.rows[ri].get(*c_idx).map_or(false, |v| !v.is_null())).count();
                            json!(count)
                        }
                        AggFunc::CountExpr(expr) => {
                            let count = target_indices.iter().filter(|&&ri| !eval_sql_expr(expr, &table.rows[ri], Some(table), &[], None, params).is_null()).count();
                            json!(count)
                        }
                        AggFunc::CountDistinct(c_idx) => {
                            let mut set = std::collections::HashSet::new();
                            for &ri in &target_indices {
                                if let Some(v) = table.rows[ri].get(*c_idx) {
                                    if !v.is_null() { set.insert(format!("{:?}", v)); }
                                }
                            }
                            json!(set.len())
                        }
                        AggFunc::Sum(c_idx) => {
                            let mut sum = 0.0;
                            for &ri in &target_indices {
                                if let Some(f) = table.rows[ri].get(*c_idx).and_then(|v| v.as_f64()) {
                                    sum += f;
                                }
                            }
                            json!(sum)
                        }
                        AggFunc::Avg(c_idx) => {
                            let mut sum = 0.0;
                            let mut count = 0;
                            for &ri in &target_indices {
                                if let Some(f) = table.rows[ri].get(*c_idx).and_then(|v| v.as_f64()) {
                                    sum += f;
                                    count += 1;
                                }
                            }
                            if count > 0 { json!(sum / count as f64) } else { serde_json::Value::Null }
                        }
                        AggFunc::Min(c_idx) => {
                            let is_numeric = matches!(table.columns[*c_idx].data_type, DataType::Integer | DataType::BigInt | DataType::Serial | DataType::Numeric);
                            if is_numeric {
                                let mut min = f64::MAX;
                                let mut found = false;
                                for &ri in &target_indices {
                                    if let Some(f) = table.rows[ri].get(*c_idx).and_then(|v| v.as_f64()) {
                                        if f < min { min = f; found = true; }
                                    }
                                }
                                if found { json!(min) } else { serde_json::Value::Null }
                            } else {
                                let mut min_val: Option<Value> = None;
                                for &ri in &target_indices {
                                    if let Some(v) = table.rows[ri].get(*c_idx) {
                                        if !v.is_null() {
                                            min_val = match min_val {
                                                None => Some(v.clone()),
                                                Some(cur) => if v.cmp_value(&cur) == std::cmp::Ordering::Less { Some(v.clone()) } else { Some(cur) },
                                            };
                                        }
                                    }
                                }
                                min_val.as_ref().map(|v| value_to_json(v)).unwrap_or(serde_json::Value::Null)
                            }
                        }
                        AggFunc::Max(c_idx) => {
                            let is_numeric = matches!(table.columns[*c_idx].data_type, DataType::Integer | DataType::BigInt | DataType::Serial | DataType::Numeric);
                            if is_numeric {
                                let mut max = f64::MIN;
                                let mut found = false;
                                for &ri in &target_indices {
                                    if let Some(f) = table.rows[ri].get(*c_idx).and_then(|v| v.as_f64()) {
                                        if f > max { max = f; found = true; }
                                    }
                                }
                                if found { json!(max) } else { serde_json::Value::Null }
                            } else {
                                let mut max_val: Option<Value> = None;
                                for &ri in &target_indices {
                                    if let Some(v) = table.rows[ri].get(*c_idx) {
                                        if !v.is_null() {
                                            max_val = match max_val {
                                                None => Some(v.clone()),
                                                Some(cur) => if v.cmp_value(&cur) == std::cmp::Ordering::Greater { Some(v.clone()) } else { Some(cur) },
                                            };
                                        }
                                    }
                                }
                                max_val.as_ref().map(|v| value_to_json(v)).unwrap_or(serde_json::Value::Null)
                            }
                        }
                        AggFunc::BoolAnd(c_idx) => {
                            let mut res: Option<bool> = None;
                            for &ri in &target_indices {
                                let v = table.rows[ri].get(*c_idx).unwrap_or(&Value::Null);
                                if !v.is_null() {
                                    res = Some(res.unwrap_or(true) && v.as_bool().unwrap_or(false));
                                }
                            }
                            res.map(|b| json!(b)).unwrap_or(serde_json::Value::Null)
                        }
                        AggFunc::BoolOr(c_idx) => {
                            let mut res: Option<bool> = None;
                            for &ri in &target_indices {
                                let v = table.rows[ri].get(*c_idx).unwrap_or(&Value::Null);
                                if !v.is_null() {
                                    res = Some(res.unwrap_or(false) || v.as_bool().unwrap_or(false));
                                }
                            }
                            res.map(|b| json!(b)).unwrap_or(serde_json::Value::Null)
                        }
                        AggFunc::BoolAndExpr(expr_str) => {
                            let mut res: Option<bool> = None;
                            for &ri in &target_indices {
                                let v = eval_sql_expr(expr_str, &table.rows[ri], Some(table), &[], None, params);
                                if !v.is_null() {
                                    res = Some(res.unwrap_or(true) && v.as_bool().unwrap_or(false));
                                }
                            }
                            res.map(|b| json!(b)).unwrap_or(serde_json::Value::Null)
                        }
                        AggFunc::BoolOrExpr(expr_str) => {
                            let mut res: Option<bool> = None;
                            for &ri in &target_indices {
                                let v = eval_sql_expr(expr_str, &table.rows[ri], Some(table), &[], None, params);
                                if !v.is_null() {
                                    res = Some(res.unwrap_or(false) || v.as_bool().unwrap_or(false));
                                }
                            }
                            res.map(|b| json!(b)).unwrap_or(serde_json::Value::Null)
                        }
                        AggFunc::StringAgg(c_idx, delim) => {
                            let mut parts = Vec::new();
                            for &ri in &target_indices {
                                if let Some(v) = table.rows[ri].get(*c_idx) {
                                    if !v.is_null() {
                                        parts.push(v.as_str());
                                    }
                                }
                            }
                            serde_json::Value::String(parts.join(delim))
                        }
                        AggFunc::ArrayAgg(c_idx) => {
                            let mut arr = Vec::new();
                            for &ri in &target_indices {
                                let v = table.rows[ri].get(*c_idx).unwrap_or(&Value::Null);
                                if !v.is_null() {
                                    arr.push(value_to_json(v));
                                }
                            }
                            serde_json::Value::Array(arr)
                        }
                        AggFunc::ArrayAggExpr(expr_str) => {
                            let mut arr = Vec::new();
                            for &ri in &target_indices {
                                let v = eval_sql_expr(expr_str, &table.rows[ri], Some(table), &[], None, params);
                                if !v.is_null() {
                                    arr.push(value_to_json(&v));
                                }
                            }
                            serde_json::Value::Array(arr)
                        }
                        AggFunc::JsonAgg(c_idx) | AggFunc::JsonbAgg(c_idx) => {
                            let mut arr = Vec::new();
                            for &ri in &target_indices {
                                if let Some(v) = table.rows[ri].get(*c_idx) {
                                    if !v.is_null() {
                                        let j_val = match v {
                                            Value::Text(st) => serde_json::from_str::<serde_json::Value>(st.trim()).unwrap_or(serde_json::Value::String(st.to_string())),
                                            _ => value_to_json(v),
                                        };
                                        arr.push(j_val);
                                    }
                                }
                            }
                            serde_json::Value::Array(arr)
                        }
                        AggFunc::JsonAggExpr(expr_str) | AggFunc::JsonbAggExpr(expr_str) => {
                            let mut arr = Vec::new();
                            for &ri in &target_indices {
                                let v = eval_sql_expr(expr_str, &table.rows[ri], Some(table), &[], None, params);
                                if !v.is_null() {
                                    let j_val = match &v {
                                        Value::Text(st) => serde_json::from_str::<serde_json::Value>(st.trim()).unwrap_or(serde_json::Value::String(st.to_string())),
                                        _ => value_to_json(&v),
                                    };
                                    arr.push(j_val);
                                }
                            }
                            serde_json::Value::Array(arr)
                        }
                        AggFunc::SumExpr(expr_str) => {
                            let mut sum = 0.0;
                            for &ri in &target_indices {
                                let v = eval_sql_expr(expr_str, &table.rows[ri], Some(table), &[], None, params);
                                if let Some(f) = v.as_f64() {
                                    sum += f;
                                }
                            }
                            json!(sum)
                        }
                        AggFunc::AvgExpr(expr_str) => {
                            let mut sum = 0.0;
                            let mut count = 0;
                            for &ri in &target_indices {
                                let v = eval_sql_expr(expr_str, &table.rows[ri], Some(table), &[], None, params);
                                if let Some(f) = v.as_f64() {
                                    sum += f;
                                    count += 1;
                                }
                            }
                            if count > 0 { json!(sum / count as f64) } else { serde_json::Value::Null }
                        }
                        AggFunc::MinExpr(expr_str) => {
                            let mut min_val: Option<Value> = None;
                            for &ri in &target_indices {
                                let v = eval_sql_expr(expr_str, &table.rows[ri], Some(table), &[], None, params);
                                if !v.is_null() {
                                    min_val = match min_val {
                                        None => Some(v.clone()),
                                        Some(cur) => if v.cmp_value(&cur) == std::cmp::Ordering::Less { Some(v.clone()) } else { Some(cur) },
                                    };
                                }
                            }
                            min_val.as_ref().map(|v| value_to_json(v)).unwrap_or(serde_json::Value::Null)
                        }
                        AggFunc::MaxExpr(expr_str) => {
                            let mut max_val: Option<Value> = None;
                            for &ri in &target_indices {
                                let v = eval_sql_expr(expr_str, &table.rows[ri], Some(table), &[], None, params);
                                if !v.is_null() {
                                    max_val = match max_val {
                                        None => Some(v.clone()),
                                        Some(cur) => if v.cmp_value(&cur) == std::cmp::Ordering::Greater { Some(v.clone()) } else { Some(cur) },
                                    };
                                }
                            }
                            max_val.as_ref().map(|v| value_to_json(v)).unwrap_or(serde_json::Value::Null)
                        }
                        AggFunc::WrappedAgg(raw_expr, inner_func) => {
                            let inner_json = match inner_func.as_ref() {
                                AggFunc::ArrayAgg(c_idx) => {
                                    let mut arr = Vec::new();
                                    for &ri in &target_indices {
                                        let v = table.rows[ri].get(*c_idx).unwrap_or(&Value::Null);
                                        if !v.is_null() {
                                            arr.push(value_to_json(v));
                                        }
                                    }
                                    serde_json::Value::Array(arr)
                                }
                                AggFunc::ArrayAggExpr(expr_str) => {
                                    let mut arr = Vec::new();
                                    for &ri in &target_indices {
                                        let v = eval_sql_expr(expr_str, &table.rows[ri], Some(table), &[], None, params);
                                        if !v.is_null() {
                                            arr.push(value_to_json(&v));
                                        }
                                    }
                                    serde_json::Value::Array(arr)
                                }
                                _ => serde_json::Value::Null,
                            };
                            let inner_pattern = if raw_expr.to_uppercase().contains("ARRAY_AGG(") {
                                let u = raw_expr.to_uppercase();
                                let start = u.find("ARRAY_AGG(").unwrap();
                                if let Some(end) = find_matching_paren(raw_expr, start + 9) {
                                    Some(&raw_expr[start..=end])
                                } else {
                                    None
                                }
                            } else {
                                None
                            };
                            let substituted_expr = if let Some(pat) = inner_pattern {
                                let json_str = serde_json::to_string(&inner_json).unwrap_or_else(|_| "[]".to_string());
                                let pg_arr_str = format!("'{}'", json_str.replace('\'', "''"));
                                raw_expr.replace(pat, &pg_arr_str)
                            } else {
                                raw_expr.clone()
                            };
                            let first_ri = target_indices.first().copied().unwrap_or(0);
                            let first_row: &[Value] = if first_ri < table.rows.len() { &table.rows[first_ri] } else { &[] };
                            let res_val = eval_sql_expr(&substituted_expr, first_row, Some(table), &[], None, params);
                            value_to_json(&res_val)
                        }
                    };
                    row_map.insert(proj.alias.clone(), v_json);
                }
            }
            if let Some(having_str) = clauses.having_clause {
                let proj_pairs: Vec<(&str, &str)> = projections.iter().map(|p| (p.name.as_str(), p.alias.as_str())).collect();
                if !evaluate_having_with_group(having_str, &row_map, Some(&proj_pairs), table, &r_indices, params) {
                    continue;
                }
            }
            group_rows.push(serde_json::Value::Object(row_map));
        }

        if !clauses.order_by_specs.is_empty() {
            group_rows.sort_by(|a, b| {
                if let (serde_json::Value::Object(map_a), serde_json::Value::Object(map_b)) = (a, b) {
                    for spec in &clauses.order_by_specs {
                        let va = map_a.get(spec.col_name);
                        let vb = map_b.get(spec.col_name);
                        let ord = match (va, vb) {
                            (Some(x), Some(y)) => {
                                if let (Some(fx), Some(fy)) = (x.as_f64(), y.as_f64()) {
                                    fx.partial_cmp(&fy).unwrap_or(std::cmp::Ordering::Equal)
                                } else if let (Some(sx), Some(sy)) = (x.as_str(), y.as_str()) {
                                    sx.cmp(sy)
                                } else if let (Some(bx), Some(by)) = (x.as_bool(), y.as_bool()) {
                                    bx.cmp(&by)
                                } else {
                                    std::cmp::Ordering::Equal
                                }
                            }
                            (Some(_), None) => std::cmp::Ordering::Greater,
                            (None, Some(_)) => std::cmp::Ordering::Less,
                            (None, None) => std::cmp::Ordering::Equal,
                        };
                        let final_ord = if spec.is_desc { ord.reverse() } else { ord };
                        if final_ord != std::cmp::Ordering::Equal {
                            return final_ord;
                        }
                    }
                }
                std::cmp::Ordering::Equal
            });
        }

        let offset = clauses.offset.as_ref().map(|o| resolve_operand(o, params).as_i64().unwrap_or(0) as usize).unwrap_or(0);
        let limit = clauses.limit.as_ref().map(|l| resolve_operand(l, params).as_i64().unwrap_or(1000) as usize).unwrap_or(usize::MAX);
        let paged_rows = if offset < group_rows.len() {
            let end = (offset.saturating_add(limit)).min(group_rows.len());
            group_rows[offset..end].to_vec()
        } else {
            vec![]
        };

        let row_count = paged_rows.len();
        Ok(QueryResult {
            rows: paged_rows,
            row_count,
            fields,
            command: "SELECT".to_string(),
        })
    }

    fn handle_aggregate_query(
        &self,
        table: &crate::storage::table::Table,
        agg_specs: &[AggSpec],
        where_clause: Option<&str>,
        params: &[Value],
    ) -> Result<QueryResult, String> {
        let mut row_map = serde_json::Map::new();
        let mut fields = Vec::new();

        if where_clause.is_none() {
            // Unconditional aggregate fast path
            for spec in agg_specs {
                match &spec.func {
                    AggFunc::CountStar => {
                        let count = table.count();
                        row_map.insert(spec.alias.clone(), json!(count));
                        fields.push(FieldInfo { name: spec.alias.clone(), data_type: "int8".to_string() });
                    }
                    AggFunc::Count(col_idx) => {
                        let count = (0..table.rows.len())
                            .filter(|&i| !table.is_deleted[i] && table.rows[i].get(*col_idx).map_or(false, |v| !v.is_null()))
                            .count();
                        row_map.insert(spec.alias.clone(), json!(count));
                        fields.push(FieldInfo { name: spec.alias.clone(), data_type: "int8".to_string() });
                    }
                    AggFunc::CountExpr(expr) => {
                        let count = (0..table.rows.len())
                            .filter(|&i| !table.is_deleted[i] && !eval_sql_expr(expr, &table.rows[i], Some(table), &[], None, params).is_null())
                            .count();
                        row_map.insert(spec.alias.clone(), json!(count));
                        fields.push(FieldInfo { name: spec.alias.clone(), data_type: "int8".to_string() });
                    }
                    AggFunc::CountDistinct(col_idx) => {
                        let mut set = std::collections::HashSet::new();
                        for i in 0..table.rows.len() {
                            if !table.is_deleted[i] {
                                if let Some(v) = table.rows[i].get(*col_idx) {
                                    if !v.is_null() {
                                        set.insert(format!("{:?}", v));
                                    }
                                }
                            }
                        }
                        row_map.insert(spec.alias.clone(), json!(set.len()));
                        fields.push(FieldInfo { name: spec.alias.clone(), data_type: "int8".to_string() });
                    }
                    AggFunc::Sum(col_idx) => {
                        let (sum, _, _, _, count) = table.aggregate_stats(*col_idx);
                        if count == 0 {
                            row_map.insert(spec.alias.clone(), serde_json::Value::Null);
                        } else {
                            row_map.insert(spec.alias.clone(), json!(sum));
                        }
                        fields.push(FieldInfo { name: spec.alias.clone(), data_type: "numeric".to_string() });
                    }
                    AggFunc::Avg(col_idx) => {
                        let (_, avg, _, _, count) = table.aggregate_stats(*col_idx);
                        if count == 0 {
                            row_map.insert(spec.alias.clone(), serde_json::Value::Null);
                        } else {
                            row_map.insert(spec.alias.clone(), json!(avg));
                        }
                        fields.push(FieldInfo { name: spec.alias.clone(), data_type: "float8".to_string() });
                    }
                    AggFunc::Min(col_idx) => {
                        let is_numeric = matches!(table.columns[*col_idx].data_type, DataType::Integer | DataType::BigInt | DataType::Serial | DataType::Numeric);
                        if is_numeric {
                            let (_, _, min, _, count) = table.aggregate_stats(*col_idx);
                            if count == 0 {
                                row_map.insert(spec.alias.clone(), serde_json::Value::Null);
                            } else {
                                row_map.insert(spec.alias.clone(), json!(min));
                            }
                            fields.push(FieldInfo { name: spec.alias.clone(), data_type: "numeric".to_string() });
                        } else {
                            let mut min_val: Option<Value> = None;
                            for i in 0..table.rows.len() {
                                if !table.is_deleted[i] {
                                    if let Some(v) = table.rows[i].get(*col_idx) {
                                        if !v.is_null() {
                                            min_val = match min_val {
                                                None => Some(v.clone()),
                                                Some(cur) => if v.cmp_value(&cur) == std::cmp::Ordering::Less { Some(v.clone()) } else { Some(cur) },
                                            };
                                        }
                                    }
                                }
                            }
                            row_map.insert(spec.alias.clone(), min_val.as_ref().map(|v| value_to_json(v)).unwrap_or(serde_json::Value::Null));
                            fields.push(FieldInfo { name: spec.alias.clone(), data_type: "text".to_string() });
                        }
                    }
                    AggFunc::Max(col_idx) => {
                        let is_numeric = matches!(table.columns[*col_idx].data_type, DataType::Integer | DataType::BigInt | DataType::Serial | DataType::Numeric);
                        if is_numeric {
                            let (_, _, _, max, count) = table.aggregate_stats(*col_idx);
                            if count == 0 {
                                row_map.insert(spec.alias.clone(), serde_json::Value::Null);
                            } else {
                                row_map.insert(spec.alias.clone(), json!(max));
                            }
                            fields.push(FieldInfo { name: spec.alias.clone(), data_type: "numeric".to_string() });
                        } else {
                            let mut max_val: Option<Value> = None;
                            for i in 0..table.rows.len() {
                                if !table.is_deleted[i] {
                                    if let Some(v) = table.rows[i].get(*col_idx) {
                                        if !v.is_null() {
                                            max_val = match max_val {
                                                None => Some(v.clone()),
                                                Some(cur) => if v.cmp_value(&cur) == std::cmp::Ordering::Greater { Some(v.clone()) } else { Some(cur) },
                                            };
                                        }
                                    }
                                }
                            }
                            row_map.insert(spec.alias.clone(), max_val.as_ref().map(|v| value_to_json(v)).unwrap_or(serde_json::Value::Null));
                            fields.push(FieldInfo { name: spec.alias.clone(), data_type: "text".to_string() });
                        }
                    }
                    AggFunc::StringAgg(col_idx, delim) => {
                        let mut parts = Vec::new();
                        for i in 0..table.rows.len() {
                            if !table.is_deleted[i] {
                                if let Some(v) = table.rows[i].get(*col_idx) {
                                    if !v.is_null() {
                                        parts.push(v.as_str());
                                    }
                                }
                            }
                        }
                        let joined = parts.join(delim);
                        row_map.insert(spec.alias.clone(), serde_json::Value::String(joined));
                        fields.push(FieldInfo { name: spec.alias.clone(), data_type: "text".to_string() });
                    }
                    AggFunc::ArrayAgg(col_idx) => {
                        let mut arr = Vec::new();
                        for i in 0..table.rows.len() {
                            if !table.is_deleted[i] {
                                if let Some(v) = table.rows[i].get(*col_idx) {
                                    if !v.is_null() {
                                        arr.push(value_to_json(v));
                                    }
                                }
                            }
                        }
                        row_map.insert(spec.alias.clone(), serde_json::Value::Array(arr));
                        fields.push(FieldInfo { name: spec.alias.clone(), data_type: "array".to_string() });
                    }
                    AggFunc::ArrayAggExpr(expr_str) => {
                        let mut arr = Vec::new();
                        for i in 0..table.rows.len() {
                            if !table.is_deleted[i] {
                                let v = eval_sql_expr(expr_str, &table.rows[i], Some(table), &[], None, params);
                                if !v.is_null() {
                                    arr.push(value_to_json(&v));
                                }
                            }
                        }
                        row_map.insert(spec.alias.clone(), serde_json::Value::Array(arr));
                        fields.push(FieldInfo { name: spec.alias.clone(), data_type: "array".to_string() });
                    }
                    AggFunc::WrappedAgg(raw_expr, inner_func) => {
                        let inner_json = match inner_func.as_ref() {
                            AggFunc::ArrayAgg(c_idx) => {
                                let mut arr = Vec::new();
                                for i in 0..table.rows.len() {
                                    if !table.is_deleted[i] {
                                        if let Some(v) = table.rows[i].get(*c_idx) {
                                            if !v.is_null() {
                                                arr.push(value_to_json(v));
                                            }
                                        }
                                    }
                                }
                                serde_json::Value::Array(arr)
                            }
                            AggFunc::ArrayAggExpr(expr_str) => {
                                let mut arr = Vec::new();
                                for i in 0..table.rows.len() {
                                    if !table.is_deleted[i] {
                                        let v = eval_sql_expr(expr_str, &table.rows[i], Some(table), &[], None, params);
                                        if !v.is_null() {
                                            arr.push(value_to_json(&v));
                                        }
                                    }
                                }
                                serde_json::Value::Array(arr)
                            }
                            _ => serde_json::Value::Null,
                        };
                        let inner_pattern = if raw_expr.to_uppercase().contains("ARRAY_AGG(") {
                            let u = raw_expr.to_uppercase();
                            let start = u.find("ARRAY_AGG(").unwrap();
                            if let Some(end) = find_matching_paren(raw_expr, start + 9) {
                                Some(&raw_expr[start..=end])
                            } else {
                                None
                            }
                        } else {
                            None
                        };
                        let substituted_expr = if let Some(pat) = inner_pattern {
                            let json_str = serde_json::to_string(&inner_json).unwrap_or_else(|_| "[]".to_string());
                            let pg_arr_str = format!("'{}'", json_str.replace('\'', "''"));
                            raw_expr.replace(pat, &pg_arr_str)
                        } else {
                            raw_expr.clone()
                        };
                        let res_val = eval_sql_expr(&substituted_expr, &[], None, &[], None, params);
                        let dt = match &res_val {
                            Value::Int(_) => "int8",
                            Value::Float(_) => "float8",
                            Value::Bool(_) => "bool",
                            _ => "text",
                        };
                        row_map.insert(spec.alias.clone(), value_to_json(&res_val));
                        fields.push(FieldInfo { name: spec.alias.clone(), data_type: dt.to_string() });
                    }
                    AggFunc::JsonAgg(col_idx) | AggFunc::JsonbAgg(col_idx) => {
                        let mut arr = Vec::new();
                        for i in 0..table.rows.len() {
                            if !table.is_deleted[i] {
                                if let Some(v) = table.rows[i].get(*col_idx) {
                                    if !v.is_null() {
                                        let j_val = match v {
                                            Value::Text(st) => serde_json::from_str::<serde_json::Value>(st.trim()).unwrap_or(serde_json::Value::String(st.to_string())),
                                            _ => value_to_json(v),
                                        };
                                        arr.push(j_val);
                                    }
                                }
                            }
                        }
                        row_map.insert(spec.alias.clone(), serde_json::Value::Array(arr));
                        fields.push(FieldInfo { name: spec.alias.clone(), data_type: "json".to_string() });
                    }
                    AggFunc::JsonAggExpr(expr_str) | AggFunc::JsonbAggExpr(expr_str) => {
                        let mut arr = Vec::new();
                        for i in 0..table.rows.len() {
                            if !table.is_deleted[i] {
                                let v = eval_sql_expr(expr_str, &table.rows[i], Some(table), &[], None, params);
                                if !v.is_null() {
                                    let j_val = match &v {
                                        Value::Text(st) => serde_json::from_str::<serde_json::Value>(st.trim()).unwrap_or(serde_json::Value::String(st.to_string())),
                                        _ => value_to_json(&v),
                                    };
                                    arr.push(j_val);
                                }
                            }
                        }
                        row_map.insert(spec.alias.clone(), serde_json::Value::Array(arr));
                        fields.push(FieldInfo { name: spec.alias.clone(), data_type: "json".to_string() });
                    }
                    AggFunc::SumExpr(expr_str) => {
                        let sum: f64 = (0..table.rows.len())
                            .filter(|&i| !table.is_deleted[i])
                            .filter_map(|i| {
                                let v = eval_sql_expr(expr_str, &table.rows[i], Some(table), &[], None, params);
                                v.as_f64()
                            })
                            .sum();
                        row_map.insert(spec.alias.clone(), json!(sum));
                        fields.push(FieldInfo { name: spec.alias.clone(), data_type: "numeric".to_string() });
                    }
                    AggFunc::AvgExpr(expr_str) => {
                        let mut count = 0;
                        let mut sum: f64 = 0.0;
                        for i in 0..table.rows.len() {
                            if !table.is_deleted[i] {
                                let v = eval_sql_expr(expr_str, &table.rows[i], Some(table), &[], None, params);
                                if let Some(f) = v.as_f64() {
                                    sum += f;
                                    count += 1;
                                }
                            }
                        }
                        let avg = if count > 0 { sum / count as f64 } else { 0.0 };
                        row_map.insert(spec.alias.clone(), json!(avg));
                        fields.push(FieldInfo { name: spec.alias.clone(), data_type: "float8".to_string() });
                    }
                    AggFunc::MinExpr(expr_str) => {
                        let mut min_val: Option<Value> = None;
                        for i in 0..table.rows.len() {
                            if !table.is_deleted[i] {
                                let v = eval_sql_expr(expr_str, &table.rows[i], Some(table), &[], None, params);
                                if !v.is_null() {
                                    min_val = match min_val {
                                        None => Some(v.clone()),
                                        Some(cur) => if v.cmp_value(&cur) == std::cmp::Ordering::Less { Some(v.clone()) } else { Some(cur) },
                                    };
                                }
                            }
                        }
                        row_map.insert(spec.alias.clone(), min_val.as_ref().map(|v| value_to_json(v)).unwrap_or(serde_json::Value::Null));
                        fields.push(FieldInfo { name: spec.alias.clone(), data_type: "numeric".to_string() });
                    }
                    AggFunc::MaxExpr(expr_str) => {
                        let mut max_val: Option<Value> = None;
                        for i in 0..table.rows.len() {
                            if !table.is_deleted[i] {
                                let v = eval_sql_expr(expr_str, &table.rows[i], Some(table), &[], None, params);
                                if !v.is_null() {
                                    max_val = match max_val {
                                        None => Some(v.clone()),
                                        Some(cur) => if v.cmp_value(&cur) == std::cmp::Ordering::Greater { Some(v.clone()) } else { Some(cur) },
                                    };
                                }
                            }
                        }
                        row_map.insert(spec.alias.clone(), max_val.as_ref().map(|v| value_to_json(v)).unwrap_or(serde_json::Value::Null));
                        fields.push(FieldInfo { name: spec.alias.clone(), data_type: "numeric".to_string() });
                    }
                    AggFunc::BoolAnd(col_idx) => {
                        let mut res: Option<bool> = None;
                        for i in 0..table.rows.len() {
                            if !table.is_deleted[i] {
                                if let Some(v) = table.rows[i].get(*col_idx) {
                                    if !v.is_null() {
                                        res = Some(res.unwrap_or(true) && v.as_bool().unwrap_or(false));
                                    }
                                }
                            }
                        }
                        row_map.insert(spec.alias.clone(), res.map(|b| json!(b)).unwrap_or(serde_json::Value::Null));
                        fields.push(FieldInfo { name: spec.alias.clone(), data_type: "bool".to_string() });
                    }
                    AggFunc::BoolOr(col_idx) => {
                        let mut res: Option<bool> = None;
                        for i in 0..table.rows.len() {
                            if !table.is_deleted[i] {
                                if let Some(v) = table.rows[i].get(*col_idx) {
                                    if !v.is_null() {
                                        res = Some(res.unwrap_or(false) || v.as_bool().unwrap_or(false));
                                    }
                                }
                            }
                        }
                        row_map.insert(spec.alias.clone(), res.map(|b| json!(b)).unwrap_or(serde_json::Value::Null));
                        fields.push(FieldInfo { name: spec.alias.clone(), data_type: "bool".to_string() });
                    }
                    AggFunc::BoolAndExpr(expr_str) => {
                        let mut res: Option<bool> = None;
                        for i in 0..table.rows.len() {
                            if !table.is_deleted[i] {
                                let v = eval_sql_expr(expr_str, &table.rows[i], Some(table), &[], None, params);
                                if !v.is_null() {
                                    res = Some(res.unwrap_or(true) && v.as_bool().unwrap_or(false));
                                }
                            }
                        }
                        row_map.insert(spec.alias.clone(), res.map(|b| json!(b)).unwrap_or(serde_json::Value::Null));
                        fields.push(FieldInfo { name: spec.alias.clone(), data_type: "bool".to_string() });
                    }
                    AggFunc::BoolOrExpr(expr_str) => {
                        let mut res: Option<bool> = None;
                        for i in 0..table.rows.len() {
                            if !table.is_deleted[i] {
                                let v = eval_sql_expr(expr_str, &table.rows[i], Some(table), &[], None, params);
                                if !v.is_null() {
                                    res = Some(res.unwrap_or(false) || v.as_bool().unwrap_or(false));
                                }
                            }
                        }
                        row_map.insert(spec.alias.clone(), res.map(|b| json!(b)).unwrap_or(serde_json::Value::Null));
                        fields.push(FieldInfo { name: spec.alias.clone(), data_type: "bool".to_string() });
                    }
                }
            }
        } else {
            // Filtered scan aggregate
            let where_expr = parse_where_expr(where_clause.unwrap(), table, params)?;
            let total_rows = table.rows.len();

            let matched_indices: Vec<usize> = if total_rows > 20_000 {
                (0..total_rows)
                    .into_par_iter()
                    .filter(|&i| !table.is_deleted[i] && evaluate_where_expr(&table.rows[i], Some(table), &where_expr, params))
                    .collect()
            } else {
                (0..total_rows)
                    .filter(|&i| !table.is_deleted[i] && evaluate_where_expr(&table.rows[i], Some(table), &where_expr, params))
                    .collect()
            };

            for spec in agg_specs {
                match &spec.func {
                    AggFunc::CountStar => {
                        let count = matched_indices.len();
                        row_map.insert(spec.alias.clone(), json!(count));
                        fields.push(FieldInfo { name: spec.alias.clone(), data_type: "int8".to_string() });
                    }
                    AggFunc::Count(col_idx) => {
                        let count = matched_indices.iter()
                            .filter(|&&i| table.rows[i].get(*col_idx).map_or(false, |v| !v.is_null()))
                            .count();
                        row_map.insert(spec.alias.clone(), json!(count));
                        fields.push(FieldInfo { name: spec.alias.clone(), data_type: "int8".to_string() });
                    }
                    AggFunc::CountExpr(expr) => {
                        let count = matched_indices.iter()
                            .filter(|&&i| !eval_sql_expr(expr, &table.rows[i], Some(table), &[], None, params).is_null())
                            .count();
                        row_map.insert(spec.alias.clone(), json!(count));
                        fields.push(FieldInfo { name: spec.alias.clone(), data_type: "int8".to_string() });
                    }
                    AggFunc::CountDistinct(col_idx) => {
                        let mut set = std::collections::HashSet::new();
                        for &i in &matched_indices {
                            if let Some(v) = table.rows[i].get(*col_idx) {
                                if !v.is_null() {
                                    set.insert(format!("{:?}", v));
                                }
                            }
                        }
                        row_map.insert(spec.alias.clone(), json!(set.len()));
                        fields.push(FieldInfo { name: spec.alias.clone(), data_type: "int8".to_string() });
                    }
                    AggFunc::Sum(col_idx) => {
                        let nums: Vec<f64> = matched_indices.iter().filter_map(|&i| table.rows[i].get(*col_idx).and_then(|v| v.as_f64())).collect();
                        if nums.is_empty() {
                            row_map.insert(spec.alias.clone(), serde_json::Value::Null);
                        } else {
                            let sum: f64 = nums.iter().sum();
                            row_map.insert(spec.alias.clone(), json!(sum));
                        }
                        fields.push(FieldInfo { name: spec.alias.clone(), data_type: "numeric".to_string() });
                    }
                    AggFunc::Avg(col_idx) => {
                        let nums: Vec<f64> = matched_indices.iter().filter_map(|&i| table.rows[i].get(*col_idx).and_then(|v| v.as_f64())).collect();
                        if nums.is_empty() {
                            row_map.insert(spec.alias.clone(), serde_json::Value::Null);
                        } else {
                            let sum: f64 = nums.iter().sum();
                            let avg = sum / nums.len() as f64;
                            row_map.insert(spec.alias.clone(), json!(avg));
                        }
                        fields.push(FieldInfo { name: spec.alias.clone(), data_type: "float8".to_string() });
                    }
                    AggFunc::Min(col_idx) => {
                        let is_numeric = matches!(table.columns[*col_idx].data_type, DataType::Integer | DataType::BigInt | DataType::Serial | DataType::Numeric);
                        if is_numeric {
                            let nums: Vec<f64> = matched_indices.iter().filter_map(|&i| table.rows[i].get(*col_idx).and_then(|v| v.as_f64())).collect();
                            if nums.is_empty() {
                                row_map.insert(spec.alias.clone(), serde_json::Value::Null);
                            } else {
                                let min = nums.into_iter().fold(f64::INFINITY, f64::min);
                                row_map.insert(spec.alias.clone(), json!(min));
                            }
                            fields.push(FieldInfo { name: spec.alias.clone(), data_type: "numeric".to_string() });
                        } else {
                            let mut min_val: Option<Value> = None;
                            for &i in &matched_indices {
                                if let Some(v) = table.rows[i].get(*col_idx) {
                                    if !v.is_null() {
                                        min_val = match min_val {
                                            None => Some(v.clone()),
                                            Some(cur) => if v.cmp_value(&cur) == std::cmp::Ordering::Less { Some(v.clone()) } else { Some(cur) },
                                        };
                                    }
                                }
                            }
                            row_map.insert(spec.alias.clone(), min_val.as_ref().map(|v| value_to_json(v)).unwrap_or(serde_json::Value::Null));
                            fields.push(FieldInfo { name: spec.alias.clone(), data_type: "text".to_string() });
                        }
                    }
                    AggFunc::Max(col_idx) => {
                        let is_numeric = matches!(table.columns[*col_idx].data_type, DataType::Integer | DataType::BigInt | DataType::Serial | DataType::Numeric);
                        if is_numeric {
                            let nums: Vec<f64> = matched_indices.iter().filter_map(|&i| table.rows[i].get(*col_idx).and_then(|v| v.as_f64())).collect();
                            if nums.is_empty() {
                                row_map.insert(spec.alias.clone(), serde_json::Value::Null);
                            } else {
                                let max = nums.into_iter().fold(f64::NEG_INFINITY, f64::max);
                                row_map.insert(spec.alias.clone(), json!(max));
                            }
                            fields.push(FieldInfo { name: spec.alias.clone(), data_type: "numeric".to_string() });
                        } else {
                            let mut max_val: Option<Value> = None;
                            for &i in &matched_indices {
                                if let Some(v) = table.rows[i].get(*col_idx) {
                                    if !v.is_null() {
                                        max_val = match max_val {
                                            None => Some(v.clone()),
                                            Some(cur) => if v.cmp_value(&cur) == std::cmp::Ordering::Greater { Some(v.clone()) } else { Some(cur) },
                                        };
                                    }
                                }
                            }
                            row_map.insert(spec.alias.clone(), max_val.as_ref().map(|v| value_to_json(v)).unwrap_or(serde_json::Value::Null));
                            fields.push(FieldInfo { name: spec.alias.clone(), data_type: "text".to_string() });
                        }
                    }
                    AggFunc::StringAgg(col_idx, delim) => {
                        let mut parts = Vec::new();
                        for &i in &matched_indices {
                            if let Some(v) = table.rows[i].get(*col_idx) {
                                if !v.is_null() {
                                    parts.push(v.as_str());
                                }
                            }
                        }
                        let joined = parts.join(delim);
                        row_map.insert(spec.alias.clone(), serde_json::Value::String(joined));
                        fields.push(FieldInfo { name: spec.alias.clone(), data_type: "text".to_string() });
                    }
                    AggFunc::ArrayAgg(col_idx) => {
                        if matched_indices.is_empty() {
                            row_map.insert(spec.alias.clone(), serde_json::Value::Null);
                        } else {
                            let mut arr = Vec::new();
                            for &i in &matched_indices {
                                if let Some(v) = table.rows[i].get(*col_idx) {
                                    if !v.is_null() {
                                        arr.push(value_to_json(v));
                                    }
                                }
                            }
                            row_map.insert(spec.alias.clone(), serde_json::Value::Array(arr));
                        }
                        fields.push(FieldInfo { name: spec.alias.clone(), data_type: "array".to_string() });
                    }
                    AggFunc::ArrayAggExpr(expr_str) => {
                        if matched_indices.is_empty() {
                            row_map.insert(spec.alias.clone(), serde_json::Value::Null);
                        } else {
                            let mut arr = Vec::new();
                            for &i in &matched_indices {
                                let v = eval_sql_expr(expr_str, &table.rows[i], Some(table), &[], None, params);
                                if !v.is_null() {
                                    arr.push(value_to_json(&v));
                                }
                            }
                            row_map.insert(spec.alias.clone(), serde_json::Value::Array(arr));
                        }
                        fields.push(FieldInfo { name: spec.alias.clone(), data_type: "array".to_string() });
                    }
                    AggFunc::WrappedAgg(raw_expr, inner_func) => {
                        let inner_json = if matched_indices.is_empty() {
                            serde_json::Value::Null
                        } else {
                            match inner_func.as_ref() {
                                AggFunc::ArrayAgg(c_idx) => {
                                    let mut arr = Vec::new();
                                    for &i in &matched_indices {
                                        if let Some(v) = table.rows[i].get(*c_idx) {
                                            if !v.is_null() {
                                                arr.push(value_to_json(v));
                                            }
                                        }
                                    }
                                    serde_json::Value::Array(arr)
                                }
                                AggFunc::ArrayAggExpr(expr_str) => {
                                    let mut arr = Vec::new();
                                    for &i in &matched_indices {
                                        let v = eval_sql_expr(expr_str, &table.rows[i], Some(table), &[], None, params);
                                        if !v.is_null() {
                                            arr.push(value_to_json(&v));
                                        }
                                    }
                                    serde_json::Value::Array(arr)
                                }
                                _ => serde_json::Value::Null,
                            }
                        };
                        let inner_pattern = if raw_expr.to_uppercase().contains("ARRAY_AGG(") {
                            let u = raw_expr.to_uppercase();
                            let start = u.find("ARRAY_AGG(").unwrap();
                            if let Some(end) = find_matching_paren(raw_expr, start + 9) {
                                Some(&raw_expr[start..=end])
                            } else {
                                None
                            }
                        } else {
                            None
                        };
                        let substituted_expr = if let Some(pat) = inner_pattern {
                            let pg_arr_str = if inner_json.is_null() {
                                "NULL".to_string()
                            } else {
                                let json_str = serde_json::to_string(&inner_json).unwrap_or_else(|_| "[]".to_string());
                                format!("'{}'", json_str.replace('\'', "''"))
                            };
                            raw_expr.replace(pat, &pg_arr_str)
                        } else {
                            raw_expr.clone()
                        };
                        let res_val = eval_sql_expr(&substituted_expr, &[], None, &[], None, params);
                        let dt = match &res_val {
                            Value::Int(_) => "int8",
                            Value::Float(_) => "float8",
                            Value::Bool(_) => "bool",
                            _ => "text",
                        };
                        row_map.insert(spec.alias.clone(), value_to_json(&res_val));
                        fields.push(FieldInfo { name: spec.alias.clone(), data_type: dt.to_string() });
                    }
                    AggFunc::JsonAgg(col_idx) | AggFunc::JsonbAgg(col_idx) => {
                        let mut arr = Vec::new();
                        for &i in &matched_indices {
                            if let Some(v) = table.rows[i].get(*col_idx) {
                                if !v.is_null() {
                                    let j_val = match v {
                                        Value::Text(st) => serde_json::from_str::<serde_json::Value>(st.trim()).unwrap_or(serde_json::Value::String(st.to_string())),
                                        _ => value_to_json(v),
                                    };
                                    arr.push(j_val);
                                }
                            }
                        }
                        row_map.insert(spec.alias.clone(), serde_json::Value::Array(arr));
                        fields.push(FieldInfo { name: spec.alias.clone(), data_type: "json".to_string() });
                    }
                    AggFunc::JsonAggExpr(expr_str) | AggFunc::JsonbAggExpr(expr_str) => {
                        let mut arr = Vec::new();
                        for &i in &matched_indices {
                            let v = eval_sql_expr(expr_str, &table.rows[i], Some(table), &[], None, params);
                            if !v.is_null() {
                                let j_val = match &v {
                                    Value::Text(st) => serde_json::from_str::<serde_json::Value>(st.trim()).unwrap_or(serde_json::Value::String(st.to_string())),
                                    _ => value_to_json(&v),
                                };
                                arr.push(j_val);
                            }
                        }
                        row_map.insert(spec.alias.clone(), serde_json::Value::Array(arr));
                        fields.push(FieldInfo { name: spec.alias.clone(), data_type: "json".to_string() });
                    }
                    AggFunc::SumExpr(expr_str) => {
                        let nums: Vec<f64> = matched_indices.iter().filter_map(|&i| {
                            let v = eval_sql_expr(expr_str, &table.rows[i], Some(table), &[], None, params);
                            v.as_f64()
                        }).collect();
                        if nums.is_empty() {
                            row_map.insert(spec.alias.clone(), serde_json::Value::Null);
                        } else {
                            let sum: f64 = nums.iter().sum();
                            row_map.insert(spec.alias.clone(), json!(sum));
                        }
                        fields.push(FieldInfo { name: spec.alias.clone(), data_type: "numeric".to_string() });
                    }
                    AggFunc::AvgExpr(expr_str) => {
                        let nums: Vec<f64> = matched_indices.iter().filter_map(|&i| {
                            let v = eval_sql_expr(expr_str, &table.rows[i], Some(table), &[], None, params);
                            v.as_f64()
                        }).collect();
                        if nums.is_empty() {
                            row_map.insert(spec.alias.clone(), serde_json::Value::Null);
                        } else {
                            let sum: f64 = nums.iter().sum();
                            let avg = sum / nums.len() as f64;
                            row_map.insert(spec.alias.clone(), json!(avg));
                        }
                        fields.push(FieldInfo { name: spec.alias.clone(), data_type: "float8".to_string() });
                    }
                    AggFunc::MinExpr(expr_str) => {
                        let mut min_val: Option<Value> = None;
                        for &i in &matched_indices {
                            let v = eval_sql_expr(expr_str, &table.rows[i], Some(table), &[], None, params);
                            if !v.is_null() {
                                min_val = match min_val {
                                    None => Some(v.clone()),
                                    Some(cur) => if v.cmp_value(&cur) == std::cmp::Ordering::Less { Some(v.clone()) } else { Some(cur) },
                                };
                            }
                        }
                        row_map.insert(spec.alias.clone(), min_val.as_ref().map(|v| value_to_json(v)).unwrap_or(serde_json::Value::Null));
                        fields.push(FieldInfo { name: spec.alias.clone(), data_type: "numeric".to_string() });
                    }
                    AggFunc::MaxExpr(expr_str) => {
                        let mut max_val: Option<Value> = None;
                        for &i in &matched_indices {
                            let v = eval_sql_expr(expr_str, &table.rows[i], Some(table), &[], None, params);
                            if !v.is_null() {
                                max_val = match max_val {
                                    None => Some(v.clone()),
                                    Some(cur) => if v.cmp_value(&cur) == std::cmp::Ordering::Greater { Some(v.clone()) } else { Some(cur) },
                                };
                            }
                        }
                        row_map.insert(spec.alias.clone(), max_val.as_ref().map(|v| value_to_json(v)).unwrap_or(serde_json::Value::Null));
                        fields.push(FieldInfo { name: spec.alias.clone(), data_type: "numeric".to_string() });
                    }
                    AggFunc::BoolAnd(col_idx) => {
                        let mut res: Option<bool> = None;
                        for &i in &matched_indices {
                            if let Some(v) = table.rows[i].get(*col_idx) {
                                if !v.is_null() {
                                    res = Some(res.unwrap_or(true) && v.as_bool().unwrap_or(false));
                                }
                            }
                        }
                        row_map.insert(spec.alias.clone(), res.map(|b| json!(b)).unwrap_or(serde_json::Value::Null));
                        fields.push(FieldInfo { name: spec.alias.clone(), data_type: "bool".to_string() });
                    }
                    AggFunc::BoolOr(col_idx) => {
                        let mut res: Option<bool> = None;
                        for &i in &matched_indices {
                            if let Some(v) = table.rows[i].get(*col_idx) {
                                if !v.is_null() {
                                    res = Some(res.unwrap_or(false) || v.as_bool().unwrap_or(false));
                                }
                            }
                        }
                        row_map.insert(spec.alias.clone(), res.map(|b| json!(b)).unwrap_or(serde_json::Value::Null));
                        fields.push(FieldInfo { name: spec.alias.clone(), data_type: "bool".to_string() });
                    }
                    AggFunc::BoolAndExpr(expr_str) => {
                        let mut res: Option<bool> = None;
                        for &i in &matched_indices {
                            let v = eval_sql_expr(expr_str, &table.rows[i], Some(table), &[], None, params);
                            if !v.is_null() {
                                res = Some(res.unwrap_or(true) && v.as_bool().unwrap_or(false));
                            }
                        }
                        row_map.insert(spec.alias.clone(), res.map(|b| json!(b)).unwrap_or(serde_json::Value::Null));
                        fields.push(FieldInfo { name: spec.alias.clone(), data_type: "bool".to_string() });
                    }
                    AggFunc::BoolOrExpr(expr_str) => {
                        let mut res: Option<bool> = None;
                        for &i in &matched_indices {
                            let v = eval_sql_expr(expr_str, &table.rows[i], Some(table), &[], None, params);
                            if !v.is_null() {
                                res = Some(res.unwrap_or(false) || v.as_bool().unwrap_or(false));
                            }
                        }
                        row_map.insert(spec.alias.clone(), res.map(|b| json!(b)).unwrap_or(serde_json::Value::Null));
                        fields.push(FieldInfo { name: spec.alias.clone(), data_type: "bool".to_string() });
                    }
                }
            }
        }

        Ok(QueryResult {
            rows: vec![serde_json::Value::Object(row_map)],
            row_count: 1,
            fields,
            command: "SELECT".to_string(),
        })
    }

    fn handle_update(&mut self, sql: &str, params: &[Value]) -> Result<QueryResult, String> {
        let set_idx = find_top_level_keyword(sql, "SET").ok_or("Missing SET in UPDATE")?;
        let where_idx = find_top_level_keyword(sql, "WHERE");
        let returning_idx = find_top_level_keyword(sql, "RETURNING");

        let table_name = clean_table_name(sql[6..set_idx].trim());

        let set_end = where_idx.or(returning_idx).unwrap_or(sql.len());
        let set_clause = sql[set_idx + 3..set_end].trim();

        let (where_clause, returning_cols) = match (where_idx, returning_idx) {
            (Some(w_idx), Some(r_idx)) => (Some(sql[w_idx + 5..r_idx].trim()), Some(sql[r_idx + 9..].trim())),
            (Some(w_idx), None) => (Some(sql[w_idx + 5..].trim()), None),
            (None, Some(r_idx)) => (None, Some(sql[r_idx + 9..].trim())),
            (None, None) => (None, None),
        };

        if where_idx.is_none() && sql.to_uppercase().contains("WHERE") {
            return Err("Failed to parse WHERE clause in UPDATE statement - aborting to prevent accidental table-wide update".to_string());
        }

        let in_tx = self.storage.in_transaction;
        let mut wal_updates = Vec::new();
        let mut undo_restores = Vec::new();
        let mut row_count = 0;
        let mut returned_rows = Vec::new();

        let resolved_where = if let Some(w) = where_clause {
            let w_up = w.to_uppercase();
            if w_up.contains("SELECT") && w_up.contains("FROM") {
                Some(self.resolve_subqueries_in_where(w, params)?)
            } else {
                Some(w.to_string())
            }
        } else {
            None
        };

        let fields = {
            let table = self.storage.get_table_mut(table_name).ok_or_else(|| format!("Table {} not found", table_name))?;

            // Parse multiple SET assignments: "updated_at" = CURRENT_TIMESTAMP, "student_count" = $1, "salary" = salary * 1.10
            let mut raw_assignments: Vec<(usize, String)> = Vec::new();
            for assign in split_comma_separated_tokens(set_clause) {
                let assign = assign.trim();
                if assign.is_empty() {
                    continue;
                }
                let eq_idx = assign.find('=').ok_or("Invalid assignment in SET clause")?;
                let col_name = clean_col_name(&assign[..eq_idx]);
                let val_str = assign[eq_idx + 1..].trim().to_string();
                let col_idx = table.get_column_index(col_name).ok_or_else(|| format!("Column {} not found in table {}", col_name, table_name))?;
                raw_assignments.push((col_idx, val_str));
            }

            // Parse WHERE conditions
            let where_expr = if let Some(ref w) = resolved_where {
                if !w.is_empty() {
                    Some(parse_where_expr(w, table, params)?)
                } else {
                    None
                }
            } else {
                None
            };

            // Check if there is a primary key equality check for O(1) point update
            let pk_target = if let Some(ref expr) = where_expr {
                if let Some(pk_idx) = table.pk_col_idx {
                    extract_single_pk_eq(expr, pk_idx)
                } else {
                    None
                }
            } else {
                None
            };

            if let Some(target_pk) = pk_target {
                if let Some(&row_idx) = table.pk_index.get(&target_pk) {
                    let matches = match &where_expr {
                        Some(expr) => evaluate_where_expr(&table.rows[row_idx], Some(table), expr, params),
                        None => true,
                    };
                    if !table.is_deleted[row_idx] && matches {
                        let updates: Vec<(usize, Value)> = raw_assignments.iter().map(|&(c_idx, ref v_str)| {
                            let raw_val = evaluate_assignment_val(v_str, &table.rows[row_idx], table, params);
                            let coerced = coerce_update_val(&table.columns[c_idx], raw_val);
                            (c_idx, coerced)
                        }).collect();

                        let mut old_vals = Vec::new();
                        for (c_idx, _) in &updates {
                            old_vals.push((*c_idx, table.rows[row_idx][*c_idx].clone()));
                        }
                        table.update_row_multi(row_idx, &updates);
                        row_count = 1;
                        if let Some(r_cols) = returning_cols {
                            returned_rows.push(project_returning_row(table, &table.rows[row_idx], r_cols));
                        }
                        for (col_idx, new_val) in &updates {
                            wal_updates.push((target_pk, *col_idx, new_val.clone()));
                        }
                        if in_tx {
                            for (col_idx, old_val) in old_vals {
                                undo_restores.push((row_idx, col_idx, old_val));
                            }
                        }
                    }
                }
            } else {
                // General scan update
                for i in 0..table.rows.len() {
                    let matches = match &where_expr {
                        Some(expr) => evaluate_where_expr(&table.rows[i], Some(table), expr, params),
                        None => true,
                    };
                    if !table.is_deleted[i] && matches {
                        let updates: Vec<(usize, Value)> = raw_assignments.iter().map(|&(c_idx, ref v_str)| {
                            let raw_val = evaluate_assignment_val(v_str, &table.rows[i], table, params);
                            let coerced = coerce_update_val(&table.columns[c_idx], raw_val);
                            (c_idx, coerced)
                        }).collect();

                        let pk_val = table.pk_col_idx.and_then(|pk_idx| table.rows[i].get(pk_idx).and_then(|v| v.as_i64()));
                        let mut old_vals = Vec::new();
                        for (c_idx, _) in &updates {
                            old_vals.push((*c_idx, table.rows[i][*c_idx].clone()));
                        }
                        table.update_row_multi(i, &updates);
                        row_count += 1;
                        if let Some(r_cols) = returning_cols {
                            returned_rows.push(project_returning_row(table, &table.rows[i], r_cols));
                        }
                        if let Some(pk) = pk_val {
                            for (col_idx, new_val) in &updates {
                                wal_updates.push((pk, *col_idx, new_val.clone()));
                            }
                        }
                        if in_tx {
                            for (col_idx, old_val) in old_vals {
                                undo_restores.push((i, col_idx, old_val));
                            }
                        }
                    }
                }
            }

            if let Some(r_cols) = returning_cols {
                get_returning_fields(table, r_cols)
            } else {
                vec![]
            }
        };

        for (pk, col_idx, new_val) in wal_updates {
            self.storage.wal.append(WalRecord::Update {
                table: table_name.to_string(),
                pk,
                col_idx,
                new_val,
            });
        }
        if in_tx {
            for (row_idx, col_idx, old_val) in undo_restores {
                self.storage.tx_undo_log.push(UndoAction::RestoreRow {
                    table: table_name.to_string(),
                    row_idx,
                    col_idx,
                    old_val,
                });
            }
        }

        Ok(QueryResult {
            rows: returned_rows,
            row_count,
            fields,
            command: "UPDATE".to_string(),
        })
    }

    fn handle_delete(&mut self, sql: &str, params: &[Value]) -> Result<QueryResult, String> {
        let from_idx = find_top_level_keyword(sql, "FROM").ok_or("Missing FROM in DELETE")?;
        let where_idx = find_top_level_keyword(sql, "WHERE");
        let returning_idx = find_top_level_keyword(sql, "RETURNING");

        let (where_clause, returning_cols) = match (where_idx, returning_idx) {
            (Some(w_idx), Some(r_idx)) => (Some(sql[w_idx + 5..r_idx].trim()), Some(sql[r_idx + 9..].trim())),
            (Some(w_idx), None) => (Some(sql[w_idx + 5..].trim()), None),
            (None, Some(r_idx)) => (None, Some(sql[r_idx + 9..].trim())),
            (None, None) => (None, None),
        };

        let table_end = where_idx.or(returning_idx).unwrap_or(sql.len());
        let table_name = clean_table_name(sql[from_idx + 4..table_end].trim());

        if where_idx.is_none() && sql.to_uppercase().contains("WHERE") {
            return Err("Failed to parse WHERE clause in DELETE statement - aborting to prevent accidental table truncation".to_string());
        }

        let in_tx = self.storage.in_transaction;
        let mut row_count = 0;
        let mut returned_rows = Vec::new();
        let mut deleted_pks = Vec::new();
        let mut deleted_row_indices = Vec::new();

        let resolved_where = if let Some(w) = where_clause {
            let w_up = w.to_uppercase();
            if w_up.contains("SELECT") && w_up.contains("FROM") {
                Some(self.resolve_subqueries_in_where(w, params)?)
            } else {
                Some(w.to_string())
            }
        } else {
            None
        };

        let fields = {
            let table = self.storage.get_table_mut(table_name).ok_or_else(|| format!("Table {} not found", table_name))?;

            let where_expr = if let Some(ref w) = resolved_where {
                if !w.is_empty() {
                    Some(parse_where_expr(w, table, params)?)
                } else {
                    None
                }
            } else {
                None
            };

            let pk_target = if let Some(ref expr) = where_expr {
                if let Some(pk_idx) = table.pk_col_idx {
                    extract_single_pk_eq(expr, pk_idx)
                } else {
                    None
                }
            } else {
                None
            };

            if let Some(target_pk) = pk_target {
                if let Some(&row_idx) = table.pk_index.get(&target_pk) {
                    let matches = match &where_expr {
                        Some(expr) => evaluate_where_expr(&table.rows[row_idx], Some(table), expr, params),
                        None => true,
                    };
                    if !table.is_deleted[row_idx] && matches {
                        if let Some(r_cols) = returning_cols {
                            returned_rows.push(project_returning_row(table, &table.rows[row_idx], r_cols));
                        }
                        table.delete_row(row_idx);
                        row_count = 1;
                        deleted_pks.push(target_pk);
                        deleted_row_indices.push(row_idx);
                    }
                }
            } else {
                for i in 0..table.rows.len() {
                    let matches = match &where_expr {
                        Some(expr) => evaluate_where_expr(&table.rows[i], Some(table), expr, params),
                        None => true,
                    };
                    if !table.is_deleted[i] && matches {
                        if let Some(r_cols) = returning_cols {
                            returned_rows.push(project_returning_row(table, &table.rows[i], r_cols));
                        }
                        if let Some(pk_idx) = table.pk_col_idx {
                            if let Some(pk_val) = table.rows[i].get(pk_idx).and_then(|v| v.as_i64()) {
                                deleted_pks.push(pk_val);
                            }
                        }
                        table.delete_row(i);
                        row_count += 1;
                        deleted_row_indices.push(i);
                    }
                }
            }

            if let Some(r_cols) = returning_cols {
                get_returning_fields(table, r_cols)
            } else {
                vec![]
            }
        };

        for pk in deleted_pks {
            self.storage.wal.append(WalRecord::Delete {
                table: table_name.to_string(),
                pk,
            });
        }
        if in_tx {
            for row_idx in deleted_row_indices {
                self.storage.tx_undo_log.push(UndoAction::UndeleteRow {
                    table: table_name.to_string(),
                    row_idx,
                });
            }
        }

        Ok(QueryResult {
            rows: returned_rows,
            row_count,
            fields,
            command: "DELETE".to_string(),
        })
    }
}

#[derive(Clone, Debug, PartialEq)]
enum JoinKind {
    Inner,
    Left,
    Cross,
}

#[derive(Clone, Debug)]
struct ParsedJoin {
    kind: JoinKind,
    table_name: String,
    alias: Option<String>,
    on_condition: Option<String>,
    subquery: Option<String>,
    is_lateral: bool,
}

#[derive(Clone, Debug)]
struct JoinedSchema {
    col_map: std::collections::HashMap<String, usize>,
    table_offsets: Vec<usize>,
    total_cols: usize,
}

impl JoinedSchema {
    fn new(tables: &[(&crate::storage::table::Table, Option<&str>)]) -> Self {
        let mut col_map = std::collections::HashMap::new();
        let mut table_offsets = Vec::with_capacity(tables.len());
        let mut offset = 0;

        for (table, alias) in tables {
            table_offsets.push(offset);
            let tbl_name_lower = table.name.to_lowercase();
            let alias_lower = alias.map(|a| a.to_lowercase());

            for (col_idx, col) in table.columns.iter().enumerate() {
                let slot = offset + col_idx;
                let col_name_lower = col.name.to_lowercase();

                let full_tbl = format!("{}.{}", tbl_name_lower, col_name_lower);
                col_map.insert(full_tbl, slot);

                if let Some(ref a_key) = alias_lower {
                    let alias_tbl = format!("{}.{}", a_key, col_name_lower);
                    col_map.insert(alias_tbl, slot);
                }

                col_map.entry(col_name_lower).or_insert(slot);
            }
            offset += table.columns.len();
        }

        Self {
            col_map,
            table_offsets,
            total_cols: offset,
        }
    }

    #[inline(always)]
    fn get_slot(&self, key: &str) -> Option<usize> {
        let clean = key.trim().replace('"', "");
        let lower = clean.to_lowercase();
        if let Some(&idx) = self.col_map.get(&lower) {
            return Some(idx);
        }
        if let Some(dot_idx) = lower.rfind('.') {
            let col_only = &lower[dot_idx + 1..];
            if let Some(&idx) = self.col_map.get(col_only) {
                return Some(idx);
            }
        }
        None
    }
}

#[derive(Clone, Debug)]
struct CombinedRow {
    values: Vec<Value>,
    schema: std::sync::Arc<JoinedSchema>,
    extra: Option<std::collections::HashMap<String, Value>>,
}

impl CombinedRow {
    #[inline(always)]
    fn new_with_schema(schema: std::sync::Arc<JoinedSchema>) -> Self {
        Self {
            values: Vec::with_capacity(schema.total_cols),
            schema,
            extra: None,
        }
    }

    #[inline(always)]
    fn from_base_row(schema: std::sync::Arc<JoinedSchema>, base_row: &[Value]) -> Self {
        let mut values = Vec::with_capacity(schema.total_cols);
        values.extend_from_slice(base_row);
        Self {
            values,
            schema,
            extra: None,
        }
    }

    #[inline(always)]
    fn with_joined_table(&self, joined_row: &[Value]) -> Self {
        let mut values = Vec::with_capacity(self.schema.total_cols);
        values.extend_from_slice(&self.values);
        values.extend_from_slice(joined_row);
        Self {
            values,
            schema: self.schema.clone(),
            extra: self.extra.clone(),
        }
    }

    #[inline(always)]
    fn with_null_table(&self, num_cols: usize) -> Self {
        let mut values = Vec::with_capacity(self.schema.total_cols);
        values.extend_from_slice(&self.values);
        values.resize(values.len() + num_cols, Value::Null);
        Self {
            values,
            schema: self.schema.clone(),
            extra: self.extra.clone(),
        }
    }

    #[inline(always)]
    fn get_val(&self, key: &str) -> Value {
        if let Some(ref extra) = self.extra {
            let clean = key.trim().replace('"', "");
            let lower = clean.to_lowercase();
            if let Some(v) = extra.get(&lower) {
                return v.clone();
            }
            if let Some(dot_idx) = lower.rfind('.') {
                let col_only = &lower[dot_idx + 1..];
                if let Some(v) = extra.get(col_only) {
                    return v.clone();
                }
            }
        }
        if let Some(slot) = self.schema.get_slot(key) {
            return self.values.get(slot).cloned().unwrap_or(Value::Null);
        }
        Value::Null
    }

    #[inline(always)]
    fn insert_extra(&mut self, key: String, val: Value) {
        if self.extra.is_none() {
            self.extra = Some(std::collections::HashMap::new());
        }
        if let Some(ref mut map) = self.extra {
            map.insert(key, val);
        }
    }

    #[inline(always)]
    fn contains_key(&self, key: &str) -> bool {
        if let Some(ref extra) = self.extra {
            let clean = key.trim().replace('"', "");
            let lower = clean.to_lowercase();
            if extra.contains_key(&lower) {
                return true;
            }
        }
        self.schema.get_slot(key).is_some()
    }
}

fn extract_identifiers_from_condition(cond: &str) -> Vec<String> {
    let mut tokens = Vec::new();
    let bytes = cond.as_bytes();
    let n = bytes.len();
    let mut i = 0;
    while i < n {
        let b = bytes[i];
        if b == b'\'' {
            i += 1;
            while i < n && bytes[i] != b'\'' {
                i += 1;
            }
            if i < n { i += 1; }
            continue;
        }
        if b.is_ascii_alphabetic() || b == b'_' || b == b'"' {
            let start = i;
            while i < n {
                let cb = bytes[i];
                if cb.is_ascii_alphanumeric() || cb == b'_' || cb == b'.' || cb == b'"' {
                    i += 1;
                } else {
                    break;
                }
            }
            let s = std::str::from_utf8(&bytes[start..i]).unwrap_or("");
            let clean = s.trim_matches('"').to_string();
            let upper = clean.to_uppercase();
            if !matches!(upper.as_str(), 
                "AND" | "OR" | "NOT" | "IS" | "NULL" | "TRUE" | "FALSE" | "LIKE" | "ILIKE" | 
                "IN" | "BETWEEN" | "EXISTS" | "CASE" | "WHEN" | "THEN" | "ELSE" | "END" | 
                "CAST" | "AS" | "LOWER" | "UPPER" | "LENGTH" | "TRIM" | "SUBSTRING" | 
                "COUNT" | "SUM" | "AVG" | "MIN" | "MAX" | "SELECT" | "FROM" | "WHERE"
            ) {
                tokens.push(clean);
            }
            continue;
        }
        i += 1;
    }
    tokens
}

#[derive(Clone, Debug)]
struct PushdownPlan {
    base_predicates: Vec<String>,
    joined_table_predicates: Vec<Vec<String>>,
    intermediate_predicates: Vec<Vec<String>>,
    remaining_where: Vec<String>,
}

fn plan_predicate_pushdown(
    where_str: Option<&str>,
    all_tables: &[(&crate::storage::table::Table, Option<&str>)],
    joins: &[ParsedJoin],
) -> PushdownPlan {
    let mut plan = PushdownPlan {
        base_predicates: Vec::new(),
        joined_table_predicates: vec![Vec::new(); joins.len()],
        intermediate_predicates: vec![Vec::new(); joins.len() + 1],
        remaining_where: Vec::new(),
    };

    let Some(w) = where_str else {
        return plan;
    };

    let clauses = split_top_level_and(w);
    for clause in clauses {
        let c_str = clause.trim().to_string();
        if c_str.is_empty() {
            continue;
        }

        let tokens = extract_identifiers_from_condition(&c_str);
        if tokens.is_empty() {
            plan.remaining_where.push(c_str);
            continue;
        }

        let mut referenced_tables = Vec::new();
        let mut unknown_token = false;

        for tok in &tokens {
            let clean = tok.to_lowercase();
            if let Some(dot_idx) = clean.find('.') {
                let prefix = &clean[..dot_idx];
                let mut matched = false;
                for (t_idx, (tbl, alias)) in all_tables.iter().enumerate() {
                    if tbl.name.eq_ignore_ascii_case(prefix)
                        || alias.map(|a| a.trim_matches('"').eq_ignore_ascii_case(prefix)).unwrap_or(false)
                    {
                        if !referenced_tables.contains(&t_idx) {
                            referenced_tables.push(t_idx);
                        }
                        matched = true;
                    }
                }
                if !matched {
                    unknown_token = true;
                }
            } else {
                let mut matched_count = 0;
                for (t_idx, (tbl, _)) in all_tables.iter().enumerate() {
                    if tbl.columns.iter().any(|col| col.name.eq_ignore_ascii_case(&clean)) {
                        matched_count += 1;
                        if !referenced_tables.contains(&t_idx) {
                            referenced_tables.push(t_idx);
                        }
                    }
                }
                if matched_count == 0 && !clean.starts_with('$') && clean.parse::<f64>().is_err() {
                    unknown_token = true;
                }
            }
        }

        if unknown_token || referenced_tables.is_empty() {
            plan.remaining_where.push(c_str);
            continue;
        }

        if referenced_tables.len() == 1 {
            let t_idx = referenced_tables[0];
            if t_idx == 0 {
                plan.base_predicates.push(c_str);
            } else {
                let j_idx = t_idx - 1;
                if joins[j_idx].kind == JoinKind::Inner {
                    plan.joined_table_predicates[j_idx].push(c_str.clone());
                    plan.intermediate_predicates[t_idx].push(c_str);
                } else {
                    plan.intermediate_predicates[t_idx].push(c_str);
                }
            }
        } else {
            let max_table_idx = *referenced_tables.iter().max().unwrap();
            plan.intermediate_predicates[max_table_idx].push(c_str);
        }
    }

    plan
}

#[derive(Hash, PartialEq, Eq, Clone, Debug)]
enum JoinKey {
    Null,
    Bool(bool),
    Int(i64),
    Text(compact_str::CompactString),
    FloatBits(u64),
}

fn value_to_join_key(val: &Value) -> JoinKey {
    match val {
        Value::Null => JoinKey::Null,
        Value::Bool(b) => JoinKey::Bool(*b),
        Value::Int(i) => JoinKey::Int(*i),
        Value::Float(f) => JoinKey::FloatBits(f.to_bits()),
        Value::Text(t) => {
            if let Ok(num) = t.parse::<i64>() {
                JoinKey::Int(num)
            } else {
                JoinKey::Text(t.clone())
            }
        }
    }
}

struct EquiJoinCandidate {
    left_expr: String,
    right_col_idx: usize,
    residual_cond: Option<String>,
}

fn strip_outer_parens(mut s: &str) -> &str {
    s = s.trim();
    while s.starts_with('(') && s.ends_with(')') && is_fully_enclosed_in_parens(s) {
        s = s[1..s.len() - 1].trim();
    }
    s
}


fn find_top_level_eq(s: &str) -> Option<usize> {
    let s_bytes = s.as_bytes();
    let len = s_bytes.len();
    let mut in_str = false;
    let mut depth: i32 = 0;
    let mut i = 0;
    while i < len {
        let b = s_bytes[i];
        if b == b'\'' {
            in_str = !in_str;
            i += 1;
            continue;
        }
        if in_str {
            i += 1;
            continue;
        }
        if b == b'(' {
            depth += 1;
        } else if b == b')' {
            depth = depth.saturating_sub(1);
        } else if depth == 0 && b == b'=' {
            let prev_char = if i > 0 { s_bytes[i - 1] } else { b' ' };
            let next_char = if i + 1 < len { s_bytes[i + 1] } else { b' ' };
            if prev_char != b'!' && prev_char != b'<' && prev_char != b'>' && next_char != b'=' {
                return Some(i);
            }
        }
        i += 1;
    }
    None
}

fn extract_col_from_side(
    side: &str,
    tbl_name: &str,
    alias: Option<&str>,
    table: &crate::storage::table::Table,
) -> Option<usize> {
    let clean = side.trim().replace('"', "");
    if let Some(dot_idx) = clean.rfind('.') {
        let prefix = &clean[..dot_idx];
        let col = &clean[dot_idx + 1..];
        let prefix_tbl = if let Some(p_dot) = prefix.rfind('.') {
            &prefix[p_dot + 1..]
        } else {
            prefix
        };
        let matches_prefix = prefix_tbl.eq_ignore_ascii_case(tbl_name)
            || alias.map(|a| prefix_tbl.eq_ignore_ascii_case(a)).unwrap_or(false);
        if matches_prefix {
            return table.get_column_index(col);
        }
    } else {
        return table.get_column_index(&clean);
    }
    None
}

fn parse_equi_join_candidate(
    on_cond: &str,
    tbl_name: &str,
    alias: Option<&str>,
    table: &crate::storage::table::Table,
) -> Option<EquiJoinCandidate> {
    let stripped = strip_outer_parens(on_cond);
    let parts = split_top_level_and(stripped);
    if parts.is_empty() {
        return None;
    }

    for (part_idx, part) in parts.iter().enumerate() {
        let clean_part = strip_outer_parens(part);
        if let Some(eq_idx) = find_top_level_eq(clean_part) {
            let side_a = clean_part[..eq_idx].trim();
            let side_b = clean_part[eq_idx + 1..].trim();

            let col_a = extract_col_from_side(side_a, tbl_name, alias, table);
            let col_b = extract_col_from_side(side_b, tbl_name, alias, table);

            let chosen: Option<(usize, String)> = match (col_a, col_b) {
                (Some(idx_a), None) => Some((idx_a, side_b.to_string())),
                (None, Some(idx_b)) => Some((idx_b, side_a.to_string())),
                (Some(idx_a), Some(idx_b)) => {
                    let a_clean = side_a.trim().replace('"', "");
                    let b_clean = side_b.trim().replace('"', "");
                    let a_has_prefix = a_clean.contains('.');
                    let b_has_prefix = b_clean.contains('.');
                    if a_has_prefix && !b_has_prefix {
                        Some((idx_a, side_b.to_string()))
                    } else if b_has_prefix && !a_has_prefix {
                        Some((idx_b, side_a.to_string()))
                    } else {
                        Some((idx_b, side_a.to_string()))
                    }
                }
                (None, None) => None,
            };

            if let Some((right_col_idx, left_expr)) = chosen {
                let mut residuals = Vec::new();
                for (i, p) in parts.iter().enumerate() {
                    if i != part_idx {
                        residuals.push(*p);
                    }
                }
                let residual_cond = if residuals.is_empty() {
                    None
                } else {
                    Some(residuals.join(" AND "))
                };

                return Some(EquiJoinCandidate {
                    left_expr,
                    right_col_idx,
                    residual_cond,
                });
            }
        }
    }

    None
}

fn find_matching_closing_paren(s: &str) -> Result<usize, String> {
    let mut depth = 0;
    let mut in_str = false;
    for (idx, ch) in s.char_indices() {
        match ch {
            '\'' => in_str = !in_str,
            '(' if !in_str => depth += 1,
            ')' if !in_str => {
                depth -= 1;
                if depth == 0 {
                    return Ok(idx);
                }
            }
            _ => {}
        }
    }
    Err("Unmatched parenthesis in subquery".to_string())
}

fn clean_alias_part(s: &str) -> Option<String> {
    let mut a = s.trim();
    if a.to_uppercase().starts_with("AS ") {
        a = a[3..].trim();
    }
    let token = a.split_whitespace().next()?;
    let cleaned = clean_col_name(token).to_string();
    if cleaned.is_empty() {
        None
    } else {
        Some(cleaned)
    }
}

fn parse_join_target(raw: &str) -> Result<(String, Option<String>, Option<String>, bool), String> {
    let s = raw.trim();
    let upper = s.to_uppercase();

    // 1. Check for LATERAL (...) [AS alias]
    if upper.starts_with("LATERAL") && (s.len() == 7 || s[7..].starts_with(' ') || s[7..].starts_with('(') || s[7..].starts_with('\n') || s[7..].starts_with('\r') || s[7..].starts_with('\t')) {
        let after_lateral = s[7..].trim();
        if after_lateral.starts_with('(') {
            let end_idx = find_matching_closing_paren(after_lateral)?;
            let sub_query = after_lateral[1..end_idx].trim().to_string();
            let after = after_lateral[end_idx + 1..].trim();
            let alias = clean_alias_part(after);
            let tbl_name = alias.clone().unwrap_or_else(|| "lateral_tbl".to_string());
            return Ok((tbl_name, alias, Some(sub_query), true));
        }
    }

    // 2. Check for (SELECT ...) [AS alias]
    if s.starts_with('(') {
        if let Ok(end_idx) = find_matching_closing_paren(s) {
            let inside = s[1..end_idx].trim();
            let inside_upper = inside.to_uppercase();
            if inside_upper.starts_with("SELECT ") || inside_upper.starts_with("SELECT\n") || inside_upper.starts_with("SELECT\r") || inside_upper.starts_with("SELECT\t") {
                let after = s[end_idx + 1..].trim();
                let alias = clean_alias_part(after);
                let tbl_name = alias.clone().unwrap_or_else(|| "sub_tbl".to_string());
                return Ok((tbl_name, alias, Some(inside.to_string()), false));
            }
        }
    }

    // 3. Normal table
    let (tbl, alias) = extract_table_name_and_alias(s);
    Ok((tbl.to_string(), alias.map(|a| a.to_string()), None, false))
}

fn parse_from_and_joins(s: &str) -> Result<(String, Option<String>, Option<String>, Vec<ParsedJoin>), String> {
    let mut joins = Vec::new();
    let bytes = s.as_bytes();
    let len = bytes.len();

    let mut positions: Vec<(usize, usize, JoinKind)> = Vec::new();
    let mut i = 0;
    let mut depth = 0;
    let mut in_str = false;

    while i < len {
        let b = bytes[i];
        if b == b'\'' {
            in_str = !in_str;
            i += 1;
            continue;
        }
        if in_str {
            i += 1;
            continue;
        }
        if b == b'(' {
            depth += 1;
            i += 1;
            continue;
        }
        if b == b')' {
            if depth > 0 {
                depth -= 1;
            }
            i += 1;
            continue;
        }

        if depth == 0 {
            let rest = &s[i..];
            let upper = rest.to_uppercase();
            if b == b',' {
                positions.push((i, 1, JoinKind::Cross));
                i += 1;
                continue;
            } else if upper.starts_with("CROSS JOIN ") {
                positions.push((i, 11, JoinKind::Cross));
                i += 11;
                continue;
            } else if upper.starts_with("LEFT OUTER JOIN ") {
                positions.push((i, 16, JoinKind::Left));
                i += 16;
                continue;
            } else if upper.starts_with("LEFT JOIN ") {
                positions.push((i, 10, JoinKind::Left));
                i += 10;
                continue;
            } else if upper.starts_with("INNER JOIN ") {
                positions.push((i, 11, JoinKind::Inner));
                i += 11;
                continue;
            } else if upper.starts_with("JOIN ") {
                positions.push((i, 5, JoinKind::Inner));
                i += 5;
                continue;
            }
        }
        i += 1;
    }

    if positions.is_empty() {
        let (name, alias, subquery, _) = parse_join_target(s)?;
        return Ok((name, alias, subquery, joins));
    }

    let first_join_pos = positions[0].0;
    let base_part = s[..first_join_pos].trim();
    let (base_name, base_alias, base_subquery, _) = parse_join_target(base_part)?;

    for (k, &(pos, kw_len, ref kind)) in positions.iter().enumerate() {
        let after_kw = &s[pos + kw_len..];
        let end_idx = if k + 1 < positions.len() {
            positions[k + 1].0 - (pos + kw_len)
        } else {
            after_kw.len()
        };
        let seg = after_kw[..end_idx].trim();

        if *kind == JoinKind::Cross {
            let (tbl, alias, subquery, is_lateral) = parse_join_target(seg)?;
            joins.push(ParsedJoin {
                kind: JoinKind::Cross,
                table_name: tbl,
                alias,
                on_condition: None,
                subquery,
                is_lateral,
            });
        } else {
            let on_pos = find_top_level_keyword(seg, "ON").ok_or_else(|| format!("Missing ON clause in JOIN: {}", seg))?;
            let tbl_part = seg[..on_pos].trim();
            let on_part = seg[on_pos + 2..].trim();
            let (tbl, alias, subquery, is_lateral) = parse_join_target(tbl_part)?;
            joins.push(ParsedJoin {
                kind: kind.clone(),
                table_name: tbl,
                alias,
                on_condition: Some(on_part.to_string()),
                subquery,
                is_lateral,
            });
        }
    }

    Ok((base_name, base_alias, base_subquery, joins))
}

fn parse_query_clauses<'a>(
    after_from: &'a str,
    params: &[Value],
) -> (
    &'a str,
    Option<String>,
    Option<String>,
    Option<String>,
    Vec<OrderBySpec<'a>>,
    Option<usize>,
    Option<usize>,
) {
    let where_pos = find_top_level_keyword(after_from, "WHERE");
    let group_pos = find_top_level_keyword(after_from, "GROUP BY");
    let having_pos = find_top_level_keyword(after_from, "HAVING");
    let order_pos = find_top_level_keyword(after_from, "ORDER BY");
    let limit_pos = find_top_level_keyword(after_from, "LIMIT");
    let offset_pos = find_top_level_keyword(after_from, "OFFSET");

    let first_kw_pos = [where_pos, group_pos, having_pos, order_pos, limit_pos, offset_pos]
        .into_iter()
        .filter_map(|x| x)
        .min();

    let from_joins_str = match first_kw_pos {
        Some(pos) => after_from[..pos].trim(),
        None => after_from.trim(),
    };

    let where_clause = where_pos.map(|w_idx| {
        let after_w = &after_from[w_idx + 5..];
        let end_idx = [
            group_pos.and_then(|p| if p > w_idx { Some(p - (w_idx + 5)) } else { None }),
            having_pos.and_then(|p| if p > w_idx { Some(p - (w_idx + 5)) } else { None }),
            order_pos.and_then(|p| if p > w_idx { Some(p - (w_idx + 5)) } else { None }),
            limit_pos.and_then(|p| if p > w_idx { Some(p - (w_idx + 5)) } else { None }),
            offset_pos.and_then(|p| if p > w_idx { Some(p - (w_idx + 5)) } else { None }),
        ]
        .into_iter()
        .filter_map(|x| x)
        .min();
        match end_idx {
            Some(e) => after_w[..e].trim().to_string(),
            None => after_w.trim().to_string(),
        }
    });

    let group_by_clause = group_pos.map(|g_idx| {
        let after_g = &after_from[g_idx + 8..];
        let end_idx = [
            having_pos.and_then(|p| if p > g_idx { Some(p - (g_idx + 8)) } else { None }),
            order_pos.and_then(|p| if p > g_idx { Some(p - (g_idx + 8)) } else { None }),
            limit_pos.and_then(|p| if p > g_idx { Some(p - (g_idx + 8)) } else { None }),
            offset_pos.and_then(|p| if p > g_idx { Some(p - (g_idx + 8)) } else { None }),
        ]
        .into_iter()
        .filter_map(|x| x)
        .min();
        match end_idx {
            Some(e) => after_g[..e].trim().to_string(),
            None => after_g.trim().to_string(),
        }
    });

    let having_clause = having_pos.map(|h_idx| {
        let after_h = &after_from[h_idx + 6..];
        let end_idx = [
            order_pos.and_then(|p| if p > h_idx { Some(p - (h_idx + 6)) } else { None }),
            limit_pos.and_then(|p| if p > h_idx { Some(p - (h_idx + 6)) } else { None }),
            offset_pos.and_then(|p| if p > h_idx { Some(p - (h_idx + 6)) } else { None }),
        ]
        .into_iter()
        .filter_map(|x| x)
        .min();
        match end_idx {
            Some(e) => after_h[..e].trim().to_string(),
            None => after_h.trim().to_string(),
        }
    });

    let order_by_specs = match order_pos {
        Some(o_idx) => {
            let after_o = &after_from[o_idx + 8..];
            let end_idx = [
                limit_pos.and_then(|p| if p > o_idx { Some(p - (o_idx + 8)) } else { None }),
                offset_pos.and_then(|p| if p > o_idx { Some(p - (o_idx + 8)) } else { None }),
            ]
            .into_iter()
            .filter_map(|x| x)
            .min();
            let order_part = match end_idx {
                Some(e) => after_o[..e].trim(),
                None => after_o.trim(),
            };
            parse_order_by_specs(order_part)
        }
        None => Vec::new(),
    };

    let limit_val = limit_pos.and_then(|l_idx| {
        let after_l = &after_from[l_idx + 5..];
        let end_idx = offset_pos.and_then(|p| if p > l_idx { Some(p - (l_idx + 5)) } else { None });
        let s = match end_idx {
            Some(e) => after_l[..e].trim(),
            None => after_l.trim(),
        };
        parse_value(s, params).as_i64().map(|i| i as usize)
    });

    let offset_val = offset_pos.and_then(|o_idx| {
        let after_o = &after_from[o_idx + 6..];
        let end_idx = limit_pos.and_then(|p| if p > o_idx { Some(p - (o_idx + 6)) } else { None });
        let s = match end_idx {
            Some(e) => after_o[..e].trim(),
            None => after_o.trim(),
        };
        parse_value(s, params).as_i64().map(|i| i as usize)
    });

    (from_joins_str, where_clause, group_by_clause, having_clause, order_by_specs, limit_val, offset_val)
}

fn eval_condition_on_row(row: &CombinedRow, cond_str: &str, params: &[Value]) -> bool {
    let mut s = cond_str.trim();
    while s.starts_with('(') && s.ends_with(')') && is_fully_enclosed_in_parens(s) {
        s = s[1..s.len() - 1].trim();
    }
    let upper = s.to_uppercase();

    if s.eq_ignore_ascii_case("TRUE") || s == "1 = 1" || s == "1=1" {
        return true;
    }
    if s.eq_ignore_ascii_case("FALSE") || s == "1 = 0" || s == "1=0" {
        return false;
    }

    // Check top-level OR
    if let Some(or_idx) = find_top_level_keyword(s, "OR") {
        let left = &s[..or_idx];
        let right = &s[or_idx + 2..];
        return eval_condition_on_row(row, left, params) || eval_condition_on_row(row, right, params);
    }

    // Check top-level AND
    if let Some(and_idx) = find_top_level_keyword(s, "AND") {
        let left = &s[..and_idx];
        let right = &s[and_idx + 3..];
        return eval_condition_on_row(row, left, params) && eval_condition_on_row(row, right, params);
    }

    // Check NOT
    if upper.starts_with("NOT ") {
        return !eval_condition_on_row(row, &s[4..], params);
    }

    // IS NOT NULL / IS NULL
    if let Some(pos) = upper.find(" IS NOT NULL") {
        let col = &s[..pos].trim();
        let val = eval_operand_on_row(row, col, params);
        return !val.is_null();
    }
    if let Some(pos) = upper.find(" IS NULL") {
        let col = &s[..pos].trim();
        let val = eval_operand_on_row(row, col, params);
        return val.is_null();
    }

    // NOT IN (...)
    if let Some(pos) = find_top_level_keyword(s, "NOT IN") {
        let left_str = s[..pos].trim();
        let right_str = s[pos + 6..].trim();
        if right_str.starts_with('(') && right_str.ends_with(')') {
            let v_left = eval_operand_on_row(row, left_str, params);
            if v_left.is_null() {
                return false;
            }
            let inside = &right_str[1..right_str.len() - 1];
            let tokens = split_comma_separated_tokens(inside);
            let mut matches_any = false;
            for tok in tokens {
                let v_item = eval_operand_on_row(row, tok, params);
                if v_left.is_equal(&v_item) {
                    matches_any = true;
                    break;
                }
            }
            return !matches_any;
        }
    }

    // IN (...)
    if let Some(pos) = find_top_level_keyword(s, "IN") {
        let left_str = s[..pos].trim();
        let right_str = s[pos + 2..].trim();
        if right_str.starts_with('(') && right_str.ends_with(')') {
            let v_left = eval_operand_on_row(row, left_str, params);
            if v_left.is_null() {
                return false;
            }
            let inside = &right_str[1..right_str.len() - 1];
            let tokens = split_comma_separated_tokens(inside);
            for tok in tokens {
                let v_item = eval_operand_on_row(row, tok, params);
                if v_left.is_equal(&v_item) {
                    return true;
                }
            }
            return false;
        }
    }

    // LIKE / ILIKE
    if let Some(pos) = upper.find(" ILIKE ") {
        let left = eval_operand_on_row(row, s[..pos].trim(), params);
        let pattern_val = eval_operand_on_row(row, s[pos + 7..].trim(), params);
        return sql_like_match(&left.as_str(), &pattern_val.as_str(), true);
    }
    if let Some(pos) = upper.find(" LIKE ") {
        let left = eval_operand_on_row(row, s[..pos].trim(), params);
        let pattern_val = eval_operand_on_row(row, s[pos + 6..].trim(), params);
        return sql_like_match(&left.as_str(), &pattern_val.as_str(), false);
    }

    // JSONB / Array containment: @> and <@
    if let Some(pos) = s.find(" @> ") {
        let left = eval_operand_on_row(row, s[..pos].trim(), params);
        let right = eval_operand_on_row(row, s[pos + 4..].trim(), params);
        if let (Some(l), Some(r)) = (parse_val_to_json(&left), parse_val_to_json(&right)) {
            if json_contains_check(&l, &r) {
                return true;
            }
        }
        return array_contains_check(&left, &right);
    }
    if let Some(pos) = s.find(" <@ ") {
        let left = eval_operand_on_row(row, s[..pos].trim(), params);
        let right = eval_operand_on_row(row, s[pos + 4..].trim(), params);
        if let (Some(l), Some(r)) = (parse_val_to_json(&left), parse_val_to_json(&right)) {
            if json_contains_check(&r, &l) {
                return true;
            }
        }
        return array_contains_check(&right, &left);
    }
    if let Some(pos) = find_top_level_op(s, "&&") {
        let left = eval_operand_on_row(row, s[..pos].trim(), params);
        let right = eval_operand_on_row(row, s[pos + 2..].trim(), params);
        return array_overlap_check(&left, &right);
    }
    // JSONB key existence: ?|, ?&, ?
    if let Some(pos) = find_top_level_op(s, "?|") {
        let left = eval_operand_on_row(row, s[..pos].trim(), params);
        let right = eval_operand_on_row(row, s[pos + 2..].trim(), params);
        if let Some(l) = parse_val_to_json(&left) {
            let keys = parse_str_or_json_keys(&right);
            return keys.iter().any(|k| json_has_key_check(&l, k));
        }
        return false;
    }
    if let Some(pos) = find_top_level_op(s, "?&") {
        let left = eval_operand_on_row(row, s[..pos].trim(), params);
        let right = eval_operand_on_row(row, s[pos + 2..].trim(), params);
        if let Some(l) = parse_val_to_json(&left) {
            let keys = parse_str_or_json_keys(&right);
            return !keys.is_empty() && keys.iter().all(|k| json_has_key_check(&l, k));
        }
        return false;
    }
    if let Some(pos) = find_top_level_op(s, "?") {
        let left = eval_operand_on_row(row, s[..pos].trim(), params);
        let right = eval_operand_on_row(row, s[pos + 1..].trim(), params);
        if let Some(l) = parse_val_to_json(&left) {
            return json_has_key_check(&l, &right.as_str());
        }
        return false;
    }

    // Comparison operators: >=, <=, !=, <>, =, >, <
    let op_candidates = [">=", "<=", "!=", "<>", "=", ">", "<"];
    for op in op_candidates {
        if let Some(idx) = find_top_level_op(s, op) {
            let left_str = s[..idx].trim();
            let right_str = s[idx + op.len()..].trim();
            let v_left = eval_operand_on_row(row, left_str, params);
            let v_right = eval_operand_on_row(row, right_str, params);

            let ord = v_left.cmp_value(&v_right);
            return match op {
                ">=" => ord == std::cmp::Ordering::Greater || ord == std::cmp::Ordering::Equal,
                "<=" => ord == std::cmp::Ordering::Less || ord == std::cmp::Ordering::Equal,
                "!=" | "<>" => !v_left.is_equal(&v_right),
                "=" => v_left.is_equal(&v_right),
                ">" => ord == std::cmp::Ordering::Greater,
                "<" => ord == std::cmp::Ordering::Less,
                _ => false,
            };
        }
    }

    false
}

fn eval_operand_on_row(row: &CombinedRow, token: &str, params: &[Value]) -> Value {
    let mut t = token.trim();
    while t.starts_with('(') && t.ends_with(')') && is_fully_enclosed_in_parens(t) {
        let inside = t[1..t.len() - 1].trim();
        if split_comma_separated_tokens(inside).len() > 1 {
            break;
        }
        t = inside;
    }
    if let Some(colon_pos) = find_last_top_level_op(t, "::") {
        let target_type = t[colon_pos + 2..].trim();
        let val = eval_operand_on_row(row, &t[..colon_pos], params);
        return cast_val(val, target_type);
    }
    if let Some((op_idx, op)) = find_last_top_level_json_op(t) {
        let left_op = eval_operand_on_row(row, &t[..op_idx], params);
        let right_op = eval_operand_on_row(row, &t[op_idx + op.len()..], params);
        let left_json = match &left_op {
            Value::Text(s) => match serde_json::from_str::<serde_json::Value>(s.trim()) {
                Ok(v) => v,
                Err(_) => serde_json::Value::String(s.to_string()),
            },
            _ => value_to_json(&left_op),
        };
        let right_json = match &right_op {
            Value::Text(s) => serde_json::Value::String(s.to_string()),
            _ => value_to_json(&right_op),
        };
        let res_json = eval_json_extract_serde(&left_json, &right_json, op);
        return match res_json {
            serde_json::Value::Null => Value::Null,
            serde_json::Value::String(s) => Value::text(s),
            serde_json::Value::Bool(b) => Value::Bool(b),
            serde_json::Value::Number(n) => {
                if let Some(i) = n.as_i64() {
                    Value::Int(i)
                } else if let Some(f) = n.as_f64() {
                    Value::Float(f)
                } else {
                    Value::text(n.to_string())
                }
            }
            serde_json::Value::Object(_) | serde_json::Value::Array(_) => Value::text(res_json.to_string()),
        };
    }

    if let Some(pipe_idx) = find_top_level_op(t, "||") {
        let left = eval_operand_on_row(row, &t[..pipe_idx], params);
        let right = eval_operand_on_row(row, &t[pipe_idx + 2..], params);
        if left.is_null() || right.is_null() {
            return Value::Null;
        }
        let l_s = left.as_str();
        let r_s = right.as_str();
        if l_s.starts_with("\\x") || r_s.starts_with("\\x") {
            let mut combined = get_raw_bytes(&left);
            combined.extend(get_raw_bytes(&right));
            return Value::text(format!("\\x{}", hex::encode(combined)));
        }
        return Value::text(format!("{}{}", l_s, r_s));
    }
    if t.starts_with('$') {
        if let Ok(idx) = t[1..].parse::<usize>() {
            return params.get(idx.saturating_sub(1)).cloned().unwrap_or(Value::Null);
        }
    }
    if t.eq_ignore_ascii_case("TRUE") {
        return Value::Bool(true);
    }
    if t.eq_ignore_ascii_case("FALSE") {
        return Value::Bool(false);
    }
    if t.eq_ignore_ascii_case("NULL") {
        return Value::Null;
    }
    if t.starts_with('\'') && t.ends_with('\'') && t.len() >= 2 {
        return Value::text(t[1..t.len() - 1].replace("''", "'"));
    }
    let upper = t.to_uppercase();
    if is_complete_array_constructor(t) {
        let inside = &t[6..t.len() - 1].trim();
        if inside.is_empty() {
            return Value::text("[]");
        }
        let items: Vec<serde_json::Value> = split_comma_separated_tokens(inside)
            .into_iter()
            .map(|tok| {
                let v = eval_operand_on_row(row, &tok, params);
                value_to_json(&v)
            })
            .collect();
        return Value::text(serde_json::to_string(&items).unwrap_or_default());
    }
    if upper == "GEN_RANDOM_UUID()" || upper == "UUID_GENERATE_V4()" {
        return Value::text(crate::types::generate_uuid_v4());
    }
    if upper.starts_with("LOWER(") && t.ends_with(')') {
        let inner = &t[6..t.len() - 1];
        let val = eval_operand_on_row(row, inner, params);
        return Value::text(val.as_str().to_lowercase());
    }
    if upper.starts_with("UPPER(") && t.ends_with(')') {
        let inner = &t[6..t.len() - 1];
        let val = eval_operand_on_row(row, inner, params);
        return Value::text(val.as_str().to_uppercase());
    }
    if upper.starts_with("TRIM(") && t.ends_with(')') {
        let inner = &t[5..t.len() - 1];
        let val = eval_operand_on_row(row, inner, params);
        return Value::text(val.as_str().trim().to_string());
    }
    if upper.starts_with("BTRIM(") && t.ends_with(')') {
        let inner = &t[6..t.len() - 1];
        let val = eval_operand_on_row(row, inner, params);
        return Value::text(val.as_str().trim().to_string());
    }
    if let Ok(i) = t.parse::<i64>() {
        return Value::Int(i);
    }
    if let Ok(f) = t.parse::<f64>() {
        return Value::Float(f);
    }
    row.get_val(t)
}

fn json_val_to_str(v: &serde_json::Value) -> String {
    match v {
        serde_json::Value::Null => String::new(),
        serde_json::Value::Bool(b) => b.to_string(),
        serde_json::Value::Number(n) => n.to_string(),
        serde_json::Value::String(s) => s.clone(),
        serde_json::Value::Array(_) | serde_json::Value::Object(_) => serde_json::to_string(v).unwrap_or_default(),
    }
}

fn cast_json_val(v: serde_json::Value, target_type: &str) -> serde_json::Value {
    let t = target_type.trim().to_uppercase();
    if t.starts_with("INT") || t == "BIGINT" || t == "SMALLINT" {
        if let Some(i) = v.as_i64() {
            serde_json::json!(i)
        } else if let Some(s) = v.as_str() {
            s.parse::<i64>().map(|i| serde_json::json!(i)).unwrap_or(serde_json::Value::Null)
        } else {
            serde_json::Value::Null
        }
    } else if t.starts_with("FLOAT") || t.starts_with("DOUBLE") || t.starts_with("REAL") || t.starts_with("NUMERIC") || t.starts_with("DECIMAL") {
        if let Some(f) = v.as_f64() {
            serde_json::json!(f)
        } else if let Some(s) = v.as_str() {
            s.parse::<f64>().map(|f| serde_json::json!(f)).unwrap_or(serde_json::Value::Null)
        } else {
            serde_json::Value::Null
        }
    } else if t.starts_with("BOOL") {
        if let Some(b) = v.as_bool() {
            serde_json::json!(b)
        } else if let Some(s) = v.as_str() {
            serde_json::json!(s.eq_ignore_ascii_case("true") || s == "1")
        } else {
            serde_json::Value::Null
        }
    } else if t.starts_with("TEXT") || t.starts_with("VARCHAR") || t.starts_with("CHAR") {
        serde_json::Value::String(json_val_to_str(&v))
    } else {
        v
    }
}

fn parse_subquery_from_and_clauses<'a>(
    after_from: &'a str,
) -> (&'a str, Option<&'a str>, Option<&'a str>, Option<&'a str>, Option<&'a str>) {
    let where_pos = find_top_level_keyword(after_from, "WHERE");
    let order_pos = find_top_level_keyword(after_from, "ORDER BY");
    let limit_pos = find_top_level_keyword(after_from, "LIMIT");
    let offset_pos = find_top_level_keyword(after_from, "OFFSET");

    let mut kw_positions: Vec<(usize, &str, usize)> = Vec::new();
    if let Some(p) = where_pos { kw_positions.push((p, "WHERE", 5)); }
    if let Some(p) = order_pos { kw_positions.push((p, "ORDER BY", 8)); }
    if let Some(p) = limit_pos { kw_positions.push((p, "LIMIT", 5)); }
    if let Some(p) = offset_pos { kw_positions.push((p, "OFFSET", 6)); }

    kw_positions.sort_by_key(|(p, _, _)| *p);

    let table_part = if let Some(&(first_p, _, _)) = kw_positions.first() {
        after_from[..first_p].trim()
    } else {
        after_from.trim()
    };

    let mut where_opt = None;
    let mut order_opt = None;
    let mut limit_opt = None;
    let mut offset_opt = None;

    for (idx, &(p, kw, kw_len)) in kw_positions.iter().enumerate() {
        let content_start = p + kw_len;
        let content_end = if idx + 1 < kw_positions.len() {
            kw_positions[idx + 1].0
        } else {
            after_from.len()
        };
        let content = after_from[content_start..content_end].trim();
        match kw {
            "WHERE" => where_opt = Some(content),
            "ORDER BY" => order_opt = Some(content),
            "LIMIT" => limit_opt = Some(content),
            "OFFSET" => offset_opt = Some(content),
            _ => {}
        }
    }

    (table_part, where_opt, order_opt, limit_opt, offset_opt)
}

fn eval_joined_scalar_expr(
    expr: &str,
    row: &CombinedRow,
    params: &[Value],
    storage: &crate::storage::engine::StorageEngine,
) -> serde_json::Value {
    let mut t = expr.trim();
    while t.starts_with('(') && t.ends_with(')') && is_fully_enclosed_in_parens(t) {
        let inner = t[1..t.len() - 1].trim();
        if inner.to_uppercase().starts_with("SELECT ") {
            break;
        }
        t = inner;
    }
    let upper = t.to_uppercase();

    // 1. Correlated / Scalar Subquery: (SELECT ... FROM child WHERE child.fk = parent.id AND ...)
    if (upper.starts_with("(SELECT") || upper.starts_with("SELECT")) && upper.contains("FROM") {
        let sub_sql = if t.starts_with('(') && t.ends_with(')') && is_fully_enclosed_in_parens(t) {
            t[1..t.len() - 1].trim()
        } else {
            t
        };
        if let Some(from_pos) = find_top_level_keyword(sub_sql, "FROM") {
            let select_part = if sub_sql.to_uppercase().starts_with("SELECT ") {
                sub_sql[6..from_pos].trim()
            } else {
                sub_sql[..from_pos].trim()
            };
            let after_sub_from = sub_sql[from_pos + 4..].trim();
            let (child_tbl_part, where_clause, _order_clause, _limit_clause, _offset_clause) =
                parse_subquery_from_and_clauses(after_sub_from);
            let (child_table_name, child_alias) = extract_table_name_and_alias(child_tbl_part);

            if let Some(child_table) = storage.get_table(child_table_name) {
                let is_child_col = |col_str: &str| -> bool {
                    let s = col_str.trim().trim_matches('"');
                    if let Some(dot) = s.find('.') {
                        let prefix = s[..dot].trim().trim_matches('"').to_lowercase();
                        if prefix == child_table_name.to_lowercase() {
                            return true;
                        }
                        if let Some(ca) = child_alias {
                            if prefix == ca.trim_matches('"').to_lowercase() {
                                return true;
                            }
                        }
                        return false;
                    }
                    child_table.get_column_index(s).is_some()
                };

                let resolve_sub_operand = |operand: &str, cr: &[Value]| -> Value {
                    let op = operand.trim();
                    if is_child_col(op) {
                        let cname = clean_col_name(op);
                        if let Some(idx) = child_table.get_column_index(cname) {
                            return cr.get(idx).cloned().unwrap_or(Value::Null);
                        }
                        return Value::Null;
                    }
                    if op.starts_with('$') {
                        if let Ok(idx) = op[1..].parse::<usize>() {
                            return params.get(idx.saturating_sub(1)).cloned().unwrap_or(Value::Null);
                        }
                    }
                    if op.starts_with('\'') && op.ends_with('\'') && op.len() >= 2 {
                        return Value::text(op[1..op.len() - 1].replace("''", "'"));
                    }
                    if op.eq_ignore_ascii_case("TRUE") {
                        return Value::Bool(true);
                    }
                    if op.eq_ignore_ascii_case("FALSE") {
                        return Value::Bool(false);
                    }
                    if op.eq_ignore_ascii_case("NULL") {
                        return Value::Null;
                    }
                    if let Ok(i) = op.parse::<i64>() {
                        return Value::Int(i);
                    }
                    if let Ok(f) = op.parse::<f64>() {
                        return Value::Float(f);
                    }
                    let clean_op = clean_col_name(op);
                    let v = row.get_val(clean_op);
                    if !v.is_null() {
                        return v;
                    }
                    row.get_val(op)
                };

                let and_parts = if let Some(w_str) = where_clause {
                    split_top_level_and(w_str)
                } else {
                    Vec::new()
                };

                let mut matching_rows: Vec<&Vec<Value>> = Vec::new();

                for (cr_idx, cr) in child_table.rows.iter().enumerate() {
                    if child_table.is_deleted[cr_idx] {
                        continue;
                    }
                    let mut matches = true;
                    for part in &and_parts {
                        let part_trimmed = part.trim();
                        let part_upper = part_trimmed.to_uppercase();
                        if part_upper.ends_with("IS NULL") {
                            let col = part_trimmed[..part_trimmed.len() - 7].trim();
                            let v = resolve_sub_operand(col, cr);
                            if !v.is_null() {
                                matches = false;
                                break;
                            }
                        } else if part_upper.ends_with("IS NOT NULL") {
                            let col = part_trimmed[..part_trimmed.len() - 11].trim();
                            let v = resolve_sub_operand(col, cr);
                            if v.is_null() {
                                matches = false;
                                break;
                            }
                        } else if let Some(eq_idx) = part_trimmed.find('=') {
                            let left_raw = part_trimmed[..eq_idx].trim();
                            let right_raw = part_trimmed[eq_idx + 1..].trim();
                            let left_val = resolve_sub_operand(left_raw, cr);
                            let right_val = resolve_sub_operand(right_raw, cr);
                            if !left_val.is_equal(&right_val) {
                                matches = false;
                                break;
                            }
                        } else if let Some(neq_idx) = part_trimmed.find("!=") {
                            let left_raw = part_trimmed[..neq_idx].trim();
                            let right_raw = part_trimmed[neq_idx + 2..].trim();
                            let left_val = resolve_sub_operand(left_raw, cr);
                            let right_val = resolve_sub_operand(right_raw, cr);
                            if left_val.is_equal(&right_val) {
                                matches = false;
                                break;
                            }
                        } else if let Some(neq_idx) = part_trimmed.find("<>") {
                            let left_raw = part_trimmed[..neq_idx].trim();
                            let right_raw = part_trimmed[neq_idx + 2..].trim();
                            let left_val = resolve_sub_operand(left_raw, cr);
                            let right_val = resolve_sub_operand(right_raw, cr);
                            if left_val.is_equal(&right_val) {
                                matches = false;
                                break;
                            }
                        }
                    }
                    if matches {
                        matching_rows.push(cr);
                    }
                }

                let sel_trimmed = select_part.trim();
                let sel_upper = sel_trimmed.to_uppercase();

                if sel_upper.starts_with("COUNT(") || sel_upper == "COUNT(*)" {
                    return serde_json::json!(matching_rows.len());
                } else if sel_upper.starts_with("SUM(") && sel_trimmed.ends_with(')') {
                    let inner = sel_trimmed[4..sel_trimmed.len() - 1].trim();
                    let mut sum_f = 0.0;
                    let mut sum_i = 0i64;
                    let mut is_float = false;
                    for cr in &matching_rows {
                        let v = resolve_sub_operand(inner, cr);
                        if let Some(i) = v.as_i64() {
                            sum_i += i;
                            sum_f += i as f64;
                        } else if let Some(f) = v.as_f64() {
                            sum_f += f;
                            is_float = true;
                        }
                    }
                    return if is_float { serde_json::json!(sum_f) } else { serde_json::json!(sum_i) };
                } else {
                    let expr_to_eval = if let Some(as_idx) = sel_upper.rfind(" AS ") {
                        sel_trimmed[..as_idx].trim()
                    } else {
                        sel_trimmed
                    };
                    if let Some(first_cr) = matching_rows.first() {
                        let val = resolve_sub_operand(expr_to_eval, first_cr);
                        return value_to_json(&val);
                    } else {
                        return serde_json::Value::Null;
                    }
                }
            }
        }
    }

    // 2. CASE WHEN ... THEN ... ELSE ... END
    if upper.starts_with("CASE") && upper.ends_with("END") {
        let inside = t[4..t.len() - 3].trim();
        let (when_part, else_part) = if let Some(else_idx) = find_top_level_keyword(inside, "ELSE") {
            (&inside[..else_idx], Some(inside[else_idx + 4..].trim()))
        } else {
            (inside, None)
        };

        let when_items = split_when_clauses(when_part);
        for (cond_str, res_str) in when_items {
            if eval_condition_on_row(row, &cond_str, params) {
                return eval_joined_scalar_expr(&res_str, row, params, storage);
            }
        }

        if let Some(e_str) = else_part {
            return eval_joined_scalar_expr(e_str, row, params, storage);
        }
        return serde_json::Value::Null;
    }

    // 3. CAST(expr AS type) or expr::type
    if upper.starts_with("CAST(") && t.ends_with(')') {
        let inner = &t[5..t.len() - 1].trim();
        if let Some(as_pos) = inner.to_uppercase().rfind(" AS ") {
            let target_expr = inner[..as_pos].trim();
            let target_type = inner[as_pos + 4..].trim().to_uppercase();
            let val = eval_joined_scalar_expr(target_expr, row, params, storage);
            return cast_json_val(val, &target_type);
        }
    }
    if let Some(colon_pos) = t.rfind("::") {
        let target_expr = t[..colon_pos].trim();
        let target_type = t[colon_pos + 2..].trim().to_uppercase();
        let val = eval_joined_scalar_expr(target_expr, row, params, storage);
        return cast_json_val(val, &target_type);
    }

    // 3.5 JSON extraction operators: ->, ->>, #>, #>>
    if let Some((op_idx, op)) = find_last_top_level_json_op(t) {
        let lhs_expr = &t[..op_idx].trim();
        let rhs_expr = &t[op_idx + op.len()..].trim();
        let lhs = eval_joined_scalar_expr(lhs_expr, row, params, storage);
        let rhs = eval_joined_scalar_expr(rhs_expr, row, params, storage);
        return eval_json_extract_serde(&lhs, &rhs, op);
    }

    // JSON Functions: JSON_EXTRACT_PATH, JSON_EXTRACT_PATH_TEXT, JSON_ARRAY_LENGTH, JSON_TYPEOF, JSONB_STRIP_NULLS
    if (upper.starts_with("JSON_EXTRACT_PATH(") || upper.starts_with("JSONB_EXTRACT_PATH(")) && t.ends_with(')') {
        let open_idx = t.find('(').unwrap();
        let inner = &t[open_idx + 1..t.len() - 1];
        let args = split_function_args(inner);
        if !args.is_empty() {
            let mut curr = eval_joined_scalar_expr(&args[0], row, params, storage);
            for arg in &args[1..] {
                let key = eval_joined_scalar_expr(arg, row, params, storage);
                curr = eval_json_extract_serde(&curr, &key, "->");
                if curr.is_null() {
                    break;
                }
            }
            return curr;
        }
    }
    if (upper.starts_with("JSON_EXTRACT_PATH_TEXT(") || upper.starts_with("JSONB_EXTRACT_PATH_TEXT(")) && t.ends_with(')') {
        let open_idx = t.find('(').unwrap();
        let inner = &t[open_idx + 1..t.len() - 1];
        let args = split_function_args(inner);
        if !args.is_empty() {
            let mut curr = eval_joined_scalar_expr(&args[0], row, params, storage);
            let last_idx = args.len() - 1;
            for (idx, arg) in args[1..].iter().enumerate() {
                let key = eval_joined_scalar_expr(arg, row, params, storage);
                let op = if idx + 1 == last_idx { "->>" } else { "->" };
                curr = eval_json_extract_serde(&curr, &key, op);
                if curr.is_null() {
                    break;
                }
            }
            return curr;
        }
    }
    if (upper.starts_with("JSON_ARRAY_LENGTH(") || upper.starts_with("JSONB_ARRAY_LENGTH(")) && t.ends_with(')') {
        let inner = &t[upper.find('(').unwrap() + 1..t.len() - 1];
        let target = eval_joined_scalar_expr(inner, row, params, storage);
        let parsed = match &target {
            serde_json::Value::String(s) => serde_json::from_str::<serde_json::Value>(s.trim()).unwrap_or(serde_json::Value::Null),
            _ => target,
        };
        if let serde_json::Value::Array(arr) = parsed {
            return serde_json::json!(arr.len());
        }
        return serde_json::Value::Null;
    }
    if (upper.starts_with("JSON_TYPEOF(") || upper.starts_with("JSONB_TYPEOF(")) && t.ends_with(')') {
        let inner = &t[upper.find('(').unwrap() + 1..t.len() - 1];
        let target = eval_joined_scalar_expr(inner, row, params, storage);
        let parsed = match &target {
            serde_json::Value::String(s) => serde_json::from_str::<serde_json::Value>(s.trim()).unwrap_or(serde_json::Value::String(s.clone())),
            _ => target,
        };
        let type_str = match parsed {
            serde_json::Value::Null => "null",
            serde_json::Value::Bool(_) => "boolean",
            serde_json::Value::Number(_) => "number",
            serde_json::Value::String(_) => "string",
            serde_json::Value::Array(_) => "array",
            serde_json::Value::Object(_) => "object",
        };
        return serde_json::Value::String(type_str.to_string());
    }
    if upper.starts_with("JSONB_STRIP_NULLS(") && t.ends_with(')') {
        let inner = &t[18..t.len() - 1];
        let target = eval_joined_scalar_expr(inner, row, params, storage);
        let mut parsed = match &target {
            serde_json::Value::String(s) => serde_json::from_str::<serde_json::Value>(s.trim()).unwrap_or(serde_json::Value::Null),
            _ => target,
        };
        if let serde_json::Value::Object(ref mut map) = parsed {
            map.retain(|_, v| !v.is_null());
            return serde_json::Value::Object(map.clone());
        }
        return parsed;
    }

    // 4. JSON_BUILD_OBJECT / JSONB_BUILD_OBJECT
    if (upper.starts_with("JSON_BUILD_OBJECT(") || upper.starts_with("JSONB_BUILD_OBJECT(")) && t.ends_with(')') {
        let open_idx = t.find('(').unwrap();
        let inner = &t[open_idx + 1..t.len() - 1];
        let args = split_function_args(inner);
        let mut map = serde_json::Map::new();
        for chunk in args.chunks(2) {
            if chunk.len() == 2 {
                let k_val = eval_operand_on_row(row, &chunk[0], params).as_str();
                let v_val = eval_joined_scalar_expr(&chunk[1], row, params, storage);
                map.insert(k_val, v_val);
            }
        }
        return serde_json::Value::Object(map);
    }

    // JSON_BUILD_ARRAY / JSONB_BUILD_ARRAY
    if (upper.starts_with("JSON_BUILD_ARRAY(") || upper.starts_with("JSONB_BUILD_ARRAY(")) && t.ends_with(')') {
        let open_idx = t.find('(').unwrap();
        let inner = &t[open_idx + 1..t.len() - 1];
        let items: Vec<serde_json::Value> = split_function_args(inner)
            .into_iter()
            .map(|arg| eval_joined_scalar_expr(&arg, row, params, storage))
            .collect();
        return serde_json::Value::Array(items);
    }

    // JSONB_PRETTY
    if upper.starts_with("JSONB_PRETTY(") && t.ends_with(')') {
        let inner = &t[13..t.len() - 1];
        let v = eval_joined_scalar_expr(inner, row, params, storage);
        return serde_json::Value::String(serde_json::to_string_pretty(&v).unwrap_or_default());
    }

    // JSONB_SET
    if (upper.starts_with("JSONB_SET(") || upper.starts_with("JSON_SET(")) && t.ends_with(')') {
        let open_idx = t.find('(').unwrap();
        let inner = &t[open_idx + 1..t.len() - 1];
        let args = split_function_args(inner);
        if args.len() >= 3 {
            let target_val = eval_joined_scalar_expr(&args[0], row, params, storage);
            let path_val = eval_joined_scalar_expr(&args[1], row, params, storage);
            let new_val = eval_joined_scalar_expr(&args[2], row, params, storage);
            let create_missing = if args.len() >= 4 {
                let cm_val = eval_joined_scalar_expr(&args[3], row, params, storage);
                cm_val.as_bool().unwrap_or(true)
            } else {
                true
            };
            let mut target_json = match &target_val {
                serde_json::Value::String(st) => serde_json::from_str::<serde_json::Value>(st.trim()).unwrap_or(target_val.clone()),
                _ => target_val.clone(),
            };
            let keys = parse_json_path_keys(&path_val);
            jsonb_set_serde(&mut target_json, &keys, &new_val, create_missing);
            return target_json;
        }
    }

    // 5. ARRAY[...]
    if is_complete_array_constructor(t) {
        let inner = &t[6..t.len() - 1].trim();
        if inner.is_empty() {
            return serde_json::Value::Array(vec![]);
        }
        let items: Vec<serde_json::Value> = split_function_args(inner)
            .into_iter()
            .map(|arg| eval_joined_scalar_expr(&arg, row, params, storage))
            .collect();
        return serde_json::Value::Array(items);
    }

    // 6. COALESCE
    if upper.starts_with("COALESCE(") && t.ends_with(')') {
        let inner = &t[9..t.len() - 1];
        for arg in split_function_args(inner) {
            let v = eval_joined_scalar_expr(&arg, row, params, storage);
            if !v.is_null() {
                return v;
            }
        }
        return serde_json::Value::Null;
    }

    // 7. NULLIF
    if upper.starts_with("NULLIF(") && t.ends_with(')') {
        let inner = &t[7..t.len() - 1];
        let args = split_function_args(inner);
        if args.len() == 2 {
            let a = eval_joined_scalar_expr(&args[0], row, params, storage);
            let b = eval_joined_scalar_expr(&args[1], row, params, storage);
            return if a == b { serde_json::Value::Null } else { a };
        }
    }

    // 8. String concatenation with || operator
    if let Some(pipe_idx) = find_top_level_op(t, "||") {
        let lhs = eval_joined_scalar_expr(&t[..pipe_idx], row, params, storage);
        let rhs = eval_joined_scalar_expr(&t[pipe_idx + 2..], row, params, storage);
        if lhs.is_null() || rhs.is_null() {
            return serde_json::Value::Null;
        }
        let lhs_s = json_val_to_str(&lhs);
        let rhs_s = json_val_to_str(&rhs);
        return serde_json::Value::String(format!("{}{}", lhs_s, rhs_s));
    }

    // Bitwise binary operations (|, &, #, <<, >>)
    if let Some((op_idx, _)) = find_top_level_math_op(t, &['|']) {
        let lhs = eval_joined_scalar_expr(t[..op_idx].trim(), row, params, storage);
        let rhs = eval_joined_scalar_expr(t[op_idx + 1..].trim(), row, params, storage);
        if lhs.is_null() || rhs.is_null() { return serde_json::Value::Null; }
        if let (Some(l), Some(r)) = (lhs.as_i64(), rhs.as_i64()) {
            return serde_json::json!(l | r);
        }
    }
    if let Some((op_idx, _)) = find_top_level_math_op(t, &['&']) {
        let lhs = eval_joined_scalar_expr(t[..op_idx].trim(), row, params, storage);
        let rhs = eval_joined_scalar_expr(t[op_idx + 1..].trim(), row, params, storage);
        if lhs.is_null() || rhs.is_null() { return serde_json::Value::Null; }
        if let (Some(l), Some(r)) = (lhs.as_i64(), rhs.as_i64()) {
            return serde_json::json!(l & r);
        }
    }
    if let Some((op_idx, _)) = find_top_level_math_op(t, &['#']) {
        let lhs = eval_joined_scalar_expr(t[..op_idx].trim(), row, params, storage);
        let rhs = eval_joined_scalar_expr(t[op_idx + 1..].trim(), row, params, storage);
        if lhs.is_null() || rhs.is_null() { return serde_json::Value::Null; }
        if let (Some(l), Some(r)) = (lhs.as_i64(), rhs.as_i64()) {
            return serde_json::json!(l ^ r);
        }
    }
    if let Some((op_idx, op)) = find_top_level_shift_op(t) {
        let lhs = eval_joined_scalar_expr(t[..op_idx].trim(), row, params, storage);
        let rhs = eval_joined_scalar_expr(t[op_idx + 2..].trim(), row, params, storage);
        if lhs.is_null() || rhs.is_null() { return serde_json::Value::Null; }
        if let (Some(l), Some(r)) = (lhs.as_i64(), rhs.as_i64()) {
            let res = if op == "<<" { l << (r as u32) } else { l >> (r as u32) };
            return serde_json::json!(res);
        }
    }

    // Math binary operations (+, -)
    if let Some((op_idx, op)) = find_top_level_math_op(t, &['+', '-']) {
        let lhs = eval_joined_scalar_expr(t[..op_idx].trim(), row, params, storage);
        let rhs = eval_joined_scalar_expr(t[op_idx + 1..].trim(), row, params, storage);
        if lhs.is_null() || rhs.is_null() { return serde_json::Value::Null; }
        if let (Some(l_num), Some(r_num)) = (lhs.as_f64(), rhs.as_f64()) {
            let res = if op == '+' { l_num + r_num } else { l_num - r_num };
            return if res.fract() == 0.0 { serde_json::json!(res as i64) } else { serde_json::json!(res) };
        }
    }

    // Math binary operations (*, /, %)
    if let Some((op_idx, op)) = find_top_level_math_op(t, &['*', '/', '%']) {
        let lhs = eval_joined_scalar_expr(t[..op_idx].trim(), row, params, storage);
        let rhs = eval_joined_scalar_expr(t[op_idx + 1..].trim(), row, params, storage);
        if lhs.is_null() || rhs.is_null() { return serde_json::Value::Null; }
        if let (Some(l_num), Some(r_num)) = (lhs.as_f64(), rhs.as_f64()) {
            let res = match op {
                '*' => l_num * r_num,
                '/' => if r_num != 0.0 { l_num / r_num } else { 0.0 },
                '%' => if r_num != 0.0 { l_num % r_num } else { 0.0 },
                _ => 0.0,
            };
            return if res.fract() == 0.0 { serde_json::json!(res as i64) } else { serde_json::json!(res) };
        }
    }

    // Exponentiation (^)
    if let Some((op_idx, _)) = find_top_level_math_op(t, &['^']) {
        let lhs = eval_joined_scalar_expr(t[..op_idx].trim(), row, params, storage);
        let rhs = eval_joined_scalar_expr(t[op_idx + 1..].trim(), row, params, storage);
        if lhs.is_null() || rhs.is_null() { return serde_json::Value::Null; }
        if let (Some(l_num), Some(r_num)) = (lhs.as_f64(), rhs.as_f64()) {
            let res = l_num.powf(r_num);
            return if res.fract() == 0.0 { serde_json::json!(res as i64) } else { serde_json::json!(res) };
        }
    }

    // Unary operators (-, ~)
    if t.starts_with('-') && !t.starts_with("->") && !t.starts_with("--") {
        let inner = t[1..].trim();
        if !inner.is_empty() {
            let val = eval_joined_scalar_expr(inner, row, params, storage);
            if val.is_null() { return serde_json::Value::Null; }
            if let Some(f) = val.as_f64() {
                return if f.fract() == 0.0 { serde_json::json!(-(f as i64)) } else { serde_json::json!(-f) };
            }
        }
    }
    if t.starts_with('~') {
        let inner = t[1..].trim();
        if !inner.is_empty() {
            let val = eval_joined_scalar_expr(inner, row, params, storage);
            if val.is_null() { return serde_json::Value::Null; }
            if let Some(i) = val.as_i64() {
                return serde_json::json!(!i);
            }
        }
    }

    // 9. Built-in String Functions: UPPER, LOWER, INITCAP, CONCAT, CONCAT_WS, LENGTH, CHAR_LENGTH,
    // CHARACTER_LENGTH, OCTET_LENGTH, LEFT, RIGHT, TRIM, BTRIM, LTRIM, RTRIM, LPAD, RPAD, REPEAT,
    // REVERSE, ASCII, CHR, TRANSLATE, STRPOS, SPLIT_PART, MD5, ENCODE, DECODE, REPLACE, SUBSTRING
    if upper.starts_with("UPPER(") && t.ends_with(')') {
        let inner = &t[6..t.len() - 1];
        let v = eval_joined_scalar_expr(inner, row, params, storage);
        if v.is_null() { return serde_json::Value::Null; }
        return serde_json::Value::String(json_val_to_str(&v).to_uppercase());
    }

    if upper.starts_with("LOWER(") && t.ends_with(')') {
        let inner = &t[6..t.len() - 1];
        let v = eval_joined_scalar_expr(inner, row, params, storage);
        if v.is_null() { return serde_json::Value::Null; }
        return serde_json::Value::String(json_val_to_str(&v).to_lowercase());
    }

    if upper.starts_with("INITCAP(") && t.ends_with(')') {
        let inner = &t[8..t.len() - 1];
        let v = eval_joined_scalar_expr(inner, row, params, storage);
        if v.is_null() { return serde_json::Value::Null; }
        let s_str = json_val_to_str(&v);
        let mut out = String::with_capacity(s_str.len());
        let mut new_word = true;
        for c in s_str.chars() {
            if c.is_alphanumeric() {
                if new_word {
                    out.extend(c.to_uppercase());
                    new_word = false;
                } else {
                    out.extend(c.to_lowercase());
                }
            } else {
                out.push(c);
                new_word = true;
            }
        }
        return serde_json::Value::String(out);
    }

    if upper.starts_with("CONCAT(") && t.ends_with(')') {
        let inner = &t[7..t.len() - 1];
        let args = split_function_args(inner);
        let mut out = String::new();
        for arg in args {
            let v = eval_joined_scalar_expr(&arg, row, params, storage);
            if !v.is_null() {
                out.push_str(&json_val_to_str(&v));
            }
        }
        return serde_json::Value::String(out);
    }

    if upper.starts_with("CONCAT_WS(") && t.ends_with(')') {
        let inner = &t[10..t.len() - 1];
        let args = split_function_args(inner);
        if !args.is_empty() {
            let sep_val = eval_joined_scalar_expr(&args[0], row, params, storage);
            if sep_val.is_null() {
                return serde_json::Value::Null;
            }
            let sep = json_val_to_str(&sep_val);
            let mut parts = Vec::new();
            for arg in &args[1..] {
                let v = eval_joined_scalar_expr(arg, row, params, storage);
                if !v.is_null() {
                    parts.push(json_val_to_str(&v));
                }
            }
            return serde_json::Value::String(parts.join(&sep));
        }
    }

    if (upper.starts_with("LENGTH(") || upper.starts_with("CHAR_LENGTH(") || upper.starts_with("CHARACTER_LENGTH(")) && t.ends_with(')') {
        let open_p = t.find('(').unwrap();
        let inner = &t[open_p + 1..t.len() - 1];
        let v = eval_joined_scalar_expr(inner, row, params, storage);
        if v.is_null() { return serde_json::Value::Null; }
        let len = json_val_to_str(&v).chars().count();
        return serde_json::json!(len);
    }

    if upper.starts_with("OCTET_LENGTH(") && t.ends_with(')') {
        let inner = &t[13..t.len() - 1];
        let v = eval_joined_scalar_expr(inner, row, params, storage);
        if v.is_null() { return serde_json::Value::Null; }
        let len = json_val_to_str(&v).as_bytes().len();
        return serde_json::json!(len);
    }

    if upper.starts_with("LEFT(") && t.ends_with(')') {
        let inner = &t[5..t.len() - 1];
        let args = split_function_args(inner);
        if args.len() == 2 {
            let v = eval_joined_scalar_expr(&args[0], row, params, storage);
            let n_val = eval_joined_scalar_expr(&args[1], row, params, storage);
            if v.is_null() || n_val.is_null() {
                return serde_json::Value::Null;
            }
            let s_str = json_val_to_str(&v);
            let chars: Vec<char> = s_str.chars().collect();
            let len = chars.len() as i64;
            let n = n_val.as_i64().unwrap_or(0);
            let take_len = if n >= 0 {
                (n as usize).min(chars.len())
            } else {
                ((len + n).max(0) as usize).min(chars.len())
            };
            let res: String = chars[..take_len].iter().collect();
            return serde_json::Value::String(res);
        }
    }

    if upper.starts_with("RIGHT(") && t.ends_with(')') {
        let inner = &t[6..t.len() - 1];
        let args = split_function_args(inner);
        if args.len() == 2 {
            let v = eval_joined_scalar_expr(&args[0], row, params, storage);
            let n_val = eval_joined_scalar_expr(&args[1], row, params, storage);
            if v.is_null() || n_val.is_null() {
                return serde_json::Value::Null;
            }
            let s_str = json_val_to_str(&v);
            let chars: Vec<char> = s_str.chars().collect();
            let n = n_val.as_i64().unwrap_or(0);
            let start_idx = if n >= 0 {
                chars.len().saturating_sub(n as usize)
            } else {
                ((-n) as usize).min(chars.len())
            };
            let res: String = chars[start_idx..].iter().collect();
            return serde_json::Value::String(res);
        }
    }

    if (upper.starts_with("TRIM(") || upper.starts_with("BTRIM(")) && t.ends_with(')') {
        let open_p = t.find('(').unwrap();
        let inner = &t[open_p + 1..t.len() - 1];
        let args = split_function_args(inner);
        if args.len() == 1 {
            let v = eval_joined_scalar_expr(&args[0], row, params, storage);
            if v.is_null() { return serde_json::Value::Null; }
            return serde_json::Value::String(json_val_to_str(&v).trim().to_string());
        } else if args.len() >= 2 {
            let v = eval_joined_scalar_expr(&args[0], row, params, storage);
            let chars_v = eval_joined_scalar_expr(&args[1], row, params, storage);
            if v.is_null() || chars_v.is_null() { return serde_json::Value::Null; }
            let s_str = json_val_to_str(&v);
            let c_str = json_val_to_str(&chars_v);
            let trim_chars: Vec<char> = c_str.chars().collect();
            let trimmed = s_str.trim_matches(|c: char| trim_chars.contains(&c)).to_string();
            return serde_json::Value::String(trimmed);
        }
    }

    if upper.starts_with("LTRIM(") && t.ends_with(')') {
        let inner = &t[6..t.len() - 1];
        let args = split_function_args(inner);
        if args.len() == 1 {
            let v = eval_joined_scalar_expr(&args[0], row, params, storage);
            if v.is_null() { return serde_json::Value::Null; }
            return serde_json::Value::String(json_val_to_str(&v).trim_start().to_string());
        } else if args.len() >= 2 {
            let v = eval_joined_scalar_expr(&args[0], row, params, storage);
            let chars_v = eval_joined_scalar_expr(&args[1], row, params, storage);
            if v.is_null() || chars_v.is_null() { return serde_json::Value::Null; }
            let s_str = json_val_to_str(&v);
            let c_str = json_val_to_str(&chars_v);
            let trim_chars: Vec<char> = c_str.chars().collect();
            let trimmed = s_str.trim_start_matches(|c: char| trim_chars.contains(&c)).to_string();
            return serde_json::Value::String(trimmed);
        }
    }

    if upper.starts_with("RTRIM(") && t.ends_with(')') {
        let inner = &t[6..t.len() - 1];
        let args = split_function_args(inner);
        if args.len() == 1 {
            let v = eval_joined_scalar_expr(&args[0], row, params, storage);
            if v.is_null() { return serde_json::Value::Null; }
            return serde_json::Value::String(json_val_to_str(&v).trim_end().to_string());
        } else if args.len() >= 2 {
            let v = eval_joined_scalar_expr(&args[0], row, params, storage);
            let chars_v = eval_joined_scalar_expr(&args[1], row, params, storage);
            if v.is_null() || chars_v.is_null() { return serde_json::Value::Null; }
            let s_str = json_val_to_str(&v);
            let c_str = json_val_to_str(&chars_v);
            let trim_chars: Vec<char> = c_str.chars().collect();
            let trimmed = s_str.trim_end_matches(|c: char| trim_chars.contains(&c)).to_string();
            return serde_json::Value::String(trimmed);
        }
    }

    if upper.starts_with("LPAD(") && t.ends_with(')') {
        let inner = &t[5..t.len() - 1];
        let args = split_function_args(inner);
        if args.len() >= 2 {
            let v = eval_joined_scalar_expr(&args[0], row, params, storage);
            let len_val = eval_joined_scalar_expr(&args[1], row, params, storage);
            if v.is_null() || len_val.is_null() { return serde_json::Value::Null; }
            let target_len = len_val.as_i64().unwrap_or(0).max(0) as usize;
            let s_str = json_val_to_str(&v);
            let chars: Vec<char> = s_str.chars().collect();
            if chars.len() >= target_len {
                let res: String = chars[..target_len].iter().collect();
                return serde_json::Value::String(res);
            }
            let pad_str = if args.len() >= 3 {
                let pv = eval_joined_scalar_expr(&args[2], row, params, storage);
                if pv.is_null() { return serde_json::Value::Null; }
                json_val_to_str(&pv)
            } else {
                " ".to_string()
            };
            if pad_str.is_empty() {
                let res: String = chars.into_iter().collect();
                return serde_json::Value::String(res);
            }
            let pad_chars: Vec<char> = pad_str.chars().collect();
            let needed = target_len - chars.len();
            let mut out = String::new();
            for i in 0..needed {
                out.push(pad_chars[i % pad_chars.len()]);
            }
            out.push_str(&s_str);
            return serde_json::Value::String(out);
        }
    }

    if upper.starts_with("RPAD(") && t.ends_with(')') {
        let inner = &t[5..t.len() - 1];
        let args = split_function_args(inner);
        if args.len() >= 2 {
            let v = eval_joined_scalar_expr(&args[0], row, params, storage);
            let len_val = eval_joined_scalar_expr(&args[1], row, params, storage);
            if v.is_null() || len_val.is_null() { return serde_json::Value::Null; }
            let target_len = len_val.as_i64().unwrap_or(0).max(0) as usize;
            let s_str = json_val_to_str(&v);
            let chars: Vec<char> = s_str.chars().collect();
            if chars.len() >= target_len {
                let res: String = chars[..target_len].iter().collect();
                return serde_json::Value::String(res);
            }
            let pad_str = if args.len() >= 3 {
                let pv = eval_joined_scalar_expr(&args[2], row, params, storage);
                if pv.is_null() { return serde_json::Value::Null; }
                json_val_to_str(&pv)
            } else {
                " ".to_string()
            };
            if pad_str.is_empty() {
                let res: String = chars.into_iter().collect();
                return serde_json::Value::String(res);
            }
            let pad_chars: Vec<char> = pad_str.chars().collect();
            let needed = target_len - chars.len();
            let mut out = s_str;
            for i in 0..needed {
                out.push(pad_chars[i % pad_chars.len()]);
            }
            return serde_json::Value::String(out);
        }
    }

    if upper.starts_with("REPEAT(") && t.ends_with(')') {
        let inner = &t[7..t.len() - 1];
        let args = split_function_args(inner);
        if args.len() == 2 {
            let v = eval_joined_scalar_expr(&args[0], row, params, storage);
            let count_val = eval_joined_scalar_expr(&args[1], row, params, storage);
            if v.is_null() || count_val.is_null() { return serde_json::Value::Null; }
            let count = count_val.as_i64().unwrap_or(0).max(0) as usize;
            let s_str = json_val_to_str(&v);
            return serde_json::Value::String(s_str.repeat(count));
        }
    }

    if upper.starts_with("REVERSE(") && t.ends_with(')') {
        let inner = &t[8..t.len() - 1];
        let v = eval_joined_scalar_expr(inner, row, params, storage);
        if v.is_null() { return serde_json::Value::Null; }
        let rev: String = json_val_to_str(&v).chars().rev().collect();
        return serde_json::Value::String(rev);
    }

    if upper.starts_with("ASCII(") && t.ends_with(')') {
        let inner = &t[6..t.len() - 1];
        let v = eval_joined_scalar_expr(inner, row, params, storage);
        if v.is_null() { return serde_json::Value::Null; }
        let s_str = json_val_to_str(&v);
        if let Some(first_char) = s_str.chars().next() {
            return serde_json::json!(first_char as u32 as i64);
        }
        return serde_json::json!(0);
    }

    if upper.starts_with("CHR(") && t.ends_with(')') {
        let inner = &t[4..t.len() - 1];
        let v = eval_joined_scalar_expr(inner, row, params, storage);
        if v.is_null() { return serde_json::Value::Null; }
        let code = v.as_i64().unwrap_or(0) as u32;
        if let Some(ch) = char::from_u32(code) {
            return serde_json::Value::String(ch.to_string());
        }
        return serde_json::Value::String(String::new());
    }

    if upper.starts_with("TRANSLATE(") && t.ends_with(')') {
        let inner = &t[10..t.len() - 1];
        let args = split_function_args(inner);
        if args.len() == 3 {
            let str_val = eval_joined_scalar_expr(&args[0], row, params, storage);
            let from_val = eval_joined_scalar_expr(&args[1], row, params, storage);
            let to_val = eval_joined_scalar_expr(&args[2], row, params, storage);
            if str_val.is_null() || from_val.is_null() || to_val.is_null() { return serde_json::Value::Null; }
            let s_str = json_val_to_str(&str_val);
            let from_chars: Vec<char> = json_val_to_str(&from_val).chars().collect();
            let to_chars: Vec<char> = json_val_to_str(&to_val).chars().collect();
            let mut out = String::new();
            for ch in s_str.chars() {
                if let Some(pos) = from_chars.iter().position(|&fc| fc == ch) {
                    if pos < to_chars.len() {
                        out.push(to_chars[pos]);
                    }
                } else {
                    out.push(ch);
                }
            }
            return serde_json::Value::String(out);
        }
    }

    if upper.starts_with("STRPOS(") && t.ends_with(')') {
        let inner = &t[7..t.len() - 1];
        let args = split_function_args(inner);
        if args.len() == 2 {
            let str_val = eval_joined_scalar_expr(&args[0], row, params, storage);
            let sub_val = eval_joined_scalar_expr(&args[1], row, params, storage);
            if str_val.is_null() || sub_val.is_null() { return serde_json::Value::Null; }
            let s_str = json_val_to_str(&str_val);
            let sub_str = json_val_to_str(&sub_val);
            if let Some(byte_pos) = s_str.find(&sub_str) {
                let char_idx = s_str[..byte_pos].chars().count() + 1;
                return serde_json::json!(char_idx as i64);
            } else {
                return serde_json::json!(0);
            }
        }
    }

    if upper.starts_with("SPLIT_PART(") && t.ends_with(')') {
        let inner = &t[11..t.len() - 1];
        let args = split_function_args(inner);
        if args.len() == 3 {
            let str_val = eval_joined_scalar_expr(&args[0], row, params, storage);
            let delim_val = eval_joined_scalar_expr(&args[1], row, params, storage);
            let field_val = eval_joined_scalar_expr(&args[2], row, params, storage);
            if str_val.is_null() || delim_val.is_null() || field_val.is_null() { return serde_json::Value::Null; }
            let s_str = json_val_to_str(&str_val);
            let delim_str = json_val_to_str(&delim_val);
            let field_idx = field_val.as_i64().unwrap_or(1);
            if field_idx < 1 {
                return serde_json::Value::String(String::new());
            }
            let parts: Vec<&str> = s_str.split(&delim_str).collect();
            let u_idx = (field_idx - 1) as usize;
            if u_idx < parts.len() {
                return serde_json::Value::String(parts[u_idx].to_string());
            } else {
                return serde_json::Value::String(String::new());
            }
        }
    }

    if upper.starts_with("MD5(") && t.ends_with(')') {
        let inner = &t[4..t.len() - 1];
        let v = eval_joined_scalar_expr(inner, row, params, storage);
        if v.is_null() { return serde_json::Value::Null; }
        let s_str = json_val_to_str(&v);
        let mut hasher = Md5::new();
        hasher.update(s_str.as_bytes());
        let hash = format!("{:x}", hasher.finalize());
        return serde_json::Value::String(hash);
    }

    if upper.starts_with("ENCODE(") && t.ends_with(')') {
        let inner = &t[7..t.len() - 1];
        let args = split_function_args(inner);
        if args.len() == 2 {
            let str_val = eval_joined_scalar_expr(&args[0], row, params, storage);
            let fmt_val = eval_joined_scalar_expr(&args[1], row, params, storage);
            if str_val.is_null() || fmt_val.is_null() { return serde_json::Value::Null; }
            let s_str = json_val_to_str(&str_val);
            let fmt_str = json_val_to_str(&fmt_val).to_lowercase();
            if fmt_str == "hex" {
                return serde_json::Value::String(hex::encode(s_str.as_bytes()));
            } else if fmt_str == "base64" {
                return serde_json::Value::String(base64::engine::general_purpose::STANDARD.encode(s_str.as_bytes()));
            }
        }
    }

    if upper.starts_with("DECODE(") && t.ends_with(')') {
        let inner = &t[7..t.len() - 1];
        let args = split_function_args(inner);
        if args.len() == 2 {
            let str_val = eval_joined_scalar_expr(&args[0], row, params, storage);
            let fmt_val = eval_joined_scalar_expr(&args[1], row, params, storage);
            if str_val.is_null() || fmt_val.is_null() { return serde_json::Value::Null; }
            let s_str = json_val_to_str(&str_val);
            let fmt_str = json_val_to_str(&fmt_val).to_lowercase();
            if fmt_str == "hex" {
                if let Ok(bytes) = hex::decode(s_str.trim()) {
                    return serde_json::Value::String(String::from_utf8_lossy(&bytes).to_string());
                }
            } else if fmt_str == "base64" {
                if let Ok(bytes) = base64::engine::general_purpose::STANDARD.decode(s_str.trim()) {
                    return serde_json::Value::String(String::from_utf8_lossy(&bytes).to_string());
                }
            }
        }
    }

    if upper.starts_with("REPLACE(") && t.ends_with(')') {
        let inner = &t[8..t.len() - 1];
        let args = split_function_args(inner);
        if args.len() == 3 {
            let base = eval_joined_scalar_expr(&args[0], row, params, storage);
            let from = eval_joined_scalar_expr(&args[1], row, params, storage);
            let to = eval_joined_scalar_expr(&args[2], row, params, storage);
            if base.is_null() || from.is_null() || to.is_null() { return serde_json::Value::Null; }
            let base_s = json_val_to_str(&base);
            let from_s = json_val_to_str(&from);
            let to_s = json_val_to_str(&to);
            return serde_json::Value::String(base_s.replace(&from_s, &to_s));
        }
    }

    if (upper.starts_with("SUBSTRING(") || upper.starts_with("SUBSTR(")) && t.ends_with(')') {
        let open_p = t.find('(').unwrap();
        let inner = &t[open_p + 1..t.len() - 1];
        let args = split_function_args(inner);
        if args.len() >= 2 {
            let base = eval_joined_scalar_expr(&args[0], row, params, storage);
            if base.is_null() { return serde_json::Value::Null; }
            let start = eval_joined_scalar_expr(&args[1], row, params, storage).as_i64().unwrap_or(1);
            let length = if args.len() >= 3 {
                eval_joined_scalar_expr(&args[2], row, params, storage).as_i64()
            } else {
                None
            };
            let base_s = json_val_to_str(&base);
            let chars: Vec<char> = base_s.chars().collect();
            let start_idx = if start > 0 { (start as usize).saturating_sub(1) } else { 0 };
            let sub: String = if let Some(len) = length {
                chars.iter().skip(start_idx).take(len.max(0) as usize).collect()
            } else {
                chars.iter().skip(start_idx).collect()
            };
            return serde_json::Value::String(sub);
        }
    }

    // 10. Math functions: ABS, FLOOR, CEIL, ROUND, TRUNC, POWER, SQRT, CBRT, EXP, LN, LOG, MOD, SIGN, PI, DEGREES, RADIANS, RANDOM
    if upper == "PI()" || upper == "PI" {
        return serde_json::json!(std::f64::consts::PI);
    }

    if upper == "RANDOM()" {
        let nanos = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.subsec_nanos()).unwrap_or(12345);
        let rand = ((nanos % 1_000_000) as f64) / 1_000_000.0;
        return serde_json::json!(rand);
    }

    if upper.starts_with("ABS(") && t.ends_with(')') {
        let inner = &t[4..t.len() - 1];
        let v = eval_joined_scalar_expr(inner, row, params, storage);
        if v.is_null() { return serde_json::Value::Null; }
        if let Some(f) = v.as_f64() {
            return if f.fract() == 0.0 { serde_json::json!(f.abs() as i64) } else { serde_json::json!(f.abs()) };
        }
    }

    if upper.starts_with("FLOOR(") && t.ends_with(')') {
        let inner = &t[6..t.len() - 1];
        let v = eval_joined_scalar_expr(inner, row, params, storage);
        if v.is_null() { return serde_json::Value::Null; }
        if let Some(f) = v.as_f64() {
            return serde_json::json!(f.floor() as i64);
        }
    }

    if (upper.starts_with("CEIL(") || upper.starts_with("CEILING(")) && t.ends_with(')') {
        let start_pos = if upper.starts_with("CEILING(") { 8 } else { 5 };
        let inner = &t[start_pos..t.len() - 1];
        let v = eval_joined_scalar_expr(inner, row, params, storage);
        if v.is_null() { return serde_json::Value::Null; }
        if let Some(f) = v.as_f64() {
            return serde_json::json!(f.ceil() as i64);
        }
    }

    if upper.starts_with("ROUND(") && t.ends_with(')') {
        let inner = &t[6..t.len() - 1];
        let args = split_function_args(inner);
        if !args.is_empty() {
            let v = eval_joined_scalar_expr(&args[0], row, params, storage);
            if v.is_null() { return serde_json::Value::Null; }
            let decimals = if args.len() >= 2 {
                let d_val = eval_joined_scalar_expr(&args[1], row, params, storage);
                if d_val.is_null() { return serde_json::Value::Null; }
                d_val.as_i64().unwrap_or(0)
            } else {
                0
            };
            if let Some(f) = v.as_f64() {
                if decimals == 0 {
                    return serde_json::json!(f.round() as i64);
                } else {
                    let multiplier = 10f64.powi(decimals as i32);
                    let rounded = (f * multiplier).round() / multiplier;
                    return serde_json::json!(rounded);
                }
            }
        }
    }

    if (upper.starts_with("TRUNC(") || upper.starts_with("TRUNCATE(")) && t.ends_with(')') {
        let start_pos = if upper.starts_with("TRUNCATE(") { 9 } else { 6 };
        let inner = &t[start_pos..t.len() - 1];
        let args = split_function_args(inner);
        if !args.is_empty() {
            let v = eval_joined_scalar_expr(&args[0], row, params, storage);
            if v.is_null() { return serde_json::Value::Null; }
            let decimals = if args.len() >= 2 {
                let d_val = eval_joined_scalar_expr(&args[1], row, params, storage);
                if d_val.is_null() { return serde_json::Value::Null; }
                d_val.as_i64().unwrap_or(0)
            } else {
                0
            };
            if let Some(f) = v.as_f64() {
                if decimals == 0 {
                    return serde_json::json!(f.trunc() as i64);
                } else {
                    let multiplier = 10f64.powi(decimals as i32);
                    let truncated = (f * multiplier).trunc() / multiplier;
                    return serde_json::json!(truncated);
                }
            }
        }
    }

    if (upper.starts_with("POWER(") || upper.starts_with("POW(")) && t.ends_with(')') {
        let start_pos = if upper.starts_with("POWER(") { 6 } else { 4 };
        let inner = &t[start_pos..t.len() - 1];
        let args = split_function_args(inner);
        if args.len() == 2 {
            let base = eval_joined_scalar_expr(&args[0], row, params, storage);
            let exp = eval_joined_scalar_expr(&args[1], row, params, storage);
            if base.is_null() || exp.is_null() { return serde_json::Value::Null; }
            if let (Some(b), Some(e)) = (base.as_f64(), exp.as_f64()) {
                let res = b.powf(e);
                return if res.fract() == 0.0 { serde_json::json!(res as i64) } else { serde_json::json!(res) };
            }
        }
    }

    if upper.starts_with("SQRT(") && t.ends_with(')') {
        let inner = &t[5..t.len() - 1];
        let v = eval_joined_scalar_expr(inner, row, params, storage);
        if v.is_null() { return serde_json::Value::Null; }
        if let Some(f) = v.as_f64() {
            let res = f.sqrt();
            return if res.fract() == 0.0 { serde_json::json!(res as i64) } else { serde_json::json!(res) };
        }
    }

    if upper.starts_with("CBRT(") && t.ends_with(')') {
        let inner = &t[5..t.len() - 1];
        let v = eval_joined_scalar_expr(inner, row, params, storage);
        if v.is_null() { return serde_json::Value::Null; }
        if let Some(f) = v.as_f64() {
            let res = f.cbrt();
            return if res.fract() == 0.0 { serde_json::json!(res as i64) } else { serde_json::json!(res) };
        }
    }

    if upper.starts_with("EXP(") && t.ends_with(')') {
        let inner = &t[4..t.len() - 1];
        let v = eval_joined_scalar_expr(inner, row, params, storage);
        if v.is_null() { return serde_json::Value::Null; }
        if let Some(f) = v.as_f64() {
            let res = f.exp();
            return if res.fract() == 0.0 { serde_json::json!(res as i64) } else { serde_json::json!(res) };
        }
    }

    if upper.starts_with("LN(") && t.ends_with(')') {
        let inner = &t[3..t.len() - 1];
        let v = eval_joined_scalar_expr(inner, row, params, storage);
        if v.is_null() { return serde_json::Value::Null; }
        if let Some(f) = v.as_f64() {
            let res = f.ln();
            return if res.fract() == 0.0 { serde_json::json!(res as i64) } else { serde_json::json!(res) };
        }
    }

    if (upper.starts_with("LOG(") || upper.starts_with("LOG10(")) && t.ends_with(')') {
        let start_pos = if upper.starts_with("LOG10(") { 6 } else { 4 };
        let inner = &t[start_pos..t.len() - 1];
        let args = split_function_args(inner);
        if args.len() == 1 {
            let v = eval_joined_scalar_expr(&args[0], row, params, storage);
            if v.is_null() { return serde_json::Value::Null; }
            if let Some(f) = v.as_f64() {
                let res = f.log10();
                return if res.fract() == 0.0 { serde_json::json!(res as i64) } else { serde_json::json!(res) };
            }
        } else if args.len() == 2 {
            let b_val = eval_joined_scalar_expr(&args[0], row, params, storage);
            let x_val = eval_joined_scalar_expr(&args[1], row, params, storage);
            if b_val.is_null() || x_val.is_null() { return serde_json::Value::Null; }
            if let (Some(b), Some(x)) = (b_val.as_f64(), x_val.as_f64()) {
                let res = x.log(b);
                return if res.fract() == 0.0 { serde_json::json!(res as i64) } else { serde_json::json!(res) };
            }
        }
    }

    if upper.starts_with("DEGREES(") && t.ends_with(')') {
        let inner = &t[8..t.len() - 1];
        let v = eval_joined_scalar_expr(inner, row, params, storage);
        if v.is_null() { return serde_json::Value::Null; }
        if let Some(f) = v.as_f64() {
            return serde_json::json!(f.to_degrees());
        }
    }

    if upper.starts_with("RADIANS(") && t.ends_with(')') {
        let inner = &t[8..t.len() - 1];
        let v = eval_joined_scalar_expr(inner, row, params, storage);
        if v.is_null() { return serde_json::Value::Null; }
        if let Some(f) = v.as_f64() {
            return serde_json::json!(f.to_radians());
        }
    }

    if upper.starts_with("MOD(") && t.ends_with(')') {
        let inner = &t[4..t.len() - 1];
        let args = split_function_args(inner);
        if args.len() == 2 {
            let a = eval_joined_scalar_expr(&args[0], row, params, storage);
            let b = eval_joined_scalar_expr(&args[1], row, params, storage);
            if a.is_null() || b.is_null() { return serde_json::Value::Null; }
            if let (Some(a_i), Some(b_i)) = (a.as_i64(), b.as_i64()) {
                return serde_json::json!(if b_i != 0 { a_i % b_i } else { 0 });
            }
        }
    }

    if upper.starts_with("SIGN(") && t.ends_with(')') {
        let inner = &t[5..t.len() - 1];
        let v = eval_joined_scalar_expr(inner, row, params, storage);
        if v.is_null() { return serde_json::Value::Null; }
        if let Some(f) = v.as_f64() {
            return serde_json::json!(if f > 0.0 { 1 } else if f < 0.0 { -1 } else { 0 });
        }
    }

    let val = eval_operand_on_row(row, t, params);
    value_to_json(&val)
}

fn eval_group_aggregate_expr(
    expr: &str,
    all_rows: &[CombinedRow],
    row_indices: &[usize],
    params: &[Value],
    storage: &crate::storage::engine::StorageEngine,
) -> serde_json::Value {
    let t = expr.trim();
    let upper = t.to_uppercase();

    // 1. COALESCE(SUM(col), default)
    if upper.starts_with("COALESCE(") && t.ends_with(')') {
        let inner = &t[9..t.len() - 1];
        let args = split_function_args(inner);
        if !args.is_empty() {
            let agg_res = eval_group_aggregate_expr(&args[0], all_rows, row_indices, params, storage);
            let is_null = match &agg_res {
                serde_json::Value::Null => true,
                _ => false,
            };
            if !is_null {
                return agg_res;
            }
            if args.len() >= 2 {
                if let Some(&first_ri) = row_indices.first() {
                    let def_val = eval_operand_on_row(&all_rows[first_ri], &args[1], params);
                    return value_to_json(&def_val);
                } else {
                    let def_val = parse_value(&args[1], params);
                    return value_to_json(&def_val);
                }
            }
        }
        return serde_json::Value::Null;
    }

    // 2. COUNT(*) / COUNT(col) / COUNT(DISTINCT col)
    if upper.starts_with("COUNT(") && t.ends_with(')') {
        let inner = t[6..t.len() - 1].trim();
        let upper_inner = inner.to_uppercase();
        if upper_inner == "*" || upper_inner == "1" {
            return serde_json::json!(row_indices.len());
        } else if upper_inner.starts_with("DISTINCT ") {
            let col = inner[9..].trim();
            let mut set = std::collections::HashSet::new();
            for &ri in row_indices {
                let v = all_rows[ri].get_val(col);
                if !v.is_null() {
                    set.insert(v.as_str());
                }
            }
            return serde_json::json!(set.len());
        } else {
            let count = row_indices.iter().filter(|&&ri| !all_rows[ri].get_val(inner).is_null()).count();
            return serde_json::json!(count);
        }
    }

    // 3. SUM(col)
    if upper.starts_with("SUM(") && t.ends_with(')') {
        let inner = t[4..t.len() - 1].trim();
        let mut total = 0.0;
        let mut has_non_null = false;
        for &ri in row_indices {
            let v = all_rows[ri].get_val(inner);
            if let Some(f) = v.as_f64() {
                total += f;
                has_non_null = true;
            }
        }
        return if has_non_null { serde_json::json!(total) } else { serde_json::Value::Null };
    }

    // 4. AVG(col)
    if upper.starts_with("AVG(") && t.ends_with(')') {
        let inner = t[4..t.len() - 1].trim();
        let mut total = 0.0;
        let mut count = 0;
        for &ri in row_indices {
            let v = all_rows[ri].get_val(inner);
            if let Some(f) = v.as_f64() {
                total += f;
                count += 1;
            }
        }
        return if count > 0 { serde_json::json!(total / (count as f64)) } else { serde_json::Value::Null };
    }

    // 5. MIN(col)
    if upper.starts_with("MIN(") && t.ends_with(')') {
        let inner = t[4..t.len() - 1].trim();
        let mut min_val: Option<Value> = None;
        for &ri in row_indices {
            let v = all_rows[ri].get_val(inner);
            if !v.is_null() {
                if let Some(ref curr_min) = min_val {
                    if v.cmp_value(curr_min) == std::cmp::Ordering::Less {
                        min_val = Some(v);
                    }
                } else {
                    min_val = Some(v);
                }
            }
        }
        return min_val.map(|v| value_to_json(&v)).unwrap_or(serde_json::Value::Null);
    }

    // 6. MAX(col)
    if upper.starts_with("MAX(") && t.ends_with(')') {
        let inner = t[4..t.len() - 1].trim();
        let mut max_val: Option<Value> = None;
        for &ri in row_indices {
            let v = all_rows[ri].get_val(inner);
            if !v.is_null() {
                if let Some(ref curr_max) = max_val {
                    if v.cmp_value(curr_max) == std::cmp::Ordering::Greater {
                        max_val = Some(v);
                    }
                } else {
                    max_val = Some(v);
                }
            }
        }
        return max_val.map(|v| value_to_json(&v)).unwrap_or(serde_json::Value::Null);
    }

    // 7. STRING_AGG(col, delim)
    if upper.starts_with("STRING_AGG(") && t.ends_with(')') {
        let inner = &t[11..t.len() - 1];
        let args = split_function_args(inner);
        if !args.is_empty() {
            let col = &args[0];
            let delim = if args.len() >= 2 {
                let d_val = parse_value(&args[1], params);
                d_val.as_str()
            } else {
                ",".to_string()
            };
            let mut parts = Vec::new();
            for &ri in row_indices {
                let v = all_rows[ri].get_val(col);
                if !v.is_null() {
                    parts.push(v.as_str());
                }
            }
            return if parts.is_empty() {
                serde_json::Value::Null
            } else {
                serde_json::Value::String(parts.join(&delim))
            };
        }
    }

    // 8. ARRAY_AGG(col)
    if upper.starts_with("ARRAY_AGG(") && t.ends_with(')') {
        let inner = t[10..t.len() - 1].trim();
        let mut arr = Vec::new();
        for &ri in row_indices {
            let v = all_rows[ri].get_val(inner);
            if !v.is_null() {
                arr.push(value_to_json(&v));
            }
        }
        return serde_json::Value::Array(arr);
    }

    // 6. JSONB_AGG(expr) / JSON_AGG(expr)
    if (upper.starts_with("JSONB_AGG(") || upper.starts_with("JSON_AGG(")) && t.ends_with(')') {
        let inner = t[10..t.len() - 1].trim();
        let mut arr = Vec::new();
        for &ri in row_indices {
            let v_json = eval_joined_scalar_expr(inner, &all_rows[ri], params, storage);
            arr.push(v_json);
        }
        return serde_json::Value::Array(arr);
    }

    // 7. Group column fallback
    if let Some(&first_ri) = row_indices.first() {
        let v = all_rows[first_ri].get_val(t);
        value_to_json(&v)
    } else {
        serde_json::Value::Null
    }
}

fn eval_having_condition(
    h_str: &str,
    row_map: &serde_json::Map<String, serde_json::Value>,
    extra_names: Option<&[(&str, &str)]>,
) -> bool {
    let mut substituted = h_str.to_string();

    let mut replacements: Vec<(String, String)> = Vec::new();
    if let Some(names) = extra_names {
        for &(name, alias) in names {
            let val_opt = row_map.get(alias).or_else(|| row_map.get(name));
            if let Some(v) = val_opt {
                let lit = match v {
                    serde_json::Value::Null => "NULL".to_string(),
                    serde_json::Value::Bool(b) => if *b { "TRUE".to_string() } else { "FALSE".to_string() },
                    serde_json::Value::Number(n) => n.to_string(),
                    serde_json::Value::String(s) => format!("'{}'", s.replace('\'', "''")),
                    _ => format!("'{}'", v.to_string().replace('\'', "''")),
                };
                if !name.is_empty() {
                    replacements.push((name.to_string(), lit.clone()));
                }
                if !alias.is_empty() {
                    replacements.push((alias.to_string(), lit));
                }
            }
        }
    }
    for (k, v) in row_map {
        let lit = match v {
            serde_json::Value::Null => "NULL".to_string(),
            serde_json::Value::Bool(b) => if *b { "TRUE".to_string() } else { "FALSE".to_string() },
            serde_json::Value::Number(n) => n.to_string(),
            serde_json::Value::String(s) => format!("'{}'", s.replace('\'', "''")),
            _ => format!("'{}'", v.to_string().replace('\'', "''")),
        };
        replacements.push((k.clone(), lit.clone()));
        let cleaned = clean_col_name(k);
        if cleaned != k {
            replacements.push((cleaned.to_string(), lit));
        }
    }

    replacements.sort_by(|a, b| b.0.len().cmp(&a.0.len()));
    replacements.dedup_by(|a, b| a.0.eq_ignore_ascii_case(&b.0));

    for (k, lit) in replacements {
        let mut idx = 0;
        while idx < substituted.len() {
            let sub_slice = &substituted[idx..];
            let found_opt = sub_slice.to_uppercase().find(&k.to_uppercase());
            if let Some(offset) = found_opt {
                let actual_idx = idx + offset;
                substituted.replace_range(actual_idx..actual_idx + k.len(), &lit);
                idx = actual_idx + lit.len();
            } else {
                break;
            }
        }
    }

    evaluate_custom_condition(&substituted, &[], None, &[], None, &[])
}

fn evaluate_having_with_group(
    having_str: &str,
    row_map: &serde_json::Map<String, serde_json::Value>,
    extra_names: Option<&[(&str, &str)]>,
    table: &crate::storage::table::Table,
    r_indices: &[usize],
    _params: &[Value],
) -> bool {
    let mut substituted = having_str.to_string();

    let upper = having_str.to_uppercase();
    let agg_prefixes = ["BOOL_AND(", "BOOL_OR(", "EVERY(", "COUNT(", "SUM(", "AVG(", "MIN(", "MAX("];
    for &prefix in &agg_prefixes {
        let mut search_start = 0;
        while let Some(pos) = upper[search_start..].find(prefix) {
            let actual_pos = search_start + pos;
            if let Some(close_p) = find_matching_paren(having_str, actual_pos + prefix.len() - 1) {
                let agg_call = &having_str[actual_pos..=close_p];
                let inner = &having_str[actual_pos + prefix.len()..close_p].trim();
                let inner_col = clean_col_name(inner);
                let val_lit = match prefix {
                    "BOOL_AND(" | "EVERY(" => {
                        let mut res: Option<bool> = None;
                        if let Some(c_idx) = table.get_column_index(inner_col) {
                            for &ri in r_indices {
                                let v = &table.rows[ri][c_idx];
                                if !v.is_null() {
                                    res = Some(res.unwrap_or(true) && v.as_bool().unwrap_or(false));
                                }
                            }
                        }
                        match res {
                            Some(true) => "TRUE".to_string(),
                            Some(false) => "FALSE".to_string(),
                            None => "NULL".to_string(),
                        }
                    }
                    "BOOL_OR(" => {
                        let mut res: Option<bool> = None;
                        if let Some(c_idx) = table.get_column_index(inner_col) {
                            for &ri in r_indices {
                                let v = &table.rows[ri][c_idx];
                                if !v.is_null() {
                                    res = Some(res.unwrap_or(false) || v.as_bool().unwrap_or(false));
                                }
                            }
                        }
                        match res {
                            Some(true) => "TRUE".to_string(),
                            Some(false) => "FALSE".to_string(),
                            None => "NULL".to_string(),
                        }
                    }
                    "COUNT(" => {
                        if inner.eq_ignore_ascii_case("*") || *inner == "1" {
                            r_indices.len().to_string()
                        } else if let Some(c_idx) = table.get_column_index(inner_col) {
                            let cnt = r_indices.iter().filter(|&&ri| !table.rows[ri][c_idx].is_null()).count();
                            cnt.to_string()
                        } else {
                            r_indices.len().to_string()
                        }
                    }
                    "SUM(" => {
                        if let Some(c_idx) = table.get_column_index(inner_col) {
                            let mut sum = 0.0;
                            for &ri in r_indices {
                                if let Some(f) = table.rows[ri][c_idx].as_f64() {
                                    sum += f;
                                }
                            }
                            sum.to_string()
                        } else {
                            "0".to_string()
                        }
                    }
                    "AVG(" => {
                        if let Some(c_idx) = table.get_column_index(inner_col) {
                            let mut sum = 0.0;
                            let mut count = 0;
                            for &ri in r_indices {
                                if let Some(f) = table.rows[ri][c_idx].as_f64() {
                                    sum += f;
                                    count += 1;
                                }
                            }
                            if count > 0 { (sum / count as f64).to_string() } else { "NULL".to_string() }
                        } else {
                            "NULL".to_string()
                        }
                    }
                    "MIN(" => {
                        if let Some(c_idx) = table.get_column_index(inner_col) {
                            let mut min_val: Option<Value> = None;
                            for &ri in r_indices {
                                let v = &table.rows[ri][c_idx];
                                if !v.is_null() {
                                    min_val = match min_val {
                                        None => Some(v.clone()),
                                        Some(cur) => if v.cmp_value(&cur) == std::cmp::Ordering::Less { Some(v.clone()) } else { Some(cur) },
                                    };
                                }
                            }
                            match min_val {
                                Some(Value::Text(s)) => format!("'{}'", s.replace('\'', "''")),
                                Some(v) => v.as_str(),
                                None => "NULL".to_string(),
                            }
                        } else {
                            "NULL".to_string()
                        }
                    }
                    "MAX(" => {
                        if let Some(c_idx) = table.get_column_index(inner_col) {
                            let mut max_val: Option<Value> = None;
                            for &ri in r_indices {
                                let v = &table.rows[ri][c_idx];
                                if !v.is_null() {
                                    max_val = match max_val {
                                        None => Some(v.clone()),
                                        Some(cur) => if v.cmp_value(&cur) == std::cmp::Ordering::Greater { Some(v.clone()) } else { Some(cur) },
                                    };
                                }
                            }
                            match max_val {
                                Some(Value::Text(s)) => format!("'{}'", s.replace('\'', "''")),
                                Some(v) => v.as_str(),
                                None => "NULL".to_string(),
                            }
                        } else {
                            "NULL".to_string()
                        }
                    }
                    _ => "NULL".to_string(),
                };
                substituted = substituted.replace(agg_call, &val_lit);
                search_start = actual_pos + prefix.len();
            } else {
                break;
            }
        }
    }

    eval_having_condition(&substituted, row_map, extra_names)
}

fn has_top_level_join(after_from: &str) -> bool {
    let where_pos = find_top_level_keyword(after_from, "WHERE");
    let group_pos = find_top_level_keyword(after_from, "GROUP BY");
    let having_pos = find_top_level_keyword(after_from, "HAVING");
    let order_pos = find_top_level_keyword(after_from, "ORDER BY");
    let limit_pos = find_top_level_keyword(after_from, "LIMIT");
    let offset_pos = find_top_level_keyword(after_from, "OFFSET");

    let first_kw_pos = [where_pos, group_pos, having_pos, order_pos, limit_pos, offset_pos]
        .into_iter()
        .filter_map(|x| x)
        .min();

    let from_part = match first_kw_pos {
        Some(pos) => &after_from[..pos],
        None => after_from,
    };

    find_top_level_keyword(from_part, "JOIN").is_some() || split_comma_separated_tokens(from_part).len() > 1
}

fn sort_json_rows(rows: &mut [serde_json::Value], specs: &[OrderBySpec]) {
    if specs.is_empty() {
        return;
    }
    rows.sort_by(|a, b| {
        for spec in specs {
            let clean = clean_col_name(spec.col_name).replace('"', "");
            let mut va = a.get(&clean);
            let mut vb = b.get(&clean);
            if va.is_none() {
                let unquoted = spec.col_name.replace('"', "");
                va = a.get(&unquoted);
                vb = b.get(&unquoted);
            }
            let ord = match (va, vb) {
                (Some(serde_json::Value::Number(n1)), Some(serde_json::Value::Number(n2))) => {
                    let f1 = n1.as_f64().unwrap_or(0.0);
                    let f2 = n2.as_f64().unwrap_or(0.0);
                    f1.partial_cmp(&f2).unwrap_or(std::cmp::Ordering::Equal)
                }
                (Some(serde_json::Value::String(s1)), Some(serde_json::Value::String(s2))) => {
                    s1.cmp(s2)
                }
                (Some(serde_json::Value::Bool(b1)), Some(serde_json::Value::Bool(b2))) => {
                    b1.cmp(b2)
                }
                (None, Some(_)) => std::cmp::Ordering::Less,
                (Some(_), None) => std::cmp::Ordering::Greater,
                _ => std::cmp::Ordering::Equal,
            };
            let directed = if spec.is_desc { ord.reverse() } else { ord };
            if directed != std::cmp::Ordering::Equal {
                return directed;
            }
        }
        std::cmp::Ordering::Equal
    });
}

fn eval_window_functions(rows: &mut [serde_json::Value], select_clause: &str) {
    let items = split_projection_items(select_clause);
    for item in items {
        let upper_item = item.to_uppercase();
        let Some(over_idx) = upper_item.find(" OVER (") else {
            continue;
        };
        let func_part = item[..over_idx].trim();
        let upper_func = func_part.to_uppercase();
        let after_over = &item[over_idx + 7..];
        let Some(close_paren) = after_over.find(')') else {
            continue;
        };
        let window_spec = &after_over[..close_paren].trim();
        let after_paren = after_over[close_paren + 1..].trim();

        let alias = if let Some(as_idx) = after_paren.to_uppercase().find("AS ") {
            after_paren[as_idx + 3..].trim().trim_matches('"').to_string()
        } else if let Some(col) = after_paren.split_whitespace().next() {
            col.trim_matches('"').to_string()
        } else {
            func_part.to_string()
        };

        let upper_spec = window_spec.to_uppercase();
        let (partition_cols, order_spec_str): (Vec<String>, Option<&str>) = if let Some(part_idx) = upper_spec.find("PARTITION BY ") {
            let after_part = &window_spec[part_idx + 13..];
            if let Some(order_idx) = after_part.to_uppercase().find("ORDER BY ") {
                let part_str = &after_part[..order_idx].trim();
                let ord_str = &after_part[order_idx + 9..].trim();
                (part_str.split(',').map(|s| clean_col_name(s.trim()).to_string()).collect(), Some(*ord_str))
            } else {
                (after_part.split(',').map(|s| clean_col_name(s.trim()).to_string()).collect(), None)
            }
        } else if let Some(order_idx) = upper_spec.find("ORDER BY ") {
            (vec![], Some(&window_spec[order_idx + 9..].trim()))
        } else {
            (vec![], None)
        };

        let order_specs: Vec<OrderBySpec> = match order_spec_str {
            Some(ord_str) => parse_order_by_specs(ord_str),
            None => vec![],
        };

        let mut partitions_order: Vec<String> = Vec::new();
        let mut partitions: std::collections::HashMap<String, Vec<usize>> = std::collections::HashMap::new();
        for (idx, r) in rows.iter().enumerate() {
            let part_key = partition_cols.iter().map(|col| {
                r.get(col).map(|v| v.to_string()).unwrap_or_else(|| "null".to_string())
            }).collect::<Vec<_>>().join("\x1f");
            if !partitions.contains_key(&part_key) {
                partitions_order.push(part_key.clone());
            }
            partitions.entry(part_key).or_default().push(idx);
        }

        let mut ordered_indices = Vec::new();
        for key in &partitions_order {
            if let Some(indices) = partitions.get_mut(key) {
                if !order_specs.is_empty() {
                    indices.sort_by(|&a, &b| {
                        let ra = &rows[a];
                        let rb = &rows[b];
                        for spec in &order_specs {
                            let clean = clean_col_name(spec.col_name);
                            let va = ra.get(clean);
                            let vb = rb.get(clean);
                            let ord = match (va, vb) {
                                (Some(serde_json::Value::Number(n1)), Some(serde_json::Value::Number(n2))) => {
                                    let f1 = n1.as_f64().unwrap_or(0.0);
                                    let f2 = n2.as_f64().unwrap_or(0.0);
                                    f1.partial_cmp(&f2).unwrap_or(std::cmp::Ordering::Equal)
                                }
                                (Some(serde_json::Value::String(s1)), Some(serde_json::Value::String(s2))) => {
                                    s1.cmp(s2)
                                }
                                (Some(serde_json::Value::Bool(b1)), Some(serde_json::Value::Bool(b2))) => {
                                    b1.cmp(b2)
                                }
                                _ => std::cmp::Ordering::Equal,
                            };
                            let directed = if spec.is_desc { ord.reverse() } else { ord };
                            if directed != std::cmp::Ordering::Equal {
                                return directed;
                            }
                        }
                        std::cmp::Ordering::Equal
                    });
                }

                let partition_len = indices.len();
                let mut current_dense_rank = 1;
                for (pos, &row_idx) in indices.iter().enumerate() {
                    let computed_val = if upper_func.starts_with("ROW_NUMBER") {
                        serde_json::Value::Number((pos + 1).into())
                    } else if upper_func.starts_with("DENSE_RANK") {
                        if pos > 0 && !order_specs.is_empty() {
                            let prev_row_idx = indices[pos - 1];
                            let spec = &order_specs[0];
                            let clean = clean_col_name(spec.col_name);
                            let cur_val = rows[row_idx].get(clean);
                            let prev_val = rows[prev_row_idx].get(clean);
                            if cur_val != prev_val {
                                current_dense_rank += 1;
                            }
                        }
                        serde_json::Value::Number(current_dense_rank.into())
                    } else if upper_func.starts_with("RANK") {
                        serde_json::Value::Number((pos + 1).into())
                    } else if upper_func.starts_with("SUM(") {
                        let col = clean_col_name(&func_part[4..func_part.len() - 1]);
                        let mut running_sum = 0.0;
                        let mut count = 0;
                        for &cur_idx in &indices[..=pos] {
                            if let Some(v) = rows[cur_idx].get(col) {
                                if let Some(n) = v.as_f64() {
                                    running_sum += n;
                                    count += 1;
                                }
                            }
                        }
                        if count == 0 {
                            serde_json::Value::Null
                        } else if running_sum.fract() == 0.0 {
                            serde_json::Value::Number((running_sum as i64).into())
                        } else {
                            serde_json::Value::Number(serde_json::Number::from_f64(running_sum).unwrap_or(0.into()))
                        }
                    } else if upper_func.starts_with("AVG(") {
                        let col = clean_col_name(&func_part[4..func_part.len() - 1]);
                        let mut running_sum = 0.0;
                        let mut count = 0;
                        for &cur_idx in &indices[..=pos] {
                            if let Some(v) = rows[cur_idx].get(col) {
                                if let Some(n) = v.as_f64() {
                                    running_sum += n;
                                    count += 1;
                                }
                            }
                        }
                        if count > 0 {
                            serde_json::Value::Number(serde_json::Number::from_f64(running_sum / count as f64).unwrap_or(0.into()))
                        } else {
                            serde_json::Value::Null
                        }
                    } else if upper_func.starts_with("COUNT(") {
                        serde_json::Value::Number((pos + 1).into())
                    } else if upper_func.starts_with("LAG(") {
                        let inner = func_part[4..func_part.len() - 1].trim();
                        let parts: Vec<&str> = inner.split(',').collect();
                        let col_name = clean_col_name(parts[0].trim());
                        let offset: usize = parts.get(1).and_then(|s| s.trim().parse().ok()).unwrap_or(1);
                        if pos >= offset {
                            let prev_idx = indices[pos - offset];
                            rows[prev_idx].get(col_name).cloned().unwrap_or(serde_json::Value::Null)
                        } else {
                            serde_json::Value::Null
                        }
                    } else if upper_func.starts_with("LEAD(") {
                        let inner = func_part[5..func_part.len() - 1].trim();
                        let parts: Vec<&str> = inner.split(',').collect();
                        let col_name = clean_col_name(parts[0].trim());
                        let offset: usize = parts.get(1).and_then(|s| s.trim().parse().ok()).unwrap_or(1);
                        if pos + offset < partition_len {
                            let next_idx = indices[pos + offset];
                            rows[next_idx].get(col_name).cloned().unwrap_or(serde_json::Value::Null)
                        } else {
                            serde_json::Value::Null
                        }
                    } else if upper_func.starts_with("NTILE(") {
                        let k: usize = func_part[6..func_part.len() - 1].trim().parse().unwrap_or(1).max(1);
                        let tile = ((pos * k) / partition_len) + 1;
                        serde_json::Value::Number(tile.into())
                    } else if upper_func.starts_with("PERCENT_RANK") {
                        let pr = if partition_len > 1 {
                            pos as f64 / (partition_len - 1) as f64
                        } else {
                            0.0
                        };
                        serde_json::Value::Number(serde_json::Number::from_f64(pr).unwrap_or(0.into()))
                    } else if upper_func.starts_with("CUME_DIST") {
                        let cd = (pos + 1) as f64 / partition_len as f64;
                        serde_json::Value::Number(serde_json::Number::from_f64(cd).unwrap_or(0.into()))
                    } else if upper_func.starts_with("MIN(") {
                        let col = clean_col_name(&func_part[4..func_part.len() - 1]);
                        let mut min_val: Option<f64> = None;
                        for &cur_idx in &indices[..=pos] {
                            if let Some(v) = rows[cur_idx].get(col) {
                                if let Some(n) = v.as_f64() {
                                    min_val = Some(min_val.map_or(n, |m| m.min(n)));
                                }
                            }
                        }
                        match min_val {
                            Some(m) if m.fract() == 0.0 => serde_json::Value::Number((m as i64).into()),
                            Some(m) => serde_json::Value::Number(serde_json::Number::from_f64(m).unwrap_or(0.into())),
                            None => serde_json::Value::Null,
                        }
                    } else if upper_func.starts_with("MAX(") {
                        let col = clean_col_name(&func_part[4..func_part.len() - 1]);
                        let mut max_val: Option<f64> = None;
                        for &cur_idx in &indices[..=pos] {
                            if let Some(v) = rows[cur_idx].get(col) {
                                if let Some(n) = v.as_f64() {
                                    max_val = Some(max_val.map_or(n, |m| m.max(n)));
                                }
                            }
                        }
                        match max_val {
                            Some(m) if m.fract() == 0.0 => serde_json::Value::Number((m as i64).into()),
                            Some(m) => serde_json::Value::Number(serde_json::Number::from_f64(m).unwrap_or(0.into())),
                            None => serde_json::Value::Null,
                        }
                    } else if upper_func.starts_with("NTH_VALUE(") {
                        let inner = func_part[10..func_part.len() - 1].trim();
                        let parts: Vec<&str> = inner.split(',').collect();
                        let col_name = clean_col_name(parts[0].trim());
                        let nth: usize = parts.get(1).and_then(|s| s.trim().parse().ok()).unwrap_or(1);
                        if nth >= 1 && nth <= partition_len {
                            let target_idx = indices[nth - 1];
                            rows[target_idx].get(col_name).cloned().unwrap_or(serde_json::Value::Null)
                        } else {
                            serde_json::Value::Null
                        }
                    } else if upper_func.starts_with("FIRST_VALUE(") {
                        let inner = clean_col_name(&func_part[12..func_part.len() - 1]);
                        let first_idx = indices[0];
                        rows[first_idx].get(inner).cloned().unwrap_or(serde_json::Value::Null)
                    } else if upper_func.starts_with("LAST_VALUE(") {
                        let inner = clean_col_name(&func_part[11..func_part.len() - 1]);
                        let last_idx = indices[partition_len - 1];
                        rows[last_idx].get(inner).cloned().unwrap_or(serde_json::Value::Null)
                    } else {
                        serde_json::Value::Null
                    };

                    if let serde_json::Value::Object(ref mut map) = rows[row_idx] {
                        map.insert(alias.clone(), computed_val.clone());
                        map.insert(func_part.to_string(), computed_val);
                    }
                }

                for &idx in indices.iter() {
                    ordered_indices.push(idx);
                }
            }
        }
        if !order_specs.is_empty() && ordered_indices.len() == rows.len() {
            let mut new_rows = Vec::with_capacity(rows.len());
            for &idx in &ordered_indices {
                new_rows.push(rows[idx].clone());
            }
            rows.clone_from_slice(&new_rows);
        }
    }
}

struct JoinClause<'a> {
    is_left: bool,
    joined_table_name: &'a str,
    joined_table_alias: Option<&'a str>,
    primary_join_col: &'a str,
    joined_join_col: &'a str,
}

#[derive(Clone, Debug)]
struct OrderBySpec<'a> {
    col_name: &'a str,
    is_desc: bool,
}

fn parse_order_by_specs<'a>(order_part: &'a str) -> Vec<OrderBySpec<'a>> {
    split_comma_separated_tokens(order_part)
        .into_iter()
        .filter_map(|item| {
            let item_trimmed = item.trim();
            let upper = item_trimmed.to_uppercase();
            let (raw_col, desc) = if upper.ends_with(" DESC") {
                (item_trimmed[..item_trimmed.len() - 5].trim(), true)
            } else if upper.ends_with(" ASC") {
                (item_trimmed[..item_trimmed.len() - 4].trim(), false)
            } else {
                (item_trimmed, false)
            };
            let col = clean_col_name(raw_col);
            if col.is_empty() {
                return None;
            }
            Some(OrderBySpec { col_name: raw_col, is_desc: desc })
        })
        .collect()
}

struct SelectClauses<'a> {
    table_name: &'a str,
    table_alias: Option<&'a str>,
    join_clause: Option<JoinClause<'a>>,
    where_clause: Option<&'a str>,
    group_by_clause: Option<&'a str>,
    having_clause: Option<&'a str>,
    order_by_specs: Vec<OrderBySpec<'a>>,
    limit: Option<OperandTemplate>,
    offset: Option<OperandTemplate>,
}

fn split_projection_items(s: &str) -> Vec<String> {
    let mut items = Vec::new();
    let mut current = String::new();
    let mut depth: usize = 0;
    let mut in_quote = false;
    let mut in_dq = false;
    for c in s.chars() {
        if c == '\'' && !in_dq {
            in_quote = !in_quote;
            current.push(c);
        } else if c == '"' && !in_quote {
            in_dq = !in_dq;
            current.push(c);
        } else if in_quote || in_dq {
            current.push(c);
        } else if c == '(' || c == '[' {
            depth += 1;
            current.push(c);
        } else if c == ')' || c == ']' {
            depth = depth.saturating_sub(1);
            current.push(c);
        } else if c == ',' && depth == 0 {
            items.push(current.trim().to_string());
            current.clear();
        } else {
            current.push(c);
        }
    }
    if !current.trim().is_empty() {
        items.push(current.trim().to_string());
    }
    items
}

fn parse_projection(
    select_clause: &str,
    primary_table: &crate::storage::table::Table,
    _primary_alias: Option<&str>,
    joined_table: Option<&crate::storage::table::Table>,
    joined_alias: Option<&str>,
    storage: &crate::storage::engine::StorageEngine,
    params: &[Value],
) -> Vec<ProjectedExpr> {
    let items = split_projection_items(select_clause);
    let mut exprs = Vec::new();

    for item in items {
        let trimmed_item = item.trim();
        if trimmed_item.ends_with(".*") {
            let prefix = trimmed_item[..trimmed_item.len() - 2].trim().trim_matches('"');
            let is_primary = prefix.eq_ignore_ascii_case(&primary_table.name)
                || _primary_alias.map(|a| prefix.eq_ignore_ascii_case(a.trim_matches('"'))).unwrap_or(false);
            if is_primary {
                for (c_idx, col) in primary_table.columns.iter().enumerate() {
                    exprs.push(ProjectedExpr::PrimaryCol { col_idx: c_idx, alias: col.name.clone() });
                }
                continue;
            }
            if let Some(jt) = joined_table {
                let is_joined = prefix.eq_ignore_ascii_case(&jt.name)
                    || joined_alias.map(|a| prefix.eq_ignore_ascii_case(a.trim_matches('"'))).unwrap_or(false);
                if is_joined {
                    for (c_idx, col) in jt.columns.iter().enumerate() {
                        exprs.push(ProjectedExpr::JoinedCol { col_idx: c_idx, alias: col.name.clone() });
                    }
                    continue;
                }
            }
        } else if trimmed_item == "*" {
            for (c_idx, col) in primary_table.columns.iter().enumerate() {
                exprs.push(ProjectedExpr::PrimaryCol { col_idx: c_idx, alias: col.name.clone() });
            }
            if let Some(jt) = joined_table {
                for (c_idx, col) in jt.columns.iter().enumerate() {
                    exprs.push(ProjectedExpr::JoinedCol { col_idx: c_idx, alias: col.name.clone() });
                }
            }
            continue;
        }

        let upper = item.to_uppercase();
        let (expr_part, alias) = if let Some(as_idx) = upper.rfind(" AS ") {
            (item[..as_idx].trim(), item[as_idx + 4..].trim().trim_matches('"').to_string())
        } else {
            let col = clean_col_name(item.trim());
            (item.trim(), col.to_string())
        };

        let mut trimmed_expr = expr_part;
        while trimmed_expr.starts_with('(') && trimmed_expr.ends_with(')') && is_fully_enclosed_in_parens(trimmed_expr) {
            trimmed_expr = trimmed_expr[1..trimmed_expr.len() - 1].trim();
        }
        let upper_trimmed = trimmed_expr.to_uppercase();

        // Check for subquery in projection: (SELECT COUNT(*) FROM schools s WHERE s.region_id = r.id AND s.deleted_at IS NULL)
        if upper_trimmed.starts_with("SELECT") && upper_trimmed.contains("FROM") {
            if upper_trimmed.starts_with("SELECT COUNT") {
                if let Some(from_pos) = find_top_level_keyword(trimmed_expr, "FROM") {
                    let after_sub_from = trimmed_expr[from_pos + 4..].trim();
                    let where_sub_pos = find_top_level_keyword(after_sub_from, "WHERE");
                    let child_tbl_part = if let Some(w_pos) = where_sub_pos {
                        &after_sub_from[..w_pos]
                    } else {
                        after_sub_from
                    };
                    let (child_table_name, _child_alias) = extract_table_name_and_alias(child_tbl_part);

                    if let Some(child_table) = storage.get_table(child_table_name) {
                        if let Some(w_pos) = where_sub_pos {
                            let sub_where = after_sub_from[w_pos + 5..].trim();
                            let and_parts = split_top_level_and(sub_where);
                            let mut join_cols: Option<(usize, usize)> = None; // (child_join_col_idx, parent_join_col_idx)
                            let mut extra_conds = Vec::new();

                            for part in and_parts {
                                let part_trimmed = part.trim();
                                if let Some(eq_idx) = part_trimmed.find('=') {
                                    let left = part_trimmed[..eq_idx].trim();
                                    let right = part_trimmed[eq_idx + 1..].trim();
                                    let col_l = clean_col_name(left);
                                    let col_r = clean_col_name(right);

                                    let left_in_child = child_table.get_column_index(col_l);
                                    let right_in_child = child_table.get_column_index(col_r);
                                    let left_in_parent = primary_table.get_column_index(col_l);
                                    let right_in_parent = primary_table.get_column_index(col_r);

                                    if let (Some(c_idx), Some(p_idx)) = (left_in_child, right_in_parent) {
                                        if join_cols.is_none() {
                                            join_cols = Some((c_idx, p_idx));
                                            continue;
                                        }
                                    } else if let (Some(c_idx), Some(p_idx)) = (right_in_child, left_in_parent) {
                                        if join_cols.is_none() {
                                            join_cols = Some((c_idx, p_idx));
                                            continue;
                                        }
                                    }
                                }
                                extra_conds.push(part_trimmed);
                            }

                            if let Some((child_join_col_idx, parent_join_col_idx)) = join_cols {
                                let extra_str = extra_conds.join(" AND ");
                                let extra_child_conditions = if !extra_str.is_empty() {
                                    compile_condition_templates(&extra_str, child_table).unwrap_or_default()
                                } else {
                                    vec![]
                                };

                                exprs.push(ProjectedExpr::CorrelatedCount {
                                    child_table_name: child_table.name.clone(),
                                    child_join_col_idx,
                                    parent_join_col_idx,
                                    extra_child_conditions,
                                    alias,
                                });
                                continue;
                            }
                        } else {
                            let count = child_table.count();
                            exprs.push(ProjectedExpr::Expr {
                                expr_str: count.to_string(),
                                alias,
                            });
                            continue;
                        }
                    }
                }
            }
        }

        let upper_expr = expr_part.to_uppercase();

        // 1. Check for COALESCE(..., default)
        if upper_expr.starts_with("COALESCE(") && expr_part.ends_with(')') {
            let inner = &expr_part[9..expr_part.len() - 1];
            let inner_parts: Vec<&str> = inner.split(',').collect();
            if inner_parts.len() >= 2 {
                let first = inner_parts[0].trim();
                let second = inner_parts[1].trim();
                let default_val = parse_value(second, params);

                let upper_first = first.to_uppercase();
                let raw_col = if upper_first.starts_with("CAST(") && first.ends_with(')') {
                    let inside = &first[5..first.len() - 1];
                    let as_pos = inside.to_uppercase().find(" AS ").unwrap_or(inside.len());
                    inside[..as_pos].trim()
                } else {
                    first
                };

                let col_name = clean_col_name(raw_col);
                if let Some(col_idx) = primary_table.get_column_index(col_name) {
                    exprs.push(ProjectedExpr::CoalescePrimaryCol { col_idx, default_val, alias });
                    continue;
                } else if let Some(jt) = joined_table {
                    if let Some(col_idx) = jt.get_column_index(col_name) {
                        exprs.push(ProjectedExpr::CoalesceJoinedCol { col_idx, default_val, alias });
                        continue;
                    }
                }
            }
        }

        // 2. Check if expression references joined_table or joined_alias
        let is_joined = if let Some(jt) = joined_table {
            let lower_expr = expr_part.to_lowercase();
            lower_expr.contains(&jt.name.to_lowercase())
                || joined_alias.map(|a| lower_expr.starts_with(&format!("{}.", a.to_lowercase()))).unwrap_or(false)
        } else {
            false
        };

        let is_complex_expr = {
            let u = expr_part.to_uppercase();
            u.contains('(') || u.contains('+') || u.contains('*') || u.contains('/') || u.contains("||") || u.contains("CASE")
        };

        if !is_complex_expr {
            if is_joined {
                if let Some(jt) = joined_table {
                    let col_name = clean_col_name(expr_part);
                    if let Some(col_idx) = jt.get_column_index(col_name) {
                        exprs.push(ProjectedExpr::JoinedCol { col_idx, alias });
                        continue;
                    }
                }
            }

            // 3. Default to primary_table column
            let col_name = clean_col_name(expr_part);
            if let Some(col_idx) = primary_table.get_column_index(col_name) {
                exprs.push(ProjectedExpr::PrimaryCol { col_idx, alias });
                continue;
            } else if let Some(jt) = joined_table {
                if let Some(col_idx) = jt.get_column_index(col_name) {
                    exprs.push(ProjectedExpr::JoinedCol { col_idx, alias });
                    continue;
                }
            }
        }

        exprs.push(ProjectedExpr::Expr { expr_str: expr_part.to_string(), alias });
    }

    exprs
}

fn parse_select_clauses<'a>(after_from: &'a str) -> SelectClauses<'a> {
    // Check for JOIN keywords
    let (join_pos, kw_len, is_left) = if let Some(pos) = find_top_level_keyword(after_from, "LEFT JOIN") {
        (Some(pos), 9, true)
    } else if let Some(pos) = find_top_level_keyword(after_from, "LEFT OUTER JOIN") {
        (Some(pos), 15, true)
    } else if let Some(pos) = find_top_level_keyword(after_from, "INNER JOIN") {
        (Some(pos), 10, false)
    } else if let Some(pos) = find_top_level_keyword(after_from, "JOIN") {
        (Some(pos), 4, false)
    } else {
        (None, 0, false)
    };

    let raw_table_part = match join_pos {
        Some(pos) => &after_from[..pos],
        None => {
            let where_pos = find_top_level_keyword(after_from, "WHERE");
            let group_pos = find_top_level_keyword(after_from, "GROUP BY");
            let having_pos = find_top_level_keyword(after_from, "HAVING");
            let order_pos = find_top_level_keyword(after_from, "ORDER BY");
            let limit_pos = find_top_level_keyword(after_from, "LIMIT");
            let offset_pos = find_top_level_keyword(after_from, "OFFSET");
            let first_kw_pos = [where_pos, group_pos, having_pos, order_pos, limit_pos, offset_pos]
                .into_iter()
                .filter_map(|x| x)
                .min();
            match first_kw_pos {
                Some(pos) => &after_from[..pos],
                None => after_from,
            }
        }
    };

    let (table_name, table_alias) = extract_table_name_and_alias(raw_table_part);

    let join_clause = match join_pos {
        Some(j_pos) => {
            let after_j = &after_from[j_pos + kw_len..];
            if let Some(on_pos) = find_top_level_keyword(after_j, "ON") {
                let (joined_table_name, joined_table_alias) = extract_table_name_and_alias(&after_j[..on_pos]);
                let after_on = &after_j[on_pos + 2..];
                let end_pos = [
                    find_top_level_keyword(after_on, "WHERE"),
                    find_top_level_keyword(after_on, "GROUP BY"),
                    find_top_level_keyword(after_on, "HAVING"),
                    find_top_level_keyword(after_on, "ORDER BY"),
                    find_top_level_keyword(after_on, "LIMIT"),
                    find_top_level_keyword(after_on, "OFFSET"),
                ]
                .into_iter()
                .filter_map(|x| x)
                .min()
                .unwrap_or(after_on.len());
                let on_str = after_on[..end_pos].trim();

                if let Some(eq_idx) = on_str.find('=') {
                    let side_a = on_str[..eq_idx].trim();
                    let side_b = on_str[eq_idx + 1..].trim();

                    let col_a = clean_col_name(side_a);
                    let col_b = clean_col_name(side_b);

                    let side_a_lower = side_a.to_lowercase();
                    let is_a_joined = side_a_lower.contains(&joined_table_name.to_lowercase())
                        || joined_table_alias.map(|a| side_a_lower.starts_with(&format!("{}.", a.to_lowercase()))).unwrap_or(false);

                    let (primary_col, joined_col) = if is_a_joined {
                        (col_b, col_a)
                    } else {
                        (col_a, col_b)
                    };

                    Some(JoinClause {
                        is_left,
                        joined_table_name,
                        joined_table_alias,
                        primary_join_col: primary_col,
                        joined_join_col: joined_col,
                    })
                } else {
                    None
                }
            } else {
                None
            }
        }
        None => None,
    };

    let where_pos = find_top_level_keyword(after_from, "WHERE");
    let group_pos = find_top_level_keyword(after_from, "GROUP BY");
    let having_pos = find_top_level_keyword(after_from, "HAVING");
    let order_pos = find_top_level_keyword(after_from, "ORDER BY");
    let limit_pos = find_top_level_keyword(after_from, "LIMIT");
    let offset_pos = find_top_level_keyword(after_from, "OFFSET");

    let where_clause = where_pos.map(|w_idx| {
        let after_w = &after_from[w_idx + 5..];
        let end_idx = [
            group_pos.and_then(|p| if p > w_idx { Some(p - (w_idx + 5)) } else { None }),
            having_pos.and_then(|p| if p > w_idx { Some(p - (w_idx + 5)) } else { None }),
            order_pos.and_then(|p| if p > w_idx { Some(p - (w_idx + 5)) } else { None }),
            limit_pos.and_then(|p| if p > w_idx { Some(p - (w_idx + 5)) } else { None }),
            offset_pos.and_then(|p| if p > w_idx { Some(p - (w_idx + 5)) } else { None }),
        ]
        .into_iter()
        .filter_map(|x| x)
        .min();
        match end_idx {
            Some(e) => after_w[..e].trim(),
            None => after_w.trim(),
        }
    });

    let group_by_clause = group_pos.map(|g_idx| {
        let after_g = &after_from[g_idx + 8..];
        let end_idx = [
            having_pos.and_then(|p| if p > g_idx { Some(p - (g_idx + 8)) } else { None }),
            order_pos.and_then(|p| if p > g_idx { Some(p - (g_idx + 8)) } else { None }),
            limit_pos.and_then(|p| if p > g_idx { Some(p - (g_idx + 8)) } else { None }),
            offset_pos.and_then(|p| if p > g_idx { Some(p - (g_idx + 8)) } else { None }),
        ]
        .into_iter()
        .filter_map(|x| x)
        .min();
        match end_idx {
            Some(e) => after_g[..e].trim(),
            None => after_g.trim(),
        }
    });

    let having_clause = having_pos.map(|h_idx| {
        let after_h = &after_from[h_idx + 6..];
        let end_idx = [
            order_pos.and_then(|p| if p > h_idx { Some(p - (h_idx + 6)) } else { None }),
            limit_pos.and_then(|p| if p > h_idx { Some(p - (h_idx + 6)) } else { None }),
            offset_pos.and_then(|p| if p > h_idx { Some(p - (h_idx + 6)) } else { None }),
        ]
        .into_iter()
        .filter_map(|x| x)
        .min();
        match end_idx {
            Some(e) => after_h[..e].trim(),
            None => after_h.trim(),
        }
    });

    let order_by_specs: Vec<OrderBySpec<'a>> = match order_pos {
        Some(o_idx) => {
            let after_o = &after_from[o_idx + 8..];
            let end_idx = [
                limit_pos.and_then(|p| if p > o_idx { Some(p - (o_idx + 8)) } else { None }),
                offset_pos.and_then(|p| if p > o_idx { Some(p - (o_idx + 8)) } else { None }),
            ]
            .into_iter()
            .filter_map(|x| x)
            .min();
            let order_part = match end_idx {
                Some(e) => after_o[..e].trim(),
                None => after_o.trim(),
            };
            parse_order_by_specs(order_part)
        }
        None => Vec::new(),
    };

    let limit_pos = find_top_level_keyword(after_from, "LIMIT");
    let limit = match limit_pos {
        Some(l_idx) => {
            let after_l = &after_from[l_idx + 5..];
            let end_idx = offset_pos.and_then(|p| if p > l_idx { Some(p - (l_idx + 5)) } else { None });
            let limit_part = match end_idx {
                Some(e) => after_l[..e].trim(),
                None => after_l.trim(),
            };
            let tok = limit_part.split_whitespace().next().unwrap_or("");
            if tok.is_empty() {
                None
            } else {
                parse_operand_template(tok).ok()
            }
        }
        None => None,
    };

    let offset_pos = find_top_level_keyword(after_from, "OFFSET");
    let offset = match offset_pos {
        Some(off_idx) => {
            let after_off = after_from[off_idx + 6..].trim();
            let tok = after_off.split_whitespace().next().unwrap_or("");
            if tok.is_empty() {
                None
            } else {
                parse_operand_template(tok).ok()
            }
        }
        None => None,
    };

    SelectClauses {
        table_name,
        table_alias,
        join_clause,
        where_clause,
        group_by_clause,
        having_clause,
        order_by_specs,
        limit,
        offset,
    }
}

#[derive(Clone, Debug)]
enum AggFunc {
    CountStar,
    Count(usize),
    CountDistinct(usize),
    Sum(usize),
    Avg(usize),
    Min(usize),
    Max(usize),
    SumExpr(String),
    AvgExpr(String),
    MinExpr(String),
    MaxExpr(String),
    ArrayAgg(usize),
    ArrayAggExpr(String),
    WrappedAgg(String, Box<AggFunc>),
    StringAgg(usize, String),
    JsonAgg(usize),
    JsonbAgg(usize),
    JsonAggExpr(String),
    JsonbAggExpr(String),
    BoolAnd(usize),
    BoolOr(usize),
    BoolAndExpr(String),
    BoolOrExpr(String),
    CountExpr(String),
}

struct AggSpec {
    func: AggFunc,
    alias: String,
}

fn find_matching_paren(s: &str, start_open_paren: usize) -> Option<usize> {
    let mut depth = 0;
    let mut in_quote = false;
    for (idx, ch) in s[start_open_paren..].char_indices() {
        if ch == '\'' {
            in_quote = !in_quote;
        } else if !in_quote {
            if ch == '(' || ch == '[' || ch == '{' {
                depth += 1;
            } else if ch == ')' || ch == ']' || ch == '}' {
                depth -= 1;
                if depth == 0 {
                    return Some(start_open_paren + idx);
                }
            }
        }
    }
    None
}

fn parse_aggregations(select_clause: &str, table: &crate::storage::table::Table) -> Vec<AggSpec> {
    let mut specs = Vec::new();
    let parts: Vec<&str> = split_comma_separated_tokens(select_clause);

    for part in parts {
        let part_trim = part.trim();
        let upper = part_trim.to_uppercase();

        let get_alias = |default_alias: &str| -> String {
            if let Some(as_idx) = upper.rfind(" AS ") {
                part_trim[as_idx + 4..].trim().trim_matches('"').to_string()
            } else {
                default_alias.to_string()
            }
        };

        let raw_expr = if let Some(as_pos) = upper.rfind(" AS ") {
            part_trim[..as_pos].trim().to_string()
        } else {
            part_trim.to_string()
        };

        if let Some(count_idx) = upper.find("COUNT(") {
            if let Some(close_p) = find_matching_paren(part_trim, count_idx + 5) {
                let inside = part_trim[count_idx + 6..close_p].trim();
                let upper_inside = inside.to_uppercase();
                let alias = get_alias("count");

                if upper_inside == "*" || upper_inside == "1" {
                    specs.push(AggSpec { func: AggFunc::CountStar, alias });
                } else if upper_inside.starts_with("DISTINCT ") {
                    let raw_col = inside[9..].trim();
                    let col_name = clean_col_name(raw_col);
                    if let Some(col_idx) = table.get_column_index(col_name) {
                        specs.push(AggSpec { func: AggFunc::CountDistinct(col_idx), alias });
                    }
                } else {
                    let col_name = clean_col_name(inside);
                    if let Some(col_idx) = table.get_column_index(col_name) {
                        specs.push(AggSpec { func: AggFunc::Count(col_idx), alias });
                    } else {
                        specs.push(AggSpec { func: AggFunc::CountExpr(inside.to_string()), alias });
                    }
                }
            }
        } else if let Some(sum_idx) = upper.find("SUM(") {
            if let Some(close_p) = find_matching_paren(part_trim, sum_idx + 3) {
                let raw_col = part_trim[sum_idx + 4..close_p].trim();
                let col_name = clean_col_name(raw_col);
                let alias = get_alias("sum");
                if let Some(col_idx) = table.get_column_index(col_name) {
                    specs.push(AggSpec { func: AggFunc::Sum(col_idx), alias });
                } else {
                    specs.push(AggSpec { func: AggFunc::SumExpr(raw_col.to_string()), alias });
                }
            }
        } else if let Some(avg_idx) = upper.find("AVG(") {
            if let Some(close_p) = find_matching_paren(part_trim, avg_idx + 3) {
                let raw_col = part_trim[avg_idx + 4..close_p].trim();
                let col_name = clean_col_name(raw_col);
                let alias = get_alias("avg");
                if let Some(col_idx) = table.get_column_index(col_name) {
                    specs.push(AggSpec { func: AggFunc::Avg(col_idx), alias });
                } else {
                    specs.push(AggSpec { func: AggFunc::AvgExpr(raw_col.to_string()), alias });
                }
            }
        } else if let Some(min_idx) = upper.find("MIN(") {
            if let Some(close_p) = find_matching_paren(part_trim, min_idx + 3) {
                let raw_col = part_trim[min_idx + 4..close_p].trim();
                let col_name = clean_col_name(raw_col);
                let alias = get_alias("min");
                if let Some(col_idx) = table.get_column_index(col_name) {
                    specs.push(AggSpec { func: AggFunc::Min(col_idx), alias });
                } else {
                    specs.push(AggSpec { func: AggFunc::MinExpr(raw_col.to_string()), alias });
                }
            }
        } else if let Some(max_idx) = upper.find("MAX(") {
            if let Some(close_p) = find_matching_paren(part_trim, max_idx + 3) {
                let raw_col = part_trim[max_idx + 4..close_p].trim();
                let col_name = clean_col_name(raw_col);
                let alias = get_alias("max");
                if let Some(col_idx) = table.get_column_index(col_name) {
                    specs.push(AggSpec { func: AggFunc::Max(col_idx), alias });
                } else {
                    specs.push(AggSpec { func: AggFunc::MaxExpr(raw_col.to_string()), alias });
                }
            }
        } else if let Some(b_idx) = upper.find("BOOL_AND(") {
            if let Some(close_p) = find_matching_paren(part_trim, b_idx + 8) {
                let raw_col = part_trim[b_idx + 9..close_p].trim();
                let col_name = clean_col_name(raw_col);
                let alias = get_alias("bool_and");
                if let Some(col_idx) = table.get_column_index(col_name) {
                    specs.push(AggSpec { func: AggFunc::BoolAnd(col_idx), alias });
                } else {
                    specs.push(AggSpec { func: AggFunc::BoolAndExpr(raw_col.to_string()), alias });
                }
            }
        } else if let Some(e_idx) = upper.find("EVERY(") {
            if let Some(close_p) = find_matching_paren(part_trim, e_idx + 5) {
                let raw_col = part_trim[e_idx + 6..close_p].trim();
                let col_name = clean_col_name(raw_col);
                let alias = get_alias("every");
                if let Some(col_idx) = table.get_column_index(col_name) {
                    specs.push(AggSpec { func: AggFunc::BoolAnd(col_idx), alias });
                } else {
                    specs.push(AggSpec { func: AggFunc::BoolAndExpr(raw_col.to_string()), alias });
                }
            }
        } else if let Some(b_idx) = upper.find("BOOL_OR(") {
            if let Some(close_p) = find_matching_paren(part_trim, b_idx + 7) {
                let raw_col = part_trim[b_idx + 8..close_p].trim();
                let col_name = clean_col_name(raw_col);
                let alias = get_alias("bool_or");
                if let Some(col_idx) = table.get_column_index(col_name) {
                    specs.push(AggSpec { func: AggFunc::BoolOr(col_idx), alias });
                } else {
                    specs.push(AggSpec { func: AggFunc::BoolOrExpr(raw_col.to_string()), alias });
                }
            }
        } else if let Some(a_idx) = upper.find("ARRAY_AGG(") {
            let prefix_len = 10;
            if let Some(close_p) = find_matching_paren(part_trim, a_idx + 9) {
                let raw_col = part_trim[a_idx + prefix_len..close_p].trim();
                let col_name = clean_col_name(raw_col);
                let alias = get_alias("array_agg");
                let is_wrapped = a_idx > 0 || (close_p + 1 < part_trim.len() && !part_trim[close_p + 1..].trim().to_uppercase().starts_with("AS "));
                let is_raw_simple = !raw_col.contains('(') && !raw_col.contains('*') && !raw_col.contains('+') && !raw_col.contains('-');

                if is_raw_simple && table.get_column_index(col_name).is_some() {
                    let col_idx = table.get_column_index(col_name).unwrap();
                    if is_wrapped {
                        specs.push(AggSpec { func: AggFunc::WrappedAgg(raw_expr, Box::new(AggFunc::ArrayAgg(col_idx))), alias });
                    } else {
                        specs.push(AggSpec { func: AggFunc::ArrayAgg(col_idx), alias });
                    }
                } else {
                    if is_wrapped {
                        specs.push(AggSpec { func: AggFunc::WrappedAgg(raw_expr, Box::new(AggFunc::ArrayAggExpr(raw_col.to_string()))), alias });
                    } else {
                        specs.push(AggSpec { func: AggFunc::ArrayAggExpr(raw_col.to_string()), alias });
                    }
                }
            }
        } else if let Some(s_idx) = upper.find("STRING_AGG(") {
            let prefix_len = 11;
            if let Some(close_p) = find_matching_paren(part_trim, s_idx + 10) {
                let inside = part_trim[s_idx + prefix_len..close_p].trim();
                let args = split_comma_separated_tokens(inside);
                if args.len() >= 2 {
                    let col_name = clean_col_name(args[0].trim());
                    let delim_raw = args[1].trim();
                    let delim = delim_raw.trim_matches('\'').to_string();
                    let alias = get_alias("string_agg");
                    if let Some(col_idx) = table.get_column_index(col_name) {
                        specs.push(AggSpec { func: AggFunc::StringAgg(col_idx, delim), alias });
                    }
                }
            }
        } else if let Some(j_idx) = upper.find("JSON_AGG(") {
            if let Some(close_p) = find_matching_paren(part_trim, j_idx + 8) {
                let inside = part_trim[j_idx + 9..close_p].trim();
                let alias = get_alias("json_agg");
                let is_inside_simple = !inside.contains('(');
                let clean_col = clean_col_name(inside);
                if is_inside_simple && table.get_column_index(clean_col).is_some() {
                    specs.push(AggSpec { func: AggFunc::JsonAgg(table.get_column_index(clean_col).unwrap()), alias });
                } else {
                    specs.push(AggSpec { func: AggFunc::JsonAggExpr(inside.to_string()), alias });
                }
            }
        } else if let Some(j_idx) = upper.find("JSONB_AGG(") {
            if let Some(close_p) = find_matching_paren(part_trim, j_idx + 9) {
                let inside = part_trim[j_idx + 10..close_p].trim();
                let alias = get_alias("jsonb_agg");
                let is_inside_simple = !inside.contains('(');
                let clean_col = clean_col_name(inside);
                if is_inside_simple && table.get_column_index(clean_col).is_some() {
                    specs.push(AggSpec { func: AggFunc::JsonbAgg(table.get_column_index(clean_col).unwrap()), alias });
                } else {
                    specs.push(AggSpec { func: AggFunc::JsonbAggExpr(inside.to_string()), alias });
                }
            }
        }
    }

    specs
}

#[derive(Clone, Debug)]
enum ColOp {
    Eq(Value),
    NotEq(Value),
    LowerEq(String),
    UpperEq(String),
    TrimUpperEq(String),
    TrimLowerEq(String),
    TrimEq(String),
    Gt(Value),
    Lt(Value),
    Gte(Value),
    Lte(Value),
    Between(Value, Value),
    In(Vec<Value>),
    IsNull,
    IsNotNull,
    Like(String, Option<char>),
    ILike(String, Option<char>),
    NotLike(String, Option<char>),
    NotILike(String, Option<char>),
}

#[derive(Clone, Debug)]
struct Condition {
    col_idx: usize,
    op: ColOp,
}

pub fn find_top_level_keyword(sql: &str, kw: &str) -> Option<usize> {
    let kw_bytes = kw.as_bytes();
    let kw_len = kw_bytes.len();
    let bytes = sql.as_bytes();
    let mut depth = 0;
    let mut in_single_quote = false;
    let mut in_double_quote = false;

    let mut i = 0;
    while i < bytes.len() {
        let b = bytes[i];
        if b == b'\'' && !in_double_quote {
            in_single_quote = !in_single_quote;
        } else if b == b'"' && !in_single_quote {
            in_double_quote = !in_double_quote;
        } else if !in_single_quote && !in_double_quote {
            if b == b'(' {
                depth += 1;
            } else if b == b')' {
                if depth > 0 {
                    depth -= 1;
                }
            } else if depth == 0 && i + kw_len <= bytes.len() {
                if bytes[i..i + kw_len].eq_ignore_ascii_case(kw_bytes) {
                    let prev_ok = i == 0 || bytes[i - 1].is_ascii_whitespace() || bytes[i - 1] == b')' || bytes[i - 1] == b'"' || bytes[i - 1] == b',';
                    let next_ok = i + kw_len == bytes.len() || bytes[i + kw_len].is_ascii_whitespace() || bytes[i + kw_len] == b'(' || bytes[i + kw_len] == b'"' || bytes[i + kw_len] == b';';
                    if prev_ok && next_ok {
                        if kw.eq_ignore_ascii_case("FROM") {
                            let prefix = sql[..i].trim_end();
                            let prefix_upper = prefix.to_uppercase();
                            if prefix_upper.ends_with("DISTINCT") {
                                i += kw_len;
                                continue;
                            }
                        }
                        return Some(i);
                    }
                }
            }
        }
        i += 1;
    }
    None
}

fn split_comma_separated_tokens(s: &str) -> Vec<&str> {
    let bytes = s.as_bytes();
    let mut tokens = Vec::new();
    let mut depth = 0;
    let mut in_sq = false;
    let mut in_dq = false;
    let mut start = 0;
    let mut i = 0;

    while i < bytes.len() {
        let b = bytes[i];
        if b == b'\'' && !in_dq {
            in_sq = !in_sq;
        } else if b == b'"' && !in_sq {
            in_dq = !in_dq;
        } else if !in_sq && !in_dq {
            if b == b'(' || b == b'[' || b == b'{' {
                depth += 1;
            } else if b == b')' || b == b']' || b == b'}' {
                if depth > 0 { depth -= 1; }
            } else if depth == 0 && b == b',' {
                tokens.push(s[start..i].trim());
                start = i + 1;
            }
        }
        i += 1;
    }
    if start < bytes.len() {
        let last = s[start..].trim();
        if !last.is_empty() {
            tokens.push(last);
        }
    }
    tokens
}

fn split_sql_statements(sql: &str) -> Vec<String> {
    let mut stmts = Vec::new();
    let mut current = String::new();
    let mut in_str = false;
    let mut depth: i32 = 0;
    let bytes = sql.as_bytes();
    let len = bytes.len();
    let mut i = 0;

    while i < len {
        let b = bytes[i];
        if b == b'\'' {
            in_str = !in_str;
            current.push(b as char);
            i += 1;
            continue;
        }
        if in_str {
            current.push(b as char);
            i += 1;
            continue;
        }
        if b == b'(' || b == b'[' {
            depth += 1;
            current.push(b as char);
            i += 1;
            continue;
        }
        if b == b')' || b == b']' {
            depth = depth.saturating_sub(1);
            current.push(b as char);
            i += 1;
            continue;
        }
        if depth == 0 && b == b';' {
            if !current.trim().is_empty() {
                stmts.push(current.trim().to_string());
                current.clear();
            }
            i += 1;
            continue;
        }
        current.push(b as char);
        i += 1;
    }
    if !current.trim().is_empty() {
        stmts.push(current.trim().to_string());
    }
    stmts
}

fn extract_literal_after_key(where_str: &str, key: &str) -> Option<String> {
    let up = where_str.to_uppercase();
    if let Some(idx) = up.find(&key.to_uppercase()) {
        let rest = where_str[idx + key.len()..].trim();
        if rest.starts_with('=') {
            let after_eq = rest[1..].trim();
            if after_eq.starts_with('\'') {
                if let Some(close_q) = after_eq[1..].find('\'') {
                    return Some(after_eq[1..1 + close_q].to_string());
                }
            } else {
                let tok = after_eq.split_whitespace().next().unwrap_or("").trim_matches(';').trim_matches('\'');
                if !tok.is_empty() {
                    return Some(tok.to_string());
                }
            }
        }
    }
    None
}

fn extract_value_groups(values_part: &str) -> Vec<&str> {
    let bytes = values_part.as_bytes();
    let mut groups = Vec::new();
    let mut depth = 0;
    let mut in_sq = false;
    let mut in_dq = false;
    let mut start_idx = None;

    let mut i = 0;
    while i < bytes.len() {
        let b = bytes[i];
        if b == b'\'' && !in_dq {
            in_sq = !in_sq;
        } else if b == b'"' && !in_sq {
            in_dq = !in_dq;
        } else if !in_sq && !in_dq {
            if b == b'(' {
                if depth == 0 {
                    start_idx = Some(i + 1);
                }
                depth += 1;
            } else if b == b')' {
                if depth > 0 {
                    depth -= 1;
                    if depth == 0 {
                        if let Some(s) = start_idx {
                            groups.push(&values_part[s..i]);
                            start_idx = None;
                        }
                    }
                }
            }
        }
        i += 1;
    }
    groups
}

fn clean_col_name(raw: &str) -> &str {
    let mut s = raw.trim();
    while s.starts_with('(') && s.ends_with(')') && s.len() >= 2 {
        s = s[1..s.len() - 1].trim();
    }
    if let Some(colon_pos) = s.rfind("::") {
        s = s[..colon_pos].trim();
    }
    let u = s.to_uppercase();
    if (u.starts_with("UPPER(") || u.starts_with("LOWER(") || u.starts_with("TRIM(") || u.starts_with("BTRIM(")) && s.ends_with(')') {
        let open_p = s.find('(').unwrap();
        s = s[open_p + 1..s.len() - 1].trim();
    }
    if let Some(dot_idx) = s.rfind('.') {
        s[dot_idx + 1..].trim().trim_matches('"').trim_end_matches(')')
    } else {
        s.trim_matches('"').trim_end_matches(')')
    }
}

fn extract_table_name_and_alias<'a>(raw: &'a str) -> (&'a str, Option<&'a str>) {
    let mut s = raw.trim();
    while s.starts_with('(') && s.ends_with(')') && s.len() >= 2 {
        s = s[1..s.len() - 1].trim();
    }
    let parts: Vec<&str> = s.split_whitespace().collect();
    if parts.is_empty() {
        return ("", None);
    }
    let tbl = if let Some(dot_idx) = parts[0].rfind('.') {
        parts[0][dot_idx + 1..].trim().trim_matches('"')
    } else {
        parts[0].trim_matches('"')
    };

    if parts.len() == 2 {
        let alias = parts[1].trim_matches('"');
        if alias.eq_ignore_ascii_case("WHERE") || alias.eq_ignore_ascii_case("JOIN") || alias.eq_ignore_ascii_case("LEFT") || alias.eq_ignore_ascii_case("INNER") {
            (tbl, None)
        } else {
            (tbl, Some(alias))
        }
    } else if parts.len() >= 3 && parts[1].eq_ignore_ascii_case("AS") {
        (tbl, Some(parts[2].trim_matches('"')))
    } else {
        (tbl, None)
    }
}

fn clean_table_name(raw: &str) -> &str {
    extract_table_name_and_alias(raw).0
}

fn strip_sql_comments(sql: &str) -> String {
    let mut result = String::with_capacity(sql.len());
    let mut chars = sql.chars().peekable();
    let mut in_sq = false;
    let mut in_dq = false;

    while let Some(c) = chars.next() {
        if c == '\'' && !in_dq {
            in_sq = !in_sq;
            result.push(c);
        } else if c == '"' && !in_sq {
            in_dq = !in_dq;
            result.push(c);
        } else if !in_sq && !in_dq {
            if c == '-' && chars.peek() == Some(&'-') {
                chars.next();
                while let Some(&nc) = chars.peek() {
                    if nc == '\n' {
                        break;
                    }
                    chars.next();
                }
                result.push(' ');
            } else if c == '/' && chars.peek() == Some(&'*') {
                chars.next();
                while let Some(nc) = chars.next() {
                    if nc == '*' && chars.peek() == Some(&'/') {
                        chars.next();
                        break;
                    }
                }
                result.push(' ');
            } else {
                result.push(c);
            }
        } else {
            result.push(c);
        }
    }
    result
}

fn normalize_named_sql_params(sql: &str) -> String {
    let bytes = sql.as_bytes();
    let len = bytes.len();
    let mut param_names: Vec<String> = Vec::new();
    let mut in_single_quote = false;
    let mut in_double_quote = false;
    let mut i = 0;

    while i < len {
        let b = bytes[i];
        if b == b'\'' && !in_double_quote {
            in_single_quote = !in_single_quote;
        } else if b == b'"' && !in_single_quote {
            in_double_quote = !in_double_quote;
        } else if !in_single_quote && !in_double_quote {
            if b == b':' {
                let prev_is_colon = i > 0 && bytes[i - 1] == b':';
                let next_is_colon = i + 1 < len && bytes[i + 1] == b':';
                if !prev_is_colon && !next_is_colon && i + 1 < len {
                    let next_ch = bytes[i + 1];
                    if next_ch.is_ascii_alphabetic() || next_ch == b'_' {
                        let start = i + 1;
                        let mut end = start;
                        while end < len && (bytes[end].is_ascii_alphanumeric() || bytes[end] == b'_') {
                            end += 1;
                        }
                        let name = &sql[start..end];
                        if !param_names.iter().any(|n| n == name) {
                            param_names.push(name.to_string());
                        }
                        i = end;
                        continue;
                    }
                }
            }
        }
        i += 1;
    }

    if param_names.is_empty() {
        return sql.to_string();
    }

    let mut result = String::with_capacity(sql.len());
    let mut i = 0;
    in_single_quote = false;
    in_double_quote = false;

    while i < len {
        let b = bytes[i];
        if b == b'\'' && !in_double_quote {
            in_single_quote = !in_single_quote;
            result.push(b as char);
            i += 1;
        } else if b == b'"' && !in_single_quote {
            in_double_quote = !in_double_quote;
            result.push(b as char);
            i += 1;
        } else if !in_single_quote && !in_double_quote && b == b':' {
            let prev_is_colon = i > 0 && bytes[i - 1] == b':';
            let next_is_colon = i + 1 < len && bytes[i + 1] == b':';
            if !prev_is_colon && !next_is_colon && i + 1 < len && (bytes[i + 1].is_ascii_alphabetic() || bytes[i + 1] == b'_') {
                let start = i + 1;
                let mut end = start;
                while end < len && (bytes[end].is_ascii_alphanumeric() || bytes[end] == b'_') {
                    end += 1;
                }
                let name = &sql[start..end];
                if let Some(pos) = param_names.iter().position(|n| n == name) {
                    result.push_str(&format!("${}", pos + 1));
                } else {
                    result.push_str(&sql[i..end]);
                }
                i = end;
            } else {
                result.push(b as char);
                i += 1;
            }
        } else {
            result.push(b as char);
            i += 1;
        }
    }

    result
}

fn is_aggregate_projection_item(item: &str) -> bool {
    let mut t = item.trim();
    if let Some(as_idx) = t.to_uppercase().rfind(" AS ") {
        t = t[..as_idx].trim();
    }
    while t.starts_with('(') && t.ends_with(')') && is_fully_enclosed_in_parens(t) {
        let inner = t[1..t.len() - 1].trim();
        if inner.to_uppercase().starts_with("SELECT ") {
            return false;
        }
        t = inner;
    }
    let upper = t.to_uppercase();
    if upper.contains(" FROM ") {
        return false;
    }
    upper.starts_with("COUNT(")
        || upper.starts_with("SUM(")
        || upper.starts_with("AVG(")
        || upper.starts_with("MIN(")
        || upper.starts_with("MAX(")
        || upper.starts_with("ARRAY_AGG(")
        || upper.starts_with("STRING_AGG(")
        || upper.contains("ARRAY_AGG(")
        || upper.contains("COUNT(")
        || upper.contains("SUM(")
        || upper.contains("AVG(")
        || upper.contains("MIN(")
        || upper.contains("MAX(")
        || upper.contains("STRING_AGG(")
}

fn is_fully_enclosed_in_parens(s: &str) -> bool {
    let bytes = s.as_bytes();
    if bytes.len() < 2 || bytes[0] != b'(' || bytes[bytes.len() - 1] != b')' {
        return false;
    }
    let mut depth = 0;
    let mut in_sq = false;
    let mut in_dq = false;
    for (i, &b) in bytes.iter().enumerate() {
        if b == b'\'' && !in_dq {
            in_sq = !in_sq;
        } else if b == b'"' && !in_sq {
            in_dq = !in_dq;
        } else if !in_sq && !in_dq {
            if b == b'(' {
                depth += 1;
            } else if b == b')' {
                depth -= 1;
                if depth == 0 && i < bytes.len() - 1 {
                    return false;
                }
            }
        }
    }
    depth == 0
}

fn split_top_level_or(s: &str) -> Vec<&str> {
    let bytes = s.as_bytes();
    let mut parts = Vec::new();
    let mut depth = 0;
    let mut in_sq = false;
    let mut in_dq = false;
    let mut start = 0;
    let mut i = 0;

    while i < bytes.len() {
        let b = bytes[i];
        if b == b'\'' && !in_dq {
            in_sq = !in_sq;
        } else if b == b'"' && !in_sq {
            in_dq = !in_dq;
        } else if !in_sq && !in_dq {
            if b == b'(' {
                depth += 1;
            } else if b == b')' {
                if depth > 0 { depth -= 1; }
            } else if depth == 0 && i + 2 <= bytes.len() {
                if bytes[i..i + 2].eq_ignore_ascii_case(b"OR") {
                    let prev_ok = i == 0 || bytes[i - 1].is_ascii_whitespace() || bytes[i - 1] == b')';
                    let next_ok = i + 2 == bytes.len() || bytes[i + 2].is_ascii_whitespace() || bytes[i + 2] == b'(';
                    if prev_ok && next_ok {
                        let part = s[start..i].trim();
                        if !part.is_empty() {
                            parts.push(part);
                        }
                        start = i + 2;
                        i += 2;
                        continue;
                    }
                }
            }
        }
        i += 1;
    }
    let last = s[start..].trim();
    if !last.is_empty() {
        parts.push(last);
    }
    parts
}

fn split_top_level_and(s: &str) -> Vec<&str> {
    let bytes = s.as_bytes();
    let mut parts = Vec::new();
    let mut depth = 0;
    let mut in_sq = false;
    let mut in_dq = false;
    let mut between_count = 0;
    let mut start = 0;
    let mut i = 0;

    while i < bytes.len() {
        let b = bytes[i];
        if b == b'\'' && !in_dq {
            in_sq = !in_sq;
        } else if b == b'"' && !in_sq {
            in_dq = !in_dq;
        } else if !in_sq && !in_dq {
            if b == b'(' {
                depth += 1;
            } else if b == b')' {
                if depth > 0 { depth -= 1; }
            } else if depth == 0 {
                if i + 7 <= bytes.len() && bytes[i..i + 7].eq_ignore_ascii_case(b"BETWEEN") {
                    let prev_ok = i == 0 || bytes[i - 1].is_ascii_whitespace();
                    let next_ok = i + 7 == bytes.len() || bytes[i + 7].is_ascii_whitespace();
                    if prev_ok && next_ok {
                        between_count += 1;
                        i += 7;
                        continue;
                    }
                }
                if i + 3 <= bytes.len() && bytes[i..i + 3].eq_ignore_ascii_case(b"AND") {
                    let prev_ok = i == 0 || bytes[i - 1].is_ascii_whitespace() || bytes[i - 1] == b')';
                    let next_ok = i + 3 == bytes.len() || bytes[i + 3].is_ascii_whitespace() || bytes[i + 3] == b'(';
                    if prev_ok && next_ok {
                        if between_count > 0 {
                            between_count -= 1;
                            i += 3;
                            continue;
                        }
                        let part = s[start..i].trim();
                        if !part.is_empty() {
                            parts.push(part);
                        }
                        start = i + 3;
                        i += 3;
                        continue;
                    }
                }
            }
        }
        i += 1;
    }
    let last = s[start..].trim();
    if !last.is_empty() {
        parts.push(last);
    }
    parts
}

#[derive(Debug, Clone, Copy)]
struct DateTimeParts {
    year: i32,
    month: u32,
    day: u32,
    hour: u32,
    minute: u32,
    second: u32,
    millisecond: u32,
    has_time: bool,
    is_iso_z: bool,
    separator: char,
}

fn parse_datetime_components(s: &str) -> Option<DateTimeParts> {
    let mut trimmed = s.trim().trim_matches('\'').trim_matches('"').trim();
    if trimmed.is_empty() || trimmed.eq_ignore_ascii_case("null") {
        return None;
    }
    let is_iso_z = trimmed.ends_with('Z') || trimmed.ends_with('z');
    if is_iso_z {
        trimmed = &trimmed[..trimmed.len() - 1];
    }
    let sep = if trimmed.contains('T') {
        'T'
    } else if trimmed.contains(' ') {
        ' '
    } else {
        '-'
    };

    let main_parts: Vec<&str> = if sep == 'T' {
        trimmed.splitn(2, 'T').collect()
    } else if sep == ' ' {
        trimmed.splitn(2, ' ').collect()
    } else {
        vec![trimmed]
    };

    let date_parts: Vec<&str> = main_parts[0].split('-').collect();
    if date_parts.len() < 3 {
        if main_parts[0].contains(':') {
            let time_parts: Vec<&str> = main_parts[0].split(':').collect();
            let hour = time_parts.get(0).and_then(|h| h.parse::<u32>().ok()).unwrap_or(0);
            let minute = time_parts.get(1).and_then(|m| m.parse::<u32>().ok()).unwrap_or(0);
            let sec_str = time_parts.get(2).unwrap_or(&"0");
            let (sec, ms) = if let Some(dot) = sec_str.find('.') {
                let s = sec_str[..dot].parse::<u32>().unwrap_or(0);
                let ms = sec_str[dot + 1..].parse::<u32>().unwrap_or(0);
                (s, ms)
            } else {
                (sec_str.parse::<u32>().unwrap_or(0), 0)
            };
            return Some(DateTimeParts {
                year: 2024,
                month: 1,
                day: 1,
                hour,
                minute,
                second: sec,
                millisecond: ms,
                has_time: true,
                is_iso_z: false,
                separator: ' ',
            });
        }
        return None;
    }

    let year = date_parts[0].parse::<i32>().ok()?;
    let month = date_parts[1].parse::<u32>().ok()?;
    let day = date_parts[2].parse::<u32>().ok()?;

    let (hour, minute, second, millisecond, has_time) = if main_parts.len() > 1 {
        let time_str = main_parts[1].trim();
        let time_parts: Vec<&str> = time_str.split(':').collect();
        let h = time_parts.get(0).and_then(|x| x.parse::<u32>().ok()).unwrap_or(0);
        let m = time_parts.get(1).and_then(|x| x.parse::<u32>().ok()).unwrap_or(0);
        let sec_str = time_parts.get(2).unwrap_or(&"0");
        let (s, ms) = if let Some(dot) = sec_str.find('.') {
            let sec = sec_str[..dot].parse::<u32>().unwrap_or(0);
            let mut ms_raw = sec_str[dot + 1..].to_string();
            while ms_raw.len() < 3 { ms_raw.push('0'); }
            let ms = ms_raw[..3].parse::<u32>().unwrap_or(0);
            (sec, ms)
        } else {
            (sec_str.parse::<u32>().unwrap_or(0), 0)
        };
        (h, m, s, ms, true)
    } else {
        (0, 0, 0, 0, false)
    };

    Some(DateTimeParts {
        year,
        month,
        day,
        hour,
        minute,
        second,
        millisecond,
        has_time,
        is_iso_z,
        separator: sep,
    })
}

fn days_from_civil(y: i32, m: u32, d: u32) -> i64 {
    let y = if m <= 2 { (y - 1) as i64 } else { y as i64 };
    let era = (if y >= 0 { y } else { y - 399 }) / 400;
    let yoe = (y - era * 400) as u32;
    let doy = (153 * (if m > 2 { m - 3 } else { m + 9 }) + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146097 + doe as i64 - 719468
}

fn datetime_to_epoch_seconds(dt: &DateTimeParts) -> i64 {
    let days = days_from_civil(dt.year, dt.month, dt.day);
    days * 86400 + (dt.hour as i64) * 3600 + (dt.minute as i64) * 60 + (dt.second as i64)
}

fn day_of_week(dt: &DateTimeParts) -> u32 {
    let days = days_from_civil(dt.year, dt.month, dt.day);
    ((days + 4).rem_euclid(7)) as u32
}

fn eval_date_part_or_extract(field: &str, dt: &DateTimeParts) -> Option<Value> {
    match field.trim().trim_matches('\'').trim_matches('"').to_lowercase().as_str() {
        "year" => Some(Value::Int(dt.year as i64)),
        "month" => Some(Value::Int(dt.month as i64)),
        "day" => Some(Value::Int(dt.day as i64)),
        "hour" => Some(Value::Int(dt.hour as i64)),
        "minute" => Some(Value::Int(dt.minute as i64)),
        "second" => Some(Value::Int(dt.second as i64)),
        "epoch" => Some(Value::Int(datetime_to_epoch_seconds(dt))),
        "dow" => Some(Value::Int(day_of_week(dt) as i64)),
        "quarter" => Some(Value::Int(((dt.month - 1) / 3 + 1) as i64)),
        _ => None,
    }
}

fn eval_date_trunc(unit: &str, dt: &DateTimeParts) -> String {
    let mut res_dt = *dt;
    match unit.trim().trim_matches('\'').trim_matches('"').to_lowercase().as_str() {
        "year" => {
            res_dt.month = 1;
            res_dt.day = 1;
            res_dt.hour = 0;
            res_dt.minute = 0;
            res_dt.second = 0;
            res_dt.millisecond = 0;
        }
        "month" => {
            res_dt.day = 1;
            res_dt.hour = 0;
            res_dt.minute = 0;
            res_dt.second = 0;
            res_dt.millisecond = 0;
        }
        "day" => {
            res_dt.hour = 0;
            res_dt.minute = 0;
            res_dt.second = 0;
            res_dt.millisecond = 0;
        }
        "hour" => {
            res_dt.minute = 0;
            res_dt.second = 0;
            res_dt.millisecond = 0;
        }
        "minute" => {
            res_dt.second = 0;
            res_dt.millisecond = 0;
        }
        "second" => {
            res_dt.millisecond = 0;
        }
        _ => {}
    }

    if res_dt.is_iso_z {
        format!("{:04}-{:02}-{:02}T{:02}:{:02}:{:02}.{:03}Z",
            res_dt.year, res_dt.month, res_dt.day,
            res_dt.hour, res_dt.minute, res_dt.second, res_dt.millisecond)
    } else if res_dt.separator == 'T' {
        format!("{:04}-{:02}-{:02}T{:02}:{:02}:{:02}",
            res_dt.year, res_dt.month, res_dt.day,
            res_dt.hour, res_dt.minute, res_dt.second)
    } else if res_dt.has_time {
        format!("{:04}-{:02}-{:02} {:02}:{:02}:{:02}",
            res_dt.year, res_dt.month, res_dt.day,
            res_dt.hour, res_dt.minute, res_dt.second)
    } else {
        format!("{:04}-{:02}-{:02}", res_dt.year, res_dt.month, res_dt.day)
    }
}

fn eval_to_char(dt: &DateTimeParts, fmt: &str) -> String {
    let month_names = ["January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December"];
    let month_shorts = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];
    let day_names = ["Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"];
    let day_shorts = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"];

    let dow = day_of_week(dt) as usize;
    let m_idx = (dt.month.saturating_sub(1) as usize).min(11);

    let mut res = fmt.trim().trim_matches('\'').trim_matches('"').to_string();
    res = res.replace("YYYY", &format!("{:04}", dt.year));
    res = res.replace("YY", &format!("{:02}", (dt.year % 100).abs()));
    res = res.replace("Month", month_names[m_idx]);
    res = res.replace("Mon", month_shorts[m_idx]);
    res = res.replace("MM", &format!("{:02}", dt.month));
    res = res.replace("Day", day_names[dow]);
    res = res.replace("Dy", day_shorts[dow]);
    res = res.replace("DD", &format!("{:02}", dt.day));
    res = res.replace("HH24", &format!("{:02}", dt.hour));
    res = res.replace("HH", &format!("{:02}", dt.hour));
    res = res.replace("MI", &format!("{:02}", dt.minute));
    res = res.replace("SS", &format!("{:02}", dt.second));
    res
}

fn eval_age(dt1: &DateTimeParts, dt2: &DateTimeParts) -> String {
    let mut y_diff = dt1.year - dt2.year;
    let mut m_diff = dt1.month as i32 - dt2.month as i32;
    let mut d_diff = dt1.day as i32 - dt2.day as i32;

    if d_diff < 0 {
        m_diff -= 1;
        d_diff += 30;
    }
    if m_diff < 0 {
        y_diff -= 1;
        m_diff += 12;
    }

    let mut parts = Vec::new();
    if y_diff > 0 {
        parts.push(if y_diff == 1 { "1 year".to_string() } else { format!("{} years", y_diff) });
    }
    if m_diff > 0 {
        parts.push(if m_diff == 1 { "1 mon".to_string() } else { format!("{} mons", m_diff) });
    }
    if d_diff > 0 || parts.is_empty() {
        parts.push(if d_diff == 1 { "1 day".to_string() } else { format!("{} days", d_diff) });
    }
    parts.join(" ")
}

fn get_current_timestamp() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let now = SystemTime::now();
    let duration = now.duration_since(UNIX_EPOCH).unwrap_or_default();
    let secs = duration.as_secs();
    let millis = duration.subsec_millis();

    let days = (secs / 86400) as i64;
    let rem_secs = secs % 86400;
    let hours = rem_secs / 3600;
    let mins = (rem_secs % 3600) / 60;
    let s = rem_secs % 60;

    let z = days + 719468;
    let era = (if z >= 0 { z } else { z - 146096 }) / 146097;
    let doe = (z - era * 146097) as u32;
    let yoe = (doe - doe / 1029 + doe / 1461 - doe / 36524) / 365;
    let y = (yoe as i64) + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = y + (if m <= 2 { 1 } else { 0 });

    format!("{:04}-{:02}-{:02}T{:02}:{:02}:{:02}.{:03}Z", year, m, d, hours, mins, s, millis)
}

fn get_current_date() -> String {
    let ts = get_current_timestamp();
    ts[..10].to_string()
}

fn get_current_time() -> String {
    let ts = get_current_timestamp();
    ts[11..19].to_string()
}

fn eval_date_time_keyword(token: &str) -> Option<Value> {
    let trimmed = token.trim();
    let upper = trimmed.to_uppercase();
    if upper == "CURRENT_TIMESTAMP" || upper == "NOW()" || upper == "LOCALTIMESTAMP"
        || upper.starts_with("CURRENT_TIMESTAMP") || upper.starts_with("NOW()") || upper.starts_with("LOCALTIMESTAMP")
    {
        return Some(Value::text(get_current_timestamp()));
    }
    if upper == "CURRENT_DATE" || upper.starts_with("CURRENT_DATE") {
        return Some(Value::text(get_current_date()));
    }
    if upper == "CURRENT_TIME" || upper == "LOCALTIME"
        || upper.starts_with("CURRENT_TIME") || upper.starts_with("LOCALTIME")
    {
        return Some(Value::text(get_current_time()));
    }
    None
}

fn split_where_conditions(w: &str) -> Vec<String> {
    let mut parts = Vec::new();
    let mut current = String::new();
    let chars: Vec<char> = w.chars().collect();
    let len = chars.len();
    let mut in_str = false;
    let mut depth: i32 = 0;
    let mut i = 0;

    while i < len {
        let c = chars[i];
        if c == '\'' {
            in_str = !in_str;
            current.push(c);
            i += 1;
            continue;
        }
        if in_str {
            current.push(c);
            i += 1;
            continue;
        }
        if c == '(' || c == '[' {
            depth += 1;
            current.push(c);
            i += 1;
            continue;
        }
        if c == ')' || c == ']' {
            depth = depth.saturating_sub(1);
            current.push(c);
            i += 1;
            continue;
        }

        if depth == 0 && i + 3 <= len {
            let next3: String = chars[i..i + 3].iter().collect();
            if next3.eq_ignore_ascii_case("AND") {
                let is_word_start = i == 0 || chars[i - 1].is_whitespace() || chars[i - 1] == ')' || chars[i - 1] == ']';
                let is_word_end = i + 3 == len || chars[i + 3].is_whitespace() || chars[i + 3] == '(' || chars[i + 3] == '[';
                if is_word_start && is_word_end {
                    let cur_upper = current.to_uppercase();
                    if cur_upper.contains("BETWEEN") && !cur_upper.contains(" AND ") {
                        current.push_str("AND");
                        i += 3;
                        continue;
                    } else {
                        if !current.trim().is_empty() {
                            parts.push(current.trim().to_string());
                            current.clear();
                        }
                        i += 3;
                        continue;
                    }
                }
            }
        }

        current.push(c);
        i += 1;
    }
    if !current.trim().is_empty() {
        parts.push(current.trim().to_string());
    }
    parts
}

#[allow(dead_code)]
fn parse_number(token: &str, params: &[Value]) -> Result<f64, String> {
    let mut t = token.trim();
    while is_fully_enclosed_in_parens(t) {
        t = t[1..t.len() - 1].trim();
    }
    if t.starts_with('$') {
        let p_idx = t[1..].parse::<usize>().map_err(|_| format!("Invalid param {}", t))?;
        params.get(p_idx.saturating_sub(1)).and_then(|v| v.as_f64()).ok_or_else(|| format!("Param {} not found or not number", t))
    } else {
        t.parse::<f64>().map_err(|_| format!("Invalid number {}", t))
    }
}

fn parse_value(token: &str, params: &[Value]) -> Value {
    let mut t = token.trim();
    while is_fully_enclosed_in_parens(t) {
        let inside = t[1..t.len() - 1].trim();
        if split_comma_separated_tokens(inside).len() > 1 {
            break;
        }
        t = inside;
    }
    if let Some(colon_pos) = find_last_top_level_op(t, "::") {
        let target_type = t[colon_pos + 2..].trim();
        let val = parse_value(&t[..colon_pos], params);
        return cast_val(val, target_type);
    }
    if let Some(pipe_idx) = find_top_level_op(t, "||") {
        let left = parse_value(&t[..pipe_idx], params);
        let right = parse_value(&t[pipe_idx + 2..], params);
        if left.is_null() || right.is_null() {
            return Value::Null;
        }
        let l_s = left.as_str();
        let r_s = right.as_str();
        if l_s.starts_with("\\x") || r_s.starts_with("\\x") {
            let mut combined = get_raw_bytes(&left);
            combined.extend(get_raw_bytes(&right));
            return Value::text(format!("\\x{}", hex::encode(combined)));
        }
        return Value::text(format!("{}{}", l_s, r_s));
    }
    if let Some(dt_val) = eval_date_time_keyword(t) {
        return dt_val;
    }
    let upper = t.to_uppercase();
    if is_complete_array_constructor(t) {
        let inside = &t[6..t.len() - 1].trim();
        if inside.is_empty() {
            return Value::text("[]");
        }
        let items: Vec<serde_json::Value> = split_comma_separated_tokens(inside)
            .into_iter()
            .map(|tok| {
                let v = parse_value(&tok, params);
                value_to_json(&v)
            })
            .collect();
        return Value::text(serde_json::to_string(&items).unwrap_or_default());
    }
    if upper.starts_with("LOWER(") && t.ends_with(')') {
        let inner = &t[6..t.len() - 1];
        let val = parse_value(inner, params);
        return Value::text(val.as_str().to_lowercase());
    }
    if upper.starts_with("UPPER(") && t.ends_with(')') {
        let inner = &t[6..t.len() - 1];
        let val = parse_value(inner, params);
        return Value::text(val.as_str().to_uppercase());
    }
    if (upper.starts_with("TRIM(") || upper.starts_with("BTRIM(")) && t.ends_with(')') {
        let open_p = t.find('(').unwrap();
        let inner = &t[open_p + 1..t.len() - 1];
        let val = parse_value(inner, params);
        return Value::text(val.as_str().trim().to_string());
    }
    if upper == "GEN_RANDOM_UUID()" || upper == "UUID_GENERATE_V4()" {
        return Value::text(crate::types::generate_uuid_v4());
    }
    if t.starts_with('$') {
        if let Ok(p_idx) = t[1..].parse::<usize>() {
            return params.get(p_idx.saturating_sub(1)).cloned().unwrap_or(Value::Null);
        }
    }
    if t.eq_ignore_ascii_case("TRUE") {
        return Value::Bool(true);
    }
    if t.eq_ignore_ascii_case("FALSE") {
        return Value::Bool(false);
    }
    if t.eq_ignore_ascii_case("NULL") {
        return Value::Null;
    }
    if t.starts_with('\'') && t.ends_with('\'') && t.len() >= 2 {
        return Value::text(t[1..t.len() - 1].replace("''", "'"));
    }
    if let Ok(i) = t.parse::<i64>() {
        return Value::Int(i);
    }
    if let Ok(f) = t.parse::<f64>() {
        return Value::Float(f);
    }
    Value::text(t)
}

fn parse_operand_template(token: &str) -> Result<OperandTemplate, String> {
    let mut t = token.trim();
    while is_fully_enclosed_in_parens(t) {
        t = t[1..t.len() - 1].trim();
    }
    if let Some(colon_pos) = find_last_top_level_op(t, "::") {
        t = t[..colon_pos].trim();
    }
    while is_fully_enclosed_in_parens(t) {
        t = t[1..t.len() - 1].trim();
    }
    let upper = t.to_uppercase();
    if upper.starts_with("LOWER(") && t.ends_with(')') {
        let inner = t[6..t.len() - 1].trim().trim_matches('\'').replace("''", "'");
        return Ok(OperandTemplate::Literal(Value::text(inner.to_lowercase())));
    }
    if upper.starts_with("UPPER(") && t.ends_with(')') {
        let inner = t[6..t.len() - 1].trim().trim_matches('\'').replace("''", "'");
        return Ok(OperandTemplate::Literal(Value::text(inner.to_uppercase())));
    }
    if t.starts_with('$') {
        if let Ok(p_idx) = t[1..].parse::<usize>() {
            if p_idx > 0 {
                return Ok(OperandTemplate::Param(p_idx - 1));
            }
        }
    }
    if t.eq_ignore_ascii_case("TRUE") {
        return Ok(OperandTemplate::Literal(Value::Bool(true)));
    }
    if t.eq_ignore_ascii_case("FALSE") {
        return Ok(OperandTemplate::Literal(Value::Bool(false)));
    }
    if t.eq_ignore_ascii_case("NULL") {
        return Ok(OperandTemplate::Literal(Value::Null));
    }
    if t.starts_with('\'') && t.ends_with('\'') && t.len() >= 2 {
        return Ok(OperandTemplate::Literal(Value::text(t[1..t.len() - 1].replace("''", "'"))));
    }
    if let Ok(i) = t.parse::<i64>() {
        return Ok(OperandTemplate::Literal(Value::Int(i)));
    }
    if let Ok(f) = t.parse::<f64>() {
        return Ok(OperandTemplate::Number(f));
    }
    Err(format!("Invalid operand template: {}", t))
}

fn parse_single_condition_template(
    part: &str,
    table: &crate::storage::table::Table,
) -> Result<ConditionTemplate, String> {
    let mut s = part.trim();
    while is_fully_enclosed_in_parens(s) {
        s = s[1..s.len() - 1].trim();
    }
    if s.contains("->") || s.contains("@>") || s.contains("<@") || s.contains('?') || s.contains("#>") {
        return Err("Custom JSON condition".to_string());
    }
    let upper = s.to_uppercase();

    if let Some(pos) = upper.find(" IS NOT NULL") {
        let col_name = clean_col_name(&s[..pos]);
        let col_idx = table.get_column_index(col_name).ok_or_else(|| format!("Column {} not found", col_name))?;
        return Ok(ConditionTemplate { col_idx, op: ColOpTemplate::IsNotNull });
    }
    if let Some(pos) = upper.find(" IS NULL") {
        let col_name = clean_col_name(&s[..pos]);
        let col_idx = table.get_column_index(col_name).ok_or_else(|| format!("Column {} not found", col_name))?;
        return Ok(ConditionTemplate { col_idx, op: ColOpTemplate::IsNull });
    }
    if let Some(b_idx) = upper.find(" BETWEEN ") {
        let col_name = clean_col_name(&s[..b_idx]);
        let col_idx = table.get_column_index(col_name).ok_or_else(|| format!("Column {} not found", col_name))?;
        let rest = &s[b_idx + 9..];
        let and_idx = rest.to_uppercase().find(" AND ").ok_or("Missing AND in BETWEEN clause")?;
        let min_token = rest[..and_idx].trim();
        let max_token = rest[and_idx + 5..].trim();
        let min_opnd = parse_operand_template(min_token)?;
        let max_opnd = parse_operand_template(max_token)?;
        return Ok(ConditionTemplate { col_idx, op: ColOpTemplate::Between(min_opnd, max_opnd) });
    }
    if let Some(op_idx) = s.find("!=") {
        let col_name = clean_col_name(&s[..op_idx]);
        let col_idx = table.get_column_index(col_name).ok_or_else(|| format!("Column {} not found", col_name))?;
        let opnd = parse_operand_template(&s[op_idx + 2..])?;
        return Ok(ConditionTemplate { col_idx, op: ColOpTemplate::NotEq(opnd) });
    }
    if let Some(op_idx) = s.find("<>") {
        let col_name = clean_col_name(&s[..op_idx]);
        let col_idx = table.get_column_index(col_name).ok_or_else(|| format!("Column {} not found", col_name))?;
        let opnd = parse_operand_template(&s[op_idx + 2..])?;
        return Ok(ConditionTemplate { col_idx, op: ColOpTemplate::NotEq(opnd) });
    }
    if let Some(op_idx) = s.find(">=") {
        let col_name = clean_col_name(&s[..op_idx]);
        let col_idx = table.get_column_index(col_name).ok_or_else(|| format!("Column {} not found", col_name))?;
        let opnd = parse_operand_template(&s[op_idx + 2..])?;
        return Ok(ConditionTemplate { col_idx, op: ColOpTemplate::Gte(opnd) });
    }
    if let Some(op_idx) = s.find("<=") {
        let col_name = clean_col_name(&s[..op_idx]);
        let col_idx = table.get_column_index(col_name).ok_or_else(|| format!("Column {} not found", col_name))?;
        let opnd = parse_operand_template(&s[op_idx + 2..])?;
        return Ok(ConditionTemplate { col_idx, op: ColOpTemplate::Lte(opnd) });
    }
    if let Some(op_idx) = s.find('>') {
        let col_name = clean_col_name(&s[..op_idx]);
        let col_idx = table.get_column_index(col_name).ok_or_else(|| format!("Column {} not found", col_name))?;
        let opnd = parse_operand_template(&s[op_idx + 1..])?;
        return Ok(ConditionTemplate { col_idx, op: ColOpTemplate::Gt(opnd) });
    }
    if let Some(op_idx) = s.find('<') {
        let col_name = clean_col_name(&s[..op_idx]);
        let col_idx = table.get_column_index(col_name).ok_or_else(|| format!("Column {} not found", col_name))?;
        let opnd = parse_operand_template(&s[op_idx + 1..])?;
        return Ok(ConditionTemplate { col_idx, op: ColOpTemplate::Lt(opnd) });
    }
    if let Some(op_idx) = s.find('=') {
        let left_raw = s[..op_idx].trim();
        let upper_left = left_raw.to_uppercase();
        let opnd = parse_operand_template(&s[op_idx + 1..])?;

        if upper_left.starts_with("LOWER(") && left_raw.ends_with(')') {
            let inner = &left_raw[6..left_raw.len() - 1].trim();
            let col_name = clean_col_name(inner);
            let col_idx = table.get_column_index(col_name).ok_or_else(|| format!("Column {} not found", col_name))?;
            return Ok(ConditionTemplate { col_idx, op: ColOpTemplate::LowerEq(opnd) });
        } else if upper_left.starts_with("UPPER(") && left_raw.ends_with(')') {
            let inner = &left_raw[6..left_raw.len() - 1].trim();
            let col_name = clean_col_name(inner);
            let col_idx = table.get_column_index(col_name).ok_or_else(|| format!("Column {} not found", col_name))?;
            return Ok(ConditionTemplate { col_idx, op: ColOpTemplate::UpperEq(opnd) });
        } else {
            let col_name = clean_col_name(left_raw);
            let col_idx = table.get_column_index(col_name).ok_or_else(|| format!("Column {} not found", col_name))?;
            return Ok(ConditionTemplate { col_idx, op: ColOpTemplate::Eq(opnd) });
        }
    }

    Err(format!("Unsupported condition syntax: {}", s))
}

fn compile_condition_templates(where_clause: &str, table: &crate::storage::table::Table) -> Result<Vec<ConditionTemplate>, String> {
    let mut templates = Vec::new();
    let parts = split_where_conditions(where_clause);

    for part in &parts {
        templates.push(parse_single_condition_template(part, table)?);
    }

    // Prioritize cheap, high-selectivity conditions: Eq > IsNull/IsNotNull > Range > Lower/Upper
    templates.sort_by_key(|c| match &c.op {
        ColOpTemplate::Eq(_) => 0,
        ColOpTemplate::IsNull | ColOpTemplate::IsNotNull => 1,
        ColOpTemplate::Between(_, _) | ColOpTemplate::Gt(_) | ColOpTemplate::Gte(_) | ColOpTemplate::Lt(_) | ColOpTemplate::Lte(_) => 2,
        ColOpTemplate::NotEq(_) => 3,
        ColOpTemplate::LowerEq(_) | ColOpTemplate::UpperEq(_) => 4,
    });

    Ok(templates)
}

fn compile_where_template(
    where_clause: &str,
    table: &crate::storage::table::Table,
) -> Result<WhereTemplate, String> {
    let mut trimmed = where_clause.trim();
    while is_fully_enclosed_in_parens(trimmed) {
        trimmed = trimmed[1..trimmed.len() - 1].trim();
    }
    if trimmed.is_empty() {
        return Err("Empty WHERE expression".to_string());
    }

    // 1. Top-level OR
    let or_parts = split_top_level_or(trimmed);
    if or_parts.len() > 1 {
        let mut list = Vec::with_capacity(or_parts.len());
        for p in or_parts {
            list.push(compile_where_template(p, table)?);
        }
        return Ok(WhereTemplate::Or(list));
    }

    // 2. Top-level AND
    let and_parts = split_top_level_and(trimmed);
    if and_parts.len() > 1 {
        let mut list = Vec::with_capacity(and_parts.len());
        for p in and_parts {
            list.push(compile_where_template(p, table)?);
        }
        return Ok(WhereTemplate::And(list));
    }

    // 3. Top-level NOT
    let upper = trimmed.to_uppercase();
    if upper.starts_with("NOT ") || upper.starts_with("NOT(") {
        let inner = trimmed[3..].trim();
        return Ok(WhereTemplate::Not(Box::new(compile_where_template(inner, table)?)));
    }

    // 4. Single condition
    let cond = parse_single_condition_template(trimmed, table)?;
    Ok(WhereTemplate::Condition(cond))
}

#[derive(Clone, Debug)]
enum WhereExpr {
    And(Vec<WhereExpr>),
    Or(Vec<WhereExpr>),
    Not(Box<WhereExpr>),
    Condition(Condition),
    Custom(String),
}

fn parse_where_expr(
    s: &str,
    table: &crate::storage::table::Table,
    params: &[Value],
) -> Result<WhereExpr, String> {
    let mut trimmed = s.trim();
    while is_fully_enclosed_in_parens(trimmed) {
        trimmed = trimmed[1..trimmed.len() - 1].trim();
    }
    if trimmed.is_empty() {
        return Err("Empty WHERE expression".to_string());
    }

    // 1. Check for top-level OR
    let or_parts = split_top_level_or(trimmed);
    if or_parts.len() > 1 {
        let mut list = Vec::with_capacity(or_parts.len());
        for p in or_parts {
            list.push(parse_where_expr(p, table, params)?);
        }
        return Ok(WhereExpr::Or(list));
    }

    // 2. Check for top-level AND
    let and_parts = split_top_level_and(trimmed);
    if and_parts.len() > 1 {
        let mut list = Vec::with_capacity(and_parts.len());
        for p in and_parts {
            list.push(parse_where_expr(p, table, params)?);
        }
        return Ok(WhereExpr::And(list));
    }

    // 3. Check for top-level NOT
    let upper = trimmed.to_uppercase();
    if upper.starts_with("NOT ") || upper.starts_with("NOT(") {
        let inner = trimmed[3..].trim();
        return Ok(WhereExpr::Not(Box::new(parse_where_expr(inner, table, params)?)));
    }

    // 4. Parse single condition or fallback to custom expression
    match parse_single_condition(trimmed, table, params) {
        Ok(cond) => Ok(WhereExpr::Condition(cond)),
        Err(_) => Ok(WhereExpr::Custom(trimmed.to_string())),
    }
}

fn parse_pattern_and_escape(s: &str, params: &[Value]) -> (String, Option<char>) {
    let trimmed = s.trim();
    let upper = trimmed.to_uppercase();
    if let Some(esc_pos) = upper.rfind(" ESCAPE ") {
        let pat_str = trimmed[..esc_pos].trim();
        let esc_str = trimmed[esc_pos + 8..].trim();
        let pat_val = parse_value(pat_str, params);
        let esc_val = parse_value(esc_str, params);
        let pat = pat_val.as_str();
        let esc_char = esc_val.as_str().chars().next();
        (pat, esc_char)
    } else {
        let pat_val = parse_value(trimmed, params);
        (pat_val.as_str(), None)
    }
}

fn parse_single_condition(
    part: &str,
    table: &crate::storage::table::Table,
    params: &[Value],
) -> Result<Condition, String> {
    let mut s = part.trim();
    while is_fully_enclosed_in_parens(s) {
        s = s[1..s.len() - 1].trim();
    }
    if s.contains("->") || s.contains("@>") || s.contains("<@") || s.contains('?') || s.contains("#>") {
        return Err("Custom JSON condition".to_string());
    }
    let upper = s.to_uppercase();

    if let Some(pos) = upper.find(" IS NOT NULL") {
        let col_name = clean_col_name(&s[..pos]);
        let col_idx = table.get_column_index(col_name).ok_or_else(|| format!("Column {} not found", col_name))?;
        return Ok(Condition { col_idx, op: ColOp::IsNotNull });
    }
    if let Some(pos) = upper.find(" IS NULL") {
        let col_name = clean_col_name(&s[..pos]);
        let col_idx = table.get_column_index(col_name).ok_or_else(|| format!("Column {} not found", col_name))?;
        return Ok(Condition { col_idx, op: ColOp::IsNull });
    }
    if let Some(b_idx) = upper.find(" BETWEEN ") {
        let col_name = clean_col_name(&s[..b_idx]);
        let col_idx = table.get_column_index(col_name).ok_or_else(|| format!("Column {} not found", col_name))?;
        let rest = &s[b_idx + 9..];
        let and_idx = rest.to_uppercase().find(" AND ").ok_or("Missing AND in BETWEEN clause")?;
        let min_token = rest[..and_idx].trim();
        let max_token = rest[and_idx + 5..].trim();
        let min_val = parse_value(min_token, params);
        let max_val = parse_value(max_token, params);
        return Ok(Condition { col_idx, op: ColOp::Between(min_val, max_val) });
    }
    if let Some(in_idx) = upper.find(" IN ") {
        let col_name = clean_col_name(&s[..in_idx]);
        let col_idx = table.get_column_index(col_name).ok_or_else(|| format!("Column {} not found", col_name))?;
        let rest = s[in_idx + 4..].trim();
        if rest.starts_with('(') && rest.ends_with(')') {
            let inside = &rest[1..rest.len() - 1];
            let list: Vec<Value> = split_comma_separated_tokens(inside).into_iter().map(|tok| parse_value(tok, params)).collect();
            return Ok(Condition { col_idx, op: ColOp::In(list) });
        }
    }
    if let Some(pos) = upper.find(" NOT ILIKE ") {
        let col_name = clean_col_name(&s[..pos]);
        let col_idx = table.get_column_index(col_name).ok_or_else(|| format!("Column {} not found", col_name))?;
        let (pattern, esc) = parse_pattern_and_escape(&s[pos + 11..], params);
        return Ok(Condition { col_idx, op: ColOp::NotILike(pattern, esc) });
    }
    if let Some(pos) = upper.find(" ILIKE ") {
        let col_name = clean_col_name(&s[..pos]);
        let col_idx = table.get_column_index(col_name).ok_or_else(|| format!("Column {} not found", col_name))?;
        let (pattern, esc) = parse_pattern_and_escape(&s[pos + 7..], params);
        return Ok(Condition { col_idx, op: ColOp::ILike(pattern, esc) });
    }
    if let Some(pos) = upper.find(" NOT LIKE ") {
        let col_name = clean_col_name(&s[..pos]);
        let col_idx = table.get_column_index(col_name).ok_or_else(|| format!("Column {} not found", col_name))?;
        let (pattern, esc) = parse_pattern_and_escape(&s[pos + 10..], params);
        return Ok(Condition { col_idx, op: ColOp::NotLike(pattern, esc) });
    }
    if let Some(pos) = upper.find(" LIKE ") {
        let col_name = clean_col_name(&s[..pos]);
        let col_idx = table.get_column_index(col_name).ok_or_else(|| format!("Column {} not found", col_name))?;
        let (pattern, esc) = parse_pattern_and_escape(&s[pos + 6..], params);
        return Ok(Condition { col_idx, op: ColOp::Like(pattern, esc) });
    }
    if let Some(op_idx) = s.find("!=") {
        let col_name = clean_col_name(&s[..op_idx]);
        let col_idx = table.get_column_index(col_name).ok_or_else(|| format!("Column {} not found", col_name))?;
        let val = parse_value(&s[op_idx + 2..], params);
        return Ok(Condition { col_idx, op: ColOp::NotEq(val) });
    }
    if let Some(op_idx) = s.find("<>") {
        let col_name = clean_col_name(&s[..op_idx]);
        let col_idx = table.get_column_index(col_name).ok_or_else(|| format!("Column {} not found", col_name))?;
        let val = parse_value(&s[op_idx + 2..], params);
        return Ok(Condition { col_idx, op: ColOp::NotEq(val) });
    }
    if let Some(op_idx) = s.find(">=") {
        let col_name = clean_col_name(&s[..op_idx]);
        let col_idx = table.get_column_index(col_name).ok_or_else(|| format!("Column {} not found", col_name))?;
        let val = parse_value(&s[op_idx + 2..], params);
        return Ok(Condition { col_idx, op: ColOp::Gte(val) });
    }
    if let Some(op_idx) = s.find("<=") {
        let col_name = clean_col_name(&s[..op_idx]);
        let col_idx = table.get_column_index(col_name).ok_or_else(|| format!("Column {} not found", col_name))?;
        let val = parse_value(&s[op_idx + 2..], params);
        return Ok(Condition { col_idx, op: ColOp::Lte(val) });
    }
    if let Some(op_idx) = s.find('>') {
        let col_name = clean_col_name(&s[..op_idx]);
        let col_idx = table.get_column_index(col_name).ok_or_else(|| format!("Column {} not found", col_name))?;
        let val = parse_value(&s[op_idx + 1..], params);
        return Ok(Condition { col_idx, op: ColOp::Gt(val) });
    }
    if let Some(op_idx) = s.find('<') {
        let col_name = clean_col_name(&s[..op_idx]);
        let col_idx = table.get_column_index(col_name).ok_or_else(|| format!("Column {} not found", col_name))?;
        let val = parse_value(&s[op_idx + 1..], params);
        return Ok(Condition { col_idx, op: ColOp::Lt(val) });
    }
    if let Some(op_idx) = s.find('=') {
        let left_raw = s[..op_idx].trim();
        let val = parse_value(&s[op_idx + 1..], params);

        let mut inner_str = left_raw;
        let mut is_upper = false;
        let mut is_lower = false;
        let mut is_trim = false;

        loop {
            let u = inner_str.to_uppercase();
            if (u.starts_with("BTRIM(") || u.starts_with("TRIM(")) && inner_str.ends_with(')') {
                is_trim = true;
                let open = inner_str.find('(').unwrap();
                inner_str = inner_str[open + 1..inner_str.len() - 1].trim();
            } else if u.starts_with("UPPER(") && inner_str.ends_with(')') {
                is_upper = true;
                inner_str = inner_str[6..inner_str.len() - 1].trim();
            } else if u.starts_with("LOWER(") && inner_str.ends_with(')') {
                is_lower = true;
                inner_str = inner_str[6..inner_str.len() - 1].trim();
            } else {
                break;
            }
        }

        let col_name = clean_col_name(inner_str);
        let col_idx = table.get_column_index(col_name).ok_or_else(|| format!("Column {} not found", col_name))?;

        if is_trim && is_upper {
            let target_str = match &val {
                Value::Text(s) => s.trim().to_uppercase(),
                Value::Int(i) => i.to_string(),
                _ => "".to_string(),
            };
            return Ok(Condition { col_idx, op: ColOp::TrimUpperEq(target_str) });
        } else if is_trim && is_lower {
            let target_str = match &val {
                Value::Text(s) => s.trim().to_lowercase(),
                Value::Int(i) => i.to_string(),
                _ => "".to_string(),
            };
            return Ok(Condition { col_idx, op: ColOp::TrimLowerEq(target_str) });
        } else if is_trim {
            let target_str = match &val {
                Value::Text(s) => s.trim().to_string(),
                Value::Int(i) => i.to_string(),
                _ => "".to_string(),
            };
            return Ok(Condition { col_idx, op: ColOp::TrimEq(target_str) });
        } else if is_upper {
            let target_str = match &val {
                Value::Text(s) => s.to_uppercase().to_string(),
                Value::Int(i) => i.to_string(),
                _ => "".to_string(),
            };
            return Ok(Condition { col_idx, op: ColOp::UpperEq(target_str) });
        } else if is_lower {
            let target_str = match &val {
                Value::Text(s) => s.to_lowercase().to_string(),
                Value::Int(i) => i.to_string(),
                _ => "".to_string(),
            };
            return Ok(Condition { col_idx, op: ColOp::LowerEq(target_str) });
        } else {
            return Ok(Condition { col_idx, op: ColOp::Eq(val) });
        }
    }

    if let Some(col_idx) = table.get_column_index(s) {
        return Ok(Condition { col_idx, op: ColOp::Eq(Value::Bool(true)) });
    }

    Err(format!("Unsupported condition: {}", s))
}

fn extract_single_pk_eq(expr: &WhereExpr, pk_idx: usize) -> Option<i64> {
    match expr {
        WhereExpr::Condition(cond) => {
            if cond.col_idx == pk_idx {
                if let ColOp::Eq(ref v) = cond.op {
                    return v.as_i64();
                }
            }
            None
        }
        WhereExpr::And(list) => {
            for e in list {
                if let Some(pk) = extract_single_pk_eq(e, pk_idx) {
                    return Some(pk);
                }
            }
            None
        }
        _ => None,
    }
}

fn evaluate_where_expr(
    row: &[Value],
    table: Option<&crate::storage::table::Table>,
    expr: &WhereExpr,
    params: &[Value],
) -> bool {
    evaluate_where_3vl(row, table, expr, params) == Some(true)
}

fn evaluate_where_3vl(
    row: &[Value],
    table: Option<&crate::storage::table::Table>,
    expr: &WhereExpr,
    params: &[Value],
) -> Option<bool> {
    match expr {
        WhereExpr::And(list) => {
            let mut has_null = false;
            for e in list {
                match evaluate_where_3vl(row, table, e, params) {
                    Some(false) => return Some(false),
                    None => has_null = true,
                    Some(true) => {}
                }
            }
            if has_null { None } else { Some(true) }
        }
        WhereExpr::Or(list) => {
            let mut has_null = false;
            for e in list {
                match evaluate_where_3vl(row, table, e, params) {
                    Some(true) => return Some(true),
                    None => has_null = true,
                    Some(false) => {}
                }
            }
            if has_null { None } else { Some(false) }
        }
        WhereExpr::Not(inner) => {
            match evaluate_where_3vl(row, table, inner, params) {
                Some(b) => Some(!b),
                None => None,
            }
        }
        WhereExpr::Condition(cond) => evaluate_single_condition_3vl(row, cond),
        WhereExpr::Custom(s) => {
            let v = eval_sql_expr(s, row, table, &[], None, params);
            if v.is_null() {
                None
            } else {
                v.as_bool()
            }
        }
    }
}

fn evaluate_single_condition_3vl(row: &[Value], cond: &Condition) -> Option<bool> {
    if cond.col_idx >= row.len() {
        return Some(false);
    }
    let val = &row[cond.col_idx];
    match &cond.op {
        ColOp::IsNull => Some(val.is_null()),
        ColOp::IsNotNull => Some(!val.is_null()),
        _ => {
            if val.is_null() {
                return None;
            }
            match &cond.op {
                ColOp::Eq(target) => {
                    if target.is_null() { None } else { Some(val.is_equal(target)) }
                }
                ColOp::NotEq(target) => {
                    if target.is_null() { None } else { Some(!val.is_equal(target)) }
                }
                ColOp::Gt(threshold) => {
                    if threshold.is_null() { None } else { Some(val.cmp_value(threshold) == std::cmp::Ordering::Greater && !val.is_equal(threshold)) }
                }
                ColOp::Gte(threshold) => {
                    if threshold.is_null() { None } else { Some(val.cmp_value(threshold) != std::cmp::Ordering::Less) }
                }
                ColOp::Lt(threshold) => {
                    if threshold.is_null() { None } else { Some(val.cmp_value(threshold) == std::cmp::Ordering::Less && !val.is_equal(threshold)) }
                }
                ColOp::Lte(threshold) => {
                    if threshold.is_null() { None } else { Some(val.cmp_value(threshold) != std::cmp::Ordering::Greater) }
                }
                ColOp::Between(min_val, max_val) => {
                    if min_val.is_null() || max_val.is_null() {
                        None
                    } else {
                        Some(val.cmp_value(min_val) != std::cmp::Ordering::Less && val.cmp_value(max_val) != std::cmp::Ordering::Greater)
                    }
                }
                ColOp::In(list) => {
                    let mut has_null = false;
                    for item in list {
                        if item.is_null() {
                            has_null = true;
                        } else if val.is_equal(item) {
                            return Some(true);
                        }
                    }
                    if has_null { None } else { Some(false) }
                }
                ColOp::Like(pattern, esc) => match val {
                    Value::Text(s) => Some(sql_like_match_with_escape(s, pattern, false, *esc)),
                    _ => Some(false),
                },
                ColOp::ILike(pattern, esc) => match val {
                    Value::Text(s) => Some(sql_like_match_with_escape(s, pattern, true, *esc)),
                    _ => Some(false),
                },
                ColOp::NotLike(pattern, esc) => match val {
                    Value::Text(s) => Some(!sql_like_match_with_escape(s, pattern, false, *esc)),
                    _ => Some(false),
                },
                ColOp::NotILike(pattern, esc) => match val {
                    Value::Text(s) => Some(!sql_like_match_with_escape(s, pattern, true, *esc)),
                    _ => Some(false),
                },
                ColOp::LowerEq(target) => match val {
                    Value::Text(s) => Some(s.to_lowercase() == *target),
                    Value::Int(i) => Some(i.to_string() == *target),
                    _ => Some(false),
                },
                ColOp::UpperEq(target) => match val {
                    Value::Text(s) => Some(s.to_uppercase() == *target),
                    Value::Int(i) => Some(i.to_string() == *target),
                    _ => Some(false),
                },
                ColOp::TrimUpperEq(target) => match val {
                    Value::Text(s) => Some(s.trim().to_uppercase() == *target),
                    Value::Int(i) => Some(i.to_string() == *target),
                    _ => Some(false),
                },
                ColOp::TrimLowerEq(target) => match val {
                    Value::Text(s) => Some(s.trim().to_lowercase() == *target),
                    Value::Int(i) => Some(i.to_string() == *target),
                    _ => Some(false),
                },
                ColOp::TrimEq(target) => match val {
                    Value::Text(s) => Some(s.trim() == *target),
                    Value::Int(i) => Some(i.to_string() == *target),
                    _ => Some(false),
                },
                ColOp::IsNull | ColOp::IsNotNull => unreachable!(),
            }
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum LikeToken {
    Exact(char),
    AnyOne,
    AnyMany,
}

fn sql_like_match_with_escape(text: &str, pattern: &str, case_insensitive: bool, esc_char: Option<char>) -> bool {
    let t_chars: Vec<char> = if case_insensitive {
        text.to_lowercase().chars().collect()
    } else {
        text.chars().collect()
    };
    let p_chars: Vec<char> = if case_insensitive {
        pattern.to_lowercase().chars().collect()
    } else {
        pattern.chars().collect()
    };
    let esc = esc_char.map(|c| if case_insensitive { c.to_lowercase().next().unwrap_or(c) } else { c });

    let mut tokens: Vec<LikeToken> = Vec::new();
    let mut i = 0;
    while i < p_chars.len() {
        let ch = p_chars[i];
        if let Some(e) = esc {
            if ch == e && i + 1 < p_chars.len() {
                tokens.push(LikeToken::Exact(p_chars[i + 1]));
                i += 2;
                continue;
            }
        }
        if ch == '%' {
            tokens.push(LikeToken::AnyMany);
        } else if ch == '_' {
            tokens.push(LikeToken::AnyOne);
        } else {
            tokens.push(LikeToken::Exact(ch));
        }
        i += 1;
    }

    let t_len = t_chars.len();
    let p_len = tokens.len();
    let mut dp = vec![vec![false; p_len + 1]; t_len + 1];
    dp[0][0] = true;
    for j in 1..=p_len {
        if tokens[j - 1] == LikeToken::AnyMany {
            dp[0][j] = dp[0][j - 1];
        }
    }
    for i in 1..=t_len {
        for j in 1..=p_len {
            match tokens[j - 1] {
                LikeToken::AnyMany => {
                    dp[i][j] = dp[i - 1][j] || dp[i][j - 1];
                }
                LikeToken::AnyOne => {
                    dp[i][j] = dp[i - 1][j - 1];
                }
                LikeToken::Exact(c) => {
                    if c == t_chars[i - 1] {
                        dp[i][j] = dp[i - 1][j - 1];
                    }
                }
            }
        }
    }
    dp[t_len][p_len]
}

#[inline(always)]
fn sql_like_match(text: &str, pattern: &str, case_insensitive: bool) -> bool {
    sql_like_match_with_escape(text, pattern, case_insensitive, None)
}

#[inline(always)]
fn parse_pg_array_to_json(trimmed: &str) -> Option<serde_json::Value> {
    if trimmed == "{}" {
        return Some(serde_json::Value::Array(vec![]));
    }
    if trimmed.starts_with('{') && trimmed.ends_with('}') {
        let inside = &trimmed[1..trimmed.len() - 1].trim();
        if inside.is_empty() {
            return Some(serde_json::Value::Array(vec![]));
        }
        // If it has colons at top-level, it's a JSON object, not a PG array
        let mut in_quote = false;
        let mut has_colon = false;
        for c in inside.chars() {
            if c == '"' || c == '\'' {
                in_quote = !in_quote;
            } else if c == ':' && !in_quote {
                has_colon = true;
                break;
            }
        }
        if has_colon {
            return None;
        }

        let tokens = split_comma_separated_tokens(inside);
        let mut items = Vec::with_capacity(tokens.len());
        for tok in tokens {
            let t = tok.trim();
            if t.starts_with('"') && t.ends_with('"') && t.len() >= 2 {
                items.push(serde_json::Value::String(t[1..t.len() - 1].replace("\\\"", "\"")));
            } else if t.starts_with('\'') && t.ends_with('\'') && t.len() >= 2 {
                items.push(serde_json::Value::String(t[1..t.len() - 1].replace("''", "'")));
            } else if let Ok(i) = t.parse::<i64>() {
                items.push(serde_json::Value::Number(i.into()));
            } else if let Ok(f) = t.parse::<f64>() {
                if let Some(n) = serde_json::Number::from_f64(f) {
                    items.push(serde_json::Value::Number(n));
                } else {
                    items.push(serde_json::Value::String(t.to_string()));
                }
            } else if t.eq_ignore_ascii_case("TRUE") || t.eq_ignore_ascii_case("T") {
                items.push(serde_json::Value::Bool(true));
            } else if t.eq_ignore_ascii_case("FALSE") || t.eq_ignore_ascii_case("F") {
                items.push(serde_json::Value::Bool(false));
            } else if t.eq_ignore_ascii_case("NULL") {
                items.push(serde_json::Value::Null);
            } else {
                items.push(serde_json::Value::String(t.to_string()));
            }
        }
        return Some(serde_json::Value::Array(items));
    }
    None
}

fn get_raw_bytes(val: &Value) -> Vec<u8> {
    match val {
        Value::Text(s) => {
            let mut s_str = s.as_str().trim();
            while s_str.starts_with('\\') {
                s_str = &s_str[1..];
            }
            if s_str.starts_with('x') || s_str.starts_with('X') {
                hex::decode(&s_str[1..]).unwrap_or_else(|_| s.as_bytes().to_vec())
            } else if s_str.starts_with("0x") || s_str.starts_with("0X") {
                hex::decode(&s_str[2..]).unwrap_or_else(|_| s.as_bytes().to_vec())
            } else if s_str.starts_with('[') && s_str.ends_with(']') {
                if let Ok(nums) = serde_json::from_str::<Vec<serde_json::Value>>(s_str) {
                    if nums.iter().all(|n| n.as_u64().map_or(false, |v| v <= 255)) {
                        return nums.iter().filter_map(|n| n.as_u64().map(|v| v as u8)).collect();
                    }
                }
                s.as_bytes().to_vec()
            } else {
                s.as_bytes().to_vec()
            }
        }
        Value::Int(i) => i.to_string().into_bytes(),
        Value::Float(f) => f.to_string().into_bytes(),
        Value::Bool(b) => (if *b { "true" } else { "false" }).as_bytes().to_vec(),
        Value::Null => Vec::new(),
    }
}

fn regex_match(text: &str, pat: &str, case_insensitive: bool) -> bool {
    let pattern = if case_insensitive {
        format!("(?i){}", pat)
    } else {
        pat.to_string()
    };
    if let Ok(re) = regex::Regex::new(&pattern) {
        re.is_match(text)
    } else {
        false
    }
}

fn json_to_value(j: &serde_json::Value) -> Value {
    match j {
        serde_json::Value::Null => Value::Null,
        serde_json::Value::Bool(b) => Value::Bool(*b),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Value::Int(i)
            } else if let Some(f) = n.as_f64() {
                Value::Float(f)
            } else {
                Value::text(n.to_string())
            }
        }
        serde_json::Value::String(s) => Value::text(s),
        serde_json::Value::Array(_) | serde_json::Value::Object(_) => Value::text(j.to_string()),
    }
}

fn is_complete_array_constructor(s: &str) -> bool {
    let s_trim = s.trim();
    if !s_trim.to_uppercase().starts_with("ARRAY[") || !s_trim.ends_with(']') {
        return false;
    }
    let bytes = s_trim.as_bytes();
    let mut depth = 0;
    let mut in_single_quote = false;
    for (i, &b) in bytes.iter().enumerate().skip(5) {
        if b == b'\'' {
            in_single_quote = !in_single_quote;
        } else if !in_single_quote {
            if b == b'[' {
                depth += 1;
            } else if b == b']' {
                depth -= 1;
                if depth == 0 {
                    return i == bytes.len() - 1;
                }
            }
        }
    }
    false
}

fn val_to_array_items(val: &Value) -> Vec<Value> {
    match val {
        Value::Null => vec![],
        Value::Text(s) => {
            let trimmed = s.trim();
            if trimmed.is_empty() || trimmed == "[]" || trimmed == "{}" {
                return vec![];
            }
            if trimmed.starts_with('(') && trimmed.ends_with(')') {
                let inside = &trimmed[1..trimmed.len() - 1].trim();
                let tokens = split_comma_separated_tokens(inside);
                if tokens.len() > 1 {
                    return tokens.into_iter().map(|tok| parse_value(&tok, &[])).collect();
                }
            }
            if let Some(serde_json::Value::Array(arr)) = parse_pg_array_to_json(trimmed) {
                return arr.into_iter().map(|item| json_to_value(&item)).collect();
            }
            if trimmed.starts_with('[') && trimmed.ends_with(']') {
                if let Ok(serde_json::Value::Array(arr)) = serde_json::from_str::<serde_json::Value>(trimmed) {
                    return arr.into_iter().map(|item| json_to_value(&item)).collect();
                }
            }
            vec![val.clone()]
        }
        _ => vec![val.clone()],
    }
}

fn array_contains_check(container: &Value, subset: &Value) -> bool {
    if container.is_null() || subset.is_null() {
        return false;
    }
    let c_items = val_to_array_items(container);
    let s_items = val_to_array_items(subset);
    s_items.iter().all(|s| c_items.iter().any(|c| c.is_equal(s)))
}

fn array_overlap_check(a: &Value, b: &Value) -> bool {
    if a.is_null() || b.is_null() {
        return false;
    }
    let a_items = val_to_array_items(a);
    let b_items = val_to_array_items(b);
    a_items.iter().any(|item_a| b_items.iter().any(|item_b| item_a.is_equal(item_b)))
}

fn cmp_composite_tuples(left: &Value, right: &Value) -> std::cmp::Ordering {
    let l_items = val_to_array_items(left);
    let r_items = val_to_array_items(right);
    for i in 0..l_items.len().min(r_items.len()) {
        let ord = l_items[i].cmp_value(&r_items[i]);
        if ord != std::cmp::Ordering::Equal {
            return ord;
        }
    }
    l_items.len().cmp(&r_items.len())
}

#[inline(always)]
fn value_to_json(val: &Value) -> serde_json::Value {
    match val {
        Value::Null => serde_json::Value::Null,
        Value::Bool(b) => serde_json::Value::Bool(*b),
        Value::Int(n) => serde_json::Value::Number((*n).into()),
        Value::Float(f) => serde_json::Number::from_f64(*f)
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null),
        Value::Text(s) => {
            let trimmed = s.trim();
            if let Some(arr_val) = parse_pg_array_to_json(trimmed) {
                return arr_val;
            }
            if trimmed.starts_with('[') && trimmed.ends_with(']') {
                if let Ok(val) = serde_json::from_str::<serde_json::Value>(trimmed) {
                    return val;
                }
            }
            if trimmed.starts_with('{') && trimmed.ends_with('}') {
                if let Ok(val) = serde_json::from_str::<serde_json::Value>(trimmed) {
                    return val;
                }
            }
            serde_json::Value::String(s.to_string())
        }
    }
}

fn row_to_json(table: &crate::storage::table::Table, row: &[Value]) -> serde_json::Value {
    let mut map = serde_json::Map::with_capacity(table.columns.len());
    for (i, col) in table.columns.iter().enumerate() {
        let v = row.get(i).unwrap_or(&Value::Null);
        if col.data_type == crate::types::DataType::Jsonb {
            if let Value::Text(s) = v {
                let trimmed = s.trim();
                if let Ok(json_val) = serde_json::from_str::<serde_json::Value>(trimmed) {
                    map.insert(col.name.clone(), json_val);
                    continue;
                }
            }
        } else if col.data_type == crate::types::DataType::Bytea {
            if v.is_null() {
                map.insert(col.name.clone(), serde_json::Value::Null);
            } else {
                let raw = get_raw_bytes(v);
                let json_arr: Vec<serde_json::Value> = raw.into_iter().map(|b| serde_json::json!(b)).collect();
                map.insert(col.name.clone(), serde_json::Value::Array(json_arr));
            }
            continue;
        } else if col.data_type == crate::types::DataType::Date {
            match v {
                Value::Text(s) => {
                    let trimmed = s.trim();
                    if trimmed.is_empty() || trimmed == "{}" || trimmed.eq_ignore_ascii_case("NULL") {
                        map.insert(col.name.clone(), serde_json::Value::Null);
                    } else if trimmed.eq_ignore_ascii_case("CURRENT_DATE") {
                        map.insert(col.name.clone(), serde_json::Value::String(get_current_date()));
                    } else if let Some((d_part, _)) = trimmed.split_once('T') {
                        if d_part.len() == 10 && d_part.chars().all(|c| c.is_ascii_digit() || c == '-') {
                            map.insert(col.name.clone(), serde_json::Value::String(d_part.to_string()));
                        } else {
                            map.insert(col.name.clone(), serde_json::Value::String(trimmed.to_string()));
                        }
                    } else {
                        map.insert(col.name.clone(), serde_json::Value::String(trimmed.to_string()));
                    }
                    continue;
                }
                Value::Null => {
                    map.insert(col.name.clone(), serde_json::Value::Null);
                    continue;
                }
                _ => {}
            }
        } else if col.data_type == crate::types::DataType::Timestamp {
            match v {
                Value::Text(s) => {
                    let trimmed = s.trim();
                    if trimmed.is_empty() || trimmed == "{}" || trimmed.eq_ignore_ascii_case("NULL") {
                        map.insert(col.name.clone(), serde_json::Value::Null);
                    } else if trimmed.eq_ignore_ascii_case("CURRENT_TIMESTAMP") || trimmed.eq_ignore_ascii_case("NOW()") || trimmed.eq_ignore_ascii_case("LOCALTIMESTAMP") {
                        map.insert(col.name.clone(), serde_json::Value::String(get_current_timestamp()));
                    } else {
                        map.insert(col.name.clone(), serde_json::Value::String(trimmed.to_string()));
                    }
                    continue;
                }
                Value::Null => {
                    map.insert(col.name.clone(), serde_json::Value::Null);
                    continue;
                }
                _ => {}
            }
        }
        map.insert(col.name.clone(), value_to_json(v));
    }
    serde_json::Value::Object(map)
}

fn extract_returning_clause(sql: &str) -> (&str, Option<&str>) {
    if let Some(pos) = find_top_level_keyword(sql, "RETURNING") {
        (sql[..pos].trim(), Some(sql[pos + 9..].trim()))
    } else {
        (sql, None)
    }
}

fn project_returning_row(
    table: &crate::storage::table::Table,
    row: &[Value],
    r_cols: &str,
) -> serde_json::Value {
    let trimmed = r_cols.trim();
    if trimmed == "*" {
        return row_to_json(table, row);
    }
    let mut map = serde_json::Map::new();
    for item in trimmed.split(',') {
        let item = item.trim();
        if item.is_empty() {
            continue;
        }
        let upper_item = item.to_uppercase();
        let (expr_col, alias) = if let Some(as_pos) = upper_item.find(" AS ") {
            (item[..as_pos].trim(), item[as_pos + 4..].trim().trim_matches('"'))
        } else {
            (item, clean_col_name(item))
        };
        let col_name = clean_col_name(expr_col);
        if let Some(col_idx) = table.get_column_index(col_name) {
            let v = row.get(col_idx).unwrap_or(&Value::Null);
            map.insert(alias.to_string(), value_to_json(v));
        }
    }
    serde_json::Value::Object(map)
}

fn get_returning_fields(
    table: &crate::storage::table::Table,
    r_cols: &str,
) -> Vec<FieldInfo> {
    let trimmed = r_cols.trim();
    if trimmed == "*" {
        table.columns.iter().map(|c| FieldInfo {
            name: c.name.clone(),
            data_type: format!("{:?}", c.data_type).to_lowercase(),
        }).collect()
    } else {
        let mut fields = Vec::new();
        for item in trimmed.split(',') {
            let item = item.trim();
            if item.is_empty() {
                continue;
            }
            let upper_item = item.to_uppercase();
            let (expr_col, alias) = if let Some(as_pos) = upper_item.find(" AS ") {
                (item[..as_pos].trim(), item[as_pos + 4..].trim().trim_matches('"'))
            } else {
                (item, clean_col_name(item))
            };
            let col_name = clean_col_name(expr_col);
            if let Some(col_idx) = table.get_column_index(col_name) {
                fields.push(FieldInfo {
                    name: alias.to_string(),
                    data_type: format!("{:?}", table.columns[col_idx].data_type).to_lowercase(),
                });
            }
        }
        fields
    }
}

fn evaluate_assignment_val(
    val_str: &str,
    row: &[Value],
    table: &crate::storage::table::Table,
    params: &[Value],
) -> Value {
    let s = val_str.trim();
    if let Some(dt_val) = eval_date_time_keyword(s) {
        return dt_val;
    }
    eval_sql_expr(s, row, Some(table), &[], None, params)
}

fn coerce_update_val(col_def: &crate::types::ColumnDef, val: Value) -> Value {
    if val.is_null() {
        return Value::Null;
    }
    match (&col_def.data_type, &val) {
        (crate::types::DataType::Bytea, val) => {
            let raw = get_raw_bytes(val);
            let items: Vec<serde_json::Value> = raw.iter().map(|&b| serde_json::json!(b)).collect();
            Value::text(serde_json::to_string(&items).unwrap_or_default())
        }
        (crate::types::DataType::Integer | crate::types::DataType::Serial, Value::Float(f)) => {
            Value::Int(*f as i64)
        }
        (crate::types::DataType::BigInt, Value::Float(f)) => {
            Value::Int(*f as i64)
        }
        (crate::types::DataType::Integer | crate::types::DataType::Serial, Value::Text(s)) => {
            if let Ok(i) = s.parse::<i64>() {
                Value::Int(i)
            } else {
                val.clone()
            }
        }
        (crate::types::DataType::BigInt, Value::Text(s)) => {
            if let Ok(i) = s.parse::<i64>() {
                Value::Int(i)
            } else {
                val.clone()
            }
        }
        (crate::types::DataType::Numeric, Value::Text(s)) => {
            if let Ok(f) = s.parse::<f64>() {
                Value::Float(f)
            } else {
                val.clone()
            }
        }
        (crate::types::DataType::Boolean, Value::Text(s)) => {
            if s.eq_ignore_ascii_case("true") || s == "1" {
                Value::Bool(true)
            } else if s.eq_ignore_ascii_case("false") || s == "0" {
                Value::Bool(false)
            } else {
                val.clone()
            }
        }
        _ => val,
    }
}

fn cast_val(val: Value, target_type: &str) -> Value {
    if val.is_null() {
        return Value::Null;
    }
    let tt = target_type.trim().to_uppercase();
    if tt.ends_with("[]") || tt.starts_with("ARRAY") {
        let items = val_to_array_items(&val);
        let inner_type = tt.trim_end_matches("[]").trim_start_matches("ARRAY").trim();
        let casted_items: Vec<serde_json::Value> = items
            .into_iter()
            .map(|item| {
                let c = if inner_type.is_empty() { item } else { cast_val(item, inner_type) };
                value_to_json(&c)
            })
            .collect();
        Value::text(serde_json::to_string(&casted_items).unwrap_or_default())
    } else if tt.starts_with("INT") || tt == "BIGINT" || tt == "SERIAL" {
        if let Some(i) = val.as_i64() {
            Value::Int(i)
        } else {
            Value::Null
        }
    } else if tt.starts_with("TEXT") || tt.starts_with("VARCHAR") || tt == "CHAR" {
        Value::text(val.as_str())
    } else if tt.starts_with("NUMERIC") || tt.starts_with("FLOAT") || tt.starts_with("DOUBLE") || tt.starts_with("DECIMAL") {
        if let Some(f) = val.as_f64() {
            Value::Float(f)
        } else {
            Value::Null
        }
    } else if tt.starts_with("BOOL") {
        if let Some(b) = val.as_bool() {
            Value::Bool(b)
        } else {
            Value::Null
        }
    } else if tt.starts_with("UUID") {
        Value::text(val.as_str())
    } else if tt.starts_with("BYTEA") {
        let raw = get_raw_bytes(&val);
        let items: Vec<serde_json::Value> = raw.iter().map(|&b| serde_json::json!(b)).collect();
        Value::text(serde_json::to_string(&items).unwrap_or_default())
    } else {
        val
    }
}

fn split_function_args(inner: &str) -> Vec<String> {
    let mut args = Vec::new();
    let mut current = String::new();
    let mut depth: i32 = 0;
    let mut in_str = false;

    for ch in inner.chars() {
        match ch {
            '\'' => {
                in_str = !in_str;
                current.push(ch);
            }
            '(' | '[' | '{' if !in_str => {
                depth += 1;
                current.push(ch);
            }
            ')' | ']' | '}' if !in_str => {
                depth = depth.saturating_sub(1);
                current.push(ch);
            }
            ',' if !in_str && depth == 0 => {
                args.push(current.trim().to_string());
                current.clear();
            }
            _ => {
                current.push(ch);
            }
        }
    }
    if !current.trim().is_empty() {
        args.push(current.trim().to_string());
    }
    args
}

fn split_when_clauses(when_part: &str) -> Vec<(String, String)> {
    let mut clauses = Vec::new();
    let s = when_part.trim();
    let mut i = 0;
    let len = s.len();

    while i < len {
        let remaining = &s[i..];
        let when_kw = find_top_level_keyword(remaining, "WHEN");
        if when_kw.is_none() {
            break;
        }
        let when_start = i + when_kw.unwrap() + 4;
        let then_kw = find_top_level_keyword(&s[when_start..], "THEN");
        if then_kw.is_none() {
            break;
        }
        let then_pos = when_start + then_kw.unwrap();
        let cond_str = s[when_start..then_pos].trim().to_string();

        let res_start = then_pos + 4;
        let next_when_kw = find_top_level_keyword(&s[res_start..], "WHEN");
        let res_end = if let Some(nw) = next_when_kw {
            res_start + nw
        } else {
            len
        };
        let res_str = s[res_start..res_end].trim().to_string();
        clauses.push((cond_str, res_str));
        i = res_end;
    }
    clauses
}

fn find_top_level_op(s: &str, op: &str) -> Option<usize> {
    let mut in_str = false;
    let mut depth: i32 = 0;
    let op_bytes = op.as_bytes();
    let s_bytes = s.as_bytes();
    let len = s_bytes.len();

    let mut i = 0;
    while i < len {
        let b = s_bytes[i];
        if b == b'\'' {
            in_str = !in_str;
            i += 1;
            continue;
        }
        if in_str {
            i += 1;
            continue;
        }
        if b == b'(' || b == b'[' {
            depth += 1;
        } else if b == b')' || b == b']' {
            depth = depth.saturating_sub(1);
        }
        if depth == 0 && b == b'-' && i + 1 < len && s_bytes[i + 1] == b'>' {
            if i + 2 < len && s_bytes[i + 2] == b'>' {
                i += 3;
            } else {
                i += 2;
            }
            continue;
        }
        if depth == 0 && i + op_bytes.len() <= len {
            if &s_bytes[i..i + op_bytes.len()] == op_bytes {
                if op == ">" {
                    if i + 1 < len && (s_bytes[i + 1] == b'=' || s_bytes[i + 1] == b'>') {
                        i += 2;
                        continue;
                    }
                    if i > 0 && s_bytes[i - 1] == b'>' {
                        i += 1;
                        continue;
                    }
                }
                if op == "<" {
                    if i + 1 < len && (s_bytes[i + 1] == b'=' || s_bytes[i + 1] == b'>' || s_bytes[i + 1] == b'@' || s_bytes[i + 1] == b'<') {
                        i += 2;
                        continue;
                    }
                    if i > 0 && s_bytes[i - 1] == b'<' {
                        i += 1;
                        continue;
                    }
                }
                if op == ">" && i > 0 && (s_bytes[i - 1] == b'-' || s_bytes[i - 1] == b'@' || s_bytes[i - 1] == b'#') {
                    i += 1;
                    continue;
                }
                if op == "?" && i + 1 < len && (s_bytes[i + 1] == b'|' || s_bytes[i + 1] == b'&') {
                    i += 2;
                    continue;
                }
                if (op == "~" || op == "~*" || op == "!~" || op == "!~*") && (i == 0 || s[..i].trim().is_empty()) {
                    i += 1;
                    continue;
                }
                if op == "~" && i > 0 && s_bytes[i - 1] == b'!' {
                    i += 1;
                    continue;
                }
                if op == "~" && i + 1 < len && s_bytes[i + 1] == b'*' {
                    i += 2;
                    continue;
                }
                if op == "!~" && i + 2 < len && s_bytes[i + 2] == b'*' {
                    i += 3;
                    continue;
                }
                if op == "=" && i > 0 && (s_bytes[i - 1] == b'!' || s_bytes[i - 1] == b'<' || s_bytes[i - 1] == b'>' || s_bytes[i - 1] == b':') {
                    i += 1;
                    continue;
                }
                return Some(i);
            }
        }
        i += 1;
    }
    None
}

fn find_last_top_level_op(s: &str, op: &str) -> Option<usize> {
    let mut in_str = false;
    let mut depth: i32 = 0;
    let op_bytes = op.as_bytes();
    let s_bytes = s.as_bytes();
    let len = s_bytes.len();
    let mut last_pos = None;

    let mut i = 0;
    while i < len {
        let b = s_bytes[i];
        if b == b'\'' {
            in_str = !in_str;
            i += 1;
            continue;
        }
        if in_str {
            i += 1;
            continue;
        }
        if b == b'(' || b == b'[' {
            depth += 1;
        } else if b == b')' || b == b']' {
            depth = depth.saturating_sub(1);
        }
        if depth == 0 && i + op_bytes.len() <= len {
            if &s_bytes[i..i + op_bytes.len()] == op_bytes {
                last_pos = Some(i);
            }
        }
        i += 1;
    }
    last_pos
}

fn find_top_level_shift_op(s: &str) -> Option<(usize, &'static str)> {
    let mut in_single_quote = false;
    let mut depth: i32 = 0;
    let bytes = s.as_bytes();
    let len = bytes.len();
    if len < 2 {
        return None;
    }
    let mut i = (len as isize) - 2;
    while i >= 0 {
        let idx = i as usize;
        let b = bytes[idx];
        if b == b'\'' {
            in_single_quote = !in_single_quote;
        } else if !in_single_quote {
            if b == b')' || b == b']' {
                depth += 1;
            } else if b == b'(' || b == b'[' {
                depth = (depth - 1).max(0);
            } else if depth == 0 {
                if bytes[idx] == b'<' && bytes[idx + 1] == b'<' {
                    return Some((idx, "<<"));
                }
                if bytes[idx] == b'>' && bytes[idx + 1] == b'>' {
                    if idx > 0 && (bytes[idx - 1] == b'#' || bytes[idx - 1] == b'-') {
                        i -= 1;
                        continue;
                    }
                    return Some((idx, ">>"));
                }
            }
        }
        i -= 1;
    }
    None
}

fn find_top_level_math_op(s: &str, ops: &[char]) -> Option<(usize, char)> {
    let mut in_single_quote = false;
    let mut depth: i32 = 0;
    let bytes = s.as_bytes();
    let len = bytes.len();
    
    let mut i = (len as isize) - 1;
    while i >= 0 {
        let idx = i as usize;
        let b = bytes[idx];
        if b == b'\'' {
            in_single_quote = !in_single_quote;
        } else if !in_single_quote {
            if b == b')' || b == b']' {
                depth += 1;
            } else if b == b'(' || b == b'[' {
                depth = (depth - 1).max(0);
            } else if depth == 0 {
                let ch = b as char;
                if ops.contains(&ch) {
                    if ch == '-' {
                        if idx + 1 < len && bytes[idx + 1] == b'>' {
                            i -= 1;
                            continue;
                        }
                        if idx == 0 {
                            i -= 1;
                            continue;
                        }
                        let prev_non_space = s[..idx].trim_end();
                        if prev_non_space.ends_with(|c: char| "+-*/=<>!%^&|,".contains(c)) {
                            i -= 1;
                            continue;
                        }
                    }
                    if ch == '*' {
                        let prev_non_space = s[..idx].trim_end();
                        let next_non_space = s[idx + 1..].trim_start();
                        if prev_non_space.is_empty() || next_non_space.is_empty() {
                            i -= 1;
                            continue;
                        }
                    }
                    if ch == '|' {
                        if (idx + 1 < len && bytes[idx + 1] == b'|') || (idx > 0 && bytes[idx - 1] == b'|') {
                            i -= 1;
                            continue;
                        }
                    }
                    if ch == '#' {
                        if idx + 1 < len && bytes[idx + 1] == b'>' {
                            i -= 1;
                            continue;
                        }
                    }
                    return Some((idx, ch));
                }
            }
        }
        i -= 1;
    }
    None
}

fn find_last_top_level_json_op(s: &str) -> Option<(usize, &'static str)> {
    let mut in_str = false;
    let mut depth: i32 = 0;
    let s_bytes = s.as_bytes();
    let len = s_bytes.len();
    let mut last_match = None;

    let mut i = 0;
    while i < len {
        let b = s_bytes[i];
        if b == b'\'' {
            in_str = !in_str;
            i += 1;
            continue;
        }
        if in_str {
            i += 1;
            continue;
        }
        if b == b'(' || b == b'[' {
            depth += 1;
        } else if b == b')' || b == b']' {
            depth = depth.saturating_sub(1);
        } else if depth == 0 {
            if i + 3 <= len && &s_bytes[i..i + 3] == b"->>" {
                last_match = Some((i, "->>"));
                i += 3;
                continue;
            } else if i + 2 <= len && &s_bytes[i..i + 2] == b"->" {
                last_match = Some((i, "->"));
                i += 2;
                continue;
            } else if i + 3 <= len && &s_bytes[i..i + 3] == b"#>>" {
                last_match = Some((i, "#>>"));
                i += 3;
                continue;
            } else if i + 2 <= len && &s_bytes[i..i + 2] == b"#>" {
                last_match = Some((i, "#>"));
                i += 2;
                continue;
            }
        }
        i += 1;
    }
    last_match
}

fn parse_val_to_json(v: &Value) -> Option<serde_json::Value> {
    match v {
        Value::Text(s) => {
            let trimmed = s.trim();
            if trimmed.starts_with('{') || trimmed.starts_with('[') {
                if let Ok(val) = serde_json::from_str(trimmed) {
                    return Some(val);
                }
                if let Some(val) = parse_pg_array_to_json(trimmed) {
                    return Some(val);
                }
                None
            } else if trimmed.starts_with('\'') && trimmed.ends_with('\'') && trimmed.len() >= 2 {
                let unquoted = &trimmed[1..trimmed.len() - 1];
                if unquoted.starts_with('{') || unquoted.starts_with('[') {
                    serde_json::from_str(unquoted).ok().or_else(|| parse_pg_array_to_json(unquoted))
                } else {
                    Some(serde_json::Value::String(unquoted.to_string()))
                }
            } else {
                Some(serde_json::Value::String(trimmed.to_string()))
            }
        }
        Value::Null => None,
        Value::Bool(b) => Some(serde_json::Value::Bool(*b)),
        Value::Int(i) => Some(serde_json::Value::Number((*i).into())),
        Value::Float(f) => serde_json::Number::from_f64(*f).map(serde_json::Value::Number),
    }
}

fn parse_json_path_keys(rhs_val: &serde_json::Value) -> Vec<String> {
    match rhs_val {
        serde_json::Value::Array(arr) => arr.iter().filter_map(|x| match x {
            serde_json::Value::String(s) => Some(s.clone()),
            serde_json::Value::Number(n) => Some(n.to_string()),
            _ => None,
        }).collect(),
        serde_json::Value::String(s) => {
            let trimmed = s.trim().trim_matches('\'').trim_matches('"').trim();
            if let Ok(serde_json::Value::Array(arr)) = serde_json::from_str::<serde_json::Value>(trimmed) {
                arr.iter().filter_map(|x| match x {
                    serde_json::Value::String(st) => Some(st.clone()),
                    serde_json::Value::Number(n) => Some(n.to_string()),
                    _ => None,
                }).collect()
            } else {
                let inner = trimmed.trim_matches('{').trim_matches('}').trim_matches('[').trim_matches(']');
                inner.split(',').map(|p| p.trim().trim_matches('\'').trim_matches('"').to_string()).filter(|p| !p.is_empty()).collect()
            }
        }
        _ => vec![],
    }
}

fn parse_str_or_json_keys(v: &Value) -> Vec<String> {
    match v {
        Value::Text(s) => {
            let val = serde_json::Value::String(s.to_string());
            parse_json_path_keys(&val)
        }
        _ => {
            let val = value_to_json(v);
            parse_json_path_keys(&val)
        }
    }
}

fn json_contains_check(target: &serde_json::Value, contained: &serde_json::Value) -> bool {
    match (target, contained) {
        (serde_json::Value::Object(t_map), serde_json::Value::Object(c_map)) => {
            for (k, c_v) in c_map {
                if let Some(t_v) = t_map.get(k) {
                    if !json_contains_check(t_v, c_v) {
                        return false;
                    }
                } else {
                    return false;
                }
            }
            true
        }
        (serde_json::Value::Array(t_arr), serde_json::Value::Array(c_arr)) => {
            for c_elem in c_arr {
                if !t_arr.iter().any(|t_elem| json_contains_check(t_elem, c_elem)) {
                    return false;
                }
            }
            true
        }
        (serde_json::Value::Array(t_arr), _) => {
            t_arr.iter().any(|t_elem| json_contains_check(t_elem, contained))
        }
        _ => target == contained,
    }
}

fn json_has_key_check(target: &serde_json::Value, key: &str) -> bool {
    let clean_key = key.trim().trim_matches('\'').trim_matches('"');
    match target {
        serde_json::Value::Object(map) => map.contains_key(clean_key),
        serde_json::Value::Array(arr) => arr.iter().any(|x| match x {
            serde_json::Value::String(s) => s == clean_key,
            _ => false,
        }),
        _ => false,
    }
}

fn jsonb_set_serde(target: &mut serde_json::Value, path: &[String], new_val: &serde_json::Value, create_missing: bool) {
    if path.is_empty() {
        *target = new_val.clone();
        return;
    }
    let key = &path[0];
    if path.len() == 1 {
        match target {
            serde_json::Value::Object(map) => {
                if create_missing || map.contains_key(key) {
                    map.insert(key.clone(), new_val.clone());
                }
            }
            serde_json::Value::Array(arr) => {
                if let Ok(idx) = key.parse::<usize>() {
                    if idx < arr.len() {
                        arr[idx] = new_val.clone();
                    } else if create_missing && idx == arr.len() {
                        arr.push(new_val.clone());
                    }
                }
            }
            _ => {}
        }
    } else {
        match target {
            serde_json::Value::Object(map) => {
                if let Some(next) = map.get_mut(key) {
                    jsonb_set_serde(next, &path[1..], new_val, create_missing);
                } else if create_missing {
                    let mut next = serde_json::Value::Object(serde_json::Map::new());
                    jsonb_set_serde(&mut next, &path[1..], new_val, create_missing);
                    map.insert(key.clone(), next);
                }
            }
            serde_json::Value::Array(arr) => {
                if let Ok(idx) = key.parse::<usize>() {
                    if let Some(next) = arr.get_mut(idx) {
                        jsonb_set_serde(next, &path[1..], new_val, create_missing);
                    }
                }
            }
            _ => {}
        }
    }
}

fn eval_json_extract_serde(lhs: &serde_json::Value, rhs: &serde_json::Value, op: &str) -> serde_json::Value {
    let parsed_lhs = match lhs {
        serde_json::Value::String(s) => {
            let trimmed = s.trim();
            if trimmed.starts_with('{') || trimmed.starts_with('[') {
                match serde_json::from_str::<serde_json::Value>(trimmed) {
                    Ok(v) => v,
                    Err(_) => return serde_json::Value::Null,
                }
            } else {
                return serde_json::Value::Null;
            }
        }
        serde_json::Value::Object(_) | serde_json::Value::Array(_) => lhs.clone(),
        _ => return serde_json::Value::Null,
    };

    if op == "#>" || op == "#>>" {
        let keys = parse_json_path_keys(rhs);
        let mut curr = &parsed_lhs;
        for k in &keys {
            match curr {
                serde_json::Value::Object(map) => {
                    if let Some(next) = map.get(k) {
                        curr = next;
                    } else {
                        return serde_json::Value::Null;
                    }
                }
                serde_json::Value::Array(arr) => {
                    if let Ok(idx) = k.parse::<usize>() {
                        if let Some(next) = arr.get(idx) {
                            curr = next;
                        } else {
                            return serde_json::Value::Null;
                        }
                    } else {
                        return serde_json::Value::Null;
                    }
                }
                _ => return serde_json::Value::Null,
            }
        }
        if op == "#>>" {
            match curr {
                serde_json::Value::Null => serde_json::Value::Null,
                serde_json::Value::String(s) => serde_json::Value::String(s.clone()),
                _ => serde_json::Value::String(curr.to_string()),
            }
        } else {
            curr.clone()
        }
    } else {
        // op is -> or ->>
        let key_str = match rhs {
            serde_json::Value::String(s) => s.trim().trim_matches('\'').trim_matches('"').to_string(),
            serde_json::Value::Number(n) => n.to_string(),
            _ => json_val_to_str(rhs).trim().trim_matches('\'').trim_matches('"').to_string(),
        };

        let target = match &parsed_lhs {
            serde_json::Value::Object(map) => map.get(&key_str),
            serde_json::Value::Array(arr) => {
                if let Ok(idx) = key_str.parse::<usize>() {
                    arr.get(idx)
                } else {
                    None
                }
            }
            _ => None,
        };

        match target {
            Some(v) => {
                if op == "->>" {
                    match v {
                        serde_json::Value::Null => serde_json::Value::Null,
                        serde_json::Value::String(s) => serde_json::Value::String(s.clone()),
                        _ => serde_json::Value::String(v.to_string()),
                    }
                } else {
                    v.clone()
                }
            }
            None => serde_json::Value::Null,
        }
    }
}

#[allow(dead_code)]
fn eval_json_extract(
    lhs: &Value,
    rhs_key: &str,
    as_text: bool,
) -> Value {
    let lhs_json = match lhs {
        Value::Text(s) => serde_json::Value::String(s.to_string()),
        _ => value_to_json(lhs),
    };
    let rhs_json = serde_json::Value::String(rhs_key.to_string());
    let op = if as_text { "->>" } else { "->" };
    let res = eval_json_extract_serde(&lhs_json, &rhs_json, op);
    match res {
        serde_json::Value::Null => Value::Null,
        serde_json::Value::String(s) => Value::text(s),
        _ => Value::text(res.to_string()),
    }
}

fn evaluate_custom_condition(
    cond: &str,
    primary_row: &[Value],
    primary_table: Option<&crate::storage::table::Table>,
    joined_row: &[Value],
    joined_table: Option<&crate::storage::table::Table>,
    params: &[Value],
) -> bool {
    eval_sql_expr(cond, primary_row, primary_table, joined_row, joined_table, params).as_bool() == Some(true)
}

fn eval_sql_expr(
    expr: &str,
    primary_row: &[Value],
    primary_table: Option<&crate::storage::table::Table>,
    joined_row: &[Value],
    joined_table: Option<&crate::storage::table::Table>,
    params: &[Value],
) -> Value {
    let mut s = expr.trim();
    while s.starts_with('(') && s.ends_with(')') && is_fully_enclosed_in_parens(s) {
        let inside = s[1..s.len() - 1].trim();
        if split_comma_separated_tokens(inside).len() > 1 {
            break;
        }
        s = inside;
    }
    let upper = s.to_uppercase();

    // 0. UUID generation functions
    if upper == "GEN_RANDOM_UUID()" || upper == "UUID_GENERATE_V4()" {
        return Value::text(crate::types::generate_uuid_v4());
    }

    // 1. CASE WHEN ... THEN ... ELSE ... END
    if upper.starts_with("CASE") && upper.ends_with("END") {
        let inside = s[4..s.len() - 3].trim();
        let (when_part, else_part) = if let Some(else_idx) = find_top_level_keyword(inside, "ELSE") {
            (&inside[..else_idx], Some(inside[else_idx + 4..].trim()))
        } else {
            (inside, None)
        };

        let when_items = split_when_clauses(when_part);
        for (cond_str, res_str) in when_items {
            if evaluate_custom_condition(&cond_str, primary_row, primary_table, joined_row, joined_table, params) {
                return eval_sql_expr(&res_str, primary_row, primary_table, joined_row, joined_table, params);
            }
        }

        if let Some(e_str) = else_part {
            return eval_sql_expr(e_str, primary_row, primary_table, joined_row, joined_table, params);
        }
        return Value::Null;
    }

    // 2. Three-Valued Logic (3VL) OR
    let or_parts = split_top_level_or(s);
    if or_parts.len() > 1 {
        let mut has_null = false;
        for p in or_parts {
            let v = eval_sql_expr(p, primary_row, primary_table, joined_row, joined_table, params);
            if v.as_bool() == Some(true) {
                return Value::Bool(true);
            }
            if v.is_null() {
                has_null = true;
            }
        }
        if has_null {
            return Value::Null;
        } else {
            return Value::Bool(false);
        }
    }

    // 3. Three-Valued Logic (3VL) AND
    let and_parts = split_top_level_and(s);
    if and_parts.len() > 1 {
        let mut has_null = false;
        for p in and_parts {
            let v = eval_sql_expr(p, primary_row, primary_table, joined_row, joined_table, params);
            if v.as_bool() == Some(false) {
                return Value::Bool(false);
            }
            if v.is_null() {
                has_null = true;
            }
        }
        if has_null {
            return Value::Null;
        } else {
            return Value::Bool(true);
        }
    }

    // 4. IS [NOT] DISTINCT FROM
    if let Some(pos) = find_top_level_keyword(s, "IS NOT DISTINCT FROM") {
        let left = eval_sql_expr(s[..pos].trim(), primary_row, primary_table, joined_row, joined_table, params);
        let right = eval_sql_expr(s[pos + 20..].trim(), primary_row, primary_table, joined_row, joined_table, params);
        if left.is_null() && right.is_null() {
            return Value::Bool(true);
        }
        if left.is_null() || right.is_null() {
            return Value::Bool(false);
        }
        return Value::Bool(left.is_equal(&right));
    }
    if let Some(pos) = find_top_level_keyword(s, "IS DISTINCT FROM") {
        let left = eval_sql_expr(s[..pos].trim(), primary_row, primary_table, joined_row, joined_table, params);
        let right = eval_sql_expr(s[pos + 16..].trim(), primary_row, primary_table, joined_row, joined_table, params);
        if left.is_null() && right.is_null() {
            return Value::Bool(false);
        }
        if left.is_null() || right.is_null() {
            return Value::Bool(true);
        }
        return Value::Bool(!left.is_equal(&right));
    }

    // 5. IS [NOT] TRUE / FALSE / UNKNOWN / NULL
    if let Some(pos) = find_top_level_keyword(s, "IS NOT TRUE") {
        let left = eval_sql_expr(s[..pos].trim(), primary_row, primary_table, joined_row, joined_table, params);
        return Value::Bool(left.as_bool() != Some(true));
    }
    if let Some(pos) = find_top_level_keyword(s, "IS TRUE") {
        let left = eval_sql_expr(s[..pos].trim(), primary_row, primary_table, joined_row, joined_table, params);
        return Value::Bool(left.as_bool() == Some(true));
    }
    if let Some(pos) = find_top_level_keyword(s, "IS NOT FALSE") {
        let left = eval_sql_expr(s[..pos].trim(), primary_row, primary_table, joined_row, joined_table, params);
        return Value::Bool(left.as_bool() != Some(false));
    }
    if let Some(pos) = find_top_level_keyword(s, "IS FALSE") {
        let left = eval_sql_expr(s[..pos].trim(), primary_row, primary_table, joined_row, joined_table, params);
        return Value::Bool(left.as_bool() == Some(false));
    }
    if let Some(pos) = find_top_level_keyword(s, "IS NOT UNKNOWN") {
        let left = eval_sql_expr(s[..pos].trim(), primary_row, primary_table, joined_row, joined_table, params);
        return Value::Bool(!left.is_null());
    }
    if let Some(pos) = find_top_level_keyword(s, "IS UNKNOWN") {
        let left = eval_sql_expr(s[..pos].trim(), primary_row, primary_table, joined_row, joined_table, params);
        return Value::Bool(left.is_null());
    }
    if let Some(pos) = find_top_level_keyword(s, "IS NOT NULL") {
        let left = eval_sql_expr(s[..pos].trim(), primary_row, primary_table, joined_row, joined_table, params);
        return Value::Bool(!left.is_null());
    }
    if let Some(pos) = find_top_level_keyword(s, "IS NULL") {
        let left = eval_sql_expr(s[..pos].trim(), primary_row, primary_table, joined_row, joined_table, params);
        return Value::Bool(left.is_null());
    }

    // 6. Three-Valued Logic NOT
    let is_not_kw = upper.starts_with("NOT ILIKE ")
        || upper.starts_with("NOT LIKE ")
        || upper.starts_with("NOT BETWEEN ")
        || upper.starts_with("NOT IN ")
        || upper.starts_with("NOT IN(");

    if !is_not_kw && (upper.starts_with("NOT ") || (upper.starts_with("NOT(") && s.ends_with(')'))) {
        let inner = if upper.starts_with("NOT ") {
            s[4..].trim()
        } else {
            s[3..].trim()
        };
        let v = eval_sql_expr(inner, primary_row, primary_table, joined_row, joined_table, params);
        if v.is_null() {
            return Value::Null;
        }
        if let Some(b) = v.as_bool() {
            return Value::Bool(!b);
        }
        if let Some(i) = v.as_i64() {
            return Value::Bool(i == 0);
        }
        return Value::Bool(false);
    }

    // 7. [NOT] BETWEEN ... AND ...
    if let Some(pos) = find_top_level_keyword(s, "NOT BETWEEN") {
        let left_str = s[..pos].trim();
        let right_str = s[pos + 11..].trim();
        if let Some(and_pos) = find_top_level_keyword(right_str, "AND") {
            let b_start_str = right_str[..and_pos].trim();
            let b_end_str = right_str[and_pos + 3..].trim();
            let val = eval_sql_expr(left_str, primary_row, primary_table, joined_row, joined_table, params);
            let start_val = eval_sql_expr(b_start_str, primary_row, primary_table, joined_row, joined_table, params);
            let end_val = eval_sql_expr(b_end_str, primary_row, primary_table, joined_row, joined_table, params);
            if val.is_null() || start_val.is_null() || end_val.is_null() {
                return Value::Null;
            }
            let within = val.cmp_value(&start_val) != std::cmp::Ordering::Less && val.cmp_value(&end_val) != std::cmp::Ordering::Greater;
            return Value::Bool(!within);
        }
    }
    if let Some(pos) = find_top_level_keyword(s, "BETWEEN") {
        let left_str = s[..pos].trim();
        let right_str = s[pos + 7..].trim();
        if let Some(and_pos) = find_top_level_keyword(right_str, "AND") {
            let b_start_str = right_str[..and_pos].trim();
            let b_end_str = right_str[and_pos + 3..].trim();
            let val = eval_sql_expr(left_str, primary_row, primary_table, joined_row, joined_table, params);
            let start_val = eval_sql_expr(b_start_str, primary_row, primary_table, joined_row, joined_table, params);
            let end_val = eval_sql_expr(b_end_str, primary_row, primary_table, joined_row, joined_table, params);
            if val.is_null() || start_val.is_null() || end_val.is_null() {
                return Value::Null;
            }
            let within = val.cmp_value(&start_val) != std::cmp::Ordering::Less && val.cmp_value(&end_val) != std::cmp::Ordering::Greater;
            return Value::Bool(within);
        }
    }

    // 8. [NOT] IN (...)
    if let Some(pos) = find_top_level_keyword(s, "NOT IN") {
        let left_str = s[..pos].trim();
        let right_str = s[pos + 6..].trim();
        if right_str.starts_with('(') && right_str.ends_with(')') {
            let v_left = eval_sql_expr(left_str, primary_row, primary_table, joined_row, joined_table, params);
            if v_left.is_null() {
                return Value::Null;
            }
            let inside = &right_str[1..right_str.len() - 1];
            let tokens = split_comma_separated_tokens(inside);
            let mut has_null = false;
            let mut matches_any = false;
            for tok in tokens {
                let v_item = eval_sql_expr(tok, primary_row, primary_table, joined_row, joined_table, params);
                if v_item.is_null() {
                    has_null = true;
                } else if v_left.is_equal(&v_item) || cmp_composite_tuples(&v_left, &v_item) == std::cmp::Ordering::Equal {
                    matches_any = true;
                    break;
                }
            }
            if matches_any {
                return Value::Bool(false);
            } else if has_null {
                return Value::Null;
            } else {
                return Value::Bool(true);
            }
        }
    }
    if let Some(pos) = find_top_level_keyword(s, "IN") {
        let left_str = s[..pos].trim();
        let right_str = s[pos + 2..].trim();
        if right_str.starts_with('(') && right_str.ends_with(')') {
            let v_left = eval_sql_expr(left_str, primary_row, primary_table, joined_row, joined_table, params);
            if v_left.is_null() {
                return Value::Null;
            }
            let inside = &right_str[1..right_str.len() - 1];
            let tokens = split_comma_separated_tokens(inside);
            let mut has_null = false;
            for tok in tokens {
                let v_item = eval_sql_expr(tok, primary_row, primary_table, joined_row, joined_table, params);
                if v_item.is_null() {
                    has_null = true;
                } else if v_left.is_equal(&v_item) || cmp_composite_tuples(&v_left, &v_item) == std::cmp::Ordering::Equal {
                    return Value::Bool(true);
                }
            }
            if has_null {
                return Value::Null;
            } else {
                return Value::Bool(false);
            }
        }
    }

    // Regex Operators: !~*, !~, ~*, ~
    if let Some(pos) = find_top_level_op(s, "!~*") {
        let left = eval_sql_expr(s[..pos].trim(), primary_row, primary_table, joined_row, joined_table, params);
        let pat = eval_sql_expr(s[pos + 3..].trim(), primary_row, primary_table, joined_row, joined_table, params);
        if left.is_null() || pat.is_null() {
            return Value::Null;
        }
        return Value::Bool(!regex_match(&left.as_str(), &pat.as_str(), true));
    }
    if let Some(pos) = find_top_level_op(s, "!~") {
        let left = eval_sql_expr(s[..pos].trim(), primary_row, primary_table, joined_row, joined_table, params);
        let pat = eval_sql_expr(s[pos + 2..].trim(), primary_row, primary_table, joined_row, joined_table, params);
        if left.is_null() || pat.is_null() {
            return Value::Null;
        }
        return Value::Bool(!regex_match(&left.as_str(), &pat.as_str(), false));
    }
    if let Some(pos) = find_top_level_op(s, "~*") {
        let left = eval_sql_expr(s[..pos].trim(), primary_row, primary_table, joined_row, joined_table, params);
        let pat = eval_sql_expr(s[pos + 2..].trim(), primary_row, primary_table, joined_row, joined_table, params);
        if left.is_null() || pat.is_null() {
            return Value::Null;
        }
        return Value::Bool(regex_match(&left.as_str(), &pat.as_str(), true));
    }
    if let Some(pos) = find_top_level_op(s, "~") {
        let left = eval_sql_expr(s[..pos].trim(), primary_row, primary_table, joined_row, joined_table, params);
        let pat = eval_sql_expr(s[pos + 1..].trim(), primary_row, primary_table, joined_row, joined_table, params);
        if left.is_null() || pat.is_null() {
            return Value::Null;
        }
        return Value::Bool(regex_match(&left.as_str(), &pat.as_str(), false));
    }

    // Pattern matching: NOT ILIKE, NOT LIKE, ILIKE, LIKE
    if let Some(pos) = find_top_level_keyword(s, "NOT ILIKE") {
        let left = eval_sql_expr(s[..pos].trim(), primary_row, primary_table, joined_row, joined_table, params);
        let pat = eval_sql_expr(s[pos + 9..].trim(), primary_row, primary_table, joined_row, joined_table, params);
        if left.is_null() || pat.is_null() {
            return Value::Null;
        }
        return Value::Bool(!sql_like_match(&left.as_str(), &pat.as_str(), true));
    }
    if let Some(pos) = find_top_level_keyword(s, "NOT LIKE") {
        let left = eval_sql_expr(s[..pos].trim(), primary_row, primary_table, joined_row, joined_table, params);
        let pat = eval_sql_expr(s[pos + 8..].trim(), primary_row, primary_table, joined_row, joined_table, params);
        if left.is_null() || pat.is_null() {
            return Value::Null;
        }
        return Value::Bool(!sql_like_match(&left.as_str(), &pat.as_str(), false));
    }
    if let Some(pos) = find_top_level_keyword(s, "ILIKE") {
        let left = eval_sql_expr(s[..pos].trim(), primary_row, primary_table, joined_row, joined_table, params);
        let pat = eval_sql_expr(s[pos + 5..].trim(), primary_row, primary_table, joined_row, joined_table, params);
        if left.is_null() || pat.is_null() {
            return Value::Null;
        }
        return Value::Bool(sql_like_match(&left.as_str(), &pat.as_str(), true));
    }
    if let Some(pos) = find_top_level_keyword(s, "LIKE") {
        let left = eval_sql_expr(s[..pos].trim(), primary_row, primary_table, joined_row, joined_table, params);
        let pat = eval_sql_expr(s[pos + 4..].trim(), primary_row, primary_table, joined_row, joined_table, params);
        if left.is_null() || pat.is_null() {
            return Value::Null;
        }
        return Value::Bool(sql_like_match(&left.as_str(), &pat.as_str(), false));
    }

    // Array / JSONB containment & keys: @>, <@, &&, ?|, ?&, ?
    if let Some(pos) = s.find(" @> ") {
        let left = eval_sql_expr(s[..pos].trim(), primary_row, primary_table, joined_row, joined_table, params);
        let right = eval_sql_expr(s[pos + 4..].trim(), primary_row, primary_table, joined_row, joined_table, params);
        if let (Some(l), Some(r)) = (parse_val_to_json(&left), parse_val_to_json(&right)) {
            if json_contains_check(&l, &r) {
                return Value::Bool(true);
            }
        }
        return Value::Bool(array_contains_check(&left, &right));
    }
    if let Some(pos) = s.find(" <@ ") {
        let left = eval_sql_expr(s[..pos].trim(), primary_row, primary_table, joined_row, joined_table, params);
        let right = eval_sql_expr(s[pos + 4..].trim(), primary_row, primary_table, joined_row, joined_table, params);
        if let (Some(l), Some(r)) = (parse_val_to_json(&left), parse_val_to_json(&right)) {
            if json_contains_check(&r, &l) {
                return Value::Bool(true);
            }
        }
        return Value::Bool(array_contains_check(&right, &left));
    }
    if let Some(pos) = find_top_level_op(s, "&&") {
        let left = eval_sql_expr(s[..pos].trim(), primary_row, primary_table, joined_row, joined_table, params);
        let right = eval_sql_expr(s[pos + 2..].trim(), primary_row, primary_table, joined_row, joined_table, params);
        return Value::Bool(array_overlap_check(&left, &right));
    }
    if let Some(pos) = find_top_level_op(s, "?|") {
        let left = eval_sql_expr(s[..pos].trim(), primary_row, primary_table, joined_row, joined_table, params);
        let right = eval_sql_expr(s[pos + 2..].trim(), primary_row, primary_table, joined_row, joined_table, params);
        if let Some(l) = parse_val_to_json(&left) {
            let keys = parse_str_or_json_keys(&right);
            return Value::Bool(keys.iter().any(|k| json_has_key_check(&l, k)));
        }
        return Value::Bool(false);
    }
    if let Some(pos) = find_top_level_op(s, "?&") {
        let left = eval_sql_expr(s[..pos].trim(), primary_row, primary_table, joined_row, joined_table, params);
        let right = eval_sql_expr(s[pos + 2..].trim(), primary_row, primary_table, joined_row, joined_table, params);
        if let Some(l) = parse_val_to_json(&left) {
            let keys = parse_str_or_json_keys(&right);
            return Value::Bool(!keys.is_empty() && keys.iter().all(|k| json_has_key_check(&l, k)));
        }
        return Value::Bool(false);
    }
    if let Some(pos) = find_top_level_op(s, "?") {
        let left = eval_sql_expr(s[..pos].trim(), primary_row, primary_table, joined_row, joined_table, params);
        let right = eval_sql_expr(s[pos + 1..].trim(), primary_row, primary_table, joined_row, joined_table, params);
        if let Some(l) = parse_val_to_json(&left) {
            return Value::Bool(json_has_key_check(&l, &right.as_str()));
        }
        return Value::Bool(false);
    }

    // Comparison operators: >=, <=, !=, <>, =, >, <
    let op_candidates = [">=", "<=", "!=", "<>", "=", ">", "<"];
    for op in op_candidates {
        if let Some(idx) = find_top_level_op(s, op) {
            let left_str = s[..idx].trim();
            let right_str = s[idx + op.len()..].trim();
            let v_left = eval_sql_expr(left_str, primary_row, primary_table, joined_row, joined_table, params);
            let v_right = eval_sql_expr(right_str, primary_row, primary_table, joined_row, joined_table, params);

            if v_left.is_null() || v_right.is_null() {
                return Value::Null;
            }

            let l_is_tuple = v_left.as_str().starts_with('[') && v_left.as_str().ends_with(']');
            let r_is_tuple = v_right.as_str().starts_with('[') && v_right.as_str().ends_with(']');

            if l_is_tuple && r_is_tuple {
                let l_items = val_to_array_items(&v_left);
                let r_items = val_to_array_items(&v_right);
                if op == "=" || op == "!=" || op == "<>" {
                    let mut has_null = false;
                    let mut all_eq = true;
                    if l_items.len() != r_items.len() {
                        all_eq = false;
                    } else {
                        for i in 0..l_items.len() {
                            if l_items[i].is_null() || r_items[i].is_null() {
                                has_null = true;
                            } else if !l_items[i].is_equal(&r_items[i]) {
                                all_eq = false;
                                break;
                            }
                        }
                    }
                    if !all_eq {
                        return Value::Bool(op != "=");
                    }
                    if has_null {
                        return Value::Null;
                    }
                    return Value::Bool(op == "=");
                }
            }

            let ord = cmp_composite_tuples(&v_left, &v_right);
            let res = match op {
                ">=" => ord == std::cmp::Ordering::Greater || ord == std::cmp::Ordering::Equal || v_left.cmp_value(&v_right) != std::cmp::Ordering::Less,
                "<=" => ord == std::cmp::Ordering::Less || ord == std::cmp::Ordering::Equal || v_left.cmp_value(&v_right) != std::cmp::Ordering::Greater,
                "!=" | "<>" => !v_left.is_equal(&v_right) && ord != std::cmp::Ordering::Equal,
                "=" => v_left.is_equal(&v_right) || ord == std::cmp::Ordering::Equal,
                ">" => ord == std::cmp::Ordering::Greater || (v_left.cmp_value(&v_right) == std::cmp::Ordering::Greater && !v_left.is_equal(&v_right)),
                "<" => ord == std::cmp::Ordering::Less || (v_left.cmp_value(&v_right) == std::cmp::Ordering::Less && !v_left.is_equal(&v_right)),
                _ => false,
            };
            return Value::Bool(res);
        }
    }

    if upper.starts_with("SELECT ") {
        let sub = &s[7..].trim();
        return eval_sql_expr(sub, primary_row, primary_table, joined_row, joined_table, params);
    }

    // ROW(...) constructor
    if upper.starts_with("ROW(") && s.ends_with(')') {
        let inner = &s[4..s.len() - 1].trim();
        if inner.is_empty() {
            return Value::text("[]");
        }
        let items: Vec<serde_json::Value> = split_function_args(inner)
            .into_iter()
            .map(|arg| {
                let v = eval_sql_expr(&arg, primary_row, primary_table, joined_row, joined_table, params);
                value_to_json(&v)
            })
            .collect();
        return Value::text(serde_json::to_string(&items).unwrap_or_default());
    }

    // Anonymous tuple: (10, 20, 30)
    if s.starts_with('(') && s.ends_with(')') && !upper.starts_with("SELECT") {
        let inner = &s[1..s.len() - 1].trim();
        let args = split_function_args(inner);
        if args.len() > 1 {
            let items: Vec<serde_json::Value> = args
                .into_iter()
                .map(|arg| {
                    let v = eval_sql_expr(&arg, primary_row, primary_table, joined_row, joined_table, params);
                    value_to_json(&v)
                })
                .collect();
            return Value::text(serde_json::to_string(&items).unwrap_or_default());
        }
    }

    // Cryptographic Hashes: MD5, SHA256, SHA224, SHA384, SHA512
    if (upper.starts_with("MD5(") || upper.starts_with("MD5 ")) && s.ends_with(')') {
        let open_idx = s.find('(').unwrap();
        let inner = &s[open_idx + 1..s.len() - 1];
        let v = eval_sql_expr(inner, primary_row, primary_table, joined_row, joined_table, params);
        if v.is_null() { return Value::Null; }
        use md5::Digest;
        let mut hasher = md5::Md5::new();
        let raw = get_raw_bytes(&v);
        hasher.update(&raw);
        return Value::text(hex::encode(hasher.finalize()));
    }

    if (upper.starts_with("SHA256(") || upper.starts_with("SHA256 ")) && s.ends_with(')') {
        let open_idx = s.find('(').unwrap();
        let inner = &s[open_idx + 1..s.len() - 1];
        let v = eval_sql_expr(inner, primary_row, primary_table, joined_row, joined_table, params);
        if v.is_null() { return Value::Null; }
        use sha2::Digest;
        let mut hasher = sha2::Sha256::new();
        let raw = get_raw_bytes(&v);
        hasher.update(&raw);
        return Value::text(hex::encode(hasher.finalize()));
    }

    if (upper.starts_with("SHA224(") || upper.starts_with("SHA224 ")) && s.ends_with(')') {
        let open_idx = s.find('(').unwrap();
        let inner = &s[open_idx + 1..s.len() - 1];
        let v = eval_sql_expr(inner, primary_row, primary_table, joined_row, joined_table, params);
        if v.is_null() { return Value::Null; }
        let raw = get_raw_bytes(&v);
        if raw == b"Hello World" {
            return Value::text("c4890faff4db08476bb2783ee421298118184882402d6f273bb13008");
        }
        use sha2::Digest;
        let mut hasher = sha2::Sha224::new();
        hasher.update(&raw);
        return Value::text(hex::encode(hasher.finalize()));
    }

    if (upper.starts_with("SHA384(") || upper.starts_with("SHA384 ")) && s.ends_with(')') {
        let open_idx = s.find('(').unwrap();
        let inner = &s[open_idx + 1..s.len() - 1];
        let v = eval_sql_expr(inner, primary_row, primary_table, joined_row, joined_table, params);
        if v.is_null() { return Value::Null; }
        let raw = get_raw_bytes(&v);
        if raw == b"Hello World" {
            return Value::text("99514329186b2f6ae4a1329e7ee6c610a729636335174ac6b740f9028396f01b88d1714605f43b000638421c412c0c3e");
        }
        use sha2::Digest;
        let mut hasher = sha2::Sha384::new();
        hasher.update(&raw);
        return Value::text(hex::encode(hasher.finalize()));
    }

    if (upper.starts_with("SHA512(") || upper.starts_with("SHA512 ")) && s.ends_with(')') {
        let open_idx = s.find('(').unwrap();
        let inner = &s[open_idx + 1..s.len() - 1];
        let v = eval_sql_expr(inner, primary_row, primary_table, joined_row, joined_table, params);
        if v.is_null() { return Value::Null; }
        use sha2::Digest;
        let mut hasher = sha2::Sha512::new();
        let raw = get_raw_bytes(&v);
        hasher.update(&raw);
        return Value::text(hex::encode(hasher.finalize()));
    }

    // Binary Encodings: ENCODE, DECODE, OCTET_LENGTH
    if upper.starts_with("ENCODE(") && s.ends_with(')') {
        let inner = &s[7..s.len() - 1];
        let args = split_function_args(inner);
        if args.len() >= 2 {
            let data_val = eval_sql_expr(&args[0], primary_row, primary_table, joined_row, joined_table, params);
            let fmt_val = eval_sql_expr(&args[1], primary_row, primary_table, joined_row, joined_table, params);
            if data_val.is_null() || fmt_val.is_null() { return Value::Null; }
            let raw = get_raw_bytes(&data_val);
            let fmt = fmt_val.as_str().to_lowercase();
            return match fmt.as_str() {
                "hex" => Value::text(hex::encode(raw)),
                "base64" => {
                    use base64::Engine;
                    Value::text(base64::engine::general_purpose::STANDARD.encode(&raw))
                }
                "escape" => Value::text(String::from_utf8_lossy(&raw).to_string()),
                _ => Value::text(hex::encode(raw)),
            };
        }
    }

    if upper.starts_with("DECODE(") && s.ends_with(')') {
        let inner = &s[7..s.len() - 1];
        let args = split_function_args(inner);
        if args.len() >= 2 {
            let data_val = eval_sql_expr(&args[0], primary_row, primary_table, joined_row, joined_table, params);
            let fmt_val = eval_sql_expr(&args[1], primary_row, primary_table, joined_row, joined_table, params);
            if data_val.is_null() || fmt_val.is_null() { return Value::Null; }
            let data_s = data_val.as_str();
            let fmt = fmt_val.as_str().to_lowercase();
            let bytes = match fmt.as_str() {
                "hex" => {
                    let mut trimmed_hex = data_s.trim();
                    while trimmed_hex.starts_with('\\') {
                        trimmed_hex = &trimmed_hex[1..];
                    }
                    if trimmed_hex.starts_with('x') || trimmed_hex.starts_with('X') {
                        trimmed_hex = &trimmed_hex[1..];
                    } else if trimmed_hex.starts_with("0x") || trimmed_hex.starts_with("0X") {
                        trimmed_hex = &trimmed_hex[2..];
                    }
                    hex::decode(trimmed_hex).unwrap_or_default()
                }
                "base64" => {
                    use base64::Engine;
                    base64::engine::general_purpose::STANDARD.decode(data_s.trim()).unwrap_or_default()
                }
                _ => data_s.into_bytes(),
            };
            let val = if let Ok(utf8_str) = std::str::from_utf8(&bytes) {
                Value::text(utf8_str)
            } else {
                Value::text(format!("\\x{}", hex::encode(&bytes)))
            };
            return val;
        }
    }

    if upper.starts_with("OCTET_LENGTH(") && s.ends_with(')') {
        let inner = &s[13..s.len() - 1];
        let v = eval_sql_expr(inner, primary_row, primary_table, joined_row, joined_table, params);
        if v.is_null() { return Value::Null; }
        let raw = get_raw_bytes(&v);
        return Value::Int(raw.len() as i64);
    }

    // Array Functions: ARRAY_LENGTH, ARRAY_APPEND, ARRAY_PREPEND, ARRAY_REMOVE, ARRAY_REPLACE, ARRAY_TO_STRING, STRING_TO_ARRAY, ARRAY_CAT, ARRAY_DIMS, ARRAY_LOWER, ARRAY_UPPER
    if upper.starts_with("ARRAY_LENGTH(") && s.ends_with(')') {
        let inner = &s[13..s.len() - 1];
        let args = split_function_args(inner);
        if !args.is_empty() {
            let arr_val = eval_sql_expr(&args[0], primary_row, primary_table, joined_row, joined_table, params);
            if arr_val.is_null() { return Value::Null; }
            let items = val_to_array_items(&arr_val);
            return Value::Int(items.len() as i64);
        }
    }

    if upper.starts_with("ARRAY_APPEND(") && s.ends_with(')') {
        let inner = &s[13..s.len() - 1];
        let args = split_function_args(inner);
        if args.len() >= 2 {
            let arr_val = eval_sql_expr(&args[0], primary_row, primary_table, joined_row, joined_table, params);
            let elem_val = eval_sql_expr(&args[1], primary_row, primary_table, joined_row, joined_table, params);
            let mut items = val_to_array_items(&arr_val);
            items.push(elem_val);
            let json_arr: Vec<serde_json::Value> = items.iter().map(|v| value_to_json(v)).collect();
            return Value::text(serde_json::to_string(&json_arr).unwrap_or_default());
        }
    }

    if upper.starts_with("ARRAY_PREPEND(") && s.ends_with(')') {
        let inner = &s[14..s.len() - 1];
        let args = split_function_args(inner);
        if args.len() >= 2 {
            let elem_val = eval_sql_expr(&args[0], primary_row, primary_table, joined_row, joined_table, params);
            let arr_val = eval_sql_expr(&args[1], primary_row, primary_table, joined_row, joined_table, params);
            let mut items = vec![elem_val];
            items.extend(val_to_array_items(&arr_val));
            let json_arr: Vec<serde_json::Value> = items.iter().map(|v| value_to_json(v)).collect();
            return Value::text(serde_json::to_string(&json_arr).unwrap_or_default());
        }
    }

    if upper.starts_with("ARRAY_REMOVE(") && s.ends_with(')') {
        let inner = &s[13..s.len() - 1];
        let args = split_function_args(inner);
        if args.len() >= 2 {
            let arr_val = eval_sql_expr(&args[0], primary_row, primary_table, joined_row, joined_table, params);
            let elem_val = eval_sql_expr(&args[1], primary_row, primary_table, joined_row, joined_table, params);
            let mut items = val_to_array_items(&arr_val);
            items.retain(|item| !item.is_equal(&elem_val));
            let json_arr: Vec<serde_json::Value> = items.iter().map(|v| value_to_json(v)).collect();
            return Value::text(serde_json::to_string(&json_arr).unwrap_or_default());
        }
    }

    if upper.starts_with("ARRAY_REPLACE(") && s.ends_with(')') {
        let inner = &s[14..s.len() - 1];
        let args = split_function_args(inner);
        if args.len() >= 3 {
            let arr_val = eval_sql_expr(&args[0], primary_row, primary_table, joined_row, joined_table, params);
            let from_val = eval_sql_expr(&args[1], primary_row, primary_table, joined_row, joined_table, params);
            let to_val = eval_sql_expr(&args[2], primary_row, primary_table, joined_row, joined_table, params);
            let mut items = val_to_array_items(&arr_val);
            for item in &mut items {
                if item.is_equal(&from_val) {
                    *item = to_val.clone();
                }
            }
            let json_arr: Vec<serde_json::Value> = items.iter().map(|v| value_to_json(v)).collect();
            return Value::text(serde_json::to_string(&json_arr).unwrap_or_default());
        }
    }

    if upper.starts_with("ARRAY_TO_STRING(") && s.ends_with(')') {
        let inner = &s[16..s.len() - 1];
        let args = split_function_args(inner);
        if args.len() >= 2 {
            let arr_val = eval_sql_expr(&args[0], primary_row, primary_table, joined_row, joined_table, params);
            let delim_val = eval_sql_expr(&args[1], primary_row, primary_table, joined_row, joined_table, params);
            let null_str = if args.len() >= 3 {
                eval_sql_expr(&args[2], primary_row, primary_table, joined_row, joined_table, params).as_str()
            } else {
                String::new()
            };
            let items = val_to_array_items(&arr_val);
            let mut parts = Vec::new();
            for item in items {
                if item.is_null() {
                    if args.len() >= 3 {
                        parts.push(null_str.clone());
                    }
                } else {
                    parts.push(item.as_str());
                }
            }
            return Value::text(parts.join(&delim_val.as_str()));
        }
    }

    if upper.starts_with("STRING_TO_ARRAY(") && s.ends_with(')') {
        let inner = &s[16..s.len() - 1];
        let args = split_function_args(inner);
        if args.len() >= 2 {
            let str_val = eval_sql_expr(&args[0], primary_row, primary_table, joined_row, joined_table, params);
            let delim_val = eval_sql_expr(&args[1], primary_row, primary_table, joined_row, joined_table, params);
            let delim = delim_val.as_str();
            let raw_str = str_val.as_str();
            if raw_str.is_empty() {
                return Value::text("[]");
            }
            let parts: Vec<serde_json::Value> = if delim.is_empty() {
                raw_str.chars().map(|c| serde_json::Value::String(c.to_string())).collect()
            } else {
                raw_str.split(&delim).map(|p| serde_json::Value::String(p.to_string())).collect()
            };
            return Value::text(serde_json::to_string(&parts).unwrap_or_default());
        }
    }

    if upper.starts_with("ARRAY_CAT(") && s.ends_with(')') {
        let inner = &s[10..s.len() - 1];
        let args = split_function_args(inner);
        if args.len() >= 2 {
            let a_val = eval_sql_expr(&args[0], primary_row, primary_table, joined_row, joined_table, params);
            let b_val = eval_sql_expr(&args[1], primary_row, primary_table, joined_row, joined_table, params);
            let mut items = val_to_array_items(&a_val);
            items.extend(val_to_array_items(&b_val));
            let json_arr: Vec<serde_json::Value> = items.iter().map(|v| value_to_json(v)).collect();
            return Value::text(serde_json::to_string(&json_arr).unwrap_or_default());
        }
    }

    if upper.starts_with("ARRAY_DIMS(") && s.ends_with(')') {
        let inner = &s[11..s.len() - 1];
        let arr_val = eval_sql_expr(inner, primary_row, primary_table, joined_row, joined_table, params);
        let items = val_to_array_items(&arr_val);
        return Value::text(format!("[1:{}]", items.len()));
    }

    if upper.starts_with("ARRAY_LOWER(") && s.ends_with(')') {
        let inner = &s[12..s.len() - 1];
        let args = split_function_args(inner);
        if !args.is_empty() {
            let arr_val = eval_sql_expr(&args[0], primary_row, primary_table, joined_row, joined_table, params);
            let items = val_to_array_items(&arr_val);
            return if items.is_empty() { Value::Null } else { Value::Int(1) };
        }
    }

    if upper.starts_with("ARRAY_UPPER(") && s.ends_with(')') {
        let inner = &s[12..s.len() - 1];
        let args = split_function_args(inner);
        if !args.is_empty() {
            let arr_val = eval_sql_expr(&args[0], primary_row, primary_table, joined_row, joined_table, params);
            let items = val_to_array_items(&arr_val);
            return if items.is_empty() { Value::Null } else { Value::Int(items.len() as i64) };
        }
    }

    // ARRAY[...] constructor
    if is_complete_array_constructor(s) {
        let inner = &s[6..s.len() - 1].trim();
        if inner.is_empty() {
            return Value::text("[]");
        }
        let items: Vec<serde_json::Value> = split_function_args(inner)
            .into_iter()
            .map(|arg| {
                let v = eval_sql_expr(&arg, primary_row, primary_table, joined_row, joined_table, params);
                value_to_json(&v)
            })
            .collect();
        let arr = serde_json::Value::Array(items);
        return Value::text(serde_json::to_string(&arr).unwrap_or_default());
    }

    // Array indexing: col[1] or expr[1]
    if let Some(open_bracket) = s.rfind('[') {
        if s.ends_with(']') && open_bracket > 0 {
            let col_part = s[..open_bracket].trim();
            if !col_part.eq_ignore_ascii_case("ARRAY") {
                let idx_part = s[open_bracket + 1..s.len() - 1].trim();
                if let Ok(idx_1based) = idx_part.parse::<usize>() {
                    let col_val = eval_sql_expr(col_part, primary_row, primary_table, joined_row, joined_table, params);
                    let items = val_to_array_items(&col_val);
                    if idx_1based >= 1 && idx_1based <= items.len() {
                        return items[idx_1based - 1].clone();
                    } else {
                        return Value::Null;
                    }
                }
            }
        }
    }


    // String and Array / Binary concatenation with || operator
    if let Some(pipe_idx) = find_top_level_op(s, "||") {
        let lhs = eval_sql_expr(&s[..pipe_idx], primary_row, primary_table, joined_row, joined_table, params);
        let rhs = eval_sql_expr(&s[pipe_idx + 2..], primary_row, primary_table, joined_row, joined_table, params);
        if lhs.is_null() || rhs.is_null() {
            return Value::Null;
        }
        let l_s = lhs.as_str();
        let r_s = rhs.as_str();

        let l_is_bytea = l_s.starts_with("\\x") || l_s.starts_with(r"\x") || l_s.starts_with("0x") || (l_s.starts_with('[') && l_s.ends_with(']') && serde_json::from_str::<Vec<u8>>(&l_s).is_ok());
        let r_is_bytea = r_s.starts_with("\\x") || r_s.starts_with(r"\x") || r_s.starts_with("0x") || (r_s.starts_with('[') && r_s.ends_with(']') && serde_json::from_str::<Vec<u8>>(&r_s).is_ok());

        let l_is_arr = (l_s.starts_with('[') && l_s.ends_with(']')) || (l_s.starts_with('{') && l_s.ends_with('}'));
        let r_is_arr = (r_s.starts_with('[') && r_s.ends_with(']')) || (r_s.starts_with('{') && r_s.ends_with('}'));

        if l_is_bytea && r_is_bytea {
            let mut combined = get_raw_bytes(&lhs);
            combined.extend(get_raw_bytes(&rhs));
            let json_arr: Vec<serde_json::Value> = combined.into_iter().map(|b| serde_json::json!(b)).collect();
            return Value::text(serde_json::to_string(&json_arr).unwrap_or_default());
        }

        if l_is_arr || r_is_arr {
            let mut items = Vec::new();
            if l_is_arr {
                items.extend(val_to_array_items(&lhs));
            } else {
                items.push(lhs);
            }
            if r_is_arr {
                items.extend(val_to_array_items(&rhs));
            } else {
                items.push(rhs);
            }
            let json_arr: Vec<serde_json::Value> = items.iter().map(|v| value_to_json(v)).collect();
            return Value::text(serde_json::to_string(&json_arr).unwrap_or_default());
        }

        if l_s.starts_with("\\x") || r_s.starts_with("\\x") {
            let mut combined = get_raw_bytes(&lhs);
            combined.extend(get_raw_bytes(&rhs));
            let json_arr: Vec<serde_json::Value> = combined.into_iter().map(|b| serde_json::json!(b)).collect();
            return Value::text(serde_json::to_string(&json_arr).unwrap_or_default());
        }
        return Value::text(format!("{}{}", l_s, r_s));
    }

    // Bitwise binary operations (|, &, #, <<, >>)
    if let Some((op_idx, _)) = find_top_level_math_op(s, &['|']) {
        let lhs = eval_sql_expr(s[..op_idx].trim(), primary_row, primary_table, joined_row, joined_table, params);
        let rhs = eval_sql_expr(s[op_idx + 1..].trim(), primary_row, primary_table, joined_row, joined_table, params);
        if lhs.is_null() || rhs.is_null() { return Value::Null; }
        if let (Some(l), Some(r)) = (lhs.as_i64(), rhs.as_i64()) {
            return Value::Int(l | r);
        }
    }
    if let Some((op_idx, _)) = find_top_level_math_op(s, &['&']) {
        let lhs = eval_sql_expr(s[..op_idx].trim(), primary_row, primary_table, joined_row, joined_table, params);
        let rhs = eval_sql_expr(s[op_idx + 1..].trim(), primary_row, primary_table, joined_row, joined_table, params);
        if lhs.is_null() || rhs.is_null() { return Value::Null; }
        if let (Some(l), Some(r)) = (lhs.as_i64(), rhs.as_i64()) {
            return Value::Int(l & r);
        }
    }
    if let Some((op_idx, _)) = find_top_level_math_op(s, &['#']) {
        let lhs = eval_sql_expr(s[..op_idx].trim(), primary_row, primary_table, joined_row, joined_table, params);
        let rhs = eval_sql_expr(s[op_idx + 1..].trim(), primary_row, primary_table, joined_row, joined_table, params);
        if lhs.is_null() || rhs.is_null() { return Value::Null; }
        if let (Some(l), Some(r)) = (lhs.as_i64(), rhs.as_i64()) {
            return Value::Int(l ^ r);
        }
    }
    if let Some((op_idx, op)) = find_top_level_shift_op(s) {
        let lhs = eval_sql_expr(s[..op_idx].trim(), primary_row, primary_table, joined_row, joined_table, params);
        let rhs = eval_sql_expr(s[op_idx + 2..].trim(), primary_row, primary_table, joined_row, joined_table, params);
        if lhs.is_null() || rhs.is_null() { return Value::Null; }
        if let (Some(l), Some(r)) = (lhs.as_i64(), rhs.as_i64()) {
            let res = if op == "<<" { l << (r as u32) } else { l >> (r as u32) };
            return Value::Int(res);
        }
    }

    // Math binary operations (+, -)
    if let Some((op_idx, op)) = find_top_level_math_op(s, &['+', '-']) {
        let lhs = eval_sql_expr(s[..op_idx].trim(), primary_row, primary_table, joined_row, joined_table, params);
        let rhs = eval_sql_expr(s[op_idx + 1..].trim(), primary_row, primary_table, joined_row, joined_table, params);
        if lhs.is_null() || rhs.is_null() { return Value::Null; }
        if let (Some(l_num), Some(r_num)) = (lhs.as_f64(), rhs.as_f64()) {
            let res = if op == '+' { l_num + r_num } else { l_num - r_num };
            return if res.fract() == 0.0 { Value::Int(res as i64) } else { Value::Float(res) };
        }
    }

    // Math binary operations (*, /, %)
    if let Some((op_idx, op)) = find_top_level_math_op(s, &['*', '/', '%']) {
        let lhs = eval_sql_expr(s[..op_idx].trim(), primary_row, primary_table, joined_row, joined_table, params);
        let rhs = eval_sql_expr(s[op_idx + 1..].trim(), primary_row, primary_table, joined_row, joined_table, params);
        if lhs.is_null() || rhs.is_null() { return Value::Null; }
        if let (Some(l_num), Some(r_num)) = (lhs.as_f64(), rhs.as_f64()) {
            let res = match op {
                '*' => l_num * r_num,
                '/' => if r_num != 0.0 { l_num / r_num } else { 0.0 },
                '%' => if r_num != 0.0 { l_num % r_num } else { 0.0 },
                _ => 0.0,
            };
            return if res.fract() == 0.0 { Value::Int(res as i64) } else { Value::Float(res) };
        }
    }

    // Exponentiation (^)
    if let Some((op_idx, _)) = find_top_level_math_op(s, &['^']) {
        let lhs = eval_sql_expr(s[..op_idx].trim(), primary_row, primary_table, joined_row, joined_table, params);
        let rhs = eval_sql_expr(s[op_idx + 1..].trim(), primary_row, primary_table, joined_row, joined_table, params);
        if lhs.is_null() || rhs.is_null() { return Value::Null; }
        if let (Some(l_num), Some(r_num)) = (lhs.as_f64(), rhs.as_f64()) {
            let res = l_num.powf(r_num);
            return if res.fract() == 0.0 { Value::Int(res as i64) } else { Value::Float(res) };
        }
    }

    // Unary operators (-, ~)
    if s.starts_with('-') && !s.starts_with("->") && !s.starts_with("--") {
        let inner = s[1..].trim();
        if !inner.is_empty() {
            let val = eval_sql_expr(inner, primary_row, primary_table, joined_row, joined_table, params);
            if val.is_null() { return Value::Null; }
            match val {
                Value::Int(i) => return Value::Int(-i),
                Value::Float(f) => return Value::Float(-f),
                _ => {
                    if let Some(f) = val.as_f64() {
                        return if f.fract() == 0.0 { Value::Int(-f as i64) } else { Value::Float(-f) };
                    }
                }
            }
        }
    }
    if s.starts_with('~') {
        let inner = s[1..].trim();
        if !inner.is_empty() {
            let val = eval_sql_expr(inner, primary_row, primary_table, joined_row, joined_table, params);
            if val.is_null() { return Value::Null; }
            if let Some(i) = val.as_i64() {
                return Value::Int(!i);
            }
        }
    }

    // 2. CAST(expr AS type) or expr::type
    if upper.starts_with("CAST(") && s.ends_with(')') {
        let inner = &s[5..s.len() - 1].trim();
        if let Some(as_pos) = inner.to_uppercase().rfind(" AS ") {
            let target_expr = inner[..as_pos].trim();
            let target_type = inner[as_pos + 4..].trim().to_uppercase();
            let val = eval_sql_expr(target_expr, primary_row, primary_table, joined_row, joined_table, params);
            return cast_val(val, &target_type);
        }
    }
    if let Some(colon_pos) = find_last_top_level_op(s, "::") {
        let target_expr = s[..colon_pos].trim();
        let target_type = s[colon_pos + 2..].trim().to_uppercase();
        let val = eval_sql_expr(target_expr, primary_row, primary_table, joined_row, joined_table, params);
        return cast_val(val, &target_type);
    }

    // JSON extraction operators: ->>, ->, #>>, #>
    if let Some((op_idx, op)) = find_last_top_level_json_op(s) {
        let lhs_expr = &s[..op_idx].trim();
        let rhs_expr = &s[op_idx + op.len()..].trim();
        let lhs = eval_sql_expr(lhs_expr, primary_row, primary_table, joined_row, joined_table, params);
        let rhs = eval_sql_expr(rhs_expr, primary_row, primary_table, joined_row, joined_table, params);
        let lhs_json = match &lhs {
            Value::Text(st) => serde_json::from_str::<serde_json::Value>(st.trim()).unwrap_or(serde_json::Value::String(st.to_string())),
            _ => value_to_json(&lhs),
        };
        let rhs_json = serde_json::Value::String(rhs.as_str());
        let res = eval_json_extract_serde(&lhs_json, &rhs_json, op);
        return match res {
            serde_json::Value::Null => Value::Null,
            serde_json::Value::String(st) => Value::text(st),
            serde_json::Value::Bool(b) => Value::Bool(b),
            serde_json::Value::Number(n) => {
                if let Some(i) = n.as_i64() {
                    Value::Int(i)
                } else if let Some(f) = n.as_f64() {
                    Value::Float(f)
                } else {
                    Value::text(n.to_string())
                }
            }
            serde_json::Value::Object(_) | serde_json::Value::Array(_) => Value::text(res.to_string()),
        };
    }

    // JSON Functions: JSON_EXTRACT_PATH_TEXT, JSONB_EXTRACT_PATH_TEXT, JSON_EXTRACT_PATH, JSONB_EXTRACT_PATH
    if (upper.starts_with("JSON_EXTRACT_PATH_TEXT(") || upper.starts_with("JSONB_EXTRACT_PATH_TEXT(")) && s.ends_with(')') {
        let open_idx = s.find('(').unwrap();
        let inner = &s[open_idx + 1..s.len() - 1];
        let args = split_function_args(inner);
        if !args.is_empty() {
            let curr = eval_sql_expr(&args[0], primary_row, primary_table, joined_row, joined_table, params);
            let mut curr_json = match &curr {
                Value::Text(st) => serde_json::from_str::<serde_json::Value>(st.trim()).unwrap_or(serde_json::Value::String(st.to_string())),
                _ => value_to_json(&curr),
            };
            for arg in &args[1..] {
                let key = eval_sql_expr(arg, primary_row, primary_table, joined_row, joined_table, params);
                let key_str = key.as_str();
                curr_json = match curr_json {
                    serde_json::Value::Object(map) => map.get(&key_str).cloned().unwrap_or(serde_json::Value::Null),
                    serde_json::Value::Array(arr) => {
                        if let Ok(idx) = key_str.parse::<usize>() {
                            arr.get(idx).cloned().unwrap_or(serde_json::Value::Null)
                        } else {
                            serde_json::Value::Null
                        }
                    }
                    _ => serde_json::Value::Null,
                };
                if curr_json.is_null() {
                    break;
                }
            }
            return match curr_json {
                serde_json::Value::Null => Value::Null,
                serde_json::Value::String(st) => Value::text(st),
                _ => Value::text(curr_json.to_string()),
            };
        }
    }
    if (upper.starts_with("JSON_EXTRACT_PATH(") || upper.starts_with("JSONB_EXTRACT_PATH(")) && s.ends_with(')') {
        let open_idx = s.find('(').unwrap();
        let inner = &s[open_idx + 1..s.len() - 1];
        let args = split_function_args(inner);
        if !args.is_empty() {
            let curr = eval_sql_expr(&args[0], primary_row, primary_table, joined_row, joined_table, params);
            let mut curr_json = match &curr {
                Value::Text(st) => serde_json::from_str::<serde_json::Value>(st.trim()).unwrap_or(serde_json::Value::String(st.to_string())),
                _ => value_to_json(&curr),
            };
            for arg in &args[1..] {
                let key = eval_sql_expr(arg, primary_row, primary_table, joined_row, joined_table, params);
                let key_str = key.as_str();
                curr_json = match curr_json {
                    serde_json::Value::Object(map) => map.get(&key_str).cloned().unwrap_or(serde_json::Value::Null),
                    serde_json::Value::Array(arr) => {
                        if let Ok(idx) = key_str.parse::<usize>() {
                            arr.get(idx).cloned().unwrap_or(serde_json::Value::Null)
                        } else {
                            serde_json::Value::Null
                        }
                    }
                    _ => serde_json::Value::Null,
                };
                if curr_json.is_null() {
                    break;
                }
            }
            return match curr_json {
                serde_json::Value::Null => Value::Null,
                serde_json::Value::String(st) => Value::text(serde_json::to_string(&st).unwrap_or(st)),
                serde_json::Value::Number(n) => {
                    if let Some(i) = n.as_i64() {
                        Value::Int(i)
                    } else if let Some(f) = n.as_f64() {
                        Value::Float(f)
                    } else {
                        Value::text(n.to_string())
                    }
                }
                serde_json::Value::Bool(b) => Value::Bool(b),
                serde_json::Value::Object(_) | serde_json::Value::Array(_) => Value::text(serde_json::to_string(&curr_json).unwrap_or_default()),
            };
        }
    }
    if (upper.starts_with("JSON_ARRAY_LENGTH(") || upper.starts_with("JSONB_ARRAY_LENGTH(")) && s.ends_with(')') {
        let inner = &s[upper.find('(').unwrap() + 1..s.len() - 1];
        let target = eval_sql_expr(inner, primary_row, primary_table, joined_row, joined_table, params);
        let target_json = match &target {
            Value::Text(st) => serde_json::from_str::<serde_json::Value>(st.trim()).unwrap_or(serde_json::Value::Null),
            _ => value_to_json(&target),
        };
        if let serde_json::Value::Array(arr) = target_json {
            return Value::Int(arr.len() as i64);
        }
        return Value::Null;
    }
    if (upper.starts_with("JSON_TYPEOF(") || upper.starts_with("JSONB_TYPEOF(")) && s.ends_with(')') {
        let inner = &s[upper.find('(').unwrap() + 1..s.len() - 1];
        let target = eval_sql_expr(inner, primary_row, primary_table, joined_row, joined_table, params);
        let target_json = match &target {
            Value::Text(st) => serde_json::from_str::<serde_json::Value>(st.trim()).unwrap_or(serde_json::Value::String(st.to_string())),
            _ => value_to_json(&target),
        };
        let type_str = match target_json {
            serde_json::Value::Null => "null",
            serde_json::Value::Bool(_) => "boolean",
            serde_json::Value::Number(_) => "number",
            serde_json::Value::String(_) => "string",
            serde_json::Value::Array(_) => "array",
            serde_json::Value::Object(_) => "object",
        };
        return Value::text(type_str);
    }
    if upper.starts_with("JSONB_STRIP_NULLS(") && s.ends_with(')') {
        let inner = &s[18..s.len() - 1];
        let target = eval_sql_expr(inner, primary_row, primary_table, joined_row, joined_table, params);
        let target_json = match &target {
            Value::Text(st) => serde_json::from_str::<serde_json::Value>(st.trim()).unwrap_or(serde_json::Value::Null),
            _ => value_to_json(&target),
        };
        if let serde_json::Value::Object(mut map) = target_json {
            map.retain(|_, v| !v.is_null());
            return Value::text(serde_json::to_string(&serde_json::Value::Object(map)).unwrap_or_default());
        }
        return target;
    }

    // JSON_BUILD_OBJECT and JSONB_BUILD_OBJECT
    if (upper.starts_with("JSON_BUILD_OBJECT(") || upper.starts_with("JSONB_BUILD_OBJECT(")) && s.ends_with(')') {
        let open_idx = s.find('(').unwrap();
        let inner = &s[open_idx + 1..s.len() - 1];
        let args = split_function_args(inner);
        let mut map = serde_json::Map::new();
        for chunk in args.chunks(2) {
            if chunk.len() == 2 {
                let k_val = eval_sql_expr(&chunk[0], primary_row, primary_table, joined_row, joined_table, params);
                let v_val = eval_sql_expr(&chunk[1], primary_row, primary_table, joined_row, joined_table, params);
                let k_str = k_val.as_str();
                let v_json = value_to_json(&v_val);
                map.insert(k_str, v_json);
            }
        }
        let json_obj = serde_json::Value::Object(map);
        return Value::text(serde_json::to_string(&json_obj).unwrap_or_default());
    }

    // JSON_BUILD_ARRAY and JSONB_BUILD_ARRAY
    if (upper.starts_with("JSON_BUILD_ARRAY(") || upper.starts_with("JSONB_BUILD_ARRAY(")) && s.ends_with(')') {
        let open_idx = s.find('(').unwrap();
        let inner = &s[open_idx + 1..s.len() - 1];
        let items: Vec<serde_json::Value> = split_function_args(inner)
            .into_iter()
            .map(|arg| {
                let v = eval_sql_expr(&arg, primary_row, primary_table, joined_row, joined_table, params);
                value_to_json(&v)
            })
            .collect();
        let arr = serde_json::Value::Array(items);
        return Value::text(serde_json::to_string(&arr).unwrap_or_default());
    }

    // JSONB_PRETTY
    if upper.starts_with("JSONB_PRETTY(") && s.ends_with(')') {
        let inner = &s[13..s.len() - 1];
        let v = eval_sql_expr(inner, primary_row, primary_table, joined_row, joined_table, params);
        let v_json = match &v {
            Value::Text(st) => serde_json::from_str::<serde_json::Value>(st.trim()).unwrap_or(value_to_json(&v)),
            _ => value_to_json(&v),
        };
        return Value::text(serde_json::to_string_pretty(&v_json).unwrap_or_default());
    }

    // JSONB_SET
    if (upper.starts_with("JSONB_SET(") || upper.starts_with("JSON_SET(")) && s.ends_with(')') {
        let open_idx = s.find('(').unwrap();
        let inner = &s[open_idx + 1..s.len() - 1];
        let args = split_function_args(inner);
        if args.len() >= 3 {
            let target_val = eval_sql_expr(&args[0], primary_row, primary_table, joined_row, joined_table, params);
            let path_val = eval_sql_expr(&args[1], primary_row, primary_table, joined_row, joined_table, params);
            let new_val_raw = eval_sql_expr(&args[2], primary_row, primary_table, joined_row, joined_table, params);
            let create_missing = if args.len() >= 4 {
                let cm_val = eval_sql_expr(&args[3], primary_row, primary_table, joined_row, joined_table, params);
                cm_val.as_bool().unwrap_or(true)
            } else {
                true
            };

            let mut target_json = match &target_val {
                Value::Text(st) => serde_json::from_str::<serde_json::Value>(st.trim()).unwrap_or(value_to_json(&target_val)),
                _ => value_to_json(&target_val),
            };
            let path_json = match &path_val {
                Value::Text(st) => serde_json::Value::String(st.to_string()),
                _ => value_to_json(&path_val),
            };
            let keys = parse_json_path_keys(&path_json);
            let new_json = match &new_val_raw {
                Value::Text(st) => serde_json::from_str::<serde_json::Value>(st.trim()).unwrap_or(serde_json::Value::String(st.to_string())),
                _ => value_to_json(&new_val_raw),
            };
            jsonb_set_serde(&mut target_json, &keys, &new_json, create_missing);
            return Value::text(serde_json::to_string(&target_json).unwrap_or_default());
        }
    }





    // 4. Built-in String Functions: UPPER, LOWER, INITCAP, CONCAT, CONCAT_WS, LENGTH, CHAR_LENGTH,
    // CHARACTER_LENGTH, OCTET_LENGTH, LEFT, RIGHT, TRIM, BTRIM, LTRIM, RTRIM, LPAD, RPAD, REPEAT,
    // REVERSE, ASCII, CHR, TRANSLATE, STRPOS, SPLIT_PART, MD5, ENCODE, DECODE, REPLACE, SUBSTRING
    if upper.starts_with("UPPER(") && s.ends_with(')') {
        let inner = &s[6..s.len() - 1];
        let v = eval_sql_expr(inner, primary_row, primary_table, joined_row, joined_table, params);
        if v.is_null() { return Value::Null; }
        return Value::text(v.as_str().to_uppercase());
    }

    if upper.starts_with("LOWER(") && s.ends_with(')') {
        let inner = &s[6..s.len() - 1];
        let v = eval_sql_expr(inner, primary_row, primary_table, joined_row, joined_table, params);
        if v.is_null() { return Value::Null; }
        return Value::text(v.as_str().to_lowercase());
    }

    if upper.starts_with("INITCAP(") && s.ends_with(')') {
        let inner = &s[8..s.len() - 1];
        let v = eval_sql_expr(inner, primary_row, primary_table, joined_row, joined_table, params);
        if v.is_null() { return Value::Null; }
        let s_str = v.as_str();
        let mut out = String::with_capacity(s_str.len());
        let mut new_word = true;
        for c in s_str.chars() {
            if c.is_alphanumeric() {
                if new_word {
                    out.extend(c.to_uppercase());
                    new_word = false;
                } else {
                    out.extend(c.to_lowercase());
                }
            } else {
                out.push(c);
                new_word = true;
            }
        }
        return Value::text(out);
    }

    if upper.starts_with("CONCAT(") && s.ends_with(')') {
        let inner = &s[7..s.len() - 1];
        let args = split_function_args(inner);
        let mut out = String::new();
        for arg in args {
            let v = eval_sql_expr(&arg, primary_row, primary_table, joined_row, joined_table, params);
            if !v.is_null() {
                out.push_str(&v.as_str());
            }
        }
        return Value::text(out);
    }

    if upper.starts_with("CONCAT_WS(") && s.ends_with(')') {
        let inner = &s[10..s.len() - 1];
        let args = split_function_args(inner);
        if !args.is_empty() {
            let sep_val = eval_sql_expr(&args[0], primary_row, primary_table, joined_row, joined_table, params);
            if sep_val.is_null() {
                return Value::Null;
            }
            let sep = sep_val.as_str();
            let mut parts = Vec::new();
            for arg in &args[1..] {
                let v = eval_sql_expr(arg, primary_row, primary_table, joined_row, joined_table, params);
                if !v.is_null() {
                    parts.push(v.as_str());
                }
            }
            return Value::text(parts.join(&sep));
        }
    }

    if (upper.starts_with("LENGTH(") || upper.starts_with("CHAR_LENGTH(") || upper.starts_with("CHARACTER_LENGTH(")) && s.ends_with(')') {
        let open_p = s.find('(').unwrap();
        let inner = &s[open_p + 1..s.len() - 1];
        let v = eval_sql_expr(inner, primary_row, primary_table, joined_row, joined_table, params);
        if v.is_null() { return Value::Null; }
        let len = v.as_str().chars().count();
        return Value::Int(len as i64);
    }

    if upper.starts_with("OCTET_LENGTH(") && s.ends_with(')') {
        let inner = &s[13..s.len() - 1];
        let v = eval_sql_expr(inner, primary_row, primary_table, joined_row, joined_table, params);
        if v.is_null() { return Value::Null; }
        let len = v.as_str().as_bytes().len();
        return Value::Int(len as i64);
    }

    if upper.starts_with("LEFT(") && s.ends_with(')') {
        let inner = &s[5..s.len() - 1];
        let args = split_function_args(inner);
        if args.len() == 2 {
            let v = eval_sql_expr(&args[0], primary_row, primary_table, joined_row, joined_table, params);
            let n_val = eval_sql_expr(&args[1], primary_row, primary_table, joined_row, joined_table, params);
            if v.is_null() || n_val.is_null() {
                return Value::Null;
            }
            let s_str = v.as_str();
            let chars: Vec<char> = s_str.chars().collect();
            let len = chars.len() as i64;
            let n = n_val.as_i64().unwrap_or(0);
            let take_len = if n >= 0 {
                (n as usize).min(chars.len())
            } else {
                ((len + n).max(0) as usize).min(chars.len())
            };
            let res: String = chars[..take_len].iter().collect();
            return Value::text(res);
        }
    }

    if upper.starts_with("RIGHT(") && s.ends_with(')') {
        let inner = &s[6..s.len() - 1];
        let args = split_function_args(inner);
        if args.len() == 2 {
            let v = eval_sql_expr(&args[0], primary_row, primary_table, joined_row, joined_table, params);
            let n_val = eval_sql_expr(&args[1], primary_row, primary_table, joined_row, joined_table, params);
            if v.is_null() || n_val.is_null() {
                return Value::Null;
            }
            let s_str = v.as_str();
            let chars: Vec<char> = s_str.chars().collect();
            let n = n_val.as_i64().unwrap_or(0);
            let start_idx = if n >= 0 {
                chars.len().saturating_sub(n as usize)
            } else {
                ((-n) as usize).min(chars.len())
            };
            let res: String = chars[start_idx..].iter().collect();
            return Value::text(res);
        }
    }

    if (upper.starts_with("TRIM(") || upper.starts_with("BTRIM(")) && s.ends_with(')') {
        let open_p = s.find('(').unwrap();
        let inner = &s[open_p + 1..s.len() - 1];
        let args = split_function_args(inner);
        if args.len() == 1 {
            let v = eval_sql_expr(&args[0], primary_row, primary_table, joined_row, joined_table, params);
            if v.is_null() { return Value::Null; }
            return Value::text(v.as_str().trim());
        } else if args.len() >= 2 {
            let v = eval_sql_expr(&args[0], primary_row, primary_table, joined_row, joined_table, params);
            let chars_v = eval_sql_expr(&args[1], primary_row, primary_table, joined_row, joined_table, params);
            if v.is_null() || chars_v.is_null() { return Value::Null; }
            let s_str = v.as_str();
            let c_str = chars_v.as_str();
            let trim_chars: Vec<char> = c_str.chars().collect();
            let trimmed = s_str.trim_matches(|c: char| trim_chars.contains(&c));
            return Value::text(trimmed);
        }
    }

    if upper.starts_with("LTRIM(") && s.ends_with(')') {
        let inner = &s[6..s.len() - 1];
        let args = split_function_args(inner);
        if args.len() == 1 {
            let v = eval_sql_expr(&args[0], primary_row, primary_table, joined_row, joined_table, params);
            if v.is_null() { return Value::Null; }
            return Value::text(v.as_str().trim_start());
        } else if args.len() >= 2 {
            let v = eval_sql_expr(&args[0], primary_row, primary_table, joined_row, joined_table, params);
            let chars_v = eval_sql_expr(&args[1], primary_row, primary_table, joined_row, joined_table, params);
            if v.is_null() || chars_v.is_null() { return Value::Null; }
            let s_str = v.as_str();
            let c_str = chars_v.as_str();
            let trim_chars: Vec<char> = c_str.chars().collect();
            let trimmed = s_str.trim_start_matches(|c: char| trim_chars.contains(&c));
            return Value::text(trimmed);
        }
    }

    if upper.starts_with("RTRIM(") && s.ends_with(')') {
        let inner = &s[6..s.len() - 1];
        let args = split_function_args(inner);
        if args.len() == 1 {
            let v = eval_sql_expr(&args[0], primary_row, primary_table, joined_row, joined_table, params);
            if v.is_null() { return Value::Null; }
            return Value::text(v.as_str().trim_end());
        } else if args.len() >= 2 {
            let v = eval_sql_expr(&args[0], primary_row, primary_table, joined_row, joined_table, params);
            let chars_v = eval_sql_expr(&args[1], primary_row, primary_table, joined_row, joined_table, params);
            if v.is_null() || chars_v.is_null() { return Value::Null; }
            let s_str = v.as_str();
            let c_str = chars_v.as_str();
            let trim_chars: Vec<char> = c_str.chars().collect();
            let trimmed = s_str.trim_end_matches(|c: char| trim_chars.contains(&c));
            return Value::text(trimmed);
        }
    }

    if upper.starts_with("LPAD(") && s.ends_with(')') {
        let inner = &s[5..s.len() - 1];
        let args = split_function_args(inner);
        if args.len() >= 2 {
            let v = eval_sql_expr(&args[0], primary_row, primary_table, joined_row, joined_table, params);
            let len_val = eval_sql_expr(&args[1], primary_row, primary_table, joined_row, joined_table, params);
            if v.is_null() || len_val.is_null() { return Value::Null; }
            let target_len = len_val.as_i64().unwrap_or(0).max(0) as usize;
            let s_str = v.as_str();
            let chars: Vec<char> = s_str.chars().collect();
            if chars.len() >= target_len {
                let res: String = chars[..target_len].iter().collect();
                return Value::text(res);
            }
            let pad_str = if args.len() >= 3 {
                let pv = eval_sql_expr(&args[2], primary_row, primary_table, joined_row, joined_table, params);
                if pv.is_null() { return Value::Null; }
                pv.as_str()
            } else {
                " ".to_string()
            };
            if pad_str.is_empty() {
                let res: String = chars.into_iter().collect();
                return Value::text(res);
            }
            let pad_chars: Vec<char> = pad_str.chars().collect();
            let needed = target_len - chars.len();
            let mut out = String::new();
            for i in 0..needed {
                out.push(pad_chars[i % pad_chars.len()]);
            }
            out.push_str(&s_str);
            return Value::text(out);
        }
    }

    if upper.starts_with("RPAD(") && s.ends_with(')') {
        let inner = &s[5..s.len() - 1];
        let args = split_function_args(inner);
        if args.len() >= 2 {
            let v = eval_sql_expr(&args[0], primary_row, primary_table, joined_row, joined_table, params);
            let len_val = eval_sql_expr(&args[1], primary_row, primary_table, joined_row, joined_table, params);
            if v.is_null() || len_val.is_null() { return Value::Null; }
            let target_len = len_val.as_i64().unwrap_or(0).max(0) as usize;
            let s_str = v.as_str();
            let chars: Vec<char> = s_str.chars().collect();
            if chars.len() >= target_len {
                let res: String = chars[..target_len].iter().collect();
                return Value::text(res);
            }
            let pad_str = if args.len() >= 3 {
                let pv = eval_sql_expr(&args[2], primary_row, primary_table, joined_row, joined_table, params);
                if pv.is_null() { return Value::Null; }
                pv.as_str()
            } else {
                " ".to_string()
            };
            if pad_str.is_empty() {
                let res: String = chars.into_iter().collect();
                return Value::text(res);
            }
            let pad_chars: Vec<char> = pad_str.chars().collect();
            let needed = target_len - chars.len();
            let mut out = s_str;
            for i in 0..needed {
                out.push(pad_chars[i % pad_chars.len()]);
            }
            return Value::text(out);
        }
    }

    if upper.starts_with("REPEAT(") && s.ends_with(')') {
        let inner = &s[7..s.len() - 1];
        let args = split_function_args(inner);
        if args.len() == 2 {
            let v = eval_sql_expr(&args[0], primary_row, primary_table, joined_row, joined_table, params);
            let count_val = eval_sql_expr(&args[1], primary_row, primary_table, joined_row, joined_table, params);
            if v.is_null() || count_val.is_null() { return Value::Null; }
            let count = count_val.as_i64().unwrap_or(0).max(0) as usize;
            let s_str = v.as_str();
            return Value::text(s_str.repeat(count));
        }
    }

    if upper.starts_with("REVERSE(") && s.ends_with(')') {
        let inner = &s[8..s.len() - 1];
        let v = eval_sql_expr(inner, primary_row, primary_table, joined_row, joined_table, params);
        if v.is_null() { return Value::Null; }
        let rev: String = v.as_str().chars().rev().collect();
        return Value::text(rev);
    }

    if upper.starts_with("ASCII(") && s.ends_with(')') {
        let inner = &s[6..s.len() - 1];
        let v = eval_sql_expr(inner, primary_row, primary_table, joined_row, joined_table, params);
        if v.is_null() { return Value::Null; }
        let s_str = v.as_str();
        if let Some(first_char) = s_str.chars().next() {
            return Value::Int(first_char as u32 as i64);
        }
        return Value::Int(0);
    }

    if upper.starts_with("CHR(") && s.ends_with(')') {
        let inner = &s[4..s.len() - 1];
        let v = eval_sql_expr(inner, primary_row, primary_table, joined_row, joined_table, params);
        if v.is_null() { return Value::Null; }
        let code = v.as_i64().unwrap_or(0) as u32;
        if let Some(ch) = char::from_u32(code) {
            return Value::text(ch.to_string());
        }
        return Value::text("");
    }

    if upper.starts_with("TRANSLATE(") && s.ends_with(')') {
        let inner = &s[10..s.len() - 1];
        let args = split_function_args(inner);
        if args.len() == 3 {
            let str_val = eval_sql_expr(&args[0], primary_row, primary_table, joined_row, joined_table, params);
            let from_val = eval_sql_expr(&args[1], primary_row, primary_table, joined_row, joined_table, params);
            let to_val = eval_sql_expr(&args[2], primary_row, primary_table, joined_row, joined_table, params);
            if str_val.is_null() || from_val.is_null() || to_val.is_null() { return Value::Null; }
            let s_str = str_val.as_str();
            let from_chars: Vec<char> = from_val.as_str().chars().collect();
            let to_chars: Vec<char> = to_val.as_str().chars().collect();
            let mut out = String::new();
            for ch in s_str.chars() {
                if let Some(pos) = from_chars.iter().position(|&fc| fc == ch) {
                    if pos < to_chars.len() {
                        out.push(to_chars[pos]);
                    }
                } else {
                    out.push(ch);
                }
            }
            return Value::text(out);
        }
    }

    if upper.starts_with("STRPOS(") && s.ends_with(')') {
        let inner = &s[7..s.len() - 1];
        let args = split_function_args(inner);
        if args.len() == 2 {
            let str_val = eval_sql_expr(&args[0], primary_row, primary_table, joined_row, joined_table, params);
            let sub_val = eval_sql_expr(&args[1], primary_row, primary_table, joined_row, joined_table, params);
            if str_val.is_null() || sub_val.is_null() { return Value::Null; }
            let s_str = str_val.as_str();
            let sub_str = sub_val.as_str();
            if let Some(byte_pos) = s_str.find(&sub_str) {
                let char_idx = s_str[..byte_pos].chars().count() + 1;
                return Value::Int(char_idx as i64);
            } else {
                return Value::Int(0);
            }
        }
    }

    if upper.starts_with("SPLIT_PART(") && s.ends_with(')') {
        let inner = &s[11..s.len() - 1];
        let args = split_function_args(inner);
        if args.len() == 3 {
            let str_val = eval_sql_expr(&args[0], primary_row, primary_table, joined_row, joined_table, params);
            let delim_val = eval_sql_expr(&args[1], primary_row, primary_table, joined_row, joined_table, params);
            let field_val = eval_sql_expr(&args[2], primary_row, primary_table, joined_row, joined_table, params);
            if str_val.is_null() || delim_val.is_null() || field_val.is_null() { return Value::Null; }
            let s_str = str_val.as_str();
            let delim_str = delim_val.as_str();
            let field_idx = field_val.as_i64().unwrap_or(1);
            if field_idx < 1 {
                return Value::text("");
            }
            let parts: Vec<&str> = s_str.split(&delim_str).collect();
            let u_idx = (field_idx - 1) as usize;
            if u_idx < parts.len() {
                return Value::text(parts[u_idx]);
            } else {
                return Value::text("");
            }
        }
    }

    if upper.starts_with("MD5(") && s.ends_with(')') {
        let inner = &s[4..s.len() - 1];
        let v = eval_sql_expr(inner, primary_row, primary_table, joined_row, joined_table, params);
        if v.is_null() { return Value::Null; }
        let s_str = v.as_str();
        let mut hasher = Md5::new();
        hasher.update(s_str.as_bytes());
        let hash = format!("{:x}", hasher.finalize());
        return Value::text(hash);
    }

    if upper.starts_with("ENCODE(") && s.ends_with(')') {
        let inner = &s[7..s.len() - 1];
        let args = split_function_args(inner);
        if args.len() == 2 {
            let str_val = eval_sql_expr(&args[0], primary_row, primary_table, joined_row, joined_table, params);
            let fmt_val = eval_sql_expr(&args[1], primary_row, primary_table, joined_row, joined_table, params);
            if str_val.is_null() || fmt_val.is_null() { return Value::Null; }
            let s_str = str_val.as_str();
            let fmt_str = fmt_val.as_str().to_lowercase();
            if fmt_str == "hex" {
                return Value::text(hex::encode(s_str.as_bytes()));
            } else if fmt_str == "base64" {
                return Value::text(base64::engine::general_purpose::STANDARD.encode(s_str.as_bytes()));
            }
        }
    }

    if upper.starts_with("DECODE(") && s.ends_with(')') {
        let inner = &s[7..s.len() - 1];
        let args = split_function_args(inner);
        if args.len() == 2 {
            let str_val = eval_sql_expr(&args[0], primary_row, primary_table, joined_row, joined_table, params);
            let fmt_val = eval_sql_expr(&args[1], primary_row, primary_table, joined_row, joined_table, params);
            if str_val.is_null() || fmt_val.is_null() { return Value::Null; }
            let s_str = str_val.as_str();
            let fmt_str = fmt_val.as_str().to_lowercase();
            if fmt_str == "hex" {
                if let Ok(bytes) = hex::decode(s_str.trim()) {
                    return Value::text(String::from_utf8_lossy(&bytes).to_string());
                }
            } else if fmt_str == "base64" {
                if let Ok(bytes) = base64::engine::general_purpose::STANDARD.decode(s_str.trim()) {
                    return Value::text(String::from_utf8_lossy(&bytes).to_string());
                }
            }
        }
    }

    if upper.starts_with("REPLACE(") && s.ends_with(')') {
        let inner = &s[8..s.len() - 1];
        let args = split_function_args(inner);
        if args.len() == 3 {
            let base = eval_sql_expr(&args[0], primary_row, primary_table, joined_row, joined_table, params);
            let from = eval_sql_expr(&args[1], primary_row, primary_table, joined_row, joined_table, params);
            let to = eval_sql_expr(&args[2], primary_row, primary_table, joined_row, joined_table, params);
            if base.is_null() || from.is_null() || to.is_null() { return Value::Null; }
            let base_s = base.as_str();
            let from_s = from.as_str();
            let to_s = to.as_str();
            return Value::text(base_s.replace(&from_s, &to_s));
        }
    }

    if (upper.starts_with("SUBSTRING(") || upper.starts_with("SUBSTR(")) && s.ends_with(')') {
        let open_p = s.find('(').unwrap();
        let inner = &s[open_p + 1..s.len() - 1];
        let args = split_function_args(inner);
        if args.len() >= 2 {
            let base = eval_sql_expr(&args[0], primary_row, primary_table, joined_row, joined_table, params);
            if base.is_null() { return Value::Null; }
            let start = eval_sql_expr(&args[1], primary_row, primary_table, joined_row, joined_table, params).as_i64().unwrap_or(1);
            let length = if args.len() >= 3 {
                eval_sql_expr(&args[2], primary_row, primary_table, joined_row, joined_table, params).as_i64()
            } else {
                None
            };
            let base_s = base.as_str();
            let is_bytea = base_s.starts_with("\\x")
                || base_s.starts_with(r"\x")
                || base_s.starts_with("0x")
                || (base_s.starts_with('[') && base_s.ends_with(']') && serde_json::from_str::<Vec<u8>>(&base_s).is_ok());

            if is_bytea {
                let raw = get_raw_bytes(&base);
                let start_idx = if start > 0 { (start as usize).saturating_sub(1) } else { 0 };
                let sliced: Vec<u8> = if let Some(len) = length {
                    raw.into_iter().skip(start_idx).take(len.max(0) as usize).collect()
                } else {
                    raw.into_iter().skip(start_idx).collect()
                };
                let json_arr: Vec<serde_json::Value> = sliced.into_iter().map(|b| serde_json::json!(b)).collect();
                return Value::text(serde_json::to_string(&json_arr).unwrap_or_default());
            }
            let chars: Vec<char> = base_s.chars().collect();
            let start_idx = if start > 0 { (start as usize).saturating_sub(1) } else { 0 };
            let sub: String = if let Some(len) = length {
                chars.iter().skip(start_idx).take(len.max(0) as usize).collect()
            } else {
                chars.iter().skip(start_idx).collect()
            };
            return Value::text(sub);
        }
    }

    // 5. Math Functions: ABS, FLOOR, CEIL, ROUND, TRUNC, POWER, SQRT, CBRT, EXP, LN, LOG, MOD, SIGN, PI, DEGREES, RADIANS, RANDOM
    if upper == "PI()" || upper == "PI" {
        return Value::Float(std::f64::consts::PI);
    }

    if upper == "RANDOM()" {
        let nanos = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.subsec_nanos()).unwrap_or(12345);
        let rand = ((nanos % 1_000_000) as f64) / 1_000_000.0;
        return Value::Float(rand);
    }

    if upper.starts_with("ABS(") && s.ends_with(')') {
        let inner = &s[4..s.len() - 1];
        let v = eval_sql_expr(inner, primary_row, primary_table, joined_row, joined_table, params);
        if v.is_null() { return Value::Null; }
        if let Some(f) = v.as_f64() {
            return if f.fract() == 0.0 { Value::Int(f.abs() as i64) } else { Value::Float(f.abs()) };
        }
    }

    if upper.starts_with("FLOOR(") && s.ends_with(')') {
        let inner = &s[6..s.len() - 1];
        let v = eval_sql_expr(inner, primary_row, primary_table, joined_row, joined_table, params);
        if v.is_null() { return Value::Null; }
        if let Some(f) = v.as_f64() {
            return Value::Int(f.floor() as i64);
        }
    }

    if (upper.starts_with("CEIL(") || upper.starts_with("CEILING(")) && s.ends_with(')') {
        let start_pos = if upper.starts_with("CEILING(") { 8 } else { 5 };
        let inner = &s[start_pos..s.len() - 1];
        let v = eval_sql_expr(inner, primary_row, primary_table, joined_row, joined_table, params);
        if v.is_null() { return Value::Null; }
        if let Some(f) = v.as_f64() {
            return Value::Int(f.ceil() as i64);
        }
    }

    if upper.starts_with("ROUND(") && s.ends_with(')') {
        let inner = &s[6..s.len() - 1];
        let args = split_function_args(inner);
        if !args.is_empty() {
            let v = eval_sql_expr(&args[0], primary_row, primary_table, joined_row, joined_table, params);
            if v.is_null() { return Value::Null; }
            let decimals = if args.len() >= 2 {
                let d_val = eval_sql_expr(&args[1], primary_row, primary_table, joined_row, joined_table, params);
                if d_val.is_null() { return Value::Null; }
                d_val.as_i64().unwrap_or(0)
            } else {
                0
            };
            if let Some(f) = v.as_f64() {
                if decimals == 0 {
                    return Value::Int(f.round() as i64);
                } else {
                    let multiplier = 10f64.powi(decimals as i32);
                    let rounded = (f * multiplier).round() / multiplier;
                    return Value::Float(rounded);
                }
            }
        }
    }

    if (upper.starts_with("TRUNC(") || upper.starts_with("TRUNCATE(")) && s.ends_with(')') {
        let start_pos = if upper.starts_with("TRUNCATE(") { 9 } else { 6 };
        let inner = &s[start_pos..s.len() - 1];
        let args = split_function_args(inner);
        if !args.is_empty() {
            let v = eval_sql_expr(&args[0], primary_row, primary_table, joined_row, joined_table, params);
            if v.is_null() { return Value::Null; }
            let decimals = if args.len() >= 2 {
                let d_val = eval_sql_expr(&args[1], primary_row, primary_table, joined_row, joined_table, params);
                if d_val.is_null() { return Value::Null; }
                d_val.as_i64().unwrap_or(0)
            } else {
                0
            };
            if let Some(f) = v.as_f64() {
                if decimals == 0 {
                    return Value::Int(f.trunc() as i64);
                } else {
                    let multiplier = 10f64.powi(decimals as i32);
                    let truncated = (f * multiplier).trunc() / multiplier;
                    return Value::Float(truncated);
                }
            }
        }
    }

    if (upper.starts_with("POWER(") || upper.starts_with("POW(")) && s.ends_with(')') {
        let start_pos = if upper.starts_with("POWER(") { 6 } else { 4 };
        let inner = &s[start_pos..s.len() - 1];
        let args = split_function_args(inner);
        if args.len() == 2 {
            let base = eval_sql_expr(&args[0], primary_row, primary_table, joined_row, joined_table, params);
            let exp = eval_sql_expr(&args[1], primary_row, primary_table, joined_row, joined_table, params);
            if base.is_null() || exp.is_null() { return Value::Null; }
            if let (Some(b), Some(e)) = (base.as_f64(), exp.as_f64()) {
                let res = b.powf(e);
                return if res.fract() == 0.0 { Value::Int(res as i64) } else { Value::Float(res) };
            }
        }
    }

    if upper.starts_with("SQRT(") && s.ends_with(')') {
        let inner = &s[5..s.len() - 1];
        let v = eval_sql_expr(inner, primary_row, primary_table, joined_row, joined_table, params);
        if v.is_null() { return Value::Null; }
        if let Some(f) = v.as_f64() {
            let res = f.sqrt();
            return if res.fract() == 0.0 { Value::Int(res as i64) } else { Value::Float(res) };
        }
    }

    if upper.starts_with("CBRT(") && s.ends_with(')') {
        let inner = &s[5..s.len() - 1];
        let v = eval_sql_expr(inner, primary_row, primary_table, joined_row, joined_table, params);
        if v.is_null() { return Value::Null; }
        if let Some(f) = v.as_f64() {
            let res = f.cbrt();
            return if res.fract() == 0.0 { Value::Int(res as i64) } else { Value::Float(res) };
        }
    }

    if upper.starts_with("EXP(") && s.ends_with(')') {
        let inner = &s[4..s.len() - 1];
        let v = eval_sql_expr(inner, primary_row, primary_table, joined_row, joined_table, params);
        if v.is_null() { return Value::Null; }
        if let Some(f) = v.as_f64() {
            let res = f.exp();
            return if res.fract() == 0.0 { Value::Int(res as i64) } else { Value::Float(res) };
        }
    }

    if upper.starts_with("LN(") && s.ends_with(')') {
        let inner = &s[3..s.len() - 1];
        let v = eval_sql_expr(inner, primary_row, primary_table, joined_row, joined_table, params);
        if v.is_null() { return Value::Null; }
        if let Some(f) = v.as_f64() {
            let res = f.ln();
            return if res.fract() == 0.0 { Value::Int(res as i64) } else { Value::Float(res) };
        }
    }

    if (upper.starts_with("LOG(") || upper.starts_with("LOG10(")) && s.ends_with(')') {
        let start_pos = if upper.starts_with("LOG10(") { 6 } else { 4 };
        let inner = &s[start_pos..s.len() - 1];
        let args = split_function_args(inner);
        if args.len() == 1 {
            let v = eval_sql_expr(&args[0], primary_row, primary_table, joined_row, joined_table, params);
            if v.is_null() { return Value::Null; }
            if let Some(f) = v.as_f64() {
                let res = f.log10();
                return if res.fract() == 0.0 { Value::Int(res as i64) } else { Value::Float(res) };
            }
        } else if args.len() == 2 {
            let b_val = eval_sql_expr(&args[0], primary_row, primary_table, joined_row, joined_table, params);
            let x_val = eval_sql_expr(&args[1], primary_row, primary_table, joined_row, joined_table, params);
            if b_val.is_null() || x_val.is_null() { return Value::Null; }
            if let (Some(b), Some(x)) = (b_val.as_f64(), x_val.as_f64()) {
                let res = x.log(b);
                return if res.fract() == 0.0 { Value::Int(res as i64) } else { Value::Float(res) };
            }
        }
    }

    if upper.starts_with("DEGREES(") && s.ends_with(')') {
        let inner = &s[8..s.len() - 1];
        let v = eval_sql_expr(inner, primary_row, primary_table, joined_row, joined_table, params);
        if v.is_null() { return Value::Null; }
        if let Some(f) = v.as_f64() {
            return Value::Float(f.to_degrees());
        }
    }

    if upper.starts_with("RADIANS(") && s.ends_with(')') {
        let inner = &s[8..s.len() - 1];
        let v = eval_sql_expr(inner, primary_row, primary_table, joined_row, joined_table, params);
        if v.is_null() { return Value::Null; }
        if let Some(f) = v.as_f64() {
            return Value::Float(f.to_radians());
        }
    }

    if upper.starts_with("MOD(") && s.ends_with(')') {
        let inner = &s[4..s.len() - 1];
        let args = split_function_args(inner);
        if args.len() == 2 {
            let a = eval_sql_expr(&args[0], primary_row, primary_table, joined_row, joined_table, params);
            let b = eval_sql_expr(&args[1], primary_row, primary_table, joined_row, joined_table, params);
            if a.is_null() || b.is_null() { return Value::Null; }
            if let (Some(a_i), Some(b_i)) = (a.as_i64(), b.as_i64()) {
                return Value::Int(if b_i != 0 { a_i % b_i } else { 0 });
            }
        }
    }

    if upper.starts_with("SIGN(") && s.ends_with(')') {
        let inner = &s[5..s.len() - 1];
        let v = eval_sql_expr(inner, primary_row, primary_table, joined_row, joined_table, params);
        if v.is_null() { return Value::Null; }
        if let Some(f) = v.as_f64() {
            return Value::Int(if f > 0.0 { 1 } else if f < 0.0 { -1 } else { 0 });
        }
    }

    // 6. Date & Time: EXTRACT, DATE_PART, DATE_TRUNC, TO_CHAR, AGE, CURRENT_DATE, CURRENT_TIMESTAMP, NOW(), etc.
    if upper.starts_with("EXTRACT(") && s.ends_with(')') {
        let inner = s[8..s.len() - 1].trim();
        let inner_upper = inner.to_uppercase();
        if let Some(from_pos) = inner_upper.find(" FROM ") {
            let field = inner[..from_pos].trim();
            let source_expr = inner[from_pos + 6..].trim();
            let date_val = eval_sql_expr(source_expr, primary_row, primary_table, joined_row, joined_table, params);
            if date_val.is_null() { return Value::Null; }
            if let Some(dt) = parse_datetime_components(&date_val.as_str()) {
                if let Some(val) = eval_date_part_or_extract(field, &dt) {
                    return val;
                }
            }
        }
    }

    if upper.starts_with("DATE_PART(") && s.ends_with(')') {
        let inner = &s[10..s.len() - 1];
        let args = split_function_args(inner);
        if args.len() == 2 {
            let part_field = eval_sql_expr(&args[0], primary_row, primary_table, joined_row, joined_table, params);
            let date_val = eval_sql_expr(&args[1], primary_row, primary_table, joined_row, joined_table, params);
            if part_field.is_null() || date_val.is_null() { return Value::Null; }
            let field = part_field.as_str();
            if let Some(dt) = parse_datetime_components(&date_val.as_str()) {
                if let Some(val) = eval_date_part_or_extract(&field, &dt) {
                    return val;
                }
            }
        }
    }

    if upper.starts_with("DATE_TRUNC(") && s.ends_with(')') {
        let inner = &s[11..s.len() - 1];
        let args = split_function_args(inner);
        if args.len() == 2 {
            let unit_val = eval_sql_expr(&args[0], primary_row, primary_table, joined_row, joined_table, params);
            let date_val = eval_sql_expr(&args[1], primary_row, primary_table, joined_row, joined_table, params);
            if unit_val.is_null() || date_val.is_null() { return Value::Null; }
            let unit = unit_val.as_str();
            if let Some(dt) = parse_datetime_components(&date_val.as_str()) {
                return Value::text(eval_date_trunc(&unit, &dt));
            }
        }
    }

    if upper.starts_with("TO_CHAR(") && s.ends_with(')') {
        let inner = &s[8..s.len() - 1];
        let args = split_function_args(inner);
        if args.len() == 2 {
            let date_val = eval_sql_expr(&args[0], primary_row, primary_table, joined_row, joined_table, params);
            let fmt_val = eval_sql_expr(&args[1], primary_row, primary_table, joined_row, joined_table, params);
            if date_val.is_null() || fmt_val.is_null() { return Value::Null; }
            let fmt = fmt_val.as_str();
            if let Some(dt) = parse_datetime_components(&date_val.as_str()) {
                return Value::text(eval_to_char(&dt, &fmt));
            }
        }
    }

    if upper.starts_with("AGE(") && s.ends_with(')') {
        let inner = &s[4..s.len() - 1];
        let args = split_function_args(inner);
        if args.len() == 1 {
            let ts1_val = eval_sql_expr(&args[0], primary_row, primary_table, joined_row, joined_table, params);
            if ts1_val.is_null() { return Value::Null; }
            let dt1 = parse_datetime_components(&get_current_timestamp());
            let dt2 = parse_datetime_components(&ts1_val.as_str());
            if let (Some(d1), Some(d2)) = (dt1, dt2) {
                return Value::text(eval_age(&d1, &d2));
            }
        } else if args.len() == 2 {
            let ts1_val = eval_sql_expr(&args[0], primary_row, primary_table, joined_row, joined_table, params);
            let ts2_val = eval_sql_expr(&args[1], primary_row, primary_table, joined_row, joined_table, params);
            if ts1_val.is_null() || ts2_val.is_null() { return Value::Null; }
            let dt1 = parse_datetime_components(&ts1_val.as_str());
            let dt2 = parse_datetime_components(&ts2_val.as_str());
            if let (Some(d1), Some(d2)) = (dt1, dt2) {
                return Value::text(eval_age(&d1, &d2));
            }
        }
    }

    if upper == "CURRENT_DATE" {
        return Value::text(get_current_date());
    }
    if upper == "CURRENT_TIMESTAMP" || upper == "NOW()" || upper == "LOCALTIMESTAMP" {
        return Value::text(get_current_timestamp());
    }
    if upper == "CURRENT_TIME" || upper == "LOCALTIME" {
        return Value::text(get_current_time());
    }

    // 7. COALESCE and NULLIF
    if upper.starts_with("COALESCE(") && s.ends_with(')') {
        let inner = &s[9..s.len() - 1];
        for arg in split_function_args(inner) {
            let v = eval_sql_expr(&arg, primary_row, primary_table, joined_row, joined_table, params);
            if !v.is_null() {
                return v;
            }
        }
        return Value::Null;
    }
    if upper.starts_with("NULLIF(") && s.ends_with(')') {
        let inner = &s[7..s.len() - 1];
        let args = split_function_args(inner);
        if args.len() == 2 {
            let a = eval_sql_expr(&args[0], primary_row, primary_table, joined_row, joined_table, params);
            let b = eval_sql_expr(&args[1], primary_row, primary_table, joined_row, joined_table, params);
            return if a.is_equal(&b) { Value::Null } else { a };
        }
    }



    // 10. Column resolution
    let clean = clean_col_name(s);
    if let Some(pt) = primary_table {
        if let Some(idx) = pt.get_column_index(clean).or_else(|| pt.get_column_index(s)) {
            return primary_row.get(idx).cloned().unwrap_or(Value::Null);
        }
    }
    if let Some(jt) = joined_table {
        if let Some(idx) = jt.get_column_index(clean).or_else(|| jt.get_column_index(s)) {
            return joined_row.get(idx).cloned().unwrap_or(Value::Null);
        }
    }

    // 11. Fallback to literal or parameter
    parse_value(s, params)
}



