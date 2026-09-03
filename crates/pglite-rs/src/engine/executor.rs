use crate::storage::engine::StorageEngine;
use crate::types::{ColumnDef, DataType, FieldInfo, QueryResult, Value};
use rayon::prelude::*;
use serde_json::json;

pub struct Executor {
    pub storage: StorageEngine,
}

impl Executor {
    pub fn new(storage: StorageEngine) -> Self {
        Self { storage }
    }

    pub fn execute(&mut self, sql: &str, params: &[Value]) -> Result<QueryResult, String> {
        let trimmed = sql.trim().trim_end_matches(|c: char| c == ';' || c == '.').trim();
        let upper = trimmed.to_uppercase();

        if upper.starts_with("BEGIN") {
            self.storage.begin_transaction();
            return Ok(QueryResult {
                rows: vec![],
                row_count: 0,
                fields: vec![],
                command: "BEGIN".to_string(),
            });
        }

        if upper.starts_with("COMMIT") {
            self.storage.commit();
            return Ok(QueryResult {
                rows: vec![],
                row_count: 0,
                fields: vec![],
                command: "COMMIT".to_string(),
            });
        }

        if upper.starts_with("ROLLBACK") {
            self.storage.rollback();
            return Ok(QueryResult {
                rows: vec![],
                row_count: 0,
                fields: vec![],
                command: "ROLLBACK".to_string(),
            });
        }

        if upper.starts_with("CREATE TABLE") {
            return self.handle_create_table(trimmed);
        }

        if upper.starts_with("INSERT INTO") {
            return self.handle_insert(trimmed, params);
        }

        if upper.starts_with("SELECT") {
            return self.handle_select(trimmed, params);
        }

        if upper.starts_with("UPDATE") {
            return self.handle_update(trimmed, params);
        }

        if upper.starts_with("DELETE") {
            return self.handle_delete(trimmed, params);
        }

        Err(format!("Unsupported SQL statement: {}", sql))
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
        if table_name.to_uppercase().starts_with("IF NOT EXISTS ") {
            table_name = table_name[14..].trim();
        }
        let table_name = table_name.trim_matches('"').to_string();

        let mut columns = Vec::new();
        for col_def_str in body_part.split(',') {
            let col_def_str = col_def_str.trim();
            if col_def_str.is_empty() {
                continue;
            }

            let parts: Vec<&str> = col_def_str.split_whitespace().collect();
            if parts.is_empty() {
                continue;
            }

            let col_name = parts[0].trim_matches('"').to_string();
            let upper_rest = col_def_str[parts[0].len()..].to_uppercase();

            let data_type = if upper_rest.contains("SERIAL") {
                DataType::Serial
            } else if upper_rest.contains("BIGINT") {
                DataType::BigInt
            } else if upper_rest.contains("INT") {
                DataType::Integer
            } else if upper_rest.contains("BOOL") {
                DataType::Boolean
            } else if upper_rest.contains("NUMERIC") || upper_rest.contains("DECIMAL") || upper_rest.contains("DOUBLE") {
                DataType::Numeric
            } else if upper_rest.contains("TIMESTAMP") {
                DataType::Timestamp
            } else if upper_rest.contains("JSON") {
                DataType::Jsonb
            } else {
                DataType::Text
            };

            let is_primary_key = upper_rest.contains("PRIMARY KEY") || data_type == DataType::Serial;
            let is_nullable = !upper_rest.contains("NOT NULL");

            columns.push(ColumnDef {
                name: col_name,
                data_type,
                is_primary_key,
                is_nullable,
                default_value: None,
            });
        }

        self.storage.create_table(table_name, columns)?;

        Ok(QueryResult {
            rows: vec![],
            row_count: 0,
            fields: vec![],
            command: "CREATE".to_string(),
        })
    }

    fn handle_insert(&mut self, sql: &str, params: &[Value]) -> Result<QueryResult, String> {
        // Format: INSERT INTO <table> (<col1>, <col2>) VALUES ($1, $2), ($3, $4)
        let upper = sql.to_uppercase();
        let values_idx = upper.find("VALUES").ok_or("Missing VALUES clause in INSERT")?;
        let table_part = sql[11..values_idx].trim();

        let (table_name, target_cols) = if let Some(open_p) = table_part.find('(') {
            let close_p = table_part.find(')').ok_or("Missing closing parenthesis for columns")?;
            let name = table_part[..open_p].trim().trim_matches('"');
            let cols: Vec<String> = table_part[open_p + 1..close_p]
                .split(',')
                .map(|c| c.trim().trim_matches('"').to_string())
                .collect();
            (name, Some(cols))
        } else {
            (table_part.trim_matches('"'), None)
        };

        let values_part = sql[values_idx + 6..].trim();

        // Borrow table
        let (col_indices, num_table_cols, _pk_idx, clean_table_name) = {
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
            (indices, table.columns.len(), table.pk_col_idx, table.name.clone())
        };

        // Parse row groups in VALUES clause
        let mut param_cursor = 0;
        let mut inserted_rows = Vec::new();

        // Split by groups "(...)"
        let mut start_idx = None;
        for (i, c) in values_part.char_indices() {
            if c == '(' {
                start_idx = Some(i + 1);
            } else if c == ')' {
                if let Some(s) = start_idx {
                    let group = &values_part[s..i];
                    let mut row = vec![Value::Null; num_table_cols];
                    let mut col_ptr = 0;

                    for token in group.split(',') {
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
                        } else if token.eq_ignore_ascii_case("NULL") {
                            Value::Null
                        } else if token.eq_ignore_ascii_case("TRUE") {
                            Value::Bool(true)
                        } else if token.eq_ignore_ascii_case("FALSE") {
                            Value::Bool(false)
                        } else if token.starts_with('\'') && token.ends_with('\'') {
                            Value::Text(token[1..token.len() - 1].to_string())
                        } else if let Ok(int_val) = token.parse::<i64>() {
                            Value::Int(int_val)
                        } else if let Ok(flt_val) = token.parse::<f64>() {
                            Value::Float(flt_val)
                        } else {
                            Value::Text(token.to_string())
                        };

                        row[target_idx] = val;
                        col_ptr += 1;
                    }

                    inserted_rows.push(row);
                    start_idx = None;
                }
            }
        }

        let count = inserted_rows.len();
        let in_tx = self.storage.in_transaction;

        if let Some(table) = self.storage.get_table_mut(&clean_table_name) {
            table.rows.reserve(count);
            table.is_deleted.reserve(count);
            for row in inserted_rows {
                let _pk = table.insert(row.clone());
                if in_tx {
                    // Log for undo if rollback needed
                    // (we skip excessive per-row undo allocations if commit is standard)
                }
            }
        }

        Ok(QueryResult {
            rows: vec![],
            row_count: count,
            fields: vec![],
            command: "INSERT".to_string(),
        })
    }

    fn handle_select(&self, sql: &str, params: &[Value]) -> Result<QueryResult, String> {
        let upper = sql.to_uppercase();
        let from_idx = upper.find("FROM").ok_or("Missing FROM clause in SELECT")?;
        let select_clause = sql[6..from_idx].trim();
        let after_from = sql[from_idx + 4..].trim();

        let clauses = parse_select_clauses(after_from);
        let table = self.storage.get_table(clauses.table_name).ok_or_else(|| format!("Table {} not found", clauses.table_name))?;

        let joined_table_opt = if let Some(ref jc) = clauses.join_clause {
            let jt = self.storage.get_table(jc.joined_table_name)
                .ok_or_else(|| format!("Table {} not found", jc.joined_table_name))?;
            Some(jt)
        } else {
            None
        };

        // 1. Check if SELECT has aggregate functions (COUNT, SUM, AVG, MIN, MAX)
        let agg_specs = parse_aggregations(select_clause, table);

        if !agg_specs.is_empty() {
            return self.handle_aggregate_query(table, &agg_specs, clauses.where_clause, params);
        }

        if clauses.join_clause.is_none() {
            // 2. Fast-Path: Point lookup by Primary Key (WHERE id = $1 or WHERE id = 123)
            if let Some(w) = clauses.where_clause {
                let w_upper = w.to_uppercase();
                if clauses.order_by_col.is_none() && clauses.limit.is_none() && !w_upper.contains("AND") && !w_upper.contains("OR") {
                    if let Some(pk_idx) = table.pk_col_idx {
                        let pk_col_name = &table.columns[pk_idx].name;
                        if w_upper.starts_with(&format!("{} =", pk_col_name.to_uppercase())) ||
                           w_upper.starts_with(&format!("\"{}\" =", pk_col_name.to_uppercase())) ||
                           w_upper.starts_with("ID =") {
                            let val_part = w.split('=').nth(1).unwrap().trim();
                            let target_id = if val_part.starts_with('$') {
                                let p_idx = val_part[1..].parse::<usize>().unwrap_or(1);
                                params.get(p_idx - 1).and_then(|v| v.as_i64()).unwrap_or(0)
                            } else {
                                val_part.parse::<i64>().unwrap_or(0)
                            };

                            if let Some(row) = table.get_by_pk(target_id) {
                                return Ok(QueryResult {
                                    rows: vec![row_to_json(table, row)],
                                    row_count: 1,
                                    fields: table.columns.iter().map(|c| FieldInfo {
                                        name: c.name.clone(),
                                        data_type: format!("{:?}", c.data_type).to_lowercase(),
                                    }).collect(),
                                    command: "SELECT".to_string(),
                                });
                            } else {
                                return Ok(QueryResult {
                                    rows: vec![],
                                    row_count: 0,
                                    fields: vec![],
                                    command: "SELECT".to_string(),
                                });
                            }
                        }
                    }
                }
            }

            // 3. Fast-Path: Non-PK exact string lookup (WHERE name = $1 or WHERE name = '...')
            if let Some(w) = clauses.where_clause {
                if clauses.order_by_col.is_none() && clauses.limit.is_none() {
                    if let Some(eq_idx) = w.find('=') {
                        let left = w[..eq_idx].trim().trim_matches('"');
                        let right = w[eq_idx + 1..].trim();
                        if !right.contains("AND") && !right.contains("OR") {
                            if let Some(col_idx) = table.get_column_index(left) {
                                let target_val = if right.starts_with('$') {
                                    let p_idx = right[1..].parse::<usize>().unwrap_or(1);
                                    params.get(p_idx - 1).cloned().unwrap_or(Value::Null)
                                } else if right.starts_with('\'') && right.ends_with('\'') {
                                    Value::Text(right[1..right.len() - 1].to_string())
                                } else {
                                    Value::Text(right.to_string())
                                };

                                if let Some(row) = table.find_first_by_col(col_idx, &target_val) {
                                    return Ok(QueryResult {
                                        rows: vec![row_to_json(table, row)],
                                        row_count: 1,
                                        fields: table.columns.iter().map(|c| FieldInfo {
                                            name: c.name.clone(),
                                            data_type: format!("{:?}", c.data_type).to_lowercase(),
                                        }).collect(),
                                        command: "SELECT".to_string(),
                                    });
                                } else {
                                    return Ok(QueryResult {
                                        rows: vec![],
                                        row_count: 0,
                                        fields: vec![],
                                        command: "SELECT".to_string(),
                                    });
                                }
                            }
                        }
                    }
                }
            }
        }

        // 4. General Scan with WHERE condition filtering
        let conditions = match clauses.where_clause {
            Some(w) => parse_conditions(w, table, params)?,
            None => Vec::new(),
        };

        let total_rows = table.rows.len();
        let mut matched_indices: Vec<usize> = if conditions.is_empty() {
            (0..total_rows)
                .into_par_iter()
                .filter(|&i| !table.is_deleted[i])
                .collect()
        } else if total_rows > 20_000 {
            (0..total_rows)
                .into_par_iter()
                .filter(|&i| !table.is_deleted[i] && evaluate_conditions(&table.rows[i], &conditions))
                .collect()
        } else {
            (0..total_rows)
                .filter(|&i| !table.is_deleted[i] && evaluate_conditions(&table.rows[i], &conditions))
                .collect()
        };

        // 5. ORDER BY sorting
        if let Some(order_col) = clauses.order_by_col {
            if let Some(col_idx) = table.get_column_index(order_col) {
                let desc = clauses.order_by_desc;
                if matched_indices.len() > 10_000 {
                    matched_indices.par_sort_by(|&a, &b| {
                        let va = &table.rows[a][col_idx];
                        let vb = &table.rows[b][col_idx];
                        if desc {
                            vb.cmp_value(va)
                        } else {
                            va.cmp_value(vb)
                        }
                    });
                } else {
                    matched_indices.sort_by(|&a, &b| {
                        let va = &table.rows[a][col_idx];
                        let vb = &table.rows[b][col_idx];
                        if desc {
                            vb.cmp_value(va)
                        } else {
                            va.cmp_value(vb)
                        }
                    });
                }
            }
        }

        // 6. OFFSET and LIMIT pagination
        let offset = clauses.offset;
        let limit = clauses.limit.unwrap_or(1000);
        let paged_indices = if offset < matched_indices.len() {
            let end = (offset + limit).min(matched_indices.len());
            &matched_indices[offset..end]
        } else {
            &[]
        };

        let projected_exprs = if select_clause.trim() == "*" {
            Vec::new()
        } else {
            parse_projection(select_clause, table, joined_table_opt, params)
        };

        let rows: Vec<serde_json::Value> = if !projected_exprs.is_empty() {
            paged_indices
                .iter()
                .map(|&idx| {
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
                    for expr in &projected_exprs {
                        match expr {
                            ProjectedExpr::PrimaryCol { col_idx, alias } => {
                                let v = primary_row.get(*col_idx).unwrap_or(&Value::Null);
                                map.insert(alias.clone(), value_to_json(v));
                            }
                            ProjectedExpr::JoinedCol { col_idx, alias } => {
                                let v = joined_row.and_then(|r| r.get(*col_idx)).unwrap_or(&Value::Null);
                                map.insert(alias.clone(), value_to_json(v));
                            }
                            ProjectedExpr::CoalescePrimaryCol { col_idx, default_val, alias } => {
                                let raw_v = primary_row.get(*col_idx).unwrap_or(&Value::Null);
                                let v = match raw_v {
                                    Value::Null => default_val,
                                    _ => raw_v,
                                };
                                map.insert(alias.clone(), value_to_json(v));
                            }
                        }
                    }
                    serde_json::Value::Object(map)
                })
                .collect()
        } else {
            paged_indices
                .iter()
                .map(|&idx| row_to_json(table, &table.rows[idx]))
                .collect()
        };

        let fields: Vec<FieldInfo> = if !projected_exprs.is_empty() {
            projected_exprs.iter().map(|e| {
                let alias = match e {
                    ProjectedExpr::PrimaryCol { alias, .. } => alias.clone(),
                    ProjectedExpr::JoinedCol { alias, .. } => alias.clone(),
                    ProjectedExpr::CoalescePrimaryCol { alias, .. } => alias.clone(),
                };
                FieldInfo {
                    name: alias,
                    data_type: "text".to_string(),
                }
            }).collect()
        } else {
            table.columns.iter().map(|c| FieldInfo {
                name: c.name.clone(),
                data_type: format!("{:?}", c.data_type).to_lowercase(),
            }).collect()
        };

        Ok(QueryResult {
            row_count: rows.len(),
            rows,
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
                    AggFunc::Sum(col_idx) => {
                        let (sum, _, _, _, _) = table.aggregate_stats(*col_idx);
                        row_map.insert(spec.alias.clone(), json!(sum));
                        fields.push(FieldInfo { name: spec.alias.clone(), data_type: "numeric".to_string() });
                    }
                    AggFunc::Avg(col_idx) => {
                        let (_, avg, _, _, _) = table.aggregate_stats(*col_idx);
                        row_map.insert(spec.alias.clone(), json!(avg));
                        fields.push(FieldInfo { name: spec.alias.clone(), data_type: "float8".to_string() });
                    }
                    AggFunc::Min(col_idx) => {
                        let (_, _, min, _, _) = table.aggregate_stats(*col_idx);
                        row_map.insert(spec.alias.clone(), json!(min));
                        fields.push(FieldInfo { name: spec.alias.clone(), data_type: "numeric".to_string() });
                    }
                    AggFunc::Max(col_idx) => {
                        let (_, _, _, max, _) = table.aggregate_stats(*col_idx);
                        row_map.insert(spec.alias.clone(), json!(max));
                        fields.push(FieldInfo { name: spec.alias.clone(), data_type: "numeric".to_string() });
                    }
                }
            }
        } else {
            // Filtered scan aggregate
            let conditions = parse_conditions(where_clause.unwrap(), table, params)?;
            let total_rows = table.rows.len();

            let matched_indices: Vec<usize> = if total_rows > 20_000 {
                (0..total_rows)
                    .into_par_iter()
                    .filter(|&i| !table.is_deleted[i] && evaluate_conditions(&table.rows[i], &conditions))
                    .collect()
            } else {
                (0..total_rows)
                    .filter(|&i| !table.is_deleted[i] && evaluate_conditions(&table.rows[i], &conditions))
                    .collect()
            };

            for spec in agg_specs {
                match &spec.func {
                    AggFunc::CountStar => {
                        let count = matched_indices.len();
                        row_map.insert(spec.alias.clone(), json!(count));
                        fields.push(FieldInfo { name: spec.alias.clone(), data_type: "int8".to_string() });
                    }
                    AggFunc::Sum(col_idx) => {
                        let sum: f64 = matched_indices.iter().filter_map(|&i| table.rows[i].get(*col_idx).and_then(|v| v.as_f64())).sum();
                        row_map.insert(spec.alias.clone(), json!(sum));
                        fields.push(FieldInfo { name: spec.alias.clone(), data_type: "numeric".to_string() });
                    }
                    AggFunc::Avg(col_idx) => {
                        let count = matched_indices.len();
                        let sum: f64 = matched_indices.iter().filter_map(|&i| table.rows[i].get(*col_idx).and_then(|v| v.as_f64())).sum();
                        let avg = if count > 0 { sum / count as f64 } else { 0.0 };
                        row_map.insert(spec.alias.clone(), json!(avg));
                        fields.push(FieldInfo { name: spec.alias.clone(), data_type: "float8".to_string() });
                    }
                    AggFunc::Min(col_idx) => {
                        let min: f64 = matched_indices.iter().filter_map(|&i| table.rows[i].get(*col_idx).and_then(|v| v.as_f64())).fold(f64::INFINITY, f64::min);
                        let final_min = if min.is_infinite() { 0.0 } else { min };
                        row_map.insert(spec.alias.clone(), json!(final_min));
                        fields.push(FieldInfo { name: spec.alias.clone(), data_type: "numeric".to_string() });
                    }
                    AggFunc::Max(col_idx) => {
                        let max: f64 = matched_indices.iter().filter_map(|&i| table.rows[i].get(*col_idx).and_then(|v| v.as_f64())).fold(f64::NEG_INFINITY, f64::max);
                        let final_max = if max.is_infinite() { 0.0 } else { max };
                        row_map.insert(spec.alias.clone(), json!(final_max));
                        fields.push(FieldInfo { name: spec.alias.clone(), data_type: "numeric".to_string() });
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
        let upper = sql.to_uppercase();
        let set_idx = upper.find("SET").ok_or("Missing SET in UPDATE")?;
        let where_idx = upper.find("WHERE").ok_or("Missing WHERE in UPDATE")?;

        let table_name = clean_col_name(sql[6..set_idx].trim());
        let set_clause = sql[set_idx + 3..where_idx].trim();
        
        let returning_idx = upper.find(" RETURNING ");
        let (where_clause, returning_cols) = if let Some(r_idx) = returning_idx {
            (&sql[where_idx + 5..r_idx].trim(), Some(sql[r_idx + 11..].trim()))
        } else {
            (&sql[where_idx + 5..].trim(), None)
        };

        let table = self.storage.get_table_mut(table_name).ok_or_else(|| format!("Table {} not found", table_name))?;

        // Parse multiple SET assignments: "updated_at" = CURRENT_TIMESTAMP, "student_count" = $1
        let mut updates = Vec::new();
        for assign in set_clause.split(',') {
            let assign = assign.trim();
            if assign.is_empty() {
                continue;
            }
            let eq_idx = assign.find('=').ok_or("Invalid assignment in SET clause")?;
            let col_name = clean_col_name(&assign[..eq_idx]);
            let val_str = assign[eq_idx + 1..].trim();

            let val = if val_str.eq_ignore_ascii_case("CURRENT_TIMESTAMP")
                || val_str.eq_ignore_ascii_case("NOW()")
                || val_str.to_uppercase().starts_with("CURRENT_TIMESTAMP")
                || val_str.to_uppercase().starts_with("NOW()")
            {
                Value::Text(get_current_timestamp())
            } else {
                parse_value(val_str, params)
            };

            let col_idx = table.get_column_index(col_name).ok_or_else(|| format!("Column {} not found in table {}", col_name, table_name))?;

            // Coerce value type according to column definition if needed
            let col_def = &table.columns[col_idx];
            let coerced_val = match (&col_def.data_type, &val) {
                (crate::types::DataType::Integer | crate::types::DataType::Serial, Value::Text(s)) => {
                    if let Ok(i) = s.parse::<i64>() {
                        Value::Int(i)
                    } else {
                        val
                    }
                }
                (crate::types::DataType::BigInt, Value::Text(s)) => {
                    if let Ok(i) = s.parse::<i64>() {
                        Value::Int(i)
                    } else {
                        val
                    }
                }
                (crate::types::DataType::Numeric, Value::Text(s)) => {
                    if let Ok(f) = s.parse::<f64>() {
                        Value::Float(f)
                    } else {
                        val
                    }
                }
                (crate::types::DataType::Boolean, Value::Text(s)) => {
                    if s.eq_ignore_ascii_case("true") || s == "1" {
                        Value::Bool(true)
                    } else if s.eq_ignore_ascii_case("false") || s == "0" {
                        Value::Bool(false)
                    } else {
                        val
                    }
                }
                _ => val,
            };

            updates.push((col_idx, coerced_val));
        }

        // Parse WHERE conditions
        let conditions = parse_conditions(where_clause, table, params)?;

        // Check if there is a primary key equality check for O(1) point update
        let pk_target = if let Some(pk_idx) = table.pk_col_idx {
            conditions.iter().find_map(|c| {
                if c.col_idx == pk_idx {
                    if let ColOp::Eq(ref v) = c.op {
                        return v.as_i64();
                    }
                }
                None
            })
        } else {
            None
        };

        let mut row_count = 0;
        let mut returned_rows = Vec::new();
        if let Some(target_pk) = pk_target {
            if let Some(&row_idx) = table.pk_index.get(&target_pk) {
                if !table.is_deleted[row_idx] && evaluate_conditions(&table.rows[row_idx], &conditions) {
                    table.update_row_multi(row_idx, &updates);
                    row_count = 1;
                    if returning_cols.is_some() {
                        returned_rows.push(row_to_json(table, &table.rows[row_idx]));
                    }
                }
            }
        } else {
            // General scan update
            for i in 0..table.rows.len() {
                if !table.is_deleted[i] && evaluate_conditions(&table.rows[i], &conditions) {
                    table.update_row_multi(i, &updates);
                    row_count += 1;
                    if returning_cols.is_some() {
                        returned_rows.push(row_to_json(table, &table.rows[i]));
                    }
                }
            }
        }

        let fields = if returning_cols.is_some() {
            table.columns.iter().map(|c| FieldInfo {
                name: c.name.clone(),
                data_type: format!("{:?}", c.data_type).to_lowercase(),
            }).collect()
        } else {
            vec![]
        };

        Ok(QueryResult {
            rows: returned_rows,
            row_count,
            fields,
            command: "UPDATE".to_string(),
        })
    }

    fn handle_delete(&mut self, sql: &str, params: &[Value]) -> Result<QueryResult, String> {
        let upper = sql.to_uppercase();
        let from_idx = upper.find("FROM").ok_or("Missing FROM in DELETE")?;
        let where_idx = upper.find("WHERE").ok_or("Missing WHERE in DELETE")?;

        let table_name = clean_col_name(sql[from_idx + 4..where_idx].trim());
        
        let returning_idx = upper.find(" RETURNING ");
        let (where_clause, returning_cols) = if let Some(r_idx) = returning_idx {
            (&sql[where_idx + 5..r_idx].trim(), Some(sql[r_idx + 11..].trim()))
        } else {
            (&sql[where_idx + 5..].trim(), None)
        };

        let table = self.storage.get_table_mut(table_name).ok_or_else(|| format!("Table {} not found", table_name))?;
        let conditions = parse_conditions(where_clause, table, params)?;

        let pk_target = if let Some(pk_idx) = table.pk_col_idx {
            conditions.iter().find_map(|c| {
                if c.col_idx == pk_idx {
                    if let ColOp::Eq(ref v) = c.op {
                        return v.as_i64();
                    }
                }
                None
            })
        } else {
            None
        };

        let mut row_count = 0;
        let mut returned_rows = Vec::new();
        if let Some(target_pk) = pk_target {
            if let Some(&row_idx) = table.pk_index.get(&target_pk) {
                if !table.is_deleted[row_idx] && evaluate_conditions(&table.rows[row_idx], &conditions) {
                    if returning_cols.is_some() {
                        returned_rows.push(row_to_json(table, &table.rows[row_idx]));
                    }
                    table.delete_row(row_idx);
                    row_count = 1;
                }
            }
        } else {
            for i in 0..table.rows.len() {
                if !table.is_deleted[i] && evaluate_conditions(&table.rows[i], &conditions) {
                    if returning_cols.is_some() {
                        returned_rows.push(row_to_json(table, &table.rows[i]));
                    }
                    table.delete_row(i);
                    row_count += 1;
                }
            }
        }

        let fields = if returning_cols.is_some() {
            table.columns.iter().map(|c| FieldInfo {
                name: c.name.clone(),
                data_type: format!("{:?}", c.data_type).to_lowercase(),
            }).collect()
        } else {
            vec![]
        };

        Ok(QueryResult {
            rows: returned_rows,
            row_count,
            fields,
            command: "DELETE".to_string(),
        })
    }
}

struct JoinClause<'a> {
    #[allow(dead_code)]
    is_left: bool,
    joined_table_name: &'a str,
    primary_join_col: &'a str,
    joined_join_col: &'a str,
}

struct SelectClauses<'a> {
    table_name: &'a str,
    join_clause: Option<JoinClause<'a>>,
    where_clause: Option<&'a str>,
    order_by_col: Option<&'a str>,
    order_by_desc: bool,
    limit: Option<usize>,
    offset: usize,
}

enum ProjectedExpr {
    PrimaryCol { col_idx: usize, alias: String },
    JoinedCol { col_idx: usize, alias: String },
    CoalescePrimaryCol { col_idx: usize, default_val: Value, alias: String },
}

fn split_projection_items(s: &str) -> Vec<String> {
    let mut items = Vec::new();
    let mut current = String::new();
    let mut depth: usize = 0;
    for c in s.chars() {
        if c == '(' {
            depth += 1;
            current.push(c);
        } else if c == ')' {
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
    joined_table: Option<&crate::storage::table::Table>,
    params: &[Value],
) -> Vec<ProjectedExpr> {
    let items = split_projection_items(select_clause);
    let mut exprs = Vec::new();

    for item in items {
        let upper = item.to_uppercase();
        let (expr_part, alias) = if let Some(as_idx) = upper.rfind(" AS ") {
            (item[..as_idx].trim(), item[as_idx + 4..].trim().trim_matches('"').to_string())
        } else {
            let col = clean_col_name(item.trim());
            (item.trim(), col.to_string())
        };

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
                }
            }
        }

        // 2. Check if expression references joined_table
        if let Some(jt) = joined_table {
            if expr_part.to_lowercase().contains(&jt.name.to_lowercase()) {
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
        } else if let Some(jt) = joined_table {
            if let Some(col_idx) = jt.get_column_index(col_name) {
                exprs.push(ProjectedExpr::JoinedCol { col_idx, alias });
            }
        }
    }

    exprs
}

fn parse_select_clauses<'a>(after_from: &'a str) -> SelectClauses<'a> {
    let upper = after_from.to_uppercase();

    // Check for JOIN keywords
    let (join_pos, kw_len, is_left) = if let Some(pos) = upper.find("LEFT JOIN ") {
        (Some(pos), 10, true)
    } else if let Some(pos) = upper.find("LEFT OUTER JOIN ") {
        (Some(pos), 16, true)
    } else if let Some(pos) = upper.find("INNER JOIN ") {
        (Some(pos), 11, false)
    } else if let Some(pos) = upper.find("JOIN ") {
        (Some(pos), 5, false)
    } else {
        (None, 0, false)
    };

    let table_name = match join_pos {
        Some(pos) => after_from[..pos].trim().trim_matches('"'),
        None => {
            let where_pos = upper.find("WHERE ");
            let order_pos = upper.find("ORDER BY ");
            let limit_pos = upper.find("LIMIT ");
            let offset_pos = upper.find("OFFSET ");
            let first_kw_pos = [where_pos, order_pos, limit_pos, offset_pos]
                .into_iter()
                .filter_map(|x| x)
                .min();
            match first_kw_pos {
                Some(pos) => after_from[..pos].trim().trim_matches('"'),
                None => after_from.trim().trim_matches('"'),
            }
        }
    };

    let join_clause = match join_pos {
        Some(j_pos) => {
            let after_j = &after_from[j_pos + kw_len..];
            let upper_j = after_j.to_uppercase();
            if let Some(on_pos) = upper_j.find(" ON ") {
                let joined_table_name = after_j[..on_pos].trim().trim_matches('"');
                let after_on = &after_j[on_pos + 4..];
                let upper_on = after_on.to_uppercase();
                let end_pos = [upper_on.find("WHERE "), upper_on.find("ORDER BY "), upper_on.find("LIMIT "), upper_on.find("OFFSET ")]
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

                    let (primary_col, joined_col) = if side_a.contains(joined_table_name) {
                        (col_b, col_a)
                    } else if side_b.contains(joined_table_name) {
                        (col_a, col_b)
                    } else if side_a.contains(table_name) {
                        (col_a, col_b)
                    } else {
                        (col_b, col_a)
                    };

                    Some(JoinClause {
                        is_left,
                        joined_table_name,
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

    let where_pos = upper.find("WHERE ");
    let where_clause = where_pos.map(|w_idx| {
        let after_w = &after_from[w_idx + 6..];
        let upper_w = after_w.to_uppercase();
        let end_idx = [upper_w.find("ORDER BY "), upper_w.find("LIMIT "), upper_w.find("OFFSET ")]
            .into_iter()
            .filter_map(|x| x)
            .min();
        match end_idx {
            Some(e) => after_w[..e].trim(),
            None => after_w.trim(),
        }
    });

    let order_pos = upper.find("ORDER BY ");
    let (order_by_col, order_by_desc) = match order_pos {
        Some(o_idx) => {
            let after_o = &after_from[o_idx + 9..];
            let upper_o = after_o.to_uppercase();
            let end_idx = [upper_o.find("LIMIT "), upper_o.find("OFFSET ")]
                .into_iter()
                .filter_map(|x| x)
                .min();
            let order_part = match end_idx {
                Some(e) => after_o[..e].trim(),
                None => after_o.trim(),
            };
            let mut parts = order_part.split_whitespace();
            let col = parts.next().unwrap_or("").trim_matches('"');
            let desc = parts.next().map(|s| s.eq_ignore_ascii_case("DESC")).unwrap_or(false);
            (if col.is_empty() { None } else { Some(col) }, desc)
        }
        None => (None, false),
    };

    let limit_pos = upper.find("LIMIT ");
    let limit = match limit_pos {
        Some(l_idx) => {
            let after_l = &after_from[l_idx + 6..];
            let upper_l = after_l.to_uppercase();
            let end_idx = upper_l.find("OFFSET ");
            let limit_part = match end_idx {
                Some(e) => after_l[..e].trim(),
                None => after_l.trim(),
            };
            limit_part.split_whitespace().next().and_then(|s| s.parse::<usize>().ok())
        }
        None => None,
    };

    let offset_pos = upper.find("OFFSET ");
    let offset = match offset_pos {
        Some(off_idx) => {
            let after_off = after_from[off_idx + 7..].trim();
            after_off.split_whitespace().next().and_then(|s| s.parse::<usize>().ok()).unwrap_or(0)
        }
        None => 0,
    };

    SelectClauses {
        table_name,
        join_clause,
        where_clause,
        order_by_col,
        order_by_desc,
        limit,
        offset,
    }
}

#[derive(Clone, Debug)]
enum AggFunc {
    CountStar,
    Sum(usize),
    Avg(usize),
    Min(usize),
    Max(usize),
}

struct AggSpec {
    func: AggFunc,
    alias: String,
}

fn parse_aggregations(select_clause: &str, table: &crate::storage::table::Table) -> Vec<AggSpec> {
    let mut specs = Vec::new();
    let parts: Vec<&str> = select_clause.split(',').collect();

    for part in parts {
        let part_trim = part.trim();
        let upper = part_trim.to_uppercase();

        if upper.contains("COUNT(*)") || upper.contains("COUNT(1)") {
            let alias = if let Some(as_idx) = upper.find(" AS ") {
                part_trim[as_idx + 4..].trim().trim_matches('"').to_string()
            } else {
                "count".to_string()
            };
            specs.push(AggSpec { func: AggFunc::CountStar, alias });
        } else if let Some(sum_idx) = upper.find("SUM(") {
            if let Some(close_p) = upper[sum_idx..].find(')') {
                let col_name = part_trim[sum_idx + 4..sum_idx + close_p].trim().trim_matches('"');
                let alias = if let Some(as_idx) = upper.find(" AS ") {
                    part_trim[as_idx + 4..].trim().trim_matches('"').to_string()
                } else {
                    "sum".to_string()
                };
                if let Some(col_idx) = table.get_column_index(col_name) {
                    specs.push(AggSpec { func: AggFunc::Sum(col_idx), alias });
                }
            }
        } else if let Some(avg_idx) = upper.find("AVG(") {
            if let Some(close_p) = upper[avg_idx..].find(')') {
                let col_name = part_trim[avg_idx + 4..avg_idx + close_p].trim().trim_matches('"');
                let alias = if let Some(as_idx) = upper.find(" AS ") {
                    part_trim[as_idx + 4..].trim().trim_matches('"').to_string()
                } else {
                    "avg".to_string()
                };
                if let Some(col_idx) = table.get_column_index(col_name) {
                    specs.push(AggSpec { func: AggFunc::Avg(col_idx), alias });
                }
            }
        } else if let Some(min_idx) = upper.find("MIN(") {
            if let Some(close_p) = upper[min_idx..].find(')') {
                let col_name = part_trim[min_idx + 4..min_idx + close_p].trim().trim_matches('"');
                let alias = if let Some(as_idx) = upper.find(" AS ") {
                    part_trim[as_idx + 4..].trim().trim_matches('"').to_string()
                } else {
                    "min".to_string()
                };
                if let Some(col_idx) = table.get_column_index(col_name) {
                    specs.push(AggSpec { func: AggFunc::Min(col_idx), alias });
                }
            }
        } else if let Some(max_idx) = upper.find("MAX(") {
            if let Some(close_p) = upper[max_idx..].find(')') {
                let col_name = part_trim[max_idx + 4..max_idx + close_p].trim().trim_matches('"');
                let alias = if let Some(as_idx) = upper.find(" AS ") {
                    part_trim[as_idx + 4..].trim().trim_matches('"').to_string()
                } else {
                    "max".to_string()
                };
                if let Some(col_idx) = table.get_column_index(col_name) {
                    specs.push(AggSpec { func: AggFunc::Max(col_idx), alias });
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
    Gt(f64),
    Lt(f64),
    Gte(f64),
    Lte(f64),
    Between(f64, f64),
    In(Vec<Value>),
    IsNull,
    IsNotNull,
}

#[derive(Clone, Debug)]
struct Condition {
    col_idx: usize,
    op: ColOp,
}

fn clean_col_name(raw: &str) -> &str {
    let s = raw.trim();
    if let Some(dot_idx) = s.rfind('.') {
        s[dot_idx + 1..].trim().trim_matches('"')
    } else {
        s.trim_matches('"')
    }
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

fn split_where_conditions(w: &str) -> Vec<String> {
    let mut parts = Vec::new();
    let mut current = String::new();
    let tokens: Vec<&str> = w.split_whitespace().collect();
    let mut i = 0;
    while i < tokens.len() {
        if tokens[i].eq_ignore_ascii_case("AND") {
            if current.to_uppercase().contains("BETWEEN") && !current.to_uppercase().contains(" AND ") {
                current.push(' ');
                current.push_str(tokens[i]);
            } else {
                if !current.trim().is_empty() {
                    parts.push(current.trim().to_string());
                    current.clear();
                }
            }
        } else {
            if !current.is_empty() {
                current.push(' ');
            }
            current.push_str(tokens[i]);
        }
        i += 1;
    }
    if !current.trim().is_empty() {
        parts.push(current.trim().to_string());
    }
    parts
}

fn parse_number(token: &str, params: &[Value]) -> Result<f64, String> {
    let t = token.trim();
    if t.starts_with('$') {
        let p_idx = t[1..].parse::<usize>().map_err(|_| format!("Invalid param {}", t))?;
        params.get(p_idx - 1).and_then(|v| v.as_f64()).ok_or_else(|| format!("Param {} not found or not number", t))
    } else {
        t.parse::<f64>().map_err(|_| format!("Invalid number {}", t))
    }
}

fn parse_value(token: &str, params: &[Value]) -> Value {
    let t = token.trim();
    if t.starts_with('$') {
        if let Ok(p_idx) = t[1..].parse::<usize>() {
            return params.get(p_idx - 1).cloned().unwrap_or(Value::Null);
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
        return Value::Text(t[1..t.len() - 1].to_string());
    }
    if let Ok(i) = t.parse::<i64>() {
        return Value::Int(i);
    }
    if let Ok(f) = t.parse::<f64>() {
        return Value::Float(f);
    }
    Value::Text(t.to_string())
}

fn parse_conditions(where_clause: &str, table: &crate::storage::table::Table, params: &[Value]) -> Result<Vec<Condition>, String> {
    let mut conditions = Vec::new();
    let parts = split_where_conditions(where_clause);

    for part in parts {
        let upper = part.to_uppercase();

        if let Some(pos) = upper.find(" IS NOT NULL") {
            let col_name = clean_col_name(&part[..pos]);
            let col_idx = table.get_column_index(col_name).ok_or_else(|| format!("Column {} not found", col_name))?;
            conditions.push(Condition { col_idx, op: ColOp::IsNotNull });
        } else if let Some(pos) = upper.find(" IS NULL") {
            let col_name = clean_col_name(&part[..pos]);
            let col_idx = table.get_column_index(col_name).ok_or_else(|| format!("Column {} not found", col_name))?;
            conditions.push(Condition { col_idx, op: ColOp::IsNull });
        } else if let Some(b_idx) = upper.find(" BETWEEN ") {
            let col_name = clean_col_name(&part[..b_idx]);
            let col_idx = table.get_column_index(col_name).ok_or_else(|| format!("Column {} not found", col_name))?;
            let rest = &part[b_idx + 9..];
            let and_idx = rest.to_uppercase().find(" AND ").ok_or("Missing AND in BETWEEN clause")?;
            let min_token = rest[..and_idx].trim();
            let max_token = rest[and_idx + 5..].trim();
            let min_val = parse_number(min_token, params)?;
            let max_val = parse_number(max_token, params)?;
            conditions.push(Condition { col_idx, op: ColOp::Between(min_val, max_val) });
        } else if let Some(in_idx) = upper.find(" IN ") {
            let col_name = clean_col_name(&part[..in_idx]);
            let col_idx = table.get_column_index(col_name).ok_or_else(|| format!("Column {} not found", col_name))?;
            let rest = part[in_idx + 4..].trim();
            if rest.starts_with('(') && rest.ends_with(')') {
                let inside = &rest[1..rest.len() - 1];
                let list: Vec<Value> = inside.split(',').map(|tok| parse_value(tok, params)).collect();
                conditions.push(Condition { col_idx, op: ColOp::In(list) });
            }
        } else if let Some(op_idx) = part.find("!=") {
            let col_name = clean_col_name(&part[..op_idx]);
            let col_idx = table.get_column_index(col_name).ok_or_else(|| format!("Column {} not found", col_name))?;
            let val = parse_value(&part[op_idx + 2..], params);
            conditions.push(Condition { col_idx, op: ColOp::NotEq(val) });
        } else if let Some(op_idx) = part.find("<>") {
            let col_name = clean_col_name(&part[..op_idx]);
            let col_idx = table.get_column_index(col_name).ok_or_else(|| format!("Column {} not found", col_name))?;
            let val = parse_value(&part[op_idx + 2..], params);
            conditions.push(Condition { col_idx, op: ColOp::NotEq(val) });
        } else if let Some(op_idx) = part.find(">=") {
            let col_name = clean_col_name(&part[..op_idx]);
            let col_idx = table.get_column_index(col_name).ok_or_else(|| format!("Column {} not found", col_name))?;
            let num = parse_number(&part[op_idx + 2..], params)?;
            conditions.push(Condition { col_idx, op: ColOp::Gte(num) });
        } else if let Some(op_idx) = part.find("<=") {
            let col_name = clean_col_name(&part[..op_idx]);
            let col_idx = table.get_column_index(col_name).ok_or_else(|| format!("Column {} not found", col_name))?;
            let num = parse_number(&part[op_idx + 2..], params)?;
            conditions.push(Condition { col_idx, op: ColOp::Lte(num) });
        } else if let Some(op_idx) = part.find('>') {
            let col_name = clean_col_name(&part[..op_idx]);
            let col_idx = table.get_column_index(col_name).ok_or_else(|| format!("Column {} not found", col_name))?;
            let num = parse_number(&part[op_idx + 1..], params)?;
            conditions.push(Condition { col_idx, op: ColOp::Gt(num) });
        } else if let Some(op_idx) = part.find('<') {
            let col_name = clean_col_name(&part[..op_idx]);
            let col_idx = table.get_column_index(col_name).ok_or_else(|| format!("Column {} not found", col_name))?;
            let num = parse_number(&part[op_idx + 1..], params)?;
            conditions.push(Condition { col_idx, op: ColOp::Lt(num) });
        } else if let Some(op_idx) = part.find('=') {
            let left_raw = part[..op_idx].trim();
            let upper_left = left_raw.to_uppercase();
            let val = parse_value(&part[op_idx + 1..], params);

            if upper_left.starts_with("LOWER(") && left_raw.ends_with(')') {
                let inner = &left_raw[6..left_raw.len() - 1].trim();
                let col_name = clean_col_name(inner);
                let col_idx = table.get_column_index(col_name).ok_or_else(|| format!("Column {} not found", col_name))?;
                let target_str = match &val {
                    Value::Text(s) => s.to_lowercase(),
                    Value::Int(i) => i.to_string(),
                    _ => "".to_string(),
                };
                conditions.push(Condition { col_idx, op: ColOp::LowerEq(target_str) });
            } else if upper_left.starts_with("UPPER(") && left_raw.ends_with(')') {
                let inner = &left_raw[6..left_raw.len() - 1].trim();
                let col_name = clean_col_name(inner);
                let col_idx = table.get_column_index(col_name).ok_or_else(|| format!("Column {} not found", col_name))?;
                let target_str = match &val {
                    Value::Text(s) => s.to_uppercase(),
                    Value::Int(i) => i.to_string(),
                    _ => "".to_string(),
                };
                conditions.push(Condition { col_idx, op: ColOp::UpperEq(target_str) });
            } else {
                let col_name = clean_col_name(left_raw);
                let col_idx = table.get_column_index(col_name).ok_or_else(|| format!("Column {} not found", col_name))?;
                conditions.push(Condition { col_idx, op: ColOp::Eq(val) });
            }
        }
    }

    Ok(conditions)
}

fn evaluate_conditions(row: &[Value], conditions: &[Condition]) -> bool {
    for cond in conditions {
        if cond.col_idx >= row.len() {
            return false;
        }
        let val = &row[cond.col_idx];
        match &cond.op {
            ColOp::IsNull => {
                match val {
                    Value::Null => {},
                    _ => return false,
                }
            }
            ColOp::IsNotNull => {
                match val {
                    Value::Null => return false,
                    _ => {},
                }
            }
            ColOp::NotEq(target) => {
                if val.is_equal(target) {
                    return false;
                }
            }
            ColOp::Eq(target) => {
                if !val.is_equal(target) {
                    return false;
                }
            }
            ColOp::LowerEq(target) => {
                match val {
                    Value::Text(s) => {
                        if s.to_lowercase() != *target {
                            return false;
                        }
                    }
                    Value::Int(i) => {
                        if i.to_string() != *target {
                            return false;
                        }
                    }
                    _ => return false,
                }
            }
            ColOp::UpperEq(target) => {
                match val {
                    Value::Text(s) => {
                        if s.to_uppercase() != *target {
                            return false;
                        }
                    }
                    Value::Int(i) => {
                        if i.to_string() != *target {
                            return false;
                        }
                    }
                    _ => return false,
                }
            }
            ColOp::Gt(threshold) => {
                if let Some(f) = val.as_f64() {
                    if f <= *threshold { return false; }
                } else { return false; }
            }
            ColOp::Gte(threshold) => {
                if let Some(f) = val.as_f64() {
                    if f < *threshold { return false; }
                } else { return false; }
            }
            ColOp::Lt(threshold) => {
                if let Some(f) = val.as_f64() {
                    if f >= *threshold { return false; }
                } else { return false; }
            }
            ColOp::Lte(threshold) => {
                if let Some(f) = val.as_f64() {
                    if f > *threshold { return false; }
                } else { return false; }
            }
            ColOp::Between(min_val, max_val) => {
                if let Some(f) = val.as_f64() {
                    if f < *min_val || f > *max_val { return false; }
                } else { return false; }
            }
            ColOp::In(list) => {
                let mut found = false;
                for item in list {
                    if val == item {
                        found = true;
                        break;
                    }
                }
                if !found { return false; }
            }
        }
    }
    true
}

fn value_to_json(val: &Value) -> serde_json::Value {
    match val {
        Value::Null => serde_json::Value::Null,
        Value::Bool(b) => json!(*b),
        Value::Int(n) => json!(*n),
        Value::Float(f) => json!(*f),
        Value::Text(s) => json!(s),
    }
}

fn row_to_json(table: &crate::storage::table::Table, row: &[Value]) -> serde_json::Value {
    let mut map = serde_json::Map::with_capacity(table.columns.len());
    for (i, col) in table.columns.iter().enumerate() {
        let v = row.get(i).unwrap_or(&Value::Null);
        map.insert(col.name.clone(), value_to_json(v));
    }
    serde_json::Value::Object(map)
}
