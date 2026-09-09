use crate::storage::page::SlottedPage;
use crate::types::{ColumnDef, Value};
use std::collections::BTreeMap;

#[derive(Debug, Clone)]
pub struct Table {
    pub name: String,
    pub comment: Option<String>,
    pub columns: Vec<ColumnDef>,
    pub pk_col_idx: Option<usize>,
    pub auto_increment: i64,
    pub rows: Vec<Vec<Value>>,
    pub pk_index: BTreeMap<i64, usize>,
    pub is_deleted: Vec<bool>,
    pub active_count: usize,
    pub pages: Vec<SlottedPage>,
}

impl Table {
    pub fn new(name: String, columns: Vec<ColumnDef>) -> Self {
        let mut seen = std::collections::HashSet::new();
        let mut deduped_columns = Vec::with_capacity(columns.len());
        for col in columns {
            let lower = col.name.to_lowercase();
            if seen.insert(lower) {
                deduped_columns.push(col);
            }
        }
        let mut columns = deduped_columns;

        let pk_col_idx = columns.iter().position(|c| c.is_primary_key)
            .or_else(|| columns.iter().position(|c| c.data_type == crate::types::DataType::Serial))
            .or_else(|| columns.iter().position(|c| c.name.eq_ignore_ascii_case("id")))
            .or_else(|| columns.iter().position(|c| c.name.eq_ignore_ascii_case("_id")));

        // Ensure the detected PK column is marked as primary key
        if let Some(idx) = pk_col_idx {
            columns[idx].is_primary_key = true;
        }

        Self {
            name,
            comment: None,
            columns,
            pk_col_idx,
            auto_increment: 1,
            rows: Vec::new(),
            pk_index: BTreeMap::new(),
            is_deleted: Vec::new(),
            active_count: 0,
            pages: Vec::new(),
        }
    }

    pub fn insert(&mut self, mut row: Vec<Value>) -> i64 {
        // Ensure row has slots for all table columns and populate default values
        while row.len() < self.columns.len() {
            let col_idx = row.len();
            let def_val = if let Some(def_str) = &self.columns[col_idx].default_value {
                let trimmed = def_str.trim();
                let up = trimmed.to_uppercase();
                if up.starts_with("GEN_RANDOM_UUID") || up.starts_with("UUID_GENERATE_V4") {
                    Value::text(uuid::Uuid::new_v4().to_string())
                } else if trimmed.starts_with('\'') && trimmed.ends_with('\'') && trimmed.len() >= 2 {
                    Value::text(&trimmed[1..trimmed.len() - 1])
                } else if let Ok(n) = trimmed.parse::<i64>() {
                    Value::Int(n)
                } else if let Ok(f) = trimmed.parse::<f64>() {
                    Value::Float(f)
                } else if trimmed.eq_ignore_ascii_case("TRUE") {
                    Value::Bool(true)
                } else if trimmed.eq_ignore_ascii_case("FALSE") {
                    Value::Bool(false)
                } else {
                    Value::text(trimmed)
                }
            } else if self.columns[col_idx].data_type == crate::types::DataType::Uuid {
                Value::Null
            } else {
                Value::Null
            };
            row.push(def_val);
        }
        if row.len() > self.columns.len() {
            row.truncate(self.columns.len());
        }

        // Handle SERIAL / Primary Key auto-increment for integer/serial ID types or UUID
        let mut assigned_pk = 0;
        if let Some(pk_idx) = self.pk_col_idx {
            let is_uuid_col = self.columns[pk_idx].data_type == crate::types::DataType::Uuid
                || self.columns[pk_idx].default_value.as_ref().map(|d| d.to_uppercase().contains("UUID")).unwrap_or(false);

            let is_missing_or_null = match row.get(pk_idx) {
                None | Some(Value::Null) => true,
                Some(Value::Text(s)) if s.trim().is_empty() => true,
                _ => false,
            };

            if is_missing_or_null {
                if is_uuid_col {
                    row[pk_idx] = Value::text(uuid::Uuid::new_v4().to_string());
                } else {
                    assigned_pk = self.auto_increment;
                    self.auto_increment += 1;
                    row[pk_idx] = Value::Int(assigned_pk);
                }
            } else if let Some(val) = row.get(pk_idx) {
                if let Some(v) = val.as_i64() {
                    assigned_pk = v;
                    if v >= self.auto_increment {
                        self.auto_increment = v + 1;
                    }
                }
            }
        }

        let row_idx = self.rows.len();
        if let Some(pk_idx) = self.pk_col_idx {
            if let Some(val) = row.get(pk_idx) {
                if let Some(pk_val) = val.as_i64() {
                    self.pk_index.insert(pk_val, row_idx);
                }
            }
        }

        self.rows.push(row);
        self.is_deleted.push(false);
        self.active_count += 1;

        assigned_pk
    }

    pub fn insert_batch(&mut self, rows: Vec<Vec<Value>>) {
        let total = rows.len();
        self.rows.reserve(total);
        self.is_deleted.reserve(total);
        for row in rows {
            self.insert(row);
        }
    }

    pub fn get_by_pk(&self, pk: i64) -> Option<&Vec<Value>> {
        if let Some(&row_idx) = self.pk_index.get(&pk) {
            if !self.is_deleted[row_idx] {
                return Some(&self.rows[row_idx]);
            }
        }
        None
    }

    pub fn update_by_pk(&mut self, pk: i64, col_idx: usize, val: Value) -> bool {
        if let Some(&row_idx) = self.pk_index.get(&pk) {
            if !self.is_deleted[row_idx] && col_idx < self.rows[row_idx].len() {
                self.rows[row_idx][col_idx] = val;
                return true;
            }
        }
        false
    }

    pub fn update_row_multi(&mut self, row_idx: usize, updates: &[(usize, Value)]) {
        if row_idx < self.rows.len() && !self.is_deleted[row_idx] {
            for (col_idx, val) in updates {
                if *col_idx < self.rows[row_idx].len() {
                    self.rows[row_idx][*col_idx] = val.clone();
                }
            }
        }
    }

    pub fn delete_row(&mut self, row_idx: usize) -> bool {
        if row_idx < self.rows.len() && !self.is_deleted[row_idx] {
            self.is_deleted[row_idx] = true;
            self.active_count = self.active_count.saturating_sub(1);
            return true;
        }
        false
    }

    pub fn delete_by_pk(&mut self, pk: i64) -> bool {
        if let Some(&row_idx) = self.pk_index.get(&pk) {
            if !self.is_deleted[row_idx] {
                self.is_deleted[row_idx] = true;
                self.active_count = self.active_count.saturating_sub(1);
                return true;
            }
        }
        false
    }

    pub fn count(&self) -> usize {
        self.active_count
    }

    pub fn get_column_index(&self, name: &str) -> Option<usize> {
        let clean = name.trim_matches('"').to_lowercase();
        self.columns
            .iter()
            .position(|c| c.name.to_lowercase() == clean)
    }

    pub fn drop_column(&mut self, name: &str) -> bool {
        let clean = name.trim_matches('"').to_lowercase();
        if let Some(pos) = self.columns.iter().position(|c| c.name.to_lowercase() == clean) {
            self.columns.remove(pos);
            for row in &mut self.rows {
                if pos < row.len() {
                    row.remove(pos);
                }
            }
            self.pk_col_idx = self.columns.iter().position(|c| c.is_primary_key)
                .or_else(|| self.columns.iter().position(|c| c.data_type == crate::types::DataType::Serial))
                .or_else(|| self.columns.iter().position(|c| c.name.eq_ignore_ascii_case("id")))
                .or_else(|| self.columns.iter().position(|c| c.name.eq_ignore_ascii_case("_id")));
            return true;
        }
        false
    }

    pub fn find_first_by_col(&self, col_idx: usize, target: &Value) -> Option<&Vec<Value>> {
        for (i, row) in self.rows.iter().enumerate() {
            if !self.is_deleted[i] && col_idx < row.len() && &row[col_idx] == target {
                return Some(row);
            }
        }
        None
    }

    pub fn aggregate_stats(&self, col_idx: usize) -> (f64, f64, f64, f64, usize) {
        use rayon::prelude::*;
        let total_rows = self.rows.len();
        if total_rows == 0 {
            return (0.0, 0.0, 0.0, 0.0, 0);
        }

        let (sum, min, max, count) = (0..total_rows)
            .into_par_iter()
            .filter(|&i| !self.is_deleted[i])
            .filter_map(|i| {
                if col_idx < self.rows[i].len() {
                    self.rows[i][col_idx].as_f64()
                } else {
                    None
                }
            })
            .fold(|| (0.0f64, f64::INFINITY, f64::NEG_INFINITY, 0usize), |(s, mi, ma, c), val| {
                (s + val, mi.min(val), ma.max(val), c + 1)
            })
            .reduce(|| (0.0f64, f64::INFINITY, f64::NEG_INFINITY, 0usize), |(s1, mi1, ma1, c1), (s2, mi2, ma2, c2)| {
                (s1 + s2, mi1.min(mi2), ma1.max(ma2), c1 + c2)
            });

        let avg = if count > 0 { sum / count as f64 } else { 0.0 };
        let final_min = if min.is_infinite() { 0.0 } else { min };
        let final_max = if max.is_infinite() { 0.0 } else { max };
        (sum, avg, final_min, final_max, count)
    }
}
