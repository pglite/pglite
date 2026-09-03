use crate::storage::page::SlottedPage;
use crate::types::{ColumnDef, Value};
use std::collections::BTreeMap;

#[derive(Debug, Clone)]
pub struct Table {
    pub name: String,
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
        let pk_col_idx = columns.iter().position(|c| c.is_primary_key);
        Self {
            name,
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
        // Handle SERIAL / Primary Key auto-increment
        let mut assigned_pk = 0;
        if let Some(pk_idx) = self.pk_col_idx {
            if pk_idx < row.len() {
                match &row[pk_idx] {
                    Value::Null => {
                        assigned_pk = self.auto_increment;
                        self.auto_increment += 1;
                        row[pk_idx] = Value::Int(assigned_pk);
                    }
                    Value::Int(v) => {
                        assigned_pk = *v;
                        if *v >= self.auto_increment {
                            self.auto_increment = *v + 1;
                        }
                    }
                    _ => {}
                }
            } else if pk_idx == row.len() {
                assigned_pk = self.auto_increment;
                self.auto_increment += 1;
                row.push(Value::Int(assigned_pk));
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
}
