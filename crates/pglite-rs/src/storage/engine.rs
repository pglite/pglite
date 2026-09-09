use crate::storage::table::Table;
use crate::storage::wal::{WalManager, WalRecord};
use crate::types::{ColumnDef, Value};
use std::collections::HashMap;

#[derive(Debug, Clone)]
pub enum UndoAction {
    DeleteLastInsertedRow { table: String },
    RestoreRow { table: String, row_idx: usize, col_idx: usize, old_val: Value },
    UndeleteRow { table: String, row_idx: usize },
}

pub struct StorageEngine {
    pub filepath: String,
    pub tables: HashMap<String, Table>,
    pub in_transaction: bool,
    pub tx_undo_log: Vec<UndoAction>,
    pub savepoints: HashMap<String, usize>,
    pub wal: WalManager,
}

impl StorageEngine {
    pub fn new(filepath: String) -> Self {
        let wal = WalManager::new(&filepath);
        let mut engine = Self {
            filepath,
            tables: HashMap::new(),
            in_transaction: false,
            tx_undo_log: Vec::new(),
            savepoints: HashMap::new(),
            wal,
        };

        let mut table_hist_cols: HashMap<String, Vec<String>> = HashMap::new();

        // Replay WAL records recovered via zero-copy mmap
        let records = engine.wal.buffer.clone();
        for record in records {
            match record {
                WalRecord::CreateTable { name, columns } => {
                    let clean = name.to_lowercase();
                    table_hist_cols.insert(clean.clone(), columns.iter().map(|c| c.name.clone()).collect());
                    engine.tables.insert(clean, Table::new(name, columns));
                }
                WalRecord::DropTable { name } => {
                    let clean = name.to_lowercase();
                    table_hist_cols.remove(&clean);
                    engine.tables.remove(&clean);
                }
                WalRecord::Insert { table, row } => {
                    let clean = table.to_lowercase();
                    if let Some(t) = engine.get_table_mut(&clean) {
                        let hist_cols = table_hist_cols.get(&clean);
                        if let Some(h_cols) = hist_cols {
                            if row.len() > t.columns.len() && row.len() <= h_cols.len() {
                                let mut aligned_row = Vec::with_capacity(t.columns.len());
                                for col in &t.columns {
                                    if let Some(h_idx) = h_cols.iter().position(|h| h.eq_ignore_ascii_case(&col.name)) {
                                        if h_idx < row.len() {
                                            aligned_row.push(row[h_idx].clone());
                                        } else {
                                            aligned_row.push(Value::Null);
                                        }
                                    } else {
                                        aligned_row.push(Value::Null);
                                    }
                                }
                                t.insert(aligned_row);
                                continue;
                            }
                        }
                        t.insert(row);
                    }
                }
                WalRecord::Update { table, pk, col_idx, new_val } => {
                    let clean = table.to_lowercase();
                    if let Some(t) = engine.get_table_mut(&clean) {
                        let hist_cols = table_hist_cols.get(&clean);
                        let target_col_idx = if let Some(h_cols) = hist_cols {
                            if h_cols.len() > t.columns.len() && col_idx < h_cols.len() {
                                t.get_column_index(&h_cols[col_idx]).unwrap_or(col_idx)
                            } else {
                                col_idx
                            }
                        } else {
                            col_idx
                        };
                        t.update_by_pk(pk, target_col_idx, new_val);
                    }
                }
                WalRecord::Delete { table, pk } => {
                    let clean = table.to_lowercase();
                    if let Some(t) = engine.get_table_mut(&clean) {
                        t.delete_by_pk(pk);
                    }
                }
                WalRecord::CommentOnTable { table, comment } => {
                    let clean = table.to_lowercase();
                    if let Some(t) = engine.get_table_mut(&clean) {
                        t.comment = comment;
                    }
                }
                WalRecord::CommentOnColumn { table, column, comment } => {
                    let clean = table.to_lowercase();
                    if let Some(t) = engine.get_table_mut(&clean) {
                        if let Some(c) = t.columns.iter_mut().find(|c| c.name.eq_ignore_ascii_case(&column)) {
                            c.comment = comment;
                        }
                    }
                }
                WalRecord::AlterTableAddColumn { table, column, default_val } => {
                    let clean = table.to_lowercase();
                    table_hist_cols.entry(clean.clone()).or_default().push(column.name.clone());
                    if let Some(t) = engine.get_table_mut(&clean) {
                        if !t.columns.iter().any(|c| c.name.eq_ignore_ascii_case(&column.name)) {
                            t.columns.push(column);
                            for row in &mut t.rows {
                                row.push(default_val.clone());
                            }
                        }
                    }
                }
                WalRecord::AlterTableDropColumn { table, column } => {
                    let clean = table.to_lowercase();
                    if let Some(h_cols) = table_hist_cols.get_mut(&clean) {
                        if let Some(pos) = h_cols.iter().position(|h| h.eq_ignore_ascii_case(&column)) {
                            h_cols.remove(pos);
                        }
                    }
                    if let Some(t) = engine.get_table_mut(&clean) {
                        t.drop_column(&column);
                    }
                }
                _ => {}
            }
        }

        engine
    }

    pub fn create_table(&mut self, name: String, columns: Vec<ColumnDef>) -> Result<(), String> {
        let clean_name = name.to_lowercase();
        if self.tables.contains_key(&clean_name) {
            return Err(format!("Table {} already exists", name));
        }
        self.wal.append(WalRecord::CreateTable {
            name: name.clone(),
            columns: columns.clone(),
        });
        self.tables.insert(clean_name.clone(), Table::new(name, columns));
        Ok(())
    }

    pub fn drop_table(&mut self, name: &str) -> bool {
        let clean_name = name.to_lowercase();
        self.wal.append(WalRecord::DropTable {
            name: name.to_string(),
        });
        self.tables.remove(&clean_name).is_some()
    }

    pub fn get_table(&self, name: &str) -> Option<&Table> {
        let clean_name = name.to_lowercase();
        self.tables.get(&clean_name)
    }

    pub fn get_table_mut(&mut self, name: &str) -> Option<&mut Table> {
        let clean_name = name.to_lowercase();
        self.tables.get_mut(&clean_name)
    }

    pub fn begin_transaction(&mut self) {
        self.in_transaction = true;
        self.tx_undo_log.clear();
        self.savepoints.clear();
        self.wal.append(WalRecord::Begin);
    }

    pub fn savepoint(&mut self, name: &str) {
        let clean = name.trim_matches('"').to_lowercase();
        self.savepoints.insert(clean, self.tx_undo_log.len());
    }

    pub fn rollback_to_savepoint(&mut self, name: &str) -> bool {
        let clean = name.trim_matches('"').to_lowercase();
        if let Some(&target_len) = self.savepoints.get(&clean) {
            while self.tx_undo_log.len() > target_len {
                if let Some(undo) = self.tx_undo_log.pop() {
                    self.apply_undo(undo);
                }
            }
            true
        } else {
            false
        }
    }

    pub fn release_savepoint(&mut self, name: &str) -> bool {
        let clean = name.trim_matches('"').to_lowercase();
        self.savepoints.remove(&clean).is_some()
    }

    pub fn apply_undo(&mut self, undo: UndoAction) {
        match undo {
            UndoAction::DeleteLastInsertedRow { table } => {
                if let Some(t) = self.get_table_mut(&table) {
                    if let Some(last_row) = t.rows.pop() {
                        t.is_deleted.pop();
                        t.active_count = t.active_count.saturating_sub(1);
                        if let Some(pk_idx) = t.pk_col_idx {
                            if let Some(pk_val) = last_row.get(pk_idx).and_then(|v| v.as_i64()) {
                                t.pk_index.remove(&pk_val);
                            }
                        }
                    }
                }
            }
            UndoAction::RestoreRow { table, row_idx, col_idx, old_val } => {
                if let Some(t) = self.get_table_mut(&table) {
                    if row_idx < t.rows.len() && col_idx < t.rows[row_idx].len() {
                        if Some(col_idx) == t.pk_col_idx {
                            if let Some(old_pk) = old_val.as_i64() {
                                if let Some(cur_pk) = t.rows[row_idx][col_idx].as_i64() {
                                    t.pk_index.remove(&cur_pk);
                                }
                                t.pk_index.insert(old_pk, row_idx);
                            }
                        }
                        t.rows[row_idx][col_idx] = old_val;
                    }
                }
            }
            UndoAction::UndeleteRow { table, row_idx } => {
                if let Some(t) = self.get_table_mut(&table) {
                    if row_idx < t.rows.len() && t.is_deleted[row_idx] {
                        t.is_deleted[row_idx] = false;
                        t.active_count += 1;
                        if let Some(pk_idx) = t.pk_col_idx {
                            if let Some(pk) = t.rows[row_idx].get(pk_idx).and_then(|v| v.as_i64()) {
                                t.pk_index.insert(pk, row_idx);
                            }
                        }
                    }
                }
            }
        }
    }

    pub fn commit(&mut self) {
        if self.in_transaction {
            self.wal.append(WalRecord::Commit);
            self.wal.flush();
            self.in_transaction = false;
            self.tx_undo_log.clear();
            self.savepoints.clear();
        }
    }

    pub fn rollback(&mut self) {
        if self.in_transaction {
            // Revert actions in reverse order
            while let Some(undo) = self.tx_undo_log.pop() {
                self.apply_undo(undo);
            }
            self.wal.append(WalRecord::Rollback);
            self.wal.flush();
            self.in_transaction = false;
            self.savepoints.clear();
        }
    }

    pub fn flush(&mut self) {
        self.wal.flush();
    }
}
