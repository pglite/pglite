use crate::storage::table::Table;
use crate::storage::wal::{WalManager, WalRecord};
use crate::types::{ColumnDef, Value};
use std::collections::HashMap;

#[derive(Debug, Clone)]
pub enum UndoAction {
    DeleteLastInsertedRow { table: String },
    RestoreRow { table: String, pk: i64, col_idx: usize, old_val: Value },
    UndeleteRow { table: String, pk: i64 },
}

pub struct StorageEngine {
    pub filepath: String,
    pub tables: HashMap<String, Table>,
    pub in_transaction: bool,
    pub tx_undo_log: Vec<UndoAction>,
    pub wal: WalManager,
}

impl StorageEngine {
    pub fn new(filepath: String) -> Self {
        let wal = WalManager::new(&filepath);
        Self {
            filepath,
            tables: HashMap::new(),
            in_transaction: false,
            tx_undo_log: Vec::new(),
            wal,
        }
    }

    pub fn create_table(&mut self, name: String, columns: Vec<ColumnDef>) -> Result<(), String> {
        let clean_name = name.to_lowercase();
        if self.tables.contains_key(&clean_name) {
            return Err(format!("Table {} already exists", name));
        }
        self.tables.insert(clean_name.clone(), Table::new(name, columns));
        Ok(())
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
        self.wal.append(WalRecord::Begin);
    }

    pub fn commit(&mut self) {
        if self.in_transaction {
            self.wal.append(WalRecord::Commit);
            self.wal.flush();
            self.in_transaction = false;
            self.tx_undo_log.clear();
        }
    }

    pub fn rollback(&mut self) {
        if self.in_transaction {
            // Revert actions in reverse order
            while let Some(undo) = self.tx_undo_log.pop() {
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
                    UndoAction::RestoreRow { table, pk, col_idx, old_val } => {
                        if let Some(t) = self.get_table_mut(&table) {
                            t.update_by_pk(pk, col_idx, old_val);
                        }
                    }
                    UndoAction::UndeleteRow { table, pk } => {
                        if let Some(t) = self.get_table_mut(&table) {
                            if let Some(&row_idx) = t.pk_index.get(&pk) {
                                if t.is_deleted[row_idx] {
                                    t.is_deleted[row_idx] = false;
                                    t.active_count += 1;
                                }
                            }
                        }
                    }
                }
            }
            self.wal.append(WalRecord::Rollback);
            self.wal.flush();
            self.in_transaction = false;
        }
    }

    pub fn flush(&mut self) {
        self.wal.flush();
    }
}
