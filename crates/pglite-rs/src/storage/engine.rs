use crate::storage::table::Table;
use crate::storage::wal::{WalManager, WalRecord};
use crate::types::{value_to_join_key, ColumnDef, Value};
use std::collections::HashMap;

#[derive(Debug, Clone)]
pub enum UndoAction {
    DeleteInsertedRows { table: String, count: usize },
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
        let (wal, recovered_records) = WalManager::new(&filepath);
        let mut engine = Self {
            filepath,
            tables: HashMap::new(),
            in_transaction: false,
            tx_undo_log: Vec::new(),
            savepoints: HashMap::new(),
            wal,
        };

        let mut table_hist_cols: HashMap<String, Vec<String>> = HashMap::new();

        // Replay WAL records recovered via zero-copy mmap.
        // Giao dịch được mô phỏng bằng đúng cơ chế undo lúc chạy, nên trạng thái sau replay
        // trùng trạng thái trong bộ nhớ. Không có gì được ghi lại vào WAL trong lúc replay.
        for record in recovered_records {
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
                    engine.replay_insert(&table_hist_cols, &table, row);
                }
                WalRecord::Update { table, pk, col_idx, new_val } => {
                    let clean = table.to_lowercase();
                    let row_idx = engine.get_table(&clean).and_then(|t| t.pk_index.get(&pk).copied());
                    if let Some(row_idx) = row_idx {
                        engine.replay_set_cell(&table_hist_cols, &clean, row_idx, col_idx, new_val);
                    }
                }
                WalRecord::Delete { table, pk } => {
                    let clean = table.to_lowercase();
                    let row_idx = engine.get_table(&clean).and_then(|t| t.pk_index.get(&pk).copied());
                    if let Some(row_idx) = row_idx {
                        engine.replay_delete(&clean, row_idx);
                    }
                }
                WalRecord::UpdateByKey { table, key, col_idx, new_val } => {
                    let clean = table.to_lowercase();
                    let row_idx = engine.get_table(&clean).and_then(|t| t.get_row_idx_by_pk_key(&value_to_join_key(&key)));
                    if let Some(row_idx) = row_idx {
                        engine.replay_set_cell(&table_hist_cols, &clean, row_idx, col_idx, new_val);
                    }
                }
                WalRecord::DeleteByKey { table, key } => {
                    let clean = table.to_lowercase();
                    let row_idx = engine.get_table(&clean).and_then(|t| t.get_row_idx_by_pk_key(&value_to_join_key(&key)));
                    if let Some(row_idx) = row_idx {
                        engine.replay_delete(&clean, row_idx);
                    }
                }
                WalRecord::UpdateByRow { table, old_row, updates } => {
                    let clean = table.to_lowercase();
                    let row_idx = engine.get_table(&clean).and_then(|t| t.find_live_row(&old_row));
                    if let Some(row_idx) = row_idx {
                        for (col_idx, new_val) in updates {
                            engine.replay_set_cell(&table_hist_cols, &clean, row_idx, col_idx, new_val);
                        }
                    }
                }
                WalRecord::DeleteByRow { table, row } => {
                    let clean = table.to_lowercase();
                    let row_idx = engine.get_table(&clean).and_then(|t| t.find_live_row(&row));
                    if let Some(row_idx) = row_idx {
                        engine.replay_delete(&clean, row_idx);
                    }
                }
                WalRecord::Begin => {
                    engine.in_transaction = true;
                    engine.tx_undo_log.clear();
                    engine.savepoints.clear();
                }
                WalRecord::Commit => engine.end_replayed_transaction(),
                // ROLLBACK ghi bởi bản cũ: giữ đúng hành vi cũ (dữ liệu không bị hoàn tác),
                // để DB cũ mở ra đúng như trước.
                WalRecord::Rollback => engine.end_replayed_transaction(),
                WalRecord::TxRollback => {
                    while let Some(undo) = engine.tx_undo_log.pop() {
                        engine.apply_undo(undo);
                    }
                    engine.end_replayed_transaction();
                }
                WalRecord::Savepoint { name } => {
                    let clean = name.trim_matches('"').to_lowercase();
                    engine.savepoints.insert(clean, engine.tx_undo_log.len());
                }
                WalRecord::RollbackToSavepoint { name } => {
                    let clean = name.trim_matches('"').to_lowercase();
                    if let Some(&target_len) = engine.savepoints.get(&clean) {
                        while engine.tx_undo_log.len() > target_len {
                            if let Some(undo) = engine.tx_undo_log.pop() {
                                engine.apply_undo(undo);
                            }
                        }
                    }
                }
                WalRecord::ReleaseSavepoint { name } => {
                    engine.savepoints.remove(&name.trim_matches('"').to_lowercase());
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
            }
        }

        // Giao dịch còn dang dở ở cuối file (BEGIN không có COMMIT/ROLLBACK) được giữ
        // nguyên như bản cũ: db-rs dùng chung một engine nên một BEGIN bị bỏ quên khiến mọi
        // lệnh ghi sau đó nằm "trong giao dịch"; bỏ chúng đi sẽ làm mất dữ liệu thật.
        engine.end_replayed_transaction();

        engine
    }

    fn end_replayed_transaction(&mut self) {
        self.in_transaction = false;
        self.tx_undo_log.clear();
        self.savepoints.clear();
    }

    /// Chỉ số cột hiện tại cho một `col_idx` ghi trong WAL (bảng từng bị DROP COLUMN).
    fn replay_col_idx(hist: &HashMap<String, Vec<String>>, t: &Table, clean: &str, col_idx: usize) -> usize {
        match hist.get(clean) {
            Some(h_cols) if h_cols.len() > t.columns.len() && col_idx < h_cols.len() => {
                t.get_column_index(&h_cols[col_idx]).unwrap_or(col_idx)
            }
            _ => col_idx,
        }
    }

    fn replay_insert(&mut self, hist: &HashMap<String, Vec<String>>, table: &str, row: Vec<Value>) {
        let clean = table.to_lowercase();
        let in_tx = self.in_transaction;
        let Some(t) = self.tables.get_mut(&clean) else {
            return;
        };
        let mut row = row;
        if let Some(h_cols) = hist.get(&clean) {
            if row.len() > t.columns.len() && row.len() <= h_cols.len() {
                row = t
                    .columns
                    .iter()
                    .map(|col| {
                        h_cols
                            .iter()
                            .position(|h| h.eq_ignore_ascii_case(&col.name))
                            .and_then(|h_idx| row.get(h_idx).cloned())
                            .unwrap_or(Value::Null)
                    })
                    .collect();
            }
        }
        t.insert(row);
        if in_tx {
            self.tx_undo_log.push(UndoAction::DeleteInsertedRows { table: clean, count: 1 });
        }
    }

    fn replay_set_cell(&mut self, hist: &HashMap<String, Vec<String>>, clean: &str, row_idx: usize, col_idx: usize, val: Value) {
        let in_tx = self.in_transaction;
        let Some(t) = self.tables.get_mut(clean) else {
            return;
        };
        if row_idx >= t.rows.len() || t.is_deleted[row_idx] {
            return;
        }
        let target = Self::replay_col_idx(hist, t, clean, col_idx);
        if let Some(old_val) = t.set_cell(row_idx, target, val) {
            if in_tx {
                self.tx_undo_log.push(UndoAction::RestoreRow {
                    table: clean.to_string(),
                    row_idx,
                    col_idx: target,
                    old_val,
                });
            }
        }
    }

    fn replay_delete(&mut self, clean: &str, row_idx: usize) {
        let in_tx = self.in_transaction;
        if let Some(t) = self.tables.get_mut(clean) {
            if t.delete_row(row_idx) && in_tx {
                self.tx_undo_log.push(UndoAction::UndeleteRow { table: clean.to_string(), row_idx });
            }
        }
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
        self.savepoints.insert(clean.clone(), self.tx_undo_log.len());
        if self.in_transaction {
            self.wal.append(WalRecord::Savepoint { name: clean });
        }
    }

    pub fn rollback_to_savepoint(&mut self, name: &str) -> bool {
        let clean = name.trim_matches('"').to_lowercase();
        if let Some(&target_len) = self.savepoints.get(&clean) {
            while self.tx_undo_log.len() > target_len {
                if let Some(undo) = self.tx_undo_log.pop() {
                    self.apply_undo(undo);
                }
            }
            self.wal.append(WalRecord::RollbackToSavepoint { name: clean });
            true
        } else {
            false
        }
    }

    pub fn release_savepoint(&mut self, name: &str) -> bool {
        let clean = name.trim_matches('"').to_lowercase();
        let existed = self.savepoints.remove(&clean).is_some();
        if existed {
            self.wal.append(WalRecord::ReleaseSavepoint { name: clean });
        }
        existed
    }

    pub fn apply_undo(&mut self, undo: UndoAction) {
        match undo {
            UndoAction::DeleteInsertedRows { table, count } => {
                if let Some(t) = self.get_table_mut(&table) {
                    for _ in 0..count {
                        t.pop_last_row();
                    }
                }
            }
            UndoAction::DeleteLastInsertedRow { table } => {
                if let Some(t) = self.get_table_mut(&table) {
                    t.pop_last_row();
                }
            }
            UndoAction::RestoreRow { table, row_idx, col_idx, old_val } => {
                if let Some(t) = self.get_table_mut(&table) {
                    t.set_cell(row_idx, col_idx, old_val);
                }
            }
            UndoAction::UndeleteRow { table, row_idx } => {
                if let Some(t) = self.get_table_mut(&table) {
                    if row_idx < t.rows.len() && t.is_deleted[row_idx] {
                        t.is_deleted[row_idx] = false;
                        t.active_count += 1;
                        if let Some(pk_idx) = t.pk_col_idx {
                            if let Some(pk) = t.rows[row_idx].get(pk_idx).cloned() {
                                t.add_pk_to_index(&pk, row_idx);
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
            self.tx_undo_log.shrink_to_fit();
            self.savepoints.clear();
        }
    }

    pub fn rollback(&mut self) {
        if self.in_transaction {
            // Revert actions in reverse order
            while let Some(undo) = self.tx_undo_log.pop() {
                self.apply_undo(undo);
            }
            // TxRollback (không phải Rollback cũ) để lần mở lại sau hoàn tác đúng giao dịch này
            self.wal.append(WalRecord::TxRollback);
            self.wal.flush();
            self.in_transaction = false;
            self.tx_undo_log.shrink_to_fit();
            self.savepoints.clear();
        }
    }

    pub fn flush(&mut self) {
        self.wal.flush();
    }
}
