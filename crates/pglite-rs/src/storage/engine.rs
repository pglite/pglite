use crate::storage::catalog::{decode_schema_op, encode_schema_op, ConstraintDef, ConstraintKind, SchemaOp, SCHEMA_OP_VERSION};
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
    /// Số câu lệnh đang chạy (lồng nhau khi một chuỗi có nhiều lệnh). Trong lúc đó undo log luôn
    /// được ghi, kể cả ngoài giao dịch, để lệnh lỗi giữa chừng không để lại thay đổi dở.
    pub stmt_depth: u32,
}

/// Mốc đầu một câu lệnh (xem `StorageEngine::begin_statement`).
pub struct StmtMark {
    undo_len: usize,
    wal_pos: u64,
    ddl_appended: u64,
    in_transaction: bool,
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
            stmt_depth: 0,
        };

        let mut table_hist_cols: HashMap<String, Vec<String>> = HashMap::new();
        // Giao dịch đang mở trong lúc replay được mở bằng `TxBegin` (bản mới) hay `Begin` (bản cũ).
        let mut open_tx_is_new = false;

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
                    open_tx_is_new = false;
                }
                WalRecord::TxBegin => {
                    engine.in_transaction = true;
                    engine.tx_undo_log.clear();
                    engine.savepoints.clear();
                    open_tx_is_new = true;
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
                WalRecord::Schema { version, payload } => match decode_schema_op(version, &payload) {
                    Some(op) => {
                        match &op {
                            SchemaOp::RenameColumn { table, from, to } => {
                                if let Some(h_cols) = table_hist_cols.get_mut(&table.to_lowercase()) {
                                    for h in h_cols.iter_mut().filter(|h| h.eq_ignore_ascii_case(from)) {
                                        *h = to.clone();
                                    }
                                }
                            }
                            SchemaOp::RenameTable { from, to } => {
                                if let Some(h_cols) = table_hist_cols.remove(&from.to_lowercase()) {
                                    table_hist_cols.insert(to.to_lowercase(), h_cols);
                                }
                            }
                            _ => {}
                        }
                        engine.apply_schema_op(&op);
                    }
                    None => eprintln!(
                        "[pglite-rs] WARNING: skipped a schema change written by a newer version (format {})",
                        version
                    ),
                },
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

        // Giao dịch còn dang dở ở cuối file (BEGIN không có COMMIT/ROLLBACK):
        // - mở bằng `Begin` (file do bản cũ ghi): giữ nguyên như bản cũ đọc, vì bản cũ không
        //   phân biệt được BEGIN bị bỏ quên với crash giữa giao dịch;
        // - mở bằng `TxBegin`: chưa từng COMMIT nên hoàn tác, như PostgreSQL sau crash.
        // Đóng giao dịch đó lại trong file, nếu không các lệnh ghi sau này sẽ bị replay như thể
        // vẫn nằm trong nó. Bản ghi đóng được chọn để mọi bản (cũ lẫn mới) đọc ra cùng kết quả.
        if engine.in_transaction {
            if open_tx_is_new {
                if !engine.tx_undo_log.is_empty() {
                    eprintln!(
                        "[pglite-rs] WARNING: uncommitted transaction at the end of the WAL for {} was rolled back",
                        engine.filepath
                    );
                }
                while let Some(undo) = engine.tx_undo_log.pop() {
                    engine.apply_undo(undo);
                }
                engine.wal.append(WalRecord::TxRollback);
            } else {
                engine.wal.append(WalRecord::Commit);
            }
            let _ = engine.wal.commit_point();
        }
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
        self.create_table_with_catalog(name, columns, Vec::new())
    }

    /// CREATE TABLE kèm catalog. Bảng mới luôn có bản ghi catalog (dù rỗng) ngay sau `CreateTable`:
    /// đánh dấu bảng "được theo dõi" để không bị nhận lại ràng buộc như bảng do bản cũ tạo.
    pub fn create_table_with_catalog(&mut self, name: String, columns: Vec<ColumnDef>, constraints: Vec<ConstraintDef>) -> Result<(), String> {
        let clean_name = name.to_lowercase();
        if self.tables.contains_key(&clean_name) {
            return Err(format!("relation \"{}\" already exists", name));
        }
        self.wal.append(WalRecord::CreateTable {
            name: name.clone(),
            columns: columns.clone(),
        });
        self.tables.insert(clean_name.clone(), Table::new(name.clone(), columns));
        self.log_schema_op(SchemaOp::TableCatalog { table: name, constraints, indexes: Vec::new(), legacy: false });
        Ok(())
    }

    /// Ghi một thay đổi catalog vào WAL rồi áp dụng nó (đúng như lúc replay).
    pub fn log_schema_op(&mut self, op: SchemaOp) {
        self.wal.append(WalRecord::Schema { version: SCHEMA_OP_VERSION, payload: encode_schema_op(&op) });
        self.apply_schema_op(&op);
    }

    /// Áp dụng một thay đổi catalog, dùng chung cho lúc chạy và lúc replay. Không kiểm tra dữ liệu:
    /// việc đó (nếu cần) do executor làm TRƯỚC khi ghi op.
    pub fn apply_schema_op(&mut self, op: &SchemaOp) {
        match op {
            SchemaOp::TableCatalog { table, constraints, indexes, legacy } => {
                if let Some(t) = self.get_table_mut(table) {
                    t.constraints = constraints.clone();
                    t.indexes = indexes.clone();
                    t.catalog_tracked = true;
                    t.legacy_data = *legacy;
                    Self::index_catalog_columns(t);
                    t.rebuild_unique_indexes();
                }
            }
            SchemaOp::AddConstraint { table, def } => {
                if let Some(t) = self.get_table_mut(table) {
                    t.constraints.retain(|c| !c.name.eq_ignore_ascii_case(&def.name));
                    t.constraints.push(def.clone());
                    if !t.catalog_tracked {
                        // Bảng do bản cũ tạo: dữ liệu cũ có thể vi phạm các ràng buộc khác đã khai báo.
                        t.legacy_data = true;
                    }
                    t.catalog_tracked = true;
                    Self::index_catalog_columns(t);
                    if matches!(def.kind, ConstraintKind::PrimaryKey { .. } | ConstraintKind::Unique { .. }) {
                        t.rebuild_unique_indexes();
                    }
                }
            }
            SchemaOp::DropConstraint { table, name } => {
                if let Some(t) = self.get_table_mut(table) {
                    t.constraints.retain(|c| !c.name.eq_ignore_ascii_case(name));
                    t.rebuild_unique_indexes();
                }
            }
            SchemaOp::CreateIndex { table, def } => {
                if let Some(t) = self.get_table_mut(table) {
                    t.indexes.retain(|ix| !ix.name.eq_ignore_ascii_case(&def.name));
                    t.indexes.push(def.clone());
                    Self::index_catalog_columns(t);
                    if def.unique {
                        t.rebuild_unique_indexes();
                    }
                }
            }
            SchemaOp::DropIndex { table, name } => {
                if let Some(t) = self.get_table_mut(table) {
                    t.indexes.retain(|ix| !ix.name.eq_ignore_ascii_case(name));
                    t.rebuild_unique_indexes();
                }
            }
            SchemaOp::AlterColumn { table, column, def } => {
                if let Some(t) = self.get_table_mut(table) {
                    if let Some(idx) = t.get_column_index(column) {
                        let keep_pk = t.columns[idx].is_primary_key;
                        let keep_name = t.columns[idx].name.clone();
                        let keep_comment = t.columns[idx].comment.clone();
                        t.columns[idx] = ColumnDef { name: keep_name, is_primary_key: keep_pk, comment: keep_comment, ..def.clone() };
                        t.rebuild_unique_indexes();
                    }
                }
            }
            SchemaOp::RenameColumn { table, from, to } => {
                let clean = table.to_lowercase();
                if let Some(t) = self.tables.get_mut(&clean) {
                    t.rename_column(from, to);
                }
                for t in self.tables.values_mut() {
                    for c in &mut t.constraints {
                        if let ConstraintKind::ForeignKey { ref_table, ref_cols, .. } = &mut c.kind {
                            if ref_table.eq_ignore_ascii_case(&clean) {
                                for rc in ref_cols.iter_mut().filter(|rc| rc.eq_ignore_ascii_case(from)) {
                                    *rc = to.clone();
                                }
                            }
                        }
                    }
                }
            }
            SchemaOp::RenameTable { from, to } => {
                if let Some(mut t) = self.tables.remove(&from.to_lowercase()) {
                    t.name = to.clone();
                    self.tables.insert(to.to_lowercase(), t);
                    for t in self.tables.values_mut() {
                        for c in &mut t.constraints {
                            if let ConstraintKind::ForeignKey { ref_table, .. } = &mut c.kind {
                                if ref_table.eq_ignore_ascii_case(from) {
                                    *ref_table = to.clone();
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    /// Cột một mình làm khoá ngoại hoặc làm index: có index phụ để tra nhanh (FK cascade, WHERE).
    fn index_catalog_columns(t: &mut Table) {
        let mut cols: Vec<usize> = Vec::new();
        for c in &t.constraints {
            if let ConstraintKind::ForeignKey { cols: fk, .. } = &c.kind {
                if fk.len() == 1 {
                    cols.extend(t.get_column_index(&fk[0]));
                }
            }
        }
        for ix in &t.indexes {
            if ix.cols.len() == 1 && ix.predicate.is_none() {
                cols.extend(t.get_column_index(&ix.cols[0]));
            }
        }
        for c in cols {
            if !t.col_indexes.contains_key(&c) {
                t.add_secondary_index(c);
            }
        }
    }

    /// Có ghi undo log không: trong giao dịch, hoặc trong lúc một câu lệnh đang chạy.
    pub fn undo_enabled(&self) -> bool {
        self.in_transaction || self.stmt_depth > 0
    }

    /// Bắt đầu một câu lệnh: từ đây mọi thay đổi đều được ghi undo, để nếu lệnh lỗi thì
    /// `end_statement` đưa bộ nhớ và WAL về đúng mốc này (lệnh lỗi không để lại gì, như PostgreSQL).
    pub fn begin_statement(&mut self) -> StmtMark {
        self.stmt_depth += 1;
        StmtMark {
            undo_len: self.tx_undo_log.len(),
            wal_pos: self.wal.position(),
            ddl_appended: self.wal.ddl_appended,
            in_transaction: self.in_transaction,
        }
    }

    pub fn end_statement(&mut self, mark: StmtMark, ok: bool) {
        self.stmt_depth = self.stmt_depth.saturating_sub(1);
        // Không hoàn tác khi lệnh đã đổi schema (DDL không có undo trong bộ nhớ), khi lệnh đã mở hay
        // đóng giao dịch, hoặc khi phần WAL cần bỏ đã được chốt: bộ nhớ và đĩa luôn phải khớp nhau.
        if !ok
            && self.wal.ddl_appended == mark.ddl_appended
            && self.in_transaction == mark.in_transaction
            && self.tx_undo_log.len() >= mark.undo_len
            && self.wal.truncate_to(mark.wal_pos)
        {
            while self.tx_undo_log.len() > mark.undo_len {
                if let Some(undo) = self.tx_undo_log.pop() {
                    self.apply_undo(undo);
                }
            }
        }
        if !self.in_transaction && self.stmt_depth == 0 {
            self.tx_undo_log.clear();
        }
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
        if self.in_transaction {
            eprintln!("[pglite-rs] WARNING: there is already a transaction in progress");
        }
        self.in_transaction = true;
        self.tx_undo_log.clear();
        self.savepoints.clear();
        // `Begin` cho binary cũ (chúng bỏ qua `TxBegin`), rồi `TxBegin` để bản này biết giao dịch
        // được mở từ bản mới: nếu không được đóng thì hoàn tác khi replay.
        self.wal.append(WalRecord::Begin);
        self.wal.append(WalRecord::TxBegin);
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
                    t.undelete_row(row_idx);
                }
            }
        }
    }

    pub fn commit(&mut self) -> Result<(), String> {
        if self.in_transaction {
            self.wal.append(WalRecord::Commit);
            self.in_transaction = false;
            self.tx_undo_log.clear();
            self.tx_undo_log.shrink_to_fit();
            self.savepoints.clear();
            return self.wal.commit_point();
        }
        Ok(())
    }

    pub fn rollback(&mut self) -> Result<(), String> {
        if self.in_transaction {
            // Revert actions in reverse order
            while let Some(undo) = self.tx_undo_log.pop() {
                self.apply_undo(undo);
            }
            // TxRollback (không phải Rollback cũ) để lần mở lại sau hoàn tác đúng giao dịch này
            self.wal.append(WalRecord::TxRollback);
            self.in_transaction = false;
            self.tx_undo_log.shrink_to_fit();
            self.savepoints.clear();
            return self.wal.commit_point();
        }
        Ok(())
    }

    /// Ghi và fsync mọi thứ đang chờ. Giao dịch còn mở thì vẫn chưa được chốt.
    pub fn flush(&mut self) -> Result<(), String> {
        self.wal.commit_point()
    }
}

impl Drop for StorageEngine {
    fn drop(&mut self) {
        if self.in_transaction && self.wal.filepath.is_some() {
            eprintln!(
                "[pglite-rs] WARNING: closing {} with an open transaction; it was not committed and will be rolled back on next open",
                self.filepath
            );
        }
    }
}
