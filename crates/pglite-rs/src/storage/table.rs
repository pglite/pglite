use crate::storage::page::SlottedPage;
use crate::types::{ColumnDef, JoinKey, Value, value_to_join_key};
use compact_str::CompactString;
use std::collections::{BTreeMap, HashMap};

#[derive(Debug, Clone)]
pub struct Table {
    pub name: String,
    pub comment: Option<String>,
    pub columns: Vec<ColumnDef>,
    pub pk_col_idx: Option<usize>,
    pub auto_increment: i64,
    pub rows: Vec<Vec<Value>>,
    pub pk_index: BTreeMap<i64, usize>,
    pub pk_index_str: BTreeMap<CompactString, usize>,
    pub col_indexes: HashMap<usize, HashMap<JoinKey, Vec<usize>>>,
    pub is_deleted: Vec<bool>,
    pub active_count: usize,
    pub pages: Vec<SlottedPage>,
    /// Đúng một cột được khai báo PRIMARY KEY: INSERT/UPDATE trùng khoá bị từ chối. Cột `id`,
    /// `_id`, SERIAL được tự đoán làm khoá thì không bị ép (giữ hành vi cũ). Chỉ kiểm tra lúc
    /// thực thi, không bao giờ lúc replay, nên DB cũ có sẵn dòng trùng vẫn mở được.
    pub pk_enforced: bool,
    /// Đã từng có hai dòng sống cùng khoá (DB cũ hoặc khoá tự đoán): khi gỡ khoá khỏi index phải
    /// tìm dòng còn lại để trỏ vào.
    pub has_dup_pk: bool,
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

        let pk_count = columns.iter().filter(|c| c.is_primary_key).count();
        let pk_enforced = pk_count == 1;
        let pk_col_idx = if pk_count == 1 {
            columns.iter().position(|c| c.is_primary_key)
        } else if pk_count > 1 {
            None
        } else {
            columns.iter().position(|c| c.data_type == crate::types::DataType::Serial)
                .or_else(|| columns.iter().position(|c| c.name.eq_ignore_ascii_case("id")))
                .or_else(|| columns.iter().position(|c| c.name.eq_ignore_ascii_case("_id")))
        };

        // Ensure the detected PK column is marked as primary key
        if let Some(idx) = pk_col_idx {
            columns[idx].is_primary_key = true;
        }

        let mut col_indexes = HashMap::new();
        for (i, c) in columns.iter().enumerate() {
            let col_name_lower = c.name.to_lowercase();
            if col_name_lower.ends_with("_id") || (col_name_lower.ends_with("id") && col_name_lower.len() > 2) {
                col_indexes.insert(i, HashMap::new());
            }
        }

        Self {
            name,
            comment: None,
            columns,
            pk_col_idx,
            auto_increment: 1,
            rows: Vec::new(),
            pk_index: BTreeMap::new(),
            pk_index_str: BTreeMap::new(),
            col_indexes,
            is_deleted: Vec::new(),
            active_count: 0,
            pages: Vec::new(),
            pk_enforced,
            has_dup_pk: false,
        }
    }

    /// Cột cho bản ghi `CreateTable` (TRUNCATE): khoá tự đoán không được ghi thành khoá khai báo.
    pub fn declared_columns(&self) -> Vec<ColumnDef> {
        let mut cols = self.columns.clone();
        if !self.pk_enforced {
            if let Some(i) = self.pk_col_idx {
                cols[i].is_primary_key = false;
            }
        }
        cols
    }

    /// Dòng sống (khác `except`) đang giữ khoá `val`, nếu có.
    pub fn live_row_with_pk(&self, val: &Value, except: Option<usize>) -> Option<usize> {
        let idx = match val {
            Value::Text(s) => self.get_row_idx_by_pk_str(s),
            v => v.as_i64().and_then(|i| self.pk_index.get(&i).copied()).filter(|&r| !self.is_deleted.get(r).copied().unwrap_or(true)),
        };
        match idx {
            Some(r) if Some(r) != except => Some(r),
            _ if self.has_dup_pk => {
                let pk_idx = self.pk_col_idx?;
                (0..self.rows.len()).find(|&r| Some(r) != except && !self.is_deleted[r] && pk_eq(&self.rows[r][pk_idx], val))
            }
            _ => None,
        }
    }

    /// Dựng lại mọi index từ các dòng còn sống (sau DROP COLUMN, TRUNCATE).
    pub fn rebuild_indexes(&mut self) {
        self.pk_index.clear();
        self.pk_index_str.clear();
        self.has_dup_pk = false;
        if let Some(pk_idx) = self.pk_col_idx {
            for r in 0..self.rows.len() {
                if !self.is_deleted[r] {
                    if let Some(v) = self.rows[r].get(pk_idx).cloned() {
                        self.add_pk_to_index(&v, r);
                    }
                }
            }
        }
        let cols: Vec<usize> = self.col_indexes.keys().copied().collect();
        for c in cols {
            self.add_secondary_index(c);
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
        // Dòng phải có mặt trước khi index để add_pk_to_index nhận ra khoá trùng.
        let pk_val = self.pk_col_idx.and_then(|i| row.get(i).cloned());

        for (&col_idx, map) in &mut self.col_indexes {
            if let Some(val) = row.get(col_idx) {
                if !val.is_null() {
                    map.entry(value_to_join_key(val)).or_default().push(row_idx);
                }
            }
        }

        self.rows.push(row);
        self.is_deleted.push(false);
        self.active_count += 1;
        if let Some(v) = pk_val {
            self.add_pk_to_index(&v, row_idx);
        }

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

    pub fn add_secondary_index(&mut self, col_idx: usize) {
        if col_idx < self.columns.len() {
            let mut map: HashMap<JoinKey, Vec<usize>> = HashMap::new();
            for (r_idx, r) in self.rows.iter().enumerate() {
                if !self.is_deleted[r_idx] {
                    if let Some(v) = r.get(col_idx) {
                        if !v.is_null() {
                            map.entry(value_to_join_key(v)).or_default().push(r_idx);
                        }
                    }
                }
            }
            self.col_indexes.insert(col_idx, map);
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

    pub fn get_row_idx_by_pk_key(&self, key: &JoinKey) -> Option<usize> {
        match key {
            JoinKey::Int(pk_val) => {
                if let Some(&row_idx) = self.pk_index.get(pk_val) {
                    if !self.is_deleted[row_idx] {
                        return Some(row_idx);
                    }
                }
                let s = CompactString::new(pk_val.to_string());
                if let Some(&row_idx) = self.pk_index_str.get(&s) {
                    if !self.is_deleted[row_idx] {
                        return Some(row_idx);
                    }
                }
                None
            }
            JoinKey::Text(s) => {
                if let Some(&row_idx) = self.pk_index_str.get(s) {
                    if !self.is_deleted[row_idx] {
                        return Some(row_idx);
                    }
                }
                if s.len() == 36 {
                    let lower = CompactString::new(s.to_lowercase());
                    if let Some(&row_idx) = self.pk_index_str.get(&lower) {
                        if !self.is_deleted[row_idx] {
                            return Some(row_idx);
                        }
                    }
                }
                if let Ok(num) = s.parse::<i64>() {
                    if let Some(&row_idx) = self.pk_index.get(&num) {
                        if !self.is_deleted[row_idx] {
                            return Some(row_idx);
                        }
                    }
                }
                None
            }
            _ => None,
        }
    }

    pub fn get_row_idx_by_pk_str(&self, pk: &str) -> Option<usize> {
        let key = CompactString::new(pk);
        if let Some(&row_idx) = self.pk_index_str.get(&key) {
            if !self.is_deleted[row_idx] {
                return Some(row_idx);
            }
        }
        if pk.len() == 36 {
            let lower = CompactString::new(pk.to_lowercase());
            if let Some(&row_idx) = self.pk_index_str.get(&lower) {
                if !self.is_deleted[row_idx] {
                    return Some(row_idx);
                }
            }
        }
        if let Ok(num) = pk.parse::<i64>() {
            if let Some(&row_idx) = self.pk_index.get(&num) {
                if !self.is_deleted[row_idx] {
                    return Some(row_idx);
                }
            }
        }
        None
    }

    pub fn get_by_pk_str(&self, pk: &str) -> Option<&Vec<Value>> {
        self.get_row_idx_by_pk_str(pk).map(|idx| &self.rows[idx])
    }

    pub fn get_by_pk_val(&self, val: &Value) -> Option<&Vec<Value>> {
        match val {
            Value::Int(i) => self.get_by_pk(*i),
            Value::Text(s) => self.get_by_pk_str(s),
            _ => None,
        }
    }

    pub fn has_pk_val(&self, val: &Value) -> bool {
        match val {
            Value::Int(i) => {
                if let Some(&row_idx) = self.pk_index.get(i) {
                    !self.is_deleted[row_idx]
                } else {
                    false
                }
            }
            Value::Text(s) => self.get_row_idx_by_pk_str(s).is_some(),
            _ => false,
        }
    }

    pub fn remove_pk_from_index(&mut self, val: &Value) {
        if let Some(i) = val.as_i64() {
            self.pk_index.remove(&i);
        }
        if let Value::Text(s) = val {
            self.pk_index_str.remove(s);
            if s.len() == 36 {
                self.pk_index_str.remove(&CompactString::new(s.to_lowercase()));
            }
        }
    }

    pub fn add_pk_to_index(&mut self, val: &Value, row_idx: usize) {
        let mut replaced = Vec::new();
        if let Some(i) = val.as_i64() {
            replaced.extend(self.pk_index.insert(i, row_idx));
        }
        if let Value::Text(s) = val {
            replaced.extend(self.pk_index_str.insert(s.clone(), row_idx));
            if s.len() == 36 {
                replaced.extend(self.pk_index_str.insert(CompactString::new(s.to_lowercase()), row_idx));
            }
        }
        if replaced.iter().any(|&r| r != row_idx && !self.is_deleted.get(r).copied().unwrap_or(true)) {
            self.has_dup_pk = true;
        }
    }

    /// Gỡ khoá của dòng `row_idx` khỏi index (chỉ khi index đang trỏ tới đúng dòng này), rồi trỏ
    /// lại vào dòng sống khác cùng khoá nếu có.
    fn unindex_pk(&mut self, val: &Value, row_idx: usize) {
        if let Some(i) = val.as_i64() {
            if self.pk_index.get(&i) == Some(&row_idx) {
                self.pk_index.remove(&i);
            }
        }
        if let Value::Text(s) = val {
            if self.pk_index_str.get(s) == Some(&row_idx) {
                self.pk_index_str.remove(s);
            }
            if s.len() == 36 {
                let lower = CompactString::new(s.to_lowercase());
                if self.pk_index_str.get(&lower) == Some(&row_idx) {
                    self.pk_index_str.remove(&lower);
                }
            }
        }
        if self.has_dup_pk {
            if let Some(pk_idx) = self.pk_col_idx {
                if let Some(other) = (0..self.rows.len())
                    .find(|&r| r != row_idx && !self.is_deleted[r] && pk_eq(&self.rows[r][pk_idx], val))
                {
                    self.add_pk_to_index(val, other);
                }
            }
        }
    }

    /// Hoàn tác DELETE của một dòng.
    pub fn undelete_row(&mut self, row_idx: usize) {
        if row_idx < self.rows.len() && self.is_deleted[row_idx] {
            self.is_deleted[row_idx] = false;
            self.active_count += 1;
            if let Some(pk_idx) = self.pk_col_idx {
                if let Some(pk) = self.rows[row_idx].get(pk_idx).cloned() {
                    self.add_pk_to_index(&pk, row_idx);
                }
            }
        }
    }

    /// Đặt giá trị một ô và giữ khoá chính + index phụ đồng bộ. Trả về giá trị cũ.
    pub fn set_cell(&mut self, row_idx: usize, col_idx: usize, val: Value) -> Option<Value> {
        if row_idx >= self.rows.len() || col_idx >= self.rows[row_idx].len() {
            return None;
        }
        let new_val = val.clone();
        let old_val = std::mem::replace(&mut self.rows[row_idx][col_idx], val);
        if Some(col_idx) == self.pk_col_idx && !self.is_deleted[row_idx] {
            self.unindex_pk(&old_val, row_idx);
            self.add_pk_to_index(&new_val, row_idx);
        }
        if let Some(map) = self.col_indexes.get_mut(&col_idx) {
            if !old_val.is_null() {
                let old_key = value_to_join_key(&old_val);
                if let Some(list) = map.get_mut(&old_key) {
                    list.retain(|&i| i != row_idx);
                    if list.is_empty() {
                        map.remove(&old_key);
                    }
                }
            }
            if !new_val.is_null() {
                map.entry(value_to_join_key(&new_val)).or_default().push(row_idx);
            }
        }
        Some(old_val)
    }

    /// Gỡ dòng chèn cuối cùng (hoàn tác INSERT) khỏi bảng và mọi index.
    pub fn pop_last_row(&mut self) -> Option<Vec<Value>> {
        let row_idx = self.rows.len().checked_sub(1)?;
        let row = self.rows.pop()?;
        let was_deleted = self.is_deleted.pop().unwrap_or(false);
        if !was_deleted {
            self.active_count = self.active_count.saturating_sub(1);
        }
        if let Some(pk_idx) = self.pk_col_idx {
            if let Some(pk_val) = row.get(pk_idx) {
                self.unindex_pk(pk_val, row_idx);
            }
        }
        for (&col_idx, map) in &mut self.col_indexes {
            if let Some(v) = row.get(col_idx) {
                if !v.is_null() {
                    let key = value_to_join_key(v);
                    if let Some(list) = map.get_mut(&key) {
                        list.retain(|&i| i != row_idx);
                        if list.is_empty() {
                            map.remove(&key);
                        }
                    }
                }
            }
        }
        Some(row)
    }

    /// Dòng còn sống đầu tiên có giá trị đúng bằng `values`. Thử trước dòng mà index khoá chính
    /// trỏ tới (tránh quét cả bảng khi replay UPDATE đổi khoá).
    pub fn find_live_row(&self, values: &[Value]) -> Option<usize> {
        if let Some(hint) = self.pk_col_idx.and_then(|i| values.get(i)).and_then(|v| self.live_row_with_pk(v, None)) {
            if self.rows[hint].as_slice() == values {
                let first = (0..hint).find(|&i| !self.is_deleted[i] && self.rows[i].as_slice() == values);
                return first.or(Some(hint));
            }
        }
        (0..self.rows.len()).find(|&i| !self.is_deleted[i] && self.rows[i].as_slice() == values)
    }

    pub fn update_by_pk(&mut self, pk: i64, col_idx: usize, val: Value) -> bool {
        if let Some(&row_idx) = self.pk_index.get(&pk) {
            if !self.is_deleted[row_idx] && col_idx < self.rows[row_idx].len() {
                self.set_cell(row_idx, col_idx, val);
                return true;
            }
        }
        false
    }

    pub fn update_row_multi(&mut self, row_idx: usize, updates: &[(usize, Value)]) {
        if row_idx < self.rows.len() && !self.is_deleted[row_idx] {
            for (col_idx, val) in updates {
                self.set_cell(row_idx, *col_idx, val.clone());
            }
        }
    }

    pub fn delete_row(&mut self, row_idx: usize) -> bool {
        if row_idx < self.rows.len() && !self.is_deleted[row_idx] {
            self.is_deleted[row_idx] = true;
            self.active_count = self.active_count.saturating_sub(1);
            if let Some(pk_idx) = self.pk_col_idx {
                if let Some(val) = self.rows[row_idx].get(pk_idx).cloned() {
                    self.unindex_pk(&val, row_idx);
                }
            }
            return true;
        }
        false
    }

    pub fn delete_by_pk(&mut self, pk: i64) -> bool {
        if let Some(&row_idx) = self.pk_index.get(&pk) {
            return self.delete_row(row_idx);
        }
        false
    }

    pub fn delete_by_pk_str(&mut self, pk: &str) -> bool {
        if let Some(row_idx) = self.get_row_idx_by_pk_str(pk) {
            return self.delete_row(row_idx);
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
            if Some(pos) == self.pk_col_idx {
                self.pk_enforced = false;
            }
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
            // Index phụ được đánh theo vị trí cột: dời các cột sau cột bị xoá.
            self.col_indexes = std::mem::take(&mut self.col_indexes)
                .into_iter()
                .filter(|(c, _)| *c != pos)
                .map(|(c, m)| (if c > pos { c - 1 } else { c }, m))
                .collect();
            self.rebuild_indexes();
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

/// Hai giá trị khoá chính có trỏ cùng một khoá trong index không.
fn pk_eq(a: &Value, b: &Value) -> bool {
    match (a, b) {
        (Value::Text(x), Value::Text(y)) => x == y || (x.len() == 36 && x.eq_ignore_ascii_case(y)),
        (Value::Text(_), _) | (_, Value::Text(_)) => false,
        _ => a.as_i64().is_some() && a.as_i64() == b.as_i64(),
    }
}
