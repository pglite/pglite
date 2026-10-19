//! Catalog ràng buộc và index được lưu bền vững (UNIQUE, PRIMARY KEY ghép, CHECK, FOREIGN KEY,
//! CREATE INDEX). Trước bản này chúng chỉ nằm trong RAM và mất khi mở lại DB.
//!
//! Mọi thay đổi catalog đi qua một `SchemaOp`, ghi vào WAL dưới dạng `WalRecord::Schema { version,
//! payload }` với `payload` là bincode của `SchemaOp`. Khung ngoài luôn giải mã được (payload là byte
//! thô), nên bản sau có thêm biến thể `SchemaOp` mới thì bản này chỉ bỏ qua op đó, không coi WAL là
//! hỏng. Binary cũ không biết `Schema` thì bỏ qua cả khung (cơ chế biến thể tương lai).
//!
//! Ràng buộc KHÔNG BAO GIỜ được kiểm tra lúc replay: DB cũ có dữ liệu vi phạm vẫn mở y nguyên.
use crate::types::{ColumnDef, Value};
use compact_str::CompactString;
use serde::{Deserialize, Serialize};

/// Phiên bản định dạng `payload` của `WalRecord::Schema`.
pub const SCHEMA_OP_VERSION: u8 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum FkAction {
    NoAction,
    Restrict,
    Cascade,
    SetNull,
    SetDefault,
}

impl FkAction {
    pub fn parse(s: &str) -> Option<Self> {
        let up = s.split_whitespace().collect::<Vec<_>>().join(" ").to_uppercase();
        Some(match up.as_str() {
            "NO ACTION" => FkAction::NoAction,
            "RESTRICT" => FkAction::Restrict,
            "CASCADE" => FkAction::Cascade,
            "SET NULL" => FkAction::SetNull,
            "SET DEFAULT" => FkAction::SetDefault,
            _ => return None,
        })
    }

    pub fn as_sql(&self) -> &'static str {
        match self {
            FkAction::NoAction => "NO ACTION",
            FkAction::Restrict => "RESTRICT",
            FkAction::Cascade => "CASCADE",
            FkAction::SetNull => "SET NULL",
            FkAction::SetDefault => "SET DEFAULT",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ConstraintKind {
    PrimaryKey { cols: Vec<String> },
    Unique { cols: Vec<String> },
    Check { expr: String },
    ForeignKey {
        cols: Vec<String>,
        ref_table: String,
        ref_cols: Vec<String>,
        on_delete: FkAction,
        on_update: FkAction,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ConstraintDef {
    pub name: String,
    pub kind: ConstraintKind,
}

impl ConstraintDef {
    /// Các cột (của bảng sở hữu) mà ràng buộc dùng tới; CHECK trả về rỗng.
    pub fn columns(&self) -> &[String] {
        match &self.kind {
            ConstraintKind::PrimaryKey { cols } | ConstraintKind::Unique { cols } => cols,
            ConstraintKind::ForeignKey { cols, .. } => cols,
            ConstraintKind::Check { .. } => &[],
        }
    }

    pub fn type_name(&self) -> &'static str {
        match self.kind {
            ConstraintKind::PrimaryKey { .. } => "PRIMARY KEY",
            ConstraintKind::Unique { .. } => "UNIQUE",
            ConstraintKind::Check { .. } => "CHECK",
            ConstraintKind::ForeignKey { .. } => "FOREIGN KEY",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct IndexDef {
    pub name: String,
    pub cols: Vec<String>,
    pub unique: bool,
    /// Index một phần: `CREATE INDEX ... WHERE <predicate>`.
    pub predicate: Option<String>,
}

/// Một thay đổi catalog. Chỉ được THÊM biến thể vào cuối (bincode ghi chỉ số biến thể).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum SchemaOp {
    /// Toàn bộ catalog của một bảng. Ghi ngay sau `CreateTable` của bản mới, và khi bảng do bản cũ
    /// tạo được nhận lại ràng buộc. Bảng có op này là bảng "được theo dõi" (`catalog_tracked`).
    /// `legacy`: bảng do bản cũ tạo, dữ liệu có thể vi phạm ràng buộc (không được kiểm tra lúc đó).
    TableCatalog { table: String, constraints: Vec<ConstraintDef>, indexes: Vec<IndexDef>, legacy: bool },
    AddConstraint { table: String, def: ConstraintDef },
    DropConstraint { table: String, name: String },
    CreateIndex { table: String, def: IndexDef },
    DropIndex { table: String, name: String },
    /// Đổi kiểu / DEFAULT / NULL của một cột (tên giữ nguyên). Giá trị đã chuyển kiểu được ghi
    /// riêng bằng các bản ghi `Update*` thường.
    AlterColumn { table: String, column: String, def: ColumnDef },
    RenameColumn { table: String, from: String, to: String },
    RenameTable { from: String, to: String },
}

pub fn encode_schema_op(op: &SchemaOp) -> Vec<u8> {
    bincode::serialize(op).unwrap_or_default()
}

/// `None` nếu op do bản mới hơn ghi (phiên bản hoặc biến thể chưa biết): bỏ qua, không lỗi.
pub fn decode_schema_op(version: u8, payload: &[u8]) -> Option<SchemaOp> {
    if version != SCHEMA_OP_VERSION {
        return None;
    }
    bincode::deserialize(payload).ok()
}

/// Một phần của khoá UNIQUE. So sánh đúng như PostgreSQL trên cột đã có kiểu: text so nguyên văn
/// (không như `JoinKey` coi '0123' bằng 123), số nguyên và số thực bằng giá trị.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum UKeyPart {
    Bool(bool),
    Int(i64),
    FloatBits(u64),
    Text(CompactString),
}

/// `None` khi giá trị là NULL (NULL không bao giờ trùng nhau trong UNIQUE).
pub fn ukey_part(v: &Value) -> Option<UKeyPart> {
    Some(match v {
        Value::Null => return None,
        Value::Bool(b) => UKeyPart::Bool(*b),
        Value::Int(i) => UKeyPart::Int(*i),
        Value::Float(f) => {
            if f.fract() == 0.0 && f.is_finite() && f.abs() < 9.0e15 {
                UKeyPart::Int(*f as i64)
            } else {
                UKeyPart::FloatBits(if *f == 0.0 { 0 } else { f.to_bits() })
            }
        }
        Value::Text(t) => {
            // UUID không phân biệt hoa thường
            if t.len() == 36 && t.as_bytes().get(8) == Some(&b'-') && t.chars().any(|c| c.is_ascii_uppercase()) {
                UKeyPart::Text(CompactString::new(t.to_lowercase()))
            } else {
                UKeyPart::Text(t.clone())
            }
        }
    })
}

/// Index UNIQUE lúc chạy (dựng lại từ catalog, không lưu). Chỉ chứa dòng còn sống.
#[derive(Debug, Clone)]
pub struct UniqueIdx {
    pub name: String,
    pub cols: Vec<usize>,
    pub predicate: Option<crate::engine::expr::CompiledExpr>,
    /// Khoá -> các dòng sống mang khoá đó. Thường chỉ một dòng; nhiều dòng khi DB cũ có sẵn dữ liệu
    /// trùng (không kiểm tra lúc replay).
    pub map: std::collections::HashMap<Vec<UKeyPart>, Vec<usize>>,
}

impl UniqueIdx {
    pub fn key(&self, row: &[Value]) -> Option<Vec<UKeyPart>> {
        if let Some(p) = &self.predicate {
            if p.eval_bool(row, None, &[]) != Some(true) {
                return None;
            }
        }
        let mut k = Vec::with_capacity(self.cols.len());
        for &c in &self.cols {
            k.push(ukey_part(row.get(c)?)?);
        }
        Some(k)
    }

    pub fn add(&mut self, row: &[Value], row_idx: usize) {
        if let Some(k) = self.key(row) {
            let list = self.map.entry(k).or_default();
            if !list.contains(&row_idx) {
                list.push(row_idx);
            }
        }
    }

    pub fn remove(&mut self, row: &[Value], row_idx: usize) {
        if let Some(k) = self.key(row) {
            if let Some(list) = self.map.get_mut(&k) {
                list.retain(|&r| r != row_idx);
                if list.is_empty() {
                    self.map.remove(&k);
                }
            }
        }
    }
}

/// Đổi tên định danh `from` thành `to` trong một biểu thức SQL (CHECK, điều kiện index một phần),
/// bỏ qua chuỗi trong nháy đơn. Không phân biệt hoa thường, chỉ thay nguyên từ.
pub fn rename_identifier(expr: &str, from: &str, to: &str) -> String {
    let mut out = String::with_capacity(expr.len());
    let mut chars = expr.char_indices().peekable();
    let mut in_str = false;
    let is_ident = |c: char| c.is_alphanumeric() || c == '_';
    let bytes = expr.as_bytes();
    while let Some((i, c)) = chars.next() {
        if c == '\'' {
            in_str = !in_str;
            out.push(c);
            continue;
        }
        if !in_str && is_ident(c) && (i == 0 || !is_ident(expr[..i].chars().next_back().unwrap())) {
            let mut end = i + c.len_utf8();
            while let Some(&(j, d)) = chars.peek() {
                if is_ident(d) {
                    end = j + d.len_utf8();
                    chars.next();
                } else {
                    break;
                }
            }
            let word = &expr[i..end];
            let quoted = i > 0 && bytes[i - 1] == b'"';
            if word.eq_ignore_ascii_case(from) && !quoted {
                out.push_str(to);
            } else {
                out.push_str(word);
            }
            continue;
        }
        out.push(c);
    }
    out
}
