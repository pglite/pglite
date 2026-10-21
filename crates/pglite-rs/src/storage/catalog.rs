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
    /// Kiểu khai báo chi tiết của cột (`numeric(6,2)`, `varchar(3)`, `timestamptz`...), dạng chuẩn của
    /// `coltypes::DeclType::canonical`. Ghi đè từng cột được liệt kê; chuỗi rỗng = xoá. Bản cũ bỏ qua op này.
    ColumnTypes { table: String, types: Vec<(String, String)> },
    /// Đánh dấu: các DDL sau đó trong cùng giao dịch được ghi bởi bản có DDL theo giao dịch, nên khi
    /// replay gặp TxRollback / RollbackToSavepoint thì hoàn tác cả DDL (bản cũ không làm vậy lúc chạy,
    /// nên WAL của bản cũ được replay như cũ).
    TxDdlUndo,
    // ---- Chỉ THÊM VÀO CUỐI (bincode ghi chỉ số biến thể). Bản cũ gặp biến thể lạ: bỏ qua op, không lỗi.
    /// Cột `GENERATED {ALWAYS | BY DEFAULT} AS IDENTITY`: `None` = bỏ identity. Cột vẫn có
    /// `DEFAULT nextval('<bảng>_<cột>_seq'::regclass)` nên bản cũ bỏ qua op này vẫn đọc dữ liệu như thường.
    ColumnIdentity { table: String, column: String, spec: Option<IdentitySpec> },
    /// `CREATE [OR REPLACE] [MATERIALIZED] VIEW`; `None` = DROP VIEW. Bản cũ bỏ qua (không có view; dữ liệu
    /// của materialized view là một bảng thường nên bản cũ vẫn đọc được như bảng).
    View { name: String, def: Option<ViewDef> },
    /// `CREATE SCHEMA` (`create = true`) / `DROP SCHEMA`.
    Schema { name: String, create: bool },
    /// `CREATE` / `ALTER SEQUENCE` độc lập (thuộc tính); `None` = DROP SEQUENCE.
    Sequence { name: String, spec: Option<SequenceSpec> },
    /// Giá trị hiện tại của sequence độc lập (ghi ở mỗi `nextval` / `setval` / `RESTART`). Không theo
    /// giao dịch, như PostgreSQL: ROLLBACK không trả lại số đã cấp.
    SequenceValue { name: String, last: i64, called: bool },
    /// `CREATE TYPE ... AS ENUM` / `ALTER TYPE ... ADD VALUE` (danh sách nhãn đầy đủ); `None` = DROP TYPE.
    EnumType { name: String, labels: Option<Vec<String>> },
    /// `CREATE DOMAIN`; `None` = DROP DOMAIN. Cột kiểu domain được lưu bằng kiểu gốc + ràng buộc nên bản
    /// cũ bỏ qua op này vẫn đọc bảng như thường.
    Domain { name: String, def: Option<DomainDef> },
    /// Thuộc tính phụ của một bảng dạng khoá → giá trị (`None` = xoá khoá). Bản cũ bỏ qua op này; bản mới gặp
    /// khoá lạ cũng bỏ qua, nên thêm loại thuộc tính mới không cần biến thể mới. Khoá đang dùng:
    /// `generated:<cột>` = biểu thức `GENERATED ALWAYS AS (...) STORED` (giá trị vẫn được lưu trong dòng nên
    /// bản cũ đọc được dữ liệu); `nulls_not_distinct:<ràng buộc/index>`; `deferrable:<ràng buộc>` =
    /// `immediate` | `deferred`; `unlogged`; `inherits` = danh sách bảng cha.
    TableMeta { table: String, key: String, value: Option<String> },
}

/// Định nghĩa domain: kiểu gốc và các ràng buộc (biểu thức CHECK dùng từ khoá `VALUE`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DomainDef {
    pub base: String,
    pub not_null: bool,
    pub default: Option<String>,
    pub checks: Vec<String>,
}

/// Định nghĩa view: câu truy vấn (đã qua lexer) và danh sách tên cột tuỳ chọn `CREATE VIEW v (a, b)`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ViewDef {
    pub query: String,
    pub columns: Vec<String>,
    /// Materialized view: dữ liệu nằm trong bảng cùng tên, `REFRESH` chạy lại `query`.
    pub materialized: bool,
    /// `WITH [CASCADED | LOCAL] CHECK OPTION`: dòng ghi qua view phải thoả điều kiện của view.
    pub check_option: bool,
}

/// Thuộc tính của một sequence độc lập.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct SequenceSpec {
    pub start: i64,
    pub increment: i64,
    pub min: i64,
    pub max: i64,
    pub cycle: bool,
}

/// Sequence độc lập: thuộc tính + giá trị cấp gần nhất (`called = false`: `nextval` kế tiếp trả `last`).
#[derive(Debug, Clone, PartialEq)]
pub struct SequenceState {
    pub spec: SequenceSpec,
    pub last: i64,
    pub called: bool,
}

/// Các đối tượng không phải bảng (chỉ trong bộ nhớ, dựng lại từ `SchemaOp` khi mở DB). Tên viết thường.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct ObjectCatalog {
    pub views: std::collections::BTreeMap<String, ViewDef>,
    /// Schema do người dùng tạo (không gồm `public`).
    pub schemas: std::collections::BTreeSet<String>,
    /// `DROP SCHEMA public` đã chạy mà chưa `CREATE SCHEMA public` lại.
    pub public_dropped: bool,
    pub sequences: std::collections::BTreeMap<String, SequenceState>,
    pub enums: std::collections::BTreeMap<String, Vec<String>>,
    pub domains: std::collections::BTreeMap<String, DomainDef>,
}

impl SchemaOp {
    /// Op sửa `ObjectCatalog` theo giao dịch (cần lưu trạng thái cũ để ROLLBACK).
    pub fn is_object_op(&self) -> bool {
        matches!(self, SchemaOp::View { .. } | SchemaOp::Schema { .. } | SchemaOp::Sequence { .. } | SchemaOp::EnumType { .. } | SchemaOp::Domain { .. })
    }
}

/// Thuộc tính identity của một cột.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct IdentitySpec {
    /// `GENERATED ALWAYS`: chỉ nhận giá trị tường minh khi có `OVERRIDING SYSTEM VALUE`
    pub always: bool,
    pub start: i64,
    pub increment: i64,
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
    /// Phần khoá là biểu thức (`CREATE UNIQUE INDEX ... (lower(name))`): cùng độ dài với `cols`, `None` = cột.
    pub exprs: Vec<Option<crate::engine::expr::CompiledExpr>>,
    /// `UNIQUE NULLS NOT DISTINCT`: NULL bằng NULL.
    pub nulls_not_distinct: bool,
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
        for (i, &c) in self.cols.iter().enumerate() {
            let computed;
            let v = match self.exprs.get(i).and_then(|e| e.as_ref()) {
                Some(e) => {
                    computed = e.eval(row, None, &[]);
                    &computed
                }
                None => row.get(c)?,
            };
            match ukey_part(v) {
                Some(p) => k.push(p),
                None if self.nulls_not_distinct => k.push(UKeyPart::Text(CompactString::new("\u{0}NULL"))),
                None => return None,
            }
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
