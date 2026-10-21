//! System catalog chính xác (đợt 14): `pg_attribute`, `pg_type`, `pg_index`, `pg_indexes`, `pg_constraint`, `pg_class`,
//! `pg_namespace`, `pg_proc`, `pg_settings`, `pg_database`, `pg_extension`, `pg_stat_activity`, `pg_roles`, `pg_user`,
//! `pg_attrdef`, `pg_trigger`, `pg_description`, `pg_am`, `pg_sequence`, `information_schema.referential_constraints /
//! check_constraints / triggers / routines / parameters`, cùng các hàm `format_type`, `pg_get_indexdef`,
//! `pg_get_constraintdef`, `pg_get_expr`, `to_regclass`, `::regclass / ::regtype / ::regnamespace`.
//!
//! Cách làm: mỗi câu SELECT nhắc tới các quan hệ trên được chạy trên bảng tạm trong bộ nhớ (không ghi WAL), dựng từ
//! một mô hình catalog duy nhất (`Model`, OID nhất quán giữa các bảng) — WHERE / JOIN / ORDER BY tự đúng bằng engine
//! thường. Dòng dựng sẵn của PostgreSQL (kiểu, tham số, hàm có trong engine...) lấy từ `pg_catalog_data.rs`.
//! Các đường catalog cũ (`ensure_virtual_catalog_tables`, introspection của NATA) không đổi; câu nào đường mới báo
//! lỗi thì quay về đường cũ như trước.
use super::coltypes::DeclType;
use super::executor::{contains_ci, replace_relation_name, Executor};
use super::pg_catalog_data as data;
use crate::storage::catalog::{ConstraintKind, FkAction};
use crate::types::{ColumnDef, DataType, QueryResult, Value};
use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;
use std::sync::OnceLock;

const NS_CATALOG: i64 = 11;
const NS_PUBLIC: i64 = 2200;

// ---------------------------------------------------------------------------------------------------------------
// Dữ liệu dựng sẵn

fn unescape(cell: &str) -> Option<String> {
    if cell == "\\N" {
        return None;
    }
    if !cell.contains('\\') {
        return Some(cell.to_string());
    }
    let mut s = String::with_capacity(cell.len());
    let mut it = cell.chars();
    while let Some(c) = it.next() {
        if c == '\\' {
            match it.next() {
                Some('t') => s.push('\t'),
                Some('n') => s.push('\n'),
                Some(x) => s.push(x),
                None => {}
            }
        } else {
            s.push(c);
        }
    }
    Some(s)
}

type Row = Vec<Option<String>>;

fn parse_rows(src: &[&str]) -> Vec<Row> {
    src.iter().map(|l| l.split('\t').map(unescape).collect()).collect()
}

macro_rules! static_rows {
    ($name:ident, $src:expr) => {
        pub(crate) fn $name() -> &'static [Row] {
            static R: OnceLock<Vec<Row>> = OnceLock::new();
            R.get_or_init(|| parse_rows($src))
        }
    };
}
static_rows!(settings_rows, data::PG_SETTINGS);
static_rows!(type_rows, data::PG_TYPE);
static_rows!(proc_rows, data::PG_PROC);

/// Giá trị như `SHOW` in (`4MB`, `1min`): `setting` của pg_settings đổi sang đơn vị lớn nhất chia hết.
pub(crate) fn show_value(r: &Row) -> String {
    let setting = r.get(1).cloned().flatten().unwrap_or_default();
    let unit = r.get(2).cloned().flatten().unwrap_or_default();
    let Ok(v) = setting.parse::<i64>() else { return setting };
    if v <= 0 || unit.is_empty() {
        return setting;
    }
    let (base, table): (i64, &[(&str, i64)]) = match unit.as_str() {
        "B" => (1, &[("TB", 1 << 40), ("GB", 1 << 30), ("MB", 1 << 20), ("kB", 1 << 10), ("B", 1)]),
        "kB" => (1 << 10, &[("TB", 1 << 40), ("GB", 1 << 30), ("MB", 1 << 20), ("kB", 1 << 10)]),
        "8kB" => (8 << 10, &[("TB", 1 << 40), ("GB", 1 << 30), ("MB", 1 << 20), ("kB", 1 << 10)]),
        "MB" => (1 << 20, &[("TB", 1 << 40), ("GB", 1 << 30), ("MB", 1 << 20)]),
        "ms" => (1, &[("d", 86_400_000), ("h", 3_600_000), ("min", 60_000), ("s", 1000), ("ms", 1)]),
        "s" => (1000, &[("d", 86_400_000), ("h", 3_600_000), ("min", 60_000), ("s", 1000)]),
        "min" => (60_000, &[("d", 86_400_000), ("h", 3_600_000), ("min", 60_000)]),
        _ => return setting,
    };
    let total = v.saturating_mul(base);
    for (name, size) in table {
        if total % size == 0 {
            return format!("{}{}", total / size, name);
        }
    }
    setting
}

/// Dòng `pg_settings` của tham số `name` (không phân biệt hoa thường).
pub(crate) fn setting_row(name: &str) -> Option<&'static Row> {
    settings_rows().iter().find(|r| r[0].as_deref().is_some_and(|n| n.eq_ignore_ascii_case(name)))
}

struct TypeIndex {
    by_oid: HashMap<i64, usize>,
    by_name: HashMap<String, i64>,
}

fn types() -> &'static TypeIndex {
    static T: OnceLock<TypeIndex> = OnceLock::new();
    T.get_or_init(|| {
        let mut by_oid = HashMap::new();
        let mut by_name = HashMap::new();
        for (i, r) in type_rows().iter().enumerate() {
            let oid = r[0].as_deref().and_then(|s| s.parse::<i64>().ok()).unwrap_or(0);
            by_oid.insert(oid, i);
            if let Some(n) = &r[1] {
                by_name.insert(n.clone(), oid);
            }
        }
        TypeIndex { by_oid, by_name }
    })
}

/// pg_type: cột thứ `col` (0-based) của kiểu `oid`.
fn type_attr(oid: i64, col: usize) -> Option<&'static str> {
    types().by_oid.get(&oid).and_then(|&i| type_rows()[i].get(col)).and_then(|c| c.as_deref())
}

/// OID của kiểu theo tên khai báo (`int`, `varchar(10)`, `text[]`, tên enum...).
fn type_oid_of_decl(d: &DeclType, enums: &HashMap<String, i64>) -> i64 {
    if let Some(o) = enums.get(&d.base) {
        return if d.array { 0 } else { *o };
    }
    if let Some(o) = d.oid() {
        return o as i64;
    }
    let base = match d.base.as_str() {
        "serial" => "int4",
        "bigserial" => "int8",
        "smallserial" => "int2",
        b => b,
    };
    let name = if d.array { format!("_{}", base) } else { base.to_string() };
    types().by_name.get(&name).copied().unwrap_or(25)
}

fn type_oid_of_name(ty: &str, enums: &HashMap<String, i64>) -> i64 {
    let t = ty.trim().to_lowercase();
    match t.as_str() {
        "void" => return 2278,
        "trigger" => return 2279,
        "record" => return 2249,
        "anyelement" => return 2283,
        _ => {}
    }
    DeclType::parse(&t).map(|d| type_oid_of_decl(&d, enums)).unwrap_or(25)
}

/// `atttypmod` của kiểu khai báo.
fn typmod_of(d: &DeclType) -> i64 {
    match (d.base.as_str(), d.mods.as_slice()) {
        ("varchar" | "bpchar", [n, ..]) => n + 4,
        ("numeric", [p]) => ((p << 16) | 0) + 4,
        ("numeric", [p, s, ..]) => ((p << 16) | (s & 0xffff)) + 4,
        ("timestamp" | "timestamptz" | "time" | "timetz" | "interval" | "bit" | "varbit", [n, ..]) => *n,
        _ => -1,
    }
}

/// `format_type(oid, typmod)` như PostgreSQL.
pub(crate) fn format_type(oid: i64, typmod: Option<i64>) -> String {
    let tm = typmod.unwrap_or(-1);
    // mảng: tên phần tử + []
    if type_attr(oid, 7) == Some("A") {
        if let Some(elem) = type_attr(oid, 13).and_then(|e| e.parse::<i64>().ok()).filter(|e| *e != 0) {
            return format!("{}[]", format_type(elem, typmod));
        }
    }
    let with_mod = |name: &str| if tm >= 4 { format!("{}({})", name, tm - 4) } else { name.to_string() };
    let prec = |name: &str, suffix: &str| if tm >= 0 { format!("{}({}){}", name, tm, suffix) } else { format!("{}{}", name, suffix) };
    match oid {
        16 => "boolean".into(),
        17 => "bytea".into(),
        18 => "\"char\"".into(),
        19 => "name".into(),
        20 => "bigint".into(),
        21 => "smallint".into(),
        23 => "integer".into(),
        25 => "text".into(),
        26 => "oid".into(),
        700 => "real".into(),
        701 => "double precision".into(),
        1042 => {
            if tm >= 4 {
                format!("character({})", tm - 4)
            } else if typmod.is_none() {
                "character".into()
            } else {
                "bpchar".into()
            }
        }
        1043 => with_mod("character varying"),
        1082 => "date".into(),
        1083 => prec("time", " without time zone"),
        1114 => prec("timestamp", " without time zone"),
        1184 => prec("timestamp", " with time zone"),
        1266 => prec("time", " with time zone"),
        1186 => "interval".into(),
        1560 => if tm >= 0 { format!("bit({})", tm) } else { "bit".into() },
        1562 => if tm >= 0 { format!("bit varying({})", tm) } else { "bit varying".into() },
        1700 => {
            if tm >= 4 {
                let v = tm - 4;
                format!("numeric({},{})", (v >> 16) & 0xffff, v & 0xffff)
            } else {
                "numeric".into()
            }
        }
        _ => match type_attr(oid, 1) {
            Some(n) => n.to_string(),
            None => with_model(|m| m.user_types.iter().find(|(o, _, _)| *o == oid).map(|(_, n, _)| n.clone())).flatten().unwrap_or_else(|| "???".into()),
        },
    }
}

// ---------------------------------------------------------------------------------------------------------------
// Mô hình catalog của DB hiện tại

struct Col {
    num: i64,
    name: String,
    typid: i64,
    typmod: i64,
    notnull: bool,
    default: Option<String>,
    identity: &'static str,
    generated: &'static str,
    dims: i64,
    comment: Option<String>,
}

struct Rel {
    oid: i64,
    name: String,
    /// r | v | m | S | i
    kind: char,
    temp: bool,
    cols: Vec<Col>,
    comment: Option<String>,
    has_index: bool,
    has_triggers: bool,
    checks: i64,
    view_def: Option<String>,
}

struct Con {
    oid: i64,
    name: String,
    contype: char,
    rel: i64,
    cols: Vec<i64>,
    frel: i64,
    fcols: Vec<i64>,
    upd: char,
    del: char,
    deferrable: bool,
    deferred: bool,
    indid: i64,
    def: String,
    check: Option<String>,
}

struct Idx {
    oid: i64,
    name: String,
    rel: i64,
    keys: Vec<i64>,
    exprs: Vec<String>,
    unique: bool,
    primary: bool,
    nnd: bool,
    pred: Option<String>,
    def: String,
}

struct Proc {
    oid: i64,
    name: String,
    kind: char,
    nargs: i64,
    ndefaults: i64,
    rettype: i64,
    retset: bool,
    argtypes: Vec<i64>,
    allargs: Option<Vec<i64>>,
    modes: Option<Vec<char>>,
    names: Option<Vec<String>>,
    lang: i64,
    src: String,
    strict: bool,
    language: String,
    ret_name: String,
    args_text: String,
}

struct Trig {
    oid: i64,
    rel: i64,
    name: String,
    tgtype: i64,
    enabled: bool,
    fnoid: i64,
    timing: String,
    events: Vec<String>,
    row: bool,
    when: Option<String>,
    function: String,
    table: String,
}

pub(crate) struct Model {
    rels: Vec<Rel>,
    cons: Vec<Con>,
    idxs: Vec<Idx>,
    procs: Vec<Proc>,
    trigs: Vec<Trig>,
    /// schema người dùng: (oid, tên)
    schemas: Vec<(i64, String)>,
    /// enum / domain: (oid, tên, typtype)
    user_types: Vec<(i64, String, char)>,
    /// tên quan hệ (viết thường) → oid
    by_name: HashMap<String, i64>,
}

impl Model {
    fn rel(&self, oid: i64) -> Option<&Rel> {
        self.rels.iter().find(|r| r.oid == oid)
    }
    fn relname(&self, oid: i64) -> Option<String> {
        self.rel(oid).map(|r| r.name.clone()).or_else(|| self.idxs.iter().find(|i| i.oid == oid).map(|i| i.name.clone()))
    }
    fn nsp_oid(&self, name: &str) -> Option<i64> {
        let n = name.trim().trim_matches('"').to_lowercase();
        match n.as_str() {
            "pg_catalog" => Some(NS_CATALOG),
            "pg_toast" => Some(99),
            "public" => Some(NS_PUBLIC),
            "information_schema" => Some(12424),
            _ => self.schemas.iter().find(|(_, s)| *s == n).map(|(o, _)| *o),
        }
    }
    fn regclass(&self, name: &str) -> Option<i64> {
        let n = name.trim().trim_matches('"');
        let n = n.strip_prefix("public.").unwrap_or(n);
        self.by_name.get(&n.to_lowercase()).copied()
    }
}

thread_local! {
    static MODEL: RefCell<Option<Rc<Model>>> = const { RefCell::new(None) };
    /// Câu đang chạy (cột `query` của pg_stat_activity).
    static CURRENT_SQL: RefCell<String> = const { RefCell::new(String::new()) };
}

fn with_model<T>(f: impl FnOnce(&Model) -> T) -> Option<T> {
    MODEL.with(|m| m.borrow().as_ref().map(|m| f(m)))
}

struct ModelGuard;
impl Drop for ModelGuard {
    fn drop(&mut self) {
        MODEL.with(|m| *m.borrow_mut() = None);
    }
}

fn fk_char(a: &FkAction) -> char {
    match a {
        FkAction::NoAction => 'a',
        FkAction::Restrict => 'r',
        FkAction::Cascade => 'c',
        FkAction::SetNull => 'n',
        FkAction::SetDefault => 'd',
    }
}

fn fk_rule(c: char) -> &'static str {
    match c {
        'r' => "RESTRICT",
        'c' => "CASCADE",
        'n' => "SET NULL",
        'd' => "SET DEFAULT",
        _ => "NO ACTION",
    }
}

fn quote_ident(s: &str) -> String {
    if !s.is_empty() && s.chars().all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_') && !s.starts_with(|c: char| c.is_ascii_digit()) {
        s.to_string()
    } else {
        format!("\"{}\"", s.replace('"', "\"\""))
    }
}

/// `CHECK (...)` như `pg_get_constraintdef`: biểu thức trong hai lớp ngoặc.
fn check_def(expr: &str) -> String {
    let e = expr.trim();
    let wrapped = e.starts_with('(') && super::constraints::matching_paren(e, 0) == Some(e.len() - 1);
    if wrapped {
        format!("CHECK ({})", e)
    } else {
        format!("CHECK (({}))", e)
    }
}

impl Executor {
    /// Tên các bảng người dùng theo thứ tự cấp OID (như `ensure_virtual_catalog_tables`: sắp tên, từ 16384).
    fn catalog_model(&self) -> Model {
        let mut next_oid = 16384i64;
        let mut by_name = HashMap::new();
        let table_names = self.user_table_names_pub();
        let _ = &table_names;
        // OID bảng giống đường cũ: bảng người dùng sắp theo tên, liên tiếp từ 16384
        let mut table_oids: Vec<(String, i64)> = Vec::new();
        for k in &table_names {
            table_oids.push((k.clone(), next_oid));
            by_name.insert(k.to_lowercase(), next_oid);
            next_oid += 1;
        }
        let mut oid = 24576i64;
        let mut fresh = || {
            oid += 1;
            oid
        };
        let objects = &self.storage.objects;
        let mut user_types = Vec::new();
        let mut enum_oids: HashMap<String, i64> = HashMap::new();
        for name in objects.enums.keys() {
            let o = fresh();
            enum_oids.insert(name.clone(), o);
            user_types.push((o, name.clone(), 'e'));
        }
        for name in objects.domains.keys() {
            user_types.push((fresh(), name.clone(), 'd'));
        }
        let schemas: Vec<(i64, String)> = objects.schemas.iter().map(|s| (fresh(), s.clone())).collect();

        let mut rels = Vec::new();
        let mut cons = Vec::new();
        let mut idxs = Vec::new();
        let mut pk_index_of: HashMap<i64, i64> = HashMap::new();
        for (key, toid) in &table_oids {
            let t = &self.storage.tables[key];
            let matview = objects.views.get(key).is_some_and(|v| v.materialized);
            let mut cols = Vec::new();
            for (i, c) in t.columns.iter().enumerate() {
                let decl = super::coltypes::column_decl(t, i);
                let (typid, typmod, dims) = match &decl {
                    Some(d) => (type_oid_of_decl(d, &enum_oids), typmod_of(d), i64::from(d.array)),
                    None => (25, -1, 0),
                };
                let serial = c.data_type == DataType::Serial || decl.as_ref().is_some_and(|d| d.base.ends_with("serial"));
                let ident = t.identity.get(&c.name.to_lowercase()).or_else(|| t.identity.get(&c.name));
                let generated = t.meta.contains_key(&format!("generated:{}", c.name.to_lowercase()));
                let default = if generated {
                    t.meta.get(&format!("generated:{}", c.name.to_lowercase())).cloned()
                } else if ident.is_some() {
                    None
                } else {
                    c.default_value.as_ref().map(|d| super::executor::normalize_default_expr(d)).filter(|d| !d.is_empty()).or_else(|| serial.then(|| format!("nextval('{}_{}_seq'::regclass)", t.name, c.name)))
                };
                let pk = c.is_primary_key && t.pk_enforced;
                cols.push(Col {
                    num: i as i64 + 1,
                    name: c.name.clone(),
                    typid,
                    typmod,
                    notnull: !c.is_nullable || pk,
                    default,
                    identity: match ident {
                        Some(s) if s.always => "a",
                        Some(_) => "d",
                        None => "",
                    },
                    generated: if generated { "s" } else { "" },
                    dims,
                    comment: c.comment.clone(),
                });
            }
            let colnum = |name: &str| -> i64 { t.get_column_index(name).map(|i| i as i64 + 1).unwrap_or(0) };
            let collist = |nums: &[i64]| nums.iter().map(|n| t.columns.get(*n as usize - 1).map(|c| quote_ident(&c.name)).unwrap_or_default()).collect::<Vec<_>>().join(", ");
            // khoá chính: ràng buộc PRIMARY KEY, hoặc cột khai báo PRIMARY KEY
            let mut pk_cols: Vec<i64> = Vec::new();
            let mut pk_name = format!("{}_pkey", t.name);
            for c in &t.constraints {
                if let ConstraintKind::PrimaryKey { cols } = &c.kind {
                    pk_cols = cols.iter().map(|c| colnum(c)).collect();
                    pk_name = c.name.clone();
                }
            }
            if pk_cols.is_empty() && t.pk_enforced {
                if let Some(i) = t.columns.iter().position(|c| c.is_primary_key) {
                    pk_cols.push(i as i64 + 1);
                }
            }
            let deferrable = |name: &str| t.meta.get(&format!("deferrable:{}", name.to_lowercase())).map(|m| (true, m == "deferred")).unwrap_or((false, false));
            if !pk_cols.is_empty() {
                let io = fresh();
                pk_index_of.insert(*toid, io);
                let def = format!("CREATE UNIQUE INDEX {} ON public.{} USING btree ({})", quote_ident(&pk_name), quote_ident(&t.name), collist(&pk_cols));
                idxs.push(Idx { oid: io, name: pk_name.clone(), rel: *toid, keys: pk_cols.clone(), exprs: vec![], unique: true, primary: true, nnd: false, pred: None, def });
                cons.push(Con {
                    oid: fresh(),
                    name: pk_name.clone(),
                    contype: 'p',
                    rel: *toid,
                    cols: pk_cols.clone(),
                    frel: 0,
                    fcols: vec![],
                    upd: ' ',
                    del: ' ',
                    deferrable: false,
                    deferred: false,
                    indid: io,
                    def: format!("PRIMARY KEY ({})", collist(&pk_cols)),
                    check: None,
                });
                by_name.insert(pk_name.to_lowercase(), io);
            }
            let mut checks = 0;
            for c in &t.constraints {
                let (d, dd) = deferrable(&c.name);
                match &c.kind {
                    ConstraintKind::PrimaryKey { .. } => {}
                    ConstraintKind::Unique { cols: ucols } => {
                        let nums: Vec<i64> = ucols.iter().map(|c| colnum(c)).collect();
                        let nnd = t.meta.contains_key(&format!("nulls_not_distinct:{}", c.name.to_lowercase()));
                        let io = fresh();
                        let def = format!(
                            "CREATE UNIQUE INDEX {} ON public.{} USING btree ({}){}",
                            quote_ident(&c.name),
                            quote_ident(&t.name),
                            collist(&nums),
                            if nnd { " NULLS NOT DISTINCT" } else { "" }
                        );
                        idxs.push(Idx { oid: io, name: c.name.clone(), rel: *toid, keys: nums.clone(), exprs: vec![], unique: true, primary: false, nnd, pred: None, def });
                        by_name.insert(c.name.to_lowercase(), io);
                        cons.push(Con {
                            oid: fresh(),
                            name: c.name.clone(),
                            contype: 'u',
                            rel: *toid,
                            cols: nums.clone(),
                            frel: 0,
                            fcols: vec![],
                            upd: ' ',
                            del: ' ',
                            deferrable: d,
                            deferred: dd,
                            indid: io,
                            def: format!("UNIQUE {}({})", if nnd { "NULLS NOT DISTINCT " } else { "" }, collist(&nums)),
                            check: None,
                        });
                    }
                    ConstraintKind::Check { expr } => {
                        checks += 1;
                        cons.push(Con {
                            oid: fresh(),
                            name: c.name.clone(),
                            contype: 'c',
                            rel: *toid,
                            cols: vec![],
                            frel: 0,
                            fcols: vec![],
                            upd: ' ',
                            del: ' ',
                            deferrable: false,
                            deferred: false,
                            indid: 0,
                            def: check_def(expr),
                            check: Some(expr.clone()),
                        });
                    }
                    ConstraintKind::ForeignKey { .. } => {}
                }
            }
            // index khai báo bằng CREATE INDEX
            for ix in &t.indexes {
                if idxs.iter().any(|i| i.rel == *toid && i.name.eq_ignore_ascii_case(&ix.name)) {
                    continue;
                }
                let mut keys = Vec::new();
                let mut exprs = Vec::new();
                for c in &ix.cols {
                    let n = colnum(c.trim().trim_matches('"'));
                    keys.push(n);
                    if n == 0 {
                        exprs.push(c.trim().to_string());
                    }
                }
                let cols_txt = ix.cols.iter().map(|c| {
                    let n = colnum(c.trim().trim_matches('"'));
                    if n > 0 { quote_ident(&t.columns[n as usize - 1].name) } else { format!("({})", c.trim()) }
                }).collect::<Vec<_>>().join(", ");
                let def = format!(
                    "CREATE {}INDEX {} ON public.{} USING btree ({}){}",
                    if ix.unique { "UNIQUE " } else { "" },
                    quote_ident(&ix.name),
                    quote_ident(&t.name),
                    cols_txt,
                    ix.predicate.as_ref().map(|p| format!(" WHERE ({})", p.trim())).unwrap_or_default()
                );
                let io = fresh();
                by_name.insert(ix.name.to_lowercase(), io);
                idxs.push(Idx { oid: io, name: ix.name.clone(), rel: *toid, keys, exprs, unique: ix.unique, primary: false, nnd: false, pred: ix.predicate.clone(), def });
            }
            // NOT NULL (PostgreSQL 18 ghi thành ràng buộc `n`)
            for col in &cols {
                if col.notnull {
                    cons.push(Con {
                        oid: fresh(),
                        name: format!("{}_{}_not_null", t.name, col.name),
                        contype: 'n',
                        rel: *toid,
                        cols: vec![col.num],
                        frel: 0,
                        fcols: vec![],
                        upd: ' ',
                        del: ' ',
                        deferrable: false,
                        deferred: false,
                        indid: 0,
                        def: format!("NOT NULL {}", quote_ident(&col.name)),
                        check: None,
                    });
                }
            }
            let has_index = idxs.iter().any(|i| i.rel == *toid);
            let has_triggers = t.meta.keys().any(|k| k.starts_with("trigger:"));
            rels.push(Rel {
                oid: *toid,
                name: t.name.clone(),
                kind: if matview { 'm' } else { 'r' },
                temp: self.storage.wal.temp_tables.contains(&key.to_lowercase()),
                cols,
                comment: t.comment.clone(),
                has_index,
                has_triggers,
                checks,
                view_def: if matview { objects.views.get(key).map(|v| v.query.clone()) } else { None },
            });
        }
        // khoá ngoại (cần OID của bảng được tham chiếu)
        for (key, toid) in &table_oids {
            let t = &self.storage.tables[key];
            for c in &t.constraints {
                if let ConstraintKind::ForeignKey { cols, ref_table, ref_cols, on_delete, on_update } = &c.kind {
                    let colnum = |tb: &crate::storage::table::Table, name: &str| tb.get_column_index(name).map(|i| i as i64 + 1).unwrap_or(0);
                    let frel = by_name.get(&ref_table.to_lowercase()).copied().unwrap_or(0);
                    let ft = self.storage.get_table(ref_table).or_else(|| self.storage.get_table(&ref_table.to_lowercase()));
                    let fcols: Vec<i64> = match ft {
                        Some(ft) if !ref_cols.is_empty() => ref_cols.iter().map(|c| colnum(ft, c)).collect(),
                        Some(ft) => ft.columns.iter().position(|c| c.is_primary_key).map(|i| vec![i as i64 + 1]).unwrap_or_default(),
                        None => vec![],
                    };
                    let ref_names: Vec<String> = match ft {
                        Some(ft) => fcols.iter().filter_map(|n| ft.columns.get((*n as usize).wrapping_sub(1)).map(|c| quote_ident(&c.name))).collect(),
                        None => ref_cols.iter().map(|c| quote_ident(c)).collect(),
                    };
                    let (upd, del) = (fk_char(on_update), fk_char(on_delete));
                    let (d, dd) = t.meta.get(&format!("deferrable:{}", c.name.to_lowercase())).map(|m| (true, m == "deferred")).unwrap_or((false, false));
                    let mut def = format!(
                        "FOREIGN KEY ({}) REFERENCES {}({})",
                        cols.iter().map(|c| quote_ident(&t.columns.get(t.get_column_index(c).unwrap_or(usize::MAX)).map(|x| x.name.clone()).unwrap_or_else(|| c.clone()))).collect::<Vec<_>>().join(", "),
                        quote_ident(&ft.map(|f| f.name.clone()).unwrap_or_else(|| ref_table.clone())),
                        ref_names.join(", ")
                    );
                    if upd != 'a' {
                        def.push_str(&format!(" ON UPDATE {}", fk_rule(upd)));
                    }
                    if del != 'a' {
                        def.push_str(&format!(" ON DELETE {}", fk_rule(del)));
                    }
                    if d {
                        def.push_str(if dd { " DEFERRABLE INITIALLY DEFERRED" } else { " DEFERRABLE" });
                    }
                    cons.push(Con {
                        oid: fresh(),
                        name: c.name.clone(),
                        contype: 'f',
                        rel: *toid,
                        cols: cols.iter().map(|c| colnum(t, c)).collect(),
                        frel,
                        fcols,
                        upd,
                        del,
                        deferrable: d,
                        deferred: dd,
                        indid: pk_index_of.get(&frel).copied().unwrap_or(0),
                        def,
                        check: None,
                    });
                }
            }
        }
        // view, sequence
        for (name, v) in objects.views.iter().filter(|(_, v)| !v.materialized) {
            let o = fresh();
            by_name.insert(name.to_lowercase(), o);
            let cols = v.columns.iter().enumerate().map(|(i, c)| Col { num: i as i64 + 1, name: c.clone(), typid: 25, typmod: -1, notnull: false, default: None, identity: "", generated: "", dims: 0, comment: None }).collect();
            rels.push(Rel { oid: o, name: name.clone(), kind: 'v', temp: false, cols, comment: None, has_index: false, has_triggers: false, checks: 0, view_def: Some(v.query.clone()) });
        }
        let mut seqs: Vec<String> = objects.sequences.keys().cloned().collect();
        for r in rels.iter().filter(|r| r.kind == 'r') {
            for c in &r.cols {
                if c.default.as_deref().is_some_and(|d| d.starts_with("nextval('")) {
                    let s = format!("{}_{}_seq", r.name, c.name);
                    if !seqs.contains(&s) {
                        seqs.push(s);
                    }
                }
            }
        }
        for s in seqs {
            let o = fresh();
            by_name.insert(s.to_lowercase(), o);
            rels.push(Rel { oid: o, name: s, kind: 'S', temp: false, cols: vec![], comment: None, has_index: false, has_triggers: false, checks: 0, view_def: None });
        }
        // routine
        let mut procs = Vec::new();
        for ((kind, _sig), json) in objects.extra.iter() {
            if kind != "routine" {
                continue;
            }
            let Ok(def) = serde_json::from_str::<super::routines::RoutineDef>(json) else { continue };
            let inputs: Vec<&super::routines::RoutineArg> = def.args.iter().filter(|a| a.mode != "out").collect();
            let argtypes: Vec<i64> = inputs.iter().map(|a| type_oid_of_name(&a.ty, &enum_oids)).collect();
            let has_out = def.args.iter().any(|a| a.mode == "out" || a.mode == "inout") || !def.table_cols.is_empty();
            let mut all: Vec<(i64, char, String)> = def.args.iter().map(|a| (type_oid_of_name(&a.ty, &enum_oids), match a.mode.as_str() { "out" => 'o', "inout" => 'b', "variadic" => 'v', _ => 'i' }, a.name.clone().unwrap_or_default())).collect();
            for (n, ty) in &def.table_cols {
                all.push((type_oid_of_name(ty, &enum_oids), 't', n.clone()));
            }
            let any_names = all.iter().any(|(_, _, n)| !n.is_empty());
            let ret = if def.procedure {
                0
            } else if def.returns == "table" || (has_out && all.iter().filter(|(_, m, _)| *m != 'i' && *m != 'v').count() > 1) {
                2249
            } else if has_out {
                all.iter().find(|(_, m, _)| *m != 'i' && *m != 'v').map(|(o, _, _)| *o).unwrap_or(2249)
            } else {
                type_oid_of_name(&def.returns, &enum_oids)
            };
            let lang = match def.language.as_str() {
                "sql" => 14,
                "plpgsql" => 13000,
                _ => 13000,
            };
            let args_text = def
                .args
                .iter()
                .filter(|a| a.mode != "out")
                .map(|a| {
                    let mode = match a.mode.as_str() { "inout" => "INOUT ", "variadic" => "VARIADIC ", _ => "" };
                    let name = a.name.as_ref().map(|n| format!("{} ", n)).unwrap_or_default();
                    let ty = format_type(type_oid_of_name(&a.ty, &enum_oids), None);
                    // literal chuỗi được in kèm kiểu như PG: `'x'::text`
                    let dflt = a.default.as_ref().map(|d| if d.trim().starts_with('\'') && !d.contains("::") { format!(" DEFAULT {}::{}", d.trim(), ty) } else { format!(" DEFAULT {}", d.trim()) }).unwrap_or_default();
                    format!("{}{}{}{}", mode, name, ty, dflt)
                })
                .collect::<Vec<_>>()
                .join(", ");
            procs.push(Proc {
                oid: fresh(),
                name: def.name.to_lowercase(),
                kind: if def.procedure { 'p' } else { 'f' },
                nargs: inputs.len() as i64,
                ndefaults: inputs.iter().filter(|a| a.default.is_some()).count() as i64,
                rettype: ret,
                retset: def.returns_set,
                argtypes,
                allargs: has_out.then(|| all.iter().map(|(o, _, _)| *o).collect()),
                modes: has_out.then(|| all.iter().map(|(_, m, _)| *m).collect()),
                names: any_names.then(|| all.iter().map(|(_, _, n)| n.clone()).collect()),
                lang,
                src: def.body.clone(),
                strict: def.strict,
                language: def.language.clone(),
                ret_name: if def.procedure { String::new() } else { def.returns.clone() },
                args_text,
            });
        }
        // trigger
        let mut trigs = Vec::new();
        for (key, toid) in &table_oids {
            let t = &self.storage.tables[key];
            for (k, v) in t.meta.range("trigger:".to_string()..) {
                if !k.starts_with("trigger:") {
                    break;
                }
                let Ok(d) = serde_json::from_str::<super::triggers::TriggerDef>(v) else { continue };
                let mut tgtype = if d.row { 1 } else { 0 };
                tgtype |= match d.timing.as_str() { "before" => 2, "instead" => 64, _ => 0 };
                for e in &d.events {
                    tgtype |= match e.as_str() { "insert" => 4, "delete" => 8, "update" => 16, "truncate" => 32, _ => 0 };
                }
                let fname = d.function.trim().trim_end_matches("()").to_lowercase();
                let fnoid = procs.iter().find(|p| p.name == fname).map(|p| p.oid).unwrap_or(0);
                trigs.push(Trig {
                    oid: fresh(),
                    rel: *toid,
                    name: d.name.clone(),
                    tgtype,
                    enabled: d.enabled,
                    fnoid,
                    timing: d.timing.to_uppercase(),
                    events: d.events.iter().map(|e| e.to_uppercase()).collect(),
                    row: d.row,
                    when: d.when.clone(),
                    function: fname,
                    table: t.name.clone(),
                });
            }
        }
        Model { rels, cons, idxs, procs, trigs, schemas, user_types, by_name }
    }
}

// ---------------------------------------------------------------------------------------------------------------
// Bảng ảo

/// Quan hệ được xử lý ở đây: (tên đầy đủ, tên bảng tạm, có thể viết không kèm schema)
const RELS: &[(&str, &str, bool)] = &[
    ("pg_catalog.pg_attribute", "__cat_attribute", true),
    ("pg_catalog.pg_type", "__cat_type", true),
    ("pg_catalog.pg_index", "__cat_index", true),
    ("pg_catalog.pg_indexes", "__cat_indexes", true),
    ("pg_catalog.pg_constraint", "__cat_constraint", true),
    ("pg_catalog.pg_class", "__cat_class", true),
    ("pg_catalog.pg_namespace", "__cat_namespace", true),
    ("pg_catalog.pg_proc", "__cat_proc", true),
    ("pg_catalog.pg_settings", "__cat_settings", true),
    ("pg_catalog.pg_database", "__cat_database", true),
    ("pg_catalog.pg_extension", "__cat_extension", true),
    ("pg_catalog.pg_stat_activity", "__cat_stat_activity", true),
    ("pg_catalog.pg_roles", "__cat_roles", true),
    ("pg_catalog.pg_user", "__cat_user", true),
    ("pg_catalog.pg_attrdef", "__cat_attrdef", true),
    ("pg_catalog.pg_trigger", "__cat_trigger", true),
    ("pg_catalog.pg_description", "__cat_description", true),
    ("pg_catalog.pg_am", "__cat_am", true),
    ("pg_catalog.pg_sequence", "__cat_sequence", true),
    ("information_schema.referential_constraints", "__cat_is_referential_constraints", false),
    ("information_schema.check_constraints", "__cat_is_check_constraints", false),
    ("information_schema.triggers", "__cat_is_triggers", false),
    ("information_schema.routines", "__cat_is_routines", false),
    ("information_schema.parameters", "__cat_is_parameters", false),
];

/// Hàm cần mô hình catalog (OID → tên / định nghĩa).
const CATALOG_FNS: &[&str] = &["pg_get_indexdef", "pg_get_constraintdef", "to_regclass", "to_regtype", "to_regnamespace", "pg_get_viewdef", "pg_get_triggerdef", "pg_get_functiondef", "pg_get_function_arguments", "pg_get_function_result"];

fn mentions_word(sql: &str, word: &str) -> bool {
    let b = sql.as_bytes();
    let w = word.as_bytes();
    let ident = |c: u8| c.is_ascii_alphanumeric() || c == b'_' || c == b'.';
    let mut i = 0;
    while i + w.len() <= b.len() {
        if b[i..i + w.len()].eq_ignore_ascii_case(w) && (i == 0 || !ident(b[i - 1])) && (i + w.len() == b.len() || !ident(b[i + w.len()]) || b[i + w.len()] == b'.') {
            return true;
        }
        i += 1;
    }
    false
}

/// Dạng ngắn (không schema) của quan hệ có trong câu, kể cả khi viết kèm schema.
fn used_relations(sql: &str) -> Vec<usize> {
    RELS.iter()
        .enumerate()
        .filter(|(_, (full, _, bare))| {
            let short = full.rsplit('.').next().unwrap();
            mentions_word(sql, full) || (*bare && mentions_word(sql, short))
        })
        .map(|(i, _)| i)
        .collect()
}

/// Câu có cần đường catalog mới không (lọc nhanh trước, rồi kiểm từng tên).
pub(crate) fn wants_catalog(sql: &str) -> bool {
    // đang chạy câu đã viết lại của chính đường này
    if MODEL.with(|m| m.borrow().is_some()) {
        return false;
    }
    if !(contains_ci(sql, "pg_") || contains_ci(sql, "information_schema") || contains_ci(sql, "::reg") || contains_ci(sql, "to_reg") || contains_ci(sql, "format_type")) {
        return false;
    }
    !used_relations(sql).is_empty() || CATALOG_FNS.iter().any(|f| contains_ci(sql, &format!("{}(", f))) || contains_ci(sql, "::regclass") || contains_ci(sql, "::regtype") || contains_ci(sql, "::regnamespace")
}

fn col(name: &str, ty: &str) -> (ColumnDef, Option<DeclType>) {
    let t = ty.trim();
    let (dt, decl) = match t {
        "oid" | "integer" | "smallint" | "bigint" | "xid" | "cardinal_number" => (DataType::Integer, match t { "oid" => "oid", "smallint" => "int2", "bigint" => "int8", "xid" => "xid", _ => "int4" }),
        "boolean" => (DataType::Boolean, "bool"),
        "real" => (DataType::Numeric, "float4"),
        "name" | "sql_identifier" => (DataType::Text, "name"),
        "timestamp with time zone" | "time_stamp" => (DataType::Text, "timestamptz"),
        t if t.ends_with("[]") => (DataType::Array(t.trim_end_matches("[]").trim_matches('"').to_string()), "text"),
        // int2vector / oidvector: lưu như mảng (`= ANY(indkey)`, JSON `[1, 2]` như bản cũ)
        "int2vector" => (DataType::Array("smallint".to_string()), "text"),
        "oidvector" => (DataType::Array("oid".to_string()), "text"),
        _ => (DataType::Text, "text"),
    };
    let decl = if let DataType::Array(e) = &dt {
        let e = match e.as_str() { "smallint" => "int2", "oid" => "oid", "text" => "text", _ => "text" };
        DeclType::parse(&format!("{}[]", e))
    } else {
        DeclType::parse(decl)
    };
    (ColumnDef { name: name.to_string(), data_type: dt, is_primary_key: false, is_nullable: true, default_value: None, comment: None }, decl)
}

fn new_table(rel: &str, tmp: &str) -> crate::storage::table::Table {
    let spec = data::RELATIONS.iter().find(|(n, _)| *n == rel).map(|(_, c)| *c).unwrap_or(&[]);
    let mut cols = Vec::new();
    let mut decls = HashMap::new();
    for (n, ty) in spec {
        let (c, d) = col(n, ty);
        if let Some(d) = d {
            decls.insert(n.to_string(), d);
        }
        cols.push(c);
    }
    let mut t = crate::storage::table::Table::new(tmp.to_string(), cols);
    t.pk_col_idx = None;
    t.decl_types = decls;
    t
}

/// Giá trị ô theo kiểu cột (dữ liệu dựng sẵn ở dạng văn bản).
fn typed(c: &ColumnDef, v: Option<&str>) -> Value {
    let Some(v) = v else { return Value::Null };
    match c.data_type {
        DataType::Integer => v.parse::<i64>().map(Value::Int).unwrap_or_else(|_| Value::text(v)),
        DataType::Boolean => Value::Bool(v == "t" || v == "true"),
        DataType::Numeric => v.parse::<f64>().map(Value::Float).unwrap_or_else(|_| Value::text(v)),
        _ => Value::text(v),
    }
}

fn push_static(t: &mut crate::storage::table::Table, rows: &[Row]) {
    for r in rows {
        let vals = t.columns.iter().enumerate().map(|(i, c)| typed(c, r.get(i).and_then(|x| x.as_deref()))).collect();
        t.insert(vals);
    }
}

fn b(v: bool) -> Value {
    Value::Bool(v)
}
fn i(v: i64) -> Value {
    Value::Int(v)
}
fn s(v: impl AsRef<str>) -> Value {
    Value::text(v)
}
fn opt(v: &Option<String>) -> Value {
    v.as_ref().map(Value::text).unwrap_or(Value::Null)
}
fn int_array(v: &[i64]) -> Value {
    Value::text(format!("{{{}}}", v.iter().map(|x| x.to_string()).collect::<Vec<_>>().join(",")))
}
fn vector(v: &[i64]) -> Value {
    int_array(v)
}

impl Executor {
    fn build_catalog_table(&self, idx: usize, m: &Model) -> crate::storage::table::Table {
        let (full, tmp, _) = RELS[idx];
        let mut t = new_table(full, tmp);
        let push = |t: &mut crate::storage::table::Table, vals: Vec<Value>| {
            debug_assert_eq!(vals.len(), t.columns.len(), "{}", t.name);
            t.insert(vals);
        };
        match full {
            "pg_catalog.pg_attribute" => {
                for r in &m.rels {
                    for c in &r.cols {
                        let len = type_attr(c.typid, 4).and_then(|v| v.parse::<i64>().ok()).unwrap_or(-1);
                        let attr = |col: usize, d: &str| type_attr(c.typid, col).unwrap_or(d).to_string();
                        push(
                            &mut t,
                            vec![
                                i(r.oid),
                                s(&c.name),
                                i(c.typid),
                                i(len),
                                i(c.num),
                                i(c.typmod),
                                i(c.dims),
                                b(attr(5, "f") == "t"),
                                s(attr(22, "i")),
                                s(attr(23, "x")),
                                s(""),
                                b(c.notnull),
                                b(c.default.is_some() && c.generated.is_empty() || !c.generated.is_empty()),
                                b(false),
                                s(c.identity),
                                s(c.generated),
                                b(false),
                                b(true),
                                i(0),
                                i(type_attr(c.typid, 28).and_then(|v| v.parse().ok()).unwrap_or(0)),
                                Value::Null,
                                Value::Null,
                                Value::Null,
                                Value::Null,
                                Value::Null,
                            ],
                        );
                    }
                    if r.kind == 'r' || r.kind == 'm' {
                        for (num, name, typ, len, align) in [(-1, "ctid", 27, 6, "s"), (-2, "xmin", 28, 4, "i"), (-3, "cmin", 29, 4, "i"), (-4, "xmax", 28, 4, "i"), (-5, "cmax", 29, 4, "i"), (-6, "tableoid", 26, 4, "i")] {
                            push(&mut t, vec![i(r.oid), s(name), i(typ), i(len), i(num), i(-1), i(0), b(num != -1), s(align), s("p"), s(""), b(true), b(false), b(false), s(""), s(""), b(false), b(true), i(0), i(0), Value::Null, Value::Null, Value::Null, Value::Null, Value::Null]);
                        }
                    }
                }
            }
            "pg_catalog.pg_type" => {
                push_static(&mut t, type_rows());
                for (oid, name, kind) in &m.user_types {
                    let mut vals: Vec<Value> = vec![Value::Null; t.columns.len()];
                    vals[0] = i(*oid);
                    vals[1] = s(name);
                    vals[2] = i(NS_PUBLIC);
                    vals[3] = i(10);
                    vals[4] = i(if *kind == 'e' { 4 } else { -1 });
                    vals[5] = b(*kind == 'e');
                    vals[6] = s(kind.to_string());
                    vals[7] = s(if *kind == 'e' { "E" } else { "S" });
                    vals[8] = b(false);
                    vals[9] = b(true);
                    vals[10] = s(",");
                    vals[11] = i(0);
                    vals[12] = s("-");
                    vals[13] = i(0);
                    vals[14] = i(0);
                    for k in 15..22 {
                        vals[k] = s("-");
                    }
                    vals[22] = s("i");
                    vals[23] = s("p");
                    vals[24] = b(false);
                    vals[25] = i(0);
                    vals[26] = i(-1);
                    vals[27] = i(0);
                    vals[28] = i(0);
                    push(&mut t, vals);
                }
            }
            "pg_catalog.pg_index" => {
                for x in &m.idxs {
                    let n = x.keys.len() as i64;
                    let exprs = (!x.exprs.is_empty()).then(|| x.exprs.join(", "));
                    push(
                        &mut t,
                        vec![
                            i(x.oid), i(x.rel), i(n), i(n), b(x.unique), b(x.nnd), b(x.primary), b(false), b(true), b(false), b(true), b(false), b(true), b(true), b(false),
                            vector(&x.keys), vector(&vec![0; x.keys.len()]), vector(&vec![1978; x.keys.len()]), vector(&vec![0; x.keys.len()]), opt(&exprs), opt(&x.pred),
                        ],
                    );
                }
            }
            "pg_catalog.pg_indexes" => {
                for x in &m.idxs {
                    let tname = m.relname(x.rel).unwrap_or_default();
                    push(&mut t, vec![s("public"), s(tname), s(&x.name), Value::Null, s(&x.def)]);
                }
            }
            "pg_catalog.pg_constraint" => {
                for c in &m.cons {
                    let nonempty = |v: &[i64]| if v.is_empty() { Value::Null } else { int_array(v) };
                    let fk = c.contype == 'f';
                    let ops = |n: usize| if fk { int_array(&vec![96; n]) } else { Value::Null };
                    push(
                        &mut t,
                        vec![
                            i(c.oid),
                            s(&c.name),
                            i(NS_PUBLIC),
                            s(c.contype.to_string()),
                            b(c.deferrable),
                            b(c.deferred),
                            b(true),
                            b(true),
                            i(c.rel),
                            i(0),
                            i(c.indid),
                            i(0),
                            i(c.frel),
                            s(c.upd.to_string()),
                            s(c.del.to_string()),
                            s(if fk { "s" } else { " " }),
                            b(true),
                            i(0),
                            b(c.contype == 'n' && false),
                            b(false),
                            nonempty(&c.cols),
                            if fk { nonempty(&c.fcols) } else { Value::Null },
                            ops(c.cols.len()),
                            ops(c.cols.len()),
                            ops(c.cols.len()),
                            Value::Null,
                            Value::Null,
                            opt(&c.check),
                        ],
                    );
                }
            }
            "pg_catalog.pg_class" => {
                for r in &m.rels {
                    let (am, replident) = match r.kind {
                        'r' | 'm' => (2, "d"),
                        'S' => (0, "n"),
                        _ => (0, "n"),
                    };
                    let natts = r.cols.len() as i64;
                    push(
                        &mut t,
                        vec![
                            i(r.oid), s(&r.name), i(NS_PUBLIC), i(0), i(0), i(10), i(am), i(if r.kind == 'v' { 0 } else { r.oid }), i(0), i(0), Value::Float(-1.0), i(0), i(0), i(0),
                            b(r.has_index), b(false), s(if r.temp { "t" } else { "p" }), s(r.kind.to_string()), i(if r.kind == 'S' { 3 } else { natts }), i(r.checks), b(r.kind == 'v'),
                            b(r.has_triggers), b(false), b(false), b(false), b(true), s(replident), b(false), i(0),
                            if r.kind == 'r' || r.kind == 'm' { s("744") } else { s("0") }, if r.kind == 'r' || r.kind == 'm' { s("1") } else { s("0") }, Value::Null, Value::Null, Value::Null,
                        ],
                    );
                }
                for x in &m.idxs {
                    push(
                        &mut t,
                        vec![
                            i(x.oid), s(&x.name), i(NS_PUBLIC), i(0), i(0), i(10), i(403), i(x.oid), i(0), i(1), Value::Float(0.0), i(0), i(0), i(0), b(false), b(false), s("p"), s("i"),
                            i(x.keys.len() as i64), i(0), b(false), b(false), b(false), b(false), b(false), b(true), s("n"), b(false), i(0), s("0"), s("0"), Value::Null, Value::Null, Value::Null,
                        ],
                    );
                }
            }
            "pg_catalog.pg_namespace" => {
                push_static(&mut t, &parse_rows(data::PG_NAMESPACE));
                for (o, n) in &m.schemas {
                    push(&mut t, vec![i(*o), s(n), i(10), Value::Null]);
                }
            }
            "pg_catalog.pg_proc" => {
                push_static(&mut t, proc_rows());
                for p in &m.procs {
                    let chars = |v: &Option<Vec<char>>| v.as_ref().map(|v| s(format!("{{{}}}", v.iter().map(|c| c.to_string()).collect::<Vec<_>>().join(",")))).unwrap_or(Value::Null);
                    let names = p.names.as_ref().map(|v| s(format!("{{{}}}", v.iter().map(|n| if n.is_empty() { "\"\"".to_string() } else { n.clone() }).collect::<Vec<_>>().join(",")))).unwrap_or(Value::Null);
                    push(
                        &mut t,
                        vec![
                            i(p.oid), s(&p.name), i(NS_PUBLIC), i(10), i(p.lang), Value::Float(100.0), Value::Float(if p.retset { 1000.0 } else { 0.0 }), i(0), s("-"),
                            s(p.kind.to_string()), b(false), b(false), b(p.strict), b(p.retset), s("v"), s("u"), i(p.nargs), i(p.ndefaults), i(p.rettype), vector(&p.argtypes),
                            p.allargs.as_ref().map(|v| int_array(v)).unwrap_or(Value::Null), chars(&p.modes), names, Value::Null, Value::Null, s(&p.src), Value::Null, Value::Null, Value::Null, Value::Null,
                        ],
                    );
                }
            }
            "pg_catalog.pg_settings" => {
                for r in settings_rows() {
                    let mut vals: Vec<Value> = t.columns.iter().enumerate().map(|(k, c)| typed(c, r.get(k).and_then(|x| x.as_deref()))).collect();
                    // giá trị engine đang dùng (SET của phiên, hoặc mặc định riêng của engine như `server_version`)
                    if let Some(name) = r[0].as_deref() {
                        let lname = name.to_lowercase();
                        if let Some(cur) = self.settings.get(&lname) {
                            vals[1] = s(cur);
                            vals[8] = s("session");
                        } else if let Some(cur) = if lname == "timezone" { Some(super::executor::default_tz()) } else { super::plpgsql::default_setting(&lname) } {
                            vals[1] = s(cur);
                        }
                    }
                    push(&mut t, vals);
                }
            }
            "pg_catalog.pg_database" => {
                let rows = parse_rows(data::PG_DATABASE);
                push_static(&mut t, &rows);
            }
            "pg_catalog.pg_extension" => {
                push_static(&mut t, &parse_rows(data::PG_EXTENSION));
            }
            "pg_catalog.pg_stat_activity" => {
                let now = super::executor::get_current_timestamp();
                push(
                    &mut t,
                    vec![
                        i(5), s("postgres"), i(std::process::id() as i64), Value::Null, i(10), s("postgres"), s(self.setting_value("application_name").unwrap_or_default()), Value::Null, Value::Null,
                        Value::Null, s(&now), if self.storage.in_transaction { s(&now) } else { Value::Null }, s(&now), s(&now), Value::Null, Value::Null, s("active"), Value::Null, Value::Null,
                        Value::Null, s(CURRENT_SQL.with(|c| c.borrow().clone())), s("client backend"),
                    ],
                );
            }
            "pg_catalog.pg_roles" => push_static(&mut t, &parse_rows(data::PG_ROLES)),
            "pg_catalog.pg_user" => push_static(&mut t, &parse_rows(data::PG_USER)),
            "pg_catalog.pg_attrdef" => {
                for r in &m.rels {
                    for c in r.cols.iter().filter(|c| c.default.is_some()) {
                        push(&mut t, vec![i(r.oid * 1000 + c.num), i(r.oid), i(c.num), opt(&c.default)]);
                    }
                }
            }
            "pg_catalog.pg_trigger" => {
                for tg in &m.trigs {
                    push(
                        &mut t,
                        vec![
                            i(tg.oid), i(tg.rel), i(0), s(&tg.name), i(tg.fnoid), i(tg.tgtype), s(if tg.enabled { "O" } else { "D" }), b(false), i(0), i(0), i(0), b(false), b(false), i(0),
                            s(""), s("\\x"), opt(&tg.when), Value::Null, Value::Null,
                        ],
                    );
                }
            }
            "pg_catalog.pg_description" => {
                for r in &m.rels {
                    if let Some(c) = &r.comment {
                        push(&mut t, vec![i(r.oid), i(1259), i(0), s(c)]);
                    }
                    for c in &r.cols {
                        if let Some(cm) = &c.comment {
                            push(&mut t, vec![i(r.oid), i(1259), i(c.num), s(cm)]);
                        }
                    }
                }
            }
            "pg_catalog.pg_am" => push_static(&mut t, &parse_rows(data::PG_AM)),
            "pg_catalog.pg_sequence" => {
                for r in m.rels.iter().filter(|r| r.kind == 'S') {
                    let st = self.storage.objects.sequences.get(&r.name);
                    let (start, inc, max, min, cycle) = match st {
                        Some(q) => (q.spec.start, q.spec.increment, q.spec.max, q.spec.min, q.spec.cycle),
                        None => (1, 1, 2147483647, 1, false),
                    };
                    push(&mut t, vec![i(r.oid), i(if max > i32::MAX as i64 { 20 } else { 23 }), i(start), i(inc), i(max), i(min), i(1), b(cycle)]);
                }
            }
            "information_schema.referential_constraints" => {
                for c in m.cons.iter().filter(|c| c.contype == 'f') {
                    let uname = m.cons.iter().find(|u| u.rel == c.frel && (u.contype == 'p' || u.contype == 'u') && u.cols == c.fcols).map(|u| u.name.clone());
                    push(&mut t, vec![s("postgres"), s("public"), s(&c.name), uname.as_ref().map(|_| s("postgres")).unwrap_or(Value::Null), uname.as_ref().map(|_| s("public")).unwrap_or(Value::Null), opt(&uname), s("NONE"), s(fk_rule(c.upd)), s(fk_rule(c.del))]);
                }
            }
            "information_schema.check_constraints" => {
                for c in m.cons.iter().filter(|c| c.contype == 'c' || c.contype == 'n') {
                    let clause = match c.contype {
                        'n' => format!("{} IS NOT NULL", m.rel(c.rel).and_then(|r| r.cols.iter().find(|x| Some(&x.num) == c.cols.first())).map(|x| x.name.clone()).unwrap_or_default()),
                        _ => c.def.trim_start_matches("CHECK ").to_string(),
                    };
                    push(&mut t, vec![s("postgres"), s("public"), s(&c.name), s(clause)]);
                }
            }
            "information_schema.triggers" => {
                for tg in &m.trigs {
                    for e in &tg.events {
                        push(
                            &mut t,
                            vec![
                                s("postgres"), s("public"), s(&tg.name), s(e), s("postgres"), s("public"), s(&tg.table), i(1), opt(&tg.when.as_ref().map(|w| format!("({})", w))),
                                s(format!("EXECUTE FUNCTION {}()", tg.function)), s(if tg.row { "ROW" } else { "STATEMENT" }), s(&tg.timing), Value::Null, Value::Null, Value::Null, Value::Null, Value::Null,
                            ],
                        );
                    }
                }
            }
            "information_schema.routines" => {
                for p in &m.procs {
                    let mut vals = vec![Value::Null; t.columns.len()];
                    let spec = format!("{}_{}", p.name, p.oid);
                    for (k, v) in [(0, s("postgres")), (1, s("public")), (2, s(&spec)), (3, s("postgres")), (4, s("public")), (5, s(&p.name)), (6, s(if p.kind == 'p' { "PROCEDURE" } else { "FUNCTION" }))] {
                        vals[k] = v;
                    }
                    if p.kind != 'p' {
                        vals[13] = s(if p.rettype == 2249 { "record".to_string() } else { format_type(p.rettype, None) });
                        vals[28] = s("postgres");
                        vals[29] = s("pg_catalog");
                        vals[30] = s(type_attr(p.rettype, 1).unwrap_or("record"));
                    }
                    vals[35] = s("0");
                    vals[36] = s(if p.language == "sql" { "SQL" } else { "EXTERNAL" });
                    vals[37] = s(&p.src);
                    vals[39] = s(p.language.to_uppercase());
                    vals[40] = s("GENERAL");
                    vals[41] = s("NO");
                    vals[42] = s("MODIFIES");
                    vals[43] = s(if p.strict { "YES" } else { "NO" });
                    vals[45] = s("YES");
                    vals[46] = i(0);
                    vals[47] = s("NO");
                    vals[48] = s("NO");
                    vals[49] = s("INVOKER");
                    vals[53] = s("NO");
                    vals[56] = s("NO");
                    vals[57] = s("NO");
                    let _ = &p.ret_name;
                    push(&mut t, vals);
                }
            }
            "information_schema.parameters" => {
                for p in &m.procs {
                    let spec = format!("{}_{}", p.name, p.oid);
                    let types = p.allargs.clone().unwrap_or_else(|| p.argtypes.clone());
                    let modes = p.modes.clone().unwrap_or_else(|| vec!['i'; types.len()]);
                    let names = p.names.clone().unwrap_or_default();
                    for (k, ty) in types.iter().enumerate() {
                        if modes.get(k) == Some(&'t') {
                            continue;
                        }
                        let mut vals = vec![Value::Null; t.columns.len()];
                        vals[0] = s("postgres");
                        vals[1] = s("public");
                        vals[2] = s(&spec);
                        vals[3] = i(k as i64 + 1);
                        vals[4] = s(match modes.get(k) { Some('o') => "OUT", Some('b') => "INOUT", _ => "IN" });
                        vals[5] = s("NO");
                        vals[6] = s("NO");
                        vals[7] = names.get(k).filter(|n| !n.is_empty()).map(s).unwrap_or(Value::Null);
                        vals[8] = s(format_type(*ty, None));
                        if let Some(c) = t.columns.iter().position(|c| c.name == "udt_name") {
                            vals[c] = s(type_attr(*ty, 1).unwrap_or("text"));
                            vals[c - 1] = s("pg_catalog");
                            vals[c - 2] = s("postgres");
                        }
                        push(&mut t, vals);
                    }
                }
                let _ = &m.procs.iter().map(|p| &p.args_text).count();
            }
            _ => {}
        }
        t
    }

    /// SELECT trên catalog (xem đầu file). `None` = câu không thuộc diện; `Some(Err)` = lỗi (nơi gọi quay về
    /// đường cũ).
    pub(crate) fn catalog_select(&mut self, sql: &str, params: &[Value]) -> Option<Result<QueryResult, String>> {
        let used = used_relations(sql);
        CURRENT_SQL.with(|c| *c.borrow_mut() = sql.to_string());
        let model = Rc::new(self.catalog_model());
        let mut q = sql.to_string();
        // `'tên'::regclass` / `to_regclass('tên')`: tên khi là cột trần của SELECT, OID ở mọi chỗ khác
        q = match rewrite_reg_casts(&q, &model) {
            Ok(q) => q,
            Err(e) => return Some(Err(e)),
        };
        if contains_ci(&q, "information_schema.table_constraints") || contains_ci(&q, "information_schema.key_column_usage") || contains_ci(&q, "information_schema.constraint_column_usage") {
            self.ensure_virtual_catalog_tables();
        }
        let mut tables = Vec::new();
        for &k in &used {
            let (full, tmp, bare) = RELS[k];
            let short = full.rsplit('.').next().unwrap();
            let pats: Vec<&str> = if bare { vec![full, short] } else { vec![full] };
            q = replace_relation_name(&q, &pats, tmp);
            tables.push((tmp, self.build_catalog_table(k, &model)));
        }
        for (name, t) in tables {
            self.storage.tables.insert(name.to_string(), t);
        }
        MODEL.with(|m| *m.borrow_mut() = Some(model));
        let _guard = ModelGuard;
        let res = self.execute_statement(&q, params);
        for &k in &used {
            self.storage.tables.remove(RELS[k].1);
        }
        Some(res)
    }
}

/// Biểu thức (vừa viết xong vào `out`, phần sau là `after`) là một mục trần của danh sách SELECT.
fn bare_select_item(out: &str, after: &str) -> bool {
    let before = out.trim_end().to_ascii_lowercase();
    let after = after.trim_start();
    (before.ends_with("select") || before.ends_with(',')) && (after.is_empty() || after.starts_with(',') || after.starts_with("as ") || after.starts_with("from ") || after.starts_with(';'))
}

/// `'x'::regclass[::oid]`, `to_regclass('x')`, `'x'::regtype`, `'x'::regnamespace`; `cột::regclass` → tên.
fn rewrite_reg_casts(sql: &str, m: &Model) -> Result<String, String> {
    let mut out = String::with_capacity(sql.len());
    let b = sql.as_bytes();
    let mut i = 0;
    let lower = sql.to_ascii_lowercase();
    while i < b.len() {
        // literal '...'
        if b[i] == b'\'' {
            let mut j = i + 1;
            while j < b.len() {
                if b[j] == b'\'' {
                    if j + 1 < b.len() && b[j + 1] == b'\'' {
                        j += 2;
                        continue;
                    }
                    break;
                }
                j += 1;
            }
            let lit_end = (j + 1).min(b.len());
            let lit = sql[i + 1..j.min(b.len())].replace("''", "'");
            let rest = &lower[lit_end..];
            let cast = ["::regclass", "::regtype", "::regnamespace", "::regproc"].iter().find(|c| rest.starts_with(**c)).copied();
            if let Some(c) = cast {
                let mut end = lit_end + c.len();
                let to_oid = lower[end..].starts_with("::oid") || lower[end..].starts_with("::int");
                if lower[end..].starts_with("::oid") {
                    end += 5;
                }
                // in tên (như kiểu regclass của PG) khi là một mục trần của danh sách SELECT hoặc ép tiếp sang text;
                // còn lại (đối số hàm, so sánh với cột oid) dùng OID
                let to_text = lower[end..].starts_with("::text") || lower[end..].starts_with("::varchar");
                let as_oid = !(bare_select_item(&out, &lower[end..]) || to_text);
                let oid = match c {
                    "::regclass" => Some(m.regclass(&lit).ok_or_else(|| format!("relation \"{}\" does not exist", lit))?),
                    "::regtype" => {
                        let o = type_oid_of_name(&lit, &HashMap::new());
                        Some(o)
                    }
                    "::regnamespace" => Some(m.nsp_oid(&lit).ok_or_else(|| format!("schema \"{}\" does not exist", lit))?),
                    _ => None,
                };
                match oid {
                    Some(o) if as_oid || to_oid => out.push_str(&o.to_string()),
                    Some(o) if c == "::regtype" => out.push_str(&format!("'{}'", format_type(o, None))),
                    _ => out.push_str(&format!("'{}'", lit.trim_matches('"').replace('\'', "''"))),
                }
                i = end;
                continue;
            }
            out.push_str(&sql[i..lit_end]);
            i = lit_end;
            continue;
        }
        // to_regclass('x') / to_regnamespace('x') / to_regtype('x')
        let at_word = i == 0 || !(b[i - 1].is_ascii_alphanumeric() || b[i - 1] == b'_');
        if at_word {
            let mut replaced = false;
            for (f, kind) in [("to_regclass(", 0), ("to_regnamespace(", 1), ("to_regtype(", 2)] {
                if lower[i..].starts_with(f) {
                    if let Some(close) = super::constraints::matching_paren(sql, i + f.len() - 1) {
                        let arg = sql[i + f.len()..close].trim();
                        if arg.len() >= 2 && arg.starts_with('\'') && arg.ends_with('\'') {
                            let name = arg[1..arg.len() - 1].replace("''", "'");
                            let found = match kind {
                                0 => m.regclass(&name).map(|o| (o, m.relname(o).unwrap_or(name.clone()))),
                                1 => m.nsp_oid(&name).map(|o| (o, name.to_lowercase())),
                                _ => DeclType::parse(&name).map(|d| type_oid_of_decl(&d, &HashMap::new())).filter(|o| *o != 25 || name.eq_ignore_ascii_case("text")).map(|o| (o, format_type(o, None))),
                            };
                            let to_oid = lower[close + 1..].starts_with("::oid");
                            let as_oid = !bare_select_item(&out, &lower[close + 1..]);
                            match found {
                                Some((o, _)) if as_oid || to_oid => out.push_str(&o.to_string()),
                                Some((_, n)) => out.push_str(&format!("'{}'", n.replace('\'', "''"))),
                                None => out.push_str("NULL"),
                            }
                            i = close + 1;
                            if to_oid {
                                i += 5;
                            }
                            replaced = true;
                            break;
                        }
                    }
                }
            }
            if replaced {
                continue;
            }
        }
        // cột::regclass / ::regtype / ::regnamespace (không phải literal): đổi thành hàm tra tên
        if b[i] == b':' && lower[i..].starts_with("::reg") {
            let c = ["::regclass", "::regtype", "::regnamespace", "::regproc"].iter().find(|c| lower[i..].starts_with(**c)).copied();
            if let Some(c) = c {
                // biểu thức trước `::` là định danh / tham chiếu cột (a.b) hoặc nhóm (...)
                let mut st = out.len();
                let ob = out.as_bytes();
                if st > 0 && ob[st - 1] == b')' {
                    let mut depth = 0i32;
                    let mut k = st;
                    while k > 0 {
                        k -= 1;
                        match ob[k] {
                            b')' => depth += 1,
                            b'(' => {
                                depth -= 1;
                                if depth == 0 {
                                    break;
                                }
                            }
                            _ => {}
                        }
                    }
                    st = k;
                    while st > 0 && (ob[st - 1].is_ascii_alphanumeric() || ob[st - 1] == b'_') {
                        st -= 1;
                    }
                } else {
                    while st > 0 && (ob[st - 1].is_ascii_alphanumeric() || ob[st - 1] == b'_' || ob[st - 1] == b'.' || ob[st - 1] == b'"') {
                        st -= 1;
                    }
                }
                let expr = out[st..].to_string();
                out.truncate(st);
                let f = match c {
                    "::regclass" => "pg_cat_relname",
                    "::regtype" => "pg_cat_typname",
                    "::regnamespace" => {
                        // `tc.table_schema::regnamespace` (tên → oid) hoặc `oid::regnamespace` (oid → tên)
                        "pg_cat_nsp"
                    }
                    _ => "pg_cat_procname",
                };
                let mut end = i + c.len();
                if lower[end..].starts_with("::oid") {
                    // `x::regclass::oid` = chính x (oid) hoặc oid của tên
                    out.push_str(&format!("pg_cat_reloid({})", expr));
                    end += 5;
                } else {
                    out.push_str(&format!("{}({})", f, expr));
                }
                i = end;
                continue;
            }
        }
        let ch = sql[i..].chars().next().unwrap();
        out.push(ch);
        i += ch.len_utf8();
    }
    Ok(out)
}

// ---------------------------------------------------------------------------------------------------------------
// Hàm (gọi từ fnlib)

pub(crate) fn is_catalog_fn(name: &str) -> bool {
    // gọi ở mỗi lần đánh giá hàm: loại nhanh theo chữ đầu
    if !matches!(name.as_bytes().first(), Some(b'P') | Some(b'F')) || name.len() < 10 {
        return false;
    }
    matches!(
        name,
        "FORMAT_TYPE" | "PG_GET_INDEXDEF" | "PG_GET_CONSTRAINTDEF" | "PG_GET_EXPR" | "PG_GET_USERBYID" | "PG_TABLE_IS_VISIBLE" | "PG_TYPE_IS_VISIBLE"
            | "PG_FUNCTION_IS_VISIBLE" | "PG_GET_VIEWDEF" | "PG_ENCODING_TO_CHAR" | "PG_CAT_RELNAME" | "PG_CAT_TYPNAME" | "PG_CAT_NSP" | "PG_CAT_PROCNAME"
            | "PG_CAT_RELOID" | "PG_GET_TRIGGERDEF" | "PG_GET_FUNCTION_ARGUMENTS" | "PG_GET_FUNCTION_RESULT" | "PG_GET_FUNCTIONDEF"
    )
}

pub(crate) fn catalog_fn_type(name: &str) -> Option<&'static str> {
    Some(match name {
        "PG_TABLE_IS_VISIBLE" | "PG_TYPE_IS_VISIBLE" | "PG_FUNCTION_IS_VISIBLE" => "boolean",
        "PG_GET_USERBYID" | "PG_ENCODING_TO_CHAR" => "name",
        "PG_CAT_RELOID" => "oid",
        n if is_catalog_fn(n) => "text",
        _ => return None,
    })
}

fn arg_i64(v: Option<&Value>) -> Option<i64> {
    match v? {
        Value::Null => None,
        x => x.as_i64(),
    }
}

pub(crate) fn call_catalog_fn(name: &str, args: &[Value]) -> Option<Value> {
    if args.first().is_some_and(|a| a.is_null()) && name != "FORMAT_TYPE" {
        return Some(Value::Null);
    }
    let text_or_null = |v: Option<String>| v.map(Value::text).unwrap_or(Value::Null);
    Some(match name {
        "FORMAT_TYPE" => match arg_i64(args.first()) {
            None => Value::Null,
            Some(o) => Value::text(format_type(o, arg_i64(args.get(1)))),
        },
        "PG_GET_USERBYID" => Value::text(if arg_i64(args.first()) == Some(10) { "postgres".to_string() } else { format!("unknown (OID={})", arg_i64(args.first()).unwrap_or(0)) }),
        "PG_TABLE_IS_VISIBLE" | "PG_TYPE_IS_VISIBLE" | "PG_FUNCTION_IS_VISIBLE" => Value::Bool(true),
        "PG_ENCODING_TO_CHAR" => Value::text(if arg_i64(args.first()) == Some(6) { "UTF8" } else { "" }),
        "PG_GET_EXPR" => args.first().cloned().unwrap_or(Value::Null),
        "PG_GET_INDEXDEF" => {
            let o = arg_i64(args.first())?;
            text_or_null(with_model(|m| m.idxs.iter().find(|x| x.oid == o).map(|x| x.def.clone())).flatten())
        }
        "PG_GET_CONSTRAINTDEF" => {
            let o = arg_i64(args.first())?;
            text_or_null(with_model(|m| m.cons.iter().find(|x| x.oid == o).map(|x| x.def.clone())).flatten())
        }
        "PG_GET_VIEWDEF" => {
            let o = match args.first()? {
                Value::Text(t) if t.parse::<i64>().is_err() => with_model(|m| m.regclass(t)).flatten()?,
                v => v.as_i64()?,
            };
            text_or_null(with_model(|m| m.rel(o).and_then(|r| r.view_def.clone()).map(|d| format!(" {};", d.trim()))).flatten())
        }
        "PG_GET_TRIGGERDEF" => {
            let o = arg_i64(args.first())?;
            text_or_null(
                with_model(|m| {
                    m.trigs.iter().find(|t| t.oid == o).map(|t| {
                        format!(
                            "CREATE TRIGGER {} {} {} ON public.{} FOR EACH {}{} EXECUTE FUNCTION {}()",
                            t.name,
                            t.timing,
                            t.events.join(" OR "),
                            t.table,
                            if t.row { "ROW" } else { "STATEMENT" },
                            t.when.as_ref().map(|w| format!(" WHEN ({})", w)).unwrap_or_default(),
                            t.function
                        )
                    })
                })
                .flatten(),
            )
        }
        "PG_GET_FUNCTION_ARGUMENTS" => {
            let o = arg_i64(args.first())?;
            text_or_null(with_model(|m| m.procs.iter().find(|p| p.oid == o).map(|p| p.args_text.clone())).flatten())
        }
        "PG_GET_FUNCTION_RESULT" => {
            let o = arg_i64(args.first())?;
            text_or_null(with_model(|m| m.procs.iter().find(|p| p.oid == o).filter(|p| p.kind != 'p').map(|p| format!("{}{}", if p.retset { "SETOF " } else { "" }, if p.rettype == 2249 { "record".into() } else { format_type(p.rettype, None) }))).flatten())
        }
        "PG_GET_FUNCTIONDEF" => {
            let o = arg_i64(args.first())?;
            text_or_null(
                with_model(|m| {
                    m.procs.iter().find(|p| p.oid == o).map(|p| {
                        format!(
                            "CREATE OR REPLACE {} public.{}({})\n{} LANGUAGE {}\nAS $function${}$function$\n",
                            if p.kind == 'p' { "PROCEDURE" } else { "FUNCTION" },
                            p.name,
                            p.args_text,
                            if p.kind == 'p' { String::new() } else { format!(" RETURNS {}{}\n", if p.retset { "SETOF " } else { "" }, if p.rettype == 2249 { "record".into() } else { format_type(p.rettype, None) }) },
                            p.language,
                            p.src
                        )
                    })
                })
                .flatten(),
            )
        }
        "PG_CAT_RELNAME" => {
            let o = arg_i64(args.first());
            match o {
                Some(o) => text_or_null(with_model(|m| m.relname(o)).flatten().or_else(|| Some(o.to_string()))),
                None => args.first().cloned().unwrap_or(Value::Null),
            }
        }
        "PG_CAT_RELOID" => match args.first()? {
            Value::Text(t) if t.parse::<i64>().is_err() => with_model(|m| m.regclass(t)).flatten().map(Value::Int).unwrap_or(Value::Null),
            v => v.as_i64().map(Value::Int).unwrap_or(Value::Null),
        },
        "PG_CAT_TYPNAME" => match arg_i64(args.first()) {
            Some(o) => Value::text(format_type(o, None)),
            None => Value::Null,
        },
        "PG_CAT_NSP" => match args.first()? {
            Value::Text(t) if t.parse::<i64>().is_err() => with_model(|m| m.nsp_oid(t)).flatten().map(Value::Int).unwrap_or(Value::Null),
            v => {
                let o = v.as_i64()?;
                Value::text(match o {
                    11 => "pg_catalog".to_string(),
                    99 => "pg_toast".to_string(),
                    2200 => "public".to_string(),
                    12424 => "information_schema".to_string(),
                    _ => with_model(|m| m.schemas.iter().find(|(x, _)| *x == o).map(|(_, n)| n.clone())).flatten().unwrap_or_else(|| o.to_string()),
                })
            }
        },
        "PG_CAT_PROCNAME" => match arg_i64(args.first()) {
            Some(o) => text_or_null(
                with_model(|m| m.procs.iter().find(|p| p.oid == o).map(|p| p.name.clone()))
                    .flatten()
                    .or_else(|| proc_rows().iter().find(|r| r[0].as_deref() == Some(&o.to_string())).and_then(|r| r[1].clone())),
            ),
            None => args.first().cloned().unwrap_or(Value::Null),
        },
        _ => return None,
    })
}
