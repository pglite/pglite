//! Kiểm tra: (1) file DB tạo bởi code cũ vẫn mở và đọc đúng, (2) API giao thức
//! Postgres (`query_wire` / `describe`) trả đúng kiểu, thứ tự cột và định dạng text.
use pglite_rs::{PGlite, Value};
use std::path::{Path, PathBuf};

fn temp_dir(tag: &str) -> PathBuf {
    let nanos = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos();
    let dir = std::env::temp_dir().join(format!("pglite_wire_{}_{}_{}", tag, std::process::id(), nanos));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

/// Chép mọi file WAL của `src_db` sang thư mục tạm; không bao giờ mở file gốc.
fn copy_db(src_db: &Path, dst_dir: &Path) -> String {
    let dst_db = dst_dir.join(src_db.file_name().unwrap());
    for suffix in ["", ".rwal", ".v2.rwal", ".wal"] {
        let src = PathBuf::from(format!("{}{}", src_db.display(), suffix));
        if src.is_file() {
            std::fs::copy(&src, format!("{}{}", dst_db.display(), suffix)).unwrap();
        }
    }
    dst_db.to_str().unwrap().to_string()
}

fn fixture_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures")
}

const SNAPSHOT_QUERIES: &[(&str, &str)] = &[
    ("schools", "SELECT * FROM schools ORDER BY id"),
    ("schedules", "SELECT * FROM schedules ORDER BY id"),
];

#[test]
fn legacy_wal_fixture_opens_with_identical_data() {
    let dir = temp_dir("legacy");
    let db_path = copy_db(&fixture_dir().join("legacy_v2.db"), &dir);
    let expected: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(fixture_dir().join("legacy_v2.expected.json")).unwrap()).unwrap();

    {
        let mut db = PGlite::open(&db_path).unwrap();
        for (key, sql) in SNAPSHOT_QUERIES {
            let rows = serde_json::Value::Array(db.query(sql, &[]).unwrap().rows);
            assert_eq!(rows, expected[*key], "dữ liệu bảng {key} khác với snapshot của code cũ");
        }
        // Ghi thêm bằng code mới trên cùng file
        db.exec("INSERT INTO schools (name, active) VALUES ('Delta', true)", &[]).unwrap();
        db.exec("ALTER TABLE schedules ADD COLUMN note TEXT", &[]).unwrap();
        db.exec("UPDATE schedules SET note = 'x' WHERE id = 2", &[]).unwrap();
        db.flush();
    }

    // Mở lại lần hai: dữ liệu cũ + mới đều còn
    let mut db = PGlite::open(&db_path).unwrap();
    let schools = db.query("SELECT * FROM schools ORDER BY id", &[]).unwrap();
    assert_eq!(schools.rows.len(), 3);
    assert_eq!(schools.rows[0], expected["schools"][0]);
    assert_eq!(schools.rows[2]["name"], "Delta");
    let sched = db.query("SELECT id, note FROM schedules ORDER BY id", &[]).unwrap();
    assert_eq!(sched.rows[1]["note"], "x");
    let _ = std::fs::remove_dir_all(dir);
}

/// Chạy với một DB thật (luôn làm trên bản sao):
///   PGLITE_LEGACY_DB=/path/to/app.db cargo test --test wire_compat_test real_db -- --nocapture
#[test]
fn real_db_copy_opens_and_every_table_is_readable() {
    let Ok(src) = std::env::var("PGLITE_LEGACY_DB") else {
        return;
    };
    let dir = temp_dir("real");
    let db_path = copy_db(Path::new(&src), &dir);
    let mut db = PGlite::open(&db_path).unwrap();
    let tables = db.query("SELECT table_name FROM information_schema.tables", &[]).unwrap();
    println!("{src}: {} bảng", tables.rows.len());
    for t in &tables.rows {
        let name = t["table_name"].as_str().unwrap();
        let old = db.query(&format!("SELECT * FROM \"{name}\""), &[]).unwrap();
        let wire = db.query_wire(&format!("SELECT * FROM \"{name}\""), &[]).unwrap();
        assert_eq!(old.rows.len(), wire.rows.len(), "bảng {name}");
        println!("{name}: {} dòng, {} cột", wire.rows.len(), wire.fields.len());
    }
    let _ = std::fs::remove_dir_all(dir);
}

fn setup() -> PGlite {
    let mut db = PGlite::in_memory().unwrap();
    for sql in [
        "CREATE TABLE schools (id SERIAL PRIMARY KEY, name VARCHAR(100) NOT NULL, active BOOLEAN DEFAULT true, rating NUMERIC, meta JSONB, created_at TIMESTAMPTZ, opened DATE, code UUID, logo BYTEA, tags TEXT[], class_ids TEXT, big BIGINT)",
        "CREATE TABLE schedules (id SERIAL PRIMARY KEY, school_id INTEGER, title TEXT, start_at TIMESTAMP)",
        "INSERT INTO schools (name, active, rating, meta, created_at, opened, code, logo, tags, class_ids, big) VALUES ('Alpha', true, 4.5, '{\"a\":1}', '2026-10-12 08:00:00', '2026-01-02', 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11', '\\x0102', '{x,y}', '[7831,7832]', 9000000000)",
        "INSERT INTO schools (name, active) VALUES ('Beta', false)",
        "INSERT INTO schedules (school_id, title, start_at) VALUES (1, 'Math', '2026-10-12 08:00:00')",
    ] {
        db.exec(sql, &[]).unwrap_or_else(|e| panic!("{sql}: {e}"));
    }
    db
}

fn names(res: &pglite_rs::WireResult) -> Vec<&str> {
    res.fields.iter().map(|f| f.name.as_str()).collect()
}

fn oids(res: &pglite_rs::WireResult) -> Vec<u32> {
    res.fields.iter().map(|f| f.type_oid).collect()
}

#[test]
fn wire_select_star_types_and_text_format() {
    let mut db = setup();
    let res = db.query_wire("SELECT * FROM schools WHERE id = 1", &[]).unwrap();
    assert_eq!(
        names(&res),
        vec!["id", "name", "active", "rating", "meta", "created_at", "opened", "code", "logo", "tags", "class_ids", "big"]
    );
    assert_eq!(oids(&res), vec![23, 25, 16, 1700, 3802, 1184, 1082, 2950, 17, 1009, 25, 20]);
    let row = &res.rows[0];
    assert_eq!(row[0].as_deref(), Some("1"));
    assert_eq!(row[2].as_deref(), Some("t"));
    assert_eq!(row[3].as_deref(), Some("4.5"));
    assert_eq!(row[4].as_deref(), Some("{\"a\":1}"));
    assert_eq!(row[5].as_deref(), Some("2026-10-12 08:00:00+00"));
    assert_eq!(row[6].as_deref(), Some("2026-01-02"));
    assert_eq!(row[8].as_deref(), Some("\\x0102"));
    assert_eq!(row[9].as_deref(), Some("{x,y}"));
    // Cột TEXT giữ nguyên văn như Postgres thật, không bị đoán thành mảng
    assert_eq!(row[10].as_deref(), Some("[7831,7832]"));
    assert_eq!(row[11].as_deref(), Some("9000000000"));
    assert_eq!(res.command, "SELECT 1");

    let beta = db.query_wire("SELECT * FROM schools WHERE name = 'Beta'", &[]).unwrap();
    assert_eq!(beta.rows[0][2].as_deref(), Some("f"));
    assert_eq!(beta.rows[0][3], None);
}

#[test]
fn wire_projection_keeps_select_order_and_types() {
    let mut db = setup();
    let res = db.query_wire("SELECT name, id, active AS ok, big FROM schools ORDER BY id", &[]).unwrap();
    assert_eq!(names(&res), vec!["name", "id", "ok", "big"]);
    assert_eq!(oids(&res), vec![25, 23, 16, 20]);
    assert_eq!(res.rows[0], vec![Some("Alpha".into()), Some("1".into()), Some("t".into()), Some("9000000000".into())]);
    assert_eq!(res.row_count, 2);
}

#[test]
fn wire_pk_lookup_miss_still_has_fields() {
    let mut db = setup();
    let res = db.query_wire("SELECT * FROM schools WHERE id = 999", &[]).unwrap();
    assert!(res.rows.is_empty());
    assert_eq!(res.fields.len(), 12);
}

#[test]
fn wire_join_fields_ordered_typed_and_present_with_zero_rows() {
    let mut db = setup();
    let sql = "SELECT s.title, sc.name AS school_name, s.start_at, sc.id FROM schedules s JOIN schools sc ON sc.id = s.school_id";
    let res = db.query_wire(sql, &[]).unwrap();
    assert_eq!(names(&res), vec!["title", "school_name", "start_at", "id"]);
    assert_eq!(oids(&res), vec![25, 25, 1184, 23]);
    assert_eq!(res.rows.len(), 1);
    assert_eq!(res.rows[0][2].as_deref(), Some("2026-10-12 08:00:00+00"));

    let empty = db.query_wire(&format!("{sql} WHERE s.id = 999"), &[]).unwrap();
    assert!(empty.rows.is_empty());
    assert_eq!(names(&empty), vec!["title", "school_name", "start_at", "id"]);
    assert_eq!(oids(&empty), vec![25, 25, 1184, 23]);
}

#[test]
fn wire_expressions_and_aggregates() {
    let mut db = setup();
    let res = db.query_wire("SELECT COUNT(*) AS n FROM schools", &[]).unwrap();
    assert_eq!(oids(&res), vec![20]);
    assert_eq!(res.rows[0][0].as_deref(), Some("2"));

    let res = db.query_wire("SELECT 1 AS one, 'a' AS txt, true AS b", &[]).unwrap();
    assert_eq!(oids(&res), vec![23, 25, 16]);
    assert_eq!(res.rows[0], vec![Some("1".into()), Some("a".into()), Some("t".into())]);
}

#[test]
fn wire_dml_tags_and_returning() {
    let mut db = setup();
    let res = db.query_wire("INSERT INTO schools (name) VALUES ('Gamma')", &[]).unwrap();
    assert_eq!(res.command, "INSERT 0 1");
    assert!(res.fields.is_empty());

    let res = db.query_wire("INSERT INTO schools (name, active) VALUES ($1, $2) RETURNING id, active", &[Value::text("Eps"), Value::Bool(true)]).unwrap();
    assert_eq!(names(&res), vec!["id", "active"]);
    assert_eq!(oids(&res), vec![23, 16]);
    assert_eq!(res.rows[0][1].as_deref(), Some("t"));

    let res = db.query_wire("UPDATE schools SET active = false WHERE active = true", &[]).unwrap();
    assert!(res.command.starts_with("UPDATE "), "{}", res.command);
    let res = db.query_wire("DELETE FROM schools WHERE name = 'Gamma'", &[]).unwrap();
    assert_eq!(res.command, "DELETE 1");
}

#[test]
fn describe_without_writing() {
    let mut db = setup();
    let d = db.describe("SELECT name, id FROM schools WHERE id = $1").unwrap();
    assert_eq!(d.param_count, 1);
    assert_eq!(d.fields.iter().map(|f| (f.name.as_str(), f.type_oid)).collect::<Vec<_>>(), vec![("name", 25), ("id", 23)]);

    let before = db.query("SELECT COUNT(*) AS n FROM schools", &[]).unwrap().rows[0]["n"].clone();
    let d = db.describe("INSERT INTO schools (name) VALUES ($1) RETURNING id, name").unwrap();
    assert_eq!(d.param_count, 1);
    assert_eq!(d.fields.iter().map(|f| f.type_oid).collect::<Vec<_>>(), vec![23, 25]);
    let after = db.query("SELECT COUNT(*) AS n FROM schools", &[]).unwrap().rows[0]["n"].clone();
    assert_eq!(before, after, "describe không được ghi dữ liệu");
}

#[test]
fn old_api_unchanged_except_key_order() {
    let mut db = setup();
    let res = db.query("SELECT * FROM schools WHERE id = 1", &[]).unwrap();
    let row = &res.rows[0];
    // API cũ vẫn "đoán" như trước để không làm vỡ app hiện tại
    assert_eq!(row["class_ids"], serde_json::json!([7831, 7832]));
    assert_eq!(row["logo"], serde_json::json!([1, 2]));
    assert_eq!(row["active"], serde_json::json!(true));
    // Thứ tự key nay theo đúng thứ tự cột
    let keys: Vec<&String> = row.as_object().unwrap().keys().collect();
    assert_eq!(keys[0], "id");
    assert_eq!(keys[1], "name");
}

/// Bản ghi ghi bằng đường mới (WalRecordRef, zero-copy) phải giống từng byte với
/// bản ghi kiểu cũ (WalRecord), nếu không code cũ/mới sẽ đọc sai WAL của nhau.
#[test]
fn wal_ref_records_are_byte_identical_to_legacy_records() {
    use pglite_rs::storage::wal::{WalRecord, WalRecordRef};
    use pglite_rs::{ColumnDef, DataType};
    let col = ColumnDef { name: "c".into(), data_type: DataType::Array("INT".into()), is_primary_key: false, is_nullable: true, default_value: Some("1".into()), comment: Some("x".into()) };
    let row = vec![Value::Int(1), Value::text("a"), Value::Null, Value::Bool(true), Value::Float(1.5)];
    let pairs: Vec<(Vec<u8>, Vec<u8>)> = vec![
        (bincode::serialize(&WalRecord::Begin).unwrap(), bincode::serialize(&WalRecordRef::Begin).unwrap()),
        (bincode::serialize(&WalRecord::Commit).unwrap(), bincode::serialize(&WalRecordRef::Commit).unwrap()),
        (bincode::serialize(&WalRecord::Rollback).unwrap(), bincode::serialize(&WalRecordRef::Rollback).unwrap()),
        (
            bincode::serialize(&WalRecord::CreateTable { name: "t".into(), columns: vec![col.clone()] }).unwrap(),
            bincode::serialize(&WalRecordRef::CreateTable { name: "t", columns: std::slice::from_ref(&col) }).unwrap(),
        ),
        (bincode::serialize(&WalRecord::DropTable { name: "t".into() }).unwrap(), bincode::serialize(&WalRecordRef::DropTable { name: "t" }).unwrap()),
        (
            bincode::serialize(&WalRecord::Insert { table: "t".into(), row: row.clone() }).unwrap(),
            bincode::serialize(&WalRecordRef::Insert { table: "t", row: &row }).unwrap(),
        ),
        (
            bincode::serialize(&WalRecord::Update { table: "t".into(), pk: 7, col_idx: 2, new_val: Value::text("z") }).unwrap(),
            bincode::serialize(&WalRecordRef::Update { table: "t", pk: 7, col_idx: 2, new_val: &Value::text("z") }).unwrap(),
        ),
        (bincode::serialize(&WalRecord::Delete { table: "t".into(), pk: 7 }).unwrap(), bincode::serialize(&WalRecordRef::Delete { table: "t", pk: 7 }).unwrap()),
        (
            bincode::serialize(&WalRecord::CommentOnTable { table: "t".into(), comment: Some("c".into()) }).unwrap(),
            bincode::serialize(&WalRecordRef::CommentOnTable { table: "t", comment: Some("c") }).unwrap(),
        ),
        (
            bincode::serialize(&WalRecord::CommentOnColumn { table: "t".into(), column: "c".into(), comment: None }).unwrap(),
            bincode::serialize(&WalRecordRef::CommentOnColumn { table: "t", column: "c", comment: None }).unwrap(),
        ),
        (
            bincode::serialize(&WalRecord::AlterTableAddColumn { table: "t".into(), column: col.clone(), default_val: Value::Int(0) }).unwrap(),
            bincode::serialize(&WalRecordRef::AlterTableAddColumn { table: "t", column: &col, default_val: &Value::Int(0) }).unwrap(),
        ),
        (
            bincode::serialize(&WalRecord::AlterTableDropColumn { table: "t".into(), column: "c".into() }).unwrap(),
            bincode::serialize(&WalRecordRef::AlterTableDropColumn { table: "t", column: "c" }).unwrap(),
        ),
        (bincode::serialize(&WalRecord::TxRollback).unwrap(), bincode::serialize(&WalRecordRef::TxRollback).unwrap()),
        (bincode::serialize(&WalRecord::Savepoint { name: "s".into() }).unwrap(), bincode::serialize(&WalRecordRef::Savepoint { name: "s" }).unwrap()),
        (
            bincode::serialize(&WalRecord::RollbackToSavepoint { name: "s".into() }).unwrap(),
            bincode::serialize(&WalRecordRef::RollbackToSavepoint { name: "s" }).unwrap(),
        ),
        (bincode::serialize(&WalRecord::ReleaseSavepoint { name: "s".into() }).unwrap(), bincode::serialize(&WalRecordRef::ReleaseSavepoint { name: "s" }).unwrap()),
        (
            bincode::serialize(&WalRecord::UpdateByKey { table: "t".into(), key: Value::text("k"), col_idx: 1, new_val: Value::Int(2) }).unwrap(),
            bincode::serialize(&WalRecordRef::UpdateByKey { table: "t", key: &Value::text("k"), col_idx: 1, new_val: &Value::Int(2) }).unwrap(),
        ),
        (
            bincode::serialize(&WalRecord::DeleteByKey { table: "t".into(), key: Value::text("k") }).unwrap(),
            bincode::serialize(&WalRecordRef::DeleteByKey { table: "t", key: &Value::text("k") }).unwrap(),
        ),
        (
            bincode::serialize(&WalRecord::UpdateByRow { table: "t".into(), old_row: row.clone(), updates: vec![(1, Value::Null)] }).unwrap(),
            bincode::serialize(&WalRecordRef::UpdateByRow { table: "t", old_row: &row, updates: &[(1, Value::Null)] }).unwrap(),
        ),
        (
            bincode::serialize(&WalRecord::DeleteByRow { table: "t".into(), row: row.clone() }).unwrap(),
            bincode::serialize(&WalRecordRef::DeleteByRow { table: "t", row: &row }).unwrap(),
        ),
        (bincode::serialize(&WalRecord::TxBegin).unwrap(), bincode::serialize(&WalRecordRef::TxBegin).unwrap()),
        (
            bincode::serialize(&WalRecord::Schema { version: 1, payload: vec![1, 2, 3] }).unwrap(),
            bincode::serialize(&WalRecordRef::Schema { version: 1, payload: &[1, 2, 3] }).unwrap(),
        ),
    ];
    // Chỉ số biến thể của các bản ghi cũ không được đổi (định dạng file).
    let tag = |b: &Vec<u8>| u32::from_le_bytes(b[0..4].try_into().unwrap());
    assert_eq!(tag(&pairs[0].0), 0, "Begin");
    assert_eq!(tag(&pairs[5].0), 5, "Insert");
    assert_eq!(tag(&pairs[11].0), 11, "AlterTableDropColumn");
    assert_eq!(tag(&pairs[12].0), 12, "TxRollback phải nằm sau mọi biến thể cũ");
    assert_eq!(tag(&pairs[20].0), 20, "TxBegin");
    assert_eq!(tag(&pairs[21].0), 21, "Schema phải nằm sau TxBegin");
    for (i, (legacy, by_ref)) in pairs.iter().enumerate() {
        assert_eq!(legacy, by_ref, "biến thể #{i} khác byte");
    }
}

/// INSERT đủ kiểu trên DB do code cũ tạo, rồi mở lại: không lỗi, không trùng id, không mất dòng.
#[test]
fn inserts_into_legacy_db_work_and_survive_reopen() {
    let dir = temp_dir("legacy_insert");
    let db_path = copy_db(&fixture_dir().join("legacy_v2.db"), &dir);
    {
        let mut db = PGlite::open(&db_path).unwrap();
        // 1. Literal, đủ kiểu cột, kể cả cột `phone` thêm bằng ALTER trong DB cũ
        db.exec("INSERT INTO schools (name, active, rating, meta, created_at, opened, code, logo, tags, class_ids, phone) VALUES ('Lit', true, 1.25, '{\"k\":[1,2]}', '2026-10-17 09:00:00', '2026-10-17', 'b0eebc99-9c0b-4ef8-bb6d-6bb9bd380a12', '\\x0a0b', '{p,q}', '[1,2]', '0123')", &[]).unwrap();
        // 2. Tham số $n
        db.exec("INSERT INTO schools (name, active, rating) VALUES ($1, $2, $3)", &[Value::text("Param"), Value::Bool(false), Value::Float(2.5)]).unwrap();
        // 3. Nhiều dòng một lệnh
        db.exec("INSERT INTO schedules (school_id, title, start_at, big, slot) VALUES (1, 'A', '2026-10-17 07:00:00', 5, '07:00:00'), (2, 'B', '2026-10-17 08:00:00', 6, '08:00:00')", &[]).unwrap();
        // 4. Qua đường gateway (query_wire) có RETURNING
        let r = db.query_wire("INSERT INTO schools (name) VALUES ($1) RETURNING id, name, active", &[Value::text("Wire")]).unwrap();
        assert_eq!(r.command, "INSERT 0 1");
        assert_eq!(r.rows.len(), 1);
        // 5. Bảng mới tạo sau khi mở DB cũ
        db.exec("CREATE TABLE extra (id SERIAL PRIMARY KEY, v TEXT)", &[]).unwrap();
        db.exec("INSERT INTO extra (v) VALUES ('e1')", &[]).unwrap();
        db.flush();
    }

    let mut db = PGlite::open(&db_path).unwrap();
    let schools = db.query("SELECT id, name, phone, active FROM schools ORDER BY id", &[]).unwrap();
    let names: Vec<&str> = schools.rows.iter().map(|r| r["name"].as_str().unwrap()).collect();
    assert_eq!(names, vec!["Alpha", "Beta", "Lit", "Param", "Wire"]);
    let mut ids: Vec<i64> = schools.rows.iter().map(|r| r["id"].as_i64().unwrap()).collect();
    let n = ids.len();
    ids.dedup();
    assert_eq!(ids.len(), n, "id SERIAL bị trùng: {ids:?}");
    assert!(ids[2] > 2, "id mới phải lớn hơn id cũ: {ids:?}");
    assert_eq!(schools.rows[2]["phone"], "0123");

    let lit = db.query_wire("SELECT active, rating, meta, tags, class_ids, logo FROM schools WHERE name = 'Lit'", &[]).unwrap();
    assert_eq!(lit.rows[0], vec![Some("t".into()), Some("1.25".into()), Some("{\"k\":[1,2]}".into()), Some("{p,q}".into()), Some("[1,2]".into()), Some("\\x0a0b".into())]);

    let sched = db.query("SELECT id, title FROM schedules ORDER BY id", &[]).unwrap();
    assert_eq!(sched.rows.len(), 4);
    let extra = db.query("SELECT v FROM extra", &[]).unwrap();
    assert_eq!(extra.rows[0]["v"], "e1");
    let _ = std::fs::remove_dir_all(dir);
}

/// Câu có subquery không được dùng lại kết quả cũ khi đổi tham số hoặc khi dữ liệu thay đổi.
#[test]
fn subquery_results_are_not_served_from_stale_plan_cache() {
    let mut db = setup();
    let sql = "SELECT id, name FROM schools WHERE id IN (SELECT school_id FROM schedules WHERE title = $1)";
    let a = db.query(sql, &[Value::text("Math")]).unwrap();
    assert_eq!(a.rows.len(), 1);
    let b = db.query(sql, &[Value::text("Không có")]).unwrap();
    assert!(b.rows.is_empty(), "tham số mới nhưng trả kết quả cũ: {:?}", b.rows);

    let no_param = "SELECT id FROM schools WHERE id IN (SELECT school_id FROM schedules)";
    assert_eq!(db.query(no_param, &[]).unwrap().rows.len(), 1);
    db.exec("INSERT INTO schedules (school_id, title, start_at) VALUES (2, 'Art', '2026-10-13 09:00:00')", &[]).unwrap();
    assert_eq!(db.query(no_param, &[]).unwrap().rows.len(), 2, "dữ liệu mới không được thấy");

    let in_select = "SELECT id, (SELECT COUNT(*) FROM schedules WHERE title = $1) AS n FROM schools WHERE id = 1";
    let x = db.query(in_select, &[Value::text("Math")]).unwrap();
    let y = db.query(in_select, &[Value::text("Art")]).unwrap();
    assert_eq!(x.rows.len(), y.rows.len());
}
