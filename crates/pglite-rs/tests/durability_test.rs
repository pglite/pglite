//! Độ bền dữ liệu: mọi thay đổi phải còn nguyên (và chỉ những thay đổi đã chốt) sau khi
//! đóng rồi mở lại database, và DB do bản cũ tạo phải mở ra đúng như bản cũ đọc.
use pglite_rs::{PGlite, Value};
use std::path::{Path, PathBuf};

fn temp_db(tag: &str) -> (PathBuf, String) {
    let nanos = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos();
    let dir = std::env::temp_dir().join(format!("pglite_dur_{}_{}_{}", tag, std::process::id(), nanos));
    std::fs::create_dir_all(&dir).unwrap();
    let db = dir.join("t.db").to_str().unwrap().to_string();
    (dir, db)
}

fn rows(db: &mut PGlite, sql: &str) -> Vec<serde_json::Value> {
    db.query(sql, &[]).unwrap_or_else(|e| panic!("{sql}: {e}")).rows
}

fn col(db: &mut PGlite, sql: &str, c: &str) -> Vec<serde_json::Value> {
    rows(db, sql).into_iter().map(|r| r[c].clone()).collect()
}

/// Chạy `f` trên DB, đóng, mở lại và trả về DB mới mở. Kiểm tra thêm rằng kết quả
/// `check` trước khi đóng và sau khi mở lại là như nhau.
fn reopen_same(path: &str, check: &[&str], f: impl FnOnce(&mut PGlite)) -> PGlite {
    let mut before = Vec::new();
    {
        let mut db = PGlite::open(path).unwrap();
        f(&mut db);
        for q in check {
            before.push(rows(&mut db, q));
        }
    }
    let mut db = PGlite::open(path).unwrap();
    for (q, b) in check.iter().zip(before) {
        assert_eq!(rows(&mut db, q), b, "khác nhau trước/sau khi mở lại: {q}");
    }
    db
}

fn open_fixture(name: &str) -> (PathBuf, String) {
    let fx = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures");
    let (dir, path) = temp_db(name);
    std::fs::copy(fx.join(format!("{name}.db.rwal")), format!("{path}.rwal")).unwrap();
    (dir, path)
}

fn fixture_expected(name: &str) -> serde_json::Value {
    let fx = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures");
    serde_json::from_str(&std::fs::read_to_string(fx.join(format!("{name}.expected.json"))).unwrap()).unwrap()
}

/// Không có bản sao lưu nào được tạo: file WAL không bị sửa.
fn assert_no_repair(dir: &Path) {
    let baks: Vec<_> = std::fs::read_dir(dir).unwrap().flatten().filter(|e| e.file_name().to_string_lossy().contains(".corrupt-")).collect();
    assert!(baks.is_empty(), "WAL bị sửa ngoài ý muốn: {:?}", baks.iter().map(|e| e.file_name()).collect::<Vec<_>>());
}

fn exec_all(db: &mut PGlite, sqls: &[&str]) {
    for sql in sqls {
        db.exec(sql, &[]).unwrap_or_else(|e| panic!("{sql}: {e}"));
    }
}

#[test]
fn rollback_stays_rolled_back_after_reopen() {
    let (dir, path) = temp_db("rb");
    let mut db = reopen_same(&path, &["SELECT * FROM t ORDER BY id"], |db| {
        exec_all(db, &[
            "CREATE TABLE t (id SERIAL PRIMARY KEY, v TEXT, owner_id INT)",
            "INSERT INTO t (v, owner_id) VALUES ('a', 1)",
            "BEGIN",
            "INSERT INTO t (v, owner_id) VALUES ('rolled-back', 2)",
            "UPDATE t SET v = 'changed', owner_id = 9 WHERE id = 1",
            "DELETE FROM t WHERE id = 1",
            "ROLLBACK",
            "INSERT INTO t (v, owner_id) VALUES ('after', 3)",
        ]);
    });
    assert_eq!(col(&mut db, "SELECT v FROM t ORDER BY id", "v"), vec!["a", "after"]);
    // Index phụ (owner_id) không còn trỏ vào dòng đã hoàn tác
    assert_eq!(col(&mut db, "SELECT v FROM t WHERE owner_id = 1", "v"), vec!["a"]);
    assert!(rows(&mut db, "SELECT v FROM t WHERE owner_id = 9").is_empty());
    assert!(rows(&mut db, "SELECT v FROM t WHERE owner_id = 2").is_empty());
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn savepoints_are_replayed() {
    let (dir, path) = temp_db("sp");
    let mut db = reopen_same(&path, &["SELECT * FROM t ORDER BY id"], |db| {
        exec_all(db, &[
            "CREATE TABLE t (id SERIAL PRIMARY KEY, v TEXT)",
            "BEGIN",
            "INSERT INTO t (v) VALUES ('b')",
            "SAVEPOINT s1",
            "INSERT INTO t (v) VALUES ('c-undone')",
            "UPDATE t SET v = 'b-undone' WHERE v = 'b'",
            "ROLLBACK TO SAVEPOINT s1",
            "INSERT INTO t (v) VALUES ('d')",
            "RELEASE SAVEPOINT s1",
            "COMMIT",
        ]);
    });
    assert_eq!(col(&mut db, "SELECT v FROM t ORDER BY id", "v"), vec!["b", "d"]);
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn ddl_inside_rolled_back_transaction_matches_live_state() {
    // Như PostgreSQL, ROLLBACK hoàn tác cả DDL; replay phải cho cùng trạng thái với lúc chạy.
    let (dir, path) = temp_db("ddl");
    let mut db = reopen_same(&path, &["SELECT * FROM t ORDER BY id"], |db| {
        exec_all(db, &[
            "CREATE TABLE t (id SERIAL PRIMARY KEY, v TEXT)",
            "INSERT INTO t (v) VALUES ('a')",
            "BEGIN",
            "INSERT INTO t (v) VALUES ('x')",
            "ALTER TABLE t ADD COLUMN extra TEXT",
            "CREATE TABLE side (id SERIAL PRIMARY KEY)",
            "ROLLBACK",
        ]);
    });
    assert_eq!(col(&mut db, "SELECT v FROM t", "v"), vec!["a"]);
    assert!(db.query("SELECT * FROM side", &[]).is_err());
    assert!(db.query("SELECT extra FROM t", &[]).is_err());
    let _ = std::fs::remove_dir_all(dir);
}

/// BEGIN không đóng trong file do bản cũ ghi (bản ghi `Begin`): vẫn giữ như bản cũ đọc.
#[test]
fn unterminated_legacy_transaction_is_kept_like_before() {
    let (dir, path) = open_fixture("legacy_open_begin");
    let expected = fixture_expected("legacy_open_begin");
    let mut db = PGlite::open(&path).unwrap();
    assert_eq!(serde_json::Value::Array(rows(&mut db, "SELECT * FROM t ORDER BY id")), expected["t"]);
    drop(db);
    let mut db = PGlite::open(&path).unwrap();
    assert_eq!(serde_json::Value::Array(rows(&mut db, "SELECT * FROM t ORDER BY id")), expected["t"]);
    assert_no_repair(&dir);
    let _ = std::fs::remove_dir_all(dir);
}

/// BEGIN ghi từ bản mới (`TxBegin`) mà chưa COMMIT khi tiến trình dừng: bị hoàn tác.
#[test]
fn unterminated_new_transaction_is_rolled_back() {
    let (dir, path) = temp_db("open_tx");
    {
        let mut db = PGlite::open(&path).unwrap();
        exec_all(&mut db, &[
            "CREATE TABLE t (id SERIAL PRIMARY KEY, v TEXT)",
            "INSERT INTO t (v) VALUES ('committed')",
            "BEGIN",
            "INSERT INTO t (v) VALUES ('never-committed')",
            "UPDATE t SET v = 'changed' WHERE id = 1",
        ]);
        db.flush();
    }
    let mut db = PGlite::open(&path).unwrap();
    assert_eq!(col(&mut db, "SELECT v FROM t ORDER BY id", "v"), vec!["committed"]);
    // Ghi tiếp sau đó vẫn bền
    exec_all(&mut db, &["INSERT INTO t (v) VALUES ('later')"]);
    drop(db);
    let mut db = PGlite::open(&path).unwrap();
    assert_eq!(col(&mut db, "SELECT v FROM t ORDER BY id", "v"), vec!["committed", "later"]);
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn uuid_text_and_no_pk_updates_survive_reopen() {
    let (dir, path) = temp_db("keys");
    let checks = ["SELECT * FROM u ORDER BY name", "SELECT * FROM k ORDER BY code", "SELECT * FROM n ORDER BY a, b"];
    let mut db = reopen_same(&path, &checks, |db| {
        exec_all(db, &[
            "CREATE TABLE u (id UUID PRIMARY KEY, name TEXT)",
            "INSERT INTO u (id, name) VALUES ('a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11', 'old'), ('b0eebc99-9c0b-4ef8-bb6d-6bb9bd380a12', 'to-delete')",
            "UPDATE u SET name = 'new' WHERE id = 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'",
            "DELETE FROM u WHERE id = 'b0eebc99-9c0b-4ef8-bb6d-6bb9bd380a12'",
            "CREATE TABLE k (code TEXT PRIMARY KEY, qty INT)",
            "INSERT INTO k (code, qty) VALUES ('AA', 1), ('BB', 2), ('CC', 3)",
            "UPDATE k SET qty = 10 WHERE code = 'AA'",
            // đổi chính khoá chính cùng cột khác trong một lệnh
            "UPDATE k SET code = 'BZ', qty = 20 WHERE code = 'BB'",
            "DELETE FROM k WHERE code = 'CC'",
            "CREATE TABLE n (a INT, b TEXT)",
            "INSERT INTO n (a, b) VALUES (1, 'x'), (1, 'x'), (2, 'y'), (3, 'z')",
            "UPDATE n SET b = 'x2' WHERE a = 1",
            "DELETE FROM n WHERE a = 3",
            "UPDATE n SET a = 5 WHERE b = 'y'",
        ]);
        db.exec("UPDATE u SET name = $1 WHERE id = $2", &[Value::text("param"), Value::text("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11")]).unwrap();
    });
    assert_eq!(col(&mut db, "SELECT name FROM u", "name"), vec!["param"]);
    assert_eq!(rows(&mut db, "SELECT code, qty FROM k ORDER BY code"), vec![
        serde_json::json!({"code": "AA", "qty": 10}),
        serde_json::json!({"code": "BZ", "qty": 20}),
    ]);
    assert_eq!(rows(&mut db, "SELECT a, b FROM n ORDER BY a, b"), vec![
        serde_json::json!({"a": 1, "b": "x2"}),
        serde_json::json!({"a": 1, "b": "x2"}),
        serde_json::json!({"a": 5, "b": "y"}),
    ]);
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn rollback_of_uuid_and_no_pk_changes_after_reopen() {
    let (dir, path) = temp_db("rb_keys");
    let mut db = reopen_same(&path, &["SELECT * FROM u", "SELECT * FROM n ORDER BY a"], |db| {
        exec_all(db, &[
            "CREATE TABLE u (id UUID PRIMARY KEY, name TEXT)",
            "CREATE TABLE n (a INT, b TEXT)",
            "INSERT INTO u (id, name) VALUES ('a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11', 'keep')",
            "INSERT INTO n (a, b) VALUES (1, 'keep')",
            "BEGIN",
            "UPDATE u SET name = 'undone'",
            "UPDATE n SET b = 'undone'",
            "DELETE FROM n",
            "ROLLBACK",
        ]);
    });
    assert_eq!(col(&mut db, "SELECT name FROM u", "name"), vec!["keep"]);
    assert_eq!(col(&mut db, "SELECT b FROM n", "b"), vec!["keep"]);
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn secondary_index_follows_updates() {
    let mut db = PGlite::in_memory().unwrap();
    exec_all(&mut db, &[
        "CREATE TABLE o (id SERIAL PRIMARY KEY, user_id INT, note TEXT)",
        "INSERT INTO o (user_id, note) VALUES (1, 'a'), (1, 'b'), (2, 'c')",
        "UPDATE o SET user_id = 7 WHERE id = 1",
    ]);
    // Lần 1 dùng index, lần 2 dùng plan cache: cả hai phải đúng
    for _ in 0..2 {
        assert_eq!(col(&mut db, "SELECT note FROM o WHERE user_id = 7", "note"), vec!["a"]);
        assert_eq!(col(&mut db, "SELECT note FROM o WHERE user_id = 1", "note"), vec!["b"]);
    }
    exec_all(&mut db, &["BEGIN", "INSERT INTO o (user_id, note) VALUES (3, 'tmp')", "ROLLBACK", "INSERT INTO o (user_id, note) VALUES (4, 'new')"]);
    assert!(rows(&mut db, "SELECT note FROM o WHERE user_id = 3").is_empty());
    assert_eq!(col(&mut db, "SELECT note FROM o WHERE user_id = 4", "note"), vec!["new"]);
}

/// Tiến trình con ghi rồi thoát ngay, không chạy Drop/flush (giống bị kill).
#[test]
fn autocommit_writes_survive_process_kill() {
    if let Ok(path) = std::env::var("PGLITE_CRASH_CHILD_DB") {
        let mut db = PGlite::open(&path).unwrap();
        exec_all(&mut db, &[
            "INSERT INTO t (v) VALUES ('autocommit-1')",
            "UPDATE t SET v = 'seed-updated' WHERE id = 1",
            "INSERT INTO t (v) VALUES ('autocommit-2')",
        ]);
        std::process::exit(0);
    }
    let (dir, path) = temp_db("crash");
    {
        let mut db = PGlite::open(&path).unwrap();
        exec_all(&mut db, &["CREATE TABLE t (id SERIAL PRIMARY KEY, v TEXT)", "INSERT INTO t (v) VALUES ('seed')"]);
    }
    let status = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "autocommit_writes_survive_process_kill", "--nocapture"])
        .env("PGLITE_CRASH_CHILD_DB", &path)
        .status()
        .unwrap();
    assert!(status.success());
    let mut db = PGlite::open(&path).unwrap();
    assert_eq!(col(&mut db, "SELECT v FROM t ORDER BY id", "v"), vec!["seed-updated", "autocommit-1", "autocommit-2"]);
    let _ = std::fs::remove_dir_all(dir);
}

/// DB do bản production (e6199b7) tạo, có BEGIN…ROLLBACK, SAVEPOINT và UPDATE trên khoá
/// UUID: bản mới phải đọc ra **đúng như bản cũ đọc**, kể cả các dòng đã rollback.
#[test]
fn legacy_transaction_fixture_reads_exactly_like_old_engine() {
    let fx = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures");
    let (dir, path) = temp_db("legacy_tx");
    std::fs::copy(fx.join("legacy_tx.db.rwal"), format!("{path}.rwal")).unwrap();
    let expected: serde_json::Value = serde_json::from_str(&std::fs::read_to_string(fx.join("legacy_tx.expected.json")).unwrap()).unwrap();
    {
        let mut db = PGlite::open(&path).unwrap();
        assert_eq!(serde_json::Value::Array(rows(&mut db, "SELECT * FROM t ORDER BY id")), expected["t"]);
        assert_eq!(serde_json::Value::Array(rows(&mut db, "SELECT * FROM u ORDER BY name")), expected["u"]);
        // Ghi thêm bằng bản mới (có rollback) trên cùng file
        exec_all(&mut db, &["BEGIN", "INSERT INTO t (v) VALUES ('new-rolled-back')", "ROLLBACK", "INSERT INTO t (v) VALUES ('new')"]);
    }
    let mut db = PGlite::open(&path).unwrap();
    let mut want: Vec<serde_json::Value> = expected["t"].as_array().unwrap().iter().map(|r| r["v"].clone()).collect();
    want.push("new".into());
    assert_eq!(col(&mut db, "SELECT v FROM t ORDER BY id", "v"), want);
    let _ = std::fs::remove_dir_all(dir);
}
