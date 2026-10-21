//! View, schema, sequence độc lập, enum, domain (engine/objects.rs): lưu bền khi mở lại DB, DDL theo
//! giao dịch, số đã cấp của sequence không bị ROLLBACK trả lại, view tạm không xuống đĩa.
use pglite_rs::PGlite;

fn temp_db(tag: &str) -> (std::path::PathBuf, String) {
    let nanos = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos();
    let dir = std::env::temp_dir().join(format!("pglite_obj_{}_{}_{}", tag, std::process::id(), nanos));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("o.db").to_str().unwrap().to_string();
    (dir, path)
}

fn ints(db: &mut PGlite, sql: &str, col: &str) -> Vec<i64> {
    db.query(sql, &[]).unwrap_or_else(|e| panic!("{sql}: {e}")).rows.iter().map(|r| r[col].as_i64().unwrap()).collect()
}

fn texts(db: &mut PGlite, sql: &str, col: &str) -> Vec<String> {
    db.query(sql, &[]).unwrap_or_else(|e| panic!("{sql}: {e}")).rows.iter().map(|r| r[col].as_str().unwrap().to_string()).collect()
}

#[test]
fn objects_survive_reopen_and_follow_transactions() {
    let (dir, path) = temp_db("reopen");
    {
        let mut db = PGlite::open(&path).unwrap();
        for sql in [
            "CREATE TABLE t (id SERIAL PRIMARY KEY, name TEXT)",
            "INSERT INTO t (name) VALUES ('a'), ('b')",
            "CREATE SEQUENCE s START 10",
            "CREATE TYPE mood AS ENUM ('lo', 'hi')",
            "CREATE TABLE m (id INT, v mood)",
            "INSERT INTO m VALUES (1, 'hi'), (2, 'lo')",
            "CREATE VIEW v AS SELECT name FROM t",
            "CREATE SCHEMA app",
            "CREATE DOMAIN posint AS int CHECK (VALUE > 0)",
        ] {
            db.exec(sql, &[]).unwrap_or_else(|e| panic!("{sql}: {e}"));
        }
        assert_eq!(ints(&mut db, "SELECT nextval('s') AS a", "a"), vec![10]);
        // DDL trong giao dịch bị ROLLBACK được hoàn tác; số sequence đã cấp thì không
        for sql in ["BEGIN", "CREATE VIEW vx AS SELECT 1 AS one", "DROP VIEW v", "ALTER TYPE mood ADD VALUE 'mid' AFTER 'lo'", "CREATE SEQUENCE s2", "DROP SCHEMA app"] {
            db.exec(sql, &[]).unwrap_or_else(|e| panic!("{sql}: {e}"));
        }
        assert_eq!(ints(&mut db, "SELECT nextval('s') AS a", "a"), vec![11]);
        db.exec("ROLLBACK", &[]).unwrap();
        assert!(db.query("SELECT * FROM vx", &[]).is_err());
        assert_eq!(texts(&mut db, "SELECT name FROM v ORDER BY name", "name"), vec!["a", "b"]);
        assert!(db.exec("INSERT INTO m VALUES (3, 'mid')", &[]).is_err(), "nhãn thêm trong giao dịch đã ROLLBACK");
        assert_eq!(ints(&mut db, "SELECT nextval('s') AS a", "a"), vec![12]);
        // view tạm: chỉ trong phiên
        db.exec("CREATE TEMP VIEW tv AS SELECT 2 AS two", &[]).unwrap();
        assert_eq!(ints(&mut db, "SELECT two FROM tv", "two"), vec![2]);
    }
    let mut db = PGlite::open(&path).unwrap();
    assert_eq!(ints(&mut db, "SELECT nextval('s') AS a", "a"), vec![13]);
    assert_eq!(texts(&mut db, "SELECT name FROM v ORDER BY name", "name"), vec!["a", "b"]);
    assert!(db.query("SELECT * FROM tv", &[]).is_err(), "view tạm không được lưu");
    assert!(db.query("SELECT * FROM vx", &[]).is_err());
    assert_eq!(texts(&mut db, "SELECT v FROM m ORDER BY v", "v"), vec!["lo", "hi"]);
    let e = db.exec("INSERT INTO m VALUES (3, 'mid')", &[]).unwrap_err();
    assert_eq!(pglite_rs::sqlstate(&e), "22P02", "{e}");
    let e = db.exec("CREATE SCHEMA app", &[]).unwrap_err();
    assert_eq!(pglite_rs::sqlstate(&e), "42P06", "{e}");
    db.exec("CREATE TABLE p (a posint)", &[]).unwrap();
    let e = db.exec("INSERT INTO p VALUES (0)", &[]).unwrap_err();
    assert_eq!(pglite_rs::sqlstate(&e), "23514", "{e}");
    drop(db);
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn failed_statement_using_sequence_is_still_undone() {
    // nextval ghi WAL nhưng không phải DDL: lệnh lỗi sau đó vẫn được hoàn tác như thường
    let (dir, path) = temp_db("undo");
    {
        let mut db = PGlite::open(&path).unwrap();
        db.exec("CREATE SEQUENCE s", &[]).unwrap();
        db.exec("CREATE TABLE q (id INT DEFAULT nextval('s') UNIQUE, x TEXT)", &[]).unwrap();
        db.exec("INSERT INTO q (x) VALUES ('a')", &[]).unwrap();
        db.exec("SELECT setval('s', 1, false)", &[]).unwrap();
        // dòng 1 lấy id 1 (trùng) → cả lệnh lỗi, không dòng nào được ghi
        assert!(db.exec("INSERT INTO q (x) VALUES ('b'), ('c')", &[]).is_err());
        assert_eq!(texts(&mut db, "SELECT x FROM q ORDER BY x", "x"), vec!["a"]);
    }
    let mut db = PGlite::open(&path).unwrap();
    assert_eq!(texts(&mut db, "SELECT x FROM q ORDER BY x", "x"), vec!["a"]);
    drop(db);
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn drop_schema_public_cascade_resets_database() {
    let (dir, path) = temp_db("reset");
    {
        let mut db = PGlite::open(&path).unwrap();
        for sql in ["CREATE TABLE a (id INT)", "CREATE SEQUENCE s", "CREATE VIEW v AS SELECT * FROM a", "CREATE TYPE e AS ENUM ('x')"] {
            db.exec(sql, &[]).unwrap();
        }
        let e = db.exec("DROP SCHEMA public", &[]).unwrap_err();
        assert_eq!(pglite_rs::sqlstate(&e), "2BP01", "{e}");
        db.exec("DROP SCHEMA public CASCADE", &[]).unwrap();
        db.exec("CREATE SCHEMA public", &[]).unwrap();
        db.exec("CREATE TABLE a (id INT)", &[]).unwrap();
        db.exec("CREATE SEQUENCE s", &[]).unwrap();
    }
    let mut db = PGlite::open(&path).unwrap();
    assert!(db.query("SELECT * FROM v", &[]).is_err());
    assert_eq!(ints(&mut db, "SELECT nextval('s') AS n", "n"), vec![1]);
    drop(db);
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn lastval_is_per_session() {
    let mut a = PGlite::in_memory().unwrap();
    let mut b = PGlite::in_memory().unwrap();
    a.exec("CREATE SEQUENCE s", &[]).unwrap();
    a.exec("SELECT nextval('s')", &[]).unwrap();
    let e = b.query("SELECT lastval()", &[]).unwrap_err();
    assert_eq!(pglite_rs::sqlstate(&e), "55000", "{e}");
    assert_eq!(ints(&mut a, "SELECT lastval() AS l", "l"), vec![1]);
}
