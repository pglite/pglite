//! BEGIN lồng nhau không làm mất khả năng ROLLBACK; savepoint không tồn tại là lỗi.
use pglite_rs::PGlite;

#[test]
fn nested_begin_keeps_outer_work_rollbackable_and_replays() {
    let nanos = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos();
    let dir = std::env::temp_dir().join(format!("pglite_tx_{}_{}", std::process::id(), nanos));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("t.db").to_str().unwrap().to_string();
    {
        let mut db = PGlite::open(&path).unwrap();
        for sql in ["CREATE TABLE t (id INT PRIMARY KEY)", "INSERT INTO t VALUES (1)", "BEGIN", "INSERT INTO t VALUES (2)", "BEGIN", "INSERT INTO t VALUES (3)", "ROLLBACK"] {
            db.exec(sql, &[]).unwrap_or_else(|e| panic!("{sql}: {e}"));
        }
        assert_eq!(db.query("SELECT id FROM t", &[]).unwrap().rows.len(), 1, "cả 2 và 3 phải bị hoàn tác");
        // Savepoint
        assert!(db.exec("SAVEPOINT s", &[]).is_err(), "ngoài giao dịch");
        db.exec("BEGIN", &[]).unwrap();
        db.exec("INSERT INTO t VALUES (4)", &[]).unwrap();
        assert!(db.exec("ROLLBACK TO SAVEPOINT nope", &[]).is_err());
        // Như PostgreSQL: lỗi làm transaction bị abort (25P02) tới khi ROLLBACK
        let e = db.exec("RELEASE SAVEPOINT nope", &[]).unwrap_err();
        assert_eq!(pglite_rs::sqlstate(&e), "25P02", "{e}");
        db.exec("ROLLBACK", &[]).unwrap();
        db.exec("BEGIN", &[]).unwrap();
        db.exec("INSERT INTO t VALUES (4)", &[]).unwrap();
        assert!(db.exec("RELEASE SAVEPOINT nope", &[]).is_err());
        db.exec("ROLLBACK", &[]).unwrap();
        db.exec("BEGIN", &[]).unwrap();
        db.exec("INSERT INTO t VALUES (4)", &[]).unwrap();
        db.exec("SAVEPOINT s1", &[]).unwrap();
        db.exec("INSERT INTO t VALUES (5)", &[]).unwrap();
        db.exec("ROLLBACK TO SAVEPOINT s1", &[]).unwrap();
        db.exec("COMMIT", &[]).unwrap();
    }
    let mut db = PGlite::open(&path).unwrap();
    let ids: Vec<i64> = db.query("SELECT id FROM t ORDER BY id", &[]).unwrap().rows.iter().map(|r| r["id"].as_i64().unwrap()).collect();
    assert_eq!(ids, vec![1, 4]);
    drop(db);
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn ddl_inside_transaction_is_rolled_back_in_memory_and_on_disk() {
    let nanos = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos();
    let dir = std::env::temp_dir().join(format!("pglite_txddl_{}_{}", std::process::id(), nanos));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("t.db").to_str().unwrap().to_string();
    let snapshot = |db: &mut PGlite| -> (Vec<String>, usize, bool) {
        let cols = db.query_wire("SELECT * FROM a", &[]).unwrap().fields.iter().map(|f| f.name.clone()).collect();
        let rows = db.query("SELECT * FROM a", &[]).unwrap().rows.len();
        (cols, rows, db.query("SELECT * FROM b", &[]).is_ok())
    };
    {
        let mut db = PGlite::open(&path).unwrap();
        for sql in ["CREATE TABLE a (x INT PRIMARY KEY, y TEXT)", "INSERT INTO a VALUES (1, 'one'), (2, 'two')"] {
            db.exec(sql, &[]).unwrap();
        }
        // Như PostgreSQL: CREATE / ALTER / RENAME / DROP trong giao dịch đều bị ROLLBACK hoàn tác
        for sql in ["BEGIN", "CREATE TABLE b (z INT)", "ALTER TABLE a ADD COLUMN w INT", "ALTER TABLE a RENAME COLUMN y TO yy", "INSERT INTO a VALUES (3, 'three', 3)", "DROP TABLE a", "ROLLBACK"] {
            db.exec(sql, &[]).unwrap_or_else(|e| panic!("{sql}: {e}"));
        }
        assert_eq!(snapshot(&mut db), (vec!["x".to_string(), "y".to_string()], 2, false));
        // ROLLBACK TO SAVEPOINT cũng vậy; phần trước savepoint được giữ
        for sql in ["BEGIN", "ALTER TABLE a ADD COLUMN keep INT", "SAVEPOINT s", "ALTER TABLE a DROP COLUMN y", "CREATE TABLE b (z INT)", "ROLLBACK TO SAVEPOINT s", "COMMIT"] {
            db.exec(sql, &[]).unwrap_or_else(|e| panic!("{sql}: {e}"));
        }
        assert_eq!(snapshot(&mut db), (vec!["x".to_string(), "y".to_string(), "keep".to_string()], 2, false));
        db.exec("INSERT INTO a VALUES (4, 'four', 4)", &[]).unwrap();
    }
    let mut db = PGlite::open(&path).unwrap();
    assert_eq!(snapshot(&mut db), (vec!["x".to_string(), "y".to_string(), "keep".to_string()], 3, false));
    drop(db);
    let _ = std::fs::remove_dir_all(dir);
}
