//! Thuộc tính phụ của bảng (`SchemaOp::TableMeta`): cột generated, `UNIQUE NULLS NOT DISTINCT`, UNIQUE trên
//! biểu thức, `EXCLUDE (... WITH =)`, khoá ngoại `DEFERRABLE` — lưu bền khi mở lại DB, đi qua TRUNCATE, và
//! ROLLBACK của `CREATE TABLE` trong giao dịch.
use pglite_rs::PGlite;

fn temp_db(tag: &str) -> (std::path::PathBuf, String) {
    let nanos = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos();
    let dir = std::env::temp_dir().join(format!("pglite_meta_{}_{}_{}", tag, std::process::id(), nanos));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("m.db").to_str().unwrap().to_string();
    (dir, path)
}

fn ints(db: &mut PGlite, sql: &str, col: &str) -> Vec<i64> {
    db.query(sql, &[]).unwrap_or_else(|e| panic!("{sql}: {e}")).rows.iter().map(|r| r[col].as_i64().unwrap()).collect()
}

fn exec(db: &mut PGlite, sqls: &[&str]) {
    for sql in sqls {
        db.exec(sql, &[]).unwrap_or_else(|e| panic!("{sql}: {e}"));
    }
}

#[test]
fn table_meta_survives_reopen_and_truncate() {
    let (dir, path) = temp_db("reopen");
    {
        let mut db = PGlite::open(&path).unwrap();
        exec(
            &mut db,
            &[
                "CREATE TABLE g (a INT, b INT GENERATED ALWAYS AS (a * 2) STORED)",
                "INSERT INTO g (a) VALUES (3)",
                "CREATE TABLE nd (x INT UNIQUE NULLS NOT DISTINCT)",
                "INSERT INTO nd VALUES (NULL)",
                "CREATE TABLE u (name TEXT)",
                "CREATE UNIQUE INDEX u_lower ON u (lower(name))",
                "INSERT INTO u VALUES ('Ann')",
                "CREATE TABLE ex (a INT, EXCLUDE (a WITH =))",
                "INSERT INTO ex VALUES (1)",
                "CREATE TABLE p (id INT PRIMARY KEY)",
                "CREATE TABLE c (p INT REFERENCES p(id) DEFERRABLE INITIALLY DEFERRED)",
            ],
        );
    }
    for round in 0..2 {
        let mut db = PGlite::open(&path).unwrap();
        // generated: giá trị cũ còn, ghi mới vẫn được tính, ghi thẳng bị từ chối
        exec(&mut db, &["UPDATE g SET a = a + 1"]);
        assert_eq!(ints(&mut db, "SELECT b FROM g", "b"), vec![(4 + round) * 2], "vòng {round}");
        assert!(db.exec("INSERT INTO g (a, b) VALUES (1, 1)", &[]).is_err());
        assert!(db.exec("INSERT INTO nd VALUES (NULL)", &[]).is_err(), "NULLS NOT DISTINCT sau khi mở lại");
        assert!(db.exec("INSERT INTO u VALUES ('ANN')", &[]).is_err(), "UNIQUE (lower(name)) sau khi mở lại");
        assert!(db.exec("INSERT INTO ex VALUES (1)", &[]).is_err(), "EXCLUDE sau khi mở lại");
        let e = db.exec("INSERT INTO ex VALUES (1)", &[]).unwrap_err();
        assert!(e.contains("exclusion constraint"), "{e}");
        // DEFERRABLE: hoãn tới COMMIT
        exec(&mut db, &["BEGIN", "INSERT INTO c VALUES (7)", "INSERT INTO p VALUES (7)", "COMMIT", "DELETE FROM c", "DELETE FROM p"]);
        exec(&mut db, &["BEGIN", "INSERT INTO c VALUES (8)"]);
        assert!(db.exec("COMMIT", &[]).is_err(), "khoá ngoại hoãn bị vi phạm lúc COMMIT");
        assert_eq!(ints(&mut db, "SELECT count(*) AS n FROM c", "n"), vec![0]);
    }
    // TRUNCATE dựng lại bảng: thuộc tính phụ vẫn còn sau khi mở lại
    {
        let mut db = PGlite::open(&path).unwrap();
        exec(&mut db, &["TRUNCATE g", "TRUNCATE nd", "INSERT INTO g (a) VALUES (10)", "INSERT INTO nd VALUES (NULL)"]);
    }
    let mut db = PGlite::open(&path).unwrap();
    assert_eq!(ints(&mut db, "SELECT b FROM g", "b"), vec![20]);
    assert!(db.exec("INSERT INTO nd VALUES (NULL)", &[]).is_err());
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn create_table_with_meta_rolls_back() {
    let (dir, path) = temp_db("rollback");
    {
        let mut db = PGlite::open(&path).unwrap();
        exec(&mut db, &["BEGIN", "CREATE TABLE g (a INT, b INT GENERATED ALWAYS AS (a + 1) STORED)", "ROLLBACK"]);
        exec(&mut db, &["CREATE TABLE g (a INT, b INT)", "INSERT INTO g VALUES (1, 5)"]);
    }
    let mut db = PGlite::open(&path).unwrap();
    // bảng tạo lại không còn là generated
    assert_eq!(ints(&mut db, "SELECT b FROM g", "b"), vec![5]);
    exec(&mut db, &["UPDATE g SET b = 9"]);
    assert_eq!(ints(&mut db, "SELECT b FROM g", "b"), vec![9]);
    let _ = std::fs::remove_dir_all(dir);
}
