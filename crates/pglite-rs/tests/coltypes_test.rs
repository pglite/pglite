//! Kiểu khai báo của cột (`SchemaOp::ColumnTypes`): làm tròn numeric(p,s), giới hạn varchar(n),
//! nhiều cột SERIAL, định dạng wire — và mọi thứ còn nguyên sau khi đóng / mở lại DB.
use pglite_rs::PGlite;

fn tmp_db(tag: &str) -> (std::path::PathBuf, String) {
    let nanos = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos();
    let dir = std::env::temp_dir().join(format!("pglite_coltypes_{}_{}_{}", tag, std::process::id(), nanos));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("t.db").to_str().unwrap().to_string();
    (dir, path)
}

fn wire(db: &mut PGlite, sql: &str) -> Vec<Vec<Option<String>>> {
    db.query_wire(sql, &[]).unwrap_or_else(|e| panic!("{sql}: {e}")).rows
}

fn s(v: &str) -> Option<String> {
    Some(v.to_string())
}

#[test]
fn declared_types_survive_reopen_alter_rename_truncate() {
    let (dir, path) = tmp_db("persist");
    {
        let mut db = PGlite::open(&path).unwrap();
        db.exec("CREATE TABLE p (id SERIAL PRIMARY KEY, price NUMERIC(6,2), code VARCHAR(3), c CHAR(3), at TIMESTAMPTZ, seq BIGSERIAL)", &[]).unwrap();
        db.exec("INSERT INTO p (price, code, c, at) VALUES (3.256, 'ab', 'x', '2024-01-02 10:00+07')", &[]).unwrap();
        assert_eq!(wire(&mut db, "SELECT price, code, c, at, seq FROM p"), vec![vec![s("3.26"), s("ab"), s("x  "), s("2024-01-02 03:00:00+00"), s("1")]]);
        let e = db.exec("INSERT INTO p (code) VALUES ('abcd')", &[]).unwrap_err();
        assert_eq!(pglite_rs::sqlstate(&e), "22001", "{e}");
        let e = db.exec("INSERT INTO p (price) VALUES (12345)", &[]).unwrap_err();
        assert_eq!(pglite_rs::sqlstate(&e), "22003", "{e}");
        // phần thừa toàn dấu cách được cắt như PostgreSQL
        db.exec("INSERT INTO p (code) VALUES ('xy   ')", &[]).unwrap();
        db.exec("ALTER TABLE p RENAME COLUMN code TO sku", &[]).unwrap();
        db.exec("ALTER TABLE p ALTER COLUMN price TYPE NUMERIC(8,3)", &[]).unwrap();
        db.exec("ALTER TABLE p ADD COLUMN qty SMALLINT", &[]).unwrap();
    }
    {
        let mut db = PGlite::open(&path).unwrap();
        assert!(db.exec("INSERT INTO p (sku) VALUES ('wxyz')", &[]).is_err(), "varchar(3) sau khi đổi tên và mở lại");
        let e = db.exec("INSERT INTO p (qty) VALUES (40000)", &[]).unwrap_err();
        assert_eq!(pglite_rs::sqlstate(&e), "22003", "{e}");
        db.exec("INSERT INTO p (price, sku) VALUES (1.23456, 'q')", &[]).unwrap();
        assert_eq!(wire(&mut db, "SELECT price, seq FROM p ORDER BY id"), vec![vec![s("3.260"), s("1")], vec![None, s("2")], vec![s("1.235"), s("3")]]);
        db.exec("TRUNCATE p", &[]).unwrap();
    }
    {
        let mut db = PGlite::open(&path).unwrap();
        assert!(db.exec("INSERT INTO p (sku) VALUES ('wxyz')", &[]).is_err(), "kiểu khai báo còn sau TRUNCATE");
        db.exec("INSERT INTO p (price) VALUES (2)", &[]).unwrap();
        assert_eq!(wire(&mut db, "SELECT price FROM p"), vec![vec![s("2.000")]]);
    }
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn rollback_keeps_declared_types_consistent() {
    let mut db = PGlite::in_memory().unwrap();
    db.exec("CREATE TABLE r (a NUMERIC(5,1))", &[]).unwrap();
    db.exec("BEGIN", &[]).unwrap();
    db.exec("ALTER TABLE r ALTER COLUMN a TYPE NUMERIC(5,3)", &[]).unwrap();
    db.exec("ROLLBACK", &[]).unwrap();
    db.exec("INSERT INTO r VALUES (1.26)", &[]).unwrap();
    assert_eq!(wire(&mut db, "SELECT a FROM r"), vec![vec![s("1.3")]]);
}

#[test]
fn old_tables_without_declared_types_are_unchanged() {
    // Bảng không có op ColumnTypes (do bản cũ tạo) giữ hành vi cũ: không làm tròn, không giới hạn độ dài.
    let mut db = PGlite::in_memory().unwrap();
    db.exec("CREATE TABLE o (a NUMERIC, b TEXT)", &[]).unwrap();
    db.exec("INSERT INTO o VALUES (3.256, 'abcdef')", &[]).unwrap();
    assert_eq!(wire(&mut db, "SELECT a, b FROM o"), vec![vec![s("3.256"), s("abcdef")]]);
}
