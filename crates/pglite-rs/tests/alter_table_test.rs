//! ALTER TABLE: các mẫu waiki dùng (ADD COLUMN IF NOT EXISTS, ALTER COLUMN SET/DROP DEFAULT, TYPE,
//! DROP CONSTRAINT IF EXISTS + ADD CONSTRAINT FOREIGN KEY) và mọi thao tác phải replay đúng.
use pglite_rs::PGlite;
use serde_json::json;
use std::path::PathBuf;

fn temp_db(tag: &str) -> (PathBuf, String) {
    let nanos = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos();
    let dir = std::env::temp_dir().join(format!("pglite_alter_{}_{}_{}", tag, std::process::id(), nanos));
    std::fs::create_dir_all(&dir).unwrap();
    let db = dir.join("t.db").to_str().unwrap().to_string();
    (dir, db)
}

fn exec_all(db: &mut PGlite, sqls: &[&str]) {
    for sql in sqls {
        db.exec(sql, &[]).unwrap_or_else(|e| panic!("{sql}: {e}"));
    }
}

fn rows(db: &mut PGlite, sql: &str) -> Vec<serde_json::Value> {
    db.query(sql, &[]).unwrap_or_else(|e| panic!("{sql}: {e}")).rows
}

#[test]
fn waiki_alter_patterns_and_replay() {
    let (dir, path) = temp_db("waiki");
    let expected;
    {
        let mut db = PGlite::open(&path).unwrap();
        exec_all(&mut db, &[
            "CREATE TABLE IF NOT EXISTS specialists (id SERIAL PRIMARY KEY, name TEXT NOT NULL)",
            "CREATE TABLE IF NOT EXISTS slots (id SERIAL PRIMARY KEY, specialist_id INT, duration INT DEFAULT 30, note VARCHAR(100), status TEXT DEFAULT 'open')",
            "INSERT INTO specialists (name) VALUES ('A'), ('B')",
            "INSERT INTO slots (specialist_id, note) VALUES (1, 'x'), (2, 'y'), (99, 'orphan')",
            // core/customer-auth: ADD COLUMN IF NOT EXISTS (chạy lại lần 2 phải không lỗi)
            "ALTER TABLE slots ADD COLUMN IF NOT EXISTS room VARCHAR(50) NOT NULL DEFAULT 'R1'",
            "ALTER TABLE slots ADD COLUMN IF NOT EXISTS room VARCHAR(50) NOT NULL DEFAULT 'R1'",
            "ALTER TABLE slots ADD COLUMN IF NOT EXISTS price DOUBLE PRECISION DEFAULT 1.5, ADD COLUMN IF NOT EXISTS starts_at timestamp with time zone",
            // specialist-schedules: SET DEFAULT ; wellness-habits: DROP DEFAULT
            "ALTER TABLE slots ALTER COLUMN duration SET DEFAULT 45",
            "ALTER TABLE slots ALTER COLUMN status DROP DEFAULT",
            // wellness-assistant: TYPE VARCHAR(150)
            "ALTER TABLE slots ALTER COLUMN note TYPE VARCHAR(150)",
            // wellness-library: DROP CONSTRAINT IF EXISTS rồi ADD CONSTRAINT FOREIGN KEY ... ON DELETE RESTRICT
            "DELETE FROM slots WHERE specialist_id = 99",
            "ALTER TABLE slots DROP CONSTRAINT IF EXISTS slots_specialist_fk",
            "ALTER TABLE slots ADD CONSTRAINT slots_specialist_fk FOREIGN KEY (specialist_id) REFERENCES specialists(id) ON DELETE RESTRICT",
            "INSERT INTO slots (specialist_id) VALUES (1)",
        ]);
        let r = rows(&mut db, "SELECT duration, status, room, price FROM slots WHERE id = 4");
        assert_eq!(r, vec![json!({"duration": 45, "status": null, "room": "R1", "price": 1.5})]);
        assert!(db.exec("INSERT INTO slots (specialist_id) VALUES (42)", &[]).is_err(), "FK mới phải được ép");
        assert!(db.exec("DELETE FROM specialists WHERE id = 1", &[]).is_err(), "ON DELETE RESTRICT");
        assert!(db.exec("ALTER TABLE slots ADD CONSTRAINT slots_specialist_fk CHECK (duration > 0)", &[]).is_err(), "trùng tên");
        expected = rows(&mut db, "SELECT * FROM slots ORDER BY id");
    }
    let mut db = PGlite::open(&path).unwrap();
    assert_eq!(rows(&mut db, "SELECT * FROM slots ORDER BY id"), expected);
    assert!(db.exec("INSERT INTO slots (specialist_id) VALUES (42)", &[]).is_err(), "FK còn sau khi mở lại");
    exec_all(&mut db, &["INSERT INTO slots (specialist_id) VALUES (2)"]);
    assert_eq!(rows(&mut db, "SELECT duration, room FROM slots ORDER BY id DESC LIMIT 1"), vec![json!({"duration": 45, "room": "R1"})]);
    drop(db);
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn not_null_type_rename_and_atomic_multi_action() {
    let (dir, path) = temp_db("multi");
    {
        let mut db = PGlite::open(&path).unwrap();
        exec_all(&mut db, &[
            "CREATE TABLE t (id SERIAL PRIMARY KEY, code TEXT, qty TEXT, note TEXT CHECK (length(note) < 10))",
            "INSERT INTO t (code, qty, note) VALUES ('a', '1', 'n1'), (NULL, '2', 'n2')",
        ]);
        // SET NOT NULL khi còn NULL: lỗi, không đổi gì
        let e = db.exec("ALTER TABLE t ALTER COLUMN code SET NOT NULL", &[]).unwrap_err();
        assert!(e.contains("contains null values"), "{e}");
        exec_all(&mut db, &["UPDATE t SET code = 'b' WHERE code IS NULL", "ALTER TABLE t ALTER COLUMN code SET NOT NULL"]);
        assert!(db.exec("INSERT INTO t (code) VALUES (NULL)", &[]).is_err());
        exec_all(&mut db, &["ALTER TABLE t ALTER COLUMN code DROP NOT NULL", "INSERT INTO t (code, qty) VALUES (NULL, '3')"]);
        // TYPE ... USING: giá trị được chuyển và ghi lại
        exec_all(&mut db, &["ALTER TABLE t ALTER COLUMN qty TYPE INTEGER USING qty::integer"]);
        assert_eq!(rows(&mut db, "SELECT qty FROM t ORDER BY id"), vec![json!({"qty": 1}), json!({"qty": 2}), json!({"qty": 3})]);
        exec_all(&mut db, &["INSERT INTO t (code, qty) VALUES ('z', 10)"]);
        // Chuyển kiểu lỗi: không đổi gì
        exec_all(&mut db, &["UPDATE t SET code = 'abc' WHERE id = 1"]);
        let e = db.exec("ALTER TABLE t ALTER COLUMN code TYPE INTEGER", &[]).unwrap_err();
        assert!(e.contains("invalid input syntax"), "{e}");
        // Nhiều thao tác, thao tác sau lỗi: thao tác trước cũng bị huỷ
        let e = db.exec("ALTER TABLE t ADD COLUMN extra INT DEFAULT 1, ADD COLUMN code TEXT", &[]).unwrap_err();
        assert!(e.contains("already exists"), "{e}");
        assert!(rows(&mut db, "SELECT * FROM t ORDER BY id")[0].get("extra").is_none(), "cột extra phải bị huỷ");
        // ADD + DROP trong một lệnh
        exec_all(&mut db, &["ALTER TABLE t ADD COLUMN flag BOOLEAN DEFAULT false, DROP COLUMN note"]);
        // RENAME COLUMN / RENAME TO
        exec_all(&mut db, &["ALTER TABLE t RENAME COLUMN code TO sku", "ALTER TABLE t RENAME TO items"]);
        assert!(db.query("SELECT * FROM t", &[]).is_err());
        // NOT NULL không có DEFAULT trên bảng có dữ liệu: lỗi
        let e = db.exec("ALTER TABLE items ADD COLUMN must TEXT NOT NULL", &[]).unwrap_err();
        assert!(e.contains("contains null values"), "{e}");
        // Thao tác không ảnh hưởng dữ liệu: chấp nhận
        exec_all(&mut db, &["ALTER TABLE items OWNER TO postgres", "ALTER TABLE IF EXISTS nope ADD COLUMN x INT"]);
    }
    let mut db = PGlite::open(&path).unwrap();
    let r = rows(&mut db, "SELECT id, sku, qty, flag FROM items ORDER BY id");
    assert_eq!(r, vec![
        json!({"id": 1, "sku": "abc", "qty": 1, "flag": false}),
        json!({"id": 2, "sku": "b", "qty": 2, "flag": false}),
        // id 3 bị lệnh INSERT lỗi dùng mất (như sequence của PostgreSQL)
        json!({"id": 4, "sku": null, "qty": 3, "flag": false}),
        json!({"id": 5, "sku": "z", "qty": 10, "flag": false}),
    ]);
    drop(db);
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn constraints_follow_renames_and_drops() {
    let mut db = PGlite::in_memory().unwrap();
    exec_all(&mut db, &[
        "CREATE TABLE p (id SERIAL PRIMARY KEY, code TEXT UNIQUE)",
        "CREATE TABLE c (id SERIAL PRIMARY KEY, p_id INT REFERENCES p(id) ON DELETE CASCADE, qty INT CHECK (qty > 0))",
        "INSERT INTO p (code) VALUES ('a')",
        "INSERT INTO c (p_id, qty) VALUES (1, 1)",
        "ALTER TABLE c RENAME COLUMN qty TO amount",
        "ALTER TABLE p RENAME TO parents",
    ]);
    assert!(db.exec("INSERT INTO c (p_id, amount) VALUES (1, -1)", &[]).is_err(), "CHECK theo tên cột mới");
    assert!(db.exec("INSERT INTO c (p_id, amount) VALUES (9, 1)", &[]).is_err(), "FK theo tên bảng mới");
    assert!(db.exec("INSERT INTO parents (code) VALUES ('a')", &[]).is_err());
    exec_all(&mut db, &["DELETE FROM parents WHERE id = 1"]);
    assert!(rows(&mut db, "SELECT * FROM c").is_empty(), "cascade sau khi đổi tên");
    let e = db.exec("ALTER TABLE parents DROP COLUMN id", &[]).unwrap_err();
    assert!(e.contains("depend on it"), "{e}");
    exec_all(&mut db, &["ALTER TABLE parents DROP COLUMN id CASCADE", "INSERT INTO c (p_id, amount) VALUES (123, 1)"]);
    // Tên ràng buộc giữ nguyên khi đổi tên bảng (như PostgreSQL)
    exec_all(&mut db, &["ALTER TABLE parents DROP CONSTRAINT p_code_key", "INSERT INTO parents (code) VALUES ('a')"]);
    assert!(db.exec("ALTER TABLE parents DROP CONSTRAINT p_code_key", &[]).is_err());
}
