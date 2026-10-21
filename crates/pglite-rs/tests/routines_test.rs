//! Hàm / thủ tục / trigger do người dùng định nghĩa: lưu bền khi mở lại DB, CREATE FUNCTION trong giao dịch bị
//! ROLLBACK được hoàn tác, trigger đi theo bảng qua TRUNCATE, lỗi trong trigger huỷ cả câu lệnh.
use pglite_rs::PGlite;

fn temp_db(tag: &str) -> (std::path::PathBuf, String) {
    let nanos = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos();
    let dir = std::env::temp_dir().join(format!("pglite_rt_{}_{}_{}", tag, std::process::id(), nanos));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("r.db").to_str().unwrap().to_string();
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
fn routines_and_triggers_survive_reopen() {
    let (dir, path) = temp_db("reopen");
    {
        let mut db = PGlite::open(&path).unwrap();
        exec(
            &mut db,
            &[
                "CREATE TABLE t (id SERIAL PRIMARY KEY, n INT, updated INT DEFAULT 0)",
                "CREATE FUNCTION add1(i int) RETURNS int AS $$ SELECT i + 1 $$ LANGUAGE sql",
                "CREATE FUNCTION bump() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN NEW.updated := OLD.updated + 1; RETURN NEW; END $$",
                "CREATE TRIGGER tr BEFORE UPDATE ON t FOR EACH ROW EXECUTE FUNCTION bump()",
                "INSERT INTO t (n) VALUES (1)",
                // trong giao dịch bị ROLLBACK: không còn sau đó
                "BEGIN",
                "CREATE FUNCTION gone() RETURNS int AS $$ SELECT 1 $$ LANGUAGE sql",
                "DROP FUNCTION add1(int)",
                "ROLLBACK",
            ],
        );
        assert!(db.query("SELECT gone()", &[]).is_err());
        assert_eq!(ints(&mut db, "SELECT add1(41) AS a", "a"), vec![42]);
    }
    for round in 1..=2 {
        let mut db = PGlite::open(&path).unwrap();
        assert_eq!(ints(&mut db, "SELECT add1(n) AS a FROM t", "a"), vec![2]);
        exec(&mut db, &["UPDATE t SET n = 1"]);
        assert_eq!(ints(&mut db, "SELECT updated FROM t", "updated"), vec![round], "trigger sau khi mở lại, vòng {round}");
    }
    // TRUNCATE giữ trigger; lỗi trong trigger huỷ cả lệnh
    {
        let mut db = PGlite::open(&path).unwrap();
        exec(
            &mut db,
            &[
                "TRUNCATE t",
                "INSERT INTO t (n) VALUES (5)",
                "CREATE FUNCTION no_neg() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.n < 0 THEN RAISE EXCEPTION 'negative'; END IF; RETURN NEW; END $$",
                "CREATE TRIGGER chk BEFORE INSERT OR UPDATE ON t FOR EACH ROW EXECUTE FUNCTION no_neg()",
            ],
        );
        assert!(db.exec("UPDATE t SET n = -1", &[]).is_err());
        assert!(db.exec("INSERT INTO t (n) VALUES (7), (-2)", &[]).is_err());
        assert_eq!(ints(&mut db, "SELECT n FROM t ORDER BY n", "n"), vec![5], "lệnh lỗi trong trigger bị huỷ hết");
    }
    let mut db = PGlite::open(&path).unwrap();
    exec(&mut db, &["UPDATE t SET n = 6"]);
    assert_eq!(ints(&mut db, "SELECT updated FROM t", "updated"), vec![1], "trigger còn sau TRUNCATE + mở lại");
    assert!(db.exec("UPDATE t SET n = -6", &[]).is_err());
    exec(&mut db, &["DROP TRIGGER chk ON t", "UPDATE t SET n = -6"]);
    let _ = std::fs::remove_dir_all(dir);
}
