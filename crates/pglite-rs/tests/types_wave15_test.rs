//! Đợt 15: kiểu composite / range / tsvector lưu bền qua mở lại DB và ROLLBACK.
use pglite_rs::PGlite;

fn temp_db(tag: &str) -> (std::path::PathBuf, String) {
    let nanos = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos();
    let dir = std::env::temp_dir().join(format!("pglite_w15_{}_{}_{}", tag, std::process::id(), nanos));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("t.db").to_str().unwrap().to_string();
    (dir, path)
}

fn exec(db: &mut PGlite, sqls: &[&str]) {
    for sql in sqls {
        db.exec(sql, &[]).unwrap_or_else(|e| panic!("{sql}: {e}"));
    }
}

fn one(db: &mut PGlite, sql: &str, col: &str) -> serde_json::Value {
    db.query(sql, &[]).unwrap_or_else(|e| panic!("{sql}: {e}")).rows[0][col].clone()
}

#[test]
fn composite_range_fts_survive_reopen() {
    let (dir, path) = temp_db("reopen");
    {
        let mut db = PGlite::open(&path).unwrap();
        exec(
            &mut db,
            &[
                "CREATE TYPE pair AS (a int, b text)",
                "CREATE TABLE r (id int PRIMARY KEY, v pair, during daterange, doc tsvector)",
                "INSERT INTO r VALUES (1, ROW(2, 'y'), '[2024-01-01,2024-01-03]', to_tsvector('english', 'cats running'))",
            ],
        );
    }
    let mut db = PGlite::open(&path).unwrap();
    assert_eq!(one(&mut db, "SELECT (v).a AS a FROM r", "a"), 2);
    assert_eq!(one(&mut db, "SELECT during FROM r", "during"), "[2024-01-01,2024-01-04)");
    assert_eq!(one(&mut db, "SELECT during @> '2024-01-03'::date AS c FROM r", "c"), true);
    assert_eq!(one(&mut db, "SELECT doc @@ to_tsquery('english', 'run & cat') AS m FROM r", "m"), true);
    // kiểu composite đã lưu: tạo lại thì lỗi
    assert!(db.exec("CREATE TYPE pair AS (c int)", &[]).is_err());
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn composite_type_rollback() {
    let (dir, path) = temp_db("rollback");
    let mut db = PGlite::open(&path).unwrap();
    exec(&mut db, &["BEGIN", "CREATE TYPE tmp_pair AS (a int)", "ROLLBACK"]);
    assert!(db.query("SELECT ROW(1)::tmp_pair", &[]).map(|r| r.rows[0].to_string()).unwrap_or_default() != "{\"row\":\"(1)\"}");
    exec(&mut db, &["CREATE TYPE tmp_pair AS (a int)"]);
    drop(db);
    let mut db = PGlite::open(&path).unwrap();
    exec(&mut db, &["DROP TYPE tmp_pair"]);
    drop(db);
    let mut db = PGlite::open(&path).unwrap();
    exec(&mut db, &["CREATE TYPE tmp_pair AS (a int, b int)"]);
    let _ = std::fs::remove_dir_all(dir);
}
