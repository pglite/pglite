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

#[test]
fn partitions_survive_reopen_and_rollback() {
    let (dir, path) = temp_db("part");
    {
        let mut db = PGlite::open(&path).unwrap();
        exec(
            &mut db,
            &[
                "CREATE TABLE ev (id int, at date) PARTITION BY RANGE (at)",
                "CREATE TABLE ev_2024 PARTITION OF ev FOR VALUES FROM ('2024-01-01') TO ('2025-01-01')",
                "CREATE TABLE ev_2025 PARTITION OF ev FOR VALUES FROM ('2025-01-01') TO ('2026-01-01')",
                "INSERT INTO ev VALUES (1, '2024-05-01'), (2, '2025-05-01')",
            ],
        );
        // lỗi giữa câu: không dòng nào được ghi
        assert!(db.exec("INSERT INTO ev VALUES (3, '2024-06-01'), (4, '2031-01-01')", &[]).is_err());
        exec(&mut db, &["BEGIN", "INSERT INTO ev VALUES (5, '2025-07-01')", "ROLLBACK"]);
    }
    let mut db = PGlite::open(&path).unwrap();
    assert_eq!(one(&mut db, "SELECT count(*) AS c FROM ev", "c"), 2);
    assert_eq!(one(&mut db, "SELECT count(*) AS c FROM ev_2025", "c"), 1);
    assert!(db.exec("INSERT INTO ev VALUES (6, '2031-01-01')", &[]).is_err());
    exec(&mut db, &["UPDATE ev SET at = '2024-12-31' WHERE id = 2"]);
    assert_eq!(one(&mut db, "SELECT count(*) AS c FROM ev_2024", "c"), 2);
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn schema_namespaces_survive_reopen_and_keep_legacy_names() {
    let (dir, path) = temp_db("ns");
    {
        let mut db = PGlite::open(&path).unwrap();
        exec(
            &mut db,
            &[
                // bảng `x` ở public, như bảng mà bản cũ tạo từ `app.x` (hồi đó schema chỉ là tên)
                "CREATE TABLE x (v int)",
                "INSERT INTO x VALUES (1)",
                "CREATE SCHEMA app",
                "CREATE TABLE app.t (v int)",
                "INSERT INTO app.t VALUES (7)",
                "CREATE TABLE t (v int)",
                "INSERT INTO t VALUES (8), (9)",
            ],
        );
    }
    let mut db = PGlite::open(&path).unwrap();
    assert_eq!(one(&mut db, "SELECT count(*) AS c FROM app.t", "c"), 1);
    assert_eq!(one(&mut db, "SELECT count(*) AS c FROM t", "c"), 2);
    // `app.x` không tồn tại riêng: dùng bảng `x` như trước đợt này
    assert_eq!(one(&mut db, "SELECT v FROM app.x", "v"), 1);
    exec(&mut db, &["SET search_path TO app, public"]);
    assert_eq!(one(&mut db, "SELECT v FROM t", "v"), 7);
    let r = db.query("SELECT table_schema, table_name FROM information_schema.tables WHERE table_name = 't' ORDER BY 1", &[]).unwrap();
    assert_eq!(r.rows.len(), 2);
    assert_eq!(r.rows[0]["table_schema"], "app");
    let _ = std::fs::remove_dir_all(dir);
}
