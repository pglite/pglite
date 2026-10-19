//! INSERT ... ON CONFLICT (các mẫu waiki dùng), INSERT ... SELECT, DEFAULT VALUES, `$n::type`, RETURNING.
use pglite_rs::{PGlite, Value};
use serde_json::json;
use std::path::PathBuf;

fn temp_db(tag: &str) -> (PathBuf, String) {
    let nanos = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos();
    let dir = std::env::temp_dir().join(format!("pglite_upsert_{}_{}_{}", tag, std::process::id(), nanos));
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
fn do_nothing_never_inserts_junk_rows() {
    let mut db = PGlite::in_memory().unwrap();
    exec_all(&mut db, &[
        "CREATE TABLE p (id INT PRIMARY KEY, name TEXT)",
        "INSERT INTO p (id, name) VALUES (1, 'a')",
    ]);
    let r = db.exec("INSERT INTO p (id, name) VALUES (1, 'b') ON CONFLICT (id) DO NOTHING", &[]).unwrap();
    assert_eq!(r.row_count, 0);
    let r = db.exec("INSERT INTO p (id, name) VALUES (2, 'x') ON CONFLICT (id) DO NOTHING", &[]).unwrap();
    assert_eq!(r.row_count, 1);
    // ON CONFLICT DO NOTHING không có đích: mọi ràng buộc
    let r = db.exec("INSERT INTO p (id, name) VALUES (2, 'y'), (3, 'z') ON CONFLICT DO NOTHING", &[]).unwrap();
    assert_eq!(r.row_count, 1);
    assert_eq!(rows(&mut db, "SELECT * FROM p ORDER BY id"), vec![
        json!({"id": 1, "name": "a"}),
        json!({"id": 2, "name": "x"}),
        json!({"id": 3, "name": "z"}),
    ]);
}

#[test]
fn do_update_with_excluded_composite_keys_and_returning() {
    let (dir, path) = temp_db("doupdate");
    let expected;
    {
        let mut db = PGlite::open(&path).unwrap();
        exec_all(&mut db, &[
            "CREATE TABLE settings (tenant_id INT PRIMARY KEY, settings JSONB DEFAULT '{}'::jsonb, hits INT DEFAULT 0, updated_at TIMESTAMP)",
            "CREATE TABLE mfa (id SERIAL PRIMARY KEY, tenant_id INT NOT NULL, customer_id INT NOT NULL, kind VARCHAR(10) NOT NULL, secret TEXT, UNIQUE (tenant_id, customer_id, kind))",
        ]);
        // Mẫu core/customer-auth: ON CONFLICT (tenant_id) DO UPDATE SET settings = EXCLUDED.settings, updated_at = CURRENT_TIMESTAMP
        let sql = "INSERT INTO settings (tenant_id, settings) VALUES ($1, $2::jsonb) ON CONFLICT (tenant_id) DO UPDATE SET settings = EXCLUDED.settings, hits = settings.hits + 1, updated_at = CURRENT_TIMESTAMP RETURNING tenant_id, settings, hits";
        let r = db.exec(sql, &[Value::Int(7), Value::text("{\"theme\":\"dark\"}")]).unwrap();
        assert_eq!(r.rows, vec![json!({"tenant_id": 7, "settings": {"theme": "dark"}, "hits": 0})]);
        let r = db.exec(sql, &[Value::Int(7), Value::text("{\"theme\":\"light\"}")]).unwrap();
        assert_eq!(r.row_count, 1);
        assert_eq!(r.rows, vec![json!({"tenant_id": 7, "settings": {"theme": "light"}, "hits": 1})]);
        assert!(rows(&mut db, "SELECT updated_at FROM settings")[0]["updated_at"].is_string());
        // Khoá ghép
        let up = "INSERT INTO mfa (tenant_id, customer_id, kind, secret) VALUES ($1, $2, $3, $4) ON CONFLICT (tenant_id, customer_id, kind) DO UPDATE SET secret = EXCLUDED.secret RETURNING id, secret";
        let a = db.exec(up, &[Value::Int(1), Value::Int(9), Value::text("totp"), Value::text("s1")]).unwrap();
        let b = db.exec(up, &[Value::Int(1), Value::Int(9), Value::text("totp"), Value::text("s2")]).unwrap();
        assert_eq!(a.rows[0]["id"], b.rows[0]["id"], "cập nhật đúng dòng cũ");
        assert_eq!(b.rows[0]["secret"], "s2");
        db.exec(up, &[Value::Int(1), Value::Int(9), Value::text("sms"), Value::text("s3")]).unwrap();
        // DO UPDATE ... WHERE: điều kiện sai thì không cập nhật, không trả dòng
        let r = db.exec("INSERT INTO mfa (tenant_id, customer_id, kind, secret) VALUES (1, 9, 'sms', 'nope') ON CONFLICT (tenant_id, customer_id, kind) DO UPDATE SET secret = EXCLUDED.secret WHERE mfa.secret IS NULL RETURNING id", &[]).unwrap();
        assert_eq!(r.row_count, 0);
        assert!(r.rows.is_empty());
        // ON CONSTRAINT
        db.exec("INSERT INTO mfa (tenant_id, customer_id, kind, secret) VALUES (1, 9, 'sms', 'by-name') ON CONFLICT ON CONSTRAINT mfa_tenant_id_customer_id_kind_key DO UPDATE SET secret = EXCLUDED.secret", &[]).unwrap();
        // Hai dòng cùng khoá trong một lệnh DO UPDATE: lỗi như PostgreSQL, và không để lại gì
        let e = db.exec("INSERT INTO mfa (tenant_id, customer_id, kind) VALUES (5, 5, 'sms'), (5, 5, 'sms') ON CONFLICT (tenant_id, customer_id, kind) DO UPDATE SET secret = 'x'", &[]).unwrap_err();
        assert!(e.contains("cannot affect row a second time"), "{e}");
        assert!(rows(&mut db, "SELECT * FROM mfa WHERE tenant_id = 5").is_empty());
        // Ràng buộc khác (NOT NULL) vẫn được kiểm tra trên nhánh UPDATE
        assert!(db.exec("INSERT INTO mfa (tenant_id, customer_id, kind) VALUES (1, 9, 'sms') ON CONFLICT (tenant_id, customer_id, kind) DO UPDATE SET kind = NULL", &[]).is_err());
        expected = (rows(&mut db, "SELECT tenant_id, settings, hits FROM settings"), rows(&mut db, "SELECT * FROM mfa ORDER BY id"));
        assert_eq!(expected.1.len(), 2);
        assert_eq!(expected.1[1]["secret"], "by-name");
    }
    let mut db = PGlite::open(&path).unwrap();
    assert_eq!((rows(&mut db, "SELECT tenant_id, settings, hits FROM settings"), rows(&mut db, "SELECT * FROM mfa ORDER BY id")), expected, "upsert phải replay đúng");
    drop(db);
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn upsert_on_legacy_table_without_unique_catalog_matches_by_columns() {
    // Bảng có UNIQUE nhưng catalog trống (giống bảng do bản cũ tạo): upsert vẫn theo đúng cột.
    let mut db = PGlite::in_memory().unwrap();
    exec_all(&mut db, &["CREATE TABLE kv (id SERIAL PRIMARY KEY, k TEXT, v TEXT)", "INSERT INTO kv (k, v) VALUES ('a', '1')"]);
    exec_all(&mut db, &["INSERT INTO kv (k, v) VALUES ('a', '2') ON CONFLICT (k) DO UPDATE SET v = EXCLUDED.v"]);
    assert_eq!(rows(&mut db, "SELECT k, v FROM kv"), vec![json!({"k": "a", "v": "2"})]);
}

#[test]
fn insert_forms_params_casts_and_returning_expressions() {
    let mut db = PGlite::in_memory().unwrap();
    exec_all(&mut db, &[
        "CREATE TABLE a (id SERIAL PRIMARY KEY, name TEXT DEFAULT 'anon', meta JSONB, n INT, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)",
        "CREATE TABLE b (id SERIAL PRIMARY KEY, a_id INT, v TEXT)",
    ]);
    db.exec("INSERT INTO a (name, meta, n) VALUES ($1, $2::jsonb, $3::int)", &[Value::text("x"), Value::text("{\"k\":1}"), Value::text("5")]).unwrap();
    assert_eq!(rows(&mut db, "SELECT name, meta, n FROM a"), vec![json!({"name": "x", "meta": {"k": 1}, "n": 5})]);
    exec_all(&mut db, &[
        "INSERT INTO a DEFAULT VALUES",
        "INSERT INTO public.a (name) VALUES ('pub')",
        "INSERT INTO \"public\".\"a\" (name) VALUES ('quoted')",
        "INSERT INTO b (a_id, v) SELECT id, name FROM a WHERE id <= 2",
    ]);
    assert_eq!(rows(&mut db, "SELECT name FROM a ORDER BY id"), vec![json!({"name": "x"}), json!({"name": "anon"}), json!({"name": "pub"}), json!({"name": "quoted"})]);
    assert_eq!(rows(&mut db, "SELECT a_id, v FROM b ORDER BY id"), vec![json!({"a_id": 1, "v": "x"}), json!({"a_id": 2, "v": "anon"})]);
    let r = db.exec("INSERT INTO a (name) VALUES ('r') RETURNING id, upper(name) AS up, id + 1 AS nxt, name", &[]).unwrap();
    assert_eq!(r.rows, vec![json!({"id": 5, "up": "R", "nxt": 6, "name": "r"})]);
    let r = db.exec("INSERT INTO a (name) VALUES ('s') RETURNING *", &[]).unwrap();
    assert_eq!(r.rows[0]["name"], "s");
    // Chuỗi chứa từ khoá không bị hiểu nhầm
    exec_all(&mut db, &["INSERT INTO b (a_id, v) VALUES (1, 'x ON CONFLICT (id) DO NOTHING RETURNING y')"]);
    assert_eq!(rows(&mut db, "SELECT v FROM b WHERE id = 3"), vec![json!({"v": "x ON CONFLICT (id) DO NOTHING RETURNING y"})]);
}
