//! Ràng buộc được lưu và được thực thi: NOT NULL, UNIQUE (một cột, ghép, một phần), CHECK, FOREIGN KEY
//! (CASCADE / SET NULL / RESTRICT / NO ACTION), lệnh lỗi không để lại gì, và DB cũ có dữ liệu vi phạm.
use pglite_rs::PGlite;
use std::path::{Path, PathBuf};

fn temp_db(tag: &str) -> (PathBuf, String) {
    let nanos = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos();
    let dir = std::env::temp_dir().join(format!("pglite_cons_{}_{}_{}", tag, std::process::id(), nanos));
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

fn err(db: &mut PGlite, sql: &str) -> String {
    match db.exec(sql, &[]) {
        Ok(r) => panic!("expected error for {sql}, got {:?}", r.rows),
        Err(e) => e,
    }
}

fn count(db: &mut PGlite, table: &str) -> usize {
    rows(db, &format!("SELECT * FROM {table}")).len()
}

const SCHEMA: &[&str] = &[
    "CREATE TABLE tenants (id SERIAL PRIMARY KEY, code VARCHAR(50) NOT NULL UNIQUE, name VARCHAR(255) NOT NULL)",
    "CREATE TABLE users (id SERIAL PRIMARY KEY, email VARCHAR(255) NOT NULL UNIQUE, full_name VARCHAR(150) NOT NULL, deleted_at TIMESTAMP)",
    "CREATE TABLE tenant_members (id SERIAL PRIMARY KEY, tenant_id INT NOT NULL REFERENCES tenants(id) ON DELETE CASCADE, user_id INT NOT NULL REFERENCES users(id) ON DELETE CASCADE, role VARCHAR(50) NOT NULL DEFAULT 'member', UNIQUE (tenant_id, user_id))",
    "CREATE TABLE mfa (id SERIAL PRIMARY KEY, user_id INT REFERENCES users(id) ON DELETE SET NULL, kind VARCHAR(10) NOT NULL CHECK (kind IN ('totp','sms')))",
    "CREATE TABLE invoices (id SERIAL PRIMARY KEY, tenant_id INT NOT NULL REFERENCES tenants(id) ON DELETE RESTRICT, total NUMERIC CHECK (total >= 0))",
    "CREATE TABLE tenant_roles (id VARCHAR(50) NOT NULL, tenant_id INT NOT NULL, name VARCHAR(100) NOT NULL, PRIMARY KEY (id, tenant_id))",
];

fn seeded(db: &mut PGlite) {
    exec_all(db, SCHEMA);
    exec_all(db, &[
        "INSERT INTO tenants (code, name) VALUES ('t1', 'Tenant 1'), ('t2', 'Tenant 2')",
        "INSERT INTO users (email, full_name) VALUES ('a@x.vn', 'A'), ('b@x.vn', 'B')",
        "INSERT INTO tenant_members (tenant_id, user_id) VALUES (1, 1), (1, 2), (2, 1)",
        "INSERT INTO mfa (user_id, kind) VALUES (1, 'totp'), (2, 'sms')",
        "INSERT INTO invoices (tenant_id, total) VALUES (2, 100)",
        "INSERT INTO tenant_roles (id, tenant_id, name) VALUES ('admin', 1, 'Admin'), ('admin', 2, 'Admin')",
    ]);
}

#[test]
fn not_null_unique_check_are_enforced() {
    let mut db = PGlite::in_memory().unwrap();
    seeded(&mut db);
    let e = err(&mut db, "INSERT INTO users (email, full_name) VALUES ('c@x.vn', NULL)");
    assert!(e.contains("null value in column \"full_name\" of relation \"users\" violates not-null constraint"), "{e}");
    let e = err(&mut db, "INSERT INTO users (email) VALUES ('c@x.vn')");
    assert!(e.contains("not-null"), "{e}");
    let e = err(&mut db, "INSERT INTO users (email, full_name) VALUES ('a@x.vn', 'Dup')");
    assert!(e.contains("duplicate key value violates unique constraint \"users_email_key\""), "{e}");
    assert!(e.contains("Key (email)=(a@x.vn) already exists"), "{e}");
    // '0123' và 123 là hai giá trị khác nhau trong cột text
    exec_all(&mut db, &["INSERT INTO tenants (code, name) VALUES ('0123', 'x')", "INSERT INTO tenants (code, name) VALUES ('123', 'y')"]);
    // UNIQUE ghép
    let e = err(&mut db, "INSERT INTO tenant_members (tenant_id, user_id) VALUES (1, 1)");
    assert!(e.contains("tenant_members_tenant_id_user_id_key"), "{e}");
    // PK ghép
    let e = err(&mut db, "INSERT INTO tenant_roles (id, tenant_id, name) VALUES ('admin', 1, 'Again')");
    assert!(e.contains("tenant_roles_pkey"), "{e}");
    exec_all(&mut db, &["INSERT INTO tenant_roles (id, tenant_id, name) VALUES ('admin', 3, 'Other tenant')"]);
    // CHECK
    let e = err(&mut db, "INSERT INTO mfa (user_id, kind) VALUES (1, 'email')");
    assert!(e.contains("violates check constraint \"mfa_kind_check\""), "{e}");
    let e = err(&mut db, "UPDATE invoices SET total = -1");
    assert!(e.contains("invoices_total_check"), "{e}");
    // UPDATE gây trùng UNIQUE
    let e = err(&mut db, "UPDATE users SET email = 'a@x.vn' WHERE id = 2");
    assert!(e.contains("users_email_key"), "{e}");
    // NULL không trùng nhau
    exec_all(&mut db, &[
        "CREATE TABLE opt (id SERIAL PRIMARY KEY, code TEXT UNIQUE)",
        "INSERT INTO opt (code) VALUES (NULL), (NULL), ('x')",
    ]);
    assert_eq!(count(&mut db, "opt"), 3);
    // Đổi chỗ hai giá trị UNIQUE trong cùng một lệnh vẫn hợp lệ (trạng thái cuối không trùng)
    exec_all(&mut db, &["UPDATE users SET email = CASE WHEN id = 1 THEN 'b@x.vn' ELSE 'a@x.vn' END"]);
    assert_eq!(rows(&mut db, "SELECT email FROM users ORDER BY id"), vec![serde_json::json!({"email": "b@x.vn"}), serde_json::json!({"email": "a@x.vn"})]);
}

#[test]
fn failed_statement_leaves_nothing_in_memory_or_on_disk() {
    let (dir, path) = temp_db("atomic");
    {
        let mut db = PGlite::open(&path).unwrap();
        seeded(&mut db);
        // Dòng 2 trùng email: cả lệnh bị huỷ, kể cả dòng 1
        let e = err(&mut db, "INSERT INTO users (email, full_name) VALUES ('new@x.vn', 'New'), ('a@x.vn', 'Dup')");
        assert!(e.contains("users_email_key"), "{e}");
        assert_eq!(count(&mut db, "users"), 2);
        // Trong giao dịch, như PostgreSQL: lệnh lỗi làm giao dịch "aborted" (25P02), COMMIT = ROLLBACK
        exec_all(&mut db, &["BEGIN", "INSERT INTO users (email, full_name) VALUES ('lost@x.vn', 'Lost')"]);
        assert!(db.exec("INSERT INTO users (email, full_name) VALUES ('tx2@x.vn', NULL)", &[]).is_err());
        let e = err(&mut db, "SELECT 1");
        assert!(e.contains("current transaction is aborted"), "{e}");
        assert_eq!(db.exec("COMMIT", &[]).unwrap().command, "ROLLBACK");
        assert_eq!(count(&mut db, "users"), 2);
        // Savepoint cứu được phần trước của giao dịch
        exec_all(&mut db, &["BEGIN", "INSERT INTO users (email, full_name) VALUES ('tx@x.vn', 'Tx')", "SAVEPOINT s"]);
        assert!(db.exec("INSERT INTO users (email, full_name) VALUES ('tx2@x.vn', NULL)", &[]).is_err());
        exec_all(&mut db, &["ROLLBACK TO SAVEPOINT s", "COMMIT"]);
        assert_eq!(count(&mut db, "users"), 3);
    }
    let mut db = PGlite::open(&path).unwrap();
    let emails: Vec<String> = rows(&mut db, "SELECT email FROM users ORDER BY id").into_iter().map(|r| r["email"].as_str().unwrap().to_string()).collect();
    assert_eq!(emails, vec!["a@x.vn", "b@x.vn", "tx@x.vn"]);
    // Ràng buộc vẫn còn sau khi mở lại
    assert!(db.exec("INSERT INTO users (email, full_name) VALUES ('a@x.vn', 'Dup')", &[]).is_err());
    drop(db);
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn foreign_keys_are_enforced_with_actions() {
    let mut db = PGlite::in_memory().unwrap();
    seeded(&mut db);
    let e = err(&mut db, "INSERT INTO tenant_members (tenant_id, user_id) VALUES (1, 999)");
    assert!(e.contains("violates foreign key constraint \"tenant_members_user_id_fkey\""), "{e}");
    assert!(e.contains("Key (user_id)=(999) is not present in table \"users\""), "{e}");
    let e = err(&mut db, "UPDATE tenant_members SET user_id = 999 WHERE id = 1");
    assert!(e.contains("tenant_members_user_id_fkey"), "{e}");
    // RESTRICT: tenant 2 còn invoice
    let e = err(&mut db, "DELETE FROM tenants WHERE id = 2");
    assert!(e.contains("violates foreign key constraint \"invoices_tenant_id_fkey\" on table \"invoices\""), "{e}");
    assert_eq!(count(&mut db, "tenant_members"), 3, "cascade của lệnh lỗi phải bị huỷ");
    // CASCADE + SET NULL khi xoá user 1
    exec_all(&mut db, &["DELETE FROM users WHERE id = 1"]);
    assert_eq!(rows(&mut db, "SELECT tenant_id, user_id FROM tenant_members ORDER BY id"), vec![serde_json::json!({"tenant_id": 1, "user_id": 2})]);
    assert_eq!(rows(&mut db, "SELECT user_id, kind FROM mfa ORDER BY id"), vec![
        serde_json::json!({"user_id": null, "kind": "totp"}),
        serde_json::json!({"user_id": 2, "kind": "sms"}),
    ]);
    // NO ACTION (mặc định): đổi khoá cha đang được tham chiếu
    exec_all(&mut db, &["CREATE TABLE notes (id SERIAL PRIMARY KEY, user_id INT REFERENCES users(id), body TEXT)", "INSERT INTO notes (user_id, body) VALUES (2, 'n')"]);
    let e = err(&mut db, "DELETE FROM users WHERE id = 2");
    assert!(e.contains("notes_user_id_fkey"), "{e}");
    assert_eq!(count(&mut db, "users"), 1);
    // NULL ở cột khoá ngoại thì không kiểm tra
    exec_all(&mut db, &["INSERT INTO notes (user_id, body) VALUES (NULL, 'free')"]);
    // Tự tham chiếu + cascade nhiều tầng
    exec_all(&mut db, &[
        "CREATE TABLE cats (id SERIAL PRIMARY KEY, parent_id INT REFERENCES cats(id) ON DELETE CASCADE, name TEXT)",
        "INSERT INTO cats (parent_id, name) VALUES (NULL, 'root')",
        "INSERT INTO cats (parent_id, name) VALUES (1, 'a'), (2, 'b'), (3, 'c')",
        "DELETE FROM cats WHERE id = 1",
    ]);
    assert_eq!(count(&mut db, "cats"), 0);
    // ON UPDATE CASCADE
    exec_all(&mut db, &[
        "CREATE TABLE p (code TEXT PRIMARY KEY)",
        "CREATE TABLE c (id SERIAL PRIMARY KEY, code TEXT REFERENCES p(code) ON UPDATE CASCADE ON DELETE SET NULL)",
        "INSERT INTO p VALUES ('x')",
        "INSERT INTO c (code) VALUES ('x')",
        "UPDATE p SET code = 'y'",
    ]);
    assert_eq!(rows(&mut db, "SELECT code FROM c"), vec![serde_json::json!({"code": "y"})]);
    // Bảng cha phải tồn tại
    let e = err(&mut db, "CREATE TABLE bad (id INT, x INT REFERENCES missing(id))");
    assert!(e.contains("relation \"missing\" does not exist"), "{e}");
}

#[test]
fn constraints_and_cascades_survive_reopen() {
    let (dir, path) = temp_db("reopen");
    let snapshot = |db: &mut PGlite| {
        ["tenants", "users", "tenant_members", "mfa", "invoices", "tenant_roles"]
            .iter()
            .map(|t| rows(db, &format!("SELECT * FROM {t} ORDER BY 1")))
            .collect::<Vec<_>>()
    };
    let before;
    {
        let mut db = PGlite::open(&path).unwrap();
        seeded(&mut db);
        exec_all(&mut db, &[
            "DELETE FROM users WHERE id = 1",
            "BEGIN",
            "DELETE FROM users WHERE id = 2",
            "ROLLBACK",
            "TRUNCATE invoices",
            "DELETE FROM tenants WHERE id = 2",
        ]);
        before = snapshot(&mut db);
    }
    let mut db = PGlite::open(&path).unwrap();
    assert_eq!(snapshot(&mut db), before);
    // Mọi loại ràng buộc vẫn được thực thi sau khi mở lại (kể cả sau TRUNCATE)
    assert!(db.exec("INSERT INTO users (email, full_name) VALUES ('b@x.vn', 'Dup')", &[]).is_err());
    assert!(db.exec("INSERT INTO tenant_members (tenant_id, user_id) VALUES (1, 999)", &[]).is_err());
    assert!(db.exec("INSERT INTO invoices (tenant_id, total) VALUES (1, -5)", &[]).is_err());
    assert!(db.exec("INSERT INTO invoices (tenant_id, total) VALUES (42, 5)", &[]).is_err());
    assert!(db.exec("INSERT INTO tenant_roles (id, tenant_id, name) VALUES ('admin', 1, 'x')", &[]).is_err());
    // information_schema thấy ràng buộc đã lưu
    let fks = rows(&mut db, "SELECT constraint_name FROM information_schema.table_constraints WHERE constraint_type = 'FOREIGN KEY' ORDER BY constraint_name");
    assert!(fks.iter().any(|r| r["constraint_name"] == "tenant_members_user_id_fkey"), "{fks:?}");
    drop(db);
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn unique_indexes_including_partial() {
    let (dir, path) = temp_db("uidx");
    {
        let mut db = PGlite::open(&path).unwrap();
        exec_all(&mut db, &[
            "CREATE TABLE customers (id SERIAL PRIMARY KEY, tenant_id INT, phone TEXT, deleted_at TIMESTAMP)",
            "CREATE UNIQUE INDEX IF NOT EXISTS uq_customers_phone ON customers(tenant_id, phone) WHERE deleted_at IS NULL",
            "CREATE UNIQUE INDEX IF NOT EXISTS uq_customers_phone ON customers(tenant_id, phone) WHERE deleted_at IS NULL",
            "INSERT INTO customers (tenant_id, phone) VALUES (1, '0909')",
            "INSERT INTO customers (tenant_id, phone) VALUES (2, '0909')",
        ]);
        let e = err(&mut db, "INSERT INTO customers (tenant_id, phone) VALUES (1, '0909')");
        assert!(e.contains("uq_customers_phone"), "{e}");
        // Xoá mềm rồi tạo lại cùng số: hợp lệ với index một phần
        exec_all(&mut db, &[
            "UPDATE customers SET deleted_at = '2026-01-01 00:00:00' WHERE id = 1",
            "INSERT INTO customers (tenant_id, phone) VALUES (1, '0909')",
        ]);
        let e = err(&mut db, "UPDATE customers SET deleted_at = NULL WHERE id = 1");
        assert!(e.contains("uq_customers_phone"), "{e}");
        let e = err(&mut db, "CREATE UNIQUE INDEX uq_bad ON customers(phone)");
        assert!(e.contains("could not create unique index \"uq_bad\""), "{e}");
        let e = err(&mut db, "CREATE INDEX uq_customers_phone ON customers(phone)");
        assert!(e.contains("already exists"), "{e}");
    }
    let mut db = PGlite::open(&path).unwrap();
    assert!(db.exec("INSERT INTO customers (tenant_id, phone) VALUES (2, '0909')", &[]).is_err(), "index phải còn sau khi mở lại");
    exec_all(&mut db, &["DROP INDEX IF EXISTS uq_customers_phone", "INSERT INTO customers (tenant_id, phone) VALUES (2, '0909')"]);
    drop(db);
    let mut db = PGlite::open(&path).unwrap();
    assert_eq!(count(&mut db, "customers"), 4);
    drop(db);
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn drop_and_truncate_respect_foreign_keys() {
    let mut db = PGlite::in_memory().unwrap();
    seeded(&mut db);
    let e = err(&mut db, "DROP TABLE users");
    assert!(e.contains("other objects depend on it"), "{e}");
    let e = err(&mut db, "TRUNCATE users");
    assert!(e.contains("cannot truncate a table referenced in a foreign key constraint"), "{e}");
    exec_all(&mut db, &["TRUNCATE users CASCADE"]);
    assert_eq!(count(&mut db, "tenant_members"), 0);
    assert_eq!(count(&mut db, "mfa"), 0);
    exec_all(&mut db, &["DROP TABLE users CASCADE", "INSERT INTO mfa (user_id, kind) VALUES (77, 'sms')"]);
    let e = err(&mut db, "DROP TABLE nope");
    assert!(e.contains("does not exist"), "{e}");
    exec_all(&mut db, &["DROP TABLE IF EXISTS nope, public.mfa"]);
    assert!(db.query("SELECT * FROM mfa", &[]).is_err());
    let e = err(&mut db, "CREATE TABLE tenants (id INT)");
    assert!(e.contains("relation \"tenants\" already exists"), "{e}");
}

/// Fixture do engine 8f9b56c ghi: dữ liệu vi phạm UNIQUE/NOT NULL/FK/CHECK/PK ghép.
#[test]
fn legacy_db_with_violating_data_opens_identically_and_adopts_valid_constraints() {
    let fx = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures");
    let (dir, path) = temp_db("legacy");
    std::fs::copy(fx.join("legacy_constraints.db.rwal"), format!("{path}.rwal")).unwrap();
    let expected: serde_json::Value = serde_json::from_str(&std::fs::read_to_string(fx.join("legacy_constraints.expected.json")).unwrap()).unwrap();
    let queries: Vec<(&str, String)> = ["tenants", "users", "tenant_members", "auth_sessions", "mfa"]
        .iter()
        .map(|t| (*t, format!("SELECT * FROM {t} ORDER BY id")))
        .chain(std::iter::once(("tenant_roles", "SELECT * FROM tenant_roles ORDER BY id, tenant_id".to_string())))
        .collect();
    let snapshot = |db: &mut PGlite| {
        let mut m = serde_json::Map::new();
        for (k, q) in &queries {
            m.insert(k.to_string(), serde_json::Value::Array(rows(db, q)));
        }
        serde_json::Value::Object(m)
    };
    {
        let mut db = PGlite::open(&path).unwrap();
        assert_eq!(snapshot(&mut db), expected, "DB cũ phải mở ra y nguyên");
        // App khởi động lại: chạy lại đúng DDL cũ. Không được lỗi.
        for sql in DDL {
            db.exec(sql, &[]).unwrap_or_else(|e| panic!("{sql}: {e}"));
        }
        assert_eq!(snapshot(&mut db), expected, "nhận lại ràng buộc không được đổi dữ liệu");
        // Ràng buộc mà dữ liệu cũ thoả thì giờ được thực thi
        assert!(db.exec("INSERT INTO auth_sessions (id, user_id, tenant_id, expires_at) VALUES ('s-9', 999, 1, '2026-12-01 00:00:00')", &[]).is_ok(),
            "auth_sessions có dòng mồ côi (user 4 đã xoá) nên FK không được nhận");
        assert!(db.exec("INSERT INTO tenant_members (tenant_id, user_id, role) VALUES (999, 1, 'x')", &[]).is_err(), "FK tenant_id được nhận");
        // Ràng buộc bị dữ liệu cũ vi phạm thì không ép (users.email trùng, tenant_roles PK ghép trùng)
        db.exec("INSERT INTO users (username, email, full_name) VALUES ('z', 'a@x.vn', 'Z')", &[]).unwrap();
        db.exec("INSERT INTO tenant_roles (id, tenant_id, name) VALUES ('admin', 1, 'Lan 3')", &[]).unwrap();
        // NOT NULL đã lưu trong schema cũ vẫn được thực thi với dòng mới
        assert!(db.exec("INSERT INTO users (email, full_name) VALUES ('n@x.vn', NULL)", &[]).is_err());
        // Dòng cũ vi phạm vẫn sửa được ở cột khác
        db.exec("UPDATE users SET username = 'c' WHERE id = 3", &[]).unwrap();
        db.exec("UPDATE tenant_members SET role = 'still orphan' WHERE id = 3", &[]).unwrap();
        // CHECK được nhận? mfa có kind 'email' cũ -> không
        db.exec("INSERT INTO mfa (user_id, kind) VALUES (1, 'push')", &[]).unwrap();
    }
    let mut db = PGlite::open(&path).unwrap();
    assert!(db.exec("INSERT INTO tenant_members (tenant_id, user_id, role) VALUES (999, 1, 'x')", &[]).is_err(), "ràng buộc đã nhận phải còn sau khi mở lại");
    assert_eq!(rows(&mut db, "SELECT username FROM users WHERE id = 3"), vec![serde_json::json!({"username": "c"})]);
    drop(db);
    let _ = std::fs::remove_dir_all(dir);
}

const DDL: &[&str] = &[
    "CREATE TABLE IF NOT EXISTS tenants (id SERIAL PRIMARY KEY, code VARCHAR(50) NOT NULL UNIQUE, name VARCHAR(255) NOT NULL, status VARCHAR(20) NOT NULL DEFAULT 'active', settings JSONB DEFAULT '{}'::jsonb, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)",
    "CREATE TABLE IF NOT EXISTS users (id SERIAL PRIMARY KEY, username VARCHAR(100) UNIQUE, email VARCHAR(255) NOT NULL UNIQUE, full_name VARCHAR(150) NOT NULL, status VARCHAR(20) NOT NULL DEFAULT 'active', deleted_at TIMESTAMP)",
    "CREATE TABLE IF NOT EXISTS tenant_members (id SERIAL PRIMARY KEY, tenant_id INT NOT NULL REFERENCES tenants(id) ON DELETE CASCADE, user_id INT NOT NULL REFERENCES users(id) ON DELETE CASCADE, role VARCHAR(50) NOT NULL DEFAULT 'member', UNIQUE (tenant_id, user_id))",
    "CREATE TABLE IF NOT EXISTS auth_sessions (id VARCHAR(128) PRIMARY KEY, user_id INT NOT NULL REFERENCES users(id) ON DELETE CASCADE, tenant_id INT NOT NULL, expires_at TIMESTAMP NOT NULL)",
    "CREATE INDEX IF NOT EXISTS idx_auth_sessions_user ON auth_sessions(user_id)",
    "CREATE TABLE IF NOT EXISTS tenant_roles (id VARCHAR(50) NOT NULL, tenant_id INT NOT NULL, name VARCHAR(100) NOT NULL, PRIMARY KEY (id, tenant_id))",
    "CREATE TABLE IF NOT EXISTS mfa (id SERIAL PRIMARY KEY, user_id INT REFERENCES users(id) ON DELETE SET NULL, kind VARCHAR(10) NOT NULL CHECK (kind IN ('totp','sms')))",
    "CREATE UNIQUE INDEX IF NOT EXISTS uq_users_email_live ON users(email) WHERE deleted_at IS NULL",
];

/// Ghi một DB dùng mọi tính năng catalog mới vào `$PGLITE_NEW_DB` để engine cũ thử đọc
/// (scripts/compat_check.sh, chiều ngược): `cargo test --test constraints_test write_new_format_db -- --ignored`
#[test]
#[ignore]
fn write_new_format_db() {
    let path = std::env::var("PGLITE_NEW_DB").expect("PGLITE_NEW_DB");
    let mut db = PGlite::open(&path).unwrap();
    seeded(&mut db);
    exec_all(&mut db, &[
        "CREATE UNIQUE INDEX uq_users_live ON users(full_name) WHERE deleted_at IS NULL",
        "CREATE INDEX idx_members_role ON tenant_members(role)",
        "DELETE FROM users WHERE id = 1",
        "UPDATE tenants SET name = 'Doi ten' WHERE id = 2",
        "BEGIN",
        "INSERT INTO users (email, full_name) VALUES ('c@x.vn', 'C')",
        "COMMIT",
    ]);
    assert!(db.exec("INSERT INTO users (email, full_name) VALUES ('b@x.vn', 'dup')", &[]).is_err());
}

/// Fixture do bản 7db3c5a ghi (catalog ràng buộc, upsert, ALTER/RENAME, DO, TRUNCATE, ROLLBACK).
#[test]
fn previous_release_db_opens_identically() {
    let fx = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures");
    let (dir, path) = temp_db("legacy_v3");
    std::fs::copy(fx.join("legacy_v3.db.rwal"), format!("{path}.rwal")).unwrap();
    let expected: serde_json::Value = serde_json::from_str(&std::fs::read_to_string(fx.join("legacy_v3.expected.json")).unwrap()).unwrap();
    let queries = [
        ("tenants", "SELECT * FROM tenants ORDER BY id"),
        ("members", "SELECT * FROM members ORDER BY id"),
        ("roles", "SELECT * FROM roles ORDER BY code, tenant_id"),
        ("items", "SELECT * FROM items ORDER BY id"),
        ("logs", "SELECT * FROM logs ORDER BY id"),
    ];
    for round in 0..2 {
        let mut db = PGlite::open(&path).unwrap();
        let mut m = serde_json::Map::new();
        for (k, q) in queries {
            m.insert(k.to_string(), serde_json::Value::Array(rows(&mut db, q)));
        }
        assert_eq!(serde_json::Value::Object(m), expected, "lần mở {round}");
        assert!(db.exec("INSERT INTO tenants (code, name) VALUES ('a', 'dup')", &[]).is_err(), "UNIQUE còn");
        assert!(db.exec("INSERT INTO members (tenant_id, email) VALUES (99, 'q')", &[]).is_err(), "FK còn");
        assert!(db.exec("INSERT INTO items (code, qty) VALUES ('s2', 1)", &[]).is_err(), "unique index một phần còn");
        assert!(db.exec("INSERT INTO roles VALUES ('owner', 2, 'x')", &[]).is_err(), "PK ghép còn sau TRUNCATE");
    }
    let _ = std::fs::remove_dir_all(dir);
}
