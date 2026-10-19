//! Sinh fixture WAL "cũ" cho test tương thích (tests/wire_compat_test.rs).
//! Fixture đã commit được sinh bằng code TRƯỚC khi thêm query_wire/describe.
//! Chỉ chạy lại khi cố ý tạo fixture mới:
//!   cargo test --test gen_legacy_fixture -- --ignored
use pglite_rs::PGlite;

#[test]
#[ignore]
fn generate_legacy_fixture() {
    let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures");
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("legacy_v2.db");
    let path = path.to_str().unwrap().to_string();
    for suffix in ["", ".rwal", ".v2.rwal", ".wal"] {
        let _ = std::fs::remove_file(format!("{}{}", path, suffix));
    }

    let mut db = PGlite::open(&path).unwrap();
    for sql in LEGACY_SCRIPT {
        db.execute(sql, &[]).unwrap_or_else(|e| panic!("{sql}: {e}"));
    }
    db.flush();
    drop(db);

    // Snapshot kết quả đọc lại bằng chính code cũ, để so với code mới.
    let mut db = PGlite::open(&path).unwrap();
    let mut snap = serde_json::Map::new();
    for (k, q) in SNAPSHOT_QUERIES {
        snap.insert(k.to_string(), serde_json::Value::Array(db.execute(q, &[]).unwrap().rows));
    }
    std::fs::write(dir.join("legacy_v2.expected.json"), serde_json::to_string_pretty(&snap).unwrap()).unwrap();
}

pub const SNAPSHOT_QUERIES: &[(&str, &str)] = &[
    ("schools", "SELECT * FROM schools ORDER BY id"),
    ("schedules", "SELECT * FROM schedules ORDER BY id"),
];

pub const LEGACY_SCRIPT: &[&str] = &[
    "CREATE TABLE schools (id SERIAL PRIMARY KEY, name VARCHAR(100) NOT NULL, active BOOLEAN DEFAULT true, rating NUMERIC, meta JSONB, created_at TIMESTAMPTZ, opened DATE, code UUID, logo BYTEA, tags TEXT[], class_ids TEXT)",
    "CREATE TABLE schedules (id SERIAL PRIMARY KEY, school_id INTEGER, title TEXT, start_at TIMESTAMP, big BIGINT, slot TIME)",
    "INSERT INTO schools (name, active, rating, meta, created_at, opened, code, logo, tags, class_ids) VALUES ('Alpha', true, 4.5, '{\"a\":1}', '2026-10-12 08:00:00', '2026-01-02', 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11', '\\x0102', '{x,y}', '[7831,7832]')",
    "INSERT INTO schools (name, active, rating) VALUES ('Beta', false, 3)",
    "INSERT INTO schools (name) VALUES ('Gamma')",
    "INSERT INTO schedules (school_id, title, start_at, big, slot) VALUES (1, 'Math', '2026-10-12 08:00:00', 9000000000, '08:00:00')",
    "INSERT INTO schedules (school_id, title, start_at, big, slot) VALUES (2, 'Art', '2026-10-13 09:30:00', 1, '09:30:00')",
    "ALTER TABLE schools ADD COLUMN phone TEXT",
    "UPDATE schools SET phone = '0909' WHERE id = 1",
    "DELETE FROM schools WHERE id = 3",
    "COMMENT ON TABLE schools IS 'truong hoc'",
];

/// Fixture do engine 6427da5 ghi: BEGIN không đóng, PK trùng, UPDATE đổi PK và TRUNCATE.
/// Code mới phải đọc ra đúng như snapshot do chính code cũ đọc.
///   cargo test --test gen_legacy_fixture generate_legacy_edge_fixtures -- --ignored
#[test]
#[ignore]
fn generate_legacy_edge_fixtures() {
    let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures");
    for (name, script, queries) in [
        ("legacy_open_begin", LEGACY_OPEN_BEGIN_SCRIPT, &[("t", "SELECT * FROM t ORDER BY id")][..]),
        ("legacy_dup_pk", LEGACY_DUP_PK_SCRIPT, &[("d", "SELECT * FROM d ORDER BY id, v"), ("s", "SELECT * FROM s ORDER BY v"), ("c", "SELECT * FROM c ORDER BY id")][..]),
    ] {
        let path = dir.join(format!("{name}.db")).to_str().unwrap().to_string();
        for suffix in ["", ".rwal", ".v2.rwal", ".wal"] {
            let _ = std::fs::remove_file(format!("{}{}", path, suffix));
        }
        let mut db = PGlite::open(&path).unwrap();
        for sql in script {
            db.execute(sql, &[]).unwrap_or_else(|e| panic!("{sql}: {e}"));
        }
        db.flush();
        drop(db);
        let mut db = PGlite::open(&path).unwrap();
        let mut snap = serde_json::Map::new();
        for (k, q) in queries {
            snap.insert(k.to_string(), serde_json::Value::Array(db.execute(q, &[]).unwrap().rows));
        }
        std::fs::write(dir.join(format!("{name}.expected.json")), serde_json::to_string_pretty(&snap).unwrap()).unwrap();
    }
}

pub const LEGACY_OPEN_BEGIN_SCRIPT: &[&str] = &[
    "CREATE TABLE t (id SERIAL PRIMARY KEY, v TEXT)",
    "INSERT INTO t (v) VALUES ('committed')",
    "BEGIN",
    "INSERT INTO t (v) VALUES ('forgotten-begin')",
];

pub const LEGACY_DUP_PK_SCRIPT: &[&str] = &[
    "CREATE TABLE d (id INT PRIMARY KEY, v TEXT)",
    "INSERT INTO d VALUES (1, 'a')",
    "INSERT INTO d VALUES (1, 'b')",
    "INSERT INTO d VALUES (2, 'c')",
    "CREATE TABLE s (id INT PRIMARY KEY, v TEXT)",
    "INSERT INTO s VALUES (1, 'a')",
    "INSERT INTO s VALUES (2, 'b')",
    "UPDATE s SET id = id + 1",
    "CREATE TABLE c (id SERIAL PRIMARY KEY, v TEXT)",
    "INSERT INTO c (v) VALUES ('gone')",
    "TRUNCATE c",
    "INSERT INTO c (v) VALUES ('kept')",
];

/// Fixture do engine 8f9b56c ghi (trước khi có catalog ràng buộc): schema kiểu waiki (UNIQUE, NOT NULL, FK
/// CASCADE, CHECK, PK ghép, khoá text, index) nhưng dữ liệu VI PHẠM các ràng buộc đó, vì engine cũ không
/// kiểm tra. Code mới phải mở ra y hệt và không được từ chối replay.
///   cargo test --test gen_legacy_fixture generate_legacy_constraints_fixture -- --ignored
#[test]
#[ignore]
fn generate_legacy_constraints_fixture() {
    let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures");
    let name = "legacy_constraints";
    let path = dir.join(format!("{name}.db")).to_str().unwrap().to_string();
    for suffix in ["", ".rwal", ".v2.rwal", ".wal"] {
        let _ = std::fs::remove_file(format!("{}{}", path, suffix));
    }
    let mut db = PGlite::open(&path).unwrap();
    for sql in LEGACY_CONSTRAINTS_SCRIPT {
        db.execute(sql, &[]).unwrap_or_else(|e| panic!("{sql}: {e}"));
    }
    db.flush();
    drop(db);
    let mut db = PGlite::open(&path).unwrap();
    let mut snap = serde_json::Map::new();
    for (k, q) in LEGACY_CONSTRAINTS_QUERIES {
        snap.insert(k.to_string(), serde_json::Value::Array(db.execute(q, &[]).unwrap().rows));
    }
    std::fs::write(dir.join(format!("{name}.expected.json")), serde_json::to_string_pretty(&snap).unwrap()).unwrap();
}

pub const LEGACY_CONSTRAINTS_QUERIES: &[(&str, &str)] = &[
    ("tenants", "SELECT * FROM tenants ORDER BY id"),
    ("users", "SELECT * FROM users ORDER BY id"),
    ("tenant_members", "SELECT * FROM tenant_members ORDER BY id"),
    ("auth_sessions", "SELECT * FROM auth_sessions ORDER BY id"),
    ("tenant_roles", "SELECT * FROM tenant_roles ORDER BY id, tenant_id"),
    ("mfa", "SELECT * FROM mfa ORDER BY id"),
];

/// DDL lấy từ waiki core/auth-basic/lifecycle.ts (+ CHECK từ core/customer-auth/lifecycle.ts).
pub const LEGACY_CONSTRAINTS_DDL: &[&str] = &[
    "CREATE TABLE IF NOT EXISTS tenants (id SERIAL PRIMARY KEY, code VARCHAR(50) NOT NULL UNIQUE, name VARCHAR(255) NOT NULL, status VARCHAR(20) NOT NULL DEFAULT 'active', settings JSONB DEFAULT '{}'::jsonb, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)",
    "CREATE TABLE IF NOT EXISTS users (id SERIAL PRIMARY KEY, username VARCHAR(100) UNIQUE, email VARCHAR(255) NOT NULL UNIQUE, full_name VARCHAR(150) NOT NULL, status VARCHAR(20) NOT NULL DEFAULT 'active', deleted_at TIMESTAMP)",
    "CREATE TABLE IF NOT EXISTS tenant_members (id SERIAL PRIMARY KEY, tenant_id INT NOT NULL REFERENCES tenants(id) ON DELETE CASCADE, user_id INT NOT NULL REFERENCES users(id) ON DELETE CASCADE, role VARCHAR(50) NOT NULL DEFAULT 'member', UNIQUE (tenant_id, user_id))",
    "CREATE TABLE IF NOT EXISTS auth_sessions (id VARCHAR(128) PRIMARY KEY, user_id INT NOT NULL REFERENCES users(id) ON DELETE CASCADE, tenant_id INT NOT NULL, expires_at TIMESTAMP NOT NULL)",
    "CREATE INDEX IF NOT EXISTS idx_auth_sessions_user ON auth_sessions(user_id)",
    "CREATE TABLE IF NOT EXISTS tenant_roles (id VARCHAR(50) NOT NULL, tenant_id INT NOT NULL, name VARCHAR(100) NOT NULL, PRIMARY KEY (id, tenant_id))",
    "CREATE TABLE IF NOT EXISTS mfa (id SERIAL PRIMARY KEY, user_id INT REFERENCES users(id) ON DELETE SET NULL, kind VARCHAR(10) NOT NULL CHECK (kind IN ('totp','sms')))",
    "CREATE UNIQUE INDEX IF NOT EXISTS uq_users_email_live ON users(email) WHERE deleted_at IS NULL",
];

pub const LEGACY_CONSTRAINTS_SCRIPT: &[&str] = &[
    LEGACY_CONSTRAINTS_DDL[0], LEGACY_CONSTRAINTS_DDL[1], LEGACY_CONSTRAINTS_DDL[2], LEGACY_CONSTRAINTS_DDL[3],
    LEGACY_CONSTRAINTS_DDL[4], LEGACY_CONSTRAINTS_DDL[5], LEGACY_CONSTRAINTS_DDL[6], LEGACY_CONSTRAINTS_DDL[7],
    "INSERT INTO tenants (code, name) VALUES ('default', 'Mac dinh')",
    "INSERT INTO tenants (code, name) VALUES ('default', 'Trung code')",
    "INSERT INTO users (username, email, full_name) VALUES ('admin', 'a@x.vn', 'Admin')",
    "INSERT INTO users (username, email, full_name) VALUES ('admin2', 'a@x.vn', 'Trung email')",
    "INSERT INTO users (username, email, full_name) VALUES (NULL, 'c@x.vn', NULL)",
    "INSERT INTO users (username, email, full_name) VALUES ('del', 'd@x.vn', 'Se xoa')",
    "INSERT INTO tenant_members (tenant_id, user_id, role) VALUES (1, 1, 'owner')",
    "INSERT INTO tenant_members (tenant_id, user_id, role) VALUES (1, 1, 'trung')",
    "INSERT INTO tenant_members (tenant_id, user_id, role) VALUES (1, 999, 'mo coi')",
    "INSERT INTO tenant_members (tenant_id, user_id, role) VALUES (1, 4, 'cha bi xoa')",
    "INSERT INTO auth_sessions (id, user_id, tenant_id, expires_at) VALUES ('s-1', 1, 1, '2026-12-01 00:00:00')",
    "INSERT INTO auth_sessions (id, user_id, tenant_id, expires_at) VALUES ('s-4', 4, 1, '2026-12-01 00:00:00')",
    "INSERT INTO tenant_roles (id, tenant_id, name) VALUES ('admin', 1, 'Quan tri')",
    "INSERT INTO tenant_roles (id, tenant_id, name) VALUES ('admin', 2, 'Quan tri 2')",
    "INSERT INTO tenant_roles (id, tenant_id, name) VALUES ('admin', 1, 'Trung PK ghep')",
    "INSERT INTO mfa (user_id, kind) VALUES (1, 'totp')",
    "INSERT INTO mfa (user_id, kind) VALUES (4, 'email')",
    "DELETE FROM users WHERE id = 4",
    "UPDATE users SET full_name = 'Quan tri vien' WHERE id = 1",
    "UPDATE tenant_roles SET name = 'Doi ten' WHERE name = 'Quan tri 2'",
];

/// Fixture do bản 7db3c5a ghi (đã có catalog ràng buộc, upsert, ALTER/RENAME, DO): bản sau phải đọc y nguyên.
///   cargo test --test gen_legacy_fixture generate_legacy_v3_fixture -- --ignored
#[test]
#[ignore]
fn generate_legacy_v3_fixture() {
    let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures");
    let name = "legacy_v3";
    let path = dir.join(format!("{name}.db")).to_str().unwrap().to_string();
    for suffix in ["", ".rwal", ".v2.rwal", ".wal"] {
        let _ = std::fs::remove_file(format!("{}{}", path, suffix));
    }
    let mut db = PGlite::open(&path).unwrap();
    for sql in LEGACY_V3_SCRIPT {
        db.execute(sql, &[]).unwrap_or_else(|e| panic!("{sql}: {e}"));
    }
    db.flush();
    drop(db);
    let mut db = PGlite::open(&path).unwrap();
    let mut snap = serde_json::Map::new();
    for (k, q) in LEGACY_V3_QUERIES {
        snap.insert(k.to_string(), serde_json::Value::Array(db.execute(q, &[]).unwrap().rows));
    }
    std::fs::write(dir.join(format!("{name}.expected.json")), serde_json::to_string_pretty(&snap).unwrap()).unwrap();
}

pub const LEGACY_V3_QUERIES: &[(&str, &str)] = &[
    ("tenants", "SELECT * FROM tenants ORDER BY id"),
    ("members", "SELECT * FROM members ORDER BY id"),
    ("roles", "SELECT * FROM roles ORDER BY code, tenant_id"),
    ("items", "SELECT * FROM items ORDER BY id"),
    ("logs", "SELECT * FROM logs ORDER BY id"),
];

pub const LEGACY_V3_SCRIPT: &[&str] = &[
    "CREATE TABLE tenants (id SERIAL PRIMARY KEY, code VARCHAR(50) NOT NULL UNIQUE, name TEXT NOT NULL, settings JSONB DEFAULT '{}'::jsonb)",
    "CREATE TABLE members (id SERIAL PRIMARY KEY, tenant_id INT NOT NULL REFERENCES tenants(id) ON DELETE CASCADE, email TEXT NOT NULL, role TEXT DEFAULT 'member' CHECK (role IN ('member','owner')), UNIQUE (tenant_id, email))",
    "CREATE TABLE roles (code VARCHAR(20) NOT NULL, tenant_id INT NOT NULL, label TEXT, PRIMARY KEY (code, tenant_id))",
    "CREATE TABLE stock (id SERIAL PRIMARY KEY, sku TEXT, qty TEXT, deleted_at TIMESTAMP)",
    "CREATE UNIQUE INDEX uq_stock_sku ON stock(sku) WHERE deleted_at IS NULL",
    "CREATE TABLE logs (id SERIAL PRIMARY KEY, msg TEXT)",
    "INSERT INTO tenants (code, name) VALUES ('a', 'Công ty A'), ('b', 'Công ty B'), ('c', 'Xoá')",
    "INSERT INTO members (tenant_id, email, role) VALUES (1, 'x@a.vn', 'owner'), (1, 'y@a.vn', 'member'), (3, 'z@c.vn', 'member')",
    "INSERT INTO members (tenant_id, email, role) VALUES (1, 'x@a.vn', 'member') ON CONFLICT (tenant_id, email) DO UPDATE SET role = EXCLUDED.role",
    "INSERT INTO tenants (code, name) VALUES ('a', 'trùng') ON CONFLICT (code) DO NOTHING",
    "INSERT INTO roles VALUES ('admin', 1, 'Quản trị'), ('admin', 2, 'Admin B')",
    "DELETE FROM tenants WHERE code = 'c'",
    "INSERT INTO stock (sku, qty) VALUES ('s1', '5'), ('s2', '7')",
    "UPDATE stock SET deleted_at = '2026-01-01 00:00:00' WHERE sku = 's1'",
    "INSERT INTO stock (sku, qty) VALUES ('s1', '9')",
    "ALTER TABLE stock ALTER COLUMN qty TYPE INTEGER USING qty::integer",
    "ALTER TABLE stock ADD COLUMN price NUMERIC DEFAULT 0 NOT NULL, ALTER COLUMN sku SET NOT NULL",
    "ALTER TABLE stock RENAME COLUMN sku TO code",
    "ALTER TABLE stock RENAME TO items",
    "BEGIN",
    "INSERT INTO logs (msg) VALUES ('rolled back')",
    "ROLLBACK",
    "BEGIN",
    "INSERT INTO logs (msg) VALUES ('kept')",
    "SAVEPOINT s1",
    "INSERT INTO logs (msg) VALUES ('undone')",
    "ROLLBACK TO SAVEPOINT s1",
    "COMMIT",
    "DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM logs WHERE msg = 'do') THEN INSERT INTO logs (msg) VALUES ('do'); END IF; END $$",
    "TRUNCATE roles",
    "INSERT INTO roles VALUES ('owner', 2, 'Sau truncate')",
];
