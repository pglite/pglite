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
