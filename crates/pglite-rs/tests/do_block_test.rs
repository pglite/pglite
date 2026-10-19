//! DO $$ ... $$ (mẫu waiki core/auth-basic), SET/SHOW/RESET, CREATE EXTENSION.
use pglite_rs::PGlite;
use serde_json::json;

fn rows(db: &mut PGlite, sql: &str) -> Vec<serde_json::Value> {
    db.query(sql, &[]).unwrap_or_else(|e| panic!("{sql}: {e}")).rows
}

const AUTH_BASIC_DO: &str = "
      DO $$
      BEGIN
        IF NOT EXISTS (
          SELECT 1 FROM information_schema.columns
          WHERE table_name = 'users' AND column_name = 'username'
        ) THEN
          ALTER TABLE users ADD COLUMN username VARCHAR(100) UNIQUE;
        END IF;

        IF NOT EXISTS (
          SELECT 1 FROM information_schema.columns
          WHERE table_name = 'users' AND column_name = 'password_hash'
        ) THEN
          ALTER TABLE users ADD COLUMN password_hash VARCHAR(255);
        END IF;
      END $$;
    ";

#[test]
fn waiki_do_block_is_idempotent() {
    let mut db = PGlite::in_memory().unwrap();
    db.exec("CREATE TABLE users (id SERIAL PRIMARY KEY, email TEXT)", &[]).unwrap();
    db.exec(AUTH_BASIC_DO, &[]).unwrap();
    db.exec(AUTH_BASIC_DO, &[]).unwrap();
    db.exec("INSERT INTO users (email, username, password_hash) VALUES ('a', 'u', 'h')", &[]).unwrap();
    assert!(db.exec("INSERT INTO users (email, username) VALUES ('b', 'u')", &[]).is_err(), "UNIQUE từ ADD COLUMN trong DO");
    // Chạy cùng các lệnh khác trong một chuỗi (thân DO chứa ';')
    db.exec("CREATE TABLE x (id INT); DO $tag$ BEGIN INSERT INTO x VALUES (1); INSERT INTO x VALUES (2); END $tag$; INSERT INTO x VALUES (3)", &[]).unwrap();
    assert_eq!(rows(&mut db, "SELECT count(*) AS n FROM x"), vec![json!({"n": 3})]);
}

#[test]
fn do_block_atomicity_exceptions_and_control_flow() {
    let mut db = PGlite::in_memory().unwrap();
    db.exec("CREATE TABLE t (id INT PRIMARY KEY, v TEXT)", &[]).unwrap();
    // Lỗi giữa khối: mọi thay đổi của khối bị huỷ
    let e = db.exec("DO $$ BEGIN INSERT INTO t VALUES (1, 'a'); INSERT INTO t VALUES (1, 'dup'); END $$", &[]).unwrap_err();
    assert!(e.contains("duplicate key"), "{e}");
    assert!(rows(&mut db, "SELECT * FROM t").is_empty());
    // EXCEPTION: bắt lỗi, huỷ phần dở của khối con, chạy tiếp
    db.exec("DO $$
        BEGIN
          INSERT INTO t VALUES (1, 'kept');
          BEGIN
            INSERT INTO t VALUES (2, 'undone');
            INSERT INTO t VALUES (1, 'dup');
          EXCEPTION WHEN unique_violation THEN
            INSERT INTO t VALUES (3, 'handler');
          END;
          IF (SELECT count(*) FROM t) = 2 THEN
            INSERT INTO t VALUES (4, 'if');
          ELSIF true THEN
            INSERT INTO t VALUES (5, 'elsif');
          ELSE
            NULL;
          END IF;
          RAISE NOTICE 'xong %', 1;
          PERFORM 1;
        END $$", &[]).unwrap();
    assert_eq!(rows(&mut db, "SELECT id, v FROM t ORDER BY id"), vec![
        json!({"id": 1, "v": "kept"}),
        json!({"id": 3, "v": "handler"}),
        json!({"id": 4, "v": "if"}),
    ]);
    // Bắt duplicate_object khi tạo lại bảng; RAISE EXCEPTION làm lỗi
    db.exec("DO $$ BEGIN CREATE TABLE t (id INT); EXCEPTION WHEN duplicate_table THEN NULL; END $$", &[]).unwrap();
    let e = db.exec("DO $$ BEGIN RAISE EXCEPTION 'stop %', 1; END $$", &[]).unwrap_err();
    assert!(e.contains("stop"), "{e}");
    // Trong giao dịch: khối lỗi không làm mất phần trước của giao dịch
    db.exec("BEGIN", &[]).unwrap();
    db.exec("INSERT INTO t VALUES (10, 'tx')", &[]).unwrap();
    db.exec("DO $$ BEGIN INSERT INTO t VALUES (11, 'x'); EXCEPTION WHEN others THEN NULL; END $$", &[]).unwrap();
    db.exec("COMMIT", &[]).unwrap();
    assert_eq!(rows(&mut db, "SELECT count(*) AS n FROM t WHERE id >= 10"), vec![json!({"n": 2})]);
}

#[test]
fn session_statements() {
    let mut db = PGlite::in_memory().unwrap();
    for sql in ["CREATE EXTENSION IF NOT EXISTS pgcrypto", "CREATE EXTENSION IF NOT EXISTS \"uuid-ossp\"", "SET search_path TO public", "SET TIME ZONE 'UTC'", "SET client_min_messages = warning", "SET SESSION statement_timeout = 0", "RESET client_min_messages", "SET TRANSACTION ISOLATION LEVEL READ COMMITTED"] {
        db.exec(sql, &[]).unwrap_or_else(|e| panic!("{sql}: {e}"));
    }
    assert_eq!(rows(&mut db, "SHOW search_path"), vec![json!({"search_path": "public"})]);
    assert_eq!(rows(&mut db, "SHOW timezone"), vec![json!({"timezone": "UTC"})]);
    assert_eq!(rows(&mut db, "SHOW server_version"), vec![json!({"server_version": "16.0"})]);
    assert!(db.exec("SHOW nope", &[]).is_err());
    assert!(db.exec("CREATE EXTENSION postgis", &[]).is_err());
}
