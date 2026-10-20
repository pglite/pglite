//! Cú pháp gặp khi chạy dự án zalo-1054 (schema.sql do công cụ dump sinh, migration, query Kysely) mà
//! golden test không phủ được: hành vi riêng của pglite-rs (PostgreSQL thật báo lỗi ở chỗ khác) và dữ liệu
//! trên đĩa sau khi mở lại DB.
use pglite_rs::{PGlite, Value};
use serde_json::json;

fn rows(db: &mut PGlite, sql: &str) -> Vec<serde_json::Value> {
    db.query(sql, &[]).unwrap_or_else(|e| panic!("{sql}: {e}")).rows
}

fn temp_path(name: &str) -> String {
    let dir = std::env::temp_dir().join(format!("pglite_zalo_{}_{}", name, std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir.join("db.db").to_string_lossy().to_string()
}

#[test]
fn nextval_default_without_sequence_acts_like_serial() {
    // dạng schema.sql: `"id" INTEGER DEFAULT nextval('x_id_seq'::regclass) PRIMARY KEY`, không có CREATE SEQUENCE
    let mut db = PGlite::in_memory().unwrap();
    db.exec(r#"CREATE TABLE IF NOT EXISTS "news" ("id" INTEGER DEFAULT nextval('news_id_seq'::regclass) PRIMARY KEY, "title" TEXT, "code" INTEGER DEFAULT nextval('news_code_seq'::regclass))"#, &[]).unwrap();
    assert_eq!(rows(&mut db, "INSERT INTO news (title) VALUES ('a'), ('b') RETURNING id, code"), vec![json!({"id": 1, "code": 1}), json!({"id": 2, "code": 2})]);
    db.exec("INSERT INTO news (id, title) VALUES (10, 'explicit')", &[]).unwrap();
    assert_eq!(rows(&mut db, "INSERT INTO news (title) VALUES ('c') RETURNING id"), vec![json!({"id": 11})]);
    assert_eq!(rows(&mut db, "INSERT INTO news (id, title) VALUES (DEFAULT, 'd') RETURNING id"), vec![json!({"id": 12})]);
    // setval không bao giờ hạ bộ đếm dưới max + 1 (không sinh khoá trùng)
    db.exec("SELECT setval('news_id_seq', 3, true)", &[]).unwrap();
    assert_eq!(rows(&mut db, "INSERT INTO news (title) VALUES ('e') RETURNING id"), vec![json!({"id": 13})]);
    assert_eq!(rows(&mut db, "SELECT setval('news_id_seq', 100) AS v, pg_get_serial_sequence('news', 'id') AS s"), vec![json!({"v": 100, "s": "public.news_id_seq"})]);
    assert_eq!(rows(&mut db, "SELECT nextval('news_id_seq') AS n, currval('news_id_seq') AS c"), vec![json!({"n": 101, "c": 101})]);
    assert!(db.query("SELECT nextval('nope_seq')", &[]).unwrap_err().contains("does not exist"));
}

#[test]
fn nextval_default_survives_reopen() {
    let path = temp_path("nextval");
    {
        let mut db = PGlite::open(&path).unwrap();
        db.exec(r#"CREATE TABLE "t" ("id" INTEGER DEFAULT nextval('t_id_seq'::regclass) PRIMARY KEY, "v" TEXT)"#, &[]).unwrap();
        db.exec("INSERT INTO t (v) VALUES ('a'), ('b')", &[]).unwrap();
        db.close();
    }
    let mut db = PGlite::open(&path).unwrap();
    assert_eq!(rows(&mut db, "INSERT INTO t (v) VALUES ('c') RETURNING id"), vec![json!({"id": 3})]);
    assert_eq!(rows(&mut db, "SELECT count(*) AS n FROM t"), vec![json!({"n": 3})]);
}

#[test]
fn temp_tables_never_reach_disk() {
    let path = temp_path("temp");
    {
        let mut db = PGlite::open(&path).unwrap();
        db.exec("CREATE TABLE att (id SERIAL PRIMARY KEY, v INT)", &[]).unwrap();
        db.exec("INSERT INTO att (v) VALUES (1), (2)", &[]).unwrap();
        db.exec("BEGIN", &[]).unwrap();
        db.exec("CREATE TEMP TABLE tmp_att ON COMMIT DROP AS SELECT id, v * 10 AS u FROM att", &[]).unwrap();
        db.exec("INSERT INTO att (v) SELECT u FROM tmp_att", &[]).unwrap();
        db.exec("COMMIT", &[]).unwrap();
        assert!(db.query("SELECT * FROM tmp_att", &[]).is_err(), "ON COMMIT DROP");
        db.exec("CREATE TEMP TABLE keep_me (id SERIAL PRIMARY KEY, v TEXT)", &[]).unwrap();
        db.exec("INSERT INTO keep_me (v) VALUES ('x')", &[]).unwrap();
        db.exec("CREATE INDEX keep_me_v ON keep_me (v)", &[]).unwrap();
        assert_eq!(rows(&mut db, "SELECT v FROM keep_me"), vec![json!({"v": "x"})]);
        db.close();
    }
    let wal: Vec<u8> = std::fs::read_dir(std::path::Path::new(&path).parent().unwrap())
        .unwrap()
        .flat_map(|e| std::fs::read(e.unwrap().path()).unwrap_or_default())
        .collect();
    let text = String::from_utf8_lossy(&wal);
    assert!(!text.contains("tmp_att") && !text.contains("keep_me"), "bảng tạm không được ghi xuống đĩa");
    let mut db = PGlite::open(&path).unwrap();
    assert!(db.query("SELECT * FROM keep_me", &[]).is_err());
    assert_eq!(rows(&mut db, "SELECT v FROM att ORDER BY id"), vec![json!({"v": 1}), json!({"v": 2}), json!({"v": 10}), json!({"v": 20})]);
    // bảng thật trùng tên bảng tạm cũ được ghi bình thường
    db.exec("CREATE TABLE keep_me (id INT)", &[]).unwrap();
    db.exec("INSERT INTO keep_me VALUES (1)", &[]).unwrap();
    db.close();
    let mut db = PGlite::open(&path).unwrap();
    assert_eq!(rows(&mut db, "SELECT id FROM keep_me"), vec![json!({"id": 1})]);
}

#[test]
fn do_block_variables_keep_old_behaviour_for_unassigned_names() {
    let mut db = PGlite::in_memory().unwrap();
    db.exec("CREATE TABLE att (id SERIAL PRIMARY KEY, user_id INT)", &[]).unwrap();
    db.exec("INSERT INTO att (user_id) VALUES (1), (2)", &[]).unwrap();
    // biến khai báo nhưng chưa gán, trùng tên cột: như bản cũ (khai báo bị bỏ qua) — `user_id` là cột
    db.exec("DO $$ DECLARE user_id int; BEGIN UPDATE att SET user_id = 7 WHERE id = 1; END $$", &[]).unwrap();
    assert_eq!(rows(&mut db, "SELECT user_id FROM att ORDER BY id"), vec![json!({"user_id": 7}), json!({"user_id": 2})]);
    // SELECT ... INTO bảng trong DO vẫn tạo bảng
    db.exec("DO $$ BEGIN SELECT id INTO att_copy FROM att; END $$", &[]).unwrap();
    assert_eq!(rows(&mut db, "SELECT count(*) AS n FROM att_copy"), vec![json!({"n": 2})]);
    // RAISE có tham số
    let e = db.exec("DO $$ DECLARE n int; BEGIN SELECT count(*) INTO n FROM att; RAISE EXCEPTION 'n=%', n; END $$", &[]).unwrap_err();
    assert!(e.contains("n=2"), "{e}");
}

#[test]
fn correlated_subqueries_and_regex_in_app_queries() {
    let mut db = PGlite::in_memory().unwrap();
    db.exec("CREATE TABLE users (id SERIAL PRIMARY KEY, phone_number TEXT)", &[]).unwrap();
    db.exec("INSERT INTO users (phone_number) VALUES ('+84 0912-345-678'), (NULL)", &[]).unwrap();
    let r = db
        .query(
            r#"SELECT "users"."id" FROM "users" WHERE regexp_replace(regexp_replace(regexp_replace(coalesce(users.phone_number, ''), '[^0-9]', '', 'g'), '^84', ''), '^0+', '') like $1"#,
            &[Value::text("%912345%")],
        )
        .unwrap()
        .rows;
    assert_eq!(r, vec![json!({"id": 1})]);
}
