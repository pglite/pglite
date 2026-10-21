//! Plan cache: chạy cùng một câu SQL nhiều lần (lần sau đi qua plan đã cache) phải luôn
//! cho cùng kết quả với lần đầu, và kết quả đó phải đúng.
use pglite_rs::{PGlite, Value};
use serde_json::json;

fn db() -> PGlite {
    let mut db = PGlite::in_memory().unwrap();
    for sql in [
        "CREATE TABLE d (id SERIAL PRIMARY KEY, c TEXT, n INT, owner_id INT)",
        "INSERT INTO d (c, n, owner_id) VALUES ('x', 1, 10), ('x', 1, 10), ('y', 2, 20), ('z', 3, 30)",
        "CREATE TABLE u (id UUID PRIMARY KEY, name TEXT)",
        "INSERT INTO u (id, name) VALUES ('a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11', 'A'), ('b0eebc99-9c0b-4ef8-bb6d-6bb9bd380a12', 'B')",
    ] {
        db.exec(sql, &[]).unwrap();
    }
    db
}

fn same_twice(db: &mut PGlite, sql: &str, params: &[Value]) -> Vec<serde_json::Value> {
    let a = db.query(sql, params).unwrap_or_else(|e| panic!("{sql}: {e}")).rows;
    let b = db.query(sql, params).unwrap().rows;
    let c = db.query(sql, params).unwrap().rows;
    assert_eq!(a, b, "lần 2 khác lần 1: {sql}");
    assert_eq!(a, c, "lần 3 khác lần 1: {sql}");
    a
}

#[test]
fn distinct() {
    let mut db = db();
    assert_eq!(same_twice(&mut db, "SELECT DISTINCT c FROM d WHERE n > 0", &[]), vec![json!({"c":"x"}), json!({"c":"y"}), json!({"c":"z"})]);
}

#[test]
fn order_by_alias_and_expression_desc() {
    let mut db = db();
    let r = same_twice(&mut db, "SELECT id, n * 10 AS score FROM d WHERE n > 0 ORDER BY score DESC", &[]);
    let scores: Vec<i64> = r.iter().map(|x| x["score"].as_i64().unwrap()).collect();
    assert_eq!(scores, vec![30, 20, 10, 10]);
    let r = same_twice(&mut db, "SELECT id FROM d WHERE n > 0 ORDER BY n DESC, id ASC", &[]);
    assert_eq!(r, vec![json!({"id":4}), json!({"id":3}), json!({"id":1}), json!({"id":2})]);
    let r = same_twice(&mut db, "SELECT id, c FROM d WHERE n > 0 ORDER BY c DESC, id DESC", &[]);
    assert_eq!(r.iter().map(|x| x["id"].as_i64().unwrap()).collect::<Vec<_>>(), vec![4, 3, 2, 1]);
}

#[test]
fn in_list_with_text_and_uuid_keys() {
    let mut db = db();
    let r = same_twice(&mut db, "SELECT name FROM u WHERE id IN ('a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11', 'b0eebc99-9c0b-4ef8-bb6d-6bb9bd380a12') ORDER BY name", &[]);
    assert_eq!(r, vec![json!({"name":"A"}), json!({"name":"B"})]);
    let r = same_twice(&mut db, "SELECT id FROM d WHERE c IN ('y', 'z') ORDER BY id", &[]);
    assert_eq!(r, vec![json!({"id":3}), json!({"id":4})]);
}

#[test]
fn uuid_pk_equality_with_params() {
    let mut db = db();
    let q = "SELECT name FROM u WHERE id = $1";
    assert_eq!(same_twice(&mut db, q, &[Value::text("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11")]), vec![json!({"name":"A"})]);
    assert_eq!(same_twice(&mut db, q, &[Value::text("b0eebc99-9c0b-4ef8-bb6d-6bb9bd380a12")]), vec![json!({"name":"B"})]);
}

#[test]
fn limit_and_offset_params() {
    let mut db = db();
    let q = "SELECT id FROM d WHERE n > 0 ORDER BY id LIMIT $1 OFFSET $2";
    assert_eq!(same_twice(&mut db, q, &[Value::Int(2), Value::Int(1)]), vec![json!({"id":2}), json!({"id":3})]);
    assert_eq!(same_twice(&mut db, q, &[Value::Int(1), Value::Int(3)]), vec![json!({"id":4})]);
}

#[test]
fn lowercase_and_or() {
    let mut db = db();
    assert!(same_twice(&mut db, "SELECT * FROM d WHERE c = $1 and n = $2", &[Value::text("x"), Value::Int(2)]).is_empty());
    assert_eq!(same_twice(&mut db, "SELECT id FROM d WHERE c = $1 or n = $2", &[Value::text("z"), Value::Int(2)]).len(), 2);
}

#[test]
fn params_change_between_runs() {
    let mut db = db();
    let q = "SELECT id FROM d WHERE owner_id = $1 ORDER BY id";
    assert_eq!(same_twice(&mut db, q, &[Value::Int(10)]).len(), 2);
    assert_eq!(same_twice(&mut db, q, &[Value::Int(30)]), vec![json!({"id":4})]);
    let q = "SELECT id FROM d WHERE c = $1";
    assert_eq!(same_twice(&mut db, q, &[Value::text("y")]), vec![json!({"id":3})]);
    assert_eq!(same_twice(&mut db, q, &[Value::text("z")]), vec![json!({"id":4})]);
}

#[test]
fn data_changes_between_runs() {
    let mut db = db();
    let q = "SELECT id FROM d WHERE n >= 2 ORDER BY id";
    assert_eq!(same_twice(&mut db, q, &[]).len(), 2);
    db.exec("INSERT INTO d (c, n, owner_id) VALUES ('w', 5, 50)", &[]).unwrap();
    db.exec("UPDATE d SET n = 0 WHERE id = 3", &[]).unwrap();
    assert_eq!(same_twice(&mut db, q, &[]), vec![json!({"id":4}), json!({"id":5})]);
}

#[test]
fn subqueries_follow_params_and_data() {
    let mut db = db();
    let q = "SELECT id FROM d WHERE owner_id IN (SELECT owner_id FROM d WHERE c = $1) ORDER BY id";
    assert_eq!(same_twice(&mut db, q, &[Value::text("x")]).len(), 2);
    assert_eq!(same_twice(&mut db, q, &[Value::text("z")]), vec![json!({"id":4})]);
}

#[test]
fn vietnamese_text_survives_multi_statement_and_named_params() {
    let mut db = PGlite::in_memory().unwrap();
    db.exec("CREATE TABLE vn (id SERIAL PRIMARY KEY, s TEXT, note TEXT)", &[]).unwrap();
    db.exec("INSERT INTO vn (s) VALUES ('Nguyễn Văn An'); INSERT INTO vn (s) VALUES ('Trường; học (1)')", &[]).unwrap();
    db.exec("INSERT INTO vn (s, note) VALUES (:ten, 'Đà Nẵng ✓')", &[Value::text("Huế")]).unwrap();
    let r = db.query("SELECT s, note FROM vn WHERE s <> 'Ánh' ORDER BY id", &[]).unwrap().rows;
    assert_eq!(r.iter().map(|x| x["s"].as_str().unwrap()).collect::<Vec<_>>(), vec!["Nguyễn Văn An", "Trường; học (1)", "Huế"]);
    assert_eq!(r[2]["note"], "Đà Nẵng ✓");
}

/// `$n` đứng trực tiếp trong danh sách SELECT có FROM: từ lần chạy thứ hai (qua plan cache) từng ra NULL
/// (DB_RS_BUGS_FOUND lỗi 6), làm `INSERT ... SELECT $1, id FROM ...` vi phạm NOT NULL.
#[test]
fn param_in_select_list_is_rebound_on_cached_plan() {
    let mut db = pglite_rs::PGlite::in_memory().unwrap();
    db.exec("CREATE TABLE cat (id SERIAL PRIMARY KEY, code TEXT)", &[]).unwrap();
    db.exec("INSERT INTO cat (code) VALUES ('a'), ('b')", &[]).unwrap();
    db.exec("CREATE TABLE pi4 (id SERIAL PRIMARY KEY, program_id INT NOT NULL, item_id INT NOT NULL)", &[]).unwrap();
    for (p, code, id) in [(8, "a", 1), (9, "a", 1), (10, "b", 2)] {
        let r = db.query("SELECT $1 AS p, id FROM cat WHERE code = $2", &[pglite_rs::Value::Int(p), pglite_rs::Value::text(code)]).unwrap().rows;
        assert_eq!(r, vec![serde_json::json!({"p": p, "id": id})], "lần chạy với p = {p}");
    }
    for p in 1..=3 {
        let r = db.query("INSERT INTO pi4 (program_id, item_id) SELECT $1, id FROM cat WHERE code = $2 RETURNING program_id", &[pglite_rs::Value::Int(p), pglite_rs::Value::text("a")]).unwrap().rows;
        assert_eq!(r, vec![serde_json::json!({"program_id": p})]);
    }
    for p in [5, 6] {
        let r = db.query("SELECT $1 AS p, id FROM cat ORDER BY id", &[pglite_rs::Value::Int(p)]).unwrap().rows;
        assert_eq!(r, vec![serde_json::json!({"p": p, "id": 1}), serde_json::json!({"p": p, "id": 2})]);
    }
}
