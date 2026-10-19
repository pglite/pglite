//! Mỗi lỗi "chạy nhưng sai kết quả / ghi sai dữ liệu" đã sửa có một test tái hiện, kết quả mong đợi
//! đã đối chiếu với PostgreSQL thật (@electric-sql/pglite).
use pglite_rs::{PGlite, Value};
use serde_json::json;

fn db() -> PGlite {
    let mut db = PGlite::in_memory().unwrap();
    for sql in [
        "CREATE TABLE t (id INT PRIMARY KEY, name TEXT DEFAULT 'anon', n INT, created TIMESTAMP, flag BOOLEAN, tags TEXT[])",
        "CREATE TABLE u (id INT PRIMARY KEY, t_id INT, v TEXT)",
        "INSERT INTO t VALUES (1, 'alice', 5, '2026-01-31 03:04:05', true, '{x,y}'), (2, 'bob', 3, '2026-02-01 00:00:00', false, '{y}'), (3, 'carol', NULL, NULL, NULL, NULL)",
        "INSERT INTO u VALUES (1, 1, 'v1'), (2, 1, 'v2'), (3, 2, 'v3'), (4, NULL, 'v4')",
    ] {
        db.exec(sql, &[]).unwrap_or_else(|e| panic!("{sql}: {e}"));
    }
    db
}

fn q(db: &mut PGlite, sql: &str) -> Vec<serde_json::Value> {
    db.query(sql, &[]).unwrap_or_else(|e| panic!("{sql}: {e}")).rows
}

#[test]
fn expressions() {
    let mut db = db();
    assert_eq!(q(&mut db, "SELECT CASE n WHEN 5 THEN 'five' WHEN 3 THEN 'three' ELSE 'other' END AS w FROM t ORDER BY id"), vec![json!({"w": "five"}), json!({"w": "three"}), json!({"w": "other"})]);
    assert_eq!(q(&mut db, "SELECT name FROM t WHERE id = ANY(ARRAY[1,3]) ORDER BY id"), vec![json!({"name": "alice"}), json!({"name": "carol"})]);
    assert_eq!(q(&mut db, "SELECT name FROM t WHERE id <> ALL(ARRAY[1,3])"), vec![json!({"name": "bob"})]);
    assert_eq!(q(&mut db, "SELECT name FROM t WHERE 'x' = ANY(tags)"), vec![json!({"name": "alice"})]);
    assert_eq!(db.query("SELECT name FROM t WHERE id = ANY($1) ORDER BY id", &[Value::text("{2,3}")]).unwrap().rows, vec![json!({"name": "bob"}), json!({"name": "carol"})]);
    assert_eq!(q(&mut db, "SELECT name FROM t WHERE id IN (SELECT t_id FROM u) ORDER BY id"), vec![json!({"name": "alice"}), json!({"name": "bob"})]);
    assert_eq!(q(&mut db, "SELECT length(name) AS l, upper(name) FROM t WHERE id = 1"), vec![json!({"l": 5, "upper": "ALICE"})]);
    assert_eq!(q(&mut db, "SELECT count(*) FILTER (WHERE n > 3) AS c, count(*) FROM t"), vec![json!({"c": 1, "count": 3})]);
    assert_eq!(q(&mut db, "SELECT 1 AS a WHERE false"), Vec::<serde_json::Value>::new());
}

#[test]
fn dates_and_intervals() {
    let mut db = db();
    assert_eq!(
        q(&mut db, "SELECT created + INTERVAL '2 hours' AS a, created + INTERVAL '1 month' AS b FROM t WHERE id = 1"),
        vec![json!({"a": "2026-01-31 05:04:05", "b": "2026-02-28 03:04:05"})]
    );
    assert_eq!(q(&mut db, "SELECT DATE '2026-03-01' - DATE '2026-02-01' AS d, '1 day 2 hours'::interval AS i"), vec![json!({"d": 28, "i": "1 day 02:00:00"})]);
    assert_eq!(q(&mut db, "SELECT name FROM t WHERE created > TIMESTAMP '2026-02-01 00:00:00' - INTERVAL '1 hour' ORDER BY id"), vec![json!({"name": "bob"})]);
}

#[test]
fn ordering_sets_and_joins() {
    let mut db = db();
    assert_eq!(q(&mut db, "SELECT name FROM t ORDER BY n"), vec![json!({"name": "bob"}), json!({"name": "alice"}), json!({"name": "carol"})]);
    assert_eq!(q(&mut db, "SELECT name FROM t ORDER BY n DESC NULLS LAST"), vec![json!({"name": "alice"}), json!({"name": "bob"}), json!({"name": "carol"})]);
    assert_eq!(q(&mut db, "SELECT name FROM t UNION SELECT v FROM u ORDER BY 1 LIMIT 4"), vec![json!({"name": "alice"}), json!({"name": "bob"}), json!({"name": "carol"}), json!({"name": "v1"})]);
    assert_eq!(q(&mut db, "SELECT name FROM t EXCEPT SELECT 'bob' ORDER BY 1"), vec![json!({"name": "alice"}), json!({"name": "carol"})]);
    assert_eq!(q(&mut db, "SELECT t.name, u.v FROM t RIGHT JOIN u ON u.t_id = t.id ORDER BY u.v").len(), 4);
    assert_eq!(q(&mut db, "SELECT t.name, u.v FROM t FULL JOIN u ON u.t_id = t.id").len(), 5);
    assert_eq!(q(&mut db, "SELECT t.name FROM t JOIN u ON u.t_id = t.id JOIN t t2 ON t2.id = u.t_id").len(), 3);
}

#[test]
fn writes_that_used_to_corrupt_data() {
    let mut db = db();
    // SET col = DEFAULT ghi chữ "DEFAULT"; UPDATE ... FROM ghi "a.name FROM a"
    db.exec("UPDATE t SET name = DEFAULT WHERE id = 3", &[]).unwrap();
    db.exec("UPDATE u SET v = t.name FROM t WHERE u.t_id = t.id", &[]).unwrap();
    assert_eq!(q(&mut db, "SELECT v FROM u ORDER BY id"), vec![json!({"v": "alice"}), json!({"v": "alice"}), json!({"v": "bob"}), json!({"v": "v4"})]);
    db.exec("DELETE FROM u USING t WHERE u.t_id = t.id AND t.name = 'bob'", &[]).unwrap();
    assert_eq!(q(&mut db, "SELECT id FROM u ORDER BY id"), vec![json!({"id": 1}), json!({"id": 2}), json!({"id": 4})]);
    db.exec("WITH s AS (SELECT id FROM t WHERE id = 2) UPDATE t SET n = 99 FROM s WHERE t.id = s.id", &[]).unwrap();
    assert_eq!(q(&mut db, "SELECT id, name, n FROM t ORDER BY id"), vec![
        json!({"id": 1, "name": "alice", "n": 5}),
        json!({"id": 2, "name": "bob", "n": 99}),
        json!({"id": 3, "name": "anon", "n": null}),
    ]);
    // CTE trùng tên bảng thật không được xoá bảng thật
    q(&mut db, "WITH t AS (SELECT 1 AS z) SELECT * FROM t");
    assert!(db.exec("WITH t AS (SELECT nope FROM missing) SELECT * FROM t", &[]).is_err());
    assert_eq!(q(&mut db, "SELECT count(*) AS c FROM t"), vec![json!({"c": 3})]);
    // Kiểu dữ liệu và số giá trị
    assert!(db.exec("INSERT INTO t (id, n) VALUES (10, 'abc')", &[]).is_err());
    assert!(db.exec("INSERT INTO t (id, created) VALUES (11, 'not a date')", &[]).is_err());
    assert!(db.exec("INSERT INTO t (id, n) VALUES (12, 1, 2)", &[]).is_err());
    db.exec("INSERT INTO t (id, n) VALUES (13, '7')", &[]).unwrap();
}
