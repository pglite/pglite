//! Đợt checklist P1 (không cần lưu metadata): những gì golden test (đường wire) không phủ được — kết quả
//! JSON giữ hành vi cũ, dữ liệu trên đĩa sau khi mở lại DB, các lỗi ghi sai dữ liệu đã gặp.
use pglite_rs::PGlite;
use serde_json::json;

fn rows(db: &mut PGlite, sql: &str) -> Vec<serde_json::Value> {
    db.query(sql, &[]).unwrap_or_else(|e| panic!("{sql}: {e}")).rows
}

fn temp_path(name: &str) -> String {
    let dir = std::env::temp_dir().join(format!("pglite_wave7_{}_{}", name, std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir.join("db.db").to_string_lossy().to_string()
}

fn setup(db: &mut PGlite) {
    db.exec("CREATE TABLE t (id SERIAL PRIMARY KEY, name TEXT, n INT)", &[]).unwrap();
    db.exec("CREATE TABLE u (id SERIAL PRIMARY KEY, t_id INT, v TEXT)", &[]).unwrap();
    db.exec("INSERT INTO t (name, n) VALUES ('alice', 5), ('bob', 3)", &[]).unwrap();
    db.exec("INSERT INTO u (t_id, v) VALUES (1, 'v1'), (1, 'v2'), (2, 'v3')", &[]).unwrap();
}

#[test]
fn uncorrelated_set_subqueries_write_the_value_and_survive_reopen() {
    // trước đây: UPDATE ghi `false`, UPDATE ... FROM ghi NULL, ON CONFLICT DO UPDATE ghi chữ "min(v) FROM u"
    let path = temp_path("setsub");
    {
        let mut db = PGlite::open(&path).unwrap();
        setup(&mut db);
        db.exec("UPDATE t SET name = (SELECT v FROM u WHERE u.id = 3) WHERE id = 1", &[]).unwrap();
        db.exec("UPDATE t SET name = (SELECT max(v) FROM u), n = u.id FROM u WHERE u.t_id = t.id AND u.id = 3", &[]).unwrap();
        db.exec("INSERT INTO t (id, name) VALUES (1, 'x') ON CONFLICT (id) DO UPDATE SET name = (SELECT min(v) FROM u)", &[]).unwrap();
        db.exec("UPDATE t SET (name, n) = (SELECT v, t_id FROM u WHERE u.id = 2) WHERE id = 2", &[]).unwrap();
        db.close();
    }
    let mut db = PGlite::open(&path).unwrap();
    assert_eq!(rows(&mut db, "SELECT id, name, n FROM t ORDER BY id"), vec![json!({"id": 1, "name": "v1", "n": 5}), json!({"id": 2, "name": "v2", "n": 1})]);
    // subquery tương quan vẫn tính theo từng dòng
    db.exec("UPDATE t SET n = (SELECT count(*) FROM u WHERE u.t_id = t.id)", &[]).unwrap();
    assert_eq!(rows(&mut db, "SELECT n FROM t ORDER BY id"), vec![json!({"n": 2}), json!({"n": 1})]);
    assert!(db.exec("UPDATE t SET name = (SELECT v FROM u)", &[]).unwrap_err().contains("more than one row"));
}

#[test]
fn catalog_tables_keep_legacy_star_shape() {
    let mut db = PGlite::in_memory().unwrap();
    setup(&mut db);
    // `SELECT *` không WHERE: các khoá của bản cũ giữ nguyên
    let all = rows(&mut db, "SELECT * FROM information_schema.tables");
    assert_eq!(all.len(), 2);
    for k in ["table_schema", "schema_name", "table_name", "table_type", "is_temporary", "comment"] {
        assert!(all[0].get(k).is_some(), "thiếu khoá {k}: {}", all[0]);
    }
    // có WHERE / danh sách cột: lọc thật và chỉ trả cột được chọn
    assert_eq!(rows(&mut db, "SELECT table_name FROM information_schema.tables WHERE table_name = 'u'"), vec![json!({"table_name": "u"})]);
    assert_eq!(rows(&mut db, "SELECT tablename FROM pg_tables WHERE schemaname = 'public' ORDER BY tablename"), vec![json!({"tablename": "t"}), json!({"tablename": "u"})]);
    // bảng tạm nội bộ không lộ ra / không còn lại sau truy vấn
    assert!(rows(&mut db, "SELECT table_name FROM information_schema.tables WHERE table_name LIKE '\\_\\_%'").is_empty());
    assert!(db.query("SELECT * FROM __pglite_info_tables", &[]).is_err());
}

#[test]
fn prepared_statements_and_maintenance_commands() {
    let path = temp_path("prepare");
    {
        let mut db = PGlite::open(&path).unwrap();
        setup(&mut db);
        db.exec("PREPARE by_name(text) AS SELECT id FROM t WHERE name = $1", &[]).unwrap();
        assert_eq!(rows(&mut db, "EXECUTE by_name('bob')"), vec![json!({"id": 2})]);
        assert!(db.exec("PREPARE by_name AS SELECT 1", &[]).unwrap_err().contains("already exists"));
        db.exec("PREPARE ins(text) AS INSERT INTO u (t_id, v) VALUES (2, $1)", &[]).unwrap();
        db.exec("EXECUTE ins('v4')", &[]).unwrap();
        db.exec("DEALLOCATE by_name", &[]).unwrap();
        assert!(db.query("EXECUTE by_name('bob')", &[]).unwrap_err().contains("does not exist"));
        for st in ["ANALYZE", "ANALYZE t", "VACUUM", "VACUUM FULL t", "VACUUM (VERBOSE, ANALYZE) u", "CHECKPOINT"] {
            db.exec(st, &[]).unwrap_or_else(|e| panic!("{st}: {e}"));
        }
        assert!(db.exec("ANALYZE nope", &[]).unwrap_err().contains("does not exist"));
        db.exec("BEGIN", &[]).unwrap();
        assert!(db.exec("VACUUM", &[]).unwrap_err().contains("transaction block"));
        db.exec("ROLLBACK", &[]).unwrap();
        db.close();
    }
    // câu lệnh chuẩn bị chỉ sống trong phiên; dữ liệu từ EXECUTE thì được ghi
    let mut db = PGlite::open(&path).unwrap();
    assert!(db.query("EXECUTE ins('v5')", &[]).is_err());
    assert_eq!(rows(&mut db, "SELECT v FROM u WHERE id = 4"), vec![json!({"v": "v4"})]);
}

#[test]
fn object_agg_natural_join_and_json_srfs() {
    let mut db = PGlite::in_memory().unwrap();
    setup(&mut db);
    assert_eq!(rows(&mut db, "SELECT jsonb_object_agg(name, n) AS o FROM t"), vec![json!({"o": {"alice": 5, "bob": 3}})]);
    assert_eq!(
        rows(&mut db, "SELECT t_id, jsonb_object_agg(id, v) AS o FROM u GROUP BY t_id ORDER BY t_id"),
        vec![json!({"t_id": 1, "o": {"1": "v1", "2": "v2"}}), json!({"t_id": 2, "o": {"3": "v3"}})]
    );
    assert_eq!(rows(&mut db, "SELECT t.name, u.v FROM t NATURAL JOIN u ORDER BY t.id"), vec![json!({"name": "alice", "v": "v1"}), json!({"name": "bob", "v": "v2"})]);
    assert_eq!(rows(&mut db, "SELECT key, value FROM jsonb_each_text('{\"b\":\"x\",\"a\":\"y\"}') ORDER BY key"), vec![json!({"key": "a", "value": "y"}), json!({"key": "b", "value": "x"})]);
    assert_eq!(rows(&mut db, "SELECT count(*) AS c FROM jsonb_array_elements('[1,2,3]')"), vec![json!({"c": 3})]);
    // bảng tạm của hàm không còn lại
    assert!(!db.executor().storage.tables.keys().any(|k| k.starts_with("__")));
}
