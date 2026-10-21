//! Repro của các lỗi đã được báo trong `bugs/PGLITE_RS_BUGS.md` và `bugs/DB_RS_BUGS_FOUND.md` (chạy backend
//! WAIKI trên db-rs). Mỗi câu chạy vài lần liên tiếp vì một số lỗi chỉ hiện từ lần thứ hai (plan cache).
//! Kết quả mong đợi lấy từ PostgreSQL thật (cùng các câu này có trong golden `BG_*`).
use pglite_rs::{PGlite, Value};
use serde_json::{json, Value as J};

fn exec(db: &mut PGlite, sql: &str) {
    db.exec(sql, &[]).unwrap_or_else(|e| panic!("{sql}: {e}"));
}

/// Chạy `sql` 3 lần; mọi lần phải cho cùng kết quả.
fn rows3(db: &mut PGlite, sql: &str, params: &[Value]) -> Vec<J> {
    let first = db.query(sql, params).unwrap_or_else(|e| panic!("{sql}: {e}")).rows;
    for i in 2..=3 {
        let again = db.query(sql, params).unwrap_or_else(|e| panic!("lần {i}: {sql}: {e}")).rows;
        assert_eq!(again, first, "lần chạy thứ {i} khác lần đầu: {sql}");
    }
    first
}

fn rewards_db() -> PGlite {
    let mut db = PGlite::in_memory().unwrap();
    exec(&mut db, "CREATE TABLE rw (id INT, title TEXT)");
    exec(&mut db, "INSERT INTO rw VALUES (1, 'r1')");
    exec(&mut db, "CREATE TABLE rd (id INT, reward_id INT, status TEXT, expires_at TIMESTAMPTZ)");
    exec(&mut db, "INSERT INTO rd VALUES (1, 1, 'available', now() + interval '3 days' + interval '1 hour')");
    db
}

fn cat_db() -> PGlite {
    let mut db = PGlite::in_memory().unwrap();
    exec(&mut db, "CREATE TABLE cat (id SERIAL PRIMARY KEY, code TEXT)");
    exec(&mut db, "CREATE TABLE item (id SERIAL PRIMARY KEY, cat_id INT REFERENCES cat(id), title TEXT)");
    exec(&mut db, "CREATE TABLE item2 (id SERIAL PRIMARY KEY, cat_id INT, title TEXT)");
    exec(&mut db, "INSERT INTO cat (code) VALUES ('a'), ('b')");
    db
}

// ---- PGLITE_RS_BUGS.md -------------------------------------------------------------------------

/// Lỗi 1: bảng dẫn xuất `(VALUES ...)`.
#[test]
fn bug1_values_derived_table() {
    let mut db = PGlite::in_memory().unwrap();
    assert_eq!(rows3(&mut db, "SELECT v.* FROM (VALUES (1,'a'),(2,'b')) AS v(id,name)", &[]), vec![json!({"id": 1, "name": "a"}), json!({"id": 2, "name": "b"})]);
    assert_eq!(rows3(&mut db, "SELECT p.m FROM (SELECT 1 AS x) s CROSS JOIN (VALUES (30),(60)) AS p(m)", &[]), vec![json!({"m": 30}), json!({"m": 60})]);
}

/// Lỗi 2: subquery vô hướng trong `INSERT ... VALUES` từng ghi `false` (có FK: 23503; không FK: hỏng âm thầm).
#[test]
fn bug2_scalar_subquery_in_insert_values() {
    let mut db = cat_db();
    for _ in 0..3 {
        let r = db.query("INSERT INTO item (cat_id, title) VALUES ((SELECT id FROM cat WHERE code = $1), $2) RETURNING cat_id", &[Value::text("b"), Value::text("x")]).unwrap().rows;
        assert_eq!(r, vec![json!({"cat_id": 2})]);
        let r = db.query("INSERT INTO item2 (cat_id, title) VALUES ((SELECT id FROM cat WHERE code = 'b'), 'y') RETURNING cat_id", &[]).unwrap().rows;
        assert_eq!(r, vec![json!({"cat_id": 2})]);
        let r = db.query("INSERT INTO item (cat_id, title) VALUES ((SELECT id FROM cat WHERE code = $1), $2) ON CONFLICT DO NOTHING RETURNING cat_id", &[Value::text("a"), Value::text("z")]).unwrap().rows;
        assert_eq!(r, vec![json!({"cat_id": 1})]);
    }
    // không có dòng nào là `false`
    assert_eq!(rows3(&mut db, "SELECT count(*) AS n FROM item2 WHERE cat_id = 2", &[]), vec![json!({"n": 3})]);
}

/// Lỗi 3: `pg_typeof()`.
#[test]
fn bug3_pg_typeof() {
    let mut db = cat_db();
    exec(&mut db, "INSERT INTO item2 (cat_id, title) VALUES (2, 'q')");
    assert_eq!(rows3(&mut db, "SELECT cat_id, pg_typeof(cat_id) AS t FROM item2", &[]), vec![json!({"cat_id": 2, "t": "integer"})]);
}

/// Lỗi 4: `GREATEST`/`LEAST` và biểu thức số học / ngày giờ trong query có JOIN (kể cả `UPDATE ... FROM`).
#[test]
fn bug4_greatest_least_and_math_in_joins() {
    let mut db = rewards_db();
    let join = "FROM rd d JOIN rw w ON w.id = d.reward_id";
    assert_eq!(rows3(&mut db, &format!("SELECT GREATEST(d.id, 5) AS g, GREATEST(5, d.id) AS g2, LEAST(5, d.id) AS l {join}"), &[]), vec![json!({"g": 5, "g2": 5, "l": 1})]);
    assert_eq!(rows3(&mut db, &format!("SELECT GREATEST(0, CEIL(EXTRACT(EPOCH FROM (d.expires_at - CURRENT_TIMESTAMP)) / 86400))::int AS days {join}"), &[]), vec![json!({"days": 4})]);

    let mut db = PGlite::in_memory().unwrap();
    exec(&mut db, "CREATE TABLE sx (id INT, fee INT, dur INT)");
    exec(&mut db, "INSERT INTO sx VALUES (1, 500000, 10)");
    let price = "SELECT p.m, ROUND(COALESCE(s.fee, 0) * 60.0 / GREATEST(s.dur, 15) * p.factor / 10000.0) * 10000 AS price FROM sx s CROSS JOIN (VALUES (30, 0.54), (60, 1.0), (90, 1.46)) AS p(m, factor) ORDER BY p.m";
    assert_eq!(rows3(&mut db, price, &[]), vec![json!({"m": 30, "price": 1080000}), json!({"m": 60, "price": 2000000}), json!({"m": 90, "price": 2920000})]);

    exec(&mut db, "CREATE TABLE bb (id INT PRIMARY KEY, dur INT, price INT)");
    exec(&mut db, "CREATE TABLE ss (id INT PRIMARY KEY, fee INT)");
    exec(&mut db, "INSERT INTO bb VALUES (1, 10, 0), (2, 30, 0)");
    exec(&mut db, "INSERT INTO ss VALUES (1, 600000), (2, 900000)");
    exec(&mut db, "UPDATE bb SET price = ROUND(ss.fee * 60.0 / GREATEST(bb.dur, 15)) FROM ss WHERE ss.id = bb.id");
    assert_eq!(rows3(&mut db, "SELECT id, price FROM bb ORDER BY id", &[]), vec![json!({"id": 1, "price": 2400000}), json!({"id": 2, "price": 1800000})]);
}

/// Lỗi 5: self-join không alias bảng bên trái từng trả 0 dòng.
#[test]
fn bug5_self_join_without_left_alias() {
    let mut db = PGlite::in_memory().unwrap();
    exec(&mut db, "CREATE TABLE sp (id INT, v TEXT)");
    exec(&mut db, "INSERT INTO sp VALUES (1, 'a'), (2, 'b')");
    assert_eq!(rows3(&mut db, "SELECT sp.id FROM sp JOIN sp sp2 ON sp2.id = sp.id ORDER BY 1", &[]), vec![json!({"id": 1}), json!({"id": 2})]);
    assert_eq!(rows3(&mut db, "SELECT sp.id, sp2.v FROM sp JOIN sp AS sp2 ON sp2.id = sp.id + 1 ORDER BY 1", &[]), vec![json!({"id": 1, "v": "b"})]);
    assert_eq!(rows3(&mut db, "SELECT sp.id FROM sp LEFT JOIN sp sp2 ON sp2.id = sp.id WHERE sp2.v = 'b'", &[]), vec![json!({"id": 2})]);
    // đối chứng: có alias cả hai bên vẫn đúng như trước
    assert_eq!(rows3(&mut db, "SELECT a.id FROM sp a JOIN sp b ON b.id = a.id ORDER BY 1", &[]), vec![json!({"id": 1}), json!({"id": 2})]);
}

// ---- DB_RS_BUGS_FOUND.md -----------------------------------------------------------------------

/// Lỗi 2c: `GREATEST(0, CEIL(EXTRACT(...)))` trong JOIN, không ép kiểu, từng ra nguyên văn chuỗi biểu thức.
#[test]
fn dbrs_bug2c_greatest_of_ceil_in_join() {
    let mut db = rewards_db();
    let join = "FROM rd d JOIN rw w ON w.id = d.reward_id";
    assert_eq!(rows3(&mut db, &format!("SELECT CEIL(EXTRACT(EPOCH FROM (d.expires_at - CURRENT_TIMESTAMP)) / 86400) AS c {join}"), &[]), vec![json!({"c": 4})]);
    assert_eq!(rows3(&mut db, &format!("SELECT GREATEST(0, CEIL(EXTRACT(EPOCH FROM (d.expires_at - CURRENT_TIMESTAMP)) / 86400)) AS c {join}"), &[]), vec![json!({"c": 4})]);
    assert_eq!(rows3(&mut db, &format!("SELECT GREATEST(0, d.id + 1)::int AS a, GREATEST(0, CEIL(1.5))::int AS b, LEAST(d.id, w.id, 7) AS l {join}"), &[]), vec![json!({"a": 2, "b": 2, "l": 1})]);
    // dạng của wellness-loyalty `days_left`, có tham số
    assert_eq!(
        rows3(&mut db, &format!("SELECT d.id, GREATEST(0, CEIL(EXTRACT(EPOCH FROM (d.expires_at - CURRENT_TIMESTAMP)) / 86400)) AS days_left {join} WHERE d.id = $1"), &[Value::Int(1)]),
        vec![json!({"id": 1, "days_left": 4})]
    );
}

/// Lỗi 5 (db-rs): `= ANY($1)` trên cột BIGINT.
#[test]
fn dbrs_bug5_any_param_on_bigint() {
    let mut db = PGlite::in_memory().unwrap();
    exec(&mut db, "CREATE TABLE big (id BIGINT PRIMARY KEY, v TEXT)");
    exec(&mut db, "INSERT INTO big VALUES (1, 'a'), (2, 'b'), (3, 'c')");
    let p = [Value::text("[1,3]")];
    assert_eq!(rows3(&mut db, "SELECT id FROM big WHERE id = ANY($1) ORDER BY id", &p), vec![json!({"id": 1}), json!({"id": 3})]);
    assert_eq!(rows3(&mut db, "SELECT count(*) AS n FROM big WHERE id = ANY($1)", &p), vec![json!({"n": 2})]);
}

/// Lỗi 6 (db-rs): `$n` trong danh sách SELECT có FROM ra NULL từ lần chạy thứ hai (plan cache), làm seed
/// `wellness-library` (`INSERT ... SELECT $1, id FROM ...`) vi phạm NOT NULL.
#[test]
fn dbrs_bug6_param_in_select_list_on_repeat() {
    let mut db = cat_db();
    exec(&mut db, "CREATE TABLE pi4 (id SERIAL PRIMARY KEY, program_id INT NOT NULL, item_id INT NOT NULL)");
    for (p, code, id) in [(8, "a", 1), (9, "a", 1), (10, "b", 2)] {
        let r = db.query("SELECT $1 AS p, id FROM cat WHERE code = $2", &[Value::Int(p), Value::text(code)]).unwrap().rows;
        assert_eq!(r, vec![json!({"p": p, "id": id})], "p = {p}");
    }
    for p in 1..=5 {
        let r = db.query("INSERT INTO pi4 (program_id, item_id) SELECT $1, id FROM cat WHERE code = $2 RETURNING program_id", &[Value::Int(p), Value::text("a")]).unwrap().rows;
        assert_eq!(r, vec![json!({"program_id": p})]);
    }
    assert_eq!(rows3(&mut db, "SELECT count(*) AS n FROM pi4 WHERE program_id IS NOT NULL", &[]), vec![json!({"n": 5})]);
    for p in [5, 6, 7] {
        let r = db.query("SELECT $1 AS p, id FROM cat ORDER BY id", &[Value::Int(p)]).unwrap().rows;
        assert_eq!(r, vec![json!({"p": p, "id": 1}), json!({"p": p, "id": 2})]);
    }
}

// ---- DB_RS_BUGS_FOUND.md, kiểm tra lần 4 -------------------------------------------------------

fn customers_db() -> PGlite {
    let mut db = PGlite::in_memory().unwrap();
    exec(&mut db, "CREATE TABLE tenants (id SERIAL PRIMARY KEY, name TEXT)");
    exec(&mut db, "INSERT INTO tenants (name) VALUES ('t1')");
    exec(&mut db, "CREATE TABLE customers (id SERIAL PRIMARY KEY, tenant_id INT, code TEXT)");
    for i in 1..=10 {
        exec(&mut db, &format!("INSERT INTO customers (tenant_id, code) VALUES (1, 'CUST-{:04}')", i));
    }
    exec(&mut db, "CREATE TABLE customer_addresses (id SERIAL PRIMARY KEY, customer_id INT, addr TEXT)");
    exec(&mut db, "INSERT INTO customer_addresses (customer_id, addr) VALUES (1, 'a'), (2, 'b'), (3, 'c')");
    exec(&mut db, "CREATE TABLE pk (id SERIAL PRIMARY KEY, slug TEXT)");
    for i in 1..=6 {
        exec(&mut db, &format!("INSERT INTO pk (slug) VALUES ('pkg-{i}')"));
    }
    exec(&mut db, "INSERT INTO pk (slug) VALUES ('spine-recovery-7n6d')");
    db
}

/// Lỗi 2c (dạng thật của wellness-loyalty `days_left`): `CASE WHEN ... THEN GREATEST(...)::int END` và
/// `CAST(GREATEST(...) AS int)` trong JOIN từng ra `22P02`.
#[test]
fn dbrs_bug2c_cast_and_case_forms() {
    let mut db = rewards_db();
    let join = "FROM rd d JOIN rw w ON w.id = d.reward_id";
    assert_eq!(rows3(&mut db, &format!("SELECT CAST(GREATEST(0, CEIL(EXTRACT(EPOCH FROM (d.expires_at - CURRENT_TIMESTAMP)) / 86400)) AS int) AS c {join}"), &[]), vec![json!({"c": 4})]);
    exec(&mut db, "INSERT INTO rd VALUES (2, 1, 'used', NULL)");
    let case = format!(
        "SELECT d.id, CASE WHEN d.status = 'available' AND d.expires_at IS NOT NULL THEN GREATEST(0, CEIL(EXTRACT(EPOCH FROM (d.expires_at - CURRENT_TIMESTAMP)) / 86400))::int END AS days_left, w.title AS reward_title {join} ORDER BY d.id"
    );
    assert_eq!(rows3(&mut db, &case, &[]), vec![json!({"id": 1, "days_left": 4, "reward_title": "r1"}), json!({"id": 2, "days_left": null, "reward_title": "r1"})]);
}

/// Lỗi 7: ép kiểu hàm gộp trong query có JOIN ra NULL.
#[test]
fn dbrs_bug7_cast_aggregate_in_join() {
    let mut db = customers_db();
    let join = "FROM customers c JOIN customer_addresses a ON a.customer_id = c.id";
    assert_eq!(rows3(&mut db, &format!("SELECT count(*) AS n {join}"), &[]), vec![json!({"n": 3})]);
    assert_eq!(rows3(&mut db, &format!("SELECT count(*)::int AS n, COUNT(c.id)::int AS m, sum(a.id)::int AS s, max(a.id)::text AS mx {join}"), &[]), vec![json!({"n": 3, "m": 3, "s": 6, "mx": "3"})]);
    assert_eq!(rows3(&mut db, &format!("SELECT c.tenant_id, count(*)::int AS n, CAST(sum(a.id) AS bigint) AS s, COALESCE(sum(a.id), 0)::int AS cs {join} GROUP BY c.tenant_id"), &[]), vec![json!({"tenant_id": 1, "n": 3, "s": 6, "cs": 6})]);
    assert_eq!(rows3(&mut db, "SELECT count(*)::int AS n FROM customers c CROSS JOIN pk p", &[]), vec![json!({"n": 70})]);
    assert_eq!(rows3(&mut db, "SELECT count(*)::int AS n FROM customers", &[]), vec![json!({"n": 10})]);
}

/// Lỗi 8: `LIMIT` trong query nhiều bảng (dấu phẩy / CROSS JOIN) + WHERE từng trả 0 dòng khi dòng khớp không nằm
/// trong N dòng ghép đầu tiên.
#[test]
fn dbrs_bug8_limit_with_multi_table_where() {
    let mut db = customers_db();
    let w = "WHERE c.code = 'CUST-0010' AND p.slug = 'spine-recovery-7n6d'";
    let want = vec![json!({"id": 10, "pid": 7})];
    for from in ["FROM customers c, pk p", "FROM customers c CROSS JOIN pk p", "FROM customers c JOIN pk p ON true"] {
        for lim in ["", " LIMIT 1", " LIMIT 5"] {
            assert_eq!(rows3(&mut db, &format!("SELECT c.id, p.id AS pid {from} {w}{lim}"), &[]), want, "{from}{lim}");
        }
    }
}

/// Lỗi 9: `EXISTS` / `NOT EXISTS` tương quan trong query có JOIN (EXISTS từng luôn đúng, NOT EXISTS luôn sai).
#[test]
fn dbrs_bug9_correlated_exists_in_join() {
    let mut db = customers_db();
    let ids = |v: std::ops::RangeInclusive<i64>| v.map(|i| json!({"id": i})).collect::<Vec<_>>();
    let sub = "(SELECT 1 FROM customer_addresses a WHERE a.customer_id = c.id)";
    assert_eq!(rows3(&mut db, &format!("SELECT c.id FROM customers c WHERE NOT EXISTS {sub} ORDER BY 1"), &[]), ids(4..=10));
    assert_eq!(rows3(&mut db, &format!("SELECT c.id FROM customers c JOIN tenants t ON t.id = c.tenant_id WHERE NOT EXISTS {sub} ORDER BY 1"), &[]), ids(4..=10));
    assert_eq!(rows3(&mut db, &format!("SELECT c.id FROM tenants t JOIN customers c ON c.tenant_id = t.id WHERE EXISTS {sub} ORDER BY 1"), &[]), ids(1..=3));
    assert_eq!(rows3(&mut db, &format!("SELECT c.id FROM customers c JOIN tenants t ON t.id = c.tenant_id WHERE c.id > 2 AND NOT EXISTS {sub} AND c.id < 6 ORDER BY 1 LIMIT 2"), &[]), ids(4..=5));
    // seed địa chỉ của core/customers/lifecycle.ts
    let r = db
        .query(&format!("INSERT INTO customer_addresses (customer_id, addr) SELECT c.id, v.addr FROM (VALUES ('CUST-0004', 'x'), ('CUST-0005', 'y')) AS v(code, addr) JOIN customers c ON c.code = v.code WHERE NOT EXISTS {sub} RETURNING customer_id"), &[])
        .unwrap()
        .rows;
    assert_eq!(r, vec![json!({"customer_id": 4}), json!({"customer_id": 5})]);
    // chạy lại: đã có địa chỉ nên không thêm nữa
    let r = db
        .query(&format!("INSERT INTO customer_addresses (customer_id, addr) SELECT c.id, v.addr FROM (VALUES ('CUST-0004', 'x'), ('CUST-0005', 'y')) AS v(code, addr) JOIN customers c ON c.code = v.code WHERE NOT EXISTS {sub} RETURNING customer_id"), &[])
        .unwrap()
        .rows;
    assert!(r.is_empty());
}

// ---- DB_RS_BUGS_FOUND.md, kiểm tra lần 5 -------------------------------------------------------

/// Lỗi 11: `current_setting(...)` từng trả nguyên văn câu chữ.
#[test]
fn dbrs_bug11_current_setting() {
    let mut db = PGlite::in_memory().unwrap();
    assert_eq!(rows3(&mut db, "SELECT current_setting('TimeZone') AS tz, current_setting('nope', true) AS m", &[]), vec![json!({"tz": "UTC", "m": null})]);
    assert!(db.query("SELECT current_setting('nope')", &[]).unwrap_err().contains("unrecognized configuration parameter"));
    exec(&mut db, "SELECT set_config('app.user_id', '42', false)");
    assert_eq!(rows3(&mut db, "SELECT current_setting('app.user_id') AS u", &[]), vec![json!({"u": "42"})]);
    exec(&mut db, "SET TIME ZONE 'Asia/Ho_Chi_Minh'");
    assert_eq!(rows3(&mut db, "SELECT current_setting('TimeZone') AS tz", &[]), vec![json!({"tz": "Asia/Ho_Chi_Minh"})]);
}

/// Lỗi 12: `::text` của numeric(p,s) / jsonb khác PostgreSQL (mất số 0 sau dấu chấm, JSON không chuẩn hoá).
#[test]
fn dbrs_bug12_text_cast_of_numeric_and_jsonb() {
    let mut db = PGlite::in_memory().unwrap();
    assert_eq!(
        rows3(&mut db, r#"SELECT 450000.00::numeric(12,2)::text AS a, '{"vi":"a","en":"b"}'::jsonb::text AS b, 1.5::numeric(10,3)::text AS c"#, &[]),
        vec![json!({"a": "450000.00", "b": "{\"en\": \"b\", \"vi\": \"a\"}", "c": "1.500"})]
    );
    exec(&mut db, "CREATE TABLE p (id SERIAL PRIMARY KEY, price NUMERIC(12,2), meta JSONB)");
    exec(&mut db, r#"INSERT INTO p (price, meta) VALUES (450000.00, '{"vi":"a","en":"b"}')"#);
    let w = db.query_wire("SELECT price::text AS p, meta::text AS m FROM p", &[]).unwrap();
    assert_eq!(w.rows, vec![vec![Some("450000.00".to_string()), Some("{\"en\": \"b\", \"vi\": \"a\"}".to_string())]]);
    assert_eq!(rows3(&mut db, "SELECT price::text AS p FROM p", &[]), vec![json!({"p": "450000.00"})]);
}

/// Lỗi 13: đường JSON mặc định đoán sai kiểu chữ: `{{app_name}} code {{code}}` thành mảng, jsonb `'{}'`
/// thành `[]`, `x::text` thành mảng / object. Cột TEXT chứa mảng PG hợp lệ vẫn ra mảng như trước (app cũ dùng).
#[test]
fn dbrs_bug13_json_output_of_text_and_jsonb() {
    let mut db = PGlite::in_memory().unwrap();
    exec(&mut db, "CREATE TABLE t13 (id SERIAL PRIMARY KEY, tpl TEXT, j JSONB, obj JSONB, arr TEXT[], legacy TEXT)");
    exec(&mut db, r#"INSERT INTO t13 (tpl, j, obj, arr, legacy) VALUES ('{{app_name}} code {{code}}', '{}', '{"x":1}', '{a,b}', '{1,2}')"#);
    let want = vec![json!({"tpl": "{{app_name}} code {{code}}", "j": {}, "obj": {"x": 1}, "arr": ["a", "b"], "legacy": [1, 2]})];
    assert_eq!(rows3(&mut db, "SELECT tpl, j, obj, arr, legacy FROM t13", &[]), want);
    assert_eq!(rows3(&mut db, "SELECT tpl, j, obj, arr, legacy FROM t13 WHERE id = $1", &[Value::Int(1)]), want);
    assert_eq!(rows3(&mut db, "SELECT j::text AS jt, legacy::text AS lt FROM t13", &[]), vec![json!({"jt": "{}", "lt": "{1,2}"})]);
    // pg_text (wire) giữ nguyên văn
    let w = db.query_wire("SELECT tpl, j FROM t13", &[]).unwrap();
    assert_eq!(w.rows, vec![vec![Some("{{app_name}} code {{code}}".to_string()), Some("{}".to_string())]]);
}
