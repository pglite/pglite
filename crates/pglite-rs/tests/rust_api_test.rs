use pglite_rs::{params, PGlite};
use serde::Deserialize;
use std::fs;

#[derive(Debug, Deserialize, PartialEq)]
struct Employee {
    id: i64,
    name: String,
    department: String,
    salary: f64,
}

#[test]
fn test_in_memory_crud_and_params() {
    let mut db = PGlite::in_memory().expect("failed to init db");

    db.exec(
        "CREATE TABLE employees (id SERIAL PRIMARY KEY, name TEXT, department TEXT, salary NUMERIC);",
        &[],
    )
    .unwrap();

    // Insert records with params! macro
    db.exec(
        "INSERT INTO employees (name, department, salary) VALUES ($1, $2, $3);",
        &params!["Alice", "Engineering", 120000.0],
    )
    .unwrap();
    db.exec(
        "INSERT INTO employees (name, department, salary) VALUES ($1, $2, $3);",
        &params!["Bob", "Design", 95000.0],
    )
    .unwrap();
    db.exec(
        "INSERT INTO employees (name, department, salary) VALUES ($1, $2, $3);",
        &params!["Charlie", "Engineering", 110000.0],
    )
    .unwrap();

    // Typed Query
    let eng_employees: Vec<Employee> = db
        .query_as(
            "SELECT id, name, department, salary FROM employees WHERE department = $1 ORDER BY salary DESC;",
            &params!["Engineering"],
        )
        .unwrap();

    assert_eq!(eng_employees.len(), 2);
    assert_eq!(eng_employees[0].name, "Alice");
    assert_eq!(eng_employees[1].name, "Charlie");

    // Single record query
    let bob: Option<Employee> = db
        .query_first_as(
            "SELECT id, name, department, salary FROM employees WHERE name = $1;",
            &params!["Bob"],
        )
        .unwrap();
    assert!(bob.is_some());
    assert_eq!(bob.unwrap().salary, 95000.0);

    // Update
    db.exec("UPDATE employees SET salary = salary + $1 WHERE name = $2;", &params![5000.0, "Bob"]).unwrap();
    let updated_bob: Employee = db
        .query_first_as("SELECT id, name, department, salary FROM employees WHERE name = 'Bob';", &[])
        .unwrap()
        .unwrap();
    assert_eq!(updated_bob.salary, 100000.0);

    // Delete
    db.exec("DELETE FROM employees WHERE name = $1;", &params!["Charlie"]).unwrap();
    let count_res = db.query("SELECT COUNT(*) AS total FROM employees;", &[]).unwrap();
    assert_eq!(count_res.rows[0]["total"], 2);
}

#[test]
fn test_disk_persistence_and_reload() {
    let db_path = "test_rust_persistence.db";
    let cleanup = |path: &str| {
        let _ = fs::remove_file(path);
        let _ = fs::remove_file(format!("{}.rwal", path));
        let _ = fs::remove_file(format!("{}.v2.rwal", path));
        let _ = fs::remove_file(format!("{}.wal", path));
    };

    cleanup(db_path);

    // Phase 1: Write data to disk
    {
        let mut db = PGlite::open(db_path).unwrap();
        db.exec("CREATE TABLE settings (key TEXT PRIMARY KEY, value TEXT);", &[]).unwrap();
        db.exec("INSERT INTO settings (key, value) VALUES ($1, $2);", &params!["theme", "dark"]).unwrap();
        db.exec("INSERT INTO settings (key, value) VALUES ($1, $2);", &params!["lang", "en"]).unwrap();
        db.flush();
    }

    // Phase 2: Reopen database from disk and verify persistence
    {
        let mut db = PGlite::open(db_path).unwrap();
        let res = db.query("SELECT key, value FROM settings ORDER BY key ASC;", &[]).unwrap();
        assert_eq!(res.row_count, 2);
        assert_eq!(res.rows[0]["key"], "lang");
        assert_eq!(res.rows[0]["value"], "en");
        assert_eq!(res.rows[1]["key"], "theme");
        assert_eq!(res.rows[1]["value"], "dark");
    }

    cleanup(db_path);
}

#[test]
fn test_transactions_commit_and_rollback() {
    let mut db = PGlite::in_memory().unwrap();
    db.exec("CREATE TABLE counters (id INT PRIMARY KEY, val INT);", &[]).unwrap();
    db.exec("INSERT INTO counters VALUES (1, 10);", &[]).unwrap();

    // 1. Rollback test
    db.exec("BEGIN", &[]).unwrap();
    db.exec("UPDATE counters SET val = 99 WHERE id = 1;", &[]).unwrap();
    db.exec("ROLLBACK", &[]).unwrap();

    let res = db.query("SELECT val FROM counters WHERE id = 1;", &[]).unwrap();
    assert_eq!(res.rows[0]["val"], 10);

    // 2. Commit test
    db.exec("BEGIN", &[]).unwrap();
    db.exec("UPDATE counters SET val = 42 WHERE id = 1;", &[]).unwrap();
    db.exec("COMMIT", &[]).unwrap();

    let res2 = db.query("SELECT val FROM counters WHERE id = 1;", &[]).unwrap();
    assert_eq!(res2.rows[0]["val"], 42);
}

#[test]
fn test_complex_subqueries_and_joins() {
    let mut db = PGlite::in_memory().unwrap();

    db.exec("CREATE TABLE depts (id INT PRIMARY KEY, name TEXT);", &[]).unwrap();
    db.exec("CREATE TABLE users (id INT PRIMARY KEY, name TEXT, dept_id INT);", &[]).unwrap();

    db.exec("INSERT INTO depts VALUES (1, 'Engineering'), (2, 'Marketing');", &[]).unwrap();
    db.exec("INSERT INTO users VALUES (10, 'Dev1', 1), (11, 'Dev2', 1), (12, 'Marketer1', 2);", &[]).unwrap();

    // INNER JOIN
    let join_res = db.query(
        "SELECT u.name AS user_name, d.name AS dept_name FROM users u JOIN depts d ON u.dept_id = d.id ORDER BY u.id;",
        &[],
    ).unwrap();
    assert_eq!(join_res.row_count, 3);
    assert_eq!(join_res.rows[0]["user_name"], "Dev1");
    assert_eq!(join_res.rows[0]["dept_name"], "Engineering");

    // Subquery IN
    let sub_res = db.query(
        "SELECT name FROM users WHERE dept_id IN (SELECT id FROM depts WHERE name = 'Engineering') ORDER BY id;",
        &[],
    ).unwrap();
    assert_eq!(sub_res.row_count, 2);
    assert_eq!(sub_res.rows[0]["name"], "Dev1");
    assert_eq!(sub_res.rows[1]["name"], "Dev2");
}

#[test]
fn test_param_types_conversion() {
    let mut db = PGlite::in_memory().unwrap();
    db.exec("CREATE TABLE types_test (i INT, f FLOAT, b BOOLEAN, t TEXT, opt TEXT);", &[]).unwrap();

    let opt_some: Option<&str> = Some("hello");
    let opt_none: Option<String> = None;

    db.exec(
        "INSERT INTO types_test VALUES ($1, $2, $3, $4, $5);",
        &params![42i32, 3.14159f64, true, "sample_text", opt_some],
    ).unwrap();

    db.exec(
        "INSERT INTO types_test VALUES ($1, $2, $3, $4, $5);",
        &params![100i64, 2.718f32, false, "other_text", opt_none],
    ).unwrap();

    let rows = db.query("SELECT * FROM types_test ORDER BY i ASC;", &[]).unwrap();
    assert_eq!(rows.row_count, 2);
    assert_eq!(rows.rows[0]["i"], 42);
    assert_eq!(rows.rows[0]["b"], true);
    assert_eq!(rows.rows[0]["opt"], "hello");
    assert_eq!(rows.rows[1]["opt"], serde_json::Value::Null);
}
