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

#[test]
fn test_kysely_schedule_left_joins_and_date_filtering() {
    let mut db = PGlite::in_memory().unwrap();

    db.exec("CREATE TABLE users (id SERIAL PRIMARY KEY, full_name TEXT);", &[]).unwrap();
    db.exec("CREATE TABLE schools (id SERIAL PRIMARY KEY, name TEXT);", &[]).unwrap();
    db.exec("CREATE TABLE schedules (
        id SERIAL PRIMARY KEY,
        user_id INT,
        school_id INT,
        start_date DATE,
        end_date DATE,
        deleted_at TIMESTAMP
    );", &[]).unwrap();

    db.exec("INSERT INTO users VALUES (1, 'Teacher Alice'), (2, 'Teacher Bob');", &[]).unwrap();
    db.exec("INSERT INTO schools VALUES (10, 'High School A'), (20, 'Primary School B');", &[]).unwrap();
    
    // Schedule 1: valid May schedule
    db.exec("INSERT INTO schedules VALUES (100, 1, 10, '2024-05-02', '2024-05-25', NULL);", &[]).unwrap();
    // Schedule 2: open-ended schedule (end_date IS NULL)
    db.exec("INSERT INTO schedules VALUES (101, 1, 10, '2024-05-10', NULL, NULL);", &[]).unwrap();
    // Schedule 3: deleted schedule (deleted_at IS NOT NULL)
    db.exec("INSERT INTO schedules VALUES (102, 1, 10, '2024-05-15', '2024-05-20', '2024-05-16 00:00:00');", &[]).unwrap();
    // Schedule 4: schedule in April (out of range)
    db.exec("INSERT INTO schedules VALUES (103, 1, 10, '2024-04-01', '2024-04-20', NULL);", &[]).unwrap();

    let sql = r#"
      SELECT
        "schedules"."id",
        "schedules"."user_id" AS "userId",
        "users"."full_name" AS "teacherName",
        "schedules"."school_id" AS "schoolId",
        "schools"."name" AS "schoolName",
        "schedules"."start_date" AS "startDate",
        "schedules"."end_date" AS "endDate"
      FROM "schedules"
      LEFT JOIN "users" ON "users"."id" = "schedules"."user_id"
      LEFT JOIN "schools" ON "schools"."id" = "schedules"."school_id"
      WHERE "schedules"."deleted_at" IS NULL
        AND "schedules"."start_date" <= $1
        AND ("schedules"."end_date" IS NULL OR "schedules"."end_date" >= $2)
        AND "schedules"."user_id" = $3
        AND "schedules"."school_id" = $4
    "#;

    let res = db.query(sql, &params!["2024-05-31T23:59:59.000Z", "2024-05-01T00:00:00.000Z", 1, 10]).unwrap();
    assert_eq!(res.row_count, 2);
    assert_eq!(res.rows[0]["teacherName"], "Teacher Alice");
    assert_eq!(res.rows[0]["schoolName"], "High School A");
}

#[test]
fn test_kysely_schedule_time_slots_in_operator() {
    let mut db = PGlite::in_memory().unwrap();

    db.exec("CREATE TABLE schools (id SERIAL PRIMARY KEY, name TEXT);", &[]).unwrap();
    db.exec("CREATE TABLE schedule_time_slots (
        id SERIAL PRIMARY KEY,
        schedule_id INT,
        day_of_week INT,
        start_time TEXT,
        end_time TEXT,
        school_id INT,
        class_ids JSONB,
        no_fuel_allowance BOOLEAN DEFAULT false,
        no_travel_allowance BOOLEAN DEFAULT false,
        support_amount NUMERIC DEFAULT 0,
        is_non_salary_session BOOLEAN DEFAULT false,
        deleted_at TIMESTAMP
    );", &[]).unwrap();

    db.exec("INSERT INTO schools VALUES (1, 'Greenwood High'), (2, 'Oakridge School');", &[]).unwrap();
    db.exec("INSERT INTO schedule_time_slots (id, schedule_id, day_of_week, start_time, end_time, school_id, deleted_at)
        VALUES (10, 100, 2, '08:00', '09:30', 1, NULL),
               (11, 100, 3, '10:00', '11:30', 2, NULL),
               (12, 101, 2, '13:00', '14:30', 1, NULL),
               (13, 100, 2, '15:00', '16:30', 1, '2024-05-01 00:00:00'),
               (14, 999, 4, '08:00', '09:30', 2, NULL);", &[]).unwrap();

    // Kysely style query: expanded IN ($1, $2)
    let sql = r#"
      SELECT
        "schedule_time_slots"."id",
        "schedule_time_slots"."schedule_id" AS "scheduleId",
        "schedule_time_slots"."day_of_week" AS "dayOfWeek",
        "schools"."name" AS "schoolName"
      FROM "schedule_time_slots"
      LEFT JOIN "schools" ON "schools"."id" = "schedule_time_slots"."school_id"
      WHERE "schedule_time_slots"."schedule_id" IN ($1, $2)
        AND "schedule_time_slots"."deleted_at" IS NULL
      ORDER BY "schedule_time_slots"."day_of_week", "schedule_time_slots"."start_time";
    "#;

    let res = db.query(sql, &params![100, 101]).unwrap();
    assert_eq!(res.row_count, 3);
}
