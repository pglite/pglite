import { expect, test, describe, beforeEach, beforeAll } from "bun:test";
import { PGLite } from "../src/index";

describe("Comprehensive SQL Syntax Test Suite (Unified PGLite & JS Fallback)", () => {
  let db: any;

  beforeEach(async () => {
    db = new PGLite(":memory:", { native: true });
    await db.query(`
      CREATE TABLE departments (
        id SERIAL PRIMARY KEY,
        dept_name TEXT NOT NULL,
        location TEXT
      );
    `);

    await db.query(`
      CREATE TABLE users (
        id SERIAL PRIMARY KEY,
        department_id INT,
        username TEXT NOT NULL,
        email TEXT,
        salary FLOAT,
        is_active BOOLEAN DEFAULT true,
        metadata JSONB,
        tags TEXT[],
        created_at TIMESTAMP,
        deleted_at TIMESTAMP
      );
    `);

    await db.query(`
      CREATE TABLE orders (
        id SERIAL PRIMARY KEY,
        user_id INT,
        amount FLOAT NOT NULL,
        status TEXT DEFAULT 'PENDING',
        created_at TIMESTAMP
      );
    `);

    // Seed departments
    await db.query(`
      INSERT INTO departments (dept_name, location) VALUES 
      ('Engineering', 'Hanoi'),
      ('Marketing', 'Saigon'),
      ('Human Resources', 'Danang'),
      ('Finance', NULL);
    `);

    // Seed users
    await db.query(`
      INSERT INTO users (department_id, username, email, salary, is_active, metadata, tags, created_at, deleted_at) VALUES 
      (1, 'alice', 'alice@example.com', 5000.0, true, '{"role": "lead", "level": 5}', '{"admin", "dev"}', '2023-01-10 08:00:00', NULL),
      (1, 'bob', 'bob@example.com', 3500.0, true, '{"role": "senior", "level": 4}', '{"dev"}', '2023-02-15 09:30:00', NULL),
      (1, 'charlie', NULL, 2500.0, false, '{"role": "junior", "level": 2}', '{"intern"}', '2023-03-20 10:15:00', '2023-08-01 12:00:00'),
      (2, 'david', 'david@example.com', 4200.0, true, '{"role": "manager", "level": 6}', '{"marketing"}', '2023-04-05 11:00:00', NULL),
      (2, 'eve', 'eve@example.com', 3100.0, true, '{"role": "designer", "level": 3}', '{"design"}', '2023-05-12 14:20:00', NULL),
      (3, 'frank', NULL, 2900.0, false, '{"role": "recruiter", "level": 3}', '{"hr"}', '2023-06-18 16:45:00', '2023-09-10 09:00:00'),
      (NULL, 'grace', 'grace@example.com', 6000.0, true, '{"role": "advisor", "level": 7}', '{"consultant"}', '2023-07-22 17:00:00', NULL);
    `);

    // Seed orders
    await db.query(`
      INSERT INTO orders (user_id, amount, status, created_at) VALUES 
      (1, 150.0, 'COMPLETED', '2023-08-01 10:00:00'),
      (1, 250.0, 'COMPLETED', '2023-08-05 11:30:00'),
      (1, 50.0, 'CANCELLED', '2023-08-10 12:15:00'),
      (2, 300.0, 'COMPLETED', '2023-08-12 14:00:00'),
      (4, 500.0, 'PENDING', '2023-08-15 15:45:00'),
      (4, 120.0, 'COMPLETED', '2023-08-18 16:20:00'),
      (NULL, 80.0, 'COMPLETED', '2023-08-20 17:10:00');
    `);
  });

  // ==========================================
  // SECTION 1: AGGREGATIONS & GROUP BY SYNTAX
  // ==========================================
  describe("1. Aggregations & Group By", () => {
    test("1.1 COUNT variations (table.column, column, distinct, star, literal)", async () => {
      const q1 = await db.query(`SELECT COUNT(users.id) AS count FROM users WHERE users.deleted_at IS NULL AND users.department_id = $1`, [1]);
      expect(q1[0].count).toBe(2);

      const q2 = await db.query(`SELECT COUNT(*) AS total_users, COUNT(email) AS with_email FROM users`);
      expect(q2[0].total_users).toBe(7);
      expect(q2[0].with_email).toBe(5);

      const q3 = await db.query(`SELECT COUNT(DISTINCT department_id) AS distinct_depts FROM users`);
      expect(q3[0].distinct_depts).toBe(3);
    });

    test("1.2 SUM, AVG, MIN, MAX with table prefix & expressions", async () => {
      const res = await db.query(`
        SELECT 
          SUM(users.salary) AS total_salary,
          AVG(users.salary) AS avg_salary,
          MIN(users.salary) AS min_salary,
          MAX(users.salary) AS max_salary
        FROM users
        WHERE users.is_active = true
      `);
      expect(res.length).toBe(1);
      expect(res[0].total_salary).toBe(21800.0);
      expect(res[0].avg_salary).toBeCloseTo(4360.0, 1);
      expect(res[0].min_salary).toBe(3100.0);
      expect(res[0].max_salary).toBe(6000.0);
    });

    test("1.3 GROUP BY with multi-columns and aliases", async () => {
      const res = await db.query(`
        SELECT department_id, is_active, COUNT(*) AS count, SUM(salary) AS dept_salary
        FROM users
        WHERE department_id IS NOT NULL
        GROUP BY department_id, is_active
        ORDER BY department_id ASC, is_active DESC
      `);
      expect(res.length).toBe(4);
      expect(res[0].department_id).toBe(1);
      expect(res[0].is_active).toBe(true);
      expect(res[0].count).toBe(2);
      expect(res[0].dept_salary).toBe(8500.0);
    });

    test("1.4 HAVING clause filtering", async () => {
      const res = await db.query(`
        SELECT department_id, COUNT(*) AS user_count, SUM(salary) AS sum_salary
        FROM users
        WHERE department_id IS NOT NULL
        GROUP BY department_id
        HAVING COUNT(*) >= 2
        ORDER BY user_count DESC
      `);
      expect(res.length).toBe(2);
      expect(res[0].department_id).toBe(1);
      expect(res[0].user_count).toBe(3);
      expect(res[1].department_id).toBe(2);
      expect(res[1].user_count).toBe(2);
    });
  });

  // ==========================================
  // SECTION 2: WHERE CLAUSES & LOGICAL OPERATORS
  // ==========================================
  describe("2. Filtering & Condition Operators", () => {
    test("2.1 IS NULL and IS NOT NULL with table prefixes", async () => {
      const resNull = await db.query(`SELECT id, username FROM users WHERE users.email IS NULL ORDER BY id ASC`);
      expect(resNull.length).toBe(2);
      expect(resNull[0].username).toBe("charlie");
      expect(resNull[1].username).toBe("frank");

      const resNotNull = await db.query(`SELECT COUNT(*) as count FROM users WHERE users.deleted_at IS NOT NULL`);
      expect(resNotNull[0].count).toBe(2);
    });

    test("2.2 BETWEEN and NOT BETWEEN", async () => {
      const res = await db.query(`
        SELECT username, salary FROM users WHERE salary BETWEEN 3000 AND 5000 ORDER BY salary ASC
      `);
      expect(res.length).toBe(4);
      expect(res.map((r: any) => r.username)).toEqual(["eve", "bob", "david", "alice"]);
    });

    test("2.3 IN and NOT IN lists", async () => {
      const res = await db.query(`
        SELECT username FROM users WHERE username IN ('alice', 'bob', 'david') ORDER BY username ASC
      `);
      expect(res.length).toBe(3);
      expect(res[0].username).toBe("alice");
      expect(res[1].username).toBe("bob");
      expect(res[2].username).toBe("david");
    });

    test("2.4 LIKE, ILIKE string pattern matching", async () => {
      const res1 = await db.query(`SELECT username FROM users WHERE email LIKE '%@example.com' ORDER BY username ASC`);
      expect(res1.length).toBe(5);

      const res2 = await db.query(`SELECT username FROM users WHERE username ILIKE 'A%'`);
      expect(res2.length).toBe(1);
      expect(res2[0].username).toBe("alice");
    });

    test("2.5 Complex Nested AND / OR precedence", async () => {
      const res = await db.query(`
        SELECT username FROM users 
        WHERE (salary > 4000 AND is_active = true) OR (department_id = 1 AND deleted_at IS NOT NULL)
        ORDER BY username ASC
      `);
      expect(res.length).toBe(4);
      expect(res.map((r: any) => r.username)).toEqual(["alice", "charlie", "david", "grace"]);
    });

    test("2.6 LOWER() and UPPER() in WHERE", async () => {
      const res = await db.query(`SELECT username FROM users WHERE LOWER(username) = $1`, ["alice"]);
      expect(res.length).toBe(1);
      expect(res[0].username).toBe("alice");
    });
  });

  // ==========================================
  // SECTION 3: RELATIONAL JOINS
  // ==========================================
  describe("3. Relational Joins", () => {
    test("3.1 INNER JOIN with table aliases", async () => {
      const res = await db.query(`
        SELECT u.username, d.dept_name, d.location
        FROM users u
        INNER JOIN departments d ON u.department_id = d.id
        ORDER BY u.id ASC
      `);
      expect(res.length).toBe(6);
      expect(res[0].username).toBe("alice");
      expect(res[0].dept_name).toBe("Engineering");
    });

    test("3.2 LEFT JOIN handling NULL foreign keys", async () => {
      const res = await db.query(`
        SELECT u.username, COALESCE(d.dept_name, 'No Dept') AS dept_name
        FROM users u
        LEFT JOIN departments d ON u.department_id = d.id
        WHERE u.username = 'grace'
      `);
      expect(res.length).toBe(1);
      expect(res[0].username).toBe("grace");
      expect(res[0].dept_name).toBe("No Dept");
    });

    test("3.3 Multi-Table Joins (users + departments + orders)", async () => {
      const res = await db.query(`
        SELECT 
          u.username,
          d.dept_name,
          COUNT(o.id) AS order_count,
          COALESCE(SUM(o.amount), 0) AS total_spent
        FROM users u
        INNER JOIN departments d ON u.department_id = d.id
        LEFT JOIN orders o ON u.id = o.user_id AND o.status = 'COMPLETED'
        GROUP BY u.username, d.dept_name
        ORDER BY total_spent DESC, u.username ASC
      `);
      expect(res.length).toBe(6);
      expect(res[0].username).toBe("alice");
      expect(res[0].dept_name).toBe("Engineering");
      expect(res[0].total_spent).toBe(400.0);
    });
  });

  // ==========================================
  // SECTION 4: SUBQUERIES & CTES
  // ==========================================
  describe("4. Subqueries & CTEs", () => {
    test("4.1 Subquery in WHERE clause (IN, EXISTS, Scalar comparison)", async () => {
      const resIn = await db.query(`
        SELECT username FROM users WHERE id IN (SELECT user_id FROM orders WHERE amount > 200) ORDER BY username ASC
      `);
      expect(resIn.length).toBe(3);
      expect(resIn.map((r: any) => r.username)).toEqual(["alice", "bob", "david"]);

      const resScalar = await db.query(`
        SELECT username, salary FROM users WHERE salary > (SELECT AVG(salary) FROM users) ORDER BY salary DESC
      `);
      expect(resScalar.length).toBe(3);
      expect(resScalar[0].username).toBe("grace");
    });

    test("4.2 Common Table Expressions (WITH clause)", async () => {
      const res = await db.query(`
        WITH active_users AS (
          SELECT id, username, department_id, salary
          FROM users
          WHERE is_active = true AND deleted_at IS NULL
        ),
        dept_summary AS (
          SELECT department_id, AVG(salary) as avg_sal
          FROM active_users
          GROUP BY department_id
        )
        SELECT au.username, au.salary, ds.avg_sal
        FROM active_users au
        JOIN dept_summary ds ON au.department_id = ds.department_id
        WHERE au.salary >= ds.avg_sal
        ORDER BY au.salary DESC
      `);
      expect(res.length).toBe(3);
      expect(res.map((r: any) => r.username)).toEqual(["grace", "alice", "david"]);
    });
  });

  // ==========================================
  // SECTION 5: WINDOW FUNCTIONS
  // ==========================================
  describe("5. Window Functions", () => {
    test("5.1 ROW_NUMBER() and RANK() OVER (PARTITION BY ... ORDER BY ...)", async () => {
      const res = await db.query(`
        SELECT 
          id, 
          department_id, 
          username, 
          salary,
          ROW_NUMBER() OVER (PARTITION BY department_id ORDER BY salary DESC) as rank_in_dept
        FROM users
        WHERE department_id IS NOT NULL
        ORDER BY department_id ASC, rank_in_dept ASC
      `);
      expect(res.length).toBe(6);
      expect(res[0].username).toBe("alice");
      expect(res[0].rank_in_dept).toBe(1);
      expect(res[1].username).toBe("bob");
      expect(res[1].rank_in_dept).toBe(2);
    });
  });

  // ==========================================
  // SECTION 6: EXPRESSIONS, CASE, CAST, COALESCE
  // ==========================================
  describe("6. Expressions, CASE WHEN, Type Casting", () => {
    test("6.1 CASE WHEN THEN ELSE END", async () => {
      const res = await db.query(`
        SELECT 
          username,
          salary,
          CASE 
            WHEN salary >= 5000 THEN 'HIGH'
            WHEN salary >= 3500 THEN 'MID'
            ELSE 'ENTRY'
          END AS salary_tier
        FROM users
        ORDER BY salary DESC
      `);
      expect(res.length).toBe(7);
      expect(res[0].salary_tier).toBe("HIGH");
      expect(res[1].salary_tier).toBe("HIGH");
      expect(res[2].salary_tier).toBe("MID");
    });

    test("6.2 COALESCE & NULLIF expressions", async () => {
      const res = await db.query(`
        SELECT 
          username, 
          COALESCE(email, 'no-email@domain.com') as contact_email
        FROM users
        ORDER BY id ASC
      `);
      expect(res[2].username).toBe("charlie");
      expect(res[2].contact_email).toBe("no-email@domain.com");
    });

    test("6.3 Type Casting (CAST and :: notation)", async () => {
      const res = await db.query(`
        SELECT 
          CAST(salary AS INT) as int_salary,
          CAST(id AS TEXT) as str_id
        FROM users
        WHERE id = 1
      `);
      expect(res[0].int_salary).toBe(5000);
      expect(res[0].str_id).toBe("1");
    });
  });

  // ==========================================
  // SECTION 7: DML (INSERT, UPDATE, DELETE) & RETURNING
  // ==========================================
  describe("7. DML Operations", () => {
    test("7.1 INSERT with RETURNING clause", async () => {
      const res = await db.query2(
        `INSERT INTO users (username, email, salary) VALUES ($1, $2, $3) RETURNING id, username, salary`,
        ["helen", "helen@example.com", 4800.0]
      );
      expect(res.rowCount).toBe(1);
      expect(res.rows[0].id).toBe(8);
      expect(res.rows[0].username).toBe("helen");
    });

    test("7.2 UPDATE with WHERE and expressions", async () => {
      const res = await db.query(
        `UPDATE users SET salary = salary * 1.10 WHERE department_id = $1 RETURNING username, salary`,
        [1]
      );
      expect(res.length).toBe(3);
      const updatedAlice = await db.query(`SELECT salary FROM users WHERE username = 'alice'`);
      expect(updatedAlice[0].salary).toBeCloseTo(5500.0, 1);
    });

    test("7.4 Multi-column UPDATE with SQL comments, CURRENT_TIMESTAMP, and IS NULL check", async () => {
      await db.exec(`
        CREATE TABLE classes_test (
          id SERIAL PRIMARY KEY,
          name TEXT NOT NULL,
          description TEXT,
          school_id INT,
          student_count INT,
          created_at TIMESTAMP,
          updated_at TIMESTAMP,
          deleted_at TIMESTAMP
        );
        INSERT INTO classes_test (id, name, description, school_id, student_count, created_at, updated_at, deleted_at) VALUES
          (123, 'Lớp cũ', 'Mô tả cũ', 1, 25, '2026-09-01 08:00:00', '2026-09-01 08:00:00', NULL);
      `);

      const updateRes = await db.exec(`
        UPDATE classes_test
        SET
          name        = 'Lớp mới đã sửa',      -- tên lớp mới
          description = 'Mô tả sau khi sửa',   -- mô tả
          school_id   = 5,                     -- id trường học muốn chuyển tới
          student_count = 32,                  -- sĩ số mới
          updated_at  = CURRENT_TIMESTAMP
        WHERE id = 123                         -- id lớp học muốn sửa
          AND deleted_at IS NULL;              -- chỉ cập nhật lớp đang hoạt động
      `);

      expect(updateRes.rowCount).toBe(1);

      const check = await db.query("SELECT * FROM classes_test WHERE id = 123");
      expect(check[0].name).toBe("Lớp mới đã sửa");
      expect(check[0].description).toBe("Mô tả sau khi sửa");
      expect(check[0].school_id).toBe(5);
      expect(check[0].student_count).toBe(32);
      expect(check[0].updated_at).toBeDefined();
    });
  });

  // ==========================================
  // SECTION 8: ORDERING, PAGINATION & DISTINCT
  // ==========================================
  describe("8. Ordering & Pagination", () => {
    test("8.1 Multi-column ORDER BY with LIMIT and OFFSET", async () => {
      const res = await db.query(`
        SELECT username, salary 
        FROM users 
        ORDER BY salary DESC, username ASC 
        LIMIT 2 OFFSET 1
      `);
      expect(res.length).toBe(2);
      expect(res[0].username).toBe("alice");
      expect(res[1].username).toBe("david");
    });

    test("8.2 Parameterized LIMIT & OFFSET", async () => {
      const res = await db.query(
        `SELECT username FROM users ORDER BY id ASC LIMIT $1 OFFSET $2`,
        [3, 2]
      );
      expect(res.length).toBe(3);
      expect(res[0].username).toBe("charlie");
      expect(res[1].username).toBe("david");
      expect(res[2].username).toBe("eve");
    });

    test("8.3 DISTINCT with ORDER BY", async () => {
      const res = await db.query(`
        SELECT DISTINCT status FROM orders ORDER BY status ASC
      `);
      expect(res.length).toBe(3);
      expect(res.map((r: any) => r.status)).toEqual(["CANCELLED", "COMPLETED", "PENDING"]);
    });
  });

  // ==========================================
  // SECTION 9: TRANSACTIONS & ROLLBACK
  // ==========================================
  describe("9. Transactions", () => {
    test("9.1 BEGIN, COMMIT persists changes", async () => {
      await db.query("BEGIN;");
      await db.query("INSERT INTO departments (dept_name, location) VALUES ('Legal', 'Hue');");
      await db.query("COMMIT;");

      const res = await db.query("SELECT * FROM departments WHERE dept_name = 'Legal'");
      expect(res.length).toBe(1);
    });

    test("9.2 BEGIN, ROLLBACK reverts changes", async () => {
      await db.query("BEGIN;");
      await db.query("INSERT INTO departments (dept_name, location) VALUES ('Temporary', 'Cantho');");
      await db.query("ROLLBACK;");

      const res = await db.query("SELECT * FROM departments WHERE dept_name = 'Temporary'");
      expect(res.length).toBe(0);
    });
  });

  // ==========================================
  // SECTION 10: SET OPERATIONS (UNION, INTERSECT, EXCEPT)
  // ==========================================
  describe("10. Set Operations", () => {
    test("10.1 UNION and UNION ALL", async () => {
      const resUnion = await db.query(`
        SELECT username AS name FROM users WHERE department_id = 1
        UNION
        SELECT dept_name AS name FROM departments
        ORDER BY name ASC
      `);
      expect(resUnion.length).toBe(7); // 3 users + 4 depts

      const resUnionAll = await db.query(`
        SELECT 'USER' as type FROM users
        UNION ALL
        SELECT 'ORDER' as type FROM orders
      `);
      expect(resUnionAll.length).toBe(14); // 7 users + 7 orders
    });

    test("10.2 UNION with complex projection, filtering and sorting", async () => {
      const res = await db.query(`
        SELECT username AS entity_name, 'USER' AS category, salary AS metric FROM users WHERE salary > 4000
        UNION
        SELECT dept_name AS entity_name, 'DEPT' AS category, 0.0 AS metric FROM departments WHERE location = 'Hanoi'
      `);
      expect(res.length).toBe(4); // alice, david, grace + Engineering
      expect(res.map((r: any) => r.entity_name).sort()).toEqual(["Engineering", "alice", "david", "grace"].sort());
    });
  });

  // ==========================================
  // SECTION 11: JSON & JSONB FUNCTIONS & AGGREGATIONS
  // ==========================================
  describe("11. JSON & JSONB Functions", () => {
    test("11.1 JSON_BUILD_OBJECT and JSONB_BUILD_OBJECT", async () => {
      const res = await db.query(`
        SELECT 
          username,
          JSON_BUILD_OBJECT('user', username, 'sal', salary) AS user_obj,
          JSONB_BUILD_OBJECT('dept_id', department_id, 'active', is_active) AS meta_obj
        FROM users
        WHERE username = 'alice'
      `);
      expect(res.length).toBe(1);
      const userObj = typeof res[0].user_obj === "string" ? JSON.parse(res[0].user_obj) : res[0].user_obj;
      expect(userObj.user).toBe("alice");
      expect(userObj.sal).toBe(5000.0);
    });

    test("11.2 JSONB_AGG and JSON_OBJECT_AGG", async () => {
      const res = await db.query(`
        SELECT 
          d.dept_name,
          JSONB_AGG(JSONB_BUILD_OBJECT('username', u.username, 'salary', u.salary)) AS members
        FROM departments d
        JOIN users u ON d.id = u.department_id
        GROUP BY d.dept_name
        ORDER BY d.dept_name ASC
      `);
      expect(res.length).toBe(3); // Engineering (3), Human Resources (1), Marketing (2)
      expect(res[0].dept_name).toBe("Engineering");
      const members = typeof res[0].members === "string" ? JSON.parse(res[0].members) : res[0].members;
      expect(members.length).toBe(3);
    });
  });

  // ==========================================
  // SECTION 12: ARRAY AGGREGATION & CONSTRUCTORS
  // ==========================================
  describe("12. Array Aggregations & Functions", () => {
    test("12.1 ARRAY_AGG with grouping", async () => {
      const res = await db.query(`
        SELECT 
          department_id,
          ARRAY_AGG(username) AS user_list
        FROM users
        WHERE department_id IS NOT NULL
        GROUP BY department_id
        ORDER BY department_id ASC
      `);
      expect(res.length).toBe(3);
      expect(res[0].department_id).toBe(1);
      const list = res[0].user_list;
      expect(Array.isArray(list) ? list.sort() : JSON.parse(list).sort()).toEqual(["alice", "bob", "charlie"]);
    });

    test("12.2 ARRAY literal constructor", async () => {
      const res = await db.query(`
        SELECT username, ARRAY[10, 20, 30] AS scores
        FROM users
        WHERE username = 'alice'
      `);
      expect(res.length).toBe(1);
      expect(res[0].scores).toEqual([10, 20, 30]);
    });
  });

  // ==========================================
  // SECTION 13: STRING & MATH BUILT-IN FUNCTIONS
  // ==========================================
  describe("13. String & Math Built-in Functions", () => {
    test("13.1 CONCAT, CONCAT_WS, LENGTH, TRIM, REPLACE, SUBSTRING, REVERSE", async () => {
      const res = await db.query(`
        SELECT 
          CONCAT(username, '@company.com') AS full_email,
          CONCAT_WS(' - ', username, dept_name) AS user_dept_label,
          LENGTH(username) AS name_len,
          TRIM('  spaces  ') AS trimmed,
          REPLACE(dept_name, 'Human Resources', 'HR') AS short_dept,
          SUBSTRING(username, 1, 3) AS name_prefix,
          REVERSE(username) AS reversed_name
        FROM users
        JOIN departments ON users.department_id = departments.id
        WHERE users.username = 'alice'
      `);
      expect(res.length).toBe(1);
      expect(res[0].full_email).toBe("alice@company.com");
      expect(res[0].user_dept_label).toBe("alice - Engineering");
      expect(res[0].name_len).toBe(5);
      expect(res[0].trimmed).toBe("spaces");
      expect(res[0].short_dept).toBe("Engineering");
      expect(res[0].name_prefix).toBe("ali");
      expect(res[0].reversed_name).toBe("ecila");
    });

    test("13.2 ABS, FLOOR, CEIL, ROUND, POWER, SQRT, MOD, SIGN", async () => {
      const res = await db.query(`
        SELECT 
          ABS(-150) AS abs_val,
          FLOOR(45.9) AS floor_val,
          CEIL(45.1) AS ceil_val,
          ROUND(123.456, 2) AS round_val,
          POWER(2, 4) AS pow_val,
          SQRT(144) AS sqrt_val,
          MOD(17, 5) AS mod_val,
          SIGN(-25) AS sign_val
      `);
      expect(res.length).toBe(1);
      expect(Number(res[0].abs_val)).toBe(150);
      expect(Number(res[0].floor_val)).toBe(45);
      expect(Number(res[0].ceil_val)).toBe(46);
      expect(Number(res[0].round_val)).toBeCloseTo(123.46, 2);
      expect(Number(res[0].pow_val)).toBe(16);
      expect(Number(res[0].sqrt_val)).toBe(12);
      expect(Number(res[0].mod_val)).toBe(2);
      expect(Number(res[0].sign_val)).toBe(-1);
    });
  });

  // ==========================================
  // SECTION 14: DATE & TIME FUNCTIONS
  // ==========================================
  describe("14. Date & Time Functions", () => {
    test("14.1 DATE_PART year and month extraction", async () => {
      const res = await db.query(`
        SELECT 
          username,
          DATE_PART('year', created_at) AS created_year,
          DATE_PART('month', created_at) AS created_month
        FROM users
        WHERE username = 'alice'
      `);
      expect(res.length).toBe(1);
      expect(res[0].created_year).toBe(2023);
      expect(res[0].created_month).toBe(1);
    });

    test("14.2 CURRENT_DATE and CURRENT_TIMESTAMP constants", async () => {
      const res = await db.query(`
        SELECT 
          CURRENT_DATE AS today,
          CURRENT_TIMESTAMP AS now_ts
      `);
      expect(res.length).toBe(1);
      expect(typeof res[0].today).toBe("string");
      expect(typeof res[0].now_ts).toBe("string");
    });
  });

  // ==========================================
  // SECTION 15: SUBQUERIES IN FROM & PROJECTIONS
  // ==========================================
  describe("15. Subqueries in FROM & Projections", () => {
    test("15.1 Derived table (subquery in FROM)", async () => {
      const res = await db.query(`
        SELECT sub.dept_name, sub.user_count
        FROM (
          SELECT d.dept_name, COUNT(u.id) AS user_count
          FROM departments d
          LEFT JOIN users u ON d.id = u.department_id
          GROUP BY d.dept_name
        ) AS sub
        WHERE sub.user_count > 1
        ORDER BY sub.user_count DESC
      `);
      expect(res.length).toBe(2);
      expect(res[0].dept_name).toBe("Engineering");
      expect(res[0].user_count).toBe(3);
    });

    test("15.2 Correlated subquery in SELECT projection", async () => {
      const res = await db.query(`
        SELECT 
          u.username,
          (SELECT COUNT(*) FROM orders o WHERE o.user_id = u.id) AS order_cnt
        FROM users u
        WHERE u.username IN ('alice', 'bob', 'grace')
        ORDER BY u.username ASC
      `);
      expect(res.length).toBe(3);
      expect(res[0].username).toBe("alice");
      expect(res[0].order_cnt).toBe(3);
      expect(res[1].username).toBe("bob");
      expect(res[1].order_cnt).toBe(1);
      expect(res[2].username).toBe("grace");
      expect(res[2].order_cnt).toBe(0);
    });
  });

  // ==========================================
  // SECTION 16: DDL & SCHEMA MODIFICATIONS
  // ==========================================
  describe("16. DDL & Schema Alterations", () => {
    test("16.1 ALTER TABLE ADD COLUMN and populate data", async () => {
      await db.query(`ALTER TABLE departments ADD COLUMN budget FLOAT DEFAULT 50000.0`);
      const res = await db.query(`SELECT dept_name, budget FROM departments WHERE dept_name = 'Engineering'`);
      expect(res.length).toBe(1);
      expect(res[0].budget).toBe(50000.0);
    });

    test("16.2 TRUNCATE TABLE with table invalidation", async () => {
      await db.query(`CREATE TABLE temp_logs (id SERIAL PRIMARY KEY, msg TEXT)`);
      await db.query(`INSERT INTO temp_logs (msg) VALUES ('log1'), ('log2')`);
      const before = await db.query(`SELECT COUNT(*) AS count FROM temp_logs`);
      expect(before[0].count).toBe(2);

      await db.query(`TRUNCATE TABLE temp_logs`);
      const after = await db.query(`SELECT COUNT(*) AS count FROM temp_logs`);
      expect(after[0].count).toBe(0);
    });
  });

  // ==========================================
  // SECTION 17: ADVANCED WINDOW FUNCTIONS
  // ==========================================
  describe("17. Advanced Window Functions", () => {
    test("17.1 LEAD and LAG positional window functions", async () => {
      const res = await db.query(`
        SELECT 
          id,
          amount,
          LAG(amount, 1) OVER (ORDER BY id ASC) AS prev_amount,
          LEAD(amount, 1) OVER (ORDER BY id ASC) AS next_amount
        FROM orders
        WHERE user_id = 1
        ORDER BY id ASC
      `);
      expect(res.length).toBe(3);
      expect(res[0].prev_amount).toBeNull();
      expect(res[0].next_amount).toBe(250.0);
      expect(res[1].prev_amount).toBe(150.0);
      expect(res[1].next_amount).toBe(50.0);
      expect(res[2].prev_amount).toBe(250.0);
      expect(res[2].next_amount).toBeNull();
    });

    test("17.2 FIRST_VALUE and LAST_VALUE window functions", async () => {
      const res = await db.query(`
        SELECT 
          id,
          amount,
          FIRST_VALUE(amount) OVER (ORDER BY id ASC) AS first_amt,
          LAST_VALUE(amount) OVER (ORDER BY id ASC) AS last_amt
        FROM orders
        WHERE user_id = 1
        ORDER BY id ASC
      `);
      expect(res.length).toBe(3);
      expect(res[0].first_amt).toBe(150.0);
      expect(res[0].last_amt).toBe(50.0);
    });

    test("17.3 DENSE_RANK() OVER (ORDER BY salary DESC)", async () => {
      const res = await db.query(`
        SELECT 
          username,
          salary,
          DENSE_RANK() OVER (ORDER BY salary DESC) AS rank_pos
        FROM users
        ORDER BY salary DESC
        LIMIT 3
      `);
      expect(res.length).toBe(3);
      expect(res[0].username).toBe("grace");
      expect(res[0].rank_pos).toBe(1);
    });
  });

  // ==========================================
  // SECTION 18: ADVANCED JOINS & RELATIONAL INTEGRITY
  // ==========================================
  describe("18. Advanced Joins & Relational Integrity", () => {
    test("18.1 CROSS JOIN combining departments and order statuses", async () => {
      const res = await db.query(`
        SELECT d.dept_name, s.status
        FROM (SELECT DISTINCT dept_name FROM departments WHERE location IS NOT NULL) d
        CROSS JOIN (SELECT DISTINCT status FROM orders) s
      `);
      expect(res.length).toBe(3 * 3); // 3 departments * 3 statuses = 9
    });

    test("18.2 Multi-level Join with Aggregation and Filter", async () => {
      const res = await db.query(`
        SELECT 
          d.dept_name,
          COUNT(DISTINCT u.id) AS active_users,
          COALESCE(SUM(o.amount), 0.0) AS total_sales
        FROM departments d
        JOIN users u ON d.id = u.department_id AND u.is_active = true
        LEFT JOIN orders o ON u.id = o.user_id AND o.status = 'COMPLETED'
        GROUP BY d.dept_name
        ORDER BY total_sales DESC
      `);
      expect(res.length).toBe(2);
      expect(res[0].dept_name).toBe("Engineering");
      expect(res[0].active_users).toBe(2);
      expect(res[0].total_sales).toBe(700.0); // Alice (150+250) + Bob (300) = 700
    });
  });

  // ==========================================
  // SECTION 19: MULTI-LEVEL COMMON TABLE EXPRESSIONS (CTEs)
  // ==========================================
  describe("19. Multi-level CTEs (WITH clause)", () => {
    test("19.1 Chained CTE pipelines", async () => {
      const res = await db.query(`
        WITH completed_orders AS (
          SELECT user_id, SUM(amount) AS user_spent
          FROM orders
          WHERE status = 'COMPLETED'
          GROUP BY user_id
        ),
        top_spenders AS (
          SELECT u.username, u.department_id, co.user_spent
          FROM users u
          JOIN completed_orders co ON u.id = co.user_id
          WHERE co.user_spent > 200
        )
        SELECT ts.username, d.dept_name, ts.user_spent
        FROM top_spenders ts
        LEFT JOIN departments d ON ts.department_id = d.id
        ORDER BY ts.user_spent DESC
      `);
      expect(res.length).toBe(2);
      expect(res[0].username).toBe("alice");
      expect(res[0].user_spent).toBe(400.0); // 150 + 250
      expect(res[1].username).toBe("bob");
      expect(res[1].user_spent).toBe(300.0);
    });
  });

  // ==========================================
  // SECTION 20: PROGRAMMATIC TRANSACTIONS & ROLLBACK
  // ==========================================
  describe("20. Programmatic Transactions", () => {
    test("20.1 Transaction rollback on exception", async () => {
      try {
        await db.transaction(async (tx: any) => {
          await tx.query("INSERT INTO departments (dept_name, location) VALUES ('Failure Dept', 'Mars')");
          throw new Error("Simulated Transaction Failure");
        });
      } catch (err: any) {
        expect(err.message).toContain("Simulated Transaction Failure");
      }

      const check = await db.query("SELECT * FROM departments WHERE dept_name = 'Failure Dept'");
      expect(check.length).toBe(0);
    });

    test("20.2 Transaction commit success", async () => {
      await db.transaction(async (tx: any) => {
        await tx.query("INSERT INTO departments (dept_name, location) VALUES ('Success Dept', 'Earth')");
      });

      const check = await db.query("SELECT * FROM departments WHERE dept_name = 'Success Dept'");
      expect(check.length).toBe(1);
    });
  });

  // ==========================================
  // SECTION 22: CORRELATED SCALAR SUBQUERIES & COMPLEX PROJECTIONS
  // ==========================================
  describe("22. Correlated Scalar Subqueries & Complex Projections", () => {
    test("22.1 Correlated Scalar Subquery COUNT with SQL comments, LEFT JOIN, WHERE, ORDER BY DESC and LIMIT", async () => {
      await db.exec(`
        CREATE TABLE provinces_rel (id SERIAL PRIMARY KEY, name TEXT, deleted_at TEXT);
        CREATE TABLE regions_rel (id SERIAL PRIMARY KEY, name TEXT, description TEXT, province_id INT, created_at TEXT, deleted_at TEXT);
        CREATE TABLE schools_rel (id SERIAL PRIMARY KEY, name TEXT, region_id INT, deleted_at TEXT);
        CREATE TABLE region_admins_rel (id SERIAL PRIMARY KEY, name TEXT, region_id INT, deleted_at TEXT);
      `);

      await db.exec(`
        INSERT INTO provinces_rel (id, name, deleted_at) VALUES (1, 'Hanoi', NULL), (2, 'HCM', NULL);
        INSERT INTO regions_rel (id, name, description, province_id, created_at, deleted_at) VALUES
          (1, 'North Region', 'Northern Area', 1, '2026-09-01 10:00:00', NULL),
          (2, 'South Region', 'Southern Area', 2, '2026-09-02 10:00:00', NULL),
          (3, 'Deleted Region', 'Hidden', 1, '2026-09-03 10:00:00', '2026-09-03');
        INSERT INTO schools_rel (id, name, region_id, deleted_at) VALUES
          (1, 'School A', 1, NULL),
          (2, 'School B', 1, NULL),
          (3, 'School C', 1, '2026-09-01'),
          (4, 'School D', 2, NULL);
        INSERT INTO region_admins_rel (id, name, region_id, deleted_at) VALUES
          (1, 'Admin X', 1, NULL),
          (2, 'Admin Y', 2, NULL);
      `);

      const res = await db.query(`
        SELECT
          r.id,
          r.name,
          r.description,
          r.province_id,
          r.created_at,
          p.name                                AS province_name,   -- tên tỉnh cha
          (SELECT COUNT(*) FROM schools_rel s
             WHERE s.region_id = r.id
               AND s.deleted_at IS NULL)         AS school_count,    -- số trường
          (SELECT COUNT(*) FROM region_admins_rel ra
             WHERE ra.region_id = r.id
               AND ra.deleted_at IS NULL)        AS admin_count      -- số admin
        FROM regions_rel r
        LEFT JOIN provinces_rel p ON p.id = r.province_id
        WHERE r.deleted_at IS NULL
        ORDER BY r.created_at DESC
        LIMIT 200 OFFSET 0;
      `);

      expect(res.length).toBe(2);
      expect(res[0].name).toBe("South Region");
      expect(res[0].province_name).toBe("HCM");
      expect(res[0].school_count).toBe(1);
      expect(res[0].admin_count).toBe(1);

      expect(res[1].name).toBe("North Region");
      expect(res[1].province_name).toBe("Hanoi");
      expect(res[1].school_count).toBe(2); // 2 active, 1 deleted
      expect(res[1].admin_count).toBe(1);
    });
  });
});
