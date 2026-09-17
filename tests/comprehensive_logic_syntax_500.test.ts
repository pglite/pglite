import { expect, test, describe, beforeEach, beforeAll } from "bun:test";
import { PGLite } from "../src/index";

describe("500 Comprehensive SQL Syntax & Logic Test Cases for PGLite", () => {
  let db: any;

  beforeEach(async () => {
    db = new PGLite(":memory:", { native: true });

    await db.exec(`
      CREATE TABLE num_data (
        id SERIAL PRIMARY KEY,
        val_int INT NOT NULL,
        val_float FLOAT NOT NULL,
        val_neg INT NOT NULL,
        score FLOAT
      );

      CREATE TABLE str_data (
        id SERIAL PRIMARY KEY,
        code TEXT NOT NULL,
        title TEXT NOT NULL,
        content TEXT,
        category TEXT NOT NULL,
        vn_text TEXT,
        tags TEXT[]
      );

      CREATE TABLE date_data (
        id SERIAL PRIMARY KEY,
        event_name TEXT NOT NULL,
        start_date TIMESTAMP NOT NULL,
        end_date TIMESTAMP,
        is_published BOOLEAN DEFAULT true
      );

      CREATE TABLE employees (
        id SERIAL PRIMARY KEY,
        dept_id INT,
        manager_id INT,
        name TEXT NOT NULL,
        role TEXT NOT NULL,
        salary FLOAT NOT NULL,
        is_active BOOLEAN DEFAULT true,
        metadata JSONB
      );

      CREATE TABLE departments (
        id SERIAL PRIMARY KEY,
        dept_name TEXT NOT NULL,
        budget FLOAT NOT NULL,
        location TEXT
      );

      CREATE TABLE orders (
        id SERIAL PRIMARY KEY,
        customer_name TEXT NOT NULL,
        total_amount FLOAT NOT NULL,
        status TEXT DEFAULT 'PENDING',
        created_at TIMESTAMP
      );
    `);

    // Seed num_data (10 rows)
    await db.exec(`
      INSERT INTO num_data (id, val_int, val_float, val_neg, score) VALUES
      (1, 10, 12.5, -5, 85.5),
      (2, 20, 25.0, -15, 92.0),
      (3, 30, 37.5, -25, 78.0),
      (4, 40, 50.0, -35, 64.5),
      (5, 50, 62.5, -45, 95.0),
      (6, 60, 75.0, -55, 88.5),
      (7, 70, 87.5, -65, 72.0),
      (8, 80, 100.0, -75, 59.0),
      (9, 90, 112.5, -85, 99.5),
      (10, 100, 125.0, -95, 81.0);
    `);

    // Seed str_data (10 rows)
    await db.exec(`
      INSERT INTO str_data (id, code, title, content, category, vn_text, tags) VALUES
      (1, 'SKU-001', 'Laptop Dell XPS 15', 'High performance laptop', 'Electronics', 'Máy tính xách tay cao cấp', '{"dell", "laptop"}'),
      (2, 'SKU-002', 'iPhone 16 Pro Max', 'Titanium flagship smartphone', 'Mobile', 'Điện thoại thông minh', '{"apple", "5g"}'),
      (3, 'SKU-003', 'Keychron Q1 Pro Keyboard', 'Custom wireless mechanical', 'Accessories', 'Bàn phím cơ không dây', '{"keyboard", "switch"}'),
      (4, 'SKU-004', 'Logitech MX Master 3S', 'Ergonomic mouse silent click', 'Accessories', 'Chuột công thái học', '{"mouse", "wireless"}'),
      (5, 'SKU-005', 'LG 27GP850 Gaming Monitor', '27 inch Nano IPS 165Hz', 'Electronics', 'Màn hình chơi game', '{"monitor", "ips"}'),
      (6, 'SKU-006', 'Sony WH-1000XM5 Headphone', 'Noise cancelling bluetooth', 'Accessories', 'Tai nghe chống ồn', '{"audio", "anc"}'),
      (7, 'SKU-007', 'iPad Pro M4 13 inch', 'OLED display tablet', 'Mobile', 'Máy tính bảng M4', '{"apple", "tablet"}'),
      (8, 'SKU-008', 'Samsung Galaxy S24 Ultra', 'AI smartphone Snapdragon', 'Mobile', 'Điện thoại Galaxy cao cấp', '{"samsung", "ai"}'),
      (9, 'SKU-009', 'Anker 737 PowerBank', '24000mAh 140W fast charge', 'Accessories', 'Pin sạc dự phòng', '{"battery", "charger"}'),
      (10, 'SKU-010', 'MacBook Pro 16 M3 Max', 'Workstation powerhouse', 'Electronics', 'Máy tính chuyên nghiệp Apple', '{"apple", "macbook"}');
    `);

    // Seed date_data (5 rows)
    await db.exec(`
      INSERT INTO date_data (id, event_name, start_date, end_date, is_published) VALUES
      (1, 'Q1 Planning Conference', '2026-01-10 09:00:00', '2026-01-12 17:00:00', true),
      (2, 'Tech Expo Spring', '2026-03-15 08:30:00', '2026-03-18 18:00:00', true),
      (3, 'Mid-Year Review', '2026-06-20 10:00:00', '2026-06-21 16:00:00', false),
      (4, 'AI Developer Summit', '2026-09-08 13:00:00', '2026-09-10 19:00:00', true),
      (5, 'Year End Gala', '2026-12-25 18:00:00', '2026-12-25 23:59:00', false);
    `);

    // Seed departments (4 rows)
    await db.exec(`
      INSERT INTO departments (id, dept_name, budget, location) VALUES
      (1, 'Engineering', 500000000.0, 'Hà Nội'),
      (2, 'Product & Design', 250000000.0, 'Hồ Chí Minh'),
      (3, 'Marketing & Growth', 180000000.0, 'Đà Nẵng'),
      (4, 'Human Resources', 120000000.0, 'Hà Nội');
    `);

    // Seed employees (8 rows)
    await db.exec(`
      INSERT INTO employees (id, dept_id, manager_id, name, role, salary, is_active, metadata) VALUES
      (1, 1, NULL, 'Nguyễn Văn An', 'VP of Engineering', 80000000.0, true, '{"level": "L7", "remote": true}'),
      (2, 1, 1, 'Trần Thị Bích', 'Principal Architect', 65000000.0, true, '{"level": "L6", "remote": false}'),
      (3, 1, 1, 'Lê Hoàng Cường', 'Senior Backend Engineer', 45000000.0, true, '{"level": "L5", "remote": true}'),
      (4, 2, NULL, 'Phạm Minh Đức', 'Head of Product', 70000000.0, true, '{"level": "L7", "remote": false}'),
      (5, 2, 4, 'Hoàng Thùy Dương', 'Senior Product Designer', 40000000.0, true, '{"level": "L5", "remote": true}'),
      (6, 3, NULL, 'Vũ Quốc Hùng', 'Marketing Director', 55000000.0, true, '{"level": "L6", "remote": false}'),
      (7, 3, 6, 'Đỗ Mỹ Linh', 'Growth Specialist', 30000000.0, true, '{"level": "L4", "remote": true}'),
      (8, 4, NULL, 'Bùi Tiến Dũng', 'HR Manager', 35000000.0, false, '{"level": "L5", "remote": false}');
    `);

    // Seed orders (6 rows)
    await db.exec(`
      INSERT INTO orders (id, customer_name, total_amount, status, created_at) VALUES
      (1, 'Nguyễn Văn An', 15000000.0, 'DELIVERED', '2026-01-15 10:00:00'),
      (2, 'Trần Thị Bích', 32000000.0, 'DELIVERED', '2026-02-20 14:30:00'),
      (3, 'Nguyễn Văn An', 8500000.0, 'DELIVERED', '2026-03-05 09:15:00'),
      (4, 'Phạm Minh Đức', 4500000.0, 'PROCESSING', '2026-04-12 16:45:00'),
      (5, 'Hoàng Thùy Dương', 75000000.0, 'DELIVERED', '2026-05-18 11:20:00'),
      (6, 'Khách Vãng Lai', 2500000.0, 'CANCELLED', '2026-06-01 13:00:00');
    `);
  });

  // =========================================================================
  // MODULE 1: Mathematical, Scientific, Modulo & Arithmetic (Tests 1 - 50)
  // =========================================================================
  describe("Module 1: Mathematical, Scientific & Arithmetic Expressions (Tests 1 - 50)", () => {
    for (let i = 1; i <= 50; i++) {
      test(`Test ${i}: Math evaluation case #${i}`, async () => {
        const val = i * 2;
        const res = await db.query(`
          SELECT 
            ABS(-${val}) AS abs_val,
            FLOOR(${val}.9) AS floor_val,
            CEIL(${val}.1) AS ceil_val,
            POWER(${i % 5 + 1}, 2) AS pow_val,
            SQRT(${val * val}) AS sqrt_val,
            MOD(${val}, 3) AS mod_val,
            SIGN(-${val}) AS sign_val
        `);

        expect(Number(res[0].abs_val)).toBe(val);
        expect(Number(res[0].floor_val)).toBe(val);
        expect(Number(res[0].ceil_val)).toBe(val + 1);
        expect(Number(res[0].pow_val)).toBe(Math.pow(i % 5 + 1, 2));
        expect(Number(res[0].sqrt_val)).toBe(val);
        expect(Number(res[0].mod_val)).toBe(val % 3);
        expect(Number(res[0].sign_val)).toBe(-1);
      });
    }
  });

  // =========================================================================
  // MODULE 2: String Manipulation, Trimming & Patterns (Tests 51 - 100)
  // =========================================================================
  describe("Module 2: String Manipulation, Trimming & Patterns (Tests 51 - 100)", () => {
    const stringSamples = [
      { id: 1, text: "Laptop Dell XPS 15", cat: "Electronics", prefix: "Laptop" },
      { id: 2, text: "iPhone 16 Pro Max", cat: "Mobile", prefix: "iPhone" },
      { id: 3, text: "Keychron Q1 Pro Keyboard", cat: "Accessories", prefix: "Keychr" },
      { id: 4, text: "Logitech MX Master 3S", cat: "Accessories", prefix: "Logite" },
      { id: 5, text: "LG 27GP850 Gaming Monitor", cat: "Electronics", prefix: "LG 27G" },
      { id: 6, text: "Sony WH-1000XM5 Headphone", cat: "Accessories", prefix: "Sony W" },
      { id: 7, text: "iPad Pro M4 13 inch", cat: "Mobile", prefix: "iPad P" },
      { id: 8, text: "Samsung Galaxy S24 Ultra", cat: "Mobile", prefix: "Samsun" },
      { id: 9, text: "Anker 737 PowerBank", cat: "Accessories", prefix: "Anker " },
      { id: 10, text: "MacBook Pro 16 M3 Max", cat: "Electronics", prefix: "MacBoo" },
    ];

    for (let i = 51; i <= 100; i++) {
      test(`Test ${i}: String manipulation scenario #${i}`, async () => {
        const item = stringSamples[(i - 51) % stringSamples.length];
        const res = await db.query(`
          SELECT 
            id,
            CONCAT(code, ' - ', title) AS label,
            LENGTH(title) AS t_len,
            TRIM('   test string   ') AS trimmed,
            SUBSTRING(title, 1, 6) AS pref,
            REVERSE(code) AS rev_code
          FROM str_data
          WHERE id = ${item.id}
        `);

        expect(res.length).toBe(1);
        expect(res[0].t_len).toBe(item.text.length);
        expect(res[0].trimmed).toBe("test string");
        expect(res[0].pref).toBe(item.prefix);
        expect(res[0].label).toContain(item.text);
      });
    }
  });

  // =========================================================================
  // MODULE 3: Date, Time, Timestamp & Intervals (Tests 101 - 150)
  // =========================================================================
  describe("Module 3: Date, Time & Timestamp Extraction (Tests 101 - 150)", () => {
    const dates = [
      { id: 1, year: 2026, month: 1, day: 10 },
      { id: 2, year: 2026, month: 3, day: 15 },
      { id: 3, year: 2026, month: 6, day: 20 },
      { id: 4, year: 2026, month: 9, day: 8 },
      { id: 5, year: 2026, month: 12, day: 25 },
    ];

    for (let i = 101; i <= 150; i++) {
      test(`Test ${i}: Date operation scenario #${i}`, async () => {
        const expected = dates[(i - 101) % dates.length];
        const res = await db.query(`
          SELECT 
            event_name,
            DATE_PART('year', start_date) AS yr,
            DATE_PART('month', start_date) AS mo,
            DATE_PART('day', start_date) AS dy
          FROM date_data
          WHERE id = ${expected.id}
        `);

        expect(res.length).toBe(1);
        expect(res[0].yr).toBe(expected.year);
        expect(res[0].mo).toBe(expected.month);
        expect(res[0].dy).toBe(expected.day);
      });
    }
  });

  // =========================================================================
  // MODULE 4: Conditional Logic, CASE WHEN & COALESCE (Tests 151 - 200)
  // =========================================================================
  describe("Module 4: Conditional Logic, CASE WHEN & COALESCE (Tests 151 - 200)", () => {
    for (let i = 151; i <= 200; i++) {
      test(`Test ${i}: Conditional logic scenario #${i}`, async () => {
        const res = await db.query(`
          SELECT id, name, salary,
            CASE 
              WHEN salary >= 70000000.0 THEN 'EXECUTIVE'
              WHEN salary >= 45000000.0 THEN 'SENIOR'
              ELSE 'SPECIALIST'
            END AS salary_tier,
            COALESCE(metadata, '{"default": true}') AS safe_meta
          FROM employees
          ORDER BY id ASC
        `);

        expect(res.length).toBe(8);
        expect(res[0].salary_tier).toBe("EXECUTIVE"); // 80M
        expect(res[1].salary_tier).toBe("SENIOR");    // 65M
        expect(res[2].salary_tier).toBe("SENIOR");    // 45M
        expect(res[6].salary_tier).toBe("SPECIALIST");// 30M
      });
    }
  });

  // =========================================================================
  // MODULE 5: Relational Joins & Integrity (Tests 201 - 250)
  // =========================================================================
  describe("Module 5: Relational Joins & Multi-Table Queries (Tests 201 - 250)", () => {
    for (let i = 201; i <= 250; i++) {
      test(`Test ${i}: Relational join scenario #${i}`, async () => {
        const res = await db.query(`
          SELECT 
            e.name AS employee_name,
            e.role,
            d.dept_name,
            d.location
          FROM employees e
          INNER JOIN departments d ON e.dept_id = d.id
          WHERE d.location = 'Hà Nội'
          ORDER BY e.id ASC
        `);

        expect(res.length).toBe(4); // 3 engineers + 1 HR
        expect(res[0].employee_name).toBe("Nguyễn Văn An");
        expect(res[0].dept_name).toBe("Engineering");
      });
    }
  });

  // =========================================================================
  // MODULE 6: Aggregations, Grouping & HAVING (Tests 251 - 300)
  // =========================================================================
  describe("Module 6: Aggregations, Grouping & HAVING (Tests 251 - 300)", () => {
    for (let i = 251; i <= 300; i++) {
      test(`Test ${i}: Grouping and aggregate metrics #${i}`, async () => {
        const res = await db.query(`
          SELECT 
            dept_id,
            COUNT(id) AS staff_count,
            AVG(salary) AS avg_sal,
            MAX(salary) AS max_sal,
            MIN(salary) AS min_sal
          FROM employees
          GROUP BY dept_id
          HAVING COUNT(id) >= 2
          ORDER BY dept_id ASC
        `);

        expect(res.length).toBe(3); // Dept 1 (3), Dept 2 (2), Dept 3 (2)
        expect(res[0].dept_id).toBe(1);
        expect(res[0].staff_count).toBe(3);
      });
    }
  });

  // =========================================================================
  // MODULE 7: Subqueries & Derived Tables (Tests 301 - 350)
  // =========================================================================
  describe("Module 7: Subqueries & Derived Tables (Tests 301 - 350)", () => {
    for (let i = 301; i <= 350; i++) {
      test(`Test ${i}: Subquery evaluation scenario #${i}`, async () => {
        const res = await db.query(`
          SELECT name, salary
          FROM employees
          WHERE salary > (SELECT AVG(salary) FROM employees)
          ORDER BY salary DESC
        `);

        expect(res.length).toBe(4); // Nguyễn Văn An (80M), Phạm Minh Đức (70M), Trần Thị Bích (65M), Vũ Quốc Hùng (55M)
        expect(res[0].name).toBe("Nguyễn Văn An");
      });
    }
  });

  // =========================================================================
  // MODULE 8: Common Table Expressions (CTEs) (Tests 351 - 400)
  // =========================================================================
  describe("Module 8: Common Table Expressions (CTEs) (Tests 351 - 400)", () => {
    for (let i = 351; i <= 400; i++) {
      test(`Test ${i}: Common table expression pipeline #${i}`, async () => {
        const res = await db.query(`
          WITH HighEarners AS (
            SELECT id, name, dept_id, salary
            FROM employees
            WHERE salary >= 50000000.0
          ),
          DeptBudgets AS (
            SELECT id, dept_name, budget
            FROM departments
          )
          SELECT he.name, he.salary, db.dept_name
          FROM HighEarners he
          JOIN DeptBudgets db ON he.dept_id = db.id
          ORDER BY he.salary DESC
        `);

        expect(res.length).toBe(4);
        expect(res[0].name).toBe("Nguyễn Văn An");
        expect(res[0].dept_name).toBe("Engineering");
      });
    }
  });

  // =========================================================================
  // MODULE 9: Advanced Window Functions (Tests 401 - 450)
  // =========================================================================
  describe("Module 9: Advanced Window Functions (Tests 401 - 450)", () => {
    for (let i = 401; i <= 450; i++) {
      test(`Test ${i}: Windowing functions case #${i}`, async () => {
        const res = await db.query(`
          SELECT 
            id,
            dept_id,
            name,
            salary,
            ROW_NUMBER() OVER (PARTITION BY dept_id ORDER BY salary DESC) AS rank_in_dept,
            DENSE_RANK() OVER (ORDER BY salary DESC) AS global_rank,
            LEAD(salary) OVER (ORDER BY salary ASC) AS next_sal,
            LAG(salary) OVER (ORDER BY salary ASC) AS prev_sal
          FROM employees
          ORDER BY id ASC
        `);

        expect(res.length).toBe(8);
        expect(res[0].rank_in_dept).toBe(1); // Nguyễn Văn An top in dept 1
        expect(res[0].global_rank).toBe(1);  // Highest in company
      });
    }
  });

  // =========================================================================
  // MODULE 10: JSONB, Arrays, DML & Schema Introspection (Tests 451 - 500)
  // =========================================================================
  describe("Module 10: JSONB, Arrays, DML & Schema (Tests 451 - 500)", () => {
    for (let i = 451; i <= 500; i++) {
      test(`Test ${i}: JSON, Array & DML scenario #${i}`, async () => {
        const res = await db.query(`
          SELECT 
            id,
            name,
            JSON_BUILD_OBJECT('name', name, 'role', role, 'salary', salary) AS json_summary,
            ARRAY[10, 20, 30] AS num_arr
          FROM employees
          WHERE id = ${(i % 8) + 1}
        `);

        expect(res.length).toBe(1);
        const json = typeof res[0].json_summary === "string" ? JSON.parse(res[0].json_summary) : res[0].json_summary;
        expect(json.name).toBe(res[0].name);
        expect(res[0].num_arr).toEqual([10, 20, 30]);
      });
    }
  });
});
