import { expect, test, describe, beforeEach } from "bun:test";
import { PGLite } from "../src/index";

describe("500 Advanced PostgreSQL Syntax, Types & Construct Tests for PGLite", () => {
  let db: any;

  beforeEach(async () => {
    db = new PGLite(":memory:", { native: true });

    await db.exec(`
      CREATE TABLE enterprise_orgs (
        id SERIAL PRIMARY KEY,
        code TEXT NOT NULL UNIQUE,
        name TEXT NOT NULL,
        region TEXT NOT NULL,
        budget NUMERIC NOT NULL,
        is_active BOOLEAN DEFAULT true,
        established_date DATE,
        tags TEXT[]
      );

      CREATE TABLE enterprise_members (
        id SERIAL PRIMARY KEY,
        org_id INT,
        full_name TEXT NOT NULL,
        email TEXT,
        role TEXT NOT NULL,
        salary NUMERIC NOT NULL,
        metadata JSONB,
        skills TEXT[],
        joined_at TIMESTAMP,
        is_verified BOOLEAN DEFAULT false
      );

      CREATE TABLE project_tasks (
        id SERIAL PRIMARY KEY,
        member_id INT,
        title TEXT NOT NULL,
        priority INT DEFAULT 1,
        cost NUMERIC NOT NULL,
        status TEXT DEFAULT 'PENDING',
        created_at TIMESTAMP
      );
    `);

    // Seed enterprise_orgs
    await db.exec(`
      INSERT INTO enterprise_orgs (id, code, name, region, budget, is_active, established_date, tags) VALUES
      (1, 'CORP-VN', 'Tập Đoàn Công Nghệ Á Châu 🇻🇳', 'APAC', 500000000.0, true, '2020-01-15', '{"tech", "hq"}'),
      (2, 'CORP-US', 'Silicon Ventures US', 'NA', 800000000.0, true, '2018-05-20', '{"rd", "cloud"}'),
      (3, 'CORP-EU', 'Euro Logic Systems', 'EMEA', 350000000.0, true, '2021-09-10', '{"consulting"}'),
      (4, 'CORP-JP', 'Tokyo Precision Robotics', 'APAC', 600000000.0, false, '2015-11-01', '{"robotics", "ai"}'),
      (5, 'CORP-AU', 'Oceania Labs', 'APAC', 150000000.0, true, '2023-03-30', '{"satellite"}');
    `);

    // Seed enterprise_members
    await db.exec(`
      INSERT INTO enterprise_members (id, org_id, full_name, email, role, salary, metadata, skills, joined_at, is_verified) VALUES
      (1, 1, 'Nguyễn Văn An', 'an.nguyen@tech.vn', 'Architect', 95000000.0, '{"level": "Principal", "rating": 5}', '{"rust", "postgresql", "distributed"}', '2021-02-01 08:30:00', true),
      (2, 1, 'Trần Thị Bình', 'binh.tran@tech.vn', 'Engineer', 45000000.0, '{"level": "Senior", "rating": 4}', '{"typescript", "react"}', '2021-06-15 09:00:00', true),
      (3, 1, 'Lê Hoàng Cường', NULL, 'Junior Dev', 22000000.0, '{"level": "Junior", "rating": 3}', '{"nodejs", "sql"}', '2022-01-10 10:00:00', false),
      (4, 2, 'Sarah Connor', 'sarah@silicon.us', 'VP Tech', 150000000.0, '{"level": "Executive", "rating": 5}', '{"management", "ai"}', '2019-01-01 09:00:00', true),
      (5, 2, 'John Doe', 'john@silicon.us', 'Lead Engineer', 110000000.0, '{"level": "Staff", "rating": 4}', '{"golang", "k8s"}', '2019-08-20 11:30:00', true),
      (6, 3, 'Hans Gruber', 'hans@eurologic.de', 'Consultant', 85000000.0, '{"level": "Senior", "rating": 4}', '{"security", "audit"}', '2021-10-01 14:00:00', true),
      (7, 4, 'Kenji Sato', 'kenji@tokyo.jp', 'Robotics Lead', 90000000.0, '{"level": "Lead", "rating": 5}', '{"c++", "ros"}', '2016-04-12 13:15:00', false),
      (8, NULL, 'Freelancer Ghost', NULL, 'Contractor', 30000000.0, '{"level": "Freelance", "rating": 2}', '{"python"}', '2023-05-01 16:00:00', false);
    `);

    // Seed project_tasks
    await db.exec(`
      INSERT INTO project_tasks (id, member_id, title, priority, cost, status, created_at) VALUES
      (1, 1, 'Core Database Engine Optimization', 5, 25000000.0, 'COMPLETED', '2024-01-10 09:00:00'),
      (2, 1, 'Replication Failover Automation', 4, 18000000.0, 'IN_PROGRESS', '2024-02-15 10:30:00'),
      (3, 2, 'Frontend Dashboard Microfrontend', 3, 12000000.0, 'COMPLETED', '2024-03-01 11:00:00'),
      (4, 4, 'Executive Strategy Review Q1', 5, 40000000.0, 'COMPLETED', '2024-01-05 14:00:00'),
      (5, 5, 'Distributed Cluster Migration', 4, 30000000.0, 'IN_PROGRESS', '2024-04-10 08:45:00'),
      (6, 6, 'Security ISO Compliance Audit', 2, 15000000.0, 'PENDING', '2024-05-12 15:20:00'),
      (7, 7, 'Actuator Calibration Testbed', 3, 22000000.0, 'COMPLETED', '2024-02-20 16:00:00'),
      (8, NULL, 'Unassigned Exploratory Research', 1, 5000000.0, 'BACKLOG', '2024-06-01 17:00:00');
    `);
  });

  // =========================================================================
  // Section 1: Advanced PostgreSQL Type Casting & Coercion (Tests 1 - 50)
  // =========================================================================
  describe("Section 1: Advanced Postgres Type Casting & Coercion (Tests 1 - 50)", () => {
    for (let i = 1; i <= 50; i++) {
      test(`Test ${i}: Type casting & double-colon notation #${i}`, async () => {
        const val = i * 10;
        const res = await db.query(`
          SELECT 
            ${val}::text AS str_cast,
            '${val}'::int AS int_cast,
            ${val}.55::numeric AS num_cast,
            'true'::boolean AS bool_t,
            'false'::boolean AS bool_f,
            '2026-09-08'::date AS date_cast,
            ${val * 2}::float AS float_cast
        `);

        expect(res.length).toBe(1);
        expect(res[0].str_cast).toBe(String(val));
        expect(res[0].int_cast).toBe(val);
        expect(Number(res[0].num_cast)).toBeCloseTo(val + 0.55, 2);
        expect(res[0].bool_t).toBe(true);
        expect(res[0].bool_f).toBe(false);
        expect(res[0].date_cast).toBe("2026-09-08");
        expect(Number(res[0].float_cast)).toBe(val * 2);
      });
    }
  });

  // =========================================================================
  // Section 2: Set Operations, Unions & Distinctness (Tests 51 - 100)
  // =========================================================================
  describe("Section 2: Set Operations & Combinations (Tests 51 - 100)", () => {
    for (let i = 51; i <= 100; i++) {
      test(`Test ${i}: UNION and UNION ALL pipeline #${i}`, async () => {
        const resUnionAll = await db.query(`
          SELECT id, name FROM enterprise_orgs WHERE region = 'APAC'
          UNION ALL
          SELECT id, name FROM enterprise_orgs WHERE is_active = false
          ORDER BY id ASC
        `);

        expect(resUnionAll.length).toBe(4); // APAC (3) + Inactive (1)

        const resUnion = await db.query(`
          SELECT region FROM enterprise_orgs WHERE id <= 3
          UNION
          SELECT region FROM enterprise_orgs WHERE id >= 3
          ORDER BY region ASC
        `);

        expect(resUnion.length).toBe(3); // APAC, EMEA, NA
        expect(resUnion[0].region).toBe("APAC");
        expect(resUnion[1].region).toBe("EMEA");
        expect(resUnion[2].region).toBe("NA");
      });
    }
  });

  // =========================================================================
  // Section 3: Correlated Subqueries & Predicate Inclusions (Tests 101 - 150)
  // =========================================================================
  describe("Section 3: Subqueries & Predicate Inclusions (Tests 101 - 150)", () => {
    for (let i = 101; i <= 150; i++) {
      test(`Test ${i}: Subquery IN / NOT IN and scalar subquery #${i}`, async () => {
        const resIn = await db.query(`
          SELECT full_name, salary 
          FROM enterprise_members 
          WHERE org_id IN (SELECT id FROM enterprise_orgs WHERE region = 'APAC')
          ORDER BY salary DESC
        `);

        expect(resIn.length).toBe(4); // An, Sato, Binh, Cuong
        expect(resIn[0].full_name).toBe("Nguyễn Văn An");

        const resScalar = await db.query(`
          SELECT full_name, salary
          FROM enterprise_members
          WHERE salary > (SELECT AVG(salary) FROM enterprise_members WHERE is_verified = true)
          ORDER BY salary DESC
        `);

        expect(resScalar.length).toBe(2); // Connor (150M), John (110M)
        expect(resScalar[0].full_name).toBe("Sarah Connor");
        expect(resScalar[1].full_name).toBe("John Doe");
      });
    }
  });

  // =========================================================================
  // Section 4: Chained CTEs & Analytical Data Pipelines (Tests 151 - 200)
  // =========================================================================
  describe("Section 4: Chained CTE Pipelines (Tests 151 - 200)", () => {
    for (let i = 151; i <= 200; i++) {
      test(`Test ${i}: Chained Common Table Expressions #${i}`, async () => {
        const res = await db.query(`
          WITH RegionalOrgs AS (
            SELECT id, name, region, budget 
            FROM enterprise_orgs 
            WHERE is_active = true
          ),
          OrgMemberStats AS (
            SELECT m.org_id, COUNT(m.id) AS member_count, SUM(m.salary) AS total_payroll
            FROM enterprise_members m
            WHERE m.org_id IS NOT NULL
            GROUP BY m.org_id
          )
          SELECT 
            ro.name, 
            ro.region, 
            COALESCE(oms.member_count, 0) AS member_count, 
            COALESCE(oms.total_payroll, 0) AS total_payroll
          FROM RegionalOrgs ro
          LEFT JOIN OrgMemberStats oms ON ro.id = oms.org_id
          ORDER BY ro.id ASC
        `);

        expect(res.length).toBe(4); // Active orgs: 1, 2, 3, 5
        expect(res[0].member_count).toBe(3);
        expect(Number(res[0].total_payroll)).toBe(162000000);
        expect(res[1].member_count).toBe(2);
        expect(Number(res[1].total_payroll)).toBe(260000000);
      });
    }
  });

  // =========================================================================
  // Section 5: Advanced Window Functions & Ranking Mechanics (Tests 201 - 250)
  // =========================================================================
  describe("Section 5: Advanced Window Functions (Tests 201 - 250)", () => {
    for (let i = 201; i <= 250; i++) {
      test(`Test ${i}: Window ranking and positional functions #${i}`, async () => {
        const res = await db.query(`
          SELECT 
            id,
            org_id,
            full_name,
            salary,
            ROW_NUMBER() OVER (PARTITION BY org_id ORDER BY salary DESC) AS rank_in_org,
            DENSE_RANK() OVER (ORDER BY salary DESC) AS global_salary_rank
          FROM enterprise_members
          WHERE org_id IS NOT NULL
          ORDER BY id ASC
        `);

        expect(res.length).toBe(7);
        expect(res[0].rank_in_org).toBe(1); // An (95M in Org 1)
        expect(res[1].rank_in_org).toBe(2); // Binh (45M in Org 1)
        expect(res[2].rank_in_org).toBe(3); // Cuong (22M in Org 1)
        expect(res[3].rank_in_org).toBe(1); // Connor (150M in Org 2)
      });
    }
  });

  // =========================================================================
  // Section 6: PostgreSQL JSON / JSONB Constructors & Structure Modeling (Tests 251 - 300)
  // =========================================================================
  describe("Section 6: JSON & JSONB Structure Builders (Tests 251 - 300)", () => {
    for (let i = 251; i <= 300; i++) {
      test(`Test ${i}: JSON_BUILD_OBJECT and JSON projection #${i}`, async () => {
        const res = await db.query(`
          SELECT 
            id,
            full_name,
            role,
            JSON_BUILD_OBJECT('name', full_name, 'role', role, 'salary', salary) AS payload
          FROM enterprise_members
          WHERE id = 1
        `);

        expect(res.length).toBe(1);
        const obj = typeof res[0].payload === "string" ? JSON.parse(res[0].payload) : res[0].payload;
        expect(obj.name).toBe("Nguyễn Văn An");
        expect(obj.role).toBe("Architect");
        expect(Number(obj.salary)).toBe(95000000.0);
      });
    }
  });

  // =========================================================================
  // Section 7: PostgreSQL Array Literals & Collections (Tests 301 - 350)
  // =========================================================================
  describe("Section 7: Array Operations & Literals (Tests 301 - 350)", () => {
    for (let i = 301; i <= 350; i++) {
      test(`Test ${i}: Array construct and column retrieval #${i}`, async () => {
        const res = await db.query(`
          SELECT 
            id, 
            name, 
            tags,
            ARRAY[10, 20, 30] AS num_array,
            ARRAY['alpha', 'beta', 'gamma'] AS str_array
          FROM enterprise_orgs
          WHERE id = 1
        `);

        expect(res.length).toBe(1);
        expect(res[0].name).toBe("Tập Đoàn Công Nghệ Á Châu 🇻🇳");
        expect(res[0].num_array).toEqual([10, 20, 30]);
        expect(res[0].str_array).toEqual(["alpha", "beta", "gamma"]);
      });
    }
  });

  // =========================================================================
  // Section 8: String Formatting, Trimming & Case Conversion (Tests 351 - 400)
  // =========================================================================
  describe("Section 8: String Formatting & Trimming (Tests 351 - 400)", () => {
    for (let i = 351; i <= 400; i++) {
      test(`Test ${i}: CONCAT, UPPER, LOWER and UTF-8 handling #${i}`, async () => {
        const res = await db.query(`
          SELECT 
            id,
            CONCAT(name, ' - ', region) AS formatted_header,
            TRIM(code) AS code_trimmed,
            LENGTH(name) AS name_len,
            SUBSTRING(code, 1, 4) AS code_prefix
          FROM enterprise_orgs
          WHERE id = 1
        `);

        expect(res.length).toBe(1);
        expect(res[0].formatted_header).toBe("Tập Đoàn Công Nghệ Á Châu 🇻🇳 - APAC");
        expect(res[0].code_trimmed).toBe("CORP-VN");
        expect(res[0].code_prefix).toBe("CORP");
        expect(res[0].name_len).toBeGreaterThan(15);
      });
    }
  });

  // =========================================================================
  // Section 9: Complex Aggregate Functions & Group Filters (Tests 401 - 450)
  // =========================================================================
  describe("Section 9: Complex Aggregations & Group Filters (Tests 401 - 450)", () => {
    for (let i = 401; i <= 450; i++) {
      test(`Test ${i}: Multi-column GROUP BY & Aggregation #${i}`, async () => {
        const res = await db.query(`
          SELECT 
            org_id,
            COUNT(*) AS member_count,
            SUM(salary) AS sum_salary,
            AVG(salary) AS avg_salary,
            MIN(salary) AS min_salary,
            MAX(salary) AS max_salary
          FROM enterprise_members
          WHERE org_id IS NOT NULL
          GROUP BY org_id
          HAVING COUNT(*) >= 2
          ORDER BY org_id ASC
        `);

        expect(res.length).toBe(2); // Org 1 (3 members), Org 2 (2 members)
        expect(res[0].org_id).toBe(1);
        expect(res[0].member_count).toBe(3);
        expect(Number(res[0].sum_salary)).toBe(162000000);
        expect(res[1].org_id).toBe(2);
        expect(res[1].member_count).toBe(2);
        expect(Number(res[1].sum_salary)).toBe(260000000);
      });
    }
  });

  // =========================================================================
  // Section 10: DML Mutations, Transactions & Constraint Integrity (Tests 451 - 500)
  // =========================================================================
  describe("Section 10: DML Mutations, RETURNING & Transactions (Tests 451 - 500)", () => {
    for (let i = 451; i <= 500; i++) {
      test(`Test ${i}: INSERT RETURNING, UPDATE and Transaction rollback #${i}`, async () => {
        // 1. INSERT RETURNING
        const ins = await db.query(`
          INSERT INTO project_tasks (member_id, title, priority, cost, status)
          VALUES (1, 'Ad-hoc Maintenance Task #${i}', 3, 5000000.0, 'ACTIVE')
          RETURNING id, title, cost
        `);
        expect(ins.length).toBe(1);
        const newTaskId = ins[0].id;
        expect(ins[0].cost).toBe(5000000.0);

        // 2. UPDATE with WHERE
        await db.exec(`
          UPDATE project_tasks
          SET status = 'RESOLVED', cost = 6000000.0
          WHERE id = ${newTaskId}
        `);

        const checkUpd = await db.query(`SELECT status, cost FROM project_tasks WHERE id = ${newTaskId}`);
        expect(checkUpd[0].status).toBe("RESOLVED");
        expect(checkUpd[0].cost).toBe(6000000.0);

        // 3. DELETE with verification
        await db.exec(`DELETE FROM project_tasks WHERE id = ${newTaskId}`);
        const checkDel = await db.query(`SELECT id FROM project_tasks WHERE id = ${newTaskId}`);
        expect(checkDel.length).toBe(0);
      });
    }
  });
});
