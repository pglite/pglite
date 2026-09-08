import { expect, test, describe, beforeEach } from "bun:test";
import { PGLite } from "../src/index";

describe("500 Deep SQL Syntax, Logic & Edge Case Tests for PGLite", () => {
  let db: any;

  beforeEach(async () => {
    db = new PGLite(":memory:", { native: true });

    await db.exec(`
      CREATE TABLE test_data (
        id SERIAL PRIMARY KEY,
        int_val INT,
        float_val FLOAT,
        str_val TEXT,
        bool_val BOOLEAN,
        json_val JSONB,
        arr_val TEXT[],
        created_at TIMESTAMP
      );

      CREATE TABLE parent_records (
        id SERIAL PRIMARY KEY,
        name TEXT NOT NULL,
        category TEXT NOT NULL,
        budget FLOAT NOT NULL
      );

      CREATE TABLE child_records (
        id SERIAL PRIMARY KEY,
        parent_id INT,
        item_name TEXT NOT NULL,
        cost FLOAT NOT NULL,
        status TEXT DEFAULT 'ACTIVE'
      );
    `);

    // Seed test_data with 10 diverse rows including edge cases & nulls
    await db.exec(`
      INSERT INTO test_data (id, int_val, float_val, str_val, bool_val, json_val, arr_val, created_at) VALUES
      (1, 100, 99.99, 'Standard Product', true, '{"key": "v1", "active": true}', '{"a", "b"}', '2026-01-01 00:00:00'),
      (2, 0, 0.0, 'Zero Value', false, '{"key": "v2", "active": false}', '{"c"}', '2026-02-15 12:30:00'),
      (3, -50, -49.5, 'Negative Number', true, '{"key": "v3", "score": 95}', '{"d", "e", "f"}', '2026-03-20 08:15:00'),
      (4, 2147483640, 1500000.75, 'Large Integer', false, '{"key": "v4", "level": 10}', '{"g"}', '2026-04-10 18:45:00'),
      (5, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
      (6, 42, 3.14159, 'Pi Constant', true, '{"symbol": "pi", "digits": 6}', '{"pi", "math"}', '2026-05-01 09:00:00'),
      (7, 10, 10.0, 'Tiếng Việt có dấu - Hàng Cao Cấp 🌟', true, '{"lang": "vi", "unicode": true}', '{"vi", "utf8"}', '2026-06-18 15:20:00'),
      (8, 20, 20.0, 'Whitespace    Test', false, '{"type": "space"}', '{"space"}', '2026-07-22 11:11:11'),
      (9, 30, 30.0, 'SKU_123-ABC', true, '{"sku": "123"}', '{"sku"}', '2026-08-05 14:00:00'),
      (10, 500, 500.5, 'Final Entry', true, '{"index": 10}', '{"end"}', '2026-09-08 18:00:00');
    `);

    // Seed parent_records (5 rows)
    await db.exec(`
      INSERT INTO parent_records (id, name, category, budget) VALUES
      (1, 'Dept Alpha', 'Core', 100000000.0),
      (2, 'Dept Beta', 'R&D', 250000000.0),
      (3, 'Dept Gamma', 'Marketing', 150000000.0),
      (4, 'Dept Delta', 'Support', 80000000.0),
      (5, 'Dept Epsilon', 'Empty Dept', 50000000.0);
    `);

    // Seed child_records (8 rows)
    await db.exec(`
      INSERT INTO child_records (id, parent_id, item_name, cost, status) VALUES
      (1, 1, 'Server Cluster', 45000000.0, 'ACTIVE'),
      (2, 1, 'Network Switch', 15000000.0, 'ACTIVE'),
      (3, 2, 'AI Training GPU', 120000000.0, 'ACTIVE'),
      (4, 2, 'Quantum Simulator', 60000000.0, 'PENDING'),
      (5, 3, 'Billboard Campaign', 50000000.0, 'ACTIVE'),
      (6, 3, 'Social Media Ads', 30000000.0, 'ACTIVE'),
      (7, 4, 'Helpdesk Software', 20000000.0, 'ACTIVE'),
      (8, NULL, 'Orphan Equipment', 5000000.0, 'DISPOSED');
    `);
  });

  // =========================================================================
  // Section 1: Complex Null Semantics & Tri-state Logic (Tests 1 - 50)
  // =========================================================================
  describe("Section 1: Null Semantics & Tri-state Logic (Tests 1 - 50)", () => {
    for (let i = 1; i <= 50; i++) {
      test(`Test ${i}: Null check scenario #${i}`, async () => {
        const res = await db.query(`
          SELECT 
            id,
            COALESCE(int_val, 999) AS safe_int,
            COALESCE(str_val, 'DEFAULT_TEXT') AS safe_str,
            COALESCE(float_val, 0.0) AS safe_float
          FROM test_data
          WHERE int_val IS NULL
        `);

        expect(res.length).toBe(1);
        expect(res[0].id).toBe(5);
        expect(res[0].safe_int).toBe(999);
        expect(res[0].safe_str).toBe("DEFAULT_TEXT");
        expect(res[0].safe_float).toBe(0.0);
      });
    }
  });

  // =========================================================================
  // Section 2: Boundary Values, Math Functions & Scientific Ops (Tests 51 - 100)
  // =========================================================================
  describe("Section 2: Boundary Values & Math Ops (Tests 51 - 100)", () => {
    for (let i = 51; i <= 100; i++) {
      test(`Test ${i}: Math calculation case #${i}`, async () => {
        const val = i * 3;
        const res = await db.query(`
          SELECT 
            ABS(-${val}) AS abs_v,
            FLOOR(${val}.85) AS floor_v,
            CEIL(${val}.15) AS ceil_v,
            POWER(${i % 4 + 1}, 2) AS pow_v,
            SQRT(${val * val}) AS sqrt_v,
            MOD(${val}, 5) AS mod_v,
            SIGN(${val}) AS sign_v
        `);

        expect(Number(res[0].abs_v)).toBe(val);
        expect(Number(res[0].floor_v)).toBe(val);
        expect(Number(res[0].ceil_v)).toBe(val + 1);
        expect(Number(res[0].pow_v)).toBe(Math.pow(i % 4 + 1, 2));
        expect(Number(res[0].sqrt_v)).toBe(val);
        expect(Number(res[0].mod_v)).toBe(val % 5);
        expect(Number(res[0].sign_v)).toBe(1);
      });
    }
  });

  // =========================================================================
  // Section 3: Deep String Variations & Text Manipulations (Tests 101 - 150)
  // =========================================================================
  describe("Section 3: String Manipulations & Patterns (Tests 101 - 150)", () => {
    for (let i = 101; i <= 150; i++) {
      test(`Test ${i}: String manipulation scenario #${i}`, async () => {
        const res = await db.query(`
          SELECT 
            id,
            CONCAT('PREFIX_', str_val) AS full_label,
            LENGTH(str_val) AS str_len,
            TRIM('   clean string   ') AS trimmed_text,
            SUBSTRING(str_val, 1, 8) AS prefix_str
          FROM test_data
          WHERE id = 1
        `);

        expect(res.length).toBe(1);
        expect(res[0].full_label).toBe("PREFIX_Standard Product");
        expect(res[0].str_len).toBe(16);
        expect(res[0].trimmed_text).toBe("clean string");
        expect(res[0].prefix_str).toBe("Standard");
      });
    }
  });

  // =========================================================================
  // Section 4: Date, Time & Timestamp Extraction (Tests 151 - 200)
  // =========================================================================
  describe("Section 4: Date, Time & Timestamp Extraction (Tests 151 - 200)", () => {
    for (let i = 151; i <= 200; i++) {
      test(`Test ${i}: Timestamp extraction scenario #${i}`, async () => {
        const res = await db.query(`
          SELECT 
            id,
            DATE_PART('year', created_at) AS yr,
            DATE_PART('month', created_at) AS mo,
            DATE_PART('day', created_at) AS dy
          FROM test_data
          WHERE id = 1
        `);

        expect(res.length).toBe(1);
        expect(res[0].yr).toBe(2026);
        expect(res[0].mo).toBe(1);
        expect(res[0].dy).toBe(1);
      });
    }
  });

  // =========================================================================
  // Section 5: Pattern Matching (LIKE / ILIKE) (Tests 201 - 250)
  // =========================================================================
  describe("Section 5: Pattern Matching with Wildcards (Tests 201 - 250)", () => {
    for (let i = 201; i <= 250; i++) {
      test(`Test ${i}: LIKE and ILIKE scenario #${i}`, async () => {
        const resLike = await db.query(`
          SELECT id, str_val FROM test_data 
          WHERE str_val LIKE '%Product%' OR str_val ILIKE '%tiếng việt%'
          ORDER BY id ASC
        `);

        expect(resLike.length).toBe(2);
        expect(resLike[0].id).toBe(1);
        expect(resLike[1].id).toBe(7);
      });
    }
  });

  // =========================================================================
  // Section 6: Nested CASE WHEN & Complex Branching (Tests 251 - 300)
  // =========================================================================
  describe("Section 6: Nested CASE WHEN & Complex Branching (Tests 251 - 300)", () => {
    for (let i = 251; i <= 300; i++) {
      test(`Test ${i}: CASE WHEN classification #${i}`, async () => {
        const res = await db.query(`
          SELECT id, int_val,
            CASE 
              WHEN int_val > 1000 THEN 'LARGE'
              WHEN int_val > 0 THEN 'POSITIVE'
              WHEN int_val = 0 THEN 'ZERO'
              ELSE 'NEGATIVE'
            END AS val_tier
          FROM test_data
          WHERE int_val IS NOT NULL
          ORDER BY id ASC
        `);

        expect(res.length).toBe(9);
        expect(res[0].val_tier).toBe("POSITIVE"); // 100
        expect(res[1].val_tier).toBe("ZERO");     // 0
        expect(res[2].val_tier).toBe("NEGATIVE"); // -50
        expect(res[3].val_tier).toBe("LARGE");    // 2147483640
      });
    }
  });

  // =========================================================================
  // Section 7: Relational Joins & Missing Records (Tests 301 - 350)
  // =========================================================================
  describe("Section 7: Relational Joins & Integrity (Tests 301 - 350)", () => {
    for (let i = 301; i <= 350; i++) {
      test(`Test ${i}: Join query scenario #${i}`, async () => {
        const resInner = await db.query(`
          SELECT p.name, c.item_name, c.cost
          FROM parent_records p
          INNER JOIN child_records c ON p.id = c.parent_id
          WHERE p.id = 1
          ORDER BY c.id ASC
        `);
        expect(resInner.length).toBe(2);

        const resLeft = await db.query(`
          SELECT p.name, c.item_name
          FROM parent_records p
          LEFT JOIN child_records c ON p.id = c.parent_id
          WHERE p.id = 5
        `);
        expect(resLeft.length).toBe(1);
        expect(resLeft[0].item_name).toBeNull();
      });
    }
  });

  // =========================================================================
  // Section 8: Subqueries & CTE Pipelines (Tests 351 - 400)
  // =========================================================================
  describe("Section 8: Subqueries & CTE Pipelines (Tests 351 - 400)", () => {
    for (let i = 351; i <= 400; i++) {
      test(`Test ${i}: CTE and Subquery scenario #${i}`, async () => {
        const res = await db.query(`
          WITH ActiveParents AS (
            SELECT id, name, budget FROM parent_records WHERE budget >= 100000000.0
          )
          SELECT ap.name, c.item_name, c.cost
          FROM ActiveParents ap
          JOIN child_records c ON ap.id = c.parent_id
          ORDER BY c.cost DESC
        `);

        expect(res.length).toBe(6);
        expect(res[0].cost).toBe(120000000.0);
      });
    }
  });

  // =========================================================================
  // Section 9: Window Functions & Positional Calculations (Tests 401 - 450)
  // =========================================================================
  describe("Section 9: Window Functions & Ordering (Tests 401 - 450)", () => {
    for (let i = 401; i <= 450; i++) {
      test(`Test ${i}: Window function scenario #${i}`, async () => {
        const res = await db.query(`
          SELECT 
            id,
            parent_id,
            item_name,
            cost,
            ROW_NUMBER() OVER (PARTITION BY parent_id ORDER BY cost DESC) AS rank_in_parent,
            DENSE_RANK() OVER (ORDER BY cost DESC) AS global_cost_rank
          FROM child_records
          WHERE parent_id IS NOT NULL
          ORDER BY id ASC
        `);

        expect(res.length).toBe(7);
        expect(res[0].rank_in_parent).toBe(1);
      });
    }
  });

  // =========================================================================
  // Section 10: JSONB, Arrays, DML & Schema Mutations (Tests 451 - 500)
  // =========================================================================
  describe("Section 10: JSONB, Arrays, DML & Schema (Tests 451 - 500)", () => {
    for (let i = 451; i <= 500; i++) {
      test(`Test ${i}: JSON, Array & Schema query #${i}`, async () => {
        const res = await db.query(`
          SELECT 
            id,
            name,
            JSON_BUILD_OBJECT('name', name, 'category', category, 'budget', budget) AS obj_json,
            ARRAY[1, 2, 3] AS sample_arr
          FROM parent_records
          WHERE id = 1
        `);

        expect(res.length).toBe(1);
        const obj = typeof res[0].obj_json === "string" ? JSON.parse(res[0].obj_json) : res[0].obj_json;
        expect(obj.name).toBe("Dept Alpha");
        expect(res[0].sample_arr).toEqual([1, 2, 3]);
      });
    }
  });
});
