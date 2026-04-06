import { expect, test, describe, beforeAll, afterAll } from "bun:test";
import { LitePostgres } from "../src/database";

import { unlinkSync, existsSync } from "fs";
import { NodeFSAdapter } from "../src/adapters/node";

const DB_FILE = "test_engine.db";

describe("LitePostgres Engine Comprehensive Test Suite", () => {
  let db: LitePostgres;

  beforeAll(() => {
    if (existsSync(DB_FILE)) unlinkSync(DB_FILE);
    if (existsSync(DB_FILE + ".wal")) unlinkSync(DB_FILE + ".wal");
    db = new LitePostgres(DB_FILE, {
      database: "testdb",
      adapter: new NodeFSAdapter(),
    });
  });

  describe("LEVEL 6: Procedural Features", () => {
    test("6.1 Anonymous Blocks (DO statement with $$ strings)", async () => {
      const sql = `
        DO $$
        BEGIN
          -- Simulated procedural block
          NULL;
        END;
        $$
      `;
      const res = await db.exec(sql);
      expect(res.success).toBe(true);
      expect(res.executed_block).toContain("BEGIN");
    });

    test("6.2 DO statement with explicit LANGUAGE", async () => {
      const sql = `DO LANGUAGE plpgsql $$ RAISE NOTICE 'Hello'; $$`;
      const res = await db.exec(sql);
      expect(res.success).toBe(true);
      expect(res.language).toBe("plpgsql");
      expect(res.executed_block).toBe(" RAISE NOTICE 'Hello'; ");
    });

    test("6.3 Using $DO alias (PostgreSQL extension-like syntax)", async () => {
      const sql = `$DO $$ RETURN 1; $$`;
      const res = await db.exec(sql);
      expect(res.success).toBe(true);
      expect(res.executed_block).toBe(" RETURN 1; ");
    });
  });

  afterAll(() => {
    if (existsSync(DB_FILE)) unlinkSync(DB_FILE);
    if (existsSync(DB_FILE + ".wal")) unlinkSync(DB_FILE + ".wal");
  });

  describe("LEVEL 1: Basic Operations (Currently Supported)", () => {
    test("1.1 Create Table", async () => {
      const res = await db.exec(
        `CREATE TABLE users (id SERIAL PRIMARY KEY, name TEXT, age NUMBER)`,
      );
      expect(res.success).toBe(true);
    });

    test("1.2 Insert Data", async () => {
      await db.exec(`INSERT INTO users (name, age) VALUES ('Alice', 25)`);
      await db.exec(`INSERT INTO users (name, age) VALUES ('Bob', 30)`);
      const res = await db.exec(
        `INSERT INTO users (name, age) VALUES ('Charlie', 35)`,
      );
      expect(res.success).toBe(true);
    });

    test("1.3 Simple Select", async () => {
      const rows = await db.query(`SELECT * FROM users`);
      expect(rows.length).toBe(3);
      expect(rows[0].name).toBe("Alice");
    });

    test("1.4 Select with basic WHERE (Uses Index Pushdown)", async () => {
      const rows = await db.query(`SELECT name FROM users WHERE id = 2`);
      expect(rows.length).toBe(1);
      expect(rows[0].name).toBe("Bob");
      expect(rows[0].id).toBeUndefined();
    });

    test("1.5 Update with WHERE", async () => {
      const res = await db.exec(
        `UPDATE users SET age = 26 WHERE name = 'Alice'`,
      );
      expect(res.updated).toBe(1);
      const rows = await db.query(`SELECT * FROM users WHERE id = 1`);
      expect(rows[0].age).toBe(26);
    });

    test("1.6 Delete with WHERE", async () => {
      const res = await db.exec(`DELETE FROM users WHERE name = 'Charlie'`);
      expect(res.deleted).toBe(1);
      const rows = await db.query(`SELECT * FROM users`);
      expect(rows.length).toBe(2);
    });

    test("1.7 Parameterized Queries ($1, $2, ...)", async () => {
      // Test INSERT with params
      const resInsert = await db.exec(
        `INSERT INTO users (name, age) VALUES ($1, $2)`,
        ["Diana", 28],
      );
      expect(resInsert.success).toBe(true);

      // Test SELECT with multiple params
      const rows = await db.query(
        `SELECT * FROM users WHERE name = $1 AND age = $2`,
        ["Diana", 28],
      );
      expect(rows.length).toBe(1);
      expect(rows[0].name).toBe("Diana");
      expect(rows[0].age).toBe(28);

      // Test same param used multiple times
      const rowsMulti = await db.query(
        `SELECT * FROM users WHERE name = $1 OR name = $1`,
        ["Diana"],
      );
      expect(rowsMulti.length).toBe(1);

      // Test positional consistency
      const rowsReordered = await db.query(
        `SELECT name FROM users WHERE age = $2 AND name = $1`,
        ["Diana", 28],
      );
      expect(rowsReordered.length).toBe(1);
      expect(rowsReordered[0].name).toBe("Diana");

      // Test UPDATE with params
      const resUpdate = await db.exec(
        `UPDATE users SET age = $1 WHERE name = $2`,
        [29, "Diana"],
      );
      expect(resUpdate.updated).toBe(1);

      // Verify UPDATE
      const rowsVerify = await db.query(
        `SELECT age FROM users WHERE name = $1`,
        ["Diana"],
      );
      expect(rowsVerify[0].age).toBe(29);

      // Test DELETE with params
      const resDelete = await db.exec(`DELETE FROM users WHERE name = $1`, [
        "Diana",
      ]);
      expect(resDelete.deleted).toBe(1);
    });
  });

  describe("LEVEL 2: Intermediate Querying (Target Features)", () => {
    test("2.1 Comparison Operators (>, <, >=, <=, !=)", async () => {
      const rows = await db.query(`SELECT * FROM users WHERE age >= 30`);
      expect(rows.length).toBe(1);
      expect(rows[0].name).toBe("Bob");
    });

    test("2.2 Logical Operators (AND, OR, NOT)", async () => {
      const rows = await db.query(
        `SELECT * FROM users WHERE age > 20 AND name = 'Alice'`,
      );
      expect(rows.length).toBe(1);
      const rows2 = await db.query(
        `SELECT * FROM users WHERE age = 30 OR name = 'Alice'`,
      );
      expect(rows2.length).toBe(2);
    });

    test("2.3 IN and LIKE operators", async () => {
      const rows = await db.query(`SELECT * FROM users WHERE name LIKE 'Al%'`);
      expect(rows.length).toBe(1);
      const rows2 = await db.query(`SELECT * FROM users WHERE age IN (26, 30)`);
      expect(rows2.length).toBe(2);
    });

    test("2.4 ORDER BY and LIMIT / OFFSET", async () => {
      await db.exec(`INSERT INTO users (name, age) VALUES ('David', 40)`);
      const rows = await db.query(
        `SELECT * FROM users ORDER BY age DESC LIMIT 2 OFFSET 1`,
      );
      // 40 (David), 30 (Bob), 26 (Alice)
      // LIMIT 2 OFFSET 1 -> Bob, Alice
      expect(rows.length).toBe(2);
      expect(rows[0].name).toBe("Bob");
      expect(rows[1].name).toBe("Alice");
    });
  });

  describe("LEVEL 3: Advanced Relational Features", () => {
    beforeAll(async () => {
      await db.exec(
        `CREATE TABLE posts (id SERIAL PRIMARY KEY, title TEXT, user_id NUMBER)`,
      );
      await db.exec(
        `INSERT INTO posts (title, user_id) VALUES ('Hello World', 1)`,
      );
      await db.exec(
        `INSERT INTO posts (title, user_id) VALUES ('Bun is fast', 1)`,
      );
      await db.exec(
        `INSERT INTO posts (title, user_id) VALUES ('Postgres Lite', 2)`,
      );
    });

    test("3.1 INNER JOIN", async () => {
      const rows = await db.query(`
        SELECT users.name, posts.title 
        FROM users 
        INNER JOIN posts ON users.id = posts.user_id 
        WHERE users.name = 'Alice'
      `);
      expect(rows.length).toBe(2);
      expect(rows[0].title).toBe("Hello World");
    });

    test("3.2 LEFT JOIN", async () => {
      await db.exec(`INSERT INTO users (name, age) VALUES ('Eve', 22)`);
      const rows = await db.query(`
        SELECT users.name, posts.title 
        FROM users 
        LEFT JOIN posts ON users.id = posts.user_id 
        WHERE users.name = 'Eve'
      `);
      expect(rows.length).toBe(1);
      expect(rows[0].title).toBeUndefined();
    });

    test("3.2.1 RIGHT JOIN", async () => {
      await db.exec(`INSERT INTO posts (title, user_id) VALUES ('Orphan Post', 99)`);
      const rows = await db.query(`
        SELECT users.name, posts.title 
        FROM users 
        RIGHT JOIN posts ON users.id = posts.user_id 
        ORDER BY posts.id
      `);
      // posts have 4 rows total now: 
      // Hello World (user 1), Bun is fast (user 1), Postgres Lite (user 2), Orphan Post (user 99)
      expect(rows.length).toBe(4);
      const orphan = rows.find(r => r.title === 'Orphan Post');
      expect(orphan).toBeDefined();
      expect(orphan.name).toBeUndefined();
      
      const pglite = rows.find(r => r.title === 'Postgres Lite');
      expect(pglite).toBeDefined();
      expect(pglite.name).toBe('Bob');
    });

    test("3.2.2 FULL OUTER JOIN", async () => {
      await db.exec(`CREATE TABLE full_a (id NUMBER, val_a TEXT)`);
      await db.exec(`CREATE TABLE full_b (id NUMBER, val_b TEXT)`);
      await db.exec(`INSERT INTO full_a VALUES (1, 'A1'), (2, 'A2')`);
      await db.exec(`INSERT INTO full_b VALUES (2, 'B2'), (3, 'B3')`);

      const rows = await db.query(`
        SELECT full_a.id as a_id, full_a.val_a, full_b.id as b_id, full_b.val_b
        FROM full_a
        FULL OUTER JOIN full_b ON full_a.id = full_b.id
        ORDER BY COALESCE(full_a.id, full_b.id)
      `);

      expect(rows.length).toBe(3);
      
      // 1 (only in A)
      expect(rows[0].a_id).toBe(1);
      expect(rows[0].val_a).toBe('A1');
      expect(rows[0].b_id).toBeUndefined();
      
      // 2 (in both)
      expect(rows[1].a_id).toBe(2);
      expect(rows[1].val_a).toBe('A2');
      expect(rows[1].b_id).toBe(2);
      expect(rows[1].val_b).toBe('B2');

      // 3 (only in B)
      expect(rows[2].a_id).toBeUndefined();
      expect(rows[2].b_id).toBe(3);
      expect(rows[2].val_b).toBe('B3');
    });

    test("3.2.2 FULL OUTER JOIN", async () => {
      await db.exec(`DROP TABLE IF EXISTS full_a`);
      await db.exec(`DROP TABLE IF EXISTS full_b`);
      await db.exec(`CREATE TABLE full_a (id NUMBER, val_a TEXT)`);
      await db.exec(`CREATE TABLE full_b (id NUMBER, val_b TEXT)`);
      // Đã test luôn tính năng tự nhận diện cột khi insert (sau khi fix ở executor)
      await db.exec(`INSERT INTO full_a VALUES (1, 'A1'), (2, 'A2')`);
      await db.exec(`INSERT INTO full_b VALUES (2, 'B2'), (3, 'B3')`);

      const rows = await db.query(`
        SELECT full_a.id as a_id, full_a.val_a, full_b.id as b_id, full_b.val_b
        FROM full_a
        FULL OUTER JOIN full_b ON full_a.id = full_b.id
        ORDER BY COALESCE(full_a.id, full_b.id)
      `);

      expect(rows.length).toBe(3);
      
      // 1 (only in A)
      expect(rows[0].a_id).toBe(1);
      expect(rows[0].val_a).toBe('A1');
      expect(rows[0].b_id).toBeUndefined();
      
      // 2 (in both)
      expect(rows[1].a_id).toBe(2);
      expect(rows[1].val_a).toBe('A2');
      expect(rows[1].b_id).toBe(2);
      expect(rows[1].val_b).toBe('B2');

      // 3 (only in B)
      expect(rows[2].a_id).toBeUndefined();
      expect(rows[2].b_id).toBe(3);
      expect(rows[2].val_b).toBe('B3');
    });

    test("3.2.3 CROSS JOIN", async () => {
      await db.exec(`CREATE TABLE cross_a (id NUMBER, val_a TEXT)`);
      await db.exec(`CREATE TABLE cross_b (id NUMBER, val_b TEXT)`);
      await db.exec(`INSERT INTO cross_a VALUES (1, 'A1'), (2, 'A2')`);
      await db.exec(`INSERT INTO cross_b VALUES (1, 'B1'), (2, 'B2')`);

      const rows = await db.query(`
        SELECT cross_a.val_a, cross_b.val_b
        FROM cross_a
        CROSS JOIN cross_b
        ORDER BY cross_a.val_a, cross_b.val_b
      `);

      expect(rows.length).toBe(4);
      expect(rows[0].val_a).toBe('A1');
      expect(rows[0].val_b).toBe('B1');
      expect(rows[1].val_a).toBe('A1');
      expect(rows[1].val_b).toBe('B2');
      expect(rows[2].val_a).toBe('A2');
      expect(rows[2].val_b).toBe('B1');
      expect(rows[3].val_a).toBe('A2');
      expect(rows[3].val_b).toBe('B2');
    });

    test("3.3 Aggregation & GROUP BY", async () => {
      const rows = await db.query(`
        SELECT user_id, COUNT(id) as post_count 
        FROM posts 
        GROUP BY user_id
        HAVING COUNT(id) > 1
      `);
      expect(rows.length).toBe(1);
      expect(rows[0].user_id).toBe(1);
      expect(rows[0].post_count).toBe(2);
    });

    test("3.4 Subqueries in WHERE and FROM", async () => {
      const rows = await db.query(`
        SELECT name FROM users 
        WHERE id IN (SELECT user_id FROM posts WHERE title = 'Postgres Lite')
      `);
      expect(rows.length).toBe(1);
      expect(rows[0].name).toBe("Bob");

      const rows2 = await db.query(`
        SELECT avg_age FROM (SELECT AVG(age) as avg_age FROM users)
      `);
      expect(rows2.length).toBe(1);
      expect(rows2[0].avg_age).toBeGreaterThan(0);
    });

    test("3.5 Computed Columns / Expressions", async () => {
      const rows = await db.query(
        `SELECT age * 2 as double_age FROM users WHERE name = 'Alice'`,
      );
      expect(rows[0].double_age).toBe(52);
    });

    test("3.6 ARRAY_AGG function", async () => {
      const rows = await db.query(`
        SELECT user_id, ARRAY_AGG(title) as titles 
        FROM posts 
        GROUP BY user_id
        ORDER BY user_id ASC
      `);
      expect(rows.length).toBe(3);
      expect(rows[0].user_id).toBe(1);
      expect(rows[0].titles).toEqual(["Hello World", "Bun is fast"]);
      expect(rows[1].user_id).toBe(2);
      expect(rows[1].titles).toEqual(["Postgres Lite"]);
      expect(rows[2].user_id).toBe(99);
      expect(rows[2].titles).toEqual(["Orphan Post"]);
    });
  });

  describe("LEVEL 4: Data Integrity, Schema & Transactions", () => {
    test("4.1 Constraints (UNIQUE, NOT NULL)", async () => {
      expect(async () => {
        await db.exec(
          `CREATE TABLE tags (id SERIAL PRIMARY KEY, name TEXT UNIQUE NOT NULL)`,
        );
        await db.exec(`INSERT INTO tags (name) VALUES (NULL)`);
      }).toThrow();

      expect(async () => {
        await db.exec(`INSERT INTO tags (name) VALUES ('database')`);
        await db.exec(`INSERT INTO tags (name) VALUES ('database')`);
      }).toThrow();
    });

    test("4.2 Foreign Keys", async () => {
      expect(async () => {
        await db.exec(
          `CREATE TABLE comments (id SERIAL PRIMARY KEY, post_id NUMBER REFERENCES posts(id))`,
        );
        await db.exec(`INSERT INTO comments (post_id) VALUES (9999)`); // Does not exist
      }).toThrow();
    });

    test("4.3 ALTER TABLE - ADD COLUMN", async () => {
      await db.exec(`ALTER TABLE users ADD COLUMN active NUMBER DEFAULT 1`);
      const rows = await db.query(
        `SELECT active FROM users WHERE name = 'Alice'`,
      );
      expect(rows[0].active).toBe(1);
    });

    test("4.3.1 ALTER TABLE - ADD COLUMN IF NOT EXISTS", async () => {
      await db.exec(`ALTER TABLE users ADD COLUMN IF NOT EXISTS phone TEXT`);
      const rows = await db.query(`SELECT * FROM users WHERE name = 'Alice'`);
      expect(rows[0].phone).toBeDefined();

      // Should not throw when adding existing column
      await db.exec(`ALTER TABLE users ADD COLUMN IF NOT EXISTS active NUMBER`);
    });

    test("4.3.2 ALTER TABLE - RENAME COLUMN", async () => {
      await db.exec(`CREATE TABLE rename_col_test (old_name TEXT, val NUMBER)`);
      await db.exec(
        `INSERT INTO rename_col_test (old_name, val) VALUES ('test_val', 123)`,
      );
      await db.exec(
        `ALTER TABLE rename_col_test RENAME COLUMN old_name TO new_name`,
      );
      const rows = await db.query(`SELECT new_name, val FROM rename_col_test`);
      expect(rows[0].new_name).toBe("test_val");
      expect(rows[0].old_name).toBeUndefined();
    });

    test("4.3.3 ALTER TABLE - DROP COLUMN", async () => {
      await db.exec(`ALTER TABLE rename_col_test DROP COLUMN val`);
      const rows = await db.query(`SELECT * FROM rename_col_test`);
      expect(rows[0].val).toBeUndefined();
      expect(rows[0].new_name).toBe("test_val");
    });

    test("4.3.4 ALTER TABLE - DROP COLUMN IF EXISTS", async () => {
      await db.exec(
        `ALTER TABLE rename_col_test DROP COLUMN IF EXISTS new_name`,
      );
      await db.exec(
        `ALTER TABLE rename_col_test DROP COLUMN IF EXISTS non_existent`,
      );
      // Table should still exist and allow schema modification
      await db.exec(`ALTER TABLE rename_col_test ADD COLUMN check_col TEXT`);
      const res = await db.exec(
        `INSERT INTO rename_col_test (check_col) VALUES ('ok')`,
      );
      expect(res.success).toBe(true);
    });

    test("4.3.5 ALTER TABLE - RENAME TABLE", async () => {
      await db.exec(
        `CREATE TABLE table_to_rename (id SERIAL PRIMARY KEY, data TEXT)`,
      );
      await db.exec(`INSERT INTO table_to_rename (data) VALUES ('find_me')`);
      await db.exec(`ALTER TABLE table_to_rename RENAME TO table_renamed`);
      const rows = await db.query(`SELECT * FROM table_renamed`);
      expect(rows.length).toBe(1);
      expect(rows[0].data).toBe("find_me");

      expect(async () => {
        await db.query(`SELECT * FROM table_to_rename`);
      }).toThrow();
    });

    test("4.3.6 ALTER TABLE - SET/DROP DEFAULT", async () => {
      await db.exec(
        `CREATE TABLE alter_default (id SERIAL PRIMARY KEY, val NUMBER)`,
      );
      await db.exec(
        `ALTER TABLE alter_default ALTER COLUMN val SET DEFAULT 100`,
      );
      await db.exec(`INSERT INTO alter_default (id) VALUES (1)`);
      let rows = await db.query(`SELECT val FROM alter_default WHERE id = 1`);
      expect(rows[0].val).toBe(100);

      await db.exec(`ALTER TABLE alter_default ALTER COLUMN val DROP DEFAULT`);
      await db.exec(`INSERT INTO alter_default (id) VALUES (2)`);
      rows = await db.query(`SELECT val FROM alter_default WHERE id = 2`);
      expect(rows[0].val).toBeNull();
    });

    test("4.3.7 ALTER TABLE - SET/DROP NOT NULL", async () => {
      await db.exec(
        `CREATE TABLE alter_not_null (id SERIAL PRIMARY KEY, name TEXT)`,
      );
      await db.exec(
        `ALTER TABLE alter_not_null ALTER COLUMN name SET NOT NULL`,
      );

      expect(async () => {
        await db.exec(`INSERT INTO alter_not_null (name) VALUES (NULL)`);
      }).toThrow();

      await db.exec(
        `ALTER TABLE alter_not_null ALTER COLUMN name DROP NOT NULL`,
      );
      await db.exec(`INSERT INTO alter_not_null (name) VALUES (NULL)`);
      const rows = await db.query(
        `SELECT * FROM alter_not_null WHERE name IS NULL`,
      );
      expect(rows.length).toBe(1);
    });

    test("4.3.8 ALTER TABLE - ADD COLUMN with UNIQUE", async () => {
      await db.exec(`CREATE TABLE alter_unique_test (id SERIAL PRIMARY KEY)`);
      await db.exec(
        `ALTER TABLE alter_unique_test ADD COLUMN slug TEXT UNIQUE`,
      );
      await db.exec(`INSERT INTO alter_unique_test (slug) VALUES ('first')`);

      expect(async () => {
        await db.exec(`INSERT INTO alter_unique_test (slug) VALUES ('first')`);
      }).toThrow();
    });

    test("4.3.9 ALTER TABLE - ADD COLUMN with NOT NULL", async () => {
      await db.exec(`CREATE TABLE alter_not_null_test (id SERIAL PRIMARY KEY)`);
      await db.exec(
        `ALTER TABLE alter_not_null_test ADD COLUMN description TEXT NOT NULL DEFAULT 'none'`,
      );

      // Should allow inserting with default
      await db.exec(`INSERT INTO alter_not_null_test (id) VALUES (1)`);

      expect(async () => {
        // Attempt to insert null explicitly
        await db.exec(
          `INSERT INTO alter_not_null_test (id, description) VALUES (2, NULL)`,
        );
      }).toThrow();
    });

    test("4.3.10 ALTER TABLE - ADD COLUMN with REFERENCES", async () => {
      await db.exec(`CREATE TABLE parent_ref (id NUMBER PRIMARY KEY)`);
      await db.exec(`INSERT INTO parent_ref (id) VALUES (1)`);

      await db.exec(`CREATE TABLE child_ref (id SERIAL PRIMARY KEY)`);
      await db.exec(
        `ALTER TABLE child_ref ADD COLUMN parent_id NUMBER REFERENCES parent_ref(id)`,
      );

      // Valid reference
      await db.exec(`INSERT INTO child_ref (parent_id) VALUES (1)`);

      // Invalid reference
      expect(async () => {
        await db.exec(`INSERT INTO child_ref (parent_id) VALUES (999)`);
      }).toThrow();
    });

    test("4.4 Transactions (ACID)", async () => {
      await db.exec(`BEGIN`);
      await db.exec(`INSERT INTO users (name, age) VALUES ('Ghost', 99)`);
      await db.exec(`ROLLBACK`);
      const rows = await db.query(`SELECT * FROM users WHERE name = 'Ghost'`);
      expect(rows.length).toBe(0);

      await db.exec(`BEGIN`);
      await db.exec(`INSERT INTO users (name, age) VALUES ('Phantom', 88)`);
      await db.exec(`COMMIT`);
      const rows2 = await db.query(
        `SELECT * FROM users WHERE name = 'Phantom'`,
      );
      expect(rows2.length).toBe(1);
    });

    test("4.4.1 Transactions - START TRANSACTION and END", async () => {
      await db.exec(`START TRANSACTION`);
      await db.exec(`INSERT INTO users (name, age) VALUES ('StartEnd', 10)`);
      await db.exec(`END`);
      const rows = await db.query(
        `SELECT * FROM users WHERE name = 'StartEnd'`,
      );
      expect(rows.length).toBe(1);
    });

    test("4.4.2 Transactions - Schema Rollback", async () => {
      await db.exec(`BEGIN`);
      await db.exec(`CREATE TABLE rollback_schema (id NUMBER)`);
      await db.exec(`ROLLBACK`);

      // Table should not exist
      expect(async () => {
        await db.query(`SELECT * FROM rollback_schema`);
      }).toThrow();
    });

    test("4.4.3 Transactions - Alter Table Rollback", async () => {
      await db.exec(`CREATE TABLE alter_rollback (id NUMBER)`);
      await db.exec(`BEGIN`);
      await db.exec(`ALTER TABLE alter_rollback ADD COLUMN secret TEXT`);
      await db.exec(`ROLLBACK`);

      const rows = await db.query(`SELECT * FROM alter_rollback`);
      expect(rows.length).toBe(0);
      // Column 'secret' should not exist in record
      await db.exec(`INSERT INTO alter_rollback (id) VALUES (1)`);
      const rows2 = await db.query(`SELECT * FROM alter_rollback`);
      expect(rows2[0].secret).toBeUndefined();
    });

    test("4.4.4 Transactions - Index Consistency after Rollback", async () => {
      await db.exec(
        `CREATE TABLE index_rollback (id NUMBER PRIMARY KEY, val TEXT)`,
      );
      await db.exec(
        `INSERT INTO index_rollback (id, val) VALUES (1, 'initial')`,
      );

      await db.exec(`BEGIN`);
      await db.exec(`UPDATE index_rollback SET val = 'changed' WHERE id = 1`);
      await db.exec(
        `INSERT INTO index_rollback (id, val) VALUES (2, 'temporary')`,
      );
      await db.exec(`ROLLBACK`);

      // Verify row 1 reverted
      const row1 = await db.query(
        `SELECT val FROM index_rollback WHERE id = 1`,
      );
      expect(row1[0].val).toBe("initial");

      // Verify row 2 is gone from index lookup
      const row2 = await db.query(`SELECT * FROM index_rollback WHERE id = 2`);
      expect(row2.length).toBe(0);
    });

    test("4.4.5 Transactions - Abort alias", async () => {
      await db.exec(`BEGIN`);
      await db.exec(`INSERT INTO users (name, age) VALUES ('Aborted', 0)`);
      await db.exec(`ABORT`);
      const rows = await db.query(`SELECT * FROM users WHERE name = 'Aborted'`);
      expect(rows.length).toBe(0);
    });
  });

  describe("LEVEL 5: Extremely Complex Queries", () => {
    test("5.1 Multi-join with aggregations, subqueries, and grouping", async () => {
      const sql = `
        SELECT 
          u.name,
          COUNT(p.id) as total_posts,
          (SELECT COUNT(*) FROM comments c WHERE c.post_id = p.id) as total_comments
        FROM users u
        LEFT JOIN posts p ON u.id = p.user_id
        WHERE u.age >= 20 AND u.active = 1
        GROUP BY u.id, u.name
        HAVING COUNT(p.id) > 0
        ORDER BY total_posts DESC, u.name ASC
        LIMIT 10
      `;
      // Ensure parser and executor can handle the complexity without throwing
      expect(async () => await db.query(sql)).not.toThrow();
    });

    test("5.2 CTE (Common Table Expressions)", async () => {
      const sql = `
        WITH user_stats AS (
          SELECT user_id, COUNT(*) as post_count
          FROM posts
          GROUP BY user_id
        )
        SELECT u.name, s.post_count
        FROM users u
        JOIN user_stats s ON u.id = s.user_id
        WHERE s.post_count >= 1
      `;
      expect(async () => await db.query(sql)).not.toThrow();
    });

    test("5.3 UNION, INTERSECT, and EXCEPT", async () => {
      await db.exec(`CREATE TABLE set_ops_test (val INT)`);
      await db.exec(`INSERT INTO set_ops_test (val) VALUES (1), (2), (3), (4), (5)`);

      const unionRows = await db.query(`
        SELECT val FROM set_ops_test WHERE val < 3
        UNION
        SELECT val FROM set_ops_test WHERE val > 4
      `);
      expect(unionRows.length).toBe(3);
      const unionVals = unionRows.map((r: any) => r.val).sort();
      expect(unionVals).toEqual([1, 2, 5]);

      const intersectRows = await db.query(`
        SELECT val FROM set_ops_test WHERE val < 4
        INTERSECT
        SELECT val FROM set_ops_test WHERE val > 2
      `);
      expect(intersectRows.length).toBe(1);
      expect(intersectRows[0].val).toBe(3);

      const exceptRows = await db.query(`
        SELECT val FROM set_ops_test WHERE val < 5
        EXCEPT
        SELECT val FROM set_ops_test WHERE val < 3
      `);
      expect(exceptRows.length).toBe(2);
      const exceptVals = exceptRows.map((r: any) => r.val).sort();
      expect(exceptVals).toEqual([3, 4]);
    });

    test("5.15 Set Operations - UNION, INTERSECT, EXCEPT and ALL variants", async () => {
      await db.exec(`CREATE TABLE set_ops_all (val INT)`);
      await db.exec(`INSERT INTO set_ops_all (val) VALUES (1), (1), (1), (2)`);
      await db.exec(`CREATE TABLE set_ops_all_right (val INT)`);
      await db.exec(`INSERT INTO set_ops_all_right (val) VALUES (1), (1), (3)`);

      // 1. UNION ALL (1,1,1,2 + 1,1,3)
      const unionAll = await db.query(`
        SELECT val FROM set_ops_all UNION ALL SELECT val FROM set_ops_all_right
      `);
      expect(unionAll.length).toBe(7);

      // 2. INTERSECT (Distinct intersection of {1,1,1,2} and {1,1,3}) -> {1}
      const intersectDistinct = await db.query(`
        SELECT val FROM set_ops_all INTERSECT SELECT val FROM set_ops_all_right
      `);
      expect(intersectDistinct.length).toBe(1);
      expect(intersectDistinct[0].val).toBe(1);

      // 3. INTERSECT ALL (Multiset intersection) -> {1, 1}
      // Left has three 1s, Right has two 1s. Intersection has min(3, 2) = 2.
      const intersectAll = await db.query(`
        SELECT val FROM set_ops_all INTERSECT ALL SELECT val FROM set_ops_all_right
      `);
      expect(intersectAll.length).toBe(2);
      expect(intersectAll.filter(r => r.val === 1).length).toBe(2);

      // 4. EXCEPT (Distinct difference) -> {2}
      // {1,1,1,2} - {1,1,3} -> remove all 1s from left that appear in right.
      const exceptDistinct = await db.query(`
        SELECT val FROM set_ops_all EXCEPT SELECT val FROM set_ops_all_right
      `);
      expect(exceptDistinct.length).toBe(1);
      expect(exceptDistinct[0].val).toBe(2);

      // 5. EXCEPT ALL (Multiset difference) -> {1, 2}
      // Left has three 1s, Right has two 1s. Result has max(3-2, 0) = 1 one.
      const exceptAll = await db.query(`
        SELECT val FROM set_ops_all EXCEPT ALL SELECT val FROM set_ops_all_right
      `);
      expect(exceptAll.length).toBe(2);
      const exceptAllVals = exceptAll.map(r => r.val).sort();
      expect(exceptAllVals).toEqual([1, 2]);
    });

    test("5.4 Complex catalog query with NOT IN and obj_description", async () => {
      await db.exec(`COMMENT ON TABLE users IS 'User record table'`);
      const sql = `
        SELECT 
          table_name,
          obj_description((QUOTE_IDENT(table_schema) || '.' || QUOTE_IDENT(table_name))::regclass) AS comment
        FROM information_schema.tables
        WHERE table_schema NOT IN ('information_schema', 'pg_catalog') 
          AND table_schema = 'public'
          AND table_name = 'users'
      `;
      const rows = await db.query(sql);
      expect(rows.length).toBe(1);
      expect(rows[0].table_name).toBe("users");
      expect(rows[0].comment).toBe("User record table");
    });

    test("5.5 Unary NOT and NOT LIKE", async () => {
      const rows = await db.query(
        `SELECT name FROM users WHERE NOT (age < 25) AND name NOT LIKE 'Eve%'`,
      );
      // Alice (26), Bob (30), Diana (28), David (40)
      expect(rows.length).toBe(4);
    });

    test("5.6 Complex catalog query with NOT IN and obj_description", async () => {
      const sql = `
        SELECT 
          table_schema AS schema_name,
          table_name,
          table_type,
          obj_description((QUOTE_IDENT(table_schema) || '.' || QUOTE_IDENT(table_name))::regclass) AS comment,
          CASE WHEN table_type = 'TEMPORARY' THEN true ELSE false END AS is_temporary
        FROM information_schema.tables
        WHERE table_schema NOT IN ('information_schema', 'pg_catalog') AND table_schema = 'public'
      `;
      const rows = await db.query(sql);
      expect(rows.length).toBeGreaterThanOrEqual(1); // Should at least find our testing tables in 'public'
      expect(rows[0].table_type).toBe("BASE TABLE");
    });

    test("5.7 Query information_schema.schemata", async () => {
      const sql = `
        SELECT schema_name 
        FROM information_schema.schemata
        WHERE schema_name NOT IN ('information_schema', 'pg_catalog')
          AND schema_name NOT LIKE 'pg_toast%'
          AND schema_name NOT LIKE 'pg_temp%';
      `;
      const rows = await db.query(sql);
      expect(rows.length).toBeGreaterThanOrEqual(1);
      expect(rows.map((r: any) => r.schema_name)).toContain("public");
    });

    test("5.8 Query pg_namespace", async () => {
      const sql = `
        SELECT nspname AS schema_name
        FROM pg_namespace;
      `;
      const rows = await db.query(sql);
      expect(rows.length).toBeGreaterThanOrEqual(3);
      const schemas = rows.map((r: any) => r.schema_name);
      expect(schemas).toContain("public");
      expect(schemas).toContain("pg_catalog");
      expect(schemas).toContain("information_schema");
    });

    test("5.9 Query pg_constraint", async () => {
      const sql = `
        SELECT conname, contype 
        FROM pg_constraint 
        WHERE conrelid = (SELECT oid FROM pg_class WHERE relname = 'users')
      `;
      const rows = await db.query(sql);
      expect(rows.length).toBeGreaterThanOrEqual(1);
      const types = rows.map((r: any) => r.contype);
      expect(types).toContain("p");
    });

    test("5.10 Query pg_constraint for foreign keys and unique", async () => {
      await db.exec(`CREATE TABLE ref_target (id SERIAL PRIMARY KEY)`);
      await db.exec(
        `CREATE TABLE ref_source (id SERIAL PRIMARY KEY, target_id INTEGER REFERENCES ref_target(id), uniq_col TEXT UNIQUE)`,
      );

      const sql = `
        SELECT conname, contype 
        FROM pg_constraint 
        WHERE conrelid = (SELECT oid FROM pg_class WHERE relname = 'ref_source')
        ORDER BY contype
      `;
      const rows = await db.query(sql);
      expect(rows.length).toBe(3);

      const types = rows.map((r: any) => r.contype);
      expect(types).toContain("p");
      expect(types).toContain("f");
      expect(types).toContain("u");
    });

    test("5.11 Query pg_attribute for table columns", async () => {
      const sql = `
        SELECT attname, attnum, attnotnull
        FROM pg_attribute 
        WHERE attrelid = (SELECT oid FROM pg_class WHERE relname = 'users')
        ORDER BY attnum;
      `;
      const rows = await db.query(sql);
      expect(rows.length).toBeGreaterThanOrEqual(1);

      const names = rows.map((r: any) => r.attname);
      expect(names).toContain("id");
      expect(names).toContain("name");
      expect(names).toContain("age");

      const attnums = rows.map((r: any) => r.attnum);
      expect(attnums).toContain(1);
      expect(attnums).toContain(2);
      expect(attnums).toContain(3);

      const idCol = rows.find((r: any) => r.attname === "id");
      expect(idCol.attnotnull).toBe(true);
    });

    test("5.12 col_description function", async () => {
      await db.exec(
        `CREATE TABLE comment_test (id SERIAL PRIMARY KEY, note TEXT)`,
      );
      await db.exec(
        `COMMENT ON COLUMN comment_test.note IS 'This is a note column'`,
      );

      const sql = `
        SELECT 
          col_description((QUOTE_IDENT(table_schema) || '.' || QUOTE_IDENT(table_name))::regclass, ordinal_position) as comment
        FROM information_schema.columns
        WHERE table_name = 'comment_test' AND column_name = 'note'
      `;
      const rows = await db.query(sql);
      expect(rows.length).toBe(1);
      expect(rows[0].comment).toBe("This is a note column");
    });

    test("5.14 Regression: Introspection query with quoted regclass cast", async () => {
      await db.exec(`CREATE TABLE public.introspection_test (id SERIAL PRIMARY KEY)`);
      await db.exec(`COMMENT ON TABLE public.introspection_test IS 'Introspection comment'`);

      const sql = `
        SELECT 
          table_schema AS schema_name,
          table_name,
          table_type,
          obj_description(('"' || table_schema || '"."' || table_name || '"')::regclass) AS comment,
          CASE WHEN table_type = 'TEMPORARY' THEN true ELSE false END AS is_temporary
        FROM information_schema.tables
        WHERE table_schema = 'public' AND table_name = 'introspection_test'
      `;
      
      const rows = await db.query(sql);
      expect(rows.length).toBe(1);
      expect(rows[0].schema_name).toBe("public");
      expect(rows[0].comment).toBe("Introspection comment");
    });

    test("5.13 Advanced col_description with JSON comments and complex casting", async () => {
      // 1. Setup table
      await db.exec(`CREATE TABLE users2 (
        id SERIAL PRIMARY KEY,
        username TEXT,
        password TEXT,
        email TEXT,
        created_at TIMESTAMP,
        zalo_id TEXT,
        phone_number TEXT,
        updated_at TIMESTAMP,
        deleted_at TIMESTAMP
      )`);

      // 2. Apply the specific comments provided by the user
      const comments = [
        `COMMENT ON TABLE users IS '{"label": "Người dùng", "description": "Bảng lưu trữ thông tin người dùng"}';`,
        `COMMENT ON COLUMN users2.username IS '{"label": "Tên người dùng", "type": "short_text", "required": true, "description": "Tên người dùng duy nhất", "visible": true}';`,
        `COMMENT ON COLUMN users2.password IS '{"label": "Mật khẩu", "type": "password", "required": true, "description": "Mật khẩu của người dùng", "visible": false}';`,
        `COMMENT ON COLUMN users2.email IS '{"label": "Email", "type": "email", "required": true, "description": "Địa chỉ email của người dùng", "visible": true}';`,
        `COMMENT ON COLUMN users2.created_at IS '{"label": "Thời gian tạo", "type": "datetime", "required": true, "description": "Thời điểm người dùng được tạo", "visible": false}';`,
        `COMMENT ON COLUMN users2.zalo_id IS '{"label": "Zalo ID", "type": "short_text", "required": false, "description": "ID Zalo của người dùng", "visible": true}';`,
        `COMMENT ON COLUMN users2.phone_number IS '{"label": "Số điện thoại", "type": "short_text", "required": false, "description": "Số điện thoại của người dùng", "visible": true}';`,
        `COMMENT ON COLUMN users2.updated_at IS '{"label": "Thời gian cập nhật", "type": "datetime", "required": true, "description": "Thời điểm người dùng được cập nhật lần cuối", "visible": false}';`,
        `COMMENT ON COLUMN users2.deleted_at IS '{"label": "Thời gian xóa", "type": "datetime", "required": false, "description": "Thời điểm người dùng bị xóa", "visible": false}';`,
      ];

      await db.exec(comments.join("\n"));

      // 3. Execute the exact query provided by the user
      const sql = `
        SELECT 
          cols.table_schema AS schema_name,
          cols.table_name,
          cols.column_name,
          cols.data_type,
          cols.is_nullable,
          cols.column_default,
          NULL AS constraint_type,
          col_description(
            (quote_ident(cols.table_schema) || '.' || quote_ident(cols.table_name))::regclass::oid,
            cols.ordinal_position
          ) AS comment
        FROM information_schema.columns cols
        WHERE cols.table_schema = $1
          AND cols.table_schema NOT IN ('information_schema', 'pg_catalog')
          AND cols.table_name = $2
        ORDER BY cols.table_name, cols.ordinal_position
      `;

      const rows = await db.query(sql, ["public", "users2"]);

      const allColumns = await db.query(
        `SELECT 
        *,
        col_description(
            (quote_ident(cols.table_schema) || '.' || quote_ident(cols.table_name))::regclass::oid,
            cols.ordinal_position
          ) AS comment 
           FROM information_schema.columns cols 
           WHERE cols.table_schema = 'public' AND cols.table_name = 'users2'
           `,
      );
      console.log("USERS", allColumns);

      // 4. Verify results
      const usernameCol = rows.find((r) => r.column_name === "username");
      expect(usernameCol).toBeDefined();
      expect(usernameCol.comment).toContain('"label": "Tên người dùng"');
      expect(usernameCol.is_nullable).toBe("YES"); // SERIAL/PK handling might make id 'NO' but others 'YES'

      const emailCol = rows.find((r) => r.column_name === "email");
      expect(emailCol.comment).toContain("Địa chỉ email");
    });
  });

  describe("LEVEL 7: Schema Operations", () => {
    test("7.1 CREATE SCHEMA", async () => {
      const res = await db.exec(`CREATE SCHEMA analytics`);
      expect(res.success).toBe(true);

      const res2 = await db.exec(`CREATE SCHEMA IF NOT EXISTS analytics`);
      expect(res2.success).toBe(true); // Should not throw because of IF NOT EXISTS
    });

    test("7.2 CREATE TABLE inside specific schema", async () => {
      const res = await db.exec(
        `CREATE TABLE analytics.metrics (id SERIAL PRIMARY KEY, name TEXT, value NUMBER)`,
      );
      expect(res.success).toBe(true);
    });

    test("7.3 INSERT and SELECT across schemas", async () => {
      await db.exec(
        `INSERT INTO analytics.metrics (name, value) VALUES ('visitors', 1500)`,
      );
      await db.exec(
        `INSERT INTO analytics.metrics (name, value) VALUES ('bounces', 300)`,
      );

      const rows = await db.query(
        `SELECT * FROM analytics.metrics WHERE value > 500`,
      );
      expect(rows.length).toBe(1);
      expect(rows[0].name).toBe("visitors");
    });

    test("7.4 DROP SCHEMA RESTRICT (should fail if tables exist)", async () => {
      expect(async () => {
        await db.exec(`DROP SCHEMA analytics RESTRICT`);
      }).toThrow();
    });

    test("7.5 DROP TABLE and DROP SCHEMA", async () => {
      const dropTableRes = await db.exec(
        `DROP TABLE IF EXISTS analytics.metrics`,
      );
      expect(dropTableRes.success).toBe(true);

      const dropSchemaRes = await db.exec(`DROP SCHEMA analytics`);
      expect(dropSchemaRes.success).toBe(true);

      expect(async () => {
        await db.query(`SELECT * FROM analytics.metrics`);
      }).toThrow();
    });

    test("7.6 Default schema is public", async () => {
      await db.exec(`CREATE TABLE public.test_public (id SERIAL PRIMARY KEY)`);
      await db.exec(`INSERT INTO test_public (id) VALUES (1)`); // Defaults to public.test_public

      const rows = await db.query(`SELECT * FROM public.test_public`);
      expect(rows.length).toBe(1);
    });
  });

  describe("LEVEL 28: LIKE Escape and NOT LIKE with underscore", () => {
    test("28.1 NOT LIKE with escaped underscore filters correctly", async () => {
      await db.exec(`CREATE TABLE _hidden_table (id SERIAL PRIMARY KEY, val TEXT)`);
      await db.exec(`CREATE TABLE visible_table (id SERIAL PRIMARY KEY, val TEXT)`);

      // Using escaped underscore to match literal underscore prefix
      const rows = await db.query(`
        SELECT table_name 
        FROM information_schema.tables
        WHERE table_schema = 'public'
          AND table_name NOT LIKE '\\_%%'
        ORDER BY table_name
      `);
      // _hidden_table should be excluded, visible_table should be included
      const names = rows.map((r: any) => r.table_name);
      expect(names).not.toContain("_hidden_table");
      expect(names).toContain("visible_table");
    });

    test("28.2 LIKE with underscore wildcard", async () => {
      const rows = await db.query(`SELECT 'abc' LIKE '_bc' as res`);
      expect(rows[0].res).toBe(true);
    });

    test("28.3 LIKE with escaped underscore literal", async () => {
      const rows = await db.query(`SELECT '_bc' LIKE '\\_bc' as res`);
      expect(rows[0].res).toBe(true);

      const rows2 = await db.query(`SELECT 'abc' LIKE '\\_bc' as res`);
      expect(rows2[0].res).toBe(false);
    });

    test("28.4 information_schema.columns query with NOT LIKE escaped underscore", async () => {
      const rows = await db.query(`
        SELECT table_name, column_name, data_type, is_nullable, column_default, ordinal_position
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name NOT LIKE '\\_%%'
        ORDER BY table_name, ordinal_position
      `);
      expect(rows.length).toBeGreaterThan(0);
      // No table starting with underscore should appear
      for (const r of rows) {
        expect(r.table_name.startsWith('_')).toBe(false);
      }
    });
  });

  describe("LEVEL 21: Regex, JSON, Array Operators & Interval", () => {
    test("21.1 Regex Operators (~, ~*, !~)", async () => {
      const rows = await db.query(`
        SELECT 
          'apple' ~ 'a.p' as res1,
          'APPLE' ~* 'apple' as res2,
          'banana' !~ 'apple' as res3
      `);
      expect(rows[0].res1).toBe(true);
      expect(rows[0].res2).toBe(true);
      expect(rows[0].res3).toBe(true);
    });

    test("21.2 JSON Operators (->, ->>, #>, @>, ?)", async () => {
      const rows = await db.query(`
        SELECT 
          '{"a": 1, "b": {"c": 2}}'::json -> 'b' as res1,
          '{"a": 1, "b": 2}'::json ->> 'a' as res2,
          '{"a": [1, 2, 3]}'::json #> ARRAY['a', 1] as res3,
          '{"a": 1, "b": 2}'::json @> '{"a": 1}'::json as res4,
          '{"a": 1, "b": 2}'::json ? 'a' as res5
      `);
      expect(rows[0].res1).toEqual({ c: 2 });
      expect(rows[0].res2).toBe("1");
      expect(rows[0].res3).toBe(2);
      expect(rows[0].res4).toBe(true);
      expect(rows[0].res5).toBe(true);
    });

    test("21.2.1 Advanced JSON Functions", async () => {
      const rows = await db.query(`
        SELECT 
          JSONB_BUILD_OBJECT('name', 'Alice', 'age', 30) as obj,
          JSONB_BUILD_ARRAY(1, 'two', false) as arr,
          JSONB_SET('{"a": 1}'::json, ARRAY['b'], '2'::json) as set1,
          JSONB_SET('{"a": 1}'::json, ARRAY['a'], '10'::json) as set2,
          JSONB_TYPEOF('{"a": 1}'::json) as t1,
          JSONB_TYPEOF('[1,2]'::json) as t2,
          JSONB_STRIP_NULLS('{"a": 1, "b": null}'::json) as strip
      `);
      expect(rows[0].obj).toEqual({ name: 'Alice', age: 30 });
      expect(rows[0].arr).toEqual([1, 'two', false]);
      expect(rows[0].set1).toEqual({ a: 1, b: 2 });
      expect(rows[0].set2).toEqual({ a: 10 });
      expect(rows[0].t1).toBe('object');
      expect(rows[0].t2).toBe('array');
      expect(rows[0].strip).toEqual({ a: 1 });
    });

    test("21.2.2 JSON Aggregates", async () => {
      await db.exec(`CREATE TABLE json_agg_test (id INT, val TEXT)`);
      await db.exec(`INSERT INTO json_agg_test VALUES (1, 'A'), (1, 'B'), (2, 'C')`);
      
      const rows = await db.query(`
        SELECT 
          id, 
          JSONB_AGG(val) as vals,
          JSONB_OBJECT_AGG(val, val || '_suff') as obj
        FROM json_agg_test 
        GROUP BY id
        ORDER BY id
      `);
      
      expect(rows.length).toBe(2);
      expect(rows[0].id).toBe(1);
      expect(rows[0].vals).toEqual(['A', 'B']);
      expect(rows[0].obj).toEqual({ 'A': 'A_suff', 'B': 'B_suff' });
      expect(rows[1].id).toBe(2);
      expect(rows[1].vals).toEqual(['C']);
      expect(rows[1].obj).toEqual({ 'C': 'C_suff' });
    });

    test("21.3 Array Operators (&&, @>)", async () => {
      const rows = await db.query(`
        SELECT 
          ARRAY[1, 2] && ARRAY[2, 3] as res1,
          ARRAY[1, 2, 3] @> ARRAY[1, 2] as res2,
          ARRAY[1, 2] && ARRAY[3, 4] as res3
      `);
      expect(rows[0].res1).toBe(true);
      expect(rows[0].res2).toBe(true);
      expect(rows[0].res3).toBe(false);
    });

    test("21.4 INTERVAL Syntax & Arithmetic", async () => {
      const rows = await db.query(`
        SELECT 
          '2024-01-01'::timestamp + INTERVAL '1 day' as res1,
          '2024-01-02'::timestamp - INTERVAL '2 days' as res2,
          '2024-01-01'::timestamp + INTERVAL '1 month' as res3
      `);
      expect(rows[0].res1).toContain("2024-01-02");
      expect(rows[0].res2).toContain("2023-12-31");
      expect(rows[0].res3).toContain("2024-02-01");
    });
  });

  describe("LEVEL 13: Type Casting", () => {
    test("13.1 Cast string to integer with :: syntax", async () => {
      const rows = await db.query("SELECT '100'::int + 5 as result");
      expect(rows[0].result).toBe(105);
    });

    test("13.2 Cast boolean to string", async () => {
      const rows = await db.query("SELECT true::text as result");
      expect(rows[0].result).toBe("true");
    });

    test("13.3 Cast string to boolean", async () => {
      const rows = await db.query(
        "SELECT 'true'::boolean as result1, '0'::boolean as result2",
      );
      expect(rows[0].result1).toBe(true);
      expect(rows[0].result2).toBe(false);
    });

    test("13.4 Cast using CAST(expr AS type) syntax", async () => {
      const rows = await db.query("SELECT CAST('200' AS INT) * 2 as result");
      expect(rows[0].result).toBe(400);
    });

    test("13.5 Multiple cascaded casts", async () => {
      const rows = await db.query("SELECT '300'::int::text as result");
      expect(rows[0].result).toBe("300");
    });

    test("13.6 Cast string to regnamespace", async () => {
      const rows = await db.query("SELECT 'public'::regnamespace as oid");
      expect(rows[0].oid).toBe(2200);
      
      const rows2 = await db.query("SELECT 'pg_catalog'::regnamespace as oid");
      expect(rows2[0].oid).toBe(11);
    });
  });

  describe("LEVEL 12: Built-in Functions", () => {
    test("12.1 NOW()", async () => {
      const rows = await db.query("SELECT NOW() as current_time");
      expect(rows[0].current_time).toBeDefined();
      expect(
        new Date(rows[0].current_time).getFullYear(),
      ).toBeGreaterThanOrEqual(2024);
    });

    test("12.2 UPPER()", async () => {
      const rows = await db.query("SELECT UPPER('hello') as val");
      expect(rows[0].val).toBe("HELLO");
    });

    test("12.3 COALESCE()", async () => {
      const rows = await db.query(
        "SELECT COALESCE(NULL, NULL, 'first_non_null', 'second') as val",
      );
      expect(rows[0].val).toBe("first_non_null");
    });

    test("12.4 JSON_EXTRACT()", async () => {
      const rows = await db.query(
        "SELECT JSON_EXTRACT('{\"a\": {\"b\": 123}}', 'a', 'b') as val",
      );
      expect(rows[0].val).toBe(123);
    });

    test("12.5 DATE_TRUNC()", async () => {
      const rows = await db.query(
        "SELECT DATE_TRUNC('year', '2024-05-15 10:20:30') as val",
      );
      expect(rows[0].val).toContain("2024-01-01T00:00:00");
    });

    test("12.6 QUOTE_IDENT()", async () => {
      const rows = await db.query(`
        SELECT 
          QUOTE_IDENT('my table') as res1, 
          QUOTE_IDENT('my "table"') as res2, 
          QUOTE_IDENT(NULL) as res3,
          QUOTE_IDENT('schema') || '.' || QUOTE_IDENT('table') as res4
      `);
      expect(rows[0].res1).toBe('"my table"');
      expect(rows[0].res2).toBe('"my ""table"""');
      expect(rows[0].res3).toBeNull();
      expect(rows[0].res4).toBe('"schema"."table"');
    });
  });

  describe("LEVEL 8: Comments & Unicode Support", () => {
    test("8.1 Skip single-line comments with Vietnamese characters", async () => {
      const sql = `
        -- Đây là bình luận tiếng Việt với ký tự đ
        SELECT 1 as value;
      `;
      const rows = await db.query(sql);
      expect(rows.length).toBe(1);
      expect(rows[0].value).toBe(1);
    });

    test("8.2 Skip multi-line comments", async () => {
      const sql = `
        /* 
           Multi-line comment 
           with đ character
        */
        SELECT 2 as value;
      `;
      const rows = await db.query(sql);
      expect(rows.length).toBe(1);
      expect(rows[0].value).toBe(2);
    });

    test("8.3 Non-ASCII characters in strings and quoted identifiers", async () => {
      // Create table with Unicode name and columns
      await db.exec(`CREATE TABLE "Dữ Liệu" ("Mô Tả" TEXT)`);
      await db.exec(`INSERT INTO "Dữ Liệu" ("Mô Tả") VALUES ('Điểm danh')`);
      const rows = await db.query(`SELECT "Mô Tả" FROM "Dữ Liệu"`);
      expect(rows.length).toBe(1);
      expect(rows[0]["Mô Tả"]).toBe("Điểm danh");
    });
  });

  describe("LEVEL 9: RETURNING Clause", () => {
    test("9.1 INSERT ... RETURNING *", async () => {
      await db.exec(
        `CREATE TABLE returning_test (id SERIAL PRIMARY KEY, val TEXT)`,
      );
      const res = await db.query(
        `INSERT INTO returning_test (val) VALUES ('test1') RETURNING *`,
      );
      expect(res.length).toBe(1);
      expect(res[0].id).toBe(1);
      expect(res[0].val).toBe("test1");
    });

    test("9.2 INSERT ... RETURNING specific columns", async () => {
      const res = await db.query(
        `INSERT INTO returning_test (val) VALUES ('test2') RETURNING val, id AS my_id`,
      );
      expect(res.length).toBe(1);
      expect(res[0].val).toBe("test2");
      expect(res[0].my_id).toBe(2);
    });

    test("9.3 UPDATE ... RETURNING", async () => {
      const res = await db.query(
        `UPDATE returning_test SET val = 'updated' WHERE id = 1 RETURNING id, val`,
      );
      expect(res.length).toBe(1);
      expect(res[0].id).toBe(1);
      expect(res[0].val).toBe("updated");
    });

    test("9.4 DELETE ... RETURNING", async () => {
      const res = await db.query(
        `DELETE FROM returning_test WHERE id = 2 RETURNING *`,
      );
      expect(res.length).toBe(1);
      expect(res[0].id).toBe(2);
      expect(res[0].val).toBe("test2");

      const count = await db.query(`SELECT COUNT(*) as c FROM returning_test`);
      expect(count[0].c).toBe(1);
    });

    test("9.5 UPDATE multiple rows with RETURNING", async () => {
      await db.exec(`INSERT INTO returning_test (val) VALUES ('a')`);
      await db.exec(`INSERT INTO returning_test (val) VALUES ('b')`);
      const res = await db.query(
        `UPDATE returning_test SET val = 'x' RETURNING val`,
      );
      // We had 1 row ('updated') + 2 new rows = 3 rows total.
      expect(res.length).toBe(3);
      expect(res.every((r) => r.val === "x")).toBe(true);
    });
  });

  describe("LEVEL 11: Window Functions", () => {
    beforeAll(async () => {
      await db.exec(
        `CREATE TABLE employees (id SERIAL PRIMARY KEY, name TEXT, department TEXT, salary NUMBER)`,
      );
      await db.exec(
        `INSERT INTO employees (name, department, salary) VALUES ('Alice', 'IT', 5000)`,
      );
      await db.exec(
        `INSERT INTO employees (name, department, salary) VALUES ('Bob', 'IT', 6000)`,
      );
      await db.exec(
        `INSERT INTO employees (name, department, salary) VALUES ('Charlie', 'HR', 4500)`,
      );
      await db.exec(
        `INSERT INTO employees (name, department, salary) VALUES ('David', 'IT', 5000)`,
      );
      await db.exec(
        `INSERT INTO employees (name, department, salary) VALUES ('Eve', 'HR', 4500)`,
      );
    });

    test("11.1 ROW_NUMBER() OVER (ORDER BY ...)", async () => {
      const rows = await db.query(`
        SELECT name, salary, ROW_NUMBER() OVER (ORDER BY salary DESC, name ASC) as rn 
        FROM employees
      `);
      expect(rows[0].rn).toBe(1);
      expect(rows[0].name).toBe("Bob");
      expect(rows[1].rn).toBe(2);
      expect(rows[1].salary).toBe(5000);
    });

    test("11.2 RANK() OVER (PARTITION BY ... ORDER BY ...)", async () => {
      const rows = await db.query(`
        SELECT name, department, salary, RANK() OVER (PARTITION BY department ORDER BY salary DESC) as rnk
        FROM employees
        ORDER BY department, rnk
      `);

      const itRows = rows.filter((r) => r.department === "IT");
      expect(itRows[0].name).toBe("Bob");
      expect(itRows[0].rnk).toBe(1);
      expect(itRows[1].salary).toBe(5000);
      expect(itRows[1].rnk).toBe(2);
      expect(itRows[2].rnk).toBe(2); // Tie

      const hrRows = rows.filter((r) => r.department === "HR");
      expect(hrRows[0].rnk).toBe(1);
      expect(hrRows[1].rnk).toBe(1); // Tie
    });

    test("11.3 LEAD() and LAG()", async () => {
      const rows = await db.query(`
        SELECT 
          name, 
          salary,
          LAG(salary) OVER (ORDER BY salary ASC) as prev_salary,
          LEAD(salary) OVER (ORDER BY salary ASC) as next_salary
        FROM employees
        WHERE department = 'IT'
        ORDER BY salary ASC
      `);
      // IT salaries: 5000 (Alice), 5000 (David), 6000 (Bob)
      expect(rows.length).toBe(3);
      
      // Alice (5000)
      expect(rows[0].prev_salary).toBeNull();
      expect(rows[0].next_salary).toBe(5000);
      
      // David (5000)
      expect(rows[1].prev_salary).toBe(5000);
      expect(rows[1].next_salary).toBe(6000);
      
      // Bob (6000)
      expect(rows[2].prev_salary).toBe(5000);
      expect(rows[2].next_salary).toBeNull();
    });

    test("11.4 DENSE_RANK() OVER (PARTITION BY ... ORDER BY ...)", async () => {
      const rows = await db.query(`
        SELECT name, department, salary, DENSE_RANK() OVER (PARTITION BY department ORDER BY salary DESC) as drnk
        FROM employees
        ORDER BY department, drnk
      `);

      const itRows = rows.filter((r) => r.department === "IT");
      // IT salaries: Bob (6000), Alice (5000), David (5000)
      expect(itRows[0].name).toBe("Bob");
      expect(itRows[0].drnk).toBe(1);
      expect(itRows[1].salary).toBe(5000);
      expect(itRows[1].drnk).toBe(2);
      expect(itRows[2].drnk).toBe(2); // Same rank for same salary

      const hrRows = rows.filter((r) => r.department === "HR");
      // HR salaries: Charlie (4500), Eve (4500)
      expect(hrRows[0].drnk).toBe(1);
      expect(hrRows[1].drnk).toBe(1);
    });

    test("11.5 LEAD() with offset and default", async () => {
      const rows = await db.query(`
        SELECT 
          name,
          LEAD(name, 2, 'N/A') OVER (ORDER BY name ASC) as lead_two
        FROM employees
        ORDER BY name ASC
      `);
      // Names: Alice, Bob, Charlie, David, Eve
      expect(rows[0].name).toBe('Alice');
      expect(rows[0].lead_two).toBe('Charlie');
      
      expect(rows[3].name).toBe('David');
      expect(rows[3].lead_two).toBe('N/A');
    });

    test("11.5 FIRST_VALUE() and LAST_VALUE()", async () => {
      const rows = await db.query(`
        SELECT 
          name, 
          department,
          salary,
          FIRST_VALUE(name) OVER (PARTITION BY department ORDER BY salary ASC) as first_emp,
          LAST_VALUE(name) OVER (PARTITION BY department ORDER BY salary ASC) as last_emp
        FROM employees
        ORDER BY department, salary
      `);

      // HR department: Charlie (4500), Eve (4500)
      const hrRows = rows.filter(r => r.department === 'HR');
      expect(hrRows[0].first_emp).toBe('Charlie');
      expect(hrRows[0].last_emp).toBe('Eve');
      expect(hrRows[1].first_emp).toBe('Charlie');
      expect(hrRows[1].last_emp).toBe('Eve');

      // IT department: Alice (5000), David (5000), Bob (6000)
      const itRows = rows.filter(r => r.department === 'IT');
      expect(itRows[0].first_emp).toBe('Alice');
      expect(itRows[0].last_emp).toBe('Bob');
      expect(itRows[2].first_emp).toBe('Alice');
      expect(itRows[2].last_emp).toBe('Bob');
    });
  });

  describe("LEVEL 14: Multiple Row Inserts and Advanced Schema Definitions", () => {
    test("14.1 Create table with complex constraints and references", async () => {
      await db.exec(`CREATE TABLE parent_users (id SERIAL PRIMARY KEY)`);
      const sql = `
        CREATE TABLE IF NOT EXISTS news_articles (
            id SERIAL PRIMARY KEY,
            title TEXT NOT NULL,
            content TEXT NOT NULL,
            thumbnail_url TEXT,
            author_id INTEGER REFERENCES parent_users(id) ON UPDATE CASCADE ON DELETE SET NULL,
            category VARCHAR(100),
            published_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
            created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
        );
      `;
      const res = await db.exec(sql);
      expect(res.success).toBe(true);
    });

    test("14.2 Insert multiple rows in a single statement", async () => {
      const sql = `
        INSERT INTO news_articles (title, content, category) VALUES 
        ('Khám phá AI', 'Nội dung 1', 'Công nghệ'),
        ('Thiết kế 2024', 'Nội dung 2', 'Thiết kế'),
        ('Bảo mật', 'Nội dung 3', 'Hệ thống');
      `;
      const res = await db.exec(sql);
      expect(res.success).toBe(true);
      expect(res.inserted).toBeDefined();

      const rows = await db.query(`SELECT * FROM news_articles`);
      expect(rows.length).toBe(3);
      expect(rows[0].title).toBe("Khám phá AI");
      expect(rows[2].category).toBe("Hệ thống");
    });

    test("14.3 Add Comments to table and columns", async () => {
      await db.exec(
        `COMMENT ON TABLE news_articles IS '{ "label": "Tin tức" }'`,
      );
      await db.exec(
        `COMMENT ON COLUMN news_articles.title IS '{ "label": "Tiêu đề" }'`,
      );

      const rows = await db.query(`
        SELECT table_name, column_name, column_comment 
        FROM information_schema.columns 
        WHERE table_name = 'news_articles' AND column_name = 'title'
      `);
      expect(rows.length).toBe(1);
      expect(rows[0].column_comment).toContain("Tiêu đề");
    });
  });

  describe("LEVEL 34: Aggregate SUM, MIN, MAX and COUNT(DISTINCT)", () => {
    test("34.1 Advanced aggregations", async () => {
      await db.exec(`CREATE TABLE orders_34 (id SERIAL PRIMARY KEY, user_id INTEGER, total_amount NUMBER, status TEXT)`);
      await db.exec(`INSERT INTO orders_34 (user_id, total_amount, status) VALUES 
        (1, 100, 'completed'),
        (1, 150, 'completed'),
        (2, 200, 'completed'),
        (2, 50, 'pending'),
        (3, 300, 'completed'),
        (3, 300, 'completed')
      `);

      const rows = await db.query(`
        SELECT 
          COUNT(*) AS total_orders, 
          SUM(total_amount) AS total_revenue, 
          MIN(total_amount) AS min_amount,
          MAX(total_amount) AS max_amount,
          COUNT(DISTINCT user_id) AS customers_with_orders 
        FROM orders_34 
        WHERE status = 'completed' 
        LIMIT 1000
      `);

      expect(rows.length).toBe(1);
      expect(rows[0].total_orders).toBe(5); // 1, 1, 2, 3, 3 are completed
      expect(rows[0].total_revenue).toBe(1050); // 100 + 150 + 200 + 300 + 300
      expect(rows[0].min_amount).toBe(100);
      expect(rows[0].max_amount).toBe(300);
      expect(rows[0].customers_with_orders).toBe(3); // users 1, 2, 3
    });

    test("34.2 COUNT with nulls", async () => {
      await db.exec(`CREATE TABLE null_counts (id SERIAL PRIMARY KEY, val TEXT)`);
      await db.exec(`INSERT INTO null_counts (val) VALUES ('a'), (NULL), ('b'), ('a')`);
      
      const rows = await db.query(`
        SELECT 
          COUNT(*) AS total,
          COUNT(val) AS count_val,
          COUNT(DISTINCT val) AS distinct_val
        FROM null_counts
      `);

      expect(rows[0].total).toBe(4);
      expect(rows[0].count_val).toBe(3); // 'a', 'b', 'a'
      expect(rows[0].distinct_val).toBe(2); // 'a', 'b'
    });
  });

  describe("LEVEL 16: Aggregate FILTER", () => {
    beforeAll(async () => {
      await db.exec(
        `CREATE TABLE sales (id SERIAL PRIMARY KEY, category TEXT, amount NUMBER)`,
      );
      await db.exec(`INSERT INTO sales (category, amount) VALUES ('A', 100)`);
      await db.exec(`INSERT INTO sales (category, amount) VALUES ('A', 200)`);
      await db.exec(`INSERT INTO sales (category, amount) VALUES ('B', 300)`);
      await db.exec(`INSERT INTO sales (category, amount) VALUES ('B', 400)`);
      await db.exec(`INSERT INTO sales (category, amount) VALUES ('A', 500)`);
    });

    test("16.1 COUNT with FILTER", async () => {
      const sql = `
        SELECT 
          COUNT(*) AS total_count,
          COUNT(*) FILTER (WHERE category = 'A') AS count_a,
          COUNT(*) FILTER (WHERE amount >= 300) AS count_high
        FROM sales
      `;
      const rows = await db.query(sql);
      expect(rows[0].total_count).toBe(5);
      expect(rows[0].count_a).toBe(3);
      expect(rows[0].count_high).toBe(3); // 300, 400, 500
    });

    test("16.2 AVG and ARRAY_AGG with FILTER", async () => {
      const sql = `
        SELECT 
          AVG(amount) FILTER (WHERE category = 'A') AS avg_a,
          ARRAY_AGG(amount) FILTER (WHERE category = 'B') AS arr_b
        FROM sales
      `;
      const rows = await db.query(sql);
      expect(rows[0].avg_a).toBe((100 + 200 + 500) / 3);
      expect(rows[0].arr_b).toEqual([300, 400]);
    });
  });

  describe("LEVEL 18: Complex Introspection", () => {
    test("18.3 Advanced Table Introspection Query (DBeaver/TablePlus style)", async () => {
      await db.exec(`CREATE TABLE public.intro_target (id SERIAL PRIMARY KEY, val TEXT DEFAULT 'abc')`);
      await db.exec(`COMMENT ON COLUMN public.intro_target.val IS 'Test comment'`);
      
      const sql = `
        SELECT 
          n.nspname AS schema_name,
          c.relname AS table_name,
          a.attname AS column_name,
          format_type(a.atttypid, a.atttypmod) AS data_type,
          CASE WHEN a.attnotnull THEN 'NO' ELSE 'YES' END AS is_nullable,
          pg_get_expr(ad.adbin, ad.adrelid) AS column_default,
          d.description AS comment
        FROM pg_attribute a
        JOIN pg_class c ON a.attrelid = c.oid
        JOIN pg_namespace n ON c.relnamespace = n.oid
        LEFT JOIN pg_description d ON d.objoid = c.oid AND d.objsubid = a.attnum
        LEFT JOIN pg_attrdef ad ON a.attrelid = ad.adrelid AND a.attnum = ad.adnum
        WHERE n.nspname = $1
          AND c.relkind = 'r' -- Chỉ lấy table (không lấy view/index)
          AND a.attnum > 0 
          AND NOT a.attisdropped
        ORDER BY c.relname, a.attnum;
      `;
      
      const rows = await db.query(sql, ["public"]);
      expect(rows.length).toBeGreaterThan(0);
      
      const col = rows.find(r => r.table_name === 'intro_target' && r.column_name === 'val');
      expect(col).toBeDefined();
      expect(col.column_default).toContain('abc');
      expect(col.comment).toBe('Test comment');
    });

    test("18.1 Advanced pg_constraint join with LATERAL and FILTER", async () => {
      // Create some tables with constraints mapped to non-public schema
      await db.exec(`CREATE SCHEMA IF NOT EXISTS introspection_schema`);
      await db.exec(
        `CREATE TABLE introspection_schema.users (id SERIAL PRIMARY KEY, name TEXT)`,
      );
      await db.exec(
        `CREATE TABLE introspection_schema.posts (id SERIAL PRIMARY KEY, user_id INTEGER REFERENCES introspection_schema.users(id), title TEXT UNIQUE)`,
      );

      const sql = `
        SELECT
          nsp.nspname AS schema_name,
          rel.relname AS table_name,
          con.conname AS constraint_name,
          CASE con.contype
            WHEN 'f' THEN 'FOREIGN KEY'
            WHEN 'p' THEN 'PRIMARY KEY'
            WHEN 'u' THEN 'UNIQUE'
            WHEN 'c' THEN 'CHECK'
            ELSE con.contype::text
          END AS constraint_type,
          rnsp.nspname AS referenced_table_schema,
          rrel.relname AS referenced_table_name,
          ARRAY_AGG(att2.attname ORDER BY ucols.ordinality) FILTER (WHERE att2.attname IS NOT NULL) AS column_names,
          ARRAY_AGG(att1.attname ORDER BY rcols.ordinality) FILTER (WHERE att1.attname IS NOT NULL) AS referenced_columns
        FROM pg_constraint con
          INNER JOIN pg_class rel ON rel.oid = con.conrelid
          INNER JOIN pg_namespace nsp ON nsp.oid = rel.relnamespace
          LEFT JOIN pg_class rrel ON rrel.oid = con.confrelid
          LEFT JOIN pg_namespace rnsp ON rnsp.oid = rrel.relnamespace
          LEFT JOIN LATERAL unnest(con.conkey) WITH ORDINALITY AS ucols(attnum, ordinality) ON TRUE
          LEFT JOIN pg_attribute att2 ON att2.attnum = ucols.attnum AND att2.attrelid = con.conrelid
          LEFT JOIN LATERAL unnest(con.confkey) WITH ORDINALITY AS rcols(attnum, ordinality) ON TRUE
          LEFT JOIN pg_attribute att1 ON att1.attnum = rcols.attnum AND att1.attrelid = con.confrelid
        WHERE nsp.nspname = $1
          AND con.contype IN ('f', 'p', 'u')
        GROUP BY nsp.nspname, rel.relname, con.conname, con.contype, rnsp.nspname, rrel.relname
        ORDER BY rel.relname, constraint_type;
      `;

      const rows = await db.query(sql, ["introspection_schema"]);
      expect(rows.length).toBeGreaterThanOrEqual(3); // We have FK, PK, and Unique defined

      // Verify FOREIGN KEY behavior and resolution completeness
      const fk = rows.find(
        (r) => r.constraint_type === "FOREIGN KEY" && r.table_name === "posts",
      );
      expect(fk).toBeDefined();
      expect(fk.referenced_table_name).toBe("users");
      expect(fk.column_names).toEqual(["user_id"]);
      expect(fk.referenced_columns).toEqual(["id"]);

      // Verify UNIQUE isolation
      const uniq = rows.find(
        (r) => r.constraint_type === "UNIQUE" && r.table_name === "posts",
      );
      expect(uniq).toBeDefined();
      expect(uniq.column_names).toEqual(["title"]);

      // Verify PRIMARY KEY
      const pk = rows.find(
        (r) => r.constraint_type === "PRIMARY KEY" && r.table_name === "posts",
      );
      expect(pk).toBeDefined();
      expect(pk.column_names).toEqual(["id"]);
    });

    test("18.2 Query pg_attrdef system catalog", async () => {
      await db.exec(`CREATE TABLE attr_test (id SERIAL PRIMARY KEY, val TEXT DEFAULT 'hello')`);
      
      const sql = `
        SELECT adnum, adbin 
        FROM pg_attrdef 
        WHERE adrelid = (SELECT oid FROM pg_class WHERE relname = 'attr_test')
      `;
      const rows = await db.query(sql);
      expect(rows.length).toBe(1);
      expect(rows[0].adnum).toBe(2); // 'val' is second column
      expect(rows[0].adbin).toContain("hello");
    });

    test("18.4 Composite Primary Key introspection via pg_constraint", async () => {
      // In this engine, marking multiple columns as PRIMARY KEY results in a composite PK
      await db.exec(`CREATE TABLE composite_test (a INT PRIMARY KEY, b INT PRIMARY KEY)`);
      
      const sql = `
        SELECT conname, conkey 
        FROM pg_constraint 
        WHERE conrelid = (SELECT oid FROM pg_class WHERE relname = 'composite_test')
          AND contype = 'p'
      `;
      const rows = await db.query(sql);
      expect(rows.length).toBe(1);
      expect(rows[0].conkey).toEqual([1, 2]); // 'a' is attnum 1, 'b' is attnum 2
    });
  });

  describe("LEVEL 17: LATERAL Joins", () => {
    beforeAll(async () => {
      await db.exec(
        `CREATE TABLE departments (id SERIAL PRIMARY KEY, name TEXT)`,
      );
      await db.exec(
        `CREATE TABLE emp (id SERIAL PRIMARY KEY, dept_id NUMBER, name TEXT)`,
      );

      await db.exec(
        `INSERT INTO departments (name) VALUES ('Sales'), ('Marketing')`,
      );
      await db.exec(
        `INSERT INTO emp (dept_id, name) VALUES (1, 'Alice'), (1, 'Bob'), (2, 'Charlie')`,
      );
    });

    test("17.1 LEFT JOIN LATERAL function", async () => {
      const sql = `
        SELECT d.name as dname, e.val
        FROM departments d
        LEFT JOIN LATERAL unnest(ARRAY[d.name, d.name]) AS e(val) ON true
        WHERE d.id = 1
      `;
      const rows = await db.query(sql);
      expect(rows.length).toBe(2);
      expect(rows[0].val).toBe("Sales");
      expect(rows[1].val).toBe("Sales");
    });

    test("17.2 INNER JOIN LATERAL subquery", async () => {
      const sql = `
        SELECT d.name as dname, top_emp.name as ename
        FROM departments d
        JOIN LATERAL (
          SELECT name FROM emp WHERE dept_id = d.id ORDER BY id DESC LIMIT 1
        ) AS top_emp ON true
        ORDER BY d.id
      `;
      const rows = await db.query(sql);
      expect(rows.length).toBe(2);
      expect(rows[0].dname).toBe("Sales");
      expect(rows[0].ename).toBe("Bob"); // Bob is inserted after Alice so has a higher ID, DESC gets Bob first
      expect(rows[1].dname).toBe("Marketing");
      expect(rows[1].ename).toBe("Charlie");
    });

    test("17.3 LATERAL without ON clause defaults to TRUE", async () => {
      const sql = `
        SELECT d.name, t.v
        FROM departments d
        JOIN LATERAL unnest(ARRAY[1, 2]) AS t(v)
      `;
      const rows = await db.query(sql);
      expect(rows.length).toBe(4); // 2 departments * 2 values
    });
  });

  describe("LEVEL 15: Unnest and Ordinality", () => {
    test("15.1 basic unnest with array literal", async () => {
      const sql = `SELECT * FROM unnest(ARRAY['a', 'b', 'c']) AS t(val)`;
      const rows = await db.query(sql);
      expect(rows.length).toBe(3);
      expect(rows[0].val).toBe("a");
      expect(rows[2].val).toBe("c");
    });

    test("15.2 unnest WITH ORDINALITY", async () => {
      const sql = `SELECT val, ord FROM unnest(ARRAY['x', 'y']) WITH ORDINALITY AS t(val, ord)`;
      const rows = await db.query(sql);
      expect(rows.length).toBe(2);
      expect(rows[0].val).toBe("x");
      expect(rows[0].ord).toBe(1);
      expect(rows[1].val).toBe("y");
      expect(rows[1].ord).toBe(2);
    });

    test("15.3 unnest with numeric array and arithmetic", async () => {
      const sql = `SELECT item * 10 as calculation FROM unnest(ARRAY[1, 2, 3]) AS t(item)`;
      const rows = await db.query(sql);
      expect(rows[0].calculation).toBe(10);
      expect(rows[2].calculation).toBe(30);
    });
  });

  describe("LEVEL 10: ON CONFLICT", () => {
    beforeAll(async () => {
      await db.exec(
        `CREATE TABLE conflict_test (id SERIAL PRIMARY KEY, email TEXT UNIQUE, name TEXT)`,
      );
    });

    test("10.1 ON CONFLICT DO NOTHING", async () => {
      await db.exec(
        `INSERT INTO conflict_test (email, name) VALUES ('test@example.com', 'Original')`,
      );

      // Attempting to insert same email should not throw
      const res = await db.exec(`
        INSERT INTO conflict_test (email, name) 
        VALUES ('test@example.com', 'Conflict') 
        ON CONFLICT (email) DO NOTHING
      `);
      expect(res.success).toBe(true);

      const rows = await db.query(
        `SELECT name FROM conflict_test WHERE email = 'test@example.com'`,
      );
      expect(rows[0].name).toBe("Original");
    });

    test("10.2 ON CONFLICT DO UPDATE SET", async () => {
      const res = await db.exec(`
        INSERT INTO conflict_test (email, name) 
        VALUES ('test@example.com', 'Updated') 
        ON CONFLICT (email) DO UPDATE SET name = 'Updated'
      `);
      expect(res.success).toBe(true);

      const rows = await db.query(
        `SELECT name FROM conflict_test WHERE email = 'test@example.com'`,
      );
      expect(rows[0].name).toBe("Updated");
    });

    test("10.3 ON CONFLICT DO UPDATE with EXCLUDED table", async () => {
      await db.exec(`
        INSERT INTO conflict_test (email, name) 
        VALUES ('test@example.com', 'FromExcluded') 
        ON CONFLICT (email) DO UPDATE SET name = excluded.name
      `);

      const rows = await db.query(
        `SELECT name FROM conflict_test WHERE email = 'test@example.com'`,
      );
      expect(rows[0].name).toBe("FromExcluded");
    });

    test("10.4 ON CONFLICT with RETURNING", async () => {
      const res = await db.query(`
        INSERT INTO conflict_test (email, name) 
        VALUES ('test@example.com', 'ReturningVal') 
        ON CONFLICT (email) DO UPDATE SET name = excluded.name
        RETURNING name
      `);
      expect(res.length).toBe(1);
      expect(res[0].name).toBe("ReturningVal");
    });
  });

  describe("LEVEL 19: Extended Catalog Columns", () => {
    test("19.1 Check pg_class.relkind", async () => {
      const rows = await db.query(`
        SELECT relname, relkind 
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE c.relname = 'users' AND n.nspname = 'public'
      `);
      expect(rows.length).toBe(1);
      expect(rows[0].relkind).toBe('r');
    });

    test("19.2 Check pg_attribute.atttypmod and attisdropped", async () => {
      const rows = await db.query(`
        SELECT attname, atttypmod, attisdropped 
        FROM pg_attribute 
        WHERE attrelid = (
          SELECT c.oid 
          FROM pg_class c 
          JOIN pg_namespace n ON n.oid = c.relnamespace
          WHERE c.relname = 'users' AND n.nspname = 'public'
        )
        AND attname = 'id'
      `);
      expect(rows.length).toBe(1);
      expect(rows[0].atttypmod).toBe(-1);
      expect(rows[0].attisdropped).toBe(false);
    });

    test("19.3 Introspection query using relkind", async () => {
      const sql = `
        SELECT relname 
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE c.relkind = 'r' AND c.relname = 'posts' AND n.nspname = 'public'
      `;
      const rows = await db.query(sql);
      expect(rows.length).toBe(1);
      expect(rows[0].relname).toBe('posts');
    });
  });

  describe("LEVEL 20: System Functions (format_type, pg_get_expr)", () => {
    test("20.1 format_type returns type name", async () => {
      const sql = `
        SELECT format_type(atttypid, atttypmod) as type_name
        FROM pg_attribute
        WHERE attrelid = (SELECT oid FROM pg_class WHERE relname = 'users')
          AND attname = 'name'
      `;
      const rows = await db.query(sql);
      expect(rows.length).toBe(1);
      expect(rows[0].type_name).toBe("TEXT");
    });

    test("20.2 pg_get_expr decodes default value expressions", async () => {
      await db.exec(`CREATE TABLE expr_test (id SERIAL PRIMARY KEY, val TEXT DEFAULT 'my_default_val')`);
      const sql = `
        SELECT pg_get_expr(adbin, adrelid) as default_val
        FROM pg_attrdef
        WHERE adrelid = (SELECT oid FROM pg_class WHERE relname = 'expr_test')
          AND adnum = 2
      `;
      const rows = await db.query(sql);
      expect(rows.length).toBe(1);
      expect(rows[0].default_val).toBe("my_default_val");
    });

    test("20.3 pg_index introspection", async () => {
      await db.exec(`CREATE TABLE idx_info_test (id SERIAL PRIMARY KEY, email TEXT UNIQUE)`);
      
      const sql = `
        SELECT 
          idx.indisprimary, 
          idx.indisunique, 
          idx.indkey
        FROM pg_index idx
        JOIN pg_class cls ON idx.indrelid = cls.oid
        WHERE cls.relname = 'idx_info_test'
        ORDER BY idx.indisprimary DESC
      `;
      const rows = await db.query(sql);
      expect(rows.length).toBe(2);
      
      const pk = rows.find(r => r.indisprimary);
      expect(pk).toBeDefined();
      expect(pk.indisunique).toBe(true);
      expect(pk.indkey).toEqual([1]);

      const uniq = rows.find(r => !r.indisprimary && r.indisunique);
      expect(uniq).toBeDefined();
      expect(uniq.indkey).toEqual([2]);
    });
  });

  describe("LEVEL 22: Foreign Key Metadata Persistence", () => {
    test("22.1 Create table with FK actions and verify metadata in pg_attribute", async () => {
      await db.exec(`CREATE TABLE departments_fk (id SERIAL PRIMARY KEY)`);
      await db.exec(`
        CREATE TABLE employees_fk (
          id SERIAL PRIMARY KEY,
          dept_id INTEGER REFERENCES departments_fk(id) ON DELETE CASCADE ON UPDATE RESTRICT
        )
      `);

      const rows = await db.query(`
        SELECT 
          attref_table, 
          attref_col, 
          attref_on_delete, 
          attref_on_update 
        FROM pg_attribute 
        WHERE attrelid = (SELECT oid FROM pg_class WHERE relname = 'employees_fk')
          AND attname = 'dept_id'
      `);

      expect(rows.length).toBe(1);
      expect(rows[0].attref_table).toBe("departments_fk");
      expect(rows[0].attref_on_delete).toBe("CASCADE");
      expect(rows[0].attref_on_update).toBe("RESTRICT");
    });

    test("22.2 Foreign Key Update Validation", async () => {
      await db.exec(`CREATE TABLE parent_update (id NUMBER PRIMARY KEY)`);
      await db.exec(`INSERT INTO parent_update (id) VALUES (1), (2)`);
      await db.exec(`CREATE TABLE child_update (id SERIAL PRIMARY KEY, p_id NUMBER REFERENCES parent_update(id))`);
      await db.exec(`INSERT INTO child_update (p_id) VALUES (1)`);

      // Valid update
      await db.exec(`UPDATE child_update SET p_id = 2 WHERE p_id = 1`);
      let rows = await db.query(`SELECT p_id FROM child_update`);
      expect(rows[0].p_id).toBe(2);

      // Invalid update - should throw to prevent orphaned data
      expect(async () => {
        await db.exec(`UPDATE child_update SET p_id = 999 WHERE id = 1`);
      }).toThrow();
    });

    test("22.3 ON DELETE CASCADE", async () => {
      await db.exec(`CREATE TABLE parent_cascade (id NUMBER PRIMARY KEY)`);
      await db.exec(`CREATE TABLE child_cascade (id SERIAL PRIMARY KEY, p_id NUMBER REFERENCES parent_cascade(id) ON DELETE CASCADE)`);
      
      await db.exec(`INSERT INTO parent_cascade (id) VALUES (10)`);
      await db.exec(`INSERT INTO child_cascade (p_id) VALUES (10), (10)`);
      
      await db.exec(`DELETE FROM parent_cascade WHERE id = 10`);
      
      const rows = await db.query(`SELECT * FROM child_cascade`);
      expect(rows.length).toBe(0);
    });

    test("22.4 ON DELETE SET NULL", async () => {
      await db.exec(`CREATE TABLE parent_setnull (id NUMBER PRIMARY KEY)`);
      await db.exec(`CREATE TABLE child_setnull (id SERIAL PRIMARY KEY, p_id NUMBER REFERENCES parent_setnull(id) ON DELETE SET NULL)`);
      
      await db.exec(`INSERT INTO parent_setnull (id) VALUES (20)`);
      await db.exec(`INSERT INTO child_setnull (p_id) VALUES (20)`);
      
      await db.exec(`DELETE FROM parent_setnull WHERE id = 20`);
      
      const rows = await db.query(`SELECT p_id FROM child_setnull`);
      expect(rows[0].p_id).toBeNull();
    });

    test("22.5 ON UPDATE CASCADE", async () => {
      await db.exec(`CREATE TABLE parent_upd_cascade (id NUMBER PRIMARY KEY)`);
      await db.exec(`CREATE TABLE child_upd_cascade (id SERIAL PRIMARY KEY, p_id NUMBER REFERENCES parent_upd_cascade(id) ON UPDATE CASCADE)`);
      
      await db.exec(`INSERT INTO parent_upd_cascade (id) VALUES (30)`);
      await db.exec(`INSERT INTO child_upd_cascade (p_id) VALUES (30)`);
      
      await db.exec(`UPDATE parent_upd_cascade SET id = 31 WHERE id = 30`);
      
      const rows = await db.query(`SELECT p_id FROM child_upd_cascade`);
      expect(rows[0].p_id).toBe(31);
    });

    test("22.6 ON DELETE RESTRICT (Default)", async () => {
      await db.exec(`CREATE TABLE parent_restrict (id NUMBER PRIMARY KEY)`);
      await db.exec(`CREATE TABLE child_restrict (id SERIAL PRIMARY KEY, p_id NUMBER REFERENCES parent_restrict(id))`);
      
      await db.exec(`INSERT INTO parent_restrict (id) VALUES (40)`);
      await db.exec(`INSERT INTO child_restrict (p_id) VALUES (40)`);
      
      // Should throw error because child record exists
      expect(async () => {
        await db.exec(`DELETE FROM parent_restrict WHERE id = 40`);
      }).toThrow();
    });
  });

  describe("LEVEL 23: Foreign Key Performance (Indexing)", () => {
    test("23.1 Validate index usage for FK checks", async () => {
      // Create a large table to simulate where scanning would be slow
      await db.exec(`CREATE TABLE big_parent (id NUMBER PRIMARY KEY, info TEXT)`);
      await db.exec(`CREATE TABLE big_child (id SERIAL PRIMARY KEY, p_id NUMBER REFERENCES big_parent(id))`);

      // Populate parent
      await db.exec(`INSERT INTO big_parent (id, info) VALUES (1, 'p1'), (2, 'p2'), (3, 'p3')`);

      // 1. Outgoing FK check (Insert into child) - Should pass using index
      const res = await db.exec(`INSERT INTO big_child (p_id) VALUES (2)`);
      expect(res.success).toBe(true);

      // 2. Parent-side check (Delete from parent) - Should be blocked using index
      expect(async () => {
        await db.exec(`DELETE FROM big_parent WHERE id = 2`);
      }).toThrow();

      // 3. Outgoing FK check (Invalid value) - Should fail quickly using index
      expect(async () => {
        await db.exec(`INSERT INTO big_child (p_id) VALUES (999)`);
      }).toThrow();
    });
  });

  describe("LEVEL 24: information_schema.referential_constraints", () => {
    test("24.1 Query referential_constraints for FK rules", async () => {
      await db.exec(`CREATE TABLE parent_rules (id SERIAL PRIMARY KEY)`);
      await db.exec(`
        CREATE TABLE child_rules (
          id SERIAL PRIMARY KEY, 
          p_id INTEGER REFERENCES parent_rules(id) ON DELETE CASCADE ON UPDATE SET NULL
        )
      `);

      const rows = await db.query(`
        SELECT 
          constraint_name, 
          update_rule, 
          delete_rule 
        FROM information_schema.referential_constraints 
        WHERE constraint_schema = 'public' AND constraint_name = 'child_rules_p_id_fkey'
      `);

      expect(rows.length).toBe(1);
      expect(rows[0].constraint_name).toBe("child_rules_p_id_fkey");
      expect(rows[0].update_rule).toBe("SET NULL");
      expect(rows[0].delete_rule).toBe("CASCADE");
    });
  });

  describe("LEVEL 27: Generated Columns (GENERATED ALWAYS AS ... STORED)", () => {
    test("27.1 Create table with generated column and insert data", async () => {
      await db.exec(`
        CREATE TABLE IF NOT EXISTS revenue_monthly (
          id SERIAL PRIMARY KEY,
          month DATE NOT NULL,
          revenue NUMERIC(12,2) NOT NULL,
          expenses NUMERIC(12,2) DEFAULT 0,
          profit NUMERIC(12,2) GENERATED ALWAYS AS (revenue - expenses) STORED
        )
      `);

      await db.exec(`INSERT INTO revenue_monthly (month, revenue, expenses) VALUES ('2024-01-01', 10000, 3000)`);
      await db.exec(`INSERT INTO revenue_monthly (month, revenue, expenses) VALUES ('2024-02-01', 15000, 5000)`);
      await db.exec(`INSERT INTO revenue_monthly (month, revenue, expenses) VALUES ('2024-03-01', 20000, 8000)`);

      const rows = await db.query(`SELECT * FROM revenue_monthly ORDER BY id`);
      expect(rows.length).toBe(3);
      expect(rows[0].profit).toBe(7000);
      expect(rows[1].profit).toBe(10000);
      expect(rows[2].profit).toBe(12000);
    });

    test("27.2 Generated column recalculates on UPDATE", async () => {
      await db.exec(`UPDATE revenue_monthly SET expenses = 1000 WHERE id = 1`);
      const rows = await db.query(`SELECT profit FROM revenue_monthly WHERE id = 1`);
      expect(rows[0].profit).toBe(9000);
    });

    test("27.3 Generated column with default expenses", async () => {
      await db.exec(`INSERT INTO revenue_monthly (month, revenue) VALUES ('2024-04-01', 25000)`);
      const rows = await db.query(`SELECT profit FROM revenue_monthly WHERE id = 4`);
      expect(rows[0].profit).toBe(25000); // revenue - 0 (default)
    });

    test("27.4 Query generated column with WHERE filter", async () => {
      const rows = await db.query(`SELECT month, profit FROM revenue_monthly WHERE profit > 10000 ORDER BY profit DESC`);
      expect(rows.length).toBe(2);
      expect(rows[0].profit).toBe(25000);
      expect(rows[1].profit).toBe(12000);
    });

    test("27.5 Generated column with RETURNING", async () => {
      const rows = await db.query(`
        INSERT INTO revenue_monthly (month, revenue, expenses) VALUES ('2024-05-01', 30000, 10000)
        RETURNING id, profit
      `);
      expect(rows.length).toBe(1);
      expect(rows[0].profit).toBe(20000);
    });
  });

  describe("LEVEL 26: Overflow Pages (Data > 4KB)", () => {
    test("26.1 Insert and Read a row larger than 4KB", async () => {
      await db.exec(`CREATE TABLE overflow_test (id SERIAL PRIMARY KEY, large_text TEXT)`);
      
      // Generate a string larger than 4KB (e.g., 10KB)
      let largeStr = "";
      for (let i = 0; i < 10000; i++) {
        largeStr += "A";
      }

      const res = await db.exec(`INSERT INTO overflow_test (large_text) VALUES ($1)`, [largeStr]);
      expect(res.success).toBe(true);

      const rows = await db.query(`SELECT id, large_text FROM overflow_test`);
      expect(rows.length).toBe(1);
      expect(rows[0].id).toBe(1);
      expect(rows[0].large_text.length).toBe(10000);
      expect(rows[0].large_text).toBe(largeStr);

      // Update the row to an even larger string
      let largerStr = largeStr + "B".repeat(5000); // 15KB
      await db.exec(`UPDATE overflow_test SET large_text = $1 WHERE id = 1`, [largerStr]);

      const updatedRows = await db.query(`SELECT large_text FROM overflow_test WHERE id = 1`);
      expect(updatedRows[0].large_text.length).toBe(15000);
      expect(updatedRows[0].large_text).toBe(largerStr);

      // Delete the row
      const delRes = await db.exec(`DELETE FROM overflow_test WHERE id = 1`);
      expect(delRes.deleted).toBe(1);

      const emptyRows = await db.query(`SELECT * FROM overflow_test`);
      expect(emptyRows.length).toBe(0);
    });
  });

  describe("LEVEL 25: Quoted Identifiers & Decimal Numbers", () => {
    test("25.1 Create table with quoted identifiers and insert decimal numbers", async () => {
      await db.exec(`
        CREATE TABLE IF NOT EXISTS "user_25" (
            "id" TEXT PRIMARY KEY,
            "email" TEXT,
            "fullName" TEXT,
            "role" TEXT
          )
      `);

      await db.exec(`
        CREATE TABLE IF NOT EXISTS "product_25" (
            "id" TEXT PRIMARY KEY,
            "title" TEXT,
            "price" NUMERIC,
            "inStock" BOOLEAN
          )
      `);

      await db.exec(`
        CREATE TABLE IF NOT EXISTS "order_25" (
            "id" TEXT PRIMARY KEY,
            "productId" TEXT,
            "userId" TEXT,
            "status" TEXT
          )
      `);

      await db.exec(`
        INSERT INTO "user_25" (id, email, fullName, role) VALUES
        ('1', 'alice@example.com', 'Alice', 'admin'),
        ('2', 'bob@example.com', 'Bob', 'user')
      `);

      await db.exec(`
        INSERT INTO "product_25" (id, title, price, inStock) VALUES
        ('1', 'Product 1', 9.99, true),
        ('2', 'Product 2', 19.99, false),
        ('3', 'Product 3', .50, true),
        ('4', 'Product 4', 1.2e-2, true)
      `);

      await db.exec(`
        INSERT INTO "order_25" (id, productId, userId, status) VALUES
        ('1', '1', '1', 'shipped'),
        ('2', '2', '2', 'pending')
      `);

      const allUsers = await db.query(`SELECT * FROM "user_25"`);
      expect(allUsers.length).toBe(2);

      const allProducts = await db.query(`SELECT * FROM "product_25"`);
      expect(allProducts.length).toBe(4);
      expect(allProducts[0].price).toBe(9.99);
      expect(allProducts[2].price).toBe(0.5);
      expect(allProducts[3].price).toBe(0.012);

      const allOrders = await db.query(`SELECT * FROM "order_25"`);
      expect(allOrders.length).toBe(2);
    });

    test("25.2 Update with quoted identifiers and string values", async () => {
      await db.exec(`CREATE TABLE "users_update_test" ("id" TEXT PRIMARY KEY, "username" TEXT)`);
      await db.exec(`INSERT INTO "users_update_test" ("id", "username") VALUES ('1', 'admin')`);

      const res = await db.exec(`UPDATE "users_update_test" SET "username" = 'admin123' WHERE "id" = '1'`);
      expect(res.success).toBe(true);
      expect(res.updated).toBe(1);

      const rows = await db.query(`SELECT "username" FROM "users_update_test" WHERE "id" = '1'`);
      expect(rows.length).toBe(1);
      expect(rows[0].username).toBe('admin123');
    });
  });

  describe("LEVEL 29: EXISTS Subqueries", () => {
    test("29.1 SELECT EXISTS", async () => {
      await db.exec(`CREATE TABLE migrations (id SERIAL PRIMARY KEY)`);
      const rows = await db.query(`
        SELECT EXISTS (
          SELECT FROM information_schema.tables 
          WHERE table_schema = 'public' AND table_name = 'migrations'
        ) as exists;
      `);
      expect(rows[0].exists).toBe(true);

      const rows2 = await db.query(`
        SELECT EXISTS (
          SELECT FROM information_schema.tables 
          WHERE table_schema = 'public' AND table_name = 'non_existent_table'
        ) as exists;
      `);
      expect(rows2[0].exists).toBe(false);
    });
  });

  describe("LEVEL 30: DISTINCT and AS alias", () => {
    test("30.2 SELECT DISTINCT ON", async () => {
      await db.exec(`CREATE TABLE distinct_on_test (id SERIAL PRIMARY KEY, group_id INT, val TEXT)`);
      await db.exec(`INSERT INTO distinct_on_test (group_id, val) VALUES 
        (1, 'A'), (1, 'B'), (2, 'C'), (2, 'D')`);
      
      // ORDER BY causes B to appear before A for group 1 and D before C for group 2.
      // DISTINCT ON removes duplicates based on group_id and picks the first encountered.
      const rows = await db.query(`
        SELECT DISTINCT ON (group_id) group_id, val 
        FROM distinct_on_test 
        ORDER BY group_id, val DESC
      `);
      expect(rows.length).toBe(2);
      expect(rows[0].group_id).toBe(1);
      expect(rows[0].val).toBe('B');
      expect(rows[1].group_id).toBe(2);
      expect(rows[1].val).toBe('D');
    });

    test("30.1 SELECT DISTINCT with multiple AS aliases", async () => {
      await db.exec(`CREATE TABLE products (id SERIAL PRIMARY KEY, category TEXT)`);
      await db.exec(`INSERT INTO products (category) VALUES ('Electronics'), ('Clothing'), ('Electronics'), ('Home')`);

      const rows = await db.query(
        "SELECT DISTINCT category AS value, category AS label FROM products WHERE category ILIKE '%' || $1 || '%' LIMIT 1000",
        ['e']
      );

      // 'Electronics', 'Home' contain 'e'
      // 'Clothing' does not
      expect(rows.length).toBe(2);
      
      const values = rows.map(r => r.value).sort();
      expect(values).toEqual(['Electronics', 'Home']);
      
      const labels = rows.map(r => r.label).sort();
      expect(labels).toEqual(['Electronics', 'Home']);
    });
  });

  describe("LEVEL 32: Auto-Destroy on Close", () => {
    test("32.1 Database destroys its files when destroyOnClose is true", async () => {
      const DESTROY_DB_FILE = "test_destroy.db";
      if (existsSync(DESTROY_DB_FILE)) unlinkSync(DESTROY_DB_FILE);
      if (existsSync(DESTROY_DB_FILE + ".wal")) unlinkSync(DESTROY_DB_FILE + ".wal");

      const dbDestroy = new LitePostgres(DESTROY_DB_FILE, {
        database: "testdb",
        adapter: new NodeFSAdapter(),
        destroyOnClose: true,
      });

      // Insert some data
      await dbDestroy.exec(`CREATE TABLE test_destroy (id SERIAL PRIMARY KEY)`);
      await dbDestroy.exec(`INSERT INTO test_destroy (id) VALUES (1)`);
      
      // Ensure file exists
      expect(existsSync(DESTROY_DB_FILE)).toBe(true);

      // Trigger disconnect / close
      await dbDestroy.close();

      // Database files should be deleted automatically
      expect(existsSync(DESTROY_DB_FILE)).toBe(false);
      expect(existsSync(DESTROY_DB_FILE + ".wal")).toBe(false);
    });
  });

  describe("LEVEL 33: BETWEEN operator", () => {
    test("33.1 Basic BETWEEN and NOT BETWEEN", async () => {
      await db.exec(`CREATE TABLE between_test (id SERIAL PRIMARY KEY, val NUMBER)`);
      await db.exec(`INSERT INTO between_test (val) VALUES (10), (20), (30), (40), (50)`);

      const rows1 = await db.query(`SELECT val FROM between_test WHERE val BETWEEN 20 AND 40 ORDER BY val`);
      expect(rows1.length).toBe(3);
      expect(rows1[0].val).toBe(20);
      expect(rows1[1].val).toBe(30);
      expect(rows1[2].val).toBe(40);

      const rows2 = await db.query(`SELECT val FROM between_test WHERE val NOT BETWEEN 20 AND 40 ORDER BY val`);
      expect(rows2.length).toBe(2);
      expect(rows2[0].val).toBe(10);
      expect(rows2[1].val).toBe(50);
    });
  });

  describe("LEVEL 31: Complex DDL with ADD CONSTRAINT", () => {
    test("31.1 Parse and execute ALTER TABLE ADD CONSTRAINT FOREIGN KEY", async () => {
      const sql = `
        -- 1. Tạo Giai đoạn Phễu
        CREATE TABLE IF NOT EXISTS sales_stages_31 (id BIGSERIAL PRIMARY KEY);
        ALTER TABLE sales_stages_31 ADD COLUMN IF NOT EXISTS name VARCHAR(255);
        
        -- 2. Tạo Khách hàng tiềm năng (Leads)
        CREATE TABLE IF NOT EXISTS leads_31 (id BIGSERIAL PRIMARY KEY);
        ALTER TABLE leads_31 ADD COLUMN IF NOT EXISTS full_name VARCHAR(255);

        -- 3. Tạo Cơ hội kinh doanh (Deals)
        CREATE TABLE IF NOT EXISTS deals_31 (id BIGSERIAL PRIMARY KEY);
        ALTER TABLE deals_31 ADD COLUMN IF NOT EXISTS lead_id BIGINT;
        ALTER TABLE deals_31 ADD COLUMN IF NOT EXISTS stage_id BIGINT;

        ALTER TABLE deals_31 ADD CONSTRAINT fk_deal_lead_31 FOREIGN KEY (lead_id) REFERENCES leads_31(id) ON UPDATE CASCADE ON DELETE SET NULL;
        ALTER TABLE deals_31 ADD CONSTRAINT fk_deal_stage_31 FOREIGN KEY (stage_id) REFERENCES sales_stages_31(id) ON UPDATE CASCADE ON DELETE SET NULL;

        -- Seed Data Giai đoạn
        INSERT INTO sales_stages_31 (name) VALUES 
        ('Mới'), ('Liên hệ')
        ON CONFLICT DO NOTHING;
      `;
      const res = await db.exec(sql);
      expect(res).toBeDefined();

      const rows = await db.query("SELECT * FROM sales_stages_31");
      expect(rows.length).toBe(2);

      // Verify foreign key metadata using information_schema
      const fkRows = await db.query(`
        SELECT constraint_name, update_rule, delete_rule
        FROM information_schema.referential_constraints
        WHERE constraint_name IN ('deals_31_lead_id_fkey', 'deals_31_stage_id_fkey')
      `);
      expect(fkRows.length).toBe(2);
      for (const fk of fkRows) {
        expect(fk.update_rule).toBe("CASCADE");
        expect(fk.delete_rule).toBe("SET NULL");
      }
    });
  });


  describe("LEVEL 36: CREATE TABLE IF NOT EXISTS in Transaction", () => {
    test("36.1 Create table if not exists in a transaction", async () => {
      // Create first table successfully
      await db.transaction(async (tx) => {
        await tx.query(`CREATE TABLE IF NOT EXISTS tx_test_a (id SERIAL PRIMARY KEY, val TEXT)`);
        await tx.query(`INSERT INTO tx_test_a (val) VALUES ('A')`);
      });

      // Failed transaction
      try {
        await db.transaction(async (tx) => {
          await tx.query(`CREATE TABLE IF NOT EXISTS tx_test_fail (id SERIAL PRIMARY KEY, val TEXT)`);
          await tx.query(`INSERT INTO tx_test_fail (val) VALUES ('B')`);
          throw new Error("Simulate error");
        });
      } catch (e) {}

      // Try creating table again in new transaction
      await db.transaction(async (tx) => {
        await tx.exec(`CREATE TABLE IF NOT EXISTS tx_test_b (id SERIAL PRIMARY KEY, val TEXT)`);
        await tx.exec(`CREATE TABLE IF NOT EXISTS tx_test_b (id SERIAL PRIMARY KEY, val TEXT)`);
        await tx.exec(`INSERT INTO tx_test_b (val) VALUES ('C')`);
      });

      const rowsA = await db.query(`SELECT * FROM tx_test_a`);
      expect(rowsA.length).toBe(1);

      expect(async () => {
        await db.query(`SELECT * FROM tx_test_fail`);
      }).toThrow();

      const rowsB = await db.query(`SELECT * FROM tx_test_b`);
      expect(rowsB.length).toBe(1);
      expect(rowsB[0].val).toBe('C');
    });
  });

  describe("LEVEL 35: Transaction Callback", () => {
    test("35.1 Successful transaction", async () => {
      await db.exec(`CREATE TABLE tx_cb_test (id SERIAL PRIMARY KEY, val TEXT)`);
      const res = await db.transaction(async (tx) => {
        await tx.exec(`INSERT INTO tx_cb_test (val) VALUES ('A')`);
        await tx.exec(`INSERT INTO tx_cb_test (val) VALUES ('B')`);
        return 'success';
      });
      expect(res).toBe('success');
      const rows = await db.query(`SELECT * FROM tx_cb_test`);
      expect(rows.length).toBe(2);
    });

    test("35.2 Failed transaction (JS Error)", async () => {
      try {
        await db.transaction(async (tx) => {
          await tx.exec(`INSERT INTO tx_cb_test (val) VALUES ('C')`);
          throw new Error("Abort");
        });
      } catch (e) {}
      const rows = await db.query(`SELECT * FROM tx_cb_test WHERE val = 'C'`);
      expect(rows.length).toBe(0);
    });

    test("35.3 Failed transaction (SQL Error)", async () => {
      try {
        await db.transaction(async (tx) => {
          await tx.exec(`INSERT INTO tx_cb_test (val) VALUES ('D')`);
          await tx.exec(`INVALID SQL`);
        });
      } catch (e) {}
      const rows = await db.query(`SELECT * FROM tx_cb_test WHERE val = 'D'`);
      expect(rows.length).toBe(0);
    });
  });

  describe("LEVEL 38: INSERT INTO ... SELECT", () => {
    test("38.1 Insert using SELECT without column names specified", async () => {
      await db.exec(`CREATE TABLE src_table (id SERIAL PRIMARY KEY, val TEXT)`);
      await db.exec(`INSERT INTO src_table (val) VALUES ('A'), ('B'), ('C')`);
      
      await db.exec(`CREATE TABLE dest_table (id SERIAL PRIMARY KEY, val TEXT)`);
      // Insert all rows from src_table into dest_table
      await db.exec(`INSERT INTO dest_table SELECT * FROM src_table`);
      
      const rows = await db.query(`SELECT * FROM dest_table ORDER BY id`);
      expect(rows.length).toBe(3);
      expect(rows[0].val).toBe('A');
      expect(rows[2].val).toBe('C');
    });

    test("38.2 Insert using SELECT with column names specified", async () => {
      await db.exec(`CREATE TABLE dest_table2 (id SERIAL PRIMARY KEY, title TEXT, score NUMBER)`);
      
      await db.exec(`
        INSERT INTO dest_table2 (title, score)
        SELECT val, id * 10 FROM src_table
      `);
      
      const rows = await db.query(`SELECT * FROM dest_table2 ORDER BY id`);
      expect(rows.length).toBe(3);
      expect(rows[0].title).toBe('A');
      expect(rows[0].score).toBe(10);
      expect(rows[2].score).toBe(30);
    });
  });

  describe("LEVEL 39: CREATE INDEX", () => {
    test("39.1 Create basic index", async () => {
      await db.exec(`CREATE TABLE users_idx_test (id SERIAL PRIMARY KEY, email TEXT)`);
      const res = await db.exec(`CREATE INDEX idx_users_email ON users_idx_test(email)`);
      expect(res.success).toBe(true);
      expect(res.message).toContain("idx_users_email");

      const res2 = await db.exec(`CREATE INDEX IF NOT EXISTS idx_users_email ON users_idx_test(email)`);
      expect(res2.success).toBe(true);

      const rows = await db.query(`
        SELECT relname FROM pg_class WHERE relname = 'idx_users_email'
      `);
      expect(rows.length).toBe(1);
    });

    test("39.2 Create unique index with USING btree and ASC/DESC", async () => {
      const res = await db.exec(`CREATE UNIQUE INDEX idx_users_email_uniq ON users_idx_test USING btree (email DESC NULLS LAST)`);
      expect(res.success).toBe(true);
      
      const rows = await db.query(`
        SELECT indisunique FROM pg_index 
        WHERE indexrelid = (SELECT oid FROM pg_class WHERE relname = 'idx_users_email_uniq')
      `);
      expect(rows.length).toBe(1);
      expect(rows[0].indisunique).toBe(true);
    });

    test("39.3 Column named index still works", async () => {
      const res = await db.exec(`CREATE TABLE index_col_test (id SERIAL, index INT)`);
      expect(res.success).toBe(true);
      
      await db.exec(`INSERT INTO index_col_test (index) VALUES (42)`);
      const rows = await db.query(`SELECT index FROM index_col_test`);
      expect(rows[0].index).toBe(42);
    });

    test("39.4 Create index on table with foreign keys (password_reset_tokens)", async () => {
      await db.exec(`CREATE TABLE users_39 (id SERIAL PRIMARY KEY)`);
      await db.exec(`
        CREATE TABLE password_reset_tokens (
            id SERIAL PRIMARY KEY,
            user_id INTEGER NOT NULL REFERENCES users_39(id) ON DELETE CASCADE,
            token TEXT NOT NULL,
            expires_at TIMESTAMP NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
      `);
      const res = await db.exec(`CREATE INDEX idx_password_reset_tokens_token ON password_reset_tokens(token)`);
      expect(res.success).toBe(true);
    });

    test("39.5 Create index on notifications with quotes and constraints", async () => {
      await db.exec(`
        CREATE TABLE "notifications_39" (
            "id" SERIAL PRIMARY KEY,
            "user_id" INTEGER NOT NULL REFERENCES "users_39"("id") ON DELETE CASCADE,
            "title" TEXT NOT NULL,
            "content" TEXT NOT NULL,
            "type" TEXT NOT NULL CHECK ("type" IN ('order', 'promotion', 'system', 'appointment')),
            "is_read" BOOLEAN DEFAULT FALSE,
            "metadata" JSONB,
            "created_at" TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
            "updated_at" TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
            "deleted_at" TIMESTAMP WITHOUT TIME ZONE
        );
      `);
      const res1 = await db.exec(`CREATE INDEX "idx_notifications_user_id" ON "notifications_39"("user_id");`);
      expect(res1.success).toBe(true);
      const res2 = await db.exec(`CREATE INDEX "idx_notifications_type" ON "notifications_39"("type");`);
      expect(res2.success).toBe(true);
    });
  });

  describe("LEVEL 41: Foreign Key with Parameter Type Coercion", () => {
    test("41.1 Insert into child table with string param when FK is integer", async () => {
      await db.exec(`CREATE TABLE parent_fk_coerce (id SERIAL PRIMARY KEY, name TEXT)`);
      await db.exec(`CREATE TABLE child_fk_coerce (id SERIAL PRIMARY KEY, parent_id INTEGER REFERENCES parent_fk_coerce(id))`);

      await db.exec(`INSERT INTO parent_fk_coerce (name) VALUES ('Parent 1')`);
      
      // Should correctly coerce '1' to 1 for the foreign key check and insertion
      const res = await db.exec(`INSERT INTO child_fk_coerce (parent_id) VALUES ($1)`, ['1']);
      expect(res.success).toBe(true);

      const rows = await db.query(`SELECT * FROM child_fk_coerce`);
      expect(rows.length).toBe(1);
      expect(rows[0].parent_id).toBe(1); // Should be number, not string
    });
    
    test("41.2 Update child table with string param when FK is integer", async () => {
      await db.exec(`INSERT INTO parent_fk_coerce (name) VALUES ('Parent 2')`);
      
      // Update with string parameter
      const res = await db.exec(`UPDATE child_fk_coerce SET parent_id = $1 WHERE id = 1`, ['2']);
      expect(res.updated).toBe(1);

      const rows = await db.query(`SELECT * FROM child_fk_coerce`);
      expect(rows[0].parent_id).toBe(2);
    });

    test("41.3 Index lookup pushdown with string param for integer PK", async () => {
      const rows = await db.query(`SELECT name FROM parent_fk_coerce WHERE id = $1`, ['2']);
      expect(rows.length).toBe(1);
      expect(rows[0].name).toBe('Parent 2');
    });
  });

  describe("LEVEL 40: Corrupted dbMetaCache on Rollback", () => {
    test("40.1 Table columns are not lost after a failed transaction causes rollback", async () => {
      try {
        await db.transaction(async (tx) => {
          // Force an overflow in pgAttributeDef by creating a table with many columns
          let cols = [];
          for (let i = 0; i < 100; i++) cols.push(`col_${i} TEXT`);
          await tx.exec(`CREATE TABLE wide_table_fail (${cols.join(", ")})`);
          
          // Throw to trigger rollback
          throw new Error("Force rollback");
        });
      } catch (e) {}

      // Now create a normal table in a new transaction
      // Without the fix, this would fail during CREATE INDEX because 'token' would be written to an orphaned page
      await db.transaction(async (tx) => {
        await tx.exec(`CREATE TABLE safe_table (id SERIAL PRIMARY KEY, token TEXT)`);
        await tx.exec(`CREATE INDEX idx_safe_table_token ON safe_table(token)`);
      });

      const rows = await db.query(`SELECT * FROM safe_table`);
      expect(rows.length).toBe(0);

      // Verify the columns exist in the catalog
      const tableInfo = await db.query(`
        SELECT column_name 
        FROM information_schema.columns 
        WHERE table_name = 'safe_table'
      `);
      expect(tableInfo.length).toBe(2);
      expect(tableInfo.map((r: any) => r.column_name)).toContain('token');
    });
  });

  describe("LEVEL 37: Multi-database Context via exec/query Overloads", () => {
    test("37.1 Overloaded exec/query passing dbName as the second argument", async () => {
      const customDbFile = "test_multidb.db";
      if (existsSync(customDbFile)) unlinkSync(customDbFile);
      if (existsSync(customDbFile + ".wal")) unlinkSync(customDbFile + ".wal");

      const pg = new LitePostgres(customDbFile, { adapter: new NodeFSAdapter() });

      // Create a table in database "db1" by omitting params array
      await pg.exec(`CREATE TABLE custom_test (id SERIAL PRIMARY KEY, val TEXT)`, "db1");
      
      // Insert using the omitted params overload
      await pg.exec(`INSERT INTO custom_test (val) VALUES ('Hello')`, "db1");

      // Select using omitted params
      const rows = await pg.query(`SELECT * FROM custom_test`, "db1");
      expect(rows.length).toBe(1);
      expect(rows[0].val).toBe('Hello');

      // Attempt to read from default database ('postgres'), should fail because table isn't there
      expect(async () => {
        await pg.query(`SELECT * FROM custom_test`);
      }).toThrow();

      // Test transaction targeting specific db
      await pg.transaction(async (tx) => {
        await tx.exec(`INSERT INTO custom_test (val) VALUES ('World')`);
      }, "db1");

      const txRows = await pg.query(`SELECT * FROM custom_test`,[], "db1");
      expect(txRows.length).toBe(2);
      expect(txRows[1].val).toBe('World');

      // Test transaction cross-database failure
      expect(async () => {
        await pg.transaction(async (tx) => {
          await tx.exec(`INSERT INTO custom_test (val) VALUES ('Fail')`, "db2");
        }, "db1");
      }).toThrow();

      await pg.close();
      if (existsSync(customDbFile)) unlinkSync(customDbFile);
      if (existsSync(customDbFile + ".wal")) unlinkSync(customDbFile + ".wal");
    });
  });

  describe("LEVEL 42: Multiple Table FROM (Implicit CROSS JOIN)", () => {
    test("42.1 Basic cross join with comma syntax", async () => {
      await db.exec(`CREATE TABLE colors (id SERIAL PRIMARY KEY, name TEXT)`);
      await db.exec(`CREATE TABLE sizes (id SERIAL PRIMARY KEY, name TEXT)`);
      
      await db.exec(`INSERT INTO colors (name) VALUES ('Red'), ('Blue')`);
      await db.exec(`INSERT INTO sizes (name) VALUES ('S'), ('M'), ('L')`);

      const rows = await db.query(`
        SELECT colors.name as color, sizes.name as size 
        FROM colors, sizes
        ORDER BY color, size
      `);

      expect(rows.length).toBe(6);
      expect(rows[0].color).toBe('Blue');
      expect(rows[0].size).toBe('L');
    });

    test("42.2 Comma FROM with WHERE clause (Implicit INNER JOIN)", async () => {
      const rows = await db.query(`
        SELECT c.name as color, s.name as size
        FROM colors c, sizes s
        WHERE c.name = 'Red' AND s.name = 'M'
      `);

      expect(rows.length).toBe(1);
      expect(rows[0].color).toBe('Red');
      expect(rows[0].size).toBe('M');
    });

    test("42.3 Triple cross join", async () => {
      await db.exec(`CREATE TABLE shapes (name TEXT)`);
      await db.exec(`INSERT INTO shapes VALUES ('Circle'), ('Square')`);

      const rows = await db.query(`SELECT * FROM colors, sizes, shapes`);
      expect(rows.length).toBe(12); // 2 * 3 * 2
    });
  });

  describe("LEVEL 43: String Functions", () => {
    test("43.1 LOWER() and LENGTH()", async () => {
      const rows = await db.query("SELECT LOWER('HeLLo') as l, LENGTH('world') as len, LENGTH(NULL) as ln");
      expect(rows[0].l).toBe('hello');
      expect(rows[0].len).toBe(5);
      expect(rows[0].ln).toBeNull();
    });

    test("43.2 TRIM() and REPLACE()", async () => {
      const rows = await db.query("SELECT TRIM('  spaces  ') as t, REPLACE('banana', 'a', 'o') as r");
      expect(rows[0].t).toBe('spaces');
      expect(rows[0].r).toBe('bonono');
    });

    test("43.3 SUBSTRING()", async () => {
      const rows = await db.query(`
        SELECT 
          SUBSTRING('alphabet', 3, 2) as s1, 
          SUBSTRING('alphabet', 3) as s2,
          SUBSTRING(NULL, 1) as s3
      `);
      expect(rows[0].s1).toBe('ph');
      expect(rows[0].s2).toBe('phabet');
      expect(rows[0].s3).toBeNull();
    });

    test("43.4 CONCAT_WS()", async () => {
      const rows = await db.query(`
        SELECT 
          CONCAT_WS('-', '2024', '05', '20') as date,
          CONCAT_WS(',', 'a', NULL, 'b', 'c') as list,
          CONCAT_WS(NULL, 'a', 'b') as n
      `);
      expect(rows[0].date).toBe('2024-05-20');
      expect(rows[0].list).toBe('a,b,c');
      expect(rows[0].n).toBeNull();
    });
  });

  describe("LEVEL 44: Extended String Functions", () => {
    test("44.1 CONCAT, LTRIM, RTRIM", async () => {
      const rows = await db.query(`
        SELECT 
          CONCAT('Post', 'gres', 'Lite') as c,
          LTRIM('   left') as l,
          RTRIM('right   ') as r
      `);
      expect(rows[0].c).toBe('PostgresLite');
      expect(rows[0].l).toBe('left');
      expect(rows[0].r).toBe('right');
    });

    test("44.2 LEFT and RIGHT", async () => {
      const rows = await db.query(`
        SELECT 
          LEFT('abcde', 2) as l1,
          LEFT('abcde', -2) as l2,
          RIGHT('abcde', 2) as r1,
          RIGHT('abcde', -2) as r2
      `);
      expect(rows[0].l1).toBe('ab');
      expect(rows[0].l2).toBe('abc');
      expect(rows[0].r1).toBe('de');
      expect(rows[0].r2).toBe('cde');
    });

    test("44.3 LPAD and RPAD", async () => {
      const rows = await db.query(`
        SELECT 
          LPAD('hi', 5, 'x') as l1,
          RPAD('hi', 5, 'y') as r1,
          LPAD('longstring', 4) as l2
      `);
      expect(rows[0].l1).toBe('xxxhi');
      expect(rows[0].r1).toBe('hiyyy');
      expect(rows[0].l2).toBe('long');
    });

    test("44.4 INITCAP and REVERSE", async () => {
      const rows = await db.query(`
        SELECT 
          INITCAP('hELLO wORLD') as i,
          REVERSE('desserts') as r
      `);
      expect(rows[0].i).toBe('Hello World');
      expect(rows[0].r).toBe('stressed');
    });

    test("44.5 STRPOS, REPEAT, SPLIT_PART", async () => {
      const rows = await db.query(`
        SELECT 
          STRPOS('high', 'ig') as s,
          REPEAT('a', 3) as r,
          SPLIT_PART('a,b,c', ',', 2) as sp
      `);
      expect(rows[0].s).toBe(2);
      expect(rows[0].r).toBe('aaa');
      expect(rows[0].sp).toBe('b');
    });
  });

  describe("LEVEL 45: Mathematical Functions", () => {
    test("45.1 Basic Math: ABS, CEIL, FLOOR, SIGN", async () => {
      const rows = await db.query(`
        SELECT 
          ABS(-10.5) as a,
          CEIL(4.2) as c1,
          CEILING(-4.2) as c2,
          FLOOR(4.8) as f1,
          FLOOR(-4.8) as f2,
          SIGN(-50) as s1,
          SIGN(0) as s2,
          SIGN(50) as s3
      `);
      expect(rows[0].a).toBe(10.5);
      expect(rows[0].c1).toBe(5);
      expect(rows[0].c2).toBe(-4);
      expect(rows[0].f1).toBe(4);
      expect(rows[0].f2).toBe(-5);
      expect(rows[0].s1).toBe(-1);
      expect(rows[0].s2).toBe(0);
      expect(rows[0].s3).toBe(1);
    });

    test("45.2 Rounding and Truncation", async () => {
      const rows = await db.query(`
        SELECT 
          ROUND(10.4) as r1,
          ROUND(10.5) as r2,
          ROUND(10.6) as r3,
          ROUND(1.2345, 2) as r4,
          TRUNC(1.2345, 2) as t1,
          TRUNC(1.99) as t2
      `);
      expect(rows[0].r1).toBe(10);
      expect(rows[0].r2).toBe(11);
      expect(rows[0].r3).toBe(11);
      expect(rows[0].r4).toBe(1.23);
      expect(rows[0].t1).toBe(1.23);
      expect(rows[0].t2).toBe(1);
    });

    test("45.3 Power, Square Root, Exponential and Logarithm", async () => {
      const rows = await db.query(`
        SELECT 
          POWER(2, 3) as p1,
          POW(3, 2) as p2,
          SQRT(16) as s,
          EXP(1) as e,
          LN(2.718281828459) as ln,
          LOG(100) as log10
      `);
      expect(rows[0].p1).toBe(8);
      expect(rows[0].p2).toBe(9);
      expect(rows[0].s).toBe(4);
      expect(rows[0].e).toBeCloseTo(2.71828);
      expect(rows[0].ln).toBeCloseTo(1);
      expect(rows[0].log10).toBe(2);
    });

    test("45.4 Trigonometry Helpers: PI, DEGREES, RADIANS", async () => {
      const rows = await db.query(`
        SELECT 
          PI() as p,
          DEGREES(PI()) as d,
          RADIANS(180) as r
      `);
      expect(rows[0].p).toBeCloseTo(Math.PI);
      expect(rows[0].d).toBe(180);
      expect(rows[0].r).toBeCloseTo(Math.PI);
    });

    test("45.5 RANDOM and MOD", async () => {
      const rows = await db.query(`
        SELECT 
          RANDOM() as rnd,
          MOD(10, 3) as m
      `);
      expect(rows[0].rnd).toBeGreaterThanOrEqual(0);
      expect(rows[0].rnd).toBeLessThan(1);
      expect(rows[0].m).toBe(1);
    });
  });

  describe("LEVEL 46: Advanced Date Functions", () => {
    test("46.1 EXTRACT() function", async () => {
      const sql = "SELECT EXTRACT(YEAR FROM '2024-05-20'::timestamp) as yr, EXTRACT(MONTH FROM '2024-05-20'::timestamp) as mon";
      const rows = await db.query(sql);
      expect(rows[0].yr).toBe(2024);
      expect(rows[0].mon).toBe(5);
    });

    test("46.2 AGE() function", async () => {
      const sql = "SELECT AGE('2024-05-20'::timestamp, '2023-01-01'::timestamp) as age_val";
      const rows = await db.query(sql);
      expect(rows[0].age_val).toContain("1 year");
      expect(rows[0].age_val).toContain("4 months");
      expect(rows[0].age_val).toContain("19 days");
    });

    test("46.3 TO_CHAR() function", async () => {
      const sql = "SELECT TO_CHAR('2024-05-20'::timestamp, 'YYYY-MM-DD') as fmt";
      const rows = await db.query(sql);
      expect(rows[0].fmt).toBe("2024-05-20");
    });

    test("46.4 DATE_PART() function", async () => {
      const sql = "SELECT DATE_PART('year', '2024-05-20'::timestamp) as yr";
      const rows = await db.query(sql);
      expect(rows[0].yr).toBe(2024);
    });

    test("46.5 Date Constants", async () => {
      const sql = "SELECT CURRENT_DATE as cd, LOCALTIMESTAMP as lts";
      const rows = await db.query(sql);
      expect(rows[0].cd).toMatch(/^\d{4}-\d{2}-\d{2}$/);
      expect(rows[0].lts).toBeDefined();
    });
  });

  describe("LEVEL 47: JSON Set Returning Functions", () => {
    test("47.1 jsonb_each()", async () => {
      const sql = `SELECT * FROM jsonb_each('{"a": 1, "b": "foo"}'::json) AS t(k, v)`;
      const rows = await db.query(sql);
      expect(rows.length).toBe(2);
      expect(rows[0].k).toBe("a");
      expect(rows[0].v).toBe(1);
      expect(rows[1].k).toBe("b");
      expect(rows[1].v).toBe("foo");
    });

    test("47.2 jsonb_array_elements()", async () => {
      const sql = `SELECT * FROM jsonb_array_elements('[1, "foo", {"x": 10}]'::json) AS t(val)`;
      const rows = await db.query(sql);
      expect(rows.length).toBe(3);
      expect(rows[0].val).toBe(1);
      expect(rows[1].val).toBe("foo");
      expect(rows[2].val).toEqual({"x": 10});
    });

    test("47.3 jsonb_each() with ordinality", async () => {
      const sql = `SELECT k, v, n FROM jsonb_each('{"x": 10, "y": 20}'::json) WITH ORDINALITY AS t(k, v, n)`;
      const rows = await db.query(sql);
      expect(rows.length).toBe(2);
      expect(rows[0].n).toBe(1);
      expect(rows[1].n).toBe(2);
    });

    test("47.4 LATERAL join with jsonb_each", async () => {
      await db.exec(`CREATE TABLE json_data (id INT, doc JSONB)`);
      await db.exec(`INSERT INTO json_data VALUES (1, '{"tags": ["a", "b"], "meta": {"owner": "alice"}}')`);
      
      const sql = `
        SELECT j.id, kv.key, kv.value
        FROM json_data j
        CROSS JOIN LATERAL jsonb_each(j.doc) AS kv(key, value)
        WHERE j.id = 1
      `;
      const rows = await db.query(sql);
      expect(rows.length).toBe(2);
      const tags = rows.find(r => r.key === 'tags');
      expect(tags.value).toEqual(["a", "b"]);
    });
  });

  describe("LEVEL 49: VALUES in FROM clause", () => {
    test("49.1 Basic VALUES in FROM with column aliases", async () => {
      const sql = `SELECT id, name FROM (VALUES (1, 'a'), (2, 'b')) AS t(id, name)`;
      const rows = await db.query(sql);
      expect(rows.length).toBe(2);
      expect(rows[0].id).toBe(1);
      expect(rows[0].name).toBe('a');
      expect(rows[1].id).toBe(2);
      expect(rows[1].name).toBe('b');
    });

    test("49.2 VALUES as a standalone statement", async () => {
      const sql = `VALUES (1, 'foo'), (2, 'bar')`;
      const rows = await db.query(sql);
      expect(rows.length).toBe(2);
      expect(rows[0].column1).toBe(1);
      expect(rows[0].column2).toBe('foo');
    });

    test("49.3 VALUES with expressions", async () => {
      const sql = `SELECT * FROM (VALUES (1+1, UPPER('hello'))) AS t(num, str)`;
      const rows = await db.query(sql);
      expect(rows[0].num).toBe(2);
      expect(rows[0].str).toBe('HELLO');
    });

    test("49.4 JOIN with VALUES subquery", async () => {
      await db.exec(`CREATE TABLE users_49 (id INT, email TEXT)`);
      await db.exec(`INSERT INTO users_49 VALUES (1, 'a@b.com'), (2, 'c@d.com')`);
      
      const sql = `
        SELECT u.email, v.role
        FROM users_49 u
        JOIN (VALUES (1, 'admin'), (2, 'user')) AS v(user_id, role)
        ON u.id = v.user_id
        ORDER BY u.id
      `;
      const rows = await db.query(sql);
      expect(rows.length).toBe(2);
      expect(rows[0].email).toBe('a@b.com');
      expect(rows[0].role).toBe('admin');
      expect(rows[1].role).toBe('user');
    });
  });

  describe("LEVEL 48: FIRST_VALUE and LAST_VALUE Window Functions", () => {
    test("48.1 FIRST_VALUE and LAST_VALUE with PARTITION BY and ORDER BY", async () => {
      const rows = await db.query(`
        SELECT 
          name, 
          department, 
          salary,
          FIRST_VALUE(name) OVER (PARTITION BY department ORDER BY salary DESC) as top_earner,
          LAST_VALUE(name) OVER (PARTITION BY department ORDER BY salary DESC) as lowest_earner
        FROM employees
        ORDER BY department, salary DESC
      `);
      
      const itRows = rows.filter(r => r.department === 'IT');
      // IT salaries: Bob (6000), Alice (5000), David (5000)
      expect(itRows[0].top_earner).toBe('Bob');
      expect(itRows[0].lowest_earner).toBe('David');
      expect(itRows[1].top_earner).toBe('Bob');
      expect(itRows[2].lowest_earner).toBe('David');

      const hrRows = rows.filter(r => r.department === 'HR');
      // HR salaries: Charlie (4500), Eve (4500)
      expect(hrRows[0].top_earner).toBe('Charlie');
      expect(hrRows[1].top_earner).toBe('Charlie');
      expect(hrRows[1].lowest_earner).toBe('Eve');
    });

    test("48.2 FIRST_VALUE without partition", async () => {
      const rows = await db.query(`
        SELECT name, FIRST_VALUE(salary) OVER (ORDER BY name ASC) as first_sal
        FROM employees
        ORDER BY name ASC
      `);
      // Names alphabetically: Alice, Bob, Charlie, David, Eve
      // Alice's salary is 5000
      expect(rows.every(r => r.first_sal === 5000)).toBe(true);
    });
  });
});