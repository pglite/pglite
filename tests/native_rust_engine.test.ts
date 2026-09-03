import { expect, test, describe, beforeAll, afterAll } from "bun:test";
const { LitePostgresNative } = require("../crates/pglite-rs/pglite.node");
import { PGLiteNative } from "../src/native";
import { unlinkSync, existsSync } from "fs";

const TEST_DB_FILE = "test_native_engine.db";

function cleanFiles(prefix: string) {
  for (const ext of ["", ".wal", ".lock"]) {
    if (existsSync(prefix + ext)) {
      try { unlinkSync(prefix + ext); } catch {}
    }
  }
}

describe("Native Rust Engine (pglite-rs) Comprehensive Test Suite", () => {
  let db: any;

  beforeAll(() => {
    cleanFiles(TEST_DB_FILE);
    db = new LitePostgresNative(TEST_DB_FILE);
  });

  afterAll(() => {
    cleanFiles(TEST_DB_FILE);
  });

  // ==========================================
  // LEVEL 1: DDL (Data Definition Language)
  // ==========================================
  describe("LEVEL 1: DDL Schema Operations", () => {
    test("1.1 CREATE TABLE with multiple data types and constraints", () => {
      const sql = `
        CREATE TABLE "users" (
          "id" SERIAL PRIMARY KEY,
          "username" TEXT,
          "email" TEXT,
          "score" FLOAT,
          "is_active" BOOLEAN,
          "created_at" TIMESTAMP
        )
      `;
      const res = db.exec(sql);
      expect(res).toBeDefined();
    });

    test("1.2 CREATE TABLE IF NOT EXISTS without error", () => {
      const sql = `CREATE TABLE IF NOT EXISTS "users" ("id" SERIAL PRIMARY KEY, "username" TEXT)`;
      const res = db.exec(sql);
      expect(res).toBeDefined();
    });

    test("1.3 CREATE secondary table for relational testing", () => {
      const sql = `
        CREATE TABLE "schools" (
          "id" SERIAL PRIMARY KEY,
          "name" TEXT,
          "city" TEXT
        )
      `;
      const res = db.exec(sql);
      expect(res).toBeDefined();
    });

    test("1.4 CREATE child table with foreign key reference", () => {
      const sql = `
        CREATE TABLE "classes" (
          "id" SERIAL PRIMARY KEY,
          "school_id" INT,
          "name" TEXT,
          "student_count" INT,
          "deleted_at" TIMESTAMP
        )
      `;
      const res = db.exec(sql);
      expect(res).toBeDefined();
    });
  });

  // ==========================================
  // LEVEL 2: DML INSERT Operations
  // ==========================================
  describe("LEVEL 2: DML INSERT Operations", () => {
    test("2.1 Parameterized INSERT ($1, $2, ...)", () => {
      const res = db.query(
        `INSERT INTO "users" ("username", "email", "score", "is_active", "created_at") VALUES ($1, $2, $3, $4, $5)`,
        ["alice", "alice@example.com", 95.5, true, "2026-09-01T10:00:00Z"]
      );
      expect(res).toBeDefined();
    });

    test("2.2 Multiple INSERT statements verifying auto-incrementing SERIAL PK", () => {
      db.query(
        `INSERT INTO "users" ("username", "email", "score", "is_active", "created_at") VALUES ($1, $2, $3, $4, $5)`,
        ["bob", "bob@example.com", 82.0, true, "2026-09-02T11:00:00Z"]
      );
      db.query(
        `INSERT INTO "users" ("username", "email", "score", "is_active", "created_at") VALUES ($1, $2, $3, $4, $5)`,
        ["charlie", "charlie@example.com", 70.0, false, "2026-09-03T12:00:00Z"]
      );

      const rows = db.query(`SELECT "id", "username" FROM "users" ORDER BY "id" ASC`);
      expect(rows.length).toBe(3);
      expect(rows[0].id).toBe(1);
      expect(rows[1].id).toBe(2);
      expect(rows[2].id).toBe(3);
      expect(rows[0].username).toBe("alice");
      expect(rows[1].username).toBe("bob");
      expect(rows[2].username).toBe("charlie");
    });

    test("2.3 INSERT with NULL values", () => {
      db.query(
        `INSERT INTO "users" ("username", "email", "score", "is_active", "created_at") VALUES ($1, $2, $3, $4, $5)`,
        ["david", null, null, true, null]
      );
      const rows = db.query(`SELECT "id", "username", "email", "score" FROM "users" WHERE "username" = $1`, ["david"]);
      expect(rows.length).toBe(1);
      expect(rows[0].email).toBeNull();
      expect(rows[0].score).toBeNull();
    });

    test("2.4 INSERT with RETURNING clause", () => {
      db.exec(`CREATE TABLE "provinces" ("id" SERIAL PRIMARY KEY, "name" TEXT, "description" TEXT)`);
      const res1 = db.query2(
        `INSERT INTO "provinces" ("name", "description") VALUES ($1, $2) RETURNING "id"`,
        ["Hanoi", "Capital"]
      );
      expect(res1.rowCount).toBe(1);
      expect(res1.rows.length).toBe(1);
      expect(res1.rows[0].id).toBe(1);
      expect(res1.fields).toEqual([{ name: "id", data_type: "serial" }]);

      const res2 = db.query2(
        `INSERT INTO "provinces" ("name", "description") VALUES ($1, $2) RETURNING "id", "name" AS "provinceName"`,
        ["Danang", "Central"]
      );
      expect(res2.rowCount).toBe(1);
      expect(res2.rows.length).toBe(1);
      expect(res2.rows[0].id).toBe(2);
      expect(res2.rows[0].provinceName).toBe("Danang");
    });

    test("2.4 Populating schools and classes for relational testing", () => {
      db.query(`INSERT INTO "schools" ("name", "city") VALUES ($1, $2)`, ["Hoa Mai School", "Hanoi"]);
      db.query(`INSERT INTO "schools" ("name", "city") VALUES ($1, $2)`, ["Sao Mai School", "Saigon"]);

      db.query(
        `INSERT INTO "classes" ("school_id", "name", "student_count", "deleted_at") VALUES ($1, $2, $3, $4)`,
        [1, "Lớp Mầm 1", 25, null]
      );
      db.query(
        `INSERT INTO "classes" ("school_id", "name", "student_count", "deleted_at") VALUES ($1, $2, $3, $4)`,
        [1, "Lớp Chồi 2", 30, null]
      );
      db.query(
        `INSERT INTO "classes" ("school_id", "name", "student_count", "deleted_at") VALUES ($1, $2, $3, $4)`,
        [2, "Lớp Lá 3", 28, null]
      );
      db.query(
        `INSERT INTO "classes" ("school_id", "name", "student_count", "deleted_at") VALUES ($1, $2, $3, $4)`,
        [null, "Lớp Tự Do", null, null] // No school
      );
      db.query(
        `INSERT INTO "classes" ("school_id", "name", "student_count", "deleted_at") VALUES ($1, $2, $3, $4)`,
        [1, "Lớp Đã Xóa", 10, "2026-08-01T00:00:00Z"] // Deleted
      );

      const classes = db.query(`SELECT * FROM "classes"`);
      expect(classes.length).toBe(5);
    });
  });

  // ==========================================
  // LEVEL 3: DQL Querying & Filtering
  // ==========================================
  describe("LEVEL 3: DQL Querying & Filtering", () => {
    test("3.1 Point lookup by Primary Key (Fast-Path)", () => {
      const res1 = db.query(`SELECT * FROM "users" WHERE "id" = 1`);
      expect(res1.length).toBe(1);
      expect(res1[0].username).toBe("alice");

      const res2 = db.query(`SELECT * FROM "users" WHERE "id" = $1`, [2]);
      expect(res2.length).toBe(1);
      expect(res2[0].username).toBe("bob");
    });

    test("3.2 Non-PK exact string lookup", () => {
      const res = db.query(`SELECT "id", "score" FROM "users" WHERE "username" = $1`, ["charlie"]);
      expect(res.length).toBe(1);
      expect(res[0].id).toBe(3);
      expect(res[0].score).toBe(70.0);
    });

    test("3.3 Numeric comparison operators (>, <, >=, <=)", () => {
      const gt = db.query(`SELECT "username" FROM "users" WHERE "score" > 80`);
      expect(gt.length).toBe(2); // alice (95.5), bob (82.0)

      const lte = db.query(`SELECT "username" FROM "users" WHERE "score" <= 70`);
      expect(lte.length).toBe(1); // charlie (70.0)
    });

    test("3.4 BETWEEN operator", () => {
      const between = db.query(`SELECT "username" FROM "users" WHERE "score" BETWEEN 80 AND 100`);
      expect(between.length).toBe(2);
    });

    test("3.5 IS NULL and IS NOT NULL", () => {
      const nullEmails = db.query(`SELECT "username" FROM "users" WHERE "email" IS NULL`);
      expect(nullEmails.length).toBe(1);
      expect(nullEmails[0].username).toBe("david");

      const notNullEmails = db.query(`SELECT "username" FROM "users" WHERE "email" IS NOT NULL`);
      expect(notNullEmails.length).toBe(3);
    });

    test("3.6 String LOWER() and UPPER() case-insensitive matching", () => {
      const lowerMatch = db.query(
        `SELECT "id", "name" FROM "classes" WHERE lower(classes.name) = $1`,
        ["lớp lá 3"]
      );
      expect(lowerMatch.length).toBe(1);
      expect(lowerMatch[0].name).toBe("Lớp Lá 3");

      const upperMatch = db.query(
        `SELECT "id", "name" FROM "classes" WHERE upper("name") = $1`,
        ["LỚP MẦM 1"]
      );
      expect(upperMatch.length).toBe(1);
      expect(upperMatch[0].name).toBe("Lớp Mầm 1");
    });

    test("3.7 Compound WHERE condition (AND, IS NULL)", () => {
      const activeClasses = db.query(
        `SELECT "id", "name" FROM "classes" WHERE "classes"."deleted_at" IS NULL AND "classes"."school_id" = $1`,
        [1]
      );
      expect(activeClasses.length).toBe(2); // Lớp Mầm 1, Lớp Chồi 2
    });
  });

  // ==========================================
  // LEVEL 4: Sorting & Pagination
  // ==========================================
  describe("LEVEL 4: Sorting & Pagination", () => {
    test("4.1 ORDER BY ASC and DESC", () => {
      const asc = db.query(`SELECT "username" FROM "users" ORDER BY "id" ASC`);
      expect(asc[0].username).toBe("alice");
      expect(asc[asc.length - 1].username).toBe("david");

      const desc = db.query(`SELECT "username" FROM "users" ORDER BY "id" DESC`);
      expect(desc[0].username).toBe("david");
      expect(desc[desc.length - 1].username).toBe("alice");
    });

    test("4.2 LIMIT and OFFSET pagination", () => {
      const page1 = db.query(`SELECT "username" FROM "users" ORDER BY "id" ASC LIMIT 2`);
      expect(page1.length).toBe(2);
      expect(page1[0].username).toBe("alice");
      expect(page1[1].username).toBe("bob");

      const page2 = db.query(`SELECT "username" FROM "users" ORDER BY "id" ASC LIMIT 2 OFFSET 2`);
      expect(page2.length).toBe(2);
      expect(page2[0].username).toBe("charlie");
      expect(page2[1].username).toBe("david");
    });
  });

  // ==========================================
  // LEVEL 5: Aggregations
  // ==========================================
  describe("LEVEL 5: Aggregate Functions", () => {
    test("5.1 COUNT(*) on table", () => {
      const res = db.query(`SELECT COUNT(*) FROM "users"`);
      expect(res.length).toBe(1);
      expect(res[0].count).toBe(4);
    });

    test("5.2 COUNT(*) with WHERE clause", () => {
      const res = db.query(`SELECT COUNT(*) FROM "users" WHERE "is_active" = true`);
      expect(res.length).toBe(1);
      expect(res[0].count).toBe(3);
    });

    test("5.3 SUM, AVG, MIN, MAX calculations", () => {
      const res = db.query(`SELECT SUM("score"), AVG("score"), MIN("score"), MAX("score") FROM "users" WHERE "score" IS NOT NULL`);
      expect(res.length).toBe(1);
      // Scores: 95.5, 82.0, 70.0 => SUM = 247.5
      expect(res[0].sum).toBeCloseTo(247.5, 1);
      expect(res[0].avg).toBeCloseTo(82.5, 1);
      expect(res[0].min).toBe(70.0);
      expect(res[0].max).toBe(95.5);
    });
  });

  // ==========================================
  // LEVEL 6: Relational LEFT JOIN & Projections
  // ==========================================
  describe("LEVEL 6: Relational LEFT JOIN & Smart Projections", () => {
    test("6.1 LEFT JOIN with column aliases", () => {
      const sql = `
        SELECT 
          "classes"."id", 
          "classes"."name", 
          "classes"."school_id" as "schoolId", 
          "schools"."name" as "schoolName" 
        FROM "classes" 
        LEFT JOIN "schools" ON "schools"."id" = "classes"."school_id"
        ORDER BY "classes"."id" ASC
      `;
      const res = db.query(sql);
      expect(res.length).toBe(5);

      // Class 1 belongs to Hoa Mai School
      expect(res[0].name).toBe("Lớp Mầm 1");
      expect(res[0].schoolName).toBe("Hoa Mai School");
      expect(res[0].schoolId).toBe(1);

      // Class 3 belongs to Sao Mai School
      expect(res[2].name).toBe("Lớp Lá 3");
      expect(res[2].schoolName).toBe("Sao Mai School");

      // Class 4 has null school_id => schoolName must be null
      expect(res[3].name).toBe("Lớp Tự Do");
      expect(res[3].schoolName).toBeNull();
    });

    test("6.2 LEFT JOIN with COALESCE(CAST(...), default)", () => {
      const sql = `
        SELECT 
          "classes"."id", 
          "classes"."name", 
          COALESCE(CAST(classes.student_count AS INTEGER), 0) as "studentCount",
          "schools"."name" as "schoolName"
        FROM "classes" 
        LEFT JOIN "schools" ON "schools"."id" = "classes"."school_id"
        WHERE "classes"."name" = $1
      `;
      const res = db.query(sql, ["Lớp Tự Do"]);
      expect(res.length).toBe(1);
      expect(res[0].studentCount).toBe(0); // Coalesced from NULL to 0
    });

    test("6.3 Full production query structure with WHERE and JOIN", () => {
      const sql = `
        SELECT 
          "classes"."id", 
          "classes"."name", 
          "classes"."school_id" as "schoolId", 
          "schools"."name" as "schoolName", 
          COALESCE(CAST(classes.student_count AS INTEGER), 0) as "studentCount"
        FROM "classes" 
        LEFT JOIN "schools" ON "schools"."id" = "classes"."school_id" 
        WHERE "classes"."deleted_at" IS NULL AND "classes"."school_id" = $1 AND lower(classes.name) = $2
      `;
      const res = db.query(sql, [1, "lớp mầm 1"]);
      expect(res.length).toBe(1);
      expect(res[0].name).toBe("Lớp Mầm 1");
      expect(res[0].schoolName).toBe("Hoa Mai School");
      expect(res[0].studentCount).toBe(25);
    });

    test("6.4 Kysely-generated query with multiple WHEREs, ORDER BY DESC and parameterized LIMIT $2", () => {
      const sql = `
        SELECT 
          "classes"."id", 
          "schools"."name" as "schoolName", 
          "classes"."name" as "className", 
          "classes"."school_id" as "schoolId"
        FROM "classes" 
        LEFT JOIN "schools" ON "schools"."id" = "classes"."school_id" 
        WHERE "classes"."school_id" = $1 AND "classes"."deleted_at" IS NULL 
        ORDER BY "classes"."id" DESC 
        LIMIT $2
      `;
      const res = db.query(sql, [1, 1]);
      expect(res.length).toBe(1);
      expect(res[0].className).toBe("Lớp Chồi 2"); // DESC order gives ID 2 first
      expect(res[0].schoolName).toBe("Hoa Mai School");
    });
  });

  // ==========================================
  // LEVEL 7: DML UPDATE Operations
  // ==========================================
  describe("LEVEL 7: DML UPDATE Operations", () => {
    test("7.1 Single-column UPDATE with WHERE condition", () => {
      db.query(`UPDATE "classes" SET "student_count" = $1 WHERE "classes"."id" = $2`, [35, 1]);
      const res = db.query(`SELECT "student_count" FROM "classes" WHERE "id" = 1`);
      expect(res[0].student_count).toBe(35);
    });

    test("7.2 UPDATE with CURRENT_TIMESTAMP keyword", () => {
      const updateSql = `
        UPDATE "classes" 
        SET "deleted_at" = CURRENT_TIMESTAMP, "student_count" = $1 
        WHERE "classes"."id" = $2
      `;
      db.query(updateSql, [40, 2]);

      const res = db.query(`SELECT "student_count", "deleted_at" FROM "classes" WHERE "id" = 2`);
      expect(res[0].student_count).toBe(40);
      expect(res[0].deleted_at).toBeDefined();
      expect(typeof res[0].deleted_at).toBe("string");
    });
  });

  // ==========================================
  // LEVEL 8: DML DELETE Operations
  // ==========================================
  describe("LEVEL 8: DML DELETE Operations", () => {
    test("8.1 DELETE with WHERE clause", () => {
      db.query(`DELETE FROM "classes" WHERE "id" = $1`, [4]); // Delete "Lớp Tự Do"
      const res = db.query(`SELECT * FROM "classes" WHERE "id" = 4`);
      expect(res.length).toBe(0);
    });

    test("8.2 Verify remaining row count after DELETE", () => {
      const remaining = db.query(`SELECT COUNT(*) FROM "classes"`);
      expect(remaining[0].count).toBe(4);
    });
  });

  // ==========================================
  // LEVEL 9: Persistence & WAL Crash Recovery
  // ==========================================
  describe("LEVEL 9: Binary WAL Persistence & Recovery", () => {
    const PERSIST_DB = "test_persist.db";

    beforeAll(() => {
      cleanFiles(PERSIST_DB);
    });

    afterAll(() => {
      cleanFiles(PERSIST_DB);
    });

    test("9.1 Write records and flush WAL through PGLiteNative", async () => {
      const dbInstance1 = new PGLiteNative(PERSIST_DB, { native: true });
      await dbInstance1.exec(`CREATE TABLE "items" ("id" SERIAL PRIMARY KEY, "title" TEXT, "price" FLOAT)`);
      await dbInstance1.query(`INSERT INTO "items" ("title", "price") VALUES ($1, $2)`, ["Product A", 19.99]);
      await dbInstance1.query(`INSERT INTO "items" ("title", "price") VALUES ($1, $2)`, ["Product B", 49.50]);
      await dbInstance1.query(`UPDATE "items" SET "price" = $1 WHERE "id" = $2`, [24.99, 1]);

      const rows = await dbInstance1.query(`SELECT * FROM "items"`);
      expect(rows.length).toBe(2);
      expect(rows[0].price).toBe(24.99);

      await dbInstance1.close();
    });

    test("9.2 Re-open database instance and verify data integrity across reboots", async () => {
      // Create new instance on the same file
      const dbInstance2 = new PGLiteNative(PERSIST_DB, { native: true });
      const res = await dbInstance2.query2(`SELECT "id", "title", "price" FROM "items" ORDER BY "id" ASC`);
      expect(res.rows.length).toBe(2);
      expect(res.rows[0].title).toBe("Product A");
      expect(Number(res.rows[0].price)).toBe(24.99); // Verified updated price recovered
      expect(res.rows[1].title).toBe("Product B");
      expect(Number(res.rows[1].price)).toBe(49.50);
    });
  });

  // ==========================================
  // LEVEL 10: JIT Auto-Hydration in PGLiteNative
  // ==========================================
  describe("LEVEL 10: TypeScript JIT Auto-Hydration Integration", () => {
    test("10.1 Auto-hydrate table from JS to Rust and execute query", async () => {
      const pgNative = new PGLiteNative(":memory:", { native: true });

      // Populate JS engine with a table
      await (pgNative as any).getJsEngine().exec(`
        CREATE TABLE "auto_test" ("id" SERIAL PRIMARY KEY, "val" TEXT)
      `);
      await (pgNative as any).getJsEngine().query(`
        INSERT INTO "auto_test" ("val") VALUES ($1)
      `, ["Hydrated Data"]);

      // First query triggers auto-hydration to Rust
      const res1 = await pgNative.query2(`SELECT * FROM "auto_test" WHERE "id" = $1`, [1]);
      expect(res1.rows.length).toBe(1);
      expect(res1.rows[0].val).toBe("Hydrated Data");

      // Second query runs on pure Rust Native
      const res2 = await pgNative.query2(`SELECT * FROM "auto_test" WHERE "id" = $1`, [1]);
      expect(res2.rows.length).toBe(1);
      expect(res2.rows[0].val).toBe("Hydrated Data");
    });
  });

  // ==========================================
  // LEVEL 11: Complex IN Conditions & Multi-value Filtering
  // ==========================================
  describe("LEVEL 11: IN Lists & Multi-value Filtering", () => {
    test("11.1 IN clause with literal numeric list", () => {
      const res = db.query(`SELECT "id", "name" FROM "classes" WHERE "id" IN (1, 2, 99) ORDER BY "id" ASC`);
      expect(res.length).toBe(2);
      expect(res[0].name).toBe("Lớp Mầm 1");
      expect(res[1].name).toBe("Lớp Chồi 2");
    });

    test("11.2 IN clause combined with secondary conditions", () => {
      const res = db.query(`SELECT "id", "name" FROM "classes" WHERE "id" IN (1, 3, 4) AND "school_id" = $1`, [1]);
      expect(res.length).toBe(1);
      expect(res[0].name).toBe("Lớp Mầm 1");
    });
  });

  // ==========================================
  // LEVEL 12: Comparison Edge Cases & Type Coercion
  // ==========================================
  describe("LEVEL 12: Comparison Operators & Data Types", () => {
    test("12.1 Numeric inequality != and <>", () => {
      const notEq1 = db.query(`SELECT "username" FROM "users" WHERE "score" != 95.5 AND "score" IS NOT NULL`);
      expect(notEq1.length).toBe(2); // bob (82.0), charlie (70.0)

      const notEq2 = db.query(`SELECT "username" FROM "users" WHERE "score" <> 70.0 AND "score" IS NOT NULL`);
      expect(notEq2.length).toBe(2); // alice (95.5), bob (82.0)
    });

    test("12.2 String comparisons and sorting", () => {
      const sortedUsers = db.query(`SELECT "username" FROM "users" ORDER BY "username" ASC`);
      expect(sortedUsers[0].username).toBe("alice");
      expect(sortedUsers[sortedUsers.length - 1].username).toBe("david");
    });

    test("12.3 Boolean column filtering", () => {
      const active = db.query(`SELECT "username" FROM "users" WHERE "is_active" = true`);
      expect(active.length).toBe(3);

      const inactive = db.query(`SELECT "username" FROM "users" WHERE "is_active" = false`);
      expect(inactive.length).toBe(1);
      expect(inactive[0].username).toBe("charlie");
    });
  });

  // ==========================================
  // LEVEL 13: Relational INNER JOIN & Complex Multi-column Expressions
  // ==========================================
  describe("LEVEL 13: Relational INNER JOIN & Multi-column Expressions", () => {
    test("13.1 INNER JOIN only returns matching records", () => {
      const sql = `
        SELECT 
          "classes"."id", 
          "classes"."name" as "className", 
          "schools"."name" as "schoolName"
        FROM "classes" 
        INNER JOIN "schools" ON "schools"."id" = "classes"."school_id"
        ORDER BY "classes"."id" ASC
      `;
      const res = db.query(sql);
      // Lớp Tự Do has no school_id, so it must not be in INNER JOIN results
      expect(res.length).toBe(4);
      expect(res.every((r: any) => r.schoolName !== null && r.schoolName !== undefined)).toBe(true);
    });

    test("13.2 LEFT JOIN with non-existent foreign keys returns NULL for joined columns", () => {
      db.query(`INSERT INTO "classes" ("name", "school_id") VALUES ($1, $2)`, ["Lớp Mới Không Trường", null]);
      const sql = `
        SELECT 
          "classes"."name" as "className", 
          "schools"."name" as "schoolName"
        FROM "classes" 
        LEFT JOIN "schools" ON "schools"."id" = "classes"."school_id"
        WHERE "classes"."name" = $1
      `;
      const res = db.query(sql, ["Lớp Mới Không Trường"]);
      expect(res.length).toBe(1);
      expect(res[0].className).toBe("Lớp Mới Không Trường");
      expect(res[0].schoolName).toBeNull();
    });
  });

  // ==========================================
  // LEVEL 14: Batch INSERT & Large Volume Verification
  // ==========================================
  describe("LEVEL 14: Bulk Operations & Scalability", () => {
    test("14.1 Insert 100 records sequentially and verify aggregations", () => {
      db.exec(`CREATE TABLE "metrics" ("id" SERIAL PRIMARY KEY, "sensor_id" INT, "reading" FLOAT)`);
      for (let i = 1; i <= 100; i++) {
        db.query(`INSERT INTO "metrics" ("sensor_id", "reading") VALUES ($1, $2)`, [i % 5, i * 1.5]);
      }

      const countRes = db.query(`SELECT COUNT(*) FROM "metrics"`);
      expect(countRes[0].count).toBe(100);

      const sensor0 = db.query(`SELECT COUNT(*), SUM("reading"), AVG("reading") FROM "metrics" WHERE "sensor_id" = 0`);
      expect(sensor0[0].count).toBe(20); // 20 readings for sensor 0 (5, 10, ..., 100)
    });

    test("14.2 Point lookup performance and indexing check on 100 records", () => {
      const res = db.query(`SELECT * FROM "metrics" WHERE "id" = $1`, [50]);
      expect(res.length).toBe(1);
      expect(res[0].id).toBe(50);
      expect(res[0].reading).toBe(75.0);
    });
  });

  // ==========================================
  // LEVEL 15: Unified Engine Full Pipeline (Native + Fallback)
  // ==========================================
  describe("LEVEL 15: Unified Engine Full Pipeline (PGLite Native + Auto Fallback)", () => {
    let pglite: PGLiteNative;

    beforeAll(() => {
      pglite = new PGLiteNative(":memory:");
    });

    afterAll(async () => {
      await pglite.close();
    });

    test("15.1 Basic CRUD through unified PGLite instance", async () => {
      await pglite.exec(`CREATE TABLE "orders" ("id" SERIAL PRIMARY KEY, "total" FLOAT, "status" TEXT)`);
      await pglite.query(`INSERT INTO "orders" ("total", "status") VALUES ($1, $2)`, [150.0, "PAID"]);
      await pglite.query(`INSERT INTO "orders" ("total", "status") VALUES ($1, $2)`, [300.5, "PENDING"]);

      const rows = await pglite.query(`SELECT * FROM "orders" WHERE "status" = $1`, ["PAID"]);
      expect(rows.length).toBe(1);
      expect(rows[0].total).toBe(150.0);
    });

    test("15.2 Fallback for Subqueries in WHERE", async () => {
      await pglite.query(`INSERT INTO "orders" ("total", "status") VALUES ($1, $2)`, [50.0, "CANCELLED"]);
      
      const res = await pglite.query2(`
        SELECT "id", "total" FROM "orders" 
        WHERE "total" > (SELECT AVG("total") FROM "orders")
      `);
      expect(res.rows.length).toBeGreaterThanOrEqual(1);
      expect(Number(res.rows[0].total)).toBe(300.5);
    });

    test("15.3 Fallback for CTE (Common Table Expressions)", async () => {
      const res = await pglite.query2(`
        WITH paid_orders AS (
          SELECT * FROM "orders" WHERE "status" = 'PAID'
        )
        SELECT "id", "total" FROM paid_orders
      `);
      expect(res.rows.length).toBe(1);
      expect(Number(res.rows[0].total)).toBe(150.0);
    });

    test("15.4 Fallback for Window Functions OVER (PARTITION BY)", async () => {
      const res = await pglite.query2(`
        SELECT 
          "id", 
          "total", 
          "status",
          ROW_NUMBER() OVER (PARTITION BY "status" ORDER BY "total" DESC) as row_num
        FROM "orders"
      `);
      expect(res.rows.length).toBe(3);
    });

    test("15.5 Fallback for Transactions", async () => {
      const res = await pglite.transaction(async (tx) => {
        await tx.exec(`INSERT INTO "orders" ("total", "status") VALUES (999.99, 'TX_TEST')`);
        return await tx.query(`SELECT * FROM "orders" WHERE "status" = 'TX_TEST'`);
      });
      expect(res.length).toBe(1);
      expect(Number(res[0].total)).toBe(999.99);
    });
  });

  // ==========================================
  // LEVEL 16: Parameterized Type Fidelity & Extreme Values
  // ==========================================
  describe("LEVEL 16: Parameterized Type Fidelity & Extreme Values", () => {
    test("16.1 Handle integer limits and negative values", () => {
      db.exec(`CREATE TABLE "num_types" ("id" SERIAL PRIMARY KEY, "big_num" FLOAT, "small_num" INT, "neg_num" FLOAT)`);
      db.query(`INSERT INTO "num_types" ("big_num", "small_num", "neg_num") VALUES ($1, $2, $3)`, [
        9007199254740991,
        2147483647,
        -123456.789
      ]);
      const res = db.query(`SELECT * FROM "num_types" WHERE "small_num" = $1`, [2147483647]);
      expect(res.length).toBe(1);
      expect(res[0].big_num).toBe(9007199254740991);
      expect(res[0].neg_num).toBe(-123456.789);
    });

    test("16.2 Handle special strings with single quotes, double quotes and escape characters", () => {
      db.exec(`CREATE TABLE "escaped_texts" ("id" SERIAL PRIMARY KEY, "content" TEXT)`);
      const payload = `It's a "quoted" string with \n newline and \t tabs and unicode: 🚀 Tiếng Việt`;
      db.query(`INSERT INTO "escaped_texts" ("content") VALUES ($1)`, [payload]);

      const res = db.query(`SELECT "content" FROM "escaped_texts" WHERE "id" = 1`);
      expect(res.length).toBe(1);
      expect(res[0].content).toBe(payload);
    });
  });

  // ==========================================
  // LEVEL 17: String Filtering & Pattern Matching
  // ==========================================
  describe("LEVEL 17: String Filtering & Case Variations", () => {
    beforeAll(() => {
      db.exec(`CREATE TABLE "products" ("id" SERIAL PRIMARY KEY, "sku" TEXT, "title" TEXT, "price" FLOAT)`);
      db.query(`INSERT INTO "products" ("sku", "title", "price") VALUES ('PROD-A1', 'Mechanical Keyboard RGB', 99.99)`);
      db.query(`INSERT INTO "products" ("sku", "title", "price") VALUES ('PROD-B2', 'Wireless Gaming Mouse', 49.50)`);
      db.query(`INSERT INTO "products" ("sku", "title", "price") VALUES ('PROD-C3', 'Ultra-Wide 4K Monitor', 499.00)`);
      db.query(`INSERT INTO "products" ("sku", "title", "price") VALUES ('PROD-D4', 'Noise Cancelling Headphones', 199.99)`);
    });

    test("17.1 Exact SKU Point Lookup", () => {
      const res = db.query(`SELECT "title", "price" FROM "products" WHERE "sku" = 'PROD-B2'`);
      expect(res.length).toBe(1);
      expect(res[0].title).toBe("Wireless Gaming Mouse");
    });

    test("17.2 Price range filtering with compound AND", () => {
      const res = db.query(`SELECT "sku", "price" FROM "products" WHERE "price" >= 50.0 AND "price" <= 200.0 ORDER BY "price" ASC`);
      expect(res.length).toBe(2);
      expect(res[0].sku).toBe("PROD-A1");
      expect(res[1].sku).toBe("PROD-D4");
    });
  });

  // ==========================================
  // LEVEL 18: Multi-Table 3-Way Relational JOIN
  // ==========================================
  describe("LEVEL 18: Multi-Table Relational Integrity", () => {
    beforeAll(() => {
      db.exec(`CREATE TABLE "departments" ("id" SERIAL PRIMARY KEY, "dept_name" TEXT)`);
      db.exec(`CREATE TABLE "employees" ("id" SERIAL PRIMARY KEY, "name" TEXT, "dept_id" INT)`);
      db.exec(`CREATE TABLE "projects" ("id" SERIAL PRIMARY KEY, "proj_name" TEXT, "emp_id" INT)`);

      db.query(`INSERT INTO "departments" ("dept_name") VALUES ('Engineering')`);
      db.query(`INSERT INTO "departments" ("dept_name") VALUES ('Marketing')`);

      db.query(`INSERT INTO "employees" ("name", "dept_id") VALUES ('Alex', 1)`);
      db.query(`INSERT INTO "employees" ("name", "dept_id") VALUES ('Beth', 1)`);
      db.query(`INSERT INTO "employees" ("name", "dept_id") VALUES ('Charlie', 2)`);
      db.query(`INSERT INTO "employees" ("name", "dept_id") VALUES ('Dana', NULL)`);

      db.query(`INSERT INTO "projects" ("proj_name", "emp_id") VALUES ('Rust Engine', 1)`);
      db.query(`INSERT INTO "projects" ("proj_name", "emp_id") VALUES ('Web App', 2)`);
    });

    test("18.1 LEFT JOIN between employees and departments with NULL handling", () => {
      const res = db.query(`
        SELECT 
          "employees"."name" as emp_name, 
          "departments"."dept_name" as department
        FROM "employees"
        LEFT JOIN "departments" ON "departments"."id" = "employees"."dept_id"
        ORDER BY "employees"."id" ASC
      `);
      expect(res.length).toBe(4);
      expect(res[0].emp_name).toBe("Alex");
      expect(res[0].department).toBe("Engineering");
      expect(res[3].emp_name).toBe("Dana");
      expect(res[3].department).toBeNull();
    });

    test("18.2 Relational Filter with WHERE and JOIN", () => {
      const res = db.query(`
        SELECT 
          "employees"."name" as emp_name,
          "departments"."dept_name" as dept
        FROM "employees"
        LEFT JOIN "departments" ON "departments"."id" = "employees"."dept_id"
        WHERE "employees"."dept_id" = 1
        ORDER BY "employees"."name" ASC
      `);
      expect(res.length).toBe(2);
      expect(res[0].emp_name).toBe("Alex");
      expect(res[1].emp_name).toBe("Beth");
    });
  });

  // ==========================================
  // LEVEL 19: High-Volume Batch Operations & Scalability (1,000 Records)
  // ==========================================
  describe("LEVEL 19: High-Volume Scalability & Indexing (1,000 Records)", () => {
    test("19.1 Insert 1,000 records in batch and perform aggregations", () => {
      db.exec(`CREATE TABLE "sensor_logs" ("id" SERIAL PRIMARY KEY, "device_id" INT, "temp" FLOAT, "status" TEXT)`);

      for (let i = 1; i <= 1000; i++) {
        const devId = (i % 10) + 1;
        const temp = 20.0 + (i % 30);
        const status = i % 5 === 0 ? "ALERT" : "NORMAL";
        db.query(`INSERT INTO "sensor_logs" ("device_id", "temp", "status") VALUES ($1, $2, $3)`, [devId, temp, status]);
      }

      const countRes = db.query(`SELECT COUNT(*) as total FROM "sensor_logs"`);
      expect(countRes[0].total).toBe(1000);

      const alertCount = db.query(`SELECT COUNT(*) as alerts FROM "sensor_logs" WHERE "status" = 'ALERT'`);
      expect(alertCount[0].alerts).toBe(200);

      const stats = db.query(`SELECT MIN("temp") as min_temp, MAX("temp") as max_temp, AVG("temp") as avg_temp FROM "sensor_logs"`);
      expect(stats[0].min_temp).toBe(20.0);
      expect(stats[0].max_temp).toBe(49.0);
    });

    test("19.2 Point Lookup Performance across 1,000 records", () => {
      const res = db.query(`SELECT * FROM "sensor_logs" WHERE "id" = 500`);
      expect(res.length).toBe(1);
      expect(res[0].id).toBe(500);
      expect(res[0].device_id).toBe((500 % 10) + 1);
    });
  });

  // ==========================================
  // LEVEL 20: Complex Kysely Query Builder Simulation
  // ==========================================
  describe("LEVEL 20: Complex Kysely Query Builder Simulation", () => {
    beforeAll(() => {
      db.exec(`CREATE TABLE "retreat_programs" ("id" SERIAL PRIMARY KEY, "name" TEXT, "duration_days" INT)`);
      db.exec(`
        CREATE TABLE "bookings" (
          "id" SERIAL PRIMARY KEY,
          "program_id" INT,
          "user_id" INT,
          "check_in_date" TEXT,
          "check_out_date" TEXT,
          "check_in_status" TEXT,
          "deleted_at" TIMESTAMP
        )
      `);

      db.query(`INSERT INTO "retreat_programs" ("name", "duration_days") VALUES ('Mindfulness 7 Days', 7)`);
      db.query(`INSERT INTO "retreat_programs" ("name", "duration_days") VALUES ('Yoga Detox Weekend', 3)`);

      db.query(`
        INSERT INTO "bookings" ("program_id", "user_id", "check_in_date", "check_out_date", "check_in_status", "deleted_at")
        VALUES (1, 42, '2026-10-01', '2026-10-08', 'CONFIRMED', NULL)
      `);
      db.query(`
        INSERT INTO "bookings" ("program_id", "user_id", "check_in_date", "check_out_date", "check_in_status", "deleted_at")
        VALUES (2, 42, '2026-11-15', '2026-11-18', 'PENDING', NULL)
      `);
      db.query(`
        INSERT INTO "bookings" ("program_id", "user_id", "check_in_date", "check_out_date", "check_in_status", "deleted_at")
        VALUES (1, 42, '2026-08-01', '2026-08-08', 'CANCELLED', '2026-08-02T10:00:00Z')
      `);
      db.query(`
        INSERT INTO "bookings" ("program_id", "user_id", "check_in_date", "check_out_date", "check_in_status", "deleted_at")
        VALUES (1, 99, '2026-12-01', '2026-12-08', 'CONFIRMED', NULL)
      `);
    });

    test("20.1 Kysely executeTakeFirst() query with Left Join, Aliases, Null Check, ORDER BY DESC and Parameterized LIMIT", () => {
      const sql = `
        SELECT 
          "bookings"."id",
          "retreat_programs"."name" as "programName",
          "bookings"."check_in_date" as "checkInDate",
          "bookings"."check_out_date" as "checkOutDate",
          "bookings"."check_in_status" as "status"
        FROM "bookings"
        LEFT JOIN "retreat_programs" ON "retreat_programs"."id" = "bookings"."program_id"
        WHERE "bookings"."user_id" = $1
          AND "bookings"."deleted_at" IS NULL
        ORDER BY "bookings"."check_in_date" DESC
        LIMIT $2
      `;

      const rows = db.query(sql, [42, 1]);
      expect(rows.length).toBe(1);
      expect(rows[0].programName).toBe("Yoga Detox Weekend");
      expect(rows[0].checkInDate).toBe("2026-11-15");
      expect(rows[0].status).toBe("PENDING");
    });

    test("20.2 Kysely query with LIMIT 2 and OFFSET 1", () => {
      const sql = `
        SELECT 
          "bookings"."id",
          "retreat_programs"."name" as "programName",
          "bookings"."check_in_date" as "checkInDate"
        FROM "bookings"
        LEFT JOIN "retreat_programs" ON "retreat_programs"."id" = "bookings"."program_id"
        WHERE "bookings"."user_id" = $1
          AND "bookings"."deleted_at" IS NULL
        ORDER BY "bookings"."check_in_date" DESC
        LIMIT 1 OFFSET 1
      `;

      const rows = db.query(sql, [42]);
      expect(rows.length).toBe(1);
      expect(rows[0].programName).toBe("Mindfulness 7 Days");
      expect(rows[0].checkInDate).toBe("2026-10-01");
    });
  });

  // ==========================================
  // LEVEL 21: Direct Rust WAL Checkpointing & Crash Recovery
  // ==========================================
  describe("LEVEL 21: Direct Rust WAL Checkpointing & Crash Recovery", () => {
    const WAL_CRASH_DB = "test_wal_crash.db";

    beforeAll(() => {
      cleanFiles(WAL_CRASH_DB);
    });

    afterAll(() => {
      cleanFiles(WAL_CRASH_DB);
    });

    test("21.1 Write data and verify automatic flush on close", async () => {
      const engine1 = new PGLiteNative(WAL_CRASH_DB);
      await engine1.exec(`CREATE TABLE "crash_test" ("id" SERIAL PRIMARY KEY, "val" TEXT)`);
      await engine1.query(`INSERT INTO "crash_test" ("val") VALUES ('Persistence Verification 1')`);
      await engine1.query(`INSERT INTO "crash_test" ("val") VALUES ('Persistence Verification 2')`);
      await engine1.close();

      const engine2 = new PGLiteNative(WAL_CRASH_DB);
      const rows = await engine2.query(`SELECT * FROM "crash_test" ORDER BY "id" ASC`);
      expect(rows.length).toBe(2);
      expect(rows[0].val).toBe("Persistence Verification 1");
      expect(rows[1].val).toBe("Persistence Verification 2");
      await engine2.close();
    });
  });
});

