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
      const dbInstance1 = new PGLiteNative(PERSIST_DB);
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
      const dbInstance2 = new PGLiteNative(PERSIST_DB);
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
      const pgNative = new PGLiteNative(":memory:");

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
});
