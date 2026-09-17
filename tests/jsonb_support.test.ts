import { expect, test, describe, beforeAll, afterAll } from "bun:test";
import { PGLiteNative } from "../src/native";
import { unlinkSync, existsSync } from "fs";

const TEST_DB = "test_jsonb_migration.db";

function cleanFiles(prefix: string) {
  for (const ext of ["", ".wal", ".rwal", ".lock"]) {
    if (existsSync(prefix + ext)) {
      try { unlinkSync(prefix + ext); } catch {}
    }
  }
}

describe("JSONB Native Rust Engine Support Test Suite", () => {
  beforeAll(() => {
    cleanFiles(TEST_DB);
  });

  afterAll(() => {
    cleanFiles(TEST_DB);
  });

  test("1. Create DB with JSONB and timestamps, then verify Native Rust reads it properly", async () => {
    // Step 1: Create table and insert via native Rust engine
    const initDb = new PGLiteNative(TEST_DB);
    await initDb.exec(`
      CREATE TABLE "users" (
        "id" SERIAL PRIMARY KEY,
        "username" TEXT NOT NULL,
        "profile" JSONB,
        "tags" JSONB,
        "created_at" TIMESTAMP
      )
    `);

    await initDb.query(`
      INSERT INTO "users" ("username", "profile", "tags", "created_at")
      VALUES ($1, $2, $3, $4)
    `, [
      "alice",
      { role: "admin", address: { city: "Hanoi", zip: "10000" }, age: 28 },
      ["typescript", "rust", "database"],
      "2026-09-08T10:00:00.000Z"
    ]);

    await initDb.query(`
      INSERT INTO "users" ("username", "profile", "tags", "created_at")
      VALUES ($1, $2, $3, $4)
    `, [
      "bob",
      { role: "user", address: { city: "Saigon", zip: "70000" }, age: 32 },
      ["javascript", "python"],
      "2026-09-08T11:00:00.000Z"
    ]);

    await initDb.close();

    // Step 2: Open with PGLiteNative (Rust engine)
    const nativeDb = new PGLiteNative(TEST_DB, { autoFallback: false });

    // Verify SELECT *
    const rows = await nativeDb.query(`SELECT * FROM "users" ORDER BY "id" ASC`);
    expect(rows.length).toBe(2);

    // Verify row 1 (alice)
    expect(rows[0].username).toBe("alice");
    expect(typeof rows[0].profile).toBe("object");
    expect(rows[0].profile).not.toBeNull();
    expect(rows[0].profile.role).toBe("admin");
    expect(rows[0].profile.address.city).toBe("Hanoi");
    expect(Array.isArray(rows[0].tags)).toBe(true);
    expect(rows[0].tags).toEqual(["typescript", "rust", "database"]);

    // Verify row 2 (bob)
    expect(rows[1].username).toBe("bob");
    expect(typeof rows[1].profile).toBe("object");
    expect(rows[1].profile.role).toBe("user");
    expect(rows[1].profile.address.city).toBe("Saigon");
    expect(rows[1].tags).toEqual(["javascript", "python"]);
  });

  test("2. Test JSON extraction operator ->> (as text) in Native Rust", async () => {
    const nativeDb = new PGLiteNative(TEST_DB, { autoFallback: false });

    const rows = await nativeDb.query(`
      SELECT "username", "profile"->>'role' AS role, "tags"->>0 AS first_tag
      FROM "users"
      ORDER BY "id" ASC
    `);

    expect(rows.length).toBe(2);
    expect(rows[0].role).toBe("admin");
    expect(rows[0].first_tag).toBe("typescript");
    expect(rows[1].role).toBe("user");
    expect(rows[1].first_tag).toBe("javascript");
  });

  test("3. Test chained JSON extraction -> and ->> in Native Rust", async () => {
    const nativeDb = new PGLiteNative(TEST_DB, { autoFallback: false });

    const rows = await nativeDb.query(`
      SELECT "username", "profile"->'address'->>'city' AS city
      FROM "users"
      WHERE "profile"->>'role' = 'admin'
    `);

    expect(rows.length).toBe(1);
    expect(rows[0].username).toBe("alice");
    expect(rows[0].city).toBe("Hanoi");
  });

  test("4. Test WHERE filtering using JSON operator ->>", async () => {
    const nativeDb = new PGLiteNative(TEST_DB, { autoFallback: false });

    const resAdmin = await nativeDb.query(`
      SELECT * FROM "users" WHERE "profile"->>'role' = 'admin'
    `);
    expect(resAdmin.length).toBe(1);
    expect(resAdmin[0].username).toBe("alice");

    const resUser = await nativeDb.query(`
      SELECT * FROM "users" WHERE "profile"->>'role' = 'user'
    `);
    expect(resUser.length).toBe(1);
    expect(resUser[0].username).toBe("bob");
  });
});
