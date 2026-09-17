import { describe, test, expect, beforeEach, afterEach } from "bun:test";
import { PGLiteNative } from "../src/native";
import * as fs from "fs";
import * as path from "path";

const TEST_DB_PATH = path.join(__dirname, "test_alter_dedup.db");

describe("ALTER TABLE Deduplication & Schema Integrity Test Suite", () => {
  const cleanup = () => {
    try {
      if (fs.existsSync(TEST_DB_PATH)) fs.unlinkSync(TEST_DB_PATH);
      if (fs.existsSync(`${TEST_DB_PATH}.rwal`)) fs.unlinkSync(`${TEST_DB_PATH}.rwal`);
      if (fs.existsSync(`${TEST_DB_PATH}.v2.rwal`)) fs.unlinkSync(`${TEST_DB_PATH}.v2.rwal`);
      if (fs.existsSync(`${TEST_DB_PATH}.wal`)) fs.unlinkSync(`${TEST_DB_PATH}.wal`);
    } catch {}
  };

  beforeEach(cleanup);
  afterEach(cleanup);

  test("1. ALTER TABLE ADD COLUMN IF NOT EXISTS does not create duplicate columns and preserves values", async () => {
    const db = new PGLiteNative(TEST_DB_PATH);

    await db.query(`
      CREATE TABLE locations (
        id SERIAL PRIMARY KEY,
        name TEXT,
        address TEXT
      );
    `);

    await db.query(`
      INSERT INTO locations (id, name, address)
      VALUES (1, 'Thiền đường Trúc Lâm', 'Đà Nẵng');
    `);

    // Repeated migrations with IF NOT EXISTS
    await db.query(`ALTER TABLE locations ADD COLUMN IF NOT EXISTS name TEXT;`);
    await db.query(`ALTER TABLE locations ADD COLUMN IF NOT EXISTS name TEXT;`);
    await db.query(`ALTER TABLE locations ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP;`);
    await db.query(`ALTER TABLE locations ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP;`);

    // Query full result
    const res: any = await db.query2(`SELECT * FROM locations WHERE id = 1;`);
    expect(res.fields.filter((f: any) => f.name.toLowerCase() === "name").length).toBe(1);
    expect(res.fields.filter((f: any) => f.name.toLowerCase() === "updated_at").length).toBe(1);
    expect(res.rows[0].name).toBe("Thiền đường Trúc Lâm");
    expect(res.rows[0].address).toBe("Đà Nẵng");

    // UPDATE name and verify value is not nullified
    await db.query(`UPDATE locations SET name = 'Thiền đường Mới' WHERE id = 1;`);
    const resAfterUpdate: any = await db.query2(`SELECT * FROM locations WHERE id = 1;`);
    expect(resAfterUpdate.rows[0].name).toBe("Thiền đường Mới");

    await db.close();
  });

  test("2. ALTER TABLE ADD COLUMN without IF NOT EXISTS throws error when column already exists", async () => {
    const db = new PGLiteNative(TEST_DB_PATH);

    await db.query(`
      CREATE TABLE users (
        id SERIAL PRIMARY KEY,
        email TEXT
      );
    `);

    let errorThrown = false;
    try {
      await db.query(`ALTER TABLE users ADD COLUMN email TEXT;`);
    } catch (e: any) {
      errorThrown = true;
      expect(e.message).toContain("already exists");
    }
    expect(errorThrown).toBe(true);

    await db.close();
  });

  test("3. ALTER TABLE DROP COLUMN removes column and correctly aligns row data", async () => {
    const db = new PGLiteNative(TEST_DB_PATH);

    await db.query(`
      CREATE TABLE products (
        id SERIAL PRIMARY KEY,
        name TEXT,
        temp_col TEXT,
        price NUMERIC
      );
    `);

    await db.query(`
      INSERT INTO products (id, name, temp_col, price)
      VALUES (1, 'Trà Shan Tuyết', 'Xóa tôi', 150000);
    `);

    // Drop column temp_col
    await db.query(`ALTER TABLE products DROP COLUMN temp_col;`);

    const res: any = await db.query2(`SELECT * FROM products WHERE id = 1;`);
    expect(res.fields.some((f: any) => f.name === "temp_col")).toBe(false);
    expect(res.rows[0].temp_col).toBeUndefined();
    expect(res.rows[0].name).toBe("Trà Shan Tuyết");
    expect(Number(res.rows[0].price)).toBe(150000);

    // DROP COLUMN IF EXISTS on non-existent column should not throw
    await db.query(`ALTER TABLE products DROP COLUMN IF EXISTS temp_col;`);

    await db.close();
  });

  test("5. Multi-duplicate ALTER TABLE with soft-delete: auto-aligns columns so deleted_at remains null and active rows are returned", async () => {
    const db1 = new PGLiteNative(TEST_DB_PATH);

    await db1.query(`
      CREATE TABLE locations (
        id SERIAL PRIMARY KEY,
        name TEXT,
        address TEXT,
        created_at TIMESTAMP
      );
    `);

    await db1.query(`
      INSERT INTO locations (id, name, address, created_at)
      VALUES (1, 'Thiền đường Trúc Lâm', 'Đà Nẵng', '2026-07-21T12:27:41.270Z');
    `);

    // Simulate duplicate migrations executed on older engines
    await db1.query(`ALTER TABLE locations ADD COLUMN IF NOT EXISTS name TEXT;`);
    await db1.query(`ALTER TABLE locations ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP;`);
    await db1.query(`ALTER TABLE locations ADD COLUMN IF NOT EXISTS name TEXT;`);
    await db1.query(`ALTER TABLE locations ADD COLUMN IF NOT EXISTS deleted_at TIMESTAMP;`);
    await db1.query(`ALTER TABLE locations ADD COLUMN IF NOT EXISTS status TEXT;`);

    // Update row 1 with an updated_at timestamp
    await db1.query(`UPDATE locations SET updated_at = '2026-07-21T17:07:20.745Z' WHERE id = 1;`);

    // Insert row 2 after migrations
    await db1.query(`
      INSERT INTO locations (id, name, address, created_at, updated_at, deleted_at, status)
      VALUES (2, 'Khu vườn Yoga', 'Hồ Chí Minh', '2026-08-24T07:43:02.667Z', '2026-09-08T06:28:40.302Z', NULL, 'active');
    `);

    await db1.flush();
    await db1.close();

    // Reopen database
    const db2 = new PGLiteNative(TEST_DB_PATH);

    // Query active locations (WHERE deleted_at IS NULL)
    const activeRows: any = await db2.query(`SELECT * FROM locations WHERE deleted_at IS NULL;`);
    expect(activeRows.length).toBe(2);

    const row1 = activeRows.find((r: any) => r.id === 1);
    expect(row1.name).toBe("Thiền đường Trúc Lâm");
    expect(row1.updated_at).toBe("2026-07-21T17:07:20.745Z");
    expect(row1.deleted_at).toBeNull();

    const row2 = activeRows.find((r: any) => r.id === 2);
    expect(row2.name).toBe("Khu vườn Yoga");
    expect(row2.updated_at).toBe("2026-09-08T06:28:40.302Z");
    expect(row2.deleted_at).toBeNull();
    expect(row2.status).toBe("active");

    await db2.close();
  });
});
