import { expect, test, describe, beforeAll, afterAll } from "bun:test";
import { LitePostgres } from "../src/database";
import { unlinkSync, existsSync } from "fs";
import { NodeFSAdapter } from "../src/adapters/node";

const DB_FILE = "test_alter_multi.db";

describe("LEVEL 75: Multiple ALTER Actions and Table Constraints", () => {
  let db: LitePostgres;

  beforeAll(() => {
    if (existsSync(DB_FILE)) unlinkSync(DB_FILE);
    if (existsSync(DB_FILE + ".wal")) unlinkSync(DB_FILE + ".wal");
    db = new LitePostgres(DB_FILE, {
      database: "testdb",
      adapter: new NodeFSAdapter(),
    });
  });

  afterAll(() => {
    if (existsSync(DB_FILE)) unlinkSync(DB_FILE);
    if (existsSync(DB_FILE + ".wal")) unlinkSync(DB_FILE + ".wal");
  });

  test("75.1 CREATE TABLE with multiple UNIQUE constraint and references", async () => {
    await db.exec(`CREATE TABLE brands (id SERIAL PRIMARY KEY)`);
    await db.exec(`CREATE TABLE branches (id SERIAL PRIMARY KEY)`);
    await db.exec(`CREATE TABLE users (id SERIAL PRIMARY KEY)`);

    const sql = `
      CREATE TABLE IF NOT EXISTS user_brand_access (
          id SERIAL PRIMARY KEY,
          user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
          brand_id INTEGER NOT NULL REFERENCES brands(id) ON DELETE CASCADE,
          branch_id INTEGER REFERENCES branches(id) ON DELETE SET NULL,
          role VARCHAR(50) NOT NULL,
          created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
          updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
          deleted_at TIMESTAMP,
          UNIQUE(user_id, brand_id, branch_id)
      );
    `;
    const res = await db.exec(sql);
    expect(res.success).toBe(true);

    // Verify index is created
    const rows = await db.query(`
      SELECT indisunique 
      FROM pg_index 
      WHERE indrelid = (SELECT oid FROM pg_class WHERE relname = 'user_brand_access')
      ORDER BY indisprimary ASC
    `);
    // Should have 1 for primary key, 1 for unique
    expect(rows.length).toBe(2);
    expect(rows[0].indisunique).toBe(true);
  });

  test("75.2 ALTER TABLE multiple ADD COLUMN actions", async () => {
    await db.exec(`CREATE TABLE zalo_oa_configs (id SERIAL PRIMARY KEY)`);
    const sql = `
      ALTER TABLE zalo_oa_configs 
      ADD COLUMN IF NOT EXISTS access_token TEXT,
      ADD COLUMN IF NOT EXISTS refresh_token TEXT,
      ADD COLUMN IF NOT EXISTS expires_at TIMESTAMP,
      ADD COLUMN IF NOT EXISTS oa_name TEXT,
      ADD COLUMN IF NOT EXISTS avatar_url TEXT,
      ADD COLUMN IF NOT EXISTS follower_count INTEGER DEFAULT 0,
      ADD COLUMN IF NOT EXISTS last_sync_at TIMESTAMP;
    `;
    const res = await db.exec(sql);
    expect(res.success).toBe(true);

    const cols = await db.query(`
      SELECT column_name 
      FROM information_schema.columns 
      WHERE table_name = 'zalo_oa_configs'
    `);
    const colNames = cols.map((c: any) => c.column_name);
    expect(colNames).toContain('access_token');
    expect(colNames).toContain('follower_count');
    expect(colNames).toContain('last_sync_at');
    expect(colNames.length).toBe(8); // id + 7 new cols
  });

  test("75.3 CREATE TABLE with multiple PRIMARY KEY columns", async () => {
    const sql = `
      CREATE TABLE IF NOT EXISTS oa_statistics (
          oa_id TEXT NOT NULL,
          stat_date DATE NOT NULL,
          follower_count INTEGER DEFAULT 0,
          PRIMARY KEY(oa_id, stat_date)
      );
    `;
    const res = await db.exec(sql);
    expect(res.success).toBe(true);

    const pkRows = await db.query(`
      SELECT indisprimary, indkey 
      FROM pg_index 
      WHERE indrelid = (SELECT oid FROM pg_class WHERE relname = 'oa_statistics')
    `);
    expect(pkRows.length).toBe(1);
    expect(pkRows[0].indisprimary).toBe(true);
    expect(pkRows[0].indkey).toEqual([1, 2]); // oa_id is 1, stat_date is 2
  });
});