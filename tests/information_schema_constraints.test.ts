import { expect, test, describe, beforeAll, afterAll } from "bun:test";
import { LitePostgres } from "../src/database";
import { PGLite } from "../src/index";
import { unlinkSync, existsSync } from "fs";
import { NodeFSAdapter } from "../src/adapters/node";

const DB_FILE = "test_info_schema.db";

describe("LEVEL 94: information_schema Constraint Views & Compatibility Suite", () => {
  let db: LitePostgres;

  beforeAll(async () => {
    if (existsSync(DB_FILE)) unlinkSync(DB_FILE);
    if (existsSync(DB_FILE + ".wal")) unlinkSync(DB_FILE + ".wal");
    db = new LitePostgres(DB_FILE, {
      database: "testdb",
      adapter: new NodeFSAdapter(),
    });

    await db.exec(`
      CREATE TABLE departments (
        dept_id SERIAL PRIMARY KEY,
        dept_name TEXT NOT NULL UNIQUE
      );

      CREATE TABLE employees (
        emp_id SERIAL PRIMARY KEY,
        dept_id INT REFERENCES departments(dept_id),
        full_name TEXT NOT NULL,
        email TEXT UNIQUE,
        salary NUMERIC DEFAULT 1000
      );

      CREATE TABLE project_assignments (
        project_id INT NOT NULL,
        emp_id INT NOT NULL REFERENCES employees(emp_id),
        role TEXT NOT NULL,
        PRIMARY KEY (project_id, emp_id)
      );
    `);
  });

  afterAll(() => {
    if (existsSync(DB_FILE)) unlinkSync(DB_FILE);
    if (existsSync(DB_FILE + ".wal")) unlinkSync(DB_FILE + ".wal");
  });

  test("94.1 User Query 1: Foreign Key Introspection via 3-way join", async () => {
    const q1 = `
      SELECT
        kcu.table_name,
        kcu.column_name,
        ccu.table_name AS foreign_table_name,
        ccu.column_name AS foreign_column_name
      FROM information_schema.table_constraints AS tc
      JOIN information_schema.key_column_usage AS kcu
        ON tc.constraint_name = kcu.constraint_name
        AND tc.table_schema = kcu.table_schema
      JOIN information_schema.constraint_column_usage AS ccu
        ON ccu.constraint_name = tc.constraint_name
        AND ccu.table_schema = tc.table_schema
      WHERE tc.constraint_type = 'FOREIGN KEY'
        AND tc.table_schema = 'public'
      ORDER BY kcu.table_name, kcu.column_name;
    `;

    const rows = await db.query<any>(q1);
    expect(rows.length).toBe(2);

    const empFk = rows.find((r) => r.table_name === "employees");
    expect(empFk).toBeDefined();
    expect(empFk.column_name).toBe("dept_id");
    expect(empFk.foreign_table_name).toBe("departments");
    expect(empFk.foreign_column_name).toBe("dept_id");

    const projFk = rows.find((r) => r.table_name === "project_assignments");
    expect(projFk).toBeDefined();
    expect(projFk.column_name).toBe("emp_id");
    expect(projFk.foreign_table_name).toBe("employees");
    expect(projFk.foreign_column_name).toBe("emp_id");
  });

  test("94.2 User Query 2: Primary Key Introspection via 2-way join (including Composite PK)", async () => {
    const q2 = `
      SELECT
        tc.table_name, 
        kcu.column_name
      FROM information_schema.table_constraints tc
      JOIN information_schema.key_column_usage kcu
        ON tc.constraint_name = kcu.constraint_name
        AND tc.table_schema = kcu.table_schema
      WHERE tc.constraint_type = 'PRIMARY KEY'
        AND tc.table_schema = 'public'
      ORDER BY tc.table_name, kcu.column_name;
    `;

    const rows = await db.query<any>(q2);
    expect(rows.length).toBe(4);

    const deptsPk = rows.filter((r) => r.table_name === "departments");
    expect(deptsPk.map((r) => r.column_name)).toEqual(["dept_id"]);

    const empsPk = rows.filter((r) => r.table_name === "employees");
    expect(empsPk.map((r) => r.column_name)).toEqual(["emp_id"]);

    const projPk = rows.filter((r) => r.table_name === "project_assignments");
    expect(projPk.map((r) => r.column_name).sort()).toEqual(["emp_id", "project_id"]);
  });

  test("94.3 User Query 3: Column Introspection via information_schema.columns", async () => {
    const q3 = `
      SELECT 
        table_name,
        column_name,
        data_type,
        udt_name,
        is_nullable,
        column_default
      FROM information_schema.columns 
      WHERE table_schema = 'public' 
      ORDER BY table_name, ordinal_position;
    `;

    const rows = await db.query<any>(q3);
    expect(rows.length).toBeGreaterThanOrEqual(10);

    const empCols = rows.filter((r) => r.table_name === "employees");
    expect(empCols.map((c) => c.column_name)).toEqual([
      "emp_id",
      "dept_id",
      "full_name",
      "email",
      "salary",
    ]);

    const emailCol = empCols.find((c) => c.column_name === "email");
    expect(emailCol.is_nullable).toBe("YES");

    const nameCol = empCols.find((c) => c.column_name === "full_name");
    expect(nameCol.is_nullable).toBe("NO");
  });

  test("94.4 User Query 4: Table Introspection via information_schema.tables", async () => {
    const q4 = `
      SELECT table_name 
      FROM information_schema.tables 
      WHERE table_schema = 'public' 
        AND table_type = 'BASE TABLE'
      ORDER BY table_name;
    `;

    const rows = await db.query<any>(q4);
    const tableNames = rows.map((r) => r.table_name);
    expect(tableNames).toEqual(["departments", "employees", "project_assignments"]);
  });

  test("94.5 Direct inspection of information_schema.table_constraints with UNIQUE and PK", async () => {
    const rows = await db.query<any>(`
      SELECT constraint_name, table_name, constraint_type
      FROM information_schema.table_constraints
      WHERE table_schema = 'public'
      ORDER BY table_name, constraint_type, constraint_name;
    `);

    expect(rows.some((r) => r.table_name === "departments" && r.constraint_type === "PRIMARY KEY")).toBe(true);
    expect(rows.some((r) => r.table_name === "departments" && r.constraint_type === "UNIQUE")).toBe(true);
    expect(rows.some((r) => r.table_name === "employees" && r.constraint_type === "FOREIGN KEY")).toBe(true);
    expect(rows.some((r) => r.table_name === "employees" && r.constraint_type === "UNIQUE")).toBe(true);
    expect(rows.some((r) => r.table_name === "project_assignments" && r.constraint_type === "PRIMARY KEY")).toBe(true);
  });

  test("94.6 Compatibility with PGLite / PGLiteNative wrapper & in-memory mode", async () => {
    const memDb = new PGLite(":memory:");
    await memDb.exec(`
      CREATE TABLE orders (
        order_id SERIAL PRIMARY KEY,
        customer_email TEXT NOT NULL
      );

      CREATE TABLE order_items (
        item_id SERIAL PRIMARY KEY,
        order_id INT REFERENCES orders(order_id),
        item_name TEXT NOT NULL
      );
    `);

    const fkRes = await memDb.query<any>(`
      SELECT
        kcu.table_name,
        kcu.column_name,
        ccu.table_name AS foreign_table_name,
        ccu.column_name AS foreign_column_name
      FROM information_schema.table_constraints AS tc
      JOIN information_schema.key_column_usage AS kcu
        ON tc.constraint_name = kcu.constraint_name
        AND tc.table_schema = kcu.table_schema
      JOIN information_schema.constraint_column_usage AS ccu
        ON ccu.constraint_name = tc.constraint_name
        AND ccu.table_schema = tc.table_schema
      WHERE tc.constraint_type = 'FOREIGN KEY'
        AND tc.table_schema = 'public';
    `);

    expect(fkRes.length).toBe(1);
    expect(fkRes[0].table_name).toBe("order_items");
    expect(fkRes[0].column_name).toBe("order_id");
    expect(fkRes[0].foreign_table_name).toBe("orders");
    expect(fkRes[0].foreign_column_name).toBe("order_id");

    const pkRes = await memDb.query<any>(`
      SELECT tc.table_name, kcu.column_name
      FROM information_schema.table_constraints tc
      JOIN information_schema.key_column_usage kcu
        ON tc.constraint_name = kcu.constraint_name
        AND tc.table_schema = kcu.table_schema
      WHERE tc.constraint_type = 'PRIMARY KEY'
        AND tc.table_schema = 'public'
      ORDER BY tc.table_name;
    `);

    expect(pkRes.length).toBe(2);
  });

  test("94.7 Existing on-disk database files are 100% compatible and unchanged", async () => {
    const testDiskFile = "test_persistence_compat.db";
    if (existsSync(testDiskFile)) unlinkSync(testDiskFile);
    if (existsSync(testDiskFile + ".wal")) unlinkSync(testDiskFile + ".wal");

    // 1. Create and write data
    const db1 = new LitePostgres(testDiskFile, { adapter: new NodeFSAdapter() });
    await db1.exec(`
      CREATE TABLE products (
        id SERIAL PRIMARY KEY,
        sku TEXT NOT NULL UNIQUE,
        price NUMERIC NOT NULL
      );
      INSERT INTO products (sku, price) VALUES ('SKU-100', 49.99);
    `);

    // 2. Re-open existing database file
    const db2 = new LitePostgres(testDiskFile, { adapter: new NodeFSAdapter() });
    const products = await db2.query<any>("SELECT * FROM products");
    expect(products.length).toBe(1);
    expect(products[0].sku).toBe("SKU-100");

    // 3. Introspect schema on the existing file
    const pks = await db2.query<any>(`
      SELECT tc.table_name, kcu.column_name
      FROM information_schema.table_constraints tc
      JOIN information_schema.key_column_usage kcu
        ON tc.constraint_name = kcu.constraint_name
        AND tc.table_schema = kcu.table_schema
      WHERE tc.constraint_type = 'PRIMARY KEY' AND tc.table_schema = 'public';
    `);
    expect(pks).toEqual([{ table_name: "products", column_name: "id" }]);

    // Cleanup
    if (existsSync(testDiskFile)) unlinkSync(testDiskFile);
    if (existsSync(testDiskFile + ".wal")) unlinkSync(testDiskFile + ".wal");
  });
});
