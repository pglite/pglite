import { expect, test } from "bun:test";
import { PGLite } from "../src/index";

// Engine có một trạng thái giao dịch cho mỗi instance: các transaction() song song và lệnh xen giữa
// phải được xếp hàng, không được lọt vào giao dịch của nhau.
test("concurrent transaction() calls and outside queries do not interleave", async () => {
  const db = new PGLite(":memory:");
  await db.exec("CREATE TABLE t (id INT PRIMARY KEY, v TEXT)");
  const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

  const failing = db.transaction(async (tx: any) => {
    await tx.exec("INSERT INTO t VALUES (1, 'tx1')");
    await sleep(20);
    throw new Error("boom");
  }).catch((e: Error) => e.message);

  const outside = db.exec("INSERT INTO t VALUES (2, 'outside')");

  const ok = db.transaction(async (tx: any) => {
    await tx.exec("INSERT INTO t VALUES (3, 'tx2')");
    // Gọi thẳng db bên trong callback vẫn thuộc giao dịch này (không bị treo)
    await db.exec("INSERT INTO t VALUES (4, 'tx2-db')");
    return "done";
  });

  expect(await failing).toBe("boom");
  await outside;
  expect(await ok).toBe("done");
  const rows = await db.query("SELECT id FROM t ORDER BY id");
  expect(rows.map((r: any) => Number(r.id))).toEqual([2, 3, 4]);
  await expect(db.transaction(async () => db.transaction(async () => 1))).rejects.toThrow("Nested transactions not supported");
});

test("named params are not substituted inside string literals or dollar quotes", async () => {
  const db = new PGLite(":memory:");
  await db.exec("CREATE TABLE j (id INT, doc JSONB, note TEXT)");
  await db.exec(`INSERT INTO j (id, doc, note) VALUES ($1, '{"a":true,"b":{"c":1}}', 'key:value')`, [1]);
  const rows = await db.query("SELECT doc, note FROM j WHERE id = :id", { id: 1 } as any);
  expect(rows[0].doc).toEqual({ a: true, b: { c: 1 } });
  expect(rows[0].note).toBe("key:value");
  await db.exec("DO $$ BEGIN INSERT INTO j (id, note) VALUES (2, 'x;y'); END $$; INSERT INTO j (id) VALUES (3)");
  expect((await db.query("SELECT count(*) AS n FROM j"))[0].n).toBe(3);
});

test("errors carry PostgreSQL SQLSTATE codes", async () => {
  const db = new PGLite(":memory:");
  await db.exec("CREATE TABLE p (id INT PRIMARY KEY, email TEXT UNIQUE NOT NULL)");
  await db.exec("CREATE TABLE c (id INT PRIMARY KEY, p_id INT REFERENCES p(id), n INT CHECK (n > 0))");
  await db.exec("INSERT INTO p VALUES (1, 'a')");
  const code = async (sql: string) => { try { await db.exec(sql); return "ok"; } catch (e: any) { return e.code; } };
  expect(await code("INSERT INTO p VALUES (2, 'a')")).toBe("23505");
  expect(await code("INSERT INTO p VALUES (3, NULL)")).toBe("23502");
  expect(await code("INSERT INTO c VALUES (1, 99, 1)")).toBe("23503");
  expect(await code("INSERT INTO c VALUES (1, 1, -1)")).toBe("23514");
  expect(await code("SELECT * FROM nope")).toBe("42P01");
  expect(await code("CREATE TABLE p (id INT)")).toBe("42P07");
});
