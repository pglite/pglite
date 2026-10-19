import { expect, test, describe, beforeEach } from "bun:test";
import { PGLiteNative } from "../src/native";

// Chuỗi nhiều câu lệnh, cú pháp literal của PostgreSQL và lỗi trong giao dịch (kết quả đối chiếu PostgreSQL 17).
describe("Multi-statement strings, PostgreSQL literals and aborted transactions", () => {
  let db: PGLiteNative;
  beforeEach(async () => {
    db = new PGLiteNative(":memory:");
    await db.exec("CREATE TABLE m (id INT PRIMARY KEY, s TEXT)");
  });

  test("semicolons inside E'' and $$ strings do not split statements", async () => {
    await db.exec(`INSERT INTO m VALUES (1, E'a\\'b;c'); INSERT INTO m VALUES (2, $$x;y$$)`);
    const rows = await db.query<any>("SELECT s FROM m ORDER BY id");
    expect(rows.map((r) => r.s)).toEqual(["a'b;c", "x;y"]);
  });

  test("a failing statement rolls back the whole string (implicit transaction)", async () => {
    await expect(db.exec("INSERT INTO m VALUES (1, 'a'); INSERT INTO m VALUES (1, 'dup')")).rejects.toMatchObject({ code: "23505" });
    const rows = await db.query<any>("SELECT count(*)::int AS c FROM m");
    expect(rows[0].c).toBe(0);
  });

  test("explicit BEGIN/COMMIT inside the string is respected", async () => {
    await db.exec("BEGIN; INSERT INTO m VALUES (1, 'a'); COMMIT; INSERT INTO m VALUES (2, 'b')");
    const rows = await db.query<any>("SELECT count(*)::int AS c FROM m");
    expect(rows[0].c).toBe(2);
  });

  test("errors abort the transaction until ROLLBACK (25P02)", async () => {
    await db.exec("BEGIN");
    await db.exec("INSERT INTO m VALUES (1, 'a')");
    await expect(db.query("SELECT 1/0")).rejects.toMatchObject({ code: "22012" });
    await expect(db.query("SELECT 1")).rejects.toMatchObject({ code: "25P02" });
    await db.exec("ROLLBACK");
    const rows = await db.query<any>("SELECT count(*)::int AS c FROM m");
    expect(rows[0].c).toBe(0);
  });

  test("savepoint recovers an aborted transaction", async () => {
    await db.exec("BEGIN");
    await db.exec("INSERT INTO m VALUES (1, 'a')");
    await db.exec("SAVEPOINT s");
    await expect(db.exec("INSERT INTO m VALUES (1, 'dup')")).rejects.toMatchObject({ code: "23505" });
    await db.exec("ROLLBACK TO SAVEPOINT s");
    await db.exec("INSERT INTO m VALUES (2, 'b')");
    await db.exec("COMMIT");
    const rows = await db.query<any>("SELECT id FROM m ORDER BY id");
    expect(rows.map((r) => r.id)).toEqual([1, 2]);
  });

  test("PostgreSQL literal syntax", async () => {
    const r = await db.query<any>(
      `SELECT E'tab\\there' AS e, $q$it's$q$ AS d, U&'d\\0061t\\+000061' AS u, 0x1F AS h, 1_000 AS n, 1 /* a /* b */ c */ + 1 AS two`
    );
    expect(r[0]).toEqual({ e: "tab\there", d: "it's", u: "data", h: 31, n: 1000, two: 2 });
    await expect(db.query("SELECT 'a' 'b'")).rejects.toMatchObject({ code: "42601" });
  });

  test("quoted identifiers with spaces and punctuation", async () => {
    await db.exec(`CREATE TABLE "Order Items" (id INT PRIMARY KEY, "Unit Price" NUMERIC, "x-y" INT)`);
    await db.query(`INSERT INTO "Order Items" VALUES ($1, $2, $3)`, [1, 9.5, 2]);
    const r = await db.query<any>(`SELECT "Unit Price" * 2 AS "Total Amount", "x-y" FROM "Order Items" WHERE "x-y" = 2`);
    expect(r).toEqual([{ "Total Amount": 19, "x-y": 2 }]);
  });

  test("runtime errors carry PostgreSQL SQLSTATE codes", async () => {
    await expect(db.query("SELECT nope FROM m")).rejects.toMatchObject({ code: "42703" });
    await expect(db.query("SELECT 'abc'::int")).rejects.toMatchObject({ code: "22P02" });
    await expect(db.query("SELECT 99999999999::int")).rejects.toMatchObject({ code: "22003" });
  });
});
