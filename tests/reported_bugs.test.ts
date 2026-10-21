// Repro các lỗi đã báo trong bugs/PGLITE_RS_BUGS.md và bugs/DB_RS_BUGS_FOUND.md, chạy qua native addon với
// tham số JS thật (số, mảng) như db-rs / WAIKI gửi. Mỗi câu chạy nhiều lần: lỗi 6 chỉ hiện từ lần thứ hai.
import { expect, test, describe, beforeEach, afterEach } from "bun:test";
import { PGLite } from "../src/index";

let db: PGLite;
const q = (sql: string, params?: any[]) => db.query(sql, params) as Promise<any[]>;
const times = async (n: number, sql: string, params?: any[]) => {
  const first = await q(sql, params);
  for (let i = 1; i < n; i++) expect(await q(sql, params)).toEqual(first);
  return first;
};

beforeEach(() => {
  db = new PGLite(":memory:");
});
afterEach(async () => {
  await db?.close();
});

describe("PGLITE_RS_BUGS.md", () => {
  test("Lỗi 1: bảng dẫn xuất (VALUES ...)", async () => {
    expect(await times(3, "SELECT v.* FROM (VALUES (1,'a'),(2,'b')) AS v(id,name)")).toEqual([{ id: 1, name: "a" }, { id: 2, name: "b" }]);
    expect(await times(3, "SELECT p.m FROM (SELECT 1 AS x) s CROSS JOIN (VALUES (30),(60)) AS p(m)")).toEqual([{ m: 30 }, { m: 60 }]);
  });

  test("Lỗi 2: subquery trong INSERT ... VALUES không còn ghi false", async () => {
    await db.exec("CREATE TABLE cat (id SERIAL PRIMARY KEY, code TEXT)");
    await db.exec("CREATE TABLE item (id SERIAL PRIMARY KEY, cat_id INT REFERENCES cat(id), title TEXT)");
    await db.exec("CREATE TABLE item2 (id SERIAL PRIMARY KEY, cat_id INT, title TEXT)");
    await db.exec("INSERT INTO cat (code) VALUES ('a'), ('b')");
    for (let i = 0; i < 3; i++) {
      expect(await q("INSERT INTO item (cat_id, title) VALUES ((SELECT id FROM cat WHERE code = $1), $2) RETURNING cat_id", ["b", "x"])).toEqual([{ cat_id: 2 }]);
      expect(await q("INSERT INTO item2 (cat_id, title) VALUES ((SELECT id FROM cat WHERE code = 'b'), 'y') RETURNING cat_id")).toEqual([{ cat_id: 2 }]);
    }
  });

  test("Lỗi 4: GREATEST/LEAST và phép toán trong JOIN", async () => {
    await db.exec("CREATE TABLE rw (id INT)");
    await db.exec("INSERT INTO rw VALUES (1)");
    await db.exec("CREATE TABLE rd (id INT, reward_id INT, expires_at TIMESTAMPTZ)");
    await db.exec("INSERT INTO rd VALUES (1, 1, now() + interval '3 days' + interval '1 hour')");
    const join = "FROM rd d JOIN rw w ON w.id = d.reward_id";
    expect(await times(3, `SELECT GREATEST(d.id, 5) AS g, LEAST(5, d.id) AS l ${join}`)).toEqual([{ g: 5, l: 1 }]);
    expect(await times(3, `SELECT GREATEST(0, CEIL(EXTRACT(EPOCH FROM (d.expires_at - CURRENT_TIMESTAMP)) / 86400))::int AS days ${join}`)).toEqual([{ days: 4 }]);
    await db.exec("CREATE TABLE sx (id INT, fee INT, dur INT)");
    await db.exec("INSERT INTO sx VALUES (1, 500000, 10)");
    expect(
      await times(3, "SELECT p.m, ROUND(COALESCE(s.fee, 0) * 60.0 / GREATEST(s.dur, 15) * p.factor / 10000.0) * 10000 AS price FROM sx s CROSS JOIN (VALUES (30, 0.54), (60, 1.0)) AS p(m, factor) ORDER BY p.m")
    ).toEqual([{ m: 30, price: 1080000 }, { m: 60, price: 2000000 }]);
  });

  test("Lỗi 5: self-join không alias bảng bên trái", async () => {
    await db.exec("CREATE TABLE sp (id INT, v TEXT)");
    await db.exec("INSERT INTO sp VALUES (1, 'a'), (2, 'b')");
    expect(await times(3, "SELECT sp.id FROM sp JOIN sp sp2 ON sp2.id = sp.id ORDER BY 1")).toEqual([{ id: 1 }, { id: 2 }]);
    expect(await times(3, "SELECT sp.id, sp2.v FROM sp JOIN sp AS sp2 ON sp2.id = sp.id + 1 ORDER BY 1")).toEqual([{ id: 1, v: "b" }]);
  });
});

describe("DB_RS_BUGS_FOUND.md", () => {
  test("Lỗi 2c: GREATEST(0, CEIL(EXTRACT(...))) trong JOIN, không ép kiểu", async () => {
    await db.exec("CREATE TABLE rw (id INT)");
    await db.exec("INSERT INTO rw VALUES (1)");
    await db.exec("CREATE TABLE rd (id INT, reward_id INT, expires_at TIMESTAMPTZ)");
    await db.exec("INSERT INTO rd VALUES (1, 1, now() + interval '3 days' + interval '1 hour')");
    expect(
      await times(3, "SELECT d.id, GREATEST(0, CEIL(EXTRACT(EPOCH FROM (d.expires_at - CURRENT_TIMESTAMP)) / 86400)) AS days_left FROM rd d JOIN rw w ON w.id = d.reward_id WHERE d.id = $1", [1])
    ).toEqual([{ id: 1, days_left: 4 }]);
  });

  test("Lỗi 5: = ANY($1) với mảng JS trên cột BIGINT", async () => {
    await db.exec("CREATE TABLE big (id BIGINT PRIMARY KEY, v TEXT)");
    await db.exec("INSERT INTO big VALUES (1, 'a'), (2, 'b'), (3, 'c')");
    expect(await times(3, "SELECT id FROM big WHERE id = ANY($1) ORDER BY id", [[1, 3]])).toEqual([{ id: 1 }, { id: 3 }]);
  });

  test("Lỗi 6: $n trong danh sách SELECT có FROM, chạy lặp lại", async () => {
    await db.exec("CREATE TABLE cat (id SERIAL PRIMARY KEY, code TEXT)");
    await db.exec("INSERT INTO cat (code) VALUES ('a'), ('b')");
    await db.exec("CREATE TABLE pi4 (id SERIAL PRIMARY KEY, program_id INT NOT NULL, item_id INT NOT NULL)");
    for (const [p, code, id] of [[8, "a", 1], [9, "a", 1], [10, "b", 2]] as const) {
      expect(await q("SELECT $1 AS p, id FROM cat WHERE code = $2", [p, code])).toEqual([{ p, id }]);
    }
    // seed wellness-library: gắn nhiều bài vào chương trình bằng cùng một câu
    for (let p = 1; p <= 5; p++) {
      expect(await q("INSERT INTO pi4 (program_id, item_id) SELECT $1, id FROM cat WHERE code = $2 RETURNING program_id", [p, "a"])).toEqual([{ program_id: p }]);
    }
    expect(await q("SELECT count(*) AS n FROM pi4")).toEqual([{ n: 5 }]);
  });
});

describe("DB_RS_BUGS_FOUND.md, kiểm tra lần 4", () => {
  const setup = async () => {
    await db.exec("CREATE TABLE tenants (id SERIAL PRIMARY KEY, name TEXT)");
    await db.exec("INSERT INTO tenants (name) VALUES ('t1')");
    await db.exec("CREATE TABLE customers (id SERIAL PRIMARY KEY, tenant_id INT, code TEXT)");
    for (let i = 1; i <= 10; i++) await db.exec(`INSERT INTO customers (tenant_id, code) VALUES (1, 'CUST-${String(i).padStart(4, "0")}')`);
    await db.exec("CREATE TABLE customer_addresses (id SERIAL PRIMARY KEY, customer_id INT, addr TEXT)");
    await db.exec("INSERT INTO customer_addresses (customer_id, addr) VALUES (1, 'a'), (2, 'b'), (3, 'c')");
    await db.exec("CREATE TABLE pk (id SERIAL PRIMARY KEY, slug TEXT)");
    for (let i = 1; i <= 6; i++) await db.exec(`INSERT INTO pk (slug) VALUES ('pkg-${i}')`);
    await db.exec("INSERT INTO pk (slug) VALUES ('spine-recovery-7n6d')");
  };

  test("Lỗi 2c: CASE WHEN ... THEN GREATEST(...)::int END (days_left của wellness-loyalty)", async () => {
    await db.exec("CREATE TABLE rw (id INT, title TEXT)");
    await db.exec("INSERT INTO rw VALUES (1, 'r1')");
    await db.exec("CREATE TABLE rd (id INT, reward_id INT, status TEXT, expires_at TIMESTAMPTZ)");
    await db.exec("INSERT INTO rd VALUES (1, 1, 'available', now() + interval '3 days' + interval '1 hour'), (2, 1, 'used', NULL)");
    const sql = "SELECT d.id, CASE WHEN d.status = 'available' AND d.expires_at IS NOT NULL THEN GREATEST(0, CEIL(EXTRACT(EPOCH FROM (d.expires_at - CURRENT_TIMESTAMP)) / 86400))::int END AS days_left FROM rd d JOIN rw w ON w.id = d.reward_id ORDER BY d.id";
    expect(await times(3, sql)).toEqual([{ id: 1, days_left: 4 }, { id: 2, days_left: null }]);
    expect(await times(3, "SELECT CAST(GREATEST(0, CEIL(EXTRACT(EPOCH FROM (d.expires_at - CURRENT_TIMESTAMP)) / 86400)) AS int) AS c FROM rd d JOIN rw w ON w.id = d.reward_id WHERE d.id = $1", [1])).toEqual([{ c: 4 }]);
  });

  test("Lỗi 7: ép kiểu hàm gộp trong JOIN", async () => {
    await setup();
    expect(await times(3, "SELECT count(*)::int AS n, sum(a.id)::int AS s FROM customers c JOIN customer_addresses a ON a.customer_id = c.id")).toEqual([{ n: 3, s: 6 }]);
    expect(await times(3, "SELECT count(*)::int AS n FROM customers c CROSS JOIN pk p")).toEqual([{ n: 70 }]);
  });

  test("Lỗi 8: LIMIT trong query nhiều bảng + WHERE", async () => {
    await setup();
    for (const from of ["FROM customers c, pk p", "FROM customers c CROSS JOIN pk p"]) {
      expect(await times(3, `SELECT c.id, p.id AS pid ${from} WHERE c.code = 'CUST-0010' AND p.slug = $1 LIMIT 1`, ["spine-recovery-7n6d"])).toEqual([{ id: 10, pid: 7 }]);
    }
  });

  test("Lỗi 9: EXISTS / NOT EXISTS tương quan trong JOIN", async () => {
    await setup();
    const sub = "(SELECT 1 FROM customer_addresses a WHERE a.customer_id = c.id)";
    expect((await times(3, `SELECT c.id FROM customers c JOIN tenants t ON t.id = c.tenant_id WHERE NOT EXISTS ${sub} ORDER BY 1`)).map((r) => r.id)).toEqual([4, 5, 6, 7, 8, 9, 10]);
    expect((await times(3, `SELECT c.id FROM tenants t JOIN customers c ON c.tenant_id = t.id WHERE EXISTS ${sub} ORDER BY 1`)).map((r) => r.id)).toEqual([1, 2, 3]);
    const seed = `INSERT INTO customer_addresses (customer_id, addr) SELECT c.id, v.addr FROM (VALUES ('CUST-0004', 'x'), ('CUST-0005', 'y')) AS v(code, addr) JOIN customers c ON c.code = v.code WHERE NOT EXISTS ${sub} RETURNING customer_id`;
    expect(await q(seed)).toEqual([{ customer_id: 4 }, { customer_id: 5 }]);
    expect(await q(seed)).toEqual([]);
  });
});

describe("DB_RS_BUGS_FOUND.md, kiểm tra lần 5", () => {
  test("Lỗi 11: current_setting / set_config", async () => {
    expect(await times(3, "SELECT current_setting('TimeZone') AS tz, current_setting('nope', true) AS m")).toEqual([{ tz: "UTC", m: null }]);
    await q("SELECT set_config('app.user_id', '42', false)");
    expect(await q("SELECT current_setting('app.user_id') AS u")).toEqual([{ u: "42" }]);
  });

  test("Lỗi 12: ::text của numeric(p,s) / jsonb", async () => {
    expect(await times(3, `SELECT 450000.00::numeric(12,2)::text AS a, '{"vi":"a","en":"b"}'::jsonb::text AS b`)).toEqual([{ a: "450000.00", b: '{"en": "b", "vi": "a"}' }]);
  });

  test("Lỗi 13: JSON mặc định không đoán sai kiểu chữ / jsonb", async () => {
    await db.exec("CREATE TABLE t13 (id SERIAL PRIMARY KEY, tpl TEXT, j JSONB, arr TEXT[], legacy TEXT)");
    await db.exec(`INSERT INTO t13 (tpl, j, arr, legacy) VALUES ('{{app_name}} code {{code}}', '{}', '{a,b}', '{1,2}')`);
    expect(await times(3, "SELECT tpl, j, arr, legacy FROM t13")).toEqual([{ tpl: "{{app_name}} code {{code}}", j: {}, arr: ["a", "b"], legacy: [1, 2] }]);
    expect(await times(3, "SELECT * FROM t13 WHERE id = $1", [1])).toEqual([{ id: 1, tpl: "{{app_name}} code {{code}}", j: {}, arr: ["a", "b"], legacy: [1, 2] }]);
  });
});
