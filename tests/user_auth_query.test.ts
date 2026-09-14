import { describe, it, expect, beforeEach, afterEach } from "bun:test";
import { PGLiteNative } from "../src/native";

describe("User Authentication & Compound Boolean Queries (Plan Caching & Execution)", () => {
  let db: PGLiteNative;

  beforeEach(async () => {
    db = new PGLiteNative(":memory:");
    await db.exec(`
      CREATE TABLE "users" (
        id SERIAL PRIMARY KEY,
        username TEXT NOT NULL,
        email TEXT NOT NULL,
        deleted_at TEXT
      );
    `);

    await db.exec(`
      INSERT INTO "users" (username, email, deleted_at) VALUES 
      ('son_dev', 'son@nata.vn', NULL),
      ('admin', 'admin@nata.vn', '2026-09-01 10:00:00'),
      ('test_user', 'test@nata.vn', NULL),
      ('john_doe', 'john@example.com', NULL);
    `);
  });

  afterEach(async () => {
    await db.close();
  });

  const authQuery = `select * from "users" as "u" where ("u"."username" = $1 or "u"."email" = $2) and "u"."deleted_at" is null`;

  it("1. should find user by username on first execution", async () => {
    const res = await db.query(authQuery, ["son_dev", "wrong@mail.com"]);
    expect(res.length).toBe(1);
    expect(res[0].id).toBe(1);
    expect(res[0].username).toBe("son_dev");
    expect(res[0].email).toBe("son@nata.vn");
    expect(res[0].deleted_at).toBeNull();
  });

  it("2. should find user by email on subsequent executions (hitting precompiled plan cache)", async () => {
    // Warm up plan cache
    await db.query(authQuery, ["son_dev", "dummy@mail.com"]);

    // Query matching 2nd parameter ($2 = email)
    const res = await db.query(authQuery, ["non_existent_username", "test@nata.vn"]);
    expect(res.length).toBe(1);
    expect(res[0].id).toBe(3);
    expect(res[0].username).toBe("test_user");
    expect(res[0].email).toBe("test@nata.vn");
  });

  it("3. should not return soft-deleted user even if username and email match", async () => {
    // Warm up cache
    await db.query(authQuery, ["son_dev", "dummy@mail.com"]);

    // Query matching soft-deleted user 'admin'
    const res1 = await db.query(authQuery, ["admin", "non_matching@mail.com"]);
    expect(res1.length).toBe(0);

    const res2 = await db.query(authQuery, ["non_matching_user", "admin@nata.vn"]);
    expect(res2.length).toBe(0);

    const res3 = await db.query(authQuery, ["admin", "admin@nata.vn"]);
    expect(res3.length).toBe(0);
  });

  it("4. should return empty array if neither username nor email matches", async () => {
    // Warm up cache
    await db.query(authQuery, ["son_dev", "dummy@mail.com"]);

    const res = await db.query(authQuery, ["ghost_user", "ghost@mail.com"]);
    expect(res.length).toBe(0);
  });

  it("5. should support unquoted identifiers and aliases identically", async () => {
    const unquotedQuery = `SELECT * FROM users u WHERE (u.username = $1 OR u.email = $2) AND u.deleted_at IS NULL`;
    
    // First execution
    const r1 = await db.query(unquotedQuery, ["john_doe", "nomatch"]);
    expect(r1.length).toBe(1);
    expect(r1[0].username).toBe("john_doe");

    // Cached execution
    const r2 = await db.query(unquotedQuery, ["nomatch", "john@example.com"]);
    expect(r2.length).toBe(1);
    expect(r2[0].username).toBe("john_doe");
  });

  it("6. should handle complex nested OR / AND conditions", async () => {
    const complexQuery = `SELECT id, username FROM "users" WHERE (("username" = $1 OR "username" = $2) OR "email" = $3) AND "deleted_at" IS NULL`;
    
    const r1 = await db.query(complexQuery, ["son_dev", "test_user", "john@example.com"]);
    expect(r1.length).toBe(3);

    const r2 = await db.query(complexQuery, ["no1", "no2", "john@example.com"]);
    expect(r2.length).toBe(1);
    expect(r2[0].username).toBe("john_doe");
  });
});
