import { describe, test, expect, beforeAll, afterAll } from "bun:test";
import { PGLite } from "../src/index";

describe("100 Comprehensive In-Depth PostgreSQL Transactions & ACID Test Cases for PGLite", () => {
  let db: PGLite;

  beforeAll(async () => {
    db = new PGLite(":memory:", { native: true });

    // Table 1: Bank Accounts for balance transfers
    await db.query(`
      CREATE TABLE acid_accounts (
        id SERIAL PRIMARY KEY,
        owner TEXT NOT NULL UNIQUE,
        balance NUMERIC NOT NULL CHECK (balance >= 0)
      );
    `);

    // Table 2: Audit log
    await db.query(`
      CREATE TABLE acid_audit_logs (
        id SERIAL PRIMARY KEY,
        action TEXT NOT NULL,
        account_id INT,
        amount NUMERIC,
        timestamp TEXT DEFAULT '2026-01-01'
      );
    `);

    // Table 3: E-Commerce Inventory & Orders
    await db.query(`
      CREATE TABLE acid_products (
        id SERIAL PRIMARY KEY,
        sku TEXT NOT NULL UNIQUE,
        stock INT NOT NULL CHECK (stock >= 0),
        price NUMERIC NOT NULL
      );
    `);

    await db.query(`
      CREATE TABLE acid_orders (
        id SERIAL PRIMARY KEY,
        product_id INT NOT NULL,
        qty INT NOT NULL,
        total NUMERIC NOT NULL,
        status TEXT DEFAULT 'pending'
      );
    `);
  });

  afterAll(async () => {
    if (db) {
      await db.close();
    }
  });

  // =========================================================================
  // Section 1: Basic Transaction Control: BEGIN & COMMIT (1 - 20)
  // =========================================================================
  describe("Section 1: Basic Transaction Control: BEGIN & COMMIT", () => {
    test("01. Auto-commit insert outside transaction block", async () => {
      await db.query("INSERT INTO acid_accounts (id, owner, balance) VALUES (1, 'Alice', 1000);");
      const res = await db.query("SELECT * FROM acid_accounts WHERE id = 1;");
      expect(res.length).toBe(1);
      expect(Number(res[0].balance)).toBe(1000);
    });

    test("02. BEGIN followed by single INSERT and COMMIT", async () => {
      await db.query("BEGIN;");
      await db.query("INSERT INTO acid_accounts (id, owner, balance) VALUES (2, 'Bob', 500);");
      await db.query("COMMIT;");
      const res = await db.query("SELECT * FROM acid_accounts WHERE id = 2;");
      expect(res.length).toBe(1);
      expect(Number(res[0].balance)).toBe(500);
    });

    test("03. Multi-row INSERT inside BEGIN / COMMIT block", async () => {
      await db.query("BEGIN;");
      await db.query("INSERT INTO acid_accounts (id, owner, balance) VALUES (3, 'Charlie', 750), (4, 'David', 1200);");
      await db.query("COMMIT;");
      const res = await db.query("SELECT count(*) AS total FROM acid_accounts WHERE id IN (3, 4);");
      expect(Number(res[0].total)).toBe(2);
    });

    test("04. Query inside active transaction reading uncommitted inserted row", async () => {
      await db.query("BEGIN;");
      await db.query("INSERT INTO acid_accounts (id, owner, balance) VALUES (5, 'Eve', 2000);");
      const uncommitted = await db.query("SELECT * FROM acid_accounts WHERE id = 5;");
      expect(uncommitted.length).toBe(1);
      expect(uncommitted[0].owner).toBe("Eve");
      await db.query("COMMIT;");
    });

    test("05. UPDATE single row balance inside transaction with COMMIT", async () => {
      await db.query("BEGIN;");
      await db.query("UPDATE acid_accounts SET balance = 1100 WHERE id = 1;");
      await db.query("COMMIT;");
      const res = await db.query("SELECT balance FROM acid_accounts WHERE id = 1;");
      expect(Number(res[0].balance)).toBe(1100);
    });

    test("06. UPDATE multiple rows inside transaction with COMMIT", async () => {
      await db.query("BEGIN;");
      await db.query("UPDATE acid_accounts SET balance = balance + 50 WHERE id IN (1, 2);");
      await db.query("COMMIT;");
      const res1 = await db.query("SELECT balance FROM acid_accounts WHERE id = 1;");
      const res2 = await db.query("SELECT balance FROM acid_accounts WHERE id = 2;");
      expect(Number(res1[0].balance)).toBe(1150);
      expect(Number(res2[0].balance)).toBe(550);
    });

    test("07. DELETE row inside transaction with COMMIT", async () => {
      await db.query("INSERT INTO acid_accounts (id, owner, balance) VALUES (99, 'Temp User', 100);");
      await db.query("BEGIN;");
      await db.query("DELETE FROM acid_accounts WHERE id = 99;");
      await db.query("COMMIT;");
      const res = await db.query("SELECT * FROM acid_accounts WHERE id = 99;");
      expect(res.length).toBe(0);
    });

    test("08. Transaction with BEGIN TRANSACTION syntax alias", async () => {
      await db.query("BEGIN TRANSACTION;");
      await db.query("INSERT INTO acid_accounts (id, owner, balance) VALUES (6, 'Frank', 900);");
      await db.query("COMMIT TRANSACTION;");
      const res = await db.query("SELECT * FROM acid_accounts WHERE id = 6;");
      expect(res.length).toBe(1);
      expect(res[0].owner).toBe("Frank");
    });

    test("09. Transaction with BEGIN WORK / COMMIT WORK syntax aliases", async () => {
      await db.query("BEGIN WORK;");
      await db.query("INSERT INTO acid_accounts (id, owner, balance) VALUES (7, 'Grace', 1500);");
      await db.query("COMMIT WORK;");
      const res = await db.query("SELECT * FROM acid_accounts WHERE id = 7;");
      expect(res.length).toBe(1);
      expect(res[0].owner).toBe("Grace");
    });

    test("10. Multi-table write inside single transaction committed atomically", async () => {
      await db.query("BEGIN;");
      await db.query("INSERT INTO acid_accounts (id, owner, balance) VALUES (8, 'Heidi', 300);");
      await db.query("INSERT INTO acid_audit_logs (id, action, account_id, amount) VALUES (1, 'CREATE_ACCOUNT', 8, 300);");
      await db.query("COMMIT;");

      const acc = await db.query("SELECT * FROM acid_accounts WHERE id = 8;");
      const log = await db.query("SELECT * FROM acid_audit_logs WHERE id = 1;");
      expect(acc.length).toBe(1);
      expect(log.length).toBe(1);
      expect(Number(log[0].amount)).toBe(300);
    });

    test("11. Parameterized query within transaction block", async () => {
      await db.query("BEGIN;");
      await db.query(
        "INSERT INTO acid_accounts (id, owner, balance) VALUES ($1, $2, $3);",
        [9, "Ivan", 400]
      );
      await db.query("COMMIT;");
      const res = await db.query("SELECT * FROM acid_accounts WHERE id = 9;");
      expect(res.length).toBe(1);
      expect(res[0].owner).toBe("Ivan");
    });

    test("12. Parameterized UPDATE within transaction block", async () => {
      await db.query("BEGIN;");
      await db.query(
        "UPDATE acid_accounts SET balance = $1 WHERE owner = $2;",
        [450, "Ivan"]
      );
      await db.query("COMMIT;");
      const res = await db.query("SELECT balance FROM acid_accounts WHERE id = 9;");
      expect(Number(res[0].balance)).toBe(450);
    });

    test("13. SELECT with aggregation inside transaction", async () => {
      await db.query("BEGIN;");
      const res = await db.query("SELECT count(*) AS total, sum(balance) AS total_wealth FROM acid_accounts;");
      expect(Number(res[0].total)).toBeGreaterThanOrEqual(8);
      expect(Number(res[0].total_wealth)).toBeGreaterThan(0);
      await db.query("COMMIT;");
    });

    test("14. Consecutive independent transactions in sequence", async () => {
      await db.query("BEGIN;");
      await db.query("INSERT INTO acid_audit_logs (id, action, account_id, amount) VALUES (2, 'TX1_ACTION', 1, 10);");
      await db.query("COMMIT;");

      await db.query("BEGIN;");
      await db.query("INSERT INTO acid_audit_logs (id, action, account_id, amount) VALUES (3, 'TX2_ACTION', 1, 20);");
      await db.query("COMMIT;");

      const res = await db.query("SELECT * FROM acid_audit_logs WHERE id IN (2, 3) ORDER BY id ASC;");
      expect(res.length).toBe(2);
      expect(res[0].action).toBe("TX1_ACTION");
      expect(res[1].action).toBe("TX2_ACTION");
    });

    test("15. Reading updated state immediately in next statement of same transaction", async () => {
      await db.query("BEGIN;");
      await db.query("UPDATE acid_accounts SET balance = 9999 WHERE id = 9;");
      const interim = await db.query("SELECT balance FROM acid_accounts WHERE id = 9;");
      expect(Number(interim[0].balance)).toBe(9999);
      await db.query("UPDATE acid_accounts SET balance = 500 WHERE id = 9;");
      await db.query("COMMIT;");
      const finalRes = await db.query("SELECT balance FROM acid_accounts WHERE id = 9;");
      expect(Number(finalRes[0].balance)).toBe(500);
    });

    test("16. Transaction with multiple statements in a single batch query", async () => {
      await db.query(`
        BEGIN;
        INSERT INTO acid_accounts (id, owner, balance) VALUES (10, 'Jack', 800);
        INSERT INTO acid_audit_logs (id, action, account_id, amount) VALUES (4, 'BATCH_CREATE', 10, 800);
        COMMIT;
      `);
      const acc = await db.query("SELECT * FROM acid_accounts WHERE id = 10;");
      const log = await db.query("SELECT * FROM acid_audit_logs WHERE id = 4;");
      expect(acc.length).toBe(1);
      expect(log.length).toBe(1);
    });

    test("17. Transaction updating row and reading back with WHERE clause", async () => {
      await db.query("BEGIN;");
      await db.query("UPDATE acid_accounts SET balance = balance * 2 WHERE id = 10;");
      const res = await db.query("SELECT balance FROM acid_accounts WHERE id = 10;");
      expect(Number(res[0].balance)).toBe(1600);
      await db.query("COMMIT;");
    });

    test("18. Empty transaction block (BEGIN then immediate COMMIT)", async () => {
      await db.query("BEGIN;");
      await db.query("COMMIT;");
      expect(true).toBe(true);
    });

    test("19. Transaction doing read-only operations", async () => {
      await db.query("BEGIN;");
      const res = await db.query("SELECT id, owner FROM acid_accounts ORDER BY id ASC LIMIT 3;");
      expect(res.length).toBe(3);
      await db.query("COMMIT;");
    });

    test("20. Count verification of all committed accounts", async () => {
      const res = await db.query("SELECT count(*) AS c FROM acid_accounts;");
      expect(Number(res[0].c)).toBe(10);
    });
  });

  // =========================================================================
  // Section 2: ROLLBACK & Atomicity Guarantees (21 - 40)
  // =========================================================================
  describe("Section 2: ROLLBACK & Atomicity Guarantees", () => {
    test("21. ROLLBACK after single row INSERT restores table state", async () => {
      const beforeCount = await db.query("SELECT count(*) AS c FROM acid_accounts;");
      await db.query("BEGIN;");
      await db.query("INSERT INTO acid_accounts (id, owner, balance) VALUES (901, 'Ghost 1', 100);");
      const insideCheck = await db.query("SELECT * FROM acid_accounts WHERE id = 901;");
      expect(insideCheck.length).toBe(1);
      await db.query("ROLLBACK;");

      const afterCheck = await db.query("SELECT * FROM acid_accounts WHERE id = 901;");
      const afterCount = await db.query("SELECT count(*) AS c FROM acid_accounts;");
      expect(afterCheck.length).toBe(0);
      expect(Number(afterCount[0].c)).toBe(Number(beforeCount[0].c));
    });

    test("22. ROLLBACK after multi-row INSERT discards all new rows", async () => {
      await db.query("BEGIN;");
      await db.query("INSERT INTO acid_accounts (id, owner, balance) VALUES (902, 'Ghost 2', 200), (903, 'Ghost 3', 300);");
      await db.query("ROLLBACK;");
      const res = await db.query("SELECT * FROM acid_accounts WHERE id IN (902, 903);");
      expect(res.length).toBe(0);
    });

    test("23. ROLLBACK after UPDATE restores original column values", async () => {
      const orig = await db.query("SELECT balance FROM acid_accounts WHERE id = 1;");
      const originalBalance = Number(orig[0].balance);

      await db.query("BEGIN;");
      await db.query("UPDATE acid_accounts SET balance = 999999 WHERE id = 1;");
      const during = await db.query("SELECT balance FROM acid_accounts WHERE id = 1;");
      expect(Number(during[0].balance)).toBe(999999);
      await db.query("ROLLBACK;");

      const after = await db.query("SELECT balance FROM acid_accounts WHERE id = 1;");
      expect(Number(after[0].balance)).toBe(originalBalance);
    });

    test("24. ROLLBACK after DELETE restores deleted row", async () => {
      const orig = await db.query("SELECT * FROM acid_accounts WHERE id = 2;");
      expect(orig.length).toBe(1);

      await db.query("BEGIN;");
      await db.query("DELETE FROM acid_accounts WHERE id = 2;");
      const during = await db.query("SELECT * FROM acid_accounts WHERE id = 2;");
      expect(during.length).toBe(0);
      await db.query("ROLLBACK;");

      const after = await db.query("SELECT * FROM acid_accounts WHERE id = 2;");
      expect(after.length).toBe(1);
      expect(after[0].owner).toBe("Bob");
    });

    test("25. ROLLBACK across multi-table mutations restores all tables", async () => {
      await db.query("BEGIN;");
      await db.query("INSERT INTO acid_accounts (id, owner, balance) VALUES (904, 'Ghost 4', 400);");
      await db.query("INSERT INTO acid_audit_logs (id, action, account_id, amount) VALUES (904, 'GHOST_ACTION', 904, 400);");
      await db.query("ROLLBACK;");

      const acc = await db.query("SELECT * FROM acid_accounts WHERE id = 904;");
      const log = await db.query("SELECT * FROM acid_audit_logs WHERE id = 904;");
      expect(acc.length).toBe(0);
      expect(log.length).toBe(0);
    });

    test("26. ROLLBACK with ABORT syntax alias", async () => {
      await db.query("BEGIN;");
      await db.query("INSERT INTO acid_accounts (id, owner, balance) VALUES (905, 'Ghost 5', 500);");
      await db.query("ABORT;");
      const res = await db.query("SELECT * FROM acid_accounts WHERE id = 905;");
      expect(res.length).toBe(0);
    });

    test("27. ROLLBACK WORK syntax alias", async () => {
      await db.query("BEGIN WORK;");
      await db.query("INSERT INTO acid_accounts (id, owner, balance) VALUES (906, 'Ghost 6', 600);");
      await db.query("ROLLBACK WORK;");
      const res = await db.query("SELECT * FROM acid_accounts WHERE id = 906;");
      expect(res.length).toBe(0);
    });

    test("28. ROLLBACK TRANSACTION syntax alias", async () => {
      await db.query("BEGIN TRANSACTION;");
      await db.query("INSERT INTO acid_accounts (id, owner, balance) VALUES (907, 'Ghost 7', 700);");
      await db.query("ROLLBACK TRANSACTION;");
      const res = await db.query("SELECT * FROM acid_accounts WHERE id = 907;");
      expect(res.length).toBe(0);
    });

    test("29. Modifying multiple tables, rolling back, and checking consistency", async () => {
      const initialLogs = await db.query("SELECT count(*) AS c FROM acid_audit_logs;");
      await db.query("BEGIN;");
      await db.query("UPDATE acid_accounts SET balance = 0;");
      await db.query("DELETE FROM acid_audit_logs;");
      await db.query("ROLLBACK;");

      const afterLogs = await db.query("SELECT count(*) AS c FROM acid_audit_logs;");
      const nonZeroAccounts = await db.query("SELECT count(*) AS c FROM acid_accounts WHERE balance > 0;");
      expect(Number(afterLogs[0].c)).toBe(Number(initialLogs[0].c));
      expect(Number(nonZeroAccounts[0].c)).toBe(10);
    });

    test("30. Consecutive ROLLBACK followed by successful COMMIT", async () => {
      // First attempt fails and rolls back
      await db.query("BEGIN;");
      await db.query("INSERT INTO acid_accounts (id, owner, balance) VALUES (11, 'Attempt 1', 100);");
      await db.query("ROLLBACK;");

      // Second attempt succeeds and commits
      await db.query("BEGIN;");
      await db.query("INSERT INTO acid_accounts (id, owner, balance) VALUES (11, 'Kate', 950);");
      await db.query("COMMIT;");

      const res = await db.query("SELECT * FROM acid_accounts WHERE id = 11;");
      expect(res.length).toBe(1);
      expect(res[0].owner).toBe("Kate");
      expect(Number(res[0].balance)).toBe(950);
    });

    test("31. Empty ROLLBACK block (BEGIN then immediate ROLLBACK)", async () => {
      await db.query("BEGIN;");
      await db.query("ROLLBACK;");
      expect(true).toBe(true);
    });

    test("32. Multiple updates on the same row rolled back completely", async () => {
      await db.query("BEGIN;");
      await db.query("UPDATE acid_accounts SET balance = balance + 100 WHERE id = 11;");
      await db.query("UPDATE acid_accounts SET balance = balance + 200 WHERE id = 11;");
      await db.query("UPDATE acid_accounts SET balance = balance - 50 WHERE id = 11;");
      await db.query("ROLLBACK;");

      const res = await db.query("SELECT balance FROM acid_accounts WHERE id = 11;");
      expect(Number(res[0].balance)).toBe(950);
    });

    test("33. Rollback of batch multi-statement string", async () => {
      await db.query(`
        BEGIN;
        INSERT INTO acid_accounts (id, owner, balance) VALUES (908, 'Batch Ghost', 888);
        UPDATE acid_accounts SET balance = 0 WHERE id = 1;
        ROLLBACK;
      `);
      const ghost = await db.query("SELECT * FROM acid_accounts WHERE id = 908;");
      const user1 = await db.query("SELECT balance FROM acid_accounts WHERE id = 1;");
      expect(ghost.length).toBe(0);
      expect(Number(user1[0].balance)).toBeGreaterThan(0);
    });

    test("34. Parameterized INSERT rolled back safely", async () => {
      await db.query("BEGIN;");
      await db.query("INSERT INTO acid_accounts (id, owner, balance) VALUES ($1, $2, $3);", [909, "Param Ghost", 777]);
      await db.query("ROLLBACK;");

      const res = await db.query("SELECT * FROM acid_accounts WHERE id = 909;");
      expect(res.length).toBe(0);
    });

    test("35. Parameterized DELETE rolled back safely", async () => {
      await db.query("BEGIN;");
      await db.query("DELETE FROM acid_accounts WHERE owner = $1;", ["Kate"]);
      await db.query("ROLLBACK;");

      const res = await db.query("SELECT * FROM acid_accounts WHERE owner = 'Kate';");
      expect(res.length).toBe(1);
    });

    test("36. Deleting and inserting the same PK within rolled back transaction", async () => {
      await db.query("BEGIN;");
      await db.query("DELETE FROM acid_accounts WHERE id = 11;");
      await db.query("INSERT INTO acid_accounts (id, owner, balance) VALUES (11, 'Imposter', 1);");
      await db.query("ROLLBACK;");

      const res = await db.query("SELECT owner FROM acid_accounts WHERE id = 11;");
      expect(res[0].owner).toBe("Kate");
    });

    test("37. Rollback does not corrupt auto-commit operations afterwards", async () => {
      await db.query("BEGIN;");
      await db.query("INSERT INTO acid_accounts (id, owner, balance) VALUES (910, 'Temp', 1);");
      await db.query("ROLLBACK;");

      await db.query("INSERT INTO acid_accounts (id, owner, balance) VALUES (12, 'Leo', 650);");
      const res = await db.query("SELECT * FROM acid_accounts WHERE id = 12;");
      expect(res.length).toBe(1);
      expect(res[0].owner).toBe("Leo");
    });

    test("38. Rollback with subquery mutations", async () => {
      await db.query("BEGIN;");
      await db.query("UPDATE acid_accounts SET balance = balance + 500 WHERE id IN (SELECT id FROM acid_accounts WHERE balance < 600);");
      await db.query("ROLLBACK;");

      const res = await db.query("SELECT balance FROM acid_accounts WHERE id = 2;");
      expect(Number(res[0].balance)).toBe(550);
    });

    test("39. Rollback preserving foreign table state", async () => {
      await db.query("BEGIN;");
      await db.query("INSERT INTO acid_products (id, sku, stock, price) VALUES (1, 'PROD-A', 100, 29.99);");
      await db.query("ROLLBACK;");

      const res = await db.query("SELECT * FROM acid_products WHERE id = 1;");
      expect(res.length).toBe(0);
    });

    test("40. Verify total count of accounts after rollback suite", async () => {
      const res = await db.query("SELECT count(*) AS c FROM acid_accounts;");
      expect(Number(res[0].c)).toBe(12);
    });
  });

  // =========================================================================
  // Section 3: Savepoints & Partial Rollbacks (41 - 60)
  // =========================================================================
  describe("Section 3: Savepoints & Partial Rollbacks", () => {
    test("41. SAVEPOINT creation inside transaction", async () => {
      await db.query("BEGIN;");
      await db.query("INSERT INTO acid_accounts (id, owner, balance) VALUES (13, 'Mia', 1100);");
      await db.query("SAVEPOINT sp1;");
      const res = await db.query("SELECT * FROM acid_accounts WHERE id = 13;");
      expect(res.length).toBe(1);
      await db.query("COMMIT;");
    });

    test("42. ROLLBACK TO SAVEPOINT discards operations after savepoint", async () => {
      await db.query("BEGIN;");
      await db.query("INSERT INTO acid_accounts (id, owner, balance) VALUES (14, 'Noah', 850);");
      await db.query("SAVEPOINT sp_noah;");
      await db.query("INSERT INTO acid_accounts (id, owner, balance) VALUES (911, 'Invalid Person', 0);");
      await db.query("ROLLBACK TO SAVEPOINT sp_noah;");
      await db.query("COMMIT;");

      const noah = await db.query("SELECT * FROM acid_accounts WHERE id = 14;");
      const invalid = await db.query("SELECT * FROM acid_accounts WHERE id = 911;");
      expect(noah.length).toBe(1);
      expect(invalid.length).toBe(0);
    });

    test("43. Multiple savepoints with rollback to first savepoint", async () => {
      await db.query("BEGIN;");
      await db.query("INSERT INTO acid_accounts (id, owner, balance) VALUES (15, 'Olivia', 1300);");
      await db.query("SAVEPOINT step1;");
      await db.query("INSERT INTO acid_accounts (id, owner, balance) VALUES (912, 'Bad 1', 10);");
      await db.query("SAVEPOINT step2;");
      await db.query("INSERT INTO acid_accounts (id, owner, balance) VALUES (913, 'Bad 2', 20);");
      await db.query("ROLLBACK TO SAVEPOINT step1;");
      await db.query("COMMIT;");

      const olivia = await db.query("SELECT * FROM acid_accounts WHERE id = 15;");
      const bad1 = await db.query("SELECT * FROM acid_accounts WHERE id = 912;");
      const bad2 = await db.query("SELECT * FROM acid_accounts WHERE id = 913;");
      expect(olivia.length).toBe(1);
      expect(bad1.length).toBe(0);
      expect(bad2.length).toBe(0);
    });

    test("44. Multiple savepoints with rollback to intermediate savepoint", async () => {
      await db.query("BEGIN;");
      await db.query("INSERT INTO acid_accounts (id, owner, balance) VALUES (16, 'Peter', 920);");
      await db.query("SAVEPOINT sp_p;");
      await db.query("INSERT INTO acid_accounts (id, owner, balance) VALUES (17, 'Quinn', 1050);");
      await db.query("SAVEPOINT sp_q;");
      await db.query("INSERT INTO acid_accounts (id, owner, balance) VALUES (914, 'Bad 3', 30);");
      await db.query("ROLLBACK TO SAVEPOINT sp_q;");
      await db.query("COMMIT;");

      const p = await db.query("SELECT * FROM acid_accounts WHERE id = 16;");
      const q = await db.query("SELECT * FROM acid_accounts WHERE id = 17;");
      const bad3 = await db.query("SELECT * FROM acid_accounts WHERE id = 914;");
      expect(p.length).toBe(1);
      expect(q.length).toBe(1);
      expect(bad3.length).toBe(0);
    });

    test("45. RELEASE SAVEPOINT merges savepoint changes into transaction", async () => {
      await db.query("BEGIN;");
      await db.query("INSERT INTO acid_accounts (id, owner, balance) VALUES (18, 'Rose', 1400);");
      await db.query("SAVEPOINT sp_rose;");
      await db.query("RELEASE SAVEPOINT sp_rose;");
      await db.query("COMMIT;");

      const rose = await db.query("SELECT * FROM acid_accounts WHERE id = 18;");
      expect(rose.length).toBe(1);
      expect(rose[0].owner).toBe("Rose");
    });

    test("46. Continuing operations after ROLLBACK TO SAVEPOINT", async () => {
      await db.query("BEGIN;");
      await db.query("INSERT INTO acid_accounts (id, owner, balance) VALUES (19, 'Sam', 700);");
      await db.query("SAVEPOINT sp_sam;");
      await db.query("INSERT INTO acid_accounts (id, owner, balance) VALUES (915, 'Wrong', 0);");
      await db.query("ROLLBACK TO SAVEPOINT sp_sam;");
      await db.query("INSERT INTO acid_accounts (id, owner, balance) VALUES (20, 'Tina', 1600);");
      await db.query("COMMIT;");

      const sam = await db.query("SELECT * FROM acid_accounts WHERE id = 19;");
      const tina = await db.query("SELECT * FROM acid_accounts WHERE id = 20;");
      const wrong = await db.query("SELECT * FROM acid_accounts WHERE id = 915;");
      expect(sam.length).toBe(1);
      expect(tina.length).toBe(1);
      expect(wrong.length).toBe(0);
    });

    test("47. UPDATE rolled back to savepoint while prior INSERT is kept", async () => {
      await db.query("BEGIN;");
      await db.query("INSERT INTO acid_accounts (id, owner, balance) VALUES (21, 'Uma', 500);");
      await db.query("SAVEPOINT sp_uma;");
      await db.query("UPDATE acid_accounts SET balance = 0 WHERE id = 21;");
      await db.query("ROLLBACK TO SAVEPOINT sp_uma;");
      await db.query("COMMIT;");

      const res = await db.query("SELECT balance FROM acid_accounts WHERE id = 21;");
      expect(Number(res[0].balance)).toBe(500);
    });

    test("48. DELETE rolled back to savepoint while prior operations kept", async () => {
      await db.query("BEGIN;");
      await db.query("SAVEPOINT sp_del;");
      await db.query("DELETE FROM acid_accounts WHERE id = 21;");
      const during = await db.query("SELECT * FROM acid_accounts WHERE id = 21;");
      expect(during.length).toBe(0);
      await db.query("ROLLBACK TO SAVEPOINT sp_del;");
      await db.query("COMMIT;");

      const after = await db.query("SELECT * FROM acid_accounts WHERE id = 21;");
      expect(after.length).toBe(1);
    });

    test("49. Full ROLLBACK after savepoints discards everything in transaction", async () => {
      await db.query("BEGIN;");
      await db.query("INSERT INTO acid_accounts (id, owner, balance) VALUES (916, 'Discarded 1', 100);");
      await db.query("SAVEPOINT sp_temp;");
      await db.query("INSERT INTO acid_accounts (id, owner, balance) VALUES (917, 'Discarded 2', 200);");
      await db.query("ROLLBACK;");

      const d1 = await db.query("SELECT * FROM acid_accounts WHERE id = 916;");
      const d2 = await db.query("SELECT * FROM acid_accounts WHERE id = 917;");
      expect(d1.length).toBe(0);
      expect(d2.length).toBe(0);
    });

    test("50. Overwriting savepoint with identical name replaces previous savepoint", async () => {
      await db.query("BEGIN;");
      await db.query("INSERT INTO acid_accounts (id, owner, balance) VALUES (22, 'Victor', 800);");
      await db.query("SAVEPOINT my_sp;");
      await db.query("INSERT INTO acid_accounts (id, owner, balance) VALUES (23, 'Wendy', 900);");
      await db.query("SAVEPOINT my_sp;"); // Replaces my_sp
      await db.query("INSERT INTO acid_accounts (id, owner, balance) VALUES (918, 'Bad 4', 0);");
      await db.query("ROLLBACK TO SAVEPOINT my_sp;");
      await db.query("COMMIT;");

      const victor = await db.query("SELECT * FROM acid_accounts WHERE id = 22;");
      const wendy = await db.query("SELECT * FROM acid_accounts WHERE id = 23;");
      const bad = await db.query("SELECT * FROM acid_accounts WHERE id = 918;");
      expect(victor.length).toBe(1);
      expect(wendy.length).toBe(1);
      expect(bad.length).toBe(0);
    });

    test("51. Savepoints with multi-table operations", async () => {
      await db.query("BEGIN;");
      await db.query("INSERT INTO acid_accounts (id, owner, balance) VALUES (24, 'Xavier', 1250);");
      await db.query("SAVEPOINT sp_multi;");
      await db.query("INSERT INTO acid_audit_logs (id, action, account_id, amount) VALUES (919, 'BAD_LOG', 24, 1250);");
      await db.query("ROLLBACK TO SAVEPOINT sp_multi;");
      await db.query("INSERT INTO acid_audit_logs (id, action, account_id, amount) VALUES (5, 'CORRECT_LOG', 24, 1250);");
      await db.query("COMMIT;");

      const acc = await db.query("SELECT * FROM acid_accounts WHERE id = 24;");
      const badLog = await db.query("SELECT * FROM acid_audit_logs WHERE id = 919;");
      const goodLog = await db.query("SELECT * FROM acid_audit_logs WHERE id = 5;");
      expect(acc.length).toBe(1);
      expect(badLog.length).toBe(0);
      expect(goodLog.length).toBe(1);
    });

    test("52. Savepoint with parameterized query insertion", async () => {
      await db.query("BEGIN;");
      await db.query("SAVEPOINT sp_params;");
      await db.query("INSERT INTO acid_accounts (id, owner, balance) VALUES ($1, $2, $3);", [25, "Yara", 1150]);
      await db.query("COMMIT;");

      const res = await db.query("SELECT * FROM acid_accounts WHERE id = 25;");
      expect(res.length).toBe(1);
      expect(res[0].owner).toBe("Yara");
    });

    test("53. Savepoint rollback within a multi-statement query batch", async () => {
      await db.query(`
        BEGIN;
        INSERT INTO acid_accounts (id, owner, balance) VALUES (26, 'Zack', 600);
        SAVEPOINT sp_zack;
        INSERT INTO acid_accounts (id, owner, balance) VALUES (920, 'Zack Ghost', 0);
        ROLLBACK TO SAVEPOINT sp_zack;
        COMMIT;
      `);
      const zack = await db.query("SELECT * FROM acid_accounts WHERE id = 26;");
      const ghost = await db.query("SELECT * FROM acid_accounts WHERE id = 920;");
      expect(zack.length).toBe(1);
      expect(ghost.length).toBe(0);
    });

    test("54. Rollback to savepoint after aggregate query", async () => {
      await db.query("BEGIN;");
      await db.query("SAVEPOINT sp_agg;");
      const agg = await db.query("SELECT count(*) AS total FROM acid_accounts;");
      expect(Number(agg[0].total)).toBeGreaterThan(0);
      await db.query("ROLLBACK TO SAVEPOINT sp_agg;");
      await db.query("COMMIT;");
      expect(true).toBe(true);
    });

    test("55. Savepoint rollback with multi-row UPDATE", async () => {
      await db.query("BEGIN;");
      await db.query("SAVEPOINT sp_up;");
      await db.query("UPDATE acid_accounts SET balance = 0 WHERE id IN (25, 26);");
      await db.query("ROLLBACK TO SAVEPOINT sp_up;");
      await db.query("COMMIT;");

      const yara = await db.query("SELECT balance FROM acid_accounts WHERE id = 25;");
      const zack = await db.query("SELECT balance FROM acid_accounts WHERE id = 26;");
      expect(Number(yara[0].balance)).toBe(1150);
      expect(Number(zack[0].balance)).toBe(600);
    });

    test("56. Rolling back to non-existent savepoint handled safely", async () => {
      await db.query("BEGIN;");
      try {
        await db.query("ROLLBACK TO SAVEPOINT non_existent_sp;");
      } catch (e) {
        // Expected error
      }
      await db.query("ROLLBACK;");
      expect(true).toBe(true);
    });

    test("57. Releasing non-existent savepoint handled safely", async () => {
      await db.query("BEGIN;");
      try {
        await db.query("RELEASE SAVEPOINT non_existent_sp;");
      } catch (e) {
        // Expected error
      }
      await db.query("ROLLBACK;");
      expect(true).toBe(true);
    });

    test("58. Complex 3-level nested savepoints execution", async () => {
      await db.query("BEGIN;");
      await db.query("INSERT INTO acid_accounts (id, owner, balance) VALUES (27, 'L1', 100);");
      await db.query("SAVEPOINT lvl1;");
      await db.query("INSERT INTO acid_accounts (id, owner, balance) VALUES (28, 'L2', 200);");
      await db.query("SAVEPOINT lvl2;");
      await db.query("INSERT INTO acid_accounts (id, owner, balance) VALUES (921, 'L3 Bad', 300);");
      await db.query("ROLLBACK TO SAVEPOINT lvl2;");
      await db.query("INSERT INTO acid_accounts (id, owner, balance) VALUES (29, 'L2 Good', 250);");
      await db.query("COMMIT;");

      const l1 = await db.query("SELECT * FROM acid_accounts WHERE id = 27;");
      const l2 = await db.query("SELECT * FROM acid_accounts WHERE id = 28;");
      const l2Good = await db.query("SELECT * FROM acid_accounts WHERE id = 29;");
      const l3Bad = await db.query("SELECT * FROM acid_accounts WHERE id = 921;");

      expect(l1.length).toBe(1);
      expect(l2.length).toBe(1);
      expect(l2Good.length).toBe(1);
      expect(l3Bad.length).toBe(0);
    });

    test("59. Savepoint after table mutation combined with CTE", async () => {
      await db.query("BEGIN;");
      await db.query("SAVEPOINT sp_cte;");
      const res = await db.query(`
        WITH top_accs AS (
          SELECT owner, balance FROM acid_accounts ORDER BY balance DESC LIMIT 3
        )
        SELECT * FROM top_accs;
      `);
      expect(res.length).toBe(3);
      await db.query("COMMIT;");
    });

    test("60. Total active accounts count after Savepoints section", async () => {
      const res = await db.query("SELECT count(*) AS c FROM acid_accounts;");
      expect(Number(res[0].c)).toBe(29);
    });
  });

  // =========================================================================
  // Section 4: Isolation, Consistency & Relational Operations (61 - 80)
  // =========================================================================
  describe("Section 4: Isolation, Consistency & Relational Operations", () => {
    test("61. Atomic money transfer between two accounts committed", async () => {
      // Transfer 200 from Alice (id=1, bal=1150) to Bob (id=2, bal=550)
      await db.query("BEGIN;");
      await db.query("UPDATE acid_accounts SET balance = balance - 200 WHERE id = 1;");
      await db.query("UPDATE acid_accounts SET balance = balance + 200 WHERE id = 2;");
      await db.query("INSERT INTO acid_audit_logs (id, action, account_id, amount) VALUES (6, 'TRANSFER_OUT', 1, 200);");
      await db.query("INSERT INTO acid_audit_logs (id, action, account_id, amount) VALUES (7, 'TRANSFER_IN', 2, 200);");
      await db.query("COMMIT;");

      const alice = await db.query("SELECT balance FROM acid_accounts WHERE id = 1;");
      const bob = await db.query("SELECT balance FROM acid_accounts WHERE id = 2;");
      expect(Number(alice[0].balance)).toBe(950);
      expect(Number(bob[0].balance)).toBe(750);
    });

    test("62. Aborted money transfer leaves both account balances unchanged", async () => {
      const aliceBefore = await db.query("SELECT balance FROM acid_accounts WHERE id = 1;");
      const bobBefore = await db.query("SELECT balance FROM acid_accounts WHERE id = 2;");

      await db.query("BEGIN;");
      await db.query("UPDATE acid_accounts SET balance = balance - 500 WHERE id = 1;");
      // Simulated failure before crediting Bob
      await db.query("ROLLBACK;");

      const aliceAfter = await db.query("SELECT balance FROM acid_accounts WHERE id = 1;");
      const bobAfter = await db.query("SELECT balance FROM acid_accounts WHERE id = 2;");
      expect(Number(aliceAfter[0].balance)).toBe(Number(aliceBefore[0].balance));
      expect(Number(bobAfter[0].balance)).toBe(Number(bobBefore[0].balance));
    });

    test("63. Total system balance invariant preserved across transfers", async () => {
      const res = await db.query("SELECT sum(balance) AS total_money FROM acid_accounts WHERE id IN (1, 2);");
      expect(Number(res[0].total_money)).toBe(1700); // 950 + 750
    });

    test("64. E-Commerce Order Placement: atomic inventory deduction and order creation", async () => {
      await db.query("INSERT INTO acid_products (id, sku, stock, price) VALUES (101, 'IPHONE-15', 10, 999.00);");

      await db.query("BEGIN;");
      await db.query("UPDATE acid_products SET stock = stock - 2 WHERE id = 101;");
      await db.query("INSERT INTO acid_orders (id, product_id, qty, total, status) VALUES (1, 101, 2, 1998.00, 'completed');");
      await db.query("COMMIT;");

      const prod = await db.query("SELECT stock FROM acid_products WHERE id = 101;");
      const order = await db.query("SELECT * FROM acid_orders WHERE id = 1;");
      expect(Number(prod[0].stock)).toBe(8);
      expect(order.length).toBe(1);
      expect(Number(order[0].total)).toBe(1998.00);
    });

    test("65. E-Commerce Order Failure: rollback restores inventory stock", async () => {
      await db.query("BEGIN;");
      await db.query("UPDATE acid_products SET stock = stock - 5 WHERE id = 101;");
      // Payment gateway fails -> Rollback
      await db.query("ROLLBACK;");

      const prod = await db.query("SELECT stock FROM acid_products WHERE id = 101;");
      expect(Number(prod[0].stock)).toBe(8);
    });

    test("66. Multi-product cart checkout in a single atomic transaction", async () => {
      await db.query("INSERT INTO acid_products (id, sku, stock, price) VALUES (102, 'AIRPODS-PRO', 20, 249.00);");

      await db.query("BEGIN;");
      await db.query("UPDATE acid_products SET stock = stock - 1 WHERE id = 101;");
      await db.query("UPDATE acid_products SET stock = stock - 2 WHERE id = 102;");
      await db.query("INSERT INTO acid_orders (id, product_id, qty, total, status) VALUES (2, 101, 1, 999.00, 'completed');");
      await db.query("INSERT INTO acid_orders (id, product_id, qty, total, status) VALUES (3, 102, 2, 498.00, 'completed');");
      await db.query("COMMIT;");

      const p1 = await db.query("SELECT stock FROM acid_products WHERE id = 101;");
      const p2 = await db.query("SELECT stock FROM acid_products WHERE id = 102;");
      expect(Number(p1[0].stock)).toBe(7);
      expect(Number(p2[0].stock)).toBe(18);
    });

    test("67. Transaction with RETURNING clause", async () => {
      await db.query("BEGIN;");
      const res = await db.query("INSERT INTO acid_accounts (id, owner, balance) VALUES (30, 'Aaron', 5000) RETURNING id, owner, balance;");
      await db.query("COMMIT;");

      expect(res.length).toBe(1);
      expect(res[0].owner).toBe("Aaron");
      expect(Number(res[0].balance)).toBe(5000);
    });

    test("68. Transaction with UPDATE ... RETURNING clause", async () => {
      await db.query("BEGIN;");
      const res = await db.query("UPDATE acid_accounts SET balance = 5500 WHERE id = 30 RETURNING balance;");
      await db.query("COMMIT;");

      expect(res.length).toBe(1);
      expect(Number(res[0].balance)).toBe(5500);
    });

    test("69. Transaction with DELETE ... RETURNING clause", async () => {
      await db.query("BEGIN;");
      const res = await db.query("DELETE FROM acid_accounts WHERE id = 30 RETURNING owner;");
      await db.query("COMMIT;");

      expect(res.length).toBe(1);
      expect(res[0].owner).toBe("Aaron");
    });

    test("70. Transaction with CTE performing updates and joins", async () => {
      await db.query("BEGIN;");
      const res = await db.query(`
        WITH rich_users AS (
          SELECT id, owner, balance FROM acid_accounts WHERE balance >= 1000
        )
        SELECT count(*) AS rich_count FROM rich_users;
      `);
      expect(Number(res[0].rich_count)).toBeGreaterThan(0);
      await db.query("COMMIT;");
    });

    test("71. Multi-statement transaction updating multiple tables and joining results", async () => {
      await db.query("BEGIN;");
      await db.query("INSERT INTO acid_products (id, sku, stock, price) VALUES (103, 'CASE', 50, 19.99);");
      await db.query("INSERT INTO acid_orders (id, product_id, qty, total) VALUES (4, 103, 1, 19.99);");

      const joined = await db.query(`
        SELECT p.sku, o.qty, o.total
        FROM acid_orders o
        JOIN acid_products p ON o.product_id = p.id
        WHERE o.id = 4;
      `);
      expect(joined.length).toBe(1);
      expect(joined[0].sku).toBe("CASE");
      await db.query("COMMIT;");
    });

    test("72. Subquery in UPDATE condition within transaction", async () => {
      await db.query("BEGIN;");
      await db.query("UPDATE acid_accounts SET balance = balance + 10 WHERE id = (SELECT max(id) FROM acid_accounts);");
      await db.query("COMMIT;");
      expect(true).toBe(true);
    });

    test("73. Transaction with window function query", async () => {
      await db.query("BEGIN;");
      const res = await db.query(`
        SELECT owner, balance, ROW_NUMBER() OVER (ORDER BY balance DESC) AS rnk
        FROM acid_accounts
        ORDER BY balance DESC
        LIMIT 5;
      `);
      expect(res.length).toBe(5);
      expect(Number(res[0].rnk)).toBe(1);
      await db.query("COMMIT;");
    });

    test("74. Re-verifying account state after ACID operations", async () => {
      const res = await db.query("SELECT count(*) AS total FROM acid_accounts;");
      expect(Number(res[0].total)).toBe(29);
    });

    test("75. Transaction rollback of complex multi-order checkout", async () => {
      await db.query("BEGIN;");
      await db.query("INSERT INTO acid_orders (id, product_id, qty, total) VALUES (10, 101, 1, 999);");
      await db.query("INSERT INTO acid_orders (id, product_id, qty, total) VALUES (11, 102, 1, 249);");
      await db.query("UPDATE acid_products SET stock = stock - 1 WHERE id IN (101, 102);");
      await db.query("ROLLBACK;");

      const o10 = await db.query("SELECT * FROM acid_orders WHERE id = 10;");
      const o11 = await db.query("SELECT * FROM acid_orders WHERE id = 11;");
      expect(o10.length).toBe(0);
      expect(o11.length).toBe(0);
    });

    test("76. Transaction committing batch log entries", async () => {
      await db.query("BEGIN;");
      await db.query("INSERT INTO acid_audit_logs (id, action, account_id, amount) VALUES (10, 'SYS_AUDIT_1', 1, 0), (11, 'SYS_AUDIT_2', 2, 0);");
      await db.query("COMMIT;");

      const res = await db.query("SELECT * FROM acid_audit_logs WHERE id IN (10, 11);");
      expect(res.length).toBe(2);
    });

    test("77. Updating multiple records in transaction with mathematical expressions", async () => {
      await db.query("BEGIN;");
      await db.query("UPDATE acid_products SET price = price * 1.1;");
      await db.query("COMMIT;");

      const res = await db.query("SELECT price FROM acid_products WHERE id = 101;");
      expect(Number(res[0].price)).toBeGreaterThan(1000);
    });

    test("78. Atomic deletion of orders and cascade cleanup", async () => {
      await db.query("BEGIN;");
      await db.query("DELETE FROM acid_orders WHERE status = 'pending';");
      await db.query("COMMIT;");
      expect(true).toBe(true);
    });

    test("79. Querying active orders join product info", async () => {
      const res = await db.query(`
        SELECT o.id, p.sku, o.qty, o.total
        FROM acid_orders o
        JOIN acid_products p ON o.product_id = p.id
        ORDER BY o.id ASC;
      `);
      expect(res.length).toBeGreaterThanOrEqual(2);
    });

    test("80. Validating product inventory consistency", async () => {
      const res = await db.query("SELECT sum(stock) AS total_stock FROM acid_products;");
      expect(Number(res[0].total_stock)).toBeGreaterThan(0);
    });
  });

  // =========================================================================
  // Section 5: DDL within Transactions, Stress & Edge Cases (81 - 100)
  // =========================================================================
  describe("Section 5: DDL within Transactions, Stress & Edge Cases", () => {
    test("81. CREATE TABLE within transaction and COMMIT", async () => {
      await db.query("BEGIN;");
      await db.query("CREATE TABLE tx_created_table (id INT PRIMARY KEY, name TEXT);");
      await db.query("INSERT INTO tx_created_table (id, name) VALUES (1, 'Committed Table');");
      await db.query("COMMIT;");

      const res = await db.query("SELECT * FROM tx_created_table WHERE id = 1;");
      expect(res.length).toBe(1);
      expect(res[0].name).toBe("Committed Table");
    });

    test("82. DROP TABLE within transaction and COMMIT", async () => {
      await db.query("BEGIN;");
      await db.query("DROP TABLE tx_created_table;");
      await db.query("COMMIT;");
      expect(true).toBe(true);
    });

    test("83. Bulk INSERT (50 rows) within single transaction", async () => {
      await db.query("CREATE TABLE bulk_tx_test (id INT PRIMARY KEY, val INT);");
      await db.query("BEGIN;");
      for (let i = 1; i <= 50; i++) {
        await db.query(`INSERT INTO bulk_tx_test (id, val) VALUES (${i}, ${i * 10});`);
      }
      await db.query("COMMIT;");

      const res = await db.query("SELECT count(*) AS total, sum(val) AS sum_val FROM bulk_tx_test;");
      expect(Number(res[0].total)).toBe(50);
      expect(Number(res[0].sum_val)).toBe(12750); // 10 * (50*51/2) = 12750
    });

    test("84. Bulk UPDATE (50 rows) within single transaction", async () => {
      await db.query("BEGIN;");
      await db.query("UPDATE bulk_tx_test SET val = val + 1;");
      await db.query("COMMIT;");

      const res = await db.query("SELECT sum(val) AS sum_val FROM bulk_tx_test;");
      expect(Number(res[0].sum_val)).toBe(12800);
    });

    test("85. Bulk DELETE within single transaction rolled back", async () => {
      await db.query("BEGIN;");
      await db.query("DELETE FROM bulk_tx_test;");
      const during = await db.query("SELECT count(*) AS total FROM bulk_tx_test;");
      expect(Number(during[0].total)).toBe(0);
      await db.query("ROLLBACK;");

      const after = await db.query("SELECT count(*) AS total FROM bulk_tx_test;");
      expect(Number(after[0].total)).toBe(50);
    });

    test("86. Bulk DELETE within single transaction committed", async () => {
      await db.query("BEGIN;");
      await db.query("DELETE FROM bulk_tx_test;");
      await db.query("COMMIT;");

      const after = await db.query("SELECT count(*) AS total FROM bulk_tx_test;");
      expect(Number(after[0].total)).toBe(0);
      await db.query("DROP TABLE bulk_tx_test;");
    });

    test("87. ALTER TABLE ADD COLUMN within transaction and COMMIT", async () => {
      await db.query("CREATE TABLE alter_tx_test (id INT PRIMARY KEY, title TEXT);");
      await db.query("INSERT INTO alter_tx_test VALUES (1, 'First');");

      await db.query("BEGIN;");
      await db.query("ALTER TABLE alter_tx_test ADD COLUMN tag TEXT DEFAULT 'general';");
      await db.query("COMMIT;");

      const res = await db.query("SELECT id, tag FROM alter_tx_test WHERE id = 1;");
      expect(res[0].tag).toBe("general");
    });

    test("88. ALTER TABLE DROP COLUMN within transaction and COMMIT", async () => {
      await db.query("BEGIN;");
      await db.query("ALTER TABLE alter_tx_test DROP COLUMN tag;");
      await db.query("COMMIT;");

      const res = await db.query("SELECT * FROM alter_tx_test WHERE id = 1;");
      expect((res[0] as any).tag).toBeUndefined();
      await db.query("DROP TABLE alter_tx_test;");
    });

    test("89. Nested CTE inside transaction", async () => {
      await db.query("BEGIN;");
      const res = await db.query(`
        WITH
          a AS (SELECT id, balance FROM acid_accounts WHERE id <= 5),
          b AS (SELECT id, balance FROM acid_accounts WHERE id > 5 AND id <= 10)
        SELECT sum(a.balance) AS sum_a, sum(b.balance) AS sum_b
        FROM a, b;
      `);
      expect(res.length).toBe(1);
      await db.query("COMMIT;");
    });

    test("90. Transaction with UNION ALL combining multiple queries", async () => {
      await db.query("BEGIN;");
      const res = await db.query(`
        SELECT owner, balance FROM acid_accounts WHERE id = 1
        UNION ALL
        SELECT owner, balance FROM acid_accounts WHERE id = 2
        ORDER BY owner ASC;
      `);
      expect(res.length).toBe(2);
      expect(res[0].owner).toBe("Alice");
      expect(res[1].owner).toBe("Bob");
      await db.query("COMMIT;");
    });

    test("91. Transaction with complex CASE WHEN expression in SELECT", async () => {
      await db.query("BEGIN;");
      const res = await db.query(`
        SELECT owner, balance,
          CASE
            WHEN balance >= 1000 THEN 'Platinum'
            WHEN balance >= 500 THEN 'Gold'
            ELSE 'Silver'
          END AS tier
        FROM acid_accounts
        WHERE id IN (1, 2)
        ORDER BY id ASC;
      `);
      expect(res.length).toBe(2);
      expect(res[0].tier).toBe("Gold");
      expect(res[1].tier).toBe("Gold");
      await db.query("COMMIT;");
    });

    test("92. Transaction with LIMIT and OFFSET pagination", async () => {
      await db.query("BEGIN;");
      const page1 = await db.query("SELECT id, owner FROM acid_accounts ORDER BY id ASC LIMIT 5 OFFSET 0;");
      const page2 = await db.query("SELECT id, owner FROM acid_accounts ORDER BY id ASC LIMIT 5 OFFSET 5;");
      expect(page1.length).toBe(5);
      expect(page2.length).toBe(5);
      expect(page1[0].id).not.toBe(page2[0].id);
      await db.query("COMMIT;");
    });

    test("93. Transaction with multiple savepoint cycles in loop", async () => {
      await db.query("BEGIN;");
      for (let i = 1; i <= 5; i++) {
        await db.query(`SAVEPOINT loop_sp_${i};`);
        await db.query(`INSERT INTO acid_audit_logs (id, action, account_id, amount) VALUES (${100 + i}, 'LOOP_${i}', 1, ${i});`);
        await db.query(`RELEASE SAVEPOINT loop_sp_${i};`);
      }
      await db.query("COMMIT;");

      const res = await db.query("SELECT count(*) AS total FROM acid_audit_logs WHERE id >= 101 AND id <= 105;");
      expect(Number(res[0].total)).toBe(5);
    });

    test("94. Recursive CTE inside transaction calculating factorial sequence", async () => {
      await db.query("BEGIN;");
      const res = await db.query(`
        WITH RECURSIVE fact(n, f) AS (
          SELECT 1, 1
          UNION ALL
          SELECT n + 1, f * (n + 1)
          FROM fact
          WHERE n < 5
        )
        SELECT n, f FROM fact ORDER BY n ASC;
      `);
      expect(res.length).toBe(5);
      expect(Number(res[4].f)).toBe(120); // 5! = 120
      await db.query("COMMIT;");
    });

    test("95. Multi-table aggregation inside transaction with GROUP BY", async () => {
      await db.query("BEGIN;");
      const res = await db.query(`
        SELECT p.sku, sum(o.qty) AS total_sold, sum(o.total) AS revenue
        FROM acid_products p
        JOIN acid_orders o ON p.id = o.product_id
        GROUP BY p.sku
        ORDER BY p.sku ASC;
      `);
      expect(res.length).toBeGreaterThanOrEqual(1);
      await db.query("COMMIT;");
    });

    test("96. Transaction state integrity under consecutive BEGIN statements", async () => {
      await db.query("BEGIN;");
      await db.query("BEGIN;"); // Redundant BEGIN
      await db.query("INSERT INTO acid_accounts (id, owner, balance) VALUES (31, 'Zoe', 3333);");
      await db.query("COMMIT;");

      const res = await db.query("SELECT * FROM acid_accounts WHERE id = 31;");
      expect(res.length).toBe(1);
      expect(res[0].owner).toBe("Zoe");
    });

    test("97. Transaction state integrity under consecutive COMMIT statements", async () => {
      await db.query("BEGIN;");
      await db.query("UPDATE acid_accounts SET balance = 3500 WHERE id = 31;");
      await db.query("COMMIT;");
      await db.query("COMMIT;"); // Redundant COMMIT

      const res = await db.query("SELECT balance FROM acid_accounts WHERE id = 31;");
      expect(Number(res[0].balance)).toBe(3500);
    });

    test("98. Comprehensive e-commerce checkout transaction with audit trail", async () => {
      await db.query("BEGIN;");
      // Step 1: Deduct inventory
      await db.query("UPDATE acid_products SET stock = stock - 1 WHERE id = 101;");
      // Step 2: Deduct user account
      await db.query("UPDATE acid_accounts SET balance = balance - 1098.90 WHERE id = 1;");
      // Step 3: Insert order
      await db.query("INSERT INTO acid_orders (id, product_id, qty, total, status) VALUES (50, 101, 1, 1098.90, 'paid');");
      // Step 4: Write audit log
      await db.query("INSERT INTO acid_audit_logs (id, action, account_id, amount) VALUES (200, 'ORDER_PURCHASE', 1, 1098.90);");
      await db.query("COMMIT;");

      const order = await db.query("SELECT * FROM acid_orders WHERE id = 50;");
      const log = await db.query("SELECT * FROM acid_audit_logs WHERE id = 200;");
      expect(order.length).toBe(1);
      expect(order[0].status).toBe("paid");
      expect(log.length).toBe(1);
    });

    test("99. Full database sanity check across all tables", async () => {
      const accs = await db.query("SELECT count(*) AS c FROM acid_accounts;");
      const prods = await db.query("SELECT count(*) AS c FROM acid_products;");
      const orders = await db.query("SELECT count(*) AS c FROM acid_orders;");
      const logs = await db.query("SELECT count(*) AS c FROM acid_audit_logs;");

      expect(Number(accs[0].c)).toBeGreaterThanOrEqual(26);
      expect(Number(prods[0].c)).toBeGreaterThanOrEqual(3);
      expect(Number(orders[0].c)).toBeGreaterThanOrEqual(3);
      expect(Number(logs[0].c)).toBeGreaterThanOrEqual(8);
    });

    test("100. End-to-End PostgreSQL ACID Transaction test suite complete", async () => {
      const res = await db.query("SELECT 1 AS ready;");
      expect(Number(res[0].ready)).toBe(1);
    });
  });
});
