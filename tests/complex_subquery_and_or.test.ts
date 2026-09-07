import { describe, test, expect, beforeEach, afterEach } from "bun:test";
import { PGLiteNative } from "../src/native";
import * as fs from "fs";

describe("Comprehensive Test Suite: Complex AND / OR with Nested Subqueries in WHERE", () => {
  let db: PGLiteNative;
  const dbPath = "./test_complex_subqueries_suite.db";

  beforeEach(async () => {
    try { if (fs.existsSync(dbPath)) fs.unlinkSync(dbPath); } catch {}
    db = new PGLiteNative(dbPath);

    await db.exec(`
      CREATE TABLE regions (
        id SERIAL PRIMARY KEY,
        name TEXT,
        deleted_at TIMESTAMP
      );

      CREATE TABLE schools (
        id SERIAL PRIMARY KEY,
        name TEXT,
        region_id INT,
        deleted_at TIMESTAMP
      );

      CREATE TABLE users (
        id SERIAL PRIMARY KEY,
        username TEXT,
        email TEXT,
        full_name TEXT,
        phone_number TEXT,
        avatar_url TEXT,
        region_id INT,
        role TEXT,
        is_active BOOLEAN,
        start_work_date TEXT,
        official_work_date TEXT,
        date_of_birth TEXT,
        created_at TIMESTAMP,
        deleted_at TIMESTAMP
      );

      CREATE TABLE region_admins (
        id SERIAL PRIMARY KEY,
        user_id INT,
        region_id INT,
        deleted_at TIMESTAMP
      );

      CREATE TABLE school_admins (
        id SERIAL PRIMARY KEY,
        user_id INT,
        school_id INT,
        deleted_at TIMESTAMP
      );
    `);

    // Ingest reference data
    await db.exec(`
      INSERT INTO regions (id, name, deleted_at) VALUES 
        (1, 'Miền Bắc', NULL),
        (2, 'Miền Nam', NULL),
        (3, 'Miền Trung', NULL);

      INSERT INTO schools (id, name, region_id, deleted_at) VALUES
        (101, 'THPT Phan Chu Trinh', 3, NULL),
        (102, 'THPT Le Quy Don', 1, NULL);

      -- User 1: Direct region 3, Active Teacher
      INSERT INTO users (id, username, email, full_name, phone_number, region_id, role, is_active, start_work_date, created_at, deleted_at)
      VALUES (1, 'u1_teacher', 'u1@gmail.com', 'Nguyen Van A', '0901', 3, 'TEACHER', true, '2024-01-01', '2026-09-01 10:00:00', NULL);

      -- User 2: Region 1, Admin of Region 3 in region_admins
      INSERT INTO users (id, username, email, full_name, phone_number, region_id, role, is_active, start_work_date, created_at, deleted_at)
      VALUES (2, 'u2_reg_admin', 'u2@gmail.com', 'Tran Thi B', '0902', 1, 'ADMIN', true, '2024-02-01', '2026-09-02 11:00:00', NULL);
      INSERT INTO region_admins (id, user_id, region_id, deleted_at) VALUES (1, 2, 3, NULL);

      -- User 3: Region 1, School Admin of School 101 (in region 3)
      INSERT INTO users (id, username, email, full_name, phone_number, region_id, role, is_active, start_work_date, created_at, deleted_at)
      VALUES (3, 'u3_sch_admin', 'u3@gmail.com', 'Le Van C', '0903', 1, 'SCHOOL_ADMIN', true, '2023-01-01', '2026-09-03 12:00:00', NULL);
      INSERT INTO school_admins (id, user_id, school_id, deleted_at) VALUES (1, 3, 101, NULL);

      -- User 4: Region 1 only, no admin roles, active
      INSERT INTO users (id, username, email, full_name, phone_number, region_id, role, is_active, start_work_date, created_at, deleted_at)
      VALUES (4, 'u4_unrelated', 'u4@gmail.com', 'Pham Van D', '0904', 1, 'TEACHER', true, '2023-05-01', '2026-09-04 13:00:00', NULL);

      -- User 5: Region 3 direct, but SOFT-DELETED (deleted_at is set)
      INSERT INTO users (id, username, email, full_name, phone_number, region_id, role, is_active, start_work_date, created_at, deleted_at)
      VALUES (5, 'u5_deleted_direct', 'u5@gmail.com', 'Hoang Van E', '0905', 3, 'TEACHER', false, '2022-01-01', '2026-09-05 14:00:00', '2026-09-06 00:00:00');

      -- User 6: Region 1, in region_admins for Region 3, but region_admin record is SOFT-DELETED
      INSERT INTO users (id, username, email, full_name, phone_number, region_id, role, is_active, start_work_date, created_at, deleted_at)
      VALUES (6, 'u6_del_reg_admin', 'u6@gmail.com', 'Vo Thi F', '0906', 1, 'ADMIN', true, '2024-03-01', '2026-09-06 15:00:00', NULL);
      INSERT INTO region_admins (id, user_id, region_id, deleted_at) VALUES (2, 6, 3, '2026-09-07 00:00:00');
    `);
  });

  afterEach(async () => {
    await db.close();
    try { if (fs.existsSync(dbPath)) fs.unlinkSync(dbPath); } catch {}
  });

  test("1. Basic AND (direct OR IN subquery) matches only valid users", async () => {
    const q = `
      SELECT
        users.id,
        users.full_name AS "fullName",
        regions.name AS "regionName"
      FROM users
      LEFT JOIN regions ON regions.id = users.region_id
      WHERE users.deleted_at IS NULL
        AND (
          users.region_id = 3
          OR users.id IN (
            SELECT region_admins.user_id
            FROM region_admins
            WHERE region_admins.region_id = 3
              AND region_admins.deleted_at IS NULL
          )
        )
      ORDER BY users.id ASC;
    `;
    const res = await db.query(q);
    expect(res.length).toBe(2);
    expect(res[0].id).toBe(1); // direct region 3
    expect(res[0].fullName).toBe("Nguyen Van A");
    expect(res[1].id).toBe(2); // active region admin of 3
    expect(res[1].fullName).toBe("Tran Thi B");
  });

  test("2. Parameterized ($1, $2) and Named Parameters (:REGION_ID)", async () => {
    const q = `
      SELECT users.id, users.username
      FROM users
      WHERE users.deleted_at IS NULL
        AND (
          users.region_id = :REGION_ID
          OR users.id IN (
            SELECT region_admins.user_id
            FROM region_admins
            WHERE region_admins.region_id = :REGION_ID
              AND region_admins.deleted_at IS NULL
          )
        )
      ORDER BY users.id ASC;
    `;
    const res = await db.query(q, { REGION_ID: 3 });
    expect(res.map((r: any) => r.id)).toEqual([1, 2]);
  });

  test("3. Multiple OR branches with 2 distinct Subqueries (region_admins OR school_admins)", async () => {
    const q = `
      SELECT users.id, users.username, users.role
      FROM users
      WHERE users.deleted_at IS NULL
        AND (
          users.region_id = 3
          OR users.id IN (
            SELECT region_admins.user_id
            FROM region_admins
            WHERE region_admins.region_id = 3
              AND region_admins.deleted_at IS NULL
          )
          OR users.id IN (
            SELECT school_admins.user_id
            FROM school_admins
            WHERE school_admins.school_id = 101
              AND school_admins.deleted_at IS NULL
          )
        )
      ORDER BY users.id ASC;
    `;
    const res = await db.query(q);
    // User 1 (direct 3), User 2 (reg admin 3), User 3 (sch admin 101)
    expect(res.map((r: any) => r.id)).toEqual([1, 2, 3]);
  });

  test("4. Multi-level nested AND / OR with role checks and subqueries", async () => {
    const q = `
      SELECT users.id, users.role
      FROM users
      WHERE users.deleted_at IS NULL
        AND (
          (users.role = 'TEACHER' AND users.region_id = 3)
          OR (users.role = 'ADMIN' AND users.id IN (
            SELECT region_admins.user_id
            FROM region_admins
            WHERE region_admins.region_id = 3
              AND region_admins.deleted_at IS NULL
          ))
        )
      ORDER BY users.id ASC;
    `;
    const res = await db.query(q);
    expect(res.length).toBe(2);
    expect(res[0].id).toBe(1);
    expect(res[1].id).toBe(2);
  });

  test("5. NOT IN Subquery filtering", async () => {
    const q = `
      SELECT users.id
      FROM users
      WHERE users.deleted_at IS NULL
        AND users.id NOT IN (
          SELECT region_admins.user_id
          FROM region_admins
          WHERE region_admins.deleted_at IS NULL
        )
      ORDER BY users.id ASC;
    `;
    const res = await db.query(q);
    // Users that are NOT active region admins (1, 3, 4, 6)
    expect(res.map((r: any) => r.id)).toEqual([1, 3, 4, 6]);
  });

  test("6. COUNT(DISTINCT users.id) accurately matches item row count", async () => {
    const qCount = `
      SELECT COUNT(DISTINCT users.id) AS "totalCount"
      FROM users
      WHERE users.deleted_at IS NULL
        AND (
          users.region_id = 3
          OR users.id IN (
            SELECT region_admins.user_id
            FROM region_admins
            WHERE region_admins.region_id = 3
              AND region_admins.deleted_at IS NULL
          )
        );
    `;
    const res = await db.query(qCount);
    expect(res[0].totalCount).toBe(2);
  });

  test("7. Pagination with LIMIT and OFFSET on complex subquery query", async () => {
    const qPage1 = `
      SELECT users.id
      FROM users
      WHERE users.deleted_at IS NULL
        AND (
          users.region_id = 3
          OR users.id IN (
            SELECT region_admins.user_id
            FROM region_admins
            WHERE region_admins.region_id = 3
              AND region_admins.deleted_at IS NULL
          )
        )
      ORDER BY users.id ASC
      LIMIT 1 OFFSET 0;
    `;
    const page1 = await db.query(qPage1);
    expect(page1.length).toBe(1);
    expect(page1[0].id).toBe(1);

    const qPage2 = `
      SELECT users.id
      FROM users
      WHERE users.deleted_at IS NULL
        AND (
          users.region_id = 3
          OR users.id IN (
            SELECT region_admins.user_id
            FROM region_admins
            WHERE region_admins.region_id = 3
              AND region_admins.deleted_at IS NULL
          )
        )
      ORDER BY users.id ASC
      LIMIT 1 OFFSET 1;
    `;
    const page2 = await db.query(qPage2);
    expect(page2.length).toBe(1);
    expect(page2[0].id).toBe(2);
  });

  test("8. Subquery returning 0 rows falls back cleanly to direct condition", async () => {
    const q = `
      SELECT users.id
      FROM users
      WHERE users.deleted_at IS NULL
        AND (
          users.region_id = 3
          OR users.id IN (
            SELECT region_admins.user_id
            FROM region_admins
            WHERE region_admins.region_id = 99999 -- non-existent region
              AND region_admins.deleted_at IS NULL
          )
        );
    `;
    const res = await db.query(q);
    expect(res.length).toBe(1);
    expect(res[0].id).toBe(1);
  });

  test("9. Soft-deleted exclusion inside subquery (User 6 admin record is deleted)", async () => {
    // User 6 is assigned to region 3 in region_admins, but that row has deleted_at set!
    const q = `
      SELECT users.id
      FROM users
      WHERE users.deleted_at IS NULL
        AND users.id IN (
          SELECT region_admins.user_id
          FROM region_admins
          WHERE region_admins.region_id = 3
            AND region_admins.deleted_at IS NULL
        );
    `;
    const res = await db.query(q);
    expect(res.length).toBe(1);
    expect(res[0].id).toBe(2); // Only User 2, User 6 is excluded because deleted_at is not null
  });

  test("10. Full production query with camelCase aliases and LEFT JOIN", async () => {
    const q = `
      SELECT
        users.id,
        users.username,
        users.email,
        users.full_name            AS "fullName",
        users.phone_number         AS "phoneNumber",
        users.avatar_url           AS "avatarUrl",
        users.region_id            AS "regionId",
        users.role,
        regions.name               AS "regionName",
        users.is_active            AS "isActive",
        users.start_work_date      AS "startWorkDate",
        users.official_work_date   AS "officialWorkDate",
        users.date_of_birth        AS "dateOfBirth",
        users.created_at           AS "createdAt"
      FROM users
      LEFT JOIN regions
        ON regions.id = users.region_id
      WHERE users.deleted_at IS NULL
        AND (
          users.region_id = 3
          OR users.id IN (
            SELECT region_admins.user_id
            FROM region_admins
            WHERE region_admins.region_id = 3
              AND region_admins.deleted_at IS NULL
          )
        )
      ORDER BY users.created_at DESC
      LIMIT 10
      OFFSET 0;
    `;
    const res = await db.query(q);
    expect(res.length).toBe(2);
    expect(res[0].fullName).toBe("Tran Thi B");
    expect(res[0].regionName).toBe("Miền Bắc");
    expect(res[1].fullName).toBe("Nguyen Van A");
    expect(res[1].regionName).toBe("Miền Trung");
  });

  test("11. User exact scenario: ID 4 and ID 64 (both region_id = 3, one active, one inactive, different timestamps)", async () => {
    // Insert user's exact records
    await db.exec(`
      INSERT INTO regions (id, name, deleted_at) VALUES (30, 'hcm-fix-bug', NULL);

      INSERT INTO users (id, username, email, full_name, phone_number, avatar_url, region_id, role, is_active, start_work_date, official_work_date, date_of_birth, created_at, deleted_at)
      VALUES 
        (400, 'vietduong', NULL, 'vietduong', NULL, NULL, 30, 'user', true, '2026-09-06', '2026-09-06', '2026-09-06', '2026-09-06 17:04:09.908', NULL),
        (640, 'vietduong1', NULL, 'Viet Dương 2', NULL, NULL, 30, 'user', false, NULL, NULL, NULL, '2026-09-07 18:17:08.393', NULL);
    `);

    const qDirect = `
      SELECT
        users.id,
        users.username,
        users.full_name AS "fullName",
        users.region_id AS "regionId",
        regions.name AS "regionName",
        users.is_active AS "isActive"
      FROM users
      LEFT JOIN regions ON regions.id = users.region_id
      WHERE users.deleted_at IS NULL
        AND (users.region_id = 30)
      ORDER BY users.created_at DESC;
    `;
    const directRes = await db.query(qDirect);
    expect(directRes.length).toBe(2);
    expect(directRes[0].id).toBe(640);
    expect(directRes[0].fullName).toBe("Viet Dương 2");
    expect(directRes[0].isActive).toBe(false);
    expect(directRes[1].id).toBe(400);
    expect(directRes[1].fullName).toBe("vietduong");
    expect(directRes[1].isActive).toBe(true);

    const qSubquery = `
      SELECT
        users.id,
        users.username,
        users.full_name AS "fullName",
        users.region_id AS "regionId",
        regions.name AS "regionName",
        users.is_active AS "isActive"
      FROM users
      LEFT JOIN regions ON regions.id = users.region_id
      WHERE users.deleted_at IS NULL
        AND (
          users.region_id = 30
          OR users.id IN (
            SELECT region_admins.user_id
            FROM region_admins
            WHERE region_admins.region_id = 30
              AND region_admins.deleted_at IS NULL
          )
        )
      ORDER BY users.created_at DESC;
    `;
    const subqueryRes = await db.query(qSubquery);
    expect(subqueryRes.length).toBe(2);
    expect(subqueryRes[0].id).toBe(640);
    expect(subqueryRes[0].fullName).toBe("Viet Dương 2");
    expect(subqueryRes[1].id).toBe(400);
    expect(subqueryRes[1].fullName).toBe("vietduong");
  });

  test("12. Dynamic INSERT immediately queried with subquery and ORDER BY created_at DESC", async () => {
    // Insert a new user on the fly
    await db.exec(`
      INSERT INTO users (id, username, full_name, region_id, role, is_active, created_at, deleted_at)
      VALUES (999, 'new_dynamic_user', 'Nguyen Dynamic', 3, 'user', true, '2026-09-08 00:00:00', NULL);
    `);

    const q = `
      SELECT users.id, users.username, users.full_name AS "fullName"
      FROM users
      WHERE users.deleted_at IS NULL
        AND (
          users.region_id = 3
          OR users.id IN (
            SELECT region_admins.user_id
            FROM region_admins
            WHERE region_admins.region_id = 3
              AND region_admins.deleted_at IS NULL
          )
        )
      ORDER BY users.created_at DESC;
    `;
    const res = await db.query(q);
    expect(res.length).toBe(3);
    expect(res[0].id).toBe(999);
    expect(res[0].fullName).toBe("Nguyen Dynamic");
  });

  test("13. Dynamic UPDATE retains subquery matching correctness", async () => {
    // Change User 4 (who previously had region 1) to region 3
    await db.exec(`UPDATE users SET region_id = 3 WHERE id = 4;`);

    const q = `
      SELECT users.id, users.username
      FROM users
      WHERE users.deleted_at IS NULL
        AND (
          users.region_id = 3
          OR users.id IN (
            SELECT region_admins.user_id
            FROM region_admins
            WHERE region_admins.region_id = 3
              AND region_admins.deleted_at IS NULL
          )
        )
      ORDER BY users.id ASC;
    `;
    const res = await db.query(q);
    // Users 1, 2, and now 4 should match
    expect(res.map((r: any) => r.id)).toEqual([1, 2, 4]);
  });

  test("14. Filtering with active status along with subquery: AND is_active = true vs false", async () => {
    const qActive = `
      SELECT users.id
      FROM users
      WHERE users.deleted_at IS NULL
        AND users.is_active = true
        AND (
          users.region_id = 3
          OR users.id IN (
            SELECT region_admins.user_id
            FROM region_admins
            WHERE region_admins.region_id = 3
              AND region_admins.deleted_at IS NULL
          )
        )
      ORDER BY users.id ASC;
    `;
    const activeRes = await db.query(qActive);
    expect(activeRes.map((r: any) => r.id)).toEqual([1, 2]);

    // Make User 1 inactive
    await db.exec(`UPDATE users SET is_active = false WHERE id = 1;`);
    const activeResAfter = await db.query(qActive);
    expect(activeResAfter.map((r: any) => r.id)).toEqual([2]); // Only User 2 remains active
  });

  test("15. Multi-column search with LIKE and subquery in WHERE", async () => {
    const q = `
      SELECT users.id, users.full_name AS "fullName"
      FROM users
      WHERE users.deleted_at IS NULL
        AND (users.full_name LIKE '%Van%' OR users.username LIKE '%teacher%')
        AND (
          users.region_id = 3
          OR users.id IN (
            SELECT region_admins.user_id
            FROM region_admins
            WHERE region_admins.region_id = 3
              AND region_admins.deleted_at IS NULL
          )
        )
      ORDER BY users.id ASC;
    `;
    const res = await db.query(q);
    expect(res.length).toBe(1);
    expect(res[0].id).toBe(1);
  });

  test("16. Subquery with NULL values in region_admins.user_id", async () => {
    await db.exec(`INSERT INTO region_admins (id, user_id, region_id, deleted_at) VALUES (99, NULL, 3, NULL);`);

    const q = `
      SELECT users.id
      FROM users
      WHERE users.deleted_at IS NULL
        AND (
          users.region_id = 3
          OR users.id IN (
            SELECT region_admins.user_id
            FROM region_admins
            WHERE region_admins.region_id = 3
              AND region_admins.deleted_at IS NULL
          )
        )
      ORDER BY users.id ASC;
    `;
    const res = await db.query(q);
    expect(res.map((r: any) => r.id)).toEqual([1, 2]);
  });

  test("17. Subquery with JOIN inside the subquery itself", async () => {
    const q = `
      SELECT users.id, users.username
      FROM users
      WHERE users.deleted_at IS NULL
        AND (
          users.region_id = 3
          OR users.id IN (
            SELECT ra.user_id
            FROM region_admins ra
            JOIN regions r ON r.id = ra.region_id
            WHERE r.name = 'Miền Trung'
              AND ra.deleted_at IS NULL
          )
        )
      ORDER BY users.id ASC;
    `;
    const res = await db.query(q);
    expect(res.map((r: any) => r.id)).toEqual([1, 2]);
  });

  test("18. Persistence verification across DB close and reopen", async () => {
    await db.exec(`
      INSERT INTO users (id, username, full_name, region_id, role, is_active, created_at, deleted_at)
      VALUES (777, 'persisted_user', 'Tran Persist', 3, 'user', true, '2026-09-07 12:00:00', NULL);
    `);

    await db.close();

    const dbReopened = new PGLiteNative(dbPath);

    const q = `
      SELECT users.id, users.username, users.full_name AS "fullName"
      FROM users
      WHERE users.deleted_at IS NULL
        AND (
          users.region_id = 3
          OR users.id IN (
            SELECT region_admins.user_id
            FROM region_admins
            WHERE region_admins.region_id = 3
              AND region_admins.deleted_at IS NULL
          )
        )
      ORDER BY users.id ASC;
    `;
    const res = await dbReopened.query(q);
    expect(res.map((r: any) => r.id)).toContain(777);
    expect(res.find((r: any) => r.id === 777).fullName).toBe("Tran Persist");
    await dbReopened.close();
  });

  test("19. COUNT(DISTINCT) with various subquery and filter combinations", async () => {
    const countTotal = `
      SELECT COUNT(DISTINCT users.id) AS "cnt"
      FROM users
      WHERE users.deleted_at IS NULL
        AND (
          users.region_id = 3
          OR users.id IN (
            SELECT region_admins.user_id
            FROM region_admins
            WHERE region_admins.region_id = 3
              AND region_admins.deleted_at IS NULL
          )
        );
    `;
    const resTotal = await db.query(countTotal);
    expect(Number(resTotal[0].cnt)).toBe(2);

    const countRole = `
      SELECT COUNT(DISTINCT users.id) AS "cnt"
      FROM users
      WHERE users.deleted_at IS NULL
        AND users.role = 'ADMIN'
        AND (
          users.region_id = 3
          OR users.id IN (
            SELECT region_admins.user_id
            FROM region_admins
            WHERE region_admins.region_id = 3
              AND region_admins.deleted_at IS NULL
          )
        );
    `;
    const resRole = await db.query(countRole);
    expect(Number(resRole[0].cnt)).toBe(1);
  });

  test("20. Large dataset subquery handling (100 users, 10 region admins)", async () => {
    const userInserts: string[] = [];
    for (let i = 1000; i < 1100; i++) {
      const regId = (i % 3) + 1; // 1, 2, or 3
      userInserts.push(`(${i}, 'user_${i}', 'u${i}@test.com', 'User ${i}', '0900', NULL, ${regId}, 'user', true, '2024-01-01', '2024-01-01', '2000-01-01', '2026-09-01 00:00:00', NULL)`);
    }
    await db.exec(`INSERT INTO users (id, username, email, full_name, phone_number, avatar_url, region_id, role, is_active, start_work_date, official_work_date, date_of_birth, created_at, deleted_at) VALUES ${userInserts.join(", ")};`);

    const adminInserts: string[] = [];
    for (let i = 1000; i < 1010; i++) {
      adminInserts.push(`(${i + 5000}, ${i}, 3, NULL)`);
    }
    await db.exec(`INSERT INTO region_admins (id, user_id, region_id, deleted_at) VALUES ${adminInserts.join(", ")};`);

    const q = `
      SELECT COUNT(DISTINCT users.id) AS "cnt"
      FROM users
      WHERE users.deleted_at IS NULL
        AND (
          users.region_id = 3
          OR users.id IN (
            SELECT region_admins.user_id
            FROM region_admins
            WHERE region_admins.region_id = 3
              AND region_admins.deleted_at IS NULL
          )
        );
    `;
    const res = await db.query(q);
    expect(Number(res[0].cnt)).toBe(35);
  });
});
