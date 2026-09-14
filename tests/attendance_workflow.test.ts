import { describe, test, expect, beforeEach } from "bun:test";
import { PGLite } from "../src/index";

describe("Attendance Lifecycle & DateTime Formats Test Suite", () => {
  let db: PGLite;

  beforeEach(async () => {
    db = new PGLite(":memory:", { native: true });

    // Tạo bảng attendances với cấu trúc chuẩn
    await db.query(`
      CREATE TABLE attendances (
        id SERIAL PRIMARY KEY,
        user_id INTEGER NOT NULL,
        school_id INTEGER NOT NULL,
        check_in_time TIMESTAMP NOT NULL,
        check_out_time TIMESTAMP,
        status TEXT NOT NULL,
        lateness_minutes INTEGER,
        photo_url TEXT,
        wrong_position BOOLEAN DEFAULT false,
        no_fuel_allowance BOOLEAN DEFAULT false,
        no_travel_allowance BOOLEAN DEFAULT false,
        support_amount NUMERIC DEFAULT 0,
        is_non_salary_session BOOLEAN DEFAULT false,
        schedule_time_slot_id INTEGER,
        adhoc_schedule_id INTEGER,
        created_by INTEGER NOT NULL,
        created_at TIMESTAMP NOT NULL,
        updated_at TIMESTAMP NOT NULL
      );
    `);
  });

  test("1. Check-In đúng giờ (On-time) với ca định kỳ (schedule_time_slot_id)", async () => {
    await db.query(`
      INSERT INTO attendances (
        user_id,
        school_id,
        check_in_time,
        check_out_time,
        status,
        lateness_minutes,
        photo_url,
        wrong_position,
        no_fuel_allowance,
        no_travel_allowance,
        support_amount,
        is_non_salary_session,
        schedule_time_slot_id,
        adhoc_schedule_id,
        created_by,
        created_at,
        updated_at
      )
      VALUES (
        1,
        3,
        NOW(),
        NULL,
        'on_time',
        NULL,
        'https://example.com/checkin.jpg',
        false,
        false,
        false,
        0,
        false,
        12,
        NULL,
        1,
        NOW(),
        NOW()
      );
    `);

    const rows = await db.query<any>("SELECT * FROM attendances WHERE user_id = 1;");
    expect(rows.length).toBe(1);
    expect(Number(rows[0].user_id)).toBe(1);
    expect(Number(rows[0].school_id)).toBe(3);
    expect(rows[0].status).toBe("on_time");
    expect(rows[0].check_out_time).toBeNull();
    expect(rows[0].photo_url).toBe("https://example.com/checkin.jpg");
    expect(rows[0].wrong_position).toBe(false);
    expect(Number(rows[0].schedule_time_slot_id)).toBe(12);
    expect(rows[0].adhoc_schedule_id).toBeNull();
  });

  test("2. Exact Payload từ App Debug (ISO 8601 với milliseconds 'Z', support_amount string '0')", async () => {
    const params = [
      826,
      1978,
      "2026-09-09T15:32:34.636Z",
      "2026-09-09T15:32:34.636Z",
      "2026-09-09T15:32:34.636Z",
      "on_time",
      "https://hcm04.vstorage.vngcloud.vn/files/attendance.jpg",
      false,
      66,
      false,
      false,
      "0",
      false,
      826
    ];

    const sql = `
      INSERT INTO attendances (
        user_id, school_id, check_in_time, created_at, updated_at,
        status, photo_url, wrong_position, schedule_time_slot_id,
        no_fuel_allowance, no_travel_allowance, support_amount,
        is_non_salary_session, created_by
      ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
      RETURNING *;
    `;

    const res = await db.query<any>(sql, params);
    expect(res.length).toBe(1);
    expect(Number(res[0].user_id)).toBe(826);
    expect(Number(res[0].school_id)).toBe(1978);
    expect(res[0].check_in_time).toBe("2026-09-09T15:32:34.636Z");
    expect(Number(res[0].schedule_time_slot_id)).toBe(66);
    expect(Number(res[0].support_amount)).toBe(0);
    expect(res[0].wrong_position).toBe(false);
  });

  test("3. Hỗ trợ định dạng ISO 8601 kèm Timezone Offset (+07:00)", async () => {
    const params = [
      827,
      1978,
      "2026-09-09T22:32:34.636+07:00",
      "2026-09-09T22:32:34.636+07:00",
      "2026-09-09T22:32:34.636+07:00",
      "on_time",
      827
    ];

    await db.query(`
      INSERT INTO attendances (
        user_id, school_id, check_in_time, created_at, updated_at, status, created_by
      ) VALUES ($1, $2, $3, $4, $5, $6, $7);
    `, params);

    const rows = await db.query<any>("SELECT * FROM attendances WHERE user_id = 827;");
    expect(rows.length).toBe(1);
    expect(rows[0].check_in_time).toBe("2026-09-09T22:32:34.636+07:00");
  });

  test("4. Hỗ trợ định dạng SQL Timestamp tiêu chuẩn (YYYY-MM-DD HH:MM:SS.mmm)", async () => {
    await db.query(`
      INSERT INTO attendances (
        user_id, school_id, check_in_time, created_at, updated_at, status, created_by
      ) VALUES ($1, $2, $3, $4, $5, $6, $7);
    `, [828, 1978, "2026-09-09 15:32:34.636", "2026-09-09 15:32:34.636", "2026-09-09 15:32:34.636", "on_time", 828]);

    const rows = await db.query<any>("SELECT * FROM attendances WHERE user_id = 828;");
    expect(rows.length).toBe(1);
    expect(rows[0].check_in_time).toBe("2026-09-09 15:32:34.636");
  });

  test("5. Hỗ trợ truyền JavaScript Date Object trực tiếp vào params", async () => {
    const now = new Date("2026-09-09T15:32:34.636Z");
    await db.query(`
      INSERT INTO attendances (
        user_id, school_id, check_in_time, created_at, updated_at, status, created_by
      ) VALUES ($1, $2, $3, $4, $5, $6, $7);
    `, [829, 1978, now, now, now, "on_time", 829]);

    const rows = await db.query<any>("SELECT * FROM attendances WHERE user_id = 829;");
    expect(rows.length).toBe(1);
    expect(rows[0].check_in_time).toBe("2026-09-09T15:32:34.636Z");
  });

  test("6. Check-In đi muộn (Late) kèm số phút muộn (lateness_minutes)", async () => {
    await db.query(`
      INSERT INTO attendances (
        user_id, school_id, check_in_time, check_out_time,
        status, lateness_minutes, photo_url,
        wrong_position, no_fuel_allowance, no_travel_allowance,
        support_amount, is_non_salary_session,
        schedule_time_slot_id, adhoc_schedule_id,
        created_by, created_at, updated_at
      ) VALUES (
        2, 3, "2026-09-09T15:32:34.636Z", NULL,
        'late', 15, 'https://example.com/late.jpg',
        false, false, false,
        0, false,
        14, NULL,
        2, "2026-09-09T15:32:34.636Z", "2026-09-09T15:32:34.636Z"
      );
    `);

    const rows = await db.query<any>("SELECT * FROM attendances WHERE user_id = 2;");
    expect(rows.length).toBe(1);
    expect(rows[0].status).toBe("late");
    expect(Number(rows[0].lateness_minutes)).toBe(15);
  });

  test("7. Check-In ca dạy bù / đột xuất (adhoc_schedule_id) có phụ cấp hỗ trợ", async () => {
    await db.query(`
      INSERT INTO attendances (
        user_id, school_id, check_in_time, check_out_time,
        status, lateness_minutes, photo_url,
        wrong_position, no_fuel_allowance, no_travel_allowance,
        support_amount, is_non_salary_session,
        schedule_time_slot_id, adhoc_schedule_id,
        created_by, created_at, updated_at
      ) VALUES (
        3, 4, NOW(), NULL,
        'unscheduled', NULL, NULL,
        false, false, false,
        50000, false,
        NULL, 88,
        3, NOW(), NOW()
      );
    `);

    const rows = await db.query<any>("SELECT * FROM attendances WHERE user_id = 3;");
    expect(rows.length).toBe(1);
    expect(rows[0].status).toBe("unscheduled");
    expect(rows[0].schedule_time_slot_id).toBeNull();
    expect(Number(rows[0].adhoc_schedule_id)).toBe(88);
    expect(Number(rows[0].support_amount)).toBe(50000);
  });

  test("8. Check-Out thành công (UPDATE check_out_time và updated_at)", async () => {
    await db.query(`
      INSERT INTO attendances (
        user_id, school_id, check_in_time, check_out_time, status,
        lateness_minutes, photo_url, wrong_position, no_fuel_allowance,
        no_travel_allowance, support_amount, is_non_salary_session,
        schedule_time_slot_id, adhoc_schedule_id, created_by, created_at, updated_at
      )
      VALUES (
        1, 3, "2026-09-09T15:00:00.000Z", NULL, 'on_time',
        NULL, 'https://example.com/checkin.jpg', false, false,
        false, 0, false, 12, NULL, 1, "2026-09-09T15:00:00.000Z", "2026-09-09T15:00:00.000Z"
      );
    `);

    // Thực hiện Check-Out bằng ISO timestamp string
    await db.query(`
      UPDATE attendances
      SET check_out_time = $1, updated_at = $2
      WHERE user_id = $3 AND check_out_time IS NULL;
    `, ["2026-09-09T17:00:00.000Z", "2026-09-09T17:00:00.000Z", 1]);

    const rows = await db.query<any>("SELECT * FROM attendances WHERE user_id = 1;");
    expect(rows.length).toBe(1);
    expect(rows[0].check_out_time).toBe("2026-09-09T17:00:00.000Z");
  });

  test("9. Truy vấn lọc theo ngày (DATE / String comparison) với định dạng ISO", async () => {
    await db.query(`
      INSERT INTO attendances (user_id, school_id, check_in_time, created_at, updated_at, status, created_by)
      VALUES (50, 1, '2026-09-09T08:00:00.000Z', NOW(), NOW(), 'on_time', 50),
             (51, 1, '2026-09-08T08:00:00.000Z', NOW(), NOW(), 'on_time', 51);
    `);

    const todayRows = await db.query<any>(`
      SELECT * FROM attendances WHERE check_in_time >= '2026-09-09T00:00:00.000Z';
    `);
    expect(todayRows.length).toBe(1);
    expect(Number(todayRows[0].user_id)).toBe(50);
  });

  test("10. Transaction ACID: ROLLBACK an toàn khi thao tác điểm danh gặp lỗi", async () => {
    await db.query("BEGIN;");
    await db.query(`
      INSERT INTO attendances (
        user_id, school_id, check_in_time, check_out_time,
        status, lateness_minutes, photo_url,
        wrong_position, no_fuel_allowance, no_travel_allowance,
        support_amount, is_non_salary_session,
        schedule_time_slot_id, adhoc_schedule_id,
        created_by, created_at, updated_at
      ) VALUES (
        99, 5, "2026-09-09T15:32:34.636Z", NULL,
        'on_time', NULL, NULL,
        false, false, false,
        0, false,
        20, NULL,
        99, "2026-09-09T15:32:34.636Z", "2026-09-09T15:32:34.636Z"
      );
    `);

    const inTx = await db.query<any>("SELECT * FROM attendances WHERE user_id = 99;");
    expect(inTx.length).toBe(1);

    await db.query("ROLLBACK;");

    const afterRollback = await db.query<any>("SELECT * FROM attendances WHERE user_id = 99;");
    expect(afterRollback.length).toBe(0);
  });
});
