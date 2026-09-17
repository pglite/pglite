import { describe, test, expect, beforeAll } from "bun:test";
import { PGLite } from "../src/index";

describe("Comprehensive Complex Booking Query Test Suite", () => {
  let db: PGLite;

  beforeAll(async () => {
    db = new PGLite(":memory:", { native: true });

    await db.query(`
      CREATE TABLE users (
        id INT PRIMARY KEY,
        full_name TEXT NOT NULL
      );

      CREATE TABLE retreat_programs (
        id INT PRIMARY KEY,
        name TEXT NOT NULL,
        thumbnail_url TEXT
      );

      CREATE TABLE rooms (
        id INT PRIMARY KEY,
        name TEXT NOT NULL,
        room_type TEXT,
        price_per_night NUMERIC,
        images TEXT,
        amenities TEXT,
        description TEXT,
        capacity INT,
        area_sqm NUMERIC
      );

      CREATE TABLE bookings (
        id INT PRIMARY KEY,
        booking_code TEXT NOT NULL,
        check_in_date TEXT,
        program_id INT,
        check_out_date TEXT,
        room_info TEXT,
        room_id INT,
        check_in_status TEXT,
        phone_number TEXT,
        user_id INT,
        payment_status TEXT,
        paid_amount NUMERIC,
        payment_method TEXT,
        deleted_at TEXT
      );

      CREATE TABLE intake_forms (
        id INT PRIMARY KEY,
        booking_id INT NOT NULL,
        status TEXT NOT NULL
      );

      CREATE TABLE user_checklists (
        id INT PRIMARY KEY,
        booking_id INT NOT NULL,
        deleted_at TEXT,
        is_completed BOOLEAN NOT NULL
      );
    `);

    // Insert sample data
    await db.query(`
      INSERT INTO users (id, full_name) VALUES 
        (1, 'Nguyen Van A'),
        (2, 'Tran Thi B'),
        (3, 'Le Van C');

      INSERT INTO retreat_programs (id, name, thumbnail_url) VALUES 
        (10, 'Healing Meditation 3D2N', 'https://example.com/thumb1.jpg'),
        (20, 'Yoga Detox Weekend', 'https://example.com/thumb2.jpg'),
        (30, 'Silent Mindfulness', 'https://example.com/thumb3.jpg');

      INSERT INTO rooms (id, name, room_type, price_per_night, images, amenities, description, capacity, area_sqm) VALUES 
        (100, 'Deluxe Ocean View', 'Deluxe', 2500000, 'img1.jpg,img2.jpg', 'WiFi,AC,Bath', 'Luxury ocean room', 2, 45.5),
        (200, 'Garden Villa', 'Villa', 4000000, 'v1.jpg', 'Pool,WiFi', 'Private villa', 4, 120.0),
        (300, 'Standard Mountain', 'Standard', 1200000, 'm1.jpg', 'WiFi,Fan', 'Cozy mountain room', 1, 28.0);

      INSERT INTO bookings (id, booking_code, check_in_date, program_id, check_out_date, room_info, room_id, check_in_status, phone_number, user_id, payment_status, paid_amount, payment_method, deleted_at) VALUES 
        (1001, ' BK-2026-XYZ ', '2026-10-01', 10, '2026-10-04', 'Deluxe double bed', 100, 'confirmed', '0901234567', 1, 'paid', 7500000, 'credit_card', NULL),
        (1002, 'BK-DELETED', '2026-10-05', 20, '2026-10-08', 'Garden villa', 200, 'cancelled', '0909999999', 2, 'refunded', 0, 'bank_transfer', '2026-09-01'),
        (1003, 'BK-NO-INTAKE', '2026-11-01', 10, '2026-11-04', 'Deluxe single', 100, 'pending', '0911222333', 1, 'unpaid', 0, 'cash', NULL),
        (1004, 'bk-lowercase-004', '2026-12-01', 30, '2026-12-05', 'Mountain single', 300, 'confirmed', '0988776655', 3, 'paid', 4800000, 'credit_card', NULL);

      INSERT INTO intake_forms (id, booking_id, status) VALUES 
        (501, 1001, 'completed'),
        (502, 1004, 'in_review');

      INSERT INTO user_checklists (id, booking_id, deleted_at, is_completed) VALUES 
        (1, 1001, NULL, true),
        (2, 1001, NULL, true),
        (3, 1001, NULL, false),
        (4, 1001, '2026-09-10', true),
        (5, 1003, NULL, false),
        (6, 1004, NULL, true),
        (7, 1004, NULL, true),
        (8, 1004, NULL, true);
    `);
  });

  test("1. Executes the exact complex booking query with subqueries, joins, and parameters ($1, $2, $3)", async () => {
    const query = `
      select "b"."id", "b"."booking_code", "b"."check_in_date", "b"."program_id", "b"."check_out_date", "b"."room_info", "b"."room_id", "b"."check_in_status", "b"."phone_number", "b"."user_id", "b"."payment_status", "b"."paid_amount", "b"."payment_method", "p"."name" as "program_name", "p"."thumbnail_url", "u"."full_name" as "user_name", "r"."name" as "room_name", "r"."room_type" as "room_type", "r"."price_per_night" as "room_price", "r"."images" as "room_images", "r"."amenities" as "room_amenities", "r"."description" as "room_description", "r"."capacity" as "room_capacity", "r"."area_sqm" as "room_area", (select "status" from "intake_forms" where "booking_id" = "b"."id" limit $1) as "intake_status", (select count("id") as "count" from "user_checklists" where "booking_id" = "b"."id" and "deleted_at" is null) as "checklist_total", (select count("id") as "count" from "user_checklists" where "booking_id" = "b"."id" and "deleted_at" is null and "is_completed" = $2) as "checklist_done" from "bookings" as "b" left join "retreat_programs" as "p" on "p"."id" = "b"."program_id" left join "users" as "u" on "u"."id" = "b"."user_id" left join "rooms" as "r" on "r"."id" = "b"."room_id" where btrim(upper(b.booking_code)) = $3 and "b"."deleted_at" is null
    `;

    const res = await db.query(query, [1, true, "BK-2026-XYZ"]);

    expect(res.length).toBe(1);
    const row = res[0];

    // Primary table and Joined tables check
    expect(Number(row.id)).toBe(1001);
    expect(row.booking_code).toBe(" BK-2026-XYZ ");
    expect(row.program_name).toBe("Healing Meditation 3D2N");
    expect(row.thumbnail_url).toBe("https://example.com/thumb1.jpg");
    expect(row.user_name).toBe("Nguyen Van A");
    expect(row.room_name).toBe("Deluxe Ocean View");
    expect(row.room_type).toBe("Deluxe");
    expect(Number(row.room_price)).toBe(2500000);
    expect(Number(row.room_capacity)).toBe(2);
    expect(Number(row.room_area)).toBe(45.5);

    // Correlated Subqueries check
    expect(row.intake_status).toBe("completed");
    expect(Number(row.checklist_total)).toBe(3); // 3 non-deleted (ids: 1, 2, 3)
    expect(Number(row.checklist_done)).toBe(2);  // 2 non-deleted and is_completed = true (ids: 1, 2)
  });

  test("2. Handles case-insensitive and trimmed booking_code with btrim(upper(...))", async () => {
    const query = `
      select 
        "b"."id",
        "b"."booking_code",
        "u"."full_name" as "user_name",
        (select "status" from "intake_forms" where "booking_id" = "b"."id" limit $1) as "intake_status",
        (select count("id") as "count" from "user_checklists" where "booking_id" = "b"."id" and "deleted_at" is null and "is_completed" = $2) as "checklist_done"
      from "bookings" as "b"
      left join "users" as "u" on "u"."id" = "b"."user_id"
      where btrim(upper(b.booking_code)) = $3 and "b"."deleted_at" is null
    `;

    const res = await db.query(query, [1, true, "BK-LOWERCASE-004"]);
    expect(res.length).toBe(1);
    const row = res[0];

    expect(Number(row.id)).toBe(1004);
    expect(row.booking_code).toBe("bk-lowercase-004");
    expect(row.user_name).toBe("Le Van C");
    expect(row.intake_status).toBe("in_review");
    expect(Number(row.checklist_done)).toBe(3);
  });

  test("3. Handles subquery returning NULL when child record does not exist", async () => {
    const query = `
      select 
        "b"."id",
        (select "status" from "intake_forms" where "booking_id" = "b"."id" limit $1) as "intake_status",
        (select count("id") as "count" from "user_checklists" where "booking_id" = "b"."id" and "deleted_at" is null) as "checklist_total",
        (select count("id") as "count" from "user_checklists" where "booking_id" = "b"."id" and "deleted_at" is null and "is_completed" = $2) as "checklist_done"
      from "bookings" as "b"
      where btrim(upper(b.booking_code)) = $3 and "b"."deleted_at" is null
    `;

    const res = await db.query(query, [1, true, "BK-NO-INTAKE"]);
    expect(res.length).toBe(1);
    const row = res[0];

    expect(Number(row.id)).toBe(1003);
    expect(row.intake_status).toBeNull();
    expect(Number(row.checklist_total)).toBe(1);
    expect(Number(row.checklist_done)).toBe(0);
  });

  test("4. Correctly filters out soft deleted bookings in outer WHERE clause", async () => {
    const query = `
      select "b"."id"
      from "bookings" as "b"
      where btrim(upper(b.booking_code)) = $1 and "b"."deleted_at" is null
    `;

    const res = await db.query(query, ["BK-DELETED"]);
    expect(res.length).toBe(0);
  });

  test("5. Counts false completion status when $2 is false", async () => {
    const query = `
      select 
        "b"."id",
        (select count("id") as "count" from "user_checklists" where "booking_id" = "b"."id" and "deleted_at" is null and "is_completed" = $1) as "checklist_pending"
      from "bookings" as "b"
      where "b"."id" = 1001
    `;

    const res = await db.query(query, [false]);
    expect(res.length).toBe(1);
    expect(Number(res[0].checklist_pending)).toBe(1); // id: 3
  });

  test("6. Performs atomic updates inside transaction with subqueries", async () => {
    await db.query("BEGIN;");
    await db.query("UPDATE user_checklists SET is_completed = true WHERE id = 3;");

    const query = `
      select 
        (select count("id") as "count" from "user_checklists" where "booking_id" = "b"."id" and "deleted_at" is null and "is_completed" = $1) as "checklist_done"
      from "bookings" as "b"
      where "b"."id" = 1001;
    `;
    const res = await db.query(query, [true]);
    expect(Number(res[0].checklist_done)).toBe(3);

    await db.query("ROLLBACK;");

    const rollbackRes = await db.query(query, [true]);
    expect(Number(rollbackRes[0].checklist_done)).toBe(2);
  });
});
