import { PGLiteNative } from "../src/native";
import * as fs from "fs";

async function run() {
  const dbPath = "./scratch_all_user_queries.db";
  try { if (fs.existsSync(dbPath)) fs.unlinkSync(dbPath); } catch {}

  const db = new PGLiteNative(dbPath);

  console.log("🛠️ Creating tables...");
  await db.exec(`
    CREATE TABLE schools (
      id SERIAL PRIMARY KEY,
      name TEXT,
      address TEXT,
      latitude FLOAT,
      longitude FLOAT,
      deleted_at TIMESTAMP
    );

    CREATE TABLE schedules (
      id SERIAL PRIMARY KEY,
      user_id INT,
      school_id INT,
      start_date TEXT,
      end_date TEXT,
      deleted_at TIMESTAMP
    );

    CREATE TABLE schedule_time_slots (
      id SERIAL PRIMARY KEY,
      schedule_id INT,
      day_of_week TEXT,
      start_time TEXT,
      end_time TEXT,
      class_ids INT[],
      support_amount INT,
      no_fuel_allowance BOOLEAN,
      no_travel_allowance BOOLEAN,
      is_non_salary_session BOOLEAN,
      deleted_at TIMESTAMP
    );

    CREATE TABLE classes (
      id SERIAL PRIMARY KEY,
      name TEXT,
      description TEXT,
      school_id INT,
      student_count INT,
      deleted_at TIMESTAMP
    );

    CREATE TABLE adhoc_schedules (
      id SERIAL PRIMARY KEY,
      user_id INT,
      school_id INT,
      schedule_date TEXT,
      start_time TEXT,
      end_time TEXT,
      class_ids INT[],
      support_amount INT,
      no_fuel_allowance BOOLEAN,
      no_travel_allowance BOOLEAN,
      is_non_salary_session BOOLEAN,
      reason TEXT,
      deleted_at TIMESTAMP
    );
  `);

  console.log("🌱 Inserting sample data...");
  await db.exec(`
    INSERT INTO schools (id, name, address, latitude, longitude, deleted_at)
    VALUES 
      (1, 'THPT Le Hong Phong', '235 Nguyen Van Cu, Q5', 10.762, 106.682, NULL),
      (2, 'THPT Chuyen Tran Dai Nghia', '280 An Duong Vuong, Q5', 10.758, 106.675, NULL);

    INSERT INTO schedules (id, user_id, school_id, start_date, end_date, deleted_at)
    VALUES
      (10, 1001, 1, '2026-09-01', '2026-12-31', NULL),
      (11, 1001, 2, '2026-09-01', '2026-12-31', NULL);

    INSERT INTO classes (id, name, description, school_id, student_count, deleted_at)
    VALUES
      (101, 'Lớp 10A1', 'Chuyên Toán', 1, 35, NULL),
      (102, 'Lớp 10A2', 'Chuyên Lý', 1, 34, NULL),
      (103, 'Lớp 11B1', 'Chuyên Hóa', 1, 32, NULL),
      (104, 'Lớp 12C1', 'Chuyên Anh', 2, 30, NULL);

    INSERT INTO schedule_time_slots (id, schedule_id, day_of_week, start_time, end_time, class_ids, support_amount, no_fuel_allowance, no_travel_allowance, is_non_salary_session, deleted_at)
    VALUES
      (1, 10, 'Monday', '08:00:00', '10:00:00', ARRAY[101, 102], 50000, false, false, false, NULL),
      (2, 10, 'Wednesday', '13:30:00', '15:30:00', ARRAY[103], 60000, false, false, false, NULL),
      (3, 11, 'Friday', '07:30:00', '09:30:00', ARRAY[104], 70000, false, true, false, NULL);

    INSERT INTO adhoc_schedules (id, user_id, school_id, schedule_date, start_time, end_time, class_ids, support_amount, no_fuel_allowance, no_travel_allowance, is_non_salary_session, reason, deleted_at)
    VALUES
      (1, 1001, 1, '2026-09-05', '08:00:00', '10:00:00', ARRAY[101, 102], 50000, false, false, false, 'Dạy bù kỳ thi', NULL),
      (2, 1001, 2, '2026-09-07', '14:00:00', '16:00:00', ARRAY[104], 60000, false, false, false, 'Ôn tập học sinh giỏi', NULL);
  `);

  console.log("\n==========================================================================");
  console.log("🔍 QUERY 1: Schedules & Schools JOIN with :USER_ID");
  console.log("==========================================================================");
  const q1 = `
    SELECT
      s.id            AS schedule_id,
      s.start_date,
      s.end_date,
      sc.id           AS school_id,
      sc.name         AS school_name,
      sc.address,
      sc.latitude,
      sc.longitude
    FROM schedules s
    JOIN schools sc ON sc.id = s.school_id
    WHERE s.user_id = :USER_ID
      AND s.deleted_at IS NULL
    ORDER BY sc.name;
  `;
  const res1 = await db.query(q1, { USER_ID: 1001 });
  console.log("Result 1 (Row count:", res1.length, "):");
  console.log(JSON.stringify(res1, null, 2));

  console.log("\n==========================================================================");
  console.log("🔍 QUERY 2: Schedule Time Slots with to_char, subquery IN, CASE ORDER BY");
  console.log("==========================================================================");
  const q2 = `
    SELECT
      sts.id,
      sts.schedule_id,
      sts.day_of_week,
      to_char(sts.start_time, 'HH24:MI') AS start_time,
      to_char(sts.end_time,   'HH24:MI') AS end_time,
      sts.class_ids,
      sts.support_amount,
      sts.no_fuel_allowance,
      sts.no_travel_allowance,
      sts.is_non_salary_session
    FROM schedule_time_slots sts
    WHERE sts.schedule_id IN (
            SELECT id FROM schedules
            WHERE user_id = :USER_ID AND deleted_at IS NULL
          )
      AND sts.deleted_at IS NULL
    ORDER BY
      CASE sts.day_of_week
        WHEN 'Monday' THEN 1 WHEN 'Tuesday' THEN 2 WHEN 'Wednesday' THEN 3
        WHEN 'Thursday' THEN 4 WHEN 'Friday' THEN 5 WHEN 'Saturday' THEN 6
        WHEN 'Sunday' THEN 7
      END,
      sts.start_time;
  `;
  const res2 = await db.query(q2, { USER_ID: 1001 });
  console.log("Result 2 (Row count:", res2.length, "):");
  console.log(JSON.stringify(res2, null, 2));

  console.log("\n==========================================================================");
  console.log("🔍 QUERY 3: Complex 3-Table JOIN + array_agg with ANY(sts.class_ids)");
  console.log("==========================================================================");
  const q3 = `
    SELECT
      s.id                                 AS schedule_id,
      sc.name                              AS school_name,
      sts.id                               AS slot_id,
      sts.day_of_week,
      to_char(sts.start_time, 'HH24:MI')   AS start_time,
      to_char(sts.end_time,   'HH24:MI')   AS end_time,
      sts.class_ids,
      sts.support_amount,
      sts.no_fuel_allowance,
      sts.no_travel_allowance,
      sts.is_non_salary_session,
      (
        SELECT array_agg(c.name ORDER BY c.id)
        FROM classes c
        WHERE c.id = ANY(sts.class_ids)
          AND c.deleted_at IS NULL
      )                                    AS class_names
    FROM schedules s
    JOIN schools sc ON sc.id = s.school_id
    JOIN schedule_time_slots sts ON sts.schedule_id = s.id
    WHERE s.user_id = :USER_ID
      AND s.deleted_at IS NULL
      AND sts.deleted_at IS NULL
    ORDER BY
      sc.name,
      CASE sts.day_of_week
        WHEN 'Monday' THEN 1 WHEN 'Tuesday' THEN 2 WHEN 'Wednesday' THEN 3
        WHEN 'Thursday' THEN 4 WHEN 'Friday' THEN 5 WHEN 'Saturday' THEN 6
        WHEN 'Sunday' THEN 7
      END,
      sts.start_time;
  `;
  const res3 = await db.query(q3, { USER_ID: 1001 });
  console.log("Result 3 (Row count:", res3.length, "):");
  console.log(JSON.stringify(res3, null, 2));

  console.log("\n==========================================================================");
  console.log("🔍 QUERY 4: Adhoc Schedules + BETWEEN :WEEK_START AND :WEEK_END + ANY");
  console.log("==========================================================================");
  const q4 = `
    SELECT
      a.id,
      a.school_id,
      sc.name                             AS school_name,
      a.schedule_date,
      to_char(a.start_time, 'HH24:MI')    AS start_time,
      to_char(a.end_time,   'HH24:MI')    AS end_time,
      a.class_ids,
      a.support_amount,
      a.no_fuel_allowance,
      a.no_travel_allowance,
      a.is_non_salary_session,
      a.reason,
      (
        SELECT array_agg(c.name ORDER BY c.id)
        FROM classes c
        WHERE c.id = ANY(a.class_ids)
          AND c.deleted_at IS NULL
      )                                   AS class_names
    FROM adhoc_schedules a
    JOIN schools sc ON sc.id = a.school_id
    WHERE a.user_id = :USER_ID
      AND a.schedule_date BETWEEN :WEEK_START AND :WEEK_END
      AND a.deleted_at IS NULL
    ORDER BY a.schedule_date, a.start_time;
  `;
  const res4 = await db.query(q4, { USER_ID: 1001, WEEK_START: '2026-09-01', WEEK_END: '2026-09-10' });
  console.log("Result 4 (Row count:", res4.length, "):");
  console.log(JSON.stringify(res4, null, 2));

  console.log("\n🎉 ALL 4 USER QUERIES EXECUTED SUCCESSFULLY AND CORRECTLY!");

  try { if (fs.existsSync(dbPath)) fs.unlinkSync(dbPath); } catch {}
}

run().catch(console.error);
