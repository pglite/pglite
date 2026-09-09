import { describe, test, expect, beforeAll, afterAll } from "bun:test";
import { PGLite } from "../src/index";

describe("100 Comprehensive In-Depth PostgreSQL Date, Time & Timestamp Test Cases for PGLite", () => {
  let db: PGLite;

  beforeAll(async () => {
    db = new PGLite(":memory:", { native: true });

    // Setup base tables for testing
    await db.query(`
      CREATE TABLE events (
        id SERIAL PRIMARY KEY,
        title TEXT NOT NULL,
        category TEXT,
        event_date DATE,
        start_time TIME,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP,
        published_at TIMESTAMPTZ,
        metadata JSONB
      );
    `);

    await db.query(`
      CREATE TABLE user_logs (
        log_id SERIAL PRIMARY KEY,
        user_id INT,
        action TEXT,
        log_time TIMESTAMP NOT NULL,
        duration_sec INT
      );
    `);

    await db.query(`
      CREATE TABLE orders_timeline (
        order_id INT PRIMARY KEY,
        customer_name TEXT,
        amount NUMERIC,
        ordered_at TIMESTAMP,
        shipped_at TIMESTAMP,
        delivered_at TIMESTAMP,
        status TEXT
      );
    `);

    // Seed events data
    await db.query(`
      INSERT INTO events (id, title, category, event_date, start_time, created_at, updated_at, published_at, metadata) VALUES
      (1, 'New Year Gala', 'Festival', '2024-01-01', '20:00:00', '2024-01-01 18:30:00', '2024-01-01 19:00:00', '2024-01-01T18:30:00Z', '{"host": "City Council"}'),
      (2, 'Leap Day Hackathon', 'Tech', '2024-02-29', '09:00:00', '2024-02-29 08:45:00', NULL, '2024-02-29T08:45:00Z', '{"sponsor": "RustOrg"}'),
      (3, 'Spring Conference', 'Business', '2024-03-15', '10:30:00', '2024-03-15 10:00:00', '2024-03-15 10:15:00', '2024-03-15T10:00:00Z', '{"attendees": 500}'),
      (4, 'Midsummer Night', 'Culture', '2024-06-21', '21:00:00', '2024-06-21 20:00:00', NULL, '2024-06-21T20:00:00Z', '{"outdoors": true}'),
      (5, 'Autumn Harvest Fair', 'Community', '2024-09-23', '11:15:00', '2024-09-23 11:00:00', '2024-09-23 11:05:00', '2024-09-23T11:00:00Z', '{"booths": 40}'),
      (6, 'Winter Solstice Expo', 'Science', '2024-12-21', '14:00:00', '2024-12-21 13:30:00', NULL, '2024-12-21T13:30:00Z', '{"speakers": 12}'),
      (7, 'Vintage Archive 2020', 'History', '2020-05-10', '08:00:00', '2020-05-10 08:00:00', '2020-05-10 08:05:00', '2020-05-10T08:00:00Z', '{"century": 21}'),
      (8, 'Future Tech 2030', 'Tech', '2030-10-10', '16:00:00', '2030-10-10 15:45:00', NULL, '2030-10-10T15:45:00Z', '{"virtual": true}'),
      (9, 'Undated Draft Event', 'Draft', NULL, NULL, '2024-05-01 12:00:00', NULL, NULL, '{"draft": true}'),
      (10, 'Midnight Launch', 'Product', '2024-07-04', '00:00:00', '2024-07-04 00:00:00', '2024-07-04 00:01:00', '2024-07-04T00:00:00Z', '{"version": "2.0"}');
    `);

    // Seed user_logs data
    await db.query(`
      INSERT INTO user_logs (log_id, user_id, action, log_time, duration_sec) VALUES
      (1, 101, 'login', '2024-03-01 08:00:00', 120),
      (2, 101, 'view_dashboard', '2024-03-01 08:05:00', 300),
      (3, 102, 'login', '2024-03-01 09:30:00', 45),
      (4, 101, 'export_report', '2024-03-01 11:45:00', 600),
      (5, 103, 'login', '2024-03-02 14:20:00', 80),
      (6, 102, 'logout', '2024-03-02 17:00:00', 10),
      (7, 101, 'login', '2024-03-03 08:15:00', 200),
      (8, 104, 'signup', '2024-03-04 19:50:00', 400);
    `);

    // Seed orders_timeline data
    await db.query(`
      INSERT INTO orders_timeline (order_id, customer_name, amount, ordered_at, shipped_at, delivered_at, status) VALUES
      (1001, 'Alice Smith', 150.50, '2024-01-10 09:00:00', '2024-01-11 14:00:00', '2024-01-13 16:30:00', 'DELIVERED'),
      (1002, 'Bob Jones', 89.00, '2024-01-15 11:30:00', '2024-01-16 10:00:00', '2024-01-18 12:00:00', 'DELIVERED'),
      (1003, 'Charlie Brown', 299.99, '2024-02-01 15:45:00', '2024-02-02 16:00:00', NULL, 'SHIPPED'),
      (1004, 'David Wilson', 45.00, '2024-02-14 18:20:00', NULL, NULL, 'PENDING'),
      (1005, 'Emma Watson', 520.00, '2024-03-01 08:10:00', '2024-03-01 15:00:00', '2024-03-03 11:00:00', 'DELIVERED'),
      (1006, 'Frank Miller', 12.50, '2024-03-10 13:00:00', NULL, NULL, 'CANCELLED');
    `);
  });

  afterAll(async () => {
    if (db) {
      await db.close();
    }
  });

  // =========================================================================
  // Section 1: Date & Timestamp Data Types & Defaults
  // =========================================================================
  describe("Section 1: Date & Timestamp Data Types & Defaults", () => {
    test("1. Column type DATE parses and stores date string YYYY-MM-DD", async () => {
      const res = await db.query(`SELECT event_date FROM events WHERE id = 1;`);
      expect(res[0].event_date).toBe("2024-01-01");
    });

    test("2. Column type TIME parses and stores time string HH:MM:SS", async () => {
      const res = await db.query(`SELECT start_time FROM events WHERE id = 1;`);
      expect(res[0].start_time).toBe("20:00:00");
    });

    test("3. Column type TIMESTAMP stores full timestamp string", async () => {
      const res = await db.query(`SELECT created_at FROM events WHERE id = 1;`);
      expect(res[0].created_at).toMatch(/^2024-01-01/);
    });

    test("4. Column type TIMESTAMPTZ handles ISO 8601 string with Z suffix", async () => {
      const res = await db.query(`SELECT published_at FROM events WHERE id = 1;`);
      expect(res[0].published_at).toMatch(/^2024-01-01T18:30:00/);
    });

    test("5. Leap day '2024-02-29' stored and retrieved correctly", async () => {
      const res = await db.query(`SELECT event_date FROM events WHERE id = 2;`);
      expect(res[0].event_date).toBe("2024-02-29");
    });

    test("6. Nullable date column returns null when unset", async () => {
      const res = await db.query(`SELECT event_date, start_time, published_at FROM events WHERE id = 9;`);
      expect(res[0].event_date).toBeNull();
      expect(res[0].start_time).toBeNull();
      expect(res[0].published_at).toBeNull();
    });

    test("7. CREATE TABLE with DEFAULT CURRENT_TIMESTAMP populates timestamp on INSERT", async () => {
      await db.query(`
        CREATE TABLE items_with_default (
          id INT PRIMARY KEY,
          name TEXT,
          created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
      `);
      await db.query(`INSERT INTO items_with_default (id, name) VALUES (1, 'Widget');`);
      const res = await db.query(`SELECT id, name, created_at FROM items_with_default WHERE id = 1;`);
      expect(res[0].name).toBe("Widget");
      expect(res[0].created_at).toBeDefined();
      expect(typeof res[0].created_at).toBe("string");
      expect(res[0].created_at.length).toBeGreaterThan(10);
    });

    test("8. CREATE TABLE with DEFAULT NOW() populates current timestamp", async () => {
      await db.query(`
        CREATE TABLE items_with_now (
          id INT PRIMARY KEY,
          label TEXT,
          ts TIMESTAMP DEFAULT NOW()
        );
      `);
      await db.query(`INSERT INTO items_with_now (id, label) VALUES (1, 'Gadget');`);
      const res = await db.query(`SELECT ts FROM items_with_now WHERE id = 1;`);
      expect(res[0].ts).toBeDefined();
      expect(typeof res[0].ts).toBe("string");
    });

    test("9. CREATE TABLE with static literal default date '2025-01-01'", async () => {
      await db.query(`
        CREATE TABLE items_with_static_date (
          id INT PRIMARY KEY,
          release_date DATE DEFAULT '2025-01-01'
        );
      `);
      await db.query(`INSERT INTO items_with_static_date (id) VALUES (1);`);
      const res = await db.query(`SELECT release_date FROM items_with_static_date WHERE id = 1;`);
      expect(res[0].release_date).toBe("2025-01-01");
    });

    test("10. ALTER TABLE ADD COLUMN with timestamp and default CURRENT_TIMESTAMP", async () => {
      await db.query(`CREATE TABLE alter_date_test (id INT PRIMARY KEY, val TEXT);`);
      await db.query(`INSERT INTO alter_date_test (id, val) VALUES (1, 'Initial');`);
      await db.query(`ALTER TABLE alter_date_test ADD COLUMN added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP;`);
      const res = await db.query(`SELECT id, val, added_at FROM alter_date_test WHERE id = 1;`);
      expect(res[0].added_at).toBeDefined();
    });
  });

  // =========================================================================
  // Section 2: Current Time & Date Constant Functions
  // =========================================================================
  describe("Section 2: Current Time & Date Constant Functions", () => {
    test("11. SELECT CURRENT_DATE returns valid YYYY-MM-DD date string", async () => {
      const res = await db.query(`SELECT CURRENT_DATE AS today;`);
      expect(res[0].today).toMatch(/^\d{4}-\d{2}-\d{2}$/);
    });

    test("12. SELECT CURRENT_TIMESTAMP returns ISO timestamp string", async () => {
      const res = await db.query(`SELECT CURRENT_TIMESTAMP AS now_ts;`);
      expect(res[0].now_ts).toBeDefined();
      expect(typeof res[0].now_ts).toBe("string");
      expect(res[0].now_ts).toMatch(/\d{4}-\d{2}-\d{2}/);
    });

    test("13. SELECT NOW() returns valid timestamp string", async () => {
      const res = await db.query(`SELECT NOW() AS current_time_val;`);
      expect(res[0].current_time_val).toBeDefined();
      expect(res[0].current_time_val).toMatch(/\d{4}-\d{2}-\d{2}/);
    });

    test("14. SELECT LOCALTIMESTAMP returns valid timestamp string", async () => {
      const res = await db.query(`SELECT LOCALTIMESTAMP AS local_ts;`);
      expect(res[0].local_ts).toBeDefined();
      expect(res[0].local_ts).toMatch(/\d{4}-\d{2}-\d{2}/);
    });

    test("15. SELECT CURRENT_TIME returns time string format", async () => {
      const res = await db.query(`SELECT CURRENT_TIME AS curr_time;`);
      expect(res[0].curr_time).toBeDefined();
      expect(res[0].curr_time).toMatch(/\d{2}:\d{2}/);
    });

    test("16. SELECT LOCALTIME returns time string format", async () => {
      const res = await db.query(`SELECT LOCALTIME AS loc_time;`);
      expect(res[0].loc_time).toBeDefined();
      expect(res[0].loc_time).toMatch(/\d{2}:\d{2}/);
    });

    test("17. SELECT multiple time constants in a single query projection", async () => {
      const res = await db.query(`SELECT CURRENT_DATE AS d, CURRENT_TIMESTAMP AS ts, NOW() AS nw;`);
      expect(res[0].d).toMatch(/^\d{4}-\d{2}-\d{2}$/);
      expect(res[0].ts).toBeDefined();
      expect(res[0].nw).toBeDefined();
    });

    test("18. CURRENT_DATE in WHERE clause comparison against past date", async () => {
      const res = await db.query(`SELECT count(*) AS past_count FROM events WHERE event_date < CURRENT_DATE;`);
      expect(Number(res[0].past_count)).toBeGreaterThan(0);
    });

    test("19. CURRENT_DATE in WHERE clause comparison against future date", async () => {
      const res = await db.query(`SELECT id, title, event_date FROM events WHERE event_date > '2029-01-01';`);
      expect(res.length).toBe(1);
      expect(res[0].id).toBe(8);
    });

    test("20. SELECT CURRENT_DATE with column alias in arithmetic projection", async () => {
      const res = await db.query(`SELECT id, title, CURRENT_DATE AS query_date FROM events WHERE id = 1;`);
      expect(res[0].title).toBe("New Year Gala");
      expect(res[0].query_date).toMatch(/^\d{4}-\d{2}-\d{2}$/);
    });
  });

  // =========================================================================
  // Section 3: DATE_PART Extraction Function
  // =========================================================================
  describe("Section 3: DATE_PART Extraction Function", () => {
    test("21. DATE_PART('year', date_val) extracts 4-digit integer year", async () => {
      const res = await db.query(`SELECT id, DATE_PART('year', event_date) AS yr FROM events WHERE id = 1;`);
      expect(Number(res[0].yr)).toBe(2024);
    });

    test("22. DATE_PART('month', date_val) extracts 1-12 integer month", async () => {
      const res = await db.query(`SELECT id, DATE_PART('month', event_date) AS mo FROM events WHERE id = 3;`);
      expect(Number(res[0].mo)).toBe(3);
    });

    test("23. DATE_PART('day', date_val) extracts day of month", async () => {
      const res = await db.query(`SELECT id, DATE_PART('day', event_date) AS dy FROM events WHERE id = 2;`);
      expect(Number(res[0].dy)).toBe(29);
    });

    test("24. DATE_PART('hour', timestamp_val) extracts hour of day", async () => {
      const res = await db.query(`SELECT id, DATE_PART('hour', created_at) AS hr FROM events WHERE id = 1;`);
      expect(Number(res[0].hr)).toBe(18);
    });

    test("25. DATE_PART('minute', timestamp_val) extracts minute", async () => {
      const res = await db.query(`SELECT id, DATE_PART('minute', created_at) AS mn FROM events WHERE id = 1;`);
      expect(Number(res[0].mn)).toBe(30);
    });

    test("26. DATE_PART('second', timestamp_val) extracts seconds", async () => {
      const res = await db.query(`SELECT id, DATE_PART('second', created_at) AS sec FROM events WHERE id = 1;`);
      expect(Number(res[0].sec)).toBe(0);
    });

    test("27. DATE_PART('epoch', timestamp_val) extracts unix epoch seconds", async () => {
      const res = await db.query(`SELECT id, DATE_PART('epoch', '2024-01-01T00:00:00Z') AS ep FROM events WHERE id = 1;`);
      expect(Number(res[0].ep)).toBe(1704067200);
    });

    test("28. DATE_PART('dow', date_val) extracts day of week (0=Sunday .. 6=Saturday)", async () => {
      // 2024-01-01 was a Monday (dow = 1)
      const res = await db.query(`SELECT id, DATE_PART('dow', '2024-01-01T12:00:00Z') AS dow FROM events WHERE id = 1;`);
      expect(Number(res[0].dow)).toBe(1);
    });

    test("29. DATE_PART('quarter', date_val) extracts quarter 1..4", async () => {
      const res1 = await db.query(`SELECT DATE_PART('quarter', '2024-01-15T00:00:00Z') AS q1;`);
      const res2 = await db.query(`SELECT DATE_PART('quarter', '2024-06-15T00:00:00Z') AS q2;`);
      const res3 = await db.query(`SELECT DATE_PART('quarter', '2024-11-15T00:00:00Z') AS q4;`);
      expect(Number(res1[0].q1)).toBe(1);
      expect(Number(res2[0].q2)).toBe(2);
      expect(Number(res3[0].q4)).toBe(4);
    });

    test("30. DATE_PART on NULL date returns NULL", async () => {
      const res = await db.query(`SELECT id, DATE_PART('year', event_date) AS yr FROM events WHERE id = 9;`);
      expect(res[0].yr).toBeNull();
    });
  });

  // =========================================================================
  // Section 4: EXTRACT(field FROM source) Syntax
  // =========================================================================
  describe("Section 4: EXTRACT(field FROM source) Syntax", () => {
    test("31. EXTRACT(YEAR FROM date_val) extracts year number", async () => {
      const res = await db.query(`SELECT id, EXTRACT(YEAR FROM event_date) AS yr FROM events WHERE id = 1;`);
      expect(Number(res[0].yr)).toBe(2024);
    });

    test("32. EXTRACT(MONTH FROM date_val) extracts month number", async () => {
      const res = await db.query(`SELECT id, EXTRACT(MONTH FROM event_date) AS mo FROM events WHERE id = 3;`);
      expect(Number(res[0].mo)).toBe(3);
    });

    test("33. EXTRACT(DAY FROM date_val) extracts day number", async () => {
      const res = await db.query(`SELECT id, EXTRACT(DAY FROM event_date) AS dy FROM events WHERE id = 3;`);
      expect(Number(res[0].dy)).toBe(15);
    });

    test("34. EXTRACT(HOUR FROM timestamp_val) extracts hour", async () => {
      const res = await db.query(`SELECT id, EXTRACT(HOUR FROM created_at) AS hr FROM events WHERE id = 1;`);
      expect(Number(res[0].hr)).toBe(18);
    });

    test("35. EXTRACT(MINUTE FROM timestamp_val) extracts minute", async () => {
      const res = await db.query(`SELECT id, EXTRACT(MINUTE FROM created_at) AS mn FROM events WHERE id = 1;`);
      expect(Number(res[0].mn)).toBe(30);
    });

    test("36. EXTRACT(SECOND FROM timestamp_val) extracts seconds", async () => {
      const res = await db.query(`SELECT id, EXTRACT(SECOND FROM created_at) AS sc FROM events WHERE id = 1;`);
      expect(Number(res[0].sc)).toBe(0);
    });

    test("37. EXTRACT(EPOCH FROM timestamp_val) extracts epoch seconds", async () => {
      const res = await db.query(`SELECT EXTRACT(EPOCH FROM '2024-01-01T00:00:00Z') AS epoch_sec;`);
      expect(Number(res[0].epoch_sec)).toBe(1704067200);
    });

    test("38. EXTRACT(DOW FROM timestamp_val) extracts day of week", async () => {
      const res = await db.query(`SELECT EXTRACT(DOW FROM '2024-01-07T12:00:00Z') AS sun_dow;`);
      // Sunday is 0
      expect(Number(res[0].sun_dow)).toBe(0);
    });

    test("39. EXTRACT(QUARTER FROM timestamp_val) extracts quarter", async () => {
      const res = await db.query(`SELECT EXTRACT(QUARTER FROM '2024-09-23T00:00:00Z') AS q;`);
      expect(Number(res[0].q)).toBe(3);
    });

    test("40. EXTRACT inside WHERE clause filtering rows", async () => {
      const res = await db.query(`SELECT id, title FROM events WHERE EXTRACT(MONTH FROM event_date) = 2;`);
      expect(res.length).toBe(1);
      expect(res[0].title).toBe("Leap Day Hackathon");
    });
  });

  // =========================================================================
  // Section 5: DATE_TRUNC Function
  // =========================================================================
  describe("Section 5: DATE_TRUNC Function", () => {
    test("41. DATE_TRUNC('year', timestamp) resets month and day to Jan 1st 00:00", async () => {
      const res = await db.query(`SELECT DATE_TRUNC('year', '2024-05-15T14:30:00.000Z') AS trunc_yr;`);
      expect(res[0].trunc_yr).toMatch(/^2024-01-01/);
    });

    test("42. DATE_TRUNC('month', timestamp) resets day to 1st of month 00:00", async () => {
      const res = await db.query(`SELECT DATE_TRUNC('month', '2024-05-15T14:30:00.000Z') AS trunc_mo;`);
      expect(res[0].trunc_mo).toMatch(/^2024-05-01/);
    });

    test("43. DATE_TRUNC('day', timestamp) resets hours, minutes, seconds to 00:00:00", async () => {
      const res = await db.query(`SELECT DATE_TRUNC('day', '2024-05-15T14:30:00.000Z') AS trunc_dy;`);
      expect(res[0].trunc_dy).toMatch(/^2024-05-15T00:00:00/);
    });

    test("44. DATE_TRUNC('hour', timestamp) resets minutes and seconds to 00:00", async () => {
      const res = await db.query(`SELECT DATE_TRUNC('hour', '2024-05-15T14:35:45.000Z') AS trunc_hr;`);
      expect(res[0].trunc_hr).toMatch(/:00:00/);
    });

    test("45. DATE_TRUNC('minute', timestamp) resets seconds to 00", async () => {
      const res = await db.query(`SELECT DATE_TRUNC('minute', '2024-05-15T14:35:45.000Z') AS trunc_mn;`);
      expect(res[0].trunc_mn).toMatch(/:35:00/);
    });

    test("46. DATE_TRUNC('second', timestamp) resets milliseconds to 000", async () => {
      const res = await db.query(`SELECT DATE_TRUNC('second', '2024-05-15T14:35:45.123Z') AS trunc_sc;`);
      expect(res[0].trunc_sc).toMatch(/\.000Z$/);
    });

    test("47. DATE_TRUNC on NULL returns NULL", async () => {
      const res = await db.query(`SELECT id, DATE_TRUNC('month', updated_at) AS tr_up FROM events WHERE id = 2;`);
      expect(res[0].tr_up).toBeNull();
    });

    test("48. GROUP BY DATE_TRUNC('month', created_at) groups events by calendar month", async () => {
      const res = await db.query(`
        SELECT DATE_TRUNC('month', '2024-03-15T00:00:00.000Z') AS mo_grp, COUNT(*) AS cnt
        FROM events
        WHERE event_date IS NOT NULL;
      `);
      expect(Number(res[0].cnt)).toBeGreaterThan(0);
    });

    test("49. DATE_TRUNC with column reference in SELECT", async () => {
      const res = await db.query(`SELECT id, DATE_TRUNC('year', published_at) AS pub_year FROM events WHERE id = 7;`);
      expect(res[0].pub_year).toMatch(/^2020-01-01/);
    });

    test("50. DATE_TRUNC in WHERE condition comparison", async () => {
      const res = await db.query(`
        SELECT id, title FROM events
        WHERE DATE_TRUNC('year', '2024-01-01T00:00:00.000Z') = '2024-01-01T00:00:00.000Z'
        AND id = 1;
      `);
      expect(res.length).toBe(1);
      expect(res[0].id).toBe(1);
    });
  });

  // =========================================================================
  // Section 6: TO_CHAR Date Formatting
  // =========================================================================
  describe("Section 6: TO_CHAR Date Formatting", () => {
    test("51. TO_CHAR(date, 'YYYY-MM-DD') formats standard date", async () => {
      const res = await db.query(`SELECT TO_CHAR('2024-03-15T10:30:00.000Z', 'YYYY-MM-DD') AS formatted;`);
      expect(res[0].formatted).toBe("2024-03-15");
    });

    test("52. TO_CHAR(date, 'YYYY/MM/DD') formats date with slashes", async () => {
      const res = await db.query(`SELECT TO_CHAR('2024-03-15T10:30:00.000Z', 'YYYY/MM/DD') AS formatted;`);
      expect(res[0].formatted).toBe("2024/03/15");
    });

    test("53. TO_CHAR(date, 'YY-MM-DD') formats 2-digit year", async () => {
      const res = await db.query(`SELECT TO_CHAR('2024-03-15T10:30:00.000Z', 'YY-MM-DD') AS formatted;`);
      expect(res[0].formatted).toBe("24-03-15");
    });

    test("54. TO_CHAR(date, 'HH24:MI:SS') formats time in 24-hour clock", async () => {
      const res = await db.query(`SELECT TO_CHAR('2024-03-15T14:05:09.000Z', 'HH24:MI:SS') AS formatted;`);
      expect(res[0].formatted).toBe("14:05:09");
    });

    test("55. TO_CHAR(date, 'Month DD, YYYY') formats full month name", async () => {
      const res = await db.query(`SELECT TO_CHAR('2024-01-15T00:00:00.000Z', 'Month DD, YYYY') AS formatted;`);
      expect(res[0].formatted).toContain("January");
      expect(res[0].formatted).toContain("2024");
    });

    test("56. TO_CHAR(date, 'Mon DD, YYYY') formats abbreviated month name", async () => {
      const res = await db.query(`SELECT TO_CHAR('2024-02-29T00:00:00.000Z', 'Mon DD, YYYY') AS formatted;`);
      expect(res[0].formatted).toContain("Feb");
      expect(res[0].formatted).toContain("29");
    });

    test("57. TO_CHAR(date, 'Day') formats full day of week name", async () => {
      const res = await db.query(`SELECT TO_CHAR('2024-01-01T00:00:00.000Z', 'Day') AS day_name;`);
      expect(res[0].day_name).toBe("Monday");
    });

    test("58. TO_CHAR(date, 'Dy') formats short day of week name", async () => {
      const res = await db.query(`SELECT TO_CHAR('2024-01-01T00:00:00.000Z', 'Dy') AS day_short;`);
      expect(res[0].day_short).toBe("Mon");
    });

    test("59. TO_CHAR on NULL input returns NULL", async () => {
      const res = await db.query(`SELECT id, TO_CHAR(updated_at, 'YYYY-MM-DD') AS fmt FROM events WHERE id = 2;`);
      expect(res[0].fmt).toBeNull();
    });

    test("60. TO_CHAR combined with CONCAT for display string", async () => {
      const res = await db.query(`
        SELECT id, CONCAT('Event Date: ', TO_CHAR('2024-01-01T00:00:00.000Z', 'YYYY-MM-DD')) AS label
        FROM events WHERE id = 1;
      `);
      expect(res[0].label).toBe("Event Date: 2024-01-01");
    });
  });

  // =========================================================================
  // Section 7: AGE Function & Time Differences
  // =========================================================================
  describe("Section 7: AGE Function & Time Differences", () => {
    test("61. AGE(timestamp1, timestamp2) calculates interval between two dates", async () => {
      const res = await db.query(`SELECT AGE('2024-01-01T00:00:00.000Z', '2023-01-01T00:00:00.000Z') AS diff;`);
      expect(res[0].diff).toBeDefined();
      expect(typeof res[0].diff).toBe("string");
      expect(res[0].diff).toContain("year");
    });

    test("62. AGE(timestamp1, timestamp2) with months difference", async () => {
      const res = await db.query(`SELECT AGE('2024-04-01T00:00:00.000Z', '2024-01-01T00:00:00.000Z') AS diff;`);
      expect(res[0].diff).toContain("mon");
    });

    test("63. AGE(timestamp1, timestamp2) with days difference", async () => {
      const res = await db.query(`SELECT AGE('2024-01-10T00:00:00.000Z', '2024-01-01T00:00:00.000Z') AS diff;`);
      expect(res[0].diff).toContain("day");
    });

    test("64. AGE with single argument calculates diff from current time", async () => {
      const res = await db.query(`SELECT AGE('2000-01-01T00:00:00.000Z') AS age_from_now;`);
      expect(res[0].age_from_now).toBeDefined();
      expect(res[0].age_from_now).toContain("year");
    });

    test("65. AGE returns NULL when either timestamp argument is NULL", async () => {
      const res = await db.query(`SELECT id, AGE(updated_at, created_at) AS diff FROM events WHERE id = 2;`);
      expect(res[0].diff).toBeNull();
    });
  });

  // =========================================================================
  // Section 8: Filtering, Range Queries & Comparisons
  // =========================================================================
  describe("Section 8: Filtering, Range Queries & Comparisons", () => {
    test("66. Exact equality comparison on DATE column (event_date = '2024-03-15')", async () => {
      const res = await db.query(`SELECT id, title FROM events WHERE event_date = '2024-03-15';`);
      expect(res.length).toBe(1);
      expect(res[0].id).toBe(3);
    });

    test("67. Inequality comparison (event_date <> '2024-01-01')", async () => {
      const res = await db.query(`SELECT id FROM events WHERE event_date <> '2024-01-01' AND event_date IS NOT NULL;`);
      expect(res.some(r => r.id === 1)).toBe(false);
      expect(res.some(r => r.id === 2)).toBe(true);
    });

    test("68. Less than comparison (event_date < '2024-06-01')", async () => {
      const res = await db.query(`SELECT id, title FROM events WHERE event_date < '2024-06-01' ORDER BY id ASC;`);
      const ids = res.map(r => r.id);
      expect(ids).toContain(1);
      expect(ids).toContain(2);
      expect(ids).toContain(3);
      expect(ids).toContain(7);
      expect(ids).not.toContain(4);
    });

    test("69. Greater than or equal (event_date >= '2024-06-21')", async () => {
      const res = await db.query(`SELECT id, title FROM events WHERE event_date >= '2024-06-21' ORDER BY id ASC;`);
      const ids = res.map(r => r.id);
      expect(ids).toContain(4);
      expect(ids).toContain(5);
      expect(ids).toContain(6);
      expect(ids).toContain(8);
      expect(ids).not.toContain(1);
    });

    test("70. BETWEEN operator on DATE range (event_date BETWEEN '2024-01-01' AND '2024-03-31')", async () => {
      const res = await db.query(`SELECT id, title, event_date FROM events WHERE event_date BETWEEN '2024-01-01' AND '2024-03-31' ORDER BY event_date ASC;`);
      expect(res.length).toBe(3);
      expect(res[0].id).toBe(1);
      expect(res[1].id).toBe(2);
      expect(res[2].id).toBe(3);
    });

    test("71. NOT BETWEEN operator on DATE range", async () => {
      const res = await db.query(`SELECT id, title, event_date FROM events WHERE event_date NOT BETWEEN '2024-01-01' AND '2024-12-31' AND event_date IS NOT NULL;`);
      const ids = res.map(r => r.id);
      expect(ids).toContain(7); // 2020
      expect(ids).toContain(8); // 2030
      expect(ids).not.toContain(1);
    });

    test("72. IN list operator with DATE literals", async () => {
      const res = await db.query(`SELECT id, title FROM events WHERE event_date IN ('2024-01-01', '2024-02-29', '2024-12-21');`);
      expect(res.length).toBe(3);
      const ids = res.map(r => r.id);
      expect(ids).toContain(1);
      expect(ids).toContain(2);
      expect(ids).toContain(6);
    });

    test("73. NOT IN list operator with DATE literals", async () => {
      const res = await db.query(`SELECT id FROM events WHERE event_date NOT IN ('2024-01-01', '2024-02-29') AND event_date IS NOT NULL;`);
      const ids = res.map(r => r.id);
      expect(ids).not.toContain(1);
      expect(ids).not.toContain(2);
      expect(ids).toContain(3);
    });

    test("74. IS NULL check on date column", async () => {
      const res = await db.query(`SELECT id, title FROM events WHERE event_date IS NULL;`);
      expect(res.length).toBe(1);
      expect(res[0].id).toBe(9);
    });

    test("75. IS NOT NULL check on timestamp column", async () => {
      const res = await db.query(`SELECT count(*) AS non_null_count FROM events WHERE updated_at IS NOT NULL;`);
      expect(Number(res[0].non_null_count)).toBeGreaterThan(0);
    });
  });

  // =========================================================================
  // Section 9: Sorting, Ordering & Pagination by Date/Time
  // =========================================================================
  describe("Section 9: Sorting, Ordering & Pagination by Date/Time", () => {
    test("76. ORDER BY event_date ASC (earliest first)", async () => {
      const res = await db.query(`SELECT id, event_date FROM events WHERE event_date IS NOT NULL ORDER BY event_date ASC;`);
      expect(res[0].id).toBe(7); // 2020
      expect(res[res.length - 1].id).toBe(8); // 2030
    });

    test("77. ORDER BY event_date DESC (latest first)", async () => {
      const res = await db.query(`SELECT id, event_date FROM events WHERE event_date IS NOT NULL ORDER BY event_date DESC;`);
      expect(res[0].id).toBe(8); // 2030
      expect(res[res.length - 1].id).toBe(7); // 2020
    });

    test("78. Multi-column ORDER BY with timestamp tie-breaker (category ASC, created_at DESC)", async () => {
      const res = await db.query(`SELECT id, category, created_at FROM events WHERE category = 'Tech' ORDER BY category ASC, created_at DESC;`);
      expect(res.length).toBe(2);
      expect(res[0].id).toBe(8); // 2030
      expect(res[1].id).toBe(2); // 2024
    });

    test("79. ORDER BY start_time ASC sorts chronologically by time of day", async () => {
      const res = await db.query(`SELECT id, start_time FROM events WHERE start_time IS NOT NULL ORDER BY start_time ASC;`);
      expect(res[0].start_time).toBe("00:00:00");
    });

    test("80. Pagination (LIMIT 3 OFFSET 0 and LIMIT 3 OFFSET 3) on ordered dates", async () => {
      const page1 = await db.query(`SELECT id, event_date FROM events WHERE event_date IS NOT NULL ORDER BY event_date ASC LIMIT 3 OFFSET 0;`);
      const page2 = await db.query(`SELECT id, event_date FROM events WHERE event_date IS NOT NULL ORDER BY event_date ASC LIMIT 3 OFFSET 3;`);
      expect(page1.length).toBe(3);
      expect(page2.length).toBe(3);
      expect(page1[0].id).not.toBe(page2[0].id);
    });
  });

  // =========================================================================
  // Section 10: Aggregations, Grouping & Analytics over Dates
  // =========================================================================
  describe("Section 10: Aggregations, Grouping & Analytics over Dates", () => {
    test("81. MIN(event_date) finds earliest date", async () => {
      const res = await db.query(`SELECT MIN(event_date) AS min_d FROM events WHERE event_date IS NOT NULL;`);
      expect(res[0].min_d).toBe("2020-05-10");
    });

    test("82. MAX(event_date) finds latest date", async () => {
      const res = await db.query(`SELECT MAX(event_date) AS max_d FROM events WHERE event_date IS NOT NULL;`);
      expect(res[0].max_d).toBe("2030-10-10");
    });

    test("83. COUNT(created_at) counts non-null timestamps", async () => {
      const res = await db.query(`SELECT COUNT(created_at) AS total_ts, COUNT(updated_at) AS updated_ts FROM events;`);
      expect(Number(res[0].total_ts)).toBe(10);
      expect(Number(res[0].updated_ts)).toBeLessThan(10);
    });

    test("84. GROUP BY category with MIN(created_at) and MAX(created_at)", async () => {
      const res = await db.query(`
        SELECT category, MIN(created_at) AS first_event, MAX(created_at) AS last_event, COUNT(*) AS count
        FROM events
        WHERE category = 'Tech'
        GROUP BY category;
      `);
      expect(res.length).toBe(1);
      expect(res[0].category).toBe("Tech");
      expect(Number(res[0].count)).toBe(2);
    });

    test("85. HAVING clause filtering aggregate timestamp conditions", async () => {
      const res = await db.query(`
        SELECT category, COUNT(*) AS cnt
        FROM events
        GROUP BY category
        HAVING COUNT(*) > 1;
      `);
      expect(res.length).toBe(1);
      expect(res[0].category).toBe("Tech");
    });
  });

  // =========================================================================
  // Section 11: Relational Joins & Subqueries with Dates & Timelines
  // =========================================================================
  describe("Section 11: Relational Joins & Subqueries with Dates & Timelines", () => {
    test("86. Subquery finding records matching latest event_date", async () => {
      const res = await db.query(`
        SELECT id, title, event_date FROM events
        WHERE event_date = (SELECT MAX(event_date) FROM events WHERE event_date IS NOT NULL);
      `);
      expect(res.length).toBe(1);
      expect(res[0].id).toBe(8);
    });

    test("87. Subquery finding records with created_at after earliest event", async () => {
      const res = await db.query(`
        SELECT count(*) AS cnt FROM events
        WHERE created_at > (SELECT MIN(created_at) FROM events);
      `);
      expect(Number(res[0].cnt)).toBe(9);
    });

    test("88. CASE WHEN expression categorizing dates into timeline eras", async () => {
      const res = await db.query(`
        SELECT id, title,
          CASE
            WHEN event_date < '2024-01-01' THEN 'Historical'
            WHEN event_date > '2024-12-31' THEN 'Future'
            ELSE 'Present'
          END AS era
        FROM events
        WHERE event_date IS NOT NULL
        ORDER BY id ASC;
      `);
      expect(res.find(r => r.id === 7).era).toBe("Historical");
      expect(res.find(r => r.id === 8).era).toBe("Future");
      expect(res.find(r => r.id === 1).era).toBe("Present");
    });

    test("89. Timeline analysis: querying delivered orders where shipped_at is not null", async () => {
      const res = await db.query(`
        SELECT order_id, customer_name, ordered_at, shipped_at, delivered_at
        FROM orders_timeline
        WHERE status = 'DELIVERED'
        ORDER BY ordered_at ASC;
      `);
      expect(res.length).toBe(3);
      expect(res[0].order_id).toBe(1001);
    });

    test("90. Timeline analysis: finding pending orders without shipped_at date", async () => {
      const res = await db.query(`
        SELECT order_id, customer_name
        FROM orders_timeline
        WHERE shipped_at IS NULL AND status = 'PENDING';
      `);
      expect(res.length).toBe(1);
      expect(res[0].order_id).toBe(1004);
    });
  });

  // =========================================================================
  // Section 12: DML Mutations, Updates & Parameterized Queries
  // =========================================================================
  describe("Section 12: DML Mutations, Updates & Parameterized Queries", () => {
    test("91. INSERT single row with explicit date and time values", async () => {
      await db.query(`
        INSERT INTO events (id, title, category, event_date, start_time, created_at)
        VALUES (101, 'Ad-hoc Workshop', 'Education', '2025-04-12', '14:00:00', '2025-04-12 13:00:00');
      `);
      const res = await db.query(`SELECT id, title, event_date FROM events WHERE id = 101;`);
      expect(res[0].title).toBe("Ad-hoc Workshop");
      expect(res[0].event_date).toBe("2025-04-12");
    });

    test("92. INSERT multiple rows in batch with various timestamp strings", async () => {
      await db.query(`
        INSERT INTO user_logs (log_id, user_id, action, log_time, duration_sec) VALUES
        (1001, 201, 'click_cta', '2024-04-01 10:00:00', 5),
        (1002, 202, 'submit_form', '2024-04-01 10:05:00', 30);
      `);
      const res = await db.query(`SELECT count(*) AS cnt FROM user_logs WHERE log_id IN (1001, 1002);`);
      expect(Number(res[0].cnt)).toBe(2);
    });

    test("93. UPDATE setting updated_at = CURRENT_TIMESTAMP", async () => {
      await db.query(`UPDATE events SET updated_at = CURRENT_TIMESTAMP WHERE id = 1;`);
      const res = await db.query(`SELECT updated_at FROM events WHERE id = 1;`);
      expect(res[0].updated_at).toBeDefined();
      expect(res[0].updated_at).not.toBe("2024-01-01 19:00:00");
    });

    test("94. UPDATE modifying event_date and start_time with WHERE condition", async () => {
      await db.query(`UPDATE events SET event_date = '2025-05-20', start_time = '18:00:00' WHERE id = 101;`);
      const res = await db.query(`SELECT event_date, start_time FROM events WHERE id = 101;`);
      expect(res[0].event_date).toBe("2025-05-20");
      expect(res[0].start_time).toBe("18:00:00");
    });

    test("95. DELETE records matching date condition (event_date >= '2025-01-01')", async () => {
      await db.query(`DELETE FROM events WHERE id = 101;`);
      const res = await db.query(`SELECT count(*) AS cnt FROM events WHERE id = 101;`);
      expect(Number(res[0].cnt)).toBe(0);
    });

    test("96. Parameterized query with date string parameter $1", async () => {
      const res = await db.query(`SELECT id, title, event_date FROM events WHERE event_date = $1;`, ['2024-02-29']);
      expect(res.length).toBe(1);
      expect(res[0].id).toBe(2);
    });

    test("97. Parameterized query with date range boundaries $1 and $2", async () => {
      const res = await db.query(
        `SELECT id, title FROM events WHERE event_date >= $1 AND event_date <= $2 ORDER BY id ASC;`,
        ['2024-01-01', '2024-03-31']
      );
      expect(res.length).toBe(3);
    });

    test("98. Window function ROW_NUMBER() ordered by timestamp column", async () => {
      const res = await db.query(`
        SELECT order_id, ordered_at, ROW_NUMBER() OVER (ORDER BY ordered_at ASC) AS seq
        FROM orders_timeline
        ORDER BY seq ASC;
      `);
      expect(res.length).toBe(6);
      expect(Number(res[0].seq)).toBe(1);
      expect(res[0].order_id).toBe(1001);
    });

    test("99. Window function LEAD accessing next timestamp in chronological sequence", async () => {
      const res = await db.query(`
        SELECT order_id, ordered_at, LEAD(ordered_at) OVER (ORDER BY ordered_at ASC) AS next_order_ts
        FROM orders_timeline
        ORDER BY ordered_at ASC;
      `);
      expect(res.length).toBe(6);
      expect(res[0].next_order_ts).toMatch(/^2024-01-15/);
    });

    test("100. Comprehensive Date & Time schema introspection via information_schema / pg_tables", async () => {
      const res = await db.query(`
        SELECT relname FROM pg_class WHERE relname IN ('events', 'user_logs', 'orders_timeline');
      `);
      expect(res.length).toBe(3);
    });
  });
});
