import { describe, test, expect, beforeAll, afterAll } from "bun:test";
import { PGLite } from "../src/index";

describe("100 Comprehensive In-Depth PostgreSQL BOOLEAN & 3VL Test Cases for PGLite", () => {
  let db: PGLite;

  beforeAll(async () => {
    db = new PGLite(":memory:", { native: true });

    // Base user accounts table with boolean flags
    await db.query(`
      CREATE TABLE user_profiles (
        user_id SERIAL PRIMARY KEY,
        username TEXT NOT NULL,
        department TEXT NOT NULL,
        is_active BOOLEAN DEFAULT true,
        is_admin BOOL DEFAULT false,
        is_verified BOOLEAN,
        two_factor_enabled BOOL DEFAULT false,
        permission_flags BOOL[]
      );
    `);

    // Task items table for three-valued logic and status checks
    await db.query(`
      CREATE TABLE task_items (
        task_id SERIAL PRIMARY KEY,
        title TEXT NOT NULL,
        project_id INT,
        is_completed BOOLEAN,
        is_priority BOOLEAN DEFAULT false,
        is_blocked BOOLEAN DEFAULT false
      );
    `);

    // Seed user_profiles
    await db.query(`
      INSERT INTO user_profiles (user_id, username, department, is_active, is_admin, is_verified, two_factor_enabled, permission_flags) VALUES
      (1, 'alice', 'engineering', true, true, true, true, '{true, true, false}'),
      (2, 'bob', 'engineering', true, false, true, false, '{true, false, false}'),
      (3, 'charlie', 'sales', true, false, false, false, '{false, false, false}'),
      (4, 'david', 'sales', false, false, false, false, '{false, false, false}'),
      (5, 'eve', 'marketing', true, false, NULL, true, '{true, true, true}'),
      (6, 'frank', 'marketing', false, false, NULL, false, NULL),
      (7, 'grace', 'engineering', true, false, true, true, '{true, false, true}'),
      (8, 'heidi', 'hr', NULL, false, NULL, false, NULL);
    `);

    // Seed task_items
    await db.query(`
      INSERT INTO task_items (task_id, title, project_id, is_completed, is_priority, is_blocked) VALUES
      (1, 'Setup CI/CD', 101, true, true, false),
      (2, 'Database Migration', 101, true, true, false),
      (3, 'API Authentication', 101, false, true, true),
      (4, 'Landing Page Copy', 102, false, false, false),
      (5, 'User Interviews', 102, NULL, false, false),
      (6, 'Quarterly Review', 103, NULL, true, NULL);
    `);
  });

  afterAll(async () => {
    if (db) {
      await db.close();
    }
  });

  // =========================================================================
  // Section 1: Boolean Literals, Types, Inputs & Constants (Tests 1 - 15)
  // =========================================================================
  describe("Section 1: Boolean Literals, Types, Inputs & Constants", () => {
    test("1. TRUE literal in SELECT projection", async () => {
      const res = await db.query(`SELECT TRUE AS val;`);
      expect(res[0].val).toBe(true);
    });

    test("2. FALSE literal in SELECT projection", async () => {
      const res = await db.query(`SELECT FALSE AS val;`);
      expect(res[0].val).toBe(false);
    });

    test("3. Lowercase true and false literals", async () => {
      const res = await db.query(`SELECT true AS t, false AS f;`);
      expect(res[0].t).toBe(true);
      expect(res[0].f).toBe(false);
    });

    test("4. Cast string 'true' to boolean 'true'::boolean", async () => {
      const res = await db.query(`SELECT 'true'::boolean AS b_val;`);
      expect(res[0].b_val).toBe(true);
    });

    test("5. Cast string 'false' to boolean 'false'::boolean", async () => {
      const res = await db.query(`SELECT 'false'::boolean AS b_val;`);
      expect(res[0].b_val).toBe(false);
    });

    test("6. Cast string 't' and 'f' to boolean", async () => {
      const res = await db.query(`SELECT 't'::bool AS b_t, 'f'::bool AS b_f;`);
      expect(res[0].b_t).toBe(true);
      expect(res[0].b_f).toBe(false);
    });

    test("7. Cast string 'yes' and 'no' to boolean", async () => {
      const res = await db.query(`SELECT 'yes'::bool AS b_yes, 'no'::bool AS b_no;`);
      expect(res[0].b_yes).toBe(true);
      expect(res[0].b_no).toBe(false);
    });

    test("8. Cast string '1' and '0' to boolean", async () => {
      const res = await db.query(`SELECT '1'::bool AS b_one, '0'::bool AS b_zero;`);
      expect(res[0].b_one).toBe(true);
      expect(res[0].b_zero).toBe(false);
    });

    test("9. Table column with default boolean value TRUE", async () => {
      const res = await db.query(`SELECT is_active FROM user_profiles WHERE username = 'alice';`);
      expect(res[0].is_active).toBe(true);
    });

    test("10. Table column with default boolean value FALSE", async () => {
      const res = await db.query(`SELECT is_admin FROM user_profiles WHERE username = 'bob';`);
      expect(res[0].is_admin).toBe(false);
    });

    test("11. Nullable boolean column stores and retrieves NULL", async () => {
      const res = await db.query(`SELECT is_verified FROM user_profiles WHERE username = 'eve';`);
      expect(res[0].is_verified).toBeNull();
    });

    test("12. Cast boolean to integer: true::int and false::int", async () => {
      const res = await db.query(`SELECT true::int AS int_t, false::int AS int_f;`);
      expect(Number(res[0].int_t)).toBe(1);
      expect(Number(res[0].int_f)).toBe(0);
    });

    test("13. Boolean array BOOL[] stores series of boolean elements", async () => {
      const res = await db.query(`SELECT permission_flags FROM user_profiles WHERE username = 'alice';`);
      expect(res[0].permission_flags).toEqual([true, true, false]);
    });

    test("14. Boolean array indexing permission_flags[1]", async () => {
      const res = await db.query(`SELECT permission_flags[1] AS p1, permission_flags[3] AS p3 FROM user_profiles WHERE username = 'alice';`);
      expect(res[0].p1).toBe(true);
      expect(res[0].p3).toBe(false);
    });

    test("15. Schema column type introspection verifies BOOLEAN type", async () => {
      const res = await db.query(`
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_name = 'user_profiles' AND column_name IN ('is_active', 'is_admin');
      `);
      expect(res.length).toBe(2);
    });
  });

  // =========================================================================
  // Section 2: Logical Operators (AND, OR, NOT) & Precedence (Tests 16 - 30)
  // =========================================================================
  describe("Section 2: Logical Operators (AND, OR, NOT) & Precedence", () => {
    test("16. Logical AND: true AND true = true", async () => {
      const res = await db.query(`SELECT true AND true AS r;`);
      expect(res[0].r).toBe(true);
    });

    test("17. Logical AND: true AND false = false", async () => {
      const res = await db.query(`SELECT true AND false AS r;`);
      expect(res[0].r).toBe(false);
    });

    test("18. Logical AND: false AND false = false", async () => {
      const res = await db.query(`SELECT false AND false AS r;`);
      expect(res[0].r).toBe(false);
    });

    test("19. Logical OR: true OR false = true", async () => {
      const res = await db.query(`SELECT true OR false AS r;`);
      expect(res[0].r).toBe(true);
    });

    test("20. Logical OR: false OR false = false", async () => {
      const res = await db.query(`SELECT false OR false AS r;`);
      expect(res[0].r).toBe(false);
    });

    test("21. Logical OR: true OR true = true", async () => {
      const res = await db.query(`SELECT true OR true AS r;`);
      expect(res[0].r).toBe(true);
    });

    test("22. Logical NOT: NOT true = false, NOT false = true", async () => {
      const res = await db.query(`SELECT NOT true AS r1, NOT false AS r2;`);
      expect(res[0].r1).toBe(false);
      expect(res[0].r2).toBe(true);
    });

    test("23. Logical operator precedence: NOT before AND before OR", async () => {
      const res = await db.query(`SELECT false OR NOT false AND true AS r;`);
      expect(res[0].r).toBe(true); // false OR (true AND true) = true
    });

    test("24. Parenthesized logical expression overrides precedence", async () => {
      const res = await db.query(`SELECT (false OR true) AND false AS r;`);
      expect(res[0].r).toBe(false);
    });

    test("25. De Morgan's Law 1: NOT (A AND B) = (NOT A) OR (NOT B)", async () => {
      const res = await db.query(`
        SELECT (NOT (true AND false)) AS left_side,
               ((NOT true) OR (NOT false)) AS right_side;
      `);
      expect(res[0].left_side).toBe(true);
      expect(res[0].right_side).toBe(true);
    });

    test("26. De Morgan's Law 2: NOT (A OR B) = (NOT A) AND (NOT B)", async () => {
      const res = await db.query(`
        SELECT (NOT (false OR false)) AS left_side,
               ((NOT false) AND (NOT false)) AS right_side;
      `);
      expect(res[0].left_side).toBe(true);
      expect(res[0].right_side).toBe(true);
    });

    test("27. Logical operators in WHERE clause: is_active = true AND is_admin = true", async () => {
      const res = await db.query(`SELECT username FROM user_profiles WHERE is_active = true AND is_admin = true;`);
      expect(res.length).toBe(1);
      expect(res[0].username).toBe("alice");
    });

    test("28. Logical operators in WHERE clause: department = 'sales' OR department = 'hr'", async () => {
      const res = await db.query(`SELECT username FROM user_profiles WHERE department = 'sales' OR department = 'hr' ORDER BY username ASC;`);
      expect(res.length).toBe(3);
      expect(res.map(r => r.username)).toEqual(["charlie", "david", "heidi"]);
    });

    test("29. Unary NOT on boolean column: WHERE NOT is_active", async () => {
      const res = await db.query(`SELECT username FROM user_profiles WHERE NOT is_active ORDER BY username ASC;`);
      expect(res.length).toBe(2);
      expect(res.map(r => r.username)).toEqual(["david", "frank"]);
    });

    test("30. Complex compound WHERE filter with AND, OR, NOT", async () => {
      const res = await db.query(`
        SELECT username FROM user_profiles
        WHERE (department = 'engineering' OR department = 'marketing') AND is_active = true AND NOT is_admin
        ORDER BY username ASC;
      `);
      expect(res.length).toBe(3);
      expect(res.map(r => r.username)).toEqual(["bob", "eve", "grace"]);
    });
  });

  // =========================================================================
  // Section 3: Three-Valued Logic (3VL) Truth Tables with NULL (Tests 31 - 45)
  // =========================================================================
  describe("Section 3: Three-Valued Logic (3VL) Truth Tables with NULL", () => {
    test("31. 3VL AND: NULL AND false evaluates to false", async () => {
      const res = await db.query(`SELECT NULL AND false AS r;`);
      expect(res[0].r).toBe(false);
    });

    test("32. 3VL AND: false AND NULL evaluates to false", async () => {
      const res = await db.query(`SELECT false AND NULL AS r;`);
      expect(res[0].r).toBe(false);
    });

    test("33. 3VL AND: NULL AND true evaluates to NULL", async () => {
      const res = await db.query(`SELECT NULL AND true AS r;`);
      expect(res[0].r).toBeNull();
    });

    test("34. 3VL AND: NULL AND NULL evaluates to NULL", async () => {
      const res = await db.query(`SELECT NULL AND NULL AS r;`);
      expect(res[0].r).toBeNull();
    });

    test("35. 3VL OR: NULL OR true evaluates to true", async () => {
      const res = await db.query(`SELECT NULL OR true AS r;`);
      expect(res[0].r).toBe(true);
    });

    test("36. 3VL OR: true OR NULL evaluates to true", async () => {
      const res = await db.query(`SELECT true OR NULL AS r;`);
      expect(res[0].r).toBe(true);
    });

    test("37. 3VL OR: NULL OR false evaluates to NULL", async () => {
      const res = await db.query(`SELECT NULL OR false AS r;`);
      expect(res[0].r).toBeNull();
    });

    test("38. 3VL OR: NULL OR NULL evaluates to NULL", async () => {
      const res = await db.query(`SELECT NULL OR NULL AS r;`);
      expect(res[0].r).toBeNull();
    });

    test("39. 3VL NOT: NOT NULL evaluates to NULL", async () => {
      const res = await db.query(`SELECT NOT NULL AS r;`);
      expect(res[0].r).toBeNull();
    });

    test("40. 3VL WHERE clause rejects NULL results", async () => {
      const res = await db.query(`SELECT title FROM task_items WHERE is_completed;`);
      expect(res.length).toBe(2); // Setup CI/CD (true), Database Migration (true)
    });

    test("41. 3VL WHERE clause with negated condition (WHERE NOT is_completed) rejects NULL", async () => {
      const res = await db.query(`SELECT title FROM task_items WHERE NOT is_completed;`);
      expect(res.length).toBe(2); // API Auth (false), Landing Page Copy (false)
    });

    test("42. COALESCE with boolean column fallback: COALESCE(is_verified, false)", async () => {
      const res = await db.query(`
        SELECT username, COALESCE(is_verified, false) AS verified_status
        FROM user_profiles WHERE username = 'eve';
      `);
      expect(res[0].verified_status).toBe(false);
    });

    test("43. COALESCE with boolean column fallback preserving TRUE", async () => {
      const res = await db.query(`
        SELECT username, COALESCE(is_verified, false) AS verified_status
        FROM user_profiles WHERE username = 'alice';
      `);
      expect(res[0].verified_status).toBe(true);
    });

    test("44. NULLIF with matching booleans returns NULL", async () => {
      const res = await db.query(`SELECT NULLIF(true, true) AS r;`);
      expect(res[0].r).toBeNull();
    });

    test("45. NULLIF with different booleans returns first value", async () => {
      const res = await db.query(`SELECT NULLIF(true, false) AS r;`);
      expect(res[0].r).toBe(true);
    });
  });

  // =========================================================================
  // Section 4: IS TRUE, IS FALSE, IS UNKNOWN Predicates (Tests 46 - 60)
  // =========================================================================
  describe("Section 4: IS TRUE, IS FALSE, IS UNKNOWN Predicates", () => {
    test("46. IS TRUE matches true values only", async () => {
      const res = await db.query(`
        SELECT username FROM user_profiles
        WHERE is_verified IS TRUE
        ORDER BY username ASC;
      `);
      expect(res.length).toBe(3);
      expect(res.map(r => r.username)).toEqual(["alice", "bob", "grace"]);
    });

    test("47. IS NOT TRUE matches false AND NULL values", async () => {
      const res = await db.query(`
        SELECT username FROM user_profiles
        WHERE is_verified IS NOT TRUE
        ORDER BY username ASC;
      `);
      expect(res.length).toBe(5);
      const names = res.map(r => r.username);
      expect(names).toContain("charlie"); // false
      expect(names).toContain("david");   // false
      expect(names).toContain("eve");     // NULL
      expect(names).toContain("frank");   // NULL
      expect(names).toContain("heidi");   // NULL
    });

    test("48. IS FALSE matches false values only", async () => {
      const res = await db.query(`
        SELECT username FROM user_profiles
        WHERE is_verified IS FALSE
        ORDER BY username ASC;
      `);
      expect(res.length).toBe(2);
      expect(res.map(r => r.username)).toEqual(["charlie", "david"]);
    });

    test("49. IS NOT FALSE matches true AND NULL values", async () => {
      const res = await db.query(`
        SELECT username FROM user_profiles
        WHERE is_verified IS NOT FALSE
        ORDER BY username ASC;
      `);
      expect(res.length).toBe(6);
      const names = res.map(r => r.username);
      expect(names).toContain("alice"); // true
      expect(names).toContain("bob");   // true
      expect(names).toContain("eve");   // NULL
    });

    test("50. IS UNKNOWN matches NULL boolean values", async () => {
      const res = await db.query(`
        SELECT username FROM user_profiles
        WHERE is_verified IS UNKNOWN
        ORDER BY username ASC;
      `);
      expect(res.length).toBe(3);
      expect(res.map(r => r.username)).toEqual(["eve", "frank", "heidi"]);
    });

    test("51. IS NOT UNKNOWN matches true and false, excluding NULL", async () => {
      const res = await db.query(`
        SELECT username FROM user_profiles
        WHERE is_verified IS NOT UNKNOWN
        ORDER BY username ASC;
      `);
      expect(res.length).toBe(5);
      const names = res.map(r => r.username);
      expect(names).toContain("alice");
      expect(names).toContain("charlie");
      expect(names).not.toContain("eve");
    });

    test("52. Literal boolean predicate: (true) IS TRUE", async () => {
      const res = await db.query(`SELECT (true) IS TRUE AS r1, (false) IS TRUE AS r2;`);
      expect(res[0].r1).toBe(true);
      expect(res[0].r2).toBe(false);
    });

    test("53. Literal boolean predicate: (NULL) IS UNKNOWN", async () => {
      const res = await db.query(`SELECT (NULL) IS UNKNOWN AS r;`);
      expect(res[0].r).toBe(true);
    });

    test("54. Literal boolean predicate: (false) IS FALSE", async () => {
      const res = await db.query(`SELECT (false) IS FALSE AS r;`);
      expect(res[0].r).toBe(true);
    });

    test("55. Comparison expression with IS TRUE: (10 > 5) IS TRUE", async () => {
      const res = await db.query(`SELECT (10 > 5) IS TRUE AS r;`);
      expect(res[0].r).toBe(true);
    });

    test("56. IS TRUE predicate in projection", async () => {
      const res = await db.query(`
        SELECT username, is_verified IS TRUE AS verified_bool
        FROM user_profiles WHERE username IN ('alice', 'eve')
        ORDER BY username ASC;
      `);
      expect(res[0].verified_bool).toBe(true);
      expect(res[1].verified_bool).toBe(false);
    });

    test("57. IS FALSE predicate in projection", async () => {
      const res = await db.query(`
        SELECT username, is_verified IS FALSE AS unverified_bool
        FROM user_profiles WHERE username IN ('charlie', 'eve')
        ORDER BY username ASC;
      `);
      expect(res[0].unverified_bool).toBe(true);
      expect(res[1].unverified_bool).toBe(false);
    });

    test("58. IS UNKNOWN predicate in task_items table filter", async () => {
      const res = await db.query(`
        SELECT title FROM task_items WHERE is_completed IS UNKNOWN ORDER BY task_id ASC;
      `);
      expect(res.length).toBe(2);
      expect(res[0].title).toBe("User Interviews");
      expect(res[1].title).toBe("Quarterly Review");
    });

    test("59. IS NOT UNKNOWN predicate in task_items table filter", async () => {
      const res = await db.query(`
        SELECT title FROM task_items WHERE is_completed IS NOT UNKNOWN ORDER BY task_id ASC;
      `);
      expect(res.length).toBe(4);
    });

    test("60. Combined IS TRUE and IS FALSE with OR operator", async () => {
      const res = await db.query(`
        SELECT title FROM task_items
        WHERE is_completed IS TRUE OR is_priority IS TRUE
        ORDER BY task_id ASC;
      `);
      expect(res.length).toBe(4);
    });
  });

  // =========================================================================
  // Section 5: IS DISTINCT FROM & IS NOT DISTINCT FROM (Tests 61 - 70)
  // =========================================================================
  describe("Section 5: IS DISTINCT FROM & IS NOT DISTINCT FROM", () => {
    test("61. true IS DISTINCT FROM false = true", async () => {
      const res = await db.query(`SELECT true IS DISTINCT FROM false AS r;`);
      expect(res[0].r).toBe(true);
    });

    test("62. true IS DISTINCT FROM true = false", async () => {
      const res = await db.query(`SELECT true IS DISTINCT FROM true AS r;`);
      expect(res[0].r).toBe(false);
    });

    test("63. true IS DISTINCT FROM NULL = true", async () => {
      const res = await db.query(`SELECT true IS DISTINCT FROM NULL AS r;`);
      expect(res[0].r).toBe(true);
    });

    test("64. NULL IS DISTINCT FROM NULL = false (NULL safe)", async () => {
      const res = await db.query(`SELECT NULL IS DISTINCT FROM NULL AS r;`);
      expect(res[0].r).toBe(false);
    });

    test("65. true IS NOT DISTINCT FROM true = true", async () => {
      const res = await db.query(`SELECT true IS NOT DISTINCT FROM true AS r;`);
      expect(res[0].r).toBe(true);
    });

    test("66. NULL IS NOT DISTINCT FROM NULL = true", async () => {
      const res = await db.query(`SELECT NULL IS NOT DISTINCT FROM NULL AS r;`);
      expect(res[0].r).toBe(true);
    });

    test("67. true IS NOT DISTINCT FROM NULL = false", async () => {
      const res = await db.query(`SELECT true IS NOT DISTINCT FROM NULL AS r;`);
      expect(res[0].r).toBe(false);
    });

    test("68. IS DISTINCT FROM in table WHERE filter with column and literal", async () => {
      const res = await db.query(`
        SELECT title FROM task_items
        WHERE is_completed IS DISTINCT FROM true
        ORDER BY task_id ASC;
      `);
      expect(res.length).toBe(4); // API Auth (false), Landing Page Copy (false), User Interviews (null), Quarterly Review (null)
    });

    test("69. IS NOT DISTINCT FROM in table WHERE filter", async () => {
      const res = await db.query(`
        SELECT title FROM task_items
        WHERE is_completed IS NOT DISTINCT FROM true
        ORDER BY task_id ASC;
      `);
      expect(res.length).toBe(2);
    });

    test("70. Parameterized query with IS NOT DISTINCT FROM $1", async () => {
      const res = await db.query(`
        SELECT title FROM task_items
        WHERE is_completed IS NOT DISTINCT FROM $1
        ORDER BY task_id ASC;
      `, [false]);
      expect(res.length).toBe(2);
      expect(res[0].title).toBe("API Authentication");
      expect(res[1].title).toBe("Landing Page Copy");
    });
  });

  // =========================================================================
  // Section 6: Boolean Aggregates (BOOL_AND, BOOL_OR, EVERY) & Grouping (Tests 71 - 85)
  // =========================================================================
  describe("Section 6: Boolean Aggregates (BOOL_AND, BOOL_OR, EVERY) & Grouping", () => {
    test("71. BOOL_AND() returns false when at least one value is false", async () => {
      const res = await db.query(`SELECT BOOL_AND(is_active) AS all_active FROM user_profiles;`);
      expect(res[0].all_active).toBe(false);
    });

    test("72. BOOL_AND() returns true when all filtered rows are true", async () => {
      const res = await db.query(`SELECT BOOL_AND(is_active) AS all_eng_active FROM user_profiles WHERE department = 'engineering';`);
      expect(res[0].all_eng_active).toBe(true);
    });

    test("73. BOOL_OR() returns true when at least one value is true", async () => {
      const res = await db.query(`SELECT BOOL_OR(is_admin) AS any_admin FROM user_profiles;`);
      expect(res[0].any_admin).toBe(true);
    });

    test("74. BOOL_OR() returns false when all values are false", async () => {
      const res = await db.query(`SELECT BOOL_OR(is_admin) AS any_sales_admin FROM user_profiles WHERE department = 'sales';`);
      expect(res[0].any_sales_admin).toBe(false);
    });

    test("75. EVERY() aggregate is equivalent to BOOL_AND()", async () => {
      const res = await db.query(`SELECT EVERY(is_active) AS every_active FROM user_profiles WHERE department = 'engineering';`);
      expect(res[0].every_active).toBe(true);
    });

    test("76. BOOL_AND() ignores NULL values", async () => {
      const res = await db.query(`SELECT BOOL_AND(is_verified) AS all_verified FROM user_profiles WHERE department = 'marketing';`);
      // eve (null), frank (null) -> null
      expect(res[0].all_verified).toBeNull();
    });

    test("77. GROUP BY boolean column: is_active", async () => {
      const res = await db.query(`
        SELECT is_active, COUNT(*) AS user_count
        FROM user_profiles
        WHERE is_active IS NOT NULL
        GROUP BY is_active
        ORDER BY is_active ASC;
      `);
      expect(res.length).toBe(2);
      expect(res[0].is_active).toBe(false);
      expect(Number(res[0].user_count)).toBe(2); // david, frank
      expect(res[1].is_active).toBe(true);
      expect(Number(res[1].user_count)).toBe(5); // alice, bob, charlie, eve, grace
    });

    test("78. GROUP BY department with BOOL_AND(is_active)", async () => {
      const res = await db.query(`
        SELECT department, BOOL_AND(is_active) AS all_active
        FROM user_profiles
        WHERE department IN ('engineering', 'sales')
        GROUP BY department
        ORDER BY department ASC;
      `);
      expect(res.length).toBe(2);
      expect(res[0].all_active).toBe(true);  // engineering
      expect(res[1].all_active).toBe(false); // sales
    });

    test("79. GROUP BY department with BOOL_OR(is_admin)", async () => {
      const res = await db.query(`
        SELECT department, BOOL_OR(is_admin) AS has_admin
        FROM user_profiles
        WHERE department IN ('engineering', 'sales')
        GROUP BY department
        ORDER BY department ASC;
      `);
      expect(res.length).toBe(2);
      expect(res[0].has_admin).toBe(true);  // engineering
      expect(res[1].has_admin).toBe(false); // sales
    });

    test("80. HAVING clause filtering groups by BOOL_AND(is_active) = true", async () => {
      const res = await db.query(`
        SELECT department
        FROM user_profiles
        WHERE department IS NOT NULL
        GROUP BY department
        HAVING BOOL_AND(is_active) = true;
      `);
      expect(res.length).toBeGreaterThanOrEqual(1);
      expect(res.map(r => r.department)).toContain("engineering");
    });

    test("81. HAVING clause filtering groups by BOOL_OR(is_admin) = true", async () => {
      const res = await db.query(`
        SELECT department
        FROM user_profiles
        GROUP BY department
        HAVING BOOL_OR(is_admin) = true;
      `);
      expect(res.length).toBe(1);
      expect(res[0].department).toBe("engineering");
    });

    test("82. Boolean aggregate over empty dataset returns NULL", async () => {
      const res = await db.query(`SELECT BOOL_AND(is_active) AS b_and, BOOL_OR(is_active) AS b_or FROM user_profiles WHERE user_id = 9999;`);
      expect(res[0].b_and).toBeNull();
      expect(res[0].b_or).toBeNull();
    });

    test("83. Combined statistical COUNT with boolean filter condition", async () => {
      const res = await db.query(`
        SELECT COUNT(*) AS total_users,
               COUNT(CASE WHEN is_active THEN 1 END) AS active_users,
               COUNT(CASE WHEN is_admin THEN 1 END) AS admin_users
        FROM user_profiles;
      `);
      expect(Number(res[0].total_users)).toBe(8);
      expect(Number(res[0].active_users)).toBe(5);
      expect(Number(res[0].admin_users)).toBe(1);
    });

    test("84. Multiple boolean aggregates in single query", async () => {
      const res = await db.query(`
        SELECT BOOL_AND(is_active) AS and_act, BOOL_OR(is_active) AS or_act,
               BOOL_AND(is_admin) AS and_adm, BOOL_OR(is_admin) AS or_adm
        FROM user_profiles;
      `);
      expect(res[0].and_act).toBe(false);
      expect(res[0].or_act).toBe(true);
      expect(res[0].and_adm).toBe(false);
      expect(res[0].or_adm).toBe(true);
    });

    test("85. Boolean aggregate on task_items priority check", async () => {
      const res = await db.query(`SELECT BOOL_OR(is_priority) AS has_priority_tasks FROM task_items WHERE project_id = 101;`);
      expect(res[0].has_priority_tasks).toBe(true);
    });
  });

  // =========================================================================
  // Section 7: Conditional Expressions, CASE WHEN, Filtering & DML (Tests 86 - 100)
  // =========================================================================
  describe("Section 7: Conditional Expressions, CASE WHEN, Filtering & DML", () => {
    test("86. Simple CASE WHEN with boolean column", async () => {
      const res = await db.query(`
        SELECT username,
          CASE
            WHEN is_active = true THEN 'Active User'
            ELSE 'Inactive User'
          END AS status_label
        FROM user_profiles WHERE username IN ('alice', 'david')
        ORDER BY username ASC;
      `);
      expect(res[0].status_label).toBe("Active User");
      expect(res[1].status_label).toBe("Inactive User");
    });

    test("87. CASE WHEN with 3-state boolean handling (true, false, null)", async () => {
      const res = await db.query(`
        SELECT username,
          CASE
            WHEN is_verified IS TRUE THEN 'Verified'
            WHEN is_verified IS FALSE THEN 'Unverified'
            ELSE 'Pending Verification'
          END AS verification_status
        FROM user_profiles WHERE username IN ('alice', 'charlie', 'eve')
        ORDER BY username ASC;
      `);
      expect(res[0].verification_status).toBe("Verified");
      expect(res[1].verification_status).toBe("Unverified");
      expect(res[2].verification_status).toBe("Pending Verification");
    });

    test("88. Bare boolean column in WHERE filter: WHERE is_active", async () => {
      const res = await db.query(`SELECT username FROM user_profiles WHERE is_active ORDER BY username ASC;`);
      expect(res.length).toBe(5);
    });

    test("89. Bare boolean column with NOT in WHERE filter: WHERE NOT is_active", async () => {
      const res = await db.query(`SELECT username FROM user_profiles WHERE NOT is_active ORDER BY username ASC;`);
      expect(res.length).toBe(2);
    });

    test("90. Window function partitioned by boolean column", async () => {
      const res = await db.query(`
        SELECT username, is_active,
               ROW_NUMBER() OVER (PARTITION BY is_active ORDER BY username ASC) AS row_num
        FROM user_profiles
        WHERE is_active IS NOT NULL;
      `);
      expect(res.length).toBe(7);
      expect(Number(res[0].row_num)).toBe(1);
    });

    test("91. UPDATE toggling boolean column (is_active = NOT is_active)", async () => {
      await db.query(`UPDATE user_profiles SET is_active = NOT is_active WHERE username = 'david';`);
      const res = await db.query(`SELECT is_active FROM user_profiles WHERE username = 'david';`);
      expect(res[0].is_active).toBe(true);
    });

    test("92. UPDATE resetting boolean column back", async () => {
      await db.query(`UPDATE user_profiles SET is_active = false WHERE username = 'david';`);
      const res = await db.query(`SELECT is_active FROM user_profiles WHERE username = 'david';`);
      expect(res[0].is_active).toBe(false);
    });

    test("93. Parameterized query passing boolean true parameter $1", async () => {
      const res = await db.query(`SELECT username FROM user_profiles WHERE is_active = $1 ORDER BY username ASC;`, [true]);
      expect(res.length).toBe(5);
    });

    test("94. Parameterized query passing boolean false parameter $1", async () => {
      const res = await db.query(`SELECT username FROM user_profiles WHERE is_active = $1 ORDER BY username ASC;`, [false]);
      expect(res.length).toBe(2);
    });

    test("95. INSERT new row with boolean literals", async () => {
      await db.query(`
        INSERT INTO user_profiles (user_id, username, department, is_active, is_admin, is_verified)
        VALUES (101, 'ian', 'security', true, true, true);
      `);
      const res = await db.query(`SELECT is_active, is_admin, is_verified FROM user_profiles WHERE user_id = 101;`);
      expect(res[0].is_active).toBe(true);
      expect(res[0].is_admin).toBe(true);
      expect(res[0].is_verified).toBe(true);
    });

    test("96. INSERT new row with omitted default boolean columns", async () => {
      await db.query(`
        INSERT INTO user_profiles (user_id, username, department)
        VALUES (102, 'jack', 'support');
      `);
      const res = await db.query(`SELECT is_active, is_admin, two_factor_enabled FROM user_profiles WHERE user_id = 102;`);
      expect(res[0].is_active).toBe(true);
      expect(res[0].is_admin).toBe(false);
      expect(res[0].two_factor_enabled).toBe(false);
    });

    test("97. DELETE rows matching boolean filter condition", async () => {
      await db.query(`DELETE FROM user_profiles WHERE user_id IN (101, 102);`);
      const res = await db.query(`SELECT count(*) AS cnt FROM user_profiles WHERE user_id IN (101, 102);`);
      expect(Number(res[0].cnt)).toBe(0);
    });

    test("98. UNION of queries with boolean conditions", async () => {
      const res = await db.query(`
        SELECT username, 'admin' AS role FROM user_profiles WHERE is_admin = true
        UNION
        SELECT username, 'standard' AS role FROM user_profiles WHERE is_admin = false AND is_active = true
        ORDER BY username ASC;
      `);
      expect(res.length).toBe(5);
      expect(res[0].username).toBe("alice");
      expect(res[0].role).toBe("admin");
    });

    test("99. CTE with boolean filtering and aggregation", async () => {
      const res = await db.query(`
        WITH active_members AS (
          SELECT * FROM user_profiles WHERE is_active = true
        )
        SELECT department, COUNT(*) AS count_active
        FROM active_members
        GROUP BY department
        ORDER BY department ASC;
      `);
      expect(res.length).toBe(3);
    });

    test("100. RETURNING clause with boolean column projection on UPDATE", async () => {
      const res = await db.query(`
        UPDATE user_profiles
        SET two_factor_enabled = true
        WHERE username = 'bob'
        RETURNING username, two_factor_enabled;
      `);
      expect(res.length).toBe(1);
      expect(res[0].username).toBe("bob");
      expect(res[0].two_factor_enabled).toBe(true);
    });
  });
});
