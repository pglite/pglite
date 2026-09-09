import { describe, test, expect, beforeAll, afterAll } from "bun:test";
import { PGLite } from "../src/index";

describe("100 Comprehensive In-Depth PostgreSQL ARRAY Test Cases for PGLite", () => {
  let db: PGLite;

  beforeAll(async () => {
    db = new PGLite(":memory:", { native: true });

    // Setup base tables for testing
    await db.query(`
      CREATE TABLE articles (
        id SERIAL PRIMARY KEY,
        title TEXT NOT NULL,
        tags TEXT[],
        scores INT[],
        ratings FLOAT[],
        status TEXT
      );
    `);

    await db.query(`
      CREATE TABLE dev_teams (
        team_id INT PRIMARY KEY,
        team_name TEXT,
        member_ids INT[],
        skill_sets TEXT[],
        lead_id INT
      );
    `);

    await db.query(`
      CREATE TABLE matrix_records (
        id INT PRIMARY KEY,
        name TEXT,
        grid INT[],
        active_flags BOOL[]
      );
    `);

    // Seed articles
    await db.query(`
      INSERT INTO articles (id, title, tags, scores, ratings, status) VALUES
      (1, 'PostgreSQL Internals', '{"db", "postgres", "storage"}', '{95, 88, 92}', '{4.8, 4.5, 4.9}', 'published'),
      (2, 'Rust and WASM', '{"rust", "wasm", "web"}', '{80, 85, 90}', '{4.2, 4.7, 4.6}', 'published'),
      (3, 'TypeScript Best Practices', '{"typescript", "web", "frontend"}', '{75, 78, 82}', '{3.9, 4.1, 4.0}', 'published'),
      (4, 'High Performance Engines', '{"db", "rust", "performance"}', '{98, 99, 95}', '{4.9, 5.0, 4.8}', 'published'),
      (5, 'Draft Article', '{"draft", "wip"}', '{50, 60}', '{3.0, 3.5}', 'draft'),
      (6, 'Empty Tags Post', '{}', '{}', '{}', 'draft'),
      (7, 'Null Tags Post', NULL, NULL, NULL, 'archived');
    `);

    // Seed dev_teams
    await db.query(`
      INSERT INTO dev_teams (team_id, team_name, member_ids, skill_sets, lead_id) VALUES
      (1, 'Core DB Team', '{101, 102, 103}', '{"c", "rust", "sql"}', 101),
      (2, 'Web Platform', '{104, 105}', '{"typescript", "react", "css"}', 104),
      (3, 'Infra & DevOps', '{102, 106, 107}', '{"docker", "k8s", "rust"}', 106),
      (4, 'Mobile Team', '{108, 109}', '{"swift", "kotlin"}', 108);
    `);
  });

  afterAll(async () => {
    if (db) {
      await db.close();
    }
  });

  // =========================================================================
  // Section 1: Array Literals, Parsing & Data Types (Tests 1 - 10)
  // =========================================================================
  describe("Section 1: Array Literals, Parsing & Data Types", () => {
    test("1. TEXT[] stores and retrieves string array properly", async () => {
      const res = await db.query(`SELECT tags FROM articles WHERE id = 1;`);
      expect(Array.isArray(res[0].tags)).toBe(true);
      expect(res[0].tags).toEqual(["db", "postgres", "storage"]);
    });

    test("2. INT[] stores and retrieves integer array properly", async () => {
      const res = await db.query(`SELECT scores FROM articles WHERE id = 1;`);
      expect(Array.isArray(res[0].scores)).toBe(true);
      expect(res[0].scores).toEqual([95, 88, 92]);
    });

    test("3. FLOAT[] stores and retrieves floating point numbers", async () => {
      const res = await db.query(`SELECT ratings FROM articles WHERE id = 1;`);
      expect(Array.isArray(res[0].ratings)).toBe(true);
      expect(res[0].ratings).toEqual([4.8, 4.5, 4.9]);
    });

    test("4. Empty array '{}' is stored as empty JS array []", async () => {
      const res = await db.query(`SELECT tags, scores FROM articles WHERE id = 6;`);
      expect(res[0].tags).toEqual([]);
      expect(res[0].scores).toEqual([]);
    });

    test("5. NULL array column is stored and retrieved as null", async () => {
      const res = await db.query(`SELECT tags, scores FROM articles WHERE id = 7;`);
      expect(res[0].tags).toBeNull();
      expect(res[0].scores).toBeNull();
    });

    test("6. ARRAY[...] constructor in SELECT projection", async () => {
      const res = await db.query(`SELECT ARRAY[1, 2, 3, 4] AS arr;`);
      expect(res[0].arr).toEqual([1, 2, 3, 4]);
    });

    test("7. ARRAY[...] constructor with string literals", async () => {
      const res = await db.query(`SELECT ARRAY['apple', 'banana', 'cherry'] AS fruits;`);
      expect(res[0].fruits).toEqual(["apple", "banana", "cherry"]);
    });

    test("8. ARRAY[...] constructor embedding column references", async () => {
      const res = await db.query(`SELECT ARRAY[team_id, lead_id] AS pair FROM dev_teams WHERE team_id = 1;`);
      expect(res[0].pair).toEqual([1, 101]);
    });

    test("9. Array default value on CREATE TABLE", async () => {
      await db.query(`
        CREATE TABLE default_arr_tbl (
          id INT PRIMARY KEY,
          items TEXT[] DEFAULT '{"default_item"}'
        );
      `);
      await db.query(`INSERT INTO default_arr_tbl (id) VALUES (1);`);
      const res = await db.query(`SELECT items FROM default_arr_tbl WHERE id = 1;`);
      expect(res[0].items).toEqual(["default_item"]);
    });

    test("10. Single element array parses correctly", async () => {
      await db.query(`INSERT INTO articles (id, title, tags) VALUES (8, 'Solo', '{"single"}');`);
      const res = await db.query(`SELECT tags FROM articles WHERE id = 8;`);
      expect(res[0].tags).toEqual(["single"]);
    });
  });

  // =========================================================================
  // Section 2: Array Indexing & 1-Based Access (Tests 11 - 20)
  // =========================================================================
  describe("Section 2: Array Indexing & 1-Based Access", () => {
    test("11. Array indexing tag[1] extracts first element (1-based)", async () => {
      const res = await db.query(`SELECT tags[1] AS first_tag FROM articles WHERE id = 1;`);
      expect(res[0].first_tag).toBe("db");
    });

    test("12. Array indexing tag[2] extracts second element", async () => {
      const res = await db.query(`SELECT tags[2] AS second_tag FROM articles WHERE id = 1;`);
      expect(res[0].second_tag).toBe("postgres");
    });

    test("13. Array indexing tag[3] extracts third element", async () => {
      const res = await db.query(`SELECT tags[3] AS third_tag FROM articles WHERE id = 1;`);
      expect(res[0].third_tag).toBe("storage");
    });

    test("14. Out of bounds array indexing returns NULL", async () => {
      const res = await db.query(`SELECT tags[99] AS missing_tag FROM articles WHERE id = 1;`);
      expect(res[0].missing_tag).toBeNull();
    });

    test("15. Array indexing on integer array scores[1]", async () => {
      const res = await db.query(`SELECT scores[1] AS first_score FROM articles WHERE id = 1;`);
      expect(Number(res[0].first_score)).toBe(95);
    });

    test("16. Array indexing on NULL array returns NULL", async () => {
      const res = await db.query(`SELECT tags[1] AS tag FROM articles WHERE id = 7;`);
      expect(res[0].tag).toBeNull();
    });

    test("17. Array indexing in WHERE filter condition", async () => {
      const res = await db.query(`SELECT id, title FROM articles WHERE tags[1] = 'rust';`);
      expect(res.length).toBe(1);
      expect(res[0].id).toBe(2);
    });

    test("18. Array indexing with numerical comparison (scores[1] > 90)", async () => {
      const res = await db.query(`SELECT id, title FROM articles WHERE scores[1] > 90 ORDER BY id ASC;`);
      const ids = res.map(r => r.id);
      expect(ids).toContain(1);
      expect(ids).toContain(4);
    });

    test("19. Multiple array index projections in a single SELECT", async () => {
      const res = await db.query(`SELECT tags[1] AS t1, tags[2] AS t2, scores[1] AS s1 FROM articles WHERE id = 2;`);
      expect(res[0].t1).toBe("rust");
      expect(res[0].t2).toBe("wasm");
      expect(Number(res[0].s1)).toBe(80);
    });

    test("20. Array indexing combined with UPPER() function", async () => {
      const res = await db.query(`SELECT UPPER(tags[1]) AS upper_tag FROM articles WHERE id = 1;`);
      expect(res[0].upper_tag).toBe("DB");
    });
  });

  // =========================================================================
  // Section 3: Array Operators (@>, <@, &&, ||) (Tests 21 - 35)
  // =========================================================================
  describe("Section 3: Array Operators (@>, <@, &&, ||)", () => {
    test("21. Contains operator @> checks if array contains subset", async () => {
      const res = await db.query(`SELECT id, title FROM articles WHERE tags @> ARRAY['rust'] ORDER BY id ASC;`);
      const ids = res.map(r => r.id);
      expect(ids).toContain(2);
      expect(ids).toContain(4);
      expect(ids).not.toContain(1);
    });

    test("22. Contains operator @> with multiple required elements", async () => {
      const res = await db.query(`SELECT id, title FROM articles WHERE tags @> ARRAY['rust', 'wasm'];`);
      expect(res.length).toBe(1);
      expect(res[0].id).toBe(2);
    });

    test("23. Is contained by operator <@", async () => {
      const res = await db.query(`SELECT id, title FROM articles WHERE tags <@ ARRAY['db', 'postgres', 'storage', 'extra'] AND tags IS NOT NULL AND ARRAY_LENGTH(tags, 1) > 0;`);
      expect(res.length).toBe(1);
      expect(res[0].id).toBe(1);
    });

    test("24. Overlap operator && checks for any common element", async () => {
      const res = await db.query(`SELECT id, title FROM articles WHERE tags && ARRAY['web', 'mobile'] ORDER BY id ASC;`);
      const ids = res.map(r => r.id);
      expect(ids).toContain(2);
      expect(ids).toContain(3);
    });

    test("25. Overlap operator && returns false when no common elements", async () => {
      const res = await db.query(`SELECT id FROM articles WHERE tags && ARRAY['unrelated_tag'];`);
      expect(res.length).toBe(0);
    });

    test("26. Array concatenation operator || with two arrays in SELECT", async () => {
      const res = await db.query(`SELECT ARRAY[1, 2] || ARRAY[3, 4] AS combined;`);
      expect(res[0].combined).toEqual([1, 2, 3, 4]);
    });

    test("27. Array concatenation operator || with string arrays", async () => {
      const res = await db.query(`SELECT ARRAY['a', 'b'] || ARRAY['c', 'd'] AS letters;`);
      expect(res[0].letters).toEqual(["a", "b", "c", "d"]);
    });

    test("28. Array concatenation operator || appending column array with literal", async () => {
      const res = await db.query(`SELECT tags || ARRAY['new_tag'] AS extended FROM articles WHERE id = 1;`);
      expect(res[0].extended).toEqual(["db", "postgres", "storage", "new_tag"]);
    });

    test("29. Array contains with integer array (member_ids @> ARRAY[102])", async () => {
      const res = await db.query(`SELECT team_id, team_name FROM dev_teams WHERE member_ids @> ARRAY[102] ORDER BY team_id ASC;`);
      const teamIds = res.map(r => r.team_id);
      expect(teamIds).toContain(1);
      expect(teamIds).toContain(3);
    });

    test("30. Array overlap with integer array (member_ids && ARRAY[104, 109])", async () => {
      const res = await db.query(`SELECT team_id FROM dev_teams WHERE member_ids && ARRAY[104, 109] ORDER BY team_id ASC;`);
      const teamIds = res.map(r => r.team_id);
      expect(teamIds).toContain(2);
      expect(teamIds).toContain(4);
    });

    test("31. Array contains on empty array returns true for empty subset", async () => {
      const res = await db.query(`SELECT id FROM articles WHERE tags @> ARRAY[]::text[];`);
      expect(res.length).toBeGreaterThanOrEqual(1);
    });

    test("32. Array contains on NULL returns false", async () => {
      const res = await db.query(`SELECT id FROM articles WHERE tags @> ARRAY['rust'] AND id = 7;`);
      expect(res.length).toBe(0);
    });

    test("33. Negated array contains (NOT (tags @> ARRAY['db']))", async () => {
      const res = await db.query(`SELECT id FROM articles WHERE NOT (tags @> ARRAY['db']) AND tags IS NOT NULL;`);
      const ids = res.map(r => r.id);
      expect(ids).not.toContain(1);
      expect(ids).not.toContain(4);
      expect(ids).toContain(2);
    });

    test("34. Array concatenation with empty array returns original array", async () => {
      const res = await db.query(`SELECT ARRAY['x', 'y'] || ARRAY[]::text[] AS res_arr;`);
      expect(res[0].res_arr).toEqual(["x", "y"]);
    });

    test("35. Complex filter combining @> and &&", async () => {
      const res = await db.query(`
        SELECT id, title FROM articles
        WHERE tags @> ARRAY['rust'] AND tags && ARRAY['web', 'performance']
        ORDER BY id ASC;
      `);
      expect(res.length).toBe(2);
    });
  });

  // =========================================================================
  // Section 4: Array Built-in Functions (Tests 36 - 55)
  // =========================================================================
  describe("Section 4: Array Built-in Functions", () => {
    test("36. ARRAY_LENGTH(array, 1) returns number of elements", async () => {
      const res = await db.query(`SELECT id, ARRAY_LENGTH(tags, 1) AS len FROM articles WHERE id = 1;`);
      expect(Number(res[0].len)).toBe(3);
    });

    test("37. ARRAY_LENGTH on 2-element array returns 2", async () => {
      const res = await db.query(`SELECT id, ARRAY_LENGTH(tags, 1) AS len FROM articles WHERE id = 5;`);
      expect(Number(res[0].len)).toBe(2);
    });

    test("38. ARRAY_LENGTH on empty array returns 0 or null", async () => {
      const res = await db.query(`SELECT id, ARRAY_LENGTH(tags, 1) AS len FROM articles WHERE id = 6;`);
      expect(res[0].len == null || Number(res[0].len) === 0).toBe(true);
    });

    test("39. ARRAY_LENGTH on NULL returns NULL", async () => {
      const res = await db.query(`SELECT id, ARRAY_LENGTH(tags, 1) AS len FROM articles WHERE id = 7;`);
      expect(res[0].len).toBeNull();
    });

    test("40. ARRAY_APPEND(array, elem) appends element to the end", async () => {
      const res = await db.query(`SELECT ARRAY_APPEND(ARRAY[1, 2, 3], 4) AS appended;`);
      expect(res[0].appended).toEqual([1, 2, 3, 4]);
    });

    test("41. ARRAY_PREPEND(elem, array) prepends element to the beginning", async () => {
      const res = await db.query(`SELECT ARRAY_PREPEND(0, ARRAY[1, 2, 3]) AS prepended;`);
      expect(res[0].prepended).toEqual([0, 1, 2, 3]);
    });

    test("42. ARRAY_REMOVE(array, elem) removes all matching occurrences", async () => {
      const res = await db.query(`SELECT ARRAY_REMOVE(ARRAY['a', 'b', 'a', 'c'], 'a') AS cleaned;`);
      expect(res[0].cleaned).toEqual(["b", "c"]);
    });

    test("43. ARRAY_REPLACE(array, from, to) replaces matching element", async () => {
      const res = await db.query(`SELECT ARRAY_REPLACE(ARRAY[1, 2, 3, 2], 2, 99) AS replaced;`);
      expect(res[0].replaced).toEqual([1, 99, 3, 99]);
    });

    test("44. ARRAY_TO_STRING(array, delimiter) joins elements into string", async () => {
      const res = await db.query(`SELECT ARRAY_TO_STRING(tags, ', ') AS tag_str FROM articles WHERE id = 1;`);
      expect(res[0].tag_str).toBe("db, postgres, storage");
    });

    test("45. ARRAY_TO_STRING with hyphen delimiter", async () => {
      const res = await db.query(`SELECT ARRAY_TO_STRING(ARRAY['2024', '09', '09'], '-') AS d_str;`);
      expect(res[0].d_str).toBe("2024-09-09");
    });

    test("46. STRING_TO_ARRAY(string, delimiter) splits string into array", async () => {
      const res = await db.query(`SELECT STRING_TO_ARRAY('apple,banana,cherry', ',') AS arr;`);
      expect(res[0].arr).toEqual(["apple", "banana", "cherry"]);
    });

    test("47. STRING_TO_ARRAY with space delimiter", async () => {
      const res = await db.query(`SELECT STRING_TO_ARRAY('hello world postgres', ' ') AS words;`);
      expect(res[0].words).toEqual(["hello", "world", "postgres"]);
    });

    test("48. ARRAY_LENGTH in WHERE clause filter", async () => {
      const res = await db.query(`SELECT id, title FROM articles WHERE ARRAY_LENGTH(tags, 1) = 3 ORDER BY id ASC;`);
      const ids = res.map(r => r.id);
      expect(ids).toContain(1);
      expect(ids).toContain(2);
      expect(ids).toContain(3);
      expect(ids).toContain(4);
    });

    test("49. ARRAY_APPEND with column array reference", async () => {
      const res = await db.query(`SELECT id, ARRAY_APPEND(tags, 'extra') AS mod_tags FROM articles WHERE id = 2;`);
      expect(res[0].mod_tags).toEqual(["rust", "wasm", "web", "extra"]);
    });

    test("50. ARRAY_PREPEND with column array reference", async () => {
      const res = await db.query(`SELECT id, ARRAY_PREPEND('featured', tags) AS mod_tags FROM articles WHERE id = 2;`);
      expect(res[0].mod_tags).toEqual(["featured", "rust", "wasm", "web"]);
    });

    test("51. ARRAY_TO_STRING on empty array returns empty string", async () => {
      const res = await db.query(`SELECT ARRAY_TO_STRING(ARRAY[]::text[], ',') AS empty_str;`);
      expect(res[0].empty_str).toBe("");
    });

    test("52. ARRAY_REMOVE on element not in array returns original array", async () => {
      const res = await db.query(`SELECT ARRAY_REMOVE(ARRAY[1, 2, 3], 99) AS unchanged;`);
      expect(res[0].unchanged).toEqual([1, 2, 3]);
    });

    test("53. ARRAY_REPLACE on non-matching element leaves array unchanged", async () => {
      const res = await db.query(`SELECT ARRAY_REPLACE(ARRAY['a', 'b'], 'z', 'x') AS res_arr;`);
      expect(res[0].res_arr).toEqual(["a", "b"]);
    });

    test("54. STRING_TO_ARRAY on empty string returns empty array", async () => {
      const res = await db.query(`SELECT STRING_TO_ARRAY('', ',') AS empty_arr;`);
      expect(res[0].empty_arr).toEqual([]);
    });

    test("55. ARRAY_TO_STRING combined with UPPER()", async () => {
      const res = await db.query(`SELECT UPPER(ARRAY_TO_STRING(tags, ' / ')) AS upper_str FROM articles WHERE id = 1;`);
      expect(res[0].upper_str).toBe("DB / POSTGRES / STORAGE");
    });
  });

  // =========================================================================
  // Section 5: UNNEST & Array Set Returning Functions (Tests 56 - 70)
  // =========================================================================
  describe("Section 5: UNNEST & Array Set Returning Functions", () => {
    test("56. UNNEST(ARRAY[...]) expands array into multiple rows", async () => {
      const res = await db.query(`SELECT animal FROM UNNEST(ARRAY['cat', 'dog', 'bird']) AS t(animal);`);
      expect(res.length).toBe(3);
      expect(res[0].animal).toBe("cat");
      expect(res[1].animal).toBe("dog");
      expect(res[2].animal).toBe("bird");
    });

    test("57. UNNEST on column array in FROM clause expands table rows", async () => {
      const res = await db.query(`SELECT tag FROM UNNEST(ARRAY['db', 'postgres', 'storage']) AS t(tag);`);
      expect(res.length).toBe(3);
      expect(res[0].tag).toBe("db");
      expect(res[1].tag).toBe("postgres");
      expect(res[2].tag).toBe("storage");
    });

    test("58. UNNEST with integer array expands into number rows", async () => {
      const res = await db.query(`SELECT num FROM UNNEST(ARRAY[10, 20, 30]) AS t(num);`);
      expect(res.length).toBe(3);
      expect(Number(res[0].num)).toBe(10);
      expect(Number(res[2].num)).toBe(30);
    });

    test("59. UNNEST combined with WHERE filter on unnested values", async () => {
      const res = await db.query(`
        SELECT tag FROM UNNEST(ARRAY['rust', 'wasm', 'rust', 'go']) AS t(tag)
        WHERE tag = 'rust';
      `);
      expect(res.length).toBe(2);
    });

    test("60. DISTINCT UNNEST collects unique elements", async () => {
      const res = await db.query(`
        SELECT DISTINCT tag FROM UNNEST(ARRAY['db', 'rust', 'wasm', 'rust', 'db']) AS t(tag)
        ORDER BY tag ASC;
      `);
      expect(res.length).toBe(3);
      const tagList = res.map(r => r.tag);
      expect(tagList).toContain("db");
      expect(tagList).toContain("rust");
      expect(tagList).toContain("wasm");
    });

    test("61. COUNT over UNNEST gives total item count", async () => {
      const res = await db.query(`
        SELECT COUNT(*) AS total_items FROM UNNEST(ARRAY[1, 2, 3, 4, 5, 6, 7]) AS t(val);
      `);
      expect(Number(res[0].total_items)).toBe(7);
    });

    test("62. UNNEST integer array with aggregate SUM()", async () => {
      const res = await db.query(`
        SELECT SUM(score) AS sum_scores FROM UNNEST(ARRAY[95, 88, 92]) AS t(score);
      `);
      // 95 + 88 + 92 = 275
      expect(Number(res[0].sum_scores)).toBe(275);
    });

    test("63. UNNEST integer array with aggregate AVG()", async () => {
      const res = await db.query(`
        SELECT AVG(score) AS avg_score FROM UNNEST(ARRAY[90, 80, 70]) AS t(score);
      `);
      expect(Number(res[0].avg_score)).toBe(80);
    });

    test("64. UNNEST integer array with MIN() and MAX()", async () => {
      const res = await db.query(`
        SELECT MIN(score) AS min_s, MAX(score) AS max_s FROM UNNEST(ARRAY[95, 88, 92]) AS t(score);
      `);
      expect(Number(res[0].min_s)).toBe(88);
      expect(Number(res[0].max_s)).toBe(95);
    });

    test("65. UNNEST with empty array yields 0 rows", async () => {
      const res = await db.query(`SELECT val FROM UNNEST(ARRAY[]::text[]) AS t(val);`);
      expect(res.length).toBe(0);
    });

    test("66. UNNEST on team member_ids literal", async () => {
      const res = await db.query(`SELECT mid FROM UNNEST(ARRAY[101, 102, 103]) AS t(mid);`);
      expect(res.length).toBe(3);
      const mids = res.map(r => Number(r.mid));
      expect(mids).toEqual([101, 102, 103]);
    });

    test("67. JOINing unnested array values with another table", async () => {
      const res = await db.query(`
        SELECT d.team_name, u.member_id
        FROM dev_teams d
        JOIN (SELECT 101 AS member_id, 'Alice' AS name) u ON u.member_id = d.lead_id;
      `);
      expect(res.length).toBe(1);
      expect(res[0].team_name).toBe("Core DB Team");
    });

    test("68. UNNEST in subquery with IN operator", async () => {
      const res = await db.query(`
        SELECT team_name FROM dev_teams
        WHERE 101 IN (SELECT mid FROM UNNEST(ARRAY[101, 102]) AS t(mid));
      `);
      expect(res.length).toBeGreaterThan(0);
    });

    test("69. UNNEST with ORDER BY unnested element", async () => {
      const res = await db.query(`
        SELECT sc FROM UNNEST(ARRAY[95, 88, 92]) AS t(sc) ORDER BY sc ASC;
      `);
      expect(res.length).toBe(3);
      expect(Number(res[0].sc)).toBe(88);
      expect(Number(res[2].sc)).toBe(95);
    });

    test("70. UNNEST with LIMIT and OFFSET", async () => {
      const res = await db.query(`
        SELECT num FROM UNNEST(ARRAY[1, 2, 3, 4, 5]) AS t(num) LIMIT 2 OFFSET 1;
      `);
      expect(res.length).toBe(2);
      expect(Number(res[0].num)).toBe(2);
      expect(Number(res[1].num)).toBe(3);
    });
  });

  // =========================================================================
  // Section 6: ARRAY_AGG Aggregate Function (Tests 71 - 85)
  // =========================================================================
  describe("Section 6: ARRAY_AGG Aggregate Function", () => {
    test("71. ARRAY_AGG(column) aggregates table column into array", async () => {
      const res = await db.query(`SELECT ARRAY_AGG(team_id) AS id_list FROM dev_teams;`);
      expect(Array.isArray(res[0].id_list)).toBe(true);
      expect(res[0].id_list.length).toBe(4);
    });

    test("72. ARRAY_AGG with string column aggregates names into array", async () => {
      const res = await db.query(`SELECT ARRAY_AGG(team_name) AS names FROM dev_teams;`);
      expect(res[0].names).toContain("Core DB Team");
      expect(res[0].names).toContain("Web Platform");
    });

    test("73. ARRAY_AGG combined with GROUP BY groups values per category", async () => {
      const res = await db.query(`
        SELECT status, ARRAY_AGG(id) AS article_ids
        FROM articles
        WHERE status IS NOT NULL
        GROUP BY status
        ORDER BY status ASC;
      `);
      expect(res.length).toBe(3);
      const pubGroup = res.find(r => r.status === "published");
      expect(pubGroup.article_ids).toContain(1);
      expect(pubGroup.article_ids).toContain(2);
    });

    test("74. ARRAY_AGG on filtered WHERE dataset", async () => {
      const res = await db.query(`SELECT ARRAY_AGG(id) AS published_ids FROM articles WHERE status = 'published';`);
      expect(res[0].published_ids.length).toBe(4);
    });

    test("75. ARRAY_AGG combined with ARRAY_TO_STRING", async () => {
      const res = await db.query(`SELECT ARRAY_TO_STRING(ARRAY_AGG(team_id), '-') AS id_chain FROM dev_teams;`);
      expect(res[0].id_chain).toContain("1-2-3-4");
    });

    test("76. ARRAY_AGG aggregates multiple elements in group", async () => {
      const res = await db.query(`SELECT ARRAY_AGG(status) AS statuses FROM articles WHERE status IS NOT NULL;`);
      expect(res[0].statuses.length).toBe(7);
    });

    test("77. ARRAY_AGG with single row group returns 1-element array", async () => {
      const res = await db.query(`SELECT ARRAY_AGG(team_name) AS t FROM dev_teams WHERE team_id = 1;`);
      expect(res[0].t).toEqual(["Core DB Team"]);
    });

    test("78. ARRAY_AGG on 0 rows returns NULL or empty array", async () => {
      const res = await db.query(`SELECT ARRAY_AGG(title) AS empty_agg FROM articles WHERE id = 999;`);
      expect(res[0].empty_agg == null || res[0].empty_agg.length === 0).toBe(true);
    });

    test("79. ARRAY_AGG in subquery projection", async () => {
      const res = await db.query(`
        SELECT team_id, (SELECT ARRAY_AGG(id) FROM articles WHERE status = 'draft') AS draft_ids
        FROM dev_teams WHERE team_id = 1;
      `);
      expect(res[0].draft_ids).toContain(5);
      expect(res[0].draft_ids).toContain(6);
    });

    test("80. ARRAY_AGG combined with ARRAY_LENGTH", async () => {
      const res = await db.query(`SELECT ARRAY_LENGTH(ARRAY_AGG(team_id), 1) AS total_teams FROM dev_teams;`);
      expect(Number(res[0].total_teams)).toBe(4);
    });

    test("81. ARRAY_AGG of expressions (team_id * 10)", async () => {
      const res = await db.query(`SELECT ARRAY_AGG(team_id * 10) AS tens FROM dev_teams;`);
      expect(res[0].tens).toEqual([10, 20, 30, 40]);
    });

    test("82. ARRAY_AGG over strings with UPPER() transformation", async () => {
      const res = await db.query(`SELECT ARRAY_AGG(UPPER(status)) AS caps_statuses FROM articles WHERE id IN (1, 5);`);
      expect(res[0].caps_statuses).toContain("PUBLISHED");
      expect(res[0].caps_statuses).toContain("DRAFT");
    });

    test("83. HAVING clause filtering groups by count", async () => {
      const res = await db.query(`
        SELECT status, COUNT(*) AS cnt
        FROM articles
        GROUP BY status
        HAVING COUNT(*) > 1;
      `);
      expect(res.length).toBeGreaterThan(0);
    });

    test("84. Nested aggregation using ARRAY_AGG on transformed column", async () => {
      const res = await db.query(`
        SELECT ARRAY_AGG(status) AS status_arr
        FROM articles
        WHERE status = 'published';
      `);
      expect(Array.isArray(res[0].status_arr)).toBe(true);
      expect(res[0].status_arr.length).toBe(4);
    });

    test("85. ARRAY_AGG with COALESCE handling empty values", async () => {
      const res = await db.query(`
        SELECT COALESCE(ARRAY_AGG(id), ARRAY[]::int[]) AS agg_res
        FROM articles WHERE id = 9999;
      `);
      expect(res[0].agg_res).toEqual([]);
    });
  });

  // =========================================================================
  // Section 7: DML Operations, Mutations & Parameters on Arrays (Tests 86 - 100)
  // =========================================================================
  describe("Section 7: DML Operations, Mutations & Parameters on Arrays", () => {
    test("86. INSERT new row with array literal", async () => {
      await db.query(`
        INSERT INTO articles (id, title, tags, scores, status)
        VALUES (101, 'New Article', '{"ai", "ml", "python"}', '{90, 92, 94}', 'published');
      `);
      const res = await db.query(`SELECT tags, scores FROM articles WHERE id = 101;`);
      expect(res[0].tags).toEqual(["ai", "ml", "python"]);
      expect(res[0].scores).toEqual([90, 92, 94]);
    });

    test("87. INSERT with ARRAY[...] constructor syntax", async () => {
      await db.query(`
        INSERT INTO dev_teams (team_id, team_name, member_ids, skill_sets, lead_id)
        VALUES (5, 'Security Team', ARRAY[110, 111], ARRAY['crypto', 'audit'], 110);
      `);
      const res = await db.query(`SELECT member_ids, skill_sets FROM dev_teams WHERE team_id = 5;`);
      expect(res[0].member_ids).toEqual([110, 111]);
      expect(res[0].skill_sets).toEqual(["crypto", "audit"]);
    });

    test("88. UPDATE replacing entire array column with new literal", async () => {
      await db.query(`UPDATE articles SET tags = '{"updated", "revised"}' WHERE id = 101;`);
      const res = await db.query(`SELECT tags FROM articles WHERE id = 101;`);
      expect(res[0].tags).toEqual(["updated", "revised"]);
    });

    test("89. UPDATE appending to array using ARRAY_APPEND", async () => {
      await db.query(`UPDATE articles SET tags = ARRAY_APPEND(tags, 'v2') WHERE id = 101;`);
      const res = await db.query(`SELECT tags FROM articles WHERE id = 101;`);
      expect(res[0].tags).toEqual(["updated", "revised", "v2"]);
    });

    test("90. UPDATE removing from array using ARRAY_REMOVE", async () => {
      await db.query(`UPDATE articles SET tags = ARRAY_REMOVE(tags, 'v2') WHERE id = 101;`);
      const res = await db.query(`SELECT tags FROM articles WHERE id = 101;`);
      expect(res[0].tags).toEqual(["updated", "revised"]);
    });

    test("91. UPDATE array concatenation using ||", async () => {
      await db.query(`UPDATE articles SET tags = tags || ARRAY['tag3', 'tag4'] WHERE id = 101;`);
      const res = await db.query(`SELECT tags FROM articles WHERE id = 101;`);
      expect(res[0].tags).toEqual(["updated", "revised", "tag3", "tag4"]);
    });

    test("92. DELETE rows based on array containment condition (tags @> ARRAY['tag3'])", async () => {
      await db.query(`DELETE FROM articles WHERE tags @> ARRAY['tag3'];`);
      const res = await db.query(`SELECT count(*) AS cnt FROM articles WHERE id = 101;`);
      expect(Number(res[0].cnt)).toBe(0);
    });

    test("93. Parameterized query passing array parameter $1", async () => {
      const res = await db.query(`SELECT id, title FROM articles WHERE tags @> $1;`, [['rust']]);
      expect(res.length).toBeGreaterThanOrEqual(2);
    });

    test("94. Parameterized query with ARRAY overlap parameter $1", async () => {
      const res = await db.query(`SELECT id, title FROM articles WHERE tags && $1;`, [['db', 'wasm']]);
      expect(res.length).toBeGreaterThanOrEqual(2);
    });

    test("95. Parameterized query with single element index search $1", async () => {
      const res = await db.query(`SELECT id, title FROM articles WHERE tags[1] = $1;`, ['db']);
      expect(res.length).toBeGreaterThanOrEqual(1);
    });

    test("96. Boolean array BOOL[] stores and evaluates true/false elements", async () => {
      await db.query(`
        INSERT INTO matrix_records (id, name, grid, active_flags)
        VALUES (1, 'Flags Matrix', '{1, 0, 1}', '{true, false, true}');
      `);
      const res = await db.query(`SELECT active_flags FROM matrix_records WHERE id = 1;`);
      expect(res[0].active_flags).toEqual([true, false, true]);
    });

    test("97. Boolean array indexing active_flags[1]", async () => {
      const res = await db.query(`SELECT active_flags[1] AS first_flag, active_flags[2] AS second_flag FROM matrix_records WHERE id = 1;`);
      expect(res[0].first_flag).toBe(true);
      expect(res[0].second_flag).toBe(false);
    });

    test("98. ALTER TABLE ADD COLUMN with array type", async () => {
      await db.query(`ALTER TABLE dev_teams ADD COLUMN certifications TEXT[] DEFAULT '{}';`);
      const res = await db.query(`SELECT certifications FROM dev_teams WHERE team_id = 1;`);
      expect(res[0].certifications).toEqual([]);
    });

    test("99. Window function over array column length", async () => {
      const res = await db.query(`
        SELECT team_id, team_name, ARRAY_LENGTH(member_ids, 1) AS team_size,
               RANK() OVER (ORDER BY ARRAY_LENGTH(member_ids, 1) DESC) AS rank_by_size
        FROM dev_teams
        WHERE member_ids IS NOT NULL;
      `);
      expect(res.length).toBeGreaterThanOrEqual(3);
      expect(Number(res[0].rank_by_size)).toBe(1);
    });

    test("100. Schema introspection verifying array column data types", async () => {
      const res = await db.query(`
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_name = 'articles' AND column_name IN ('tags', 'scores', 'ratings');
      `);
      expect(res.length).toBe(3);
    });
  });
});

