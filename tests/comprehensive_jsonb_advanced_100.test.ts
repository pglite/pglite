import { describe, test, expect, beforeAll, afterAll } from "bun:test";
import { PGLite } from "../src/index";

describe("100 Comprehensive In-Depth PostgreSQL JSONB & JSON Test Cases for PGLite", () => {
  let db: PGLite;

  beforeAll(async () => {
    db = new PGLite(":memory:", { native: true });

    await db.query(`
      CREATE TABLE documents (
        id INT PRIMARY KEY,
        title TEXT,
        data JSONB,
        meta JSON,
        status TEXT
      );
    `);

    await db.query(`
      INSERT INTO documents (id, title, data, meta, status) VALUES
      (1, 'Tech Spec', '{"author": "Alice", "tags": ["db", "rust", "wasm"], "specs": {"version": 2, "draft": false, "ratings": [5, 4, 5]}, "details": {"active": true, "null_val": null}}', '{"views": 1200, "featured": true}', 'published'),
      (2, 'Design Doc', '{"author": "Bob", "tags": ["ui", "design"], "specs": {"version": 1, "draft": true, "ratings": [4, 3]}, "details": {"active": false}}', '{"views": 450, "featured": false}', 'draft'),
      (3, 'API Guide', '{"author": "Charlie", "tags": ["api", "rust"], "specs": {"version": 3, "draft": false, "ratings": [5, 5, 5]}, "details": {"active": true}}', '{"views": 3200, "featured": true}', 'published'),
      (4, 'Legacy Notes', '{"author": "Alice", "tags": ["legacy"], "specs": {"version": 1, "draft": false, "ratings": []}, "details": {"active": false, "archived": true}}', '{"views": 80, "featured": false}', 'archived'),
      (5, 'Empty Doc', '{}', '{}', 'draft');
    `);
  });

  afterAll(async () => {
    if (db) {
      await db.close();
    }
  });

  // Section 1: Arrow Extraction Operators (-> and ->>)
  describe("Section 1: Arrow Extraction Operators (-> and ->>)", () => {
    test("1. Extract top-level field as JSON with ->", async () => {
      const res = await db.query(`SELECT data->'author' AS author_json FROM documents WHERE id = 1;`);
      expect(res[0].author_json).toBe("Alice");
    });

    test("2. Extract top-level field as text with ->>", async () => {
      const res = await db.query(`SELECT data->>'author' AS author_text FROM documents WHERE id = 1;`);
      expect(res[0].author_text).toBe("Alice");
    });

    test("3. Extract nested field with chained -> and ->>", async () => {
      const res = await db.query(`SELECT data->'specs'->>'version' AS ver FROM documents WHERE id = 1;`);
      expect(res[0].ver).toBe("2");
    });

    test("4. Extract boolean field as text", async () => {
      const res = await db.query(`SELECT data->'specs'->>'draft' AS is_draft FROM documents WHERE id = 1;`);
      expect(res[0].is_draft).toBe("false");
    });

    test("5. Extract non-existent key returns NULL", async () => {
      const res = await db.query(`SELECT data->>'non_existent' AS missing FROM documents WHERE id = 1;`);
      expect(res[0].missing).toBeNull();
    });

    test("6. Extract from empty JSON object returns NULL", async () => {
      const res = await db.query(`SELECT data->>'author' AS author FROM documents WHERE id = 5;`);
      expect(res[0].author).toBeNull();
    });

    test("7. Extract array element by index with ->", async () => {
      const res = await db.query(`SELECT data->'tags'->0 AS first_tag FROM documents WHERE id = 1;`);
      expect(res[0].first_tag).toBe("db");
    });

    test("8. Extract array element by index with ->>", async () => {
      const res = await db.query(`SELECT data->'tags'->>1 AS second_tag FROM documents WHERE id = 1;`);
      expect(res[0].second_tag).toBe("rust");
    });

    test("9. Extract out-of-bounds array index returns NULL", async () => {
      const res = await db.query(`SELECT data->'tags'->>99 AS no_tag FROM documents WHERE id = 1;`);
      expect(res[0].no_tag).toBeNull();
    });

    test("10. Multi-level nested array element extraction", async () => {
      const res = await db.query(`SELECT data->'specs'->'ratings'->>0 AS first_rating FROM documents WHERE id = 1;`);
      expect(res[0].first_rating).toBe("5");
    });
  });

  // Section 2: Path Extraction Operators (#> and #>>)
  describe("Section 2: Path Extraction Operators (#> and #>>)", () => {
    test("11. Extract nested value with #> path operator", async () => {
      const res = await db.query(`SELECT data#>'{"specs", "version"}' AS ver_json FROM documents WHERE id = 1;`);
      expect(res[0].ver_json).toBe(2);
    });

    test("12. Extract nested value with #>> path operator as text", async () => {
      const res = await db.query(`SELECT data#>>'{"specs", "version"}' AS ver_text FROM documents WHERE id = 1;`);
      expect(res[0].ver_text).toBe("2");
    });

    test("13. Extract array element via path #>>", async () => {
      const res = await db.query(`SELECT data#>>'{"tags", "0"}' AS tag FROM documents WHERE id = 1;`);
      expect(res[0].tag).toBe("db");
    });

    test("14. Extract deeply nested array element with #>>", async () => {
      const res = await db.query(`SELECT data#>>'{"specs", "ratings", "1"}' AS second_rating FROM documents WHERE id = 1;`);
      expect(res[0].second_rating).toBe("4");
    });

    test("15. Path extraction on non-existent path returns NULL", async () => {
      const res = await db.query(`SELECT data#>>'{"specs", "invalid", "deep"}' AS missing FROM documents WHERE id = 1;`);
      expect(res[0].missing).toBeNull();
    });
  });

  // Section 3: Containment Operators (@> and <@)
  describe("Section 3: Containment Operators (@> and <@)", () => {
    test("16. Filter WHERE data @> top-level object", async () => {
      const res = await db.query(`SELECT id, title FROM documents WHERE data @> '{"author": "Alice"}' ORDER BY id ASC;`);
      expect(res.length).toBe(2);
      expect(res[0].id).toBe(1);
      expect(res[1].id).toBe(4);
    });

    test("17. Filter WHERE data @> nested object", async () => {
      const res = await db.query(`SELECT id, title FROM documents WHERE data @> '{"specs": {"version": 2}}';`);
      expect(res.length).toBe(1);
      expect(res[0].id).toBe(1);
    });

    test("18. Filter WHERE data @> array element containment", async () => {
      const res = await db.query(`SELECT id, title FROM documents WHERE data @> '{"tags": ["rust"]}' ORDER BY id ASC;`);
      expect(res.length).toBe(2);
      expect(res[0].id).toBe(1);
      expect(res[1].id).toBe(3);
    });

    test("19. Filter WHERE data @> boolean field", async () => {
      const res = await db.query(`SELECT id, title FROM documents WHERE data @> '{"specs": {"draft": true}}';`);
      expect(res.length).toBe(1);
      expect(res[0].id).toBe(2);
    });

    test("20. Filter WHERE data @> non-matching returns empty", async () => {
      const res = await db.query(`SELECT id FROM documents WHERE data @> '{"author": "NonExistent"}';`);
      expect(res.length).toBe(0);
    });

    test("21. Contained in operator <@", async () => {
      const res = await db.query(`SELECT id FROM documents WHERE '{"version": 1, "draft": true}' <@ data->'specs';`);
      expect(res.length).toBe(1);
      expect(res[0].id).toBe(2);
    });
  });

  // Section 4: Key Existence Operators (?, ?|, ?&)
  describe("Section 4: Key Existence Operators (?, ?|, ?&)", () => {
    test("22. Check single key existence with ?", async () => {
      const res = await db.query(`SELECT id FROM documents WHERE data ? 'author' ORDER BY id ASC;`);
      expect(res.length).toBe(4);
    });

    test("23. Check single key existence on empty object returns false", async () => {
      const res = await db.query(`SELECT id FROM documents WHERE id = 5 AND data ? 'author';`);
      expect(res.length).toBe(0);
    });

    test("24. Check nested key existence with ?", async () => {
      const res = await db.query(`SELECT id FROM documents WHERE data->'details' ? 'archived';`);
      expect(res.length).toBe(1);
      expect(res[0].id).toBe(4);
    });

    test("25. Check array string element existence with ?", async () => {
      const res = await db.query(`SELECT id FROM documents WHERE data->'tags' ? 'wasm';`);
      expect(res.length).toBe(1);
      expect(res[0].id).toBe(1);
    });

    test("26. Check any key existence with ?|", async () => {
      const res = await db.query(`SELECT id FROM documents WHERE data ?| '{"author", "non_existent"}' ORDER BY id ASC;`);
      expect(res.length).toBe(4);
    });

    test("27. Check all keys existence with ?&", async () => {
      const res = await db.query(`SELECT id FROM documents WHERE data ?& '{"author", "specs", "tags"}' ORDER BY id ASC;`);
      expect(res.length).toBe(4);
    });

    test("28. Check all keys existence failing when one key is missing", async () => {
      const res = await db.query(`SELECT id FROM documents WHERE data ?& '{"author", "non_existent"}';`);
      expect(res.length).toBe(0);
    });
  });

  // Section 5: Built-in JSON Functions (JSONB_TYPEOF, JSON_ARRAY_LENGTH, JSONB_STRIP_NULLS)
  describe("Section 5: Built-in JSON Functions", () => {
    test("29. JSONB_TYPEOF on object", async () => {
      const res = await db.query(`SELECT JSONB_TYPEOF(data->'specs') AS t FROM documents WHERE id = 1;`);
      expect(res[0].t).toBe("object");
    });

    test("30. JSONB_TYPEOF on array", async () => {
      const res = await db.query(`SELECT JSONB_TYPEOF(data->'tags') AS t FROM documents WHERE id = 1;`);
      expect(res[0].t).toBe("array");
    });

    test("31. JSONB_TYPEOF on string", async () => {
      const res = await db.query(`SELECT JSONB_TYPEOF(data->'author') AS t FROM documents WHERE id = 1;`);
      expect(res[0].t).toBe("string");
    });

    test("32. JSONB_TYPEOF on number", async () => {
      const res = await db.query(`SELECT JSONB_TYPEOF(data->'specs'->'version') AS t FROM documents WHERE id = 1;`);
      expect(res[0].t).toBe("number");
    });

    test("33. JSONB_TYPEOF on boolean", async () => {
      const res = await db.query(`SELECT JSONB_TYPEOF(data->'specs'->'draft') AS t FROM documents WHERE id = 1;`);
      expect(res[0].t).toBe("boolean");
    });

    test("34. JSON_ARRAY_LENGTH on non-empty array", async () => {
      const res = await db.query(`SELECT JSON_ARRAY_LENGTH(data->'tags') AS len FROM documents WHERE id = 1;`);
      expect(res[0].len).toBe(3);
    });

    test("35. JSONB_ARRAY_LENGTH on ratings array", async () => {
      const res = await db.query(`SELECT JSONB_ARRAY_LENGTH(data->'specs'->'ratings') AS len FROM documents WHERE id = 1;`);
      expect(res[0].len).toBe(3);
    });

    test("36. JSON_ARRAY_LENGTH on empty array", async () => {
      const res = await db.query(`SELECT JSON_ARRAY_LENGTH(data->'specs'->'ratings') AS len FROM documents WHERE id = 4;`);
      expect(res[0].len).toBe(0);
    });

    test("37. JSONB_STRIP_NULLS removes null properties", async () => {
      const res = await db.query(`SELECT JSONB_STRIP_NULLS(data->'details') AS cleaned FROM documents WHERE id = 1;`);
      expect(res[0].cleaned).toEqual({ active: true });
    });

    test("38. JSON_EXTRACT_PATH function", async () => {
      const res = await db.query(`SELECT JSON_EXTRACT_PATH(data, 'specs', 'version') AS ver FROM documents WHERE id = 1;`);
      expect(res[0].ver).toBe(2);
    });

    test("39. JSONB_EXTRACT_PATH_TEXT function", async () => {
      const res = await db.query(`SELECT JSONB_EXTRACT_PATH_TEXT(data, 'specs', 'version') AS ver FROM documents WHERE id = 1;`);
      expect(res[0].ver).toBe("2");
    });

    test("40. JSONB_PRETTY formats JSON with whitespace", async () => {
      const res = await db.query(`SELECT JSONB_PRETTY(data->'specs') AS pretty FROM documents WHERE id = 2;`);
      const prettyStr = typeof res[0].pretty === "string" ? res[0].pretty : JSON.stringify(res[0].pretty, null, 2);
      expect(prettyStr).toContain("\n");
      expect(prettyStr).toContain("draft");
    });
  });

  // Section 6: JSON Construction & Mutation (JSON_BUILD_OBJECT, JSON_BUILD_ARRAY, JSONB_SET)
  describe("Section 6: JSON Construction & Mutation", () => {
    test("41. JSON_BUILD_OBJECT constructs JSON from key-value pairs", async () => {
      const res = await db.query(`SELECT JSON_BUILD_OBJECT('id', id, 'title', title) AS obj FROM documents WHERE id = 1;`);
      expect(res[0].obj).toEqual({ id: 1, title: "Tech Spec" });
    });

    test("42. JSONB_BUILD_OBJECT with nested values", async () => {
      const res = await db.query(`SELECT JSONB_BUILD_OBJECT('doc_id', id, 'author', data->>'author') AS obj FROM documents WHERE id = 1;`);
      expect(res[0].obj).toEqual({ doc_id: 1, author: "Alice" });
    });

    test("43. JSON_BUILD_ARRAY constructs JSON array from arguments", async () => {
      const res = await db.query(`SELECT JSON_BUILD_ARRAY(id, title, status) AS arr FROM documents WHERE id = 1;`);
      expect(res[0].arr).toEqual([1, "Tech Spec", "published"]);
    });

    test("44. JSONB_BUILD_ARRAY with mixed expressions", async () => {
      const res = await db.query(`SELECT JSONB_BUILD_ARRAY(data->>'author', meta->'views') AS arr FROM documents WHERE id = 1;`);
      expect(res[0].arr).toEqual(["Alice", 1200]);
    });

    test("45. JSONB_SET updates an existing scalar property", async () => {
      const res = await db.query(`SELECT JSONB_SET(data, '{"author"}', '"Alicia"') AS updated FROM documents WHERE id = 1;`);
      expect((res[0].updated as any).author).toBe("Alicia");
    });

    test("46. JSONB_SET updates a nested property", async () => {
      const res = await db.query(`SELECT JSONB_SET(data, '{"specs", "version"}', '99') AS updated FROM documents WHERE id = 1;`);
      expect((res[0].updated as any).specs.version).toBe(99);
    });

    test("47. JSONB_SET appends to object when create_missing is true", async () => {
      const res = await db.query(`SELECT JSONB_SET(data, '{"specs", "reviewed"}', 'true', true) AS updated FROM documents WHERE id = 1;`);
      expect((res[0].updated as any).specs.reviewed).toBe(true);
    });
  });

  // Section 7: Filtering with JSON in WHERE Clause
  describe("Section 7: Filtering with JSON in WHERE Clause", () => {
    test("48. WHERE ->> string equals", async () => {
      const res = await db.query(`SELECT id, title FROM documents WHERE data->>'author' = 'Alice' ORDER BY id ASC;`);
      expect(res.length).toBe(2);
      expect(res[0].id).toBe(1);
      expect(res[1].id).toBe(4);
    });

    test("49. WHERE ->> numeric comparison", async () => {
      const res = await db.query(`SELECT id, title FROM documents WHERE (data->'specs'->>'version')::int >= 2 ORDER BY id ASC;`);
      expect(res.length).toBe(2);
      expect(res[0].id).toBe(1);
      expect(res[1].id).toBe(3);
    });

    test("50. WHERE ->> boolean comparison", async () => {
      const res = await db.query(`SELECT id, title FROM documents WHERE data->'specs'->>'draft' = 'true';`);
      expect(res.length).toBe(1);
      expect(res[0].id).toBe(2);
    });

    test("51. WHERE ->> with LIKE pattern matching", async () => {
      const res = await db.query(`SELECT id, title FROM documents WHERE data->>'author' LIKE 'A%';`);
      expect(res.length).toBe(2);
    });

    test("52. WHERE ->> with ILIKE case-insensitive matching", async () => {
      const res = await db.query(`SELECT id, title FROM documents WHERE data->>'author' ILIKE 'alice';`);
      expect(res.length).toBe(2);
    });

    test("53. WHERE ->> combined with regular column filter", async () => {
      const res = await db.query(`SELECT id FROM documents WHERE status = 'published' AND data->>'author' = 'Alice';`);
      expect(res.length).toBe(1);
      expect(res[0].id).toBe(1);
    });

    test("54. WHERE ->> with IN list", async () => {
      const res = await db.query(`SELECT id FROM documents WHERE data->>'author' IN ('Bob', 'Charlie') ORDER BY id ASC;`);
      expect(res.length).toBe(2);
      expect(res[0].id).toBe(2);
      expect(res[1].id).toBe(3);
    });

    test("55. WHERE ->> IS NOT NULL", async () => {
      const res = await db.query(`SELECT id FROM documents WHERE data->>'author' IS NOT NULL ORDER BY id ASC;`);
      expect(res.length).toBe(4);
    });

    test("56. WHERE ->> IS NULL", async () => {
      const res = await db.query(`SELECT id FROM documents WHERE data->>'author' IS NULL;`);
      expect(res.length).toBe(1);
      expect(res[0].id).toBe(5);
    });
  });

  // Section 8: Sorting, Grouping & Aggregations with JSONB
  describe("Section 8: Sorting, Grouping & Aggregations with JSONB", () => {
    test("57. ORDER BY data->>'author' ASC", async () => {
      const res = await db.query(`SELECT id, data->>'author' AS author FROM documents WHERE data->>'author' IS NOT NULL ORDER BY data->>'author' ASC, id ASC;`);
      expect(res[0].author).toBe("Alice");
      expect(res[1].author).toBe("Alice");
      expect(res[2].author).toBe("Bob");
      expect(res[3].author).toBe("Charlie");
    });

    test("58. ORDER BY (data->'specs'->>'version')::int DESC", async () => {
      const res = await db.query(`SELECT id, (data->'specs'->>'version')::int AS ver FROM documents WHERE data->'specs'->>'version' IS NOT NULL ORDER BY ver DESC;`);
      expect(res[0].ver).toBe(3);
      expect(res[1].ver).toBe(2);
    });

    test("59. GROUP BY data->>'author' with COUNT(*)", async () => {
      const res = await db.query(`SELECT data->>'author' AS author, COUNT(*) AS doc_count FROM documents WHERE data->>'author' IS NOT NULL GROUP BY data->>'author' ORDER BY author ASC;`);
      expect(res.length).toBe(3);
      expect(res[0].author).toBe("Alice");
      expect(res[0].doc_count).toBe(2);
      expect(res[1].author).toBe("Bob");
      expect(res[1].doc_count).toBe(1);
    });

    test("60. JSON_AGG aggregate function", async () => {
      const res = await db.query(`SELECT JSON_AGG(title) AS all_titles FROM documents WHERE status = 'published';`);
      expect(res[0].all_titles).toEqual(["Tech Spec", "API Guide"]);
    });

    test("61. JSONB_AGG aggregate with JSON_BUILD_OBJECT", async () => {
      const res = await db.query(`SELECT JSONB_AGG(JSON_BUILD_OBJECT('id', id, 'title', title)) AS docs FROM documents WHERE status = 'published';`);
      expect(res[0].docs).toEqual([
        { id: 1, title: "Tech Spec" },
        { id: 3, title: "API Guide" }
      ]);
    });
  });

  // Section 9: JSONB Updates & Data Modifications
  describe("Section 9: JSONB Updates & Data Modifications", () => {
    test("62. UPDATE full JSONB column with new object literal", async () => {
      await db.query(`UPDATE documents SET data = '{"author": "David", "tags": ["new"]}' WHERE id = 5;`);
      const res = await db.query(`SELECT data->>'author' AS author FROM documents WHERE id = 5;`);
      expect(res[0].author).toBe("David");
    });

    test("63. UPDATE JSONB using JSONB_SET expression", async () => {
      await db.query(`UPDATE documents SET data = JSONB_SET(data, '{"specs", "version"}', '10') WHERE id = 2;`);
      const res = await db.query(`SELECT data->'specs'->>'version' AS ver FROM documents WHERE id = 2;`);
      expect(res[0].ver).toBe("10");
    });

    test("64. DELETE using JSONB containment WHERE condition", async () => {
      await db.query(`INSERT INTO documents (id, title, data, status) VALUES (99, 'Temp', '{"temp": true}', 'draft');`);
      const del = await db.query(`DELETE FROM documents WHERE data @> '{"temp": true}';`);
      expect(del.length).toBe(0);
      const check = await db.query(`SELECT * FROM documents WHERE id = 99;`);
      expect(check.length).toBe(0);
    });

    test("65. DELETE using JSON key existence WHERE condition", async () => {
      await db.query(`INSERT INTO documents (id, title, data, status) VALUES (98, 'To Delete', '{"marked_for_delete": true}', 'draft');`);
      await db.query(`DELETE FROM documents WHERE data ? 'marked_for_delete';`);
      const check = await db.query(`SELECT * FROM documents WHERE id = 98;`);
      expect(check.length).toBe(0);
    });
  });

  // Section 10: Multi-Table Queries & JOINs with JSONB
  describe("Section 10: Multi-Table Queries & JOINs with JSONB", () => {
    beforeAll(async () => {
      await db.query(`
        CREATE TABLE authors (
          name TEXT PRIMARY KEY,
          email TEXT,
          country TEXT
        );
      `);

      await db.query(`
        INSERT INTO authors (name, email, country) VALUES
        ('Alice', 'alice@example.com', 'US'),
        ('Bob', 'bob@example.com', 'UK'),
        ('Charlie', 'charlie@example.com', 'VN'),
        ('David', 'david@example.com', 'DE');
      `);
    });

    test("66. JOIN documents on JSON extracted author matching authors.name", async () => {
      const res = await db.query(`
        SELECT d.id, d.title, a.email, a.country
        FROM documents d
        JOIN authors a ON d.data->>'author' = a.name
        WHERE d.id = 1;
      `);
      expect(res.length).toBe(1);
      expect(res[0].email).toBe("alice@example.com");
      expect(res[0].country).toBe("US");
    });

    test("67. LEFT JOIN documents with authors and JSON_BUILD_OBJECT projection", async () => {
      const res = await db.query(`
        SELECT d.id, JSON_BUILD_OBJECT('title', d.title, 'author_email', a.email) AS doc_author
        FROM documents d
        LEFT JOIN authors a ON d.data->>'author' = a.name
        WHERE d.id = 1;
      `);
      expect(res[0].doc_author).toEqual({
        title: "Tech Spec",
        author_email: "alice@example.com"
      });
    });

    test("68. Correlated aggregation combining tables and JSONB fields", async () => {
      const res = await db.query(`
        SELECT a.country, COUNT(d.id) AS total_docs
        FROM authors a
        JOIN documents d ON d.data->>'author' = a.name
        GROUP BY a.country
        ORDER BY a.country ASC;
      `);
      expect(res.length).toBe(4);
    });
  });

  // Section 11: Complex Edge Cases & Nested Structures
  describe("Section 11: Complex Edge Cases & Nested Structures", () => {
    test("69. Empty array extraction returns null", async () => {
      const res = await db.query(`SELECT data->'specs'->'ratings'->>0 AS r FROM documents WHERE id = 4;`);
      expect(res[0].r).toBeNull();
    });

    test("70. Multiple JSON operators in a single SELECT projection", async () => {
      const res = await db.query(`
        SELECT
          data->>'author' AS author,
          data->'specs'->>'version' AS ver,
          data->'tags'->>0 AS first_tag,
          JSONB_TYPEOF(data->'specs') AS specs_type
        FROM documents WHERE id = 1;
      `);
      expect(res[0].author).toBe("Alice");
      expect(res[0].ver).toBe("2");
      expect(res[0].first_tag).toBe("db");
      expect(res[0].specs_type).toBe("object");
    });

    test("71. Chained JSON operators in WHERE filter with AND logic", async () => {
      const res = await db.query(`
        SELECT id FROM documents
        WHERE data->>'author' = 'Alice'
          AND data->'specs'->>'version' = '2';
      `);
      expect(res.length).toBe(1);
      expect(res[0].id).toBe(1);
    });

    test("72. Chained JSON operators in WHERE filter with OR logic", async () => {
      const res = await db.query(`
        SELECT id FROM documents
        WHERE data->>'author' = 'Bob'
           OR data->>'author' = 'Charlie'
        ORDER BY id ASC;
      `);
      expect(res.length).toBe(2);
      expect(res[0].id).toBe(2);
      expect(res[1].id).toBe(3);
    });

    test("73. JSON extraction with CASE WHEN expression", async () => {
      const res = await db.query(`
        SELECT id,
          CASE
            WHEN data->>'author' = 'Alice' THEN 'Senior Author'
            ELSE 'Contributor'
          END AS role
        FROM documents WHERE id = 1;
      `);
      expect(res[0].role).toBe("Senior Author");
    });

    test("74. JSON extraction with COALESCE fallback", async () => {
      const res = await db.query(`
        SELECT id, COALESCE(data->>'non_existent', 'DefaultValue') AS val
        FROM documents WHERE id = 1;
      `);
      expect(res[0].val).toBe("DefaultValue");
    });

    test("75. JSON extraction with string concatenation ||", async () => {
      const res = await db.query(`
        SELECT id, 'Author: ' || (data->>'author') AS formatted
        FROM documents WHERE id = 1;
      `);
      expect(res[0].formatted).toBe("Author: Alice");
    });

    test("76. JSONB with special characters and Vietnamese diacritics", async () => {
      await db.query(`
        INSERT INTO documents (id, title, data, status)
        VALUES (76, 'Hồ Sơ', '{"tác_giả": "Nguyễn Văn A", "nơi_ở": "Hà Nội", "chi_tiết": {"tuổi": 28}}', 'published');
      `);
      const res = await db.query(`SELECT data->>'tác_giả' AS tg, data->'chi_tiết'->>'tuổi' AS tuoi FROM documents WHERE id = 76;`);
      expect(res[0].tg).toBe("Nguyễn Văn A");
      expect(res[0].tuoi).toBe("28");
    });

    test("77. Containment check on Vietnamese diacritics", async () => {
      const res = await db.query(`SELECT id FROM documents WHERE data @> '{"nơi_ở": "Hà Nội"}';`);
      expect(res.length).toBe(1);
      expect(res[0].id).toBe(76);
    });

    test("78. JSON key existence on unicode Vietnamese keys", async () => {
      const res = await db.query(`SELECT id FROM documents WHERE data ? 'tác_giả';`);
      expect(res.length).toBe(1);
      expect(res[0].id).toBe(76);
    });

    test("79. JSONB array of objects insertion and query", async () => {
      await db.query(`
        INSERT INTO documents (id, title, data, status)
        VALUES (79, 'Order', '{"items": [{"name": "Book", "price": 20}, {"name": "Pen", "price": 5}]}', 'published');
      `);
      const res = await db.query(`SELECT data->'items'->0->>'name' AS first_item, data->'items'->1->>'price' AS second_price FROM documents WHERE id = 79;`);
      expect(res[0].first_item).toBe("Book");
      expect(res[0].second_price).toBe("5");
    });

    test("80. Deep nested JSON path with numeric array indices", async () => {
      const res = await db.query(`SELECT data#>>'{"items", "0", "price"}' AS price FROM documents WHERE id = 79;`);
      expect(res[0].price).toBe("20");
    });

    test("81. JSONB array length on array of objects", async () => {
      const res = await db.query(`SELECT JSON_ARRAY_LENGTH(data->'items') AS count FROM documents WHERE id = 79;`);
      expect(res[0].count).toBe(2);
    });

    test("82. JSONB containment on array of objects", async () => {
      const res = await db.query(`SELECT id FROM documents WHERE data @> '{"items": [{"name": "Book"}]}';`);
      expect(res.length).toBe(1);
      expect(res[0].id).toBe(79);
    });

    test("83. JSONB_SET modifying nested array of objects", async () => {
      const res = await db.query(`SELECT JSONB_SET(data, '{"items", "0", "price"}', '25') AS updated FROM documents WHERE id = 79;`);
      expect((res[0].updated as any).items[0].price).toBe(25);
    });

    test("84. Parameterized query with ->> filter", async () => {
      const res = await db.query(`SELECT id, title FROM documents WHERE data->>'author' = $1;`, ["Charlie"]);
      expect(res.length).toBe(1);
      expect(res[0].id).toBe(3);
    });

    test("85. Parameterized query with @> containment filter", async () => {
      const res = await db.query(`SELECT id, title FROM documents WHERE data @> $1;`, ['{"author": "Charlie"}']);
      expect(res.length).toBe(1);
      expect(res[0].id).toBe(3);
    });

    test("86. Parameterized query with ? key existence filter", async () => {
      const res = await db.query(`SELECT id FROM documents WHERE data ? $1 ORDER BY id ASC;`, ["author"]);
      expect(res.length).toBe(5); // 1, 2, 3, 4, 5(updated to David)
    });

    test("87. Subquery in WHERE filtering by JSON extracted value", async () => {
      const res = await db.query(`
        SELECT id, title FROM documents
        WHERE id IN (
          SELECT id FROM documents WHERE (data->'specs'->>'version')::int >= 2
        ) ORDER BY id ASC;
      `);
      expect(res.length).toBe(3);
    });

    test("88. DISTINCT JSON extracted authors", async () => {
      const res = await db.query(`SELECT DISTINCT data->>'author' AS author FROM documents WHERE data->>'author' IS NOT NULL ORDER BY author ASC;`);
      expect(res.length).toBe(4); // Alice, Bob, Charlie, David
    });

    test("89. LIMIT and OFFSET with JSON extracted column sorting", async () => {
      const res = await db.query(`
        SELECT id, data->>'author' AS author
        FROM documents
        WHERE data->>'author' IS NOT NULL
        ORDER BY author ASC, id ASC
        LIMIT 2 OFFSET 1;
      `);
      expect(res.length).toBe(2);
      expect(res[0].id).toBe(4);
    });

    test("90. JSONB column in CTE (Common Table Expression)", async () => {
      const res = await db.query(`
        WITH RankedDocs AS (
          SELECT id, title, data->>'author' AS author
          FROM documents
          WHERE data->>'author' IS NOT NULL
        )
        SELECT * FROM RankedDocs WHERE author = 'Alice' ORDER BY id ASC;
      `);
      expect(res.length).toBe(2);
    });

    test("91. JSONB boolean check directly in WHERE clause", async () => {
      const res = await db.query(`SELECT id FROM documents WHERE (data->'specs'->>'draft')::boolean = true;`);
      expect(res.length).toBeGreaterThan(0);
    });

    test("92. JSONB null property equality check", async () => {
      const res = await db.query(`SELECT id FROM documents WHERE data->'details'->>'null_val' IS NULL;`);
      expect(res.length).toBe(7);
    });

    test("93. Multi-column JSON extraction in single row insert returning", async () => {
      await db.query(`
        INSERT INTO documents (id, title, data, status)
        VALUES (93, 'Return Test', '{"score": 100, "grade": "A"}', 'published');
      `);
      const res = await db.query(`SELECT data->>'score' AS score, data->>'grade' AS grade FROM documents WHERE id = 93;`);
      expect(res[0].score).toBe("100");
      expect(res[0].grade).toBe("A");
    });

    test("94. JSON build object with expression operations", async () => {
      const res = await db.query(`SELECT JSON_BUILD_OBJECT('doubled_id', id * 2, 'upper_title', UPPER(title)) AS obj FROM documents WHERE id = 1;`);
      expect(res[0].obj).toEqual({ doubled_id: 2, upper_title: "TECH SPEC" });
    });

    test("95. JSONB typeof on null returns null", async () => {
      const res = await db.query(`SELECT JSONB_TYPEOF(data->'details'->'null_val') AS t FROM documents WHERE id = 1;`);
      expect(res[0].t).toBe("null");
    });

    test("96. Deeply nested JSON path with 5 levels", async () => {
      await db.query(`
        INSERT INTO documents (id, title, data, status)
        VALUES (96, 'Deep Nest', '{"a": {"b": {"c": {"d": {"e": "found_me"}}}}}', 'published');
      `);
      const res = await db.query(`SELECT data#>>'{"a", "b", "c", "d", "e"}' AS target FROM documents WHERE id = 96;`);
      expect(res[0].target).toBe("found_me");
    });

    test("97. Complex WHERE combining JSON arrow, containment and SQL functions", async () => {
      const res = await db.query(`
        SELECT id FROM documents
        WHERE LENGTH(data->>'author') > 3
          AND data @> '{"specs": {}}'
        ORDER BY id ASC;
      `);
      expect(res.length).toBe(3);
    });

    test("98. Batch JSON updates preserving non-updated keys", async () => {
      await db.query(`
        UPDATE documents
        SET data = JSONB_SET(data, '{"author"}', '"Alice Smith"')
        WHERE id = 1;
      `);
      const res = await db.query(`SELECT data->>'author' AS author, data->'specs'->>'version' AS ver FROM documents WHERE id = 1;`);
      expect(res[0].author).toBe("Alice Smith");
      expect(res[0].ver).toBe("2");
    });

    test("99. JSONB array extraction inside HAVING clause", async () => {
      const res = await db.query(`
        SELECT data->>'author' AS author, COUNT(*) AS count
        FROM documents
        WHERE data->>'author' IS NOT NULL
        GROUP BY data->>'author'
        HAVING COUNT(*) >= 1
        ORDER BY author ASC;
      `);
      expect(res.length).toBe(5);
    });

    test("100. Verification of full JSONB document round-trip integrity", async () => {
      const res = await db.query(`SELECT * FROM documents WHERE id = 1;`);
      expect(res.length).toBe(1);
      expect(typeof res[0].data).toBe("object");
      expect(res[0].data.author).toBe("Alice Smith");
      expect(Array.isArray(res[0].data.tags)).toBe(true);
      expect(res[0].data.tags).toContain("rust");
    });
  });
});
