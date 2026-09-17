import { describe, test, expect, beforeAll, afterAll } from "bun:test";
import { PGLite } from "../src/index";

describe("100 Comprehensive In-Depth PostgreSQL UUID Test Cases for PGLite", () => {
  let db: PGLite;

  // Fixed test UUID constants
  const UUID_1 = "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11";
  const UUID_2 = "b1ffcd00-ad1c-4ef9-cc7e-7ccaae491b22";
  const UUID_3 = "c200de11-be2d-4f0a-dd8f-8ddbbf502c33";
  const UUID_4 = "d311ef22-cf3e-401b-ee90-9eecce613d44";
  const UUID_5 = "e422f033-d04f-412c-ff01-0ffddf724e55";

  beforeAll(async () => {
    db = new PGLite(":memory:", { native: true });

    // Base tables for testing UUID features
    await db.query(`
      CREATE TABLE organizations (
        org_id UUID PRIMARY KEY DEFAULT GEN_RANDOM_UUID(),
        org_name TEXT NOT NULL,
        slug TEXT UNIQUE,
        created_at TEXT
      );
    `);

    await db.query(`
      CREATE TABLE accounts (
        account_id UUID PRIMARY KEY,
        org_id UUID REFERENCES organizations(org_id),
        username TEXT NOT NULL,
        email TEXT,
        is_active BOOLEAN DEFAULT true,
        extra_ids UUID[]
      );
    `);

    await db.query(`
      CREATE TABLE audit_logs (
        log_id UUID PRIMARY KEY DEFAULT UUID_GENERATE_V4(),
        account_id UUID,
        action TEXT NOT NULL,
        metadata TEXT
      );
    `);

    // Seed test data with known UUIDs
    await db.query(`
      INSERT INTO organizations (org_id, org_name, slug, created_at) VALUES
      ('${UUID_1}', 'Acme Corporation', 'acme', '2026-01-01'),
      ('${UUID_2}', 'Globex Industries', 'globex', '2026-02-01'),
      ('${UUID_3}', 'Initech LLC', 'initech', '2026-03-01');
    `);

    await db.query(`
      INSERT INTO accounts (account_id, org_id, username, email, is_active, extra_ids) VALUES
      ('${UUID_1}', '${UUID_1}', 'alice', 'alice@acme.com', true, ARRAY['${UUID_4}'::uuid, '${UUID_5}'::uuid]),
      ('${UUID_2}', '${UUID_1}', 'bob', 'bob@acme.com', true, ARRAY['${UUID_5}'::uuid]),
      ('${UUID_3}', '${UUID_2}', 'charlie', 'charlie@globex.com', false, '{}'),
      ('${UUID_4}', '${UUID_2}', 'david', 'david@globex.com', true, NULL),
      ('${UUID_5}', NULL, 'eve', 'eve@independent.org', false, NULL);
    `);
  });

  afterAll(async () => {
    if (db) {
      await db.close();
    }
  });

  // =========================================================================
  // Section 1: UUID Generation Functions (GEN_RANDOM_UUID, UUID_GENERATE_V4) (Tests 1 - 15)
  // =========================================================================
  describe("Section 1: UUID Generation Functions", () => {
    test("1. GEN_RANDOM_UUID() returns a non-null string", async () => {
      const res = await db.query(`SELECT GEN_RANDOM_UUID() AS uuid;`);
      expect(typeof res[0].uuid).toBe("string");
      expect(res[0].uuid.length).toBe(36);
    });

    test("2. GEN_RANDOM_UUID() matches standard UUID v4 format (8-4-4-4-12)", async () => {
      const res = await db.query(`SELECT GEN_RANDOM_UUID() AS uuid;`);
      const uuidRegex = /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;
      expect(uuidRegex.test(res[0].uuid)).toBe(true);
    });

    test("3. UUID_GENERATE_V4() returns a valid UUID string", async () => {
      const res = await db.query(`SELECT UUID_GENERATE_V4() AS uuid;`);
      expect(typeof res[0].uuid).toBe("string");
      expect(res[0].uuid.length).toBe(36);
      expect(res[0].uuid[14]).toBe("4");
    });

    test("4. GEN_RANDOM_UUID() generated sequentially produces distinct values", async () => {
      const res = await db.query(`SELECT GEN_RANDOM_UUID() AS u1, GEN_RANDOM_UUID() AS u2;`);
      expect(res[0].u1).not.toBe(res[0].u2);
    });

    test("5. Case-insensitive function call gen_random_uuid()", async () => {
      const res = await db.query(`SELECT gen_random_uuid() AS lower_fn;`);
      expect(res[0].lower_fn.length).toBe(36);
    });

    test("6. Case-insensitive function call uuid_generate_v4()", async () => {
      const res = await db.query(`SELECT uuid_generate_v4() AS lower_fn;`);
      expect(res[0].lower_fn.length).toBe(36);
    });

    test("7. GEN_RANDOM_UUID() in SELECT projection with table scan", async () => {
      const res = await db.query(`SELECT org_name, GEN_RANDOM_UUID() AS dynamic_id FROM organizations;`);
      expect(res.length).toBe(3);
      expect(res[0].dynamic_id).not.toBe(res[1].dynamic_id);
    });

    test("8. GEN_RANDOM_UUID() wrapped with UPPER() function", async () => {
      const res = await db.query(`SELECT UPPER(GEN_RANDOM_UUID()) AS upper_uuid;`);
      expect(res[0].upper_uuid).toBe(res[0].upper_uuid.toUpperCase());
      expect(res[0].upper_uuid.length).toBe(36);
    });

    test("9. GEN_RANDOM_UUID() wrapped with LOWER() function", async () => {
      const res = await db.query(`SELECT LOWER(GEN_RANDOM_UUID()) AS lower_uuid;`);
      expect(res[0].lower_uuid).toBe(res[0].lower_uuid.toLowerCase());
    });

    test("10. String concatenation with generated UUID", async () => {
      const res = await db.query(`SELECT 'urn:uuid:' || GEN_RANDOM_UUID() AS urn;`);
      expect(res[0].urn.startsWith("urn:uuid:")).toBe(true);
      expect(res[0].urn.length).toBe(45);
    });

    test("11. Generated UUID inside subquery expression", async () => {
      const res = await db.query(`SELECT (SELECT GEN_RANDOM_UUID()) AS sub_uuid;`);
      expect(typeof res[0].sub_uuid).toBe("string");
      expect(res[0].sub_uuid.length).toBe(36);
    });

    test("12. Generated UUID in CTE query", async () => {
      const res = await db.query(`
        WITH generated AS (
          SELECT GEN_RANDOM_UUID() AS g_id
        )
        SELECT g_id FROM generated;
      `);
      expect(res.length).toBe(1);
      expect(res[0].g_id.length).toBe(36);
    });

    test("13. INSERT into table with DEFAULT GEN_RANDOM_UUID() generates valid PK", async () => {
      await db.query(`INSERT INTO organizations (org_name, slug) VALUES ('Stark Enterprises', 'stark');`);
      const res = await db.query(`SELECT org_id, org_name FROM organizations WHERE slug = 'stark';`);
      expect(res.length).toBe(1);
      expect(typeof res[0].org_id).toBe("string");
      expect(res[0].org_id.length).toBe(36);
    });

    test("14. INSERT using omitted column triggers UUID generation", async () => {
      await db.query(`INSERT INTO organizations (org_name, slug) VALUES ('Wayne Tech', 'wayne');`);
      const res = await db.query(`SELECT org_id FROM organizations WHERE slug = 'wayne';`);
      expect(res.length).toBe(1);
      expect(res[0].org_id.length).toBe(36);
    });

    test("15. INSERT into table with DEFAULT UUID_GENERATE_V4()", async () => {
      await db.query(`INSERT INTO audit_logs (account_id, action) VALUES ('${UUID_1}', 'LOGIN');`);
      const res = await db.query(`SELECT log_id, action FROM audit_logs WHERE account_id = '${UUID_1}';`);
      expect(res.length).toBe(1);
      expect(res[0].log_id.length).toBe(36);
      expect(res[0].action).toBe("LOGIN");
    });
  });

  // =========================================================================
  // Section 2: UUID Column Definition, Storage & Retrievals (Tests 16 - 30)
  // =========================================================================
  describe("Section 2: UUID Column Definition, Storage & Retrievals", () => {
    test("16. UUID column stores exact lowercase UUID string", async () => {
      const res = await db.query(`SELECT org_id FROM organizations WHERE slug = 'acme';`);
      expect(res[0].org_id).toBe(UUID_1);
    });

    test("17. UUID column accepts and preserves uppercase UUID string", async () => {
      const upperUuid = "F533E044-E150-423D-8812-100EFA835F66";
      await db.query(`INSERT INTO accounts (account_id, username, email) VALUES ('${upperUuid}', 'upper_user', 'up@test.com');`);
      const res = await db.query(`SELECT account_id FROM accounts WHERE username = 'upper_user';`);
      expect(res[0].account_id.toLowerCase()).toBe(upperUuid.toLowerCase());
    });

    test("18. UUID column nullable value stores and retrieves NULL", async () => {
      const res = await db.query(`SELECT account_id, org_id FROM accounts WHERE username = 'eve';`);
      expect(res[0].account_id).toBe(UUID_5);
      expect(res[0].org_id).toBeNull();
    });

    test("19. Filter WHERE uuid_column IS NULL", async () => {
      const res = await db.query(`SELECT username FROM accounts WHERE org_id IS NULL;`);
      expect(res.length).toBeGreaterThanOrEqual(1);
      const names = res.map(r => r.username);
      expect(names).toContain("eve");
    });

    test("20. Filter WHERE uuid_column IS NOT NULL", async () => {
      const res = await db.query(`SELECT username FROM accounts WHERE org_id IS NOT NULL;`);
      expect(res.length).toBeGreaterThanOrEqual(4);
    });

    test("21. UUID column with alias projection", async () => {
      const res = await db.query(`SELECT org_id AS company_identifier FROM organizations WHERE slug = 'globex';`);
      expect(res[0].company_identifier).toBe(UUID_2);
    });

    test("22. UUID[] array column stores array of UUID strings", async () => {
      const res = await db.query(`SELECT extra_ids FROM accounts WHERE username = 'alice';`);
      expect(Array.isArray(res[0].extra_ids)).toBe(true);
      expect(res[0].extra_ids.length).toBe(2);
      expect(res[0].extra_ids).toContain(UUID_4);
      expect(res[0].extra_ids).toContain(UUID_5);
    });

    test("23. UUID[] empty array stores as empty JS array", async () => {
      const res = await db.query(`SELECT extra_ids FROM accounts WHERE username = 'charlie';`);
      expect(res[0].extra_ids).toEqual([]);
    });

    test("24. UUID[] NULL array retrieves as null", async () => {
      const res = await db.query(`SELECT extra_ids FROM accounts WHERE username = 'david';`);
      expect(res[0].extra_ids).toBeNull();
    });

    test("25. COALESCE with UUID column returns fallback value when NULL", async () => {
      const res = await db.query(`
        SELECT username, COALESCE(org_id, '${UUID_1}'::uuid) AS resolved_org
        FROM accounts WHERE username = 'eve';
      `);
      expect(res[0].resolved_org).toBe(UUID_1);
    });

    test("26. COALESCE with UUID column preserves existing UUID", async () => {
      const res = await db.query(`
        SELECT username, COALESCE(org_id, '${UUID_5}'::uuid) AS resolved_org
        FROM accounts WHERE username = 'alice';
      `);
      expect(res[0].resolved_org).toBe(UUID_1);
    });

    test("27. CREATE TABLE with multiple UUID columns", async () => {
      await db.query(`
        CREATE TABLE uuid_pairs (
          id UUID PRIMARY KEY,
          source_uuid UUID NOT NULL,
          target_uuid UUID,
          session_id UUID DEFAULT GEN_RANDOM_UUID()
        );
      `);
      await db.query(`
        INSERT INTO uuid_pairs (id, source_uuid, target_uuid)
        VALUES ('${UUID_1}', '${UUID_2}', '${UUID_3}');
      `);
      const res = await db.query(`SELECT * FROM uuid_pairs WHERE id = '${UUID_1}';`);
      expect(res[0].source_uuid).toBe(UUID_2);
      expect(res[0].target_uuid).toBe(UUID_3);
      expect(res[0].session_id.length).toBe(36);
    });

    test("28. UUID column with UNIQUE constraint", async () => {
      await db.query(`
        CREATE TABLE unique_uuid_tbl (
          id SERIAL PRIMARY KEY,
          unique_ref UUID UNIQUE
        );
      `);
      await db.query(`INSERT INTO unique_uuid_tbl (unique_ref) VALUES ('${UUID_1}');`);
      const res = await db.query(`SELECT unique_ref FROM unique_uuid_tbl WHERE id = 1;`);
      expect(res[0].unique_ref).toBe(UUID_1);
    });

    test("29. Information schema verifies UUID column data type", async () => {
      const res = await db.query(`
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_name = 'organizations' AND column_name = 'org_id';
      `);
      expect(res.length).toBe(1);
      expect(res[0].data_type.toUpperCase()).toBe("UUID");
    });

    test("30. Multi-row INSERT with explicit UUID literals", async () => {
      await db.query(`
        CREATE TABLE batch_uuids (id UUID PRIMARY KEY, label TEXT);
        INSERT INTO batch_uuids (id, label) VALUES
        ('${UUID_1}', 'First'),
        ('${UUID_2}', 'Second'),
        ('${UUID_3}', 'Third');
      `);
      const res = await db.query(`SELECT count(*) AS total FROM batch_uuids;`);
      expect(Number(res[0].total)).toBe(3);
    });
  });

  // =========================================================================
  // Section 3: UUID Casting, Conversions & Formatting (Tests 31 - 45)
  // =========================================================================
  describe("Section 3: UUID Casting, Conversions & Formatting", () => {
    test("31. Literal string cast to UUID syntax '...'::uuid", async () => {
      const res = await db.query(`SELECT '${UUID_1}'::uuid AS casted_uuid;`);
      expect(res[0].casted_uuid).toBe(UUID_1);
    });

    test("32. CAST('...' AS UUID) standard SQL syntax", async () => {
      const res = await db.query(`SELECT CAST('${UUID_2}' AS UUID) AS casted_uuid;`);
      expect(res[0].casted_uuid).toBe(UUID_2);
    });

    test("33. UUID to TEXT cast syntax id::text", async () => {
      const res = await db.query(`SELECT org_id::text AS txt_id FROM organizations WHERE slug = 'acme';`);
      expect(typeof res[0].txt_id).toBe("string");
      expect(res[0].txt_id).toBe(UUID_1);
    });

    test("34. UUID to VARCHAR cast syntax id::varchar", async () => {
      const res = await db.query(`SELECT org_id::varchar AS vc_id FROM organizations WHERE slug = 'acme';`);
      expect(res[0].vc_id).toBe(UUID_1);
    });

    test("35. CAST(NULL AS UUID) yields null", async () => {
      const res = await db.query(`SELECT CAST(NULL AS UUID) AS null_uuid;`);
      expect(res[0].null_uuid).toBeNull();
    });

    test("36. LENGTH() on UUID cast to text returns 36", async () => {
      const res = await db.query(`SELECT LENGTH(org_id::text) AS uuid_len FROM organizations WHERE slug = 'acme';`);
      expect(Number(res[0].uuid_len)).toBe(36);
    });

    test("37. SUBSTRING() extracting first 8 hex characters from UUID", async () => {
      const res = await db.query(`SELECT SUBSTRING(org_id::text, 1, 8) AS prefix FROM organizations WHERE slug = 'acme';`);
      expect(res[0].prefix).toBe("a0eebc99");
    });

    test("38. REPLACE() stripping hyphens to get 32-char hex string", async () => {
      const res = await db.query(`SELECT REPLACE(org_id::text, '-', '') AS compact_uuid FROM organizations WHERE slug = 'acme';`);
      expect(res[0].compact_uuid).toBe("a0eebc999c0b4ef8bb6d6bb9bd380a11");
      expect(res[0].compact_uuid.length).toBe(32);
    });

    test("39. Parameterized query passing UUID string $1", async () => {
      const res = await db.query(`SELECT username FROM accounts WHERE account_id = $1;`, [UUID_1]);
      expect(res.length).toBe(1);
      expect(res[0].username).toBe("alice");
    });

    test("40. Parameterized query passing multiple UUIDs $1, $2", async () => {
      const res = await db.query(`
        SELECT username FROM accounts WHERE account_id IN ($1, $2) ORDER BY username ASC;
      `, [UUID_1, UUID_2]);
      expect(res.length).toBe(2);
      expect(res[0].username).toBe("alice");
      expect(res[1].username).toBe("bob");
    });

    test("41. NULLIF with matching UUIDs returns NULL", async () => {
      const res = await db.query(`SELECT NULLIF('${UUID_1}'::uuid, '${UUID_1}'::uuid) AS null_res;`);
      expect(res[0].null_res).toBeNull();
    });

    test("42. NULLIF with non-matching UUIDs returns first UUID", async () => {
      const res = await db.query(`SELECT NULLIF('${UUID_1}'::uuid, '${UUID_2}'::uuid) AS val_res;`);
      expect(res[0].val_res).toBe(UUID_1);
    });

    test("43. CASE WHEN evaluating UUID equality", async () => {
      const res = await db.query(`
        SELECT username,
          CASE
            WHEN org_id = '${UUID_1}'::uuid THEN 'Acme Member'
            WHEN org_id = '${UUID_2}'::uuid THEN 'Globex Member'
            ELSE 'External'
          END AS member_type
        FROM accounts
        WHERE username IN ('alice', 'charlie', 'eve')
        ORDER BY username ASC;
      `);
      expect(res.length).toBe(3);
      expect(res[0].member_type).toBe("Acme Member");
      expect(res[1].member_type).toBe("Globex Member");
      expect(res[2].member_type).toBe("External");
    });

    test("44. ARRAY of UUIDs constructor ARRAY['...'::uuid, '...'::uuid]", async () => {
      const res = await db.query(`SELECT ARRAY['${UUID_1}'::uuid, '${UUID_2}'::uuid] AS uuid_list;`);
      expect(res[0].uuid_list).toEqual([UUID_1, UUID_2]);
    });

    test("45. Parameterized query passing UUID array parameter $1", async () => {
      const res = await db.query(`SELECT username FROM accounts WHERE extra_ids && $1;`, [[UUID_4]]);
      expect(res.length).toBeGreaterThanOrEqual(1);
      expect(res[0].username).toBe("alice");
    });
  });

  // =========================================================================
  // Section 4: UUID Filtering, Comparisons & Sorting (Tests 46 - 65)
  // =========================================================================
  describe("Section 4: UUID Filtering, Comparisons & Sorting", () => {
    test("46. Exact equality filter WHERE account_id = '...'", async () => {
      const res = await db.query(`SELECT username FROM accounts WHERE account_id = '${UUID_1}';`);
      expect(res.length).toBe(1);
      expect(res[0].username).toBe("alice");
    });

    test("47. Inequality filter WHERE account_id != '...'", async () => {
      const res = await db.query(`SELECT username FROM accounts WHERE account_id != '${UUID_1}' AND account_id IS NOT NULL;`);
      const usernames = res.map(r => r.username);
      expect(usernames).not.toContain("alice");
      expect(usernames).toContain("bob");
    });

    test("48. Inequality operator <> on UUID", async () => {
      const res = await db.query(`SELECT username FROM accounts WHERE account_id <> '${UUID_1}' AND account_id IS NOT NULL;`);
      const usernames = res.map(r => r.username);
      expect(usernames).not.toContain("alice");
      expect(usernames).toContain("bob");
    });

    test("49. Case-insensitive comparison between lower and upper UUID literals", async () => {
      const upper1 = UUID_1.toUpperCase();
      const res = await db.query(`SELECT username FROM accounts WHERE LOWER(account_id::text) = LOWER('${upper1}');`);
      expect(res.length).toBe(1);
      expect(res[0].username).toBe("alice");
    });

    test("50. WHERE id IN ('...', '...') list matching", async () => {
      const res = await db.query(`
        SELECT username FROM accounts WHERE account_id IN ('${UUID_1}', '${UUID_3}') ORDER BY username ASC;
      `);
      expect(res.length).toBe(2);
      expect(res[0].username).toBe("alice");
      expect(res[1].username).toBe("charlie");
    });

    test("51. WHERE id NOT IN ('...', '...') exclusion", async () => {
      const res = await db.query(`
        SELECT username FROM accounts WHERE account_id NOT IN ('${UUID_1}', '${UUID_2}') ORDER BY username ASC;
      `);
      const names = res.map(r => r.username);
      expect(names).not.toContain("alice");
      expect(names).not.toContain("bob");
      expect(names).toContain("charlie");
    });

    test("52. Greater than comparison > on UUID strings", async () => {
      const res = await db.query(`SELECT org_id FROM organizations WHERE org_id > '${UUID_1}' AND slug IN ('acme', 'globex', 'initech') ORDER BY org_id ASC;`);
      expect(res.length).toBe(2);
      expect(res[0].org_id).toBe(UUID_2);
    });

    test("53. Less than comparison < on UUID strings", async () => {
      const res = await db.query(`SELECT org_id FROM organizations WHERE org_id < '${UUID_3}' AND slug IN ('acme', 'globex', 'initech') ORDER BY org_id ASC;`);
      expect(res.length).toBe(2);
      expect(res[0].org_id).toBe(UUID_1);
      expect(res[1].org_id).toBe(UUID_2);
    });

    test("54. ORDER BY uuid_column ASC", async () => {
      const res = await db.query(`SELECT org_id FROM organizations WHERE slug IN ('acme', 'globex', 'initech') ORDER BY org_id ASC;`);
      expect(res[0].org_id).toBe(UUID_1);
      expect(res[1].org_id).toBe(UUID_2);
      expect(res[2].org_id).toBe(UUID_3);
    });

    test("55. ORDER BY uuid_column DESC", async () => {
      const res = await db.query(`SELECT org_id FROM organizations WHERE slug IN ('acme', 'globex', 'initech') ORDER BY org_id DESC;`);
      expect(res[0].org_id).toBe(UUID_3);
      expect(res[1].org_id).toBe(UUID_2);
      expect(res[2].org_id).toBe(UUID_1);
    });

    test("56. DISTINCT on UUID column deduplicates repeated values", async () => {
      const res = await db.query(`SELECT DISTINCT org_id FROM accounts WHERE org_id IS NOT NULL ORDER BY org_id ASC;`);
      expect(res.length).toBe(2);
      expect(res[0].org_id).toBe(UUID_1);
      expect(res[1].org_id).toBe(UUID_2);
    });

    test("57. WHERE id BETWEEN '...' AND '...' range filter", async () => {
      const res = await db.query(`
        SELECT org_id FROM organizations WHERE org_id BETWEEN '${UUID_1}' AND '${UUID_2}' AND slug IN ('acme', 'globex', 'initech') ORDER BY org_id ASC;
      `);
      expect(res.length).toBe(2);
      expect(res[0].org_id).toBe(UUID_1);
      expect(res[1].org_id).toBe(UUID_2);
    });

    test("58. LIKE prefix matching on UUID string 'a%'", async () => {
      const res = await db.query(`SELECT account_id, username FROM accounts WHERE account_id::text LIKE 'a0ee%';`);
      expect(res.length).toBe(1);
      expect(res[0].username).toBe("alice");
    });

    test("59. Regex matching ~ on UUID string", async () => {
      const res = await db.query(`SELECT account_id FROM accounts WHERE account_id::text ~ '^b1ff';`);
      expect(res.length).toBe(1);
      expect(res[0].account_id).toBe(UUID_2);
    });

    test("60. Compound filter with AND / OR on UUID and boolean", async () => {
      const res = await db.query(`
        SELECT username FROM accounts
        WHERE (org_id = '${UUID_1}' OR org_id = '${UUID_2}') AND is_active = true
        ORDER BY username ASC;
      `);
      expect(res.length).toBe(3);
      expect(res.map(r => r.username)).toEqual(["alice", "bob", "david"]);
    });

    test("61. Subquery scalar comparison WHERE org_id = (SELECT org_id ...)", async () => {
      const res = await db.query(`
        SELECT username FROM accounts
        WHERE org_id = (SELECT org_id FROM organizations WHERE slug = 'acme')
        ORDER BY username ASC;
      `);
      expect(res.length).toBe(2);
      expect(res[0].username).toBe("alice");
      expect(res[1].username).toBe("bob");
    });

    test("62. Subquery IN filter WHERE org_id IN (SELECT org_id ...)", async () => {
      const res = await db.query(`
        SELECT username FROM accounts
        WHERE org_id IN (SELECT org_id FROM organizations WHERE slug IN ('acme', 'globex'))
        ORDER BY username ASC;
      `);
      expect(res.length).toBe(4);
    });

    test("63. Correlated EXISTS subquery with UUID foreign key", async () => {
      const res = await db.query(`
        SELECT o.org_name
        FROM organizations o
        WHERE EXISTS (SELECT 1 FROM accounts a WHERE a.org_id = o.org_id AND a.is_active = false);
      `);
      expect(res.length).toBe(1);
      expect(res[0].org_name).toBe("Globex Industries");
    });

    test("64. Correlated NOT EXISTS subquery with UUID", async () => {
      const res = await db.query(`
        SELECT o.org_name
        FROM organizations o
        WHERE NOT EXISTS (SELECT 1 FROM accounts a WHERE a.org_id = o.org_id);
      `);
      expect(res.length).toBeGreaterThanOrEqual(1);
      const names = res.map(r => r.org_name);
      expect(names).toContain("Initech LLC");
    });

    test("65. LIMIT and OFFSET on UUID ordered results", async () => {
      const res = await db.query(`SELECT org_id FROM organizations WHERE slug IN ('acme', 'globex', 'initech') ORDER BY org_id ASC LIMIT 2 OFFSET 1;`);
      expect(res.length).toBe(2);
      expect(res[0].org_id).toBe(UUID_2);
      expect(res[1].org_id).toBe(UUID_3);
    });
  });

  // =========================================================================
  // Section 5: Primary Keys, Foreign Keys, Joins & Relations (Tests 66 - 80)
  // =========================================================================
  describe("Section 5: Primary Keys, Foreign Keys, Joins & Relations", () => {
    test("66. Direct O(1) PK index scan with UUID key", async () => {
      const res = await db.query(`SELECT org_name FROM organizations WHERE org_id = '${UUID_1}';`);
      expect(res.length).toBe(1);
      expect(res[0].org_name).toBe("Acme Corporation");
    });

    test("67. PK index scan returning empty on non-existent UUID", async () => {
      const res = await db.query(`SELECT org_name FROM organizations WHERE org_id = '00000000-0000-0000-0000-000000000000';`);
      expect(res.length).toBe(0);
    });

    test("68. INNER JOIN matching UUID primary key and foreign key", async () => {
      const res = await db.query(`
        SELECT a.username, o.org_name
        FROM accounts a
        INNER JOIN organizations o ON a.org_id = o.org_id
        ORDER BY a.username ASC;
      `);
      expect(res.length).toBe(4);
      expect(res[0].username).toBe("alice");
      expect(res[0].org_name).toBe("Acme Corporation");
    });

    test("69. LEFT JOIN preserving accounts with NULL org_id", async () => {
      const res = await db.query(`
        SELECT a.username, o.org_name
        FROM accounts a
        LEFT JOIN organizations o ON a.org_id = o.org_id
        WHERE a.username = 'eve';
      `);
      expect(res.length).toBe(1);
      expect(res[0].username).toBe("eve");
      expect(res[0].org_name).toBeNull();
    });

    test("70. LEFT JOIN finding organizations with zero accounts", async () => {
      const res = await db.query(`
        SELECT o.org_name, a.account_id
        FROM organizations o
        LEFT JOIN accounts a ON o.org_id = a.org_id
        WHERE a.account_id IS NULL AND o.slug = 'initech';
      `);
      expect(res.length).toBe(1);
      expect(res[0].org_name).toBe("Initech LLC");
    });

    test("71. Self JOIN via UUID parent_id relationship", async () => {
      await db.query(`
        CREATE TABLE categories (
          cat_id UUID PRIMARY KEY,
          parent_id UUID,
          name TEXT
        );
        INSERT INTO categories (cat_id, parent_id, name) VALUES
        ('${UUID_1}', NULL, 'Root Category'),
        ('${UUID_2}', '${UUID_1}', 'Sub Category 1'),
        ('${UUID_3}', '${UUID_1}', 'Sub Category 2');
      `);
      const res = await db.query(`
        SELECT child.name AS child_name, parent.name AS parent_name
        FROM categories child
        INNER JOIN categories parent ON child.parent_id = parent.cat_id
        ORDER BY child.name ASC;
      `);
      expect(res.length).toBe(2);
      expect(res[0].child_name).toBe("Sub Category 1");
      expect(res[0].parent_name).toBe("Root Category");
    });

    test("72. Three-table JOIN chained across UUID foreign keys", async () => {
      await db.query(`
        INSERT INTO audit_logs (account_id, action, metadata) VALUES
        ('${UUID_1}', 'DOCUMENT_CREATE', 'doc-001'),
        ('${UUID_3}', 'DOCUMENT_DELETE', 'doc-002');
      `);
      const res = await db.query(`
        SELECT l.action, a.username, o.org_name
        FROM audit_logs l
        INNER JOIN accounts a ON l.account_id = a.account_id
        INNER JOIN organizations o ON a.org_id = o.org_id
        WHERE l.action = 'DOCUMENT_CREATE';
      `);
      expect(res.length).toBe(1);
      expect(res[0].username).toBe("alice");
      expect(res[0].org_name).toBe("Acme Corporation");
    });

    test("73. Composite Primary Key with (tenant_id UUID, user_id UUID)", async () => {
      await db.query(`
        CREATE TABLE tenant_members (
          tenant_id UUID,
          user_id UUID,
          role TEXT,
          PRIMARY KEY (tenant_id, user_id)
        );
        INSERT INTO tenant_members (tenant_id, user_id, role) VALUES
        ('${UUID_1}', '${UUID_2}', 'admin'),
        ('${UUID_1}', '${UUID_3}', 'member'),
        ('${UUID_2}', '${UUID_3}', 'viewer');
      `);
      const res = await db.query(`SELECT role FROM tenant_members WHERE tenant_id = '${UUID_1}' AND user_id = '${UUID_2}';`);
      expect(res[0].role).toBe("admin");
    });

    test("74. UPDATE single row identified by UUID Primary Key", async () => {
      await db.query(`UPDATE accounts SET email = 'alice.new@acme.com' WHERE account_id = '${UUID_1}';`);
      const res = await db.query(`SELECT email FROM accounts WHERE account_id = '${UUID_1}';`);
      expect(res[0].email).toBe("alice.new@acme.com");
    });

    test("75. UPDATE multiple rows by UUID foreign key condition", async () => {
      await db.query(`UPDATE accounts SET is_active = false WHERE org_id = '${UUID_2}';`);
      const res = await db.query(`SELECT count(*) AS inactive_count FROM accounts WHERE org_id = '${UUID_2}' AND is_active = false;`);
      expect(Number(res[0].inactive_count)).toBe(2);
    });

    test("76. DELETE single row identified by UUID Primary Key", async () => {
      const testDeleteId = "99999999-9999-4999-a999-999999999999";
      await db.query(`INSERT INTO accounts (account_id, username) VALUES ('${testDeleteId}', 'to_delete');`);
      await db.query(`DELETE FROM accounts WHERE account_id = '${testDeleteId}';`);
      const res = await db.query(`SELECT count(*) AS cnt FROM accounts WHERE account_id = '${testDeleteId}';`);
      expect(Number(res[0].cnt)).toBe(0);
    });

    test("77. Batch DELETE using WHERE id IN (uuid1, uuid2)", async () => {
      await db.query(`
        CREATE TABLE delete_batch (id UUID PRIMARY KEY, name TEXT);
        INSERT INTO delete_batch (id, name) VALUES
        ('${UUID_1}', 'r1'), ('${UUID_2}', 'r2'), ('${UUID_3}', 'r3');
      `);
      await db.query(`DELETE FROM delete_batch WHERE id IN ('${UUID_1}', '${UUID_2}');`);
      const res = await db.query(`SELECT id FROM delete_batch;`);
      expect(res.length).toBe(1);
      expect(res[0].id).toBe(UUID_3);
    });

    test("78. Subquery UPDATE matching UUIDs", async () => {
      await db.query(`
        UPDATE accounts
        SET is_active = true
        WHERE org_id IN (SELECT org_id FROM organizations WHERE slug = 'globex');
      `);
      const res = await db.query(`SELECT count(*) AS active_cnt FROM accounts WHERE org_id = '${UUID_2}' AND is_active = true;`);
      expect(Number(res[0].active_cnt)).toBe(2);
    });

    test("79. Cross join with UUID filtering", async () => {
      const res = await db.query(`
        SELECT o.org_name, a.username
        FROM organizations o, accounts a
        WHERE o.org_id = a.org_id AND o.slug = 'acme'
        ORDER BY a.username ASC;
      `);
      expect(res.length).toBe(2);
    });

    test("80. JOIN with UUID array containment", async () => {
      const res = await db.query(`
        SELECT a1.username AS parent_user, a2.username AS linked_user
        FROM accounts a1
        JOIN accounts a2 ON a1.extra_ids @> ARRAY[a2.account_id]
        WHERE a1.username = 'alice';
      `);
      expect(res.length).toBeGreaterThanOrEqual(1);
      expect(res[0].linked_user).toBe("david");
    });
  });

  // =========================================================================
  // Section 6: Aggregations, GROUP BY & Window Functions with UUID (Tests 81 - 90)
  // =========================================================================
  describe("Section 6: Aggregations, GROUP BY & Window Functions with UUID", () => {
    test("81. COUNT(uuid_column) counts non-null UUID values", async () => {
      const res = await db.query(`SELECT COUNT(org_id) AS total_orgs FROM accounts;`);
      expect(Number(res[0].total_orgs)).toBe(4);
    });

    test("82. COUNT(DISTINCT uuid_column) counts unique UUIDs", async () => {
      const res = await db.query(`SELECT COUNT(DISTINCT org_id) AS unique_orgs FROM accounts;`);
      expect(Number(res[0].unique_orgs)).toBe(2);
    });

    test("83. GROUP BY uuid_column aggregates counts per UUID", async () => {
      const res = await db.query(`
        SELECT org_id, COUNT(*) AS member_count
        FROM accounts
        WHERE org_id IS NOT NULL
        GROUP BY org_id
        ORDER BY org_id ASC;
      `);
      expect(res.length).toBe(2);
      expect(res[0].org_id).toBe(UUID_1);
      expect(Number(res[0].member_count)).toBe(2);
      expect(res[1].org_id).toBe(UUID_2);
      expect(Number(res[1].member_count)).toBe(2);
    });

    test("84. ARRAY_AGG(uuid_column) collects UUIDs into array", async () => {
      const res = await db.query(`SELECT ARRAY_AGG(org_id) AS org_list FROM organizations;`);
      expect(Array.isArray(res[0].org_list)).toBe(true);
      expect(res[0].org_list.length).toBeGreaterThanOrEqual(3);
      expect(res[0].org_list).toContain(UUID_1);
    });

    test("85. ARRAY_AGG(uuid_column) combined with GROUP BY", async () => {
      const res = await db.query(`
        SELECT org_id, ARRAY_AGG(account_id) AS member_uuids
        FROM accounts
        WHERE org_id = '${UUID_1}'
        GROUP BY org_id;
      `);
      expect(res.length).toBe(1);
      expect(res[0].member_uuids.length).toBe(2);
      expect(res[0].member_uuids).toContain(UUID_1);
      expect(res[0].member_uuids).toContain(UUID_2);
    });

    test("86. STRING_AGG(uuid_column::text, ',') concatenates UUID strings", async () => {
      const res = await db.query(`
        SELECT STRING_AGG(org_id::text, ',') AS joined_ids
        FROM organizations
        WHERE slug IN ('acme', 'globex');
      `);
      expect(res[0].joined_ids).toContain(UUID_1);
      expect(res[0].joined_ids).toContain(UUID_2);
    });

    test("87. MIN(uuid_column) and MAX(uuid_column) lexicographical bounds", async () => {
      const res = await db.query(`
        SELECT MIN(org_id::text) AS min_uuid, MAX(org_id::text) AS max_uuid
        FROM organizations
        WHERE slug IN ('acme', 'globex', 'initech');
      `);
      expect(res[0].min_uuid).toBe(UUID_1);
      expect(res[0].max_uuid).toBe(UUID_3);
    });

    test("88. HAVING clause filtering groups by UUID count", async () => {
      const res = await db.query(`
        SELECT org_id, COUNT(*) AS cnt
        FROM accounts
        WHERE org_id IS NOT NULL
        GROUP BY org_id
        HAVING COUNT(*) >= 2;
      `);
      expect(res.length).toBe(2);
    });

    test("89. Window function ROW_NUMBER() OVER (PARTITION BY org_id ORDER BY username)", async () => {
      const res = await db.query(`
        SELECT username, org_id,
               ROW_NUMBER() OVER (PARTITION BY org_id ORDER BY username ASC) AS row_num
        FROM accounts
        WHERE org_id = '${UUID_1}';
      `);
      expect(res.length).toBe(2);
      expect(res[0].username).toBe("alice");
      expect(Number(res[0].row_num)).toBe(1);
      expect(res[1].username).toBe("bob");
      expect(Number(res[1].row_num)).toBe(2);
    });

    test("90. Window function RANK() OVER (ORDER BY org_id ASC)", async () => {
      const res = await db.query(`
        SELECT org_name, org_id, RANK() OVER (ORDER BY org_id ASC) AS rank_num
        FROM organizations
        ORDER BY org_id ASC;
      `);
      expect(res.length).toBeGreaterThanOrEqual(3);
      expect(Number(res[0].rank_num)).toBe(1);
    });
  });

  // =========================================================================
  // Section 7: DML Mutations, RETURNING, Views, CTEs & Transactions (Tests 91 - 100)
  // =========================================================================
  describe("Section 7: DML Mutations, RETURNING, Views, CTEs & Transactions", () => {
    test("91. INSERT with RETURNING uuid_column returns newly generated UUID", async () => {
      const res = await db.query(`
        INSERT INTO organizations (org_name, slug)
        VALUES ('Cyberdyne Systems', 'cyberdyne')
        RETURNING org_id, org_name;
      `);
      expect(res.length).toBe(1);
      expect(typeof res[0].org_id).toBe("string");
      expect(res[0].org_id.length).toBe(36);
      expect(res[0].org_name).toBe("Cyberdyne Systems");
    });

    test("92. Multi-row INSERT with RETURNING uuid_columns", async () => {
      const res = await db.query(`
        INSERT INTO organizations (org_name, slug) VALUES
        ('Umbrella Corp', 'umbrella'),
        ('Massive Dynamic', 'massive')
        RETURNING org_id, slug;
      `);
      expect(res.length).toBe(2);
      expect(res[0].org_id.length).toBe(36);
      expect(res[1].org_id.length).toBe(36);
      expect(res[0].org_id).not.toBe(res[1].org_id);
    });

    test("93. UPDATE with RETURNING uuid_column", async () => {
      const res = await db.query(`
        UPDATE organizations
        SET org_name = 'Acme Global'
        WHERE slug = 'acme'
        RETURNING org_id, org_name;
      `);
      expect(res.length).toBe(1);
      expect(res[0].org_id).toBe(UUID_1);
      expect(res[0].org_name).toBe("Acme Global");
    });

    test("94. DELETE with RETURNING uuid_column", async () => {
      const res = await db.query(`
        DELETE FROM organizations
        WHERE slug = 'cyberdyne'
        RETURNING org_id, slug;
      `);
      expect(res.length).toBe(1);
      expect(res[0].slug).toBe("cyberdyne");
      expect(res[0].org_id.length).toBe(36);
    });

    test("95. Subquery projection mimicking active view with UUID columns", async () => {
      const res = await db.query(`
        SELECT * FROM (
          SELECT a.account_id, a.username, o.org_name
          FROM accounts a
          LEFT JOIN organizations o ON a.org_id = o.org_id
          WHERE a.is_active = true
        ) active_accounts WHERE username = 'alice';
      `);
      expect(res.length).toBe(1);
      expect(res[0].account_id).toBe(UUID_1);
    });

    test("96. CTE with UUID filtering and joining", async () => {
      const res = await db.query(`
        WITH target_orgs AS (
          SELECT org_id, org_name FROM organizations WHERE slug IN ('acme', 'globex')
        )
        SELECT a.username, t.org_name
        FROM accounts a
        INNER JOIN target_orgs t ON a.org_id = t.org_id
        ORDER BY a.username ASC;
      `);
      expect(res.length).toBe(4);
    });

    test("97. ALTER TABLE ADD COLUMN with UUID type and default GEN_RANDOM_UUID()", async () => {
      await db.query(`ALTER TABLE accounts ADD COLUMN api_token UUID DEFAULT GEN_RANDOM_UUID();`);
      const res = await db.query(`SELECT api_token FROM accounts WHERE username = 'alice';`);
      expect(typeof res[0].api_token).toBe("string");
      expect(res[0].api_token.length).toBe(36);
    });

    test("98. Transaction COMMIT with UUID primary key records", async () => {
      await db.query("BEGIN;");
      await db.query(`INSERT INTO organizations (org_id, org_name, slug) VALUES ('${UUID_4}', 'Tyrell Corp', 'tyrell');`);
      await db.query("COMMIT;");
      const res = await db.query(`SELECT org_name FROM organizations WHERE org_id = '${UUID_4}';`);
      expect(res.length).toBe(1);
      expect(res[0].org_name).toBe("Tyrell Corp");
    });

    test("99. Transaction ROLLBACK restores state without inserting UUID row", async () => {
      await db.query("BEGIN;");
      await db.query(`INSERT INTO organizations (org_id, org_name, slug) VALUES ('${UUID_5}', 'Weyland-Yutani', 'weyland');`);
      await db.query("ROLLBACK;");
      const res = await db.query(`SELECT org_name FROM organizations WHERE org_id = '${UUID_5}';`);
      expect(res.length).toBe(0);
    });

    test("100. UNION of UUID queries combines results", async () => {
      const res = await db.query(`
        SELECT org_id FROM organizations WHERE slug = 'acme'
        UNION
        SELECT org_id FROM organizations WHERE slug = 'globex'
        ORDER BY org_id ASC;
      `);
      expect(res.length).toBe(2);
      expect(res[0].org_id).toBe(UUID_1);
      expect(res[1].org_id).toBe(UUID_2);
    });
  });
});
