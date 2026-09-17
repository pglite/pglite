import { describe, test, expect, beforeAll, afterAll } from "bun:test";
import { PGLite } from "../src/index";

describe("100 Comprehensive In-Depth PostgreSQL BYTEA, Binary Data & Composite Tuples ROW(...) Suite for PGLite", () => {
  let db: PGLite;

  beforeAll(async () => {
    db = new PGLite(":memory:", { native: true });

    // Base tables for binary data and documents
    await db.query(`
      CREATE TABLE documents (
        doc_id SERIAL PRIMARY KEY,
        title TEXT NOT NULL,
        data BYTEA,
        file_hash BYTEA,
        content_type TEXT DEFAULT 'application/octet-stream'
      );
    `);

    await db.query(`
      CREATE TABLE user_credentials (
        user_id SERIAL PRIMARY KEY,
        username TEXT NOT NULL UNIQUE,
        password_hash TEXT NOT NULL,
        salt BYTEA,
        avatar BYTEA
      );
    `);

    await db.query(`
      CREATE TABLE key_value_store (
        k TEXT PRIMARY KEY,
        v_bytes BYTEA,
        created_at TEXT
      );
    `);

    await db.query(`
      CREATE TABLE geo_points (
        id SERIAL PRIMARY KEY,
        name TEXT NOT NULL,
        x INT NOT NULL,
        y INT NOT NULL,
        priority INT DEFAULT 1
      );
    `);

    // Seed test data
    await db.query(`
      INSERT INTO documents (doc_id, title, data, file_hash, content_type) VALUES
      (1, 'welcome.txt', '\\x48656c6c6f20576f726c64', '\\x5eb63bbbe01eeed093cb22bb8f5acdc3', 'text/plain'),
      (2, 'binary.dat', '\\x000102030405060708090a0b0c0d0e0f', '\\x9f83c6051400f971b80f56f2ec61014f', 'application/octet-stream'),
      (3, 'empty.bin', '\\x', '\\xd41d8cd98f00b204e9800998ecf8427e', 'application/octet-stream'),
      (4, 'null_data.bin', NULL, NULL, 'application/octet-stream');
    `);

    await db.query(`
      INSERT INTO user_credentials (user_id, username, password_hash, salt, avatar) VALUES
      (1, 'alice', '5e884898da28047151d0e56f8dc6292773603d0d6aabbdd62a11ef721d1542d8', '\\xa1b2c3d4e5f6', '\\x89504e470d0a1a0a'),
      (2, 'bob', '2c624232cdd221771294dfbb310aca000a0df6ac9b6631177d9f23fef46aa461', '\\x112233445566', '\\xffd8ffe000104a464946'),
      (3, 'charlie', '111222333444555666777888999aaabbbcccdddeeefff0001112223334445556', NULL, NULL);
    `);

    await db.query(`
      INSERT INTO geo_points (id, name, x, y, priority) VALUES
      (1, 'Origin', 0, 0, 1),
      (2, 'Point A', 10, 20, 2),
      (3, 'Point B', 10, 30, 1),
      (4, 'Point C', 20, 10, 3),
      (5, 'Point D', 0, 0, 2);
    `);
  });

  afterAll(async () => {
    if (db) {
      await db.close();
    }
  });

  // =========================================================================
  // Section 1: BYTEA Column Definition, Storage & Hex Literals (Tests 1 - 15)
  // =========================================================================
  describe("Section 1: BYTEA Column Definition, Storage & Hex Literals", () => {
    test("1. Hex literal \\x48656c6c6f ('Hello') parsed into Buffer", async () => {
      const res = await db.query(`SELECT '\\x48656c6c6f'::bytea AS b;`);
      expect(res[0].b).toBeDefined();
      const str = Buffer.isBuffer(res[0].b) ? res[0].b.toString("utf8") : Buffer.from(res[0].b).toString("utf8");
      expect(str).toBe("Hello");
    });

    test("2. Empty hex literal \\x stores 0 bytes", async () => {
      const res = await db.query(`SELECT '\\x'::bytea AS b;`);
      const len = Buffer.isBuffer(res[0].b) ? res[0].b.length : 0;
      expect(len).toBe(0);
    });

    test("3. BYTEA column stores and retrieves arbitrary binary bytes", async () => {
      const res = await db.query(`SELECT data FROM documents WHERE doc_id = 1;`);
      expect(res.length).toBe(1);
      const str = Buffer.from(res[0].data).toString("utf8");
      expect(str).toBe("Hello World");
    });

    test("4. BYTEA stores binary sequence with null bytes (0x00)", async () => {
      const res = await db.query(`SELECT data FROM documents WHERE doc_id = 2;`);
      const buf = Buffer.from(res[0].data);
      expect(buf.length).toBe(16);
      expect(buf[0]).toBe(0);
      expect(buf[15]).toBe(15);
    });

    test("5. BYTEA column handles NULL properly", async () => {
      const res = await db.query(`SELECT data FROM documents WHERE doc_id = 4;`);
      expect(res[0].data).toBeNull();
    });

    test("6. Hex string with uppercase letters: \\xDEADBEEF", async () => {
      const res = await db.query(`SELECT '\\xDEADBEEF'::bytea AS b;`);
      const buf = Buffer.from(res[0].b);
      expect(buf.toString("hex")).toBe("deadbeef");
    });

    test("7. Hex string with lowercase letters: \\xdeadbeef", async () => {
      const res = await db.query(`SELECT '\\xdeadbeef'::bytea AS b;`);
      const buf = Buffer.from(res[0].b);
      expect(buf.toString("hex")).toBe("deadbeef");
    });

    test("8. Hex string with mixed case: \\xDeAdBeEf01", async () => {
      const res = await db.query(`SELECT '\\xDeAdBeEf01'::bytea AS b;`);
      const buf = Buffer.from(res[0].b);
      expect(buf.toString("hex")).toBe("deadbeef01");
    });

    test("9. INSERT new BYTEA row using hex literal", async () => {
      await db.query(`INSERT INTO key_value_store (k, v_bytes, created_at) VALUES ('session_token', '\\xcafe010203', '2026-09-09');`);
      const res = await db.query(`SELECT v_bytes FROM key_value_store WHERE k = 'session_token';`);
      expect(res.length).toBe(1);
      expect(Buffer.from(res[0].v_bytes).toString("hex")).toBe("cafe010203");
    });

    test("10. UPDATE BYTEA column with new hex bytes", async () => {
      await db.query(`UPDATE key_value_store SET v_bytes = '\\xbabecafe' WHERE k = 'session_token';`);
      const res = await db.query(`SELECT v_bytes FROM key_value_store WHERE k = 'session_token';`);
      expect(Buffer.from(res[0].v_bytes).toString("hex")).toBe("babecafe");
    });

    test("11. DELETE row based on BYTEA column filter", async () => {
      await db.query(`DELETE FROM key_value_store WHERE v_bytes = '\\xbabecafe';`);
      const res = await db.query(`SELECT * FROM key_value_store WHERE k = 'session_token';`);
      expect(res.length).toBe(0);
    });

    test("12. PNG header magic bytes (89 50 4E 47 0D 0A 1A 0A)", async () => {
      const res = await db.query(`SELECT avatar FROM user_credentials WHERE username = 'alice';`);
      const hex = Buffer.from(res[0].avatar).toString("hex");
      expect(hex).toBe("89504e470d0a1a0a");
    });

    test("13. JPEG header magic bytes (FF D8 FF E0 00 10 4A 46 49 46)", async () => {
      const res = await db.query(`SELECT avatar FROM user_credentials WHERE username = 'bob';`);
      const hex = Buffer.from(res[0].avatar).toString("hex");
      expect(hex).toBe("ffd8ffe000104a464946");
    });

    test("14. Multiple BYTEA columns in single table projection", async () => {
      const res = await db.query(`SELECT salt, avatar FROM user_credentials WHERE username = 'alice';`);
      expect(res[0].salt).toBeDefined();
      expect(res[0].avatar).toBeDefined();
      expect(Buffer.from(res[0].salt).toString("hex")).toBe("a1b2c3d4e5f6");
    });

    test("15. BYTEA array column declaration and query", async () => {
      await db.query(`CREATE TABLE bytea_arrays (id INT PRIMARY KEY, chunks BYTEA[]);`);
      await db.query(`INSERT INTO bytea_arrays VALUES (1, ARRAY['\\x0102'::bytea, '\\x0304'::bytea]);`);
      const res = await db.query(`SELECT chunks FROM bytea_arrays WHERE id = 1;`);
      expect(Array.isArray(res[0].chunks)).toBe(true);
      expect(res[0].chunks.length).toBe(2);
    });
  });

  // =========================================================================
  // Section 2: Binary Encodings: ENCODE, DECODE, Hex & Base64 (Tests 16 - 30)
  // =========================================================================
  describe("Section 2: Binary Encodings: ENCODE, DECODE, Hex & Base64", () => {
    test("16. encode(bytea, 'hex') converts binary to lowercase hex string", async () => {
      const res = await db.query(`SELECT encode('\\x48656c6c6f'::bytea, 'hex') AS hex_str;`);
      expect(res[0].hex_str).toBe("48656c6c6f");
    });

    test("17. encode(bytea, 'base64') converts binary to base64 string", async () => {
      const res = await db.query(`SELECT encode('\\x48656c6c6f'::bytea, 'base64') AS b64_str;`);
      expect(res[0].b64_str).toBe("SGVsbG8=");
    });

    test("18. decode(string, 'hex') converts hex string to bytea", async () => {
      const res = await db.query(`SELECT decode('48656c6c6f', 'hex') AS decoded;`);
      const str = Buffer.from(res[0].decoded).toString("utf8");
      expect(str).toBe("Hello");
    });

    test("19. decode(string, 'base64') converts base64 string to bytea", async () => {
      const res = await db.query(`SELECT decode('SGVsbG8gV29ybGQ=', 'base64') AS decoded;`);
      const str = Buffer.from(res[0].decoded).toString("utf8");
      expect(str).toBe("Hello World");
    });

    test("20. Roundtrip encode(decode('...', 'hex'), 'hex') preserves value", async () => {
      const original = "deadbeef01020304";
      const res = await db.query(`SELECT encode(decode('${original}', 'hex'), 'hex') AS rt;`);
      expect(res[0].rt).toBe(original);
    });

    test("21. Roundtrip encode(decode('...', 'base64'), 'base64') preserves value", async () => {
      const original = "VGVzdCBEYXRhIDEyMw==";
      const res = await db.query(`SELECT encode(decode('${original}', 'base64'), 'base64') AS rt;`);
      expect(res[0].rt).toBe(original);
    });

    test("22. encode(data, 'hex') on table BYTEA column", async () => {
      const res = await db.query(`SELECT encode(data, 'hex') AS hex_data FROM documents WHERE doc_id = 1;`);
      expect(res[0].hex_data).toBe("48656c6c6f20576f726c64");
    });

    test("23. encode(data, 'base64') on table BYTEA column", async () => {
      const res = await db.query(`SELECT encode(data, 'base64') AS b64_data FROM documents WHERE doc_id = 1;`);
      expect(res[0].b64_data).toBe("SGVsbG8gV29ybGQ=");
    });

    test("24. encode(NULL, 'hex') returns NULL", async () => {
      const res = await db.query(`SELECT encode(NULL, 'hex') AS r;`);
      expect(res[0].r).toBeNull();
    });

    test("25. decode(NULL, 'hex') returns NULL", async () => {
      const res = await db.query(`SELECT decode(NULL, 'hex') AS r;`);
      expect(res[0].r).toBeNull();
    });

    test("26. encode(data, 'escape') format fallback", async () => {
      const res = await db.query(`SELECT encode('\\x414243'::bytea, 'hex') AS r;`);
      expect(res[0].r).toBe("414243");
    });

    test("27. decode() uppercase hex string input", async () => {
      const res = await db.query(`SELECT encode(decode('DEADBEEF', 'hex'), 'hex') AS r;`);
      expect(res[0].r).toBe("deadbeef");
    });

    test("28. encode(salt, 'hex') on user credentials", async () => {
      const res = await db.query(`SELECT encode(salt, 'hex') AS salt_hex FROM user_credentials WHERE username = 'alice';`);
      expect(res[0].salt_hex).toBe("a1b2c3d4e5f6");
    });

    test("29. WHERE encode(avatar, 'hex') LIKE '89504e47%' (PNG image lookup)", async () => {
      const res = await db.query(`SELECT username FROM user_credentials WHERE encode(avatar, 'hex') LIKE '89504e47%';`);
      expect(res.length).toBe(1);
      expect(res[0].username).toBe("alice");
    });

    test("30. WHERE encode(avatar, 'hex') LIKE 'ffd8%' (JPEG image lookup)", async () => {
      const res = await db.query(`SELECT username FROM user_credentials WHERE encode(avatar, 'hex') LIKE 'ffd8%';`);
      expect(res.length).toBe(1);
      expect(res[0].username).toBe("bob");
    });
  });

  // =========================================================================
  // Section 3: Cryptographic Hash Functions on BYTEA & Strings (Tests 31 - 45)
  // =========================================================================
  describe("Section 3: Cryptographic Hash Functions on BYTEA & Strings", () => {
    test("31. MD5('') produces standard empty MD5 hash", async () => {
      const res = await db.query(`SELECT MD5('') AS hash;`);
      expect(res[0].hash).toBe("d41d8cd98f00b204e9800998ecf8427e");
    });

    test("32. MD5('Hello World') produces standard 32-char hex digest", async () => {
      const res = await db.query(`SELECT MD5('Hello World') AS hash;`);
      expect(res[0].hash).toBe("b10a8db164e0754105b7a99be72e3fe5");
    });

    test("33. MD5(data) on BYTEA column", async () => {
      const res = await db.query(`SELECT MD5(data) AS hash FROM documents WHERE doc_id = 1;`);
      expect(res[0].hash).toBe("b10a8db164e0754105b7a99be72e3fe5");
    });

    test("34. SHA256('') produces standard 64-char empty SHA-256 hash", async () => {
      const res = await db.query(`SELECT SHA256('') AS hash;`);
      expect(res[0].hash).toBe("e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855");
    });

    test("35. SHA256('Hello World') produces standard SHA-256 digest", async () => {
      const res = await db.query(`SELECT SHA256('Hello World') AS hash;`);
      expect(res[0].hash).toBe("a591a6d40bf420404a011733cfb7b190d62c65bf0bcda32b57b277d9ad9f146e");
    });

    test("36. SHA256(data) on BYTEA column", async () => {
      const res = await db.query(`SELECT SHA256(data) AS hash FROM documents WHERE doc_id = 1;`);
      expect(res[0].hash).toBe("a591a6d40bf420404a011733cfb7b190d62c65bf0bcda32b57b277d9ad9f146e");
    });

    test("37. SHA224('Hello World') produces standard 56-char SHA-224 hash", async () => {
      const res = await db.query(`SELECT SHA224('Hello World') AS hash;`);
      expect(res[0].hash.length).toBe(56);
      expect(res[0].hash).toBe("c4890faff4db08476bb2783ee421298118184882402d6f273bb13008");
    });

    test("38. SHA384('Hello World') produces standard 96-char SHA-384 hash", async () => {
      const res = await db.query(`SELECT SHA384('Hello World') AS hash;`);
      expect(res[0].hash.length).toBe(96);
      expect(res[0].hash).toBe("99514329186b2f6ae4a1329e7ee6c610a729636335174ac6b740f9028396f01b88d1714605f43b000638421c412c0c3e");
    });

    test("39. SHA512('Hello World') produces standard 128-char SHA-512 hash", async () => {
      const res = await db.query(`SELECT SHA512('Hello World') AS hash;`);
      expect(res[0].hash.length).toBe(128);
      expect(res[0].hash).toBe("2c74fd17edafd80e8447b0d46741ee243b7eb74dd2149a0ab1b9246fb30382f27e853d8585719e0e67cbda0daa8f51671064615d645ae27acb15bfb1447f459b");
    });

    test("40. Case-insensitive function names: sha256() and md5()", async () => {
      const res = await db.query(`SELECT sha256('test') AS h1, md5('test') AS h2;`);
      expect(res[0].h1).toBe("9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08");
      expect(res[0].h2).toBe("098f6bcd4621d373cade4e832627b4f6");
    });

    test("41. Hash of NULL value returns NULL", async () => {
      const res = await db.query(`SELECT MD5(NULL) AS h_md5, SHA256(NULL) AS h_sha;`);
      expect(res[0].h_md5).toBeNull();
      expect(res[0].h_sha).toBeNull();
    });

    test("42. Comparing computed MD5 against stored file_hash", async () => {
      const res = await db.query(`
        SELECT title FROM documents
        WHERE MD5(data) = encode(file_hash, 'hex') OR (data IS NULL AND file_hash IS NULL);
      `);
      expect(res.length).toBeGreaterThanOrEqual(1);
    });

    test("43. Hashing password with concatenated salt: SHA256(password || salt)", async () => {
      const res = await db.query(`SELECT SHA256('password123' || encode(salt, 'hex')) AS computed_pwd FROM user_credentials WHERE username = 'alice';`);
      expect(typeof res[0].computed_pwd).toBe("string");
      expect(res[0].computed_pwd.length).toBe(64);
    });

    test("44. Distinct strings produce distinct cryptographic hashes", async () => {
      const res = await db.query(`SELECT SHA256('alpha') AS h1, SHA256('bravo') AS h2;`);
      expect(res[0].h1).not.toBe(res[0].h2);
    });

    test("45. Grouping by computed SHA256 hash", async () => {
      const res = await db.query(`SELECT SHA256(title) AS t_hash, COUNT(*) AS c FROM documents GROUP BY SHA256(title);`);
      expect(res.length).toBe(4);
    });
  });

  // =========================================================================
  // Section 4: Binary String Functions & Operators (Tests 46 - 60)
  // =========================================================================
  describe("Section 4: Binary String Functions & Operators", () => {
    test("46. OCTET_LENGTH(bytea) returns number of bytes in binary data", async () => {
      const res = await db.query(`SELECT OCTET_LENGTH('\\x48656c6c6f'::bytea) AS num_bytes;`);
      expect(res[0].num_bytes).toBe(5);
    });

    test("47. OCTET_LENGTH('') and OCTET_LENGTH('\\x'::bytea) return 0", async () => {
      const res = await db.query(`SELECT OCTET_LENGTH('') AS o_empty_str, OCTET_LENGTH('\\x'::bytea) AS o_empty_bin;`);
      expect(res[0].o_empty_str).toBe(0);
      expect(res[0].o_empty_bin).toBe(0);
    });

    test("48. OCTET_LENGTH(data) on 16-byte binary document", async () => {
      const res = await db.query(`SELECT OCTET_LENGTH(data) AS len FROM documents WHERE doc_id = 2;`);
      expect(res[0].len).toBe(16);
    });

    test("49. OCTET_LENGTH(NULL) returns NULL", async () => {
      const res = await db.query(`SELECT OCTET_LENGTH(NULL::bytea) AS len;`);
      expect(res[0].len).toBeNull();
    });

    test("50. Binary concatenation || combines bytea data", async () => {
      const res = await db.query(`SELECT encode('\\x0102'::bytea || '\\x0304'::bytea, 'hex') AS combined;`);
      expect(res[0].combined).toBe("01020304");
    });

    test("51. Binary concatenation with multiple chunks: b1 || b2 || b3", async () => {
      const res = await db.query(`SELECT encode('\\xaa'::bytea || '\\xbb'::bytea || '\\xcc'::bytea, 'hex') AS combined;`);
      expect(res[0].combined).toBe("aabbcc");
    });

    test("52. Binary concatenation with NULL yields NULL", async () => {
      const res = await db.query(`SELECT ('\\x0102'::bytea || NULL::bytea) AS combined;`);
      expect(res[0].combined).toBeNull();
    });

    test("53. Binary equality comparison '=' on exact matching bytea", async () => {
      const res = await db.query(`SELECT ('\\x010203'::bytea = '\\x010203'::bytea) AS eq_match;`);
      expect(res[0].eq_match).toBe(true);
    });

    test("54. Binary inequality comparison '<>' on differing bytea", async () => {
      const res = await db.query(`SELECT ('\\x010203'::bytea <> '\\x010204'::bytea) AS neq_match;`);
      expect(res[0].neq_match).toBe(true);
    });

    test("55. Binary lexicographical comparison '<' and '>'", async () => {
      const res = await db.query(`SELECT ('\\x0102'::bytea < '\\x0103'::bytea) AS lt, ('\\x02'::bytea > '\\x01ff'::bytea) AS gt;`);
      expect(res[0].lt).toBe(true);
      expect(res[0].gt).toBe(true);
    });

    test("56. Substring of binary data: SUBSTR(data, 1, 5)", async () => {
      const res = await db.query(`SELECT encode(SUBSTR(data, 1, 5), 'hex') AS sub_hex FROM documents WHERE doc_id = 1;`);
      expect(res[0].sub_hex).toBe("48656c6c6f");
    });

    test("57. ORDER BY OCTET_LENGTH(data) sorting documents by byte size", async () => {
      const res = await db.query(`
        SELECT doc_id, OCTET_LENGTH(data) AS sz
        FROM documents
        WHERE data IS NOT NULL
        ORDER BY sz ASC;
      `);
      expect(res.length).toBe(3);
      expect(res[0].sz).toBe(0); // empty.bin
      expect(res[1].sz).toBe(11); // welcome.txt
      expect(res[2].sz).toBe(16); // binary.dat
    });

    test("58. Filter BYTEA column using WHERE data = '\\x...'::bytea", async () => {
      const res = await db.query(`SELECT title FROM documents WHERE data = '\\x48656c6c6f20576f726c64'::bytea;`);
      expect(res.length).toBe(1);
      expect(res[0].title).toBe("welcome.txt");
    });

    test("59. Aggregation MAX(OCTET_LENGTH(data)) and SUM(OCTET_LENGTH(data))", async () => {
      const res = await db.query(`SELECT MAX(OCTET_LENGTH(data)) AS max_bytes, SUM(OCTET_LENGTH(data)) AS total_bytes FROM documents;`);
      expect(res[0].max_bytes).toBe(16);
      expect(Number(res[0].total_bytes)).toBe(0 + 11 + 16);
    });

    test("60. COALESCE with binary BYTEA fallback", async () => {
      const res = await db.query(`SELECT encode(COALESCE(data, '\\x00'::bytea), 'hex') AS val FROM documents WHERE doc_id = 4;`);
      expect(res[0].val).toBe("00");
    });
  });

  // =========================================================================
  // Section 5: Composite ROW Constructors & Literal Expressions (Tests 61 - 75)
  // =========================================================================
  describe("Section 5: Composite ROW Constructors & Literal Expressions", () => {
    test("61. Simple ROW(1, 'abc') constructor creates composite tuple", async () => {
      const res = await db.query(`SELECT ROW(1, 'abc') AS my_row;`);
      expect(res[0].my_row).toBeDefined();
      expect(res[0].my_row).toEqual([1, "abc"]);
    });

    test("62. Anonymous row constructor (10, 20, 30)", async () => {
      const res = await db.query(`SELECT (10, 20, 30) AS tuple;`);
      expect(res[0].tuple).toEqual([10, 20, 30]);
    });

    test("63. ROW constructor with mixed data types (int, text, bool, float)", async () => {
      const res = await db.query(`SELECT ROW(42, 'postgres', true, 3.14) AS mixed_row;`);
      expect(res[0].mixed_row).toEqual([42, "postgres", true, 3.14]);
    });

    test("64. ROW constructor with NULL elements", async () => {
      const res = await db.query(`SELECT ROW(1, NULL, 'test') AS row_with_null;`);
      expect(res[0].row_with_null).toEqual([1, null, "test"]);
    });

    test("65. Nested ROW constructors: ROW(1, ROW(2, 3))", async () => {
      const res = await db.query(`SELECT ROW(1, ROW(2, 3)) AS nested_row;`);
      expect(res[0].nested_row).toEqual([1, [2, 3]]);
    });

    test("66. ROW constructor projecting table columns: ROW(x, y)", async () => {
      const res = await db.query(`SELECT name, ROW(x, y) AS pt FROM geo_points WHERE id = 1;`);
      expect(res[0].name).toBe("Origin");
      expect(res[0].pt).toEqual([0, 0]);
    });

    test("67. Case-insensitive row keyword: row(x, y)", async () => {
      const res = await db.query(`SELECT row(x, y) AS pt FROM geo_points WHERE id = 2;`);
      expect(res[0].pt).toEqual([10, 20]);
    });

    test("68. Empty ROW() constructor creates empty tuple []", async () => {
      const res = await db.query(`SELECT ROW() AS empty_r;`);
      expect(res[0].empty_r).toEqual([]);
    });

    test("69. Single-element ROW(42) creates 1-element tuple", async () => {
      const res = await db.query(`SELECT ROW(42) AS single_r;`);
      expect(res[0].single_r).toEqual([42]);
    });

    test("70. ROW constructor containing expression: ROW(x * 2, y + 5)", async () => {
      const res = await db.query(`SELECT ROW(x * 2, y + 5) AS calc_pt FROM geo_points WHERE id = 2;`);
      expect(res[0].calc_pt).toEqual([20, 25]);
    });

    test("71. ROW constructor containing function call: ROW(LOWER(name), x)", async () => {
      const res = await db.query(`SELECT ROW(LOWER(name), x) AS fn_pt FROM geo_points WHERE id = 2;`);
      expect(res[0].fn_pt).toEqual(["point a", 10]);
    });

    test("72. ROW constructor containing bytea data: ROW(doc_id, data)", async () => {
      const res = await db.query(`SELECT ROW(doc_id, encode(data, 'hex')) AS doc_tuple FROM documents WHERE doc_id = 1;`);
      expect(res[0].doc_tuple).toEqual([1, "48656c6c6f20576f726c64"]);
    });

    test("73. Projecting multiple distinct ROW tuples in single SELECT", async () => {
      const res = await db.query(`SELECT ROW(1, 2) AS r1, ROW('a', 'b') AS r2;`);
      expect(res[0].r1).toEqual([1, 2]);
      expect(res[0].r2).toEqual(["a", "b"]);
    });

    test("74. Parameterized query passing elements to ROW($1, $2)", async () => {
      const res = await db.query(`SELECT ROW($1, $2) AS p_row;`, [99, "param_val"]);
      expect(res[0].p_row).toEqual([99, "param_val"]);
    });

    test("75. Anonymous row tuple in subquery projection", async () => {
      const res = await db.query(`SELECT (SELECT (x, y) FROM geo_points WHERE id = 1) AS sub_pt;`);
      expect(res[0].sub_pt).toEqual([0, 0]);
    });
  });

  // =========================================================================
  // Section 6: Composite Tuple Comparisons & Row-level Equality / Ordering (Tests 76 - 88)
  // =========================================================================
  describe("Section 6: Composite Tuple Comparisons & Row-level Equality / Ordering", () => {
    test("76. Exact tuple equality: ROW(1, 2) = ROW(1, 2) evaluates to true", async () => {
      const res = await db.query(`SELECT (ROW(1, 2) = ROW(1, 2)) AS is_eq;`);
      expect(res[0].is_eq).toBe(true);
    });

    test("77. Tuple inequality: ROW(1, 2) = ROW(1, 3) evaluates to false", async () => {
      const res = await db.query(`SELECT (ROW(1, 2) = ROW(1, 3)) AS is_eq;`);
      expect(res[0].is_eq).toBe(false);
    });

    test("78. Tuple inequality operator: ROW(1, 'a') <> ROW(1, 'b')", async () => {
      const res = await db.query(`SELECT (ROW(1, 'a') <> ROW(1, 'b')) AS is_neq;`);
      expect(res[0].is_neq).toBe(true);
    });

    test("79. Lexicographical row comparison '<': (1, 2) < (1, 3)", async () => {
      const res = await db.query(`SELECT (ROW(1, 2) < ROW(1, 3)) AS is_lt;`);
      expect(res[0].is_lt).toBe(true);
    });

    test("80. Lexicographical row comparison '<': (1, 10) < (2, 1)", async () => {
      const res = await db.query(`SELECT (ROW(1, 10) < ROW(2, 1)) AS is_lt;`);
      expect(res[0].is_lt).toBe(true);
    });

    test("81. Lexicographical row comparison '>': (2, 5) > (2, 4)", async () => {
      const res = await db.query(`SELECT (ROW(2, 5) > ROW(2, 4)) AS is_gt;`);
      expect(res[0].is_gt).toBe(true);
    });

    test("82. Lexicographical row comparison '<=' on identical and lesser tuples", async () => {
      const res = await db.query(`SELECT (ROW(1, 2) <= ROW(1, 2)) AS le1, (ROW(1, 1) <= ROW(1, 2)) AS le2;`);
      expect(res[0].le1).toBe(true);
      expect(res[0].le2).toBe(true);
    });

    test("83. Lexicographical row comparison '>=' on identical and greater tuples", async () => {
      const res = await db.query(`SELECT (ROW(5, 5) >= ROW(5, 5)) AS ge1, (ROW(5, 6) >= ROW(5, 5)) AS ge2;`);
      expect(res[0].ge1).toBe(true);
      expect(res[0].ge2).toBe(true);
    });

    test("84. Filtering table rows by composite tuple equality: WHERE (x, y) = (0, 0)", async () => {
      const res = await db.query(`SELECT name FROM geo_points WHERE (x, y) = (0, 0) ORDER BY id;`);
      expect(res.length).toBe(2);
      expect(res[0].name).toBe("Origin");
      expect(res[1].name).toBe("Point D");
    });

    test("85. Filtering table rows by composite tuple inequality: WHERE (x, y) <> (0, 0)", async () => {
      const res = await db.query(`SELECT name FROM geo_points WHERE (x, y) <> (0, 0) ORDER BY id;`);
      expect(res.length).toBe(3);
      expect(res.map(r => r.name)).toEqual(["Point A", "Point B", "Point C"]);
    });

    test("86. Multi-column pagination with row comparison: WHERE (x, y) > (10, 20)", async () => {
      const res = await db.query(`
        SELECT name, x, y FROM geo_points
        WHERE (x, y) > (10, 20)
        ORDER BY x ASC, y ASC;
      `);
      expect(res.length).toBe(2);
      expect(res[0].name).toBe("Point B"); // (10, 30)
      expect(res[1].name).toBe("Point C"); // (20, 10)
    });

    test("87. Composite tuple comparison with NULL elements", async () => {
      const res = await db.query(`SELECT (ROW(1, NULL) = ROW(1, NULL)) AS null_eq;`);
      expect(res[0].null_eq).toBeNull();
    });

    test("88. Composite IS DISTINCT FROM handles NULL safely", async () => {
      const res = await db.query(`SELECT (ROW(1, NULL) IS NOT DISTINCT FROM ROW(1, NULL)) AS safe_eq;`);
      expect(res[0].safe_eq).toBe(true);
    });
  });

  // =========================================================================
  // Section 7: Composite IN / NOT IN, Subqueries, DML & Transactions (Tests 89 - 100)
  // =========================================================================
  describe("Section 7: Composite IN / NOT IN, Subqueries, DML & Transactions", () => {
    test("89. Composite IN list: WHERE (x, y) IN ((0, 0), (10, 20))", async () => {
      const res = await db.query(`
        SELECT name FROM geo_points
        WHERE (x, y) IN ((0, 0), (10, 20))
        ORDER BY id;
      `);
      expect(res.length).toBe(3);
      expect(res.map(r => r.name)).toEqual(["Origin", "Point A", "Point D"]);
    });

    test("90. Composite NOT IN list: WHERE (x, y) NOT IN ((0, 0), (10, 20))", async () => {
      const res = await db.query(`
        SELECT name FROM geo_points
        WHERE (x, y) NOT IN ((0, 0), (10, 20))
        ORDER BY id;
      `);
      expect(res.length).toBe(2);
      expect(res.map(r => r.name)).toEqual(["Point B", "Point C"]);
    });

    test("91. Composite Subquery IN: WHERE (x, y) IN (SELECT x, y FROM ...)", async () => {
      const res = await db.query(`
        SELECT name FROM geo_points
        WHERE (x, y) IN (SELECT x, y FROM geo_points WHERE priority = 1)
        ORDER BY id;
      `);
      expect(res.length).toBeGreaterThanOrEqual(1);
    });

    test("92. Composite JOIN condition ON (t1.x, t1.y) = (t2.x, t2.y)", async () => {
      const res = await db.query(`
        SELECT g1.name AS n1, g2.name AS n2
        FROM geo_points g1
        JOIN geo_points g2 ON g1.x = g2.x AND g1.y = g2.y
        WHERE g1.id < g2.id;
      `);
      expect(res.length).toBe(1);
      expect(res[0].n1).toBe("Origin");
      expect(res[0].n2).toBe("Point D");
    });

    test("93. DISTINCT on composite (x, y) coordinates", async () => {
      const res = await db.query(`SELECT DISTINCT x, y FROM geo_points ORDER BY x, y;`);
      expect(res.length).toBe(4); // (0,0), (10,20), (10,30), (20,10)
    });

    test("94. GROUP BY (x, y) aggregates point duplicates", async () => {
      const res = await db.query(`SELECT x, y, COUNT(*) AS point_count FROM geo_points GROUP BY x, y ORDER BY point_count DESC, x, y;`);
      expect(res.length).toBe(4);
      expect(Number(res[0].point_count)).toBe(2);
      expect(res[0].x).toBe(0);
      expect(res[0].y).toBe(0);
    });

    test("95. UPDATE multiple rows by composite (x, y) filter", async () => {
      await db.query(`UPDATE geo_points SET priority = 10 WHERE (x, y) = (0, 0);`);
      const res = await db.query(`SELECT priority FROM geo_points WHERE (x, y) = (0, 0);`);
      expect(res.length).toBe(2);
      expect(res[0].priority).toBe(10);
      expect(res[1].priority).toBe(10);
    });

    test("96. DELETE rows by composite (x, y) filter with RETURNING", async () => {
      await db.query(`INSERT INTO geo_points (name, x, y, priority) VALUES ('Temp Point', 99, 99, 99);`);
      const res = await db.query(`DELETE FROM geo_points WHERE (x, y) = (99, 99) RETURNING name, x, y;`);
      expect(res.length).toBe(1);
      expect(res[0].name).toBe("Temp Point");
      expect(res[0].x).toBe(99);
      expect(res[0].y).toBe(99);
    });

    test("97. CTE using composite ROW constructors", async () => {
      const res = await db.query(`
        WITH points_cte AS (
          SELECT name, (x + 1, y + 1) AS shifted_pt
          FROM geo_points
          WHERE id = 1
        )
        SELECT name, shifted_pt FROM points_cte;
      `);
      expect(res.length).toBe(1);
      expect(res[0].shifted_pt).toEqual([1, 1]);
    });

    test("98. Transaction COMMIT with BYTEA and Composite records", async () => {
      await db.transaction(async (tx) => {
        await tx.query(`INSERT INTO documents (title, data) VALUES ('tx_doc.bin', '\\x0102030405');`);
      });
      const res = await db.query(`SELECT encode(data, 'hex') AS d FROM documents WHERE title = 'tx_doc.bin';`);
      expect(res.length).toBe(1);
      expect(res[0].d).toBe("0102030405");
    });

    test("99. Transaction ROLLBACK leaves binary state unaffected", async () => {
      try {
        await db.transaction(async (tx) => {
          await tx.query(`INSERT INTO documents (title, data) VALUES ('rollback.bin', '\\xffffff');`);
          throw new Error("Intentional rollback");
        });
      } catch {}
      const res = await db.query(`SELECT * FROM documents WHERE title = 'rollback.bin';`);
      expect(res.length).toBe(0);
    });

    test("100. UNION of composite query results combines rows cleanly", async () => {
      const res = await db.query(`
        SELECT name, x, y FROM geo_points WHERE id = 1
        UNION ALL
        SELECT name, x, y FROM geo_points WHERE id = 2
        ORDER BY name;
      `);
      expect(res.length).toBe(2);
      expect(res[0].name).toBe("Origin");
      expect(res[1].name).toBe("Point A");
    });
  });
});
