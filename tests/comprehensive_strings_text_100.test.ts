import { describe, it, expect, beforeEach } from "bun:test";
import { PGLite } from "../src/index";

describe("Comprehensive Strings, Text, Unicode, Vietnamese & Regex Suite (100 Test Cases)", () => {
  let db: PGLite;

  beforeEach(async () => {
    db = new PGLite(":memory:", { native: true });
  });

  // ==========================================
  // Section 1: String Literals, Escaping & Types (1-10)
  // ==========================================

  it("001: should handle simple single-quoted string literals", async () => {
    const res = await db.query("SELECT 'Hello World' AS val;");
    expect(res[0].val).toBe("Hello World");
  });

  it("002: should handle escaped single quotes with double single-quotes", async () => {
    const res = await db.query("SELECT 'O''Reilly' AS val;");
    expect(res[0].val).toBe("O'Reilly");
  });

  it("003: should support empty string literal", async () => {
    const res = await db.query("SELECT '' AS val, LENGTH('') AS len;");
    expect(res[0].val).toBe("");
    expect(res[0].len).toBe(0);
  });

  it("004: should create table with TEXT column and insert/retrieve strings", async () => {
    await db.query("CREATE TABLE t_text (id SERIAL PRIMARY KEY, content TEXT);");
    await db.query("INSERT INTO t_text (content) VALUES ('Paragraph 1'), ('Paragraph 2');");
    const res = await db.query("SELECT * FROM t_text ORDER BY id;");
    expect(res.length).toBe(2);
    expect(res[0].content).toBe("Paragraph 1");
    expect(res[1].content).toBe("Paragraph 2");
  });

  it("005: should create table with VARCHAR(n) and CHAR(n) columns", async () => {
    await db.query("CREATE TABLE t_vc (id INT PRIMARY KEY, code CHAR(3), name VARCHAR(50));");
    await db.query("INSERT INTO t_vc VALUES (1, 'VIE', 'Vietnam'), (2, 'USA', 'United States');");
    const res = await db.query("SELECT * FROM t_vc WHERE code = 'VIE';");
    expect(res[0].name).toBe("Vietnam");
  });

  it("006: should handle multiline text", async () => {
    const multiline = "Line 1\nLine 2\nLine 3";
    await db.query("CREATE TABLE t_multi (id INT PRIMARY KEY, doc TEXT);");
    await db.query("INSERT INTO t_multi VALUES (1, $1);", [multiline]);
    const res = await db.query("SELECT doc FROM t_multi WHERE id = 1;");
    expect(res[0].doc).toBe(multiline);
  });

  it("007: should cast integer and float to TEXT", async () => {
    const res = await db.query("SELECT CAST(123 AS TEXT) AS a, 45.67::TEXT AS b;");
    expect(res[0].a).toBe("123");
    expect(res[0].b).toBe("45.67");
  });

  it("008: should cast boolean to VARCHAR", async () => {
    const res = await db.query("SELECT CAST(true AS VARCHAR) AS t, CAST(false AS VARCHAR) AS f;");
    expect(String(res[0].t).toLowerCase()).toBe("true");
    expect(String(res[0].f).toLowerCase()).toBe("false");
  });

  it("009: should handle NULL strings gracefully in columns", async () => {
    await db.query("CREATE TABLE t_null_str (id INT PRIMARY KEY, name TEXT);");
    await db.query("INSERT INTO t_null_str VALUES (1, NULL), (2, 'Alice');");
    const res = await db.query("SELECT id, name FROM t_null_str WHERE name IS NULL;");
    expect(res.length).toBe(1);
    expect(res[0].id).toBe(1);
  });

  it("010: should cast text representation to integer and float", async () => {
    const res = await db.query("SELECT '999'::INT AS i, '12.34'::FLOAT AS f;");
    expect(res[0].i).toBe(999);
    expect(res[0].f).toBeCloseTo(12.34);
  });

  // ==========================================
  // Section 2: String Concatenation & CONCAT / CONCAT_WS (11-20)
  // ==========================================

  it("011: should concatenate two strings using || operator", async () => {
    const res = await db.query("SELECT 'Hello' || ' ' || 'World' AS greeting;");
    expect(res[0].greeting).toBe("Hello World");
  });

  it("012: should return NULL when concatenating with NULL using || operator", async () => {
    const res = await db.query("SELECT 'Hello' || NULL AS val1, NULL || 'World' AS val2;");
    expect(res[0].val1).toBeNull();
    expect(res[0].val2).toBeNull();
  });

  it("013: should concatenate using CONCAT() function ignoring NULLs", async () => {
    const res = await db.query("SELECT CONCAT('Hello', ' ', 'World', NULL, '!') AS greeting;");
    expect(res[0].greeting).toBe("Hello World!");
  });

  it("014: should return empty string from CONCAT() with all NULLs", async () => {
    const res = await db.query("SELECT CONCAT(NULL, NULL) AS val;");
    expect(res[0].val).toBe("");
  });

  it("015: should concatenate non-string values with CONCAT()", async () => {
    const res = await db.query("SELECT CONCAT('User_', 42, '_', true) AS user_code;");
    expect(res[0].user_code).toBe("User_42_true");
  });

  it("016: should concatenate with separator using CONCAT_WS()", async () => {
    const res = await db.query("SELECT CONCAT_WS(', ', 'First', 'Second', 'Third') AS list;");
    expect(res[0].list).toBe("First, Second, Third");
  });

  it("017: should skip NULL arguments in CONCAT_WS()", async () => {
    const res = await db.query("SELECT CONCAT_WS('-', '2026', NULL, '09', '09') AS date_str;");
    expect(res[0].date_str).toBe("2026-09-09");
  });

  it("018: should return NULL if separator is NULL in CONCAT_WS()", async () => {
    const res = await db.query("SELECT CONCAT_WS(NULL, 'a', 'b', 'c') AS res;");
    expect(res[0].res).toBeNull();
  });

  it("019: should concatenate table columns in query using ||", async () => {
    await db.query("CREATE TABLE t_names (first_name TEXT, last_name TEXT);");
    await db.query("INSERT INTO t_names VALUES ('Nguyen', 'Son'), ('John', 'Doe');");
    const res = await db.query("SELECT first_name || ' ' || last_name AS full_name FROM t_names ORDER BY first_name DESC;");
    expect(res[0].full_name).toBe("Nguyen Son");
    expect(res[1].full_name).toBe("John Doe");
  });

  it("020: should perform chained concatenation with numbers in WHERE clause", async () => {
    await db.query("CREATE TABLE t_items (prefix TEXT, code INT);");
    await db.query("INSERT INTO t_items VALUES ('SKU-', 101), ('SKU-', 102);");
    const res = await db.query("SELECT * FROM t_items WHERE prefix || code = 'SKU-102';");
    expect(res.length).toBe(1);
    expect(res[0].code).toBe(102);
  });

  // ==========================================
  // Section 3: Substrings, Extraction & Slicing (21-30)
  // ==========================================

  it("021: should extract substring using SUBSTRING(str, start, length)", async () => {
    const res = await db.query("SELECT SUBSTRING('PostgreSQL', 1, 4) AS sub;");
    expect(res[0].sub).toBe("Post");
  });

  it("022: should extract substring using SUBSTRING(str, start) to end of string", async () => {
    const res = await db.query("SELECT SUBSTRING('PostgreSQL', 5) AS sub;");
    expect(res[0].sub).toBe("greSQL");
  });

  it("023: should support SUBSTR() as synonym for SUBSTRING()", async () => {
    const res = await db.query("SELECT SUBSTR('Database', 1, 4) AS sub1, SUBSTR('Database', 5) AS sub2;");
    expect(res[0].sub1).toBe("Data");
    expect(res[0].sub2).toBe("base");
  });

  it("024: should extract leftmost N characters using LEFT(str, n)", async () => {
    const res = await db.query("SELECT LEFT('Alphabet', 5) AS val;");
    expect(res[0].val).toBe("Alpha");
  });

  it("025: should extract rightmost N characters using RIGHT(str, n)", async () => {
    const res = await db.query("SELECT RIGHT('Alphabet', 3) AS val;");
    expect(res[0].val).toBe("bet");
  });

  it("026: should handle negative n in LEFT(str, -n)", async () => {
    const res = await db.query("SELECT LEFT('Alphabet', -2) AS val;");
    expect(res[0].val).toBe("Alphab");
  });

  it("027: should handle negative n in RIGHT(str, -n)", async () => {
    const res = await db.query("SELECT RIGHT('Alphabet', -2) AS val;");
    expect(res[0].val).toBe("phabet");
  });

  it("028: should return empty string when SUBSTRING start is beyond length", async () => {
    const res = await db.query("SELECT SUBSTRING('Hello', 10, 2) AS sub;");
    expect(res[0].sub).toBe("");
  });

  it("029: should return NULL when SUBSTRING input is NULL", async () => {
    const res = await db.query("SELECT SUBSTRING(NULL, 1, 2) AS sub;");
    expect(res[0].sub).toBeNull();
  });

  it("030: should handle SUBSTRING with large count exceeding remaining characters", async () => {
    const res = await db.query("SELECT SUBSTRING('Hello', 2, 100) AS sub;");
    expect(res[0].sub).toBe("ello");
  });

  // ==========================================
  // Section 4: String Length, Case, Trimming & Padding (31-45)
  // ==========================================

  it("031: should compute string length with LENGTH()", async () => {
    const res = await db.query("SELECT LENGTH('Hello World') AS len;");
    expect(res[0].len).toBe(11);
  });

  it("032: should compute string length with CHAR_LENGTH() and CHARACTER_LENGTH()", async () => {
    const res = await db.query("SELECT CHAR_LENGTH('Postgres') AS c_len, CHARACTER_LENGTH('Postgres') AS ch_len;");
    expect(res[0].c_len).toBe(8);
    expect(res[0].ch_len).toBe(8);
  });

  it("033: should compute byte length with OCTET_LENGTH() for ASCII and multi-byte UTF-8", async () => {
    const res = await db.query("SELECT OCTET_LENGTH('ABC') AS ascii_bytes, OCTET_LENGTH('Xin chào') AS utf8_bytes;");
    expect(res[0].ascii_bytes).toBe(3);
    expect(res[0].utf8_bytes).toBe(9); // 'à' is 2 bytes in UTF-8
  });

  it("034: should convert string to UPPER() and LOWER()", async () => {
    const res = await db.query("SELECT UPPER('hello postgres') AS u, LOWER('HELLO POSTGRES') AS l;");
    expect(res[0].u).toBe("HELLO POSTGRES");
    expect(res[0].l).toBe("hello postgres");
  });

  it("035: should convert string to title case using INITCAP()", async () => {
    const res = await db.query("SELECT INITCAP('hello world of postgres') AS cap;");
    expect(res[0].cap).toBe("Hello World Of Postgres");
  });

  it("036: should trim leading and trailing spaces with TRIM()", async () => {
    const res = await db.query("SELECT TRIM('   trimmed content   ') AS t;");
    expect(res[0].t).toBe("trimmed content");
  });

  it("037: should support BTRIM() with custom characters", async () => {
    const res = await db.query("SELECT BTRIM('xxHello Worldxx', 'x') AS t;");
    expect(res[0].t).toBe("Hello World");
  });

  it("038: should trim only leading characters with LTRIM()", async () => {
    const res = await db.query("SELECT LTRIM('   start') AS t1, LTRIM('zzstart', 'z') AS t2;");
    expect(res[0].t1).toBe("start");
    expect(res[0].t2).toBe("start");
  });

  it("039: should trim only trailing characters with RTRIM()", async () => {
    const res = await db.query("SELECT RTRIM('end   ') AS t1, RTRIM('endzz', 'z') AS t2;");
    expect(res[0].t1).toBe("end");
    expect(res[0].t2).toBe("end");
  });

  it("040: should pad string on left with LPAD()", async () => {
    const res = await db.query("SELECT LPAD('123', 6, '0') AS padded;");
    expect(res[0].padded).toBe("000123");
  });

  it("041: should pad string on right with RPAD()", async () => {
    const res = await db.query("SELECT RPAD('item', 8, '.') AS padded;");
    expect(res[0].padded).toBe("item....");
  });

  it("042: should truncate string in LPAD and RPAD if requested length is shorter", async () => {
    const res = await db.query("SELECT LPAD('LongString', 4, ' ') AS l, RPAD('LongString', 4, ' ') AS r;");
    expect(res[0].l).toBe("Long");
    expect(res[0].r).toBe("Long");
  });

  it("043: should repeat string with REPEAT()", async () => {
    const res = await db.query("SELECT REPEAT('Ha', 3) AS laughs, REPEAT('X', 0) AS empty_rep;");
    expect(res[0].laughs).toBe("HaHaHa");
    expect(res[0].empty_rep).toBe("");
  });

  it("044: should reverse string with REVERSE()", async () => {
    const res = await db.query("SELECT REVERSE('abcdef') AS rev;");
    expect(res[0].rev).toBe("fedcba");
  });

  it("045: should get ASCII code and create char with ASCII() and CHR()", async () => {
    const res = await db.query("SELECT ASCII('A') AS code, CHR(65) AS char_val;");
    expect(res[0].code).toBe(65);
    expect(res[0].char_val).toBe("A");
  });

  // ==========================================
  // Section 5: String Transformation, Search & Replace (46-55)
  // ==========================================

  it("046: should replace substring occurrences with REPLACE()", async () => {
    const res = await db.query("SELECT REPLACE('foo bar foo baz foo', 'foo', 'qux') AS res;");
    expect(res[0].res).toBe("qux bar qux baz qux");
  });

  it("047: should translate individual characters with TRANSLATE()", async () => {
    const res = await db.query("SELECT TRANSLATE('12345-12345', '14', 'ax') AS res;");
    expect(res[0].res).toBe("a23x5-a23x5");
  });

  it("048: should delete characters with TRANSLATE() when 'to' is shorter than 'from'", async () => {
    const res = await db.query("SELECT TRANSLATE('12345', '143', 'ax') AS res;");
    expect(res[0].res).toBe("a2x5"); // 3 is deleted
  });

  it("049: should find substring position with STRPOS() (1-indexed)", async () => {
    const res = await db.query("SELECT STRPOS('high high high', 'ig') AS pos1, STRPOS('hello', 'xyz') AS pos2;");
    expect(res[0].pos1).toBe(2);
    expect(res[0].pos2).toBe(0);
  });

  it("050: should split string and extract part with SPLIT_PART()", async () => {
    const res = await db.query("SELECT SPLIT_PART('apple,banana,cherry,date', ',', 2) AS part2, SPLIT_PART('apple,banana,cherry,date', ',', 4) AS part4;");
    expect(res[0].part2).toBe("banana");
    expect(res[0].part4).toBe("date");
  });

  it("051: should return empty string from SPLIT_PART() when field index is out of bounds", async () => {
    const res = await db.query("SELECT SPLIT_PART('a-b-c', '-', 10) AS res;");
    expect(res[0].res).toBe("");
  });

  it("052: should compute MD5 hash of a string with MD5()", async () => {
    const res = await db.query("SELECT MD5('pglite_test') AS hash;");
    expect(res[0].hash).toBe("d06c2b3326e4dbb8010dac82076a9698");
  });

  it("053: should encode string to hex and base64 with ENCODE()", async () => {
    const res = await db.query("SELECT ENCODE('Hello', 'hex') AS hex, ENCODE('Hello', 'base64') AS b64;");
    expect(res[0].hex).toBe("48656c6c6f");
    expect(res[0].b64).toBe("SGVsbG8=");
  });

  it("054: should decode hex and base64 back to string with DECODE()", async () => {
    const res = await db.query("SELECT DECODE('48656c6c6f', 'hex') AS txt1, DECODE('SGVsbG8=', 'base64') AS txt2;");
    expect(res[0].txt1).toBe("Hello");
    expect(res[0].txt2).toBe("Hello");
  });

  it("055: should combine multiple string functions in a single expression", async () => {
    const res = await db.query("SELECT UPPER(TRIM(REPLACE('  hello-world  ', '-', ' '))) AS res;");
    expect(res[0].res).toBe("HELLO WORLD");
  });

  // ==========================================
  // Section 6: LIKE, ILIKE & Pattern Matching (56-68)
  // ==========================================

  it("056: should match prefix with LIKE 'prefix%'", async () => {
    await db.query("CREATE TABLE t_like (val TEXT);");
    await db.query("INSERT INTO t_like VALUES ('Postgres'), ('Postman'), ('MySQL'), ('SQLite');");
    const res = await db.query("SELECT val FROM t_like WHERE val LIKE 'Post%' ORDER BY val;");
    expect(res.length).toBe(2);
    expect(res[0].val).toBe("Postgres");
    expect(res[1].val).toBe("Postman");
  });

  it("057: should match suffix with LIKE '%suffix'", async () => {
    await db.query("CREATE TABLE t_like2 (val TEXT);");
    await db.query("INSERT INTO t_like2 VALUES ('Postgres'), ('Postman'), ('MySQL'), ('SQLite');");
    const res = await db.query("SELECT val FROM t_like2 WHERE val LIKE '%SQL' ORDER BY val;");
    expect(res.length).toBe(1);
    expect(res[0].val).toBe("MySQL");
  });

  it("058: should match substring with LIKE '%sub%'", async () => {
    await db.query("CREATE TABLE t_like3 (val TEXT);");
    await db.query("INSERT INTO t_like3 VALUES ('pg_database'), ('my_table'), ('db_backup');");
    const res = await db.query("SELECT val FROM t_like3 WHERE val LIKE '%data%' ORDER BY val;");
    expect(res.length).toBe(1);
    expect(res[0].val).toBe("pg_database");
  });

  it("059: should match single character with wildcard '_'", async () => {
    await db.query("CREATE TABLE t_like4 (code TEXT);");
    await db.query("INSERT INTO t_like4 VALUES ('A1'), ('A2'), ('AB'), ('A12');");
    const res = await db.query("SELECT code FROM t_like4 WHERE code LIKE 'A_' ORDER BY code;");
    expect(res.length).toBe(3);
    expect(res.map((r) => r.code)).toEqual(["A1", "A2", "AB"]);
  });

  it("060: should support case-insensitive ILIKE matching", async () => {
    await db.query("CREATE TABLE t_ilike (email TEXT);");
    await db.query("INSERT INTO t_ilike VALUES ('User@Example.COM'), ('admin@test.org'), ('INFO@EXAMPLE.COM');");
    const res = await db.query("SELECT email FROM t_ilike WHERE email ILIKE '%@example.com' ORDER BY email;");
    expect(res.length).toBe(2);
    expect(res[0].email).toBe("INFO@EXAMPLE.COM");
    expect(res[1].email).toBe("User@Example.COM");
  });

  it("061: should filter with NOT LIKE", async () => {
    await db.query("CREATE TABLE t_nlike (code TEXT);");
    await db.query("INSERT INTO t_nlike VALUES ('dev_1'), ('test_1'), ('prod_1');");
    const res = await db.query("SELECT code FROM t_nlike WHERE code NOT LIKE 'dev%' ORDER BY code;");
    expect(res.length).toBe(2);
    expect(res.map((r) => r.code)).toEqual(["prod_1", "test_1"]);
  });

  it("062: should filter with NOT ILIKE", async () => {
    await db.query("CREATE TABLE t_nilike (code TEXT);");
    await db.query("INSERT INTO t_nilike VALUES ('DEV_1'), ('test_1'), ('prod_1');");
    const res = await db.query("SELECT code FROM t_nilike WHERE code NOT ILIKE 'dev%' ORDER BY code;");
    expect(res.length).toBe(2);
    expect(res.map((r) => r.code)).toEqual(["prod_1", "test_1"]);
  });

  it("063: should handle LIKE with escaped special characters using ESCAPE clause", async () => {
    await db.query("CREATE TABLE t_esc (val TEXT);");
    await db.query("INSERT INTO t_esc VALUES ('100% discount'), ('100 dollars'), ('50% off');");
    const res = await db.query("SELECT val FROM t_esc WHERE val LIKE '%\\%%' ESCAPE '\\' ORDER BY val;");
    expect(res.length).toBe(2);
    expect(res[0].val).toBe("100% discount");
    expect(res[1].val).toBe("50% off");
  });

  it("064: should handle LIKE with escaped underscore using ESCAPE clause", async () => {
    await db.query("CREATE TABLE t_esc2 (val TEXT);");
    await db.query("INSERT INTO t_esc2 VALUES ('user_name'), ('username'), ('user_id');");
    const res = await db.query("SELECT val FROM t_esc2 WHERE val LIKE 'user\\_%' ESCAPE '\\' ORDER BY val;");
    expect(res.length).toBe(2);
    expect(res[0].val).toBe("user_id");
    expect(res[1].val).toBe("user_name");
  });

  it("065: should match exact string with LIKE without wildcards", async () => {
    const res = await db.query("SELECT 'hello' LIKE 'hello' AS m1, 'hello' LIKE 'world' AS m2;");
    expect(res[0].m1).toBe(true);
    expect(res[0].m2).toBe(false);
  });

  it("066: should return NULL when left operand is NULL in LIKE", async () => {
    const res = await db.query("SELECT NULL LIKE '%abc%' AS m;");
    expect(res[0].m).toBeNull();
  });

  it("067: should return NULL when pattern is NULL in LIKE", async () => {
    const res = await db.query("SELECT 'abc' LIKE NULL AS m;");
    expect(res[0].m).toBeNull();
  });

  it("068: should support LIKE in CASE expressions", async () => {
    await db.query("CREATE TABLE t_files (name TEXT);");
    await db.query("INSERT INTO t_files VALUES ('report.pdf'), ('photo.jpg'), ('notes.txt');");
    const res = await db.query(`
      SELECT name,
             CASE
               WHEN name LIKE '%.pdf' THEN 'Document'
               WHEN name LIKE '%.jpg' THEN 'Image'
               ELSE 'Other'
             END AS file_type
      FROM t_files ORDER BY name;
    `);
    expect(res[0].file_type).toBe("Other");
    expect(res[1].file_type).toBe("Image");
    expect(res[2].file_type).toBe("Document");
  });

  // ==========================================
  // Section 7: Regular Expressions (~, ~*, !~, !~*) (69-80)
  // ==========================================

  it("069: should match regex case-sensitive with ~", async () => {
    const res = await db.query("SELECT 'PostgreSQL 16' ~ 'SQL \\d+' AS m1, 'postgresql 16' ~ 'SQL \\d+' AS m2;");
    expect(res[0].m1).toBe(true);
    expect(res[0].m2).toBe(false);
  });

  it("070: should match regex case-insensitive with ~*", async () => {
    const res = await db.query("SELECT 'PostgreSQL 16' ~* 'sql \\d+' AS m1, 'mysql 8' ~* 'sql \\d+' AS m2;");
    expect(res[0].m1).toBe(true);
    expect(res[0].m2).toBe(true);
  });

  it("071: should match regex negation case-sensitive with !~", async () => {
    const res = await db.query("SELECT 'PostgreSQL' !~ '^post' AS m1, 'postgresql' !~ '^post' AS m2;");
    expect(res[0].m1).toBe(true);
    expect(res[0].m2).toBe(false);
  });

  it("072: should match regex negation case-insensitive with !~*", async () => {
    const res = await db.query("SELECT 'PostgreSQL' !~* '^post' AS m1, 'MySQL' !~* '^post' AS m2;");
    expect(res[0].m1).toBe(false);
    expect(res[0].m2).toBe(true);
  });

  it("073: should filter table rows using regex ~ in WHERE clause", async () => {
    await db.query("CREATE TABLE t_phones (phone TEXT);");
    await db.query("INSERT INTO t_phones VALUES ('+84-912345678'), ('0987654321'), ('invalid-phone'), ('123-abc');");
    const res = await db.query("SELECT phone FROM t_phones WHERE phone ~ '^\\+?[0-9\\-]+$' ORDER BY phone;");
    expect(res.length).toBe(2);
    expect(res[0].phone).toBe("+84-912345678");
    expect(res[1].phone).toBe("0987654321");
  });

  it("074: should match email pattern with ~*", async () => {
    await db.query("CREATE TABLE t_emails (email TEXT);");
    await db.query("INSERT INTO t_emails VALUES ('user@domain.com'), ('admin@sub.domain.org'), ('not-an-email');");
    const res = await db.query("SELECT email FROM t_emails WHERE email ~* '^[a-z0-9._%+-]+@[a-z0-9.-]+\\.[a-z]{2,}$' ORDER BY email;");
    expect(res.length).toBe(2);
    expect(res[0].email).toBe("admin@sub.domain.org");
    expect(res[1].email).toBe("user@domain.com");
  });

  it("075: should match digits and character sets with regex", async () => {
    const res = await db.query("SELECT 'ABC-123' ~ '^[A-Z]{3}-\\d{3}$' AS m;");
    expect(res[0].m).toBe(true);
  });

  it("076: should match start and end anchors ^ and $ correctly", async () => {
    const res = await db.query("SELECT 'abc' ~ '^a' AS m_start, 'abc' ~ 'c$' AS m_end, 'abc' ~ '^b$' AS m_exact;");
    expect(res[0].m_start).toBe(true);
    expect(res[0].m_end).toBe(true);
    expect(res[0].m_exact).toBe(false);
  });

  it("077: should support regex alternation |", async () => {
    await db.query("CREATE TABLE t_colors (name TEXT);");
    await db.query("INSERT INTO t_colors VALUES ('red'), ('green'), ('blue'), ('yellow');");
    const res = await db.query("SELECT name FROM t_colors WHERE name ~ '^(red|blue)$' ORDER BY name;");
    expect(res.length).toBe(2);
    expect(res[0].name).toBe("blue");
    expect(res[1].name).toBe("red");
  });

  it("078: should handle NULL with regex operators", async () => {
    const res = await db.query("SELECT (NULL ~ 'abc') AS r1, ('abc' ~ NULL) AS r2;");
    expect(res[0].r1).toBeNull();
    expect(res[0].r2).toBeNull();
  });

  it("079: should use regex in HAVING clause after GROUP BY", async () => {
    await db.query("CREATE TABLE t_tags (item_id INT, tag TEXT);");
    await db.query("INSERT INTO t_tags VALUES (1, 'v1.0'), (1, 'beta'), (2, 'v2.1'), (3, 'deprecated');");
    const res = await db.query(`
      SELECT item_id, MAX(tag) AS latest_tag
      FROM t_tags
      GROUP BY item_id
      HAVING MAX(tag) ~ '^v[0-9]'
      ORDER BY item_id;
    `);
    expect(res.length).toBe(2);
    expect(res[0].item_id).toBe(1);
    expect(res[1].item_id).toBe(2);
  });

  it("080: should support regex matching in subqueries", async () => {
    await db.query("CREATE TABLE t_domains (domain TEXT);");
    await db.query("INSERT INTO t_domains VALUES ('google.com'), ('github.io'), ('invalid');");
    const res = await db.query("SELECT * FROM (SELECT domain FROM t_domains WHERE domain ~* '\\.(com|io)$') sub ORDER BY domain;");
    expect(res.length).toBe(2);
    expect(res[0].domain).toBe("github.io");
    expect(res[1].domain).toBe("google.com");
  });

  // ==========================================
  // Section 8: Unicode, Vietnamese Diacritics & Internationalization (81-90)
  // ==========================================

  it("081: should store and retrieve Vietnamese diacritic strings accurately", async () => {
    await db.query("CREATE TABLE t_vn (id INT PRIMARY KEY, city TEXT, description TEXT);");
    await db.query("INSERT INTO t_vn VALUES (1, 'Hà Nội', 'Thủ đô ngàn năm văn hiến'), (2, 'TP. Hồ Chí Minh', 'Trung tâm kinh tế lớn nhất');");
    const res = await db.query("SELECT * FROM t_vn WHERE city = 'Hà Nội';");
    expect(res.length).toBe(1);
    expect(res[0].city).toBe("Hà Nội");
    expect(res[0].description).toBe("Thủ đô ngàn năm văn hiến");
  });

  it("082: should perform LIKE pattern matching on Vietnamese text", async () => {
    await db.query("CREATE TABLE t_vn_users (name TEXT);");
    await db.query("INSERT INTO t_vn_users VALUES ('Nguyễn Văn A'), ('Trần Thị B'), ('Nguyễn Thị C'), ('Lê Hoàng D');");
    const res = await db.query("SELECT name FROM t_vn_users WHERE name LIKE 'Nguyễn%' ORDER BY name;");
    expect(res.length).toBe(2);
    expect(res[0].name).toBe("Nguyễn Thị C");
    expect(res[1].name).toBe("Nguyễn Văn A");
  });

  it("083: should convert Vietnamese text with UPPER() and LOWER()", async () => {
    const res = await db.query("SELECT UPPER('tiếng việt') AS u, LOWER('ĐÀ NẴNG') AS l;");
    expect(res[0].u).toBe("TIẾNG VIỆT");
    expect(res[0].l).toBe("đà nẵng");
  });

  it("084: should handle Vietnamese vowels with tone marks in LENGTH()", async () => {
    const res = await db.query("SELECT LENGTH('Hồ Chí Minh') AS char_cnt, OCTET_LENGTH('Hồ Chí Minh') AS byte_cnt;");
    expect(res[0].char_cnt).toBe(11);
    expect(res[0].byte_cnt).toBeGreaterThan(11);
  });

  it("085: should order Vietnamese strings correctly with ORDER BY", async () => {
    await db.query("CREATE TABLE t_provinces (name TEXT);");
    await db.query("INSERT INTO t_provinces VALUES ('Đà Nẵng'), ('Cần Thơ'), ('An Giang'), ('Bình Dương');");
    const res = await db.query("SELECT name FROM t_provinces ORDER BY name ASC;");
    expect(res[0].name).toBe("An Giang");
    expect(res[1].name).toBe("Bình Dương");
    expect(res[2].name).toBe("Cần Thơ");
    expect(res[3].name).toBe("Đà Nẵng");
  });

  it("086: should store and query international characters (Japanese, Chinese, Arabic, Russian)", async () => {
    await db.query("CREATE TABLE t_i18n (lang TEXT, greeting TEXT);");
    await db.query(`
      INSERT INTO t_i18n VALUES 
        ('Japanese', 'こんにちは'),
        ('Chinese', '你好世界'),
        ('Arabic', 'مرحبا'),
        ('Russian', 'Привет мир');
    `);
    const res = await db.query("SELECT greeting FROM t_i18n WHERE lang = 'Japanese';");
    expect(res[0].greeting).toBe("こんにちは");
  });

  it("087: should store and query emojis correctly", async () => {
    await db.query("CREATE TABLE t_emoji (id INT PRIMARY KEY, title TEXT, emoji TEXT);");
    await db.query("INSERT INTO t_emoji VALUES (1, 'Rocket', '🚀'), (2, 'Fire', '🔥'), (3, 'Vietnam Flag', '🇻🇳');");
    const res = await db.query("SELECT title || ' ' || emoji AS display FROM t_emoji WHERE emoji = '🚀';");
    expect(res[0].display).toBe("Rocket 🚀");
  });

  it("088: should compute emoji string length and concatenation", async () => {
    const res = await db.query("SELECT CONCAT('Hello ', '🇻🇳', '!') AS banner;");
    expect(res[0].banner).toBe("Hello 🇻🇳!");
  });

  it("089: should filter emojis using LIKE '%🚀%'", async () => {
    await db.query("CREATE TABLE t_status (msg TEXT);");
    await db.query("INSERT INTO t_status VALUES ('Deploy success 🚀🎉'), ('Bug found 🐛'), ('Fixing 🔧');");
    const res = await db.query("SELECT msg FROM t_status WHERE msg LIKE '%🚀%' ORDER BY msg;");
    expect(res.length).toBe(1);
    expect(res[0].msg).toBe("Deploy success 🚀🎉");
  });

  it("090: should match Unicode characters in regex ~", async () => {
    await db.query("CREATE TABLE t_vn_regex (name TEXT);");
    await db.query("INSERT INTO t_vn_regex VALUES ('Nguyễn'), ('Trần'), ('Phạm'), ('Hoàng');");
    const res = await db.query("SELECT name FROM t_vn_regex WHERE name ~ '^(Nguyễn|Trần)$' ORDER BY name;");
    expect(res.length).toBe(2);
    expect(res[0].name).toBe("Nguyễn");
    expect(res[1].name).toBe("Trần");
  });

  // ==========================================
  // Section 9: Advanced Queries, Parameters & Aggregations (91-100)
  // ==========================================

  it("091: should bind string parameters with $1, $2", async () => {
    await db.query("CREATE TABLE t_params (id INT PRIMARY KEY, name TEXT, email TEXT);");
    await db.query("INSERT INTO t_params VALUES ($1, $2, $3);", [1, "Son Nguyen", "son@example.com"]);
    const res = await db.query("SELECT * FROM t_params WHERE name = $1 AND email = $2;", ["Son Nguyen", "son@example.com"]);
    expect(res.length).toBe(1);
    expect(res[0].name).toBe("Son Nguyen");
  });

  it("092: should aggregate strings using STRING_AGG() with delimiter", async () => {
    await db.query("CREATE TABLE t_fruits (category TEXT, fruit TEXT);");
    await db.query("INSERT INTO t_fruits VALUES ('Citrus', 'Orange'), ('Citrus', 'Lemon'), ('Citrus', 'Lime');");
    const res = await db.query("SELECT category, STRING_AGG(fruit, ', ') AS fruit_list FROM t_fruits GROUP BY category;");
    expect(res[0].fruit_list).toBe("Orange, Lemon, Lime");
  });

  it("093: should compute MIN() and MAX() lexicographically on strings", async () => {
    await db.query("CREATE TABLE t_lex (val TEXT);");
    await db.query("INSERT INTO t_lex VALUES ('Apple'), ('Banana'), ('Cherry'), ('Date');");
    const res = await db.query("SELECT MIN(val) AS first_val, MAX(val) AS last_val FROM t_lex;");
    expect(res[0].first_val).toBe("Apple");
    expect(res[0].last_val).toBe("Date");
  });

  it("094: should group by transformed string expression", async () => {
    await db.query("CREATE TABLE t_domains_grp (email TEXT);");
    await db.query("INSERT INTO t_domains_grp VALUES ('a@gmail.com'), ('b@GMAIL.COM'), ('c@yahoo.com');");
    const res = await db.query(`
      SELECT LOWER(SPLIT_PART(email, '@', 2)) AS domain, COUNT(*) AS cnt
      FROM t_domains_grp
      GROUP BY LOWER(SPLIT_PART(email, '@', 2))
      ORDER BY domain;
    `);
    expect(res.length).toBe(2);
    expect(res[0].domain).toBe("gmail.com");
    expect(res[0].cnt).toBe(2);
    expect(res[1].domain).toBe("yahoo.com");
    expect(res[1].cnt).toBe(1);
  });

  it("095: should perform string comparison with BETWEEN ... AND ...", async () => {
    await db.query("CREATE TABLE t_alpha (letter TEXT);");
    await db.query("INSERT INTO t_alpha VALUES ('B'), ('C'), ('D'), ('M'), ('Z');");
    const res = await db.query("SELECT letter FROM t_alpha WHERE letter BETWEEN 'C' AND 'M' ORDER BY letter;");
    expect(res.map((r) => r.letter)).toEqual(["C", "D", "M"]);
  });

  it("096: should filter strings using IN ('a', 'b', 'c')", async () => {
    await db.query("CREATE TABLE t_statuses (status TEXT);");
    await db.query("INSERT INTO t_statuses VALUES ('active'), ('pending'), ('suspended'), ('deleted');");
    const res = await db.query("SELECT status FROM t_statuses WHERE status IN ('active', 'pending') ORDER BY status;");
    expect(res.map((r) => r.status)).toEqual(["active", "pending"]);
  });

  it("097: should perform window functions over string column ordering", async () => {
    await db.query("CREATE TABLE t_cust (id INT, name TEXT);");
    await db.query("INSERT INTO t_cust VALUES (1, 'Bob'), (2, 'Alice'), (3, 'Charlie');");
    const res = await db.query(`
      SELECT name, ROW_NUMBER() OVER (ORDER BY name ASC) AS seq
      FROM t_cust
      ORDER BY seq;
    `);
    expect(res[0].name).toBe("Alice");
    expect(res[0].seq).toBe(1);
    expect(res[1].name).toBe("Bob");
    expect(res[1].seq).toBe(2);
    expect(res[2].name).toBe("Charlie");
    expect(res[2].seq).toBe(3);
  });

  it("098: should join tables on string columns with UPPER() normalization", async () => {
    await db.query("CREATE TABLE t_j1 (code TEXT, val1 INT);");
    await db.query("CREATE TABLE t_j2 (code TEXT, val2 INT);");
    await db.query("INSERT INTO t_j1 VALUES ('abc', 10), ('def', 20);");
    await db.query("INSERT INTO t_j2 VALUES ('ABC', 100), ('DEF', 200);");
    const res = await db.query(`
      SELECT t_j1.code AS c1, t_j2.code AS c2, t_j1.val1 + t_j2.val2 AS total
      FROM t_j1
      JOIN t_j2 ON UPPER(t_j1.code) = UPPER(t_j2.code)
      ORDER BY t_j1.code;
    `);
    expect(res.length).toBe(2);
    expect(res[0].total).toBe(110);
    expect(res[1].total).toBe(220);
  });

  it("099: should use CTE to prepare and format string reports", async () => {
    await db.query("CREATE TABLE t_raw_logs (service TEXT, level TEXT, msg TEXT);");
    await db.query("INSERT INTO t_raw_logs VALUES ('auth', 'error', 'Token expired'), ('billing', 'info', 'Invoice sent');");
    const res = await db.query(`
      WITH formatted_logs AS (
        SELECT UPPER(service) || ' [' || UPPER(level) || ']: ' || msg AS log_entry
        FROM t_raw_logs
      )
      SELECT log_entry FROM formatted_logs ORDER BY log_entry;
    `);
    expect(res.length).toBe(2);
    expect(res[0].log_entry).toBe("AUTH [ERROR]: Token expired");
    expect(res[1].log_entry).toBe("BILLING [INFO]: Invoice sent");
  });

  it("100: should handle full lifecycle: create, insert, update, replace, substring, search and drop", async () => {
    await db.query("CREATE TABLE t_docs (id SERIAL PRIMARY KEY, title TEXT, tags TEXT);");
    await db.query("INSERT INTO t_docs (title, tags) VALUES ('Getting Started with PGLite', 'db,typescript,sql');");
    await db.query("UPDATE t_docs SET tags = REPLACE(tags, 'typescript', 'bun') WHERE id = 1;");
    const res = await db.query("SELECT title, tags FROM t_docs WHERE tags LIKE '%bun%';");
    expect(res.length).toBe(1);
    expect(res[0].tags).toBe("db,bun,sql");
    await db.query("DROP TABLE t_docs;");
  });
});
