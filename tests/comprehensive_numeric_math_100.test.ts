import { describe, test, expect, beforeAll, afterAll } from "bun:test";
import { PGLite } from "../src/index";

describe("100 Comprehensive In-Depth PostgreSQL NUMERIC, MATH & BITWISE Test Cases for PGLite", () => {
  let db: PGLite;

  beforeAll(async () => {
    db = new PGLite(":memory:", { native: true });

    // Create tables with various numeric data types
    await db.query(`
      CREATE TABLE products (
        product_id SERIAL PRIMARY KEY,
        name TEXT NOT NULL,
        category TEXT NOT NULL,
        price DECIMAL(10, 2) NOT NULL,
        cost NUMERIC(10, 4),
        stock_qty INT DEFAULT 0,
        reorder_level SMALLINT DEFAULT 10,
        total_revenue BIGINT DEFAULT 0,
        weight_kg REAL,
        rating DOUBLE PRECISION,
        flags INT DEFAULT 0
      );
    `);

    await db.query(`
      CREATE TABLE financial_ledger (
        entry_id BIGSERIAL PRIMARY KEY,
        account_code INT NOT NULL,
        amount DECIMAL(12, 2) NOT NULL,
        balance_after NUMERIC(14, 2),
        tax_rate REAL DEFAULT 0.10,
        is_credit BOOLEAN DEFAULT false
      );
    `);

    await db.query(`
      CREATE TABLE math_samples (
        id SERIAL PRIMARY KEY,
        val_int INT,
        val_float DOUBLE PRECISION,
        val_neg INT,
        val_null INT
      );
    `);

    // Seed products data
    await db.query(`
      INSERT INTO products (product_id, name, category, price, cost, stock_qty, reorder_level, total_revenue, weight_kg, rating, flags) VALUES
      (1, 'Laptop Pro', 'electronics', 1299.99, 850.5000, 45, 5, 58499, 1.85, 4.8, 5),
      (2, 'Wireless Mouse', 'electronics', 29.95, 12.2000, 150, 20, 4492, 0.12, 4.5, 1),
      (3, 'Mechanical Keyboard', 'electronics', 89.50, 45.0000, 80, 15, 7160, 0.95, 4.7, 3),
      (4, 'Standing Desk', 'furniture', 450.00, 220.0000, 20, 5, 9000, 32.50, 4.6, 2),
      (5, 'Ergonomic Chair', 'furniture', 299.00, 140.0000, 35, 10, 10465, 18.20, 4.4, 6),
      (6, 'USB-C Cable', 'accessories', 9.99, 2.1500, 500, 50, 4995, 0.05, 4.2, 0),
      (7, 'Monitor 4K', 'electronics', 399.99, 210.0000, 25, 8, 9999, 6.40, 4.9, 7),
      (8, 'Desk Lamp', 'accessories', 34.50, 15.0000, 0, 10, 0, 1.10, 3.8, 4);
    `);

    // Seed financial ledger data
    await db.query(`
      INSERT INTO financial_ledger (entry_id, account_code, amount, balance_after, tax_rate, is_credit) VALUES
      (1, 1001, 5000.00, 5000.00, 0.08, true),
      (2, 1001, 1250.50, 6250.50, 0.08, true),
      (3, 1001, -750.25, 5500.25, 0.08, false),
      (4, 2002, 12000.00, 12000.00, 0.10, true),
      (5, 2002, -3400.00, 8600.00, 0.10, false);
    `);

    // Seed math samples
    await db.query(`
      INSERT INTO math_samples (id, val_int, val_float, val_neg, val_null) VALUES
      (1, 16, 2.71828, -25, NULL),
      (2, 64, 3.14159, -100, NULL),
      (3, 100, 0.5, -9, NULL);
    `);
  });

  afterAll(async () => {
    if (db) {
      await db.close();
    }
  });

  // =========================================================================
  // Section 1: Numeric Data Types & Storage (Tests 1 - 15)
  // =========================================================================
  describe("Section 1: Numeric Data Types & Storage", () => {
    test("1. INT / INTEGER stores and retrieves standard 32-bit integers", async () => {
      const res = await db.query(`SELECT stock_qty FROM products WHERE product_id = 1;`);
      expect(Number(res[0].stock_qty)).toBe(45);
    });

    test("2. SMALLINT (INT2) stores small integer values", async () => {
      const res = await db.query(`SELECT reorder_level FROM products WHERE product_id = 1;`);
      expect(Number(res[0].reorder_level)).toBe(5);
    });

    test("3. BIGINT (INT8) stores large 64-bit integer values", async () => {
      const res = await db.query(`SELECT total_revenue FROM products WHERE product_id = 1;`);
      expect(Number(res[0].total_revenue)).toBe(58499);
    });

    test("4. DECIMAL(10, 2) stores exact currency with two decimals", async () => {
      const res = await db.query(`SELECT price FROM products WHERE product_id = 1;`);
      expect(Number(res[0].price)).toBeCloseTo(1299.99, 2);
    });

    test("5. NUMERIC(10, 4) stores high precision decimal numbers", async () => {
      const res = await db.query(`SELECT cost FROM products WHERE product_id = 1;`);
      expect(Number(res[0].cost)).toBeCloseTo(850.5000, 4);
    });

    test("6. REAL (FLOAT4) stores single precision float values", async () => {
      const res = await db.query(`SELECT weight_kg FROM products WHERE product_id = 1;`);
      expect(Number(res[0].weight_kg)).toBeCloseTo(1.85, 2);
    });

    test("7. DOUBLE PRECISION (FLOAT8) stores high precision floats", async () => {
      const res = await db.query(`SELECT rating FROM products WHERE product_id = 1;`);
      expect(Number(res[0].rating)).toBeCloseTo(4.8, 1);
    });

    test("8. SERIAL auto-increment primary key allocates unique sequential IDs", async () => {
      const res = await db.query(`SELECT product_id FROM products ORDER BY product_id ASC;`);
      expect(res.length).toBe(8);
      expect(Number(res[0].product_id)).toBe(1);
      expect(Number(res[7].product_id)).toBe(8);
    });

    test("9. BIGSERIAL auto-increment primary key on financial_ledger", async () => {
      const res = await db.query(`SELECT entry_id FROM financial_ledger ORDER BY entry_id ASC;`);
      expect(res.length).toBe(5);
      expect(Number(res[0].entry_id)).toBe(1);
    });

    test("10. Zero and negative numbers stored and retrieved accurately", async () => {
      const res = await db.query(`SELECT amount FROM financial_ledger WHERE amount < 0;`);
      expect(res.length).toBe(2);
      expect(Number(res[0].amount)).toBeLessThan(0);
    });

    test("11. NULL value in numeric column retrieved as null", async () => {
      const res = await db.query(`SELECT val_null FROM math_samples WHERE id = 1;`);
      expect(res[0].val_null).toBeNull();
    });

    test("12. Decimal literal representation 0.0001", async () => {
      const res = await db.query(`SELECT 0.0001 AS tiny_num;`);
      expect(Number(res[0].tiny_num)).toBeCloseTo(0.0001, 4);
    });

    test("13. Integer array INT[] stores series of numbers", async () => {
      await db.query(`
        CREATE TABLE num_arrays (id INT PRIMARY KEY, vals INT[]);
        INSERT INTO num_arrays (id, vals) VALUES (1, '{10, 20, 30, 40, 50}');
      `);
      const res = await db.query(`SELECT vals FROM num_arrays WHERE id = 1;`);
      expect(res[0].vals).toEqual([10, 20, 30, 40, 50]);
    });

    test("14. Float array FLOAT[] stores fractional numbers", async () => {
      await db.query(`
        CREATE TABLE float_arrays (id INT PRIMARY KEY, rates FLOAT[]);
        INSERT INTO float_arrays (id, rates) VALUES (1, '{1.1, 2.2, 3.3}');
      `);
      const res = await db.query(`SELECT rates FROM float_arrays WHERE id = 1;`);
      expect(res[0].rates).toEqual([1.1, 2.2, 3.3]);
    });

    test("15. Schema column type introspection for numeric types", async () => {
      const res = await db.query(`
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_name = 'products' AND column_name IN ('price', 'cost', 'stock_qty');
      `);
      expect(res.length).toBe(3);
    });
  });

  // =========================================================================
  // Section 2: Arithmetic Operators (+, -, *, /, %, ^) & Precedence (Tests 16 - 30)
  // =========================================================================
  describe("Section 2: Arithmetic Operators (+, -, *, /, %, ^) & Precedence", () => {
    test("16. Addition operator + adds integers and floats", async () => {
      const res = await db.query(`SELECT 100 + 250 AS total, 12.5 + 7.5 AS float_total;`);
      expect(Number(res[0].total)).toBe(350);
      expect(Number(res[0].float_total)).toBe(20);
    });

    test("17. Subtraction operator - subtracts numbers correctly", async () => {
      const res = await db.query(`SELECT 1000 - 350 AS diff, 50.75 - 10.25 AS float_diff;`);
      expect(Number(res[0].diff)).toBe(650);
      expect(Number(res[0].float_diff)).toBe(40.5);
    });

    test("18. Multiplication operator * computes product", async () => {
      const res = await db.query(`SELECT price * stock_qty AS total_stock_value FROM products WHERE product_id = 1;`);
      expect(Number(res[0].total_stock_value)).toBeCloseTo(1299.99 * 45, 2);
    });

    test("19. Division operator / computes quotient", async () => {
      const res = await db.query(`SELECT 100 / 4 AS quotient, 10 / 4.0 AS float_quotient;`);
      expect(Number(res[0].quotient)).toBe(25);
      expect(Number(res[0].float_quotient)).toBe(2.5);
    });

    test("20. Modulo operator % computes remainder", async () => {
      const res = await db.query(`SELECT 17 % 5 AS remainder, 100 % 10 AS zero_rem;`);
      expect(Number(res[0].remainder)).toBe(2);
      expect(Number(res[0].zero_rem)).toBe(0);
    });

    test("21. Exponentiation operator ^ computes power", async () => {
      const res = await db.query(`SELECT 2 ^ 8 AS byte_states, 3 ^ 3 AS cube;`);
      expect(Number(res[0].byte_states)).toBe(256);
      expect(Number(res[0].cube)).toBe(27);
    });

    test("22. Arithmetic operator precedence (* before +)", async () => {
      const res = await db.query(`SELECT 2 + 3 * 4 AS result;`);
      expect(Number(res[0].result)).toBe(14);
    });

    test("23. Parenthesized arithmetic overrides default precedence", async () => {
      const res = await db.query(`SELECT (2 + 3) * 4 AS result;`);
      expect(Number(res[0].result)).toBe(20);
    });

    test("24. Unary minus - in arithmetic expression", async () => {
      const res = await db.query(`SELECT -5 + 20 AS result, -val_neg AS pos_val FROM math_samples WHERE id = 1;`);
      expect(Number(res[0].result)).toBe(15);
      expect(Number(res[0].pos_val)).toBe(25);
    });

    test("25. Margin calculation (price - cost) in SELECT projection", async () => {
      const res = await db.query(`
        SELECT name, price - cost AS profit_margin
        FROM products WHERE product_id = 1;
      `);
      expect(Number(res[0].profit_margin)).toBeCloseTo(1299.99 - 850.5, 2);
    });

    test("26. Margin percentage ((price - cost) / price * 100)", async () => {
      const res = await db.query(`
        SELECT name, ROUND(((price - cost) / price) * 100, 2) AS margin_pct
        FROM products WHERE product_id = 1;
      `);
      const expected = ((1299.99 - 850.5) / 1299.99) * 100;
      expect(Number(res[0].margin_pct)).toBeCloseTo(expected, 1);
    });

    test("27. Arithmetic expression in WHERE filter clause", async () => {
      const res = await db.query(`
        SELECT name FROM products
        WHERE price * stock_qty > 10000
        ORDER BY product_id ASC;
      `);
      expect(res.length).toBeGreaterThanOrEqual(1);
      expect(res[0].name).toBe("Laptop Pro");
    });

    test("28. Arithmetic expression in UPDATE statement", async () => {
      await db.query(`UPDATE products SET stock_qty = stock_qty + 10 WHERE product_id = 8;`);
      const res = await db.query(`SELECT stock_qty FROM products WHERE product_id = 8;`);
      expect(Number(res[0].stock_qty)).toBe(10);
    });

    test("29. Arithmetic with NULL operand returns NULL", async () => {
      const res = await db.query(`SELECT 100 + val_null AS null_sum FROM math_samples WHERE id = 1;`);
      expect(res[0].null_sum).toBeNull();
    });

    test("30. Complex nested arithmetic formula", async () => {
      const res = await db.query(`SELECT ((10 + 20) * 2 - 10) / 5 AS calc;`);
      expect(Number(res[0].calc)).toBe(10);
    });
  });

  // =========================================================================
  // Section 3: Mathematical Built-in Functions (Tests 31 - 50)
  // =========================================================================
  describe("Section 3: Mathematical Built-in Functions", () => {
    test("31. ABS() returns absolute value of negative numbers", async () => {
      const res = await db.query(`SELECT ABS(-42.5) AS pos, ABS(100) AS same, ABS(0) AS zero;`);
      expect(Number(res[0].pos)).toBe(42.5);
      expect(Number(res[0].same)).toBe(100);
      expect(Number(res[0].zero)).toBe(0);
    });

    test("32. CEIL() / CEILING() rounds up to nearest integer", async () => {
      const res = await db.query(`SELECT CEIL(4.01) AS c1, CEILING(-4.8) AS c2;`);
      expect(Number(res[0].c1)).toBe(5);
      expect(Number(res[0].c2)).toBe(-4);
    });

    test("33. FLOOR() rounds down to nearest integer", async () => {
      const res = await db.query(`SELECT FLOOR(4.99) AS f1, FLOOR(-4.1) AS f2;`);
      expect(Number(res[0].f1)).toBe(4);
      expect(Number(res[0].f2)).toBe(-5);
    });

    test("34. ROUND() with 0 decimals rounds to nearest integer", async () => {
      const res = await db.query(`SELECT ROUND(4.4) AS r1, ROUND(4.6) AS r2;`);
      expect(Number(res[0].r1)).toBe(4);
      expect(Number(res[0].r2)).toBe(5);
    });

    test("35. ROUND() with decimal places parameter", async () => {
      const res = await db.query(`SELECT ROUND(3.14159265, 2) AS pi_2, ROUND(3.14159265, 4) AS pi_4;`);
      expect(Number(res[0].pi_2)).toBe(3.14);
      expect(Number(res[0].pi_4)).toBe(3.1416);
    });

    test("36. TRUNC() truncates digits towards zero", async () => {
      const res = await db.query(`SELECT TRUNC(4.99) AS t1, TRUNC(-4.99) AS t2;`);
      expect(Number(res[0].t1)).toBe(4);
      expect(Number(res[0].t2)).toBe(-4);
    });

    test("37. TRUNC() with decimal places parameter", async () => {
      const res = await db.query(`SELECT TRUNC(3.14159265, 2) AS t_pi;`);
      expect(Number(res[0].t_pi)).toBe(3.14);
    });

    test("38. POWER() / POW() computes base raised to exponent", async () => {
      const res = await db.query(`SELECT POWER(2, 10) AS p1, POW(5, 3) AS p2;`);
      expect(Number(res[0].p1)).toBe(1024);
      expect(Number(res[0].p2)).toBe(125);
    });

    test("39. SQRT() computes square root", async () => {
      const res = await db.query(`SELECT SQRT(64) AS s1, SQRT(100) AS s2, SQRT(2) AS s3;`);
      expect(Number(res[0].s1)).toBe(8);
      expect(Number(res[0].s2)).toBe(10);
      expect(Number(res[0].s3)).toBeCloseTo(1.4142, 3);
    });

    test("40. EXP() natural exponential e^x", async () => {
      const res = await db.query(`SELECT EXP(1) AS e_val, EXP(0) AS one;`);
      expect(Number(res[0].e_val)).toBeCloseTo(2.71828, 4);
      expect(Number(res[0].one)).toBe(1);
    });

    test("41. LN() natural logarithm", async () => {
      const res = await db.query(`SELECT LN(2.718281828459045) AS ln_e, LN(1) AS zero;`);
      expect(Number(res[0].ln_e)).toBeCloseTo(1, 4);
      expect(Number(res[0].zero)).toBe(0);
    });

    test("42. LOG() base-10 logarithm", async () => {
      const res = await db.query(`SELECT LOG(100) AS log_100, LOG(1000) AS log_1000;`);
      expect(Number(res[0].log_100)).toBe(2);
      expect(Number(res[0].log_1000)).toBe(3);
    });

    test("43. MOD() function computes modulo", async () => {
      const res = await db.query(`SELECT MOD(29, 6) AS m1;`);
      expect(Number(res[0].m1)).toBe(5);
    });

    test("44. SIGN() returns -1 for negative, 0 for zero, 1 for positive", async () => {
      const res = await db.query(`SELECT SIGN(-15) AS s_neg, SIGN(0) AS s_zero, SIGN(88) AS s_pos;`);
      expect(Number(res[0].s_neg)).toBe(-1);
      expect(Number(res[0].s_zero)).toBe(0);
      expect(Number(res[0].s_pos)).toBe(1);
    });

    test("45. PI() returns mathematical constant pi", async () => {
      const res = await db.query(`SELECT PI() AS pi_const;`);
      expect(Number(res[0].pi_const)).toBeCloseTo(Math.PI, 6);
    });

    test("46. DEGREES() converts radians to degrees", async () => {
      const res = await db.query(`SELECT ROUND(DEGREES(PI()), 2) AS deg;`);
      expect(Number(res[0].deg)).toBe(180);
    });

    test("47. RADIANS() converts degrees to radians", async () => {
      const res = await db.query(`SELECT ROUND(RADIANS(180), 4) AS rad;`);
      expect(Number(res[0].rad)).toBeCloseTo(Math.PI, 4);
    });

    test("48. RANDOM() produces floating number between 0 and 1", async () => {
      const res = await db.query(`SELECT RANDOM() AS r;`);
      expect(Number(res[0].r)).toBeGreaterThanOrEqual(0);
      expect(Number(res[0].r)).toBeLessThan(1);
    });

    test("49. Nested math functions: ROUND(SQRT(ABS(val_neg)), 2)", async () => {
      const res = await db.query(`SELECT ROUND(SQRT(ABS(val_neg)), 2) AS root FROM math_samples WHERE id = 1;`);
      expect(Number(res[0].root)).toBe(5);
    });

    test("50. Math function with NULL input yields NULL", async () => {
      const res = await db.query(`SELECT SQRT(val_null) AS sq, ABS(val_null) AS ab FROM math_samples WHERE id = 1;`);
      expect(res[0].sq).toBeNull();
      expect(res[0].ab).toBeNull();
    });
  });

  // =========================================================================
  // Section 4: Bitwise Operators (&, |, #, <<, >>) (Tests 51 - 65)
  // =========================================================================
  describe("Section 4: Bitwise Operators (&, |, #, <<, >>)", () => {
    test("51. Bitwise AND operator & on integer values", async () => {
      const res = await db.query(`SELECT 5 & 3 AS bit_and;`);
      expect(Number(res[0].bit_and)).toBe(1); // 0101 & 0011 = 0001
    });

    test("52. Bitwise OR operator | on integer values", async () => {
      const res = await db.query(`SELECT 5 | 3 AS bit_or;`);
      expect(Number(res[0].bit_or)).toBe(7); // 0101 | 0011 = 0111
    });

    test("53. Bitwise XOR operator # on integer values", async () => {
      const res = await db.query(`SELECT 5 # 3 AS bit_xor;`);
      expect(Number(res[0].bit_xor)).toBe(6); // 0101 ^ 0011 = 0110
    });

    test("54. Bitwise Shift Left operator <<", async () => {
      const res = await db.query(`SELECT 1 << 4 AS shift_left, 3 << 2 AS shift_3;`);
      expect(Number(res[0].shift_left)).toBe(16);
      expect(Number(res[0].shift_3)).toBe(12);
    });

    test("55. Bitwise Shift Right operator >>", async () => {
      const res = await db.query(`SELECT 16 >> 2 AS shift_right, 64 >> 3 AS shift_64;`);
      expect(Number(res[0].shift_right)).toBe(4);
      expect(Number(res[0].shift_64)).toBe(8);
    });

    test("56. Bitmask filtering in WHERE clause (flags & 4 = 4)", async () => {
      const res = await db.query(`
        SELECT name, flags FROM products
        WHERE flags & 4 = 4
        ORDER BY product_id ASC;
      `);
      expect(res.length).toBeGreaterThanOrEqual(1);
      const names = res.map(r => r.name);
      expect(names).toContain("Laptop Pro"); // flags = 5 (1 | 4)
      expect(names).toContain("Monitor 4K"); // flags = 7 (1 | 2 | 4)
    });

    test("57. Bitmask filtering (flags & 1 = 1) for feature flag", async () => {
      const res = await db.query(`
        SELECT name FROM products
        WHERE flags & 1 = 1
        ORDER BY product_id ASC;
      `);
      const names = res.map(r => r.name);
      expect(names).toContain("Laptop Pro");
      expect(names).toContain("Wireless Mouse");
      expect(names).toContain("Mechanical Keyboard");
    });

    test("58. Setting bit flags via UPDATE (flags = flags | 8)", async () => {
      await db.query(`UPDATE products SET flags = flags | 8 WHERE product_id = 1;`);
      const res = await db.query(`SELECT flags FROM products WHERE product_id = 1;`);
      expect(Number(res[0].flags)).toBe(13); // 5 | 8 = 13
    });

    test("59. Clearing bit flags via UPDATE (flags = flags & 7)", async () => {
      await db.query(`UPDATE products SET flags = flags & 7 WHERE product_id = 1;`);
      const res = await db.query(`SELECT flags FROM products WHERE product_id = 1;`);
      expect(Number(res[0].flags)).toBe(5); // 13 & 7 = 5
    });

    test("60. Bitwise XOR toggling bit flags", async () => {
      const res = await db.query(`SELECT flags # 1 AS toggled FROM products WHERE product_id = 2;`);
      expect(Number(res[0].toggled)).toBe(0); // 1 ^ 1 = 0
    });

    test("61. Bitwise expression combined with CASE WHEN", async () => {
      const res = await db.query(`
        SELECT name,
          CASE
            WHEN flags & 4 = 4 THEN 'Premium'
            WHEN flags & 2 = 2 THEN 'Standard'
            ELSE 'Basic'
          END AS tier
        FROM products WHERE product_id IN (1, 4, 6)
        ORDER BY product_id ASC;
      `);
      expect(res[0].tier).toBe("Premium");
      expect(res[1].tier).toBe("Standard");
      expect(res[2].tier).toBe("Basic");
    });

    test("62. Parameterized bitwise query with $1 bitmask parameter", async () => {
      const res = await db.query(`SELECT name FROM products WHERE flags & $1 = $1;`, [4]);
      expect(res.length).toBeGreaterThanOrEqual(2);
    });

    test("63. Multiple chained bitwise operations", async () => {
      const res = await db.query(`SELECT (15 & 7) | 16 AS complex_bit;`);
      expect(Number(res[0].complex_bit)).toBe(23); // (7) | 16 = 23
    });

    test("64. Bitwise operation yielding 0 result", async () => {
      const res = await db.query(`SELECT 8 & 4 AS zero_bit;`);
      expect(Number(res[0].zero_bit)).toBe(0);
    });

    test("65. Bitwise operation with NULL operand yields NULL", async () => {
      const res = await db.query(`SELECT val_null & 4 AS null_bit FROM math_samples WHERE id = 1;`);
      expect(res[0].null_bit).toBeNull();
    });
  });

  // =========================================================================
  // Section 5: Aggregations & Statistical Calculations (Tests 66 - 80)
  // =========================================================================
  describe("Section 5: Aggregations & Statistical Calculations", () => {
    test("66. SUM() calculates total sum of numeric column", async () => {
      const res = await db.query(`SELECT SUM(stock_qty) AS total_units FROM products;`);
      expect(Number(res[0].total_units)).toBeGreaterThan(0);
    });

    test("67. SUM() on decimal currency column calculates total revenue", async () => {
      const res = await db.query(`SELECT SUM(price) AS catalog_value FROM products;`);
      expect(Number(res[0].catalog_value)).toBeGreaterThan(1000);
    });

    test("68. AVG() calculates arithmetic average", async () => {
      const res = await db.query(`SELECT AVG(rating) AS avg_rating FROM products;`);
      expect(Number(res[0].avg_rating)).toBeGreaterThan(4.0);
    });

    test("69. MIN() finds minimum numeric value", async () => {
      const res = await db.query(`SELECT MIN(price) AS lowest_price FROM products;`);
      expect(Number(res[0].lowest_price)).toBe(9.99);
    });

    test("70. MAX() finds maximum numeric value", async () => {
      const res = await db.query(`SELECT MAX(price) AS highest_price FROM products;`);
      expect(Number(res[0].highest_price)).toBe(1299.99);
    });

    test("71. COUNT(*) vs COUNT(column_name) on non-null rows", async () => {
      const res = await db.query(`SELECT COUNT(*) AS total_rows, COUNT(val_null) AS non_null_count FROM math_samples;`);
      expect(Number(res[0].total_rows)).toBe(3);
      expect(Number(res[0].non_null_count)).toBe(0);
    });

    test("72. Aggregation with arithmetic expression: SUM(price * stock_qty)", async () => {
      const res = await db.query(`SELECT ROUND(SUM(price * stock_qty), 2) AS total_inventory_valuation FROM products;`);
      expect(Number(res[0].total_inventory_valuation)).toBeGreaterThan(50000);
    });

    test("73. GROUP BY category with SUM() and AVG() aggregations", async () => {
      const res = await db.query(`
        SELECT category, COUNT(*) AS item_count, SUM(stock_qty) AS total_qty, ROUND(AVG(price), 2) AS avg_price
        FROM products
        GROUP BY category
        ORDER BY category ASC;
      `);
      expect(res.length).toBe(3);
      const electronics = res.find(r => r.category === "electronics");
      expect(Number(electronics.item_count)).toBe(4);
    });

    test("74. HAVING clause filtering groups by aggregated SUM()", async () => {
      const res = await db.query(`
        SELECT category, SUM(stock_qty) AS total_qty
        FROM products
        GROUP BY category
        HAVING SUM(stock_qty) > 100;
      `);
      expect(res.length).toBeGreaterThanOrEqual(1);
    });

    test("75. HAVING clause filtering groups by aggregated AVG()", async () => {
      const res = await db.query(`
        SELECT category, AVG(rating) AS avg_rat
        FROM products
        GROUP BY category
        HAVING AVG(rating) > 4.5;
      `);
      expect(res.length).toBeGreaterThanOrEqual(1);
    });

    test("76. Aggregating over empty dataset returns NULL for SUM and 0 for COUNT", async () => {
      const res = await db.query(`SELECT SUM(price) AS sum_empty, COUNT(*) AS count_empty FROM products WHERE product_id = 9999;`);
      expect(res[0].sum_empty).toBeNull();
      expect(Number(res[0].count_empty)).toBe(0);
    });

    test("77. Multiple statistical aggregates in single projection", async () => {
      const res = await db.query(`
        SELECT MIN(price) AS min_p, MAX(price) AS max_p, AVG(price) AS avg_p, SUM(price) AS sum_p, COUNT(price) AS count_p
        FROM products;
      `);
      expect(Number(res[0].min_p)).toBe(9.99);
      expect(Number(res[0].max_p)).toBe(1299.99);
      expect(Number(res[0].count_p)).toBe(8);
    });

    test("78. Nested function with aggregate: ROUND(AVG(price), 2)", async () => {
      const res = await db.query(`SELECT ROUND(AVG(price), 2) AS rounded_avg FROM products;`);
      expect(typeof res[0].rounded_avg).toBe("number");
    });

    test("79. Aggregate with filtered WHERE condition", async () => {
      const res = await db.query(`SELECT SUM(total_revenue) AS elec_rev FROM products WHERE category = 'electronics';`);
      expect(Number(res[0].elec_rev)).toBe(58499 + 4492 + 7160 + 9999);
    });

    test("80. Financial balance computation: SUM(amount)", async () => {
      const res = await db.query(`SELECT SUM(amount) AS net_cashflow FROM financial_ledger WHERE account_code = 1001;`);
      expect(Number(res[0].net_cashflow)).toBeCloseTo(5000.00 + 1250.50 - 750.25, 2);
    });
  });

  // =========================================================================
  // Section 6: Numerical Comparisons, Range Filters & Window Functions (Tests 81 - 90)
  // =========================================================================
  describe("Section 6: Numerical Comparisons, Range Filters & Window Functions", () => {
    test("81. Exact equality comparison on integer (product_id = 1)", async () => {
      const res = await db.query(`SELECT name FROM products WHERE product_id = 1;`);
      expect(res.length).toBe(1);
      expect(res[0].name).toBe("Laptop Pro");
    });

    test("82. Numerical inequality operators != and <>", async () => {
      const res = await db.query(`SELECT count(*) AS cnt FROM products WHERE stock_qty != 0;`);
      expect(Number(res[0].cnt)).toBe(8);
    });

    test("83. Greater than and Less than comparisons (price > 100 AND price < 500)", async () => {
      const res = await db.query(`
        SELECT name, price FROM products
        WHERE price > 100 AND price < 500
        ORDER BY price ASC;
      `);
      expect(res.length).toBe(3); // Ergonomic Chair (299), Monitor 4K (399.99), Standing Desk (450)
    });

    test("84. BETWEEN ... AND numerical range filter", async () => {
      const res = await db.query(`
        SELECT name, price FROM products
        WHERE price BETWEEN 29.95 AND 89.50
        ORDER BY price ASC;
      `);
      expect(res.length).toBe(3); // Desk Lamp (34.50), Wireless Mouse (29.95), Mechanical Keyboard (89.50)
    });

    test("85. NOT BETWEEN numerical exclusion filter", async () => {
      const res = await db.query(`
        SELECT name FROM products
        WHERE price NOT BETWEEN 10.00 AND 1000.00
        ORDER BY product_id ASC;
      `);
      expect(res.length).toBe(2); // USB-C Cable (9.99), Laptop Pro (1299.99)
    });

    test("86. IN (num1, num2, num3) numerical list filter", async () => {
      const res = await db.query(`SELECT name FROM products WHERE product_id IN (1, 3, 5) ORDER BY product_id ASC;`);
      expect(res.length).toBe(3);
      expect(res.map(r => r.name)).toEqual(["Laptop Pro", "Mechanical Keyboard", "Ergonomic Chair"]);
    });

    test("87. NOT IN numerical exclusion list", async () => {
      const res = await db.query(`SELECT count(*) AS cnt FROM products WHERE product_id NOT IN (1, 2, 3);`);
      expect(Number(res[0].cnt)).toBe(5);
    });

    test("88. Window function ROW_NUMBER() OVER (ORDER BY price DESC)", async () => {
      const res = await db.query(`
        SELECT name, price, ROW_NUMBER() OVER (ORDER BY price DESC) AS row_num
        FROM products;
      `);
      expect(Number(res[0].row_num)).toBe(1);
      expect(res[0].name).toBe("Laptop Pro");
    });

    test("89. Window function RANK() OVER (ORDER BY rating DESC)", async () => {
      const res = await db.query(`
        SELECT name, rating, RANK() OVER (ORDER BY rating DESC) AS rank_num
        FROM products;
      `);
      expect(Number(res[0].rank_num)).toBe(1);
      expect(res[0].name).toBe("Monitor 4K");
    });

    test("90. Window function SUM(amount) OVER (PARTITION BY account_code ORDER BY entry_id ASC)", async () => {
      const res = await db.query(`
        SELECT entry_id, account_code, amount,
               SUM(amount) OVER (PARTITION BY account_code ORDER BY entry_id ASC) AS running_balance
        FROM financial_ledger
        WHERE account_code = 1001;
      `);
      expect(res.length).toBe(3);
      expect(Number(res[0].running_balance)).toBeCloseTo(5000.00, 2);
      expect(Number(res[1].running_balance)).toBeCloseTo(6250.50, 2);
      expect(Number(res[2].running_balance)).toBeCloseTo(5500.25, 2);
    });
  });

  // =========================================================================
  // Section 7: Casting, Formatting, DML Mutations & Edge Cases (Tests 91 - 100)
  // =========================================================================
  describe("Section 7: Casting, Formatting, DML Mutations & Edge Cases", () => {
    test("91. Cast string literal to INT '123'::int", async () => {
      const res = await db.query(`SELECT '123'::int AS num_val;`);
      expect(Number(res[0].num_val)).toBe(123);
    });

    test("92. Cast string literal to NUMERIC '456.78'::numeric", async () => {
      const res = await db.query(`SELECT '456.78'::numeric AS num_val;`);
      expect(Number(res[0].num_val)).toBeCloseTo(456.78, 2);
    });

    test("93. Explicit CAST('789.10' AS DECIMAL)", async () => {
      const res = await db.query(`SELECT CAST('789.10' AS DECIMAL) AS dec_val;`);
      expect(Number(res[0].dec_val)).toBeCloseTo(789.10, 2);
    });

    test("94. Float to Integer truncation cast: 45.89::int", async () => {
      const res = await db.query(`SELECT 45.89::int AS casted_int;`);
      expect(Math.floor(Number(res[0].casted_int))).toBe(45);
    });

    test("95. Parameterized query passing numerical parameters $1, $2", async () => {
      const res = await db.query(`
        SELECT name, price FROM products
        WHERE price >= $1 AND stock_qty <= $2
        ORDER BY price ASC;
      `, [200.00, 50]);
      expect(res.length).toBeGreaterThanOrEqual(2);
    });

    test("96. COALESCE with numeric fallback value", async () => {
      const res = await db.query(`SELECT COALESCE(val_null, 0) AS safe_val FROM math_samples WHERE id = 1;`);
      expect(Number(res[0].safe_val)).toBe(0);
    });

    test("97. Subquery calculating percentage of total revenue", async () => {
      const res = await db.query(`
        SELECT name, ROUND((total_revenue::numeric / (SELECT SUM(total_revenue) FROM products)) * 100, 2) AS revenue_share_pct
        FROM products WHERE product_id = 1;
      `);
      expect(Number(res[0].revenue_share_pct)).toBeGreaterThan(50);
    });

    test("98. INSERT new product row with numeric values and RETURNING clause", async () => {
      const res = await db.query(`
        INSERT INTO products (product_id, name, category, price, cost, stock_qty)
        VALUES (101, 'Webcam HD', 'accessories', 79.99, 35.00, 60)
        RETURNING product_id, name, price;
      `);
      expect(res.length).toBe(1);
      expect(Number(res[0].product_id)).toBe(101);
      expect(Number(res[0].price)).toBe(79.99);
    });

    test("99. UPDATE multiplying stock_qty by 2", async () => {
      await db.query(`UPDATE products SET stock_qty = stock_qty * 2 WHERE product_id = 101;`);
      const res = await db.query(`SELECT stock_qty FROM products WHERE product_id = 101;`);
      expect(Number(res[0].stock_qty)).toBe(120);
    });

    test("100. DELETE based on numeric threshold condition", async () => {
      await db.query(`DELETE FROM products WHERE product_id = 101;`);
      const res = await db.query(`SELECT count(*) AS cnt FROM products WHERE product_id = 101;`);
      expect(Number(res[0].cnt)).toBe(0);
    });
  });
});
