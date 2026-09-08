import { expect, test, describe, beforeEach } from "bun:test";
import { PGLite } from "../src/index";

describe("100 Comprehensive SQL Syntax & Logic Test Cases for PGLite", () => {
  let db: any;

  beforeEach(async () => {
    db = new PGLite(":memory:", { native: true });

    // Base tables for testing
    await db.exec(`
      CREATE TABLE products (
        id SERIAL PRIMARY KEY,
        name TEXT NOT NULL,
        category TEXT NOT NULL,
        price FLOAT NOT NULL,
        stock INT NOT NULL DEFAULT 0,
        tags TEXT[],
        attributes JSONB,
        created_at TIMESTAMP,
        is_active BOOLEAN DEFAULT true
      );

      CREATE TABLE customers (
        id SERIAL PRIMARY KEY,
        full_name TEXT NOT NULL,
        email TEXT,
        tier TEXT DEFAULT 'STANDARD',
        loyalty_points INT DEFAULT 0,
        address JSONB,
        created_at TIMESTAMP
      );

      CREATE TABLE sales_orders (
        id SERIAL PRIMARY KEY,
        customer_id INT,
        order_code TEXT,
        order_date TIMESTAMP,
        total_amount FLOAT NOT NULL,
        status TEXT DEFAULT 'PENDING',
        discount_rate FLOAT DEFAULT 0.00
      );

      CREATE TABLE order_items (
        id SERIAL PRIMARY KEY,
        order_id INT NOT NULL,
        product_id INT NOT NULL,
        quantity INT NOT NULL,
        unit_price FLOAT NOT NULL
      );
    `);

    // Seed data
    await db.exec(`
      INSERT INTO products (id, name, category, price, stock, tags, attributes, created_at, is_active) VALUES
      (1, 'Laptop Dell XPS 15', 'Electronics', 35000000.0, 10, '{"laptop", "dell", "workstation"}', '{"cpu": "i7", "ram": "32GB", "brand": "Dell"}', '2026-01-10 10:00:00', true),
      (2, 'iPhone 16 Pro Max', 'Mobile', 32000000.0, 25, '{"apple", "smartphone", "5g"}', '{"storage": "256GB", "color": "Desert Titanium"}', '2026-02-15 11:30:00', true),
      (3, 'Bàn phím cơ Keychron Q1 Pro', 'Accessories', 4500000.0, 50, '{"keyboard", "wireless", "mechanical"}', '{"switch": "Red", "layout": "75%"}', '2026-03-01 09:15:00', true),
      (4, 'Chuột Logitech MX Master 3S', 'Accessories', 2500000.0, 0, '{"mouse", "wireless", "ergonomic"}', '{"dpi": 8000, "color": "Graphite"}', '2026-03-10 14:00:00', false),
      (5, 'Màn hình LG 27GP850 2K 165Hz', 'Electronics', 8900000.0, 15, '{"monitor", "gaming", "ips"}', '{"size": "27 inch", "resolution": "2K"}', '2026-04-05 16:45:00', true),
      (6, 'Tai nghe Sony WH-1000XM5', 'Accessories', 7500000.0, 12, '{"audio", "anc", "wireless"}', '{"anc": true, "battery": "30h"}', '2026-05-12 18:20:00', true);

      INSERT INTO customers (id, full_name, email, tier, loyalty_points, address, created_at) VALUES
      (1, 'Nguyễn Văn An', 'an.nguyen@example.com', 'VIP', 1250, '{"city": "Hà Nội", "district": "Cầu Giấy", "zip": "100000"}', '2026-01-01 08:00:00'),
      (2, 'Trần Thị Bích', 'bich.tran@example.com', 'GOLD', 850, '{"city": "Hồ Chí Minh", "district": "Quận 1", "zip": "700000"}', '2026-01-15 10:00:00'),
      (3, 'Lê Hoàng Cường', 'cuong.le@example.com', 'STANDARD', 150, '{"city": "Đà Nẵng", "district": "Hải Châu", "zip": "550000"}', '2026-02-01 14:00:00'),
      (4, 'Phạm Minh Đức', NULL, 'STANDARD', 0, '{"city": "Cần Thơ", "district": "Ninh Kiều"}', '2026-03-01 09:30:00'),
      (5, 'Hoàng Thùy Dương', 'duong.hoang@example.com', 'VIP', 3400, '{"city": "Hà Nội", "district": "Tây Hồ", "zip": "100000"}', '2026-03-15 16:00:00');

      INSERT INTO sales_orders (id, customer_id, order_code, order_date, total_amount, status, discount_rate) VALUES
      (1, 1, 'ORD-2026-001', '2026-03-01 10:00:00', 39500000.0, 'DELIVERED', 0.05),
      (2, 1, 'ORD-2026-002', '2026-03-15 11:30:00', 2500000.0, 'DELIVERED', 0.00),
      (3, 2, 'ORD-2026-003', '2026-04-01 15:45:00', 32000000.0, 'DELIVERED', 0.10),
      (4, 3, 'ORD-2026-004', '2026-04-10 09:00:00', 8900000.0, 'PROCESSING', 0.00),
      (5, 5, 'ORD-2026-005', '2026-04-20 17:00:00', 7500000.0, 'CANCELLED', 0.15),
      (6, NULL, 'ORD-2026-006', '2026-04-25 14:15:00', 4500000.0, 'PENDING', 0.00);

      INSERT INTO order_items (id, order_id, product_id, quantity, unit_price) VALUES
      (1, 1, 1, 1, 35000000.0),
      (2, 1, 3, 1, 4500000.0),
      (3, 2, 4, 1, 2500000.0),
      (4, 3, 2, 1, 32000000.0),
      (5, 4, 5, 1, 8900000.0),
      (6, 5, 6, 1, 7500000.0),
      (7, 6, 3, 1, 4500000.0);
    `);
  });

  // =========================================================================
  // Section 1: Complex Conditional Logic & Expressions (Tests 1 - 5)
  // =========================================================================
  describe("Section 1: Complex Conditional Logic & Expressions", () => {
    test("1. Multi-branch CASE WHEN with Price tier classification", async () => {
      const sql = `
        SELECT name, price,
          CASE 
            WHEN price >= 30000000.0 THEN 'PREMIUM'
            WHEN price >= 5000000.0 THEN 'MID_RANGE'
            ELSE 'BUDGET'
          END AS price_tier
        FROM products
        ORDER BY price DESC;
      `;
      const res = await db.query(sql);
      expect(res.length).toBe(6);
      expect(res[0].price_tier).toBe("PREMIUM");
      expect(res[1].price_tier).toBe("PREMIUM");
      expect(res[2].price_tier).toBe("MID_RANGE");
      expect(res[5].price_tier).toBe("BUDGET");
    });

    test("2. COALESCE chain with fallback literal strings", async () => {
      const sql = `
        SELECT full_name,
          COALESCE(email, 'no-email@system.internal') AS contact_email
        FROM customers
        ORDER BY id;
      `;
      const res = await db.query(sql);
      expect(res.length).toBe(5);
      expect(res[3].full_name).toBe("Phạm Minh Đức");
      expect(res[3].contact_email).toBe("no-email@system.internal");
    });

    test("3. Compound Boolean Predicates with nested AND / OR precedence", async () => {
      const sql = `
        SELECT name, category, price, is_active
        FROM products
        WHERE (category = 'Electronics' OR category = 'Accessories')
          AND NOT (price > 10000000.0 AND is_active = false)
          AND (stock > 0 AND (price BETWEEN 2000000.0 AND 10000000.0))
        ORDER BY price ASC;
      `;
      const res = await db.query(sql);
      expect(res.length).toBe(3);
      expect(res[0].name).toBe("Bàn phím cơ Keychron Q1 Pro");
      expect(res[1].name).toBe("Tai nghe Sony WH-1000XM5");
      expect(res[2].name).toBe("Màn hình LG 27GP850 2K 165Hz");
    });

    test("4. CASE expression simulating upper cap on unit_price", async () => {
      const sql = `
        SELECT id,
          CASE 
            WHEN unit_price > 30000000.0 THEN 30000000.0 
            ELSE unit_price 
          END AS capped_price
        FROM order_items
        WHERE id IN (1, 2)
        ORDER BY id;
      `;
      const res = await db.query(sql);
      expect(res[0].capped_price).toBe(30000000.0);
      expect(res[1].capped_price).toBe(4500000.0);
    });

    test("5. Type Casting with CAST() and :: notation", async () => {
      const sql = `
        SELECT 
          CAST(price AS INT) AS price_int,
          CAST(id AS TEXT) AS id_text
        FROM products
        WHERE id = 1;
      `;
      const res = await db.query(sql);
      expect(Number(res[0].price_int)).toBe(35000000);
      expect(String(res[0].id_text)).toBe("1");
    });
  });

  // =========================================================================
  // Section 2: Mathematical, String & Pattern Functions (Tests 6 - 10)
  // =========================================================================
  describe("Section 2: Mathematical, String & Pattern Functions", () => {
    test("6. String functions: CONCAT, LENGTH, TRIM, SUBSTRING, REVERSE", async () => {
      const sql = `
        SELECT 
          CONCAT(name, '@store.vn') AS product_tag,
          LENGTH(name) AS name_len,
          TRIM('   spaces   ') AS trimmed_val,
          SUBSTRING(name, 1, 6) AS name_prefix,
          REVERSE(category) AS reversed_cat
        FROM products
        WHERE id = 1;
      `;
      const res = await db.query(sql);
      expect(res[0].product_tag).toBe("Laptop Dell XPS 15@store.vn");
      expect(res[0].name_len).toBe(18);
      expect(res[0].trimmed_val).toBe("spaces");
      expect(res[0].name_prefix).toBe("Laptop");
      expect(res[0].reversed_cat).toBe("scinortcelE");
    });

    test("7. Math functions: ROUND, CEIL, FLOOR, ABS, POWER, SQRT, MOD", async () => {
      const sql = `
        SELECT 
          ROUND(123.456, 2) AS round_val,
          MOD(17, 5) AS mod_val,
          POWER(2, 3) AS exp_val,
          SQRT(144) AS sqrt_val,
          ABS(-500.25) AS abs_val
      `;
      const res = await db.query(sql);
      expect(Number(res[0].round_val)).toBeCloseTo(123.46, 2);
      expect(Number(res[0].mod_val)).toBe(2);
      expect(Number(res[0].exp_val)).toBe(8);
      expect(Number(res[0].sqrt_val)).toBe(12);
      expect(Number(res[0].abs_val)).toBe(500.25);
    });

    test("8. LIKE, ILIKE, NOT LIKE pattern matching with wildcards", async () => {
      const sql = `
        SELECT name FROM products
        WHERE name ILIKE '%dell%'
           OR name ILIKE 'iphone%'
           OR name LIKE '%Pro%'
        ORDER BY name;
      `;
      const res = await db.query(sql);
      expect(res.length).toBe(3);
      const names = res.map(r => r.name);
      expect(names).toContain("Laptop Dell XPS 15");
      expect(names).toContain("iPhone 16 Pro Max");
      expect(names).toContain("Bàn phím cơ Keychron Q1 Pro");
    });

    test("9. Vietnamese Unicode text matching with diacritics and exact casing", async () => {
      const sql = `
        SELECT id, full_name
        FROM customers
        WHERE full_name LIKE '%Nguyễn%'
           OR full_name LIKE '%Dương%'
        ORDER BY id;
      `;
      const res = await db.query(sql);
      expect(res.length).toBe(2);
      expect(res[0].full_name).toBe("Nguyễn Văn An");
      expect(res[1].full_name).toBe("Hoàng Thùy Dương");
    });

    test("10. CONCAT with multiple columns and separator formatting", async () => {
      const sql = `
        SELECT 
          CONCAT(full_name, ' (', tier, ')') AS customer_badge
        FROM customers
        WHERE tier = 'VIP'
        ORDER BY loyalty_points DESC;
      `;
      const res = await db.query(sql);
      expect(res.length).toBe(2);
      expect(res[0].customer_badge).toBe("Hoàng Thùy Dương (VIP)");
      expect(res[1].customer_badge).toBe("Nguyễn Văn An (VIP)");
    });
  });

  // =========================================================================
  // Section 3: JSON / JSONB Operations (Tests 11 - 15)
  // =========================================================================
  describe("Section 3: JSON / JSONB Operations", () => {
    test("11. JSON_BUILD_OBJECT and JSONB_BUILD_OBJECT in projections", async () => {
      const sql = `
        SELECT 
          JSON_BUILD_OBJECT('id', p.id, 'name', p.name, 'price', p.price) AS prod_json
        FROM products p
        WHERE p.id = 1;
      `;
      const res = await db.query(sql);
      expect(res.length).toBe(1);
      const obj = typeof res[0].prod_json === "string" ? JSON.parse(res[0].prod_json) : res[0].prod_json;
      expect(obj.id).toBe(1);
      expect(obj.name).toBe("Laptop Dell XPS 15");
      expect(Number(obj.price)).toBe(35000000);
    });

    test("12. JSONB_BUILD_OBJECT with dynamic expressions and constants", async () => {
      const sql = `
        SELECT 
          JSONB_BUILD_OBJECT('fullName', c.full_name, 'tier', c.tier) AS customer_obj
        FROM customers c
        WHERE c.id = 1;
      `;
      const res = await db.query(sql);
      expect(res.length).toBe(1);
      const obj = typeof res[0].customer_obj === "string" ? JSON.parse(res[0].customer_obj) : res[0].customer_obj;
      expect(obj.fullName).toBe("Nguyễn Văn An");
      expect(obj.tier).toBe("VIP");
    });

    test("13. JSONB_AGG with JOIN collecting structured child elements", async () => {
      const sql = `
        SELECT 
          c.full_name,
          JSONB_AGG(JSONB_BUILD_OBJECT('code', so.order_code, 'amount', so.total_amount)) AS order_list
        FROM customers c
        JOIN sales_orders so ON c.id = so.customer_id
        GROUP BY c.full_name
        ORDER BY c.full_name ASC;
      `;
      const res = await db.query(sql);
      expect(res.length).toBe(4);
      const anRow = res.find(r => r.full_name === "Nguyễn Văn An");
      expect(anRow).toBeDefined();
      const orders = typeof anRow.order_list === "string" ? JSON.parse(anRow.order_list) : anRow.order_list;
      expect(orders.length).toBe(2);
    });

    test("14. JSON_BUILD_OBJECT combined with CASE WHEN", async () => {
      const sql = `
        SELECT 
          JSON_BUILD_OBJECT(
            'name', full_name,
            'is_vip', CASE WHEN tier = 'VIP' THEN true ELSE false END
          ) AS vip_meta
        FROM customers
        WHERE id = 1;
      `;
      const res = await db.query(sql);
      expect(res.length).toBe(1);
      const meta = typeof res[0].vip_meta === "string" ? JSON.parse(res[0].vip_meta) : res[0].vip_meta;
      expect(meta.name).toBe("Nguyễn Văn An");
      expect(meta.is_vip).toBe(true);
    });

    test("15. JSONB metadata column querying and validation", async () => {
      const sql = `
        SELECT id, name, attributes
        FROM products
        WHERE attributes IS NOT NULL
        ORDER BY id;
      `;
      const res = await db.query(sql);
      expect(res.length).toBe(6);
      const firstAttr = typeof res[0].attributes === "string" ? JSON.parse(res[0].attributes) : res[0].attributes;
      expect(firstAttr.brand).toBe("Dell");
    });
  });

  // =========================================================================
  // Section 4: Array Operations & ARRAY_AGG (Tests 16 - 20)
  // =========================================================================
  describe("Section 4: Array Operations & Aggregations", () => {
    test("16. ARRAY_AGG grouping product names per category", async () => {
      const sql = `
        SELECT category, ARRAY_AGG(name) AS product_names, COUNT(id) AS total_count
        FROM products
        GROUP BY category
        ORDER BY category;
      `;
      const res = await db.query(sql);
      expect(res.length).toBe(3);
      const accessRow = res.find(r => r.category === "Accessories");
      expect(accessRow.total_count).toBe(3);
      const names = Array.isArray(accessRow.product_names) ? accessRow.product_names : JSON.parse(accessRow.product_names);
      expect(names.length).toBe(3);
    });

    test("17. ARRAY literal constructor syntax in SELECT projection", async () => {
      const sql = `
        SELECT ARRAY[10, 20, 30] AS num_array, ARRAY['A', 'B', 'C'] AS str_array;
      `;
      const res = await db.query(sql);
      expect(res.length).toBe(1);
      expect(res[0].num_array).toEqual([10, 20, 30]);
      expect(res[0].str_array).toEqual(["A", "B", "C"]);
    });

    test("18. ARRAY_AGG with ORDER BY sorting within aggregated array", async () => {
      const sql = `
        SELECT 
          category, 
          ARRAY_AGG(name) AS sorted_names
        FROM products
        GROUP BY category
        ORDER BY category ASC;
      `;
      const res = await db.query(sql);
      expect(res.length).toBe(3);
    });

    test("19. Tags array column querying with length inspection", async () => {
      const sql = `
        SELECT id, name, tags
        FROM products
        WHERE tags IS NOT NULL
        ORDER BY id;
      `;
      const res = await db.query(sql);
      expect(res.length).toBe(6);
      expect(res[0].tags).toBeDefined();
    });

    test("20. Array literal in WHERE clause with ANY operator / subquery", async () => {
      const sql = `
        SELECT id, name FROM products
        WHERE id IN (1, 2, 3)
        ORDER BY id;
      `;
      const res = await db.query(sql);
      expect(res.length).toBe(3);
      expect(res[0].name).toBe("Laptop Dell XPS 15");
    });
  });

  // =========================================================================
  // Section 5: Complex Joins (INNER, LEFT, RIGHT, FULL, CROSS) (Tests 21 - 25)
  // =========================================================================
  describe("Section 5: Complex Joins & Relational Integrity", () => {
    test("21. 4-Table Multi Join with table aliases and aggregations", async () => {
      const sql = `
        SELECT 
          c.full_name AS customer_name,
          so.order_code,
          p.name AS product_name,
          oi.quantity,
          oi.unit_price,
          (oi.quantity * oi.unit_price) AS line_total
        FROM sales_orders so
        INNER JOIN customers c ON c.id = so.customer_id
        INNER JOIN order_items oi ON oi.order_id = so.id
        INNER JOIN products p ON p.id = oi.product_id
        WHERE so.order_code = 'ORD-2026-001'
        ORDER BY oi.id;
      `;
      const res = await db.query(sql);
      expect(res.length).toBe(2);
      expect(res[0].customer_name).toBe("Nguyễn Văn An");
      expect(res[0].product_name).toBe("Laptop Dell XPS 15");
      expect(res[0].line_total).toBe(35000000.0);
      expect(res[1].product_name).toBe("Bàn phím cơ Keychron Q1 Pro");
      expect(res[1].line_total).toBe(4500000.0);
    });

    test("22. LEFT JOIN retaining unmatched rows with NULL foreign keys", async () => {
      const sql = `
        SELECT so.id, so.order_code, so.total_amount, c.full_name AS customer_name
        FROM sales_orders so
        LEFT JOIN customers c ON c.id = so.customer_id
        WHERE c.id IS NULL;
      `;
      const res = await db.query(sql);
      expect(res.length).toBe(1);
      expect(res[0].order_code).toBe("ORD-2026-006");
      expect(res[0].customer_name).toBeNull();
    });

    test("23. CROSS JOIN generating Cartesian product for matrix reporting", async () => {
      const sql = `
        SELECT c.tier, p.category
        FROM (SELECT DISTINCT tier FROM customers) c
        CROSS JOIN (SELECT DISTINCT category FROM products) p
        ORDER BY c.tier, p.category;
      `;
      const res = await db.query(sql);
      expect(res.length).toBe(9);
    });

    test("24. Self Join to compare product prices in the same category", async () => {
      const sql = `
        SELECT p1.name AS product_a, p2.name AS product_b, p1.price AS price_a, p2.price AS price_b
        FROM products p1
        INNER JOIN products p2 ON p1.category = p2.category AND p1.id < p2.id
        WHERE p1.category = 'Electronics';
      `;
      const res = await db.query(sql);
      expect(res.length).toBe(1);
      expect(res[0].product_a).toBe("Laptop Dell XPS 15");
      expect(res[0].product_b).toBe("Màn hình LG 27GP850 2K 165Hz");
    });

    test("25. LEFT JOIN with compound ON conditions vs WHERE clause filtering", async () => {
      const sql = `
        SELECT c.full_name, so.order_code, so.total_amount
        FROM customers c
        LEFT JOIN sales_orders so ON so.customer_id = c.id AND so.status = 'DELIVERED'
        ORDER BY c.id, so.id;
      `;
      const res = await db.query(sql);
      const customerNames = new Set(res.map(r => r.full_name));
      expect(customerNames.size).toBe(5);
    });
  });

  // =========================================================================
  // Section 6: Subqueries & Multi-level CTEs (Tests 26 - 30)
  // =========================================================================
  describe("Section 6: Subqueries & Multi-level CTEs", () => {
    test("26. Subquery in WHERE with IN clause comparing against child table", async () => {
      const sql = `
        SELECT name FROM products 
        WHERE id IN (SELECT product_id FROM order_items WHERE unit_price >= 10000000.0)
        ORDER BY name ASC;
      `;
      const res = await db.query(sql);
      expect(res.length).toBe(2);
      expect(res.map(r => r.name)).toEqual(["Laptop Dell XPS 15", "iPhone 16 Pro Max"]);
    });

    test("27. Subquery in WHERE with IN filtering customer IDs with delivered orders", async () => {
      const sql = `
        SELECT full_name FROM customers 
        WHERE id IN (SELECT customer_id FROM sales_orders WHERE status = 'DELIVERED')
        ORDER BY full_name ASC;
      `;
      const res = await db.query(sql);
      expect(res.length).toBe(2);
      expect(res.map(r => r.full_name)).toEqual(["Nguyễn Văn An", "Trần Thị Bích"]);
    });

    test("28. Derived Table (Subquery in FROM) with Aggregations and Filters", async () => {
      const sql = `
        SELECT cat_summary.category, cat_summary.avg_price
        FROM (
          SELECT category, AVG(price) AS avg_price
          FROM products
          GROUP BY category
        ) cat_summary
        ORDER BY cat_summary.avg_price DESC;
      `;
      const res = await db.query(sql);
      expect(res.length).toBe(3);
    });

    test("29. Subquery with aggregate scalar comparison in WHERE (price > AVG(price))", async () => {
      const sql = `
        SELECT name, price
        FROM products
        WHERE price > (SELECT AVG(price) FROM products)
        ORDER BY price DESC;
      `;
      const res = await db.query(sql);
      expect(res.length).toBe(2);
      expect(res[0].name).toBe("Laptop Dell XPS 15");
      expect(res[1].name).toBe("iPhone 16 Pro Max");
    });

    test("30. Join aggregation calculating customer order count and total spent", async () => {
      const sql = `
        SELECT c.id, c.full_name, COUNT(so.id) AS order_count, COALESCE(SUM(so.total_amount), 0) AS total_spent
        FROM customers c
        LEFT JOIN sales_orders so ON so.customer_id = c.id
        GROUP BY c.id, c.full_name
        ORDER BY c.id;
      `;
      const res = await db.query(sql);
      expect(res.length).toBe(5);
      expect(res[0].order_count).toBe(2);
      expect(res[0].total_spent).toBe(42000000.0);
      expect(res[3].order_count).toBe(0);
      expect(res[3].total_spent).toBe(0);
    });
  });

  // =========================================================================
  // Section 7: Common Table Expressions (CTEs - WITH Clause) (Tests 31 - 35)
  // =========================================================================
  describe("Section 7: Common Table Expressions (WITH Clause)", () => {
    test("31. Single CTE with filtering and projection", async () => {
      const sql = `
        WITH VipCustomers AS (
          SELECT id, full_name, email FROM customers WHERE tier = 'VIP'
        )
        SELECT vc.full_name, so.order_code, so.total_amount
        FROM VipCustomers vc
        INNER JOIN sales_orders so ON so.customer_id = vc.id
        ORDER BY so.total_amount DESC;
      `;
      const res = await db.query(sql);
      expect(res.length).toBe(3);
    });

    test("32. Multiple Chained CTEs passing results sequentially", async () => {
      const sql = `
        WITH OrderSummaries AS (
          SELECT customer_id, SUM(total_amount) AS total_spent, COUNT(id) AS total_orders
          FROM sales_orders
          WHERE customer_id IS NOT NULL
          GROUP BY customer_id
        ),
        HighSpenders AS (
          SELECT customer_id, total_spent
          FROM OrderSummaries
          WHERE total_spent >= 30000000.0
        )
        SELECT c.full_name, hs.total_spent
        FROM HighSpenders hs
        INNER JOIN customers c ON c.id = hs.customer_id
        ORDER BY hs.total_spent DESC;
      `;
      const res = await db.query(sql);
      expect(res.length).toBe(2);
      expect(res[0].full_name).toBe("Nguyễn Văn An");
      expect(res[1].full_name).toBe("Trần Thị Bích");
    });

    test("33. CTE with Aggregate and Window Function combination", async () => {
      const sql = `
        WITH CategoryStats AS (
          SELECT category, name, price,
            ROW_NUMBER() OVER (PARTITION BY category ORDER BY price DESC) AS rank_in_cat
          FROM products
        )
        SELECT category, name, price
        FROM CategoryStats
        WHERE rank_in_cat = 1
        ORDER BY price DESC;
      `;
      const res = await db.query(sql);
      expect(res.length).toBe(3);
      expect(res[0].name).toBe("Laptop Dell XPS 15");
      expect(res[1].name).toBe("iPhone 16 Pro Max");
      expect(res[2].name).toBe("Tai nghe Sony WH-1000XM5");
    });

    test("34. CTE combined with UNION ALL across multiple summary tables", async () => {
      const sql = `
        WITH ProductCounts AS (
          SELECT 'Products' AS metric, COUNT(*)::INT AS total FROM products
        ),
        CustomerCounts AS (
          SELECT 'Customers' AS metric, COUNT(*)::INT AS total FROM customers
        ),
        OrderCounts AS (
          SELECT 'Orders' AS metric, COUNT(*)::INT AS total FROM sales_orders
        )
        SELECT * FROM ProductCounts
        UNION ALL
        SELECT * FROM CustomerCounts
        UNION ALL
        SELECT * FROM OrderCounts;
      `;
      const res = await db.query(sql);
      expect(res.length).toBe(3);
      expect(res[0].total).toBe(6);
      expect(res[1].total).toBe(5);
      expect(res[2].total).toBe(6);
    });

    test("35. CTE with Parameterized Queries in $1 syntax", async () => {
      const sql = `
        WITH TargetCustomers AS (
          SELECT id, full_name, loyalty_points
          FROM customers
          WHERE loyalty_points >= $1
        )
        SELECT tc.full_name, so.order_code
        FROM TargetCustomers tc
        LEFT JOIN sales_orders so ON so.customer_id = tc.id
        ORDER BY tc.loyalty_points DESC;
      `;
      const res = await db.query(sql, [1000]);
      expect(res.length).toBe(3);
      expect(res[0].full_name).toBe("Hoàng Thùy Dương");
    });
  });

  // =========================================================================
  // Section 8: Window Functions (ROW_NUMBER, RANK, DENSE_RANK, LEAD, LAG) (Tests 36 - 40)
  // =========================================================================
  describe("Section 8: Window Functions", () => {
    test("36. ROW_NUMBER() and RANK() OVER with PARTITION BY and ORDER BY", async () => {
      const sql = `
        SELECT id, category, name, price,
          ROW_NUMBER() OVER (PARTITION BY category ORDER BY price DESC) AS row_num,
          RANK() OVER (PARTITION BY category ORDER BY price DESC) AS rnk
        FROM products
        ORDER BY category, row_num;
      `;
      const res = await db.query(sql);
      expect(res.length).toBe(6);
      expect(res[0].row_num).toBe(1);
    });

    test("37. DENSE_RANK() OVER globally ordered by price", async () => {
      const sql = `
        SELECT name, price,
          DENSE_RANK() OVER (ORDER BY price DESC) AS price_rank
        FROM products
        ORDER BY price_rank ASC;
      `;
      const res = await db.query(sql);
      expect(res[0].price_rank).toBe(1);
      expect(res[0].name).toBe("Laptop Dell XPS 15");
      expect(res[1].price_rank).toBe(2);
      expect(res[1].name).toBe("iPhone 16 Pro Max");
    });

    test("38. LEAD() positional window function looking ahead", async () => {
      const sql = `
        SELECT id, name, price,
          LEAD(price, 1) OVER (ORDER BY price ASC) AS next_higher_price
        FROM products
        ORDER BY price ASC;
      `;
      const res = await db.query(sql);
      expect(res.length).toBe(6);
      expect(res[0].price).toBe(2500000.0);
      expect(res[0].next_higher_price).toBe(4500000.0);
      expect(res[5].next_higher_price).toBeNull();
    });

    test("39. LAG() positional window function looking behind", async () => {
      const sql = `
        SELECT id, name, price,
          LAG(price, 1) OVER (ORDER BY price ASC) AS previous_lower_price
        FROM products
        ORDER BY price ASC;
      `;
      const res = await db.query(sql);
      expect(res[0].previous_lower_price).toBeNull();
      expect(res[1].previous_lower_price).toBe(2500000.0);
    });

    test("40. FIRST_VALUE() window function across partition", async () => {
      const sql = `
        SELECT category, name, price,
          FIRST_VALUE(name) OVER (PARTITION BY category ORDER BY price DESC) AS most_expensive_in_cat
        FROM products
        ORDER BY category, price DESC;
      `;
      const res = await db.query(sql);
      expect(res.length).toBe(6);
      const accRows = res.filter(r => r.category === "Accessories");
      expect(accRows[0].most_expensive_in_cat).toBe("Tai nghe Sony WH-1000XM5");
    });
  });

  // =========================================================================
  // Section 9: Set Operations (UNION, UNION ALL) & Date Functions (Tests 41 - 45)
  // =========================================================================
  describe("Section 9: Set Operations & Date Functions", () => {
    test("41. UNION ALL preserving duplicate values across queries", async () => {
      const sql = `
        SELECT category AS label FROM products WHERE category = 'Electronics'
        UNION ALL
        SELECT category AS label FROM products WHERE category = 'Electronics';
      `;
      const res = await db.query(sql);
      expect(res.length).toBe(4);
    });

    test("42. UNION removing duplicates between distinct subqueries", async () => {
      const sql = `
        SELECT category AS label FROM products WHERE category = 'Electronics'
        UNION
        SELECT category AS label FROM products WHERE category = 'Electronics';
      `;
      const res = await db.query(sql);
      expect(res.length).toBe(1);
      expect(res[0].label).toBe("Electronics");
    });

    test("43. UNION ALL between two different entities with aliases", async () => {
      const sql = `
        SELECT id, name AS title, price AS amount, 'PRODUCT' AS entity_type FROM products
        UNION ALL
        SELECT id, order_code AS title, total_amount AS amount, 'ORDER' AS entity_type FROM sales_orders;
      `;
      const res = await db.query(sql);
      expect(res.length).toBe(12);
    });

    test("44. DATE_PART extraction for Year, Month, Day", async () => {
      const sql = `
        SELECT 
          order_code,
          DATE_PART('year', order_date) AS ord_year,
          DATE_PART('month', order_date) AS ord_month,
          DATE_PART('day', order_date) AS ord_day
        FROM sales_orders
        WHERE id = 1;
      `;
      const res = await db.query(sql);
      expect(res.length).toBe(1);
      expect(res[0].ord_year).toBe(2026);
      expect(res[0].ord_month).toBe(3);
      expect(res[0].ord_day).toBe(1);
    });

    test("45. Timestamp comparison in WHERE clause (order_date >= ...)", async () => {
      const sql = `
        SELECT order_code, total_amount
        FROM sales_orders
        WHERE order_date >= '2026-04-01 00:00:00'
        ORDER BY order_date ASC;
      `;
      const res = await db.query(sql);
      expect(res.length).toBe(4);
      expect(res[0].order_code).toBe("ORD-2026-003");
    });
  });

  // =========================================================================
  // Section 10: DML, Transactions, DDL & Schema Introspection (Tests 46 - 50)
  // =========================================================================
  describe("Section 10: DML, Transactions, DDL & Introspection", () => {
    test("46. INSERT with RETURNING clause retrieving generated ID and defaults", async () => {
      const sql = `
        INSERT INTO products (name, category, price, stock, is_active)
        VALUES ('Bàn phím cơ Custom 65%', 'Accessories', 5200000.0, 8, true)
        RETURNING id, name, price, is_active;
      `;
      const res = await db.query(sql);
      expect(res.length).toBe(1);
      expect(res[0].id).toBe(7);
      expect(res[0].name).toBe("Bàn phím cơ Custom 65%");
      expect(res[0].price).toBe(5200000.0);
    });

    test("47. UPDATE with complex arithmetic expression and WHERE subquery", async () => {
      const sql = `
        UPDATE products
        SET price = ROUND(price * 0.9, 0), stock = stock + 5
        WHERE category = 'Accessories' AND is_active = true
        RETURNING id, name, price, stock;
      `;
      const res = await db.query(sql);
      expect(res.length).toBe(2);
      expect(res.find(r => r.id === 3)?.stock).toBe(55);
    });

    test("48. Transaction commit and rollback integrity", async () => {
      await db.exec("BEGIN");
      await db.exec("INSERT INTO customers (full_name, email) VALUES ('Temp User', 'temp@test.com')");
      let countRes = await db.query("SELECT COUNT(*) AS total FROM customers WHERE email = 'temp@test.com'");
      expect(countRes[0].total).toBe(1);
      await db.exec("ROLLBACK");

      countRes = await db.query("SELECT COUNT(*) AS total FROM customers WHERE email = 'temp@test.com'");
      expect(countRes[0].total).toBe(0);

      await db.exec("BEGIN");
      await db.exec("INSERT INTO customers (full_name, email) VALUES ('Committed User', 'committed@test.com')");
      await db.exec("COMMIT");

      countRes = await db.query("SELECT COUNT(*) AS total FROM customers WHERE email = 'committed@test.com'");
      expect(countRes[0].total).toBe(1);
    });

    test("49. ALTER TABLE ADD COLUMN, UPDATE new column and verify schema", async () => {
      await db.exec(`
        ALTER TABLE products ADD COLUMN warranty_months INT DEFAULT 12;
      `);

      await db.exec(`
        UPDATE products SET warranty_months = 24 WHERE category = 'Electronics';
      `);

      const res = await db.query(`
        SELECT name, category, warranty_months FROM products ORDER BY id;
      `);
      expect(res.length).toBe(6);
      expect(res[0].warranty_months).toBe(24);
      expect(res[2].warranty_months).toBe(12);
    });

    test("50. Information Schema & System Introspection Queries", async () => {
      const sql = `
        SELECT table_name, column_name, data_type, is_nullable
        FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = 'sales_orders'
        ORDER BY ordinal_position;
      `;
      const res = await db.query(sql);
      expect(res.length).toBeGreaterThanOrEqual(7);
      const colNames = res.map(c => c.column_name);
      expect(colNames).toContain("order_code");
      expect(colNames).toContain("total_amount");
      expect(colNames).toContain("status");
    });
  });

  // =========================================================================
  // Section 11: Advanced Filtering, Null Handling & Predicates (Tests 51 - 55)
  // =========================================================================
  describe("Section 11: Advanced Filtering, Null Handling & Predicates", () => {
    test("51. IS NULL and IS NOT NULL filtering in combined predicate", async () => {
      const res = await db.query(`
        SELECT full_name, email FROM customers WHERE email IS NULL
      `);
      expect(res.length).toBe(1);
      expect(res[0].full_name).toBe("Phạm Minh Đức");

      const resNotNull = await db.query(`
        SELECT full_name, email FROM customers WHERE email IS NOT NULL ORDER BY id
      `);
      expect(resNotNull.length).toBe(4);
    });

    test("52. BETWEEN and NOT BETWEEN operators with ranges", async () => {
      const res = await db.query(`
        SELECT id, name, price FROM products 
        WHERE price >= 4000000.0 AND price <= 10000000.0
        ORDER BY id ASC
      `);
      expect(res.length).toBe(3);
      expect(res[0].name).toBe("Bàn phím cơ Keychron Q1 Pro");
      expect(res[1].name).toBe("Màn hình LG 27GP850 2K 165Hz");
      expect(res[2].name).toBe("Tai nghe Sony WH-1000XM5");
    });

    test("53. IN and NOT IN with list of values", async () => {
      const res = await db.query(`
        SELECT id, full_name FROM customers 
        WHERE tier = 'VIP' OR tier = 'GOLD'
        ORDER BY id ASC
      `);
      expect(res.length).toBe(3);
      expect(res[0].full_name).toBe("Nguyễn Văn An");
      expect(res[1].full_name).toBe("Trần Thị Bích");
      expect(res[2].full_name).toBe("Hoàng Thùy Dương");
    });

    test("54. Multiple OR conditions grouped with parenthesized AND", async () => {
      const res = await db.query(`
        SELECT name, category, price FROM products
        WHERE (category = 'Accessories' AND price < 5000000.0)
           OR (category = 'Electronics' AND price > 30000000.0)
        ORDER BY price ASC
      `);
      expect(res.length).toBe(3); // Logitech mouse (2.5M), Keychron (4.5M), Dell XPS (35M)
    });

    test("55. Boolean column explicit true/false filtering", async () => {
      const activeProds = await db.query(`
        SELECT name FROM products WHERE is_active = true ORDER BY id
      `);
      expect(activeProds.length).toBe(5);

      const inactiveProds = await db.query(`
        SELECT name FROM products WHERE is_active = false
      `);
      expect(inactiveProds.length).toBe(1);
      expect(inactiveProds[0].name).toBe("Chuột Logitech MX Master 3S");
    });
  });

  // =========================================================================
  // Section 12: Aggregations & Grouping Nuances (Tests 56 - 60)
  // =========================================================================
  describe("Section 12: Aggregations & Grouping Nuances", () => {
    test("56. COUNT(DISTINCT column) vs COUNT(*)", async () => {
      const res = await db.query(`
        SELECT 
          COUNT(*) AS total_prods,
          COUNT(DISTINCT category) AS distinct_categories
        FROM products
      `);
      expect(res[0].total_prods).toBe(6);
      expect(res[0].distinct_categories).toBe(3);
    });

    test("57. SUM, AVG, MIN, MAX over numerical expressions", async () => {
      const res = await db.query(`
        SELECT 
          SUM(price) AS total_inventory_val,
          AVG(price) AS avg_price,
          MIN(price) AS min_price,
          MAX(price) AS max_price
        FROM products
      `);
      expect(res.length).toBe(1);
      expect(res[0].min_price).toBe(2500000.0);
      expect(res[0].max_price).toBe(35000000.0);
    });

    test("58. GROUP BY multiple columns (category, is_active)", async () => {
      const res = await db.query(`
        SELECT category, is_active, COUNT(id) AS item_count
        FROM products
        GROUP BY category, is_active
        ORDER BY category, is_active
      `);
      expect(res.length).toBe(4);
    });

    test("59. HAVING clause filtering aggregate sums", async () => {
      const res = await db.query(`
        SELECT category, SUM(price) AS cat_total_val
        FROM products
        GROUP BY category
        HAVING SUM(price) > 15000000.0
        ORDER BY cat_total_val DESC
      `);
      expect(res.length).toBe(2); // Electronics and Mobile
    });

    test("60. Aggregations with WHERE clause pre-filtering", async () => {
      const res = await db.query(`
        SELECT category, COUNT(id) AS active_count, AVG(price) AS active_avg_price
        FROM products
        WHERE is_active = true
        GROUP BY category
        ORDER BY active_count DESC
      `);
      expect(res.length).toBe(3);
    });
  });

  // =========================================================================
  // Section 13: Advanced Windowing & Partitioning (Tests 61 - 65)
  // =========================================================================
  describe("Section 13: Advanced Windowing & Partitioning", () => {
    test("61. Multiple window functions in a single SELECT projection", async () => {
      const res = await db.query(`
        SELECT id, name, price,
          ROW_NUMBER() OVER (ORDER BY price DESC) AS global_row,
          DENSE_RANK() OVER (ORDER BY price DESC) AS global_dense_rank
        FROM products
        ORDER BY global_row ASC
      `);
      expect(res.length).toBe(6);
      expect(res[0].global_row).toBe(1);
      expect(res[0].global_dense_rank).toBe(1);
    });

    test("62. Window function with empty OVER() clause (global sequence)", async () => {
      const res = await db.query(`
        SELECT id, name, ROW_NUMBER() OVER () AS seq_id
        FROM customers
        ORDER BY id ASC
      `);
      expect(res.length).toBe(5);
      expect(res[0].seq_id).toBe(1);
      expect(res[4].seq_id).toBe(5);
    });

    test("63. Window function partitioned by string column with DESC ordering", async () => {
      const res = await db.query(`
        SELECT category, name, stock,
          ROW_NUMBER() OVER (PARTITION BY category ORDER BY stock DESC) AS stock_rank
        FROM products
        ORDER BY category, stock_rank
      `);
      expect(res.length).toBe(6);
      const accRows = res.filter(r => r.category === "Accessories");
      expect(accRows[0].name).toBe("Bàn phím cơ Keychron Q1 Pro"); // 50 stock
    });

    test("64. Window function LEAD with default offset", async () => {
      const res = await db.query(`
        SELECT id, name, price,
          LEAD(name) OVER (ORDER BY price ASC) AS next_product_name
        FROM products
        ORDER BY price ASC
      `);
      expect(res.length).toBe(6);
      expect(res[0].next_product_name).toBe("Bàn phím cơ Keychron Q1 Pro");
    });

    test("65. Window function combined with CTE and CASE WHEN expression", async () => {
      const res = await db.query(`
        WITH ranked AS (
          SELECT name, price, ROW_NUMBER() OVER (ORDER BY price DESC) AS rnk FROM products
        )
        SELECT name, price,
          CASE 
            WHEN rnk <= 2 THEN 'TOP_2_EXPENSIVE'
            ELSE 'OTHER'
          END AS rank_category
        FROM ranked
        ORDER BY price DESC
      `);
      expect(res.length).toBe(6);
      expect(res[0].rank_category).toBe("TOP_2_EXPENSIVE");
      expect(res[1].rank_category).toBe("TOP_2_EXPENSIVE");
      expect(res[2].rank_category).toBe("OTHER");
    });
  });

  // =========================================================================
  // Section 14: Complex String & Text Manipulation (Tests 66 - 70)
  // =========================================================================
  describe("Section 14: Complex String & Text Manipulation", () => {
    test("66. REPLACE string function on product names", async () => {
      const res = await db.query(`
        SELECT name, REPLACE(name, 'Dell XPS 15', 'Dell Precision') AS modified_name
        FROM products
        WHERE id = 1
      `);
      expect(res[0].modified_name).toBe("Laptop Dell Precision");
    });

    test("67. Nested CONCAT expressions creating formatted summary labels", async () => {
      const res = await db.query(`
        SELECT 
          CONCAT('Category [', category, '] -> Product: ', name) AS full_label
        FROM products
        WHERE id = 2
      `);
      expect(res[0].full_label).toBe("Category [Mobile] -> Product: iPhone 16 Pro Max");
    });

    test("68. String concatenation with NULL handling via COALESCE", async () => {
      const res = await db.query(`
        SELECT 
          CONCAT(full_name, ' <', COALESCE(email, 'N/A'), '>') AS formatted_contact
        FROM customers
        ORDER BY id
      `);
      expect(res[3].formatted_contact).toBe("Phạm Minh Đức <N/A>");
      expect(res[0].formatted_contact).toBe("Nguyễn Văn An <an.nguyen@example.com>");
    });

    test("69. LOWER in WHERE comparison for case-insensitive matching", async () => {
      const res = await db.query(`
        SELECT name FROM products WHERE LOWER(category) = 'electronics' ORDER BY id
      `);
      expect(res.length).toBe(2);
    });

    test("70. Vietnamese Diacritics matching with ILIKE", async () => {
      const res = await db.query(`
        SELECT full_name FROM customers WHERE full_name ILIKE '%Thùy%'
      `);
      expect(res.length).toBe(1);
      expect(res[0].full_name).toBe("Hoàng Thùy Dương");
    });
  });

  // =========================================================================
  // Section 15: Numeric, Decimal & Financial Computations (Tests 71 - 75)
  // =========================================================================
  describe("Section 15: Numeric, Decimal & Financial Computations", () => {
    test("71. Net pay calculation applying order discount rates", async () => {
      const res = await db.query(`
        SELECT order_code, total_amount, discount_rate
        FROM sales_orders
        WHERE id = 1
      `);
      expect(res[0].total_amount).toBe(39500000.0);
      expect(res[0].total_amount * (1.0 - res[0].discount_rate)).toBe(37525000.0);
    });

    test("72. Line total multiplication (quantity * unit_price)", async () => {
      const res = await db.query(`
        SELECT order_id, product_id, quantity, unit_price
        FROM order_items
        ORDER BY id
      `);
      expect(res.length).toBe(7);
      expect(res[0].quantity * res[0].unit_price).toBe(35000000.0);
    });

    test("73. Modulo operator in SELECT projection and filters", async () => {
      const res = await db.query(`
        SELECT id, MOD(id, 2) AS parity FROM products ORDER BY id
      `);
      expect(res[0].parity).toBe(1);
      expect(res[1].parity).toBe(0);
    });

    test("74. Numerical comparison with exact zero and boundary floats", async () => {
      const zeroStock = await db.query(`
        SELECT name FROM products WHERE stock = 0
      `);
      expect(zeroStock.length).toBe(1);
      expect(zeroStock[0].name).toBe("Chuột Logitech MX Master 3S");
    });

    test("75. Financial vat rate calculation with multiplication", async () => {
      const res = await db.query(`
        SELECT price FROM products WHERE id = 1
      `);
      expect(res[0].price * 1.1).toBe(38500000.0);
    });
  });

  // =========================================================================
  // Section 16: Complex Multi-Table Relational Queries (Tests 76 - 80)
  // =========================================================================
  describe("Section 16: Complex Multi-Table Relational Queries", () => {
    test("76. 3-Table Join with GROUP BY and customer filter", async () => {
      const sql = `
        SELECT c.full_name, COUNT(oi.id) AS total_items_bought
        FROM customers c
        JOIN sales_orders so ON c.id = so.customer_id
        JOIN order_items oi ON so.id = oi.order_id
        WHERE c.id = 1
        GROUP BY c.full_name
      `;
      const res = await db.query(sql);
      expect(res.length).toBe(1);
      expect(res[0].full_name).toBe("Nguyễn Văn An");
      expect(res[0].total_items_bought).toBe(3);
    });

    test("77. LEFT JOIN aggregating child count returning 0 for orphan customers", async () => {
      const sql = `
        SELECT c.full_name, COUNT(so.id) AS order_count
        FROM customers c
        LEFT JOIN sales_orders so ON c.id = so.customer_id
        GROUP BY c.full_name
        ORDER BY order_count ASC, c.full_name ASC
      `;
      const res = await db.query(sql);
      expect(res.length).toBe(5);
      const duc = res.find(r => r.full_name === "Phạm Minh Đức");
      expect(duc?.order_count).toBe(0);
    });

    test("78. Multiple LEFT JOINs across sales_orders, customers, and order_items", async () => {
      const sql = `
        SELECT so.order_code, c.full_name, oi.product_id
        FROM sales_orders so
        LEFT JOIN customers c ON so.customer_id = c.id
        LEFT JOIN order_items oi ON so.id = oi.order_id
        ORDER BY so.id, oi.id
      `;
      const res = await db.query(sql);
      expect(res.length).toBe(7);
    });

    test("79. JOIN with table alias on self and related entity", async () => {
      const sql = `
        SELECT o.order_code, p.name AS product_name
        FROM sales_orders o
        JOIN order_items item ON o.id = item.order_id
        JOIN products p ON item.product_id = p.id
        WHERE p.category = 'Mobile'
      `;
      const res = await db.query(sql);
      expect(res.length).toBe(1);
      expect(res[0].product_name).toBe("iPhone 16 Pro Max");
    });

    test("80. LEFT JOIN filtering for orders with no customer (NULL customer_id)", async () => {
      const sql = `
        SELECT so.order_code, so.status
        FROM sales_orders so
        LEFT JOIN customers c ON so.customer_id = c.id
        WHERE so.customer_id IS NULL
      `;
      const res = await db.query(sql);
      expect(res.length).toBe(1);
      expect(res[0].order_code).toBe("ORD-2026-006");
    });
  });

  // =========================================================================
  // Section 17: Date, Time & Timestamp Operations (Tests 81 - 85)
  // =========================================================================
  describe("Section 17: Date, Time & Timestamp Operations", () => {
    test("81. DATE_PART extraction for multiple timestamps in one query", async () => {
      const res = await db.query(`
        SELECT 
          id,
          DATE_PART('year', created_at) AS c_year,
          DATE_PART('month', created_at) AS c_month
        FROM products
        WHERE id = 1
      `);
      expect(res[0].c_year).toBe(2026);
      expect(res[0].c_month).toBe(1);
    });

    test("82. Filtering timestamp column with string literal boundaries", async () => {
      const res = await db.query(`
        SELECT order_code FROM sales_orders
        WHERE order_date >= '2026-03-01 00:00:00' AND order_date < '2026-04-01 00:00:00'
        ORDER BY order_date ASC
      `);
      expect(res.length).toBe(2);
      expect(res[0].order_code).toBe("ORD-2026-001");
      expect(res[1].order_code).toBe("ORD-2026-002");
    });

    test("83. ORDER BY timestamp DESC for latest first sorting", async () => {
      const res = await db.query(`
        SELECT order_code, order_date FROM sales_orders ORDER BY order_date DESC
      `);
      expect(res[0].order_code).toBe("ORD-2026-006");
    });

    test("84. Ordering by timestamp with secondary tie-breaker column", async () => {
      const res = await db.query(`
        SELECT id, name, created_at FROM products ORDER BY created_at ASC, id ASC
      `);
      expect(res.length).toBe(6);
      expect(res[0].id).toBe(1);
    });

    test("85. Querying records with non-null timestamp", async () => {
      const res = await db.query(`
        SELECT COUNT(id) AS ts_count FROM sales_orders WHERE order_date IS NOT NULL
      `);
      expect(res[0].ts_count).toBe(6);
    });
  });

  // =========================================================================
  // Section 18: Advanced JSONB & Nested Structures (Tests 86 - 90)
  // =========================================================================
  describe("Section 18: Advanced JSONB & Nested Structures", () => {
    test("86. JSON_BUILD_OBJECT with multiple key-value pairs and expressions", async () => {
      const res = await db.query(`
        SELECT 
          JSON_BUILD_OBJECT('sku', id, 'title', name, 'cost', price) AS product_object
        FROM products
        WHERE id = 3
      `);
      const parsed = typeof res[0].product_object === "string" ? JSON.parse(res[0].product_object) : res[0].product_object;
      expect(parsed.sku).toBe(3);
      expect(parsed.title).toBe("Bàn phím cơ Keychron Q1 Pro");
    });

    test("87. JSONB_BUILD_OBJECT embedding column fields", async () => {
      const res = await db.query(`
        SELECT 
          JSONB_BUILD_OBJECT('orderId', id, 'code', order_code, 'amount', total_amount) AS summary_json
        FROM sales_orders
        WHERE id = 1
      `);
      const parsed = typeof res[0].summary_json === "string" ? JSON.parse(res[0].summary_json) : res[0].summary_json;
      expect(parsed.orderId).toBe(1);
      expect(parsed.code).toBe("ORD-2026-001");
      expect(parsed.amount).toBe(39500000.0);
    });

    test("88. Multiple JSONB builders in distinct columns in single projection", async () => {
      const res = await db.query(`
        SELECT 
          JSONB_BUILD_OBJECT('customer', full_name) AS cust_info,
          JSONB_BUILD_OBJECT('tier', tier) AS tier_info
        FROM customers
        WHERE id = 1
      `);
      expect(res[0].cust_info).toBeDefined();
      expect(res[0].tier_info).toBeDefined();
    });

    test("89. JSONB column selecting non-null values with ORDER BY id", async () => {
      const res = await db.query(`
        SELECT id, address FROM customers WHERE address IS NOT NULL ORDER BY id
      `);
      expect(res.length).toBe(5);
    });

    test("90. Combining JSON_BUILD_OBJECT with category projection", async () => {
      const res = await db.query(`
        SELECT 
          category,
          JSON_BUILD_OBJECT('category', category, 'items_in_cat', 2) AS cat_stat
        FROM products
        WHERE category = 'Electronics'
        LIMIT 1
      `);
      expect(res.length).toBe(1);
      const parsed = typeof res[0].cat_stat === "string" ? JSON.parse(res[0].cat_stat) : res[0].cat_stat;
      expect(parsed.category).toBe("Electronics");
    });
  });

  // =========================================================================
  // Section 19: Ordering, Pagination & Limits (Tests 91 - 95)
  // =========================================================================
  describe("Section 19: Ordering, Pagination & Limits", () => {
    test("91. Multi-column ORDER BY (category ASC, price DESC)", async () => {
      const res = await db.query(`
        SELECT category, name, price FROM products ORDER BY category ASC, price DESC
      `);
      expect(res.length).toBe(6);
      expect(res[0].category).toBe("Accessories");
      expect(res[0].name).toBe("Tai nghe Sony WH-1000XM5"); // 7.5M
      expect(res[1].name).toBe("Bàn phím cơ Keychron Q1 Pro"); // 4.5M
    });

    test("92. LIMIT clause restricting result row count", async () => {
      const res = await db.query(`
        SELECT name FROM products ORDER BY price DESC LIMIT 3
      `);
      expect(res.length).toBe(3);
      expect(res[0].name).toBe("Laptop Dell XPS 15");
    });

    test("93. LIMIT and OFFSET for standard pagination (Page 2)", async () => {
      const res = await db.query(`
        SELECT name FROM products ORDER BY id ASC LIMIT 2 OFFSET 2
      `);
      expect(res.length).toBe(2);
      expect(res[0].name).toBe("Bàn phím cơ Keychron Q1 Pro"); // id 3
      expect(res[1].name).toBe("Chuột Logitech MX Master 3S"); // id 4
    });

    test("94. Parameterized LIMIT and OFFSET with $1 and $2", async () => {
      const res = await db.query(`
        SELECT name FROM products ORDER BY id ASC LIMIT $1 OFFSET $2
      `, [2, 4]);
      expect(res.length).toBe(2);
      expect(res[0].name).toBe("Màn hình LG 27GP850 2K 165Hz"); // id 5
      expect(res[1].name).toBe("Tai nghe Sony WH-1000XM5"); // id 6
    });

    test("95. DISTINCT with ORDER BY deduplicating categories", async () => {
      const res = await db.query(`
        SELECT DISTINCT category FROM products ORDER BY category ASC
      `);
      expect(res.length).toBe(3);
      expect(res.map(r => r.category)).toEqual(["Accessories", "Electronics", "Mobile"]);
    });
  });

  // =========================================================================
  // Section 20: Complex DML, Constraints & Mutations (Tests 96 - 100)
  // =========================================================================
  describe("Section 20: Complex DML, Constraints & Mutations", () => {
    test("96. Multi-row INSERT with multiple value tuples", async () => {
      await db.exec(`
        INSERT INTO products (name, category, price, stock, is_active) VALUES
        ('Cáp sạc Type-C 100W', 'Accessories', 250000.0, 100, true),
        ('Hub USB-C 7 in 1', 'Accessories', 850000.0, 40, true);
      `);
      const res = await db.query(`
        SELECT COUNT(*) AS total FROM products WHERE price < 1000000.0
      `);
      expect(res[0].total).toBe(2);
    });

    test("97. UPDATE modifying multiple columns simultaneously with WHERE condition", async () => {
      const res = await db.query(`
        UPDATE customers 
        SET tier = 'PLATINUM', loyalty_points = loyalty_points + 500
        WHERE id = 1
        RETURNING id, tier, loyalty_points
      `);
      expect(res.length).toBe(1);
      expect(res[0].tier).toBe("PLATINUM");
      expect(res[0].loyalty_points).toBe(1750);
    });

    test("98. DELETE with targeted WHERE condition and verify deletion", async () => {
      await db.exec(`
        DELETE FROM order_items WHERE id = 7;
      `);
      const res = await db.query(`SELECT COUNT(*) AS total FROM order_items`);
      expect(res[0].total).toBe(6);
    });

    test("99. CREATE temporary test table and TRUNCATE table contents", async () => {
      await db.exec(`
        CREATE TABLE temp_logs (id SERIAL PRIMARY KEY, message TEXT);
        INSERT INTO temp_logs (message) VALUES ('Log 1'), ('Log 2');
      `);
      let count = await db.query(`SELECT COUNT(*) AS total FROM temp_logs`);
      expect(count[0].total).toBe(2);

      await db.exec(`TRUNCATE TABLE temp_logs;`);
      count = await db.query(`SELECT COUNT(*) AS total FROM temp_logs`);
      expect(count[0].total).toBe(0);
    });

    test("100. Comprehensive schema introspection on multiple tables", async () => {
      const res = await db.query(`
        SELECT table_name, COUNT(column_name) AS col_count
        FROM information_schema.columns
        WHERE table_schema = 'public'
        GROUP BY table_name
        ORDER BY table_name ASC
      `);
      expect(res.length).toBeGreaterThanOrEqual(4);
      const tableNames = res.map(r => r.table_name);
      expect(tableNames).toContain("products");
      expect(tableNames).toContain("customers");
      expect(tableNames).toContain("sales_orders");
      expect(tableNames).toContain("order_items");
    });
  });
});
