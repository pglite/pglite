import { describe, test, expect, beforeAll, afterAll } from "bun:test";
import { PGLite } from "../src/index";

describe("100 Comprehensive In-Depth PostgreSQL CTE & WITH RECURSIVE Test Cases for PGLite", () => {
  let db: PGLite;

  beforeAll(async () => {
    db = new PGLite(":memory:", { native: true });

    // Table 1: Departments
    await db.query(`
      CREATE TABLE departments (
        dept_id SERIAL PRIMARY KEY,
        dept_name TEXT NOT NULL,
        budget NUMERIC,
        location TEXT
      );
    `);

    // Table 2: Employees (with self-referencing manager_id for hierarchy testing)
    await db.query(`
      CREATE TABLE employees (
        emp_id SERIAL PRIMARY KEY,
        name TEXT NOT NULL,
        dept_id INT,
        manager_id INT,
        salary NUMERIC,
        hire_date TEXT
      );
    `);

    // Table 3: Product Categories (parent-child category tree)
    await db.query(`
      CREATE TABLE categories (
        cat_id SERIAL PRIMARY KEY,
        cat_name TEXT NOT NULL,
        parent_id INT
      );
    `);

    // Table 4: Flight Network (directed graph for path traversal)
    await db.query(`
      CREATE TABLE flights (
        flight_id SERIAL PRIMARY KEY,
        origin TEXT NOT NULL,
        destination TEXT NOT NULL,
        cost NUMERIC NOT NULL
      );
    `);

    // Table 5: Sales Orders
    await db.query(`
      CREATE TABLE sales_orders (
        order_id SERIAL PRIMARY KEY,
        emp_id INT,
        order_date TEXT,
        amount NUMERIC
      );
    `);

    // Table 6: Audit logs for data-modifying CTEs
    await db.query(`
      CREATE TABLE order_archive (
        archive_id SERIAL PRIMARY KEY,
        order_id INT,
        archived_amount NUMERIC
      );
    `);

    // Seed Departments
    await db.query(`
      INSERT INTO departments (dept_id, dept_name, budget, location) VALUES
      (1, 'Engineering', 1000000, 'San Francisco'),
      (2, 'Sales', 500000, 'New York'),
      (3, 'Marketing', 300000, 'London'),
      (4, 'Executive', 2000000, 'San Francisco');
    `);

    // Seed Employees (Hierarchy: Alice (CEO) -> Bob & Charlie (Directors) -> David & Eve & Frank (Staff))
    await db.query(`
      INSERT INTO employees (emp_id, name, dept_id, manager_id, salary, hire_date) VALUES
      (1, 'Alice CEO', 4, NULL, 250000, '2020-01-15'),
      (2, 'Bob Eng Dir', 1, 1, 180000, '2020-03-01'),
      (3, 'Charlie Sales Dir', 2, 1, 160000, '2020-06-15'),
      (4, 'David Lead Dev', 1, 2, 130000, '2021-01-10'),
      (5, 'Eve Senior Dev', 1, 4, 110000, '2021-05-20'),
      (6, 'Frank Junior Dev', 1, 4, 80000, '2022-02-01'),
      (7, 'Grace Sales Rep', 2, 3, 75000, '2021-08-15'),
      (8, 'Heidi Mktg Lead', 3, 1, 95000, '2021-09-01');
    `);

    // Seed Categories (Electronics -> Computers -> Laptops / Desktops; Electronics -> Audio -> Headphones)
    await db.query(`
      INSERT INTO categories (cat_id, cat_name, parent_id) VALUES
      (1, 'Electronics', NULL),
      (2, 'Computers', 1),
      (3, 'Audio', 1),
      (4, 'Laptops', 2),
      (5, 'Desktops', 2),
      (6, 'Headphones', 3),
      (7, 'Accessories', NULL);
    `);

    // Seed Flights (Graph: SFO -> ORD -> JFK; SFO -> LAX -> MIA -> JFK)
    await db.query(`
      INSERT INTO flights (flight_id, origin, destination, cost) VALUES
      (1, 'SFO', 'ORD', 200),
      (2, 'ORD', 'JFK', 150),
      (3, 'SFO', 'LAX', 100),
      (4, 'LAX', 'MIA', 250),
      (5, 'MIA', 'JFK', 120),
      (6, 'JFK', 'LHR', 500);
    `);

    // Seed Sales Orders
    await db.query(`
      INSERT INTO sales_orders (order_id, emp_id, order_date, amount) VALUES
      (101, 3, '2026-01-10', 5000),
      (102, 7, '2026-01-15', 3000),
      (103, 7, '2026-02-01', 4500),
      (104, 3, '2026-02-10', 8000),
      (105, 7, '2026-03-05', 1200),
      (106, 2, '2026-03-12', 2500);
    `);
  });

  afterAll(async () => {
    if (db) {
      await db.close();
    }
  });

  // =========================================================================
  // Section 1: Basic Single & Multi-CTE Projections (Tests 1 - 15)
  // =========================================================================
  describe("Section 1: Basic Single & Multi-CTE Projections", () => {
    test("01. Simple CTE projection with constant scalar value", async () => {
      const res = await db.query("WITH cte AS (SELECT 42 AS answer) SELECT answer FROM cte;");
      expect(Number(res[0].answer)).toBe(42);
    });

    test("02. Simple CTE selecting all rows from a table", async () => {
      const res = await db.query("WITH all_depts AS (SELECT * FROM departments) SELECT dept_name FROM all_depts ORDER BY dept_id ASC;");
      expect(res.length).toBe(4);
      expect(res[0].dept_name).toBe("Engineering");
    });

    test("03. CTE with WHERE filter", async () => {
      const res = await db.query("WITH high_sal AS (SELECT * FROM employees WHERE salary > 150000) SELECT name FROM high_sal ORDER BY salary DESC;");
      expect(res.length).toBe(3);
      expect(res[0].name).toBe("Alice CEO");
    });

    test("04. CTE with computed columns and alias", async () => {
      const res = await db.query(`
        WITH emp_comp AS (
          SELECT name, salary, salary * 0.10 AS bonus
          FROM employees
          WHERE dept_id = 1
        )
        SELECT name, bonus FROM emp_comp ORDER BY bonus DESC;
      `);
      expect(res.length).toBe(4);
      expect(Number(res[0].bonus)).toBe(18000);
    });

    test("05. CTE with explicit column list alias syntax WITH cte(c1, c2) AS", async () => {
      const res = await db.query("WITH emp_alias(employee_title, annual_pay) AS (SELECT name, salary FROM employees WHERE emp_id = 1) SELECT employee_title, annual_pay FROM emp_alias;");
      expect(res[0].employee_title).toBe("Alice CEO");
      expect(Number(res[0].annual_pay)).toBe(250000);
    });

    test("06. Multiple CTEs separated by comma", async () => {
      const res = await db.query(`
        WITH
          eng AS (SELECT name FROM employees WHERE dept_id = 1),
          sales AS (SELECT name FROM employees WHERE dept_id = 2)
        SELECT * FROM eng ORDER BY name ASC;
      `);
      expect(res.length).toBe(4);
    });

    test("07. Main query joining multiple CTEs", async () => {
      const res = await db.query(`
        WITH
          d AS (SELECT dept_id, dept_name FROM departments),
          e AS (SELECT emp_id, name, dept_id FROM employees WHERE emp_id = 1)
        SELECT e.name, d.dept_name
        FROM e
        JOIN d ON e.dept_id = d.dept_id;
      `);
      expect(res.length).toBe(1);
      expect(res[0].name).toBe("Alice CEO");
      expect(res[0].dept_name).toBe("Executive");
    });

    test("08. CTE with string functions UPPER and CONCAT", async () => {
      const res = await db.query(`
        WITH formatted AS (
          SELECT UPPER(name) AS uname, location FROM employees e JOIN departments d ON e.dept_id = d.dept_id WHERE e.emp_id = 1
        )
        SELECT uname, location FROM formatted;
      `);
      expect(res[0].uname).toBe("ALICE CEO");
      expect(res[0].location).toBe("San Francisco");
    });

    test("09. CTE with CASE WHEN expressions", async () => {
      const res = await db.query(`
        WITH categorized AS (
          SELECT name, salary,
            CASE
              WHEN salary >= 150000 THEN 'Executive'
              WHEN salary >= 100000 THEN 'Senior'
              ELSE 'Standard'
            END AS band
          FROM employees
        )
        SELECT name, band FROM categorized WHERE band = 'Senior' ORDER BY name ASC;
      `);
      expect(res.length).toBe(2);
      expect(res[0].name).toBe("David Lead Dev");
      expect(res[1].name).toBe("Eve Senior Dev");
    });

    test("10. CTE with ORDER BY and LIMIT inside CTE definition", async () => {
      const res = await db.query(`
        WITH top_earner AS (
          SELECT name, salary FROM employees ORDER BY salary DESC LIMIT 1
        )
        SELECT name FROM top_earner;
      `);
      expect(res[0].name).toBe("Alice CEO");
    });

    test("11. CTE with DISTINCT values", async () => {
      const res = await db.query(`
        WITH distinct_depts AS (
          SELECT DISTINCT dept_id FROM employees WHERE dept_id IS NOT NULL
        )
        SELECT count(*) AS total_depts FROM distinct_depts;
      `);
      expect(Number(res[0].total_depts)).toBe(4);
    });

    test("12. CTE with subquery inside WHERE clause", async () => {
      const res = await db.query(`
        WITH avg_info AS (
          SELECT name, salary FROM employees WHERE salary > (SELECT AVG(salary) FROM employees)
        )
        SELECT count(*) AS above_avg_count FROM avg_info;
      `);
      expect(Number(res[0].above_avg_count)).toBeGreaterThan(0);
    });

    test("13. Main query filtering on CTE result with WHERE", async () => {
      const res = await db.query(`
        WITH dept_summary AS (
          SELECT dept_id, dept_name, budget FROM departments
        )
        SELECT dept_name FROM dept_summary WHERE budget >= 1000000 ORDER BY budget DESC;
      `);
      expect(res.length).toBe(2);
      expect(res[0].dept_name).toBe("Executive");
      expect(res[1].dept_name).toBe("Engineering");
    });

    test("14. Multiple independent CTEs querying different tables", async () => {
      const res = await db.query(`
        WITH
          c AS (SELECT count(*) AS c_count FROM categories),
          f AS (SELECT count(*) AS f_count FROM flights)
        SELECT c_count, f_count FROM c, f;
      `);
      expect(Number(res[0].c_count)).toBe(7);
      expect(Number(res[0].f_count)).toBe(6);
    });

    test("15. CTE with NULL handling and COALESCE", async () => {
      const res = await db.query(`
        WITH null_handled AS (
          SELECT name, COALESCE(manager_id, 0) AS safe_mgr FROM employees WHERE manager_id IS NULL
        )
        SELECT name, safe_mgr FROM null_handled;
      `);
      expect(res.length).toBe(1);
      expect(res[0].name).toBe("Alice CEO");
      expect(Number(res[0].safe_mgr)).toBe(0);
    });
  });

  // =========================================================================
  // Section 2: Chained & Nested CTEs (Tests 16 - 30)
  // =========================================================================
  describe("Section 2: Chained & Nested CTEs", () => {
    test("16. Second CTE referencing the first CTE", async () => {
      const res = await db.query(`
        WITH
          eng_emps AS (SELECT emp_id, name, salary FROM employees WHERE dept_id = 1),
          high_eng AS (SELECT emp_id, name, salary FROM eng_emps WHERE salary >= 100000)
        SELECT name FROM high_eng ORDER BY salary DESC;
      `);
      expect(res.length).toBe(3);
      expect(res[0].name).toBe("Bob Eng Dir");
    });

    test("17. Three-level chained CTE pipeline", async () => {
      const res = await db.query(`
        WITH
          step1 AS (SELECT emp_id, name, salary * 1.05 AS adj_sal FROM employees),
          step2 AS (SELECT emp_id, name, adj_sal FROM step1 WHERE adj_sal > 100000),
          step3 AS (SELECT count(*) AS total FROM step2)
        SELECT total FROM step3;
      `);
      expect(Number(res[0].total)).toBe(5);
    });

    test("18. CTE joined with a regular physical table in main query", async () => {
      const res = await db.query(`
        WITH top_mgrs AS (
          SELECT DISTINCT manager_id FROM employees WHERE manager_id IS NOT NULL
        )
        SELECT e.name
        FROM employees e
        JOIN top_mgrs m ON e.emp_id = m.manager_id
        ORDER BY e.name ASC;
      `);
      expect(res.length).toBe(4);
      expect(res[0].name).toBe("Alice CEO");
    });

    test("19. CTE joined with another CTE on non-key condition", async () => {
      const res = await db.query(`
        WITH
          d AS (SELECT dept_id, budget FROM departments),
          e AS (SELECT dept_id, salary FROM employees WHERE emp_id = 1)
        SELECT e.salary, d.budget
        FROM e
        JOIN d ON e.dept_id = d.dept_id;
      `);
      expect(Number(res[0].salary)).toBe(250000);
      expect(Number(res[0].budget)).toBe(2000000);
    });

    test("20. Left join between CTE and table preserving unmatched rows", async () => {
      const res = await db.query(`
        WITH high_orders AS (
          SELECT emp_id, sum(amount) AS total_sales FROM sales_orders GROUP BY emp_id
        )
        SELECT e.name, h.total_sales
        FROM employees e
        LEFT JOIN high_orders h ON e.emp_id = h.emp_id
        WHERE e.emp_id IN (1, 3)
        ORDER BY e.emp_id ASC;
      `);
      expect(res.length).toBe(2);
      expect(res[0].total_sales).toBeNull();
      expect(Number(res[1].total_sales)).toBe(13000);
    });

    test("21. Chained CTE with aggregation passing result down the pipeline", async () => {
      const res = await db.query(`
        WITH
          sales_per_emp AS (SELECT emp_id, sum(amount) AS total_val FROM sales_orders GROUP BY emp_id),
          max_sales AS (SELECT max(total_val) AS peak FROM sales_per_emp)
        SELECT peak FROM max_sales;
      `);
      expect(Number(res[0].peak)).toBe(13000);
    });

    test("22. CTE referencing two previous independent CTEs", async () => {
      const res = await db.query(`
        WITH
          eng AS (SELECT count(*) AS eng_cnt FROM employees WHERE dept_id = 1),
          sales AS (SELECT count(*) AS sales_cnt FROM employees WHERE dept_id = 2),
          combined AS (SELECT eng_cnt, sales_cnt, eng_cnt + sales_cnt AS total_core FROM eng, sales)
        SELECT total_core FROM combined;
      `);
      expect(Number(res[0].total_core)).toBe(6);
    });

    test("23. CTE with self-join inside the CTE query", async () => {
      const res = await db.query(`
        WITH emp_pairs AS (
          SELECT e1.name AS emp, e2.name AS mgr
          FROM employees e1
          JOIN employees e2 ON e1.manager_id = e2.emp_id
        )
        SELECT emp, mgr FROM emp_pairs WHERE mgr = 'Alice CEO' ORDER BY emp ASC;
      `);
      expect(res.length).toBe(3);
      expect(res[0].emp).toBe("Bob Eng Dir");
      expect(res[1].emp).toBe("Charlie Sales Dir");
      expect(res[2].emp).toBe("Heidi Mktg Lead");
    });

    test("24. Nested subquery in main query referencing a CTE", async () => {
      const res = await db.query(`
        WITH depts AS (SELECT dept_id, dept_name FROM departments)
        SELECT name FROM employees WHERE dept_id IN (SELECT dept_id FROM depts WHERE dept_name = 'Engineering') ORDER BY name ASC;
      `);
      expect(res.length).toBe(4);
      expect(res[0].name).toBe("Bob Eng Dir");
    });

    test("25. Multiple CTEs with arithmetic across CTE results", async () => {
      const res = await db.query(`
        WITH
          eng_budget AS (SELECT budget FROM departments WHERE dept_name = 'Engineering'),
          sales_budget AS (SELECT budget FROM departments WHERE dept_name = 'Sales')
        SELECT eng_budget.budget - sales_budget.budget AS diff
        FROM eng_budget, sales_budget;
      `);
      expect(Number(res[0].diff)).toBe(500000);
    });

    test("26. Chained CTE filtering by string matching", async () => {
      const res = await db.query(`
        WITH
          devs AS (SELECT name, salary FROM employees WHERE name LIKE '%Dev%'),
          leads AS (SELECT name, salary FROM devs WHERE name LIKE '%Lead%')
        SELECT name FROM leads;
      `);
      expect(res.length).toBe(1);
      expect(res[0].name).toBe("David Lead Dev");
    });

    test("27. CTE with IN clause filtering against list of values", async () => {
      const res = await db.query(`
        WITH key_emps AS (
          SELECT name FROM employees WHERE emp_id IN (1, 2, 3)
        )
        SELECT count(*) AS cnt FROM key_emps;
      `);
      expect(Number(res[0].cnt)).toBe(3);
    });

    test("28. CTE with BETWEEN range filter", async () => {
      const res = await db.query(`
        WITH mid_range AS (
          SELECT name, salary FROM employees WHERE salary BETWEEN 100000 AND 170000
        )
        SELECT name FROM mid_range ORDER BY salary ASC;
      `);
      expect(res.length).toBe(3);
      expect(res[0].name).toBe("Eve Senior Dev");
    });

    test("29. CTE projecting boolean expressions", async () => {
      const res = await db.query(`
        WITH flags AS (
          SELECT name, salary > 100000 AS is_high_salary FROM employees WHERE emp_id = 1
        )
        SELECT name, is_high_salary FROM flags;
      `);
      expect(res[0].is_high_salary).toBe(true);
    });

    test("30. CTE with table aliasing inside the main FROM clause", async () => {
      const res = await db.query(`
        WITH e_cte AS (SELECT emp_id, name, salary FROM employees)
        SELECT my_alias.name, my_alias.salary FROM e_cte AS my_alias WHERE my_alias.emp_id = 1;
      `);
      expect(res[0].name).toBe("Alice CEO");
    });
  });

  // =========================================================================
  // Section 3: CTEs with Aggregations, GROUP BY, HAVING & Window Functions (Tests 31 - 45)
  // =========================================================================
  describe("Section 3: CTEs with Aggregations, GROUP BY, HAVING & Window Functions", () => {
    test("31. CTE performing GROUP BY and SUM", async () => {
      const res = await db.query(`
        WITH dept_salaries AS (
          SELECT dept_id, sum(salary) AS total_dept_salary FROM employees GROUP BY dept_id
        )
        SELECT dept_id, total_dept_salary FROM dept_salaries WHERE dept_id = 1;
      `);
      expect(Number(res[0].total_dept_salary)).toBe(500000);
    });

    test("32. CTE performing GROUP BY with COUNT and AVG", async () => {
      const res = await db.query(`
        WITH dept_stats AS (
          SELECT dept_id, count(*) AS emp_count, avg(salary) AS avg_sal FROM employees GROUP BY dept_id
        )
        SELECT dept_id, emp_count, avg_sal FROM dept_stats WHERE dept_id = 1;
      `);
      expect(Number(res[0].emp_count)).toBe(4);
      expect(Number(res[0].avg_sal)).toBe(125000);
    });

    test("33. CTE with HAVING clause filtering aggregated groups", async () => {
      const res = await db.query(`
        WITH large_depts AS (
          SELECT dept_id, count(*) AS head_count FROM employees GROUP BY dept_id HAVING count(*) > 1
        )
        SELECT dept_id, head_count FROM large_depts ORDER BY dept_id ASC;
      `);
      expect(res.length).toBe(2);
      expect(Number(res[0].dept_id)).toBe(1);
      expect(Number(res[1].dept_id)).toBe(2);
    });

    test("34. Main query aggregating over CTE results", async () => {
      const res = await db.query(`
        WITH emp_salaries AS (
          SELECT salary FROM employees WHERE dept_id = 1
        )
        SELECT avg(salary) AS avg_eng, max(salary) AS max_eng, min(salary) AS min_eng FROM emp_salaries;
      `);
      expect(Number(res[0].avg_eng)).toBe(125000);
      expect(Number(res[0].max_eng)).toBe(180000);
      expect(Number(res[0].min_eng)).toBe(80000);
    });

    test("35. CTE with string aggregation STRING_AGG", async () => {
      const res = await db.query(`
        WITH eng_names AS (
          SELECT name FROM employees WHERE dept_id = 1 ORDER BY name ASC
        )
        SELECT string_agg(name, ', ') AS names_csv FROM eng_names;
      `);
      expect(res[0].names_csv).toContain("Bob Eng Dir");
    });

    test("36. CTE with ARRAY_AGG collecting elements", async () => {
      const res = await db.query(`
        WITH eng_ids AS (
          SELECT emp_id FROM employees WHERE dept_id = 1 ORDER BY emp_id ASC
        )
        SELECT array_agg(emp_id) AS id_list FROM eng_ids;
      `);
      expect(res[0].id_list).toEqual([2, 4, 5, 6]);
    });

    test("37. CTE calculating percentage of department total", async () => {
      const res = await db.query(`
        WITH
          total AS (SELECT sum(salary) AS grand_total FROM employees),
          eng AS (SELECT sum(salary) AS eng_total FROM employees WHERE dept_id = 1)
        SELECT (eng.eng_total * 100.0 / total.grand_total) AS eng_share
        FROM eng, total;
      `);
      expect(Number(res[0].eng_share)).toBeGreaterThan(30);
    });

    test("38. CTE with ROW_NUMBER() window function", async () => {
      const res = await db.query(`
        WITH ranked AS (
          SELECT emp_id, name, salary, ROW_NUMBER() OVER (ORDER BY salary DESC) AS sal_rank
          FROM employees
        )
        SELECT name, sal_rank FROM ranked WHERE sal_rank = 1;
      `);
      expect(res[0].name).toBe("Alice CEO");
      expect(Number(res[0].sal_rank)).toBe(1);
    });

    test("39. CTE with PARTITION BY in window function", async () => {
      const res = await db.query(`
        WITH dept_ranked AS (
          SELECT emp_id, name, dept_id, salary, ROW_NUMBER() OVER (PARTITION BY dept_id ORDER BY salary DESC) AS dept_rank
          FROM employees
        )
        SELECT name, dept_id FROM dept_ranked WHERE dept_rank = 1 ORDER BY dept_id ASC;
      `);
      expect(res.length).toBe(4);
      expect(res[0].name).toBe("Bob Eng Dir");
    });

    test("40. CTE with DENSE_RANK()", async () => {
      const res = await db.query(`
        WITH ranked AS (
          SELECT emp_id, name, salary, DENSE_RANK() OVER (ORDER BY salary DESC) AS rnk
          FROM employees
        )
        SELECT name, rnk FROM ranked WHERE rnk <= 2 ORDER BY rnk ASC;
      `);
      expect(res.length).toBe(2);
      expect(res[0].name).toBe("Alice CEO");
    });

    test("41. Chained CTEs combining window functions and subsequent filtering", async () => {
      const res = await db.query(`
        WITH
          w AS (SELECT emp_id, name, dept_id, salary, ROW_NUMBER() OVER (PARTITION BY dept_id ORDER BY salary ASC) AS min_rnk FROM employees),
          lowest AS (SELECT name, dept_id FROM w WHERE min_rnk = 1)
        SELECT name FROM lowest WHERE dept_id = 1;
      `);
      expect(res[0].name).toBe("Frank Junior Dev");
    });

    test("42. CTE with SUM() OVER () running total", async () => {
      const res = await db.query(`
        WITH running AS (
          SELECT emp_id, amount, SUM(amount) OVER (ORDER BY order_id ASC) AS cumulative_sales
          FROM sales_orders
        )
        SELECT max(cumulative_sales) AS grand_sales FROM running;
      `);
      expect(Number(res[0].grand_sales)).toBe(24200);
    });

    test("43. CTE with COUNT(DISTINCT col)", async () => {
      const res = await db.query(`
        WITH sellers AS (
          SELECT count(DISTINCT emp_id) AS distinct_sellers FROM sales_orders
        )
        SELECT distinct_sellers FROM sellers;
      `);
      expect(Number(res[0].distinct_sellers)).toBe(3);
    });

    test("44. CTE with multi-column GROUP BY", async () => {
      const res = await db.query(`
        WITH multi_grp AS (
          SELECT dept_id, manager_id, count(*) AS cnt FROM employees GROUP BY dept_id, manager_id
        )
        SELECT count(*) AS group_count FROM multi_grp;
      `);
      expect(Number(res[0].group_count)).toBeGreaterThan(2);
    });

    test("45. Main query joining CTE with aggregate summary", async () => {
      const res = await db.query(`
        WITH dept_avg AS (
          SELECT dept_id, avg(salary) AS avg_salary FROM employees GROUP BY dept_id
        )
        SELECT e.name, e.salary, d.avg_salary
        FROM employees e
        JOIN dept_avg d ON e.dept_id = d.dept_id
        WHERE e.emp_id = 1;
      `);
      expect(Number(res[0].salary)).toBe(250000);
      expect(Number(res[0].avg_salary)).toBe(250000);
    });
  });

  // =========================================================================
  // Section 4: WITH RECURSIVE Sequences, Counting & Math (Tests 46 - 60)
  // =========================================================================
  describe("Section 4: WITH RECURSIVE Sequences, Counting & Math", () => {
    test("46. Generate integer sequence from 1 to 5 with WITH RECURSIVE", async () => {
      const res = await db.query(`
        WITH RECURSIVE seq(n) AS (
          SELECT 1
          UNION ALL
          SELECT n + 1 FROM seq WHERE n < 5
        )
        SELECT n FROM seq ORDER BY n ASC;
      `);
      expect(res.length).toBe(5);
      expect(Number(res[0].n)).toBe(1);
      expect(Number(res[4].n)).toBe(5);
    });

    test("47. Generate step sequence (2, 4, 6, 8, 10) with UNION ALL", async () => {
      const res = await db.query(`
        WITH RECURSIVE evens(val) AS (
          SELECT 2
          UNION ALL
          SELECT val + 2 FROM evens WHERE val < 10
        )
        SELECT val FROM evens ORDER BY val ASC;
      `);
      expect(res.length).toBe(5);
      expect(Number(res[0].val)).toBe(2);
      expect(Number(res[4].val)).toBe(10);
    });

    test("48. Sum of recursive sequence (1 to 10 sum = 55)", async () => {
      const res = await db.query(`
        WITH RECURSIVE nums(n) AS (
          SELECT 1
          UNION ALL
          SELECT n + 1 FROM nums WHERE n < 10
        )
        SELECT sum(n) AS total_sum FROM nums;
      `);
      expect(Number(res[0].total_sum)).toBe(55);
    });

    test("49. Count of recursive sequence iterations", async () => {
      const res = await db.query(`
        WITH RECURSIVE counter(c) AS (
          SELECT 1
          UNION ALL
          SELECT c + 1 FROM counter WHERE c < 20
        )
        SELECT count(*) AS total_rows FROM counter;
      `);
      expect(Number(res[0].total_rows)).toBe(20);
    });

    test("50. Geometric progression (powers of 2: 1, 2, 4, 8, 16)", async () => {
      const res = await db.query(`
        WITH RECURSIVE powers(p) AS (
          SELECT 1
          UNION ALL
          SELECT p * 2 FROM powers WHERE p < 16
        )
        SELECT p FROM powers ORDER BY p ASC;
      `);
      expect(res.length).toBe(5);
      expect(Number(res[4].p)).toBe(16);
    });

    test("51. Factorial calculation via recursive CTE", async () => {
      const res = await db.query(`
        WITH RECURSIVE fact(n, f) AS (
          SELECT 1, 1
          UNION ALL
          SELECT n + 1, f * (n + 1) FROM fact WHERE n < 5
        )
        SELECT f FROM fact WHERE n = 5;
      `);
      expect(Number(res[0].f)).toBe(120);
    });

    test("52. Fibonacci series generation", async () => {
      const res = await db.query(`
        WITH RECURSIVE fib(n, a, b) AS (
          SELECT 1 AS n, 0 AS a, 1 AS b
          UNION ALL
          SELECT n + 1 AS n, b AS a, a + b AS b FROM fib WHERE n < 7
        )
        SELECT a FROM fib ORDER BY n ASC;
      `);
      expect(res.length).toBe(7);
      expect(Number(res[0].a)).toBe(0);
      expect(Number(res[1].a)).toBe(1);
      expect(Number(res[2].a)).toBe(1);
      expect(Number(res[3].a)).toBe(2);
      expect(Number(res[4].a)).toBe(3);
      expect(Number(res[5].a)).toBe(5);
      expect(Number(res[6].a)).toBe(8);
    });

    test("53. Countdown sequence from 10 to 1", async () => {
      const res = await db.query(`
        WITH RECURSIVE countdown(n) AS (
          SELECT 10
          UNION ALL
          SELECT n - 1 FROM countdown WHERE n > 1
        )
        SELECT n FROM countdown ORDER BY n DESC;
      `);
      expect(res.length).toBe(10);
      expect(Number(res[0].n)).toBe(10);
      expect(Number(res[9].n)).toBe(1);
    });

    test("54. Recursive sequence with UNION (deduplicating)", async () => {
      const res = await db.query(`
        WITH RECURSIVE dedupe(n) AS (
          SELECT 1
          UNION
          SELECT CASE WHEN n < 3 THEN n + 1 ELSE 3 END FROM dedupe WHERE n < 3
        )
        SELECT n FROM dedupe ORDER BY n ASC;
      `);
      expect(res.length).toBe(3);
    });

    test("55. Recursive sequence combined with string concatenation", async () => {
      const res = await db.query(`
        WITH RECURSIVE str_builder(step, str) AS (
          SELECT 1, 'A'
          UNION ALL
          SELECT step + 1, str || 'A' FROM str_builder WHERE step < 4
        )
        SELECT str FROM str_builder WHERE step = 4;
      `);
      expect(res[0].str).toBe("AAAA");
    });

    test("56. Square numbers sequence (1, 4, 9, 16, 25)", async () => {
      const res = await db.query(`
        WITH RECURSIVE squares(n, sq) AS (
          SELECT 1, 1
          UNION ALL
          SELECT n + 1, (n + 1) * (n + 1) FROM squares WHERE n < 5
        )
        SELECT sq FROM squares ORDER BY n ASC;
      `);
      expect(Number(res[4].sq)).toBe(25);
    });

    test("57. Sequence filtering with WHERE in the final SELECT", async () => {
      const res = await db.query(`
        WITH RECURSIVE r(x) AS (
          SELECT 1
          UNION ALL
          SELECT x + 1 FROM r WHERE x < 10
        )
        SELECT x FROM r WHERE x > 7 ORDER BY x ASC;
      `);
      expect(res.length).toBe(3);
      expect(Number(res[0].x)).toBe(8);
    });

    test("58. Recursive query with multiple base anchor rows", async () => {
      const res = await db.query(`
        WITH RECURSIVE dual_seed(grp, val) AS (
          SELECT 'A', 1
          UNION ALL
          SELECT 'B', 10
          UNION ALL
          SELECT grp, val + 1 FROM dual_seed WHERE val % 10 < 3
        )
        SELECT count(*) AS cnt FROM dual_seed;
      `);
      expect(Number(res[0].cnt)).toBeGreaterThanOrEqual(4);
    });

    test("59. Recursive sequence joined with physical table", async () => {
      const res = await db.query(`
        WITH RECURSIVE dept_ids(id) AS (
          SELECT 1
          UNION ALL
          SELECT id + 1 FROM dept_ids WHERE id < 3
        )
        SELECT d.dept_name
        FROM dept_ids i
        JOIN departments d ON i.id = d.dept_id
        ORDER BY d.dept_id ASC;
      `);
      expect(res.length).toBe(3);
      expect(res[0].dept_name).toBe("Engineering");
    });

    test("60. Aggregating min and max over recursive sequence", async () => {
      const res = await db.query(`
        WITH RECURSIVE r(n) AS (
          SELECT 5
          UNION ALL
          SELECT n + 5 FROM r WHERE n < 25
        )
        SELECT min(n) AS min_val, max(n) AS max_val FROM r;
      `);
      expect(Number(res[0].min_val)).toBe(5);
      expect(Number(res[0].max_val)).toBe(25);
    });
  });

  // =========================================================================
  // Section 5: WITH RECURSIVE Hierarchies, Trees & Breadcrumbs (Tests 61 - 75)
  // =========================================================================
  describe("Section 5: WITH RECURSIVE Hierarchies, Trees & Breadcrumbs", () => {
    test("61. Category tree traversal finding all descendants of Electronics", async () => {
      const res = await db.query(`
        WITH RECURSIVE cat_tree(cat_id, cat_name, parent_id, depth) AS (
          SELECT cat_id, cat_name, parent_id, 0
          FROM categories
          WHERE cat_name = 'Electronics'
          UNION ALL
          SELECT c.cat_id, c.cat_name, c.parent_id, t.depth + 1
          FROM categories c
          JOIN cat_tree t ON c.parent_id = t.cat_id
        )
        SELECT cat_name, depth FROM cat_tree ORDER BY depth ASC, cat_name ASC;
      `);
      expect(res.length).toBe(6);
      expect(res[0].cat_name).toBe("Electronics");
      expect(Number(res[0].depth)).toBe(0);
    });

    test("62. Breadcrumb path generation for category tree", async () => {
      const res = await db.query(`
        WITH RECURSIVE cat_path(cat_id, cat_name, breadcrumb) AS (
          SELECT cat_id, cat_name, cat_name AS breadcrumb
          FROM categories
          WHERE parent_id IS NULL
          UNION ALL
          SELECT c.cat_id, c.cat_name, p.breadcrumb || ' > ' || c.cat_name AS breadcrumb
          FROM categories c
          JOIN cat_path p ON c.parent_id = p.cat_id
        )
        SELECT breadcrumb FROM cat_path WHERE cat_name = 'Laptops';
      `);
      expect(res[0].breadcrumb).toBe("Electronics > Computers > Laptops");
    });

    test("63. Organizational hierarchy depth from CEO downwards", async () => {
      const res = await db.query(`
        WITH RECURSIVE org_chart(emp_id, name, manager_id, level) AS (
          SELECT emp_id, name, manager_id, 1 AS level
          FROM employees
          WHERE manager_id IS NULL
          UNION ALL
          SELECT e.emp_id, e.name, e.manager_id, o.level + 1 AS level
          FROM employees e
          JOIN org_chart o ON e.manager_id = o.emp_id
        )
        SELECT name, level FROM org_chart WHERE level = 2 ORDER BY name ASC;
      `);
      expect(res.length).toBe(3);
      expect(res[0].name).toBe("Bob Eng Dir");
      expect(res[1].name).toBe("Charlie Sales Dir");
      expect(res[2].name).toBe("Heidi Mktg Lead");
    });

    test("64. Direct and indirect report count for Bob Eng Dir", async () => {
      const res = await db.query(`
        WITH RECURSIVE bob_subordinates(emp_id, name) AS (
          SELECT emp_id, name FROM employees WHERE manager_id = 2
          UNION ALL
          SELECT e.emp_id, e.name
          FROM employees e
          JOIN bob_subordinates s ON e.manager_id = s.emp_id
        )
        SELECT count(*) AS total_reports FROM bob_subordinates;
      `);
      expect(Number(res[0].total_reports)).toBe(3);
    });

    test("65. Finding root ancestor of Frank Junior Dev (bottom-up traversal)", async () => {
      const res = await db.query(`
        WITH RECURSIVE mgmt_chain(emp_id, name, manager_id) AS (
          SELECT emp_id, name, manager_id FROM employees WHERE name = 'Frank Junior Dev'
          UNION ALL
          SELECT e.emp_id, e.name, e.manager_id
          FROM employees e
          JOIN mgmt_chain c ON e.emp_id = c.manager_id
        )
        SELECT name FROM mgmt_chain WHERE manager_id IS NULL;
      `);
      expect(res.length).toBe(1);
      expect(res[0].name).toBe("Alice CEO");
    });

    test("66. Hierarchy depth limit checking level <= 2", async () => {
      const res = await db.query(`
        WITH RECURSIVE limited_tree(emp_id, name, level) AS (
          SELECT emp_id, name, 1 FROM employees WHERE manager_id IS NULL
          UNION ALL
          SELECT e.emp_id, e.name, t.level + 1
          FROM employees e
          JOIN limited_tree t ON e.manager_id = t.emp_id
          WHERE t.level < 2
        )
        SELECT count(*) AS cnt FROM limited_tree;
      `);
      expect(Number(res[0].cnt)).toBe(4);
    });

    test("67. Maximum depth across category hierarchy", async () => {
      const res = await db.query(`
        WITH RECURSIVE depth_calc(cat_id, depth) AS (
          SELECT cat_id, 1 FROM categories WHERE parent_id IS NULL
          UNION ALL
          SELECT c.cat_id, d.depth + 1
          FROM categories c
          JOIN depth_calc d ON c.parent_id = d.cat_id
        )
        SELECT max(depth) AS max_tree_depth FROM depth_calc;
      `);
      expect(Number(res[0].max_tree_depth)).toBe(3);
    });

    test("68. Total salary roll-up under Engineering Director Bob", async () => {
      const res = await db.query(`
        WITH RECURSIVE eng_team(emp_id, salary) AS (
          SELECT emp_id, salary FROM employees WHERE emp_id = 2
          UNION ALL
          SELECT e.emp_id, e.salary
          FROM employees e
          JOIN eng_team t ON e.manager_id = t.emp_id
        )
        SELECT sum(salary) AS total_eng_payroll FROM eng_team;
      `);
      expect(Number(res[0].total_eng_payroll)).toBe(500000);
    });

    test("69. Finding leaf nodes (categories with no subcategories)", async () => {
      const res = await db.query(`
        WITH RECURSIVE all_cats(cat_id, cat_name) AS (
          SELECT cat_id, cat_name FROM categories
        )
        SELECT c.cat_name
        FROM all_cats c
        LEFT JOIN categories sub ON c.cat_id = sub.parent_id
        WHERE sub.cat_id IS NULL
        ORDER BY c.cat_name ASC;
      `);
      expect(res.length).toBe(4);
      expect(res.map(r => r.cat_name)).toContain("Laptops");
      expect(res.map(r => r.cat_name)).toContain("Headphones");
    });

    test("70. Hierarchical path formatting with indentation", async () => {
      const res = await db.query(`
        WITH RECURSIVE indented_tree(cat_id, formatted_name, depth) AS (
          SELECT cat_id, cat_name, 0 FROM categories WHERE parent_id IS NULL
          UNION ALL
          SELECT c.cat_id, '-- ' || c.cat_name, t.depth + 1
          FROM categories c
          JOIN indented_tree t ON c.parent_id = t.cat_id
        )
        SELECT formatted_name FROM indented_tree WHERE formatted_name LIKE '--%' ORDER BY formatted_name ASC;
      `);
      expect(res.length).toBe(5);
    });

    test("71. Multi-root hierarchy traversal", async () => {
      const res = await db.query(`
        WITH RECURSIVE roots(cat_id, cat_name) AS (
          SELECT cat_id, cat_name FROM categories WHERE parent_id IS NULL
        )
        SELECT count(*) AS root_count FROM roots;
      `);
      expect(Number(res[0].root_count)).toBe(2);
    });

    test("72. Subtree item count per department", async () => {
      const res = await db.query(`
        WITH RECURSIVE sub(emp_id, root_mgr) AS (
          SELECT emp_id, emp_id FROM employees WHERE manager_id = 1
          UNION ALL
          SELECT e.emp_id, s.root_mgr
          FROM employees e
          JOIN sub s ON e.manager_id = s.emp_id
        )
        SELECT root_mgr, count(*) AS subtree_size FROM sub GROUP BY root_mgr ORDER BY root_mgr ASC;
      `);
      expect(res.length).toBe(3);
      expect(Number(res[0].subtree_size)).toBe(4);
    });

    test("73. Hierarchical reporting line list for David Lead Dev", async () => {
      const res = await db.query(`
        WITH RECURSIVE line(emp_id, name, manager_id, step) AS (
          SELECT emp_id, name, manager_id, 1 FROM employees WHERE name = 'David Lead Dev'
          UNION ALL
          SELECT e.emp_id, e.name, e.manager_id, l.step + 1
          FROM employees e
          JOIN line l ON e.emp_id = l.manager_id
        )
        SELECT name FROM line ORDER BY step ASC;
      `);
      expect(res.length).toBe(3);
      expect(res[0].name).toBe("David Lead Dev");
      expect(res[1].name).toBe("Bob Eng Dir");
      expect(res[2].name).toBe("Alice CEO");
    });

    test("74. Joining recursive hierarchy with sales orders", async () => {
      const res = await db.query(`
        WITH RECURSIVE sales_team(emp_id) AS (
          SELECT emp_id FROM employees WHERE emp_id = 3
          UNION ALL
          SELECT e.emp_id FROM employees e JOIN sales_team s ON e.manager_id = s.emp_id
        )
        SELECT sum(o.amount) AS total_sales_team_revenue
        FROM sales_orders o
        JOIN sales_team t ON o.emp_id = t.emp_id;
      `);
      expect(Number(res[0].total_sales_team_revenue)).toBe(21700);
    });

    test("75. Combining hierarchy traversal with WHERE on final result", async () => {
      const res = await db.query(`
        WITH RECURSIVE comp_tree(cat_id, cat_name) AS (
          SELECT cat_id, cat_name FROM categories WHERE cat_name = 'Computers'
          UNION ALL
          SELECT c.cat_id, c.cat_name FROM categories c JOIN comp_tree t ON c.parent_id = t.cat_id
        )
        SELECT cat_name FROM comp_tree WHERE cat_name != 'Computers' ORDER BY cat_name ASC;
      `);
      expect(res.length).toBe(2);
      expect(res[0].cat_name).toBe("Desktops");
      expect(res[1].cat_name).toBe("Laptops");
    });
  });

  // =========================================================================
  // Section 6: WITH RECURSIVE Graph & Path Traversal (Tests 76 - 85)
  // =========================================================================
  describe("Section 6: WITH RECURSIVE Graph & Path Traversal", () => {
    test("76. Finding all reachable flight destinations starting from SFO", async () => {
      const res = await db.query(`
        WITH RECURSIVE reachable(destination) AS (
          SELECT destination FROM flights WHERE origin = 'SFO'
          UNION
          SELECT f.destination FROM flights f JOIN reachable r ON f.origin = r.destination
        )
        SELECT destination FROM reachable ORDER BY destination ASC;
      `);
      expect(res.length).toBeGreaterThanOrEqual(4);
      expect(res.map(r => r.destination)).toContain("JFK");
      expect(res.map(r => r.destination)).toContain("LHR");
    });

    test("77. Computing total flight cost from SFO to destinations", async () => {
      const res = await db.query(`
        WITH RECURSIVE flight_paths(origin, destination, total_cost, hops) AS (
          SELECT origin, destination, cost AS total_cost, 1 AS hops FROM flights WHERE origin = 'SFO'
          UNION ALL
          SELECT p.origin, f.destination, p.total_cost + f.cost AS total_cost, p.hops + 1 AS hops
          FROM flights f
          JOIN flight_paths p ON f.origin = p.destination
          WHERE p.hops < 4
        )
        SELECT min(total_cost) AS min_sfo_jfk FROM flight_paths WHERE destination = 'JFK';
      `);
      expect(Number(res[0].min_sfo_jfk)).toBe(350);
    });

    test("78. Tracking full flight route path string", async () => {
      const res = await db.query(`
        WITH RECURSIVE routes(origin, destination, path, hops) AS (
          SELECT origin, destination, origin || '->' || destination AS path, 1 AS hops FROM flights WHERE origin = 'SFO'
          UNION ALL
          SELECT r.origin, f.destination, r.path || '->' || f.destination AS path, r.hops + 1 AS hops
          FROM flights f
          JOIN routes r ON f.origin = r.destination
          WHERE r.hops <= 3
        )
        SELECT path FROM routes WHERE destination = 'LHR';
      `);
      expect(res.length).toBeGreaterThanOrEqual(1);
      expect(res[0].path).toContain("SFO");
      expect(res[0].path).toContain("LHR");
    });

    test("79. Max hops limit in graph traversal", async () => {
      const res = await db.query(`
        WITH RECURSIVE hop_test(origin, destination, hops) AS (
          SELECT origin, destination, 1 FROM flights WHERE origin = 'SFO'
          UNION ALL
          SELECT h.origin, f.destination, h.hops + 1
          FROM flights f
          JOIN hop_test h ON f.origin = h.destination
          WHERE h.hops < 1
        )
        SELECT max(hops) AS max_hops FROM hop_test;
      `);
      expect(Number(res[0].max_hops)).toBe(1);
    });

    test("80. Transitive closure destination count", async () => {
      const res = await db.query(`
        WITH RECURSIVE closure(dest) AS (
          SELECT destination FROM flights WHERE origin = 'SFO'
          UNION
          SELECT f.destination FROM flights f JOIN closure c ON f.origin = c.dest
        )
        SELECT count(DISTINCT dest) AS reach_count FROM closure;
      `);
      expect(Number(res[0].reach_count)).toBeGreaterThanOrEqual(4);
    });

    test("81. Finding all direct and 1-stop flights from SFO", async () => {
      const res = await db.query(`
        WITH RECURSIVE direct_and_1stop(dest, stops) AS (
          SELECT destination, 0 FROM flights WHERE origin = 'SFO'
          UNION ALL
          SELECT f.destination, d.stops + 1
          FROM flights f
          JOIN direct_and_1stop d ON f.origin = d.dest
          WHERE d.stops < 1
        )
        SELECT count(*) AS total_flight_options FROM direct_and_1stop;
      `);
      expect(Number(res[0].total_flight_options)).toBeGreaterThanOrEqual(4);
    });

    test("82. Graph query with destination filtering in anchor", async () => {
      const res = await db.query(`
        WITH RECURSIVE from_ord(dest) AS (
          SELECT destination FROM flights WHERE origin = 'ORD'
          UNION
          SELECT f.destination FROM flights f JOIN from_ord o ON f.origin = o.dest
        )
        SELECT dest FROM from_ord ORDER BY dest ASC;
      `);
      expect(res.map(r => r.dest)).toContain("JFK");
      expect(res.map(r => r.dest)).toContain("LHR");
    });

    test("83. Graph edge count in paths", async () => {
      const res = await db.query(`
        WITH RECURSIVE paths(dest, edges) AS (
          SELECT destination, 1 FROM flights WHERE origin = 'SFO'
          UNION ALL
          SELECT f.destination, p.edges + 1
          FROM flights f
          JOIN paths p ON f.origin = p.dest
          WHERE p.edges < 2
        )
        SELECT dest, edges FROM paths WHERE dest = 'ORD';
      `);
      expect(Number(res[0].edges)).toBe(1);
    });

    test("84. Longest flight route hops from SFO", async () => {
      const res = await db.query(`
        WITH RECURSIVE r(dest, hops) AS (
          SELECT destination, 1 FROM flights WHERE origin = 'SFO'
          UNION ALL
          SELECT f.destination, r.hops + 1
          FROM flights f
          JOIN r ON f.origin = r.dest
          WHERE r.hops < 5
        )
        SELECT max(hops) AS longest_path FROM r;
      `);
      expect(Number(res[0].longest_path)).toBeGreaterThanOrEqual(3);
    });

    test("85. Path cost aggregation over multiple graph branches", async () => {
      const res = await db.query(`
        WITH RECURSIVE all_costs(dest, cost) AS (
          SELECT destination, cost FROM flights WHERE origin = 'SFO'
          UNION ALL
          SELECT f.destination, a.cost + f.cost
          FROM flights f
          JOIN all_costs a ON f.origin = a.dest
          WHERE a.cost < 2000
        )
        SELECT avg(cost) AS avg_path_cost FROM all_costs;
      `);
      expect(Number(res[0].avg_path_cost)).toBeGreaterThan(0);
    });
  });

  // =========================================================================
  // Section 7: Data-Modifying CTEs (Tests 86 - 95)
  // =========================================================================
  describe("Section 7: Data-Modifying CTEs with RETURNING", () => {
    test("86. CTE with INSERT ... RETURNING consumed in main SELECT", async () => {
      const res = await db.query(`
        WITH new_dept AS (
          INSERT INTO departments (dept_name, budget, location)
          VALUES ('Human Resources', 250000, 'Chicago')
          RETURNING dept_id, dept_name
        )
        SELECT dept_name FROM new_dept;
      `);
      expect(res.length).toBe(1);
      expect(res[0].dept_name).toBe("Human Resources");
    });

    test("87. Data inserted via CTE is persisted in table", async () => {
      const res = await db.query("SELECT dept_name FROM departments WHERE dept_name = 'Human Resources';");
      expect(res.length).toBe(1);
      expect(res[0].dept_name).toBe("Human Resources");
    });

    test("88. CTE with UPDATE ... RETURNING updated row values", async () => {
      const res = await db.query(`
        WITH updated_emp AS (
          UPDATE employees
          SET salary = salary + 5000
          WHERE name = 'Frank Junior Dev'
          RETURNING name, salary
        )
        SELECT name, salary FROM updated_emp;
      `);
      expect(res.length).toBe(1);
      expect(Number(res[0].salary)).toBe(85000);
    });

    test("89. CTE with DELETE ... RETURNING deleted rows", async () => {
      await db.query("INSERT INTO sales_orders (order_id, emp_id, order_date, amount) VALUES (999, 1, '2026-09-01', 100);");
      const res = await db.query(`
        WITH deleted_orders AS (
          DELETE FROM sales_orders WHERE order_id = 999
          RETURNING order_id, amount
        )
        SELECT order_id, amount FROM deleted_orders;
      `);
      expect(res.length).toBe(1);
      expect(Number(res[0].order_id)).toBe(999);
      expect(Number(res[0].amount)).toBe(100);
    });

    test("90. Verification that row deleted by CTE is gone", async () => {
      const res = await db.query("SELECT * FROM sales_orders WHERE order_id = 999;");
      expect(res.length).toBe(0);
    });

    test("91. Multi-row INSERT with RETURNING in CTE", async () => {
      const res = await db.query(`
        WITH inserted_cats AS (
          INSERT INTO categories (cat_name, parent_id)
          VALUES ('Smartphones', 1), ('Smartwatches', 1)
          RETURNING cat_name
        )
        SELECT count(*) AS new_cat_count FROM inserted_cats;
      `);
      expect(Number(res[0].new_cat_count)).toBe(2);
    });

    test("92. CTE with UPDATE modifying multiple rows with RETURNING", async () => {
      const res = await db.query(`
        WITH promo AS (
          UPDATE employees
          SET salary = salary * 1.02
          WHERE dept_id = 2
          RETURNING emp_id, salary
        )
        SELECT count(*) AS promoted_count FROM promo;
      `);
      expect(Number(res[0].promoted_count)).toBe(2);
    });

    test("93. CTE with DELETE on empty match returns 0 rows", async () => {
      const res = await db.query(`
        WITH no_del AS (
          DELETE FROM sales_orders WHERE order_id = 99999
          RETURNING *
        )
        SELECT count(*) AS cnt FROM no_del;
      `);
      expect(Number(res[0].cnt)).toBe(0);
    });

    test("94. CTE combining data modification and aggregation in main query", async () => {
      const res = await db.query(`
        WITH new_sales AS (
          INSERT INTO sales_orders (order_id, emp_id, order_date, amount)
          VALUES (201, 2, '2026-04-01', 3000), (202, 2, '2026-04-02', 4000)
          RETURNING amount
        )
        SELECT sum(amount) AS added_volume FROM new_sales;
      `);
      expect(Number(res[0].added_volume)).toBe(7000);
    });

    test("95. CTE with DELETE returning columns joined to another table", async () => {
      const res = await db.query(`
        WITH del AS (
          DELETE FROM sales_orders WHERE order_id IN (201, 202)
          RETURNING emp_id, amount
        )
        SELECT e.name, sum(del.amount) AS total_deleted_amt
        FROM del
        JOIN employees e ON del.emp_id = e.emp_id
        GROUP BY e.name;
      `);
      expect(res.length).toBe(1);
      expect(res[0].name).toBe("Bob Eng Dir");
      expect(Number(res[0].total_deleted_amt)).toBe(7000);
    });
  });

  // =========================================================================
  // Section 8: Parameterized, Edge Cases & Set Operations in CTEs (Tests 96 - 100)
  // =========================================================================
  describe("Section 8: Parameterized, Edge Cases & Set Operations in CTEs", () => {
    test("96. Parameterized CTE with $1 in CTE body", async () => {
      const res = await db.query(
        "WITH p_cte AS (SELECT name, salary FROM employees WHERE dept_id = $1) SELECT name FROM p_cte ORDER BY name ASC;",
        [1]
      );
      expect(res.length).toBe(4);
      expect(res[0].name).toBe("Bob Eng Dir");
    });

    test("97. Parameterized CTE with multiple parameters $1 and $2", async () => {
      const res = await db.query(
        "WITH filter_cte AS (SELECT name, salary FROM employees WHERE salary BETWEEN $1 AND $2) SELECT count(*) AS cnt FROM filter_cte;",
        [100000, 200000]
      );
      expect(Number(res[0].cnt)).toBeGreaterThanOrEqual(3);
    });

    test("98. CTE combined with UNION query in main query", async () => {
      const res = await db.query(`
        WITH
          e AS (SELECT name AS label FROM employees WHERE dept_id = 1),
          d AS (SELECT dept_name AS label FROM departments WHERE dept_id = 1)
        SELECT label FROM e
        UNION
        SELECT label FROM d
        ORDER BY label ASC;
      `);
      expect(res.length).toBe(5);
    });

    test("99. Empty CTE result set handling", async () => {
      const res = await db.query(`
        WITH empty_cte AS (
          SELECT * FROM employees WHERE emp_id = -999
        )
        SELECT count(*) AS cnt FROM empty_cte;
      `);
      expect(Number(res[0].cnt)).toBe(0);
    });

    test("100. CTE with NULL values and IS NULL / IS NOT NULL in main query", async () => {
      const res = await db.query(`
        WITH mgr_check AS (
          SELECT emp_id, name, manager_id FROM employees
        )
        SELECT name FROM mgr_check WHERE manager_id IS NULL;
      `);
      expect(res.length).toBe(1);
      expect(res[0].name).toBe("Alice CEO");
    });
  });
});
