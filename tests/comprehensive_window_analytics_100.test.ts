import { describe, test, expect, beforeAll, afterAll } from "bun:test";
import { PGLite } from "../src/index";

describe("100 Comprehensive In-Depth PostgreSQL Window Functions & Analytics Test Cases for PGLite", () => {
  let db: PGLite;

  beforeAll(async () => {
    db = new PGLite(":memory:", { native: true });

    // Table 1: Employee Salaries & Departments
    await db.query(`
      CREATE TABLE staff_salaries (
        id SERIAL PRIMARY KEY,
        name TEXT NOT NULL,
        dept TEXT NOT NULL,
        role TEXT NOT NULL,
        salary NUMERIC NOT NULL,
        bonus NUMERIC,
        hire_year INT NOT NULL
      );
    `);

    // Table 2: Daily Sales Metrics
    await db.query(`
      CREATE TABLE daily_sales (
        sale_id SERIAL PRIMARY KEY,
        region TEXT NOT NULL,
        sale_date TEXT NOT NULL,
        units INT NOT NULL,
        revenue NUMERIC NOT NULL
      );
    `);

    // Table 3: Student Scores
    await db.query(`
      CREATE TABLE exam_scores (
        student_id SERIAL PRIMARY KEY,
        student_name TEXT NOT NULL,
        subject TEXT NOT NULL,
        score NUMERIC NOT NULL
      );
    `);

    // Seed Staff Salaries
    await db.query(`
      INSERT INTO staff_salaries (id, name, dept, role, salary, bonus, hire_year) VALUES
      (1, 'Alice', 'Engineering', 'Lead', 150000, 15000, 2020),
      (2, 'Bob', 'Engineering', 'Senior', 120000, 10000, 2021),
      (3, 'Charlie', 'Engineering', 'Junior', 80000, 5000, 2022),
      (4, 'David', 'Engineering', 'Junior', 80000, 4000, 2023),
      (5, 'Eve', 'Sales', 'Manager', 130000, 25000, 2019),
      (6, 'Frank', 'Sales', 'Representative', 90000, 15000, 2021),
      (7, 'Grace', 'Sales', 'Representative', 90000, 12000, 2022),
      (8, 'Heidi', 'Marketing', 'Lead', 110000, 8000, 2020),
      (9, 'Ivan', 'Marketing', 'Specialist', 75000, 4000, 2022);
    `);

    // Seed Daily Sales
    await db.query(`
      INSERT INTO daily_sales (sale_id, region, sale_date, units, revenue) VALUES
      (1, 'North', '2026-01-01', 10, 1000),
      (2, 'North', '2026-01-02', 15, 1500),
      (3, 'North', '2026-01-03', 20, 2200),
      (4, 'North', '2026-01-04', 12, 1300),
      (5, 'South', '2026-01-01', 8, 800),
      (6, 'South', '2026-01-02', 14, 1600),
      (7, 'South', '2026-01-03', 18, 1900),
      (8, 'South', '2026-01-04', 25, 3000);
    `);

    // Seed Exam Scores
    await db.query(`
      INSERT INTO exam_scores (student_id, student_name, subject, score) VALUES
      (1, 'Alice', 'Math', 95),
      (2, 'Bob', 'Math', 90),
      (3, 'Charlie', 'Math', 85),
      (4, 'David', 'Math', 85),
      (5, 'Eve', 'Math', 70),
      (6, 'Alice', 'Science', 88),
      (7, 'Bob', 'Science', 92),
      (8, 'Charlie', 'Science', 78),
      (9, 'David', 'Science', 95),
      (10, 'Eve', 'Science', 82);
    `);
  });

  afterAll(async () => {
    if (db) {
      await db.close();
    }
  });

  // =========================================================================
  // Section 1: Ranking Functions (ROW_NUMBER, RANK, DENSE_RANK, NTILE) (1 - 20)
  // =========================================================================
  describe("Section 1: Ranking Functions", () => {
    test("01. ROW_NUMBER() over entire table ordered by salary DESC", async () => {
      const res = await db.query(`
        SELECT name, salary, ROW_NUMBER() OVER (ORDER BY salary DESC) AS row_num
        FROM staff_salaries
        ORDER BY salary DESC;
      `);
      expect(res.length).toBe(9);
      expect(Number(res[0].row_num)).toBe(1);
      expect(res[0].name).toBe("Alice");
      expect(Number(res[8].row_num)).toBe(9);
    });

    test("02. ROW_NUMBER() with PARTITION BY dept", async () => {
      const res = await db.query(`
        SELECT name, dept, salary, ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC) AS dept_row
        FROM staff_salaries
        ORDER BY dept ASC, salary DESC;
      `);
      expect(res.length).toBe(9);
      expect(Number(res[0].dept_row)).toBe(1);
    });

    test("03. DENSE_RANK() with tied values producing consecutive ranks", async () => {
      const res = await db.query(`
        SELECT name, score, DENSE_RANK() OVER (ORDER BY score DESC) AS drank
        FROM exam_scores
        WHERE subject = 'Math'
        ORDER BY score DESC;
      `);
      expect(res.length).toBe(5);
      expect(Number(res[0].drank)).toBe(1); // 95
      expect(Number(res[1].drank)).toBe(2); // 90
      expect(Number(res[2].drank)).toBe(3); // 85 (Charlie)
      expect(Number(res[3].drank)).toBe(3); // 85 (David)
      expect(Number(res[4].drank)).toBe(4); // 70 (Eve)
    });

    test("04. NTILE(2) splitting Engineering into 2 quartiles/halves", async () => {
      const res = await db.query(`
        SELECT name, salary, NTILE(2) OVER (ORDER BY salary DESC) AS half_bucket
        FROM staff_salaries
        WHERE dept = 'Engineering'
        ORDER BY salary DESC;
      `);
      expect(res.length).toBe(4);
      expect(Number(res[0].half_bucket)).toBe(1);
      expect(Number(res[1].half_bucket)).toBe(1);
      expect(Number(res[2].half_bucket)).toBe(2);
      expect(Number(res[3].half_bucket)).toBe(2);
    });

    test("05. NTILE(3) dividing staff into 3 tiers", async () => {
      const res = await db.query(`
        SELECT name, salary, NTILE(3) OVER (ORDER BY salary DESC) AS tier
        FROM staff_salaries
        ORDER BY salary DESC;
      `);
      expect(res.length).toBe(9);
      expect(Number(res[0].tier)).toBe(1);
      expect(Number(res[3].tier)).toBe(2);
      expect(Number(res[8].tier)).toBe(3);
    });

    test("06. PERCENT_RANK() over exam scores", async () => {
      const res = await db.query(`
        SELECT student_name, score, PERCENT_RANK() OVER (ORDER BY score ASC) AS pct_rank
        FROM exam_scores
        WHERE subject = 'Math'
        ORDER BY score ASC;
      `);
      expect(res.length).toBe(5);
      expect(Number(res[0].pct_rank)).toBe(0);
      expect(Number(res[4].pct_rank)).toBe(1);
    });

    test("07. CUME_DIST() relative cumulative distribution", async () => {
      const res = await db.query(`
        SELECT student_name, score, CUME_DIST() OVER (ORDER BY score ASC) AS c_dist
        FROM exam_scores
        WHERE subject = 'Math'
        ORDER BY score ASC;
      `);
      expect(res.length).toBe(5);
      expect(Number(res[0].c_dist)).toBe(0.2);
      expect(Number(res[4].c_dist)).toBe(1.0);
    });

    test("08. Multiple ranking functions in the same SELECT statement", async () => {
      const res = await db.query(`
        SELECT name, salary,
          ROW_NUMBER() OVER (ORDER BY salary DESC) AS rn,
          DENSE_RANK() OVER (ORDER BY salary DESC) AS dr
        FROM staff_salaries
        ORDER BY salary DESC;
      `);
      expect(res.length).toBe(9);
      expect(Number(res[0].rn)).toBe(1);
      expect(Number(res[0].dr)).toBe(1);
    });

    test("09. Subquery filtering on ROW_NUMBER() = 1 to get top earner per department", async () => {
      const res = await db.query(`
        SELECT dept, name, salary FROM (
          SELECT dept, name, salary, ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC) AS rnk
          FROM staff_salaries
        ) t WHERE rnk = 1 ORDER BY dept ASC;
      `);
      expect(res.length).toBe(3);
      expect(res[0].name).toBe("Alice");
    });

    test("10. CTE filtering on DENSE_RANK() <= 2 to get top 2 salary tiers", async () => {
      const res = await db.query(`
        WITH ranked AS (
          SELECT name, dept, salary, DENSE_RANK() OVER (PARTITION BY dept ORDER BY salary DESC) AS tier
          FROM staff_salaries
        )
        SELECT name, dept FROM ranked WHERE tier <= 2 ORDER BY dept ASC, name ASC;
      `);
      expect(res.length).toBeGreaterThanOrEqual(4);
    });

    test("11. ROW_NUMBER() with ORDER BY ASC", async () => {
      const res = await db.query(`
        SELECT name, salary, ROW_NUMBER() OVER (ORDER BY salary ASC) AS asc_rn
        FROM staff_salaries
        ORDER BY salary ASC;
      `);
      expect(Number(res[0].asc_rn)).toBe(1);
      expect(res[0].name).toBe("Ivan");
    });

    test("12. ROW_NUMBER() with multi-column ORDER BY", async () => {
      const res = await db.query(`
        SELECT name, salary, bonus, ROW_NUMBER() OVER (ORDER BY salary DESC, bonus DESC) AS seq
        FROM staff_salaries
        ORDER BY salary DESC, bonus DESC;
      `);
      expect(Number(res[0].seq)).toBe(1);
      expect(res[0].name).toBe("Alice");
    });

    test("13. NTILE(4) on daily sales data", async () => {
      const res = await db.query(`
        SELECT sale_id, revenue, NTILE(4) OVER (ORDER BY revenue ASC) AS quartile
        FROM daily_sales
        ORDER BY revenue ASC;
      `);
      expect(res.length).toBe(8);
      expect(Number(res[0].quartile)).toBe(1);
      expect(Number(res[7].quartile)).toBe(4);
    });

    test("14. Ranking over partitioned exam scores across subjects", async () => {
      const res = await db.query(`
        SELECT student_name, subject, score, ROW_NUMBER() OVER (PARTITION BY subject ORDER BY score DESC) AS sub_rank
        FROM exam_scores
        ORDER BY subject ASC, score DESC;
      `);
      expect(res.length).toBe(10);
      expect(Number(res[0].sub_rank)).toBe(1);
      expect(res[0].student_name).toBe("Alice");
    });

    test("15. Top scorer per subject using window rank in CTE", async () => {
      const res = await db.query(`
        WITH top_sub AS (
          SELECT student_name, subject, score, ROW_NUMBER() OVER (PARTITION BY subject ORDER BY score DESC) AS rnk
          FROM exam_scores
        )
        SELECT subject, student_name, score FROM top_sub WHERE rnk = 1 ORDER BY subject ASC;
      `);
      expect(res.length).toBe(2);
      expect(res[0].subject).toBe("Math");
      expect(res[0].student_name).toBe("Alice");
      expect(res[1].subject).toBe("Science");
      expect(res[1].student_name).toBe("David");
    });

    test("16. Bottom performer per department via ASC order in window", async () => {
      const res = await db.query(`
        WITH min_earner AS (
          SELECT name, dept, salary, ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary ASC) AS rnk
          FROM staff_salaries
        )
        SELECT dept, name, salary FROM min_earner WHERE rnk = 1 ORDER BY dept ASC;
      `);
      expect(res.length).toBe(3);
      expect(res[0].dept).toBe("Engineering");
    });

    test("17. DENSE_RANK() with no ties produces sequential numbers", async () => {
      const res = await db.query(`
        SELECT sale_id, revenue, DENSE_RANK() OVER (ORDER BY revenue DESC) AS rnk
        FROM daily_sales
        WHERE region = 'North'
        ORDER BY revenue DESC;
      `);
      expect(res.length).toBe(4);
      expect(Number(res[0].rnk)).toBe(1);
      expect(Number(res[3].rnk)).toBe(4);
    });

    test("18. ROW_NUMBER() partitioned by region on daily sales", async () => {
      const res = await db.query(`
        SELECT region, sale_date, revenue, ROW_NUMBER() OVER (PARTITION BY region ORDER BY sale_date ASC) AS day_num
        FROM daily_sales
        ORDER BY region ASC, sale_date ASC;
      `);
      expect(res.length).toBe(8);
      expect(Number(res[0].day_num)).toBe(1);
      expect(Number(res[3].day_num)).toBe(4);
    });

    test("19. NTILE(1) assigns all rows to bucket 1", async () => {
      const res = await db.query(`
        SELECT name, NTILE(1) OVER (ORDER BY salary DESC) AS b
        FROM staff_salaries;
      `);
      expect(res.every(r => Number(r.b) === 1)).toBe(true);
    });

    test("20. PERCENT_RANK() with single row partition returns 0", async () => {
      const res = await db.query(`
        SELECT name, PERCENT_RANK() OVER (ORDER BY salary DESC) AS pr
        FROM staff_salaries
        WHERE name = 'Alice';
      `);
      expect(Number(res[0].pr)).toBe(0);
    });
  });

  // =========================================================================
  // Section 2: Value Navigation Functions (LEAD, LAG, FIRST_VALUE, LAST_VALUE, NTH_VALUE) (21 - 40)
  // =========================================================================
  describe("Section 2: Value Navigation Functions", () => {
    test("21. LAG(revenue) to compute day-over-day previous day revenue", async () => {
      const res = await db.query(`
        SELECT region, sale_date, revenue, LAG(revenue) OVER (PARTITION BY region ORDER BY sale_date ASC) AS prev_rev
        FROM daily_sales
        WHERE region = 'North'
        ORDER BY sale_date ASC;
      `);
      expect(res.length).toBe(4);
      expect(res[0].prev_rev).toBeNull();
      expect(Number(res[1].prev_rev)).toBe(1000);
      expect(Number(res[2].prev_rev)).toBe(1500);
    });

    test("22. LEAD(revenue) to peek into next day revenue", async () => {
      const res = await db.query(`
        SELECT region, sale_date, revenue, LEAD(revenue) OVER (PARTITION BY region ORDER BY sale_date ASC) AS next_rev
        FROM daily_sales
        WHERE region = 'North'
        ORDER BY sale_date ASC;
      `);
      expect(res.length).toBe(4);
      expect(Number(res[0].next_rev)).toBe(1500);
      expect(res[3].next_rev).toBeNull();
    });

    test("23. FIRST_VALUE(salary) per department", async () => {
      const res = await db.query(`
        SELECT name, dept, salary, FIRST_VALUE(salary) OVER (PARTITION BY dept ORDER BY salary DESC) AS top_dept_salary
        FROM staff_salaries
        ORDER BY dept ASC, salary DESC;
      `);
      expect(res.length).toBe(9);
      expect(Number(res[0].top_dept_salary)).toBe(150000);
    });

    test("24. LAST_VALUE(salary) per department", async () => {
      const res = await db.query(`
        SELECT name, dept, salary, LAST_VALUE(salary) OVER (PARTITION BY dept ORDER BY salary DESC) AS min_dept_salary
        FROM staff_salaries
        ORDER BY dept ASC, salary DESC;
      `);
      expect(res.length).toBe(9);
      expect(Number(res[0].min_dept_salary)).toBe(80000);
    });

    test("25. NTH_VALUE(salary, 2) getting 2nd highest salary per department", async () => {
      const res = await db.query(`
        SELECT name, dept, salary, NTH_VALUE(salary, 2) OVER (PARTITION BY dept ORDER BY salary DESC) AS second_sal
        FROM staff_salaries
        WHERE dept = 'Engineering'
        ORDER BY salary DESC;
      `);
      expect(res.length).toBe(4);
      expect(Number(res[0].second_sal)).toBe(120000);
    });

    test("26. Revenue difference from previous day using LAG in CTE", async () => {
      const res = await db.query(`
        WITH dod AS (
          SELECT region, sale_date, revenue, LAG(revenue) OVER (PARTITION BY region ORDER BY sale_date ASC) AS prev_rev
          FROM daily_sales
          WHERE region = 'North'
        )
        SELECT sale_date, revenue, (revenue - prev_rev) AS diff
        FROM dod
        WHERE prev_rev IS NOT NULL
        ORDER BY sale_date ASC;
      `);
      expect(res.length).toBe(3);
      expect(Number(res[0].diff)).toBe(500); // 1500 - 1000
      expect(Number(res[1].diff)).toBe(700); // 2200 - 1500
    });

    test("27. LAG with offset 2 checking 2-day prior revenue", async () => {
      const res = await db.query(`
        SELECT sale_date, revenue, LAG(revenue, 2) OVER (PARTITION BY region ORDER BY sale_date ASC) AS rev_2d_ago
        FROM daily_sales
        WHERE region = 'North'
        ORDER BY sale_date ASC;
      `);
      expect(res.length).toBe(4);
      expect(res[0].rev_2d_ago).toBeNull();
      expect(res[1].rev_2d_ago).toBeNull();
      expect(Number(res[2].rev_2d_ago)).toBe(1000);
    });

    test("28. LEAD with offset 2 checking 2-day future revenue", async () => {
      const res = await db.query(`
        SELECT sale_date, revenue, LEAD(revenue, 2) OVER (PARTITION BY region ORDER BY sale_date ASC) AS rev_2d_next
        FROM daily_sales
        WHERE region = 'North'
        ORDER BY sale_date ASC;
      `);
      expect(res.length).toBe(4);
      expect(Number(res[0].rev_2d_next)).toBe(2200);
      expect(res[2].rev_2d_next).toBeNull();
    });

    test("29. FIRST_VALUE() string column (top earner name per dept)", async () => {
      const res = await db.query(`
        SELECT dept, name, FIRST_VALUE(name) OVER (PARTITION BY dept ORDER BY salary DESC) AS top_earner_name
        FROM staff_salaries
        ORDER BY dept ASC;
      `);
      expect(res[0].top_earner_name).toBe("Alice");
    });

    test("30. LAST_VALUE() string column (lowest earner name in department)", async () => {
      const res = await db.query(`
        SELECT dept, name, LAST_VALUE(name) OVER (PARTITION BY dept ORDER BY salary DESC) AS lowest_earner_name
        FROM staff_salaries
        WHERE dept = 'Engineering'
        ORDER BY salary DESC;
      `);
      expect(res.length).toBe(4);
      expect(res[0].lowest_earner_name).toBe("David");
    });

    test("31. Comparing current salary to department leader salary via FIRST_VALUE", async () => {
      const res = await db.query(`
        WITH w AS (
          SELECT name, salary, FIRST_VALUE(salary) OVER (PARTITION BY dept ORDER BY salary DESC) AS top_sal
          FROM staff_salaries
          WHERE dept = 'Engineering'
        )
        SELECT name, salary, (top_sal - salary) AS gap_from_lead
        FROM w
        ORDER BY salary DESC;
      `);
      expect(Number(res[0].gap_from_lead)).toBe(0);
      expect(Number(res[1].gap_from_lead)).toBe(30000); // 150k - 120k
    });

    test("32. NTH_VALUE out of range returns NULL", async () => {
      const res = await db.query(`
        SELECT name, NTH_VALUE(salary, 99) OVER (ORDER BY salary DESC) AS missing
        FROM staff_salaries;
      `);
      expect(res.every(r => r.missing === null)).toBe(true);
    });

    test("33. LAG across daily sales across all regions", async () => {
      const res = await db.query(`
        SELECT region, sale_date, revenue, LAG(revenue) OVER (PARTITION BY region ORDER BY sale_date ASC) AS prev_rev
        FROM daily_sales
        ORDER BY region ASC, sale_date ASC;
      `);
      expect(res.length).toBe(8);
      expect(res[0].prev_rev).toBeNull();
      expect(res[4].prev_rev).toBeNull();
    });

    test("34. Combining LEAD and LAG in the same row", async () => {
      const res = await db.query(`
        SELECT sale_date,
          LAG(revenue) OVER (ORDER BY sale_date ASC) AS prev_v,
          revenue,
          LEAD(revenue) OVER (ORDER BY sale_date ASC) AS next_v
        FROM daily_sales
        WHERE region = 'North'
        ORDER BY sale_date ASC;
      `);
      expect(res.length).toBe(4);
      expect(res[1].prev_v).not.toBeNull();
      expect(res[1].next_v).not.toBeNull();
    });

    test("35. FIRST_VALUE with ORDER BY ASC", async () => {
      const res = await db.query(`
        SELECT name, salary, FIRST_VALUE(salary) OVER (ORDER BY salary ASC) AS lowest_salary_ever
        FROM staff_salaries
        ORDER BY salary ASC;
      `);
      expect(Number(res[0].lowest_salary_ever)).toBe(75000);
    });

    test("36. LAG on text string column", async () => {
      const res = await db.query(`
        SELECT student_name, score, LAG(student_name) OVER (ORDER BY score DESC) AS prev_student
        FROM exam_scores
        WHERE subject = 'Math'
        ORDER BY score DESC;
      `);
      expect(res[0].prev_student).toBeNull();
      expect(res[1].prev_student).toBe("Alice");
    });

    test("37. LEAD on text string column", async () => {
      const res = await db.query(`
        SELECT student_name, score, LEAD(student_name) OVER (ORDER BY score DESC) AS next_student
        FROM exam_scores
        WHERE subject = 'Math'
        ORDER BY score DESC;
      `);
      expect(res[0].next_student).toBe("Bob");
    });

    test("38. Growth rate calculation using LAG", async () => {
      const res = await db.query(`
        WITH calc AS (
          SELECT sale_date, revenue, LAG(revenue) OVER (ORDER BY sale_date ASC) AS prev_r
          FROM daily_sales
          WHERE region = 'South'
        )
        SELECT sale_date, (revenue * 100.0 / prev_r) AS growth_index
        FROM calc
        WHERE prev_r IS NOT NULL
        ORDER BY sale_date ASC;
      `);
      expect(Number(res[0].growth_index)).toBe(200); // 1600 / 800 * 100
    });

    test("39. FIRST_VALUE on exam scores", async () => {
      const res = await db.query(`
        SELECT student_name, score, FIRST_VALUE(score) OVER (PARTITION BY subject ORDER BY score DESC) AS best_score
        FROM exam_scores
        WHERE subject = 'Science'
        ORDER BY score DESC;
      `);
      expect(Number(res[0].best_score)).toBe(95);
    });

    test("40. NTH_VALUE(score, 1) equals FIRST_VALUE(score)", async () => {
      const res = await db.query(`
        SELECT student_name,
          FIRST_VALUE(score) OVER (PARTITION BY subject ORDER BY score DESC) AS fv,
          NTH_VALUE(score, 1) OVER (PARTITION BY subject ORDER BY score DESC) AS nv
        FROM exam_scores
        WHERE subject = 'Math';
      `);
      expect(Number(res[0].fv)).toBe(Number(res[0].nv));
    });
  });

  // =========================================================================
  // Section 3: Cumulative & Window Aggregates (SUM, AVG, MIN, MAX, COUNT) (41 - 65)
  // =========================================================================
  describe("Section 3: Cumulative & Window Aggregates", () => {
    test("41. Cumulative SUM(revenue) running total", async () => {
      const res = await db.query(`
        SELECT region, sale_date, revenue, SUM(revenue) OVER (PARTITION BY region ORDER BY sale_date ASC) AS running_rev
        FROM daily_sales
        WHERE region = 'North'
        ORDER BY sale_date ASC;
      `);
      expect(res.length).toBe(4);
      expect(Number(res[0].running_rev)).toBe(1000);
      expect(Number(res[1].running_rev)).toBe(2500); // 1000 + 1500
      expect(Number(res[2].running_rev)).toBe(4700); // 2500 + 2200
      expect(Number(res[3].running_rev)).toBe(6000); // 4700 + 1300
    });

    test("42. Running AVG(revenue) cumulative moving average", async () => {
      const res = await db.query(`
        SELECT region, sale_date, revenue, AVG(revenue) OVER (PARTITION BY region ORDER BY sale_date ASC) AS running_avg
        FROM daily_sales
        WHERE region = 'North'
        ORDER BY sale_date ASC;
      `);
      expect(res.length).toBe(4);
      expect(Number(res[0].running_avg)).toBe(1000);
      expect(Number(res[1].running_avg)).toBe(1250); // (1000 + 1500) / 2
    });

    test("43. Running MIN(revenue) tracking minimum seen so far", async () => {
      const res = await db.query(`
        SELECT sale_date, revenue, MIN(revenue) OVER (ORDER BY sale_date ASC) AS min_so_far
        FROM daily_sales
        WHERE region = 'North'
        ORDER BY sale_date ASC;
      `);
      expect(res.length).toBe(4);
      expect(Number(res[0].min_so_far)).toBe(1000);
      expect(Number(res[3].min_so_far)).toBe(1000);
    });

    test("44. Running MAX(revenue) tracking maximum seen so far", async () => {
      const res = await db.query(`
        SELECT sale_date, revenue, MAX(revenue) OVER (ORDER BY sale_date ASC) AS max_so_far
        FROM daily_sales
        WHERE region = 'North'
        ORDER BY sale_date ASC;
      `);
      expect(res.length).toBe(4);
      expect(Number(res[0].max_so_far)).toBe(1000);
      expect(Number(res[2].max_so_far)).toBe(2200);
      expect(Number(res[3].max_so_far)).toBe(2200);
    });

    test("45. Running COUNT(*) cumulative row counter", async () => {
      const res = await db.query(`
        SELECT name, salary, COUNT(*) OVER (ORDER BY salary DESC) AS cum_count
        FROM staff_salaries
        ORDER BY salary DESC;
      `);
      expect(res.length).toBe(9);
      expect(Number(res[0].cum_count)).toBe(1);
      expect(Number(res[8].cum_count)).toBe(9);
    });

    test("46. Cumulative SUM of units sold per region", async () => {
      const res = await db.query(`
        SELECT region, sale_date, units, SUM(units) OVER (PARTITION BY region ORDER BY sale_date ASC) AS total_units_so_far
        FROM daily_sales
        WHERE region = 'South'
        ORDER BY sale_date ASC;
      `);
      expect(res.length).toBe(4);
      expect(Number(res[0].total_units_so_far)).toBe(8);
      expect(Number(res[3].total_units_so_far)).toBe(65); // 8 + 14 + 18 + 25
    });

    test("47. Running payroll total per department", async () => {
      const res = await db.query(`
        SELECT dept, name, salary, SUM(salary) OVER (PARTITION BY dept ORDER BY salary DESC) AS running_payroll
        FROM staff_salaries
        WHERE dept = 'Engineering'
        ORDER BY salary DESC;
      `);
      expect(res.length).toBe(4);
      expect(Number(res[0].running_payroll)).toBe(150000);
      expect(Number(res[3].running_payroll)).toBe(430000);
    });

    test("48. Running AVG of exam scores", async () => {
      const res = await db.query(`
        SELECT student_name, score, AVG(score) OVER (ORDER BY score DESC) AS avg_score_cum
        FROM exam_scores
        WHERE subject = 'Math'
        ORDER BY score DESC;
      `);
      expect(res.length).toBe(5);
      expect(Number(res[0].avg_score_cum)).toBe(95);
    });

    test("49. Grand total comparison: current revenue vs running sum in CTE", async () => {
      const res = await db.query(`
        WITH r AS (
          SELECT sale_date, revenue, SUM(revenue) OVER (ORDER BY sale_date ASC) AS cum_rev
          FROM daily_sales
          WHERE region = 'North'
        )
        SELECT sale_date, revenue, cum_rev FROM r WHERE cum_rev >= 4000 ORDER BY sale_date ASC;
      `);
      expect(res.length).toBe(2);
      expect(res[0].sale_date).toBe("2026-01-03");
    });

    test("50. Multiple window aggregates in a single SELECT (SUM, AVG, MIN, MAX)", async () => {
      const res = await db.query(`
        SELECT sale_date, revenue,
          SUM(revenue) OVER (ORDER BY sale_date ASC) AS s,
          AVG(revenue) OVER (ORDER BY sale_date ASC) AS a,
          MIN(revenue) OVER (ORDER BY sale_date ASC) AS mn,
          MAX(revenue) OVER (ORDER BY sale_date ASC) AS mx
        FROM daily_sales
        WHERE region = 'North'
        ORDER BY sale_date ASC;
      `);
      expect(res.length).toBe(4);
      expect(res[0].s).not.toBeNull();
      expect(res[0].a).not.toBeNull();
      expect(res[0].mn).not.toBeNull();
      expect(res[0].mx).not.toBeNull();
    });

    test("51. Running sum with ties in ORDER BY", async () => {
      const res = await db.query(`
        SELECT student_name, score, SUM(score) OVER (ORDER BY score DESC) AS cum_score
        FROM exam_scores
        WHERE subject = 'Math'
        ORDER BY score DESC;
      `);
      expect(res.length).toBe(5);
      expect(Number(res[0].cum_score)).toBe(95);
    });

    test("52. Window aggregate over entire table without PARTITION BY", async () => {
      const res = await db.query(`
        SELECT name, salary, SUM(salary) OVER (ORDER BY salary ASC) AS running_total
        FROM staff_salaries
        ORDER BY salary ASC;
      `);
      expect(res.length).toBe(9);
      expect(Number(res[0].running_total)).toBe(75000);
    });

    test("53. Running MIN on salary in Sales department", async () => {
      const res = await db.query(`
        SELECT name, salary, MIN(salary) OVER (ORDER BY salary DESC) AS min_so_far
        FROM staff_salaries
        WHERE dept = 'Sales'
        ORDER BY salary DESC;
      `);
      expect(res.length).toBe(3);
      expect(Number(res[0].min_so_far)).toBe(130000);
      expect(Number(res[2].min_so_far)).toBe(90000);
    });

    test("54. Running MAX on bonus in Engineering department", async () => {
      const res = await db.query(`
        SELECT name, bonus, MAX(bonus) OVER (ORDER BY bonus ASC) AS peak_bonus
        FROM staff_salaries
        WHERE dept = 'Engineering'
        ORDER BY bonus ASC;
      `);
      expect(res.length).toBe(4);
      expect(Number(res[0].peak_bonus)).toBe(4000);
      expect(Number(res[3].peak_bonus)).toBe(15000);
    });

    test("55. Window count per department", async () => {
      const res = await db.query(`
        SELECT name, dept, COUNT(*) OVER (PARTITION BY dept ORDER BY salary DESC) AS staff_seq
        FROM staff_salaries
        WHERE dept = 'Sales'
        ORDER BY salary DESC;
      `);
      expect(res.length).toBe(3);
      expect(Number(res[2].staff_seq)).toBe(3);
    });

    test("56. Percentage of total revenue accumulated so far in CTE", async () => {
      const res = await db.query(`
        WITH running AS (
          SELECT sale_date, revenue, SUM(revenue) OVER (ORDER BY sale_date ASC) AS cum_r
          FROM daily_sales
          WHERE region = 'North'
        )
        SELECT sale_date, (cum_r * 100.0 / 6000.0) AS pct_achieved FROM running ORDER BY sale_date ASC;
      `);
      expect(res.length).toBe(4);
      expect(Number(res[3].pct_achieved)).toBe(100);
    });

    test("57. Running average on exam scores for Science", async () => {
      const res = await db.query(`
        SELECT student_name, score, AVG(score) OVER (ORDER BY score DESC) AS avg_sc
        FROM exam_scores
        WHERE subject = 'Science'
        ORDER BY score DESC;
      `);
      expect(res.length).toBe(5);
      expect(Number(res[0].avg_sc)).toBe(95);
    });

    test("58. Window sum on bonus", async () => {
      const res = await db.query(`
        SELECT name, bonus, SUM(bonus) OVER (ORDER BY bonus DESC) AS total_bonus_dist
        FROM staff_salaries
        WHERE dept = 'Marketing'
        ORDER BY bonus DESC;
      `);
      expect(res.length).toBe(2);
      expect(Number(res[1].total_bonus_dist)).toBe(12000); // 8000 + 4000
    });

    test("59. Running minimum units sold", async () => {
      const res = await db.query(`
        SELECT sale_date, units, MIN(units) OVER (ORDER BY sale_date ASC) AS min_u
        FROM daily_sales
        WHERE region = 'South'
        ORDER BY sale_date ASC;
      `);
      expect(Number(res[0].min_u)).toBe(8);
      expect(Number(res[3].min_u)).toBe(8);
    });

    test("60. Running maximum units sold", async () => {
      const res = await db.query(`
        SELECT sale_date, units, MAX(units) OVER (ORDER BY sale_date ASC) AS max_u
        FROM daily_sales
        WHERE region = 'South'
        ORDER BY sale_date ASC;
      `);
      expect(Number(res[0].max_u)).toBe(8);
      expect(Number(res[3].max_u)).toBe(25);
    });

    test("61. Cumulative sum with ORDER BY DESC", async () => {
      const res = await db.query(`
        SELECT sale_date, revenue, SUM(revenue) OVER (ORDER BY revenue DESC) AS rev_desc_cum
        FROM daily_sales
        WHERE region = 'North'
        ORDER BY revenue DESC;
      `);
      expect(res.length).toBe(4);
      expect(Number(res[0].rev_desc_cum)).toBe(2200);
    });

    test("62. Window sum on exam scores", async () => {
      const res = await db.query(`
        SELECT student_name, score, SUM(score) OVER (ORDER BY student_name ASC) AS cum_math_score
        FROM exam_scores
        WHERE subject = 'Math'
        ORDER BY student_name ASC;
      `);
      expect(res.length).toBe(5);
      expect(Number(res[0].cum_math_score)).toBe(95);
    });

    test("63. Cumulative average across multiple regions in daily sales", async () => {
      const res = await db.query(`
        SELECT region, sale_date, revenue, AVG(revenue) OVER (PARTITION BY region ORDER BY sale_date ASC) AS reg_avg
        FROM daily_sales
        ORDER BY region ASC, sale_date ASC;
      `);
      expect(res.length).toBe(8);
      expect(Number(res[0].reg_avg)).toBe(1000);
      expect(Number(res[4].reg_avg)).toBe(800);
    });

    test("64. Running total of staff salaries ordered by hire year", async () => {
      const res = await db.query(`
        SELECT name, hire_year, salary, SUM(salary) OVER (ORDER BY hire_year ASC, id ASC) AS payroll_history
        FROM staff_salaries
        ORDER BY hire_year ASC, id ASC;
      `);
      expect(res.length).toBe(9);
      expect(Number(res[0].payroll_history)).toBe(130000);
    });

    test("65. Cumulative staff count over hiring timeline", async () => {
      const res = await db.query(`
        SELECT name, hire_year, COUNT(*) OVER (ORDER BY hire_year ASC, id ASC) AS headcount_growth
        FROM staff_salaries
        ORDER BY hire_year ASC, id ASC;
      `);
      expect(res.length).toBe(9);
      expect(Number(res[8].headcount_growth)).toBe(9);
    });
  });

  // =========================================================================
  // Section 4: Multi-Column Partitioning & Sorting (66 - 80)
  // =========================================================================
  describe("Section 4: Multi-Column Partitioning & Sorting", () => {
    test("66. Partition by multiple columns PARTITION BY dept, role", async () => {
      const res = await db.query(`
        SELECT dept, role, name, salary, ROW_NUMBER() OVER (PARTITION BY dept, role ORDER BY salary DESC) AS role_rank
        FROM staff_salaries
        ORDER BY dept ASC, role ASC, salary DESC;
      `);
      expect(res.length).toBe(9);
      expect(Number(res[0].role_rank)).toBe(1);
    });

    test("67. Partition by dept with multi-column ORDER BY (salary DESC, hire_year ASC)", async () => {
      const res = await db.query(`
        SELECT dept, name, salary, hire_year, ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC, hire_year ASC) AS rnk
        FROM staff_salaries
        WHERE dept = 'Engineering'
        ORDER BY salary DESC, hire_year ASC;
      `);
      expect(res.length).toBe(4);
      expect(res[0].name).toBe("Alice");
      expect(res[2].name).toBe("Charlie"); // 80k in 2022 comes before 80k in 2023
      expect(res[3].name).toBe("David");
    });

    test("68. DENSE_RANK() partitioned by subject", async () => {
      const res = await db.query(`
        SELECT subject, student_name, score, DENSE_RANK() OVER (PARTITION BY subject ORDER BY score DESC) AS drank
        FROM exam_scores
        ORDER BY subject ASC, score DESC;
      `);
      expect(res.length).toBe(10);
      expect(Number(res[0].drank)).toBe(1);
    });

    test("69. NTILE(2) partitioned by region in sales", async () => {
      const res = await db.query(`
        SELECT region, sale_date, revenue, NTILE(2) OVER (PARTITION BY region ORDER BY revenue DESC) AS tier
        FROM daily_sales
        ORDER BY region ASC, revenue DESC;
      `);
      expect(res.length).toBe(8);
      expect(Number(res[0].tier)).toBe(1);
      expect(Number(res[2].tier)).toBe(2);
    });

    test("70. LAG partitioned by dept and role", async () => {
      const res = await db.query(`
        SELECT dept, role, name, salary, LAG(salary) OVER (PARTITION BY dept, role ORDER BY hire_year ASC) AS prev_hired_sal
        FROM staff_salaries
        WHERE dept = 'Engineering' AND role = 'Junior'
        ORDER BY hire_year ASC;
      `);
      expect(res.length).toBe(2);
      expect(res[0].prev_hired_sal).toBeNull();
      expect(Number(res[1].prev_hired_sal)).toBe(80000);
    });

    test("71. LEAD partitioned by dept", async () => {
      const res = await db.query(`
        SELECT dept, name, salary, LEAD(salary) OVER (PARTITION BY dept ORDER BY salary DESC) AS next_sal
        FROM staff_salaries
        WHERE dept = 'Sales'
        ORDER BY salary DESC;
      `);
      expect(res.length).toBe(3);
      expect(Number(res[0].next_sal)).toBe(90000);
      expect(Number(res[1].next_sal)).toBe(90000);
      expect(res[2].next_sal).toBeNull();
    });

    test("72. FIRST_VALUE partitioned by region", async () => {
      const res = await db.query(`
        SELECT region, sale_date, revenue, FIRST_VALUE(revenue) OVER (PARTITION BY region ORDER BY sale_date ASC) AS start_rev
        FROM daily_sales
        ORDER BY region ASC, sale_date ASC;
      `);
      expect(Number(res[0].start_rev)).toBe(1000);
      expect(Number(res[4].start_rev)).toBe(800);
    });

    test("73. LAST_VALUE partitioned by region", async () => {
      const res = await db.query(`
        SELECT region, sale_date, revenue, LAST_VALUE(revenue) OVER (PARTITION BY region ORDER BY sale_date ASC) AS end_rev
        FROM daily_sales
        ORDER BY region ASC, sale_date ASC;
      `);
      expect(Number(res[0].end_rev)).toBe(1300);
      expect(Number(res[4].end_rev)).toBe(3000);
    });

    test("74. Running sum partitioned by subject on exam scores", async () => {
      const res = await db.query(`
        SELECT subject, student_name, score, SUM(score) OVER (PARTITION BY subject ORDER BY score DESC) AS sub_cum
        FROM exam_scores
        ORDER BY subject ASC, score DESC;
      `);
      expect(res.length).toBe(10);
      expect(Number(res[4].sub_cum)).toBe(425);
    });

    test("75. Running average partitioned by subject", async () => {
      const res = await db.query(`
        SELECT subject, student_name, score, AVG(score) OVER (PARTITION BY subject ORDER BY score DESC) AS sub_avg
        FROM exam_scores
        WHERE subject = 'Math'
        ORDER BY score DESC;
      `);
      expect(Number(res[0].sub_avg)).toBe(95);
    });

    test("76. Percentage rank partitioned by department", async () => {
      const res = await db.query(`
        SELECT dept, name, salary, PERCENT_RANK() OVER (PARTITION BY dept ORDER BY salary ASC) AS p_rank
        FROM staff_salaries
        WHERE dept = 'Sales'
        ORDER BY salary ASC;
      `);
      expect(res.length).toBe(3);
      expect(Number(res[0].p_rank)).toBe(0);
      expect(Number(res[2].p_rank)).toBe(1);
    });

    test("77. Cumulative distribution partitioned by department", async () => {
      const res = await db.query(`
        SELECT dept, name, salary, CUME_DIST() OVER (PARTITION BY dept ORDER BY salary ASC) AS c_dist
        FROM staff_salaries
        WHERE dept = 'Engineering'
        ORDER BY salary ASC;
      `);
      expect(res.length).toBe(4);
      expect(Number(res[0].c_dist)).toBe(0.25);
      expect(Number(res[3].c_dist)).toBe(1.0);
    });

    test("78. Multiple partitions in different expressions within the same SELECT", async () => {
      const res = await db.query(`
        SELECT name, dept, role, salary,
          ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC) AS dept_rn,
          ROW_NUMBER() OVER (PARTITION BY role ORDER BY salary DESC) AS role_rn
        FROM staff_salaries
        ORDER BY salary DESC;
      `);
      expect(res.length).toBe(9);
      expect(res[0].dept_rn).not.toBeNull();
      expect(res[0].role_rn).not.toBeNull();
    });

    test("79. Ordering by alias created by window function in outer query", async () => {
      const res = await db.query(`
        SELECT * FROM (
          SELECT name, dept, salary, ROW_NUMBER() OVER (ORDER BY salary DESC) AS rnk
          FROM staff_salaries
        ) t ORDER BY rnk ASC LIMIT 3;
      `);
      expect(res.length).toBe(3);
      expect(res[0].name).toBe("Alice");
    });

    test("80. Partitioning with non-string integer column (hire_year)", async () => {
      const res = await db.query(`
        SELECT hire_year, name, salary, ROW_NUMBER() OVER (PARTITION BY hire_year ORDER BY salary DESC) AS year_rn
        FROM staff_salaries
        ORDER BY hire_year ASC, salary DESC;
      `);
      expect(res.length).toBe(9);
      expect(Number(res[0].year_rn)).toBe(1);
    });
  });

  // =========================================================================
  // Section 5: Window Functions with CTEs, Subqueries & Filtering (81 - 100)
  // =========================================================================
  describe("Section 5: Window Functions with CTEs, Subqueries & Filtering", () => {
    test("81. Top 1 salary in each department via CTE", async () => {
      const res = await db.query(`
        WITH dept_ranked AS (
          SELECT dept, name, salary, ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC) AS rnk
          FROM staff_salaries
        )
        SELECT dept, name, salary FROM dept_ranked WHERE rnk = 1 ORDER BY dept ASC;
      `);
      expect(res.length).toBe(3);
      expect(res[0].dept).toBe("Engineering");
      expect(res[0].name).toBe("Alice");
      expect(res[1].dept).toBe("Marketing");
      expect(res[1].name).toBe("Heidi");
      expect(res[2].dept).toBe("Sales");
      expect(res[2].name).toBe("Eve");
    });

    test("82. Second highest salary across company via CTE", async () => {
      const res = await db.query(`
        WITH comp_ranked AS (
          SELECT name, salary, DENSE_RANK() OVER (ORDER BY salary DESC) AS rnk
          FROM staff_salaries
        )
        SELECT name, salary FROM comp_ranked WHERE rnk = 2;
      `);
      expect(res.length).toBe(1);
      expect(res[0].name).toBe("Eve");
      expect(Number(res[0].salary)).toBe(130000);
    });

    test("83. Days where revenue grew by more than 30% vs prior day via CTE", async () => {
      const res = await db.query(`
        WITH lag_sales AS (
          SELECT sale_date, region, revenue, LAG(revenue) OVER (PARTITION BY region ORDER BY sale_date ASC) AS prev_r
          FROM daily_sales
        ),
        calc AS (
          SELECT sale_date, region, revenue, prev_r, (revenue * 1.0 / prev_r) AS growth
          FROM lag_sales
          WHERE prev_r IS NOT NULL
        )
        SELECT * FROM calc WHERE growth >= 1.3 ORDER BY sale_date ASC;
      `);
      expect(res.length).toBeGreaterThanOrEqual(1);
    });

    test("84. Window function result filtered by WHERE in derived table", async () => {
      const res = await db.query(`
        SELECT name, score FROM (
          SELECT student_name AS name, score, ROW_NUMBER() OVER (ORDER BY score DESC) AS rnk
          FROM exam_scores
          WHERE subject = 'Math'
        ) t WHERE rnk <= 3 ORDER BY score DESC;
      `);
      expect(res.length).toBe(3);
      expect(res[0].name).toBe("Alice");
    });

    test("85. Window function inside a CTE joined to another table", async () => {
      const res = await db.query(`
        WITH top_staff AS (
          SELECT id, name, dept, ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC) AS rnk
          FROM staff_salaries
        )
        SELECT t.name, t.dept, s.salary
        FROM top_staff t
        JOIN staff_salaries s ON t.id = s.id
        WHERE t.rnk = 1
        ORDER BY t.dept ASC;
      `);
      expect(res.length).toBe(3);
      expect(res[0].name).toBe("Alice");
    });

    test("86. Aggregating over window function results (average of top salaries)", async () => {
      const res = await db.query(`
        WITH tops AS (
          SELECT dept, salary, ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC) AS rnk
          FROM staff_salaries
        )
        SELECT avg(salary) AS avg_top_sal FROM tops WHERE rnk = 1;
      `);
      expect(Number(res[0].avg_top_sal)).toBe(130000); // (150k + 110k + 130k) / 3
    });

    test("87. Finding students who improved score in Science vs Math via LAG/LEAD", async () => {
      const res = await db.query(`
        WITH combined AS (
          SELECT student_name, subject, score, LAG(score) OVER (PARTITION BY student_name ORDER BY subject ASC) AS prev_score
          FROM exam_scores
        ),
        diff AS (
          SELECT student_name, subject, score AS science_score, prev_score AS math_score, (score - prev_score) AS score_diff
          FROM combined
          WHERE subject = 'Science'
        )
        SELECT student_name, science_score, math_score
        FROM diff
        WHERE score_diff > 0
        ORDER BY student_name ASC;
      `);
      expect(res.length).toBeGreaterThanOrEqual(2);
      expect(res.map(r => r.student_name)).toContain("Bob");
      expect(res.map(r => r.student_name)).toContain("David");
    });

    test("88. Window function with COUNT DISTINCT emulation via DENSE_RANK", async () => {
      const res = await db.query(`
        SELECT name, dept, DENSE_RANK() OVER (ORDER BY dept ASC) AS dept_num
        FROM staff_salaries
        ORDER BY dept ASC;
      `);
      expect(res.length).toBe(9);
      expect(Number(res[0].dept_num)).toBe(1);
    });

    test("89. Window function combined with GROUP BY in CTE", async () => {
      const res = await db.query(`
        WITH dept_sums AS (
          SELECT dept, sum(salary) AS total_payroll
          FROM staff_salaries
          GROUP BY dept
        )
        SELECT dept, total_payroll, ROW_NUMBER() OVER (ORDER BY total_payroll DESC) AS payroll_rank
        FROM dept_sums
        ORDER BY total_payroll DESC;
      `);
      expect(res.length).toBe(3);
      expect(res[0].dept).toBe("Engineering");
      expect(Number(res[0].payroll_rank)).toBe(1);
    });

    test("90. Cumulative sum over GROUP BY totals in CTE", async () => {
      const res = await db.query(`
        WITH daily_totals AS (
          SELECT sale_date, sum(revenue) AS day_revenue
          FROM daily_sales
          GROUP BY sale_date
        )
        SELECT sale_date, day_revenue, SUM(day_revenue) OVER (ORDER BY sale_date ASC) AS cum_all_regions
        FROM daily_totals
        ORDER BY sale_date ASC;
      `);
      expect(res.length).toBe(4);
      expect(Number(res[0].cum_all_regions)).toBe(1800); // 1000 + 800
      expect(Number(res[3].cum_all_regions)).toBe(13300);
    });

    test("91. Multiple CTEs feeding into window function", async () => {
      const res = await db.query(`
        WITH
          eng AS (SELECT name, salary FROM staff_salaries WHERE dept = 'Engineering'),
          sales AS (SELECT name, salary FROM staff_salaries WHERE dept = 'Sales'),
          combined AS (SELECT * FROM eng UNION ALL SELECT * FROM sales)
        SELECT name, salary, ROW_NUMBER() OVER (ORDER BY salary DESC) AS rnk
        FROM combined
        ORDER BY salary DESC;
      `);
      expect(res.length).toBe(7);
      expect(res[0].name).toBe("Alice");
    });

    test("92. Window function with LIMIT and OFFSET on outer query", async () => {
      const res = await db.query(`
        SELECT name, salary, ROW_NUMBER() OVER (ORDER BY salary DESC) AS rnk
        FROM staff_salaries
        ORDER BY salary DESC
        LIMIT 2 OFFSET 1;
      `);
      expect(res.length).toBe(2);
      expect(res[0].name).toBe("Eve");
      expect(Number(res[0].rnk)).toBe(2);
    });

    test("93. Window function on empty table returns 0 rows", async () => {
      await db.query("CREATE TABLE empty_table (val INT);");
      const res = await db.query("SELECT val, ROW_NUMBER() OVER (ORDER BY val) AS rnk FROM empty_table;");
      expect(res.length).toBe(0);
    });

    test("94. Window function with parameter binding in WHERE clause", async () => {
      const res = await db.query(
        "SELECT name, salary, ROW_NUMBER() OVER (ORDER BY salary DESC) AS rnk FROM staff_salaries WHERE dept = $1 ORDER BY salary DESC;",
        ["Marketing"]
      );
      expect(res.length).toBe(2);
      expect(res[0].name).toBe("Heidi");
    });

    test("95. Top 3 highest revenues overall with ROW_NUMBER", async () => {
      const res = await db.query(`
        SELECT region, sale_date, revenue FROM (
          SELECT region, sale_date, revenue, ROW_NUMBER() OVER (ORDER BY revenue DESC) AS rnk
          FROM daily_sales
        ) t WHERE rnk <= 3 ORDER BY revenue DESC;
      `);
      expect(res.length).toBe(3);
      expect(Number(res[0].revenue)).toBe(3000);
    });

    test("96. LAG with NULL values in source column", async () => {
      const res = await db.query(`
        SELECT name, bonus, LAG(bonus) OVER (ORDER BY id ASC) AS prev_bonus
        FROM staff_salaries
        WHERE dept = 'Marketing'
        ORDER BY id ASC;
      `);
      expect(res.length).toBe(2);
      expect(res[0].prev_bonus).toBeNull();
      expect(Number(res[1].prev_bonus)).toBe(8000);
    });

    test("97. Window functions combined with CASE WHEN in projection", async () => {
      const res = await db.query(`
        WITH r AS (
          SELECT name, salary, ROW_NUMBER() OVER (ORDER BY salary DESC) AS rnk
          FROM staff_salaries
        )
        SELECT name, salary,
          CASE
            WHEN rnk <= 3 THEN 'Top Tier'
            ELSE 'Standard Tier'
          END AS bracket
        FROM r
        ORDER BY salary DESC;
      `);
      expect(res.length).toBe(9);
      expect(res[0].bracket).toBe("Top Tier");
      expect(res[8].bracket).toBe("Standard Tier");
    });

    test("98. Median emulation using NTILE(2)", async () => {
      const res = await db.query(`
        WITH buckets AS (
          SELECT salary, NTILE(2) OVER (ORDER BY salary ASC) AS half
          FROM staff_salaries
        )
        SELECT max(salary) AS approx_median FROM buckets WHERE half = 1;
      `);
      expect(Number(res[0].approx_median)).toBeGreaterThanOrEqual(80000);
    });

    test("99. Window sum over negative numbers and zeroes", async () => {
      await db.query("CREATE TABLE transactions_flow (tx_id SERIAL PRIMARY KEY, delta NUMERIC);");
      await db.query("INSERT INTO transactions_flow (delta) VALUES (100), (-50), (200), (-100), (0);");
      const res = await db.query(`
        SELECT tx_id, delta, SUM(delta) OVER (ORDER BY tx_id ASC) AS balance
        FROM transactions_flow
        ORDER BY tx_id ASC;
      `);
      expect(res.length).toBe(5);
      expect(Number(res[0].balance)).toBe(100);
      expect(Number(res[1].balance)).toBe(50);
      expect(Number(res[2].balance)).toBe(250);
      expect(Number(res[3].balance)).toBe(150);
      expect(Number(res[4].balance)).toBe(150);
    });

    test("100. Complex analytics pipeline: CTE + Window Partition + Ranking + Growth Rate", async () => {
      const res = await db.query(`
        WITH region_growth AS (
          SELECT region, sale_date, revenue,
            LAG(revenue) OVER (PARTITION BY region ORDER BY sale_date ASC) AS prev_r,
            SUM(revenue) OVER (PARTITION BY region ORDER BY sale_date ASC) AS cum_r
          FROM daily_sales
        )
        SELECT region, sale_date, revenue, cum_r
        FROM region_growth
        WHERE cum_r >= 3000
        ORDER BY region ASC, sale_date ASC;
      `);
      expect(res.length).toBeGreaterThanOrEqual(3);
      expect(res[0].region).toBe("North");
    });
  });
});
