import { LitePostgres, PGLite } from "../src/index";
import { NodeFSAdapter } from "../src/adapters/node";
import * as fs from "fs";

interface BenchmarkResult {
  category: string;
  testName: string;
  durationMs: number;
  opsPerSec: number;
  rowCount: number;
  complexityGrade: "FAST" | "MODERATE" | "EXPENSIVE" | "CRITICAL_BOTTLENECK";
  details: string;
  optimizationAdvice?: string;
}

const results: BenchmarkResult[] = [];

function recordResult(
  category: string,
  testName: string,
  durationMs: number,
  rowCount: number,
  details: string,
  thresholds: { moderate: number; expensive: number; critical: number },
  optimizationAdvice?: string
) {
  let complexityGrade: BenchmarkResult["complexityGrade"] = "FAST";
  if (durationMs >= thresholds.critical) {
    complexityGrade = "CRITICAL_BOTTLENECK";
  } else if (durationMs >= thresholds.expensive) {
    complexityGrade = "EXPENSIVE";
  } else if (durationMs >= thresholds.moderate) {
    complexityGrade = "MODERATE";
  }

  const opsPerSec = durationMs > 0 ? Math.round((1000 / durationMs) * 100) / 100 : 999999;

  results.push({
    category,
    testName,
    durationMs: Math.round(durationMs * 100) / 100,
    opsPerSec,
    rowCount,
    complexityGrade,
    details,
    optimizationAdvice
  });
}

async function runProfiler() {
  const DB_FILE = "perf_profiler_test.db";
  try {
    if (fs.existsSync(DB_FILE)) fs.unlinkSync(DB_FILE);
    if (fs.existsSync(DB_FILE + ".wal")) fs.unlinkSync(DB_FILE + ".wal");
  } catch {}

  console.log("==========================================================================================");
  console.log("            PGLITE COMPREHENSIVE ALGORITHM & EXECUTION PERFORMANCE PROFILER               ");
  console.log("==========================================================================================\n");

  const db = new PGLite(DB_FILE, { adapter: new NodeFSAdapter() });

  // -------------------------------------------------------------
  // 1. DATASET SETUP & SCHEMA INITIALIZATION
  // -------------------------------------------------------------
  console.log("📦 [Phase 1/10] Initializing Schemas & Seeding Large-Scale Profiling Datasets...");

  await db.exec(`
    CREATE TABLE users (
      id SERIAL PRIMARY KEY,
      name TEXT NOT NULL,
      email TEXT NOT NULL,
      role TEXT NOT NULL,
      department_id INT NOT NULL,
      salary NUMERIC NOT NULL,
      score FLOAT NOT NULL,
      is_active BOOLEAN NOT NULL,
      bio TEXT,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      metadata JSONB
    );

    CREATE TABLE departments (
      id SERIAL PRIMARY KEY,
      dept_name TEXT NOT NULL,
      code TEXT NOT NULL,
      budget NUMERIC NOT NULL
    );

    CREATE TABLE orders (
      id SERIAL PRIMARY KEY,
      user_id INT NOT NULL REFERENCES users(id),
      order_code TEXT NOT NULL,
      total_amount NUMERIC NOT NULL,
      status TEXT NOT NULL,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

    CREATE TABLE order_items (
      id SERIAL PRIMARY KEY,
      order_id INT NOT NULL REFERENCES orders(id),
      product_name TEXT NOT NULL,
      quantity INT NOT NULL,
      unit_price NUMERIC NOT NULL
    );
  `);

  const NUM_DEPTS = 20;
  const NUM_USERS = 25000;
  const NUM_ORDERS = 30000;
  const NUM_ITEMS = 60000;

  console.log(`   Seeding: ${NUM_DEPTS} depts, ${NUM_USERS.toLocaleString()} users, ${NUM_ORDERS.toLocaleString()} orders, ${NUM_ITEMS.toLocaleString()} items...`);

  // Seed Departments
  await db.exec("BEGIN");
  for (let d = 1; d <= NUM_DEPTS; d++) {
    await db.exec("INSERT INTO departments (id, dept_name, code, budget) VALUES ($1, $2, $3, $4)", [
      d,
      `Phòng Ban ${d} - Khối Kỹ Thuật & Nghiên Cứu`,
      `DEPT_${d}`,
      5000000 + d * 250000
    ]);
  }
  await db.exec("COMMIT");

  // Benchmark Bulk Ingest Batch vs Single
  const tIngestStart = performance.now();
  await db.exec("BEGIN");
  const userBatchSize = 1000;
  const roles = ["admin", "manager", "developer", "tester", "analyst", "viewer"];
  for (let i = 0; i < NUM_USERS; i += userBatchSize) {
    const placeholders: string[] = [];
    const params: any[] = [];
    for (let j = 0; j < userBatchSize; j++) {
      const uIdx = i + j + 1;
      const paramOffset = j * 9;
      placeholders.push(`($${paramOffset + 1}, $${paramOffset + 2}, $${paramOffset + 3}, $${paramOffset + 4}, $${paramOffset + 5}, $${paramOffset + 6}, $${paramOffset + 7}, $${paramOffset + 8}, $${paramOffset + 9})`);
      params.push(
        `Nguyễn Văn Người Dùng Số ${uIdx}`,
        `user_${uIdx}@company.example.com`,
        roles[uIdx % roles.length],
        (uIdx % NUM_DEPTS) + 1,
        15000000 + (uIdx % 50) * 1000000,
        parseFloat(((uIdx * 17) % 100 + ((uIdx % 10) * 0.1)).toFixed(2)),
        uIdx % 3 !== 0,
        `Kỹ sư phần mềm cao cấp tại bộ phận ${uIdx % NUM_DEPTS + 1}, chuyên trách PostgreSQL và database engine tối ưu hóa logic phức tạp`,
        JSON.stringify({ tier: (uIdx % 5) + 1, tags: ["postgresql", "rust", "typescript"], config: { theme: uIdx % 2 === 0 ? "dark" : "light", notifications: true } })
      );
    }
    await db.exec(
      `INSERT INTO users (name, email, role, department_id, salary, score, is_active, bio, metadata) VALUES ${placeholders.join(", ")}`,
      params
    );
  }
  await db.exec("COMMIT");
  const tIngestUsers = performance.now() - tIngestStart;
  recordResult(
    "DML / Ingestion",
    `Batch Multi-Row INSERT (${NUM_USERS.toLocaleString()} users)`,
    tIngestUsers,
    NUM_USERS,
    `Batch size 1000 with 9 columns + JSONB`,
    { moderate: 5000, expensive: 10000, critical: 20000 },
    "Consider pre-allocating row buffers and bulk WAL flushing."
  );

  // Seed Orders
  const tIngestOrdersStart = performance.now();
  await db.exec("BEGIN");
  const orderBatchSize = 1000;
  const statuses = ["PENDING", "PROCESSING", "COMPLETED", "CANCELLED", "REFUNDED"];
  for (let i = 0; i < NUM_ORDERS; i += orderBatchSize) {
    const placeholders: string[] = [];
    const params: any[] = [];
    for (let j = 0; j < orderBatchSize; j++) {
      const oIdx = i + j + 1;
      const paramOffset = j * 4;
      placeholders.push(`($${paramOffset + 1}, $${paramOffset + 2}, $${paramOffset + 3}, $${paramOffset + 4})`);
      params.push(
        (oIdx % NUM_USERS) + 1,
        `ORD-2026-${String(oIdx).padStart(6, "0")}`,
        100000 + (oIdx % 200) * 50000,
        statuses[oIdx % statuses.length]
      );
    }
    await db.exec(
      `INSERT INTO orders (user_id, order_code, total_amount, status) VALUES ${placeholders.join(", ")}`,
      params
    );
  }
  await db.exec("COMMIT");
  const tIngestOrders = performance.now() - tIngestOrdersStart;
  recordResult(
    "DML / Ingestion",
    `Batch Multi-Row INSERT (${NUM_ORDERS.toLocaleString()} orders)`,
    tIngestOrders,
    NUM_ORDERS,
    `Batch size 1000 with foreign keys`,
    { moderate: 5000, expensive: 10000, critical: 20000 }
  );

  // Seed Order Items
  await db.exec("BEGIN");
  const itemBatchSize = 2000;
  for (let i = 0; i < NUM_ITEMS; i += itemBatchSize) {
    const placeholders: string[] = [];
    const params: any[] = [];
    for (let j = 0; j < itemBatchSize; j++) {
      const itmIdx = i + j + 1;
      const paramOffset = j * 4;
      placeholders.push(`($${paramOffset + 1}, $${paramOffset + 2}, $${paramOffset + 3}, $${paramOffset + 4})`);
      params.push(
        (itmIdx % NUM_ORDERS) + 1,
        `Sản Phẩm Cao Cấp Linh Kiện #${(itmIdx % 100) + 1}`,
        (itmIdx % 10) + 1,
        50000 + (itmIdx % 50) * 10000
      );
    }
    await db.exec(
      `INSERT INTO order_items (order_id, product_name, quantity, unit_price) VALUES ${placeholders.join(", ")}`,
      params
    );
  }
  await db.exec("COMMIT");

  console.log(`✅ Seed Complete! Total Dataset Size: ${(NUM_USERS + NUM_ORDERS + NUM_ITEMS + NUM_DEPTS).toLocaleString()} records.\n`);

  // -------------------------------------------------------------
  // 2. SCAN & LOOKUP ALGORITHMS
  // -------------------------------------------------------------
  console.log("🔍 [Phase 2/10] Benchmarking Scans & Filter Algorithms...");

  // 2.1 Primary Key Lookup (Indexed / Direct PK)
  let t0 = performance.now();
  for (let k = 0; k < 50; k++) {
    await db.query(`SELECT * FROM users WHERE id = $1`, [k * 400 + 1]);
  }
  let t1 = performance.now();
  recordResult(
    "Scans & Lookups",
    "Point Lookup by Primary Key (50 queries)",
    (t1 - t0) / 50,
    1,
    `SELECT * FROM users WHERE id = $1 on ${NUM_USERS.toLocaleString()} rows`,
    { moderate: 5, expensive: 20, critical: 50 },
    "Direct O(1) Index lookup or rowid map bypasses full table scans."
  );

  // 2.2 Full Table Sequential Scan without Index
  t0 = performance.now();
  const unindexedRes = await db.query(`SELECT * FROM users WHERE email = $1`, ["user_18750@company.example.com"]);
  t1 = performance.now();
  recordResult(
    "Scans & Lookups",
    "Full Table Unindexed Column Scan",
    t1 - t0,
    unindexedRes.length,
    `Scans all ${NUM_USERS.toLocaleString()} rows checking string equality`,
    { moderate: 30, expensive: 100, critical: 300 },
    "Requires Secondary Indexes (B-Tree or Hash Index) on non-PK columns."
  );

  // 2.3 Range Scan with Multi-Condition Filters
  t0 = performance.now();
  const rangeRes = await db.query(`SELECT id, name, salary FROM users WHERE salary >= $1 AND salary <= $2 AND is_active = true`, [25000000, 35000000]);
  t1 = performance.now();
  recordResult(
    "Scans & Lookups",
    "Numerical Range Scan with Multi-Condition",
    t1 - t0,
    rangeRes.length,
    `Scanned ${NUM_USERS.toLocaleString()} rows, filtered ${rangeRes.length.toLocaleString()} matching records`,
    { moderate: 50, expensive: 150, critical: 400 },
    "B-Tree index range scan could avoid scanning inactive rows."
  );

  // -------------------------------------------------------------
  // 3. JOIN ALGORITHMS (EQUI, HASH, MULTI-WAY, NON-EQUI)
  // -------------------------------------------------------------
  console.log("🔗 [Phase 3/10] Benchmarking Join Algorithms & Relational Graph Matching...");

  // 3.1 2-Table Equi-Join (users JOIN orders)
  t0 = performance.now();
  const join2Res = await db.query(`
    SELECT u.id, u.name, o.order_code, o.total_amount
    FROM users u
    JOIN orders o ON u.id = o.user_id
    WHERE u.department_id = $1
  `, [5]);
  t1 = performance.now();
  recordResult(
    "Joins",
    "2-Table Equi-Join (users JOIN orders)",
    t1 - t0,
    join2Res.length,
    `Joined filtered users (${(NUM_USERS / NUM_DEPTS).toLocaleString()}) with ${NUM_ORDERS.toLocaleString()} orders`,
    { moderate: 50, expensive: 200, critical: 600 },
    "Hash Join should build hash table on smaller side to achieve O(N + M)."
  );

  // 3.2 3-Table Multi-Way Join (users JOIN orders JOIN order_items)
  t0 = performance.now();
  const join3Res = await db.query(`
    SELECT u.name, o.order_code, i.product_name, i.quantity, i.unit_price
    FROM users u
    JOIN orders o ON u.id = o.user_id
    JOIN order_items i ON o.id = i.order_id
    WHERE u.id = $1
  `, [123]);
  t1 = performance.now();
  recordResult(
    "Joins",
    "3-Table Multi-Way Join (Filtered root)",
    t1 - t0,
    join3Res.length,
    `3-way join across users -> orders -> order_items for single user`,
    { moderate: 40, expensive: 150, critical: 500 },
    "Index-nested-loop join or pushdown filter optimizes root table."
  );

  // 3.3 Large Unfiltered 2-Table Hash Join
  t0 = performance.now();
  const largeJoinRes = await db.query(`
    SELECT u.department_id, o.status, o.total_amount
    FROM users u
    JOIN orders o ON u.id = o.user_id
    WHERE o.status = 'COMPLETED'
    LIMIT 5000
  `);
  t1 = performance.now();
  recordResult(
    "Joins",
    "Large Join with Filter + LIMIT 5,000",
    t1 - t0,
    largeJoinRes.length,
    `Evaluates users (${NUM_USERS.toLocaleString()}) x orders (${NUM_ORDERS.toLocaleString()})`,
    { moderate: 150, expensive: 500, critical: 1500 },
    "Early pipeline termination on LIMIT avoids materializing all joined rows."
  );

  // 3.4 LEFT OUTER JOIN with Aggregation (Detecting Orphan/Group Counts)
  t0 = performance.now();
  const leftJoinRes = await db.query(`
    SELECT d.dept_name, COUNT(u.id) as total_employees
    FROM departments d
    LEFT JOIN users u ON d.id = u.department_id
    GROUP BY d.dept_name
  `);
  t1 = performance.now();
  recordResult(
    "Joins",
    "LEFT JOIN + GROUP BY Aggregation",
    t1 - t0,
    leftJoinRes.length,
    `Aggregated 20 departments over ${NUM_USERS.toLocaleString()} users`,
    { moderate: 50, expensive: 200, critical: 600 },
    "Hash Aggregate combined with Left Join stream."
  );

  // -------------------------------------------------------------
  // 4. AGGREGATION & GROUP BY ALGORITHMS
  // -------------------------------------------------------------
  console.log("📊 [Phase 4/10] Benchmarking Aggregation & GROUP BY Scaling...");

  // 4.1 Low Cardinality Group By (20 departments)
  t0 = performance.now();
  const lowCardRes = await db.query(`
    SELECT department_id, COUNT(*) as user_count, SUM(salary) as total_salary, AVG(score) as avg_score
    FROM users
    GROUP BY department_id
  `);
  t1 = performance.now();
  recordResult(
    "Aggregation & Group By",
    "Low-Cardinality GROUP BY (20 groups on 25,000 rows)",
    t1 - t0,
    lowCardRes.length,
    `Calculates COUNT, SUM, AVG across 25,000 records`,
    { moderate: 40, expensive: 150, critical: 400 },
    "Vectorized/streaming hash aggregation maintains fast group slots."
  );

  // 4.2 High Cardinality Group By (5,000 distinct groups)
  t0 = performance.now();
  const highCardRes = await db.query(`
    SELECT department_id, role, COUNT(*) as bracket_count, MAX(score) as max_score
    FROM users
    GROUP BY department_id, role
  `);
  t1 = performance.now();
  recordResult(
    "Aggregation & Group By",
    "Multi-Column High-Cardinality GROUP BY",
    t1 - t0,
    highCardRes.length,
    `Grouped across composite columns (department_id, role)`,
    { moderate: 60, expensive: 250, critical: 800 },
    "Avoid string key formatting in hash map; use numeric composite key hashing."
  );

  // 4.3 COUNT(DISTINCT column) Deduplication Overhead
  t0 = performance.now();
  const distinctAggRes = await db.query(`
    SELECT department_id, COUNT(DISTINCT role) as distinct_roles, COUNT(DISTINCT salary) as distinct_salaries
    FROM users
    GROUP BY department_id
  `);
  t1 = performance.now();
  recordResult(
    "Aggregation & Group By",
    "COUNT(DISTINCT col) Deduplication per Group",
    t1 - t0,
    distinctAggRes.length,
    `Allocates separate HashSets per group for distinct values`,
    { moderate: 60, expensive: 250, critical: 700 },
    "Per-group hash set allocations can cause GC pressure if not pooled."
  );

  // 4.4 Aggregation with HAVING Filter
  t0 = performance.now();
  const havingRes = await db.query(`
    SELECT department_id, SUM(salary) as total_budget
    FROM users
    GROUP BY department_id
    HAVING SUM(salary) > 20000000000
  `);
  t1 = performance.now();
  recordResult(
    "Aggregation & Group By",
    "GROUP BY with HAVING Clause Filter",
    t1 - t0,
    havingRes.length,
    `Filters grouped buckets post-aggregation`,
    { moderate: 30, expensive: 100, critical: 300 }
  );

  // -------------------------------------------------------------
  // 5. SORTING & PAGINATION ALGORITHMS (ORDER BY / LIMIT / OFFSET)
  // -------------------------------------------------------------
  console.log("⚡ [Phase 5/10] Benchmarking Sorting & Pagination Algorithms...");

  // 5.1 Full Array Sort on 25,000 records
  t0 = performance.now();
  const fullSortRes = await db.query(`SELECT id, score, salary FROM users ORDER BY score DESC`);
  t1 = performance.now();
  recordResult(
    "Sorting & Pagination",
    "Full Table Sort (25,000 rows ORDER BY float DESC)",
    t1 - t0,
    fullSortRes.length,
    `Full Timsort / QuickSort on 25k records`,
    { moderate: 40, expensive: 150, critical: 450 },
    "Avoid allocating intermediate sort-key strings; compare raw numbers directly."
  );

  // 5.2 Top-K Sorting with Small LIMIT (LIMIT 10 on 25,000 rows)
  t0 = performance.now();
  const topKRes = await db.query(`SELECT id, name, score FROM users ORDER BY score DESC LIMIT 10`);
  t1 = performance.now();
  recordResult(
    "Sorting & Pagination",
    "Top-K Sort (ORDER BY score DESC LIMIT 10)",
    t1 - t0,
    topKRes.length,
    `Retrieves top 10 rows out of 25,000`,
    { moderate: 20, expensive: 80, critical: 250 },
    "Min-Heap / Binary Heap Top-K priority queue achieves O(N log K) instead of O(N log N)."
  );

  // 5.3 Deep Offset Pagination (LIMIT 20 OFFSET 20,000)
  t0 = performance.now();
  const deepOffsetRes = await db.query(`SELECT id, name, salary FROM users ORDER BY id ASC LIMIT 20 OFFSET 20000`);
  t1 = performance.now();
  recordResult(
    "Sorting & Pagination",
    "Deep Offset Pagination (LIMIT 20 OFFSET 20,000)",
    t1 - t0,
    deepOffsetRes.length,
    `Traverses 20,000 rows before yielding 20`,
    { moderate: 40, expensive: 150, critical: 500 },
    "Keyset pagination (cursor-based WHERE id > last_seen) is recommended for deep offsets."
  );

  // 5.4 Multi-Column Sort with Mixed Direction
  t0 = performance.now();
  const multiSortRes = await db.query(`SELECT id, department_id, score, name FROM users ORDER BY department_id ASC, score DESC, name ASC LIMIT 1000`);
  t1 = performance.now();
  recordResult(
    "Sorting & Pagination",
    "Multi-Column Sort (3 Columns Mixed Direction)",
    t1 - t0,
    multiSortRes.length,
    `Evaluates 3 comparator tiers per row comparison`,
    { moderate: 50, expensive: 180, critical: 600 },
    "Compact binary comparison keys reduce multiple field lookups."
  );

  // -------------------------------------------------------------
  // 6. SUBQUERY & CTE EXECUTION ALGORITHMS
  // -------------------------------------------------------------
  console.log("🧩 [Phase 6/10] Benchmarking Subqueries & CTE Execution...");

  // 6.1 Uncorrelated Subquery (Cached IN Set)
  t0 = performance.now();
  const uncorrSubqueryRes = await db.query(`
    SELECT id, name, department_id
    FROM users
    WHERE department_id IN (
      SELECT id FROM departments WHERE budget > 7000000
    )
  `);
  t1 = performance.now();
  recordResult(
    "Subqueries & CTEs",
    "Uncorrelated Subquery with IN Set (25,000 rows)",
    t1 - t0,
    uncorrSubqueryRes.length,
    `Subquery evaluated once and stored in Hash Set`,
    { moderate: 30, expensive: 120, critical: 400 },
    "Subquery result memoization ensures O(1) membership check."
  );

  // 6.2 Correlated Subquery (Per-Row Evaluation)
  t0 = performance.now();
  const corrSubqueryRes = await db.query(`
    SELECT u.id, u.name, u.salary
    FROM users u
    WHERE u.salary > (
      SELECT AVG(u2.salary) FROM users u2 WHERE u2.department_id = u.department_id
    )
    LIMIT 200
  `);
  t1 = performance.now();
  recordResult(
    "Subqueries & CTEs",
    "Correlated Subquery (Per-Row AVG comparison)",
    t1 - t0,
    corrSubqueryRes.length,
    `Evaluates inner query per outer candidate row`,
    { moderate: 150, expensive: 600, critical: 2000 },
    "Optimizer rewrite: decorrelate to CTE or Window Function AVG() OVER (PARTITION BY department_id) to avoid O(N * M) cost."
  );

  // 6.3 Common Table Expression (CTE) Pipeline
  t0 = performance.now();
  const cteRes = await db.query(`
    WITH high_earners AS (
      SELECT id, name, department_id, salary
      FROM users
      WHERE salary > 30000000
    ),
    dept_aggregates AS (
      SELECT department_id, COUNT(*) as high_earner_count, AVG(salary) as avg_high_salary
      FROM high_earners
      GROUP BY department_id
    )
    SELECT d.dept_name, da.high_earner_count, da.avg_high_salary
    FROM dept_aggregates da
    JOIN departments d ON da.department_id = d.id
  `);
  t1 = performance.now();
  recordResult(
    "Subqueries & CTEs",
    "Multi-Stage CTE Pipeline with Join & Aggregation",
    t1 - t0,
    cteRes.length,
    `2-stage CTE materialized and joined with departments`,
    { moderate: 40, expensive: 160, critical: 500 }
  );

  // -------------------------------------------------------------
  // 7. WINDOW FUNCTIONS ALGORITHMS
  // -------------------------------------------------------------
  console.log("🪟 [Phase 7/10] Benchmarking Window Functions & Partitioning...");

  // 7.1 ROW_NUMBER() and RANK() OVER (PARTITION BY ... ORDER BY ...)
  t0 = performance.now();
  const windowRankRes = await db.query(`
    SELECT id, name, department_id, score,
           ROW_NUMBER() OVER (PARTITION BY department_id ORDER BY score DESC) as rank_in_dept
    FROM users
    LIMIT 1000
  `);
  t1 = performance.now();
  recordResult(
    "Window Functions",
    "ROW_NUMBER() OVER (PARTITION BY ... ORDER BY ...)",
    t1 - t0,
    windowRankRes.length,
    `Partitions 25,000 rows across 20 depts and sorts each partition`,
    { moderate: 60, expensive: 250, critical: 800 },
    "Partition bucket sort + single-pass stream iterator."
  );

  // 7.2 Running Cumulative Window (SUM / AVG OVER PARTITION)
  t0 = performance.now();
  const windowCumulRes = await db.query(`
    SELECT id, user_id, total_amount,
           SUM(total_amount) OVER (PARTITION BY user_id ORDER BY id ASC) as running_spent
    FROM orders
    LIMIT 2000
  `);
  t1 = performance.now();
  recordResult(
    "Window Functions",
    "Running Cumulative SUM() OVER (PARTITION BY user_id)",
    t1 - t0,
    windowCumulRes.length,
    `Running cumulative window over partitioned order streams`,
    { moderate: 80, expensive: 300, critical: 1000 },
    "Incremental prefix-sum accumulator per partition."
  );

  // -------------------------------------------------------------
  // 8. STRING MATCHING, REGEX & DIACRITICS ALGORITHMS
  // -------------------------------------------------------------
  console.log("🔤 [Phase 8/10] Benchmarking String Matching, Regex & Vietnamese Diacritics...");

  // 8.1 Prefix LIKE ('Nguyễn%')
  t0 = performance.now();
  const prefixRes = await db.query(`SELECT id, name FROM users WHERE name LIKE 'Nguyễn%'`);
  t1 = performance.now();
  recordResult(
    "String & Pattern Matching",
    "Prefix LIKE ('Nguyễn%')",
    t1 - t0,
    prefixRes.length,
    `Evaluates string.startsWith on 25,000 rows`,
    { moderate: 25, expensive: 80, critical: 250 },
    "Fast-path startsWith() bypasses regex compiler."
  );

  // 8.2 Substring ILIKE with Multiple Wildcards ('%kỹ%sư%cao%cấp%')
  t0 = performance.now();
  const multiWildcardRes = await db.query(`SELECT id, name, bio FROM users WHERE bio ILIKE '%kỹ%sư%cao%cấp%'`);
  t1 = performance.now();
  recordResult(
    "String & Pattern Matching",
    "Multi-Wildcard Substring ILIKE across 25,000 rows",
    t1 - t0,
    multiWildcardRes.length,
    `Compiles & tests regex pattern across 25,000 text fields`,
    { moderate: 50, expensive: 200, critical: 700 },
    "Regex compilation caching and Boyer-Moore / Knuth-Morris-Pratt substring search."
  );

  // 8.3 Vietnamese Diacritics & Case Insensitive Match
  t0 = performance.now();
  const vietnameseRes = await db.query(`SELECT id, name FROM users WHERE name ILIKE '%Người Dùng Số 123%'`);
  t1 = performance.now();
  recordResult(
    "String & Pattern Matching",
    "Vietnamese Diacritics ILIKE Matching",
    t1 - t0,
    vietnameseRes.length,
    `Matches UTF-8 multibyte characters with case insensitivity`,
    { moderate: 30, expensive: 120, critical: 400 }
  );

  // -------------------------------------------------------------
  // 9. JSONB DATA EXTRACTION & MANIPULATION
  // -------------------------------------------------------------
  console.log("🗄 [Phase 9/10] Benchmarking JSONB Parsing, Path Extraction & Aggregations...");

  // 9.1 Deep JSONB Path Extraction (metadata->'config'->>'theme')
  t0 = performance.now();
  const jsonbPathRes = await db.query(`
    SELECT id, metadata->'config'->>'theme' as theme
    FROM users
    WHERE metadata->'config'->>'theme' = 'dark'
  `);
  t1 = performance.now();
  recordResult(
    "JSONB & Nested Structures",
    "Deep JSONB Path Extraction (metadata->config->>theme)",
    t1 - t0,
    jsonbPathRes.length,
    `Parses & extracts nested JSON keys on 25,000 rows`,
    { moderate: 60, expensive: 250, critical: 800 },
    "Binary JSONB format or parsed JSON caching avoids repetitive JSON.parse."
  );

  // 9.2 JSON_BUILD_OBJECT on 5,000 rows
  t0 = performance.now();
  const jsonbBuildRes = await db.query(`
    SELECT id, JSON_BUILD_OBJECT('id', id, 'name', name, 'score', score) as user_card
    FROM users
    LIMIT 5000
  `);
  t1 = performance.now();
  recordResult(
    "JSONB & Nested Structures",
    "JSON_BUILD_OBJECT serialization (5,000 rows)",
    t1 - t0,
    jsonbBuildRes.length,
    `Serializes key-value maps to JSON strings`,
    { moderate: 30, expensive: 120, critical: 400 }
  );

  // 9.3 JSONB_AGG Aggregation per Group
  t0 = performance.now();
  const jsonbAggRes = await db.query(`
    SELECT department_id, JSONB_AGG(name) as user_names
    FROM users
    GROUP BY department_id
  `);
  t1 = performance.now();
  recordResult(
    "JSONB & Nested Structures",
    "JSONB_AGG per Department Group (25,000 rows)",
    t1 - t0,
    jsonbAggRes.length,
    `Aggregates arrays of JSON values per group`,
    { moderate: 40, expensive: 150, critical: 500 }
  );

  // -------------------------------------------------------------
  // 10. MUTATIONS & TRANSACTIONS (UPDATE, DELETE, TRUNCATE)
  // -------------------------------------------------------------
  console.log("🔄 [Phase 10/10] Benchmarking Mutations & Atomic Transaction Write Speeds...");

  // 10.1 Bulk UPDATE with WHERE Condition
  t0 = performance.now();
  await db.exec(`UPDATE users SET salary = salary * 1.05 WHERE department_id = $1`, [3]);
  t1 = performance.now();
  recordResult(
    "Mutations & Write I/O",
    "Bulk UPDATE (${(NUM_USERS / NUM_DEPTS).toLocaleString()} rows)",
    t1 - t0,
    NUM_USERS / NUM_DEPTS,
    `Modifies salary for department 3 and logs to WAL`,
    { moderate: 40, expensive: 150, critical: 500 },
    "In-place page mutation with batched WAL records."
  );

  // 10.2 Targeted DELETE by Range
  t0 = performance.now();
  await db.exec(`DELETE FROM order_items WHERE id >= 50000 AND id < 52000`);
  t1 = performance.now();
  recordResult(
    "Mutations & Write I/O",
    "Bulk DELETE by Range (2,000 rows)",
    t1 - t0,
    2000,
    `Marks tombstone flags / deletes from table and WAL`,
    { moderate: 40, expensive: 150, critical: 400 },
    "Tombstone bitset updates or page slot compaction."
  );

  await db.close();
  try {
    if (fs.existsSync(DB_FILE)) fs.unlinkSync(DB_FILE);
    if (fs.existsSync(DB_FILE + ".wal")) fs.unlinkSync(DB_FILE + ".wal");
  } catch {}

  // -------------------------------------------------------------
  // PRESENTATION & DIAGNOSTIC REPORT
  // -------------------------------------------------------------
  console.log("\n==========================================================================================");
  console.log("                     📊 PROFILING REPORT & ALGORITHM BOTTLENECK ANALYSIS                  ");
  console.log("==========================================================================================\n");

  console.table(
    results.map((r) => ({
      Category: r.category,
      "Test Operation": r.testName,
      "Latency (ms)": r.durationMs,
      "Throughput (ops/s)": r.opsPerSec,
      "Complexity Grade": r.complexityGrade,
    }))
  );

  console.log("\n------------------------------------------------------------------------------------------");
  console.log("🔥 TOP EXPENSIVE OPERATIONS & ALGORITHM OPTIMIZATION RECOMMENDATIONS");
  console.log("------------------------------------------------------------------------------------------\n");

  const expensiveList = results
    .filter((r) => r.complexityGrade === "CRITICAL_BOTTLENECK" || r.complexityGrade === "EXPENSIVE" || r.complexityGrade === "MODERATE")
    .sort((a, b) => b.durationMs - a.durationMs);

  expensiveList.forEach((item, idx) => {
    const badge =
      item.complexityGrade === "CRITICAL_BOTTLENECK"
        ? "🔴 [CRITICAL]"
        : item.complexityGrade === "EXPENSIVE"
        ? "🟠 [EXPENSIVE]"
        : "🟡 [MODERATE]";
    console.log(`${idx + 1}. ${badge} ${item.category} -> ${item.testName}`);
    console.log(`   ⏱ Latency: ${item.durationMs} ms | ${item.details}`);
    if (item.optimizationAdvice) {
      console.log(`   💡 Proposed Optimization: ${item.optimizationAdvice}`);
    }
    console.log("");
  });

  console.log("==========================================================================================");
  console.log("🎯 SUMMARY: Algorithmic Efficiency Score");
  const criticalCount = results.filter((r) => r.complexityGrade === "CRITICAL_BOTTLENECK").length;
  const expensiveCount = results.filter((r) => r.complexityGrade === "EXPENSIVE").length;
  const fastCount = results.filter((r) => r.complexityGrade === "FAST").length;

  console.log(`   ⚡ Fast / Highly Optimized : ${fastCount} operations`);
  console.log(`   🟡 Moderate                 : ${results.filter((r) => r.complexityGrade === "MODERATE").length} operations`);
  console.log(`   🟠 Expensive (Needs Tuning) : ${expensiveCount} operations`);
  console.log(`   🔴 Critical Bottlenecks     : ${criticalCount} operations`);
  console.log("==========================================================================================\n");
}

runProfiler().catch(console.error);
