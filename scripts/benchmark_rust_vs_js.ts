import { LitePostgres as JSPostgres } from "../src/database";
import { NodeFSAdapter } from "../src/adapters/node";
import { unlinkSync, existsSync } from "fs";
import { resolve } from "path";

// Load Native Rust NAPI module
const nativeModulePath = resolve(__dirname, "../crates/pglite-rs/pglite.node");
const { LitePostgresNative: RustPostgres } = require(nativeModulePath);

interface BenchmarkMetric {
  name: string;
  jsDurationMs: number;
  rustDurationMs: number;
  jsOpsPerSec: number;
  rustOpsPerSec: number;
  speedup: string;
}

function formatNumber(num: number): string {
  return num.toLocaleString();
}

function getMemoryUsageMB(): number {
  return Math.round(process.memoryUsage().rss / 1024 / 1024);
}

async function runComparisonBenchmark() {
  console.log("\n==========================================================================================");
  console.log(" ⚡ BENCHMARK SO SÁNH HIỆU NĂNG TOÀN DIỆN: NATIVE RUST (NAPI) vs PURE JAVASCRIPT ⚡");
  console.log("==========================================================================================\n");

  const TOTAL_RECORDS = 100_000;
  const BATCH_SIZE = 1_000;
  const LOOKUP_ITERATIONS = 5_000;
  const NON_PK_LOOKUP_ITERATIONS = 1_000;

  // ==========================================
  // 1. NATIVE RUST ENGINE RUN (VIA NAPI-RS)
  // ==========================================
  console.log("------------------------------------------------------------------------------------------");
  console.log(`🦀 [1/2] Đang chạy PGLITE NATIVE RUST (NAPI Addon) - ${TOTAL_RECORDS.toLocaleString()} records...`);
  console.log("------------------------------------------------------------------------------------------");

  const RUST_DB_FILE = "bench_rust.db";
  if (existsSync(RUST_DB_FILE)) unlinkSync(RUST_DB_FILE);
  if (existsSync(RUST_DB_FILE + ".wal")) unlinkSync(RUST_DB_FILE + ".wal");

  const memBeforeRust = getMemoryUsageMB();
  const rustDb = new RustPostgres(RUST_DB_FILE);

  rustDb.exec(`
    CREATE TABLE benchmark_users (
      id SERIAL PRIMARY KEY,
      name TEXT,
      age INTEGER,
      active BOOLEAN
    )
  `);

  // 1. Bulk Ingestion (Rust)
  const rustIngestStart = performance.now();
  rustDb.exec("BEGIN");
  for (let i = 0; i < TOTAL_RECORDS; i += BATCH_SIZE) {
    const placeholders: string[] = [];
    const params: any[] = [];
    for (let j = 0; j < BATCH_SIZE; j++) {
      const idx = i + j + 1;
      const offset = j * 3;
      placeholders.push(`($${offset + 1}, $${offset + 2}, $${offset + 3})`);
      params.push(`User_${idx}`, idx % 100, idx % 2 === 0);
    }
    rustDb.exec(
      `INSERT INTO benchmark_users (name, age, active) VALUES ${placeholders.join(", ")}`,
      params
    );
  }
  rustDb.exec("COMMIT");
  const rustIngestDuration = performance.now() - rustIngestStart;
  const rustIngestOps = Math.floor(TOTAL_RECORDS / (rustIngestDuration / 1000));
  console.log(`  ✓ 1. Bulk Insert ${TOTAL_RECORDS.toLocaleString()} rows: ${rustIngestDuration.toFixed(2)}ms (${formatNumber(rustIngestOps)} ops/sec)`);

  // 2. Point Lookup by PK (Rust)
  const rustLookupStart = performance.now();
  for (let k = 0; k < LOOKUP_ITERATIONS; k++) {
    const targetId = 1000 + (k % (TOTAL_RECORDS - 2000));
    rustDb.query(`SELECT * FROM benchmark_users WHERE id = $1`, [targetId]);
  }
  const rustLookupDuration = performance.now() - rustLookupStart;
  const rustLookupOps = Math.floor(LOOKUP_ITERATIONS / (rustLookupDuration / 1000));
  console.log(`  ✓ 2. Point Lookup (${LOOKUP_ITERATIONS.toLocaleString()} PK queries): ${rustLookupDuration.toFixed(2)}ms (${formatNumber(rustLookupOps)} ops/sec)`);

  // 3. Non-PK Exact Lookup (Rust)
  const rustNonPkStart = performance.now();
  for (let k = 0; k < NON_PK_LOOKUP_ITERATIONS; k++) {
    const targetName = `User_${1000 + (k % (TOTAL_RECORDS - 2000))}`;
    rustDb.query(`SELECT * FROM benchmark_users WHERE name = $1`, [targetName]);
  }
  const rustNonPkDuration = performance.now() - rustNonPkStart;
  const rustNonPkOps = Math.floor(NON_PK_LOOKUP_ITERATIONS / (rustNonPkDuration / 1000));
  console.log(`  ✓ 3. Non-PK String Lookup (${NON_PK_LOOKUP_ITERATIONS.toLocaleString()} queries): ${rustNonPkDuration.toFixed(2)}ms (${formatNumber(rustNonPkOps)} ops/sec)`);

  // 4. Full Table Count (Rust)
  const rustCountStart = performance.now();
  const rustCountRes = rustDb.query(`SELECT COUNT(*) as total FROM benchmark_users`);
  const rustCountDuration = performance.now() - rustCountStart;
  console.log(`  ✓ 4. Full Count: ${rustCountDuration.toFixed(3)}ms (Total: ${rustCountRes[0]?.total})`);

  // 5. Full Table Aggregations SUM/AVG/MIN/MAX (Rust)
  const rustAggStart = performance.now();
  const rustAggRes = rustDb.query(`SELECT SUM(age), AVG(age), MIN(age), MAX(age) FROM benchmark_users`);
  const rustAggDuration = performance.now() - rustAggStart;
  console.log(`  ✓ 5. Full Aggregations (SUM/AVG/MIN/MAX): ${rustAggDuration.toFixed(3)}ms (Sum: ${rustAggRes[0]?.sum}, Avg: ${rustAggRes[0]?.avg})`);

  // 6. Filtered Scan (Rust)
  const rustFilterStart = performance.now();
  const rustFilterRes = rustDb.query(
    `SELECT COUNT(*) as active_users FROM benchmark_users WHERE active = true AND age > 50`
  );
  const rustFilterDuration = performance.now() - rustFilterStart;
  console.log(`  ✓ 6. Filtered Scan (active=true AND age>50): ${rustFilterDuration.toFixed(3)}ms (Matches: ${rustFilterRes[0]?.active_users})`);

  // 7. Range Scan BETWEEN (Rust)
  const rustRangeStart = performance.now();
  const rustRangeRes = rustDb.query(
    `SELECT COUNT(*) as range_count FROM benchmark_users WHERE age BETWEEN 20 AND 40`
  );
  const rustRangeDuration = performance.now() - rustRangeStart;
  console.log(`  ✓ 7. Range Scan (age BETWEEN 20 AND 40): ${rustRangeDuration.toFixed(3)}ms (Matches: ${rustRangeRes[0]?.range_count})`);

  // 8. Sorting & Pagination (Rust)
  const rustSortStart = performance.now();
  const rustSortRes = rustDb.query(
    `SELECT * FROM benchmark_users ORDER BY age DESC LIMIT 50 OFFSET 100`
  );
  const rustSortDuration = performance.now() - rustSortStart;
  console.log(`  ✓ 8. Sorting & Pagination (ORDER BY age DESC LIMIT 50 OFFSET 100): ${rustSortDuration.toFixed(3)}ms (Returned: ${rustSortRes.length} rows)`);

  // 9. Point Update (Rust)
  const rustUpdateStart = performance.now();
  for (let u = 0; u < 1000; u++) {
    rustDb.exec(`UPDATE benchmark_users SET age = $1 WHERE id = $2`, [99, 50000 + u]);
  }
  const rustUpdateDuration = performance.now() - rustUpdateStart;
  const rustUpdateOps = Math.floor(1000 / (rustUpdateDuration / 1000));
  console.log(`  ✓ 9. Point Update (1,000 queries): ${rustUpdateDuration.toFixed(2)}ms (${formatNumber(rustUpdateOps)} ops/sec)`);

  // 10. Point Delete (Rust)
  const rustDeleteStart = performance.now();
  for (let d = 0; d < 1000; d++) {
    rustDb.exec(`DELETE FROM benchmark_users WHERE id = $1`, [50000 + d]);
  }
  const rustDeleteDuration = performance.now() - rustDeleteStart;
  const rustDeleteOps = Math.floor(1000 / (rustDeleteDuration / 1000));
  console.log(`  ✓ 10. Point Delete (1,000 queries): ${rustDeleteDuration.toFixed(2)}ms (${formatNumber(rustDeleteOps)} ops/sec)`);

  const memAfterRust = getMemoryUsageMB();
  const rustMemDelta = memAfterRust - memBeforeRust;
  rustDb.close();
  if (existsSync(RUST_DB_FILE)) unlinkSync(RUST_DB_FILE);
  if (existsSync(RUST_DB_FILE + ".wal")) unlinkSync(RUST_DB_FILE + ".wal");

  // Force garbage collection if available
  if (global.gc) global.gc();

  // ==========================================
  // 2. PURE JAVASCRIPT / TYPESCRIPT ENGINE RUN
  // ==========================================
  console.log("\n------------------------------------------------------------------------------------------");
  console.log(`🟡 [2/2] Đang chạy PGLITE PURE JAVASCRIPT (Node.js/Bun V8 Engine) - ${TOTAL_RECORDS.toLocaleString()} records...`);
  console.log("------------------------------------------------------------------------------------------");

  const JS_DB_FILE = "bench_js.db";
  if (existsSync(JS_DB_FILE)) unlinkSync(JS_DB_FILE);
  if (existsSync(JS_DB_FILE + ".wal")) unlinkSync(JS_DB_FILE + ".wal");

  const memBeforeJS = getMemoryUsageMB();
  const jsDb = new JSPostgres(JS_DB_FILE, { adapter: new NodeFSAdapter() });

  await jsDb.exec(`
    CREATE TABLE benchmark_users (
      id SERIAL PRIMARY KEY,
      name TEXT,
      age INTEGER,
      active BOOLEAN
    )
  `);

  // 1. Bulk Ingestion (JS)
  const jsIngestStart = performance.now();
  await jsDb.exec("BEGIN");
  for (let i = 0; i < TOTAL_RECORDS; i += BATCH_SIZE) {
    const placeholders: string[] = [];
    const params: any[] = [];
    for (let j = 0; j < BATCH_SIZE; j++) {
      const idx = i + j + 1;
      const offset = j * 3;
      placeholders.push(`($${offset + 1}, $${offset + 2}, $${offset + 3})`);
      params.push(`User_${idx}`, idx % 100, idx % 2 === 0);
    }
    await jsDb.exec(
      `INSERT INTO benchmark_users (name, age, active) VALUES ${placeholders.join(", ")}`,
      params
    );
  }
  await jsDb.exec("COMMIT");
  const jsIngestDuration = performance.now() - jsIngestStart;
  const jsIngestOps = Math.floor(TOTAL_RECORDS / (jsIngestDuration / 1000));
  console.log(`  ✓ 1. Bulk Insert ${TOTAL_RECORDS.toLocaleString()} rows: ${jsIngestDuration.toFixed(2)}ms (${formatNumber(jsIngestOps)} ops/sec)`);

  // 2. Point Lookup by PK (JS)
  const jsLookupStart = performance.now();
  for (let k = 0; k < LOOKUP_ITERATIONS; k++) {
    const targetId = 1000 + (k % (TOTAL_RECORDS - 2000));
    await jsDb.query(`SELECT * FROM benchmark_users WHERE id = $1`, [targetId]);
  }
  const jsLookupDuration = performance.now() - jsLookupStart;
  const jsLookupOps = Math.floor(LOOKUP_ITERATIONS / (jsLookupDuration / 1000));
  console.log(`  ✓ 2. Point Lookup (${LOOKUP_ITERATIONS.toLocaleString()} PK queries): ${jsLookupDuration.toFixed(2)}ms (${formatNumber(jsLookupOps)} ops/sec)`);

  // 3. Non-PK Exact Lookup (JS)
  const jsNonPkStart = performance.now();
  for (let k = 0; k < NON_PK_LOOKUP_ITERATIONS; k++) {
    const targetName = `User_${1000 + (k % (TOTAL_RECORDS - 2000))}`;
    await jsDb.query(`SELECT * FROM benchmark_users WHERE name = $1`, [targetName]);
  }
  const jsNonPkDuration = performance.now() - jsNonPkStart;
  const jsNonPkOps = Math.floor(NON_PK_LOOKUP_ITERATIONS / (jsNonPkDuration / 1000));
  console.log(`  ✓ 3. Non-PK String Lookup (${NON_PK_LOOKUP_ITERATIONS.toLocaleString()} queries): ${jsNonPkDuration.toFixed(2)}ms (${formatNumber(jsNonPkOps)} ops/sec)`);

  // 4. Full Table Count (JS)
  const jsCountStart = performance.now();
  const jsCountRes = await jsDb.query(`SELECT COUNT(*) as total FROM benchmark_users`);
  const jsCountDuration = performance.now() - jsCountStart;
  console.log(`  ✓ 4. Full Count: ${jsCountDuration.toFixed(3)}ms (Total: ${jsCountRes[0]?.total})`);

  // 5. Full Table Aggregations SUM/AVG/MIN/MAX (JS)
  const jsAggStart = performance.now();
  const jsAggRes = await jsDb.query(`SELECT SUM(age), AVG(age), MIN(age), MAX(age) FROM benchmark_users`);
  const jsAggDuration = performance.now() - jsAggStart;
  console.log(`  ✓ 5. Full Aggregations (SUM/AVG/MIN/MAX): ${jsAggDuration.toFixed(3)}ms (Sum: ${jsAggRes[0]?.sum}, Avg: ${jsAggRes[0]?.avg})`);

  // 6. Filtered Scan (JS)
  const jsFilterStart = performance.now();
  const jsFilterRes = await jsDb.query(
    `SELECT COUNT(*) as active_users FROM benchmark_users WHERE active = true AND age > 50`
  );
  const jsFilterDuration = performance.now() - jsFilterStart;
  console.log(`  ✓ 6. Filtered Scan (active=true AND age>50): ${jsFilterDuration.toFixed(3)}ms (Matches: ${jsFilterRes[0]?.active_users})`);

  // 7. Range Scan BETWEEN (JS)
  const jsRangeStart = performance.now();
  const jsRangeRes = await jsDb.query(
    `SELECT COUNT(*) as range_count FROM benchmark_users WHERE age BETWEEN 20 AND 40`
  );
  const jsRangeDuration = performance.now() - jsRangeStart;
  console.log(`  ✓ 7. Range Scan (age BETWEEN 20 AND 40): ${jsRangeDuration.toFixed(3)}ms (Matches: ${jsRangeRes[0]?.range_count})`);

  // 8. Sorting & Pagination (JS)
  const jsSortStart = performance.now();
  const jsSortRes = await jsDb.query(
    `SELECT * FROM benchmark_users ORDER BY age DESC LIMIT 50 OFFSET 100`
  );
  const jsSortDuration = performance.now() - jsSortStart;
  console.log(`  ✓ 8. Sorting & Pagination (ORDER BY age DESC LIMIT 50 OFFSET 100): ${jsSortDuration.toFixed(3)}ms (Returned: ${jsSortRes.length} rows)`);

  // 9. Point Update (JS)
  const jsUpdateStart = performance.now();
  for (let u = 0; u < 1000; u++) {
    await jsDb.exec(`UPDATE benchmark_users SET age = $1 WHERE id = $2`, [99, 50000 + u]);
  }
  const jsUpdateDuration = performance.now() - jsUpdateStart;
  const jsUpdateOps = Math.floor(1000 / (jsUpdateDuration / 1000));
  console.log(`  ✓ 9. Point Update (1,000 queries): ${jsUpdateDuration.toFixed(2)}ms (${formatNumber(jsUpdateOps)} ops/sec)`);

  // 10. Point Delete (JS)
  const jsDeleteStart = performance.now();
  for (let d = 0; d < 1000; d++) {
    await jsDb.exec(`DELETE FROM benchmark_users WHERE id = $1`, [50000 + d]);
  }
  const jsDeleteDuration = performance.now() - jsDeleteStart;
  const jsDeleteOps = Math.floor(1000 / (jsDeleteDuration / 1000));
  console.log(`  ✓ 10. Point Delete (1,000 queries): ${jsDeleteDuration.toFixed(2)}ms (${formatNumber(jsDeleteOps)} ops/sec)`);

  const memAfterJS = getMemoryUsageMB();
  const jsMemDelta = memAfterJS - memBeforeJS;
  await jsDb.close();
  if (existsSync(JS_DB_FILE)) unlinkSync(JS_DB_FILE);
  if (existsSync(JS_DB_FILE + ".wal")) unlinkSync(JS_DB_FILE + ".wal");

  // ==========================================
  // TỔNG HỢP KẾT QUẢ SO SÁNH
  // ==========================================
  const metrics: BenchmarkMetric[] = [];

  metrics.push({
    name: "1. Bulk Ingestion (100k rows, batch 1k)",
    jsDurationMs: jsIngestDuration,
    rustDurationMs: rustIngestDuration,
    jsOpsPerSec: jsIngestOps,
    rustOpsPerSec: rustIngestOps,
    speedup: `${(jsIngestDuration / rustIngestDuration).toFixed(1)}x`,
  });

  metrics.push({
    name: `2. Point Lookup (${LOOKUP_ITERATIONS.toLocaleString()} PK queries)`,
    jsDurationMs: jsLookupDuration,
    rustDurationMs: rustLookupDuration,
    jsOpsPerSec: jsLookupOps,
    rustOpsPerSec: rustLookupOps,
    speedup: `${(jsLookupDuration / rustLookupDuration).toFixed(1)}x`,
  });

  metrics.push({
    name: `3. Non-PK Exact Lookup (1,000 string queries)`,
    jsDurationMs: jsNonPkDuration,
    rustDurationMs: rustNonPkDuration,
    jsOpsPerSec: jsNonPkOps,
    rustOpsPerSec: rustNonPkOps,
    speedup: `${(jsNonPkDuration / rustNonPkDuration).toFixed(1)}x`,
  });

  metrics.push({
    name: "4. Full Table Aggregation (COUNT)",
    jsDurationMs: jsCountDuration,
    rustDurationMs: rustCountDuration,
    jsOpsPerSec: 1000 / jsCountDuration,
    rustOpsPerSec: 1000 / rustCountDuration,
    speedup: `${(jsCountDuration / rustCountDuration).toFixed(1)}x`,
  });

  metrics.push({
    name: "5. Full Aggregations (SUM, AVG, MIN, MAX)",
    jsDurationMs: jsAggDuration,
    rustDurationMs: rustAggDuration,
    jsOpsPerSec: 1000 / jsAggDuration,
    rustOpsPerSec: 1000 / rustAggDuration,
    speedup: `${(jsAggDuration / rustAggDuration).toFixed(1)}x`,
  });

  metrics.push({
    name: "6. Filtered Scan (active=true AND age>50)",
    jsDurationMs: jsFilterDuration,
    rustDurationMs: rustFilterDuration,
    jsOpsPerSec: 1000 / jsFilterDuration,
    rustOpsPerSec: 1000 / rustFilterDuration,
    speedup: `${(jsFilterDuration / rustFilterDuration).toFixed(1)}x`,
  });

  metrics.push({
    name: "7. Range Scan (age BETWEEN 20 AND 40)",
    jsDurationMs: jsRangeDuration,
    rustDurationMs: rustRangeDuration,
    jsOpsPerSec: 1000 / jsRangeDuration,
    rustOpsPerSec: 1000 / rustRangeDuration,
    speedup: `${(jsRangeDuration / rustRangeDuration).toFixed(1)}x`,
  });

  metrics.push({
    name: "8. Sorting & Pagination (ORDER BY + LIMIT)",
    jsDurationMs: jsSortDuration,
    rustDurationMs: rustSortDuration,
    jsOpsPerSec: 1000 / jsSortDuration,
    rustOpsPerSec: 1000 / rustSortDuration,
    speedup: `${(jsSortDuration / rustSortDuration).toFixed(1)}x`,
  });

  metrics.push({
    name: "9. Point Update (1,000 queries)",
    jsDurationMs: jsUpdateDuration,
    rustDurationMs: rustUpdateDuration,
    jsOpsPerSec: jsUpdateOps,
    rustOpsPerSec: rustUpdateOps,
    speedup: `${(jsUpdateDuration / rustUpdateDuration).toFixed(1)}x`,
  });

  metrics.push({
    name: "10. Point Delete (1,000 queries)",
    jsDurationMs: jsDeleteDuration,
    rustDurationMs: rustDeleteDuration,
    jsOpsPerSec: jsDeleteOps,
    rustOpsPerSec: rustDeleteOps,
    speedup: `${(jsDeleteDuration / rustDeleteDuration).toFixed(1)}x`,
  });

  console.log("\n========================================================================================================");
  console.log(" 📊 BẢNG TỔNG KẾT SO SÁNH HIỆU NĂNG TOÀN DIỆN: NATIVE RUST (NAPI) vs PURE JS");
  console.log("========================================================================================================");
  console.table(
    metrics.map((m) => ({
      "Hạng Mục Đo Lường": m.name,
      "Native Rust Latency": `${m.rustDurationMs.toFixed(2)} ms`,
      "Pure JS Latency": `${m.jsDurationMs.toFixed(2)} ms`,
      "Rust Throughput": `${formatNumber(Math.round(m.rustOpsPerSec))} ops/s`,
      "JS Throughput": `${formatNumber(Math.round(m.jsOpsPerSec))} ops/s`,
      "Tốc Độ Vượt Trội": `🚀 ${m.speedup} nhanh hơn`,
    }))
  );

  console.log("--------------------------------------------------------------------------------------------------------");
  console.log(`💾 Tiêu Thụ Bộ Nhớ (RAM RSS Delta):`);
  console.log(`   - Native Rust:     ~${rustMemDelta} MB (Zero GC overhead, cache-locality)`);
  console.log(`   - Pure JS Delta:   ~${jsMemDelta} MB`);
  console.log("========================================================================================================\n");
}

runComparisonBenchmark().catch(console.error);
