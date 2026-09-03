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
  console.log(" ⚡ BENCHMARK SO SÁNH HIỆU NĂNG: PGLITE PURE JAVASCRIPT/TYPESCRIPT vs NATIVE RUST (NAPI) ⚡");
  console.log("==========================================================================================\n");

  const TOTAL_RECORDS = 100_000;
  const BATCH_SIZE = 1_000;
  const LOOKUP_ITERATIONS = 5_000;

  const metrics: BenchmarkMetric[] = [];

  // ==========================================
  // 1. PURE JAVASCRIPT / TYPESCRIPT ENGINE RUN
  // ==========================================
  console.log("------------------------------------------------------------------------------------------");
  console.log(`🟡 [1/2] Đang chạy PGLITE PURE JAVASCRIPT (Node.js/Bun V8 Engine) - ${TOTAL_RECORDS.toLocaleString()} records...`);
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

  // Bulk Ingestion (JS)
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
  console.log(`  ✓ Bulk Insert ${TOTAL_RECORDS.toLocaleString()} rows: ${jsIngestDuration.toFixed(2)}ms (${formatNumber(jsIngestOps)} ops/sec)`);

  // Point Lookup (JS)
  const jsLookupStart = performance.now();
  for (let k = 0; k < LOOKUP_ITERATIONS; k++) {
    const targetId = 1000 + (k % (TOTAL_RECORDS - 2000));
    await jsDb.query(`SELECT * FROM benchmark_users WHERE id = $1`, [targetId]);
  }
  const jsLookupDuration = performance.now() - jsLookupStart;
  const jsLookupOps = Math.floor(LOOKUP_ITERATIONS / (jsLookupDuration / 1000));
  console.log(`  ✓ Point Lookup (${LOOKUP_ITERATIONS.toLocaleString()} queries): ${jsLookupDuration.toFixed(2)}ms (${formatNumber(jsLookupOps)} ops/sec)`);

  // Full Table Count (JS)
  const jsCountStart = performance.now();
  const jsCountRes = await jsDb.query(`SELECT COUNT(*) as total FROM benchmark_users`);
  const jsCountDuration = performance.now() - jsCountStart;
  console.log(`  ✓ Full Count: ${jsCountDuration.toFixed(3)}ms (Total: ${jsCountRes[0]?.total})`);

  // Filtered Scan (JS)
  const jsFilterStart = performance.now();
  const jsFilterRes = await jsDb.query(
    `SELECT COUNT(*) as active_users FROM benchmark_users WHERE active = true AND age > 50`
  );
  const jsFilterDuration = performance.now() - jsFilterStart;
  console.log(`  ✓ Filtered Scan: ${jsFilterDuration.toFixed(3)}ms (Matches: ${jsFilterRes[0]?.active_users})`);

  // Point Update (JS)
  const jsUpdateStart = performance.now();
  for (let u = 0; u < 1000; u++) {
    await jsDb.exec(`UPDATE benchmark_users SET age = $1 WHERE id = $2`, [99, 50000 + u]);
  }
  const jsUpdateDuration = performance.now() - jsUpdateStart;
  const jsUpdateOps = Math.floor(1000 / (jsUpdateDuration / 1000));
  console.log(`  ✓ Point Update (1,000 queries): ${jsUpdateDuration.toFixed(2)}ms (${formatNumber(jsUpdateOps)} ops/sec)`);

  // Point Delete (JS)
  const jsDeleteStart = performance.now();
  for (let d = 0; d < 1000; d++) {
    await jsDb.exec(`DELETE FROM benchmark_users WHERE id = $1`, [50000 + d]);
  }
  const jsDeleteDuration = performance.now() - jsDeleteStart;
  const jsDeleteOps = Math.floor(1000 / (jsDeleteDuration / 1000));
  console.log(`  ✓ Point Delete (1,000 queries): ${jsDeleteDuration.toFixed(2)}ms (${formatNumber(jsDeleteOps)} ops/sec)`);

  const memAfterJS = getMemoryUsageMB();
  const jsMemDelta = memAfterJS - memBeforeJS;
  await jsDb.close();
  if (existsSync(JS_DB_FILE)) unlinkSync(JS_DB_FILE);
  if (existsSync(JS_DB_FILE + ".wal")) unlinkSync(JS_DB_FILE + ".wal");

  // Force garbage collection if available
  if (global.gc) global.gc();

  // ==========================================
  // 2. NATIVE RUST ENGINE RUN (VIA NAPI-RS)
  // ==========================================
  console.log("\n------------------------------------------------------------------------------------------");
  console.log(`🦀 [2/2] Đang chạy PGLITE NATIVE RUST (NAPI Addon) - ${TOTAL_RECORDS.toLocaleString()} records...`);
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

  // Bulk Ingestion (Rust)
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
  console.log(`  ✓ Bulk Insert ${TOTAL_RECORDS.toLocaleString()} rows: ${rustIngestDuration.toFixed(2)}ms (${formatNumber(rustIngestOps)} ops/sec)`);

  // Point Lookup (Rust)
  const rustLookupStart = performance.now();
  for (let k = 0; k < LOOKUP_ITERATIONS; k++) {
    const targetId = 1000 + (k % (TOTAL_RECORDS - 2000));
    rustDb.query(`SELECT * FROM benchmark_users WHERE id = $1`, [targetId]);
  }
  const rustLookupDuration = performance.now() - rustLookupStart;
  const rustLookupOps = Math.floor(LOOKUP_ITERATIONS / (rustLookupDuration / 1000));
  console.log(`  ✓ Point Lookup (${LOOKUP_ITERATIONS.toLocaleString()} queries): ${rustLookupDuration.toFixed(2)}ms (${formatNumber(rustLookupOps)} ops/sec)`);

  // Full Table Count (Rust)
  const rustCountStart = performance.now();
  const rustCountRes = rustDb.query(`SELECT COUNT(*) as total FROM benchmark_users`);
  const rustCountDuration = performance.now() - rustCountStart;
  console.log(`  ✓ Full Count: ${rustCountDuration.toFixed(3)}ms (Total: ${rustCountRes[0]?.total})`);

  // Filtered Scan (Rust)
  const rustFilterStart = performance.now();
  const rustFilterRes = rustDb.query(
    `SELECT COUNT(*) as active_users FROM benchmark_users WHERE active = true AND age > 50`
  );
  const rustFilterDuration = performance.now() - rustFilterStart;
  console.log(`  ✓ Filtered Scan: ${rustFilterDuration.toFixed(3)}ms (Matches: ${rustFilterRes[0]?.active_users})`);

  // Point Update (Rust)
  const rustUpdateStart = performance.now();
  for (let u = 0; u < 1000; u++) {
    rustDb.exec(`UPDATE benchmark_users SET age = $1 WHERE id = $2`, [99, 50000 + u]);
  }
  const rustUpdateDuration = performance.now() - rustUpdateStart;
  const rustUpdateOps = Math.floor(1000 / (rustUpdateDuration / 1000));
  console.log(`  ✓ Point Update (1,000 queries): ${rustUpdateDuration.toFixed(2)}ms (${formatNumber(rustUpdateOps)} ops/sec)`);

  // Point Delete (Rust)
  const rustDeleteStart = performance.now();
  for (let d = 0; d < 1000; d++) {
    rustDb.exec(`DELETE FROM benchmark_users WHERE id = $1`, [50000 + d]);
  }
  const rustDeleteDuration = performance.now() - rustDeleteStart;
  const rustDeleteOps = Math.floor(1000 / (rustDeleteDuration / 1000));
  console.log(`  ✓ Point Delete (1,000 queries): ${rustDeleteDuration.toFixed(2)}ms (${formatNumber(rustDeleteOps)} ops/sec)`);

  const memAfterRust = getMemoryUsageMB();
  const rustMemDelta = memAfterRust - memBeforeRust;
  rustDb.close();
  if (existsSync(RUST_DB_FILE)) unlinkSync(RUST_DB_FILE);
  if (existsSync(RUST_DB_FILE + ".wal")) unlinkSync(RUST_DB_FILE + ".wal");

  // ==========================================
  // TỔNG HỢP KẾT QUẢ SO SÁNH
  // ==========================================
  metrics.push({
    name: "Bulk Ingestion (100k rows, batch 1k)",
    jsDurationMs: jsIngestDuration,
    rustDurationMs: rustIngestDuration,
    jsOpsPerSec: jsIngestOps,
    rustOpsPerSec: rustIngestOps,
    speedup: `${(jsIngestDuration / rustIngestDuration).toFixed(1)}x`,
  });

  metrics.push({
    name: `Point Lookup (${LOOKUP_ITERATIONS.toLocaleString()} PK queries)`,
    jsDurationMs: jsLookupDuration,
    rustDurationMs: rustLookupDuration,
    jsOpsPerSec: jsLookupOps,
    rustOpsPerSec: rustLookupOps,
    speedup: `${(jsLookupDuration / rustLookupDuration).toFixed(1)}x`,
  });

  metrics.push({
    name: "Full Table Aggregation (COUNT)",
    jsDurationMs: jsCountDuration,
    rustDurationMs: rustCountDuration,
    jsOpsPerSec: 1000 / jsCountDuration,
    rustOpsPerSec: 1000 / rustCountDuration,
    speedup: `${(jsCountDuration / rustCountDuration).toFixed(1)}x`,
  });

  metrics.push({
    name: "Filtered Scan (active=true AND age>50)",
    jsDurationMs: jsFilterDuration,
    rustDurationMs: rustFilterDuration,
    jsOpsPerSec: 1000 / jsFilterDuration,
    rustOpsPerSec: 1000 / rustFilterDuration,
    speedup: `${(jsFilterDuration / rustFilterDuration).toFixed(1)}x`,
  });

  metrics.push({
    name: "Point Update (1,000 queries)",
    jsDurationMs: jsUpdateDuration,
    rustDurationMs: rustUpdateDuration,
    jsOpsPerSec: jsUpdateOps,
    rustOpsPerSec: rustUpdateOps,
    speedup: `${(jsUpdateDuration / rustUpdateDuration).toFixed(1)}x`,
  });

  metrics.push({
    name: "Point Delete (1,000 queries)",
    jsDurationMs: jsDeleteDuration,
    rustDurationMs: rustDeleteDuration,
    jsOpsPerSec: jsDeleteOps,
    rustOpsPerSec: rustDeleteOps,
    speedup: `${(jsDeleteDuration / rustDeleteDuration).toFixed(1)}x`,
  });

  console.log("\n==========================================================================================");
  console.log(" 📊 BẢNG TỔNG KẾT SO SÁNH HIỆU NĂNG: PURE JS vs NATIVE RUST (NAPI)");
  console.log("==========================================================================================");
  console.table(
    metrics.map((m) => ({
      "Hạng Mục Đo Lường": m.name,
      "Pure JS Latency": `${m.jsDurationMs.toFixed(2)} ms`,
      "Native Rust Latency": `${m.rustDurationMs.toFixed(2)} ms`,
      "JS Throughput": `${formatNumber(Math.round(m.jsOpsPerSec))} ops/s`,
      "Rust Throughput": `${formatNumber(Math.round(m.rustOpsPerSec))} ops/s`,
      "Tốc Độ Vượt Trội": `🚀 ${m.speedup} nhanh hơn`,
    }))
  );

  console.log("------------------------------------------------------------------------------------------");
  console.log(`💾 Tiêu Thụ Bộ Nhớ (RAM RSS Delta):`);
  console.log(`   - Pure JS Delta:   ~${jsMemDelta} MB`);
  console.log(`   - Native Rust:     ~${rustMemDelta} MB (Zero GC overhead, cache-locality)`);
  console.log("==========================================================================================\n");
}

runComparisonBenchmark().catch(console.error);
