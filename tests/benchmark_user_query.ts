import { PGLiteNative } from "../src/native";
import * as fs from "fs";

async function main() {
  console.log("==========================================================================");
  console.log(" 🚀 BENCHMARK: CORRELATED SUBQUERIES & LEFT JOIN (Native Rust Engine) 🚀");
  console.log("==========================================================================");

  const nativeDb = new PGLiteNative(":memory:");

  const setupSql = [
    "CREATE TABLE provinces (id SERIAL PRIMARY KEY, name TEXT, deleted_at TEXT)",
    "CREATE TABLE regions (id SERIAL PRIMARY KEY, name TEXT, description TEXT, province_id INT, created_at TEXT, deleted_at TEXT)",
    "CREATE TABLE schools (id SERIAL PRIMARY KEY, name TEXT, region_id INT, deleted_at TEXT)",
    "CREATE TABLE region_admins (id SERIAL PRIMARY KEY, name TEXT, region_id INT, deleted_at TEXT)",
  ];

  for (const s of setupSql) {
    await nativeDb.exec(s);
  }

  console.log("🌱 Ingesting test dataset (63 provinces, 500 regions, 5,000 schools, 1,000 admins)...");

  // Provinces
  const provIns: string[] = [];
  for (let i = 1; i <= 63; i++) {
    provIns.push(`(${i}, 'Province_${i}', NULL)`);
  }
  await nativeDb.exec(`INSERT INTO provinces (id, name, deleted_at) VALUES ${provIns.join(", ")}`);

  // Regions
  const regIns: string[] = [];
  for (let i = 1; i <= 500; i++) {
    const provId = (i % 63) + 1;
    const createdAt = `2026-09-0${(i % 9) + 1} 10:00:00`;
    const deletedAt = i % 20 === 0 ? `'2026-09-01'` : `NULL`;
    regIns.push(`(${i}, 'Region_${i}', 'Desc_${i}', ${provId}, '${createdAt}', ${deletedAt})`);
  }
  await nativeDb.exec(`INSERT INTO regions (id, name, description, province_id, created_at, deleted_at) VALUES ${regIns.join(", ")}`);

  // Schools (batches of 500)
  for (let batch = 0; batch < 5000; batch += 500) {
    const schIns: string[] = [];
    for (let i = batch + 1; i <= batch + 500; i++) {
      const regId = (i % 500) + 1;
      const deletedAt = i % 15 === 0 ? `'2026-09-01'` : `NULL`;
      schIns.push(`(${i}, 'School_${i}', ${regId}, ${deletedAt})`);
    }
    await nativeDb.exec(`INSERT INTO schools (id, name, region_id, deleted_at) VALUES ${schIns.join(", ")}`);
  }

  // Region Admins
  const admIns: string[] = [];
  for (let i = 1; i <= 1000; i++) {
    const regId = (i % 500) + 1;
    const deletedAt = i % 10 === 0 ? `'2026-09-01'` : `NULL`;
    admIns.push(`(${i}, 'Admin_${i}', ${regId}, ${deletedAt})`);
  }
  await nativeDb.exec(`INSERT INTO region_admins (id, name, region_id, deleted_at) VALUES ${admIns.join(", ")}`);

  await nativeDb.flushWriteQueue();

  const userQuery = `
    SELECT
      r.id,
      r.name,
      r.description,
      r.province_id,
      r.created_at,
      p.name                                AS province_name,   -- tên tỉnh cha
      (SELECT COUNT(*) FROM schools s
         WHERE s.region_id = r.id
           AND s.deleted_at IS NULL)         AS school_count,    -- số trường
      (SELECT COUNT(*) FROM region_admins ra
         WHERE ra.region_id = r.id
           AND ra.deleted_at IS NULL)        AS admin_count      -- số admin
    FROM regions r
    LEFT JOIN provinces p ON p.id = r.province_id
    WHERE r.deleted_at IS NULL
    ORDER BY r.created_at DESC
    LIMIT 200 OFFSET 0;
  `;

  console.log("\n⚡ [1/2] First Query Execution (Native Rust Engine)...");
  const t0_native = performance.now();
  const nativeRes = await nativeDb.query(userQuery);
  const t1_native = performance.now();
  const nativeLatency = (t1_native - t0_native);

  console.log(`  ✓ Native Rust Latency: ${nativeLatency.toFixed(3)} ms`);
  console.log(`  ✓ Row count returned: ${nativeRes.length}`);
  console.log(`  ✓ Sample row 0:`, JSON.stringify(nativeRes[0], null, 2));

  // Benchmark 100 Consecutive Executions on Native Rust
  console.log("\n🔥 [2/2] Benchmarking 100 consecutive executions on Native Rust...");
  const n_runs = 100;
  const start_r = performance.now();
  for (let i = 0; i < n_runs; i++) {
    await nativeDb.query(userQuery);
  }
  const end_r = performance.now();
  const total_time = (end_r - start_r);
  const avg_native = total_time / n_runs;
  const native_ops = (1000 / avg_native);

  console.log(`  ✓ Total time for 100 queries: ${total_time.toFixed(2)} ms`);
  console.log(`  🦀 Native Rust Average Latency: ${avg_native.toFixed(3)} ms/query`);
  console.log(`  🚀 Native Rust Throughput:      ${native_ops.toFixed(0)} queries/sec`);

  await nativeDb.close();
}

main().catch(console.error);
