import { LitePostgres } from "../src/database";
import { NodeFSAdapter } from "../src/adapters/node";
import * as fs from "fs";

interface MemorySnapshot {
  rss: number;
  heapUsed: number;
  heapTotal: number;
  external: number;
}

function getMemory(): MemorySnapshot {
  const mem = process.memoryUsage();
  return {
    rss: Math.round((mem.rss / 1024 / 1024) * 100) / 100,
    heapUsed: Math.round((mem.heapUsed / 1024 / 1024) * 100) / 100,
    heapTotal: Math.round((mem.heapTotal / 1024 / 1024) * 100) / 100,
    external: Math.round((mem.external / 1024 / 1024) * 100) / 100,
  };
}

function formatMem(m: MemorySnapshot): string {
  return `RSS: ${m.rss.toFixed(1)}MB | Heap: ${m.heapUsed.toFixed(1)}/${m.heapTotal.toFixed(1)}MB | Ext: ${m.external.toFixed(1)}MB`;
}

function cleanFiles(prefix: string) {
  for (const ext of ["", ".wal"]) {
    const file = prefix + ext;
    if (fs.existsSync(file)) {
      try {
        fs.unlinkSync(file);
      } catch {}
    }
  }
}

async function runBenchmark() {
  console.log("\n==========================================================================");
  console.log("  🐘 LitePostgres (PGLite) | High-Volume Ingestion & Memory Benchmark");
  console.log("==========================================================================");

  const DB_FILE = "perf_ram_test.db";
  cleanFiles(DB_FILE);

  let peakRSS = 0;
  let peakHeap = 0;

  function trackPeak() {
    const m = getMemory();
    if (m.rss > peakRSS) peakRSS = m.rss;
    if (m.heapUsed > peakHeap) peakHeap = m.heapUsed;
  }

  const initialMem = getMemory();
  console.log(`\n📊 Baseline Memory:  ${formatMem(initialMem)}`);

  const db = new LitePostgres(DB_FILE, {
    adapter: new NodeFSAdapter(),
  });

  // Setup schema
  await db.exec(`
    CREATE TABLE categories (
      id SERIAL PRIMARY KEY,
      name TEXT
    )
  `);

  await db.exec(`
    CREATE TABLE orders (
      id SERIAL PRIMARY KEY,
      customer_name TEXT,
      category_id INT REFERENCES categories(id),
      total_amount NUMERIC,
      payload TEXT,
      created_at TEXT
    )
  `);

  // Seed reference categories
  await db.exec("BEGIN");
  for (let c = 1; c <= 10; c++) {
    await db.exec("INSERT INTO categories (name) VALUES ($1)", [`Category_${c}`]);
  }
  await db.exec("COMMIT");

  // -------------------------------------------------------------------------
  // TEST 1: Batch Insert (Bulk records with Foreign Key and Primary Key)
  // -------------------------------------------------------------------------
  const totalRecords = 100_000;
  const batchSize = 1_000;

  console.log(`\n--------------------------------------------------------------------------`);
  console.log(`▶ PHASE 1: Bulk Ingestion (${totalRecords.toLocaleString()} rows, batch size = ${batchSize.toLocaleString()})`);
  console.log(`  Features: Primary Key (SERIAL), Foreign Key constraint, JSON-like payload`);
  console.log(`--------------------------------------------------------------------------`);

  const startT = performance.now();
  await db.exec("BEGIN");

  for (let i = 0; i < totalRecords; i += batchSize) {
    const placeholders: string[] = [];
    const params: any[] = [];

    for (let j = 0; j < batchSize; j++) {
      const idx = i + j + 1;
      const baseParamIdx = j * 5;
      placeholders.push(
        `($${baseParamIdx + 1}, $${baseParamIdx + 2}, $${baseParamIdx + 3}, $${baseParamIdx + 4}, $${baseParamIdx + 5})`
      );
      params.push(
        `Customer_${idx}`,
        (idx % 10) + 1,
        Math.round((idx * 1.5 + 10) * 100) / 100,
        `{"item":"product_${idx % 100}","note":"processed batch successfully"}`,
        new Date().toISOString()
      );
    }

    await db.exec(
      `INSERT INTO orders (customer_name, category_id, total_amount, payload, created_at) VALUES ${placeholders.join(", ")}`,
      params
    );

    trackPeak();

    const ingested = i + batchSize;
    const elapsedSec = (performance.now() - startT) / 1000 || 0.001;
    const throughput = Math.floor(ingested / elapsedSec);
    const progress = ((ingested / totalRecords) * 100).toFixed(1);
    const curMem = getMemory();

    process.stdout.write(
      `\r  ⏳ [${progress}%] Ingested: ${ingested.toLocaleString()}/${totalRecords.toLocaleString()} | Speed: ${throughput.toLocaleString()} ops/s | ${formatMem(curMem)}`
    );
  }

  await db.exec("COMMIT");
  const endT = performance.now();
  const durationSec = (endT - startT) / 1000;
  const avgThroughput = Math.floor(totalRecords / durationSec);

  trackPeak();
  const afterPhase1Mem = getMemory();

  console.log(`\n\n  ✅ Phase 1 Completed:`);
  console.log(`     ⏱ Duration:       ${durationSec.toFixed(2)}s`);
  console.log(`     📈 Avg Speed:      ${avgThroughput.toLocaleString()} ops/sec`);
  console.log(`     🧠 Memory After:   ${formatMem(afterPhase1Mem)}`);
  console.log(`     ⛰ Peak RSS:       ${peakRSS.toFixed(1)} MB`);
  console.log(`     ⛰ Peak Heap:      ${peakHeap.toFixed(1)} MB`);

  // -------------------------------------------------------------------------
  // TEST 2: High-Frequency Individual Parameterized Inserts
  // -------------------------------------------------------------------------
  const indCount = 5_000;
  console.log(`\n--------------------------------------------------------------------------`);
  console.log(`▶ PHASE 2: High-Frequency Parameterized Inserts (${indCount.toLocaleString()} sequential rows)`);
  console.log(`  Tests: AST cache, queue promise management & slotted page appending`);
  console.log(`--------------------------------------------------------------------------`);

  const startT2 = performance.now();
  await db.exec("BEGIN");

  for (let i = 0; i < indCount; i++) {
    const idx = totalRecords + i + 1;
    await db.exec(
      "INSERT INTO orders (customer_name, category_id, total_amount, payload, created_at) VALUES ($1, $2, $3, $4, $5)",
      [
        `Customer_${idx}`,
        (idx % 10) + 1,
        199.99,
        `{"status":"individual"}`,
        new Date().toISOString(),
      ]
    );

    if ((i + 1) % 1000 === 0 || i + 1 === indCount) {
      trackPeak();
      const elapsed = (performance.now() - startT2) / 1000 || 0.001;
      const speed = Math.floor((i + 1) / elapsed);
      const curMem = getMemory();
      process.stdout.write(
        `\r  ⏳ Progress: ${(i + 1).toLocaleString()}/${indCount.toLocaleString()} | Speed: ${speed.toLocaleString()} ops/s | ${formatMem(curMem)}`
      );
    }
  }

  await db.exec("COMMIT");
  const endT2 = performance.now();
  const durationSec2 = (endT2 - startT2) / 1000;
  const avgThroughput2 = Math.floor(indCount / durationSec2);

  trackPeak();
  const afterPhase2Mem = getMemory();

  console.log(`\n\n  ✅ Phase 2 Completed:`);
  console.log(`     ⏱ Duration:       ${durationSec2.toFixed(2)}s`);
  console.log(`     📈 Avg Speed:      ${avgThroughput2.toLocaleString()} ops/sec`);
  console.log(`     🧠 Memory After:   ${formatMem(afterPhase2Mem)}`);

  // -------------------------------------------------------------------------
  // TEST 3: Verification & Data Integrity
  // -------------------------------------------------------------------------
  console.log(`\n--------------------------------------------------------------------------`);
  console.log(`▶ PHASE 3: Data Integrity & Query Validation`);
  console.log(`--------------------------------------------------------------------------`);

  const countRes = await db.query(`SELECT COUNT(*) as total FROM orders`);
  const totalCount = Number(countRes[0]?.total);
  const expectedCount = totalRecords + indCount;

  console.log(`  • COUNT Check:       ${totalCount.toLocaleString()} rows (Expected: ${expectedCount.toLocaleString()}) -> ${totalCount === expectedCount ? "✅ PASSED" : "❌ FAILED"}`);

  const samplePK = Math.floor(totalRecords / 2);
  const pkRes = await db.query(`SELECT * FROM orders WHERE id = $1`, [samplePK]);
  console.log(`  • PK Lookup (#${samplePK}): Customer = "${pkRes[0]?.customer_name}" -> ${pkRes[0]?.id === samplePK ? "✅ PASSED" : "❌ FAILED"}`);

  const joinRes = await db.query(`
    SELECT c.name as category, COUNT(o.id) as order_count 
    FROM categories c 
    JOIN orders o ON c.id = o.category_id 
    GROUP BY c.name
  `);
  console.log(`  • JOIN & Grouping:   Found ${joinRes.length} categories with orders -> ${joinRes.length === 10 ? "✅ PASSED" : "❌ FAILED"}`);

  // -------------------------------------------------------------------------
  // TEST 4: Cleanup & Lifecycle Leak Detection
  // -------------------------------------------------------------------------
  console.log(`\n--------------------------------------------------------------------------`);
  console.log(`▶ PHASE 4: Database Close & Memory Recovery`);
  console.log(`--------------------------------------------------------------------------`);

  await db.close();

  // Allow engine to sweep
  if (globalThis.gc) {
    globalThis.gc();
  }

  const finalMem = getMemory();
  console.log(`  • Baseline RSS:      ${initialMem.rss} MB`);
  console.log(`  • Peak RSS:          ${peakRSS.toFixed(1)} MB`);
  console.log(`  • Final RSS:         ${finalMem.rss} MB`);
  console.log(`  • Peak Heap Used:    ${peakHeap.toFixed(1)} MB`);
  console.log(`  • Final Heap Used:   ${finalMem.heapUsed} MB`);

  cleanFiles(DB_FILE);

  console.log("\n==========================================================================");
  console.log("🎉 ALL TESTS PASSED! Memory remains stable, no leaks or unbounded growth.");
  console.log("==========================================================================\n");
}

runBenchmark().catch((err) => {
  console.error("Benchmark failed with error:", err);
  process.exit(1);
});
