
import { LitePostgres } from "../src/database";
import { unlinkSync, existsSync } from "fs";
import { NodeFSAdapter } from "../src/adapters/node";

async function runBenchmark() {
  const DB_FILE = "benchmark.db";
  
  if (existsSync(DB_FILE)) unlinkSync(DB_FILE);
  if (existsSync(DB_FILE + ".wal")) unlinkSync(DB_FILE + ".wal");

  const db = new LitePostgres(DB_FILE, {
    adapter: new NodeFSAdapter(),
  });

  console.log("\n🚀 LitePostgres Engine | Performance Benchmark Suite");
  console.log("================================================================");

  await db.exec(`
    CREATE TABLE benchmark_users (
      id SERIAL PRIMARY KEY,
      name TEXT,
      age INTEGER,
      active BOOLEAN
    )
  `);

  const totalRecords = 1_000_000;
  const batchSize = 1000;
  console.log(`\n[1/6] PHASE: Bulk Data Ingestion`);
  console.log(`      Action: Inserting ${totalRecords.toLocaleString()} records`);
  console.log(`      Config: Batch Size = ${batchSize}`);
  
  const startTime = Date.now();
  await db.exec("BEGIN");
  
  for (let i = 0; i < totalRecords; i += batchSize) {
    const placeholders = [];
    const params = [];
    for (let j = 0; j < batchSize; j++) {
      const idx = i + j + 1;
      const offset = j * 3;
      placeholders.push(`(${offset + 1}, ${offset + 2}, ${offset + 3})` as never);
      params.push(`User_${idx}` as never, idx % 100 as never, (idx % 2 === 0) as unknown as never);
    }
    
    await db.exec(
      `INSERT INTO benchmark_users (name, age, active) VALUES ${placeholders.join(", ")}`, 
      params
    );
    
    if ((i + batchSize) % 5000 === 0 || (i + batchSize) === totalRecords) {
      const elapsed = (Date.now() - startTime) / 1000 || 0.001;
      const speed = Math.floor((i + batchSize) / elapsed);
      const percent = (((i + batchSize) / totalRecords) * 100).toFixed(1);
      process.stdout.write(`\r    ⏳ Progress: ${percent}% | Ingested: ${(i + batchSize).toLocaleString()} | Throughput: ${speed.toLocaleString()} ops/sec`);
    }
  }
  
  await db.exec("COMMIT");
  const insertTime = Date.now() - startTime;
  
  console.log(`\n\n✅ Ingestion Complete`);
  console.log(`   ⏱  Total Duration: ${(insertTime / 1000).toFixed(2)}s`);
  console.log(`   📈 Avg Throughput: ${Math.floor(totalRecords / (insertTime / 1000)).toLocaleString()} ops/sec\n`);

  console.log(`[2/6] PHASE: Point Lookup`);
  console.log(`      Action: SELECT by Primary Key (id=500,000)`);
  const qStartTime1 = Date.now();
  const res1 = await db.query(`SELECT * FROM benchmark_users WHERE id = $1`, [500000]);
  const qTime1 = Date.now() - qStartTime1;
  console.log(`   ↳ Result: ${res1[0]?.name}`);
  console.log(`   ⏱  Latency: ${qTime1}ms\n`);

  console.log(`[3/6] PHASE: Full Table Aggregation`);
  console.log(`      Action: SELECT COUNT(*)`);
  const qStartTime2 = Date.now();
  const res2 = await db.query(`SELECT COUNT(*) as total FROM benchmark_users`);
  const qTime2 = Date.now() - qStartTime2;
  console.log(`   ↳ Total Records: ${res2[0]?.total.toLocaleString()}`);
  console.log(`   ⏱  Latency: ${(qTime2 / 1000).toFixed(2)}s\n`);

  console.log(`[4/6] PHASE: Filtered Scan`);
  console.log(`      Action: Complex SELECT with multiple WHERE conditions`);
  const qStartTime3 = Date.now();
  const res3 = await db.query(`SELECT COUNT(*) as active_users FROM benchmark_users WHERE active = true AND age > 50`);
  const qTime3 = Date.now() - qStartTime3;
  console.log(`   ↳ Match Count: ${res3[0]?.active_users.toLocaleString()}`);
  console.log(`   ⏱  Latency: ${(qTime3 / 1000).toFixed(2)}s\n`);

  console.log(`[5/6] PHASE: Atomic Mutation`);
  console.log(`      Action: UPDATE record by Primary Key`);
  const uStartTime = Date.now();
  await db.exec(`UPDATE benchmark_users SET age = $1 WHERE id = $2`, [99, 500000]);
  const uTime = Date.now() - uStartTime;
  console.log(`   ⏱  Latency: ${uTime}ms\n`);

  console.log(`[6/6] PHASE: Record Deletion`);
  console.log(`      Action: DELETE record by Primary Key`);
  const dStartTime = Date.now();
  await db.exec(`DELETE FROM benchmark_users WHERE id = $1`, [500000]);
  const dTime = Date.now() - dStartTime;
  console.log(`   ⏱  Latency: ${dTime}ms\n`);

  await db.close();
  
  if (existsSync(DB_FILE)) unlinkSync(DB_FILE);
  if (existsSync(DB_FILE + ".wal")) unlinkSync(DB_FILE + ".wal");
  
  console.log("✨ Performance Suite Completed Successfully!\n");
}

runBenchmark().catch(console.error);