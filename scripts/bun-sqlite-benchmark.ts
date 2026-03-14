import { Database } from "bun:sqlite";
import { unlinkSync, existsSync } from "fs";

async function runBenchmark() {
  const DB_FILE = "benchmark_bun.db";
  
  // Dọn dẹp file cũ
  if (existsSync(DB_FILE)) unlinkSync(DB_FILE);
  if (existsSync(DB_FILE + "-wal")) unlinkSync(DB_FILE + "-wal");
  if (existsSync(DB_FILE + "-shm")) unlinkSync(DB_FILE + "-shm");

  const db = new Database(DB_FILE);
  
  // Tối ưu hóa SQLite cho tốc độ thực thi
  db.run("PRAGMA journal_mode = WAL;");
  db.run("PRAGMA synchronous = NORMAL;");

  console.log("\n🚀 Bun SQLite Engine | Performance Benchmark Suite");
  console.log("================================================================");

  db.run(`
    CREATE TABLE benchmark_users (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      name TEXT,
      age INTEGER,
      active INTEGER -- SQLite không có BOOLEAN thực thụ, dùng INTEGER (0, 1)
    )
  `);

  const totalRecords = 1_000_000;
  const batchSize = 1000;
  console.log(`\n[1/6] PHASE: Bulk Data Ingestion`);
  console.log(`      Action: Inserting ${totalRecords.toLocaleString()} records`);
  console.log(`      Config: Using Transaction + Prepared Statement`);
  
  const startTime = Date.now();

  // Chuẩn bị Statement trước để đạt tốc độ tối đa
  const insertStmt = db.prepare(`
    INSERT INTO benchmark_users (name, age, active) VALUES (?, ?, ?)
  `);

  // Bun SQLite hỗ trợ transaction cực nhanh
  const insertMany = db.transaction((records) => {
    for (const row of records) {
      insertStmt.run(row.name, row.age, row.active);
    }
  });

  for (let i = 0; i < totalRecords; i += batchSize) {
    const batch = [];
    for (let j = 0; j < batchSize; j++) {
      const idx = i + j + 1;
      batch.push({
        name: `User_${idx}`,
        age: idx % 100,
        active: idx % 2 === 0 ? 1 : 0
      });
    }
    
    insertMany(batch);
    
    if ((i + batchSize) % 50000 === 0 || (i + batchSize) === totalRecords) {
      const elapsed = (Date.now() - startTime) / 1000 || 0.001;
      const speed = Math.floor((i + batchSize) / elapsed);
      const percent = (((i + batchSize) / totalRecords) * 100).toFixed(1);
      process.stdout.write(`\r    ⏳ Progress: ${percent}% | Ingested: ${(i + batchSize).toLocaleString()} | Throughput: ${speed.toLocaleString()} ops/sec`);
    }
  }
  
  const insertTime = Date.now() - startTime;
  
  console.log(`\n\n✅ Ingestion Complete`);
  console.log(`   ⏱  Total Duration: ${(insertTime / 1000).toFixed(2)}s`);
  console.log(`   📈 Avg Throughput: ${Math.floor(totalRecords / (insertTime / 1000)).toLocaleString()} ops/sec\n`);

  console.log(`[2/6] PHASE: Point Lookup`);
  console.log(`      Action: SELECT by Primary Key (id=500,000)`);
  const qStartTime1 = Date.now();
  const res1 = db.query(`SELECT * FROM benchmark_users WHERE id = ?`).get(500000) as any;
  const qTime1 = Date.now() - qStartTime1;
  console.log(`   ↳ Result: ${res1?.name}`);
  console.log(`   ⏱  Latency: ${qTime1}ms\n`);

  console.log(`[3/6] PHASE: Full Table Aggregation`);
  console.log(`      Action: SELECT COUNT(*)`);
  const qStartTime2 = Date.now();
  const res2 = db.query(`SELECT COUNT(*) as total FROM benchmark_users`).get() as any;
  const qTime2 = Date.now() - qStartTime2;
  console.log(`   ↳ Total Records: ${res2?.total.toLocaleString()}`);
  console.log(`   ⏱  Latency: ${(qTime2 / 1000).toFixed(2)}s\n`);

  console.log(`[4/6] PHASE: Filtered Scan`);
  console.log(`      Action: Complex SELECT with multiple WHERE conditions`);
  const qStartTime3 = Date.now();
  const res3 = db.query(`SELECT COUNT(*) as active_users FROM benchmark_users WHERE active = 1 AND age > 50`).get() as any;
  const qTime3 = Date.now() - qStartTime3;
  console.log(`   ↳ Match Count: ${res3?.active_users.toLocaleString()}`);
  console.log(`   ⏱  Latency: ${(qTime3 / 1000).toFixed(2)}s\n`);

  console.log(`[5/6] PHASE: Atomic Mutation`);
  console.log(`      Action: UPDATE record by Primary Key`);
  const uStartTime = Date.now();
  db.run(`UPDATE benchmark_users SET age = 99 WHERE id = 500000`);
  const uTime = Date.now() - uStartTime;
  console.log(`   ⏱  Latency: ${uTime}ms\n`);

  console.log(`[6/6] PHASE: Record Deletion`);
  console.log(`      Action: DELETE record by Primary Key`);
  const dStartTime = Date.now();
  db.run(`DELETE FROM benchmark_users WHERE id = 500000`);
  const dTime = Date.now() - dStartTime;
  console.log(`   ⏱  Latency: ${dTime}ms\n`);

  db.close();
  
  if (existsSync(DB_FILE)) unlinkSync(DB_FILE);
  if (existsSync(DB_FILE + "-wal")) unlinkSync(DB_FILE + "-wal");
  if (existsSync(DB_FILE + "-shm")) unlinkSync(DB_FILE + "-shm");
  
  console.log("✨ Bun SQLite Performance Suite Completed Successfully!\n");
}

runBenchmark().catch(console.error);