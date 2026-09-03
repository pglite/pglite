import { LitePostgres } from "../src/database";
import { NodeFSAdapter } from "../src/adapters/node";
import * as fs from "fs";

async function runTest() {
  const dbFile = "test_class_perf.db";
  try {
    fs.unlinkSync(dbFile);
    fs.unlinkSync(dbFile + ".wal");
  } catch {}

  console.log("================================================================================");
  console.log("  ⚡ BENCHMARK QUERY: classes LEFT JOIN schools");
  console.log("================================================================================\n");

  const rawJson = fs.readFileSync("sample/NGHE_AN.json", "utf-8");
  const data = JSON.parse(rawJson);

  const db = new LitePostgres(dbFile, { adapter: new NodeFSAdapter() });

  // 1. Setup schema
  await db.exec(`
    CREATE TABLE schools (
      id SERIAL PRIMARY KEY,
      school_id INT,
      name TEXT,
      address TEXT
    );

    CREATE TABLE classes (
      id SERIAL PRIMARY KEY,
      name TEXT,
      description TEXT,
      student_count TEXT,
      school_id INT,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
  `);

  // 2. Insert data from NGHE_AN.json
  await db.exec("BEGIN");
  let classCount = 0;
  let schoolCount = 0;

  for (const reg of data) {
    for (const sc of reg.schools || []) {
      const scRes = await db.query(
        "INSERT INTO schools (name, address) VALUES ($1, $2) RETURNING id",
        [sc.school, sc.address]
      );
      const scDbId = scRes[0].id;
      schoolCount++;

      for (const cl of sc.classes || []) {
        await db.exec(
          "INSERT INTO classes (name, description, student_count, school_id) VALUES ($1, $2, $3, $4)",
          [cl.name, cl.description, cl.student_count, scDbId]
        );
        classCount++;
      }
    }
  }
  await db.exec("COMMIT");

  console.log(`✅ Đã nạp dữ liệu: ${schoolCount} trường, ${classCount} lớp.\n`);

  const TARGET_QUERY = `
    SELECT 
      "classes"."id", 
      "classes"."name", 
      "classes"."description", 
      "classes"."school_id" as "schoolId", 
      "schools"."name" as "schoolName", 
      COALESCE(CAST(classes.student_count AS INTEGER), 0) as "studentCount", 
      "classes"."created_at" as "createdAt" 
    FROM "classes" 
    LEFT JOIN "schools" ON "schools"."id" = "classes"."school_id"
  `;

  // Test 1: Cold run (lần đầu tiên thực thi câu query)
  const coldStart = performance.now();
  const coldRes = await db.query(TARGET_QUERY);
  const coldDuration = performance.now() - coldStart;
  console.log(`❄️ Cold Query (Lần đầu chạy): ${coldDuration.toFixed(2)} ms (Trả về ${coldRes.length} dòng)`);

  // Test 2: Warm runs (chạy lặp lại 50 lần để đo phân phối thời gian)
  const warmTimes: number[] = [];
  const iterations = 50;
  for (let i = 0; i < iterations; i++) {
    const t0 = performance.now();
    await db.query(TARGET_QUERY);
    warmTimes.push(performance.now() - t0);
  }

  warmTimes.sort((a, b) => a - b);
  const avg = warmTimes.reduce((a, b) => a + b, 0) / warmTimes.length;
  const min = warmTimes[0];
  const max = warmTimes[warmTimes.length - 1];
  const p50 = warmTimes[Math.floor(warmTimes.length * 0.5)];
  const p95 = warmTimes[Math.floor(warmTimes.length * 0.95)];

  console.log(`🔥 Warm Query (Lặp lại ${iterations} lần):`);
  console.log(`   • Avg: ${avg.toFixed(2)} ms`);
  console.log(`   • Min: ${min.toFixed(2)} ms`);
  console.log(`   • P50: ${p50.toFixed(2)} ms`);
  console.log(`   • P95: ${p95.toFixed(2)} ms`);
  console.log(`   • Max: ${max.toFixed(2)} ms\n`);

  // Test 3: Có LIMIT 20 (phân trang thông thường)
  const limitQuery = TARGET_QUERY + " LIMIT 20";
  const limitTimes: number[] = [];
  for (let i = 0; i < iterations; i++) {
    const t0 = performance.now();
    await db.query(limitQuery);
    limitTimes.push(performance.now() - t0);
  }
  const avgLimit = limitTimes.reduce((a, b) => a + b, 0) / limitTimes.length;
  console.log(`📄 Có LIMIT 20 (Phân trang): Avg ${avgLimit.toFixed(2)} ms (Min: ${Math.min(...limitTimes).toFixed(2)} ms)`);

  // Test 4: Bỏ COALESCE + CAST (chỉ select raw student_count)
  const rawQuery = `
    SELECT 
      "classes"."id", 
      "classes"."name", 
      "classes"."description", 
      "classes"."school_id" as "schoolId", 
      "schools"."name" as "schoolName", 
      classes.student_count as "studentCount", 
      "classes"."created_at" as "createdAt" 
    FROM "classes" 
    LEFT JOIN "schools" ON "schools"."id" = "classes"."school_id"
  `;
  const rawTimes: number[] = [];
  for (let i = 0; i < iterations; i++) {
    const t0 = performance.now();
    await db.query(rawQuery);
    rawTimes.push(performance.now() - t0);
  }
  const avgRaw = rawTimes.reduce((a, b) => a + b, 0) / rawTimes.length;
  console.log(`⚡ Bỏ COALESCE(CAST(...)): Avg ${avgRaw.toFixed(2)} ms (Ảnh hưởng của CAST trên ${classCount} dòng: ${(avg - avgRaw).toFixed(2)} ms)`);

  // Test 5: Khi có INDEX trên classes(school_id)
  console.log("\n🔨 Tạo INDEX trên classes(school_id)...");
  await db.exec("CREATE INDEX idx_classes_school_id ON classes(school_id)");
  const indexTimes: number[] = [];
  for (let i = 0; i < iterations; i++) {
    const t0 = performance.now();
    await db.query(TARGET_QUERY);
    indexTimes.push(performance.now() - t0);
  }
  const avgIndex = indexTimes.reduce((a, b) => a + b, 0) / indexTimes.length;
  console.log(`📌 Sau khi có INDEX: Avg ${avgIndex.toFixed(2)} ms`);

  // Test 6: In-memory (không qua disk adapter)
  const inMemDb = new LitePostgres(":memory:");
  await inMemDb.exec(`
    CREATE TABLE schools (id SERIAL PRIMARY KEY, name TEXT);
    CREATE TABLE classes (id SERIAL PRIMARY KEY, name TEXT, description TEXT, student_count TEXT, school_id INT, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP);
  `);
  await inMemDb.exec("BEGIN");
  for (let i = 1; i <= schoolCount; i++) {
    await inMemDb.exec("INSERT INTO schools (id, name) VALUES ($1, $2)", [i, `School ${i}`]);
  }
  for (let i = 1; i <= classCount; i++) {
    await inMemDb.exec("INSERT INTO classes (name, description, student_count, school_id) VALUES ($1, $2, $3, $4)", [
      `Class ${i}`, `Desc ${i}`, "30", (i % schoolCount) + 1
    ]);
  }
  await inMemDb.exec("COMMIT");

  const inMemTimes: number[] = [];
  for (let i = 0; i < iterations; i++) {
    const t0 = performance.now();
    await inMemDb.query(TARGET_QUERY);
    inMemTimes.push(performance.now() - t0);
  }
  const avgInMem = inMemTimes.reduce((a, b) => a + b, 0) / inMemTimes.length;
  console.log(`🧠 Pure In-Memory DB: Avg ${avgInMem.toFixed(2)} ms`);

  await db.close();
  await inMemDb.close();
  try {
    fs.unlinkSync(dbFile);
    fs.unlinkSync(dbFile + ".wal");
  } catch {}

  console.log("\n================================================================================");
}

runTest().catch(console.error);
