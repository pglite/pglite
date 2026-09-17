import { LitePostgres } from "../src/database";
import { NodeFSAdapter } from "../src/adapters/node";
import * as fs from "fs";

interface MemSnapshot {
  rss: number;
  heapUsed: number;
  heapTotal: number;
  external: number;
}

function getMem(): MemSnapshot {
  const m = process.memoryUsage();
  return {
    rss: Math.round((m.rss / 1024 / 1024) * 100) / 100,
    heapUsed: Math.round((m.heapUsed / 1024 / 1024) * 100) / 100,
    heapTotal: Math.round((m.heapTotal / 1024 / 1024) * 100) / 100,
    external: Math.round((m.external / 1024 / 1024) * 100) / 100,
  };
}

function cleanDB(name: string) {
  for (const ext of ["", ".wal"]) {
    const f = name + ext;
    if (fs.existsSync(f)) {
      try { fs.unlinkSync(f); } catch {}
    }
  }
}

async function runNgheAnTest() {
  console.log("================================================================================");
  console.log("  🏫 THỰC NGHIỆM: Đo Đạc RAM & Thời Gian Import Dữ Liệu NGHE_AN.json");
  console.log("================================================================================\n");

  const rawJson = fs.readFileSync("sample/NGHE_AN.json", "utf-8");
  const data = JSON.parse(rawJson);

  let totalSchools = 0;
  let totalClasses = 0;
  for (const r of data) {
    totalSchools += r.schools?.length || 0;
    for (const s of r.schools || []) {
      totalClasses += s.classes?.length || 0;
    }
  }

  console.log(`📦 Thống kê dữ liệu đầu vào từ NGHE_AN.json:`);
  console.log(`   • Dung lượng file JSON: ${(rawJson.length / 1024).toFixed(1)} KB`);
  console.log(`   • Số Vùng (Regions):    ${data.length}`);
  console.log(`   • Số Trường (Schools):  ${totalSchools}`);
  console.log(`   • Số Lớp (Classes):     ${totalClasses}`);
  console.log(`   • Tổng thực thể:        ${data.length + totalSchools + totalClasses}`);
  console.log("--------------------------------------------------------------------------------\n");

  const DB_NAME = "nghe_an_benchmark.db";
  cleanDB(DB_NAME);

  if (globalThis.gc) globalThis.gc();
  const baselineMem = getMem();
  console.log(`📊 RAM Ban Đầu (Baseline):`);
  console.log(`   • RSS:       ${baselineMem.rss} MB`);
  console.log(`   • Heap Used: ${baselineMem.heapUsed} MB\n`);

  const db = new LitePostgres(DB_NAME, { adapter: new NodeFSAdapter() });

  // Tạo cấu trúc 4 bảng có quan hệ Foreign Key đầy đủ
  await db.exec(`
    CREATE TABLE provinces (
      id SERIAL PRIMARY KEY,
      name TEXT
    );

    CREATE TABLE regions (
      id SERIAL PRIMARY KEY,
      region_id INT,
      name TEXT,
      province_id INT REFERENCES provinces(id)
    );

    CREATE TABLE schools (
      id SERIAL PRIMARY KEY,
      school_id INT,
      name TEXT,
      address TEXT,
      latitude TEXT,
      longitude TEXT,
      region_id INT REFERENCES regions(id)
    );

    CREATE TABLE classes (
      id SERIAL PRIMARY KEY,
      name TEXT,
      description TEXT,
      student_count TEXT,
      school_id INT REFERENCES schools(id)
    );
  `);

  await db.exec("INSERT INTO provinces (id, name) VALUES (1, 'Nghệ An')");

  let peakRSS = 0;
  let peakHeap = 0;

  function trackPeak() {
    const cur = getMem();
    if (cur.rss > peakRSS) peakRSS = cur.rss;
    if (cur.heapUsed > peakHeap) peakHeap = cur.heapUsed;
  }

  trackPeak();

  // ---------------------------------------------------------------------------
  // THỰC NGHIỆM: Import toàn bộ dữ liệu NGHE_AN.json
  // Mô phỏng đúng cách thức Next.js Server Action / Kysely Repository xử lý
  // ---------------------------------------------------------------------------
  console.log("🚀 Bắt đầu quá trình Import vào PGLite Database...");
  const startTime = performance.now();

  await db.exec("BEGIN");

  for (const reg of data) {
    const regRes = await db.query(
      "INSERT INTO regions (region_id, name, province_id) VALUES ($1, $2, $3) RETURNING id",
      [reg.region_id ?? null, reg.region, 1]
    );
    const regDbId = regRes[0].id;

    for (const sc of reg.schools || []) {
      const scRes = await db.query(
        "INSERT INTO schools (school_id, name, address, latitude, longitude, region_id) VALUES ($1, $2, $3, $4, $5, $6) RETURNING id",
        [sc.school_id ?? null, sc.school, sc.address, sc.latitude, sc.longitude, regDbId]
      );
      const scDbId = scRes[0].id;

      for (const cl of sc.classes || []) {
        await db.exec(
          "INSERT INTO classes (name, description, student_count, school_id) VALUES ($1, $2, $3, $4)",
          [cl.name, cl.description, cl.student_count, scDbId]
        );
        trackPeak();
      }
    }
  }

  await db.exec("COMMIT");
  const endTime = performance.now();
  trackPeak();

  const durationMs = (endTime - startTime);
  const durationSec = durationMs / 1000;
  const afterImportMem = getMem();

  // Kiểm tra tính chính xác dữ liệu trong DB
  const rCount = await db.query("SELECT COUNT(*) as cnt FROM regions");
  const sCount = await db.query("SELECT COUNT(*) as cnt FROM schools");
  const cCount = await db.query("SELECT COUNT(*) as cnt FROM classes");

  console.log("\n================================================================================");
  console.log("  🏁 KẾT QUẢ ĐO ĐẠC THỰC TẾ");
  console.log("================================================================================");
  console.log(`⏱ THỜI GIAN THỰC THI:`);
  console.log(`   • Tổng thời gian:     ${durationMs.toFixed(2)} ms (~${durationSec.toFixed(3)} giây)`);
  console.log(`   • Tốc độ xử lý:       ${Math.round((data.length + totalSchools + totalClasses) / durationSec).toLocaleString()} records/giây`);

  console.log(`\n🧠 BỘ NHỚ RAM TIÊU THỤ:`);
  console.log(`   • RAM Ban đầu (RSS):  ${baselineMem.rss} MB (Heap: ${baselineMem.heapUsed} MB)`);
  console.log(`   • RAM Đỉnh điểm (RSS): ${peakRSS.toFixed(1)} MB (Heap đỉnh: ${peakHeap.toFixed(1)} MB)`);
  console.log(`   • RAM Sau import:     ${afterImportMem.rss} MB (Heap: ${afterImportMem.heapUsed} MB)`);
  console.log(`   • Mức RAM tăng thêm:  +${(peakRSS - baselineMem.rss).toFixed(1)} MB`);

  console.log(`\n✅ KIỂM TRA TOÀN VẸN DỮ LIỆU ĐÃ LƯU:`);
  console.log(`   • Regions trong DB:   ${rCount[0].cnt} / ${data.length} (${rCount[0].cnt == data.length ? "Khớp 100%" : "LỆCH"})`);
  console.log(`   • Schools trong DB:   ${sCount[0].cnt} / ${totalSchools} (${sCount[0].cnt == totalSchools ? "Khớp 100%" : "LỆCH"})`);
  console.log(`   • Classes trong DB:   ${cCount[0].cnt} / ${totalClasses} (${cCount[0].cnt == totalClasses ? "Khớp 100%" : "LỆCH"})`);

  // Kiểm tra thử 1 câu truy vấn JOIN phức tạp
  const testJoin = await db.query(`
    SELECT r.name as region, COUNT(c.id) as total_classes
    FROM regions r
    JOIN schools s ON s.region_id = r.id
    JOIN classes c ON c.school_id = s.id
    GROUP BY r.name
    ORDER BY total_classes DESC
    LIMIT 3
  `);
  console.log(`\n🔍 Top 3 Vùng có nhiều lớp nhất (Query JOIN thực tế):`);
  for (const item of testJoin) {
    console.log(`   • ${item.region}: ${item.total_classes} lớp`);
  }

  await db.close();
  cleanDB(DB_NAME);

  console.log("\n================================================================================\n");
}

runNgheAnTest().catch(console.error);
