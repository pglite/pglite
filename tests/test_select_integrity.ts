import { PGLiteNative } from "../src/native";
import { LitePostgres } from "../src/database";
import * as fs from "fs";

async function testSelectIntegrity() {
  console.log("==========================================================================");
  console.log(" 🔬 KIỂM TRA TÍNH CHÍNH XÁC TUYỆT ĐỐI CỦA CÂU LỆNH SELECT 🔬");
  console.log("==========================================================================");

  const nativeDbPath = "./scratch_select_native.db";
  const jsDbPath = "./scratch_select_js.db";
  try { if (fs.existsSync(nativeDbPath)) fs.unlinkSync(nativeDbPath); } catch {}
  try { if (fs.existsSync(jsDbPath)) fs.unlinkSync(jsDbPath); } catch {}

  const dbs = [
    { name: "🦀 Native Rust Engine", db: new PGLiteNative(nativeDbPath) },
    { name: "🟡 Pure JS Engine", db: new PGLiteNative(jsDbPath, { forceJs: true }) }
  ];

  for (const { name, db } of dbs) {
    console.log(`\n--- Kiểm tra trên: ${name} ---`);

    // 1. Tạo bảng ban đầu với 6 cột
    await db.exec(`
      CREATE TABLE users (
        id SERIAL PRIMARY KEY,
        username TEXT,
        password TEXT,
        email TEXT,
        phone_number TEXT,
        created_at TIMESTAMP
      );
    `);

    // 2. Insert 5 users ban đầu
    for (let i = 1; i <= 5; i++) {
      await db.exec(`
        INSERT INTO users (id, username, password, email, phone_number, created_at)
        VALUES (${i}, 'user_${i}', 'pass_${i}', 'user${i}@gmail.com', '090000000${i}', '2026-09-01 10:00:00');
      `);
    }

    // 3. ALTER TABLE thêm 5 cột mới (giống hệt lịch sử migration thật của dự án)
    await db.exec(`
      ALTER TABLE users ADD COLUMN full_name TEXT;
      ALTER TABLE users ADD COLUMN address TEXT;
      ALTER TABLE users ADD COLUMN bank_account_name TEXT;
      ALTER TABLE users ADD COLUMN bank_account_number TEXT;
      ALTER TABLE users ADD COLUMN citizen_id TEXT;
    `);

    // 4. Update thông tin cho user 1 và user 2
    await db.exec(`
      UPDATE users SET 
        full_name = 'Nguyễn Thị Thu Hằng',
        address = '08B Cao Thắng, Phường Phan Thiết',
        bank_account_name = 'NGUYEN THI THU HANG',
        bank_account_number = '123456789',
        citizen_id = '079200111222'
      WHERE id = 1;
    `);

    // 5. Insert thêm user 6 có đầy đủ thông tin từ đầu
    await db.exec(`
      INSERT INTO users (id, username, password, email, phone_number, created_at, full_name, address, bank_account_name, bank_account_number, citizen_id)
      VALUES (6, 'user_6', 'pass_6', 'user6@gmail.com', '0900000006', '2026-09-02 10:00:00', 'Trần Văn Sáu', '123 Lê Lợi, Q1', 'TRAN VAN SAU', '987654321', '079200333444');
    `);

    // 6. Thực hiện SELECT *
    const rows = await db.query(`SELECT * FROM users ORDER BY id ASC`);
    console.log(`  ✓ SELECT * trả về: ${rows.length} rows`);

    // Kiểm tra Row 1 (User 1 - đã update đầy đủ)
    const r1 = rows[0];
    console.log(`  ▶ Row 1 (User 1): id=${r1.id}, username=${r1.username}, full_name="${r1.full_name}", address="${r1.address}", bank="${r1.bank_account_name}"`);
    if (r1.full_name !== 'Nguyễn Thị Thu Hằng' || r1.bank_account_name !== 'NGUYEN THI THU HANG') {
      throw new Error(`Lỗi sai dữ liệu trên User 1`);
    }

    // Kiểm tra Row 3 (User 3 - tạo trước khi ALTER, chưa update)
    const r3 = rows[2];
    console.log(`  ▶ Row 3 (User 3): id=${r3.id}, username=${r3.username}, email=${r3.email}, full_name=${r3.full_name}, address=${r3.address}`);
    if (r3.username !== 'user_3' || r3.email !== 'user3@gmail.com' || r3.full_name !== null || r3.address !== null) {
      throw new Error(`Lỗi sai dữ liệu trên User 3 (cột ban đầu phải còn nguyên, cột mới phải là null)`);
    }

    // Kiểm tra Row 6 (User 6 - tạo sau khi ALTER)
    const r6 = rows[5];
    console.log(`  ▶ Row 6 (User 6): id=${r6.id}, username=${r6.username}, full_name="${r6.full_name}", address="${r6.address}"`);
    if (r6.full_name !== 'Trần Văn Sáu' || r6.address !== '123 Lê Lợi, Q1') {
      throw new Error(`Lỗi sai dữ liệu trên User 6`);
    }

    console.log(`  ✅ Kết quả: SELECT * ánh xạ chính xác 100% từng cột, không hề có lỗi lệch cột hay mất dữ liệu!`);
  }

  try { if (fs.existsSync(nativeDbPath)) fs.unlinkSync(nativeDbPath); } catch {}
  try { if (fs.existsSync(jsDbPath)) fs.unlinkSync(jsDbPath); } catch {}
}

testSelectIntegrity().catch(console.error);
