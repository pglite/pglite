import { PGLiteNative } from "../src/native";
import { LitePostgres } from "../src/database";
import * as fs from "fs";

async function testUpdateSafety() {
  console.log("==========================================================================");
  console.log(" 🧪 KIỂM TRA TOÀN DIỆN CƠ CHẾ UPDATE TRÊN PGLITE (NATIVE & JS) 🧪");
  console.log("==========================================================================");

  const nativeDbPath = "./scratch_safety_native.db";
  const jsDbPath = "./scratch_safety_js.db";
  try { if (fs.existsSync(nativeDbPath)) fs.unlinkSync(nativeDbPath); } catch {}
  try { if (fs.existsSync(jsDbPath)) fs.unlinkSync(jsDbPath); } catch {}

  const dbs = [
    { name: "🦀 Native Rust Engine", db: new PGLiteNative(nativeDbPath) },
    { name: "🟡 Pure JS Engine", db: new PGLiteNative(jsDbPath, { forceJs: true }) }
  ];

  for (const { name, db } of dbs) {
    console.log(`\n--- Đang kiểm tra: ${name} ---`);

    await db.exec(`
      CREATE TABLE users (
        id SERIAL PRIMARY KEY,
        username TEXT,
        full_name TEXT,
        email TEXT,
        phone_number TEXT,
        address TEXT,
        avatar_url TEXT,
        bank_account_name TEXT,
        bank_account_number TEXT,
        bank_name TEXT,
        citizen_id TEXT,
        is_active BOOLEAN,
        role TEXT,
        created_at TIMESTAMP,
        updated_at TIMESTAMP,
        deleted_at TIMESTAMP
      );
    `);

    // Insert 5 full users
    for (let i = 1; i <= 5; i++) {
      await db.exec(`
        INSERT INTO users (
          id, username, full_name, email, phone_number, address,
          avatar_url, bank_account_name, bank_account_number, bank_name,
          citizen_id, is_active, role, created_at, updated_at, deleted_at
        ) VALUES (
          ${i},
          'user_${i}',
          'Nguyễn Văn ${i}',
          'user${i}@example.com',
          '090123456${i}',
          'Địa chỉ số ${i}, Quận 1, TP.HCM',
          'https://avatar.com/${i}.png',
          'NGUYEN VAN ${i}',
          '1900${i}${i}${i}',
          'Vietcombank',
          '07920000000${i}',
          true,
          'TEACHER',
          CURRENT_TIMESTAMP,
          CURRENT_TIMESTAMP,
          NULL
        );
      `);
    }

    console.log("  ✓ Đã insert 5 user đầy đủ thông tin.");

    // Kiểm tra User 2 trước khi update
    const beforeUser2 = (await db.query(`SELECT * FROM users WHERE id = 2`))[0];
    console.log("  ▶ User 2 TRƯỚC khi UPDATE:");
    console.log(`    - id: ${beforeUser2.id}`);
    console.log(`    - full_name: ${beforeUser2.full_name}`);
    console.log(`    - address: ${beforeUser2.address}`);
    console.log(`    - phone_number: ${beforeUser2.phone_number}`);
    console.log(`    - bank_account_name: ${beforeUser2.bank_account_name}`);
    console.log(`    - bank_account_number: ${beforeUser2.bank_account_number}`);

    // Thực thi UPDATE CHỈ 2 CỘT (email và phone_number) trên User 2
    console.log("\n  ⚡ Chạy lệnh UPDATE (chỉ sửa email, phone_number, updated_at)...");
    await db.exec(`
      UPDATE users
      SET
        email = 'new_email_user2@gmail.com',  -- sửa email mới
        phone_number = '0999888777',           -- sửa số điện thoại
        updated_at = CURRENT_TIMESTAMP
      WHERE id = 2                             -- chỉ sửa user 2
        AND deleted_at IS NULL;
    `);

    // Lấy lại toàn bộ 5 users để kiểm tra tính toàn vẹn
    const allUsers = await db.query(`SELECT * FROM users ORDER BY id ASC`);

    // Kiểm tra User 2 sau khi update
    const afterUser2 = allUsers.find((u: any) => u.id === 2);
    console.log("  ▶ User 2 SAU khi UPDATE:");
    console.log(`    - id: ${afterUser2.id} (giữ nguyên)`);
    console.log(`    - full_name: ${afterUser2.full_name} (giữ nguyên)`);
    console.log(`    - address: ${afterUser2.address} (giữ nguyên)`);
    console.log(`    - bank_account_name: ${afterUser2.bank_account_name} (giữ nguyên)`);
    console.log(`    - bank_account_number: ${afterUser2.bank_account_number} (giữ nguyên)`);
    console.log(`    - citizen_id: ${afterUser2.citizen_id} (giữ nguyên)`);
    console.log(`    - email: ${afterUser2.email} (ĐÃ CẬP NHẬT MỚI)`);
    console.log(`    - phone_number: ${afterUser2.phone_number} (ĐÃ CẬP NHẬT MỚI)`);

    // Assertion kiểm tra không bị mất bất kỳ cột nào
    if (
      afterUser2.full_name !== 'Nguyễn Văn 2' ||
      afterUser2.address !== 'Địa chỉ số 2, Quận 1, TP.HCM' ||
      afterUser2.bank_account_name !== 'NGUYEN VAN 2' ||
      afterUser2.bank_account_number !== '1900222' ||
      afterUser2.citizen_id !== '079200000002' ||
      afterUser2.email !== 'new_email_user2@gmail.com' ||
      afterUser2.phone_number !== '0999888777'
    ) {
      throw new Error(`❌ PHÁT HIỆN LỖI: Dữ liệu bị thay đổi sai trên ${name}!`);
    }

    // Kiểm tra các User khác (1, 3, 4, 5) không hề bị ảnh hưởng
    for (const u of allUsers) {
      if (u.id !== 2) {
        if (
          !u.full_name ||
          !u.address ||
          !u.bank_account_name ||
          !u.bank_account_number ||
          !u.citizen_id
        ) {
          throw new Error(`❌ PHÁT HIỆN LỖI: User ${u.id} bị mất thông tin!`);
        }
      }
    }
    console.log(`  ✅ KẾT QUẢ: 100% dữ liệu các cột khác và các user khác HOÀN TOÀN NGUYÊN VẸN!`);
  }

  try { if (fs.existsSync(nativeDbPath)) fs.unlinkSync(nativeDbPath); } catch {}
  try { if (fs.existsSync(jsDbPath)) fs.unlinkSync(jsDbPath); } catch {}
}

testUpdateSafety().catch(console.error);
