# Lỗi pglite-rs (db-rs) phát hiện khi chạy backend WAIKI

- Ngày test: 2026-10-20
- Bản db-rs: build từ `NATA/backend/db-rs`, có commit `703ff36` ("upgdare pglite")
- Cách test: chạy db-rs local (port 3952, dữ liệu trong thư mục tạm); backend WAIKI dùng `core/db.ts` gửi SQL qua `POST /api/db/query`; chạy toàn bộ migration và seed rồi so 99 action `list*` với bản PGlite WASM gốc
- Kết quả: 80/99 action giống hệt. 19 action khác nhau, đều do các lỗi bên dưới (seed `customers` hỏng nên dữ liệu phụ thuộc khách hàng không có)

Cách gọi để tái hiện:

```bash
curl -s -X POST http://127.0.0.1:3052/api/db/query -H 'content-type: application/json' \
  -d '{"db_code":"repro","sql":"<SQL>","params":[...]}'
```

---

## Lỗi 1: bảng dẫn xuất `(VALUES ...)` không được hỗ trợ — ĐÃ SỬA (kiểm tra lại 2026-10-20 với bản `49af97425`)

| Trường hợp | SQLSTATE | Thông báo |
|---|---|---|
| `FROM (VALUES ...) AS v(cols)` | `42P01` | `PGlite-RS Execution error: Table VALUES not found` |
| `CROSS JOIN (VALUES ...) AS p(cols)` | `XX000` | `PGlite-RS Execution error: Failed to parse CTE query` |

Repro tối giản (Postgres trả 2 dòng):

```sql
SELECT v.* FROM (VALUES (1,'a'),(2,'b')) AS v(id,name);

SELECT p.m FROM (SELECT 1 AS x) s CROSS JOIN (VALUES (30),(60)) AS p(m);
```

Câu gốc trong WAIKI:

- `core/customers/lifecycle.ts:137` (seed khách hàng):
  ```sql
  INSERT INTO customers (tenant_id, code, full_name, phone, email, gender, birthday, status, avatar_url)
  SELECT v.* FROM (VALUES
    (1, 'CUST-0005', 'Trần Minh Khoa', '0945678901', 'khoa.tran@example.com', 'male', DATE '1985-06-12', 'active', 'https://...'),
    ...
  ) AS v(...)
  ```
- `core/customers/lifecycle.ts:154`: cũng là `FROM (VALUES ...)`
- `objects/specialists/lifecycle.ts:277`:
  ```sql
  INSERT INTO specialist_session_packages (tenant_id, specialist_id, duration_minutes, price, recommended, order_num)
  SELECT s.tenant_id, s.id, p.minutes,
         ROUND(COALESCE(s.consultation_fee, 0) * 60.0 / GREATEST(s.session_duration_minutes, 15) * p.factor / 10000.0) * 10000,
         p.minutes = 60, p.ord
  FROM specialists s
  CROSS JOIN (VALUES (30, 0.54, 0), (60, 1.0, 1), (90, 1.46, 2)) AS p(minutes, factor, ord)
  WHERE s.deleted_at IS NULL
    AND NOT EXISTS (SELECT 1 FROM specialist_session_packages x WHERE x.specialist_id = s.id AND x.tenant_id = s.tenant_id)
  ```
- `schedules/specialist-schedules/lifecycle.ts:228`: `CROSS JOIN (VALUES (0), (1), (2), (3), (4), (5), (6)) AS d(day)`

---

## Lỗi 2 (nghiêm trọng): subquery vô hướng trong `INSERT ... VALUES` trả về `false` — ĐÃ SỬA (2026-10-21)

> Sửa: subquery (không tương quan) trong `VALUES` được tính bằng chính engine trước khi chèn (`engine/upsert.rs`), áp cho cả `ON CONFLICT`. Cùng đợt cũng sửa `UPDATE ... SET col = (SELECT ... tương quan)` từng ghi `true`/NULL. Golden `BG_03`–`BG_05`, `ZA_11`–`ZA_13`.

Subquery `(SELECT id FROM ... WHERE ...)` đặt trong `VALUES` của `INSERT` được tính thành boolean `false` thay vì giá trị `id`. Cùng subquery đó đặt trong `SELECT` thì ra đúng.

- Bảng **có** khoá ngoại: báo lỗi `23503`.
- Bảng **không có** khoá ngoại: **ghi `false` vào cột INT mà không báo lỗi** (hỏng dữ liệu âm thầm).

Repro:

```sql
CREATE TABLE cat (id SERIAL PRIMARY KEY, code TEXT);
CREATE TABLE item (id SERIAL PRIMARY KEY, cat_id INT REFERENCES cat(id), title TEXT);
CREATE TABLE item2 (id SERIAL PRIMARY KEY, cat_id INT, title TEXT);
INSERT INTO cat (code) VALUES ('a'),('b');

-- Có FK + tham số: Postgres → cat_id = 2
INSERT INTO item (cat_id, title) VALUES ((SELECT id FROM cat WHERE code = $1), $2) RETURNING cat_id;
--   params: ["b","x"]
--   db-rs: code=23503 "insert or update on table "item" violates foreign key constraint "item_cat_id_fkey"
--          (Key (cat_id)=(false) is not present in table "cat".)"

-- Không FK, hằng số: Postgres → cat_id = 2
INSERT INTO item2 (cat_id, title) VALUES ((SELECT id FROM cat WHERE code = 'b'), 'y') RETURNING cat_id;
--   db-rs: thành công, trả [{"cat_id": false}]   ← sai, không báo lỗi

-- Đối chứng: trong SELECT thì đúng
SELECT (SELECT id FROM cat WHERE code = $1) AS cid;   -- params ["b"] → [{"cid": 2}] ✓
```

Câu gốc trong WAIKI (mã lỗi `23503`):

- `wellness/wellness-library/lifecycle.ts:416`:
  ```sql
  INSERT INTO wellness_library_items (tenant_id, category_id, category_code, title, ...)
  VALUES ($1, (SELECT id FROM wellness_library_categories WHERE tenant_id = $2 AND code = $3), $4, $5, ...)
  ```
  → `violates foreign key constraint "wellness_library_items_category_id_fkey" (Key (category_id)=(false) ...)`
- `wellness/wellness-protocols/lifecycle.ts:386`:
  ```sql
  INSERT INTO wellness_protocol_activity_templates (tenant_id, code, title, title_i18n, group_code, description,
    instructions, duration_minutes, performer, staff_role, location_id, safety_level, contraindications, completion)
  VALUES ($1, $2, $3, $4::jsonb, $5, $6, $7, $8, $9, $10,
    (SELECT id FROM wellness_protocol_locations WHERE tenant_id = $11 AND code = $12),
    $13, $14::jsonb, $15::jsonb)
  ON CONFLICT (tenant_id, code) DO NOTHING;
  ```
  → `violates foreign key constraint "wellness_protocol_activity_templates_location_id_fkey" (Key (location_id)=(false) ...)`
- `wellness/wellness-support/lifecycle.ts:102`:
  ```sql
  INSERT INTO support_faqs (tenant_id, code, category_id, question_i18n, answer_i18n, tags, search_i18n, sort_order)
  VALUES ($1, $2, (SELECT id FROM support_faq_categories WHERE tenant_id = $3 AND code = $4),
    $5::jsonb, $6::jsonb, $7::jsonb, $8::jsonb, $9)
  ON CONFLICT (tenant_id, code) DO NOTHING
  ```
  → `violates foreign key constraint "support_faqs_category_id_fkey" (Key (category_id)=(false) ...)`

Cùng mẫu câu này còn nằm trong các repository chạy lúc runtime (không chỉ seed):
`wellness-support/backend/infrastructure/kysely-support.repository.ts:266`,
`wellness-protocols/backend/infrastructure/catalog.repository.ts:170`,
`wellness-library/backend/infrastructure/{kysely-library.repository.ts:311, content.repository.ts:282}`.

---

## Lỗi 4 (nghiêm trọng): `GREATEST`/`LEAST` sai trong query có JOIN — ĐÃ SỬA (2026-10-21)

> Sửa: `greatest`/`least` dùng chung một cài đặt cho mọi bộ đánh giá (`engine/strfn.rs`); trên dòng JOIN, biểu thức có phép toán / hàm số học / ngày giờ / ép kiểu (`ROUND(... / GREATEST(...))`, `CEIL(EXTRACT(EPOCH FROM (a - b)))::int`) được tính bằng bộ đánh giá đầy đủ sau khi thay cột bằng giá trị. Golden `BG_06`–`BG_09` (gồm `UPDATE ... FROM`).

Câu chỉ có một bảng thì đúng. Có JOIN (bảng thật, bảng dẫn xuất hay `CROSS JOIN`) thì kết quả sai, có khi báo lỗi:

```sql
CREATE TABLE rw (id INT, title TEXT);  INSERT INTO rw VALUES (1, 'r1');
CREATE TABLE rd (id INT, reward_id INT, status TEXT, expires_at TIMESTAMPTZ);
INSERT INTO rd VALUES (1, 1, 'available', now() + interval '3 days');

SELECT GREATEST(d.id, 5) AS g FROM rd d JOIN rw w ON w.id = d.reward_id;  -- Postgres 5, db-rs NULL
SELECT GREATEST(5, d.id) AS g FROM rd d JOIN rw w ON w.id = d.reward_id;  -- Postgres 5, db-rs 1
SELECT GREATEST(0, CEIL(EXTRACT(EPOCH FROM (d.expires_at - CURRENT_TIMESTAMP)) / 86400))::int
FROM rd d JOIN rw w ON w.id = d.reward_id;
-- db-rs: invalid input syntax for type integer: "CEIL(EXTRACT(EPOCH FROM (d.expires_at - CURRENT_TIMESTAMP)) / 86400)"
```

Câu gốc trong WAIKI:

- `objects/specialists/lifecycle.ts:274` (seed gói phiên): `GREATEST(s.session_duration_minutes, 15)` trong `CROSS JOIN (VALUES ...)`
  → `null value in column "price" of relation "specialist_session_packages" violates not-null constraint`
- `wellness/wellness-loyalty/backend/infrastructure/kysely-rewards.repository.ts:58` (`days_left` của phiếu đổi thưởng, JOIN `rewards`) → báo lỗi lúc runtime
- `wellness/wellness-bookings/backend/infrastructure/demo-seed.ts:658` (`UPDATE ... FROM`), `wellness/wellness-protocols/lifecycle.ts:460` (`UPDATE ... FROM`)

## Lỗi 5: self-join không alias bảng bên trái trả 0 dòng — ĐÃ SỬA (2026-10-21)

> Sửa: khi cả hai vế của `ON` đều khớp bảng bên phải (tên gốc `sp` và alias `sp2`), vế mang alias là cột của bảng phải; vế trái là biểu thức (`sp.id + 1`) được tính bằng bộ biên dịch JOIN. Golden `BG_10`–`BG_12`.

```sql
SELECT sp.id FROM sp JOIN sp sp2 ON sp2.id = sp.id;   -- Postgres: mọi dòng của sp, db-rs: 0 dòng
SELECT a.id FROM sp a JOIN sp b ON b.id = a.id;       -- đúng
```

WAIKI chưa thấy dùng mẫu này.

## Lỗi 3 (nhỏ): `pg_typeof()` không được tính — ĐÃ SỬA (kiểm tra lại 2026-10-21, golden `BG_13`)

```sql
SELECT cat_id, pg_typeof(cat_id) FROM item2;
-- Postgres: pg_typeof = integer
-- db-rs:    pg_typeof = 'pg_typeof(cat_id)'   (trả lại nguyên văn tên biểu thức)
```

WAIKI không dùng hàm này. Chỉ phát hiện khi dựng repro.

---

## Không phải lỗi engine

`relation "customers" does not exist` khi chạy migration `customer-auth`: do thứ tự migration (bảng `customers` chưa được tạo). Postgres thật cũng báo lỗi này, và backend tự chạy lại migration hỏng sau đó. Bảng `customer_auth_identities` vẫn được tạo bình thường.
