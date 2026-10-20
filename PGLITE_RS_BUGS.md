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

## Lỗi 1: bảng dẫn xuất `(VALUES ...)` không được hỗ trợ

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

## Lỗi 2 (nghiêm trọng): subquery vô hướng trong `INSERT ... VALUES` trả về `false`

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

## Lỗi 3 (nhỏ): `pg_typeof()` không được tính

```sql
SELECT cat_id, pg_typeof(cat_id) FROM item2;
-- Postgres: pg_typeof = integer
-- db-rs:    pg_typeof = 'pg_typeof(cat_id)'   (trả lại nguyên văn tên biểu thức)
```

WAIKI không dùng hàm này. Chỉ phát hiện khi dựng repro.

---

## Không phải lỗi engine

`relation "customers" does not exist` khi chạy migration `customer-auth`: do thứ tự migration (bảng `customers` chưa được tạo). Postgres thật cũng báo lỗi này, và backend tự chạy lại migration hỏng sau đó. Bảng `customer_auth_identities` vẫn được tạo bình thường.
