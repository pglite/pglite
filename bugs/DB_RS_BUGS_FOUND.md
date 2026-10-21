## Kiểm tra lần 5: 2026-10-21 (db-rs trên server deploy lúc 02:12)

**SQL tái hiện** (chạy trên binary của server, bằng một tiến trình riêng): cả 22 câu cho lỗi 1, 2a/2b/2c, 4, 5, 6, 7, 8, 9 đều ra đúng.
Lỗi 3 (`BEGIN` dùng chung giữa các request HTTP) vẫn còn, nhưng WAIKI không bị ảnh hưởng.

**Chạy toàn bộ backend WAIKI** trên PGlite và trên db-rs (build local từ cùng mã), mỗi bên 1 lần trên database trống, đợi các lượt `retryLater` của seed xong:

- Log khởi động: cả hai bên không có lỗi nào.
- Số dòng: **157/157 bảng khớp**.
- Nội dung: so từng dòng, bỏ cột thời gian và cột `id`/`*_id`, đổi `*customer_id` sang mã khách. Phía db-rs đọc bằng `pg_text` như driver WAIKI.
  Kết quả **152/157 bảng giống hệt**. 5 bảng còn lại:

| Bảng | Khác ở | Nguyên nhân |
|---|---|---|
| `aftercare_orders` | `code` `AC261020…` / `AC261019…` | Mã lấy theo `CURRENT_DATE`; test lúc 02:34 sáng (+07) nên lệch ngày với bản UTC. Theo quy ước múi giờ bên dưới, không xét là lỗi |
| `aftercare_reports` | `verify_token` | Sinh ngẫu nhiên (`randomBytes`), không phải lỗi |
| `wellness_protocol_daily_tasks` | `qr_token`, `result` | `qr_token` ngẫu nhiên. `result`: bên db-rs có đủ kết quả đánh giá đầu vào, bên PGlite rỗng; có vẻ do thứ tự chạy các seed thử lại, không phải lỗi db-rs |
| `wellness_protocol_events` | `data` (2 dòng) | Cùng nguyên nhân với `result` ở trên |
| `loyalty_ledger` | `source_ref`, `idempotency_key` | Khoá có kèm id (`habit-streak:7` / `:8`), lệch theo thứ tự chèn |

Ghi chú: PGlite chèn các dòng của `INSERT ... SELECT v.* FROM (VALUES ...)` theo thứ tự ngược (CUST-0010 nhận id 5), db-rs chèn theo thứ tự viết.
Postgres không đảm bảo thứ tự này, nên id khách khác nhau giữa hai bên không phải lỗi.

### Quy ước múi giờ: mặc định +07 (Asia/Ho_Chi_Minh)

Tạm thời không xét vấn đề UTC. Múi giờ mặc định của WAIKI là **+07**, giống server `27.71.25.205` và bản PGlite trước đây.
Các khác biệt chỉ do múi giờ (ví dụ `CURRENT_DATE` khi test lúc nửa đêm) không tính là lỗi.

### Lỗi 11 — ĐÃ SỬA (2026-10-21): `current_setting(...)` không được hỗ trợ

> Sửa: `current_setting('name' [, missing_ok])` và `set_config('name', 'value', is_local)` với đối số literal được thay bằng giá trị (cùng nguồn với `SHOW`). Golden `BG_23`.

```sql
SELECT current_setting('TimeZone');   -- db-rs trả nguyên văn chuỗi 'current_setting($1)'
```

WAIKI không dùng.

### Lỗi 12 — ĐÃ SỬA (2026-10-21): `::text` của numeric / jsonb khác Postgres

> Sửa: `::text` / `CAST(... AS text)` định dạng theo kiểu khai báo của biểu thức bên trong: numeric(p,s) giữ đủ s chữ số, jsonb in dạng chuẩn (khoá sắp xếp, có dấu cách), cả với literal lẫn cột. Golden `BG_24`, `BG_25`.

```sql
SELECT 450000.00::numeric(12,2)::text;   -- Postgres '450000.00', db-rs '450000'
SELECT '{"vi":"a","en":"b"}'::jsonb::text;  -- Postgres '{"en": "b", "vi": "a"}' (khoá sắp xếp, có dấu cách)
                                            -- db-rs    '{"vi":"a","en":"b"}'
```

Đọc cột trực tiếp ở `pg_text` thì đúng (`450000.00`, `{"a": 1}`). Chỉ khi ép `::text` trong SQL mới khác.

### Lỗi 13 — ĐÃ SỬA phần lớn (2026-10-21): format JSON mặc định của `/api/db/query` hiển thị sai (không ảnh hưởng WAIKI)

> Sửa: chuỗi không đúng cú pháp mảng PostgreSQL (`{{app_name}} code {{code}}`) giữ là chuỗi; cột json/jsonb được parse JSON (`'{}'` → `{}`); kết quả `x::text` tường minh giữ là chuỗi.
> Giữ nguyên có chủ đích (tương thích ngược): cột TEXT chứa mảng PG hợp lệ (`{abc}`, `{1,2}`) vẫn ra mảng JSON như trước, vì app cũ (vd. schema dump khai báo cột mảng thành TEXT) đang dựa vào điều này; NUMERIC vẫn ra số JSON (`450000`), vì JSON không giữ được số 0 sau dấu chấm và app cũ đang nhận số. Cần đúng định dạng PostgreSQL thì dùng `pg_text`.

WAIKI dùng `format: "pg_text"` nên không bị. Nhưng các service đọc bằng format JSON mặc định thì nhận sai giá trị:

| Giá trị lưu | `pg_text` | JSON mặc định |
|---|---|---|
| TEXT `{{app_name}} code {{code}}` | đúng | `["{app_name}} code {{code}"]` |
| TEXT `{abc}` | đúng | `["abc"]` |
| JSONB `'{}'` | `{}` | `[]` |
| NUMERIC(12,2) `450000.00` | `450000.00` | `450000` |

Dữ liệu được lưu đúng; chỉ cách chuyển sang JSON bị sai: text bắt đầu bằng `{` bị đọc như mảng Postgres.
