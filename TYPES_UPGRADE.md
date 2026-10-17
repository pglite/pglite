1. Lưu kiểu cột chính xác hơn (src/types.rs)

DataType hiện gộp nhiều kiểu Postgres thành một, nên không thể suy ra đúng OID:

┌──────────────────────────┬────────────────┬───────────────────────────┐
│ Kiểu khai báo trong DDL  │ pglite-rs lưu  │          Hệ quả           │
│                          │     thành      │                           │
├──────────────────────────┼────────────────┼───────────────────────────┤
│ INT2, INT4               │ Integer        │                           │
├──────────────────────────┼────────────────┼───────────────────────────┤
│ NUMERIC, FLOAT4/8, REAL, │ Numeric        │ không biết nên parse      │
│  MONEY                   │                │ thành số hay chuỗi        │
├──────────────────────────┼────────────────┼───────────────────────────┤
│ TIMESTAMP, TIMESTAMPTZ   │ Timestamp      │ lệch múi giờ              │
├──────────────────────────┼────────────────┼───────────────────────────┤
│ TIME, TIMETZ             │ Time           │                           │
├──────────────────────────┼────────────────┼───────────────────────────┤
│ JSON, JSONB              │ Jsonb          │                           │
├──────────────────────────┼────────────────┼───────────────────────────┤
│ VARCHAR, CHAR, enum, các │ Text           │                           │
│  kiểu khác               │                │                           │
└──────────────────────────┴────────────────┴───────────────────────────┘

Cách làm: thêm vào ColumnDef trường declared_type (dùng #[serde(default)] để file .db cũ vẫn đọc được) cùng hàm pg_oid(). Bảng cũ chưa có thông tin này thì dùng giá trị mặc định gần đúng nhất.

2. Trả fields có kiểu thật, theo đúng thứ tự SELECT, ở mọi nhánh thực thi

Hiện tại mỗi nhánh tự tạo fields theo một kiểu:

- SELECT đơn (executor.rs:3883): PrimaryCol/JoinedCol đã biết col_idx nhưng vẫn ghi cứng "text". Sửa chỗ này dễ: lấy kiểu từ table.columns[col_idx].
- SELECT có JOIN (executor.rs:1927): tên cột lấy từ key của dòng đầu tiên, kiểu ghi cứng "text". Truy vấn của bạn là JOIN schedules với schools nên chạy vào nhánh này. Hệ quả:
  - Kết quả 0 dòng thì không có field nào. Driver nhận RowDescription rỗng.
  - Thứ tự cột bị sắp theo ABC.
  - Hai cột cùng tên (ví dụ a.id, b.id) gộp làm một, nên mất trường.
- GROUP BY / aggregate (:4359, :4890): đã có kiểu một phần (int8, numeric, bool).
- CTE, UNION, bảng dẫn xuất, RETURNING: phải truyền fields của truy vấn con ra ngoài.
- Biểu thức (COALESCE, ::int, count(), now(), to_char…): cần một hàm suy kiểu nhỏ. Không suy được thì trả text.

3. Trả rows dạng mảng thay vì object

Đổi rows: Vec<serde_json::Value> (dạng object) thành Vec<Vec<…>> xếp theo đúng thứ tự fields. Đây là cách duy nhất để không mất cột trùng tên.

4. Giá trị trả ra theo định dạng text chuẩn của Postgres

Hiện value_to_json (executor.rs:11804) tự "đoán" giá trị:
- Chuỗi trông giống mảng PG hoặc JSON thì bị parse thành mảng/object, kể cả khi cột là TEXT. Ví dụ class_ids trong schema.sql khai báo là TEXT, nhưng log của bạn ra [7831,7832].
- BYTEA bị biến thành mảng số JSON.
- Bool là true/false.

Postgres thật gửi t/f, 2026-10-12, 08:00:00, {1,2}, \x…. Nếu pglite-rs xuất đúng chuẩn này kèm OID, gateway chỉ việc chuyển tiếp nguyên văn.

⚠️ Rủi ro tương thích: app hiện tại có thể đang dựa vào các "phép màu" đó, ví dụ class_ids (TEXT) tự thành mảng. Khi trả đúng kiểu, những chỗ này sẽ nhận chuỗi "[7831,7832]", đúng như Postgres thật. Hướng sửa đúng là đổi cột sang jsonb hoặc int[], nhưng cần rà lại code app.

5. Hàm describe(sql) không chạy truy vấn (nên có)

Hàm này trả về fields mà không thực thi, để gateway khỏi chạy mỗi query hai lần. Nếu làm được mục 2 thì mục này gần như có sẵn: chỉ cần dựng fields từ phần projection mà bỏ qua bước quét dữ liệu.

Ngoài phần kiểu (liên quan chuyện chậm)

PGlite::query(&mut self) cần quyền ghi độc quyền, nên ngay cả các lệnh đọc cũng không chạy song song được, cộng thêm mutex trong db-rs. Muốn cho đọc song song thì phải tách &self cho SELECT. Phần này độc lập với chuyện kiểu, có thể làm sau.

Khối lượng

- Mục 1, mục 2 cho SELECT đơn + JOIN, mục 3 giải quyết đúng các truy vấn trong log của bạn, khoảng vài trăm dòng.
- Mục 4 cần đi kèm việc rà code app.
- Các nhánh CTE/UNION/biểu thức làm dần.

Đề xuất bước tiếp: trước khi sửa, mình dựng pglite-rs local, nạp schema.sql của zalo-1054 rồi chạy 4 truy vấn trong log. Như vậy sẽ thấy chính xác cột nào sai hoặc mất ở tầng engine, tách bạch với lỗi của gateway. Bạn đồng ý thì mình làm luôn.