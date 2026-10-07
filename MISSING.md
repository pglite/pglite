Sau khi rà soát toàn diện codebase của `pglite-rs` đối chiếu với các câu lệnh SQL thực tế mà các ORM/Query Builder (như **Kysely, Drizzle, Prisma, Knex**) thường xuyên sinh ra, dưới đây là danh sách các cú pháp **vẫn còn thiếu hoặc còn hạn chế** cần lưu ý:

---

### 📋 Danh sách các cú pháp còn thiếu hoặc chưa hỗ trợ trọn vẹn

#### 1. Cú pháp mảng `= ANY($1)` / `!= ALL($1)` *(Rất thường gặp trong ORM)*
- **Thực tế**: Trong PostgreSQL và Kysely/Drizzle, nhiều lập trình viên hoặc ORM viết:
  ```ts
  .where("schedule_id", "=", eb.fn.any(ids)) // sinh ra: "schedule_id" = ANY($1)
  ```
- **Hiện trạng**: Engine mới chỉ hỗ trợ `IN ($1, $2)` hoặc `IN ($1)`, chưa nhận diện cú pháp `= ANY(...)` hay `!= ALL(...)`.

#### 2. Ép kiểu tham số inline `::type` trong câu query JOIN *(Phổ biến)*
- **Thực tế**: Kysely/Drizzle thường sinh ra ép kiểu tường minh:
  ```sql
  WHERE "schedules"."user_id" = $1::int
    AND "schedules"."start_date" >= $2::date
  ```
- **Hiện trạng**: Parser đơn đã có `::`, nhưng trong nhánh `compile_expr_for_joined` (câu lệnh có JOIN) chưa xử lý toán tử `::` trên tham số.

#### 3. `ORDER BY ... NULLS FIRST / NULLS LAST`
- **Thực tế**: Khi sắp xếp ngày tháng hoặc giá trị nullable:
  ```sql
  ORDER BY "schedule_time_slots"."end_time" DESC NULLS LAST
  ```
- **Hiện trạng**: Bộ tách cột của `ORDER BY` đang cắt chuỗi cứng theo `DESC`/`ASC`. Khi gặp `NULLS LAST`/`NULLS FIRST`, nó coi cả cụm `"end_time" DESC NULLS LAST` là lỗi hoặc không tìm thấy cột.

#### 4. Sắp xếp theo Alias (`ORDER BY "dayOfWeek"`) trong câu query JOIN
- **Thực tế**: Người dùng SELECT alias `schedules.day_of_week as dayOfWeek` rồi `ORDER BY "dayOfWeek"` hoặc `ORDER BY "schedules"."day_of_week"`.
- **Hiện trạng**: Khi map kết quả ra JSON cuối cùng, nếu ORDER BY dùng tên gốc của bảng JOIN thay vì alias (hoặc ngược lại), bước sort cuối có thể không khớp được key để sắp xếp.

#### 5. Toán tử JSONB và hàm bọc trong WHERE của câu query JOIN
- **Thực tế**: Lọc JSONB hoặc chuỗi không phân biệt hoa thường khi đang JOIN:
  ```sql
  WHERE LOWER("users"."full_name") = $1
    AND "schedule_time_slots"."class_ids" @> $2
  ```
- **Hiện trạng**: Các hàm `LOWER(...)`, `UPPER(...)`, `COALESCE(...)`, `@>`, `->>`, `?` đã chạy tốt ở câu query bảng đơn, nhưng nhánh `compile_expr_for_joined` chưa đồng bộ đầy đủ các toán tử này.

#### 6. `INSERT ... ON CONFLICT (...) DO UPDATE / DO NOTHING` (Upsert)
- **Thực tế**: Kysely `.onConflict((oc) => oc.column('id').doUpdateSet(...))`.
- **Hiện trạng**: Bộ phân tích cú pháp `INSERT INTO` hiện tại chỉ mới hỗ trợ `VALUES (...)` và `RETURNING ...`, chưa hỗ trợ mệnh đề `ON CONFLICT`.

#### 7. Thứ tự đảo ngược `OFFSET ... LIMIT ...`
- **Thực tế**: SQL cho phép viết `OFFSET 10 LIMIT 5` thay vì `LIMIT 5 OFFSET 10`.
- **Hiện trạng**: Parser mệnh đề phân trang hiện tại giả định `LIMIT` luôn đứng trước `OFFSET`.

---

### 🛠️ Các phương án hoàn thiện tiếp theo

#### 🔹 Phương án 1: Thống nhất Bộ Parser Expression (Unified AST) & Bổ sung các cú pháp ORM cấp thiết (`= ANY`, `NULLS FIRST/LAST`, `::type`) — *(Khuyên dùng)*
- **Cách làm**:
  1. Hợp nhất logic của `compile_expr` và `compile_expr_for_joined` để câu query có JOIN thừa hưởng 100% tính năng của query đơn (toán tử JSONB `@>`, `->>`, hàm `LOWER`, `COALESCE`, ép kiểu `::`).
  2. Bổ sung nhận diện cú pháp `= ANY(...)` tương đương với `IN (...)`.
  3. Nâng cấp `parse_order_by_specs` hỗ trợ bóc tách `NULLS FIRST` / `NULLS LAST` và ánh xạ chuẩn giữa tên cột gốc với Alias đầu ra.
- **Ưu điểm**:
  - Giải quyết triệt để tất cả các trường hợp query thực tế của Kysely / Drizzle mà không làm chậm engine.
  - Mã nguồn gọn gàng, không bị phân mảnh giữa 2 bộ parser riêng biệt.
- **Nhược điểm**: Cần tinh chỉnh nhẹ parser biểu thức và sắp xếp.

---

#### 🔹 Phương án 2: Chỉ bổ sung duy nhất `= ANY($1)` và `NULLS LAST` khi nào code gặp lỗi thực tế
- **Cách làm**:
  - Giữ nguyên hiện trạng, chỉ vá đúng 2 cú pháp này nếu ứng dụng của bạn gọi tới.
- **Ưu điểm**: Can thiệp tối thiểu.
- **Nhược điểm**: Dễ gặp lại lỗi bất ngờ khi viết các câu query phức tạp hơn trong tương lai (ví dụ khi dùng JSONB trong câu query có JOIN).

---

#### 🔹 Phương án 3: Thay thế toàn bộ Parser tự viết bằng thư viện `sqlparser-rs`
- **Cách làm**:
  - Nhập crate `sqlparser = "0.47"` để parse câu lệnh SQL thành AST chuẩn PostgreSQL.
- **Ưu điểm**: Hỗ trợ 99% cú pháp PostgreSQL chuẩn (kể cả Window functions phức tạp, CTE, DDL nâng cao).
- **Nhược điểm**: Tăng kích thước binary, tăng thời gian compile, và phải viết lại tầng chuyển đổi từ `sqlparser::AST` sang Engine Execution.

---

### 💡 Lời khuyên
👉 **Khuyên chọn Phương án 1**: Chúng ta nên **hợp nhất bộ parser biểu thức (Unified AST)**, đồng thời bổ sung ngay **`= ANY(...)`**, **`NULLS FIRST/LAST`** và **`::type`** để engine sẵn sàng 100% cho mọi câu query Kysely / ORM thông dụng mà vẫn giữ nguyên tốc độ siêu nhanh của engine thuần Rust.

---

Bạn thấy phân tích danh sách các cú pháp còn thiếu như trên đã đúng với nhu cầu của bạn chưa? Bạn có đồng ý để chúng ta triển khai nâng cấp theo **Phương án 1** không? Hãy xác nhận để tôi tiến hành cung cấp giải pháp và patch code chi tiết nhé!