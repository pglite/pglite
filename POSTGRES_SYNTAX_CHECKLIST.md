# Checklist cú pháp PostgreSQL cho `pglite-rs`

Danh sách cú pháp PostgreSQL (tham chiếu PostgreSQL 17/18) dùng để theo dõi và triển khai dần.

Trạng thái được điền bằng **differential test**: cùng một câu SQL chạy trên `pglite-rs` và trên PostgreSQL 17 thật (`@electric-sql/pglite` 0.5.8), rồi so từng giá trị dạng text.
Đo ban đầu: 2026-10-20, branch `feat/pg-wire-types`, commit `7db3c5a`. Lần chạy đó có 593 probe, kết quả: khớp 212 (+11 khớp sau chuẩn hoá), sai kết quả 140, chưa hỗ trợ 102, PG báo lỗi nhưng rs vẫn chạy 18, tên cột sai 21.

## Tiến độ triển khai (cập nhật theo từng đợt)

Nguồn sự thật: bộ golden test `crates/pglite-rs/tests/pg_golden/` (mỗi case so với PostgreSQL 17 thật; `known_gaps.txt`
chỉ được co lại). Dòng nào mọi probe đã khớp được đánh dấu bằng `python3 scripts/pg_golden/checklist_status.py --apply`
(case chỉ so được hình dạng — thời gian thực, ngẫu nhiên — không được tính).

| Thời điểm | Commit | Golden khớp PG | JS test fail | Ghi chú |
|---|---|---|---|---|
| Trước đợt 1 | `9178640` | 332 / 880 | 27 (đều có từ trước) | |
| Đợt 1 xong | `d198760` | 395 / 905 | 26 | lỗi runtime, 25P02, lexer, cột không tồn tại |
| Đợt 2 (phần a) | `1cf6dda` | 407 / 905 | 26 | kiểu khai báo, wire format, DDL theo giao dịch |
| Đợt 2 (phần b) | `445a332` | 465 / 921 | 26 (y hệt danh sách cũ) | chia nguyên / tràn int, tên cột wire, mảng/ROW/bytea/jsonb, RETURNING có kiểu, pg_typeof, box/bit/jsonpath; +16 case `W2_*` |
| Mục 3 (toán tử) | `9ba3592` | 503 / 937 | 20 (6 test regex cũ đã qua, không có fail mới) | regex mọi đường, SIMILAR TO, ISNULL, BETWEEN SYMMETRIC, `\|/ \|\|/ @`, AT TIME ZONE, slice, ARRAY(subquery), IS NORMALIZED; +16 case `W3_*` |
| Mục 4 (SELECT) | `b4d5b02` | 547 / 959 | 20 (không đổi) | VALUES, TABLE, SELECT INTO / CTAS, FROM ONLY, FETCH ... WITH TIES, GROUPING SETS/ROLLUP/CUBE, biểu thức trên aggregate (sửa `count(*)` ở mục 0); +22 case `W4_*` |
| Mục 18.3 (ngày giờ) | `c6cadf5` | 613 / 993 | 20 (không đổi) | múi giờ phiên +7, toàn bộ hàm ngày giờ, generate_series/unnest trong FROM, sửa WHERE với `now() - interval` / `::date`, plan cache trả mảng cũ dạng chuỗi; +35 case `W5_*` |

Số golden ở bảng là kết quả của `cargo test --test pg_golden_test`. Script `checklist_status.py` in một con số thấp hơn
(407/921 sau đợt 2b, 444/937 sau mục 3, 551/993 sau 18.3) vì không tính các case chỉ so được hình dạng (thời gian thực, ngẫu nhiên).

Tổng hợp theo mục (sau mục 18.3, cập nhật 2026-10-20): **206 dòng xong / 129 dòng còn lại** (tính cả dòng con).

| Mục | Xong | Còn | | Mục | Xong | Còn |
|---|---|---|---|---|---|---|
| 0. Xuyên suốt | 14 | 4 | | 10. UPDATE | 3 | 4 |
| 1. Lexical | 13 | 0 ✅ | | 11. DELETE/TRUNCATE/MERGE/COPY | 2 | 3 |
| 2. Kiểu dữ liệu | 24 | 6 | | 12. DDL | 20 | 32 |
| 3. Toán tử & biểu thức | 24 | 1 | | 13. Transaction | 5 | 3 |
| 4. SELECT | 22 | 0 ✅ | | 14. Cấu hình phiên | 2 | 3 |
| 5. JOIN | 4 | 4 | | 15. Lệnh tiện ích | 3 | 6 |
| 6. WITH (CTE) | 3 | 2 | | 16. Bảo mật / CSDL | 1 | 3 |
| 7. Aggregate | 5 | 10 | | 17. System catalog | 1 | 7 |
| 8. Window | 5 | 2 | | 18. Hàm dựng sẵn | 33 | 37 |
| 9. INSERT | 5 | 1 | | 19. Xử lý lỗi | 6 | 0 ✅ |

6 dòng còn lại của mục 2 đều cần đối tượng mới nên chuyển đợt: ENUM, composite, DOMAIN (đợt 6); range, multirange,
tsvector/tsquery (đợt 10). Dòng còn lại của mục 3 là `(x).field` trên kiểu composite thật (đợt 6).

| Đợt | Nội dung | Trạng thái |
|---|---|---|
| 0 | Hạ tầng test: golden suite, fixture `legacy_v3`, compat check | ✅ |
| 1 | Lỗi xuyên suốt: chia 0 / cast / tràn số / 25P02 / lexer / cột không tồn tại / nhiều câu = 1 giao dịch | ✅ (lỗi `42883` hàm không tồn tại để sau đợt 5) |
| 2 | Kiểu dữ liệu & định dạng wire | ✅ phần a (numeric(p,s), varchar/char(n), int2, timestamp/timestamptz/timetz, money, nhiều SERIAL, literal có kiểu, DDL theo giao dịch) + phần b (chia nguyên & tràn int theo kiểu, tên cột PG ở wire, mảng nhiều chiều, jsonb `": "`, `ROW()`, bytea `\x`, RETURNING, pg_typeof, 22007/22008, box/bit/jsonpath). Chuyển đợt khác: enum/domain/composite (6), range/multirange/tsvector (10), `AT TIME ZONE` (5) |
| 3 | SELECT / biểu thức còn lại (VALUES, TABLE, cột trùng tên, GROUPING SETS...) | ⬜ |
| 4 | Hàm trả tập (generate_series, unnest, jsonb_each...) | ⬜ |
| 5 | Thư viện hàm & aggregate | ⬜ |
| 6 | Đối tượng DDL (sequence, identity, generated, view, schema, enum, domain, temp, partition...) | ⬜ |
| 7 | Function / procedure / trigger (plpgsql) | ⬜ |
| 8 | MERGE, COPY, EXPLAIN, PREPARE, cursor, LISTEN, READ ONLY | ⬜ |
| 9 | System catalog chính xác | ⬜ |
| 10 | Full-text search & range | ⬜ |
| 11 | P3: lệnh quyền hạn = no-op, phần còn lại báo `0A000` rõ ràng | ⬜ |

Tương thích ngược sau mỗi đợt: DB do 8f9b56c và 7db3c5a ghi mở y nguyên (`scripts/compat_check.sh`, 28 DB đã backup,
checksum không đổi); metadata mới chỉ đi bằng `SchemaOp` mới thêm cuối enum (bản cũ bỏ qua).
Đợt 2b (`445a332`): không đổi định dạng file/WAL, không thêm `SchemaOp`; 28/28 DB khớp `7db3c5a`, 27/27 khớp `8f9b56c`
(trừ fixture `legacy_v3` do `7db3c5a` ghi), mở lại lần 2 và bản cũ đọc lại đều y hệt. Hiệu năng chênh < 3% so với trước đợt.

## Cách đọc

| Ký hiệu | Ý nghĩa |
|---|---|
| `[x]` ✅ | Chạy và trả kết quả giống PostgreSQL |
| `[ ]` 🟡 | Chạy được nhưng **kết quả sai / ngữ nghĩa bị bỏ qua / lẽ ra phải báo lỗi** (nguy hiểm hơn ❌) |
| `[ ]` ❌ | Báo lỗi / chưa hỗ trợ |
| `[ ]` ⬜ | Chưa kiểm chứng được |

Ưu tiên: **P0** = ORM/driver (Prisma, Drizzle, Kysely, node-postgres, psql) dùng thường xuyên · **P1** = SQL ứng dụng phổ biến · **P2** = nâng cao · **P3** = hiếm dùng / ngoài phạm vi engine nhúng.

Mã trong ngoặc (`S04`, `J13b`...) là id probe trong bộ differential test, dùng để tra `report.md`.

Dữ liệu dùng khi probe:

```sql
CREATE TABLE t (id SERIAL PRIMARY KEY, name TEXT, n INT, price NUMERIC(10,2),
                meta JSONB, tags TEXT[], created TIMESTAMP DEFAULT now(), flag BOOLEAN);
CREATE TABLE u (id SERIAL PRIMARY KEY, t_id INT REFERENCES t(id), v TEXT);
-- t: (alice,5,1.50,{"a":1},{x,y},true) (bob,3,2.00,{"a":2},{y},false) (carol, NULL...)
-- u: (1,'v1') (1,'v2') (2,'v3')
```

---

## 0. Vấn đề xuyên suốt (nên sửa trước)

- [ ] 🟡 **P0** Hàm/biểu thức chưa hỗ trợ bị **trả nguyên văn dạng chuỗi** thay vì báo lỗi `42883`/`42703`. Ví dụ `SELECT nope FROM t` → `"nope"`; `regexp_replace(...)`, `format(...)`, `to_date(...)` trả lại chính câu chữ. (`ERR03`, `STR11`, `X02`) — cột không tồn tại đã báo `42703`; hàm không tồn tại (`42883`) làm sau đợt 5
- [x] ✅ **P0** Cột không tồn tại trong `WHERE` bị bỏ qua, trả 0 dòng mà không báo lỗi. (`ERR09`) — khớp PG
- [ ] 🟡 **P0** `SELECT a.name, b.name` hoặc `SELECT *` qua JOIN **mất cột trùng tên**. PG giữ cả hai cột. (`X10`, `J10`, `S41`, `J15`, `J17`, `J19`)
- [x] ✅ **P0** `'abc'::int` trả `NULL` thay vì lỗi `22P02`. (`ERR10`) — khớp PG
- [x] ✅ **P0** Chia số nguyên: `7 / 2` → `3`, `-7 / 2` → `-3` như PG (literal nguyên, cột INT/BIGINT/SMALLINT, `::int`, `count()`); cột NUMERIC vẫn chia thực (`F08`, `F08b`, `W2_01`, `W2_15`, `W2_16`) — khớp PG
- [x] ✅ **P0** `1/0` không báo lỗi `22012`, kể cả trong transaction. (`ERR05`, `X15`) — khớp PG
- [x] ✅ **P1** Tràn số `int4`/`int8` khi tính: `2147483647 + 1`, `n * 1000000000` → `22003` (`F09`, `W2_03`, `W2_04`); giá trị cũ đã lớn hơn int4 trong DB cũ thì tính theo int8 thay vì báo lỗi — khớp PG
- [x] ✅ **P1** `count(*)` đứng chung với `count(n)`, `count(DISTINCT n)`, `sum`, `avg` (trước đây ra 2 thay vì 3 vì trùng tên cột); biểu thức trên aggregate `sum(n) / count(n)`, `count(*) / 2`, `max(n) + 1`, `count(*)::int`, `coalesce(min(n), 0)`, cả khi có GROUP BY; `sum` của nhóm toàn NULL ra NULL (`A01`, `A01b`, `X09`, `W4_14`, `W4_15`, `W4_22`) — khớp PG
- [x] ✅ **P1** Tên cột mặc định như PostgreSQL ở đường wire, kể cả nhiều cột cùng tên: `5 & 3`, `1 + 1`, `meta->'a'` → `?column?`; `'1'::bit(1)::varbit` → `varbit`; `n::numeric::text` → `n` (`X08`, `E02`, `JS01`, `T02`, `T11`, `T28`, `W2_13`). Kết quả JS (object) vẫn đặt tên cũ cho cột trùng tên để không đè dữ liệu — khớp PG
- [ ] 🟡 **P1** Định dạng text khác PG (đường wire `query_wire`; JS API giữ dạng JSON cũ):
  - [x] `timestamp` bị thêm `+00` — cột, ép kiểu, `RETURNING`, `date_trunc`, `ts ± interval` đã đúng (`X13`, `T07`, `W2_09`)
  - [x] `numeric` giữ scale theo kiểu khai báo, cả ở `RETURNING` (`I10` chỉ còn lệch cột thời gian thực `created`, `W2_12`)
  - [x] mảng trả từ hàm / `ARRAY[...]` ra `{a,b}`, mảng nhiều chiều `{{1,2},{3,4}}` (`STR09`, `AR04`, `T21`, `W2_07`)
  - [x] `ROW(...)`, `(1, 'a')` ra `(1,a)` (`E30`, `W2_07`)
  - [x] `bytea` từ `decode()` / `sha256()` có tiền tố `\x` (`STR16` chỉ còn lệch `encode(bytea, 'base64')` — hàm chuỗi, đợt 5)
  - [x] `jsonb` in `{"a": 1, "bb": [1, 2]}` (khoá xếp như PG), cả kết quả của `||`, `->` (`W2_08`; `JS03` còn lệch `meta - 'a'`, `#-` — hàm JSON, đợt 5)
  - [ ] `min(int)` ra `3.0`; numeric nhân/chia chưa giữ scale (`3.00 * 2` ra `6`, PG `6.00`) (`X09`) — golden `X09` qua chỉ vì so số lỏng (`3.0` = `3`); text vẫn khác PG
- [x] ✅ Alias không có `AS` (`SELECT n "Num"`). (`S02`)
- [x] ✅ Kiểm tra kiểu khi ghi (`INSERT INTO t (n) VALUES ('abc')` → `22P02`). (`ERR07`)
- [x] ✅ `INSERT` có nhiều giá trị hơn số cột → lỗi. (`ERR11`)
- [x] ✅ Window function không còn sinh cột trùng. (`WF01`)
- [x] ✅ Thứ tự NULL mặc định và `NULLS FIRST/LAST`. (`S13b`, `S14b`, `S15b`)
- [x] ✅ SQLSTATE cho các lỗi đã hỗ trợ: 23505, 23503, 23502, 23514, 42P01, 42703, 22P02, 2BP01, 42601 khớp PG ở cả 25 probe cùng báo lỗi.
- [ ] 🟡 P2 Lệnh chưa hỗ trợ trả `42601` (lỗi cú pháp). PG dùng `0A000 feature_not_supported` cho tính năng không hỗ trợ. (`M01`, `TY03`, `TX09`, `UT10`...)
- [x] ✅ **P1** Sau khi có lỗi trong transaction, lệnh tiếp theo báo `25P02` cho tới khi `ROLLBACK` / `ROLLBACK TO SAVEPOINT`; `COMMIT` lúc đó = `ROLLBACK` (`K_S24`, test `constraints_test`, `multi_statement_lexer.test.ts`)

---

## 1. Cú pháp từ vựng (Lexical)

- [x] ✅ Chuỗi `'...'`, escape `''` (`L02`)
- [x] ✅ P1 Chuỗi escape `E'a\nb'` (`L03`) — khớp PG
- [x] ✅ P1 Dollar-quoting `$$...$$`, `$tag$...$tag$` trong biểu thức (`L04`, `L05`). Trong `DO $$...$$` thì đã chạy (`FN05`). — khớp PG
- [x] ✅ P3 Unicode `U&'\0041'` (`L06`) — khớp PG
- [x] ✅ P3 Bit-string `B'1010'`, hex `X'FF'` (`L07`, `L08`) — khớp PG
- [x] ✅ Số thực `1.5e3`, `.5`, hex `0x1F` (`L09`)
- [x] ✅ P3 Dấu gạch trong số `1_000_000` (`L10`) — khớp PG
- [x] ✅ Định danh có ngoặc kép, kể cả có dấu cách `"Mixed Case"` (`L11`, `DT19`)
- [x] ✅ Comment `-- ...` (`L12`)
- [x] ✅ P2 Comment khối lồng nhau `/* a /* b */ */` (`L13`) — khớp PG
- [x] ✅ Nhiều câu lệnh cách bằng `;` (trả kết quả câu cuối) (`L14`); như PG, cả chuỗi chạy trong một giao dịch ngầm (lỗi giữa chừng → huỷ hết)
- [x] ✅ P3 `'a' 'b'` trên cùng một dòng phải là lỗi cú pháp (`L16`) — khớp PG
- [x] ✅ Tham số `$1, $2`, `t.*`, `schema.table.column` (`L15`, `L17`, `L18`)

## 2. Kiểu dữ liệu

### Số
- [x] ✅ `smallint`, `integer`, `bigint`, `real`, `double precision` (`T01`, `T02`)
- [x] ✅ P1 `numeric(p,s)` không làm tròn theo scale (`3.256::numeric(10,2)` vẫn ra `3.256`) (`T02b`) — khớp PG
- [x] ✅ `SERIAL`
- [x] ✅ P1 `SMALLSERIAL`, `BIGSERIAL` ra `NULL` (`T03`) — khớp PG
- [x] ✅ P3 `money` (`T04`) — khớp PG

### Chuỗi / nhị phân
- [x] ✅ `text`, `bytea` literal (`T06`)
- [x] ✅ P1 `varchar(n)`, `char(n)` chưa cắt/đệm theo độ dài (`T05`, `T05b`) — khớp PG

### Ngày giờ
- [x] ✅ `interval` (`T10`, `T10b`)
- [x] ✅ `date - date`, `date + int`, `timestamp + interval` (`X01`, `DTF06c`)
- [x] ✅ P0 `timestamp`/`time` định dạng output (`+00`, thiếu giây) (`T07`, `X13`, `W2_09`) — cột, ép kiểu, `date_trunc`, `ts ± interval` — khớp PG
- [x] ✅ P1 Ngày giờ sai báo đúng mã lỗi: `'x'::timestamp` → `22007`, `'2024-02-30'::date` → `22008` (`W2_10`, `W2_11`, `K_T14`) — khớp PG
- [x] ✅ P2 Interval âm số nhiều như PG: `-1 days` (`K_T09`, `W2_14`) — khớp PG
- [x] ✅ P0 `timestamptz`, `timetz` định dạng output sai (`T08`, `T09`) — khớp PG

### Khác
- [x] ✅ `boolean`, `uuid`, `json`, `jsonb`, `xml` (dạng text), `inet/cidr/macaddr` (dạng text), `regclass` (`T11`, `T17`, `T19`, `T18`, `T14`, `T26`)
- [x] ✅ Mảng một chiều `int[]`, `'{1,2}'`, `ARRAY[...]`
- [x] ✅ P2 Mảng nhiều chiều `{{1,2},{3,4}}` (`T21`, `W2_07`) — khớp PG
- [ ] ❌ P1 `ENUM` (`T12`) — cần catalog kiểu mới: đợt 6
- [ ] ❌ P2 Composite type (`T22`) — đợt 6 (định dạng `ROW(...)` → `(1,x)` đã sẵn)
- [ ] 🟡 P2 Range `int4range`...: literal đã in đúng, toán tử `@>` sai (`T23`) — đợt 10
- [ ] 🟡 P3 Multirange (`T24`) — đợt 10
- [ ] ❌ P2 Domain (`T25`) — đợt 6
- [x] ✅ P3 Hình học `point`, `box` (box chuẩn hoá góc `(3,4),(1,2)`) (`T13`, `W2_14`) — khớp PG; toán tử hình học chưa có
- [x] ✅ P3 `bit(n)`, `varbit` (`T15`) — khớp PG
- [ ] 🟡 P2 `tsvector`, `tsquery` (`T16`) — đợt 10
- [x] ✅ P3 `jsonpath` ép kiểu & in (`$."a"[0]."b"`) (`T20`, `W2_14`) — khớp PG; truy vấn jsonpath ở đợt 5
- [x] ✅ P2 `pg_typeof(x)`: literal, ép kiểu, cột (theo kiểu khai báo), phép tính, hàm thường gặp (`T27`, `X03`, `K_T11`, `W2_06`) — khớp PG (`X03` dùng hàm thời gian thực nên golden chỉ so hình dạng; giá trị đã kiểm tay)

### Ép kiểu
- [x] ✅ `x::type`, `CAST(x AS type)` (`T28b`, `X07`)
- [x] ✅ P2 Typed literal `int '5'`, `DATE '...'`, `TIMESTAMP '...'`, `INTERVAL '1' DAY` (`T28`, `DTF14`) — khớp PG
- [x] ✅ P1 Alias không `AS` sau ép kiểu: `'x'::timestamp a`, `1.5::double precision v` (trước đây tên cột thành `timestamp a` và bỏ qua kiểm tra giá trị) — khớp PG
- [x] ✅ P2 `(x)::numeric(5,0)` không còn bị hiểu nhầm là tuple (`K_T19`) — khớp PG

## 3. Toán tử & biểu thức

- [x] ✅ `+ - * / % ^` (xem lỗi chia nguyên ở mục 0)
- [x] ✅ P3 `|/`, `||/`, `@` (`E01`, `K_E15`, `W3_09`) — khớp PG
- [x] ✅ Bitwise `& | # ~ << >>` (`E02`)
- [x] ✅ So sánh, `BETWEEN`, `IS [NOT] NULL`, `IS [NOT] DISTINCT FROM`, `IS TRUE/FALSE/UNKNOWN`, `AND/OR/NOT` (`E03`, `E04`, `E06`, `E07`, `E32`)
- [x] ✅ P3 `BETWEEN SYMMETRIC`, `NOT BETWEEN SYMMETRIC` (`E04`, `K_E14`, `W3_05`, `W3_06`) — khớp PG (cận là hằng `NULL` thì có thể lệch PG: viết lại bằng `LEAST/GREATEST`)
- [x] ✅ P3 `ISNULL`, `NOTNULL`, cả trong WHERE (`E05`, `K_E13`, `W3_05`) — khớp PG
- [x] ✅ Dấu trừ đơn `-n`
- [x] ✅ P3 Dấu cộng đơn `+n` (`E33`) — khớp PG
- [x] ✅ `||` nối chuỗi (`E08`, `STR23`)
- [x] ✅ `LIKE`, `ILIKE`, `NOT LIKE`, `ESCAPE`, `~`, `!~` (`E09`, `E10`)
- [x] ✅ P1 `~`, `~*`, `!~`, `!~*` ở mọi đường: SELECT, WHERE, JOIN, tiếng Việt; mẫu sai → `2201B` (`E10`, `E10b`, `X06`, `W3_01`–`W3_04`). Trước đây `~*` bị hiểu thành phép nhân và regex trong WHERE luôn sai — khớp PG
- [x] ✅ P2 `SIMILAR TO`, `NOT SIMILAR TO`, `ESCAPE` (`X06`, `E34`, `K_E12`, `W3_07`, `W3_08`) — khớp PG (mẫu phải là hằng chuỗi)
- [x] ✅ `CASE WHEN ...` và simple `CASE x WHEN ...` (`E11`, `E12`)
- [x] ✅ `COALESCE`, `NULLIF`, `GREATEST`, `LEAST` (`E13`, `E13b`)
- [x] ✅ `IN (list)`, `IN (subquery)`, `EXISTS`, `NOT EXISTS`, scalar subquery (`E14`–`E18`)
- [x] ✅ `= ANY(ARRAY[...])`, `= ANY($1)`, `<> ALL(...)`, `ANY/SOME (subquery)` (`E19`–`E23`, `E20b`)
- [x] ✅ `'x' = ANY(array_column)` (`AR10b`)
- [x] ✅ Row constructor, so sánh row, `(a, b) IN ((...), (...))` (`E24`, `E25`)
- [ ] 🟡 P2 Truy cập field composite `(x).field` — trên cột không phải composite đã báo `42809` như PG (`E26`); kiểu composite thật ở đợt 6
- [x] ✅ Subscript mảng `tags[1]`
- [x] ✅ P2 Slice mảng `tags[1:2]`, `tags[2:]`, `tags[:1]`, cận là biểu thức, ngoài miền → `{}` (`E27`, `K_E18`, `W3_15`) — khớp PG
- [x] ✅ `COLLATE "C"` (`E28`)
- [x] ✅ P1 `AT TIME ZONE` / `timezone()`: timestamp ↔ timestamptz đúng chiều, nối tiếp, DST châu Âu / Mỹ / Úc / NZ, offset POSIX (`'+07'` = UTC−7 như PG), múi giờ lạ → `22023` (`X05`, `K_E20`–`K_E22`, `W3_10`–`W3_13`) — khớp PG. Bảng múi giờ nhúng ~90 vùng thường dùng (không có tzdata đầy đủ)
- [x] ✅ P2 `ARRAY(subquery)` không tương quan, kể cả rỗng / có NULL (`E36`, `K_E19`, `W3_14`) — khớp PG; dạng tương quan báo `0A000` thay vì trả kết quả sai
- [x] ✅ P3 `IS [NOT] [NFC|NFD|NFKC|NFKD] NORMALIZED` (`E35`, `K_E23`, `W3_16`) — khớp PG

## 4. SELECT

### Cơ bản
- [x] ✅ `SELECT *`, danh sách cột, alias, `DISTINCT` (`S01`–`S03`)
- [x] ✅ `DISTINCT ON (...)` (`S04`)
- [x] ✅ `WHERE`, `FROM a, b` (`S05`, `S41`)
- [x] ✅ `SELECT ... WHERE false` không có FROM (`S33`, `S33b`)
- [x] ✅ P1 `VALUES (...), (...)` đứng độc lập, tên cột `column1..`, có `ORDER BY/LIMIT` (`S34`, `K_S01`, `W4_01`) — khớp PG
- [x] ✅ P1 `FROM (VALUES ...) AS v(a, b)`, JOIN với VALUES, CTE `x(a, b) AS (VALUES ...)` (`S35`, `K_S02`, `K_S03`, `W4_02`, `W4_03`) — khớp PG
- [x] ✅ P3 `TABLE t` (`S36`, `K_S04`) — khớp PG (`S36` chỉ khác cột thời gian thực `created`)
- [x] ✅ P2 `SELECT ... INTO [UNLOGGED] [TABLE] t` → `CREATE TABLE AS` (`S40`, `K_S05`, `W4_04`) — khớp PG. `INTO TEMP` báo `0A000` (bảng tạm làm ở đợt 6)
- [x] ✅ P3 `FROM ONLY t`, `UPDATE ONLY t` (`S43`, `K_S06`, `W4_21`) — khớp PG (chưa có kế thừa bảng nên ONLY không đổi gì)
- [x] ✅ `TABLESAMPLE` (chấp nhận) (`S44`)

### GROUP BY / HAVING
- [x] ✅ `GROUP BY col`, `GROUP BY 1`, `HAVING` (`S06`–`S08`, `S08b`)
- [x] ✅ P2 `GROUPING SETS`, `ROLLUP`, `CUBE`, `GROUPING(a, b)`, kết hợp nhiều phần tử, alias trong GROUP BY, HAVING (`S09`–`S12`, `S09b`, `K_S10`, `K_S11`, `W4_12`, `W4_13`) — khớp PG; `K_S09` chỉ khác thứ tự hai dòng có khoá ORDER BY bằng nhau (PG sort không ổn định)

### ORDER BY
- [x] ✅ `ASC/DESC`, nhiều cột, alias, vị trí, biểu thức, `NULLS FIRST/LAST` (`S13`–`S18`)
- [x] ✅ P3 `ORDER BY x USING >` / `USING <` (`S19`, `K_S13`, `W4_09`) — khớp PG

### LIMIT / OFFSET / FETCH
- [x] ✅ `LIMIT`, `OFFSET`, `OFFSET ... LIMIT`, `LIMIT ALL`, `LIMIT $1`, `OFFSET m ROWS FETCH FIRST n ROWS ONLY` (`S20`–`S24`, `S26`)
- [x] ✅ P1 `FETCH FIRST|NEXT n ROW[S] ONLY`, `OFFSET m ROW[S]` (`S25b`, `W4_09`, `W4_10`) — khớp PG
- [x] ✅ P2 `FETCH FIRST n ROWS WITH TIES`, khoá là alias / biểu thức (`S25`, `K_S12`, `W4_11`) — khớp PG

### Set operations
- [x] ✅ `UNION`, `UNION ALL`, `INTERSECT`, `EXCEPT` (theo vị trí cột, có loại trùng) (`S27`–`S30`, `S27b`)
- [x] ✅ Set op có ngoặc, `ORDER BY/LIMIT` sau set op (`S31`, `S32`, `S32b`)
- [x] ✅ P2 `INTERSECT ALL`, `EXCEPT ALL` giữ đúng số lần lặp (`W4_16`, `W4_17`) — khớp PG

### Khoá dòng
- [x] ✅ `FOR UPDATE`, `FOR SHARE`, `SKIP LOCKED`, `NOWAIT` (`S37`–`S39`)
- [x] ✅ P2 `FOR NO KEY UPDATE`, `FOR KEY SHARE`, `FOR UPDATE OF t`; dùng cùng aggregate / GROUP BY / DISTINCT báo `0A000` như PG (`W4_18`–`W4_20`) — khớp PG

## 5. JOIN

- [x] ✅ `[INNER] JOIN`, `LEFT [OUTER]`, `RIGHT [OUTER]`, `FULL [OUTER]`, `CROSS` (`J01`–`J07`, `J05b`, `J06b`)
- [x] ✅ `JOIN ... USING (col)` (`J08`)
- [ ] ❌ P2 `NATURAL JOIN` (`J09`)
- [ ] 🟡 P0 Self-join chọn cột trùng tên (`a.name, b.name`) bị mất cột (`J10`, `X10`)
- [x] ✅ JOIN với derived table, `LEFT JOIN LATERAL (...)`, 3+ bảng, điều kiện ON phức hợp, anti-join (`J11`–`J15`, `J13b`)
- [ ] ❌ P2 `LATERAL` với hàm trả tập (`J20`)
- [ ] 🟡 P2 JOIN trong ngoặc `FROM (a JOIN b ON ...)` chỉ trả cột bảng đầu (`J16`)
- [x] ✅ JOIN + `$1::int`, `lower()`, `@>`, `ORDER BY alias` (`J17b`, `J18`, `J19b`)

## 6. WITH (CTE)

- [x] ✅ CTE thường, nhiều CTE, `RECURSIVE`, `[NOT] MATERIALIZED` (`W01`–`W05`)
- [x] ✅ Data-modifying CTE `DELETE/INSERT ... RETURNING` (`W06`, `W07`)
- [ ] ⬜ P2 `WITH u AS (UPDATE ... RETURNING *)`
- [x] ✅ P2 `WITH x(a, b) AS (VALUES ...)` (`W10`) — khớp PG
- [ ] ❌ P3 `SEARCH`, `CYCLE` (`W08`, `W09`)

## 7. Hàm tổng hợp (Aggregate)

- [x] ✅ `count`, `count(DISTINCT)`, `sum`, `avg`, `min`, `max`, kết hợp nhiều hàm và biểu thức trên chúng; kiểu wire như PG (`sum(int)` → bigint `8`, `max(numeric(10,2))` → `2.00`) (`A01`, `A01b`, `W4_14`, `W4_22`)
- [x] ✅ `string_agg`, `array_agg`, có `ORDER BY` (`A02`, `A02c`, `A03`)
- [x] ✅ `json_agg(col)`, `jsonb_agg(col)` (`A04b`)
- [ ] ⬜ P0 `json_agg(t)` với cả dòng (chạy được, chưa so giá trị) (`A04`)
- [ ] ❌ P1 `json_agg(t ORDER BY ...)` báo `Column t not found` (`X11`)
- [ ] ❌ P1 `json_object_agg`, `jsonb_object_agg` (`A05`)
- [x] ✅ `bool_and`, `bool_or`, `every` (`A06`)
- [x] ✅ `agg(...) FILTER (WHERE ...)` (`A08`)
- [ ] 🟡 P2 `bit_and`, `bit_or` (`A07`)
- [ ] 🟡 P2 `stddev`, `variance`, `var_*`, `stddev_*` (`A09`, `A09b`)
- [ ] 🟡 P2 `percentile_cont`, `percentile_disc`, `mode() WITHIN GROUP` (`A10`, `A11`, `A10b`)
- [ ] 🟡 P3 `corr`, `covar_*`, `regr_*` (`A12`)
- [ ] 🟡 P2 `any_value` (`A13`)
- [ ] 🟡 P3 Hypothetical-set `rank(x) WITHIN GROUP` (`A14`)
- [ ] 🟡 P3 `xmlagg` (`A15`)

## 8. Window function

- [x] ✅ `row_number`, `rank`, `dense_rank`, `ntile`, `percent_rank`, `cume_dist` (`WF01`, `WF02`, `WF10`)
- [x] ✅ Aggregate dạng window `sum/avg/count(...) OVER (...)`, có `PARTITION BY` (`WF03`, `WF04`)
- [x] ✅ `lag`, `lead`, `first_value`, `last_value`, `nth_value` (`WF05`, `WF06`)
- [x] ✅ Frame `ROWS BETWEEN`, `WINDOW w AS (...)`, `FILTER ... OVER` (`WF06`, `WF07`, `WF11`)
- [x] ✅ Lọc kết quả window qua subquery (`WF12`)
- [ ] 🟡 P2 Frame `RANGE BETWEEN n PRECEDING ...` (`WF08`)
- [ ] 🟡 P2 Frame `GROUPS`, `EXCLUDE ...` (`WF09`)

## 9. INSERT

- [x] ✅ `VALUES` một/nhiều dòng, không liệt kê cột, `DEFAULT VALUES`, `DEFAULT` (`I01`–`I05`)
- [x] ✅ `INSERT ... SELECT`, `WITH ... INSERT` (`I06`, `I16`)
- [x] ✅ `RETURNING *`, `RETURNING expr AS alias` (`I07`, `I08`)
- [x] ✅ `ON CONFLICT (col) DO NOTHING / DO UPDATE SET ... = EXCLUDED... [WHERE]`, `ON CONSTRAINT`, không target, alias bảng (`I09`–`I14`)
- [x] ✅ `OVERRIDING SYSTEM VALUE`, tham số `$2::jsonb` (`I15`, `I17`)
- [ ] ⬜ P2 `ON CONFLICT (expr) WHERE ...` (arbiter partial index), `OVERRIDING USER VALUE`

## 10. UPDATE

- [x] ✅ `SET a = expr WHERE`, `RETURNING`, alias, `jsonb_set` (`U01`, `U02`, `U07`, `U09`)
- [x] ✅ `UPDATE ... FROM`, `WITH ... UPDATE ... FROM` (`U05`, `U08`)
- [x] ✅ `SET col = DEFAULT` (`U06`)
- [ ] ❌ P1 `SET (a, b) = (v1, v2)` (`U03`)
- [ ] ❌ P2 `SET (a, b) = (SELECT ...)` (`U04`)
- [ ] ❌ P2 `SET arr[1] = ...` (`U10`)
- [ ] ⬜ P3 `WHERE CURRENT OF cursor`

## 11. DELETE / TRUNCATE / MERGE / COPY

- [x] ✅ `DELETE ... WHERE`, `RETURNING`, alias, `USING` (`D01`–`D05`)
- [x] ✅ `TRUNCATE`, `RESTART IDENTITY CASCADE` (`D06`, `D07`)
- [ ] ❌ P2 `MERGE` (`M01`–`M03`)
- [ ] ❌ P1 `COPY ... FROM STDIN` (`C02`; harness chưa so được với PG)
- [ ] ❌ P2 `COPY ... TO STDOUT` (`C01`)

## 12. DDL

### 12.1 CREATE / DROP TABLE
- [x] ✅ `CREATE TABLE [IF NOT EXISTS]`, `schema.table` (`DT01`, `DT02`, `DT18`)
- [x] ✅ `PRIMARY KEY`, `NOT NULL`, `UNIQUE`, `DEFAULT`, `CHECK` cột và bảng, có cưỡng chế (`DT03`, `DT03b`–`d`, `DT04`)
- [x] ✅ `FOREIGN KEY`, `ON DELETE CASCADE`, `ON DELETE SET NULL` (`DT05`, `X17`, `DT05b`)
- [ ] ⬜ P2 `ON DELETE SET DEFAULT/RESTRICT`, `ON UPDATE ...`
- [x] ✅ Default bằng hàm (`gen_random_uuid()`, `now()`, `'{}'::jsonb`), `COLLATE` (`DT20`, `DT15`)
- [ ] 🟡 P1 `DEFERRABLE INITIALLY DEFERRED` vẫn kiểm tra ngay (`DT06`)
- [ ] ❌ **P0** `GENERATED ALWAYS/BY DEFAULT AS IDENTITY (...)` (`DT07`)
- [ ] 🟡 P1 `GENERATED ALWAYS AS IDENTITY` không chặn ghi đè (`DT07b`)
- [ ] 🟡 P1 `GENERATED ALWAYS AS (expr) STORED` ra `NULL` (`DT08`)
- [ ] ❌ P1 `CREATE TEMP TABLE` (`DT09`)
- [ ] ❌ P3 `CREATE UNLOGGED TABLE` (`DT10`)
- [x] ✅ P1 `CREATE [UNLOGGED] TABLE [IF NOT EXISTS] t [(cột)] AS <truy vấn> [WITH [NO] DATA]`: kiểu cột theo kiểu khai báo của nguồn, dữ liệu chép bằng INSERT thường trong một giao dịch, trùng tên cột → `42701` (`DT11`, `W4_05`–`W4_08`) — khớp PG
- [ ] 🟡 P2 `(LIKE t INCLUDING ALL)` tạo bảng không có cột (`DT12`)
- [ ] 🟡 P3 `INHERITS` bị bỏ qua (`DT13`)
- [ ] ❌ P2 `PARTITION BY`, `PARTITION OF` (`DT14`)
- [ ] 🟡 P3 `EXCLUDE USING gist` bị bỏ qua (`DT16`)
- [ ] 🟡 P2 `UNIQUE NULLS NOT DISTINCT` bị bỏ qua (`DT17`)
- [x] ✅ `DROP TABLE [IF EXISTS] ... [CASCADE]`, chặn khi có phụ thuộc (`DT21`, `DT22`, `DT22b`)

### 12.2 ALTER TABLE
- [x] ✅ `ADD COLUMN [IF NOT EXISTS] ... DEFAULT ... NOT NULL` (`AT01`, `AT02`)
- [x] ✅ `DROP COLUMN [IF EXISTS] [CASCADE]` (`AT03`, `AT04`)
- [x] ✅ `RENAME COLUMN`, `RENAME TO` (`AT05`, `AT06`)
- [x] ✅ `ALTER COLUMN ... TYPE ... [USING]` (`AT07`, `AT08`)
- [x] ✅ `SET/DROP DEFAULT`, `SET/DROP NOT NULL` (`AT09`–`AT12`)
- [x] ✅ `ADD CONSTRAINT CHECK/UNIQUE/FOREIGN KEY`, kiểm tra cả dữ liệu cũ và có cưỡng chế (`AT13`, `AT13b`, `AT14`, `AT15`, `X18`)
- [x] ✅ `DROP CONSTRAINT`, `NOT VALID` + `VALIDATE CONSTRAINT`, `RENAME CONSTRAINT` (`AT16`–`AT18`)
- [x] ✅ `ALTER TABLE IF EXISTS`, nhiều action một câu (`AT19`, `AT20`)
- [x] ✅ `SET SCHEMA`, `OWNER TO`, `ENABLE ROW LEVEL SECURITY` (chấp nhận, no-op) (`AT22`–`AT24`)
- [ ] 🟡 P2 `ALTER COLUMN ... ADD GENERATED AS IDENTITY` không báo lỗi khi cột đã có default (`AT21`)
- [ ] ⬜ P3 `ATTACH/DETACH PARTITION`, `SET (storage params)`

### 12.3 INDEX
- [x] ✅ `CREATE [UNIQUE] INDEX [IF NOT EXISTS]`, unique có cưỡng chế, partial unique (`IX01`–`IX03`, `IX02`, `IX02b`)
- [x] ✅ `DESC`, `INCLUDE`, `USING gin/btree`, opclass, `CONCURRENTLY`, partial `WHERE` (`IX05`–`IX08`, `IX13`)
- [ ] 🟡 P1 `UNIQUE` index trên biểu thức `(lower(name))` không cưỡng chế (`IX04`)
- [x] ✅ `DROP INDEX [IF EXISTS]` (`IX09`, `IX10`)
- [ ] ❌ P2 `ALTER INDEX ... RENAME TO` (`IX11`)
- [ ] ❌ P3 `REINDEX` (`IX12`)

### 12.4 SEQUENCE
- [ ] ❌ **P0** `nextval`, `currval`, `setval`, `pg_get_serial_sequence` (`SQ01`, `SQ03`, `SQ06`)
- [ ] 🟡 P1 `lastval()` không báo lỗi khi chưa có giá trị (`SQ07`)
- [ ] ❌ P1 `CREATE/ALTER/DROP SEQUENCE` (`SQ01`–`SQ05`)

### 12.5 VIEW
- [ ] ❌ P1 `CREATE [OR REPLACE] VIEW`, `DROP VIEW` (`V01`–`V03`)
- [ ] ❌ P2 `MATERIALIZED VIEW`, `REFRESH` (`V04`)
- [ ] ❌ P3 View cập nhật được, `WITH CHECK OPTION`, `RECURSIVE VIEW` (`V05`–`V07`)

### 12.6 SCHEMA
- [x] ✅ `SET search_path TO ...` (`SC04`)
- [ ] ❌ P1 `CREATE SCHEMA [IF NOT EXISTS]`, `DROP SCHEMA` (`SC01`–`SC03`)
- [ ] ❌ P2 Phân giải tên theo `search_path` (`SC05`)

### 12.7 TYPE / DOMAIN
- [ ] ❌ P1 `CREATE TYPE ... AS ENUM`, `ALTER TYPE ... ADD VALUE`, `DROP TYPE` (`TY01`, `TY02`)
- [ ] ❌ P2 `CREATE DOMAIN`, `CREATE TYPE ... AS RANGE` (`TY03`, `TY04`)

### 12.8 Function / Procedure / Trigger
- [x] ✅ `DO $$ ... $$` (`FN05`)
- [ ] ❌ P2 `CREATE [OR REPLACE] FUNCTION ... LANGUAGE sql/plpgsql`, `RETURNS TABLE` (`FN01`, `FN02`, `FN06`)
- [ ] ❌ P2 `CREATE TRIGGER` (`updated_at` trigger rất phổ biến) (`FN03`, `FN08`)
- [ ] ❌ P2 `CREATE PROCEDURE`, `CALL` (`FN04`)
- [ ] ❌ P2 `DROP FUNCTION` (`FN07`)
- [ ] ❌ P3 `CREATE RULE/AGGREGATE/OPERATOR/CAST` (`FN09`–`FN12`)

## 13. Transaction

- [x] ✅ `BEGIN/COMMIT/ROLLBACK`, `START TRANSACTION`, `END`, `ABORT` (`TX01`–`TX03`, `TX12`)
- [x] ✅ `SAVEPOINT`, `ROLLBACK TO`, `RELEASE` (`TX04`, `TX05`)
- [x] ✅ **P0** DDL theo giao dịch: `ROLLBACK` / `ROLLBACK TO SAVEPOINT` hoàn tác `CREATE` / `ALTER` / `DROP TABLE` cả trong bộ nhớ lẫn khi mở lại DB (trước đây `DROP TABLE` đã ROLLBACK vẫn làm mất bảng) (test `transaction_test`)
- [x] ✅ `BEGIN ISOLATION LEVEL ...`, `BEGIN READ ONLY`, `SET TRANSACTION ...`, `SET CONSTRAINTS ...` (chấp nhận) (`TX05`–`TX08`)
- [x] ✅ `pg_advisory_xact_lock` (`TX10`)
- [ ] ⬜ P2 `READ ONLY` có chặn ghi không
- [ ] ❌ P2 `LOCK TABLE ... IN ... MODE` (`TX09`)
- [ ] ❌ P3 `PREPARE TRANSACTION` (`TX11`)

## 14. Cấu hình phiên

- [x] ✅ `SET x = v`, `SET x TO v`, `SET LOCAL`, `SET TIME ZONE` (có hiệu lực thật, xem 18.3), `SET NAMES`, `RESET` (`CF01`, `CF02`, `CF05`, `CF07`, `CF08`, `CF11`, `CF12`, `W5_01`–`W5_05`)
- [x] ✅ `SHOW name` (`CF03`; tên cột chưa đúng)
- [ ] ❌ P2 `SHOW ALL` (`CF04`)
- [ ] 🟡 **P0** `set_config(...)`, `current_setting(...)` trả nguyên văn (`CF06`)
- [ ] 🟡 **P0** `version()`, `current_database()`, `current_schema()`, `current_user`, `session_user`, `current_schemas()` (`CF09`, `CF10`, `X04`)

## 15. Lệnh tiện ích

- [x] ✅ `CREATE EXTENSION [IF NOT EXISTS] pgcrypto / "uuid-ossp"`, `uuid_generate_v4()` (`UT18`, `UT19`)
- [x] ✅ `LISTEN`, `NOTIFY`, `UNLISTEN`, `DISCARD ALL`, `COMMENT ON` (chấp nhận) (`UT12`, `UT13`, `UT15`–`UT17`)
- [ ] 🟡 P2 `pg_notify()` (`UT14`)
- [ ] ❌ P1 `EXPLAIN [ANALYZE] [(FORMAT JSON)]` (`UT01`–`UT03`)
- [ ] ❌ P2 `ANALYZE`, `VACUUM`, `CHECKPOINT` (nên no-op) (`UT04`–`UT07`)
- [ ] ❌ P1 `PREPARE / EXECUTE / DEALLOCATE` (`UT08`, `UT09`)
- [ ] ❌ P2 Cursor `DECLARE / FETCH / CLOSE` (`UT10`, `UT11`)
- [x] ✅ P2 `obj_description`, `col_description` (`CAT20`) — khớp PG
- [ ] ❌ P3 `CLUSTER`, `SECURITY LABEL`, `IMPORT FOREIGN SCHEMA`, FDW, publication, statistics (`UT20`–`UT22`)

## 16. Bảo mật / CSDL

- [x] ✅ `SET ROLE` (chấp nhận) (`SEC07`)
- [ ] ❌ P2 `GRANT`, `REVOKE`, `ALTER DEFAULT PRIVILEGES` (migration có sinh, nên no-op) (`SEC03`, `SEC04`, `SEC08`)
- [ ] ❌ P3 `CREATE/ALTER/DROP ROLE|USER`, `CREATE POLICY` (`SEC01`, `SEC02`, `SEC05`, `SEC06`, `SEC09`)
- [ ] ❌ P3 `CREATE/DROP DATABASE`, `CREATE TABLESPACE` (`DB01`–`DB03`)

## 17. System catalog

- [x] ✅ `information_schema.columns` (`CAT02`)
- [ ] 🟡 **P1** `SELECT table_name FROM information_schema.tables` trả **mọi cột** thay vì cột được chọn (`CAT01`)
- [ ] 🟡 P1 `pg_tables` cũng trả mọi cột và sai tên cột (`CAT13`)
- [ ] 🟡 P1 `information_schema.table_constraints`, `.key_column_usage`, `.schemata`: catalog tên `litepostgres` (PG: tên DB), thiếu NOT NULL constraint, thiếu schema hệ thống (`CAT03`, `CAT04`, `CAT06`)
- [ ] 🟡 P1 `pg_class`, `pg_namespace`, `pg_constraint`, `pg_index` sai cột / thiếu dòng hệ thống (`CAT07`, `CAT10`, `CAT12`, `CAT17`)
- [ ] ❌ P1 `information_schema.referential_constraints`, `.views` (`CAT05`, `CAT21`)
- [ ] ❌ **P1** `pg_attribute`, `pg_type`, `pg_indexes`, `pg_proc`, `pg_enum`, `pg_settings`, `pg_database`, `pg_extension`, `pg_stat_activity` (`CAT08`, `CAT09`, `CAT11`, `CAT14`–`CAT16`, `CAT19`, `CAT22`, `CAT23`)
- [ ] 🟡 P1 `format_type`, `pg_get_indexdef`, `pg_get_constraintdef`, `to_regclass` (`CAT18`, `CAT17`, `SYS05`, `CAT24`)

## 18. Hàm dựng sẵn

### 18.1 Toán học
- [x] ✅ `abs`, `ceil`, `floor`, `round`, `trunc`, `mod`, `sign`, `sqrt`, `cbrt`, `power`, `exp`, `ln`, `log`, `pi`, `degrees`, `radians`, `random()` (`F01`, `F02`, `F01b`)
- [ ] 🟡 P2 Lượng giác `sin/cos/tan/asin/acos/atan/atan2` (`F04`)
- [ ] 🟡 P2 `div`, `gcd`, `lcm`, `factorial`, `width_bucket` (`F05`)
- [ ] 🟡 P3 `scale`, `trim_scale`, `min_scale`, `setseed`, `random(min, max)` (`F06`, `X12`, `X20`)

### 18.2 Chuỗi
- [x] ✅ `length`, `char_length`, `octet_length`, `lower`, `upper`, `initcap` (`STR01`, `STR02`)
- [x] ✅ `substring(s FROM i FOR n)`, `substr`, `position(x IN s)`, `strpos`, `left`, `right` (`STR03b`, `STR05b`, `STR12`)
- [x] ✅ `trim/ltrim/rtrim/btrim`, `lpad`, `rpad`, `repeat`, `reverse`, `replace`, `translate`, `concat`, `concat_ws`, `split_part`, `array_to_string` (`STR06`–`STR10`)
- [x] ✅ P1 `string_to_array` trả JSON thay vì mảng PG (`STR09`) — khớp PG
- [ ] 🟡 P1 `substring(s FROM 'regex')` (`STR04`)
- [ ] 🟡 P1 `trim(BOTH|LEADING|TRAILING x FROM s)` (`STR06`)
- [ ] 🟡 P1 `format('%s %I %L', ...)` (`STR11`)
- [ ] 🟡 P1 `regexp_replace`, `regexp_matches`, `regexp_split_to_array` (`STR13`)
- [ ] ❌ P1 `regexp_split_to_table` (`STR15`)
- [ ] 🟡 P2 `regexp_like/count/substr/instr` (`STR14`)
- [x] ✅ `md5`, `ascii`, `chr` (`STR16`, `STR17`)
- [ ] 🟡 P1 `sha256`, `encode`, `decode` (định dạng bytea) (`STR16`)
- [ ] 🟡 P2 `quote_ident`, `quote_literal`, `quote_nullable` (`STR17`)
- [ ] 🟡 P2 `starts_with`, `^@` (`STR18`)
- [ ] 🟡 P1 `to_hex`, `to_char(number, fmt)`, `to_number` (`STR19`)
- [ ] 🟡 P3 `overlay`, `bit_length`, `normalize`, `casefold` (`STR08`, `STR01`, `STR22`)

### 18.3 Ngày giờ
- [x] ✅ **P0** Múi giờ phiên: `SET TIME ZONE 'Asia/Ho_Chi_Minh'` / `'+07'` / `7`, `SET timezone = ...`, `LOCAL`/`DEFAULT`/`RESET`, biến môi trường `PGLITE_TIMEZONE`; múi giờ lạ → `22023`. Mặc định UTC nên DB cũ không đổi gì (`W5_01`–`W5_05`, `W5_25`, `W5_26`, `W5_34`) — khớp PG
  - [x] `now()` / `current_timestamp` là thời điểm UTC (JSON vẫn `...Z` như cũ); ở wire in theo múi giờ phiên `2024-01-02 03:00:00+07`
  - [x] `current_date`, `localtimestamp`, `localtime`, `current_time` theo múi giờ phiên
  - [x] ép / ghi timestamptz → `timestamp` / `date` (gồm `DEFAULT now()`, `DEFAULT current_date`) ra giờ địa phương; `timestamp` → `timestamptz` hiểu là giờ địa phương
  - [x] `extract`, `date_trunc`, `to_char`, `age` trên timestamptz tính theo giờ địa phương; `date_trunc(unit, ts, 'zone')`
  - [x] bảng múi giờ nhúng (~90 vùng, DST châu Âu / Mỹ / Úc / NZ); không có tzdata đầy đủ
- [x] ✅ `now()`, `current_timestamp/date/time`, `localtimestamp`, `localtime` (`DTF01`)
- [x] ✅ P1 `clock_timestamp`, `statement_timestamp`, `transaction_timestamp`, `timeofday` (`DTF02`, `X03`, `W5_29`) — chạy; giá trị thời gian thực nên golden chỉ so hình dạng
- [x] ✅ **P1** `date_trunc` đủ đơn vị: microseconds … `week` (thứ Hai), `quarter`, `decade`, `century`, `millennium`, trên interval; đơn vị sai → `22023` (`X13`, `W5_06`, `W5_24`, `W5_33`) — khớp PG
- [x] ✅ **P1** `extract` / `date_part` đủ trường: `isodow`, `dow`, `week` (ISO), `isoyear`, `doy`, `quarter`, `decade`, `century`, `millennium`, `julian`, `epoch`, `second`/`milliseconds`/`microseconds` có phần lẻ, `timezone*`; trên interval và time; trường sai → `22023` (`W5_07`–`W5_09`, `W5_23`, `W5_28`, `W5_30`) — khớp PG
- [x] ✅ P1 `to_char(timestamp|date|interval, fmt)` đủ mẫu: `YYYY`, `IYYY`, `MM`, `Mon/Month` (đệm 9 ký tự như PG), `Dy/Day`, `DD`, `DDD`, `D`, `ID`, `IW`, `WW`, `Q`, `HH/HH12/HH24`, `MI`, `SS`, `MS`, `US`, `AM/PM`, `TZ`, `OF`, `J`, `CC`, `FM`, `TH`, `"chữ"` (`X14`, `W5_10`) — khớp PG
- [x] ✅ `age(a, b)` mượn theo độ dài tháng, có phần giờ, kết quả âm; `age(ts)` (`X13`, `W5_15`) — khớp PG
- [x] ✅ `timestamp ± interval`, `date - date`, `date + int` (`DTF06c`, `X01`)
- [x] ✅ P1 `to_date`, `to_timestamp(text, fmt)` (YYYY, MM, DD, Mon/Month, HH24/HH12, MI, SS, MS, US, AM/PM, DDD), `to_timestamp(epoch)`; ngày sai → `22008` (`X02`, `DTF08`, `W5_11`, `W5_12`) — khớp PG
- [x] ✅ P2 `make_date`, `make_time`, `make_timestamp`, `make_timestamptz`, `make_interval` (vị trí và `days => 3`); ngày sai → `22008` (`DTF09`, `W5_13`, `W5_14`) — khớp PG
- [x] ✅ P3 `justify_days/hours/interval`, `OVERLAPS` (cả dạng có interval), `isfinite`, `'epoch'`, `'infinity'` (`DTF10`, `DTF11`, `W5_15`, `W5_16`) — khớp PG; `'today'`/`'tomorrow'`/`'yesterday'` chạy (`DTF13` chỉ lệch vì ngày sinh kết quả PG khác ngày chạy test)
- [x] ✅ P1 `AT TIME ZONE`, `timezone(zone, ts)` (`X05`, `DTF15`, `K_E20`–`K_E22`, `W3_10`–`W3_13`) — khớp PG (xem mục 3)
- [x] ✅ P2 `generate_series(timestamp | date, ..., interval)`, cả theo múi giờ phiên (`DTF16`, `W5_18`, `W5_19`, `W5_22`) — khớp PG
- [x] ✅ P2 `date_add`, `date_subtract` (có tham số múi giờ), `date_bin` (`DTF17`, `W5_17`) — khớp PG
- [x] ✅ P1 WHERE với ngày giờ: `created > now() - interval '1 hour'` (trước đây so với `now()`), `created::date = current_date` (trước đây bỏ qua ép kiểu), biểu thức hằng trong WHERE / VALUES (`W5_31`, `W5_32`, `W5_35`) — khớp PG

### 18.4 JSON / JSONB
- [x] ✅ `->`, `->>`, `#>`, `#>>`, `(meta->>'a')::int` (`JS01`, `JS23`)
- [ ] 🟡 P1 `@>`, `<@`, `?`, `?|`, `?&` với cột `NULL` trả `false` thay vì `NULL` (`JS02`)
- [ ] 🟡 **P0** `jsonb || jsonb` nối như chuỗi; `jsonb - key`, `#- path` ra `NULL` (`JS03`, `JS03b`)
- [x] ✅ `jsonb_build_object`, `json_build_object`, `jsonb_build_array`, `json_build_array` (`JS04`)
- [ ] ⬜ P0 `to_json(row)`, `to_jsonb(row)`, `row_to_json(row)` (chạy được, chưa so giá trị) (`JS05`, `JS05b`)
- [x] ✅ `jsonb_set`, `jsonb_array_length`, `jsonb_typeof`, `json_typeof`, `jsonb_strip_nulls`, `jsonb_pretty` (`JS06`, `JS11`, `JS12`)
- [ ] 🟡 P1 `jsonb_insert`, `jsonb_set_lax` (`JS06`)
- [ ] 🟡 P1 `jsonb_object_keys`, `jsonb_array_elements_text` trong SELECT (`JS08`, `JS10`)
- [ ] ❌ P1 `jsonb_each`, `jsonb_array_elements` trong FROM (`JS07`, `JS09`)
- [ ] ❌ P2 `jsonb_to_record(set)`, `jsonb_populate_record` (`JS13`–`JS15`)
- [ ] 🟡 P2 jsonpath `jsonb_path_query`, `jsonb_path_exists`, `@?`, `@@` (`JS16`)
- [ ] 🟡 P2 Subscript `jsonb['a']` (`JS17b`)
- [ ] 🟡 P3 `json_object(...)`, SQL/JSON `IS JSON`, `JSON_QUERY/VALUE/EXISTS`, `JSON_ARRAYAGG/OBJECTAGG` (`JS18`, `JS19`, `JS21`, `JS22`)
- [ ] ❌ P3 `JSON_TABLE` (`JS20`)

### 18.5 Mảng
- [x] ✅ `@>`, `<@`, `&&`, `=`, `<` (`AR02`, `AR11`)
- [ ] 🟡 P1 `||` giữa mảng/phần tử: `0 || ARRAY[1]`, `ARRAY[1] || ARRAY[2]` sai (`AR01`, `AR01b`)
- [x] ✅ P1 `array_append/prepend/cat/remove/replace` đúng giá trị nhưng output dạng JSON (`AR04`) — khớp PG
- [x] ✅ `array_length`, `array_dims`, `array_upper`, `array_lower` (`AR03`)
- [ ] 🟡 P1 `cardinality`, `array_ndims`, `array_position(s)` (`AR03`, `AR05`)
- [x] ✅ **P0** `unnest(array)` trong SELECT khi là mục duy nhất, trong FROM (`AR06`, `STR20`) — khớp PG; SRF đứng cạnh cột khác trong SELECT để đợt 4
- [x] ✅ P1 `unnest(a, b)` trong FROM, mảng dài ngắn khác nhau (`AR07`, `K_R05`) — khớp PG. Sửa luôn lỗi `unnest(...) AS t` đè rồi xoá mất bảng thật tên `t` khỏi bộ nhớ
- [x] ✅ P1 `WITH ORDINALITY` (`AR08`) — khớp PG
- [ ] 🟡 P3 `array_fill`, `trim_array`, `array_reverse`, `array_sort` (`AR09`)

### 18.6 Hàm trả tập (SRF)
- [x] ✅ **P1** `generate_series(int | numeric | timestamp | date, ...[, step])` trong FROM, có alias / danh sách cột, bước âm, `WITH ORDINALITY` (`SRF01`, `SRF03`, `W5_18`–`W5_22`) — khớp PG; JOIN / LATERAL với SRF để đợt 4
- [x] ✅ P1 `generate_series(...)` trong SELECT (`SRF02`) — khớp PG
- [ ] 🟡 P2 `generate_subscripts` (`SRF04`)
- [ ] ❌ P3 `ROWS FROM (...)` (`SRF05`)

### 18.7 Hệ thống
- [x] ✅ `gen_random_uuid()` (`T17`)
- [ ] ⬜ P2 `uuidv4()`, `uuidv7()`, `pg_backend_pid`, `txid_current`, `pg_size_pretty`, `pg_relation_size`... (chỉ so được số dòng) (`SYS01`, `SYS03`, `SYS06`, `SYS07`)
- [ ] 🟡 P2 `pg_sleep`, `has_table_privilege`, `pg_has_role` (`SYS02`, `SYS04`)
- [ ] 🟡 P2 pgcrypto `crypt`, `gen_salt`, `digest`, `hmac` (`SYS08`)

### 18.8 Full-text search / Range / Geometry / XML
- [ ] 🟡 P2 `to_tsvector`, `to_tsquery`, `@@`, `ts_rank`, `ts_headline`... (`FTS01`–`FTS03`)
- [ ] 🟡 P2 Toán tử/hàm range `&&`, `lower`, `upper`, `isempty` (`GEO01b`, `RNG01`)
- [ ] 🟡 P3 Toán tử hình học `<->`, `@>` (`GEO01`)
- [ ] 🟡 P3 `xmlelement`, `xpath` (`XML01`)

## 19. Xử lý lỗi

- [x] ✅ Vi phạm PK/UNIQUE, FK (insert và delete cha), NOT NULL, CHECK (`ERR01`, `ERR02`, `DT03*`, `DT05c`)
- [x] ✅ Bảng không tồn tại, cột không tồn tại trong `INSERT (cols)` (`ERR04`, `ERR08`)
- [x] ✅ Sai kiểu khi ghi, thừa giá trị trong `VALUES` (`ERR07`, `ERR11`)
- [x] ✅ **P0** Cột không tồn tại trong SELECT/WHERE (`ERR03`, `ERR09`) — khớp PG
- [x] ✅ **P0** Chia cho 0, cast không hợp lệ, tràn số (`ERR05`, `ERR10`, `F09`) — chia 0 và cast đã đúng; tràn int4 khi tính toán (`F09`) cần kiểu khai báo (đợt 2b) — khớp PG
- [x] ✅ P1 Thông báo lỗi cú pháp theo kiểu PG (`syntax error at or near "SELEC"`) (`ERR06`) — khớp PG

---

## Lộ trình gợi ý (phần còn lại)

1. **Mục 0** (còn lại): lỗi `42883` khi gặp hàm không biết (sau đợt 5), mất cột trùng tên trong kết quả JSON (đợt 3), scale của numeric khi tính toán. Đã xong: chia nguyên, chia 0, cast, tràn số, cột không tồn tại, định dạng wire của timestamp/numeric/mảng/ROW/bytea/jsonb.
2. **P0 còn lại**: IDENTITY, sequence functions, `version()`/`current_*()`/`set_config`, `jsonb ||`/`-`, `unnest`.
3. **P1**: `generate_series`, regex functions, `to_date/to_timestamp`, `AT TIME ZONE`, `json_object_agg`, views, `CREATE SCHEMA`, enum, `VALUES` độc lập, `CREATE TABLE AS`, `TEMP TABLE`, catalog (`pg_attribute`, `pg_type`, `pg_index`, cột được chọn trong `information_schema.tables`), `EXPLAIN`, `PREPARE/EXECUTE`, `COPY FROM STDIN`.
4. **P2–P3**: functions/triggers, MERGE, partitioning, grouping sets, FTS, range, SQL/JSON, quyền hạn.

Khi triển khai xong một mục: thêm case vào `crates/pglite-rs/tests/pg_golden/cases.tsv`, sinh kết quả PG bằng
`node scripts/pg_golden/generate.mjs <tiền tố id>`, chạy `cargo test --release --test pg_golden_test` (xoá case đã khớp
khỏi `known_gaps.txt`), rồi `python3 scripts/pg_golden/checklist_status.py --apply` và sửa tay các dòng không có probe.
