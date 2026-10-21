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
| Zalo-1054 + lỗi WAIKI (lần 1–2) | `ac748b0` | 666 / 1034 | 20 (không đổi) | regex (`regexp_*`), `format()`, `ANY(mảng)` trong JOIN / subquery, subquery tương quan trong SELECT / UPDATE SET / INSERT VALUES, `DEFAULT nextval(...)` + `setval`/`nextval`/`pg_get_serial_sequence` (sequence gắn với cột), `udt_name` mảng, PL/pgSQL có biến / vòng lặp / `EXECUTE format()`, `CREATE TEMP TABLE` (không xuống WAL), `GRANT`/`REVOKE` no-op, `pg_namespace`, GREATEST/LEAST và phép toán trong JOIN, self-join; +27 `ZA_*`, +14 `BG_*` |
| Lỗi db-rs (lần 3–5) + P0 | `f53bc65` | 706 / 1058 | 20 (không đổi) | `$n` trong SELECT qua plan cache, `CASE`/`CAST` quanh GREATEST trong JOIN, ép kiểu hàm gộp trong JOIN, `LIMIT` + nhiều bảng, `EXISTS` tương quan trong JOIN, `current_setting`/`set_config`, `::text` của numeric(p,s)/jsonb, JSON mặc định không đoán sai jsonb / `::text` / chuỗi `{...}` không phải mảng; P0: `GENERATED AS IDENTITY` (`SchemaOp::ColumnIdentity` thêm cuối enum, bản cũ bỏ qua), `version()`/`current_database()`/`current_user`..., `jsonb - key`/`#- path`, cả dòng `json_agg(t)`/`to_json(t)`; +11 `BG_*`, +13 `W6_*` |
| P1 không cần metadata (đợt 7) | `eeff801` | 758 / 1079 | 20 (không đổi) | cột trùng tên qua JOIN (wire), `information_schema.tables`/`pg_tables` lọc + đúng cột, `UPDATE SET (a, b) = (...)`, `NATURAL JOIN`, `json[b]_object_agg`, `jsonb_each`/`jsonb_array_elements`/`jsonb_object_keys` trong FROM, `ANALYZE`/`VACUUM`/`CHECKPOINT`, `PREPARE`/`EXECUTE`/`DEALLOCATE`; sửa 3 lỗi ghi sai dữ liệu có sẵn: subquery không tương quan trong `UPDATE SET` (ghi `false`), `UPDATE ... FROM` (ghi NULL), `ON CONFLICT DO UPDATE SET` (ghi nguyên văn); jsonb chuỗi vô hướng qua `SELECT *` trên wire mất nháy |
| Đối tượng có metadata (đợt 8) | `ec3f85f` | 838 / 1114 | 20 (không đổi) | `CREATE/ALTER/DROP SEQUENCE` (+ `lastval`, `currval` theo phiên, `DEFAULT nextval('s')`), `VIEW` (mở rộng thành CTE; lồng nhau; `OR REPLACE`; `TEMP`; cập nhật được qua view một bảng; `WITH CHECK OPTION`; `RECURSIVE`), `MATERIALIZED VIEW` + `REFRESH`, `CREATE/DROP SCHEMA` (+ `DROP SCHEMA public CASCADE`), `ENUM` (kiểm tra nhãn, thứ tự trong ORDER BY / so sánh, `enum_range`), `DOMAIN`; catalog `.views`/`pg_views`/`pg_matviews`/`.sequences`/`pg_sequences`/`pg_enum`/`.schemata`; 6 `SchemaOp` mới thêm cuối enum (bản cũ bỏ qua, đã kiểm với `50bdd5b`, `8f9b56c`); +35 `W8_*` |
| Thư viện hàm & aggregate (đợt 9) | `a393ccd` | 963 / 1149 | 20 (không đổi) | `engine/fnlib.rs` (một registry cho cả 4 bộ đánh giá): lượng giác, `div/gcd/lcm/factorial/width_bucket`, `scale`, `setseed/random(a,b)`, `to_char(số)`/`to_number`/`to_hex`, `quote_*`, `starts_with`/`^@`, `normalize/casefold`, `overlay`/`trim(BOTH ... FROM)`/`substring(FROM regex)`, `regexp_instr`, mảng (`cardinality`, `array_position(s)`, `array_fill/sort/reverse`, `trim_array`, `\|\|` với mảng), pgcrypto (`crypt/gen_salt` bcrypt + md5, `digest/hmac`), `unaccent`, `convert_from`; JSON: `jsonb_insert`, `jsonb_set_lax`, jsonpath (`engine/jsonpath.rs`: `jsonb_path_*`, `@?`, `@@`), `JSON_VALUE/QUERY/EXISTS`, `IS JSON`, `json_object`, subscript `jsonb['a']`, NULL ở `@> ? ?\| ?&`; hàm trả tập `engine/srf.rs` (`jsonb_object_keys`/`jsonb_array_elements[_text]`/`jsonb_path_query`/`regexp_matches g`/`regexp_split_to_table`/`generate_subscripts` trong SELECT, `jsonb_to_record[set]`, `jsonb_populate_record`, `ROWS FROM`, `JSON_TABLE`); aggregate `bit_*`, `stddev/variance`, `corr/covar/regr_*`, `any_value`, `percentile_cont/disc`, `mode`, hypothetical `rank(x) WITHIN GROUP`, `JSON_ARRAYAGG/OBJECTAGG`, `xmlagg`; lỗi `42883`; scale numeric trên wire; float8 in như PG; +35 `W9_*`. Thêm crate `bcrypt` (không getrandom), `hmac`; không thêm `SchemaOp`; 36/36 DB khớp `50bdd5b`, 35/35 khớp `8f9b56c`; hiệu năng ±2% so với đợt 8 |
| Window / JOIN / CTE / MERGE (đợt 10) | `7cc278a` | 1002 / 1169 | 20 (không đổi) | window frame `RANGE n PRECEDING/FOLLOWING`, `GROUPS`, `EXCLUDE CURRENT ROW/GROUP/TIES` (tìm nhị phân trên khoá đã sắp); `FROM (a JOIN b ON ...)`; `LATERAL f(...)` (hàm trả tập theo từng dòng ngoài, `SELECT *` gồm cột của LATERAL); hai `jsonb_each` trong một FROM; CTE có `UPDATE/INSERT/DELETE ... RETURNING` (kiểm tra cột không tồn tại ở câu chính); `SEARCH DEPTH/BREADTH FIRST`, `CYCLE`; `UPDATE SET a[i] = v` / `SET j['k'] = v`; `OVERRIDING USER VALUE`; `MERGE` đầy đủ (`engine/merge.rs`); +20 `W10_*`. Không thêm `SchemaOp`; 36/36 DB khớp `50bdd5b`, 35/35 khớp `8f9b56c`; hiệu năng ±1,5% |
| DDL bảng (đợt 11) | `cf7969d` | 1041 / 1191 | 20 (không đổi) | `SchemaOp::TableMeta` (một biến thể khoá → giá trị, thêm cuối enum; bản cũ bỏ qua, đã kiểm `50bdd5b`/`8f9b56c` đọc DB mới ra dữ liệu y hệt): cột `GENERATED ... STORED` (INSERT / UPDATE / ON CONFLICT / MERGE / ADD COLUMN / DROP EXPRESSION), `UNIQUE NULLS NOT DISTINCT`, UNIQUE trên biểu thức (+ `ON CONFLICT (lower(x))`), `EXCLUDE (... WITH =)`, khoá ngoại `DEFERRABLE` + `SET CONSTRAINTS`, `LIKE ... INCLUDING`, `INHERITS` (cột), `UNLOGGED`, `ADD GENERATED AS IDENTITY` / `DROP IDENTITY` / `SET GENERATED`, `ALTER INDEX RENAME`, `REINDEX`, RESTRICT `23001`; test `table_meta_test.rs`; +22 `W11_*` |
| Function / procedure / trigger (đợt 12) | `2a54d80` | 1087 / 1213 | 20 (không đổi) | `engine/routines.rs`: `CREATE/DROP FUNCTION/PROCEDURE`, `CALL`, SQL + PL/pgSQL (`RETURN expr`, `RETURN NEXT/QUERY`, OUT/INOUT/VARIADIC/DEFAULT, gọi theo tên, overload, STRICT, đệ quy), inline hàm SQL một biểu thức, gọi theo dòng trong danh sách SELECT, hàm trả tập trong FROM / LATERAL, `DEFAULT f()`; `engine/triggers.rs`: trigger BEFORE/AFTER ROW/STATEMENT trên INSERT/UPDATE/DELETE, `UPDATE OF`, `WHEN`, `TG_*`, ENABLE/DISABLE/DROP; PL/pgSQL: `RAISE ... USING ERRCODE` (SQLSTATE riêng, `RAISE` mặc định `P0001`), `EXCEPTION WHEN` theo mã SQLSTATE. Lưu: `SchemaOp::ObjectMeta` mới (thêm cuối enum) cho routine, trigger trong `TableMeta`; bản cũ đọc DB có hàm / trigger ra dữ liệu y hệt; test `routines_test.rs`; +22 `W12_*` |
| Lệnh tiện ích & giao dịch (đợt 13) | `f2eb686` | 1133 / 1241 | 20 (không đổi) | `EXPLAIN [ANALYZE] [(FORMAT JSON / COSTS OFF / BUFFERS)]` (`engine/explain.rs`); `COPY FROM STDIN / TO STDOUT / file` text + CSV (`engine/copy.rs`, API mới `copy_from`/`copy_to`, `copyFrom`/`copyTo`); cursor `DECLARE/FETCH/MOVE/CLOSE` + `WHERE CURRENT OF`, `READ ONLY` (`25006`), `SET LOCAL`, `LOCK TABLE` (`25P01`), `PREPARE TRANSACTION` / `COMMIT PREPARED`, `SHOW ALL` (bảng tham số của PG, `pg_settings_data.rs` sinh từ PG), tên cột `SHOW` trên wire, `LISTEN/NOTIFY/pg_notify` có hàng đợi (`notifications()`) (`engine/session.rs`); lệnh chưa hỗ trợ → `0A000`, từ khoá lạ → `42601`; sửa `"excluded".cột` trong `ON CONFLICT` (ghi sai dữ liệu, mẫu ORM), `coalesce((subquery tương quan))` trả `__sq1.__sqv`; test `session_test.rs`; +28 `W13_*`. Không thêm `SchemaOp` |
| System catalog (đợt 14) | `319f563` | 1190 / 1273 | 20 (không đổi) | `engine/catalog_views.rs`: một mô hình catalog (OID nhất quán) → bảng tạm đủ cột như PG cho `pg_attribute`, `pg_type`, `pg_index`, `pg_indexes`, `pg_constraint` (cả `n` NOT NULL), `pg_class` (bảng, index, sequence, view), `pg_namespace`, `pg_proc` (hàm dựng sẵn + routine người dùng), `pg_settings`, `pg_database`, `pg_extension`, `pg_stat_activity`, `pg_roles`, `pg_user`, `pg_attrdef`, `pg_trigger`, `pg_description`, `pg_am`, `pg_sequence`, `information_schema.referential_constraints / check_constraints / triggers / routines / parameters`; hàm `format_type`, `pg_get_*def`, `pg_get_expr`, `::regclass/regtype/regnamespace`, `to_regclass`; dữ liệu dựng sẵn sinh từ PG thật (`pg_catalog_data.rs`, `gen_catalog.mjs`); hai truy vấn introspection của NATA giữ đường cũ, lỗi ở đường mới thì quay về đường cũ; `information_schema` dùng catalog `postgres`; +32 `W14_*`. Không thêm `SchemaOp` |
| Kiểu dữ liệu mở rộng (đợt 15a) | `f7aa5ee` | 1243 / 1308 | 20 (không đổi) | range / multirange (`engine/range.rs`, chuẩn hoá như PG, toán tử viết lại chỉ khi toán hạng chắc chắn là range), full-text search (`engine/fts.rs`: Snowball English tự cài, `ts_rank` theo `tsrank.c`), kiểu composite (`engine/composite.rs`, `SchemaOp::ObjectMeta` kind `composite`), `point` / `<->` / `xmlelement` (`engine/geo.rs`); hình học / XML còn lại báo `0A000` thay vì trả nguyên văn biểu thức; cột `daterange`/`datemultirange` không còn bị hiểu là `date` (chỉ bảng tạo mới); test `types_wave15_test.rs`; +35 `W15_*`. Không thêm `SchemaOp` |

Số golden ở bảng là kết quả của `cargo test --test pg_golden_test`. Script `checklist_status.py` in một con số thấp hơn
(407/921 sau đợt 2b, 444/937 sau mục 3, 551/993 sau 18.3) vì không tính các case chỉ so được hình dạng (thời gian thực, ngẫu nhiên).

Tổng hợp theo mục (cập nhật 2026-10-22, sau đợt 15a — range, FTS, composite): **325 dòng xong / 11 dòng còn lại** (tính cả dòng con).

| Mục | Xong | Còn | | Mục | Xong | Còn |
|---|---|---|---|---|---|---|
| 0. Xuyên suốt | 24 | 1 | | 10. UPDATE | 6 | 1 |
| 1. Lexical | 13 | 0 ✅ | | 11. DELETE/TRUNCATE/MERGE/COPY | 3 | 2 |
| 2. Kiểu dữ liệu | 26 | 4 | | 12. DDL | 48 | 5 |
| 3. Toán tử & biểu thức | 24 | 1 | | 13. Transaction | 5 | 3 |
| 4. SELECT | 22 | 0 ✅ | | 14. Cấu hình phiên | 4 | 1 |
| 5. JOIN | 8 | 0 ✅ | | 15. Lệnh tiện ích | 6 | 3 |
| 6. WITH (CTE) | 5 | 0 ✅ | | 16. Bảo mật / CSDL | 2 | 2 |
| 7. Aggregate | 15 | 0 ✅ | | 17. System catalog | 3 | 5 |
| 8. Window | 7 | 0 ✅ | | 18. Hàm dựng sẵn | 71 | 4 |
| 9. INSERT | 6 | 0 ✅ | | 19. Xử lý lỗi | 6 | 0 ✅ |

4 dòng còn lại của mục 2: composite (`CREATE TYPE ... AS (...)`); range, multirange, tsvector/tsquery. Dòng còn lại của
mục 3 là `(x).field` trên kiểu composite thật. 5 dòng còn lại của mục 18: `jsonb_each` hai hàm trong một FROM / LATERAL
(đợt 10), `pg_get_constraintdef`... (catalog, đợt 14), FTS / range / hình học / XML (đợt 15). Kế hoạch các đợt còn lại:
9 hàm & aggregate ✅ → 10 window/JOIN/CTE/MERGE → 11 DDL bảng (generated, deferrable, unique biểu thức, partition) →
12 function/procedure/trigger → 13 EXPLAIN/COPY/cursor/0A000 → 14 catalog → 15 schema riêng, composite, range, FTS → 16 P3.

| Đợt | Nội dung | Trạng thái |
|---|---|---|
| 0 | Hạ tầng test: golden suite, fixture `legacy_v3`, compat check | ✅ |
| 1 | Lỗi xuyên suốt: chia 0 / cast / tràn số / 25P02 / lexer / cột không tồn tại / nhiều câu = 1 giao dịch | ✅ (`42883` hàm không tồn tại xong ở đợt 9) |
| 2 | Kiểu dữ liệu & định dạng wire | ✅ phần a (numeric(p,s), varchar/char(n), int2, timestamp/timestamptz/timetz, money, nhiều SERIAL, literal có kiểu, DDL theo giao dịch) + phần b (chia nguyên & tràn int theo kiểu, tên cột PG ở wire, mảng nhiều chiều, jsonb `": "`, `ROW()`, bytea `\x`, RETURNING, pg_typeof, 22007/22008, box/bit/jsonpath). Chuyển đợt khác: enum/domain/composite (6), range/multirange/tsvector (10), `AT TIME ZONE` (5) |
| 3 | SELECT / biểu thức còn lại (VALUES, TABLE, cột trùng tên, GROUPING SETS...) | ✅ (mục 4, đợt 7) |
| 4 | Hàm trả tập (generate_series, unnest, jsonb_each...) | ✅ phần lớn (18.3, đợt 7, đợt 9: `engine/srf.rs`, `ROWS FROM`, `JSON_TABLE`, SRF trong SELECT không FROM). Còn: SRF cạnh bảng / LATERAL (đợt 10) |
| 5 | Thư viện hàm & aggregate | ✅ (đợt 9: `engine/fnlib.rs`, `engine/jsonpath.rs`) |
| 6 | Đối tượng DDL (sequence, identity, generated, view, schema, enum, domain, temp, partition...) | ✅ phần lớn (đợt 8): sequence, identity, view (+ materialized, cập nhật được, recursive), schema (chỉ tên), enum, domain, temp table. Còn: composite type, cột `GENERATED ... STORED`, partition, `LIKE`/`INHERITS`, không gian tên theo schema |
| 7 | Function / procedure / trigger (plpgsql) | ⬜ |
| 8 | MERGE, COPY, EXPLAIN, PREPARE, cursor, LISTEN, READ ONLY | ⬜ |
| 9 | System catalog chính xác | ⬜ |
| 10 | Full-text search & range | ⬜ |
| 11 | P3: lệnh quyền hạn = no-op, phần còn lại báo `0A000` rõ ràng | ⬜ |

Tương thích ngược sau mỗi đợt: DB do 8f9b56c và 7db3c5a ghi mở y nguyên (`scripts/compat_check.sh`, 28 DB đã backup,
checksum không đổi); metadata mới chỉ đi bằng `SchemaOp` mới thêm cuối enum (bản cũ bỏ qua).
Đợt 2b (`445a332`): không đổi định dạng file/WAL, không thêm `SchemaOp`; 28/28 DB khớp `7db3c5a`, 27/27 khớp `8f9b56c`
(trừ fixture `legacy_v3` do `7db3c5a` ghi), mở lại lần 2 và bản cũ đọc lại đều y hệt. Hiệu năng chênh < 3% so với trước đợt.
Đợt 8: thêm 6 `SchemaOp` cuối enum (`View`, `Schema`, `Sequence`, `SequenceValue`, `EnumType`, `Domain`); 36/36 DB khớp
`50bdd5b`, 35/35 khớp `8f9b56c` (bỏ fixture `legacy_v3`); DB do bản này ghi (có view, sequence, enum, domain, materialized view)
được cả hai bản cũ mở ra bảng y hệt (chỉ cảnh báo bỏ qua op lạ). Hiệu năng ngang `9b075e0` (chênh trong nhiễu đo).

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

- [x] ✅ **P0** Hàm/biểu thức chưa hỗ trợ bị **trả nguyên văn dạng chuỗi** thay vì báo lỗi `42883`/`42703`. Ví dụ `SELECT nope FROM t` → `"nope"`; `regexp_replace(...)`, `format(...)`, `to_date(...)` trả lại chính câu chữ. (`ERR03`, `STR11`, `X02`, `K_S22`, `W9_27`) — đợt 9: hàm không tồn tại báo `42883 function f(integer) does not exist` như PG (DEFAULT dạng chữ trong DB cũ vẫn giữ hành vi cũ) — cột không tồn tại đã báo `42703`; hàm không tồn tại (`42883`) làm sau đợt 5 — các ví dụ đã có hàm thật (regex, `format`, `to_date`); hàm chưa hỗ trợ khác (vd. `regexp_instr`) vẫn trả nguyên văn — khớp PG
- [x] ✅ **P0** Cột không tồn tại trong `WHERE` bị bỏ qua, trả 0 dòng mà không báo lỗi. (`ERR09`) — khớp PG
- [x] ✅ **P0** `SELECT a.name, b.name` hoặc `SELECT *` qua JOIN **mất cột trùng tên**. PG giữ cả hai cột. (`X10`, `J10`, `S41`, `J15`, `J17`, `J19`) — khớp PG trên đường wire (alias tạm rồi trả tên gốc); kết quả JSON là object theo tên cột nên giữ hành vi cũ (cột sau đè cột trước, như node-postgres)
- [x] ✅ **P0** `'abc'::int` trả `NULL` thay vì lỗi `22P02`. (`ERR10`) — khớp PG
- [x] ✅ **P0** Chia số nguyên: `7 / 2` → `3`, `-7 / 2` → `-3` như PG (literal nguyên, cột INT/BIGINT/SMALLINT, `::int`, `count()`); cột NUMERIC vẫn chia thực (`F08`, `F08b`, `W2_01`, `W2_15`, `W2_16`) — khớp PG
- [x] ✅ **P0** `1/0` không báo lỗi `22012`, kể cả trong transaction. (`ERR05`, `X15`) — khớp PG
- [x] ✅ **P1** Tràn số `int4`/`int8` khi tính: `2147483647 + 1`, `n * 1000000000` → `22003` (`F09`, `W2_03`, `W2_04`); giá trị cũ đã lớn hơn int4 trong DB cũ thì tính theo int8 thay vì báo lỗi — khớp PG
- [x] ✅ **P1** `count(*)` đứng chung với `count(n)`, `count(DISTINCT n)`, `sum`, `avg` (trước đây ra 2 thay vì 3 vì trùng tên cột); biểu thức trên aggregate `sum(n) / count(n)`, `count(*) / 2`, `max(n) + 1`, `count(*)::int`, `coalesce(min(n), 0)`, cả khi có GROUP BY; `sum` của nhóm toàn NULL ra NULL (`A01`, `A01b`, `X09`, `W4_14`, `W4_15`, `W4_22`) — khớp PG
- [x] ✅ **P1** Tên cột mặc định như PostgreSQL ở đường wire, kể cả nhiều cột cùng tên: `5 & 3`, `1 + 1`, `meta->'a'` → `?column?`; `'1'::bit(1)::varbit` → `varbit`; `n::numeric::text` → `n` (`X08`, `E02`, `JS01`, `T02`, `T11`, `T28`, `W2_13`). Kết quả JS (object) vẫn đặt tên cũ cho cột trùng tên để không đè dữ liệu — khớp PG
- [x] ✅ **P1** Định dạng text khác PG (đường wire `query_wire`; JS API giữ dạng JSON cũ):
  - [x] `timestamp` bị thêm `+00` — cột, ép kiểu, `RETURNING`, `date_trunc`, `ts ± interval` đã đúng (`X13`, `T07`, `W2_09`)
  - [x] `numeric` giữ scale theo kiểu khai báo, cả ở `RETURNING` (`I10` chỉ còn lệch cột thời gian thực `created`, `W2_12`)
  - [x] mảng trả từ hàm / `ARRAY[...]` ra `{a,b}`, mảng nhiều chiều `{{1,2},{3,4}}` (`STR09`, `AR04`, `T21`, `W2_07`)
  - [x] `ROW(...)`, `(1, 'a')` ra `(1,a)` (`E30`, `W2_07`)
  - [x] `bytea` từ `decode()` / `sha256()` có tiền tố `\x` (`STR16` chỉ còn lệch `encode(bytea, 'base64')` — hàm chuỗi, đợt 5)
  - [x] `jsonb` in `{"a": 1, "bb": [1, 2]}` (khoá xếp như PG), cả kết quả của `||`, `->` (`W2_08`; `JS03` còn lệch `meta - 'a'`, `#-` — hàm JSON, đợt 5)
  - [x] `min(int)` in `3`; numeric giữ scale khi tính trên wire: hằng `1.500`, `3.00 * 2` → `6.00`, `+`/`-`/`%` lấy scale lớn hơn, `*` cộng scale, `round/trunc(x, n)`, `round(1.005, 2)` = `1.01` (`X09`, `K_F47`, `W9_28`, `W9_29`) — đợt 9; phép chia vẫn in theo giá trị (PG dùng scale động)
- [x] ✅ Alias không có `AS` (`SELECT n "Num"`). (`S02`)
- [x] ✅ Kiểm tra kiểu khi ghi (`INSERT INTO t (n) VALUES ('abc')` → `22P02`). (`ERR07`)
- [x] ✅ `INSERT` có nhiều giá trị hơn số cột → lỗi. (`ERR11`)
- [x] ✅ Window function không còn sinh cột trùng. (`WF01`)
- [x] ✅ Thứ tự NULL mặc định và `NULLS FIRST/LAST`. (`S13b`, `S14b`, `S15b`)
- [x] ✅ SQLSTATE cho các lỗi đã hỗ trợ: 23505, 23503, 23502, 23514, 42P01, 42703, 22P02, 2BP01, 42601 khớp PG ở cả 25 probe cùng báo lỗi.
- [x] ✅ P2 Lệnh chưa hỗ trợ trả `42601` (lỗi cú pháp). PG dùng `0A000 feature_not_supported` cho tính năng không hỗ trợ. (`M01`, `TY03`, `TX09`, `UT10`...) — đợt 13: lệnh hợp lệ của PG mà engine chưa làm → `0A000` (`feature not supported: CREATE RULE is not supported ...`); từ khoá lạ → `syntax error at or near "FOO"` `42601` như PG (`W13_22`)
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
- [x] ✅ P1 `ENUM` (`T12`, `W8_26`–`W8_32`) — khớp PG: nhãn được kiểm tra khi ghi / ép kiểu (`22P02`), `ORDER BY` và `< > <= >=` theo thứ tự khai báo, `enum_range/first/last`, `information_schema.columns` = `USER-DEFINED`; trên wire kiểu enum vẫn mang OID của text
- [x] ✅ P2 Composite type (`T22`) — đợt 15 (`engine/composite.rs`): `CREATE TYPE t AS (...)` (lưu `SchemaOp::ObjectMeta` kind `composite`), `ROW(...)::t`, `'(1,x)'::t`, cột kiểu composite (chuẩn hoá khi ghi), `DROP TYPE` kiểm cột phụ thuộc, `pg_type` (`T22`, `K_Y08`, `K_Y09`, `W15_29`–`W15_33`)
- [x] ✅ P2 Range `int4range`...: literal đã in đúng, toán tử `@>` sai (`T23`) — đợt 15 (`engine/range.rs`): 6 kiểu range, chuẩn hoá range rời rạc, hàm dựng 2/3 đối số, ép kiểu, cột kiểu range, `@> <@ && << >> &< &> -|- + * -`, lỗi `22000` như PG; toán tử chỉ viết lại khi toán hạng chắc chắn là range (`@>` / `&&` của mảng, jsonb không đổi) (`W15_01`–`W15_16`)
- [x] ✅ P3 Multirange (`T24`) — đợt 15: literal / hàm dựng (sắp xếp, gộp range chồng / kề), `@>` phần tử / range, `&&`
- [x] ✅ P2 Domain (`T25`, `TY03`, `W8_33`, `W8_34`) — khớp PG: cột kiểu domain được lưu bằng kiểu gốc + `NOT NULL` / `CHECK` / `DEFAULT` của domain (cùng SQLSTATE, khác câu chữ thông báo); `x::domain` chưa kiểm tra CHECK; `DROP DOMAIN` không chặn khi còn cột dùng
- [x] ✅ P3 Hình học `point`, `box` (box chuẩn hoá góc `(3,4),(1,2)`) (`T13`, `W2_14`) — khớp PG; toán tử hình học chưa có
- [x] ✅ P3 `bit(n)`, `varbit` (`T15`) — khớp PG
- [x] ✅ P2 `tsvector`, `tsquery` (`T16`) — đợt 15 (`engine/fts.rs`), xem mục 18.8
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
- [x] ✅ P2 Truy cập field composite `(x).field` — trên cột không phải composite đã báo `42809` như PG (`E26`, `T22`); đợt 15: `(ROW(...)::t).a`, `(cột).a` trong SELECT / WHERE, giá trị theo kiểu của trường, tên cột wire / JSON = tên trường
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
- [x] ✅ P2 `NATURAL JOIN` (`J09`, `W7_07`) — khớp PG (viết lại thành `JOIN ... USING (cột chung)`)
- [x] ✅ P0 Self-join chọn cột trùng tên (`a.name, b.name`) bị mất cột (`J10`, `X10`) — khớp PG
- [x] ✅ JOIN với derived table, `LEFT JOIN LATERAL (...)`, 3+ bảng, điều kiện ON phức hợp, anti-join (`J11`–`J15`, `J13b`)
- [x] ✅ P2 `LATERAL` với hàm trả tập (`J20`) — khớp PG
- [x] ✅ P2 JOIN trong ngoặc `FROM (a JOIN b ON ...)` (`J16`) — đợt 10 (`W10_05`; `J16`/`K_S08` chỉ còn lệch cột thời gian thực `created`)
- [x] ✅ JOIN + `$1::int`, `lower()`, `@>`, `ORDER BY alias` (`J17b`, `J18`, `J19b`)

## 6. WITH (CTE)

- [x] ✅ CTE thường, nhiều CTE, `RECURSIVE`, `[NOT] MATERIALIZED` (`W01`–`W05`)
- [x] ✅ Data-modifying CTE `DELETE/INSERT ... RETURNING` (`W06`, `W07`)
- [x] ✅ P2 `WITH u AS (UPDATE ... RETURNING *)` — đợt 10 (`W10_09`, `W10_10`); lỗi ở câu chính hoàn tác cả phần ghi; khác PG: câu chính thấy dữ liệu đã đổi (PG dùng snapshot trước câu lệnh)
- [x] ✅ P2 `WITH x(a, b) AS (VALUES ...)` (`W10`) — khớp PG
- [x] ✅ P3 `SEARCH`, `CYCLE` (`W08`, `W09`) — khớp PG

## 7. Hàm tổng hợp (Aggregate)

- [x] ✅ `count`, `count(DISTINCT)`, `sum`, `avg`, `min`, `max`, kết hợp nhiều hàm và biểu thức trên chúng; kiểu wire như PG (`sum(int)` → bigint `8`, `max(numeric(10,2))` → `2.00`) (`A01`, `A01b`, `W4_14`, `W4_22`)
- [x] ✅ `string_agg`, `array_agg`, có `ORDER BY` (`A02`, `A02c`, `A03`)
- [x] ✅ `json_agg(col)`, `jsonb_agg(col)` (`A04b`)
- [x] ✅ P0 `json_agg(t)` với cả dòng, kể cả theo alias `json_agg(x) FROM t x` (`A04`, `W6_01`–`W6_03`) — khớp PG (trong JOIN: chưa)
- [x] ✅ P1 `json_agg(t ORDER BY ...)` (`X11`, `W6_02`) — khớp PG (`X11` chỉ khác giá trị `now()` của cột `created`)
- [x] ✅ P1 `json_object_agg`, `jsonb_object_agg` (`A05`) — khớp PG
- [x] ✅ `bool_and`, `bool_or`, `every` (`A06`)
- [x] ✅ `agg(...) FILTER (WHERE ...)` (`A08`)
- [x] ✅ P2 `bit_and`, `bit_or` (`A07`) — khớp PG
- [x] ✅ P2 `stddev`, `variance`, `var_*`, `stddev_*` (`A09`, `A09b`) — khớp PG
- [x] ✅ P2 `percentile_cont`, `percentile_disc`, `mode() WITHIN GROUP` (`A10`, `A11`, `A10b`) — khớp PG
- [x] ✅ P3 `corr`, `covar_*`, `regr_*` (`A12`) — khớp PG
- [x] ✅ P2 `any_value` (`A13`) — khớp PG
- [x] ✅ P3 Hypothetical-set `rank(x) WITHIN GROUP` (`A14`) — khớp PG
- [x] ✅ P3 `xmlagg` (`A15`) — khớp PG

## 8. Window function

- [x] ✅ `row_number`, `rank`, `dense_rank`, `ntile`, `percent_rank`, `cume_dist` (`WF01`, `WF02`, `WF10`)
- [x] ✅ Aggregate dạng window `sum/avg/count(...) OVER (...)`, có `PARTITION BY` (`WF03`, `WF04`)
- [x] ✅ `lag`, `lead`, `first_value`, `last_value`, `nth_value` (`WF05`, `WF06`)
- [x] ✅ Frame `ROWS BETWEEN`, `WINDOW w AS (...)`, `FILTER ... OVER` (`WF06`, `WF07`, `WF11`)
- [x] ✅ Lọc kết quả window qua subquery (`WF12`)
- [x] ✅ P2 Frame `RANGE BETWEEN n PRECEDING ...` (`WF08`) — khớp PG
- [x] ✅ P2 Frame `GROUPS`, `EXCLUDE ...` (`WF09`) — khớp PG

## 9. INSERT

- [x] ✅ `VALUES` một/nhiều dòng, không liệt kê cột, `DEFAULT VALUES`, `DEFAULT` (`I01`–`I05`)
- [x] ✅ `INSERT ... SELECT`, `WITH ... INSERT` (`I06`, `I16`)
- [x] ✅ `RETURNING *`, `RETURNING expr AS alias` (`I07`, `I08`)
- [x] ✅ `ON CONFLICT (col) DO NOTHING / DO UPDATE SET ... = EXCLUDED... [WHERE]`, `ON CONSTRAINT`, không target, alias bảng (`I09`–`I14`)
- [x] ✅ `OVERRIDING SYSTEM VALUE`, tham số `$2::jsonb` (`I15`, `I17`)
- [x] ✅ P2 `ON CONFLICT (expr) WHERE ...` (arbiter partial index), `OVERRIDING USER VALUE` — đợt 10 (`W10_15`, `W10_16`); arbiter là biểu thức cần UNIQUE index biểu thức (đợt 11)

## 10. UPDATE

- [x] ✅ `SET a = expr WHERE`, `RETURNING`, alias, `jsonb_set` (`U01`, `U02`, `U07`, `U09`)
- [x] ✅ `UPDATE ... FROM`, `WITH ... UPDATE ... FROM` (`U05`, `U08`)
- [x] ✅ `SET col = DEFAULT` (`U06`)
- [x] ✅ P1 `SET (a, b) = (v1, v2)` (`U03`) — khớp PG
- [x] ✅ P2 `SET (a, b) = (SELECT ...)` (`U04`) — khớp PG
- [x] ✅ P2 `SET arr[1] = ...` (`U10`) — khớp PG
- [x] ✅ P3 `WHERE CURRENT OF cursor` — đợt 13: dòng hiện tại của cursor trên một bảng, khớp theo khoá chính (`W13_05`)

## 11. DELETE / TRUNCATE / MERGE / COPY

- [x] ✅ `DELETE ... WHERE`, `RETURNING`, alias, `USING` (`D01`–`D05`)
- [x] ✅ `TRUNCATE`, `RESTART IDENTITY CASCADE` (`D06`, `D07`)
- [x] ✅ P2 `MERGE` (`M01`–`M03`) — đợt 10 (`engine/merge.rs`): `WHEN [NOT] MATCHED [BY SOURCE|TARGET] [AND ...]`, UPDATE/DELETE/INSERT/DO NOTHING, `RETURNING merge_action()`, lỗi 21000 khi một dòng bị sửa hai lần, khoá ngoại (`K_M01`–`K_M04`, `W10_17`–`W10_19`)
- [x] ✅ P1 `COPY ... FROM STDIN` (`C02`; harness chưa so được với PG) — đợt 13 (`engine/copy.rs`): text / CSV (`DELIMITER`, `NULL`, `HEADER`, `QUOTE`, `ESCAPE`, `FORCE_QUOTE`, cú pháp cũ `WITH CSV HEADER`), `COPY bảng (cột)`, `COPY (truy vấn) TO`, file `'đường dẫn'`; dữ liệu qua API mới `copy_from` / `copy_to` (Rust), `copyFrom` / `copyTo` (Node, wasm); ghi bằng INSERT lô 1000 dòng (ràng buộc, trigger, WAL như INSERT) (test `session_test.rs`)
- [x] ✅ P2 `COPY ... TO STDOUT` (`C01`) — khớp PG

## 12. DDL

### 12.1 CREATE / DROP TABLE
- [x] ✅ `CREATE TABLE [IF NOT EXISTS]`, `schema.table` (`DT01`, `DT02`, `DT18`)
- [x] ✅ `PRIMARY KEY`, `NOT NULL`, `UNIQUE`, `DEFAULT`, `CHECK` cột và bảng, có cưỡng chế (`DT03`, `DT03b`–`d`, `DT04`)
- [x] ✅ `FOREIGN KEY`, `ON DELETE CASCADE`, `ON DELETE SET NULL` (`DT05`, `X17`, `DT05b`)
- [x] ✅ P2 `ON DELETE SET DEFAULT/RESTRICT`, `ON UPDATE ...` — đợt 11 (`W11_15`–`W11_17`; RESTRICT báo `23001` như PG)
- [x] ✅ Default bằng hàm (`gen_random_uuid()`, `now()`, `'{}'::jsonb`), `COLLATE` (`DT20`, `DT15`)
- [x] ✅ P1 `DEFERRABLE [INITIALLY DEFERRED]` khoá ngoại: kiểm tra lúc COMMIT (vi phạm → ROLLBACK), `SET CONSTRAINTS {ALL | tên} {DEFERRED | IMMEDIATE}` (`DT06`, `K_D09`, `W11_12`–`W11_14`) — đợt 11, khớp PG
- [x] ✅ **P0** `GENERATED ALWAYS/BY DEFAULT AS IDENTITY (...)` (`DT07`) — khớp PG
- [x] ✅ P1 `GENERATED ALWAYS AS IDENTITY` không chặn ghi đè (`DT07b`) — khớp PG
- [x] ✅ P1 `GENERATED ALWAYS AS (expr) STORED`: tính khi INSERT / UPDATE / ON CONFLICT / MERGE, `ADD COLUMN ... GENERATED`, `DROP EXPRESSION`, ghi thẳng → `428C9` (`DT08`, `K_Q11`, `K_Q12`, `W11_01`–`W11_04`, `W11_20`) — đợt 11, khớp PG; giá trị lưu trong dòng nên bản cũ vẫn đọc được
- [x] ✅ P1 `CREATE TEMP TABLE` (`DT09`) — khớp PG
- [x] ✅ P3 `CREATE UNLOGGED TABLE` (`DT10`, `K_D02`) — đợt 11 (bảng vẫn bền như bảng thường), khớp PG
- [x] ✅ P1 `CREATE [UNLOGGED] TABLE [IF NOT EXISTS] t [(cột)] AS <truy vấn> [WITH [NO] DATA]`: kiểu cột theo kiểu khai báo của nguồn, dữ liệu chép bằng INSERT thường trong một giao dịch, trùng tên cột → `42701` (`DT11`, `W4_05`–`W4_08`) — khớp PG
- [x] ✅ P2 `(LIKE t [INCLUDING|EXCLUDING DEFAULTS/CONSTRAINTS/INDEXES/IDENTITY/GENERATED/ALL])` (`DT12`, `K_D04`, `W11_05`, `W11_06`) — đợt 11, khớp PG
- [x] ✅ P3 `INHERITS (cha)`: bảng con có cột của cha (`DT13`, `W11_22`) — đợt 11; khác PG: truy vấn bảng cha không gồm dòng của bảng con
- [ ] ❌ P2 `PARTITION BY`, `PARTITION OF` (`DT14`)
- [x] ✅ P3 `EXCLUDE ... (cột WITH =, ...)` — đợt 11: cưỡng chế như UNIQUE, lỗi `23P01` (`W11_21`); toán tử khác `=` (vd. `&&` trên range) báo `0A000` thay vì bỏ qua. `DT16`/`K_D07` vẫn khác vì PG thiếu `btree_gist` cho kiểu int
- [x] ✅ P2 `UNIQUE NULLS NOT DISTINCT` cấp cột và cấp bảng (`DT17`, `K_D08`, `W11_07`) — đợt 11, khớp PG
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
- [x] ✅ P2 `ALTER COLUMN ... ADD GENERATED AS IDENTITY` (báo `55000` khi cột đã có default), `SET GENERATED`, `DROP IDENTITY` (`AT21`, `W11_18`, `W11_19`) — đợt 11, khớp PG
- [ ] ⬜ P3 `ATTACH/DETACH PARTITION`, `SET (storage params)` — `SET (...)` / `RESET (...)` của bảng, cột, index là no-op hợp lệ (đợt 11); ATTACH/DETACH cùng PARTITION ở đợt 15

### 12.3 INDEX
- [x] ✅ `CREATE [UNIQUE] INDEX [IF NOT EXISTS]`, unique có cưỡng chế, partial unique (`IX01`–`IX03`, `IX02`, `IX02b`)
- [x] ✅ `DESC`, `INCLUDE`, `USING gin/btree`, opclass, `CONCURRENTLY`, partial `WHERE` (`IX05`–`IX08`, `IX13`)
- [x] ✅ P1 `UNIQUE` index trên biểu thức `(lower(name))`: cưỡng chế khi INSERT / UPDATE, `ON CONFLICT (lower(name))` (`IX04`, `K_D11`, `W11_09`–`W11_11`) — đợt 11, khớp PG
- [x] ✅ `DROP INDEX [IF EXISTS]` (`IX09`, `IX10`)
- [x] ✅ P2 `ALTER INDEX ... RENAME TO` (cả index của ràng buộc), `ALTER INDEX ... SET (...)` (`IX11`) — đợt 11, khớp PG
- [x] ✅ P3 `REINDEX {TABLE|INDEX|SCHEMA|DATABASE|SYSTEM}` (`IX12`) — đợt 11, khớp PG

### 12.4 SEQUENCE
- [x] ✅ **P0** `nextval`, `currval`, `setval`, `pg_get_serial_sequence` (`SQ01`, `SQ03`, `SQ06`, `W8_01`–`W8_04`) — khớp PG: sequence gắn với cột (SERIAL, IDENTITY, `DEFAULT nextval('t_id_seq'::regclass)`; bộ đếm dựng lại từ dữ liệu, `setval` không hạ dưới max + 1) và sequence độc lập (`CREATE SEQUENCE`, giá trị lưu bền, ROLLBACK không trả lại số như PG); `currval`/`lastval` theo từng phiên (`Executor`), kể cả sau `INSERT` dùng SERIAL
- [x] ✅ P1 `lastval()` báo `55000` khi chưa có giá trị (`SQ07`, `W8_11`) — khớp PG (`SQ07` chỉ lệch vì golden chạy dữ liệu mẫu trong cùng phiên, các INSERT SERIAL đã đặt `lastval`)
- [x] ✅ P1 `CREATE/ALTER/DROP SEQUENCE` (`SQ01`–`SQ05`, `W8_01`–`W8_10`): `AS`, `INCREMENT`, `MINVALUE/MAXVALUE`, `START`, `CYCLE`, `RESTART`, `RENAME TO`, kiểm tra miền (`22023`, `2200H`, `22003`), `DEFAULT nextval('s')`, `DROP` chặn khi còn cột dùng (`CASCADE` bỏ DEFAULT), `SELECT * FROM s`, `pg_sequences`, `information_schema.sequences` — khớp PG (`log_cnt` luôn 0)

### 12.5 VIEW
- [x] ✅ P1 `CREATE [OR REPLACE] [TEMP] VIEW [(cột)]`, `DROP VIEW [CASCADE]`, `ALTER VIEW ... RENAME` (`V01`–`V03`, `W8_12`–`W8_18`) — khớp PG: view được mở rộng thành CTE nên dùng được trong JOIN / self-join / subquery / `INSERT ... SELECT`; view lồng nhau; `DROP TABLE` chặn khi có view phụ thuộc (`CASCADE` xoá view); view có trong `information_schema.tables` (`VIEW`), `.views`, `pg_views`
- [x] ✅ P2 `MATERIALIZED VIEW`, `REFRESH [CONCURRENTLY]`, `WITH [NO] DATA`, `pg_matviews` (`V04`, `W8_22`) — khớp PG (dữ liệu là một bảng thường)
- [x] ✅ P3 View cập nhật được, `WITH CHECK OPTION`, `RECURSIVE VIEW` (`V05`–`V07`, `W8_19`–`W8_21`, `W8_35`) — khớp PG: `INSERT/UPDATE/DELETE` qua view một bảng không gộp (UPDATE/DELETE chỉ đụng dòng thấy qua view), view khác → `55000`; CHECK OPTION mới kiểm tra `INSERT ... VALUES`

### 12.6 SCHEMA
- [x] ✅ `SET search_path TO ...` (`SC04`)
- [x] ✅ P1 `CREATE SCHEMA [IF NOT EXISTS] [AUTHORIZATION]`, `DROP SCHEMA`, `ALTER SCHEMA ... RENAME`, `information_schema.schemata` (`SC01`–`SC03`, `CAT06`, `W8_23`–`W8_25`) — khớp PG; `DROP SCHEMA public CASCADE` xoá sạch bảng / view / sequence / kiểu như PG (rồi `CREATE SCHEMA public`). Giới hạn: bảng `app.x` vẫn dùng chung không gian tên với `public` (`K_C02`, `K_C04`), `DROP SCHEMA app CASCADE` không xoá bảng
- [ ] 🟡 P2 Phân giải tên theo `search_path` (`SC05`, `K_C03`) — `SC05` qua vì tên bảng bỏ qua schema; chưa có không gian tên riêng theo schema

### 12.7 TYPE / DOMAIN
- [x] ✅ P1 `CREATE TYPE ... AS ENUM`, `ALTER TYPE ... ADD VALUE [IF NOT EXISTS] [BEFORE|AFTER]`, `RENAME VALUE` (đổi cả dữ liệu), `DROP TYPE [CASCADE]` (`TY01`, `TY02`, `W8_31`, `W8_32`) — khớp PG
- [x] ✅ P2 `CREATE DOMAIN`, `DROP DOMAIN` (`TY03`) — khớp PG (xem mục 2)
- [ ] 🟡 P2 `CREATE TYPE ... AS RANGE`, kiểu composite `CREATE TYPE ... AS (...)` (`TY04`, `T22`) — báo `0A000`; đợt 15: composite xong (`T22`), `AS RANGE` (kiểu range tự định nghĩa) vẫn `0A000`

### 12.8 Function / Procedure / Trigger
- [x] ✅ `DO $$ ... $$` (`FN05`)
- [x] ✅ P2 `CREATE [OR REPLACE] FUNCTION ... LANGUAGE sql/plpgsql`: `RETURNS scalar | SETOF | TABLE (...) | void | trigger`, tham số OUT / INOUT / VARIADIC / DEFAULT, gọi theo tên `f(a => 1)`, overload, STRICT, `RETURN biểu_thức` (SQL chuẩn), đệ quy; hàm SQL một biểu thức được inline (dùng được cả trong WHERE với cột), đối số hằng tính trước, cột trong danh sách SELECT tính theo từng dòng, hàm trả tập trong FROM / LATERAL; `DEFAULT f()` của cột (`FN01`, `FN02`, `FN06`, `K_P01`–`K_P17`, `W12_*`) — đợt 12, khớp PG. Chưa hỗ trợ: hàm không inline được có đối số là cột trong WHERE / ORDER BY (báo lỗi rõ ràng)
- [x] ✅ P2 `CREATE [OR REPLACE] TRIGGER {BEFORE|AFTER} {INSERT|UPDATE [OF ...]|DELETE} [OR ...] ON t FOR EACH {ROW|STATEMENT} [WHEN (...)] EXECUTE FUNCTION f(args)`, `NEW`/`OLD`/`TG_OP`/`TG_WHEN`/`TG_LEVEL`/`TG_ARGV`, `RETURN NULL` bỏ dòng, `DROP TRIGGER`, `ALTER TABLE ... ENABLE/DISABLE TRIGGER` (`FN03`, `FN08`, `W12_11`–`W12_16`) — đợt 12, khớp PG. Chưa: INSTEAD OF (view), trigger cho phần UPDATE của `ON CONFLICT`, TRUNCATE trigger
- [x] ✅ P2 `CREATE PROCEDURE`, `CALL` (INOUT trả một dòng), gọi procedure trong SELECT báo `42809` (`FN04`, `W12_09`, `W12_10`) — đợt 12, khớp PG
- [x] ✅ P2 `DROP FUNCTION / PROCEDURE / ROUTINE [IF EXISTS] tên[(kiểu)]` (`FN07`, `K_P11`, `W12_19`) — đợt 12, khớp PG
- [ ] ❌ P3 `CREATE RULE/AGGREGATE/OPERATOR/CAST` (`FN09`–`FN12`)

## 13. Transaction

- [x] ✅ `BEGIN/COMMIT/ROLLBACK`, `START TRANSACTION`, `END`, `ABORT` (`TX01`–`TX03`, `TX12`)
- [x] ✅ `SAVEPOINT`, `ROLLBACK TO`, `RELEASE` (`TX04`, `TX05`)
- [x] ✅ **P0** DDL theo giao dịch: `ROLLBACK` / `ROLLBACK TO SAVEPOINT` hoàn tác `CREATE` / `ALTER` / `DROP TABLE` cả trong bộ nhớ lẫn khi mở lại DB (trước đây `DROP TABLE` đã ROLLBACK vẫn làm mất bảng) (test `transaction_test`)
- [x] ✅ `BEGIN ISOLATION LEVEL ...`, `BEGIN READ ONLY`, `SET TRANSACTION ...`, `SET CONSTRAINTS ...` (chấp nhận) (`TX05`–`TX08`)
- [x] ✅ `pg_advisory_xact_lock` (`TX10`)
- [x] ✅ P2 `READ ONLY` có chặn ghi không — đợt 13: `BEGIN / START TRANSACTION / SET TRANSACTION READ ONLY` chặn INSERT/UPDATE/DELETE/MERGE/TRUNCATE/DDL/COPY FROM với `25006` như PG (`K_U08`, `W13_10`–`W13_13`)
- [x] ✅ P2 `LOCK TABLE ... IN ... MODE` (`TX09`) — khớp PG
- [x] ✅ P3 `PREPARE TRANSACTION` (`TX11`) — khớp PG

## 14. Cấu hình phiên

- [x] ✅ `SET x = v`, `SET x TO v`, `SET LOCAL`, `SET TIME ZONE` (có hiệu lực thật, xem 18.3), `SET NAMES`, `RESET` (`CF01`, `CF02`, `CF05`, `CF07`, `CF08`, `CF11`, `CF12`, `W5_01`–`W5_05`)
- [x] ✅ `SHOW name` (`CF03`) — đợt 13: tên cột theo PG trên wire (`TimeZone`, `DateStyle`; JSON giữ khoá cũ), mọi tham số của PG có giá trị mặc định (`work_mem`...), `SET LOCAL` trả lại giá trị khi giao dịch kết thúc (`K_U12`, `W13_16`–`W13_19`)
- [x] ✅ P2 `SHOW ALL` (`CF04`) — khớp PG
- [x] ✅ **P0** `set_config(...)`, `current_setting(...)` trả nguyên văn (`CF06`) — khớp PG
- [x] ✅ **P0** `version()`, `current_database()`, `current_schema()`, `current_user`, `session_user`, `current_schemas()` (`CF09`, `CF10`, `X04`, `W6_12`, `W6_13`) — khớp PG; `version()` trả `PostgreSQL 16.0 (pglite-rs)`, `DEFAULT current_user` cũng đúng

## 15. Lệnh tiện ích

- [x] ✅ `CREATE EXTENSION [IF NOT EXISTS] pgcrypto / "uuid-ossp"`, `uuid_generate_v4()` (`UT18`, `UT19`)
- [x] ✅ `LISTEN`, `NOTIFY`, `UNLISTEN`, `DISCARD ALL`, `COMMENT ON` (chấp nhận) (`UT12`, `UT13`, `UT15`–`UT17`) — đợt 13: hàng đợi thông báo thật theo phiên (gửi khi COMMIT, gộp trùng, bỏ khi ROLLBACK), lấy bằng API `take_notifications` / `notifications()`
- [x] ✅ P2 `pg_notify()` (`UT14`) — khớp PG
- [x] ✅ P1 `EXPLAIN [ANALYZE] [(FORMAT JSON)]` (`UT01`–`UT03`) — đợt 13 (`engine/explain.rs`): cây kế hoạch dạng PG từ kế hoạch thật của engine (Seq Scan / Index Scan + `Index Cond` / Hash Join / HashAggregate / Sort / Limit / Insert / Update / Delete), `COSTS OFF`, `FORMAT JSON`, `ANALYZE` chạy thật và đo thời gian, `BUFFERS`; chi phí là ước lượng đơn giản (golden chỉ so hình dạng, `W13_25`–`W13_28`)
- [x] ✅ P2 `ANALYZE`, `VACUUM`, `CHECKPOINT` (nên no-op) (`UT04`–`UT07`) — khớp PG
- [x] ✅ P1 `PREPARE / EXECUTE / DEALLOCATE` (`UT08`, `UT09`, `W7_11`, `W7_12`) — khớp PG; chỉ sống trong phiên, không ghi xuống đĩa
- [x] ✅ P2 Cursor `DECLARE / FETCH / CLOSE` (`UT10`, `UT11`) — đợt 13 (`engine/session.rs`): `DECLARE [SCROLL] CURSOR [WITH HOLD] FOR`, `FETCH / MOVE` mọi hướng (`NEXT/PRIOR/FIRST/LAST/ABSOLUTE/RELATIVE/n/ALL/FORWARD/BACKWARD`), `CLOSE [ALL]`, lỗi `25P01` / `34000` / `42P03` như PG (`W13_01`–`W13_09`)
- [x] ✅ P2 `obj_description`, `col_description` (`CAT20`) — khớp PG
- [ ] ❌ P3 `CLUSTER`, `SECURITY LABEL`, `IMPORT FOREIGN SCHEMA`, FDW, publication, statistics (`UT20`–`UT22`)

## 16. Bảo mật / CSDL

- [x] ✅ `SET ROLE` (chấp nhận) (`SEC07`)
- [x] ✅ P2 `GRANT`, `REVOKE`, `ALTER DEFAULT PRIVILEGES` (migration có sinh, nên no-op) (`SEC03`, `SEC04`, `SEC08`) — khớp PG
- [ ] ❌ P3 `CREATE/ALTER/DROP ROLE|USER`, `CREATE POLICY` (`SEC01`, `SEC02`, `SEC05`, `SEC06`, `SEC09`)
- [ ] ❌ P3 `CREATE/DROP DATABASE`, `CREATE TABLESPACE` (`DB01`–`DB03`)

## 17. System catalog

- [x] ✅ `information_schema.columns` (`CAT02`)
- [x] ✅ **P1** `SELECT table_name FROM information_schema.tables` trả **mọi cột** thay vì cột được chọn (`CAT01`) — khớp PG
- [x] ✅ P1 `pg_tables` cũng trả mọi cột và sai tên cột (`CAT13`) — khớp PG
- [ ] 🟡 P1 `information_schema.table_constraints`, `.key_column_usage`: catalog tên `litepostgres` (PG: tên DB), thiếu NOT NULL constraint (`CAT03`, `CAT04`); `.schemata` đã khớp PG (`CAT06`) — đợt 14: catalog = `postgres` (như `current_database()`), có cột `nulls_distinct`, `CAT04` khớp PG; **cố ý chưa** thêm dòng NOT NULL (`t_id_not_null` kiểu CHECK) vào `table_constraints` vì công cụ quản trị của NATA (db-rs `listConstraints`) đang hiển thị nguyên danh sách này — `pg_constraint` thì đã có (contype `n`)
- [x] ✅ P1 `pg_class`, `pg_namespace`, `pg_constraint`, `pg_index` sai cột / thiếu dòng hệ thống (`CAT07`, `CAT10`, `CAT12`, `CAT17`) — đợt 14 (`engine/catalog_views.rs`): đủ cột như PG 18, dòng cho bảng / index / sequence / view / matview / ràng buộc (p, u, f, c, n) của người dùng với OID nhất quán giữa các bảng, `CAT10` khớp PG; không tái tạo các bảng / index / ràng buộc nội bộ của PG (`pg_toast_*`, `pg_proc_oid_index`...) nên `CAT07`/`CAT12`/`CAT17` vẫn khác (`W14_01`–`W14_05`, `W14_17`, `W14_24`)
- [x] ✅ P1 `information_schema.referential_constraints`, `.views` (`CAT05`, `CAT21`) — `.views` / `pg_views` / `pg_matviews` / `.sequences` / `pg_sequences` / `pg_enum` đã có dòng của đối tượng người dùng (chưa có view hệ thống nên `CAT21` khác PG); đợt 14: `referential_constraints` (`CAT05`, `W14_10`), thêm `.check_constraints`, `.triggers`, `.routines`, `.parameters`
- [x] ✅ **P1** `pg_attribute`, `pg_type`, `pg_indexes`, `pg_proc`, `pg_enum`, `pg_settings`, `pg_database`, `pg_extension`, `pg_stat_activity` (`CAT08`, `CAT09`, `CAT11`, `CAT14`–`CAT16`, `CAT19`, `CAT22`, `CAT23`) — đợt 14: khớp PG (kể cả `pg_attrdef`, `pg_trigger`, `pg_description`, `pg_am`, `pg_sequence`, `pg_roles`, `pg_user`); dòng dựng sẵn của `pg_type` (329 kiểu), `pg_settings`, `pg_proc` (649 hàm mà engine có) sinh từ PG thật bằng `scripts/pg_golden/gen_catalog.mjs`; `pg_settings` / `SHOW ALL` in giá trị engine đang dùng (`server_version` 16.0) nên `CAT14` / `CF04` khác oracle (PG 18.3) có chủ đích
- [x] ✅ P1 `format_type`, `pg_get_indexdef`, `pg_get_constraintdef`, `to_regclass` (`CAT18`, `CAT17`, `SYS05`, `CAT24`) — đợt 14: khớp PG cho đối tượng người dùng, cùng `pg_get_expr`, `pg_get_viewdef`, `pg_get_triggerdef`, `pg_get_functiondef`, `pg_get_function_arguments/result`, `pg_table_is_visible`, `pg_get_userbyid`, `'x'::regclass / ::regtype / ::regnamespace` (tên khi in, OID khi so sánh), `cột::regclass` (`W14_06`, `W14_07`, `W14_19`, `W14_23`, `W14_28`, `W14_29`)

## 18. Hàm dựng sẵn

### 18.1 Toán học
- [x] ✅ `abs`, `ceil`, `floor`, `round`, `trunc`, `mod`, `sign`, `sqrt`, `cbrt`, `power`, `exp`, `ln`, `log`, `pi`, `degrees`, `radians`, `random()` (`F01`, `F02`, `F01b`)
- [x] ✅ P2 Lượng giác `sin/cos/tan/asin/acos/atan/atan2` (`F04`) — khớp PG
- [x] ✅ P2 `div`, `gcd`, `lcm`, `factorial`, `width_bucket` (`F05`) — khớp PG
- [x] ✅ P3 `scale`, `trim_scale`, `min_scale`, `setseed`, `random(min, max)` (`F06`, `X12`, `X20`) — đợt 9 (`random(a, b)` chỉ so được hình dạng); `scale()` của hằng giữ số 0 cuối

### 18.2 Chuỗi
- [x] ✅ `length`, `char_length`, `octet_length`, `lower`, `upper`, `initcap` (`STR01`, `STR02`)
- [x] ✅ `substring(s FROM i FOR n)`, `substr`, `position(x IN s)`, `strpos`, `left`, `right` (`STR03b`, `STR05b`, `STR12`)
- [x] ✅ `trim/ltrim/rtrim/btrim`, `lpad`, `rpad`, `repeat`, `reverse`, `replace`, `translate`, `concat`, `concat_ws`, `split_part`, `array_to_string` (`STR06`–`STR10`)
- [x] ✅ P1 `string_to_array` trả JSON thay vì mảng PG (`STR09`) — khớp PG
- [x] ✅ P1 `substring(s FROM 'regex')` (`STR04`) — khớp PG
- [x] ✅ P1 `trim(BOTH|LEADING|TRAILING x FROM s)` (`STR06`) — khớp PG
- [x] ✅ P1 `format('%s %I %L', ...)` (`STR11`) — khớp PG
- [x] ✅ P1 `regexp_replace`, `regexp_matches`, `regexp_split_to_array` (`STR13`) — đã chạy ở mọi đường (SELECT, WHERE, JOIN, hàm gộp), cờ `g`/`i`/`n`, `\1`; đợt 9: `regexp_matches` ra `{b}`, `regexp_matches(..., 'g')` và `regexp_split_to_table` là hàm trả tập (trong SELECT không FROM và trong FROM) — khớp PG
- [x] ✅ P1 `regexp_split_to_table` (`STR15`) — khớp PG
- [x] ✅ P2 `regexp_like/count/substr/instr` (`STR14`) — `regexp_like`, `regexp_count`, `regexp_substr` khớp PG; `regexp_instr` chưa có
- [x] ✅ `md5`, `ascii`, `chr` (`STR16`, `STR17`)
- [x] ✅ P1 `sha256`, `encode`, `decode` (định dạng bytea) (`STR16`) — khớp PG
- [x] ✅ P2 `quote_ident`, `quote_literal`, `quote_nullable` (`STR17`) — khớp PG
- [x] ✅ P2 `starts_with`, `^@` (`STR18`) — khớp PG
- [x] ✅ P1 `to_hex`, `to_char(number, fmt)`, `to_number` (`STR19`) — khớp PG
- [x] ✅ P3 `overlay`, `bit_length`, `normalize`, `casefold` (`STR08`, `STR01`, `STR22`) — khớp PG

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
- [x] ✅ P1 `@>`, `<@`, `?`, `?|`, `?&` với cột `NULL` trả `false` thay vì `NULL` (`JS02`) — khớp PG
- [x] ✅ **P0** `jsonb || jsonb` nối như chuỗi; `jsonb - key`, `#- path` ra `NULL` (`JS03`, `JS03b`) — khớp PG
- [x] ✅ `jsonb_build_object`, `json_build_object`, `jsonb_build_array`, `json_build_array` (`JS04`)
- [x] ✅ P0 `to_json(row)`, `to_jsonb(row)`, `row_to_json(row)` và `to_json(giá trị)` (`JS05`, `JS05b`, `W6_04`–`W6_06`) — khớp PG
- [x] ✅ `jsonb_set`, `jsonb_array_length`, `jsonb_typeof`, `json_typeof`, `jsonb_strip_nulls`, `jsonb_pretty` (`JS06`, `JS11`, `JS12`)
- [x] ✅ P1 `jsonb_insert`, `jsonb_set_lax` (`JS06`) — khớp PG
- [x] ✅ P1 `jsonb_object_keys`, `jsonb_array_elements_text` trong SELECT (`JS08`, `JS10`) — khớp PG
- [x] ✅ P1 `jsonb_each`, `jsonb_array_elements` trong FROM (`JS07`, `JS09`) — xong với đối số hằng/tham số, `WITH ORDINALITY`, alias cột (`JS09`, `W7_08`–`W7_10`, `W7_17`); còn: hai hàm trùng tên cột trong một FROM (`JS07`), đối số tham chiếu cột (LATERAL) — khớp PG
- [x] ✅ P2 `jsonb_to_record(set)`, `jsonb_populate_record` (`JS13`–`JS15`) — khớp PG
- [x] ✅ P2 jsonpath `jsonb_path_query`, `jsonb_path_exists`, `@?`, `@@` (`JS16`) — khớp PG
- [x] ✅ P2 Subscript `jsonb['a']` (`JS17b`) — khớp PG
- [x] ✅ P3 `json_object(...)`, SQL/JSON `IS JSON`, `JSON_QUERY/VALUE/EXISTS`, `JSON_ARRAYAGG/OBJECTAGG` (`JS18`, `JS19`, `JS21`, `JS22`) — khớp PG
- [x] ✅ P3 `JSON_TABLE` (`JS20`) — khớp PG

### 18.5 Mảng
- [x] ✅ `@>`, `<@`, `&&`, `=`, `<` (`AR02`, `AR11`)
- [x] ✅ P1 `||` giữa mảng/phần tử: `0 || ARRAY[1]`, `ARRAY[1] || ARRAY[2]` sai (`AR01`, `AR01b`) — khớp PG
- [x] ✅ P1 `array_append/prepend/cat/remove/replace` đúng giá trị nhưng output dạng JSON (`AR04`) — khớp PG
- [x] ✅ `array_length`, `array_dims`, `array_upper`, `array_lower` (`AR03`)
- [x] ✅ P1 `cardinality`, `array_ndims`, `array_position(s)` (`AR03`, `AR05`) — khớp PG
- [x] ✅ **P0** `unnest(array)` trong SELECT khi là mục duy nhất, trong FROM (`AR06`, `STR20`) — khớp PG; SRF đứng cạnh cột khác trong SELECT để đợt 4
- [x] ✅ P1 `unnest(a, b)` trong FROM, mảng dài ngắn khác nhau (`AR07`, `K_R05`) — khớp PG. Sửa luôn lỗi `unnest(...) AS t` đè rồi xoá mất bảng thật tên `t` khỏi bộ nhớ
- [x] ✅ P1 `WITH ORDINALITY` (`AR08`) — khớp PG
- [x] ✅ P3 `array_fill`, `trim_array`, `array_reverse`, `array_sort` (`AR09`) — khớp PG

### 18.6 Hàm trả tập (SRF)
- [x] ✅ **P1** `generate_series(int | numeric | timestamp | date, ...[, step])` trong FROM, có alias / danh sách cột, bước âm, `WITH ORDINALITY` (`SRF01`, `SRF03`, `W5_18`–`W5_22`) — khớp PG; JOIN / LATERAL với SRF để đợt 4
- [x] ✅ P1 `generate_series(...)` trong SELECT (`SRF02`) — khớp PG
- [x] ✅ P2 `generate_subscripts` (`SRF04`) — khớp PG
- [x] ✅ P3 `ROWS FROM (...)` (`SRF05`) — khớp PG

### 18.7 Hệ thống
- [x] ✅ `gen_random_uuid()` (`T17`)
- [x] ✅ P2 `uuidv4()`, `uuidv7()`, `pg_backend_pid`, `txid_current`, `pg_size_pretty`, `pg_relation_size`... (chỉ so được số dòng) (`SYS01`, `SYS03`, `SYS06`, `SYS07`) — đợt 9: có đủ hàm (kích thước quan hệ trả 8192 như bảng nhỏ của PG; engine không chia trang)
- [x] ✅ P2 `pg_sleep`, `has_table_privilege`, `pg_has_role` (`SYS02`, `SYS04`) — đợt 9; khoá advisory luôn lấy được (một tiến trình)
- [x] ✅ P2 pgcrypto `crypt`, `gen_salt`, `digest`, `hmac` (`SYS08`) — đợt 9: bcrypt (`$2a$`), md5-crypt (`$1$`), `digest`/`hmac` md5/sha1/sha2 (`W9_25` khớp PG khi có `CREATE EXTENSION pgcrypto`; engine không bắt buộc CREATE EXTENSION nên `SYS08`/`K_F44`/`K_F45` — PG báo 42883 vì chưa cài extension — vẫn nằm trong known_gaps). `unaccent` cũng có sẵn (`W9_26`)

### 18.8 Full-text search / Range / Geometry / XML
- [x] ✅ P2 `to_tsvector`, `to_tsquery`, `@@`, `ts_rank`, `ts_headline`... (`FTS01`–`FTS03`) — đợt 15: cấu hình `english` (stemmer Snowball English tự cài + stopword của PG) và `simple`; `plainto/phraseto/websearch_to_tsquery`, toán tử `& | ! <-> <N>`, tiền tố `:*`, trọng số, `ts_rank` theo đúng công thức `tsrank.c` (khớp tới chữ số float4), `ts_rank_cd`, `setweight`, `strip`, `numnode`, `tsvector_to_array`; GIN index nhận như index thường (`K_X01`–`K_X03`, `W15_17`–`W15_28`)
- [x] ✅ P2 Toán tử/hàm range `&&`, `lower`, `upper`, `isempty` (`GEO01b`, `RNG01`) — đợt 15, cùng `lower_inc/upper_inc/lower_inf/upper_inf`, `range_merge` (`K_Y10`, `K_Y11`, `K_X05`)
- [ ] 🟡 P3 Toán tử hình học `<->`, `@>` (`GEO01`) — đợt 15: `point(x, y)`, `<->` giữa hai điểm (và vector `[..]` như pgvector) (`K_Z04`, `W15_34`); `circle`/`lseg`/`polygon`... báo `0A000` thay vì trả nguyên văn
- [ ] 🟡 P3 `xmlelement`, `xpath` (`XML01`) — đợt 15: `xmlelement(name x, ...)` (`K_Z05`, `W15_35`); `xpath` / `xmlparse` / `xmlforest`... báo `0A000` thay vì trả nguyên văn

## 19. Xử lý lỗi

- [x] ✅ Vi phạm PK/UNIQUE, FK (insert và delete cha), NOT NULL, CHECK (`ERR01`, `ERR02`, `DT03*`, `DT05c`)
- [x] ✅ Bảng không tồn tại, cột không tồn tại trong `INSERT (cols)` (`ERR04`, `ERR08`)
- [x] ✅ Sai kiểu khi ghi, thừa giá trị trong `VALUES` (`ERR07`, `ERR11`)
- [x] ✅ **P0** Cột không tồn tại trong SELECT/WHERE (`ERR03`, `ERR09`) — khớp PG
- [x] ✅ **P0** Chia cho 0, cast không hợp lệ, tràn số (`ERR05`, `ERR10`, `F09`) — chia 0 và cast đã đúng; tràn int4 khi tính toán (`F09`) cần kiểu khai báo (đợt 2b) — khớp PG
- [x] ✅ P1 Thông báo lỗi cú pháp theo kiểu PG (`syntax error at or near "SELEC"`) (`ERR06`) — khớp PG

---

## Lộ trình gợi ý (phần còn lại)

1. **Mục 0** (còn lại): lỗi `42883` khi gặp hàm không biết (sau đợt 5), scale của numeric khi tính toán. Đã xong: chia nguyên, chia 0, cast, tràn số, cột không tồn tại, định dạng wire của timestamp/numeric/mảng/ROW/bytea/jsonb.
2. **P0 còn lại**: IDENTITY, sequence functions, `version()`/`current_*()`/`set_config`, `jsonb ||`/`-`, `unnest`.
3. **P1** còn lại: catalog (`pg_attribute`, `pg_type`, `pg_index`, `pg_indexes`, `referential_constraints`), `EXPLAIN`, `COPY FROM STDIN`, `DEFERRABLE`, cột `GENERATED ... STORED`, unique index trên biểu thức, hàm chuỗi / JSON / mảng còn lệch (mục 18). Đã xong: `generate_series`, regex, `to_date/to_timestamp`, `AT TIME ZONE`, `json_object_agg`, view, schema, enum, sequence, `VALUES`, CTAS, `TEMP TABLE`, `PREPARE/EXECUTE`.
4. **P2–P3**: functions/triggers, MERGE, partitioning, grouping sets, FTS, range, SQL/JSON, quyền hạn.

Khi triển khai xong một mục: thêm case vào `crates/pglite-rs/tests/pg_golden/cases.tsv`, sinh kết quả PG bằng
`node scripts/pg_golden/generate.mjs <tiền tố id>`, chạy `cargo test --release --test pg_golden_test` (xoá case đã khớp
khỏi `known_gaps.txt`), rồi `python3 scripts/pg_golden/checklist_status.py --apply` và sửa tay các dòng không có probe.
