#!/usr/bin/env python3
"""Báo cáo trạng thái từng dòng của POSTGRES_SYNTAX_CHECKLIST.md theo kết quả golden test.

Case khớp PostgreSQL = có trong cases.tsv mà không nằm trong known_gaps.txt. Với mỗi dòng còn `[ ]`
có mã probe trong backtick: in ra dòng nào mọi mã đã khớp (có thể đánh ✅) và dòng khớp một phần.
  python3 scripts/pg_golden/checklist_status.py [--apply]
--apply: đánh `[x] ✅` cho các dòng mọi mã đã khớp (giữ nguyên nội dung, thêm "— khớp PG").
"""
import re, sys, pathlib
root = pathlib.Path(__file__).resolve().parents[2]
g = root / "crates/pglite-rs/tests/pg_golden"
ids = {l.split("\t")[0] for l in (g / "cases.tsv").read_text().splitlines() if l.strip() and not l.startswith("#")}
gaps = {l.split("#")[0].strip() for l in (g / "known_gaps.txt").read_text().splitlines() if l.strip()}
# Case chỉ so hình dạng (giá trị thay đổi theo lần chạy, xem is_volatile trong pg_golden_test.rs) hoặc bị
# bỏ qua (COPY FROM STDIN) không chứng minh được là đúng: không tính.
VOLATILE = ["now()", "current_timestamp", "current_time", "current_date", "localtime", "clock_timestamp", "statement_timestamp",
    "transaction_timestamp", "timeofday", "random", "uuid", "gen_salt", "crypt(", "pg_backend_pid", "txid_current", "xact_id",
    "pg_postmaster", "version()", "created", "t.*", "to_json(t)", "to_jsonb(t)", "row_to_json", "json_agg(t)", "jsonb_agg(t)",
    "pg_stat_activity", "tablesample", "size", "inet_server", "pg_sleep"]
sql = {l.split("\t")[0]: l.split("\t")[1].lower() for l in (g / "cases.tsv").read_text().splitlines() if l.strip() and not l.startswith("#")}
def provable(i):
    q = sql.get(i, "")
    return not any(k in q for k in VOLATILE) and "from stdin" not in q and not q.endswith("select * from t") and "select * from t " not in q
ok = {i for i in ids - gaps if provable(i)}
path = root / "POSTGRES_SYNTAX_CHECKLIST.md"
lines = path.read_text().splitlines()
done, partial = [], []
for i, line in enumerate(lines):
    if not line.lstrip().startswith("- [ ]"):
        continue
    refs = [r for r in re.findall(r"`([A-Z][A-Z_]*\d+[a-z]?)`", line) if r in ids]
    if not refs:
        continue
    good = [r for r in refs if r in ok]
    if len(good) == len(refs):
        done.append(i)
    elif good:
        partial.append((i, good, [r for r in refs if r not in ok]))
print(f"golden: {len(ok)}/{len(ids)} khớp PostgreSQL")
print(f"\n== Mọi probe đã khớp ({len(done)} dòng):")
for i in done:
    print(f"  L{i+1}: {lines[i].strip()[:150]}")
print(f"\n== Khớp một phần ({len(partial)} dòng):")
for i, good, bad in partial:
    print(f"  L{i+1}: khớp {good} còn {bad}: {lines[i].strip()[:110]}")
if "--apply" in sys.argv:
    for i in done:
        l = lines[i].replace("- [ ]", "- [x]", 1)
        l = re.sub(r"🟡|❌|⬜", "✅", l, count=1)
        if "khớp PG" not in l:
            l += " — khớp PG"
        lines[i] = l
    path.write_text("\n".join(lines) + "\n")
    print(f"\nđã đánh dấu {len(done)} dòng")
