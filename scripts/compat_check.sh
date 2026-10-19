#!/usr/bin/env bash
# So engine cũ (OLD_DIR, một worktree của commit cũ) và engine hiện tại trên mọi DB trong BACKUP_DIR.
# Mỗi DB được copy ra 2 thư mục tạm riêng; bản trong BACKUP_DIR không bao giờ bị mở.
#   scripts/compat_check.sh <OLD_DIR> <BACKUP_DIR>
set -euo pipefail
OLD_DIR="$1"; BACKUP_DIR="$2"
NEW_DIR="$(cd "$(dirname "$0")/.." && pwd)"
WORK="$(mktemp -d)"
cp "$NEW_DIR/crates/pglite-rs/examples/dump_db.rs" "$OLD_DIR/crates/pglite-rs/examples/dump_db.rs"
(cd "$OLD_DIR/crates/pglite-rs" && cargo build -q --release --example dump_db)
(cd "$NEW_DIR/crates/pglite-rs" && cargo build -q --release --example dump_db)
OLD_BIN="$(find "$OLD_DIR" -path '*/release/examples/dump_db' -type f | head -1)"
NEW_BIN="$(find "$NEW_DIR/target" "$NEW_DIR/crates/pglite-rs/target" -path "*/release/examples/dump_db" -type f 2>/dev/null | head -1 || true)"

fail=0; n=0
while IFS= read -r base; do
  n=$((n+1))
  for side in old new; do
    mkdir -p "$WORK/$side/$(dirname "$base")"
    for f in "$BACKUP_DIR/$base" "$BACKUP_DIR/$base".*; do
      [ -f "$f" ] && cp -p "$f" "$WORK/$side/$(dirname "$base")/"
    done
  done
  "$OLD_BIN" "$WORK/old/$base" > "$WORK/old.json" 2>/dev/null || echo "{\"_err\":\"old failed\"}" > "$WORK/old.json"
  before=$(cat "$WORK/new/$base".* 2>/dev/null | shasum -a 256)
  "$NEW_BIN" "$WORK/new/$base" > "$WORK/new.json" 2>/dev/null || echo "{\"_err\":\"new failed\"}" > "$WORK/new.json"
  if cmp -s "$WORK/old.json" "$WORK/new.json"; then
    echo "OK    $base ($(grep -c '"live_rows"' "$WORK/new.json") tables)"
  else
    echo "DIFF  $base"; diff "$WORK/old.json" "$WORK/new.json" | head -20; fail=1
  fi
  # Mở lại lần 2 bằng engine mới phải ra y hệt lần 1 (không hỏng sau khi engine mới chạm vào file).
  "$NEW_BIN" "$WORK/new/$base" > "$WORK/new2.json" 2>/dev/null || true
  cmp -s "$WORK/new.json" "$WORK/new2.json" || { echo "REOPEN-DIFF $base"; fail=1; }
  # Engine cũ đọc lại file mà engine mới đã mở phải ra y hệt.
  "$OLD_BIN" "$WORK/new/$base" > "$WORK/back.json" 2>/dev/null || true
  cmp -s "$WORK/new.json" "$WORK/back.json" || { echo "BACKWARD-DIFF $base"; fail=1; }
done < <(cd "$BACKUP_DIR" && find . -type f ! -name SHA256SUMS ! -name '*.expected.json' ! -name '*.bak' \
  | sed -E 's#^\./##; s#\.(v2\.rwal|rwal|wal|wal\.migrated)$##' | sort -u)
rm -rf "$WORK"
echo "checked $n databases"; exit $fail
