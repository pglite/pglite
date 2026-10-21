#!/usr/bin/env python3
"""Sinh src/engine/pg_settings_data.rs từ kết quả `SHOW ALL` (case CF04) của PG thật trong expected.jsonl."""
import json, os
here = os.path.dirname(os.path.abspath(__file__))
crate = os.path.join(here, '..', '..', 'crates', 'pglite-rs')
rows = None
for line in open(os.path.join(crate, 'tests', 'pg_golden', 'expected.jsonl')):
    d = json.loads(line)
    if d['id'] == 'CF04':
        rows = d['rows']
assert rows
out = ['//! Sinh bởi `scripts/pg_golden/gen_settings.py` từ `SHOW ALL` của PostgreSQL 17: (tên, giá trị mặc định, mô tả).',
       '//! Không sửa tay.', '#[rustfmt::skip]', 'pub(crate) const SETTINGS: &[(&str, &str, &str)] = &[']
for n, s, desc in rows:
    out.append('    (%s, %s, %s),' % (json.dumps(n), json.dumps(s), json.dumps(desc)))
out.append('];\n')
open(os.path.join(crate, 'src', 'engine', 'pg_settings_data.rs'), 'w').write('\n'.join(out))
print(len(rows))
