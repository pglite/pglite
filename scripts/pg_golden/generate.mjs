// Sinh kết quả "chuẩn" cho bộ golden test của pglite-rs bằng PostgreSQL thật (@electric-sql/pglite, PG 17 WASM).
// Chỉ cần chạy lại khi thêm/sửa case trong crates/pglite-rs/tests/pg_golden/cases.tsv:
//   PGLITE_REF=<thư mục @electric-sql/pglite> node scripts/pg_golden/generate.mjs
// Mặc định PGLITE_REF = ~/Documents/GitHub/com.ai.vn-v2/node_modules/@electric-sql/pglite
// Mỗi case: id <TAB> sql <TAB> params-json; nhiều câu trong một case nối bằng " ;; ", chỉ kết quả câu cuối được so.
import { readFileSync, writeFileSync } from 'node:fs'
import { pathToFileURL, fileURLToPath } from 'node:url'
import { join, dirname } from 'node:path'
import { homedir } from 'node:os'

const here = dirname(fileURLToPath(import.meta.url))
const dir = join(here, '../../crates/pglite-rs/tests/pg_golden')
const pkgDir = process.env.PGLITE_REF || join(homedir(), 'Documents/GitHub/com.ai.vn-v2/node_modules/@electric-sql/pglite')
const only = process.argv[2]
const imp = (p) => import(pathToFileURL(join(pkgDir, 'dist', p)).href)
const { PGlite } = await imp('index.js')
const { pgcrypto } = await imp('contrib/pgcrypto.js')
const { uuid_ossp } = await imp('contrib/uuid_ossp.js')
const { unaccent } = await imp('contrib/unaccent.js')

// Giữ nguyên text output của PostgreSQL cho mọi OID (giống query_wire của pglite-rs).
const parsers = {}
for (let oid = 0; oid < 20000; oid++) parsers[oid] = (x) => x

const setup = readFileSync(join(dir, 'setup.sql'), 'utf8').split('\n').filter((l) => l.trim())
const cases = readFileSync(join(dir, 'cases.tsv'), 'utf8').split('\n').filter((l) => l.trim() && !l.startsWith('#'))
const outPath = join(dir, 'expected.jsonl')
const previous = new Map()
if (only) {
  for (const l of readFileSync(outPath, 'utf8').split('\n').filter(Boolean)) {
    const r = JSON.parse(l)
    previous.set(r.id, l)
  }
}

const base = new PGlite({ extensions: { pgcrypto, uuid_ossp, unaccent }, parsers })
for (const s of setup) await base.query(s)

const out = []
for (const line of cases) {
  const [id, sql, paramsJson] = line.split('\t')
  if (only && !id.startsWith(only)) {
    if (previous.has(id)) out.push(previous.get(id))
    continue
  }
  const params = JSON.parse(paramsJson || '[]')
  const rec = { id, ok: true }
  if (/FROM\s+STDIN/i.test(sql)) {
    out.push(JSON.stringify({ ...rec, ok: false, skipped: true, error: 'COPY FROM STDIN cần dữ liệu đi kèm' }))
    continue
  }
  const db = await base.clone()
  // clone() làm mất cache sequence (nextval nhảy 32) và lấy múi giờ máy: chuẩn hoá cả hai.
  await db.exec(`SET TimeZone = 'UTC'; SELECT setval('t_id_seq', (SELECT max(id) FROM t)); SELECT setval('u_id_seq', (SELECT max(id) FROM u));`)
  const stmts = sql.split(' ;; ')
  for (let i = 0; i < stmts.length; i++) {
    const st = stmts[i]
    const p = st.includes('$') && !st.includes('$$') && !/^\s*PREPARE\b/i.test(st) ? params : []
    try {
      const res = p.length === 0 && /;\s*\S/.test(st.replace(/\$\$[\s\S]*?\$\$/g, ''))
        ? (await db.exec(st, { parsers })).at(-1)
        : await db.query(st, p, { rowMode: 'array', parsers })
      rec.fields = res.fields.map((f) => f.name)
      rec.rows = res.rows.map((r) => (Array.isArray(r) ? r : res.fields.map((f) => r[f.name])))
    } catch (e) {
      rec.ok = false
      rec.error = e.message
      rec.sqlstate = e.code
      rec.failed_stmt = i
      break
    }
  }
  out.push(JSON.stringify(rec))
  await db.close()
}
await base.close()
writeFileSync(outPath, out.join('\n') + '\n')
console.log(`wrote ${out.length} results to ${outPath}`)
