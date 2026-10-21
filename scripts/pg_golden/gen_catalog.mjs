// Sinh crates/pglite-rs/src/engine/pg_catalog_data.rs từ PostgreSQL thật (@electric-sql/pglite): các dòng dựng
// sẵn của pg_type, pg_settings, pg_am, pg_proc (chỉ hàm engine có), danh sách cột + kiểu của các bảng catalog.
//   PGLITE_REF=<thư mục @electric-sql/pglite> node scripts/pg_golden/gen_catalog.mjs
import { readFileSync, writeFileSync } from 'node:fs'
import { pathToFileURL, fileURLToPath } from 'node:url'
import { join, dirname } from 'node:path'
import { homedir } from 'node:os'

const here = dirname(fileURLToPath(import.meta.url))
const crate = join(here, '../../crates/pglite-rs')
const pkgDir = process.env.PGLITE_REF || join(homedir(), 'Documents/GitHub/com.ai.vn-v2/node_modules/@electric-sql/pglite')
const { PGlite } = await import(pathToFileURL(join(pkgDir, 'dist', 'index.js')).href)
const parsers = {}
for (let oid = 0; oid < 20000; oid++) parsers[oid] = (x) => x
const db = new PGlite({ parsers })

const RELS = [
  'pg_catalog.pg_attribute', 'pg_catalog.pg_type', 'pg_catalog.pg_index', 'pg_catalog.pg_indexes', 'pg_catalog.pg_constraint',
  'pg_catalog.pg_class', 'pg_catalog.pg_namespace', 'pg_catalog.pg_proc', 'pg_catalog.pg_settings', 'pg_catalog.pg_database',
  'pg_catalog.pg_extension', 'pg_catalog.pg_stat_activity', 'pg_catalog.pg_roles', 'pg_catalog.pg_user', 'pg_catalog.pg_attrdef',
  'pg_catalog.pg_trigger', 'pg_catalog.pg_description', 'pg_catalog.pg_am', 'pg_catalog.pg_sequence',
  'information_schema.referential_constraints', 'information_schema.check_constraints', 'information_schema.triggers',
  'information_schema.routines', 'information_schema.parameters',
]
const esc = (s) => JSON.stringify(s)
// pglite trả mảng PG thành mảng JS: in lại dạng văn bản của PG `{a,"b c"}`
const pgArray = (a) => '{' + a.map((e) => (e === null ? 'NULL' : Array.isArray(e) ? pgArray(e)
  : /^$|[,{}"\\\s]|^null$/i.test(String(e)) ? '"' + String(e).replace(/\\/g, '\\\\').replace(/"/g, '\\"') + '"' : String(e))).join(',') + '}'
const cell = (v) => (v === null ? '\\N' : (Array.isArray(v) ? pgArray(v) : String(v)).replace(/\\/g, '\\\\').replace(/\t/g, '\\t').replace(/\n/g, '\\n'))
const out = [
  '//! Sinh bởi `scripts/pg_golden/gen_catalog.mjs` từ PostgreSQL thật. Không sửa tay.',
  '//! Dòng dữ liệu: các ô cách nhau bởi tab, `\\N` = NULL, `\\t` `\\n` `\\\\` thoát như COPY text.',
  '#![allow(dead_code)]',
  '',
  '/// (bảng, [(cột, kiểu theo format_type)])',
  '#[rustfmt::skip]',
  'pub(crate) const RELATIONS: &[(&str, &[(&str, &str)])] = &[',
]
for (const r of RELS) {
  const [s, n] = r.split('.')
  const q = await db.query(
    `SELECT a.attname, format_type(a.atttypid, a.atttypmod) t FROM pg_attribute a JOIN pg_class c ON c.oid=a.attrelid
     JOIN pg_namespace ns ON ns.oid=c.relnamespace WHERE ns.nspname=$1 AND c.relname=$2 AND a.attnum>0 AND NOT a.attisdropped ORDER BY a.attnum`, [s, n])
  const cols = q.rows.map((x) => `(${esc(x.attname)}, ${esc(x.t.replace(/^information_schema\./, ''))})`).join(', ')
  out.push(`    (${esc(r)}, &[${cols}]),`)
}
out.push('];', '')

async function rows(name, sql, note) {
  const q = await db.query(sql)
  const f = q.fields.map((x) => x.name)
  out.push(`/// ${note || sql.replace(/\s+/g, ' ')}`, '#[rustfmt::skip]', `pub(crate) const ${name}: &[&str] = &[`)
  for (const r of q.rows) out.push(`    ${esc(f.map((k) => cell(r[k])).join('\t'))},`)
  out.push('];', '')
  return q.rows.length
}
const known = new Set(
  [...readFileSync(join(crate, 'src/engine/known_fns.rs'), 'utf8').matchAll(/"([A-Z0-9_]+)"/g)].map((m) => m[1].toLowerCase()),
)
const n1 = await rows('PG_TYPE', 'SELECT * FROM pg_type WHERE typnamespace = 11 AND typrelid = 0')
const n2 = await rows('PG_SETTINGS', 'SELECT * FROM pg_settings')
const n3 = await rows('PG_AM', 'SELECT * FROM pg_am')
const procs = (await db.query("SELECT DISTINCT proname FROM pg_proc WHERE pronamespace = 11 AND prokind IN ('f','a','w')")).rows
  .map((r) => r.proname).filter((p) => known.has(p))
const list = procs.map((p) => `'${p}'`).join(',')
const n4 = await rows('PG_PROC', `SELECT * FROM pg_proc WHERE pronamespace = 11 AND proname IN (${list}) ORDER BY oid`,
  'SELECT * FROM pg_proc WHERE pronamespace = 11 AND proname IN (<hàm engine có trong known_fns.rs>) ORDER BY oid')
await rows('PG_NAMESPACE', 'SELECT * FROM pg_namespace ORDER BY oid')
await rows('PG_DATABASE', 'SELECT * FROM pg_database')
await rows('PG_EXTENSION', 'SELECT * FROM pg_extension')
await rows('PG_ROLES', "SELECT * FROM pg_roles WHERE rolname = 'postgres'")
await rows('PG_USER', "SELECT * FROM pg_user WHERE usename = 'postgres'")
writeFileSync(join(crate, 'src/engine/pg_catalog_data.rs'), out.join('\n'))
console.log({ pg_type: n1, pg_settings: n2, pg_am: n3, pg_proc: n4 })
