//! Golden test: mỗi case trong `tests/pg_golden/cases.tsv` chạy trên pglite-rs (DB in-memory mới, sau
//! `setup.sql`) và được so với kết quả của PostgreSQL thật đã ghi sẵn trong `expected.jsonl`
//! (sinh bằng `node scripts/pg_golden/generate.mjs`). Không cần PostgreSQL khi chạy `cargo test`.
//!
//! `known_gaps.txt` liệt kê các case CHƯA khớp. Danh sách chỉ được co lại:
//! - case ngoài danh sách mà lệch PostgreSQL → test fail (hồi quy);
//! - case trong danh sách mà đã khớp → test fail, nhắc xoá khỏi danh sách.
//! Chạy `PG_GOLDEN_REPORT=1 cargo test --test pg_golden_test -- --nocapture` để in chi tiết từng case lệch.
use pglite_rs::{PGlite, Value};
use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

#[derive(Debug, Clone)]
struct Outcome {
    ok: bool,
    fields: Vec<String>,
    rows: Vec<Vec<Option<String>>>,
    error: String,
    sqlstate: String,
}

fn dir() -> std::path::PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/pg_golden")
}

fn run_case(setup: &[String], sql: &str, params: &[String]) -> Outcome {
    let mut db = PGlite::in_memory().unwrap();
    for s in setup {
        db.execute(s, &[]).unwrap_or_else(|e| panic!("setup failed: {s}: {e}"));
    }
    let params: Vec<Value> = params.iter().map(Value::text).collect();
    let mut out = Outcome { ok: true, fields: vec![], rows: vec![], error: String::new(), sqlstate: String::new() };
    for st in sql.split(" ;; ") {
        let p = if st.contains('$') && !st.contains("$$") && !st.trim_start().to_uppercase().starts_with("PREPARE") { params.clone() } else { vec![] };
        match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| db.query_wire(st, &p))) {
            Ok(Ok(w)) => {
                out.fields = w.fields.iter().map(|f| f.name.clone()).collect();
                out.rows = w.rows;
            }
            Ok(Err(e)) => {
                out.ok = false;
                out.sqlstate = pglite_rs::sqlstate(&e).to_string();
                out.error = e;
                break;
            }
            Err(_) => {
                out.ok = false;
                out.error = "PANIC".to_string();
                break;
            }
        }
    }
    out
}

fn canon_json(v: &serde_json::Value) -> serde_json::Value {
    match v {
        serde_json::Value::Object(m) => {
            let sorted: BTreeMap<String, serde_json::Value> = m.iter().map(|(k, v)| (k.clone(), canon_json(v))).collect();
            serde_json::to_value(sorted).unwrap()
        }
        serde_json::Value::Array(a) => serde_json::Value::Array(a.iter().map(canon_json).collect()),
        other => other.clone(),
    }
}

/// Hai ô text có "bằng nhau" không: chính xác, hoặc lỏng (số, bool t/true, JSON khác khoảng trắng/thứ tự khoá).
fn cell_eq(a: &Option<String>, b: &Option<String>) -> bool {
    match (a, b) {
        (None, None) => true,
        (Some(x), Some(y)) => {
            if x == y {
                return true;
            }
            let bool_of = |s: &str| match s {
                "t" | "true" => Some(true),
                "f" | "false" => Some(false),
                _ => None,
            };
            if let (Some(p), Some(q)) = (bool_of(x), bool_of(y)) {
                return p == q;
            }
            if let (Ok(p), Ok(q)) = (x.trim().parse::<f64>(), y.trim().parse::<f64>()) {
                if !x.trim().is_empty() && !y.trim().is_empty() && (p - q).abs() < 1e-9 * p.abs().max(1.0) {
                    return true;
                }
            }
            let jsonish = |s: &str| s.starts_with('{') || s.starts_with('[') || s.starts_with('"');
            if jsonish(x) && jsonish(y) {
                if let (Ok(p), Ok(q)) = (serde_json::from_str::<serde_json::Value>(x), serde_json::from_str::<serde_json::Value>(y)) {
                    return canon_json(&p) == canon_json(&q);
                }
            }
            false
        }
        _ => false,
    }
}

fn rows_eq(a: &[Vec<Option<String>>], b: &[Vec<Option<String>>], ordered: bool) -> bool {
    if a.len() != b.len() {
        return false;
    }
    if ordered {
        return a.iter().zip(b).all(|(r, s)| r.len() == s.len() && r.iter().zip(s).all(|(x, y)| cell_eq(x, y)));
    }
    // Không có ORDER BY: so như tập hợp có lặp (ghép tham lam)
    let mut pool: Vec<&Vec<Option<String>>> = b.iter().collect();
    for r in a {
        match pool.iter().position(|p| p.len() == r.len() && r.iter().zip(p.iter()).all(|(x, y)| cell_eq(x, y))) {
            Some(i) => {
                pool.remove(i);
            }
            None => return false,
        }
    }
    true
}

/// Case có giá trị thay đổi theo lần chạy: chỉ so số dòng và số cột.
fn is_volatile(sql: &str) -> bool {
    let s = sql.to_lowercase();
    [
        "now()", "current_timestamp", "current_time", "current_date", "localtime", "clock_timestamp", "statement_timestamp",
        "transaction_timestamp", "timeofday", "random", "uuid", "gen_salt", "crypt(", "pg_backend_pid", "txid_current", "xact_id",
        "pg_postmaster", "version()", "created", "t.*", "to_json(t)", "to_jsonb(t)", "row_to_json", "json_agg(t)", "jsonb_agg(t)",
        "pg_stat_activity", "tablesample", "size", "inet_server", "pg_sleep",
    ]
    .iter()
    .any(|k| s.contains(k))
        || s.contains("select * from t ")
        || s.ends_with("select * from t")
}

fn has_top_level_order_by(sql: &str) -> bool {
    let last = sql.split(" ;; ").last().unwrap_or(sql);
    // bỏ phần trong ngoặc để không tính ORDER BY bên trong hàm/subquery
    let mut depth = 0i32;
    let mut flat = String::new();
    for c in last.chars() {
        match c {
            '(' => depth += 1,
            ')' => depth -= 1,
            _ if depth == 0 => flat.push(c),
            _ => {}
        }
    }
    flat.to_lowercase().contains("order by")
}

fn compare(sql: &str, rs: &Outcome, pg: &serde_json::Value) -> Result<(), String> {
    let pg_ok = pg["ok"].as_bool().unwrap_or(false);
    if pg.get("skipped").is_some() {
        return Ok(());
    }
    match (pg_ok, rs.ok) {
        (false, false) => {
            // Cả hai lỗi: mã SQLSTATE phải trùng (lỗi "Unsupported" 42601 không được che lỗi thật / 0A000)
            let pg_code = pg["sqlstate"].as_str().unwrap_or("");
            if pg_code.is_empty() || pg_code == rs.sqlstate {
                Ok(())
            } else {
                Err(format!("cùng lỗi nhưng khác SQLSTATE: pg {} ({}), rs {} ({})", pg_code, pg["error"].as_str().unwrap_or(""), rs.sqlstate, rs.error))
            }
        }
        (false, true) => Err(format!("PostgreSQL báo lỗi ({} {}) nhưng pglite-rs chạy được", pg["sqlstate"].as_str().unwrap_or(""), pg["error"].as_str().unwrap_or(""))),
        (true, false) => Err(format!("pglite-rs báo lỗi: {}", rs.error)),
        (true, true) => {
            let pg_fields: Vec<String> = pg["fields"].as_array().map(|a| a.iter().map(|f| f.as_str().unwrap_or("").to_string()).collect()).unwrap_or_default();
            let pg_rows: Vec<Vec<Option<String>>> = pg["rows"]
                .as_array()
                .map(|rows| rows.iter().map(|r| r.as_array().map(|c| c.iter().map(|v| v.as_str().map(|s| s.to_string())).collect()).unwrap_or_default()).collect())
                .unwrap_or_default();
            if is_volatile(sql) {
                if pg_rows.len() == rs.rows.len() && pg_fields.len() == rs.fields.len() {
                    return Ok(());
                }
                return Err(format!("khác hình dạng: pg {} dòng × {} cột, rs {} dòng × {} cột", pg_rows.len(), pg_fields.len(), rs.rows.len(), rs.fields.len()));
            }
            if !rows_eq(&rs.rows, &pg_rows, has_top_level_order_by(sql)) {
                return Err(format!("khác dữ liệu:\n      pg: {:?}\n      rs: {:?}", trunc(&pg_rows), trunc(&rs.rows)));
            }
            if pg_fields != rs.fields {
                return Err(format!("khác tên cột: pg {:?} rs {:?}", pg_fields, rs.fields));
            }
            Ok(())
        }
    }
}

fn trunc(rows: &[Vec<Option<String>>]) -> String {
    let s = format!("{:?}", rows);
    if s.chars().count() > 300 { format!("{}…", s.chars().take(300).collect::<String>()) } else { s }
}

#[test]
fn matches_postgresql() {
    let d = dir();
    let setup: Vec<String> = std::fs::read_to_string(d.join("setup.sql")).unwrap().lines().filter(|l| !l.trim().is_empty()).map(|l| l.to_string()).collect();
    let expected: BTreeMap<String, serde_json::Value> = std::fs::read_to_string(d.join("expected.jsonl"))
        .unwrap()
        .lines()
        .filter(|l| !l.trim().is_empty())
        .map(|l| {
            let v: serde_json::Value = serde_json::from_str(l).unwrap();
            (v["id"].as_str().unwrap().to_string(), v)
        })
        .collect();
    let gaps: BTreeSet<String> = std::fs::read_to_string(d.join("known_gaps.txt"))
        .unwrap_or_default()
        .lines()
        .map(|l| l.split('#').next().unwrap_or("").trim().to_string())
        .filter(|l| !l.is_empty())
        .collect();
    let report = std::env::var("PG_GOLDEN_REPORT").is_ok();
    let mut regressions = Vec::new();
    let mut fixed = Vec::new();
    let mut failing = Vec::new();
    let mut total = 0;
    for line in std::fs::read_to_string(d.join("cases.tsv")).unwrap().lines() {
        if line.trim().is_empty() || line.starts_with('#') {
            continue;
        }
        let mut parts = line.splitn(3, '\t');
        let id = parts.next().unwrap().to_string();
        let sql = parts.next().unwrap_or("").to_string();
        let params: Vec<String> = serde_json::from_str(parts.next().unwrap_or("[]")).unwrap_or_default();
        let Some(pg) = expected.get(&id) else {
            panic!("case {id} chưa có kết quả PostgreSQL: chạy node scripts/pg_golden/generate.mjs");
        };
        total += 1;
        if std::env::var("PG_GOLDEN_TRACE").is_ok() {
            eprintln!("case {id}");
        }
        let rs = run_case(&setup, &sql, &params);
        let res = compare(&sql, &rs, pg);
        let in_gaps = gaps.contains(&id);
        match (&res, in_gaps) {
            (Err(e), false) => regressions.push(format!("{id}: {sql}\n    {e}")),
            (Ok(()), true) => fixed.push(id.clone()),
            _ => {}
        }
        if let Err(e) = &res {
            failing.push(id.clone());
            if report {
                println!("LỆCH {id}: {sql}\n    {e}");
            }
        }
    }
    println!("pg_golden: {} case, {} khớp PostgreSQL, {} còn lệch (known_gaps: {})", total, total - failing.len(), failing.len(), gaps.len());
    if std::env::var("PG_GOLDEN_WRITE_GAPS").is_ok() {
        std::fs::write(d.join("known_gaps.txt"), failing.join("\n") + "\n").unwrap();
    }
    assert!(regressions.is_empty(), "{} case lệch PostgreSQL mà không nằm trong known_gaps.txt:\n{}", regressions.len(), regressions.join("\n"));
    assert!(fixed.is_empty(), "các case đã khớp PostgreSQL, hãy xoá khỏi known_gaps.txt: {:?}", fixed);
}
