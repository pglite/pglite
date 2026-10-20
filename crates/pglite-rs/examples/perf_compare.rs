//! So hiệu năng engine cũ/mới trên cùng thao tác (chạy bản release, lấy trung vị nhiều lần).
//!   cargo run --release --example perf_compare -- <dir>
use pglite_rs::{PGlite, Value};
use std::time::Instant;

fn time<F: FnMut()>(mut f: F) -> f64 {
    let t = Instant::now();
    f();
    t.elapsed().as_secs_f64() * 1000.0
}

fn main() {
    let dir = std::env::args().nth(1).unwrap_or_else(|| std::env::temp_dir().to_string_lossy().to_string());
    let mut results: Vec<(String, Vec<f64>)> = Vec::new();
    for round in 0..5 {
        let path = format!("{}/perf_{}_{}.db", dir, std::process::id(), round);
        for suf in ["", ".rwal", ".v2.rwal", ".wal"] {
            let _ = std::fs::remove_file(format!("{}{}", path, suf));
        }
        let mut db = PGlite::open(&path).unwrap();
        db.execute("CREATE TABLE plain (id SERIAL PRIMARY KEY, name TEXT, age INT, active BOOLEAN)", &[]).unwrap();
        db.execute("CREATE TABLE cons (id SERIAL PRIMARY KEY, email TEXT UNIQUE NOT NULL, parent INT, age INT CHECK (age >= 0))", &[]).unwrap();
        let mut rec = |name: &str, ms: f64| {
            match results.iter_mut().find(|(n, _)| n == name) {
                Some((_, v)) => v.push(ms),
                None => results.push((name.to_string(), vec![ms])),
            }
        };
        let n = 100_000;
        rec("insert_plain_100k_tx", time(|| {
            db.execute("BEGIN", &[]).unwrap();
            for i in (0..n).step_by(500) {
                let mut ph = Vec::new();
                let mut ps = Vec::new();
                for j in 0..500 {
                    let k = ps.len();
                    ph.push(format!("(${}, ${}, ${})", k + 1, k + 2, k + 3));
                    ps.push(Value::text(format!("user{}", i + j)));
                    ps.push(Value::Int(((i + j) % 90) as i64));
                    ps.push(Value::Bool((i + j) % 2 == 0));
                }
                db.execute(&format!("INSERT INTO plain (name, age, active) VALUES {}", ph.join(",")), &ps).unwrap();
            }
            db.execute("COMMIT", &[]).unwrap();
        }));
        rec("insert_constrained_100k_tx", time(|| {
            db.execute("BEGIN", &[]).unwrap();
            for i in (0..n).step_by(500) {
                let mut ph = Vec::new();
                let mut ps = Vec::new();
                for j in 0..500 {
                    let k = ps.len();
                    ph.push(format!("(${}, ${})", k + 1, k + 2));
                    ps.push(Value::text(format!("u{}@x.vn", i + j)));
                    ps.push(Value::Int(((i + j) % 90) as i64));
                }
                db.execute(&format!("INSERT INTO cons (email, age) VALUES {}", ph.join(",")), &ps).unwrap();
            }
            db.execute("COMMIT", &[]).unwrap();
        }));
        rec("pk_lookup_20k", time(|| {
            for i in 1..=20_000i64 {
                db.query("SELECT * FROM plain WHERE id = $1", &[Value::Int(i)]).unwrap();
            }
        }));
        rec("autocommit_update_2k", time(|| {
            for i in 1..=2_000i64 {
                db.execute("UPDATE plain SET age = age + 1 WHERE id = $1", &[Value::Int(i)]).unwrap();
            }
        }));
        rec("scan_filter_agg_50x", time(|| {
            for _ in 0..50 {
                db.query("SELECT count(*), avg(age) FROM plain WHERE active = true AND age > 30", &[]).unwrap();
            }
        }));
        rec("scan_arith_20x", time(|| {
            for _ in 0..20 {
                db.query("SELECT id, age * 2 + id / 3 AS x FROM plain WHERE age + 1 > 30", &[]).unwrap();
            }
        }));
        rec("bulk_update_tx", time(|| {
            db.execute("BEGIN", &[]).unwrap();
            db.execute("UPDATE plain SET active = NOT active WHERE age < 45", &[]).unwrap();
            db.execute("COMMIT", &[]).unwrap();
        }));
        rec("autocommit_delete_2k", time(|| {
            for i in 50_001..=52_000i64 {
                db.execute("DELETE FROM plain WHERE id = $1", &[Value::Int(i)]).unwrap();
            }
        }));
        drop(db);
        rec("reopen_replay", time(|| {
            let _db = PGlite::open(&path).unwrap();
        }));
        for suf in ["", ".rwal", ".v2.rwal", ".wal"] {
            let _ = std::fs::remove_file(format!("{}{}", path, suf));
        }
    }
    for (name, mut v) in results {
        v.sort_by(|a, b| a.partial_cmp(b).unwrap());
        println!("{:<28} {:>10.1} ms", name, v[v.len() / 2]);
    }
}
