//! Chạy từng câu SQL (đối số dòng lệnh, `;;` tách câu) trên DB in-memory có setup golden, in kết quả wire và JSON.
use pglite_rs::PGlite;
fn main() {
    let mut db = PGlite::in_memory().unwrap();
    let setup = std::fs::read_to_string(concat!(env!("CARGO_MANIFEST_DIR"), "/tests/pg_golden/setup.sql")).unwrap();
    for s in setup.lines().filter(|l| !l.trim().is_empty()) {
        db.execute(s, &[]).unwrap();
    }
    for arg in std::env::args().skip(1) {
        for st in arg.split(";;") {
            println!(">> {}", st.trim());
            match db.query_wire(st.trim(), &[]) {
                Ok(w) => {
                    println!("   wire fields: {:?}", w.fields.iter().map(|f| (f.name.clone(), f.type_oid)).collect::<Vec<_>>());
                    for r in w.rows.iter().take(5) {
                        println!("   {:?}", r);
                    }
                }
                Err(e) => println!("   ERR {} [{}]", e, pglite_rs::sqlstate(&e)),
            }
            if std::env::var("JSON").is_ok() {
                match db.query(st.trim(), &[]) {
                    Ok(r) => println!("   json: {}", serde_json::to_string(&r.rows).unwrap()),
                    Err(e) => println!("   json ERR {e}"),
                }
            }
        }
    }
}
