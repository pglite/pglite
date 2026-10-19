//! Dump toàn bộ dữ liệu vật lý của một DB ra JSON ổn định, để so engine cũ và mới (tương thích ngược).
//! Chỉ chạy trên BẢN COPY: mở DB có thể ghi thêm bản ghi đóng giao dịch dở vào WAL.
//!   cargo run --release --example dump_db -- <db_path>
use pglite_rs::StorageEngine;

fn main() {
    let path = std::env::args().nth(1).expect("usage: dump_db <db_path>");
    let storage = StorageEngine::new(path);
    let mut names: Vec<&String> = storage.tables.keys().collect();
    names.sort();
    let mut out = serde_json::Map::new();
    for name in names {
        let t = &storage.tables[name];
        let cols: Vec<serde_json::Value> = t
            .columns
            .iter()
            .map(|c| {
                serde_json::json!({
                    "name": c.name,
                    "type": format!("{:?}", c.data_type),
                    "pk": c.is_primary_key,
                    "nullable": c.is_nullable,
                    "default": c.default_value,
                    "comment": c.comment,
                })
            })
            .collect();
        let rows: Vec<serde_json::Value> = t
            .rows
            .iter()
            .enumerate()
            .filter(|(i, _)| !t.is_deleted.get(*i).copied().unwrap_or(false))
            .map(|(_, r)| serde_json::to_value(r).unwrap())
            .collect();
        out.insert(
            name.clone(),
            serde_json::json!({
                "comment": t.comment,
                "auto_increment": t.auto_increment,
                "columns": cols,
                "live_rows": rows.len(),
                "rows": rows,
            }),
        );
    }
    println!("{}", serde_json::to_string_pretty(&out).unwrap());
}
