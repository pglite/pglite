#[cfg(feature = "napi-binding")]
pub mod binding;
pub mod engine;
pub mod storage;
pub mod types;

#[cfg(feature = "napi-binding")]
pub use binding::LitePostgresNative;
pub use engine::Executor;
pub use storage::StorageEngine;
pub use types::{ColumnDef, DataType, QueryResult, Value};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_jsonb_rust() {
        let storage = StorageEngine::new(":memory:".to_string());
        let mut executor = Executor::new(storage);

        executor.execute("CREATE TABLE products (id INT PRIMARY KEY, name TEXT, details JSONB);", &[]).unwrap();
        executor.execute("INSERT INTO products (id, name, details) VALUES (1, 'Laptop', '{\"brand\": \"Apple\", \"specs\": {\"cpu\": \"M3\", \"ram\": 16, \"ports\": [\"USB-C\", \"MagSafe\"]}, \"tags\": [\"tech\", \"work\"], \"in_stock\": true, \"null_field\": null}');", &[]).unwrap();

        let r1 = executor.execute("SELECT name, details->'brand' AS brand_json, details->>'brand' AS brand_text, details->'specs'->>'cpu' AS cpu FROM products;", &[]).unwrap();
        println!("r1: {:?}", r1.rows);

        let r2 = executor.execute("SELECT name, details#>'{\"specs\", \"cpu\"}' AS cpu_json, details#>>'{\"specs\", \"cpu\"}' AS cpu_text FROM products;", &[]).unwrap();
        println!("r2: {:?}", r2.rows);

        let r4 = executor.execute("SELECT name FROM products WHERE details->>'brand' = 'Apple';", &[]).unwrap();
        println!("r4: {:?}", r4.rows);

        let r5 = executor.execute("SELECT name FROM products WHERE details @> '{\"brand\": \"Apple\"}';", &[]).unwrap();
        println!("r5: {:?}", r5.rows);

        let r6 = executor.execute("SELECT name FROM products WHERE details ? 'in_stock';", &[]).unwrap();
        println!("r6: {:?}", r6.rows);

        let r7 = executor.execute("SELECT name, JSONB_TYPEOF(details->'specs') AS specs_type, JSON_ARRAY_LENGTH(details->'tags') AS tag_count FROM products WHERE details ? 'tags';", &[]).unwrap();
        println!("r7: {:?}", r7.rows);

        let r8 = executor.execute("SELECT JSONB_STRIP_NULLS(details) AS cleaned FROM products WHERE id = 1;", &[]).unwrap();
        println!("r8: {:?}", r8.rows);

        assert_eq!(r5.rows.len(), 1);
        assert_eq!(r6.rows.len(), 1);
    }
}
