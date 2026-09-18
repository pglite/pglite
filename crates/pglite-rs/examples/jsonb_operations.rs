use pglite_rs::{params, PGlite};

fn main() -> Result<(), String> {
    println!("🦀 PGlite Native Rust: JSONB Operations Example\n");

    let mut db = PGlite::in_memory()?;

    // 1. Create table with JSONB document column
    db.exec(
        "CREATE TABLE products (
            id SERIAL PRIMARY KEY,
            name TEXT NOT NULL,
            attributes JSONB NOT NULL
        );",
        &[],
    )?;

    // 2. Insert rich JSON documents
    db.exec(
        "INSERT INTO products (name, attributes) VALUES ($1, $2);",
        &params![
            "MacBook Pro 16",
            r#"{
                "brand": "Apple",
                "category": "laptops",
                "specs": {
                    "cpu": "M3 Max",
                    "ram": 64,
                    "storage": "2TB SSD"
                },
                "ports": ["Thunderbolt 4", "HDMI", "SD Card", "MagSafe 3"],
                "in_stock": true,
                "ratings": {"average": 4.9, "count": 128}
            }"#
        ],
    )?;

    db.exec(
        "INSERT INTO products (name, attributes) VALUES ($1, $2);",
        &params![
            "ThinkPad X1 Carbon",
            r#"{
                "brand": "Lenovo",
                "category": "laptops",
                "specs": {
                    "cpu": "Intel Core Ultra 7",
                    "ram": 32,
                    "storage": "1TB SSD"
                },
                "ports": ["Thunderbolt 4", "USB-A", "HDMI"],
                "in_stock": true,
                "ratings": {"average": 4.7, "count": 95}
            }"#
        ],
    )?;

    db.exec(
        "INSERT INTO products (name, attributes) VALUES ($1, $2);",
        &params![
            "Dell XPS 13",
            r#"{
                "brand": "Dell",
                "category": "laptops",
                "specs": {
                    "cpu": "Intel Core Ultra 5",
                    "ram": 16,
                    "storage": "512GB SSD"
                },
                "ports": ["Thunderbolt 4"],
                "in_stock": false,
                "ratings": {"average": 4.3, "count": 42}
            }"#
        ],
    )?;
    println!("✓ Inserted 3 products with nested JSONB attributes");

    // 3. Extract JSON fields using -> and ->>
    println!("\n[1] Extracting JSONB fields (-> and ->>):");
    let res = db.query(
        "SELECT name, attributes->>'brand' AS brand, attributes->'specs'->>'cpu' AS cpu FROM products;",
        &[],
    )?;
    for row in &res.rows {
        println!("  • {} | Brand: {} | CPU: {}", row["name"], row["brand"], row["cpu"]);
    }

    // 4. Nested Path Extraction (#> and #>>)
    println!("\n[2] Nested path extraction (#>):");
    let res = db.query(
        "SELECT name, attributes#>>'{\"specs\",\"ram\"}' AS ram_gb FROM products WHERE attributes->>'brand' = 'Apple';",
        &[],
    )?;
    for row in &res.rows {
        println!("  • {} RAM: {} GB", row["name"], row["ram_gb"]);
    }

    // 5. Containment (@>) and Key Existence (?)
    println!("\n[3] JSONB Containment Filter (@>):");
    let res = db.query(
        "SELECT name FROM products WHERE attributes @> '{\"in_stock\": true}';",
        &[],
    )?;
    println!("  In stock products: {:?}", res.rows);

    println!("\n[4] JSONB Key Existence (?):");
    let res = db.query(
        "SELECT name FROM products WHERE attributes ? 'ports';",
        &[],
    )?;
    println!("  Products with 'ports' attribute count: {}", res.row_count);

    println!("\n✨ JSONB Operations Example Completed Successfully!");
    Ok(())
}
