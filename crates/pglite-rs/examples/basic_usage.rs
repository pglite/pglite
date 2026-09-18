use pglite_rs::{params, PGlite};

fn main() -> Result<(), String> {
    println!("🦀 PGlite Native Rust: Basic Usage Example\n");

    // 1. Initialize an in-memory database
    let mut db = PGlite::in_memory()?;
    println!("✓ In-memory database initialized");

    // 2. Create tables
    db.exec(
        "CREATE TABLE accounts (
            id SERIAL PRIMARY KEY,
            username TEXT NOT NULL,
            email TEXT UNIQUE,
            balance NUMERIC DEFAULT 0,
            is_active BOOLEAN DEFAULT true
        );",
        &[],
    )?;
    println!("✓ Table 'accounts' created");

    // 3. Insert records with parameterized queries
    db.exec(
        "INSERT INTO accounts (username, email, balance, is_active) VALUES ($1, $2, $3, $4);",
        &params!["alice", "alice@example.com", 1500.50, true],
    )?;
    db.exec(
        "INSERT INTO accounts (username, email, balance, is_active) VALUES ($1, $2, $3, $4);",
        &params!["bob", "bob@example.com", 250.00, true],
    )?;
    db.exec(
        "INSERT INTO accounts (username, email, balance, is_active) VALUES ($1, $2, $3, $4);",
        &params!["charlie", "charlie@example.com", 0.00, false],
    )?;
    println!("✓ Inserted 3 accounts with params![]");

    // 4. Query data
    let query_result = db.query(
        "SELECT id, username, balance FROM accounts WHERE is_active = $1 ORDER BY balance DESC;",
        &params![true],
    )?;

    println!("\nActive Accounts (Row Count: {}):", query_result.row_count);
    for row in &query_result.rows {
        println!("  • ID: {}, Username: {}, Balance: ${}",
            row["id"], row["username"], row["balance"]
        );
    }

    // 5. Transaction: Atomically transfer balance
    println!("\n--- Performing Atomic Transfer in Transaction ---");
    db.exec("BEGIN", &[])?;
    db.exec(
        "UPDATE accounts SET balance = balance - $1 WHERE username = $2;",
        &params![100.0, "alice"],
    )?;
    db.exec(
        "UPDATE accounts SET balance = balance + $1 WHERE username = $2;",
        &params![100.0, "bob"],
    )?;
    db.exec("COMMIT", &[])?;
    println!("✓ Transfer committed successfully");

    // Verify updated balances
    let final_res = db.query_json("SELECT username, balance FROM accounts WHERE username IN ('alice', 'bob');", &[])?;
    println!("Updated Balances: {}", final_res);

    println!("\n✨ Basic Usage Example Completed Successfully!");
    Ok(())
}
