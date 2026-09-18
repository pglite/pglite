use pglite_rs::{params, PGlite};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct UserProfile {
    id: i64,
    name: String,
    email: String,
    age: i32,
    tags: Vec<String>,
    active: bool,
}

#[derive(Debug, Deserialize)]
struct UserSummary {
    total_users: i64,
    avg_age: f64,
}

fn main() -> Result<(), String> {
    println!("🦀 PGlite Native Rust: Typed Deserialization Example (query_as)\n");

    let mut db = PGlite::in_memory()?;

    // 1. Setup schema
    db.exec(
        "CREATE TABLE users (
            id SERIAL PRIMARY KEY,
            name TEXT NOT NULL,
            email TEXT NOT NULL,
            age INT NOT NULL,
            tags JSONB NOT NULL,
            active BOOLEAN DEFAULT true
        );",
        &[],
    )?;

    // 2. Insert records
    db.exec(
        "INSERT INTO users (name, email, age, tags, active) VALUES ($1, $2, $3, $4, $5);",
        &params!["Alice", "alice@rust.org", 28, r#"["developer", "rust", "database"]"#, true],
    )?;
    db.exec(
        "INSERT INTO users (name, email, age, tags, active) VALUES ($1, $2, $3, $4, $5);",
        &params!["Bob", "bob@tokio.rs", 34, r#"["architect", "systems"]"#, true],
    )?;
    db.exec(
        "INSERT INTO users (name, email, age, tags, active) VALUES ($1, $2, $3, $4, $5);",
        &params!["Charlie", "charlie@postgres.org", 22, r#"["intern"]"#, false],
    )?;

    // 3. Query into Vec<UserProfile> directly
    println!("[1] Querying all active users into Rust struct Vec<UserProfile>:");
    let active_users: Vec<UserProfile> = db.query_as(
        "SELECT id, name, email, age, tags, active FROM users WHERE active = $1 ORDER BY age ASC;",
        &params![true],
    )?;

    for user in &active_users {
        println!("  • User #{} | Name: {:<8} | Age: {} | Tags: {:?}",
            user.id, user.name, user.age, user.tags
        );
    }

    // 4. Query single record using query_first_as
    println!("\n[2] Querying single user with query_first_as:");
    let alice: Option<UserProfile> = db.query_first_as(
        "SELECT id, name, email, age, tags, active FROM users WHERE name = $1;",
        &params!["Alice"],
    )?;
    if let Some(user) = alice {
        println!("  Found Alice: {:?}", user);
    }

    // 5. Query Aggregations into Summary Struct
    println!("\n[3] Querying aggregation summary:");
    let summary: Option<UserSummary> = db.query_first_as(
        "SELECT COUNT(*) AS total_users, AVG(age) AS avg_age FROM users;",
        &[],
    )?;
    if let Some(s) = summary {
        println!("  Total users: {}, Average age: {:.1} years", s.total_users, s.avg_age);
    }

    println!("\n✨ Typed Query Example Completed Successfully!");
    Ok(())
}
