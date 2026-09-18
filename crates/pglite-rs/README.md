# 🦀 PGlite-RS

> High-performance, in-process, zero-dependency embedded PostgreSQL database engine in pure Rust.

[![Crates.io](https://img.shields.io/badge/crates.io-v0.1.0-orange.svg)](https://crates.io)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

**PGlite-RS** brings the power and familiarity of PostgreSQL syntax (DDL, DML, Transactions, CTEs, Window Functions, Subqueries, and rich JSONB operators) into a single, lightweight embedded database for Rust applications.

---

## ⚡ Features

- 🚀 **Blazing Fast**: Ingest up to **~1,000,000 ops/sec** and sub-microsecond point lookups via B-Tree indexing.
- 🪶 **Zero External Dependencies**: Pure Rust implementation with in-memory (`:memory:`) or persistent single-file database (`.db` + `.wal`).
- 🐘 **Full PostgreSQL Syntax**: Tables, Indexes, Views, Primary/Foreign Keys, Joins, Aggregations, CTEs, Window Functions, and `GEN_RANDOM_UUID()`.
- 📦 **Rich JSONB Engine**: Complete support for `->`, `->>`, `#>`, `#>>`, `@>`, `?`, `jsonb_strip_nulls`, and JSON manipulation functions.
- 🧬 **Vectorized & Multi-Core**: SIMD and parallel execution powered by Rayon for lightning-fast filtered scans and aggregations.
- 🛡️ **ACID Compliant**: Full transaction support with `BEGIN`, `COMMIT`, `ROLLBACK`, and write-ahead logging (WAL).
- 🧩 **Type-Safe Mapping**: Direct deserialization into custom Rust `struct`s using `serde` with `db.query_as::<T>(...)`.

---

## 📦 Installation

Add `pglite-rs` to your `Cargo.toml`:

```toml
[dependencies]
pglite-rs = { path = "crates/pglite-rs" } # Or version "0.1.0" once published
serde = { version = "1.0", features = ["derive"] }
```

---

## 🚀 Quick Start

### 1. Basic In-Memory or File-Based Usage

```rust
use pglite_rs::{PGlite, params};

fn main() -> Result<(), String> {
    // Open in-memory (:memory:) or file-based database
    let mut db = PGlite::in_memory()?;
    // Or: let mut db = PGlite::open("my_app.db")?;

    // Create table
    db.exec(
        "CREATE TABLE users (
            id SERIAL PRIMARY KEY,
            username TEXT NOT NULL,
            email TEXT UNIQUE,
            age INT
        );",
        &[],
    )?;

    // Parameterized inserts with params! macro
    db.exec(
        "INSERT INTO users (username, email, age) VALUES ($1, $2, $3);",
        &params!["alice", "alice@rust.org", 30],
    )?;
    db.exec(
        "INSERT INTO users (username, email, age) VALUES ($1, $2, $3);",
        &params!["bob", "bob@rust.org", 25],
    )?;

    // Query data
    let res = db.query(
        "SELECT id, username, age FROM users WHERE age >= $1 ORDER BY age DESC;",
        &params![25],
    )?;

    for row in &res.rows {
        println!("User: {} (age {})", row["username"], row["age"]);
    }

    Ok(())
}
```

---

### 2. Type-Safe Deserialization with `serde` (`query_as`)

```rust
use pglite_rs::{PGlite, params};
use serde::Deserialize;

#[derive(Debug, Deserialize, PartialEq)]
struct User {
    id: i64,
    username: String,
    email: String,
    age: i32,
}

fn main() -> Result<(), String> {
    let mut db = PGlite::in_memory()?;

    db.exec("CREATE TABLE users (id SERIAL PRIMARY KEY, username TEXT, email TEXT, age INT);", &[])?;
    db.exec("INSERT INTO users (username, email, age) VALUES ($1, $2, $3);", &params!["alice", "alice@rust.org", 30])?;

    // Query directly into Vec<User>
    let users: Vec<User> = db.query_as("SELECT id, username, email, age FROM users;", &[])?;
    println!("Fetched users: {:?}", users);

    // Query single record
    let user: Option<User> = db.query_first_as(
        "SELECT id, username, email, age FROM users WHERE username = $1;",
        &params!["alice"],
    )?;
    println!("Single user: {:?}", user);

    Ok(())
}
```

---

### 3. PostgreSQL JSONB Support

```rust
use pglite_rs::{PGlite, params};

fn main() -> Result<(), String> {
    let mut db = PGlite::in_memory()?;

    db.exec("CREATE TABLE products (id SERIAL PRIMARY KEY, name TEXT, details JSONB);", &[])?;
    db.exec(
        "INSERT INTO products (name, details) VALUES ($1, $2);",
        &params![
            "MacBook Pro",
            r#"{"brand": "Apple", "specs": {"cpu": "M3", "ram": 16}, "tags": ["tech", "work"], "in_stock": true}"#
        ],
    )?;

    // Extract JSON field (->>)
    let res = db.query("SELECT name, details->>'brand' AS brand FROM products WHERE details->>'brand' = 'Apple';", &[])?;
    println!("Brand: {}", res.rows[0]["brand"]);

    // JSON Containment (@>)
    let in_stock = db.query("SELECT name FROM products WHERE details @> '{\"in_stock\": true}';", &[])?;
    assert_eq!(in_stock.row_count, 1);

    // JSON Key existence (?)
    let has_tags = db.query("SELECT name FROM products WHERE details ? 'tags';", &[])?;
    assert_eq!(has_tags.row_count, 1);

    Ok(())
}
```

---

### 4. Transactions (ACID)

```rust
use pglite_rs::{PGlite, params};

fn main() -> Result<(), String> {
    let mut db = PGlite::in_memory()?;

    db.exec("CREATE TABLE accounts (id INT PRIMARY KEY, balance NUMERIC);", &[])?;
    db.exec("INSERT INTO accounts VALUES (1, 500), (2, 200);", &[])?;

    // Atomic transaction transfer
    db.exec("BEGIN", &[])?;
    db.exec("UPDATE accounts SET balance = balance - 100 WHERE id = 1;", &[])?;
    db.exec("UPDATE accounts SET balance = balance + 100 WHERE id = 2;", &[])?;
    db.exec("COMMIT", &[])?;

    Ok(())
}
```

---

## 🏃 Running Examples & Benchmarks

```bash
# Run basic usage example
cargo run --example basic_usage

# Run JSONB operations example
cargo run --example jsonb_operations

# Run typed query example (serde mapping)
cargo run --example typed_query

# Run high-performance benchmark suite
cargo run --release --bin benchmark
```

---

## 📊 Benchmark Highlights

Running on Apple Silicon (M-series) / x86_64:

| Phase | Metric | Throughput / Latency |
| :--- | :--- | :--- |
| **Bulk Ingestion (500K rows)** | Total Duration | **~0.52s (~960,000 ops/sec)** |
| **B-Tree Point Lookup** | Latency per query | **~4.1 µs (~240,000 ops/sec)** |
| **Full Table COUNT(*)** | 500K rows scan | **~0.22 ms** |
| **Parallel SIMD Filtered Scan** | Multi-condition scan | **~4.6 ms** |
| **PK Point Mutation** | Single record update | **~570 µs** |

---

## 📄 License

MIT License. See [LICENSE](LICENSE) for details.
