//! # PGlite-RS: Embedded PostgreSQL Database Engine in Pure Rust
//!
//! `pglite-rs` is a lightweight, zero-dependency, in-process embedded PostgreSQL database engine
//! written in Rust. It provides full PostgreSQL SQL syntax support, JSONB manipulation, WAL
//! persistence, B-Tree indexes, and SIMD/Rayon vectorized execution.
//!
//! ## Quick Start
//!
//! ```rust,no_run
//! use pglite_rs::{PGlite, params};
//! use serde::Deserialize;
//!
//! #[derive(Debug, Deserialize)]
//! struct User {
//!     id: i64,
//!     name: String,
//!     age: i32,
//! }
//!
//! fn main() -> Result<(), String> {
//!     // Open in-memory or on-disk database
//!     let mut db = PGlite::in_memory()?;
//!
//!     // Create table and insert records
//!     db.exec("CREATE TABLE users (id SERIAL PRIMARY KEY, name TEXT, age INT);", &[])?;
//!     db.exec("INSERT INTO users (name, age) VALUES ($1, $2);", &params!["Alice", 30])?;
//!     db.exec("INSERT INTO users (name, age) VALUES ($1, $2);", &params!["Bob", 25])?;
//!
//!     // Query with typed serde deserialization
//!     let users: Vec<User> = db.query_as("SELECT id, name, age FROM users WHERE age >= $1;", &params![25])?;
//!     println!("Found users: {:?}", users);
//!
//!     Ok(())
//! }
//! ```

#[cfg(feature = "napi-binding")]
pub mod binding;
#[cfg(feature = "wasm-binding")]
pub mod wasm_binding;
pub mod engine;
pub mod storage;
pub mod types;

#[cfg(feature = "napi-binding")]
pub use binding::LitePostgresNative;
#[cfg(feature = "wasm-binding")]
pub use wasm_binding::PGliteWasm;
pub use engine::Executor;
pub use storage::StorageEngine;
pub use types::{ColumnDef, DataType, FieldInfo, QueryResult, Value};

/// High-level embedded PostgreSQL database client for native Rust applications.
pub struct PGlite {
    executor: Executor,
    filepath: String,
}

impl PGlite {
    /// Open an on-disk database file at the specified path (creates if not exists).
    ///
    /// # Example
    /// ```rust,no_run
    /// use pglite_rs::PGlite;
    /// let mut db = PGlite::open("app_data.db").unwrap();
    /// ```
    pub fn open(filepath: impl AsRef<str>) -> Result<Self, String> {
        let path = filepath.as_ref().to_string();
        let storage = StorageEngine::new(path.clone());
        let executor = Executor::new(storage);
        Ok(Self {
            executor,
            filepath: path,
        })
    }

    /// Open a temporary, high-speed in-memory database (`:memory:`).
    ///
    /// # Example
    /// ```rust
    /// use pglite_rs::PGlite;
    /// let mut db = PGlite::in_memory().unwrap();
    /// ```
    pub fn in_memory() -> Result<Self, String> {
        Self::open(":memory:")
    }

    /// Execute a SQL statement (DDL, DML, or Query) with parameterized values.
    ///
    /// # Example
    /// ```rust
    /// use pglite_rs::{PGlite, params};
    /// let mut db = PGlite::in_memory().unwrap();
    /// db.execute("CREATE TABLE items (id INT, label TEXT);", &[]).unwrap();
    /// db.execute("INSERT INTO items (id, label) VALUES ($1, $2);", &params![1, "Widget"]).unwrap();
    /// ```
    pub fn execute(&mut self, sql: &str, params: &[Value]) -> Result<QueryResult, String> {
        self.executor.execute(sql, params)
    }

    /// Alias for `execute`.
    #[inline]
    pub fn exec(&mut self, sql: &str, params: &[Value]) -> Result<QueryResult, String> {
        self.execute(sql, params)
    }

    /// Alias for `execute`.
    #[inline]
    pub fn query(&mut self, sql: &str, params: &[Value]) -> Result<QueryResult, String> {
        self.execute(sql, params)
    }

    /// Execute a query and return rows serialized as a JSON string.
    ///
    /// # Example
    /// ```rust
    /// use pglite_rs::{PGlite, params};
    /// let mut db = PGlite::in_memory().unwrap();
    /// db.exec("CREATE TABLE tags (name TEXT);", &[]).unwrap();
    /// db.exec("INSERT INTO tags VALUES ('rust'), ('database');", &[]).unwrap();
    /// let json_output = db.query_json("SELECT * FROM tags;", &[]).unwrap();
    /// assert!(json_output.contains("rust"));
    /// ```
    pub fn query_json(&mut self, sql: &str, params: &[Value]) -> Result<String, String> {
        self.executor.execute_rows_json(sql, params)
    }

    /// Execute a query and deserialize the resulting rows into typed Rust structs using `serde`.
    ///
    /// # Example
    /// ```rust
    /// use pglite_rs::{PGlite, params};
    /// use serde::Deserialize;
    ///
    /// #[derive(Debug, Deserialize, PartialEq)]
    /// struct Product {
    ///     id: i64,
    ///     name: String,
    ///     price: f64,
    /// }
    ///
    /// let mut db = PGlite::in_memory().unwrap();
    /// db.exec("CREATE TABLE products (id INT, name TEXT, price FLOAT);", &[]).unwrap();
    /// db.exec("INSERT INTO products VALUES (1, 'Book', 19.99);", &[]).unwrap();
    ///
    /// let products: Vec<Product> = db.query_as("SELECT * FROM products;", &[]).unwrap();
    /// assert_eq!(products.len(), 1);
    /// assert_eq!(products[0].name, "Book");
    /// ```
    pub fn query_as<T: serde::de::DeserializeOwned>(
        &mut self,
        sql: &str,
        params: &[Value],
    ) -> Result<Vec<T>, String> {
        let json_str = self.executor.execute_rows_json(sql, params)?;
        serde_json::from_str(&json_str)
            .map_err(|e| format!("Failed to deserialize query result into struct: {e}"))
    }

    /// Execute a query and deserialize the first matching row into an `Option<T>`.
    pub fn query_first_as<T: serde::de::DeserializeOwned>(
        &mut self,
        sql: &str,
        params: &[Value],
    ) -> Result<Option<T>, String> {
        let mut list: Vec<T> = self.query_as(sql, params)?;
        if list.is_empty() {
            Ok(None)
        } else {
            Ok(Some(list.remove(0)))
        }
    }

    /// Flush all memory-buffered WAL and table pages to disk.
    pub fn flush(&mut self) {
        self.executor.storage.flush();
    }

    /// Persist pending data and close database handle.
    pub fn close(&mut self) {
        self.executor.storage.flush();
    }

    /// Get the database filepath.
    pub fn filepath(&self) -> &str {
        &self.filepath
    }

    /// Reference to underlying raw `Executor`.
    pub fn executor(&self) -> &Executor {
        &self.executor
    }

    /// Mutable reference to underlying raw `Executor`.
    pub fn executor_mut(&mut self) -> &mut Executor {
        &mut self.executor
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::Deserialize;

    #[derive(Debug, Deserialize, PartialEq)]
    struct ProductRow {
        id: i64,
        name: String,
        details: serde_json::Value,
    }

    #[test]
    fn test_pglite_rust_client_api() {
        let mut db = PGlite::in_memory().unwrap();

        // 1. DDL
        db.exec(
            "CREATE TABLE products (id INT PRIMARY KEY, name TEXT, details JSONB);",
            &[],
        )
        .unwrap();

        // 2. DML with params! macro
        db.exec(
            "INSERT INTO products (id, name, details) VALUES ($1, $2, $3);",
            &params![
                1,
                "Laptop",
                r#"{"brand": "Apple", "specs": {"cpu": "M3", "ram": 16}, "tags": ["tech", "work"], "in_stock": true}"#
            ],
        )
        .unwrap();

        // 3. Query as typed struct
        let items: Vec<ProductRow> = db.query_as("SELECT * FROM products WHERE id = $1;", &params![1]).unwrap();
        assert_eq!(items.len(), 1);
        assert_eq!(items[0].name, "Laptop");
        assert_eq!(items[0].details["brand"], "Apple");

        // 4. JSON Query
        let json_str = db.query_json("SELECT name, details->>'brand' AS brand FROM products;", &[]).unwrap();
        assert!(json_str.contains("Apple"));

        // 5. JSONB operators in Rust
        let match_res = db
            .query("SELECT * FROM products WHERE details @> '{\"brand\": \"Apple\"}';", &[])
            .unwrap();
        assert_eq!(match_res.rows.len(), 1);

        let key_exists = db
            .query("SELECT * FROM products WHERE details ? 'in_stock';", &[])
            .unwrap();
        assert_eq!(key_exists.rows.len(), 1);
    }
}
