# PostgresLite 🚀

**The High-Performance Embedded PostgreSQL Engine for Bun & Node.js**

[![Bun](https://img.shields.io/badge/Bun-%23000000.svg?style=for-the-badge&logo=bun&logoColor=white)](https://bun.sh)
[![Node.js](https://img.shields.io/badge/Node.js-6DA55F?style=for-the-badge&logo=node.js&logoColor=white)](https://nodejs.org/)
[![Browser](https://img.shields.io/badge/Browser-4285F4?style=for-the-badge&logo=google-chrome&logoColor=white)](https://developer.mozilla.org/en-US/docs/Web/API/IndexedDB_API)
[![TypeScript](https://img.shields.io/badge/typescript-%23007ACC.svg?style=for-the-badge&logo=typescript&logoColor=white)](https://www.typescriptlang.org/)

**PostgresLite** is a high-performance, **in-process**, zero-dependency **embedded PostgreSQL database** engine for **Bun, Node.js, and the Browser**. It provides a PostgreSQL-compatible SQL interface with the simplicity of a local storage engine—effectively bringing the power of Postgres to the developer experience, serving as a robust **SQLite alternative**.

Unlike traditional PostgreSQL, **PostgresLite** requires **no server**, no network overhead, and zero configuration. It utilizes a custom-built storage engine designed for low-latency I/O, high concurrency, and full **ACID compliance**.

## 🚀 Key Features

-   **Cross-Runtime:** Native support for [Bun](https://bun.sh), [Node.js](https://nodejs.org), and **Modern Browsers** (via IndexedDB).
-   **Serverless:** In-process execution; no connection strings, background processes, or docker containers needed.
-   **PostgreSQL Dialect:** Supports a vast subset of the Postgres syntax including Joins, CTEs, and Window Functions.
-   **Performance:** Capable of handling **1M+ records** per table via B-Tree indexing and advanced Buffer Pool management.
-   **ACID Compliant:** Supports full transactions with `BEGIN`, `COMMIT`, and `ROLLBACK` via Write-Ahead Logging (WAL).
-   **Schema Isolation:** Multi-schema support (`public`, `pg_catalog`, `information_schema`).

## 🛠 Supported Syntax

| Category | Supported Keywords / Features |
| :--- | :--- |
| **DDL** | `CREATE/DROP TABLE`, `CREATE/DROP SCHEMA`, `ALTER TABLE` (ADD, DROP, RENAME, TYPE, DEFAULT, NOT NULL) |
| **DML** | `SELECT`, `INSERT`, `UPDATE`, `DELETE`, `ON CONFLICT` (DO NOTHING / DO UPDATE) |
| **Query Clauses** | `WHERE`, `GROUP BY`, `HAVING`, `ORDER BY` (ASC/DESC), `LIMIT`, `OFFSET`, `RETURNING` |
| **Joins** | `INNER JOIN`, `LEFT JOIN`, `LATERAL JOIN`, `CROSS JOIN` |
| **Advanced** | `WITH` (CTE), `UNION`, `INTERSECT`, `SUBQUERY` (In WHERE/FROM) |
| **Functions** | `COUNT`, `SUM`, `AVG`, `MIN`, `MAX`, `ARRAY_AGG`, `COALESCE`, `NOW`, `DATE_TRUNC`, `UPPER`, `JSON_EXTRACT` |
| **Operators** | `LIKE`, `IN`, `IS NULL`, `Regex (~, ~*, !~)`, `JSON (->, ->>, #>, @>, ?)`, `Array (&&, @>)` |
| **Window** | `ROW_NUMBER()`, `RANK()` via `OVER (PARTITION BY ... ORDER BY ...)` |

## 🏗 Optimization Technologies

PostgresLite is built with several advanced database engineering techniques to ensure high performance:

1.  **Slotted Page Layout:** Data is stored in fixed 4KB pages using a slotted-page architecture. This allows for efficient management of variable-length records (like `JSONB` or `TEXT`) and prevents page fragmentation.
2.  **Write-Ahead Logging (WAL):** Every mutation is logged to a persistent WAL file before being applied to the main database. This ensures durability and allows for automatic crash recovery.
3.  **B-Tree Indexing:** Primary keys are automatically indexed using a B-Tree, enabling $O(\log n)$ point lookups even as datasets scale into the millions.
4.  **Volcano Execution Model:** The engine uses an iterator-based processing model. Rows are "pulled" through the execution plan one by one, ensuring that complex queries (like `SELECT *`) use a constant and minimal memory footprint.
5.  **External Merge Sort:** For large `ORDER BY` operations that exceed available RAM, the engine automatically spills to disk and performs a multi-way merge sort to maintain memory safety.
6.  **LRU Buffer Pool:** A sophisticated Least-Recently-Used (LRU) cache minimizes physical disk I/O by keeping frequently accessed pages in memory.
7.  **Predicate Pushdown:** The execution engine optimizes filters by pushing them down to the storage layer, utilizing indices for $O(1)$ lookups whenever possible.

## 📦 Installation

# Using Bun
bun add @pglite/core

# Using NPM
npm installsh
bun add @pglite/core
```

## 🛠 Usage

import { PGLite } from "@pglite/core";

// For Node.js/Bun:
import { NodeFSAdapter } from "@pglite/core/node-fs";
const db = new PGLite("app.db", { adapter: new NodeFSAdapter() });

// For Browser:
import { BrowserFSAdapter } from "@pglite/core/browser";
const db = new PGLite("app.db", { adapter: new BrowserFSAdapter() });

// 1. DDL & Data Mutation
await db.exec(`
  CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    metadata JSONB
  )
`);

await db.exec(`
  CREATE TABLE posts (
    id SERIAL PRIMARY KEY,
    user_id INTEGER REFERENCES users(id),
    title TEXT NOT NULL,
    content TEXT
  )
`);

// 2. Parameterized Queries (SQL Injection Protected)
await db.exec(
  "INSERT INTO users (name, metadata) VALUES ($1, $2)", 
  ["Alice", { role: "admin", active: true }]
);

// 3. Complex Querying (Joins, Aggregates, Grouping)
const results = await db.query(`
  SELECT u.name, COUNT(p.id) as post_count
  FROM users u
  LEFT JOIN posts p ON u.id = p.user_id
  WHERE u.name LIKE $1
  GROUP BY u.name
  ORDER BY post_count DESC
`, ["Al%"]);

console.table(results);
```

## 📉 Scalability & Performance Benchmarks

PostgresLite is designed for heavy lifting in local environments:

*   **Point Lookups:** $O(1)$ to $O(\log n)$ via Primary Key B-Tree Index.
*   **Sequential Scans:** High-throughput streaming via the Pager's Buffer Pool.
*   **Memory Efficiency:** The Volcano-style iterator ensures that running a `SELECT *` on a 1,000,000 row table does not result in an `OutOfMemory` error.
*   **Large Sorts:** Automatically triggers disk-backed sorting when result sets exceed the configurable buffer threshold.

## 🔧 Engineering Deep-Dive

### The Slotted Page Layout
PostgresLite does not store rows as raw strings. Each 4KB page contains a header, a slot array pointing to record offsets, and the data area. This prevents fragmentation and allows for variable-length records (like `JSONB` or `TEXT`) to be updated in place efficiently.

### Write-Ahead Logging (WAL)
Every mutation is first appended to a `.wal` file. In the event of a process crash, the engine automatically replays the WAL on the next initialization, ensuring your database state remains consistent and corruption-free.

## 🤝 Contributing

We welcome contributions to the core engine, specifically in the following areas:
-   Expansion of the SQL Parser for more complex PostgreSQL dialects.
-   Implementation of Secondary Indexes.
-   Full-text search (TSVECTOR) integration.

## 📄 License

MIT © Senior Systems Programming Team.
