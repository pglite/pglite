import { PGLite } from "@pglite/core";
import { NodeFSAdapter } from "@pglite/core/node-fs";

// Initialize the engine with a local file path
const db = new PGLite("app.db", {
  adapter: new NodeFSAdapter(),
});

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