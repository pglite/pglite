import { PGLite } from "@pglite/core";
import { NodeFSAdapter } from "@pglite/core/node-fs";

// Initialize the engine with a local file path
const db = new PGLite("app.db", {
  adapter: new NodeFSAdapter(),
});

// 1. DDL & Data Mutation
await db.exec(`
  CREATE TABLE IF NOT EXISTS "user" (
      "id" TEXT PRIMARY KEY,
      "email" TEXT,
      "fullName" TEXT,
      "role" TEXT
    )
`);

await db.exec(`
  CREATE TABLE IF NOT EXISTS "product" (
      "id" TEXT PRIMARY KEY,
      "title" TEXT,
      "price" NUMERIC,
      "inStock" BOOLEAN
    )
`);

// 2. Parameterized Queries (SQL Injection Protected)
await db.exec(`
  CREATE TABLE IF NOT EXISTS "order" (
      "id" TEXT PRIMARY KEY,
      "productId" TEXT,
      "userId" TEXT,
      "status" TEXT
    )
  `);

// insert sample data
await db.exec(`
  INSERT INTO "user" (id, email, fullName, role) VALUES
  ('1', 'alice@example.com', 'Alice', 'admin'),
  ('2', 'bob@example.com', 'Bob', 'user')
`);

await db.exec(`
  INSERT INTO "product" (id, title, price, inStock) VALUES
  ('1', 'Product 1', 9.99, true),
  ('2', 'Product 2', 19.99, false)
`);

await db.exec(`
  INSERT INTO "order" (id, productId, userId, status) VALUES
  ('1', '1', '1', 'shipped'),
  ('2', '2', '2', 'pending')
`);

const allUsers = await db.query(`SELECT * FROM "user"`);
const allProducts = await db.query(`SELECT * FROM "product"`);
const allOrders = await db.query(`SELECT * FROM "order"`);
console.table(allUsers);
console.table(allProducts);
console.table(allOrders);
