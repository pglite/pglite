import { readFileSync } from "fs";
import { resolve } from "path";
import init, { initSync, PGliteWasm } from "../dist/wasm/pglite_rs.js";

async function testWasm() {
  console.log("🧪 Testing PGlite WebAssembly module...");

  // Load WASM buffer for Node/Bun
  const wasmPath = resolve(import.meta.dir, "../dist/wasm/pglite_rs_bg.wasm");
  const wasmBytes = readFileSync(wasmPath);

  // Initialize WASM
  initSync({ module: wasmBytes });

  // Instantiate DB in memory
  const db = new PGliteWasm(":memory:");

  // 1. Create table
  const createRes = db.exec(`
    CREATE TABLE users (
      id INT PRIMARY KEY,
      name TEXT,
      score FLOAT,
      details JSONB
    );
  `, null);
  console.log("Create table result:", createRes);

  // 2. Insert with parameters
  db.exec("INSERT INTO users VALUES ($1, $2, $3, $4);", [
    1,
    "Alice",
    99.5,
    JSON.stringify({ role: "admin", skills: ["Rust", "WASM", "Postgres"] }),
  ]);

  db.exec("INSERT INTO users VALUES ($1, $2, $3, $4);", [
    2,
    "Bob",
    88.0,
    JSON.stringify({ role: "developer", skills: ["TypeScript", "React"] }),
  ]);

  // 3. Query records
  const queryRows = db.query(
    "SELECT id, name, score, details->>'role' AS role, details->'skills' AS skills FROM users ORDER BY id ASC;",
    null
  );
  console.log("Query Rows:", JSON.stringify(queryRows, null, 2));

  // 4. Query JSON directly
  const jsonStr = db.query_json("SELECT count(*) as total_users FROM users;", null);
  console.log("Query JSON string:", jsonStr);

  // 5. Query with WHERE clause & JSON operators
  const adminQuery = db.query("SELECT name FROM users WHERE details @> '{\"role\": \"admin\"}';", null);
  console.log("Admin query result:", adminQuery);

  console.log("🎉 All WASM operations succeeded!");
}

testWasm().catch((err) => {
  console.error("❌ WASM test failed:", err);
  process.exit(1);
});
