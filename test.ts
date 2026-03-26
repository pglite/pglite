import { PGLite } from "./src/index";
export const SQLS = [];
import { NodeFSAdapter } from "@pglite/core/node-fs";
import * as fs from "fs";

// remove existing database file if it exists
if (fs.existsSync("app.db")) {
    fs.unlinkSync("app.db");
}
const db = new PGLite("app.db", { adapter: new NodeFSAdapter() });

async function runMigrations() {
    for (const migration of SQLS) {
        try {
            await db.query(migration);
            console.log("Migration executed successfully.");
        } catch (error) {
            console.log("======================================");
            console.log(migration);
            console.log("======================================");
            console.error(error);
        }
    }
}

runMigrations().then(() => {
    console.log("All migrations completed.");
}).catch((error) => {
    console.error("Error running migrations:", error);
});