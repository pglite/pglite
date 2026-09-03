import { LitePostgres as JSPostgres, QueryResult } from "./database";
import { getNativeBinding, isNativeAvailable } from "./native-loader";

export class PGLiteNative {
  private nativeInstance: any = null;
  private jsFallback: JSPostgres | null = null;
  private filepath: string;
  private options: any;

  constructor(filepath: string, options: any = {}) {
    this.filepath = filepath;
    this.options = options;
    const binding = getNativeBinding();
    if (binding && binding.LitePostgresNative) {
      try {
        this.nativeInstance = new binding.LitePostgresNative(filepath);
      } catch (err) {
        console.warn("[PGLiteNative] Failed to initialize native engine, falling back to JS:", err);
      }
    }

    // Initialize JS fallback in case native is unavailable or query is unsupported
    if (!this.nativeInstance || options.forceJs) {
      this.getJsEngine();
    }
  }

  private hydratedTables = new Set<string>();

  private async tryHydrateTable(tableName: string, dbName?: string): Promise<boolean> {
    const key = `${dbName || "public"}.${tableName.toLowerCase()}`;
    if (this.hydratedTables.has(key)) return false;
    this.hydratedTables.add(key);

    try {
      const js = this.getJsEngine();
      const res = await js.query2(`SELECT * FROM "${tableName}"`, [], dbName);
      if (!res || !res.fields || res.fields.length === 0) {
        return false;
      }

      const colDefs = res.fields.map((f: any) => {
        let typeStr = "TEXT";
        const dt = (f.data_type || "").toLowerCase();
        if (dt.includes("int") || dt.includes("serial")) {
          typeStr = f.name.toLowerCase() === "id" ? "SERIAL PRIMARY KEY" : "INT";
        } else if (dt.includes("float") || dt.includes("double") || dt.includes("numeric") || dt.includes("real")) {
          typeStr = "FLOAT";
        } else if (dt.includes("bool")) {
          typeStr = "BOOLEAN";
        } else if (dt.includes("time") || dt.includes("date")) {
          typeStr = "TIMESTAMP";
        }
        return `"${f.name}" ${typeStr}`;
      }).join(", ");

      const createSql = `CREATE TABLE "${tableName}" (${colDefs})`;
      this.nativeInstance.exec(createSql);

      if (res.rows && res.rows.length > 0) {
        for (const row of res.rows) {
          const cols = Object.keys(row).map(c => `"${c}"`).join(", ");
          const placeholders = Object.keys(row).map((_, idx) => `$${idx + 1}`).join(", ");
          const vals = Object.values(row);
          this.nativeInstance.query(`INSERT INTO "${tableName}" (${cols}) VALUES (${placeholders})`, vals);
        }
      }

      return true;
    } catch {
      return false;
    }
  }

  public async exec<T = any>(sql: string, params?: any, dbName?: string): Promise<T> {
    if (this.nativeInstance) {
      try {
        let p = Array.isArray(params) ? params : undefined;
        let db = typeof params === "string" ? params : dbName;
        const res = this.nativeInstance.exec(sql, p, db) as T;
        const upper = sql.trim().toUpperCase();
        if (!upper.startsWith("SELECT")) {
          this.getJsEngine().exec<T>(sql, params, dbName).catch(() => {});
        }
        return res;
      } catch (err: any) {
        console.warn(`[PGLite Native Fallback] exec: ${err?.message || err} -> Falling back to JS. Query: ${sql.slice(0, 100)}`);
        return this.getJsEngine().exec<T>(sql, params, dbName);
      }
    }
    return this.getJsEngine().exec<T>(sql, params, dbName);
  }

  public async exec2<T = any>(sql: string, params?: any, dbName?: string): Promise<QueryResult<T>> {
    if (this.nativeInstance) {
      try {
        let p = Array.isArray(params) ? params : undefined;
        let db = typeof params === "string" ? params : dbName;
        const res = this.nativeInstance.exec2(sql, p, db) as QueryResult<T>;
        const upper = sql.trim().toUpperCase();
        if (!upper.startsWith("SELECT")) {
          this.getJsEngine().exec2<T>(sql, params, dbName).catch(() => {});
        }
        return res;
      } catch (err: any) {
        console.warn(`[PGLite Native Fallback] exec2: ${err?.message || err} -> Falling back to JS. Query: ${sql.slice(0, 100)}`);
        return this.getJsEngine().exec2<T>(sql, params, dbName);
      }
    }
    return this.getJsEngine().exec2<T>(sql, params, dbName);
  }

  public async query<T = any>(sql: string, params?: any, dbName?: string): Promise<T[]> {
    if (this.nativeInstance) {
      try {
        let p = Array.isArray(params) ? params : undefined;
        let db = typeof params === "string" ? params : dbName;
        const res = this.nativeInstance.query(sql, p, db) as T[];
        console.log(`[PGLite Native ⚡] Executed in Rust (${res.length} rows): ${sql.slice(0, 80)}`);
        const upper = sql.trim().toUpperCase();
        if (!upper.startsWith("SELECT")) {
          this.getJsEngine().query<T>(sql, params, dbName).catch(() => {});
        }
        return res;
      } catch (err: any) {
        let currentErr = err;
        for (let attempt = 0; attempt < 3; attempt++) {
          const errMsg = currentErr?.message || String(currentErr);
          const match = errMsg.match(/Table\s+([a-zA-Z0-9_"\.-]+)\s+not found/i);
          if (match) {
            const rawTable = match[1].replace(/"/g, "");
            const tbl = rawTable.includes(".") ? rawTable.split(".").pop()! : rawTable;
            const hydrated = await this.tryHydrateTable(tbl, dbName);
            if (hydrated) {
              try {
                let p = Array.isArray(params) ? params : undefined;
                let db = typeof params === "string" ? params : dbName;
                const res = this.nativeInstance.query(sql, p, db) as T[];
                console.log(`[PGLite Native ⚡ (Auto-Hydrated)] Executed in Rust (${res.length} rows): ${sql.slice(0, 80)}`);
                const upper = sql.trim().toUpperCase();
                if (!upper.startsWith("SELECT")) {
                  this.getJsEngine().query<T>(sql, params, dbName).catch(() => {});
                }
                return res;
              } catch (retryErr) {
                currentErr = retryErr;
                continue;
              }
            }
          }
          break;
        }
        const errMsg = currentErr?.message || String(currentErr);
        console.warn(`[PGLite Native Fallback] query: ${errMsg} -> Falling back to JS. Query: ${sql.slice(0, 100)}`);
        return this.getJsEngine().query<T>(sql, params, dbName);
      }
    }
    return this.getJsEngine().query<T>(sql, params, dbName);
  }

  public async query2<T = any>(sql: string, params?: any, dbName?: string): Promise<QueryResult<T>> {
    if (this.nativeInstance) {
      try {
        let p = Array.isArray(params) ? params : undefined;
        let db = typeof params === "string" ? params : dbName;
        const res = this.nativeInstance.query2(sql, p, db) as QueryResult<T>;
        console.log(`[PGLite Native ⚡] Executed in Rust (${res.rowCount} rows): ${sql.slice(0, 80)}`);
        const upper = sql.trim().toUpperCase();
        if (!upper.startsWith("SELECT")) {
          this.getJsEngine().query2<T>(sql, params, dbName).catch(() => {});
        }
        return res;
      } catch (err: any) {
        let currentErr = err;
        for (let attempt = 0; attempt < 3; attempt++) {
          const errMsg = currentErr?.message || String(currentErr);
          const match = errMsg.match(/Table\s+([a-zA-Z0-9_"\.-]+)\s+not found/i);
          if (match) {
            const rawTable = match[1].replace(/"/g, "");
            const tbl = rawTable.includes(".") ? rawTable.split(".").pop()! : rawTable;
            const hydrated = await this.tryHydrateTable(tbl, dbName);
            if (hydrated) {
              try {
                let p = Array.isArray(params) ? params : undefined;
                let db = typeof params === "string" ? params : dbName;
                const res = this.nativeInstance.query2(sql, p, db) as QueryResult<T>;
                console.log(`[PGLite Native ⚡ (Auto-Hydrated)] Executed in Rust (${res.rowCount} rows): ${sql.slice(0, 80)}`);
                const upper = sql.trim().toUpperCase();
                if (!upper.startsWith("SELECT")) {
                  this.getJsEngine().query2<T>(sql, params, dbName).catch(() => {});
                }
                return res;
              } catch (retryErr) {
                currentErr = retryErr;
                continue;
              }
            }
          }
          break;
        }
        const errMsg = currentErr?.message || String(currentErr);
        console.warn(`[PGLite Native Fallback] query2: ${errMsg} -> Falling back to JS. Query: ${sql.slice(0, 100)}`);
        return this.getJsEngine().query2<T>(sql, params, dbName);
      }
    }
    return this.getJsEngine().query2<T>(sql, params, dbName);
  }

  public async transaction<T = any>(callback: (tx: any) => Promise<T>, dbName?: string): Promise<T> {
    return this.getJsEngine().transaction<T>(callback, dbName);
  }

  public async transaction2<T = any>(callback: (tx: any) => Promise<T>, dbName?: string): Promise<T> {
    return this.getJsEngine().transaction2<T>(callback, dbName);
  }

  public async close(): Promise<void> {
    if (this.nativeInstance) {
      try {
        this.nativeInstance.close();
      } catch {}
    }
    if (this.jsFallback) {
      await this.jsFallback.close();
    }
  }

  private getJsEngine(): JSPostgres {
    if (!this.jsFallback) {
      let opts = { ...(this.options || {}) };
      if (!opts.adapter && typeof window === "undefined" && this.filepath !== ":memory:") {
        try {
          const { NodeFSAdapter } = require("./adapters/node");
          opts.adapter = new NodeFSAdapter();
        } catch {
          try {
            const { NodeFSAdapter } = require("./adapters/node.js");
            opts.adapter = new NodeFSAdapter();
          } catch {}
        }
      }
      this.jsFallback = new JSPostgres(this.filepath, opts);
    }
    return this.jsFallback;
  }
}
