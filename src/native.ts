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

  public async exec<T = any>(sql: string, params?: any, dbName?: string): Promise<T> {
    if (this.nativeInstance) {
      try {
        let p = Array.isArray(params) ? params : undefined;
        let db = typeof params === "string" ? params : dbName;
        return this.nativeInstance.exec(sql, p, db) as T;
      } catch (err) {
        // Fallback to JS if query not yet supported in fast path
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
        return this.nativeInstance.exec2(sql, p, db) as QueryResult<T>;
      } catch (err) {
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
        return this.nativeInstance.query(sql, p, db) as T[];
      } catch (err) {
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
        return this.nativeInstance.query2(sql, p, db) as QueryResult<T>;
      } catch (err) {
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
