import { LitePostgres as JSPostgres, QueryResult } from "./database";
import { getNativeBinding, isNativeAvailable } from "./native-loader";

function stripSqlComments(sql: string): string {
  let result = "";
  let inLineComment = false;
  let inBlockComment = false;
  let inSingleQuote = false;
  let inDoubleQuote = false;
  const len = sql.length;
  let i = 0;

  while (i < len) {
    const ch = sql[i];
    const nextCh = i + 1 < len ? sql[i + 1] : "";

    if (inLineComment) {
      if (ch === "\n" || ch === "\r") {
        inLineComment = false;
        result += ch;
      }
      i++;
    } else if (inBlockComment) {
      if (ch === "*" && nextCh === "/") {
        inBlockComment = false;
        i += 2;
      } else {
        i++;
      }
    } else if (inSingleQuote) {
      result += ch;
      if (ch === "'") {
        if (nextCh === "'") {
          result += "'";
          i += 2;
          continue;
        }
        inSingleQuote = false;
      }
      i++;
    } else if (inDoubleQuote) {
      result += ch;
      if (ch === '"') {
        inDoubleQuote = false;
      }
      i++;
    } else {
      if (ch === "-" && nextCh === "-") {
        inLineComment = true;
        i += 2;
      } else if (ch === "/" && nextCh === "*") {
        inBlockComment = true;
        i += 2;
      } else {
        if (ch === "'") inSingleQuote = true;
        else if (ch === '"') inDoubleQuote = true;
        result += ch;
        i++;
      }
    }
  }
  return result;
}

function hasSubqueryInWhere(sql: string): boolean {
  let depth = 0;
  let inWhereOrHaving = false;
  const len = sql.length;

  for (let i = 0; i < len; i++) {
    const ch = sql[i];
    if (ch === "(") {
      if (depth === 0 && inWhereOrHaving) {
        const rest = sql.slice(i + 1).trimStart();
        if (/^SELECT\b/i.test(rest)) {
          return true;
        }
      }
      depth++;
    } else if (ch === ")") {
      if (depth > 0) depth--;
    } else if (depth === 0) {
      const rest = sql.slice(i);
      if (/^\b(WHERE|HAVING)\b/i.test(rest)) {
        inWhereOrHaving = true;
      }
    }
  }
  return false;
}

function isComplexQuery(rawSql: string): boolean {
  const sql = stripSqlComments(rawSql);

  if (hasSubqueryInWhere(sql)) {
    return true;
  }

  return (
    /^\s*WITH\b/i.test(sql) ||
    /\b(UNION|INTERSECT|EXCEPT)\b/i.test(sql) ||
    /\bOVER\s*\(/i.test(sql) ||
    /\bGROUP\s+BY\b/i.test(sql) ||
    /\bHAVING\b/i.test(sql) ||
    /\bON\s+CONFLICT\b/i.test(sql) ||
    /\bTRUNCATE\b/i.test(sql) ||
    /\b(JSONB_AGG|JSON_AGG|ARRAY_AGG|JSON_BUILD_OBJECT|JSONB_BUILD_OBJECT|JSON_BUILD_ARRAY|JSONB_BUILD_ARRAY|JSONB_SET|JSONB_EXTRACT|JSON_EXTRACT|STRING_AGG|DATE_PART|DATE_TRUNC|GEN_RANDOM_UUID|UUID_GENERATE_V4)\b/i.test(sql) ||
    /\[\]/.test(sql) ||
    /\bARRAY\[/i.test(sql) ||
    /\bCASE\b/i.test(sql) ||
    /\bCAST\s*\(/i.test(sql) ||
    /::[a-zA-Z_]/.test(sql) ||
    /^\s*(BEGIN|COMMIT|ROLLBACK|SAVEPOINT)\b/i.test(sql) ||
    /SET\s+[^=]+=\s*[^,;]+[\+\-\*\/]/i.test(sql) ||
    /\b(CONCAT|CONCAT_WS|LENGTH|TRIM|LTRIM|RTRIM|REPLACE|SUBSTRING|LEFT|RIGHT|LPAD|RPAD|INITCAP|REVERSE|STRPOS|SPLIT_PART|ROUND|CEIL|CEILING|FLOOR|ABS|POWER|SQRT|MOD|SIGN|DATE_PART|DATE_TRUNC)\s*\(/i.test(sql)
  );
}

export class PGLiteNative {
  private nativeInstance: any = null;
  private jsFallback: JSPostgres | null = null;
  private filepath: string;
  private options: any;

  constructor(filepath: string, options: any = {}) {
    this.filepath = filepath;
    this.options = options;

    const disableNative = Boolean(
      options.forceJs ||
      options.native === false ||
      options.useNative === false ||
      options.mode === "js" ||
      options.mode === "node" ||
      process.env.PGLITE_NATIVE === "0" ||
      process.env.PGLITE_NATIVE === "false" ||
      process.env.PGLITE_USE_NATIVE === "0" ||
      process.env.PGLITE_USE_NATIVE === "false" ||
      process.env.PGLITE_FORCE_JS === "1" ||
      process.env.PGLITE_FORCE_JS === "true"
    );

    if (!disableNative) {
      const binding = getNativeBinding();
      if (binding && binding.LitePostgresNative) {
        try {
          this.nativeInstance = new binding.LitePostgresNative(filepath);
        } catch (err) {
          console.warn("[PGLiteNative] Failed to initialize native engine, falling back to JS:", err);
        }
      }
    }

    // Initialize JS fallback in case native is unavailable or query is unsupported
    if (!this.nativeInstance || disableNative) {
      this.getJsEngine();
    }
  }

  public get storage() {
    return (this.getJsEngine() as any).storage;
  }

  public get tables() {
    return (this.getJsEngine() as any).tables;
  }

  private hydratedTables = new Set<string>();
  private knownTables = new Set<string>();
  private writeQueue: Array<{ sql: string; params?: any[]; dbName?: string; isExec?: boolean }> = [];
  private flushTimer: any = null;
  private isFlushing = false;
  private readonly MAX_BATCH_SIZE = 50;
  private readonly FLUSH_INTERVAL_MS = 50;

  private recordKnownTable(sql: string) {
    const match = sql.match(/CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?([a-zA-Z0-9_"\.-]+)/i);
    if (match) {
      const raw = match[1].replace(/"/g, "");
      const tbl = raw.includes(".") ? raw.split(".").pop()! : raw;
      this.knownTables.add(tbl.toLowerCase());
    }
  }

  private queueBackgroundWrite(sql: string, params?: any, dbName?: string, isExec: boolean = false) {
    this.recordKnownTable(sql);
    let p = Array.isArray(params) ? params : undefined;
    let db = typeof params === "string" ? params : dbName;
    this.writeQueue.push({ sql, params: p, dbName: db, isExec });

    if (this.writeQueue.length >= this.MAX_BATCH_SIZE) {
      this.flushWriteQueue().catch(() => {});
    } else if (!this.flushTimer) {
      this.flushTimer = setTimeout(() => {
        this.flushTimer = null;
        this.flushWriteQueue().catch(() => {});
      }, this.FLUSH_INTERVAL_MS);
    }
  }

  public async flushWriteQueue(): Promise<void> {
    if (this.flushTimer) {
      clearTimeout(this.flushTimer);
      this.flushTimer = null;
    }
    if (this.writeQueue.length === 0 || this.isFlushing) {
      return;
    }

    const tasks = this.writeQueue.splice(0, this.writeQueue.length);
    this.isFlushing = true;

    try {
      const js = this.getJsEngine();
      for (const task of tasks) {
        try {
          if (task.isExec) {
            await js.exec(task.sql, task.params, task.dbName);
          } else {
            await js.query2(task.sql, task.params, task.dbName);
          }
        } catch {
          // Ignore write-through errors in background flush
        }
      }
    } finally {
      this.isFlushing = false;
      if (this.writeQueue.length > 0 && !this.flushTimer) {
        this.flushTimer = setTimeout(() => {
          this.flushTimer = null;
          this.flushWriteQueue().catch(() => {});
        }, this.FLUSH_INTERVAL_MS);
      }
    }
  }

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

      let schemaCols: Record<string, string> = {};
      try {
        const tblMeta = (await (js as any).storage?.getTableAsync?.(tableName)) || 
                        (await (js as any).storage?.getTableAsync?.(`public.${tableName}`)) ||
                        (js as any).storage?.tables?.get(tableName) || 
                        (js as any).storage?.tables?.get(tableName.toLowerCase());
        if (tblMeta && tblMeta.columns) {
          for (const col of tblMeta.columns) {
            schemaCols[col.name.toLowerCase()] = col.dataType || col.data_type || "TEXT";
          }
        }
      } catch {}

      const colDefs = res.fields.map((f: any) => {
        let typeStr = "TEXT";
        const colNameLower = (f.name || "").toLowerCase();
        const dt = (schemaCols[colNameLower] || f.data_type || "").toLowerCase();

        if (colNameLower === "id" || colNameLower === "_id" || dt.includes("serial")) {
          typeStr = "SERIAL PRIMARY KEY";
        } else if (dt.includes("int")) {
          typeStr = "INT";
        } else if (dt.includes("float") || dt.includes("double") || dt.includes("numeric") || dt.includes("real")) {
          typeStr = "FLOAT";
        } else if (dt.includes("bool")) {
          typeStr = "BOOLEAN";
        } else if (dt.includes("time") || dt.includes("date")) {
          typeStr = "TIMESTAMP";
        } else if (colNameLower.endsWith("_id")) {
          typeStr = "INT";
        }
        return `"${f.name}" ${typeStr}`;
      }).join(", ");

      const createSql = `CREATE TABLE "${tableName}" (${colDefs})`;
      this.nativeInstance.exec(createSql);

      if (res.rows && res.rows.length > 0) {
        const colNames = res.fields.map((f: any) => f.name);
        const colsList = colNames.map(c => `"${c}"`).join(", ");
        const CHUNK_SIZE = 500;

        for (let i = 0; i < res.rows.length; i += CHUNK_SIZE) {
          const chunk = res.rows.slice(i, i + CHUNK_SIZE);
          const valueClauses: string[] = [];
          const allVals: any[] = [];

          for (let r = 0; r < chunk.length; r++) {
            const row = chunk[r];
            const placeholders: string[] = [];
            for (let c = 0; c < colNames.length; c++) {
              allVals.push(row[colNames[c]]);
              placeholders.push(`$${allVals.length}`);
            }
            valueClauses.push(`(${placeholders.join(", ")})`);
          }

          this.nativeInstance.query(`INSERT INTO "${tableName}" (${colsList}) VALUES ${valueClauses.join(", ")}`, allVals);
        }
      }

      return true;
    } catch {
      return false;
    }
  }

  private handleFallbackWrite(sql: string, dbName?: string) {
    const match = sql.trim().match(/(?:INSERT\s+INTO|UPDATE|DELETE\s+FROM|DROP\s+TABLE|ALTER\s+TABLE|TRUNCATE\s+TABLE|TRUNCATE)\s+([a-zA-Z0-9_"\.-]+)/i);
    if (match) {
      const rawTable = match[1].replace(/"/g, "");
      const tbl = rawTable.includes(".") ? rawTable.split(".").pop()! : rawTable;
      const key = `${dbName || "public"}.${tbl.toLowerCase()}`;
      this.hydratedTables.delete(key);
      this.knownTables.delete(tbl.toLowerCase());
      try {
        this.nativeInstance?.exec(`DROP TABLE IF EXISTS "${tbl}"`);
      } catch {}
    }
  }

  public async exec<T = any>(sql: string, params?: any, dbName?: string): Promise<T> {
    const stripped = stripSqlComments(sql).trim();
    if (stripped.includes(";")) {
      const stmts = stripped.split(";").map(s => s.trim()).filter(s => s.length > 0);
      if (stmts.length > 1) {
        let lastRes: any = null;
        for (const stmt of stmts) {
          lastRes = await this.exec(stmt, params, dbName);
        }
        return lastRes;
      }
    }

    const inTx = Boolean((this.jsFallback as any)?.storage?.inTransaction);
    if (inTx || isComplexQuery(sql)) {
      if (this.writeQueue.length > 0) {
        await this.flushWriteQueue();
      }
      this.handleFallbackWrite(sql, dbName);
      return this.getJsEngine().exec<T>(sql, params, dbName);
    }

    this.recordKnownTable(sql);
    if (this.nativeInstance) {
      try {
        let p = Array.isArray(params) ? params : undefined;
        let db = typeof params === "string" ? params : dbName;
        const res = this.nativeInstance.exec(sql, p, db) as T;
        if (res && typeof res === "object" && (res as any).success === undefined) {
          (res as any).success = true;
        }
        const upper = sql.trim().toUpperCase();
        if (upper.startsWith("CREATE") || upper.startsWith("DROP") || upper.startsWith("ALTER")) {
          await this.flushWriteQueue();
          try {
            await this.getJsEngine().exec<T>(sql, params, dbName);
          } catch {}
        } else if (!upper.startsWith("SELECT")) {
          this.queueBackgroundWrite(sql, params, dbName, true);
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
                const res = this.nativeInstance.exec(sql, p, db) as T;
                const upper = sql.trim().toUpperCase();
                if (!upper.startsWith("SELECT")) {
                  this.queueBackgroundWrite(sql, params, dbName, true);
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
        console.warn(`[PGLite Native Fallback] exec: ${currentErr?.message || currentErr} -> Falling back to JS. Query: ${sql.slice(0, 100)}`);
        await this.flushWriteQueue();
        this.handleFallbackWrite(sql, dbName);
        return this.getJsEngine().exec<T>(sql, params, dbName);
      }
    }
    await this.flushWriteQueue();
    this.handleFallbackWrite(sql, dbName);
    return this.getJsEngine().exec<T>(sql, params, dbName);
  }

  public async exec2<T = any>(sql: string, params?: any, dbName?: string): Promise<QueryResult<T>> {
    const stripped = stripSqlComments(sql).trim();
    if (stripped.includes(";")) {
      const stmts = stripped.split(";").map(s => s.trim()).filter(s => s.length > 0);
      if (stmts.length > 1) {
        let lastRes: any = null;
        for (const stmt of stmts) {
          lastRes = await this.exec2(stmt, params, dbName);
        }
        return lastRes;
      }
    }

    const inTx = Boolean((this.jsFallback as any)?.storage?.inTransaction);
    if (inTx || isComplexQuery(sql)) {
      if (this.writeQueue.length > 0) {
        await this.flushWriteQueue();
      }
      this.handleFallbackWrite(sql, dbName);
      return this.getJsEngine().exec2<T>(sql, params, dbName);
    }

    if (this.nativeInstance) {
      try {
        let p = Array.isArray(params) ? params : undefined;
        let db = typeof params === "string" ? params : dbName;
        const res = this.nativeInstance.exec2(sql, p, db) as QueryResult<T>;
        const upper = sql.trim().toUpperCase();
        if (upper.startsWith("CREATE") || upper.startsWith("DROP") || upper.startsWith("ALTER")) {
          await this.flushWriteQueue();
          try {
            await this.getJsEngine().exec2<T>(sql, params, dbName);
          } catch {}
        } else if (!upper.startsWith("SELECT")) {
          this.queueBackgroundWrite(sql, params, dbName, true);
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
                const res = this.nativeInstance.exec2(sql, p, db) as QueryResult<T>;
                const upper = sql.trim().toUpperCase();
                if (!upper.startsWith("SELECT")) {
                  this.queueBackgroundWrite(sql, params, dbName, true);
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
        console.warn(`[PGLite Native Fallback] exec2: ${currentErr?.message || currentErr} -> Falling back to JS. Query: ${sql.slice(0, 100)}`);
        await this.flushWriteQueue();
        this.handleFallbackWrite(sql, dbName);
        return this.getJsEngine().exec2<T>(sql, params, dbName);
      }
    }
    await this.flushWriteQueue();
    this.handleFallbackWrite(sql, dbName);
    return this.getJsEngine().exec2<T>(sql, params, dbName);
  }

  private runNativeQuery<T = any>(sql: string, p?: any[], db?: string): T[] {
    const fn = this.nativeInstance.queryJson || this.nativeInstance.query_json;
    if (typeof fn === "function") {
      const raw = fn.call(this.nativeInstance, sql, p, db);
      return JSON.parse(raw) as T[];
    }
    return this.nativeInstance.query(sql, p, db) as T[];
  }

  private runNativeQuery2<T = any>(sql: string, p?: any[], db?: string): QueryResult<T> {
    const fn = this.nativeInstance.query2Json || this.nativeInstance.query2_json;
    if (typeof fn === "function") {
      const raw = fn.call(this.nativeInstance, sql, p, db);
      return JSON.parse(raw) as QueryResult<T>;
    }
    return this.nativeInstance.query2(sql, p, db) as QueryResult<T>;
  }

  public async query<T = any>(sql: string, params?: any, dbName?: string): Promise<T[]> {
    const inTx = Boolean((this.jsFallback as any)?.storage?.inTransaction);
    if (inTx || isComplexQuery(sql)) {
      if (this.writeQueue.length > 0) {
        await this.flushWriteQueue();
      }
      this.handleFallbackWrite(sql, dbName);
      return this.getJsEngine().query<T>(sql, params, dbName);
    }

    this.recordKnownTable(sql);
    if (this.nativeInstance) {
      try {
        let p = Array.isArray(params) ? params : undefined;
        let db = typeof params === "string" ? params : dbName;
        const res = this.runNativeQuery<T>(sql, p, db);
        if (this.options?.debug || process.env.PGLITE_DEBUG) {
          console.log(`[PGLite Native ⚡] Executed in Rust (${res.length} rows): ${sql.slice(0, 80)}`);
        }
        const upper = sql.trim().toUpperCase();
        if (!upper.startsWith("SELECT")) {
          this.queueBackgroundWrite(sql, params, dbName, false);
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
                const res = this.runNativeQuery<T>(sql, p, db);
                if (this.options?.debug || process.env.PGLITE_DEBUG) {
                  console.log(`[PGLite Native ⚡ (Auto-Hydrated)] Executed in Rust (${res.length} rows): ${sql.slice(0, 80)}`);
                }
                const upper = sql.trim().toUpperCase();
                if (!upper.startsWith("SELECT")) {
                  this.queueBackgroundWrite(sql, params, dbName, false);
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
        console.warn(`[PGLite Native Fallback] query: ${currentErr?.message || currentErr} -> Falling back to JS. Query: ${sql.slice(0, 100)}`);
        await this.flushWriteQueue();
        this.handleFallbackWrite(sql, dbName);
        return this.getJsEngine().query<T>(sql, params, dbName);
      }
    }
    await this.flushWriteQueue();
    this.handleFallbackWrite(sql, dbName);
    return this.getJsEngine().query<T>(sql, params, dbName);
  }

  public async query2<T = any>(sql: string, params?: any, dbName?: string): Promise<QueryResult<T>> {
    const inTx = Boolean((this.jsFallback as any)?.storage?.inTransaction);
    if (inTx || isComplexQuery(sql)) {
      if (this.writeQueue.length > 0) {
        await this.flushWriteQueue();
      }
      this.handleFallbackWrite(sql, dbName);
      return this.getJsEngine().query2<T>(sql, params, dbName);
    }

    this.recordKnownTable(sql);
    if (this.nativeInstance) {
      try {
        let p = Array.isArray(params) ? params : undefined;
        let db = typeof params === "string" ? params : dbName;
        const res = this.runNativeQuery2<T>(sql, p, db);
        if (this.options?.debug || process.env.PGLITE_DEBUG) {
          console.log(`[PGLite Native ⚡] Executed in Rust (${res.rowCount} rows): ${sql.slice(0, 80)}`);
        }
        const upper = sql.trim().toUpperCase();
        if (!upper.startsWith("SELECT")) {
          this.queueBackgroundWrite(sql, params, dbName, false);
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
                const res = this.runNativeQuery2<T>(sql, p, db);
                if (this.options?.debug || process.env.PGLITE_DEBUG) {
                  console.log(`[PGLite Native ⚡ (Auto-Hydrated)] Executed in Rust (${res.rowCount} rows): ${sql.slice(0, 80)}`);
                }
                const upper = sql.trim().toUpperCase();
                if (!upper.startsWith("SELECT")) {
                  this.queueBackgroundWrite(sql, params, dbName, false);
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
        await this.flushWriteQueue();
        this.handleFallbackWrite(sql, dbName);
        return this.getJsEngine().query2<T>(sql, params, dbName);
      }
    }
    await this.flushWriteQueue();
    this.handleFallbackWrite(sql, dbName);
    return this.getJsEngine().query2<T>(sql, params, dbName);
  }

  public async transaction<T = any>(callback: (tx: any) => Promise<T>, dbName?: string): Promise<T> {
    await this.flushWriteQueue();
    const result = await this.getJsEngine().transaction<T>(callback, dbName);
    for (const tbl of this.knownTables) {
      try {
        this.nativeInstance?.exec(`DROP TABLE IF EXISTS "${tbl}"`);
      } catch {}
    }
    for (const key of this.hydratedTables) {
      const parts = key.split(".");
      const tbl = parts[1] || parts[0];
      try {
        this.nativeInstance?.exec(`DROP TABLE IF EXISTS "${tbl}"`);
      } catch {}
    }
    this.hydratedTables.clear();
    this.knownTables.clear();
    return result;
  }

  public async transaction2<T = any>(callback: (tx: any) => Promise<T>, dbName?: string): Promise<T> {
    await this.flushWriteQueue();
    const result = await this.getJsEngine().transaction2<T>(callback, dbName);
    for (const tbl of this.knownTables) {
      try {
        this.nativeInstance?.exec(`DROP TABLE IF EXISTS "${tbl}"`);
      } catch {}
    }
    for (const key of this.hydratedTables) {
      const parts = key.split(".");
      const tbl = parts[1] || parts[0];
      try {
        this.nativeInstance?.exec(`DROP TABLE IF EXISTS "${tbl}"`);
      } catch {}
    }
    this.hydratedTables.clear();
    this.knownTables.clear();
    return result;
  }

  public async close(): Promise<void> {
    await this.flushWriteQueue();
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
