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

function splitStatements(sql: string): string[] {
  const stripped = stripSqlComments(sql).trim();
  const stmts: string[] = [];
  let current = "";
  let inSingleQuote = false;
  let inDoubleQuote = false;
  for (let i = 0; i < stripped.length; i++) {
    const ch = stripped[i];
    if (ch === "'" && !inDoubleQuote) {
      if (inSingleQuote && stripped[i + 1] === "'") {
        current += "''";
        i++;
        continue;
      }
      inSingleQuote = !inSingleQuote;
      current += ch;
    } else if (ch === '"' && !inSingleQuote) {
      inDoubleQuote = !inDoubleQuote;
      current += ch;
    } else if (ch === ";" && !inSingleQuote && !inDoubleQuote) {
      const trimmed = current.trim();
      if (trimmed) stmts.push(trimmed);
      current = "";
    } else {
      current += ch;
    }
  }
  const last = current.trim();
  if (last) stmts.push(last);
  return stmts.length > 0 ? stmts : [stripped];
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
  private allowFallback: boolean;

  private hydratedTables = new Set<string>();
  private knownTables = new Set<string>();
  private writeQueue: Array<{ sql: string; params?: any[]; dbName?: string; isExec?: boolean }> = [];
  private flushTimer: any = null;
  private isFlushing = false;
  private readonly MAX_BATCH_SIZE = 50;
  private readonly FLUSH_INTERVAL_MS = 50;

  constructor(filepath: string, options: any = {}) {
    this.filepath = filepath;
    this.options = options;
    // By default, fallback is disabled for absolute data integrity.
    // Set fallback: true or autoFallback: true to enable safe, non-destructive fallback.
    this.allowFallback = Boolean(options.fallback || options.autoFallback);

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
          console.warn("[PGLiteNative] Failed to initialize native engine:", err);
        }
      }
    }

    if (!this.nativeInstance) {
      this.getJsEngine();
    }
  }

  public get storage() {
    return (this.getJsEngine() as any).storage;
  }

  public get tables() {
    return (this.getJsEngine() as any).tables;
  }

  private recordKnownTable(sql: string) {
    const match = sql.match(/CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?([a-zA-Z0-9_"\.-]+)/i);
    if (match) {
      const raw = match[1].replace(/"/g, "");
      const tbl = raw.includes(".") ? raw.split(".").pop()! : raw;
      this.knownTables.add(tbl.toLowerCase());
    }
  }

  private queueBackgroundWrite(sql: string, params?: any, dbName?: string, isExec: boolean = false) {
    if (!this.allowFallback) return;
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
        } catch {}
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

  private legacyMigrationChecked = false;

  private async ensureLegacyMigrated(dbName?: string): Promise<void> {
    if (this.legacyMigrationChecked || !this.nativeInstance || this.filepath === ":memory:") return;
    this.legacyMigrationChecked = true;

    try {
      const fs = require("fs");
      if (!fs.existsSync(this.filepath)) return;
      const stat = fs.statSync(this.filepath);
      if (stat.size === 0) return;

      const rwalPath = this.filepath + ".rwal";
      if (fs.existsSync(rwalPath)) {
        const rwalStat = fs.statSync(rwalPath);
        if (rwalStat.size > 4) {
          return;
        }
      }

      const legacyWal = this.filepath + ".wal";
      if (fs.existsSync(legacyWal)) {
        try {
          const buf = Buffer.alloc(4);
          const fd = fs.openSync(legacyWal, "r");
          fs.readSync(fd, buf, 0, 4, 0);
          fs.closeSync(fd);
          if (buf.toString("utf8") === "PGL1") {
            return;
          }
        } catch {}
      }

      let adapter: any;
      try {
        const { NodeFSAdapter } = require("./adapters/node");
        adapter = new NodeFSAdapter();
      } catch {
        try {
          const { NodeFSAdapter } = require("./adapters/node.js");
          adapter = new NodeFSAdapter();
        } catch {}
      }

      const legacyDb = new JSPostgres(this.filepath, {
        adapter,
        database: dbName || this.options?.database,
      });

      const catSql = `
        SELECT
          c.relname AS name,
          obj_description(c.oid, 'pg_class') AS comment,
          COALESCE(
            json_agg(
              json_build_object(
                'name', a.attname,
                'type', format_type(a.atttypid, a.atttypmod),
                'nullable', NOT a.attnotnull,
                'default', pg_get_expr(d.adbin, d.adrelid),
                'comment', col_description(c.oid, a.attnum)
              ) ORDER BY a.attnum
            ) FILTER (WHERE a.attnum > 0),
            '[]'::json
          ) AS columns
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        LEFT JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum > 0 AND NOT a.attisdropped
        LEFT JOIN pg_attrdef d ON (a.attrelid = d.adrelid AND a.attnum = d.adnum)
        WHERE n.nspname = 'public'
          AND c.relkind IN ('r', 'v', 'm', 'p')
        GROUP BY c.oid, c.relname
        ORDER BY c.relname;
      `.trim();

      const tableRows = await legacyDb.query(catSql, ["public"]);
      if (tableRows && tableRows.length > 0) {
        for (const tbl of tableRows) {
          let cols = tbl.columns;
          if (typeof cols === "string") {
            try { cols = JSON.parse(cols); } catch { cols = []; }
          }
          if (!Array.isArray(cols) || cols.length === 0) continue;

          const colDefs = cols.map((c: any) => {
            let def = `"${c.name}" ${c.type || "TEXT"}`;
            if (!c.nullable) def += " NOT NULL";
            if (c.default) def += ` DEFAULT ${c.default}`;
            return def;
          });

          try {
            this.nativeInstance.exec(`CREATE TABLE IF NOT EXISTS "${tbl.name}" (${colDefs.join(", ")})`);
          } catch {}

          if (tbl.comment) {
            try {
              this.nativeInstance.exec(`COMMENT ON TABLE "${tbl.name}" IS '${tbl.comment.replace(/'/g, "''")}'`);
            } catch {}
          }

          for (const c of cols) {
            if (c.comment) {
              try {
                this.nativeInstance.exec(`COMMENT ON COLUMN "${tbl.name}"."${c.name}" IS '${c.comment.replace(/'/g, "''")}'`);
              } catch {}
            }
          }

          try {
            const data = await legacyDb.query(`SELECT * FROM "${tbl.name}"`);
            if (data && data.length > 0) {
              for (const row of data) {
                const keys = Object.keys(row);
                if (keys.length === 0) continue;
                const insertCols = keys.map(k => `"${k}"`).join(", ");
                const vals = keys.map(k => {
                  const v = row[k];
                  if (v === null || v === undefined) return "NULL";
                  if (typeof v === "number" || typeof v === "boolean") return String(v);
                  return `'${String(v).replace(/'/g, "''")}'`;
                }).join(", ");
                try {
                  this.nativeInstance.exec(`INSERT INTO "${tbl.name}" (${insertCols}) VALUES (${vals})`);
                } catch {}
              }
            }
          } catch {}
        }

        try {
          this.nativeInstance.flush();
        } catch {}
      }

      await legacyDb.close().catch(() => {});
    } catch (err) {
      // Non-blocking
    }
  }

  private async tryHydrateTable(tableName: string, dbName?: string, force: boolean = false): Promise<boolean> {
    const key = `${dbName || "public"}.${tableName.toLowerCase()}`;
    if (!force && this.hydratedTables.has(key)) return false;
    this.hydratedTables.add(key);

    try {
      const js = this.getJsEngine();
      const res = await js.query2(`SELECT * FROM "${tableName}"`, [], dbName);
      if (!res) {
        return false;
      }

      let schemaCols: Record<string, string> = {};
      let metaCols: any[] = [];
      try {
        const tblMeta = (await (js as any).storage?.getTableAsync?.(tableName)) || 
                        (await (js as any).storage?.getTableAsync?.(`public.${tableName}`)) ||
                        (js as any).storage?.tables?.get(tableName) || 
                        (js as any).storage?.tables?.get(tableName.toLowerCase());
        if (tblMeta && tblMeta.columns) {
          metaCols = tblMeta.columns;
          for (const col of tblMeta.columns) {
            schemaCols[col.name.toLowerCase()] = col.dataType || col.data_type || "TEXT";
          }
        }
      } catch {}

      if (res.fields && res.fields.length > 0) {
        for (const f of res.fields) {
          const lower = f.name.toLowerCase();
          if (!schemaCols[lower]) {
            schemaCols[lower] = "TEXT";
          }
        }
      }

      const colDefs: string[] = [];
      const colNames: string[] = [];

      if (metaCols.length > 0) {
        for (const col of metaCols) {
          const colName = col.name;
          const colType = col.dataType || col.data_type || "TEXT";
          colNames.push(`"${colName}"`);
          let def = `"${colName}" ${colType}`;
          if (col.isPrimaryKey || col.is_primary_key) {
            def += " PRIMARY KEY";
          }
          colDefs.push(def);
        }
      } else if (res.fields && res.fields.length > 0) {
        for (const f of res.fields) {
          colNames.push(`"${f.name}"`);
          colDefs.push(`"${f.name}" ${schemaCols[f.name.toLowerCase()] || "TEXT"}`);
        }
      }

      if (colDefs.length > 0) {
        try {
          this.nativeInstance?.exec(`CREATE TABLE IF NOT EXISTS "${tableName}" (${colDefs.join(", ")})`);
        } catch {}
      }

      if (res.rows && res.rows.length > 0) {
        try {
          this.nativeInstance?.exec(`DELETE FROM "${tableName}"`);
        } catch {}

        for (const row of res.rows) {
          const keys = Object.keys(row);
          if (keys.length === 0) continue;
          const cols = keys.map(k => `"${k}"`).join(", ");
          const vals = keys.map(k => {
            const v = row[k];
            if (v === null || v === undefined) return "NULL";
            if (typeof v === "number" || typeof v === "boolean") return String(v);
            if (typeof v === "object") return `'${JSON.stringify(v).replace(/'/g, "''")}'`;
            return `'${String(v).replace(/'/g, "''")}'`;
          }).join(", ");

          try {
            this.nativeInstance?.exec(`INSERT INTO "${tableName}" (${cols}) VALUES (${vals})`);
          } catch {}
        }
      }

      return true;
    } catch {
      return false;
    }
  }

  private async handleFallbackWrite(sql: string, dbName?: string): Promise<void> {
    const trimmed = sql.trim();
    const inTx = Boolean((this.jsFallback as any)?.storage?.inTransaction);

    if (/^\s*COMMIT\b/i.test(trimmed)) {
      this.hydratedTables.clear();
      for (const tbl of Array.from(this.knownTables)) {
        await this.tryHydrateTable(tbl, dbName, true).catch(() => {});
      }
      return;
    }

    if (inTx) return;

    const dropMatch = trimmed.match(/^DROP\s+TABLE(?:\s+IF\s+EXISTS)?\s+([a-zA-Z0-9_"\.-]+)/i);
    if (dropMatch) {
      const rawTable = dropMatch[1].replace(/"/g, "");
      const tbl = rawTable.includes(".") ? rawTable.split(".").pop()! : rawTable;
      const key = `${dbName || "public"}.${tbl.toLowerCase()}`;
      this.hydratedTables.delete(key);
      this.knownTables.delete(tbl.toLowerCase());
      try {
        this.nativeInstance?.exec(`DROP TABLE IF EXISTS "${tbl}"`);
      } catch {}
      return;
    }

    const alterMatch = trimmed.match(/^ALTER\s+TABLE\s+([a-zA-Z0-9_"\.-]+)/i);
    if (alterMatch) {
      const rawTable = alterMatch[1].replace(/"/g, "");
      const tbl = rawTable.includes(".") ? rawTable.split(".").pop()! : rawTable;
      const key = `${dbName || "public"}.${tbl.toLowerCase()}`;
      this.hydratedTables.delete(key);
      try {
        this.nativeInstance?.exec(`DROP TABLE IF EXISTS "${tbl}"`);
      } catch {}
      await this.tryHydrateTable(tbl, dbName, true);
      return;
    }

    const truncMatch = trimmed.match(/^TRUNCATE(?:\s+TABLE)?\s+([a-zA-Z0-9_"\.-]+)/i);
    if (truncMatch) {
      const rawTable = truncMatch[1].replace(/"/g, "");
      const tbl = rawTable.includes(".") ? rawTable.split(".").pop()! : rawTable;
      try {
        this.nativeInstance?.exec(`DELETE FROM "${tbl}"`);
      } catch {}
      return;
    }

    const writeMatch = trimmed.match(/^(?:INSERT\s+INTO|UPDATE|DELETE\s+FROM)\s+([a-zA-Z0-9_"\.-]+)/i);
    if (writeMatch) {
      const rawTable = writeMatch[1].replace(/"/g, "");
      const tbl = rawTable.includes(".") ? rawTable.split(".").pop()! : rawTable;
      const key = `${dbName || "public"}.${tbl.toLowerCase()}`;
      this.hydratedTables.delete(key);
      await this.tryHydrateTable(tbl, dbName, true);
    }
  }

  public async exec<T = any>(sql: string, params?: any, dbName?: string): Promise<T> {
    await this.ensureLegacyMigrated(dbName);
    const stmts = splitStatements(sql);
    if (stmts.length > 1) {
      let lastRes: any = null;
      for (const stmt of stmts) {
        lastRes = await this.exec(stmt, params, dbName);
      }
      return lastRes;
    }

    const singleSql = stmts[0] || sql;

    if (this.nativeInstance) {
      if (!this.allowFallback) {
        let p = Array.isArray(params) ? params : undefined;
        let db = typeof params === "string" ? params : dbName;
        const res = this.nativeInstance.exec(singleSql, p, db) as T;
        if (res && typeof res === "object" && (res as any).success === undefined) {
          (res as any).success = true;
        }
        return res;
      }

      const inTx = Boolean((this.jsFallback as any)?.storage?.inTransaction);
      if (inTx || isComplexQuery(singleSql)) {
        if (this.writeQueue.length > 0) await this.flushWriteQueue();
        const res = await this.getJsEngine().exec<T>(singleSql, params, dbName);
        await this.handleFallbackWrite(singleSql, dbName);
        return res;
      }

      this.recordKnownTable(singleSql);
      try {
        let p = Array.isArray(params) ? params : undefined;
        let db = typeof params === "string" ? params : dbName;
        const res = this.nativeInstance.exec(singleSql, p, db) as T;
        if (res && typeof res === "object" && (res as any).success === undefined) {
          (res as any).success = true;
        }
        const upper = singleSql.trim().toUpperCase();
        if (!upper.startsWith("SELECT")) {
          try {
            await this.getJsEngine().exec(singleSql, params, dbName);
          } catch {}
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
                const res = this.nativeInstance.exec(singleSql, p, db) as T;
                return res;
              } catch (retryErr) {
                currentErr = retryErr;
                continue;
              }
            }
          }
          break;
        }
        await this.flushWriteQueue();
        const res = await this.getJsEngine().exec<T>(singleSql, params, dbName);
        await this.handleFallbackWrite(singleSql, dbName);
        return res;
      }
    }

    const res: any = await this.getJsEngine().exec<T>(singleSql, params, dbName);
    if (res && typeof res === "object" && res.rowCount === undefined) {
      if (res.updated !== undefined) res.rowCount = res.updated;
      else if (res.inserted !== undefined) res.rowCount = Array.isArray(res.inserted) ? res.inserted.length : 1;
      else if (res.deleted !== undefined) res.rowCount = res.deleted;
      else if (res.affectedRows !== undefined) res.rowCount = res.affectedRows;
    }
    return res;
  }

  public async exec2<T = any>(sql: string, params?: any, dbName?: string): Promise<QueryResult<T>> {
    await this.ensureLegacyMigrated(dbName);
    const stmts = splitStatements(sql);
    if (stmts.length > 1) {
      let lastRes: any = null;
      for (const stmt of stmts) {
        lastRes = await this.exec2(stmt, params, dbName);
      }
      return lastRes;
    }

    const singleSql = stmts[0] || sql;

    if (this.nativeInstance) {
      if (!this.allowFallback) {
        let p = Array.isArray(params) ? params : undefined;
        let db = typeof params === "string" ? params : dbName;
        return this.nativeInstance.exec2(singleSql, p, db) as QueryResult<T>;
      }

      const inTx = Boolean((this.jsFallback as any)?.storage?.inTransaction);
      if (inTx || isComplexQuery(singleSql)) {
        if (this.writeQueue.length > 0) await this.flushWriteQueue();
        const res = await this.getJsEngine().exec2<T>(singleSql, params, dbName);
        await this.handleFallbackWrite(singleSql, dbName);
        return res;
      }

      try {
        let p = Array.isArray(params) ? params : undefined;
        let db = typeof params === "string" ? params : dbName;
        const res = this.nativeInstance.exec2(singleSql, p, db) as QueryResult<T>;
        const upper = singleSql.trim().toUpperCase();
        if (!upper.startsWith("SELECT")) {
          try { await this.getJsEngine().exec2<T>(singleSql, params, dbName); } catch {}
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
                return this.nativeInstance.exec2(singleSql, p, db) as QueryResult<T>;
              } catch (retryErr) {
                currentErr = retryErr;
                continue;
              }
            }
          }
          break;
        }
        await this.flushWriteQueue();
        const res = await this.getJsEngine().exec2<T>(singleSql, params, dbName);
        await this.handleFallbackWrite(singleSql, dbName);
        return res;
      }
    }

    return this.getJsEngine().exec2<T>(singleSql, params, dbName);
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
    await this.ensureLegacyMigrated(dbName);
    if (this.nativeInstance) {
      if (!this.allowFallback) {
        let p = Array.isArray(params) ? params : undefined;
        let db = typeof params === "string" ? params : dbName;
        const res = this.runNativeQuery<T>(sql, p, db);
        if (this.options?.debug || process.env.PGLITE_DEBUG) {
          console.log(`[PGLite Native ⚡] Executed in Rust (${res.length} rows): ${sql.slice(0, 80)}`);
        }
        return res;
      }

      const inTx = Boolean((this.jsFallback as any)?.storage?.inTransaction);
      if (inTx || isComplexQuery(sql)) {
        if (this.writeQueue.length > 0) await this.flushWriteQueue();
        const res = await this.getJsEngine().query<T>(sql, params, dbName);
        await this.handleFallbackWrite(sql, dbName);
        return res;
      }

      this.recordKnownTable(sql);
      try {
        let p = Array.isArray(params) ? params : undefined;
        let db = typeof params === "string" ? params : dbName;
        const res = this.runNativeQuery<T>(sql, p, db);
        const upper = sql.trim().toUpperCase();
        if (!upper.startsWith("SELECT")) {
          try { await this.getJsEngine().query<T>(sql, params, dbName); } catch (e) {
            console.error("SYNC QUERY ERROR:", e);
          }
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
                return this.runNativeQuery<T>(sql, p, db);
              } catch (retryErr) {
                currentErr = retryErr;
                continue;
              }
            }
          }
          break;
        }
        await this.flushWriteQueue();
        const res = await this.getJsEngine().query<T>(sql, params, dbName);
        await this.handleFallbackWrite(sql, dbName);
        return res;
      }
    }

    return this.getJsEngine().query<T>(sql, params, dbName);
  }

  public async query2<T = any>(sql: string, params?: any, dbName?: string): Promise<QueryResult<T>> {
    await this.ensureLegacyMigrated(dbName);
    if (this.nativeInstance) {
      if (!this.allowFallback) {
        let p = Array.isArray(params) ? params : undefined;
        let db = typeof params === "string" ? params : dbName;
        const res = this.runNativeQuery2<T>(sql, p, db);
        if (this.options?.debug || process.env.PGLITE_DEBUG) {
          console.log(`[PGLite Native ⚡] Executed in Rust (${res.rowCount} rows): ${sql.slice(0, 80)}`);
        }
        return res;
      }

      const inTx = Boolean((this.jsFallback as any)?.storage?.inTransaction);
      if (inTx || isComplexQuery(sql)) {
        if (this.writeQueue.length > 0) await this.flushWriteQueue();
        const res = await this.getJsEngine().query2<T>(sql, params, dbName);
        await this.handleFallbackWrite(sql, dbName);
        return res;
      }

      this.recordKnownTable(sql);
      try {
        let p = Array.isArray(params) ? params : undefined;
        let db = typeof params === "string" ? params : dbName;
        const res = this.runNativeQuery2<T>(sql, p, db);
        const upper = sql.trim().toUpperCase();
        if (!upper.startsWith("SELECT")) {
          try { await this.getJsEngine().query2<T>(sql, params, dbName); } catch {}
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
                return this.runNativeQuery2<T>(sql, p, db);
              } catch (retryErr) {
                currentErr = retryErr;
                continue;
              }
            }
          }
          break;
        }
        await this.flushWriteQueue();
        const res = await this.getJsEngine().query2<T>(sql, params, dbName);
        await this.handleFallbackWrite(sql, dbName);
        return res;
      }
    }

    return this.getJsEngine().query2<T>(sql, params, dbName);
  }

  public async transaction<T = any>(callback: (tx: any) => Promise<T>, dbName?: string): Promise<T> {
    await this.ensureLegacyMigrated(dbName);
    if (!this.nativeInstance) {
      return this.getJsEngine().transaction<T>(callback, dbName);
    }

    if (!this.allowFallback) {
      await this.exec("BEGIN", undefined, dbName);
      let finished = false;
      const txObj = {
        exec: (sql: string, params?: any, db?: string) => this.exec(sql, params, db || dbName),
        exec2: (sql: string, params?: any, db?: string) => this.exec2(sql, params, db || dbName),
        query: (sql: string, params?: any, db?: string) => this.query(sql, params, db || dbName),
        query2: (sql: string, params?: any, db?: string) => this.query2(sql, params, db || dbName),
        transaction: async () => { throw new Error("Nested transactions not supported"); },
        transaction2: async () => { throw new Error("Nested transactions not supported"); },
      };

      try {
        const result = await callback(txObj);
        await this.exec("COMMIT", undefined, dbName);
        finished = true;
        return result;
      } catch (err) {
        if (!finished) {
          try {
            await this.exec("ROLLBACK", undefined, dbName);
          } catch {}
        }
        throw err;
      }
    }

    await this.flushWriteQueue();
    const result = await this.getJsEngine().transaction<T>(callback, dbName);
    this.hydratedTables.clear();
    for (const tbl of Array.from(this.knownTables)) {
      await this.tryHydrateTable(tbl, dbName, true).catch(() => {});
    }
    return result;
  }

  public async transaction2<T = any>(callback: (tx: any) => Promise<T>, dbName?: string): Promise<T> {
    return this.transaction(callback, dbName);
  }

  public async flush(): Promise<void> {
    await this.flushWriteQueue();
    if (this.nativeInstance?.flush) {
      this.nativeInstance.flush();
    }
  }

  public async close(): Promise<void> {
    await this.flushWriteQueue();
    if (this.nativeInstance?.close) {
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
      // When native instance is active, JS engine must NEVER touch the disk file directly.
      // JS engine runs strictly in-memory (:memory:) to prevent corrupting the native database file.
      const targetPath = this.nativeInstance ? ":memory:" : this.filepath;
      if (!opts.adapter && typeof window === "undefined" && targetPath !== ":memory:") {
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
      this.jsFallback = new JSPostgres(targetPath, opts);
    }
    return this.jsFallback;
  }
}
