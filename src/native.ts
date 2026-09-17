import { getNativeBinding } from "./native-loader";

export interface QueryResult<R = any> {
  rows: R[];
  rowCount: number;
  fields: { name: string; dataType?: string }[];
  command: string;
}

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

function sanitizeParamValue(v: any): any {
  if (v === null || v === undefined) return null;
  if (v instanceof Date) {
    return isNaN(v.getTime()) ? null : v.toISOString();
  }
  return v;
}

function normalizeQueryParams(sql: string, params: any): { sql: string; params: any[] | undefined } {
  sql = stripSqlComments(sql);
  if (!params) return { sql, params: undefined };

  if (Array.isArray(params)) {
    const sanitizedParams = params.map(sanitizeParamValue);
    const paramNames: string[] = [];
    const regex = /(?<!:):([a-zA-Z_][a-zA-Z0-9_]*)\b/g;
    let match;
    while ((match = regex.exec(sql)) !== null) {
      if (!paramNames.includes(match[1])) {
        paramNames.push(match[1]);
      }
    }
    if (paramNames.length > 0) {
      let replacedSql = sql;
      paramNames.forEach((name, idx) => {
        const nameRegex = new RegExp(`(?<!:):${name}\\b`, "g");
        replacedSql = replacedSql.replace(nameRegex, `$${idx + 1}`);
      });
      return { sql: replacedSql, params: sanitizedParams };
    }
    return { sql, params: sanitizedParams };
  }

  if (typeof params === "object") {
    const paramNames: string[] = [];
    const regex = /(?<!:):([a-zA-Z_][a-zA-Z0-9_]*)\b/g;
    let match;
    while ((match = regex.exec(sql)) !== null) {
      if (!paramNames.includes(match[1])) {
        paramNames.push(match[1]);
      }
    }
    if (paramNames.length > 0) {
      let replacedSql = sql;
      const paramArray: any[] = [];
      paramNames.forEach((name, idx) => {
        const raw = params[name];
        paramArray.push(sanitizeParamValue(raw !== undefined ? raw : null));
        const nameRegex = new RegExp(`(?<!:):${name}\\b`, "g");
        replacedSql = replacedSql.replace(nameRegex, `$${idx + 1}`);
      });
      return { sql: replacedSql, params: paramArray };
    }
  }

  return { sql, params: undefined };
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

export class PGLiteNative {
  private nativeInstance: any = null;
  private filepath: string;
  private options: any;

  constructor(filepath: string = ":memory:", options: any = {}) {
    this.filepath = filepath;
    this.options = options;

    const binding = getNativeBinding();
    if (!binding || !binding.LitePostgresNative) {
      throw new Error(
        `[PGLite] Failed to load native Rust addon (pglite.node) for ${process.platform}-${process.arch}. ` +
        `Ensure that the native binary exists or compile it via 'bun run build:native'.`
      );
    }

    this.nativeInstance = new binding.LitePostgresNative(filepath);
  }

  public get native(): any {
    return this.nativeInstance;
  }

  public async exec<T = any>(sql: string, params?: any, dbName?: string): Promise<T> {
    const stmts = splitStatements(sql);
    if (stmts.length > 1) {
      let lastRes: any = null;
      for (const stmt of stmts) {
        lastRes = await this.exec(stmt, params, dbName);
      }
      return lastRes;
    }

    const singleSql = stmts[0] || sql;
    const normalized = normalizeQueryParams(singleSql, params);
    let p = normalized.params;
    let db = typeof params === "string" ? params : dbName;
    const res = this.nativeInstance.exec(normalized.sql, p, db) as T;
    if (res && typeof res === "object" && (res as any).success === undefined) {
      (res as any).success = true;
    }
    return res;
  }

  public async exec2<T = any>(sql: string, params?: any, dbName?: string): Promise<T> {
    const stmts = splitStatements(sql);
    if (stmts.length > 1) {
      let lastRes: any = null;
      for (const stmt of stmts) {
        lastRes = await this.exec2(stmt, params, dbName);
      }
      return lastRes;
    }

    const singleSql = stmts[0] || sql;
    const normalized = normalizeQueryParams(singleSql, params);
    let p = normalized.params;
    let db = typeof params === "string" ? params : dbName;
    return this.nativeInstance.exec2(normalized.sql, p, db) as T;
  }

  public async query<T = any>(sql: string, params?: any, dbName?: string): Promise<T[]> {
    const stmts = splitStatements(sql);
    if (stmts.length > 1) {
      let lastRes: any = [];
      for (const stmt of stmts) {
        lastRes = await this.query<T>(stmt, params, dbName);
      }
      return lastRes;
    }

    const singleSql = stmts[0] || sql;
    const normalized = normalizeQueryParams(singleSql, params);
    let p = normalized.params;
    let db = typeof params === "string" ? params : dbName;
    return this.nativeInstance.query(normalized.sql, p, db) as T[];
  }

  public async query2<T = any>(sql: string, params?: any, dbName?: string): Promise<QueryResult<T>> {
    const stmts = splitStatements(sql);
    if (stmts.length > 1) {
      let lastRes: any = null;
      for (const stmt of stmts) {
        lastRes = await this.query2<T>(stmt, params, dbName);
      }
      return lastRes;
    }

    const singleSql = stmts[0] || sql;
    const normalized = normalizeQueryParams(singleSql, params);
    let p = normalized.params;
    let db = typeof params === "string" ? params : dbName;
    return this.nativeInstance.query2(normalized.sql, p, db) as QueryResult<T>;
  }

  public query_json(sql: string, params?: any, dbName?: string): string {
    const normalized = normalizeQueryParams(sql, params);
    let p = normalized.params;
    let db = typeof params === "string" ? params : dbName;
    return this.nativeInstance.query_json(normalized.sql, p, db);
  }

  public query2_json(sql: string, params?: any, dbName?: string): string {
    const normalized = normalizeQueryParams(sql, params);
    let p = normalized.params;
    let db = typeof params === "string" ? params : dbName;
    return this.nativeInstance.query2_json(normalized.sql, p, db);
  }

  public async transaction<T = any>(callback: (tx: any) => Promise<T>, dbName?: string): Promise<T> {
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

  public async transaction2<T = any>(callback: (tx: any) => Promise<T>, dbName?: string): Promise<T> {
    return this.transaction(callback, dbName);
  }

  public async flush(): Promise<void> {
    if (this.nativeInstance?.flush) {
      this.nativeInstance.flush();
    }
  }

  public async close(): Promise<void> {
    if (this.nativeInstance?.close) {
      try {
        this.nativeInstance.close();
      } catch {}
    }
  }
}

export { PGLiteNative as PGLite };
