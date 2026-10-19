import { AsyncLocalStorage } from "node:async_hooks";
import { getNativeBinding } from "./native-loader";

/** Instance đang chạy transaction() trong ngữ cảnh async hiện tại (để lệnh bên trong không phải chờ). */
const txContext = new AsyncLocalStorage<object>();

export interface QueryResult<R = any> {
  rows: R[];
  rowCount: number;
  fields: { name: string; dataType?: string }[];
  command: string;
}

function stripSqlComments(sql: string): string {
  if (!sql.includes("--") && !sql.includes("/*")) {
    return sql;
  }
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

/**
 * Đổi tham số có tên `:name` thành `$n`, chỉ ở ngoài chuỗi ('...', "...", $$...$$) và không đụng
 * `::type`. Trước đây regex chạy trên cả chuỗi literal nên `'{"a":true}'` bị đổi thành `'{"a"$1}'`.
 */
function rewriteNamedParams(sql: string): { sql: string; names: string[] } {
  const names: string[] = [];
  let out = "";
  let i = 0;
  const n = sql.length;
  while (i < n) {
    const ch = sql[i];
    if (ch === "'" || ch === '"') {
      let j = i + 1;
      while (j < n) {
        if (sql[j] === ch) {
          if (sql[j + 1] === ch) { j += 2; continue; }
          break;
        }
        j++;
      }
      out += sql.slice(i, j + 1);
      i = j + 1;
      continue;
    }
    if (ch === "$") {
      const m = /^\$([A-Za-z_][A-Za-z0-9_]*)?\$/.exec(sql.slice(i));
      if (m && !(i > 0 && /[A-Za-z0-9_]/.test(sql[i - 1]))) {
        const close = sql.indexOf(m[0], i + m[0].length);
        const end = close === -1 ? n : close + m[0].length;
        out += sql.slice(i, end);
        i = end;
        continue;
      }
    }
    if (ch === ":" && sql[i + 1] !== ":" && (i === 0 || sql[i - 1] !== ":")) {
      const m = /^:([a-zA-Z_][a-zA-Z0-9_]*)\b/.exec(sql.slice(i));
      if (m) {
        let idx = names.indexOf(m[1]);
        if (idx === -1) { names.push(m[1]); idx = names.length - 1; }
        out += `$${idx + 1}`;
        i += m[0].length;
        continue;
      }
    }
    out += ch;
    i++;
  }
  return { sql: out, names };
}

function normalizeQueryParams(sql: string, params: any): { sql: string; params: any[] | undefined } {
  sql = stripSqlComments(sql);
  if (!params) return { sql, params: undefined };

  if (Array.isArray(params)) {
    const sanitizedParams = params.map(sanitizeParamValue);
    const rewritten = rewriteNamedParams(sql);
    return { sql: rewritten.names.length > 0 ? rewritten.sql : sql, params: sanitizedParams };
  }

  if (typeof params === "object") {
    const rewritten = rewriteNamedParams(sql);
    if (rewritten.names.length > 0) {
      const paramArray = rewritten.names.map((name) => {
        const raw = params[name];
        return sanitizeParamValue(raw !== undefined ? raw : null);
      });
      return { sql: rewritten.sql, params: paramArray };
    }
  }

  return { sql, params: undefined };
}

/**
 * Chuỗi nhiều câu lệnh không kèm tham số được gửi nguyên cho engine (như simple query của PostgreSQL):
 * engine tự tách câu (hiểu E'..', $$..$$, comment lồng nhau) và chạy cả chuỗi trong một giao dịch ngầm.
 */
function noParams(params: any): boolean {
  return params === undefined || params === null || typeof params === "string" || (Array.isArray(params) && params.length === 0);
}

function splitStatements(sql: string): string[] {
  if (!sql.includes(";") && !sql.includes("--") && !sql.includes("/*")) {
    const trimmed = sql.trim();
    return trimmed ? [trimmed] : [];
  }
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
    } else if (ch === "$" && !inSingleQuote && !inDoubleQuote) {
      // Chuỗi $tag$...$tag$ (thân DO / hàm) chứa ';' bên trong; $1 là tham số, không phải chuỗi.
      const prev = i > 0 ? stripped[i - 1] : "";
      const m = /^\$([A-Za-z_][A-Za-z0-9_]*)?\$/.exec(stripped.slice(i));
      if (m && !/[A-Za-z0-9_]/.test(prev)) {
        const tag = m[0];
        const close = stripped.indexOf(tag, i + tag.length);
        const end = close === -1 ? stripped.length : close + tag.length;
        current += stripped.slice(i, end);
        i = end - 1;
      } else {
        current += ch;
      }
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
  /**
   * Engine chỉ có một trạng thái giao dịch cho mỗi instance. transaction() giữ lượt từ BEGIN tới
   * COMMIT/ROLLBACK; lệnh từ nơi khác chờ tới khi giao dịch xong, thay vì lọt vào giữa giao dịch.
   */
  private txTail: Promise<void> = Promise.resolve();

  private async waitTurn(): Promise<void> {
    if (txContext.getStore() === this) return;
    await this.txTail;
  }

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
    this.sqlstateFn = typeof (binding as any).sqlstate === "function" ? (binding as any).sqlstate : null;
  }

  private sqlstateFn: ((message: string) => string) | null = null;

  /** Gọi engine; lỗi được gắn `code` = SQLSTATE của PostgreSQL (23505, 23503, ...) như node-postgres. */
  private callNative<T>(fn: () => T): T {
    try {
      return fn();
    } catch (err: any) {
      if (err && typeof err === "object" && this.sqlstateFn && (!err.code || err.code === "GenericFailure")) {
        try {
          err.code = this.sqlstateFn(String(err.message ?? err));
        } catch {}
      }
      throw err;
    }
  }

  public get native(): any {
    return this.nativeInstance;
  }

  public async exec<T = any>(sql: string, params?: any, dbName?: string): Promise<T> {
    await this.waitTurn();
    const stmts = splitStatements(sql);
    if (stmts.length > 1 && noParams(params)) {
      const db = typeof params === "string" ? params : dbName;
      const res = this.callNative(() => this.nativeInstance.exec(sql, undefined, db)) as T;
      if (res && typeof res === "object" && (res as any).success === undefined) {
        (res as any).success = true;
      }
      return res;
    }
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
    const res = this.callNative(() => this.nativeInstance.exec(normalized.sql, p, db)) as T;
    if (res && typeof res === "object" && (res as any).success === undefined) {
      (res as any).success = true;
    }
    return res;
  }

  public async exec2<T = any>(sql: string, params?: any, dbName?: string): Promise<T> {
    await this.waitTurn();
    const stmts = splitStatements(sql);
    if (stmts.length > 1 && noParams(params)) {
      const db = typeof params === "string" ? params : dbName;
      return this.callNative(() => this.nativeInstance.exec2(sql, undefined, db));
    }
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
    return this.callNative(() => this.nativeInstance.exec2(normalized.sql, p, db)) as T;
  }

  public async query<T = any>(sql: string, params?: any, dbName?: string): Promise<T[]> {
    await this.waitTurn();
    const stmts = splitStatements(sql);
    if (stmts.length > 1 && noParams(params)) {
      const db = typeof params === "string" ? params : dbName;
      return this.callNative(() => this.nativeInstance.query(sql, undefined, db));
    }
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
    return this.callNative(() => this.nativeInstance.query(normalized.sql, p, db)) as T[];
  }

  public async query2<T = any>(sql: string, params?: any, dbName?: string): Promise<QueryResult<T>> {
    await this.waitTurn();
    const stmts = splitStatements(sql);
    if (stmts.length > 1 && noParams(params)) {
      const db = typeof params === "string" ? params : dbName;
      return this.callNative(() => this.nativeInstance.query2(sql, undefined, db));
    }
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
    return this.callNative(() => this.nativeInstance.query2(normalized.sql, p, db)) as QueryResult<T>;
  }

  public query_json(sql: string, params?: any, dbName?: string): string {
    const normalized = normalizeQueryParams(sql, params);
    let p = normalized.params;
    let db = typeof params === "string" ? params : dbName;
    return this.callNative(() => this.nativeInstance.query_json(normalized.sql, p, db));
  }

  public query2_json(sql: string, params?: any, dbName?: string): string {
    const normalized = normalizeQueryParams(sql, params);
    let p = normalized.params;
    let db = typeof params === "string" ? params : dbName;
    return this.callNative(() => this.nativeInstance.query2_json(normalized.sql, p, db));
  }

  public async transaction<T = any>(callback: (tx: any) => Promise<T>, dbName?: string): Promise<T> {
    if (txContext.getStore() === this) {
      throw new Error("Nested transactions not supported");
    }
    let release!: () => void;
    const gate = new Promise<void>((resolve) => (release = resolve));
    const prev = this.txTail;
    this.txTail = prev.then(() => gate);
    await prev;
    try {
      return await txContext.run(this, () => this.runTransaction(callback, dbName));
    } finally {
      release();
    }
  }

  private async runTransaction<T = any>(callback: (tx: any) => Promise<T>, dbName?: string): Promise<T> {
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
