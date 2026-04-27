import { Lexer } from './parser/lexer';
import { Parser } from './parser/parser';
import { Executor } from './execution/executor';
import { StorageEngine, type VFS } from './storage/engine';
import { Statement } from './ast';

export interface QueryResult<R = any> {
  rows: R[];
  rowCount: number;
  fields: { name: string }[];
  command: string;
}

export class FileMutex {
  private static locks = new Map<string, Promise<void>>();

  static async acquire(filepath: string): Promise<() => void> {
    while (this.locks.has(filepath)) {
      await this.locks.get(filepath);
    }
    let release!: () => void;
    const p = new Promise<void>((resolve) => {
      release = () => {
        this.locks.delete(filepath);
        resolve();
      };
    });
    this.locks.set(filepath, p);
    return release;
  }
}

export class LitePostgres {
  private static readonly executor = new Executor();
  private static readonly astCache = new Map<string, Statement[]>();
  private storage: StorageEngine;

  private defaultDb: string;
  private currentDb?: string;
  private destroyOnClose: boolean;
  private txRelease?: () => void;
  private queue = Promise.resolve();

  constructor(filepath: string, options: { database?: string, adapter: VFS, destroyOnClose?: boolean }) {
    this.defaultDb = options.database || 'postgres';
    this.destroyOnClose = !!options.destroyOnClose;
    if (!options.adapter) {
      throw new Error("A VFS adapter must be provided. For Node.js, use NodeFSAdapter from '@pglite/core/node-fs'.");
    }
    this.storage = new StorageEngine(options.adapter, filepath);
  }

  /**
   * Used for statements that do not return rows (CREATE, INSERT, UPDATE, DELETE)
   */
  public async exec<T = any>(sql: string, params?: any[] | Record<string, any> | string, dbName?: string): Promise<T> {
    let actualParams: any = [];
    let actualDbName = dbName;
    if (typeof params === 'string') {
      actualDbName = params;
    } else if (params !== undefined && params !== null) {
      actualParams = params;
    }

    return new Promise((resolve, reject) => {
      this.queue = this.queue.then(async () => {
        try {
          resolve(await this.run(sql, actualParams, actualDbName || this.defaultDb));
        } catch (e) {
          reject(e);
        }
      });
    });
  }

  /**
   * Used for statements that do not return rows (CREATE, INSERT, UPDATE, DELETE)
   * returns standard PostgreSQL result format
   */
  public async exec2<T = any>(sql: string, params?: any[] | Record<string, any> | string, dbName?: string): Promise<QueryResult<T>> {
    return this.query2(sql, params, dbName);
  }

  private formatQueryResult<T>(sql: string, result: any): QueryResult<T> {
    let rows: any[] = [];
    let rowCount = 0;
    const match = sql.trim().match(/^[A-Za-z]+/);
    let command = match ? match[0].toUpperCase() : '';
    
    if (Array.isArray(result)) {
      rows = result;
      rowCount = rows.length;
    } else if (result && typeof result === 'object') {
      if (result.inserted !== undefined) {
        if (Array.isArray(result.inserted)) {
          rowCount = result.inserted.length;
        } else {
          rowCount = 1;
        }
      } else if (result.updated !== undefined) {
        rowCount = result.updated;
      } else if (result.deleted !== undefined) {
        rowCount = result.deleted;
      } else if (result.conflict === 'nothing') {
        rowCount = 0;
      }
    }
    const fields = rows.length > 0 ? Object.keys(rows[0]).map(k => ({ name: k })) : [];
    return { rows, rowCount, fields, command };
  }

  /**
   * Used for statements that return rows (SELECT) in standard PostgreSQL format
   */
  public async query2<T = any>(sql: string, params?: any[] | Record<string, any> | string, dbName?: string): Promise<QueryResult<T>> {
    let actualParams: any = [];
    let actualDbName = dbName;
    if (typeof params === 'string') {
      actualDbName = params;
    } else if (params !== undefined && params !== null) {
      actualParams = params;
    }

    return new Promise((resolve, reject) => {
      this.queue = this.queue.then(async () => {
        try {
          const result = await this.run(sql, actualParams, actualDbName || this.defaultDb);
          resolve(this.formatQueryResult<T>(sql, result));
        } catch (e) {
          reject(e);
        }
      });
    });
  }

  /**
   * Used for statements that return rows (SELECT)
   */
  public async query<T = any>(sql: string, params?: any[] | Record<string, any> | string, dbName?: string): Promise<T[]> {
    let actualParams: any = [];
    let actualDbName = dbName;
    if (typeof params === 'string') {
      actualDbName = params;
    } else if (params !== undefined && params !== null) {
      actualParams = params;
    }

    return new Promise((resolve, reject) => {
      this.queue = this.queue.then(async () => {
        try {
          const result = await this.run(sql, actualParams, actualDbName || this.defaultDb);
          resolve(Array.isArray(result) ? result :[]);
        } catch (e) {
          reject(e);
        }
      });
    });
  }

  /**
   * Execute a callback function within a database transaction.
   * If the callback resolves, the transaction is committed.
   * If the callback throws an error, the transaction is rolled back.
   */
  public async transaction<T>(callback: (tx: LitePostgres) => Promise<T>, dbName?: string): Promise<T> {
    const txDb = dbName || this.defaultDb;
    return new Promise((resolve, reject) => {
      this.queue = this.queue.then(async () => {
        let txQueue = Promise.resolve();
        try {
          await this.run('BEGIN',[], txDb);
          
          const txObj = new Proxy(this, {
            get: (target, prop) => {
              if (prop === 'exec') return (sql: string, p?: any[] | Record<string, any> | string, db?: string) => {
                let actualP: any = [];
                let actualDb = db;
                if (typeof p === 'string') { actualDb = p; } else if (p !== undefined && p !== null) { actualP = p; }
                return new Promise((resolve, reject) => {
                  txQueue = txQueue.then(async () => {
                    try { resolve(await target.run(sql, actualP, actualDb || txDb)); }
                    catch (e) { reject(e); }
                  });
                });
              };
              if (prop === 'exec2') return (sql: string, p?: any[] | Record<string, any> | string, db?: string) => {
                let actualP: any = [];
                let actualDb = db;
                if (typeof p === 'string') { actualDb = p; } else if (p !== undefined && p !== null) { actualP = p; }
                return new Promise((resolve, reject) => {
                  txQueue = txQueue.then(async () => {
                    try {
                      const res = await target.run(sql, actualP, actualDb || txDb);
                      resolve((target as any).formatQueryResult(sql, res));
                    } catch (e) { reject(e); }
                  });
                });
              };
              if (prop === 'query') return (sql: string, p?: any[] | Record<string, any> | string, db?: string) => {
                let actualP: any = [];
                let actualDb = db;
                if (typeof p === 'string') { actualDb = p; } else if (p !== undefined && p !== null) { actualP = p; }
                return new Promise((resolve, reject) => {
                  txQueue = txQueue.then(async () => {
                    try {
                      const res = await target.run(sql, actualP, actualDb || txDb);
                      resolve(Array.isArray(res) ? res : []);
                    } catch (e) { reject(e); }
                  });
                });
              };
              if (prop === 'query2') return (sql: string, p?: any[] | Record<string, any> | string, db?: string) => {
                let actualP: any = [];
                let actualDb = db;
                if (typeof p === 'string') { actualDb = p; } else if (p !== undefined && p !== null) { actualP = p; }
                return new Promise((resolve, reject) => {
                  txQueue = txQueue.then(async () => {
                    try {
                      const res = await target.run(sql, actualP, actualDb || txDb);
                      resolve((target as any).formatQueryResult(sql, res));
                    } catch (e) { reject(e); }
                  });
                });
              };
              if (prop === 'transaction' || prop === 'transaction2') return async () => { throw new Error("Nested transactions not supported"); };
              return (target as any)[prop];
            }
          });

          const result = await callback(txObj as unknown as LitePostgres);
          
          await txQueue;

          if (this.storage.isInTransaction()) {
            await this.run('COMMIT',[], txDb);
          }
          resolve(result);
        } catch (error) {
          await txQueue;
          if (this.storage.isInTransaction()) {
            try {
              await this.run('ROLLBACK',[], txDb);
            } catch (rollbackError) {}
          }
          reject(error);
        }
      });
    });
  }

  public async transaction2<T>(callback: (tx: LitePostgres) => Promise<T>, dbName?: string): Promise<T> {
    return this.transaction(callback, dbName);
  }

  /**
   * Explicitly release resources. Crucial for handling 1M+ database instances.
   */
  public async close(): Promise<void> {
    return new Promise((resolve, reject) => {
      this.queue = this.queue.then(async () => {
        try {
          if (this.destroyOnClose) {
            await this.storage.destroy();
          } else {
            await this.storage.close();
          }
          resolve();
        } catch (e) {
          reject(e);
        }
      });
    });
  }

  /**
   * Explicitly destroy the database files.
   */
  public async destroy(): Promise<void> {
    return new Promise((resolve, reject) => {
      this.queue = this.queue.then(async () => {
        try {
          await this.storage.destroy();
          resolve();
        } catch (e) {
          reject(e);
        }
      });
    });
  }

  private async run(sql: string, params: any = [], dbName: string): Promise<any> {
    let release: (() => void) | undefined;
    
    if (!this.storage.isInTransaction()) {
      release = await FileMutex.acquire(this.storage.filepath);
    }

    try {
      sql = sql.trim();
      if (this.currentDb !== dbName) {
        if (this.storage.isInTransaction()) {
          throw new Error("Cannot switch database within a transaction");
        }
        await this.storage.init(dbName);
        this.currentDb = dbName;
      }

      let asts = LitePostgres.astCache.get(sql);
      if (!asts) {
        const lexer = new Lexer(sql);
        const tokens = lexer.tokenize();
        const parser = new Parser(tokens);
        asts = [];
        while (parser.hasMore()) {
          const ast = parser.parse();
          if (ast) asts.push(ast);
          if (parser.match('SYMBOL', ';')) {
            parser.consume();
          }
        }
        LitePostgres.astCache.set(sql, asts);
      }
      
      const results = [];
      for (const ast of asts) {
        results.push(await LitePostgres.executor.execute(this.storage, ast, params));
      }

      if (!this.storage.isInTransaction()) {
        await this.storage.flush();
        if (release) release();
        if (this.txRelease) {
          this.txRelease();
          this.txRelease = undefined;
        }
      } else {
        if (release) {
          this.txRelease = release;
        }
      }
      
      return results.length === 1 ? results[0] : results;
    } catch (error: any) {
      if (this.storage.isInTransaction()) {
        await this.storage.rollback();
      } else {
        await this.storage.rollbackStatement();
      }
      if (this.txRelease) {
        this.txRelease();
        this.txRelease = undefined;
      }
      if (release) release();
      
      console.log(`[LitePostgres Error]`, error);
      throw new Error(`[LitePostgres Error] ${error.message}`);
    }
  }
}