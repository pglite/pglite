import { Lexer } from './parser/lexer';
import { Parser } from './parser/parser';
import { Executor } from './execution/executor';
import { StorageEngine, type VFS } from './storage/engine';
import { Statement } from './ast';

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
  public async exec<T = any>(sql: string, params: any[] = [], dbName?: string): Promise<T> {
    return this.run(sql, params, dbName || this.defaultDb);
  }

  /**
   * Used for statements that return rows (SELECT)
   */
  public async query<T = any>(sql: string, params: any[] = [], dbName?: string): Promise<T[]> {
    const result = await this.run(sql, params, dbName || this.defaultDb);
    return Array.isArray(result) ? result : [];
  }

  /**
   * Explicitly release resources. Crucial for handling 1M+ database instances.
   */
  public async close(): Promise<void> {
    if (this.destroyOnClose) {
      await this.storage.destroy();
    } else {
      await this.storage.close();
    }
  }

  /**
   * Explicitly destroy the database files.
   */
  public async destroy(): Promise<void> {
    await this.storage.destroy();
  }

  private async run(sql: string, params: any[] = [], dbName: string): Promise<any> {
    let release: (() => void) | undefined;
    
    if (!this.storage.isInTransaction()) {
      release = await FileMutex.acquire(this.storage.filepath);
    }

    try {
      sql = sql.trim();
      if (this.currentDb !== dbName) {
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