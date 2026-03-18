import { Lexer } from './parser/lexer';
import { Parser } from './parser/parser';
import { Executor } from './execution/executor';
import { StorageEngine, type VFS } from './storage/engine';
import { Statement } from './ast';

export class LitePostgres {
  private static readonly executor = new Executor();
  private static readonly astCache = new Map<string, Statement[]>();
  private storage: StorageEngine;

  private defaultDb: string;
  private currentDb?: string;

  constructor(filepath: string, options: { database?: string, adapter: VFS }) {
    this.defaultDb = options.database || 'postgres';
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
    await this.storage.close();
  }

  private async run(sql: string, params: any[] = [], dbName: string): Promise<any> {
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
      }
      
      return results.length === 1 ? results[0] : results;
    } catch (error: any) {
      console.log(`[LitePostgres Error]`, error);
      throw new Error(`[LitePostgres Error] ${error.message}`);
    }
  }
}