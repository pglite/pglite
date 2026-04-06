export type TokenType = 'KEYWORD' | 'IDENTIFIER' | 'STRING' | 'NUMBER' | 'SYMBOL' | 'EOF' | 'PARAMETER';

export interface Token {
  type: TokenType;
  value: string;
}

export type Expr =
  | { type: 'Literal'; value: string | number | boolean | null }
  | { type: 'Identifier'; name: string }
  | { type: 'Parameter'; index: number }
  | { type: 'Binary'; left: Expr; operator: string; right: Expr }
  | { type: 'Logical'; left: Expr; operator: string; right: Expr }
  | { type: 'Not'; expr: Expr }
  | { type: 'Call'; fnName: string; args: Expr[]; distinct?: boolean; argsOrderBy?: OrderBy[]; over?: { partitionBy?: Expr[]; orderBy?: OrderBy[] }; filter?: Expr }
  | { type: 'In'; left: Expr; right: Expr[] | Statement; not?: boolean }
  | { type: 'Like'; left: Expr; right: Expr; not?: boolean; escapeStr?: string; ilike?: boolean }
  | { type: 'Alias'; expr: Expr; alias: string }
  | { type: 'Subquery'; stmt: Statement }
  | { type: 'IsNull'; expr: Expr; not: boolean }
  | { type: 'Cast'; expr: Expr; dataType: string }
  | { type: 'Case'; cases: { when: Expr; then: Expr }[]; elseExpr?: Expr }
  | { type: 'Array'; elements: Expr[] }
  | { type: 'Interval'; value: string }
  | { type: 'Extract'; field: string; source: Expr }
  | { type: 'Exists'; stmt: Statement };

export interface OnConflict {
  targetColumns?: string[];
  action: 'NOTHING' | 'UPDATE';
  assignments?: Record<string, Expr>;
}

export interface ColumnDef {
  name: string;
  dataType: string;
  isPrimaryKey: boolean;
  isUnique?: boolean;
  isNotNull?: boolean;
  references?: { 
    table: string; 
    column: string;
    onDelete?: 'CASCADE' | 'RESTRICT' | 'SET NULL' | 'SET DEFAULT' | 'NO ACTION';
    onUpdate?: 'CASCADE' | 'RESTRICT' | 'SET NULL' | 'SET DEFAULT' | 'NO ACTION';
  };
  defaultVal?: Expr;
  generatedExpr?: Expr;
  comment?: string;
}

export interface JoinClause {
  type: 'INNER' | 'LEFT' | 'RIGHT' | 'FULL' | 'CROSS';
  tableName?: string;
  stmt?: Statement;
  fn?: Expr;
  withOrdinality?: boolean;
  columnAliases?: string[];
  lateral?: boolean;
  alias?: string;
  on: Expr;
}

export interface OrderBy {
  expr: Expr;
  desc: boolean;
}

export type AlterTableAction =
  | { type: 'AddColumn'; column: ColumnDef; ifNotExists?: boolean }
  | { type: 'DropColumn'; columnName: string; ifExists?: boolean }
  | { type: 'RenameColumn'; oldColumnName: string; newColumnName: string }
  | { type: 'RenameTable'; newTableName: string }
  | { type: 'AlterColumnType'; columnName: string; dataType: string }
  | { type: 'AlterColumnSetDefault'; columnName: string; defaultVal: Expr }
  | { type: 'AlterColumnDropDefault'; columnName: string }
  | { type: 'AlterColumnSetNotNull'; columnName: string }
  | { type: 'AlterColumnDropNotNull'; columnName: string }
  | { type: 'AddForeignKey'; columnName: string; references: { table: string; column: string; onDelete?: 'CASCADE' | 'RESTRICT' | 'SET NULL' | 'SET DEFAULT' | 'NO ACTION'; onUpdate?: 'CASCADE' | 'RESTRICT' | 'SET NULL' | 'SET DEFAULT' | 'NO ACTION' } };

export type Statement =
  | { type: 'CreateSchema'; schemaName: string; ifNotExists?: boolean }
  | { type: 'CreateIndex'; indexName: string; tableName: string; columns: string[]; unique?: boolean; ifNotExists?: boolean }
  | { type: 'DropSchema'; schemaName: string; ifExists?: boolean; cascade?: boolean }
  | { type: 'DropTable'; tableName: string; ifExists?: boolean }
  | { type: 'CreateTable'; tableName: string; columns: ColumnDef[]; ifNotExists?: boolean }
  | { type: 'Insert'; tableName: string; columns: string[]; values?: Expr[][] | Expr[]; select?: Statement; returning?: Expr[]; onConflict?: OnConflict }
  | { 
      type: 'Select'; 
      columns: Expr[]; 
      distinct?: boolean;
      distinctOn?: Expr[];
      from?: { 
        tableName?: string; 
        alias?: string; 
        stmt?: Statement; 
        fn?: Expr;
        withOrdinality?: boolean;
        columnAliases?: string[];
      }; 
      joins?: JoinClause[]; 
      where?: Expr;
      groupBy?: Expr[];
      having?: Expr;
      orderBy?: OrderBy[];
      limit?: Expr;
      offset?: Expr;
      ctes?: { name: string; columnAliases?: string[]; stmt: Statement; recursive?: boolean }[];
      union?: Statement;
      unionAll?: Statement;
      intersect?: Statement;
      intersectAll?: Statement;
      except?: Statement;
      exceptAll?: Statement;
    }
  | { type: 'Update'; tableName: string; assignments: Record<string, Expr>; where?: Expr; returning?: Expr[] }
  | { type: 'Delete'; tableName: string; where?: Expr; returning?: Expr[] }
  | { type: 'Truncate'; tableNames: string[]; cascade?: boolean; restartIdentity?: boolean }
  | { type: 'Values'; values: Expr[][]; ctes?: { name: string; columnAliases?: string[]; stmt: Statement; recursive?: boolean }[] }
  | { type: 'AlterTable'; tableName: string; action: AlterTableAction }
  | { type: 'Do'; code: string; language?: string }
  | { type: 'Comment'; objectType: string; objectName: string; comment: string }
  | { type: 'Begin' }
  | { type: 'Commit' }
  | { type: 'Rollback' };