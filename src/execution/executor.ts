import { Statement, Expr, JoinClause, OrderBy } from "../ast";
import { endBenchmarks, startBenchmarks } from "../benchmarks";
import { StorageEngine } from "../storage/engine";
import { Lexer } from "../parser/lexer";
import { Parser } from "../parser/parser";
import { JITCompiler } from "./jit";

export class Executor {
  private exprKeyMap = new WeakMap<Expr, string>();
  private keyCount = 0;

  /**
   * Generates or retrieves a unique, stable key for an AST expression object.
   * This avoids expensive JSON.stringify calls in hot loops (1M+ rows).
   */
  private getExprKey(expr: Expr): string {
    if (!expr || typeof expr !== "object") return String(expr);
    let key = this.exprKeyMap.get(expr);
    if (!key) {
      key = `__e${++this.keyCount}`;
      this.exprKeyMap.set(expr, key);
    }
    return key;
  }

  private getTableCopy(r: any): any {
    if (!r) return r;
    const copy: any = {};
    for (const k in r) {
      if (k.charCodeAt(0) !== 95 || !k.startsWith("__lpg_tbl_")) {
        copy[k] = r[k];
      }
    }
    return copy;
  }

  private hasAsyncOps(expr: any): boolean {
    if (!expr) return false;
    if (expr._hasAsync !== undefined) return expr._hasAsync;
    let res = false;
    switch (expr.type) {
      case "Subquery":
      case "Exists":
        res = true;
        break;
      case "In":
        if (!Array.isArray(expr.right)) res = true;
        else res = this.hasAsyncOps(expr.left) || expr.right.some((e: any) => this.hasAsyncOps(e));
        break;
      case "Binary":
      case "Logical":
      case "Like":
        res = this.hasAsyncOps(expr.left) || this.hasAsyncOps(expr.right);
        break;
      case "Not":
      case "IsNull":
      case "Cast": {
        const dt = (expr.dataType || "").toUpperCase();
        if (
          dt.includes("REGCLASS") ||
          dt.includes("REGTYPE") ||
          dt.includes("REGNAMESPACE")
        ) {
          res = true;
        } else {
          res = this.hasAsyncOps(expr.expr);
        }
        break;
      }
      case "Extract":
        res = this.hasAsyncOps(expr.source);
        break;
      case "Alias":
        res = this.hasAsyncOps(expr.expr);
        break;
      case "Call": {
        let fn = (expr.fnName || "").toUpperCase();
        if (fn.includes(".")) fn = fn.split(".").pop()!;
        if (fn === "OBJ_DESCRIPTION" || fn === "COL_DESCRIPTION") {
          res = true;
        } else {
          res =
            (expr.args && expr.args.some((e: any) => this.hasAsyncOps(e))) ||
            (expr.filter && this.hasAsyncOps(expr.filter));
        }
        break;
      }
      case "Case":
        res =
          (expr.cases &&
            expr.cases.some(
              (c: any) => this.hasAsyncOps(c.when) || this.hasAsyncOps(c.then),
            )) ||
          (expr.elseExpr && this.hasAsyncOps(expr.elseExpr));
        break;
      case "Array":
        res = expr.elements && expr.elements.some((e: any) => this.hasAsyncOps(e));
        break;
      default:
        res = false;
    }
    expr._hasAsync = res;
    return res;
  }

  private hasOuterReferences(subquery: any, row: any): boolean {
    if (!row || !subquery) return false;
    const rowKeys = Object.keys(row);
    if (rowKeys.length === 0) return false;

    const localTables = new Set<string>();
    const collectTables = (stmt: any) => {
      if (!stmt) return;
      if (stmt.from) {
        if (stmt.from.tableName) localTables.add(stmt.from.tableName.toLowerCase());
        if (stmt.from.alias) localTables.add(stmt.from.alias.toLowerCase());
        if (stmt.from.stmt) collectTables(stmt.from.stmt);
      }
      if (stmt.joins) {
        for (const j of stmt.joins) {
          if (j.tableName) localTables.add(j.tableName.toLowerCase());
          if (j.alias) localTables.add(j.alias.toLowerCase());
          if (j.stmt) collectTables(j.stmt);
        }
      }
      if (stmt.where) collectSubqueryTables(stmt.where);
    };

    const collectSubqueryTables = (e: any) => {
      if (!e) return;
      if (e.type === "In" && !Array.isArray(e.right)) {
        collectTables(e.right);
      } else if (e.type === "Subquery" && e.stmt) {
        collectTables(e.stmt);
      }
      for (const k in e) {
        if (e[k] && typeof e[k] === "object") collectSubqueryTables(e[k]);
      }
    };

    collectTables(subquery);

    let isCorrelated = false;
    const scanExpr = (e: any) => {
      if (!e || isCorrelated) return;
      if (e.type === "Identifier") {
        if (e.name.includes(".")) {
          const [tbl] = e.name.split(".");
          if (tbl && !localTables.has(tbl.toLowerCase())) {
            isCorrelated = true;
          }
        }
      } else {
        for (const k in e) {
          if (e[k] && typeof e[k] === "object") scanExpr(e[k]);
        }
      }
    };

    if (subquery.where) scanExpr(subquery.where);
    return isCorrelated;
  }

  private getMaxParamIndex(node: any): number {
    let max = 0;
    const scan = (n: any) => {
      if (!n) return;
      if (n.type === "Parameter" && typeof n.index === "number") {
        if (n.index > max) max = n.index;
      }
      for (const k in n) {
        if (n[k] && typeof n[k] === "object") scan(n[k]);
      }
    };
    scan(node);
    return max;
  }

  private extractAggregates(expr: any, aggs: Expr[]) {
    if (!expr) return;
    if (
      expr.type === "Call" &&
      !expr.over &&
      [
        "COUNT",
        "AVG",
        "SUM",
        "MIN",
        "MAX",
        "ARRAY_AGG",
        "JSON_AGG",
        "JSONB_AGG",
        "JSON_OBJECT_AGG",
        "JSONB_OBJECT_AGG",
      ].includes(expr.fnName.toUpperCase())
    ) {
      aggs.push(expr);
      return;
    }
    switch (expr.type) {
      case "Alias":
        this.extractAggregates(expr.expr, aggs);
        break;
      case "Binary":
      case "Logical":
        this.extractAggregates(expr.left, aggs);
        this.extractAggregates(expr.right, aggs);
        break;
      case "Not":
      case "IsNull":
      case "Cast":
      case "Extract":
        this.extractAggregates(expr.expr || expr.source, aggs);
        break;
      case "Call":
        for (const arg of expr.args) this.extractAggregates(arg, aggs);
        if (expr.filter) this.extractAggregates(expr.filter, aggs);
        break;
      case "Case":
        for (const c of expr.cases) {
          this.extractAggregates(c.when, aggs);
          this.extractAggregates(c.then, aggs);
        }
        if (expr.elseExpr) this.extractAggregates(expr.elseExpr, aggs);
        break;
      case "Array":
        for (const e of expr.elements) this.extractAggregates(e, aggs);
        break;
      case "In":
        this.extractAggregates(expr.left, aggs);
        if (Array.isArray(expr.right)) {
          for (const e of expr.right) this.extractAggregates(e, aggs);
        }
        break;
      case "Like":
        this.extractAggregates(expr.left, aggs);
        this.extractAggregates(expr.right, aggs);
        break;
    }
  }

  constructor() {}

  private async rewriteTableData(
    storage: StorageEngine,
    tableName: string,
    schemaModifier: () => Promise<void>,
    rowModifier: (row: any) => Promise<void> | void,
  ) {
    const oldRows = [];
    for await (const row of storage.scanRows(tableName)) {
      oldRows.push(row);
    }

    const visited = new Set<string>();
    const tablesInStmt = new Set<string>([storage.getFullTableName(tableName)]);
    await storage.truncateTable(
      tableName,
      false,
      false,
      visited,
      tablesInStmt,
      true,
    );

    await schemaModifier();

    for (const row of oldRows) {
      await rowModifier(row);
      await storage.insertRow(tableName, row);
    }
  }

  public async execute(
    storage: StorageEngine,
    stmt: Statement,
    params: any = [],
  ): Promise<any> {
    if (Array.isArray(params)) {
      const maxParam = this.getMaxParamIndex(stmt);
      if (maxParam > params.length) {
        throw new Error(
          `bind message supplies ${params.length} parameters, but prepared statement requires at least ${maxParam}`,
        );
      }
    }

    switch (stmt.type) {
      case "Begin":
        storage.begin();
        return { success: true };
      case "Commit":
        await storage.commit();
        return { success: true };
      case "Rollback":
        await storage.rollback();
        return { success: true };

      case "AutoFix":
        const fixRes = await (storage as any).autoFix();
        return fixRes;

      case "Reindex": {
        if (stmt.targetType === "DATABASE") {
          await (storage as any).reindexDatabase();
          return { success: true, message: `Reindex of database completed.` };
        } else if (stmt.targetType === "TABLE" && stmt.targetName) {
          await (storage as any).reindexTable(stmt.targetName);
          return {
            success: true,
            message: `Reindex of table ${stmt.targetName} completed.`,
          };
        }
        return {
          success: true,
          message: `Reindex for ${stmt.targetType} ${stmt.targetName || ""} completed.`,
        };
      }

      case "CreateSchema": {
        await storage.createSchema(stmt.schemaName, stmt.ifNotExists);
        return { success: true, message: `Schema ${stmt.schemaName} created.` };
      }

      case "CreateIndex": {
        await storage.createIndex(
          stmt.indexName,
          stmt.tableName,
          stmt.columns,
          !!stmt.unique,
          !!stmt.ifNotExists,
        );
        return { success: true, message: `Index ${stmt.indexName} created.` };
      }

      case "DropSchema": {
        for (const schemaName of stmt.schemaNames) {
          await storage.dropSchema(schemaName, stmt.ifExists, stmt.cascade);
        }
        return {
          success: true,
          message: `Schemas ${stmt.schemaNames.join(", ")} dropped.`,
        };
      }

      case "DropTable": {
        for (const tableName of stmt.tableNames) {
          await storage.dropTable(tableName, stmt.ifExists, stmt.cascade);
        }
        return {
          success: true,
          message: `Tables ${stmt.tableNames.join(", ")} dropped.`,
        };
      }

      case "DropIndex": {
        for (const indexName of stmt.indexNames) {
          await storage.dropIndex(indexName, !!stmt.ifExists, stmt.cascade);
        }
        return {
          success: true,
          message: `Indexes ${stmt.indexNames.join(", ")} dropped.`,
        };
      }

      case "DropOther": {
        return {
          success: true,
          message: `Ignored DROP ${stmt.objectType} ${stmt.names.join(", ")}.`,
        };
      }

      case "CreateType": {
        let maxOid = 0;
        for await (const r of storage.scanRows("pg_catalog.pg_type")) {
          if (r.oid > maxOid) maxOid = r.oid;
        }
        const typeOid = maxOid + 1;
        let cleanTypeName = stmt.typeName.includes(".")
          ? stmt.typeName.split(".")[1]
          : stmt.typeName;
        cleanTypeName = cleanTypeName.replace(/^"|"$/g, "");

        await storage.insertRow("pg_catalog.pg_type", {
          oid: typeOid,
          typname: cleanTypeName,
          typnamespace: 2200, // public
          typtype: "e",
        });

        let enumMaxOid = 0;
        for await (const r of storage.scanRows("pg_catalog.pg_enum")) {
          if (r.oid > enumMaxOid) enumMaxOid = r.oid;
        }

        for (let i = 0; i < stmt.enumValues.length; i++) {
          await storage.insertRow("pg_catalog.pg_enum", {
            oid: enumMaxOid + 1 + i,
            enumtypid: typeOid,
            enumsortorder: i + 1,
            enumlabel: stmt.enumValues[i],
          });
        }
        return { success: true, message: `Type ${stmt.typeName} created.` };
      }

      case "AlterType": {
        if (stmt.action.type === "AddValue") {
          let cleanTypeName = stmt.typeName.includes(".")
            ? stmt.typeName.split(".")[1]
            : stmt.typeName;
          cleanTypeName = cleanTypeName.replace(/^"|"$/g, "");

          let typeOid = -1;
          for await (const r of storage.scanRows("pg_catalog.pg_type")) {
            if (r.typname === cleanTypeName) {
              typeOid = r.oid;
              break;
            }
          }
          if (typeOid === -1)
            throw new Error(`Type ${stmt.typeName} does not exist`);

          let maxSortOrder = 0;
          let exists = false;
          let enumMaxOid = 0;
          for await (const r of storage.scanRows("pg_catalog.pg_enum")) {
            if (r.oid > enumMaxOid) enumMaxOid = r.oid;
            if (r.enumtypid === typeOid) {
              if (r.enumlabel === stmt.action.value) {
                exists = true;
              }
              if (r.enumsortorder > maxSortOrder) {
                maxSortOrder = r.enumsortorder;
              }
            }
          }
          if (exists) {
            if (stmt.action.ifNotExists)
              return { success: true, message: `Enum value already exists` };
            throw new Error(
              `enum label "${stmt.action.value}" already exists, in type "${stmt.typeName}"`,
            );
          }

          await storage.insertRow("pg_catalog.pg_enum", {
            oid: enumMaxOid + 1,
            enumtypid: typeOid,
            enumsortorder: maxSortOrder + 1,
            enumlabel: stmt.action.value,
          });
          return { success: true, message: `Type ${stmt.typeName} altered.` };
        }
        return { success: true };
      }

      case "AlterTable": {
        const table = await (storage as any).getTableAsync(stmt.tableName);
        if (!table) throw new Error(`Table ${stmt.tableName} not found`);

        const actionsToRun = stmt.actions || [stmt.action];

        for (const action of actionsToRun) {
          if (action.type === "AddColumn") {
            if (table.columns.some((c: any) => c.name === action.column.name)) {
              if (action.ifNotExists) continue;
              throw new Error(
                `Column "${action.column.name}" of relation "${stmt.tableName}" already exists`,
              );
            }
            if (action.column.references) {
              storage.invalidateTableCache(action.column.references.table);
            }

            let maxAttnum = 0;
            for (const c of table.columns) {
              if ((c as any)._attnum > maxAttnum)
                maxAttnum = (c as any)._attnum;
            }
            const nextAttnum = maxAttnum + 1;
            (action.column as any)._attnum = nextAttnum;

            table.columns.push(action.column);
            await storage.updateTableSchema(stmt.tableName, table);

            await storage.insertRow("pg_catalog.pg_attribute", {
              attrelid: table.firstPage,
              attname: action.column.name,
              atttypid: action.column.dataType,
              attnum: nextAttnum,
              attnotnull: !!action.column.isNotNull,
              attprimary: !!action.column.isPrimaryKey,
              attunique: !!action.column.isUnique,
              attref_table: action.column.references?.table || null,
              attref_col: action.column.references?.column || null,
              attref_on_delete: action.column.references?.onDelete || null,
              attref_on_update: action.column.references?.onUpdate || null,
              attdef: action.column.defaultVal
                ? JSON.stringify(action.column.defaultVal)
                : action.column.generatedExpr
                  ? JSON.stringify({
                      __generated__: true,
                      expr: action.column.generatedExpr,
                    })
                  : null,
              atttypmod: -1,
              attisdropped: false,
            });

            if (action.column.defaultVal || action.column.generatedExpr) {
              await storage.insertRow("pg_catalog.pg_attrdef", {
                adrelid: table.firstPage,
                adnum: nextAttnum,
                adbin: action.column.generatedExpr
                  ? JSON.stringify({
                      __generated__: true,
                      expr: action.column.generatedExpr,
                    })
                  : JSON.stringify(action.column.defaultVal),
              });
            }

            if (action.column.defaultVal) {
              await storage.updateRows(
                stmt.tableName,
                async () => true,
                async (row: any) => {
                  row[action.column.name] = await this.evaluateExpr(
                    storage,
                    action.column.defaultVal!,
                    {},
                    params,
                  );
                },
              );
            }
          } else if (action.type === "DropColumn") {
            const colIndex = table.columns.findIndex(
              (c: any) => c.name === action.columnName,
            );
            if (colIndex === -1) {
              if (action.ifExists) continue;
              throw new Error(`Column ${action.columnName} does not exist`);
            }
            const actualAttnum = (table.columns[colIndex] as any)._attnum;

            await this.rewriteTableData(
              storage,
              stmt.tableName,
              async () => {
                table.columns.splice(colIndex, 1);
                await storage.updateTableSchema(stmt.tableName, table);
                await storage.deleteRows(
                  "pg_catalog.pg_attribute",
                  async (r: any) =>
                    r.attrelid === table.firstPage &&
                    r.attname === action.columnName,
                );
                await storage.deleteRows(
                  "pg_catalog.pg_attrdef",
                  async (r: any) =>
                    r.adrelid === table.firstPage && r.adnum === actualAttnum,
                );
              },
              (row) => {
                delete row[action.columnName];
              },
            );
          } else if (action.type === "RenameColumn") {
            const col = table.columns.find(
              (c: any) => c.name === action.oldColumnName,
            );
            if (!col)
              throw new Error(`Column ${action.oldColumnName} does not exist`);
            if (
              table.columns.some((c: any) => c.name === action.newColumnName)
            ) {
              throw new Error(
                `Column "${action.newColumnName}" of relation "${stmt.tableName}" already exists`,
              );
            }

            col.name = action.newColumnName;
            await storage.updateTableSchema(stmt.tableName, table);

            await storage.updateRows(
              "pg_catalog.pg_attribute",
              async (r: any) =>
                r.attrelid === table.firstPage &&
                r.attname === action.oldColumnName,
              async (r: any) => {
                r.attname = action.newColumnName;
              },
            );
          } else if (action.type === "RenameTable") {
            await storage.renameTable(stmt.tableName, action.newTableName);
          } else if (action.type === "AlterColumnType") {
            const col = table.columns.find(
              (c: any) => c.name === action.columnName,
            );
            if (!col)
              throw new Error(`Column ${action.columnName} does not exist`);

            await this.rewriteTableData(
              storage,
              stmt.tableName,
              async () => {
                col.dataType = action.dataType;
                col._isNumeric = undefined;
                col._isBool = undefined;
                col._isJson = undefined;
                col._isSerial = undefined;
                await storage.updateTableSchema(stmt.tableName, table);
                await storage.updateRows(
                  "pg_catalog.pg_attribute",
                  async (r: any) =>
                    r.attrelid === table.firstPage &&
                    r.attname === action.columnName,
                  async (r: any) => {
                    r.atttypid = action.dataType;
                  },
                );
              },
              async (row) => {
                if (
                  row[action.columnName] !== undefined &&
                  row[action.columnName] !== null
                ) {
                  row[action.columnName] = await this.castValue(
                    storage,
                    row[action.columnName],
                    action.dataType,
                  );
                }
              },
            );
          } else if (action.type === "AlterColumnSetDefault") {
            const colIndex = table.columns.findIndex(
              (c: any) => c.name === action.columnName,
            );
            const col = table.columns[colIndex];
            if (!col)
              throw new Error(`Column ${action.columnName} does not exist`);
            const actualAttnum = (col as any)._attnum;
            col.defaultVal = action.defaultVal;
            await storage.updateTableSchema(stmt.tableName, table);

            const strDef = JSON.stringify(action.defaultVal);
            await storage.updateRows(
              "pg_catalog.pg_attribute",
              async (r: any) =>
                r.attrelid === table.firstPage &&
                r.attname === action.columnName,
              async (r: any) => {
                r.attdef = strDef;
              },
            );

            let updatedAd = false;
            await storage.updateRows(
              "pg_catalog.pg_attrdef",
              async (r: any) =>
                r.adrelid === table.firstPage && r.adnum === actualAttnum,
              async (r: any) => {
                r.adbin = strDef;
                updatedAd = true;
              },
            );
            if (!updatedAd) {
              await storage.insertRow("pg_catalog.pg_attrdef", {
                adrelid: table.firstPage,
                adnum: actualAttnum,
                adbin: strDef,
              });
            }
          } else if (action.type === "AlterColumnDropDefault") {
            const colIndex = table.columns.findIndex(
              (c: any) => c.name === action.columnName,
            );
            const col = table.columns[colIndex];
            if (!col)
              throw new Error(`Column ${action.columnName} does not exist`);
            const actualAttnum = (col as any)._attnum;
            delete col.defaultVal;
            await storage.updateTableSchema(stmt.tableName, table);

            await storage.updateRows(
              "pg_catalog.pg_attribute",
              async (r: any) =>
                r.attrelid === table.firstPage &&
                r.attname === action.columnName,
              async (r: any) => {
                r.attdef = null;
              },
            );
            await storage.deleteRows(
              "pg_catalog.pg_attrdef",
              async (r: any) =>
                r.adrelid === table.firstPage && r.adnum === actualAttnum,
            );
          } else if (action.type === "AlterColumnSetNotNull") {
            const col = table.columns.find(
              (c: any) => c.name === action.columnName,
            );
            if (!col)
              throw new Error(`Column ${action.columnName} does not exist`);
            col.isNotNull = true;
            await storage.updateTableSchema(stmt.tableName, table);
            await storage.updateRows(
              "pg_catalog.pg_attribute",
              async (r: any) =>
                r.attrelid === table.firstPage &&
                r.attname === action.columnName,
              async (r: any) => {
                r.attnotnull = true;
              },
            );
          } else if (action.type === "AlterColumnDropNotNull") {
            const col = table.columns.find(
              (c: any) => c.name === action.columnName,
            );
            if (!col)
              throw new Error(`Column ${action.columnName} does not exist`);
            col.isNotNull = false;
            await storage.updateTableSchema(stmt.tableName, table);
            await storage.updateRows(
              "pg_catalog.pg_attribute",
              async (r: any) =>
                r.attrelid === table.firstPage &&
                r.attname === action.columnName,
              async (r: any) => {
                r.attnotnull = false;
              },
            );
          } else if (action.type === "AddUniqueConstraint") {
            // Verify no duplicate data exists
            const rows = [];
            for await (const row of storage.scanRows(stmt.tableName))
              rows.push(row);

            const keySet = new Set();
            for (const row of rows) {
              const vals = action.columns.map((c) => row[c]);
              if (vals.some((v) => v === null || v === undefined)) continue;
              const key = JSON.stringify(vals);
              if (keySet.has(key)) {
                throw new Error(
                  `Constraint Error: data contains duplicate values for unique constraint ${action.constraintName || "UNIQUE"}`,
                );
              }
              keySet.add(key);
            }

            for (const c of action.columns) {
              const col = table.columns.find((col: any) => col.name === c);
              if (!col) throw new Error(`Column ${c} does not exist`);
              col.isUnique = true;
            }
            await storage.updateTableSchema(stmt.tableName, table);

            for (const c of action.columns) {
              await storage.updateRows(
                "pg_catalog.pg_attribute",
                async (r: any) =>
                  r.attrelid === table.firstPage && r.attname === c,
                async (r: any) => {
                  r.attunique = true;
                },
              );
            }

            const tableShortName = stmt.tableName.includes(".")
              ? stmt.tableName.split(".").pop()!
              : stmt.tableName;
            const constraintName =
              action.constraintName ||
              `${tableShortName}_${action.columns.join("_")}_key`;
            await storage.createIndex(
              constraintName,
              stmt.tableName,
              action.columns,
              true,
              true,
            );
          } else if (action.type === "AddPrimaryKeyConstraint") {
            // Verify no null or duplicate
            const rows = [];
            for await (const row of storage.scanRows(stmt.tableName))
              rows.push(row);

            const keySet = new Set();
            for (const row of rows) {
              const vals = action.columns.map((c) => row[c]);
              if (vals.some((v) => v === null || v === undefined)) {
                throw new Error(
                  `Constraint Error: Primary key columns cannot be null`,
                );
              }
              const key = JSON.stringify(vals);
              if (keySet.has(key)) {
                throw new Error(
                  `Constraint Error: data contains duplicate values for primary key constraint ${action.constraintName || "PRIMARY KEY"}`,
                );
              }
              keySet.add(key);
            }

            for (const c of action.columns) {
              const col = table.columns.find((col: any) => col.name === c);
              if (!col) throw new Error(`Column ${c} does not exist`);
              col.isPrimaryKey = true;
              col.isNotNull = true;
            }
            table.pkColumn =
              action.columns.length === 1 ? action.columns[0] : null;
            await storage.updateTableSchema(stmt.tableName, table);

            for (const c of action.columns) {
              await storage.updateRows(
                "pg_catalog.pg_attribute",
                async (r: any) =>
                  r.attrelid === table.firstPage && r.attname === c,
                async (r: any) => {
                  r.attprimary = true;
                  r.attnotnull = true;
                },
              );
            }

            await (storage as any).addPrimaryKeyIndex(
              stmt.tableName,
              action.columns,
            );
          } else if (action.type === "AddForeignKey") {
            const col = table.columns.find(
              (c: any) => c.name === action.columnName,
            );
            if (!col)
              throw new Error(`Column ${action.columnName} does not exist`);
            col.references = action.references;
            storage.invalidateTableCache(action.references.table);
            await storage.updateTableSchema(stmt.tableName, table);

            await storage.updateRows(
              "pg_catalog.pg_attribute",
              async (r: any) =>
                r.attrelid === table.firstPage &&
                r.attname === action.columnName,
              async (r: any) => {
                r.attref_table = action.references.table;
                r.attref_col = action.references.column;
                r.attref_on_delete = action.references.onDelete || null;
                r.attref_on_update = action.references.onUpdate || null;
              },
            );
          } else if (action.type === "DropConstraint") {
            let dropped = false;
            const tableShortName = stmt.tableName.includes(".")
              ? stmt.tableName.split(".").pop()!
              : stmt.tableName;
            for (const col of table.columns) {
              if (action.constraintName === `${tableShortName}_pkey`) {
                if (col.isPrimaryKey) {
                  col.isPrimaryKey = false;
                  dropped = true;
                }
              } else if (
                action.constraintName === `${tableShortName}_${col.name}_key`
              ) {
                if (col.isUnique) {
                  col.isUnique = false;
                  dropped = true;
                }
              } else if (
                action.constraintName === `${tableShortName}_${col.name}_fkey`
              ) {
                if (col.references) {
                  col.references = undefined;
                  dropped = true;
                }
              }
            }
            if (!dropped && !action.ifExists) {
              throw new Error(
                `Constraint ${action.constraintName} does not exist`,
              );
            }
            if (dropped) {
              await storage.updateTableSchema(stmt.tableName, table);
              await storage.updateRows(
                "pg_catalog.pg_attribute",
                async (r: any) => r.attrelid === table.firstPage,
                async (r: any) => {
                  const c = table.columns.find(
                    (col: any) => col.name === r.attname,
                  );
                  if (c) {
                    r.attprimary = !!c.isPrimaryKey;
                    r.attunique = !!c.isUnique;
                    if (!c.references) {
                      r.attref_table = null;
                      r.attref_col = null;
                      r.attref_on_delete = null;
                      r.attref_on_update = null;
                    }
                  }
                },
              );
            }
          }
        }

        return { success: true };
      }

      case "CreateTable":
        await storage.createTable(
          stmt.tableName,
          stmt.columns,
          stmt.ifNotExists,
          stmt.tableConstraints,
        );
        return { success: true, message: `Table ${stmt.tableName} created.` };

      case "Insert": {
        const table = await (storage as any).getTableAsync(stmt.tableName);
        if (!table) throw new Error(`Table ${stmt.tableName} not found`);

        const insertedRecords = [];
        const returningRecords = [];
        let insertedCount = 0;
        let schemaUpdated = false;
        const verifiedFKs = new Set<string>();

        let insertRows: any[] = [];

        if (stmt.select) {
          const selectStream = this.executeSelect(storage, stmt.select, params);
          for await (const row of selectStream) {
            insertRows.push(row);
          }
        } else if (stmt.values) {
          const valuesList: Expr[][] =
            stmt.values.length > 0 && Array.isArray(stmt.values[0])
              ? (stmt.values as Expr[][])
              : [stmt.values as Expr[]];

          const colsToUse =
            stmt.columns && stmt.columns.length > 0
              ? stmt.columns
              : table.columns.map((c: any) => c.name);

          for (const rowVals of valuesList) {
            const record: any = {};
            for (let idx = 0; idx < colsToUse.length; idx++) {
              if (!rowVals[idx]) record[colsToUse[idx]!] = undefined;
              else
                record[colsToUse[idx]!] = await this.evaluateExpr(
                  storage,
                  rowVals[idx]!,
                  {},
                  params,
                );
            }
            insertRows.push(record);
          }
        }

        for (const recordData of insertRows) {
          const record: any = {};

          // 1. Map values and identifiers
          startBenchmarks();
          if (stmt.select) {
            const selectKeys = Object.keys(recordData).filter(
              (k) => !k.startsWith("__"),
            );
            let idx = 0;
            for (const k of selectKeys) {
              if (stmt.columns && stmt.columns.length > 0) {
                if (idx < stmt.columns.length) {
                  record[stmt.columns[idx]!] = recordData[k];
                }
              } else if (idx < table.columns.length) {
                const targetCol = table.columns[idx]?.name;
                if (targetCol) {
                  record[targetCol] = recordData[k];
                }
              }
              idx++;
            }
          } else {
            Object.assign(record, recordData);
          }
          endBenchmarks("expr_evaluation");

          // 2. Apply Serials and Defaults before checking constraints
          startBenchmarks();
          for (let i = 0; i < table.columns.length; i++) {
            const col = table.columns[i];
            if (col._isSerial === undefined) {
              const dt = col.dataType ? col.dataType.toUpperCase() : "";
              const isIdCol = col.name.toLowerCase() === "id" || col.name.toLowerCase() === "_id";
              col._isSerial =
                dt === "SERIAL" || dt === "BIGSERIAL" || dt === "SMALLSERIAL" || dt.includes("SERIAL")
                || (col.isPrimaryKey && (dt.includes("INT") || dt === "NUMBER" || dt === ""))
                || (isIdCol && (col.isPrimaryKey || dt.includes("INT") || dt === "SERIAL" || dt === "NUMBER" || dt === ""));
            }
            if (col._isSerial && (record[col.name] === undefined || record[col.name] === null)) {
              table.sequence = (table.sequence || 0) + 1;
              schemaUpdated = true;
              record[col.name] = table.sequence;
            } else if (col._isSerial && record[col.name] !== undefined && record[col.name] !== null) {
              const valNum = Number(record[col.name]);
              if (!isNaN(valNum) && valNum >= (table.sequence || 0)) {
                table.sequence = valNum;
              }
            }
            if (record[col.name] === undefined && col.defaultVal) {
              record[col.name] = await this.evaluateExpr(
                storage,
                col.defaultVal,
                {},
                params,
              );
            }
          }
          // 2b. Evaluate generated columns
          for (let i = 0; i < table.columns.length; i++) {
            const col = table.columns[i];
            if ((col as any).generatedExpr) {
              record[col.name] = await this.evaluateExpr(
                storage,
                (col as any).generatedExpr,
                record,
                params,
              );
            }
          }

          // 2c. Cast values to column types
          for (let i = 0; i < table.columns.length; i++) {
            const col = table.columns[i];
            if (record[col.name] !== undefined && record[col.name] !== null) {
              record[col.name] = await this.castValue(
                storage,
                record[col.name],
                col.dataType,
              );
            }
          }
          endBenchmarks("serial_default_assignment");

          // 3. Unique/Conflict Checking
          startBenchmarks();
          let conflictRow = null;
          const pkColumnsForCheck = table.columns.filter(
            (c: any) => c.isPrimaryKey,
          );

          if (pkColumnsForCheck.length > 1) {
            // Composite primary key: must be checked as a combined tuple,
            // not column-by-column (matching Postgres semantics).
            const hasAllPkValues = pkColumnsForCheck.every(
              (c: any) =>
                record[c.name] !== undefined && record[c.name] !== null,
            );
            if (hasAllPkValues) {
              for await (const r of storage.scanRows(stmt.tableName)) {
                if (
                  pkColumnsForCheck.every(
                    (c: any) => r[c.name] == record[c.name],
                  )
                ) {
                  conflictRow = r;
                  break;
                }
              }
              if (conflictRow && !stmt.onConflict) {
                const pkNames = pkColumnsForCheck
                  .map((c: any) => c.name)
                  .join(", ");
                throw new Error(
                  `Constraint Error: (${pkNames}) must be unique`,
                );
              }
            }
          }

          for (const col of table.columns) {
            if (conflictRow) break;
            if (col.isPrimaryKey && pkColumnsForCheck.length > 1) continue; // handled above as a group
            if (
              (col.isUnique || col.isPrimaryKey) &&
              record[col.name] !== undefined &&
              record[col.name] !== null
            ) {
              let existing = null;
              if (col.isPrimaryKey) {
                existing = await storage.getRowByPK(
                  stmt.tableName,
                  record[col.name],
                );
              } else if (col.isUnique) {
                for await (const r of storage.scanRows(stmt.tableName)) {
                  if (r[col.name] == record[col.name]) {
                    existing = r;
                    break;
                  }
                }
              }
              if (existing) {
                if (stmt.onConflict) {
                  conflictRow = existing;
                  break;
                }
                throw new Error(`Constraint Error: ${col.name} must be unique`);
              }
            }
          }
          endBenchmarks("conflict_checking");

          // 4. Resolve Conflict if any
          startBenchmarks();
          if (conflictRow && stmt.onConflict) {
            if (stmt.onConflict.action === "NOTHING") {
              continue; // Skip this row
            } else {
              // DO UPDATE SET
              const pkColName = await storage.getPKColumn(stmt.tableName);
              if (!pkColName)
                throw new Error(
                  "ON CONFLICT UPDATE requires a Primary Key for identification",
                );
              const pkVal = conflictRow[pkColName];

              const updatedRows: any[] = [];
              await storage.updateRows(
                stmt.tableName,
                async (r) => r[pkColName] == pkVal,
                async (r) => {
                  const evalContext = { ...r, excluded: record };
                  for (const [col, expr] of Object.entries(
                    stmt.onConflict!.assignments!,
                  )) {
                    let newVal = await this.evaluateExpr(
                      storage,
                      expr,
                      evalContext,
                      params,
                    );
                    const colDef = table.columns.find(
                      (c: any) => c.name === col,
                    );
                    if (colDef && newVal !== undefined && newVal !== null) {
                      newVal = await this.castValue(
                        storage,
                        newVal,
                        colDef.dataType,
                      );
                    }
                    r[col] = newVal;
                  }
                  if (stmt.returning) {
                    updatedRows.push(
                      await this.projectRow(storage, r, stmt.returning, params),
                    );
                  }
                },
              );
              if (stmt.returning) returningRecords.push(...updatedRows);
              continue;
            }
          }
          endBenchmarks("conflict_resolution");

          // 5. Normal Insert Path (Constraint checks and actual IO)
          startBenchmarks();
          for (const col of table.columns) {
            if (
              col.isNotNull &&
              (record[col.name] === null || record[col.name] === undefined)
            )
              throw new Error(`Constraint Error: ${col.name} cannot be null`);

            if (
              col.references &&
              record[col.name] !== undefined &&
              record[col.name] !== null
            ) {
              const fkKey = `${col.references.table}:${col.references.column}:${record[col.name]}`;
              let exists = verifiedFKs.has(fkKey);
              if (!exists) {
                // Optimization: Use O(log N) index lookup if referenced column is the Primary Key
                const refPK = await storage.getPKColumn(col.references.table);
                if (refPK === col.references.column) {
                  const refRow = await storage.getRowByPK(
                    col.references.table,
                    record[col.name],
                  );
                  if (refRow) exists = true;
                }
                if (!exists) {
                  for await (const r of storage.scanRows(col.references.table)) {
                    if (r[col.references.column] == record[col.name]) {
                      exists = true;
                      break;
                    }
                  }
                }
                if (exists) verifiedFKs.add(fkKey);
              }
              if (!exists) {
                const errorDetails = [
                  `Foreign Key Violation (Insert): insert on table "${stmt.tableName}" violates foreign key constraint.`,
                  `- Source Column: "${col.name}"`,
                  `- Source Value: ${JSON.stringify(record[col.name])}`,
                  `- Target Table: "${col.references.table}"`,
                  `- Target Column: "${col.references.column}"`,
                  `Reason: The value ${JSON.stringify(record[col.name])} was not found in the referenced column "${col.references.column}" of table "${col.references.table}".`,
                ].join("\n");
                throw new Error(errorDetails);
              }
            }
          }
          endBenchmarks("constraint_checks");

          await storage.insertRow(stmt.tableName, record);
          insertedCount++;
          if (stmt.returning || insertedRecords.length < 500) {
            insertedRecords.push(record);
          }

          startBenchmarks();
          if (stmt.returning) {
            returningRecords.push(
              await this.projectRow(storage, record, stmt.returning, params),
            );
          }
          endBenchmarks("returning_projection");
        }

        if (schemaUpdated)
          await storage.updateTableSchema(stmt.tableName, table);

        if (stmt.returning) return returningRecords;

        if (
          insertedCount === 0 &&
          stmt.onConflict?.action === "NOTHING"
        ) {
          return { success: true, conflict: "nothing" };
        }

        return {
          success: true,
          inserted:
            insertedCount === 1 
              ? insertedRecords[0] 
              : (insertedCount <= 500 ? insertedRecords : insertedCount),
          rowCount: insertedCount,
        };
      }

      case "Select": {
        const rows = [];
        for await (const row of this.executeSelect(storage, stmt, params)) {
          rows.push(row);
        }

        let fields: { name: string }[] = [];
        if (rows.length > 0) {
          fields = Object.keys(rows[0]).map((k) => ({ name: k }));
        } else {
          for (const col of stmt.columns) {
            if (col.type === "Alias") {
              fields.push({ name: col.alias });
            } else if (col.type === "Identifier") {
              if (col.name === "*") {
                if (stmt.from && stmt.from.tableName) {
                  const tableInfo = await (storage as any).getTableAsync(
                    stmt.from.tableName,
                  );
                  if (tableInfo) {
                    for (const c of tableInfo.columns)
                      fields.push({ name: c.name });
                  }
                }
              } else if (col.name.endsWith(".*")) {
                const prefix = col.name.substring(0, col.name.length - 2);
                let targetTable = prefix;
                if (
                  stmt.from &&
                  stmt.from.alias === prefix &&
                  stmt.from.tableName
                )
                  targetTable = stmt.from.tableName;
                if (stmt.joins) {
                  for (const j of stmt.joins) {
                    if (j.alias === prefix && j.tableName)
                      targetTable = j.tableName;
                  }
                }
                if (targetTable) {
                  const tableInfo = await (storage as any).getTableAsync(
                    targetTable,
                  );
                  if (tableInfo) {
                    for (const c of tableInfo.columns)
                      fields.push({ name: c.name });
                  }
                }
              } else {
                const name = col.name.includes(".")
                  ? col.name.split(".")[1]
                  : col.name;
                fields.push({ name });
              }
            } else if (col.type === "Call") {
              fields.push({ name: col.fnName.toLowerCase() });
            } else {
              fields.push({ name: "col" });
            }
          }

          const finalFields: { name: string }[] = [];
          const seen = new Set<string>();
          for (const f of fields) {
            let outKey = f.name;
            if (seen.has(outKey)) {
              let suffix = 1;
              while (seen.has(`${outKey}${suffix}`)) suffix++;
              outKey = `${outKey}${suffix}`;
            }
            seen.add(outKey);
            finalFields.push({ name: outKey });
          }
          fields = finalFields;
        }

        return { rows, fields };
      }

      case "Values": {
        const rows = [];
        for await (const row of this.executeSelect(storage, stmt, params)) {
          rows.push(row);
        }
        const fields =
          rows.length > 0 ? Object.keys(rows[0]).map((k) => ({ name: k })) : [];
        return { rows, fields };
      }

      case "Update": {
        const table = await (storage as any).getTableAsync(stmt.tableName);
        if (!table) throw new Error(`Table ${stmt.tableName} not found`);

        const referencingCols = await storage.getReferencingColumns(
          stmt.tableName,
        );
        const updatedRows: any[] = [];

        let sourceStream: AsyncIterableIterator<any> | null = null;

        // If we have FROM or JOINs in UPDATE, we must evaluate them
        if (stmt.from || (stmt.joins && stmt.joins.length > 0)) {
          // We will map the base table into a stream
          sourceStream = this.mapStream(
            storage.scanRows(stmt.tableName),
            (r) => {
              const tblCopy = this.getTableCopy(r);
              r["__lpg_tbl_" + stmt.tableName] = tblCopy;
              if (stmt.alias) r["__lpg_tbl_" + stmt.alias] = tblCopy;
              return r;
            },
          );

          if (stmt.from) {
            // Treat FROM as a CROSS JOIN
            const fromJoin: JoinClause = {
              type: "CROSS",
              tableName: stmt.from.tableName,
              stmt: stmt.from.stmt,
              fn: stmt.from.fn,
              withOrdinality: stmt.from.withOrdinality,
              columnAliases: stmt.from.columnAliases,
              alias: stmt.from.alias,
              on: { type: "Literal", value: true },
            };
            if (fromJoin.lateral || fromJoin.fn || fromJoin.stmt) {
              sourceStream = this.nestedLoopJoinStream(
                storage,
                sourceStream,
                fromJoin,
                params,
              );
            } else {
              const rightRows = [];
              for await (const r of storage.scanRows(fromJoin.tableName!)) {
                const tblCopy = this.getTableCopy(r);
                r["__lpg_tbl_" + fromJoin.tableName!] = tblCopy;
                if (fromJoin.alias) r["__lpg_tbl_" + fromJoin.alias] = tblCopy;
                rightRows.push(r);
              }
              sourceStream = this.hashJoinStream(
                storage,
                sourceStream,
                rightRows,
                fromJoin,
                params,
              );
            }
          }

          if (stmt.joins) {
            for (const join of stmt.joins) {
              if (join.lateral || join.fn || join.stmt) {
                sourceStream = this.nestedLoopJoinStream(
                  storage,
                  sourceStream,
                  join,
                  params,
                );
              } else {
                const rightRows = [];
                for await (const r of storage.scanRows(join.tableName!)) {
                  const tblCopy = this.getTableCopy(r);
                  r["__lpg_tbl_" + join.tableName!] = tblCopy;
                  if (join.alias) r["__lpg_tbl_" + join.alias] = tblCopy;
                  rightRows.push(r);
                }
                sourceStream = this.hashJoinStream(
                  storage,
                  sourceStream,
                  rightRows,
                  join,
                  params,
                );
              }
            }
          }
        }

        let updatedCount = 0;

        if (sourceStream) {
          // We have a complex stream of joined rows.
          // However, storage.updateRows only iterates the base table.
          // To apply the matched joined contexts to the base table:
          // 1. Gather all matched rows in memory.
          const matchedRows = [];
          for await (const r of sourceStream) {
            if (
              !stmt.where ||
              (await this.evaluateExpr(storage, stmt.where, r, params))
            ) {
              matchedRows.push(r);
            }
          }

          // We need to identify rows in the base table by their primary key if possible,
          // or we have to match them completely.
          // Since we don't have CTID, we'll try to find the row by PK.
          const pkColName = await storage.getPKColumn(stmt.tableName);
          if (!pkColName) {
            throw new Error(
              "UPDATE ... FROM requires a PRIMARY KEY on the target table for row identification",
            );
          }

          const pkValuesToUpdate = new Map<any, any>();
          for (const r of matchedRows) {
            const baseTblObj = r["__lpg_tbl_" + (stmt.alias || stmt.tableName)];
            const pkVal = baseTblObj[pkColName];
            if (!pkValuesToUpdate.has(pkVal)) {
              pkValuesToUpdate.set(pkVal, r);
            }
          }

          updatedCount = await storage.updateRows(
            stmt.tableName,
            async (baseRow) => pkValuesToUpdate.has(baseRow[pkColName]),
            async (baseRow) => {
              const pkVal = baseRow[pkColName];
              const joinContext = pkValuesToUpdate.get(pkVal);
              // We pass the joinContext (which has __lpg_tbl_* and flat fields) to evaluateExpr
              // But wait, the assignments evaluate on `row`.
              // So we need to evaluate the assignments using `joinContext`,
              // and apply them to `baseRow`.

              const oldRow = { ...baseRow };
              for (const [colName, expr] of Object.entries(stmt.assignments)) {
                let newVal = await this.evaluateExpr(
                  storage,
                  expr,
                  joinContext,
                  params,
                );

                const colDef = table.columns.find(
                  (c: any) => c.name === colName,
                );
                if (colDef && newVal !== undefined && newVal !== null) {
                  newVal = await this.castValue(
                    storage,
                    newVal,
                    colDef.dataType,
                  );
                }

                if (
                  colDef?.references &&
                  newVal !== undefined &&
                  newVal !== null
                ) {
                  let exists = false;
                  const refPK = await storage.getPKColumn(
                    colDef.references.table,
                  );
                  if (refPK === colDef.references.column) {
                    const refRow = await storage.getRowByPK(
                      colDef.references.table,
                      newVal,
                    );
                    if (refRow) exists = true;
                  }
                  if (!exists) {
                    for await (const r of storage.scanRows(
                      colDef.references.table,
                    )) {
                      if (r[colDef.references.column] == newVal) {
                        exists = true;
                        break;
                      }
                    }
                  }
                  if (!exists) {
                    const errorDetails = [
                      `Foreign Key Violation (Update): update on table "${stmt.tableName}" violates foreign key constraint.`,
                      `- Source Column: "${colDef.name}"`,
                      `- New Source Value: ${JSON.stringify(newVal)}`,
                      `- Target Table: "${colDef.references.table}"`,
                      `- Target Column: "${colDef.references.column}"`,
                    ].join("\n");
                    throw new Error(errorDetails);
                  }
                }

                const oldVal = oldRow[colName];
                if (newVal !== oldVal) {
                  for (const ref of referencingCols) {
                    if (ref.parentColumn === colName) {
                      const action = ref.onUpdate;
                      const childrenExist = async () => {
                        const childPK = await storage.getPKColumn(
                          ref.childTable,
                        );
                        if (childPK === ref.childColumn) {
                          const r = await storage.getRowByPK(
                            ref.childTable,
                            oldVal,
                          );
                          if (r) return true;
                        }
                        for await (const r of storage.scanRows(
                          ref.childTable,
                        )) {
                          if (r[ref.childColumn] == oldVal) return true;
                        }
                        return false;
                      };

                      if (await childrenExist()) {
                        if (action === "RESTRICT" || action === "NO ACTION") {
                          throw new Error(
                            `Foreign Key Violation (Update RESTRICT)`,
                          );
                        } else if (action === "CASCADE") {
                          await storage.updateRows(
                            ref.childTable,
                            async (r) => r[ref.childColumn] == oldVal,
                            async (r) => {
                              r[ref.childColumn] = newVal;
                            },
                          );
                        } else if (action === "SET NULL") {
                          await storage.updateRows(
                            ref.childTable,
                            async (r) => r[ref.childColumn] == oldVal,
                            async (r) => {
                              r[ref.childColumn] = null;
                            },
                          );
                        }
                      }
                    }
                  }
                }
                baseRow[colName] = newVal;
              }

              for (const col of table.columns) {
                if (
                  (col.isUnique || col.isPrimaryKey) &&
                  baseRow[col.name] !== undefined &&
                  baseRow[col.name] !== null
                ) {
                  if (baseRow[col.name] !== oldRow[col.name]) {
                    let existing = null;
                    if (col.isPrimaryKey)
                      existing = await storage.getRowByPK(
                        stmt.tableName,
                        baseRow[col.name],
                      );
                    if (!existing) {
                      for await (const r of storage.scanRows(stmt.tableName)) {
                        if (r[col.name] == baseRow[col.name]) {
                          existing = r;
                          break;
                        }
                      }
                    }
                    if (existing)
                      throw new Error(
                        `Constraint Error: ${col.name} must be unique`,
                      );
                  }
                }
              }

              for (const col of table.columns) {
                if ((col as any).generatedExpr) {
                  baseRow[col.name] = await this.evaluateExpr(
                    storage,
                    (col as any).generatedExpr,
                    baseRow,
                    params,
                  );
                }
              }

              if (stmt.returning) {
                updatedRows.push(
                  await this.projectRow(
                    storage,
                    baseRow,
                    stmt.returning,
                    params,
                  ),
                );
              }
            },
          );
        } else {
          // Normal update (no FROM)
          const pkColName = await storage.getPKColumn(stmt.tableName);
          let pkExpr: Expr | null = null;
          let pkVal: any = undefined;

          if (pkColName && stmt.where) {
            const findPkCondition = (expr: Expr): Expr | null => {
              if (expr.type === "Binary" && expr.operator === "=") {
                if (
                  expr.left.type === "Identifier" &&
                  expr.left.name === pkColName &&
                  (expr.right.type === "Literal" ||
                    expr.right.type === "Parameter" ||
                    expr.right.type === "Identifier")
                ) {
                  return expr;
                }
              } else if (expr.type === "Logical" && expr.operator === "AND") {
                return (
                  findPkCondition(expr.left) || findPkCondition(expr.right)
                );
              }
              return null;
            };

            pkExpr = findPkCondition(stmt.where);
            if (pkExpr && pkExpr.type === "Binary") {
              pkVal = await this.evaluateExpr(
                storage,
                pkExpr.right,
                {},
                params,
              );
              const tableInfo = await (storage as any).getTableAsync(
                stmt.tableName,
              );
              const pkCol = tableInfo?.columns.find(
                (c: any) => c.name === pkColName,
              );
              if (pkCol) {
                pkVal = await this.castValue(storage, pkVal, pkCol.dataType);
              }
            }
          }

          const localUpdateFn = async (row: any) => {
            const oldRow = { ...row };
            for (const [colName, expr] of Object.entries(stmt.assignments)) {
              let newVal = await this.evaluateExpr(storage, expr, row, params);

              const colDef = table.columns.find((c: any) => c.name === colName);
              if (colDef && newVal !== undefined && newVal !== null) {
                newVal = await this.castValue(storage, newVal, colDef.dataType);
              }

              if (
                colDef?.references &&
                newVal !== undefined &&
                newVal !== null
              ) {
                let exists = false;
                const refPK = await storage.getPKColumn(
                  colDef.references.table,
                );
                if (refPK === colDef.references.column) {
                  const refRow = await storage.getRowByPK(
                    colDef.references.table,
                    newVal,
                  );
                  if (refRow) exists = true;
                }
                if (!exists) {
                  for await (const r of storage.scanRows(
                    colDef.references.table,
                  )) {
                    if (r[colDef.references.column] == newVal) {
                      exists = true;
                      break;
                    }
                  }
                }
                if (!exists) {
                  const errorDetails = [
                    `Foreign Key Violation (Update): update on table "${stmt.tableName}" violates foreign key constraint.`,
                    `- Source Column: "${colDef.name}"`,
                    `- New Source Value: ${JSON.stringify(newVal)}`,
                    `- Target Table: "${colDef.references.table}"`,
                    `- Target Column: "${colDef.references.column}"`,
                  ].join("\n");
                  throw new Error(errorDetails);
                }
              }

              const oldVal = oldRow[colName];
              if (newVal !== oldVal) {
                for (const ref of referencingCols) {
                  if (ref.parentColumn === colName) {
                    const action = ref.onUpdate;
                    const childrenExist = async () => {
                      const childPK = await storage.getPKColumn(ref.childTable);
                      if (childPK === ref.childColumn) {
                        const r = await storage.getRowByPK(
                          ref.childTable,
                          oldVal,
                        );
                        if (r) return true;
                      }
                      for await (const r of storage.scanRows(ref.childTable)) {
                        if (r[ref.childColumn] == oldVal) return true;
                      }
                      return false;
                    };

                    if (await childrenExist()) {
                      if (action === "RESTRICT" || action === "NO ACTION") {
                        throw new Error(
                          `Foreign Key Violation (Update RESTRICT)`,
                        );
                      } else if (action === "CASCADE") {
                        await storage.updateRows(
                          ref.childTable,
                          async (r) => r[ref.childColumn] == oldVal,
                          async (r) => {
                            r[ref.childColumn] = newVal;
                          },
                        );
                      } else if (action === "SET NULL") {
                        await storage.updateRows(
                          ref.childTable,
                          async (r) => r[ref.childColumn] == oldVal,
                          async (r) => {
                            r[ref.childColumn] = null;
                          },
                        );
                      }
                    }
                  }
                }
              }

              row[colName] = newVal;
            }

            for (const col of table.columns) {
              if (
                (col.isUnique || col.isPrimaryKey) &&
                row[col.name] !== undefined &&
                row[col.name] !== null
              ) {
                if (row[col.name] !== oldRow[col.name]) {
                  let existing = null;
                  if (col.isPrimaryKey)
                    existing = await storage.getRowByPK(
                      stmt.tableName,
                      row[col.name],
                    );
                  if (!existing) {
                    for await (const r of storage.scanRows(stmt.tableName)) {
                      if (r[col.name] == row[col.name]) {
                        existing = r;
                        break;
                      }
                    }
                  }
                  if (existing)
                    throw new Error(
                      `Constraint Error: ${col.name} must be unique`,
                    );
                }
              }
            }

            for (const col of table.columns) {
              if ((col as any).generatedExpr) {
                row[col.name] = await this.evaluateExpr(
                  storage,
                  (col as any).generatedExpr,
                  row,
                  params,
                );
              }
            }

            if (stmt.returning) {
              updatedRows.push(
                await this.projectRow(storage, row, stmt.returning, params),
              );
            }
          };

          if (pkVal !== undefined && pkVal !== null) {
            updatedCount = await storage.updateRowByPK(
              stmt.tableName,
              pkVal,
              async (row: any) =>
                !stmt.where ||
                (await this.evaluateExpr(storage, stmt.where, row, params)),
              localUpdateFn,
            );
            if (updatedCount === 0) {
              updatedCount = await storage.updateRows(
                stmt.tableName,
                async (row: any) => {
                  if (row[pkColName!] != pkVal) return false;
                  return (
                    !stmt.where ||
                    (await this.evaluateExpr(storage, stmt.where, row, params))
                  );
                },
                localUpdateFn,
              );
            }
          } else {
            updatedCount = await storage.updateRows(
              stmt.tableName,
              async (row: any) =>
                !stmt.where ||
                (await this.evaluateExpr(storage, stmt.where, row, params)),
              localUpdateFn,
            );
          }
        }

        if (stmt.returning) return updatedRows;
        return { success: true, updated: updatedCount };
      }

      case "Truncate": {
        const visited = new Set<string>();
        const tableNamesToTruncate = new Set(
          stmt.tableNames.map((t) => storage.getFullTableName(t)),
        );
        for (const tableName of stmt.tableNames) {
          await storage.truncateTable(
            tableName,
            stmt.cascade,
            stmt.restartIdentity,
            visited,
            tableNamesToTruncate,
          );
        }
        return {
          success: true,
          message: `Truncated ${stmt.tableNames.join(", ")}`,
        };
      }

      case "Delete": {
        const referencingCols = await storage.getReferencingColumns(
          stmt.tableName,
        );
        const deletedRows: any[] = [];

        const filterFn = async (row: any) => {
          const match =
            !stmt.where ||
            (await this.evaluateExpr(storage, stmt.where, row, params));
          if (match) {
            // Referential Integrity Check (Parent side)
            for (const ref of referencingCols) {
              const parentVal = row[ref.parentColumn];
              if (parentVal === null || parentVal === undefined) continue;

              // Check for existence in child table
              let hasChildren = false;
              // Optimization: Use O(log N) index lookup if child column is the Primary Key
              const childPK = await storage.getPKColumn(ref.childTable);
              if (childPK === ref.childColumn) {
                const r = await storage.getRowByPK(ref.childTable, parentVal);
                if (r) hasChildren = true;
              }
              if (!hasChildren) {
                for await (const childRow of storage.scanRows(ref.childTable)) {
                  if (childRow[ref.childColumn] == parentVal) {
                    hasChildren = true;
                    break;
                  }
                }
              }

              if (hasChildren) {
                const action = ref.onDelete;
                if (action === "RESTRICT" || action === "NO ACTION") {
                  const errorDetails = [
                    `Foreign Key Violation (Delete RESTRICT): delete on table "${stmt.tableName}" violates foreign key constraint.`,
                    `- Target Table: "${stmt.tableName}" (Parent)`,
                    `- Target Column: "${ref.parentColumn}"`,
                    `- Deleted Value: ${JSON.stringify(parentVal)}`,
                    `- Dependent Table: "${ref.childTable}" (Child)`,
                    `- Dependent Column: "${ref.childColumn}"`,
                    `Reason: Cannot delete record with value ${JSON.stringify(parentVal)} because child records depend on it in table "${ref.childTable}" and the ON DELETE action is ${action}.`,
                  ].join("\n");
                  throw new Error(errorDetails);
                } else if (action === "CASCADE") {
                  await storage.deleteRows(
                    ref.childTable,
                    async (r) => r[ref.childColumn] == parentVal,
                  );
                } else if (action === "SET NULL") {
                  await storage.updateRows(
                    ref.childTable,
                    async (r) => r[ref.childColumn] == parentVal,
                    async (r) => {
                      r[ref.childColumn] = null;
                    },
                  );
                }
              }
            }

            if (stmt.returning) {
              deletedRows.push(
                await this.projectRow(storage, row, stmt.returning, params),
              );
            }
          }
          return match;
        };

        const pkColName = await storage.getPKColumn(stmt.tableName);
        let pkExpr: Expr | null = null;
        let pkVal: any = undefined;

        if (pkColName && stmt.where) {
          const findPkCondition = (expr: Expr): Expr | null => {
            if (expr.type === "Binary" && expr.operator === "=") {
              if (
                expr.left.type === "Identifier" &&
                expr.left.name === pkColName &&
                (expr.right.type === "Literal" ||
                  expr.right.type === "Parameter" ||
                  expr.right.type === "Identifier")
              ) {
                return expr;
              }
            } else if (expr.type === "Logical" && expr.operator === "AND") {
              return findPkCondition(expr.left) || findPkCondition(expr.right);
            }
            return null;
          };

          pkExpr = findPkCondition(stmt.where);
          if (pkExpr && pkExpr.type === "Binary") {
            pkVal = await this.evaluateExpr(storage, pkExpr.right, {}, params);
            const tableInfo = await (storage as any).getTableAsync(
              stmt.tableName,
            );
            const pkCol = tableInfo?.columns.find(
              (c: any) => c.name === pkColName,
            );
            if (pkCol) {
              pkVal = await this.castValue(storage, pkVal, pkCol.dataType);
            }
          }
        }

        let deletedCount = 0;
        if (pkVal !== undefined && pkVal !== null) {
          deletedCount = await storage.deleteRowByPK(
            stmt.tableName,
            pkVal,
            filterFn,
          );
          if (deletedCount === 0) {
            deletedCount = await storage.deleteRows(
              stmt.tableName,
              async (row: any) => {
                if (row[pkColName!] != pkVal) return false;
                return await filterFn(row);
              },
            );
          }
        } else {
          deletedCount = await storage.deleteRows(stmt.tableName, filterFn);
        }

        if (stmt.returning) return deletedRows;
        return { success: true, deleted: deletedCount };
      }

      case "Comment": {
        const isColumn = stmt.objectType === "COLUMN";
        const parts = stmt.objectName.split(".");
        const colName = isColumn ? parts.pop() || null : null;
        const tableName = parts.join(".");

        // Validate existence
        const table = await (storage as any).getTableAsync(tableName);
        if (!table) throw new Error(`Table ${tableName} not found`);
        let objsubid = 0;
        if (isColumn && colName) {
          const colIdx = table.columns.findIndex(
            (c: any) => c.name === colName,
          );
          if (colIdx === -1)
            throw new Error(
              `Column ${colName} does not exist in table ${tableName}`,
            );
          objsubid = (table.columns[colIdx] as any)._attnum;
        }

        const objoid = table.firstPage;
        const objNameWithoutSchema = tableName.includes(".")
          ? tableName.split(".").pop()!
          : tableName;

        // Upsert metadata directly into internal table for supreme optimization
        await storage.deleteRows(
          "pg_catalog.pg_description",
          async (r: any) => {
            return (
              Number(r.objoid) === Number(objoid) &&
              Number(r.objsubid) === Number(objsubid)
            );
          },
        );

        try {
          await storage.insertRow("pg_catalog.pg_description", {
            objoid,
            classoid: 1259,
            objsubid,
            description: stmt.comment,
            objname: objNameWithoutSchema,
            column_name: colName,
          });

          storage.invalidateDescriptionCache();
          return {
            success: true,
            message: `Comment recorded for ${stmt.objectName}`,
          };
        } catch (error) {
          return {
            success: false,
            message: `Failed to record comment for ${stmt.objectName}: ${(error as any).message}`,
          };
        }
      }

      case "Do": {
        // Handle anonymous block execution
        // For standard "Lite" implementation, we return success and metadata

        const code = stmt.code;
        const cleanCode = code
          .replace(/--.*$/gm, "")
          .replace(/\/\*[\s\S]*?\*\//g, "");

        // --- HACK: Support conditional execution blocks (IF EXISTS) ---
        const ifRegex =
          /IF\s+(NOT\s+)?EXISTS\s*\(([\s\S]+?)\)\s*THEN\s*([\s\S]+?)\s*END IF;/gi;
        let match;
        let executedIf = false;
        while ((match = ifRegex.exec(cleanCode)) !== null) {
          executedIf = true;
          const isNot = !!match[1];
          const query = match[2];
          const innerSql = match[3];

          const lexer = new Lexer(query);
          const parser = new Parser(lexer.tokenize());
          const selectStmt = parser.parse();

          let exists = false;
          try {
            for await (const row of this.executeSelect(
              storage,
              selectStmt,
              params,
            )) {
              exists = true;
              break;
            }
          } catch (e) {
            // Ignore missing table errors for system catalogs like pg_type
            exists = false;
          }

          const conditionMet = isNot ? !exists : exists;

          if (conditionMet) {
            const innerLexer = new Lexer(innerSql);
            const innerParser = new Parser(innerLexer.tokenize());
            while (innerParser.hasMore()) {
              const innerStmt = innerParser.parse();
              if (innerStmt) {
                await this.execute(storage, innerStmt, params);
              }
              if (innerParser.match("SYMBOL", ";")) {
                innerParser.consume();
              }
            }
          }
        }

        // --- HACK: Support TypeORM / Prisma dynamic constraint dropping ---
        if (
          !executedIf &&
          /ALTER\s+TABLE\s+.*?\s+DROP\s+CONSTRAINT/i.test(code) &&
          /pg_constraint/i.test(code)
        ) {
          const relMatch = code.match(/conrelid\s*=\s*'([^']+)'::regclass/i);
          const targetMatch = code.match(
            /confrelid\s*=\s*'([^']+)'::regclass/i,
          );

          let sourceTable = relMatch ? relMatch[1].replace(/"/g, "") : null;
          if (!sourceTable) {
            const alterMatch = code.match(/ALTER\s+TABLE\s+([a-zA-Z0-9_]+)/i);
            if (alterMatch) sourceTable = alterMatch[1];
          }

          const targetTable = targetMatch
            ? targetMatch[1].replace(/"/g, "")
            : null;

          if (sourceTable) {
            const isFk = /contype\s*=\s*'f'/i.test(code);
            const isUnique = /contype\s*=\s*'u'/i.test(code);
            const isPk = /contype\s*=\s*'p'/i.test(code);

            const dropFk = isFk || !!targetTable;

            const conNameMatch = code.match(
              /DROP\s+CONSTRAINT\s+(?!'|"|\||r\.conname)([\w_]+)/i,
            );
            const explicitConName = conNameMatch ? conNameMatch[1] : null;

            const dropAll =
              !isFk && !isUnique && !isPk && !targetTable && !explicitConName;

            const tableInfo = await (storage as any).getTableAsync(sourceTable);
            if (tableInfo) {
              let dropped = false;
              for (const col of tableInfo.columns) {
                if (explicitConName) {
                  if (
                    explicitConName === `${sourceTable}_pkey` &&
                    col.isPrimaryKey
                  ) {
                    col.isPrimaryKey = false;
                    dropped = true;
                  } else if (
                    explicitConName === `${sourceTable}_${col.name}_key` &&
                    col.isUnique
                  ) {
                    col.isUnique = false;
                    dropped = true;
                  } else if (
                    explicitConName === `${sourceTable}_${col.name}_fkey` &&
                    col.references
                  ) {
                    col.references = undefined;
                    dropped = true;
                  }
                } else {
                  if ((dropFk || dropAll) && col.references) {
                    if (!targetTable || col.references.table === targetTable) {
                      col.references = undefined;
                      dropped = true;
                    }
                  }
                  if (
                    (isUnique || dropAll) &&
                    col.isUnique &&
                    !col.isPrimaryKey
                  ) {
                    col.isUnique = false;
                    dropped = true;
                  }
                  if ((isPk || dropAll) && col.isPrimaryKey) {
                    col.isPrimaryKey = false;
                    dropped = true;
                  }
                }
              }
              if (dropped) {
                await storage.updateTableSchema(sourceTable, tableInfo);
                await storage.updateRows(
                  "pg_catalog.pg_attribute",
                  async (r: any) => r.attrelid === tableInfo.firstPage,
                  async (r: any) => {
                    const c = tableInfo.columns.find(
                      (col: any) => col.name === r.attname,
                    );
                    if (c) {
                      r.attprimary = !!c.isPrimaryKey;
                      r.attunique = !!c.isUnique;
                      if (!c.references) {
                        r.attref_table = null;
                        r.attref_col = null;
                        r.attref_on_delete = null;
                        r.attref_on_update = null;
                      }
                    }
                  },
                );
              }
            }
          }
        }

        return {
          success: true,
          executed_block: stmt.code,
          language: stmt.language || "plpgsql",
          params,
        };
      }

      case "XrayMeta": {
        const meta: any = { tables: {}, schemas: [], enums: {} };

        const nspMap = new Map<number, string>();
        for await (const nsp of storage.scanRows("pg_catalog.pg_namespace")) {
          nspMap.set(nsp.oid, nsp.nspname);
          meta.schemas.push(nsp.nspname);
        }

        const clsMap = new Map<number, any>();
        for await (const cls of storage.scanRows("pg_catalog.pg_class")) {
          clsMap.set(cls.oid, cls);
          if (cls.relkind === "r" || cls.relkind === "i") {
            const schemaName = nspMap.get(cls.relnamespace) || "public";
            const fullTableName =
              schemaName === "public"
                ? cls.relname
                : `${schemaName}.${cls.relname}`;
            if (cls.relkind === "r") {
              meta.tables[fullTableName] = {
                oid: cls.oid,
                name: cls.relname,
                schema: schemaName,
                columns: [],
                indexes: [],
                comment: null,
              };
            }
          }
        }

        for await (const attr of storage.scanRows("pg_catalog.pg_attribute")) {
          const cls = clsMap.get(attr.attrelid);
          if (cls && cls.relkind === "r") {
            const schemaName = nspMap.get(cls.relnamespace) || "public";
            const fullTableName =
              schemaName === "public"
                ? cls.relname
                : `${schemaName}.${cls.relname}`;
            if (meta.tables[fullTableName]) {
              let parsedDef = null;
              if (attr.attdef) {
                try {
                  parsedDef = JSON.parse(attr.attdef);
                } catch (e) {
                  parsedDef = attr.attdef;
                }
              }
              meta.tables[fullTableName].columns.push({
                name: attr.attname,
                type: attr.atttypid,
                num: attr.attnum,
                notNull: attr.attnotnull,
                isPrimary: attr.attprimary,
                isUnique: attr.attunique,
                default: parsedDef,
                references: attr.attref_table
                  ? {
                      table: attr.attref_table,
                      column: attr.attref_col,
                      onDelete: attr.attref_on_delete,
                      onUpdate: attr.attref_on_update,
                    }
                  : null,
                comment: null,
              });
            }
          }
        }

        for await (const desc of storage.scanRows(
          "pg_catalog.pg_description",
        )) {
          if (desc.classoid === 1259) {
            const rel = clsMap.get(desc.objoid);
            if (rel && rel.relkind === "r") {
              const schemaName = nspMap.get(rel.relnamespace) || "public";
              const fullTableName =
                schemaName === "public"
                  ? rel.relname
                  : `${schemaName}.${rel.relname}`;
              const tbl = meta.tables[fullTableName];
              if (tbl) {
                if (desc.objsubid === 0) {
                  tbl.comment = desc.description;
                } else {
                  const col = tbl.columns.find(
                    (c: any) => c.num === desc.objsubid,
                  );
                  if (col) col.comment = desc.description;
                }
              }
            }
          }
        }

        for await (const idx of storage.scanRows("pg_catalog.pg_index")) {
          const rel = clsMap.get(idx.indrelid);
          const idxRel = clsMap.get(idx.indexrelid);
          if (rel && idxRel && rel.relkind === "r") {
            const schemaName = nspMap.get(rel.relnamespace) || "public";
            const fullTableName =
              schemaName === "public"
                ? rel.relname
                : `${schemaName}.${rel.relname}`;
            if (meta.tables[fullTableName]) {
              meta.tables[fullTableName].indexes.push({
                name: idxRel.relname,
                isPrimary: idx.indisprimary,
                isUnique: idx.indisunique,
                keys: idx.indkey,
              });
            }
          }
        }

        const typeMap = new Map<number, string>();
        for await (const typ of storage.scanRows("pg_catalog.pg_type")) {
          typeMap.set(typ.oid, typ.typname);
          if (typ.typtype === "e") {
            meta.enums[typ.typname] = [];
          }
        }

        for await (const en of storage.scanRows("pg_catalog.pg_enum")) {
          const typname = typeMap.get(en.enumtypid);
          if (typname && meta.enums[typname]) {
            meta.enums[typname].push({
              label: en.enumlabel,
              order: en.enumsortorder,
            });
          }
        }

        for (const enumName in meta.enums) {
          meta.enums[enumName].sort((a: any, b: any) => a.order - b.order);
          meta.enums[enumName] = meta.enums[enumName].map((e: any) => e.label);
        }

        for (const tbl in meta.tables) {
          meta.tables[tbl].columns.sort((a: any, b: any) => a.num - b.num);
        }

        return { rows: [{ meta }], fields: [{ name: "meta" }] };
      }
    }
  }

  // Volcano Model Iterator Pattern
  private async *executeSelect(
    storage: StorageEngine,
    stmt: any,
    params: any = [],
    outerRow: any = {},
  ): AsyncIterableIterator<any> {
    if (!stmt.ctes && Object.keys(outerRow).length === 0) {
      if ((stmt as any)._jitExecutable === undefined) {
        (stmt as any)._jitExecutable = JITCompiler.compile(stmt);
      }
      const jit = (stmt as any)._jitExecutable;
      if (jit) {
        const rows = await jit.execute(storage, params);
        for (let i = 0; i < rows.length; i++) {
          yield rows[i];
        }
        return;
      }
    }

    if (stmt.ctes) {
      for (const cte of stmt.ctes) {
        if (cte.recursive && (cte.stmt.union || cte.stmt.unionAll)) {
          const baseStmt = {
            ...cte.stmt,
            union: undefined,
            unionAll: undefined,
          };
          const recStmt = cte.stmt.unionAll || cte.stmt.union;
          const isUnionAll = !!cte.stmt.unionAll;

          let workingTable = [];
          for await (const r of this.executeSelect(
            storage,
            baseStmt,
            params,
            outerRow,
          )) {
            workingTable.push(r);
          }

          let effectiveColumnAliases = cte.columnAliases;
          if (!effectiveColumnAliases && workingTable.length > 0) {
            effectiveColumnAliases = Object.keys(workingTable[0]).filter(
              (k) => !k.startsWith("__"),
            );
          }

          workingTable = workingTable.map((r) => {
            if (effectiveColumnAliases) {
              const mapped: any = {};
              const keys = Object.keys(r).filter((k) => !k.startsWith("__"));
              for (let i = 0; i < effectiveColumnAliases.length; i++) {
                mapped[effectiveColumnAliases[i]] = r[keys[i]];
              }
              return mapped;
            }
            return r;
          });

          let finalTable = [...workingTable];
          const seen = new Set<string>();

          const getRowKey = (row: any) => {
            const clean: any = {};
            for (const k in row) if (!k.startsWith("__")) clean[k] = row[k];
            return JSON.stringify(clean);
          };

          if (!isUnionAll) {
            workingTable.forEach((r) => seen.add(getRowKey(r)));
          }

          storage.createTempTable(cte.name, workingTable);

          let iterations = 0;
          const MAX_ITERATIONS = 10000;
          while (workingTable.length > 0) {
            iterations++;
            if (iterations > MAX_ITERATIONS) {
              throw new Error(
                "Recursive CTE exceeded max iterations (10000). Possible infinite loop.",
              );
            }
            const nextWorkingTable = [];
            for await (const r of this.executeSelect(
              storage,
              recStmt,
              params,
              outerRow,
            )) {
              let mapped = r;
              if (effectiveColumnAliases) {
                mapped = {};
                const keys = Object.keys(r).filter((k) => !k.startsWith("__"));
                for (let i = 0; i < effectiveColumnAliases.length; i++) {
                  mapped[effectiveColumnAliases[i]] = r[keys[i]];
                }
              }

              if (!isUnionAll) {
                const key = getRowKey(mapped);
                if (seen.has(key)) continue;
                seen.add(key);
              }
              nextWorkingTable.push(mapped);
              finalTable.push(mapped);
            }
            workingTable = nextWorkingTable;
            storage.createTempTable(cte.name, workingTable);
          }
          storage.createTempTable(cte.name, finalTable);
        } else {
          const rows = [];
          for await (const r of this.executeSelect(
            storage,
            cte.stmt,
            params,
            outerRow,
          )) {
            let mapped = r;
            if (cte.columnAliases) {
              mapped = {};
              const keys = Object.keys(r).filter((k) => !k.startsWith("__"));
              for (let i = 0; i < keys.length; i++) {
                if (cte.columnAliases[i])
                  mapped[cte.columnAliases[i]] = r[keys[i]];
                else mapped[keys[i]] = r[keys[i]];
              }
            }
            rows.push(mapped);
          }
          storage.createTempTable(cte.name, rows);
        }
      }
    }

    try {
      if (stmt.type === "Values") {
        for (const rowExprs of stmt.values) {
          const row: any = {};
          for (let i = 0; i < rowExprs.length; i++) {
            row[`column${i + 1}`] = await this.evaluateExpr(
              storage,
              rowExprs[i],
              outerRow,
              params,
            );
          }
          yield row;
        }
        return;
      }

      let source: any;
      if (stmt.from) {
        if (stmt.from.stmt) {
          source = this.executeSelect(
            storage,
            stmt.from.stmt,
            params,
            outerRow,
          );

          if (stmt.from.columnAliases) {
            const aliases = stmt.from.columnAliases;
            source = this.mapStream(source, (r) => {
              const newR: any = {};
              const keys = Object.keys(r).filter((k) => !k.startsWith("__"));
              for (let i = 0; i < aliases.length; i++) {
                if (keys[i]) {
                  newR[aliases[i]] = r[keys[i]];
                }
              }
              return newR;
            });
          }

          if (stmt.from.alias)
            source = this.mapStream(source, (r) => {
              const newR = { ...r };
              newR["__lpg_tbl_" + stmt.from.alias] = this.getTableCopy(r);
              return newR;
            });
        } else if (stmt.from.fn) {
          const fnExpr = stmt.from.fn;
          let rows: any[] = [];
          if (fnExpr.type === "Call" && fnExpr.fnName === "UNNEST") {
            const arr = await this.evaluateExpr(
              storage,
              fnExpr.args[0],
              outerRow,
              params,
            );
            if (Array.isArray(arr)) {
              rows = arr.map((item, idx) => {
                const row: any = {};
                const alias1 = stmt.from.columnAliases?.[0] || "unnest";
                row[alias1] = item;
                if (stmt.from.withOrdinality) {
                  const alias2 = stmt.from.columnAliases?.[1] || "ordinality";
                  row[alias2] = idx + 1;
                }
                return row;
              });
            }
          } else if (
            fnExpr.type === "Call" &&
            (fnExpr.fnName === "JSONB_EACH" || fnExpr.fnName === "JSON_EACH")
          ) {
            const obj = await this.evaluateExpr(
              storage,
              fnExpr.args[0],
              outerRow,
              params,
            );
            if (obj && typeof obj === "object" && !Array.isArray(obj)) {
              rows = Object.entries(obj).map(([k, v], idx) => {
                const row: any = {};
                const alias1 = stmt.from.columnAliases?.[0] || "key";
                const alias2 = stmt.from.columnAliases?.[1] || "value";
                row[alias1] = k;
                row[alias2] = v;
                if (stmt.from.withOrdinality) {
                  const alias3 = stmt.from.columnAliases?.[2] || "ordinality";
                  row[alias3] = idx + 1;
                }
                return row;
              });
            }
          } else if (
            fnExpr.type === "Call" &&
            (fnExpr.fnName === "JSONB_ARRAY_ELEMENTS" ||
              fnExpr.fnName === "JSON_ARRAY_ELEMENTS")
          ) {
            const arr = await this.evaluateExpr(
              storage,
              fnExpr.args[0],
              outerRow,
              params,
            );
            if (Array.isArray(arr)) {
              rows = arr.map((item, idx) => {
                const row: any = {};
                const alias1 = stmt.from.columnAliases?.[0] || "value";
                row[alias1] = item;
                if (stmt.from.withOrdinality) {
                  const alias2 = stmt.from.columnAliases?.[1] || "ordinality";
                  row[alias2] = idx + 1;
                }
                return row;
              });
            }
          }
          source = (async function* (_this) {
            for (const r of rows) {
              const newR = { ...r };
              newR["__lpg_tbl_" + (stmt.from.alias ? stmt.from.alias : "t")] =
                _this.getTableCopy(r);
              yield newR;
            }
          })(this);
        } else {
          await (storage as any).getTableAsync(stmt.from.tableName);
          let useIndex = false;
          // Predicate Pushdown + O(1) Index Lookup
          if ((!stmt.joins || stmt.joins.length === 0) && stmt.where) {
            const pkColName = await storage.getPKColumn(stmt.from.tableName);
            if (pkColName) {
              let pkExpr: Expr | null = null;

              const findPkCondition = (expr: Expr): Expr | null => {
                if (expr.type === "Binary" && expr.operator === "=") {
                  if (
                    expr.left.type === "Identifier" &&
                    expr.left.name === pkColName &&
                    (expr.right.type === "Literal" ||
                      expr.right.type === "Parameter" ||
                      expr.right.type === "Identifier")
                  ) {
                    return expr;
                  }
                } else if (expr.type === "Logical" && expr.operator === "AND") {
                  return (
                    findPkCondition(expr.left) || findPkCondition(expr.right)
                  );
                }
                return null;
              };

              pkExpr = findPkCondition(stmt.where);

              if (pkExpr && pkExpr.type === "Binary") {
                useIndex = true;
                let val = await this.evaluateExpr(
                  storage,
                  pkExpr.right,
                  outerRow,
                  params,
                );
                const tableInfo = await (storage as any).getTableAsync(
                  stmt.from.tableName,
                );
                const pkCol = tableInfo?.columns.find(
                  (c: any) => c.name === pkColName,
                );
                if (pkCol) {
                  val = await this.castValue(storage, val, pkCol.dataType);
                }
                const row = await storage.getRowByPK(stmt.from.tableName, val);
                source = (async function* (_this) {
                  if (row) {
                    const tblCopy = _this.getTableCopy(row);
                    row["__lpg_tbl_" + stmt.from.tableName!] = tblCopy;
                    if (stmt.from.alias)
                      row["__lpg_tbl_" + stmt.from.alias] = tblCopy;
                    if (Object.keys(outerRow).length > 0) {
                      yield { ...outerRow, ...row };
                    } else {
                      yield row;
                    }
                  }
                })(this);
              }
            }
          }

          if (!useIndex) {
            const fromTableName = stmt.from.tableName!;
            const fromAlias = stmt.from.alias;
            const hasOuterRow = Object.keys(outerRow).length > 0;
            source = this.mapStreamSync(storage.scanRows(fromTableName), (r) => {
              const tblCopy = this.getTableCopy(r);
              r["__lpg_tbl_" + fromTableName] = tblCopy;
              if (fromAlias) r["__lpg_tbl_" + fromAlias] = tblCopy;
              if (hasOuterRow) {
                return { ...outerRow, ...r };
              }
              return r;
            });
            if ((!stmt.joins || stmt.joins.length === 0) && stmt.where) {
              if (!this.hasAsyncOps(stmt.where)) {
                source = this.filterStreamSync(
                  source,
                  (r) => !!this.evaluateExprSync(storage, stmt.where, r, params),
                );
              } else {
                source = this.filterStream(
                  source,
                  async (r) =>
                    await this.evaluateExpr(storage, stmt.where, r, params),
                );
              }
            }
          }
        }
      } else {
        source = (async function* () {
          yield { ...outerRow };
        })();
      }

      if (stmt.joins) {
        for (const join of stmt.joins) {
          if (join.lateral || join.fn || join.stmt) {
            source = this.nestedLoopJoinStream(storage, source, join, params);
          } else {
            // In-memory Hash Join optimization
            const rightRows = [];
            for await (const r of storage.scanRows(join.tableName!)) {
              const tblCopy = this.getTableCopy(r);
              r["__lpg_tbl_" + join.tableName!] = tblCopy;
              if (join.alias) r["__lpg_tbl_" + join.alias] = tblCopy;
              rightRows.push(r);
            }
            source = this.hashJoinStream(
              storage,
              source,
              rightRows,
              join,
              params,
            );
          }
        }
      }

      if (stmt.where && stmt.joins && stmt.joins.length > 0) {
        if (!this.hasAsyncOps(stmt.where)) {
          source = this.filterStreamSync(
            source,
            (r) => !!this.evaluateExprSync(storage, stmt.where, r, params),
          );
        } else {
          source = this.filterStream(
            source,
            async (r) => await this.evaluateExpr(storage, stmt.where, r, params),
          );
        }
      } else if (stmt.where && (stmt.from?.stmt || stmt.from?.fn || !stmt.from)) {
        if (!this.hasAsyncOps(stmt.where)) {
          source = this.filterStreamSync(
            source,
            (r) => !!this.evaluateExprSync(storage, stmt.where, r, params),
          );
        } else {
          source = this.filterStream(
            source,
            async (r) => await this.evaluateExpr(storage, stmt.where, r, params),
          );
        }
      }

      const allAggs: Expr[] = [];
      for (const c of stmt.columns) this.extractAggregates(c, allAggs);
      if (stmt.having) this.extractAggregates(stmt.having, allAggs);
      if (stmt.orderBy) {
        for (const ob of stmt.orderBy) this.extractAggregates(ob.expr, allAggs);
      }
      const isAgg = allAggs.length > 0;

      let sourceStream = source;

      if (stmt.groupBy || isAgg) {
        sourceStream = this.streamingAggregate(
          storage,
          sourceStream,
          stmt,
          params,
          allAggs,
        );
      }

      const hasWindow = stmt.columns.some((c: any) => {
        let target = c;
        if (c.type === "Alias") target = c.expr;
        return target.type === "Call" && target.over;
      });

      if (hasWindow) {
        const bufferedRows = [];
        for await (const row of sourceStream) bufferedRows.push(row);
        sourceStream = this.processWindowFunctions(
          storage,
          bufferedRows,
          stmt.columns,
          params,
        );
      }

      const exclusions = new Set<string>();
      if (stmt.from) {
        if (stmt.from.tableName) exclusions.add(stmt.from.tableName);
        if (stmt.from.alias) exclusions.add(stmt.from.alias);
        else if (stmt.from.fn) exclusions.add("t");
      }
      if (stmt.joins) {
        for (const join of stmt.joins) {
          if (join.tableName) exclusions.add(join.tableName);
          if (join.alias) exclusions.add(join.alias);
        }
      }

      const _this = this;
      const needInternalProps = !!(stmt.orderBy || stmt.distinctOn || stmt.having);
      const hasAsyncProj = stmt.columns.some((c: any) => this.hasAsyncOps(c));
      const hasWildcard = stmt.columns.some(
        (c: any) =>
          c.type === "Identifier" && (c.name === "*" || c.name.endsWith(".*")),
      );

      if (!hasAsyncProj && !hasWildcard) {
        const compiledCols: { outKey: string; expr: Expr; key: string }[] = [];
        const usedKeys = new Set<string>();
        for (let i = 0; i < stmt.columns.length; i++) {
          const col = stmt.columns[i]!;
          let outKey = "col";
          let expr = col;
          if (col.type === "Alias") {
            outKey = col.alias;
            expr = col.expr;
          } else if (col.type === "Identifier") {
            outKey = col.name.includes(".")
              ? col.name.split(".").pop()!
              : col.name;
          } else if (col.type === "Call") {
            outKey = col.fnName.toLowerCase();
          }

          if (usedKeys.has(outKey)) {
            let suffix = 1;
            while (usedKeys.has(`${outKey}${suffix}`)) suffix++;
            outKey = `${outKey}${suffix}`;
          }
          usedKeys.add(outKey);
          compiledCols.push({ outKey, expr, key: this.getExprKey(expr) });
        }

        sourceStream = this.mapStreamSync(sourceStream, function (r) {
          const proj: any = {};
          for (let i = 0; i < compiledCols.length; i++) {
            const c = compiledCols[i]!;
            if (r[c.key] !== undefined) {
              proj[c.outKey] = r[c.key];
            } else {
              proj[c.outKey] = _this.evaluateExprSync(storage, c.expr, r, params);
            }
          }
          return needInternalProps ? { ...r, ...proj, ___lpg_projected___: proj } : proj;
        });
      } else if (!hasAsyncProj) {
        sourceStream = this.mapStreamSync(sourceStream, function (r) {
          const proj = _this.projectRowSync(
            storage,
            r,
            stmt.columns,
            params,
            exclusions,
          );
          return needInternalProps ? { ...r, ...proj, ___lpg_projected___: proj } : proj;
        });
      } else {
        sourceStream = this.mapStream(sourceStream, async function (r) {
          const proj = await _this.projectRow(
            storage,
            r,
            stmt.columns,
            params,
            exclusions,
          );
          return needInternalProps ? { ...r, ...proj, ___lpg_projected___: proj } : proj;
        });
      }

      if (stmt.distinct && !stmt.distinctOn) {
        sourceStream = this.distinctStream(sourceStream);
      }

      if (stmt.unionAll) {
        const rightStream = this.executeSelect(
          storage,
          stmt.unionAll,
          params,
          outerRow,
        );
        sourceStream = this.concatStreams(sourceStream, rightStream);
      }

      if (stmt.union) {
        const rightStream = this.executeSelect(
          storage,
          stmt.union,
          params,
          outerRow,
        );
        sourceStream = this.distinctStream(
          this.concatStreams(sourceStream, rightStream),
        );
      }

      if (stmt.intersect) {
        const rightStream = this.executeSelect(
          storage,
          stmt.intersect,
          params,
          outerRow,
        );
        sourceStream = this.intersectStream(sourceStream, rightStream);
      }

      if (stmt.intersectAll) {
        const rightStream = this.executeSelect(
          storage,
          stmt.intersectAll,
          params,
          outerRow,
        );
        sourceStream = this.intersectAllStream(sourceStream, rightStream);
      }

      if (stmt.except) {
        const rightStream = this.executeSelect(
          storage,
          stmt.except,
          params,
          outerRow,
        );
        sourceStream = this.exceptStream(sourceStream, rightStream);
      }

      if (stmt.exceptAll) {
        const rightStream = this.executeSelect(
          storage,
          stmt.exceptAll,
          params,
          outerRow,
        );
        sourceStream = this.exceptAllStream(sourceStream, rightStream);
      }

      let handledSortLimit = false;
      if (
        stmt.orderBy &&
        stmt.limit &&
        !stmt.distinct &&
        !stmt.distinctOn &&
        !stmt.union &&
        !stmt.unionAll &&
        !stmt.intersect &&
        !stmt.except
      ) {
        let limitVal: number | null = null;
        let offsetVal = 0;
        if (!this.hasAsyncOps(stmt.limit)) {
          limitVal = Number(this.evaluateExprSync(storage, stmt.limit, {}, params));
        } else {
          limitVal = Number(await this.evaluateExpr(storage, stmt.limit, {}, params));
        }
        if (stmt.offset) {
          if (!this.hasAsyncOps(stmt.offset)) {
            offsetVal = Number(this.evaluateExprSync(storage, stmt.offset, {}, params)) || 0;
          } else {
            offsetVal = Number(await this.evaluateExpr(storage, stmt.offset, {}, params)) || 0;
          }
        }
        if (
          limitVal !== null &&
          !isNaN(limitVal) &&
          limitVal > 0 &&
          limitVal < 10000 &&
          !isNaN(offsetVal) &&
          offsetVal >= 0 &&
          offsetVal < 10000
        ) {
          sourceStream = this.topNSortStream(
            storage,
            sourceStream,
            stmt.orderBy,
            limitVal,
            offsetVal,
            params,
          );
          handledSortLimit = true;
        }
      }

      if (!handledSortLimit) {
        if (stmt.orderBy) {
          sourceStream = this.externalSortStream(
            storage,
            sourceStream,
            stmt.orderBy,
            params,
          );
        }

        if (stmt.distinctOn) {
          sourceStream = this.distinctStream(
            sourceStream,
            stmt.distinctOn,
            storage,
            params,
          );
        }

        if (stmt.offset) {
          sourceStream = this.applyOffset(
            sourceStream,
            await this.evaluateExpr(storage, stmt.offset, {}, params),
          );
        }

        if (stmt.limit) {
          sourceStream = this.applyLimit(
            sourceStream,
            await this.evaluateExpr(storage, stmt.limit, {}, params),
          );
        }
      }

      for await (const row of sourceStream) {
        yield row.___lpg_projected___ ? row.___lpg_projected___ : row;
      }
    } finally {
      if (stmt.ctes) {
        for (const cte of stmt.ctes) storage.dropTempTable(cte.name);
      }
    }
  }

  private async *mapStreamSync(
    source: AsyncIterableIterator<any>,
    fn: (r: any) => any,
  ) {
    for await (const row of source) yield fn(row);
  }

  private async *filterStreamSync(
    source: AsyncIterableIterator<any>,
    fn: (r: any) => boolean,
  ) {
    for await (const row of source) if (fn(row)) yield row;
  }

  private async *topNSortStream(
    storage: StorageEngine,
    source: AsyncIterableIterator<any>,
    orderBy: OrderBy[],
    limit: number,
    offset: number,
    params: any = [],
  ): AsyncIterableIterator<any> {
    const isSync = orderBy.every((ob) => !this.hasAsyncOps(ob.expr));
    const kTotal = limit + offset;

    const evalSortKey = isSync
      ? (r: any) =>
          orderBy.map((ob) => this.evaluateExprSync(storage, ob.expr, r, params))
      : async (r: any) => {
          const vals = [];
          for (const ob of orderBy) {
            vals.push(await this.evaluateExpr(storage, ob.expr, r, params));
          }
          return vals;
        };

    const compareVals = (vA: any[], vB: any[]): number => {
      for (let i = 0; i < orderBy.length; i++) {
        const ob = orderBy[i]!;
        const a = vA[i];
        const b = vB[i];
        if ((a === null || a === undefined) && b !== null && b !== undefined)
          return ob.nullsFirst ? -1 : ob.nullsLast ? 1 : ob.desc ? -1 : 1;
        if (a !== null && a !== undefined && (b === null || b === undefined))
          return ob.nullsFirst ? 1 : ob.nullsLast ? -1 : ob.desc ? 1 : -1;
        if (a < b) return ob.desc ? 1 : -1;
        if (a > b) return ob.desc ? -1 : 1;
      }
      return 0;
    };

    interface HeapNode {
      row: any;
      vals: any[];
    }
    const heap: HeapNode[] = [];

    const siftUp = (idx: number) => {
      while (idx > 0) {
        const parentIdx = (idx - 1) >> 1;
        if (compareVals(heap[idx]!.vals, heap[parentIdx]!.vals) > 0) {
          const tmp = heap[idx]!;
          heap[idx] = heap[parentIdx]!;
          heap[parentIdx] = tmp;
          idx = parentIdx;
        } else {
          break;
        }
      }
    };

    const siftDown = (idx: number) => {
      const len = heap.length;
      while (true) {
        let largest = idx;
        const left = (idx << 1) + 1;
        const right = left + 1;
        if (
          left < len &&
          compareVals(heap[left]!.vals, heap[largest]!.vals) > 0
        ) {
          largest = left;
        }
        if (
          right < len &&
          compareVals(heap[right]!.vals, heap[largest]!.vals) > 0
        ) {
          largest = right;
        }
        if (largest !== idx) {
          const tmp = heap[idx]!;
          heap[idx] = heap[largest]!;
          heap[largest] = tmp;
          idx = largest;
        } else {
          break;
        }
      }
    };

    for await (const row of source) {
      const vals = isSync
        ? (evalSortKey(row) as any[])
        : await evalSortKey(row);
      if (heap.length < kTotal) {
        heap.push({ row, vals });
        siftUp(heap.length - 1);
      } else {
        if (compareVals(vals, heap[0]!.vals) < 0) {
          heap[0] = { row, vals };
          siftDown(0);
        }
      }
    }

    heap.sort((a, b) => compareVals(a.vals, b.vals));

    for (let i = offset; i < Math.min(offset + limit, heap.length); i++) {
      yield heap[i]!.row;
    }
  }

  private async *mapStream(
    source: AsyncIterableIterator<any>,
    fn: (r: any) => any,
  ) {
    for await (const row of source) yield await fn(row);
  }

  private async *filterStream(
    source: AsyncIterableIterator<any>,
    fn: (r: any) => Promise<boolean>,
  ) {
    for await (const row of source) if (await fn(row)) yield row;
  }

  private async *nestedLoopJoinStream(
    storage: StorageEngine,
    source: AsyncIterableIterator<any>,
    join: JoinClause,
    params: any = [],
  ) {
    if (join.type === "RIGHT" || join.type === "FULL") {
      const leftRows = [];
      for await (const r of source) leftRows.push({ row: r, matched: false });

      let rightSource: AsyncIterableIterator<any>;

      if (join.stmt) {
        rightSource = this.executeSelect(storage, join.stmt, params, {});

        if (join.columnAliases) {
          const aliases = join.columnAliases;
          rightSource = this.mapStream(rightSource, (r) => {
            const newR: any = {};
            const keys = Object.keys(r).filter((k) => !k.startsWith("__"));
            for (let i = 0; i < aliases.length; i++) {
              if (keys[i]) {
                newR[aliases[i]] = r[keys[i]];
              }
            }
            return newR;
          });
        }

        if (join.alias) {
          const alias = join.alias;
          rightSource = this.mapStream(rightSource, (r) => {
            const newR = { ...r };
            newR["__lpg_tbl_" + alias] = this.getTableCopy(r);
            return newR;
          });
        }
      } else if (join.fn) {
        const fnExpr = join.fn;
        let rows: any[] = [];
        if (fnExpr.type === "Call" && fnExpr.fnName === "UNNEST") {
          if (!fnExpr.args[0])
            throw new Error("UNNEST requires an array argument");
          const arr = await this.evaluateExpr(
            storage,
            fnExpr.args[0],
            {},
            params,
          );
          if (Array.isArray(arr)) {
            rows = arr.map((item, idx) => {
              const r: any = {};
              const alias1 = join.columnAliases?.[0] || "unnest";
              r[alias1] = item;
              if (join.withOrdinality) {
                const alias2 = join.columnAliases?.[1] || "ordinality";
                r[alias2] = idx + 1;
              }
              return r;
            });
          }
        } else if (
          fnExpr.type === "Call" &&
          (fnExpr.fnName === "JSONB_EACH" || fnExpr.fnName === "JSON_EACH")
        ) {
          const obj = await this.evaluateExpr(
            storage,
            fnExpr.args[0],
            {},
            params,
          );
          if (obj && typeof obj === "object" && !Array.isArray(obj)) {
            rows = Object.entries(obj).map(([k, v], idx) => {
              const r: any = {};
              const alias1 = join.columnAliases?.[0] || "key";
              const alias2 = join.columnAliases?.[1] || "value";
              r[alias1] = k;
              r[alias2] = v;
              if (join.withOrdinality) {
                const alias3 = join.columnAliases?.[2] || "ordinality";
                r[alias3] = idx + 1;
              }
              return r;
            });
          }
        } else if (
          fnExpr.type === "Call" &&
          (fnExpr.fnName === "JSONB_ARRAY_ELEMENTS" ||
            fnExpr.fnName === "JSON_ARRAY_ELEMENTS")
        ) {
          const arr = await this.evaluateExpr(
            storage,
            fnExpr.args[0],
            {},
            params,
          );
          if (Array.isArray(arr)) {
            rows = arr.map((item, idx) => {
              const r: any = {};
              const alias1 = join.columnAliases?.[0] || "value";
              r[alias1] = item;
              if (join.withOrdinality) {
                const alias2 = join.columnAliases?.[1] || "ordinality";
                r[alias2] = idx + 1;
              }
              return r;
            });
          }
        }
        rightSource = (async function* (_this) {
          for (const r of rows) {
            const newR = { ...r };
            newR["__lpg_tbl_" + (join.alias ? join.alias : "t")] =
              _this.getTableCopy(r);
            yield newR;
          }
        })(this);
      } else if (join.tableName) {
        rightSource = this.mapStream(storage.scanRows(join.tableName), (r) => {
          const tblCopy = this.getTableCopy(r);
          r["__lpg_tbl_" + join.tableName!] = tblCopy;
          if (join.alias) r["__lpg_tbl_" + join.alias] = tblCopy;
          return r;
        });
      } else {
        rightSource = (async function* () {
          yield {};
        })();
      }

      for await (const jRow of rightSource) {
        let matched = false;
        for (const item of leftRows) {
          const candidate = { ...item.row, ...jRow };
          if (await this.evaluateExpr(storage, join.on, candidate, params)) {
            yield candidate;
            matched = true;
            item.matched = true;
          }
        }
        if (!matched) {
          yield { ...jRow };
        }
      }

      if (join.type === "FULL") {
        for (const item of leftRows) {
          if (!item.matched) {
            yield { ...item.row };
          }
        }
      }
    } else {
      for await (const row of source) {
        let matched = false;
        let rightSource: AsyncIterableIterator<any>;

        if (join.stmt) {
          rightSource = this.executeSelect(storage, join.stmt, params, row);

          if (join.columnAliases) {
            const aliases = join.columnAliases;
            rightSource = this.mapStream(rightSource, (r) => {
              const newR: any = {};
              const keys = Object.keys(r).filter((k) => !k.startsWith("__"));
              for (let i = 0; i < aliases.length; i++) {
                if (keys[i]) {
                  newR[aliases[i]] = r[keys[i]];
                }
              }
              return newR;
            });
          }

          if (join.alias) {
            const alias = join.alias;
            rightSource = this.mapStream(rightSource, (r) => {
              const newR = { ...r };
              newR["__lpg_tbl_" + alias] = this.getTableCopy(r);
              return newR;
            });
          }
        } else if (join.fn) {
          const fnExpr = join.fn;
          let rows: any[] = [];
          if (fnExpr.type === "Call" && fnExpr.fnName === "UNNEST") {
            if (!fnExpr.args[0])
              throw new Error("UNNEST requires an array argument");
            const arr = await this.evaluateExpr(
              storage,
              fnExpr.args[0],
              row,
              params,
            );
            if (Array.isArray(arr)) {
              rows = arr.map((item, idx) => {
                const r: any = {};
                const alias1 = join.columnAliases?.[0] || "unnest";
                r[alias1] = item;
                if (join.withOrdinality) {
                  const alias2 = join.columnAliases?.[1] || "ordinality";
                  r[alias2] = idx + 1;
                }
                return r;
              });
            }
          } else if (
            fnExpr.type === "Call" &&
            (fnExpr.fnName === "JSONB_EACH" || fnExpr.fnName === "JSON_EACH")
          ) {
            const obj = await this.evaluateExpr(
              storage,
              fnExpr.args[0],
              row,
              params,
            );
            if (obj && typeof obj === "object" && !Array.isArray(obj)) {
              rows = Object.entries(obj).map(([k, v], idx) => {
                const r: any = {};
                const alias1 = join.columnAliases?.[0] || "key";
                const alias2 = join.columnAliases?.[1] || "value";
                r[alias1] = k;
                r[alias2] = v;
                if (join.withOrdinality) {
                  const alias3 = join.columnAliases?.[2] || "ordinality";
                  r[alias3] = idx + 1;
                }
                return r;
              });
            }
          } else if (
            fnExpr.type === "Call" &&
            (fnExpr.fnName === "JSONB_ARRAY_ELEMENTS" ||
              fnExpr.fnName === "JSON_ARRAY_ELEMENTS")
          ) {
            const arr = await this.evaluateExpr(
              storage,
              fnExpr.args[0],
              row,
              params,
            );
            if (Array.isArray(arr)) {
              rows = arr.map((item, idx) => {
                const r: any = {};
                const alias1 = join.columnAliases?.[0] || "value";
                r[alias1] = item;
                if (join.withOrdinality) {
                  const alias2 = join.columnAliases?.[1] || "ordinality";
                  r[alias2] = idx + 1;
                }
                return r;
              });
            }
          }
          rightSource = (async function* (_this) {
            for (const r of rows) {
              const newR = { ...r };
              newR["__lpg_tbl_" + (join.alias ? join.alias : "t")] =
                _this.getTableCopy(r);
              yield newR;
            }
          })(this);
        } else if (join.tableName) {
          rightSource = this.mapStream(
            storage.scanRows(join.tableName),
            (r) => {
              const tblCopy = this.getTableCopy(r);
              r["__lpg_tbl_" + join.tableName!] = tblCopy;
              if (join.alias) r["__lpg_tbl_" + join.alias] = tblCopy;
              return r;
            },
          );
        } else {
          rightSource = (async function* () {
            yield {};
          })();
        }

        for await (const jRow of rightSource) {
          const candidate = { ...row, ...jRow };
          if (await this.evaluateExpr(storage, join.on, candidate, params)) {
            yield candidate;
            matched = true;
          }
        }
        if (!matched && join.type === "LEFT") {
          yield { ...row };
        }
      }
    }
  }

  private async *hashJoinStream(
    storage: StorageEngine,
    source: AsyncIterableIterator<any>,
    rightRows: any[],
    join: JoinClause,
    params: any = [],
  ) {
    let hashKeyLeft: Expr | null = null;
    let hashKeyRight: Expr | null = null;
    let isEquiJoin = false;

    const extractEquiJoin = (expr: Expr): Extract<Expr, { type: "Binary" }> | null => {
      if (!expr) return null;
      if (expr.type === "Binary" && expr.operator === "=") {
        return expr as Extract<Expr, { type: "Binary" }>;
      }
      if (expr.type === "Logical" && expr.operator === "AND") {
        return extractEquiJoin(expr.left) || extractEquiJoin(expr.right);
      }
      return null;
    };

    const eqExpr = extractEquiJoin(join.on);
    if (eqExpr) {
      isEquiJoin = true;
      hashKeyLeft = eqExpr.left;
      hashKeyRight = eqExpr.right;
    }

    if (isEquiJoin && hashKeyLeft && hashKeyRight) {
      const rightMap = new Map<string, any[]>();
      let rightKeyExpr = hashKeyRight;
      let leftKeyExpr = hashKeyLeft;

      if (rightRows.length > 0) {
        try {
          const getTablePrefix = (e: Expr): string | null => {
            if (e.type === "Identifier" && e.name.includes(".")) return e.name.split(".")[0] || null;
            return null;
          };
          const leftPrefix = getTablePrefix(hashKeyLeft);
          const rightPrefix = getTablePrefix(hashKeyRight);

          const rightHasLeftPrefix = leftPrefix && rightRows[0]["__lpg_tbl_" + leftPrefix] !== undefined;
          const rightHasRightPrefix = rightPrefix && rightRows[0]["__lpg_tbl_" + rightPrefix] !== undefined;

          if (rightHasLeftPrefix && !rightHasRightPrefix) {
            rightKeyExpr = hashKeyLeft;
            leftKeyExpr = hashKeyRight;
          } else if (rightHasRightPrefix && !rightHasLeftPrefix) {
            rightKeyExpr = hashKeyRight;
            leftKeyExpr = hashKeyLeft;
          } else {
            const testRight = await this.evaluateExpr(storage, hashKeyRight, rightRows[0], params);
            const testLeft = await this.evaluateExpr(storage, hashKeyLeft, rightRows[0], params);
            if (testRight !== null && testLeft === null) {
              rightKeyExpr = hashKeyRight;
              leftKeyExpr = hashKeyLeft;
            } else if (testLeft !== null && testRight === null) {
              rightKeyExpr = hashKeyLeft;
              leftKeyExpr = hashKeyRight;
            }
          }
        } catch (e) {}

        const isRightKeySync = !this.hasAsyncOps(rightKeyExpr);
        for (let i = 0; i < rightRows.length; i++) {
          const jRow = rightRows[i];
          const k = String(
            isRightKeySync
              ? this.evaluateExprSync(storage, rightKeyExpr, jRow, params)
              : await this.evaluateExpr(storage, rightKeyExpr, jRow, params),
          );
          let arr = rightMap.get(k);
          if (!arr) {
            arr = [];
            rightMap.set(k, arr);
          }
          arr.push(jRow);
        }
      }

      if (join.type === "RIGHT" || join.type === "FULL") {
        const leftRows = [];
        for await (const r of source) leftRows.push({ row: r, matched: false });

        for (const jRow of rightRows) {
          let matched = false;
          const kRight = String(
            await this.evaluateExpr(storage, rightKeyExpr, jRow, params),
          );
          for (const item of leftRows) {
            const kLeft = String(
              await this.evaluateExpr(storage, leftKeyExpr, item.row, params),
            );
            if (kLeft === kRight) {
              const candidate = { ...item.row, ...jRow };
              if (await this.evaluateExpr(storage, join.on, candidate, params)) {
                yield candidate;
                matched = true;
                item.matched = true;
              }
            }
          }
          if (!matched) yield { ...jRow };
        }

        if (join.type === "FULL") {
          for (const item of leftRows) {
            if (!item.matched) yield { ...item.row };
          }
        }
      } else {
        const isLeftKeySync = !this.hasAsyncOps(leftKeyExpr);
        const isSimpleEqui = join.on.type === "Binary" && join.on.operator === "=";
        for await (const row of source) {
          let matched = false;
          const kLeft = String(
            isLeftKeySync
              ? this.evaluateExprSync(storage, leftKeyExpr, row, params)
              : await this.evaluateExpr(storage, leftKeyExpr, row, params),
          );
          const matches = rightMap.get(kLeft);
          if (matches) {
            for (let m = 0; m < matches.length; m++) {
              const jRow = matches[m];
              const candidate = { ...row, ...jRow };
              if (
                isSimpleEqui ||
                (await this.evaluateExpr(storage, join.on, candidate, params))
              ) {
                yield candidate;
                matched = true;
              }
            }
          }
          if (!matched && join.type === "LEFT") yield { ...row };
        }
      }
      return;
    }

    if (join.type === "RIGHT" || join.type === "FULL") {
      const leftRows = [];
      for await (const r of source) leftRows.push({ row: r, matched: false });

      for (const jRow of rightRows) {
        let matched = false;
        for (const item of leftRows) {
          const candidate = { ...item.row, ...jRow };
          if (await this.evaluateExpr(storage, join.on, candidate, params)) {
            yield candidate;
            matched = true;
            item.matched = true;
          }
        }
        if (!matched) yield { ...jRow };
      }

      if (join.type === "FULL") {
        for (const item of leftRows) {
          if (!item.matched) yield { ...item.row };
        }
      }
    } else {
      for await (const row of source) {
        let matched = false;
        for (const jRow of rightRows) {
          const candidate = { ...row, ...jRow };
          if (await this.evaluateExpr(storage, join.on, candidate, params)) {
            yield candidate;
            matched = true;
          }
        }
        if (!matched && join.type === "LEFT") yield { ...row };
      }
    }
  }

  private async *distinctStream(
    source: AsyncIterableIterator<any>,
    distinctOn?: Expr[],
    storage?: StorageEngine,
    params?: any,
  ) {
    const seen = new Set<string>();
    for await (const row of source) {
      let key;
      if (distinctOn && distinctOn.length > 0 && storage && params) {
        const vals = [];
        for (const expr of distinctOn) {
          vals.push(await this.evaluateExpr(storage, expr, row, params));
        }
        key = JSON.stringify(vals);
      } else {
        const proj = row.___lpg_projected___ ? row.___lpg_projected___ : row;
        key = JSON.stringify(proj);
      }

      if (!seen.has(key)) {
        seen.add(key);
        yield row;
      }
    }
  }

  private async *concatStreams(
    s1: AsyncIterableIterator<any>,
    s2: AsyncIterableIterator<any>,
  ) {
    for await (const r of s1) yield r;
    for await (const r of s2) yield r;
  }

  private async *intersectStream(
    s1: AsyncIterableIterator<any>,
    s2: AsyncIterableIterator<any>,
  ) {
    const rightSet = new Set<string>();
    for await (const r of s2) {
      const proj = r.___lpg_projected___ ? r.___lpg_projected___ : r;
      rightSet.add(JSON.stringify(proj));
    }

    const seen = new Set<string>();
    for await (const r of s1) {
      const proj = r.___lpg_projected___ ? r.___lpg_projected___ : r;
      const key = JSON.stringify(proj);
      if (rightSet.has(key) && !seen.has(key)) {
        seen.add(key);
        yield r;
      }
    }
  }

  private async *intersectAllStream(
    s1: AsyncIterableIterator<any>,
    s2: AsyncIterableIterator<any>,
  ) {
    const rightCounts = new Map<string, number>();
    for await (const r of s2) {
      const proj = r.___lpg_projected___ ? r.___lpg_projected___ : r;
      const key = JSON.stringify(proj);
      rightCounts.set(key, (rightCounts.get(key) || 0) + 1);
    }

    for await (const r of s1) {
      const proj = r.___lpg_projected___ ? r.___lpg_projected___ : r;
      const key = JSON.stringify(proj);
      const count = rightCounts.get(key) || 0;
      if (count > 0) {
        yield r;
        rightCounts.set(key, count - 1);
      }
    }
  }

  private async *exceptStream(
    s1: AsyncIterableIterator<any>,
    s2: AsyncIterableIterator<any>,
  ) {
    const rightSet = new Set<string>();
    for await (const r of s2) {
      const proj = r.___lpg_projected___ ? r.___lpg_projected___ : r;
      rightSet.add(JSON.stringify(proj));
    }

    const seen = new Set<string>();
    for await (const r of s1) {
      const proj = r.___lpg_projected___ ? r.___lpg_projected___ : r;
      const key = JSON.stringify(proj);
      if (!rightSet.has(key) && !seen.has(key)) {
        seen.add(key);
        yield r;
      }
    }
  }

  private async *exceptAllStream(
    s1: AsyncIterableIterator<any>,
    s2: AsyncIterableIterator<any>,
  ) {
    const rightCounts = new Map<string, number>();
    for await (const r of s2) {
      const proj = r.___lpg_projected___ ? r.___lpg_projected___ : r;
      const key = JSON.stringify(proj);
      rightCounts.set(key, (rightCounts.get(key) || 0) + 1);
    }

    for await (const r of s1) {
      const proj = r.___lpg_projected___ ? r.___lpg_projected___ : r;
      const key = JSON.stringify(proj);
      const count = rightCounts.get(key) || 0;
      if (count > 0) {
        rightCounts.set(key, count - 1);
      } else {
        yield r;
      }
    }
  }

  private async *applyOffset(
    source: AsyncIterableIterator<any>,
    offset: number,
  ) {
    let skipped = 0;
    for await (const r of source) {
      if (skipped < offset) {
        skipped++;
        continue;
      }
      yield r;
    }
  }

  private async *applyLimit(source: AsyncIterableIterator<any>, limit: number) {
    let count = 0;
    for await (const r of source) {
      if (count >= limit) break;
      yield r;
      count++;
    }
  }

  private async *externalSortStream(
    storage: StorageEngine,
    source: AsyncIterableIterator<any>,
    orderBy: OrderBy[],
    params: any = [],
  ): AsyncIterableIterator<any> {
    const CHUNK_SIZE = 100000;
    let chunk = [];
    let fileIndex = 0;
    const tempFiles: string[] = [];
    const vfs = storage.vfs;

    const compareFn = async (a: any, b: any) => {
      for (const ob of orderBy) {
        const vA = await this.evaluateExpr(storage, ob.expr, a, params);
        const vB = await this.evaluateExpr(storage, ob.expr, b, params);
        if (
          (vA === null || vA === undefined) &&
          vB !== null &&
          vB !== undefined
        )
          return ob.nullsFirst ? -1 : ob.nullsLast ? 1 : ob.desc ? -1 : 1;
        if (
          vA !== null &&
          vA !== undefined &&
          (vB === null || vB === undefined)
        )
          return ob.nullsFirst ? 1 : ob.nullsLast ? -1 : ob.desc ? 1 : -1;
        if (vA < vB) return ob.desc ? 1 : -1;
        if (vA > vB) return ob.desc ? -1 : 1;
      }
      return 0;
    };

    const isSync = orderBy.every((ob) => !this.hasAsyncOps(ob.expr));
    const asyncSort = async (arr: any[]) => {
      const mapVals = new Map<any, any>();
      for (const r of arr) {
        let vals: any[];
        if (isSync) {
          vals = orderBy.map((ob) =>
            this.evaluateExprSync(storage, ob.expr, r, params),
          );
        } else {
          vals = [];
          for (const ob of orderBy)
            vals.push(await this.evaluateExpr(storage, ob.expr, r, params));
        }
        mapVals.set(r, vals);
      }
      arr.sort((a, b) => {
        const vA = mapVals.get(a);
        const vB = mapVals.get(b);
        for (let i = 0; i < orderBy.length; i++) {
          const ob = orderBy[i]!;
          const valA = vA[i];
          const valB = vB[i];
          if (
            (valA === null || valA === undefined) &&
            valB !== null &&
            valB !== undefined
          )
            return ob.nullsFirst ? -1 : ob.nullsLast ? 1 : ob.desc ? -1 : 1;
          if (
            valA !== null &&
            valA !== undefined &&
            (valB === null || valB === undefined)
          )
            return ob.nullsFirst ? 1 : ob.nullsLast ? -1 : ob.desc ? 1 : -1;
          if (valA < valB) return ob.desc ? 1 : -1;
          if (valA > valB) return ob.desc ? -1 : 1;
        }
        return 0;
      });
    };

    for await (const row of source) {
      chunk.push(row);
      if (chunk.length >= CHUNK_SIZE) {
        await asyncSort(chunk);
        const tmpFile = vfs.join(
          vfs.tempDir(),
          `lpg_sort_${Date.now()}_${fileIndex++}.json`,
        );
        await vfs.writeFile(
          tmpFile,
          chunk.map((r: any) => JSON.stringify(r)).join("\n"),
        );
        tempFiles.push(tmpFile);
        chunk = [];
      }
    }

    if (chunk.length > 0) {
      await asyncSort(chunk);
      if (tempFiles.length === 0) {
        for (const row of chunk) yield row;
        return;
      }
      const tmpFile = vfs.join(
        vfs.tempDir(),
        `lpg_sort_${Date.now()}_${fileIndex++}.json`,
      );
      await vfs.writeFile(
        tmpFile,
        chunk.map((r: any) => JSON.stringify(r)).join("\n"),
      );
      tempFiles.push(tmpFile);
    }

    if (tempFiles.length === 0) return;

    const iterators = tempFiles.map((f: string) =>
      vfs.readLines(f)[Symbol.asyncIterator](),
    );

    const currentRows: any[] = [];
    for (let i = 0; i < iterators.length; i++) {
      const res = await iterators[i]!.next();
      if (!res.done) currentRows[i] = JSON.parse(res.value);
      else currentRows[i] = null;
    }

    while (true) {
      let minIdx = -1;
      let minRow = null;
      for (let i = 0; i < currentRows.length; i++) {
        if (currentRows[i] !== null) {
          if (minIdx === -1) {
            minIdx = i;
            minRow = currentRows[i];
          } else {
            const cmp = await compareFn(currentRows[i], minRow);
            if (cmp < 0) {
              minIdx = i;
              minRow = currentRows[i];
            }
          }
        }
      }

      if (minIdx === -1) break;

      yield minRow;

      const res = await iterators[minIdx]!.next();
      if (!res.done) currentRows[minIdx] = JSON.parse(res.value);
      else currentRows[minIdx] = null;
    }

    await Promise.all(tempFiles.map((f: string) => vfs.unlink(f)));
  }

  private projectRowSync(
    storage: StorageEngine,
    row: any,
    columns: Expr[],
    params: any = [],
    exclusions?: Set<string>,
  ): any {
    const outRow: any = {};
    for (let i = 0; i < columns.length; i++) {
      const col = columns[i];
      if (
        col.type === "Identifier" &&
        (col.name === "*" || col.name.endsWith(".*"))
      ) {
        if (col.name === "*") {
          const keys = Object.keys(row);
          for (let j = 0; j < keys.length; j++) {
            const k = keys[j];
            if (
              !k.startsWith("__") &&
              !k.startsWith("___") &&
              (!exclusions || !exclusions.has(k))
            )
              outRow[k] = row[k];
          }
        } else {
          if ((col as any)._prefix === undefined) {
            (col as any)._prefix = col.name.substring(0, col.name.length - 2);
          }
          const prefix = (col as any)._prefix;
          const targetObj = row["__lpg_tbl_" + prefix] || row[prefix];
          if (targetObj && typeof targetObj === "object") {
            const keys = Object.keys(targetObj);
            for (let j = 0; j < keys.length; j++) {
              const k = keys[j];
              if (!k.startsWith("__") && !k.startsWith("___"))
                outRow[k] = targetObj[k];
            }
          }
        }
      } else if (col.type === "Alias") {
        const key = this.getExprKey(col.expr);
        let outKey = col.alias;
        if (outRow[outKey] !== undefined) {
          let suffix = 1;
          while (outRow[`${outKey}${suffix}`] !== undefined) suffix++;
          outKey = `${outKey}${suffix}`;
        }
        if (row[key] !== undefined) outRow[outKey] = row[key];
        else
          outRow[outKey] = this.evaluateExprSync(
            storage,
            col.expr,
            row,
            params,
          );
      } else {
        const key = this.getExprKey(col);
        let outKey = "col";
        if ((col as any).name) {
          outKey = (col as any).name.includes(".")
            ? (col as any).name.split(".")[1]
            : (col as any).name;
        } else if (col.type === "Call") {
          outKey = col.fnName.toLowerCase();
        }

        if (outRow[outKey] !== undefined) {
          let suffix = 1;
          while (outRow[`${outKey}${suffix}`] !== undefined) suffix++;
          outKey = `${outKey}${suffix}`;
        }

        if (row[key] !== undefined) {
          outRow[outKey] = row[key];
        } else {
          outRow[outKey] = this.evaluateExprSync(storage, col, row, params);
        }
      }
    }
    return outRow;
  }

  private async projectRow(
    storage: StorageEngine,
    row: any,
    columns: Expr[],
    params: any = [],
    exclusions?: Set<string>,
  ): Promise<any> {
    const outRow: any = {};
    for (let i = 0; i < columns.length; i++) {
      const col = columns[i];
      if (
        col.type === "Identifier" &&
        (col.name === "*" || col.name.endsWith(".*"))
      ) {
        if (col.name === "*") {
          const keys = Object.keys(row);
          for (let j = 0; j < keys.length; j++) {
            const k = keys[j];
            if (
              !k.startsWith("__") &&
              !k.startsWith("___") &&
              (!exclusions || !exclusions.has(k))
            )
              outRow[k] = row[k];
          }
        } else {
          if ((col as any)._prefix === undefined) {
            (col as any)._prefix = col.name.substring(0, col.name.length - 2);
          }
          const prefix = (col as any)._prefix;
          const targetObj = row["__lpg_tbl_" + prefix] || row[prefix];
          if (targetObj && typeof targetObj === "object") {
            const keys = Object.keys(targetObj);
            for (let j = 0; j < keys.length; j++) {
              const k = keys[j];
              if (!k.startsWith("__") && !k.startsWith("___"))
                outRow[k] = targetObj[k];
            }
          }
        }
      } else if (col.type === "Alias") {
        const key = this.getExprKey(col.expr);
        let outKey = col.alias;
        if (outRow[outKey] !== undefined) {
          let suffix = 1;
          while (outRow[`${outKey}${suffix}`] !== undefined) suffix++;
          outKey = `${outKey}${suffix}`;
        }
        if (row[key] !== undefined) outRow[outKey] = row[key];
        else
          outRow[outKey] = await this.evaluateExpr(
            storage,
            col.expr,
            row,
            params,
          );
      } else {
        const key = this.getExprKey(col);
        let outKey = "col";
        if ((col as any).name) {
          outKey = (col as any).name.includes(".")
            ? (col as any).name.split(".")[1]
            : (col as any).name;
        } else if (col.type === "Call") {
          outKey = col.fnName.toLowerCase();
        }

        if (outRow[outKey] !== undefined) {
          let suffix = 1;
          while (outRow[`${outKey}${suffix}`] !== undefined) suffix++;
          outKey = `${outKey}${suffix}`;
        }

        if (row[key] !== undefined) {
          outRow[outKey] = row[key];
        } else {
          outRow[outKey] = await this.evaluateExpr(storage, col, row, params);
        }
      }
    }
    return outRow;
  }

  private async *processWindowFunctions(
    storage: StorageEngine,
    rows: any[],
    columns: any[],
    params: any,
  ): AsyncIterableIterator<any> {
    for (const col of columns) {
      let expr = col;
      if (col.type === "Alias") expr = col.expr;
      if (expr.type !== "Call" || !expr.over) continue;

      const windowKey = this.getExprKey(expr);
      const partitions = new Map<string, any[]>();

      for (const row of rows) {
        let pKey = "all";
        if (expr.over.partitionBy) {
          const vals = [];
          for (const e of expr.over.partitionBy)
            vals.push(await this.evaluateExpr(storage, e, row, params));
          pKey = JSON.stringify(vals);
        }
        if (!partitions.has(pKey)) partitions.set(pKey, []);
        partitions.get(pKey)!.push(row);
      }

      const orderedRows: any[] = [];
      for (const [_, pRows] of partitions) {
        if (expr.over.orderBy) {
          const orderBy = expr.over.orderBy;
          const compareRows = async (a: any, b: any) => {
            for (const ob of orderBy) {
              const vA = await this.evaluateExpr(storage, ob.expr, a, params);
              const vB = await this.evaluateExpr(storage, ob.expr, b, params);
              if (
                (vA === null || vA === undefined) &&
                vB !== null &&
                vB !== undefined
              )
                return ob.nullsFirst ? -1 : ob.nullsLast ? 1 : ob.desc ? -1 : 1;
              if (
                vA !== null &&
                vA !== undefined &&
                (vB === null || vB === undefined)
              )
                return ob.nullsFirst ? 1 : ob.nullsLast ? -1 : ob.desc ? 1 : -1;
              if (vA < vB) return ob.desc ? 1 : -1;
              if (vA > vB) return ob.desc ? -1 : 1;
            }
            return 0;
          };

          // Simplified async sort for windowing
          for (let i = 0; i < pRows.length; i++) {
            for (let j = i + 1; j < pRows.length; j++) {
              if ((await compareRows(pRows[i], pRows[j])) > 0) {
                [pRows[i], pRows[j]] = [pRows[j], pRows[i]];
              }
            }
          }
        }

        let rank = 0;
        let denseRank = 0;
        let lastOrderVals: any[] = [];

        for (let i = 0; i < pRows.length; i++) {
          const row = pRows[i];
          if (expr.fnName === "ROW_NUMBER") {
            row[windowKey] = i + 1;
          } else if (expr.fnName === "RANK" || expr.fnName === "DENSE_RANK") {
            if (expr.over.orderBy) {
              const currentOrderVals = [];
              for (const ob of expr.over.orderBy)
                currentOrderVals.push(
                  await this.evaluateExpr(storage, ob.expr, row, params),
                );

              if (
                i === 0 ||
                JSON.stringify(currentOrderVals) !==
                  JSON.stringify(lastOrderVals)
              ) {
                rank = i + 1;
                denseRank++;
                lastOrderVals = currentOrderVals;
              }
              row[windowKey] = expr.fnName === "RANK" ? rank : denseRank;
            } else {
              row[windowKey] = 1;
            }
          } else if (expr.fnName === "FIRST_VALUE") {
            if (pRows.length > 0) {
              row[windowKey] = await this.evaluateExpr(
                storage,
                expr.args[0],
                pRows[0],
                params,
              );
            } else {
              row[windowKey] = null;
            }
          } else if (expr.fnName === "LAST_VALUE") {
            if (pRows.length > 0) {
              row[windowKey] = await this.evaluateExpr(
                storage,
                expr.args[0],
                pRows[pRows.length - 1],
                params,
              );
            } else {
              row[windowKey] = null;
            }
          } else if (expr.fnName === "LEAD" || expr.fnName === "LAG") {
            const offsetExpr = expr.args[1];
            const defaultExpr = expr.args[2];

            let offset = 1;
            if (offsetExpr) {
              const offVal = await this.evaluateExpr(
                storage,
                offsetExpr,
                row,
                params,
              );
              offset = Number(offVal);
            }

            const targetIdx = expr.fnName === "LEAD" ? i + offset : i - offset;

            if (targetIdx >= 0 && targetIdx < pRows.length) {
              const targetRow = pRows[targetIdx];
              row[windowKey] = await this.evaluateExpr(
                storage,
                expr.args[0],
                targetRow,
                params,
              );
            } else {
              if (defaultExpr) {
                row[windowKey] = await this.evaluateExpr(
                  storage,
                  defaultExpr,
                  row,
                  params,
                );
              } else {
                row[windowKey] = null;
              }
            }
          } else if (expr.fnName === "FIRST_VALUE") {
            if (pRows.length > 0) {
              row[windowKey] = await this.evaluateExpr(
                storage,
                expr.args[0],
                pRows[0],
                params,
              );
            } else {
              row[windowKey] = null;
            }
          } else if (expr.fnName === "LAST_VALUE") {
            if (pRows.length > 0) {
              row[windowKey] = await this.evaluateExpr(
                storage,
                expr.args[0],
                pRows[pRows.length - 1],
                params,
              );
            } else {
              row[windowKey] = null;
            }
          }
          orderedRows.push(row);
        }
      }
      for (let i = 0; i < rows.length; i++) rows[i] = orderedRows[i];
    }

    for (const row of rows) yield row;
  }

  private async *streamingAggregate(
    storage: StorageEngine,
    source: AsyncIterableIterator<any>,
    stmt: any,
    params: any = [],
    allAggs: Expr[] = [],
  ) {
    const groups = new Map<string, any>();

    if (!allAggs || allAggs.length === 0) {
      allAggs = [];
      for (const col of stmt.columns) this.extractAggregates(col, allAggs);
      if (stmt.having) this.extractAggregates(stmt.having, allAggs);
      if (stmt.orderBy) {
        for (const ob of stmt.orderBy) this.extractAggregates(ob.expr, allAggs);
      }
    }

    for await (const row of source) {
      let key = "all";
      if (stmt.groupBy) {
        const keys = [];
        for (const g of stmt.groupBy)
          keys.push(await this.evaluateExpr(storage, g, row, params));
        key = keys.join("|");
      }

      if (!groups.has(key)) {
        groups.set(key, {
          __COUNT__: 0,
          __COUNTS__: {},
          __SUMS__: {},
          __ARRAYS__: {},
          __JSON_OBJ_AGGS__: {},
          __DISTINCTS__: {},
          baseRow: row,
        });
      }
      const state = groups.get(key);
      state.__COUNT__++;

      for (const target of allAggs) {
        const colName = this.getExprKey(target);
        if ((target as any).filter) {
          if (
            !(await this.evaluateExpr(
              storage,
              (target as any).filter,
              row,
              params,
            ))
          )
            continue;
        }

        if ((target as any).fnName === "COUNT") {
          if (
            (target as any).distinct &&
            (target as any).args &&
            (target as any).args[0]
          ) {
            const val = await this.evaluateExpr(
              storage,
              (target as any).args[0],
              row,
              params,
            );
            if (val !== null && val !== undefined) {
              if (!state.__DISTINCTS__[colName])
                state.__DISTINCTS__[colName] = new Set();
              const valKey =
                typeof val === "object" ? JSON.stringify(val) : val;
              state.__DISTINCTS__[colName].add(valKey);
            }
          } else {
            let isNotNull = true;
            if (
              (target as any).args &&
              (target as any).args[0] &&
              !(
                (target as any).args[0].type === "Identifier" &&
                (target as any).args[0].name === "*"
              )
            ) {
              const val = await this.evaluateExpr(
                storage,
                (target as any).args[0],
                row,
                params,
              );
              if (val === null || val === undefined) isNotNull = false;
            }
            if (isNotNull)
              state.__COUNTS__[colName] = (state.__COUNTS__[colName] || 0) + 1;
          }
        } else if ((target as any).fnName === "SUM") {
          if (state.__SUMS__[colName] === undefined)
            state.__SUMS__[colName] = null;
          if ((target as any).args && (target as any).args[0]) {
            const val = await this.evaluateExpr(
              storage,
              (target as any).args[0],
              row,
              params,
            );
            if (val !== null && val !== undefined) {
              const num = Number(val);
              if (!isNaN(num)) {
                if (state.__SUMS__[colName] === null)
                  state.__SUMS__[colName] = 0;
                state.__SUMS__[colName] += num;
              }
            }
          }
        } else if ((target as any).fnName === "MIN") {
          if ((target as any).args && (target as any).args[0]) {
            const val = await this.evaluateExpr(
              storage,
              (target as any).args[0],
              row,
              params,
            );
            if (val !== null && val !== undefined) {
              if (state.__SUMS__[colName] === undefined)
                state.__SUMS__[colName] = val;
              else if (val < state.__SUMS__[colName])
                state.__SUMS__[colName] = val;
            }
          }
        } else if ((target as any).fnName === "MAX") {
          if ((target as any).args && (target as any).args[0]) {
            const val = await this.evaluateExpr(
              storage,
              (target as any).args[0],
              row,
              params,
            );
            if (val !== null && val !== undefined) {
              if (state.__SUMS__[colName] === undefined)
                state.__SUMS__[colName] = val;
              else if (val > state.__SUMS__[colName])
                state.__SUMS__[colName] = val;
            }
          }
        } else if ((target as any).fnName === "AVG") {
          if (!state.__SUMS__[colName]) state.__SUMS__[colName] = 0;
          if ((target as any).args && (target as any).args[0]) {
            const val = await this.evaluateExpr(
              storage,
              (target as any).args[0],
              row,
              params,
            );
            if (val !== null && val !== undefined) {
              const num = Number(val);
              if (!isNaN(num)) {
                state.__SUMS__[colName] += num;
                state.__COUNTS__[colName] =
                  (state.__COUNTS__[colName] || 0) + 1;
              }
            }
          }
        } else if (
          (target as any).fnName === "ARRAY_AGG" ||
          (target as any).fnName === "JSON_AGG" ||
          (target as any).fnName === "JSONB_AGG"
        ) {
          if (!state.__ARRAYS__[colName]) state.__ARRAYS__[colName] = [];
          if ((target as any).args && (target as any).args[0]) {
            const val = await this.evaluateExpr(
              storage,
              (target as any).args[0],
              row,
              params,
            );
            if ((target as any).argsOrderBy) {
              const orderVals = [];
              for (const ob of (target as any).argsOrderBy)
                orderVals.push(
                  await this.evaluateExpr(storage, ob.expr, row, params),
                );
              state.__ARRAYS__[colName].push({ val, orderVals });
            } else {
              state.__ARRAYS__[colName].push(val);
            }
          }
        } else if (
          (target as any).fnName === "JSON_OBJECT_AGG" ||
          (target as any).fnName === "JSONB_OBJECT_AGG"
        ) {
          if (!state.__JSON_OBJ_AGGS__[colName])
            state.__JSON_OBJ_AGGS__[colName] = {};
          if ((target as any).args && (target as any).args.length >= 2) {
            const k = await this.evaluateExpr(
              storage,
              (target as any).args[0],
              row,
              params,
            );
            const v = await this.evaluateExpr(
              storage,
              (target as any).args[1],
              row,
              params,
            );
            if (k !== null) {
              state.__JSON_OBJ_AGGS__[colName][String(k)] = v;
            }
          }
        }
      }
    }

    if (groups.size === 0 && !stmt.groupBy) {
      groups.set("all", {
        __COUNT__: 0,
        __COUNTS__: {},
        __SUMS__: {},
        __ARRAYS__: {},
        __JSON_OBJ_AGGS__: {},
        __DISTINCTS__: {},
        baseRow: {},
      });
    }

    for (const state of groups.values()) {
      const outRow: any = { ...state.baseRow, __COUNT__: state.__COUNT__ };

      for (const target of allAggs) {
        const colName = this.getExprKey(target);
        const fn = (target as any).fnName;
        if (fn === "COUNT") {
          const count = (target as any).distinct
            ? state.__DISTINCTS__[colName]?.size || 0
            : state.__COUNTS__[colName] || 0;
          outRow[colName] = count;
        } else if (fn === "SUM" || fn === "MIN" || fn === "MAX") {
          const val =
            state.__SUMS__[colName] === undefined
              ? null
              : state.__SUMS__[colName];
          outRow[colName] = val;
        } else if (fn === "AVG") {
          const sum = state.__SUMS__[colName] || 0;
          const count = state.__COUNTS__[colName] || 0;
          const avg = count ? sum / count : null;
          outRow[colName] = avg;
        } else if (
          fn === "ARRAY_AGG" ||
          fn === "JSON_AGG" ||
          fn === "JSONB_AGG"
        ) {
          let arr = state.__ARRAYS__[colName] || [];
          const obList = (target as any).argsOrderBy;
          if (obList && arr.length > 0) {
            arr.sort((a: any, b: any) => {
              for (let i = 0; i < obList.length; i++) {
                const ob = obList[i];
                const vA = a.orderVals[i];
                const vB = b.orderVals[i];
                if (
                  (vA === null || vA === undefined) &&
                  vB !== null &&
                  vB !== undefined
                )
                  return ob.nullsFirst
                    ? -1
                    : ob.nullsLast
                      ? 1
                      : ob.desc
                        ? -1
                        : 1;
                if (
                  vA !== null &&
                  vA !== undefined &&
                  (vB === null || vB === undefined)
                )
                  return ob.nullsFirst
                    ? 1
                    : ob.nullsLast
                      ? -1
                      : ob.desc
                        ? 1
                        : -1;
                if (vA < vB) return ob.desc ? 1 : -1;
                if (vA > vB) return ob.desc ? -1 : 1;
              }
              return 0;
            });
            arr = arr.map((x: any) => x.val);
          }
          outRow[colName] = arr;
        } else if (fn === "JSON_OBJECT_AGG" || fn === "JSONB_OBJECT_AGG") {
          const obj = state.__JSON_OBJ_AGGS__[colName] || {};
          outRow[colName] = obj;
        }
      }

      for (const col of stmt.columns) {
        let target = col;
        let alias = null;
        if (col.type === "Alias") {
          alias = col.alias;
          target = col.expr;
        }
        let outKey = alias;
        if (!outKey) {
          if (target.type === "Call") outKey = target.fnName.toLowerCase();
          else if ((target as any).name)
            outKey = (target as any).name.includes(".")
              ? (target as any).name.split(".")[1]
              : (target as any).name;
          else outKey = "col";
        }
        if (outRow[outKey] !== undefined) {
          let suffix = 1;
          while (outRow[`${outKey}${suffix}`] !== undefined) suffix++;
          outKey = `${outKey}${suffix}`;
        }
        outRow[outKey] = await this.evaluateExpr(
          storage,
          target,
          outRow,
          params,
        );
      }
      if (stmt.having) {
        if (await this.evaluateExpr(storage, stmt.having, outRow, params))
          yield outRow;
      } else {
        yield outRow;
      }
    }
  }

  private castValueSync(val: any, dataType: string): any {
    if (val === null || val === undefined) return null;
    const dt = dataType.toUpperCase().split("(")[0]?.trim();

    const numerics = [
      "INT",
      "INTEGER",
      "SMALLINT",
      "BIGINT",
      "DECIMAL",
      "NUMERIC",
      "REAL",
      "DOUBLE",
      "PRECISION",
      "NUMBER",
      "SERIAL",
      "BIGSERIAL",
      "SMALLSERIAL",
      "MONEY",
      "OID",
      "INT2",
      "INT4",
      "INT8",
      "FLOAT4",
      "FLOAT8",
    ];
    if (numerics.includes(dt!)) {
      const num = Number(val);
      return isNaN(num) ? null : num;
    } else if (dt === "BOOLEAN" || dt === "BOOL") {
      if (typeof val === "boolean") return val;
      const s = String(val).toUpperCase();
      if (s === "TRUE" || s === "T" || s === "1" || s === "Y" || s === "YES")
        return true;
      if (s === "FALSE" || s === "F" || s === "0" || s === "N" || s === "NO")
        return false;
      return Boolean(val);
    } else if (
      dt === "TEXT" ||
      dt === "VARCHAR" ||
      dt === "CHAR" ||
      dt === "CHARACTER" ||
      dt === "UUID" ||
      dt === "STRING"
    ) {
      if (val instanceof Date) return val.toISOString();
      return typeof val === "object" ? JSON.stringify(val) : String(val);
    } else if (dt?.includes("JSON")) {
      if (typeof val === "string") {
        try {
          return JSON.parse(val);
        } catch {
          return null;
        }
      }
      return val;
    } else if (dt?.endsWith("[]") || dt === "ARRAY") {
      if (typeof val === "string") {
        if (val.trim().startsWith("{") && val.trim().endsWith("}")) {
          try {
            const jsonStr = val
              .trim()
              .replace(/^{/, "[")
              .replace(/}$/, "]")
              .replace(/NULL/gi, "null");
            return JSON.parse(jsonStr);
          } catch {
            return val;
          }
        }
        try {
          return JSON.parse(val);
        } catch {
          return [val];
        }
      }
      return Array.isArray(val) ? val : [val];
    } else if (dt?.includes("TIMESTAMP") || dt?.includes("DATETIME")) {
      if (val instanceof Date) return val.toISOString();
      let strVal = val;
      if (typeof val === "string") strVal = val.replace(/^"|"$/g, "");
      const d = new Date(strVal);
      return isNaN(d.getTime()) ? null : d.toISOString();
    } else if (dt?.includes("DATE")) {
      if (val instanceof Date) return val.toISOString().split("T")[0];
      let strVal = val;
      if (typeof val === "string") strVal = val.replace(/^"|"$/g, "").trim();
      if (typeof strVal === "string" && /^\d{4}-\d{2}-\d{2}$/.test(strVal))
        return strVal;
      const d = new Date(strVal);
      return isNaN(d.getTime()) ? null : d.toISOString().split("T")[0];
    } else if (dt?.includes("TIME")) {
      if (val instanceof Date) {
        const timePart = val.toISOString().split("T")[1];
        return timePart ? timePart.replace("Z", "") : null;
      }
      let strVal = val;
      if (typeof val === "string") {
        strVal = val.replace(/^"|"$/g, "").trim();
        if (
          /^\d{1,2}:\d{2}(:\d{2}(\.\d+)?)?(Z|[-+]\d{2}(:?\d{2})?)?$/i.test(
            strVal,
          )
        )
          return strVal;
      }
      const d = new Date(strVal);
      if (!isNaN(d.getTime())) {
        const timePart = d.toISOString().split("T")[1];
        return timePart ? timePart.replace("Z", "") : strVal;
      }
      const t = new Date(`1970-01-01T${strVal}Z`);
      if (!isNaN(t.getTime())) return String(strVal);
      const t2 = new Date(`1970-01-01 ${strVal}`);
      if (!isNaN(t2.getTime())) {
        const timePart = t2.toISOString().split("T")[1];
        return timePart ? timePart.replace("Z", "") : strVal;
      }
      return null;
    }
    return val;
  }

  private async castValue(
    storage: StorageEngine,
    val: any,
    dataType: string,
  ): Promise<any> {
    if (val === null || val === undefined) return null;
    const dt = dataType.toUpperCase().split("(")[0]?.trim();

    if (dt === "REGCLASS" || dt === "REGTYPE" || dt === "REGNAMESPACE") {
      if (typeof val === "string") {
        try {
          if (dt === "REGNAMESPACE") {
            const name = val.trim().replace(/^"|"$/g, "").replace(/""/g, '"');
            for await (const row of storage.scanRows("pg_namespace")) {
              if (row.nspname === name) return row.oid;
            }
            return null;
          } else if (dt === "REGTYPE") {
            const typeName = val
              .trim()
              .replace(/^"|"$/g, "")
              .replace(/""/g, '"');
            for await (const row of storage.scanRows("pg_catalog.pg_type")) {
              if (row.typname === typeName) return row.oid;
            }
            return null;
          } else {
            const tableName = val
              .split(".")
              .map((p) => p.trim().replace(/^"|"$/g, "").replace(/""/g, '"'))
              .join(".");
            const tbl = await (storage as any).getTableAsync(tableName);
            return tbl?.firstPage || null;
          }
        } catch {
          return null;
        }
      } else if (typeof val === "number") {
        return val;
      }
      const num = Number(val);
      return isNaN(num) ? null : num;
    }

    return this.castValueSync(val, dataType);
  }

  private getDatePart(date: Date, field: string): number {
    field = field.toUpperCase();
    switch (field) {
      case "YEAR":
        return date.getFullYear();
      case "MONTH":
        return date.getMonth() + 1;
      case "DAY":
        return date.getDate();
      case "HOUR":
        return date.getHours();
      case "MINUTE":
        return date.getMinutes();
      case "SECOND":
        return date.getSeconds();
      case "MILLISECONDS":
        return date.getMilliseconds();
      case "EPOCH":
        return Math.floor(date.getTime() / 1000);
      case "DOW":
        return date.getDay();
      case "DOY": {
        const start = new Date(date.getFullYear(), 0, 0);
        const diff = date.getTime() - start.getTime();
        const oneDay = 1000 * 60 * 60 * 24;
        return Math.floor(diff / oneDay);
      }
      case "QUARTER":
        return Math.floor(date.getMonth() / 3) + 1;
      case "WEEK": {
        const d = new Date(
          Date.UTC(date.getFullYear(), date.getMonth(), date.getDate()),
        );
        const dayNum = d.getUTCDay() || 7;
        d.setUTCDate(d.getUTCDate() + 4 - dayNum);
        const yearStart = new Date(Date.UTC(d.getUTCFullYear(), 0, 1));
        return Math.ceil(
          ((d.getTime() - yearStart.getTime()) / 86400000 + 1) / 7,
        );
      }
      default:
        return 0;
    }
  }

  private calculateAge(ts1: Date, ts2: Date): string {
    let diff = ts1.getTime() - ts2.getTime();
    const isNegative = diff < 0;
    if (isNegative) {
      const temp = ts1;
      ts1 = ts2;
      ts2 = temp;
    }

    let years = ts1.getFullYear() - ts2.getFullYear();
    let months = ts1.getMonth() - ts2.getMonth();
    let days = ts1.getDate() - ts2.getDate();

    if (days < 0) {
      months -= 1;
      const prevMonthLastDay = new Date(
        ts1.getFullYear(),
        ts1.getMonth(),
        0,
      ).getDate();
      days += prevMonthLastDay;
    }
    if (months < 0) {
      years -= 1;
      months += 12;
    }

    const parts = [];
    if (years !== 0) parts.push(`${years} year${years > 1 ? "s" : ""}`);
    if (months !== 0) parts.push(`${months} month${months > 1 ? "s" : ""}`);
    if (days !== 0) parts.push(`${days} day${days > 1 ? "s" : ""}`);

    let res = parts.length === 0 ? "0 days" : parts.join(" ");
    return isNegative ? `-${res}` : res;
  }

  private deepSet(
    obj: any,
    path: any[],
    value: any,
    createMissing: boolean,
  ): any {
    if (path.length === 0) return value;
    const key = path[0];

    if (obj === null || (typeof obj !== "object" && !Array.isArray(obj))) {
      if (!createMissing) return obj;
      obj = {};
    }

    let nextObj: any;
    let actualKey: any = key;

    if (Array.isArray(obj)) {
      nextObj = [...obj];
      actualKey = parseInt(String(key));
      if (isNaN(actualKey)) return obj;
    } else {
      nextObj = { ...obj };
    }

    if (path.length === 1) {
      if (!createMissing && nextObj[actualKey] === undefined) return obj;
      nextObj[actualKey] = value;
      return nextObj;
    }

    if (nextObj[actualKey] === undefined) {
      if (!createMissing) return obj;
      nextObj[actualKey] = {};
    }

    nextObj[actualKey] = this.deepSet(
      nextObj[actualKey],
      path.slice(1),
      value,
      createMissing,
    );
    return nextObj;
  }

  private deepInsert(
    obj: any,
    path: any[],
    value: any,
    insertAfter: boolean,
  ): any {
    if (path.length === 0) return value;
    const key = path[0];

    if (path.length === 1) {
      if (!Array.isArray(obj)) return obj;
      const newArr = [...obj];
      let idx = parseInt(String(key));
      if (isNaN(idx)) return obj;
      if (idx < 0) idx = newArr.length + idx;
      newArr.splice(insertAfter ? idx + 1 : idx, 0, value);
      return newArr;
    }

    if (obj === null || typeof obj !== "object") return obj;

    let nextObj = Array.isArray(obj) ? [...obj] : { ...obj };
    let actualKey: any = key;
    if (Array.isArray(obj)) {
      actualKey = parseInt(String(key));
      if (isNaN(actualKey)) return obj;
    }

    if (nextObj[actualKey] === undefined) return obj;

    nextObj[actualKey] = this.deepInsert(
      nextObj[actualKey],
      path.slice(1),
      value,
      insertAfter,
    );
    return nextObj;
  }

  private stripNulls(obj: any): any {
    if (Array.isArray(obj)) {
      return obj.map((v) => this.stripNulls(v));
    }
    if (obj !== null && typeof obj === "object") {
      const res: any = {};
      for (const k in obj) {
        if (obj[k] !== null) {
          res[k] = this.stripNulls(obj[k]);
        }
      }
      return res;
    }
    return obj;
  }

  private jsonTypeof(val: any): string {
    if (val === null) return "null";
    if (Array.isArray(val)) return "array";
    if (typeof val === "object") return "object";
    if (typeof val === "number") return "number";
    if (typeof val === "string") return "string";
    if (typeof val === "boolean") return "boolean";
    return "null";
  }

  private evaluateExprSync(
    storage: StorageEngine,
    expr: Expr,
    row: any,
    params: any = [],
  ): any {
    switch (expr.type) {
      case "Literal":
        return expr.value;
      case "Parameter": {
        if (!Array.isArray(params) || expr.index > params.length) {
          throw new Error(
            `bind message supplies ${Array.isArray(params) ? params.length : 0} parameters, but prepared statement requires at least ${expr.index}`,
          );
        }
        const pVal = params[expr.index - 1];
        if (
          typeof pVal === "string" &&
          pVal.includes(".") &&
          !pVal.includes(" ") &&
          !pVal.includes("%")
        ) {
          const parts = pVal.split(".");
          if (parts.length === 2) {
            const tbl = parts[0]!;
            const col = parts[1]!;
            const tblObj = row["__lpg_tbl_" + tbl] || row[tbl];
            if (tblObj && tblObj[col] !== undefined) {
              return tblObj[col];
            }
          }
        }
        return pVal instanceof Date ? pVal.toISOString() : pVal;
      }
      case "NamedParameter": {
        if (
          !params ||
          (params[expr.name] === undefined && !(expr.name in params))
        ) {
          throw new Error(
            `bind message does not supply named parameter '${expr.name}'`,
          );
        }
        const npVal = params[expr.name];
        return npVal instanceof Date ? npVal.toISOString() : npVal;
      }
      case "Identifier": {
        if ((expr as any)._nameUpper === undefined) {
          (expr as any)._nameUpper = expr.name.toUpperCase();
        }
        const nameUpper = (expr as any)._nameUpper;
        if (nameUpper === "CURRENT_TIMESTAMP") return new Date().toISOString();
        if (nameUpper === "CURRENT_DATE")
          return new Date().toISOString().split("T")[0];
        if (nameUpper === "CURRENT_TIME")
          return new Date().toISOString().split("T")[1];
        if (nameUpper === "LOCALTIMESTAMP") return new Date().toISOString();
        if (nameUpper === "LOCALTIME")
          return new Date().toISOString().split("T")[1];

        if (expr.name === "*") return "*";

        if ((expr as any)._isNested === undefined) {
          (expr as any)._isNested = expr.name.includes(".");
          if ((expr as any)._isNested) {
            const parts = expr.name.split(".");
            (expr as any)._col = parts.pop()!;
            (expr as any)._tbl = parts.join(".");
            (expr as any)._tblProp = "__lpg_tbl_" + (expr as any)._tbl;
          }
        }

        if ((expr as any)._isNested) {
          const tblObj =
            row[(expr as any)._tblProp] || row[(expr as any)._tbl];
          if (tblObj && tblObj[(expr as any)._col] !== undefined) {
            return tblObj[(expr as any)._col];
          }
          if (row[expr.name] !== undefined) return row[expr.name];
          return null;
        }

        if (row[expr.name] !== undefined) return row[expr.name];
        if ((expr as any).isDoubleQuoted) return expr.name;
        return null;
      }
      case "Binary": {
        const left = this.evaluateExprSync(storage, expr.left, row, params);
        const right = this.evaluateExprSync(storage, expr.right, row, params);
        switch (expr.operator) {
          case "=":
            return left == right;
          case "!=":
            return left != right;
          case ">":
            return left > right;
          case "<":
            return left < right;
          case ">=":
            return left >= right;
          case "<=":
            return left <= right;
          case "+":
          case "-": {
            if (expr.operator === "-") {
              if (left !== null && typeof left === "object") {
                if (Array.isArray(left)) {
                  if (typeof right === "number") {
                    let idx = right;
                    if (idx < 0) idx = left.length + idx;
                    return left.filter((_, i) => i !== idx);
                  } else {
                    return left.filter((v) => String(v) !== String(right));
                  }
                } else {
                  if (typeof right === "string") {
                    const res = { ...left };
                    delete res[right];
                    return res;
                  } else if (Array.isArray(right)) {
                    const res = { ...left };
                    for (const k of right) delete res[String(k)];
                    return res;
                  }
                }
              }
            }
            if (
              typeof left === "string" &&
              left.includes("-") &&
              !isNaN(Date.parse(left)) &&
              typeof right === "string"
            ) {
              const parts = right.toLowerCase().trim().split(/\s+/);
              const val = parseFloat(parts[0] || "0");
              const unit = parts[1] || "day";
              const d = new Date(left);
              const multiplier = expr.operator === "+" ? 1 : -1;
              if (unit.startsWith("year"))
                d.setFullYear(d.getFullYear() + multiplier * val);
              else if (unit.startsWith("month"))
                d.setMonth(d.getMonth() + multiplier * val);
              else if (unit.startsWith("day"))
                d.setDate(d.getDate() + multiplier * val);
              else if (unit.startsWith("hour"))
                d.setHours(d.getHours() + multiplier * val);
              else if (unit.startsWith("minute"))
                d.setMinutes(d.getMinutes() + multiplier * val);
              else if (unit.startsWith("second"))
                d.setSeconds(d.getSeconds() + multiplier * val);
              return d.toISOString();
            }
            return expr.operator === "+" ? left + right : left - right;
          }
          case "*":
            return left * right;
          case "/":
            return left / right;
          case "||":
            if (left == null || right == null) return null;
            if (typeof left === "object" && typeof right === "object") {
              if (Array.isArray(left) && Array.isArray(right))
                return [...left, ...right];
              if (Array.isArray(left)) return [...left, right];
              if (Array.isArray(right)) return [left, ...right];
              return { ...left, ...right };
            }
            return String(left) + String(right);
          case "~":
            return (
              left != null &&
              right != null &&
              new RegExp(String(right)).test(String(left))
            );
          case "~*":
            return (
              left != null &&
              right != null &&
              new RegExp(String(right), "i").test(String(left))
            );
          case "!~":
            return (
              left != null &&
              right != null &&
              !new RegExp(String(right)).test(String(left))
            );
          case "->":
            return left != null && typeof left === "object"
              ? left[right]
              : null;
          case "->>":
            return left != null && typeof left === "object"
              ? left[right] != null
                ? String(left[right])
                : null
              : null;
          case "#>": {
            let path = right;
            if (typeof path === "string") {
              const trimmed = path.trim();
              if (trimmed === "{}") path = [];
              else if (trimmed.startsWith("{") && trimmed.endsWith("}")) {
                path = trimmed
                  .slice(1, -1)
                  .split(",")
                  .map((s) => s.trim().replace(/^"|"$/g, ""));
              } else path = [path];
            }
            if (left == null || !Array.isArray(path)) return null;
            let curr = left;
            for (const p of path) curr = curr?.[p];
            return curr;
          }
          case "#-": {
            let path = right;
            if (typeof path === "string") {
              const trimmed = path.trim();
              if (trimmed === "{}") path = [];
              else if (trimmed.startsWith("{") && trimmed.endsWith("}")) {
                path = trimmed
                  .slice(1, -1)
                  .split(",")
                  .map((s) => s.trim().replace(/^"|"$/g, ""));
              } else path = [path];
            }
            if (left == null || !Array.isArray(path)) return left;

            const deletePath = (obj: any, p: any[]): any => {
              if (p.length === 0) return obj;
              if (obj === null || typeof obj !== "object") return obj;

              const key = p[0];
              if (p.length === 1) {
                if (Array.isArray(obj)) {
                  let idx = parseInt(String(key));
                  if (isNaN(idx)) return obj;
                  if (idx < 0) idx = obj.length + idx;
                  if (idx < 0 || idx >= obj.length) return obj;
                  const newArr = [...obj];
                  newArr.splice(idx, 1);
                  return newArr;
                } else {
                  const newObj = { ...obj };
                  delete newObj[key];
                  return newObj;
                }
              }

              if (Array.isArray(obj)) {
                let idx = parseInt(String(key));
                if (isNaN(idx)) return obj;
                if (idx < 0) idx = obj.length + idx;
                if (idx < 0 || idx >= obj.length) return obj;

                const newArr = [...obj];
                newArr[idx] = deletePath(newArr[idx], p.slice(1));
                return newArr;
              } else {
                if (obj[key] === undefined) return obj;
                const newObj = { ...obj };
                newObj[key] = deletePath(newObj[key], p.slice(1));
                return newObj;
              }
            };

            return deletePath(left, path);
          }
          case "@>":
            if (Array.isArray(left) && Array.isArray(right))
              return right.every((v) => left.includes(v));
            if (
              typeof left === "object" &&
              typeof right === "object" &&
              left !== null &&
              right !== null
            ) {
              return Object.keys(right).every(
                (k) => JSON.stringify(left[k]) === JSON.stringify(right[k]),
              );
            }
            return false;
          case "?":
            if (Array.isArray(left)) return left.includes(right);
            if (typeof left === "object" && left !== null)
              return Object.prototype.hasOwnProperty.call(left, right);
            return false;
          case "&&":
            if (Array.isArray(left) && Array.isArray(right))
              return left.some((v) => right.includes(v));
            return false;
        }
        return false;
      }
      case "Logical": {
        if (expr.operator === "AND") {
          const left = this.evaluateExprSync(storage, expr.left, row, params);
          if (!left) return false;
          const right = this.evaluateExprSync(storage, expr.right, row, params);
          return !!right;
        }
        if (expr.operator === "OR") {
          const left = this.evaluateExprSync(storage, expr.left, row, params);
          if (left) return true;
          const right = this.evaluateExprSync(storage, expr.right, row, params);
          return !!right;
        }
        return false;
      }
      case "Like": {
        const left = this.evaluateExprSync(storage, expr.left, row, params);
        if (typeof left !== "string") return false;

        let regex: RegExp = (expr as any)._cachedRegex;
        if (!regex) {
          const right = this.evaluateExprSync(storage, expr.right, row, params);
          if (typeof right !== "string") return false;
          const escapeChar =
            (expr as any).escapeStr !== undefined
              ? (expr as any).escapeStr
              : "\\";
          let pattern = "";
          for (let i = 0; i < right.length; i++) {
            const ch = right[i];
            if (ch === escapeChar && i + 1 < right.length && escapeChar !== "") {
              const next = right[i + 1];
              if (next === "_" || next === "%" || next === escapeChar) {
                pattern += next!.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
                i++;
                continue;
              }
            }
            if (ch === "%") {
              pattern += ".*";
            } else if (ch === "_") {
              pattern += ".";
            } else {
              pattern += ch!.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
            }
          }
          regex = new RegExp(
            "^" + pattern + "$",
            (expr as any).ilike ? "i" : "",
          );
          if (expr.right?.type === "Literal" || expr.right?.type === "Parameter") {
            (expr as any)._cachedRegex = regex;
          }
        }
        const res = regex.test(left);
        return (expr as any).not ? !res : res;
      }
      case "In": {
        const left = this.evaluateExprSync(storage, expr.left, row, params);
        let res = false;
        if (Array.isArray(expr.right)) {
          let valSet: Set<any> | undefined = (expr as any)._cachedSet;
          if (!valSet) {
            let isStatic = true;
            for (const e of expr.right) {
              if (e.type === "Identifier") {
                isStatic = false;
                break;
              }
            }
            if (isStatic) {
              valSet = new Set();
              for (const e of expr.right) {
                const evaluated = this.evaluateExprSync(storage, e, {}, params);
                if (Array.isArray(evaluated) && expr.right.length === 1) {
                  for (const v of evaluated) {
                    valSet.add(v);
                    valSet.add(String(v));
                    if (typeof v === "number") valSet.add(Number(v));
                  }
                } else {
                  valSet.add(evaluated);
                  valSet.add(String(evaluated));
                  if (typeof evaluated === "number") valSet.add(Number(evaluated));
                }
              }
              (expr as any)._cachedSet = valSet;
            }
          }

          if (valSet) {
            res = valSet.has(left) || valSet.has(String(left)) || (typeof left === "number" && valSet.has(Number(left)));
          } else {
            let vals: any[] = [];
            for (const e of expr.right) {
              const evaluated = this.evaluateExprSync(storage, e, row, params);
              if (Array.isArray(evaluated) && expr.right.length === 1) {
                vals = vals.concat(evaluated);
              } else {
                vals.push(evaluated);
              }
            }
            res = vals.some((v) => v == left);
          }
        }
        return (expr as any).not ? !res : res;
      }
      case "Interval":
        return expr.value;
      case "Extract": {
        const val = this.evaluateExprSync(storage, expr.source, row, params);
        if (val == null) return null;
        const d = new Date(val);
        if (isNaN(d.getTime())) return null;
        return this.getDatePart(d, expr.field);
      }
      case "Alias":
        return this.evaluateExprSync(storage, expr.expr, row, params);
      case "Not":
        return !this.evaluateExprSync(storage, expr.expr, row, params);
      case "IsNull": {
        const val = this.evaluateExprSync(storage, expr.expr, row, params);
        const isNull = val === null || val === undefined;
        return expr.not ? !isNull : isNull;
      }
      case "Case": {
        for (const c of expr.cases) {
          if (this.evaluateExprSync(storage, c.when, row, params)) {
            return this.evaluateExprSync(storage, c.then, row, params);
          }
        }
        if (expr.elseExpr)
          return this.evaluateExprSync(storage, expr.elseExpr, row, params);
        return null;
      }
      case "Array": {
        const elements = expr.elements;
        const res = new Array(elements.length);
        for (let i = 0; i < elements.length; i++) {
          res[i] = this.evaluateExprSync(storage, elements[i], row, params);
        }
        return res;
      }
      case "Cast": {
        const val = this.evaluateExprSync(storage, expr.expr, row, params);
        return this.castValueSync(val, expr.dataType);
      }
      case "Call": {
        const key = this.getExprKey(expr);
        if (row[key] !== undefined) return row[key];

        if ((expr as any)._fnNameUpper === undefined) {
          let rawFnName = expr.fnName;
          if (rawFnName.includes(".")) rawFnName = rawFnName.split(".").pop()!;
          (expr as any)._fnNameUpper = rawFnName.toUpperCase();
        }
        const fnName = (expr as any)._fnNameUpper;
        if (fnName === "COUNT") return row.__COUNT__ || 0;
        if (fnName === "AVG") return row.__AVG__ || 0;
        if (fnName === "SUM" || fnName === "MIN" || fnName === "MAX")
          return null;
        if (
          fnName === "ARRAY_AGG" ||
          fnName === "JSON_AGG" ||
          fnName === "JSONB_AGG"
        )
          return [];
        if (fnName === "JSON_OBJECT_AGG" || fnName === "JSONB_OBJECT_AGG")
          return {};

        const args = [];
        for (const argExpr of expr.args) {
          args.push(this.evaluateExprSync(storage, argExpr, row, params));
        }

        if (fnName === "VERSION") return "PostgreSQL 16.2 (LitePostgres)";
        if (
          fnName === "NOW" ||
          fnName === "CURRENT_TIMESTAMP" ||
          fnName === "LOCALTIMESTAMP"
        )
          return new Date().toISOString();
        if (fnName === "CURRENT_DATE")
          return new Date().toISOString().split("T")[0];
        if (fnName === "CURRENT_TIME" || fnName === "LOCALTIME")
          return new Date().toISOString().split("T")[1];
        if (fnName === "UPPER")
          return args[0] != null ? String(args[0]).toUpperCase() : null;
        if (fnName === "LOWER")
          return args[0] != null ? String(args[0]).toLowerCase() : null;
        if (fnName === "LENGTH")
          return args[0] != null ? String(args[0]).length : null;
        if (fnName === "TRIM" || fnName === "BTRIM") {
          if (args[0] == null) return null;
          const s = String(args[0]);
          if (args[1] != null) {
            const chars = String(args[1]);
            const escaped = chars.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
            return s.replace(new RegExp(`^[${escaped}]+|[${escaped}]+$`, "g"), "");
          }
          return s.trim();
        }
        if (fnName === "LTRIM") {
          if (args[0] == null) return null;
          const s = String(args[0]);
          if (args[1] != null) {
            const chars = String(args[1]);
            const escaped = chars.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
            return s.replace(new RegExp(`^[${escaped}]+`, "g"), "");
          }
          return s.trimStart();
        }
        if (fnName === "RTRIM") {
          if (args[0] == null) return null;
          const s = String(args[0]);
          if (args[1] != null) {
            const chars = String(args[1]);
            const escaped = chars.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
            return s.replace(new RegExp(`[${escaped}]+$`, "g"), "");
          }
          return s.trimEnd();
        }
        if (fnName === "REPLACE") {
          if (args[0] == null || args[1] == null || args[2] == null)
            return args[0];
          return String(args[0]).split(String(args[1])).join(String(args[2]));
        }
        if (fnName === "SUBSTRING") {
          if (args[0] == null) return null;
          const str = String(args[0]);
          const start = (args[1] != null ? Number(args[1]) : 1) - 1;
          if (args[2] != null) {
            const count = Number(args[2]);
            return str.substring(start, start + count);
          }
          return str.substring(start);
        }
        if (fnName === "CONCAT") {
          return args
            .filter((v) => v != null)
            .map((v) => String(v))
            .join("");
        }
        if (fnName === "CONCAT_WS") {
          if (args[0] == null) return null;
          const sep = String(args[0]);
          return args
            .slice(1)
            .filter((v) => v != null)
            .map((v) => String(v))
            .join(sep);
        }
        if (fnName === "LTRIM")
          return args[0] != null ? String(args[0]).trimStart() : null;
        if (fnName === "RTRIM")
          return args[0] != null ? String(args[0]).trimEnd() : null;
        if (fnName === "LEFT") {
          if (args[0] == null || args[1] == null) return null;
          const str = String(args[0]);
          const n = Number(args[1]);
          return n >= 0
            ? str.substring(0, n)
            : str.substring(0, Math.max(0, str.length + n));
        }
        if (fnName === "RIGHT") {
          if (args[0] == null || args[1] == null) return null;
          const str = String(args[0]);
          const n = Number(args[1]);
          return n >= 0
            ? str.substring(Math.max(0, str.length - n))
            : str.substring(Math.max(0, -n));
        }
        if (fnName === "LPAD") {
          if (args[0] == null || args[1] == null) return null;
          const str = String(args[0]);
          const n = Number(args[1]);
          const fill = String(args[2] ?? " ");
          return str.length > n ? str.substring(0, n) : str.padStart(n, fill);
        }
        if (fnName === "RPAD") {
          if (args[0] == null || args[1] == null) return null;
          const str = String(args[0]);
          const n = Number(args[1]);
          const fill = String(args[2] ?? " ");
          return str.length > n ? str.substring(0, n) : str.padEnd(n, fill);
        }
        if (fnName === "INITCAP") {
          if (args[0] == null) return null;
          const str = String(args[0]).toLowerCase();
          return str.replace(/(^|\s)\S/g, (l) => l.toUpperCase());
        }
        if (fnName === "REVERSE") {
          return args[0] != null
            ? String(args[0]).split("").reverse().join("")
            : null;
        }
        if (fnName === "STRPOS") {
          if (args[0] == null || args[1] == null) return null;
          return String(args[0]).indexOf(String(args[1])) + 1;
        }
        if (fnName === "REPEAT") {
          if (args[0] == null || args[1] == null) return null;
          const n = Number(args[1]);
          return n > 0 ? String(args[0]).repeat(n) : "";
        }
        if (fnName === "SPLIT_PART") {
          if (args[0] == null || args[1] == null || args[2] == null)
            return null;
          const parts = String(args[0]).split(String(args[1]));
          const idx = Number(args[2]);
          return idx > 0 && idx <= parts.length ? parts[idx - 1] : "";
        }
        if (fnName === "COALESCE") {
          for (const val of args) {
            if (val !== null && val !== undefined) return val;
          }
          return null;
        }
        if (fnName === "ABS")
          return args[0] != null ? Math.abs(Number(args[0])) : null;
        if (fnName === "CEIL" || fnName === "CEILING")
          return args[0] != null ? Math.ceil(Number(args[0])) : null;
        if (fnName === "FLOOR")
          return args[0] != null ? Math.floor(Number(args[0])) : null;
        if (fnName === "ROUND") {
          if (args[0] == null) return null;
          const num = Number(args[0]);
          const precision = args[1] != null ? Math.floor(Number(args[1])) : 0;
          const factor = Math.pow(10, precision);
          return Math.round(num * factor) / factor;
        }
        if (fnName === "TRUNC") {
          if (args[0] == null) return null;
          const num = Number(args[0]);
          const precision = args[1] != null ? Math.floor(Number(args[1])) : 0;
          const factor = Math.pow(10, precision);
          return Math.trunc(num * factor) / factor;
        }
        if (fnName === "POWER" || fnName === "POW") {
          if (args[0] == null || args[1] == null) return null;
          return Math.pow(Number(args[0]), Number(args[1]));
        }
        if (fnName === "SQRT")
          return args[0] != null ? Math.sqrt(Number(args[0])) : null;
        if (fnName === "EXP")
          return args[0] != null ? Math.exp(Number(args[0])) : null;
        if (fnName === "LN")
          return args[0] != null ? Math.log(Number(args[0])) : null;
        if (fnName === "LOG")
          return args[0] != null ? Math.log10(Number(args[0])) : null;
        if (fnName === "MOD") {
          if (args[0] == null || args[1] == null) return null;
          return Number(args[0]) % Number(args[1]);
        }
        if (fnName === "SIGN") {
          if (args[0] == null) return null;
          const n = Number(args[0]);
          return n > 0 ? 1 : n < 0 ? -1 : 0;
        }
        if (fnName === "PI") return Math.PI;
        if (fnName === "RANDOM") return Math.random();
        if (fnName === "GEN_RANDOM_UUID" || fnName === "UUID_GENERATE_V4") {
          return "xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx".replace(
            /[xy]/g,
            function (c) {
              const r = (Math.random() * 16) | 0,
                v = c === "x" ? r : (r & 0x3) | 0x8;
              return v.toString(16);
            },
          );
        }
        if (fnName === "DEGREES")
          return args[0] != null ? Number(args[0]) * (180 / Math.PI) : null;
        if (fnName === "RADIANS")
          return args[0] != null ? Number(args[0]) * (Math.PI / 180) : null;

        if (fnName === "JSON_EXTRACT" || fnName === "JSONB_EXTRACT") {
          let json = args[0];
          if (typeof json === "string") {
            try {
              json = JSON.parse(json);
            } catch {
              return null;
            }
          }
          let current = json;
          for (let i = 1; i < args.length; i++) {
            current = current?.[args[i]];
          }
          return current;
        }

        if (fnName === "JSON_BUILD_OBJECT" || fnName === "JSONB_BUILD_OBJECT") {
          const obj: any = {};
          for (let i = 0; i < args.length; i += 2) {
            if (args[i] !== undefined && args[i] !== null) {
              obj[String(args[i])] = args[i + 1];
            }
          }
          return obj;
        }

        if (fnName === "JSON_BUILD_ARRAY" || fnName === "JSONB_BUILD_ARRAY") {
          return args;
        }

        if (fnName === "JSONB_SET") {
          let target = args[0];
          if (typeof target === "string") {
            try {
              target = JSON.parse(target);
            } catch (e) {}
          }
          let path = args[1];
          const newValue = args[2];
          const createMissing = args[3] !== false;
          if (typeof path === "string") {
            const trimmed = path.trim();
            if (trimmed === "{}") path = [];
            else if (trimmed.startsWith("{") && trimmed.endsWith("}")) {
              path = trimmed
                .slice(1, -1)
                .split(",")
                .map((s) => s.trim().replace(/^"|"$/g, ""));
            } else path = [path];
          }
          if (!Array.isArray(path)) return target;
          return this.deepSet(target, path, newValue, createMissing);
        }

        if (fnName === "JSONB_INSERT") {
          let target = args[0];
          if (typeof target === "string") {
            try {
              target = JSON.parse(target);
            } catch (e) {}
          }
          let path = args[1];
          const newValue = args[2];
          const insertAfter = args[3] === true;
          if (typeof path === "string") {
            const trimmed = path.trim();
            if (trimmed === "{}") path = [];
            else if (trimmed.startsWith("{") && trimmed.endsWith("}")) {
              path = trimmed
                .slice(1, -1)
                .split(",")
                .map((s) => s.trim().replace(/^"|"$/g, ""));
            } else path = [path];
          }
          if (!Array.isArray(path)) return target;
          return this.deepInsert(target, path, newValue, insertAfter);
        }

        if (fnName === "JSON_TYPEOF" || fnName === "JSONB_TYPEOF") {
          return this.jsonTypeof(args[0]);
        }

        if (fnName === "JSON_STRIP_NULLS" || fnName === "JSONB_STRIP_NULLS") {
          return this.stripNulls(args[0]);
        }

        if (fnName === "JSONB_PRETTY") {
          return JSON.stringify(args[0], null, 2);
        }
        if (fnName === "DATE_TRUNC") {
          const unit = String(args[0]).toLowerCase();
          const val = args[1];
          if (val == null) return null;
          const d = new Date(val);
          if (isNaN(d.getTime())) return null;
          switch (unit) {
            case "year":
              d.setMonth(0, 1);
              d.setHours(0, 0, 0, 0);
              break;
            case "month":
              d.setDate(1);
              d.setHours(0, 0, 0, 0);
              break;
            case "day":
              d.setHours(0, 0, 0, 0);
              break;
            case "hour":
              d.setMinutes(0, 0, 0);
              break;
            case "minute":
              d.setSeconds(0, 0);
              break;
            case "second":
              d.setMilliseconds(0);
              break;
          }
          return d.toISOString();
        }
        if (fnName === "AGE") {
          if (args.length === 0) return null;
          const t1 = new Date(args[0]);
          const t2 = args.length > 1 ? new Date(args[1]) : new Date();
          if (isNaN(t1.getTime()) || isNaN(t2.getTime())) return null;
          if (args.length === 1) return this.calculateAge(new Date(), t1);
          return this.calculateAge(t1, t2);
        }
        if (fnName === "TO_CHAR") {
          const val = args[0];
          const format = args[1];
          if (val == null || format == null) return null;
          const d = new Date(val);
          if (isNaN(d.getTime())) return String(val);
          let result = String(format);
          const months = [
            "January",
            "February",
            "March",
            "April",
            "May",
            "June",
            "July",
            "August",
            "September",
            "October",
            "November",
            "December",
          ];
          const days = [
            "Sunday",
            "Monday",
            "Tuesday",
            "Wednesday",
            "Thursday",
            "Friday",
            "Saturday",
          ];
          const pad = (n: number, l: number = 2) => String(n).padStart(l, "0");
          const replacements: Record<string, () => string> = {
            YYYY: () => String(d.getFullYear()),
            YY: () => String(d.getFullYear()).slice(-2),
            MM: () => pad(d.getMonth() + 1),
            DD: () => pad(d.getDate()),
            HH24: () => pad(d.getHours()),
            HH: () => pad(d.getHours() % 12 || 12),
            MI: () => pad(d.getMinutes()),
            SS: () => pad(d.getSeconds()),
            MS: () => pad(d.getMilliseconds(), 3),
            Month: () => months[d.getMonth()]!,
            Mon: () => months[d.getMonth()]!.slice(0, 3),
            Day: () => days[d.getDay()]!,
            Dy: () => days[d.getDay()]!.slice(0, 3),
          };
          const sortedPatterns = Object.keys(replacements).sort(
            (a, b) => b.length - a.length,
          );
          for (const pattern of sortedPatterns) {
            result = result.replace(
              new RegExp(pattern, "g"),
              replacements[pattern]!(),
            );
          }
          return result;
        }
        if (fnName === "DATE_PART") {
          const field = args[0];
          const source = args[1];
          if (field == null || source == null) return null;
          const d = new Date(source);
          if (isNaN(d.getTime())) return null;
          return this.getDatePart(d, String(field));
        }
        if (fnName === "QUOTE_IDENT") {
          if (args[0] == null) return null;
          const str = String(args[0]);
          return `"${str.replace(/"/g, '""')}"`;
        }
        if (fnName === "FORMAT_TYPE") {
          return args[0] != null ? String(args[0]) : null;
        }
        if (fnName === "PG_GET_EXPR") {
          const adbin = args[0];
          if (adbin == null) return null;
          try {
            const parsed = JSON.parse(String(adbin));
            if (
              parsed &&
              typeof parsed === "object" &&
              parsed.type === "Literal"
            ) {
              return String(parsed.value);
            }
            return String(adbin);
          } catch {
            return String(adbin);
          }
        }
        return null;
      }
      default:
        return null;
    }
  }

  private async evaluateExpr(
    storage: StorageEngine,
    expr: Expr,
    row: any,
    params: any = [],
  ): Promise<any> {
    if (!this.hasAsyncOps(expr)) {
      return this.evaluateExprSync(storage, expr, row, params);
    }

    switch (expr.type) {
      case "Literal":
        return expr.value;
      case "Parameter": {
        if (!Array.isArray(params) || expr.index > params.length) {
          throw new Error(
            `bind message supplies ${Array.isArray(params) ? params.length : 0} parameters, but prepared statement requires at least ${expr.index}`,
          );
        }
        const pVal = params[expr.index - 1];

        // Hack for ORMs like TypeORM/Knex mistakenly parameterizing identifiers in Joins
        if (
          typeof pVal === "string" &&
          pVal.includes(".") &&
          !pVal.includes(" ") &&
          !pVal.includes("%")
        ) {
          const parts = pVal.split(".");
          if (parts.length === 2) {
            const tbl = parts[0]!;
            const col = parts[1]!;
            const tblObj = row["__lpg_tbl_" + tbl] || row[tbl];
            if (tblObj && tblObj[col] !== undefined) {
              return tblObj[col];
            }
          }
        }

        return pVal instanceof Date ? pVal.toISOString() : pVal;
      }
      case "NamedParameter": {
        if (
          !params ||
          (params[expr.name] === undefined && !(expr.name in params))
        ) {
          throw new Error(
            `bind message does not supply named parameter '${expr.name}'`,
          );
        }
        const npVal = params[expr.name];
        return npVal instanceof Date ? npVal.toISOString() : npVal;
      }
      case "Identifier": {
        if ((expr as any)._nameUpper === undefined) {
          (expr as any)._nameUpper = expr.name.toUpperCase();
        }
        const nameUpper = (expr as any)._nameUpper;
        if (nameUpper === "CURRENT_TIMESTAMP") return new Date().toISOString();
        if (nameUpper === "CURRENT_DATE")
          return new Date().toISOString().split("T")[0];
        if (nameUpper === "CURRENT_TIME")
          return new Date().toISOString().split("T")[1];
        if (nameUpper === "LOCALTIMESTAMP") return new Date().toISOString();
        if (nameUpper === "LOCALTIME")
          return new Date().toISOString().split("T")[1];

        if (expr.name === "*") return "*";

        if ((expr as any)._isNested === undefined) {
          (expr as any)._isNested = expr.name.includes(".");
          if ((expr as any)._isNested) {
            const parts = expr.name.split(".");
            (expr as any)._col = parts.pop()!;
            (expr as any)._tbl = parts.join(".");
            (expr as any)._tblProp = "__lpg_tbl_" + (expr as any)._tbl;
          }
        }

        if ((expr as any)._isNested) {
          const tblObj =
            row[(expr as any)._tblProp] || row[(expr as any)._tbl];
          if (tblObj && tblObj[(expr as any)._col] !== undefined) {
            return tblObj[(expr as any)._col];
          }
          if (row[expr.name] !== undefined) return row[expr.name];
          // For nested identifiers like "tbl"."col", we should return null if not found,
          // never fall back to string literal.
          return null;
        }

        if (row[expr.name] !== undefined) return row[expr.name];
        // Only simple double-quoted identifiers fall back to string literals (SQLite style)
        if ((expr as any).isDoubleQuoted) return expr.name;
        return null;
      }
      case "Binary": {
        const left = await this.evaluateExpr(storage, expr.left, row, params);
        const right = await this.evaluateExpr(storage, expr.right, row, params);
        switch (expr.operator) {
          case "=":
            return left == right;
          case "!=":
            return left != right;
          case ">":
            return left > right;
          case "<":
            return left < right;
          case ">=":
            return left >= right;
          case "<=":
            return left <= right;
          case "+":
          case "-": {
            if (expr.operator === "-") {
              if (left !== null && typeof left === "object") {
                if (Array.isArray(left)) {
                  if (typeof right === "number") {
                    let idx = right;
                    if (idx < 0) idx = left.length + idx;
                    return left.filter((_, i) => i !== idx);
                  } else {
                    return left.filter((v) => String(v) !== String(right));
                  }
                } else {
                  if (typeof right === "string") {
                    const res = { ...left };
                    delete res[right];
                    return res;
                  } else if (Array.isArray(right)) {
                    const res = { ...left };
                    for (const k of right) delete res[String(k)];
                    return res;
                  }
                }
              }
            }
            if (
              typeof left === "string" &&
              left.includes("-") &&
              !isNaN(Date.parse(left)) &&
              typeof right === "string"
            ) {
              const parts = right.toLowerCase().trim().split(/\s+/);
              const val = parseFloat(parts[0] || "0");
              const unit = parts[1] || "day";
              const d = new Date(left);
              const multiplier = expr.operator === "+" ? 1 : -1;
              if (unit.startsWith("year"))
                d.setFullYear(d.getFullYear() + multiplier * val);
              else if (unit.startsWith("month"))
                d.setMonth(d.getMonth() + multiplier * val);
              else if (unit.startsWith("day"))
                d.setDate(d.getDate() + multiplier * val);
              else if (unit.startsWith("hour"))
                d.setHours(d.getHours() + multiplier * val);
              else if (unit.startsWith("minute"))
                d.setMinutes(d.getMinutes() + multiplier * val);
              else if (unit.startsWith("second"))
                d.setSeconds(d.getSeconds() + multiplier * val);
              return d.toISOString();
            }
            return expr.operator === "+" ? left + right : left - right;
          }
          case "*":
            return left * right;
          case "/":
            return left / right;
          case "||":
            if (left == null || right == null) return null;
            if (typeof left === "object" && typeof right === "object") {
              if (Array.isArray(left) && Array.isArray(right))
                return [...left, ...right];
              if (Array.isArray(left)) return [...left, right];
              if (Array.isArray(right)) return [left, ...right];
              return { ...left, ...right };
            }
            return String(left) + String(right);
          case "~":
            return (
              left != null &&
              right != null &&
              new RegExp(String(right)).test(String(left))
            );
          case "~*":
            return (
              left != null &&
              right != null &&
              new RegExp(String(right), "i").test(String(left))
            );
          case "!~":
            return (
              left != null &&
              right != null &&
              !new RegExp(String(right)).test(String(left))
            );
          case "->":
            return left != null && typeof left === "object"
              ? left[right]
              : null;
          case "->>":
            return left != null && typeof left === "object"
              ? left[right] != null
                ? String(left[right])
                : null
              : null;
          case "#>": {
            let path = right;
            if (typeof path === "string") {
              const trimmed = path.trim();
              if (trimmed === "{}") path = [];
              else if (trimmed.startsWith("{") && trimmed.endsWith("}")) {
                path = trimmed
                  .slice(1, -1)
                  .split(",")
                  .map((s) => s.trim().replace(/^"|"$/g, ""));
              } else path = [path];
            }
            if (left == null || !Array.isArray(path)) return null;
            let curr = left;
            for (const p of path) curr = curr?.[p];
            return curr;
          }
          case "#-": {
            let path = right;
            if (typeof path === "string") {
              const trimmed = path.trim();
              if (trimmed === "{}") path = [];
              else if (trimmed.startsWith("{") && trimmed.endsWith("}")) {
                path = trimmed
                  .slice(1, -1)
                  .split(",")
                  .map((s) => s.trim().replace(/^"|"$/g, ""));
              } else path = [path];
            }
            if (left == null || !Array.isArray(path)) return left;

            const deletePath = (obj: any, path: any[]): any => {
              if (path.length === 0) return obj;
              if (obj === null || typeof obj !== "object") return obj;

              const key = path[0];
              if (path.length === 1) {
                if (Array.isArray(obj)) {
                  let idx = parseInt(String(key));
                  if (isNaN(idx)) return obj;
                  if (idx < 0) idx = obj.length + idx;
                  if (idx < 0 || idx >= obj.length) return obj;
                  const newArr = [...obj];
                  newArr.splice(idx, 1);
                  return newArr;
                } else {
                  const newObj = { ...obj };
                  delete newObj[key];
                  return newObj;
                }
              }

              if (Array.isArray(obj)) {
                let idx = parseInt(String(key));
                if (isNaN(idx)) return obj;
                if (idx < 0) idx = obj.length + idx;
                if (idx < 0 || idx >= obj.length) return obj;

                const newArr = [...obj];
                newArr[idx] = deletePath(newArr[idx], path.slice(1));
                return newArr;
              } else {
                if (obj[key] === undefined) return obj;
                const newObj = { ...obj };
                newObj[key] = deletePath(newObj[key], path.slice(1));
                return newObj;
              }
            };

            return deletePath(left, path);
          }
          case "@>":
            if (Array.isArray(left) && Array.isArray(right))
              return right.every((v) => left.includes(v));
            if (
              typeof left === "object" &&
              typeof right === "object" &&
              left !== null &&
              right !== null
            ) {
              return Object.keys(right).every(
                (k) => JSON.stringify(left[k]) === JSON.stringify(right[k]),
              );
            }
            return false;
          case "?":
            if (Array.isArray(left)) return left.includes(right);
            if (typeof left === "object" && left !== null)
              return Object.prototype.hasOwnProperty.call(left, right);
            return false;
          case "&&":
            if (Array.isArray(left) && Array.isArray(right))
              return left.some((v) => right.includes(v));
            return false;
        }
        return false;
      }
      case "Logical": {
        if (expr.operator === "AND") {
          const left = await this.evaluateExpr(storage, expr.left, row, params);
          if (!left) return false;
          const right = await this.evaluateExpr(storage, expr.right, row, params);
          return !!right;
        }
        if (expr.operator === "OR") {
          const left = await this.evaluateExpr(storage, expr.left, row, params);
          if (left) return true;
          const right = await this.evaluateExpr(storage, expr.right, row, params);
          return !!right;
        }
        return false;
      }
      case "Like": {
        const left = await this.evaluateExpr(storage, expr.left, row, params);
        if (typeof left !== "string") return false;

        let regex: RegExp = (expr as any)._cachedRegex;
        if (!regex) {
          const right = await this.evaluateExpr(storage, expr.right, row, params);
          if (typeof right !== "string") return false;
          // Build regex with proper escape handling
          const escapeChar =
            (expr as any).escapeStr !== undefined
              ? (expr as any).escapeStr
              : "\\";
          let pattern = "";
          for (let i = 0; i < right.length; i++) {
            const ch = right[i];
            if (ch === escapeChar && i + 1 < right.length && escapeChar !== "") {
              const next = right[i + 1];
              if (next === "_" || next === "%" || next === escapeChar) {
                // Escaped special char → literal (escape for regex)
                pattern += next!.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
                i++;
                continue;
              }
            }
            if (ch === "%") {
              pattern += ".*";
            } else if (ch === "_") {
              pattern += ".";
            } else {
              pattern += ch!.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
            }
          }
          regex = new RegExp(
            "^" + pattern + "$",
            (expr as any).ilike ? "i" : "",
          );
          if (expr.right?.type === "Literal" || expr.right?.type === "Parameter") {
            (expr as any)._cachedRegex = regex;
          }
        }
        const res = regex.test(left);
        return (expr as any).not ? !res : res;
      }
      case "In": {
        const left = await this.evaluateExpr(storage, expr.left, row, params);
        let res = false;
        if (Array.isArray(expr.right)) {
          let valSet: Set<any> | undefined = (expr as any)._cachedSet;
          if (!valSet) {
            let isStatic = true;
            for (const e of expr.right) {
              if (e.type === "Identifier") {
                isStatic = false;
                break;
              }
            }
            if (isStatic) {
              valSet = new Set();
              for (const e of expr.right) {
                const evaluated = await this.evaluateExpr(storage, e, {}, params);
                if (Array.isArray(evaluated) && expr.right.length === 1) {
                  for (const v of evaluated) {
                    valSet.add(v);
                    valSet.add(String(v));
                    if (typeof v === "number") valSet.add(Number(v));
                  }
                } else {
                  valSet.add(evaluated);
                  valSet.add(String(evaluated));
                  if (typeof evaluated === "number") valSet.add(Number(evaluated));
                }
              }
              (expr as any)._cachedSet = valSet;
            }
          }

          if (valSet) {
            res = valSet.has(left) || valSet.has(String(left)) || (typeof left === "number" && valSet.has(Number(left)));
          } else {
            let vals: any[] = [];
            for (const e of expr.right) {
              const evaluated = await this.evaluateExpr(storage, e, row, params);
              if (Array.isArray(evaluated) && expr.right.length === 1) {
                vals = vals.concat(evaluated);
              } else {
                vals.push(evaluated);
              }
            }
            res = vals.some((v) => v == left);
          }
        } else {
          let valSet: Set<any> | undefined = (expr as any)._cachedSubquerySet;
          if (!valSet) {
            const isCorrelated = this.hasOuterReferences(expr.right, row);
            if (!isCorrelated) {
              valSet = new Set();
              for await (const r of this.executeSelect(storage, expr.right, params, {})) {
                const firstVal = Object.values(r)[0];
                valSet.add(firstVal);
                valSet.add(String(firstVal));
                if (typeof firstVal === "number") valSet.add(Number(firstVal));
              }
              (expr as any)._cachedSubquerySet = valSet;
            }
          }

          if (valSet) {
            res = valSet.has(left) || valSet.has(String(left)) || (typeof left === "number" && valSet.has(Number(left)));
          } else {
            const results = [];
            for await (const r of this.executeSelect(
              storage,
              expr.right,
              params,
              row,
            )) {
              results.push(r);
            }
            const vals = results.map((r: any) => Object.values(r)[0]);
            res = vals.some((v) => v == left);
          }
        }
        return (expr as any).not ? !res : res;
      }
      case "Subquery": {
        const results = [];
        for await (const r of this.executeSelect(
          storage,
          expr.stmt,
          params,
          row,
        ))
          results.push(r);
        if (results.length > 0) return Object.values(results[0])[0];
        return null;
      }
      case "Exists": {
        for await (const _ of this.executeSelect(
          storage,
          expr.stmt,
          params,
          row,
        )) {
          return true;
        }
        return false;
      }
      case "Interval":
        return expr.value;
      case "Extract": {
        const val = await this.evaluateExpr(storage, expr.source, row, params);
        if (val == null) return null;
        const d = new Date(val);
        if (isNaN(d.getTime())) return null;
        return this.getDatePart(d, expr.field);
      }
      case "Alias":
        return await this.evaluateExpr(storage, expr.expr, row, params);
      case "Not":
        return !(await this.evaluateExpr(storage, expr.expr, row, params));
      case "IsNull": {
        const val = await this.evaluateExpr(storage, expr.expr, row, params);
        const isNull = val === null || val === undefined;
        return expr.not ? !isNull : isNull;
      }
      case "Case": {
        for (const c of expr.cases) {
          if (await this.evaluateExpr(storage, c.when, row, params)) {
            return await this.evaluateExpr(storage, c.then, row, params);
          }
        }
        if (expr.elseExpr)
          return await this.evaluateExpr(storage, expr.elseExpr, row, params);
        return null;
      }
      case "Array": {
        const elements = expr.elements;
        const res = new Array(elements.length);
        for (let i = 0; i < elements.length; i++) {
          res[i] = await this.evaluateExpr(storage, elements[i], row, params);
        }
        return res;
      }
      case "Cast": {
        let val = await this.evaluateExpr(storage, expr.expr, row, params);
        return await this.castValue(storage, val, expr.dataType);
      }
      case "Call": {
        const key = this.getExprKey(expr);
        if (row[key] !== undefined) return row[key];

        if ((expr as any)._fnNameUpper === undefined) {
          let rawFnName = expr.fnName;
          if (rawFnName.includes(".")) rawFnName = rawFnName.split(".").pop()!;
          (expr as any)._fnNameUpper = rawFnName.toUpperCase();
        }
        const fnName = (expr as any)._fnNameUpper;
        if (fnName === "COUNT") return row.__COUNT__ || 0;
        if (fnName === "AVG") return row.__AVG__ || 0;
        if (fnName === "SUM" || fnName === "MIN" || fnName === "MAX")
          return null;
        if (
          fnName === "ARRAY_AGG" ||
          fnName === "JSON_AGG" ||
          fnName === "JSONB_AGG"
        )
          return [];
        if (fnName === "JSON_OBJECT_AGG" || fnName === "JSONB_OBJECT_AGG")
          return {};

        const args = [];
        for (const argExpr of expr.args) {
          args.push(await this.evaluateExpr(storage, argExpr, row, params));
        }

        if (fnName === "VERSION") return "PostgreSQL 16.2 (LitePostgres)";
        if (
          fnName === "NOW" ||
          fnName === "CURRENT_TIMESTAMP" ||
          fnName === "LOCALTIMESTAMP"
        )
          return new Date().toISOString();
        if (fnName === "CURRENT_DATE")
          return new Date().toISOString().split("T")[0];
        if (fnName === "CURRENT_TIME" || fnName === "LOCALTIME")
          return new Date().toISOString().split("T")[1];
        if (fnName === "UPPER")
          return args[0] != null ? String(args[0]).toUpperCase() : null;
        if (fnName === "LOWER")
          return args[0] != null ? String(args[0]).toLowerCase() : null;
        if (fnName === "LENGTH")
          return args[0] != null ? String(args[0]).length : null;
        if (fnName === "TRIM" || fnName === "BTRIM") {
          if (args[0] == null) return null;
          const s = String(args[0]);
          if (args[1] != null) {
            const chars = String(args[1]);
            const escaped = chars.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
            return s.replace(new RegExp(`^[${escaped}]+|[${escaped}]+$`, "g"), "");
          }
          return s.trim();
        }
        if (fnName === "LTRIM") {
          if (args[0] == null) return null;
          const s = String(args[0]);
          if (args[1] != null) {
            const chars = String(args[1]);
            const escaped = chars.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
            return s.replace(new RegExp(`^[${escaped}]+`, "g"), "");
          }
          return s.trimStart();
        }
        if (fnName === "RTRIM") {
          if (args[0] == null) return null;
          const s = String(args[0]);
          if (args[1] != null) {
            const chars = String(args[1]);
            const escaped = chars.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
            return s.replace(new RegExp(`[${escaped}]+$`, "g"), "");
          }
          return s.trimEnd();
        }
        if (fnName === "REPLACE") {
          if (args[0] == null || args[1] == null || args[2] == null)
            return args[0];
          return String(args[0]).split(String(args[1])).join(String(args[2]));
        }
        if (fnName === "SUBSTRING") {
          if (args[0] == null) return null;
          const str = String(args[0]);
          const start = (args[1] != null ? Number(args[1]) : 1) - 1;
          if (args[2] != null) {
            const count = Number(args[2]);
            return str.substring(start, start + count);
          }
          return str.substring(start);
        }
        if (fnName === "CONCAT") {
          return args
            .filter((v) => v != null)
            .map((v) => String(v))
            .join("");
        }
        if (fnName === "CONCAT_WS") {
          if (args[0] == null) return null;
          const sep = String(args[0]);
          return args
            .slice(1)
            .filter((v) => v != null)
            .map((v) => String(v))
            .join(sep);
        }
        if (fnName === "LTRIM")
          return args[0] != null ? String(args[0]).trimStart() : null;
        if (fnName === "RTRIM")
          return args[0] != null ? String(args[0]).trimEnd() : null;
        if (fnName === "LEFT") {
          if (args[0] == null || args[1] == null) return null;
          const str = String(args[0]);
          const n = Number(args[1]);
          return n >= 0
            ? str.substring(0, n)
            : str.substring(0, Math.max(0, str.length + n));
        }
        if (fnName === "RIGHT") {
          if (args[0] == null || args[1] == null) return null;
          const str = String(args[0]);
          const n = Number(args[1]);
          return n >= 0
            ? str.substring(Math.max(0, str.length - n))
            : str.substring(Math.max(0, -n));
        }
        if (fnName === "LPAD") {
          if (args[0] == null || args[1] == null) return null;
          const str = String(args[0]);
          const n = Number(args[1]);
          const fill = String(args[2] ?? " ");
          return str.length > n ? str.substring(0, n) : str.padStart(n, fill);
        }
        if (fnName === "RPAD") {
          if (args[0] == null || args[1] == null) return null;
          const str = String(args[0]);
          const n = Number(args[1]);
          const fill = String(args[2] ?? " ");
          return str.length > n ? str.substring(0, n) : str.padEnd(n, fill);
        }
        if (fnName === "INITCAP") {
          if (args[0] == null) return null;
          const str = String(args[0]).toLowerCase();
          return str.replace(/(^|\s)\S/g, (l) => l.toUpperCase());
        }
        if (fnName === "REVERSE") {
          return args[0] != null
            ? String(args[0]).split("").reverse().join("")
            : null;
        }
        if (fnName === "STRPOS") {
          if (args[0] == null || args[1] == null) return null;
          return String(args[0]).indexOf(String(args[1])) + 1;
        }
        if (fnName === "REPEAT") {
          if (args[0] == null || args[1] == null) return null;
          const n = Number(args[1]);
          return n > 0 ? String(args[0]).repeat(n) : "";
        }
        if (fnName === "SPLIT_PART") {
          if (args[0] == null || args[1] == null || args[2] == null)
            return null;
          const parts = String(args[0]).split(String(args[1]));
          const idx = Number(args[2]);
          return idx > 0 && idx <= parts.length ? parts[idx - 1] : "";
        }
        if (fnName === "COALESCE") {
          for (const val of args) {
            if (val !== null && val !== undefined) return val;
          }
          return null;
        }
        if (fnName === "ABS")
          return args[0] != null ? Math.abs(Number(args[0])) : null;
        if (fnName === "CEIL" || fnName === "CEILING")
          return args[0] != null ? Math.ceil(Number(args[0])) : null;
        if (fnName === "FLOOR")
          return args[0] != null ? Math.floor(Number(args[0])) : null;
        if (fnName === "ROUND") {
          if (args[0] == null) return null;
          const num = Number(args[0]);
          const precision = args[1] != null ? Math.floor(Number(args[1])) : 0;
          const factor = Math.pow(10, precision);
          return Math.round(num * factor) / factor;
        }
        if (fnName === "TRUNC") {
          if (args[0] == null) return null;
          const num = Number(args[0]);
          const precision = args[1] != null ? Math.floor(Number(args[1])) : 0;
          const factor = Math.pow(10, precision);
          return Math.trunc(num * factor) / factor;
        }
        if (fnName === "POWER" || fnName === "POW") {
          if (args[0] == null || args[1] == null) return null;
          return Math.pow(Number(args[0]), Number(args[1]));
        }
        if (fnName === "SQRT")
          return args[0] != null ? Math.sqrt(Number(args[0])) : null;
        if (fnName === "EXP")
          return args[0] != null ? Math.exp(Number(args[0])) : null;
        if (fnName === "LN")
          return args[0] != null ? Math.log(Number(args[0])) : null;
        if (fnName === "LOG")
          return args[0] != null ? Math.log10(Number(args[0])) : null;
        if (fnName === "MOD") {
          if (args[0] == null || args[1] == null) return null;
          return Number(args[0]) % Number(args[1]);
        }
        if (fnName === "SIGN") {
          if (args[0] == null) return null;
          const n = Number(args[0]);
          return n > 0 ? 1 : n < 0 ? -1 : 0;
        }
        if (fnName === "PI") return Math.PI;
        if (fnName === "RANDOM") return Math.random();
        if (fnName === "GEN_RANDOM_UUID" || fnName === "UUID_GENERATE_V4") {
          return "xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx".replace(
            /[xy]/g,
            function (c) {
              const r = (Math.random() * 16) | 0,
                v = c === "x" ? r : (r & 0x3) | 0x8;
              return v.toString(16);
            },
          );
        }
        if (fnName === "DEGREES")
          return args[0] != null ? Number(args[0]) * (180 / Math.PI) : null;
        if (fnName === "RADIANS")
          return args[0] != null ? Number(args[0]) * (Math.PI / 180) : null;

        if (fnName === "JSON_EXTRACT" || fnName === "JSONB_EXTRACT") {
          let json = args[0];
          if (typeof json === "string") {
            try {
              json = JSON.parse(json);
            } catch {
              return null;
            }
          }
          let current = json;
          for (let i = 1; i < args.length; i++) {
            current = current?.[args[i]];
          }
          return current;
        }

        if (fnName === "JSON_BUILD_OBJECT" || fnName === "JSONB_BUILD_OBJECT") {
          const obj: any = {};
          for (let i = 0; i < args.length; i += 2) {
            if (args[i] !== undefined && args[i] !== null) {
              obj[String(args[i])] = args[i + 1];
            }
          }
          return obj;
        }

        if (fnName === "JSON_BUILD_ARRAY" || fnName === "JSONB_BUILD_ARRAY") {
          return args;
        }

        if (fnName === "JSONB_SET") {
          let target = args[0];
          if (typeof target === "string") {
            try {
              target = JSON.parse(target);
            } catch (e) {}
          }
          let path = args[1];
          const newValue = args[2];
          const createMissing = args[3] !== false;
          if (typeof path === "string") {
            const trimmed = path.trim();
            if (trimmed === "{}") path = [];
            else if (trimmed.startsWith("{") && trimmed.endsWith("}")) {
              path = trimmed
                .slice(1, -1)
                .split(",")
                .map((s) => s.trim().replace(/^"|"$/g, ""));
            } else path = [path];
          }
          if (!Array.isArray(path)) return target;
          return this.deepSet(target, path, newValue, createMissing);
        }

        if (fnName === "JSONB_INSERT") {
          let target = args[0];
          if (typeof target === "string") {
            try {
              target = JSON.parse(target);
            } catch (e) {}
          }
          let path = args[1];
          const newValue = args[2];
          const insertAfter = args[3] === true;
          if (typeof path === "string") {
            const trimmed = path.trim();
            if (trimmed === "{}") path = [];
            else if (trimmed.startsWith("{") && trimmed.endsWith("}")) {
              path = trimmed
                .slice(1, -1)
                .split(",")
                .map((s) => s.trim().replace(/^"|"$/g, ""));
            } else path = [path];
          }
          if (!Array.isArray(path)) return target;
          return this.deepInsert(target, path, newValue, insertAfter);
        }

        if (fnName === "JSON_TYPEOF" || fnName === "JSONB_TYPEOF") {
          return this.jsonTypeof(args[0]);
        }

        if (fnName === "JSON_STRIP_NULLS" || fnName === "JSONB_STRIP_NULLS") {
          return this.stripNulls(args[0]);
        }

        if (fnName === "JSONB_PRETTY") {
          return JSON.stringify(args[0], null, 2);
        }
        if (fnName === "DATE_TRUNC") {
          const unit = String(args[0]).toLowerCase();
          const val = args[1];
          if (val == null) return null;
          const d = new Date(val);
          if (isNaN(d.getTime())) return null;
          switch (unit) {
            case "year":
              d.setMonth(0, 1);
              d.setHours(0, 0, 0, 0);
              break;
            case "month":
              d.setDate(1);
              d.setHours(0, 0, 0, 0);
              break;
            case "day":
              d.setHours(0, 0, 0, 0);
              break;
            case "hour":
              d.setMinutes(0, 0, 0);
              break;
            case "minute":
              d.setSeconds(0, 0);
              break;
            case "second":
              d.setMilliseconds(0);
              break;
          }
          return d.toISOString();
        }
        if (fnName === "AGE") {
          if (args.length === 0) return null;
          const t1 = new Date(args[0]);
          const t2 = args.length > 1 ? new Date(args[1]) : new Date();
          if (isNaN(t1.getTime()) || isNaN(t2.getTime())) return null;
          if (args.length === 1) return this.calculateAge(new Date(), t1);
          return this.calculateAge(t1, t2);
        }
        if (fnName === "TO_CHAR") {
          const val = args[0];
          const format = args[1];
          if (val == null || format == null) return null;
          const d = new Date(val);
          if (isNaN(d.getTime())) return String(val);
          let result = String(format);
          const months = [
            "January",
            "February",
            "March",
            "April",
            "May",
            "June",
            "July",
            "August",
            "September",
            "October",
            "November",
            "December",
          ];
          const days = [
            "Sunday",
            "Monday",
            "Tuesday",
            "Wednesday",
            "Thursday",
            "Friday",
            "Saturday",
          ];
          const pad = (n: number, l: number = 2) => String(n).padStart(l, "0");
          const replacements: Record<string, () => string> = {
            YYYY: () => String(d.getFullYear()),
            YY: () => String(d.getFullYear()).slice(-2),
            MM: () => pad(d.getMonth() + 1),
            DD: () => pad(d.getDate()),
            HH24: () => pad(d.getHours()),
            HH: () => pad(d.getHours() % 12 || 12),
            MI: () => pad(d.getMinutes()),
            SS: () => pad(d.getSeconds()),
            MS: () => pad(d.getMilliseconds(), 3),
            Month: () => months[d.getMonth()]!,
            Mon: () => months[d.getMonth()]!.slice(0, 3),
            Day: () => days[d.getDay()]!,
            Dy: () => days[d.getDay()]!.slice(0, 3),
          };
          const sortedPatterns = Object.keys(replacements).sort(
            (a, b) => b.length - a.length,
          );
          for (const pattern of sortedPatterns) {
            result = result.replace(
              new RegExp(pattern, "g"),
              replacements[pattern]!(),
            );
          }
          return result;
        }
        if (fnName === "DATE_PART") {
          const field = args[0];
          const source = args[1];
          if (field == null || source == null) return null;
          const d = new Date(source);
          if (isNaN(d.getTime())) return null;
          return this.getDatePart(d, String(field));
        }
        if (fnName === "OBJ_DESCRIPTION") {
          if (args[0] == null) return null;
          const oid = Number(args[0]);
          if (isNaN(oid)) return null;
          return await storage.getDescription(oid, 0);
        }
        if (fnName === "COL_DESCRIPTION") {
          if (args[0] == null || args[1] == null) return null;
          const oid = Number(args[0]);
          const subid = Number(args[1]);
          if (isNaN(oid) || isNaN(subid)) return null;
          return await storage.getDescription(oid, subid);
        }
        if (fnName === "QUOTE_IDENT") {
          if (args[0] == null) return null;
          const str = String(args[0]);
          return `"${str.replace(/"/g, '""')}"`;
        }
        if (fnName === "FORMAT_TYPE") {
          // In LitePostgres, atttypid stores the type string directly in pg_attribute
          return args[0] != null ? String(args[0]) : null;
        }
        if (fnName === "PG_GET_EXPR") {
          // In LitePostgres, adbin stores the JSON string of the expression object
          const adbin = args[0];
          if (adbin == null) return null;
          try {
            const parsed = JSON.parse(String(adbin));
            if (
              parsed &&
              typeof parsed === "object" &&
              parsed.type === "Literal"
            ) {
              return String(parsed.value);
            }
            return String(adbin);
          } catch {
            return String(adbin);
          }
        }
        return null;
      }
    }
  }
}
