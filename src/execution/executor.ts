import { Statement, Expr, JoinClause, OrderBy } from "../ast";
import { endBenchmarks, startBenchmarks } from "../benchmarks";
import { StorageEngine } from "../storage/engine";

export class Executor {
  private exprKeyMap = new WeakMap<Expr, string>();
  private keyCount = 0;

  /**
   * Generates or retrieves a unique, stable key for an AST expression object.
   * This avoids expensive JSON.stringify calls in hot loops (1M+ rows).
   */
  private getExprKey(expr: Expr): string {
    if (!expr || typeof expr !== 'object') return String(expr);
    let key = this.exprKeyMap.get(expr);
    if (!key) {
      key = `__e${++this.keyCount}`;
      this.exprKeyMap.set(expr, key);
    }
    return key;
  }

  constructor() {}

  public async execute(storage: StorageEngine, stmt: Statement, params: any[] = []): Promise<any> {
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

      case "CreateSchema": {
        await storage.createSchema(stmt.schemaName, stmt.ifNotExists);
        return { success: true, message: `Schema ${stmt.schemaName} created.` };
      }

      case "CreateIndex": {
        await storage.createIndex(stmt.indexName, stmt.tableName, stmt.columns, !!stmt.unique, !!stmt.ifNotExists);
        return { success: true, message: `Index ${stmt.indexName} created.` };
      }
      
      case "DropSchema": {
        await storage.dropSchema(stmt.schemaName, stmt.ifExists, stmt.cascade);
        return { success: true, message: `Schema ${stmt.schemaName} dropped.` };
      }

      case "DropTable": {
        await storage.dropTable(stmt.tableName, stmt.ifExists);
        return { success: true, message: `Table ${stmt.tableName} dropped.` };
      }

      case "AlterTable": {
        const table = await (storage as any).getTableAsync(stmt.tableName);
        if (!table) throw new Error(`Table ${stmt.tableName} not found`);
        const action = stmt.action;

        if (action.type === 'AddColumn') {
          if (action.ifNotExists && table.columns.some((c: any) => c.name === action.column.name)) {
            return { success: true };
          }
          if (action.column.references) {
            storage.invalidateTableCache(action.column.references.table);
          }
          table.columns.push(action.column);
          await storage.updateTableSchema(stmt.tableName, table);

          await storage.insertRow('pg_catalog.pg_attribute', {
            attrelid: table.firstPage,
            attname: action.column.name,
            atttypid: action.column.dataType,
            attnum: table.columns.length,
            attnotnull: !!action.column.isNotNull,
            attprimary: !!action.column.isPrimaryKey,
            attunique: !!action.column.isUnique,
            attref_table: action.column.references?.table || null,
            attref_col: action.column.references?.column || null,
            attref_on_delete: action.column.references?.onDelete || null,
            attref_on_update: action.column.references?.onUpdate || null,
            attdef: action.column.defaultVal ? JSON.stringify(action.column.defaultVal) : (action.column.generatedExpr ? JSON.stringify({ __generated__: true, expr: action.column.generatedExpr }) : null),
            atttypmod: -1,
            attisdropped: false,
          });

          if (action.column.defaultVal || action.column.generatedExpr) {
            await storage.insertRow('pg_catalog.pg_attrdef', {
              adrelid: table.firstPage,
              adnum: table.columns.length,
              adbin: action.column.generatedExpr ? JSON.stringify({ __generated__: true, expr: action.column.generatedExpr }) : JSON.stringify(action.column.defaultVal),
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
                  params
                );
              },
            );
          }
        } else if (action.type === 'DropColumn') {
          const colIndex = table.columns.findIndex((c: any) => c.name === action.columnName);
          if (colIndex === -1) {
            if (action.ifExists) return { success: true };
            throw new Error(`Column ${action.columnName} does not exist`);
          }
          const colNum = colIndex + 1;
          table.columns.splice(colIndex, 1);
          await storage.updateTableSchema(stmt.tableName, table);
          
          await storage.deleteRows('pg_catalog.pg_attribute', async (r: any) => r.attrelid === table.firstPage && r.attname === action.columnName);
          await storage.deleteRows('pg_catalog.pg_attrdef', async (r: any) => r.adrelid === table.firstPage && r.adnum === colNum);

          await storage.updateRows(
            stmt.tableName,
            async () => true,
            async (row: any) => {
              delete row[action.columnName];
            },
          );
        } else if (action.type === 'RenameColumn') {
          const col = table.columns.find((c: any) => c.name === action.oldColumnName);
          if (!col) throw new Error(`Column ${action.oldColumnName} does not exist`);
          col.name = action.newColumnName;
          await storage.updateTableSchema(stmt.tableName, table);
          
          await storage.updateRows('pg_catalog.pg_attribute', 
            async (r: any) => r.attrelid === table.firstPage && r.attname === action.oldColumnName,
            async (r: any) => { r.attname = action.newColumnName; }
          );

          await storage.updateRows(
            stmt.tableName,
            async () => true,
            async (row: any) => {
              if (row[action.oldColumnName] !== undefined) {
                row[action.newColumnName] = row[action.oldColumnName];
                delete row[action.oldColumnName];
              }
            },
          );
        } else if (action.type === 'RenameTable') {
          await storage.renameTable(stmt.tableName, action.newTableName);
        } else if (action.type === 'AlterColumnType') {
          const col = table.columns.find((c: any) => c.name === action.columnName);
          if (!col) throw new Error(`Column ${action.columnName} does not exist`);
          col.dataType = action.dataType;
          await storage.updateTableSchema(stmt.tableName, table);
          await storage.updateRows('pg_catalog.pg_attribute', 
            async (r: any) => r.attrelid === table.firstPage && r.attname === action.columnName,
            async (r: any) => { r.atttypid = action.dataType; }
          );
        } else if (action.type === 'AlterColumnSetDefault') {
          const colIndex = table.columns.findIndex((c: any) => c.name === action.columnName);
          const col = table.columns[colIndex];
          if (!col) throw new Error(`Column ${action.columnName} does not exist`);
          col.defaultVal = action.defaultVal;
          await storage.updateTableSchema(stmt.tableName, table);
          
          const strDef = JSON.stringify(action.defaultVal);
          await storage.updateRows('pg_catalog.pg_attribute', 
            async (r: any) => r.attrelid === table.firstPage && r.attname === action.columnName,
            async (r: any) => { r.attdef = strDef; }
          );
          
          let updatedAd = false;
          await storage.updateRows('pg_catalog.pg_attrdef',
            async (r: any) => r.adrelid === table.firstPage && r.adnum === colIndex + 1,
            async (r: any) => { r.adbin = strDef; updatedAd = true; }
          );
          if (!updatedAd) {
            await storage.insertRow('pg_catalog.pg_attrdef', {
              adrelid: table.firstPage,
              adnum: colIndex + 1,
              adbin: strDef
            });
          }
        } else if (action.type === 'AlterColumnDropDefault') {
          const colIndex = table.columns.findIndex((c: any) => c.name === action.columnName);
          const col = table.columns[colIndex];
          if (!col) throw new Error(`Column ${action.columnName} does not exist`);
          delete col.defaultVal;
          await storage.updateTableSchema(stmt.tableName, table);

          await storage.updateRows('pg_catalog.pg_attribute', 
            async (r: any) => r.attrelid === table.firstPage && r.attname === action.columnName,
            async (r: any) => { r.attdef = null; }
          );
          await storage.deleteRows('pg_catalog.pg_attrdef', async (r: any) => r.adrelid === table.firstPage && r.adnum === colIndex + 1);
        } else if (action.type === 'AlterColumnSetNotNull') {
          const col = table.columns.find((c: any) => c.name === action.columnName);
          if (!col) throw new Error(`Column ${action.columnName} does not exist`);
          col.isNotNull = true;
          await storage.updateTableSchema(stmt.tableName, table);
          await storage.updateRows('pg_catalog.pg_attribute', 
            async (r: any) => r.attrelid === table.firstPage && r.attname === action.columnName,
            async (r: any) => { r.attnotnull = true; }
          );
        } else if (action.type === 'AlterColumnDropNotNull') {
          const col = table.columns.find((c: any) => c.name === action.columnName);
          if (!col) throw new Error(`Column ${action.columnName} does not exist`);
          col.isNotNull = false;
          await storage.updateTableSchema(stmt.tableName, table);
          await storage.updateRows('pg_catalog.pg_attribute', 
            async (r: any) => r.attrelid === table.firstPage && r.attname === action.columnName,
            async (r: any) => { r.attnotnull = false; }
          );
        } else if (action.type === 'AddForeignKey') {
          const col = table.columns.find((c: any) => c.name === action.columnName);
          if (!col) throw new Error(`Column ${action.columnName} does not exist`);
          col.references = action.references;
          storage.invalidateTableCache(action.references.table);
          await storage.updateTableSchema(stmt.tableName, table);
          
          await storage.updateRows(
            'pg_catalog.pg_attribute',
            async (r: any) => r.attrelid === table.firstPage && r.attname === action.columnName,
            async (r: any) => {
              r.attref_table = action.references.table;
              r.attref_col = action.references.column;
              r.attref_on_delete = action.references.onDelete || null;
              r.attref_on_update = action.references.onUpdate || null;
            }
          );
        }

        return { success: true };
      }

      case "CreateTable":
        await storage.createTable(stmt.tableName, stmt.columns, stmt.ifNotExists);
        return { success: true, message: `Table ${stmt.tableName} created.` };

      case "Insert": {
        const table = await (storage as any).getTableAsync(stmt.tableName);
        if (!table) throw new Error(`Table ${stmt.tableName} not found`);

        const insertedRecords = [];
        const returningRecords = [];
        let schemaUpdated = false;

        let insertRows: any[] = [];
        
        if (stmt.select) {
          const selectStream = this.executeSelect(storage, stmt.select, params);
          for await (const row of selectStream) {
            insertRows.push(row);
          }
        } else if (stmt.values) {
          const valuesList: Expr[][] = (stmt.values.length > 0 && Array.isArray(stmt.values[0])) 
            ? (stmt.values as Expr[][]) 
            : [stmt.values as Expr[]];
            
          const colsToUse = stmt.columns && stmt.columns.length > 0 
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
                  params
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
             const selectKeys = Object.keys(recordData).filter(k => !k.startsWith('__'));
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
          for (const col of table.columns) {
            const dt = col.dataType.toUpperCase();
            if (
              (dt === "SERIAL" || dt === "BIGSERIAL" || dt === "SMALLSERIAL") &&
              record[col.name] === undefined
            ) {
              table.sequence += 1;
              schemaUpdated = true;
              record[col.name] = table.sequence;
            }
            if (record[col.name] === undefined && col.defaultVal) {
              record[col.name] = await this.evaluateExpr(storage, col.defaultVal, {}, params);
            }
          }
          // 2b. Evaluate generated columns
          for (const col of table.columns) {
            if ((col as any).generatedExpr) {
              record[col.name] = await this.evaluateExpr(storage, (col as any).generatedExpr, record, params);
            }
          }
          
          // 2c. Cast values to column types
          for (const col of table.columns) {
            if (record[col.name] !== undefined && record[col.name] !== null) {
              record[col.name] = await this.castValue(storage, record[col.name], col.dataType);
            }
          }
          endBenchmarks("serial_default_assignment");

          // 3. Unique/Conflict Checking
          startBenchmarks();
          let conflictRow = null;
          for (const col of table.columns) {
            if (col.isUnique && record[col.name] !== undefined && record[col.name] !== null) {
              let existing = null;
              if (col.isPrimaryKey) {
                existing = await storage.getRowByPK(stmt.tableName, record[col.name]);
              } else {
                for await (const r of storage.scanRows(stmt.tableName)) {
                  if (r[col.name] === record[col.name]) { existing = r; break; }
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
            if (stmt.onConflict.action === 'NOTHING') {
              continue; // Skip this row
            } else {
              // DO UPDATE SET
              const pkColName = await storage.getPKColumn(stmt.tableName);
              if (!pkColName) throw new Error("ON CONFLICT UPDATE requires a Primary Key for identification");
              const pkVal = conflictRow[pkColName];
              
              const updatedRows: any[] = [];
              await storage.updateRows(
                stmt.tableName,
                async (r) => r[pkColName] === pkVal,
                async (r) => {
                  const evalContext = { ...r, excluded: record };
                  for (const [col, expr] of Object.entries(stmt.onConflict!.assignments!)) {
                    let newVal = await this.evaluateExpr(storage, expr, evalContext, params);
                    const colDef = table.columns.find((c: any) => c.name === col);
                    if (colDef && newVal !== undefined && newVal !== null) {
                      newVal = await this.castValue(storage, newVal, colDef.dataType);
                    }
                    r[col] = newVal;
                  }
                  if (stmt.returning) {
                    updatedRows.push(await this.projectRow(storage, r, stmt.returning, params));
                  }
                }
              );
              if (stmt.returning) returningRecords.push(...updatedRows);
              continue;
            }
          }
          endBenchmarks("conflict_resolution");

          // 5. Normal Insert Path (Constraint checks and actual IO)
          startBenchmarks();
          for (const col of table.columns) {
            if (col.isNotNull && (record[col.name] === null || record[col.name] === undefined))
              throw new Error(`Constraint Error: ${col.name} cannot be null`);

            if (col.references && record[col.name] !== undefined && record[col.name] !== null) {
              let exists = false;
              // Optimization: Use O(log N) index lookup if referenced column is the Primary Key
              const refPK = await storage.getPKColumn(col.references.table);
              if (refPK === col.references.column) {
                const refRow = await storage.getRowByPK(col.references.table, record[col.name]);
                if (refRow) exists = true;
              } else {
                for await (const r of storage.scanRows(col.references.table)) {
                  if (r[col.references.column] === record[col.name]) { exists = true; break; }
                }
              }
              if (!exists) throw new Error(`Foreign Key Error: insert or update on table "${stmt.tableName}" violates foreign key constraint on table "${col.references.table}"`);
            }
          }
          endBenchmarks("constraint_checks");

          await storage.insertRow(stmt.tableName, record);
          insertedRecords.push(record);

          startBenchmarks();
          if (stmt.returning) {
            returningRecords.push(await this.projectRow(storage, record, stmt.returning, params));
          }
          endBenchmarks("returning_projection");
        }

        if (schemaUpdated)
          await storage.updateTableSchema(stmt.tableName, table);
        
        if (stmt.returning) return returningRecords;
        
        if (insertedRecords.length === 0 && stmt.onConflict?.action === 'NOTHING') {
          return { success: true, conflict: 'nothing' };
        }
        
        return { success: true, inserted: insertedRecords.length === 1 ? insertedRecords[0] : insertedRecords };
      }

      case "Select": {
        const rows = [];
        for await (const row of this.executeSelect(storage, stmt, params)) {
          rows.push(row);
        }
        return rows;
      }

      case "Update": {
        const table = await (storage as any).getTableAsync(stmt.tableName);
        if (!table) throw new Error(`Table ${stmt.tableName} not found`);
        
        const referencingCols = await storage.getReferencingColumns(stmt.tableName);
        const updatedRows: any[] = [];
        const updatedCount = await storage.updateRows(
          stmt.tableName,
          async (row: any) =>
            !stmt.where || (await this.evaluateExpr(storage, stmt.where, row, params)),
          async (row: any) => {
            const oldRow = { ...row };
            for (const [colName, expr] of Object.entries(stmt.assignments)) {
              let newVal = await this.evaluateExpr(storage, expr, row, params);

              const colDef = table.columns.find((c: any) => c.name === colName);
              if (colDef && newVal !== undefined && newVal !== null) {
                newVal = await this.castValue(storage, newVal, colDef.dataType);
              }

              // 1. Validate outgoing Foreign Key constraints (Child side)
              if (colDef?.references && newVal !== undefined && newVal !== null) {
                let exists = false;
                // Optimization: Use O(log N) index lookup if referenced column is the Primary Key
                const refPK = await storage.getPKColumn(colDef.references.table);
                if (refPK === colDef.references.column) {
                  const refRow = await storage.getRowByPK(colDef.references.table, newVal);
                  if (refRow) exists = true;
                } else {
                  for await (const r of storage.scanRows(colDef.references.table)) {
                    if (r[colDef.references.column] === newVal) { exists = true; break; }
                  }
                }
                if (!exists) throw new Error(`Foreign Key Error: value ${newVal} does not exist in referenced table ${colDef.references.table}`);
              }

              // 2. Handle Referential Integrity for incoming references (Parent side)
              const oldVal = oldRow[colName];
              if (newVal !== oldVal) {
                for (const ref of referencingCols) {
                  if (ref.parentColumn === colName) {
                    const action = ref.onUpdate;
                    
                    // Check if any child records exist
                    const childrenExist = async () => {
                      // Optimization: Use O(log N) index lookup if child column is the Primary Key
                      const childPK = await storage.getPKColumn(ref.childTable);
                      if (childPK === ref.childColumn) {
                        const r = await storage.getRowByPK(ref.childTable, oldVal);
                        return !!r;
                      }
                      for await (const r of storage.scanRows(ref.childTable)) {
                        if (r[ref.childColumn] === oldVal) return true;
                      }
                      return false;
                    };

                    if (await childrenExist()) {
                      if (action === 'RESTRICT' || action === 'NO ACTION') {
                        throw new Error(`Foreign Key Violation: update on table "${stmt.tableName}" violates foreign key constraint on table "${ref.childTable}"`);
                      } else if (action === 'CASCADE') {
                        await storage.updateRows(ref.childTable, async (r) => r[ref.childColumn] === oldVal, async (r) => { r[ref.childColumn] = newVal; });
                      } else if (action === 'SET NULL') {
                        await storage.updateRows(ref.childTable, async (r) => r[ref.childColumn] === oldVal, async (r) => { r[ref.childColumn] = null; });
                      }
                    }
                  }
                }
              }

              row[colName] = newVal;
            }
            // Re-evaluate generated columns after update
            for (const col of table.columns) {
              if ((col as any).generatedExpr) {
                row[col.name] = await this.evaluateExpr(storage, (col as any).generatedExpr, row, params);
              }
            }

            if (stmt.returning) {
              updatedRows.push(await this.projectRow(storage, row, stmt.returning, params));
            }
          },
        );
        if (stmt.returning) return updatedRows;
        return { success: true, updated: updatedCount };
      }

      case "Delete": {
        const referencingCols = await storage.getReferencingColumns(stmt.tableName);
        const deletedRows: any[] = [];
        const deletedCount = await storage.deleteRows(
          stmt.tableName,
          async (row: any) => {
            const match = !stmt.where || (await this.evaluateExpr(storage, stmt.where, row, params));
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
                } else {
                  for await (const childRow of storage.scanRows(ref.childTable)) {
                    if (childRow[ref.childColumn] === parentVal) {
                      hasChildren = true;
                      break;
                    }
                  }
                }

                if (hasChildren) {
                  const action = ref.onDelete;
                  if (action === 'RESTRICT' || action === 'NO ACTION') {
                    throw new Error(`Foreign Key Violation: delete on table "${stmt.tableName}" violates foreign key constraint on table "${ref.childTable}"`);
                  } else if (action === 'CASCADE') {
                    await storage.deleteRows(ref.childTable, async (r) => r[ref.childColumn] === parentVal);
                  } else if (action === 'SET NULL') {
                    await storage.updateRows(ref.childTable, async (r) => r[ref.childColumn] === parentVal, async (r) => { r[ref.childColumn] = null; });
                  }
                }
              }

              if (stmt.returning) {
                deletedRows.push(await this.projectRow(storage, row, stmt.returning, params));
              }
            }
            return match;
          },
        );
        if (stmt.returning) return deletedRows;
        return { success: true, deleted: deletedCount };
      }

      case "Comment": {
        const isColumn = stmt.objectType === 'COLUMN';
        const parts = stmt.objectName.split('.');
        const colName = isColumn ? parts.pop() || null : null;
        const tableName = parts.join('.');
        
        // Validate existence
        const table = await (storage as any).getTableAsync(tableName);
        if (!table) throw new Error(`Table ${tableName} not found`);
        let objsubid = 0;
        if (isColumn && colName) {
          const colIdx = table.columns.findIndex((c: any) => c.name === colName);
          if (colIdx === -1) throw new Error(`Column ${colName} does not exist in table ${tableName}`);
          objsubid = colIdx + 1;
        }

        const objoid = table.firstPage;
        const objNameWithoutSchema = tableName.includes('.') ? tableName.split('.').pop()! : tableName;

        // Upsert metadata directly into internal table for supreme optimization
        await storage.deleteRows('pg_catalog.pg_description', async (r: any) => {
          return Number(r.objoid) === Number(objoid) && Number(r.objsubid) === Number(objsubid);
        });

        await storage.insertRow('pg_catalog.pg_description', {
          objoid,
          objsubid,
          description: stmt.comment,
          objname: objNameWithoutSchema,
          column_name: colName
        });

        return { success: true, message: `Comment recorded for ${stmt.objectName}` };
      }

      case "Do": {
        // Handle anonymous block execution
        // For standard "Lite" implementation, we return success and metadata
        return { 
          success: true, 
          executed_block: stmt.code, 
          language: stmt.language || 'plpgsql',
          params 
        };
      }
    }
  }

  // Volcano Model Iterator Pattern
  private async *executeSelect(storage: StorageEngine, stmt: any, params: any[] = [], outerRow: any = {}): AsyncIterableIterator<any> {
    if (stmt.ctes) {
      for (const cte of stmt.ctes) {
        const rows = [];
        for await (const r of this.executeSelect(storage, cte.stmt, params, outerRow)) rows.push(r);
        storage.createTempTable(cte.name, rows);
      }
    }

    try {
      let source: any;
      if (stmt.from) {
        if (stmt.from.stmt) {
          source = this.executeSelect(storage, stmt.from.stmt, params, outerRow);
          if (stmt.from.alias)
            source = this.mapStream(source, (r) => ({
              ...r,
              [stmt.from.alias]: r,
            }));
        } else if (stmt.from.fn) {
          const fnExpr = stmt.from.fn;
          let rows: any[] = [];
          if (fnExpr.type === 'Call' && fnExpr.fnName === 'UNNEST') {
            const arr = await this.evaluateExpr(storage, fnExpr.args[0], outerRow, params);
            if (Array.isArray(arr)) {
              rows = arr.map((item, idx) => {
                const row: any = {};
                const alias1 = stmt.from.columnAliases?.[0] || 'unnest';
                row[alias1] = item;
                if (stmt.from.withOrdinality) {
                  const alias2 = stmt.from.columnAliases?.[1] || 'ordinality';
                  row[alias2] = idx + 1;
                }
                return row;
              });
            }
          } else if (fnExpr.type === 'Call' && (fnExpr.fnName === 'JSONB_EACH' || fnExpr.fnName === 'JSON_EACH')) {
            const obj = await this.evaluateExpr(storage, fnExpr.args[0], outerRow, params);
            if (obj && typeof obj === 'object' && !Array.isArray(obj)) {
              rows = Object.entries(obj).map(([k, v], idx) => {
                const row: any = {};
                const alias1 = stmt.from.columnAliases?.[0] || 'key';
                const alias2 = stmt.from.columnAliases?.[1] || 'value';
                row[alias1] = k;
                row[alias2] = v;
                if (stmt.from.withOrdinality) {
                  const alias3 = stmt.from.columnAliases?.[2] || 'ordinality';
                  row[alias3] = idx + 1;
                }
                return row;
              });
            }
          } else if (fnExpr.type === 'Call' && (fnExpr.fnName === 'JSONB_ARRAY_ELEMENTS' || fnExpr.fnName === 'JSON_ARRAY_ELEMENTS')) {
            const arr = await this.evaluateExpr(storage, fnExpr.args[0], outerRow, params);
            if (Array.isArray(arr)) {
              rows = arr.map((item, idx) => {
                const row: any = {};
                const alias1 = stmt.from.columnAliases?.[0] || 'value';
                row[alias1] = item;
                if (stmt.from.withOrdinality) {
                  const alias2 = stmt.from.columnAliases?.[1] || 'ordinality';
                  row[alias2] = idx + 1;
                }
                return row;
              });
            }
          }
          source = (async function* () {
            for (const r of rows) {
              yield {
                ...r,
                ...(stmt.from.alias ? { [stmt.from.alias]: r } : { t: r })
              };
            }
          })();
        } else {
          await (storage as any).getTableAsync(stmt.from.tableName);
          let useIndex = false;
          // Predicate Pushdown + O(1) Index Lookup
          if (
            !stmt.joins &&
            stmt.where &&
            stmt.where.type === "Binary" &&
            stmt.where.operator === "="
          ) {
            const pkColName = await storage.getPKColumn(stmt.from.tableName);
            if (
              pkColName &&
              stmt.where.left.type === "Identifier" &&
              stmt.where.left.name === pkColName &&
              (stmt.where.right.type === "Literal" || stmt.where.right.type === "Parameter" || stmt.where.right.type === "Identifier")
            ) {
              useIndex = true;
              let val = await this.evaluateExpr(storage, stmt.where.right, outerRow, params);
              const tableInfo = await (storage as any).getTableAsync(stmt.from.tableName);
              const pkCol = tableInfo?.columns.find((c: any) => c.name === pkColName);
              if (pkCol) {
                val = await this.castValue(storage, val, pkCol.dataType);
              }
              const row = await storage.getRowByPK(
                stmt.from.tableName,
                val,
              );
              source = (async function* () {
                if (row)
                  yield {
                    ...outerRow,
                    ...row,
                    [stmt.from.tableName]: row,
                    ...(stmt.from.alias ? { [stmt.from.alias]: row } : {}),
                  };
              })();
            }
          }

          if (!useIndex) {
            source = this.mapStream(
              storage.scanRows(stmt.from.tableName),
              (r) => ({
                ...outerRow,
                ...r,
                [stmt.from.tableName]: r,
                ...(stmt.from.alias ? { [stmt.from.alias]: r } : {}),
              }),
            );
            if (!stmt.joins && stmt.where)
              source = this.filterStream(
                source,
                async (r) => await this.evaluateExpr(storage, stmt.where, r, params),
              );
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
              rightRows.push({
                ...r,
                [join.tableName!]: r,
                ...(join.alias ? { [join.alias]: r } : {}),
              });
            }
            source = this.hashJoinStream(storage, source, rightRows, join, params);
          }
        }
      }

      if (stmt.where && stmt.joins)
        source = this.filterStream(
          source,
          async (r) => await this.evaluateExpr(storage, stmt.where, r, params),
        );
      else if (stmt.where && (stmt.from?.stmt || stmt.from?.fn))
        source = this.filterStream(
          source,
          async (r) => await this.evaluateExpr(storage, stmt.where, r, params),
        );

      const isAgg = stmt.columns.some(
        (c: Expr) => {
          let target = c;
          if (c.type === "Alias") target = c.expr;
          return target.type === "Call" && !target.over && ["COUNT", "AVG", "SUM", "MIN", "MAX", "ARRAY_AGG", "JSON_AGG", "JSONB_AGG", "JSON_OBJECT_AGG", "JSONB_OBJECT_AGG"].includes(target.fnName);
        }
      );

      let sourceStream = source;

      if (stmt.groupBy || isAgg) {
         sourceStream = this.streamingAggregate(storage, sourceStream, stmt, params);
      }

      const hasWindow = stmt.columns.some((c: any) => {
        let target = c;
        if (c.type === 'Alias') target = c.expr;
        return target.type === 'Call' && target.over;
      });

      if (hasWindow) {
        const bufferedRows = [];
        for await (const row of sourceStream) bufferedRows.push(row);
        sourceStream = this.processWindowFunctions(storage, bufferedRows, stmt.columns, params);
      }

      const exclusions = new Set<string>();
      if (stmt.from) {
        if (stmt.from.tableName) exclusions.add(stmt.from.tableName);
        if (stmt.from.alias) exclusions.add(stmt.from.alias);
        else if (stmt.from.fn) exclusions.add('t');
      }
      if (stmt.joins) {
        for (const join of stmt.joins) {
          if (join.tableName) exclusions.add(join.tableName);
          if (join.alias) exclusions.add(join.alias);
        }
      }

      const _this = this;
      sourceStream = this.mapStream(sourceStream, async function(r) {
         const proj = await _this.projectRow(storage, r, stmt.columns, params, exclusions);
         return { ...r, ...proj, ___lpg_projected___: proj };
      });

      if (stmt.distinct && !stmt.distinctOn) {
         sourceStream = this.distinctStream(sourceStream);
      }

      if (stmt.unionAll) {
         const rightStream = this.executeSelect(storage, stmt.unionAll, params, outerRow);
         sourceStream = this.concatStreams(sourceStream, rightStream);
      }

      if (stmt.union) {
         const rightStream = this.executeSelect(storage, stmt.union, params, outerRow);
         sourceStream = this.distinctStream(this.concatStreams(sourceStream, rightStream));
      }

      if (stmt.intersect) {
         const rightStream = this.executeSelect(storage, stmt.intersect, params, outerRow);
         sourceStream = this.intersectStream(sourceStream, rightStream);
      }

      if (stmt.intersectAll) {
         const rightStream = this.executeSelect(storage, stmt.intersectAll, params, outerRow);
         sourceStream = this.intersectAllStream(sourceStream, rightStream);
      }

      if (stmt.except) {
         const rightStream = this.executeSelect(storage, stmt.except, params, outerRow);
         sourceStream = this.exceptStream(sourceStream, rightStream);
      }

      if (stmt.exceptAll) {
         const rightStream = this.executeSelect(storage, stmt.exceptAll, params, outerRow);
         sourceStream = this.exceptAllStream(sourceStream, rightStream);
      }

      if (stmt.orderBy) {
         sourceStream = this.externalSortStream(storage, sourceStream, stmt.orderBy, params);
      }

      if (stmt.distinctOn) {
         sourceStream = this.distinctStream(sourceStream, stmt.distinctOn, storage, params);
      }

      if (stmt.offset) {
         sourceStream = this.applyOffset(sourceStream, await this.evaluateExpr(storage, stmt.offset, {}, params));
      }

      if (stmt.limit) {
         sourceStream = this.applyLimit(sourceStream, await this.evaluateExpr(storage, stmt.limit, {}, params));
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
    params: any[] = []
  ) {
    if (join.type === 'RIGHT' || join.type === 'FULL') {
      const leftRows = [];
      for await (const r of source) leftRows.push({ row: r, matched: false });

      let rightSource: AsyncIterableIterator<any>;

      if (join.stmt) {
        rightSource = this.executeSelect(storage, join.stmt, params, {});
        if (join.alias) {
          const alias = join.alias;
          rightSource = this.mapStream(rightSource, (r) => ({
            ...r,
            [alias]: r,
          }));
        }
      } else if (join.fn) {
        const fnExpr = join.fn;
        let rows: any[] = [];
        if (fnExpr.type === 'Call' && fnExpr.fnName === 'UNNEST') {
          if (!fnExpr.args[0]) throw new Error("UNNEST requires an array argument");
          const arr = await this.evaluateExpr(storage, fnExpr.args[0], {}, params);
          if (Array.isArray(arr)) {
            rows = arr.map((item, idx) => {
              const r: any = {};
              const alias1 = join.columnAliases?.[0] || 'unnest';
              r[alias1] = item;
              if (join.withOrdinality) {
                const alias2 = join.columnAliases?.[1] || 'ordinality';
                r[alias2] = idx + 1;
              }
              return r;
            });
          }
        } else if (fnExpr.type === 'Call' && (fnExpr.fnName === 'JSONB_EACH' || fnExpr.fnName === 'JSON_EACH')) {
          const obj = await this.evaluateExpr(storage, fnExpr.args[0], {}, params);
          if (obj && typeof obj === 'object' && !Array.isArray(obj)) {
            rows = Object.entries(obj).map(([k, v], idx) => {
              const r: any = {};
              const alias1 = join.columnAliases?.[0] || 'key';
              const alias2 = join.columnAliases?.[1] || 'value';
              r[alias1] = k;
              r[alias2] = v;
              if (join.withOrdinality) {
                const alias3 = join.columnAliases?.[2] || 'ordinality';
                r[alias3] = idx + 1;
              }
              return r;
            });
          }
        } else if (fnExpr.type === 'Call' && (fnExpr.fnName === 'JSONB_ARRAY_ELEMENTS' || fnExpr.fnName === 'JSON_ARRAY_ELEMENTS')) {
          const arr = await this.evaluateExpr(storage, fnExpr.args[0], {}, params);
          if (Array.isArray(arr)) {
            rows = arr.map((item, idx) => {
              const r: any = {};
              const alias1 = join.columnAliases?.[0] || 'value';
              r[alias1] = item;
              if (join.withOrdinality) {
                const alias2 = join.columnAliases?.[1] || 'ordinality';
                r[alias2] = idx + 1;
              }
              return r;
            });
          }
        }
        rightSource = (async function* () {
          for (const r of rows) {
            yield {
              ...r,
              ...(join.alias ? { [join.alias]: r } : { t: r })
            };
          }
        })();
      } else if (join.tableName) {
        rightSource = this.mapStream(
          storage.scanRows(join.tableName),
          (r) => ({
            ...r,
            [join.tableName!]: r,
            ...(join.alias ? { [join.alias]: r } : {})
          })
        );
      } else {
        rightSource = (async function*() { yield {}; })();
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

      if (join.type === 'FULL') {
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
          if (join.alias) {
            const alias = join.alias;
            rightSource = this.mapStream(rightSource, (r) => ({
              ...r,
              [alias]: r,
            }));
          }
        } else if (join.fn) {
          const fnExpr = join.fn;
          let rows: any[] = [];
        if (fnExpr.type === 'Call' && fnExpr.fnName === 'UNNEST') {
          if (!fnExpr.args[0]) throw new Error("UNNEST requires an array argument");
          const arr = await this.evaluateExpr(storage, fnExpr.args[0], row, params);
          if (Array.isArray(arr)) {
            rows = arr.map((item, idx) => {
              const r: any = {};
              const alias1 = join.columnAliases?.[0] || 'unnest';
              r[alias1] = item;
              if (join.withOrdinality) {
                const alias2 = join.columnAliases?.[1] || 'ordinality';
                r[alias2] = idx + 1;
              }
              return r;
            });
          }
        } else if (fnExpr.type === 'Call' && (fnExpr.fnName === 'JSONB_EACH' || fnExpr.fnName === 'JSON_EACH')) {
          const obj = await this.evaluateExpr(storage, fnExpr.args[0], row, params);
          if (obj && typeof obj === 'object' && !Array.isArray(obj)) {
            rows = Object.entries(obj).map(([k, v], idx) => {
              const r: any = {};
              const alias1 = join.columnAliases?.[0] || 'key';
              const alias2 = join.columnAliases?.[1] || 'value';
              r[alias1] = k;
              r[alias2] = v;
              if (join.withOrdinality) {
                const alias3 = join.columnAliases?.[2] || 'ordinality';
                r[alias3] = idx + 1;
              }
              return r;
            });
          }
        } else if (fnExpr.type === 'Call' && (fnExpr.fnName === 'JSONB_ARRAY_ELEMENTS' || fnExpr.fnName === 'JSON_ARRAY_ELEMENTS')) {
          const arr = await this.evaluateExpr(storage, fnExpr.args[0], row, params);
          if (Array.isArray(arr)) {
            rows = arr.map((item, idx) => {
              const r: any = {};
              const alias1 = join.columnAliases?.[0] || 'value';
              r[alias1] = item;
              if (join.withOrdinality) {
                const alias2 = join.columnAliases?.[1] || 'ordinality';
                r[alias2] = idx + 1;
              }
              return r;
            });
          }
        }
          rightSource = (async function* () {
            for (const r of rows) {
              yield {
                ...r,
                ...(join.alias ? { [join.alias]: r } : { t: r })
              };
            }
          })();
        } else if (join.tableName) {
          rightSource = this.mapStream(
            storage.scanRows(join.tableName),
            (r) => ({
              ...r,
              [join.tableName!]: r,
              ...(join.alias ? { [join.alias]: r } : {})
            })
          );
        } else {
          rightSource = (async function*() { yield {}; })();
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
    params: any[] = [],
  ) {
    if (join.type === 'RIGHT' || join.type === 'FULL') {
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

      if (join.type === 'FULL') {
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

  private async *distinctStream(source: AsyncIterableIterator<any>, distinctOn?: Expr[], storage?: StorageEngine, params?: any[]) {
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

  private async *concatStreams(s1: AsyncIterableIterator<any>, s2: AsyncIterableIterator<any>) {
    for await (const r of s1) yield r;
    for await (const r of s2) yield r;
  }

  private async *intersectStream(s1: AsyncIterableIterator<any>, s2: AsyncIterableIterator<any>) {
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

  private async *intersectAllStream(s1: AsyncIterableIterator<any>, s2: AsyncIterableIterator<any>) {
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

  private async *exceptStream(s1: AsyncIterableIterator<any>, s2: AsyncIterableIterator<any>) {
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

  private async *exceptAllStream(s1: AsyncIterableIterator<any>, s2: AsyncIterableIterator<any>) {
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

  private async *applyOffset(source: AsyncIterableIterator<any>, offset: number) {
    let skipped = 0;
    for await (const r of source) {
      if (skipped < offset) { skipped++; continue; }
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

  private async *externalSortStream(storage: StorageEngine, source: AsyncIterableIterator<any>, orderBy: OrderBy[], params: any[] = []): AsyncIterableIterator<any> {
    const CHUNK_SIZE = 100000;
    let chunk = [];
    let fileIndex = 0;
    const tempFiles: string[] = [];
    const vfs = storage.vfs;

    const compareFn = async (a: any, b: any) => {
      for (const ob of orderBy) {
         const vA = await this.evaluateExpr(storage, ob.expr, a, params);
         const vB = await this.evaluateExpr(storage, ob.expr, b, params);
         if (vA < vB) return ob.desc ? 1 : -1;
         if (vA > vB) return ob.desc ? -1 : 1;
      }
      return 0;
    };

    const asyncSort = async (arr: any[]) => {
       const mapVals = new Map<any, any>();
       for (const r of arr) {
          const vals = [];
          for (const ob of orderBy) vals.push(await this.evaluateExpr(storage, ob.expr, r, params));
          mapVals.set(r, vals);
       }
       arr.sort((a, b) => {
          const vA = mapVals.get(a);
          const vB = mapVals.get(b);
          for (let i = 0; i < orderBy.length; i++) {
             if (vA[i] < vB[i]) return orderBy[i]?.desc ? 1 : -1;
             if (vA[i] > vB[i]) return orderBy[i]?.desc ? -1 : 1;
          }
          return 0;
       });
    };

    for await (const row of source) {
       chunk.push(row);
       if (chunk.length >= CHUNK_SIZE) {
          await asyncSort(chunk);
          const tmpFile = vfs.join(vfs.tempDir(), `lpg_sort_${Date.now()}_${fileIndex++}.json`);
          await vfs.writeFile(tmpFile, chunk.map((r: any) => JSON.stringify(r)).join("\n"));
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
       const tmpFile = vfs.join(vfs.tempDir(), `lpg_sort_${Date.now()}_${fileIndex++}.json`);
       await vfs.writeFile(tmpFile, chunk.map((r: any) => JSON.stringify(r)).join("\n"));
       tempFiles.push(tmpFile);
    }

    if (tempFiles.length === 0) return;

    const iterators = tempFiles.map((f: string) => vfs.readLines(f)[Symbol.asyncIterator]());
    
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
                minIdx = i; minRow = currentRows[i];
             } else {
                const cmp = await compareFn(currentRows[i], minRow);
                if (cmp < 0) { minIdx = i; minRow = currentRows[i]; }
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

  private async projectRow(storage: StorageEngine, row: any, columns: Expr[], params: any[] = [], exclusions?: Set<string>): Promise<any> {
    const outRow: any = {};
    for (const col of columns) {
      if (col.type === "Identifier" && col.name === "*") {
        for (const k of Object.keys(row)) {
          if (!k.startsWith("__") && !k.startsWith("___") && (!exclusions || !exclusions.has(k)))
            outRow[k] = row[k];
        }
      } else if (col.type === "Alias") {
        const key = this.getExprKey(col.expr);
        if (row[key] !== undefined) outRow[col.alias] = row[key];
        else outRow[col.alias] = await this.evaluateExpr(storage, col.expr, row, params);
      } else {
        const key = this.getExprKey(col);
        if (row[key] !== undefined) {
           const name = (col as any).name;
           const outKey = name && name.includes(".") ? name.split(".")[1] : name || "col";
           outRow[outKey] = row[key];
        } else {
           const name = (col as any).name;
           const outKey = name && name.includes(".") ? name.split(".")[1] : name || "col";
           outRow[outKey] = await this.evaluateExpr(storage, col, row, params);
        }
      }
    }
    return outRow;
  }

  private async *processWindowFunctions(storage: StorageEngine, rows: any[], columns: any[], params: any[]): AsyncIterableIterator<any> {
    for (const col of columns) {
      let expr = col;
      if (col.type === 'Alias') expr = col.expr;
      if (expr.type !== 'Call' || !expr.over) continue;

      const windowKey = this.getExprKey(expr);
      const partitions = new Map<string, any[]>();

      for (const row of rows) {
        let pKey = 'all';
        if (expr.over.partitionBy) {
          const vals = [];
          for (const e of expr.over.partitionBy) vals.push(await this.evaluateExpr(storage, e, row, params));
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
              if (vA < vB) return ob.desc ? 1 : -1;
              if (vA > vB) return ob.desc ? -1 : 1;
            }
            return 0;
          };
          
          // Simplified async sort for windowing
          for (let i = 0; i < pRows.length; i++) {
            for (let j = i + 1; j < pRows.length; j++) {
              if (await compareRows(pRows[i], pRows[j]) > 0) {
                [pRows[i], pRows[j]] = [pRows[j], pRows[i]];
              }
            }
          }
        }

        let rank = 0;
        let lastOrderVals: any[] = [];

        for (let i = 0; i < pRows.length; i++) {
          const row = pRows[i];
          if (expr.fnName === 'ROW_NUMBER') {
            row[windowKey] = i + 1;
          } else if (expr.fnName === 'RANK') {
            if (expr.over.orderBy) {
              const currentOrderVals = [];
              for (const ob of expr.over.orderBy) currentOrderVals.push(await this.evaluateExpr(storage, ob.expr, row, params));
              
              if (i === 0 || JSON.stringify(currentOrderVals) !== JSON.stringify(lastOrderVals)) {
                rank = i + 1;
                lastOrderVals = currentOrderVals;
              }
              row[windowKey] = rank;
            } else {
              row[windowKey] = 1;
            }
          } else if (expr.fnName === 'LEAD' || expr.fnName === 'LAG') {
            const offsetExpr = expr.args[1];
            const defaultExpr = expr.args[2];

            let offset = 1;
            if (offsetExpr) {
              const offVal = await this.evaluateExpr(storage, offsetExpr, row, params);
              offset = Number(offVal);
            }

            const targetIdx = expr.fnName === 'LEAD' ? i + offset : i - offset;

            if (targetIdx >= 0 && targetIdx < pRows.length) {
              const targetRow = pRows[targetIdx];
              row[windowKey] = await this.evaluateExpr(storage, expr.args[0], targetRow, params);
            } else {
              if (defaultExpr) {
                row[windowKey] = await this.evaluateExpr(storage, defaultExpr, row, params);
              } else {
                row[windowKey] = null;
              }
            }
          }
          orderedRows.push(row);
        }
      }
      for (let i = 0; i < rows.length; i++) rows[i] = orderedRows[i];
    }

    for (const row of rows) yield row;
  }

  private async *streamingAggregate(storage: StorageEngine, source: AsyncIterableIterator<any>, stmt: any, params: any[] = []) {
    const groups = new Map<string, any>();
    
    for await (const row of source) {
      let key = "all";
      if (stmt.groupBy) {
        const keys = [];
        for (const g of stmt.groupBy) keys.push(await this.evaluateExpr(storage, g, row, params));
        key = keys.join("|");
      }
      
      if (!groups.has(key)) {
        groups.set(key, { __COUNT__: 0, __COUNTS__: {}, __SUMS__: {}, __ARRAYS__: {}, __JSON_OBJ_AGGS__: {}, __DISTINCTS__: {}, baseRow: row });
      }
      const state = groups.get(key);
      state.__COUNT__++;
      
      for (const col of stmt.columns) {
        let target = col;
        if (col.type === "Alias") target = col.expr;
        if (target.type === "Call") {
          const colName = this.getExprKey(target);
          if (target.filter) {
             if (!(await this.evaluateExpr(storage, target.filter, row, params))) continue;
          }

          if (target.fnName === "COUNT") {
            if (target.distinct && target.args && target.args[0]) {
               const val = await this.evaluateExpr(storage, target.args[0], row, params);
               if (val !== null && val !== undefined) {
                 if (!state.__DISTINCTS__[colName]) state.__DISTINCTS__[colName] = new Set();
                 const valKey = typeof val === 'object' ? JSON.stringify(val) : val;
                 state.__DISTINCTS__[colName].add(valKey);
               }
            } else {
               let isNotNull = true;
               if (target.args && target.args[0] && !(target.args[0].type === "Identifier" && target.args[0].name === "*")) {
                  const val = await this.evaluateExpr(storage, target.args[0], row, params);
                  if (val === null || val === undefined) isNotNull = false;
               }
               if (isNotNull) state.__COUNTS__[colName] = (state.__COUNTS__[colName] || 0) + 1;
            }
          } else if (target.fnName === "SUM") {
            if (state.__SUMS__[colName] === undefined) state.__SUMS__[colName] = null;
            if (target.args && target.args[0]) {
               const val = await this.evaluateExpr(storage, target.args[0], row, params);
               if (val !== null && val !== undefined) {
                 if (state.__SUMS__[colName] === null) state.__SUMS__[colName] = 0;
                 state.__SUMS__[colName] += val;
               }
            }
          } else if (target.fnName === "MIN") {
            if (target.args && target.args[0]) {
               const val = await this.evaluateExpr(storage, target.args[0], row, params);
               if (val !== null && val !== undefined) {
                 if (state.__SUMS__[colName] === undefined) state.__SUMS__[colName] = val;
                 else if (val < state.__SUMS__[colName]) state.__SUMS__[colName] = val;
               }
            }
          } else if (target.fnName === "MAX") {
            if (target.args && target.args[0]) {
               const val = await this.evaluateExpr(storage, target.args[0], row, params);
               if (val !== null && val !== undefined) {
                 if (state.__SUMS__[colName] === undefined) state.__SUMS__[colName] = val;
                 else if (val > state.__SUMS__[colName]) state.__SUMS__[colName] = val;
               }
            }
          } else if (target.fnName === "AVG") {
            if (!state.__SUMS__[colName]) state.__SUMS__[colName] = 0;
            if (target.args && target.args[0]) {
               state.__SUMS__[colName] += await this.evaluateExpr(storage, target.args[0], row, params);
               state.__COUNTS__[colName] = (state.__COUNTS__[colName] || 0) + 1;
            }
          } else if (target.fnName === "ARRAY_AGG" || target.fnName === "JSON_AGG" || target.fnName === "JSONB_AGG") {
            if (!state.__ARRAYS__[colName]) state.__ARRAYS__[colName] = [];
            if (target.args && target.args[0]) {
               const val = await this.evaluateExpr(storage, target.args[0], row, params);
               if (target.argsOrderBy) {
                 const orderVals = [];
                 for (const ob of target.argsOrderBy) orderVals.push(await this.evaluateExpr(storage, ob.expr, row, params));
                 state.__ARRAYS__[colName].push({ val, orderVals });
               } else {
                 state.__ARRAYS__[colName].push(val);
               }
            }
          } else if (target.fnName === "JSON_OBJECT_AGG" || target.fnName === "JSONB_OBJECT_AGG") {
            if (!state.__JSON_OBJ_AGGS__[colName]) state.__JSON_OBJ_AGGS__[colName] = {};
            if (target.args && target.args.length >= 2) {
               const k = await this.evaluateExpr(storage, target.args[0], row, params);
               const v = await this.evaluateExpr(storage, target.args[1], row, params);
               if (k !== null) {
                  state.__JSON_OBJ_AGGS__[colName][String(k)] = v;
               }
            }
          }
        }
      }
    }
    
    if (groups.size === 0 && !stmt.groupBy) {
       groups.set("all", { __COUNT__: 0, __COUNTS__: {}, __SUMS__: {}, __ARRAYS__: {}, __JSON_OBJ_AGGS__: {}, __DISTINCTS__: {}, baseRow: {} });
    }

    for (const state of groups.values()) {
       const outRow: any = { ...state.baseRow, __COUNT__: state.__COUNT__ };
       for (const col of stmt.columns) {
          let target = col;
          let alias = null;
          if (col.type === "Alias") { alias = col.alias; target = col.expr; }
          
          if (target.type === "Call") {
             const colName = this.getExprKey(target);
             const fn = target.fnName;
             if (fn === "COUNT") {
                const count = target.distinct ? (state.__DISTINCTS__[colName]?.size || 0) : (state.__COUNTS__[colName] || 0);
                outRow[alias || "count"] = count;
                outRow[colName] = count;
             } else if (fn === "SUM" || fn === "MIN" || fn === "MAX") {
                const val = state.__SUMS__[colName] === undefined ? null : state.__SUMS__[colName];
                outRow[alias || fn.toLowerCase()] = val;
                outRow[colName] = val;
             } else if (fn === "AVG") {
                const sum = state.__SUMS__[colName] || 0;
                const count = state.__COUNTS__[colName] || 0;
                const avg = count ? sum / count : 0;
                outRow[alias || "avg"] = avg;
                outRow["__AVG__"] = avg;
                outRow[colName] = avg;
             } else if (fn === "ARRAY_AGG" || fn === "JSON_AGG" || fn === "JSONB_AGG") {
                let arr = state.__ARRAYS__[colName] || [];
                const obList = target.argsOrderBy;
                if (obList && arr.length > 0) {
                  arr.sort((a: any, b: any) => {
                    for (let i = 0; i < obList.length; i++) {
                      const ob = obList[i];
                      if (a.orderVals[i] < b.orderVals[i]) return ob.desc ? 1 : -1;
                      if (a.orderVals[i] > b.orderVals[i]) return ob.desc ? -1 : 1;
                    }
                    return 0;
                  });
                  arr = arr.map((x: any) => x.val);
                }
                const outKey = alias || (fn === "ARRAY_AGG" ? "array_agg" : (fn === "JSON_AGG" ? "json_agg" : "jsonb_agg"));
                outRow[outKey] = arr;
                outRow[colName] = arr;
             } else if (fn === "JSON_OBJECT_AGG" || fn === "JSONB_OBJECT_AGG") {
                const obj = state.__JSON_OBJ_AGGS__[colName] || {};
                const outKey = alias || (fn === "JSON_OBJECT_AGG" ? "json_object_agg" : "jsonb_object_agg");
                outRow[outKey] = obj;
                outRow[colName] = obj;
             }
          } else {
             const name = (target as any).name;
             const outKey = name && name.includes(".") ? name.split(".")[1] : name || "col";
             outRow[alias || outKey] = await this.evaluateExpr(storage, target, state.baseRow, params);
          }
       }
       if (stmt.having) {
          if (await this.evaluateExpr(storage, stmt.having, outRow, params)) yield outRow;
       } else {
          yield outRow;
       }
    }
  }

  private async castValue(storage: StorageEngine, val: any, dataType: string): Promise<any> {
    if (val === null || val === undefined) return null;
    const dt = dataType.toUpperCase().split('(')[0]?.trim();
    
    const numerics = ["INT", "INTEGER", "SMALLINT", "BIGINT", "DECIMAL", "NUMERIC", "REAL", "DOUBLE", "PRECISION", "NUMBER", "SERIAL", "BIGSERIAL", "SMALLSERIAL", "MONEY", "OID", "REGCLASS", "REGTYPE", "REGNAMESPACE", "INT2", "INT4", "INT8", "FLOAT4", "FLOAT8"];
    if (numerics.includes(dt!)) {
      if ((dt === "REGCLASS" || dt === "REGTYPE" || dt === "REGNAMESPACE")) {
        if (typeof val === "string") {
          try {
            if (dt === "REGNAMESPACE") {
              const name = val.trim().replace(/^"|"$/g, '').replace(/""/g, '"');
              for await (const row of storage.scanRows('pg_namespace')) {
                if (row.nspname === name) return row.oid;
              }
              return null;
            } else {
              const tableName = val.split('.').map(p => p.trim().replace(/^"|"$/g, '').replace(/""/g, '"')).join('.');
              const tbl = await (storage as any).getTableAsync(tableName);
              return tbl?.firstPage || null;
            }
          } catch {
            return null;
          }
        } else if (typeof val === "number") {
          return val;
        }
      }
      const num = Number(val);
      return isNaN(num) ? null : num;
    } else if (dt === "BOOLEAN" || dt === "BOOL") {
      if (typeof val === "boolean") return val;
      const s = String(val).toUpperCase();
      if (s === "TRUE" || s === "T" || s === "1" || s === "Y" || s === "YES") return true;
      if (s === "FALSE" || s === "F" || s === "0" || s === "N" || s === "NO") return false;
      return Boolean(val);
    } else if (dt === "TEXT" || dt === "VARCHAR" || dt === "CHAR" || dt === "CHARACTER" || dt === "UUID" || dt === "STRING") {
      return typeof val === "object" ? JSON.stringify(val) : String(val);
    } else if (dt?.includes("JSON")) {
      if (typeof val === "string") {
        try { return JSON.parse(val); } catch { return null; }
      }
      return val;
    } else if (dt?.includes("DATE") || dt?.includes("TIME") || dt?.includes("TIMESTAMP")) {
      const d = new Date(val);
      return isNaN(d.getTime()) ? null : d.toISOString();
    }
    return val;
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
          Date.UTC(date.getFullYear(), date.getMonth(), date.getDate())
        );
        const dayNum = d.getUTCDay() || 7;
        d.setUTCDate(d.getUTCDate() + 4 - dayNum);
        const yearStart = new Date(Date.UTC(d.getUTCFullYear(), 0, 1));
        return Math.ceil(
          ((d.getTime() - yearStart.getTime()) / 86400000 + 1) / 7
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
        0
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

  private deepSet(obj: any, path: any[], value: any, createMissing: boolean): any {
    if (path.length === 0) return value;
    const key = path[0];
    
    if (obj === null || (typeof obj !== 'object' && !Array.isArray(obj))) {
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

    nextObj[actualKey] = this.deepSet(nextObj[actualKey], path.slice(1), value, createMissing);
    return nextObj;
  }

  private deepInsert(obj: any, path: any[], value: any, insertAfter: boolean): any {
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

    if (obj === null || typeof obj !== 'object') return obj;

    let nextObj = Array.isArray(obj) ? [...obj] : { ...obj };
    let actualKey: any = key;
    if (Array.isArray(obj)) {
        actualKey = parseInt(String(key));
        if (isNaN(actualKey)) return obj;
    }
    
    if (nextObj[actualKey] === undefined) return obj;

    nextObj[actualKey] = this.deepInsert(nextObj[actualKey], path.slice(1), value, insertAfter);
    return nextObj;
  }

  private stripNulls(obj: any): any {
    if (Array.isArray(obj)) {
        return obj.map(v => this.stripNulls(v));
    }
    if (obj !== null && typeof obj === 'object') {
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
    if (val === null) return 'null';
    if (Array.isArray(val)) return 'array';
    if (typeof val === 'object') return 'object';
    if (typeof val === 'number') return 'number';
    if (typeof val === 'string') return 'string';
    if (typeof val === 'boolean') return 'boolean';
    return 'null';
  }

  private async evaluateExpr(storage: StorageEngine, expr: Expr, row: any, params: any[] = []): Promise<any> {
    switch (expr.type) {
      case "Literal":
        return expr.value;
      case "Parameter":
        return params[expr.index - 1];
      case "Identifier": {
        const nameUpper = expr.name.toUpperCase();
        if (nameUpper === "CURRENT_TIMESTAMP") return new Date().toISOString();
        if (nameUpper === "CURRENT_DATE")
          return new Date().toISOString().split("T")[0];
        if (nameUpper === "CURRENT_TIME")
          return new Date().toISOString().split("T")[1];
        if (nameUpper === "LOCALTIMESTAMP") return new Date().toISOString();
        if (nameUpper === "LOCALTIME")
          return new Date().toISOString().split("T")[1];

        if (expr.name === "*") return "*";
        if (expr.name.includes(".")) {
          const parts = expr.name.split(".");
          if (parts.length >= 2) {
            const c = parts.pop()!;
            const t = parts.join(".");
            if (row[t] && row[t][c] !== undefined) return row[t][c];
          }
          if (row[expr.name] !== undefined) return row[expr.name];
        }
        return row[expr.name];
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
            if (typeof left === "string" && !isNaN(Date.parse(left)) && typeof right === "string") {
              const parts = right.toLowerCase().trim().split(/\s+/);
              const val = parseFloat(parts[0] || "0");
              const unit = parts[1] || "day";
              const d = new Date(left);
              const multiplier = expr.operator === "+" ? 1 : -1;
              if (unit.startsWith("year")) d.setFullYear(d.getFullYear() + multiplier * val);
              else if (unit.startsWith("month")) d.setMonth(d.getMonth() + multiplier * val);
              else if (unit.startsWith("day")) d.setDate(d.getDate() + multiplier * val);
              else if (unit.startsWith("hour")) d.setHours(d.getHours() + multiplier * val);
              else if (unit.startsWith("minute")) d.setMinutes(d.getMinutes() + multiplier * val);
              else if (unit.startsWith("second")) d.setSeconds(d.getSeconds() + multiplier * val);
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
            return String(left) + String(right);
          case "~":
            return left != null && right != null && new RegExp(String(right)).test(String(left));
          case "~*":
            return left != null && right != null && new RegExp(String(right), "i").test(String(left));
          case "!~":
            return left != null && right != null && !new RegExp(String(right)).test(String(left));
          case "->":
            return (left != null && typeof left === "object") ? left[right] : null;
          case "->>":
            return (left != null && typeof left === "object") ? (left[right] != null ? String(left[right]) : null) : null;
          case "#>": {
            if (left == null || !Array.isArray(right)) return null;
            let curr = left;
            for (const p of right) curr = curr?.[p];
            return curr;
          }
          case "@>":
            if (Array.isArray(left) && Array.isArray(right)) return right.every(v => left.includes(v));
            if (typeof left === "object" && typeof right === "object" && left !== null && right !== null) {
              return Object.keys(right).every(k => JSON.stringify(left[k]) === JSON.stringify(right[k]));
            }
            return false;
          case "?":
            if (Array.isArray(left)) return left.includes(right);
            if (typeof left === "object" && left !== null) return Object.prototype.hasOwnProperty.call(left, right);
            return false;
          case "&&":
            if (Array.isArray(left) && Array.isArray(right)) return left.some(v => right.includes(v));
            return false;
        }
        return false;
      }
      case "Logical": {
        const left = await this.evaluateExpr(storage, expr.left, row, params);
        const right = await this.evaluateExpr(storage, expr.right, row, params);
        if (expr.operator === "AND") return left && right;
        if (expr.operator === "OR") return left || right;
        return false;
      }
      case "Like": {
        const left = await this.evaluateExpr(storage, expr.left, row, params);
        const right = await this.evaluateExpr(storage, expr.right, row, params);
        if (typeof left !== "string" || typeof right !== "string") return false;
        // Build regex with proper escape handling
        const escapeChar = (expr as any).escapeStr !== undefined ? (expr as any).escapeStr : '\\';
        let pattern = "";
        for (let i = 0; i < right.length; i++) {
          const ch = right[i];
          if (ch === escapeChar && i + 1 < right.length && escapeChar !== '') {
            const next = right[i + 1];
            if (next === '_' || next === '%' || next === escapeChar) {
              // Escaped special char → literal (escape for regex)
              pattern += next!.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
              i++;
              continue;
            }
          }
          if (ch === '%') {
            pattern += ".*";
          } else if (ch === '_') {
            pattern += ".";
          } else {
            pattern += ch!.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
          }
        }
        const regex = new RegExp("^" + pattern + "$", (expr as any).ilike ? "i" : "");
        const res = regex.test(left);
        return (expr as any).not ? !res : res;
      }
      case "In": {
        const left = await this.evaluateExpr(storage, expr.left, row, params);
        let res = false;
        if (Array.isArray(expr.right)) {
          const vals = await Promise.all(
            expr.right.map((e) => this.evaluateExpr(storage, e, row, params)),
          );
          res = vals.some(v => v == left);
        } else {
          const results = [];
          for await (const r of this.executeSelect(storage, expr.right, params, row)) results.push(r);
          const vals = results.map((r: any) => Object.values(r)[0]);
          res = vals.some(v => v == left);
        }
        return (expr as any).not ? !res : res;
      }
      case "Subquery": {
        const results = [];
        for await (const r of this.executeSelect(storage, expr.stmt, params, row)) results.push(r);
        if (results.length > 0) return Object.values(results[0])[0];
        return null;
      }
      case "Exists": {
        for await (const _ of this.executeSelect(storage, expr.stmt, params, row)) {
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
        if (expr.elseExpr) return await this.evaluateExpr(storage, expr.elseExpr, row, params);
        return null;
      }
      case "Array": {
        return await Promise.all(expr.elements.map(e => this.evaluateExpr(storage, e, row, params)));
      }
      case "Cast": {
        let val = await this.evaluateExpr(storage, expr.expr, row, params);
        return await this.castValue(storage, val, expr.dataType);
      }
      case "Call": {
        const key = this.getExprKey(expr);
        if (row[key] !== undefined) return row[key];

        const fnName = expr.fnName.toUpperCase();
        if (fnName === "COUNT") return row.__COUNT__ || 0;
        if (fnName === "AVG") return row.__AVG__ || 0;
        if (fnName === "SUM" || fnName === "MIN" || fnName === "MAX") return null;
        if (fnName === "ARRAY_AGG" || fnName === "JSON_AGG" || fnName === "JSONB_AGG") return [];
        if (fnName === "JSON_OBJECT_AGG" || fnName === "JSONB_OBJECT_AGG") return {};

        const args = [];
        for (const argExpr of expr.args) {
          args.push(await this.evaluateExpr(storage, argExpr, row, params));
        }

        if (fnName === "NOW") return new Date().toISOString();
        if (fnName === "UPPER") return args[0] != null ? String(args[0]).toUpperCase() : null;
        if (fnName === "LOWER") return args[0] != null ? String(args[0]).toLowerCase() : null;
        if (fnName === "LENGTH") return args[0] != null ? String(args[0]).length : null;
        if (fnName === "TRIM") return args[0] != null ? String(args[0]).trim() : null;
        if (fnName === "REPLACE") {
          if (args[0] == null || args[1] == null || args[2] == null) return args[0];
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
          return args.filter(v => v != null).map(v => String(v)).join('');
        }
        if (fnName === "CONCAT_WS") {
          if (args[0] == null) return null;
          const sep = String(args[0]);
          return args.slice(1).filter(v => v != null).map(v => String(v)).join(sep);
        }
        if (fnName === "LTRIM") return args[0] != null ? String(args[0]).trimStart() : null;
        if (fnName === "RTRIM") return args[0] != null ? String(args[0]).trimEnd() : null;
        if (fnName === "LEFT") {
          if (args[0] == null || args[1] == null) return null;
          const str = String(args[0]);
          const n = Number(args[1]);
          return n >= 0 ? str.substring(0, n) : str.substring(0, Math.max(0, str.length + n));
        }
        if (fnName === "RIGHT") {
          if (args[0] == null || args[1] == null) return null;
          const str = String(args[0]);
          const n = Number(args[1]);
          return n >= 0 ? str.substring(Math.max(0, str.length - n)) : str.substring(Math.max(0, -n));
        }
        if (fnName === "LPAD") {
          if (args[0] == null || args[1] == null) return null;
          const str = String(args[0]);
          const n = Number(args[1]);
          const fill = String(args[2] ?? ' ');
          return str.length > n ? str.substring(0, n) : str.padStart(n, fill);
        }
        if (fnName === "RPAD") {
          if (args[0] == null || args[1] == null) return null;
          const str = String(args[0]);
          const n = Number(args[1]);
          const fill = String(args[2] ?? ' ');
          return str.length > n ? str.substring(0, n) : str.padEnd(n, fill);
        }
        if (fnName === "INITCAP") {
          if (args[0] == null) return null;
          const str = String(args[0]).toLowerCase();
          return str.replace(/(^|\s)\S/g, l => l.toUpperCase());
        }
        if (fnName === "REVERSE") {
          return args[0] != null ? String(args[0]).split('').reverse().join('') : null;
        }
        if (fnName === "STRPOS") {
          if (args[0] == null || args[1] == null) return null;
          return String(args[0]).indexOf(String(args[1])) + 1;
        }
        if (fnName === "REPEAT") {
          if (args[0] == null || args[1] == null) return null;
          const n = Number(args[1]);
          return n > 0 ? String(args[0]).repeat(n) : '';
        }
        if (fnName === "SPLIT_PART") {
          if (args[0] == null || args[1] == null || args[2] == null) return null;
          const parts = String(args[0]).split(String(args[1]));
          const idx = Number(args[2]);
          return (idx > 0 && idx <= parts.length) ? parts[idx - 1] : '';
        }
        if (fnName === "COALESCE") {
          for (const val of args) {
            if (val !== null && val !== undefined) return val;
          }
          return null;
        }
        if (fnName === "ABS") return args[0] != null ? Math.abs(Number(args[0])) : null;
        if (fnName === "CEIL" || fnName === "CEILING") return args[0] != null ? Math.ceil(Number(args[0])) : null;
        if (fnName === "FLOOR") return args[0] != null ? Math.floor(Number(args[0])) : null;
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
        if (fnName === "SQRT") return args[0] != null ? Math.sqrt(Number(args[0])) : null;
        if (fnName === "EXP") return args[0] != null ? Math.exp(Number(args[0])) : null;
        if (fnName === "LN") return args[0] != null ? Math.log(Number(args[0])) : null;
        if (fnName === "LOG") return args[0] != null ? Math.log10(Number(args[0])) : null;
        if (fnName === "MOD") {
          if (args[0] == null || args[1] == null) return null;
          return Number(args[0]) % Number(args[1]);
        }
        if (fnName === "SIGN") {
          if (args[0] == null) return null;
          const n = Number(args[0]);
          return n > 0 ? 1 : (n < 0 ? -1 : 0);
        }
        if (fnName === "PI") return Math.PI;
        if (fnName === "RANDOM") return Math.random();
        if (fnName === "DEGREES") return args[0] != null ? Number(args[0]) * (180 / Math.PI) : null;
        if (fnName === "RADIANS") return args[0] != null ? Number(args[0]) * (Math.PI / 180) : null;

        if (fnName === "JSON_EXTRACT" || fnName === "JSONB_EXTRACT") {
          let json = args[0];
          if (typeof json === "string") {
            try { json = JSON.parse(json); } catch { return null; }
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
          const target = args[0];
          const path = args[1];
          const newValue = args[2];
          const createMissing = args[3] !== false;
          if (!Array.isArray(path)) return target;
          return this.deepSet(target, path, newValue, createMissing);
        }

        if (fnName === "JSONB_INSERT") {
          const target = args[0];
          const path = args[1];
          const newValue = args[2];
          const insertAfter = args[3] === true;
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
            case "year": d.setMonth(0, 1); d.setHours(0, 0, 0, 0); break;
            case "month": d.setDate(1); d.setHours(0, 0, 0, 0); break;
            case "day": d.setHours(0, 0, 0, 0); break;
            case "hour": d.setMinutes(0, 0, 0); break;
            case "minute": d.setSeconds(0, 0); break;
            case "second": d.setMilliseconds(0); break;
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
          const months = ["January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December"];
          const days = ["Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"];
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
          const sortedPatterns = Object.keys(replacements).sort((a, b) => b.length - a.length);
          for (const pattern of sortedPatterns) {
            result = result.replace(new RegExp(pattern, "g"), replacements[pattern]!());
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
          for await (const r of storage.scanRows("pg_catalog.pg_description")) {
            if (Number(r.objoid) === oid && Number(r.objsubid) === 0) return r.description;
          }
          return null;
        }
        if (fnName === "COL_DESCRIPTION") {
          if (args[0] == null || args[1] == null) return null;
          const oid = Number(args[0]);
          const subid = Number(args[1]);
          if (isNaN(oid) || isNaN(subid)) return null;
          for await (const r of storage.scanRows("pg_catalog.pg_description")) {
            if (Number(r.objoid) === oid && Number(r.objsubid) === subid) return r.description;
          }
          return null;
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
            if (parsed && typeof parsed === 'object' && parsed.type === 'Literal') {
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
