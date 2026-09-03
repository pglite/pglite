import { Statement, Expr, JoinClause, OrderBy } from "../ast";
import { StorageEngine } from "../storage/engine";

export interface JITExecutable {
  execute(storage: StorageEngine, params: any[]): Promise<any[]>;
}

export class JITCompiler {
  public static canCompile(stmt: Statement): boolean {
    if (stmt.type !== "Select") return false;
    if (!stmt.from || !stmt.from.tableName) return false;
    if (stmt.from.stmt || stmt.from.fn) return false;
    if (stmt.groupBy || stmt.having) return false;
    if (stmt.distinct || stmt.distinctOn) return false;
    if (
      stmt.union ||
      stmt.unionAll ||
      stmt.intersect ||
      stmt.intersectAll ||
      stmt.except ||
      stmt.exceptAll
    )
      return false;

    // Check columns for unsupported or async ops
    for (const col of stmt.columns) {
      if (!this.canCompileExpr(col)) return false;
    }

    // Check WHERE clause
    if (stmt.where && !this.canCompileExpr(stmt.where)) return false;

    // Check joins
    if (stmt.joins && stmt.joins.length > 0) {
      for (const j of stmt.joins) {
        if (j.type !== "LEFT" && j.type !== "INNER" && j.type !== undefined) return false;
        if (!j.tableName || j.stmt || j.fn || j.lateral) return false;
        if (!j.on || j.on.type !== "Binary" || j.on.operator !== "=") return false;
        if (!this.canCompileExpr(j.on.left) || !this.canCompileExpr(j.on.right)) return false;
      }
    }

    // Check order by
    if (stmt.orderBy) {
      for (const ob of stmt.orderBy) {
        if (!this.canCompileExpr(ob.expr)) return false;
      }
    }

    return true;
  }

  private static canCompileExpr(expr: Expr): boolean {
    if (!expr) return true;
    switch (expr.type) {
      case "Literal":
      case "Parameter":
        return true;
      case "Identifier":
        return expr.name !== "*" && !expr.name.endsWith(".*");
      case "Alias":
        return this.canCompileExpr(expr.expr);
      case "Binary":
        return (
          ["=", "!=", ">", "<", ">=", "<=", "+", "-", "*", "/", "%", "||"].includes(
            expr.operator,
          ) &&
          this.canCompileExpr(expr.left) &&
          this.canCompileExpr(expr.right)
        );
      case "Logical":
        return (
          (expr.operator === "AND" || expr.operator === "OR") &&
          this.canCompileExpr(expr.left) &&
          this.canCompileExpr(expr.right)
        );
      case "Not":
      case "IsNull":
        return this.canCompileExpr(expr.expr);
      case "Cast": {
        const dt = (expr.dataType || "").toUpperCase();
        if (
          dt.includes("REGCLASS") ||
          dt.includes("REGTYPE") ||
          dt.includes("REGNAMESPACE")
        )
          return false;
        return this.canCompileExpr(expr.expr);
      }
      case "Call": {
        let fn = (expr.fnName || "").toUpperCase();
        if (fn.includes(".")) fn = fn.split(".").pop()!;
        const supported = [
          "COALESCE",
          "UPPER",
          "LOWER",
          "LENGTH",
          "CONCAT",
          "ABS",
          "ROUND",
          "FLOOR",
          "CEIL",
          "CEILING",
          "TRIM",
        ];
        if (!supported.includes(fn)) return false;
        return expr.args.every((a) => this.canCompileExpr(a));
      }
      default:
        return false;
    }
  }

  public static compile(stmtParam: Statement): JITExecutable | null {
    if (!this.canCompile(stmtParam)) return null;

    try {
      const stmt: any = stmtParam;
      const fromTable = stmt.from!.tableName!;
      const fromAlias = stmt.from!.alias || fromTable;

      const tableParamMap: Record<string, string> = {};
      const tableNames: string[] = [fromTable];
      const tableAliases: string[] = [fromAlias];
      tableParamMap[fromTable] = "t_0";
      tableParamMap[fromAlias] = "t_0";

      const joinDefs: {
        tableName: string;
        alias: string;
        type: string;
        paramName: string;
        leftKeyExpr: Expr;
        rightKeyExpr: Expr;
        leftParam: string;
        leftKeyProp: string;
        rightKeyProp: string;
      }[] = [];

      if (stmt.joins) {
        for (let j = 0; j < stmt.joins.length; j++) {
          const join = stmt.joins[j]!;
          const joinParam = `t_${j + 1}`;
          const jAlias = join.alias || join.tableName!;
          tableParamMap[join.tableName!] = joinParam;
          tableParamMap[jAlias] = joinParam;
          tableNames.push(join.tableName!);
          tableAliases.push(jAlias);

          const leftExpr = (join.on as any).left;
          const rightExpr = (join.on as any).right;

          let leftKeyProp = "";
          let rightKeyProp = "";
          let leftParam = "t_0";

          const getPrefix = (e: any) =>
            e.name && e.name.includes(".") ? e.name.split(".")[0] : null;
          const getCol = (e: any) =>
            e.name && e.name.includes(".") ? e.name.split(".")[1] : e.name;

          const leftPrefix = getPrefix(leftExpr);
          const rightPrefix = getPrefix(rightExpr);

          let leftK = leftExpr;
          let rightK = rightExpr;

          if (
            leftPrefix &&
            (leftPrefix === join.tableName || leftPrefix === join.alias)
          ) {
            leftK = rightExpr;
            rightK = leftExpr;
          }

          leftKeyProp = getCol(leftK);
          rightKeyProp = getCol(rightK);
          const lp = getPrefix(leftK);
          if (lp && tableParamMap[lp]) {
            leftParam = tableParamMap[lp]!;
          }

          joinDefs.push({
            tableName: join.tableName!,
            alias: jAlias,
            type: join.type || "LEFT",
            paramName: joinParam,
            leftKeyExpr: leftK,
            rightKeyExpr: rightK,
            leftParam,
            leftKeyProp,
            rightKeyProp,
          });
        }
      }

      // Collect required columns per table for projection pushdown
      const requiredColumnsByTable: Record<string, Set<string>> = {};
      for (const t of tableNames) requiredColumnsByTable[t] = new Set();

      const recordColAccess = (expr: Expr) => {
        if (!expr) return;
        if (expr.type === "Identifier" && expr.name.includes(".")) {
          const parts = expr.name.split(".");
          const tbl = parts[0]!;
          const col = parts[1]!;
          for (let i = 0; i < tableAliases.length; i++) {
            if (tableAliases[i] === tbl || tableNames[i] === tbl) {
              requiredColumnsByTable[tableNames[i]!]?.add(col);
              break;
            }
          }
        } else if (expr.type === "Identifier") {
          requiredColumnsByTable[fromTable]?.add(expr.name);
        }
        if ((expr as any).expr) recordColAccess((expr as any).expr);
        if ((expr as any).left) recordColAccess((expr as any).left);
        if ((expr as any).right) recordColAccess((expr as any).right);
        if ((expr as any).args && Array.isArray((expr as any).args)) {
          for (const a of (expr as any).args) recordColAccess(a);
        }
      };

      for (const col of stmt.columns) recordColAccess(col);
      if (stmt.where) recordColAccess(stmt.where);
      if (stmt.joins) {
        for (const j of stmt.joins) {
          recordColAccess(j.on);
        }
      }
      if (stmt.orderBy) {
        for (const ob of stmt.orderBy) recordColAccess(ob.expr);
      }

      const compileExprStr = (expr: Expr): string => {
        switch (expr.type) {
          case "Literal":
            return JSON.stringify(expr.value);
          case "Parameter":
            return `params[${expr.index - 1}]`;
          case "Identifier": {
            if (expr.name.includes(".")) {
              const parts = expr.name.split(".");
              const tbl = parts[0]!;
              const col = parts[1]!;
              const param = tableParamMap[tbl] || "t_0";
              return `(${param} ? ${param}[${JSON.stringify(col)}] : null)`;
            }
            return `(t_0 ? t_0[${JSON.stringify(expr.name)}] : null)`;
          }
          case "Alias":
            return compileExprStr(expr.expr);
          case "Binary": {
            const left = compileExprStr(expr.left);
            const right = compileExprStr(expr.right);
            if (expr.operator === "=") return `(${left} == ${right})`;
            if (expr.operator === "!=") return `(${left} != ${right})`;
            if (expr.operator === "||") {
              return `((${left} == null || ${right} == null) ? null : (String(${left}) + String(${right})))`;
            }
            return `(${left} ${expr.operator} ${right})`;
          }
          case "Logical": {
            const left = compileExprStr(expr.left);
            const right = compileExprStr(expr.right);
            const op = expr.operator === "AND" ? "&&" : "||";
            return `(${left} ${op} ${right})`;
          }
          case "Not":
            return `(!${compileExprStr(expr.expr)})`;
          case "IsNull":
            return `(${compileExprStr(expr.expr)} == null)`;
          case "Cast": {
            const dt = (expr.dataType || "").toUpperCase();
            const inner = compileExprStr(expr.expr);
            if (dt.includes("INT")) {
              return `((${inner}) != null ? (parseInt(${inner}) || 0) : null)`;
            }
            if (
              dt.includes("FLOAT") ||
              dt.includes("DOUBLE") ||
              dt.includes("NUMERIC") ||
              dt.includes("REAL")
            ) {
              return `((${inner}) != null ? (parseFloat(${inner}) || 0) : null)`;
            }
            if (dt.includes("BOOL")) {
              return `((${inner}) != null ? Boolean(${inner}) : null)`;
            }
            return `((${inner}) != null ? String(${inner}) : null)`;
          }
          case "Call": {
            let fn = (expr.fnName || "").toUpperCase();
            if (fn.includes(".")) fn = fn.split(".").pop()!;
            const args = expr.args.map(compileExprStr);
            if (fn === "COALESCE") {
              return `((${args.join(" ?? ")}) ?? null)`;
            }
            if (fn === "UPPER") {
              return `((${args[0]}) != null ? String(${args[0]}).toUpperCase() : null)`;
            }
            if (fn === "LOWER") {
              return `((${args[0]}) != null ? String(${args[0]}).toLowerCase() : null)`;
            }
            if (fn === "LENGTH") {
              return `((${args[0]}) != null ? String(${args[0]}).length : null)`;
            }
            if (fn === "TRIM") {
              return `((${args[0]}) != null ? String(${args[0]}).trim() : null)`;
            }
            if (fn === "ABS") {
              return `((${args[0]}) != null ? Math.abs(Number(${args[0]})) : null)`;
            }
            if (fn === "ROUND") {
              return `((${args[0]}) != null ? Math.round(Number(${args[0]})) : null)`;
            }
            if (fn === "FLOOR") {
              return `((${args[0]}) != null ? Math.floor(Number(${args[0]})) : null)`;
            }
            if (fn === "CEIL" || fn === "CEILING") {
              return `((${args[0]}) != null ? Math.ceil(Number(${args[0]})) : null)`;
            }
            if (fn === "CONCAT") {
              return `[${args.join(",")}].filter(x => x != null).join("")`;
            }
            return `null`;
          }
          default:
            return `null`;
        }
      };

      const projFields: string[] = [];
      const usedOutKeys = new Set<string>();
      for (const col of stmt.columns) {
        let outKey = "col";
        let targetExpr = col;
        if (col.type === "Alias") {
          outKey = col.alias;
          targetExpr = col.expr;
        } else if (col.type === "Identifier") {
          outKey = col.name.includes(".") ? col.name.split(".").pop()! : col.name;
        } else if (col.type === "Call") {
          outKey = col.fnName.toLowerCase();
        }

        if (usedOutKeys.has(outKey)) {
          let s = 1;
          while (usedOutKeys.has(`${outKey}${s}`)) s++;
          outKey = `${outKey}${s}`;
        }
        usedOutKeys.add(outKey);
        projFields.push(`${JSON.stringify(outKey)}: ${compileExprStr(targetExpr)}`);
      }

      const paramNames = ["t_0", ...joinDefs.map((j) => j.paramName), "params"];
      const projFnBody = `return { ${projFields.join(", ")} };`;
      const compiledProjector = new Function(...paramNames, projFnBody);

      let compiledWhereFn: Function | null = null;
      if (stmt.where) {
        const whereBody = `return Boolean(${compileExprStr(stmt.where)});`;
        compiledWhereFn = new Function(...paramNames, whereBody);
      }

      let compiledSortFn: ((a: any, b: any, params?: any) => number) | null = null;
      if (stmt.orderBy && stmt.orderBy.length > 0) {
        const obClauses: string[] = [];
        for (let i = 0; i < stmt.orderBy.length; i++) {
          const ob = stmt.orderBy[i]!;
          const exprStr = compileExprStr(ob.expr);
          const desc = ob.desc;
          obClauses.push(`
            const vA_${i} = ((t_0, params) => ${exprStr})(a, params);
            const vB_${i} = ((t_0, params) => ${exprStr})(b, params);
            if (vA_${i} < vB_${i}) return ${desc ? 1 : -1};
            if (vA_${i} > vB_${i}) return ${desc ? -1 : 1};
          `);
        }
        obClauses.push(`return 0;`);
        compiledSortFn = new Function("a", "b", "params", obClauses.join("\n")) as any;
      }

      const reqColsRecord: Record<string, string[]> = {};
      for (const t of tableNames) {
        const s = requiredColumnsByTable[t];
        reqColsRecord[t] = s && s.size > 0 ? Array.from(s) : [];
      }

      return {
        async execute(storage: StorageEngine, params: any[] = []): Promise<any[]> {
          const fromRows = await storage.scanRowsArray(
            fromTable,
            reqColsRecord[fromTable],
          );
          if (fromRows.length === 0) return [];

          // Hash join multi-slot map
          const joinMaps: Map<any, any[]>[] = [];
          for (let j = 0; j < joinDefs.length; j++) {
            const jDef = joinDefs[j]!;
            const jRows = await storage.scanRowsArray(
              jDef.tableName,
              reqColsRecord[jDef.tableName],
            );
            const map = new Map<any, any[]>();
            const rKey = jDef.rightKeyProp;
            for (let i = 0; i < jRows.length; i++) {
              const r = jRows[i];
              const k = r[rKey];
              if (k !== null && k !== undefined) {
                let arr = map.get(k);
                if (!arr) {
                  arr = [];
                  map.set(k, arr);
                }
                arr.push(r);
              }
            }
            joinMaps.push(map);
          }

          let limitVal = stmt.limit
            ? Number(
                stmt.limit.type === "Literal"
                  ? stmt.limit.value
                  : stmt.limit.type === "Parameter"
                  ? params[stmt.limit.index - 1]
                  : null,
              )
            : null;
          let offsetVal = stmt.offset
            ? Number(
                stmt.offset.type === "Literal"
                  ? stmt.offset.value
                  : stmt.offset.type === "Parameter"
                  ? params[stmt.offset.index - 1]
                  : 0,
              ) || 0
            : 0;

          const hasJoins = joinDefs.length > 0;
          const outRows: any[] = [];
          const maxRows =
            limitVal !== null && !stmt.orderBy
              ? offsetVal + limitVal
              : fromRows.length;

          if (!hasJoins) {
            for (let i = 0; i < fromRows.length; i++) {
              const r0 = fromRows[i];
              if (compiledWhereFn && !compiledWhereFn(r0, params)) continue;
              outRows.push(compiledProjector(r0, params));
              if (outRows.length >= maxRows) break;
            }
          } else if (joinDefs.length === 1) {
            const jDef = joinDefs[0]!;
            const jMap = joinMaps[0]!;
            const lKey = jDef.leftKeyProp;
            const isInner = jDef.type === "INNER";

            for (let i = 0; i < fromRows.length; i++) {
              const r0 = fromRows[i];
              const k = r0[lKey];
              const matches = k !== null && k !== undefined ? jMap.get(k) : null;

              if (matches && matches.length > 0) {
                for (let m = 0; m < matches.length; m++) {
                  const r1 = matches[m];
                  if (compiledWhereFn && !compiledWhereFn(r0, r1, params))
                    continue;
                  outRows.push(compiledProjector(r0, r1, params));
                  if (outRows.length >= maxRows) break;
                }
              } else if (!isInner) {
                if (compiledWhereFn && !compiledWhereFn(r0, null, params))
                  continue;
                outRows.push(compiledProjector(r0, null, params));
              }
              if (outRows.length >= maxRows) break;
            }
          } else {
            // Multi-join pipeline
            for (let i = 0; i < fromRows.length; i++) {
              const r0 = fromRows[i];
              const joinRowArgs: any[] = [r0];
              let matchAll = true;

              for (let j = 0; j < joinDefs.length; j++) {
                const jDef = joinDefs[j]!;
                const jMap = joinMaps[j]!;
                const lObj =
                  jDef.leftParam === "t_0"
                    ? r0
                    : joinRowArgs[Number(jDef.leftParam.slice(2))];
                const k = lObj ? lObj[jDef.leftKeyProp] : null;
                const matches = k !== null && k !== undefined ? jMap.get(k) : null;
                if (jDef.type === "INNER" && (!matches || matches.length === 0)) {
                  matchAll = false;
                  break;
                }
                joinRowArgs.push(matches && matches[0] ? matches[0] : null);
              }

              if (!matchAll) continue;
              if (compiledWhereFn && !compiledWhereFn(...joinRowArgs, params))
                continue;

              outRows.push(compiledProjector(...joinRowArgs, params));
              if (outRows.length >= maxRows) break;
            }
          }

          if (compiledSortFn && outRows.length > 1) {
            outRows.sort((a, b) => compiledSortFn!(a, b, params));
          }

          if (offsetVal > 0 || (limitVal !== null && outRows.length > limitVal)) {
            const end =
              limitVal !== null ? offsetVal + limitVal : outRows.length;
            return outRows.slice(offsetVal, end);
          }

          return outRows;
        },
      };
    } catch (e) {
      return null;
    }
  }
}
