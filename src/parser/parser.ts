import { Token, TokenType, Statement, Expr, ColumnDef, JoinClause, OrderBy } from '../ast';

export class Parser {
  private pos = 0;

  constructor(private tokens: Token[]) {}

  private current(): Token | undefined { return this.tokens[this.pos]; }

  public hasMore(): boolean {
    while (this.current() && this.match('SYMBOL', ';')) this.pos++;
    const token = this.current();
    return !!token && token.type !== 'EOF';
  }

  public match(type: TokenType, value?: string): boolean {
    const token = this.current();
    if (!token) return false;
    if (token.type !== type) return false;
    if (value && token.value.toUpperCase() !== value.toUpperCase()) return false;
    return true;
  }

  private matchIdentifier(): boolean {
    const token = this.current();
    return !!token && (token.type === 'IDENTIFIER' || token.type === 'KEYWORD');
  }

  private consumeIdentifier(): string {
    const token = this.current();
    if (!token || (token.type !== 'IDENTIFIER' && token.type !== 'KEYWORD')) {
      throw new Error(`Parse Error: Expected identifier, got ${token?.type} (${token?.value})`);
    }
    this.pos++;
    return token.value;
  }

  public consume(expectedType?: TokenType, expectedValue?: string): Token {
    const token = this.current();
    if (!token) throw new Error(`Parse Error: Unexpected end of input`);
    if (expectedType && token.type !== expectedType) throw new Error(`Parse Error: Expected type ${expectedType}, got ${token.type} (${token.value})`);
    if (expectedValue && token.value.toUpperCase() !== expectedValue.toUpperCase()) throw new Error(`Parse Error: Expected value '${expectedValue}', got '${token.value}'`);
    this.pos++;
    return token;
  }

  public parse(): Statement {
    let ctes: any[] =[];
    if (this.match('KEYWORD', 'WITH')) {
      this.consume();
      while (true) {
        const name = this.consumeIdentifier();
        this.consume('KEYWORD', 'AS');
        this.consume('SYMBOL', '(');
        const stmt = this.parseSelect();
        this.consume('SYMBOL', ')');
        ctes.push({ name, stmt });
        if (this.match('SYMBOL', ',')) {
          this.consume();
        } else break;
      }
    }

    const token = this.current();
    if (!token) throw new Error(`Parse Error: No input to parse`);
    let stmt: Statement;
    if (token.type === 'KEYWORD') {
      switch (token.value.toUpperCase()) {
        case 'CREATE': stmt = this.parseCreate(); break;
        case 'INSERT': stmt = this.parseInsert(); break;
        case 'SELECT': stmt = this.parseSelect(); break;
        case 'VALUES': stmt = this.parseValues(); break;
        case 'UPDATE': stmt = this.parseUpdate(); break;
        case 'DELETE': stmt = this.parseDelete(); break;
        case 'ALTER': stmt = this.parseAlter(); break;
        case 'DROP': stmt = this.parseDrop(); break;
        case 'COMMENT': stmt = this.parseComment(); break;
        case 'DO':
        case '$DO': stmt = this.parseDo(); break;
        case 'BEGIN': this.consume(); stmt = { type: 'Begin' }; break;
        case 'START':
          this.consume();
          this.consume('KEYWORD', 'TRANSACTION');
          stmt = { type: 'Begin' };
          break;
        case 'COMMIT':
        case 'END':
          this.consume();
          stmt = { type: 'Commit' };
          break;
        case 'ROLLBACK':
        case 'ABORT':
          this.consume();
          stmt = { type: 'Rollback' };
          break;
        default: throw new Error(`Parse Error: Unsupported statement starting with '${token.value}'`);
      }
    } else {
      throw new Error(`Parse Error: Expected statement, got ${token.type}`);
    }

    if (ctes.length > 0 && (stmt.type === 'Select' || stmt.type === 'Values')) (stmt as any).ctes = ctes;
    return stmt;
  }

  private parseExpr(): Expr { return this.parseOr(); }

  private parseQuery(): Statement {
    if (this.match('KEYWORD', 'WITH')) return this.parse();
    if (this.match('KEYWORD', 'SELECT')) return this.parseSelect();
    if (this.match('KEYWORD', 'VALUES')) return this.parseValues();
    throw new Error(`Parse Error: Expected SELECT or VALUES, got ${this.current()?.value}`);
  }

  private parseValues(): Statement {
    this.consume('KEYWORD', 'VALUES');
    const valuesList: Expr[][] = [];
    do {
      this.consume('SYMBOL', '(');
      const values: Expr[] = [];
      if (!this.match('SYMBOL', ')')) {
        values.push(this.parseExpr());
        while (this.match('SYMBOL', ',')) {
          this.consume();
          values.push(this.parseExpr());
        }
      }
      this.consume('SYMBOL', ')');
      valuesList.push(values);
    } while (this.match('SYMBOL', ',') && this.consume());
    return { type: 'Values', values: valuesList };
  }

  private parseReturning(): Expr[] {
    this.consume('KEYWORD', 'RETURNING');
    const columns: Expr[] = [];

    const parseColumn = () => {
      let expr = this.parseExpr();
      if (this.match('KEYWORD', 'AS')) {
        this.consume();
        const alias = this.consumeIdentifier();
        expr = { type: 'Alias', expr, alias };
      } else if (this.matchIdentifier() && !['FROM', 'WHERE', 'GROUP', 'ORDER', 'LIMIT', 'OFFSET', 'UNION', 'INTERSECT', 'EXCEPT', 'HAVING', 'RETURNING'].includes(this.current()?.value.toUpperCase() || '')) {
        const alias = this.consumeIdentifier();
        expr = { type: 'Alias', expr, alias };
      }
      return expr;
    };

    columns.push(parseColumn());
    while (this.match('SYMBOL', ',')) {
      this.consume();
      columns.push(parseColumn());
    }
    return columns;
  }

  private parseOr(): Expr {
    let left = this.parseAnd();
    while (this.match('KEYWORD', 'OR')) {
      const op = this.consume().value.toUpperCase();
      const right = this.parseAnd();
      left = { type: 'Logical', left, operator: op, right };
    }
    return left;
  }

  private parseAnd(): Expr {
    let left = this.parseUnaryNot();
    while (this.match('KEYWORD', 'AND')) {
      const op = this.consume().value.toUpperCase();
      const right = this.parseUnaryNot();
      left = { type: 'Logical', left, operator: op, right };
    }
    return left;
  }

  private parseUnaryNot(): Expr {
    if (this.match('KEYWORD', 'NOT')) {
      this.consume();
      return { type: 'Not', expr: this.parseUnaryNot() };
    }
    return this.parseCondition();
  }

  private parseCondition(): Expr {
    let left = this.parseAdditive();
    const ops = ['=', '>', '<', '>=', '<=', '!=', '->', '->>', '#>', '@>', '?', '&&', '~', '~*', '!~'];
    if (ops.includes(this.current()?.value || "") || this.match('KEYWORD', 'LIKE') || this.match('KEYWORD', 'ILIKE')) {
      const opToken = this.consume();
      const op = opToken.value.toUpperCase();
      const right = this.parseAdditive();
      if (op === 'LIKE' || op === 'ILIKE') {
        let escapeStr: string | undefined;
        if (this.match('KEYWORD', 'ESCAPE')) {
          this.consume();
          escapeStr = this.consume('STRING').value;
        }
        return { type: 'Like', left, right, escapeStr, ilike: op === 'ILIKE' };
      }
      return { type: 'Binary', left, operator: op, right };
    }

    let not = false;
    if (this.match('KEYWORD', 'NOT')) {
      this.consume();
      not = true;
    }

    if (this.match('KEYWORD', 'LIKE') || this.match('KEYWORD', 'ILIKE')) {
      const opToken = this.consume();
      const op = opToken.value.toUpperCase();
      const right = this.parseAdditive();
      let escapeStr: string | undefined;
      if (this.match('KEYWORD', 'ESCAPE')) {
        this.consume();
        escapeStr = this.consume('STRING').value;
      }
      return { type: 'Like', left, right, not, escapeStr, ilike: op === 'ILIKE' };
    }

    if (this.match('KEYWORD', 'BETWEEN')) {
      this.consume();
      const lower = this.parseAdditive();
      this.consume('KEYWORD', 'AND');
      const upper = this.parseAdditive();
      
      const leftExpr: Expr = { type: 'Binary', left, operator: '>=', right: lower };
      const rightExpr: Expr = { type: 'Binary', left, operator: '<=', right: upper };
      const betweenExpr: Expr = { type: 'Logical', left: leftExpr, operator: 'AND', right: rightExpr };
      
      return not ? { type: 'Not', expr: betweenExpr } : betweenExpr;
    }

    if (this.match('KEYWORD', 'IN')) {
      this.consume();
      this.consume('SYMBOL', '(');
      if (this.match('KEYWORD', 'SELECT')) {
        const stmt = this.parseSelect();
        this.consume('SYMBOL', ')');
        return { type: 'In', left, right: stmt, not };
      } else {
        const right: Expr[] =[];
        right.push(this.parseExpr());
        while (this.match('SYMBOL', ',')) {
          this.consume(); right.push(this.parseExpr());
        }
        this.consume('SYMBOL', ')');
        return { type: 'In', left, right, not };
      }
    }

    if (not) throw new Error("Parse Error: Unexpected NOT");

    if (this.match('KEYWORD', 'IS')) {
      this.consume();
      let not = false;
      if (this.match('KEYWORD', 'NOT')) {
        this.consume();
        not = true;
      }
      this.consume('KEYWORD', 'NULL');
      return { type: 'IsNull', expr: left, not };
    }
    return left;
  }

  private parseAdditive(): Expr {
    let left = this.parseMultiplicative();
    while (this.match('SYMBOL', '+') || this.match('SYMBOL', '-') || this.match('SYMBOL', '||')) {
      const op = this.consume().value;
      const right = this.parseMultiplicative();
      left = { type: 'Binary', left, operator: op, right };
    }
    return left;
  }

  private parseMultiplicative(): Expr {
    let left = this.parsePrimary();
    while (this.match('SYMBOL', '*') || this.match('SYMBOL', '/')) {
      const op = this.consume().value;
      const right = this.parsePrimary();
      left = { type: 'Binary', left, operator: op, right };
    }
    return left;
  }

  private parsePrimary(): Expr {
    return this.handleCast(this.parsePrimaryBase());
  }

  private parsePrimaryBase(): Expr {
    if (this.match('SYMBOL', '-')) {
      this.consume();
      const num = this.consume('NUMBER');
      return { type: 'Literal', value: -Number(num.value) };
    }
    if (this.match('SYMBOL', '(')) {
      this.consume();
      if (this.match('KEYWORD', 'SELECT')) {
        const stmt = this.parseSelect();
        this.consume('SYMBOL', ')');
        return { type: 'Subquery', stmt };
      }
      const expr = this.parseExpr();
      this.consume('SYMBOL', ')');
      return expr;
    }
    if (this.match('KEYWORD', 'NULL')) {
      this.consume();
      return { type: 'Literal', value: null };
    }
    if (this.match('KEYWORD', 'TRUE')) {
      this.consume();
      return { type: 'Literal', value: true };
    }
    if (this.match('KEYWORD', 'FALSE')) {
      this.consume();
      return { type: 'Literal', value: false };
    }
    if (this.match('KEYWORD', 'INTERVAL')) {
      this.consume();
      const val = this.consume('STRING').value;
      return { type: 'Interval', value: val };
    }
    if (this.match('KEYWORD', 'EXTRACT')) {
      this.consume();
      this.consume('SYMBOL', '(');
      const field = this.consumeIdentifier();
      this.consume('KEYWORD', 'FROM');
      const source = this.parseExpr();
      this.consume('SYMBOL', ')');
      return { type: 'Extract', field, source };
    }
    if (this.match('KEYWORD', 'EXISTS') && this.tokens[this.pos + 1]?.value === '(') {
      this.consume(); // EXISTS
      this.consume('SYMBOL', '(');
      const stmt = this.parseSelect();
      this.consume('SYMBOL', ')');
      return { type: 'Exists', stmt };
    }
    if (this.match('KEYWORD', 'CASE')) {
      this.consume();
      let caseExpr: Expr | undefined;
      if (!this.match('KEYWORD', 'WHEN')) {
        caseExpr = this.parseExpr();
      }
      const cases: { when: Expr; then: Expr }[] = [];
      while (this.match('KEYWORD', 'WHEN')) {
        this.consume();
        const when = this.parseExpr();
        this.consume('KEYWORD', 'THEN');
        const then = this.parseExpr();
        cases.push({ when, then });
      }
      let elseExpr;
      if (this.match('KEYWORD', 'ELSE')) {
        this.consume();
        elseExpr = this.parseExpr();
      }
      this.consume('KEYWORD', 'END');
      
      if (caseExpr) {
        for (const c of cases) {
          c.when = { type: 'Binary', left: caseExpr, operator: '=', right: c.when };
        }
      }
      return { type: 'Case', cases, elseExpr };
    }
    if (this.match('NUMBER')) {
      return { type: 'Literal', value: Number(this.consume().value) };
    }
    if (this.match('STRING')) {
      return { type: 'Literal', value: this.consume().value };
    }
    if (this.match('PARAMETER')) {
      const val = this.consume().value;
      const index = parseInt(val.substring(1));
      return { type: 'Parameter', index };
    }
    if (this.match('SYMBOL', '*')) {
      this.consume();
      return { type: 'Identifier', name: '*' };
    }
    if (this.matchIdentifier()) {
      let name = this.consume().value;
      if (name.toUpperCase() === 'ARRAY' && this.match('SYMBOL', '[')) {
        this.consume(); // [
        const elements: Expr[] = [];
        if (!this.match('SYMBOL', ']')) {
          elements.push(this.parseExpr());
          while (this.match('SYMBOL', ',')) {
            this.consume();
            elements.push(this.parseExpr());
          }
        }
        this.consume('SYMBOL', ']');
        return { type: 'Array', elements };
      }

      if (name.toUpperCase() === 'CAST' && this.match('SYMBOL', '(')) {
        this.consume();
        const expr = this.parseExpr();
        this.consume('KEYWORD', 'AS');
        const dataType = this.parseDataType();
        this.consume('SYMBOL', ')');
        return { type: 'Cast', expr, dataType };
      }

      while (this.match('SYMBOL', '.')) {
        this.consume();
        name += '.' + this.consumeIdentifier();
      }

      if (this.match('SYMBOL', '(')) {
        this.consume();
        const args: Expr[] =[];
        let distinct = false;
        if (this.match('KEYWORD', 'DISTINCT')) {
          this.consume();
          distinct = true;
        }
        let argsOrderBy: OrderBy[] | undefined;
        if (this.match('SYMBOL', '*')) {
          this.consume();
          args.push({ type: 'Identifier', name: '*' });
        } else if (!this.match('SYMBOL', ')')) {
          args.push(this.parseExpr());
          while (this.match('SYMBOL', ',')) {
            this.consume(); args.push(this.parseExpr());
          }
          if (this.match('KEYWORD', 'ORDER')) {
            this.consume(); this.consume('KEYWORD', 'BY');
            argsOrderBy = [];
            do {
              const expr = this.parseExpr();
              let desc = false;
              if (this.match('KEYWORD', 'DESC')) { this.consume(); desc = true; }
              else if (this.match('KEYWORD', 'ASC')) { this.consume(); }
              argsOrderBy.push({ expr, desc });
            } while (this.match('SYMBOL', ',') && this.consume());
          }
        }
        this.consume('SYMBOL', ')');
        
        let filter;
        if (this.match('KEYWORD', 'FILTER')) {
          this.consume();
          this.consume('SYMBOL', '(');
          this.consume('KEYWORD', 'WHERE');
          filter = this.parseExpr();
          this.consume('SYMBOL', ')');
        }

        let over;
        if (this.match('KEYWORD', 'OVER')) {
          this.consume();
          this.consume('SYMBOL', '(');
          let partitionBy;
          if (this.match('KEYWORD', 'PARTITION')) {
            this.consume(); this.consume('KEYWORD', 'BY');
            partitionBy =[];
            partitionBy.push(this.parseExpr());
            while (this.match('SYMBOL', ',')) {
              this.consume(); partitionBy.push(this.parseExpr());
            }
          }
          let orderBy;
          if (this.match('KEYWORD', 'ORDER')) {
            this.consume(); this.consume('KEYWORD', 'BY');
            orderBy =[];
            do {
              const expr = this.parseExpr();
              let desc = false;
              if (this.match('KEYWORD', 'DESC')) { this.consume(); desc = true; }
              else if (this.match('KEYWORD', 'ASC')) { this.consume(); }
              orderBy.push({ expr, desc });
            } while (this.match('SYMBOL', ',') && this.consume());
          }
          this.consume('SYMBOL', ')');
          over = { partitionBy, orderBy };
        }

        return { type: 'Call', fnName: name.toUpperCase(), args, distinct, argsOrderBy, over, filter };
      }
      return { type: 'Identifier', name };
    }
    throw new Error(`Parse Error: Unexpected token ${this.current()?.value} at primary`);
  }

  private handleCast(expr: Expr): Expr {
    let result = expr;
    while (this.match('SYMBOL', '::')) {
      this.consume();
      const dataType = this.parseDataType(); // Consume the whole type name string
      result = { type: 'Cast', expr: result, dataType };
    }
    return result;
  }

  private parseTableName(): string {
    let name = this.consumeIdentifier();
    while (this.match('SYMBOL', '.')) {
      this.consume();
      name += '.' + this.consumeIdentifier();
    }
    return name;
  }

  private parseDataType(): string {
    let dataType = this.consume().value;

    // Handle (n) or (p,s)
    if (this.match('SYMBOL', '(')) {
      dataType += this.consume().value;
      while (!this.match('SYMBOL', ')')) {
        dataType += this.consume().value;
      }
      dataType += this.consume().value;
    }

    // Handle sequences of identifiers/keywords for complex types
    // e.g., TIMESTAMP WITH TIME ZONE, integer[], USER-DEFINED
    const stopKeywords = new Set([
      'PRIMARY', 'UNIQUE', 'NOT', 'NULL', 'REFERENCES', 'DEFAULT', 'CONSTRAINT', 'CHECK', 'GENERATED',
      'AS', 'FROM', 'WHERE', 'GROUP', 'ORDER', 'LIMIT', 'OFFSET', 'UNION', 'INTERSECT', 'EXCEPT',
      'HAVING', 'RETURNING', 'ON', 'INNER', 'LEFT', 'RIGHT', 'FULL', 'CROSS', 'OUTER', 'JOIN', 'VALUES', 'SET', 'CONFLICT', 'DO',
      'END', 'THEN', 'ELSE', 'WHEN', 'FILTER', 'OVER', 'PARTITION', 'AND', 'OR', 'IS', 'IN',
      'ASC', 'DESC', 'TRUE', 'FALSE', 'LIKE'
    ]);
    while (this.match('KEYWORD') || this.match('IDENTIFIER') || this.match('SYMBOL', '[') || this.match('SYMBOL', ']')) {
      const next = this.current()!;
      if ((next.type === 'KEYWORD' || next.type === 'IDENTIFIER') && stopKeywords.has(next.value.toUpperCase())) break;
      if (next.type === 'SYMBOL' && !['[', ']'].includes(next.value)) break;
      
      if (next.value === '[' || next.value === ']') {
        dataType += this.consume().value;
      } else {
        dataType += ' ' + this.consume().value;
      }
    }

    return dataType;
  }

  private parseCreate(): Statement {
    this.consume('KEYWORD', 'CREATE');

    let isUnique = false;
    if (this.match('KEYWORD', 'UNIQUE')) {
      this.consume();
      isUnique = true;
    }

    if (this.match('KEYWORD', 'INDEX')) {
      this.consume();
      if ((this.match('KEYWORD') || this.matchIdentifier()) && this.current()?.value.toUpperCase() === 'CONCURRENTLY') {
        this.consume();
      }
      let ifNotExists = false;
      if (this.match('KEYWORD', 'IF')) {
        this.consume(); this.consume('KEYWORD', 'NOT'); this.consume('KEYWORD', 'EXISTS');
        ifNotExists = true;
      }
      const indexName = this.consumeIdentifier();
      this.consume('KEYWORD', 'ON');
      const tableName = this.parseTableName();
      
      if (this.matchIdentifier() && this.current()?.value.toUpperCase() === 'USING') {
        this.consume();
        this.consumeIdentifier();
      }

      this.consume('SYMBOL', '(');
      const columns: string[] = [];
      const parseIndexCol = () => {
        const col = this.consumeIdentifier();
        if (this.match('KEYWORD', 'ASC') || this.match('KEYWORD', 'DESC')) {
           this.consume();
        }
        if ((this.match('KEYWORD') || this.matchIdentifier()) && this.current()?.value.toUpperCase() === 'NULLS') {
           this.consume();
           if ((this.match('KEYWORD') || this.matchIdentifier()) && (this.current()?.value.toUpperCase() === 'FIRST' || this.current()?.value.toUpperCase() === 'LAST')) {
             this.consume();
           }
        }
        return col;
      };

      columns.push(parseIndexCol());
      while (this.match('SYMBOL', ',')) {
        this.consume();
        columns.push(parseIndexCol());
      }
      this.consume('SYMBOL', ')');
      
      return { type: 'CreateIndex', indexName, tableName, columns, unique: isUnique, ifNotExists };
    } else if (isUnique) {
      throw new Error("Parse Error: UNIQUE can only be used with INDEX");
    }

    if (this.match('KEYWORD', 'SCHEMA')) {
      this.consume();
      let ifNotExists = false;
      if (this.match('KEYWORD', 'IF')) {
        this.consume(); this.consume('KEYWORD', 'NOT'); this.consume('KEYWORD', 'EXISTS');
        ifNotExists = true;
      }
      const schemaName = this.consumeIdentifier();
      return { type: 'CreateSchema', schemaName, ifNotExists };
    }

    this.consume('KEYWORD', 'TABLE');
    let ifNotExists = false;
    if (this.match('KEYWORD', 'IF')) {
      this.consume(); this.consume('KEYWORD', 'NOT'); this.consume('KEYWORD', 'EXISTS');
      ifNotExists = true;
    }
    const tableName = this.parseTableName();
    this.consume('SYMBOL', '(');
    
    const columns: ColumnDef[] =[];
    while (!this.match('SYMBOL', ')')) {
      let isConstraint = false;
      if (this.match('KEYWORD', 'CONSTRAINT')) {
        this.consume(); // CONSTRAINT
        if (this.matchIdentifier()) this.consume(); // constraint name
        isConstraint = true;
      }
      
      if (this.match('KEYWORD', 'PRIMARY') || this.match('KEYWORD', 'FOREIGN') || this.match('KEYWORD', 'UNIQUE') || (this.matchIdentifier() && this.current()?.value.toUpperCase() === 'CHECK')) {
        isConstraint = true;
      }

      if (isConstraint) {
        if (this.match('KEYWORD', 'PRIMARY')) {
          this.consume(); this.consume('KEYWORD', 'KEY');
          if (this.match('SYMBOL', '(')) {
            this.consume();
            while (!this.match('SYMBOL', ')')) this.consume();
            this.consume('SYMBOL', ')');
          }
        } else if (this.match('KEYWORD', 'FOREIGN')) {
          this.consume(); this.consume('KEYWORD', 'KEY');
          this.consume('SYMBOL', '(');
          while (!this.match('SYMBOL', ')')) this.consume();
          this.consume('SYMBOL', ')');
          if (this.match('KEYWORD', 'REFERENCES')) {
            this.consume();
            this.parseTableName();
            if (this.match('SYMBOL', '(')) {
              this.consume();
              while (!this.match('SYMBOL', ')')) this.consume();
              this.consume('SYMBOL', ')');
            }
          }
        } else if (this.match('KEYWORD', 'UNIQUE')) {
          this.consume();
          if (this.match('SYMBOL', '(')) {
            this.consume();
            while (!this.match('SYMBOL', ')')) this.consume();
            this.consume('SYMBOL', ')');
          }
          if (this.match('KEYWORD', 'NULL') || this.match('IDENTIFIER', 'null')) this.consume();
        } else if (this.matchIdentifier() && this.current()?.value.toUpperCase() === 'CHECK') {
          this.consume(); // CHECK
          if (this.match('SYMBOL', '(')) {
            this.consume();
            let depth = 1;
            while (depth > 0 && this.current() && this.current()?.type !== 'EOF') {
              const token = this.consume();
              if (token.value === '(') depth++;
              else if (token.value === ')') depth--;
            }
          }
        }
      } else {
        const name = this.consumeIdentifier();
        let dataType = this.parseDataType();
        let isPrimaryKey = false, isUnique = false, isNotNull = false;
        let references: any, defaultVal;
        let generatedExpr: Expr | undefined;

        if (dataType.toUpperCase().includes('SERIAL')) isNotNull = true;

        while (true) {
          if (this.match('KEYWORD', 'PRIMARY')) {
            this.consume(); this.consume('KEYWORD', 'KEY'); isPrimaryKey = true; isNotNull = true;
          } else if (this.match('KEYWORD', 'UNIQUE')) {
            this.consume(); isUnique = true;
          } else if (this.match('KEYWORD', 'NOT')) {
            this.consume(); this.consume('KEYWORD', 'NULL'); isNotNull = true;
          } else if (this.match('KEYWORD', 'NULL')) {
            this.consume();
          } else if (this.match('KEYWORD', 'REFERENCES')) {
            this.consume(); 
            const refTable = this.parseTableName();
            let refCol = "id";
            if (this.match('SYMBOL', '(')) {
              this.consume();
              refCol = this.consumeIdentifier();
              this.consume('SYMBOL', ')');
            }
            references = { table: refTable, column: refCol };

            while (this.match('KEYWORD', 'ON')) {
              this.consume(); // ON
              const isDelete = this.match('KEYWORD', 'DELETE');
              const isUpdate = this.match('KEYWORD', 'UPDATE');
              if (isDelete || isUpdate) this.consume(); // DELETE or UPDATE

              let action: any;
              if (this.match('KEYWORD', 'SET')) {
                this.consume();
                if (this.match('KEYWORD', 'NULL')) {
                  this.consume(); action = 'SET NULL';
                } else if (this.match('KEYWORD', 'DEFAULT')) {
                  this.consume(); action = 'SET DEFAULT';
                }
              } else if (this.match('KEYWORD', 'CASCADE')) {
                this.consume(); action = 'CASCADE';
              } else if (this.match('KEYWORD', 'RESTRICT')) {
                this.consume(); action = 'RESTRICT';
              } else if (this.matchIdentifier() && this.current()?.value.toUpperCase() === 'NO') {
                this.consume();
                if (this.matchIdentifier() && this.current()?.value.toUpperCase() === 'ACTION') {
                  this.consume(); action = 'NO ACTION';
                }
              }
              if (isDelete) references.onDelete = action;
              if (isUpdate) references.onUpdate = action;
            }
          } else if (this.match('KEYWORD', 'DEFAULT')) {
            this.consume(); defaultVal = this.parseExpr();
          } else if (this.matchIdentifier() && this.current()?.value.toUpperCase() === 'GENERATED') {
            this.consume(); // GENERATED
            if (this.matchIdentifier() && ['ALWAYS', 'BY'].includes(this.current()?.value.toUpperCase() || '')) {
              this.consume();
              if (this.matchIdentifier() && this.current()?.value.toUpperCase() === 'DEFAULT') this.consume();
            }
            if (this.match('KEYWORD', 'AS')) {
              this.consume();
              if (this.match('SYMBOL', '(')) {
                // GENERATED ALWAYS AS (expr) STORED
                this.consume();
                generatedExpr = this.parseExpr();
                this.consume('SYMBOL', ')');
                if (this.matchIdentifier() && this.current()?.value.toUpperCase() === 'STORED') {
                  this.consume();
                }
              } else if (this.matchIdentifier() && this.current()?.value.toUpperCase() === 'IDENTITY') {
                this.consume();
                isNotNull = true;
                dataType = 'SERIAL';
              }
            } else if (this.matchIdentifier() && this.current()?.value.toUpperCase() === 'IDENTITY') {
              this.consume();
              isNotNull = true;
              dataType = 'SERIAL';
            } else {
              isNotNull = true;
              dataType = 'SERIAL';
            }
          } else if (this.matchIdentifier() && this.current()?.value.toUpperCase() === 'CHECK') {
            this.consume(); // CHECK
            if (this.match('SYMBOL', '(')) {
              this.consume();
              let depth = 1;
              while (depth > 0 && this.current() && this.current()?.type !== 'EOF') {
                const token = this.consume();
                if (token.value === '(') depth++;
                else if (token.value === ')') depth--;
              }
            }
          } else {
            break;
          }
        }
        columns.push({ name, dataType, isPrimaryKey, isUnique, isNotNull, references, defaultVal, generatedExpr });
      }
      
      if (this.match('SYMBOL', ',')) this.consume();
    }
    this.consume('SYMBOL', ')');
    return { type: 'CreateTable', tableName, columns, ifNotExists };
  }

  private parseAlter(): Statement {
    this.consume('KEYWORD', 'ALTER');
    this.consume('KEYWORD', 'TABLE');
    const tableName = this.parseTableName();

    if (this.match('KEYWORD', 'RENAME')) {
      this.consume();
      if (this.match('KEYWORD', 'COLUMN')) {
        this.consume();
        const oldColumnName = this.consumeIdentifier();
        this.consume('KEYWORD', 'TO');
        const newColumnName = this.consumeIdentifier();
        return { type: 'AlterTable', tableName, action: { type: 'RenameColumn', oldColumnName, newColumnName } };
      } else if (this.match('KEYWORD', 'TO')) {
        this.consume();
        const newTableName = this.parseTableName();
        return { type: 'AlterTable', tableName, action: { type: 'RenameTable', newTableName } };
      } else {
        const oldColumnName = this.consumeIdentifier();
        this.consume('KEYWORD', 'TO');
        const newColumnName = this.consumeIdentifier();
        return { type: 'AlterTable', tableName, action: { type: 'RenameColumn', oldColumnName, newColumnName } };
      }
    } else if (this.match('KEYWORD', 'DROP')) {
      this.consume();
      if (this.match('KEYWORD', 'COLUMN')) this.consume();
      let ifExists = false;
      if (this.match('KEYWORD', 'IF')) {
        this.consume(); this.consume('KEYWORD', 'EXISTS');
        ifExists = true;
      }
      const columnName = this.consumeIdentifier();
      return { type: 'AlterTable', tableName, action: { type: 'DropColumn', columnName, ifExists } };
    } else if (this.match('KEYWORD', 'ADD')) {
      this.consume();
      
      if (this.match('KEYWORD', 'CONSTRAINT')) {
        this.consume(); // CONSTRAINT
        const constraintName = this.consumeIdentifier();
        if (this.match('KEYWORD', 'FOREIGN')) {
          this.consume(); // FOREIGN
          this.consume('KEYWORD', 'KEY'); // KEY
          this.consume('SYMBOL', '(');
          const columnName = this.consumeIdentifier();
          this.consume('SYMBOL', ')');
          
          this.consume('KEYWORD', 'REFERENCES');
          const refTable = this.parseTableName();
          let refCol = "id";
          if (this.match('SYMBOL', '(')) {
            this.consume();
            refCol = this.consumeIdentifier();
            this.consume('SYMBOL', ')');
          }
          const references: any = { table: refTable, column: refCol };
          
          while (this.match('KEYWORD', 'ON')) {
            this.consume(); // ON
            const isDelete = this.match('KEYWORD', 'DELETE');
            const isUpdate = this.match('KEYWORD', 'UPDATE');
            if (isDelete || isUpdate) this.consume(); // DELETE or UPDATE

            let action: any;
            if (this.match('KEYWORD', 'SET')) {
              this.consume();
              if (this.match('KEYWORD', 'NULL')) {
                this.consume(); action = 'SET NULL';
              } else if (this.match('KEYWORD', 'DEFAULT')) {
                this.consume(); action = 'SET DEFAULT';
              }
            } else if (this.match('KEYWORD', 'CASCADE')) {
              this.consume(); action = 'CASCADE';
            } else if (this.match('KEYWORD', 'RESTRICT')) {
              this.consume(); action = 'RESTRICT';
            } else if (this.matchIdentifier() && this.current()?.value.toUpperCase() === 'NO') {
              this.consume();
              if (this.matchIdentifier() && this.current()?.value.toUpperCase() === 'ACTION') {
                this.consume(); action = 'NO ACTION';
              }
            }
            if (isDelete) references.onDelete = action;
            if (isUpdate) references.onUpdate = action;
          }
          
          return { type: 'AlterTable', tableName, action: { type: 'AddForeignKey', columnName, references } };
        } else {
           throw new Error("Parse Error: Only FOREIGN KEY constraints are supported in ALTER TABLE ADD CONSTRAINT currently");
        }
      }

      if (this.match('KEYWORD', 'COLUMN')) this.consume();
      let ifNotExists = false;
      if (this.match('KEYWORD', 'IF')) {
        this.consume(); this.consume('KEYWORD', 'NOT'); this.consume('KEYWORD', 'EXISTS');
        ifNotExists = true;
      }
      const name = this.consumeIdentifier();
      let dataType = this.parseDataType();
      let isPrimaryKey = false, isUnique = false, isNotNull = false;
      let references: any, defaultVal;
      let generatedExpr: Expr | undefined;

      if (dataType.toUpperCase().includes('SERIAL')) isNotNull = true;
      
      while (true) {
        if (this.match('KEYWORD', 'PRIMARY')) {
          this.consume(); this.consume('KEYWORD', 'KEY'); isPrimaryKey = true; isNotNull = true;
        } else if (this.match('KEYWORD', 'UNIQUE')) {
          this.consume(); isUnique = true;
        } else if (this.match('KEYWORD', 'NOT')) {
          this.consume(); this.consume('KEYWORD', 'NULL'); isNotNull = true;
        } else if (this.match('KEYWORD', 'NULL')) {
          this.consume();
        } else if (this.match('KEYWORD', 'REFERENCES')) {
          this.consume(); 
          const refTable = this.parseTableName();
          let refCol = "id";
          if (this.match('SYMBOL', '(')) {
            this.consume();
            refCol = this.consumeIdentifier();
            this.consume('SYMBOL', ')');
          }
          references = { table: refTable, column: refCol };

          while (this.match('KEYWORD', 'ON')) {
            this.consume(); // ON
            const isDelete = this.match('KEYWORD', 'DELETE');
            const isUpdate = this.match('KEYWORD', 'UPDATE');
            if (isDelete || isUpdate) this.consume(); // DELETE or UPDATE

            let action: any;
            if (this.match('KEYWORD', 'SET')) {
              this.consume();
              if (this.match('KEYWORD', 'NULL')) {
                this.consume(); action = 'SET NULL';
              } else if (this.match('KEYWORD', 'DEFAULT')) {
                this.consume(); action = 'SET DEFAULT';
              }
            } else if (this.match('KEYWORD', 'CASCADE')) {
              this.consume(); action = 'CASCADE';
            } else if (this.match('KEYWORD', 'RESTRICT')) {
              this.consume(); action = 'RESTRICT';
            } else if (this.matchIdentifier() && this.current()?.value.toUpperCase() === 'NO') {
              this.consume();
              if (this.matchIdentifier() && this.current()?.value.toUpperCase() === 'ACTION') {
                this.consume(); action = 'NO ACTION';
              }
            }
            if (isDelete) references.onDelete = action;
            if (isUpdate) references.onUpdate = action;
          }
        } else if (this.match('KEYWORD', 'DEFAULT')) {
          this.consume(); defaultVal = this.parseExpr();
        } else if (this.matchIdentifier() && this.current()?.value.toUpperCase() === 'GENERATED') {
          this.consume(); // GENERATED
          if (this.matchIdentifier() && ['ALWAYS', 'BY'].includes(this.current()?.value.toUpperCase() || '')) {
            this.consume();
            if (this.matchIdentifier() && this.current()?.value.toUpperCase() === 'DEFAULT') this.consume();
          }
          if (this.match('KEYWORD', 'AS')) {
            this.consume();
            if (this.match('SYMBOL', '(')) {
              this.consume();
              generatedExpr = this.parseExpr();
              this.consume('SYMBOL', ')');
              if (this.matchIdentifier() && this.current()?.value.toUpperCase() === 'STORED') {
                this.consume();
              }
            } else if (this.matchIdentifier() && this.current()?.value.toUpperCase() === 'IDENTITY') {
              this.consume();
              isNotNull = true;
              dataType = 'SERIAL';
            }
          } else if (this.matchIdentifier() && this.current()?.value.toUpperCase() === 'IDENTITY') {
            this.consume();
            isNotNull = true;
            dataType = 'SERIAL';
          } else {
            isNotNull = true;
            dataType = 'SERIAL';
          }
        } else if (this.matchIdentifier() && this.current()?.value.toUpperCase() === 'CHECK') {
          this.consume(); // CHECK
          if (this.match('SYMBOL', '(')) {
            this.consume();
            let depth = 1;
            while (depth > 0 && this.current() && this.current()?.type !== 'EOF') {
              const token = this.consume();
              if (token.value === '(') depth++;
              else if (token.value === ')') depth--;
            }
          }
        } else {
          break;
        }
      }
      return { type: 'AlterTable', tableName, action: { type: 'AddColumn', column: { name, dataType, isPrimaryKey, isUnique, isNotNull, references, defaultVal, generatedExpr }, ifNotExists } };
    } else if (this.match('KEYWORD', 'ALTER')) {
      this.consume();
      if (this.match('KEYWORD', 'COLUMN')) this.consume();
      const columnName = this.consumeIdentifier();
      
      if (this.match('KEYWORD', 'TYPE')) {
        this.consume();
        const dataType = this.parseDataType();
        return { type: 'AlterTable', tableName, action: { type: 'AlterColumnType', columnName, dataType } };
      } else if (this.match('KEYWORD', 'SET')) {
        this.consume();
        if (this.match('KEYWORD', 'NOT')) {
          this.consume(); this.consume('KEYWORD', 'NULL');
          return { type: 'AlterTable', tableName, action: { type: 'AlterColumnSetNotNull', columnName } };
        } else if (this.match('KEYWORD', 'DEFAULT')) {
          this.consume();
          const defaultVal = this.parseExpr();
          return { type: 'AlterTable', tableName, action: { type: 'AlterColumnSetDefault', columnName, defaultVal } };
        }
      } else if (this.match('KEYWORD', 'DROP')) {
        this.consume();
        if (this.match('KEYWORD', 'NOT')) {
          this.consume(); this.consume('KEYWORD', 'NULL');
          return { type: 'AlterTable', tableName, action: { type: 'AlterColumnDropNotNull', columnName } };
        } else if (this.match('KEYWORD', 'DEFAULT')) {
          this.consume();
          return { type: 'AlterTable', tableName, action: { type: 'AlterColumnDropDefault', columnName } };
        }
      }
    }

    throw new Error(`Parse Error: Unsupported ALTER TABLE action`);
  }

  private parseSelect(): Statement {
    this.consume('KEYWORD', 'SELECT');
    
    let distinct = false;
    let distinctOn: Expr[] | undefined;
    if (this.match('KEYWORD', 'DISTINCT')) {
      this.consume();
      distinct = true;
      if (this.match('KEYWORD', 'ON')) {
        this.consume();
        this.consume('SYMBOL', '(');
        distinctOn = [];
        distinctOn.push(this.parseExpr());
        while (this.match('SYMBOL', ',')) {
          this.consume();
          distinctOn.push(this.parseExpr());
        }
        this.consume('SYMBOL', ')');
      }
    }

    const columns: Expr[] =[];

    const parseColumn = () => {
      let expr = this.parseExpr();
      if (this.match('KEYWORD', 'AS')) {
        this.consume(); const alias = this.consumeIdentifier();
        expr = { type: 'Alias', expr, alias };
      } else if (this.matchIdentifier() && !['FROM', 'WHERE', 'GROUP', 'ORDER', 'LIMIT', 'OFFSET', 'UNION', 'INTERSECT', 'EXCEPT', 'HAVING', 'RETURNING'].includes(this.current()?.value.toUpperCase() || '')) {
        const alias = this.consumeIdentifier();
        expr = { type: 'Alias', expr, alias };
      }
      return expr;
    };

    if (!this.match('KEYWORD', 'FROM') && !this.match('EOF') && !this.match('SYMBOL', ';')) {
      columns.push(parseColumn());
      while (this.match('SYMBOL', ',')) {
        this.consume(); columns.push(parseColumn());
      }
    }
    
    let from: any = undefined;
    const joins: JoinClause[] = [];

    const parseFromSource = () => {
      let source: any = {};
      if (this.match('SYMBOL', '(')) {
        this.consume();
        const stmt = this.parseQuery();
        this.consume('SYMBOL', ')');
        source.stmt = stmt;
      } else {
        const next = this.tokens[this.pos + 1];
        if (this.matchIdentifier() && next?.type === 'SYMBOL' && next.value === '(') {
          source.fn = this.parseExpr();
        } else {
          source.tableName = this.parseTableName();
        }
      }

      if (this.match('KEYWORD', 'WITH')) {
        this.consume();
        this.consume('KEYWORD', 'ORDINALITY');
        source.withOrdinality = true;
      }

      if (this.match('KEYWORD', 'AS')) {
        this.consume();
        source.alias = this.consumeIdentifier();
        if (this.match('SYMBOL', '(')) {
          this.consume();
          const columnAliases = [];
          columnAliases.push(this.consumeIdentifier());
          while (this.match('SYMBOL', ',')) {
            this.consume();
            columnAliases.push(this.consumeIdentifier());
          }
          this.consume('SYMBOL', ')');
          source.columnAliases = columnAliases;
        }
      } else if (this.matchIdentifier() && !['INNER', 'LEFT', 'RIGHT', 'FULL', 'CROSS', 'OUTER', 'JOIN', 'WHERE', 'GROUP', 'ORDER', 'LIMIT', 'OFFSET', 'UNION', 'INTERSECT', 'EXCEPT', 'HAVING', 'RETURNING', 'WITH', 'ON', 'LATERAL'].includes(this.current()?.value.toUpperCase() || '')) {
        source.alias = this.consumeIdentifier();
      }
      return source;
    };

    if (this.match('KEYWORD', 'FROM')) {
      this.consume();
      from = parseFromSource();
      while (this.match('SYMBOL', ',')) {
        this.consume();
        const nextSource = parseFromSource();
        joins.push({
          type: 'CROSS',
          tableName: nextSource.tableName,
          stmt: nextSource.stmt,
          fn: nextSource.fn,
          withOrdinality: nextSource.withOrdinality,
          columnAliases: nextSource.columnAliases,
          alias: nextSource.alias,
          on: { type: 'Literal', value: true }
        });
      }
    }
    while (this.match('KEYWORD', 'INNER') || this.match('KEYWORD', 'LEFT') || this.match('KEYWORD', 'RIGHT') || this.match('KEYWORD', 'FULL') || this.match('KEYWORD', 'CROSS') || this.match('KEYWORD', 'JOIN')) {
      let type: 'INNER' | 'LEFT' | 'RIGHT' | 'FULL' | 'CROSS' = 'INNER';
      if (this.match('KEYWORD', 'LEFT')) {
        this.consume(); 
        if (this.match('KEYWORD', 'OUTER')) this.consume();
        type = 'LEFT'; this.consume('KEYWORD', 'JOIN');
      } else if (this.match('KEYWORD', 'RIGHT')) {
        this.consume(); 
        if (this.match('KEYWORD', 'OUTER')) this.consume();
        type = 'RIGHT'; this.consume('KEYWORD', 'JOIN');
      } else if (this.match('KEYWORD', 'FULL')) {
        this.consume(); 
        if (this.match('KEYWORD', 'OUTER')) this.consume();
        type = 'FULL'; this.consume('KEYWORD', 'JOIN');
      } else if (this.match('KEYWORD', 'CROSS')) {
        this.consume(); this.consume('KEYWORD', 'JOIN');
        type = 'CROSS';
      } else if (this.match('KEYWORD', 'INNER')) {
        this.consume(); this.consume('KEYWORD', 'JOIN');
      } else {
        this.consume('KEYWORD', 'JOIN');
      }
      
      let lateral = false;
      if (this.match('KEYWORD', 'LATERAL')) {
        this.consume();
        lateral = true;
      }
      
      let stmt_join: Statement | undefined;
      let fn_join: Expr | undefined;
      let tableName: string | undefined;
      let withOrdinality = false;
      let columnAliases: string[] | undefined;

      if (this.match('SYMBOL', '(')) {
        this.consume();
        stmt_join = this.parseQuery();
        this.consume('SYMBOL', ')');
      } else {
        const next = this.tokens[this.pos + 1];
        if (this.matchIdentifier() && next?.type === 'SYMBOL' && next.value === '(') {
          fn_join = this.parseExpr();
        } else {
          tableName = this.parseTableName();
        }
      }

      if (this.match('KEYWORD', 'WITH')) {
        this.consume();
        this.consume('KEYWORD', 'ORDINALITY');
        withOrdinality = true;
      }

      let alias;
      if (this.match('KEYWORD', 'AS')) {
        this.consume();
        alias = this.consumeIdentifier();
        if (this.match('SYMBOL', '(')) {
          this.consume();
          const aliases = [];
          aliases.push(this.consumeIdentifier());
          while (this.match('SYMBOL', ',')) {
            this.consume();
            aliases.push(this.consumeIdentifier());
          }
          this.consume('SYMBOL', ')');
          columnAliases = aliases;
        }
      } else if (this.matchIdentifier() && !['ON', 'WHERE', 'GROUP', 'ORDER', 'LIMIT', 'OFFSET', 'UNION', 'INTERSECT', 'EXCEPT', 'HAVING', 'INNER', 'LEFT', 'RIGHT', 'FULL', 'CROSS', 'OUTER', 'JOIN', 'LATERAL', 'WITH'].includes(this.current()?.value.toUpperCase() || '')) {
        alias = this.consumeIdentifier();
      }
      
      let on: Expr;
      if (this.match('KEYWORD', 'ON')) {
        this.consume();
        on = this.parseExpr();
      } else {
        on = { type: 'Literal', value: true };
      }
      
      joins.push({ type, lateral, tableName, stmt: stmt_join, fn: fn_join, withOrdinality, columnAliases, alias, on });
    }

    let where: Expr | undefined;
    if (this.match('KEYWORD', 'WHERE')) {
      this.consume(); where = this.parseExpr();
    }
    
    let groupBy: Expr[] | undefined;
    if (this.match('KEYWORD', 'GROUP')) {
      this.consume(); this.consume('KEYWORD', 'BY');
      groupBy =[];
      groupBy.push(this.parseExpr());
      while (this.match('SYMBOL', ',')) {
         this.consume(); groupBy.push(this.parseExpr());
      }
    }

    let having: Expr | undefined;
    if (this.match('KEYWORD', 'HAVING')) {
      this.consume(); having = this.parseExpr();
    }

    let orderBy: OrderBy[] | undefined;
    if (this.match('KEYWORD', 'ORDER')) {
      this.consume(); this.consume('KEYWORD', 'BY');
      orderBy =[];
      do {
         const expr = this.parseExpr();
         let desc = false;
         if (this.match('KEYWORD', 'DESC')) { this.consume(); desc = true; }
         else if (this.match('KEYWORD', 'ASC')) { this.consume(); }
         orderBy.push({ expr, desc });
      } while (this.match('SYMBOL', ',') && this.consume());
    }

    let limit: Expr | undefined;
    let offset: Expr | undefined;
    if (this.match('KEYWORD', 'LIMIT')) {
      this.consume(); limit = this.parseExpr();
    }
    if (this.match('KEYWORD', 'OFFSET')) {
      this.consume(); offset = this.parseExpr();
    }

    let stmt: Statement = { type: 'Select', columns, distinct, distinctOn, from, joins, where, groupBy, having, orderBy, limit, offset };

    if (this.match('KEYWORD', 'UNION')) {
      this.consume();
      let isAll = false;
      if (this.match('KEYWORD', 'ALL')) {
        this.consume();
        isAll = true;
      }
      const right = this.parseSelect();
      if (isAll) {
        stmt = { ...stmt, unionAll: right };
      } else {
        stmt = { ...stmt, union: right };
      }
    }
    if (this.match('KEYWORD', 'INTERSECT')) {
      this.consume();
      let isAll = false;
      if (this.match('KEYWORD', 'ALL')) {
        this.consume();
        isAll = true;
      }
      const right = this.parseSelect();
      if (isAll) {
        stmt = { ...stmt, intersectAll: right };
      } else {
        stmt = { ...stmt, intersect: right };
      }
    }
    if (this.match('KEYWORD', 'EXCEPT')) {
      this.consume();
      let isAll = false;
      if (this.match('KEYWORD', 'ALL')) {
        this.consume();
        isAll = true;
      }
      const right = this.parseSelect();
      if (isAll) {
        stmt = { ...stmt, exceptAll: right };
      } else {
        stmt = { ...stmt, except: right };
      }
    }

    return stmt;
  }

  private parseInsert(): Statement {
    this.consume('KEYWORD', 'INSERT');
    this.consume('KEYWORD', 'INTO');
    const tableName = this.parseTableName();
    
    const columns: string[] =[];
    if (this.match('SYMBOL', '(')) {
      this.consume('SYMBOL', '(');
      columns.push(this.consumeIdentifier());
      while (this.match('SYMBOL', ',')) {
        this.consume(); columns.push(this.consumeIdentifier());
      }
      this.consume('SYMBOL', ')');
    }
    
    let valuesList: Expr[][] | undefined;
    let selectStmt: Statement | undefined;

    if (this.match('KEYWORD', 'VALUES')) {
      const vStmt = this.parseValues() as any;
      valuesList = vStmt.values;
    } else if (this.match('KEYWORD', 'SELECT')) {
      selectStmt = this.parseSelect();
    } else {
      throw new Error(`Parse Error: Expected VALUES or SELECT in INSERT statement, got ${this.current()?.value}`);
    }

    let onConflict: any;
    if (this.match('KEYWORD', 'ON')) {
      this.consume(); // ON
      this.consume('KEYWORD', 'CONFLICT');
      
      let targetColumns: string[] | undefined;
      if (this.match('SYMBOL', '(')) {
        this.consume();
        targetColumns = [];
        targetColumns.push(this.consumeIdentifier());
        while (this.match('SYMBOL', ',')) {
          this.consume();
          targetColumns.push(this.consumeIdentifier());
        }
        this.consume('SYMBOL', ')');
      }

      this.consume('KEYWORD', 'DO');
      if (this.match('KEYWORD', 'NOTHING')) {
        this.consume();
        onConflict = { action: 'NOTHING', targetColumns };
      } else if (this.match('KEYWORD', 'UPDATE')) {
        this.consume();
        this.consume('KEYWORD', 'SET');
        const assignments: Record<string, Expr> = {};
        do {
          const colName = this.consumeIdentifier();
          this.consume('SYMBOL', '=');
          assignments[colName] = this.parseExpr();
        } while (this.match('SYMBOL', ',') && this.consume());
        onConflict = { action: 'UPDATE', targetColumns, assignments };
      }
    }

    let returning: Expr[] | undefined;
    if (this.match('KEYWORD', 'RETURNING')) {
      returning = this.parseReturning();
    }
    
    return { type: 'Insert', tableName, columns, values: valuesList, select: selectStmt, returning, onConflict };
  }

  private parseUpdate(): Statement {
    this.consume('KEYWORD', 'UPDATE');
    const tableName = this.parseTableName();
    this.consume('KEYWORD', 'SET');
    
    const assignments: Record<string, Expr> = {};
    do {
      const colName = this.consumeIdentifier();
      this.consume('SYMBOL', '=');
      assignments[colName] = this.parseExpr();
    } while (this.match('SYMBOL', ',') && this.consume());
    
    let where: Expr | undefined;
    if (this.match('KEYWORD', 'WHERE')) {
      this.consume(); where = this.parseExpr();
    }

    let returning: Expr[] | undefined;
    if (this.match('KEYWORD', 'RETURNING')) {
      returning = this.parseReturning();
    }
    
    return { type: 'Update', tableName, assignments, where, returning };
  }

  private parseDelete(): Statement {
    this.consume('KEYWORD', 'DELETE');
    this.consume('KEYWORD', 'FROM');
    const tableName = this.parseTableName();
    
    let where: Expr | undefined;
    if (this.match('KEYWORD', 'WHERE')) {
      this.consume(); where = this.parseExpr();
    }

    let returning: Expr[] | undefined;
    if (this.match('KEYWORD', 'RETURNING')) {
      returning = this.parseReturning();
    }
    
    return { type: 'Delete', tableName, where, returning };
  }

  private parseDrop(): Statement {
    this.consume('KEYWORD', 'DROP');
    if (this.match('KEYWORD', 'SCHEMA')) {
      this.consume();
      let ifExists = false;
      if (this.match('KEYWORD', 'IF')) {
        this.consume(); this.consume('KEYWORD', 'EXISTS');
        ifExists = true;
      }
      const schemaName = this.consumeIdentifier();
      let cascade = false;
      if (this.match('KEYWORD', 'CASCADE')) {
        this.consume(); cascade = true;
      } else if (this.match('KEYWORD', 'RESTRICT')) {
        this.consume();
      }
      return { type: 'DropSchema', schemaName, ifExists, cascade };
    } else if (this.match('KEYWORD', 'TABLE')) {
      this.consume();
      let ifExists = false;
      if (this.match('KEYWORD', 'IF')) {
        this.consume(); this.consume('KEYWORD', 'EXISTS');
        ifExists = true;
      }
      const tableName = this.parseTableName();
      return { type: 'DropTable', tableName, ifExists };
    }
    throw new Error(`Parse Error: Unsupported DROP statement`);
  }

  private parseComment(): Statement {
    this.consume('KEYWORD', 'COMMENT');
    this.consume('KEYWORD', 'ON');
    const objectType = this.consume().value.toUpperCase(); // TABLE or COLUMN
    const objectName = this.parseTableName();
    this.consume('KEYWORD', 'IS');
    const comment = this.consume('STRING').value;
    return { type: 'Comment', objectType, objectName, comment };
  }

  private parseDo(): Statement {
    this.consume(); // consume DO or $DO
    let language;
    if (this.match('KEYWORD', 'LANGUAGE')) {
      this.consume();
      language = this.consumeIdentifier();
    }
    const code = this.consume('STRING').value;
    return { type: 'Do', code, language };
  }
}