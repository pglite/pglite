import { Token } from "../ast";

export class Lexer {
  private pos = 0;
  private readonly input: string;
  private readonly KEYWORDS = new Set([
    "CREATE", "TABLE", "INSERT", "INTO", "VALUES", "SELECT", "FROM", "WHERE", "UPDATE", "SET", "DELETE",
    "PRIMARY", "KEY", "SERIAL", "TEXT", "NUMBER", "AND", "OR", "NOT", "IN", "LIKE", "ORDER", "BY",
    "DESC", "ASC", "LIMIT", "OFFSET", "INNER", "LEFT", "RIGHT", "FULL", "CROSS", "OUTER", "JOIN", "ON", "GROUP", "HAVING", "AS", "RETURNING", "BETWEEN",
    "UNIQUE", "NULL", "IS", "REFERENCES", "ALTER", "ADD", "COLUMN", "DEFAULT", "BEGIN", "COMMIT", "ROLLBACK",
    "CONFLICT", "NOTHING", "OVER", "PARTITION", "CASE", "WHEN", "THEN", "ELSE", "ARRAY", "UNNEST", "ORDINALITY",
    "WITH", "UNION", "ALL", "INTERSECT", "EXCEPT", "DO", "LANGUAGE", "$DO", "SCHEMA", "IF", "EXISTS", "CASCADE", "RESTRICT", "DROP", "FILTER", "INTERVAL", "ESCAPE", "ILIKE", "DISTINCT",
    "EXTRACT", "AGE", "TO_CHAR", "DATE_PART",
    "RENAME", "TO", "TYPE", "COMMENT", "ON", "IS", "CONSTRAINT", "FOREIGN", "TRUE", "FALSE", "CAST", "LATERAL",
    "START", "TRANSACTION", "ABORT", "END", "CHECK", "GENERATED", "ALWAYS", "IDENTITY", "INDEX",
    "INT", "INTEGER", "SMALLINT", "BIGINT", "DECIMAL", "NUMERIC", "REAL", "DOUBLE", "PRECISION", "MONEY",
    "VARCHAR", "CHAR", "CHARACTER", "VARYING", "TEXT", "BYTEA", "TIMESTAMP", "TIMESTAMPTZ", "DATE", "TIME", "TIMETZ", "INTERVAL",
    "BOOLEAN", "POINT", "LINE", "LSEG", "BOX", "PATH", "POLYGON", "CIRCLE", "INET", "CIDR", "MACADDR", "MACADDR8",
    "BIT", "TSVECTOR", "TSQUERY", "UUID", "XML", "JSON", "JSONB", "RECORD", "OID", "REGCLASS", "REGTYPE", "REGNAMESPACE"
  ]);

  constructor(input: string) {
    this.input = input;
  }

  public tokenize(): Token[] {
    const tokens: Token[] =[];
    while (this.pos < this.input.length) {
      const char = this.input[this.pos];
      if (!char) break;

      if (/\s/.test(char)) {
        this.pos++;
        continue;
      }

      // Single-line comments (--)
      if (char === '-' && this.input[this.pos + 1] === '-') {
        this.pos += 2;
        while (this.pos < this.input.length && this.input[this.pos] !== '\n') {
          this.pos++;
        }
        continue;
      }

      // Multi-line comments (/* ... */)
      if (char === '/' && this.input[this.pos + 1] === '*') {
        this.pos += 2;
        while (this.pos < this.input.length) {
          if (this.input[this.pos] === '*' && this.input[this.pos + 1] === '/') {
            this.pos += 2;
            break;
          }
          this.pos++;
        }
        continue;
      }

      if (char === "$") {
        const paramMatch = this.input.slice(this.pos).match(/^\$([0-9]+)/);
        if (paramMatch) {
          tokens.push({ type: "PARAMETER", value: paramMatch[0] });
          this.pos += paramMatch[0].length;
          continue;
        }

        let isQuote = false;
        let tag = "$";
        const tagMatch = this.input.slice(this.pos).match(/^\$([a-zA-Z0-9_]*)\$/);
        if (tagMatch) {
          isQuote = true;
          tag = tagMatch[0];
        } else if (this.pos + 1 >= this.input.length || !/[a-zA-Z0-9_]/.test(this.input[this.pos + 1] ?? "")) {
          isQuote = true;
          tag = "$";
        }

        if (isQuote) {
          tokens.push(this.readDollarString(tag));
          continue;
        }
      }

      if (char === '"') {
        tokens.push(this.readQuotedIdentifier());
        continue;
      }

      if (/[a-zA-Z_$]/.test(char)) {
        tokens.push(this.readWord());
        continue;
      }

      if (/[0-9]/.test(char) || (char === '.' && /[0-9]/.test(this.input[this.pos + 1] || ""))) {
        tokens.push(this.readNumber());
        continue;
      }

      if (char === "'") {
        tokens.push(this.readString());
        continue;
      }

      const threeChar = this.input.slice(this.pos, this.pos + 3);
      if (threeChar === '->>') {
        tokens.push({ type: "SYMBOL", value: threeChar });
        this.pos += 3;
        continue;
      }

      const twoChar = this.input.slice(this.pos, this.pos + 2);
      if (['>=', '<=', '!=', '::', '||', '->', '~*', '!~', '#>', '@>', '&&'].includes(twoChar)) {
        tokens.push({ type: "SYMBOL", value: twoChar });
        this.pos += 2;
        continue;
      }

      if (/[=><*+,();\/.:;[\]|!~?\-]/.test(char)) {
        tokens.push({ type: "SYMBOL", value: char });
        this.pos++;
        continue;
      }

      throw new Error(`Lexer Error: Unexpected character '${char}' at position ${this.pos}`);
    }
    tokens.push({ type: "EOF", value: "" });
    return tokens;
  }

  private readWord(): Token {
    let start = this.pos;
    while (
      this.pos < this.input.length &&
      /[a-zA-Z0-9_$]/.test(this.input[this.pos] || "")
    ) {
      this.pos++;
    }
    const word = this.input.slice(start, this.pos);
    return {
      type: this.KEYWORDS.has(word.toUpperCase()) ? "KEYWORD" : "IDENTIFIER",
      value: word,
    };
  }

  private readNumber(): Token {
    let start = this.pos;
    let hasDot = false;
    let hasE = false;
    while (this.pos < this.input.length) {
      const char = this.input[this.pos] || "";
      if (/[0-9]/.test(char)) {
        this.pos++;
      } else if (char === '.' && !hasDot && !hasE) {
        hasDot = true;
        this.pos++;
      } else if ((char === 'e' || char === 'E') && !hasE) {
        hasE = true;
        this.pos++;
        const nextChar = this.input[this.pos] || "";
        if (nextChar === '+' || nextChar === '-') {
          this.pos++;
        }
      } else {
        break;
      }
    }
    return { type: "NUMBER", value: this.input.slice(start, this.pos) };
  }

  private readString(): Token {
    this.pos++; // skip opening quote
    let res = "";
    let start = this.pos;
    while (this.pos < this.input.length) {
      if (this.input[this.pos] === "'") {
        if (this.input[this.pos + 1] === "'") {
          // PostgreSQL escaped single quote: ''
          res += this.input.slice(start, this.pos) + "'";
          this.pos += 2;
          start = this.pos;
        } else {
          // End of string
          res += this.input.slice(start, this.pos);
          this.pos++; // skip closing quote
          return { type: "STRING", value: res };
        }
      } else {
        this.pos++;
      }
    }
    throw new Error("Lexer Error: Unterminated string");
  }

  private readQuotedIdentifier(): Token {
    this.pos++; // skip "
    let start = this.pos;
    while (this.pos < this.input.length && this.input[this.pos] !== '"') {
      this.pos++;
    }
    if (this.pos >= this.input.length) throw new Error("Lexer Error: Unterminated quoted identifier");
    const id = this.input.slice(start, this.pos);
    this.pos++; // skip "
    return { type: "IDENTIFIER", value: id };
  }

  private readDollarString(tag: string): Token {
    this.pos += tag.length; // skip opening tag
    let start = this.pos;
    while (this.pos < this.input.length) {
      if (this.input.startsWith(tag, this.pos)) {
        break;
      }
      this.pos++;
    }
    if (this.pos >= this.input.length) throw new Error("Lexer Error: Unterminated dollar-quoted string");
    const str = this.input.slice(start, this.pos);
    this.pos += tag.length; // skip closing tag
    return { type: "STRING", value: str };
  }
}
