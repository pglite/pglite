//! Tên (viết hoa) mọi hàm / từ khoá dạng `tên(` mà engine xử lý ở đâu đó (sinh từ mã nguồn rồi bổ sung tay).
//! Dùng để báo `42883 function ... does not exist` chỉ cho hàm thật sự không có — hàm có trong danh sách mà
//! tính lỗi vẫn giữ hành vi cũ. Hàm mới cài trong `fnlib` / `strfn` / `srf` / aggregate tự được tính là đã biết.
pub(crate) const KNOWN: &[&str] = &[
    "ABS", "AD", "ADD", "AFTER", "AGE", "ALL", "ALTER", "AM", "AND", "ANY", "ANY_VALUE", "ARRAY", "ARRAY_AGG", "ARRAY_APPEND",
    "ARRAY_CAT", "ARRAY_DIMS", "ARRAY_FILL", "ARRAY_LENGTH", "ARRAY_LOWER", "ARRAY_POSITIONS", "ARRAY_PREPEND", "ARRAY_REMOVE",
    "ARRAY_REPLACE", "ARRAY_REVERSE", "ARRAY_SAMPLE", "ARRAY_SHUFFLE", "ARRAY_SORT", "ARRAY_TO_STRING", "ARRAY_TO_TSVECTOR",
    "ARRAY_UPPER", "AS", "ASC", "ASCII", "AUTHORIZATION", "AVG", "BC", "BEFORE", "BEGIN", "BETWEEN", "BIGINT", "BIGSERIAL",
    "BIT", "BIT_AND", "BIT_OR", "BIT_XOR", "BOOL", "BOOLEAN", "BOOL_AND", "BOOL_OR", "BOX", "BPCHAR", "BTRIM", "BY", "BYTEA",
    "CACHE", "CARDINALITY", "CASE", "CAST", "CBRT", "CC", "CEIL", "CEILING", "CHAR", "CHARACTER", "CHARACTERISTICS",
    "CHARACTER_LENGTH", "CHAR_LENGTH", "CHECK", "CHR", "CIDR", "CIRCLE", "CLOCK_TIMESTAMP", "COALESCE", "COLLATE", "COLUMN",
    "COL_DESCRIPTION", "COMMIT", "CONCAT", "CONCAT_WS", "CONCURRENTLY", "CONSTRAINT", "CONSTRAINTS", "CONTINUE", "CONVERT_TO",
    "COPY", "COUNT", "CREATE", "CROSS", "CUBE", "CUME_DIST", "CURRENT", "CURRENT_CATALOG", "CURRENT_DATABASE", "CURRENT_DATE",
    "CURRENT_ROLE", "CURRENT_SCHEMA", "CURRENT_SCHEMAS", "CURRENT_SETTING", "CURRENT_TIME", "CURRENT_TIMESTAMP", "CURRENT_USER",
    "CURRVAL", "CYCLE", "DATE", "DATEMULTIRANGE", "DATERANGE", "DATETIME", "DATE_ADD", "DATE_BIN", "DATE_PART", "DATE_SUBTRACT",
    "DATE_TRUNC", "DAY", "DD", "DDD", "DEBUG", "DECIMAL", "DECLARE", "DECODE", "DEFAULT", "DEFERRABLE", "DEGREES", "DELETE",
    "DENSE_RANK", "DESC", "DIAGNOSTICS", "DIGEST", "DISCARD", "DISTINCT", "DIV", "DO", "DOMAIN", "DOUBLE", "DROP", "DY", "ELSE",
    "ELSEIF", "ELSIF", "ENCODE", "END", "ENUM", "ESCAPE", "EVERY", "EXCEPT", "EXCEPTION", "EXCLUDE", "EXECUTE", "EXISTS", "EXIT",
    "EXP", "EXTRACT", "FALSE", "FETCH", "FILTER", "FIRST", "FIRST_VALUE", "FLOAT", "FLOAT4", "FLOAT8", "FLOOR", "FM",
    "FOLLOWING", "FOR", "FOREACH", "FOREIGN", "FORMAT", "FORMAT_TYPE", "FROM", "FULL", "FUNCTION", "GCD", "GENERATED",
    "GENERATE_SERIES", "GENERATE_SUBSCRIPTS", "GEN_RANDOM_UUID", "GET", "GREATEST", "GROUP", "GROUPING", "GROUPS", "HAVING",
    "HH", "HH12", "HH24", "HMAC", "ID", "IDDD", "IDENTITY", "IF", "ILIKE", "IN", "INCREMENT", "INDEX", "INET", "INFO", "INITCAP",
    "INNER", "INSERT", "INT", "INT2", "INT4", "INT4MULTIRANGE", "INT4RANGE", "INT8", "INT8MULTIRANGE", "INT8RANGE", "INTEGER",
    "INTERSECT", "INTERVAL", "INTO", "IS", "ISEMPTY", "ISFINITE", "ISNULL", "IS_NORMALIZED", "IW", "IY", "IYY", "IYYY", "JOIN",
    "JSON", "JSONB", "JSONB_AGG", "JSONB_ARRAY_LENGTH", "JSONB_BUILD_ARRAY", "JSONB_BUILD_OBJECT", "JSONB_EXTRACT_PATH",
    "JSONB_EXTRACT_PATH_TEXT", "JSONB_OBJECT_AGG", "JSONB_PRETTY", "JSONB_SET", "JSONB_STRIP_NULLS", "JSONB_TYPEOF", "JSON_AGG",
    "JSON_ARRAY_LENGTH", "JSON_BUILD_ARRAY", "JSON_BUILD_OBJECT", "JSON_EXTRACT_PATH", "JSON_EXTRACT_PATH_TEXT",
    "JSON_OBJECT_AGG", "JSON_PRETTY", "JSON_SET", "JSON_TABLE", "JSON_TYPEOF", "JUSTIFY_DAYS", "JUSTIFY_HOURS",
    "JUSTIFY_INTERVAL", "KEY", "LAG", "LAST", "LASTVAL", "LAST_VALUE", "LATERAL", "LCM", "LEAD", "LEAST", "LEFT", "LENGTH",
    "LIKE", "LIMIT", "LINE", "LISTEN", "LN", "LOCAL", "LOCALTIME", "LOCALTIMESTAMP", "LOG", "LOG10", "LOGGED", "LOOP", "LOWER",
    "LOWER_INC", "LOWER_INF", "LPAD", "LSEG", "LTRIM", "MACADDR", "MAKE_DATE", "MAKE_INTERVAL", "MAKE_TIME", "MAKE_TIMESTAMP",
    "MAKE_TIMESTAMPTZ", "MATERIALIZED", "MAX", "MAXVALUE", "MD5", "MERGE", "MI", "MIN", "MINVALUE", "MM", "MOD", "MODE", "MON",
    "MONEY", "MONTH", "MOVE", "MS", "MULTIRANGE", "NAMES", "NATURAL", "ND", "NEXTVAL", "NO", "NONE", "NORMALIZED", "NOT",
    "NOTICE", "NOTIFY", "NOTNULL", "NOW", "NTH_VALUE", "NTILE", "NULL", "NULLIF", "NULLS", "NUMERIC", "NUMMULTIRANGE", "NUMNODE",
    "NUMRANGE", "OBJ_DESCRIPTION", "OCTET_LENGTH", "OF", "OFFSET", "ON", "ONLY", "OPERATOR", "OR", "ORDER", "OUTER", "OVER",
    "OVERLAPS", "OVERLAY", "OWNED", "OWNER", "PARTITION", "PATH", "PATH_QUERY", "PERCENTILE_DISC", "PERCENT_RANK", "PERFORM",
    "PGLITE_TIMEZONE", "PG_GET_SERIAL_SEQUENCE", "PG_TYPEOF", "PHRASETO_TSQUERY", "PI", "PLAINTO_TSQUERY", "PM", "POINT",
    "POLYGON", "POSITION", "POW", "POWER", "PRECEDING", "PRECISION", "PRIMARY", "QUERYTREE", "QUOTE_IDENT", "QUOTE_LITERAL",
    "RADIANS", "RAISE", "RANDOM", "RANGE", "RANGE_MERGE", "RANK", "RD", "REAL", "RECURSIVE", "REFERENCES", "REFRESH",
    "REGEXP_COUNT", "REGEXP_INSTR", "REGEXP_LIKE", "REGEXP_MATCH", "REGEXP_MATCHES", "REGEXP_REPLACE", "REGEXP_SPLIT_TO_ARRAY",
    "REGEXP_SPLIT_TO_TABLE", "REGEXP_SUBSTR", "RENAME", "REPEAT", "REPLACE", "RESET", "RESTART", "RETURN", "RETURNING",
    "RETURNS", "REVERSE", "RIGHT", "ROLE", "ROLLBACK", "ROLLUP", "ROUND", "ROW", "ROWS", "ROW_COUNT", "ROW_NUMBER",
    "ROW_TO_JSON", "RPAD", "RTRIM", "SCHEMA", "SELECT", "SEQUENCE", "SERIAL", "SERIAL2", "SERIAL4", "SERIAL8", "SESSION_USER",
    "SET", "SETS", "SETVAL", "SETWEIGHT", "SET_CONFIG", "SHA224", "SHA256", "SHA384", "SHA512", "SHOW", "SIGN", "SIMILAR",
    "SMALLINT", "SMALLSERIAL", "SOME", "SPLIT_PART", "SPLIT_TO_TABLE", "SQRT", "SS", "SSSS", "SSSSS", "ST", "START",
    "STATEMENT_TIMESTAMP", "STRICT", "STRING", "STRING_AGG", "STRING_TO_ARRAY", "STRIP", "STRPOS", "SUBSTR", "SUBSTRING", "SUM",
    "SYMMETRIC", "TABLE", "TEMP", "TEMPORARY", "TEXT", "TH", "THEN", "TIME", "TIMEOFDAY", "TIMESTAMP", "TIMESTAMPTZ", "TIMETZ",
    "TIMEZONE", "TO", "TO_CHAR", "TO_DATE", "TO_JSON", "TO_JSONB", "TO_NUMBER", "TO_TIMESTAMP", "TO_TSQUERY", "TO_TSVECTOR",
    "TRANSACTION", "TRANSACTION_TIMESTAMP", "TRANSLATE", "TRIM", "TRIM_ARRAY", "TRUE", "TRUNC", "TRUNCATE", "TSMULTIRANGE",
    "TSQUERY", "TSRANGE", "TSTZMULTIRANGE", "TSTZRANGE", "TSVECTOR", "TSVECTOR_TO_ARRAY", "TS_HEADLINE", "TS_RANK", "TS_RANK_CD",
    "TYPE", "TZ", "TZH", "TZM", "UNION", "UNIQUE", "UNKNOWN", "UNLISTEN", "UNLOGGED", "UNNEST", "UPDATE", "UPPER", "UPPER_INC",
    "UPPER_INF", "US", "USER", "USING", "UTC", "UTF8", "UUID", "UUIDV4", "UUIDV7", "UUID_GENERATE_V4", "VALUE", "VALUES",
    "VARBIT", "VARCHAR", "VARYING", "VERSION", "VIEW", "WARNING", "WEBSEARCH_TO_TSQUERY", "WHEN", "WHERE", "WHILE", "WINDOW",
    "WITH", "WITHIN", "WITHOUT", "WW", "XML", "XMLELEMENT", "XMLFOREST", "XPATH", "XX000", "YES", "YY", "YYY", "YYYY", "ZONE",
];

/// `name` (viết hoa, không kèm ngoặc) là hàm engine biết xử lý.
pub(crate) fn is_known_function(name: &str) -> bool {
    KNOWN.binary_search(&name).is_ok()
        || super::fnlib::is_known(name)
        || super::fnlib::is_partial(name)
        || super::strfn::is_known(name)
        || super::srf::is_srf(name, false)
        || super::fnlib::EXTRA_AGGS.contains(&name)
}

thread_local! {
    /// > 0: đang tính DEFAULT của cột — không báo lỗi hàm lạ (DB cũ có thể lưu default dạng chữ không tính được)
    static SUPPRESS: std::cell::Cell<u32> = const { std::cell::Cell::new(0) };
}

/// Chạy `f` mà không báo lỗi hàm không tồn tại.
pub(crate) fn without_check<R>(f: impl FnOnce() -> R) -> R {
    SUPPRESS.with(|s| s.set(s.get() + 1));
    let r = f();
    SUPPRESS.with(|s| s.set(s.get() - 1));
    r
}

/// `t` là đúng một lời gọi `tên(...)` với tên engine không biết → thông điệp lỗi kiểu PostgreSQL
/// (`function f(integer, unknown) does not exist`).
pub(crate) fn unknown_function_error(t: &str) -> Option<String> {
    if SUPPRESS.with(|s| s.get()) > 0 {
        return None;
    }
    let open = t.find('(')?;
    let name = &t[..open];
    if name.is_empty() || !name.chars().next()?.is_ascii_alphabetic() || !name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '.') {
        return None;
    }
    if !t.ends_with(')') || !super::executor::is_fully_enclosed_in_parens(&t[open..]) {
        return None;
    }
    let base = name.rsplit('.').next().unwrap_or(name);
    let up = base.to_ascii_uppercase();
    if is_known_function(&up) || up.starts_with("PG_") && name.contains('.') {
        return None;
    }
    let inner = t[open + 1..t.len() - 1].trim();
    let col = |_: &str| None;
    let types: Vec<String> = if inner.is_empty() {
        vec![]
    } else {
        super::executor::split_function_args(inner).iter().map(|a| super::coltypes::pg_typeof(a, &col).unwrap_or_else(|| "unknown".into())).collect()
    };
    Some(format!("function {}({}) does not exist", base.to_ascii_lowercase(), types.join(", ")))
}
