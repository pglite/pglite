//! Kiểu khai báo của cột (`numeric(6,2)`, `varchar(3)`, `timestamptz`, `int2`...). `DataType` cũ chỉ
//! giữ nhóm kiểu, nên phần chi tiết được lưu riêng bằng `SchemaOp::ColumnTypes` (bảng do bản cũ tạo
//! không có op này → hành vi như cũ). Dùng cho:
//! - ép kiểu khi ghi: làm tròn numeric theo scale, tràn numeric/int2 (22003), varchar/char quá dài (22001);
//! - đường wire (`query_wire`): OID chính xác và định dạng text như PostgreSQL (scale của numeric,
//!   timestamp không có múi giờ, timestamptz quy về UTC, char(n) đệm dấu cách...). Kết quả JS API giữ nguyên.
use crate::types::Value;

#[derive(Debug, Clone, PartialEq)]
pub struct DeclType {
    /// Tên chuẩn: int2, int4, int8, float4, float8, numeric, varchar, bpchar, text, bool, timestamp,
    /// timestamptz, date, time, timetz, interval, json, jsonb, uuid, bytea, money, bit, varbit,
    /// smallserial, serial, bigserial, hoặc tên khác (viết thường) cho kiểu chưa biết.
    pub base: String,
    pub mods: Vec<i64>,
    pub array: bool,
}

const CONSTRAINT_WORDS: &[&str] = &[
    "DEFAULT", "NOT", "NULL", "PRIMARY", "UNIQUE", "REFERENCES", "CHECK", "CONSTRAINT", "GENERATED", "COLLATE", "IDENTITY", "DEFERRABLE",
];

impl DeclType {
    /// Đọc kiểu ở đầu phần định nghĩa cột (sau tên cột) hoặc một tên kiểu trong biểu thức ép kiểu.
    pub fn parse(def: &str) -> Option<DeclType> {
        let s = def.trim();
        if s.is_empty() {
            return None;
        }
        // Gom các từ của tên kiểu (dừng ở từ khoá ràng buộc), kèm (...) và [].
        let mut words: Vec<String> = Vec::new();
        let mut mods_text: Option<String> = None;
        let mut array = false;
        let mut rest = s;
        loop {
            rest = rest.trim_start();
            if rest.is_empty() {
                break;
            }
            if let Some(r) = rest.strip_prefix("[]") {
                array = true;
                rest = r;
                continue;
            }
            if rest.starts_with('[') {
                // int[3]
                match rest.find(']') {
                    Some(e) => {
                        array = true;
                        rest = &rest[e + 1..];
                        continue;
                    }
                    None => break,
                }
            }
            if rest.starts_with('(') {
                if words.is_empty() || mods_text.is_some() && !matches!(words.last().map(|w| w.as_str()), Some("TIMESTAMP" | "TIME")) {
                    break;
                }
                let e = rest.find(')')?;
                if mods_text.is_none() {
                    mods_text = Some(rest[1..e].to_string());
                }
                rest = &rest[e + 1..];
                continue;
            }
            let end = rest.find(|c: char| c.is_whitespace() || c == '(' || c == '[' || c == ',' || c == ')').unwrap_or(rest.len());
            if end == 0 {
                break;
            }
            let w = rest[..end].trim_matches('"').to_uppercase();
            if CONSTRAINT_WORDS.contains(&w.as_str()) || w == "ARRAY" && !words.is_empty() {
                if w == "ARRAY" {
                    array = true;
                    rest = &rest[end..];
                    continue;
                }
                break;
            }
            // chỉ nhận thêm từ khi là phần của tên kiểu nhiều từ
            if !words.is_empty() {
                let joined = words.join(" ");
                let ok = matches!(
                    (joined.as_str(), w.as_str()),
                    ("DOUBLE", "PRECISION")
                        | ("CHARACTER", "VARYING")
                        | ("BIT", "VARYING")
                        | ("TIMESTAMP" | "TIME", "WITH" | "WITHOUT")
                        | ("TIMESTAMP WITH" | "TIMESTAMP WITHOUT" | "TIME WITH" | "TIME WITHOUT", "TIME")
                        | ("TIMESTAMP WITH TIME" | "TIMESTAMP WITHOUT TIME" | "TIME WITH TIME" | "TIME WITHOUT TIME", "ZONE")
                );
                if !ok {
                    break;
                }
            }
            words.push(w);
            rest = &rest[end..];
        }
        if words.is_empty() {
            return None;
        }
        let name = words.join(" ");
        let name = name.rsplit('.').next().unwrap_or(&name).to_string(); // pg_catalog.int4
        let base = match name.as_str() {
            "SMALLINT" | "INT2" => "int2",
            "INT" | "INTEGER" | "INT4" => "int4",
            "BIGINT" | "INT8" => "int8",
            "REAL" | "FLOAT4" => "float4",
            "DOUBLE PRECISION" | "FLOAT8" => "float8",
            "FLOAT" => "float8",
            "NUMERIC" | "DECIMAL" => "numeric",
            "VARCHAR" | "CHARACTER VARYING" => "varchar",
            "CHAR" | "CHARACTER" | "BPCHAR" => "bpchar",
            "TEXT" | "STRING" => "text",
            "BOOL" | "BOOLEAN" => "bool",
            "TIMESTAMP" | "TIMESTAMP WITHOUT TIME ZONE" | "DATETIME" => "timestamp",
            "TIMESTAMPTZ" | "TIMESTAMP WITH TIME ZONE" => "timestamptz",
            "DATE" => "date",
            "TIME" | "TIME WITHOUT TIME ZONE" => "time",
            "TIMETZ" | "TIME WITH TIME ZONE" => "timetz",
            "INTERVAL" => "interval",
            "JSON" => "json",
            "JSONB" => "jsonb",
            "UUID" => "uuid",
            "BYTEA" => "bytea",
            "MONEY" => "money",
            "BIT" => "bit",
            "VARBIT" | "BIT VARYING" => "varbit",
            "SMALLSERIAL" | "SERIAL2" => "smallserial",
            "SERIAL" | "SERIAL4" => "serial",
            "BIGSERIAL" | "SERIAL8" => "bigserial",
            other => return Some(DeclType { base: other.to_lowercase(), mods: vec![], array }),
        }
        .to_string();
        let mut mods: Vec<i64> = match &mods_text {
            Some(m) => m.split(',').filter_map(|x| x.trim().parse::<i64>().ok()).collect(),
            None => vec![],
        };
        // char không có độ dài = char(1); FLOAT(p) chọn float4/float8 theo p như PostgreSQL
        let mut base = base;
        if base == "bpchar" && mods.is_empty() && name != "BPCHAR" {
            mods.push(1);
        }
        if name == "FLOAT" {
            if let Some(&p) = mods.first() {
                base = if p <= 24 { "float4".to_string() } else { "float8".to_string() };
            }
            mods.clear();
        }
        if base == "bit" && mods.is_empty() {
            mods.push(1);
        }
        Some(DeclType { base, mods, array })
    }

    /// Dạng chuẩn để lưu trong catalog: `numeric(6,2)`, `varchar(3)`, `int4[]`.
    pub fn canonical(&self) -> String {
        let mut s = self.base.clone();
        if !self.mods.is_empty() {
            s.push('(');
            s.push_str(&self.mods.iter().map(|m| m.to_string()).collect::<Vec<_>>().join(","));
            s.push(')');
        }
        if self.array {
            s.push_str("[]");
        }
        s
    }

    /// Kiểu khai báo có thêm thông tin so với `DataType` (đáng lưu / đáng định dạng riêng).
    pub fn is_informative(&self) -> bool {
        !matches!(self.base.as_str(), "int4" | "text" | "bool" | "date" | "jsonb" | "uuid" | "bytea" | "serial") || !self.mods.is_empty()
    }

    pub fn scale(&self) -> Option<i64> {
        if self.base == "numeric" {
            return Some(self.mods.get(1).copied().unwrap_or(if self.mods.is_empty() { -1 } else { 0 })).filter(|s| *s >= 0);
        }
        None
    }

    pub fn oid(&self) -> Option<u32> {
        use crate::pg_types::*;
        let o = if self.array {
            match self.base.as_str() {
                "int2" | "smallserial" => OID_INT2_ARRAY,
                "int4" | "serial" => OID_INT4_ARRAY,
                "int8" | "bigserial" => OID_INT8_ARRAY,
                "text" => OID_TEXT_ARRAY,
                "varchar" => 1015,
                "bpchar" => 1014,
                "bool" => OID_BOOL_ARRAY,
                "float4" => 1021,
                "float8" => OID_FLOAT8_ARRAY,
                "numeric" => OID_NUMERIC_ARRAY,
                "uuid" => OID_UUID_ARRAY,
                "jsonb" => OID_JSONB_ARRAY,
                "json" => 199,
                "date" => OID_DATE_ARRAY,
                "timestamp" => OID_TIMESTAMP_ARRAY,
                "timestamptz" => OID_TIMESTAMPTZ_ARRAY,
                _ => return None,
            }
        } else {
            match self.base.as_str() {
                "int2" | "smallserial" => OID_INT2,
                "int4" | "serial" => OID_INT4,
                "int8" | "bigserial" => OID_INT8,
                "float4" => OID_FLOAT4,
                "float8" => OID_FLOAT8,
                "numeric" => OID_NUMERIC,
                "varchar" => OID_VARCHAR,
                "bpchar" => 1042,
                "text" => OID_TEXT,
                "bool" => OID_BOOL,
                "timestamp" => OID_TIMESTAMP,
                "timestamptz" => OID_TIMESTAMPTZ,
                "date" => OID_DATE,
                "time" => OID_TIME,
                "timetz" => 1266,
                "interval" => 1186,
                "json" => OID_JSON,
                "jsonb" => OID_JSONB,
                "uuid" => OID_UUID,
                "bytea" => OID_BYTEA,
                "money" => 790,
                "bit" => 1560,
                "varbit" => 1562,
                "jsonpath" => 4072,
                "point" => 600,
                "box" => 603,
                _ => return None,
            }
        };
        Some(o)
    }

    /// Tên kiểu như `format_type()` / `pg_typeof()` của PostgreSQL (không kèm typmod).
    pub fn sql_name(&self) -> String {
        let n = match self.base.as_str() {
            "int2" | "smallserial" => "smallint",
            "int4" | "serial" => "integer",
            "int8" | "bigserial" => "bigint",
            "float4" => "real",
            "float8" => "double precision",
            "varchar" => "character varying",
            "bpchar" => "character",
            "bool" => "boolean",
            "timestamp" => "timestamp without time zone",
            "timestamptz" => "timestamp with time zone",
            "time" => "time without time zone",
            "timetz" => "time with time zone",
            "varbit" => "bit varying",
            other => other,
        };
        if self.array { format!("{}[]", n) } else { n.to_string() }
    }

    fn type_label(&self) -> String {
        match self.base.as_str() {
            "varchar" => format!("character varying({})", self.mods.first().copied().unwrap_or(0)),
            "bpchar" => format!("character({})", self.mods.first().copied().unwrap_or(1)),
            "bit" => format!("bit({})", self.mods.first().copied().unwrap_or(1)),
            "varbit" => format!("bit varying({})", self.mods.first().copied().unwrap_or(0)),
            other => other.to_string(),
        }
    }
}

/// Làm tròn số thập phân dạng chuỗi tới `scale` chữ số (nửa ra xa số 0, như numeric của PostgreSQL).
pub(crate) fn round_decimal_str(s: &str, scale: usize) -> Option<String> {
    let t = s.trim();
    let (neg, digits) = match t.strip_prefix('-') {
        Some(r) => (true, r),
        None => (false, t.strip_prefix('+').unwrap_or(t)),
    };
    // dạng mũ (1e3): đi qua f64
    if digits.contains(['e', 'E']) {
        let f: f64 = t.parse().ok()?;
        return Some(format!("{:.*}", scale, f));
    }
    let (int_part, frac_part) = match digits.split_once('.') {
        Some((i, f)) => (i, f),
        None => (digits, ""),
    };
    if !int_part.chars().all(|c| c.is_ascii_digit()) || !frac_part.chars().all(|c| c.is_ascii_digit()) || (int_part.is_empty() && frac_part.is_empty()) {
        return None;
    }
    let mut all: Vec<u8> = int_part.bytes().chain(frac_part.bytes()).map(|b| b - b'0').collect();
    let int_len = int_part.len();
    let keep = int_len + scale;
    let round_up = frac_part.len() > scale && all[keep] >= 5;
    all.truncate(keep.min(all.len()));
    while all.len() < keep {
        all.push(0);
    }
    let mut int_len = int_len;
    if round_up {
        let mut i = all.len();
        loop {
            if i == 0 {
                all.insert(0, 1);
                int_len += 1;
                break;
            }
            i -= 1;
            if all[i] == 9 {
                all[i] = 0;
            } else {
                all[i] += 1;
                break;
            }
        }
    }
    let mut ip: String = all[..int_len].iter().map(|d| (b'0' + d) as char).collect();
    let ip_trim = ip.trim_start_matches('0');
    ip = if ip_trim.is_empty() { "0".to_string() } else { ip_trim.to_string() };
    let fp: String = all[int_len..].iter().map(|d| (b'0' + d) as char).collect();
    let is_zero = ip == "0" && fp.chars().all(|c| c == '0');
    let mut out = String::new();
    if neg && !is_zero {
        out.push('-');
    }
    out.push_str(&ip);
    if scale > 0 {
        out.push('.');
        out.push_str(&fp);
    }
    Some(out)
}

fn value_num_str(v: &Value) -> Option<String> {
    match v {
        Value::Int(i) => Some(i.to_string()),
        Value::Float(f) if f.is_finite() => {
            // biểu diễn ngắn nhất của f64 (3.256, không phải 3.25599999...)
            let s = format!("{}", f);
            Some(s)
        }
        Value::Text(t) => {
            let t = t.trim();
            t.parse::<f64>().ok().filter(|f| f.is_finite()).map(|_| t.to_string())
        }
        _ => None,
    }
}

/// Ép giá trị khi ghi vào cột có kiểu khai báo. Lỗi (22001, 22003) đi qua kênh lỗi của executor.
pub(crate) fn coerce_for_write(decl: Option<&DeclType>, v: Value) -> Value {
    let Some(d) = decl else { return v };
    if v.is_null() || d.array {
        return v;
    }
    match d.base.as_str() {
        "numeric" => {
            let Some(s) = value_num_str(&v) else { return v };
            let precision = d.mods.first().copied();
            let scale = d.scale();
            let rounded = match scale {
                Some(sc) => match round_decimal_str(&s, sc as usize) {
                    Some(r) => r,
                    None => return v,
                },
                None => s,
            };
            if let Some(p) = precision {
                let sc = scale.unwrap_or(0);
                let int_digits = rounded.trim_start_matches('-').split('.').next().unwrap_or("").trim_start_matches('0').len() as i64;
                if int_digits > p - sc {
                    crate::engine::errors::raise(format!(
                        "numeric field overflow (A field with precision {}, scale {} must round to an absolute value less than 10^{}.)",
                        p,
                        sc,
                        p - sc
                    ));
                    return v;
                }
            }
            match &v {
                Value::Text(_) => Value::text(rounded),
                _ => match rounded.parse::<i64>() {
                    Ok(i) if !rounded.contains('.') && matches!(v, Value::Int(_)) => Value::Int(i),
                    _ => rounded.parse::<f64>().map(Value::Float).unwrap_or(v),
                },
            }
        }
        "int2" | "smallserial" => {
            if let Value::Int(i) = v {
                if i < i16::MIN as i64 || i > i16::MAX as i64 {
                    crate::engine::errors::raise("smallint out of range");
                }
            }
            v
        }
        "varchar" | "bpchar" | "bit" | "varbit" => {
            let Some(&n) = d.mods.first() else { return v };
            let s = match &v {
                Value::Text(t) => t.to_string(),
                Value::Int(i) => i.to_string(),
                Value::Float(f) => f.to_string(),
                Value::Bool(b) => b.to_string(),
                _ => return v,
            };
            let len = s.chars().count() as i64;
            if d.base == "bit" && len != n {
                crate::engine::errors::raise(format!("bit string length {} does not match type bit({})", len, n));
                return v;
            }
            if len > n {
                // như PostgreSQL: phần thừa toàn dấu cách thì cắt, ngược lại báo lỗi
                let keep: String = s.chars().take(n as usize).collect();
                if d.base != "varbit" && s.chars().skip(n as usize).all(|c| c == ' ') {
                    return Value::text(keep);
                }
                if d.base == "varbit" {
                    crate::engine::errors::raise(format!("bit string too long for type bit varying({})", n));
                } else {
                    crate::engine::errors::raise(format!("value too long for type {}", d.type_label()));
                }
            }
            v
        }
        _ => v,
    }
}

/// Định dạng lại text PostgreSQL của một ô theo kiểu khai báo (chỉ đường wire).
pub(crate) fn format_wire(decl: &DeclType, text: String) -> String {
    if decl.array {
        return text;
    }
    match decl.base.as_str() {
        "numeric" => match decl.scale() {
            Some(sc) => round_decimal_str(&text, sc as usize).unwrap_or(text),
            None => text,
        },
        "bpchar" => {
            let n = decl.mods.first().copied().unwrap_or(1) as usize;
            let len = text.chars().count();
            if len < n {
                format!("{}{}", text, " ".repeat(n - len))
            } else {
                text
            }
        }
        "timestamp" => crate::engine::temporal::wire_timestamp(&text, false).unwrap_or(text),
        "timestamptz" => crate::engine::temporal::wire_timestamp(&text, true).unwrap_or(text),
        "time" | "timetz" => crate::engine::temporal::wire_time(&text, decl.base == "timetz").unwrap_or(text),
        "money" => match text.trim().parse::<f64>() {
            Ok(f) => format_money(f),
            Err(_) => text,
        },
        // số nguyên đã qua phép tính số thực (`8.0` từ sum/min): in như số nguyên
        "int2" | "int4" | "int8" | "serial" | "bigserial" | "smallserial" if text.contains('.') => match text.trim().parse::<f64>() {
            Ok(f) if f.fract() == 0.0 && f.abs() < 9.0e15 => format!("{}", f as i64),
            _ => text,
        },
        "float4" => match text.trim().parse::<f32>() {
            Ok(f) if f.is_finite() => format!("{}", f),
            _ => text,
        },
        _ => text,
    }
}

fn format_money(f: f64) -> String {
    let neg = f < 0.0;
    let s = format!("{:.2}", f.abs());
    let (ip, fp) = s.split_once('.').unwrap_or((&s, "00"));
    let mut grouped = String::new();
    for (i, c) in ip.chars().enumerate() {
        if i > 0 && (ip.len() - i) % 3 == 0 {
            grouped.push(',');
        }
        grouped.push(c);
    }
    format!("{}${}.{}", if neg { "-" } else { "" }, grouped, fp)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn p(s: &str) -> String {
        DeclType::parse(s).unwrap().canonical()
    }

    #[test]
    fn parse_types() {
        assert_eq!(p("numeric(6,2) NOT NULL"), "numeric(6,2)");
        assert_eq!(p("DECIMAL"), "numeric");
        assert_eq!(p("character varying(30) DEFAULT 'x'"), "varchar(30)");
        assert_eq!(p("char"), "bpchar(1)");
        assert_eq!(p("double precision"), "float8");
        assert_eq!(p("timestamp(3) with time zone"), "timestamptz(3)");
        assert_eq!(p("TIMESTAMP WITHOUT TIME ZONE DEFAULT now()"), "timestamp");
        assert_eq!(p("int[] NOT NULL"), "int4[]");
        assert_eq!(p("text ARRAY"), "text[]");
        assert_eq!(p("BIGSERIAL PRIMARY KEY"), "bigserial");
        assert_eq!(p("float(10)"), "float4");
        assert_eq!(p("\"char\""), "bpchar(1)");
    }

    #[test]
    fn rounding() {
        assert_eq!(round_decimal_str("3.256", 2).unwrap(), "3.26");
        assert_eq!(round_decimal_str("1", 2).unwrap(), "1.00");
        assert_eq!(round_decimal_str("-2.5", 0).unwrap(), "-3");
        assert_eq!(round_decimal_str("9.995", 2).unwrap(), "10.00");
        assert_eq!(round_decimal_str("-0.001", 2).unwrap(), "0.00");
        assert_eq!(format_money(1234.5), "$1,234.50");
    }

    #[test]
    fn write_coercion() {
        crate::engine::errors::clear();
        let d = DeclType::parse("numeric(6,2)").unwrap();
        assert_eq!(coerce_for_write(Some(&d), Value::Float(3.256)), Value::Float(3.26));
        assert!(crate::engine::errors::take().is_none());
        coerce_for_write(Some(&d), Value::Float(12345.0));
        assert!(crate::engine::errors::take().unwrap().starts_with("numeric field overflow"));
        let v = DeclType::parse("varchar(3)").unwrap();
        coerce_for_write(Some(&v), Value::text("abcd"));
        assert_eq!(crate::engine::errors::take().unwrap(), "value too long for type character varying(3)");
        assert_eq!(coerce_for_write(Some(&v), Value::text("ab  ")), Value::text("ab "));
    }
}

/// Kiểu khai báo của từng cột kết quả một câu SELECT (để định dạng wire như PostgreSQL). Chỉ suy khi
/// chắc chắn: mục là tham chiếu cột (`col`, `t.col`, `*`, `t.*`), ép kiểu (`x::numeric(5,2)`,
/// `CAST(x AS ...)`) hoặc literal có kiểu (`TIMESTAMP '...'`). Không khớp số cột thì trả rỗng.
pub(crate) fn wire_decls(sql: &str, storage: &crate::storage::engine::StorageEngine, field_count: usize) -> Vec<Option<DeclType>> {
    use crate::engine::executor::{find_top_level_keyword, is_fully_enclosed_in_parens, split_comma_separated_tokens};
    let Ok(lexed) = crate::engine::frontend::lex(sql) else { return vec![] };
    let mut s = lexed.trim().trim_end_matches(';').trim();
    if s.len() < 7 || !s.as_bytes()[..7].eq_ignore_ascii_case(b"SELECT ") || s.contains(';') {
        return vec![];
    }
    for kw in ["UNION", "INTERSECT", "EXCEPT"] {
        if let Some(p) = find_top_level_keyword(s, kw) {
            s = &s[..p];
        }
    }
    let after = s[7..].trim_start();
    let after = match after.get(..9) {
        Some(p) if p.eq_ignore_ascii_case("DISTINCT ") => {
            let r = after[9..].trim_start();
            if r.len() >= 3 && r.as_bytes()[..3].eq_ignore_ascii_case(b"ON ") || r.len() >= 3 && r.as_bytes()[..3].eq_ignore_ascii_case(b"ON(") {
                return vec![];
            }
            r
        }
        _ => after.strip_prefix("ALL ").unwrap_or(after),
    };
    let (list, rest) = match find_top_level_keyword(after, "FROM") {
        Some(p) => (&after[..p], &after[p + 4..]),
        None => (after, ""),
    };
    // Bảng trong FROM (kèm JOIN): (bảng, alias)
    let mut tables: Vec<(&crate::storage::table::Table, Option<String>)> = Vec::new();
    if !rest.is_empty() {
        let mut end = rest.len();
        for kw in ["WHERE", "GROUP", "ORDER", "LIMIT", "OFFSET", "HAVING", "WINDOW", "FOR", "FETCH"] {
            if let Some(p) = find_top_level_keyword(rest, kw) {
                end = end.min(p);
            }
        }
        let from = &rest[..end];
        let join_words = ["JOIN", "LEFT", "RIGHT", "FULL", "INNER", "OUTER", "CROSS", "NATURAL", "LATERAL"];
        let mut piece = String::new();
        let mut pieces: Vec<String> = Vec::new();
        let mut depth = 0i32;
        for tok in from.split_whitespace() {
            depth += tok.matches('(').count() as i32 - tok.matches(')').count() as i32;
            let up = tok.to_uppercase();
            if depth == 0 && join_words.contains(&up.as_str()) {
                if !piece.trim().is_empty() {
                    pieces.push(std::mem::take(&mut piece));
                }
                continue;
            }
            piece.push_str(tok);
            piece.push(' ');
        }
        if !piece.trim().is_empty() {
            pieces.push(piece);
        }
        for p in pieces {
            for sub in split_comma_separated_tokens(&p) {
                let words: Vec<&str> = sub.split_whitespace().collect();
                let Some(name) = words.first() else { continue };
                if name.starts_with('(') {
                    return vec![]; // subquery / VALUES: không suy
                }
                let short = name.trim_matches('"').rsplit('.').next().unwrap_or(name).trim_matches('"');
                let alias = match words.get(1).map(|w| w.to_uppercase()) {
                    Some(w) if w == "AS" => words.get(2).map(|a| a.trim_matches('"').to_string()),
                    Some(w) if w == "ON" || w == "USING" => None,
                    Some(_) => Some(words[1].trim_matches('"').to_string()),
                    None => None,
                };
                match storage.get_table(short) {
                    Some(t) => tables.push((t, alias)),
                    None => return vec![],
                }
            }
        }
    }
    let col_decl = |e: &str| -> Option<Option<DeclType>> {
        let e = e.trim();
        let (q, c) = match e.rsplit_once('.') {
            Some((q, c)) => (Some(q.trim_matches('"')), c.trim_matches('"')),
            None => (None, e.trim_matches('"')),
        };
        if c.is_empty() || !c.chars().all(|ch| ch.is_alphanumeric() || ch == '_' || ch == '$') {
            return None;
        }
        for (t, alias) in &tables {
            if let Some(q) = q {
                let qs = q.rsplit('.').next().unwrap_or(q);
                if !qs.eq_ignore_ascii_case(&t.name) && !alias.as_deref().is_some_and(|a| a.eq_ignore_ascii_case(qs)) {
                    continue;
                }
            }
            if let Some(i) = t.get_column_index(c) {
                return Some(t.decl_type(i).cloned().or_else(|| implied_decl(&t.columns[i].data_type)));
            }
        }
        None
    };
    let mut out: Vec<Option<DeclType>> = Vec::new();
    for item in split_comma_separated_tokens(list) {
        let it = item.trim();
        if it == "*" {
            for (t, _) in &tables {
                out.extend((0..t.columns.len()).map(|i| t.decl_type(i).cloned()));
            }
            continue;
        }
        if let Some(prefix) = it.strip_suffix(".*") {
            let p = prefix.trim().trim_matches('"');
            match tables.iter().find(|(t, a)| t.name.eq_ignore_ascii_case(p) || a.as_deref().is_some_and(|a| a.eq_ignore_ascii_case(p))) {
                Some((t, _)) => out.extend((0..t.columns.len()).map(|i| t.decl_type(i).cloned())),
                None => return vec![],
            }
            continue;
        }
        let mut e = match find_top_level_keyword(it, "AS") {
            Some(p) => it[..p].trim(),
            None => it,
        };
        while e.starts_with('(') && e.ends_with(')') && is_fully_enclosed_in_parens(e) {
            e = e[1..e.len() - 1].trim();
        }
        // INTERVAL '...' đứng riêng: engine đã trả đúng kiểu và định dạng
        let decl = expr_decl(e, &col_decl).filter(|d| d.base != "interval" || !e.as_bytes()[..e.len().min(8)].eq_ignore_ascii_case(b"INTERVAL"));
        out.push(decl);
    }
    if out.len() != field_count || out.iter().all(|d| d.is_none()) {
        return vec![];
    }
    out
}

/// Kiểu khai báo của một biểu thức trong SELECT (đường wire): cột, ép kiểu, literal có kiểu, và một số
/// dạng suy ra từ chúng (`ts + interval`, `date_trunc(u, ts)`). `col` trả `Some(kiểu?)` khi `e` là cột.
fn expr_decl(e: &str, col: &dyn Fn(&str) -> Option<Option<DeclType>>) -> Option<DeclType> {
    use crate::engine::executor::{find_top_level_keyword, is_fully_enclosed_in_parens};
    let mut e = e.trim();
    while e.starts_with('(') && e.ends_with(')') && is_fully_enclosed_in_parens(e) {
        e = e[1..e.len() - 1].trim();
    }
    if let Some(d) = col(e) {
        return d;
    }
    // so sánh / logic ở cấp ngoài cùng: kết quả boolean (`::` phía sau chỉ thuộc một vế)
    let upper_e = e.to_ascii_uppercase();
    if upper_e.starts_with("SELECT") || upper_e.starts_with("WITH") || upper_e.starts_with("VALUES") {
        return None; // subquery vô hướng: kiểu là của cột nó trả
    }
    if ["AND", "OR", "NOT", "IS", "LIKE", "ILIKE", "IN", "BETWEEN", "EXISTS"].iter().any(|k| find_top_level_keyword(&upper_e, k).is_some_and(|p| p > 0 || *k == "NOT" || *k == "EXISTS"))
        || ["=", "<", ">", "<>", "!=", "~", "~*", "!~", "!~*", "@>", "<@", "?"].iter().any(|op| crate::engine::executor::find_top_level_op(e, op).is_some_and(|p| p > 0))
    {
        if !(upper_e.starts_with("CASE") && upper_e.ends_with("END")) {
            return DeclType::parse("bool");
        }
    }
    let is_json = |d: &Option<DeclType>| d.as_ref().is_some_and(|d| !d.array && (d.base == "jsonb" || d.base == "json"));
    // jsonb: a || b, a - k, a #- p giữ kiểu jsonb; a -> k, a #> p trả jsonb; ->>, #>> trả text
    for op in ["||", "#-"] {
        if let Some(p) = crate::engine::executor::find_top_level_op(e, op) {
            let (dl, dr) = (expr_decl(&e[..p], col), expr_decl(&e[p + op.len()..], col));
            if is_json(&dl) || is_json(&dr) && op == "||" {
                return DeclType::parse("jsonb");
            }
            return None;
        }
    }
    for op in ["->", "#>"] {
        if let Some(p) = find_top_level_plain(e, op) {
            if e[p + 2..].starts_with('>') {
                return DeclType::parse("text");
            }
            return if is_json(&expr_decl(&e[..p], col)) { DeclType::parse("jsonb") } else { None };
        }
    }
    // ts ± interval, date ± interval → timestamp; date ± int → date
    if let Some((p, _)) = crate::engine::executor::find_top_level_math_op(e, &['+', '-']) {
        let (l, r) = (e[..p].trim(), e[p + 1..].trim());
        if l.is_empty() {
            return None;
        }
        let (dl, dr) = (expr_decl(l, col), expr_decl(r, col));
        let base = |d: &Option<DeclType>| d.as_ref().filter(|d| !d.array).map(|d| d.base.clone()).unwrap_or_default();
        let (bl, br) = (base(&dl), base(&dr));
        let temporal = |b: &str| matches!(b, "timestamp" | "timestamptz");
        return match (bl.as_str(), br.as_str()) {
            (a, "interval") if temporal(a) => dl,
            ("interval", b) if temporal(b) => dr,
            ("date", "interval") | ("interval", "date") => DeclType::parse("timestamp"),
            ("interval", "interval") => DeclType::parse("interval"),
            ("jsonb", "text" | "") if e.as_bytes()[p] == b'-' => DeclType::parse("jsonb"),
            (a, b) => numeric_result(a, b),
        };
    }
    if let Some((p, _)) = crate::engine::executor::find_top_level_math_op(e, &['*', '/', '%']) {
        let (l, r) = (e[..p].trim(), e[p + 1..].trim());
        if l.is_empty() {
            return None;
        }
        let base = |d: Option<DeclType>| d.filter(|d| !d.array).map(|d| d.base).unwrap_or_default();
        return numeric_result(&base(expr_decl(l, col)), &base(expr_decl(r, col)));
    }
    if let Some(p) = crate::engine::executor::find_last_top_level_op(e, "::") {
        return DeclType::parse(&e[p + 2..]);
    }
    if e.len() > 5 && e.as_bytes()[..5].eq_ignore_ascii_case(b"CAST(") && e.ends_with(')') && is_fully_enclosed_in_parens(&e[4..]) {
        let inner = &e[5..e.len() - 1];
        return find_top_level_keyword(inner, "AS").and_then(|p| DeclType::parse(&inner[p + 2..]));
    }
    // timezone(z, x): timestamptz → timestamp và ngược lại
    if e.len() > 9 && e.as_bytes()[..9].eq_ignore_ascii_case(b"TIMEZONE(") && e.ends_with(')') && is_fully_enclosed_in_parens(&e[8..]) {
        let args = crate::engine::executor::split_comma_separated_tokens(&e[9..e.len() - 1]);
        let naive = matches!(expr_decl(args.get(1)?, col).map(|d| d.base), Some(b) if b == "timestamp" || b == "date");
        return DeclType::parse(if naive { "timestamptz" } else { "timestamp" });
    }
    // aggregate: count → int8; sum(int) → int8, sum(numeric) → numeric; min/max giữ kiểu; avg → numeric
    if let Some(open) = e.find('(') {
        let fname = e[..open].trim().to_ascii_uppercase();
        if fname == "GROUPING" && e.ends_with(')') && is_fully_enclosed_in_parens(&e[open..]) {
            return DeclType::parse("int4");
        }
        if matches!(fname.as_str(), "COUNT" | "SUM" | "MIN" | "MAX" | "AVG") && e.ends_with(')') && is_fully_enclosed_in_parens(&e[open..]) {
            let inner = e[open + 1..e.len() - 1].trim();
            let inner = inner.strip_prefix("DISTINCT ").or_else(|| inner.strip_prefix("distinct ")).unwrap_or(inner);
            let arg = || expr_decl(inner, col).filter(|d| !d.array);
            return match fname.as_str() {
                "COUNT" => DeclType::parse("int8"),
                "AVG" => match arg().map(|d| d.base) {
                    Some(b) if b == "float4" || b == "float8" => DeclType::parse("float8"),
                    _ => DeclType::parse("numeric"),
                },
                "SUM" => match arg().map(|d| d.base) {
                    Some(b) if matches!(b.as_str(), "int2" | "int4" | "serial" | "smallserial") => DeclType::parse("int8"),
                    Some(b) if b == "int8" || b == "bigserial" => DeclType::parse("numeric"),
                    // sum(numeric(p,s)) giữ scale s
                    Some(b) if b == "numeric" => arg(),
                    Some(b) if b == "float4" || b == "float8" || b == "money" || b == "interval" => DeclType::parse(&b),
                    _ => None,
                },
                _ => arg(),
            };
        }
    }
    if e.len() > 10 && e.as_bytes()[..10].eq_ignore_ascii_case(b"DATE_TRUNC") && e.ends_with(')') {
        let open = e.find('(')?;
        if is_fully_enclosed_in_parens(&e[open..]) {
            let args = crate::engine::executor::split_comma_separated_tokens(&e[open + 1..e.len() - 1]);
            let d = expr_decl(args.get(1)?, col)?;
            return match d.base.as_str() {
                "timestamp" | "timestamptz" | "interval" => Some(DeclType { mods: vec![], ..d }),
                "date" => DeclType::parse("timestamptz"),
                _ => None,
            };
        }
    }
    if let Some(q) = e.find('\'') {
        // literal có kiểu: TIMESTAMP '...', DATE '...', INTERVAL '1 day'
        let head = e[..q].trim();
        if !head.is_empty() && head.chars().all(|c| c.is_alphabetic() || c == ' ') && e.ends_with('\'') {
            return DeclType::parse(head);
        }
    }
    None
}

/// Kiểu kết quả phép tính số học giữa hai kiểu khai báo (rỗng = chưa biết): numeric thắng số nguyên,
/// float8 thắng tất cả; chỉ trả khi một vế đã biết là numeric/float (phần số nguyên do engine lo).
fn numeric_result(a: &str, b: &str) -> Option<DeclType> {
    let rank = |t: &str| match t {
        "int2" | "int4" | "int8" | "serial" | "bigserial" | "smallserial" => Some(1),
        "numeric" | "money" => Some(2),
        "float4" | "float8" => Some(3),
        _ => None,
    };
    let (ra, rb) = (rank(a), rank(b));
    let top = match (ra, rb) {
        (Some(x), Some(y)) => x.max(y),
        (Some(x), None) | (None, Some(x)) if x >= 2 => x,
        _ => return None,
    };
    match top {
        2 => DeclType::parse("numeric"),
        3 => DeclType::parse("float8"),
        _ => None,
    }
}

/// Kiểu khai báo suy từ `DataType` khi bảng không lưu riêng (kiểu không cần thêm thông tin: INT, TEXT,
/// JSONB...). Timestamp không suy được (bản cũ lưu chung timestamp/timestamptz).
fn implied_decl(dt: &crate::types::DataType) -> Option<DeclType> {
    use crate::types::DataType as D;
    let name = match dt {
        D::Integer => "int4",
        D::BigInt => "int8",
        D::Text => "text",
        D::Boolean => "bool",
        D::Numeric => "numeric",
        D::Date => "date",
        D::Jsonb => "jsonb",
        D::Uuid => "uuid",
        D::Bytea => "bytea",
        _ => return None,
    };
    DeclType::parse(name)
}

/// Vị trí cuối cùng của `op` ở cấp ngoài cùng (ngoài chuỗi và ngoặc), không có xử lý đặc biệt nào.
fn find_top_level_plain(s: &str, op: &str) -> Option<usize> {
    let b = s.as_bytes();
    let (mut depth, mut in_str, mut last) = (0i32, false, None);
    for i in 0..b.len() {
        match b[i] {
            b'\'' => in_str = !in_str,
            b'(' | b'[' if !in_str => depth += 1,
            b')' | b']' if !in_str => depth -= 1,
            _ if !in_str && depth == 0 && b[i..].starts_with(op.as_bytes()) => last = Some(i),
            _ => {}
        }
    }
    last
}

/// Kiểu khai báo của cột thứ `idx` (lưu riêng, hoặc suy từ `DataType`; timestamp cũ coi là timestamptz
/// như OID engine vẫn báo).
pub(crate) fn column_decl(t: &crate::storage::table::Table, idx: usize) -> Option<DeclType> {
    t.decl_type(idx).cloned().or_else(|| match &t.columns.get(idx)?.data_type {
        crate::types::DataType::Timestamp => DeclType::parse("timestamptz"),
        crate::types::DataType::Serial => DeclType::parse("int4"),
        crate::types::DataType::Time => DeclType::parse("time"),
        crate::types::DataType::Array(e) => DeclType::parse(&format!("{}[]", e.trim_matches('"'))),
        dt => implied_decl(dt),
    })
}

/// `pg_typeof(expr)` suy tĩnh từ biểu thức; `col` trả kiểu của một tham chiếu cột. `None` khi không
/// suy được (nơi gọi dùng kiểu của giá trị lúc chạy).
pub(crate) fn pg_typeof(e: &str, col: &dyn Fn(&str) -> Option<DeclType>) -> Option<String> {
    use crate::engine::executor::is_fully_enclosed_in_parens;
    let mut e = e.trim();
    while e.starts_with('(') && e.ends_with(')') && is_fully_enclosed_in_parens(e) {
        e = e[1..e.len() - 1].trim();
    }
    if e.is_empty() {
        return None;
    }
    let up = e.to_ascii_uppercase();
    if e.bytes().all(|b| b.is_ascii_digit()) {
        return Some(match e.parse::<i64>() {
            Ok(v) if i32::try_from(v).is_ok() => "integer",
            Ok(_) => "bigint",
            Err(_) => "numeric",
        }
        .into());
    }
    if e.bytes().all(|b| b.is_ascii_digit() || b == b'.') || up.bytes().all(|b| b.is_ascii_digit() || b == b'.' || b == b'E' || b == b'-' || b == b'+') && up.contains('E') && up.as_bytes()[0].is_ascii_digit() {
        return Some("numeric".into());
    }
    match up.as_str() {
        "TRUE" | "FALSE" => return Some("boolean".into()),
        "NULL" => return Some("unknown".into()),
        "CURRENT_TIMESTAMP" | "NOW()" | "CLOCK_TIMESTAMP()" | "STATEMENT_TIMESTAMP()" | "TRANSACTION_TIMESTAMP()" => {
            return Some("timestamp with time zone".into())
        }
        "LOCALTIMESTAMP" => return Some("timestamp without time zone".into()),
        "CURRENT_DATE" => return Some("date".into()),
        "CURRENT_TIME" => return Some("time with time zone".into()),
        "LOCALTIME" => return Some("time without time zone".into()),
        _ => {}
    }
    if e.starts_with('\'') && e.ends_with('\'') && e.len() >= 2 && !e[1..e.len() - 1].contains('\'') {
        return Some("unknown".into());
    }
    let colk = |c: &str| col(c).and_then(|d| crate::engine::inttype::IntKind::from_type_name(&d.canonical()));
    if let Some(k) = crate::engine::inttype::int_kind(e, &colk) {
        return Some(match k {
            crate::engine::inttype::IntKind::I2 => "smallint",
            crate::engine::inttype::IntKind::I4 => "integer",
            crate::engine::inttype::IntKind::I8 => "bigint",
        }
        .into());
    }
    if up.starts_with("ARRAY[") && e.ends_with(']') {
        let items = crate::engine::executor::split_comma_separated_tokens(&e[6..e.len() - 1]);
        let first = pg_typeof(items.first()?, col)?;
        return Some(if first == "unknown" { "text[]".into() } else { format!("{}[]", first) });
    }
    let colref = |c: &str| col(c);
    if let Some(d) = expr_decl(e, &|c: &str| {
        let ok = !c.is_empty() && c.chars().all(|ch| ch.is_alphanumeric() || ch == '_' || ch == '.' || ch == '"');
        if ok { colref(c).map(Some) } else { None }
    }) {
        return Some(d.sql_name());
    }
    if let Some(open) = e.find('(') {
        if e.ends_with(')') && is_fully_enclosed_in_parens(&e[open..]) {
            let name = up[..open].trim();
            let r = match name {
                "COUNT" => "bigint",
                "LENGTH" | "CHAR_LENGTH" | "OCTET_LENGTH" | "STRPOS" | "POSITION" | "ASCII" | "ARRAY_LENGTH" | "CARDINALITY" => "integer",
                "AVG" => "numeric",
                "LOWER" | "UPPER" | "CONCAT" | "CONCAT_WS" | "SUBSTRING" | "SUBSTR" | "TRIM" | "REPLACE" | "MD5" | "INITCAP" | "LEFT"
                | "RIGHT" | "LPAD" | "RPAD" | "REPEAT" | "REVERSE" | "SPLIT_PART" | "TO_CHAR" | "FORMAT" | "ENCODE" | "CHR"
                | "STRING_AGG" | "VERSION" | "ARRAY_TO_STRING" => "text",
                "SHA256" | "SHA224" | "SHA384" | "SHA512" | "DECODE" => "bytea",
                "AGE" => "interval",
                "TO_TIMESTAMP" => "timestamp with time zone",
                "TO_DATE" => "date",
                "GEN_RANDOM_UUID" | "UUIDV4" | "UUIDV7" => "uuid",
                "JSONB_BUILD_OBJECT" | "JSONB_BUILD_ARRAY" | "TO_JSONB" | "JSONB_AGG" | "JSONB_SET" | "JSONB_OBJECT_AGG" => "jsonb",
                "JSON_BUILD_OBJECT" | "JSON_BUILD_ARRAY" | "TO_JSON" | "JSON_AGG" | "JSON_OBJECT_AGG" => "json",
                "STRING_TO_ARRAY" => "text[]",
                "PG_TYPEOF" => "regtype",
                "IS_NORMALIZED" => "boolean",
                "SUM" => {
                    let inner = &e[open + 1..e.len() - 1];
                    return Some(match pg_typeof(inner, col).as_deref() {
                        Some("integer" | "smallint") => "bigint",
                        Some("real") => "real",
                        Some("double precision") => "double precision",
                        _ => "numeric",
                    }
                    .into());
                }
                "MIN" | "MAX" | "COALESCE" | "NULLIF" | "GREATEST" | "LEAST" | "ABS" | "ROUND" | "TRUNC" | "FLOOR" | "CEIL" => {
                    let args = crate::engine::executor::split_comma_separated_tokens(&e[open + 1..e.len() - 1]);
                    let t = pg_typeof(args.first()?, col)?;
                    return Some(if matches!(name, "ROUND" | "TRUNC" | "FLOOR" | "CEIL") && t != "double precision" && t != "real" { "numeric".into() } else { t });
                }
                _ => return None,
            };
            return Some(r.into());
        }
    }
    // so sánh / logic
    for op in [" AND ", " OR ", " IS ", " LIKE ", " ILIKE ", " IN ", " BETWEEN "] {
        if crate::engine::executor::find_top_level_keyword(e, op.trim()).is_some() {
            return Some("boolean".into());
        }
    }
    for op in ["<=", ">=", "<>", "!=", "=", "<", ">"] {
        if crate::engine::executor::find_top_level_op(e, op).is_some_and(|p| p > 0) {
            return Some("boolean".into());
        }
    }
    None
}
