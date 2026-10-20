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
                _ => return None,
            }
        };
        Some(o)
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
    if s.len() < 7 || !s[..7].eq_ignore_ascii_case("SELECT ") || s.contains(';') {
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
            if r.len() >= 3 && r[..3].eq_ignore_ascii_case("ON ") || r.len() >= 3 && r[..3].eq_ignore_ascii_case("ON(") {
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
                return Some(t.decl_type(i).cloned());
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
        let decl = if let Some(d) = col_decl(e) {
            d
        } else if let Some(p) = crate::engine::executor::find_last_top_level_op(e, "::") {
            DeclType::parse(&e[p + 2..])
        } else if e.len() > 5 && e[..5].eq_ignore_ascii_case("CAST(") && e.ends_with(')') && is_fully_enclosed_in_parens(&e[4..]) {
            let inner = &e[5..e.len() - 1];
            find_top_level_keyword(inner, "AS").and_then(|p| DeclType::parse(&inner[p + 2..]))
        } else if let Some(q) = e.find('\'') {
            // literal có kiểu: TIMESTAMP '...', DATE '...'
            let head = e[..q].trim();
            if !head.is_empty() && head.chars().all(|c| c.is_alphabetic() || c == ' ') && e.ends_with('\'') {
                DeclType::parse(head).filter(|d| !matches!(d.base.as_str(), "interval"))
            } else {
                None
            }
        } else {
            None
        };
        out.push(decl);
    }
    if out.len() != field_count || out.iter().all(|d| d.is_none()) {
        return vec![];
    }
    out
}
