//! `COPY bảng [(cột, ...)] FROM {STDIN | 'tệp'} [[WITH] (tuỳ chọn)]` và `COPY {bảng [(cột)] | (truy vấn)} TO {STDOUT |
//! 'tệp'} [[WITH] (tuỳ chọn)]`, định dạng `text` (mặc định: tab, `\N`, thoát `\`) và `csv` (`HEADER`, `DELIMITER`,
//! `NULL`, `QUOTE`, `ESCAPE`, `FORCE_QUOTE`). Cú pháp cũ `WITH CSV HEADER DELIMITER ','` cũng được nhận.
//!
//! Dữ liệu của STDIN được đưa vào bằng API (`PGlite::copy_from`, `copyFrom` của bản Node); kết quả của STDOUT
//! được lấy bằng `PGlite::copy_to` / `copyTo`. Khi gọi bằng SQL thường, `COPY ... TO STDOUT` trả 0 dòng (như
//! `@electric-sql/pglite`). Dòng được ghi bằng câu INSERT nhiều dòng thường (ràng buộc, trigger, WAL như INSERT).
use super::executor::{split_comma_separated_tokens, Executor};
use crate::types::{QueryResult, Value};

#[derive(Debug, Clone)]
struct CopyOpts {
    csv: bool,
    delim: char,
    null: String,
    header: bool,
    quote: char,
    escape: char,
    force_quote_all: bool,
    force_quote: Vec<String>,
}

impl Default for CopyOpts {
    fn default() -> Self {
        Self { csv: false, delim: '\t', null: "\\N".into(), header: false, quote: '"', escape: '"', force_quote_all: false, force_quote: vec![] }
    }
}

fn unquote_opt(v: &str) -> String {
    let t = v.trim();
    if t.len() >= 2 && t.starts_with('\'') && t.ends_with('\'') {
        let inner = t[1..t.len() - 1].replace("''", "'");
        // E'\t'
        return inner;
    }
    if t.len() >= 3 && (t.starts_with("E'") || t.starts_with("e'")) && t.ends_with('\'') {
        return t[2..t.len() - 1].replace("\\t", "\t").replace("\\n", "\n").replace("\\\\", "\\");
    }
    t.to_string()
}

fn parse_opts(s: &str) -> Result<CopyOpts, String> {
    let mut o = CopyOpts::default();
    let mut t = s.trim();
    if t.len() >= 4 && t[..4].eq_ignore_ascii_case("WITH") {
        t = t[4..].trim_start();
    }
    let set_csv = |o: &mut CopyOpts, on: bool| {
        if on && !o.csv {
            o.csv = true;
            o.delim = ',';
            o.null = String::new();
        }
    };
    if t.starts_with('(') {
        let c = super::constraints::matching_paren(t, 0).ok_or("syntax error in COPY options")?;
        for item in split_comma_separated_tokens(&t[1..c]) {
            let item = item.trim();
            let (k, v) = match item.find(char::is_whitespace) {
                Some(p) => (item[..p].to_ascii_uppercase(), item[p..].trim().to_string()),
                None => (item.to_ascii_uppercase(), String::new()),
            };
            let flag = !matches!(v.to_ascii_uppercase().as_str(), "FALSE" | "OFF" | "0");
            match k.as_str() {
                "FORMAT" => {
                    let f = unquote_opt(&v).to_ascii_lowercase();
                    match f.as_str() {
                        "csv" => set_csv(&mut o, true),
                        "text" => {}
                        "binary" => return Err("COPY format \"binary\" is not supported".into()),
                        other => return Err(format!("COPY format \"{}\" not recognized", other)),
                    }
                }
                "DELIMITER" => o.delim = unquote_opt(&v).chars().next().ok_or("COPY delimiter must be a single one-byte character")?,
                "NULL" => o.null = unquote_opt(&v),
                "HEADER" => o.header = flag || v.is_empty(),
                "QUOTE" => o.quote = unquote_opt(&v).chars().next().unwrap_or('"'),
                "ESCAPE" => o.escape = unquote_opt(&v).chars().next().unwrap_or('"'),
                "FORCE_QUOTE" => {
                    if v.trim() == "*" {
                        o.force_quote_all = true;
                    } else {
                        let inner = v.trim().trim_start_matches('(').trim_end_matches(')');
                        o.force_quote = split_comma_separated_tokens(inner).into_iter().map(|c| c.trim().trim_matches('"').to_lowercase()).collect();
                    }
                }
                "ENCODING" | "FREEZE" | "FORCE_NOT_NULL" | "FORCE_NULL" | "ON_ERROR" | "LOG_VERBOSITY" | "DEFAULT" => {}
                other => return Err(format!("option \"{}\" not recognized", other.to_lowercase())),
            }
        }
        return Ok(o);
    }
    // cú pháp cũ: [BINARY] [DELIMITER 'x'] [NULL 'x'] [CSV [HEADER] [QUOTE 'x'] [ESCAPE 'x'] [FORCE QUOTE ...]]
    let words: Vec<String> = t.split_whitespace().map(|w| w.to_string()).collect();
    let mut i = 0;
    while i < words.len() {
        let w = words[i].to_ascii_uppercase();
        match w.as_str() {
            "CSV" => set_csv(&mut o, true),
            "HEADER" => o.header = true,
            "BINARY" => return Err("COPY format \"binary\" is not supported".into()),
            "DELIMITER" | "NULL" | "QUOTE" | "ESCAPE" if i + 1 < words.len() => {
                i += 1;
                let v = unquote_opt(&words[i]);
                match w.as_str() {
                    "DELIMITER" => o.delim = v.chars().next().unwrap_or('\t'),
                    "NULL" => o.null = v,
                    "QUOTE" => o.quote = v.chars().next().unwrap_or('"'),
                    _ => o.escape = v.chars().next().unwrap_or('"'),
                }
            }
            "AS" => {}
            "FORCE" if words.get(i + 1).is_some_and(|x| x.eq_ignore_ascii_case("QUOTE")) => {
                i += 2;
                if words.get(i).map(|x| x.as_str()) == Some("*") {
                    o.force_quote_all = true;
                } else {
                    while i < words.len() {
                        o.force_quote.push(words[i].trim_end_matches(',').trim_matches('"').to_lowercase());
                        if !words[i].ends_with(',') {
                            break;
                        }
                        i += 1;
                    }
                }
            }
            _ => {}
        }
        i += 1;
    }
    Ok(o)
}

/// Tách dữ liệu text thành các dòng giá trị (`None` = NULL).
fn parse_text(data: &str, o: &CopyOpts) -> Result<Vec<Vec<Option<String>>>, String> {
    let mut rows = Vec::new();
    for (ln, line) in data.split('\n').enumerate() {
        let line = line.strip_suffix('\r').unwrap_or(line);
        if line == "\\." {
            break;
        }
        if line.is_empty() && ln + 1 == data.split('\n').count() {
            continue;
        }
        if o.header && ln == 0 {
            continue;
        }
        let mut fields = Vec::new();
        for raw in line.split(o.delim) {
            if raw == o.null {
                fields.push(None);
                continue;
            }
            let mut s = String::with_capacity(raw.len());
            let mut it = raw.chars().peekable();
            while let Some(c) = it.next() {
                if c == '\\' {
                    match it.next() {
                        Some('t') => s.push('\t'),
                        Some('n') => s.push('\n'),
                        Some('r') => s.push('\r'),
                        Some('b') => s.push('\u{8}'),
                        Some('f') => s.push('\u{c}'),
                        Some('v') => s.push('\u{b}'),
                        Some('N') => {}
                        Some(x) => s.push(x),
                        None => {}
                    }
                } else {
                    s.push(c);
                }
            }
            fields.push(Some(s));
        }
        rows.push(fields);
    }
    Ok(rows)
}

/// Tách dữ liệu CSV (giá trị có thể chứa xuống dòng trong nháy).
fn parse_csv(data: &str, o: &CopyOpts) -> Result<Vec<Vec<Option<String>>>, String> {
    let mut rows = Vec::new();
    let mut row: Vec<Option<String>> = Vec::new();
    let mut field = String::new();
    let mut quoted = false;
    let mut in_quotes = false;
    let chars: Vec<char> = data.chars().collect();
    let mut i = 0;
    let push_field = |row: &mut Vec<Option<String>>, field: &mut String, quoted: &mut bool| {
        let v = std::mem::take(field);
        // NULL chỉ khi khớp chuỗi NULL và không được bọc nháy
        if !*quoted && v == o.null {
            row.push(None);
        } else {
            row.push(Some(v));
        }
        *quoted = false;
    };
    while i < chars.len() {
        let c = chars[i];
        if in_quotes {
            if c == o.escape && o.escape != o.quote && chars.get(i + 1) == Some(&o.quote) {
                field.push(o.quote);
                i += 2;
                continue;
            }
            if c == o.quote {
                if chars.get(i + 1) == Some(&o.quote) && o.escape == o.quote {
                    field.push(o.quote);
                    i += 2;
                    continue;
                }
                in_quotes = false;
                i += 1;
                continue;
            }
            field.push(c);
            i += 1;
            continue;
        }
        if c == o.quote {
            in_quotes = true;
            quoted = true;
        } else if c == o.delim {
            push_field(&mut row, &mut field, &mut quoted);
        } else if c == '\n' || c == '\r' {
            if c == '\r' && chars.get(i + 1) == Some(&'\n') {
                i += 1;
            }
            push_field(&mut row, &mut field, &mut quoted);
            if !(row.len() == 1 && row[0].as_deref() == Some("\\.")) {
                rows.push(std::mem::take(&mut row));
            } else {
                row.clear();
                break;
            }
        } else {
            field.push(c);
        }
        i += 1;
    }
    if in_quotes {
        return Err("unterminated CSV quoted field".into());
    }
    if !field.is_empty() || quoted || !row.is_empty() {
        push_field(&mut row, &mut field, &mut quoted);
        rows.push(row);
    }
    if o.header && !rows.is_empty() {
        rows.remove(0);
    }
    Ok(rows)
}

fn sql_lit(v: &Option<String>) -> String {
    match v {
        None => "NULL".into(),
        Some(s) => format!("'{}'", s.replace('\'', "''")),
    }
}

impl Executor {
    /// `COPY ... FROM STDIN` với dữ liệu `data` (text/CSV theo tuỳ chọn của câu lệnh). Trả số dòng đã ghi.
    pub fn copy_from(&mut self, sql: &str, data: &str, params: &[Value]) -> Result<usize, String> {
        self.session.copy_in = Some(data.to_string());
        let r = self.execute(sql, params);
        self.session.copy_in = None;
        Ok(r?.row_count)
    }

    /// `COPY ... TO STDOUT`: trả dữ liệu đã định dạng.
    pub fn copy_to(&mut self, sql: &str, params: &[Value]) -> Result<String, String> {
        self.session.copy_out = None;
        self.execute(sql, params)?;
        Ok(self.session.copy_out.take().unwrap_or_default())
    }

    pub(crate) fn handle_copy(&mut self, sql: &str, params: &[Value]) -> Result<QueryResult, String> {
        let t = sql.trim().trim_end_matches(';').trim()[4..].trim_start();
        // nguồn: (truy vấn) | bảng [(cột, ...)]
        let (source, rest) = if t.starts_with('(') {
            let c = super::constraints::matching_paren(t, 0).ok_or("syntax error at or near \"(\"")?;
            (t[..=c].to_string(), t[c + 1..].trim_start())
        } else {
            let mut end = t.find(|c: char| c.is_whitespace() || c == '(').unwrap_or(t.len());
            let after = t[end..].trim_start();
            if after.starts_with('(') {
                let off = t.len() - after.len();
                let c = super::constraints::matching_paren(t, off).ok_or("syntax error in COPY column list")?;
                end = c + 1;
            }
            (t[..end].to_string(), t[end..].trim_start())
        };
        let ru = rest.to_ascii_uppercase();
        let from = ru.starts_with("FROM ");
        if !from && !ru.starts_with("TO ") {
            return Err("syntax error: expected FROM or TO in COPY".into());
        }
        let after_dir = rest[if from { 5 } else { 3 }..].trim_start();
        let (target, opts_src) = if after_dir.to_ascii_uppercase().starts_with("STDIN") || after_dir.to_ascii_uppercase().starts_with("STDOUT") {
            let w = if after_dir.to_ascii_uppercase().starts_with("STDIN") { 5 } else { 6 };
            (None, &after_dir[w..])
        } else if after_dir.starts_with('\'') {
            let end = after_dir[1..].find('\'').map(|e| e + 2).ok_or("unterminated file name in COPY")?;
            (Some(after_dir[1..end - 1].to_string()), &after_dir[end..])
        } else if after_dir.to_ascii_uppercase().starts_with("PROGRAM") {
            return Err("COPY ... PROGRAM is not supported".into());
        } else {
            return Err("syntax error: expected STDIN, STDOUT or a file name in COPY".into());
        };
        let o = parse_opts(opts_src)?;
        if from {
            if source.starts_with('(') {
                return Err("COPY FROM does not support a query as source".into());
            }
            let data = match target {
                Some(path) => std::fs::read_to_string(&path).map_err(|e| format!("could not open file \"{}\" for reading: {}", path, e))?,
                None => self.session.copy_in.take().ok_or("COPY FROM STDIN needs data: use copyFrom(sql, data) / PGlite::copy_from")?,
            };
            return self.copy_rows_in(&source, &data, &o, params);
        }
        // TO
        let query = if source.starts_with('(') {
            source[1..source.len() - 1].to_string()
        } else {
            match source.find('(') {
                Some(p) => format!("SELECT {} FROM {}", &source[p + 1..source.len() - 1], &source[..p]),
                None => format!("SELECT * FROM {}", source),
            }
        };
        let w = self.execute_wire(&query, params)?;
        let mut out = String::new();
        if o.header {
            let names: Vec<String> = w.fields.iter().map(|f| self.copy_field(Some(&f.name), &f.name, &o)).collect();
            out.push_str(&names.join(&o.delim.to_string()));
            out.push('\n');
        }
        for row in &w.rows {
            let cells: Vec<String> = row.iter().enumerate().map(|(i, v)| self.copy_field(v.as_deref(), &w.fields[i].name, &o)).collect();
            out.push_str(&cells.join(&o.delim.to_string()));
            out.push('\n');
        }
        let n = w.rows.len();
        match target {
            Some(path) => std::fs::write(&path, out).map_err(|e| format!("could not open file \"{}\" for writing: {}", path, e))?,
            None => self.session.copy_out = Some(out),
        }
        Ok(QueryResult { rows: vec![], row_count: n, fields: vec![], command: "COPY".to_string() })
    }

    /// Một ô của COPY TO theo định dạng.
    fn copy_field(&self, v: Option<&str>, col: &str, o: &CopyOpts) -> String {
        let Some(s) = v else { return o.null.clone() };
        if o.csv {
            let force = o.force_quote_all || o.force_quote.iter().any(|c| c.eq_ignore_ascii_case(col));
            let needs = force || s.is_empty() && o.null.is_empty() || s.contains(o.delim) || s.contains(o.quote) || s.contains('\n') || s.contains('\r') || s == o.null;
            if needs {
                let q = o.quote.to_string();
                let esc = format!("{}{}", o.escape, o.quote);
                return format!("{}{}{}", q, s.replace(&q, &esc), q);
            }
            return s.to_string();
        }
        let mut r = String::with_capacity(s.len());
        for c in s.chars() {
            match c {
                '\\' => r.push_str("\\\\"),
                '\n' => r.push_str("\\n"),
                '\r' => r.push_str("\\r"),
                '\t' => r.push_str("\\t"),
                c if c == o.delim => {
                    r.push('\\');
                    r.push(c);
                }
                c => r.push(c),
            }
        }
        r
    }

    /// Ghi dữ liệu COPY FROM bằng INSERT nhiều dòng (mỗi lô 1000 dòng).
    fn copy_rows_in(&mut self, source: &str, data: &str, o: &CopyOpts, params: &[Value]) -> Result<QueryResult, String> {
        let (table, cols) = match source.find('(') {
            Some(p) => (source[..p].trim().to_string(), Some(source[p + 1..source.len() - 1].to_string())),
            None => (source.trim().to_string(), None),
        };
        let clean = super::executor::strip_schema_keep_case(&table);
        let ncols = match &cols {
            Some(c) => split_comma_separated_tokens(c).len(),
            None => self.storage.get_table(&clean).ok_or_else(|| format!("relation \"{}\" does not exist", clean))?.columns.len(),
        };
        let rows = if o.csv { parse_csv(data, o)? } else { parse_text(data, o)? };
        let col_sql = cols.as_ref().map(|c| format!(" ({})", c)).unwrap_or_default();
        let mut total = 0usize;
        for (k, chunk) in rows.chunks(1000).enumerate() {
            let mut groups = Vec::with_capacity(chunk.len());
            for (j, r) in chunk.iter().enumerate() {
                if r.len() != ncols {
                    let line = k * 1000 + j + 1 + usize::from(o.header);
                    return Err(format!("{} data for column (COPY {}, line {})", if r.len() < ncols { "missing" } else { "extra" }, clean, line));
                }
                groups.push(format!("({})", r.iter().map(sql_lit).collect::<Vec<_>>().join(", ")));
            }
            let q = format!("INSERT INTO {}{} VALUES {}", table, col_sql, groups.join(", "));
            let res = self.execute_statement(&q, params)?;
            total += res.row_count;
        }
        Ok(QueryResult { rows: vec![], row_count: total, fields: vec![], command: "COPY".to_string() })
    }
}
