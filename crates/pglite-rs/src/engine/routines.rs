//! Hàm và thủ tục do người dùng định nghĩa: `CREATE [OR REPLACE] FUNCTION / PROCEDURE`, `DROP FUNCTION /
//! PROCEDURE`, `CALL`, và lời gọi hàm trong câu lệnh.
//!
//! Lưu bằng `SchemaOp::ObjectMeta { kind: "routine", name: chữ ký, value: JSON }` (bản cũ bỏ qua). Thân
//! `LANGUAGE sql` chạy từng câu với tham số đã thay bằng hằng (câu cuối là kết quả); `LANGUAGE plpgsql` dùng bộ
//! thông dịch của `plpgsql.rs` (tham số là biến, `RETURN`, `RETURN NEXT / QUERY`, `NEW` / `OLD` của trigger).
//!
//! Lời gọi `f(...)` trong một câu lệnh được xử lý trước khi chạy câu đó:
//! - hàm SQL chỉ gồm `SELECT <biểu thức>` (không FROM): thay thẳng bằng biểu thức (inline như PostgreSQL), dùng
//!   được ở mọi chỗ, kể cả với đối số là cột;
//! - đối số là hằng: tính một lần, thay bằng giá trị (hàm trả tập trong FROM thành bảng tạm);
//! - đối số là cột, trong danh sách SELECT: chạy truy vấn với các đối số làm cột ẩn rồi gọi hàm cho từng dòng;
//! - còn lại (cột trong WHERE với hàm không inline được...): lỗi `0A000` rõ ràng.
use super::executor::{find_top_level_keyword, json_to_value, split_comma_separated_tokens, split_function_args, Executor};
use crate::types::{DataType, FieldInfo, QueryResult, Value};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub(crate) struct RoutineArg {
    pub name: Option<String>,
    pub ty: String,
    pub default: Option<String>,
    /// `in` | `out` | `inout` | `variadic`
    pub mode: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub(crate) struct RoutineDef {
    pub name: String,
    pub procedure: bool,
    pub args: Vec<RoutineArg>,
    /// kiểu trả về (viết thường): `int`, `trigger`, `void`, `record`, tên bảng...; `table` khi `RETURNS TABLE`
    pub returns: String,
    pub returns_set: bool,
    /// cột của `RETURNS TABLE (...)`
    pub table_cols: Vec<(String, String)>,
    pub language: String,
    pub body: String,
    /// `STRICT` / `RETURNS NULL ON NULL INPUT`
    pub strict: bool,
}

impl RoutineDef {
    fn inputs(&self) -> impl Iterator<Item = &RoutineArg> {
        self.args.iter().filter(|a| a.mode != "out")
    }
    fn outputs(&self) -> Vec<(String, String)> {
        let mut v: Vec<(String, String)> = self.args.iter().filter(|a| a.mode == "out" || a.mode == "inout").map(|a| (a.name.clone().unwrap_or_default(), a.ty.clone())).collect();
        v.extend(self.table_cols.iter().cloned());
        v
    }
    pub(crate) fn signature(&self) -> String {
        format!("{}({})", self.name.to_lowercase(), self.inputs().map(|a| canon_type(&a.ty)).collect::<Vec<_>>().join(","))
    }
}

/// Kết quả một lần gọi.
pub(crate) struct RoutineOut {
    pub scalar: Value,
    /// hàm trả tập / TABLE: các dòng (object JSON) và tên cột
    pub rows: Vec<serde_json::Value>,
    pub cols: Vec<(String, String)>,
    /// bản ghi trả về (trigger: NEW đã sửa; `None` trong `Some` = RETURN NULL)
    pub record: Option<Option<serde_json::Map<String, serde_json::Value>>>,
}

/// Ngữ cảnh của trigger khi gọi hàm trigger.
pub(crate) struct TriggerCall {
    pub new: Option<serde_json::Map<String, serde_json::Value>>,
    pub old: Option<serde_json::Map<String, serde_json::Value>>,
    pub op: String,
    pub name: String,
    pub table: String,
    pub when: String,
    pub level: String,
    pub argv: Vec<String>,
    /// bản ghi toàn NULL (cột của bảng) cho NEW / OLD không có
    pub null_record: serde_json::Map<String, serde_json::Value>,
}

fn canon_type(ty: &str) -> String {
    let t = ty.trim().to_lowercase();
    match super::coltypes::DeclType::parse(&t) {
        Some(d) => {
            let c = d.canonical();
            // bỏ độ dài / độ chính xác: varchar(10) và varchar cùng chữ ký
            let base = c.split('(').next().unwrap_or(&c).to_string();
            if d.array && !base.ends_with("[]") { format!("{}[]", base) } else { base }
        }
        None => t,
    }
}

/// Tách định nghĩa một tham số: `[IN|OUT|INOUT|VARIADIC] [tên] kiểu [DEFAULT biểu_thức | = biểu_thức]`.
fn parse_arg(s: &str) -> Option<RoutineArg> {
    let mut t = s.trim();
    if t.is_empty() {
        return None;
    }
    let mut mode = "in".to_string();
    for m in ["INOUT ", "OUT ", "IN ", "VARIADIC "] {
        if t.len() > m.len() && t[..m.len()].eq_ignore_ascii_case(m) {
            mode = m.trim().to_lowercase();
            t = t[m.len()..].trim_start();
            break;
        }
    }
    let (decl, default) = match find_top_level_keyword(t, "DEFAULT") {
        Some(p) => (t[..p].trim(), Some(t[p + 7..].trim().to_string())),
        None => match super::executor::find_top_level_op(t, "=") {
            Some(p) if !t[..p].ends_with(['<', '>', '!', ':']) => (t[..p].trim(), Some(t[p + 1..].trim().to_string())),
            _ => (t, None),
        },
    };
    let words: Vec<&str> = decl.split_whitespace().collect();
    let type_starts = ["double", "character", "timestamp", "time", "bit", "national", "interval"];
    let (name, ty) = if words.len() >= 2 && !type_starts.contains(&words[0].to_lowercase().as_str()) {
        (Some(words[0].trim_matches('"').to_lowercase()), words[1..].join(" "))
    } else {
        (None, decl.to_string())
    };
    Some(RoutineArg { name, ty: ty.to_lowercase(), default, mode })
}

/// `$tag$ ... $tag$` hoặc `'...'` bắt đầu tại `i` → (nội dung, vị trí sau literal).
fn string_body_at(s: &str, i: usize) -> Option<(String, usize)> {
    let b = s.as_bytes();
    match b.get(i)? {
        b'$' => {
            let end = super::executor::dollar_quote_end(s, i)?;
            let tag_len = s[i + 1..].find('$')? + 2;
            Some((s[i + tag_len..end - tag_len].to_string(), end))
        }
        b'\'' => {
            let mut j = i + 1;
            let mut out = String::new();
            while j < b.len() {
                if b[j] == b'\'' {
                    if b.get(j + 1) == Some(&b'\'') {
                        out.push('\'');
                        j += 2;
                        continue;
                    }
                    return Some((out, j + 1));
                }
                let ch = s[j..].chars().next()?;
                out.push(ch);
                j += ch.len_utf8();
            }
            None
        }
        _ => None,
    }
}

/// Đọc `CREATE [OR REPLACE] {FUNCTION|PROCEDURE} ...`.
pub(crate) fn parse_create_routine(sql: &str) -> Result<(RoutineDef, bool), String> {
    let t = sql.trim().trim_end_matches(';').trim();
    let up = t.to_ascii_uppercase();
    let mut pos = 6; // CREATE
    let rest = up[pos..].trim_start();
    pos += up[pos..].len() - rest.len();
    let or_replace = rest.starts_with("OR REPLACE ");
    if or_replace {
        pos += 11;
        pos += up[pos..].len() - up[pos..].trim_start().len();
    }
    let procedure = up[pos..].starts_with("PROCEDURE");
    pos += if procedure { 9 } else { 8 };
    let open = t[pos..].find('(').map(|o| pos + o).ok_or("syntax error: expected ( after function name")?;
    let name = super::executor::strip_schema_keep_case(t[pos..open].trim()).trim_matches('"').to_lowercase();
    let close = super::constraints::matching_paren(t, open).ok_or("syntax error: unbalanced parentheses in argument list")?;
    let args: Vec<RoutineArg> = split_function_args(&t[open + 1..close]).iter().filter_map(|a| parse_arg(a)).collect();
    // phần sau danh sách tham số: các mệnh đề và thân
    let tail = &t[close + 1..];
    // tách thân (AS '...' | AS $$...$$ | RETURN biểu_thức) khỏi mệnh đề
    let mut clauses = String::new();
    let mut body: Option<String> = None;
    let tb = tail.as_bytes();
    let mut i = 0;
    let mut last_kw_start = 0;
    while i < tb.len() {
        let c = tb[i];
        if c == b'$' || c == b'\'' {
            // literal ngoài AS (vd. SET search_path = '...'): giữ nguyên trong mệnh đề
            if let Some((lit, end)) = string_body_at(tail, i) {
                let before = tail[last_kw_start..i].trim_end().to_ascii_uppercase();
                if before.ends_with(" AS") || before == "AS" {
                    clauses.push_str(&tail[last_kw_start..i - 0].trim_end()[..tail[last_kw_start..i].trim_end().len() - 2]);
                    clauses.push(' ');
                    body = Some(lit);
                    i = end;
                    // `AS 'obj', 'link'` của C: bỏ phần sau
                    let after = tail[i..].trim_start();
                    if let Some(a) = after.strip_prefix(',') {
                        if let Some((_, e2)) = string_body_at(tail, i + (tail[i..].len() - a.trim_start().len())) {
                            i = e2;
                        }
                    }
                    last_kw_start = i;
                    continue;
                }
                i = end;
                continue;
            }
        }
        i += 1;
    }
    clauses.push_str(&tail[last_kw_start..]);
    let cu = clauses.to_ascii_uppercase();
    if body.is_none() {
        // SQL chuẩn: `RETURN biểu_thức` hoặc `BEGIN ATOMIC ... END`
        if let Some(p) = find_top_level_keyword(&clauses, "RETURN") {
            body = Some(format!("SELECT {}", clauses[p + 6..].trim()));
            clauses.truncate(p);
        } else if let Some(p) = find_top_level_keyword(&clauses, "BEGIN") {
            let inner = clauses[p + 5..].trim();
            let inner = inner.strip_prefix("ATOMIC").or_else(|| inner.strip_prefix("atomic")).unwrap_or(inner).trim();
            let inner = inner.strip_suffix("END").or_else(|| inner.strip_suffix("end")).unwrap_or(inner).trim();
            body = Some(inner.to_string());
            clauses.truncate(p);
        }
    }
    let body = body.ok_or("syntax error: function body (AS $$ ... $$) is missing")?;
    let clauses_kw = ["LANGUAGE", "IMMUTABLE", "STABLE", "VOLATILE", "STRICT", "CALLED", "SECURITY", "PARALLEL", "COST", "ROWS", "SET", "LEAKPROOF", "NOT", "WINDOW", "TRANSFORM", "SUPPORT", "EXTERNAL"];
    let language = match find_top_level_keyword(&clauses, "LANGUAGE") {
        Some(p) => clauses[p + 8..].trim_start().split(|c: char| c.is_whitespace() || c == ';').next().unwrap_or("sql").trim_matches(|c| c == '\'' || c == '"').to_lowercase(),
        None => "sql".into(),
    };
    if !matches!(language.as_str(), "sql" | "plpgsql") {
        return Err(format!("language \"{}\" does not exist", language));
    }
    let strict = cu.contains("STRICT") || cu.contains("RETURNS NULL ON NULL INPUT");
    let (mut returns, mut returns_set, mut table_cols) = ("void".to_string(), false, Vec::new());
    if let Some(p) = find_top_level_keyword(&clauses, "RETURNS").filter(|&p| !cu[p..].starts_with("RETURNS NULL ON NULL INPUT")) {
        let r = clauses[p + 7..].trim_start();
        let end = clauses_kw.iter().filter_map(|k| find_top_level_keyword(r, k)).filter(|&q| q > 0).min().unwrap_or(r.len());
        let r = r[..end].trim();
        let ru = r.to_ascii_uppercase();
        if ru.starts_with("TABLE") {
            let o = r.find('(').ok_or("syntax error in RETURNS TABLE")?;
            let c = super::constraints::matching_paren(r, o).ok_or("syntax error in RETURNS TABLE")?;
            for col in split_function_args(&r[o + 1..c]) {
                let mut it = col.trim().splitn(2, char::is_whitespace);
                let n = it.next().unwrap_or("").trim_matches('"').to_lowercase();
                table_cols.push((n, it.next().unwrap_or("text").trim().to_lowercase()));
            }
            returns = "table".into();
            returns_set = true;
        } else if let Some(s) = ru.strip_prefix("SETOF ") {
            let _ = s;
            returns = r[6..].trim().to_lowercase();
            returns_set = true;
        } else {
            returns = r.to_lowercase();
        }
    } else if !procedure && !args.iter().any(|a| a.mode == "out" || a.mode == "inout") {
        return Err("function result type must be specified".into());
    }
    if procedure {
        returns = "void".into();
    }
    Ok((RoutineDef { name, procedure, args, returns, returns_set, table_cols, language, body, strict }, or_replace))
}

/// Danh sách đối số của một lời gọi, kèm tên (`f(a => 1, b := 2)`).
fn split_call_args(s: &str) -> Vec<(Option<String>, String)> {
    split_function_args(s)
        .into_iter()
        .filter(|a| !a.trim().is_empty())
        .map(|a| {
            for op in ["=>", ":="] {
                if let Some(p) = super::executor::find_top_level_op(&a, op) {
                    let n = a[..p].trim();
                    if !n.is_empty() && n.chars().all(|c| c.is_alphanumeric() || c == '_' || c == '"') {
                        return (Some(n.trim_matches('"').to_lowercase()), a[p + 2..].trim().to_string());
                    }
                }
            }
            (None, a.trim().to_string())
        })
        .collect()
}

fn literal(v: &Value) -> String {
    match v {
        Value::Null => "NULL".into(),
        Value::Bool(b) => if *b { "TRUE" } else { "FALSE" }.into(),
        Value::Int(i) => if *i < 0 { format!("({})", i) } else { i.to_string() },
        Value::Float(f) => if *f < 0.0 { format!("({})", f) } else { format!("{}", f) },
        Value::Text(t) => format!("'{}'", t.replace('\'', "''")),
    }
}

/// Literal có kiểu (`'2024-01-01'::date`) để giá trị giữ kiểu của tham số khi thay vào câu lệnh.
fn typed_literal(v: &Value, ty: &str) -> String {
    let t = ty.trim().to_lowercase();
    if v.is_null() || t.is_empty() || matches!(t.as_str(), "anyelement" | "any" | "record") {
        return literal(v);
    }
    match v {
        Value::Text(_) => format!("{}::{}", literal(v), t),
        _ => literal(v),
    }
}

fn json_scalar(v: &serde_json::Value) -> Value {
    match v {
        serde_json::Value::Object(_) | serde_json::Value::Array(_) => Value::text(v.to_string()),
        other => json_to_value(other),
    }
}

impl Executor {
    /// Routine đã lưu có tên `name` (viết thường, không schema).
    pub(crate) fn routines_named(&self, name: &str) -> Vec<RoutineDef> {
        let prefix = format!("{}(", name);
        self.storage
            .objects
            .extra
            .range(("routine".to_string(), prefix.clone())..)
            .take_while(|((k, n), _)| k == "routine" && n.starts_with(&prefix))
            .filter_map(|(_, v)| serde_json::from_str::<RoutineDef>(v).ok())
            .collect()
    }

    /// Tên các routine được gọi trong `sql`.
    pub(crate) fn routine_calls_in(&self, sql: &str) -> Vec<String> {
        self.find_routine_calls(sql).into_iter().map(|c| c.3).collect()
    }

    pub(crate) fn has_routines(&self) -> bool {
        self.storage.objects.extra.range(("routine".to_string(), String::new())..).next().is_some_and(|((k, _), _)| k == "routine")
    }

    /// `CREATE FUNCTION / PROCEDURE`, `DROP FUNCTION / PROCEDURE / ROUTINE`, `CALL`. `None` = không phải lệnh này.
    pub(crate) fn handle_routine_ddl(&mut self, sql: &str, upper: &str, params: &[Value]) -> Option<Result<QueryResult, String>> {
        let w: Vec<&str> = upper.split_whitespace().take(4).collect();
        let done = |cmd: &str| Ok(QueryResult { rows: vec![], row_count: 0, fields: vec![], command: cmd.to_string() });
        match w.as_slice() {
            ["CREATE", "FUNCTION" | "PROCEDURE", ..] | ["CREATE", "OR", "REPLACE", "FUNCTION" | "PROCEDURE"] => {
                let r = (|| {
                    let (def, or_replace) = parse_create_routine(sql)?;
                    let sig = def.signature();
                    let exists = self.storage.objects.extra.contains_key(&("routine".to_string(), sig.clone()));
                    if exists && !or_replace {
                        return Err(format!("function \"{}\" already exists with same argument types", def.name));
                    }
                    if def.language == "plpgsql" {
                        // kiểm tra cú pháp ngay như PostgreSQL
                        self.plpgsql_check(&def.body)?;
                    }
                    let json = serde_json::to_string(&def).map_err(|e| e.to_string())?;
                    self.storage.log_schema_op(crate::storage::catalog::SchemaOp::ObjectMeta { kind: "routine".into(), name: sig, value: Some(json) });
                    self.plan_cache.clear();
                    done(if def.procedure { "CREATE PROCEDURE" } else { "CREATE FUNCTION" })
                })();
                Some(r)
            }
            ["DROP", "FUNCTION" | "PROCEDURE" | "ROUTINE", ..] => Some(self.drop_routines(sql)),
            ["CALL", ..] => Some(self.call_procedure(sql, params)),
            _ => None,
        }
    }

    fn drop_routines(&mut self, sql: &str) -> Result<QueryResult, String> {
        let t = sql.trim().trim_end_matches(';').trim();
        let mut rest = t[4..].trim_start();
        let kw_len = rest.find(char::is_whitespace).unwrap_or(rest.len());
        let kind = rest[..kw_len].to_ascii_uppercase();
        rest = rest[kw_len..].trim_start();
        let if_exists = rest.to_ascii_uppercase().starts_with("IF EXISTS ");
        if if_exists {
            rest = rest[10..].trim_start();
        }
        for suf in [" CASCADE", " RESTRICT"] {
            if rest.to_ascii_uppercase().ends_with(suf) {
                rest = &rest[..rest.len() - suf.len()];
            }
        }
        for item in split_comma_separated_tokens(rest) {
            let item = item.trim();
            let (name, types) = match item.find('(') {
                Some(o) => {
                    let c = super::constraints::matching_paren(item, o).unwrap_or(item.len() - 1);
                    let types: Vec<String> = split_function_args(&item[o + 1..c]).iter().filter_map(|a| parse_arg(a)).filter(|a| a.mode != "out").map(|a| canon_type(&a.ty)).collect();
                    (super::executor::strip_schema_keep_case(item[..o].trim()).trim_matches('"').to_lowercase(), Some(types))
                }
                None => (super::executor::strip_schema_keep_case(item).trim_matches('"').to_lowercase(), None),
            };
            let candidates = self.routines_named(&name);
            let targets: Vec<RoutineDef> = candidates
                .into_iter()
                .filter(|d| types.as_ref().map_or(true, |ts| d.inputs().map(|a| canon_type(&a.ty)).collect::<Vec<_>>() == *ts))
                .filter(|d| kind == "ROUTINE" || d.procedure == (kind == "PROCEDURE"))
                .collect();
            if targets.is_empty() {
                if if_exists {
                    continue;
                }
                let what = if kind == "PROCEDURE" { "procedure" } else { "function" };
                return Err(match types {
                    Some(ts) => format!("{} {}({}) does not exist", what, name, ts.join(", ")),
                    None => format!("{} {}() does not exist", what, name),
                });
            }
            if types.is_none() && targets.len() > 1 {
                return Err(format!("function name \"{}\" is not unique", name));
            }
            for d in targets {
                self.storage.log_schema_op(crate::storage::catalog::SchemaOp::ObjectMeta { kind: "routine".into(), name: d.signature(), value: None });
            }
        }
        self.plan_cache.clear();
        Ok(QueryResult { rows: vec![], row_count: 0, fields: vec![], command: format!("DROP {}", kind) })
    }

    /// Chọn routine theo tên và đối số (số lượng, tên, giá trị mặc định); trả về đối số đã sắp theo vị trí.
    fn resolve_routine(&self, name: &str, call_args: &[(Option<String>, String)], want_procedure: Option<bool>) -> Result<(RoutineDef, Vec<Option<String>>), String> {
        let cands = self.routines_named(name);
        let mut best: Option<(RoutineDef, Vec<Option<String>>)> = None;
        for d in cands {
            if want_procedure.is_some_and(|p| p != d.procedure) {
                continue;
            }
            let inputs: Vec<&RoutineArg> = d.inputs().collect();
            let mut slots: Vec<Option<String>> = vec![None; inputs.len()];
            let mut ok = true;
            let mut positional = 0;
            for (n, e) in call_args {
                match n {
                    Some(n) => match inputs.iter().position(|a| a.name.as_deref() == Some(n.as_str())) {
                        Some(i) => slots[i] = Some(e.clone()),
                        None => ok = false,
                    },
                    None => {
                        if positional < slots.len() {
                            slots[positional] = Some(e.clone());
                        } else if inputs.last().is_some_and(|a| a.mode == "variadic") {
                            let last = slots.len() - 1;
                            let prev = slots[last].take().unwrap_or_default();
                            slots[last] = Some(if prev.is_empty() { e.clone() } else { format!("{}, {}", prev, e) });
                        } else {
                            ok = false;
                        }
                        positional += 1;
                    }
                }
            }
            if !ok {
                continue;
            }
            // tham số còn thiếu phải có DEFAULT
            for (i, a) in inputs.iter().enumerate() {
                if slots[i].is_none() {
                    match &a.default {
                        Some(dft) => slots[i] = Some(dft.clone()),
                        None => ok = false,
                    }
                }
            }
            if !ok {
                continue;
            }
            // VARIADIC: gom thành mảng
            if let Some(a) = inputs.last().filter(|a| a.mode == "variadic") {
                let last = slots.len() - 1;
                if let Some(v) = &slots[last] {
                    if !v.trim_start().to_ascii_uppercase().starts_with("VARIADIC ") {
                        slots[last] = Some(format!("ARRAY[{}]", v));
                    } else {
                        slots[last] = Some(v.trim_start()[9..].to_string());
                    }
                }
                let _ = a;
            }
            if best.is_none() {
                best = Some((d, slots));
            }
        }
        if best.is_none() && want_procedure == Some(false) && self.routines_named(name).iter().any(|d| d.procedure) {
            return Err(format!("{}() is a procedure", name));
        }
        best.ok_or_else(|| {
            let what = if want_procedure == Some(true) { "procedure" } else { "function" };
            format!("{} {}({}) does not exist", what, name, call_args.iter().map(|(_, e)| super::coltypes::pg_typeof(e, &|_| None).unwrap_or_else(|| "unknown".into())).collect::<Vec<_>>().join(", "))
        })
    }

    /// Giá trị các biểu thức đối số (không tham chiếu cột).
    fn eval_args(&mut self, exprs: &[Option<String>], params: &[Value]) -> Result<Vec<Value>, String> {
        let mut out = Vec::with_capacity(exprs.len());
        for e in exprs {
            let e = e.as_deref().unwrap_or("NULL");
            let r = self.execute_statement(&format!("SELECT ({}) AS __a", e), params)?;
            if let Some(err) = crate::engine::errors::take() {
                return Err(err);
            }
            let v = r.rows.first().and_then(|r| r.get("__a")).map(json_scalar).unwrap_or(Value::Null);
            // định danh không tính được (cột của bảng ngoài) quay về nguyên văn: không phải hằng
            if let Value::Text(t) = &v {
                let et = e.trim();
                if !et.starts_with('\'') && (t.as_str() == et || t.as_str() == et.trim_matches('"')) {
                    return Err(format!("column reference in argument: {}", et));
                }
            }
            out.push(v);
        }
        Ok(out)
    }

    /// `CALL p(...)`: thủ tục; tham số INOUT / OUT trả về một dòng như PostgreSQL.
    fn call_procedure(&mut self, sql: &str, params: &[Value]) -> Result<QueryResult, String> {
        let t = sql.trim().trim_end_matches(';').trim()[4..].trim();
        let open = t.find('(').ok_or("syntax error at or near \"CALL\"")?;
        let close = super::constraints::matching_paren(t, open).ok_or("syntax error: unbalanced parentheses")?;
        let name = super::executor::strip_schema_keep_case(t[..open].trim()).trim_matches('"').to_lowercase();
        let call_args = split_call_args(&t[open + 1..close]);
        let (def, exprs) = self.resolve_routine(&name, &call_args, Some(true))?;
        let args = self.eval_args(&exprs, params)?;
        let out = self.call_routine(&def, &args, None)?;
        let outs = def.outputs();
        if outs.is_empty() {
            return Ok(QueryResult { rows: vec![], row_count: 0, fields: vec![], command: "CALL".to_string() });
        }
        let rec = out.record.flatten().unwrap_or_default();
        let fields = outs.iter().map(|(n, ty)| FieldInfo { name: n.clone(), data_type: DataType::from_sql_str(ty).field_type_str() }).collect();
        Ok(QueryResult { rows: vec![serde_json::Value::Object(rec)], row_count: 1, fields, command: "CALL".to_string() })
    }

    /// Chạy routine với đối số đã tính.
    pub(crate) fn call_routine(&mut self, def: &RoutineDef, args: &[Value], trig: Option<TriggerCall>) -> Result<RoutineOut, String> {
        thread_local! {
            static DEPTH: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
        }
        if DEPTH.with(|d| d.get()) > 100 {
            return Err("stack depth limit exceeded".into());
        }
        let inputs: Vec<RoutineArg> = def.inputs().cloned().collect();
        if def.strict && args.iter().any(|a| a.is_null()) {
            return Ok(RoutineOut { scalar: Value::Null, rows: vec![], cols: def.outputs(), record: Some(None) });
        }
        // giá trị theo kiểu khai báo của tham số
        let args: Vec<Value> = args.iter().zip(&inputs).map(|(v, a)| super::executor::cast_val(v.clone(), &a.ty.to_uppercase())).collect();
        DEPTH.with(|d| d.set(d.get() + 1));
        let res = if def.language == "plpgsql" { self.call_plpgsql_routine(def, &inputs, &args, trig) } else { self.call_sql_routine(def, &inputs, &args) };
        DEPTH.with(|d| d.set(d.get() - 1));
        res
    }

    fn call_sql_routine(&mut self, def: &RoutineDef, inputs: &[RoutineArg], args: &[Value]) -> Result<RoutineOut, String> {
        let named: Vec<(String, Value)> = inputs.iter().zip(args).filter_map(|(a, v)| a.name.clone().map(|n| (n, v.clone()))).collect();
        let stmts = super::executor::split_sql_statements(&def.body);
        let mut last: Option<QueryResult> = None;
        for st in stmts {
            let st = st.trim();
            if st.is_empty() {
                continue;
            }
            // $1, $2 ... và tên tham số (`tên_hàm.tham_số` cũng được) → hằng có kiểu
            let mut s = st.to_string();
            for (i, (a, v)) in inputs.iter().zip(args).enumerate().rev() {
                s = replace_param_ref(&s, &format!("${}", i + 1), &typed_literal(v, &a.ty));
            }
            let s = self.subst_routine_params(&s, &def.name, &named, false)?;
            let r = self.execute_statement(&s, &[])?;
            if let Some(e) = crate::engine::errors::take() {
                return Err(e);
            }
            last = Some(r);
        }
        let r = last.unwrap_or(QueryResult { rows: vec![], row_count: 0, fields: vec![], command: String::new() });
        let outs = def.outputs();
        if def.returns_set {
            let cols = if outs.is_empty() { r.fields.iter().map(|f| (f.name.clone(), f.data_type.clone())).collect() } else { outs };
            return Ok(RoutineOut { scalar: Value::Null, rows: r.rows, cols, record: None });
        }
        let first = r.rows.first().and_then(|row| row.as_object()).cloned();
        if !outs.is_empty() || def.returns == "record" || self.storage.get_table(&def.returns).is_some() {
            return Ok(RoutineOut { scalar: Value::Null, rows: vec![], cols: outs, record: Some(first) });
        }
        let v = first.as_ref().and_then(|o| o.values().next()).map(json_scalar).unwrap_or(Value::Null);
        let v = if def.returns == "void" { Value::text("") } else { super::executor::cast_val(v, &def.returns.to_uppercase()) };
        Ok(RoutineOut { scalar: v, rows: vec![], cols: vec![], record: None })
    }

    fn call_plpgsql_routine(&mut self, def: &RoutineDef, inputs: &[RoutineArg], args: &[Value], trig: Option<TriggerCall>) -> Result<RoutineOut, String> {
        let mut vars: Vec<(String, Value, String)> = Vec::new();
        for (i, (a, v)) in inputs.iter().zip(args).enumerate() {
            let n = a.name.clone().unwrap_or_else(|| format!("${}", i + 1));
            vars.push((n, v.clone(), a.ty.clone()));
            vars.push((format!("${}", i + 1), v.clone(), a.ty.clone()));
        }
        let outs = def.outputs();
        for (n, ty) in &outs {
            if !vars.iter().any(|(x, _, _)| x == n) {
                vars.push((n.clone(), Value::Null, ty.clone()));
            }
        }
        let out_names: String = outs.iter().map(|(n, _)| n.clone()).collect::<Vec<_>>().join(",");
        vars.push(("__out_params".into(), Value::text(out_names), "text".into()));
        let mut records: Vec<(String, Option<serde_json::Map<String, serde_json::Value>>)> = Vec::new();
        let mut null_records: Vec<String> = Vec::new();
        if let Some(tc) = &trig {
            // NEW / OLD không có (INSERT / DELETE): bản ghi NULL — đọc trường ra NULL, RETURN nó = RETURN NULL
            records.push(("new".into(), tc.new.clone().or_else(|| Some(tc.null_record.clone()))));
            records.push(("old".into(), tc.old.clone().or_else(|| Some(tc.null_record.clone()))));
            null_records = [(tc.new.is_none(), "new"), (tc.old.is_none(), "old")].iter().filter(|(n, _)| *n).map(|(_, r)| r.to_string()).collect();
            vars.push(("tg_op".into(), Value::text(&tc.op), "text".into()));
            vars.push(("tg_name".into(), Value::text(&tc.name), "text".into()));
            vars.push(("tg_table_name".into(), Value::text(&tc.table), "text".into()));
            vars.push(("tg_relname".into(), Value::text(&tc.table), "text".into()));
            vars.push(("tg_table_schema".into(), Value::text("public"), "text".into()));
            vars.push(("tg_when".into(), Value::text(&tc.when), "text".into()));
            vars.push(("tg_level".into(), Value::text(&tc.level), "text".into()));
            vars.push(("tg_nargs".into(), Value::Int(tc.argv.len() as i64), "int".into()));
            vars.push(("tg_argv".into(), Value::text(serde_json::to_string(&tc.argv).unwrap_or_default()), "text[]".into()));
        }
        // TG_ARGV[n] (đánh số từ 0) → hằng
        let body = match &trig {
            Some(tc) if def.body.to_ascii_lowercase().contains("tg_argv[") => {
                let mut b = def.body.clone();
                loop {
                    let low = b.to_ascii_lowercase();
                    let Some(p) = low.find("tg_argv[") else { break };
                    let Some(e) = b[p..].find(']').map(|e| p + e) else { break };
                    let idx: usize = b[p + 8..e].trim().parse().unwrap_or(usize::MAX);
                    let v = tc.argv.get(idx).map(|a| format!("'{}'", a.replace('\'', "''"))).unwrap_or_else(|| "NULL".into());
                    b.replace_range(p..=e, &v);
                }
                b
            }
            _ => def.body.clone(),
        };
        let env = self.run_plpgsql_body(&body, vars, records, &null_records)?;
        if def.returns_set {
            // SETOF vô hướng: cột mang tên hàm
            let rows: Vec<serde_json::Value> = env
                .ret_rows
                .into_iter()
                .map(|r| match r {
                    serde_json::Value::Object(mut m) if m.len() == 1 && m.contains_key("__v") => {
                        let v = m.remove("__v").unwrap_or(serde_json::Value::Null);
                        serde_json::json!({ def.name.clone(): v })
                    }
                    other => other,
                })
                .collect();
            let cols = if outs.is_empty() { vec![(def.name.clone(), def.returns.clone())] } else { outs };
            return Ok(RoutineOut { scalar: Value::Null, rows, cols, record: None });
        }
        if trig.is_some() {
            return Ok(RoutineOut { scalar: Value::Null, rows: vec![], cols: vec![], record: env.ret_record });
        }
        if !outs.is_empty() && env.ret.is_none() {
            let rec = env.out_values.clone();
            return Ok(RoutineOut { scalar: Value::Null, rows: vec![], cols: outs, record: Some(Some(rec)) });
        }
        if let Some(r) = env.ret_record.clone() {
            return Ok(RoutineOut { scalar: Value::Null, rows: vec![], cols: vec![], record: Some(r) });
        }
        let v = env.ret.unwrap_or(Value::Null);
        let v = if def.returns == "void" { Value::text("") } else { super::executor::cast_val(v, &def.returns.to_uppercase()) };
        Ok(RoutineOut { scalar: v, rows: vec![], cols: vec![], record: None })
    }

    /// Thay lời gọi routine trong câu lệnh (xem đầu module). `None` = câu không có lời gọi nào.
    pub(crate) fn resolve_routine_calls(&mut self, sql: &str, params: &[Value]) -> Option<Result<QueryResult, String>> {
        // câu vừa được viết lại và còn lời gọi để lại cho LATERAL: không xử lý lại
        if SKIP_SQL.with(|k| k.borrow().as_deref() == Some(sql)) {
            SKIP_SQL.with(|k| *k.borrow_mut() = None);
            return None;
        }
        let calls = self.find_routine_calls(sql);
        if calls.is_empty() {
            return None;
        }
        Some(self.execute_with_routine_calls(sql, params))
    }

    /// Vị trí (đầu tên, mở ngoặc, đóng ngoặc) của các lời gọi routine trong `sql` (ngoài chuỗi), trong cùng trước.
    fn find_routine_calls(&self, sql: &str) -> Vec<(usize, usize, usize, String)> {
        let b = sql.as_bytes();
        let mut out = Vec::new();
        let mut i = 0;
        let names: std::collections::BTreeSet<String> = self
            .storage
            .objects
            .extra
            .range(("routine".to_string(), String::new())..)
            .take_while(|((k, _), _)| k == "routine")
            .filter_map(|((_, n), _)| n.split('(').next().map(str::to_string))
            .collect();
        if names.is_empty() {
            return out;
        }
        while i < b.len() {
            let c = b[i];
            if c == b'\'' {
                i += 1;
                while i < b.len() && !(b[i] == b'\'' && b.get(i + 1) != Some(&b'\'')) {
                    if b[i] == b'\'' {
                        i += 1;
                    }
                    i += 1;
                }
                i += 1;
                continue;
            }
            if c == b'$' {
                if let Some(e) = super::executor::dollar_quote_end(sql, i) {
                    i = e;
                    continue;
                }
            }
            if (c.is_ascii_alphabetic() || c == b'_') && (i == 0 || !(b[i - 1].is_ascii_alphanumeric() || b[i - 1] == b'_' || b[i - 1] == b'"')) {
                let st = i;
                while i < b.len() && (b[i].is_ascii_alphanumeric() || b[i] == b'_' || b[i] == b'.') {
                    i += 1;
                }
                let word = sql[st..i].rsplit('.').next().unwrap_or("").to_lowercase();
                let mut j = i;
                while j < b.len() && b[j].is_ascii_whitespace() {
                    j += 1;
                }
                if j < b.len() && b[j] == b'(' && names.contains(&word) {
                    if let Some(close) = super::constraints::matching_paren(sql, j) {
                        out.push((st, j, close, word));
                    }
                }
                continue;
            }
            i += 1;
        }
        // lời gọi bên trong trước (để đối số không còn chứa lời gọi khác)
        out.sort_by_key(|(st, _, close, _)| (close - st, *st));
        out
    }

    fn execute_with_routine_calls(&mut self, sql: &str, params: &[Value]) -> Result<QueryResult, String> {
        let mut s = sql.to_string();
        let mut temps: Vec<String> = Vec::new();
        let mut deferred: Vec<(usize, usize, usize, String)> = Vec::new();
        let mut lateral_left = false;
        let res = (|| -> Result<Option<QueryResult>, String> {
            for _ in 0..256 {
                let calls = self.find_routine_calls(&s);
                let Some((st, open, close, name)) = calls.into_iter().find(|c| !deferred.iter().any(|d| d.0 == c.0)) else { break };
                let call_args = split_call_args(&s[open + 1..close]);
                let (def, exprs) = self.resolve_routine(&name, &call_args, Some(false))?;
                // 1. inline: hàm SQL `SELECT <biểu thức>` không FROM
                if let Some(expr) = inline_body(&def) {
                    let mut e = expr;
                    let inputs: Vec<RoutineArg> = def.inputs().cloned().collect();
                    for (i, a) in inputs.iter().enumerate().rev() {
                        let arg = format!("({})", exprs[i].clone().unwrap_or_else(|| "NULL".into()));
                        let cast = if a.ty.is_empty() || matches!(a.ty.as_str(), "anyelement" | "any") { arg.clone() } else { format!("CAST({} AS {})", arg, a.ty) };
                        e = replace_param_ref(&e, &format!("${}", i + 1), &cast);
                        if let Some(n) = &a.name {
                            e = replace_ident(&e, n, &cast);
                            e = replace_ident(&e, &format!("{}.{}", def.name, n), &cast);
                        }
                    }
                    let ret = if def.returns.is_empty() || def.returns == "void" { format!("({})", e) } else { format!("CAST(({}) AS {})", e, def.returns) };
                    s.replace_range(st..=close, &ret);
                    continue;
                }
                let in_from = call_in_from(&s, st);
                // 2. đối số hằng
                match self.eval_args(&exprs, params) {
                    Ok(args) => {
                        let out = self.call_routine(&def, &args, None)?;
                        if in_from || def.returns_set {
                            if !in_from {
                                return Err(format!("set-returning function {}() is only supported in FROM or as the only select item", def.name));
                            }
                            // hàm trong FROM → bảng tạm
                            let alias_follows = alias_after(&s, close + 1);
                            // `FROM f(...) AS a` của hàm trả một cột vô hướng: cột mang tên alias
                            let mut out = out;
                            if alias_follows && def.returns_set && def.outputs().is_empty() {
                                let t2 = s[close + 1..].trim_start();
                                let t2 = if t2.len() >= 3 && t2[..3].eq_ignore_ascii_case("AS ") { t2[3..].trim_start() } else { t2 };
                                let a: String = t2.chars().take_while(|c| c.is_alphanumeric() || *c == '_' || *c == '"').collect();
                                let has_cols = t2[a.len()..].trim_start().starts_with('(');
                                if !a.is_empty() && !has_cols {
                                    let a = a.trim_matches('"').to_lowercase();
                                    out.cols = vec![(a.clone(), def.returns.clone())];
                                    out.rows = out.rows.into_iter().map(|r| match r {
                                        serde_json::Value::Object(m) => serde_json::json!({ a.clone(): m.into_iter().next().map(|x| x.1).unwrap_or(serde_json::Value::Null) }),
                                        other => other,
                                    }).collect();
                                }
                            }
                            let tmp = self.routine_temp_table(&def, &out, temps.len())?;
                            let rep = if alias_follows { tmp.clone() } else { format!("{} AS {}", tmp, def.name) };
                            s.replace_range(st..=close, &rep);
                            temps.push(tmp);
                        } else {
                            let v = match &out.record {
                                Some(Some(r)) if !out.cols.is_empty() || def.returns == "record" => Value::text(super::executor::json_record_text(r)),
                                _ => out.scalar.clone(),
                            };
                            s.replace_range(st..=close, &format!("{}", typed_literal(&v, &def.returns)));
                        }
                    }
                    Err(_) if in_from => {
                        // `LATERAL (SELECT * FROM f(ngoài.cột) ...)`: đường LATERAL thay giá trị dòng ngoài rồi
                        // chạy subquery (khi đó đối số là hằng)
                        lateral_left = true;
                        deferred.push((st, open, close, String::new()));
                    }
                    Err(_) => {
                        // 3. đối số là cột: chỉ trong danh sách SELECT
                        deferred.push((st, open, close, name));
                    }
                }
            }
            if deferred.iter().any(|d| !d.3.is_empty()) {
                return self.select_with_row_calls(&s, params).map(Some);
            }
            Ok(None)
        })();
        let out = match res {
            Ok(Some(r)) => Ok(r),
            Ok(None) => {
                if lateral_left {
                    SKIP_SQL.with(|k| *k.borrow_mut() = Some(super::frontend::lex(&s).map(|l| super::executor::normalize_named_sql_params_pub(&l)).unwrap_or_else(|_| s.clone()).trim().trim_end_matches(|c: char| c == ';' || c == '.').trim().to_string()));
                }
                self.execute_statement(&s, params)
            }
            Err(e) => Err(e),
        };
        for t in temps {
            self.storage.tables.remove(&t);
        }
        out
    }

    /// Bảng tạm chứa kết quả của một routine dùng trong FROM.
    fn routine_temp_table(&mut self, def: &RoutineDef, out: &RoutineOut, n: usize) -> Result<String, String> {
        static SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let tmp = format!("__udf_{}_{}", SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed), n);
        let (cols, rows): (Vec<(String, String)>, Vec<Vec<Value>>) = if def.returns_set {
            let cols = if out.cols.is_empty() { vec![(def.name.clone(), def.returns.clone())] } else { out.cols.clone() };
            let rows = out.rows.iter().map(|r| cols.iter().enumerate().map(|(i, (c, _))| r.get(c).or_else(|| r.as_object().and_then(|o| o.values().nth(i))).map(json_scalar).unwrap_or(Value::Null)).collect()).collect();
            (cols, rows)
        } else if let Some(Some(rec)) = &out.record {
            let cols: Vec<(String, String)> = if out.cols.is_empty() { rec.keys().map(|k| (k.clone(), "text".to_string())).collect() } else { out.cols.clone() };
            (cols.clone(), vec![cols.iter().map(|(c, _)| rec.get(c).map(json_scalar).unwrap_or(Value::Null)).collect()])
        } else {
            (vec![(def.name.clone(), def.returns.clone())], vec![vec![out.scalar.clone()]])
        };
        let defs = cols.iter().map(|(c, ty)| crate::types::ColumnDef { name: c.clone(), data_type: DataType::from_sql_str(ty), is_primary_key: false, is_nullable: true, default_value: None, comment: None }).collect();
        let mut t = crate::storage::table::Table::new(tmp.clone(), defs);
        t.pk_col_idx = None;
        for r in rows {
            t.insert(r);
        }
        self.storage.tables.insert(tmp.clone(), t);
        Ok(tmp)
    }

    /// `SELECT ..., f(cột) ... FROM ...`: chạy với đối số làm cột ẩn, gọi hàm cho từng dòng.
    fn select_with_row_calls(&mut self, sql: &str, params: &[Value]) -> Result<QueryResult, String> {
        let t = sql.trim();
        if !t.get(..6).is_some_and(|p| p.eq_ignore_ascii_case("SELECT")) {
            return Err("function calls with column arguments are only supported in the SELECT list".into());
        }
        let from = find_top_level_keyword(t, "FROM").ok_or("function calls with column arguments need a FROM clause")?;
        let list = &t[6..from];
        let items = super::executor::split_projection_items_pub(list);
        let mut new_items = Vec::new();
        // (vị trí cột kết quả, routine, cột ẩn của đối số, tên đầu ra)
        let mut plan: Vec<(usize, RoutineDef, Vec<String>, String)> = Vec::new();
        for (idx, it) in items.iter().enumerate() {
            let it = it.trim();
            let (expr, alias) = match super::executor::rfind_top_level_as(&it.to_uppercase()) {
                Some(p) => (it[..p].trim(), Some(it[p + 4..].trim().trim_matches('"').to_string())),
                None => (it, None),
            };
            let calls = self.find_routine_calls(expr);
            let whole = calls.iter().find(|(st, _, close, _)| *st == 0 && *close == expr.len() - 1);
            match whole {
                Some((_, open, close, name)) => {
                    let call_args = split_call_args(&expr[open + 1..*close]);
                    let (def, exprs) = self.resolve_routine(name, &call_args, Some(false))?;
                    if def.returns_set {
                        return Err(format!("set-returning function {}() with column arguments is not supported here; use FROM t, LATERAL {}(...)", def.name, def.name));
                    }
                    let hidden: Vec<String> = (0..exprs.len()).map(|j| format!("__udf{}_{}", idx, j)).collect();
                    for (j, e) in exprs.iter().enumerate() {
                        new_items.push(format!("({}) AS \"{}\"", e.clone().unwrap_or_else(|| "NULL".into()), hidden[j]));
                    }
                    if exprs.is_empty() {
                        new_items.push(format!("1 AS \"__udf{}_none\"", idx));
                    }
                    let out_name = alias.unwrap_or_else(|| def.name.clone());
                    plan.push((idx, def, hidden, out_name));
                }
                None if !calls.is_empty() => {
                    return Err(format!("function {}() with column arguments inside an expression is not supported; select it as its own column", calls[0].3));
                }
                None => new_items.push(it.to_string()),
            }
        }
        let rest = &t[from..];
        let ru = rest.to_ascii_uppercase();
        if self.find_routine_calls(rest).len() > 0 {
            return Err("function calls with column arguments are only supported in the SELECT list".into());
        }
        if ru.contains("GROUP BY") || ru.contains(" DISTINCT") {
            return Err("function calls with column arguments are not supported together with GROUP BY / DISTINCT".into());
        }
        let q = format!("SELECT {} {}", new_items.join(", "), rest);
        let mut res = self.execute_statement(&q, params)?;
        for row in res.rows.iter_mut() {
            let Some(obj) = row.as_object() else { continue };
            let mut computed: Vec<(usize, String, Value)> = Vec::new();
            for (idx, def, hidden, out_name) in &plan {
                let args: Vec<Value> = hidden.iter().map(|h| obj.get(h).map(json_scalar).unwrap_or(Value::Null)).collect();
                let out = self.call_routine(def, &args, None)?;
                let v = match &out.record {
                    Some(Some(r)) => Value::text(super::executor::json_record_text(r)),
                    _ => out.scalar,
                };
                computed.push((*idx, out_name.clone(), v));
            }
            // dựng lại object theo thứ tự cột: cột thường giữ nguyên, cột hàm thay cho các cột ẩn
            let mut m = serde_json::Map::new();
            let mut done_idx = std::collections::HashSet::new();
            for (k, v) in obj.iter() {
                if let Some(rest) = k.strip_prefix("__udf") {
                    let idx: usize = rest.split('_').next().and_then(|x| x.parse().ok()).unwrap_or(usize::MAX);
                    if done_idx.insert(idx) {
                        if let Some((_, n, val)) = computed.iter().find(|(i, _, _)| *i == idx) {
                            m.insert(n.clone(), super::executor::value_to_json_pub(val));
                        }
                    }
                    continue;
                }
                m.insert(k.clone(), v.clone());
            }
            *row = serde_json::Value::Object(m);
        }
        // trường kết quả
        let mut fields = Vec::new();
        let mut done_idx = std::collections::HashSet::new();
        for f in &res.fields {
            if let Some(rest) = f.name.strip_prefix("__udf") {
                let idx: usize = rest.split('_').next().and_then(|x| x.parse().ok()).unwrap_or(usize::MAX);
                if done_idx.insert(idx) {
                    if let Some((_, def, _, n)) = plan.iter().find(|(i, _, _, _)| *i == idx) {
                        fields.push(FieldInfo { name: n.clone(), data_type: DataType::from_sql_str(&def.returns).field_type_str() });
                    }
                }
                continue;
            }
            fields.push(f.clone());
        }
        res.fields = fields;
        Ok(res)
    }
}

thread_local! {
    static SKIP_SQL: std::cell::RefCell<Option<String>> = const { std::cell::RefCell::new(None) };
}

/// Thân hàm SQL dạng `SELECT <biểu thức>` (một câu, không FROM / WHERE...) → biểu thức.
fn inline_body(def: &RoutineDef) -> Option<String> {
    if def.language != "sql" || def.returns_set || def.strict || !def.outputs().is_empty() {
        return None;
    }
    let b = def.body.trim().trim_end_matches(';').trim();
    if super::executor::split_sql_statements(b).len() != 1 || !b.get(..7).is_some_and(|p| p.eq_ignore_ascii_case("SELECT ")) {
        return None;
    }
    let e = b[7..].trim();
    for kw in ["FROM", "WHERE", "GROUP", "ORDER", "LIMIT", "UNION", "HAVING", "OFFSET", "INTO"] {
        if find_top_level_keyword(e, kw).is_some() {
            return None;
        }
    }
    if split_comma_separated_tokens(e).len() != 1 || super::executor::contains_ci_pub(e, "SELECT") {
        return None;
    }
    let e = match super::executor::rfind_top_level_as(&e.to_uppercase()) {
        Some(p) => e[..p].trim(),
        None => e,
    };
    Some(e.to_string())
}

/// Lời gọi bắt đầu tại `st` đứng ở vị trí một mục của FROM (sau FROM / JOIN / `,` của FROM / LATERAL).
fn call_in_from(s: &str, st: usize) -> bool {
    let before = s[..st].trim_end();
    let bu = before.to_ascii_uppercase();
    if bu.ends_with(" FROM") || bu.ends_with(" JOIN") || bu.ends_with(" LATERAL") || bu == "FROM" {
        return true;
    }
    if before.ends_with(',') {
        // dấu phẩy thuộc danh sách FROM nếu FROM cuối cùng ở cấp ngoài đứng sau SELECT cuối
        let from = super::executor::find_top_level_keyword(before, "FROM");
        let where_ = super::executor::find_top_level_keyword(before, "WHERE");
        return from.is_some() && where_.map_or(true, |w| w < from.unwrap());
    }
    false
}

/// Sau vị trí `i` có alias (`AS x` / `x`) không.
fn alias_after(s: &str, i: usize) -> bool {
    let t = s[i..].trim_start();
    if t.len() >= 3 && t[..3].eq_ignore_ascii_case("AS ") {
        return true;
    }
    let w: String = t.chars().take_while(|c| c.is_alphanumeric() || *c == '_' || *c == '"').collect();
    !w.is_empty() && !["WHERE", "ORDER", "GROUP", "LIMIT", "OFFSET", "JOIN", "LEFT", "RIGHT", "INNER", "CROSS", "FULL", "ON", "UNION", "HAVING", "WINDOW", "FETCH", "FOR", "NATURAL", "USING", "WITH"].contains(&w.to_ascii_uppercase().as_str())
}

/// Thay `$n` (không phải `$n0`...) ngoài chuỗi.
fn replace_param_ref(s: &str, p: &str, rep: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let b = s.as_bytes();
    let mut i = 0;
    let mut in_str = false;
    while i < b.len() {
        if b[i] == b'\'' {
            in_str = !in_str;
        }
        if !in_str && s[i..].starts_with(p) && !b.get(i + p.len()).is_some_and(|c| c.is_ascii_digit()) {
            out.push_str(rep);
            i += p.len();
            continue;
        }
        let ch = s[i..].chars().next().unwrap();
        out.push(ch);
        i += ch.len_utf8();
    }
    out
}

/// Thay định danh `name` (cả từ, không sau `.`, ngoài chuỗi) bằng `rep`.
fn replace_ident(s: &str, name: &str, rep: &str) -> String {
    let b = s.as_bytes();
    let n = name.as_bytes();
    let mut out = String::with_capacity(s.len());
    let mut i = 0;
    let mut in_str = false;
    let is_w = |c: u8| c.is_ascii_alphanumeric() || c == b'_';
    while i < b.len() {
        if b[i] == b'\'' {
            in_str = !in_str;
        }
        if !in_str && i + n.len() <= b.len() && b[i..i + n.len()].eq_ignore_ascii_case(n) && (i == 0 || !is_w(b[i - 1]) && b[i - 1] != b'.') && b.get(i + n.len()).map_or(true, |c| !is_w(*c) && *c != b'(') {
            out.push_str(rep);
            i += n.len();
            continue;
        }
        let ch = s[i..].chars().next().unwrap();
        out.push(ch);
        i += ch.len_utf8();
    }
    out
}
