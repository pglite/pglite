//! Sequence gắn với cột: `DEFAULT nextval('t_id_seq'::regclass)` (dạng mà các công cụ dump schema sinh
//! ra cho SERIAL) và `nextval` / `currval` / `setval` / `pg_get_serial_sequence` trên các sequence đó.
//!
//! Không có đối tượng sequence lưu riêng: sequence `<bảng>_<cột>_seq` của cột khoá chính dùng chính bộ
//! đếm `auto_increment` của bảng (như SERIAL), cột khác dùng `serial_next` (tính lười từ max của cột).
//! Vì vậy định dạng file DB và WAL không đổi; bộ đếm được dựng lại từ dữ liệu khi mở DB. `setval` không
//! bao giờ hạ bộ đếm xuống dưới max hiện có + 1 (không thể sinh khoá trùng), và giá trị đặt cao hơn chỉ
//! giữ trong phiên. `CREATE SEQUENCE` độc lập vẫn chưa hỗ trợ.
use super::executor::{contains_ci, find_top_level_keyword, split_function_args, Executor};
use crate::types::{DataType, Value};
use std::cell::RefCell;
use std::collections::HashMap;

thread_local! {
    /// `currval`: giá trị `nextval` gần nhất của mỗi sequence trong phiên.
    static LAST_VALUES: RefCell<HashMap<String, i64>> = RefCell::new(HashMap::new());
}

const FUNCS: [&str; 4] = ["NEXTVAL", "CURRVAL", "SETVAL", "PG_GET_SERIAL_SEQUENCE"];

/// `DEFAULT nextval(...)`: cột tự tăng.
pub(crate) fn is_nextval_default(def: &str) -> bool {
    def.trim_start().get(..8).map_or(false, |p| p.eq_ignore_ascii_case("NEXTVAL("))
}

/// Câu lệnh có gọi hàm sequence (lọc nhanh).
pub(crate) fn mentions_sequence_fn(sql: &str) -> bool {
    const CALLS: [&str; 4] = ["NEXTVAL(", "CURRVAL(", "SETVAL(", "PG_GET_SERIAL_SEQUENCE("];
    CALLS.iter().any(|f| contains_ci(sql, f))
}

/// Tên sequence từ đối số: bỏ `::regclass`, schema `public.` và ngoặc kép, viết thường.
fn seq_key(name: &str) -> String {
    let n = name.trim();
    let n = n.rsplit_once('.').map_or(n, |(schema, rest)| if schema.trim_matches('"').eq_ignore_ascii_case("public") { rest } else { n });
    n.trim_matches('"').to_lowercase()
}

/// Tên sequence ghi trong `DEFAULT nextval('<tên>'::regclass)`.
fn default_seq_name(def: &str) -> Option<String> {
    let inner = def.trim().get(8..)?;
    let start = inner.find('\'')?;
    let end = inner[start + 1..].find('\'')? + start + 1;
    Some(seq_key(&inner[start + 1..end]))
}

/// Cột có sequence: SERIAL hoặc `DEFAULT nextval(...)`.
fn owned_seq_name(table: &crate::storage::table::Table, col: usize) -> Option<String> {
    let c = &table.columns[col];
    if let Some(d) = c.default_value.as_deref().filter(|d| is_nextval_default(d)) {
        return default_seq_name(d).or_else(|| Some(format!("{}_{}_seq", table.name, c.name).to_lowercase()));
    }
    (c.data_type == DataType::Serial).then(|| format!("{}_{}_seq", table.name, c.name).to_lowercase())
}

fn sql_literal(v: &Value) -> String {
    match v {
        Value::Null => "NULL".to_string(),
        Value::Int(i) => i.to_string(),
        Value::Float(f) => f.to_string(),
        Value::Bool(b) => (if *b { "TRUE" } else { "FALSE" }).to_string(),
        Value::Text(s) => format!("'{}'", s.replace('\'', "''")),
    }
}

/// Lời gọi hàm sequence trong cùng (đối số không chứa lời gọi khác), trái nhất: (vị trí, tên, mở, đóng).
fn find_innermost_call(sql: &str) -> Option<(usize, &'static str, usize, usize)> {
    let b = sql.as_bytes();
    let mut in_str = false;
    let mut i = 0;
    while i < b.len() {
        if b[i] == b'\'' {
            in_str = !in_str;
        } else if !in_str && (i == 0 || !(b[i - 1].is_ascii_alphanumeric() || b[i - 1] == b'_')) {
            for f in FUNCS {
                let n = f.len();
                if i + n < b.len() && b[i..i + n].eq_ignore_ascii_case(f.as_bytes()) && b[i + n] == b'(' {
                    let open = i + n;
                    let close = crate::engine::constraints::matching_paren(sql, open)?;
                    if mentions_sequence_fn(&sql[open + 1..close]) {
                        break; // còn lời gọi lồng bên trong: xử lý nó trước
                    }
                    return Some((i, f, open, close));
                }
            }
        }
        i += 1;
    }
    None
}

impl Executor {
    /// Sequence `key` → (bảng, cột).
    fn find_owned_sequence(&self, key: &str) -> Option<(String, usize)> {
        let mut names: Vec<&String> = self.storage.tables.keys().collect();
        names.sort();
        names.into_iter().find_map(|n| {
            let t = &self.storage.tables[n];
            (0..t.columns.len()).find(|&c| owned_seq_name(t, c).as_deref() == Some(key)).map(|c| (n.clone(), c))
        })
    }

    /// Giá trị kế tiếp của cột tự tăng (và tiến bộ đếm). Cột khoá chính dùng `auto_increment` như SERIAL.
    pub(crate) fn take_next_value(table: &mut crate::storage::table::Table, col: usize) -> i64 {
        let live_max = |t: &crate::storage::table::Table| {
            t.rows.iter().zip(&t.is_deleted).filter_map(|(r, d)| if *d { None } else { r.get(col).and_then(|v| v.as_i64()) }).max().unwrap_or(0)
        };
        // cột identity có START / INCREMENT riêng: giá trị kế tiếp = max (min nếu bước âm) hiện có + bước,
        // hoặc START khi chưa có dòng (bộ đếm dựng lại được từ dữ liệu khi mở DB)
        if let Some(sp) = table.columns.get(col).and_then(|c| table.identity.get(&c.name.to_lowercase())).copied() {
            if sp.start != 1 || sp.increment != 1 {
                let next = match table.serial_next.get(&col) {
                    Some(n) => *n,
                    None => {
                        let vals = table.rows.iter().zip(&table.is_deleted).filter_map(|(r, d)| if *d { None } else { r.get(col).and_then(|v| v.as_i64()) });
                        let edge = if sp.increment > 0 { vals.max() } else { vals.min() };
                        match edge {
                            Some(e) => e + sp.increment,
                            None => sp.start,
                        }
                    }
                };
                table.serial_next.insert(col, next + sp.increment);
                return next;
            }
        }
        if Some(col) == table.pk_col_idx {
            let v = table.auto_increment.max(1);
            table.auto_increment = v + 1;
            return v;
        }
        let next = match table.serial_next.get(&col) {
            Some(n) => *n,
            None => live_max(table) + 1,
        };
        table.serial_next.insert(col, next + 1);
        next
    }

    fn sequence_call(&mut self, func: &str, args: &[Value]) -> Result<Value, String> {
        if args.iter().take(2).any(|v| v.is_null()) {
            return Ok(Value::Null);
        }
        if func == "PG_GET_SERIAL_SEQUENCE" {
            if args.len() != 2 {
                return Err("function pg_get_serial_sequence requires 2 arguments".to_string());
            }
            let tname = seq_key(&args[0].as_str());
            let cname = args[1].as_str();
            let t = self.storage.get_table(&tname).ok_or_else(|| format!("relation \"{}\" does not exist", args[0].as_str()))?;
            let c = t.get_column_index(cname.trim_matches('"')).ok_or_else(|| format!("column \"{}\" of relation \"{}\" does not exist", cname, tname))?;
            return Ok(owned_seq_name(t, c).map_or(Value::Null, |s| Value::text(format!("public.{}", s))));
        }
        let raw = args.first().map(|v| v.as_str()).unwrap_or_default();
        let key = seq_key(&raw);
        let (tname, col) = self.find_owned_sequence(&key).ok_or_else(|| format!("relation \"{}\" does not exist", raw))?;
        match func {
            "NEXTVAL" => {
                let t = self.storage.tables.get_mut(&tname).unwrap();
                let v = Self::take_next_value(t, col);
                LAST_VALUES.with(|m| m.borrow_mut().insert(key, v));
                Ok(Value::Int(v))
            }
            "CURRVAL" => LAST_VALUES
                .with(|m| m.borrow().get(&key).copied())
                .map(Value::Int)
                .ok_or_else(|| format!("currval of sequence \"{}\" is not yet defined in this session", key)),
            _ => {
                // SETVAL(seq, v [, is_called = true]): nextval kế tiếp trả v + 1 (hoặc v nếu is_called = false)
                let v = args.get(1).and_then(|x| x.as_i64().or_else(|| x.as_str().trim().parse().ok())).ok_or_else(|| "setval: value must be an integer".to_string())?;
                let is_called = args.get(2).map_or(true, |x| x.as_bool().unwrap_or(true));
                let t = self.storage.tables.get_mut(&tname).unwrap();
                let floor = t.rows.iter().zip(&t.is_deleted).filter_map(|(r, d)| if *d { None } else { r.get(col).and_then(|x| x.as_i64()) }).max().unwrap_or(0) + 1;
                let next = (if is_called { v + 1 } else { v }).max(floor);
                if Some(col) == t.pk_col_idx {
                    t.auto_increment = next;
                } else {
                    t.serial_next.insert(col, next);
                }
                if is_called {
                    LAST_VALUES.with(|m| m.borrow_mut().insert(key, v));
                }
                Ok(Value::Int(v))
            }
        }
    }

    /// Thay mọi lời gọi hàm sequence bằng giá trị (đối số tính bằng `SELECT <đối số>`, nên subquery như
    /// `(SELECT COALESCE(MAX(id), 1) FROM t)` dùng được). `nextval` / `setval` / `currval` chỉ được tính
    /// một lần mỗi lời gọi, nên chỉ nhận ở câu không đọc dòng nào: `SELECT` không có FROM ở cấp ngoài
    /// cùng, hoặc `INSERT ... VALUES`. `None` nếu không đổi gì.
    pub(crate) fn resolve_sequence_calls(&mut self, sql: &str, params: &[Value]) -> Result<Option<String>, String> {
        let up = sql.trim_start().to_uppercase();
        // DDL (`CREATE TABLE ... DEFAULT nextval(...)`) giữ nguyên biểu thức
        if !["SELECT", "INSERT", "UPDATE", "DELETE", "WITH", "VALUES"].iter().any(|k| up.starts_with(k)) {
            return Ok(None);
        }
        let per_row = if up.starts_with("SELECT") {
            find_top_level_keyword(sql, "FROM").is_some()
        } else if up.starts_with("INSERT") {
            find_top_level_keyword(sql, "VALUES").is_none()
        } else {
            true
        };
        let mut out = sql.to_string();
        let mut changed = false;
        while let Some((start, func, open, close)) = find_innermost_call(&out) {
            let raw_args = split_function_args(&out[open + 1..close]);
            let literal = |a: &str| a.starts_with('\'') && a.ends_with('\'') && a.len() >= 2 && !a[1..a.len() - 1].contains('\'');
            if per_row && (func != "PG_GET_SERIAL_SEQUENCE" || !raw_args.iter().all(|a| literal(a.trim()))) {
                return Err(format!("{}() is only supported in SELECT without FROM or INSERT ... VALUES, or with literal arguments", func.to_lowercase()));
            }
            let mut args = Vec::new();
            for a in raw_args {
                let a = a.trim();
                let v = if literal(a) {
                    Value::text(&a[1..a.len() - 1])
                } else if let Ok(i) = a.parse::<i64>() {
                    Value::Int(i)
                } else {
                    let r = self.execute_statement(&format!("SELECT {} AS v", a), params)?;
                    r.rows.first().and_then(|row| row.as_object()).and_then(|o| o.values().next()).map(super::executor::json_to_value).unwrap_or(Value::Null)
                };
                args.push(v);
            }
            let v = self.sequence_call(func, &args)?;
            // cả mục SELECT không alias: giữ tên cột PostgreSQL đặt (`setval`, `nextval`...)
            let before = out[..start].trim_end().to_uppercase();
            let after = out[close + 1..].trim_start().to_uppercase();
            let whole_item = (before.ends_with(',') || before.ends_with("SELECT"))
                && (after.is_empty() || after.starts_with(',') || after.starts_with("FROM ") || after.starts_with(';'));
            let lit = if whole_item && up.starts_with("SELECT") { format!("{} AS \"{}\"", sql_literal(&v), func.to_lowercase()) } else { sql_literal(&v) };
            out = format!("{}{}{}", &out[..start], lit, &out[close + 1..]);
            changed = true;
        }
        Ok(changed.then_some(out))
    }
}
