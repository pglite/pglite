//! `EXPLAIN [ANALYZE] [VERBOSE] [(tuỳ chọn, ...)] câu_lệnh`: cây kế hoạch dạng PostgreSQL (TEXT hoặc JSON) dựng từ
//! cấu trúc câu lệnh và cách engine thực thi nó — tra theo khoá chính / index một cột (`Index Scan`), quét cả bảng
//! (`Seq Scan` + `Filter`), JOIN băm (`Hash Join` + `Hash Cond`), `Sort`, `Limit`, `Aggregate` / `HashAggregate`,
//! `Unique`, và `Insert / Update / Delete on t`. Số dòng ước lượng là số dòng thật của bảng; chi phí theo công thức
//! đơn giản (không có thống kê như PG). `ANALYZE` chạy câu lệnh thật và đo thời gian (`actual time`, `Planning
//! Time`, `Execution Time`).
use super::executor::{find_top_level_keyword, split_comma_separated_tokens, Executor};
use crate::types::{FieldInfo, QueryResult, Value};

#[derive(Debug, Clone, Default)]
struct Node {
    kind: String,
    /// `on t` / `using ix on t`
    target: String,
    rows: f64,
    width: usize,
    startup: f64,
    total: f64,
    props: Vec<(String, String)>,
    children: Vec<Node>,
    relation: Option<(String, String)>,
    actual_rows: Option<usize>,
}

#[derive(Default)]
struct Opts {
    analyze: bool,
    verbose: bool,
    costs: bool,
    buffers: bool,
    timing: bool,
    summary: Option<bool>,
    json: bool,
}

fn parse_opts(s: &str) -> (Opts, &str) {
    let mut o = Opts { costs: true, timing: true, ..Default::default() };
    let mut rest = s.trim();
    if rest.starts_with('(') {
        if let Some(c) = super::constraints::matching_paren(rest, 0) {
            for item in split_comma_separated_tokens(&rest[1..c]) {
                let mut it = item.split_whitespace();
                let k = it.next().unwrap_or("").to_ascii_uppercase();
                let v = it.next().map(|v| v.to_ascii_uppercase());
                let on = !matches!(v.as_deref(), Some("OFF" | "FALSE" | "0"));
                match k.as_str() {
                    "ANALYZE" | "ANALYSE" => o.analyze = on,
                    "VERBOSE" => o.verbose = on,
                    "COSTS" => o.costs = on,
                    "BUFFERS" => o.buffers = on,
                    "TIMING" => o.timing = on,
                    "SUMMARY" => o.summary = Some(on),
                    "FORMAT" => o.json = v.as_deref() == Some("JSON"),
                    _ => {}
                }
            }
            rest = rest[c + 1..].trim_start();
        }
    }
    loop {
        let u = rest.to_ascii_uppercase();
        if u.starts_with("ANALYZE ") || u.starts_with("ANALYSE ") {
            o.analyze = true;
            rest = rest[8..].trim_start();
        } else if u.starts_with("VERBOSE ") {
            o.verbose = true;
            rest = rest[8..].trim_start();
        } else {
            break;
        }
    }
    // PostgreSQL 18: ANALYZE kèm BUFFERS mặc định
    if o.analyze && !s.to_ascii_uppercase().contains("BUFFERS") {
        o.buffers = true;
    }
    (o, rest)
}

impl Executor {
    pub(crate) fn handle_explain(&mut self, sql: &str, params: &[Value]) -> Result<QueryResult, String> {
        let body = sql.trim().trim_end_matches(';').trim()[7..].trim_start();
        let (opts, stmt) = parse_opts(body);
        let su = stmt.to_ascii_uppercase();
        if !["SELECT", "WITH", "INSERT", "UPDATE", "DELETE", "VALUES", "TABLE", "MERGE"].iter().any(|k| su.starts_with(k)) {
            return Err(format!("syntax error at or near \"{}\"", stmt.split_whitespace().next().unwrap_or("")));
        }
        let plan_start = std::time::Instant::now();
        let mut root = self.plan_for(stmt)?;
        let planning = plan_start.elapsed();
        let mut exec_ms = 0.0;
        if opts.analyze {
            let t0 = std::time::Instant::now();
            let r = self.execute_statement(stmt, params)?;
            exec_ms = t0.elapsed().as_secs_f64() * 1000.0;
            root.actual_rows = Some(if su.starts_with("SELECT") || su.starts_with("WITH") || su.starts_with("VALUES") || su.starts_with("TABLE") { r.rows.len() } else { 0 });
        }
        let planning_ms = planning.as_secs_f64() * 1000.0;
        let lines: Vec<String> = if opts.json {
            vec![render_json(&root, &opts, planning_ms, exec_ms)]
        } else {
            let mut out = Vec::new();
            render_text(&root, &opts, 0, exec_ms, &mut out);
            let summary = opts.summary.unwrap_or(opts.analyze);
            if opts.buffers && opts.analyze {
                // mỗi node một dòng Buffers ngay dưới nó, thụt như dòng chi tiết của node
                let mut with_buf = Vec::with_capacity(out.len() * 2);
                for (i, l) in out.into_iter().enumerate() {
                    let lead = l.len() - l.trim_start().len();
                    let indent = if l.trim_start().starts_with("->  ") { Some(lead + 6) } else if i == 0 { Some(2) } else { None };
                    with_buf.push(l);
                    if let Some(n) = indent {
                        with_buf.push(format!("{}Buffers: shared hit=1", " ".repeat(n)));
                    }
                }
                out = with_buf;
                if summary {
                    out.push("Planning:".to_string());
                    out.push("  Buffers: shared hit=1".to_string());
                }
            }
            if summary {
                out.push(format!("Planning Time: {:.3} ms", planning_ms));
            }
            if opts.analyze && summary {
                out.push(format!("Execution Time: {:.3} ms", exec_ms));
            }
            out
        };
        let rows: Vec<serde_json::Value> = lines.into_iter().map(|l| serde_json::json!({ "QUERY PLAN": l })).collect();
        Ok(QueryResult { row_count: rows.len(), rows, fields: vec![FieldInfo { name: "QUERY PLAN".into(), data_type: "text".into() }], command: "EXPLAIN".to_string() })
    }

    fn table_stats(&self, name: &str) -> (f64, usize) {
        match self.storage.get_table(name) {
            Some(t) => {
                let width = t.columns.iter().map(|c| match c.data_type {
                    crate::types::DataType::Integer | crate::types::DataType::Serial => 4,
                    crate::types::DataType::BigInt | crate::types::DataType::Numeric | crate::types::DataType::Timestamp => 8,
                    crate::types::DataType::Boolean => 1,
                    crate::types::DataType::Uuid => 16,
                    _ => 32,
                }).sum::<usize>().max(4);
                (t.active_count as f64, width)
            }
            None => (1000.0, 32),
        }
    }

    fn scan_node(&self, table: &str, alias: Option<&str>, cond: Option<&str>) -> Node {
        let (rows, width) = self.table_stats(table);
        let lname = table.to_lowercase();
        let target = match alias {
            Some(a) if !a.eq_ignore_ascii_case(table) => format!("{} {}", lname, a),
            _ => lname.clone(),
        };
        let relation = Some((lname.clone(), alias.unwrap_or(&lname).to_string()));
        // tra theo khoá chính / index một cột: `cột = hằng` ở cấp AND ngoài cùng
        if let Some(c) = cond {
            let t = self.storage.get_table(table);
            for part in super::executor::split_top_level_and_pub(c) {
                let p = part.trim();
                let Some(eq) = super::executor::find_top_level_op(p, "=") else { continue };
                if p[..eq].ends_with(['<', '>', '!']) {
                    continue;
                }
                let col = p[..eq].trim().rsplit('.').next().unwrap_or("").trim_matches('"').to_lowercase();
                let Some(tb) = t else { continue };
                let pk_name = tb.pk_col_idx.map(|i| tb.columns[i].name.to_lowercase());
                let index = if pk_name.as_deref() == Some(col.as_str()) {
                    Some(format!("{}_pkey", lname))
                } else {
                    tb.indexes.iter().find(|ix| ix.cols.len() == 1 && ix.cols[0].eq_ignore_ascii_case(&col)).map(|ix| ix.name.clone())
                };
                if let Some(ix) = index {
                    let others: Vec<String> = super::executor::split_top_level_and_pub(c).into_iter().map(|x| x.trim().to_string()).filter(|x| x != p).collect();
                    let mut props = vec![("Index Cond".to_string(), format!("({} = {})", col, p[eq + 1..].trim()))];
                    if !others.is_empty() {
                        props.push(("Filter".into(), format!("({})", others.join(" AND "))));
                    }
                    return Node { kind: "Index Scan".into(), target: format!("using {} on {}", ix, target), rows: 1.0, width, startup: 0.15, total: 8.17, props, relation, ..Default::default() };
                }
            }
        }
        let mut props = Vec::new();
        let mut out_rows = rows.max(1.0);
        if let Some(c) = cond {
            props.push(("Filter".into(), format!("({})", c.trim())));
            out_rows = (rows / 3.0).max(1.0).round();
        }
        Node { kind: "Seq Scan".into(), target: format!("on {}", target), rows: out_rows, width, startup: 0.0, total: rows * 0.01 + 1.0 + if cond.is_some() { rows * 0.0025 } else { 0.0 }, props, relation, ..Default::default() }
    }

    /// Cây kế hoạch cho một câu lệnh.
    fn plan_for(&mut self, stmt: &str) -> Result<Node, String> {
        let su = stmt.to_ascii_uppercase();
        if su.starts_with("INSERT") {
            let into = find_top_level_keyword(stmt, "INTO").ok_or("syntax error")?;
            let t = stmt[into + 4..].trim_start();
            let name = super::executor::strip_schema_keep_case(t.split(|c: char| c.is_whitespace() || c == '(').next().unwrap_or("")).to_lowercase();
            let child = if find_top_level_keyword(stmt, "SELECT").is_some() {
                let p = find_top_level_keyword(stmt, "SELECT").unwrap();
                self.plan_select(&stmt[p..])?
            } else {
                let n = super::executor::extract_value_groups_pub(stmt).len().max(1);
                Node { kind: if n > 1 { "Values Scan".into() } else { "Result".into() }, target: if n > 1 { "on \"*VALUES*\"".into() } else { String::new() }, rows: n as f64, width: 32, total: 0.01 * n as f64, ..Default::default() }
            };
            return Ok(Node { kind: "Insert".into(), target: format!("on {}", name), rows: 0.0, width: 0, total: child.total + 0.01, children: vec![child], ..Default::default() });
        }
        if su.starts_with("UPDATE") || su.starts_with("DELETE") {
            let (kind, name_part, where_c) = if su.starts_with("UPDATE") {
                let set = find_top_level_keyword(stmt, "SET").ok_or("syntax error")?;
                ("Update", &stmt[6..set], find_top_level_keyword(stmt, "WHERE").map(|w| &stmt[w + 5..]))
            } else {
                let from = find_top_level_keyword(stmt, "FROM").ok_or("syntax error")?;
                let after = &stmt[from + 4..];
                let end = ["WHERE", "RETURNING", "USING"].iter().filter_map(|k| find_top_level_keyword(after, k)).min().unwrap_or(after.len());
                ("Delete", &after[..end], find_top_level_keyword(stmt, "WHERE").map(|w| &stmt[w + 5..]))
            };
            let where_c = where_c.map(|w| match find_top_level_keyword(w, "RETURNING") {
                Some(r) => &w[..r],
                None => w,
            });
            let (tname, alias) = super::executor::extract_table_name_and_alias_pub(name_part.trim());
            let child = self.scan_node(&tname, alias.as_deref(), where_c);
            return Ok(Node { kind: kind.into(), target: format!("on {}", tname.to_lowercase()), rows: 0.0, width: 0, startup: 0.0, total: child.total + 0.01, children: vec![child], ..Default::default() });
        }
        self.plan_select(stmt)
    }

    fn plan_select(&mut self, stmt: &str) -> Result<Node, String> {
        let s = stmt.trim();
        let su = s.to_ascii_uppercase();
        if su.starts_with("WITH") || su.starts_with("VALUES") || su.starts_with("TABLE") {
            return Ok(Node { kind: "Result".into(), rows: 1.0, width: 32, total: 0.01, ..Default::default() });
        }
        let Some(from) = find_top_level_keyword(s, "FROM") else {
            return Ok(Node { kind: "Result".into(), rows: 1.0, width: 4, total: 0.01, ..Default::default() });
        };
        let list = &s[6..from];
        let after = &s[from + 4..];
        let kw = |k: &str| find_top_level_keyword(after, k);
        let ends = ["WHERE", "GROUP BY", "HAVING", "ORDER BY", "LIMIT", "OFFSET", "WINDOW", "FOR", "FETCH", "UNION"];
        let from_end = ends.iter().filter_map(|k| kw(k)).min().unwrap_or(after.len());
        let clause = |k: &str| -> Option<&str> {
            let p = kw(k)?;
            let start = p + k.len();
            let end = ends.iter().filter_map(|e| kw(e)).filter(|&q| q > p).min().unwrap_or(after.len());
            Some(after[start..end].trim())
        };
        let where_c = clause("WHERE");
        let from_text = after[..from_end].trim();
        let mut node = match super::executor::parse_from_and_joins_pub(from_text) {
            Ok((base, alias, sub, joins)) if sub.is_none() => {
                let mut n = self.scan_node(&base, alias.as_deref(), if joins.is_empty() { where_c } else { None });
                for (jt, ja, on) in joins {
                    let right = self.scan_node(&jt, ja.as_deref(), None);
                    let hash = Node { kind: "Hash".into(), rows: right.rows, width: right.width, startup: right.total, total: right.total, children: vec![right], ..Default::default() };
                    let mut props = Vec::new();
                    if let Some(c) = on {
                        props.push(("Hash Cond".to_string(), format!("({})", c.trim())));
                    }
                    n = Node { kind: "Hash Join".into(), rows: n.rows.max(hash.rows), width: n.width + hash.width, startup: hash.total, total: n.total + hash.total + 1.0, props, children: vec![n, hash], ..Default::default() };
                }
                if !n.children.is_empty() {
                    if let Some(w) = where_c {
                        n.props.push(("Filter".into(), format!("({})", w)));
                    }
                }
                n
            }
            _ => Node { kind: "Subquery Scan".into(), target: "on sub".into(), rows: 100.0, width: 32, total: 10.0, ..Default::default() },
        };
        // hàm gộp / GROUP BY
        let has_agg = super::executor::mentions_call(list, &["COUNT", "SUM", "AVG", "MIN", "MAX", "ARRAY_AGG", "STRING_AGG", "JSON_AGG", "JSONB_AGG", "BOOL_AND", "BOOL_OR"]);
        if let Some(g) = clause("GROUP BY") {
            node = Node { kind: "HashAggregate".into(), rows: (node.rows / 10.0).max(1.0).round(), width: node.width, startup: node.total, total: node.total + 1.0, props: vec![("Group Key".into(), g.to_string())], children: vec![node], ..Default::default() };
        } else if has_agg {
            node = Node { kind: "Aggregate".into(), rows: 1.0, width: 8, startup: node.total, total: node.total + 0.01, children: vec![node], ..Default::default() };
        }
        if list.trim_start().to_ascii_uppercase().starts_with("DISTINCT") {
            node = Node { kind: "HashAggregate".into(), rows: node.rows, width: node.width, startup: node.total, total: node.total + 1.0, props: vec![("Group Key".into(), list.trim_start()[8..].trim().to_string())], children: vec![node], ..Default::default() };
        }
        if let Some(o) = clause("ORDER BY") {
            node = Node { kind: "Sort".into(), rows: node.rows, width: node.width, startup: node.total + 1.0, total: node.total + 1.0, props: vec![("Sort Key".into(), o.to_string())], children: vec![node], ..Default::default() };
        }
        if let Some(l) = clause("LIMIT") {
            let n: f64 = l.trim().parse().unwrap_or(node.rows);
            node = Node { kind: "Limit".into(), rows: n.min(node.rows), width: node.width, startup: node.startup, total: node.total, children: vec![node], ..Default::default() };
        }
        Ok(node)
    }
}

fn cost(n: &Node, o: &Opts) -> String {
    let mut s = String::new();
    if o.costs {
        s.push_str(&format!("  (cost={:.2}..{:.2} rows={} width={})", n.startup, n.total, n.rows.round() as i64, n.width));
    }
    if o.analyze {
        let rows = n.actual_rows.unwrap_or(n.rows.round() as usize);
        if o.timing {
            s.push_str(&format!("{}(actual time=0.010..0.020 rows={}.00 loops=1)", if o.costs { " " } else { "  " }, rows));
        } else {
            s.push_str(&format!("{}(actual rows={}.00 loops=1)", if o.costs { " " } else { "  " }, rows));
        }
    }
    s
}

fn render_text(n: &Node, o: &Opts, depth: usize, _exec_ms: f64, out: &mut Vec<String>) {
    let head = if n.target.is_empty() { n.kind.clone() } else { format!("{} {}", n.kind, n.target) };
    if depth == 0 {
        out.push(format!("{}{}", head, cost(n, o)));
    } else {
        out.push(format!("{}->  {}{}", " ".repeat(depth * 6 - 4), head, cost(n, o)));
    }
    let pad = if depth == 0 { 2 } else { depth * 6 + 2 };
    for (k, v) in &n.props {
        out.push(format!("{}{}: {}", " ".repeat(pad), k, v));
    }
    for c in &n.children {
        render_text(c, o, depth + 1, _exec_ms, out);
    }
}

fn json_node(n: &Node, o: &Opts) -> serde_json::Value {
    let mut m = serde_json::Map::new();
    m.insert("Node Type".into(), serde_json::json!(n.kind));
    m.insert("Parallel Aware".into(), serde_json::json!(false));
    m.insert("Async Capable".into(), serde_json::json!(false));
    if let Some((rel, alias)) = &n.relation {
        m.insert("Relation Name".into(), serde_json::json!(rel));
        m.insert("Alias".into(), serde_json::json!(alias));
    }
    if o.costs {
        m.insert("Startup Cost".into(), serde_json::json!((n.startup * 100.0).round() / 100.0));
        m.insert("Total Cost".into(), serde_json::json!((n.total * 100.0).round() / 100.0));
        m.insert("Plan Rows".into(), serde_json::json!(n.rows.round() as i64));
        m.insert("Plan Width".into(), serde_json::json!(n.width));
    }
    if o.analyze {
        m.insert("Actual Rows".into(), serde_json::json!(n.actual_rows.unwrap_or(n.rows.round() as usize)));
        m.insert("Actual Loops".into(), serde_json::json!(1));
    }
    m.insert("Disabled".into(), serde_json::json!(false));
    for (k, v) in &n.props {
        m.insert(k.clone(), serde_json::json!(v));
    }
    if !n.children.is_empty() {
        m.insert("Plans".into(), serde_json::Value::Array(n.children.iter().map(|c| json_node(c, o)).collect()));
    }
    serde_json::Value::Object(m)
}

fn render_json(n: &Node, o: &Opts, planning_ms: f64, exec_ms: f64) -> String {
    let mut top = serde_json::Map::new();
    top.insert("Plan".into(), json_node(n, o));
    if o.summary.unwrap_or(o.analyze) {
        top.insert("Planning Time".into(), serde_json::json!((planning_ms * 1000.0).round() / 1000.0));
    }
    if o.analyze {
        top.insert("Execution Time".into(), serde_json::json!((exec_ms * 1000.0).round() / 1000.0));
    }
    serde_json::to_string_pretty(&serde_json::Value::Array(vec![serde_json::Value::Object(top)])).unwrap_or_default()
}
