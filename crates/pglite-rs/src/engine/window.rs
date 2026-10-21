//! Window function: `f(...) [FILTER (WHERE ...)] OVER ([PARTITION BY ...] [ORDER BY ...] [frame])`,
//! `OVER w` với `WINDOW w AS (...)`.
//!
//! Thứ tự như PostgreSQL: truy vấn bên trong (FROM/WHERE/GROUP BY/HAVING) chạy trước, kèm các cột ẩn
//! cho đối số / PARTITION BY / ORDER BY của từng window; window được tính trên các dòng đó; sau cùng
//! mới tới DISTINCT, ORDER BY, OFFSET/LIMIT của câu ngoài.
//!
//! Frame: không ORDER BY = cả partition; có ORDER BY = RANGE UNBOUNDED PRECEDING .. CURRENT ROW (gồm
//! các dòng ngang hàng); `ROWS`, `GROUPS` và `RANGE` (offset số trên một khoá ORDER BY) `BETWEEN ...`,
//! cùng `EXCLUDE CURRENT ROW | GROUP | TIES | NO OTHERS`. Biên tính bằng tìm nhị phân trên khoá đã sắp.
use super::executor::{find_top_level_keyword, json_order_cmp_pub, split_comma_separated_tokens, split_projection_items_pub, Executor};
use crate::types::{FieldInfo, QueryResult, Value};

#[derive(Clone, Copy, Debug, PartialEq)]
enum Bound {
    UnboundedPreceding,
    Preceding(f64),
    CurrentRow,
    Following(f64),
    UnboundedFollowing,
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum FrameMode {
    Rows,
    Range,
    Groups,
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum Exclude {
    NoOthers,
    CurrentRow,
    Group,
    Ties,
}

#[derive(Clone, Copy, Debug)]
struct Frame {
    mode: FrameMode,
    start: Bound,
    end: Bound,
    exclude: Exclude,
}

#[derive(Debug)]
struct WinItem {
    alias: String,
    func: String,
    args: Vec<String>,
    filter: Option<String>,
    partition: Vec<String>,
    order: Vec<(String, bool, bool)>,
    frame: Option<Frame>,
}

fn starts_kw(s: &str, kw: &str) -> bool {
    s.get(..kw.len()).map_or(false, |p| p.eq_ignore_ascii_case(kw)) && s[kw.len()..].chars().next().map_or(true, |c| !c.is_alphanumeric() && c != '_')
}

/// Vị trí `OVER` ở cấp ngoài cùng của một phần tử SELECT.
fn find_over(item: &str) -> Option<usize> {
    find_top_level_keyword(item, "OVER")
}

fn parse_bound(s: &str) -> Option<Bound> {
    let u = s.trim().to_uppercase();
    Some(match u.as_str() {
        "UNBOUNDED PRECEDING" => Bound::UnboundedPreceding,
        "UNBOUNDED FOLLOWING" => Bound::UnboundedFollowing,
        "CURRENT ROW" => Bound::CurrentRow,
        _ => {
            let mut it = u.split_whitespace();
            let n: f64 = it.next()?.trim_matches('\'').parse().ok().filter(|n: &f64| *n >= 0.0)?;
            match it.next()? {
                "PRECEDING" => Bound::Preceding(n),
                "FOLLOWING" => Bound::Following(n),
                _ => return None,
            }
        }
    })
}

/// Đọc phần trong ngoặc của OVER (...): PARTITION BY, ORDER BY, frame.
fn parse_spec(spec: &str) -> (Vec<String>, Vec<(String, bool, bool)>, Option<Frame>) {
    let mut partition = Vec::new();
    let mut order = Vec::new();
    let mut frame = None;
    let frame_pos = ["ROWS", "RANGE", "GROUPS"].iter().filter_map(|kw| find_top_level_keyword(spec, kw)).min();
    let (head, frame_str) = match frame_pos {
        Some(p) => (&spec[..p], Some(&spec[p..])),
        None => (spec, None),
    };
    let ob = find_top_level_keyword(head, "ORDER BY");
    if let Some(pb) = find_top_level_keyword(head, "PARTITION BY") {
        let end = ob.filter(|&o| o > pb).unwrap_or(head.len());
        partition = split_comma_separated_tokens(&head[pb + 12..end]).into_iter().map(|s| s.trim().to_string()).collect();
    }
    if let Some(o) = ob {
        for item in split_comma_separated_tokens(&head[o + 8..]) {
            let mut it = item.trim();
            let mut nulls: Option<bool> = None;
            for (kw, first) in [(" NULLS FIRST", true), (" NULLS LAST", false)] {
                if it.len() > kw.len() && it.get(it.len() - kw.len()..).map_or(false, |x| x.eq_ignore_ascii_case(kw)) {
                    it = it[..it.len() - kw.len()].trim();
                    nulls = Some(first);
                }
            }
            let up = it.to_uppercase();
            let (e, desc) = if up.ends_with(" DESC") {
                (it[..it.len() - 5].trim(), true)
            } else if up.ends_with(" ASC") {
                (it[..it.len() - 4].trim(), false)
            } else {
                (it, false)
            };
            order.push((e.to_string(), desc, nulls.unwrap_or(desc)));
        }
    }
    if let Some(f) = frame_str {
        let f = f.trim();
        let up = f.to_uppercase();
        let (mode, kw_len) = if up.starts_with("ROWS") {
            (FrameMode::Rows, 4)
        } else if up.starts_with("GROUPS") {
            (FrameMode::Groups, 6)
        } else {
            (FrameMode::Range, 5)
        };
        let body = f[kw_len..].trim();
        let (body, exclude) = match find_top_level_keyword(body, "EXCLUDE") {
            Some(p) => {
                let ex = body[p + 7..].trim().to_uppercase();
                let ex = match ex.as_str() {
                    "CURRENT ROW" => Exclude::CurrentRow,
                    "GROUP" => Exclude::Group,
                    "TIES" => Exclude::Ties,
                    _ => Exclude::NoOthers,
                };
                (body[..p].trim(), ex)
            }
            None => (body, Exclude::NoOthers),
        };
        let bounds = if starts_kw(body, "BETWEEN") {
            let inner = &body[7..];
            find_top_level_keyword(inner, "AND").and_then(|a| Some((parse_bound(&inner[..a])?, parse_bound(&inner[a + 3..])?)))
        } else {
            parse_bound(body).map(|s| (s, Bound::CurrentRow))
        };
        if let Some((start, end)) = bounds {
            frame = Some(Frame { mode, start, end, exclude });
        }
    }
    (partition, order, frame)
}

/// Thay `OVER w` bằng định nghĩa trong `WINDOW w AS (...)`.
fn window_defs(clause: &str) -> Vec<(String, String)> {
    let mut out = Vec::new();
    for def in split_comma_separated_tokens(clause) {
        if let Some(a) = find_top_level_keyword(def, "AS") {
            let name = def[..a].trim().trim_matches('"').to_lowercase();
            let body = def[a + 2..].trim();
            if body.starts_with('(') && body.ends_with(')') {
                out.push((name, body[1..body.len() - 1].to_string()));
            }
        }
    }
    out
}

fn num(v: &serde_json::Value) -> Option<f64> {
    match v {
        serde_json::Value::Number(n) => n.as_f64(),
        serde_json::Value::String(s) => s.trim().parse().ok(),
        _ => None,
    }
}

fn json_num(f: f64, int_like: bool) -> serde_json::Value {
    if int_like && f.fract() == 0.0 && f.abs() < 9.0e15 {
        serde_json::json!(f as i64)
    } else {
        serde_json::Number::from_f64(f).map(serde_json::Value::Number).unwrap_or(serde_json::Value::Null)
    }
}

impl Executor {
    /// `None` nếu câu không có window function ở danh sách SELECT.
    pub(crate) fn try_handle_window_select(&mut self, sql: &str, params: &[Value]) -> Option<Result<QueryResult, String>> {
        let t = sql.trim();
        let from = find_top_level_keyword(t, "FROM").unwrap_or(t.len());
        let mut list_start = 6;
        let head_up = t[6..from].trim_start().to_uppercase();
        let distinct = head_up.starts_with("DISTINCT ") && !head_up.starts_with("DISTINCT ON");
        if distinct {
            list_start = t.len() - t[6..].trim_start().len() + 8;
        }
        let items = split_projection_items_pub(&t[list_start..from]);
        if !items.iter().any(|i| find_over(i).is_some()) {
            return None;
        }
        Some(self.handle_window_select(t, from, &items, distinct, params))
    }

    fn handle_window_select(&mut self, t: &str, from: usize, items: &[String], distinct: bool, params: &[Value]) -> Result<QueryResult, String> {
        let rest = &t[from..];
        // Mệnh đề của câu ngoài: WINDOW, ORDER BY, LIMIT/OFFSET/FETCH
        let win_pos = find_top_level_keyword(rest, "WINDOW");
        let tail_pos = ["ORDER BY", "LIMIT", "OFFSET", "FETCH"].iter().filter_map(|kw| find_top_level_keyword(rest, kw)).min();
        let body_end = [win_pos, tail_pos].iter().flatten().copied().min().unwrap_or(rest.len());
        let body = &rest[..body_end];
        let defs = match win_pos {
            Some(w) => window_defs(&rest[w + 6..tail_pos.filter(|&p| p > w).unwrap_or(rest.len())]),
            None => Vec::new(),
        };
        let tail = tail_pos.map(|p| &rest[p..]).unwrap_or("");

        let mut inner_items: Vec<String> = Vec::new();
        let mut out_cols: Vec<(String, Option<usize>)> = Vec::new(); // (tên, chỉ số window)
        let mut wins: Vec<WinItem> = Vec::new();
        for (i, raw) in items.iter().enumerate() {
            let item = raw.trim();
            let (expr, alias) = match find_top_level_keyword(item, "AS") {
                Some(p) if find_over(item).map_or(true, |o| p > o) => (item[..p].trim(), Some(item[p + 2..].trim().trim_matches('"').to_string())),
                _ => (item, None),
            };
            let Some(o) = find_over(expr) else {
                let name = alias.unwrap_or_else(|| super::select_list::default_column_name(expr));
                inner_items.push(format!("{} AS \"__c{}\"", expr, i));
                out_cols.push((name, None));
                continue;
            };
            let call = expr[..o].trim();
            let mut spec = expr[o + 4..].trim().to_string();
            if spec.starts_with('(') && spec.ends_with(')') {
                spec = spec[1..spec.len() - 1].to_string();
            } else if let Some((_, body)) = defs.iter().find(|(n, _)| n.eq_ignore_ascii_case(spec.trim_matches('"'))) {
                spec = body.clone();
            }
            // OVER (w ORDER BY ...): kế thừa cửa sổ có tên
            if let Some((_, base)) = defs.iter().find(|(n, _)| spec.trim().split_whitespace().next().map_or(false, |f| f.eq_ignore_ascii_case(n))) {
                let extra = spec.trim()[spec.trim().split_whitespace().next().unwrap().len()..].to_string();
                spec = format!("{} {}", base, extra);
            }
            let (call, filter) = match find_top_level_keyword(call, "FILTER") {
                Some(fp) => {
                    let f = call[fp + 6..].trim();
                    let f = f.trim_start_matches('(').trim_end_matches(')').trim();
                    let f = if starts_kw(f, "WHERE") { f[5..].trim() } else { f };
                    (call[..fp].trim(), Some(f.to_string()))
                }
                None => (call, None),
            };
            let open = call.find('(').ok_or_else(|| format!("invalid window function: {}", call))?;
            let func = call[..open].trim().to_lowercase();
            let args_str = call[open + 1..call.len().saturating_sub(1)].trim();
            let args: Vec<String> = if args_str.is_empty() || args_str == "*" { Vec::new() } else { split_comma_separated_tokens(args_str).into_iter().map(|a| a.trim().to_string()).collect() };
            let (partition, order, frame) = parse_spec(&spec);
            let w = wins.len();
            for (k, a) in args.iter().enumerate() {
                inner_items.push(format!("{} AS \"__w{}a{}\"", a, w, k));
            }
            if let Some(f) = &filter {
                inner_items.push(format!("CASE WHEN {} THEN 1 ELSE 0 END AS \"__w{}f\"", f, w));
            }
            for (k, p) in partition.iter().enumerate() {
                inner_items.push(format!("{} AS \"__w{}p{}\"", p, w, k));
            }
            for (k, (e, _, _)) in order.iter().enumerate() {
                inner_items.push(format!("{} AS \"__w{}o{}\"", e, w, k));
            }
            let name = alias.unwrap_or_else(|| func.clone());
            wins.push(WinItem { alias: name.clone(), func, args, filter, partition, order, frame });
            out_cols.push((name, Some(w)));
        }
        // Cột ẩn cho ORDER BY của câu ngoài
        let order_specs: Vec<(String, bool, bool)> = match find_top_level_keyword(tail, "ORDER BY") {
            Some(o) => {
                let end = ["LIMIT", "OFFSET", "FETCH"].iter().filter_map(|kw| find_top_level_keyword(&tail[o + 8..], kw).map(|p| o + 8 + p)).min().unwrap_or(tail.len());
                parse_spec(&format!("ORDER BY {}", &tail[o + 8..end])).1
            }
            None => Vec::new(),
        };
        for (k, (e, _, _)) in order_specs.iter().enumerate() {
            let is_out = e.parse::<usize>().is_ok() || out_cols.iter().any(|(n, _)| n.eq_ignore_ascii_case(e.trim_matches('"')));
            if !is_out {
                inner_items.push(format!("{} AS \"__s{}\"", e, k));
            }
        }
        if inner_items.is_empty() {
            inner_items.push("1 AS \"__one\"".to_string());
        }
        let inner_sql = format!("SELECT {} {}", inner_items.join(", "), body);
        let res = self.execute_statement(&inner_sql, params)?;
        let rows = res.rows;
        let n = rows.len();
        let get = |r: usize, k: &str| -> serde_json::Value { rows[r].get(k).cloned().unwrap_or(serde_json::Value::Null) };

        // Tính từng window
        let mut computed: Vec<Vec<serde_json::Value>> = Vec::with_capacity(wins.len());
        for (w, win) in wins.iter().enumerate() {
            let mut out = vec![serde_json::Value::Null; n];
            // partition (giữ thứ tự xuất hiện)
            let mut parts: Vec<(String, Vec<usize>)> = Vec::new();
            let mut index: std::collections::HashMap<String, usize> = std::collections::HashMap::new();
            for r in 0..n {
                let key = (0..win.partition.len()).map(|k| get(r, &format!("__w{}p{}", w, k)).to_string()).collect::<Vec<_>>().join("\u{1f}");
                match index.get(&key) {
                    Some(&p) => parts[p].1.push(r),
                    None => {
                        index.insert(key.clone(), parts.len());
                        parts.push((key, vec![r]));
                    }
                }
            }
            let okey = |r: usize| -> Vec<serde_json::Value> { (0..win.order.len()).map(|k| get(r, &format!("__w{}o{}", w, k))).collect() };
            for (_, idxs) in parts.iter_mut() {
                if !win.order.is_empty() {
                    idxs.sort_by(|&a, &b| {
                        let (ka, kb) = (okey(a), okey(b));
                        for (k, (_, desc, nf)) in win.order.iter().enumerate() {
                            let o = json_order_cmp_pub(Some(&ka[k]), Some(&kb[k]), *desc, *nf);
                            if o != std::cmp::Ordering::Equal {
                                return o;
                            }
                        }
                        a.cmp(&b)
                    });
                }
                let len = idxs.len();
                let keys: Vec<Vec<serde_json::Value>> = idxs.iter().map(|&r| okey(r)).collect();
                // nhóm ngang hàng (peer): cùng khoá ORDER BY
                let mut peer_start = vec![0usize; len];
                let mut peer_end = vec![0usize; len];
                let mut i = 0;
                while i < len {
                    let mut j = i;
                    while j + 1 < len && !win.order.is_empty() && keys[j + 1] == keys[i] {
                        j += 1;
                    }
                    if win.order.is_empty() {
                        j = len - 1;
                    }
                    for k in i..=j {
                        peer_start[k] = i;
                        peer_end[k] = j;
                    }
                    i = j + 1;
                }
                // chỉ số nhóm ngang hàng của từng vị trí (cho GROUPS)
                let mut group_of = vec![0usize; len];
                let mut group_bounds: Vec<(usize, usize)> = Vec::new();
                for pos in 0..len {
                    if pos == 0 || peer_start[pos] == pos {
                        group_bounds.push((peer_start[pos], peer_end[pos] + 1));
                    }
                    group_of[pos] = group_bounds.len() - 1;
                }
                // RANGE có offset: khoá số của ORDER BY (một khoá), NULL nằm ở nhóm riêng
                let range_key: Vec<Option<f64>> = keys.iter().map(|k| k.first().and_then(num)).collect();
                let desc = win.order.first().is_some_and(|o| o.1);
                // vị trí đầu tiên (trong [0, len]) mà `pred` đúng; `pred` đơn điệu trên các dòng có khoá
                let first_where = |pred: &dyn Fn(f64) -> bool| -> usize {
                    let (mut lo, mut hi) = (0usize, len);
                    // các dòng NULL ở đầu hoặc cuối: bỏ qua bằng cách coi chúng thoả / không thoả theo vị trí
                    while lo < hi {
                        let mid = (lo + hi) / 2;
                        let ok = match range_key[mid] {
                            Some(v) => pred(v),
                            None => mid > 0 && range_key[..mid].iter().any(|x| x.is_some()),
                        };
                        if ok { hi = mid } else { lo = mid + 1 }
                    }
                    lo
                };
                let frame = |pos: usize| -> Vec<usize> {
                    let Some(fr) = win.frame else {
                        return if win.order.is_empty() { (0..len).collect() } else { (0..peer_end[pos] + 1).collect() };
                    };
                    let (st, en) = match fr.mode {
                        FrameMode::Rows => {
                            let st = match fr.start {
                                Bound::UnboundedPreceding => 0,
                                Bound::Preceding(k) => pos.saturating_sub(k as usize),
                                Bound::CurrentRow => pos,
                                Bound::Following(k) => (pos + k as usize).min(len),
                                Bound::UnboundedFollowing => len,
                            };
                            let en = match fr.end {
                                Bound::UnboundedPreceding => 0,
                                Bound::Preceding(k) => if pos >= k as usize { pos - k as usize + 1 } else { 0 },
                                Bound::CurrentRow => pos + 1,
                                Bound::Following(k) => (pos + k as usize + 1).min(len),
                                Bound::UnboundedFollowing => len,
                            };
                            (st, en)
                        }
                        FrameMode::Groups => {
                            let g = group_of[pos];
                            let ng = group_bounds.len();
                            let st = match fr.start {
                                Bound::UnboundedPreceding => 0,
                                Bound::Preceding(k) => group_bounds[g.saturating_sub(k as usize)].0,
                                Bound::CurrentRow => group_bounds[g].0,
                                Bound::Following(k) => group_bounds.get(g + k as usize).map_or(len, |b| b.0),
                                Bound::UnboundedFollowing => len,
                            };
                            let en = match fr.end {
                                Bound::UnboundedPreceding => 0,
                                Bound::Preceding(k) => if g >= k as usize { group_bounds[g - k as usize].1 } else { 0 },
                                Bound::CurrentRow => group_bounds[g].1,
                                Bound::Following(k) => group_bounds[(g + k as usize).min(ng - 1)].1,
                                Bound::UnboundedFollowing => len,
                            };
                            (st, en)
                        }
                        FrameMode::Range => {
                            let cur = range_key[pos];
                            let peers = (peer_start[pos], peer_end[pos] + 1);
                            // giá trị khoá ở biên: theo hướng sắp xếp
                            let bound_val = |b: Bound| -> Option<f64> {
                                let v = cur?;
                                match b {
                                    Bound::Preceding(k) => Some(if desc { v + k } else { v - k }),
                                    Bound::Following(k) => Some(if desc { v - k } else { v + k }),
                                    _ => None,
                                }
                            };
                            let st = match fr.start {
                                Bound::UnboundedPreceding => 0,
                                Bound::CurrentRow => peers.0,
                                Bound::UnboundedFollowing => len,
                                b => match bound_val(b) {
                                    // dòng đầu tiên có khoá "không trước" giá trị biên
                                    Some(bv) => first_where(&|x| if desc { x <= bv } else { x >= bv }),
                                    None => peers.0,
                                },
                            };
                            let en = match fr.end {
                                Bound::UnboundedPreceding => 0,
                                Bound::CurrentRow => peers.1,
                                Bound::UnboundedFollowing => len,
                                b => match bound_val(b) {
                                    // dòng đầu tiên có khoá "sau" giá trị biên
                                    Some(bv) => first_where(&|x| if desc { x < bv } else { x > bv }),
                                    None => peers.1,
                                },
                            };
                            (st, en)
                        }
                    };
                    let (gs, ge) = (peer_start[pos], peer_end[pos]);
                    (st..en.max(st))
                        .filter(|&i| match fr.exclude {
                            Exclude::NoOthers => true,
                            Exclude::CurrentRow => i != pos,
                            Exclude::Group => i < gs || i > ge,
                            Exclude::Ties => i == pos || i < gs || i > ge,
                        })
                        .collect()
                };
                let arg = |r: usize, k: usize| get(r, &format!("__w{}a{}", w, k));
                let passes = |r: usize| win.filter.is_none() || num(&get(r, &format!("__w{}f", w))) == Some(1.0);
                let int_arg = |k: usize, default: i64| -> i64 { win.args.get(k).and_then(|a| a.trim().parse::<i64>().ok()).unwrap_or(default) };
                let mut rank = 1usize;
                let mut dense = 0usize;
                for pos in 0..len {
                    let r = idxs[pos];
                    if pos == 0 || peer_start[pos] == pos {
                        rank = pos + 1;
                        dense += 1;
                    }
                    let v = match win.func.as_str() {
                        "row_number" => serde_json::json!(pos + 1),
                        "rank" => serde_json::json!(rank),
                        "dense_rank" => serde_json::json!(dense),
                        "percent_rank" => json_num(if len > 1 { (rank - 1) as f64 / (len - 1) as f64 } else { 0.0 }, false),
                        "cume_dist" => json_num((peer_end[pos] + 1) as f64 / len as f64, false),
                        "ntile" => {
                            let buckets = int_arg(0, 1).max(1) as usize;
                            let base = len / buckets;
                            let extra = len % buckets;
                            // các nhóm đầu có thêm một dòng
                            let mut acc = 0;
                            let mut b = 1;
                            for bi in 0..buckets {
                                let size = base + if bi < extra { 1 } else { 0 };
                                if pos < acc + size {
                                    b = bi + 1;
                                    break;
                                }
                                acc += size;
                            }
                            serde_json::json!(b)
                        }
                        "lag" | "lead" => {
                            let off = int_arg(1, 1).max(0) as usize;
                            let target = if win.func == "lag" { pos.checked_sub(off) } else { Some(pos + off).filter(|&p| p < len) };
                            match target {
                                Some(p) => arg(idxs[p], 0),
                                None => {
                                    if win.args.len() >= 3 { arg(r, 2) } else { serde_json::Value::Null }
                                }
                            }
                        }
                        "first_value" | "last_value" | "nth_value" => {
                            let fr = frame(pos);
                            if fr.is_empty() {
                                serde_json::Value::Null
                            } else if win.func == "first_value" {
                                arg(idxs[fr[0]], 0)
                            } else if win.func == "last_value" {
                                arg(idxs[fr[fr.len() - 1]], 0)
                            } else {
                                let nth = int_arg(1, 1).max(1) as usize;
                                fr.get(nth - 1).map_or(serde_json::Value::Null, |&p| arg(idxs[p], 0))
                            }
                        }
                        "count" | "sum" | "avg" | "min" | "max" | "bool_and" | "bool_or" | "every" | "string_agg" | "array_agg" | "json_agg" | "jsonb_agg" => {
                            let rows_in: Vec<usize> = frame(pos).into_iter().map(|p| idxs[p]).filter(|&x| passes(x)).collect();
                            match win.func.as_str() {
                                "count" => {
                                    if win.args.is_empty() {
                                        serde_json::json!(rows_in.len())
                                    } else {
                                        serde_json::json!(rows_in.iter().filter(|&&x| !arg(x, 0).is_null()).count())
                                    }
                                }
                                "sum" | "avg" => {
                                    let vals: Vec<f64> = rows_in.iter().filter_map(|&x| num(&arg(x, 0))).collect();
                                    let int_like = rows_in.iter().all(|&x| matches!(arg(x, 0), serde_json::Value::Null) || arg(x, 0).as_i64().is_some());
                                    if vals.is_empty() {
                                        serde_json::Value::Null
                                    } else if win.func == "sum" {
                                        json_num(vals.iter().sum(), int_like)
                                    } else {
                                        json_num(vals.iter().sum::<f64>() / vals.len() as f64, false)
                                    }
                                }
                                "min" | "max" => {
                                    let mut best: Option<serde_json::Value> = None;
                                    for &x in &rows_in {
                                        let v = arg(x, 0);
                                        if v.is_null() {
                                            continue;
                                        }
                                        best = Some(match best {
                                            None => v,
                                            Some(b) => {
                                                let o = json_order_cmp_pub(Some(&v), Some(&b), false, false);
                                                if (win.func == "min" && o == std::cmp::Ordering::Less) || (win.func == "max" && o == std::cmp::Ordering::Greater) { v } else { b }
                                            }
                                        });
                                    }
                                    best.unwrap_or(serde_json::Value::Null)
                                }
                                "bool_and" | "every" => {
                                    let vals: Vec<bool> = rows_in.iter().filter_map(|&x| arg(x, 0).as_bool()).collect();
                                    if vals.is_empty() { serde_json::Value::Null } else { serde_json::json!(vals.iter().all(|b| *b)) }
                                }
                                "bool_or" => {
                                    let vals: Vec<bool> = rows_in.iter().filter_map(|&x| arg(x, 0).as_bool()).collect();
                                    if vals.is_empty() { serde_json::Value::Null } else { serde_json::json!(vals.iter().any(|b| *b)) }
                                }
                                "string_agg" => {
                                    let sep = win.args.get(1).map(|s| s.trim().trim_matches('\'').to_string()).unwrap_or_default();
                                    let parts: Vec<String> = rows_in.iter().map(|&x| arg(x, 0)).filter(|v| !v.is_null()).map(|v| v.as_str().map(|s| s.to_string()).unwrap_or_else(|| v.to_string())).collect();
                                    if parts.is_empty() { serde_json::Value::Null } else { serde_json::json!(parts.join(&sep)) }
                                }
                                _ => serde_json::Value::Array(rows_in.iter().map(|&x| arg(x, 0)).collect()),
                            }
                        }
                        other => return Err(format!("function {}() is not supported as a window function", other)),
                    };
                    out[r] = v;
                }
            }
            computed.push(out);
        }

        // Dòng kết quả theo đúng thứ tự cột của SELECT
        let mut out_rows: Vec<(serde_json::Map<String, serde_json::Value>, Vec<serde_json::Value>)> = (0..n)
            .map(|r| {
                let mut m = serde_json::Map::new();
                for (ci, (name, w)) in out_cols.iter().enumerate() {
                    let v = match w {
                        Some(w) => computed[*w][r].clone(),
                        None => get(r, &format!("__c{}", ci)),
                    };
                    m.insert(name.clone(), v);
                }
                let sort_keys = order_specs
                    .iter()
                    .enumerate()
                    .map(|(k, (e, _, _))| {
                        if let Ok(pos) = e.parse::<usize>() {
                            m.values().nth(pos.saturating_sub(1)).cloned().unwrap_or(serde_json::Value::Null)
                        } else if let Some(v) = m.iter().find(|(n, _)| n.eq_ignore_ascii_case(e.trim_matches('"'))).map(|(_, v)| v.clone()) {
                            v
                        } else {
                            get(r, &format!("__s{}", k))
                        }
                    })
                    .collect();
                (m, sort_keys)
            })
            .collect();
        if order_specs.is_empty() {
            // Không có ORDER BY ngoài: như PostgreSQL, dòng ra theo thứ tự sắp của window cuối cùng
            // (PARTITION BY rồi ORDER BY của nó).
            if let Some(w) = wins.len().checked_sub(1) {
                let win = &wins[w];
                if !win.partition.is_empty() || !win.order.is_empty() {
                    let key = |r: usize| -> Vec<serde_json::Value> {
                        (0..win.partition.len()).map(|k| get(r, &format!("__w{}p{}", w, k))).chain((0..win.order.len()).map(|k| get(r, &format!("__w{}o{}", w, k)))).collect()
                    };
                    let mut idx: Vec<usize> = (0..n).collect();
                    idx.sort_by(|&a, &b| {
                        let (ka, kb) = (key(a), key(b));
                        for k in 0..ka.len() {
                            let (desc, nf) = if k < win.partition.len() { (false, false) } else { let o = &win.order[k - win.partition.len()]; (o.1, o.2) };
                            let o = json_order_cmp_pub(Some(&ka[k]), Some(&kb[k]), desc, nf);
                            if o != std::cmp::Ordering::Equal {
                                return o;
                            }
                        }
                        std::cmp::Ordering::Equal
                    });
                    let mut taken: Vec<Option<_>> = out_rows.into_iter().map(Some).collect();
                    out_rows = idx.into_iter().filter_map(|i| taken[i].take()).collect();
                }
            }
        }
        if distinct {
            let mut seen = std::collections::HashSet::new();
            out_rows.retain(|(m, _)| seen.insert(serde_json::to_string(m).unwrap_or_default()));
        }
        if !order_specs.is_empty() {
            out_rows.sort_by(|(_, a), (_, b)| {
                for (k, (_, desc, nf)) in order_specs.iter().enumerate() {
                    let o = json_order_cmp_pub(Some(&a[k]), Some(&b[k]), *desc, *nf);
                    if o != std::cmp::Ordering::Equal {
                        return o;
                    }
                }
                std::cmp::Ordering::Equal
            });
        }
        let num_after = |kw: &str| -> Option<usize> {
            let p = find_top_level_keyword(tail, kw)?;
            let tok = tail[p + kw.len()..].split_whitespace().next()?;
            match tok.strip_prefix('$') {
                Some(x) => params.get(x.parse::<usize>().ok()?.checked_sub(1)?)?.as_i64().map(|v| v.max(0) as usize),
                None => tok.parse().ok(),
            }
        };
        let offset = num_after("OFFSET").unwrap_or(0);
        let limit = num_after("LIMIT").unwrap_or(usize::MAX);
        let rows: Vec<serde_json::Value> = out_rows.into_iter().skip(offset).take(limit).map(|(m, _)| serde_json::Value::Object(m)).collect();
        let fields: Vec<FieldInfo> = out_cols
            .iter()
            .enumerate()
            .map(|(ci, (name, w))| {
                let data_type = match w {
                    Some(_) => rows.iter().find_map(|r| r.get(name).filter(|v| !v.is_null()).map(super::executor::type_str_from_json_value_pub)).unwrap_or_else(|| "bigint".to_string()),
                    None => res.fields.iter().find(|f| f.name == format!("__c{}", ci)).map(|f| f.data_type.clone()).unwrap_or_else(|| "text".to_string()),
                };
                FieldInfo { name: name.clone(), data_type }
            })
            .collect();
        Ok(QueryResult { row_count: rows.len(), rows, fields, command: "SELECT".to_string() })
    }
}
