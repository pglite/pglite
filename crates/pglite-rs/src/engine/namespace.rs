//! Không gian tên schema thật (đợt 15c): `app.t` khác `public.t`, `search_path`.
//!
//! Bảng / view ở schema khác `public` được lưu với tên nội bộ là định danh có nháy `"app.t"` (mã hoá
//! `__q<hex>_` như mọi định danh có nháy — tên cột, khoá JSON, thông báo lỗi tự giải mã ra `app.t` như PG).
//! Bảng `public` giữ nguyên tên ⇒ DB cũ không đổi một byte. Câu lệnh được viết lại một lần sau lexer: tên có
//! schema người dùng → tên nội bộ; tên không kèm schema → theo `search_path` của phiên. Chỉ chạy khi DB có schema
//! người dùng. Bảng cũ tạo bằng `app.x` trước đợt này (khi đó nằm chung namespace, tên lưu là `x`) vẫn được tìm
//! thấy: nếu `"app.x"` không tồn tại mà `x` có thì dùng `x` như trước.
use super::executor::Executor;
use super::frontend::{decode_ident, encode_ident};

/// Tên nội bộ của `schema.tên` (đều viết thường).
pub(crate) fn internal_name(schema: &str, name: &str) -> String {
    encode_ident(&format!("{}.{}", schema, name))
}

/// (schema, tên) của một tên nội bộ; bảng `public` → `None`.
pub(crate) fn split_internal(key: &str, schemas: &std::collections::BTreeSet<String>) -> Option<(String, String)> {
    let d = decode_ident(key)?;
    let (s, n) = d.split_once('.')?;
    schemas.contains(s).then(|| (s.to_string(), n.to_string()))
}

const TABLE_KW: &[&str] = &["FROM", "JOIN", "INTO", "UPDATE", "TABLE", "REFERENCES", "TRUNCATE", "ONLY", "VIEW", "EXISTS", "LIKE", "INHERITS"];
const NOT_ALIAS: &[&str] = &[
    "WHERE", "JOIN", "LEFT", "RIGHT", "INNER", "FULL", "CROSS", "ON", "USING", "GROUP", "ORDER", "LIMIT", "OFFSET", "UNION", "EXCEPT", "INTERSECT", "HAVING",
    "WINDOW", "FOR", "NATURAL", "RETURNING", "SET", "FETCH", "VALUES", "SELECT", "DEFAULT", "AS", "ADD", "DROP", "ALTER", "RENAME", "OWNER", "PARTITION",
    "OVERRIDING", "WITH", "CASCADE", "RESTRICT", "IF", "TO", "OF", "FOR", "ENABLE", "DISABLE", "NO", "INHERIT", "ATTACH", "DETACH",
];

fn word_ci(s: &str, kw: &str) -> bool {
    s.len() >= kw.len() && s[..kw.len()].eq_ignore_ascii_case(kw) && s[kw.len()..].chars().next().map_or(true, |c| !(c.is_alphanumeric() || c == '_'))
}

impl Executor {
    /// Schema trong `search_path` (bỏ `$user`, `pg_catalog`), theo thứ tự.
    fn search_path(&self) -> Vec<String> {
        let raw = self.setting_value("search_path").unwrap_or_else(|| "public".to_string());
        let v: Vec<String> = raw
            .split(',')
            .map(|p| p.trim().trim_matches('"').trim_matches('\'').to_lowercase())
            .filter(|p| !p.is_empty() && p != "$user" && p != "pg_catalog")
            .collect();
        if v.is_empty() {
            vec!["public".to_string()]
        } else {
            v
        }
    }

    /// Câu có thể cần phân giải schema: DB có schema người dùng.
    pub(crate) fn namespaces_active(&self) -> bool {
        !self.storage.objects.schemas.is_empty()
    }

    fn rel_exists(&self, key: &str) -> bool {
        self.storage.get_table(key).is_some() || self.storage.objects.views.contains_key(key)
    }

    /// Viết lại tên bảng theo schema. `None` = không đổi.
    pub(crate) fn resolve_namespaces(&self, sql: &str) -> Option<String> {
        let schemas = &self.storage.objects.schemas;
        let path = self.search_path();
        let path_has_user = path.iter().take_while(|p| *p != "public").any(|p| schemas.contains(p));
        let up = sql.trim_start().to_ascii_uppercase();
        // câu tạo bảng / view: tên không kèm schema → schema đầu tiên của search_path
        let creating = (up.starts_with("CREATE") && (up.contains(" TABLE ") || up.contains(" VIEW "))) || up.starts_with("ALTER TABLE") && up.contains(" RENAME TO ");
        let m = super::oprewrite::code_mask(sql);
        let b = sql.as_bytes();
        let is_id = |c: u8| c.is_ascii_alphanumeric() || c == b'_';
        let mut out = String::with_capacity(sql.len() + 32);
        let mut last = 0;
        let mut i = 0;
        let mut changed = false;
        let mut created_one = false;
        while i < b.len() {
            if !m[i] || !(b[i].is_ascii_alphabetic() || b[i] == b'_') || (i > 0 && (is_id(b[i - 1]) || b[i - 1] == b'.' || b[i - 1] == b'"' || b[i - 1] == b':' || b[i - 1] == b'$')) {
                i += 1;
                continue;
            }
            let st = i;
            while i < b.len() && is_id(b[i]) {
                i += 1;
            }
            let first = sql[st..i].to_ascii_lowercase();
            // schema.tên (tên có thể đã được mã hoá vì viết trong nháy)
            let qualified = b.get(i) == Some(&b'.') && b.get(i + 1).is_some_and(|c| c.is_ascii_alphabetic() || *c == b'_');
            let (schema, name, end) = if qualified {
                let ns = i + 1;
                let mut ne = ns;
                while ne < b.len() && is_id(b[ne]) {
                    ne += 1;
                }
                let n = sql[ns..ne].to_string();
                let n = decode_ident(&n).unwrap_or_else(|| n.to_lowercase());
                (Some(decode_ident(&sql[st..i]).unwrap_or(first.clone())), n, ne)
            } else {
                (None, first.clone(), i)
            };
            let before = sql[..st].trim_end();
            // chỉ cần vài từ cuối (không chép cả phần đầu câu cho mỗi định danh)
            let tail = before.char_indices().rev().nth(23).map(|(p, _)| p).unwrap_or(0);
            let bu = before[tail..].to_ascii_uppercase();
            let in_from_list = before.ends_with(',') && super::executor::find_top_level_keyword(&sql[..st], "FROM").is_some() && super::executor::find_top_level_keyword(&sql[..st], "WHERE").is_none();
            let table_pos = TABLE_KW.iter().any(|k| bu.ends_with(k) && bu[..bu.len() - k.len()].chars().last().map_or(true, |c| !(c.is_alphanumeric() || c == '_'))) || in_from_list
                || (bu.ends_with("ON") && up.starts_with("CREATE") && up.contains("INDEX"))
                || (bu.ends_with("NOT EXISTS") || bu.ends_with("IF EXISTS"));
            let resolved: Option<String> = match &schema {
                Some(s) if s == "public" || s == "pg_catalog" || s == "information_schema" => None,
                Some(s) if schemas.contains(s) => {
                    if b.get(end) == Some(&b'.') {
                        // schema.bảng.cột → bảng.cột (bảng đã có alias trong FROM)
                        out.push_str(&sql[last..st]);
                        out.push_str(&name);
                        last = end;
                        i = end;
                        changed = true;
                        continue;
                    }
                    let key = internal_name(s, &name.to_lowercase());
                    let legacy = !creating && !self.rel_exists(&key) && self.rel_exists(&name.to_lowercase());
                    (!legacy).then_some(key)
                }
                Some(_) => None,
                None if table_pos && path_has_user && b.get(end) != Some(&b'(') => {
                    let lname = name.to_lowercase();
                    let mut found = None;
                    if creating && !created_one {
                        // tạo mới: schema đầu tiên trong search_path
                        if let Some(s) = path.first() {
                            if schemas.contains(s) {
                                found = Some(internal_name(s, &lname));
                            }
                        }
                        created_one = true;
                    } else {
                        for s in &path {
                            if s == "public" {
                                break;
                            }
                            if schemas.contains(s) && self.rel_exists(&internal_name(s, &lname)) {
                                found = Some(internal_name(s, &lname));
                                break;
                            }
                        }
                    }
                    found
                }
                None => None,
            };
            if table_pos && schema.is_some() && !created_one && creating {
                created_one = true;
            }
            let Some(key) = resolved else {
                i = end;
                continue;
            };
            out.push_str(&sql[last..st]);
            out.push_str(&key);
            // FROM / JOIN không có alias: giữ tên bảng làm alias để `t.cột` vẫn đúng
            let rest = sql[end..].trim_start();
            let from_pos = bu.ends_with("FROM") || bu.ends_with("JOIN") || in_from_list || bu.ends_with("ONLY");
            let has_alias = rest.chars().next().is_some_and(|c| c.is_alphabetic() || c == '_' || c == '"') && !NOT_ALIAS.iter().any(|k| word_ci(rest, k)) || word_ci(rest, "AS");
            let delete_or_update = up.starts_with("DELETE") || up.starts_with("UPDATE");
            if from_pos && !has_alias && !(delete_or_update && bu.ends_with("FROM") && super::executor::find_top_level_keyword(&sql[..st], "FROM").map_or(false, |p| sql[..p].trim().to_ascii_uppercase() == "DELETE")) {
                out.push_str(&format!(" AS {}", name.to_lowercase()));
            }
            last = end;
            i = end;
            changed = true;
        }
        if !changed {
            return None;
        }
        out.push_str(&sql[last..]);
        Some(out)
    }

    /// Bảng / view thuộc schema `s` (tên nội bộ).
    pub(crate) fn relations_in_schema(&self, s: &str) -> Vec<String> {
        let prefix = format!("{}.", s);
        let mut v: Vec<String> = self
            .storage
            .tables
            .keys()
            .chain(self.storage.objects.views.keys())
            .filter(|k| decode_ident(k).is_some_and(|d| d.starts_with(&prefix)))
            .cloned()
            .collect();
        v.sort();
        v.dedup();
        v
    }
}
