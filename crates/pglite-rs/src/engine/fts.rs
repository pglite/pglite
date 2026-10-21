//! Full-text search (đợt 15): `to_tsvector`, `to_tsquery`, `plainto_tsquery`, `phraseto_tsquery`,
//! `websearch_to_tsquery`, `@@` (qua `pg_atat` khi hai vế không phải jsonb / jsonpath), `ts_rank`, `ts_rank_cd`,
//! `ts_headline`, `setweight`, `strip`, `numnode`, `tsvector_to_array`, `array_to_tsvector`.
//!
//! Cấu hình `english` (mặc định, như `default_text_search_config` của PG) dùng stemmer Snowball English
//! (Porter2) và danh sách stopword của PG; `simple` chỉ viết thường. tsvector / tsquery lưu dạng văn bản của PG
//! (`'cat':2 'run':4`, `'cat' & !'dog'`).
use crate::types::Value;
use std::collections::BTreeMap;

// ---------------------------------------------------------------------------------------------------------------
// Snowball English (Porter2)

const STOPWORDS: &[&str] = &[
    "i", "me", "my", "myself", "we", "our", "ours", "ourselves", "you", "your", "yours", "yourself", "yourselves", "he", "him", "his", "himself",
    "she", "her", "hers", "herself", "it", "its", "itself", "they", "them", "their", "theirs", "themselves", "what", "which", "who", "whom", "this",
    "that", "these", "those", "am", "is", "are", "was", "were", "be", "been", "being", "have", "has", "had", "having", "do", "does", "did", "doing",
    "a", "an", "the", "and", "but", "if", "or", "because", "as", "until", "while", "of", "at", "by", "for", "with", "about", "against", "between",
    "into", "through", "during", "before", "after", "above", "below", "to", "from", "up", "down", "in", "out", "on", "off", "over", "under",
    "again", "further", "then", "once", "here", "there", "when", "where", "why", "how", "all", "any", "both", "each", "few", "more", "most",
    "other", "some", "such", "no", "nor", "not", "only", "own", "same", "so", "than", "too", "very", "s", "t", "can", "will", "just", "don",
    "should", "now",
];

fn is_vowel(c: u8) -> bool {
    matches!(c, b'a' | b'e' | b'i' | b'o' | b'u' | b'y')
}

/// R1 / R2 của Porter2: vị trí sau phụ âm đầu tiên đứng sau một nguyên âm.
fn region_after(w: &[u8], from: usize) -> usize {
    let mut i = from;
    while i + 1 < w.len() {
        if is_vowel(w[i]) && !is_vowel(w[i + 1]) {
            return i + 2;
        }
        i += 1;
    }
    w.len()
}

fn ends(w: &[u8], s: &str) -> bool {
    w.ends_with(s.as_bytes())
}

/// Âm tiết ngắn kết thúc ở cuối `w`.
fn short_syllable_end(w: &[u8]) -> bool {
    let n = w.len();
    if n == 2 {
        return is_vowel(w[0]) && !is_vowel(w[1]);
    }
    n >= 3 && !is_vowel(w[n - 3]) && is_vowel(w[n - 2]) && !is_vowel(w[n - 1]) && !matches!(w[n - 1], b'w' | b'x' | b'Y')
}

fn has_vowel(w: &[u8]) -> bool {
    w.iter().any(|&c| is_vowel(c))
}

pub(crate) fn stem(word: &str) -> String {
    if word.len() <= 2 || !word.is_ascii() {
        return word.to_string();
    }
    match word {
        "skis" => return "ski".into(),
        "skies" => return "sky".into(),
        "dying" => return "die".into(),
        "lying" => return "lie".into(),
        "tying" => return "tie".into(),
        "idly" => return "idl".into(),
        "gently" => return "gentl".into(),
        "ugly" => return "ugli".into(),
        "early" => return "earli".into(),
        "only" => return "onli".into(),
        "singly" => return "singl".into(),
        "sky" | "news" | "howe" | "atlas" | "cosmos" | "bias" | "andes" => return word.into(),
        _ => {}
    }
    let mut w: Vec<u8> = word.trim_start_matches('\'').as_bytes().to_vec();
    // y đầu từ hoặc sau nguyên âm → Y (phụ âm)
    for i in 0..w.len() {
        if w[i] == b'y' && (i == 0 || is_vowel(w[i - 1])) {
            w[i] = b'Y';
        }
    }
    let r1 = if w.starts_with(b"gener") || w.starts_with(b"arsen") {
        5
    } else if w.starts_with(b"commun") {
        6
    } else {
        region_after(&w, 0)
    };
    let r2 = region_after(&w, r1);
    let in_r = |w: &[u8], suf: usize, r: usize| w.len() - suf >= r;

    // Step 0
    for s in ["'s'", "'s", "'"] {
        if ends(&w, s) {
            w.truncate(w.len() - s.len());
            break;
        }
    }
    // Step 1a
    if ends(&w, "sses") {
        w.truncate(w.len() - 2);
    } else if ends(&w, "ied") || ends(&w, "ies") {
        let base = w.len() - 3;
        w.truncate(base);
        w.extend_from_slice(if base > 1 { b"i" } else { b"ie" });
    } else if ends(&w, "us") || ends(&w, "ss") {
    } else if ends(&w, "s") {
        let n = w.len();
        if n >= 3 && has_vowel(&w[..n - 2]) {
            w.truncate(n - 1);
        }
    }
    let s = String::from_utf8_lossy(&w).to_string();
    if matches!(s.as_str(), "inning" | "outing" | "canning" | "herring" | "earring" | "proceed" | "exceed" | "succeed") {
        return s;
    }
    // Step 1b
    let mut step1b_done = false;
    if ends(&w, "eedly") || ends(&w, "eed") {
        let suf = if ends(&w, "eedly") { 5 } else { 3 };
        if in_r(&w, suf, r1) {
            w.truncate(w.len() - suf);
            w.extend_from_slice(b"ee");
        }
    } else {
        for suf in ["ingly", "edly", "ing", "ed"] {
            if ends(&w, suf) {
                let base = w.len() - suf.len();
                if has_vowel(&w[..base]) {
                    w.truncate(base);
                    step1b_done = true;
                }
                break;
            }
        }
    }
    if step1b_done {
        if ends(&w, "at") || ends(&w, "bl") || ends(&w, "iz") {
            w.push(b'e');
        } else if w.len() >= 2 && w[w.len() - 1] == w[w.len() - 2] && matches!(w[w.len() - 1], b'b' | b'd' | b'f' | b'g' | b'm' | b'n' | b'p' | b'r' | b't') {
            w.pop();
        } else if r1 >= w.len() && short_syllable_end(&w) {
            w.push(b'e');
        }
    }
    // Step 1c
    if w.len() > 2 && matches!(w[w.len() - 1], b'y' | b'Y') && !is_vowel(w[w.len() - 2]) {
        let n = w.len();
        w[n - 1] = b'i';
    }
    // Step 2
    const S2: &[(&str, &str)] = &[
        ("ization", "ize"), ("ational", "ate"), ("fulness", "ful"), ("ousness", "ous"), ("iveness", "ive"), ("tional", "tion"), ("biliti", "ble"),
        ("lessli", "less"), ("entli", "ent"), ("ation", "ate"), ("alism", "al"), ("aliti", "al"), ("ousli", "ous"), ("iviti", "ive"), ("fulli", "ful"),
        ("enci", "ence"), ("anci", "ance"), ("abli", "able"), ("izer", "ize"), ("ator", "ate"), ("alli", "al"), ("bli", "ble"), ("ogi", "og"), ("li", ""),
    ];
    for (suf, rep) in S2 {
        if ends(&w, suf) {
            if in_r(&w, suf.len(), r1) {
                let base = w.len() - suf.len();
                let ok = match *suf {
                    "ogi" => base > 0 && w[base - 1] == b'l',
                    "li" => base > 0 && matches!(w[base - 1], b'c' | b'd' | b'e' | b'g' | b'h' | b'k' | b'm' | b'n' | b'r' | b't'),
                    _ => true,
                };
                if ok {
                    w.truncate(base);
                    w.extend_from_slice(rep.as_bytes());
                }
            }
            break;
        }
    }
    // Step 3
    const S3: &[(&str, &str)] = &[("ational", "ate"), ("tional", "tion"), ("alize", "al"), ("icate", "ic"), ("iciti", "ic"), ("ative", ""), ("ical", "ic"), ("ness", ""), ("ful", "")];
    for (suf, rep) in S3 {
        if ends(&w, suf) {
            if in_r(&w, suf.len(), r1) && (*suf != "ative" || in_r(&w, suf.len(), r2)) {
                let base = w.len() - suf.len();
                w.truncate(base);
                w.extend_from_slice(rep.as_bytes());
            }
            break;
        }
    }
    // Step 4
    const S4: &[&str] = &["ement", "ance", "ence", "able", "ible", "ment", "ant", "ent", "ism", "ate", "iti", "ous", "ive", "ize", "ion", "al", "er", "ic"];
    for suf in S4 {
        if ends(&w, suf) {
            if in_r(&w, suf.len(), r2) {
                let base = w.len() - suf.len();
                if *suf != "ion" || (base > 0 && matches!(w[base - 1], b's' | b't')) {
                    w.truncate(base);
                }
            }
            break;
        }
    }
    // Step 5
    if ends(&w, "e") {
        let base = w.len() - 1;
        if base >= r2 || (base >= r1 && !short_syllable_end(&w[..base])) {
            w.truncate(base);
        }
    } else if ends(&w, "l") && w.len() - 1 >= r2 && w.len() >= 2 && w[w.len() - 2] == b'l' {
        w.pop();
    }
    String::from_utf8_lossy(&w).replace('Y', "y")
}

// ---------------------------------------------------------------------------------------------------------------
// Cấu hình, tách từ

#[derive(Clone, Copy, PartialEq)]
enum Config {
    English,
    Simple,
}

fn config_of(name: &str) -> Result<Config, String> {
    let n = name.trim().trim_matches('\'').to_ascii_lowercase();
    let n = n.strip_prefix("pg_catalog.").unwrap_or(&n);
    match n {
        "english" => Ok(Config::English),
        "simple" => Ok(Config::Simple),
        other => Err(format!("text search configuration \"{}\" does not exist", other)),
    }
}

/// Từ trong văn bản (vị trí bắt đầu, kết thúc theo byte).
fn words(text: &str) -> Vec<(usize, usize)> {
    let mut out = Vec::new();
    let mut start = None;
    for (i, c) in text.char_indices() {
        if c.is_alphanumeric() || c == '_' {
            if start.is_none() {
                start = Some(i);
            }
        } else if let Some(s) = start.take() {
            out.push((s, i));
        }
    }
    if let Some(s) = start {
        out.push((s, text.len()));
    }
    out
}

/// Lexeme của một từ; `None` = stopword.
fn normalize(word: &str, cfg: Config) -> Option<String> {
    let w = word.to_lowercase();
    match cfg {
        Config::Simple => Some(w),
        Config::English => {
            if STOPWORDS.contains(&w.as_str()) {
                None
            } else if w.chars().all(|c| c.is_ascii_digit()) {
                Some(w)
            } else {
                Some(stem(&w))
            }
        }
    }
}

// ---------------------------------------------------------------------------------------------------------------
// tsvector

/// lexeme → [(vị trí, trọng số 0=D..3=A)]
type Vector = BTreeMap<String, Vec<(u32, u8)>>;

fn to_vector(text: &str, cfg: Config) -> Vector {
    let mut v: Vector = BTreeMap::new();
    for (pos, (s, e)) in words(text).into_iter().enumerate() {
        if let Some(lex) = normalize(&text[s..e], cfg) {
            let p = (pos as u32 + 1).min(16383);
            let entry = v.entry(lex).or_default();
            if entry.len() < 256 {
                entry.push((p, 0));
            }
        }
    }
    v
}

fn quote_lexeme(l: &str) -> String {
    format!("'{}'", l.replace('\\', "\\\\").replace('\'', "''"))
}

fn weight_char(w: u8) -> &'static str {
    match w {
        3 => "A",
        2 => "B",
        1 => "C",
        _ => "",
    }
}

fn vector_text(v: &Vector) -> String {
    v.iter()
        .map(|(l, ps)| {
            if ps.is_empty() {
                quote_lexeme(l)
            } else {
                format!("{}:{}", quote_lexeme(l), ps.iter().map(|(p, w)| format!("{}{}", p, weight_char(*w))).collect::<Vec<_>>().join(","))
            }
        })
        .collect::<Vec<_>>()
        .join(" ")
}

/// Đọc văn bản tsvector (`'cat':2A 'run'`, cả lexeme không nháy).
fn parse_vector(s: &str) -> Option<Vector> {
    let mut v: Vector = BTreeMap::new();
    let b: Vec<char> = s.chars().collect();
    let mut i = 0;
    while i < b.len() {
        while i < b.len() && b[i].is_whitespace() {
            i += 1;
        }
        if i >= b.len() {
            break;
        }
        let mut lex = String::new();
        if b[i] == '\'' {
            i += 1;
            loop {
                if i >= b.len() {
                    return None;
                }
                if b[i] == '\'' {
                    if b.get(i + 1) == Some(&'\'') {
                        lex.push('\'');
                        i += 2;
                        continue;
                    }
                    i += 1;
                    break;
                }
                if b[i] == '\\' && i + 1 < b.len() {
                    lex.push(b[i + 1]);
                    i += 2;
                    continue;
                }
                lex.push(b[i]);
                i += 1;
            }
        } else {
            while i < b.len() && !b[i].is_whitespace() && b[i] != ':' {
                if matches!(b[i], '\'' | '&' | '|' | '!' | '(' | ')') {
                    return None;
                }
                lex.push(b[i]);
                i += 1;
            }
        }
        let entry = v.entry(lex).or_default();
        if i < b.len() && b[i] == ':' {
            i += 1;
            loop {
                let st = i;
                while i < b.len() && b[i].is_ascii_digit() {
                    i += 1;
                }
                if st == i {
                    return None;
                }
                let p: u32 = b[st..i].iter().collect::<String>().parse().ok()?;
                let mut w = 0u8;
                if i < b.len() && matches!(b[i].to_ascii_uppercase(), 'A' | 'B' | 'C' | 'D') {
                    w = match b[i].to_ascii_uppercase() {
                        'A' => 3,
                        'B' => 2,
                        'C' => 1,
                        _ => 0,
                    };
                    i += 1;
                }
                entry.push((p, w));
                if i < b.len() && b[i] == ',' {
                    i += 1;
                    continue;
                }
                break;
            }
            entry.sort();
            entry.dedup_by_key(|x| x.0);
        }
    }
    Some(v)
}

// ---------------------------------------------------------------------------------------------------------------
// tsquery

#[derive(Clone, Debug)]
enum Q {
    Term { lex: String, prefix: bool, weights: u8 },
    Not(Box<Q>),
    And(Box<Q>, Box<Q>),
    Or(Box<Q>, Box<Q>),
    Phrase(Box<Q>, Box<Q>, u32),
}

fn prec(q: &Q) -> u8 {
    match q {
        Q::Or(..) => 1,
        Q::And(..) => 2,
        Q::Phrase(..) => 3,
        Q::Not(_) => 4,
        Q::Term { .. } => 5,
    }
}

fn query_text(q: &Q) -> String {
    let sub = |c: &Q, parent: u8, right: bool| {
        let t = query_text(c);
        let p = prec(c);
        if p < parent || (right && p == parent && parent == 3) {
            format!("( {} )", t)
        } else {
            t
        }
    };
    match q {
        Q::Term { lex, prefix, weights } => {
            let mut s = quote_lexeme(lex);
            if *prefix || *weights != 0 {
                s.push(':');
                if *prefix {
                    s.push('*');
                }
                for (bit, c) in [(8, 'A'), (4, 'B'), (2, 'C'), (1, 'D')] {
                    if weights & bit != 0 {
                        s.push(c);
                    }
                }
            }
            s
        }
        Q::Not(a) => {
            let t = query_text(a);
            if prec(a) < 4 {
                format!("!( {} )", t)
            } else {
                format!("!{}", t)
            }
        }
        Q::And(a, b) => format!("{} & {}", sub(a, 2, false), sub(b, 2, true)),
        Q::Or(a, b) => format!("{} | {}", sub(a, 1, false), sub(b, 1, true)),
        Q::Phrase(a, b, d) => format!("{} {} {}", sub(a, 3, false), if *d == 1 { "<->".to_string() } else { format!("<{}>", d) }, sub(b, 3, true)),
    }
}

#[derive(Debug, Clone, PartialEq)]
enum Tok {
    Word(String, bool, u8, bool),
    And,
    Or,
    Not,
    Phrase(u32),
    Open,
    Close,
}

/// Tách tsquery thô. `quoted` của Word = lexeme viết trong nháy (không chuẩn hoá lại khi đọc văn bản tsquery).
fn tokenize_query(s: &str) -> Result<Vec<Tok>, String> {
    let b: Vec<char> = s.chars().collect();
    let mut out = Vec::new();
    let mut i = 0;
    let err = || format!("syntax error in tsquery: \"{}\"", s);
    while i < b.len() {
        let c = b[i];
        if c.is_whitespace() {
            i += 1;
            continue;
        }
        match c {
            '&' => {
                out.push(Tok::And);
                i += 1;
            }
            '|' => {
                out.push(Tok::Or);
                i += 1;
            }
            '!' => {
                out.push(Tok::Not);
                i += 1;
            }
            '(' => {
                out.push(Tok::Open);
                i += 1;
            }
            ')' => {
                out.push(Tok::Close);
                i += 1;
            }
            '<' => {
                let end = b[i..].iter().position(|&x| x == '>').ok_or_else(err)? + i;
                let inner: String = b[i + 1..end].iter().collect();
                let d = if inner == "-" { 1 } else { inner.trim().parse::<u32>().map_err(|_| err())? };
                out.push(Tok::Phrase(d));
                i = end + 1;
            }
            _ => {
                let mut w = String::new();
                let quoted = c == '\'';
                if quoted {
                    i += 1;
                    loop {
                        if i >= b.len() {
                            return Err(err());
                        }
                        if b[i] == '\'' {
                            if b.get(i + 1) == Some(&'\'') {
                                w.push('\'');
                                i += 2;
                                continue;
                            }
                            i += 1;
                            break;
                        }
                        w.push(b[i]);
                        i += 1;
                    }
                } else {
                    while i < b.len() && !b[i].is_whitespace() && !matches!(b[i], '&' | '|' | '!' | '(' | ')' | '<' | ':') {
                        w.push(b[i]);
                        i += 1;
                    }
                }
                let mut prefix = false;
                let mut weights = 0u8;
                if i < b.len() && b[i] == ':' {
                    i += 1;
                    while i < b.len() && matches!(b[i].to_ascii_uppercase(), '*' | 'A' | 'B' | 'C' | 'D') {
                        match b[i].to_ascii_uppercase() {
                            '*' => prefix = true,
                            'A' => weights |= 8,
                            'B' => weights |= 4,
                            'C' => weights |= 2,
                            _ => weights |= 1,
                        }
                        i += 1;
                    }
                }
                out.push(Tok::Word(w, prefix, weights, quoted));
            }
        }
    }
    Ok(out)
}

struct QParser<'a> {
    toks: &'a [Tok],
    i: usize,
    cfg: Option<Config>,
}

impl QParser<'_> {
    // or := and ('|' and)*
    fn or(&mut self) -> Result<Option<Q>, String> {
        let mut l = self.and()?;
        while self.toks.get(self.i) == Some(&Tok::Or) {
            self.i += 1;
            let r = self.and()?;
            l = match (l, r) {
                (Some(a), Some(b)) => Some(Q::Or(Box::new(a), Box::new(b))),
                (a, b) => a.or(b),
            };
        }
        Ok(l)
    }
    fn and(&mut self) -> Result<Option<Q>, String> {
        let mut l = self.phrase()?;
        while self.toks.get(self.i) == Some(&Tok::And) {
            self.i += 1;
            let r = self.phrase()?;
            l = match (l, r) {
                (Some(a), Some(b)) => Some(Q::And(Box::new(a), Box::new(b))),
                (a, b) => a.or(b),
            };
        }
        Ok(l)
    }
    fn phrase(&mut self) -> Result<Option<Q>, String> {
        let mut l = self.unary()?;
        while let Some(Tok::Phrase(d)) = self.toks.get(self.i).cloned() {
            self.i += 1;
            let r = self.unary()?;
            l = match (l, r) {
                (Some(a), Some(b)) => Some(Q::Phrase(Box::new(a), Box::new(b), d)),
                (a, b) => a.or(b),
            };
        }
        Ok(l)
    }
    fn unary(&mut self) -> Result<Option<Q>, String> {
        match self.toks.get(self.i).cloned() {
            Some(Tok::Not) => {
                self.i += 1;
                Ok(self.unary()?.map(|q| Q::Not(Box::new(q))))
            }
            Some(Tok::Open) => {
                self.i += 1;
                let q = self.or()?;
                if self.toks.get(self.i) != Some(&Tok::Close) {
                    return Err("syntax error in tsquery".to_string());
                }
                self.i += 1;
                Ok(q)
            }
            Some(Tok::Word(w, prefix, weights, _quoted)) => {
                self.i += 1;
                match self.cfg {
                    None => Ok(Some(Q::Term { lex: w, prefix, weights })),
                    Some(cfg) => {
                        // một toán hạng có thể sinh nhiều lexeme (`foo-bar`): nối bằng <->
                        let mut q: Option<Q> = None;
                        for (s, e) in words(&w) {
                            if let Some(l) = normalize(&w[s..e], cfg) {
                                let t = Q::Term { lex: l, prefix, weights };
                                q = Some(match q {
                                    None => t,
                                    Some(p) => Q::Phrase(Box::new(p), Box::new(t), 1),
                                });
                            }
                        }
                        Ok(q)
                    }
                }
            }
            _ => Err("syntax error in tsquery".to_string()),
        }
    }
}

fn parse_query(s: &str, cfg: Option<Config>) -> Result<Option<Q>, String> {
    let toks = tokenize_query(s)?;
    if toks.is_empty() {
        return Ok(None);
    }
    let mut p = QParser { toks: &toks, i: 0, cfg };
    let q = p.or()?;
    if p.i != toks.len() {
        return Err(format!("syntax error in tsquery: \"{}\"", s));
    }
    Ok(q)
}

fn plain_query(text: &str, cfg: Config, phrase: bool) -> Option<Q> {
    let mut q: Option<Q> = None;
    let mut gap = 1;
    for (s, e) in words(text) {
        match normalize(&text[s..e], cfg) {
            Some(l) => {
                let t = Q::Term { lex: l, prefix: false, weights: 0 };
                q = Some(match q {
                    None => t,
                    Some(p) if phrase => Q::Phrase(Box::new(p), Box::new(t), gap),
                    Some(p) => Q::And(Box::new(p), Box::new(t)),
                });
                gap = 1;
            }
            None => {
                if q.is_some() {
                    gap += 1;
                }
            }
        }
    }
    q
}

fn websearch_query(text: &str, cfg: Config) -> Option<Q> {
    // đơn vị: (phủ định, nối bằng OR với đơn vị trước, truy vấn)
    let mut units: Vec<(bool, bool, Q)> = Vec::new();
    let chars: Vec<char> = text.chars().collect();
    let mut i = 0;
    let mut pending_or = false;
    while i < chars.len() {
        let c = chars[i];
        if c.is_whitespace() {
            i += 1;
            continue;
        }
        let mut neg = false;
        if c == '-' {
            neg = true;
            i += 1;
        }
        if i < chars.len() && chars[i] == '"' {
            let end = chars[i + 1..].iter().position(|&x| x == '"').map(|p| p + i + 1).unwrap_or(chars.len());
            let phrase: String = chars[i + 1..end.min(chars.len())].iter().collect();
            if let Some(q) = plain_query(&phrase, cfg, true) {
                units.push((neg, pending_or, q));
                pending_or = false;
            }
            i = end + 1;
            continue;
        }
        let st = i;
        while i < chars.len() && !chars[i].is_whitespace() && chars[i] != '"' {
            i += 1;
        }
        let w: String = chars[st..i].iter().collect();
        if w.eq_ignore_ascii_case("or") && !neg {
            pending_or = !units.is_empty();
            continue;
        }
        if let Some(q) = plain_query(&w, cfg, true) {
            units.push((neg, pending_or, q));
            pending_or = false;
        }
    }
    let mut out: Option<Q> = None;
    for (neg, or, q) in units {
        let q = if neg { Q::Not(Box::new(q)) } else { q };
        out = Some(match out {
            None => q,
            Some(p) if or => Q::Or(Box::new(p), Box::new(q)),
            Some(p) => Q::And(Box::new(p), Box::new(q)),
        });
    }
    out
}

/// Vị trí khớp của một nút (cho toán tử cụm từ): `None` = không khớp; danh sách rỗng = khớp nhưng không có vị trí.
fn positions(q: &Q, v: &Vector) -> Option<Vec<u32>> {
    match q {
        Q::Term { lex, prefix, weights } => {
            let mut out = Vec::new();
            let mut found = false;
            let wanted = |w: u8| *weights == 0 || (*weights & (1 << w)) != 0;
            if *prefix {
                for (l, ps) in v.range(lex.clone()..) {
                    if !l.starts_with(lex.as_str()) {
                        break;
                    }
                    let sel: Vec<u32> = ps.iter().filter(|(_, w)| wanted(*w)).map(|(p, _)| *p).collect();
                    if !sel.is_empty() || (ps.is_empty() && *weights == 0) {
                        found = true;
                        out.extend(sel);
                    }
                }
            } else if let Some(ps) = v.get(lex) {
                let sel: Vec<u32> = ps.iter().filter(|(_, w)| wanted(*w)).map(|(p, _)| *p).collect();
                if !sel.is_empty() || (ps.is_empty() && *weights == 0) {
                    found = true;
                    out = sel;
                }
            }
            if !found {
                return None;
            }
            out.sort();
            out.dedup();
            Some(out)
        }
        Q::Phrase(a, b, d) => {
            let pa = positions(a, v)?;
            let pb = positions(b, v)?;
            if pa.is_empty() || pb.is_empty() {
                return Some(Vec::new());
            }
            let hits: Vec<u32> = pb.iter().copied().filter(|q| pa.iter().any(|p| p + d == *q)).collect();
            if hits.is_empty() {
                None
            } else {
                Some(hits)
            }
        }
        _ => matches(q, v).then(Vec::new),
    }
}

fn matches(q: &Q, v: &Vector) -> bool {
    match q {
        Q::Term { .. } | Q::Phrase(..) => positions(q, v).is_some(),
        Q::Not(a) => !matches(a, v),
        Q::And(a, b) => matches(a, v) && matches(b, v),
        Q::Or(a, b) => matches(a, v) || matches(b, v),
    }
}

fn query_terms(q: &Q, out: &mut Vec<(String, bool)>) {
    match q {
        Q::Term { lex, prefix, .. } => {
            if !out.iter().any(|(l, _)| l == lex) {
                out.push((lex.clone(), *prefix));
            }
        }
        Q::Not(_) => {}
        Q::And(a, b) | Q::Or(a, b) | Q::Phrase(a, b, _) => {
            query_terms(a, out);
            query_terms(b, out);
        }
    }
}

fn count_nodes(q: &Q) -> i64 {
    match q {
        Q::Term { .. } => 1,
        Q::Not(a) => 1 + count_nodes(a),
        Q::And(a, b) | Q::Or(a, b) | Q::Phrase(a, b, _) => 1 + count_nodes(a) + count_nodes(b),
    }
}

// ---------------------------------------------------------------------------------------------------------------
// ts_rank (theo tsrank.c)

const DEFAULT_WEIGHTS: [f32; 4] = [0.1, 0.2, 0.4, 1.0];

// Tính như C trong tsrank.c: biểu thức có hằng double được tính bằng double rồi mới gán về float4.
fn word_distance(d: i32) -> f32 {
    if d > 100 {
        1e-30
    } else {
        (1.0f64 / (1.005f64 + 0.05f64 * (((d as f32) as f64) / 1.5 - 2.0).exp())) as f32
    }
}

fn entry_positions<'a>(v: &'a Vector, term: &(String, bool)) -> Vec<&'a Vec<(u32, u8)>> {
    if term.1 {
        v.range(term.0.clone()..).take_while(|(l, _)| l.starts_with(term.0.as_str())).map(|(_, p)| p).collect()
    } else {
        v.get(&term.0).into_iter().collect()
    }
}

fn rank_or(w: &[f32; 4], v: &Vector, terms: &[(String, bool)]) -> f32 {
    let mut res = 0.0f32;
    for t in terms {
        for ps in entry_positions(v, t) {
            let posnull = vec![(0u32, 0u8)];
            let ps = if ps.is_empty() { &posnull } else { ps };
            let mut resj = 0.0f32;
            let mut wjm = -1.0f32;
            let mut jm = 0usize;
            for (j, (_, wt)) in ps.iter().enumerate() {
                let wp = w[*wt as usize];
                resj += wp / (((j + 1) * (j + 1)) as f32);
                if wp > wjm {
                    wjm = wp;
                    jm = j;
                }
            }
            res = (res as f64 + ((wjm + resj - wjm / (((jm + 1) * (jm + 1)) as f32)) as f64) / 1.64493406685f64) as f32;
        }
    }
    if !terms.is_empty() {
        res /= terms.len() as f32;
    }
    res
}

fn rank_and(w: &[f32; 4], v: &Vector, terms: &[(String, bool)]) -> f32 {
    if terms.len() < 2 {
        return rank_or(w, v, terms);
    }
    const MAXENTRYPOS: i32 = 1 << 14;
    let posnull = vec![(MAXENTRYPOS as u32 - 1, 0u8)];
    let mut res = -1.0f32;
    let mut seen: Vec<Option<Vec<(u32, u8)>>> = vec![None; terms.len()];
    for (i, t) in terms.iter().enumerate() {
        for ps in entry_positions(v, t) {
            let cur = if ps.is_empty() { posnull.clone() } else { ps.clone() };
            let cur_null = ps.is_empty();
            for k in 0..i {
                let Some(prev) = &seen[k] else { continue };
                let prev_null = prev.len() == 1 && prev[0].0 == MAXENTRYPOS as u32 - 1;
                for (pl, wl) in &cur {
                    for (pp, wp) in prev {
                        let mut dist = (*pl as i32 - *pp as i32).abs();
                        if dist != 0 || cur_null || prev_null {
                            if dist == 0 {
                                dist = MAXENTRYPOS;
                            }
                            let curw = ((w[*wl as usize] * w[*wp as usize] * word_distance(dist)) as f64).sqrt() as f32;
                            res = if res < 0.0 { curw } else { (1.0f64 - (1.0f64 - res as f64) * (1.0f64 - curw as f64)) as f32 };
                        }
                    }
                }
            }
            seen[i] = Some(cur);
        }
    }
    res
}

fn rank(w: &[f32; 4], v: &Vector, q: &Option<Q>, norm: i64) -> f32 {
    let Some(q) = q else { return 0.0 };
    if v.is_empty() {
        return 0.0;
    }
    let mut terms = Vec::new();
    query_terms(q, &mut terms);
    let mut res = match q {
        Q::And(..) | Q::Phrase(..) => rank_and(w, v, &terms),
        _ => rank_or(w, v, &terms),
    };
    if res < 0.0 {
        res = 1e-20;
    }
    let len: usize = v.values().map(|p| p.len().max(1)).sum();
    if norm & 1 != 0 {
        res /= ((len + 1) as f32).ln();
    }
    if norm & 2 != 0 && len > 0 {
        res /= len as f32;
    }
    if norm & 8 != 0 && !v.is_empty() {
        res /= v.len() as f32;
    }
    if norm & 16 != 0 {
        res /= ((v.len() + 1) as f32).ln();
    }
    if norm & 32 != 0 {
        res /= res + 1.0;
    }
    res
}

fn f32_text(f: f32) -> String {
    if f == 0.0 {
        return "0".to_string();
    }
    let s = format!("{}", f);
    if s.contains('e') {
        format!("{:e}", f).replace("e-", "e-").to_string()
    } else {
        s
    }
}

// ---------------------------------------------------------------------------------------------------------------
// Hàm (gọi từ fnlib)

pub(crate) fn is_fts_fn(name: &str) -> bool {
    matches!(
        name,
        "TO_TSVECTOR" | "TO_TSQUERY" | "PLAINTO_TSQUERY" | "PHRASETO_TSQUERY" | "WEBSEARCH_TO_TSQUERY" | "TS_RANK" | "TS_RANK_CD" | "TS_HEADLINE" | "SETWEIGHT"
            | "STRIP" | "NUMNODE" | "TSVECTOR_TO_ARRAY" | "ARRAY_TO_TSVECTOR" | "QUERYTREE" | "TS_MATCH" | "GET_CURRENT_TS_CONFIG"
    )
}

pub(crate) fn fts_fn_type(name: &str) -> Option<&'static str> {
    Some(match name {
        "TO_TSVECTOR" | "SETWEIGHT" | "STRIP" | "ARRAY_TO_TSVECTOR" => "tsvector",
        "TO_TSQUERY" | "PLAINTO_TSQUERY" | "PHRASETO_TSQUERY" | "WEBSEARCH_TO_TSQUERY" => "tsquery",
        "TS_RANK" | "TS_RANK_CD" => "real",
        "TS_HEADLINE" | "QUERYTREE" => "text",
        "NUMNODE" => "integer",
        "TSVECTOR_TO_ARRAY" => "text[]",
        "TS_MATCH" => "boolean",
        "GET_CURRENT_TS_CONFIG" => "regconfig",
        _ => return None,
    })
}

fn txt(v: &Value) -> Option<String> {
    match v {
        Value::Null => None,
        Value::Text(t) => Some(t.to_string()),
        Value::Int(i) => Some(i.to_string()),
        Value::Float(f) => Some(f.to_string()),
        Value::Bool(b) => Some(b.to_string()),
    }
}

fn fail(e: String) -> Option<Value> {
    super::errors::raise(e);
    Some(Value::Null)
}

/// Giá trị tsvector: văn bản tsvector, hoặc văn bản thường (→ to_tsvector cấu hình mặc định).
fn vector_arg(v: &Value) -> Option<Vector> {
    let t = txt(v)?;
    parse_vector(&t).filter(|_| t.trim_start().starts_with('\'') || t.trim().is_empty()).or_else(|| Some(to_vector(&t, Config::English)))
}

fn query_arg(v: &Value) -> Result<Option<Q>, String> {
    let Some(t) = txt(v) else { return Ok(None) };
    parse_query(&t, None)
}

/// `a @@ b` khi không phải jsonb @@ jsonpath. `None` = không phải full-text search.
pub(crate) fn atat(a: &Value, b: &Value) -> Option<Value> {
    let at = txt(a)?;
    let bt = txt(b)?;
    let looks_vector = |s: &str| s.trim_start().starts_with('\'') && parse_vector(s).is_some();
    let looks_query = |s: &str| s.contains('\'') && (s.contains('&') || s.contains('|') || s.contains('!') || s.contains("<") || s.trim_start().starts_with('\''));
    // jsonb @@ jsonpath: vế trái là JSON hợp lệ không phải chuỗi tsvector
    if serde_json::from_str::<serde_json::Value>(&at).is_ok() && !looks_vector(&at) {
        return None;
    }
    let (vec_side, q_side) = if looks_query(&at) && !looks_vector(&at) { (&bt, &at) } else { (&at, &bt) };
    let v = if looks_vector(vec_side) { parse_vector(vec_side)? } else { to_vector(vec_side, Config::English) };
    let q = if looks_query(q_side) || looks_vector(q_side) { parse_query(q_side, None).ok()? } else { plain_query(q_side, Config::English, false) };
    Some(Value::Bool(q.as_ref().is_some_and(|q| matches(q, &v))))
}

pub(crate) fn call_fts_fn(name: &str, args: &[Value]) -> Option<Value> {
    if args.iter().any(|a| a.is_null()) && !matches!(name, "GET_CURRENT_TS_CONFIG") {
        return Some(Value::Null);
    }
    // (cấu hình, văn bản) hoặc (văn bản)
    let cfg_text = |args: &[Value]| -> Result<(Config, String), String> {
        if args.len() >= 2 {
            Ok((config_of(&txt(&args[0]).unwrap_or_default())?, txt(&args[1]).unwrap_or_default()))
        } else {
            Ok((Config::English, args.first().and_then(txt).unwrap_or_default()))
        }
    };
    let qtext = |q: Option<Q>| Value::text(q.map(|q| query_text(&q)).unwrap_or_default());
    Some(match name {
        "GET_CURRENT_TS_CONFIG" => Value::text("english"),
        "TO_TSVECTOR" => match cfg_text(args) {
            Ok((cfg, t)) => Value::text(vector_text(&to_vector(&t, cfg))),
            Err(e) => return fail(e),
        },
        "TO_TSQUERY" => match cfg_text(args).and_then(|(cfg, t)| parse_query(&t, Some(cfg))) {
            Ok(q) => qtext(q),
            Err(e) => return fail(e),
        },
        "PLAINTO_TSQUERY" | "PHRASETO_TSQUERY" | "WEBSEARCH_TO_TSQUERY" => match cfg_text(args) {
            Ok((cfg, t)) => qtext(match name {
                "PLAINTO_TSQUERY" => plain_query(&t, cfg, false),
                "PHRASETO_TSQUERY" => plain_query(&t, cfg, true),
                _ => websearch_query(&t, cfg),
            }),
            Err(e) => return fail(e),
        },
        "TS_MATCH" => atat(&args[0], args.get(1)?).unwrap_or(Value::Null),
        "TS_RANK" | "TS_RANK_CD" => {
            // ts_rank([weights,] vector, query [, normalization])
            let (w, rest) = match txt(&args[0]) {
                Some(t) if t.trim_start().starts_with('{') => {
                    let vals: Vec<f32> = t.trim().trim_matches(['{', '}']).split(',').filter_map(|x| x.trim().parse().ok()).collect();
                    let mut w = DEFAULT_WEIGHTS;
                    if vals.len() == 4 {
                        w = [vals[0], vals[1], vals[2], vals[3]];
                    }
                    (w, &args[1..])
                }
                _ => (DEFAULT_WEIGHTS, args),
            };
            let v = vector_arg(rest.first()?)?;
            let q = match query_arg(rest.get(1)?) {
                Ok(q) => q,
                Err(e) => return fail(e),
            };
            let norm = rest.get(2).and_then(|n| n.as_i64()).unwrap_or(0);
            Value::text(f32_text(rank(&w, &v, &q, norm)))
        }
        "TS_HEADLINE" => {
            // ts_headline([config,] document, query [, options])
            let (cfg, doc, qv, opts) = if args.len() >= 3 && config_of(&txt(&args[0]).unwrap_or_default()).is_ok() && txt(&args[2]).is_some_and(|t| !t.contains('=')) {
                (config_of(&txt(&args[0]).unwrap_or_default()).ok()?, txt(&args[1])?, &args[2], args.get(3))
            } else {
                (Config::English, txt(&args[0])?, &args[1], args.get(2))
            };
            let q = match query_arg(qv) {
                Ok(q) => q,
                Err(e) => return fail(e),
            };
            Value::text(headline(&doc, &q, cfg, opts.and_then(txt).as_deref().unwrap_or("")))
        }
        "SETWEIGHT" => {
            let mut v = vector_arg(&args[0])?;
            let w = match txt(args.get(1)?)?.to_ascii_uppercase().as_str() {
                "A" => 3,
                "B" => 2,
                "C" => 1,
                "D" => 0,
                _ => return fail("unrecognized weight".to_string()),
            };
            for ps in v.values_mut() {
                for p in ps.iter_mut() {
                    p.1 = w;
                }
            }
            Value::text(vector_text(&v))
        }
        "STRIP" => {
            let mut v = vector_arg(&args[0])?;
            for ps in v.values_mut() {
                ps.clear();
            }
            Value::text(vector_text(&v))
        }
        "NUMNODE" => match query_arg(&args[0]) {
            Ok(q) => Value::Int(q.map(|q| count_nodes(&q)).unwrap_or(0)),
            Err(e) => return fail(e),
        },
        "QUERYTREE" => match query_arg(&args[0]) {
            Ok(q) => qtext(q),
            Err(e) => return fail(e),
        },
        "TSVECTOR_TO_ARRAY" => {
            let v = vector_arg(&args[0])?;
            Value::text(format!("{{{}}}", v.keys().map(|k| if k.chars().any(|c| matches!(c, ',' | '{' | '}' | '"' | ' ' | '\\')) { format!("\"{}\"", k.replace('"', "\\\"")) } else { k.clone() }).collect::<Vec<_>>().join(",")))
        }
        "ARRAY_TO_TSVECTOR" => {
            let t = txt(&args[0])?;
            let mut v: Vector = BTreeMap::new();
            for e in t.trim().trim_matches(['{', '}']).split(',') {
                let e = e.trim().trim_matches('"');
                if !e.is_empty() {
                    v.entry(e.to_string()).or_default();
                }
            }
            Value::text(vector_text(&v))
        }
        _ => return None,
    })
}

/// `ts_headline`: tô các từ khớp; văn bản dài hơn MaxWords thì lấy đoạn quanh từ khớp đầu tiên.
fn headline(doc: &str, q: &Option<Q>, cfg: Config, opts: &str) -> String {
    let mut start_sel = "<b>".to_string();
    let mut stop_sel = "</b>".to_string();
    let mut max_words = 35usize;
    let mut min_words = 15usize;
    let mut highlight_all = false;
    for o in opts.split(',') {
        let Some((k, v)) = o.split_once('=') else { continue };
        let v = v.trim().trim_matches('"').to_string();
        match k.trim().to_ascii_lowercase().as_str() {
            "startsel" => start_sel = v,
            "stopsel" => stop_sel = v,
            "maxwords" => max_words = v.parse().unwrap_or(35),
            "minwords" => min_words = v.parse().unwrap_or(15),
            "highlightall" => highlight_all = matches!(v.to_ascii_lowercase().as_str(), "true" | "on" | "1" | "t"),
            _ => {}
        }
    }
    let mut terms = Vec::new();
    if let Some(q) = q {
        query_terms(q, &mut terms);
    }
    let ws = words(doc);
    let hit = |s: usize, e: usize| normalize(&doc[s..e], cfg).is_some_and(|l| terms.iter().any(|(t, p)| if *p { l.starts_with(t.as_str()) } else { &l == t }));
    let (from, to) = if highlight_all || ws.len() <= max_words {
        (0, ws.len())
    } else {
        let first = ws.iter().position(|(s, e)| hit(*s, *e)).unwrap_or(0);
        let start = first.saturating_sub(min_words.min(max_words) / 3);
        (start, (start + max_words).min(ws.len()))
    };
    if ws.is_empty() {
        return doc.to_string();
    }
    let begin = if from == 0 { 0 } else { ws[from].0 };
    let end = if to == ws.len() { doc.len() } else { ws[to - 1].1 };
    let mut out = String::new();
    let mut last = begin;
    for &(s, e) in &ws[from..to] {
        out.push_str(&doc[last..s]);
        if hit(s, e) {
            out.push_str(&start_sel);
            out.push_str(&doc[s..e]);
            out.push_str(&stop_sel);
        } else {
            out.push_str(&doc[s..e]);
        }
        last = e;
    }
    out.push_str(&doc[last..end]);
    out
}

#[cfg(test)]
mod tests {
    use super::stem;
    #[test]
    fn porter2() {
        for (w, s) in [
            ("cats", "cat"), ("running", "run"), ("caresses", "caress"), ("ponies", "poni"), ("ties", "tie"), ("happily", "happili"), ("relational", "relat"),
            ("generously", "generous"), ("hopping", "hop"), ("hoping", "hope"), ("agreed", "agre"), ("national", "nation"), ("connection", "connect"),
            ("organization", "organ"), ("sky", "sky"), ("dying", "die"), ("cried", "cri"), ("gently", "gentl"), ("controlling", "control"),
        ] {
            assert_eq!(stem(w), s, "{}", w);
        }
    }
}
