//! Hàm ngày giờ (mục 18.3 của checklist) và múi giờ của phiên.
//!
//! Múi giờ phiên (`SET TIME ZONE 'Asia/Ho_Chi_Minh'`, `SET timezone = '+07'`, biến môi trường
//! `PGLITE_TIMEZONE`) mặc định là UTC, khi đó mọi hành vi như các bản trước. Giá trị timestamptz luôn
//! được lưu dạng thời điểm UTC (`2024-01-02T10:00:00.000Z`, như `now()` vẫn ghi) để so sánh và sắp xếp
//! giữa dữ liệu cũ và mới không đổi; múi giờ phiên chỉ áp dụng ở các chỗ PostgreSQL áp dụng:
//! `current_date`, `localtimestamp`, ép timestamptz → timestamp/date (gồm cả ghi vào cột TIMESTAMP,
//! `DEFAULT now()`), timestamp → timestamptz, `extract` / `date_trunc` / `to_char` / `age` trên
//! timestamptz, và text timestamptz ở đường wire (`2024-01-02 17:00:00+07`).
use super::temporal::{self, format_interval, parse_interval, Interval};
use crate::types::Value;
use std::cell::RefCell;

const US: i64 = 1_000_000;
const DAY_US: i64 = 86_400 * US;

thread_local! {
    /// Múi giờ của phiên đang chạy trên luồng này (rỗng = UTC). Executor đặt ở đầu mỗi câu lệnh.
    static SESSION_TZ: RefCell<String> = const { RefCell::new(String::new()) };
}

fn is_utc_name(z: &str) -> bool {
    matches!(z.trim().to_ascii_lowercase().as_str(), "" | "utc" | "gmt" | "z" | "etc/utc" | "etc/gmt" | "universal" | "zulu" | "ut")
}

pub(crate) fn set_session_tz(tz: &str) {
    let v = if is_utc_name(tz) { String::new() } else { tz.trim().to_string() };
    SESSION_TZ.with(|s| {
        let mut s = s.borrow_mut();
        if *s != v {
            *s = v;
        }
    });
}

pub(crate) fn session_is_utc() -> bool {
    SESSION_TZ.with(|s| s.borrow().is_empty())
}

fn session_tz() -> String {
    SESSION_TZ.with(|s| s.borrow().clone())
}

/// Múi giờ mặc định của phiên mới: `PGLITE_TIMEZONE` nếu hợp lệ, ngược lại UTC.
pub(crate) fn default_timezone() -> String {
    match std::env::var("PGLITE_TIMEZONE") {
        Ok(z) if valid_zone(&z) => z,
        _ => "UTC".to_string(),
    }
}

pub(crate) fn valid_zone(z: &str) -> bool {
    is_utc_name(z) || temporal::zone_offset_at(z, 0).is_some()
}

fn zone_off_utc(zone: &str, utc_us: i64) -> i64 {
    if is_utc_name(zone) {
        return 0;
    }
    temporal::zone_offset_at(zone, utc_us.div_euclid(US)).unwrap_or(0)
}

fn zone_off_local(zone: &str, wall_us: i64) -> i64 {
    if is_utc_name(zone) {
        return 0;
    }
    temporal::zone_offset_for_local(zone, wall_us.div_euclid(US)).unwrap_or(0)
}

fn session_off_utc(utc_us: i64) -> i64 {
    if session_is_utc() { 0 } else { zone_off_utc(&session_tz(), utc_us) }
}

fn session_off_local(wall_us: i64) -> i64 {
    if session_is_utc() { 0 } else { zone_off_local(&session_tz(), wall_us) }
}

// ---------------------------------------------------------------------------------------------
// Đọc / ghi timestamp

/// Timestamp đã đọc: giờ treo tường (micro giây tính như epoch), độ lệch nếu chuỗi có.
#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) struct Ts {
    pub wall: i64,
    pub offset: Option<i64>,
    pub has_time: bool,
    pub t_sep: bool,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) enum TsVal {
    At(Ts),
    PosInf,
    NegInf,
}

fn now_utc_us() -> i64 {
    let d = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default();
    (d.as_secs() as i64) * US + (d.subsec_micros() as i64)
}

fn days(y: i32, m: u32, d: u32) -> i64 {
    super::executor::days_from_civil(y, m, d)
}

fn civil(days: i64) -> (i32, u32, u32) {
    temporal::civil_from_days_pub(days)
}

fn dim(y: i32, m: u32) -> u32 {
    temporal::days_in_month_pub(y, m)
}

/// Phần lẻ giây `.123456` → micro giây.
fn frac_us(f: &str) -> Option<i64> {
    if f.is_empty() || !f.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    let mut d: String = f.chars().take(6).collect();
    while d.len() < 6 {
        d.push('0');
    }
    let mut v: i64 = d.parse().ok()?;
    // làm tròn chữ số thứ 7 như PostgreSQL
    if f.len() > 6 && f.as_bytes()[6] >= b'5' {
        v += 1;
    }
    Some(v)
}

/// Độ lệch dạng `Z`, `UTC`, `+07`, `+0700`, `+07:00`, `-05:30` → giây phía đông.
fn parse_offset(s: &str) -> Option<i64> {
    let t = s.trim();
    if t.eq_ignore_ascii_case("z") || t.eq_ignore_ascii_case("utc") || t.eq_ignore_ascii_case("gmt") {
        return Some(0);
    }
    let sign = match t.as_bytes().first()? {
        b'+' => 1,
        b'-' => -1,
        _ => return None,
    };
    let body = &t[1..];
    let parts: Vec<&str> = if body.contains(':') {
        body.split(':').collect()
    } else if body.len() == 4 {
        vec![&body[..2], &body[2..]]
    } else if body.len() == 6 {
        vec![&body[..2], &body[2..4], &body[4..]]
    } else {
        vec![body]
    };
    let h: i64 = parts.first()?.parse().ok()?;
    let m: i64 = parts.get(1).map(|x| x.parse().ok()).unwrap_or(Some(0))?;
    let sec: i64 = parts.get(2).map(|x| x.parse().ok()).unwrap_or(Some(0))?;
    if h > 15 || m > 59 || sec > 59 {
        return None;
    }
    Some(sign * (h * 3600 + m * 60 + sec))
}

/// Đọc chuỗi ngày giờ (dạng engine lưu và dạng PostgreSQL nhận): `2024-01-02`, `2024-01-02 10:00`,
/// `2024-01-02T10:00:00.5Z`, `2024-01-02 10:00:00+07`, các giá trị đặc biệt `epoch`, `now`, `today`,
/// `tomorrow`, `yesterday`, `infinity`, `-infinity`.
pub(crate) fn parse_ts(s: &str) -> Option<TsVal> {
    let t = s.trim();
    let l = t.to_ascii_lowercase();
    match l.as_str() {
        "infinity" | "+infinity" => return Some(TsVal::PosInf),
        "-infinity" => return Some(TsVal::NegInf),
        "epoch" => return Some(TsVal::At(Ts { wall: 0, offset: Some(0), has_time: true, t_sep: false })),
        "now" => return Some(TsVal::At(Ts { wall: now_utc_us(), offset: Some(0), has_time: true, t_sep: false })),
        "today" | "tomorrow" | "yesterday" => {
            let n = now_utc_us();
            let local = n + session_off_utc(n) * US;
            let d = local.div_euclid(DAY_US) + match l.as_str() { "tomorrow" => 1, "yesterday" => -1, _ => 0 };
            return Some(TsVal::At(Ts { wall: d * DAY_US, offset: None, has_time: false, t_sep: false }));
        }
        _ => {}
    }
    let b = t.as_bytes();
    // năm có thể dài hơn 4 chữ số
    let mut i = 0;
    while i < b.len() && b[i].is_ascii_digit() {
        i += 1;
    }
    if i < 4 || b.get(i) != Some(&b'-') {
        return None;
    }
    let y: i32 = t[..i].parse().ok()?;
    let rest = &t[i + 1..];
    let (mo_s, rest) = rest.split_once('-')?;
    let dlen = rest.bytes().take_while(|c| c.is_ascii_digit()).count();
    if dlen == 0 || mo_s.is_empty() || mo_s.len() > 2 || dlen > 2 {
        return None;
    }
    let mo: u32 = mo_s.parse().ok()?;
    let d: u32 = rest[..dlen].parse().ok()?;
    if !(1..=12).contains(&mo) || d < 1 || d > dim(y, mo) {
        return None;
    }
    let mut rest = &rest[dlen..];
    let mut wall = days(y, mo, d) * DAY_US;
    let mut has_time = false;
    let mut t_sep = false;
    let mut offset = None;
    if !rest.is_empty() {
        let sep = rest.as_bytes()[0];
        if sep == b'T' || sep == b't' {
            t_sep = true;
            rest = &rest[1..];
        } else if sep == b' ' {
            rest = rest.trim_start();
        } else {
            return None;
        }
        // giờ
        let tlen = rest.bytes().take_while(|c| c.is_ascii_digit() || *c == b':' || *c == b'.').count();
        if tlen > 0 {
            let clock = &rest[..tlen];
            let parts: Vec<&str> = clock.split(':').collect();
            if parts.len() < 2 || parts.len() > 3 {
                return None;
            }
            let h: i64 = parts[0].parse().ok()?;
            let mi: i64 = parts[1].parse().ok()?;
            let (sec, fr) = match parts.get(2) {
                Some(sp) => match sp.split_once('.') {
                    Some((a, f)) => (a.parse::<i64>().ok()?, frac_us(f)?),
                    None => (sp.parse::<i64>().ok()?, 0),
                },
                None => (0, 0),
            };
            if h > 24 || mi > 59 || sec > 60 || h == 24 && (mi > 0 || sec > 0 || fr > 0) {
                return None;
            }
            wall += (h * 3600 + mi * 60 + sec) * US + fr;
            has_time = true;
            rest = &rest[tlen..];
        }
        let r = rest.trim();
        if !r.is_empty() {
            offset = Some(parse_offset(r)?);
        }
    }
    Some(TsVal::At(Ts { wall, offset, has_time, t_sep }))
}

/// Thời điểm UTC (micro giây). Chuỗi không có độ lệch: `naive_in_session` → giờ địa phương của phiên,
/// ngược lại coi là UTC (cách engine vẫn hiểu timestamptz không có múi giờ).
fn instant(ts: &Ts, naive_in_session: bool) -> i64 {
    match ts.offset {
        Some(o) => ts.wall - o * US,
        None if naive_in_session => ts.wall - session_off_local(ts.wall) * US,
        None => ts.wall,
    }
}

fn split_us(us: i64) -> (i32, u32, u32, i64, i64, i64, i64) {
    let d = us.div_euclid(DAY_US);
    let rem = us.rem_euclid(DAY_US);
    let (y, m, dd) = civil(d);
    let secs = rem / US;
    (y, m, dd, secs / 3600, (secs % 3600) / 60, secs % 60, rem % US)
}

fn frac_text(us: i64) -> String {
    if us == 0 {
        String::new()
    } else {
        format!(".{}", format!("{:06}", us).trim_end_matches('0'))
    }
}

/// `YYYY-MM-DD HH:MM:SS[.ffffff]` (giờ treo tường).
pub(crate) fn fmt_naive(wall: i64) -> String {
    let (y, m, d, h, mi, s, f) = split_us(wall);
    format!("{:04}-{:02}-{:02} {:02}:{:02}:{:02}{}", y, m, d, h, mi, s, frac_text(f))
}

fn fmt_date(wall: i64) -> String {
    let (y, m, d, ..) = split_us(wall);
    format!("{:04}-{:02}-{:02}", y, m, d)
}

fn fmt_off(off: i64) -> String {
    let sign = if off < 0 { '-' } else { '+' };
    let a = off.abs();
    let (h, m, s) = (a / 3600, (a % 3600) / 60, a % 60);
    if s != 0 {
        format!("{}{:02}:{:02}:{:02}", sign, h, m, s)
    } else if m != 0 {
        format!("{}{:02}:{:02}", sign, h, m)
    } else {
        format!("{}{:02}", sign, h)
    }
}

/// Dạng lưu của timestamptz trong engine (giống `now()`): `YYYY-MM-DDTHH:MM:SS.mmmZ`.
pub(crate) fn fmt_canonical(utc: i64) -> String {
    let (y, m, d, h, mi, s, f) = split_us(utc);
    if f % 1000 == 0 {
        format!("{:04}-{:02}-{:02}T{:02}:{:02}:{:02}.{:03}Z", y, m, d, h, mi, s, f / 1000)
    } else {
        format!("{:04}-{:02}-{:02}T{:02}:{:02}:{:02}.{:06}Z", y, m, d, h, mi, s, f)
    }
}

/// Text PostgreSQL của timestamptz theo múi giờ phiên: `2024-01-02 17:00:00+07`.
pub(crate) fn fmt_pg_tz(utc: i64) -> String {
    let off = session_off_utc(utc);
    format!("{}{}", fmt_naive(utc + off * US), fmt_off(off))
}

/// Text wire của một giá trị timestamptz engine lưu (chuỗi không có độ lệch coi là UTC).
pub(crate) fn wire_tz(s: &str) -> Option<String> {
    match parse_ts(s)? {
        TsVal::PosInf => Some("infinity".into()),
        TsVal::NegInf => Some("-infinity".into()),
        TsVal::At(ts) => Some(fmt_pg_tz(instant(&ts, false))),
    }
}

// ---------------------------------------------------------------------------------------------
// Giá trị "hiện tại" theo phiên

/// `current_date` theo múi giờ phiên.
pub(crate) fn current_date() -> String {
    let n = now_utc_us();
    fmt_date(n + session_off_utc(n) * US)
}

/// `localtimestamp`: UTC → như bản cũ (chuỗi `...Z`); múi giờ khác → giờ địa phương không có hậu tố.
pub(crate) fn local_timestamp() -> Option<String> {
    if session_is_utc() {
        return None;
    }
    let n = now_utc_us();
    let w = n + session_off_utc(n) * US;
    let (y, m, d, h, mi, s, f) = split_us(w);
    Some(format!("{:04}-{:02}-{:02}T{:02}:{:02}:{:02}.{:03}", y, m, d, h, mi, s, f / 1000))
}

/// `current_time` / `localtime` (giờ địa phương `HH:MM:SS`) khi phiên không phải UTC.
pub(crate) fn local_time() -> Option<String> {
    if session_is_utc() {
        return None;
    }
    let n = now_utc_us();
    let (_, _, _, h, mi, s, _) = split_us(n + session_off_utc(n) * US);
    Some(format!("{:02}:{:02}:{:02}", h, mi, s))
}

// ---------------------------------------------------------------------------------------------
// Ép kiểu khi ghi / cast (chỉ khi phiên không phải UTC: UTC giữ nguyên chuỗi như bản cũ)

/// timestamptz → timestamp: chuỗi có độ lệch thành giờ địa phương của phiên.
pub(crate) fn tz_to_naive(s: &str) -> Option<String> {
    if session_is_utc() {
        return None;
    }
    let TsVal::At(ts) = parse_ts(s)? else { return None };
    ts.offset?;
    let w = instant(&ts, false) + session_off_utc(instant(&ts, false)) * US;
    let (y, m, d, h, mi, sec, f) = split_us(w);
    Some(if f % 1000 == 0 {
        format!("{:04}-{:02}-{:02}T{:02}:{:02}:{:02}.{:03}", y, m, d, h, mi, sec, f / 1000)
    } else {
        format!("{:04}-{:02}-{:02}T{:02}:{:02}:{:02}.{:06}", y, m, d, h, mi, sec, f)
    })
}

/// timestamptz → date theo giờ địa phương của phiên.
pub(crate) fn tz_to_date(s: &str) -> Option<String> {
    if session_is_utc() {
        return None;
    }
    let TsVal::At(ts) = parse_ts(s)? else { return None };
    ts.offset?;
    let u = instant(&ts, false);
    Some(fmt_date(u + session_off_utc(u) * US))
}

/// timestamp → timestamptz: chuỗi không có độ lệch là giờ địa phương của phiên → thời điểm UTC.
pub(crate) fn naive_to_tz(s: &str) -> Option<String> {
    if session_is_utc() {
        return None;
    }
    let TsVal::At(ts) = parse_ts(s)? else { return None };
    if ts.offset.is_some() {
        return None;
    }
    Some(fmt_canonical(instant(&ts, true)))
}

/// Giá trị đặc biệt khi ép kiểu timestamp / timestamptz: `epoch`, `now`, `today`, `tomorrow`, `yesterday`.
pub(crate) fn resolve_special(s: &str, with_tz: bool) -> Option<String> {
    let l = s.trim().to_ascii_lowercase();
    if !matches!(l.as_str(), "epoch" | "now" | "today" | "tomorrow" | "yesterday") {
        return None;
    }
    let TsVal::At(ts) = parse_ts(&l)? else { return None };
    Some(if with_tz { fmt_canonical(instant(&ts, true)) } else if l == "now" {
        let u = instant(&ts, false);
        fmt_naive(u + session_off_utc(u) * US)
    } else {
        fmt_naive(ts.wall)
    })
}

/// `generate_series(start, stop, step)` cho timestamp / timestamptz / date: (các giá trị, là timestamptz).
pub(crate) fn series(start: &str, stop: &str, step: &str, start_is_tz: Option<bool>) -> Result<(Vec<String>, bool), String> {
    let iv = parse_interval(step).ok_or_else(|| format!("invalid input syntax for type interval: \"{}\"", step))?;
    if iv.months == 0 && iv.days == 0 && iv.micros == 0 {
        return Err("step size cannot equal zero".to_string());
    }
    let (Some(TsVal::At(a)), Some(TsVal::At(b))) = (parse_ts(start), parse_ts(stop)) else {
        return Err(format!("invalid input syntax for type timestamp: \"{}\"", start));
    };
    let tz = start_is_tz.unwrap_or(a.offset.is_some());
    let (mut cur, end) = if tz { (instant(&a, true), instant(&b, true)) } else { (a.wall, b.wall) };
    let forward = add_interval(0, iv, 1) > 0;
    let mut out = Vec::new();
    while if forward { cur <= end } else { cur >= end } {
        if out.len() >= 10_000_000 {
            return Err("generate_series would return more than 10000000 rows".to_string());
        }
        out.push(if tz { fmt_canonical(cur) } else { fmt_naive(cur) });
        cur = if tz {
            // cộng theo giờ địa phương của phiên (như PostgreSQL cộng interval vào timestamptz)
            let w = cur + session_off_utc(cur) * US;
            let nw = add_interval(w, iv, 1);
            nw - session_off_local(nw) * US
        } else {
            add_interval(cur, iv, 1)
        };
    }
    Ok((out, tz))
}

// ---------------------------------------------------------------------------------------------
// Interval

fn iv_of(v: &Value) -> Option<Interval> {
    match v {
        Value::Text(t) => parse_interval(t),
        _ => None,
    }
}

fn add_interval(wall: i64, iv: Interval, sign: i64) -> i64 {
    let (y, m, d, ..) = split_us(wall);
    let tod = wall.rem_euclid(DAY_US);
    let total_m = y as i64 * 12 + (m as i64 - 1) + sign * iv.months;
    let ny = total_m.div_euclid(12) as i32;
    let nm = (total_m.rem_euclid(12) + 1) as u32;
    let nd = d.min(dim(ny, nm));
    days(ny, nm, nd) * DAY_US + tod + sign * (iv.days * DAY_US + iv.micros)
}

fn iv_seconds(iv: &Interval) -> f64 {
    // như extract(epoch from interval): năm = 365.25 ngày, tháng lẻ = 30 ngày
    let years = (iv.months / 12) as f64;
    let mons = (iv.months % 12) as f64;
    years * 365.25 * 86400.0 + mons * 30.0 * 86400.0 + iv.days as f64 * 86400.0 + iv.micros as f64 / 1e6
}

// ---------------------------------------------------------------------------------------------
// Bộ đánh giá hàm

pub(crate) struct Ctx<'a> {
    pub eval: &'a dyn Fn(&str) -> Value,
    pub type_of: &'a dyn Fn(&str) -> Option<String>,
}

#[derive(Clone, Copy, PartialEq, Debug)]
enum Kind {
    Tz,
    Naive,
    Date,
    Interval,
    Time,
}

fn kind_of(arg: &str, v: &Value, ctx: &Ctx) -> Option<Kind> {
    match (ctx.type_of)(arg).as_deref() {
        Some("timestamp with time zone") => return Some(Kind::Tz),
        Some("timestamp without time zone") => return Some(Kind::Naive),
        Some("date") => return Some(Kind::Date),
        Some("interval") => return Some(Kind::Interval),
        Some("time without time zone" | "time with time zone") => return Some(Kind::Time),
        _ => {}
    }
    let t = v.as_str();
    match parse_ts(&t) {
        Some(TsVal::At(ts)) if ts.offset.is_some() => Some(Kind::Tz),
        Some(TsVal::At(ts)) if ts.has_time => Some(Kind::Naive),
        Some(TsVal::At(_)) => Some(Kind::Date),
        Some(_) => Some(Kind::Naive),
        None if parse_interval(&t).is_some() => Some(Kind::Interval),
        None if parse_clock_us(&t).is_some() => Some(Kind::Time),
        None => None,
    }
}

/// `HH:MM[:SS[.f]]` → micro giây trong ngày.
fn parse_clock_us(s: &str) -> Option<i64> {
    let t = s.trim();
    let clock = t.split(|c| c == '+' || c == '-' || c == 'Z').next()?;
    let p: Vec<&str> = clock.trim().split(':').collect();
    if p.len() < 2 || p.len() > 3 {
        return None;
    }
    let h: i64 = p[0].parse().ok()?;
    let m: i64 = p[1].parse().ok()?;
    let (sec, f) = match p.get(2) {
        Some(sp) => match sp.split_once('.') {
            Some((a, f)) => (a.parse::<i64>().ok()?, frac_us(f)?),
            None => (sp.parse::<i64>().ok()?, 0),
        },
        None => (0, 0),
    };
    if h > 24 || m > 59 || sec > 60 {
        return None;
    }
    Some((h * 3600 + m * 60 + sec) * US + f)
}

/// Giờ treo tường dùng cho extract / date_trunc / to_char: timestamptz theo múi giờ `zone` (mặc định phiên).
fn wall_for(kind: Kind, v: &Value, zone: Option<&str>) -> Option<Result<i64, TsVal>> {
    match parse_ts(&v.as_str())? {
        TsVal::At(ts) => Some(Ok(match kind {
            Kind::Tz => {
                let u = instant(&ts, false);
                u + match zone {
                    Some(z) => zone_off_utc(z, u),
                    None => session_off_utc(u),
                } * US
            }
            _ => ts.wall,
        })),
        inf => Some(Err(inf)),
    }
}

fn num(v: f64) -> Value {
    if v.fract() == 0.0 && v.abs() < 9.0e15 { Value::Int(v as i64) } else { Value::Float(v) }
}

fn raise(msg: String) -> Value {
    crate::engine::errors::raise(msg);
    Value::Null
}

fn type_label(k: Kind) -> &'static str {
    match k {
        Kind::Tz => "timestamp with time zone",
        Kind::Naive => "timestamp without time zone",
        Kind::Date => "date",
        Kind::Interval => "interval",
        Kind::Time => "time without time zone",
    }
}

/// ISO: (năm ISO, tuần ISO, thứ ISO 1..7) của một ngày (số ngày từ epoch).
fn iso_week(day: i64) -> (i32, i64, i64) {
    let isodow = (day + 3).rem_euclid(7) + 1; // 1970-01-01 là thứ Năm
    let thursday = day - isodow + 4;
    let (ty, _, _) = civil(thursday);
    let jan1 = days(ty, 1, 1);
    ((ty), (thursday - jan1) / 7 + 1, isodow)
}

fn extract_field(field: &str, kind: Kind, v: &Value) -> Value {
    let f = field.trim().trim_matches('\'').trim_matches('"').to_ascii_lowercase();
    let f = match f.as_str() {
        "years" => "year",
        "months" | "mon" | "mons" => "month",
        "days" => "day",
        "hours" => "hour",
        "minutes" | "min" | "mins" => "minute",
        "seconds" | "sec" | "secs" => "second",
        "millisecond" | "msec" | "msecs" => "milliseconds",
        "microsecond" | "usec" | "usecs" => "microseconds",
        "centuries" => "century",
        "decades" => "decade",
        other => other,
    }
    .to_string();
    if kind == Kind::Interval {
        let Some(iv) = iv_of(v) else { return Value::Null };
        let tsec = iv.micros as f64 / 1e6;
        return match f.as_str() {
            "epoch" => num(iv_seconds(&iv)),
            "millennium" => Value::Int(iv.months / 12 / 1000),
            "century" => Value::Int(iv.months / 12 / 100),
            "decade" => Value::Int(iv.months / 12 / 10),
            "year" => Value::Int(iv.months / 12),
            "quarter" => Value::Int((iv.months % 12) / 3 + 1),
            "month" => Value::Int(iv.months % 12),
            "day" => Value::Int(iv.days),
            "hour" => Value::Int(iv.micros / (3600 * US)),
            "minute" => Value::Int((iv.micros / (60 * US)) % 60),
            "second" => num((tsec % 60.0 * 1e6).round() / 1e6),
            "milliseconds" => num(((iv.micros % (60 * US)) as f64 / 1000.0 * 1000.0).round() / 1000.0),
            "microseconds" => Value::Int(iv.micros % (60 * US)),
            _ => raise(format!("unit \"{}\" not supported for type interval", field.trim().trim_matches('\''))),
        };
    }
    if kind == Kind::Time {
        let Some(us) = parse_clock_us(&v.as_str()) else { return Value::Null };
        return match f.as_str() {
            "hour" => Value::Int(us / (3600 * US)),
            "minute" => Value::Int((us / (60 * US)) % 60),
            "second" => num((us % (60 * US)) as f64 / 1e6),
            "milliseconds" => num((us % (60 * US)) as f64 / 1000.0),
            "microseconds" => Value::Int(us % (60 * US)),
            "epoch" => num(us as f64 / 1e6),
            _ => raise(format!("unit \"{}\" not supported for type time without time zone", field.trim().trim_matches('\''))),
        };
    }
    let wall = match wall_for(kind, v, None) {
        Some(Ok(w)) => w,
        Some(Err(TsVal::PosInf)) => return Value::Float(f64::INFINITY),
        Some(Err(_)) => return Value::Float(f64::NEG_INFINITY),
        None => return Value::Null,
    };
    let (y, m, d, h, mi, s, fr) = split_us(wall);
    let day = wall.div_euclid(DAY_US);
    let (iy, iw, idow) = iso_week(day);
    let y64 = y as i64;
    match f.as_str() {
        "year" => Value::Int(y64),
        "isoyear" => Value::Int(iy as i64),
        "month" => Value::Int(m as i64),
        "day" => Value::Int(d as i64),
        "hour" if kind != Kind::Date => Value::Int(h),
        "minute" if kind != Kind::Date => Value::Int(mi),
        "second" if kind != Kind::Date => num(s as f64 + fr as f64 / 1e6),
        "milliseconds" if kind != Kind::Date => num(s as f64 * 1000.0 + fr as f64 / 1000.0),
        "microseconds" if kind != Kind::Date => Value::Int(s * US + fr),
        "dow" => Value::Int((day + 4).rem_euclid(7)),
        "isodow" => Value::Int(idow),
        "doy" => Value::Int(day - days(y, 1, 1) + 1),
        "week" => Value::Int(iw),
        "quarter" => Value::Int(((m - 1) / 3 + 1) as i64),
        "decade" => Value::Int(y64.div_euclid(10)),
        "century" => Value::Int(if y64 > 0 { (y64 + 99) / 100 } else { -((99 - y64) / 100) }),
        "millennium" => Value::Int(if y64 > 0 { (y64 + 999) / 1000 } else { -((999 - y64) / 1000) }),
        "julian" => {
            let jd = day + 2_440_588;
            if kind == Kind::Date { Value::Int(jd) } else { num(jd as f64 + (wall.rem_euclid(DAY_US)) as f64 / DAY_US as f64) }
        }
        "epoch" => {
            let utc = match (kind, parse_ts(&v.as_str())) {
                (Kind::Tz, Some(TsVal::At(ts))) => instant(&ts, false),
                _ => wall,
            };
            num(utc as f64 / 1e6)
        }
        "timezone" | "timezone_hour" | "timezone_minute" if kind == Kind::Tz => {
            let u = match parse_ts(&v.as_str()) {
                Some(TsVal::At(ts)) => instant(&ts, false),
                _ => 0,
            };
            let off = session_off_utc(u);
            Value::Int(match f.as_str() {
                "timezone" => off,
                "timezone_hour" => off / 3600,
                _ => (off % 3600) / 60,
            })
        }
        _ => raise(format!("unit \"{}\" not supported for type {}", field.trim().trim_matches('\''), type_label(kind))),
    }
}

fn trunc_wall(unit: &str, wall: i64) -> Option<i64> {
    let (y, m, d, h, mi, s, fr) = split_us(wall);
    let day0 = days(y, m, d) * DAY_US;
    Some(match unit {
        "microseconds" | "microsecond" => wall,
        "milliseconds" | "millisecond" => wall - fr % 1000,
        "second" | "seconds" => day0 + (h * 3600 + mi * 60 + s) * US,
        "minute" | "minutes" => day0 + (h * 3600 + mi * 60) * US,
        "hour" | "hours" => day0 + h * 3600 * US,
        "day" | "days" => day0,
        "week" | "weeks" => {
            let dd = wall.div_euclid(DAY_US);
            let isodow = (dd + 3).rem_euclid(7) + 1;
            (dd - (isodow - 1)) * DAY_US
        }
        "month" | "months" => days(y, m, 1) * DAY_US,
        "quarter" => days(y, (m - 1) / 3 * 3 + 1, 1) * DAY_US,
        "year" | "years" => days(y, 1, 1) * DAY_US,
        "decade" | "decades" => days(y - y.rem_euclid(10), 1, 1) * DAY_US,
        "century" | "centuries" => days(((y - 1).div_euclid(100)) * 100 + 1, 1, 1) * DAY_US,
        "millennium" | "millennia" => days(((y - 1).div_euclid(1000)) * 1000 + 1, 1, 1) * DAY_US,
        _ => return None,
    })
}

fn trunc_interval(unit: &str, iv: Interval) -> Option<Interval> {
    let mut r = iv;
    let cut = |r: &mut Interval, us: i64| r.micros -= r.micros % us;
    match unit {
        "microseconds" => {}
        "milliseconds" => cut(&mut r, 1000),
        "second" => cut(&mut r, US),
        "minute" => cut(&mut r, 60 * US),
        "hour" => cut(&mut r, 3600 * US),
        "day" => r.micros = 0,
        "month" => {
            r.days = 0;
            r.micros = 0;
        }
        "quarter" => {
            r.months -= r.months % 3;
            r.days = 0;
            r.micros = 0;
        }
        "year" | "decade" | "century" | "millennium" => {
            let div = match unit { "year" => 12, "decade" => 120, "century" => 1200, _ => 12000 };
            r.months -= r.months % div;
            r.days = 0;
            r.micros = 0;
        }
        _ => return None,
    }
    Some(r)
}

/// Ghi kết quả date_trunc / date_bin theo dạng của giá trị vào (giữ cách engine vẫn ghi).
fn render_like(kind: Kind, input: &str, wall: i64, zone: Option<&str>) -> String {
    match kind {
        Kind::Tz => {
            let off = match zone {
                Some(z) => zone_off_local(z, wall),
                None => session_off_local(wall),
            };
            let utc = wall - off * US;
            if session_is_utc() && zone.is_none() {
                // như bản cũ: giữ kiểu ISO `...Z` của chuỗi vào, kiểu khác thì `YYYY-MM-DD HH:MM:SS`
                if input.trim_end().ends_with('Z') || input.trim_end().ends_with('z') {
                    return fmt_canonical(utc);
                }
            }
            fmt_canonical(utc)
        }
        Kind::Date if wall.rem_euclid(DAY_US) == 0 => fmt_naive(wall),
        _ => {
            let t_sep = matches!(parse_ts(input), Some(TsVal::At(Ts { t_sep: true, .. })));
            let s = fmt_naive(wall);
            if t_sep { s.replacen(' ', "T", 1) } else { s }
        }
    }
}

fn eval_args(inner: &str, ctx: &Ctx) -> (Vec<String>, Vec<Value>) {
    let texts = super::executor::split_function_args(inner);
    let vals = texts.iter().map(|a| (ctx.eval)(a)).collect();
    (texts, vals)
}

const MONTHS: [&str; 12] = ["January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December"];
const DAYS: [&str; 7] = ["Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"];

fn ordinal(n: i64, upper: bool) -> &'static str {
    let s = if (11..=13).contains(&(n % 100)) {
        "th"
    } else {
        match n % 10 {
            1 => "st",
            2 => "nd",
            3 => "rd",
            _ => "th",
        }
    };
    if upper {
        match s {
            "st" => "ST",
            "nd" => "ND",
            "rd" => "RD",
            _ => "TH",
        }
    } else {
        s
    }
}

/// `to_char(timestamp|date|interval, fmt)` với các mẫu của PostgreSQL.
fn to_char(kind: Kind, v: &Value, fmt: &str) -> Option<String> {
    let (wall, utc, iv) = if kind == Kind::Interval {
        let iv = iv_of(v)?;
        (iv.days * DAY_US + iv.micros, 0, Some(iv))
    } else {
        let w = match wall_for(kind, v, None)? {
            Ok(w) => w,
            Err(TsVal::PosInf) => return Some("infinity".into()),
            Err(_) => return Some("-infinity".into()),
        };
        let u = match parse_ts(&v.as_str()) {
            Some(TsVal::At(ts)) if kind == Kind::Tz => instant(&ts, false),
            _ => w,
        };
        (w, u, None)
    };
    let (y, m, d, h, mi, s, fr) = split_us(wall);
    let (y, m, d) = match iv {
        Some(iv) => ((iv.months / 12) as i32, (iv.months % 12) as u32, iv.days as u32),
        None => (y, m, d),
    };
    let (h, mi, s, fr) = match iv {
        Some(iv) => {
            let t = iv.micros;
            (t / (3600 * US), (t / (60 * US)) % 60, (t / US) % 60, t % US)
        }
        None => (h, mi, s, fr),
    };
    let day = wall.div_euclid(DAY_US);
    let dow = (day + 4).rem_euclid(7) as usize;
    let (iy, iw, idow) = iso_week(day);
    let doy = if iv.is_some() { 0 } else { day - days(y, 1, 1) + 1 };
    let mut out = String::new();
    let f = fmt.as_bytes();
    let mut i = 0;
    let starts = |i: usize, p: &str| f.len() >= i + p.len() && &fmt[i..i + p.len()] == p;
    while i < f.len() {
        // chữ trong nháy kép
        if f[i] == b'"' {
            let mut j = i + 1;
            while j < f.len() && f[j] != b'"' {
                if f[j] == b'\\' && j + 1 < f.len() {
                    j += 1;
                }
                out.push(f[j] as char);
                j += 1;
            }
            i = j + 1;
            continue;
        }
        let mut fm = false;
        if starts(i, "FM") || starts(i, "fm") {
            fm = true;
            i += 2;
        }
        let pad = |n: i64, w: usize| if fm { n.to_string() } else { format!("{:0w$}", n, w = w) };
        let name = |full: &str, style: u8, short: bool| -> String {
            let base: String = if short { full.chars().take(3).collect() } else { full.to_string() };
            let s = match style {
                0 => base.to_uppercase(),
                1 => base,
                _ => base.to_lowercase(),
            };
            if !short && !fm { format!("{:<9}", s) } else { s }
        };
        // mẫu dài trước
        let pats: &[&str] = &[
            "HH24", "HH12", "HH", "MI", "SSSSS", "SSSS", "SS", "MS", "US", "A.M.", "P.M.", "a.m.", "p.m.", "AM", "PM", "am", "pm", "Y,YYY", "YYYY", "YYY", "YY",
            "IYYY", "IYY", "IY", "MONTH", "Month", "month", "MON", "Mon", "mon", "MM", "DAY", "Day", "day", "DY", "Dy", "dy", "DDD", "DD", "IDDD", "ID", "D",
            "WW", "IW", "W", "CC", "J", "Q", "TZH", "TZM", "TZ", "tz", "OF", "BC", "AD", "B.C.", "A.D.", "Y", "I",
        ];
        let Some(p) = pats.iter().find(|p| starts(i, p)) else {
            if f[i] == b'\\' && i + 1 < f.len() {
                out.push(f[i + 1] as char);
                i += 2;
            } else {
                let ch = fmt[i..].chars().next().unwrap_or(' ');
                out.push(ch);
                i += ch.len_utf8();
            }
            continue;
        };
        i += p.len();
        let h12 = if h % 12 == 0 { 12 } else { h % 12 };
        let (val, numeric): (String, Option<i64>) = match *p {
            "HH24" => (pad(h, 2), Some(h)),
            "HH12" | "HH" => (pad(h12, 2), Some(h12)),
            "MI" => (pad(mi, 2), Some(mi)),
            "SS" => (pad(s, 2), Some(s)),
            "SSSS" | "SSSSS" => ((h * 3600 + mi * 60 + s).to_string(), Some(h * 3600 + mi * 60 + s)),
            "MS" => (pad(fr / 1000, 3), Some(fr / 1000)),
            "US" => (pad(fr, 6), Some(fr)),
            "AM" | "PM" => ((if h < 12 { "AM" } else { "PM" }).into(), None),
            "am" | "pm" => ((if h < 12 { "am" } else { "pm" }).into(), None),
            "A.M." | "P.M." => ((if h < 12 { "A.M." } else { "P.M." }).into(), None),
            "a.m." | "p.m." => ((if h < 12 { "a.m." } else { "p.m." }).into(), None),
            "Y,YYY" => (format!("{},{:03}", y / 1000, y % 1000), None),
            "YYYY" => (pad(y as i64, 4), Some(y as i64)),
            "YYY" => (pad((y % 1000) as i64, 3), Some((y % 1000) as i64)),
            "YY" => (pad((y % 100) as i64, 2), Some((y % 100) as i64)),
            "Y" => ((y % 10).to_string(), Some((y % 10) as i64)),
            "IYYY" => (pad(iy as i64, 4), Some(iy as i64)),
            "IYY" => (pad((iy % 1000) as i64, 3), None),
            "IY" => (pad((iy % 100) as i64, 2), None),
            "I" => ((iy % 10).to_string(), None),
            "MONTH" => (name(MONTHS[(m.max(1) - 1) as usize], 0, false), None),
            "Month" => (name(MONTHS[(m.max(1) - 1) as usize], 1, false), None),
            "month" => (name(MONTHS[(m.max(1) - 1) as usize], 2, false), None),
            "MON" => (name(MONTHS[(m.max(1) - 1) as usize], 0, true), None),
            "Mon" => (name(MONTHS[(m.max(1) - 1) as usize], 1, true), None),
            "mon" => (name(MONTHS[(m.max(1) - 1) as usize], 2, true), None),
            "MM" => (pad(m as i64, 2), Some(m as i64)),
            "DAY" => (name(DAYS[dow], 0, false), None),
            "Day" => (name(DAYS[dow], 1, false), None),
            "day" => (name(DAYS[dow], 2, false), None),
            "DY" => (name(DAYS[dow], 0, true), None),
            "Dy" => (name(DAYS[dow], 1, true), None),
            "dy" => (name(DAYS[dow], 2, true), None),
            "DDD" => (pad(doy, 3), Some(doy)),
            "IDDD" => (pad((iw - 1) * 7 + idow, 3), Some((iw - 1) * 7 + idow)),
            "DD" => (pad(d as i64, 2), Some(d as i64)),
            "D" => ((dow + 1).to_string(), Some(dow as i64 + 1)),
            "ID" => (idow.to_string(), Some(idow)),
            "WW" => (pad((doy - 1) / 7 + 1, 2), Some((doy - 1) / 7 + 1)),
            "IW" => (pad(iw, 2), Some(iw)),
            "W" => (((d as i64 - 1) / 7 + 1).to_string(), Some((d as i64 - 1) / 7 + 1)),
            "CC" => {
                let c = if y > 0 { (y as i64 + 99) / 100 } else { -((99 - y as i64) / 100) };
                (pad(c, 2), Some(c))
            }
            "J" => ((day + 2_440_588).to_string(), Some(day + 2_440_588)),
            "Q" => ((((m.max(1) - 1) / 3) + 1).to_string(), None),
            "BC" | "AD" => ((if y > 0 { "AD" } else { "BC" }).into(), None),
            "B.C." | "A.D." => ((if y > 0 { "A.D." } else { "B.C." }).into(), None),
            "TZ" | "tz" => {
                let off = if kind == Kind::Tz { session_off_utc(utc) } else { 0 };
                let z = if kind != Kind::Tz {
                    String::new()
                } else if session_is_utc() {
                    "UTC".into()
                } else {
                    fmt_off(off)
                };
                (if *p == "tz" { z.to_lowercase() } else { z }, None)
            }
            "TZH" => {
                let off = if kind == Kind::Tz { session_off_utc(utc) } else { 0 };
                (format!("{}{:02}", if off < 0 { '-' } else { '+' }, (off.abs()) / 3600), None)
            }
            "TZM" => {
                let off = if kind == Kind::Tz { session_off_utc(utc) } else { 0 };
                (format!("{:02}", (off.abs() % 3600) / 60), None)
            }
            "OF" => (if kind == Kind::Tz { fmt_off(session_off_utc(utc)) } else { "+00".into() }, None),
            _ => (String::new(), None),
        };
        out.push_str(&val);
        // hậu tố thứ tự: DDth, DDTH
        if let Some(n) = numeric {
            if starts(i, "TH") || starts(i, "th") {
                out.push_str(ordinal(n, starts(i, "TH")));
                i += 2;
            }
        }
    }
    Some(out)
}

/// `to_timestamp(text, fmt)` / `to_date(text, fmt)`: giờ treo tường đọc được.
fn parse_with_format(text: &str, fmt: &str) -> Result<i64, String> {
    let t = text.as_bytes();
    let f = fmt.as_bytes();
    let (mut y, mut mo, mut d, mut h, mut mi, mut s, mut us) = (1i64, 1i64, 1i64, 0i64, 0i64, 0i64, 0i64);
    let mut pm: Option<bool> = None;
    let mut doy: Option<i64> = None;
    let (mut i, mut j) = (0usize, 0usize);
    let digits = |j: &mut usize, max: usize, tok: &str| -> Result<i64, String> {
        while *j < t.len() && t[*j] == b' ' {
            *j += 1;
        }
        let neg = *j < t.len() && t[*j] == b'-' && tok == "YYYY";
        if neg {
            *j += 1;
        }
        let st = *j;
        while *j < t.len() && t[*j].is_ascii_digit() && *j - st < max {
            *j += 1;
        }
        if *j == st {
            return Err(format!("invalid value \"{}\" for \"{}\"", String::from_utf8_lossy(&t[st..t.len().min(st + max)]), tok));
        }
        let v: i64 = std::str::from_utf8(&t[st..*j]).unwrap_or("0").parse().unwrap_or(0);
        Ok(if neg { -v } else { v })
    };
    while i < f.len() {
        let rest = &fmt[i..];
        if rest.starts_with("FM") || rest.starts_with("fm") {
            i += 2;
            continue;
        }
        let tok = ["YYYY", "HH24", "HH12", "DDD", "MONTH", "Month", "month", "MON", "Mon", "mon", "YY", "MM", "DD", "HH", "MI", "SS", "MS", "US", "AM", "PM", "am", "pm"]
            .iter()
            .find(|p| rest.starts_with(**p));
        match tok {
            Some(&p) => {
                i += p.len();
                match p {
                    "YYYY" => y = digits(&mut j, 6, p)?,
                    "YY" => {
                        let v = digits(&mut j, 2, p)?;
                        y = if v < 70 { 2000 + v } else { 1900 + v };
                    }
                    "MM" => mo = digits(&mut j, 2, p)?,
                    "DD" => d = digits(&mut j, 2, p)?,
                    "DDD" => doy = Some(digits(&mut j, 3, p)?),
                    "HH24" | "HH12" | "HH" => h = digits(&mut j, 2, p)?,
                    "MI" => mi = digits(&mut j, 2, p)?,
                    "SS" => s = digits(&mut j, 2, p)?,
                    "MS" => {
                        let st = j;
                        let v = digits(&mut j, 3, p)?;
                        us = v * 10i64.pow(3 - (j - st) as u32) * 1000;
                    }
                    "US" => {
                        let st = j;
                        let v = digits(&mut j, 6, p)?;
                        us = v * 10i64.pow(6 - (j - st) as u32);
                    }
                    "AM" | "PM" | "am" | "pm" => {
                        let w = std::str::from_utf8(&t[j..(j + 2).min(t.len())]).unwrap_or("").to_ascii_uppercase();
                        pm = Some(w == "PM");
                        j += 2;
                    }
                    _ => {
                        // tên tháng
                        let short = p.len() == 3;
                        let rest_t = std::str::from_utf8(&t[j..]).unwrap_or("").to_ascii_lowercase();
                        let found = MONTHS.iter().enumerate().find(|(_, n)| {
                            let key = if short { n[..3].to_ascii_lowercase() } else { n.to_ascii_lowercase() };
                            rest_t.starts_with(&key)
                        });
                        let Some((k, n)) = found else { return Err(format!("invalid value \"{}\" for \"{}\"", rest_t.split_whitespace().next().unwrap_or(""), p)) };
                        mo = k as i64 + 1;
                        j += if short { 3 } else { n.len() };
                    }
                }
            }
            None => {
                // ký tự phân cách: bỏ qua một ký tự không phải chữ số ở chuỗi vào
                i += 1;
                if j < t.len() && !t[j].is_ascii_alphanumeric() {
                    j += 1;
                } else if j < t.len() && f[i - 1] == t[j] {
                    j += 1;
                }
            }
        }
    }
    if let Some(p) = pm {
        if h == 12 {
            h = 0;
        }
        if p {
            h += 12;
        }
    }
    if let Some(dy) = doy {
        let base = days(y as i32, 1, 1) + dy - 1;
        return Ok(base * DAY_US + (h * 3600 + mi * 60 + s) * US + us);
    }
    if !(1..=12).contains(&mo) || d < 1 || d > dim(y as i32, mo as u32) as i64 {
        return Err(format!("date/time field value out of range: \"{}\"", text));
    }
    if h > 24 || mi > 59 || s > 60 {
        return Err(format!("date/time field value out of range: \"{}\"", text));
    }
    Ok(days(y as i32, mo as u32, d as u32) * DAY_US + (h * 3600 + mi * 60 + s) * US + us)
}

fn f64_of(v: &Value) -> Option<f64> {
    match v {
        Value::Int(i) => Some(*i as f64),
        Value::Float(f) => Some(*f),
        Value::Text(t) => t.trim().parse().ok(),
        _ => None,
    }
}

fn age(a: i64, b: i64) -> Interval {
    // như PostgreSQL: hiệu từng trường, mượn theo độ dài tháng
    let (y1, m1, d1, h1, mi1, s1, f1) = split_us(a);
    let (y2, m2, d2, h2, mi2, s2, f2) = split_us(b);
    let neg = a < b;
    let (mut fr, mut sec, mut min, mut hour, mut day, mut mon, mut year) =
        (f1 - f2, s1 - s2, mi1 - mi2, h1 - h2, d1 as i64 - d2 as i64, m1 as i64 - m2 as i64, (y1 - y2) as i64);
    if neg {
        fr = -fr;
        sec = -sec;
        min = -min;
        hour = -hour;
        day = -day;
        mon = -mon;
        year = -year;
    }
    while fr < 0 {
        fr += US;
        sec -= 1;
    }
    while sec < 0 {
        sec += 60;
        min -= 1;
    }
    while min < 0 {
        min += 60;
        hour -= 1;
    }
    while hour < 0 {
        hour += 24;
        day -= 1;
    }
    while day < 0 {
        let (yy, mm) = if neg { (y1, m1) } else { (y2, m2) };
        day += dim(yy, mm) as i64;
        mon -= 1;
    }
    while mon < 0 {
        mon += 12;
        year -= 1;
    }
    let sign = if neg { -1 } else { 1 };
    Interval { months: sign * (year * 12 + mon), days: sign * day, micros: sign * ((hour * 3600 + min * 60 + sec) * US + fr) }
}

/// Lời gọi hàm ngày giờ `s` (đã cắt khoảng trắng; `upper` là bản viết hoa). `None` nếu không phải.
pub(crate) fn try_eval(s: &str, upper: &str, ctx: &Ctx) -> Option<Value> {
    let open = s.find('(')?;
    if !s.ends_with(')') || !super::executor::is_fully_enclosed_in_parens(&s[open..]) {
        return None;
    }
    let name = upper[..open].trim();
    let inner = &s[open + 1..s.len() - 1];
    match name {
        "CLOCK_TIMESTAMP" | "STATEMENT_TIMESTAMP" | "TRANSACTION_TIMESTAMP" if inner.trim().is_empty() => {
            Some(Value::text(super::executor::get_current_timestamp()))
        }
        "TIMEOFDAY" if inner.trim().is_empty() => {
            let n = now_utc_us();
            let off = session_off_utc(n);
            let w = n + off * US;
            let (y, m, d, h, mi, sec, f) = split_us(w);
            let dow = (w.div_euclid(DAY_US) + 4).rem_euclid(7) as usize;
            let zone = if session_is_utc() { "UTC".to_string() } else { fmt_off(off) };
            Some(Value::text(format!("{} {} {:02} {:02}:{:02}:{:02}.{:06} {} {}", &DAYS[dow][..3], &MONTHS[(m - 1) as usize][..3], d, h, mi, sec, f, y, zone)))
        }
        "EXTRACT" => {
            let up_inner = inner.to_uppercase();
            let p = super::executor::find_top_level_keyword(&up_inner, "FROM")?;
            let field = inner[..p].trim();
            let src = inner[p + 4..].trim();
            let v = (ctx.eval)(src);
            if v.is_null() {
                return Some(Value::Null);
            }
            let k = kind_of(src, &v, ctx)?;
            Some(extract_field(field, k, &v))
        }
        "DATE_PART" => {
            let (texts, vals) = eval_args(inner, ctx);
            if vals.len() != 2 {
                return None;
            }
            if vals.iter().any(|v| v.is_null()) {
                return Some(Value::Null);
            }
            let k = kind_of(&texts[1], &vals[1], ctx)?;
            Some(extract_field(&vals[0].as_str(), k, &vals[1]))
        }
        "DATE_TRUNC" => {
            let (texts, vals) = eval_args(inner, ctx);
            if vals.len() < 2 || vals.len() > 3 {
                return None;
            }
            if vals.iter().any(|v| v.is_null()) {
                return Some(Value::Null);
            }
            let unit = vals[0].as_str().trim().to_ascii_lowercase();
            let k = kind_of(&texts[1], &vals[1], ctx)?;
            if k == Kind::Interval {
                return Some(match iv_of(&vals[1]).and_then(|iv| trunc_interval(&unit, iv)) {
                    Some(iv) => Value::text(format_interval(iv)),
                    None => raise(format!("unit \"{}\" not recognized for type interval", unit)),
                });
            }
            let zone = vals.get(2).map(|z| z.as_str().to_string());
            if let Some(z) = &zone {
                if !valid_zone(z) {
                    return Some(raise(format!("time zone \"{}\" not recognized", z)));
                }
            }
            let k = if k == Kind::Date { Kind::Date } else { k };
            let w = match wall_for(k, &vals[1], zone.as_deref())? {
                Ok(w) => w,
                Err(TsVal::PosInf) => return Some(Value::text("infinity")),
                Err(_) => return Some(Value::text("-infinity")),
            };
            Some(match trunc_wall(&unit, w) {
                Some(tw) => {
                    if k == Kind::Date && tw.rem_euclid(DAY_US) == 0 && !vals[1].as_str().contains(':') {
                        // như bản cũ với giá trị chỉ có ngày
                        Value::text(fmt_date(tw))
                    } else {
                        Value::text(render_like(k, &vals[1].as_str(), tw, zone.as_deref()))
                    }
                }
                None => raise(format!("unit \"{}\" not recognized for type {}", unit, type_label(k))),
            })
        }
        "TO_CHAR" => {
            let (texts, vals) = eval_args(inner, ctx);
            if vals.len() != 2 {
                return None;
            }
            if vals.iter().any(|v| v.is_null()) {
                return Some(Value::Null);
            }
            if matches!(vals[0], Value::Int(_) | Value::Float(_)) {
                return None; // to_char(số, fmt): hàm số
            }
            let k = kind_of(&texts[0], &vals[0], ctx)?;
            to_char(k, &vals[0], &vals[1].as_str()).map(Value::text)
        }
        "AGE" => {
            let (texts, vals) = eval_args(inner, ctx);
            if vals.is_empty() || vals.len() > 2 {
                return None;
            }
            if vals.iter().any(|v| v.is_null()) {
                return Some(Value::Null);
            }
            let w = |i: usize| -> Option<i64> {
                let k = kind_of(&texts[i], &vals[i], ctx)?;
                wall_for(k, &vals[i], None)?.ok()
            };
            let (a, b) = if vals.len() == 1 {
                let today = parse_ts(&current_date()).and_then(|t| if let TsVal::At(t) = t { Some(t.wall) } else { None })?;
                (today, w(0)?)
            } else {
                (w(0)?, w(1)?)
            };
            Some(Value::text(format_interval(age(a, b))))
        }
        "TO_TIMESTAMP" => {
            let (_, vals) = eval_args(inner, ctx);
            if vals.iter().any(|v| v.is_null()) {
                return Some(Value::Null);
            }
            match vals.len() {
                1 => {
                    let e = f64_of(&vals[0])?;
                    if e.is_infinite() {
                        return Some(Value::text(if e > 0.0 { "infinity" } else { "-infinity" }));
                    }
                    Some(Value::text(fmt_canonical((e * 1e6).round() as i64)))
                }
                2 => Some(match parse_with_format(&vals[0].as_str(), &vals[1].as_str()) {
                    Ok(w) => Value::text(fmt_canonical(w - session_off_local(w) * US)),
                    Err(e) => raise(e),
                }),
                _ => None,
            }
        }
        "TO_DATE" => {
            let (_, vals) = eval_args(inner, ctx);
            if vals.len() != 2 {
                return None;
            }
            if vals.iter().any(|v| v.is_null()) {
                return Some(Value::Null);
            }
            Some(match parse_with_format(&vals[0].as_str(), &vals[1].as_str()) {
                Ok(w) => Value::text(fmt_date(w)),
                Err(e) => raise(e),
            })
        }
        "MAKE_DATE" | "MAKE_TIMESTAMP" | "MAKE_TIMESTAMPTZ" | "MAKE_TIME" => {
            let (_, vals) = eval_args(inner, ctx);
            if vals.iter().any(|v| v.is_null()) {
                return Some(Value::Null);
            }
            let n = |i: usize| vals.get(i).and_then(f64_of);
            let (y, mo, d, h, mi, sec) = if name == "MAKE_TIME" {
                (2000.0, 1.0, 1.0, n(0)?, n(1)?, n(2)?)
            } else if name == "MAKE_DATE" {
                (n(0)?, n(1)?, n(2)?, 0.0, 0.0, 0.0)
            } else {
                (n(0)?, n(1)?, n(2)?, n(3)?, n(4)?, n(5)?)
            };
            let (yi, mi_, di) = (y as i32, mo as u32, d as u32);
            if name != "MAKE_TIME" && (!(1..=12).contains(&mi_) || di < 1 || di > dim(yi, mi_) || y == 0.0) {
                return Some(raise(format!("date field value out of range: {}-{:02}-{:02}", yi, mi_, di)));
            }
            if !(0.0..=24.0).contains(&h) || !(0.0..60.0).contains(&mi) || !(0.0..=60.0).contains(&sec) {
                return Some(raise(format!("time field value out of range: {}:{:02}:{:02}", h, mi, sec)));
            }
            let tod = ((h * 3600.0 + mi * 60.0) * 1e6) as i64 + (sec * 1e6).round() as i64;
            let wall = days(yi, mi_, di) * DAY_US + tod;
            Some(Value::text(match name {
                "MAKE_DATE" => fmt_date(wall),
                "MAKE_TIME" => {
                    let s = fmt_naive(wall);
                    s[11..].to_string()
                }
                "MAKE_TIMESTAMP" => fmt_naive(wall),
                _ => {
                    let off = match vals.get(6) {
                        Some(z) => {
                            let z = z.as_str();
                            if !valid_zone(&z) {
                                return Some(raise(format!("time zone \"{}\" not recognized", z)));
                            }
                            zone_off_local(&z, wall)
                        }
                        None => session_off_local(wall),
                    };
                    fmt_canonical(wall - off * US)
                }
            }))
        }
        "MAKE_INTERVAL" => {
            let texts = super::executor::split_function_args(inner);
            let order = ["years", "months", "weeks", "days", "hours", "mins", "secs"];
            let mut vals = [0f64; 7];
            for (pos, t) in texts.iter().enumerate() {
                let (key, expr) = match t.split_once("=>") {
                    Some((k, e)) => (k.trim().to_ascii_lowercase(), e.trim()),
                    None => (order.get(pos)?.to_string(), t.as_str()),
                };
                let idx = order.iter().position(|o| *o == key)?;
                let v = (ctx.eval)(expr);
                if v.is_null() {
                    return Some(Value::Null);
                }
                vals[idx] = f64_of(&v)?;
            }
            let iv = Interval {
                months: (vals[0] * 12.0 + vals[1]) as i64,
                days: (vals[2] * 7.0 + vals[3]) as i64,
                micros: ((vals[4] * 3600.0 + vals[5] * 60.0 + vals[6]) * 1e6).round() as i64,
            };
            Some(Value::text(format_interval(iv)))
        }
        "JUSTIFY_DAYS" | "JUSTIFY_HOURS" | "JUSTIFY_INTERVAL" => {
            let (_, vals) = eval_args(inner, ctx);
            let v = vals.first()?;
            if v.is_null() {
                return Some(Value::Null);
            }
            let mut iv = iv_of(v)?;
            if name != "JUSTIFY_DAYS" {
                iv.days += iv.micros / DAY_US;
                iv.micros %= DAY_US;
            }
            if name != "JUSTIFY_HOURS" {
                iv.months += iv.days / 30;
                iv.days %= 30;
            }
            if name == "JUSTIFY_INTERVAL" {
                // dấu thống nhất: tháng dương thì ngày/giờ không âm (mượn), và ngược lại
                if iv.months > 0 && (iv.days < 0 || iv.days == 0 && iv.micros < 0) {
                    iv.months -= 1;
                    iv.days += 30;
                } else if iv.months < 0 && (iv.days > 0 || iv.days == 0 && iv.micros > 0) {
                    iv.months += 1;
                    iv.days -= 30;
                }
                if iv.days > 0 && iv.micros < 0 {
                    iv.days -= 1;
                    iv.micros += DAY_US;
                } else if iv.days < 0 && iv.micros > 0 {
                    iv.days += 1;
                    iv.micros -= DAY_US;
                }
            } else if name == "JUSTIFY_HOURS" {
                if iv.days > 0 && iv.micros < 0 {
                    iv.days -= 1;
                    iv.micros += DAY_US;
                } else if iv.days < 0 && iv.micros > 0 {
                    iv.days += 1;
                    iv.micros -= DAY_US;
                }
            }
            Some(Value::text(format_interval(iv)))
        }
        "ISFINITE" => {
            let (_, vals) = eval_args(inner, ctx);
            let v = vals.first()?;
            if v.is_null() {
                return Some(Value::Null);
            }
            let t = v.as_str().trim().to_ascii_lowercase();
            Some(Value::Bool(!matches!(t.as_str(), "infinity" | "+infinity" | "-infinity")))
        }
        "DATE_ADD" | "DATE_SUBTRACT" => {
            let (texts, vals) = eval_args(inner, ctx);
            if vals.len() < 2 || vals.len() > 3 {
                return None;
            }
            if vals.iter().any(|v| v.is_null()) {
                return Some(Value::Null);
            }
            let iv = iv_of(&vals[1])?;
            let zone = vals.get(2).map(|z| z.as_str().to_string());
            let _ = &texts;
            let TsVal::At(ts) = parse_ts(&vals[0].as_str())? else { return Some(vals[0].clone()) };
            let u = instant(&ts, true);
            let off = |u: i64| match &zone {
                Some(z) => zone_off_utc(z, u),
                None => session_off_utc(u),
            };
            let wall = u + off(u) * US;
            let nw = add_interval(wall, iv, if name == "DATE_ADD" { 1 } else { -1 });
            let back = match &zone {
                Some(z) => zone_off_local(z, nw),
                None => session_off_local(nw),
            };
            Some(Value::text(fmt_canonical(nw - back * US)))
        }
        "DATE_BIN" => {
            let (texts, vals) = eval_args(inner, ctx);
            if vals.len() != 3 {
                return None;
            }
            if vals.iter().any(|v| v.is_null()) {
                return Some(Value::Null);
            }
            let stride = iv_of(&vals[0])?;
            if stride.months != 0 {
                return Some(raise("timestamps cannot be binned into intervals containing months or years".to_string()));
            }
            let st = stride.days * DAY_US + stride.micros;
            if st <= 0 {
                return Some(raise("stride must be greater than zero".to_string()));
            }
            let k = kind_of(&texts[1], &vals[1], ctx)?;
            let (TsVal::At(src), TsVal::At(org)) = (parse_ts(&vals[1].as_str())?, parse_ts(&vals[2].as_str())?) else { return None };
            let (a, o) = if k == Kind::Tz { (instant(&src, true), instant(&org, true)) } else { (src.wall, org.wall) };
            let binned = o + (a - o).div_euclid(st) * st;
            Some(Value::text(if k == Kind::Tz { fmt_canonical(binned) } else { fmt_naive(binned) }))
        }
        "OVERLAPS" => {
            let (texts, vals) = eval_args(inner, ctx);
            if vals.len() != 4 {
                return None;
            }
            let point = |i: usize, start: Option<i64>| -> Option<Option<i64>> {
                if vals[i].is_null() {
                    return Some(None);
                }
                if let (Some(s0), Some(iv)) = (start, iv_of(&vals[i]).filter(|_| parse_ts(&vals[i].as_str()).is_none())) {
                    return Some(Some(add_interval(s0, iv, 1)));
                }
                let k = kind_of(&texts[i], &vals[i], ctx)?;
                match parse_ts(&vals[i].as_str())? {
                    TsVal::At(ts) => Some(Some(if k == Kind::Tz { instant(&ts, true) } else { ts.wall })),
                    TsVal::PosInf => Some(Some(i64::MAX)),
                    TsVal::NegInf => Some(Some(i64::MIN)),
                }
            };
            let s1 = point(0, None)?;
            let e1 = point(1, s1)?;
            let s2 = point(2, None)?;
            let e2 = point(3, s2)?;
            let norm = |a: Option<i64>, b: Option<i64>| match (a, b) {
                (Some(x), Some(y)) if y < x => (Some(y), Some(x)),
                other => other,
            };
            let (s1, e1) = norm(s1, e1);
            let (s2, e2) = norm(s2, e2);
            // như overlaps_timestamp của PostgreSQL (có xử lý NULL)
            let lt = |a: Option<i64>, b: Option<i64>| a.zip(b).map(|(x, y)| x < y);
            let res = match (s1, s2) {
                (Some(a), Some(b)) if a > b => {
                    if lt(s1, e2) == Some(true) {
                        Some(true)
                    } else if e2.is_none() || s1.is_none() {
                        None
                    } else {
                        Some(false)
                    }
                }
                (Some(a), Some(b)) if a < b => {
                    if lt(s2, e1) == Some(true) {
                        Some(true)
                    } else if e1.is_none() {
                        None
                    } else {
                        Some(false)
                    }
                }
                (Some(_), Some(_)) => {
                    if e1.is_none() || e2.is_none() { None } else { Some(true) }
                }
                _ => None,
            };
            Some(match res {
                Some(b) => Value::Bool(b),
                None => Value::Null,
            })
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ctx_eval(e: &str) -> Value {
        let t = e.trim();
        if t.starts_with('\'') && t.ends_with('\'') {
            Value::text(&t[1..t.len() - 1])
        } else if let Ok(i) = t.parse::<i64>() {
            Value::Int(i)
        } else if let Ok(f) = t.parse::<f64>() {
            Value::Float(f)
        } else {
            Value::text(t)
        }
    }

    fn ev(s: &str) -> Value {
        let ctx = Ctx { eval: &ctx_eval, type_of: &|_| None };
        try_eval(s, &s.to_uppercase(), &ctx).unwrap_or(Value::text("<none>"))
    }

    #[test]
    fn functions() {
        set_session_tz("UTC");
        assert_eq!(ev("date_trunc('week', '2024-05-08 10:00:00')"), Value::text("2024-05-06 00:00:00"));
        assert_eq!(ev("date_trunc('quarter', '2024-05-08 10:00:00')"), Value::text("2024-04-01 00:00:00"));
        assert_eq!(ev("extract(isodow FROM '2024-05-05')"), Value::Int(7));
        assert_eq!(ev("extract(week FROM '2024-05-05')"), Value::Int(18));
        assert_eq!(ev("extract(week FROM '2021-01-01')"), Value::Int(53));
        assert_eq!(ev("extract(isoyear FROM '2021-01-01')"), Value::Int(2020));
        assert_eq!(ev("extract(doy FROM '2024-03-01')"), Value::Int(61));
        assert_eq!(ev("extract(epoch FROM '1 day')"), Value::Int(86400));
        assert_eq!(ev("to_char('2024-05-05 14:07:09', 'YYYY-MM-DD HH12:MI:SS AM Dy FMMonth DDth')"), Value::text("2024-05-05 02:07:09 PM Sun May 05th"));
        assert_eq!(ev("to_char('2024-05-05', 'Month')"), Value::text("May      "));
        assert_eq!(ev("age('2024-01-01', '2020-03-01')"), Value::text("3 years 10 mons"));
        assert_eq!(ev("age('2024-03-01', '2024-01-31')"), Value::text("1 mon 1 day"));
        assert_eq!(ev("make_interval(days => 3, hours => 2)"), Value::text("3 days 02:00:00"));
        assert_eq!(ev("justify_interval('1 mon -1 hour')"), Value::text("29 days 23:00:00"));
        assert_eq!(ev("to_timestamp(1700000000)"), Value::text("2023-11-14T22:13:20.000Z"));
        assert_eq!(ev("to_date('05 Jan 2024', 'DD Mon YYYY')"), Value::text("2024-01-05"));
        assert_eq!(ev("to_timestamp('2024-01-02 10:00', 'YYYY-MM-DD HH24:MI')"), Value::text("2024-01-02T10:00:00.000Z"));
        assert_eq!(ev("overlaps('2024-01-01', '2024-02-01', '2024-01-15', '2024-03-01')"), Value::Bool(true));
        assert_eq!(ev("date_bin('15 minutes', '2024-01-01 10:37:00', '2024-01-01 00:00:00')"), Value::text("2024-01-01 10:30:00"));
        // múi giờ phiên +7
        set_session_tz("Asia/Ho_Chi_Minh");
        assert_eq!(ev("extract(hour FROM '2024-01-01T20:00:00.000Z')"), Value::Int(3));
        assert_eq!(ev("date_trunc('day', '2024-01-01T20:00:00.000Z')"), Value::text("2024-01-01T17:00:00.000Z"));
        assert_eq!(tz_to_naive("2024-01-01T20:00:00.000Z").unwrap(), "2024-01-02T03:00:00.000");
        assert_eq!(naive_to_tz("2024-01-02 03:00:00").unwrap(), "2024-01-01T20:00:00.000Z");
        assert_eq!(wire_tz("2024-01-01T20:00:00.000Z").unwrap(), "2024-01-02 03:00:00+07");
        set_session_tz("UTC");
        assert_eq!(wire_tz("2024-01-01 10:00:00+07").unwrap(), "2024-01-01 03:00:00+00");
    }
}
