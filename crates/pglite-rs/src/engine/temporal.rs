//! Phép tính ngày giờ: `timestamp ± interval`, `date ± số ngày`, `date - date`, `timestamp - timestamp`,
//! và literal có kiểu (`INTERVAL '1 day'`, `TIMESTAMP '...'`, `DATE '...'`).
//!
//! Interval được biểu diễn bằng chuỗi dạng PostgreSQL (`1 day 02:00:00`); chuỗi chỉ được coi là
//! interval khi phép tính có một vế là ngày giờ, nên chuỗi văn bản thường không bị ảnh hưởng.
use super::executor::{days_from_civil, parse_datetime_components, DateTimeParts};
use crate::types::Value;

/// (tháng, ngày, micro giây)
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub(crate) struct Interval {
    pub months: i64,
    pub days: i64,
    pub micros: i64,
}

/// Đọc interval kiểu PostgreSQL: `1 day`, `2 hours 30 minutes`, `-1 week`, `3 days 04:05:06`,
/// `1 year 2 mons`, `90 seconds`, `00:30:00`, `@ 1 day ago`.
pub(crate) fn parse_interval(s: &str) -> Option<Interval> {
    let mut t = s.trim().trim_matches('\'').trim().to_lowercase();
    if t.is_empty() {
        return None;
    }
    let ago = t.ends_with(" ago");
    if ago {
        t.truncate(t.len() - 4);
    }
    let t = t.trim_start_matches('@').trim();
    let words: Vec<&str> = t.split_whitespace().collect();
    let mut iv = Interval::default();
    let mut i = 0;
    let mut any = false;
    while i < words.len() {
        let w = words[i];
        // hh:mm[:ss[.ffffff]]
        if w.contains(':') {
            let neg = w.starts_with('-');
            let parts: Vec<&str> = w.trim_start_matches(['-', '+']).split(':').collect();
            if parts.len() < 2 || parts.len() > 3 {
                return None;
            }
            let h: i64 = parts[0].parse().ok()?;
            let m: i64 = parts[1].parse().ok()?;
            let sec: f64 = parts.get(2).map_or(Some(0.0), |x| x.parse().ok())?;
            let mut micros = h * 3_600_000_000 + m * 60_000_000 + (sec * 1_000_000.0).round() as i64;
            if neg {
                micros = -micros;
            }
            iv.micros += micros;
            any = true;
            i += 1;
            continue;
        }
        // số và đơn vị có thể dính nhau: "2h", "10days"
        let (num_str, unit_inline) = match w.find(|c: char| c.is_alphabetic()) {
            Some(p) if p > 0 => (&w[..p], Some(&w[p..])),
            _ => (w, None),
        };
        let n: f64 = num_str.parse().ok()?;
        let unit = match unit_inline {
            Some(u) => u,
            None => {
                i += 1;
                *words.get(i)?
            }
        };
        let unit = unit.trim_end_matches(',');
        match unit {
            "millennium" | "millennia" | "millenniums" => iv.months += (n * 12000.0) as i64,
            "century" | "centuries" => iv.months += (n * 1200.0) as i64,
            "decade" | "decades" => iv.months += (n * 120.0) as i64,
            "y" | "yr" | "yrs" | "year" | "years" => iv.months += (n * 12.0).round() as i64,
            "mon" | "mons" | "month" | "months" => {
                iv.months += n.trunc() as i64;
                iv.days += (n.fract() * 30.0).round() as i64;
            }
            "w" | "week" | "weeks" => iv.days += (n * 7.0).round() as i64,
            "d" | "day" | "days" => {
                iv.days += n.trunc() as i64;
                iv.micros += (n.fract() * 86_400_000_000.0).round() as i64;
            }
            "h" | "hr" | "hrs" | "hour" | "hours" => iv.micros += (n * 3_600_000_000.0).round() as i64,
            "m" | "min" | "mins" | "minute" | "minutes" => iv.micros += (n * 60_000_000.0).round() as i64,
            "s" | "sec" | "secs" | "second" | "seconds" => iv.micros += (n * 1_000_000.0).round() as i64,
            "ms" | "msec" | "msecs" | "millisecond" | "milliseconds" => iv.micros += (n * 1000.0).round() as i64,
            "us" | "usec" | "usecs" | "microsecond" | "microseconds" => iv.micros += n.round() as i64,
            _ => return None,
        }
        any = true;
        i += 1;
    }
    if !any {
        return None;
    }
    if ago {
        iv = Interval { months: -iv.months, days: -iv.days, micros: -iv.micros };
    }
    Some(iv)
}

/// Định dạng như PostgreSQL (IntervalStyle = postgres): `1 year 2 mons 3 days 04:05:06`.
pub(crate) fn format_interval(iv: Interval) -> String {
    let mut parts = Vec::new();
    let (y, mo) = (iv.months / 12, iv.months % 12);
    // PostgreSQL chỉ dùng số ít khi đúng bằng 1: `1 day`, `-1 days`
    let plural = |n: i64, one: &str, many: &str| format!("{} {}", n, if n == 1 { one } else { many });
    if y != 0 {
        parts.push(plural(y, "year", "years"));
    }
    if mo != 0 {
        parts.push(plural(mo, "mon", "mons"));
    }
    if iv.days != 0 {
        parts.push(plural(iv.days, "day", "days"));
    }
    if iv.micros != 0 || parts.is_empty() {
        let neg = iv.micros < 0;
        let m = iv.micros.abs();
        let (h, rem) = (m / 3_600_000_000, m % 3_600_000_000);
        let (mi, rem) = (rem / 60_000_000, rem % 60_000_000);
        let (sec, frac) = (rem / 1_000_000, rem % 1_000_000);
        let mut time = format!("{}{:02}:{:02}:{:02}", if neg { "-" } else { "" }, h, mi, sec);
        if frac != 0 {
            let f = format!("{:06}", frac);
            time.push('.');
            time.push_str(f.trim_end_matches('0'));
        }
        parts.push(time);
    }
    parts.join(" ")
}

fn civil_from_days(days: i64) -> (i32, u32, u32) {
    let z = days + 719468;
    let era = (if z >= 0 { z } else { z - 146096 }) / 146097;
    let doe = (z - era * 146097) as u32;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    ((y + if m <= 2 { 1 } else { 0 }) as i32, m, d)
}

fn days_in_month(y: i32, m: u32) -> u32 {
    match m {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        _ => {
            if (y % 4 == 0 && y % 100 != 0) || y % 400 == 0 {
                29
            } else {
                28
            }
        }
    }
}

fn to_micros(dt: &DateTimeParts) -> i64 {
    let days = days_from_civil(dt.year, dt.month, dt.day);
    (days * 86_400 + dt.hour as i64 * 3600 + dt.minute as i64 * 60 + dt.second as i64) * 1_000_000 + dt.millisecond as i64 * 1000
}

/// Ghi lại thời điểm theo đúng kiểu định dạng của giá trị gốc (ISO `T...Z`, dạng có dấu cách, chỉ ngày).
fn format_like(template: &DateTimeParts, micros: i64, force_time: bool) -> String {
    let total_secs = micros.div_euclid(1_000_000);
    let frac_ms = (micros.rem_euclid(1_000_000) / 1000) as u32;
    let days = total_secs.div_euclid(86_400);
    let rem = total_secs.rem_euclid(86_400);
    let (y, m, d) = civil_from_days(days);
    let (h, mi, s) = (rem / 3600, (rem % 3600) / 60, rem % 60);
    if !template.has_time && !force_time && rem == 0 && frac_ms == 0 {
        return format!("{:04}-{:02}-{:02}", y, m, d);
    }
    if template.is_iso_z || template.separator == 'T' {
        let z = if template.is_iso_z { "Z" } else { "" };
        if template.millisecond != 0 || frac_ms != 0 || template.is_iso_z {
            return format!("{:04}-{:02}-{:02}T{:02}:{:02}:{:02}.{:03}{}", y, m, d, h, mi, s, frac_ms, z);
        }
        return format!("{:04}-{:02}-{:02}T{:02}:{:02}:{:02}{}", y, m, d, h, mi, s, z);
    }
    if frac_ms != 0 {
        return format!("{:04}-{:02}-{:02} {:02}:{:02}:{:02}.{:03}", y, m, d, h, mi, s, frac_ms);
    }
    format!("{:04}-{:02}-{:02} {:02}:{:02}:{:02}", y, m, d, h, mi, s)
}

/// Ngày giờ dạng chuỗi (có ngày tháng năm đầy đủ), không nhận chuỗi chỉ có giờ.
fn as_datetime(v: &Value) -> Option<DateTimeParts> {
    match v {
        Value::Text(t) => {
            let s = t.trim();
            let b = s.as_bytes();
            if b.len() >= 10 && b[4] == b'-' && b[7] == b'-' && b[..4].iter().all(|c| c.is_ascii_digit()) {
                parse_datetime_components(s)
            } else {
                None
            }
        }
        _ => None,
    }
}

fn as_interval(v: &Value) -> Option<Interval> {
    match v {
        Value::Text(t) => parse_interval(t),
        _ => None,
    }
}

fn shift(dt: &DateTimeParts, iv: Interval, sign: i64) -> String {
    // Tháng trước (theo lịch, kẹp ngày cuối tháng), rồi ngày và thời gian, như PostgreSQL.
    let total_m = dt.year as i64 * 12 + (dt.month as i64 - 1) + sign * iv.months;
    let y = total_m.div_euclid(12) as i32;
    let m = (total_m.rem_euclid(12) + 1) as u32;
    let d = dt.day.min(days_in_month(y, m));
    let moved = DateTimeParts { year: y, month: m, day: d, hour: dt.hour, minute: dt.minute, second: dt.second, millisecond: dt.millisecond, has_time: dt.has_time, is_iso_z: dt.is_iso_z, separator: dt.separator };
    let micros = to_micros(&moved) + sign * (iv.days * 86_400_000_000 + iv.micros);
    let force_time = iv.micros != 0;
    format_like(dt, micros, force_time)
}

/// `l <op> r` khi có ít nhất một vế là ngày giờ / interval. `None` nếu không phải phép tính ngày giờ.
pub(crate) fn temporal_arith(op: char, l: &Value, r: &Value) -> Option<Value> {
    if l.is_null() || r.is_null() {
        return None;
    }
    match op {
        '+' => {
            if let Some(dt) = as_datetime(l) {
                if let Some(iv) = as_interval(r) {
                    return Some(Value::text(shift(&dt, iv, 1)));
                }
                if let (Value::Int(n), false) = (r, dt.has_time) {
                    return Some(Value::text(shift(&dt, Interval { days: *n, ..Default::default() }, 1)));
                }
            }
            if let Some(dt) = as_datetime(r) {
                if let Some(iv) = as_interval(l) {
                    return Some(Value::text(shift(&dt, iv, 1)));
                }
                if let (Value::Int(n), false) = (l, dt.has_time) {
                    return Some(Value::text(shift(&dt, Interval { days: *n, ..Default::default() }, 1)));
                }
            }
            if let (Some(a), Some(b)) = (as_interval(l), as_interval(r)) {
                if as_datetime(l).is_none() && as_datetime(r).is_none() {
                    return Some(Value::text(format_interval(Interval { months: a.months + b.months, days: a.days + b.days, micros: a.micros + b.micros })));
                }
            }
            None
        }
        '-' => {
            let dl = as_datetime(l)?;
            if let Some(dr) = as_datetime(r) {
                if !dl.has_time && !dr.has_time {
                    // date - date = số ngày
                    return Some(Value::Int(days_from_civil(dl.year, dl.month, dl.day) - days_from_civil(dr.year, dr.month, dr.day)));
                }
                let diff = to_micros(&dl) - to_micros(&dr);
                let days = diff / 86_400_000_000;
                return Some(Value::text(format_interval(Interval { months: 0, days, micros: diff - days * 86_400_000_000 })));
            }
            if let Some(iv) = as_interval(r) {
                return Some(Value::text(shift(&dl, iv, -1)));
            }
            if let (Value::Int(n), false) = (r, dl.has_time) {
                return Some(Value::text(shift(&dl, Interval { days: *n, ..Default::default() }, -1)));
            }
            None
        }
        _ => None,
    }
}

/// Literal có kiểu: `INTERVAL '1 day'`, `TIMESTAMP '...'`, `TIMESTAMPTZ '...'`, `DATE '...'`, `TIME '...'`.
pub(crate) fn typed_literal(s: &str) -> Option<Value> {
    let t = s.trim();
    // Lọc nhanh: literal có kiểu luôn kết thúc bằng nháy đơn và bắt đầu bằng chữ cái
    if !t.ends_with('\'') || !t.as_bytes().first().map_or(false, |b| b.is_ascii_alphabetic()) {
        return None;
    }
    let up_prefix: String = t.chars().take(30).collect::<String>().to_uppercase();
    for kw in ["INTERVAL", "TIMESTAMPTZ", "TIMESTAMP WITH TIME ZONE", "TIMESTAMP WITHOUT TIME ZONE", "TIMESTAMP", "DATE", "TIME"] {
        if up_prefix.starts_with(kw) {
            let rest = t[kw.len()..].trim_start();
            if rest.len() >= 2 && rest.starts_with('\'') && rest.ends_with('\'') && !rest[1..rest.len() - 1].contains('\'') {
                let inner = &rest[1..rest.len() - 1];
                if kw == "INTERVAL" {
                    return parse_interval(inner).map(|iv| Value::text(format_interval(iv)));
                }
                return Some(Value::text(inner));
            }
            return None;
        }
    }
    None
}


fn valid_parts(dt: &DateTimeParts) -> bool {
    (1..=12).contains(&dt.month) && dt.day >= 1 && dt.day <= days_in_month(dt.year, dt.month) && dt.hour < 25 && dt.minute < 60 && dt.second < 61
}

/// Giá trị đặc biệt của PostgreSQL cho date/timestamp.
fn special_datetime(s: &str) -> Option<String> {
    let l = s.trim().to_lowercase();
    let now = super::executor::get_current_timestamp();
    Some(match l.as_str() {
        "epoch" => "1970-01-01 00:00:00".to_string(),
        "infinity" | "+infinity" => "infinity".to_string(),
        "-infinity" => "-infinity".to_string(),
        "now" => now,
        "today" => now[..10].to_string(),
        "tomorrow" | "yesterday" => {
            let dt = parse_datetime_components(&now[..10])?;
            let micros = to_micros(&dt) + if l == "tomorrow" { 86_400_000_000 } else { -86_400_000_000 };
            format_like(&dt, micros, false)
        }
        _ => return None,
    })
}

/// `'...'::date`: kiểm tra và chuẩn hoá về `YYYY-MM-DD`. `None` nếu không hợp lệ.
pub(crate) fn cast_date(s: &str) -> Option<String> {
    if let Some(sp) = special_datetime(s) {
        return Some(if sp.len() >= 10 && sp.as_bytes()[4] == b'-' { sp[..10].to_string() } else { sp });
    }
    let dt = parse_datetime_components(s.trim())?;
    let b = s.trim().as_bytes();
    if b.len() < 8 || !b[0].is_ascii_digit() || !valid_parts(&dt) {
        return None;
    }
    Some(format!("{:04}-{:02}-{:02}", dt.year, dt.month, dt.day))
}

/// Chuỗi đúng dạng ngày giờ nhưng trường nằm ngoài miền (`2024-13-01`, `2024-02-30`): PostgreSQL báo
/// `date/time field value out of range` (22008) thay vì lỗi cú pháp.
pub(crate) fn field_out_of_range(s: &str) -> bool {
    let t = s.trim();
    t.len() >= 8 && t.as_bytes()[0].is_ascii_digit() && special_datetime(t).is_none() && parse_datetime_components(t).is_some_and(|dt| !valid_parts(&dt))
}

/// Chuỗi có phải timestamp hợp lệ (ngày tháng có thật) không.
pub(crate) fn is_valid_timestamp(s: &str) -> bool {
    if special_datetime(s).is_some() {
        return true;
    }
    let t = s.trim();
    let b = t.as_bytes();
    if b.len() < 8 || !b[0].is_ascii_digit() {
        return false;
    }
    match parse_datetime_components(t) {
        Some(dt) => valid_parts(&dt),
        None => false,
    }
}

/// Phần giờ `HH:MM[:SS[.ffffff]]` kèm offset `Z` / `±HH[:MM]` (giây). Trả (giây trong ngày, phần lẻ, offset).
fn parse_clock(s: &str) -> Option<(i64, String, Option<i64>)> {
    let s = s.trim();
    let (clock, offset) = match s.find(|c: char| c == '+' || c == '-' || c == 'Z' || c == 'z') {
        Some(p) => {
            let off = &s[p..];
            let secs = if off.eq_ignore_ascii_case("z") {
                0
            } else {
                let sign = if off.starts_with('-') { -1 } else { 1 };
                let body = off[1..].trim();
                let (h, m) = match body.split_once(':') {
                    Some((h, m)) => (h.parse::<i64>().ok()?, m.parse::<i64>().ok()?),
                    None if body.len() == 4 => (body[..2].parse::<i64>().ok()?, body[2..].parse::<i64>().ok()?),
                    None => (body.parse::<i64>().ok()?, 0),
                };
                sign * (h * 3600 + m * 60)
            };
            (s[..p].trim(), Some(secs))
        }
        None => (s, None),
    };
    let parts: Vec<&str> = clock.split(':').collect();
    if parts.len() < 2 || parts.len() > 3 {
        return None;
    }
    let h: i64 = parts[0].parse().ok()?;
    let m: i64 = parts[1].parse().ok()?;
    let (sec, frac) = match parts.get(2) {
        Some(sp) => match sp.split_once('.') {
            Some((a, f)) => (a.parse::<i64>().ok()?, f.chars().take(6).collect::<String>()),
            None => (sp.parse::<i64>().ok()?, String::new()),
        },
        None => (0, String::new()),
    };
    if !frac.chars().all(|c| c.is_ascii_digit()) || h > 24 || m > 59 || sec > 60 {
        return None;
    }
    Some((h * 3600 + m * 60 + sec, frac.trim_end_matches('0').to_string(), offset))
}

fn fmt_clock(secs: i64, frac: &str) -> String {
    let f = if frac.is_empty() { String::new() } else { format!(".{}", frac) };
    format!("{:02}:{:02}:{:02}{}", secs / 3600, (secs % 3600) / 60, secs % 60, f)
}

fn fmt_offset(off: i64) -> String {
    let sign = if off < 0 { '-' } else { '+' };
    let a = off.abs();
    if a % 3600 == 0 {
        format!("{}{:02}", sign, a / 3600)
    } else {
        format!("{}{:02}:{:02}", sign, a / 3600, (a % 3600) / 60)
    }
}

/// Text PostgreSQL của một giá trị timestamp (`tz = false`) / timestamptz (quy về UTC, phiên đặt
/// TimeZone = UTC) từ chuỗi engine lưu (`2024-01-02T10:00:00.5Z`, `2024-01-02 10:00+07`, `2024-01-02`).
pub(crate) fn wire_timestamp(s: &str, tz: bool) -> Option<String> {
    let t = s.trim();
    if matches!(t.to_lowercase().as_str(), "infinity" | "-infinity") {
        return Some(t.to_lowercase());
    }
    let b = t.as_bytes();
    if b.len() < 10 || b[4] != b'-' || b[7] != b'-' {
        return None;
    }
    let y: i32 = t[..4].parse().ok()?;
    let mo: u32 = t[5..7].parse().ok()?;
    let d: u32 = t[8..10].parse().ok()?;
    let rest = t[10..].trim_start_matches(['T', 't', ' ']);
    let (secs, frac, offset) = if rest.is_empty() { (0, String::new(), None) } else { parse_clock(rest)? };
    if !tz {
        return Some(format!("{:04}-{:02}-{:02} {}", y, mo, d, fmt_clock(secs, &frac)));
    }
    let total = days_from_civil(y, mo, d) * 86_400 + secs - offset.unwrap_or(0);
    let (yy, mm, dd) = civil_from_days(total.div_euclid(86_400));
    Some(format!("{:04}-{:02}-{:02} {}+00", yy, mm, dd, fmt_clock(total.rem_euclid(86_400), &frac)))
}

/// Text PostgreSQL của time / timetz.
pub(crate) fn wire_time(s: &str, tz: bool) -> Option<String> {
    let (secs, frac, offset) = parse_clock(s)?;
    let mut out = fmt_clock(secs, &frac);
    if tz {
        out.push_str(&fmt_offset(offset.unwrap_or(0)));
    }
    Some(out)
}

#[cfg(test)]
mod wire_tests {
    use super::*;

    #[test]
    fn wire_formats() {
        assert_eq!(wire_timestamp("2024-01-02 10:00+07", true).unwrap(), "2024-01-02 03:00:00+00");
        assert_eq!(wire_timestamp("2024-01-02T10:00:00.500Z", false).unwrap(), "2024-01-02 10:00:00.5");
        assert_eq!(wire_timestamp("2024-01-02", true).unwrap(), "2024-01-02 00:00:00+00");
        assert_eq!(wire_timestamp("2024-01-01 02:00:00+05:30", true).unwrap(), "2023-12-31 20:30:00+00");
        assert_eq!(wire_time("10:00+07", true).unwrap(), "10:00:00+07");
        assert_eq!(wire_time("10:11", false).unwrap(), "10:11:00");
    }
}

/// Quy tắc giờ của một múi giờ (bảng nhúng các vùng thường dùng; không có tzdata đầy đủ).
#[derive(Clone, Copy)]
enum ZoneRule {
    Fixed(i64),
    /// EU: giờ mùa hè từ CN cuối tháng 3 01:00 UTC tới CN cuối tháng 10 01:00 UTC
    Eu(i64),
    /// Mỹ/Canada: CN thứ 2 tháng 3 02:00 giờ chuẩn tới CN đầu tháng 11 02:00 giờ mùa hè
    Us(i64),
    /// Đông nam Úc: CN đầu tháng 10 02:00 giờ chuẩn tới CN đầu tháng 4 03:00 giờ mùa hè
    AuSouth(i64),
    /// New Zealand: CN cuối tháng 9 02:00 giờ chuẩn tới CN đầu tháng 4 03:00 giờ mùa hè
    Nz(i64),
}

const H: i64 = 3600;

fn zone_rule(zone: &str) -> Option<ZoneRule> {
    use ZoneRule::*;
    let z = zone.trim();
    let lower = z.to_ascii_lowercase();
    let fixed: &[(&str, i64)] = &[
        ("utc", 0), ("gmt", 0), ("z", 0), ("ut", 0), ("zulu", 0), ("etc/utc", 0), ("etc/gmt", 0), ("etc/universal", 0), ("universal", 0),
        ("asia/ho_chi_minh", 7 * H), ("asia/saigon", 7 * H), ("asia/bangkok", 7 * H), ("asia/jakarta", 7 * H), ("asia/phnom_penh", 7 * H),
        ("asia/vientiane", 7 * H), ("ict", 7 * H), ("asia/singapore", 8 * H), ("asia/shanghai", 8 * H), ("asia/hong_kong", 8 * H),
        ("asia/taipei", 8 * H), ("asia/manila", 8 * H), ("asia/kuala_lumpur", 8 * H), ("australia/perth", 8 * H), ("prc", 8 * H),
        ("hkt", 8 * H), ("sgt", 8 * H), ("asia/tokyo", 9 * H), ("asia/seoul", 9 * H), ("jst", 9 * H), ("kst", 9 * H),
        ("asia/kolkata", 5 * H + 1800), ("asia/calcutta", 5 * H + 1800), ("ist", 5 * H + 1800), ("asia/kathmandu", 5 * H + 2700),
        ("asia/dhaka", 6 * H), ("asia/yangon", 6 * H + 1800), ("asia/karachi", 5 * H), ("asia/dubai", 4 * H), ("asia/riyadh", 3 * H),
        ("europe/moscow", 3 * H), ("europe/istanbul", 3 * H), ("msk", 3 * H), ("australia/brisbane", 10 * H), ("australia/darwin", 9 * H + 1800),
        ("america/phoenix", -7 * H), ("pacific/honolulu", -10 * H), ("hst", -10 * H), ("america/sao_paulo", -3 * H),
        ("america/mexico_city", -6 * H), ("america/bogota", -5 * H), ("america/lima", -5 * H), ("africa/cairo", 2 * H),
        ("africa/johannesburg", 2 * H), ("africa/lagos", H), ("africa/nairobi", 3 * H),
        ("est", -5 * H), ("edt", -4 * H), ("cst", -6 * H), ("cdt", -5 * H), ("mst", -7 * H), ("mdt", -6 * H), ("pst", -8 * H), ("pdt", -7 * H),
        ("cet", H), ("cest", 2 * H), ("eet", 2 * H), ("eest", 3 * H), ("wet", 0), ("west", H), ("bst", H), ("aest", 10 * H), ("aedt", 11 * H),
    ];
    if let Some((_, o)) = fixed.iter().find(|(n, _)| *n == lower) {
        return Some(Fixed(*o));
    }
    let eu: &[(&str, i64)] = &[
        ("europe/london", 0), ("europe/dublin", 0), ("europe/lisbon", 0), ("gb", 0), ("europe/paris", H), ("europe/berlin", H),
        ("europe/madrid", H), ("europe/rome", H), ("europe/amsterdam", H), ("europe/brussels", H), ("europe/vienna", H),
        ("europe/stockholm", H), ("europe/oslo", H), ("europe/copenhagen", H), ("europe/warsaw", H), ("europe/prague", H),
        ("europe/zurich", H), ("europe/budapest", H), ("europe/belgrade", H), ("europe/athens", 2 * H), ("europe/helsinki", 2 * H),
        ("europe/kiev", 2 * H), ("europe/kyiv", 2 * H), ("europe/bucharest", 2 * H), ("europe/sofia", 2 * H),
    ];
    if let Some((_, o)) = eu.iter().find(|(n, _)| *n == lower) {
        return Some(Eu(*o));
    }
    let us: &[(&str, i64)] = &[
        ("america/new_york", -5 * H), ("us/eastern", -5 * H), ("america/toronto", -5 * H), ("america/detroit", -5 * H),
        ("america/chicago", -6 * H), ("us/central", -6 * H), ("america/denver", -7 * H), ("us/mountain", -7 * H),
        ("america/los_angeles", -8 * H), ("us/pacific", -8 * H), ("america/vancouver", -8 * H), ("america/anchorage", -9 * H),
        ("america/halifax", -4 * H),
    ];
    if let Some((_, o)) = us.iter().find(|(n, _)| *n == lower) {
        return Some(Us(*o));
    }
    match lower.as_str() {
        "australia/sydney" | "australia/melbourne" | "australia/canberra" | "australia/hobart" => return Some(AuSouth(10 * H)),
        "australia/adelaide" => return Some(AuSouth(9 * H + 1800)),
        "pacific/auckland" | "nz" => return Some(Nz(12 * H)),
        _ => {}
    }
    // Etc/GMT+N: dấu ngược (POSIX), Etc/GMT+7 = UTC-7
    if let Some(n) = lower.strip_prefix("etc/gmt") {
        return posix_offset(n).map(Fixed);
    }
    // POSIX: `UTC+7`, `+07`, `-05:30`, `7` → độ lệch về phía TÂY (như PostgreSQL)
    let body = lower.strip_prefix("utc").or_else(|| lower.strip_prefix("gmt")).unwrap_or(&lower);
    posix_offset(body).map(Fixed)
}

/// `+7`, `-05:30`, `7` theo quy ước POSIX (dương = phía tây Greenwich) → giây phía đông.
fn posix_offset(s: &str) -> Option<i64> {
    let s = s.trim();
    if s.is_empty() || !s.trim_start_matches(['+', '-']).starts_with(|c: char| c.is_ascii_digit()) {
        return None;
    }
    let (sign, body) = match s.as_bytes()[0] {
        b'-' => (1, &s[1..]),
        b'+' => (-1, &s[1..]),
        _ => (-1, s),
    };
    let (h, m) = match body.split_once(':') {
        Some((h, m)) => (h.parse::<i64>().ok()?, m.parse::<i64>().ok()?),
        None => (body.parse::<i64>().ok()?, 0),
    };
    if h > 15 || m > 59 {
        return None;
    }
    Some(sign * (h * H + m * 60))
}

/// Ngày (tính từ epoch) của Chủ nhật thứ `n` (n > 0) hoặc cuối cùng (n = 0) trong tháng.
fn sunday(y: i32, m: u32, n: u32) -> i64 {
    if n == 0 {
        let last = days_from_civil(y, m, days_in_month(y, m));
        last - (last + 4).rem_euclid(7)
    } else {
        let first = days_from_civil(y, m, 1);
        let to_sun = (7 - (first + 4).rem_euclid(7)) % 7;
        first + to_sun + 7 * (n as i64 - 1)
    }
}

/// Độ lệch (giây phía đông UTC) của múi giờ tại thời điểm UTC `utc` (giây epoch).
fn offset_at(rule: ZoneRule, utc: i64) -> i64 {
    use ZoneRule::*;
    let (y, _, _) = civil_from_days(utc.div_euclid(86_400));
    match rule {
        Fixed(o) => o,
        Eu(std) => {
            let start = sunday(y, 3, 0) * 86_400 + H;
            let end = sunday(y, 10, 0) * 86_400 + H;
            if utc >= start && utc < end { std + H } else { std }
        }
        Us(std) => {
            let start = sunday(y, 3, 2) * 86_400 + 2 * H - std;
            let end = sunday(y, 11, 1) * 86_400 + 2 * H - (std + H);
            if utc >= start && utc < end { std + H } else { std }
        }
        AuSouth(std) => {
            let end = sunday(y, 4, 1) * 86_400 + 3 * H - (std + H);
            let start = sunday(y, 10, 1) * 86_400 + 2 * H - std;
            if utc < end || utc >= start { std + H } else { std }
        }
        Nz(std) => {
            let end = sunday(y, 4, 1) * 86_400 + 3 * H - (std + H);
            let start = sunday(y, 9, 0) * 86_400 + 2 * H - std;
            if utc < end || utc >= start { std + H } else { std }
        }
    }
}

/// `timezone(zone, ts)` / `ts AT TIME ZONE zone`. `input_tz`: giá trị vào là timestamptz (thời điểm tuyệt
/// đối) → trả giờ địa phương (timestamp); ngược lại là timestamp (giờ địa phương của `zone`) → trả
/// timestamptz (UTC, phiên đặt TimeZone = UTC).
pub(crate) fn timezone_convert(zone: &str, ts: &str, input_tz: bool) -> Result<String, String> {
    let rule = zone_rule(zone).ok_or_else(|| format!("time zone \"{}\" not recognized", zone))?;
    let t = ts.trim();
    let b = t.as_bytes();
    if b.len() < 10 || b[4] != b'-' || b[7] != b'-' {
        return Err(format!("invalid input syntax for type timestamp with time zone: \"{}\"", ts));
    }
    let y: i32 = t[..4].parse().map_err(|_| format!("invalid input syntax for type timestamp: \"{}\"", ts))?;
    let mo: u32 = t[5..7].parse().map_err(|_| format!("invalid input syntax for type timestamp: \"{}\"", ts))?;
    let d: u32 = t[8..10].parse().map_err(|_| format!("invalid input syntax for type timestamp: \"{}\"", ts))?;
    let rest = t[10..].trim_start_matches(['T', 't', ' ']);
    let (secs, frac, offset) = if rest.is_empty() { (0, String::new(), None) } else { parse_clock(rest).ok_or_else(|| format!("invalid input syntax for type timestamp: \"{}\"", ts))? };
    let wall = days_from_civil(y, mo, d) * 86_400 + secs;
    let fmt = |total: i64, suffix: &str| {
        let (yy, mm, dd) = civil_from_days(total.div_euclid(86_400));
        format!("{:04}-{:02}-{:02} {}{}", yy, mm, dd, fmt_clock(total.rem_euclid(86_400), &frac), suffix)
    };
    if input_tz {
        // thời điểm tuyệt đối → giờ địa phương
        let utc = wall - offset.unwrap_or(0);
        Ok(fmt(utc + offset_at(rule, utc), ""))
    } else {
        // giờ địa phương của zone → UTC (timestamp không có múi giờ: bỏ hậu tố `Z` engine ghi kèm)
        let guess = offset_at(rule, wall - offset_at(rule, wall));
        let utc = wall - guess;
        let off = offset_at(rule, utc);
        Ok(fmt(wall - off, "+00"))
    }
}

#[cfg(test)]
mod zone_tests {
    use super::*;

    #[test]
    fn conversions() {
        assert_eq!(timezone_convert("Asia/Ho_Chi_Minh", "2024-01-02 10:00:00+00", true).unwrap(), "2024-01-02 17:00:00");
        assert_eq!(timezone_convert("UTC", "2024-01-02 10:00:00", false).unwrap(), "2024-01-02 10:00:00+00");
        assert_eq!(timezone_convert("UTC", "2024-01-02 10:00:00+07", true).unwrap(), "2024-01-02 03:00:00");
        assert_eq!(timezone_convert("Asia/Ho_Chi_Minh", "2024-01-01 10:00:00", false).unwrap(), "2024-01-01 03:00:00+00");
        // DST châu Âu / Mỹ
        assert_eq!(timezone_convert("Europe/Paris", "2024-07-01 12:00:00+00", true).unwrap(), "2024-07-01 14:00:00");
        assert_eq!(timezone_convert("Europe/Paris", "2024-01-01 12:00:00+00", true).unwrap(), "2024-01-01 13:00:00");
        assert_eq!(timezone_convert("America/New_York", "2024-03-10 06:59:00+00", true).unwrap(), "2024-03-10 01:59:00");
        assert_eq!(timezone_convert("America/New_York", "2024-03-10 07:00:00+00", true).unwrap(), "2024-03-10 03:00:00");
        assert_eq!(timezone_convert("America/New_York", "2024-07-04 12:00:00", false).unwrap(), "2024-07-04 16:00:00+00");
        assert_eq!(timezone_convert("Australia/Sydney", "2024-01-01 00:00:00+00", true).unwrap(), "2024-01-01 11:00:00");
        // POSIX: '+07' là phía tây
        assert_eq!(timezone_convert("+07", "2024-01-01 00:00:00+00", true).unwrap(), "2023-12-31 17:00:00");
        assert!(timezone_convert("Mars/Base", "2024-01-01", true).is_err());
    }
}
