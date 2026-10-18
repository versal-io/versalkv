use crate::engine::command::{CommandArgs, CommandError};
use crate::foundation::io::protocol::Value;
use crate::foundation::io::store::structure::hashtable::HashTable;
use bytes::Bytes;

command_macros::portable_command_support!();

pub fn fmt_score(v: f64) -> Bytes {
    if v.is_infinite() {
        return Bytes::from_static(if v > 0.0 { b"inf" } else { b"-inf" });
    }
    if v == v.trunc() && v.abs() < 1e17 {
        return Bytes::from(format!("{}", v as i64));
    }
    if v == 0.0 {
        return Bytes::from_static(b"0");
    }
    let abs_v = v.abs();
    let exp = abs_v.log10().floor() as i64;
    if !(-4..17).contains(&exp) {
        let s = format!("{:.16e}", v);
        let e_pos = s.find('e').unwrap_or(s.len());
        let mantissa = &s[..e_pos];
        let exponent = &s[e_pos..];
        let exponent_str = if exponent.len() > 2 && exponent.as_bytes()[1] == b'-' {
            exponent.to_string()
        } else {
            format!("e+{}", &exponent[1..])
        };
        let trimmed = mantissa.trim_end_matches('0').trim_end_matches('.');
        Bytes::from(format!("{}{}", trimmed, exponent_str))
    } else {
        let decimal_places = (17 - exp - 1) as usize;
        let s = format!("{:.prec$}", v, prec = decimal_places);
        Bytes::from(s.trim_end_matches('0').trim_end_matches('.').to_string())
    }
}

pub fn score_value(v: f64, resp3: bool) -> Value {
    if resp3 {
        Value::Double(v)
    } else {
        Value::bulk(fmt_score(v))
    }
}

pub fn parse_score(b: &[u8]) -> Option<f64> {
    let s = std::str::from_utf8(b).ok()?.trim();
    match s.to_ascii_lowercase().as_str() {
        "inf" | "+inf" | "infinity" | "+infinity" => Some(f64::INFINITY),
        "-inf" | "-infinity" => Some(f64::NEG_INFINITY),
        _ => s.parse::<f64>().ok().filter(|f| !f.is_nan()),
    }
}

pub fn parse_score_range(b: &[u8]) -> Option<(f64, bool)> {
    let mut inclusive = true;
    let mut s = b;
    if let Some(rest) = s.strip_prefix(b"(") {
        inclusive = false;
        s = rest;
    }
    let score = parse_score(s)?;
    Some((score, inclusive))
}

pub fn parse_lex_bound(b: &[u8]) -> Option<(Bytes, bool)> {
    if b == b"-" || b == b"+" {
        return Some((Bytes::copy_from_slice(b), true));
    }
    let inclusive: bool;
    let s = if let Some(rest) = b.strip_prefix(b"(") {
        inclusive = false;
        rest
    } else {
        let rest = b.strip_prefix(b"[")?;
        inclusive = true;
        rest
    };
    Some((Bytes::copy_from_slice(s), inclusive))
}

pub fn sorted_entries(m: &HashTable<f64>) -> Vec<(Bytes, f64)> {
    let mut v: Vec<(Bytes, f64)> = m.iter().map(|(k, &s)| (k.clone(), s)).collect();
    v.sort_by(|a, b| {
        match (a.1.is_nan(), b.1.is_nan()) {
            (true, true) | (false, false) => {
                a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal)
            }
            (true, false) => std::cmp::Ordering::Greater,
            (false, true) => std::cmp::Ordering::Less,
        }
        .then_with(|| a.0.cmp(&b.0))
    });
    v
}

pub fn simple_random_usize(max: usize) -> usize {
    crate::foundation::io::sys::rand::random_usize(max)
}

pub fn parse_count(args: &CommandArgs) -> Result<(usize, bool), Value> {
    if args.len() == 2 {
        let s = std::str::from_utf8(&args.args[1])
            .ok()
            .and_then(|value| value.parse::<i64>().ok())
            .ok_or_else(|| Value::error("ERR value is not an integer or out of range"))?;
        if s < 0 {
            return Err(Value::error("ERR value is out of range, must be positive"));
        }
        Ok((s as usize, true))
    } else if args.len() == 1 {
        Ok((1, false))
    } else {
        Err(CommandError::syntax_error())
    }
}

#[derive(Clone, Copy, PartialEq)]
pub enum Aggregate {
    Sum,
    Min,
    Max,
}

pub fn parse_set_operation_args(
    args: &[Bytes],
    start_idx: usize,
    cmd_name: &str,
) -> Result<(Vec<(Bytes, f64)>, Aggregate), Value> {
    let numkeys = match std::str::from_utf8(&args[start_idx])
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
    {
        Some(n) if n > 0 => n,
        _ => {
            return Err(Value::error(format!(
                "ERR at least 1 input key is needed for '{}' command",
                cmd_name
            )))
        }
    };
    if start_idx + 1 + numkeys > args.len() {
        return Err(CommandError::syntax_error());
    }
    let keys: Vec<Bytes> = args[start_idx + 1..start_idx + 1 + numkeys].to_vec();
    let mut weights: Vec<f64> = Vec::new();
    let mut aggregate = Aggregate::Sum;
    let mut i = start_idx + 1 + numkeys;
    while i < args.len() {
        if args[i].eq_ignore_ascii_case(b"weights") {
            if i + numkeys >= args.len() {
                return Err(CommandError::syntax_error());
            }
            for j in 0..numkeys {
                match parse_score(&args[i + 1 + j]) {
                    Some(w) => weights.push(w),
                    None => return Err(Value::error("ERR weight value is not a valid float")),
                }
            }
            i += 1 + numkeys;
        } else if args[i].eq_ignore_ascii_case(b"aggregate") {
            if i + 1 >= args.len() {
                return Err(CommandError::syntax_error());
            }
            if args[i + 1].eq_ignore_ascii_case(b"sum") {
                aggregate = Aggregate::Sum;
            } else if args[i + 1].eq_ignore_ascii_case(b"min") {
                aggregate = Aggregate::Min;
            } else if args[i + 1].eq_ignore_ascii_case(b"max") {
                aggregate = Aggregate::Max;
            } else {
                return Err(Value::error("ERR syntax error"));
            }
            i += 2;
        } else {
            return Err(CommandError::syntax_error());
        }
    }
    if weights.is_empty() {
        weights = vec![1.0; numkeys];
    }
    Ok((keys.into_iter().zip(weights).collect(), aggregate))
}

pub fn aggregate_score(a: f64, b: f64, agg: Aggregate) -> f64 {
    let result = match agg {
        Aggregate::Sum => a + b,
        Aggregate::Min => a.min(b),
        Aggregate::Max => a.max(b),
    };
    if result.is_nan() {
        0.0
    } else {
        result
    }
}
