use super::{ParseError, PROT_MAX_BULK};

pub(super) fn parse_mbulk_count(buf: &[u8], from: usize, to: usize) -> Result<i64, ParseError> {
    let n = parse_signed_len(buf, from, to, "multibulk")?;
    if n >= 0 && n > i32::MAX as i64 {
        return Err(invalid_len("multibulk"));
    }
    Ok(n)
}

pub(super) fn parse_bulk_len(buf: &[u8], from: usize, to: usize) -> Result<i64, ParseError> {
    let n = parse_signed_len(buf, from, to, "bulk")?;
    if !(0..=PROT_MAX_BULK).contains(&n) {
        return Err(invalid_len("bulk"));
    }
    Ok(n)
}

fn parse_signed_len(buf: &[u8], from: usize, to: usize, kind: &str) -> Result<i64, ParseError> {
    if from >= to {
        return Err(invalid_len(kind));
    }
    let s = &buf[from..to];
    let (negative, digits) = match s.first() {
        Some(&b'-') => (true, &s[1..]),
        _ => (false, s),
    };
    if digits.is_empty() {
        return Err(invalid_len(kind));
    }
    // Single-pass digit validation and value accumulation. This avoids the
    // separate `all(is_ascii_digit)` pre-scan; each byte is validated as it is
    // consumed. Overflow is still caught by `checked_mul`/`checked_add`.
    let mut value: i64 = 0;
    for &byte in digits {
        if !byte.is_ascii_digit() {
            return Err(invalid_len(kind));
        }
        value = value
            .checked_mul(10)
            .and_then(|v| v.checked_add((byte - b'0') as i64))
            .ok_or_else(|| invalid_len(kind))?;
    }
    Ok(if negative { -value } else { value })
}

fn invalid_len(kind: &str) -> ParseError {
    ParseError::Protocol(format!("Protocol error: invalid {} length", kind))
}
