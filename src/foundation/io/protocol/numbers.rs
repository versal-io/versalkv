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
    let digit_start = usize::from(s.first() == Some(&b'-'));
    if digit_start >= s.len() || !s[digit_start..].iter().all(u8::is_ascii_digit) {
        return Err(invalid_len(kind));
    }
    std::str::from_utf8(s)
        .ok()
        .and_then(|s| s.parse::<i64>().ok())
        .ok_or_else(|| invalid_len(kind))
}

fn invalid_len(kind: &str) -> ParseError {
    ParseError::Protocol(format!("Protocol error: invalid {} length", kind))
}
