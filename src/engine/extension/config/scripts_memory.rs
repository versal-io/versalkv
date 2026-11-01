const PARSE_ERROR: &str = "argument must be a memory or percent value";

/// Store percentages as negative integers, with both zero forms canonicalized.
pub(crate) fn parse(value: &str) -> Result<i64, String> {
    if let Some(percent) = value.strip_suffix('%') {
        if percent.is_empty() || !percent.bytes().all(|byte| byte.is_ascii_digit()) {
            return Err(PARSE_ERROR.to_string());
        }
        let percent = percent
            .parse::<u64>()
            .map_err(|_| PARSE_ERROR.to_string())?;
        if percent > 100 {
            return Err("percentage argument must be less or equal to 100".to_string());
        }
        return Ok(-(percent as i64));
    }
    let digits = value.bytes().take_while(u8::is_ascii_digit).count();
    if digits == 0 {
        return Err(PARSE_ERROR.to_string());
    }
    let multiplier = match value[digits..].to_ascii_lowercase().as_str() {
        "" | "b" => 1,
        "k" => 1_000,
        "m" => 1_000_000,
        "g" => 1_000_000_000,
        "kb" => 1 << 10,
        "mb" => 1 << 20,
        "gb" => 1 << 30,
        _ => return Err(PARSE_ERROR.to_string()),
    };
    value[..digits]
        .parse::<u64>()
        .ok()
        .and_then(|number| number.checked_mul(multiplier))
        .filter(|number| *number <= isize::MAX as u64)
        .map(|number| number as i64)
        .ok_or_else(|| PARSE_ERROR.to_string())
}

pub(crate) fn format(value: i64) -> String {
    if value < 0 {
        format!("{}%", -value)
    } else {
        value.to_string()
    }
}

pub(crate) fn effective_limit(value: i64, maxmemory: u64) -> Option<usize> {
    let bytes = if value < 0 {
        if maxmemory == 0 {
            return None;
        }
        (u128::from(maxmemory) * u128::from(value.unsigned_abs()) / 100).min(usize::MAX as u128)
            as usize
    } else if value > 0 {
        value as usize
    } else {
        return None;
    };
    Some(bytes.max(128 * 1024))
}
