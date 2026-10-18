use bytes::Bytes;

command_macros::portable_command_support!();

pub const NOT_INT: &str = "ERR value is not an integer or out of range";
pub const NOT_FLOAT: &str = "ERR value is not a valid float";

pub enum ParseIntResult {
    Ok(i64),
    NotInteger,
    OutOfRange,
}

pub fn parse_i64_range(value: &[u8], min: i64, max: i64) -> ParseIntResult {
    let value = match std::str::from_utf8(value) {
        Ok(value) => value,
        Err(_) => return ParseIntResult::NotInteger,
    };
    let parsed: i128 = match value.parse() {
        Ok(parsed) => parsed,
        Err(_) => return ParseIntResult::NotInteger,
    };
    if parsed < min as i128 || parsed > max as i128 {
        return ParseIntResult::OutOfRange;
    }
    if parsed.to_string() != value {
        return ParseIntResult::NotInteger;
    }
    ParseIntResult::Ok(parsed as i64)
}

pub fn parse_float(value: &[u8]) -> Option<f64> {
    let value = std::str::from_utf8(value).ok()?;
    if value.is_empty() || value.trim() != value {
        return None;
    }
    value.parse::<f64>().ok()
}

pub fn format_float(value: f64) -> String {
    if value == 0.0 {
        "0".to_string()
    } else {
        value.to_string()
    }
}

pub fn hash_field_value_propagation(
    key: &[u8],
    field: &[u8],
    value: &[u8],
    expire_at_ms: Option<u64>,
) -> Vec<Bytes> {
    match expire_at_ms {
        Some(expire_at_ms) => vec![
            Bytes::from_static(b"HSETEX"),
            Bytes::copy_from_slice(key),
            Bytes::from_static(b"PXAT"),
            expire_at_ms.to_string().into(),
            Bytes::from_static(b"FIELDS"),
            Bytes::from_static(b"1"),
            Bytes::copy_from_slice(field),
            Bytes::copy_from_slice(value),
        ],
        None => vec![
            Bytes::from_static(b"HSET"),
            Bytes::copy_from_slice(key),
            Bytes::copy_from_slice(field),
            Bytes::copy_from_slice(value),
        ],
    }
}

pub fn hash_field_deletion_propagation(key: &[u8], fields: &[Bytes]) -> Vec<Bytes> {
    let mut parts = Vec::with_capacity(fields.len() + 2);
    parts.push(Bytes::from_static(b"HDEL"));
    parts.push(Bytes::copy_from_slice(key));
    parts.extend(fields.iter().cloned());
    parts
}

pub fn hash_field_expiry_propagation(
    key: &[u8],
    when_ms: u64,
    fields: &[Bytes],
) -> Vec<Bytes> {
    let mut parts = vec![
        Bytes::from_static(b"HPEXPIREAT"),
        Bytes::copy_from_slice(key),
        when_ms.to_string().into(),
        Bytes::from_static(b"FIELDS"),
        fields.len().to_string().into(),
    ];
    parts.extend(fields.iter().cloned());
    parts
}

pub fn hash_field_persist_propagation(key: &[u8], fields: &[Bytes]) -> Vec<Bytes> {
    let mut parts = vec![
        Bytes::from_static(b"HPERSIST"),
        Bytes::copy_from_slice(key),
        Bytes::from_static(b"FIELDS"),
        fields.len().to_string().into(),
    ];
    parts.extend(fields.iter().cloned());
    parts
}

pub fn parse_fields(
    args: &[Bytes],
    start_idx: usize,
) -> Result<(usize, Vec<Bytes>), crate::foundation::io::protocol::Value> {
    use crate::foundation::io::protocol::Value;
    if start_idx >= args.len() {
        return Err(Value::error("ERR wrong number of arguments"));
    }
    if !args[start_idx].eq_ignore_ascii_case(b"fields") {
        return Ok((1, vec![args[start_idx].clone()]));
    }
    if start_idx + 1 >= args.len() {
        return Err(Value::error(
            "ERR numfields should be greater than 0 and match the provided number of fields",
        ));
    }
    let count = std::str::from_utf8(&args[start_idx + 1])
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .ok_or_else(|| Value::error("ERR numfields should be greater than 0"))?;
    if count == 0 || start_idx + 2 + count > args.len() {
        return Err(Value::error(
            "ERR numfields should be greater than 0 and match the provided number of fields",
        ));
    }
    Ok((2 + count, args[start_idx + 2..start_idx + 2 + count].to_vec()))
}
