use crate::foundation::io::protocol::Value;
use crate::foundation::io::store::structure::stream::StreamID;

pub(crate) fn parse_signed_integer(raw: &[u8]) -> Option<i64> {
    let text = std::str::from_utf8(raw).ok()?;
    let value: i64 = text.parse().ok()?;
    (value.to_string() == text).then_some(value)
}

fn parse_unsigned(raw: &[u8]) -> Option<u64> {
    if let Some(value) = parse_signed_integer(raw) {
        return u64::try_from(value).ok();
    }
    // Match string2ull's strtoull fallback, not the signed count grammar.
    let raw = raw.trim_ascii_start();
    let (is_negative, digits) = match raw.first() {
        Some(b'+') => (false, &raw[1..]),
        Some(b'-') => (true, &raw[1..]),
        _ => (false, raw),
    };
    if digits.is_empty() || !digits.iter().all(u8::is_ascii_digit) {
        return None;
    }
    let value = digits.iter().try_fold(0u64, |value, digit| {
        value.checked_mul(10)?.checked_add(u64::from(digit - b'0'))
    })?;
    Some(if is_negative {
        value.wrapping_neg()
    } else {
        value
    })
}

fn invalid_id() -> Value {
    Value::error("ERR Invalid stream ID specified as stream command argument")
}

fn parse_id(raw: &[u8], allow_auto_seq: bool) -> Result<(StreamID, bool), Value> {
    if raw.len() > 127 {
        return Err(invalid_id());
    }
    let raw = raw.split(|byte| *byte == 0).next().unwrap_or_default();
    let mut parts = raw.splitn(2, |byte| *byte == b'-');
    let ms = parse_unsigned(parts.next().unwrap_or_default()).ok_or_else(invalid_id)?;
    let seq_raw = parts.next();
    let seq_auto = allow_auto_seq && seq_raw == Some(b"*".as_slice());
    let seq = if seq_auto {
        0
    } else {
        seq_raw
            .map(parse_unsigned)
            .unwrap_or(Some(0))
            .ok_or_else(invalid_id)?
    };
    Ok((StreamID::new(ms, seq), seq_auto))
}

pub(crate) fn parse_stream_id_strict(raw: &[u8]) -> Result<StreamID, Value> {
    parse_id(raw, false).map(|(id, _)| id)
}

pub(crate) fn parse_xadd_id(raw: &[u8]) -> Result<(bool, bool, StreamID), Value> {
    if raw == b"*" {
        return Ok((false, false, StreamID::zero()));
    }
    let (id, seq_auto) = parse_id(raw, true)?;
    Ok((true, seq_auto, id))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn strict_ids_match_unsigned_target_grammar_without_range_sentinels() {
        for (raw, expected) in [
            (b"1".as_slice(), StreamID::new(1, 0)),
            (b"+01- 2".as_slice(), StreamID::new(1, 2)),
            (b"1-0\0ignored".as_slice(), StreamID::new(1, 0)),
            (
                b"18446744073709551615-18446744073709551615".as_slice(),
                StreamID::max(),
            ),
        ] {
            assert_eq!(parse_stream_id_strict(raw).unwrap(), expected);
        }
        for raw in [
            b"+".as_slice(),
            b"-",
            b"(1-0",
            b"1-*",
            b"18446744073709551616-0",
            b"1-0 ",
        ] {
            assert_eq!(parse_stream_id_strict(raw), Err(invalid_id()));
        }
        assert_eq!(
            parse_xadd_id(b"0-*").unwrap(),
            (true, true, StreamID::zero())
        );
        assert_eq!(
            parse_xadd_id(b"*").unwrap(),
            (false, false, StreamID::zero())
        );
        for raw in [
            b"+1".as_slice(),
            b"01",
            b"-0",
            b" 1",
            b"9223372036854775808",
        ] {
            assert_eq!(parse_signed_integer(raw), None);
        }
        assert_eq!(
            parse_signed_integer(b"-9223372036854775808"),
            Some(i64::MIN)
        );
    }
}
