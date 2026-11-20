use command_macros::command;

use crate::engine::command::{CommandArgs, CommandError};
use crate::engine::core::{CommandOutcome, Value};
use crate::framework::commands::command_runtime::CommandRuntime;
use crate::framework::commands::pattern::string_match;

use super::runtime_support::{parse_scan_options, read_path_hash, reply_page};
use bytes::Bytes;

fn parse_cursor(raw: &[u8]) -> Option<u64> {
    // string2ull rejects canonical signed negatives before its strtoull fallback.
    if raw.len() < 21 {
        if let Some(value) = std::str::from_utf8(raw)
            .ok()
            .and_then(|text| text.parse::<i64>().ok())
            .filter(|value| value.to_string().as_bytes() == raw)
        {
            return u64::try_from(value).ok();
        }
    }
    // The fallback reads a C string, accepts all six C-locale space bytes,
    // and negates unsigned magnitudes only after checking overflow.
    let raw = raw.split(|byte| *byte == 0).next().unwrap_or_default();
    let start = raw
        .iter()
        .position(|byte| !matches!(byte, b' ' | b'\t'..=b'\r'))
        .unwrap_or(raw.len());
    let digits = &raw[start..];
    let (is_negative, digits) = match digits.first() {
        Some(b'+') => (false, &digits[1..]),
        Some(b'-') => (true, &digits[1..]),
        _ => (false, digits),
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

#[command("PHSCAN", runtime)]
pub fn phscan(runtime: &mut dyn CommandRuntime, args: &CommandArgs) -> CommandOutcome {
    if args.args.len() < 3 {
        return CommandOutcome::reply(CommandError::wrong_arity("phscan"));
    }
    let cursor = match parse_cursor(&args.args[2]) {
        Some(cursor) => cursor,
        None => return CommandOutcome::reply(Value::error("ERR invalid cursor")),
    };
    let options = match parse_scan_options(&args.args[3..], b"MATCH") {
        Ok(options) => options,
        Err(error) => return CommandOutcome::reply(error),
    };
    let payload = match read_path_hash(runtime, &args.args[0]) {
        Ok(tree) => tree.and_then(|tree| tree.get(&args.args[1])),
        Err(error) => return CommandOutcome::reply(error),
    };
    let Some(payload) = payload else {
        return CommandOutcome::reply(reply_page(Bytes::from_static(b"0"), Vec::new()));
    };
    let (cursor, pairs) = payload.scan(cursor, options.count);
    let mut entries = Vec::new();
    for (field, value) in pairs {
        if options.filter.is_none_or(|pattern| {
            if field.is_empty() {
                // The target disables filtering for exactly one star;
                // its matcher does not consume stars on an empty input.
                pattern.is_empty() || pattern == b"*"
            } else {
                string_match(pattern, &field, false)
            }
        }) {
            entries.push(Value::bulk(field));
            entries.push(Value::bulk(value));
        }
    }
    CommandOutcome::reply(reply_page(Bytes::from(cursor.to_string()), entries))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::foundation::io::store::structure::path_hash::PathHash;
    use crate::foundation::io::store::Robj;
    use crate::framework::commands::command_runtime::TestCommandRuntime;

    fn scan_args(key: &[u8], cursor: &[u8], options: &[&[u8]]) -> CommandArgs {
        let mut parts = vec![
            Bytes::from_static(b"PHSCAN"),
            Bytes::copy_from_slice(key),
            Bytes::from_static(b"path"),
            Bytes::copy_from_slice(cursor),
        ];
        parts.extend(options.iter().map(|part| Bytes::copy_from_slice(part)));
        CommandArgs::from_parts(parts)
    }

    #[test]
    fn empty_fields_follow_target_match_rules_in_both_encodings() {
        for max_entries in [0, 8] {
            let mut runtime = TestCommandRuntime::new(1, 100);
            let mut tree = PathHash::new();
            tree.set_pairs(
                b"path",
                &[(Bytes::new(), Bytes::from_static(b"v"))],
                max_entries,
                64,
            );
            runtime
                .dbs
                .get_mut(0)
                .unwrap()
                .set(b"key".to_vec(), Robj::PathHash(Box::new(tree)));
            for pattern in [b"".as_slice(), b"*", b"**", b"***"] {
                let expected = if pattern.is_empty() || pattern == b"*" {
                    vec![Value::bulk(Bytes::new()), Value::bulk("v")]
                } else {
                    Vec::new()
                };
                assert_eq!(
                    phscan(
                        &mut runtime,
                        &scan_args(b"key", b"0", &[b"MATCH", pattern, b"COUNT", b"10000"]),
                    )
                    .reply,
                    reply_page(Bytes::from_static(b"0"), expected),
                    "{max_entries} {pattern:?}"
                );
            }
        }
    }

    #[test]
    fn phscan_cursor_matches_target_unsigned_conversion_and_error_precedence() {
        for (raw, expected) in [
            (b" 1".as_slice(), 1),
            (b"\x0b+01\0suffix", 1),
            (b"0000000000000000000000000000", 0),
            (b" -1", u64::MAX),
            (b"-1\0suffix", u64::MAX),
            (b"-01", u64::MAX),
            (b"-9223372036854775809", i64::MAX as u64),
            (b"-18446744073709551615", 1),
            (b"18446744073709551615", u64::MAX),
        ] {
            assert_eq!(parse_cursor(raw), Some(expected), "{raw:?}");
        }
        let mut runtime = TestCommandRuntime::new(1, 100);
        let mut tree = PathHash::new();
        tree.set_pairs(
            b"path",
            &[(Bytes::from_static(b"f"), Bytes::from_static(b"v"))],
            512,
            64,
        );
        let db = runtime.dbs.get_mut(0).unwrap();
        db.set(b"compact".to_vec(), Robj::PathHash(Box::new(tree)));
        db.set(b"wrongtype".to_vec(), Robj::string("value"));
        for raw in [
            b"0".as_slice(),
            b" 0",
            b"\t0",
            b"\n0",
            b"\r0",
            b"\x0b0",
            b"\x0c0",
            b"0\0suffix",
            b"0\0\xff",
            b"  +000\0suffix",
            b"+01",
            b"00001",
            b"-0",
            b"-01",
            b" -1",
            b"-1\0suffix",
            b"18446744073709551615",
            b"-9223372036854775809",
            b"-18446744073709551615",
        ] {
            let actual = phscan(&mut runtime, &scan_args(b"compact", raw, &[]));
            assert_eq!(
                actual.reply,
                reply_page(
                    Bytes::from_static(b"0"),
                    vec![Value::bulk("f"), Value::bulk("v")]
                ),
                "{raw:?}"
            );
            assert!(actual.effects.changed_keys.is_empty());
            assert_eq!(
                phscan(&mut runtime, &scan_args(b"missing", raw, &[])).reply,
                reply_page(Bytes::from_static(b"0"), vec![]),
                "{raw:?}"
            );
            assert_eq!(
                phscan(&mut runtime, &scan_args(b"wrongtype", raw, &[])).reply,
                CommandError::wrong_type(),
                "{raw:?}"
            );
            assert_eq!(
                phscan(
                    &mut runtime,
                    &scan_args(b"wrongtype", raw, &[b"COUNT", b"0"])
                )
                .reply,
                super::super::runtime_support::parse_long_range(b"0", 1).unwrap_err(),
                "{raw:?}"
            );
        }
        for raw in [
            b"".as_slice(),
            b"\0suffix",
            b" ",
            b"  \0suffix",
            b"+",
            b"-",
            b"-1",
            b"-9223372036854775808",
            b"18446744073709551616",
            b"-18446744073709551616",
            b"0 ",
            b"0 \0suffix",
            b"1x\0suffix",
            b"\xff0",
        ] {
            for key in [b"missing".as_slice(), b"compact", b"wrongtype"] {
                for options in [vec![], vec![b"COUNT".as_slice(), b"0"]] {
                    assert_eq!(
                        phscan(&mut runtime, &scan_args(key, raw, &options)).reply,
                        Value::error("ERR invalid cursor"),
                        "{key:?} {raw:?}"
                    );
                }
            }
        }
    }
}
