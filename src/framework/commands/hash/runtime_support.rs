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

pub fn hash_field_expiry_propagation(key: &[u8], when_ms: u64, fields: &[Bytes]) -> Vec<Bytes> {
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
    parse_fields_with_count_error(
        args,
        start_idx,
        "ERR numfields should be greater than 0 and match the provided number of fields",
    )
}

pub fn parse_query_fields(
    args: &[Bytes],
    start_idx: usize,
) -> Result<(usize, Vec<Bytes>), crate::foundation::io::protocol::Value> {
    parse_fields_with_count_error(args, start_idx, "ERR syntax error")
}

fn parse_fields_with_count_error(
    args: &[Bytes],
    start_idx: usize,
    count_error: &'static str,
) -> Result<(usize, Vec<Bytes>), crate::foundation::io::protocol::Value> {
    use crate::foundation::io::protocol::Value;
    if start_idx >= args.len() {
        return Err(Value::error("ERR wrong number of arguments"));
    }
    if !args[start_idx].eq_ignore_ascii_case(b"fields") {
        return Err(Value::error("ERR syntax error"));
    }
    if start_idx + 1 >= args.len() {
        return Err(Value::error(count_error));
    }
    let count = match parse_i64_range(&args[start_idx + 1], i64::MIN, i64::MAX) {
        ParseIntResult::Ok(count) => count,
        ParseIntResult::NotInteger | ParseIntResult::OutOfRange => {
            return Err(Value::error(NOT_INT));
        }
    };
    let fields_start = start_idx + 2;
    if count <= 0 || usize::try_from(count).ok() != Some(args.len() - fields_start) {
        return Err(Value::error(count_error));
    }
    let count = args.len() - fields_start;
    Ok((
        2 + count,
        args[start_idx + 2..start_idx + 2 + count].to_vec(),
    ))
}

#[cfg(test)]
mod tests {
    use super::{parse_fields, parse_query_fields, NOT_INT};
    use crate::foundation::io::protocol::Value;
    use bytes::Bytes;

    fn hfe_args(parts: &[&str]) -> crate::engine::command::CommandArgs {
        crate::engine::command::CommandArgs::from_parts(
            parts
                .iter()
                .map(|part| Bytes::copy_from_slice(part.as_bytes()))
                .collect(),
        )
    }

    fn hfe_parts(parts: &[&str]) -> Vec<Bytes> {
        parts
            .iter()
            .map(|part| Bytes::copy_from_slice(part.as_bytes()))
            .collect()
    }

    fn hfe_runtime(
        max_entries: usize,
    ) -> crate::framework::commands::command_runtime::TestCommandRuntime {
        use crate::framework::commands::command_runtime::{CommandRuntime, TestCommandRuntime};
        let mut runtime = TestCommandRuntime::new(1, 100);
        let db = runtime.db_mut().unwrap();
        for field in [b"nottl".as_slice(), b"ghost", b"live", b"live2"] {
            db.hash_set_with_limits(b"h", field, b"v", max_entries, 64)
                .unwrap();
        }
        db.set_hash_field_expire(b"h", b"ghost", 99);
        db.set_hash_field_expire(b"h", b"live", 200);
        db.set_hash_field_expire(b"h", b"live2", 200);
        runtime
    }

    #[test]
    #[cfg(not(feature = "wasm"))]
    fn hfe_ordinary_reads_hide_without_reaping() {
        use crate::framework::commands::command_runtime::CommandRuntime;
        use crate::framework::commands::hash::{hexists, hget, hscan};
        let _clock = crate::foundation::io::sys::time::enter_command_time_snapshot(100);
        for max_entries in [128, 0] {
            let mut runtime = hfe_runtime(max_entries);
            assert_eq!(
                hget::hget(&mut runtime, &hfe_args(&["HGET", "h", "ghost"])).reply,
                Value::null()
            );
            assert_eq!(
                hexists::hexists(&mut runtime, &hfe_args(&["HEXISTS", "h", "ghost"])).reply,
                Value::integer(0)
            );
            let scan = hscan::hscan(&mut runtime, &hfe_args(&["HSCAN", "h", "0"]));
            let Value::Array(scan) = scan.reply else {
                panic!("expected scan array")
            };
            let Value::Array(pairs) = &scan[1] else {
                panic!("expected scan pairs")
            };
            assert!(!pairs.contains(&Value::bulk(b"ghost".to_vec())));
            let db = runtime.db_mut().unwrap();
            assert_eq!(db.get_hash_object(b"h").unwrap().unwrap().hash_len(), 4);
            assert_eq!(db.get_hash_field_expire(b"h", b"ghost"), Some(99));
            assert_eq!(db.lazy_expired_hash_fields, 0);
        }
    }

    #[test]
    #[cfg(not(feature = "wasm"))]
    fn hfe_delete_and_persist_preserve_skipped_prefixes() {
        use crate::framework::commands::command_runtime::CommandRuntime;
        use crate::framework::commands::hash::{hdel, hgetdel, hpersist};
        let _clock = crate::foundation::io::sys::time::enter_command_time_snapshot(100);
        for max_entries in [128, 0] {
            let mut runtime = hfe_runtime(max_entries);
            let outcome = hdel::hdel(&mut runtime, &hfe_args(&["HDEL", "h", "ghost", "live"]));
            assert_eq!(outcome.reply, Value::integer(1));
            assert!(outcome.effects.skip_propagate);
            assert_eq!(
                outcome.effects.custom_propagations,
                vec![hfe_parts(&["HDEL", "h", "live"])]
            );
            assert_eq!(
                runtime
                    .db_mut()
                    .unwrap()
                    .get_hash_object(b"h")
                    .unwrap()
                    .unwrap()
                    .hash_len(),
                3
            );

            let mut runtime = hfe_runtime(max_entries);
            let outcome = hgetdel::hgetdel(
                &mut runtime,
                &hfe_args(&["HGETDEL", "h", "FIELDS", "2", "ghost", "live"]),
            );
            assert_eq!(
                outcome.reply,
                Value::Array(vec![Value::null(), Value::bulk(b"v".to_vec())])
            );
            assert_eq!(
                outcome.effects.custom_propagations,
                vec![hfe_parts(&["HDEL", "h", "live"])]
            );
            assert_eq!(
                runtime
                    .db_mut()
                    .unwrap()
                    .get_hash_object(b"h")
                    .unwrap()
                    .unwrap()
                    .hash_len(),
                3
            );

            let mut runtime = hfe_runtime(max_entries);
            let outcome = hpersist::hpersist(
                &mut runtime,
                &hfe_args(&["HPERSIST", "h", "FIELDS", "3", "nottl", "ghost", "live"]),
            );
            assert_eq!(
                outcome.reply,
                Value::Array(vec![
                    Value::integer(-1),
                    Value::integer(-2),
                    Value::integer(1)
                ])
            );
            assert_eq!(
                outcome.effects.custom_propagations,
                vec![hfe_parts(&[
                    "HPERSIST", "h", "FIELDS", "2", "nottl", "live"
                ])]
            );
            assert_eq!(
                runtime
                    .db_mut()
                    .unwrap()
                    .get_hash_field_expire(b"h", b"ghost"),
                Some(99)
            );
        }
    }

    #[test]
    #[cfg(not(feature = "wasm"))]
    fn hfe_expiry_keeps_flags_and_pre_skipped_condition_failures() {
        use crate::framework::commands::command_runtime::CommandRuntime;
        use crate::framework::commands::hash::hpexpireat;
        let _clock = crate::foundation::io::sys::time::enter_command_time_snapshot(100);
        for max_entries in [128, 0] {
            let mut runtime = hfe_runtime(max_entries);
            let outcome = hpexpireat::hpexpireat(
                &mut runtime,
                &hfe_args(&[
                    "HPEXPIREAT",
                    "h",
                    "300",
                    "XX",
                    "GT",
                    "FIELDS",
                    "3",
                    "nottl",
                    "ghost",
                    "live",
                ]),
            );
            assert_eq!(
                outcome.reply,
                Value::Array(vec![
                    Value::integer(0),
                    Value::integer(-2),
                    Value::integer(1)
                ])
            );
            assert_eq!(
                outcome.effects.custom_propagations,
                vec![hfe_parts(&[
                    "HPEXPIREAT",
                    "h",
                    "300",
                    "XX",
                    "GT",
                    "FIELDS",
                    "2",
                    "nottl",
                    "live"
                ])]
            );
            assert_eq!(
                runtime
                    .db_mut()
                    .unwrap()
                    .get_hash_field_expire(b"h", b"ghost"),
                Some(99)
            );
        }
    }

    #[test]
    fn hfe_remaining_readers_hide_ghosts_without_reaping() {
        use crate::framework::commands::command_runtime::CommandRuntime;
        use crate::framework::commands::hash::{hgetex, hkeys, hstrlen, hvals};
        for max_entries in [128, 0] {
            let mut runtime = hfe_runtime(max_entries);
            assert_eq!(
                hstrlen::hstrlen(&mut runtime, &hfe_args(&["HSTRLEN", "h", "ghost"])).reply,
                Value::integer(0)
            );
            assert_eq!(
                hkeys::hkeys(&mut runtime, &hfe_args(&["HKEYS", "h"])).reply,
                Value::Array(vec![
                    Value::bulk(b"nottl".to_vec()),
                    Value::bulk(b"live".to_vec()),
                    Value::bulk(b"live2".to_vec())
                ])
            );
            assert_eq!(
                hvals::hvals(&mut runtime, &hfe_args(&["HVALS", "h"])).reply,
                Value::Array(vec![Value::bulk(b"v".to_vec()); 3])
            );
            assert_eq!(
                hgetex::hgetex(
                    &mut runtime,
                    &hfe_args(&["HGETEX", "h", "FIELDS", "1", "ghost"])
                )
                .reply,
                Value::Array(vec![Value::null()])
            );
            let db = runtime.db_mut().unwrap();
            assert_eq!(db.get_hash_object(b"h").unwrap().unwrap().hash_len(), 4);
            assert_eq!(db.get_hash_field_expire(b"h", b"ghost"), Some(99));
            assert_eq!(db.lazy_expired_hash_fields, 0);
        }
    }

    #[test]
    fn hfe_readers_distinguish_ignore_from_keep_and_use_command_clock() {
        use crate::framework::commands::hash::{hgetall, hkeys, hmget, hstrlen, hvals};
        for ignore_expiry in [false, true] {
            let mut runtime = hfe_runtime(128);
            runtime.read_expired_values = ignore_expiry;
            runtime.suppress_lazy_expire = true;
            let count = if ignore_expiry { 4 } else { 3 };
            let expected = if ignore_expiry {
                Value::bulk(b"v".to_vec())
            } else {
                Value::null()
            };
            assert_eq!(
                hmget::hmget(&mut runtime, &hfe_args(&["HMGET", "h", "ghost"])).reply,
                Value::Array(vec![expected])
            );
            assert_eq!(
                hstrlen::hstrlen(&mut runtime, &hfe_args(&["HSTRLEN", "h", "ghost"])).reply,
                Value::integer(i64::from(ignore_expiry))
            );
            for outcome in [
                hkeys::hkeys(&mut runtime, &hfe_args(&["HKEYS", "h"])),
                hvals::hvals(&mut runtime, &hfe_args(&["HVALS", "h"])),
            ] {
                let Value::Array(items) = outcome.reply else {
                    panic!("expected array")
                };
                assert_eq!(items.len(), count);
            }
            let Value::Array(items) =
                hgetall::hgetall(&mut runtime, &hfe_args(&["HGETALL", "h"])).reply
            else {
                panic!("expected array")
            };
            assert_eq!(items.len(), count * 2);
        }
    }

    #[test]
    #[cfg(not(feature = "wasm"))]
    fn hfe_hsetex_default_overwrite_clears_ttl_and_keeps_physical_ghosts() {
        use crate::framework::commands::command_runtime::CommandRuntime;
        use crate::framework::commands::hash::hsetex;
        let _clock = crate::foundation::io::sys::time::enter_command_time_snapshot(100);
        let mut runtime = hfe_runtime(128);
        let outcome = hsetex::hsetex(
            &mut runtime,
            &hfe_args(&["HSETEX", "h", "FIELDS", "1", "live", "new"]),
        );
        assert_eq!(outcome.reply, Value::integer(1));
        let db = runtime.db_mut().unwrap();
        assert_eq!(db.get_hash_field_expire(b"h", b"live"), None);
        assert_eq!(db.get_hash_field_expire(b"h", b"ghost"), Some(99));
        assert_eq!(db.keys_with_volatile_items_count(), 1);
    }

    #[test]
    #[cfg(not(feature = "wasm"))]
    fn hfe_hgetex_retains_ghost_storage_with_active_expiry_enabled() {
        use crate::framework::commands::command_runtime::CommandRuntime;
        use crate::framework::commands::hash::hgetex;
        let _clock = crate::foundation::io::sys::time::enter_command_time_snapshot(100);
        let mut runtime = hfe_runtime(128);
        let outcome = hgetex::hgetex(
            &mut runtime,
            &hfe_args(&["HGETEX", "h", "FIELDS", "1", "ghost"]),
        );
        assert_eq!(outcome.reply, Value::Array(vec![Value::null()]));
        assert_eq!(
            runtime
                .db_mut()
                .unwrap()
                .get_hash_object(b"h")
                .unwrap()
                .unwrap()
                .hash_len(),
            4
        );
        assert_eq!(runtime.db_mut().unwrap().lazy_expired_hash_fields, 0);
    }

    #[test]
    fn hfe_bulk_duplicate_pair_precheck_converts_and_retains_ghost_ttl() {
        use crate::framework::commands::command_runtime::CommandRuntime;
        use crate::framework::commands::hash::{hmset, hset, hsetex};
        for command in ["HSET", "HMSET", "HSETEX"] {
            let mut runtime = hfe_runtime(128);
            runtime.hash_encoding_limits = (1, 64);
            let parts = if command == "HSETEX" {
                vec![command, "h", "FIELDS", "2", "live", "v1", "live", "v2"]
            } else {
                vec![command, "h", "live", "v1", "live", "v2"]
            };
            let args = hfe_args(&parts);
            match command {
                "HSET" => {
                    hset::hset(&mut runtime, &args);
                }
                "HMSET" => {
                    hmset::hmset(&mut runtime, &args);
                }
                _ => {
                    hsetex::hsetex(&mut runtime, &args);
                }
            }
            let db = runtime.db_mut().unwrap();
            assert!(
                matches!(
                    db.get_no_expire(b"h"),
                    Some(crate::foundation::io::store::Robj::HashHashtable(_))
                ),
                "{command}"
            );
            assert_eq!(db.get_hash_field_expire(b"h", b"ghost"), Some(99));
            assert_eq!(db.get_hash_object(b"h").unwrap().unwrap().hash_len(), 4);
            assert_eq!(db.lazy_expired_hash_fields, 0);
        }
    }

    #[test]
    fn hfe_ttl_queries_use_runtime_clock_and_clamp_ignored_expiry() {
        use crate::framework::commands::hash::{hexpiretime, hpexpiretime, hpttl, httl};
        for ignore_expiry in [false, true] {
            let mut runtime = hfe_runtime(128);
            runtime.read_expired_values = ignore_expiry;
            let ghost_ttl = if ignore_expiry { 0 } else { -2 };
            let ghost_expiry = if ignore_expiry { 99 } else { -2 };
            assert_eq!(
                hpttl::hpttl(
                    &mut runtime,
                    &hfe_args(&["HPTTL", "h", "FIELDS", "2", "ghost", "live"])
                )
                .reply,
                Value::Array(vec![Value::integer(ghost_ttl), Value::integer(100)])
            );
            assert_eq!(
                httl::httl(
                    &mut runtime,
                    &hfe_args(&["HTTL", "h", "FIELDS", "2", "ghost", "live"])
                )
                .reply,
                Value::Array(vec![Value::integer(ghost_ttl), Value::integer(0)])
            );
            assert_eq!(
                hpexpiretime::hpexpiretime(
                    &mut runtime,
                    &hfe_args(&["HPEXPIRETIME", "h", "FIELDS", "1", "ghost"])
                )
                .reply,
                Value::Array(vec![Value::integer(ghost_expiry)])
            );
            assert_eq!(
                hexpiretime::hexpiretime(
                    &mut runtime,
                    &hfe_args(&["HEXPIRETIME", "h", "FIELDS", "1", "ghost"])
                )
                .reply,
                Value::Array(vec![Value::integer(if ignore_expiry { 0 } else { -2 })])
            );
        }
    }

    #[test]
    fn hfe_random_single_read_honors_import_source_ghost_fallback() {
        use crate::framework::commands::command_runtime::{CommandRuntime, TestCommandRuntime};
        let mut failures = Vec::new();
        for max_entries in [128, 0] {
            for ignore_expiry in [false, true] {
                let mut runtime = TestCommandRuntime::new(1, 100);
                runtime.read_expired_values = ignore_expiry;
                let db = runtime.db_mut().unwrap();
                db.hash_set_with_limits(b"h", b"ghost", b"value", max_entries, 64)
                    .unwrap();
                db.set_hash_field_expire(b"h", b"ghost", 99);
                let outcome = super::super::hrandfield::hrandfield(
                    &mut runtime,
                    &hfe_args(&["HRANDFIELD", "h"]),
                );
                let expected = if ignore_expiry {
                    Value::bulk(b"ghost".to_vec())
                } else {
                    Value::null()
                };
                if outcome.reply != expected {
                    failures.push((max_entries, ignore_expiry, outcome.reply));
                }
                assert_eq!(
                    runtime
                        .db_mut()
                        .unwrap()
                        .get_hash_object(b"h")
                        .unwrap()
                        .unwrap()
                        .hash_len(),
                    1
                );
            }
        }
        assert!(failures.is_empty(), "unexpected single-field replies: {failures:?}");
    }

    #[test]
    fn hfe_random_count_reads_respect_ignore_policy_without_ghost_reaping() {
        use crate::framework::commands::command_runtime::CommandRuntime;
        use crate::framework::commands::hash::hrandfield;
        for max_entries in [128, 0] {
            for ignore_expiry in [false, true] {
                let mut runtime = hfe_runtime(max_entries);
                runtime.read_expired_values = ignore_expiry;
                let Value::Array(items) = hrandfield::hrandfield(
                    &mut runtime,
                    &hfe_args(&["HRANDFIELD", "h", "100", "WITHVALUES"]),
                )
                .reply
                else {
                    panic!("expected array")
                };
                assert_eq!(items.len(), if ignore_expiry { 8 } else { 6 });
                assert_eq!(
                    items.contains(&Value::bulk(b"ghost".to_vec())),
                    ignore_expiry
                );
                assert_eq!(
                    runtime
                        .db_mut()
                        .unwrap()
                        .get_hash_object(b"h")
                        .unwrap()
                        .unwrap()
                        .hash_len(),
                    4
                );
            }
        }
    }

    #[test]
    fn fields_parser_distinguishes_syntax_integer_and_count_errors() {
        let count_error =
            "ERR numfields should be greater than 0 and match the provided number of fields";
        for (input, expected) in [
            (vec!["key", "other", "1", "field"], "ERR syntax error"),
            (vec!["key", "FIELDS", "other", "field"], NOT_INT),
            (
                vec!["key", "FIELDS", "9223372036854775808", "field"],
                NOT_INT,
            ),
            (vec!["key", "FIELDS", "01", "field"], NOT_INT),
            (vec!["key", "FIELDS", "-1", "field"], count_error),
            (vec!["key", "FIELDS", "0", "field"], count_error),
            (vec!["key", "FIELDS", "2", "field"], count_error),
            (vec!["key", "FIELDS", "1", "first", "second"], count_error),
        ] {
            let args: Vec<_> = input.iter().map(|value| Bytes::from(*value)).collect();
            assert_eq!(
                parse_fields(&args, 1),
                Err(Value::error(expected)),
                "{input:?}"
            );
            let query_error = if expected == count_error {
                "ERR syntax error"
            } else {
                expected
            };
            assert_eq!(
                parse_query_fields(&args, 1),
                Err(Value::error(query_error)),
                "{input:?}"
            );
        }
        let args = [
            Bytes::from_static(b"key"),
            Bytes::from_static(b"fIeLdS"),
            Bytes::from_static(b"2"),
            Bytes::from_static(b"first"),
            Bytes::from_static(b"second"),
        ];
        assert_eq!(
            parse_fields(&args, 1),
            Ok((
                4,
                vec![Bytes::from_static(b"first"), Bytes::from_static(b"second")]
            ))
        );
        assert_eq!(parse_query_fields(&args, 1), parse_fields(&args, 1));
    }
}
