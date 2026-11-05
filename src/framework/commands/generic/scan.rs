use crate::engine::command::{CommandArgs, CommandError};
use crate::engine::core::{CommandOutcome, Value};
use crate::framework::commands::command_runtime::CommandRuntime;
use crate::framework::commands::pattern::string_match;
use command_macros::command;

#[command("SCAN", runtime)]
pub fn scan(runtime: &mut dyn CommandRuntime, args: &CommandArgs) -> CommandOutcome {
    let Some((cursor, options)) = args.args.split_first() else {
        return CommandOutcome::reply(CommandError::wrong_arity("scan"));
    };
    let Some(cursor) = std::str::from_utf8(cursor)
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
    else {
        return CommandOutcome::reply(Value::error("ERR invalid cursor"));
    };
    let mut count = 10usize;
    let mut pattern = None;
    let mut type_filter = None;
    let mut index = 0;
    while index < options.len() {
        if options[index].eq_ignore_ascii_case(b"count") && index + 1 < options.len() {
            count = match std::str::from_utf8(&options[index + 1])
                .ok()
                .and_then(|value| value.parse().ok())
            {
                Some(value) if value > 0 => value,
                _ => return CommandOutcome::reply(Value::error("ERR invalid count")),
            };
            index += 2;
        } else if options[index].eq_ignore_ascii_case(b"match") && index + 1 < options.len() {
            pattern = Some(options[index + 1].as_ref());
            index += 2;
        } else if options[index].eq_ignore_ascii_case(b"type") && index + 1 < options.len() {
            let value = options[index + 1].to_ascii_lowercase();
            if !matches!(
                value.as_slice(),
                b"string"
                    | b"list"
                    | b"hash"
                    | b"set"
                    | b"zset"
                    | b"stream"
                    | b"pathhash"
            ) {
                return CommandOutcome::reply(Value::error(format!(
                    "unknown type name '{}'",
                    String::from_utf8_lossy(&value)
                )));
            }
            type_filter = Some(value);
            index += 2;
        } else {
            return CommandOutcome::reply(CommandError::syntax_error());
        }
    }
    runtime.reset_replication_selected_db();
    let suppress_lazy_expire = runtime.suppress_lazy_expire();
    let Some(db) = runtime.db_mut() else {
        return CommandOutcome::reply(CommandError::db_index_out_of_range());
    };
    db.set_no_lazy_expire(suppress_lazy_expire);
    let (next, mut keys) = db.scan_include_expired(cursor, count);
    if let Some(type_filter) = type_filter {
        keys.retain(|key| {
            db.get_allow_expired(key).is_some_and(|value| {
                value
                    .type_name()
                    .as_bytes()
                    .eq_ignore_ascii_case(&type_filter)
            })
        });
    }
    if let Some(pattern) = pattern {
        keys.retain(|key| string_match(pattern, key, false));
    }
    if !suppress_lazy_expire {
        let mut live_keys = Vec::with_capacity(keys.len());
        for key in keys {
            db.set_no_lazy_expire(true);
            let expired = db.is_expired(&key);
            db.set_no_lazy_expire(false);
            if expired {
                db.del(&key);
                db.expired_deletions.push(key);
            } else {
                live_keys.push(key);
            }
        }
        keys = live_keys;
    }
    db.set_no_lazy_expire(false);
    keys.sort();
    CommandOutcome::reply(Value::Array(vec![
        Value::bulk(next.to_string()),
        Value::Array(keys.into_iter().map(Value::bulk).collect()),
    ]))
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use crate::engine::command::CommandArgs;
    use crate::engine::core::Value;
    use crate::foundation::io::store::Robj;
    use crate::framework::commands::command_runtime::TestCommandRuntime;

    #[test]
    fn scan_matches_character_classes_ranges_negation_and_escapes() {
        let mut runtime = TestCommandRuntime::new(1, 0);
        let db = runtime.dbs.get_mut(0).expect("database zero");
        for key in [
            b"abc".as_slice(),
            b"bbc",
            b"cbc",
            b"a*c",
            b"a?c",
            b"a[c",
            b"a\0c",
        ] {
            db.set(key.to_vec(), Robj::string("value"));
        }
        for (pattern, expected) in [
            (b"[a]bc".as_slice(), vec![b"abc".as_slice()]),
            (b"[a-b]bc", vec![b"abc".as_slice(), b"bbc".as_slice()]),
            (b"[^ab]bc", vec![b"cbc".as_slice()]),
            (br"a\*c", vec![b"a*c".as_slice()]),
            (br"a\?c", vec![b"a?c".as_slice()]),
            (br"a\[c", vec![b"a[c".as_slice()]),
            (b"a\0c", vec![b"a\0c".as_slice()]),
        ] {
            let mut cursor = Bytes::from_static(b"0");
            let mut matches = Vec::new();
            loop {
                let args = CommandArgs::from_parts(vec![
                    Bytes::from_static(b"SCAN"),
                    cursor,
                    Bytes::from_static(b"MATCH"),
                    Bytes::copy_from_slice(pattern),
                    Bytes::from_static(b"COUNT"),
                    Bytes::from_static(b"1"),
                ]);
                let Value::Array(reply) = super::scan(&mut runtime, &args).reply else {
                    panic!("SCAN must return an array");
                };
                let [Value::Bulk(next), Value::Array(keys)] = reply.as_slice() else {
                    panic!("SCAN must return a cursor and matching keys");
                };
                matches.extend(keys.iter().cloned());
                if next.as_ref() == b"0" {
                    break;
                }
                cursor = next.clone();
            }
            let expected: Vec<_> = expected
                .into_iter()
                .map(|key| Value::bulk(key.to_vec()))
                .collect();
            // A stable dictionary visits each key once; ignore bucket order.
            assert_eq!(matches.len(), expected.len());
            for key in expected {
                assert!(matches.contains(&key), "missing match {key:?}");
            }
        }
    }
}
