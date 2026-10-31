use crate::engine::command::{CommandArgs, CommandError};
use crate::engine::core::{CommandEffects, CommandOutcome, CoreEvent, Value};
use crate::framework::commands::command_runtime::CommandRuntime;
use bytes::Bytes;
use command_macros::command;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
enum MSetExCondition {
    #[default]
    Any,
    Missing,
    Existing,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
enum MSetExExpiry {
    #[default]
    None,
    KeepTtl,
    At {
        at_ms: i64,
        pxat_token: Option<Bytes>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct MSetExRequest {
    pairs: Vec<(Vec<u8>, Bytes)>,
    condition: MSetExCondition,
    expiry: MSetExExpiry,
}

impl MSetExRequest {
    fn parse(args: &CommandArgs, now_ms: i64) -> Result<Self, Value> {
        if args.args.len() < 3 {
            return Err(CommandError::wrong_arity("msetex"));
        }
        let numkeys = parse_numkeys(&args.args[0])?;
        let options_start = 1 + numkeys * 2;
        if args.args.len() < options_start {
            return Err(CommandError::syntax_error());
        }
        let pairs = (0..numkeys)
            .map(|index| {
                (
                    args.args[1 + index * 2].to_vec(),
                    args.args[2 + index * 2].clone(),
                )
            })
            .collect();
        let (condition, expiry) = parse_msetex_options(&args.args[options_start..], now_ms)?;
        Ok(Self {
            pairs,
            condition,
            expiry,
        })
    }
}

#[command(
    "MSETEX",
    runtime,
    get_keys = crate::framework::commands::key_extractors::msetex_get_keys
)]
pub fn msetex(runtime: &mut dyn CommandRuntime, args: &CommandArgs) -> CommandOutcome {
    let now_ms = runtime.now_ms();
    let request = match MSetExRequest::parse(args, now_ms as i64) {
        Ok(request) => request,
        Err(error) => return CommandOutcome::reply(error),
    };
    let db_index = runtime.selected_db();
    let should_read_expired = runtime.read_expired_values();
    let should_suppress_expire = runtime.suppress_lazy_expire();
    let Some(db) = runtime.db_mut() else {
        return CommandOutcome::reply(CommandError::db_index_out_of_range());
    };
    db.set_no_lazy_expire(should_suppress_expire);
    let is_allowed = condition_allows(db, &request, now_ms, should_read_expired);
    db.set_no_lazy_expire(false);
    if !is_allowed {
        return CommandOutcome {
            reply: Value::integer(0),
            effects: CommandEffects {
                skip_propagate: true,
                ..CommandEffects::default()
            },
        };
    }

    let mut effects = CommandEffects::default();
    for (key, value) in &request.pairs {
        let value = crate::foundation::io::store::Robj::string(value.clone());
        match request.expiry {
            MSetExExpiry::KeepTtl => db.set_keep_ttl(key.clone(), value),
            MSetExExpiry::None => db.set(key.clone(), value),
            MSetExExpiry::At { at_ms, .. } => {
                db.set(key.clone(), value);
                db.set_expire(key, at_ms as u64);
            }
        }
        let key = Bytes::copy_from_slice(key);
        effects.changed_keys.push((db_index, key.clone()));
        effects.events.push(CoreEvent {
            kind: "set",
            key: key.clone(),
        });
        if matches!(request.expiry, MSetExExpiry::At { .. }) {
            effects.events.push(CoreEvent {
                kind: "expire",
                key,
            });
        }
    }
    effects.custom_propagations.push(encode_msetex(&request));
    CommandOutcome {
        reply: Value::integer(1),
        effects,
    }
}

fn condition_allows(
    db: &mut crate::foundation::io::store::kv_engine::Db,
    request: &MSetExRequest,
    now_ms: u64,
    should_read_expired: bool,
) -> bool {
    match request.condition {
        MSetExCondition::Any => true,
        MSetExCondition::Missing | MSetExCondition::Existing => {
            let should_exist = request.condition == MSetExCondition::Existing;
            request.pairs.iter().all(|(key, _)| {
                let has_key = if should_read_expired {
                    db.get_allow_expired(key).is_some()
                } else {
                    db.exists_at(key, now_ms)
                };
                has_key == should_exist
            })
        }
    }
}

fn parse_numkeys(value: &[u8]) -> Result<usize, Value> {
    std::str::from_utf8(value)
        .ok()
        .and_then(|value| value.trim().parse::<i32>().ok())
        .filter(|value| *value > 0)
        .map(|value| value as usize)
        .ok_or_else(|| Value::error("ERR invalid numkeys value or out of range"))
}

fn parse_msetex_options(
    options: &[Bytes],
    now_ms: i64,
) -> Result<(MSetExCondition, MSetExExpiry), Value> {
    let mut condition = MSetExCondition::Any;
    let mut expiry = MSetExExpiry::None;
    let mut expiry_mode: Option<&[u8]> = None;
    let mut expiry_index = None;
    let mut index = 0;
    while index < options.len() {
        let option = &options[index];
        if option.eq_ignore_ascii_case(b"nx") {
            set_condition(&mut condition, MSetExCondition::Missing)?;
            index += 1;
        } else if option.eq_ignore_ascii_case(b"xx") {
            set_condition(&mut condition, MSetExCondition::Existing)?;
            index += 1;
        } else if option.eq_ignore_ascii_case(b"keepttl") {
            if expiry_mode.is_some_and(|mode| !mode.eq_ignore_ascii_case(option)) {
                return Err(CommandError::syntax_error());
            }
            expiry_mode = Some(option);
            expiry = MSetExExpiry::KeepTtl;
            index += 1;
        } else {
            if ![b"EX".as_slice(), b"PX", b"EXAT", b"PXAT"]
                .iter()
                .any(|mode| option.eq_ignore_ascii_case(mode))
                || expiry_mode.is_some_and(|mode| !mode.eq_ignore_ascii_case(option))
                || index + 1 >= options.len()
            {
                return Err(CommandError::syntax_error());
            }
            expiry_mode = Some(option);
            expiry_index = Some(index);
            index += 2;
        }
    }
    if let Some(index) = expiry_index {
        expiry = parse_msetex_expiry(options, index, now_ms)?;
    }
    Ok((condition, expiry))
}

fn set_condition(target: &mut MSetExCondition, value: MSetExCondition) -> Result<(), Value> {
    if *target != MSetExCondition::Any && *target != value {
        return Err(CommandError::syntax_error());
    }
    *target = value;
    Ok(())
}

fn parse_msetex_expiry(
    options: &[Bytes],
    index: usize,
    now_ms: i64,
) -> Result<MSetExExpiry, Value> {
    let option = &options[index];
    let is_ex = option.eq_ignore_ascii_case(b"ex");
    let is_px = option.eq_ignore_ascii_case(b"px");
    let is_exat = option.eq_ignore_ascii_case(b"exat");
    let is_pxat = option.eq_ignore_ascii_case(b"pxat");
    if !is_ex && !is_px && !is_exat && !is_pxat {
        return Err(CommandError::syntax_error());
    }
    if index + 1 >= options.len() {
        return Err(CommandError::syntax_error());
    }
    let value = crate::foundation::io::store::structure::string::encoding::int_encodable(
        &options[index + 1],
    )
    .ok_or_else(|| Value::error("ERR value is not an integer or out of range"))?;
    if value <= 0 {
        return Err(invalid_msetex_expire());
    }
    let absolute = if is_ex {
        value
            .checked_mul(1_000)
            .and_then(|delta| delta.checked_add(now_ms))
    } else if is_px {
        value.checked_add(now_ms)
    } else if is_exat {
        value.checked_mul(1_000)
    } else {
        Some(value)
    };
    absolute
        .map(|at_ms| MSetExExpiry::At {
            at_ms,
            pxat_token: is_pxat.then(|| option.clone()),
        })
        .ok_or_else(invalid_msetex_expire)
}

fn encode_msetex(request: &MSetExRequest) -> Vec<Bytes> {
    let mut parts = vec![
        Bytes::from_static(b"MSETEX"),
        Bytes::from(request.pairs.len().to_string()),
    ];
    for (key, value) in &request.pairs {
        parts.push(Bytes::copy_from_slice(key));
        parts.push(value.clone());
    }
    match request.expiry {
        MSetExExpiry::At {
            at_ms,
            ref pxat_token,
        } => {
            parts.push(
                pxat_token
                    .as_ref()
                    .cloned()
                    .unwrap_or_else(|| Bytes::from_static(b"PXAT")),
            );
            parts.push(Bytes::from(at_ms.to_string()));
        }
        MSetExExpiry::KeepTtl => parts.push(Bytes::from_static(b"KEEPTTL")),
        MSetExExpiry::None => {}
    }
    parts
}

fn invalid_msetex_expire() -> Value {
    Value::error("ERR invalid expire time in 'msetex' command")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::foundation::io::store::Robj;
    use crate::framework::commands::command_runtime::{CommandRuntime, TestCommandRuntime};

    fn import_runtime() -> TestCommandRuntime {
        let mut runtime = TestCommandRuntime::new(1, 1_000);
        runtime.read_expired_values = true;
        let db = runtime.db_mut().unwrap();
        db.set(
            b"expired".to_vec(),
            crate::foundation::io::store::Robj::string(Bytes::from_static(b"original")),
        );
        db.set_expire(b"expired", 1);
        runtime
    }

    #[test]
    fn import_nx_preserves_expired_key_and_entire_batch() {
        let mut runtime = import_runtime();
        let args = CommandArgs::from_parts(
            [
                "MSETEX",
                "2",
                "missing",
                "new",
                "expired",
                "replacement",
                "NX",
            ]
            .into_iter()
            .map(Bytes::from)
            .collect(),
        );

        let outcome = msetex(&mut runtime, &args);

        assert_eq!(outcome.reply, Value::integer(0));
        assert!(outcome.effects.skip_propagate);
        assert!(outcome.effects.changed_keys.is_empty());
        assert!(outcome.effects.events.is_empty());
        assert!(outcome.effects.custom_propagations.is_empty());
        let db = runtime.db_mut().unwrap();
        assert_eq!(
            db.get_string_allow_expired(b"expired"),
            Ok(Some(b"original".as_slice()))
        );
        assert_eq!(db.get_expire(b"expired"), Some(1));
        assert!(db.get_allow_expired(b"missing").is_none());
        assert!(db.expired_deletions.is_empty());
    }

    #[test]
    fn import_xx_updates_expired_key_and_entire_batch() {
        let mut runtime = import_runtime();
        runtime.db_mut().unwrap().set(
            b"live".to_vec(),
            crate::foundation::io::store::Robj::string(Bytes::from_static(b"old")),
        );
        let args = CommandArgs::from_parts(
            ["MSETEX", "2", "expired", "replacement", "live", "new", "XX"]
                .into_iter()
                .map(Bytes::from)
                .collect(),
        );

        let outcome = msetex(&mut runtime, &args);

        assert_eq!(outcome.reply, Value::integer(1));
        assert!(!outcome.effects.skip_propagate);
        assert_eq!(outcome.effects.changed_keys.len(), 2);
        let db = runtime.db_mut().unwrap();
        assert_eq!(
            db.get_string_allow_expired(b"expired"),
            Ok(Some(b"replacement".as_slice()))
        );
        assert_eq!(
            db.get_string_allow_expired(b"live"),
            Ok(Some(b"new".as_slice()))
        );
        assert_eq!(db.get_expire(b"expired"), None);
        assert!(db.expired_deletions.is_empty());
    }

    #[test]
    fn import_xx_missing_later_key_preserves_entire_batch() {
        let mut runtime = import_runtime();
        let args = CommandArgs::from_parts(
            [
                "MSETEX",
                "2",
                "expired",
                "replacement",
                "missing",
                "new",
                "XX",
            ]
            .into_iter()
            .map(Bytes::from)
            .collect(),
        );

        let outcome = msetex(&mut runtime, &args);

        assert_eq!(outcome.reply, Value::integer(0));
        assert!(outcome.effects.skip_propagate);
        assert!(outcome.effects.changed_keys.is_empty());
        assert!(outcome.effects.events.is_empty());
        assert!(outcome.effects.custom_propagations.is_empty());
        let db = runtime.db_mut().unwrap();
        assert_eq!(
            db.get_string_allow_expired(b"expired"),
            Ok(Some(b"original".as_slice()))
        );
        assert_eq!(db.get_expire(b"expired"), Some(1));
        assert!(db.get_allow_expired(b"missing").is_none());
        assert!(db.expired_deletions.is_empty());
    }

    #[test]
    fn import_duplicate_keys_check_before_writing_and_keep_ttl() {
        for (condition, expected, value) in [
            ("NX", 0, b"original".as_slice()),
            ("XX", 1, b"last".as_slice()),
        ] {
            let mut runtime = import_runtime();
            let args = CommandArgs::from_parts(
                [
                    "MSETEX", "2", "expired", "first", "expired", "last", condition, "KEEPTTL",
                ]
                .into_iter()
                .map(Bytes::from)
                .collect(),
            );

            let outcome = msetex(&mut runtime, &args);

            assert_eq!(outcome.reply, Value::integer(expected), "{condition}");
            assert_eq!(outcome.effects.skip_propagate, expected == 0);
            let db = runtime.db_mut().unwrap();
            assert_eq!(db.get_string_allow_expired(b"expired"), Ok(Some(value)));
            assert_eq!(db.get_expire(b"expired"), Some(1));
            assert!(db.expired_deletions.is_empty());
        }
    }

    #[test]
    fn ordinary_conditions_hide_expired_keys_and_restore_lazy_expire() {
        for should_suppress_expire in [false, true] {
            for (condition, expected) in [("NX", 1), ("XX", 0)] {
                let mut runtime = import_runtime();
                runtime.read_expired_values = false;
                runtime.suppress_lazy_expire = should_suppress_expire;
                let args = CommandArgs::from_parts(
                    ["MSETEX", "1", "expired", "replacement", condition]
                        .into_iter()
                        .map(Bytes::from)
                        .collect(),
                );

                let outcome = msetex(&mut runtime, &args);

                assert_eq!(outcome.reply, Value::integer(expected), "{condition}");
                assert_eq!(outcome.effects.skip_propagate, expected == 0);
                let db = runtime.db_mut().unwrap();
                let retained = if expected == 1 {
                    Some(b"replacement".as_slice())
                } else if should_suppress_expire {
                    Some(b"original".as_slice())
                } else {
                    None
                };
                assert_eq!(db.get_string_allow_expired(b"expired"), Ok(retained));
                assert_eq!(db.expired_deletions.is_empty(), should_suppress_expire);
                db.expired_deletions.clear();
                db.set(b"probe".to_vec(), Robj::string("value"));
                db.set_expire(b"probe", 1);
                assert!(!db.exists_at(b"probe", 1_000));
                assert!(db.get_allow_expired(b"probe").is_none());
                assert_eq!(db.expired_deletions, vec![b"probe".to_vec()]);
            }
        }
    }

    #[test]
    fn import_set_then_xx_sequence_keeps_expired_value_visible() {
        let mut runtime = TestCommandRuntime::new(1, 1_000);
        runtime.read_expired_values = true;
        let set_args = CommandArgs::from_parts(
            ["SET", "expired", "original", "PXAT", "1"]
                .into_iter()
                .map(Bytes::from)
                .collect(),
        );
        assert_eq!(
            super::super::set::set(&mut runtime, &set_args).reply,
            Value::ok()
        );
        let args = CommandArgs::from_parts(
            ["MSETEX", "1", "expired", "replacement", "XX", "KEEPTTL"]
                .into_iter()
                .map(Bytes::from)
                .collect(),
        );

        let outcome = msetex(&mut runtime, &args);

        assert_eq!(outcome.reply, Value::integer(1));
        let db = runtime.db_mut().unwrap();
        assert_eq!(
            db.get_string_allow_expired(b"expired"),
            Ok(Some(b"replacement".as_slice()))
        );
        assert_eq!(db.get_expire(b"expired"), Some(1));
        assert!(db.expired_deletions.is_empty());
    }

    #[test]
    fn request_parses_pairs_condition_and_absolute_expiry() {
        let args = CommandArgs::from_parts(vec![
            Bytes::from_static(b"MSETEX"),
            Bytes::from_static(b"1"),
            Bytes::from_static(b"key"),
            Bytes::from_static(b"value"),
            Bytes::from_static(b"NX"),
            Bytes::from_static(b"PXAT"),
            Bytes::from_static(b"4000000000000"),
        ]);

        let request = MSetExRequest::parse(&args, 1_000).unwrap();
        assert_eq!(request.condition, MSetExCondition::Missing);
        assert_eq!(
            request.expiry,
            MSetExExpiry::At {
                at_ms: 4_000_000_000_000,
                pxat_token: Some(Bytes::from_static(b"PXAT")),
            }
        );
        assert_eq!(request.pairs.len(), 1);
    }

    #[test]
    fn past_deadline_writes_all_pairs_and_propagates() {
        let mut runtime = TestCommandRuntime::new(1, 1_000);
        let args = CommandArgs::from_parts(
            ["MSETEX", "2", "first", "a", "second", "b", "EXAT", "1"]
                .into_iter()
                .map(Bytes::from)
                .collect(),
        );
        let outcome = msetex(&mut runtime, &args);
        assert_eq!(outcome.reply, Value::integer(1));
        for (key, value) in [(b"first".as_slice(), b"a".as_slice()), (b"second", b"b")] {
            assert_eq!(
                runtime
                    .db_mut()
                    .unwrap()
                    .get_allow_expired(key)
                    .unwrap()
                    .as_bytes(),
                value
            );
            assert_eq!(runtime.db_mut().unwrap().get_expire(key), Some(1_000));
        }
        assert!(!outcome.effects.skip_propagate);
        assert_eq!(
            outcome.effects.custom_propagations,
            vec![["MSETEX", "2", "first", "a", "second", "b", "PXAT", "1000"]
                .into_iter()
                .map(Bytes::from)
                .collect::<Vec<_>>()]
        );
        assert!(runtime.db_mut().unwrap().expired_deletions.is_empty());
        assert_eq!(
            outcome
                .effects
                .events
                .iter()
                .map(|event| event.kind)
                .collect::<Vec<_>>(),
            vec!["set", "expire", "set", "expire"]
        );
    }

    #[test]
    fn repeated_deadlines_and_complete_syntax_preserve_atomicity() {
        for (options, expected, deadline) in [
            (
                vec!["PX", "bogus", "PX", "60000"],
                Value::integer(1),
                Some(61_000),
            ),
            (vec!["PXAT", "0", "PXAT", "1"], Value::integer(1), Some(1)),
            (vec!["KEEPTTL", "KEEPTTL"], Value::integer(1), None),
            (
                vec!["PX", "+1", "invalid"],
                Value::error("ERR syntax error"),
                None,
            ),
            (
                vec!["PX", "bogus", "EX", "1"],
                Value::error("ERR syntax error"),
                None,
            ),
            (
                vec!["PX", "+1"],
                Value::error("ERR value is not an integer or out of range"),
                None,
            ),
        ] {
            let mut runtime = TestCommandRuntime::new(1, 1_000);
            let args = CommandArgs::from_parts(
                ["MSETEX", "1", "key", "value"]
                    .into_iter()
                    .chain(options.iter().copied())
                    .map(|part| Bytes::copy_from_slice(part.as_bytes()))
                    .collect(),
            );
            let outcome = msetex(&mut runtime, &args);
            assert_eq!(outcome.reply, expected, "{options:?}");
            assert_eq!(runtime.db_mut().unwrap().get_expire(b"key"), deadline);
            if matches!(expected, Value::Error(_)) {
                assert!(runtime
                    .db_mut()
                    .unwrap()
                    .get_allow_expired(b"key")
                    .is_none());
                assert!(outcome.effects.changed_keys.is_empty());
            }
        }
    }
}
