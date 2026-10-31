use bytes::Bytes;
use command_macros::command;
use rustc_apfloat::Float;

use crate::engine::command::{CommandArgs, CommandError};
use crate::engine::core::{CommandEffects, CommandOutcome, CoreEvent, Value};
use crate::foundation::io::protocol::DecimalDoubleText;
use crate::foundation::io::store::structure::string::encoding::int_encodable;
use crate::foundation::io::store::Robj;
use crate::foundation::memory::numeric::{
    add_long_double, format_long_double_human, parse_long_double, sub_long_double, LongDouble,
};
use crate::framework::commands::command_runtime::CommandRuntime;

#[command("INCREX", runtime)]
pub fn increx(runtime: &mut dyn CommandRuntime, args: &CommandArgs) -> CommandOutcome {
    let [key, options @ ..] = args.args.as_slice() else {
        return CommandOutcome::reply(CommandError::wrong_arity("increx"));
    };
    let options = match parse_options(options) {
        Ok(options) => options,
        Err(error) => return CommandOutcome::reply(error),
    };
    let now_ms = runtime.now_ms();
    let deadline_ms = match options.deadline_ms(now_ms) {
        Ok(deadline) => deadline,
        Err(error) => return CommandOutcome::reply(error),
    };
    let increment = match options.increment() {
        Ok(increment) => increment,
        Err(error) => return CommandOutcome::reply(error),
    };
    let read_expired = runtime.read_expired_values();
    let suppress_lazy_expire = runtime.suppress_lazy_expire();
    let current = {
        let Some(db) = runtime.db_mut() else {
            return CommandOutcome::reply(CommandError::db_index_out_of_range());
        };
        db.set_no_lazy_expire(suppress_lazy_expire);
        let object = if read_expired {
            db.get_allow_expired(key)
        } else {
            db.get_at(key, now_ms)
        };
        let current = match object {
            Some(object) if object.is_string() => {
                Ok(Some(Bytes::copy_from_slice(object.as_bytes())))
            }
            Some(_) => Err(CommandError::wrong_type()),
            None => Ok(None),
        };
        db.set_no_lazy_expire(false);
        match current {
            Ok(current) => current,
            Err(error) => return CommandOutcome::reply(error),
        }
    };
    let is_declined = match options.condition {
        Some(Condition::Nx) => current.is_some(),
        Some(Condition::Xx) => current.is_none(),
        None => false,
    };
    let calculated = match calculate(increment, current.as_deref(), is_declined) {
        Ok(calculated) => calculated,
        Err(error) => return CommandOutcome::reply(error),
    };
    if !calculated.should_write {
        return CommandOutcome {
            reply: calculated.reply,
            effects: CommandEffects {
                skip_propagate: true,
                ..CommandEffects::default()
            },
        };
    }
    let db_index = runtime.selected_db();
    if deadline_ms.is_some_and(|deadline| deadline <= now_ms)
        && !runtime.preserves_expired_deadline()
    {
        let effects = CommandEffects {
            skip_propagate: true,
            ..CommandEffects::default()
        };
        if current.is_some() {
            let Some(db) = runtime.db_mut() else {
                return CommandOutcome::reply(CommandError::db_index_out_of_range());
            };
            if db.del(key) {
                db.expired_deletions.push(key.to_vec());
            }
        }
        return CommandOutcome {
            reply: calculated.reply,
            effects,
        };
    }
    let should_mark_expire = runtime.should_mark_replica_local_expire();
    let Some(db) = runtime.db_mut() else {
        return CommandOutcome::reply(CommandError::db_index_out_of_range());
    };
    db.set_keep_ttl(key.to_vec(), Robj::string(calculated.stored.clone()));
    if let Some(deadline) = deadline_ms {
        db.set_expire(key, deadline);
        if should_mark_expire {
            db.mark_replica_local_expire(key);
        }
    }
    let mut effects = CommandEffects {
        changed_keys: vec![(db_index, key.clone())],
        events: vec![CoreEvent {
            kind: if matches!(increment, Increment::Float(_)) {
                "incrbyfloat"
            } else {
                "incrby"
            },
            key: key.clone(),
        }],
        ..CommandEffects::default()
    };
    if let Some(deadline) = deadline_ms {
        effects.events.push(CoreEvent {
            kind: "expire",
            key: key.clone(),
        });
        effects.custom_propagations.push(vec![
            Bytes::from_static(b"SET"),
            key.clone(),
            calculated.stored,
            Bytes::from_static(b"PXAT"),
            Bytes::from(deadline.to_string()),
        ]);
    } else if matches!(increment, Increment::Float(_)) {
        effects.custom_propagations.push(vec![
            Bytes::from_static(b"SET"),
            key.clone(),
            calculated.stored,
            Bytes::from_static(b"KEEPTTL"),
        ]);
    }
    CommandOutcome {
        reply: calculated.reply,
        effects,
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Condition {
    Nx,
    Xx,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Expiry {
    Ex,
    Px,
    Exat,
    Pxat,
}

struct Options<'a> {
    condition: Option<Condition>,
    expiry: Option<(Expiry, &'a [u8])>,
    increment: Option<(bool, &'a [u8])>,
}

fn parse_options(options: &[Bytes]) -> Result<Options<'_>, Value> {
    let mut parsed = Options {
        condition: None,
        expiry: None,
        increment: None,
    };
    let mut index = 0;
    while let Some(option) = options.get(index) {
        if option.eq_ignore_ascii_case(b"NX") || option.eq_ignore_ascii_case(b"XX") {
            let condition = if option.eq_ignore_ascii_case(b"NX") {
                Condition::Nx
            } else {
                Condition::Xx
            };
            if parsed
                .condition
                .is_some_and(|previous| previous != condition)
            {
                return Err(CommandError::syntax_error());
            }
            parsed.condition = Some(condition);
            index += 1;
            continue;
        }
        let expiry = if option.eq_ignore_ascii_case(b"EX") {
            Some(Expiry::Ex)
        } else if option.eq_ignore_ascii_case(b"PX") {
            Some(Expiry::Px)
        } else if option.eq_ignore_ascii_case(b"EXAT") {
            Some(Expiry::Exat)
        } else if option.eq_ignore_ascii_case(b"PXAT") {
            Some(Expiry::Pxat)
        } else {
            None
        };
        let value = options
            .get(index + 1)
            .ok_or_else(CommandError::syntax_error)?;
        if let Some(expiry) = expiry {
            if parsed
                .expiry
                .is_some_and(|(previous, _)| previous != expiry)
            {
                return Err(CommandError::syntax_error());
            }
            parsed.expiry = Some((expiry, value));
        } else if option.eq_ignore_ascii_case(b"BYINT") || option.eq_ignore_ascii_case(b"BYFLOAT") {
            if parsed.increment.is_some() {
                return Err(CommandError::syntax_error());
            }
            parsed.increment = Some((option.eq_ignore_ascii_case(b"BYFLOAT"), value));
        } else {
            return Err(CommandError::syntax_error());
        }
        index += 2;
    }
    Ok(parsed)
}

impl Options<'_> {
    fn deadline_ms(&self, now_ms: u64) -> Result<Option<u64>, Value> {
        let Some((expiry, text)) = self.expiry else {
            return Ok(None);
        };
        let value = int_encodable(text)
            .ok_or_else(|| Value::error("ERR value is not an integer or out of range"))?;
        let invalid = || Value::error("ERR invalid expire time in 'increx' command");
        if value <= 0 {
            return Err(invalid());
        }
        let milliseconds = if matches!(expiry, Expiry::Ex | Expiry::Exat) {
            value.checked_mul(1000).ok_or_else(invalid)?
        } else {
            value
        };
        let deadline = if matches!(expiry, Expiry::Ex | Expiry::Px) {
            let now_ms = i64::try_from(now_ms).map_err(|_| invalid())?;
            milliseconds.checked_add(now_ms).ok_or_else(invalid)?
        } else {
            milliseconds
        };
        Ok(Some(deadline as u64))
    }

    fn increment(&self) -> Result<Increment, Value> {
        match self.increment {
            None => Ok(Increment::Int(1)),
            Some((false, text)) => int_encodable(text)
                .map(Increment::Int)
                .ok_or_else(|| Value::error("ERR Increment is not an integer or out of range")),
            Some((true, text)) => parse_long_double(text)
                .map(Increment::Float)
                .map_err(|_| Value::error("ERR Increment is not a valid float")),
        }
    }
}

#[derive(Clone, Copy)]
enum Increment {
    Int(i64),
    Float(LongDouble),
}

struct Calculated {
    reply: Value,
    stored: Bytes,
    should_write: bool,
}

fn calculate(
    increment: Increment,
    current: Option<&[u8]>,
    is_declined: bool,
) -> Result<Calculated, Value> {
    match increment {
        Increment::Int(increment) => {
            let old = match current {
                Some(text) => int_encodable(text)
                    .ok_or_else(|| Value::error("ERR value is not an integer or out of range"))?,
                None => 0,
            };
            let next = if is_declined {
                None
            } else {
                old.checked_add(increment)
            };
            let value = next.unwrap_or(old);
            Ok(Calculated {
                reply: Value::Array(vec![
                    Value::Integer(value),
                    Value::Integer(if next.is_some() { increment } else { 0 }),
                ]),
                stored: Bytes::from(value.to_string()),
                should_write: next.is_some(),
            })
        }
        Increment::Float(increment) => {
            let old = match current {
                Some(text) => parse_long_double(text)
                    .map_err(|_| Value::error("ERR value is not a valid float"))?,
                None => LongDouble::ZERO,
            };
            if is_declined {
                return float_result(old, LongDouble::ZERO, false);
            }
            if increment.is_infinite() {
                return Err(Value::error("ERR BYFLOAT increment cannot be Infinity"));
            }
            if old.is_infinite() {
                return Err(Value::error("ERR value cannot be Infinity"));
            }
            let next = add_long_double(old, increment).value;
            if next.is_nan() {
                return Err(Value::error("ERR Increment is not a valid float"));
            }
            if next.is_infinite() {
                return float_result(old, LongDouble::ZERO, false);
            }
            float_result(next, sub_long_double(next, old).value, true)
        }
    }
}

fn float_result(
    value: LongDouble,
    delta: LongDouble,
    should_write: bool,
) -> Result<Calculated, Value> {
    let invalid = || Value::error("ERR failed to format numeric reply");
    let stored = Bytes::from(format_long_double_human(value).map_err(|_| invalid())?);
    let delta = Bytes::from(format_long_double_human(delta).map_err(|_| invalid())?);
    let value = DecimalDoubleText::new(stored.clone()).map_err(|_| invalid())?;
    let delta = DecimalDoubleText::new(delta).map_err(|_| invalid())?;
    Ok(Calculated {
        reply: Value::Array(vec![
            Value::DecimalDouble(value),
            Value::DecimalDouble(delta),
        ]),
        stored,
        should_write,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::framework::commands::command_runtime::TestCommandRuntime;

    fn command(options: &[&str]) -> CommandArgs {
        CommandArgs::from_parts(
            ["INCREX", "key"]
                .into_iter()
                .chain(options.iter().copied())
                .map(|text| Bytes::from(text.to_owned()))
                .collect(),
        )
    }

    #[test]
    fn increx_options_validate_order_repetition_and_ranges() {
        for options in [
            vec!["NX", "NX", "EX", "1", "EX", "2", "BYINT", "0"],
            vec!["BYFLOAT", ".5", "PXAT", "9223372036854775807", "XX", "XX"],
            vec![],
        ] {
            let args = command(&options);
            let parsed = parse_options(&args.args[1..]).unwrap();
            assert!(parsed.deadline_ms(1_000).is_ok());
            assert!(parsed.increment().is_ok());
        }
        for options in [
            vec!["NX", "XX"],
            vec!["EX", "1", "PX", "1"],
            vec!["BYINT", "1", "BYINT", "2"],
            vec!["BYFLOAT", "1", "BYINT", "2"],
            vec!["BYFLOAT"],
            vec!["unknown", "1"],
            vec!["EX", "1", "unknown"],
        ] {
            let args = command(&options);
            assert_eq!(
                parse_options(&args.args[1..]).err(),
                Some(CommandError::syntax_error())
            );
        }
        for (options, message) in [
            (
                vec!["EX", "0"],
                "ERR invalid expire time in 'increx' command",
            ),
            (
                vec!["EX", "9223372036854775807"],
                "ERR invalid expire time in 'increx' command",
            ),
            (
                vec!["PX", "9223372036854775807"],
                "ERR invalid expire time in 'increx' command",
            ),
            (
                vec!["PXAT", "+1"],
                "ERR value is not an integer or out of range",
            ),
        ] {
            let args = command(&options);
            assert_eq!(
                parse_options(&args.args[1..])
                    .unwrap()
                    .deadline_ms(1_000)
                    .err(),
                Some(Value::error(message))
            );
        }
        for text in ["+1", "01", "-0", "9223372036854775808"] {
            let args = command(&["BYINT", text]);
            assert_eq!(
                parse_options(&args.args[1..]).unwrap().increment().err(),
                Some(Value::error(
                    "ERR Increment is not an integer or out of range"
                ))
            );
        }
    }

    #[test]
    fn increx_integer_calculation_reports_applied_delta() {
        for (old, increment, declined, expected, delta, should_write) in [
            (None, 1, false, 1, 1, true),
            (Some("2"), -3, false, -1, -3, true),
            (Some("2"), 0, false, 2, 0, true),
            (Some("2"), 3, true, 2, 0, false),
            (Some("9223372036854775807"), 1, false, i64::MAX, 0, false),
            (Some("-9223372036854775808"), -1, false, i64::MIN, 0, false),
        ] {
            let result =
                calculate(Increment::Int(increment), old.map(str::as_bytes), declined).unwrap();
            assert_eq!(
                result.reply,
                Value::Array(vec![Value::Integer(expected), Value::Integer(delta)])
            );
            assert_eq!(result.stored, Bytes::from(expected.to_string()));
            assert_eq!(result.should_write, should_write);
        }
        assert_eq!(
            calculate(Increment::Int(1), Some(b"invalid"), true).err(),
            Some(Value::error("ERR value is not an integer or out of range"))
        );
    }

    #[test]
    fn increx_decimal_calculation_preserves_condition_and_infinity_order() {
        let half = Increment::Float(parse_long_double(b".5").unwrap());
        let result = calculate(half, Some(b"1.25"), false).unwrap();
        assert_eq!(result.stored, b"1.75".as_slice());
        assert_eq!(
            result.reply,
            Value::Array(vec![
                Value::DecimalDouble(DecimalDoubleText::new("1.75").unwrap()),
                Value::DecimalDouble(DecimalDoubleText::new("0.5").unwrap()),
            ])
        );
        let infinite = Increment::Float(LongDouble::INFINITY);
        assert_eq!(
            calculate(infinite, None, false).err(),
            Some(Value::error("ERR BYFLOAT increment cannot be Infinity"))
        );
        assert_eq!(
            calculate(half, Some(b"inf"), false).err(),
            Some(Value::error("ERR value cannot be Infinity"))
        );
        assert_eq!(
            calculate(half, Some(b"invalid"), true).err(),
            Some(Value::error("ERR value is not a valid float"))
        );
        let declined = calculate(infinite, Some(b"-infinity"), true).unwrap();
        assert!(!declined.should_write);
        assert_eq!(declined.stored, b"-inf".as_slice());
        let overflow = calculate(
            Increment::Float(LongDouble::largest()),
            Some(
                format_long_double_human(LongDouble::largest())
                    .unwrap()
                    .as_bytes(),
            ),
            false,
        )
        .unwrap();
        assert!(!overflow.should_write);
    }

    #[test]
    fn increx_write_effects_preserve_storage_ttl_and_rewritten_arguments() {
        let now_ms = crate::foundation::io::store::kv_engine::now_ms();
        for (options, expected, event, deadline, rewritten) in [
            (vec![], "3", "incrby", now_ms + 20_000, vec![]),
            (
                vec!["BYFLOAT", ".5"],
                "2.5",
                "incrbyfloat",
                now_ms + 20_000,
                vec![
                    "SET".to_owned(),
                    "key".to_owned(),
                    "2.5".to_owned(),
                    "KEEPTTL".to_owned(),
                ],
            ),
            (
                vec!["PX", "50000", "BYINT", "0"],
                "2",
                "incrby",
                now_ms + 50_000,
                vec![
                    "SET".to_owned(),
                    "key".to_owned(),
                    "2".to_owned(),
                    "PXAT".to_owned(),
                    (now_ms + 50_000).to_string(),
                ],
            ),
        ] {
            let mut runtime = TestCommandRuntime::new(1, now_ms);
            let db = runtime.db_mut().unwrap();
            db.set(b"key".to_vec(), Robj::string("2"));
            db.set_expire(b"key", now_ms + 20_000);
            let outcome = increx(&mut runtime, &command(&options));
            let db = runtime.db_mut().unwrap();
            assert_eq!(db.get(b"key").unwrap().as_bytes(), expected.as_bytes());
            assert_eq!(db.get_expire(b"key"), Some(deadline));
            assert_eq!(
                outcome.effects.changed_keys,
                vec![(0, Bytes::from_static(b"key"))]
            );
            assert_eq!(outcome.effects.events[0].kind, event);
            assert_eq!(
                outcome.effects.events.len(),
                if options.contains(&"PX") { 2 } else { 1 }
            );
            let actual: Vec<Vec<String>> = outcome
                .effects
                .custom_propagations
                .iter()
                .map(|args| {
                    args.iter()
                        .map(|arg| String::from_utf8(arg.to_vec()).unwrap())
                        .collect()
                })
                .collect();
            assert_eq!(
                actual,
                if rewritten.is_empty() {
                    vec![]
                } else {
                    vec![rewritten]
                }
            );
        }
    }

    #[test]
    fn increx_immediate_expiry_and_preserved_deadline_use_distinct_effects() {
        let now_ms = crate::foundation::io::store::kv_engine::now_ms();
        for (exists, preserve) in [(true, false), (false, false), (true, true)] {
            let mut runtime = TestCommandRuntime::new(1, now_ms);
            runtime.read_expired_values = preserve;
            if exists {
                runtime
                    .db_mut()
                    .unwrap()
                    .set(b"key".to_vec(), Robj::string("2"));
            }
            let outcome = increx(&mut runtime, &command(&["PXAT", "1", "BYINT", "3"]));
            assert_eq!(
                outcome.reply,
                Value::Array(vec![
                    Value::Integer(if exists { 5 } else { 3 }),
                    Value::Integer(3)
                ])
            );
            let db = runtime.db_mut().unwrap();
            if preserve {
                assert_eq!(db.get_allow_expired(b"key").unwrap().as_bytes(), b"5");
                assert_eq!(db.get_expire(b"key"), Some(1));
                assert!(db.expired_deletions.is_empty());
                assert_eq!(
                    outcome.effects.custom_propagations[0][3],
                    b"PXAT".as_slice()
                );
            } else {
                assert!(db.get_allow_expired(b"key").is_none());
                assert!(outcome.effects.skip_propagate);
                assert!(outcome.effects.custom_propagations.is_empty());
                assert!(outcome.effects.changed_keys.is_empty());
                assert_eq!(
                    db.expired_deletions,
                    if exists {
                        vec![b"key".to_vec()]
                    } else {
                        vec![]
                    }
                );
            }
        }
    }

    #[test]
    fn increx_declined_operation_preserves_value_deadline_and_effects() {
        let now_ms = crate::foundation::io::store::kv_engine::now_ms();
        let mut runtime = TestCommandRuntime::new(1, now_ms);
        let db = runtime.db_mut().unwrap();
        db.set(b"key".to_vec(), Robj::string("2"));
        db.set_expire(b"key", now_ms + 20_000);
        let outcome = increx(&mut runtime, &command(&["NX", "PXAT", "1"]));
        assert_eq!(
            outcome.reply,
            Value::Array(vec![Value::Integer(2), Value::Integer(0)])
        );
        let db = runtime.db_mut().unwrap();
        assert_eq!(db.get(b"key").unwrap().as_bytes(), b"2");
        assert_eq!(db.get_expire(b"key"), Some(now_ms + 20_000));
        assert!(db.expired_deletions.is_empty());
        assert!(outcome.effects.skip_propagate);
        assert!(outcome.effects.changed_keys.is_empty());
        assert!(outcome.effects.events.is_empty());
    }
}
