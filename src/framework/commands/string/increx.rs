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
    let increment = match options.increment() {
        Ok(increment) => increment,
        Err(error) => return CommandOutcome::reply(error),
    };
    let operation = match options.operation(increment) {
        Ok(operation) => operation,
        Err(error) => return CommandOutcome::reply(error),
    };
    if options.expire_nx && options.expiry.is_none() {
        return CommandOutcome::reply(Value::error("ERR ENX flag requires an expiration"));
    }
    let now_ms = runtime.now_ms();
    let mut deadline_ms = match options.deadline_ms(now_ms) {
        Ok(deadline) => deadline,
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
    let calculated =
        match operation.calculate(current.as_deref(), is_declined, options.should_saturate) {
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
    if options.expire_nx
        && runtime
            .db_mut()
            .is_some_and(|db| db.get_expire(key).is_some())
    {
        deadline_ms = None;
    }
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
    let did_persist = options.should_persist && db.persist(key);
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
    } else {
        let mut propagation = vec![Bytes::from_static(b"SET"), key.clone(), calculated.stored];
        if !options.should_persist {
            propagation.push(Bytes::from_static(b"KEEPTTL"));
        }
        if did_persist {
            effects.events.push(CoreEvent {
                kind: "persist",
                key: key.clone(),
            });
        }
        effects.custom_propagations.push(propagation);
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

#[derive(Default)]
struct Options<'a> {
    condition: Option<Condition>,
    expiry: Option<(Expiry, &'a [u8])>,
    increment: Option<(bool, &'a [u8])>,
    lower: Option<&'a [u8]>,
    upper: Option<&'a [u8]>,
    should_saturate: bool,
    should_persist: bool,
    expire_nx: bool,
}

fn parse_options(options: &[Bytes]) -> Result<Options<'_>, Value> {
    let mut parsed = Options::default();
    let mut index = 0;
    while let Some(option) = options.get(index) {
        if option.eq_ignore_ascii_case(b"NX") || option.eq_ignore_ascii_case(b"XX") {
            let condition = if option.eq_ignore_ascii_case(b"NX") {
                Condition::Nx
            } else {
                Condition::Xx
            };
            if parsed.condition.is_some() {
                return Err(CommandError::syntax_error());
            }
            parsed.condition = Some(condition);
            index += 1;
            continue;
        }
        let flag = if option.eq_ignore_ascii_case(b"SATURATE") {
            Some(&mut parsed.should_saturate)
        } else if option.eq_ignore_ascii_case(b"PERSIST") {
            if parsed.expiry.is_some() || parsed.expire_nx {
                return Err(CommandError::syntax_error());
            }
            Some(&mut parsed.should_persist)
        } else if option.eq_ignore_ascii_case(b"ENX") {
            if parsed.should_persist {
                return Err(CommandError::syntax_error());
            }
            Some(&mut parsed.expire_nx)
        } else {
            None
        };
        if let Some(flag) = flag {
            if *flag {
                return Err(CommandError::syntax_error());
            }
            *flag = true;
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
            if parsed.expiry.is_some() || parsed.should_persist {
                return Err(CommandError::syntax_error());
            }
            parsed.expiry = Some((expiry, value));
        } else if option.eq_ignore_ascii_case(b"BYINT") || option.eq_ignore_ascii_case(b"BYFLOAT") {
            if parsed.increment.is_some() {
                return Err(CommandError::syntax_error());
            }
            parsed.increment = Some((option.eq_ignore_ascii_case(b"BYFLOAT"), value));
        } else if option.eq_ignore_ascii_case(b"LBOUND") {
            if parsed.lower.replace(value).is_some() {
                return Err(CommandError::syntax_error());
            }
        } else if option.eq_ignore_ascii_case(b"UBOUND") {
            if parsed.upper.replace(value).is_some() {
                return Err(CommandError::syntax_error());
            }
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
            Some((true, text)) => {
                let value = parse_long_double(text)
                    .map_err(|_| Value::error("ERR Increment is not a valid float"))?;
                if value.is_infinite() {
                    return Err(Value::error("ERR BYFLOAT increment cannot be Infinity"));
                }
                Ok(Increment::Float(value))
            }
        }
    }

    fn operation(&self, increment: Increment) -> Result<Operation, Value> {
        match increment {
            Increment::Int(increment) => {
                let parse = |text, name| {
                    int_encodable(text).ok_or_else(|| {
                        Value::error(format!("ERR {name} is not an integer or out of range"))
                    })
                };
                let lower = self
                    .lower
                    .map(|text| parse(text, "LBOUND"))
                    .transpose()?
                    .unwrap_or(i64::MIN);
                let upper = self
                    .upper
                    .map(|text| parse(text, "UBOUND"))
                    .transpose()?
                    .unwrap_or(i64::MAX);
                validate_bounds(lower, upper)?;
                Ok(Operation::Int {
                    increment,
                    lower,
                    upper,
                })
            }
            Increment::Float(increment) => {
                if increment.is_infinite() {
                    return Err(Value::error("ERR BYFLOAT increment cannot be Infinity"));
                }
                let parse = |text, name| {
                    parse_long_double(text)
                        .map_err(|_| Value::error(format!("ERR {name} is not a valid float")))
                };
                let lower = self
                    .lower
                    .map(|text| parse(text, "LBOUND"))
                    .transpose()?
                    .unwrap_or(-LongDouble::largest());
                let upper = self
                    .upper
                    .map(|text| parse(text, "UBOUND"))
                    .transpose()?
                    .unwrap_or(LongDouble::largest());
                validate_bounds(lower, upper)?;
                Ok(Operation::Float {
                    increment,
                    lower,
                    upper,
                })
            }
        }
    }
}

fn validate_bounds<T: PartialOrd>(lower: T, upper: T) -> Result<(), Value> {
    if lower > upper {
        Err(Value::error("ERR LBOUND can't be greater than UBOUND"))
    } else {
        Ok(())
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

enum Operation {
    Int {
        increment: i64,
        lower: i64,
        upper: i64,
    },
    Float {
        increment: LongDouble,
        lower: LongDouble,
        upper: LongDouble,
    },
}

impl Operation {
    fn calculate(
        self,
        current: Option<&[u8]>,
        is_declined: bool,
        should_saturate: bool,
    ) -> Result<Calculated, Value> {
        match self {
            Self::Int {
                increment,
                lower,
                upper,
            } => {
                let old = match current {
                    Some(text) => int_encodable(text).ok_or_else(|| {
                        Value::error("ERR value is not an integer or out of range")
                    })?,
                    None => 0,
                };
                let computed = i128::from(old) + i128::from(increment);
                let is_outside = computed < i128::from(lower) || computed > i128::from(upper);
                let next = if is_declined || (is_outside && !should_saturate) {
                    None
                } else {
                    Some(computed.clamp(i128::from(lower), i128::from(upper)) as i64)
                };
                let value = next.unwrap_or(old);
                let delta = value
                    .checked_sub(old)
                    .ok_or_else(|| Value::error("ERR applied increment would overflow"))?;
                Ok(Calculated {
                    reply: Value::Array(vec![Value::Integer(value), Value::Integer(delta)]),
                    stored: Bytes::from(value.to_string()),
                    should_write: next.is_some(),
                })
            }
            Self::Float {
                increment,
                lower,
                upper,
            } => {
                let old = match current {
                    Some(text) => parse_long_double(text)
                        .map_err(|_| Value::error("ERR value is not a valid float"))?,
                    None => LongDouble::ZERO,
                };
                if old.is_infinite() {
                    return Err(Value::error("ERR value cannot be Infinity"));
                }
                if is_declined {
                    return float_result(old, LongDouble::ZERO, false);
                }
                let mut next = add_long_double(old, increment).value;
                if next.is_nan() {
                    return Err(Value::error("ERR Increment is not a valid float"));
                }
                let is_upper = if next.is_infinite() {
                    Some(!next.is_negative())
                } else if next > upper {
                    Some(true)
                } else if next < lower {
                    Some(false)
                } else {
                    None
                };
                if let Some(is_upper) = is_upper {
                    if !should_saturate {
                        return float_result(old, LongDouble::ZERO, false);
                    }
                    next = if is_upper { upper } else { lower };
                    if next.is_infinite() || sub_long_double(next, old).value.is_infinite() {
                        return Err(Value::error("ERR applied increment would be Infinity"));
                    }
                }
                float_result(next, sub_long_double(next, old).value, true)
            }
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

    fn calculate(
        increment: Increment,
        current: Option<&[u8]>,
        is_declined: bool,
    ) -> Result<Calculated, Value> {
        Options::default()
            .operation(increment)?
            .calculate(current, is_declined, false)
    }

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
            vec!["NX", "EX", "2", "BYINT", "0"],
            vec!["BYFLOAT", ".5", "PXAT", "9223372036854775807", "XX"],
            vec![],
        ] {
            let args = command(&options);
            let parsed = parse_options(&args.args[1..]).unwrap();
            assert!(parsed.deadline_ms(1_000).is_ok());
            assert!(parsed.increment().is_ok());
        }
        for options in [
            vec!["NX", "NX"],
            vec!["XX", "XX"],
            vec!["EX", "1", "EX", "2"],
            vec!["ENX", "PERSIST"],
            vec!["PERSIST", "ENX"],
            vec!["ENX", "ENX", "EX", "1"],
            vec!["SATURATE", "SATURATE"],
            vec!["LBOUND", "0", "LBOUND", "1"],
            vec!["UBOUND", "0", "UBOUND", "1"],
            vec!["PERSIST", "EX", "1"],
            vec!["EX", "1", "PERSIST"],
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
        assert_eq!(
            calculate(infinite, Some(b"-infinity"), true).err(),
            Some(Value::error("ERR BYFLOAT increment cannot be Infinity"))
        );
        assert_eq!(
            calculate(half, Some(b"-infinity"), true).err(),
            Some(Value::error("ERR value cannot be Infinity"))
        );
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
            (
                vec![],
                "3",
                "incrby",
                now_ms + 20_000,
                vec![
                    "SET".to_owned(),
                    "key".to_owned(),
                    "3".to_owned(),
                    "KEEPTTL".to_owned(),
                ],
            ),
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

    #[test]
    fn increx_bounds_control_writes_and_applied_delta() {
        let now_ms = crate::foundation::io::store::kv_engine::now_ms();
        for (old, options, value, delta, should_write) in [
            (Some("5"), vec!["BYINT", "8", "UBOUND", "10"], 5, 0, false),
            (
                Some("5"),
                vec!["BYINT", "8", "UBOUND", "10", "SATURATE"],
                10,
                5,
                true,
            ),
            (
                Some("5"),
                vec!["BYINT", "-8", "LBOUND", "0", "SATURATE"],
                0,
                -5,
                true,
            ),
            (None, vec!["LBOUND", "10"], 0, 0, false),
            (None, vec!["LBOUND", "10", "SATURATE"], 10, 10, true),
            (
                Some("9223372036854775807"),
                vec!["SATURATE"],
                i64::MAX,
                0,
                true,
            ),
            (
                Some("-9223372036854775808"),
                vec!["BYINT", "-1", "SATURATE"],
                i64::MIN,
                0,
                true,
            ),
        ] {
            let mut runtime = TestCommandRuntime::new(1, now_ms);
            if let Some(old) = old {
                let db = runtime.db_mut().unwrap();
                db.set(b"key".to_vec(), Robj::string(old));
                db.set_expire(b"key", now_ms + 20_000);
            }
            let outcome = increx(&mut runtime, &command(&options));
            assert_eq!(
                outcome.reply,
                Value::Array(vec![Value::Integer(value), Value::Integer(delta)]),
                "{options:?}"
            );
            assert_eq!(!outcome.effects.changed_keys.is_empty(), should_write);
            assert_eq!(outcome.effects.skip_propagate, !should_write);
            let db = runtime.db_mut().unwrap();
            assert_eq!(db.get_expire(b"key"), old.map(|_| now_ms + 20_000));
            assert_eq!(
                db.get(b"key").map(|o| o.as_bytes().to_vec()),
                if should_write {
                    Some(value.to_string().into_bytes())
                } else {
                    old.map(|s| s.as_bytes().to_vec())
                }
            );
        }
    }

    #[test]
    fn increx_float_bounds_and_errors_preserve_state() {
        let now_ms = crate::foundation::io::store::kv_engine::now_ms();
        let largest = format_long_double_human(LongDouble::largest()).unwrap();
        let negative_largest = format!("-{largest}");
        for (old, options, expected) in [
            (
                "1.25",
                vec!["BYFLOAT", "1", "UBOUND", "2", "SATURATE"],
                Value::Array(vec![
                    Value::DecimalDouble(DecimalDoubleText::new("2").unwrap()),
                    Value::DecimalDouble(DecimalDoubleText::new("0.75").unwrap()),
                ]),
            ),
            (
                "1",
                vec!["BYFLOAT", "inf", "NX"],
                Value::error("ERR BYFLOAT increment cannot be Infinity"),
            ),
            (
                "inf",
                vec!["BYFLOAT", "1", "NX"],
                Value::error("ERR value cannot be Infinity"),
            ),
            (
                "-9223372036854775808",
                vec!["LBOUND", "9223372036854775807", "SATURATE", "PERSIST"],
                Value::error("ERR applied increment would overflow"),
            ),
            (
                negative_largest.as_str(),
                vec![
                    "BYFLOAT",
                    "0",
                    "LBOUND",
                    largest.as_str(),
                    "SATURATE",
                    "PERSIST",
                ],
                Value::error("ERR applied increment would be Infinity"),
            ),
            (
                "1",
                vec!["LBOUND", "2", "UBOUND", "1"],
                Value::error("ERR LBOUND can't be greater than UBOUND"),
            ),
            (
                "1",
                vec!["LBOUND", "1.5"],
                Value::error("ERR LBOUND is not an integer or out of range"),
            ),
            (
                "1",
                vec!["BYFLOAT", "1", "UBOUND", "nan"],
                Value::error("ERR UBOUND is not a valid float"),
            ),
            (
                "1",
                vec!["ENX"],
                Value::error("ERR ENX flag requires an expiration"),
            ),
        ] {
            let mut runtime = TestCommandRuntime::new(1, now_ms);
            let db = runtime.db_mut().unwrap();
            db.set(b"key".to_vec(), Robj::string(old.to_owned()));
            db.set_expire(b"key", now_ms + 20_000);
            let outcome = increx(&mut runtime, &command(&options));
            assert_eq!(outcome.reply, expected, "{options:?}");
            if matches!(expected, Value::Error(_)) {
                assert!(outcome.effects.changed_keys.is_empty());
                assert!(outcome.effects.custom_propagations.is_empty());
                assert_eq!(
                    runtime.db_mut().unwrap().get(b"key").unwrap().as_bytes(),
                    old.as_bytes()
                );
            }
            assert_eq!(
                runtime.db_mut().unwrap().get_expire(b"key"),
                Some(now_ms + 20_000)
            );
        }
    }

    #[test]
    fn increx_expiry_options_control_ttl_notifications_and_propagation() {
        let now_ms = crate::foundation::io::store::kv_engine::now_ms();
        for (has_ttl, options, expected_ttl, suffix, event) in [
            (
                true,
                vec!["ENX", "PXAT", "1"],
                Some(now_ms + 20_000),
                vec!["KEEPTTL".to_owned()],
                None,
            ),
            (
                false,
                vec!["ENX", "PX", "50000"],
                Some(now_ms + 50_000),
                vec!["PXAT".to_owned(), (now_ms + 50_000).to_string()],
                Some("expire"),
            ),
            (true, vec!["PERSIST"], None, vec![], Some("persist")),
            (false, vec!["PERSIST"], None, vec![], None),
        ] {
            let mut runtime = TestCommandRuntime::new(1, now_ms);
            let db = runtime.db_mut().unwrap();
            db.set(b"key".to_vec(), Robj::string("2"));
            if has_ttl {
                db.set_expire(b"key", now_ms + 20_000);
            }
            let outcome = increx(&mut runtime, &command(&options));
            assert_eq!(
                outcome.reply,
                Value::Array(vec![Value::Integer(3), Value::Integer(1)]),
                "{options:?}"
            );
            assert_eq!(runtime.db_mut().unwrap().get_expire(b"key"), expected_ttl);
            assert_eq!(
                outcome
                    .effects
                    .events
                    .iter()
                    .map(|e| e.kind)
                    .collect::<Vec<_>>(),
                std::iter::once("incrby").chain(event).collect::<Vec<_>>()
            );
            let expected: Vec<Bytes> = ["SET".to_owned(), "key".to_owned(), "3".to_owned()]
                .into_iter()
                .chain(suffix)
                .map(Bytes::from)
                .collect();
            assert_eq!(outcome.effects.custom_propagations, vec![expected]);
        }
    }
}
