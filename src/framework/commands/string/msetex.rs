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
    let Some(db) = runtime.db_mut() else {
        return CommandOutcome::reply(CommandError::db_index_out_of_range());
    };
    if !condition_allows(db, &request, now_ms) {
        return CommandOutcome {
            reply: Value::integer(0),
            effects: CommandEffects {
                skip_propagate: true,
                ..CommandEffects::default()
            },
        };
    }

    let past_time =
        matches!(request.expiry, MSetExExpiry::At { at_ms, .. } if at_ms <= now_ms as i64);
    let mut effects = CommandEffects {
        skip_propagate: past_time,
        ..CommandEffects::default()
    };
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
    if !past_time {
        effects.custom_propagations.push(encode_msetex(&request));
    }
    CommandOutcome {
        reply: Value::integer(1),
        effects,
    }
}

fn condition_allows(
    db: &mut crate::foundation::io::store::kv_engine::Db,
    request: &MSetExRequest,
    now_ms: u64,
) -> bool {
    match request.condition {
        MSetExCondition::Any => true,
        MSetExCondition::Missing => request
            .pairs
            .iter()
            .all(|(key, _)| !db.exists_at(key, now_ms)),
        MSetExCondition::Existing => request
            .pairs
            .iter()
            .all(|(key, _)| db.exists_at(key, now_ms)),
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
            set_expiry(&mut expiry, MSetExExpiry::KeepTtl)?;
            index += 1;
        } else {
            let parsed = parse_msetex_expiry(options, index, now_ms)?;
            set_expiry(&mut expiry, parsed)?;
            index += 2;
        }
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

fn set_expiry(target: &mut MSetExExpiry, value: MSetExExpiry) -> Result<(), Value> {
    if *target != MSetExExpiry::None {
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
    let value = std::str::from_utf8(&options[index + 1])
        .ok()
        .and_then(|value| value.trim().parse::<i64>().ok())
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
}
