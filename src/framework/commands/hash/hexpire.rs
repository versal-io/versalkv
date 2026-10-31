//! Shared typed pipeline for HEXPIRE/HPEXPIRE/HEXPIREAT/HPEXPIREAT.

use super::runtime_support::{
    hash_field_deletion_propagation, parse_fields,
};
use crate::engine::core::{CommandEffects, CommandOutcome, CoreEvent, Value};
use crate::foundation::io::store::kv_engine::Db;
use crate::framework::commands::command_runtime::CommandRuntime;
use crate::framework::{CommandArgs, CommandError};
use bytes::Bytes;
use command_macros::command;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum HashExpireMode {
    Seconds,
    Milliseconds,
    UnixSeconds,
    UnixMilliseconds,
}

impl HashExpireMode {
    fn command_name(self) -> &'static str {
        match self {
            Self::Seconds => "hexpire",
            Self::Milliseconds => "hpexpire",
            Self::UnixSeconds => "hexpireat",
            Self::UnixMilliseconds => "hpexpireat",
        }
    }

    fn absolute_ms(self, value: i64, now: i64) -> Result<i64, Value> {
        let result = match self {
            Self::Seconds => value.checked_mul(1_000).and_then(|delta| delta.checked_add(now)),
            Self::Milliseconds => value.checked_add(now),
            Self::UnixSeconds => value.checked_mul(1_000),
            Self::UnixMilliseconds => Some(value),
        };
        result.ok_or_else(|| {
            Value::error(format!(
                "ERR invalid expire time in '{}' command",
                self.command_name()
            ))
        })
    }

    fn is_relative(self) -> bool {
        matches!(self, Self::Seconds | Self::Milliseconds)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
enum ExpiryExistence {
    #[default]
    Any,
    Missing,
    Existing,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
enum ExpiryComparison {
    #[default]
    Any,
    Greater,
    Less,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
struct HashExpireCondition {
    existence: ExpiryExistence,
    comparison: ExpiryComparison,
}

impl HashExpireCondition {
    fn allows(self, current: Option<u64>, proposed: i64) -> bool {
        let existence = match self.existence {
            ExpiryExistence::Any => true,
            ExpiryExistence::Missing => current.is_none(),
            ExpiryExistence::Existing => current.is_some(),
        };
        let comparison = match self.comparison {
            ExpiryComparison::Any => true,
            ExpiryComparison::Greater => current.is_some_and(|value| proposed > value as i64),
            ExpiryComparison::Less => current.is_none_or(|value| proposed < value as i64),
        };
        existence && comparison
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct HashExpireRequest {
    key: Bytes,
    value: i64,
    mode: HashExpireMode,
    condition: HashExpireCondition,
    flags: Vec<Bytes>,
    fields: Vec<Bytes>,
}

impl HashExpireRequest {
    pub(super) fn parse(args: &CommandArgs, mode: HashExpireMode) -> Result<Self, Value> {
        if args.args.len() < 4 {
            return Err(CommandError::wrong_arity(mode.command_name()));
        }
        let value = parse_integer(&args.args[1])?;
        if mode.is_relative() && value < 0 {
            return Err(Value::error(format!(
                "ERR invalid expire time in '{}' command",
                mode.command_name()
            )));
        }
        let fields_index = args.args[2..]
            .iter()
            .position(|arg| arg.eq_ignore_ascii_case(b"fields"))
            .map(|index| index + 2)
            .unwrap_or(args.args.len());
        let condition = parse_condition(&args.args[2..fields_index])?;
        let (_, fields) = parse_fields(&args.args, fields_index)?;
        Ok(Self {
            key: args.args[0].clone(),
            value,
            mode,
            condition,
            flags: args.args[2..fields_index].to_vec(),
            fields,
        })
    }
}

fn parse_integer(value: &[u8]) -> Result<i64, Value> {
    std::str::from_utf8(value)
        .ok()
        .and_then(|value| value.trim().parse().ok())
        .ok_or_else(|| Value::error("ERR value is not an integer or out of range"))
}

fn parse_condition(flags: &[Bytes]) -> Result<HashExpireCondition, Value> {
    let mut condition = HashExpireCondition::default();
    for flag in flags {
        apply_condition_flag(&mut condition, flag)?;
    }
    validate_condition(condition)?;
    Ok(condition)
}

fn apply_condition_flag(condition: &mut HashExpireCondition, flag: &[u8]) -> Result<(), Value> {
    if flag.eq_ignore_ascii_case(b"nx") {
        if condition.existence == ExpiryExistence::Existing
            || condition.comparison != ExpiryComparison::Any
        {
            return Err(incompatible_nx());
        }
        condition.existence = ExpiryExistence::Missing;
    } else if flag.eq_ignore_ascii_case(b"xx") {
        if condition.existence == ExpiryExistence::Missing {
            return Err(incompatible_nx());
        }
        condition.existence = ExpiryExistence::Existing;
    } else if flag.eq_ignore_ascii_case(b"gt") || flag.eq_ignore_ascii_case(b"lt") {
        apply_comparison_flag(condition, flag)?;
    } else {
        return Err(Value::error(format!(
            "ERR Unsupported option {}",
            String::from_utf8_lossy(flag).to_ascii_uppercase()
        )));
    }
    Ok(())
}

fn apply_comparison_flag(condition: &mut HashExpireCondition, flag: &[u8]) -> Result<(), Value> {
    if condition.existence == ExpiryExistence::Missing {
        return Err(incompatible_nx());
    }
    let comparison = if flag.eq_ignore_ascii_case(b"gt") {
        ExpiryComparison::Greater
    } else {
        ExpiryComparison::Less
    };
    if condition.comparison != ExpiryComparison::Any && condition.comparison != comparison {
        return Err(incompatible_comparison());
    }
    condition.comparison = comparison;
    Ok(())
}

fn validate_condition(condition: HashExpireCondition) -> Result<(), Value> {
    if condition.existence == ExpiryExistence::Missing
        && condition.comparison != ExpiryComparison::Any
    {
        return Err(incompatible_nx());
    }
    Ok(())
}

fn incompatible_nx() -> Value {
    Value::error("ERR NX and XX, GT or LT options at the same time are not compatible")
}

fn incompatible_comparison() -> Value {
    Value::error("ERR GT and LT options at the same time are not compatible")
}

struct HashExpirePlan<'a> {
    request: &'a HashExpireRequest,
    when_ms: i64,
    now_ms: i64,
    ignore_expiry: bool,
    read_expired_values: bool,
    has_volatile_fields: bool,
}

#[derive(Default)]
struct HashExpireOutcome {
    results: Vec<Value>,
    updated_fields: Vec<Bytes>,
    expired_fields: Vec<Bytes>,
    propagated_fields: Vec<Bytes>,
    has_skipped: bool,
}

impl HashExpireOutcome {
    fn apply_field(&mut self, db: &mut Db, field: &Bytes, plan: &HashExpirePlan<'_>) -> Result<(), Value> {
        let hash = match db.get_hash_object(&plan.request.key) {
            Ok(Some(hash)) => hash,
            Ok(None) => {
                self.results.push(Value::integer(-2));
                self.has_skipped |= plan.has_volatile_fields;
                return Ok(());
            }
            Err(_) => return Err(CommandError::wrong_type()),
        };
        if !hash.hash_contains(field)
            || (!plan.read_expired_values
                && db.hash_field_expired_at(&plan.request.key, field, plan.now_ms as u64))
        {
            self.results.push(Value::integer(-2));
            self.has_skipped |= plan.has_volatile_fields;
            return Ok(());
        }
        let current = db.get_hash_field_expire(&plan.request.key, field);
        if !plan.request.condition.allows(current, plan.when_ms) {
            self.results.push(Value::integer(0));
            if !self.has_skipped {
                self.propagated_fields.push(field.clone());
            }
        } else if plan.when_ms <= plan.now_ms && !plan.ignore_expiry {
            self.expire_field(db, field, plan);
        } else {
            db.set_hash_field_expire(&plan.request.key, field, plan.when_ms as u64);
            self.updated_fields.push(field.clone());
            self.propagated_fields.push(field.clone());
            self.results.push(Value::integer(1));
        }
        Ok(())
    }

    fn expire_field(&mut self, db: &mut Db, field: &Bytes, plan: &HashExpirePlan<'_>) {
        if let Ok(Some(hash)) = db.get_hash_object_mut(&plan.request.key) {
            hash.hash_del(field);
        }
        db.remove_hash_field_expire(&plan.request.key, field);
        self.expired_fields.push(field.clone());
        self.results.push(Value::integer(2));
    }
}

struct HashExpireCommit {
    when_ms: i64,
    hash_empty: bool,
    outcome: HashExpireOutcome,
}

impl HashExpireCommit {
    fn into_outcome(
        self,
        db: &mut Db,
        db_index: u32,
        request: &HashExpireRequest,
    ) -> CommandOutcome {
        let changed = !self.outcome.expired_fields.is_empty()
            || !self.outcome.updated_fields.is_empty();
        let mut events = Vec::new();
        if !self.outcome.updated_fields.is_empty() {
            events.push(CoreEvent {
                kind: "hexpire",
                key: request.key.clone(),
            });
        }
        if !self.outcome.expired_fields.is_empty() {
            events.push(CoreEvent {
                kind: "hexpired",
                key: request.key.clone(),
            });
            if self.hash_empty {
                db.del(&request.key);
                events.push(CoreEvent {
                    kind: "del",
                    key: request.key.clone(),
                });
            }
        }
        let propagation = if !self.outcome.expired_fields.is_empty() {
            Some(hash_field_deletion_propagation(
                &request.key,
                &self.outcome.expired_fields,
            ))
        } else if !self.outcome.updated_fields.is_empty() {
            let mut parts = vec![
                Bytes::from_static(b"HPEXPIREAT"),
                request.key.clone(),
                self.when_ms.to_string().into(),
            ];
            parts.extend(request.flags.iter().cloned());
            parts.push(Bytes::from_static(b"FIELDS"));
            parts.push(self.outcome.propagated_fields.len().to_string().into());
            parts.extend(self.outcome.propagated_fields.iter().cloned());
            Some(parts)
        } else {
            None
        };
        let mut custom_propagations = Vec::new();
        if let Some(propagation) = propagation {
            custom_propagations.push(propagation);
        }
        CommandOutcome {
            reply: Value::Array(self.outcome.results),
            effects: CommandEffects {
                changed_keys: changed.then(|| (db_index, request.key.clone())).into_iter().collect(),
                events,
                skip_propagate: true,
                custom_propagations,
                expired_hash_fields: self.outcome.expired_fields.len() as u64,
                ..CommandEffects::default()
            },
        }
    }
}

pub(super) fn hash_expire(
    runtime: &mut dyn CommandRuntime,
    args: &CommandArgs,
    mode: HashExpireMode,
) -> CommandOutcome {
    let request = match HashExpireRequest::parse(args, mode) {
        Ok(request) => request,
        Err(error) => return CommandOutcome::reply(error),
    };
    let db_index = runtime.selected_db();
    let now_ms = runtime.now_ms();
    let when_ms = match request.mode.absolute_ms(request.value, now_ms as i64) {
        Ok(value) => value,
        Err(error) => return CommandOutcome::reply(error),
    };
    let ignore_expiry = runtime.suppress_lazy_expire();
    let read_expired_values = runtime.read_expired_values();
    let Some(db) = runtime.db_mut() else {
        return CommandOutcome::reply(CommandError::db_index_out_of_range());
    };
    let commit = match execute_hash_expire(db, &request, when_ms, now_ms as i64, ignore_expiry, read_expired_values) {
        Ok(commit) => commit,
        Err(error) => return CommandOutcome::reply(error),
    };
    commit.into_outcome(db, db_index, &request)
}

fn execute_hash_expire(
    db: &mut Db,
    request: &HashExpireRequest,
    when_ms: i64,
    now_ms: i64,
    ignore_expiry: bool,
    read_expired_values: bool,
) -> Result<HashExpireCommit, Value> {
    let plan = HashExpirePlan {
        request,
        when_ms,
        now_ms,
        ignore_expiry,
        read_expired_values,
        has_volatile_fields: db.hash_key_has_volatile_fields(&request.key),
    };
    let mut outcome = HashExpireOutcome::default();
    for field in &request.fields {
        outcome.apply_field(db, field, &plan)?;
    }
    let hash_empty = !outcome.expired_fields.is_empty()
        && matches!(db.get_hash_object(&request.key), Ok(Some(hash)) if hash.hash_is_empty());
    Ok(HashExpireCommit {
        when_ms,
        hash_empty,
        outcome,
    })
}

#[command("HEXPIRE", runtime)]
pub fn hexpire(runtime: &mut dyn CommandRuntime, args: &CommandArgs) -> CommandOutcome {
    hash_expire(runtime, args, HashExpireMode::Seconds)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn request_parses_typed_existing_and_greater_condition() {
        let args = CommandArgs::from_parts(vec![
            Bytes::from_static(b"HEXPIRE"),
            Bytes::from_static(b"hash"),
            Bytes::from_static(b"10"),
            Bytes::from_static(b"XX"),
            Bytes::from_static(b"GT"),
            Bytes::from_static(b"FIELDS"),
            Bytes::from_static(b"1"),
            Bytes::from_static(b"field"),
        ]);

        let request = HashExpireRequest::parse(&args, HashExpireMode::Seconds).unwrap();
        assert_eq!(request.condition.existence, ExpiryExistence::Existing);
        assert_eq!(request.condition.comparison, ExpiryComparison::Greater);
    }
}
