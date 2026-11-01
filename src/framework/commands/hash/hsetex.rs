//! HSETEX typed parse, execute, and side-effect commit pipeline.

use super::runtime_support::{
    hash_field_deletion_propagation, hash_field_value_propagation,
};
use crate::engine::core::{CommandEffects, CommandOutcome, CoreEvent, Value};
use crate::foundation::io::store::kv_engine::Db;
use crate::framework::commands::command_runtime::CommandRuntime;
use crate::framework::{CommandArgs, CommandError};
use bytes::Bytes;
use command_macros::command;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
enum HashExistence {
    #[default]
    Any,
    Missing,
    Existing,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
enum FieldExistence {
    #[default]
    Any,
    AllMissing,
    AllExisting,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
enum HSetExExpiry {
    #[default]
    None,
    KeepTtl,
    Seconds(i64),
    Milliseconds(i64),
    UnixSeconds(i64),
    UnixMilliseconds(i64),
}

impl HSetExExpiry {
    fn absolute_ms(self, now: u64) -> Result<Option<u64>, Value> {
        let raw = match self {
            Self::Seconds(value)
            | Self::Milliseconds(value)
            | Self::UnixSeconds(value)
            | Self::UnixMilliseconds(value) => Some(value),
            Self::None | Self::KeepTtl => None,
        };
        if raw.is_some_and(|value| value < 0) {
            return Err(Value::error("ERR invalid expire time in 'hsetex' command"));
        }
        let value = match self {
            Self::None | Self::KeepTtl => return Ok(None),
            Self::Seconds(value) => value.checked_mul(1_000).and_then(|v| v.checked_add(now as i64)),
            Self::Milliseconds(value) => value.checked_add(now as i64),
            Self::UnixSeconds(value) => value.checked_mul(1_000),
            Self::UnixMilliseconds(value) => Some(value),
        };
        value
            .filter(|value| *value >= 0)
            .map(|value| Some(value as u64))
            .ok_or_else(|| Value::error("ERR invalid expire time in 'hsetex' command"))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct HSetExRequest {
    key: Bytes,
    hash_condition: HashExistence,
    field_condition: FieldExistence,
    expiry: HSetExExpiry,
    pairs: Vec<(Bytes, Bytes)>,
}

impl HSetExRequest {
    fn parse(args: &CommandArgs) -> Result<Self, Value> {
        if args.args.len() < 5 {
            return Err(CommandError::wrong_arity("hsetex"));
        }
        let fields_index = args.args[1..]
            .iter()
            .position(|arg| arg.eq_ignore_ascii_case(b"fields"))
            .map(|index| index + 1)
            .ok_or_else(CommandError::syntax_error)?;
        let (hash_condition, field_condition, expiry) =
            parse_options(&args.args[1..fields_index])?;
        let pairs = parse_pairs(&args.args, fields_index)?;
        Ok(Self {
            key: args.args[0].clone(),
            hash_condition,
            field_condition,
            expiry,
            pairs,
        })
    }
}

fn parse_options(
    options: &[Bytes],
) -> Result<(HashExistence, FieldExistence, HSetExExpiry), Value> {
    let mut hash = HashExistence::Any;
    let mut field = FieldExistence::Any;
    let mut expiry = HSetExExpiry::None;
    let mut index = 0;
    while index < options.len() {
        let option = &options[index];
        if option.eq_ignore_ascii_case(b"nx") {
            set_once(&mut hash, HashExistence::Missing, "ERR NX and XX are incompatible")?;
        } else if option.eq_ignore_ascii_case(b"xx") {
            set_once(&mut hash, HashExistence::Existing, "ERR NX and XX are incompatible")?;
        } else if option.eq_ignore_ascii_case(b"fnx") {
            set_once(&mut field, FieldExistence::AllMissing, "ERR FNX and FXX are incompatible")?;
        } else if option.eq_ignore_ascii_case(b"fxx") {
            set_once(&mut field, FieldExistence::AllExisting, "ERR FNX and FXX are incompatible")?;
        } else {
            let (parsed, consumed) = parse_expiry(options, index)?;
            if expiry != HSetExExpiry::None
                && !(expiry == HSetExExpiry::KeepTtl && parsed == HSetExExpiry::KeepTtl)
            {
                return Err(expiry_conflict());
            }
            expiry = parsed;
            index += consumed;
            continue;
        }
        index += 1;
    }
    Ok((hash, field, expiry))
}

fn set_once<T: Copy + Default + PartialEq>(
    target: &mut T,
    value: T,
    error: &'static str,
) -> Result<(), Value> {
    if *target != T::default() && *target != value {
        return Err(Value::error(error));
    }
    *target = value;
    Ok(())
}

fn parse_expiry(options: &[Bytes], index: usize) -> Result<(HSetExExpiry, usize), Value> {
    let option = &options[index];
    if option.eq_ignore_ascii_case(b"keepttl") {
        return Ok((HSetExExpiry::KeepTtl, 1));
    }
    if index + 1 >= options.len() {
        return Err(Value::error("ERR value is not an integer or out of range"));
    }
    let value = parse_integer(&options[index + 1])?;
    let expiry = if option.eq_ignore_ascii_case(b"ex") {
        HSetExExpiry::Seconds(value)
    } else if option.eq_ignore_ascii_case(b"px") {
        HSetExExpiry::Milliseconds(value)
    } else if option.eq_ignore_ascii_case(b"exat") {
        HSetExExpiry::UnixSeconds(value)
    } else if option.eq_ignore_ascii_case(b"pxat") {
        HSetExExpiry::UnixMilliseconds(value)
    } else {
        return Err(CommandError::syntax_error());
    };
    Ok((expiry, 2))
}

fn parse_integer(value: &[u8]) -> Result<i64, Value> {
    std::str::from_utf8(value)
        .ok()
        .and_then(|value| value.trim().parse().ok())
        .ok_or_else(|| Value::error("ERR value is not an integer or out of range"))
}

fn expiry_conflict() -> Value {
    Value::error("ERR only one expire option (EX, PX, EXAT, PXAT, or KEEPTTL) is allowed")
}

fn parse_pairs(args: &[Bytes], fields_index: usize) -> Result<Vec<(Bytes, Bytes)>, Value> {
    if fields_index + 1 >= args.len() {
        return Err(fields_count_error());
    }
    let count = std::str::from_utf8(&args[fields_index + 1])
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|count| *count > 0)
        .ok_or_else(|| Value::error("ERR numfields should be greater than 0"))?;
    if fields_index + 2 + count * 2 != args.len() {
        return Err(fields_count_error());
    }
    Ok((0..count)
        .map(|index| {
            (
                args[fields_index + 2 + index * 2].clone(),
                args[fields_index + 3 + index * 2].clone(),
            )
        })
        .collect())
}

fn fields_count_error() -> Value {
    Value::error("ERR numfields should be greater than 0 and match the provided number of fields")
}

struct HSetExSettings {
    ignore_expiry: bool,
    should_preserve_deadline: bool,
    max_entries: usize,
    max_value_size: usize,
    now_ms: u64,
    expire_at_ms: Option<u64>,
}

#[derive(Default)]
struct HSetExOutcome {
    expired_fields: Vec<Bytes>,
    overwritten_expired: u64,
    preserved_expiries: Vec<Option<u64>>,
}

struct HSetExCommit {
    request: HSetExRequest,
    settings: HSetExSettings,
    outcome: HSetExOutcome,
}

impl HSetExOutcome {
    fn expired_count(&self) -> u64 {
        self.expired_fields.len() as u64 + self.overwritten_expired
    }
}

fn conditions_allow(db: &mut Db, request: &HSetExRequest, settings: &HSetExSettings) -> bool {
    let key_exists = db.get_hash_object(&request.key).ok().flatten().is_some();
    if matches!(request.hash_condition, HashExistence::Missing) && key_exists
        || matches!(request.hash_condition, HashExistence::Existing) && !key_exists
    {
        return false;
    }
    request.pairs.iter().all(|(field, _)| {
        let exists = db
            .get_hash_object(&request.key)
            .ok()
            .flatten()
            .is_some_and(|hash| hash.hash_contains(field))
            && (settings.ignore_expiry || !db.hash_field_expired_at(&request.key, field, settings.now_ms));
        match request.field_condition {
            FieldExistence::Any => true,
            FieldExistence::AllMissing => !exists,
            FieldExistence::AllExisting => exists,
        }
    })
}

fn execute_hsetex(
    db: &mut Db,
    request: &HSetExRequest,
    settings: &HSetExSettings,
) -> Result<HSetExOutcome, Value> {
    db.hash_prepare_pairs_with_limits(
        &request.key,
        &request.pairs,
        settings.max_entries,
        settings.max_value_size,
    )
    .map_err(|_| CommandError::wrong_type())?;
    let mut outcome = HSetExOutcome::default();
    for (field, value) in &request.pairs {
        let expired = !settings.ignore_expiry && db.hash_field_expired_at(&request.key, field, settings.now_ms);
        let preserved = if matches!(request.expiry, HSetExExpiry::KeepTtl) && !expired {
            db.get_hash_field_expire(&request.key, field)
        } else {
            None
        };
        if expired {
            let _ = db.clear_expired_hash_field_ttl(&request.key, field);
            outcome.overwritten_expired += 1;
        }
        outcome.preserved_expiries.push(preserved);
        if preserved.is_none() {
            db.remove_hash_field_expire(&request.key, field);
        }
        db.hash_set_with_limits(
            &request.key,
            field,
            value,
            settings.max_entries,
            settings.max_value_size,
        )
        .map_err(|_| CommandError::wrong_type())?;
        apply_new_expiry(db, request, field, settings, &mut outcome);
    }
    Ok(outcome)
}

fn apply_new_expiry(
    db: &mut Db,
    request: &HSetExRequest,
    field: &Bytes,
    settings: &HSetExSettings,
    outcome: &mut HSetExOutcome,
) {
    let Some(expire_at) = settings.expire_at_ms else { return };
    db.set_hash_field_expire(&request.key, field, expire_at);
    if expire_at <= settings.now_ms && !settings.should_preserve_deadline {
        if let Ok(Some(hash)) = db.get_hash_object_mut(&request.key) {
            hash.hash_del(field);
        }
        db.remove_hash_field_expire(&request.key, field);
        outcome.expired_fields.push(field.clone());
    }
}

#[command("HSETEX", runtime)]
pub fn hsetex(runtime: &mut dyn CommandRuntime, args: &CommandArgs) -> CommandOutcome {
    let request = match HSetExRequest::parse(args) {
        Ok(request) => request,
        Err(error) => return CommandOutcome::reply(error),
    };
    let settings = match build_settings(runtime, request.expiry) {
        Ok(settings) => settings,
        Err(error) => return CommandOutcome::reply(error),
    };
    let db_index = runtime.selected_db();
    let db = match runtime.db_mut() {
        Some(db) => db,
        None => return CommandOutcome::reply(CommandError::db_index_out_of_range()),
    };
    if !conditions_allow(db, &request, &settings) {
        return CommandOutcome {
            reply: Value::integer(0),
            effects: CommandEffects {
                skip_propagate: true,
                ..CommandEffects::default()
            },
        };
    }
    let outcome = match execute_hsetex(db, &request, &settings) {
        Ok(outcome) => outcome,
        Err(error) => return CommandOutcome::reply(error),
    };
    HSetExCommit {
        request,
        settings,
        outcome,
    }
    .into_outcome(db, db_index)
}

fn build_settings(
    runtime: &dyn CommandRuntime,
    expiry: HSetExExpiry,
) -> Result<HSetExSettings, Value> {
    let now = runtime.now_ms();
    let (max_entries, max_value_size) = runtime.hash_encoding_limits();
    Ok(HSetExSettings {
        ignore_expiry: runtime.read_expired_values(),
        should_preserve_deadline: runtime.preserves_expired_deadline(),
        max_entries,
        max_value_size,
        now_ms: now,
        expire_at_ms: expiry.absolute_ms(now)?,
    })
}

impl HSetExCommit {
    fn into_outcome(
        self,
        db: &mut Db,
        db_index: u32,
    ) -> CommandOutcome {
        let mut effects = CommandEffects {
            changed_keys: vec![(db_index, self.request.key.clone())],
            expired_hash_fields: self.outcome.expired_count(),
            ..CommandEffects::default()
        };
        if !self.outcome.expired_fields.is_empty() {
            effects.skip_propagate = true;
            effects
                .custom_propagations
                .push(hash_field_deletion_propagation(
                    &self.request.key,
                    &self.outcome.expired_fields,
                ));
        } else if let Some(expire_at) = self.settings.expire_at_ms {
            effects
                .custom_propagations
                .push(encode_hsetex_at(&self.request, expire_at));
        } else if matches!(self.request.expiry, HSetExExpiry::KeepTtl) {
            effects.skip_propagate = true;
            for ((field, value), expiry) in self
                .request
                .pairs
                .iter()
                .zip(&self.outcome.preserved_expiries)
            {
                effects.custom_propagations.push(hash_field_value_propagation(
                    &self.request.key,
                    field,
                    value,
                    *expiry,
                ));
            }
        }
        if self.outcome.overwritten_expired > 0 {
            effects.events.push(self.event("hexpired"));
        }
        effects.events.push(self.event("hset"));
        if self.settings.expire_at_ms.is_some() {
            effects.events.push(self.event("hexpire"));
        }
        if !self.outcome.expired_fields.is_empty() {
            effects.events.push(self.event("hexpired"));
            if remove_empty_hash(db, &self.request.key) {
                effects.events.push(self.event("del"));
            }
        }
        CommandOutcome {
            reply: Value::integer(1),
            effects,
        }
    }

    fn event(&self, kind: &'static str) -> CoreEvent {
        CoreEvent {
            kind,
            key: self.request.key.clone(),
        }
    }
}

fn encode_hsetex_at(request: &HSetExRequest, expire_at: u64) -> Vec<Bytes> {
    let mut parts = vec![
        Bytes::from_static(b"HSETEX"),
        request.key.clone(),
        Bytes::from_static(b"PXAT"),
        expire_at.to_string().into(),
        Bytes::from_static(b"FIELDS"),
        request.pairs.len().to_string().into(),
    ];
    for (field, value) in &request.pairs {
        parts.push(field.clone());
        parts.push(value.clone());
    }
    parts
}

fn remove_empty_hash(db: &mut Db, key: &[u8]) -> bool {
    let empty = db
        .get_hash_object(key)
        .ok()
        .flatten()
        .map(|hash| hash.hash_is_empty())
        .unwrap_or(false);
    if empty {
        db.del(key);
    }
    empty
}

#[cfg(test)]
mod tests {
    use super::{hsetex, FieldExistence, HashExistence, HSetExExpiry, HSetExRequest};
    use crate::engine::command::CommandArgs;
    use crate::framework::commands::command_runtime::{CommandRuntime, TestCommandRuntime};
    use crate::foundation::io::protocol::Value;
    use crate::foundation::io::store::kv_engine::now_ms;
    use bytes::Bytes;

    fn runtime_with_field(now_ms: u64, expire_at: u64) -> TestCommandRuntime {
        let mut runtime = TestCommandRuntime::new(1, now_ms);
        let db = runtime.db_mut().unwrap();
        db.get_or_create_hash(b"hash")
            .unwrap()
            .insert(Bytes::from_static(b"field"), Bytes::from_static(b"old"));
        db.set_hash_field_expire(b"hash", b"field", expire_at);
        runtime
    }

    fn keepttl_args() -> CommandArgs {
        CommandArgs::from_parts(vec![
            Bytes::from_static(b"HSETEX"),
            Bytes::from_static(b"hash"),
            Bytes::from_static(b"KEEPTTL"),
            Bytes::from_static(b"FIELDS"),
            Bytes::from_static(b"1"),
            Bytes::from_static(b"field"),
            Bytes::from_static(b"new"),
        ])
    }

    #[test]
    fn request_parses_typed_conditions_expiry_and_pairs() {
        let args = CommandArgs::from_parts(vec![
            Bytes::from_static(b"HSETEX"),
            Bytes::from_static(b"hash"),
            Bytes::from_static(b"XX"),
            Bytes::from_static(b"FNX"),
            Bytes::from_static(b"PX"),
            Bytes::from_static(b"10"),
            Bytes::from_static(b"FIELDS"),
            Bytes::from_static(b"1"),
            Bytes::from_static(b"field"),
            Bytes::from_static(b"value"),
        ]);

        let request = HSetExRequest::parse(&args).unwrap();
        assert_eq!(request.hash_condition, HashExistence::Existing);
        assert_eq!(request.field_condition, FieldExistence::AllMissing);
        assert_eq!(request.expiry, HSetExExpiry::Milliseconds(10));
        assert_eq!(request.pairs.len(), 1);
    }

    #[test]
    fn keepttl_on_expired_field_propagates_hset_without_stale_ttl() {
        let now_ms = now_ms();
        let mut runtime = runtime_with_field(now_ms, now_ms.saturating_sub(1));
        let args = keepttl_args();
        let outcome = hsetex(&mut runtime, &args);
        assert_eq!(outcome.reply, Value::integer(1));
        assert_eq!(
            runtime
                .dbs
                .get(0)
                .unwrap()
                .get_hash_field_expire(b"hash", b"field"),
            None
        );
        assert_eq!(
            outcome.effects.custom_propagations,
            vec![vec![
                Bytes::from_static(b"HSET"),
                Bytes::from_static(b"hash"),
                Bytes::from_static(b"field"),
                Bytes::from_static(b"new"),
            ]]
        );
    }

    #[test]
    fn keepttl_on_live_field_propagates_absolute_expiry() {
        let now_ms = now_ms();
        let expire_at = now_ms.saturating_add(60_000);
        let mut runtime = runtime_with_field(now_ms, expire_at);
        let args = keepttl_args();
        let outcome = hsetex(&mut runtime, &args);
        assert_eq!(outcome.reply, Value::integer(1));
        assert_eq!(
            runtime
                .dbs
                .get(0)
                .unwrap()
                .get_hash_field_expire(b"hash", b"field"),
            Some(expire_at)
        );
        assert_eq!(
            outcome.effects.custom_propagations,
            vec![vec![
                Bytes::from_static(b"HSETEX"),
                Bytes::from_static(b"hash"),
                Bytes::from_static(b"PXAT"),
                Bytes::from(expire_at.to_string()),
                Bytes::from_static(b"FIELDS"),
                Bytes::from_static(b"1"),
                Bytes::from_static(b"field"),
                Bytes::from_static(b"new"),
            ]]
        );
    }
}
