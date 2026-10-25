use crate::engine::core::{CommandEffects, CommandOutcome, CoreEvent, Value};
use crate::foundation::io::store::Robj;
use crate::framework::commands::command_runtime::CommandRuntime;
use crate::framework::{CommandArgs, CommandError};
use bytes::Bytes;
use command_macros::command;

#[command("APPEND", runtime)]
pub fn append(runtime: &mut dyn CommandRuntime, args: &CommandArgs) -> CommandOutcome {
    let key = &args.args[0];
    let suffix = &args.args[1];
    let db_idx = runtime.selected_db();
    let len;

    {
        let db = match runtime.db_mut() {
            Some(d) => d,
            None => return CommandOutcome::reply(CommandError::db_index_out_of_range()),
        };
        let mut buf = match db.get_string(key) {
            Err(_) => return CommandOutcome::reply(CommandError::wrong_type()),
            Ok(None) => Vec::new(),
            Ok(Some(b)) => b.to_vec(),
        };
        buf.extend_from_slice(suffix);
        len = buf.len() as i64;
        db.set(key.to_vec(), Robj::string_raw(Bytes::from(buf)));
    }

    changed_key_outcome(Value::integer(len), db_idx, key, "append")
}

#[command("SETRANGE", runtime)]
pub fn setrange(runtime: &mut dyn CommandRuntime, args: &CommandArgs) -> CommandOutcome {
    let key = &args.args[0];
    let db_idx = runtime.selected_db();
    let should_touch = runtime.should_touch_read_keys();
    let offset = match std::str::from_utf8(&args.args[1])
        .ok()
        .and_then(|s| s.parse::<i64>().ok())
    {
        Some(n) if n >= 0 => n as usize,
        Some(_) => return CommandOutcome::reply(Value::error("ERR offset is out of range")),
        None => {
            return CommandOutcome::reply(Value::error(
                "ERR value is not an integer or out of range",
            ))
        }
    };
    let patch = &args.args[2];

    let len;
    {
        let db = match runtime.db_mut() {
            Some(d) => d,
            None => return CommandOutcome::reply(CommandError::db_index_out_of_range()),
        };
        if should_touch {
            db.touch_access_time(key);
        }
        let current_len = match db.get_string(key) {
            Err(_) => return CommandOutcome::reply(CommandError::wrong_type()),
            Ok(None) => 0,
            Ok(Some(bytes)) => bytes.len(),
        };
        if patch.is_empty() {
            return CommandOutcome::reply(Value::integer(current_len as i64));
        }
        let end = offset.saturating_add(patch.len());
        if end > MAX_STRING_SIZE {
            return CommandOutcome::reply(Value::error("ERR string exceeds maximum allowed size"));
        }
        len = match db.set_string_range(key, offset, patch) {
            Ok(len) => len as i64,
            Err(()) => return CommandOutcome::reply(CommandError::wrong_type()),
        };
    }

    changed_key_outcome(Value::integer(len), db_idx, key, "setrange")
}

#[command("STRLEN", runtime)]
pub fn strlen(runtime: &mut dyn CommandRuntime, args: &CommandArgs) -> CommandOutcome {
    let db = match runtime.db_mut() {
        Some(d) => d,
        None => return CommandOutcome::reply(CommandError::db_index_out_of_range()),
    };
    CommandOutcome::reply(match db.get_string(&args.args[0]) {
        Err(_) => CommandError::wrong_type(),
        Ok(None) => Value::integer(0),
        Ok(Some(b)) => Value::integer(b.len() as i64),
    })
}

const MAX_BIT_OFFSET: u64 = (1u64 << 32) - 1;
const MAX_STRING_SIZE: usize = 512 * 1024 * 1024;

#[command("SETBIT", runtime)]
pub fn setbit(runtime: &mut dyn CommandRuntime, args: &CommandArgs) -> CommandOutcome {
    let db_idx = runtime.selected_db();
    let db = match runtime.db_mut() {
        Some(d) => d,
        None => return CommandOutcome::reply(CommandError::db_index_out_of_range()),
    };
    let key = &args.args[0];

    let offset = match std::str::from_utf8(&args.args[1])
        .ok()
        .and_then(|s| s.parse::<i64>().ok())
    {
        Some(n) if n >= 0 && n as u64 <= MAX_BIT_OFFSET => n as u64,
        Some(_) => {
            return CommandOutcome::reply(Value::error(
                "ERR bit offset is not an integer or out of range",
            ));
        }
        None => {
            return CommandOutcome::reply(Value::error(
                "ERR bit offset is not an integer or out of range",
            ));
        }
    };

    let bit_value = match std::str::from_utf8(&args.args[2])
        .ok()
        .and_then(|s| s.parse::<i64>().ok())
    {
        Some(0) => 0u8,
        Some(1) => 1u8,
        Some(_) | None => {
            return CommandOutcome::reply(Value::error(
                "ERR bit is not an integer or out of range",
            ));
        }
    };

    let byte_offset = (offset / 8) as usize;
    let bit_in_byte = 7 - (offset % 8) as u8;

    let (mut buf, key_new) = match db.get_string(key) {
        Err(_) => return CommandOutcome::reply(CommandError::wrong_type()),
        Ok(None) => (Vec::new(), true),
        Ok(Some(b)) => (b.to_vec(), false),
    };

    if byte_offset >= MAX_STRING_SIZE {
        return CommandOutcome::reply(Value::error(
            "ERR string exceeds maximum allowed size",
        ));
    }

    let extended = buf.len() <= byte_offset;
    if extended {
        buf.resize(byte_offset + 1, 0);
    }

    let old_bit = (buf[byte_offset] >> bit_in_byte) & 1;

    let dirty = key_new || extended || (old_bit != bit_value);
    if !dirty {
        return CommandOutcome {
            reply: Value::integer(old_bit as i64),
            effects: CommandEffects {
                skip_propagate: true,
                ..CommandEffects::default()
            },
        };
    }

    if bit_value == 1 {
        buf[byte_offset] |= 1 << bit_in_byte;
    } else {
        buf[byte_offset] &= !(1 << bit_in_byte);
    }

    db.set(key.to_vec(), Robj::string_raw(Bytes::from(buf)));
    changed_key_outcome(Value::integer(old_bit as i64), db_idx, key, "setbit")
}

#[command("GETBIT", runtime)]
pub fn getbit(runtime: &mut dyn CommandRuntime, args: &CommandArgs) -> CommandOutcome {
    let db = match runtime.db_mut() {
        Some(d) => d,
        None => return CommandOutcome::reply(CommandError::db_index_out_of_range()),
    };
    let key = &args.args[0];

    let offset = match std::str::from_utf8(&args.args[1])
        .ok()
        .and_then(|s| s.parse::<i64>().ok())
    {
        Some(n) if n >= 0 && n as u64 <= MAX_BIT_OFFSET => n as u64,
        Some(_) => {
            return CommandOutcome::reply(Value::error(
                "ERR bit offset is not an integer or out of range",
            ));
        }
        None => {
            return CommandOutcome::reply(Value::error(
                "ERR bit offset is not an integer or out of range",
            ));
        }
    };

    let byte_offset = (offset / 8) as usize;
    let bit_in_byte = 7 - (offset % 8) as u8;

    CommandOutcome::reply(match db.get_string(key) {
        Err(_) => CommandError::wrong_type(),
        Ok(None) => Value::integer(0),
        Ok(Some(b)) => {
            let bytes = b;
            if byte_offset >= bytes.len() {
                return CommandOutcome::reply(Value::integer(0));
            }
            let bit = (bytes[byte_offset] >> bit_in_byte) & 1;
            Value::integer(bit as i64)
        }
    })
}

#[command("SUBSTR", runtime)]
#[command("GETRANGE", runtime)]
pub fn getrange(runtime: &mut dyn CommandRuntime, args: &CommandArgs) -> CommandOutcome {
    let db = match runtime.db_mut() {
        Some(d) => d,
        None => return CommandOutcome::reply(CommandError::db_index_out_of_range()),
    };
    let key = &args.args[0];

    let start = match std::str::from_utf8(&args.args[1])
        .ok()
        .and_then(|s| s.parse::<i64>().ok())
    {
        Some(n) => n,
        None => {
            return CommandOutcome::reply(Value::error(
                "ERR value is not an integer or out of range",
            ))
        }
    };

    let end = match std::str::from_utf8(&args.args[2])
        .ok()
        .and_then(|s| s.parse::<i64>().ok())
    {
        Some(n) => n,
        None => {
            return CommandOutcome::reply(Value::error(
                "ERR value is not an integer or out of range",
            ))
        }
    };

    CommandOutcome::reply(match db.get_string(key) {
        Err(_) => CommandError::wrong_type(),
        Ok(None) => Value::bulk(Bytes::new()),
        Ok(Some(b)) => {
            let bytes = b;
            let len = bytes.len() as i64;

            let mut s = if start < 0 { len + start } else { start };
            let mut e = if end < 0 { len + end } else { end };

            if s < 0 {
                s = 0;
            }
            if e >= len {
                e = len - 1;
            }

            if s > e || len == 0 {
                return CommandOutcome::reply(Value::bulk(Bytes::new()));
            }

            let s = s as usize;
            let e = e as usize;
            Value::bulk(Bytes::copy_from_slice(&bytes[s..=e]))
        }
    })
}

fn changed_key_outcome(
    reply: Value,
    db_idx: u32,
    key: &Bytes,
    event: &'static str,
) -> CommandOutcome {
    CommandOutcome {
        reply,
        effects: CommandEffects {
            changed_keys: vec![(db_idx, key.clone())],
            events: vec![CoreEvent {
                kind: event,
                key: key.clone(),
            }],
            ..CommandEffects::default()
        },
    }
}

#[cfg(test)]
mod tests {
    use super::{setrange, MAX_STRING_SIZE};
    use crate::engine::core::{CommandOutcome, Value};
    use crate::foundation::io::store::structure::list::ListValue;
    use crate::foundation::io::store::Robj;
    use crate::foundation::memory::metrics::memory_stats::{self, MemoryCategory};
    use crate::framework::commands::command_runtime::{CommandRuntime, TestCommandRuntime};
    use crate::framework::{CommandArgs, CommandError};
    use bytes::{Bytes, BytesMut};

    fn run_setrange(
        runtime: &mut TestCommandRuntime,
        key: &'static [u8],
        offset: usize,
        patch: &'static [u8],
    ) -> CommandOutcome {
        let args = CommandArgs::from_parts(vec![
            Bytes::from_static(b"SETRANGE"),
            Bytes::from_static(key),
            Bytes::from(offset.to_string()),
            Bytes::from_static(patch),
        ]);
        setrange(runtime, &args)
    }

    #[test]
    fn setrange_reuses_unique_raw_allocation_for_repeated_small_writes() {
        let mut runtime = TestCommandRuntime::new(1, 0);
        let original = Bytes::from(vec![b'x'; 100 * 1024]);
        let original_ptr = original.as_ptr();
        runtime
            .db_mut()
            .unwrap()
            .set(b"key".to_vec(), Robj::string_raw(original));

        for _ in 0..64 {
            let outcome = run_setrange(&mut runtime, b"key", 50 * 1024, b"ab");
            assert_eq!(outcome.reply, Value::integer(100 * 1024));
            assert_eq!(
                outcome.effects.changed_keys,
                vec![(0, Bytes::from_static(b"key"))]
            );
            assert_eq!(outcome.effects.events.len(), 1);
            assert_eq!(outcome.effects.events[0].kind, "setrange");
            let stored = runtime.db_mut().unwrap().get(b"key").unwrap();
            assert_eq!(&stored.as_bytes()[50 * 1024..50 * 1024 + 2], b"ab");
            assert_eq!(
                stored.as_bytes().as_ptr(),
                original_ptr,
                "an in-bounds write must retain the uniquely owned allocation"
            );
        }
    }

    #[test]
    fn setrange_preserves_ttl_across_string_encodings() {
        for original in [
            Robj::string_raw(Bytes::from(vec![b'x'; 128])),
            Robj::string("hello"),
            Robj::string("12345"),
        ] {
            let original_len = original.as_bytes().len();
            let mut runtime = TestCommandRuntime::new(1, 0);
            let db = runtime.db_mut().unwrap();
            db.set(b"key".to_vec(), original);
            db.set_expire(b"key", u64::MAX);
            db.mark_replica_local_expire(b"key");

            let outcome = run_setrange(&mut runtime, b"key", 1, b"z");

            assert_eq!(outcome.reply, Value::integer(original_len as i64));
            let db = runtime.db_mut().unwrap();
            assert_eq!(db.expire_of(b"key"), Some(u64::MAX));
            assert_eq!(db.get(b"key").unwrap().encoding_name(), "raw");
            db.set_replica_local_expire_only(true);
            db.set_expire(b"key", 0);
            assert!(db.get(b"key").is_none());
            assert_eq!(db.expired_deletions, vec![b"key".to_vec()]);
        }
    }

    #[test]
    fn setrange_no_touch_preserves_lfu_metadata() {
        let mut runtime = TestCommandRuntime::new(1, 0);
        let db = runtime.db_mut().unwrap();
        db.set_eviction_metadata_config(true, 10, 0);
        db.set(b"key".to_vec(), Robj::string("hello"));
        db.set_lfu_freq(b"key", 73);
        let access_minutes = db.get_lfu_access_minutes(b"key");

        assert_eq!(
            run_setrange(&mut runtime, b"key", 0, b"j").reply,
            Value::integer(5)
        );

        let db = runtime.db_mut().unwrap();
        assert_eq!(db.get_lfu_freq(b"key"), 73);
        assert_eq!(db.get_lfu_access_minutes(b"key"), access_minutes);
    }

    #[test]
    fn setrange_no_touch_preserves_lru_metadata() {
        let mut runtime = TestCommandRuntime::new(1, 0);
        let db = runtime.db_mut().unwrap();
        db.set(b"key".to_vec(), Robj::string("hello"));
        db.set_lru_idle(b"key", 120);
        let lru = db.get_lru_raw(b"key");

        assert_eq!(
            run_setrange(&mut runtime, b"key", 0, b"j").reply,
            Value::integer(5)
        );
        assert_eq!(runtime.db_mut().unwrap().get_lru_raw(b"key"), lru);
    }

    #[test]
    fn setrange_normal_lookup_refreshes_lru_without_clearing_ttl() {
        let mut runtime = TestCommandRuntime::new(1, 0);
        runtime.touch_read_keys = true;
        let db = runtime.db_mut().unwrap();
        db.set(b"key".to_vec(), Robj::string("hello"));
        db.set_expire(b"key", u64::MAX);
        db.set_lru_idle(b"key", 120);

        assert_eq!(
            run_setrange(&mut runtime, b"key", 0, b"j").reply,
            Value::integer(5)
        );
        let db = runtime.db_mut().unwrap();
        assert!(db.get_lru_idle(b"key") < 2);
        assert_eq!(db.expire_of(b"key"), Some(u64::MAX));
    }

    #[test]
    fn setrange_normal_lookup_increments_lfu_without_resetting_frequency() {
        let mut runtime = TestCommandRuntime::new(1, 0);
        runtime.touch_read_keys = true;
        let db = runtime.db_mut().unwrap();
        db.set_eviction_metadata_config(true, 0, 0);
        db.set(b"key".to_vec(), Robj::string("hello"));
        db.set_lfu_freq(b"key", 73);

        assert_eq!(
            run_setrange(&mut runtime, b"key", 0, b"j").reply,
            Value::integer(5)
        );
        assert_eq!(runtime.db_mut().unwrap().get_lfu_freq(b"key"), 74);
    }

    #[test]
    fn setrange_isolates_shared_objects_and_retained_bytes() {
        let mut runtime = TestCommandRuntime::new(1, 0);
        let original = Bytes::from(vec![b'x'; 128]);
        let retained = original.slice(32..96);
        let snapshot = Robj::string_raw(original);
        let db = runtime.db_mut().unwrap();
        db.set(b"key".to_vec(), snapshot.clone());
        db.set(b"copy".to_vec(), snapshot.clone());

        assert_eq!(
            run_setrange(&mut runtime, b"key", 32, b"ab").reply,
            Value::integer(128)
        );

        let db = runtime.db_mut().unwrap();
        let stored = db.get(b"key").unwrap();
        assert_eq!(&stored.as_bytes()[32..34], b"ab");
        assert_ne!(stored.as_bytes().as_ptr(), snapshot.as_bytes().as_ptr());
        assert_eq!(snapshot.as_bytes(), &[b'x'; 128]);
        assert_eq!(retained.as_ref(), &[b'x'; 64]);
        assert_eq!(db.get(b"copy").unwrap().as_bytes(), &[b'x'; 128]);
    }

    #[test]
    fn setrange_growth_reuses_spare_capacity_and_updates_memory_accounting() {
        let guard = memory_stats::lock_global_for_test();
        let initial = memory_stats::get_tracker().get(MemoryCategory::Dataset);
        let mut runtime = TestCommandRuntime::new(1, 0);
        let mut original = BytesMut::with_capacity(256);
        original.resize(64, b'x');
        let original_ptr = original.as_ptr();
        runtime
            .db_mut()
            .unwrap()
            .set(b"key".to_vec(), Robj::string_raw(original.freeze()));
        let before = memory_stats::get_tracker().get(MemoryCategory::Dataset);

        let outcome = run_setrange(&mut runtime, b"key", 80, b"ab");

        let db = runtime.db_mut().unwrap();
        let stored = db.get(b"key").unwrap();
        let stored_bytes = stored.as_bytes().to_vec();
        let stored_ptr = stored.as_bytes().as_ptr();
        let after = memory_stats::get_tracker().get(MemoryCategory::Dataset);
        db.flush();
        let final_size = memory_stats::get_tracker().get(MemoryCategory::Dataset);
        drop(guard);

        assert_eq!(outcome.reply, Value::integer(82));
        assert_eq!(before, initial + 3 + 64 + 16);
        assert_eq!(&stored_bytes[..64], &[b'x'; 64]);
        assert_eq!(&stored_bytes[64..80], &[0; 16]);
        assert_eq!(&stored_bytes[80..], b"ab");
        assert_eq!(after, initial + 3 + 82 + 16);
        assert_eq!(final_size, initial);
        assert_eq!(stored_ptr, original_ptr);
    }

    #[test]
    fn setrange_creates_zero_filled_missing_string() {
        let mut runtime = TestCommandRuntime::new(1, 0);
        let outcome = run_setrange(&mut runtime, b"key", 3, b"ab");
        assert_eq!(outcome.reply, Value::integer(5));
        let db = runtime.db_mut().unwrap();
        assert_eq!(db.get(b"key").unwrap().as_bytes(), b"\0\0\0ab");
        assert_eq!(db.expire_of(b"key"), None);
    }

    #[test]
    fn setrange_empty_patch_does_not_change_value_or_create_key() {
        let mut runtime = TestCommandRuntime::new(1, 0);
        let db = runtime.db_mut().unwrap();
        db.set(b"key".to_vec(), Robj::string("12345"));
        db.set_expire(b"key", u64::MAX);
        let original_ptr = db.get(b"key").unwrap().as_bytes().as_ptr();

        let outcome = run_setrange(&mut runtime, b"key", MAX_STRING_SIZE + 1, b"");
        assert_eq!(outcome.reply, Value::integer(5));
        assert!(outcome.effects.changed_keys.is_empty());
        assert!(outcome.effects.events.is_empty());
        let db = runtime.db_mut().unwrap();
        let stored = db.get(b"key").unwrap();
        assert_eq!(stored.encoding_name(), "int");
        assert_eq!(stored.as_bytes().as_ptr(), original_ptr);
        assert_eq!(db.expire_of(b"key"), Some(u64::MAX));

        let outcome = run_setrange(&mut runtime, b"missing", MAX_STRING_SIZE + 1, b"");
        assert_eq!(outcome.reply, Value::integer(0));
        assert!(outcome.effects.changed_keys.is_empty());
        assert!(!runtime.db_mut().unwrap().exists(b"missing"));
    }

    #[test]
    fn setrange_rejects_invalid_writes_without_mutating_storage() {
        let mut runtime = TestCommandRuntime::new(1, 0);
        let db = runtime.db_mut().unwrap();
        db.set(b"key".to_vec(), Robj::string("hello"));
        db.set_expire(b"key", u64::MAX);
        db.set(b"list".to_vec(), Robj::ListQuicklist(ListValue::new()));

        let outcome = run_setrange(&mut runtime, b"key", MAX_STRING_SIZE, b"x");
        assert_eq!(
            outcome.reply,
            Value::error("ERR string exceeds maximum allowed size")
        );
        assert!(outcome.effects.changed_keys.is_empty());
        let db = runtime.db_mut().unwrap();
        assert_eq!(db.get(b"key").unwrap().as_bytes(), b"hello");
        assert_eq!(db.expire_of(b"key"), Some(u64::MAX));

        for patch in [b"".as_slice(), b"x".as_slice()] {
            let outcome = run_setrange(&mut runtime, b"list", MAX_STRING_SIZE, patch);
            assert_eq!(outcome.reply, CommandError::wrong_type());
            assert!(outcome.effects.changed_keys.is_empty());
        }
    }
}
