use super::*;
use crate::foundation::io::store::Robj;
use crate::framework::commands::command_runtime::{CommandRuntime, TestCommandRuntime};
use crate::framework::commands::hyperloglog::{
    pfadd::pfadd, pfcount::pfcount, pfdebug::pfdebug, pfmerge::pfmerge,
};
use crate::framework::CommandArgs;
use bytes::Bytes;

fn sparse(payload: &[u8]) -> Vec<u8> {
    let mut data = c_format::empty();
    data.truncate(HLL_HDR_SIZE);
    data.extend_from_slice(payload);
    data
}

fn args(parts: &[&'static [u8]]) -> CommandArgs {
    CommandArgs::from_parts(parts.iter().map(|part| Bytes::from_static(part)).collect())
}

#[test]
fn partial_error_preserves_bytes_and_propagates_only_successful_prefix() {
    let initial = sparse(&[0x5f, 0xff]);
    let mut expected = initial.clone();
    assert!(c_format::add(&mut expected, b"d", 3000).unwrap());
    let mut runtime = TestCommandRuntime::new(1, 1000);
    runtime
        .db_mut()
        .unwrap()
        .set(b"hll".to_vec(), Robj::string_raw(initial));

    let outcome = pfadd(&mut runtime, &args(&[b"pfadd", b"hll", b"d", b"a"]));

    assert_eq!(outcome.reply, error_value(HllError::InvalidObject));
    assert_eq!(outcome.effects.dirty_delta, Some(1));
    assert_eq!(
        outcome.effects.changed_keys,
        vec![(0, Bytes::from_static(b"hll"))]
    );
    assert!(outcome.effects.skip_propagate);
    assert_eq!(
        outcome.effects.custom_propagations,
        vec![vec![
            Bytes::from_static(b"pfadd"),
            Bytes::from_static(b"hll"),
            Bytes::from_static(b"d"),
        ]]
    );
    assert_eq!(
        runtime.db_mut().unwrap().get_string(b"hll"),
        Ok(Some(expected.as_slice()))
    );
}

#[test]
fn error_before_first_update_has_no_effects() {
    let initial = sparse(&[0x5f, 0xff]);
    let mut runtime = TestCommandRuntime::new(1, 1000);
    runtime
        .db_mut()
        .unwrap()
        .set(b"hll".to_vec(), Robj::string_raw(initial.clone()));

    let outcome = pfadd(&mut runtime, &args(&[b"pfadd", b"hll", b"a", b"d"]));

    assert_eq!(outcome.reply, error_value(HllError::InvalidObject));
    assert!(outcome.effects.changed_keys.is_empty());
    assert!(outcome.effects.custom_propagations.is_empty());
    assert_eq!(
        runtime.db_mut().unwrap().get_string(b"hll"),
        Ok(Some(initial.as_slice()))
    );
}

#[test]
fn short_standard_merge_includes_destination_and_sets_real_cache() {
    let mut runtime = TestCommandRuntime::new(1, 1000);
    runtime.db_mut().unwrap().set(
        b"dst".to_vec(),
        Robj::string_raw(sparse(&[0x40, 0x40, 0x7f, 0xbe])),
    );
    runtime.db_mut().unwrap().set(
        b"src".to_vec(),
        Robj::string_raw(sparse(&[0x80, 0x7f, 0xfe])),
    );

    let outcome = pfmerge(&mut runtime, &args(&[b"pfmerge", b"dst", b"src"]));
    assert_eq!(outcome.reply, Value::ok());
    assert_eq!(
        pfcount(&mut runtime, &args(&[b"pfcount", b"dst"])).reply,
        Value::integer(1)
    );
    let data = runtime
        .db_mut()
        .unwrap()
        .get_string(b"dst")
        .unwrap()
        .unwrap();
    assert_eq!(&data[8..16], &1u64.to_le_bytes());
    assert_eq!(hll_format(data), Ok(HllFormat::Standard));

    let mut existing = c_format::empty();
    c_format::set_register(&mut existing, 100, 1, 3000).unwrap();
    runtime
        .db_mut()
        .unwrap()
        .set(b"dst".to_vec(), Robj::string_raw(existing));
    assert_eq!(
        pfmerge(&mut runtime, &args(&[b"pfmerge", b"dst", b"src"])).reply,
        Value::ok()
    );
    assert_eq!(
        pfcount(&mut runtime, &args(&[b"pfcount", b"dst"])).reply,
        Value::integer(2)
    );
    assert_eq!(
        pfcount(&mut runtime, &args(&[b"pfcount", b"dst", b"src"])).reply,
        Value::integer(2)
    );
}

#[test]
fn malformed_legacy_framing_never_falls_back_to_standard() {
    let mut data = empty_hll();
    data[16..20].copy_from_slice(&1u32.to_le_bytes());
    data.extend_from_slice(&[0xff, 0xff, 1]);
    assert_eq!(hll_format(&data), Ok(HllFormat::Legacy));
    assert!(matches!(parse_hll(&data), Err(HllError::InvalidObject)));
    assert_eq!(cardinality_from_bytes(&data), Err(HllError::InvalidObject));
}

#[test]
fn private_serialization_hash_and_observed_count_are_preserved() {
    let mut state = HllState::empty();
    state.add(b"legacy");
    let mut data = serialize_hll(&state, 3000, true);
    set_observed_count(&mut data, 123);
    let decoded = parse_hll(&data).unwrap();
    assert_eq!(decoded.format, HllFormat::Legacy);
    assert_eq!(decoded.registers(), state.registers());
    assert_eq!(cached_observed_count(&data), Some(123));
    assert_eq!(data.len(), HLL_HDR_SIZE + 4 + 3);
    assert_eq!(
        hll_format(&serialize_hll_as(&state, HllEncoding::Dense, true)),
        Ok(HllFormat::Legacy)
    );
}

#[test]
fn standard_objects_never_read_private_observed_metadata() {
    let mut data = sparse(&[0x80, 0x7f, 0xfe]);
    data[14] = HLL_NON_INTEGER_INPUTS;
    assert_eq!(cached_observed_count(&data), None);
    assert_eq!(cardinality_from_bytes(&data), Ok(1));
}

fn ttl_now_ms() -> u64 {
    #[cfg(not(feature = "wasm"))]
    {
        1000
    }
    #[cfg(feature = "wasm")]
    {
        crate::foundation::io::sys::time::mstime() as u64
    }
}

fn ttl_expires_at_ms() -> u64 {
    ttl_now_ms() + 60000
}

#[cfg(not(feature = "wasm"))]
fn enter_ttl_snapshot() -> crate::foundation::io::sys::time::CommandTimeSnapshotGuard {
    crate::foundation::io::sys::time::enter_command_time_snapshot(ttl_now_ms() as i64)
}

#[cfg(feature = "wasm")]
struct TtlSnapshot;

#[cfg(feature = "wasm")]
fn enter_ttl_snapshot() -> TtlSnapshot {
    TtlSnapshot
}

fn store_expiring_standard_hll(
    runtime: &mut TestCommandRuntime,
    key: &[u8],
    data: Vec<u8>,
    expires_at_ms: u64,
) {
    assert_eq!(hll_format(&data), Ok(HllFormat::Standard));
    let db = runtime.db_mut().unwrap();
    db.set(key.to_vec(), Robj::string_raw(data));
    db.set_expire(key, expires_at_ms);
    assert_eq!(db.get_expire(key), Some(expires_at_ms));
    assert!(!db.is_expired(key));
}

fn assert_absolute_expiry(runtime: &mut TestCommandRuntime, key: &[u8], expires_at_ms: u64) {
    let db = runtime.db_mut().unwrap();
    assert_eq!(
        db.get_expire(key),
        Some(expires_at_ms),
        "HLL writeback must retain the original absolute expiry"
    );
    assert!(db.get_at(key, expires_at_ms - 1).is_some());
    assert!(db.get_at(key, expires_at_ms + 1).is_none());
}

fn assert_standard_pfadd_ttl(is_dense: bool) {
    let _guard = enter_ttl_snapshot();
    let initial = if is_dense {
        c_format::to_dense(&c_format::empty()).unwrap()
    } else {
        c_format::empty()
    };
    let mut runtime = TestCommandRuntime::new(1, ttl_now_ms());
    store_expiring_standard_hll(&mut runtime, b"hll", initial.clone(), ttl_expires_at_ms());

    let outcome = pfadd(&mut runtime, &args(&[b"pfadd", b"hll", b"d"]));

    assert_eq!(outcome.reply, Value::integer(1));
    assert_eq!(outcome.effects.dirty_delta, Some(1));
    assert_eq!(
        outcome.effects.changed_keys,
        vec![(0, Bytes::from_static(b"hll"))]
    );
    let data = runtime.db_mut().unwrap().get_string(b"hll").unwrap().unwrap();
    assert_ne!(data, initial.as_slice());
    assert_eq!(hll_format(data), Ok(HllFormat::Standard));
    assert_eq!(data[4], if is_dense { 0 } else { 1 });
    assert_eq!(data[15] & 0x80, 0x80);
    let mut expected_registers = vec![0; HLL_REGISTERS];
    expected_registers[7292] = 1;
    assert_eq!(c_format::decode(data).unwrap(), expected_registers);
    assert_absolute_expiry(&mut runtime, b"hll", ttl_expires_at_ms());
}

#[test]
fn standard_pfadd_sparse_success_preserves_absolute_ttl() {
    assert_standard_pfadd_ttl(false);
}

#[test]
fn standard_pfadd_dense_success_preserves_absolute_ttl() {
    assert_standard_pfadd_ttl(true);
}

#[test]
fn standard_pfadd_partial_failure_preserves_absolute_ttl() {
    let _guard = enter_ttl_snapshot();
    let initial = sparse(&[0x5f, 0xff]);
    let mut expected = sparse(&[0x5c, 0x7b, 0x80, 0x43, 0x82]);
    expected[15] = 0x80;
    assert_ne!(expected, initial);
    let mut runtime = TestCommandRuntime::new(1, ttl_now_ms());
    store_expiring_standard_hll(&mut runtime, b"hll", initial, ttl_expires_at_ms());

    let outcome = pfadd(&mut runtime, &args(&[b"pfadd", b"hll", b"d", b"a"]));

    assert_eq!(outcome.reply, error_value(HllError::InvalidObject));
    assert_eq!(outcome.effects.dirty_delta, Some(1));
    assert_eq!(
        outcome.effects.changed_keys,
        vec![(0, Bytes::from_static(b"hll"))]
    );
    assert!(outcome.effects.skip_propagate);
    assert_eq!(
        outcome.effects.custom_propagations,
        vec![vec![
            Bytes::from_static(b"pfadd"),
            Bytes::from_static(b"hll"),
            Bytes::from_static(b"d"),
        ]]
    );
    assert_eq!(
        runtime.db_mut().unwrap().get_string(b"hll"),
        Ok(Some(expected.as_slice()))
    );
    assert_absolute_expiry(&mut runtime, b"hll", ttl_expires_at_ms());
}

fn assert_standard_pfcount_ttl(is_dense: bool, is_dirty: bool) {
    let _guard = enter_ttl_snapshot();
    let initial = sparse(&[0x80, 0x7f, 0xfe]);
    let mut initial = if is_dense {
        c_format::to_dense(&initial).unwrap()
    } else {
        initial
    };
    initial[8..16].copy_from_slice(&(if is_dirty { 99u64 } else { 1u64 }).to_le_bytes());
    if is_dirty {
        initial[15] |= 0x80;
    }
    let mut expected = initial.clone();
    expected[8..16].copy_from_slice(&1u64.to_le_bytes());
    if is_dirty {
        assert_ne!(expected, initial);
    }
    let mut runtime = TestCommandRuntime::new(1, ttl_now_ms());
    store_expiring_standard_hll(&mut runtime, b"hll", initial, ttl_expires_at_ms());

    let outcome = pfcount(&mut runtime, &args(&[b"pfcount", b"hll"]));

    assert_eq!(outcome.reply, Value::integer(1));
    assert_eq!(
        outcome.effects.dirty_delta,
        if is_dirty { Some(1) } else { None }
    );
    assert_eq!(
        outcome.effects.changed_keys,
        if is_dirty {
            vec![(0, Bytes::from_static(b"hll"))]
        } else {
            vec![]
        }
    );
    assert_eq!(
        runtime.db_mut().unwrap().get_string(b"hll"),
        Ok(Some(expected.as_slice()))
    );
    assert_absolute_expiry(&mut runtime, b"hll", ttl_expires_at_ms());
}

#[test]
fn standard_pfcount_sparse_dirty_cache_preserves_absolute_ttl() {
    assert_standard_pfcount_ttl(false, true);
}

#[test]
fn standard_pfcount_dense_dirty_cache_preserves_absolute_ttl() {
    assert_standard_pfcount_ttl(true, true);
}

#[test]
fn standard_pfcount_sparse_clean_cache_preserves_absolute_ttl() {
    assert_standard_pfcount_ttl(false, false);
}

#[test]
fn standard_pfcount_dense_clean_cache_preserves_absolute_ttl() {
    assert_standard_pfcount_ttl(true, false);
}

fn assert_standard_pfdebug_ttl(is_dense: bool, should_get_registers: bool) {
    let _guard = enter_ttl_snapshot();
    let initial = sparse(&[0x80, 0x7f, 0xfe]);
    let initial = if is_dense {
        c_format::to_dense(&initial).unwrap()
    } else {
        initial
    };
    let expected = c_format::to_dense(&initial).unwrap();
    let mut runtime = TestCommandRuntime::new(1, ttl_now_ms());
    store_expiring_standard_hll(&mut runtime, b"hll", initial, ttl_expires_at_ms());
    let subcommand: &'static [u8] = if should_get_registers {
        b"getreg"
    } else {
        b"todense"
    };

    let outcome = pfdebug(&mut runtime, &args(&[b"pfdebug", subcommand, b"hll"]));

    if should_get_registers {
        let mut registers = vec![Value::integer(0); HLL_REGISTERS];
        registers[0] = Value::integer(1);
        assert_eq!(outcome.reply, Value::Array(registers));
    } else {
        assert_eq!(outcome.reply, Value::integer(if is_dense { 0 } else { 1 }));
    }
    assert_eq!(
        outcome.effects.dirty_delta,
        if is_dense { None } else { Some(1) }
    );
    assert_eq!(
        outcome.effects.changed_keys,
        if is_dense {
            vec![]
        } else {
            vec![(0, Bytes::from_static(b"hll"))]
        }
    );
    assert_eq!(expected.len(), 12304);
    assert_eq!(expected[4], 0);
    assert_eq!(
        runtime.db_mut().unwrap().get_string(b"hll"),
        Ok(Some(expected.as_slice()))
    );
    assert_absolute_expiry(&mut runtime, b"hll", ttl_expires_at_ms());
}

#[test]
fn standard_pfdebug_getreg_sparse_conversion_preserves_absolute_ttl() {
    assert_standard_pfdebug_ttl(false, true);
}

#[test]
fn standard_pfdebug_getreg_dense_read_preserves_absolute_ttl() {
    assert_standard_pfdebug_ttl(true, true);
}

#[test]
fn standard_pfdebug_todense_sparse_conversion_preserves_absolute_ttl() {
    assert_standard_pfdebug_ttl(false, false);
}

#[test]
fn standard_pfdebug_todense_dense_noop_preserves_absolute_ttl() {
    assert_standard_pfdebug_ttl(true, false);
}

fn assert_standard_pfmerge_ttl(is_dense: bool, has_destination: bool) {
    let _guard = enter_ttl_snapshot();
    let mut runtime = TestCommandRuntime::new(1, ttl_now_ms());
    if has_destination {
        let destination = sparse(&[0x80, 0x7f, 0xfe]);
        let destination = if is_dense {
            c_format::to_dense(&destination).unwrap()
        } else {
            destination
        };
        store_expiring_standard_hll(&mut runtime, b"dst", destination, ttl_expires_at_ms());
    } else {
        assert!(runtime.db_mut().unwrap().get(b"dst").is_none());
        assert_eq!(runtime.db_mut().unwrap().get_expire(b"dst"), None);
    }
    let source = sparse(&[0x00, 0x84, 0x7f, 0xfd]);
    let source = if is_dense {
        c_format::to_dense(&source).unwrap()
    } else {
        source
    };
    let source_expires_at_ms = ttl_expires_at_ms() + 17000;
    store_expiring_standard_hll(
        &mut runtime,
        b"src",
        source.clone(),
        source_expires_at_ms,
    );

    let outcome = pfmerge(&mut runtime, &args(&[b"pfmerge", b"dst", b"src"]));

    assert_eq!(outcome.reply, Value::ok());
    assert_eq!(
        outcome.effects.changed_keys,
        vec![(0, Bytes::from_static(b"dst"))]
    );
    let data = runtime.db_mut().unwrap().get_string(b"dst").unwrap().unwrap();
    assert_eq!(hll_format(data), Ok(HllFormat::Standard));
    assert_eq!(data[4], if is_dense { 0 } else { 1 });
    assert_eq!(data[15] & 0x80, 0x80);
    let mut expected_registers = vec![0; HLL_REGISTERS];
    if has_destination {
        expected_registers[0] = 1;
    }
    expected_registers[1] = 2;
    assert_eq!(c_format::decode(data).unwrap(), expected_registers);
    assert_eq!(
        runtime.db_mut().unwrap().get_string(b"src"),
        Ok(Some(source.as_slice()))
    );
    assert_absolute_expiry(&mut runtime, b"src", source_expires_at_ms);
    if has_destination {
        assert_absolute_expiry(&mut runtime, b"dst", ttl_expires_at_ms());
    } else {
        let db = runtime.db_mut().unwrap();
        assert_eq!(db.get_expire(b"dst"), None);
        assert!(db.get_at(b"dst", source_expires_at_ms + 1).is_some());
    }
}

#[test]
fn standard_pfmerge_sparse_existing_target_preserves_absolute_ttl() {
    assert_standard_pfmerge_ttl(false, true);
}

#[test]
fn standard_pfmerge_dense_existing_target_preserves_absolute_ttl() {
    assert_standard_pfmerge_ttl(true, true);
}

#[test]
fn standard_pfmerge_sparse_missing_target_does_not_inherit_source_ttl() {
    assert_standard_pfmerge_ttl(false, false);
}

#[test]
fn standard_pfmerge_dense_missing_target_does_not_inherit_source_ttl() {
    assert_standard_pfmerge_ttl(true, false);
}
