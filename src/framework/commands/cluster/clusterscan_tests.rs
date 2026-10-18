
use super::*;
use crate::engine::distributed::cluster::ClusterState;
use crate::engine::command::CommandArgs;
use crate::foundation::io::store::Robj;
use crate::framework::capabilities::connection::Connection;
use crate::framework::capabilities::io::Token;
use bytes::Bytes;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};

fn args(parts: &[&'static [u8]]) -> CommandArgs {
    CommandArgs::from_parts(parts.iter().map(|p| Bytes::from_static(p)).collect())
}

fn conn() -> Connection {
    Connection::new(
        Token(1),
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 12345),
        false,
    )
}

fn cluster_context() -> ServerContext {
    let mut ctx = ServerContext::new_with_slot_router(1, 14, |key| {
        crate::engine::distributed::cluster::node::key_hash_slot(key) as usize
    });
    ctx.config_mut().server.cluster_enabled = true;
    let mut state = ClusterState::new("127.0.0.1", 6379, 16379);
    for slot in 0..CLUSTER_SLOTS {
        state.add_slot(slot).unwrap();
    }
    state.recompute_state();
    *ctx.cluster_state_mut() = Some(Box::new(state));
    ctx
}

#[test]
fn clusterscan_when_cluster_disabled_returns_error() {
    let mut ctx = ServerContext::new(1);
    let mut conn = conn();
    let reply = clusterscan(&mut conn, &args(&[b"CLUSTERSCAN", b"0"]), &mut ctx);

    assert_eq!(
        reply,
        Value::error("ERR This instance has cluster support disabled")
    );
}

#[test]
fn clusterscan_slot_restricts_scan_to_single_slot() {
    let mut ctx = cluster_context();
    ctx.db_mut(0)
        .unwrap()
        .set(b"{06S}key".to_vec(), Robj::string("slot0"));
    ctx.db_mut(0)
        .unwrap()
        .set(b"{6ZJ}key".to_vec(), Robj::string("slot16383"));

    let mut conn = conn();
    let reply = clusterscan(
        &mut conn,
        &args(&[b"CLUSTERSCAN", b"0", b"SLOT", b"0"]),
        &mut ctx,
    );

    let Value::Array(items) = reply else {
        panic!("CLUSTERSCAN should return an array");
    };
    assert_eq!(items.len(), 2);
    assert_eq!(items[0], Value::bulk("0"));
    assert_eq!(
        items[1],
        Value::Array(vec![Value::bulk(Bytes::from_static(b"{06S}key"))])
    );
}

#[test]
fn clusterscan_accepts_hashtag_cursor_and_rejects_slot_mismatch() {
    let mut ctx = cluster_context();
    ctx.db_mut(0)
        .unwrap()
        .set(b"{06S}key".to_vec(), Robj::string("slot0"));
    let mut conn = conn();

    let reply = clusterscan(
        &mut conn,
        &args(&[b"CLUSTERSCAN", b"0-{06S}-0", b"SLOT", b"0"]),
        &mut ctx,
    );
    assert!(matches!(reply, Value::Array(_)));

    let mismatch = clusterscan(
        &mut conn,
        &args(&[b"CLUSTERSCAN", b"0-{06S}-0", b"SLOT", b"1"]),
        &mut ctx,
    );
    assert_eq!(mismatch, Value::error("ERR Cursor slot mismatch"));
}

#[test]
fn clusterscan_returns_clusterdown_for_unserved_cursor_slot() {
    let mut ctx = ServerContext::new_with_slot_router(1, 14, |key| {
        crate::engine::distributed::cluster::node::key_hash_slot(key) as usize
    });
    ctx.config_mut().server.cluster_enabled = true;
    *ctx.cluster_state_mut() = Some(Box::new(ClusterState::new("127.0.0.1", 6379, 16379)));

    let mut conn = conn();
    let reply = clusterscan(&mut conn, &args(&[b"CLUSTERSCAN", b"0-{06S}-0"]), &mut ctx);

    assert_eq!(reply, Value::error("CLUSTERDOWN Hash slot not served"));
}

#[test]
fn clusterscan_returns_clusterdown_for_owned_slot_when_cluster_state_is_fail() {
    let mut ctx = ServerContext::new_with_slot_router(1, 14, |key| {
        crate::engine::distributed::cluster::node::key_hash_slot(key) as usize
    });
    ctx.config_mut().server.cluster_enabled = true;
    let mut state = ClusterState::new("127.0.0.1", 6379, 16379);
    state.add_slot(0).unwrap();
    *ctx.cluster_state_mut() = Some(Box::new(state));
    ctx.db_mut(0)
        .unwrap()
        .set(b"{06S}key".to_vec(), Robj::string("slot0"));

    let mut conn = conn();
    let reply = clusterscan(&mut conn, &args(&[b"CLUSTERSCAN", b"0-{06S}-0"]), &mut ctx);

    assert_eq!(reply, Value::error("CLUSTERDOWN The cluster is down"));
}

#[test]
fn clusterscan_count_errors_match_valkey_shapes() {
    let mut ctx = cluster_context();
    let mut conn = conn();

    let non_integer = clusterscan(
        &mut conn,
        &args(&[b"CLUSTERSCAN", b"0", b"COUNT", b"not-an-integer"]),
        &mut ctx,
    );
    assert_eq!(
        non_integer,
        Value::error("ERR value is not an integer or out of range")
    );

    let zero = clusterscan(
        &mut conn,
        &args(&[b"CLUSTERSCAN", b"0", b"COUNT", b"0"]),
        &mut ctx,
    );
    assert_eq!(zero, CommandError::syntax_error());
}

#[test]
fn clusterscan_single_slot_match_initializes_cursor_without_scanning() {
    let mut ctx = cluster_context();
    ctx.db_mut(0)
        .unwrap()
        .set(b"{5L5}-key".to_vec(), Robj::string("v"));
    let mut conn = conn();

    let reply = clusterscan(
        &mut conn,
        &args(&[b"CLUSTERSCAN", b"0", b"MATCH", b"{5L5}-*"]),
        &mut ctx,
    );

    assert_eq!(
        reply,
        Value::Array(vec![Value::bulk("0-{5L5}-0"), Value::Array(Vec::new()),])
    );
}

#[test]
fn clusterscan_without_slot_returns_continuation_cursor() {
    let mut ctx = cluster_context();
    ctx.db_mut(0)
        .unwrap()
        .set(b"{06S}key".to_vec(), Robj::string("slot0"));
    ctx.db_mut(0)
        .unwrap()
        .set(b"{6ZJ}key".to_vec(), Robj::string("slot16383"));
    let mut conn = conn();

    let reply = clusterscan(
        &mut conn,
        &args(&[b"CLUSTERSCAN", b"0", b"COUNT", b"1"]),
        &mut ctx,
    );

    let Value::Array(items) = reply else {
        panic!("CLUSTERSCAN should return an array");
    };
    assert_ne!(items[0], Value::bulk("0"));
}

#[test]
fn clusterscan_count_one_advances_after_exhausting_slot() {
    let mut ctx = cluster_context();
    ctx.db_mut(0)
        .unwrap()
        .set(b"{4ZG}:key".to_vec(), Robj::string("slot50"));
    ctx.db_mut(0)
        .unwrap()
        .set(b"{0or}:key".to_vec(), Robj::string("slot100"));
    let mut conn = conn();

    let reply = clusterscan(
        &mut conn,
        &args(&[b"CLUSTERSCAN", b"0-{4ZG}-0", b"COUNT", b"1"]),
        &mut ctx,
    );

    let Value::Array(items) = reply else {
        panic!("CLUSTERSCAN should return an array");
    };
    assert_eq!(
        items[1],
        Value::Array(vec![Value::bulk(Bytes::from_static(b"{4ZG}:key"))])
    );
    let Value::Bulk(cursor_bytes) = &items[0] else {
        panic!("CLUSTERSCAN cursor should be a bulk string");
    };
    let cursor = String::from_utf8_lossy(cursor_bytes);
    assert!(
        cursor.contains("-{0or}-"),
        "an exhausted one-key slot must advance to the next populated slot; got '{cursor}'"
    );
    assert!(
        cursor.split('-').next().is_some_and(|fp| fp.len() == 6),
        "an in-range continuation must carry a six-character fingerprint; got '{cursor}'"
    );
}

#[test]
fn clusterscan_count_one_does_not_skip_live_key_after_expired_key() {
    let mut ctx = cluster_context();
    let expired = b"{4ZG}:expired";
    let live = b"{4ZG}:live";
    let db = ctx.db_mut(0).unwrap();
    db.set(expired.to_vec(), Robj::string("expired"));
    db.set(live.to_vec(), Robj::string("live"));
    db.set_expire(
        expired,
        crate::foundation::io::store::kv_engine::now_ms() - 1,
    );
    let mut conn = conn();
    let mut cursor = "0-{4ZG}-0".to_string();
    let mut seen_live = false;

    for _ in 0..32 {
        let cmd_args = CommandArgs::from_parts(vec![
            Bytes::from_static(b"CLUSTERSCAN"),
            Bytes::copy_from_slice(cursor.as_bytes()),
            Bytes::from_static(b"SLOT"),
            Bytes::from_static(b"50"),
            Bytes::from_static(b"COUNT"),
            Bytes::from_static(b"1"),
        ]);
        let Value::Array(items) = clusterscan(&mut conn, &cmd_args, &mut ctx) else {
            panic!("CLUSTERSCAN should return an array");
        };
        let Value::Bulk(next) = &items[0] else {
            panic!("CLUSTERSCAN cursor should be a bulk string");
        };
        let Value::Array(keys) = &items[1] else {
            panic!("CLUSTERSCAN keys should be an array");
        };
        seen_live |= keys
            .iter()
            .any(|key| key == &Value::bulk(Bytes::from_static(live)));
        cursor = String::from_utf8_lossy(next).into_owned();
        if cursor == "0" {
            break;
        }
    }

    assert!(
        seen_live,
        "deleting an expired sample must not skip a live key"
    );
}

#[test]
fn clusterscan_returns_clusterdown_when_local_bitmap_owner_is_fail() {
    use crate::engine::distributed::cluster::node::CLUSTER_NODE_FAIL;

    const CLUSTER_FAIL_STATE: i32 = 1;

    let mut ctx = ServerContext::new_with_slot_router(1, 14, |key| {
        crate::engine::distributed::cluster::node::key_hash_slot(key) as usize
    });
    ctx.config_mut().server.cluster_enabled = true;
    let mut state = ClusterState::new("127.0.0.1", 6379, 16379);
    state.myself.slots[0] = true;
    state.myself.flags |= CLUSTER_NODE_FAIL;
    state.state = CLUSTER_FAIL_STATE;
    *ctx.cluster_state_mut() = Some(Box::new(state));
    ctx.db_mut(0)
        .unwrap()
        .set(b"{06S}key".to_vec(), Robj::string("slot0"));

    let mut conn = conn();
    let reply = clusterscan(&mut conn, &args(&[b"CLUSTERSCAN", b"0-{06S}-0"]), &mut ctx);

    assert_eq!(reply, Value::error("CLUSTERDOWN The cluster is down"));
}

#[test]
fn hashtag_cursor_roundtrip_preserves_encoded_kvstore_cursor() {
    let slot = crate::engine::distributed::cluster::node::key_hash_slot(b"{06S}") as usize;
    for dc in [1u64, 2, 7, 42, 255, 1000, 16384] {
        let encoded = (dc << SLOT_BITS) | slot as u64;
        let cursor_text = format_cursor(encoded);
        let parsed = parse_cursor(cursor_text.as_bytes()).expect(
            "hashtag cursor roundtrip: parsing our own format_cursor output must never fail",
        );
        assert_eq!(parsed.slot, Some(slot));
        assert_eq!(
            parsed.raw, encoded,
            "cursor roundtrip must preserve the complete KVStore cursor"
        );
    }
    let zero = format_cursor(0);
    assert_eq!(zero, "0", "finished hashtag cursor returns plain \"0\"");
}

#[test]
fn hashtag_zero_init_cursor_decodes_to_zero_dict_cursor_for_scan() {
    let slot = crate::engine::distributed::cluster::node::key_hash_slot(b"{Qi}") as usize;
    let parsed = parse_cursor(b"0-{Qi}-0").unwrap();
    assert_eq!(parsed.slot, Some(slot));
    let dc_for_scan = parsed.raw >> SLOT_BITS;
    assert_eq!(
        dc_for_scan, 0,
        "cursor 0-{{tag}}-0 must produce dc=0 for the very first dict scan, \
             not the slot-number ({slot}) accidentally used as dict cursor"
    );

    let stale = parse_cursor(b"0-{Qi}-9393399393").unwrap();
    assert_eq!(
        stale.raw >> SLOT_BITS,
        0,
        "fingerprint 0 must discard a stale local dict cursor"
    );

    let mismatched = parse_cursor(b"ABCDEF-{Qi}-9393399393").unwrap();
    assert_eq!(
        mismatched.raw >> SLOT_BITS,
        0,
        "a nonmatching fingerprint must discard a stale local cursor"
    );
}

#[test]
fn slot_tag_lookup_reuses_cached_value() {
    let first = slot_tag(0);
    let second = slot_tag(0);

    assert!(
        std::ptr::eq(first.as_ptr(), second.as_ptr()),
        "CLUSTERSCAN cursor rendering must not brute-force the same CRC16 tag on every reply"
    );
    for slot in 0..CLUSTER_SLOTS {
        let tag = slot_tag(slot);
        assert_eq!(
            key_hash_slot(tag.as_bytes()) as usize,
            slot,
            "cached tag '{tag}' must route back to slot {slot}"
        );
    }
}

#[test]
fn hashtag_cursor_type_filter_zero_pass_stops_in_sampled_cap() {
    // This is the direct reproduction of the 180 s TYPE-filter TIMEOUT:
    // when TYPE filter yield is 0% (e.g. TYPE zset on string-only db) the
    // handler call must stop after count × 10 sampled keys instead of
    // walking all 16384 slots per call.
    let mut ctx = cluster_context();
    for i in 0..200 {
        ctx.db_mut(0).unwrap().set(
            format!("slotmixed_k{i}").into_bytes(),
            Robj::string(format!("v{i}")),
        );
    }

    // Bypass pure-cursor=0 empty-array shortcut.
    let mut cursor_text = format!("0-{{{}}}-0", slot_tag(0));
    let mut iters = 0usize;
    let mut total_keys = 0usize;
    let mut conn = conn();
    let started = std::time::Instant::now();
    const MAX: usize = 500;
    while cursor_text != "0" {
        iters += 1;
        assert!(
                iters <= MAX,
                "zero-pass TYPE filter did not terminate; cursor='{cursor_text}' after {iters} calls — sampled_cap (count×10) per-call cap is broken"
            );
        let cur_arg = Bytes::copy_from_slice(cursor_text.as_bytes());
        let cmd_args = CommandArgs::from_parts(vec![
            Bytes::from_static(b"CLUSTERSCAN"),
            cur_arg,
            Bytes::from_static(b"COUNT"),
            Bytes::from_static(b"10"),
            Bytes::from_static(b"TYPE"),
            Bytes::from_static(b"zset"),
        ]);
        let reply = clusterscan(&mut conn, &cmd_args, &mut ctx);
        let Value::Array(items) = reply else {
            panic!("expected Array")
        };
        let Value::Bulk(next) = &items[0] else {
            panic!("expected Bulk cursor")
        };
        cursor_text = String::from_utf8_lossy(next).to_string();
        let Value::Array(keys_arr) = &items[1] else {
            panic!("expected keys array")
        };
        total_keys += keys_arr.len();
    }
    assert_eq!(
        total_keys, 0,
        "TYPE zset over all-string db must yield 0 keys; got {total_keys}"
    );
    assert!(
        iters <= 40,
        "range-aware scan should skip empty slot dictionaries inside one storage call and \
             finish 200 keys near 200 / COUNT = 20 handler calls; got {iters}"
    );
    assert!(
        started.elapsed() < std::time::Duration::from_secs(2),
        "range-aware zero-pass TYPE scan took {:?}; per-slot storage calls regress the \
             external Tcl case into its 180 s timeout",
        started.elapsed()
    );
}
