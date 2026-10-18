use super::cluster_slot_stats;
use crate::engine::distributed::cluster::node::{key_hash_slot, ClusterNode, CLUSTER_NODE_MYSELF};
use crate::engine::distributed::cluster::ClusterState;
use crate::foundation::io::protocol::Value;
use crate::foundation::io::store::Robj;
use crate::framework::context::ServerContext;
use bytes::Bytes;

fn stat_map(reply: &Value) -> &[(Value, Value)] {
    let Value::Array(rows) = reply else {
        panic!("slot stats must return an array");
    };
    let Value::Array(row) = &rows[0] else {
        panic!("slot row must be an array");
    };
    let Value::Map(stats) = &row[1] else {
        panic!("slot metrics must be a map");
    };
    stats
}

#[test]
fn slotsrange_counts_keys_across_all_databases() {
    let key = b"{x}key".to_vec();
    let slot = key_hash_slot(&key) as usize;
    let mut ctx = ServerContext::new_with_slot_router(16, 14, |key| key_hash_slot(key) as usize);
    let mut state = ClusterState::new("127.0.0.1", 6379, 16379);
    state.add_slot(slot).unwrap();
    *ctx.cluster_state_mut() = Some(Box::new(state));
    ctx.db_mut(0).unwrap().set(key.clone(), Robj::string("db0"));
    ctx.db_mut(15).unwrap().set(key, Robj::string("db15"));

    let reply = cluster_slot_stats(
        &[
            Bytes::from_static(b"SLOTSRANGE"),
            Bytes::from(slot.to_string()),
            Bytes::from(slot.to_string()),
        ],
        &ctx,
    );

    assert_eq!(
        reply,
        Value::Array(vec![Value::Array(vec![
            Value::integer(slot as i64),
            Value::map(vec![(Value::bulk("key-count"), Value::integer(2),)]),
        ])])
    );
}

#[test]
fn slotsrange_hides_slots_not_owned_by_my_shard() {
    let mut ctx = ServerContext::new_with_slot_router(1, 14, |key| key_hash_slot(key) as usize);
    *ctx.cluster_state_mut() = Some(Box::new(ClusterState::new("127.0.0.1", 6379, 16379)));

    assert_eq!(
        cluster_slot_stats(
            &[
                Bytes::from_static(b"SLOTSRANGE"),
                Bytes::from_static(b"0"),
                Bytes::from_static(b"1"),
            ],
            &ctx,
        ),
        Value::Array(Vec::new())
    );
}

#[test]
fn slotsrange_hides_raw_local_slot_claim_lost_to_higher_epoch_owner() {
    const REMOTE: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    let mut ctx = ServerContext::new_with_slot_router(1, 14, |key| key_hash_slot(key) as usize);
    let mut state = ClusterState::new("127.0.0.1", 6379, 16_379);
    state.add_slot(7).unwrap();
    let mut remote = ClusterNode::new(
        REMOTE.to_string(),
        "127.0.0.1".to_string(),
        6380,
        16_380,
        true,
    );
    remote.flags &= !CLUSTER_NODE_MYSELF;
    remote.config_epoch = 1;
    state.insert_node(remote);
    state.set_slot_node(7, REMOTE).unwrap();
    state.myself.slots[7] = true;
    state.myself.numslots = 1;
    *ctx.cluster_state_mut() = Some(Box::new(state));
    ctx.slot_metrics_mut().record(&[7], 9, 0, 0);

    assert_eq!(
        cluster_slot_stats(
            &[
                Bytes::from_static(b"SLOTSRANGE"),
                Bytes::from_static(b"7"),
                Bytes::from_static(b"7"),
            ],
            &ctx,
        ),
        Value::Array(Vec::new())
    );
}

#[test]
fn slotsrange_reports_advanced_metrics_when_enabled() {
    let key = b"{metric}key".to_vec();
    let slot = key_hash_slot(&key) as usize;
    let mut ctx = ServerContext::new_with_slot_router(1, 14, |key| key_hash_slot(key) as usize);
    ctx.config_mut().server.cluster_slot_stats_enabled = true;
    let mut state = ClusterState::new("127.0.0.1", 6379, 16379);
    state.add_slot(slot).unwrap();
    *ctx.cluster_state_mut() = Some(Box::new(state));
    ctx.db_mut(0).unwrap().set(key, Robj::string("value"));
    ctx.slot_metrics_mut().record(&[slot as u16], 7, 11, 13);

    let reply = cluster_slot_stats(
        &[
            Bytes::from_static(b"SLOTSRANGE"),
            Bytes::from(slot.to_string()),
            Bytes::from(slot.to_string()),
        ],
        &ctx,
    );

    assert_eq!(
        stat_map(&reply),
        &[
            (Value::bulk("key-count"), Value::integer(1)),
            (Value::bulk("cpu-usec"), Value::integer(7)),
            (Value::bulk("network-bytes-in"), Value::integer(11)),
            (Value::bulk("network-bytes-out"), Value::integer(13)),
        ]
    );
}

#[test]
fn orderby_applies_direction_limit_and_slot_tie_break() {
    let mut ctx = ServerContext::new(1);
    ctx.config_mut().server.cluster_slot_stats_enabled = true;
    let mut state = ClusterState::new("127.0.0.1", 6379, 16379);
    for slot in [1usize, 3, 4] {
        state.add_slot(slot).unwrap();
    }
    *ctx.cluster_state_mut() = Some(Box::new(state));
    ctx.slot_metrics_mut().record(&[1, 3], 9, 0, 0);
    ctx.slot_metrics_mut().record(&[4], 4, 0, 0);

    let reply = cluster_slot_stats(
        &[
            Bytes::from_static(b"ORDERBY"),
            Bytes::from_static(b"cpu-usec"),
            Bytes::from_static(b"LIMIT"),
            Bytes::from_static(b"2"),
            Bytes::from_static(b"DESC"),
        ],
        &ctx,
    );

    let Value::Array(rows) = reply else {
        panic!("ORDERBY must return an array");
    };
    assert_eq!(rows.len(), 2);
    let Value::Array(first) = &rows[0] else {
        panic!("slot row must be an array");
    };
    let Value::Array(second) = &rows[1] else {
        panic!("slot row must be an array");
    };
    assert_eq!(first[0], Value::integer(1));
    assert_eq!(second[0], Value::integer(3));
}

#[test]
fn orderby_validates_metric_and_limit() {
    let mut ctx = ServerContext::new(1);
    *ctx.cluster_state_mut() = Some(Box::new(ClusterState::new("127.0.0.1", 6379, 16379)));

    assert!(matches!(
        cluster_slot_stats(
            &[
                Bytes::from_static(b"ORDERBY"),
                Bytes::from_static(b"unknown"),
            ],
            &ctx,
        ),
        Value::Error(_)
    ));
    assert!(matches!(
        cluster_slot_stats(
            &[
                Bytes::from_static(b"ORDERBY"),
                Bytes::from_static(b"key-count"),
                Bytes::from_static(b"LIMIT"),
                Bytes::from_static(b"0"),
            ],
            &ctx,
        ),
        Value::Error(_)
    ));
}
