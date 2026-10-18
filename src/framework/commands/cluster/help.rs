use crate::foundation::io::protocol::Value;
use bytes::Bytes;

pub(super) fn cluster_help() -> Value {
    Value::Array(vec![
        Value::bulk(Bytes::from_static(
            b"CLUSTER <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
        )),
        Value::bulk(Bytes::from_static(b"ADDSLOTS <slot> [<slot> ...]")),
        Value::bulk(Bytes::from_static(
            b"ADDSLOTSRANGE <start> <end> [<start> <end> ...]",
        )),
        Value::bulk(Bytes::from_static(b"BUMPEPOCH")),
        Value::bulk(Bytes::from_static(b"COUNT-FAILURE-REPORTS <node-id>")),
        Value::bulk(Bytes::from_static(b"COUNTKEYSINSLOT <slot>")),
        Value::bulk(Bytes::from_static(b"DELSLOTS <slot> [<slot> ...]")),
        Value::bulk(Bytes::from_static(
            b"DELSLOTSRANGE <start> <end> [<start> <end> ...]",
        )),
        Value::bulk(Bytes::from_static(b"FAILOVER [FORCE|TAKEOVER]")),
        Value::bulk(Bytes::from_static(b"FLUSHSLOT <slot> [<slot> ...]")),
        Value::bulk(Bytes::from_static(b"FLUSHSLOTS")),
        Value::bulk(Bytes::from_static(b"FORGET <node-id>")),
        Value::bulk(Bytes::from_static(b"GETKEYSINSLOT <slot> <count>")),
        Value::bulk(Bytes::from_static(b"INFO")),
        Value::bulk(Bytes::from_static(b"KEYSLOT <key>")),
        Value::bulk(Bytes::from_static(b"LINKS")),
        Value::bulk(Bytes::from_static(b"MEET <ip> <port> [<cport>]")),
        Value::bulk(Bytes::from_static(b"MYID")),
        Value::bulk(Bytes::from_static(b"MYSHARDID")),
        Value::bulk(Bytes::from_static(b"NODES")),
        Value::bulk(Bytes::from_static(b"REPLICAS <node-id>")),
        Value::bulk(Bytes::from_static(b"REPLICATE <node-id>")),
        Value::bulk(Bytes::from_static(b"RESET [HARD|SOFT]")),
        Value::bulk(Bytes::from_static(b"SAVECONFIG")),
        Value::bulk(Bytes::from_static(b"SET-CONFIG-EPOCH <epoch>")),
        Value::bulk(Bytes::from_static(
            b"SETSLOT <slot> (IMPORTING|MIGRATING|STABLE|NODE) [node-id]",
        )),
        Value::bulk(Bytes::from_static(b"SHARDS")),
        Value::bulk(Bytes::from_static(b"SLAVES <node-id>")),
        Value::bulk(Bytes::from_static(b"SLOTS")),
        Value::bulk(Bytes::from_static(b"SYNCSLOTS <slot> [<slot> ...]")),
    ])
}
