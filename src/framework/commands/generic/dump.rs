use crate::engine::core::{CommandOutcome, Value};
use crate::foundation::io::store::Robj;
use crate::foundation::io::persistence as persistence;
use crate::framework::commands::command_runtime::CommandRuntime;
use crate::framework::{CommandArgs, CommandError};
use command_macros::command;
use std::collections::HashMap;

#[command("DUMP", runtime)]
pub fn dump(runtime: &mut dyn CommandRuntime, args: &CommandArgs) -> CommandOutcome {
    let compression = runtime.rdb_compression();
    let db = match runtime.db_mut() {
        Some(d) => d,
        None => return CommandOutcome::reply(CommandError::db_index_out_of_range()),
    };
    let (obj, _expire, hash_field_expires) = match db.clone_entry(&args.args[0]) {
        Some(entry) => entry,
        None => return CommandOutcome::reply(Value::Null),
    };
    let payload = match serialize_object_with_hash_field_expires_with_compression(
        &obj,
        Some(&hash_field_expires),
        compression,
    ) {
        Some(p) => p,
        None => {
            return CommandOutcome::reply(Value::error(
                "ERR DUMP of this type is not supported yet",
            ))
        }
    };
    CommandOutcome::reply(Value::bulk(payload))
}

pub(crate) fn serialize_object_with_hash_field_expires_with_compression(
    obj: &Robj,
    hash_field_expires: Option<&HashMap<Vec<u8>, u64>>,
    compression: bool,
) -> Option<Vec<u8>> {
    let mut buf = Vec::new();
    buf.push(persistence::type_byte_with_hash_field_expires(
        obj,
        hash_field_expires,
    ));
    let mut writer = crate::foundation::io::persistence::rdb::RdbVecWriter::new(
        &mut buf,
        compression,
    );
    persistence::save_robj_with_hash_field_expires(obj, hash_field_expires, &mut writer);
    buf.extend_from_slice(&persistence::RDB_VERSION.to_le_bytes());
    let crc = persistence::crc64(0, &buf);
    buf.extend_from_slice(&crc.to_le_bytes());
    Some(buf)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::foundation::io::store::structure::stream::{StreamID, ValkeyStream};
    use bytes::Bytes;

    #[test]
    fn stream_dump_payload_round_trips_consumer_groups() {
        let mut stream = ValkeyStream::new();
        stream
            .append(
                StreamID::new(1, 0),
                vec![(Bytes::from_static(b"f"), Bytes::from_static(b"v"))],
                true,
                false,
                0,
            )
            .unwrap();
        stream
            .create_group(Bytes::from_static(b"g"), StreamID::zero(), Some(0))
            .unwrap();
        stream.read_group_new(b"g", b"c", 1, false, 1_000).unwrap();

        let payload = serialize_object_with_hash_field_expires_with_compression(
            &Robj::StreamListpack(Box::new(stream)),
            None,
            true,
        )
        .unwrap();
        let body = &payload[..payload.len() - 10];
        let mut pos = 0;
        let restored = persistence::load_typed_object(body, &mut pos).unwrap();
        let Robj::StreamListpack(restored) = restored else {
            panic!("expected stream");
        };

        let group = restored.group(b"g").unwrap();
        assert_eq!(group.pending.len(), 1);
        assert!(group.consumers.contains_key(b"c" as &[u8]));
        assert_eq!(pos, body.len());
    }
}
