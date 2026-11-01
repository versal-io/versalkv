use crate::framework::{CommandArgs, CommandError};
use crate::framework::commands::stream::xread::{entry_to_value, parse_stream_id};
use crate::framework::context::ServerContext;
use crate::engine::command::context::{BlockedCmd, CommandConn};
use crate::engine::distributed::replication::protocol::encode_command;
use crate::foundation::io::protocol::Value;
use crate::foundation::io::sys::time::mstime;
use crate::framework::observability::keyspace;
use bytes::Bytes;
use command_macros::command;

#[command("XREADGROUP", get_keys = crate::framework::commands::server::command::getkeys_helpers::xread_get_keys)]
pub fn xreadgroup(
    conn: &mut dyn CommandConn,
    args: &CommandArgs,
    ctx: &mut ServerContext,
) -> Value {
    let mut i = 0;
    let mut group_name: Option<&[u8]> = None;
    let mut consumer_name: Option<&[u8]> = None;
    let mut count: Option<usize> = None;
    let mut block_ms: Option<u64> = None;
    let mut noack = false;

    while i < args.args.len() {
        let arg_upper = args.args[i].to_ascii_uppercase();
        match arg_upper.as_slice() {
            b"GROUP" => {
                if i + 2 >= args.args.len() {
                    return CommandError::syntax_error();
                }
                group_name = Some(&args.args[i + 1]);
                consumer_name = Some(&args.args[i + 2]);
                i += 3;
            }
            b"COUNT" => {
                if i + 1 >= args.args.len() {
                    return CommandError::syntax_error();
                }
                i += 1;
                let c: i64 = match args.parse(i) {
                    Ok(c) => c,
                    Err(_) => return Value::error("ERR value is not an integer or out of range"),
                };
                if c <= 0 {
                    return Value::error("ERR value is not an integer or out of range");
                }
                count = Some(c as usize);
                i += 1;
            }
            b"BLOCK" => {
                if i + 1 >= args.args.len() {
                    return CommandError::syntax_error();
                }
                i += 1;
                let ms: i64 = match args.parse(i) {
                    Ok(ms) => ms,
                    Err(_) => return Value::error("ERR timeout is not a float or out of range"),
                };
                if ms < 0 {
                    return Value::error("ERR timeout is not a float or out of range");
                }
                block_ms = Some(ms as u64);
                i += 1;
            }
            b"NOACK" => {
                noack = true;
                i += 1;
            }
            b"STREAMS" => {
                i += 1;
                break;
            }
            _ => {
                return CommandError::syntax_error();
            }
        }
    }

    if group_name.is_none() {
        return Value::error("ERR The GROUP option is mandatory for XREADGROUP");
    }

    if i >= args.args.len() {
        return CommandError::syntax_error();
    }

    let remaining = args.args.len() - i;
    if remaining == 0 || !remaining.is_multiple_of(2) {
        return Value::error(
            "ERR Unbalanced 'xreadgroup' list of streams: for each stream key an ID or '>' must be specified.",
        );
    }

    let num_keys = remaining / 2;
    let first_key_index = i;
    let keys = &args.args[first_key_index..first_key_index + num_keys];
    let id_args = &args.args[i + num_keys..];

    let db_idx = conn.selected_db();
    let db = match ctx.db_mut(db_idx) {
        Some(d) => d,
        None => return CommandError::db_index_out_of_range(),
    };

    // Validate every stream and ID before creating consumers or delivering entries.
    for (key, id_arg) in keys.iter().zip(id_args) {
        if id_arg.as_ref() != b">" {
            if let Err(error) = parse_stream_id(id_arg, false) {
                return error;
            }
        }
        match db.get_stream(key) {
            Ok(Some(stream)) if stream.group(group_name.unwrap()).is_some() => {}
            Ok(_) => return Value::error("NOGROUP No such key or consumer group"),
            Err(_) => return CommandError::wrong_type(),
        }
    }

    let mut streams_result = Vec::with_capacity(num_keys);
    let mut has_new_message_request = false;
    let mut propagate = Vec::new();
    let mut created_consumers = Vec::new();

    for idx in 0..num_keys {
        let key = &keys[idx];
        let id_arg = &id_args[idx];
        let is_new_messages = id_arg.as_ref() == b">";
        has_new_message_request |= is_new_messages;

        let stream = match db.get_stream_mut(key) {
            Ok(Some(stream)) => stream,
            Ok(None) => return Value::error("NOGROUP No such key or consumer group"),
            Err(_) => return CommandError::wrong_type(),
        };

        let entries = if is_new_messages {
            match stream.read_group_new(
                group_name.unwrap(),
                consumer_name.unwrap(),
                count.unwrap_or(usize::MAX),
                noack,
                mstime(),
            ) {
                Ok(result) => {
                    if result.consumer_created || !result.entries.is_empty() {
                        conn.record_dirty_key((db_idx, key.clone()));
                        conn.record_dirty_delta(
                            i64::from(result.consumer_created) + i64::from(!result.entries.is_empty()),
                        );
                    }
                    if result.consumer_created {
                        created_consumers.push(key.clone());
                    }
                    if !result.entries.is_empty() {
                        append_xreadgroup_propagation(
                            &mut propagate,
                            key,
                            group_name.unwrap(),
                            consumer_name.unwrap(),
                            &result
                                .entries
                                .iter()
                                .map(|entry| entry.id)
                                .collect::<Vec<_>>(),
                            result.last_id,
                            result.entries_read,
                            noack,
                            result.consumer_created,
                        );
                    } else if result.consumer_created && noack {
                        propagate.push(encode_command(&[
                            b"XGROUP" as &[u8],
                            b"CREATECONSUMER",
                            key.as_ref(),
                            group_name.unwrap(),
                            consumer_name.unwrap(),
                        ]));
                    }
                    result
                        .entries
                        .into_iter()
                        .map(|entry| (entry.id, Some(entry.fields)))
                        .collect::<Vec<_>>()
                }
                Err(_) => return Value::error("NOGROUP No such key or consumer group"),
            }
        } else {
            let start_id = match parse_stream_id(id_arg, false) {
                Ok(id) => id,
                Err(e) => return e,
            };
            match stream.read_group_history(
                group_name.unwrap(),
                consumer_name.unwrap(),
                start_id,
                count.unwrap_or(usize::MAX),
                mstime(),
            ) {
                Ok(result) => {
                    if result.consumer_created || !result.entries.is_empty() {
                        conn.record_dirty_key((db_idx, key.clone()));
                        conn.record_dirty_delta(
                            i64::from(result.consumer_created) + i64::from(!result.entries.is_empty()),
                        );
                    }
                    if result.consumer_created {
                        created_consumers.push(key.clone());
                        if noack {
                            propagate.push(encode_command(&[
                                b"XGROUP" as &[u8],
                                b"CREATECONSUMER",
                                key.as_ref(),
                                group_name.unwrap(),
                                consumer_name.unwrap(),
                            ]));
                        }
                    }
                    result.entries
                }
                Err(_) => return Value::error("NOGROUP No such key or consumer group"),
            }
        };

        if entries.is_empty() && is_new_messages {
            continue;
        }

        let mut entry_arr = Vec::with_capacity(entries.len());
        for (id, fields) in &entries {
            let fields = fields.clone().unwrap_or_default();
            entry_arr.push(entry_to_value(id, &fields));
        }
        let key_entry = vec![Value::bulk(key.clone()), Value::Array(entry_arr)];
        streams_result.push(Value::Array(key_entry));
    }

    for key in created_consumers {
        keyspace::notify_keyspace_event(
            conn,
            ctx,
            keyspace::NOTIFY_STREAM,
            "xgroup-createconsumer",
            db_idx,
            &key,
        );
    }

    conn.set_skip_propagate(true);
    if !propagate.is_empty() {
        conn.extend_custom_propagation(propagate);
    }

    if streams_result.is_empty() {
        if let Some(timeout_ms) = block_ms {
            if has_new_message_request && !ctx.has_running_script() {
                return conn.enter_blocking(
                    BlockedCmd::Xreadgroup,
                    keys.to_vec(),
                    timeout_ms,
                    &freeze_xreadgroup_args(args, first_key_index + num_keys),
                    conn.req_bytes_curr_cmd(),
                );
            }
        }
        return Value::null_array();
    }

    Value::Array(streams_result)
}

fn append_xreadgroup_propagation(
    propagate: &mut Vec<Vec<u8>>,
    key: &[u8],
    group: &[u8],
    consumer: &[u8],
    ids: &[crate::foundation::io::store::structure::stream::StreamID],
    last_id: crate::foundation::io::store::structure::stream::StreamID,
    entries_read: Option<u64>,
    noack: bool,
    _consumer_created: bool,
) {
    if noack {
        propagate.push(encode_command(&[
            b"XGROUP" as &[u8],
            b"CREATECONSUMER",
            key,
            group,
            consumer,
        ]));
    } else {
        let last_id_arg = last_id.fmt_id();
        for id in ids {
            let id_arg = id.fmt_id();
            propagate.push(encode_command(&[
                b"XCLAIM" as &[u8],
                key,
                group,
                consumer,
                b"0",
                id_arg.as_bytes(),
                b"TIME",
                b"0",
                b"RETRYCOUNT",
                b"1",
                b"FORCE",
                b"JUSTID",
                b"LASTID",
                last_id_arg.as_bytes(),
            ]));
        }
    }
    let entries_read_arg = entries_read.unwrap_or(0).to_string();
    let last_id_arg = last_id.fmt_id();
    propagate.push(encode_command(&[
        b"XGROUP" as &[u8],
        b"SETID",
        key,
        group,
        last_id_arg.as_bytes(),
        b"ENTRIESREAD",
        entries_read_arg.as_bytes(),
    ]));
}

fn freeze_xreadgroup_args(args: &CommandArgs, first_id_index: usize) -> CommandArgs {
    let mut frozen = args.clone();
    for idx in first_id_index..frozen.args.len() {
        if frozen.args[idx].as_ref() == b"$" {
            frozen.args[idx] = Bytes::from_static(b">");
        }
    }
    frozen
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::foundation::io::store::structure::stream::StreamID;
    use crate::framework::capabilities::connection::Connection;

    fn args(parts: &[&[u8]]) -> CommandArgs {
        CommandArgs::from_parts(parts.iter().map(|part| Bytes::copy_from_slice(part)).collect())
    }

    fn setup() -> (Connection, ServerContext) {
        let mut conn = Connection::new_fake();
        conn.selected_db = 1;
        let mut ctx = ServerContext::new(2);
        let stream = ctx.db_mut(1).unwrap().get_or_create_stream(b"s").unwrap();
        stream.create_group(Bytes::from_static(b"g"), StreamID::zero(), None).unwrap();
        (conn, ctx)
    }

    #[test]
    fn group_reads_record_creation_delivery_and_history_effects() {
        for noack in [false, true] {
            let (mut conn, mut ctx) = setup();
            let stream = ctx.db_mut(1).unwrap().get_stream_mut(b"s").unwrap().unwrap();
            stream.append(StreamID::new(1, 0),
                vec![(Bytes::from_static(b"f"), Bytes::from_static(b"v"))], true, false, 0).unwrap();
            stream.create_consumer(b"g", b"c", 0).unwrap();
            let mut parts = vec![b"XREADGROUP".as_slice(), b"GROUP", b"g", b"c"];
            if noack {
                parts.push(b"NOACK");
            }
            parts.extend([b"STREAMS".as_slice(), b"s", b">"]);
            let reply = xreadgroup(&mut conn, &args(&parts), &mut ctx);
            assert!(matches!(reply, Value::Array(_)));
            assert_eq!(conn.dirty_keys(), &[(1, Bytes::from_static(b"s"))]);
            assert!(!conn.custom_propagation().is_empty());
            let stream = ctx.db_mut(1).unwrap().get_stream(b"s").unwrap().unwrap();
            let group = stream.group(b"g").unwrap();
            assert_eq!(group.last_id, StreamID::new(1, 0));
            assert_eq!(group.pending.len(), usize::from(!noack));

            conn.replace_dirty_keys(Vec::new());
            conn.clear_custom_propagation();
            let reply = xreadgroup(&mut conn,
                &args(&[b"XREADGROUP", b"GROUP", b"g", b"c", b"STREAMS", b"s", b"0"]), &mut ctx);
            assert!(matches!(reply, Value::Array(_)));
            if noack {
                assert!(conn.dirty_keys().is_empty());
            } else {
                assert_eq!(conn.dirty_keys(), &[(1, Bytes::from_static(b"s"))]);
                assert_eq!(ctx.db_mut(1).unwrap().get_stream(b"s").unwrap().unwrap()
                    .group(b"g").unwrap().pending[&StreamID::new(1, 0)].delivery_count, 2);
            }
        }
    }

    #[test]
    fn empty_group_reads_dirty_only_new_consumers_and_preserve_noack_propagation() {
        for is_blocking in [false, true] {
            for noack in [false, true] {
                let (mut conn, mut ctx) = setup();
                let mut parts = vec![b"XREADGROUP".as_slice(), b"GROUP", b"g", b"c"];
                if is_blocking {
                    parts.extend([b"BLOCK".as_slice(), b"0"]);
                }
                if noack {
                    parts.push(b"NOACK");
                }
                parts.extend([b"STREAMS".as_slice(), b"s", b">"]);
                xreadgroup(&mut conn, &args(&parts), &mut ctx);
                assert_eq!(conn.dirty_keys(), &[(1, Bytes::from_static(b"s"))]);
                assert_eq!(conn.blocked.is_some(), is_blocking);
                if noack {
                    assert_eq!(conn.custom_propagation(), &[encode_command(&[
                        b"XGROUP", b"CREATECONSUMER", b"s", b"g", b"c",
                    ])]);
                } else {
                    assert!(conn.custom_propagation().is_empty());
                }
                conn.blocked = None;
                conn.replace_dirty_keys(Vec::new());
                conn.clear_custom_propagation();
                xreadgroup(&mut conn, &args(&parts), &mut ctx);
                assert!(conn.dirty_keys().is_empty());
                assert!(conn.custom_propagation().is_empty());
            }
        }
    }

    #[test]
    fn invalid_multistream_reads_do_not_create_consumers_or_record_effects() {
        for (second_key, second_id) in [(b"missing".as_slice(), b">".as_slice()), (b"s", b"invalid")] {
            let (mut conn, mut ctx) = setup();
            let reply = xreadgroup(&mut conn, &args(&[
                b"XREADGROUP", b"GROUP", b"g", b"c", b"STREAMS", b"s", second_key, b">", second_id,
            ]), &mut ctx);
            assert!(matches!(reply, Value::Error(_)));
            assert!(conn.dirty_keys().is_empty());
            assert!(conn.custom_propagation().is_empty());
            assert!(ctx.db_mut(1).unwrap().get_stream(b"s").unwrap().unwrap()
                .group(b"g").unwrap().consumers.is_empty());
        }
    }

    #[test]
    fn noack_propagation_creates_consumer_without_pending_claim() {
        let mut frames = Vec::new();

        append_xreadgroup_propagation(
            &mut frames,
            b"mystream",
            b"mygroup",
            b"Bob",
            &[StreamID::new(2, 0)],
            StreamID::new(2, 0),
            Some(2),
            true,
            false,
        );

        assert_eq!(frames.len(), 2);
        let decoded = frames
            .iter()
            .map(|frame| String::from_utf8_lossy(frame).into_owned())
            .collect::<Vec<_>>()
            .join("\n");
        assert!(decoded.contains("CREATECONSUMER"), "{decoded}");
        assert!(decoded.contains("SETID"), "{decoded}");
        assert!(!decoded.contains("XCLAIM"), "{decoded}");
    }
}
