use super::{EmbeddedEvent, EmbeddedOutput, EmbeddedRuntime, EmbeddedRuntimeError, Reply};
use crate::engine::command::CommandArgs;
use crate::runtime::driver::{
    RuntimeClock, RuntimeDispatch, RuntimeInput, RuntimeInstant, RuntimeWait,
};
use bytes::Bytes;

fn args(parts: &[&str]) -> Vec<String> {
    parts.iter().map(|part| (*part).to_owned()).collect()
}

#[test]
fn shared_handlers_mutate_the_shared_db() {
    let mut runtime = EmbeddedRuntime::new();
    assert_eq!(
        runtime.capabilities(),
        crate::runtime::driver::RuntimeCapabilities::embedded()
    );
    assert_eq!(
        runtime.execute(&args(&["PING"]), 100),
        Reply::Simple("PONG".into())
    );
    assert_eq!(
        runtime.execute(&args(&["ECHO", "hello"]), 100),
        Reply::Bulk(Some("hello".into()))
    );
    assert_eq!(
        runtime.execute(&args(&["SET", "key", "1"]), 100),
        Reply::Simple("OK".into())
    );
    assert_eq!(
        runtime.execute(&args(&["INCR", "key"]), 100),
        Reply::Integer(2)
    );
    assert_eq!(
        runtime.execute(&args(&["GET", "key"]), 100),
        Reply::Bulk(Some("2".into()))
    );
}

#[test]
fn dispatch_returns_correlated_reply_and_deadline() {
    let mut runtime = EmbeddedRuntime::new();
    let command = CommandArgs::from_parts(vec![
        Bytes::from_static(b"SET"),
        Bytes::from_static(b"key"),
        Bytes::from_static(b"value"),
        Bytes::from_static(b"PX"),
        Bytes::from_static(b"5"),
    ]);
    let step = runtime
        .dispatch(RuntimeDispatch::new(
            RuntimeClock::new(1_000, 10_000),
            RuntimeInput::External(EmbeddedEvent::Execute {
                request_id: 17,
                session: crate::engine::command::SessionId(1),
                args: command,
            }),
        ))
        .unwrap();
    let EmbeddedOutput::Reply { request_id, value } = &step.outputs[0];
    assert_eq!(*request_id, 17);
    assert_eq!(*value, crate::engine::core::Value::ok());
    assert_eq!(
        step.wait,
        RuntimeWait::Event {
            deadline: Some(RuntimeInstant::from_millis(1_005))
        }
    );
}

#[test]
fn idle_runtime_waits_without_a_deadline() {
    let mut runtime = EmbeddedRuntime::new();
    let step = runtime
        .dispatch(RuntimeDispatch::new(
            RuntimeClock::new(1_000, 10_000),
            RuntimeInput::Wake,
        ))
        .unwrap();

    assert!(step.outputs.is_empty());
    assert_eq!(step.wait, RuntimeWait::Event { deadline: None });
}

#[test]
fn embedded_rejects_unowned_host_completion() {
    let mut runtime = EmbeddedRuntime::new();
    let result = runtime.dispatch(RuntimeDispatch::new(
        RuntimeClock::new(1_000, 10_000),
        RuntimeInput::HostCompletion(crate::runtime::driver::OwnedHostCompletion {
            operation: crate::runtime::driver::HostOperationId::from_u64(1),
            result: crate::runtime::driver::HostOperationResult::Ack,
        }),
    ));

    assert!(matches!(
        result,
        Err(crate::runtime::driver::RuntimeDriverError::Machine(
            EmbeddedRuntimeError::UnsupportedHostCompletion
        ))
    ));
}

#[test]
fn host_time_controls_expiry() {
    let mut runtime = EmbeddedRuntime::new();
    assert_eq!(
        runtime.execute(&args(&["SET", "key", "value", "PX", "5"]), 100),
        Reply::Simple("OK".into())
    );
    assert_eq!(
        runtime.execute(&args(&["PTTL", "key"]), 102),
        Reply::Integer(3)
    );
    runtime
        .dispatch(RuntimeDispatch::new(
            RuntimeClock::new(105, 105),
            RuntimeInput::Wake,
        ))
        .unwrap();
    assert_eq!(
        runtime.execute(&args(&["GET", "key"]), 105),
        Reply::Bulk(None)
    );
}

#[test]
fn one_deadline_wake_expires_all_due_keys() {
    let mut runtime = EmbeddedRuntime::new();
    assert_eq!(
        runtime.execute(&args(&["SET", "first", "1", "PX", "5"]), 100),
        Reply::Simple("OK".into())
    );
    assert_eq!(
        runtime.execute(&args(&["SET", "second", "2", "PX", "5"]), 100),
        Reply::Simple("OK".into())
    );

    runtime
        .dispatch(RuntimeDispatch::new(
            RuntimeClock::new(105, 105),
            RuntimeInput::Wake,
        ))
        .unwrap();

    assert_eq!(
        runtime.execute(&args(&["GET", "first"]), 105),
        Reply::Bulk(None)
    );
    assert_eq!(
        runtime.execute(&args(&["GET", "second"]), 105),
        Reply::Bulk(None)
    );
}

#[test]
fn collection_handlers_share_one_database() {
    let mut runtime = EmbeddedRuntime::new();
    assert_eq!(
        runtime.execute(&args(&["RPUSH", "list", "a", "b"]), 0),
        Reply::Integer(2)
    );
    assert_eq!(
        runtime.execute(&args(&["LRANGE", "list", "0", "-1"]), 0),
        Reply::Array(vec![Some("a".into()), Some("b".into())])
    );
    assert_eq!(
        runtime.execute(&args(&["SADD", "set", "b", "a"]), 0),
        Reply::Integer(2)
    );
    assert_eq!(
        runtime.execute(&args(&["SMEMBERS", "set"]), 0),
        Reply::Array(vec![Some("a".into()), Some("b".into())])
    );
    assert_eq!(
        runtime.execute(&args(&["HSET", "hash", "field", "value"]), 0),
        Reply::Integer(1)
    );
    assert_eq!(
        runtime.execute(&args(&["HGET", "hash", "field"]), 0),
        Reply::Bulk(Some("value".into()))
    );
    assert_eq!(
        runtime.execute(&args(&["ZADD", "zset", "2", "b", "1", "a"]), 0),
        Reply::Integer(2)
    );
    assert_eq!(
        runtime.execute(&args(&["ZRANGE", "zset", "0", "-1"]), 0),
        Reply::Array(vec![Some("a".into()), Some("b".into())])
    );
}

#[test]
fn conditional_and_numeric_string_handlers_are_shared() {
    let mut runtime = EmbeddedRuntime::new();
    assert_eq!(
        runtime.execute(&args(&["MSETNX", "a", "1", "b", "2"]), 100),
        Reply::Integer(1)
    );
    assert_eq!(
        runtime.execute(&args(&["MSETNX", "a", "3", "c", "4"]), 100),
        Reply::Integer(0)
    );
    assert_eq!(
        runtime.execute(&args(&["INCRBYFLOAT", "a", "0.5"]), 100),
        Reply::Bulk(Some("1.5".into()))
    );
    assert_eq!(
        runtime.execute(&args(&["MSETEX", "1", "short", "value", "PX", "5"]), 100),
        Reply::Integer(1)
    );
    assert_eq!(
        runtime.execute(&args(&["PTTL", "short"]), 102),
        Reply::Integer(3)
    );
}

#[test]
fn extended_zset_handlers_use_the_shared_runtime() {
    let mut runtime = EmbeddedRuntime::new();
    assert_eq!(
        runtime.execute(&args(&["ZADD", "left", "1", "a", "2", "b"]), 0),
        Reply::Integer(2)
    );
    assert_eq!(
        runtime.execute(&args(&["ZADD", "right", "3", "b", "4", "c"]), 0),
        Reply::Integer(2)
    );
    assert_eq!(
        runtime.execute(&args(&["ZINTER", "2", "left", "right"]), 0),
        Reply::Array(vec![Some("b".into())])
    );
    assert_eq!(
        runtime.execute(&args(&["ZUNIONSTORE", "union", "2", "left", "right"]), 0,),
        Reply::Integer(3)
    );
    assert_eq!(
        runtime.execute(&args(&["ZRANGEBYSCORE", "union", "-inf", "+inf"]), 0),
        Reply::Array(vec![Some("a".into()), Some("c".into()), Some("b".into())])
    );
    assert_eq!(
        runtime.execute(&args(&["ZPOPMIN", "union"]), 0),
        Reply::Array(vec![Some("a".into()), Some("1".into())])
    );
}

#[test]
fn command_count_list_and_help_use_the_compiled_catalog() {
    let mut runtime = EmbeddedRuntime::new();
    let Reply::Integer(count) = runtime.execute(&args(&["COMMAND", "COUNT"]), 0) else {
        panic!("expected COMMAND COUNT integer");
    };
    assert_eq!(count, 205);
    let Reply::Array(mut pathhash_names) = runtime.execute(
        &args(&["COMMAND", "LIST", "FILTERBY", "ACLCAT", "PATHHASH"]),
        0,
    ) else {
        panic!("expected PathHash command list");
    };
    pathhash_names.sort();
    assert_eq!(
        pathhash_names,
        [
            "phcard",
            "phdel",
            "phdelprefix",
            "phexists",
            "phget",
            "phgetall",
            "phlongest",
            "phmget",
            "phmset",
            "phprefixes",
            "phscan",
            "phset"
        ]
        .into_iter()
        .map(|name| Some(name.into()))
        .collect::<Vec<_>>()
    );

    let Reply::Array(names) =
        runtime.execute(&args(&["COMMAND", "LIST", "FILTERBY", "PATTERN", "m*"]), 0)
    else {
        panic!("expected COMMAND LIST flat array");
    };
    assert!(names.contains(&Some("mget".into())));
    assert!(names.contains(&Some("msetex".into())));
    assert!(names
        .iter()
        .all(|name| name.as_deref().is_some_and(|name| name.starts_with('m'))));

    assert!(matches!(
        runtime.execute(&args(&["COMMAND", "HELP"]), 0),
        Reply::Bulk(Some(help)) if help.starts_with("COMMAND <subcommand>")
    ));

    let Reply::Array(command_names) = runtime.execute(
        &args(&["COMMAND", "LIST", "FILTERBY", "PATTERN", "command*"]),
        0,
    ) else {
        panic!("expected COMMAND subcommand list");
    };
    assert!(command_names.contains(&Some("command".into())));
    assert!(command_names.contains(&Some("command|count".into())));
    assert!(command_names.contains(&Some("command|getkeysandflags".into())));

    assert_eq!(
        runtime.execute(
            &args(&["COMMAND", "LIST", "FILTERBY", "MODULE", "anything"]),
            0,
        ),
        Reply::Array(Vec::new())
    );
    let Reply::Array(read_commands) =
        runtime.execute(&args(&["COMMAND", "LIST", "FILTERBY", "ACLCAT", "READ"]), 0)
    else {
        panic!("expected ACL category command list");
    };
    assert!(read_commands.contains(&Some("get".into())));
    assert!(!read_commands.contains(&Some("set".into())));
}

#[test]
fn command_getkeys_uses_portable_dynamic_extractors() {
    let mut runtime = EmbeddedRuntime::new();
    assert_eq!(
        runtime.execute(
            &args(&["COMMAND", "GETKEYS", "MSET", "a", "1", "b", "2"]),
            0,
        ),
        Reply::Array(vec![Some("a".into()), Some("b".into())])
    );

    let reply = runtime.execute(
        &args(&[
            "COMMAND",
            "GETKEYSANDFLAGS",
            "ZINTERSTORE",
            "dest",
            "2",
            "left",
            "right",
        ]),
        0,
    );
    assert_eq!(
        reply,
        Reply::Replies(vec![
            Reply::Replies(vec![
                Reply::Bulk(Some("dest".into())),
                Reply::Array(vec![Some("OW".into()), Some("update".into())]),
            ]),
            Reply::Replies(vec![
                Reply::Bulk(Some("left".into())),
                Reply::Array(vec![Some("RO".into()), Some("access".into())]),
            ]),
            Reply::Replies(vec![
                Reply::Bulk(Some("right".into())),
                Reply::Array(vec![Some("RO".into()), Some("access".into())]),
            ]),
        ])
    );
    assert_eq!(
        runtime.execute(&args(&["COMMAND", "GETKEYS", "PING"]), 0),
        Reply::Error("ERR The command has no key arguments".into())
    );
}

#[test]
fn command_info_and_docs_preserve_recursive_reply_shapes() {
    let mut runtime = EmbeddedRuntime::new();
    let Reply::Replies(info) = runtime.execute(&args(&["COMMAND", "INFO", "GET"]), 0) else {
        panic!("expected nested COMMAND INFO reply");
    };
    let Reply::Replies(fields) = &info[0] else {
        panic!("expected COMMAND INFO entry");
    };
    assert_eq!(fields[0], Reply::Bulk(Some("get".into())));
    assert_eq!(fields[1], Reply::Integer(2));
    assert!(matches!(&fields[8], Reply::Replies(specs) if !specs.is_empty()));

    let Reply::Replies(subcommand_info) = runtime.execute(
        &args(&["COMMAND", "INFO", "COMMAND|COUNT", "DOES-NOT-EXIST"]),
        0,
    ) else {
        panic!("expected COMMAND subcommand info");
    };
    assert!(
        matches!(&subcommand_info[0], Reply::Replies(fields) if fields[0] == Reply::Bulk(Some("command|count".into())))
    );
    assert_eq!(subcommand_info[1], Reply::Bulk(None));

    let Reply::Map(docs) = runtime.execute(&args(&["COMMAND", "DOCS", "GET"]), 0) else {
        panic!("expected COMMAND DOCS map");
    };
    assert_eq!(docs[0].0, Reply::Bulk(Some("GET".into())));
    assert!(matches!(&docs[0].1, Reply::Map(_)));
    assert_eq!(
        runtime.execute(&args(&["COMMAND", "DOCS", "DOES-NOT-EXIST"]), 0),
        Reply::Bulk(None)
    );

    let expected_count = command_count(&mut runtime);
    assert!(matches!(
        runtime.execute(&args(&["COMMAND"]), 0),
        Reply::Replies(entries) if entries.len() == expected_count
    ));
}

#[cfg(not(feature = "wasm"))]
#[test]
fn command_introspection_matches_the_server_catalog_for_portable_queries() {
    use crate::framework::capabilities::connection::Connection;
    use crate::framework::context::ServerContext;
    use crate::framework::{CommandRegistry, CommandSpec};
    use bytes::Bytes;

    fn server_reply(parts: &[&str]) -> Reply {
        let mut registry = CommandRegistry::new();
        crate::framework::commands::register_all(&mut registry);
        let mut context = ServerContext::new(16);
        context.set_command_registry_snapshot(&registry);
        let mut connection = Connection::new_fake();
        let args = crate::engine::command::CommandArgs::from_parts(
            parts
                .iter()
                .map(|part| Bytes::copy_from_slice(part.as_bytes()))
                .collect(),
        );
        let spec: CommandSpec = crate::framework::commands::server::command::COMMAND_SPEC;
        Reply::from_value(spec.execute(&mut connection, &args, &mut context))
    }

    let mut runtime = EmbeddedRuntime::new();
    for query in [
        &["COMMAND", "INFO", "GET"][..],
        &["COMMAND", "DOCS", "GET"][..],
        &["COMMAND", "LIST", "FILTERBY", "PATTERN", "get"][..],
        &["COMMAND", "GETKEYS", "MSET", "a", "1", "b", "2"][..],
        &[
            "COMMAND",
            "GETKEYSANDFLAGS",
            "ZINTERSTORE",
            "dest",
            "2",
            "left",
            "right",
        ][..],
    ] {
        assert_eq!(runtime.execute(&args(query), 0), server_reply(query));
    }
}

fn command_count(runtime: &mut EmbeddedRuntime) -> usize {
    match runtime.execute(&args(&["COMMAND", "COUNT"]), 0) {
        Reply::Integer(count) => count as usize,
        _ => panic!("expected COMMAND COUNT integer"),
    }
}

#[test]
fn scan_flush_and_unknown_commands_use_shared_dispatch() {
    let mut runtime = EmbeddedRuntime::new();
    assert_eq!(
        runtime.execute(&args(&["SET", "greeting", "hello"]), 0),
        Reply::Simple("OK".into())
    );
    assert_eq!(
        runtime.execute(&args(&["SCAN", "0", "MATCH", "greet*", "COUNT", "10"]), 0),
        Reply::Replies(vec![
            Reply::Bulk(Some("0".into())),
            Reply::Array(vec![Some("greeting".into())])
        ])
    );
    assert_eq!(
        runtime.execute(&args(&["FLUSHDB"]), 0),
        Reply::Simple("OK".into())
    );
    assert_eq!(runtime.execute(&args(&["DBSIZE"]), 0), Reply::Integer(0));
    assert_eq!(
        runtime.execute(&args(&["BGSAVE"]), 0),
        Reply::Error("ERR command requires a runtime capability unavailable in this host".into())
    );
    assert_eq!(
        runtime.execute(&args(&["BLPOP", "key", "0"]), 0),
        Reply::Error("ERR command requires a blocking capability".into())
    );
}

#[test]
fn expanded_data_domains_execute_in_embedded_runtime() {
    let mut runtime = EmbeddedRuntime::new();
    assert_eq!(
        runtime.execute(&args(&["APPEND", "bits", "A"]), 100),
        Reply::Integer(1)
    );
    assert_eq!(
        runtime.execute(&args(&["BITCOUNT", "bits"]), 100),
        Reply::Integer(2)
    );
    assert_eq!(
        runtime.execute(&args(&["SETBIT", "bits", "0", "1"]), 100),
        Reply::Integer(0)
    );

    assert_eq!(
        runtime.execute(&args(&["SADD", "left", "a", "b", "c"]), 100),
        Reply::Integer(3)
    );
    assert_eq!(
        runtime.execute(&args(&["SADD", "right", "b", "c", "d"]), 100),
        Reply::Integer(3)
    );
    assert!(matches!(
        runtime.execute(&args(&["SINTER", "left", "right"]), 100),
        Reply::Array(values) if values.len() == 2
    ));

    assert_eq!(
        runtime.execute(&args(&["HSET", "hash", "counter", "1"]), 100),
        Reply::Integer(1)
    );
    assert_eq!(
        runtime.execute(&args(&["HINCRBY", "hash", "counter", "2"]), 100),
        Reply::Integer(3)
    );
    assert_eq!(
        runtime.execute(
            &args(&["HSETEX", "hash", "PX", "50", "FIELDS", "1", "ttl", "value"]),
            100,
        ),
        Reply::Integer(1)
    );

    assert_eq!(
        runtime.execute(&args(&["GEOADD", "places", "13.4", "52.5", "berlin"]), 100),
        Reply::Integer(1)
    );
    assert!(matches!(
        runtime.execute(&args(&["GEOPOS", "places", "berlin"]), 100),
        Reply::Replies(values) if values.len() == 1
    ));

    assert_eq!(
        runtime.execute(&args(&["PFADD", "visitors", "alice"]), 100),
        Reply::Integer(1)
    );
    assert_eq!(
        runtime.execute(&args(&["PFDEBUG", "ENCODING", "visitors"]), 100),
        Reply::Bulk(Some("sparse".into()))
    );
    assert_eq!(
        runtime.execute(&args(&["PFSELFTEST"]), 100),
        Reply::Simple("OK".into())
    );
}

#[test]
fn stream_and_cross_database_commands_execute_in_embedded_runtime() {
    let mut runtime = EmbeddedRuntime::new();
    assert!(matches!(
        runtime.execute(&args(&["XADD", "events", "*", "kind", "created"]), 1_000),
        Reply::Bulk(Some(_))
    ));
    assert_eq!(
        runtime.execute(&args(&["XLEN", "events"]), 1_000),
        Reply::Integer(1)
    );
    assert!(matches!(
        runtime.execute(&args(&["XRANGE", "events", "-", "+"]), 1_000),
        Reply::Replies(entries) if entries.len() == 1
    ));
    assert_eq!(
        runtime.execute(
            &args(&["XGROUP", "CREATE", "events", "workers", "0"]),
            1_000
        ),
        Reply::Simple("OK".into())
    );

    assert_eq!(
        runtime.execute(&args(&["SET", "source", "value"]), 1_000),
        Reply::Simple("OK".into())
    );
    assert_eq!(
        runtime.execute(&args(&["COPY", "source", "copied", "DB", "1"]), 1_000),
        Reply::Integer(1)
    );
    assert_eq!(
        runtime.execute(&args(&["SELECT", "1"]), 1_000),
        Reply::Simple("OK".into())
    );
    assert_eq!(
        runtime.execute(&args(&["GET", "copied"]), 1_000),
        Reply::Bulk(Some("value".into()))
    );
    assert_eq!(
        runtime.execute(&args(&["RPUSH", "numbers", "3", "1", "2"]), 1_000),
        Reply::Integer(3)
    );
    assert_eq!(
        runtime.execute(&args(&["SORT", "numbers"]), 1_000),
        Reply::Array(vec![Some("1".into()), Some("2".into()), Some("3".into())])
    );
    assert!(matches!(
        runtime.execute(&args(&["DUMP", "copied"]), 1_000),
        Reply::Bulk(Some(_))
    ));
}
