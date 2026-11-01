use crate::engine::extension::lua::runtime::{
    compiled_eval_count, eval_argument_tables, reset_lua_runtime,
};
use crate::foundation::io::protocol::Value;
use crate::framework::capabilities::connection::Connection;
use crate::framework::capabilities::io::Token;
use crate::framework::commands::scripting::script_cache::ScriptCache;
use crate::framework::commands::scripting::script_cmd::script_cmd;
use crate::framework::context::ServerContext;
use crate::framework::host_tasks::{prepare_lua_task, run_lua_task_inline, PreparedHostTask};
use crate::framework::CommandArgs;
use bytes::Bytes;
use mlua::prelude::LuaTable;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};

fn connection() -> Connection {
    Connection::new(
        Token(1),
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 6379),
        false,
    )
}

fn run_hosted(conn: &mut Connection, ctx: &mut ServerContext, args: Vec<Bytes>) -> Value {
    let args = CommandArgs::from_parts(args);
    match prepare_lua_task(conn, &args, ctx).unwrap() {
        PreparedHostTask::Lua(task) => run_lua_task_inline(task, conn, ctx),
        _ => panic!("Expected a hosted Lua task"),
    }
}

#[test]
fn hosted_eval_reuses_one_compilation_across_argument_variants() {
    reset_lua_runtime();
    let script = Bytes::from_static(
        b"local reply={KEYS[1] or false,ARGV[1] or false,#KEYS,#ARGV}; KEYS[3]='stale'; ARGV[3]='stale'; return reply",
    );
    let digest = Bytes::from(ScriptCache::compute_sha1(
        std::str::from_utf8(&script).unwrap(),
    ));
    let mut conn = connection();
    let mut ctx = ServerContext::new(16);
    let mut previous_tables: Option<(LuaTable, LuaTable)> = None;
    let mut cache_memory = None;
    for index in 0..24 {
        let command = match index % 4 {
            0 => Bytes::from_static(b"EVAL"),
            1 => Bytes::from_static(b"EVALSHA"),
            2 => Bytes::from_static(b"EVAL_RO"),
            _ => Bytes::from_static(b"EVALSHA_RO"),
        };
        let numkeys = if index < 16 { 1 } else { index % 3 };
        let numargs = if index < 16 { 1 } else { (index + 1) % 3 };
        let key = Bytes::from(vec![
            b'k',
            0xff,
            0,
            b'"',
            b'\\',
            if index < 16 { 0 } else { index as u8 },
        ]);
        let arg = Bytes::from(vec![b'a', 0x80, 0, b'\n', b'\r', index as u8]);
        let mut parts = vec![
            command,
            if index % 2 == 0 {
                script.clone()
            } else {
                digest.clone()
            },
            Bytes::from(numkeys.to_string()),
        ];
        parts.extend((0..numkeys).map(|_| key.clone()));
        parts.extend((0..numargs).map(|_| arg.clone()));
        assert_eq!(
            run_hosted(&mut conn, &mut ctx, parts),
            Value::Array(vec![
                if numkeys == 0 {
                    Value::Null
                } else {
                    Value::Bulk(key)
                },
                if numargs == 0 {
                    Value::Null
                } else {
                    Value::Bulk(arg)
                },
                Value::Integer(numkeys as i64),
                Value::Integer(numargs as i64),
            ]),
            "Invocation {index} must use its own raw arguments"
        );
        let tables = eval_argument_tables().unwrap();
        if let Some((keys, args)) = previous_tables {
            assert_ne!(keys.to_pointer(), tables.0.to_pointer());
            assert_ne!(args.to_pointer(), tables.1.to_pointer());
        }
        previous_tables = Some(tables);
        assert_eq!(ctx.script().script_cache.len(), 1);
        let memory = ctx.script().script_cache.memory_usage();
        assert_eq!(*cache_memory.get_or_insert(memory), memory);
        assert_eq!(compiled_eval_count(), 1, "Invocation {index}");
    }
    reset_lua_runtime();
}

#[test]
fn script_load_and_hosted_eval_share_original_compilation() {
    reset_lua_runtime();
    let script = Bytes::from_static(b"#!lua flags=no-writes\nreturn {KEYS[1],ARGV[1]}");
    let mut conn = connection();
    let mut ctx = ServerContext::new(16);
    let load = CommandArgs::from_parts(vec![
        Bytes::from_static(b"SCRIPT"),
        Bytes::from_static(b"LOAD"),
        script.clone(),
    ]);
    let Value::Bulk(digest) = script_cmd(&mut conn, &load, &mut ctx) else {
        panic!("SCRIPT LOAD must return the original digest");
    };
    assert_eq!(compiled_eval_count(), 1);
    for index in 0..8 {
        let key = Bytes::from(vec![b'k', 0xff, 0, index]);
        let arg = Bytes::from(vec![b'a', 0x80, 0, index]);
        assert_eq!(
            run_hosted(
                &mut conn,
                &mut ctx,
                vec![
                    Bytes::from_static(b"EVALSHA_RO"),
                    digest.clone(),
                    Bytes::from_static(b"1"),
                    key.clone(),
                    arg.clone(),
                ],
            ),
            Value::Array(vec![Value::Bulk(key), Value::Bulk(arg)])
        );
        assert_eq!(compiled_eval_count(), 1);
    }
    let flush = CommandArgs::from_parts(vec![
        Bytes::from_static(b"SCRIPT"),
        Bytes::from_static(b"FLUSH"),
    ]);
    assert_eq!(script_cmd(&mut conn, &flush, &mut ctx), Value::ok());
    assert_eq!(compiled_eval_count(), 0);
    assert_eq!(ctx.script().script_cache.len(), 0);
    assert_eq!(
        run_hosted(
            &mut conn,
            &mut ctx,
            vec![
                Bytes::from_static(b"EVAL"),
                script,
                Bytes::from_static(b"1"),
                Bytes::from_static(b"new-key"),
                Bytes::from_static(b"new-arg"),
            ],
        ),
        Value::Array(vec![
            Value::Bulk(Bytes::from_static(b"new-key")),
            Value::Bulk(Bytes::from_static(b"new-arg")),
        ])
    );
    assert_eq!(compiled_eval_count(), 1);
    reset_lua_runtime();
}

#[test]
fn hosted_eval_reports_original_script_error_lines() {
    reset_lua_runtime();
    let mut conn = connection();
    let mut ctx = ServerContext::new(16);
    for (script, line) in [
        (
            b"local value=1\nlocal other=2\nerror('line-marker')".as_slice(),
            3,
        ),
        (
            b"#!lua flags=no-writes\nlocal value=1\nlocal other=2\nerror('line-marker')".as_slice(),
            4,
        ),
    ] {
        let result = run_hosted(
            &mut conn,
            &mut ctx,
            vec![
                Bytes::from_static(b"EVAL"),
                Bytes::copy_from_slice(script),
                Bytes::from_static(b"1"),
                Bytes::from_static(b"key"),
                Bytes::from_static(b"arg"),
            ],
        );
        let Value::Error(message) = result else {
            panic!("Expected a runtime error");
        };
        let message = std::str::from_utf8(&message).unwrap();
        assert!(
            message.contains(&format!("user_script:{line}:")),
            "{message}"
        );
    }
    reset_lua_runtime();
}

#[test]
fn hosted_eval_and_script_load_report_original_compile_error_lines() {
    reset_lua_runtime();
    let mut conn = connection();
    let mut ctx = ServerContext::new(16);
    for (script, line) in [
        (b"local value=1\nlocal = 2".as_slice(), 2),
        (
            b"#!lua flags=no-writes\nlocal value=1\nlocal = 2".as_slice(),
            3,
        ),
    ] {
        let eval = CommandArgs::from_parts(vec![
            Bytes::from_static(b"EVAL"),
            Bytes::copy_from_slice(script),
            Bytes::from_static(b"0"),
            Bytes::from_static(b"arg"),
        ]);
        let result = match prepare_lua_task(&mut conn, &eval, &mut ctx) {
            Err(result) => result,
            Ok(_) => panic!("Invalid script must fail during preparation"),
        };
        let Value::Error(message) = result else {
            panic!("Expected a compilation error");
        };
        let message = std::str::from_utf8(&message).unwrap();
        assert!(
            message.contains(&format!("user_script:{line}:")),
            "{message}"
        );
        let load = CommandArgs::from_parts(vec![
            Bytes::from_static(b"SCRIPT"),
            Bytes::from_static(b"LOAD"),
            Bytes::copy_from_slice(script),
        ]);
        let Value::Error(message) = script_cmd(&mut conn, &load, &mut ctx) else {
            panic!("SCRIPT LOAD must reject invalid code");
        };
        let message = std::str::from_utf8(&message).unwrap();
        assert!(
            message.contains(&format!("user_script:{line}:")),
            "{message}"
        );
        assert_eq!(compiled_eval_count(), 0);
        assert_eq!(ctx.script().script_cache.len(), 0);
    }
    reset_lua_runtime();
}
