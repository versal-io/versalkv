use crate::engine::extension::script::SCRIPT_NO_WRITES;
use crate::engine::command::context::CommandConn;
use crate::foundation::io::protocol::Value;
use crate::framework::commands::pattern::string_match;
use crate::framework::context::ServerContext;
use crate::framework::CommandArgs;

/// FUNCTION LIST [WITHCODE] [LIBRARYNAME <pattern>]
pub(super) fn function_list(
    _conn: &mut dyn CommandConn,
    args: &CommandArgs,
    ctx: &mut ServerContext,
) -> Value {
    let mut withcode = false;
    let mut libraryname_pattern: Option<String> = None;
    let mut i = 1;
    while i < args.args.len() {
        let opt = match std::str::from_utf8(&args.args[i]) {
            Ok(s) => s,
            Err(_) => return Value::error("ERR Unknown argument"),
        };
        match opt.to_uppercase().as_str() {
            "WITHCODE" => {
                if withcode {
                    return Value::error("ERR Unknown argument withcode");
                }
                withcode = true;
            }
            "LIBRARYNAME" => {
                if libraryname_pattern.is_some() {
                    return Value::error("ERR Unknown argument libraryname");
                }
                i += 1;
                if i >= args.args.len() {
                    return Value::error("ERR library name argument was not given");
                }
                libraryname_pattern =
                    Some(std::str::from_utf8(&args.args[i]).unwrap_or("").to_string());
            }
            _ => return Value::error(format!("ERR Unknown argument {}", opt)),
        }
        i += 1;
    }

    let libs = ctx.script_mut().function_registry.list_libraries();
    let mut result = Vec::with_capacity(libs.len());

    for lib in &libs {
        if let Some(ref pattern) = libraryname_pattern {
            if !string_match(pattern.as_bytes(), lib.name.as_bytes(), false) {
                continue;
            }
        }

        let mut entry = vec![Value::Bulk(bytes::Bytes::from_static(b"library_name"))];
        entry.push(Value::Bulk(bytes::Bytes::copy_from_slice(
            lib.name.as_bytes(),
        )));
        entry.push(Value::Bulk(bytes::Bytes::from_static(b"engine")));
        entry.push(Value::Bulk(bytes::Bytes::copy_from_slice(
            lib.engine.name().as_bytes(),
        )));
        entry.push(Value::Bulk(bytes::Bytes::from_static(b"functions")));
        let functions = lib
            .functions
            .iter()
            .map(|func| {
                let flags = if func.flags & SCRIPT_NO_WRITES != 0 {
                    Value::Array(vec![Value::Bulk(bytes::Bytes::from_static(b"no-writes"))])
                } else {
                    Value::Array(Vec::new())
                };
                let desc = if func.description.is_empty() {
                    Value::Array(Vec::new())
                } else {
                    Value::Bulk(bytes::Bytes::copy_from_slice(func.description.as_bytes()))
                };
                Value::Array(vec![
                    Value::Bulk(bytes::Bytes::from_static(b"name")),
                    Value::Bulk(bytes::Bytes::copy_from_slice(func.name.as_bytes())),
                    Value::Bulk(bytes::Bytes::from_static(b"description")),
                    desc,
                    Value::Bulk(bytes::Bytes::from_static(b"flags")),
                    flags,
                ])
            })
            .collect();
        entry.push(Value::Array(functions));

        if withcode {
            entry.push(Value::Bulk(bytes::Bytes::from_static(b"library_code")));
            entry.push(Value::Bulk(bytes::Bytes::copy_from_slice(
                lib.code.as_bytes(),
            )));
        }

        result.push(Value::Array(entry));
    }

    Value::Array(result)
}

/// FUNCTION STATS
pub(super) fn function_stats(
    _conn: &mut dyn CommandConn,
    _args: &CommandArgs,
    ctx: &mut ServerContext,
) -> Value {
    let mut stats = Vec::new();

    stats.push(Value::Bulk(bytes::Bytes::from_static(b"running_script")));
    if let Some(control) = ctx.script().running_script.as_ref() {
        if let Some((func_name, caller_args, start_time)) = control.function_info() {
            let mut info = Vec::new();
            info.push(Value::Bulk(bytes::Bytes::from_static(b"name")));
            info.push(Value::Bulk(bytes::Bytes::copy_from_slice(
                func_name.as_bytes(),
            )));
            info.push(Value::Bulk(bytes::Bytes::from_static(b"command")));
            let cmd_parts: Vec<Value> = caller_args
                .iter()
                .map(|a| Value::Bulk(bytes::Bytes::copy_from_slice(a)))
                .collect();
            info.push(Value::Array(cmd_parts));
            info.push(Value::Bulk(bytes::Bytes::from_static(b"duration_ms")));
            let duration = start_time
                .map(|t| t.elapsed().as_millis() as i64)
                .unwrap_or(0);
            info.push(Value::Integer(duration));
            stats.push(Value::Array(info));
        } else {
            stats.push(Value::Null);
        }
    } else {
        stats.push(Value::Null);
    }

    stats.push(Value::Bulk(bytes::Bytes::from_static(b"engines")));
    stats.push(Value::Array(vec![
        Value::Bulk(bytes::Bytes::from_static(b"LUA")),
        Value::Array(vec![
            Value::Bulk(bytes::Bytes::from_static(b"libraries_count")),
            Value::Integer(ctx.script_mut().function_registry.library_count() as i64),
            Value::Bulk(bytes::Bytes::from_static(b"functions_count")),
            Value::Integer(ctx.script_mut().function_registry.function_count() as i64),
        ]),
    ]));

    Value::Array(stats)
}

/// FUNCTION HELP
pub(super) fn function_help(
    _conn: &mut dyn CommandConn,
    _args: &CommandArgs,
    _ctx: &mut ServerContext,
) -> Value {
    let help = vec![
        Value::Bulk(bytes::Bytes::from_static(
            b"FUNCTION <subcommand> [<arg> [value] [opt] ...]. Available subcommands:",
        )),
        Value::Bulk(bytes::Bytes::from_static(
            b"LOAD <code> [REPLACE] -- Load a new library to the server",
        )),
        Value::Bulk(bytes::Bytes::from_static(
            b"DELETE <library_name> -- Delete a library from the server",
        )),
        Value::Bulk(bytes::Bytes::from_static(
            b"LIST [WITHCODE] -- List all libraries with their metadata",
        )),
        Value::Bulk(bytes::Bytes::from_static(b"FLUSH -- Delete all libraries")),
        Value::Bulk(bytes::Bytes::from_static(
            b"STATS -- Show information about the current function execution",
        )),
        Value::Bulk(bytes::Bytes::from_static(
            b"KILL -- Kill the currently executing function",
        )),
        Value::Bulk(bytes::Bytes::from_static(
            b"DUMP -- Dump all functions into a serialized binary payload",
        )),
        Value::Bulk(bytes::Bytes::from_static(
            b"RESTORE <payload> [FLUSH | APPEND | REPLACE] -- Restore libraries from a serialized payload",
        )),
        Value::Bulk(bytes::Bytes::from_static(
            b"HELP -- Prints this help message",
        )),
    ];
    Value::Array(help)
}

/// FUNCTION DUMP
pub(super) fn function_dump(
    _conn: &mut dyn CommandConn,
    _args: &CommandArgs,
    ctx: &mut ServerContext,
) -> Value {
    let data = ctx.script_mut().function_registry.dump();
    Value::Bulk(bytes::Bytes::from(data))
}
