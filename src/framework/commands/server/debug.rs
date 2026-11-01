use crate::engine::command::context::CommandConn;
use crate::engine::distributed::replication::protocol::encode_command;
use crate::foundation::io::protocol::Value;
use crate::framework::context::ServerContext;
use crate::framework::{CommandArgs, CommandError};
use bytes::Bytes;
use command_macros::command;
use std::path::Path;
mod danger;
mod dataset;
mod help;
mod introspection;
mod load_aof;
mod object_info;

use load_aof::prepare_debug_loadaof;

#[command("DEBUG", hosted = crate::framework::commands::server::debug::prepare_debug_task)]
pub fn debug_cmd(conn: &mut dyn CommandConn, args: &CommandArgs, ctx: &mut ServerContext) -> Value {
    if !debug_allowed(conn, ctx) {
        return Value::error(
            "ERR DEBUG command not allowed. If the enable-debug-command option is set to \"local\", \
             you can run it from a local connection, otherwise you need to set this option \
             in the configuration file, and then restart the server.",
        );
    }
    if args.args.is_empty() {
        return CommandError::syntax_error();
    }
    let sub = String::from_utf8_lossy(&args.args[0]).to_ascii_lowercase();
    match sub.as_str() {
        "help" => help::debug_help(),
        "force-tls-write-error" => {
            if args.args.len() != 2 {
                return CommandError::syntax_error();
            }
            if !cfg!(feature = "tls") {
                return Value::error("ERR TLS is not enabled");
            }
            let should_fail = match args.args[1].as_ref() {
                b"0" => false,
                b"1" => true,
                _ => return Value::error("ERR Value must be 0 or 1"),
            };
            #[cfg(not(feature = "wasm"))]
            crate::foundation::io::transport::set_tls_write_error(should_fail);
            #[cfg(feature = "wasm")]
            let _ = should_fail;
            Value::ok()
        }
        "client-enforce-reply-list" => {
            if args.args.len() != 2 {
                return CommandError::syntax_error();
            }
            let val = String::from_utf8_lossy(&args.args[1]);
            ctx.config_mut().server.enforce_reply_list =
                matches!(val.as_ref(), "1" | "yes" | "true");
            Value::ok()
        }
        "set-active-expire" => {
            if args.args.len() != 2 {
                return CommandError::syntax_error();
            }
            let val = String::from_utf8_lossy(&args.args[1]);
            ctx.config_mut().server.active_expire_enabled =
                matches!(val.as_ref(), "1" | "yes" | "true");
            Value::ok()
        }
        "sweep-tracking-table" => {
            if args.args.len() != 1 {
                return CommandError::syntax_error();
            }
            ctx.tracking_mut().sweep_disconnected_clients();
            Value::ok()
        }
        "dict-resizing" => {
            if args.args.len() != 2 {
                return CommandError::syntax_error();
            }
            let val = String::from_utf8_lossy(&args.args[1]);
            let enabled = matches!(val.as_ref(), "1" | "yes" | "true");
            ctx.config_mut().server.dict_resizing = enabled;
            ctx.update_dict_resize_policy();
            Value::ok()
        }
        "htstats" => introspection::debug_htstats(args, ctx),
        "object" => object_info::debug_object(conn, args, ctx),
        "protocol" => debug_protocol(conn, args),
        "loadaof" => Value::error("ERR DEBUG LOADAOF requires the runtime host"),
        "change-repl-id" => {
            if args.args.len() != 1 {
                return CommandError::syntax_error();
            }
            crate::server_log!(
                notice,
                "Changing replication IDs after receiving DEBUG change-repl-id"
            );
            ctx.replication_mut().backlog.change_replication_id();
            Value::ok()
        }
        "digest" => dataset::debug_digest(ctx),
        "digest-value" => dataset::debug_digest_value(conn, args, ctx),
        "populate" => dataset::debug_populate(conn, args, ctx),
        "replicate" => debug_replicate(conn, args),
        "error" => debug_error(args),
        "segfault" => {
            danger::flush_diagnostic_query_line(conn, ctx);
            danger::debug_segfault(args)
        },
        "panic" => danger::debug_panic(args),
        "assert" => {
            danger::flush_diagnostic_query_line(conn, ctx);
            danger::debug_assert(args)
        },
        "oom" => danger::debug_oom(args),
        "restart" => danger::debug_restart(conn, args, ctx, true),
        "crash-and-recover" => danger::debug_restart(conn, args, ctx, false),
        "log" => introspection::debug_log(args),
        "leak" => introspection::debug_leak(args),
        "structsize" => introspection::debug_structsize(args),
        "sdslen" => introspection::debug_sdslen(conn, args, ctx),
        "listpack" => introspection::debug_listpack(conn, args, ctx),
        "quicklist" => introspection::debug_quicklist(conn, args, ctx),
        "htstats-key" => introspection::debug_htstats_key(conn, args, ctx),
        "stringmatch-test" => introspection::debug_stringmatch_test(args),
        "hashtable-can-abort-shrink" => introspection::debug_hashtable_can_abort_shrink(args),
        "drop-cluster-packet-filter"
        | "close-cluster-link-on-packet-drop"
        | "disable-cluster-random-ping"
        | "disable-cluster-reconnection" => {
            match ctx.cluster_state_mut().as_deref_mut() {
                Some(state) => debug_cluster_packet_filter(args, state),
                None => Value::error("ERR This instance has cluster support disabled"),
            }
        }
        "clusterlink" => match ctx.cluster_state_mut().as_deref_mut() {
            Some(state) => debug_cluster_link(args, state),
            None => Value::error("ERR Debug option only available for cluster mode enabled setup!"),
        },
        "cluster-replica-priority" => match ctx.cluster_state().as_deref() {
            Some(state) => debug_cluster_replica_priority(args, state),
            None => Value::error("ERR Debug option only available for cluster mode enabled setup!"),
        },
        "cluster-failover-delay" | "cluster-failover-epoch" => {
            if args.args.len() != 2 { return CommandError::syntax_error(); }
            let value = match std::str::from_utf8(&args.args[1]).ok().and_then(|s| s.parse::<i64>().ok()) {
                Some(value) => value,
                None => return Value::error("ERR value is not an integer or out of range"),
            };
            let is_delay = sub == "cluster-failover-delay";
            if is_delay && i32::try_from(value).is_err() {
                return Value::error("ERR value is out of range");
            }
            if value < -1 {
                return Value::error(if is_delay {
                    "ERR delay-ms must be -1 (default) or a non-negative value in ms"
                } else { "ERR epoch must be -1 (default) or a non-negative value" });
            }
            let Some(state) = ctx.cluster_state_mut().as_deref_mut() else {
                return Value::error("ERR This instance has cluster support disabled");
            };
            let override_value = u64::try_from(value).ok();
            if is_delay { state.automatic_failover.set_debug_delay_ms(override_value); }
            else { state.automatic_failover.set_debug_epoch(override_value); }
            Value::ok()
        }
        "slotmigration" => match ctx.cluster_state_mut().as_deref_mut() {
            Some(state) => debug_slot_migration(args, state),
            None => Value::error("ERR This instance has cluster support disabled"),
        },
        "client-eviction" => {
            if args.args.len() != 1 {
                return CommandError::syntax_error();
            }
            if ctx.config().server.maxmemory_clients == "0" {
                Value::error("ERR maxmemory-clients is disabled.")
            } else {
                Value::verbatim(*b"txt", Bytes::new())
            }
        }
        "set-disable-deny-scripts" => debug_set_disable_deny_scripts(args, ctx),
        "config-rewrite-force-all" => {
            if args.args.len() != 1 {
                return CommandError::syntax_error();
            }
            crate::framework::commands::server::config::config_rewrite(ctx)
        }
        "pause-after-fork" => introspection::debug_pause_after_fork(args, ctx),
        "pause-before-psync" => introspection::debug_pause_before_psync(args, ctx),
        "delay-rdb-client-free-seconds" => {
            introspection::debug_delay_rdb_client_free_seconds(args, ctx)
        }
        "aof-flush-sleep" | "force-free-primary-async" => introspection::debug_set_integer(args),
        "sleep" => introspection::debug_sleep(args, ctx),
        "quicklist-packed-threshold" => {
            if args.args.len() != 2 {
                return CommandError::syntax_error();
            }
            match parse_memtoull(&args.args[1]) {
                Some(sz) if sz <= (1u64 << 32) - (1u64 << 20) => {
                    Value::ok()
                }
                _ => Value::error(
                    "ERR argument must be a memory value bigger than 1 and smaller than 4gb",
                ),
            }
        }
        "replybuffer" => debug_replybuffer(args),
        "set-skip-checksum-validation" => {
            if args.args.len() != 2 {
                return CommandError::syntax_error();
            }
            let val = String::from_utf8_lossy(&args.args[1]);
            ctx.config_mut().server.skip_checksum_validation =
                matches!(val.as_ref(), "1" | "yes" | "true");
            Value::ok()
        }
        "reload" => Value::error("ERR DEBUG RELOAD requires the runtime host"),
        "pause-cron" => {
            if args.args.len() != 2 {
                return Value::error(
                    "ERR wrong number of arguments for 'debug|pause-cron' command",
                );
            }
            let val = match std::str::from_utf8(&args.args[1])
                .ok()
                .and_then(|s| s.parse::<u32>().ok())
            {
                Some(v) => v,
                None => return Value::error("ERR value is not an integer or out of range"),
            };
            ctx.set_pause_cron(val);
            Value::ok()
        }
        "protect-client" => {
            if args.args.len() != 2 {
                return Value::error(
                    "ERR wrong number of arguments for 'debug|protect-client' command",
                );
            }
            let id = match std::str::from_utf8(&args.args[1])
                .ok()
                .and_then(|s| s.parse::<u64>().ok())
            {
                Some(v) => v,
                None => return Value::error("ERR value is not an integer or out of range"),
            };
            ctx.protect_client(id);
            Value::ok()
        }
        _ => CommandError::syntax_error(),
    }
}

pub(crate) fn prepare_debug_task(
    conn: &mut dyn CommandConn,
    args: &CommandArgs,
    ctx: &mut ServerContext,
) -> Result<crate::framework::host_tasks::PreparedHostTask, Value> {
    if !debug_allowed(conn, ctx) || args.args.is_empty() {
        return Err(debug_cmd(conn, args, ctx));
    }
    if args.args[0].eq_ignore_ascii_case(b"loadaof") {
        if args.args.len() != 1 {
            return Err(CommandError::syntax_error());
        }
        return prepare_debug_loadaof(ctx);
    }
    if args.args[0].eq_ignore_ascii_case(b"bio-drain") {
        if args.args.len() != 2 || !args.args[1].eq_ignore_ascii_case(b"BIO_CLUSTER_SAVE") {
            return Err(CommandError::syntax_error());
        }
        return Ok(crate::framework::host_tasks::PreparedHostTask::DrainClusterConfig);
    }
    if !args.args[0].eq_ignore_ascii_case(b"reload") {
        return Err(debug_cmd(conn, args, ctx));
    }

    let mut should_flush = true;
    let mut should_save = true;
    let mut should_merge = false;
    for option in args.args.iter().skip(1) {
        match String::from_utf8_lossy(option).to_ascii_lowercase().as_str() {
            "merge" => should_merge = true,
            "noflush" => should_flush = false,
            "nosave" => should_save = false,
            _ => {
                return Err(Value::error(
                    "ERR DEBUG RELOAD only supports the MERGE, NOFLUSH and NOSAVE options.",
                ));
            }
        }
    }
    if !should_flush && !should_merge {
        return Err(Value::error(
            "ERR Error trying to load the RDB dump, check server logs.",
        ));
    }
    if should_save && crate::framework::commands::server::save::do_save(ctx).is_err() {
        return Err(Value::error("ERR Error saving the DB before reload"));
    }

    let path = Path::new(&ctx.config().persistence.dir)
        .join(&ctx.config().persistence.dbfilename);
    let bytes = std::fs::read(&path)
        .map_err(|error| Value::error(format!("ERR Error reading RDB file: {}", error)))?;
    if should_flush {
        ctx.persistence().cancel_forkless_save();
    }
    let placeholder = ctx.dbs().empty_like();
    let existing = std::mem::replace(ctx.dbs_mut(), placeholder);
    let (dataset, rollback_dataset) = if should_flush {
        ctx.reset_repl_seldb();
        (existing.empty_like(), Some(existing))
    } else {
        (existing, None)
    };
    let mut policy = crate::foundation::io::persistence::RdbDecodePolicy::persisted(
        true,
        ctx.config().persistence.key_load_delay,
    ).with_zset_limits(
        ctx.config().encoding.zset_max_ziplist_entries as usize,
        ctx.config().encoding.zset_max_ziplist_value as usize,
    );
    policy.allow_duplicate_keys = should_merge;
    let task = crate::framework::host_tasks::RdbLoadTask::new(
        bytes,
        dataset,
        policy,
        crate::framework::host_tasks::DatasetVisibility::Visible,
        ctx.config()
            .persistence
            .loading_process_events_interval_bytes,
    ).with_skip_codec_checksums(
        !ctx.config().persistence.rdbchecksum || ctx.config().server.skip_checksum_validation,
    );
    ctx.set_host_task_loading(true);
    Ok(crate::framework::host_tasks::PreparedHostTask::DebugRdb(
        crate::framework::host_tasks::DebugRdbTask::new(
            task,
            should_flush,
            rollback_dataset,
        ),
    ))
}

fn debug_allowed(conn: &dyn CommandConn, ctx: &ServerContext) -> bool {
    debug_allowed_action(ctx.config().server.enable_debug_command, conn.is_local())
}

pub(super) fn debug_allowed_action(
    action: crate::engine::extension::config::server::ProtectedAction,
    is_local: bool,
) -> bool {
    match action {
        crate::engine::extension::config::server::ProtectedAction::Yes => true,
        crate::engine::extension::config::server::ProtectedAction::Local => is_local,
        crate::engine::extension::config::server::ProtectedAction::No => false,
    }
}

fn debug_set_disable_deny_scripts(args: &CommandArgs, ctx: &mut ServerContext) -> Value {
    if args.args.len() != 2 {
        return CommandError::syntax_error();
    }
    let disabled = match std::str::from_utf8(&args.args[1])
        .ok()
        .and_then(|value| value.parse::<u8>().ok())
    {
        Some(0) => false,
        Some(1) => true,
        _ => return Value::error("ERR value is not an integer or out of range"),
    };
    ctx.set_script_disable_deny_scripts(disabled);
    Value::ok()
}

fn debug_protocol(conn: &mut dyn CommandConn, args: &CommandArgs) -> Value {
    if args.args.len() != 2 {
        return CommandError::syntax_error();
    }
    let what = String::from_utf8_lossy(&args.args[1]).to_ascii_lowercase();
    match what.as_str() {
        "string" => Value::bulk(Bytes::from_static(b"Hello World")),
        "integer" => Value::Integer(12345),
        "double" => {
            #[allow(clippy::approx_constant)]
            Value::Double(3.141)
        }
        "bignum" => Value::bignum(Bytes::from_static(b"1234567999999999999999999999999999999")),
        "null" => Value::Null,
        "array" => Value::Array(vec![
            Value::integer(0),
            Value::integer(1),
            Value::integer(2),
        ]),
        "set" => Value::set(vec![
            Value::integer(0),
            Value::integer(1),
            Value::integer(2),
        ]),
        "map" => Value::map(vec![
            (Value::integer(0), Value::boolean(false)),
            (Value::integer(1), Value::boolean(true)),
            (Value::integer(2), Value::boolean(false)),
        ]),
        "true" => Value::boolean(true),
        "false" => Value::boolean(false),
        "verbatim" => Value::verbatim(*b"txt", Bytes::from_static(b"This is a verbatim\nstring")),
        "attrib" => {
            let inner = Value::bulk(Bytes::from_static(
                b"Some real reply following the attribute",
            ));
            if conn.resp_version() >= 3 {
                let attr_map = vec![(
                    Value::bulk(Bytes::from_static(b"key-popularity")),
                    Value::Array(vec![
                        Value::bulk(Bytes::from_static(b"key:123")),
                        Value::integer(90),
                    ]),
                )];
                Value::attribute(attr_map, inner)
            } else {
                inner
            }
        }
        "push" => {
            if conn.resp_version() < 3 {
                return Value::error("ERR RESP2 is not supported by this command");
            }
            Value::push(vec![
                Value::bulk(Bytes::from_static(b"server-cpu-usage")),
                Value::integer(42),
            ])
        }
        _ => Value::error(
            "ERR Wrong protocol type name. Please use one of the following: string|integer|double|bignum|null|array|set|map|attrib|push|verbatim|true|false",
        ),
    }
}

fn debug_error(args: &CommandArgs) -> Value {
    if args.args.len() != 2 {
        return CommandError::syntax_error();
    }
    let msg = String::from_utf8_lossy(&args.args[1]).replace(['\r', '\n'], " ");
    Value::Error(Bytes::from(msg))
}

fn debug_replicate(conn: &mut dyn CommandConn, args: &CommandArgs) -> Value {
    if args.args.len() < 2 || args.args[1].is_empty() {
        return CommandError::syntax_error();
    }
    let parts: Vec<&[u8]> = args.args[1..].iter().map(Bytes::as_ref).collect();
    conn.record_custom_propagation(encode_command(&parts));
    Value::ok()
}

fn parse_memtoull(raw: &[u8]) -> Option<u64> {
    let s = String::from_utf8_lossy(raw);
    let s = s.trim();
    if s.is_empty() || s.starts_with('-') {
        return None;
    }
    let split_at = s
        .char_indices()
        .find(|&(_, ch)| !ch.is_ascii_digit())
        .map(|(i, _)| i)
        .unwrap_or(s.len());
    let (digits, unit) = s.split_at(split_at);
    let value: u64 = digits.parse().ok()?;
    let mul: u64 = match unit.to_ascii_lowercase().as_str() {
        "" | "b" => 1,
        "k" => 1_000,
        "kb" => 1_024,
        "m" => 1_000_000,
        "mb" => 1_024 * 1_024,
        "g" => 1_000_000_000,
        "gb" => 1_024 * 1_024 * 1_024,
        _ => return None,
    };
    value.checked_mul(mul)
}

fn debug_cluster_packet_filter(
    args: &CommandArgs,
    state: &mut crate::engine::distributed::cluster::ClusterState,
) -> Value {
    if args.args.len() != 2 {
        return CommandError::syntax_error();
    }
    let value = match std::str::from_utf8(&args.args[1])
        .ok()
        .and_then(|raw| raw.parse::<i64>().ok())
    {
        Some(value) => value,
        None => return Value::error("ERR value is not an integer or out of range"),
    };
    if args.args[0].eq_ignore_ascii_case(b"drop-cluster-packet-filter") {
        state.set_debug_packet_filter(value);
    } else if args.args[0].eq_ignore_ascii_case(b"close-cluster-link-on-packet-drop") {
        state.set_debug_close_link_on_packet_drop(value != 0);
    } else if args.args[0].eq_ignore_ascii_case(b"disable-cluster-random-ping") {
        state.set_debug_disable_random_ping(value != 0);
    } else {
        state.set_debug_disable_reconnection(value != 0);
    }
    Value::ok()
}

fn debug_slot_migration(
    args: &CommandArgs,
    state: &mut crate::engine::distributed::cluster::ClusterState,
) -> Value {
    if args.args.len() != 3 {
        return CommandError::syntax_error();
    }
    let enabled = match args.args[2].as_ref() {
        b"0" => false,
        b"1" => true,
        _ => return Value::error("ERR value is not an integer or out of range"),
    };
    if args.args[1].eq_ignore_ascii_case(b"prevent-pause") {
        state.set_debug_prevent_slot_migration_pause(enabled);
    } else if args.args[1].eq_ignore_ascii_case(b"prevent-failover") {
        state.set_debug_prevent_slot_migration_failover(enabled);
    } else {
        return CommandError::syntax_error();
    }
    Value::ok()
}

fn debug_cluster_replica_priority(
    args: &CommandArgs,
    state: &crate::engine::distributed::cluster::ClusterState,
) -> Value {
    if args.args.len() != 2 {
        return CommandError::syntax_error();
    }
    let node_id = String::from_utf8_lossy(&args.args[1]);
    let node = if node_id == state.myself.name {
        Some(&state.myself)
    } else {
        state.nodes.get(node_id.as_ref())
    };
    match node {
        Some(node) => Value::Integer(i64::from(node.cluster_replica_priority)),
        None => Value::error(format!("ERR Unknown node {node_id}")),
    }
}

fn debug_cluster_link(
    args: &CommandArgs,
    state: &mut crate::engine::distributed::cluster::ClusterState,
) -> Value {
    if args.args.len() != 4 || !args.args[1].eq_ignore_ascii_case(b"kill") {
        return CommandError::syntax_error();
    }
    let node_id = String::from_utf8_lossy(&args.args[3]).into_owned();
    if node_id == state.myself.name {
        return Value::error("ERR Cannot free cluster link(s) to myself");
    }
    if !state.nodes.contains_key(&node_id) {
        return Value::error(format!("ERR Unknown node {}", node_id));
    }
    let direction = if args.args[2].eq_ignore_ascii_case(b"to") {
        crate::engine::distributed::cluster::ClusterLinkDirection::To
    } else if args.args[2].eq_ignore_ascii_case(b"from") {
        crate::engine::distributed::cluster::ClusterLinkDirection::From
    } else if args.args[2].eq_ignore_ascii_case(b"all") {
        crate::engine::distributed::cluster::ClusterLinkDirection::All
    } else {
        return Value::error(format!(
            "ERR Unknown direction {}",
            String::from_utf8_lossy(&args.args[2])
        ));
    };
    state.observe_link_close(&node_id, direction);
    state.queue_link_close((node_id, direction));
    Value::ok()
}

fn debug_replybuffer(args: &CommandArgs) -> Value {
    if args.args.len() != 3 {
        return CommandError::syntax_error();
    }
    let sub = String::from_utf8_lossy(&args.args[1]).to_ascii_lowercase();
    let value = String::from_utf8_lossy(&args.args[2]).to_ascii_lowercase();
    match sub.as_str() {
        "peak-reset-time" => match value.as_str() {
            "never" | "reset" => Value::ok(),
            _ if value.parse::<u64>().is_ok() => Value::ok(),
            _ => CommandError::syntax_error(),
        },
        // Rust does not keep C's fixed reply buffer, but accepting this debug
        // switch preserves the test/control contract. Both values therefore
        // select the same allocator-backed queue implementation.
        "resizing" if matches!(value.as_str(), "0" | "1") => Value::ok(),
        _ => CommandError::syntax_error(),
    }
}

#[cfg(test)]
mod tests;
