#[path = "info_reply.rs"]
mod info_reply;

use std::net::ToSocketAddrs;

use self::info_reply::{instance_reply, master_ids, master_reply_instance};
use crate::engine::command::context::CommandConn;
use crate::engine::distributed::sentinel::{split_sentinel_endpoint, DownState, InstanceRole};
use crate::foundation::io::protocol::Value;
use crate::foundation::io::sys::time::mstime;
use crate::framework::context::ServerContext;
use crate::framework::{CommandArgs, CommandError};
use bytes::Bytes;

/// SENTINEL MASTERS
pub fn masters(_conn: &mut dyn CommandConn, _args: &CommandArgs, ctx: &mut ServerContext) -> Value {
    let now = mstime();
    let state = ctx.sentinel_state();
    let mut result = Vec::with_capacity(state.registry.master_count());
    for master in state.registry.masters().map(|(_, master)| master) {
        let reply = master_reply_instance(master, state.registry.instances().get(&master.id));
        result.push(instance_reply(&reply, now, state.announce_hostnames));
    }
    Value::Array(result)
}

/// SENTINEL MASTER <name>
pub fn master(_conn: &mut dyn CommandConn, args: &CommandArgs, ctx: &mut ServerContext) -> Value {
    if args.args.len() < 2 {
        return CommandError::wrong_arity("sentinel|master");
    }
    let now = mstime();
    let name = String::from_utf8_lossy(&args.args[1]);
    let state = ctx.sentinel_state();
    match state.registry.master(name.as_ref()) {
        Some(master_ref) => {
            let reply = master_reply_instance(master_ref, state.registry.instances().get(&master_ref.id));
            instance_reply(&reply, now, state.announce_hostnames)
        }
        None => Value::error(format!("ERR No such master with name '{}'", name)),
    }
}

/// SENTINEL REPLICAS <name>
pub fn replicas(_conn: &mut dyn CommandConn, args: &CommandArgs, ctx: &mut ServerContext) -> Value {
    if args.args.len() < 2 {
        return CommandError::wrong_arity("sentinel|replicas");
    }
    let now = mstime();
    let name = String::from_utf8_lossy(&args.args[1]);
    let ids = match master_ids(ctx, name.as_ref(), true) {
        Some(ids) => ids,
        None => return Value::error(format!("ERR No such master with name '{}'", name)),
    };
    let mut result = Vec::with_capacity(ids.len());
    let announce_hostnames = ctx.sentinel_state().announce_hostnames;
    for id in ids {
        if let Some(instance) = ctx.sentinel_state().registry.instances().get(&id) {
            if instance.role == InstanceRole::Replica && !instance.replica_announced {
                continue;
            }
            result.push(instance_reply(instance, now, announce_hostnames));
        }
    }
    Value::Array(result)
}

/// SENTINEL SENTINELS <name>
pub fn sentinels(
    _conn: &mut dyn CommandConn,
    args: &CommandArgs,
    ctx: &mut ServerContext,
) -> Value {
    if args.args.len() < 2 {
        return CommandError::wrong_arity("sentinel|sentinels");
    }
    let now = mstime();
    let name = String::from_utf8_lossy(&args.args[1]);
    let ids = match master_ids(ctx, name.as_ref(), false) {
        Some(ids) => ids,
        None => return Value::error(format!("ERR No such master with name '{}'", name)),
    };
    let mut result = Vec::with_capacity(ids.len());
    let announce_hostnames = ctx.sentinel_state().announce_hostnames;
    for id in ids {
        if let Some(instance) = ctx.sentinel_state().registry.instances().get(&id) {
            result.push(instance_reply(instance, now, announce_hostnames));
        }
    }
    Value::Array(result)
}

/// SENTINEL GET-MASTER-ADDR-BY-NAME <name>
pub fn get_master_addr_by_name(
    _conn: &mut dyn CommandConn,
    args: &CommandArgs,
    ctx: &mut ServerContext,
) -> Value {
    if args.args.len() < 2 {
        return CommandError::wrong_arity("sentinel|get-master-addr-by-name");
    }
    let name = String::from_utf8_lossy(&args.args[1]);
    let state = ctx.sentinel_state();
    let master = match state.registry.master(name.as_ref()) {
        Some(master) => master,
        None => return Value::Null,
    };
    let endpoint = match split_sentinel_endpoint(&master.addr) {
        Some(endpoint) => endpoint,
        None => return Value::Null,
    };
    let port = endpoint.port.to_string();
    let host = endpoint.host;
    let host = if state.announce_hostnames {
        host.to_string()
    } else {
        (host, 0_u16)
            .to_socket_addrs()
            .ok()
            .and_then(|addrs| {
                let mut fallback = None;
                for addr in addrs {
                    if addr.is_ipv4() {
                        return Some(addr.ip().to_string());
                    }
                    fallback.get_or_insert(addr.ip().to_string());
                }
                fallback
            })
            .unwrap_or_else(|| host.to_string())
    };
    Value::Array(vec![
        Value::bulk(host.as_bytes().to_vec()),
        Value::bulk(port.as_bytes().to_vec()),
    ])
}

/// SENTINEL INFO-CACHE <name> [<name> ...]
/// Returns the cached INFO response for each monitored primary and its replicas.
pub fn info_cache(
    _conn: &mut dyn CommandConn,
    args: &CommandArgs,
    ctx: &mut ServerContext,
) -> Value {
    let now = mstime();
    let state = ctx.sentinel_state();

    let names: Vec<String> = if args.args.len() == 1 {
        state.registry.master_names().cloned().collect()
    } else {
        args.args[1..]
            .iter()
            .map(|b| String::from_utf8_lossy(b).to_string())
            .collect()
    };

    let mut result = Vec::new();
    for name in &names {
        let master = match state.registry.master(name) {
            Some(m) => m,
            None => continue,
        };
        let master_id = master.id;
        let master_inst = state.registry.instances().get(&master_id).unwrap_or(master);

        result.push(Value::bulk(name.as_bytes().to_vec()));

        let mut entries = Vec::new();
        let cached_ms = if master_inst.info_refresh > 0 {
            now - master_inst.info_refresh
        } else {
            0
        };
        entries.push(Value::Array(vec![
            Value::Integer(cached_ms),
            if master_inst.info_cache.is_empty() {
                Value::Null
            } else {
                Value::bulk(master_inst.info_cache.as_bytes().to_vec())
            },
        ]));

        for rid in &master.replicas {
            if let Some(replica) = state.registry.instances().get(rid) {
                let cached_ms = if replica.info_refresh > 0 {
                    now - replica.info_refresh
                } else {
                    0
                };
                entries.push(Value::Array(vec![
                    Value::Integer(cached_ms),
                    if replica.info_cache.is_empty() {
                        Value::Null
                    } else {
                        Value::bulk(replica.info_cache.as_bytes().to_vec())
                    },
                ]));
            }
        }

        result.push(Value::Array(entries));
    }

    Value::Array(result)
}

/// SENTINEL PENDING-SCRIPTS
pub fn pending_scripts(
    _conn: &mut dyn CommandConn,
    _args: &CommandArgs,
    ctx: &mut ServerContext,
) -> Value {
    let engine = &ctx.sentinel_state().script_engine;
    let mut result = Vec::new();
    for job in &engine.pending {
        result.push(Value::Map(vec![
            (
                Value::bulk("script-type"),
                Value::bulk(job.script_type.as_str().as_bytes().to_vec()),
            ),
            (
                Value::bulk("script-path"),
                Value::bulk(job.script_path.as_bytes().to_vec()),
            ),
            (
                Value::bulk("retry-count"),
                Value::Integer(job.retry_count as i64),
            ),
        ]));
    }
    for (job, _) in &engine.running {
        result.push(Value::Map(vec![
            (
                Value::bulk("script-type"),
                Value::bulk(job.script_type.as_str().as_bytes().to_vec()),
            ),
            (
                Value::bulk("script-path"),
                Value::bulk(job.script_path.as_bytes().to_vec()),
            ),
            (Value::bulk("start-time"), Value::Integer(job.start_time)),
            (
                Value::bulk("pid"),
                Value::Integer(job.pid.unwrap_or(0) as i64),
            ),
            (
                Value::bulk("retry-count"),
                Value::Integer(job.retry_count as i64),
            ),
        ]));
    }
    Value::Array(result)
}

/// SENTINEL CKQUORUM <name>
pub fn ckquorum(_conn: &mut dyn CommandConn, args: &CommandArgs, ctx: &mut ServerContext) -> Value {
    if args.args.len() < 2 {
        return CommandError::wrong_arity("sentinel|ckquorum");
    }
    let name = String::from_utf8_lossy(&args.args[1]);
    let state = ctx.sentinel_state();
    let master = match state.registry.master(name.as_ref()) {
        Some(m) => m,
        None => return Value::error(format!("ERR No such master with name '{}'", name)),
    };

    let voters = master.sentinels.len() as i64 + 1; //  sentinels +
    let mut usable = 1; //  sentinel
    for sid in &master.sentinels {
        if let Some(sentinel) = state.registry.instances().get(sid) {
            if sentinel.down_state == DownState::SDown || sentinel.down_state == DownState::ODown {
                continue;
            }
            usable += 1;
        }
    }

    let mut noquorum = false;
    let mut noauth = false;
    if usable < master.quorum {
        noquorum = true;
    }
    if usable < voters / 2 + 1 {
        noauth = true;
    }

    if !noquorum && !noauth {
        Value::bulk(
            format!(
                "OK {} usable Sentinels. Quorum and failover authorization can be reached",
                usable
            )
            .into_bytes(),
        )
    } else {
        let mut e = format!("NOQUORUM {} usable Sentinels. ", usable);
        if noquorum {
            e.push_str(
                "Not enough available Sentinels to reach the specified quorum for this master",
            );
        }
        if noauth {
            if noquorum {
                e.push_str(". ");
            }
            e.push_str(
                "Not enough available Sentinels to reach the majority and authorize a failover",
            );
        }
        Value::error(e)
    }
}

/// SENTINEL HELP
pub fn help(_conn: &mut dyn CommandConn, _args: &CommandArgs, _ctx: &mut ServerContext) -> Value {
    let help_text = vec![
        Value::bulk(Bytes::from_static(b"SENTINEL <subcommand> [<arg>] ...")),
        Value::bulk(Bytes::from_static(b"subcommands:")),
        Value::bulk(Bytes::from_static(b"  MASTERS")),
        Value::bulk(Bytes::from_static(b"  MASTER <name>")),
        Value::bulk(Bytes::from_static(b"  REPLICAS <name>")),
        Value::bulk(Bytes::from_static(b"  SENTINELS <name>")),
        Value::bulk(Bytes::from_static(b"  GET-MASTER-ADDR-BY-NAME <name>")),
        Value::bulk(Bytes::from_static(
            b"  MONITOR <name> <host> <port> <quorum>",
        )),
        Value::bulk(Bytes::from_static(b"  REMOVE <name>")),
        Value::bulk(Bytes::from_static(b"  SET <name> <option> <value>")),
        Value::bulk(Bytes::from_static(b"  FAILOVER <name>")),
        Value::bulk(Bytes::from_static(b"  CKQUORUM <name>")),
        Value::bulk(Bytes::from_static(
            b"  IS-MASTER-DOWN-BY-ADDR <ip> <port> <epoch> <runid>",
        )),
        Value::bulk(Bytes::from_static(b"  RESET <pattern>")),
        Value::bulk(Bytes::from_static(b"  MYID")),
        Value::bulk(Bytes::from_static(b"  CONFIG GET/SET <param>")),
        Value::bulk(Bytes::from_static(b"  FLUSHCONFIG")),
        Value::bulk(Bytes::from_static(b"  INFO-CACHE")),
        Value::bulk(Bytes::from_static(b"  PENDING-SCRIPTS")),
        Value::bulk(Bytes::from_static(b"  SIMULATE-FAILURE <flag>")),
        Value::bulk(Bytes::from_static(b"  DEBUG <subcommand>")),
        Value::bulk(Bytes::from_static(b"  HELP")),
    ];
    Value::Array(help_text)
}
