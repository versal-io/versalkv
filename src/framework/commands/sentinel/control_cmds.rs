use crate::engine::command::context::CommandConn;
use crate::engine::distributed::sentinel::failover::vote_for_leader;
use crate::engine::distributed::sentinel::failover::{select_replica, start_failover};
use crate::engine::distributed::sentinel::{
    format_sentinel_endpoint, DownState, SentinelInstance, SentinelInstanceId, SentinelState,
    MONITORED_INSTANCE_FAILOVER_NS,
};
use crate::foundation::io::protocol::Value;
use crate::foundation::io::sys::time::mstime;
use crate::framework::context::ServerContext;
use crate::framework::{CommandArgs, CommandError};
use bytes::Bytes;

#[path = "control_set.rs"]
mod control_set;

use control_set::validate_no_control_chars;

fn failover_in_progress_error() -> Value {
    Value::error("INPROG Failover already in progress")
}

fn ensure_failover_candidate(
    master: &SentinelInstance,
    instances: &std::collections::HashMap<SentinelInstanceId, SentinelInstance>,
    link_rebuild_needed: bool,
    now: i64,
) -> Result<(), Value> {
    if link_rebuild_needed {
        return Err(Value::error("NOGOODSLAVE No suitable replica to promote"));
    }
    // Manual failover marks the primary SDOWN immediately after this check.
    // Validate with the same stricter freshness window the state machine will
    // use, otherwise the command can return OK and abort one tick later.
    let mut failing_master = master.clone();
    failing_master.down_state = DownState::SDown;
    select_replica(&failing_master, instances, now)
        .map(|_| ())
        .ok_or_else(|| Value::error("NOGOODSLAVE No suitable replica to promote"))
}

fn is_primary_down(
    master_state: DownState,
    instance_state: DownState,
    down_after_ms: i64,
    act_ping_time: i64,
    last_ping_ok: i64,
    link_state: i64,
    now: i64,
) -> bool {
    let elapsed = if act_ping_time > 0 {
        now - act_ping_time
    } else if link_state != 0 && last_ping_ok > 0 {
        now - last_ping_ok
    } else {
        0
    };
    master_state != DownState::Up || instance_state == DownState::SDown || elapsed > down_after_ms
}

fn remove_monitored_master(state: &mut SentinelState, name: &str) -> Result<(), Value> {
    let master = match state.registry.remove_master(name) {
        Some(master) => master,
        None => {
            return Err(Value::error(format!(
                "ERR No such master with name '{}'",
                name
            )))
        }
    };

    for replica_id in &master.replicas {
        state.registry.remove_instance(*replica_id);
    }
    for sentinel_id in &master.sentinels {
        state.registry.remove_instance(*sentinel_id);
    }
    state.link_rebuild_needed = true;

    Ok(())
}

fn parse_failover_request(args: &CommandArgs) -> Result<(String, bool), Value> {
    if args.args.len() < 2 {
        return Err(CommandError::wrong_arity("sentinel|failover"));
    }
    let name = String::from_utf8_lossy(&args.args[1]).to_string();
    let coordinated = args.args.len() == 3 && args.args[2].eq_ignore_ascii_case(b"COORDINATED");
    if args.args.len() > 2 && !coordinated {
        return Err(CommandError::syntax_error());
    }
    Ok((name, coordinated))
}

/// SENTINEL MONITOR <name> <host> <port> <quorum>
pub fn monitor(_conn: &mut dyn CommandConn, args: &CommandArgs, ctx: &mut ServerContext) -> Value {
    if args.args.len() < 5 {
        return CommandError::wrong_arity("sentinel|monitor");
    }
    let name = String::from_utf8_lossy(&args.args[1]).to_string();
    let host = String::from_utf8_lossy(&args.args[2]).to_string();
    let port = match String::from_utf8_lossy(&args.args[3]).parse::<u16>() {
        Ok(p) => p,
        Err(_) => return Value::error("ERR Invalid port"),
    };
    let quorum = match String::from_utf8_lossy(&args.args[4]).parse::<i64>() {
        Ok(q) => q,
        Err(_) => return Value::error("ERR Invalid quorum"),
    };
    if quorum <= 0 {
        return Value::error("ERR Quorum must be 1 or greater.");
    }

    if !validate_no_control_chars(&name) || !validate_no_control_chars(&host) {
        return Value::error("ERR Invalid argument: control characters not allowed");
    }

    let state = ctx.sentinel_state_mut();
    if state.registry.contains_master(&name) {
        return Value::error(format!(
            "DUPLICATE-SENTINEL-MASTER Master '{}' already exists",
            name
        ));
    }

    let id = state.next_instance_id;
    let addr = format_sentinel_endpoint(&host, port);

    let mut instance = SentinelInstance::new_master(id, &name, &addr, quorum);
    if state.tunables.default_down_after_ms != 30000 {
        instance.down_after_ms = state.tunables.default_down_after_ms;
    }
    if let Err(error) = state.registry.insert_master(name.clone(), instance) {
        return Value::error(format!("ERR {}", error));
    }
    state.next_instance_id += 1;
    state.initialized = true;

    Value::ok()
}

/// SENTINEL REMOVE <name>
pub fn remove(_conn: &mut dyn CommandConn, args: &CommandArgs, ctx: &mut ServerContext) -> Value {
    if args.args.len() < 2 {
        return CommandError::wrong_arity("sentinel|remove");
    }
    let name = String::from_utf8_lossy(&args.args[1]);

    if let Err(error) = remove_monitored_master(ctx.sentinel_state_mut(), name.as_ref()) {
        return error;
    }

    Value::ok()
}

/// SENTINEL SET <name> <option> <value> [...]
pub fn set(_conn: &mut dyn CommandConn, args: &CommandArgs, ctx: &mut ServerContext) -> Value {
    control_set::set(_conn, args, ctx)
}

/// SENTINEL FAILOVER [COORD] <name>
pub fn failover(_conn: &mut dyn CommandConn, args: &CommandArgs, ctx: &mut ServerContext) -> Value {
    let (name, coord) = match parse_failover_request(args) {
        Ok(request) => request,
        Err(error) => return error,
    };
    let mut master = match ctx
        .sentinel_state()
        .registry
        .take_master_snapshot(name.as_str())
    {
        Some(master) => master,
        None => return Value::error(format!("ERR No such master with name '{}'", name)),
    };
    if master.failover_state != crate::engine::distributed::sentinel::FailoverState::None {
        ctx.sentinel_state_mut()
            .registry
            .replace_master(name.clone(), master)
            .expect("failover updates an existing configured master");
        return failover_in_progress_error();
    }
    let now = mstime();
    if let Err(error) = ensure_failover_candidate(
        &master,
        ctx.sentinel_state().registry.instances(),
        ctx.sentinel_state().link_rebuild_needed,
        now,
    ) {
        ctx.sentinel_state_mut()
            .registry
            .replace_master(name.clone(), master)
            .expect("failover updates an existing configured master");
        return error;
    }
    master.sdown_since_time = now;
    master.down_state = DownState::SDown;
    if coord {
        if master.monitored_instance_failover_state == MONITORED_INSTANCE_FAILOVER_NS {
            ctx.sentinel_state_mut()
                .registry
                .replace_master(name.clone(), master)
                .expect("failover updates an existing configured master");
            return Value::error("NOGOODPRIMARY Primary does not support FAILOVER command");
        }
        master.flags |= crate::engine::distributed::sentinel::SRI_COORD_FAILOVER
            | crate::engine::distributed::sentinel::SRI_FORCE_FAILOVER;
    } else {
        master.flags |= crate::engine::distributed::sentinel::SRI_FORCE_FAILOVER;
    }
    {
        let state = ctx.sentinel_state_mut();
        start_failover(
            &mut state.current_epoch,
            state.tunables.ask_period_ms,
            &mut master,
            now,
        );
        if state.simfailure_flags & 1 != 0 {
            crate::server_log!(warning, "SIMULATE-FAILURE: crashing after election");
            ctx.set_shutdown_requested(true);
            return Value::ok();
        }
        if state.simfailure_flags & 2 != 0 {
            crate::server_log!(warning, "SIMULATE-FAILURE: crashing after promotion");
            ctx.set_shutdown_requested(true);
            return Value::ok();
        }
    }
    ctx.sentinel_state_mut()
        .registry
        .replace_master(name, master)
        .expect("failover updates an existing configured master");
    Value::ok()
}

/// SENTINEL IS-MASTER-DOWN-BY-ADDR <ip> <port> <current_epoch> <runid>
pub fn is_master_down_by_addr(
    _conn: &mut dyn CommandConn,
    args: &CommandArgs,
    ctx: &mut ServerContext,
) -> Value {
    let request = match parse_down_request(args) {
        Ok(request) => request,
        Err(error) => return error,
    };
    if ctx.sentinel_state_mut().tilt {
        return empty_down_reply();
    }
    let name = match master_name_for_addr(ctx, &request.addr) {
        Some(name) => name,
        None => return empty_down_reply(),
    };
    let master_id = ctx.sentinel_state().registry.master(&name).unwrap().id;
    crate::framework::distributed::sentinel::timer::check_sdown(
        ctx,
        master_id,
        crate::foundation::io::sys::time::mstime(),
    );
    let mut master = ctx
        .sentinel_state()
        .registry
        .take_master_snapshot(&name)
        .unwrap();
    let now = crate::foundation::io::sys::time::mstime();
    let down_state = reported_down(ctx, &master, now);
    let (leader, leader_epoch) = record_requested_vote(ctx, &mut master, &request, now);
    ctx.sentinel_state_mut()
        .registry
        .replace_master(name, master)
        .expect("vote updates an existing configured master");
    Value::Array(vec![
        Value::Integer(down_state),
        Value::bulk(leader.into_bytes()),
        Value::Integer(leader_epoch),
    ])
}

fn master_name_for_addr(ctx: &ServerContext, addr: &str) -> Option<String> {
    ctx.sentinel_state()
        .registry
        .masters()
        .find(|(_, master)| master.addr == addr)
        .map(|(name, _)| name.to_string())
}

fn reported_down(ctx: &ServerContext, master: &SentinelInstance, now: i64) -> i64 {
    let instance = ctx.sentinel_state().registry.instances().get(&master.id);
    i64::from(is_primary_down(
        master.down_state,
        instance
            .map(|instance| instance.down_state)
            .unwrap_or(DownState::Up),
        master.down_after_ms,
        instance.map(|instance| instance.act_ping_time).unwrap_or(0),
        instance.map(|instance| instance.last_ping_ok).unwrap_or(0),
        instance.map(|instance| instance.link_state).unwrap_or(1),
        now,
    ))
}

fn record_requested_vote(
    ctx: &mut ServerContext,
    master: &mut SentinelInstance,
    request: &DownRequest,
    now: i64,
) -> (String, i64) {
    if request.epoch > ctx.sentinel_state().current_epoch {
        ctx.sentinel_state_mut().current_epoch = request.epoch;
    }
    if request.leader == "*" {
        return ("*".to_string(), master.leader_epoch);
    }
    vote_for_leader(
        ctx.sentinel_state_mut(),
        master,
        request.epoch,
        &request.leader,
        now,
    )
}

struct DownRequest {
    addr: String,
    epoch: i64,
    leader: String,
}

fn parse_down_request(args: &CommandArgs) -> Result<DownRequest, Value> {
    if args.args.len() < 5 {
        return Err(CommandError::wrong_arity("sentinel|is-master-down-by-addr"));
    }
    let host = String::from_utf8_lossy(&args.args[1]);
    let port = String::from_utf8_lossy(&args.args[2])
        .parse::<u16>()
        .map_err(|_| Value::error("ERR Invalid port"))?;
    Ok(DownRequest {
        addr: format_sentinel_endpoint(&host, port),
        epoch: String::from_utf8_lossy(&args.args[3]).parse().unwrap_or(0),
        leader: String::from_utf8_lossy(&args.args[4]).to_string(),
    })
}

fn empty_down_reply() -> Value {
    Value::Array(vec![
        Value::Integer(0),
        Value::bulk(Bytes::from_static(b"*")),
        Value::Integer(0),
    ])
}

/// SENTINEL SIMULATE-FAILURE [CRASH-AFTER-ELECTION] [CRASH-AFTER-PROMOTION] [HELP]
pub fn simulate_failure(
    _conn: &mut dyn CommandConn,
    args: &CommandArgs,
    ctx: &mut ServerContext,
) -> Value {
    const CRASH_AFTER_ELECTION: u32 = 1 << 0;
    const CRASH_AFTER_PROMOTION: u32 = 1 << 1;

    if args.args.len() == 1 {
        ctx.sentinel_state_mut().simfailure_flags = 0;
        return Value::ok();
    }

    let mut flags: u32 = 0;
    for i in 1..args.args.len() {
        let arg = String::from_utf8_lossy(&args.args[i]).to_lowercase();
        match arg.as_str() {
            "crash-after-election" => {
                flags |= CRASH_AFTER_ELECTION;
            }
            "crash-after-promotion" => {
                flags |= CRASH_AFTER_PROMOTION;
            }
            "help" => {
                return Value::Array(vec![
                    Value::bulk(Bytes::from_static(b"crash-after-election")),
                    Value::bulk(Bytes::from_static(b"crash-after-promotion")),
                ]);
            }
            _ => {
                return Value::error("Unknown failure simulation specified");
            }
        }
    }

    ctx.sentinel_state_mut().simfailure_flags = flags;
    Value::ok()
}

#[cfg(test)]
#[path = "control_tests.rs"]
mod tests;
