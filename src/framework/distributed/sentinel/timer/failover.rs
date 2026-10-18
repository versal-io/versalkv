use crate::engine::distributed::sentinel::failover::{
    advance_failover, find_misconfigured_replica, promotion_sent, start_failover, FailoverAction,
};
use crate::engine::distributed::sentinel::script::{ScriptType, SentinelScriptEngine};
use crate::engine::distributed::sentinel::{
    split_sentinel_endpoint, DownState, FailoverState, SentinelInstance, SentinelInstanceId,
    SentinelRegistry,
};
use crate::framework::commands::sentinel::admin_cmds::flushconfig_internal;
use crate::framework::context::ServerContext;

use super::super::transport::{
    authenticate_stream, connect_command_stream, read_ok, sentinel_kill_clients, write_command,
};

#[derive(Clone, Copy)]
struct DriveParams {
    now: i64,
    election_timeout: i64,
    reconf_timeout: i64,
    ask_period: i64,
    simfailure_flags: u32,
}

pub(super) fn drive_failovers(ctx: &mut ServerContext, now: i64) {
    let state = ctx.sentinel_state_mut();
    let myid = state.myid.clone();
    let params = DriveParams {
        now,
        election_timeout: state.tunables.election_timeout_ms,
        reconf_timeout: state.tunables.replica_reconf_timeout_ms,
        ask_period: state.tunables.ask_period_ms,
        simfailure_flags: state.simfailure_flags,
    };
    let names: Vec<String> = state.registry.master_names().cloned().collect();
    let mut should_flush_config = false;
    for name in names {
        let mut master = match state.registry.take_master_snapshot(&name) {
            Some(master) => master,
            None => continue,
        };
        if !start_if_needed(&mut state.current_epoch, &mut master, params) {
            state
                .registry
                .replace_master(name, master)
                .expect("failover updates an existing configured master");
            continue;
        }
        let action = advance_failover(
            &myid,
            state.current_epoch,
            params.election_timeout,
            params.reconf_timeout,
            params.simfailure_flags,
            &mut master,
            state.registry.instances(),
            params.now,
        );
        if let Some(action) = action {
            execute_action(
                &mut master,
                &mut state.registry,
                &mut state.script_engine,
                action,
                params,
            );
        }
        let should_flush = finish_if_ready(
            &mut master,
            &mut state.registry,
            &mut state.script_engine,
            params.now,
        );
        should_flush_config |= should_flush;
        if master.failover_state == FailoverState::None {
            repair_idle_topology(&master, &mut state.registry, params);
        }
        state
            .registry
            .replace_master(name, master)
            .expect("failover updates an existing configured master");
    }
    if should_flush_config && flushconfig_internal(state).is_err() {
        crate::server_log!(warning, "SENTINEL: failed to flush config after failover");
    }
}

fn start_if_needed(
    current_epoch: &mut i64,
    master: &mut SentinelInstance,
    params: DriveParams,
) -> bool {
    if master.down_state != DownState::ODown || master.failover_state != FailoverState::None {
        return true;
    }
    if master.failover_start_time != 0 {
        let backoff_ms = master.failover_timeout * 2;
        if params.now - master.failover_start_time < backoff_ms {
            return false;
        }
    }
    start_failover(current_epoch, params.ask_period, master, params.now);
    true
}

fn execute_action(
    master: &mut SentinelInstance,
    registry: &mut SentinelRegistry,
    scripts: &mut SentinelScriptEngine,
    action: FailoverAction,
    params: DriveParams,
) {
    match action {
        FailoverAction::Promote(replica_id) => {
            promote_replica(master, registry.instances(), scripts, replica_id, params)
        }
        FailoverAction::Reconfigure(replica_id) => {
            reconfigure_replica(master, registry, replica_id, params.now)
        }
        FailoverAction::SendFailover(_) => {
            send_coordinated(master, registry.instances(), scripts, params)
        }
        FailoverAction::HandoffClients(replica_id) => {
            handoff_coordinated_clients(master, registry.instances(), replica_id)
        }
    }
}

fn handoff_coordinated_clients(
    master: &SentinelInstance,
    instances: &std::collections::HashMap<SentinelInstanceId, SentinelInstance>,
    promoted_id: SentinelInstanceId,
) {
    let (user, pass) = master_auth(master);
    sentinel_kill_clients(&master.addr, user, pass);
    if let Some(promoted) = instances.get(&promoted_id) {
        sentinel_kill_clients(&promoted.addr, user, pass);
    }
}

fn promote_replica(
    master: &mut SentinelInstance,
    instances: &std::collections::HashMap<SentinelInstanceId, SentinelInstance>,
    scripts: &mut SentinelScriptEngine,
    replica_id: SentinelInstanceId,
    params: DriveParams,
) {
    crash_after_promotion(params.simfailure_flags);
    let Some(addr) = instances
        .get(&replica_id)
        .map(|replica| replica.addr.clone())
    else {
        return;
    };
    let (user, pass) = master_auth(master);
    if send_replicaof(&addr, None, user, pass, false).is_err() {
        return;
    }
    enqueue_notification(master, scripts);
    sentinel_kill_clients(&addr, user, pass);
    if let Some(primary) = instances.get(&master.id) {
        sentinel_kill_clients(&primary.addr, user, pass);
    }
    promotion_sent(master, params.now);
}

fn reconfigure_replica(
    master: &SentinelInstance,
    registry: &mut SentinelRegistry,
    replica_id: SentinelInstanceId,
    now: i64,
) {
    let addr = registry
        .instances()
        .get(&replica_id)
        .map(|replica| replica.addr.clone());
    let primary = master
        .promoted_replica
        .and_then(|id| registry.instance(id).map(|replica| replica.addr.clone()))
        .unwrap_or_else(|| master.addr.clone());
    let should_abort = registry.instance(replica_id).is_some_and(|replica| {
        replica.monitored_instance_failover_state
            == crate::engine::distributed::sentinel::MONITORED_INSTANCE_FAILOVER
    });
    let (user, pass) = master_auth(master);
    if let Some(addr) = addr {
        if send_replicaof(&addr, Some(&primary), user, pass, should_abort).is_ok() {
            if let Some(replica) = registry.instance_mut(replica_id) {
                replica.reconfig_sent = true;
                replica.reconfig_sent_time = now;
            }
        }
    }
}

fn send_coordinated(
    master: &mut SentinelInstance,
    instances: &std::collections::HashMap<SentinelInstanceId, SentinelInstance>,
    scripts: &mut SentinelScriptEngine,
    params: DriveParams,
) {
    crash_after_promotion(params.simfailure_flags);
    let Some(target) = master
        .promoted_replica
        .and_then(|id| instances.get(&id))
        .map(|replica| replica.addr.clone())
    else {
        return;
    };
    let (user, pass) = master_auth(master);
    if send_failover_to(&master.addr, &target, master.failover_timeout, user, pass).is_ok() {
        promotion_sent(master, params.now);
        enqueue_notification(master, scripts);
    }
}

fn finish_if_ready(
    master: &mut SentinelInstance,
    registry: &mut SentinelRegistry,
    scripts: &mut SentinelScriptEngine,
    now: i64,
) -> bool {
    if master.failover_state != FailoverState::UpdateConfig {
        return false;
    }
    let old_addr = master.addr.clone();
    registry.finish_failover(master, now);
    if master.addr == old_addr {
        return false;
    }
    if !master.client_reconfig_script.is_empty() {
        let (from_ip, from_port) = endpoint_script_parts(&old_addr);
        let (to_ip, to_port) = endpoint_script_parts(&master.addr);
        let args = vec![
            master.client_reconfig_script.clone(),
            "client-reconfig".to_string(),
            master.name.clone(),
            "leader".to_string(),
            "switch-master".to_string(),
            from_ip,
            from_port,
            to_ip,
            to_port,
        ];
        scripts.enqueue(
            ScriptType::ClientReconfig,
            &master.client_reconfig_script,
            args,
        );
    }
    crate::engine::distributed::sentinel::event::sentinel_event(
        scripts,
        "warning",
        "+switch-master",
        registry.instance(master.id),
        &format!("{} {}", master.name, master.addr),
    );
    crate::engine::distributed::sentinel::event::sentinel_event(
        scripts,
        "notice",
        "+failover-end",
        registry.instance(master.id),
        &master.name,
    );
    true
}

fn endpoint_script_parts(addr: &str) -> (String, String) {
    split_sentinel_endpoint(addr)
        .map(|endpoint| (endpoint.host.to_string(), endpoint.port.to_string()))
        .unwrap_or_else(|| (addr.to_string(), String::new()))
}

fn repair_idle_topology(
    master: &SentinelInstance,
    registry: &mut SentinelRegistry,
    params: DriveParams,
) {
    for id in &master.replicas {
        if let Some(replica) = registry.instance_mut(*id) {
            if replica.reconfig_sent
                && params.now - replica.reconfig_sent_time > params.reconf_timeout
            {
                replica.reconfig_sent = false;
                replica.reconfig_sent_time = 0;
            }
        }
    }
    if let Some(replica_id) = find_misconfigured_replica(master, registry.instances(), params.now) {
        reconfigure_replica(master, registry, replica_id, params.now);
    }
}

fn master_auth(master: &SentinelInstance) -> (Option<&str>, Option<&str>) {
    (
        (!master.auth_user.is_empty()).then_some(master.auth_user.as_str()),
        (!master.auth_pass.is_empty()).then_some(master.auth_pass.as_str()),
    )
}

fn enqueue_notification(master: &SentinelInstance, scripts: &mut SentinelScriptEngine) {
    if master.notification_script.is_empty() {
        return;
    }
    let args = vec![
        master.notification_script.clone(),
        "notification".to_string(),
        master.name.clone(),
        master.addr.clone(),
        "failover".to_string(),
    ];
    scripts.enqueue(ScriptType::Notification, &master.notification_script, args);
}

fn crash_after_promotion(flags: u32) {
    if flags & 2 != 0 {
        crate::server_log!(warning, "SIMULATE-FAILURE: crashing after promotion");
        // （"Port 20000 does not return available" / "Address already in use"）
        std::process::exit(99);
    }
}

pub(super) fn collect_pending_actions(
    ctx: &ServerContext,
    id: SentinelInstanceId,
    _now: i64,
    to_check_sdown: &mut Vec<SentinelInstanceId>,
) {
    let exists = ctx.sentinel_state().registry.instances().contains_key(&id);
    if exists {
        to_check_sdown.push(id);
    }
}

fn send_replicaof(
    addr: &str,
    primary_addr: Option<&str>,
    auth_user: Option<&str>,
    auth_pass: Option<&str>,
    should_abort_failover: bool,
) -> Result<(), String> {
    let mut stream = connect_command_stream(addr)?;
    authenticate_stream(&mut stream, auth_user, auth_pass)?;
    if should_abort_failover {
        write_command(&mut stream, &["FAILOVER", "ABORT"])?;
        let _ = read_ok(&mut stream);
    }
    let args = replicaof_args(primary_addr)?;
    let refs: Vec<&str> = args.iter().map(String::as_str).collect();
    write_command(&mut stream, &refs)?;
    read_ok(&mut stream)
}

fn replicaof_args(primary_addr: Option<&str>) -> Result<Vec<String>, String> {
    match primary_addr {
        Some(primary) => {
            let endpoint = split_sentinel_endpoint(primary)
                .ok_or_else(|| format!("Invalid primary address '{}'", primary))?;
            Ok(vec![
                "SLAVEOF".to_string(),
                endpoint.host.to_string(),
                endpoint.port.to_string(),
            ])
        }
        None => Ok(vec!["SLAVEOF".into(), "NO".into(), "ONE".into()]),
    }
}

fn send_failover_to(
    primary_addr: &str,
    target_addr: &str,
    timeout_ms: i64,
    auth_user: Option<&str>,
    auth_pass: Option<&str>,
) -> Result<(), String> {
    let mut stream = connect_command_stream(primary_addr)?;
    authenticate_stream(&mut stream, auth_user, auth_pass)?;
    let target = split_sentinel_endpoint(target_addr)
        .ok_or_else(|| format!("Invalid target addr '{}'", target_addr))?;
    let target_port = target.port.to_string();
    let timeout = timeout_ms.to_string();
    write_command(&mut stream, &["MULTI"])?;
    write_command(&mut stream, &["CLIENT", "PAUSE", timeout.as_str(), "WRITE"])?;
    write_command(
        &mut stream,
        &[
            "FAILOVER",
            "TO",
            target.host,
            target_port.as_str(),
            "TIMEOUT",
            timeout.as_str(),
        ],
    )?;
    write_command(&mut stream, &["EXEC"])?;
    read_ok(&mut stream)
}

#[cfg(test)]
mod tests {
    use super::{replicaof_args, send_failover_to, send_replicaof};
    use crate::framework::distributed::sentinel::transport::{
        send_script_kill, sentinel_kill_clients,
    };
    use std::io::{Read, Write};
    use std::net::TcpListener;

    fn capture_request(run: impl FnOnce(String)) -> Vec<u8> {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind capture listener");
        let addr = listener.local_addr().expect("capture listener address");
        let handle = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("accept command connection");
            stream
                .set_read_timeout(Some(std::time::Duration::from_secs(2)))
                .expect("bound capture pipeline read");
            let mut request = Vec::new();
            let mut parser = crate::foundation::io::protocol::parser::Parser::new();
            let mut bytes = [0u8; 4096];
            loop {
                let read = stream.read(&mut bytes).expect("read command pipeline");
                assert!(read > 0, "command pipeline closed before EXEC");
                request.extend_from_slice(&bytes[..read]);
                parser.feed(&bytes[..read]);
                while let Some(command) = parser.try_parse().expect("parse capture command") {
                    if command
                        .first()
                        .is_some_and(|name| name.eq_ignore_ascii_case(b"EXEC"))
                    {
                        stream
                            .write_all(b"+OK\r\n")
                            .expect("reply to complete pipeline");
                        return request;
                    }
                }
            }
        });

        run(addr.to_string());
        handle.join().expect("join capture server")
    }

    fn capture_hostname_request<R>(run: impl FnOnce(String) -> R) -> (R, Vec<u8>) {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind capture listener");
        let port = listener
            .local_addr()
            .expect("capture listener address")
            .port();
        listener
            .set_nonblocking(true)
            .expect("set capture listener nonblocking");
        let handle = std::thread::spawn(move || {
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
            loop {
                match listener.accept() {
                    Ok((mut stream, _)) => {
                        stream
                            .set_nonblocking(false)
                            .expect("capture accepted stream is blocking");
                        stream
                            .set_read_timeout(Some(std::time::Duration::from_secs(2)))
                            .expect("set capture read timeout");
                        let mut request = Vec::new();
                        let mut buffer = [0_u8; 4096];
                        let mut parser = crate::foundation::io::protocol::parser::Parser::new();
                        let mut is_transaction = false;
                        loop {
                            let read = stream
                                .read(&mut buffer)
                                .expect("read complete capture command");
                            assert!(read > 0, "capture peer closed before command completed");
                            request.extend_from_slice(&buffer[..read]);
                            parser.feed(&buffer[..read]);
                            while let Some(command) =
                                parser.try_parse().expect("parse capture command")
                            {
                                let name = command.first().expect("capture command has a name");
                                if name.eq_ignore_ascii_case(b"MULTI") {
                                    is_transaction = true;
                                }
                                if !is_transaction || name.eq_ignore_ascii_case(b"EXEC") {
                                    stream
                                        .write_all(b"+OK\r\n")
                                        .expect("reply to capture command");
                                    return request;
                                }
                            }
                        }
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        if std::time::Instant::now() >= deadline {
                            return Vec::new();
                        }
                        std::thread::sleep(std::time::Duration::from_millis(10));
                    }
                    Err(error) => panic!("accept command connection: {error}"),
                }
            }
        });
        let result = run(format!("localhost:{}", port));
        (result, handle.join().expect("join capture server"))
    }

    #[test]
    fn sentinel_uses_slaveof_for_legacy_acl_compatibility() {
        assert_eq!(replicaof_args(None).unwrap(), vec!["SLAVEOF", "NO", "ONE"]);
        assert_eq!(
            replicaof_args(Some("127.0.0.1:6380")).unwrap(),
            vec!["SLAVEOF", "127.0.0.1", "6380"]
        );
        assert_eq!(
            replicaof_args(Some("[::1]:6380")).unwrap(),
            vec!["SLAVEOF", "::1", "6380"]
        );
    }

    #[test]
    fn coordinated_failover_pauses_writes_until_client_handoff() {
        let request = capture_request(|primary_addr| {
            send_failover_to(&primary_addr, "127.0.0.1:6380", 180_000, None, None)
                .expect("send coordinated failover");
        });

        assert_eq!(
            request,
            b"*1\r\n$5\r\nMULTI\r\n\
*4\r\n$6\r\nCLIENT\r\n$5\r\nPAUSE\r\n$6\r\n180000\r\n$5\r\nWRITE\r\n\
*6\r\n$8\r\nFAILOVER\r\n$2\r\nTO\r\n$9\r\n127.0.0.1\r\n$4\r\n6380\r\n$7\r\nTIMEOUT\r\n$6\r\n180000\r\n\
*1\r\n$4\r\nEXEC\r\n"
        );
    }

    #[test]
    fn coordinated_failover_ipv6_target_uses_bare_host_argument() {
        let request = capture_request(|primary_addr| {
            send_failover_to(&primary_addr, "[::1]:6380", 180_000, None, None)
                .expect("send coordinated failover");
        });

        assert!(request
            .windows(b"$3\r\n::1\r\n".len())
            .any(|window| { window == b"$3\r\n::1\r\n" }));
        assert!(!request
            .windows(b"[::1]".len())
            .any(|window| window == b"[::1]"));
    }

    #[test]
    fn coordinated_client_handoff_kills_clients_then_unpauses_writes() {
        let request = capture_request(|addr| {
            sentinel_kill_clients(&addr, None, None);
        });

        assert_eq!(
            request,
            b"*1\r\n$5\r\nMULTI\r\n\
*2\r\n$6\r\nCONFIG\r\n$7\r\nREWRITE\r\n\
*4\r\n$6\r\nCLIENT\r\n$4\r\nKILL\r\n$4\r\nTYPE\r\n$6\r\nnormal\r\n\
*4\r\n$6\r\nCLIENT\r\n$4\r\nKILL\r\n$4\r\nTYPE\r\n$6\r\npubsub\r\n\
*2\r\n$6\r\nCLIENT\r\n$7\r\nUNPAUSE\r\n\
*1\r\n$4\r\nEXEC\r\n"
        );
    }

    #[test]
    fn hostname_slaveof_command_connects() {
        let (slaveof, slaveof_request) = capture_hostname_request(|addr| {
            send_replicaof(&addr, Some("localhost:6380"), None, None, false)
        });
        assert!(slaveof.is_ok());
        assert!(slaveof_request
            .windows(b"SLAVEOF".len())
            .any(|w| w == b"SLAVEOF"));
    }

    #[test]
    fn hostname_coordinated_failover_command_connects() {
        let (failover, failover_request) = capture_hostname_request(|addr| {
            send_failover_to(&addr, "localhost:6380", 180_000, None, None)
        });
        assert!(failover.is_ok());
        assert!(failover_request
            .windows(b"FAILOVER".len())
            .any(|w| w == b"FAILOVER"));
    }

    #[test]
    fn hostname_script_kill_command_connects() {
        let (script_kill, script_request) =
            capture_hostname_request(|addr| send_script_kill(&addr));
        assert!(script_kill.is_ok());
        assert!(script_request
            .windows(b"SCRIPT".len())
            .any(|w| w == b"SCRIPT"));
    }

    #[test]
    fn hostname_client_handoff_connects() {
        let (_, handoff_request) = capture_hostname_request(|addr| {
            sentinel_kill_clients(&addr, None, None);
        });
        assert!(handoff_request
            .windows(b"MULTI".len())
            .any(|w| w == b"MULTI"));
    }
}
