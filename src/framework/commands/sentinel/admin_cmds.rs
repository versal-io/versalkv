use std::collections::HashSet;
use std::fs;
use std::io::{BufRead, BufReader};
use std::path::Path;

use crate::engine::command::context::CommandConn;
use crate::engine::distributed::sentinel::{split_sentinel_endpoint, SentinelState};
use crate::foundation::io::protocol::Value;
use crate::framework::context::ServerContext;
use crate::framework::CommandArgs;

#[path = "admin_config.rs"]
mod admin_config;
#[path = "admin_debug.rs"]
mod admin_debug;
#[path = "admin_reset.rs"]
mod admin_reset;

pub fn flushconfig(
    _conn: &mut dyn CommandConn,
    _args: &CommandArgs,
    ctx: &mut ServerContext,
) -> Value {
    match flushconfig_internal(ctx.sentinel_state_mut()) {
        Ok(_) => Value::ok(),
        Err(_) => Value::error("ERR Failed to save config file. Check server logs."),
    }
}

pub fn flushconfig_internal(state: &mut SentinelState) -> std::result::Result<(), ()> {
    let config_path = state.config_file.clone();
    if config_path.is_empty() {
        return Err(());
    }

    let new_content = rewrite_config(state, &config_path)?;

    let tmp_path = format!("{}.tmp", config_path);
    fs::write(&tmp_path, &new_content).map_err(|_| ())?;
    fs::rename(&tmp_path, &config_path).map_err(|_| {
        let _ = fs::remove_file(&tmp_path);
    })?;

    Ok(())
}

fn rewrite_config(state: &SentinelState, config_path: &str) -> Result<String, ()> {
    let config_lines = generate_config_lines(state);

    let original_lines = if Path::new(config_path).exists() {
        let file = fs::File::open(config_path).map_err(|_| ())?;
        let reader = BufReader::new(file);
        reader
            .lines()
            .collect::<Result<Vec<_>, _>>()
            .map_err(|_| ())?
    } else {
        Vec::new()
    };

    let mut processed: HashSet<String> = HashSet::new();

    let mut output: Vec<String> = Vec::new();

    for line in &original_lines {
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') || trimmed.starts_with(';') {
            output.push(line.clone());
            continue;
        }

        let directive = extract_directive_key(trimmed);
        if let Some(key) = directive {
            if processed.contains(&key) {
                continue;
            }
            if let Some(new_line) = config_lines.get(&key) {
                processed.insert(key);
                output.push(new_line.clone());
            }
            continue;
        }

        output.push(line.clone());
    }

    for (key, line) in &config_lines {
        if !processed.contains(key) {
            output.push(line.clone());
        }
    }

    if let Some(last) = output.last() {
        if !last.ends_with('\n') {
            if let Some(last) = output.last_mut() {
                last.push('\n');
            }
        }
    } else {
        output.push(String::new());
    }

    Ok(output.join("\n") + "\n")
}

fn is_per_master_directive(directive: &str) -> bool {
    // Legacy non-C directives stay recognizable here only so CONFIG REWRITE
    // removes them from existing files instead of preserving unloadable lines.
    matches!(
        directive,
        "monitor"
            | "down-after-milliseconds"
            | "down-after-millis"
            | "failover-timeout"
            | "parallel-syncs"
            | "notification-script"
            | "client-reconfig-script"
            | "auth-pass"
            | "auth-user"
            | "config-epoch"
            | "leader-epoch"
            | "leader"
            | "known-replica"
            | "known-slave"
            | "known-sentinel"
            | "rename-command"
            | "primary-reboot-down-after-period"
            | "master-reboot-down-after-period"
    )
}

fn extract_directive_key(line: &str) -> Option<String> {
    let parts: Vec<&str> = line.split_whitespace().collect();
    if parts.len() < 2 {
        return None;
    }
    if parts[0] != "sentinel" {
        return None;
    }
    let directive = parts[1].to_lowercase();
    if matches!(directive.as_str(), "known-replica" | "known-slave") {
        if parts.len() < 5 {
            return None;
        }
        return Some(format!(
            "sentinel known-replica {} {} {}",
            parts[2], parts[3], parts[4]
        ));
    }
    if directive == "known-sentinel" {
        if parts.len() < 6 {
            return None;
        }
        return Some(format!(
            "sentinel known-sentinel {} {} {} {}",
            parts[2], parts[3], parts[4], parts[5]
        ));
    }
    if is_per_master_directive(&directive) {
        // per-master: key = "sentinel <directive> <master_name>"
        let master_name = parts.get(2).copied().unwrap_or("");
        if master_name.is_empty() {
            return None;
        }
        Some(format!("sentinel {} {}", directive, master_name))
    } else {
        Some(format!("sentinel {}", directive))
    }
}

fn config_repr(value: &str) -> String {
    let mut repr = String::with_capacity(value.len() + 2);
    repr.push('"');
    for byte in value.bytes() {
        match byte {
            b'\\' | b'"' => {
                repr.push('\\');
                repr.push(byte as char);
            }
            b'\n' => repr.push_str("\\n"),
            b'\r' => repr.push_str("\\r"),
            b'\t' => repr.push_str("\\t"),
            0x07 => repr.push_str("\\a"),
            0x08 => repr.push_str("\\b"),
            b' '..=b'~' => repr.push(byte as char),
            _ => repr.push_str(&format!("\\x{byte:02x}")),
        }
    }
    repr.push('"');
    repr
}

fn config_address_parts(addr: &str) -> (String, String) {
    split_sentinel_endpoint(addr)
        .map(|endpoint| (endpoint.host.to_string(), endpoint.port.to_string()))
        .unwrap_or_else(|| (addr.to_string(), "0".to_string()))
}

fn generate_config_lines(state: &SentinelState) -> std::collections::HashMap<String, String> {
    let mut lines = std::collections::HashMap::new();

    // sentinel myid
    lines.insert(
        "sentinel myid".to_string(),
        format!("sentinel myid {}", state.myid),
    );

    // sentinel current-epoch
    lines.insert(
        "sentinel current-epoch".to_string(),
        format!("sentinel current-epoch {}", state.current_epoch),
    );

    for (name, master) in state.registry.masters() {
        let (host, port) = config_address_parts(&master.addr);

        // sentinel monitor
        lines.insert(
            format!("sentinel monitor {}", name),
            format!(
                "sentinel monitor {} {} {} {}",
                name, host, port, master.quorum
            ),
        );

        // sentinel down-after-milliseconds
        lines.insert(
            format!("sentinel down-after-milliseconds {}", name),
            format!(
                "sentinel down-after-milliseconds {} {}",
                name, master.down_after_ms
            ),
        );

        // sentinel parallel-syncs
        lines.insert(
            format!("sentinel parallel-syncs {}", name),
            format!("sentinel parallel-syncs {} {}", name, master.parallel_syncs),
        );

        // sentinel failover-timeout
        lines.insert(
            format!("sentinel failover-timeout {}", name),
            format!(
                "sentinel failover-timeout {} {}",
                name, master.failover_timeout
            ),
        );

        // sentinel config-epoch
        lines.insert(
            format!("sentinel config-epoch {}", name),
            format!("sentinel config-epoch {} {}", name, master.config_epoch),
        );

        // sentinel leader-epoch
        lines.insert(
            format!("sentinel leader-epoch {}", name),
            format!("sentinel leader-epoch {} {}", name, master.leader_epoch),
        );

        // sentinel notification-script (if set)
        if !master.notification_script.is_empty() {
            lines.insert(
                format!("sentinel notification-script {}", name),
                format!(
                    "sentinel notification-script {} {}",
                    name,
                    config_repr(&master.notification_script)
                ),
            );
        }

        // sentinel client-reconfig-script (if set)
        if !master.client_reconfig_script.is_empty() {
            lines.insert(
                format!("sentinel client-reconfig-script {}", name),
                format!(
                    "sentinel client-reconfig-script {} {}",
                    name,
                    config_repr(&master.client_reconfig_script)
                ),
            );
        }

        // sentinel auth-pass (if set)
        if !master.auth_pass.is_empty() {
            lines.insert(
                format!("sentinel auth-pass {}", name),
                format!(
                    "sentinel auth-pass {} {}",
                    name,
                    config_repr(&master.auth_pass)
                ),
            );
        }

        // sentinel auth-user (if set)
        if !master.auth_user.is_empty() {
            lines.insert(
                format!("sentinel auth-user {}", name),
                format!(
                    "sentinel auth-user {} {}",
                    name,
                    config_repr(&master.auth_user)
                ),
            );
        }

        // sentinel primary-reboot-down-after-period (if set)
        if master.primary_reboot_down_after_period > 0 {
            lines.insert(
                format!("sentinel primary-reboot-down-after-period {}", name),
                format!(
                    "sentinel primary-reboot-down-after-period {} {}",
                    name, master.primary_reboot_down_after_period
                ),
            );
        }

        // sentinel known-replica (for each replica)
        for rid in &master.replicas {
            if let Some(replica) = state.registry.instances().get(rid) {
                let (rhost, rport) = config_address_parts(&replica.addr);
                let line = format!("sentinel known-replica {} {} {}", name, rhost, rport);
                let unique_key = format!("sentinel known-replica {} {} {}", name, rhost, rport);
                lines.insert(unique_key, line);
            }
        }

        // sentinel known-sentinel (for each sentinel)
        for sid in &master.sentinels {
            if let Some(sentinel) = state.registry.instances().get(sid) {
                let (shost, sport) = config_address_parts(&sentinel.addr);
                let runid = &sentinel.runid;
                let unique_key = format!(
                    "sentinel known-sentinel {} {} {} {}",
                    name, shost, sport, runid
                );
                let line = format!(
                    "sentinel known-sentinel {} {} {} {}",
                    name, shost, sport, runid
                );
                lines.insert(unique_key, line);
            }
        }
    }

    // sentinel announce-ip (if set)
    if !state.announce_ip.is_empty() {
        lines.insert(
            "sentinel announce-ip".to_string(),
            format!("sentinel announce-ip {}", config_repr(&state.announce_ip)),
        );
    }

    // sentinel announce-port (if set)
    if state.announce_port != 0 {
        lines.insert(
            "sentinel announce-port".to_string(),
            format!("sentinel announce-port {}", state.announce_port),
        );
    }

    // sentinel sentinel-user (if set)
    if !state.sentinel_auth_user.is_empty() {
        lines.insert(
            "sentinel sentinel-user".to_string(),
            format!(
                "sentinel sentinel-user {}",
                config_repr(&state.sentinel_auth_user)
            ),
        );
    }

    // sentinel sentinel-pass (if set)
    if !state.sentinel_auth_pass.is_empty() {
        lines.insert(
            "sentinel sentinel-pass".to_string(),
            format!(
                "sentinel sentinel-pass {}",
                config_repr(&state.sentinel_auth_pass)
            ),
        );
    }

    // sentinel resolve-hostnames
    lines.insert(
        "sentinel resolve-hostnames".to_string(),
        format!(
            "sentinel resolve-hostnames {}",
            if state.resolve_hostnames { "yes" } else { "no" }
        ),
    );

    // sentinel announce-hostnames
    lines.insert(
        "sentinel announce-hostnames".to_string(),
        format!(
            "sentinel announce-hostnames {}",
            if state.announce_hostnames {
                "yes"
            } else {
                "no"
            }
        ),
    );

    // sentinel deny-scripts-reconfig
    lines.insert(
        "sentinel deny-scripts-reconfig".to_string(),
        format!(
            "sentinel deny-scripts-reconfig {}",
            if state.deny_scripts_reconfig {
                "yes"
            } else {
                "no"
            }
        ),
    );

    lines
}

/// SENTINEL RESET <pattern>
pub fn reset(_conn: &mut dyn CommandConn, args: &CommandArgs, ctx: &mut ServerContext) -> Value {
    admin_reset::reset(_conn, args, ctx)
}

/// SENTINEL MYID
pub fn myid(_conn: &mut dyn CommandConn, _args: &CommandArgs, ctx: &mut ServerContext) -> Value {
    Value::bulk(ctx.sentinel_state_mut().myid.as_bytes().to_vec())
}

/// SENTINEL CONFIG GET/SET <param>
/// Supported parameters: announce-ip, sentinel-user, sentinel-pass,
/// resolve-hostnames, announce-port, announce-hostnames, loglevel,
/// deny-scripts-reconfig
pub fn config(_conn: &mut dyn CommandConn, args: &CommandArgs, ctx: &mut ServerContext) -> Value {
    admin_config::config(_conn, args, ctx)
}

/// SENTINEL DEBUG [<option> <value> ...]
/// Without arguments, returns a map of all debug tunable parameters.
/// With option-value pairs, sets the specified parameters.
/// Supported options: info-period, ping-period, ask-period, publish-period,
/// default-down-after, tilt-trigger, tilt-period, slave-reconf-timeout,
/// min-link-reconnect-period, default-failover-timeout, election-timeout,
/// script-max-runtime, script-retry-delay
pub fn debug(_conn: &mut dyn CommandConn, args: &CommandArgs, ctx: &mut ServerContext) -> Value {
    admin_debug::debug(_conn, args, ctx)
}

#[cfg(test)]
#[path = "admin_rewrite_tests.rs"]
mod admin_rewrite_tests;
