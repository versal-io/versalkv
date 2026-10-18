use crate::engine::command::context::CommandConn;
use crate::engine::distributed::sentinel::{DownState, SentinelInstance, SentinelState};
use crate::foundation::io::protocol::Value;
use crate::framework::context::ServerContext;
use crate::framework::{CommandArgs, CommandError};

pub(super) fn sync_master_config(source: &SentinelInstance, target: &mut SentinelInstance) {
    target.down_after_ms = source.down_after_ms;
    target.quorum = source.quorum;
    target.parallel_syncs = source.parallel_syncs;
    target.failover_timeout = source.failover_timeout;
    target
        .notification_script
        .clone_from(&source.notification_script);
    target
        .client_reconfig_script
        .clone_from(&source.client_reconfig_script);
    target.auth_pass.clone_from(&source.auth_pass);
    target.auth_user.clone_from(&source.auth_user);
    target.primary_reboot_down_after_period = source.primary_reboot_down_after_period;
    target.renamed_commands.clone_from(&source.renamed_commands);
}

pub(super) fn reset_instance_health_after_auth_change(
    state: &mut SentinelState,
    master_name: &str,
) {
    let Some(master) = state.registry.master(master_name).cloned() else {
        return;
    };
    let mut instance_ids = master.replicas.clone();
    instance_ids.push(master.id);
    let addresses: Vec<String> = instance_ids
        .iter()
        .filter_map(|id| {
            state.registry.instances()
                .get(id)
                .map(|instance| instance.addr.clone())
        })
        .collect();
    for instance_id in instance_ids {
        if let Some(instance) = state.registry.instance_mut(instance_id) {
            instance.act_ping_time = 0;
            instance.link_down_since_time = 0;
            instance.down_state = DownState::Up;
            instance.sdown_since_time = 0;
            instance.sdown_time = 0;
            // Credentials can change for an existing replica instance that was
            // originally discovered with stale auth. Always back-sync from the
            // master before rebuilding links so subsequent AUTH uses the user's
            // latest SENTINEL SET values (09-acl-support + runtime ACL-reconf).
            sync_master_config(&master, instance);
        }
    }
    state
        .pending_link_close
        .retain(|address| !addresses.contains(address));
}

pub(super) fn set_down_after_ms(state: &mut SentinelState, name: &str, timeout: i64) {
    let Some(master) = state.registry.master_mut(name) else {
        return;
    };
    master.down_after_ms = timeout;
    let mut instance_ids = master.replicas.clone();
    instance_ids.push(master.id);
    for instance_id in instance_ids {
        if let Some(instance) = state.registry.instance_mut(instance_id) {
            instance.down_after_ms = timeout;
        }
    }
}

pub(super) fn validate_no_control_chars(value: &str) -> bool {
    !value.bytes().any(|byte| byte < b' ' || byte == 0x7f)
}

pub(super) fn invalid_set_argument(value: &str, option: &str) -> Value {
    if !validate_no_control_chars(value) || !validate_no_control_chars(option) {
        return Value::error("ERR Invalid argument: control characters not allowed");
    }
    Value::error(format!(
        "ERR Invalid argument '{}' for SENTINEL SET '{}'",
        value, option
    ))
}

pub(super) fn set(
    _conn: &mut dyn CommandConn,
    args: &CommandArgs,
    ctx: &mut ServerContext,
) -> Value {
    if args.args.len() < 4 {
        return CommandError::wrong_arity("sentinel|set");
    }
    let name = String::from_utf8_lossy(&args.args[1]);
    let state = ctx.sentinel_state_mut();
    if !state.registry.contains_master(name.as_ref()) {
        return Value::error(format!("ERR No such master with name '{}'", name));
    }
    let mut index = 2;
    let mut credentials_changed = false;
    while index + 1 < args.args.len() {
        let option = String::from_utf8_lossy(&args.args[index]).to_lowercase();
        let value = String::from_utf8_lossy(&args.args[index + 1]);
        if !validate_no_control_chars(&option) || !validate_no_control_chars(&value) {
            return invalid_set_argument(&value, &option);
        }
        let outcome = match apply_master_option(state, name.as_ref(), args, index, &option, &value)
        {
            Ok(outcome) => outcome,
            Err(error) => return error,
        };
        credentials_changed |= outcome.credentials_changed;
        index += outcome.consumed;
    }
    if credentials_changed {
        state.link_rebuild_needed = true;
        reset_instance_health_after_auth_change(state, name.as_ref());
    }
    // The configured master is the registry's canonical instance; no mirror
    // needs to be synchronized after applying options.
    if crate::framework::commands::sentinel::admin_cmds::flushconfig_internal(state).is_err() {
        return Value::error("ERR Failed to save config file. Check server logs.");
    }
    Value::ok()
}

struct SetOutcome {
    consumed: usize,
    credentials_changed: bool,
}

fn apply_master_option(
    state: &mut SentinelState,
    name: &str,
    args: &CommandArgs,
    index: usize,
    option: &str,
    value: &str,
) -> Result<SetOutcome, Value> {
    let mut outcome = SetOutcome {
        consumed: 2,
        credentials_changed: false,
    };
    if apply_numeric_option(state, name, option, value)? {
        return Ok(outcome);
    }
    if apply_script_option(state, name, option, value)? {
        return Ok(outcome);
    }
    if apply_credential_option(state, name, option, value) {
        outcome.credentials_changed = true;
        return Ok(outcome);
    }
    if option == "rename-command" {
        apply_rename_option(state, name, args, index)?;
        outcome.consumed = 4;
        return Ok(outcome);
    }
    Err(Value::error(format!(
        "ERR Unknown option or number of arguments for SENTINEL SET '{}'",
        option
    )))
}

fn apply_numeric_option(
    state: &mut SentinelState,
    name: &str,
    option: &str,
    value: &str,
) -> Result<bool, Value> {
    match option {
        "down-after-milliseconds" | "down-after-millis" => {
            let timeout = value
                .parse::<i64>()
                .ok()
                .filter(|timeout| *timeout > 0)
                .ok_or_else(|| invalid_set_argument(value, "down-after-milliseconds"))?;
            set_down_after_ms(state, name, timeout);
        }
        "failover-timeout" => {
            state.registry.master_mut(name)
                .expect("master checked by caller")
                .failover_timeout = positive_value(value, option)?;
        }
        "parallel-syncs" => {
            state.registry.master_mut(name)
                .expect("master checked by caller")
                .parallel_syncs = positive_value(value, option)?;
        }
        "quorum" => {
            state.registry.master_mut(name)
                .expect("master checked by caller")
                .quorum = positive_value(value, option)?;
        }
        "primary-reboot-down-after-period" | "master-reboot-down-after-period" => {
            state.registry.master_mut(name)
                .expect("master checked by caller")
                .primary_reboot_down_after_period =
                nonnegative_value(value, "primary-reboot-down-after-period")?;
        }
        _ => return Ok(false),
    }
    Ok(true)
}

fn apply_script_option(
    state: &mut SentinelState,
    name: &str,
    option: &str,
    value: &str,
) -> Result<bool, Value> {
    let denied = state.deny_scripts_reconfig;
    let master = state.registry.master_mut(name)
        .expect("master checked by caller");
    match option {
        "notification-script" => {
            validate_script_path(
                denied,
                value,
                "Notification script seems non existing or non executable",
            )?;
            master.notification_script = value.to_string();
        }
        "client-reconfig-script" => {
            validate_script_path(
                denied,
                value,
                "Client reconfiguration script seems non existing or non executable",
            )?;
            master.client_reconfig_script = value.to_string();
        }
        _ => return Ok(false),
    }
    Ok(true)
}

fn apply_credential_option(
    state: &mut SentinelState,
    name: &str,
    option: &str,
    value: &str,
) -> bool {
    let master = state.registry.master_mut(name)
        .expect("master checked by caller");
    match option {
        "auth-pass" => {
            master.auth_pass = value.to_string();
        }
        "auth-user" => {
            master.auth_user = value.to_string();
        }
        _ => return false,
    }
    true
}

fn apply_rename_option(
    state: &mut SentinelState,
    name: &str,
    args: &CommandArgs,
    index: usize,
) -> Result<(), Value> {
    if index + 2 >= args.args.len() {
        return Err(invalid_set_argument("rename-command", "rename-command"));
    }
    let from = String::from_utf8_lossy(&args.args[index + 1]).to_string();
    let to = String::from_utf8_lossy(&args.args[index + 2]).to_string();
    if !validate_no_control_chars(&from) || !validate_no_control_chars(&to) {
        return Err(Value::error(
            "ERR Invalid argument: control characters not allowed",
        ));
    }
    state.registry.master_mut(name)
        .expect("master checked by caller")
        .renamed_commands
        .insert(from, to);
    Ok(())
}

fn positive_value(value: &str, option: &str) -> Result<i64, Value> {
    value
        .parse::<i64>()
        .ok()
        .filter(|value| *value > 0)
        .ok_or_else(|| {
            Value::error(format!(
                "ERR Invalid argument '{}' to SENTINEL SET: {}",
                value, option
            ))
        })
}

fn nonnegative_value(value: &str, option: &str) -> Result<i64, Value> {
    value
        .parse::<i64>()
        .ok()
        .filter(|value| *value >= 0)
        .ok_or_else(|| {
            Value::error(format!(
                "ERR Invalid argument '{}' to SENTINEL SET: {}",
                value, option
            ))
        })
}

fn validate_script_path(denied: bool, value: &str, missing_message: &str) -> Result<(), Value> {
    if denied {
        return Err(Value::error(
            "ERR Reconfiguration of scripts path is denied for security reasons. Check the \
             deny-scripts-reconfig configuration directive in your Sentinel configuration",
        ));
    }
    if !value.is_empty() && !std::path::Path::new(value).exists() {
        return Err(Value::error(format!("ERR {}", missing_message)));
    }
    Ok(())
}
