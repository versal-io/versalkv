use bytes::Bytes;

use crate::engine::command::context::CommandConn;
use crate::foundation::io::protocol::Value;
use crate::framework::context::ServerContext;
use crate::framework::CommandArgs;

pub(super) fn debug(
    _conn: &mut dyn CommandConn,
    args: &CommandArgs,
    ctx: &mut ServerContext,
) -> Value {
    if args.args.len() == 1 {
        return debug_show_all(ctx);
    }
    debug_set_params(ctx, args)
}

fn debug_show_all(ctx: &mut ServerContext) -> Value {
    let state = ctx.sentinel_state();
    Value::Array(vec![
        Value::bulk(Bytes::from_static(b"INFO-PERIOD")),
        Value::Integer(state.tunables.info_period_ms),
        Value::bulk(Bytes::from_static(b"PING-PERIOD")),
        Value::Integer(state.tunables.ping_period_ms),
        Value::bulk(Bytes::from_static(b"ASK-PERIOD")),
        Value::Integer(state.tunables.ask_period_ms),
        Value::bulk(Bytes::from_static(b"PUBLISH-PERIOD")),
        Value::Integer(state.tunables.publish_period_ms),
        Value::bulk(Bytes::from_static(b"DEFAULT-DOWN-AFTER")),
        Value::Integer(state.tunables.default_down_after_ms),
        Value::bulk(Bytes::from_static(b"DEFAULT-FAILOVER-TIMEOUT")),
        Value::Integer(state.tunables.default_failover_timeout_ms),
        Value::bulk(Bytes::from_static(b"TILT-TRIGGER")),
        Value::Integer(state.tunables.tilt_trigger_ms),
        Value::bulk(Bytes::from_static(b"TILT-PERIOD")),
        Value::Integer(state.tunables.tilt_period_ms),
        Value::bulk(Bytes::from_static(b"SLAVE-RECONF-TIMEOUT")),
        Value::Integer(state.tunables.replica_reconf_timeout_ms),
        Value::bulk(Bytes::from_static(b"MIN-LINK-RECONNECT-PERIOD")),
        Value::Integer(state.tunables.min_link_reconnect_period_ms),
        Value::bulk(Bytes::from_static(b"ELECTION-TIMEOUT")),
        Value::Integer(state.tunables.election_timeout_ms),
        Value::bulk(Bytes::from_static(b"SCRIPT-MAX-RUNTIME")),
        Value::Integer(state.tunables.script_max_runtime_ms),
        Value::bulk(Bytes::from_static(b"SCRIPT-RETRY-DELAY")),
        Value::Integer(state.tunables.script_retry_delay_ms),
    ])
}

fn debug_set_params(ctx: &mut ServerContext, args: &CommandArgs) -> Value {
    const VALID_OPTIONS: &[&str] = &[
        "info-period",
        "ping-period",
        "ask-period",
        "publish-period",
        "default-down-after",
        "tilt-trigger",
        "tilt-period",
        "slave-reconf-timeout",
        "min-link-reconnect-period",
        "default-failover-timeout",
        "election-timeout",
        "script-max-runtime",
        "script-retry-delay",
    ];
    let mut updates = Vec::new();
    let mut index = 1;
    while index < args.args.len() {
        let option = String::from_utf8_lossy(&args.args[index]).to_lowercase();
        if !VALID_OPTIONS.contains(&option.as_str()) || index + 1 >= args.args.len() {
            return Value::error(format!(
                "Unknown option or number of arguments for SENTINEL DEBUG '{}'",
                option
            ));
        }
        index += 1;
        let value_text = String::from_utf8_lossy(&args.args[index]);
        let value = match value_text.parse() {
            Ok(value) if value > 0 => value,
            _ => {
                return Value::error(format!(
                    "Invalid argument '{}' for SENTINEL DEBUG '{}'",
                    value_text, option
                ));
            }
        };
        updates.push((option, value));
        index += 1;
    }
    for (option, value) in updates {
        apply_debug_param(ctx, &option, value);
    }
    Value::ok()
}

fn apply_debug_param(ctx: &mut ServerContext, option: &str, value: i64) {
    let state = ctx.sentinel_state_mut();
    match option {
        "info-period" => state.tunables.info_period_ms = value,
        "ping-period" => state.tunables.ping_period_ms = value,
        "ask-period" => state.tunables.ask_period_ms = value,
        "publish-period" => state.tunables.publish_period_ms = value,
        "default-down-after" => {
            state.tunables.default_down_after_ms = value;
        }
        "tilt-trigger" => state.tunables.tilt_trigger_ms = value,
        "tilt-period" => state.tunables.tilt_period_ms = value,
        "slave-reconf-timeout" => state.tunables.replica_reconf_timeout_ms = value,
        "min-link-reconnect-period" => state.tunables.min_link_reconnect_period_ms = value,
        "default-failover-timeout" => state.tunables.default_failover_timeout_ms = value,
        "election-timeout" => state.tunables.election_timeout_ms = value,
        "script-max-runtime" => {
            state.tunables.script_max_runtime_ms = value;
            state.script_engine.debug_max_runtime_ms = value;
        }
        "script-retry-delay" => {
            state.tunables.script_retry_delay_ms = value;
            state.script_engine.debug_retry_delay_ms = value;
        }
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::apply_debug_param;
    use crate::engine::distributed::sentinel::SentinelInstance;
    use crate::framework::context::ServerContext;

    #[test]
    fn default_down_after_debug_setting_does_not_rewrite_loaded_masters() {
        let mut ctx = ServerContext::new(1);
        let mut master = SentinelInstance::new_master(1, "setmaster", "127.0.0.1:6379", 2);
        master.down_after_ms = 20_000;
        ctx.sentinel_state_mut()
            .registry
            .insert_master(master.name.clone(), master)
            .unwrap();

        apply_debug_param(&mut ctx, "default-down-after", 1_000);

        assert_eq!(ctx.sentinel_state().tunables.default_down_after_ms, 1_000);
        assert_eq!(
            ctx.sentinel_state()
                .registry
                .master("setmaster")
                .unwrap()
                .down_after_ms,
            20_000
        );
    }
}
