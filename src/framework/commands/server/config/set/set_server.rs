use crate::framework::context::ServerContext;

use super::super::set_oom_score_adj;
use crate::engine::extension::config::server::{
    CONFIG_OOM_COUNT, OOM_SCORE_ADJ_ABSOLUTE, OOM_SCORE_ADJ_NO, OOM_SCORE_RELATIVE,
};

pub(super) fn config_set_server(
    ctx: &mut ServerContext,
    param: &str,
    val_trimmed: &str,
    _val_bytes: &[u8],
) -> Result<(), String> {
    match param {
        "hz" => {
            if let Ok(n) = val_trimmed.parse::<i64>() {
                ctx.config_mut().server.hz = (n as i32).clamp(1, 500);
            }
        }
        "watchdog-period" => {
            // ConfigRegistry has already validated and C-clamped the stored
            // period. Apply it immediately so CONFIG SET 0 disarms a pending
            // alarm and a nonzero update replaces the active deadline.
            crate::foundation::io::sys::signal::schedule_watchdog(
                ctx.config().server.watchdog_period,
            );
        }
        "acllog-max-len" => {
            if let Ok(n) = val_trimmed.parse::<usize>() {
                ctx.config_mut().server.acllog_max_len = n;
                crate::engine::extension::acl::set_acl_log_max_len(n);
            } else {
                return Err("ERR argument must be a non-negative integer".to_string());
            }
        }
        "oom-score-adj" => {
            let mode = match val_trimmed {
                "no" | "false" | "0" => OOM_SCORE_ADJ_NO,
                "yes" | "true" | "1" | "relative" => OOM_SCORE_RELATIVE,
                "absolute" => OOM_SCORE_ADJ_ABSOLUTE,
                _ => {
                    return Err(
                        "ERR Invalid oom-score-adj value, must be no/yes/relative/absolute"
                            .to_string(),
                    )
                }
            };
            let prev_values = ctx.config().server.oom_score_adj_values;
            let prev_mode = ctx.config().server.oom_score_adj;
            ctx.config_mut().server.oom_score_adj = mode;
            if let Err(e) = set_oom_score_adj(ctx.config_mut(), -1) {
                ctx.config_mut().server.oom_score_adj = prev_mode;
                ctx.config_mut().server.oom_score_adj_values = prev_values;
                return Err(format!("ERR {}", e));
            }
        }
        "oom-score-adj-values" => {
            let parts: Vec<&str> = val_trimmed.split_whitespace().collect();
            if parts.len() != CONFIG_OOM_COUNT {
                return Err("ERR wrong number of arguments".to_string());
            }
            let mut values = [0i32; CONFIG_OOM_COUNT];
            for i in 0..CONFIG_OOM_COUNT {
                match parts[i].parse::<i32>() {
                    Ok(v) if (-2000..=2000).contains(&v) => values[i] = v,
                    _ => {
                        return Err(
                            "ERR Invalid oom-score-adj-values, elements must be between -2000 and 2000."
                                .to_string(),
                        );
                    }
                }
            }
            let prev_values = ctx.config().server.oom_score_adj_values;
            ctx.config_mut().server.oom_score_adj_values = values;
            if ctx.config().server.oom_score_adj != OOM_SCORE_ADJ_NO {
                if let Err(e) = set_oom_score_adj(ctx.config_mut(), -1) {
                    ctx.config_mut().server.oom_score_adj_values = prev_values;
                    return Err(format!("ERR {}", e));
                }
            }
        }
        "latency-monitor-threshold" => {
            if let Ok(n) = val_trimmed.parse::<i64>() {
                if n >= 0 {
                    ctx.config_mut().server.latency_monitor_threshold = n;
                    ctx.latency_mut().set_threshold(n);
                }
            }
        }
        "client-output-buffer-limit" => {
            let fields: Vec<_> = val_trimmed.split_whitespace().collect();
            if fields.is_empty() || !fields.len().is_multiple_of(4) {
                return Err("ERR Wrong number of arguments".to_string());
            }
            let mut replica_limits = None;
            for chunk in fields.as_chunks::<4>().0 {
                let class = chunk[0];
                if !matches!(
                    class.to_ascii_lowercase().as_str(),
                    "normal" | "replica" | "slave" | "pubsub"
                ) {
                    return Err(format!("ERR Invalid client class '{}'", class));
                }
                let hard_limit = super::super::parse_mem_unit_unsigned(chunk[1])
                    .ok_or_else(|| format!("ERR Error invalid {class} output buffer hard limit"))?;
                let soft_limit = super::super::parse_mem_unit_unsigned(chunk[2])
                    .ok_or_else(|| format!("ERR Error invalid {class} output buffer soft limit"))?;
                let soft_seconds = chunk[3].parse::<u64>().map_err(|_| {
                    format!("ERR Error invalid {class} output buffer soft limit time")
                })?;
                if matches!(class.to_ascii_lowercase().as_str(), "replica" | "slave") {
                    replica_limits = Some((hard_limit as usize, soft_limit as usize, soft_seconds));
                }
                match class.to_ascii_lowercase().as_str() {
                    "normal" => {
                        ctx.config_mut().replication.normal_output_buffer_hard_limit =
                            hard_limit as usize;
                        ctx.config_mut().replication.normal_output_buffer_soft_limit =
                            soft_limit as usize;
                        ctx.config_mut()
                            .replication
                            .normal_output_buffer_soft_limit_seconds = soft_seconds;
                    }
                    "pubsub" => {
                        ctx.config_mut().replication.pubsub_output_buffer_hard_limit =
                            hard_limit as usize;
                        ctx.config_mut().replication.pubsub_output_buffer_soft_limit =
                            soft_limit as usize;
                        ctx.config_mut()
                            .replication
                            .pubsub_output_buffer_soft_limit_seconds = soft_seconds;
                    }
                    _ => {}
                }
            }
            if let Some((hard_limit, soft_limit, soft_seconds)) = replica_limits {
                ctx.config_mut()
                    .replication
                    .replica_output_buffer_hard_limit = hard_limit;
                ctx.config_mut()
                    .replication
                    .replica_output_buffer_soft_limit = soft_limit;
                ctx.config_mut()
                    .replication
                    .replica_output_buffer_soft_limit_seconds = soft_seconds;
            }
        }
        _ => unreachable!(),
    }
    Ok(())
}
