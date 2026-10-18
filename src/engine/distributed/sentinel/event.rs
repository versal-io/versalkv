use crate::engine::distributed::sentinel::script::{ScriptType, SentinelScriptEngine};
use crate::engine::distributed::sentinel::SentinelInstance;

pub fn sentinel_event(
    script_engine: &mut SentinelScriptEngine,
    level: &str,
    type_: &str,
    ri: Option<&SentinelInstance>,
    msg: &str,
) {
    let log_msg = format!("{} {}", type_, msg);
    match level {
        "warning" => crate::server_log!(warning, "{}", log_msg),
        "notice" => crate::server_log!(notice, "{}", log_msg),
        _ => crate::server_log!(notice, "{}", log_msg),
    }

    let should_notify = matches!(
        type_,
        "+sdown"
            | "-sdown"
            | "+odown"
            | "-odown"
            | "+switch-master"
            | "+failover-end"
            | "-failover-end"
            | "+monitor"
            | "+reconfigured"
    );

    if should_notify {
        if let Some(instance) = ri {
            if !instance.notification_script.is_empty() {
                let args = vec![
                    instance.notification_script.clone(),
                    "notification".to_string(),
                    type_.to_string(),
                    instance.name.clone(),
                    instance.addr.clone(),
                    msg.to_string(),
                ];
                script_engine.enqueue(
                    ScriptType::Notification,
                    &instance.notification_script,
                    args,
                );
            }
        }
    }
}
