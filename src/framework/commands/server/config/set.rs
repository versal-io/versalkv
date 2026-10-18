use crate::engine::command::context::CommandConn;
use crate::foundation::io::protocol::Value;
use crate::framework::context::ServerContext;
use crate::framework::{CommandArgs, CommandError};

use super::{config_registry, read_oom_score_adj, set_oom_score_adj, write_oom_score_adj};

pub(super) mod set_cluster;
mod set_memory;
mod set_misc;
mod set_persistence;
mod set_server;
mod set_stats;

use set_cluster::apply_cluster_config_snapshot;
use set_memory::config_set_memory;
use set_misc::config_set_misc;
use set_persistence::config_set_persistence;
use set_server::config_set_server;
use set_stats::config_set_stats;

#[cfg(test)]
static LOG_LEVEL_TEST_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

#[cfg(test)]
pub(super) struct LogLevelTestGuard {
    _lock: std::sync::MutexGuard<'static, ()>,
    previous: log::LevelFilter,
}

#[cfg(test)]
impl Drop for LogLevelTestGuard {
    fn drop(&mut self) {
        log::set_max_level(self.previous);
    }
}

#[cfg(test)]
pub(super) fn log_level_test_guard() -> LogLevelTestGuard {
    let lock = LOG_LEVEL_TEST_LOCK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    LogLevelTestGuard {
        _lock: lock,
        previous: log::max_level(),
    }
}

fn config_set_value_arg_count(args: &CommandArgs, param_index: usize, param: &str) -> usize {
    match param {
        // C declares this SPECIAL config with MULTI_ARG_CONFIG: the three
        // process-class values are separate RESP arguments, not one string.
        "oom-score-adj-values" => {
            // Preserve config-file/Tcl callers that pass the three values as
            // one quoted argument while accepting C's three independent RESP
            // arguments for MULTI_ARG_CONFIG.
            if std::str::from_utf8(&args.args[param_index + 1])
                .ok()
                .is_some_and(|value| value.split_whitespace().count() > 1)
            {
                1
            } else {
                crate::engine::extension::config::server::CONFIG_OOM_COUNT
            }
        }
        // C defines this as MULTI_ARG_CONFIG. Subsequent numeric arguments
        // remain percentile values; the next non-numeric argument begins the
        // next CONFIG SET parameter/value pair.
        "latency-tracking-info-percentiles" => {
            let mut value_count = 1usize;
            while param_index + value_count + 1 < args.args.len()
                && std::str::from_utf8(&args.args[param_index + value_count + 1])
                    .ok()
                    .and_then(|value| value.parse::<f64>().ok())
                    .is_some()
            {
                value_count += 1;
            }
            value_count
        }
        _ => 1,
    }
}

/// Single-key "CONFIG SET key value" workhorse. Shared by the command handler
/// and by `Server::apply_config`'s CLI-unknown fallback path, so the two agree
/// on semantics (mem-unit parsing, side effects like `needs_eviction`, OOM
/// score, etc.). Visible to `crate::server` for the CLI fallback.
fn apply_config_set_one_with_exec(
    ctx: &mut ServerContext,
    param: &str,
    val_trimmed: &str,
    val_bytes: &[u8],
    in_exec: bool,
) -> Result<(), String> {
    if matches!(param, "requirepass" | "primaryauth" | "masterauth") {
        let value = val_bytes
            .strip_prefix(b"'")
            .and_then(|value| value.strip_suffix(b"'"))
            .or_else(|| {
                val_bytes
                    .strip_prefix(b"\"")
                    .and_then(|value| value.strip_suffix(b"\""))
            })
            .unwrap_or(val_bytes);
        if param == "requirepass" {
            let password = (!value.is_empty()).then(|| value.to_vec());
            crate::engine::extension::acl::set_default_user_requirepass(password.as_deref());
            ctx.config_mut().security.requirepass = password;
        } else {
            ctx.config_mut().security.primaryauth = (!value.is_empty()).then(|| value.to_vec());
        }
        return Ok(());
    }
    let old_dual_channel = ctx.config().replication.dual_channel_replication;
    if param == "loglevel" {
        let level = match val_trimmed.to_lowercase().as_str() {
            "debug" => log::LevelFilter::Debug,
            "verbose" | "notice" => log::LevelFilter::Info,
            "warning" | "warn" => log::LevelFilter::Warn,
            "nothing" => log::LevelFilter::Off,
            _ => return Err(format!("ERR Invalid log level '{}'", val_trimmed)),
        };
        log::set_max_level(level);
        // fall through to registry update below
    }
    if param == "maxmemory" {
        return config_set_memory(ctx, param, val_trimmed, val_bytes);
    }
    if let Some(result) = config_registry().try_set(ctx.config_mut(), param, val_trimmed) {
        result?;
        if matches!(
            param,
            "maxmemory-policy" | "lfu-log-factor" | "lfu-decay-time"
        ) {
            ctx.sync_eviction_policy();
        }
        if param == "dual-channel-replication-enabled"
            && old_dual_channel != ctx.config().replication.dual_channel_replication
        {
            ctx.replication_mut().force_full_sync_on_next_reconnect = true;
        }
        if param == "lua-enable-insecure-api" {
            crate::engine::extension::lua::reset_lua_runtime();
        }
        if param == "tracking-table-max-keys" {
            ctx.tracking_mut().max_keys = ctx.config().server.tracking_table_max_keys;
        }
        match param {
            "slowlog-log-slower-than"
            | "slowlog-max-len"
            | "commandlog-execution-slower-than"
            | "commandlog-request-larger-than"
            | "commandlog-reply-larger-than"
            | "commandlog-slow-execution-max-len"
            | "commandlog-large-request-max-len"
            | "commandlog-large-reply-max-len" => {
                return config_set_stats(ctx, param, val_trimmed, val_bytes);
            }
            "hz" | "watchdog-period" | "acllog-max-len" | "latency-monitor-threshold" => {
                return config_set_server(ctx, param, val_trimmed, val_bytes);
            }
            "aof-disable-auto-gc" | "rdb-key-save-delay" => {
                return config_set_persistence(ctx, param, val_trimmed, val_bytes, in_exec);
            }
            // Align with C's setConfigSaveOption: updating the save parameters
            // only reconfigures the auto-save schedule; it must NOT fork a
            // BGSAVE. The periodic serverCron auto-save (should_auto_bgsave)
            // starts a child once dirty >= changes and enough time has elapsed,
            // exactly as C does. Forking here raced tests that issue their own
            // BGSAVE right after `CONFIG SET save` (e.g. integration/rdb.tcl's
            // "failed bgsave prevents writes"), intermittently producing
            // "Background save already in progress". The saveparams themselves
            // are applied by config_set_persistence below.
            "save" => {
                return Ok(());
            }
            _ => return Ok(()),
        }
    }

    match param {
        "oom-score-adj" | "oom-score-adj-values" | "client-output-buffer-limit" => {
            config_set_server(ctx, param, val_trimmed, val_bytes)
        }
        "rdb-key-save-delay" | "rdb-version-check" | "appendonly" => {
            config_set_persistence(ctx, param, val_trimmed, val_bytes, in_exec)
        }
        "maxmemory" => config_set_memory(ctx, param, val_trimmed, val_bytes),
        _ => config_set_misc(ctx, param, val_trimmed, val_bytes),
    }
}

pub(crate) fn apply_config_set_one(
    ctx: &mut ServerContext,
    param: &str,
    val_trimmed: &str,
    val_bytes: &[u8],
) -> Result<(), String> {
    apply_config_set_one_with_exec(ctx, param, val_trimmed, val_bytes, false)
}

fn config_set_one(
    conn: &mut dyn CommandConn,
    ctx: &mut ServerContext,
    param: &str,
    val_bytes: &[u8],
) -> Result<(), String> {
    if (ctx.is_loading() || ctx.replication().async_loading)
        && matches!(param, "appendonly" | "repl-diskless-load" | "dir")
    {
        return Err("LOADING Valkey is loading the dataset in memory".to_string());
    }

    let val_str = String::from_utf8_lossy(val_bytes).to_string();
    let val_trimmed = val_str.trim_matches(|c| c == '\'' || c == '"');
    apply_config_set_one_with_exec(ctx, param, val_trimmed, val_bytes, conn.in_transaction())
}

fn format_config_set_error(param: &str, error: String) -> String {
    const PREFIXES: &[&str] = &[
        "ERR ",
        "OOM ",
        "LOADING ",
        "READONLY ",
        "MASTERDOWN ",
        "NOAUTH ",
        "NOPERM ",
        "BUSY ",
        "NOREPLICAS ",
        "MISCONF ",
    ];
    if PREFIXES.iter().any(|prefix| error.starts_with(prefix)) {
        return error;
    }
    format!(
        "ERR CONFIG SET failed (possibly related to argument '{}') - {}",
        param, error
    )
}

fn protected_config_allowed(
    action: crate::engine::extension::config::server::ProtectedAction,
    is_local: bool,
) -> bool {
    match action {
        crate::engine::extension::config::server::ProtectedAction::Yes => true,
        crate::engine::extension::config::server::ProtectedAction::Local => is_local,
        crate::engine::extension::config::server::ProtectedAction::No => false,
    }
}

pub fn config_set(
    conn: &mut dyn CommandConn,
    args: &CommandArgs,
    ctx: &mut ServerContext,
) -> Value {
    if args.args.len() < 3 {
        return CommandError::wrong_arity("config");
    }
    let allow_protected = protected_config_allowed(
        ctx.config().server.enable_protected_configs,
        conn.is_local(),
    );
    let mut check = 1usize;
    let mut seen = std::collections::HashSet::new();
    let mut cluster_config_changed = false;
    let mut tls_config_changed = false;
    while check < args.args.len() {
        let param = String::from_utf8_lossy(&args.args[check]).to_ascii_lowercase();
        let value_arg_count = config_set_value_arg_count(args, check, &param);
        if check + value_arg_count >= args.args.len() {
            return CommandError::wrong_arity("config");
        }
        let value = String::from_utf8_lossy(&args.args[check + 1]);
        if param == "daemonize" {
            return Value::error("ERR CONFIG SET failed (immutable config)");
        }
        if !allow_protected {
            if let Some(spec) = config_registry().lookup(&param) {
                if spec.is_protected() {
                    return Value::error(format!(
                        "ERR CONFIG SET failed (possibly related to argument '{}') - can't set protected config",
                        param
                    ));
                }
            }
        }
        if !seen.insert(param.clone()) {
            return Value::error(format!("ERR duplicate config parameter '{}'", param));
        }
        cluster_config_changed |= param.starts_with("cluster-")
            || matches!(
                param.as_str(),
                "availability-zone" | "bind" | "port" | "tls-port"
            );
        tls_config_changed |= matches!(
            param.as_str(),
            "tls-port"
                | "tls-cert-file"
                | "tls-key-file"
                | "tls-ca-cert-file"
                | "tls-ca-cert-dir"
                | "tls-auth-clients"
                | "tls-protocols"
                | "tls-ciphers"
                | "tls-ciphersuites"
                | "tls-prefer-server-ciphers"
                | "tls-session-caching"
                | "tls-session-cache-size"
                | "tls-replication"
                | "tls-cluster"
                | "tls-client-cert-file"
                | "tls-client-key-file"
        );
        if param == "maxmemory-clients"
            && value.ends_with('%')
            && value[..value.len() - 1].parse::<u64>().unwrap_or(101) > 100
        {
            return Value::error("ERR CONFIG SET failed (possibly related to argument 'maxmemory-clients') - percentage argument must be less or equal to 100");
        }
        check += 1 + value_arg_count;
    }

    // Snapshot all server config fields for cross-param rollback on apply failure.
    let server_snapshot = ctx.config().server.clone();
    let runtime_config_snapshot = ctx.config().clone();
    // Snapshot raw OS-level oom_score_adj before any apply; best-effort (ignore failure on non-Linux).
    let oom_snapshot: Option<i32> = read_oom_score_adj();

    let mut i = 1usize;
    while i < args.args.len() {
        let param = String::from_utf8_lossy(&args.args[i]).to_ascii_lowercase();
        let value_arg_count = config_set_value_arg_count(args, i, &param);
        let joined_value;
        let value: &[u8] = if value_arg_count == 1 {
            args.args[i + 1].as_ref()
        } else {
            joined_value = args.args[i + 1..=i + value_arg_count]
                .iter()
                .enumerate()
                .fold(Vec::new(), |mut joined, (index, value)| {
                    if index != 0 {
                        joined.push(b' ');
                    }
                    joined.extend_from_slice(value);
                    joined
                });
            joined_value.as_slice()
        };
        let result = if param == "port" {
            let value = String::from_utf8_lossy(value);
            match value.parse::<u16>() {
                Ok(port) => {
                    ctx.config_mut().server.listening_port = port;
                    Ok(())
                }
                Err(_) => Err("ERR argument port must be numeric".to_string()),
            }
        } else {
            config_set_one(conn, ctx, &param, value)
        };
        if let Err(e) = result {
            // Rollback: restore server fields from snapshot taken before first mutation.
            ctx.config_mut().server = server_snapshot.clone();
            ctx.sync_eviction_policy();
            // Restore OS-level oom_score_adj via the high-level helper so oom_score_adj_adjusted
            // flag is reset correctly.
            let _ = set_oom_score_adj(ctx.config_mut(), -1);
            // If a raw snapshot was available, enforce it directly so base-capture mid-apply
            // cannot drift the restore value.
            if let Some(raw) = oom_snapshot {
                let _ = write_oom_score_adj(raw);
            }
            return Value::error(format_config_set_error(&param, e));
        }
        i += 1 + value_arg_count;
    }
    let requested_bind = ctx.config().server.bind.clone();
    let requested_port = ctx.config().server.listening_port;
    let requested_tls_port = ctx.config().server.tls_port;
    let requested_io_threads = ctx.config().server.io_threads as usize;
    let bind_changed = requested_bind != runtime_config_snapshot.server.bind;
    let port_changed = requested_port != runtime_config_snapshot.server.listening_port;
    let tls_port_changed = requested_tls_port != runtime_config_snapshot.server.tls_port;
    let tls_replication_changed =
        ctx.config().server.tls_replication != runtime_config_snapshot.server.tls_replication;
    let tls_cluster_changed =
        ctx.config().server.tls_cluster != runtime_config_snapshot.server.tls_cluster;
    let tls_client_material_changed = ctx.config().server.tls_client_cert_file
        != runtime_config_snapshot.server.tls_client_cert_file
        || ctx.config().server.tls_client_key_file
            != runtime_config_snapshot.server.tls_client_key_file
        || ctx.config().server.tls_ca_cert_file != runtime_config_snapshot.server.tls_ca_cert_file
        || ctx.config().server.tls_ca_cert_dir != runtime_config_snapshot.server.tls_ca_cert_dir;
    let tls_context_policy_changed = ctx.config().server.tls_protocols
        != runtime_config_snapshot.server.tls_protocols
        || ctx.config().server.tls_ciphers != runtime_config_snapshot.server.tls_ciphers
        || ctx.config().server.tls_ciphersuites != runtime_config_snapshot.server.tls_ciphersuites
        || ctx.config().server.tls_prefer_server_ciphers
            != runtime_config_snapshot.server.tls_prefer_server_ciphers
        || ctx.config().server.tls_session_caching
            != runtime_config_snapshot.server.tls_session_caching
        || ctx.config().server.tls_session_cache_size
            != runtime_config_snapshot.server.tls_session_cache_size;
    let io_threads_changed =
        requested_io_threads != runtime_config_snapshot.server.io_threads as usize;
    let listener_changed = bind_changed || port_changed;
    if cluster_config_changed && !listener_changed {
        if let Err(error) = apply_cluster_config_snapshot(ctx) {
            ctx.config_mut().server = server_snapshot;
            ctx.sync_eviction_policy();
            return Value::error(error);
        }
    }
    if io_threads_changed {
        ctx.request_io_threads_reconfigure(runtime_config_snapshot.clone(), requested_io_threads);
    }
    if listener_changed {
        ctx.request_tcp_rebind(
            runtime_config_snapshot.clone(),
            oom_snapshot,
            requested_bind,
            requested_port,
            if bind_changed { "bind" } else { "port" },
        );
        conn.set_custom_reply(true);
    }
    if tls_config_changed {
        ctx.request_tls_reconfigure(
            runtime_config_snapshot,
            tls_port_changed,
            tls_replication_changed || tls_client_material_changed || tls_context_policy_changed,
            tls_cluster_changed || (tls_context_policy_changed && ctx.config().server.tls_cluster),
            if tls_port_changed {
                "tls-port"
            } else {
                "TLS configuration"
            },
        );
        conn.set_custom_reply(true);
    }
    if listener_changed || tls_config_changed {
        return Value::Null;
    }
    Value::ok()
}

#[cfg(test)]
mod tests {
    use super::{apply_config_set_one, config_set, log_level_test_guard, protected_config_allowed};
    use crate::engine::command::CommandArgs;
    use crate::foundation::io::protocol::Value;
    use crate::foundation::io::store::Robj;
    use crate::framework::capabilities::connection::Connection;
    use crate::framework::commands::server::config::get::config_get;
    use crate::framework::context::ServerContext;
    use bytes::Bytes;

    #[test]
    fn protected_config_gate_honors_no_yes_and_local_modes() {
        use crate::engine::extension::config::server::ProtectedAction;

        assert!(!protected_config_allowed(ProtectedAction::No, true));
        assert!(protected_config_allowed(ProtectedAction::Yes, false));
        assert!(protected_config_allowed(ProtectedAction::Local, true));
        assert!(!protected_config_allowed(ProtectedAction::Local, false));
    }

    #[test]
    fn maxmemory_policy_set_synchronizes_database_metadata_mode() {
        let mut ctx = ServerContext::new(1);

        apply_config_set_one(&mut ctx, "maxmemory-policy", "allkeys-lfu", b"allkeys-lfu").unwrap();
        ctx.db_mut(0)
            .unwrap()
            .set(b"lfu".to_vec(), Robj::string("value"));

        assert_eq!(ctx.db(0).unwrap().get_lfu_freq(b"lfu"), 5);
    }

    #[test]
    fn maxmemory_sampling_and_lfu_tuning_are_runtime_configurable() {
        let mut ctx = ServerContext::new(1);

        apply_config_set_one(&mut ctx, "maxmemory-samples", "9", b"9").unwrap();
        apply_config_set_one(&mut ctx, "lfu-log-factor", "7", b"7").unwrap();
        apply_config_set_one(&mut ctx, "lfu-decay-time", "3", b"3").unwrap();

        assert_eq!(ctx.config().server.maxmemory_samples, 9);
        assert_eq!(ctx.config().server.lfu_log_factor, 7);
        assert_eq!(ctx.config().server.lfu_decay_time, 3);
    }

    #[test]
    fn tracking_table_max_keys_updates_runtime_and_config_get() {
        let mut ctx = ServerContext::new(1);
        let mut conn = Connection::new_fake();
        let set_args = CommandArgs::from_parts(vec![
            Bytes::from_static(b"CONFIG"),
            Bytes::from_static(b"SET"),
            Bytes::from_static(b"tracking-table-max-keys"),
            Bytes::from_static(b"17"),
        ]);

        assert_eq!(config_set(&mut conn, &set_args, &mut ctx), Value::ok());
        assert_eq!(ctx.tracking().max_keys, 17);

        let get_args = CommandArgs::from_parts(vec![
            Bytes::from_static(b"CONFIG"),
            Bytes::from_static(b"GET"),
            Bytes::from_static(b"tracking-table-max-keys"),
        ]);
        assert_eq!(
            config_get(&get_args, &ctx),
            Value::Array(vec![
                Value::bulk(Bytes::from_static(b"tracking-table-max-keys")),
                Value::bulk(Bytes::from_static(b"17")),
            ])
        );
    }

    #[test]
    fn save_config_replaces_the_live_auto_bgsave_policy() {
        let mut ctx = ServerContext::new(1);
        let mut conn = Connection::new_fake();
        ctx.persistence_mut().record_rdb_save(100);
        ctx.persistence_mut().add_dirty(2);
        let args = CommandArgs::from_parts(vec![
            Bytes::from_static(b"CONFIG"),
            Bytes::from_static(b"SET"),
            Bytes::from_static(b"save"),
            Bytes::from_static(b"10 2"),
        ]);

        assert_eq!(config_set(&mut conn, &args, &mut ctx), Value::ok());
        assert_eq!(ctx.config().persistence.saveparams.len(), 1);
        assert!(crate::engine::persistence::rdb::should_auto_bgsave(
            ctx.persistence(),
            &ctx.config().persistence.saveparams,
            111,
        ));
        assert!(!crate::engine::persistence::rdb::should_auto_bgsave(
            ctx.persistence(),
            &ctx.config().persistence.saveparams,
            110,
        ));
    }

    #[test]
    fn loglevel_config_updates_the_process_log_filter() {
        let _log_level_guard = log_level_test_guard();
        let mut ctx = ServerContext::new(1);
        let mut conn = Connection::new_fake();
        let args = CommandArgs::from_parts(vec![
            Bytes::from_static(b"CONFIG"),
            Bytes::from_static(b"SET"),
            Bytes::from_static(b"loglevel"),
            Bytes::from_static(b"warning"),
        ]);

        assert_eq!(config_set(&mut conn, &args, &mut ctx), Value::ok());
        assert_eq!(ctx.config().server.loglevel, "warning");
        assert_eq!(log::max_level(), log::LevelFilter::Warn);
    }

    #[test]
    fn lazyfree_lazy_expire_is_independent_from_server_del() {
        let mut ctx = ServerContext::new(1);
        let mut conn = Connection::new_fake();
        let set_args = CommandArgs::from_parts(vec![
            Bytes::from_static(b"CONFIG"),
            Bytes::from_static(b"SET"),
            Bytes::from_static(b"lazyfree-lazy-expire"),
            Bytes::from_static(b"no"),
        ]);

        assert_eq!(config_set(&mut conn, &set_args, &mut ctx), Value::ok());
        assert!(!ctx.config().server.lazyfree_lazy_expire);
        assert!(ctx.config().server.lazyfree_lazy_server_del);

        let get_args = CommandArgs::from_parts(vec![
            Bytes::from_static(b"CONFIG"),
            Bytes::from_static(b"GET"),
            Bytes::from_static(b"lazyfree-lazy-expire"),
        ]);
        assert_eq!(
            config_get(&get_args, &ctx),
            Value::Array(vec![
                Value::bulk(Bytes::from_static(b"lazyfree-lazy-expire")),
                Value::bulk(Bytes::from_static(b"no")),
            ])
        );
    }

    #[test]
    fn slowlog_alias_reconfigures_both_observability_owners() {
        let mut ctx = ServerContext::new(1);
        let mut conn = Connection::new_fake();
        let args = CommandArgs::from_parts(vec![
            Bytes::from_static(b"CONFIG"),
            Bytes::from_static(b"SET"),
            Bytes::from_static(b"slowlog-log-slower-than"),
            Bytes::from_static(b"42"),
        ]);

        assert_eq!(config_set(&mut conn, &args, &mut ctx), Value::ok());
        assert_eq!(ctx.config().server.commandlog_slow_threshold, 42);
        assert_eq!(ctx.config().server.slowlog_log_slower_than, 42);
        assert_eq!(ctx.slowlog().threshold_us, 42);
        assert_eq!(ctx.commandlog().slow.threshold_us, 42);
    }

    #[test]
    fn tcp_keepalive_config_round_trips_through_config_get() {
        let mut ctx = ServerContext::new(1);
        let mut conn = Connection::new_fake();
        let set_args = CommandArgs::from_parts(vec![
            Bytes::from_static(b"CONFIG"),
            Bytes::from_static(b"SET"),
            Bytes::from_static(b"tcp-keepalive"),
            Bytes::from_static(b"17"),
        ]);

        assert_eq!(config_set(&mut conn, &set_args, &mut ctx), Value::ok());

        let get_args = CommandArgs::from_parts(vec![
            Bytes::from_static(b"CONFIG"),
            Bytes::from_static(b"GET"),
            Bytes::from_static(b"tcp-keepalive"),
        ]);
        assert_eq!(
            config_get(&get_args, &ctx),
            Value::Array(vec![
                Value::bulk(Bytes::from_static(b"tcp-keepalive")),
                Value::bulk(Bytes::from_static(b"17")),
            ])
        );
    }

    #[test]
    fn tls_port_queues_a_runtime_reconfigure_instead_of_silently_succeeding() {
        let mut ctx = ServerContext::new(1);
        let mut conn = Connection::new_fake();
        let args = CommandArgs::from_parts(vec![
            Bytes::from_static(b"CONFIG"),
            Bytes::from_static(b"SET"),
            Bytes::from_static(b"tls-port"),
            Bytes::from_static(b"6380"),
        ]);

        assert_eq!(config_set(&mut conn, &args, &mut ctx), Value::Null);
        assert_eq!(ctx.config().server.tls_port, 6380);
        let request = ctx
            .take_tls_reconfigure_request()
            .expect("tls-port must queue an L5 reconfigure request");
        assert!(request.port_changed);
        assert_eq!(request.changed_parameter, "tls-port");
    }

    #[test]
    fn tls_cluster_queues_a_cluster_bus_rebind() {
        let mut ctx = ServerContext::new(1);
        let mut conn = Connection::new_fake();
        let args = CommandArgs::from_parts(vec![
            Bytes::from_static(b"CONFIG"),
            Bytes::from_static(b"SET"),
            Bytes::from_static(b"tls-cluster"),
            Bytes::from_static(b"yes"),
        ]);

        assert_eq!(config_set(&mut conn, &args, &mut ctx), Value::Null);
        assert!(ctx.config().server.tls_cluster);
        let request = ctx
            .take_tls_reconfigure_request()
            .expect("tls-cluster must queue an L5 reconfigure request");
        assert!(request.rebind_cluster_bus);
        assert!(!request.port_changed);
    }

    #[test]
    fn disabled_tls_accepts_clearing_sensitive_password_fields() {
        let mut ctx = ServerContext::new(1);
        let mut conn = Connection::new_fake();
        for name in [b"tls-key-file-pass" as &[u8], b"tls-client-key-file-pass"] {
            let args = CommandArgs::from_parts(vec![
                Bytes::from_static(b"CONFIG"),
                Bytes::from_static(b"SET"),
                Bytes::copy_from_slice(name),
                Bytes::new(),
            ]);
            assert_eq!(config_set(&mut conn, &args, &mut ctx), Value::ok());
        }
    }
}
