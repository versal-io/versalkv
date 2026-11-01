use crate::engine::extension::acl::{acl_roles, acl_users};
use crate::foundation::io::persistence::AofFsync;
use crate::foundation::io::protocol::Value;
use crate::framework::context::ServerContext;

use super::bool_to_yesno;

pub fn config_rewrite(ctx: &mut ServerContext) -> Value {
    use std::fs;
    let path = match &ctx.config().server.config_file {
        Some(p) => p.clone(),
        None => return Value::error("ERR The server is running without a config file"),
    };

    let num_dbs = ctx.num_dbs();
    let r = ctx.config();
    let mut lines: Vec<String> = Vec::new();

    lines.push(format!("port {}", r.server.listening_port));
    if r.server.bind != "* -::*" {
        lines.push(format!("bind {}", r.server.bind));
    }
    if !r.server.bind_source_addr.is_empty() {
        lines.push(format!("bind-source-addr {}", r.server.bind_source_addr));
    }
    if !r.server.hash_seed.is_empty() {
        lines.push(format!(
            "hash-seed {}",
            quote_binary_config_value(&r.server.hash_seed)
        ));
    }
    lines.push(format!("loglevel {}", r.server.loglevel));
    lines.push(format!("databases {}", num_dbs));
    if let (Some(host), port) = (
        r.replication.master_host.as_deref(),
        r.replication.master_port,
    ) {
        lines.push(format!("replicaof {} {}", host, port));
    }
    if let Some(ref cfg_path) = r.server.config_file {
        if config_file_contains_key(cfg_path, "unixsocket") {
            lines.push(format!("unixsocket {}", r.server.unixsocket));
        }
        if config_file_contains_key(cfg_path, "unixsocketgroup") {
            lines.push(format!("unixsocketgroup {}", r.server.unixsocketgroup));
        }
        if config_file_contains_key(cfg_path, "unixsocketperm") {
            lines.push(format!("unixsocketperm {:o}", r.server.unixsocketperm));
        }
    }
    lines.push(format!(
        "shutdown-on-sigterm {}",
        crate::engine::distributed::shutdown::format_sig_flags(r.server.shutdown_on_sigterm)
    ));
    lines.push(format!(
        "shutdown-on-sigint {}",
        crate::engine::distributed::shutdown::format_sig_flags(r.server.shutdown_on_sigint)
    ));
    lines.push(format!("shutdown-timeout {}", r.server.shutdown_timeout));
    if !r.server.notify_keyspace_events.is_empty() {
        lines.push(format!(
            "notify-keyspace-events {}",
            r.server.notify_keyspace_events
        ));
    }
    lines.push(format!(
        "latency-monitor-threshold {}",
        r.server.latency_monitor_threshold
    ));
    lines.push(format!("dir {}", r.persistence.dir));
    lines.push(format!("dbfilename {}", r.persistence.dbfilename));
    let save_value = r
        .persistence
        .saveparams
        .iter()
        .map(|sp| format!("{} {}", sp.seconds, sp.changes))
        .collect::<Vec<_>>()
        .join(" ");
    lines.push(format!("save {}", save_value));
    lines.push(format!(
        "appendonly {}",
        bool_to_yesno(r.persistence.aof_enabled)
    ));
    lines.push(format!("appendfilename {}", r.persistence.aof_filename));
    lines.push(format!("appenddirname {}", r.persistence.aof_appenddirname));
    lines.push(format!(
        "appendfsync {}",
        match r.persistence.aof_fsync {
            AofFsync::No => "no",
            AofFsync::EverySec => "everysec",
            AofFsync::Always => "always",
        }
    ));
    lines.push(format!(
        "no-appendfsync-on-rewrite {}",
        bool_to_yesno(r.persistence.aof_no_fsync_on_rewrite)
    ));
    lines.push(format!(
        "repl-diskless-load {}",
        r.replication.repl_diskless_load
    ));
    lines.push(format!(
        "rdb-version-check {}",
        if r.persistence.rdb_version_check_relaxed {
            "relaxed"
        } else {
            "strict"
        }
    ));
    lines.push(format!(
        "rdb-key-save-delay {}",
        r.persistence.rdb_key_save_delay
    ));
    lines.push(format!("key-load-delay {}", r.persistence.key_load_delay));
    lines.push(format!(
        "loading-process-events-interval-bytes {}",
        r.persistence.loading_process_events_interval_bytes
    ));
    lines.push(format!(
        "list-max-listpack-size {}",
        r.encoding.list_max_listpack_size
    ));
    lines.push(format!(
        "list-compress-depth {}",
        r.encoding.list_compress_depth
    ));
    lines.push(format!(
        "set-max-intset-entries {}",
        r.encoding.set_max_intset_entries
    ));
    lines.push(format!(
        "set-max-listpack-entries {}",
        r.encoding.set_max_listpack_entries
    ));
    lines.push(format!(
        "set-max-listpack-value {}",
        r.encoding.set_max_listpack_value
    ));
    lines.push(format!("mptcp {}", bool_to_yesno(r.server.mptcp)));
    lines.push(format!(
        "cluster-allow-pubsubshard-when-down {}",
        bool_to_yesno(r.server.cluster_allow_pubsubshard_when_down)
    ));
    lines.push(format!(
        "lazyfree-lazy-server-del {}",
        bool_to_yesno(r.server.lazyfree_lazy_server_del)
    ));
    lines.push(format!(
        "lazyfree-lazy-expire {}",
        bool_to_yesno(r.server.lazyfree_lazy_expire)
    ));
    lines.push(format!(
        "lazyfree-lazy-eviction {}",
        bool_to_yesno(r.server.lazyfree_lazy_eviction)
    ));
    lines.push(format!(
        "prefetch-batch-max-size {}",
        r.server.prefetch_batch_max_size
    ));
    lines.push(format!("ignore-warnings {}", r.server.ignore_warnings));
    lines.push(format!(
        "activedefrag {}",
        bool_to_yesno(r.server.activedefrag)
    ));
    lines.push(format!(
        "active-defrag-cycle-min {}",
        r.server.active_defrag_cycle_min
    ));
    lines.push(format!(
        "active-defrag-cycle-max {}",
        r.server.active_defrag_cycle_max
    ));
    lines.push(format!(
        "active-defrag-threshold-lower {}",
        r.server.active_defrag_threshold_lower
    ));
    lines.push(format!(
        "active-defrag-threshold-upper {}",
        r.server.active_defrag_threshold_upper
    ));
    lines.push(format!(
        "active-defrag-cycle-us {}",
        r.server.active_defrag_cycle_us
    ));
    lines.push(format!(
        "active-defrag-ignore-bytes {}",
        r.server.active_defrag_ignore_bytes
    ));
    lines.push(format!(
        "active-defrag-max-scan-fields {}",
        r.server.active_defrag_max_scan_fields
    ));
    lines.push(format!(
        "proto-max-bulk-len {}",
        r.server.proto_max_bulk_len
    ));
    lines.push(format!(
        "tracking-table-max-keys {}",
        r.server.tracking_table_max_keys
    ));
    lines.push(format!(
        "commandlog-execution-slower-than {}",
        r.server.commandlog_slow_threshold
    ));
    lines.push(format!(
        "commandlog-slow-execution-max-len {}",
        r.server.commandlog_slow_max_len
    ));
    lines.push(format!(
        "commandlog-request-larger-than {}",
        r.server.large_request_threshold
    ));
    lines.push(format!(
        "commandlog-large-request-max-len {}",
        r.server.large_request_max_len
    ));
    lines.push(format!(
        "commandlog-reply-larger-than {}",
        r.server.large_reply_threshold
    ));
    lines.push(format!(
        "commandlog-large-reply-max-len {}",
        r.server.large_reply_max_len
    ));
    lines.push(format!("maxmemory {}", r.server.maxmemory));
    lines.push(format!(
        "maxmemory-scripts {}",
        crate::engine::extension::config::server::scripts_memory::format(
            r.server.maxmemory_scripts
        )
    ));
    lines.push(format!("maxmemory-clients {}", r.server.maxmemory_clients));
    lines.push(format!(
        "hash-max-listpack-entries {}",
        r.encoding.hash_max_listpack_entries
    ));
    lines.push(format!(
        "hash-max-listpack-value {}",
        r.encoding.hash_max_listpack_value
    ));
    lines.push(format!(
        "zset-max-listpack-entries {}",
        r.encoding.zset_max_ziplist_entries
    ));
    lines.push(format!(
        "zset-max-listpack-value {}",
        r.encoding.zset_max_ziplist_value
    ));
    lines.push(format!(
        "slot-migration-max-failover-repl-bytes {}",
        r.server.slot_migration_max_failover_repl_bytes
    ));
    let rewrite_gossip_percentage = r.server.cluster_message_gossip_perc != 10
        || r.server
            .config_file
            .as_deref()
            .is_some_and(|path| config_file_contains_key(path, "cluster-message-gossip-perc"));
    if rewrite_gossip_percentage {
        lines.push(format!(
            "cluster-message-gossip-perc {}",
            r.server.cluster_message_gossip_perc
        ));
    }
    lines.push(format!(
        "protected-mode {}",
        bool_to_yesno(r.server.protected_mode)
    ));
    lines.push(format!(
        "enable-protected-configs {}",
        r.server.enable_protected_configs.as_str()
    ));
    lines.push(format!(
        "enable-debug-command {}",
        r.server.enable_debug_command.as_str()
    ));
    lines.push(format!(
        "enable-module-command {}",
        r.server.enable_module_command.as_str()
    ));
    for module in ctx.module_registry().list() {
        if module.is_static {
            continue;
        }
        if let Some((path, args)) = &module.load_path {
            let mut line = format!("loadmodule {}", path);
            for arg in args {
                line.push(' ');
                line.push_str(arg);
            }
            lines.push(line);
        }
    }
    for (old_name, new_name) in &r.server.rename_commands {
        lines.push(format!("rename-command {} {}", old_name, new_name));
    }
    if let Some(ref pw) = r.security.requirepass {
        lines.push(format!("requirepass {}", String::from_utf8_lossy(pw)));
    }
    if r.server.aclfile.is_empty() {
        let users = match acl_users().lock() {
            Ok(users) => users,
            Err(_) => return Value::error("ERR ACL user registry is unavailable"),
        };
        let roles = match acl_roles().lock() {
            Ok(roles) => roles,
            Err(_) => return Value::error("ERR ACL role registry is unavailable"),
        };
        let mut roles: Vec<_> = roles.iter().collect();
        roles.sort_by_key(|(name, _)| *name);
        for (name, role) in roles {
            lines.push(format!("role {}", role.describe_role(name)));
        }
        let mut users: Vec<_> = users.iter().collect();
        users.sort_by_key(|(name, _)| *name);
        for (username, user) in users {
            if username == "default" && user.is_factory_default_privileged() {
                continue;
            }
            lines.push(format!("user {}", user.to_acl_rule_string(username)));
        }
    }
    let oom_mode = match r.server.oom_score_adj {
        crate::engine::extension::config::server::OOM_SCORE_ADJ_NO => "no",
        crate::engine::extension::config::server::OOM_SCORE_RELATIVE => "yes",
        crate::engine::extension::config::server::OOM_SCORE_ADJ_ABSOLUTE => "absolute",
        _ => "no",
    };
    lines.push(format!("oom-score-adj {}", oom_mode));
    lines.push(format!(
        "oom-score-adj-values {} {} {}",
        r.server.oom_score_adj_values[0],
        r.server.oom_score_adj_values[1],
        r.server.oom_score_adj_values[2]
    ));
    lines.push(format!(
        "latency-tracking {}",
        bool_to_yesno(r.server.latency_tracking)
    ));
    let pct: Vec<String> = r
        .server
        .latency_tracking_info_percentiles
        .iter()
        .map(|v| {
            if v.fract() == 0.0 {
                format!("{}", *v as i64)
            } else {
                format!("{}", v)
            }
        })
        .collect();
    if pct.is_empty() {
        lines.push("latency-tracking-info-percentiles \"\"".to_string());
    } else {
        lines.push(format!(
            "latency-tracking-info-percentiles {}",
            pct.join(" ")
        ));
    }
    if r.server.cluster_enabled {
        lines.push("cluster-enabled yes".to_string());
        lines.push(format!(
            "cluster-config-file {}",
            r.server.cluster_config_file
        ));
        lines.push(format!("cluster-port {}", r.server.cluster_port));
        lines.push(format!(
            "cluster-node-timeout {}",
            r.server.cluster_node_timeout
        ));
        lines.push(format!("cluster-databases {}", r.server.cluster_databases));
        lines.push(format!(
            "cluster-slot-stats-enabled {}",
            bool_to_yesno(r.server.cluster_slot_stats_enabled)
        ));
    }

    let mut emitted: std::collections::HashSet<String> = lines
        .iter()
        .filter_map(|line| line.split_whitespace().next().map(|k| k.to_string()))
        .collect();
    for spec in crate::framework::commands::server::config::config_registry().all_specs() {
        if spec.is_hidden() {
            continue;
        }
        if matches!(spec.name, "bind" | "bind-source-addr") {
            continue;
        }
        if emitted.contains(spec.name) {
            continue;
        }
        emitted.insert(spec.name.to_string());
        let value = (spec.get)(r).as_str();
        let value = if spec.name.starts_with("tls-")
            && matches!(spec.type_, crate::engine::extension::config::spec::ConfigType::String)
        {
            quote_binary_config_value(&value)
        } else {
            value
        };
        lines.push(format!("{} {}", spec.name, value));
    }

    let content = lines.join("\n") + "\n";
    match fs::write(&path, content) {
        Ok(_) => Value::ok(),
        Err(e) => Value::error(format!("ERR writing config file: {}", e)),
    }
}

fn quote_binary_config_value(value: &str) -> String {
    use std::fmt::Write as _;

    let mut quoted = String::with_capacity(value.len() + 2);
    quoted.push('"');
    for byte in value.bytes() {
        match byte {
            b'\n' => quoted.push_str("\\n"),
            b'\r' => quoted.push_str("\\r"),
            b'\t' => quoted.push_str("\\t"),
            b'"' => quoted.push_str("\\\""),
            b'\\' => quoted.push_str("\\\\"),
            0x20..=0x7e => quoted.push(byte as char),
            _ => write!(quoted, "\\x{byte:02x}").expect("writing to String cannot fail"),
        }
    }
    quoted.push('"');
    quoted
}

fn config_file_contains_key(path: &str, key: &str) -> bool {
    let Ok(contents) = std::fs::read_to_string(path) else {
        return false;
    };
    let prefix = format!("{} ", key);
    for raw in contents.lines() {
        let line = raw.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        if line.starts_with(&prefix) || line == key {
            return true;
        }
    }
    false
}

#[cfg(test)]
mod tests {
    use super::config_rewrite;
    use crate::foundation::io::protocol::Value;
    use crate::framework::context::ServerContext;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn loader_rewrite_preserves_dynamic_order_and_omits_static_module_paths() {
        use crate::engine::extension::module::ModuleRef;
        let path = std::env::temp_dir().join(format!(
            "versal-loader-rewrite-{}-{}.conf",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::write(&path, "loadmodule stale.so\n").unwrap();
        let mut ctx = ServerContext::new(1);
        ctx.config_mut().server.config_file = Some(path.to_string_lossy().into_owned());
        for (name, is_static) in [("infotest", false), ("datatype", false), ("lua", true)] {
            ctx.module_registry_mut()
                .register(ModuleRef::new(name, std::ptr::null_mut(), 1, is_static))
                .unwrap();
            assert!(ctx.module_registry_mut().set_load_path(
                name,
                format!("{name}.so"),
                vec!["argument".to_string()]
            ));
        }
        assert_eq!(config_rewrite(&mut ctx), Value::ok());
        let rewritten = std::fs::read_to_string(&path).unwrap();
        assert_eq!(
            rewritten
                .lines()
                .filter(|line| line.starts_with("loadmodule "))
                .collect::<Vec<_>>(),
            [
                "loadmodule infotest.so argument",
                "loadmodule datatype.so argument"
            ]
        );
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn rewrite_preserves_hidden_cluster_gossip_percentage() {
        let path = std::env::temp_dir().join(format!(
            "valkey-cluster-config-rewrite-{}-{}.conf",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::write(&path, "cluster-message-gossip-perc 10\n").unwrap();
        let mut context = ServerContext::new(1);
        context.config_mut().server.config_file = Some(path.to_string_lossy().into_owned());
        context.config_mut().server.cluster_message_gossip_perc = 80;

        assert_eq!(config_rewrite(&mut context), Value::ok());
        let rewritten = std::fs::read_to_string(&path).unwrap();
        let _ = std::fs::remove_file(&path);

        assert!(rewritten.contains("cluster-message-gossip-perc 80"));
    }

    #[test]
    fn rewrite_preserves_binary_hash_seed() {
        let path = std::env::temp_dir().join(format!(
            "valkey-hash-seed-rewrite-{}-{}.conf",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("system clock follows the Unix epoch")
                .as_nanos()
        ));
        std::fs::write(&path, "port 6379\n").expect("create config fixture");
        let mut context = ServerContext::new(1);
        context.config_mut().server.config_file = Some(path.to_string_lossy().into_owned());
        context.config_mut().server.hash_seed = "valkey\0AAAAA".to_string();

        assert_eq!(config_rewrite(&mut context), Value::ok());
        let rewritten = std::fs::read(&path).expect("read rewritten config");
        let _ = std::fs::remove_file(&path);

        assert!(rewritten
            .windows(b"hash-seed \"valkey\\x00AAAAA\"".len())
            .any(|window| window == b"hash-seed \"valkey\\x00AAAAA\""));
    }

    #[test]
    fn rewrite_persists_lazyfree_and_prefetch_controls() {
        let path = std::env::temp_dir().join(format!(
            "valkey-lazyfree-user-config-rewrite-{}-{}.conf",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("system clock follows the Unix epoch")
                .as_nanos()
        ));
        std::fs::write(&path, "port 6379\n").expect("create config fixture");
        let mut context = ServerContext::new(1);
        context.config_mut().server.config_file = Some(path.to_string_lossy().into_owned());
        context.config_mut().server.lazyfree_lazy_user_del = false;
        context.config_mut().server.lazyfree_lazy_user_flush = false;
        context.config_mut().server.prefetch_batch_max_size = 2;
        context.config_mut().server.ignore_warnings = "ARM64-COW-BUG".to_string();
        context.config_mut().server.activedefrag = true;
        context.config_mut().server.active_defrag_cycle_us = 1000;

        assert_eq!(config_rewrite(&mut context), Value::ok());
        let rewritten = std::fs::read_to_string(&path).expect("read rewritten config");
        let _ = std::fs::remove_file(&path);

        assert!(rewritten.contains("lazyfree-lazy-user-del no"));
        assert!(rewritten.contains("lazyfree-lazy-user-flush no"));
        assert!(rewritten.contains("prefetch-batch-max-size 2"));
        assert!(rewritten.contains("ignore-warnings ARM64-COW-BUG"));
        assert!(rewritten.contains("activedefrag yes"));
        assert!(rewritten.contains("active-defrag-cycle-us 1000"));
    }
}
