use super::Config;
use crate::engine::extension::acl::acl_users;

enum Setter {
    Str(fn(&mut Config, &str)),
    Lower(fn(&mut Config, String)),
    Bool(fn(&mut Config, bool)),
    ParsedI64(fn(&mut Config, i64)),
    ParsedI32(fn(&mut Config, i32)),
    ParsedU16(fn(&mut Config, u16)),
    ParsedU32(fn(&mut Config, u32)),
    ParsedU64(fn(&mut Config, u64)),
}

fn parse_signed_memory(value: &str) -> Option<i64> {
    let value = value.trim().to_ascii_lowercase();
    let split = value
        .find(|ch: char| !ch.is_ascii_digit() && ch != '-')
        .unwrap_or(value.len());
    let number = value[..split].parse::<i64>().ok()?;
    let multiplier = match &value[split..] {
        "" | "b" => 1,
        "k" | "kb" => 1024,
        "m" | "mb" => 1024 * 1024,
        "g" | "gb" => 1024 * 1024 * 1024,
        _ => return None,
    };
    number.checked_mul(multiplier)
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum ConfigQuote {
    Double,
    Single,
}

fn parse_config_arguments(value: &str) -> Result<Vec<String>, String> {
    let input = value.as_bytes();
    let mut index = 0;
    let mut arguments = Vec::new();
    while index < input.len() {
        while input.get(index).is_some_and(u8::is_ascii_whitespace) {
            index += 1;
        }
        if index == input.len() {
            break;
        }
        let bytes = parse_config_argument(input, &mut index)?;
        let argument = String::from_utf8(bytes)
            .map_err(|_| "configuration argument is not valid UTF-8".to_string())?;
        arguments.push(argument);
    }
    Ok(arguments)
}

fn parse_config_argument(input: &[u8], index: &mut usize) -> Result<Vec<u8>, String> {
    let mut output = Vec::new();
    let mut quote = None;
    while let Some(&byte) = input.get(*index) {
        match quote {
            None => {
                if !consume_unquoted_byte(byte, index, &mut output, &mut quote) {
                    break;
                }
            }
            Some(ConfigQuote::Double) => {
                if consume_double_quoted_byte(input, index, &mut output)? {
                    quote = None;
                }
            }
            Some(ConfigQuote::Single) => {
                if consume_single_quoted_byte(input, index, &mut output) {
                    quote = None;
                }
            }
        }
    }
    if quote.is_some() {
        return Err("unbalanced quotes in configuration argument".to_string());
    }
    Ok(output)
}

fn consume_unquoted_byte(
    byte: u8,
    index: &mut usize,
    output: &mut Vec<u8>,
    quote: &mut Option<ConfigQuote>,
) -> bool {
    if byte.is_ascii_whitespace() {
        return false;
    }
    *index += 1;
    match byte {
        b'"' => *quote = Some(ConfigQuote::Double),
        b'\'' => *quote = Some(ConfigQuote::Single),
        _ => output.push(byte),
    }
    true
}

fn consume_double_quoted_byte(
    input: &[u8],
    index: &mut usize,
    output: &mut Vec<u8>,
) -> Result<bool, String> {
    match input[*index] {
        b'"' => {
            *index += 1;
            Ok(true)
        }
        b'\\' => {
            consume_double_escape(input, index, output)?;
            Ok(false)
        }
        byte => {
            output.push(byte);
            *index += 1;
            Ok(false)
        }
    }
}

fn consume_single_quoted_byte(input: &[u8], index: &mut usize, output: &mut Vec<u8>) -> bool {
    if input[*index] == b'\'' {
        *index += 1;
        return true;
    }
    if input[*index] == b'\\' && input.get(*index + 1) == Some(&b'\'') {
        output.push(b'\'');
        *index += 2;
    } else {
        output.push(input[*index]);
        *index += 1;
    }
    false
}

fn consume_double_escape(
    input: &[u8],
    index: &mut usize,
    output: &mut Vec<u8>,
) -> Result<(), String> {
    let escaped = *input
        .get(*index + 1)
        .ok_or_else(|| "unfinished configuration escape".to_string())?;
    if escaped == b'x'
        && input
            .get(*index + 2..*index + 4)
            .is_some_and(valid_hex_pair)
    {
        let hex = std::str::from_utf8(&input[*index + 2..*index + 4]).expect("ASCII hex digits");
        output.push(u8::from_str_radix(hex, 16).expect("validated hex digits"));
        *index += 4;
        return Ok(());
    }
    output.push(match escaped {
        b'n' => b'\n',
        b'r' => b'\r',
        b't' => b'\t',
        b'b' => 8,
        b'a' => 7,
        byte => byte,
    });
    *index += 2;
    Ok(())
}

fn valid_hex_pair(bytes: &[u8]) -> bool {
    bytes.len() == 2 && bytes.iter().all(u8::is_ascii_hexdigit)
}

pub(crate) fn encode_config_argument(value: &str) -> String {
    let mut encoded = String::from("\"");
    for &byte in value.as_bytes() {
        match byte {
            b'\\' | b'"' => encoded.push_str(&format!("\\{}", char::from(byte))),
            b'\n' => encoded.push_str("\\n"),
            b'\r' => encoded.push_str("\\r"),
            b'\t' => encoded.push_str("\\t"),
            7 => encoded.push_str("\\a"),
            8 => encoded.push_str("\\b"),
            0x20..=0x7e => encoded.push(char::from(byte)),
            _ => encoded.push_str(&format!("\\x{byte:02x}")),
        }
    }
    encoded.push('"');
    encoded
}

#[derive(Debug, PartialEq, Eq)]
enum ReplicaOfConfig {
    NoOne,
    Target { host: String, port: u16 },
}

fn parse_replicaof_config(value: &str) -> Result<ReplicaOfConfig, String> {
    let arguments = parse_config_arguments(value).map_err(|error| format!("replicaof: {error}"))?;
    let [host, port] = arguments.as_slice() else {
        return Err("replicaof has wrong number of arguments".to_string());
    };
    if host.eq_ignore_ascii_case("no") && port.eq_ignore_ascii_case("one") {
        return Ok(ReplicaOfConfig::NoOne);
    }
    let port =
        parse_replicaof_port(port).map_err(|_| "replicaof has invalid primary port".to_string())?;
    Ok(ReplicaOfConfig::Target {
        host: host.clone(),
        port,
    })
}

pub(crate) fn parse_replicaof_port(value: &str) -> Result<u16, ()> {
    if value.is_empty() {
        return Ok(0);
    }
    let bytes = value.as_bytes();
    let mut index = 0;
    while bytes.get(index).is_some_and(u8::is_ascii_whitespace) {
        index += 1;
    }
    let is_negative = bytes.get(index) == Some(&b'-');
    if is_negative || bytes.get(index) == Some(&b'+') {
        index += 1;
    }
    let digit_start = index;
    let mut port = 0u32;
    while let Some(digit) = bytes.get(index).and_then(|byte| byte.checked_sub(b'0')) {
        if digit > 9 {
            break;
        }
        port = port
            .checked_mul(10)
            .and_then(|value| value.checked_add(u32::from(digit)))
            .ok_or(())?;
        index += 1;
    }
    if index == digit_start || index != bytes.len() || port > u32::from(u16::MAX) {
        return Err(());
    }
    if is_negative && port != 0 {
        return Err(());
    }
    Ok(port as u16)
}

pub(crate) fn validate_config_file_directives(contents: &str) -> Result<(), String> {
    for raw_line in contents.lines() {
        let line = raw_line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let mut parts = line.splitn(2, char::is_whitespace);
        let Some(key) = parts.next() else { continue };
        let raw_value = parts.next();
        let key = key.to_ascii_lowercase();
        if key == "replicaof" {
            parse_replicaof_config(raw_value.unwrap_or_default().trim())?;
            continue;
        }
        if key == "cluster-replica-priority" {
            let value = decode_config_value(raw_value.unwrap_or_default().trim());
            if !value
                .parse::<u32>()
                .is_ok_and(|number| number <= i32::MAX as u32)
            {
                return Err(format!("invalid value for {key}"));
            }
            continue;
        }
        let Some(raw_value) = raw_value else { continue };
        let value = decode_config_value(raw_value.trim());
        let valid = match key.as_str() {
            "commandlog-execution-slower-than" | "slowlog-log-slower-than" => {
                value.parse::<i64>().is_ok_and(|number| number >= -1)
            }
            "commandlog-slow-execution-max-len"
            | "slowlog-max-len"
            | "commandlog-large-request-max-len"
            | "commandlog-large-reply-max-len" => {
                value.parse::<i64>().is_ok_and(|number| number >= 0)
            }
            "commandlog-request-larger-than" | "commandlog-reply-larger-than" => {
                parse_signed_memory(&value).is_some_and(|number| number >= -1)
            }
            "acl-pubsub-default" => matches!(
                value.to_ascii_lowercase().as_str(),
                "allchannels" | "resetchannels"
            ),
            "appendfilename" | "appenddirname" => {
                crate::engine::extension::config::persistence::validate_aof_basename(&value, &key)
                    .is_ok()
            }
            _ => continue,
        };
        if !valid {
            return Err(format!("invalid value for {key}"));
        }
    }
    Ok(())
}

#[rustfmt::skip]
static SIMPLE_CONFIGS: &[(&str, Setter)] = &[
    ("bind",                              Setter::Str(|c, v| c.bind = v.to_string())),
    ("loglevel",                          Setter::Str(|c, v| c.loglevel = v.to_string())),
    ("logfile",                           Setter::Str(|c, v| c.logfile = v.to_string())),
    ("log-format",                        Setter::Lower(|c, v| c.log_format = v)),
    ("log-timestamp-format",              Setter::Lower(|c, v| c.log_timestamp_format = v)),
    ("debug-context",                     Setter::Str(|c, v| c.debug_context = v.to_string())),
    ("crash-log-enabled",                 Setter::Bool(|c, v| c.crash_log_enabled = v)),
    ("syslog-enabled",                    Setter::Bool(|c, v| c.syslog_enabled = v)),
    ("syslog-ident",                      Setter::Str(|c, v| c.syslog_ident = v.to_string())),
    ("syslog-facility",                   Setter::Lower(|c, v| c.syslog_facility = v)),
    ("hide-user-data-from-log",           Setter::Str(|c, v| c.hide_user_data_from_log = v.to_string())),
    ("availability-zone",                 Setter::Str(|c, v| c.availability_zone = v.to_string())),
    ("watchdog-period",                  Setter::ParsedU64(|c, v| c.watchdog_period = v)),
    ("crash-memcheck-enabled",            Setter::Bool(|c, v| c.crash_memcheck_enabled = v)),
    ("use-exit-on-panic",                Setter::Bool(|c, v| c.use_exit_on_panic = v)),
    ("requirepass",                       Setter::Str(|c, v| c.requirepass = Some(v.as_bytes().to_vec()))),
    ("dir",                               Setter::Str(|c, v| c.dir = v.to_string())),
    ("dbfilename",                        Setter::Str(|c, v| c.dbfilename = v.to_string())),
    ("shutdown-on-sigterm",               Setter::Str(|c, v| c.shutdown_on_sigterm = v.split_whitespace().map(|s| s.to_string()).collect())),
    ("shutdown-on-sigint",                Setter::Str(|c, v| c.shutdown_on_sigint = v.split_whitespace().map(|s| s.to_string()).collect())),
    ("appendfilename",                    Setter::Str(|c, v| c.appendfilename = v.to_string())),
    ("appenddirname",                     Setter::Str(|c, v| c.appenddirname = v.to_string())),
    ("enable-module-command",             Setter::Str(|c, v| c.enable_module_command = v.to_string())),
    ("enable-protected-configs",          Setter::Str(|c, v| c.enable_protected_configs = v.to_string())),
    ("enable-debug-command",              Setter::Str(|c, v| c.enable_debug_command = v.to_string())),
    ("protected-mode",                    Setter::Str(|c, v| c.protected_mode = v.to_string())),
    ("cluster-config-file",               Setter::Str(|c, v| c.cluster_config_file = v.to_string())),
    ("cluster-announce-ip",               Setter::Str(|c, v| c.cluster_announce_ip = v.to_string())),
    ("cluster-announce-client-ipv4",       Setter::Str(|c, v| c.cluster_announce_client_ipv4 = v.to_string())),
    ("cluster-announce-client-ipv6",       Setter::Str(|c, v| c.cluster_announce_client_ipv6 = v.to_string())),
    ("cluster-announce-hostname",         Setter::Str(|c, v| c.cluster_announce_hostname = v.to_string())),
    ("cluster-announce-human-nodename",   Setter::Str(|c, v| c.cluster_announce_human_nodename = v.to_string())),
    ("cluster-config-save-behavior",       Setter::Lower(|c, v| c.cluster_config_save_behavior = v)),
    ("aclfile",                           Setter::Str(|c, v| c.aclfile = v.to_string())),
    ("acl-pubsub-default",                Setter::Lower(|c, v| if matches!(v.as_str(), "allchannels" | "resetchannels") { c.acl_pubsub_default = v })),
    ("unixsocket",                        Setter::Str(|c, v| c.unixsocket = v.to_string())),
    ("unixsocketgroup",                   Setter::Str(|c, v| c.unixsocketgroup = v.to_string())),
    ("proc-title-template",               Setter::Str(|c, v| c.proc_title_template = v.to_string())),
    ("set-proc-title",                    Setter::Bool(|c, v| c.set_proc_title = v)),
    ("always-show-logo",                  Setter::Bool(|c, v| c.always_show_logo = v)),
    ("appendfsync",                       Setter::Lower(|c, v| c.appendfsync = v)),
    ("repl-diskless-load",                Setter::Lower(|c, v| c.repl_diskless_load = v)),
    ("rdb-version-check",                 Setter::Lower(|c, v| c.rdb_version_check = v)),
    ("daemonize",                         Setter::Bool(|c, v| c.daemonize = v)),
    ("supervised",                        Setter::Lower(|c, v| c.supervised = v)),
    ("server-cpulist",                    Setter::Str(|c, v| c.server_cpulist = v.to_string())),
    ("server_cpulist",                    Setter::Str(|c, v| c.server_cpulist = v.to_string())),
    ("bio-cpulist",                       Setter::Str(|c, v| c.bio_cpulist = v.to_string())),
    ("bio_cpulist",                       Setter::Str(|c, v| c.bio_cpulist = v.to_string())),
    ("aof-rewrite-cpulist",               Setter::Str(|c, v| c.aof_rewrite_cpulist = v.to_string())),
    ("aof_rewrite_cpulist",               Setter::Str(|c, v| c.aof_rewrite_cpulist = v.to_string())),
    ("bgsave-cpulist",                    Setter::Str(|c, v| c.bgsave_cpulist = v.to_string())),
    ("bgsave_cpulist",                    Setter::Str(|c, v| c.bgsave_cpulist = v.to_string())),
    ("ignore-warnings",                   Setter::Str(|c, v| c.ignore_warnings = v.to_string())),
    ("appendonly",                        Setter::Bool(|c, v| c.appendonly = v)),
    ("rdb-save-incremental-fsync",        Setter::Bool(|c, v| c.rdb_save_incremental_fsync = v)),
    ("rdb-del-sync-files",                Setter::Bool(|c, v| c.rdb_del_sync_files = v)),
    ("aof-rewrite-incremental-fsync",     Setter::Bool(|c, v| c.aof_rewrite_incremental_fsync = v)),
    ("aof-use-rdb-preamble",              Setter::Bool(|c, v| c.aof_use_rdb_preamble = v)),
    ("aof-load-truncated",                Setter::Bool(|c, v| c.aof_load_truncated = v)),
    ("aof-timestamp-enabled",             Setter::Bool(|c, v| c.aof_timestamp_enabled = v)),
    ("aof-disable-auto-gc",               Setter::Bool(|c, v| c.aof_disable_auto_gc = v)),
    ("repl-diskless-sync",                Setter::Bool(|c, v| c.repl_diskless_sync = v)),
    ("no-appendfsync-on-rewrite",         Setter::Bool(|c, v| c.no_appendfsync_on_rewrite = v)),
    ("lazyfree-lazy-expire",              Setter::Bool(|c, v| c.lazyfree_lazy_expire = v)),
    ("lazyfree-lazy-eviction",            Setter::Bool(|c, v| c.lazyfree_lazy_eviction = v)),
    ("lazyfree-lazy-server-del",          Setter::Bool(|c, v| c.lazyfree_lazy_server_del = v)),
    ("lazyfree-lazy-user-del",            Setter::Bool(|c, v| c.lazyfree_lazy_user_del = v)),
    ("lazyfree-lazy-user-flush",          Setter::Bool(|c, v| c.lazyfree_lazy_user_flush = v)),
    ("mptcp",                             Setter::Bool(|c, v| c.mptcp = v)),
    ("pidfile",                           Setter::Str(|c, v| c.pidfile = v.to_string())),
    ("cluster-enabled",                   Setter::Bool(|c, v| c.cluster_enabled = v)),
    ("cluster-slot-stats-enabled",        Setter::Bool(|c, v| c.cluster_slot_stats_enabled = v)),
    ("cluster-allow-pubsubshard-when-down", Setter::Bool(|c, v| c.cluster_allow_pubsubshard_when_down = v)),
    ("port",                              Setter::ParsedU16(|c, v| c.port = v)),
    ("tls-port",                          Setter::ParsedU16(|c, v| c.tls_port = v)),
    ("tls-cert-file",                     Setter::Str(|c, v| c.tls_cert_file = v.to_string())),
    ("tls-key-file",                      Setter::Str(|c, v| c.tls_key_file = v.to_string())),
    ("tls-ca-cert-file",                  Setter::Str(|c, v| c.tls_ca_cert_file = v.to_string())),
    ("tls-ca-cert-dir",                   Setter::Str(|c, v| c.tls_ca_cert_dir = v.to_string())),
    ("tls-auth-clients",                  Setter::Lower(|c, v| c.tls_auth_clients = v)),
    ("tls-protocols",                     Setter::Str(|c, v| c.tls_protocols = v.to_string())),
    ("tls-ciphers",                       Setter::Str(|c, v| c.tls_ciphers = v.to_string())),
    ("tls-ciphersuites",                  Setter::Str(|c, v| c.tls_ciphersuites = v.to_string())),
    ("tls-prefer-server-ciphers",         Setter::Bool(|c, v| c.tls_prefer_server_ciphers = v)),
    ("tls-session-caching",               Setter::Bool(|c, v| c.tls_session_caching = v)),
    ("tls-session-cache-size",            Setter::ParsedI32(|c, v| c.tls_session_cache_size = v)),
    ("tls-replication",                   Setter::Bool(|c, v| c.tls_replication = v)),
    ("tls-cluster",                       Setter::Bool(|c, v| c.tls_cluster = v)),
    ("tls-client-cert-file",              Setter::Str(|c, v| c.tls_client_cert_file = v.to_string())),
    ("tls-client-key-file",               Setter::Str(|c, v| c.tls_client_key_file = v.to_string())),
    ("cluster-port",                      Setter::ParsedU16(|c, v| c.cluster_port = v)),
    ("cluster-ping-interval",             Setter::ParsedU64(|c, v| c.cluster_ping_interval = v)),
    ("cluster-announce-port",             Setter::ParsedU16(|c, v| c.cluster_announce_port = v)),
    ("cluster-announce-tls-port",         Setter::ParsedU16(|c, v| c.cluster_announce_tls_port = v)),
    ("cluster-announce-client-port",      Setter::ParsedU16(|c, v| c.cluster_announce_client_port = v)),
    ("cluster-announce-client-tls-port",  Setter::ParsedU16(|c, v| c.cluster_announce_client_tls_port = v)),
    ("cluster-announce-bus-port",         Setter::ParsedU16(|c, v| c.cluster_announce_bus_port = v)),
    ("cluster-databases",                 Setter::ParsedU16(|c, v| c.cluster_databases = v)),
    ("databases",                         Setter::ParsedU32(|c, v| c.databases = v)),
    ("cluster-node-timeout",              Setter::ParsedU64(|c, v| c.cluster_node_timeout = v)),
    ("repl-timeout",                      Setter::ParsedU64(|c, v| c.repl_timeout = v)),
    ("replica-announce-port",             Setter::ParsedU16(|c, v| c.replica_announce_port = v)),
    ("slave-announce-port",               Setter::ParsedU16(|c, v| c.replica_announce_port = v)),
    ("replica-ignore-maxmemory",          Setter::Bool(|c, v| c.replica_ignore_maxmemory = v)),
    ("slave-ignore-maxmemory",            Setter::Bool(|c, v| c.replica_ignore_maxmemory = v)),
    ("replica-ignore-disk-write-errors",  Setter::Bool(|c, v| c.replica_ignore_disk_write_errors = v)),
    ("replica-lazy-flush",                Setter::Bool(|c, v| c.replica_lazy_flush = v)),
    ("slave-lazy-flush",                  Setter::Bool(|c, v| c.replica_lazy_flush = v)),
    ("repl-disable-tcp-nodelay",          Setter::Bool(|c, v| c.repl_disable_tcp_nodelay = v)),
    ("repl-mptcp",                        Setter::Bool(|c, v| c.repl_mptcp = v)),
    ("repl-diskless-sync-delay",          Setter::ParsedI64(|c, v| c.repl_diskless_sync_delay = v)),
    ("shutdown-timeout",                  Setter::ParsedU64(|c, v| c.shutdown_timeout = v)),
    ("cluster-migration-barrier",          Setter::ParsedI64(|c, v| c.cluster_migration_barrier = v)),
    ("cluster-blacklist-ttl",              Setter::ParsedU64(|c, v| c.cluster_blacklist_ttl = v)),
    ("cluster-message-gossip-perc",        Setter::ParsedI64(|c, v| c.cluster_message_gossip_perc = v)),
    ("rdb-key-save-delay",                Setter::ParsedI64(|c, v| if (i32::MIN as i64..=i32::MAX as i64).contains(&v) { c.rdb_key_save_delay = v })),
    ("key-load-delay",                    Setter::ParsedI64(|c, v| if (i32::MIN as i64..=i32::MAX as i64).contains(&v) { c.key_load_delay = v })),
    ("loading-process-events-interval-bytes", Setter::ParsedU64(|c, v| if (1024..=i32::MAX as u64).contains(&v) { c.loading_process_events_interval_bytes = v })),
    ("tcp-keepalive",                     Setter::ParsedU64(|c, v| c.tcp_keepalive = v)),
    ("prefetch-batch-max-size",           Setter::ParsedU64(|c, v| if v <= 128 { c.prefetch_batch_max_size = v as usize })),
    ("active-expire-effort",              Setter::ParsedU64(|c, v| if (1..=10).contains(&v) { c.active_expire_effort = v as u8 })),
    ("activedefrag",                     Setter::Bool(|c, v| c.activedefrag = v)),
    ("active-defrag-cycle-min",          Setter::ParsedU64(|c, v| if (1..=99).contains(&v) { c.active_defrag_cycle_min = v as u8 })),
    ("active-defrag-cycle-max",          Setter::ParsedU64(|c, v| if (1..=99).contains(&v) { c.active_defrag_cycle_max = v as u8 })),
    ("active-defrag-threshold-lower",    Setter::ParsedU64(|c, v| if v <= 1000 { c.active_defrag_threshold_lower = v as u16 })),
    ("active-defrag-threshold-upper",    Setter::ParsedU64(|c, v| if v <= 1000 { c.active_defrag_threshold_upper = v as u16 })),
    ("active-defrag-cycle-us",           Setter::ParsedU64(|c, v| if v <= 100_000 { c.active_defrag_cycle_us = v })),
    ("active-defrag-ignore-bytes",       Setter::ParsedU64(|c, v| if v > 0 { c.active_defrag_ignore_bytes = v as usize })),
    ("active-defrag-max-scan-fields",    Setter::ParsedU64(|c, v| if v > 0 { c.active_defrag_max_scan_fields = v as usize })),
    ("set-max-intset-entries",            Setter::ParsedI64(|c, v| c.set_max_intset_entries = v)),
    ("set-max-listpack-entries",          Setter::ParsedI64(|c, v| c.set_max_listpack_entries = v)),
    ("set-max-listpack-value",            Setter::ParsedI64(|c, v| c.set_max_listpack_value = v)),
    ("stream-node-max-entries",           Setter::ParsedI64(|c, v| c.stream_node_max_entries = v)),
    ("stream-node-max-bytes",             Setter::ParsedI64(|c, v| c.stream_node_max_bytes = v)),
    ("commandlog-execution-slower-than",  Setter::ParsedI64(|c, v| if v >= -1 { c.commandlog_slow_threshold = v })),
    ("slowlog-log-slower-than",           Setter::ParsedI64(|c, v| if v >= -1 { c.commandlog_slow_threshold = v })),
    ("commandlog-slow-execution-max-len", Setter::ParsedI64(|c, v| if v >= 0 { c.commandlog_slow_max_len = v })),
    ("slowlog-max-len",                   Setter::ParsedI64(|c, v| if v >= 0 { c.commandlog_slow_max_len = v })),
    ("commandlog-request-larger-than",    Setter::Str(|c, v| if let Some(n) = parse_signed_memory(v) { if n >= -1 { c.commandlog_large_request_threshold = n } })),
    ("commandlog-large-request-max-len",  Setter::ParsedI64(|c, v| if v >= 0 { c.commandlog_large_request_max_len = v })),
    ("commandlog-reply-larger-than",      Setter::Str(|c, v| if let Some(n) = parse_signed_memory(v) { if n >= -1 { c.commandlog_large_reply_threshold = n } })),
    ("commandlog-large-reply-max-len",    Setter::ParsedI64(|c, v| if v >= 0 { c.commandlog_large_reply_max_len = v })),
];

fn apply_replicaof(config: &mut Config, value: &str) {
    match parse_replicaof_config(value) {
        Ok(ReplicaOfConfig::NoOne) => {
            config.replicaof_host = None;
            config.replicaof_port = 0;
        }
        Ok(ReplicaOfConfig::Target { host, port }) => {
            config.replicaof_host = Some(host);
            config.replicaof_port = port;
        }
        Err(_) => {}
    }
}

#[rustfmt::skip]
static EMPTY_CONFIGS: &[(&str, fn(&mut Config))] = &[
    ("requirepass",                     |c| c.requirepass = None),
    ("save",                            |c| c.save = Some(Vec::new())),
    ("bind",                            |c| c.bind.clear()),
    ("cluster-announce-ip",             |c| c.cluster_announce_ip.clear()),
    ("cluster-announce-client-ipv4",    |c| c.cluster_announce_client_ipv4.clear()),
    ("cluster-announce-client-ipv6",    |c| c.cluster_announce_client_ipv6.clear()),
    ("cluster-announce-hostname",       |c| c.cluster_announce_hostname.clear()),
    ("cluster-announce-human-nodename", |c| c.cluster_announce_human_nodename.clear()),
    ("availability-zone",               |c| c.availability_zone.clear()),
];

impl Config {
    fn apply_simple(&mut self, setter: &Setter, val_trimmed: &str) {
        match setter {
            Setter::Str(f) => f(self, val_trimmed),
            Setter::Lower(f) => f(self, val_trimmed.to_ascii_lowercase()),
            Setter::Bool(f) => f(
                self,
                matches!(val_trimmed.to_ascii_lowercase().as_str(), "yes" | "true"),
            ),
            numeric => self.apply_numeric(numeric, val_trimmed),
        }
    }

    fn apply_numeric(&mut self, setter: &Setter, value: &str) {
        match setter {
            Setter::ParsedI64(f) => {
                if let Ok(n) = value.parse::<i64>() {
                    f(self, n)
                }
            }
            Setter::ParsedI32(f) => {
                if let Ok(n) = value.parse::<i32>() {
                    f(self, n)
                }
            }
            Setter::ParsedU16(f) => {
                if let Ok(n) = value.parse::<u16>() {
                    f(self, n)
                }
            }
            Setter::ParsedU32(f) => {
                if let Ok(n) = value.parse::<u32>() {
                    f(self, n)
                }
            }
            Setter::ParsedU64(f) => {
                if let Ok(n) = value.parse::<u64>() {
                    f(self, n)
                }
            }
            _ => unreachable!("non-numeric config setters are handled before numeric setters"),
        }
    }

    pub fn parse_config_string(&mut self, contents: &str) {
        self.parse_config_string_inner(contents, false, false);
    }

    /// Parse a config-file style string. When `skip_cli_set` is true, directives
    /// whose key was explicitly set on the command line (`self.cli_set_keys`)
    /// are ignored, so a config file cannot clobber a CLI-provided value —
    /// matching C valkey's "config file first, CLI appended last, last wins".
    pub fn parse_config_string_filtered(&mut self, contents: &str, skip_cli_set: bool) {
        self.parse_config_string_inner(contents, skip_cli_set, true);
    }

    fn parse_config_string_inner(
        &mut self,
        contents: &str,
        skip_cli_set: bool,
        capture_registry: bool,
    ) {
        for line in contents.lines() {
            self.parse_config_line(line, skip_cli_set, capture_registry);
        }
    }

    fn parse_config_line(&mut self, line: &str, skip_cli_set: bool, capture_registry: bool) {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            return;
        }
        let mut parts = line.splitn(2, |c: char| c.is_whitespace());
        let key = parts.next().unwrap_or_default().to_ascii_lowercase();
        self.remember_explicit_key(&key);
        if skip_cli_set && self.cli_set_keys.contains(&key) {
            return;
        }
        let Some(raw_value) = parts.next() else {
            self.apply_empty_config_value(&key);
            return;
        };
        if key == "replicaof" {
            apply_replicaof(self, raw_value.trim());
            return;
        }
        let decoded = decode_config_value(raw_value.trim());
        if decoded.is_empty() {
            self.apply_empty_config_value(&key);
        } else if let Some((_, setter)) = SIMPLE_CONFIGS.iter().find(|(name, _)| *name == key) {
            self.apply_simple(setter, &decoded);
        } else {
            self.apply_special_config(&key, &decoded, raw_value.trim(), capture_registry);
        }
    }

    fn remember_explicit_key(&mut self, key: &str) {
        if !self.explicit_config_keys.iter().any(|known| known == key) {
            self.explicit_config_keys.push(key.to_string());
        }
    }

    fn apply_empty_config_value(&mut self, key: &str) {
        if let Some((_, clear)) = EMPTY_CONFIGS.iter().find(|(name, _)| *name == key) {
            clear(self);
        }
    }

    fn apply_special_config(
        &mut self,
        key: &str,
        value: &str,
        raw_value: &str,
        capture_registry: bool,
    ) {
        if self.apply_bounded_numeric_config(key, value) {
            return;
        }
        match key {
            "save" => self.prepend_save_config(value),
            "sentinel" => self.sentinel_directives.push(raw_value.to_string()),
            "user" => self.apply_user_config(value),
            "rename-command" => self.apply_rename_command(value),
            "loadmodule" => self.apply_loadmodule(value),
            _ if capture_registry => self.capture_registry_config(key, value),
            _ => {}
        }
    }

    fn apply_bounded_numeric_config(&mut self, key: &str, value: &str) -> bool {
        match key {
            "list-max-listpack-size" | "list-max-ziplist-size" => {
                value
                    .parse::<i64>()
                    .map(|n| self.list_max_listpack_size = n)
                    .ok();
            }
            "unixsocketperm" => {
                // C parses this permission mask as octal.
                u32::from_str_radix(value, 8)
                    .map(|n| self.unixsocketperm = n)
                    .ok();
            }
            _ => return self.apply_connection_limit(key, value),
        }
        true
    }

    fn apply_connection_limit(&mut self, key: &str, value: &str) -> bool {
        match key {
            "maxclients" => {
                if let Some(value) = value.parse::<u32>().ok().filter(|n| *n >= 1) {
                    self.maxclients = value;
                }
            }
            "max-new-connections-per-cycle" => {
                if let Some(value) = value.parse::<u16>().ok().filter(|n| (1..=1000).contains(n)) {
                    self.max_new_connections_per_cycle = value;
                }
            }
            "max-new-tls-connections-per-cycle" => {
                if let Some(value) = value.parse::<u16>().ok().filter(|n| (1..=1000).contains(n)) {
                    self.max_new_tls_connections_per_cycle = value;
                }
            }
            "timeout" => {
                if let Some(value) = value.parse::<u64>().ok().filter(|n| *n <= i32::MAX as u64) {
                    self.client_timeout = value;
                }
            }
            "tcp-backlog" => {
                if let Some(value) = value.parse::<u32>().ok().filter(|n| *n <= i32::MAX as u32) {
                    self.tcp_backlog = value;
                }
            }
            _ => return false,
        }
        true
    }

    fn prepend_save_config(&mut self, value: &str) {
        let parsed = value.split_whitespace().map(str::to_string);
        self.save = match self.save.take() {
            Some(existing) if !existing.is_empty() => Some(parsed.chain(existing).collect()),
            Some(existing) => Some(existing),
            None => Some(parsed.collect()),
        };
    }

    fn apply_user_config(&mut self, value: &str) {
        let user_parts: Vec<&str> = value.split_whitespace().collect();
        let Some((username, rules)) = user_parts.split_first() else {
            return;
        };
        let users = acl_users();
        let mut users = users.lock().unwrap();
        if let Some(user) = users.get_mut(*username) {
            if username.eq_ignore_ascii_case("default") && !rules.is_empty() {
                let _ = user.apply_rule("reset");
                for rule in rules {
                    let _ = user.apply_rule(rule);
                }
                return;
            }
            eprintln!("Error in user declaration '{}': Duplicate user found. A user can only be defined once in config files", username);
            std::process::exit(1);
        }
        let mut user = crate::engine::extension::acl::AclUser::new_disabled();
        for rule in rules {
            let _ = user.apply_rule(rule);
        }
        users.insert(username.to_string(), user);
    }

    fn apply_rename_command(&mut self, value: &str) {
        let mut parts = value.splitn(2, |c: char| c.is_whitespace());
        if let (Some(old_name), Some(new_name)) = (parts.next(), parts.next()) {
            self.rename_commands
                .push((old_name.to_ascii_lowercase(), new_name.to_ascii_lowercase()));
        }
    }

    fn apply_loadmodule(&mut self, value: &str) {
        let parts: Vec<String> = value.split_whitespace().map(str::to_string).collect();
        if let Some((path, args)) = parts.split_first() {
            self.load_modules.push((path.clone(), args.to_vec()));
        }
    }

    fn capture_registry_config(&mut self, key: &str, value: &str) {
        // Registry-only directives are replayed by Server::apply_config. The
        // CLI fallback already records unknown command-line flags directly.
        if crate::framework::commands::server::config::config_registry()
            .lookup(key)
            .is_some()
        {
            self.extra_cli_configs
                .push((key.to_string(), value.to_string()));
        }
    }
}

fn decode_config_value(value: &str) -> String {
    if value.len() < 2 {
        return value.to_owned();
    }
    let bytes = value.as_bytes();
    if bytes[0] == b'\'' && bytes[value.len() - 1] == b'\'' {
        return value[1..value.len() - 1].to_owned();
    }
    if bytes[0] != b'"' || bytes[value.len() - 1] != b'"' {
        return value.to_owned();
    }

    let encoded = &bytes[1..bytes.len() - 1];
    let mut decoded = Vec::with_capacity(encoded.len());
    let mut index = 0usize;
    while index < encoded.len() {
        let byte = encoded[index];
        index += 1;
        if byte != b'\\' {
            decoded.push(byte);
            continue;
        }
        decode_config_escape(encoded, &mut index, &mut decoded);
    }
    String::from_utf8(decoded)
        .unwrap_or_else(|error| String::from_utf8_lossy(error.as_bytes()).into_owned())
}

fn decode_config_escape(encoded: &[u8], index: &mut usize, decoded: &mut Vec<u8>) {
    let Some(&escaped) = encoded.get(*index) else {
        decoded.push(b'\\');
        return;
    };
    *index += 1;
    if let Some(byte) = simple_config_escape(escaped) {
        decoded.push(byte);
        return;
    }
    if escaped == b'x' && *index + 1 < encoded.len() {
        let hex = &encoded[*index..*index + 2];
        if let Ok(hex) = std::str::from_utf8(hex) {
            if let Ok(decoded_byte) = u8::from_str_radix(hex, 16) {
                decoded.push(decoded_byte);
                *index += 2;
                return;
            }
        }
    }
    decoded.push(b'\\');
    decoded.push(escaped);
}

fn simple_config_escape(escaped: u8) -> Option<u8> {
    match escaped {
        b'n' => Some(b'\n'),
        b'r' => Some(b'\r'),
        b't' => Some(b'\t'),
        b'"' => Some(b'"'),
        b'\\' => Some(b'\\'),
        _ => None,
    }
}

#[cfg(test)]
mod quoted_value_tests {
    use super::{decode_config_value, parse_config_arguments};

    #[test]
    fn config_arguments_concatenate_quoted_and_unquoted_segments_like_sds() {
        let cases = [
            (
                "unquoted\" \"with' 'quotes string",
                vec!["unquoted with quotes", "string"],
            ),
            (
                "\"quoted\"' another 'quoted string",
                vec!["quoted another quoted", "string"],
            ),
            (
                "\"shell-like \"'\"'\"'\"' quote-escaping '",
                vec!["shell-like \"' quote-escaping "],
            ),
        ];

        for (input, expected) in cases {
            assert_eq!(parse_config_arguments(input).unwrap(), expected);
        }
    }

    #[test]
    fn double_quoted_config_values_decode_whitespace_escapes() {
        assert_eq!(
            decode_config_value("\" file seq \\n\\n.aof \""),
            " file seq \n\n.aof "
        );
        assert_eq!(decode_config_value("'literal value'"), "literal value");
        assert_eq!(decode_config_value("plain"), "plain");
    }

    #[test]
    fn double_quoted_config_values_decode_hex_bytes() {
        assert_eq!(decode_config_value("\"valkey\\x00AAAAA\""), "valkey\0AAAAA");
    }
}
