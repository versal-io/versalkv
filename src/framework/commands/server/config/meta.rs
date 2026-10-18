use crate::engine::extension::config::spec::{ConfigSpec, ConfigType as RegistryConfigType};
use crate::foundation::io::protocol::Value;
use crate::framework::{CommandArgs, CommandError};
use bytes::Bytes;

use super::config_registry;

#[derive(Clone, Copy, PartialEq)]
pub(super) enum ConfigType {
    Bool,
    Numeric,
    String,
    Enum,
    Special,
}

impl ConfigType {
    pub(super) fn as_str(&self) -> &'static str {
        match self {
            ConfigType::Bool => "bool",
            ConfigType::Numeric => "numeric",
            ConfigType::String => "string",
            ConfigType::Enum => "enum",
            ConfigType::Special => "special",
        }
    }
}

pub(super) struct ConfigMeta {
    pub(super) name: &'static str,
    pub(super) type_: ConfigType,
    pub(super) enum_values: &'static [&'static str],
    pub(super) numeric_min: i64,
    pub(super) numeric_max: i64,
    pub(super) flags: &'static [&'static str],
    pub(super) alias: Option<&'static str>,
}

impl ConfigMeta {
    fn from_registry_spec(spec: &ConfigSpec) -> Self {
        Self {
            name: spec.name,
            type_: match spec.type_ {
                RegistryConfigType::Bool => ConfigType::Bool,
                RegistryConfigType::Numeric => ConfigType::Numeric,
                RegistryConfigType::String => ConfigType::String,
                RegistryConfigType::Enum => ConfigType::Enum,
                RegistryConfigType::Special => ConfigType::Special,
            },
            enum_values: spec.enum_values,
            numeric_min: spec.numeric_min,
            numeric_max: spec.numeric_max,
            flags: spec.flags,
            alias: spec.alias,
        }
    }

    pub(super) fn new_bool(name: &'static str, flags: &'static [&'static str]) -> Self {
        Self {
            name,
            type_: ConfigType::Bool,
            enum_values: &[],
            numeric_min: 0,
            numeric_max: 0,
            flags,
            alias: None,
        }
    }
    pub(super) fn new_numeric(
        name: &'static str,
        min: i64,
        max: i64,
        flags: &'static [&'static str],
    ) -> Self {
        Self {
            name,
            type_: ConfigType::Numeric,
            enum_values: &[],
            numeric_min: min,
            numeric_max: max,
            flags,
            alias: None,
        }
    }
    pub(super) fn new_string(name: &'static str, flags: &'static [&'static str]) -> Self {
        Self {
            name,
            type_: ConfigType::String,
            enum_values: &[],
            numeric_min: 0,
            numeric_max: 0,
            flags,
            alias: None,
        }
    }
    pub(super) fn new_enum(
        name: &'static str,
        values: &'static [&'static str],
        flags: &'static [&'static str],
    ) -> Self {
        Self {
            name,
            type_: ConfigType::Enum,
            enum_values: values,
            numeric_min: 0,
            numeric_max: 0,
            flags,
            alias: None,
        }
    }
    pub(super) fn new_special(name: &'static str, flags: &'static [&'static str]) -> Self {
        Self {
            name,
            type_: ConfigType::Special,
            enum_values: &[],
            numeric_min: 0,
            numeric_max: 0,
            flags,
            alias: None,
        }
    }
    pub(super) fn with_alias(mut self, alias: &'static str) -> Self {
        self.alias = Some(alias);
        self
    }
}

/// Metadata for legacy/special CONFIG surfaces that do not yet have a
/// ConfigSpec. Registry-backed entries are layered in by `all_config_meta`.
fn legacy_config_meta() -> Vec<ConfigMeta> {
    vec![
        ConfigMeta::new_string("requirepass", &["readonly", "sensitive"]),
        ConfigMeta::new_string("primaryauth", &["readonly", "sensitive"]).with_alias("masterauth"),
        ConfigMeta::new_string("primaryuser", &["readonly", "sensitive"]),
        ConfigMeta::new_string("dir", &["readonly", "replication", "site"]),
        ConfigMeta::new_string("dbfilename", &["readonly", "replication"]),
        ConfigMeta::new_bool("appendonly", &["reload_apply"]),
        ConfigMeta::new_string("appendfilename", &["readonly", "replication"]),
        ConfigMeta::new_string("appenddirname", &["readonly", "replication"]),
        ConfigMeta::new_enum(
            "appendfsync",
            &["always", "everysec", "no"],
            &["reload_apply"],
        ),
        ConfigMeta::new_bool("no-appendfsync-on-rewrite", &["reload_apply"]),
        ConfigMeta::new_bool("aof-use-rdb-preamble", &["reload_apply"]),
        ConfigMeta::new_bool("aof-timestamp-enabled", &["reload_apply"]),
        ConfigMeta::new_bool("aof-disable-auto-gc", &["reload_apply"]),
        ConfigMeta::new_bool("aof-load-truncated", &["reload_apply"]),
        ConfigMeta::new_string("notify-keyspace-events", &["reload_apply"]),
        ConfigMeta::new_bool("lazyfree-lazy-server-del", &[]),
        ConfigMeta::new_bool("lazyfree-lazy-expire", &[]),
        ConfigMeta::new_bool("lazyfree-lazy-eviction", &[]),
        ConfigMeta::new_numeric("active-expire-effort", 1, 10, &[]),
        ConfigMeta::new_numeric("zset-max-ziplist-entries", 1, i64::MAX, &["reload_apply"]),
        ConfigMeta::new_numeric("zset-max-ziplist-value", 1, i64::MAX, &["reload_apply"]),
        ConfigMeta::new_numeric("list-max-listpack-size", -5, i64::MAX, &["reload_apply"]),
        ConfigMeta::new_numeric("list-max-ziplist-size", -5, i64::MAX, &["reload_apply"]),
        ConfigMeta::new_numeric("hash-max-listpack-entries", 1, i64::MAX, &["reload_apply"]),
        ConfigMeta::new_numeric("hash-max-listpack-value", 1, i64::MAX, &["reload_apply"]),
        ConfigMeta::new_numeric("set-max-intset-entries", 1, i64::MAX, &["reload_apply"]),
        ConfigMeta::new_numeric("set-max-listpack-entries", 1, i64::MAX, &["reload_apply"]),
        ConfigMeta::new_numeric("set-max-listpack-value", 1, i64::MAX, &["reload_apply"]),
        ConfigMeta::new_numeric("stream-node-max-entries", 1, i64::MAX, &["reload_apply"]),
        ConfigMeta::new_numeric("slowlog-log-slower-than", -1, i64::MAX, &["reload_apply"]),
        ConfigMeta::new_numeric("slowlog-max-len", 0, i64::MAX, &["reload_apply"]),
        ConfigMeta::new_numeric(
            "commandlog-execution-slower-than",
            -1,
            i64::MAX,
            &["reload_apply"],
        ),
        ConfigMeta::new_numeric(
            "commandlog-request-larger-than",
            -1,
            i64::MAX,
            &["reload_apply"],
        ),
        ConfigMeta::new_numeric(
            "commandlog-reply-larger-than",
            -1,
            i64::MAX,
            &["reload_apply"],
        ),
        ConfigMeta::new_numeric(
            "commandlog-slow-execution-max-len",
            0,
            i64::MAX,
            &["reload_apply"],
        ),
        ConfigMeta::new_numeric(
            "commandlog-large-request-max-len",
            0,
            i64::MAX,
            &["reload_apply"],
        ),
        ConfigMeta::new_numeric(
            "commandlog-large-reply-max-len",
            0,
            i64::MAX,
            &["reload_apply"],
        ),
        ConfigMeta::new_numeric("latency-monitor-threshold", 0, i64::MAX, &["reload_apply"]),
        ConfigMeta::new_numeric(
            "proto-max-bulk-len",
            1024 * 1024,
            i64::MAX,
            &["reload_apply"],
        ),
        ConfigMeta::new_string("availability-zone", &["readonly"]),
        ConfigMeta::new_enum(
            "loglevel",
            &["debug", "verbose", "notice", "warning"],
            &["reload_apply"],
        ),
        ConfigMeta::new_special("save", &["reload_apply", "replication"]),
        ConfigMeta::new_bool("rdbcompression", &["reload_apply", "replication"]),
        ConfigMeta::new_bool("rdbchecksum", &["reload_apply", "replication"]),
        ConfigMeta::new_bool("stop-writes-on-bgsave-error", &["reload_apply", "replication"]),
        ConfigMeta::new_numeric("auto-aof-rewrite-percentage", 0, i64::MAX, &["reload_apply"]),
        ConfigMeta::new_numeric("auto-aof-rewrite-min-size", 0, i64::MAX, &["reload_apply"]),
        ConfigMeta::new_bool("aof-rewrite-incremental-fsync", &["reload_apply"]),
        ConfigMeta::new_numeric("hz", 1, 500, &["reload_apply"]),
        ConfigMeta::new_bool("import-mode", &["readonly"]),
        ConfigMeta::new_numeric("maxmemory", 0, i64::MAX, &["memory", "reload_apply"]),
        ConfigMeta::new_numeric("maxmemory-samples", 1, 64, &["memory", "reload_apply"]),
        ConfigMeta::new_numeric("lfu-log-factor", 0, i32::MAX as i64, &["memory", "reload_apply"]),
        ConfigMeta::new_numeric("lfu-decay-time", 0, i32::MAX as i64, &["memory", "reload_apply"]),
        ConfigMeta::new_enum(
            "maxmemory-policy",
            &[
                "volatile-lru",
                "allkeys-lru",
                "volatile-lfu",
                "allkeys-lfu",
                "volatile-random",
                "allkeys-random",
                "volatile-ttl",
                "noeviction",
            ],
            &["memory", "reload_apply"],
        ),
        ConfigMeta::new_numeric("lua-time-limit", 0, i64::MAX, &["reload_apply"]),
        ConfigMeta::new_numeric(
            "min-replicas-to-write",
            0,
            i64::MAX,
            &["reload_apply", "replication"],
        ),
        ConfigMeta::new_bool("replica-serve-stale-data", &["reload_apply", "replication"]),
        ConfigMeta::new_numeric("io-threads", 1, 256, &["reload_apply", "replication"]),
        ConfigMeta::new_bool("io-threads-always-active", &["hidden"]),
        ConfigMeta::new_special("client-output-buffer-limit", &["reload_apply"]),
        ConfigMeta::new_numeric(
            "min-string-size-avoid-copy-reply",
            0,
            i64::MAX,
            &["reload_apply"],
        ),
        ConfigMeta::new_numeric(
            "min-string-size-avoid-copy-reply-threaded",
            0,
            i64::MAX,
            &["reload_apply"],
        ),
        ConfigMeta::new_numeric("min-io-threads-avoid-copy-reply", 0, i64::MAX, &["reload_apply"]),
        ConfigMeta::new_numeric(
            "repl-ping-replica-period",
            1,
            i64::MAX,
            &["reload_apply", "replication"],
        ),
        ConfigMeta::new_numeric("repl-timeout", 1, i64::MAX, &["reload_apply", "replication"]),
        ConfigMeta::new_bool("repl-diskless-sync", &["reload_apply", "replication"]),
        ConfigMeta::new_enum(
            "repl-diskless-load",
            &["disabled", "on-empty-db", "swapdb"],
            &["reload_apply", "replication"],
        ),
        ConfigMeta::new_numeric(
            "repl-diskless-sync-delay",
            0,
            i64::MAX,
            &["reload_apply", "replication"],
        ),
        ConfigMeta::new_numeric(
            "repl-diskless-sync-max-replicas",
            0,
            i64::MAX,
            &["reload_apply", "replication"],
        ),
        ConfigMeta::new_bool(
            "dual-channel-replication-enabled",
            &["reload_apply", "replication"],
        ),
        ConfigMeta::new_numeric("tcp-keepalive", 0, i64::MAX, &["reload_apply", "replication"]),
        ConfigMeta::new_numeric("hll-sparse-max-bytes", 0, i64::MAX, &["reload_apply"]),
        ConfigMeta::new_numeric("maxclients", 1, 1000000, &["readonly", "replication"]),
        ConfigMeta::new_enum("oom-score-adj", &["no", "yes", "absolute"], &["readonly"]),
        ConfigMeta::new_string("oom-score-adj-values", &["readonly"]),
        ConfigMeta::new_bool("mptcp", &["readonly", "replication"]),
        ConfigMeta::new_bool("latency-tracking", &["reload_apply"]),
        ConfigMeta::new_string("latency-tracking-info-percentiles", &["reload_apply"]),
        ConfigMeta::new_enum("acl-pubsub-default", &["allchannels", "resetchannels"], &[]),
        ConfigMeta::new_string("aclfile", &[]),
        ConfigMeta::new_numeric("acllog-max-len", 0, i64::MAX, &[]),
        ConfigMeta::new_bool("activerehashing", &["reload_apply"]),
        ConfigMeta::new_numeric("databases", 1, 1000000, &["readonly", "immutable"]),
        ConfigMeta::new_string("replicaof", &["reload_apply", "replication"]).with_alias("slaveof"),
    ]
}

/// CONFIG INFO uses ConfigRegistry metadata for every registry-backed key.
/// The small legacy table exists only for surfaces still implemented by a
/// specialised command owner, and is intentionally overridden by no registry
/// entry so that adding a ConfigSpec automatically makes INFO discover it.
pub(super) fn all_config_meta() -> Vec<ConfigMeta> {
    let mut metas: std::collections::BTreeMap<&'static str, ConfigMeta> =
        legacy_config_meta()
            .into_iter()
            .map(|meta| (meta.name, meta))
            .collect();
    for spec in config_registry().all_specs() {
        metas
            .entry(spec.name)
            .or_insert_with(|| ConfigMeta::from_registry_spec(spec));
    }
    metas.into_values().collect()
}

pub(super) fn config_info(args: &CommandArgs) -> Value {
    if args.args.len() < 2 {
        return CommandError::wrong_arity("config");
    }

    let patterns: Vec<String> = args.args[1..]
        .iter()
        .map(|p| String::from_utf8_lossy(p).to_ascii_lowercase())
        .collect();

    let metas = all_config_meta();
    let mut matched_keys: Vec<String> = Vec::new();
    let mut results: Vec<Value> = Vec::new();

    let pattern_matches = |name: &str| -> bool {
        for pat in &patterns {
            if pat == "*" {
                return true;
            }
            if pat == name {
                return true;
            }
            if let Some(prefix) = pat.strip_suffix('*') {
                if name.starts_with(prefix) {
                    return true;
                }
            }
        }
        false
    };

    let add_entry = |name: &str, meta: &ConfigMeta, is_alias: bool, alias_name: &str| {
        let mut entry = vec![Value::bulk(Bytes::from_static(b"name"))];
        entry.push(Value::bulk(Bytes::from(name.as_bytes().to_vec())));

        entry.push(Value::bulk(Bytes::from_static(b"type")));
        entry.push(Value::bulk(Bytes::from(
            meta.type_.as_str().as_bytes().to_vec(),
        )));

        if meta.type_ == ConfigType::Enum {
            entry.push(Value::bulk(Bytes::from_static(b"values")));
            let values: Vec<Value> = meta
                .enum_values
                .iter()
                .map(|v| Value::bulk(Bytes::from(v.as_bytes().to_vec())))
                .collect();
            entry.push(Value::Array(values));
        }

        if meta.type_ == ConfigType::Numeric {
            entry.push(Value::bulk(Bytes::from_static(b"range")));
            let mut range = vec![Value::integer(meta.numeric_min)];
            range.push(Value::integer(meta.numeric_max));
            entry.push(Value::Array(range));
        }

        entry.push(Value::bulk(Bytes::from_static(b"flags")));
        let mut flags: Vec<Value> = meta
            .flags
            .iter()
            .map(|f| Value::bulk(Bytes::from(f.as_bytes().to_vec())))
            .collect();
        if is_alias {
            flags.push(Value::bulk(Bytes::from_static(b"alias")));
        }
        entry.push(Value::Array(flags));

        if let Some(alias) = meta.alias {
            if !is_alias {
                entry.push(Value::bulk(Bytes::from_static(b"alias")));
                entry.push(Value::bulk(Bytes::from(alias.as_bytes().to_vec())));
            }
        }
        if is_alias {
            entry.push(Value::bulk(Bytes::from_static(b"alias")));
            entry.push(Value::bulk(Bytes::from(alias_name.as_bytes().to_vec())));
        }

        Value::Array(entry)
    };

    for meta in &metas {
        if pattern_matches(meta.name) {
            let key = meta.name.to_string();
            if !matched_keys.iter().any(|k| k == &key) {
                matched_keys.push(key);
                results.push(add_entry(meta.name, meta, false, ""));
            }
        }
        if let Some(alias) = meta.alias {
            if pattern_matches(alias) {
                let key = alias.to_string();
                if !matched_keys.iter().any(|k| k == &key) {
                    matched_keys.push(key);
                    results.push(add_entry(alias, meta, true, meta.name));
                }
            }
        }
    }

    let mut entries: Vec<(String, Value)> = results
        .into_iter()
        .filter_map(|v| {
            if let Value::Array(ref items) = v {
                if let Some(Value::Bulk(name_bytes)) = items.get(1) {
                    let name = String::from_utf8_lossy(name_bytes).to_string();
                    return Some((name, v));
                }
            }
            None
        })
        .collect();
    entries.sort_by(|a, b| a.0.cmp(&b.0));
    Value::Array(entries.into_iter().map(|(_, v)| v).collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::framework::CommandArgs;
    use std::collections::BTreeSet;

    use super::super::config_registry;

    #[test]
    fn aof_disable_auto_gc_has_config_info_metadata() {
        let meta = all_config_meta()
            .into_iter()
            .find(|meta| meta.name == "aof-disable-auto-gc")
            .expect("aof-disable-auto-gc metadata should exist");

        assert!(meta.type_ == ConfigType::Bool);
        assert_eq!(meta.flags, &["reload_apply"]);
        assert_eq!(meta.alias, None);
    }

    #[test]
    fn config_info_covers_every_visible_registry_spec() {
        let args = CommandArgs::from_parts(vec![
            Bytes::from_static(b"CONFIG"),
            Bytes::from_static(b"INFO"),
            Bytes::from_static(b"*"),
        ]);
        let Value::Array(entries) = config_info(&args) else {
            panic!("CONFIG INFO must return an array");
        };
        let info_names: BTreeSet<String> = entries
            .iter()
            .map(|entry| match entry {
                Value::Array(fields) => match fields.get(1) {
                    Some(Value::Bulk(name)) => String::from_utf8_lossy(name).to_string(),
                    _ => panic!("CONFIG INFO entry must contain its name"),
                },
                _ => panic!("CONFIG INFO entry must be an array"),
            })
            .collect();

        let missing: Vec<&str> = config_registry()
            .all_specs()
            .into_iter()
            .filter(|spec| !spec.is_hidden() && !info_names.contains(spec.name))
            .map(|spec| spec.name)
            .collect();
        assert!(missing.is_empty(), "CONFIG INFO is missing: {missing:?}");
    }
}
