use std::io::Write;
use std::sync::OnceLock;

use crate::engine::extension::config::server::{
    CONFIG_OOM_BGCHILD, CONFIG_OOM_COUNT, CONFIG_OOM_PRIMARY, CONFIG_OOM_REPLICA,
    OOM_SCORE_ADJ_NO, OOM_SCORE_RELATIVE,
};
use crate::engine::extension::config::spec_registry::ConfigRegistry;

pub fn config_registry() -> &'static ConfigRegistry {
    static REGISTRY: OnceLock<ConfigRegistry> = OnceLock::new();
    REGISTRY.get_or_init(ConfigRegistry::new)
}

pub(super) fn read_oom_score_adj() -> Option<i32> {
    let content = std::fs::read_to_string("/proc/self/oom_score_adj").ok()?;
    content.trim().parse().ok()
}

pub(crate) fn write_oom_score_adj(val: i32) -> std::io::Result<()> {
    let clamped = val.clamp(-1000, 1000);
    let path = "/proc/self/oom_score_adj";
    let mut f = std::fs::OpenOptions::new().write(true).open(path)?;
    let s = format!("{}\n", clamped);
    f.write_all(s.as_bytes())
}

pub fn set_oom_score_adj(
    rcfg: &mut crate::framework::context::RuntimeConfig,
    process_class: isize,
) -> Result<(), &'static str> {
    let class = if process_class == -1 {
        if rcfg.replication.master_host.is_some() {
            CONFIG_OOM_REPLICA
        } else {
            CONFIG_OOM_PRIMARY
        }
    } else {
        process_class as usize
    };
    if class >= CONFIG_OOM_COUNT {
        return Err("Invalid oom class");
    }

    if rcfg.server.oom_score_adj != OOM_SCORE_ADJ_NO {
        if !rcfg.server.oom_score_adj_adjusted {
            let base = read_oom_score_adj()
                .ok_or("Failed to set current oom_score_adj. Check server logs.")?;
            rcfg.server.oom_score_adj_adjusted = true;
            rcfg.server.oom_score_adj_base = base;
        }
        let mut val = rcfg.server.oom_score_adj_values[class];
        if rcfg.server.oom_score_adj == OOM_SCORE_RELATIVE {
            val += rcfg.server.oom_score_adj_base;
        }
        if let Err(e) = write_oom_score_adj(val) {
            crate::server_log!(
                warning,
                "Failed to set current oom_score_adj value {} for pid {}: {}",
                val,
                std::process::id(),
                e
            );
            return Err("Failed to set current oom_score_adj. Check server logs.");
        }
    } else if rcfg.server.oom_score_adj_adjusted {
        rcfg.server.oom_score_adj_adjusted = false;
        if let Err(e) = write_oom_score_adj(rcfg.server.oom_score_adj_base) {
            crate::server_log!(
                warning,
                "Failed to restore oom_score_adj value {} for pid {}: {}",
                rcfg.server.oom_score_adj_base,
                std::process::id(),
                e
            );
            return Err("Failed to set current oom_score_adj. Check server logs.");
        }
    }
    Ok(())
}

pub fn set_oom_score_adj_child(
    rcfg: &crate::framework::context::RuntimeConfig,
) -> Result<i32, String> {
    if rcfg.server.oom_score_adj == OOM_SCORE_ADJ_NO {
        return Ok(rcfg.server.oom_score_adj_base);
    }
    let mut val = rcfg.server.oom_score_adj_values[CONFIG_OOM_BGCHILD];
    if rcfg.server.oom_score_adj == OOM_SCORE_RELATIVE {
        val += rcfg.server.oom_score_adj_base;
    }
    match write_oom_score_adj(val) {
        Ok(()) => Ok(val),
        Err(e) => Err(format!(
            "Failed to set oom_score_adj value {} for BGSAVE child: {}",
            val, e
        )),
    }
}

/// Unsigned memory-value parser for C `unsigned long long` configs like
/// `maxmemory` (memtoull in util.c). Accepts an optional size unit suffix and
/// permits values up to `u64::MAX`, so e.g. `9223372036854775808` (2^63) is
/// valid where the signed `parse_mem_unit` would overflow. Negative values are
/// rejected (no leading `-`).
pub(in crate::framework::commands::server::config) fn parse_mem_unit_unsigned(
    s: &str,
) -> Option<u64> {
    let s = s.trim().to_ascii_lowercase();
    if s.is_empty() {
        return None;
    }
    let mut num_str = String::new();
    let mut unit = String::new();
    for ch in s.chars() {
        if ch.is_ascii_digit() {
            if unit.is_empty() {
                num_str.push(ch);
            } else {
                return None;
            }
        } else {
            unit.push(ch);
        }
    }
    let n: u64 = num_str.parse().ok()?;
    let mult: u64 = match unit.as_str() {
        "" | "b" => 1,
        "k" | "kb" => 1024,
        "m" | "mb" => 1024 * 1024,
        "g" | "gb" => 1024 * 1024 * 1024,
        _ => return None,
    };
    n.checked_mul(mult)
}

pub(in crate::framework::commands::server::config) fn parse_bool_strict(
    v: &str,
) -> Result<bool, String> {
    let lower = v.to_ascii_lowercase();
    match lower.as_str() {
        "1" | "yes" | "true" => Ok(true),
        "0" | "no" | "false" => Ok(false),
        _ => Err("ERR argument must be 'yes' or 'no'".to_string()),
    }
}

pub(in crate::framework::commands::server::config) fn bool_to_yesno(b: bool) -> &'static str {
    if b { "yes" } else { "no" }
}
