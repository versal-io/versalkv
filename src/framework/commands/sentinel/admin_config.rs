use std::collections::HashSet;

use crate::engine::command::context::CommandConn;
use crate::engine::distributed::sentinel::SentinelState;
use crate::foundation::io::protocol::Value;
use crate::framework::context::ServerContext;
use crate::framework::{CommandArgs, CommandError};

pub(super) fn config(
    _conn: &mut dyn CommandConn,
    args: &CommandArgs,
    ctx: &mut ServerContext,
) -> Value {
    if args.args.len() < 2 {
        return CommandError::wrong_arity("sentinel|config");
    }
    let subcommand = String::from_utf8_lossy(&args.args[1]).to_lowercase();
    match subcommand.as_str() {
        "get" => config_get(ctx, args),
        "set" => config_set(ctx, args),
        _ => Value::error(format!("ERR Unknown subcommand '{}'", subcommand)),
    }
}

fn config_get(ctx: &mut ServerContext, args: &CommandArgs) -> Value {
    if args.args.len() < 3 {
        return CommandError::wrong_arity("sentinel|config|get");
    }
    let mut result = Vec::new();
    let mut seen = HashSet::new();
    let patterns: Vec<String> = args.args[2..]
        .iter()
        .map(|value| String::from_utf8_lossy(value).to_string())
        .collect();
    let config_items: &[(&str, fn(&SentinelState) -> ConfigValue)] = &[
        ("resolve-hostnames", |state| {
            ConfigValue::string(if state.resolve_hostnames { "yes" } else { "no" })
        }),
        ("announce-hostnames", |state| {
            ConfigValue::string(if state.announce_hostnames {
                "yes"
            } else {
                "no"
            })
        }),
        ("announce-ip", |state| {
            ConfigValue::string(&state.announce_ip)
        }),
        ("announce-port", |state| {
            ConfigValue::integer(state.announce_port as i64)
        }),
        ("sentinel-user", |state| {
            ConfigValue::string(&state.sentinel_auth_user)
        }),
        ("sentinel-pass", |state| {
            ConfigValue::string(&state.sentinel_auth_pass)
        }),
        ("deny-scripts-reconfig", |state| {
            ConfigValue::string(if state.deny_scripts_reconfig {
                "yes"
            } else {
                "no"
            })
        }),
    ];
    for pattern in patterns {
        for (name, value_fn) in config_items {
            if stringmatch(&pattern, name) && seen.insert((*name).to_string()) {
                result.push(Value::bulk(name.as_bytes().to_vec()));
                match value_fn(ctx.sentinel_state()) {
                    ConfigValue::String(value) => result.push(Value::bulk(value.into_bytes())),
                    ConfigValue::Integer(value) => result.push(Value::Integer(value)),
                }
            }
        }
        if stringmatch(&pattern, "loglevel") && seen.insert("loglevel".to_string()) {
            result.push(Value::bulk(b"loglevel".to_vec()));
            result.push(Value::bulk(
                ctx.config().server.loglevel.as_bytes().to_vec(),
            ));
        }
    }
    Value::Array(result)
}

enum ConfigValue {
    String(String),
    Integer(i64),
}

impl ConfigValue {
    fn string(value: &str) -> Self {
        Self::String(value.to_string())
    }

    fn integer(value: i64) -> Self {
        Self::Integer(value)
    }
}

fn config_set(ctx: &mut ServerContext, args: &CommandArgs) -> Value {
    if args.args.len() < 4 {
        return CommandError::wrong_arity("sentinel|config|set");
    }
    const VALID_OPTIONS: &[&str] = &[
        "announce-ip",
        "sentinel-user",
        "sentinel-pass",
        "deny-scripts-reconfig",
        "resolve-hostnames",
        "announce-port",
        "announce-hostnames",
        "loglevel",
    ];
    let mut option_values = Vec::new();
    let mut seen = HashSet::new();
    let mut index = 2;
    while index < args.args.len() {
        let option = String::from_utf8_lossy(&args.args[index]).to_lowercase();
        if !VALID_OPTIONS.contains(&option.as_str()) {
            return Value::error(format!(
                "Invalid argument '{}' to SENTINEL CONFIG SET",
                option
            ));
        }
        if !seen.insert(option.clone()) {
            return Value::error(format!(
                "Duplicate argument '{}' to SENTINEL CONFIG SET",
                option
            ));
        }
        if index + 1 >= args.args.len() {
            return Value::error(format!("Missing argument '{}' value", option));
        }
        index += 1;
        let value = String::from_utf8_lossy(&args.args[index]).to_string();
        if let Err(error) = validate_config_value(&option, &value) {
            return error;
        }
        option_values.push((option, value));
        index += 1;
    }
    for (option, value) in option_values {
        apply_config_value(ctx, &option, &value);
    }
    Value::ok()
}

fn validate_config_value(option: &str, value: &str) -> Result<(), Value> {
    if value.bytes().any(|byte| byte < b' ' || byte == 0x7f) {
        return Err(Value::error(
            "ERR Invalid argument: control characters not allowed",
        ));
    }
    match option {
        "resolve-hostnames" | "announce-hostnames" | "deny-scripts-reconfig" => {
            let normalized = value.to_lowercase();
            if normalized != "yes" && normalized != "no" {
                return Err(invalid_config_value(option, value));
            }
        }
        "announce-port" if value.parse::<u16>().is_err() => {
            return Err(invalid_config_value(option, value));
        }
        "loglevel"
            if !["debug", "verbose", "notice", "warning", "nothing"]
                .contains(&value.to_lowercase().as_str()) =>
        {
            return Err(invalid_config_value(option, value));
        }
        _ => {}
    }
    Ok(())
}

fn invalid_config_value(option: &str, value: &str) -> Value {
    Value::error(format!(
        "Invalid value '{}' to SENTINEL CONFIG SET '{}'",
        value, option
    ))
}

fn apply_config_value(ctx: &mut ServerContext, option: &str, value: &str) {
    if option == "loglevel" {
        ctx.config_mut().server.loglevel = value.to_lowercase();
        let level = match value.to_lowercase().as_str() {
            "debug" => log::LevelFilter::Debug,
            "verbose" | "notice" => log::LevelFilter::Info,
            "warning" => log::LevelFilter::Warn,
            "nothing" => log::LevelFilter::Off,
            _ => return,
        };
        log::set_max_level(level);
        return;
    }
    let state = ctx.sentinel_state_mut();
    let mut rebuild_needed = false;
    match option {
        "resolve-hostnames" => state.resolve_hostnames = value.eq_ignore_ascii_case("yes"),
        "announce-hostnames" => {
            state.announce_hostnames = value.eq_ignore_ascii_case("yes");
            rebuild_needed = true;
        }
        "announce-ip" => {
            state.announce_ip = value.to_string();
            rebuild_needed = true;
        }
        "announce-port" => {
            if let Ok(port) = value.parse::<u16>() {
                state.announce_port = port.into();
                rebuild_needed = true;
            }
        }
        "sentinel-user" => {
            state.sentinel_auth_user = value.to_string();
            rebuild_needed = true;
        }
        "sentinel-pass" => {
            state.sentinel_auth_pass = value.to_string();
            rebuild_needed = true;
        }
        "deny-scripts-reconfig" => {
            state.deny_scripts_reconfig = value.eq_ignore_ascii_case("yes");
        }
        _ => {}
    }
    if rebuild_needed {
        state.link_rebuild_needed = true;
    }
}

fn stringmatch(pattern: &str, value: &str) -> bool {
    stringmatchlen(pattern.as_bytes(), value.as_bytes())
}

fn stringmatchlen(pattern: &[u8], value: &[u8]) -> bool {
    let mut pattern_index = 0;
    let mut value_index = 0;
    let mut star_pattern = None;
    let mut star_value = None;
    while value_index < value.len() {
        if pattern_index < pattern.len()
            && (pattern[pattern_index] == b'?' || pattern[pattern_index] == value[value_index])
        {
            pattern_index += 1;
            value_index += 1;
        } else if pattern_index < pattern.len() && pattern[pattern_index] == b'*' {
            star_pattern = Some(pattern_index);
            star_value = Some(value_index + 1);
            pattern_index += 1;
        } else if let (Some(pattern), Some(value)) = (star_pattern, star_value) {
            pattern_index = pattern + 1;
            value_index = value;
            star_value = Some(value_index + 1);
        } else {
            return false;
        }
    }
    while pattern_index < pattern.len() && pattern[pattern_index] == b'*' {
        pattern_index += 1;
    }
    pattern_index == pattern.len()
}

#[cfg(test)]
mod tests {
    use super::{apply_config_value, validate_config_value};
    use crate::foundation::io::protocol::Value;
    use crate::framework::context::ServerContext;

    #[test]
    fn config_values_reject_protocol_control_characters() {
        for (option, value) in [
            ("announce-ip", "1.2.3.4\r\ninjected"),
            ("sentinel-user", "user\ninjected"),
            ("sentinel-pass", "pass\0word"),
            ("announce-ip", "1.2\t.3.4"),
            ("sentinel-pass", "pass\u{7f}"),
        ] {
            assert_eq!(
                validate_config_value(option, value),
                Err(Value::error(
                    "ERR Invalid argument: control characters not allowed"
                ))
            );
        }
    }

    #[test]
    fn config_password_allows_printable_special_characters() {
        assert_eq!(
            validate_config_value("sentinel-pass", "my\"pass\\word with spaces"),
            Ok(())
        );
    }

    fn make_ctx() -> ServerContext {
        ServerContext::new(1)
    }

    #[test]
    fn config_set_updates_state_and_flags_link_rebuild_for_connection_affecting_keys() {
        for (opt, val) in [
            ("announce-ip", "10.0.0.1"),
            ("announce-port", "26380"),
            ("announce-hostnames", "yes"),
            ("sentinel-user", "srv"),
            ("sentinel-pass", "secret"),
        ] {
            let mut ctx = make_ctx();
            apply_config_value(&mut ctx, opt, val);
            assert!(
                ctx.sentinel_state().link_rebuild_needed,
                "{opt} should mark link_rebuild_needed"
            );
        }
        // resolve-hostnames and deny-scripts-reconfig do not require link rebuild.
        let mut ctx = make_ctx();
        apply_config_value(&mut ctx, "resolve-hostnames", "yes");
        assert!(!ctx.sentinel_state().link_rebuild_needed);
        apply_config_value(&mut ctx, "deny-scripts-reconfig", "yes");
        assert!(!ctx.sentinel_state().link_rebuild_needed);
    }

    #[test]
    fn config_set_boolean_and_numeric_values_are_applied() {
        let mut ctx = make_ctx();
        apply_config_value(&mut ctx, "resolve-hostnames", "yes");
        assert!(ctx.sentinel_state().resolve_hostnames);
        apply_config_value(&mut ctx, "resolve-hostnames", "no");
        assert!(!ctx.sentinel_state().resolve_hostnames);
        apply_config_value(&mut ctx, "announce-port", "26381");
        assert_eq!(ctx.sentinel_state().announce_port, 26381);
        apply_config_value(&mut ctx, "deny-scripts-reconfig", "yes");
        assert!(ctx.sentinel_state().deny_scripts_reconfig);
    }
}
