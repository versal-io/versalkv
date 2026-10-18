use crate::framework::{CommandError, CommandRegistry};
use crate::framework::context::ServerContext;
use crate::framework::generated::commands::SUBCOMMAND_NAMES;
use crate::foundation::io::protocol::Value;
use bytes::Bytes;

fn has_keys_via_subcommand(spec: &crate::framework::CommandSpec) -> bool {
    if spec.get_keys.is_some() {
        return true;
    }
    let cmd_name = spec.name.to_ascii_lowercase();
    SUBCOMMAND_NAMES
        .iter()
        .any(|n| n.starts_with(&format!("{}|", cmd_name)))
}

fn has_module_keys(spec: &crate::framework::CommandSpec, ctx: &ServerContext) -> bool {
    ctx.module_registry()
        .lookup_cmd_func(spec.name)
        .and_then(|entry| entry.info.as_ref())
        .is_some_and(|info| {
            !info.key_specs.is_empty() || info.legacy_keys.is_some_and(|(first, _, _)| first > 0)
        })
}

fn registry(ctx: &ServerContext) -> &CommandRegistry {
    ctx.command_registry()
}

pub(crate) fn command_getkeys(
    reg: &CommandRegistry,
    ctx: &ServerContext,
    args: &[Bytes],
) -> Value {
    if args.is_empty() {
        return CommandError::wrong_arity("command");
    }
    let cmd_name = match std::str::from_utf8(&args[0]) {
        Ok(s) => s.to_ascii_lowercase(),
        Err(_) => return CommandError::unknown(&args[0]),
    };
    let key_args = &args[1..];
    let spec = match reg.lookup(cmd_name.as_bytes()) {
        Some(s) => s,
        None => return Value::error("ERR The command has no key arguments"),
    };
    if spec.first_key == 0 && !has_keys_via_subcommand(spec) && !has_module_keys(spec, ctx) {
        return Value::error("ERR The command has no key arguments");
    }
    let keys = extract_command_keys(&cmd_name, key_args, ctx);
    let result: Vec<Value> = keys
        .iter()
        .map(|(k, _)| Value::bulk(Bytes::copy_from_slice(k)))
        .collect();
    Value::Array(result)
}

pub(crate) fn command_getkeysandflags(
    reg: &CommandRegistry,
    ctx: &ServerContext,
    args: &[Bytes],
) -> Value {
    if args.is_empty() {
        return CommandError::wrong_arity("command");
    }
    let cmd_name = match std::str::from_utf8(&args[0]) {
        Ok(s) => s.to_ascii_lowercase(),
        Err(_) => return CommandError::unknown(&args[0]),
    };
    let key_args = &args[1..];
    let spec = match reg.lookup(cmd_name.as_bytes()) {
        Some(s) => s,
        None => return Value::error("ERR The command has no key arguments"),
    };
    if spec.first_key == 0 && !has_keys_via_subcommand(spec) && !has_module_keys(spec, ctx) {
        return Value::error("ERR The command has no key arguments");
    }
    let keys = extract_command_keys(&cmd_name, key_args, ctx);
    let result: Vec<Value> = keys
        .iter()
        .map(|(k, flags)| {
            let flag_vals: Vec<Value> = flags
                .iter()
                .map(|f| Value::bulk(Bytes::from_static(f.as_bytes())))
                .collect();
            Value::Array(vec![
                Value::bulk(Bytes::copy_from_slice(k)),
                Value::Array(flag_vals),
            ])
        })
        .collect();
    Value::Array(result)
}

pub(crate) fn extract_command_keys<'a>(
    cmd_name: &str,
    key_args: &'a [Bytes],
    ctx: &ServerContext,
) -> Vec<(&'a [u8], Vec<&'static str>)> {
    let reg = registry(ctx);
    let spec = match reg.lookup(cmd_name.as_bytes()) {
        Some(s) => s,
        None => return Vec::new(),
    };
    if let Some(info) = ctx
        .module_registry()
        .lookup_cmd_func(spec.name)
        .and_then(|entry| entry.info.as_ref())
    {
        return extract_module_keys(info, key_args);
    }

    if let Some(get_keys) = spec.get_keys {
        return get_keys(key_args);
    }

    let first = spec.first_key as usize;
    let last = spec.last_key;
    let step = spec.key_step as usize;
    let default_flags: Vec<&'static str> = spec.key_flags.to_vec();

    if first == 0 || first > key_args.len() {
        return Vec::new();
    }

    let mut result = Vec::new();
    let end = if last == -1 {
        key_args.len()
    } else if last <= 0 {
        first
    } else {
        let abs_last = last as usize;
        if abs_last > key_args.len() {
            key_args.len()
        } else {
            abs_last
        }
    };

    let mut i = first - 1;
    while i < end && i < key_args.len() {
        result.push((key_args[i].as_ref(), default_flags.clone()));
        if step == 0 {
            break;
        }
        i += step;
    }
    result
}

fn extract_module_keys<'a>(
    info: &crate::engine::extension::module::ModuleCommandInfo,
    args: &'a [Bytes],
) -> Vec<(&'a [u8], Vec<&'static str>)> {
    let specs = if info.key_specs.is_empty() {
        let Some((first, last, step)) = info.legacy_keys else {
            return Vec::new();
        };
        if first <= 0 {
            return Vec::new();
        }
        vec![crate::engine::extension::module::ModuleKeySpec {
            flags: if info.getkeys_api {
                vec!["RO", "access"]
            } else {
                vec!["RW", "access", "update"]
            },
            begin_search: crate::engine::extension::module::ModuleKeyBeginSearch::Index {
                pos: first,
            },
            find_keys: crate::engine::extension::module::ModuleKeyFindKeys::Range {
                last_key: last,
                key_step: step,
                limit: 0,
            },
        }]
    } else {
        info.key_specs.clone()
    };
    let mut keys = Vec::new();
    for spec in &specs {
        let begin = match &spec.begin_search {
            crate::engine::extension::module::ModuleKeyBeginSearch::Index { pos } => {
                pos.saturating_sub(1) as usize
            }
            crate::engine::extension::module::ModuleKeyBeginSearch::Keyword {
                keyword,
                start_from,
            } => {
                let start = if *start_from >= 0 {
                    start_from.saturating_sub(1) as usize
                } else {
                    args.len().saturating_sub(start_from.unsigned_abs() as usize)
                };
                match (start..args.len())
                    .find(|index| args[*index].eq_ignore_ascii_case(keyword.as_bytes()))
                {
                    Some(index) => index + 1,
                    None => continue,
                }
            }
            crate::engine::extension::module::ModuleKeyBeginSearch::Unknown => continue,
        };
        let flags = spec.flags.clone();
        match &spec.find_keys {
            crate::engine::extension::module::ModuleKeyFindKeys::Range {
                last_key,
                key_step,
                ..
            } => {
                if *key_step == 0 {
                    continue;
                }
                let end = if *last_key < 0 {
                    (args.len() as i32 + *last_key + 1).max(begin as i32) as usize
                } else {
                    (begin as i32 + *last_key + 1).max(begin as i32) as usize
                };
                let mut index = begin;
                while index < args.len() && index < end {
                    keys.push((args[index].as_ref(), flags.clone()));
                    index = index.saturating_add(key_step.unsigned_abs() as usize);
                }
            }
            crate::engine::extension::module::ModuleKeyFindKeys::KeyNum {
                key_num_idx,
                first_key,
                key_step,
            } => {
                let Some(count) = args
                    .get((begin as i32 + *key_num_idx) as usize)
                    .and_then(|value| std::str::from_utf8(value).ok()?.parse::<usize>().ok())
                else {
                    continue;
                };
                let mut index = (begin as i32 + *first_key).max(0) as usize;
                for _ in 0..count {
                    if let Some(key) = args.get(index) {
                        keys.push((key.as_ref(), flags.clone()));
                    }
                    index = index.saturating_add(key_step.unsigned_abs() as usize);
                }
            }
            crate::engine::extension::module::ModuleKeyFindKeys::Unknown => {}
        }
    }
    keys
}
