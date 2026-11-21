use crate::foundation::io::protocol::Value;
use crate::framework::context::ServerContext;
use crate::framework::generated::commands::SUBCOMMAND_NAMES;
use crate::framework::{CommandError, CommandRegistry};
use bytes::Bytes;

type CommandKeys<'a> = Vec<(&'a [u8], Vec<&'static str>)>;

fn has_keys_via_subcommand(spec: &crate::framework::CommandSpec) -> bool {
    if spec.get_keys.is_some() {
        return true;
    }
    let cmd_name = spec.name.as_str().to_ascii_lowercase();
    SUBCOMMAND_NAMES
        .iter()
        .any(|n| n.starts_with(&format!("{}|", cmd_name)))
}

fn has_module_keys(spec: &crate::framework::CommandSpec, ctx: &ServerContext) -> bool {
    ctx.module_registry()
        .lookup_cmd_func(spec.name.as_str())
        .and_then(|entry| entry.info.as_ref())
        .is_some_and(|info| {
            info.getkeys_api
                || info
                    .key_specs
                    .iter()
                    .any(|spec| !spec.flags.contains(&"not_key"))
                || info.legacy_keys.is_some_and(|(first, _, _)| first > 0)
        })
}

fn registry(ctx: &ServerContext) -> &CommandRegistry {
    ctx.command_registry()
}

pub(crate) fn command_getkeys(reg: &CommandRegistry, ctx: &ServerContext, args: &[Bytes]) -> Value {
    if args.is_empty() {
        return CommandError::wrong_arity("command");
    }
    let keys = match command_key_result(reg, ctx, args) {
        Ok(keys) => keys,
        Err(error) => return error,
    };
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
    let keys = match command_key_result(reg, ctx, args) {
        Ok(keys) => keys,
        Err(error) => return error,
    };
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

fn command_key_result<'a>(
    reg: &CommandRegistry,
    ctx: &ServerContext,
    args: &'a [Bytes],
) -> Result<CommandKeys<'a>, Value> {
    let cmd_name = std::str::from_utf8(&args[0]).map_err(|_| CommandError::unknown(&args[0]))?;
    let spec = reg
        .lookup(cmd_name.as_bytes())
        .ok_or_else(|| Value::error("ERR The command has no key arguments"))?;
    if spec.first_key == 0 && !has_keys_via_subcommand(spec) && !has_module_keys(spec, ctx) {
        return Err(Value::error("ERR The command has no key arguments"));
    }
    let argc = i64::try_from(args.len())
        .map_err(|_| Value::error("ERR Invalid number of arguments specified for command"))?;
    let arity = i64::from(spec.arity);
    if (arity > 0 && argc != arity) || (arity < 0 && argc < -arity) {
        return Err(Value::error(
            "ERR Invalid number of arguments specified for command",
        ));
    }
    let keys = extract_command_keys_result(cmd_name, &args[1..], ctx).unwrap_or_default();
    if keys.is_empty() && spec.flags & crate::engine::command::CMD_NO_MANDATORY_KEYS == 0 {
        return Err(Value::error("ERR Invalid arguments specified for command"));
    }
    Ok(keys)
}

pub(crate) fn extract_command_keys<'a>(
    cmd_name: &str,
    key_args: &'a [Bytes],
    ctx: &ServerContext,
) -> CommandKeys<'a> {
    extract_command_keys_result(cmd_name, key_args, ctx).unwrap_or_default()
}

fn extract_command_keys_result<'a>(
    cmd_name: &str,
    key_args: &'a [Bytes],
    ctx: &ServerContext,
) -> Result<CommandKeys<'a>, ()> {
    let reg = registry(ctx);
    let spec = match reg.lookup(cmd_name.as_bytes()) {
        Some(s) => s,
        None => return Err(()),
    };
    if let Some(entry) = ctx.module_registry().lookup_cmd_func(spec.name.as_str()) {
        let Some(info) = entry.info.as_ref() else {
            return Ok(Vec::new());
        };
        let has_static_keys = info
            .key_specs
            .iter()
            .any(|spec| !spec.flags.contains(&"not_key"));
        let has_variable_flags = info
            .key_specs
            .iter()
            .any(|spec| spec.flags.contains(&"variable_flags"));
        if !has_variable_flags && (has_static_keys || !info.getkeys_api) {
            if let Ok(keys) = extract_module_keys(info, key_args) {
                return Ok(keys);
            }
        }
        if !info.getkeys_api {
            return Err(());
        }
        let positions = crate::framework::capabilities::module::query_module_key_positions(
            entry,
            ctx.module_registry()
                .lookup(&entry.module_name)
                .ok_or(())?
                .handle,
            cmd_name.as_bytes(),
            key_args,
        )
        .ok_or(())?;
        return positions
            .into_iter()
            .map(|(pos, flags)| {
                let index = pos.checked_sub(1).ok_or(())?;
                Ok((key_args.get(index).ok_or(())?.as_ref(), flags))
            })
            .collect();
    }

    if let Some(get_keys) = spec.get_keys {
        return Ok(get_keys(key_args));
    }

    let first = spec.first_key as usize;
    let last = spec.last_key;
    let step = spec.key_step as usize;
    let default_flags: Vec<&'static str> = spec.key_flags.to_vec();

    if first == 0 || first > key_args.len() {
        return Ok(Vec::new());
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
    Ok(result)
}

fn extract_module_keys<'a>(
    info: &crate::engine::extension::module::ModuleCommandInfo,
    args: &'a [Bytes],
) -> Result<CommandKeys<'a>, ()> {
    let specs = if info.key_specs.is_empty() {
        let Some((first, last, step)) = info.legacy_keys else {
            return Ok(Vec::new());
        };
        if first <= 0 {
            return Ok(Vec::new());
        }
        vec![crate::engine::extension::module::ModuleKeySpec {
            flags: vec!["RW", "access", "update"],
            begin_search: crate::engine::extension::module::ModuleKeyBeginSearch::Index {
                pos: first,
            },
            find_keys: crate::engine::extension::module::ModuleKeyFindKeys::Range {
                last_key: if last < 0 {
                    last
                } else {
                    last.checked_sub(first).ok_or(())?
                },
                key_step: step,
                limit: 0,
            },
        }]
    } else {
        info.key_specs.clone()
    };
    let mut keys = Vec::new();
    let argc = i64::try_from(args.len())
        .map_err(|_| ())?
        .checked_add(1)
        .ok_or(())?;
    for spec in &specs {
        if spec.flags.contains(&"not_key") {
            continue;
        }
        let is_incomplete = spec.flags.contains(&"incomplete");
        let mut first = match &spec.begin_search {
            crate::engine::extension::module::ModuleKeyBeginSearch::Index { pos } => {
                let first = i64::from(*pos);
                if first <= 0 {
                    return Err(());
                }
                if first >= argc {
                    if is_incomplete {
                        return Err(());
                    }
                    continue;
                }
                first
            }
            crate::engine::extension::module::ModuleKeyBeginSearch::Keyword {
                keyword,
                start_from,
            } => {
                let mut index = if *start_from > 0 {
                    i64::from(*start_from)
                } else {
                    argc.checked_add(i64::from(*start_from)).ok_or(())?
                };
                let end = if *start_from > 0 { argc - 1 } else { 1 };
                let direction = if index <= end { 1 } else { -1 };
                let mut found = None;
                while index != end && index > 0 && index < argc {
                    let arg_index = usize::try_from(index - 1).map_err(|_| ())?;
                    if args[arg_index].eq_ignore_ascii_case(keyword.as_bytes()) {
                        found = Some(index.checked_add(1).ok_or(())?);
                        break;
                    }
                    index = index.checked_add(direction).ok_or(())?;
                }
                match found {
                    Some(first) => first,
                    None => continue,
                }
            }
            crate::engine::extension::module::ModuleKeyBeginSearch::Unknown => return Err(()),
        };
        let (last, step) = match &spec.find_keys {
            crate::engine::extension::module::ModuleKeyFindKeys::Range {
                last_key,
                key_step,
                limit,
            } => {
                let last = if *last_key >= 0 {
                    first.checked_add(i64::from(*last_key)).ok_or(())?
                } else if *limit == 0 {
                    argc.checked_add(i64::from(*last_key)).ok_or(())?
                } else {
                    if *limit < 0 || *last_key != -1 {
                        return Err(());
                    }
                    first
                        .checked_add((argc - first) / i64::from(*limit))
                        .and_then(|last| last.checked_sub(1))
                        .ok_or(())?
                };
                (last, i64::from(*key_step))
            }
            crate::engine::extension::module::ModuleKeyFindKeys::KeyNum {
                key_num_idx,
                first_key,
                key_step,
            } => {
                let count_pos = first.checked_add(i64::from(*key_num_idx)).ok_or(())?;
                if count_pos <= 0 || count_pos >= argc {
                    return Err(());
                }
                let count_index = usize::try_from(count_pos - 1).map_err(|_| ())?;
                let count = parse_key_count(&args[count_index]).ok_or(())?;
                first = first.checked_add(i64::from(*first_key)).ok_or(())?;
                let step = i64::from(*key_step);
                let last = count
                    .checked_sub(1)
                    .and_then(|count| count.checked_mul(step))
                    .and_then(|offset| first.checked_add(offset))
                    .ok_or(())?;
                if i32::try_from(last).is_err() {
                    return Err(());
                }
                (last, step)
            }
            crate::engine::extension::module::ModuleKeyFindKeys::Unknown => return Err(()),
        };
        if step <= 0 || first <= 0 || first >= argc || last < first || last >= argc {
            return Err(());
        }
        let mut index = first;
        while index <= last {
            let arg_index = usize::try_from(index - 1).map_err(|_| ())?;
            keys.push((args[arg_index].as_ref(), spec.flags.clone()));
            index = index.checked_add(step).ok_or(())?;
        }
        if is_incomplete {
            return Err(());
        }
    }
    Ok(keys)
}

fn parse_key_count(value: &[u8]) -> Option<i64> {
    if value.is_empty()
        || (value.len() > 1 && value[0] == b'0')
        || !value.iter().all(u8::is_ascii_digit)
    {
        return None;
    }
    std::str::from_utf8(value).ok()?.parse().ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::framework::commands::introspection::CommandCatalog;

    fn assert_pfmerge_keys(key_args: &[&'static [u8]], expected_flags: &[&[&'static str]]) {
        assert_eq!(key_args.len(), expected_flags.len());
        let mut registry = CommandRegistry::new();
        registry.register(crate::framework::commands::hyperloglog::pfmerge::PFMERGE_SPEC);
        let mut ctx = ServerContext::new(1);
        ctx.set_command_registry_snapshot(&registry);
        let mut args = vec![Bytes::from_static(b"PfMeRgE")];
        args.extend(key_args.iter().map(|key| Bytes::from_static(key)));
        let expected_keys = Value::Array(
            key_args
                .iter()
                .map(|key| Value::bulk(Bytes::from_static(key)))
                .collect(),
        );
        let expected_keys_and_flags = Value::Array(
            key_args
                .iter()
                .zip(expected_flags)
                .map(|(key, flags)| {
                    Value::Array(vec![
                        Value::bulk(Bytes::from_static(key)),
                        Value::Array(
                            flags
                                .iter()
                                .map(|flag| Value::bulk(Bytes::from_static(flag.as_bytes())))
                                .collect(),
                        ),
                    ])
                })
                .collect(),
        );
        let catalogs: [&dyn CommandCatalog; 2] =
            [&ctx, &super::super::static_catalog::STATIC_COMMAND_CATALOG];
        for catalog in catalogs {
            assert_eq!(catalog.getkeys(&args, false), expected_keys);
            assert_eq!(catalog.getkeys(&args, true), expected_keys_and_flags);
        }
        assert_eq!(
            extract_command_keys("pfmerge", &args[1..], &ctx),
            key_args
                .iter()
                .zip(expected_flags)
                .map(|(key, flags)| (*key, flags.to_vec()))
                .collect::<Vec<_>>(),
        );
        assert_eq!(
            registry
                .lookup(b"pfmerge")
                .unwrap()
                .descriptor()
                .extract_key_accesses(&crate::framework::CommandArgs::from_parts(args)),
            Some(
                key_args
                    .iter()
                    .zip(expected_flags)
                    .map(|(key, flags)| (Bytes::from_static(key), flags.to_vec()))
                    .collect::<Vec<_>>(),
            ),
        );
    }

    #[test]
    fn pfmerge_getkeys_without_source_keys() {
        assert_pfmerge_keys(&[b"k1"], &[&["RW", "access", "insert"]]);
    }

    #[test]
    fn pfmerge_getkeys_sources_are_read_only() {
        assert_pfmerge_keys(
            &[b"k1", b"k2"],
            &[&["RW", "access", "insert"], &["RO", "access"]],
        );
        assert_pfmerge_keys(
            &[b"k1", b"k2", b"k3"],
            &[
                &["RW", "access", "insert"],
                &["RO", "access"],
                &["RO", "access"],
            ],
        );
    }

    #[test]
    fn pfmerge_getkeys_preserves_duplicate_sources_and_destination_aliases() {
        assert_pfmerge_keys(
            &[b"k1", b"k2", b"k2", b"k1", b"k1"],
            &[
                &["RW", "access", "insert"],
                &["RO", "access"],
                &["RO", "access"],
                &["RO", "access"],
                &["RO", "access"],
            ],
        );
    }

    #[test]
    fn pfmerge_getkeys_preserves_binary_and_empty_keys() {
        assert_pfmerge_keys(
            &[b"dst\0\xff", b"", b"src\0\xfe", b"dst\0\xff"],
            &[
                &["RW", "access", "insert"],
                &["RO", "access"],
                &["RO", "access"],
                &["RO", "access"],
            ],
        );
    }
}
