use bytes::Bytes;

use crate::engine::command::CommandError;
use crate::engine::core::Value;
use crate::framework::commands::command_runtime::RuntimeCommandSpec;
use crate::framework::commands::command_runtime::{command_specs, lookup_command_name};
use crate::framework::commands::introspection::CommandCatalog;
use crate::framework::CommandMeta;

use super::render::{metadata_entry, metadata_name};

pub struct StaticCommandCatalog;

pub static STATIC_COMMAND_CATALOG: StaticCommandCatalog = StaticCommandCatalog;

command_macros::portable_command_support!();

fn all_command_info() -> Value {
    let mut entries = command_specs()
        .iter()
        .map(|spec| make_entry(spec.meta, true))
        .collect::<Vec<_>>();
    entries.sort_by(|left, right| entry_name(left).cmp(entry_name(right)));
    Value::Array(entries)
}

fn command_info(names: &[Bytes]) -> Value {
    if names.is_empty() {
        return all_command_info();
    }
    Value::Array(
        names
            .iter()
            .map(|name| {
                std::str::from_utf8(name)
                    .ok()
                    .and_then(supported_meta)
                    .map(|meta| make_entry(meta, meta.container.is_none()))
                    .unwrap_or_else(Value::null)
            })
            .collect(),
    )
}

fn command_docs(names: &[Bytes]) -> Value {
    if names.is_empty() {
        return Value::Map(Vec::new());
    }
    let docs = names
        .iter()
        .filter_map(|name| {
            let command_name = std::str::from_utf8(name).ok()?;
            supported_meta(command_name)?;
            Some((
                Value::bulk(name.clone()),
                Value::Map(vec![
                    (bulk("summary"), Value::bulk(Bytes::new())),
                    (bulk("since"), Value::bulk(Bytes::new())),
                    (bulk("group"), Value::bulk(Bytes::new())),
                ]),
            ))
        })
        .collect::<Vec<_>>();
    if docs.is_empty() {
        Value::null()
    } else {
        Value::Map(docs)
    }
}

fn command_list(args: &[Bytes]) -> Value {
    let filter = match parse_list_filter(args) {
        Ok(filter) => filter,
        Err(error) => return error,
    };
    let mut names = command_specs()
        .iter()
        .map(|spec| spec.meta.name.to_owned())
        .chain(supported_subcommands().map(metadata_name))
        .filter(|name| filter.matches(name))
        .map(|name| Value::bulk(Bytes::from(name)))
        .collect::<Vec<_>>();
    names.sort_by(|left, right| bulk_bytes(left).cmp(bulk_bytes(right)));
    Value::Array(names)
}

enum ListFilter<'a> {
    None,
    Module,
    AclCategory(&'a str),
    Pattern(&'a str),
}

impl ListFilter<'_> {
    fn matches(&self, name: &str) -> bool {
        match self {
            Self::None => true,
            Self::Module => false,
            Self::AclCategory(category) => supported_meta(name).is_some_and(|meta| {
                meta.acl_categories
                    .iter()
                    .any(|candidate| candidate.eq_ignore_ascii_case(category))
            }),
            Self::Pattern(pattern) => simple_glob(name, pattern),
        }
    }
}

fn parse_list_filter(args: &[Bytes]) -> Result<ListFilter<'_>, Value> {
    if args.is_empty() {
        return Ok(ListFilter::None);
    }
    let [filterby, kind, argument] = args else {
        return Err(CommandError::syntax_error());
    };
    if !filterby.eq_ignore_ascii_case(b"filterby") {
        return Err(CommandError::syntax_error());
    }
    let argument = std::str::from_utf8(argument).map_err(|_| CommandError::syntax_error())?;
    if kind.eq_ignore_ascii_case(b"module") {
        Ok(ListFilter::Module)
    } else if kind.eq_ignore_ascii_case(b"aclcat") {
        Ok(ListFilter::AclCategory(argument))
    } else if kind.eq_ignore_ascii_case(b"pattern") {
        Ok(ListFilter::Pattern(argument))
    } else {
        Err(CommandError::syntax_error())
    }
}

fn command_getkeys(args: &[Bytes], include_flags: bool) -> Value {
    let Some((name, command_args)) = args.split_first() else {
        return CommandError::wrong_arity("command");
    };
    let Ok(name) = std::str::from_utf8(name) else {
        return CommandError::unknown(name);
    };
    let Some(spec) = lookup_command_name(name) else {
        return Value::error("ERR The command has no key arguments");
    };
    if spec.meta.first_key == 0 && spec.get_keys.is_none() {
        return Value::error("ERR The command has no key arguments");
    }
    let keys = extract_keys(spec, command_args);
    Value::Array(
        keys.into_iter()
            .map(|(key, flags)| {
                if include_flags {
                    Value::Array(vec![
                        Value::bulk(Bytes::copy_from_slice(key)),
                        Value::Array(flags.into_iter().map(bulk).collect()),
                    ])
                } else {
                    Value::bulk(Bytes::copy_from_slice(key))
                }
            })
            .collect(),
    )
}

fn extract_keys<'a>(
    spec: &RuntimeCommandSpec,
    args: &'a [Bytes],
) -> Vec<(&'a [u8], Vec<&'static str>)> {
    if let Some(get_keys) = spec.get_keys {
        return get_keys(args);
    }
    generic_range_get_keys(spec.meta, args)
}

fn generic_range_get_keys<'a>(
    meta: &CommandMeta,
    args: &'a [Bytes],
) -> Vec<(&'a [u8], Vec<&'static str>)> {
    let first = meta.first_key as usize;
    if first == 0 || first > args.len() || meta.key_step <= 0 {
        return Vec::new();
    }
    let end = if meta.last_key == -1 {
        args.len()
    } else if meta.last_key <= 0 {
        first
    } else {
        (meta.last_key as usize).min(args.len())
    };
    (first - 1..end)
        .step_by(meta.key_step as usize)
        .map(|index| (args[index].as_ref(), meta.key_flags.to_vec()))
        .collect()
}

fn supported_meta(name: &str) -> Option<&'static CommandMeta> {
    if let Some(spec) = lookup_command_name(name) {
        return Some(spec.meta);
    }
    let meta = crate::framework::generated::commands::acl_command_meta(name)?;
    let parent = meta.container?;
    lookup_command_name(parent).map(|_| meta)
}

fn supported_subcommands() -> impl Iterator<Item = &'static CommandMeta> {
    crate::framework::generated::commands::ALL_COMMAND_META
        .iter()
        .copied()
        .filter(|meta| {
            meta.container
                .is_some_and(|parent| lookup_command_name(parent).is_some())
        })
}

fn make_entry(meta: &CommandMeta, include_subcommands: bool) -> Value {
    let subcommands = if include_subcommands {
        supported_subcommands()
            .filter(|subcommand| {
                subcommand
                    .container
                    .is_some_and(|parent| parent.eq_ignore_ascii_case(meta.name))
            })
            .map(|subcommand| make_entry(subcommand, false))
            .collect()
    } else {
        Vec::new()
    };
    metadata_entry(meta, subcommands)
}

fn entry_name(entry: &Value) -> &[u8] {
    match entry {
        Value::Array(parts) => parts.first().map(bulk_bytes).unwrap_or_default(),
        _ => &[],
    }
}

fn bulk_bytes(value: &Value) -> &[u8] {
    match value {
        Value::Bulk(value) | Value::SimpleString(value) => value,
        _ => &[],
    }
}

fn bulk(value: &'static str) -> Value {
    Value::bulk(Bytes::from_static(value.as_bytes()))
}

fn simple_glob(name: &str, pattern: &str) -> bool {
    let name = name.to_ascii_lowercase();
    let pattern = pattern.to_ascii_lowercase();
    let name = name.as_bytes();
    let pattern = pattern.as_bytes();
    let (mut name_index, mut pattern_index) = (0, 0);
    let (mut star_index, mut match_index) = (None, 0);
    while name_index < name.len() {
        if pattern_index < pattern.len()
            && (pattern[pattern_index] == b'?' || pattern[pattern_index] == name[name_index])
        {
            name_index += 1;
            pattern_index += 1;
        } else if pattern_index < pattern.len() && pattern[pattern_index] == b'*' {
            star_index = Some(pattern_index);
            match_index = name_index;
            pattern_index += 1;
        } else if let Some(star) = star_index {
            pattern_index = star + 1;
            match_index += 1;
            name_index = match_index;
        } else {
            return false;
        }
    }
    while pattern_index < pattern.len() && pattern[pattern_index] == b'*' {
        pattern_index += 1;
    }
    pattern_index == pattern.len()
}

impl CommandCatalog for StaticCommandCatalog {
    fn count(&self) -> usize {
        command_specs().len()
    }

    fn all_info(&self) -> Value {
        all_command_info()
    }

    fn list(&self, args: &[Bytes]) -> Value {
        command_list(args)
    }

    fn docs(&self, names: &[Bytes]) -> Value {
        command_docs(names)
    }

    fn info(&self, names: &[Bytes]) -> Value {
        command_info(names)
    }

    fn getkeys(&self, args: &[Bytes], include_flags: bool) -> Value {
        command_getkeys(args, include_flags)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn catalog_names_are_unique_and_metadata_backed() {
        let mut names = command_specs()
            .iter()
            .map(|spec| spec.meta.name)
            .collect::<Vec<_>>();
        names.sort_unstable();
        names.dedup();
        assert_eq!(names.len(), command_specs().len());
        assert!(names.contains(&"command"));
    }

    #[test]
    fn dynamic_key_extractors_preserve_key_specific_flags() {
        let args = [
            Bytes::from_static(b"dest"),
            Bytes::from_static(b"2"),
            Bytes::from_static(b"left"),
            Bytes::from_static(b"right"),
        ];
        let spec = lookup_command_name("zinterstore").unwrap();
        assert_eq!(
            extract_keys(spec, &args),
            vec![
                (b"dest".as_slice(), vec!["OW", "update"]),
                (b"left".as_slice(), vec!["RO", "access"]),
                (b"right".as_slice(), vec!["RO", "access"]),
            ]
        );
    }
}
