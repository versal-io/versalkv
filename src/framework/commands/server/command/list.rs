use super::command_info::bulk_as_bytes;
use crate::framework::{CommandError, CommandRegistry};
use crate::framework::context::ServerContext;
use crate::framework::generated::commands::SUBCOMMAND_NAMES;
use crate::foundation::io::protocol::Value;
use bytes::Bytes;

pub(crate) fn command_list(reg: &CommandRegistry, ctx: &ServerContext, args: &[Bytes]) -> Value {
    let mut filterby_type: Option<&str> = None;
    let mut filterby_arg: Option<&str> = None;
    let mut i = 0; // args  "list"

    while i < args.len() {
        let tok = match std::str::from_utf8(&args[i]) {
            Ok(s) => s.to_ascii_lowercase(),
            Err(_) => return CommandError::syntax_error(),
        };
        let moreargs = args.len() - 1 - i;
        if tok == "filterby" && moreargs == 2 {
            let filtertype = match std::str::from_utf8(&args[i + 1]) {
                Ok(s) => s,
                Err(_) => return CommandError::syntax_error(),
            };
            match filtertype.to_ascii_lowercase().as_str() {
                "module" => filterby_type = Some("module"),
                "aclcat" => filterby_type = Some("aclcat"),
                "pattern" => filterby_type = Some("pattern"),
                _ => return CommandError::syntax_error(),
            }
            filterby_arg = Some(match std::str::from_utf8(&args[i + 2]) {
                Ok(s) => s,
                Err(_) => return CommandError::syntax_error(),
            });
            i += 3;
        } else {
            return CommandError::syntax_error();
        }
    }

    let is_sentinel = ctx.sentinel_state().sentinel_mode;

    let all_names: Vec<&str> = reg
        .iter()
        .map(|spec| spec.name)
        .chain(SUBCOMMAND_NAMES.iter().copied())
        .collect();
    let mut names: Vec<Value> = all_names
        .into_iter()
        .filter(|name| {
            if is_sentinel {
                let has_sentinel_flag = reg
                    .iter()
                    .find(|spec| spec.name == *name)
                    .map(|spec| (spec.flags & crate::framework::CMD_SENTINEL) != 0)
                    .unwrap_or_else(|| {
                        name.split('|')
                            .next()
                            .and_then(|parent| reg.lookup(parent.as_bytes()))
                            .map(|spec| (spec.flags & crate::framework::CMD_SENTINEL) != 0)
                            .unwrap_or(false)
                    });
                if !has_sentinel_flag {
                    return false;
                }
            }

            match filterby_type {
                Some("aclcat") => {
                    let category = filterby_arg.unwrap_or("");
                    crate::framework::generated::commands::acl_command_meta(name)
                        .is_some_and(|meta| {
                            meta.acl_categories
                                .iter()
                                .any(|candidate| candidate.eq_ignore_ascii_case(category))
                        })
                        || ctx
                            .module_registry()
                            .lookup_cmd_func(name)
                            .and_then(|entry| entry.info.as_ref())
                            .is_some_and(|info| {
                                info.acl_categories
                                    .iter()
                                    .any(|candidate| candidate.eq_ignore_ascii_case(category))
                            })
                }
                Some("pattern") => {
                    let pat = filterby_arg.unwrap_or("");
                    matches_pattern(name, pat)
                }
                Some("module") => {
                    ctx.module_registry().command_module_name(name)
                        == Some(filterby_arg.unwrap_or(""))
                }
                Some(_) => false,
                None => true,
            }
        })
        .map(|name| Value::bulk(Bytes::copy_from_slice(name.as_bytes())))
        .collect();

    names.sort_by(|a, b| {
        let a_str = bulk_as_bytes(a);
        let b_str = bulk_as_bytes(b);
        a_str.cmp(b_str)
    });
    Value::Array(names)
}

fn matches_pattern(name: &str, pattern: &str) -> bool {
    if pattern.is_empty() {
        return name.is_empty();
    }
    let pattern = pattern.to_ascii_lowercase();
    let name = name.to_ascii_lowercase();
    simple_glob(&name, &pattern)
}

fn simple_glob(name: &str, pattern: &str) -> bool {
    let name_bytes = name.as_bytes();
    let pat_bytes = pattern.as_bytes();
    let mut ni = 0;
    let mut pi = 0;
    let mut star_idx: Option<usize> = None;
    let mut match_idx = 0;

    while ni < name_bytes.len() {
        if pi < pat_bytes.len() && (pat_bytes[pi] == b'?' || pat_bytes[pi] == name_bytes[ni]) {
            ni += 1;
            pi += 1;
        } else if pi < pat_bytes.len() && pat_bytes[pi] == b'*' {
            star_idx = Some(pi);
            match_idx = ni;
            pi += 1;
        } else if let Some(si) = star_idx {
            pi = si + 1;
            match_idx += 1;
            ni = match_idx;
        } else {
            return false;
        }
    }

    while pi < pat_bytes.len() && pat_bytes[pi] == b'*' {
        pi += 1;
    }

    pi == pat_bytes.len()
}

#[cfg(test)]
mod tests {
    use super::command_list;
    use crate::foundation::io::protocol::Value;
    use crate::framework::context::ServerContext;
    use crate::framework::CommandRegistry;
    use bytes::Bytes;

    #[test]
    fn aclcat_filter_uses_generated_subcommand_categories() {
        let mut registry = CommandRegistry::new();
        crate::framework::commands::register_all(&mut registry);
        let ctx = ServerContext::new(1);
        let args = [
            Bytes::from_static(b"FILTERBY"),
            Bytes::from_static(b"ACLCAT"),
            Bytes::from_static(b"ADMIN"),
        ];

        let Value::Array(values) = command_list(&registry, &ctx, &args) else {
            panic!("expected COMMAND LIST array");
        };
        let names = values
            .into_iter()
            .map(|value| match value {
                Value::Bulk(value) => String::from_utf8(value.to_vec()).unwrap(),
                _ => panic!("expected command name"),
            })
            .collect::<Vec<_>>();

        assert!(names.contains(&"config|get".to_string()));
        assert!(!names.contains(&"get".to_string()));
    }
}
