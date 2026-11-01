use crate::foundation::io::protocol::Value;
use crate::framework::context::ServerContext;
use crate::framework::CommandRegistry;
use crate::framework::CMD_SENTINEL;
use bytes::Bytes;

pub(crate) fn command_docs(reg: &CommandRegistry, ctx: &ServerContext, args: &[Bytes]) -> Value {
    if args.is_empty() {
        return Value::Map(Vec::new());
    }
    let is_sentinel = ctx.sentinel_state().sentinel_mode;
    let mut result = Vec::new();
    for name_bytes in args {
        let name = match std::str::from_utf8(name_bytes) {
            Ok(s) => s.to_ascii_lowercase(),
            Err(_) => continue,
        };
        let parent_name = name.split('|').next().unwrap_or(&name);
        let found = reg
            .iter()
            .find(|spec| spec.name.as_str().eq_ignore_ascii_case(&name))
            .or_else(|| {
                reg.iter()
                    .find(|spec| spec.name.as_str().eq_ignore_ascii_case(parent_name))
            });
        if let Some(spec) = found {
            if is_sentinel && (spec.flags & CMD_SENTINEL) == 0 {
                continue;
            }
            let doc = if let Some(module_name) = ctx
                .module_registry()
                .command_module_name(spec.name.as_str())
            {
                module_doc(ctx, spec.name.as_str(), module_name)
            } else {
                super::render::builtin_doc(&name)
            };
            result.push((Value::bulk(name_bytes.clone()), doc));
        }
    }
    if result.is_empty() {
        Value::null()
    } else {
        Value::Map(result)
    }
}

fn module_doc(ctx: &ServerContext, command_name: &str, module_name: &str) -> Value {
    let info = ctx
        .module_registry()
        .lookup_cmd_func(command_name)
        .and_then(|entry| entry.info.as_ref());
    let mut fields = vec![
        (
            bulk("summary"),
            Value::bulk(Bytes::copy_from_slice(
                info.and_then(|info| info.summary.as_deref())
                    .unwrap_or("")
                    .as_bytes(),
            )),
        ),
        (
            bulk("since"),
            Value::bulk(Bytes::copy_from_slice(
                info.and_then(|info| info.since.as_deref())
                    .unwrap_or("")
                    .as_bytes(),
            )),
        ),
        (bulk("group"), bulk("module")),
        (
            bulk("module"),
            Value::bulk(Bytes::copy_from_slice(module_name.as_bytes())),
        ),
    ];
    let subcommands: Vec<_> = ctx
        .module_registry()
        .subcommand_names(command_name)
        .into_iter()
        .flat_map(|name| {
            [
                Value::bulk(Bytes::copy_from_slice(name.as_bytes())),
                Value::Map(vec![
                    (bulk("group"), bulk("module")),
                    (
                        bulk("module"),
                        Value::bulk(Bytes::copy_from_slice(module_name.as_bytes())),
                    ),
                ]),
            ]
        })
        .collect();
    if !subcommands.is_empty() {
        fields.push((bulk("subcommands"), Value::Array(subcommands)));
    }
    Value::Map(fields)
}

fn bulk(value: &'static str) -> Value {
    Value::bulk(Bytes::from_static(value.as_bytes()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::extension::module::{ModuleCommandInfo, ModuleRef};

    #[test]
    fn module_docs_keep_exported_fields_after_acl_metadata_updates() {
        let mut ctx = ServerContext::new(1);
        ctx.module_registry_mut()
            .register(ModuleRef::new("docs-probe", std::ptr::null_mut(), 1, true))
            .unwrap();
        ctx.module_registry_mut()
            .register_command("docs-probe", "docs-probe.xadd", None)
            .unwrap();
        assert!(ctx.module_registry_mut().set_legacy_command_info(
            "docs-probe.xadd",
            (0, 0, 0),
            false,
            -5,
        ));
        let mut info = ModuleCommandInfo {
            summary: Some("Appends a message.".into()),
            since: Some("5.0.0".into()),
            key_specs: Vec::new(),
            acl_categories: Vec::new(),
            legacy_keys: None,
            getkeys_api: false,
            arity: None,
        };
        assert!(ctx
            .module_registry_mut()
            .set_command_info("docs-probe.xadd", info.clone()));
        info.summary = None;
        info.since = None;
        info.acl_categories.push("write".into());
        assert!(ctx
            .module_registry_mut()
            .set_command_info("docs-probe.xadd", info));
        assert_eq!(
            module_doc(&ctx, "docs-probe.xadd", "docs-probe"),
            Value::Map(vec![
                (bulk("summary"), Value::bulk("Appends a message.")),
                (bulk("since"), Value::bulk("5.0.0")),
                (bulk("group"), bulk("module")),
                (bulk("module"), Value::bulk("docs-probe")),
            ])
        );
    }
}
