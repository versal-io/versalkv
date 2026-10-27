use crate::framework::CommandRegistry;
use crate::framework::CMD_SENTINEL;
use crate::framework::context::ServerContext;
use crate::foundation::io::protocol::Value;
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
            .or_else(|| reg.iter().find(|spec| spec.name.as_str().eq_ignore_ascii_case(parent_name)));
        if let Some(spec) = found {
            if is_sentinel && (spec.flags & CMD_SENTINEL) == 0 {
                continue;
            }
            let doc = if let Some(module_name) =
                ctx.module_registry().command_module_name(spec.name.as_str())
            {
                module_doc(ctx, spec.name.as_str(), module_name)
            } else {
                Value::Map(vec![
                    (bulk("summary"), Value::bulk(Bytes::from_static(b""))),
                    (bulk("since"), Value::bulk(Bytes::from_static(b""))),
                    (bulk("group"), Value::bulk(Bytes::from_static(b""))),
                ])
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
    let mut fields = vec![
        (bulk("summary"), Value::bulk(Bytes::from_static(b""))),
        (bulk("since"), Value::bulk(Bytes::from_static(b""))),
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
