use crate::engine::command::CmdFlags;
use crate::engine::command::CommandArgs;
use crate::framework::context::ServerContext;

/// Resolve the ACL categories of the concrete subcommand selected by the
/// first argument. Unknown or non-UTF-8 subcommands retain the container's
/// categories so the handler can report its normal syntax error.
pub(crate) fn effective_acl_categories(
    name: &str,
    base_categories: &'static [&'static str],
    sub: Option<&[u8]>,
) -> &'static [&'static str] {
    crate::framework::spec::effective_acl_categories(name, base_categories, sub)
}

/// Resolve flags that depend on the full invocation. Besides container
/// subcommands, EVAL/EVALSHA/FCALL derive their write capability from the
/// selected script or function, matching C `getCommandFlags()`.
pub(crate) fn effective_command_flags_for_call(
    name: &str,
    base_flags: CmdFlags,
    args: &CommandArgs,
    ctx: &ServerContext,
) -> CmdFlags {
    crate::framework::spec::effective_command_flags_for_call(name, base_flags, args, ctx)
}

#[cfg(test)]
mod tests {
    use super::{effective_acl_categories, effective_command_flags_for_call};
    use crate::engine::command::{
        CommandArgs, CMD_DENYOOM, CMD_MAY_REPLICATE, CMD_STALE, CMD_WRITE,
    };
    use crate::framework::context::ServerContext;
    use bytes::Bytes;

    #[test]
    fn effective_flags_include_cluster_setslot_replication_metadata() {
        let ctx = ServerContext::new(1);
        let args = CommandArgs::from_parts(vec![
            Bytes::from_static(b"CLUSTER"),
            Bytes::from_static(b"SETSLOT"),
        ]);
        let flags = effective_command_flags_for_call("cluster", 0, &args, &ctx);
        assert_ne!(flags & CMD_MAY_REPLICATE, 0);
    }

    #[test]
    fn effective_acl_categories_use_resolved_subcommand_metadata() {
        assert_eq!(
            effective_acl_categories("config", &["SLOW"], Some(b"GET")),
            &["ADMIN", "DANGEROUS", "SLOW"]
        );
        assert_eq!(
            effective_acl_categories("get", &["FAST", "READ", "STRING"], None),
            &["FAST", "READ", "STRING"]
        );
    }

    #[test]
    fn effective_flags_derive_eval_write_capability_from_shebang() {
        let ctx = ServerContext::new(1);
        let writable = CommandArgs::from_parts(vec![
            Bytes::from_static(b"EVAL"),
            Bytes::from_static(b"#!lua\nreturn 1"),
            Bytes::from_static(b"0"),
        ]);
        let readonly = CommandArgs::from_parts(vec![
            Bytes::from_static(b"EVAL"),
            Bytes::from_static(b"#!lua flags=no-writes\nreturn 1"),
            Bytes::from_static(b"0"),
        ]);

        let write_flags =
            effective_command_flags_for_call("eval", CMD_MAY_REPLICATE, &writable, &ctx);
        assert_ne!(write_flags & CMD_WRITE, 0);
        assert_ne!(write_flags & CMD_DENYOOM, 0);
        assert_eq!(write_flags & CMD_MAY_REPLICATE, 0);

        let read_flags =
            effective_command_flags_for_call("eval", CMD_MAY_REPLICATE, &readonly, &ctx);
        assert_eq!(
            read_flags & (CMD_WRITE | CMD_DENYOOM | CMD_MAY_REPLICATE),
            0
        );
    }

    #[test]
    fn effective_flags_preserve_base_on_bad_shebang_and_derive_allow_stale() {
        let ctx = ServerContext::new(1);
        let malformed = CommandArgs::from_parts(vec![
            Bytes::from_static(b"EVAL"),
            Bytes::from_static(b"#!missing\nreturn 1"),
            Bytes::from_static(b"0"),
        ]);
        assert_eq!(
            effective_command_flags_for_call("eval", CMD_MAY_REPLICATE, &malformed, &ctx),
            CMD_MAY_REPLICATE
        );

        let stale = CommandArgs::from_parts(vec![
            Bytes::from_static(b"EVAL"),
            Bytes::from_static(b"#!lua flags=no-writes,allow-stale\nreturn 1"),
            Bytes::from_static(b"0"),
        ]);
        let flags = effective_command_flags_for_call("eval", CMD_MAY_REPLICATE, &stale, &ctx);
        assert_ne!(flags & CMD_STALE, 0);
        assert_eq!(flags & (CMD_WRITE | CMD_DENYOOM | CMD_MAY_REPLICATE), 0);
    }

    #[test]
    fn effective_fcall_flags_derive_allow_stale_from_registered_function() {
        let mut ctx = ServerContext::new(1);
        ctx.script_mut()
            .function_registry
            .load_library(
                "allow_stale_flags",
                "#!lua name=allow_stale_flags\nserver.register_function{function_name='allow_stale_flags_fn', callback=function() return 'hello' end, flags={'allow-stale', 'no-writes'}}",
            )
            .expect("test function library must compile");
        let args = CommandArgs::from_parts(vec![
            Bytes::from_static(b"FCALL"),
            Bytes::from_static(b"allow_stale_flags_fn"),
            Bytes::from_static(b"0"),
        ]);

        let flags = effective_command_flags_for_call("fcall", CMD_STALE, &args, &ctx);

        assert_ne!(flags & CMD_STALE, 0);
        assert_eq!(flags & (CMD_WRITE | CMD_DENYOOM | CMD_MAY_REPLICATE), 0);
    }
}
