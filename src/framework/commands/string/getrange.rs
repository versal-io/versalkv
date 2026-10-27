#[allow(non_upper_case_globals)]
pub const GETRANGE_SPEC: crate::framework::CommandSpec = {
    use crate::framework::generated::commands::GETRANGE_META as META;
    crate::framework::CommandSpec {
        name: crate::engine::command::CommandName::Static(META.name),
        arity: META.arity,
        flags: META.flags,
        dispatch_group: META.dispatch_group,
        first_key: META.first_key,
        last_key: META.last_key,
        key_step: META.key_step,
        container: META.container,
        key_flags: META.key_flags,
        acl_categories: META.acl_categories,
        executor: crate::framework::CommandExecutor::Runtime(super::strops::getrange),
        get_keys: META.get_keys,
    }
};
