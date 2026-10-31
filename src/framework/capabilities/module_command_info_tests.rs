use crate::engine::extension::module::api::{
    ActiveModuleRegistryScope, ValkeyModuleCommandInfo, ValkeyModuleCommandInfoVersion,
    ValkeyModuleCommandKeySpec, ValkeyModule_SetCommandInfo,
};
use crate::engine::extension::module::capability::{ModuleRuntime, ModuleRuntimeHandle};
use crate::engine::extension::module::{
    registry::ModuleCommandHandle, ModuleRef, ValkeyModuleCtx, ValkeyModuleString, VALKEYMODULE_OK,
};
use crate::framework::{context::ServerContext, CommandRegistry};
use libc::c_int;
use std::ffi::CString;

#[test]
fn command_info_copies_documentation_before_module_strings_are_freed() {
    extern "C" fn command(
        _ctx: *mut ValkeyModuleCtx,
        _argv: *mut *mut ValkeyModuleString,
        _argc: c_int,
    ) -> c_int {
        VALKEYMODULE_OK
    }

    let mut ctx = ServerContext::new(1);
    let mut registry = CommandRegistry::new();
    ctx.set_module_command_registry(&mut registry);
    assert!(ctx.register_module_command("docs-probe.command", -2, 0, 0, 0, 0));
    ctx.module_registry_mut()
        .register(ModuleRef::new("docs-probe", std::ptr::null_mut(), 1, true))
        .unwrap();
    ctx.module_registry_mut()
        .register_command("docs-probe", "docs-probe.command", Some(command))
        .unwrap();
    assert!(ctx.module_registry_mut().set_legacy_command_info(
        "docs-probe.command",
        (0, 0, 0),
        false,
        -2,
    ));
    let mut runtime_handle = ModuleRuntimeHandle::new(&mut ctx);
    let mut handle = ModuleCommandHandle {
        module_name: "docs-probe".into(),
        command_name: "docs-probe.command".into(),
        runtime_handle: (&mut runtime_handle as *mut ModuleRuntimeHandle).cast(),
    };
    let version = ValkeyModuleCommandInfoVersion {
        version: 1,
        sizeof_historyentry: 0,
        sizeof_keyspec: std::mem::size_of::<ValkeyModuleCommandKeySpec>(),
        sizeof_arg: 0,
    };
    let summary = CString::new("Appends a message.").unwrap();
    let since = CString::new("5.0.0").unwrap();
    let info = ValkeyModuleCommandInfo {
        version: &version,
        summary: summary.as_ptr(),
        complexity: std::ptr::null(),
        since: since.as_ptr(),
        history: std::ptr::null_mut(),
        tips: std::ptr::null(),
        arity: -3,
        key_specs: std::ptr::null_mut(),
        args: std::ptr::null_mut(),
    };
    let scope = ActiveModuleRegistryScope::enter(ctx.module_registry_mut()).unwrap();
    // SAFETY: all ABI structs and C strings remain live during this call, and
    // the registry scope ends before inspecting the stored information.
    assert_eq!(
        unsafe {
            ValkeyModule_SetCommandInfo((&mut handle as *mut ModuleCommandHandle).cast(), &info)
        },
        VALKEYMODULE_OK
    );
    drop(scope);
    drop(summary);
    drop(since);
    let stored = ctx
        .module_registry()
        .lookup_cmd_func("docs-probe.command")
        .unwrap()
        .info
        .as_ref()
        .unwrap();
    assert_eq!(stored.summary.as_deref(), Some("Appends a message."));
    assert_eq!(stored.since.as_deref(), Some("5.0.0"));
    assert_eq!(stored.arity, Some(-3));
    assert_eq!(registry.lookup(b"docs-probe.command").unwrap().arity, -3);
    ctx.set_module_command_registry(std::ptr::null_mut());
}
