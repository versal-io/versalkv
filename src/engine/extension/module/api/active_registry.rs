use crate::engine::extension::module::ModuleRegistry;
use crate::engine::extension::module::ValkeyModuleCtx;
use std::cell::Cell;

thread_local! {
    static ACTIVE_MODULE_REGISTRY: Cell<*mut ModuleRegistry> =
        const { Cell::new(std::ptr::null_mut()) };
    static ACTIVE_MODULE_CONTEXT: Cell<*mut ValkeyModuleCtx> =
        const { Cell::new(std::ptr::null_mut()) };
}

pub(crate) fn active_registry_ptr() -> Option<*mut ModuleRegistry> {
    ACTIVE_MODULE_REGISTRY.with(|active| {
        let registry = active.get();
        (!registry.is_null()).then_some(registry)
    })
}

pub(crate) struct ActiveModuleRegistryScope {
    previous: *mut ModuleRegistry,
}

impl ActiveModuleRegistryScope {
    pub(crate) fn enter(registry: *mut ModuleRegistry) -> Option<Self> {
        if registry.is_null() {
            return None;
        }
        let previous = ACTIVE_MODULE_REGISTRY.with(|active| {
            let previous = active.get();
            active.set(registry);
            previous
        });
        Some(Self { previous })
    }
}

impl Drop for ActiveModuleRegistryScope {
    fn drop(&mut self) {
        ACTIVE_MODULE_REGISTRY.with(|active| active.set(self.previous));
    }
}

pub(crate) struct ActiveModuleContextScope {
    previous: *mut ValkeyModuleCtx,
}

impl ActiveModuleContextScope {
    pub(crate) fn enter(ctx: *mut ValkeyModuleCtx) -> Self {
        let previous = ACTIVE_MODULE_CONTEXT.with(|active| {
            let previous = active.get();
            active.set(ctx);
            previous
        });
        Self { previous }
    }
}

impl Drop for ActiveModuleContextScope {
    fn drop(&mut self) {
        ACTIVE_MODULE_CONTEXT.with(|active| active.set(self.previous));
    }
}

pub(crate) fn active_module_context() -> Option<*mut ValkeyModuleCtx> {
    ACTIVE_MODULE_CONTEXT.with(|active| {
        let ctx = active.get();
        (!ctx.is_null()).then_some(ctx)
    })
}

pub(crate) fn with_active_registry<F, R>(f: F) -> Option<R>
where
    F: FnOnce(&mut ModuleRegistry) -> R,
{
    let registry = active_registry_ptr()?;

    // The pointer is installed by ActiveModuleRegistryScope for one synchronous
    // module callback and restored before the callback returns.
    // SAFETY: The pointer was obtained from `active_registry_ptr()` which returns a
    // non-null pointer previously installed by `ActiveModuleRegistryScope::enter`.
    // The scope guarantees the pointer is valid for the duration of this synchronous
    // callback, and we have verified it is non-null via the `?` operator above.
    Some(unsafe { f(&mut *registry) })
}

#[cfg(test)]
mod tests {
    use super::{active_registry_ptr, ActiveModuleRegistryScope};
    use crate::engine::extension::module::api::ctx::ValkeyModule_IsModuleNameBusy;
    use crate::engine::extension::module::ModuleRegistry;
    use std::ffi::CString;

    #[test]
    fn nested_scopes_restore_the_previous_registry() {
        let mut outer = ModuleRegistry::new();
        let mut inner = ModuleRegistry::new();
        let outer_ptr = &mut outer as *mut ModuleRegistry;

        let outer_scope = ActiveModuleRegistryScope::enter(outer_ptr).unwrap();
        assert_eq!(active_registry_ptr(), Some(outer_ptr));
        {
            let inner_scope =
                ActiveModuleRegistryScope::enter(&mut inner as *mut ModuleRegistry).unwrap();
            assert_ne!(active_registry_ptr(), Some(outer_ptr));
            drop(inner_scope);
        }
        assert_eq!(active_registry_ptr(), Some(outer_ptr));
        drop(outer_scope);
        assert_eq!(active_registry_ptr(), None);
    }

    #[test]
    fn no_context_lookup_returns_an_abi_error_without_an_active_scope() {
        let name = CString::new("missing").unwrap();

        // SAFETY: ValkeyModule_IsModuleNameBusy is an extern "C" function that
        // reads a C string from the provided pointer. The CString `name` is
        // guaranteed to be a valid null-terminated string that lives for the
        // duration of the call.
        assert_eq!(unsafe { ValkeyModule_IsModuleNameBusy(name.as_ptr()) }, 1);
    }
}
