use super::ServerContext;
use crate::engine::command::CommandArgs;
use crate::engine::extension::hotkeys::HotkeysConfig;
use crate::foundation::memory::key_access::{KeyAccessObserver, KeyAccessScope};
use std::rc::Rc;

impl ServerContext {
    pub(crate) fn configure_hotkeys(&self, force: bool) {
        let config = &self.config().server;
        self.hotkeys.configure(
            HotkeysConfig {
                top_k: config.hotkeys_top_k,
                sampling_percentage: config.hotkeys_sampling_percentage,
                window_seconds: config.hotkeys_window_seconds,
            },
            force,
        );
    }

    pub(crate) fn hotkeys_command_scope(&self, args: &CommandArgs) -> KeyAccessScope {
        self.configure_hotkeys(false);
        let is_introspection = [b"object".as_slice(), b"debug", b"memory"]
            .iter()
            .any(|name| args.name.eq_ignore_ascii_case(name));
        let observer =
            (!self.is_loading() && !is_introspection && self.config().server.hotkeys_top_k != 0)
                .then(|| Rc::clone(&self.hotkeys) as Rc<dyn KeyAccessObserver>);
        KeyAccessScope::enter(
            observer,
            args.name.eq_ignore_ascii_case(b"del") || args.name.eq_ignore_ascii_case(b"unlink"),
        )
    }

    pub(crate) fn hotkeys(&self) -> &crate::engine::extension::hotkeys::Hotkeys {
        &self.hotkeys
    }

    pub(crate) fn take_slot_entries(
        &mut self,
        slot: usize,
    ) -> Vec<crate::foundation::io::store::kv_engine::SlotEntry> {
        self.hotkeys.purge(|_, key| {
            usize::from(crate::engine::distributed::cluster::node::key_hash_slot(
                key,
            )) == slot
        });
        let _suppression = crate::foundation::memory::key_access::KeyAccessSuppression::enter();
        self.dbs_mut().take_slot(slot)
    }
}
