mod command_info;
mod docs;
pub(crate) mod getkeys_helpers;
mod info;
mod keys;
mod list;
pub(crate) mod render;
pub(crate) mod runtime;
#[cfg(any(feature = "wasm", test))]
pub(crate) mod static_catalog;

pub(crate) use keys::extract_command_keys;
pub use runtime::COMMAND_SPEC;

use self::command_info::{cmd_entry_name, make_entry};
use crate::framework::context::ServerContext;
use crate::foundation::io::protocol::Value;
use crate::framework::commands::introspection::CommandCatalog;
use crate::framework::CommandRegistry;

fn registry(ctx: &ServerContext) -> &CommandRegistry {
    ctx.command_registry()
}

impl CommandCatalog for ServerContext {
    fn count(&self) -> usize {
        registry(self).len()
    }

    fn all_info(&self) -> Value {
        let reg = registry(self);
        let mut entries: Vec<Value> = reg
            .iter()
            .map(|spec| make_entry(spec, reg, self.module_registry()))
            .collect();
        entries.sort_by(|a, b| {
            let a_name = cmd_entry_name(a);
            let b_name = cmd_entry_name(b);
            a_name.cmp(b_name)
        });
        Value::Array(entries)
    }

    fn list(&self, args: &[bytes::Bytes]) -> Value {
        list::command_list(registry(self), self, args)
    }

    fn docs(&self, names: &[bytes::Bytes]) -> Value {
        docs::command_docs(registry(self), self, names)
    }

    fn info(&self, names: &[bytes::Bytes]) -> Value {
        info::command_info(registry(self), self.module_registry(), names)
    }

    fn getkeys(&self, args: &[bytes::Bytes], include_flags: bool) -> Value {
        if include_flags {
            keys::command_getkeysandflags(registry(self), self, args)
        } else {
            keys::command_getkeys(registry(self), self, args)
        }
    }
}
