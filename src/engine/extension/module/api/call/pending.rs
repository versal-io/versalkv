use libc::{c_int, c_void};

use super::call_api::{deliver_call_reply, ModuleCallFormat, ValkeyModuleReplyHandlers};
use super::call_conn::ModuleCallConn;
use super::call_reply::{ValkeyModuleCallReply, ValkeyModule_FreeCallReply};
use crate::engine::command::CommandDescriptor;
use crate::engine::extension::module::{ValkeyModuleCtx, ValkeyModuleString};
use crate::engine::extension::script::PropagationOp;
use crate::foundation::io::protocol::Value;
use bytes::Bytes;

#[derive(Default)]
pub struct ModuleCallEffects {
    pub(crate) dirty_delta: Option<i64>,
    pub(crate) dirty_keys: Vec<(u32, Bytes)>,
    pub(crate) propagation: Vec<PropagationOp>,
    pub(crate) notifications: Vec<(Bytes, Bytes)>,
    pub(crate) shard_notifications: Vec<(Bytes, Bytes)>,
    pub(crate) cluster_notifications: Vec<(Bytes, Bytes)>,
    pub(crate) cluster_shard_notifications: Vec<(Bytes, Bytes)>,
}

impl ModuleCallEffects {
    pub(crate) fn is_empty(&self) -> bool {
        self.dirty_delta.is_none()
            && self.dirty_keys.is_empty()
            && self.propagation.is_empty()
            && self.notifications.is_empty()
            && self.shard_notifications.is_empty()
            && self.cluster_notifications.is_empty()
            && self.cluster_shard_notifications.is_empty()
    }
}

pub struct PendingModuleCall {
    pub(crate) module_name: String,
    pub(crate) conn: ModuleCallConn,
    pub(crate) handlers: Option<ValkeyModuleReplyHandlers>,
    pub(crate) reply_ctx: *mut c_void,
    pub(crate) command: CommandDescriptor,
    pub(crate) format: ModuleCallFormat,
    argv: Vec<*mut ValkeyModuleString>,
}

impl PendingModuleCall {
    #[cfg(test)]
    pub(crate) fn from_test_command(
        conn: ModuleCallConn,
        command: CommandDescriptor,
        args: &crate::engine::command::CommandArgs,
    ) -> Self {
        let mut argv: Vec<_> = std::iter::once(&args.name)
            .chain(args.args.iter())
            .map(|value| Box::into_raw(ValkeyModuleString::from_bytes(value)))
            .collect();
        // SAFETY: argv owns live strings, and the pending owner retains them before release.
        let call = unsafe {
            Self::new(
                "pending-test".to_owned(),
                conn,
                argv.as_mut_ptr(),
                argv.len() as c_int,
                std::ptr::null(),
                std::ptr::null_mut(),
                command,
                ModuleCallFormat::from_raw((1 << 11) | (1 << 0)),
            )
        };
        for string in argv {
            // SAFETY: only the caller reference is released; the pending owner remains live.
            unsafe { ValkeyModuleString::release(string) };
        }
        call
    }

    /// # Safety
    /// argv contains argc live module strings; the handler table is live.
    // SAFETY: the caller supplies live borrowed argv and a readable handler table.
    pub(crate) unsafe fn new(
        module_name: String,
        conn: ModuleCallConn,
        argv: *mut *mut ValkeyModuleString,
        argc: c_int,
        handlers: *const ValkeyModuleReplyHandlers,
        reply_ctx: *mut c_void,
        command: CommandDescriptor,
        format: ModuleCallFormat,
    ) -> Self {
        let mut owned_argv = Vec::with_capacity(argc as usize);
        // SAFETY: the caller guarantees the positive-length array and all strings.
        for string in unsafe { std::slice::from_raw_parts(argv, argc as usize) } {
            // SAFETY: each borrowed string is retained for the pending client.
            unsafe { ValkeyModuleString::incr_ref(*string) };
            owned_argv.push(*string);
        }
        Self {
            module_name,
            conn,
            // SAFETY: a non-null handler table is live and copied, never retained by pointer.
            handlers: unsafe { handlers.as_ref().copied() },
            reply_ctx,
            command,
            format,
            argv: owned_argv,
        }
    }

    pub(crate) fn suppress_callbacks(&mut self) {
        self.handlers = None;
        self.reply_ctx = std::ptr::null_mut();
    }

    /// # Safety
    /// ctx is a fresh live module context for this completion.
    // SAFETY: the caller keeps the owned completion context live until callbacks return.
    pub(crate) unsafe fn complete(mut self, ctx: *mut ValkeyModuleCtx, value: Value) {
        use crate::engine::command::context::CommandConn;
        let is_resp3 = self.conn.resp_version() == 3;
        let reply = if let Some(proto) = self.conn.take_raw_reply() {
            ValkeyModuleCallReply::from_raw_proto_with_resp(proto, is_resp3)
        } else {
            ValkeyModuleCallReply::from_value_with_resp(&value, is_resp3, is_resp3)
        };
        let reply = Box::into_raw(reply);
        if let Some(handlers) = self.handlers.take() {
            // SAFETY: the owned reply and copied handlers remain live throughout dispatch.
            if unsafe { deliver_call_reply(ctx, reply, &handlers, self.reply_ctx) }.is_err() {
                log::warn!("Unable to dispatch pending module call reply");
            }
        }
        // SAFETY: this is the fresh owned root and all callbacks have returned.
        unsafe { ValkeyModule_FreeCallReply(reply) };
    }
}

impl Drop for PendingModuleCall {
    fn drop(&mut self) {
        for string in self.argv.drain(..) {
            // SAFETY: new retained one reference per entry; drop consumes it once.
            unsafe { ValkeyModuleString::release(string) };
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::command::context::{BlockedCmd, CommandConn};
    use crate::engine::command::{CommandArgs, CommandName};
    use crate::engine::extension::module::lifecycle_test_support;

    #[test]
    fn pending_call_keeps_argv_alive_until_cancelled_owner_is_dropped() {
        let mut conn = ModuleCallConn::new(0);
        conn.set_deny_blocking(false);
        let args = CommandArgs::from_parts(vec![
            Bytes::from_static(b"BLPOP"),
            Bytes::from_static(b"pending"),
            Bytes::from_static(b"0"),
        ]);
        conn.enter_blocking(
            BlockedCmd::Blpop,
            vec![Bytes::from_static(b"pending")],
            0,
            &args,
            0,
        );
        let mut argv: Vec<_> = std::iter::once(&args.name)
            .chain(args.args.iter())
            .map(|value| Box::into_raw(ValkeyModuleString::from_bytes(value)))
            .collect();
        let command = CommandDescriptor {
            name: CommandName::Static("blpop"),
            arity: -3,
            flags: crate::engine::command::CMD_WRITE,
            first_key: 1,
            last_key: -2,
            key_step: 1,
            container: None,
            acl_categories: &[],
        };
        let before = lifecycle_test_support::snapshot();
        // SAFETY: all argv pointers are owned, live module strings; no handlers are installed.
        let mut call = unsafe {
            PendingModuleCall::new(
                "pending-test".to_owned(),
                conn,
                argv.as_mut_ptr(),
                argv.len() as c_int,
                std::ptr::null(),
                std::ptr::null_mut(),
                command,
                ModuleCallFormat::from_raw(1 << 11),
            )
        };
        for string in argv {
            // SAFETY: release only the caller's original reference, not the retained pending owner.
            unsafe { ValkeyModuleString::release(string) };
        }
        assert_eq!(lifecycle_test_support::snapshot().strings, before.strings);
        call.suppress_callbacks();
        drop(call);
        assert_eq!(
            lifecycle_test_support::snapshot().strings,
            before.strings + 3
        );
    }
}
