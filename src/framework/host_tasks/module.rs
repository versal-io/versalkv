use crate::engine::command::context::CommandConn;
use crate::engine::command::CommandArgs;
use crate::engine::extension::module::api::{ActiveModuleContextScope, ActiveModuleRegistryScope};
use crate::engine::extension::module::capability::{
    ActiveModuleHostScope, ActiveModuleRuntimeScope,
};
use crate::engine::extension::module::{ValkeyModuleCmdFunc, VALKEYMODULE_OK};
use crate::engine::host::HostDispatcher;
use crate::foundation::io::protocol::Value;
use crate::framework::capabilities::connection::{CommandEffectDrain, Connection};
use crate::framework::capabilities::module::{
    module_argv, module_dispatch_name, ModuleCommandConnScope, ModuleInvocationScope,
};
use crate::framework::context::ServerContext;

use super::PreparedHostTask;

pub(crate) struct ModuleTask {
    invocation: ModuleInvocationScope,
    command_func: ValkeyModuleCmdFunc,
    command_name: String,
    conn: Box<Connection>,
    previous_busy_reply: Option<String>,
    previous_yield_clients: bool,
    original_name: Option<bytes::Bytes>,
}

pub(crate) fn prepare_module_task(
    conn: &mut dyn CommandConn,
    args: &CommandArgs,
    ctx: &mut ServerContext,
) -> Result<PreparedHostTask, Value> {
    let command_name = std::str::from_utf8(&args.name)
        .map_err(|_| Value::error("ERR invalid command name encoding"))?;
    let command_name = module_dispatch_name(command_name, args, ctx.module_registry());
    let entry = ctx
        .module_registry()
        .lookup_cmd_func(&command_name)
        .cloned()
        .ok_or_else(|| Value::error(format!("ERR module command '{command_name}' not found")))?;
    if entry.func.is_none() {
        return Err(Value::error(format!(
            "ERR unknown subcommand for module command '{command_name}'"
        )));
    }
    let mut guest = Box::new(Connection::new_with_client_id(
        conn.token(),
        conn.client_id(),
        conn.peer_addr(),
        false,
    ));
    guest.selected_db = conn.selected_db();
    guest.resp_version = conn.resp_version();
    guest.username = conn.username().to_owned();
    guest.authenticated = conn.authenticated();
    guest.ever_authenticated = conn.ever_authenticated();
    guest.name = conn.name().cloned();
    guest.module_auth_user = conn.module_auth_user().map(str::to_owned);
    guest.module_auth_callback = conn.module_auth_callback();
    guest.readonly = conn.readonly();
    guest.deny_blocking = conn.deny_blocking();
    guest.import_source = conn.import_source();
    guest.no_touch = conn.no_touch();
    guest.no_evict = conn.no_evict();
    guest.monitoring = conn.monitoring();
    guest.sub_channel_count = conn.sub_channel_count();
    guest.psub_channel_count = conn.psub_channel_count();
    guest.ssub_channel_count = conn.ssub_channel_count();
    if conn.in_transaction() {
        guest.begin_transaction();
    }
    guest.set_tracking(conn.tracking());
    guest.set_tracking_bcast(conn.tracking_bcast());
    guest.set_tracking_optin(conn.tracking_optin());
    guest.set_tracking_optout(conn.tracking_optout());
    guest.set_tracking_caching(conn.tracking_caching());
    guest.set_tracking_noloop(conn.tracking_noloop());
    guest.set_tracking_redirect(conn.tracking_redirect());
    guest.set_tracking_prefixes(conn.tracking_prefixes().to_vec());
    guest.set_tracking_broken_redirect(conn.tracking_broken_redirect());
    guest.set_last_write_offset(conn.last_write_offset());
    let previous_busy_reply = ctx.module_registry().busy_reply.clone();
    let previous_yield_clients = ctx.module_registry().yield_clients;
    let argv = module_argv(args, ctx.module_registry());
    let invocation = ModuleInvocationScope::new(
        ctx,
        &entry.module_name,
        &command_name,
        conn.selected_db(),
        conn.resp_version(),
        Some(conn.username()),
        argv,
    );
    Ok(PreparedHostTask::Module(ModuleTask {
        invocation,
        command_func: entry.func,
        command_name,
        conn: guest,
        previous_busy_reply,
        previous_yield_clients,
        original_name: conn.name().cloned(),
    }))
}

impl ModuleTask {
    pub(crate) fn session_id(&self) -> crate::engine::command::SessionId {
        self.conn.session_id()
    }

    pub(crate) fn run(&mut self, dispatcher: &mut HostDispatcher<'_>) -> Value {
        let module_ctx = self.invocation.module_ctx();
        let argv = self.invocation.argv();
        let argc = self.invocation.argc();
        let runtime_handle = self.invocation.runtime_handle();
        let registry = self.invocation.registry();
        let Some(command_func) = self.command_func else {
            return Value::error("ERR missing module command callback");
        };
        let ret = {
            let _host_scope = ActiveModuleHostScope::enter(Some(dispatcher));
            let _conn_scope = ModuleCommandConnScope::enter(&mut *self.conn);
            let _ctx_scope = ActiveModuleContextScope::enter(module_ctx);
            let _runtime_scope = ActiveModuleRuntimeScope::enter(runtime_handle);
            let _registry_scope = ActiveModuleRegistryScope::enter(registry);
            command_func(module_ctx, argv, argc)
        };
        if ret != VALKEYMODULE_OK {
            log::warn!(
                "Module command '{}' returned error code {}",
                self.command_name,
                ret
            );
        }
        self.invocation.take_reply()
    }

    pub(crate) fn finish(
        mut self,
        result: Value,
        conn: &mut dyn CommandConn,
        ctx: &mut ServerContext,
    ) -> Value {
        self.restore_busy_state(ctx);
        conn.set_selected_db(self.conn.selected_db);
        conn.set_resp_version(self.conn.resp_version);
        conn.set_username(self.conn.username.clone());
        conn.set_authenticated(self.conn.authenticated);
        conn.set_ever_authenticated(self.conn.ever_authenticated);
        conn.set_module_auth_user(self.conn.module_auth_user.clone());
        let (callback, data) = self.conn.module_auth_callback.map_or(
            (std::ptr::null_mut(), std::ptr::null_mut()),
            |(callback, data)| (callback as *mut libc::c_void, data),
        );
        conn.set_module_auth_callback(callback, data);
        if self.conn.name != self.original_name {
            conn.set_name(self.conn.name.clone());
        }
        if self.conn.state == crate::engine::command::context::ConnectionState::Closing {
            conn.mark_closing();
        }
        if let Some(mut effects) = self.conn.take_command_effects(CommandEffectDrain::Nested) {
            for position in 1..self.invocation.argc() as usize {
                if effects.is_argument_redacted(position) {
                    conn.redact_command_argument(position);
                }
            }
            if let Some(delta) = effects.take_dirty_delta() {
                conn.record_dirty_delta(delta);
            }
            if effects.skip_propagation() {
                conn.set_skip_propagate(true);
            }
            conn.extend_custom_propagation(effects.take_custom_propagation());
            conn.extend_script_propagation(effects.take_script_propagation());
            for key in effects.take_dirty_keys() {
                conn.record_dirty_key(key);
            }
            conn.extend_notifications(effects.take_notifications());
            for notification in effects.take_shard_notifications() {
                conn.record_shard_notification(notification);
            }
            for notification in effects.take_cluster_notifications() {
                conn.record_cluster_notification(notification);
            }
            for notification in effects.take_cluster_shard_notifications() {
                conn.record_cluster_shard_notification(notification);
            }
            for object in effects.take_lazyfree_objects() {
                conn.defer_lazyfree_object(object);
            }
            if let Some(dbs) = effects.take_swapdb() {
                conn.set_swapdb_dbs(Some(dbs));
            }
            if effects.has_custom_reply() {
                conn.set_custom_reply(true);
            }
            if effects.should_suppress_reply() {
                conn.suppress_reply();
            }
        }
        for reply in std::mem::take(&mut self.conn.output.queue) {
            conn.queue_reply(reply);
        }
        result
    }

    pub(crate) fn finish_disconnected(mut self, ctx: &mut ServerContext) -> Box<Connection> {
        self.restore_busy_state(ctx);
        self.conn
    }

    fn restore_busy_state(&mut self, ctx: &mut ServerContext) {
        ctx.module_registry_mut().busy_reply = self.previous_busy_reply.take();
        ctx.module_registry_mut().yield_clients = self.previous_yield_clients;
    }
}
