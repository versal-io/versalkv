use crate::foundation::io::protocol::Value;
use crate::framework::context::ServerContext;
use bytes::Bytes;

pub(crate) fn cluster_cancelslotmigrations(args: &[Bytes], ctx: &mut ServerContext) -> Value {
    if !args.is_empty() {
        return crate::framework::CommandError::wrong_arity("cluster|cancelslotmigrations");
    }
    let Some(state) = ctx.cluster_state_mut().as_deref_mut() else {
        return Value::error("ERR This instance has cluster support disabled");
    };
    if !state.migrations.has_active_exports() {
        return Value::error("ERR No migrations ongoing");
    }
    let now = crate::foundation::io::sys::time::unixtime();
    let actions = state.cancel_export_migrations(now);
    state.queue_migration_actions(actions);
    Value::ok()
}
