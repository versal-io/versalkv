use crate::engine::distributed::sentinel::hello::{apply_hello, HelloMessage};
use crate::framework::commands::sentinel::admin_cmds::flushconfig_internal;
use crate::framework::context::ServerContext;

mod config;
pub mod io;
pub mod link;
pub mod timer;
mod transport;

pub(crate) use config::load_config_directives;

pub fn apply_hello_and_flush_config(ctx: &mut ServerContext, hello: &HelloMessage, now: i64) {
    let before = ctx
        .sentinel_state()
        .registry
        .master(&hello.master_name)
        .map(|master| (master.addr.clone(), master.config_epoch));
    apply_hello(ctx.sentinel_state_mut(), hello, now);
    let after = ctx
        .sentinel_state()
        .registry
        .master(&hello.master_name)
        .map(|master| (master.addr.clone(), master.config_epoch));
    if before != after && flushconfig_internal(ctx.sentinel_state_mut()).is_err() {
        crate::server_log!(
            warning,
            "SENTINEL: failed to flush config after hello update"
        );
    }
}
