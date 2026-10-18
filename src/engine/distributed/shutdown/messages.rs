//! Byte-exact strings the Tcl suite asserts on. Do NOT edit without updating
//! `tests/unit/shutdown.tcl` expectations in Valkey upstream.

pub const MANUALLY_ABORTED: &str = "Shutdown manually aborted.";
pub const READY_TO_EXIT: &str = "Valkey is now ready to exit, bye bye...";
pub const ERR_SHUTDOWN: &str = "Errors trying to SHUTDOWN. Check logs.";
pub const ERR_SAVE_CANT_EXIT: &str = "Error trying to save the DB, can't exit.";
pub const ERR_AOF_INIT_CANT_EXIT: &str = "Writing initial AOF, can't exit.";
pub const USER_REQUESTED: &str = "User requested shutdown...";
pub const YOU_INSIST: &str = "You insist... exiting now.";
