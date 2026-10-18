pub type CmdFlags = u64;

pub const CMD_WRITE: CmdFlags = 1 << 0;
pub const CMD_DENYOOM: CmdFlags = 1 << 1;
pub const CMD_FAST: CmdFlags = 1 << 2;
pub const CMD_READONLY: CmdFlags = 1 << 3;
pub const CMD_ADMIN: CmdFlags = 1 << 6;
pub const CMD_LOADING: CmdFlags = 1 << 7;
pub const CMD_STALE: CmdFlags = 1 << 8;
pub const CMD_NOSCRIPT: CmdFlags = 1 << 9;
pub const CMD_NO_AUTH: CmdFlags = 1 << 10;
pub const CMD_SKIP_MONITOR: CmdFlags = 1 << 11;
pub const CMD_NO_ASYNC_LOADING: CmdFlags = 1 << 12;
pub const CMD_PUBSUB: CmdFlags = 1 << 13;
pub const CMD_SLOW: CmdFlags = 1 << 14;
pub const CMD_MOVABLE_KEYS: CmdFlags = 1 << 15;
pub const CMD_BLOCKING: CmdFlags = 1 << 16;
pub const CMD_NO_MULTI: CmdFlags = 1 << 17;
pub const CMD_ALLOW_BUSY: CmdFlags = 1 << 18;
pub const CMD_ALL_DBS: CmdFlags = 1 << 19;
pub const CMD_ASKING: CmdFlags = 1 << 20;
pub const CMD_MAY_REPLICATE: CmdFlags = 1 << 21;
pub const CMD_NO_MANDATORY_KEYS: CmdFlags = 1 << 22;
pub const CMD_ONLY_SENTINEL: CmdFlags = 1 << 23;
pub const CMD_PROTECTED: CmdFlags = 1 << 24;
pub const CMD_SENTINEL: CmdFlags = 1 << 25;
pub const CMD_SKIP_COMMANDLOG: CmdFlags = 1 << 26;
pub const CMD_TOUCHES_ARBITRARY_KEYS: CmdFlags = 1 << 27;
pub const CMD_SKIP_SLOWLOG: CmdFlags = 1 << 28;
pub const CMD_MODULE: CmdFlags = 1 << 29;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum DispatchGroup {
    Default = 0,
    Transport,
    PubSubControl,
    Transaction,
    ClientAdmin,
    Auth,
    Shutdown,
}
