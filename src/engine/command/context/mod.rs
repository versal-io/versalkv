mod conn;

use crate::engine::command::CommandArgs;
use bytes::Bytes;

pub use conn::{CommandConn, WatchedKey};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Token(pub usize);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectionState {
    Connected,
    Closing,
    CloseAfterReply,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BlockedCmd {
    Blpop,
    Brpop,
    Brpoplpush {
        dst_key: Bytes,
    },
    Blmove {
        dst_key: Bytes,
        wherefrom: bool,
        whereto: bool,
    },
    Blmpop {
        direction: bool,
        count: usize,
    },
    Bzpopmin,
    Bzpopmax,
    Bzmpop {
        is_min: bool,
        count: usize,
    },
    Xread,
    Xreadgroup,
    Wait {
        required: i64,
        target_offset: crate::engine::distributed::replication::ReplicationOffset,
    },
    WaitAof {
        numlocal: i64,
        numreplicas: i64,
        target_offset: crate::engine::distributed::replication::ReplicationOffset,
    },
    /// `CLUSTER SETSLOT` waiting for enough online replicas (C
    /// `BLOCKED_WAIT` installed by `blockClientForReplicaAck` from
    /// `clusterCommandSetSlot`). The command body runs once when the ACK
    /// threshold is reached; the timeout path replies `NOREPLICAS` without
    /// re-running the body.
    SetSlot {
        required: i64,
        target_offset: crate::engine::distributed::replication::ReplicationOffset,
    },
    Paused,
    Forkless,
    Throttled,
    SlotMigration(crate::engine::distributed::cluster::migration::MigrationJobId),
    Shutdown,
    #[allow(dead_code)]
    // TODO(dead_code): construct this when timed-out script clients enter the unified blocked registry.
    Script,
}

#[derive(Clone)]
pub struct BlockedState {
    pub cmd: BlockedCmd,
    pub keys: Vec<Bytes>,
    pub timeout_ms: u64,
    pub start_time_ms: i64,
    pub orig_args: CommandArgs,
    pub req_bytes: usize,
    /// When a blocking command (BLPOP/BRPOP/XREAD/...) is wrapped by the outer
    /// CLIENT PAUSE postpone layer, the original block info is saved here so
    /// CLIENT UNBLOCK / UNPAUSE semantics can reason about the underlying
    /// timeout-able block even while the postpone wrapper is still in place.
    /// Aligns with C Valkey's separation between BLOCKED_POSTPONE (outer
    /// postponement in processCommand) and the real BLOCKED_LIST/ZSET/STREAM
    /// (btype set inside the command handler after blockForKeys).
    pub orig_wrapped: Option<Box<BlockedState>>,
}
