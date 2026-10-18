// The non-replication variants define the source contract for the next dispatch
// migrations; only replication replay is routed through it in this phase.
#[allow(dead_code)]
// TODO(dead_code): route client, AOF replay, and internal dispatch through CommandOrigin.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommandOrigin {
    Client,
    Replication,
    AofReplay,
    Internal,
}

#[derive(Clone)]
pub enum ReplicationApply {
    Select(u32),
    Ping,
    GetAck,
    Command(crate::engine::command::CommandArgs),
}
