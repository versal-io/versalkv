pub mod config;
pub mod endpoint;
pub mod event;
pub mod failover;
pub mod hello;
pub mod script;

mod state;

pub use endpoint::{format_sentinel_endpoint, split_sentinel_endpoint};
pub(crate) use state::SentinelRegistry;
pub use state::{
    DownState, FailoverState, InstanceRole, SentinelInstance, SentinelInstanceId,
    SentinelPeerMasterState, SentinelState, MONITORED_INSTANCE_FAILOVER,
    MONITORED_INSTANCE_FAILOVER_NS, MONITORED_INSTANCE_NO_FAILOVER, SRI_COORD_FAILOVER,
    SRI_FAILOVER_IN_PROGRESS, SRI_FORCE_FAILOVER, SRI_PRIMARY_REBOOT,
};
