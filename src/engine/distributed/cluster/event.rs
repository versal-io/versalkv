/// A domain event consumed by the Cluster reducer.
/// Events are grouped by intent: topology broadcast, link lifecycle, and
/// ordered pending-operation drains. Bus input, timer ticks, and migration
/// progress remain future inputs until they have real reducer consumers.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ClusterEvent {
    /// Drain a coalesced request to promptly announce local topology changes.
    BroadcastTopology,
    /// Drain deferred Cluster Bus link-close requests into explicit actions.
    DrainLinkCloses,
    /// Drain deferred Cluster FORGET requests into explicit actions.
    DrainPendingForgets,
    /// Drain deferred Cluster MEET requests into explicit actions.
    DrainPendingMeets,
    /// Drain deferred outgoing Cluster Bus connection requests.
    DrainPendingConnects {
        resolved_endpoints: Vec<ResolvedConnectEndpoint>,
    },
}

/// A DNS-resolved, token-free endpoint fact supplied by the L5 runtime.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ResolvedConnectEndpoint {
    pub host: String,
    pub bus_port: u16,
    pub resolved_ip: String,
}
