#[cfg(feature = "wasm")]
#[path = "bin/wasm/browser.rs"]
pub mod browser;

mod engine;
pub mod foundation;
mod framework;
mod runtime;
pub use runtime::driver;
#[cfg(feature = "wasm")]
pub use runtime::embedded;

cfg_if::cfg_if! {
    if #[cfg(not(feature = "wasm"))] {
        mod server;

        pub use engine::distributed::cluster::{
            AdvertisedEndpoint, ClientTransport, ClusterDebugSnapshot, ClusterEndpoints,
            ClusterHealth, ClusterNodeHealth, ClusterNodeRole, ClusterProjection,
            ClusterRouteDecision, ClusterTopologySnapshot, EndpointError, NodeId, NodeIdError,
            NodeView, PreferredEndpoint, RouteRequest, ShardView, SlotRange, SlotRangeError,
        };
        pub use foundation::memory::util_alloc;
        pub use server::run_from_env;
    }
}
