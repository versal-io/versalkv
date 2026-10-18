#[path = "failover/automatic.rs"]
mod automatic;
#[path = "failover/eligibility.rs"]
mod eligibility;
#[path = "failover/manual.rs"]
mod manual;
#[path = "failover/mod.rs"]
mod operations;

pub use automatic::{
    AutomaticFailover, AutomaticFailoverFacts, AutomaticFailoverPhase, FailedPrimaryFacts,
    LocalReplicaFailoverStatus, VoteRequestFacts,
};
pub(crate) use eligibility::ReplicaRankKey;
pub use eligibility::{
    evaluate_failure, failover_election_delay_ms, rank_replicas, FailureDecision, FailureFacts,
    FailureReport, ReplicaEligibility,
};
pub use manual::{ManualFailover, ManualFailoverMode};

#[cfg(test)]
#[path = "failover/tests.rs"]
mod tests;
