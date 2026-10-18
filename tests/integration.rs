#![forbid(unsafe_code)]

#[path = "integration/common/mod.rs"]
mod common;

#[path = "integration/protocol.rs"]
mod protocol;

#[path = "integration/connection.rs"]
mod connection;

#[path = "integration/acl.rs"]
mod acl;

#[path = "integration/wakeup_transients.rs"]
mod wakeup_transients;

#[path = "integration/keyspace.rs"]
mod keyspace;

#[path = "integration/expire.rs"]
mod expire;

#[path = "integration/admin.rs"]
mod admin;

#[path = "integration/config_compat.rs"]
mod config_compat;

#[path = "integration/cluster.rs"]
mod cluster;

#[path = "integration/debug.rs"]
mod debug;

#[path = "integration/debug_digest_cross.rs"]
mod debug_digest_cross;

#[path = "integration/benchmark_memtier.rs"]
mod benchmark_memtier;

#[path = "integration/versal_cli.rs"]
mod versal_cli;

#[path = "integration/datastructures.rs"]
mod datastructures;

#[path = "integration/dict_rehash.rs"]
mod dict_rehash;

#[path = "integration/pubsub.rs"]
mod pubsub;

#[path = "integration/dump.rs"]
mod dump;

#[path = "integration/migrate.rs"]
mod migrate;

#[path = "integration/persistence.rs"]
mod persistence;

#[path = "integration/replication/mod.rs"]
mod replication;

#[path = "integration/replication_filter_only.rs"]
mod replication_filter_only;

#[path = "integration/sentinel.rs"]
mod sentinel;

#[path = "integration/scripting.rs"]
mod scripting;

#[path = "integration/shutdown.rs"]
mod shutdown;

#[path = "integration/startup_loading.rs"]
mod startup_loading;

#[path = "integration/module_ffi.rs"]
mod module_ffi;

#[path = "integration/mt_io.rs"]
mod mt_io;

#[path = "integration/polling_backend.rs"]
mod polling_backend;

#[path = "integration/zset_perf.rs"]
mod zset_perf;

#[path = "integration/harness.rs"]
mod harness;

// #[path = "integration/zset_resp_test.rs"]
// mod zset_resp_test;
