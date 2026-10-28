use crate::common::{Client, TestServer};
use std::time::Duration;

fn config(client: &mut Client, name: &[u8], value: &[u8]) {
    assert_eq!(
        client.command(&[b"CONFIG", b"SET", name, value]),
        b"+OK\r\n"
    );
}

struct DualSync {
    primary: TestServer,
    replica: TestServer,
    control: Client,
    receiver: Client,
    is_disk_backed: bool,
}

impl DualSync {
    fn start(load_mode: &str) -> Self {
        let primary = TestServer::start(&["--save", ""]);
        let replica = TestServer::start(&["--save", "", "--repl-diskless-load", load_mode]);
        let mut control = primary.connect();
        config(&mut control, b"repl-diskless-sync", b"yes");
        config(&mut control, b"repl-diskless-sync-delay", b"0");
        config(&mut control, b"dual-channel-replication-enabled", b"yes");
        assert_eq!(
            control.command(&[b"DEBUG", b"POPULATE", b"1000", b"seed", b"1024"]),
            b"+OK\r\n"
        );
        config(&mut control, b"rdb-key-save-delay", b"1000");
        let mut receiver = replica.connect();
        config(&mut receiver, b"dual-channel-replication-enabled", b"yes");
        config(&mut receiver, b"propagation-error-behavior", b"panic");
        config(
            &mut receiver,
            b"loading-process-events-interval-bytes",
            b"1024",
        );
        Self {
            primary,
            replica,
            control,
            receiver,
            is_disk_backed: load_mode == "disabled",
        }
    }

    fn begin_transfer(&mut self) {
        assert_eq!(
            self.receiver.command(&[
                b"REPLICAOF",
                b"127.0.0.1",
                self.primary.port.to_string().as_bytes()
            ]),
            b"+OK\r\n"
        );
        let start_notice = if self.is_disk_backed {
            "receiving streamed RDB from primary for persistence to disk"
        } else {
            "Loading RDB produced"
        };
        assert!(
            super::wait_until(
                || self.replica.server_log().contains(start_notice),
                Duration::from_secs(10)
            ),
            "replica never started the expected RDB phase: {}",
            self.replica.server_log()
        );
        // Ensure this tests the paired protocol, including successful main
        // PSYNC before the snapshot finishes, rather than classic fallback.
        let info = self.control.command_str(&[b"INFO", b"replication"]);
        assert!(
            info.contains("type=rdb-channel") && info.contains("type=main-channel"),
            "not a dual-channel transfer: {info}"
        );
    }

    fn interrupt(&mut self, close_rdb: bool) {
        let clients = self.control.command_str(&[b"CLIENT", b"LIST"]);
        let command = if close_rdb { "cmd=sync" } else { "cmd=psync" };
        let id = clients
            .lines()
            .find(|line| line.split_whitespace().any(|field| field == command))
            .and_then(|line| {
                line.split_whitespace()
                    .find_map(|field| field.strip_prefix("id="))
            })
            .unwrap_or_else(|| panic!("missing {command} channel: {clients}"));
        assert_eq!(
            self.control.command(&[b"SET", b"after-snapshot", b"tail"]),
            b"+OK\r\n"
        );
        config(&mut self.control, b"rdb-key-save-delay", b"0");
        assert_eq!(
            self.control
                .command(&[b"CLIENT", b"KILL", b"ID", id.as_bytes()]),
            b":1\r\n"
        );
        assert!(
            super::wait_until(
                || self.replica.server_log().contains("Failed trying to load"),
                Duration::from_secs(5)
            ),
            "transfer did not abort: {}",
            self.replica.server_log()
        );
    }

    fn assert_recovered(&mut self) {
        assert!(
            super::wait_until(
                || self
                    .control
                    .command_str(&[b"INFO", b"replication"])
                    .contains("state=online"),
                Duration::from_secs(10)
            ),
            "replica did not recover: {}",
            self.replica.server_log()
        );
        let stats = self.control.command_str(&[b"INFO", b"stats"]);
        assert!(
            super::extract_field(&stats, "sync_full")
                .parse::<u64>()
                .unwrap()
                >= 2,
            "interrupted snapshot must cause another full sync: {stats}; logs: {}",
            self.replica.server_log()
        );
        assert_eq!(self.receiver.command(&[b"DBSIZE"]), b":1001\r\n");
        assert_eq!(
            self.receiver.command(&[b"DEBUG", b"DIGEST"]),
            self.control.command(&[b"DEBUG", b"DIGEST"])
        );
        assert_eq!(
            self.control.command(&[b"SET", b"after-recovery", b"ok"]),
            b"+OK\r\n"
        );
        assert!(super::wait_until(
            || self.receiver.command(&[b"GET", b"after-recovery"]) == b"$2\r\nok\r\n",
            Duration::from_secs(5)
        ));
    }
}

fn assert_interrupted_load_recovers(load_mode: &str, close_rdb: bool) {
    let mut sync = DualSync::start(load_mode);
    sync.begin_transfer();
    sync.interrupt(close_rdb);
    sync.assert_recovered();
}

#[test]
fn main_disconnect_during_dual_rdb_load_restarts_full_sync() {
    for mode in ["flush-before-load", "swapdb", "disabled"] {
        assert_interrupted_load_recovers(mode, false);
    }
}

#[test]
fn rdb_disconnect_during_dual_rdb_load_restarts_full_sync() {
    for mode in ["flush-before-load", "swapdb", "disabled"] {
        assert_interrupted_load_recovers(mode, true);
    }
}
