use crate::common::{Client, TestServer};
use crate::replication::common::wait_until;
use std::sync::{Arc, Barrier};
use std::time::Duration;

#[test]
fn concurrent_full_sync_under_write_load_converges() {
    let master = TestServer::start(&["--save", ""]);
    let mut primary = master.connect();
    for i in 0..2_000u32 {
        let key = format!("p0:seed:{i}");
        assert_eq!(
            primary.command(&[b"SET", key.as_bytes(), b"seed-value"]),
            b"+OK\x0d\x0a"
        );
    }

    let replicas = [
        TestServer::start(&["--save", ""]),
        TestServer::start(&["--save", ""]),
        TestServer::start(&["--save", ""]),
    ];
    let port = master.port.to_string();
    let start_writes = Arc::new(Barrier::new(2));
    let writer_barrier = Arc::clone(&start_writes);
    let primary_port = master.port;
    let writer = std::thread::spawn(move || {
        writer_barrier.wait();
        let mut client = Client::connect_to(primary_port);
        for i in 0..20_000u32 {
            let key = format!("p0:write:{i}");
            assert_eq!(
                client.command(&[b"SET", key.as_bytes(), b"value"]),
                b"+OK\x0d\x0a"
            );
        }
    });

    for replica in &replicas {
        assert_eq!(
            replica
                .connect()
                .command(&[b"REPLICAOF", b"127.0.0.1", port.as_bytes(),]),
            b"+OK\x0d\x0a"
        );
    }
    start_writes.wait();

    assert!(
        wait_until(
            || {
                master
                    .connect()
                    .command_str(&[b"INFO", b"replication"])
                    .matches("state=online")
                    .count()
                    == 3
            },
            Duration::from_secs(60),
        ),
        "all replicas must reach online while writes continue"
    );
    writer.join().unwrap();

    let digest = master.connect().command_str(&[b"DEBUG", b"DIGEST"]);
    for replica in &replicas {
        assert!(
            wait_until(
                || replica.connect().command_str(&[b"DEBUG", b"DIGEST"]) == digest,
                Duration::from_secs(30),
            ),
            "replica digest did not converge"
        );
    }
}
