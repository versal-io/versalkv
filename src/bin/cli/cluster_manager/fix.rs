use super::super::RespValue;
use super::ClusterManagerSession;

pub(super) fn execute(args: &[String], command_index: usize) -> Result<(), String> {
    let seed = args
        .get(command_index + 1)
        .ok_or_else(|| "missing cluster seed address".to_string())?;
    let (host, port) = super::parse_address(seed)?;
    let mut session = ClusterManagerSession::default();
    let response = session.send_command(&host, port, &[b"CLUSTER", b"NODES"])?;
    let nodes_text = match response {
        RespValue::Bulk(bytes) => String::from_utf8_lossy(&bytes).into_owned(),
        RespValue::Error(error) => return Err(error),
        other => return Err(format!("unexpected CLUSTER NODES response: {:?}", other)),
    };
    let mut nodes = super::parse_cluster_nodes(&nodes_text)?;
    for node in &mut nodes {
        let Ok(RespValue::Bulk(bytes)) =
            session.send_command(&node.host, node.port, &[b"CLUSTER", b"NODES"])
        else {
            continue;
        };
        let Ok(view) = super::parse_cluster_nodes(&String::from_utf8_lossy(&bytes)) else {
            continue;
        };
        if let Some(local) = view.into_iter().find(|node| node.is_myself) {
            node.migrating = local.migrating;
            node.importing = local.importing;
        }
    }
    let mut open_slots = nodes
        .iter()
        .flat_map(|node| node.migrating.iter().chain(&node.importing))
        .map(|(slot, _)| *slot)
        .collect::<Vec<_>>();
    open_slots.sort_unstable();
    open_slots.dedup();
    for slot in open_slots {
        fix_open_slot(&nodes, slot, &mut session)?;
    }
    Ok(())
}

fn fix_open_slot(
    nodes: &[super::ClusterNode],
    slot: u16,
    session: &mut ClusterManagerSession,
) -> Result<(), String> {
    let source = nodes.iter().find(|node| {
        node.is_primary
            && node
                .migrating
                .iter()
                .any(|(candidate, _)| *candidate == slot)
    });
    let target = nodes.iter().find(|node| {
        node.is_primary
            && node
                .importing
                .iter()
                .any(|(candidate, _)| *candidate == slot)
    });

    match (source, target) {
        (Some(source), Some(target)) => {
            let plan = super::RebalancePlan {
                source_id: source.id.clone(),
                source_host: source.host.clone(),
                source_port: source.port,
                target_id: target.id.clone(),
                target_host: target.host.clone(),
                target_port: target.port,
                slots: vec![slot],
            };
            super::move_slot_with_session(nodes, &plan, slot, session)
        }
        (Some(source), None) => send_stable(source, slot, session),
        (None, Some(target)) => {
            let owner = nodes
                .iter()
                .find(|node| node.is_primary && node.slots.contains(&slot));
            if owner.is_some_and(|owner| owner.id == target.id) {
                super::send_slot_owner_with_session(
                    session,
                    &target.host,
                    target.port,
                    slot,
                    &target.id,
                )
            } else {
                send_stable(target, slot, session)
            }
        }
        (None, None) => Ok(()),
    }
}

fn send_stable(
    node: &super::ClusterNode,
    slot: u16,
    session: &mut ClusterManagerSession,
) -> Result<(), String> {
    let slot = slot.to_string();
    session.send_ok(
        &node.host,
        node.port,
        &[b"CLUSTER", b"SETSLOT", slot.as_bytes(), b"STABLE"],
    )
}

#[cfg(test)]
mod tests {
    #[test]
    fn fix_reuses_discovery_connection_across_orphaned_slots() {
        use std::io::{Read, Write};
        use std::net::TcpListener;
        use std::sync::{
            atomic::{AtomicBool, Ordering},
            Arc,
        };
        use std::time::Duration;

        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let port = listener.local_addr().unwrap().port();
        let stopped = Arc::new(AtomicBool::new(false));
        let stop = Arc::clone(&stopped);
        let server = std::thread::spawn(move || {
            let mut workers = Vec::new();
            while !stop.load(Ordering::Relaxed) {
                match listener.accept() {
                    Ok((mut stream, _)) => workers.push(std::thread::spawn(move || {
                        stream.set_nonblocking(false).unwrap();
                        stream.set_read_timeout(Some(Duration::from_secs(2))).unwrap();
                        let mut parser = versal::foundation::io::protocol::parser::Parser::new();
                        let mut buffer = [0; 1024];
                        while let Ok(read) = stream.read(&mut buffer) {
                            if read == 0 { break; }
                            parser.feed(&buffer[..read]);
                            while let Some(command) = parser.try_parse().unwrap() {
                                if command[1].eq_ignore_ascii_case(b"NODES") {
                                    let view = format!("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa 127.0.0.1:{port}@17000 myself,master - 0 0 1 connected 42-43 [42->-bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb] [43->-bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb]\n");
                                    write!(stream, "${}\r\n{}\r\n", view.len(), view).unwrap();
                                } else {
                                    assert_eq!(command[1].as_ref(), b"SETSLOT");
                                    assert_eq!(command[3].as_ref(), b"STABLE");
                                    stream.write_all(b"+OK\r\n").unwrap();
                                }
                            }
                        }
                    })),
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => std::thread::yield_now(),
                    Err(error) => panic!("accept fixture connection: {error}"),
                }
            }
            let count = workers.len();
            for worker in workers {
                worker.join().unwrap();
            }
            count
        });
        let result = super::execute(
            &[
                "--cluster".into(),
                "fix".into(),
                format!("127.0.0.1:{port}"),
            ],
            1,
        );
        stopped.store(true, Ordering::Relaxed);
        let accepts = server.join().unwrap();
        result.unwrap();
        assert_eq!(
            accepts, 1,
            "fix must reuse one node connection for discovery and all slots"
        );
    }

    #[test]
    fn open_slots_are_discovered_once() {
        let nodes = super::super::parse_cluster_nodes(concat!(
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa 127.0.0.1:7000@17000 master - 0 0 1 connected 609 [609->-bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb]\n",
            "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb 127.0.0.1:7001@17001 myself,master - 0 0 2 connected [609-<-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa]\n",
        ))
        .unwrap();
        let mut slots = nodes
            .iter()
            .flat_map(|node| node.migrating.iter().chain(&node.importing))
            .map(|(slot, _)| *slot)
            .collect::<Vec<_>>();
        slots.sort_unstable();
        slots.dedup();
        assert_eq!(slots, vec![609]);
    }
}
