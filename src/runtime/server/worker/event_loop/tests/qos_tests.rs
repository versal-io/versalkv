use super::*;
use crate::engine::extension::config::server::ProtectedAction;
use crate::runtime::server::worker::event_loop::transport::RuntimeEvent;
use crate::runtime::server::worker::event_loop::{
    EventLane, EventLoopEvent, EventPhase, ReadinessEvent, ReadinessOrigin,
};
use std::io::{BufRead, BufReader};

fn connect_client(runtime: &mut ServerWorkerState) -> (Token, TcpStream) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind QoS test peer");
    let address = listener.local_addr().expect("QoS test peer address");
    let peer = runtime
        .transport
        .connect_tcp(address)
        .expect("connect QoS test transport");
    let (client, _) = listener.accept().expect("accept QoS test transport");
    client
        .set_read_timeout(Some(Duration::from_secs(1)))
        .expect("set QoS reply timeout");
    runtime.add_connection(RuntimeConnectedPeer {
        token: peer.token,
        peer_addr: address,
    });
    (peer.token, client)
}

#[test]
fn priority_arriving_during_normal_batch_obeys_preemption_setting() {
    for interval_us in [0, 1] {
        let mut runtime = ServerWorkerState::new(1).expect("create QoS test runtime");
        crate::framework::commands::register_all(&mut runtime.registry);
        runtime.ctx.config_mut().server.enable_debug_command = ProtectedAction::Yes;
        runtime
            .ctx
            .config_mut()
            .server
            .priority_preemptive_poll_interval_us = interval_us;
        let (slow_token, mut slow_client) = connect_client(&mut runtime);
        let (query_token, mut query_client) = connect_client(&mut runtime);
        let (priority_token, mut priority_client) = connect_client(&mut runtime);
        runtime.mark_client_subnet_priority(priority_token);
        slow_client
            .write_all(b"DEBUG SLEEP 0.01\r\n")
            .expect("send ordinary slow command");
        query_client
            .write_all(b"GET order\r\n")
            .expect("send subsequent ordinary query");
        priority_client
            .write_all(b"SET order priority\r\n")
            .expect("send newly arriving priority write");
        for token in [slow_token, query_token, priority_token] {
            runtime.transport.wait_readable_for_test(token);
        }
        runtime.transport.queue_test_transport_event_batch(&[(
            priority_token,
            true,
            false,
            false,
            false,
        )]);
        runtime.dispatch_ready_batch(
            [slow_token, query_token]
                .into_iter()
                .map(|token| RuntimeEvent {
                    token,
                    readable: true,
                    writable: false,
                    read_hup: false,
                    error: false,
                })
                .collect(),
        );
        let mut reply = String::new();
        BufReader::new(query_client)
            .read_line(&mut reply)
            .expect("read ordinary query result");
        assert_eq!(
            reply,
            if interval_us == 0 {
                "$-1\r\n"
            } else {
                "$8\r\n"
            },
            "priority preemption interval {interval_us}"
        );
        if interval_us > 0 {
            assert!(runtime.ctx.stats().eventloop_priority_cycles > 0);
            assert!(runtime.ctx.stats().eventloop_priority_cmd_per_cycle_max > 0);
        }
    }
}

#[test]
fn priority_events_remain_in_normal_lane_after_config_change() {
    // With dual-epoll, lane reclassification is no longer needed:
    // preempted priority events are dispatched immediately via
    // dispatch_readiness_now instead of being enqueued. Events
    // always remain in their original lane.
    for is_next_turn in [false, true] {
        for mark_priority in [false, true] {
            let mut runtime = ServerWorkerState::new(1).expect("create classification runtime");
            let (token, _client) = connect_client(&mut runtime);
            runtime.event_batch.push(EventLoopEvent::readiness(
                EventPhase::Ready,
                EventLane::Normal,
                ReadinessEvent::readable(token, ReadinessOrigin::Polled),
            ));
            if is_next_turn {
                runtime.event_batch.finish_phase(EventPhase::Ready);
            }
            if mark_priority {
                runtime.mark_client_subnet_priority(token);
            } else {
                runtime.ctx.config_mut().server.priority_subnets_generation += 1;
                runtime.scan_and_apply_priority_subnets();
            }
            if is_next_turn {
                assert!(runtime.event_batch.pop_ready().is_none());
                runtime.event_batch.begin_next_turn();
            }
            let event = runtime
                .event_batch
                .pop_ready()
                .expect("classified readiness");
            assert_eq!(event.sequence, 1);
            assert_eq!(event.phase, EventPhase::Ready);
            assert_eq!(
                event.lane,
                EventLane::Normal,
                "next turn {is_next_turn}, mark_priority {mark_priority}"
            );
            assert!(runtime.event_batch.pop_ready().is_none());
        }
    }
}
