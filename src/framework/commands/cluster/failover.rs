use crate::foundation::io::protocol::Value;
use crate::framework::commands::cluster::cluster::with_cluster_state;
use crate::framework::context::ServerContext;
use crate::framework::CommandError;
use bytes::Bytes;

pub(crate) fn cluster_failover(
    args: &[Bytes],
    is_replication_origin: bool,
    ctx: &mut ServerContext,
) -> Value {
    use crate::engine::distributed::cluster::state::failover::ManualFailoverMode;

    let mut mode = ManualFailoverMode::Coordinated;
    let mut target_replica = None;
    let mut index = 0;
    while index < args.len() {
        if args[index].eq_ignore_ascii_case(b"force") {
            mode = ManualFailoverMode::Force;
        } else if args[index].eq_ignore_ascii_case(b"takeover") {
            mode = ManualFailoverMode::Takeover;
        } else if is_replication_origin && args[index].eq_ignore_ascii_case(b"replicaid") {
            let Some(node_id) = args.get(index + 1) else {
                return CommandError::syntax_error();
            };
            target_replica = Some(node_id.as_ref());
            index += 1;
        } else {
            return CommandError::syntax_error();
        }
        index += 1;
    }

    if let Some(target_replica) = target_replica {
        let local_matches = ctx.cluster_state().as_ref().is_some_and(|state| {
            state.myself.name.as_bytes() == target_replica
        });
        if !local_matches {
            return Value::ok();
        }
    }
    let now = crate::foundation::io::sys::time::mstime();
    let timeout_ms = ctx.config().server.cluster_manual_failover_timeout;
    match with_cluster_state(ctx)
        .and_then(|state| state.start_manual_failover(mode, now, timeout_ms).map_err(Value::error))
    {
        Ok(()) => {
            match mode {
                ManualFailoverMode::Force if target_replica.is_some() => crate::server_log!(
                    notice,
                    "Forced failover primary request accepted."
                ),
                ManualFailoverMode::Force => crate::server_log!(
                    notice,
                    "Forced failover user request accepted."
                ),
                ManualFailoverMode::Takeover => {
                    crate::server_log!(notice, "Taking over the primary (user request).")
                }
                ManualFailoverMode::Coordinated => {
                    crate::server_log!(notice, "Manual failover user request accepted.")
                }
            }
            Value::ok()
        }
        Err(reply) => reply,
    }
}

#[cfg(test)]
mod tests {
    use super::cluster_failover;
    use crate::engine::distributed::cluster::ClusterState;
    use crate::foundation::io::protocol::Value;
    use crate::framework::context::ServerContext;
    use bytes::Bytes;

    const LOCAL_ID: &[u8] = b"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    const OTHER_ID: &[u8] = b"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";

    fn args(parts: &[&'static [u8]]) -> Vec<Bytes> {
        parts
            .iter()
            .map(|part| Bytes::from_static(part))
            .collect()
    }

    fn context_with_local_id() -> ServerContext {
        let mut context = ServerContext::new(1);
        let mut state = ClusterState::new("127.0.0.1", 6379, 16_379);
        state.myself.name = String::from_utf8(LOCAL_ID.to_vec()).unwrap();
        *context.cluster_state_mut() = Some(Box::new(state));
        context
    }

    #[test]
    fn client_cannot_use_internal_replicaid_option() {
        let mut context = context_with_local_id();

        assert_eq!(
            cluster_failover(
                &args(&[b"FORCE", b"REPLICAID", LOCAL_ID]),
                false,
                &mut context,
            ),
            crate::framework::CommandError::syntax_error()
        );
    }

    #[test]
    fn replication_origin_ignores_non_target_replica_before_preconditions() {
        let mut context = context_with_local_id();

        assert_eq!(
            cluster_failover(
                &args(&[b"FORCE", b"REPLICAID", OTHER_ID]),
                true,
                &mut context,
            ),
            Value::ok()
        );
    }
}
