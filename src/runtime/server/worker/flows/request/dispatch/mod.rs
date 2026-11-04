mod acl_stage;
mod command_exec;
mod completion;
mod dispatch_loop;
pub(crate) mod monitor;
mod postloop;
mod pre_dispatch;
mod runner;
mod script_helpers;
mod side_effects;
pub(crate) mod subcmd_flags;
mod tracking;
mod types;

#[cfg(test)]
mod tests;

pub(crate) use command_exec::TransactionPropagationInput;
pub(crate) use completion::redacted_argv;
pub(crate) use dispatch_loop::SpecialCommandCompletion;
pub(crate) use pre_dispatch::route_error_value;
pub(crate) use script_helpers::{is_eval_without_shebang, is_ro_script};
pub(crate) use side_effects::DeferredSlotMetric;
pub(crate) use side_effects::ReadBatchEffects;
pub(crate) use side_effects::{CommandEffects, EffectQueue, ExecCollectedEffects};
use types::PreflightRequest;
pub(crate) use types::{
    CmdCallFlags, CmdLogEntry, CmdPreconditions, CommandClientFacts, DispatchAction,
    DispatchOutcome, LoopAction, PostponeReason,
};

use super::super::ServerWorkerState;
use crate::engine::command::{CommandArgs, CMD_MAY_REPLICATE, CMD_WRITE};
use crate::foundation::io::protocol::Value;
use crate::framework::context::ServerContext;

pub(crate) use crate::engine::command::command_may_mutate_dataset;

impl ServerWorkerState {
    pub(crate) fn pre_dispatch_exec(
        &mut self,
        connection_token: usize,
        meta: CmdPreconditions,
        args: &CommandArgs,
    ) -> DispatchOutcome {
        let facts = CommandClientFacts::capture(
            self.connection_registry
                .get(crate::framework::capabilities::io::Token(connection_token))
                .expect("preflight target must still be registered"),
        );
        let client_asking = facts.asking;
        if consumes_asking(meta.name.as_str()) && !facts.transaction {
            self.connection_registry[facts.token].asking = false;
        }
        let call_flags = CmdCallFlags {
            is_write: command_may_mutate_dataset(meta.name.as_str(), meta.flags),
            client_asking,
        };
        let request = PreflightRequest {
            facts,
            meta: &meta,
            args,
            call_flags,
        };
        let outcome = self
            .preflight_protocol(&request)
            .or_else(|| self.preflight_resources(&request))
            .or_else(|| self.preflight_access(&request))
            // C processCommand routes remote slots before its loading gate:
            // a loading replica that no longer owns the slot must return MOVED.
            .or_else(|| self.preflight_routing(&request))
            .or_else(|| self.preflight_availability(&request))
            .or_else(|| self.preflight_write_safety(&request));
        if let Some(outcome) = outcome {
            return outcome;
        }
        if self.should_throttle_command(request.facts.token, meta.flags) {
            return DispatchOutcome {
                action: DispatchAction::Postpone(PostponeReason::ReplicationThrottle),
                call_flags,
                reject_reply: None,
            };
        }
        DispatchOutcome {
            action: DispatchAction::Execute,
            call_flags,
            reject_reply: None,
        }
    }

    pub(crate) fn should_throttle_command(
        &mut self,
        token: crate::framework::capabilities::io::Token,
        flags: u64,
    ) -> bool {
        if !self.ctx.config().replication.repl_throttling_enabled
            || self.ctx.replication().is_replica()
            || !self.ctx.replication().throttle.is_active()
            || self.connection_registry[token].throttle_checked
        {
            return false;
        }
        self.connection_registry[token].throttle_checked = true;
        let has_queued_clients = !self.ctx.throttled_clients().is_empty();
        flags & (CMD_WRITE | CMD_MAY_REPLICATE) != 0
            && self
                .ctx
                .replication_mut()
                .throttle
                .should_postpone(has_queued_clients)
    }

    fn preflight_protocol(&self, request: &PreflightRequest<'_>) -> Option<DispatchOutcome> {
        self.stage_sentinel_mode(
            &request.facts,
            request.meta,
            request.args,
            request.call_flags,
        )
        .or_else(|| {
            self.stage_arity(
                &request.facts,
                request.meta,
                request.args,
                request.call_flags,
            )
        })
    }

    fn preflight_resources(&self, request: &PreflightRequest<'_>) -> Option<DispatchOutcome> {
        self.stage_import_mode_oom(
            &request.facts,
            request.meta,
            request.args,
            request.call_flags,
        )
        .or_else(|| {
            self.stage_no_eviction_candidate_oom(
                &request.facts,
                request.meta,
                request.args,
                request.call_flags,
            )
        })
        .or_else(|| {
            self.stage_noeviction_oom(
                &request.facts,
                request.meta,
                request.args,
                request.call_flags,
            )
        })
    }

    fn preflight_availability(&self, request: &PreflightRequest<'_>) -> Option<DispatchOutcome> {
        self.stage_script_busy(
            &request.facts,
            request.meta,
            request.args,
            request.call_flags,
        )
        .or_else(|| {
            self.stage_loading(
                &request.facts,
                request.meta,
                request.args,
                request.call_flags,
            )
        })
    }

    fn preflight_access(&mut self, request: &PreflightRequest<'_>) -> Option<DispatchOutcome> {
        self.stage_acl_permissions(
            &request.facts,
            request.meta,
            request.args,
            request.call_flags,
        )
    }

    fn preflight_routing(&self, request: &PreflightRequest<'_>) -> Option<DispatchOutcome> {
        self.stage_cluster_redirect(
            &request.facts,
            request.meta,
            request.args,
            request.call_flags,
        )
        .or_else(|| {
            self.stage_standalone_replica_redirect(
                &request.facts,
                request.meta,
                request.args,
                request.call_flags,
            )
        })
        .or_else(|| {
            self.stage_slot_migration_pause(
                &request.facts,
                request.meta,
                request.args,
                request.call_flags,
            )
        })
    }

    fn preflight_write_safety(
        &mut self,
        request: &PreflightRequest<'_>,
    ) -> Option<DispatchOutcome> {
        self.stage_forkless_key_barrier(
            &request.facts,
            request.meta,
            request.args,
            request.call_flags,
        )
        .or_else(|| {
            self.stage_client_pause(
                &request.facts,
                request.meta,
                request.args,
                request.call_flags,
            )
        })
        .or_else(|| {
            self.stage_bgsave_error(
                &request.facts,
                request.meta,
                request.args,
                request.call_flags,
            )
        })
        .or_else(|| {
            self.stage_min_replicas(
                &request.facts,
                request.meta,
                request.args,
                request.call_flags,
            )
        })
        .or_else(|| {
            self.stage_readonly_replica(
                &request.facts,
                request.meta,
                request.args,
                request.call_flags,
            )
        })
        .or_else(|| {
            self.stage_stale_replica(
                &request.facts,
                request.meta,
                request.args,
                request.call_flags,
            )
        })
    }

    pub(crate) fn good_replicas_count(&self) -> i64 {
        let max_lag_sec = self.ctx.config().replication.min_replicas_max_lag;
        if max_lag_sec == 0 {
            return self.replication_runtime.live_replica_count() as i64;
        }
        self.replication_runtime.count_good_replicas(
            max_lag_sec.saturating_mul(1000),
            crate::foundation::io::sys::time::mstime(),
        ) as i64
    }
}

/// ASKING authorizes the next routed command. MULTI itself has no keys and
/// must leave that one-shot permission available to the command queued next.
pub(super) fn consumes_asking(command: &str) -> bool {
    !matches!(command, "asking" | "multi")
}

pub(crate) fn command_stats_name(name_lower: &str, args: &CommandArgs) -> String {
    let Some(subcommand) = args.args.first() else {
        return name_lower.to_string();
    };
    let full_name = format!(
        "{}|{}",
        name_lower,
        String::from_utf8_lossy(subcommand).to_ascii_lowercase()
    );
    if crate::framework::generated::commands::SUBCOMMAND_NAMES
        .iter()
        .any(|known| known.eq_ignore_ascii_case(&full_name))
    {
        full_name
    } else {
        name_lower.to_string()
    }
}

pub(crate) fn error_code_from_reply(reply: &Value) -> Option<String> {
    let Value::Error(err_bytes) = reply else {
        return None;
    };
    let err_str = String::from_utf8_lossy(err_bytes);
    Some(
        err_str
            .split_whitespace()
            .next()
            .unwrap_or("ERR")
            .to_string(),
    )
}

pub(crate) fn record_error_reply(ctx: &mut ServerContext, reply: &Value, failed_cmd: Option<&str>) {
    let Some(err_name) = error_code_from_reply(reply) else {
        return;
    };
    ctx.incr_error_reply(&err_name);
    if let Some(command) = failed_cmd {
        ctx.incr_cmd_failed(command);
    }
}

#[cfg(test)]
mod asking_tests {
    use super::consumes_asking;

    #[test]
    fn asking_survives_multi_until_a_routed_command_consumes_it() {
        assert!(!consumes_asking("asking"));
        assert!(!consumes_asking("multi"));
        assert!(consumes_asking("exists"));
    }
}
