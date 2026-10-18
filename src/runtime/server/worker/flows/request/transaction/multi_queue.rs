use super::super::super::ServerWorkerState;
use crate::engine::command::{CommandArgs, CommandError};
use crate::foundation::io::protocol::serializer::{serialize_to_vec_resp2, serialize_to_vec_resp3};
use crate::foundation::io::protocol::Value;
use crate::framework::capabilities::io::Token;
use crate::framework::CommandSpec;
use crate::runtime::server::worker::flows::request::dispatch::{
    CmdPreconditions, DispatchAction, DispatchOutcome,
};
use bytes::Bytes;

#[derive(Clone, Copy)]
struct MultiQueueInput<'a> {
    token: Token,
    args: &'a CommandArgs,
    name: &'a str,
    resp3: bool,
}

enum MultiQueueDecision {
    Queue,
    Reply(Value),
    Dispatch(DispatchOutcome),
}

impl ServerWorkerState {
    pub(crate) fn try_handle_multi_queue(
        &mut self,
        token: Token,
        args: &CommandArgs,
        name: &str,
        resp3: bool,
        replies: &mut Vec<Vec<u8>>,
    ) -> Option<bool> {
        if !self.should_queue_in_transaction(token, name) {
            return None;
        }
        let input = MultiQueueInput {
            token,
            args,
            name,
            resp3,
        };
        let rejected = match self.decide_multi_queue(&input) {
            MultiQueueDecision::Queue => {
                let conn = &mut self.connection_registry[token];
                conn.transaction.queue(args.clone());
                self.finish_multi_queue_reply(
                    input,
                    replies,
                    Value::SimpleString(Bytes::from_static(b"QUEUED")),
                    false,
                )
            }
            MultiQueueDecision::Reply(reply) => {
                self.finish_multi_queue_reply(input, replies, reply, true)
            }
            MultiQueueDecision::Dispatch(outcome) => {
                self.finish_multi_dispatch_outcome(input, replies, outcome)
            }
        };
        Some(rejected)
    }

    fn should_queue_in_transaction(&self, token: Token, name: &str) -> bool {
        self.connection_registry[token].transaction.is_active()
            && !matches!(name, "multi" | "discard" | "reset" | "watch" | "unwatch")
    }

    fn decide_multi_queue(&mut self, input: &MultiQueueInput<'_>) -> MultiQueueDecision {
        let Some(spec) = self.registry.lookup(&input.args.name).cloned() else {
            return MultiQueueDecision::Reply(CommandError::unknown(&input.args.name));
        };
        let meta = self.multi_queue_preconditions(&spec, input.args);
        // C processCommand checks arity, CMD_NO_MULTI, and ACL before any
        // redirect. pre_dispatch_exec owns arity/ACL and the later gates, but
        // CMD_NO_MULTI remains a transaction-only check between the two.
        let facts =
            crate::runtime::server::worker::flows::request::dispatch::CommandClientFacts::capture(
                &self.connection_registry[input.token],
            );
        if let Some(outcome) = self.stage_arity(
            &facts,
            &meta,
            input.args,
            crate::runtime::server::worker::flows::request::dispatch::CmdCallFlags::default(),
        ) {
            return MultiQueueDecision::Dispatch(outcome);
        }
        let command_name = super::super::dispatch::command_stats_name(input.name, input.args);
        if let Some(reply) = Self::check_no_multi_pure(meta, input.name, &command_name) {
            return MultiQueueDecision::Reply(reply);
        }
        let outcome = self.pre_dispatch_exec(input.token.0, meta, input.args);
        if outcome.action == DispatchAction::Execute {
            MultiQueueDecision::Queue
        } else {
            MultiQueueDecision::Dispatch(outcome)
        }
    }

    fn multi_queue_preconditions(
        &self,
        spec: &CommandSpec,
        args: &CommandArgs,
    ) -> CmdPreconditions {
        CmdPreconditions {
            name: spec.name,
            arity: spec.arity,
            flags: super::super::dispatch::subcmd_flags::effective_command_flags_for_call(
                spec.name, spec.flags, args, &self.ctx,
            ),
            first_key: spec.first_key,
            last_key: spec.last_key,
            key_step: spec.key_step,
            key_flags: spec.key_flags,
            acl_categories: super::super::dispatch::subcmd_flags::effective_acl_categories(
                spec.name,
                spec.acl_categories,
                args.args.first().map(|arg| arg.as_ref()),
            ),
            get_keys: spec.get_keys,
        }
    }

    fn finish_multi_dispatch_outcome(
        &mut self,
        input: MultiQueueInput<'_>,
        replies: &mut Vec<Vec<u8>>,
        outcome: DispatchOutcome,
    ) -> bool {
        match outcome.action {
            DispatchAction::Rejected => self.finish_multi_queue_reply(
                input,
                replies,
                outcome.reject_reply.unwrap_or_else(Value::null),
                true,
            ),
            DispatchAction::Postpone(_) => {
                let conn = &mut self.connection_registry[input.token];
                conn.postpone_transaction_command(input.args);
                self.ctx.paused_clients_mut().insert(input.token);
                if let Some(info) = self.ctx.client_info_mut(input.token.0) {
                    info.flags.retain(|flag| flag != 'N');
                    if !info.flags.contains('b') {
                        info.flags.push('b');
                    }
                }
                conn.cmd_count = conn.cmd_count.saturating_sub(1);
                false
            }
            DispatchAction::Execute => false,
        }
    }

    fn finish_multi_queue_reply(
        &mut self,
        input: MultiQueueInput<'_>,
        replies: &mut Vec<Vec<u8>>,
        reply: Value,
        rejected: bool,
    ) -> bool {
        if rejected {
            self.connection_registry[input.token]
                .transaction
                .mark_queue_error();
            self.ctx.end_cmd(input.name, true);
            crate::runtime::server::worker::flows::request::dispatch::record_error_reply(
                &mut self.ctx,
                &reply,
                None,
            );
        }
        let selected_db = self.connection_registry[input.token].selected_db;
        if let Some(info) = self.ctx.client_info_mut(input.token.0) {
            info.last_cmd = input.name.to_string();
            info.db = selected_db;
        }
        let conn = &mut self.connection_registry[input.token];
        if !conn.has_custom_reply() {
            replies.push(if input.resp3 {
                serialize_to_vec_resp3(&reply)
            } else {
                serialize_to_vec_resp2(&reply)
            });
        }
        rejected
    }
}
