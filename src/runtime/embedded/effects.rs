use crate::engine::command::SessionId;
use crate::engine::core::{CommandEffects, Value};
use crate::runtime::driver::{CommandEffectSink, CommandTransition, RuntimeCapabilities};

/// Result of committing a command in the embedded runtime: the client reply
/// plus, when the command completed normally, the effects needed to decide
/// durable persistence.
pub(super) struct EmbeddedCommit {
    pub(super) reply: Value,
    /// Present only for a normally completed command whose reply should be
    /// sent to the client. `None` when the reply is itself an error produced
    /// by the sink (missing capability) or when no effects apply.
    pub(super) effects: Option<CommandEffects>,
}

impl EmbeddedCommit {
    fn reply_only(reply: Value) -> Self {
        Self {
            reply,
            effects: None,
        }
    }
}

pub(super) struct EmbeddedEffectSink {
    capabilities: RuntimeCapabilities,
}

impl EmbeddedEffectSink {
    pub(super) const fn new(capabilities: RuntimeCapabilities) -> Self {
        Self { capabilities }
    }
}

impl CommandEffectSink for EmbeddedEffectSink {
    type Output = EmbeddedCommit;

    fn commit(&mut self, _session: SessionId, transition: CommandTransition) -> Self::Output {
        let outcome = match transition {
            CommandTransition::Complete(outcome) => outcome,
            CommandTransition::Suspend(_) if !self.capabilities.blocking => {
                return EmbeddedCommit::reply_only(Value::error(
                    "ERR command requires a blocking capability",
                ));
            }
            CommandTransition::Suspend(_) => {
                return EmbeddedCommit::reply_only(Value::error(
                    "ERR embedded blocking continuation was not registered",
                ));
            }
            CommandTransition::Close(_) if !self.capabilities.connections => {
                return EmbeddedCommit::reply_only(Value::error(
                    "ERR command requires a connection lifecycle capability",
                ));
            }
            CommandTransition::Close(outcome) => outcome,
        };
        if outcome.effects.suppress_reply || outcome.effects.close_connection {
            return EmbeddedCommit::reply_only(Value::error(
                "ERR command requires a connection lifecycle capability",
            ));
        }
        EmbeddedCommit {
            reply: outcome.reply,
            effects: Some(outcome.effects),
        }
    }
}
