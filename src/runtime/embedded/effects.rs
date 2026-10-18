use crate::engine::command::SessionId;
use crate::engine::core::{CommandEffects, Value};
use crate::runtime::driver::{CommandEffectSink, CommandTransition, RuntimeCapabilities};

pub(super) struct EmbeddedEffectSink {
    capabilities: RuntimeCapabilities,
}

impl EmbeddedEffectSink {
    pub(super) const fn new(capabilities: RuntimeCapabilities) -> Self {
        Self { capabilities }
    }
}

impl CommandEffectSink for EmbeddedEffectSink {
    type Output = Value;

    fn commit(&mut self, _session: SessionId, transition: CommandTransition) -> Self::Output {
        let outcome = match transition {
            CommandTransition::Complete(outcome) => outcome,
            CommandTransition::Suspend(_) if !self.capabilities.blocking => {
                return Value::error("ERR command requires a blocking capability");
            }
            CommandTransition::Suspend(_) => {
                return Value::error("ERR embedded blocking continuation was not registered");
            }
            CommandTransition::Close(_) if !self.capabilities.connections => {
                return Value::error("ERR command requires a connection lifecycle capability");
            }
            CommandTransition::Close(outcome) => outcome,
        };
        let CommandEffects {
            changed_keys,
            events,
            db_events,
            skip_propagate,
            custom_propagations,
            deferred_objects,
            expired_hash_fields,
            suppress_reply,
            close_connection,
        } = outcome.effects;
        drop((
            changed_keys,
            events,
            db_events,
            skip_propagate,
            custom_propagations,
            deferred_objects,
            expired_hash_fields,
        ));
        if suppress_reply || close_connection {
            return Value::error("ERR command requires a connection lifecycle capability");
        }
        outcome.reply
    }
}
