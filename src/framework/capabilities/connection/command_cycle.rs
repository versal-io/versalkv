//! State and effects owned by one connection command lifecycle.

use super::notifications::PendingNotifications;
use super::Connection;
use crate::engine::extension::script::PropagationOp;
use crate::foundation::io::protocol::Value;
use crate::foundation::io::store::Robj;
use bytes::Bytes;
use std::collections::HashSet;

#[derive(Default)]
pub(crate) struct CommandCycleState {
    request_bytes: usize,
    reply_bytes: usize,
    output_before: u64,
    custom_propagation: Vec<Vec<u8>>,
    script_propagation: Vec<PropagationOp>,
    skip_propagation: bool,
    dirty_delta: Option<i64>,
    dirty_keys: Vec<(u32, Bytes)>,
    notifications: PendingNotifications,
    lazyfree_objects: Vec<Robj>,
    swapdb: Option<(u32, u32)>,
    redacted_arguments: HashSet<usize>,
    cluster_config_save: Option<Value>,
    custom_reply: bool,
    reply_suppressed: bool,
    effects_pending: bool,
    outer_custom_reply: bool,
    outer_reply_suppressed: bool,
    outer_reply_pending: bool,
}

pub(crate) struct ConnectionCommandEffects {
    request_bytes: usize,
    reply_bytes: usize,
    output_before: u64,
    custom_propagation: Vec<Vec<u8>>,
    script_propagation: Vec<PropagationOp>,
    skip_propagation: bool,
    dirty_delta: Option<i64>,
    dirty_keys: Vec<(u32, Bytes)>,
    notifications: PendingNotifications,
    lazyfree_objects: Vec<Robj>,
    swapdb: Option<(u32, u32)>,
    redacted_arguments: HashSet<usize>,
    cluster_config_save: Option<Value>,
    custom_reply: bool,
    suppress_reply: bool,
}

#[derive(Clone, Copy)]
pub(crate) enum CommandEffectDrain {
    Completed,
    Nested,
}

impl CommandCycleState {
    pub(super) fn synthetic() -> Self {
        Self {
            skip_propagation: true,
            custom_reply: true,
            ..Self::default()
        }
    }

    pub(super) fn begin(&mut self, request_bytes: usize, output_before: u64) {
        debug_assert!(
            !self.has_effect_payload(),
            "previous command effects were not drained: {}",
            self.pending_effects_summary()
        );
        self.request_bytes = request_bytes;
        self.reply_bytes = 0;
        self.output_before = output_before;
        self.effects_pending = true;
    }

    pub(super) fn begin_nested(&mut self) {
        debug_assert!(
            !self.effects_pending,
            "previous nested effects were not drained: {}",
            self.pending_effects_summary()
        );
        self.effects_pending = true;
    }

    pub(super) fn begin_postponed(&mut self) {
        debug_assert!(
            !self.has_transient_effect_payload(),
            "postponed command effects were not retired: {}",
            self.pending_effects_summary()
        );
        self.effects_pending = true;
    }

    pub(super) fn retire_attempt(&mut self) {
        self.custom_propagation.clear();
        self.script_propagation.clear();
        self.skip_propagation = false;
        self.dirty_delta = None;
        self.dirty_keys.clear();
        self.notifications = PendingNotifications::default();
        self.lazyfree_objects.clear();
        self.swapdb = None;
        self.cluster_config_save = None;
        self.custom_reply = false;
        self.reply_suppressed = false;
        self.effects_pending = false;
        self.outer_custom_reply = false;
        self.outer_reply_suppressed = false;
        self.outer_reply_pending = false;
        debug_assert!(!self.has_transient_effect_payload());
    }

    fn has_effect_payload(&self) -> bool {
        (self.effects_pending
            && (self.has_transient_effect_payload() || !self.redacted_arguments.is_empty()))
            || self.outer_reply_pending
    }

    fn has_transient_effect_payload(&self) -> bool {
        self.skip_propagation
            || self.dirty_delta.is_some()
            || !self.custom_propagation.is_empty()
            || !self.script_propagation.is_empty()
            || !self.dirty_keys.is_empty()
            || !self.notifications.local.is_empty()
            || !self.notifications.shard.is_empty()
            || !self.notifications.cluster.is_empty()
            || !self.notifications.cluster_shard.is_empty()
            || !self.lazyfree_objects.is_empty()
            || self.swapdb.is_some()
            || self.cluster_config_save.is_some()
            || self.custom_reply
            || self.reply_suppressed
    }

    fn pending_effects_summary(&self) -> String {
        format!(
            "pending={} outer_reply_pending={} skip={} custom={} script={} dirty={} notifications={}/{}/{}/{} lazyfree={} swapdb={} redacted={} config_save={} custom_reply={}/{} suppressed={}/{}",
            self.effects_pending,
            self.outer_reply_pending,
            self.skip_propagation,
            self.custom_propagation.len(),
            self.script_propagation.len(),
            self.dirty_keys.len(),
            self.notifications.local.len(),
            self.notifications.shard.len(),
            self.notifications.cluster.len(),
            self.notifications.cluster_shard.len(),
            self.lazyfree_objects.len(),
            self.swapdb.is_some(),
            self.redacted_arguments.len(),
            self.cluster_config_save.is_some(),
            self.custom_reply,
            self.outer_custom_reply,
            self.reply_suppressed,
            self.outer_reply_suppressed,
        )
    }

    pub(super) fn take_effects(
        &mut self,
        include_outer_reply_state: bool,
    ) -> Option<ConnectionCommandEffects> {
        if !(self.effects_pending || include_outer_reply_state && self.outer_reply_pending) {
            return None;
        }
        self.effects_pending = false;
        let custom_reply = std::mem::take(&mut self.custom_reply);
        let reply_suppressed = std::mem::take(&mut self.reply_suppressed);
        let outer_custom_reply = if include_outer_reply_state {
            self.outer_reply_pending = false;
            std::mem::take(&mut self.outer_custom_reply)
        } else {
            false
        };
        let outer_reply_suppressed = if include_outer_reply_state {
            std::mem::take(&mut self.outer_reply_suppressed)
        } else {
            false
        };
        Some(ConnectionCommandEffects {
            request_bytes: self.request_bytes,
            reply_bytes: self.reply_bytes,
            output_before: self.output_before,
            custom_propagation: std::mem::take(&mut self.custom_propagation),
            script_propagation: std::mem::take(&mut self.script_propagation),
            skip_propagation: std::mem::take(&mut self.skip_propagation),
            dirty_delta: self.dirty_delta.take(),
            dirty_keys: std::mem::take(&mut self.dirty_keys),
            notifications: std::mem::take(&mut self.notifications),
            lazyfree_objects: std::mem::take(&mut self.lazyfree_objects),
            swapdb: self.swapdb.take(),
            redacted_arguments: std::mem::take(&mut self.redacted_arguments),
            cluster_config_save: self.cluster_config_save.take(),
            custom_reply: custom_reply || outer_custom_reply,
            suppress_reply: reply_suppressed || outer_reply_suppressed,
        })
    }

    pub(super) fn discard_effects(&mut self) {
        let _ = self.take_effects(true);
    }

    pub(super) fn finish_accounting(&mut self) {
        self.request_bytes = 0;
        self.reply_bytes = 0;
        self.output_before = 0;
        self.redacted_arguments.clear();
        self.outer_custom_reply = false;
        self.outer_reply_suppressed = false;
        self.outer_reply_pending = false;
    }

    pub(super) fn add_reply_bytes(&mut self, bytes: usize) {
        self.reply_bytes = self.reply_bytes.saturating_add(bytes);
        self.effects_pending = true;
    }

    pub(super) fn request_bytes(&self) -> usize {
        self.request_bytes
    }

    pub(super) fn set_skip_propagation(&mut self) {
        self.skip_propagation = true;
        self.effects_pending = true;
    }
}

impl ConnectionCommandEffects {
    pub(crate) fn request_bytes(&self) -> usize {
        self.request_bytes
    }

    pub(crate) fn reply_bytes(&self) -> usize {
        self.reply_bytes
    }

    pub(crate) fn output_before(&self) -> u64 {
        self.output_before
    }

    pub(crate) fn skip_propagation(&self) -> bool {
        self.skip_propagation
    }

    pub(crate) fn take_dirty_delta(&mut self) -> Option<i64> {
        self.dirty_delta.take()
    }

    pub(crate) fn custom_propagation(&self) -> &[Vec<u8>] {
        &self.custom_propagation
    }

    pub(crate) fn script_propagation(&self) -> &[PropagationOp] {
        &self.script_propagation
    }

    pub(crate) fn dirty_keys(&self) -> &[(u32, Bytes)] {
        &self.dirty_keys
    }

    #[cfg(test)]
    pub(crate) fn notifications(&self) -> &[(Bytes, Bytes)] {
        &self.notifications.local
    }

    #[cfg(test)]
    pub(crate) fn swapdb(&self) -> Option<(u32, u32)> {
        self.swapdb
    }

    pub(crate) fn is_argument_redacted(&self, position: usize) -> bool {
        self.redacted_arguments.contains(&position)
    }

    pub(crate) fn has_custom_reply(&self) -> bool {
        self.custom_reply
    }

    pub(crate) fn should_suppress_reply(&self) -> bool {
        self.suppress_reply
    }

    pub(crate) fn allow_default_reply(&mut self) {
        self.custom_reply = false;
    }

    pub(crate) fn set_reply_bytes(&mut self, bytes: usize) {
        self.reply_bytes = bytes;
    }

    pub(crate) fn take_custom_propagation(&mut self) -> Vec<Vec<u8>> {
        std::mem::take(&mut self.custom_propagation)
    }

    pub(crate) fn take_script_propagation(&mut self) -> Vec<PropagationOp> {
        std::mem::take(&mut self.script_propagation)
    }

    pub(crate) fn take_dirty_keys(&mut self) -> Vec<(u32, Bytes)> {
        std::mem::take(&mut self.dirty_keys)
    }

    pub(crate) fn take_notifications(&mut self) -> Vec<(Bytes, Bytes)> {
        std::mem::take(&mut self.notifications.local)
    }

    pub(crate) fn take_shard_notifications(&mut self) -> Vec<(Bytes, Bytes)> {
        std::mem::take(&mut self.notifications.shard)
    }

    pub(crate) fn take_cluster_notifications(&mut self) -> Vec<(Bytes, Bytes)> {
        std::mem::take(&mut self.notifications.cluster)
    }

    pub(crate) fn take_cluster_shard_notifications(&mut self) -> Vec<(Bytes, Bytes)> {
        std::mem::take(&mut self.notifications.cluster_shard)
    }

    pub(crate) fn take_lazyfree_objects(&mut self) -> Vec<Robj> {
        std::mem::take(&mut self.lazyfree_objects)
    }

    pub(crate) fn take_swapdb(&mut self) -> Option<(u32, u32)> {
        self.swapdb.take()
    }

    pub(crate) fn take_cluster_config_save(&mut self) -> Option<Value> {
        self.cluster_config_save.take()
    }
}

impl Connection {
    pub(crate) fn begin_nested_command_effects(&mut self) {
        self.command_cycle.begin_nested();
    }

    pub(crate) fn resume_command_effects(&mut self) {
        self.command_cycle.effects_pending = true;
    }

    pub(crate) fn resume_postponed_command_effects(&mut self) {
        self.command_cycle.begin_postponed();
    }

    /// Close a preflight attempt without releasing its command-level input
    /// and output accounting; the eventual wakeup completes that lifecycle.
    pub(crate) fn retire_command_attempt_for_postpone(&mut self) {
        self.command_cycle.retire_attempt();
    }

    pub(crate) fn take_command_effects(
        &mut self,
        drain: CommandEffectDrain,
    ) -> Option<ConnectionCommandEffects> {
        if matches!(drain, CommandEffectDrain::Completed) && self.blocked.is_some() {
            return None;
        }
        let mut effects = self
            .command_cycle
            .take_effects(matches!(drain, CommandEffectDrain::Completed))?;
        match drain {
            CommandEffectDrain::Completed => {
                effects.suppress_reply |=
                    self.reply.should_suppress_current(effects.suppress_reply);
            }
            CommandEffectDrain::Nested => {}
        }
        Some(effects)
    }

    pub(crate) fn finish_nested_reply_state(&mut self, effects: &mut ConnectionCommandEffects) {
        let custom_reply = std::mem::take(&mut effects.custom_reply);
        let suppress_reply = std::mem::take(&mut effects.suppress_reply);
        self.command_cycle.outer_custom_reply |= custom_reply;
        self.command_cycle.outer_reply_suppressed |= suppress_reply;
        self.command_cycle.outer_reply_pending |= custom_reply || suppress_reply;
    }

    pub(crate) fn discard_command_effects(&mut self) {
        self.command_cycle.discard_effects();
    }

    pub(crate) fn command_has_custom_reply(&self) -> bool {
        self.command_cycle.custom_reply || self.command_cycle.outer_custom_reply
    }

    pub(crate) fn set_command_custom_reply(&mut self, enabled: bool) {
        self.command_cycle.custom_reply = enabled;
        self.command_cycle.effects_pending |= enabled;
    }

    pub(crate) fn command_reply_suppressed(&self) -> bool {
        self.command_cycle.reply_suppressed || self.command_cycle.outer_reply_suppressed
    }

    pub(crate) fn suppress_command_reply(&mut self) {
        self.command_cycle.reply_suppressed = true;
        self.command_cycle.effects_pending = true;
    }

    pub(crate) fn finish_command_reply_state(&mut self) {
        self.command_cycle.custom_reply = false;
        self.command_cycle.reply_suppressed = false;
        self.command_cycle.outer_custom_reply = false;
        self.command_cycle.outer_reply_suppressed = false;
        self.command_cycle.outer_reply_pending = false;
    }

    pub(crate) fn request_bytes_current_command(&self) -> usize {
        self.command_cycle.request_bytes
    }

    pub(crate) fn reply_bytes_current_command(&self) -> usize {
        self.command_cycle.reply_bytes
    }

    pub(crate) fn output_before_current_command(&self) -> u64 {
        self.command_cycle.output_before
    }

    #[cfg(test)]
    pub(crate) fn set_request_bytes_for_test(&mut self, bytes: usize) {
        self.command_cycle.request_bytes = bytes;
        self.command_cycle.effects_pending = true;
    }

    #[cfg(test)]
    pub(crate) fn set_reply_bytes_for_test(&mut self, bytes: usize) {
        self.command_cycle.reply_bytes = bytes;
        self.command_cycle.effects_pending = true;
    }

    pub(crate) fn record_direct_output_bytes(&mut self, bytes: usize) {
        self.tot_net_out = self.tot_net_out.saturating_add(bytes as u64);
    }

    pub(crate) fn record_command_reply_bytes(&mut self, bytes: usize) {
        self.command_cycle.add_reply_bytes(bytes);
    }

    pub(crate) fn skip_propagation(&self) -> bool {
        self.command_cycle.skip_propagation
    }

    pub(crate) fn set_skip_propagation(&mut self, skip: bool) {
        self.command_cycle.skip_propagation = skip;
        self.command_cycle.effects_pending |= skip;
    }

    pub(crate) fn record_custom_propagation(&mut self, frame: Vec<u8>) {
        self.command_cycle.effects_pending = true;
        self.command_cycle.custom_propagation.push(frame);
    }

    pub(crate) fn extend_custom_propagation(&mut self, frames: Vec<Vec<u8>>) {
        if !frames.is_empty() {
            self.command_cycle.effects_pending = true;
            self.command_cycle.custom_propagation.extend(frames);
        }
    }

    pub(crate) fn clear_custom_propagation(&mut self) {
        self.command_cycle.custom_propagation.clear();
    }

    pub(crate) fn custom_propagation_checkpoint(&self) -> usize {
        self.command_cycle.custom_propagation.len()
    }

    pub(crate) fn take_custom_propagation_since(&mut self, checkpoint: usize) -> Vec<Vec<u8>> {
        self.command_cycle
            .custom_propagation
            .split_off(checkpoint.min(self.command_cycle.custom_propagation.len()))
    }

    pub(crate) fn custom_propagation(&self) -> &[Vec<u8>] {
        &self.command_cycle.custom_propagation
    }

    pub(crate) fn record_script_propagation(&mut self, operation: PropagationOp) {
        self.command_cycle.effects_pending = true;
        self.command_cycle.script_propagation.push(operation);
    }

    pub(crate) fn extend_script_propagation(&mut self, operations: Vec<PropagationOp>) {
        if !operations.is_empty() {
            self.command_cycle.effects_pending = true;
            self.command_cycle.script_propagation.extend(operations);
        }
    }

    pub(crate) fn script_propagation(&self) -> &[PropagationOp] {
        &self.command_cycle.script_propagation
    }

    pub(crate) fn record_dirty_key(&mut self, dirty_key: (u32, Bytes)) {
        self.command_cycle.effects_pending = true;
        self.command_cycle.dirty_keys.push(dirty_key);
    }

    pub(crate) fn record_dirty_delta(&mut self, delta: i64) {
        self.command_cycle.effects_pending = true;
        self.command_cycle.dirty_delta = Some(
            self.command_cycle
                .dirty_delta
                .unwrap_or(0)
                .saturating_add(delta),
        );
    }

    pub(crate) fn replace_dirty_keys(&mut self, dirty_keys: Vec<(u32, Bytes)>) {
        self.command_cycle.effects_pending = true;
        self.command_cycle.dirty_keys = dirty_keys;
    }

    pub(crate) fn dirty_keys(&self) -> &[(u32, Bytes)] {
        &self.command_cycle.dirty_keys
    }

    pub(crate) fn record_notification(&mut self, notification: (Bytes, Bytes)) {
        self.command_cycle.effects_pending = true;
        self.command_cycle.notifications.local.push(notification);
    }

    pub(crate) fn extend_notifications(&mut self, notifications: Vec<(Bytes, Bytes)>) {
        if !notifications.is_empty() {
            self.command_cycle.effects_pending = true;
            self.command_cycle.notifications.local.extend(notifications);
        }
    }

    pub(crate) fn record_shard_notification(&mut self, notification: (Bytes, Bytes)) {
        self.command_cycle.effects_pending = true;
        self.command_cycle.notifications.shard.push(notification);
    }

    pub(crate) fn record_cluster_notification(&mut self, notification: (Bytes, Bytes)) {
        self.command_cycle.effects_pending = true;
        self.command_cycle.notifications.cluster.push(notification);
    }

    pub(crate) fn record_cluster_shard_notification(&mut self, notification: (Bytes, Bytes)) {
        self.command_cycle.effects_pending = true;
        self.command_cycle
            .notifications
            .cluster_shard
            .push(notification);
    }

    pub(crate) fn defer_command_lazyfree_object(&mut self, object: Robj) {
        self.command_cycle.effects_pending = true;
        self.command_cycle.lazyfree_objects.push(object);
    }

    #[cfg(test)]
    pub(crate) fn pending_lazyfree_objects(&self) -> &[Robj] {
        &self.command_cycle.lazyfree_objects
    }

    #[cfg(test)]
    pub(crate) fn notifications(&self) -> &[(Bytes, Bytes)] {
        &self.command_cycle.notifications.local
    }

    #[cfg(test)]
    pub(crate) fn shard_notifications(&self) -> &[(Bytes, Bytes)] {
        &self.command_cycle.notifications.shard
    }

    pub(crate) fn swapdb_intent(&self) -> Option<(u32, u32)> {
        self.command_cycle.swapdb
    }

    pub(crate) fn set_swapdb_intent(&mut self, swapdb: Option<(u32, u32)>) {
        self.command_cycle.effects_pending = true;
        self.command_cycle.swapdb = swapdb;
    }

    pub(crate) fn request_cluster_config_save_intent(&mut self, reply: Value) {
        self.command_cycle.effects_pending = true;
        self.command_cycle.cluster_config_save = Some(reply);
    }

    pub(crate) fn redact_argument(&mut self, position: usize) -> bool {
        if position == 0 {
            return false;
        }
        self.command_cycle.effects_pending = true;
        self.command_cycle.redacted_arguments.insert(position)
    }

    pub(crate) fn is_argument_redacted(&self, position: usize) -> bool {
        self.command_cycle.redacted_arguments.contains(&position)
    }
}

#[cfg(test)]
mod dirty_tests {
    use super::*;

    #[test]
    fn dirty_delta_is_drained_once_across_nested_and_retired_attempts() {
        let mut conn = Connection::new_fake();
        conn.record_dirty_delta(2);
        conn.record_dirty_delta(3);
        let mut effects = conn
            .take_command_effects(CommandEffectDrain::Nested)
            .unwrap();
        assert_eq!(effects.take_dirty_delta(), Some(5));
        assert_eq!(effects.take_dirty_delta(), None);
        assert!(conn
            .take_command_effects(CommandEffectDrain::Nested)
            .is_none());
        conn.begin_nested_command_effects();
        conn.record_dirty_delta(0);
        let mut effects = conn
            .take_command_effects(CommandEffectDrain::Nested)
            .unwrap();
        assert_eq!(effects.take_dirty_delta(), Some(0));
        conn.begin_nested_command_effects();
        conn.record_dirty_delta(8);
        conn.retire_command_attempt_for_postpone();
        conn.resume_postponed_command_effects();
        let mut effects = conn
            .take_command_effects(CommandEffectDrain::Completed)
            .unwrap();
        assert_eq!(effects.take_dirty_delta(), None);
        conn.record_dirty_delta(4);
        conn.discard_command_effects();
        assert!(conn
            .take_command_effects(CommandEffectDrain::Completed)
            .is_none());
    }
}
