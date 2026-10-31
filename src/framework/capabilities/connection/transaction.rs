//! Per-client MULTI/EXEC and WATCH state.
//! The fields share one transaction lifecycle: queueing begins at MULTI and
//! every completion, discard, teardown, or failed EXEC must clear them as one
//! unit.

use crate::engine::command::context::WatchedKey;
use crate::engine::command::CommandArgs;
use std::collections::HashSet;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum TransactionEnd {
    Discard,
    WatchInvalidated,
    AdmissionRejected,
    ExecutionCompleted,
    Reset,
}

#[derive(Default)]
pub(crate) struct TransactionState {
    multi: Option<MultiQueue>,
    watch: WatchState,
}

struct MultiQueue {
    db_id: i64,
    input_bytes: u64,
    commands: Vec<CommandArgs>,
    queue_error: bool,
    exec_arguments: Vec<bytes::Bytes>,
}

#[derive(Default)]
struct WatchState {
    keys: Vec<WatchedKey>,
    is_dirty: bool,
}

impl TransactionState {
    pub(crate) fn is_active(&self) -> bool {
        self.multi.is_some()
    }

    pub(crate) fn begin(&mut self, db_id: i64, input_bytes: u64) -> bool {
        if let Some(multi) = &mut self.multi {
            multi.queue_error = true;
            return false;
        }
        self.multi = Some(MultiQueue {
            db_id,
            input_bytes,
            commands: Vec::new(),
            queue_error: false,
            exec_arguments: Vec::new(),
        });
        true
    }

    pub(crate) fn end(&mut self, end: TransactionEnd) {
        match end {
            TransactionEnd::Discard | TransactionEnd::AdmissionRejected => {
                debug_assert!(self.is_active());
            }
            TransactionEnd::WatchInvalidated => {
                debug_assert!(self.is_active() && self.watch.is_dirty);
            }
            TransactionEnd::ExecutionCompleted => {
                debug_assert!(self
                    .multi
                    .as_ref()
                    .is_some_and(|multi| multi.commands.is_empty()));
            }
            TransactionEnd::Reset => {}
        }
        self.multi = None;
        self.watch = WatchState::default();
    }

    pub(crate) fn record_input(&mut self, bytes: u64) {
        if let Some(multi) = &mut self.multi {
            multi.input_bytes = multi.input_bytes.saturating_add(bytes);
        }
    }

    pub(crate) fn input_bytes_with_exec(&self, exec_bytes: u64) -> Option<u64> {
        self.multi
            .as_ref()
            .map(|multi| multi.input_bytes.saturating_add(exec_bytes))
    }

    pub(crate) fn selected_db(&self) -> Option<i64> {
        self.multi.as_ref().map(|multi| multi.db_id)
    }

    pub(crate) fn set_exec_arguments(&mut self, arguments: Vec<bytes::Bytes>) {
        if let Some(multi) = &mut self.multi {
            multi.exec_arguments = arguments;
        }
    }

    pub(crate) fn exec_arguments(&self) -> &[bytes::Bytes] {
        self.multi
            .as_ref()
            .map_or(&[], |multi| &multi.exec_arguments)
    }

    pub(crate) fn queue(&mut self, num_dbs: usize, command: CommandArgs) -> bool {
        let Some(multi) = &mut self.multi else {
            return false;
        };
        if command.name.eq_ignore_ascii_case(b"select") {
            if let Some(db_id) = command
                .args
                .first()
                .and_then(|arg| std::str::from_utf8(arg).ok())
                .and_then(|arg| arg.parse::<u32>().ok())
                .filter(|db_id| usize::try_from(*db_id).is_ok_and(|db_id| db_id < num_dbs))
            {
                multi.db_id = i64::from(db_id);
            }
        }
        multi.commands.push(command);
        true
    }

    pub(crate) fn queued_commands(&self) -> &[CommandArgs] {
        self.multi
            .as_ref()
            .map_or(&[], |multi| multi.commands.as_slice())
    }

    pub(crate) fn take_queued_commands(&mut self) -> Vec<CommandArgs> {
        self.multi
            .as_mut()
            .map(|multi| std::mem::take(&mut multi.commands))
            .unwrap_or_default()
    }

    pub(crate) fn mark_queue_error(&mut self) {
        if let Some(multi) = &mut self.multi {
            multi.queue_error = true;
        }
    }

    pub(crate) fn has_queue_error(&self) -> bool {
        self.multi.as_ref().is_some_and(|multi| multi.queue_error)
    }

    pub(crate) fn watch_many(&mut self, watched: Vec<WatchedKey>) -> usize {
        let mut seen: HashSet<(u32, bytes::Bytes)> = self
            .watch
            .keys
            .iter()
            .map(|current| (current.db_id, current.key.clone()))
            .collect();
        let before = self.watch.keys.len();
        for watched in watched {
            if seen.insert((watched.db_id, watched.key.clone())) {
                self.watch.keys.push(watched);
            }
        }
        self.watch.keys.len().saturating_sub(before)
    }

    pub(crate) fn watched_keys(&self) -> &[WatchedKey] {
        &self.watch.keys
    }

    pub(crate) fn watched_key_count(&self) -> usize {
        self.watch.keys.len()
    }

    pub(crate) fn clear_watches(&mut self) {
        self.watch = WatchState::default();
    }

    pub(crate) fn is_watch_dirty(&self) -> bool {
        self.watch.is_dirty
    }

    pub(crate) fn touch_watched_key(&mut self, db_id: u32, key: &[u8], exists_after: bool) -> bool {
        let changed = self
            .watch
            .keys
            .iter_mut()
            .find(|watched| watched.matches(db_id, key))
            .is_some_and(|watched| watched.touched(exists_after));
        self.watch.is_dirty |= changed;
        changed
    }

    pub(crate) fn update_watches_after_swap(
        &mut self,
        db1: u32,
        db2: u32,
        mut key_state: impl FnMut(u32, &[u8]) -> (bool, bool),
    ) {
        for watched in &mut self.watch.keys {
            if watched.db_id != db1 && watched.db_id != db2 {
                continue;
            }
            let original_db = if watched.db_id == db1 { db2 } else { db1 };
            let (original_exists, _) = key_state(original_db, &watched.key);
            let (replacement_exists, replacement_expired) = key_state(watched.db_id, &watched.key);
            if !original_exists && !replacement_exists {
                continue;
            }
            if watched.expired && !replacement_exists {
                watched.expired = false;
            } else if watched.expired && replacement_expired {
                continue;
            } else if !watched.expired && !original_exists && replacement_expired {
                watched.expired = true;
            } else {
                self.watch.is_dirty = true;
                break;
            }
        }
    }

    pub(crate) fn queue_memory_usage(&self) -> usize {
        let Some(multi) = &self.multi else {
            return 0;
        };
        multi
            .commands
            .capacity()
            .saturating_mul(std::mem::size_of::<CommandArgs>())
            .saturating_add(multi.commands.iter().fold(0usize, |total, command| {
                total.saturating_add(command_memory_usage(command))
            }))
            .saturating_add(
                multi
                    .exec_arguments
                    .capacity()
                    .saturating_mul(std::mem::size_of::<bytes::Bytes>()),
            )
            .saturating_add(
                multi
                    .exec_arguments
                    .iter()
                    .map(bytes::Bytes::len)
                    .sum::<usize>(),
            )
    }

    pub(crate) fn watch_memory_usage(&self) -> usize {
        self.watch
            .keys
            .capacity()
            .saturating_mul(std::mem::size_of::<WatchedKey>())
            .saturating_add(self.watch.keys.iter().fold(0usize, |total, watched| {
                total.saturating_add(watched.key.len())
            }))
    }
}

fn command_memory_usage(command: &CommandArgs) -> usize {
    command
        .name
        .len()
        .saturating_add(
            command
                .args
                .capacity()
                .saturating_mul(std::mem::size_of::<bytes::Bytes>()),
        )
        .saturating_add(
            command
                .args
                .iter()
                .fold(0usize, |total, arg| total.saturating_add(arg.len())),
        )
}

#[cfg(test)]
mod tests {
    use super::{TransactionEnd, TransactionState};
    use crate::engine::command::context::WatchedKey;
    use crate::engine::command::CommandArgs;
    use bytes::Bytes;

    fn command(parts: &[&'static [u8]]) -> CommandArgs {
        CommandArgs::from_parts(parts.iter().map(|part| Bytes::from_static(part)).collect())
    }

    fn populated_transaction() -> TransactionState {
        let mut state = TransactionState::default();
        assert!(state.begin(2, 11));
        assert!(state.queue(16, command(&[b"SET", b"key", b"value"])));
        assert_eq!(
            state.watch_many(vec![WatchedKey::new(
                2,
                Bytes::from_static(b"watched"),
                false,
            )]),
            1
        );
        state.record_input(13);
        state.mark_queue_error();
        assert!(state.touch_watched_key(2, b"watched", true));
        state
    }

    fn assert_transaction_cleared(state: &TransactionState) {
        assert!(!state.is_active());
        assert!(state.queued_commands().is_empty());
        assert!(!state.has_queue_error());
        assert_eq!(state.input_bytes_with_exec(17), None);
        assert!(state.watched_keys().is_empty());
        assert!(!state.is_watch_dirty());
    }

    #[test]
    fn begin_queue_and_accounting_form_one_lifecycle() {
        let mut state = TransactionState::default();

        assert!(state.begin(3, 10));
        assert!(state.queue(16, command(&[b"PING"])));
        assert!(state.queue(16, command(&[b"SELECT", b"7"])));
        state.record_input(20);

        assert!(state.is_active());
        assert_eq!(state.selected_db(), Some(7));
        assert_eq!(state.queued_commands().len(), 2);
        assert_eq!(state.input_bytes_with_exec(5), Some(35));
        assert!(!state.begin(9, 100));
        assert!(state.has_queue_error());
        assert_eq!(state.selected_db(), Some(7));
    }

    #[test]
    fn queued_select_matches_database_index_validity_from_nonzero_db() {
        for (select_db, expected_db) in [
            (b"-0".as_slice(), 1),
            (b"-1", 1),
            (b"16", 1),
            (b"9223372036854775807", 1),
            (b"4294967296", 1),
            (b"0", 0),
            (b"1", 1),
            (b"15", 15),
        ] {
            let mut state = TransactionState::default();
            assert!(state.begin(1, 0));
            let select = command(&[b"SELECT", select_db]);

            assert!(state.queue(16, select.clone()));

            assert_eq!(
                state.selected_db(),
                Some(expected_db),
                "SELECT {}",
                String::from_utf8_lossy(select_db)
            );
            assert_eq!(state.queued_commands().len(), 1);
            assert_eq!(state.queued_commands()[0].args, select.args);
            assert!(!state.has_queue_error());
        }
    }

    #[test]
    fn discard_ends_queue_watch_error_and_accounting_state() {
        let mut state = populated_transaction();

        state.end(TransactionEnd::Discard);

        assert_transaction_cleared(&state);
    }

    #[test]
    fn watch_invalidated_ends_the_whole_transaction() {
        let mut state = populated_transaction();

        state.end(TransactionEnd::WatchInvalidated);

        assert_transaction_cleared(&state);
    }

    #[test]
    fn admission_rejected_ends_the_whole_transaction() {
        let mut state = populated_transaction();

        state.end(TransactionEnd::AdmissionRejected);

        assert_transaction_cleared(&state);
    }

    #[test]
    fn execution_completed_ends_after_the_queue_is_taken() {
        let mut state = populated_transaction();
        let commands = state.take_queued_commands();

        state.end(TransactionEnd::ExecutionCompleted);

        assert_eq!(commands.len(), 1);
        assert_transaction_cleared(&state);
    }

    #[test]
    fn reset_clears_transaction_and_watch_state_idempotently() {
        let mut state = populated_transaction();

        state.end(TransactionEnd::Reset);
        state.end(TransactionEnd::Reset);

        assert_transaction_cleared(&state);
    }

    #[test]
    fn watch_transitions_deduplicate_and_own_dirty_marking() {
        let mut state = TransactionState::default();
        let watched = WatchedKey::new(4, Bytes::from_static(b"key"), false);

        assert_eq!(state.watch_many(vec![watched.clone(), watched]), 1);
        assert!(state.touch_watched_key(4, b"key", true));

        assert_eq!(state.watched_key_count(), 1);
        assert!(state.is_watch_dirty());
        state.clear_watches();
        assert!(state.watched_keys().is_empty());
        assert!(!state.is_watch_dirty());
    }
}
