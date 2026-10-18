use std::cmp::Reverse;
use std::collections::hash_map::Entry;
use std::collections::{BinaryHeap, HashMap};

use crate::foundation::io::sys::time::MonotonicInstant;
use crate::framework::capabilities::io::Token;
use crate::runtime::server::worker::reactor::timers::TimeEventId;

use super::{
    BeforePollSlot, EventLane, EventLimits, EventLoopEvent, EventOwner, EventPayload, EventPhase,
    ReadinessOrigin, RunnableKey,
};

const BEFORE_POLL_SLOT_COUNT: usize = 7;
const READY_LANE_COUNT: usize = 3;
const PHASE_COUNT: usize = 3;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum QueueTarget {
    BeforePoll(BeforePollSlot),
    Ready(EventLane),
    Time {
        deadline: MonotonicInstant,
        id: TimeEventId,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct TimeQueueKey {
    deadline: MonotonicInstant,
    id: TimeEventId,
    sequence: u64,
}

struct SequenceNode<T> {
    previous: Option<u64>,
    next: Option<u64>,
    value: T,
}

struct SequenceQueue<T> {
    head: Option<u64>,
    tail: Option<u64>,
    nodes: HashMap<u64, SequenceNode<T>>,
    #[cfg(test)]
    removal_work: usize,
}

impl<T> Default for SequenceQueue<T> {
    fn default() -> Self {
        Self {
            head: None,
            tail: None,
            nodes: HashMap::new(),
            #[cfg(test)]
            removal_work: 0,
        }
    }
}

impl<T> SequenceQueue<T> {
    fn push_back(&mut self, sequence: u64, value: T) {
        let previous = self.tail;
        match self.nodes.entry(sequence) {
            Entry::Vacant(entry) => entry.insert(SequenceNode {
                previous,
                next: None,
                value,
            }),
            Entry::Occupied(_) => panic!("runtime event sequence must be queued at most once"),
        };
        if let Some(previous) = previous {
            self.nodes
                .get_mut(&previous)
                .expect("queued tail must have a sequence node")
                .next = Some(sequence);
        } else {
            self.head = Some(sequence);
        }
        self.tail = Some(sequence);
    }

    fn pop_front(&mut self) -> Option<(u64, T)> {
        if self.is_empty() {
            return None;
        }
        let sequence = self
            .head
            .expect("non-empty sequence queue must have a head");
        let value = self
            .remove(sequence)
            .expect("queued head must have a sequence node");
        Some((sequence, value))
    }

    fn remove(&mut self, sequence: u64) -> Option<T> {
        #[cfg(test)]
        {
            self.removal_work += 1;
        }
        let node = self.nodes.remove(&sequence)?;
        if let Some(previous) = node.previous {
            #[cfg(test)]
            {
                self.removal_work += 1;
            }
            self.nodes
                .get_mut(&previous)
                .expect("previous sequence must remain queued")
                .next = node.next;
        } else {
            self.head = node.next;
        }
        if let Some(next) = node.next {
            #[cfg(test)]
            {
                self.removal_work += 1;
            }
            self.nodes
                .get_mut(&next)
                .expect("next sequence must remain queued")
                .previous = node.previous;
        } else {
            self.tail = node.previous;
        }
        Some(node.value)
    }

    fn len(&self) -> usize {
        self.nodes.len()
    }

    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    #[cfg(test)]
    fn removal_work(&self) -> usize {
        self.removal_work
    }
}

impl QueueTarget {
    fn has_higher_priority_than(self, other: Self) -> bool {
        match (self, other) {
            (Self::BeforePoll(left), Self::BeforePoll(right)) => left.index() < right.index(),
            (Self::Ready(left), Self::Ready(right)) => left.index() < right.index(),
            _ => false,
        }
    }
}

pub(crate) struct EventBatch {
    limits: EventLimits,
    before_poll: [SequenceQueue<()>; BEFORE_POLL_SLOT_COUNT],
    ready: [SequenceQueue<()>; READY_LANE_COUNT],
    time: BinaryHeap<Reverse<TimeQueueKey>>,
    next_turn: SequenceQueue<QueueTarget>,
    events: HashMap<u64, EventLoopEvent>,
    event_targets: HashMap<u64, QueueTarget>,
    readiness_index: HashMap<(EventPhase, Token), u64>,
    runnable_sequences: HashMap<RunnableKey, u64>,
    completed_before_poll_slots: [bool; BEFORE_POLL_SLOT_COUNT],
    completed_phases: [bool; PHASE_COUNT],
    dispatched_events_by_phase: [usize; PHASE_COUNT],
    owner_dispatches: HashMap<EventOwner, usize>,
    next_sequence: u64,
}

impl Default for EventBatch {
    fn default() -> Self {
        Self::with_limits(EventLimits::default())
    }
}

impl EventBatch {
    pub(crate) fn with_limits(limits: EventLimits) -> Self {
        assert!(
            limits.max_events_per_phase_per_turn > 0,
            "max_events_per_phase_per_turn must be greater than zero"
        );
        assert!(
            limits.max_same_owner_events_per_turn > 0,
            "max_same_owner_events_per_turn must be greater than zero"
        );
        Self {
            limits,
            before_poll: std::array::from_fn(|_| SequenceQueue::default()),
            ready: std::array::from_fn(|_| SequenceQueue::default()),
            time: BinaryHeap::new(),
            next_turn: SequenceQueue::default(),
            events: HashMap::new(),
            event_targets: HashMap::new(),
            readiness_index: HashMap::new(),
            runnable_sequences: HashMap::new(),
            completed_before_poll_slots: [false; BEFORE_POLL_SLOT_COUNT],
            completed_phases: [false; PHASE_COUNT],
            dispatched_events_by_phase: [0; PHASE_COUNT],
            owner_dispatches: HashMap::new(),
            next_sequence: 0,
        }
    }

    pub(crate) fn push(&mut self, event: EventLoopEvent) {
        let target = match event.phase {
            EventPhase::BeforePoll => QueueTarget::BeforePoll(BeforePollSlot::PendingInput),
            EventPhase::Ready => QueueTarget::Ready(event.lane),
            EventPhase::Time => panic!("time events require an explicit deadline"),
        };
        self.insert(target, event);
    }

    pub(crate) fn push_before_poll(&mut self, slot: BeforePollSlot, mut event: EventLoopEvent) {
        event.phase = EventPhase::BeforePoll;
        self.insert(QueueTarget::BeforePoll(slot), event);
    }

    pub(crate) fn push_before_poll_next_turn(
        &mut self,
        slot: BeforePollSlot,
        mut event: EventLoopEvent,
    ) {
        event.phase = EventPhase::BeforePoll;
        self.insert_next_turn(QueueTarget::BeforePoll(slot), event);
    }

    pub(crate) fn push_time(&mut self, deadline: MonotonicInstant, mut event: EventLoopEvent) {
        let EventOwner::Timer(id) = event.owner else {
            panic!("time events require a timer owner");
        };
        event.phase = EventPhase::Time;
        self.insert(QueueTarget::Time { deadline, id }, event);
    }

    #[cfg(test)]
    pub(crate) fn pop_before_poll(&mut self) -> Option<EventLoopEvent> {
        for slot in 0..BEFORE_POLL_SLOT_COUNT {
            if let Some(event) = self.pop_before_poll_slot(BeforePollSlot::from_index(slot)) {
                return Some(event);
            }
        }
        None
    }

    pub(crate) fn pop_before_poll_slot(&mut self, slot: BeforePollSlot) -> Option<EventLoopEvent> {
        loop {
            if self.dispatch_budget_exhausted(EventPhase::BeforePoll) {
                self.roll_phase_to_next_turn(EventPhase::BeforePoll);
                return None;
            }
            let (sequence, ()) = self.before_poll[slot.index()].pop_front()?;
            let target = QueueTarget::BeforePoll(slot);
            if self.owner_budget_exhausted(sequence) {
                self.next_turn.push_back(sequence, target);
                continue;
            }
            if let Some(event) = self.take_dispatchable(sequence) {
                return Some(event);
            }
        }
    }

    pub(crate) fn finish_before_poll_slot(&mut self, slot: BeforePollSlot) {
        self.completed_before_poll_slots[slot.index()] = true;
        while let Some((sequence, ())) = self.before_poll[slot.index()].pop_front() {
            self.next_turn
                .push_back(sequence, QueueTarget::BeforePoll(slot));
        }
    }

    pub(crate) fn pop_ready(&mut self) -> Option<EventLoopEvent> {
        for lane in 0..READY_LANE_COUNT {
            loop {
                if self.dispatch_budget_exhausted(EventPhase::Ready) {
                    self.roll_phase_to_next_turn(EventPhase::Ready);
                    return None;
                }
                let Some((sequence, ())) = self.ready[lane].pop_front() else {
                    break;
                };
                let target = QueueTarget::Ready(EventLane::from_index(lane));
                if self.owner_budget_exhausted(sequence) {
                    self.next_turn.push_back(sequence, target);
                    continue;
                }
                if let Some(event) = self.take_dispatchable(sequence) {
                    return Some(event);
                }
            }
        }
        None
    }

    pub(crate) fn pop_time(&mut self) -> Option<EventLoopEvent> {
        loop {
            if self.dispatch_budget_exhausted(EventPhase::Time) {
                self.roll_phase_to_next_turn(EventPhase::Time);
                return None;
            }
            let Reverse(key) = self.time.pop()?;
            let target = QueueTarget::Time {
                deadline: key.deadline,
                id: key.id,
            };
            if self.owner_budget_exhausted(key.sequence) {
                self.next_turn.push_back(key.sequence, target);
                continue;
            }
            if let Some(event) = self.take_dispatchable(key.sequence) {
                return Some(event);
            }
        }
    }

    pub(crate) fn finish_phase(&mut self, phase: EventPhase) {
        if phase == EventPhase::BeforePoll {
            self.completed_before_poll_slots = [true; BEFORE_POLL_SLOT_COUNT];
        }
        self.roll_phase_to_next_turn(phase);
    }

    fn roll_phase_to_next_turn(&mut self, phase: EventPhase) {
        self.completed_phases[phase.index()] = true;
        self.defer_phase(phase);
    }

    pub(crate) fn begin_next_turn(&mut self) {
        let mut retained = SequenceQueue::default();
        self.drain_phase_into(EventPhase::BeforePoll, &mut retained);
        self.drain_phase_into(EventPhase::Ready, &mut retained);
        self.drain_phase_into(EventPhase::Time, &mut retained);
        while let Some((sequence, target)) = self.next_turn.pop_front() {
            retained.push_back(sequence, target);
        }

        self.completed_before_poll_slots = [false; BEFORE_POLL_SLOT_COUNT];
        self.completed_phases = [false; PHASE_COUNT];
        self.dispatched_events_by_phase = [0; PHASE_COUNT];
        self.owner_dispatches.clear();
        while let Some((sequence, target)) = retained.pop_front() {
            self.queue_current(target, sequence);
        }
    }

    pub(crate) fn has_pending_transport_data(&self) -> bool {
        self.events
            .values()
            .any(|event| matches!(event.payload, EventPayload::Readiness(_)))
    }

    pub(crate) fn has_due_time_events(&self) -> bool {
        !self.time.is_empty()
    }

    pub(crate) fn contains_timer(&self, kind: super::TimerEvent) -> bool {
        self.events
            .values()
            .any(|event| matches!(event.payload, EventPayload::Timer(timer) if timer == kind))
    }

    pub(crate) fn contains_runnable(&self, key: RunnableKey) -> bool {
        self.runnable_sequences.contains_key(&key)
    }

    pub(crate) fn remove_runnable(&mut self, key: RunnableKey) -> bool {
        let Some(&sequence) = self.runnable_sequences.get(&key) else {
            return false;
        };
        let target = self
            .event_targets
            .get(&sequence)
            .copied()
            .expect("indexed runnable must have a queue target");
        let removed_from_queue = if let Some(queued_target) = self.next_turn.remove(sequence) {
            debug_assert_eq!(queued_target, target);
            true
        } else {
            self.remove_from_current_queue(target, sequence)
        };
        assert!(
            removed_from_queue,
            "indexed runnable sequence must remain physically queued"
        );
        let event = self
            .events
            .remove(&sequence)
            .expect("indexed runnable must have an event payload");
        self.event_targets.remove(&sequence);
        self.remove_index(&event);
        true
    }

    pub(crate) fn remove_timer(&mut self, id: TimeEventId) -> bool {
        let Some(sequence) = self.events.iter().find_map(|(&sequence, event)| {
            (event.owner == EventOwner::Timer(id)).then_some(sequence)
        }) else {
            return false;
        };
        let Some(event) = self.events.remove(&sequence) else {
            return false;
        };
        self.event_targets.remove(&sequence);
        self.next_turn.remove(sequence);
        self.time
            .retain(|Reverse(key)| key.sequence != sequence && key.id != id);
        self.remove_index(&event);
        true
    }

    pub(crate) fn activate_next_turn_before_poll_slots(&mut self, slots: &[BeforePollSlot]) {
        let mut retained = SequenceQueue::default();
        while let Some((sequence, target)) = self.next_turn.pop_front() {
            let should_activate = matches!(
                target,
                QueueTarget::BeforePoll(slot) if slots.contains(&slot)
            );
            if should_activate {
                self.queue_current(target, sequence);
            } else {
                retained.push_back(sequence, target);
            }
        }
        self.next_turn = retained;
        for slot in slots {
            self.completed_before_poll_slots[slot.index()] = false;
        }
    }

    #[cfg(test)]
    pub(crate) fn drain_before_poll_for_test(&mut self) -> Vec<EventLoopEvent> {
        let mut events = Vec::new();
        while let Some(event) = self.pop_before_poll() {
            events.push(event);
        }
        events
    }

    #[cfg(test)]
    pub(crate) fn drain_ready_for_test(&mut self) -> Vec<EventLoopEvent> {
        let mut events = Vec::new();
        while let Some(event) = self.pop_ready() {
            events.push(event);
        }
        events
    }

    #[cfg(test)]
    pub(crate) fn current_queued_sequence_count_for_test(&self) -> usize {
        self.before_poll
            .iter()
            .map(SequenceQueue::len)
            .sum::<usize>()
            + self.ready.iter().map(SequenceQueue::len).sum::<usize>()
            + self.time.len()
    }

    #[cfg(test)]
    pub(crate) fn next_turn_queued_sequence_count_for_test(&self) -> usize {
        self.next_turn.len()
    }

    #[cfg(test)]
    pub(crate) fn sequence_unlink_work_for_test(&self) -> usize {
        self.before_poll
            .iter()
            .map(SequenceQueue::removal_work)
            .sum::<usize>()
            + self
                .ready
                .iter()
                .map(SequenceQueue::removal_work)
                .sum::<usize>()
            + self.next_turn.removal_work()
    }

    fn insert(&mut self, target: QueueTarget, mut event: EventLoopEvent) {
        if self.coalesce_readiness(target, &event) || self.is_duplicate_runnable(&event) {
            return;
        }

        self.next_sequence = self
            .next_sequence
            .checked_add(1)
            .expect("runtime event sequence exhausted");
        event.sequence = self.next_sequence;
        let sequence = event.sequence;
        self.index_event(sequence, &event);
        let phase = event.phase;
        self.events.insert(sequence, event);
        self.event_targets.insert(sequence, target);
        let completed_slot = matches!(
            target,
            QueueTarget::BeforePoll(slot) if self.completed_before_poll_slots[slot.index()]
        );
        if self.completed_phases[phase.index()] || completed_slot {
            self.next_turn.push_back(sequence, target);
        } else {
            self.queue_current(target, sequence);
        }
    }

    fn insert_next_turn(&mut self, target: QueueTarget, mut event: EventLoopEvent) {
        if self.coalesce_readiness(target, &event) || self.is_duplicate_runnable(&event) {
            return;
        }
        self.next_sequence = self
            .next_sequence
            .checked_add(1)
            .expect("runtime event sequence exhausted");
        event.sequence = self.next_sequence;
        let sequence = event.sequence;
        self.index_event(sequence, &event);
        self.events.insert(sequence, event);
        self.event_targets.insert(sequence, target);
        self.next_turn.push_back(sequence, target);
    }

    fn coalesce_readiness(&mut self, target: QueueTarget, incoming: &EventLoopEvent) -> bool {
        let EventPayload::Readiness(incoming_readiness) = &incoming.payload else {
            return false;
        };
        let incoming_readiness = *incoming_readiness;
        let key = (incoming.phase, incoming_readiness.token);
        let Some(&sequence) = self.readiness_index.get(&key) else {
            return false;
        };
        let Some(existing_target) = self.event_targets.get(&sequence).copied() else {
            self.readiness_index.remove(&key);
            return false;
        };
        {
            let Some(existing) = self.events.get_mut(&sequence) else {
                self.readiness_index.remove(&key);
                self.event_targets.remove(&sequence);
                return false;
            };
            let EventPayload::Readiness(existing) = &mut existing.payload else {
                return false;
            };
            existing.readable |= incoming_readiness.readable;
            existing.writable |= incoming_readiness.writable;
            existing.read_closed |= incoming_readiness.read_closed;
            existing.error |= incoming_readiness.error;
            if incoming_readiness.origin == ReadinessOrigin::Polled {
                existing.origin = ReadinessOrigin::Polled;
            }
        }
        if target.has_higher_priority_than(existing_target) {
            self.promote_queued_event(sequence, existing_target, target);
        }
        true
    }

    fn promote_queued_event(
        &mut self,
        sequence: u64,
        old_target: QueueTarget,
        new_target: QueueTarget,
    ) {
        if let Some(queued_target) = self.next_turn.remove(sequence) {
            debug_assert_eq!(queued_target, old_target);
            self.next_turn.push_back(sequence, new_target);
        } else {
            assert!(
                self.remove_from_current_queue(old_target, sequence),
                "coalesced readiness must remain in its indexed queue"
            );
            self.queue_current(new_target, sequence);
        }
        self.event_targets.insert(sequence, new_target);
        if let QueueTarget::Ready(lane) = new_target {
            if let Some(event) = self.events.get_mut(&sequence) {
                event.lane = lane;
            }
        }
    }

    fn remove_from_current_queue(&mut self, target: QueueTarget, sequence: u64) -> bool {
        match target {
            QueueTarget::BeforePoll(slot) => {
                self.before_poll[slot.index()].remove(sequence).is_some()
            }
            QueueTarget::Ready(lane) => self.ready[lane.index()].remove(sequence).is_some(),
            QueueTarget::Time { .. } => false,
        }
    }

    fn is_duplicate_runnable(&self, event: &EventLoopEvent) -> bool {
        let EventPayload::Runnable(runnable) = &event.payload else {
            return false;
        };
        self.runnable_sequences.contains_key(&runnable.key)
    }

    fn index_event(&mut self, sequence: u64, event: &EventLoopEvent) {
        match &event.payload {
            EventPayload::Readiness(readiness) => {
                self.readiness_index
                    .insert((event.phase, readiness.token), sequence);
            }
            EventPayload::Runnable(runnable) => {
                self.runnable_sequences.insert(runnable.key, sequence);
            }
            EventPayload::Timer(_)
            | EventPayload::WorkerCompletion(_)
            | EventPayload::Control(_)
            | EventPayload::ChildCompletion(_) => {}
        }
    }

    fn take_dispatchable(&mut self, sequence: u64) -> Option<EventLoopEvent> {
        let event = self.events.remove(&sequence)?;
        let owner = event.owner;
        self.event_targets.remove(&sequence);
        self.remove_index(&event);
        self.dispatched_events_by_phase[event.phase.index()] += 1;
        *self.owner_dispatches.entry(owner).or_default() += 1;
        Some(event)
    }

    /// Pop loops intentionally continue past capped-owner events until they
    /// find unrelated work or exhaust the queue. Returning early would make
    /// callers treat the phase as drained and could starve that unrelated
    /// work. Each capped event is removed once, so the scan is amortized
    /// linear in the number of queued events for the turn.
    fn owner_budget_exhausted(&self, sequence: u64) -> bool {
        let Some(owner) = self.events.get(&sequence).map(|event| event.owner) else {
            return false;
        };
        self.owner_dispatches.get(&owner).copied().unwrap_or(0)
            >= self.limits.max_same_owner_events_per_turn
    }

    fn remove_index(&mut self, event: &EventLoopEvent) {
        match &event.payload {
            EventPayload::Readiness(readiness) => {
                self.readiness_index.remove(&(event.phase, readiness.token));
            }
            EventPayload::Runnable(runnable) => {
                if self.runnable_sequences.get(&runnable.key) == Some(&event.sequence) {
                    self.runnable_sequences.remove(&runnable.key);
                }
            }
            EventPayload::Timer(_)
            | EventPayload::WorkerCompletion(_)
            | EventPayload::Control(_)
            | EventPayload::ChildCompletion(_) => {}
        }
    }

    fn dispatch_budget_exhausted(&self, phase: EventPhase) -> bool {
        self.dispatched_events_by_phase[phase.index()] >= self.limits.max_events_per_phase_per_turn
    }

    fn defer_phase(&mut self, phase: EventPhase) {
        let mut deferred = SequenceQueue::default();
        self.drain_phase_into(phase, &mut deferred);
        while let Some((sequence, target)) = deferred.pop_front() {
            self.next_turn.push_back(sequence, target);
        }
    }

    fn drain_phase_into(
        &mut self,
        phase: EventPhase,
        destination: &mut SequenceQueue<QueueTarget>,
    ) {
        match phase {
            EventPhase::BeforePoll => {
                for slot in 0..BEFORE_POLL_SLOT_COUNT {
                    let target = QueueTarget::BeforePoll(BeforePollSlot::from_index(slot));
                    while let Some((sequence, ())) = self.before_poll[slot].pop_front() {
                        destination.push_back(sequence, target);
                    }
                }
            }
            EventPhase::Ready => {
                for lane in 0..READY_LANE_COUNT {
                    let target = QueueTarget::Ready(EventLane::from_index(lane));
                    while let Some((sequence, ())) = self.ready[lane].pop_front() {
                        destination.push_back(sequence, target);
                    }
                }
            }
            EventPhase::Time => {
                while let Some(Reverse(key)) = self.time.pop() {
                    destination.push_back(
                        key.sequence,
                        QueueTarget::Time {
                            deadline: key.deadline,
                            id: key.id,
                        },
                    );
                }
            }
        }
    }

    fn queue_current(&mut self, target: QueueTarget, sequence: u64) {
        match target {
            QueueTarget::BeforePoll(slot) => {
                self.before_poll[slot.index()].push_back(sequence, ());
            }
            QueueTarget::Ready(lane) => self.ready[lane.index()].push_back(sequence, ()),
            QueueTarget::Time { deadline, id } => {
                self.time.push(Reverse(TimeQueueKey {
                    deadline,
                    id,
                    sequence,
                }));
            }
        }
    }
}

impl EventPhase {
    fn index(self) -> usize {
        match self {
            Self::BeforePoll => 0,
            Self::Ready => 1,
            Self::Time => 2,
        }
    }
}

impl EventLane {
    fn index(self) -> usize {
        match self {
            Self::Control => 0,
            Self::Upstream => 1,
            Self::Normal => 2,
        }
    }

    fn from_index(index: usize) -> Self {
        match index {
            0 => Self::Control,
            1 => Self::Upstream,
            2 => Self::Normal,
            _ => unreachable!("ready lane index is bounded by the queue array"),
        }
    }
}

impl BeforePollSlot {
    fn index(self) -> usize {
        self as usize
    }

    fn from_index(index: usize) -> Self {
        match index {
            0 => Self::PendingInput,
            1 => Self::PreAof,
            2 => Self::Maintenance,
            3 => Self::AofBarrier,
            4 => Self::OrdinaryWrites,
            5 => Self::DistributedWrites,
            6 => Self::PollPreparation,
            _ => unreachable!("before-poll slot index is bounded by the queue array"),
        }
    }
}

#[cfg(test)]
mod sequence_queue_tests {
    use super::SequenceQueue;

    #[test]
    fn arbitrary_removal_preserves_fifo_links() {
        let mut queue = SequenceQueue::default();
        queue.push_back(10, "first");
        queue.push_back(20, "middle");
        queue.push_back(30, "last");

        assert_eq!(queue.remove(20), Some("middle"));
        assert_eq!(queue.len(), 2);
        assert_eq!(queue.pop_front(), Some((10, "first")));
        assert_eq!(queue.pop_front(), Some((30, "last")));
        assert!(queue.is_empty());
        assert_eq!(queue.pop_front(), None);
    }

    #[test]
    fn removal_work_is_bounded_per_sequence() {
        const SEQUENCE_COUNT: usize = 4_096;
        let mut queue = SequenceQueue::default();
        for sequence in 0..SEQUENCE_COUNT as u64 {
            queue.push_back(sequence, ());
        }

        for sequence in (0..SEQUENCE_COUNT as u64).step_by(2) {
            assert_eq!(queue.remove(sequence), Some(()));
        }
        for sequence in (1..SEQUENCE_COUNT as u64).step_by(2) {
            assert_eq!(queue.remove(sequence), Some(()));
        }

        assert!(queue.is_empty());
        assert!(queue.removal_work() <= SEQUENCE_COUNT * 3);
    }
}
