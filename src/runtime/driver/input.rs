use super::{OwnedHostCompletion, RuntimeClock};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RuntimeInput<E> {
    External(E),
    Wake,
    HostCompletion(OwnedHostCompletion),
    Shutdown,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RuntimeDispatch<E> {
    pub now: RuntimeClock,
    pub input: RuntimeInput<E>,
}

impl<E> RuntimeDispatch<E> {
    pub const fn new(now: RuntimeClock, input: RuntimeInput<E>) -> Self {
        Self { now, input }
    }
}
