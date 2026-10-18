use super::{RuntimeClock, RuntimeDispatch, RuntimeInstant};

pub trait RuntimeMachine {
    type ExternalEvent;
    type Output;
    type Error;

    fn accept(&mut self, dispatch: RuntimeDispatch<Self::ExternalEvent>)
        -> Result<(), Self::Error>;

    fn advance(&mut self, now: RuntimeClock) -> Result<RuntimeProgress<Self::Output>, Self::Error>;

    fn next_deadline(&self, now: RuntimeClock) -> Option<RuntimeInstant>;

    fn has_immediate_work(&self) -> bool;

    fn is_stopped(&self) -> bool;
}

#[derive(Debug, PartialEq, Eq)]
pub enum RuntimeProgress<O> {
    Output(O),
    Progress,
    Pending,
    Stopped,
}
