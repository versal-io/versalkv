use super::RuntimeInstant;

#[derive(Debug, PartialEq, Eq)]
pub struct RuntimeStep<O> {
    pub outputs: Vec<O>,
    pub wait: RuntimeWait,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RuntimeWait {
    Immediate,
    Event { deadline: Option<RuntimeInstant> },
    Stopped,
}
