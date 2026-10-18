use std::fmt;

use super::{
    RuntimeDispatch, RuntimeDriver, RuntimeMachine, RuntimeProgress, RuntimeStep, RuntimeWait,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RuntimeTurnLimits {
    pub max_quanta_per_turn: usize,
    pub max_outputs_per_turn: usize,
}

impl Default for RuntimeTurnLimits {
    fn default() -> Self {
        Self {
            max_quanta_per_turn: 4_096,
            max_outputs_per_turn: 4_096,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RuntimeConfigurationError {
    ZeroQuantumBudget,
    ZeroOutputBudget,
}

impl fmt::Display for RuntimeConfigurationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ZeroQuantumBudget => {
                formatter.write_str("runtime quantum budget must be positive")
            }
            Self::ZeroOutputBudget => formatter.write_str("runtime output budget must be positive"),
        }
    }
}

impl std::error::Error for RuntimeConfigurationError {}

#[derive(Debug, PartialEq, Eq)]
pub enum RuntimeDriverError<E> {
    Machine(E),
    Stopped,
}

impl<E: fmt::Display> fmt::Display for RuntimeDriverError<E> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Machine(error) => write!(formatter, "runtime machine failed: {error}"),
            Self::Stopped => formatter.write_str("runtime driver has stopped"),
        }
    }
}

impl<E> std::error::Error for RuntimeDriverError<E> where E: std::error::Error + 'static {}

pub struct RuntimeKernel<M> {
    machine: M,
    limits: RuntimeTurnLimits,
}

impl<M> RuntimeKernel<M> {
    pub fn new(machine: M, limits: RuntimeTurnLimits) -> Result<Self, RuntimeConfigurationError> {
        if limits.max_quanta_per_turn == 0 {
            return Err(RuntimeConfigurationError::ZeroQuantumBudget);
        }
        if limits.max_outputs_per_turn == 0 {
            return Err(RuntimeConfigurationError::ZeroOutputBudget);
        }
        Ok(Self { machine, limits })
    }

    pub fn with_default_limits(machine: M) -> Self {
        Self {
            machine,
            limits: RuntimeTurnLimits::default(),
        }
    }

    pub const fn machine(&self) -> &M {
        &self.machine
    }

    pub fn machine_mut(&mut self) -> &mut M {
        &mut self.machine
    }

    pub fn into_machine(self) -> M {
        self.machine
    }
}

impl<M> RuntimeDriver for RuntimeKernel<M>
where
    M: RuntimeMachine,
{
    type ExternalEvent = M::ExternalEvent;
    type Output = M::Output;
    type Error = RuntimeDriverError<M::Error>;

    fn dispatch(
        &mut self,
        dispatch: RuntimeDispatch<Self::ExternalEvent>,
    ) -> Result<RuntimeStep<Self::Output>, Self::Error> {
        if self.machine.is_stopped() {
            return Err(RuntimeDriverError::Stopped);
        }
        let now = dispatch.now;
        self.machine
            .accept(dispatch)
            .map_err(RuntimeDriverError::Machine)?;

        let mut outputs = Vec::new();
        let mut quanta = 0;
        while quanta < self.limits.max_quanta_per_turn
            && outputs.len() < self.limits.max_outputs_per_turn
        {
            quanta += 1;
            match self
                .machine
                .advance(now)
                .map_err(RuntimeDriverError::Machine)?
            {
                RuntimeProgress::Output(output) => outputs.push(output),
                RuntimeProgress::Progress => {}
                RuntimeProgress::Pending => break,
                RuntimeProgress::Stopped => break,
            }
        }

        let wait = if self.machine.is_stopped() {
            RuntimeWait::Stopped
        } else if self.machine.has_immediate_work() {
            RuntimeWait::Immediate
        } else {
            RuntimeWait::Event {
                deadline: self.machine.next_deadline(now),
            }
        };
        Ok(RuntimeStep { outputs, wait })
    }
}
