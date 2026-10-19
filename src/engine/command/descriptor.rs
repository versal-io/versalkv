use super::{CmdFlags, CommandArgs};
use bytes::Bytes;

#[derive(Clone, Copy, Debug)]
pub struct CommandKeyIndices {
    next: usize,
    last: usize,
    step: usize,
    arg_count: usize,
}

impl Iterator for CommandKeyIndices {
    type Item = usize;

    fn next(&mut self) -> Option<Self::Item> {
        if self.next > self.last || self.next >= self.arg_count {
            return None;
        }
        let index = self.next;
        self.next = self.next.saturating_add(self.step);
        Some(index)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CommandDescriptor {
    pub name: &'static str,
    pub arity: i32,
    pub flags: CmdFlags,
    pub first_key: i32,
    pub last_key: i32,
    pub key_step: i32,
    pub container: Option<&'static str>,
    pub acl_categories: &'static [&'static str],
}

#[derive(Clone, Copy)]
pub struct ResolvedCommand {
    descriptor: CommandDescriptor,
    registry_id: u64,
    execution_id: usize,
    generation: u64,
}

impl ResolvedCommand {
    pub(crate) fn new(
        descriptor: CommandDescriptor,
        registry_id: u64,
        execution_id: usize,
        generation: u64,
    ) -> Self {
        Self {
            descriptor,
            registry_id,
            execution_id,
            generation,
        }
    }

    pub fn descriptor(self) -> CommandDescriptor {
        self.descriptor
    }

    pub(crate) fn execution_token(self) -> (u64, usize, u64) {
        (self.registry_id, self.execution_id, self.generation)
    }
}

impl CommandDescriptor {
    pub fn key_indices(self, args: &CommandArgs) -> Option<CommandKeyIndices> {
        if self.first_key <= 0 {
            return None;
        }
        let first = self.first_key as usize;
        let last = if self.last_key == 0 {
            first - 1
        } else if self.last_key < 0 {
            let step = self.key_step.unsigned_abs() as usize;
            let arg_count = args.args.len();
            if step == 0 || arg_count == 0 {
                return None;
            }
            ((arg_count - 1) / step) * step
        } else {
            self.last_key as usize - 1
        };
        let step = self.key_step.unsigned_abs() as usize;
        if step == 0 {
            return None;
        }
        Some(CommandKeyIndices {
            next: first - 1,
            last,
            step,
            arg_count: args.args.len(),
        })
    }

    pub fn extract_keys(self, args: &CommandArgs) -> Option<Vec<Bytes>> {
        self.key_indices(args)
            .map(|indices| indices.map(|index| args.args[index].clone()).collect())
    }
}
