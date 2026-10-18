mod core;

pub use core::{can_abort_shrink, set_can_abort_shrink, HashTable, TableStats, ENTRIES_PER_BUCKET};

#[cfg(test)]
mod tests;
