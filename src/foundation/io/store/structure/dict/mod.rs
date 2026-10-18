mod core;

#[cfg(test)]
#[path = "tests.rs"]
pub mod tests;

pub(crate) use core::ResizeBudget;
pub use core::{Dict, ResizePolicy};
