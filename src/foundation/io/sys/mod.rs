#![deny(unsafe_op_in_unsafe_fn)]

pub mod arm64_cow;
pub mod fd;
pub mod log;
pub mod os;
pub mod polling;
pub mod process;
pub mod proctitle;
pub mod rand;
pub mod signal;
pub mod socket;
pub mod time;

#[cfg(test)]
mod adapter_tests;
