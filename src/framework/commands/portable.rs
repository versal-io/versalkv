#[path = "command_runtime.rs"]
pub mod command_runtime;

#[path = "key_extractors.rs"]
pub(crate) mod key_extractors;

include!(concat!(env!("OUT_DIR"), "/portable_command_modules.rs"));
