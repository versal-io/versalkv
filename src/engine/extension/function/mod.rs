mod registry;

pub(crate) use registry::parse_engine_name;
#[cfg(test)]
pub(crate) use registry::EngineManager;
pub use registry::{FunctionLib, FunctionRegistry};

#[cfg(test)]
mod tests;
