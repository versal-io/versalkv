mod registry;

#[cfg(test)]
pub(crate) use registry::EngineManager;
pub use registry::FunctionRegistry;
pub(crate) use registry::{parse_engine_name, FunctionLib};

#[cfg(test)]
mod tests;
