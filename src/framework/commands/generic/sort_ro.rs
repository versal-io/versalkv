// Re-exports SORT_RO_SPEC from the sort module for build.rs registration.
// The actual handler is defined in sort.rs via #[command("SORT_RO")].
pub use super::sort::SORT_RO_SPEC;
