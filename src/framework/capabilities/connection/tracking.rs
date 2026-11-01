//! Client-side key tracking state.
//! All tracking flags, redirect ownership and prefix accounting move together
//! so callers cannot accidentally update one projection without the others.

use bytes::Bytes;

#[derive(Default)]
pub(super) struct TrackingState {
    pub(super) enabled: bool,
    pub(super) broadcast: bool,
    pub(super) no_loop: bool,
    pub(super) opt_in: bool,
    pub(super) opt_out: bool,
    pub(super) caching: bool,
    /// The outer CLIENT command retains selection for the next command.
    pub(super) should_preserve_caching: bool,
    pub(super) redirect: Option<usize>,
    pub(super) prefixes: Vec<Bytes>,
    pub(super) prefix_node_count: usize,
    pub(super) broken_redirect: bool,
}
