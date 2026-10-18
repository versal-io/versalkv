#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(super) struct WorkerId(usize);

impl WorkerId {
    pub(super) const PRIMARY: Self = Self(0);

    pub(super) const fn index(self) -> usize {
        self.0
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum WorkerControl {
    Wake,
    Shutdown,
}
