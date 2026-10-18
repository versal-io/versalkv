pub type WorkerId = usize;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IoDriverMtConfig {
    n_workers: usize,
}

impl IoDriverMtConfig {
    pub fn new(n_workers: usize) -> Self {
        Self { n_workers }
    }

    pub fn n_workers(self) -> usize {
        self.n_workers
    }
}
