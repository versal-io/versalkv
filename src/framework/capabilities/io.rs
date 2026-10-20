pub use crate::engine::command::context::Token;

pub mod mt_driver {
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
}

#[cfg(test)]
mod tests {
    use super::mt_driver::IoDriverMtConfig;

    #[test]
    fn mt_driver_config_counts_the_main_thread() {
        let config = IoDriverMtConfig::new(1);
        assert_eq!(config.n_workers(), 1);
    }
}
