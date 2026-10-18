pub mod mt_driver;

pub use crate::engine::command::context::Token;

#[cfg(test)]
mod tests {
    use super::mt_driver::IoDriverMtConfig;

    #[test]
    fn mt_driver_config_counts_the_main_thread() {
        let config = IoDriverMtConfig::new(1);
        assert_eq!(config.n_workers(), 1);
    }
}
