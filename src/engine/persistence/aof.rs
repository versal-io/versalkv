use super::PersistenceState;
use crate::engine::extension::config::PersistenceConfig;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AofRewritePolicy {
    pub enabled: bool,
    pub percentage: i64,
    pub min_size: u64,
}

impl AofRewritePolicy {
    pub fn from_config(config: &PersistenceConfig) -> Self {
        Self {
            enabled: config.aof_enabled,
            percentage: config.aof_rewrite_perc,
            min_size: config.aof_rewrite_min_size,
        }
    }
}

pub fn should_auto_rewrite(state: &PersistenceState, policy: AofRewritePolicy) -> bool {
    if !policy.enabled
        || policy.percentage <= 0
        || state.has_active_child()
        || state.aof().current_size <= policy.min_size
    {
        return false;
    }

    let current_size = u128::from(state.aof().current_size);
    let base_size = u128::from(state.aof().rewrite_base_size.max(1));
    let growth_percentage = current_size.saturating_mul(100) / base_size;
    growth_percentage >= 100 + policy.percentage as u128
}

#[cfg(test)]
mod tests {
    use super::{should_auto_rewrite, AofRewritePolicy};
    use crate::engine::persistence::PersistenceState;

    fn policy() -> AofRewritePolicy {
        AofRewritePolicy {
            enabled: true,
            percentage: 100,
            min_size: 64,
        }
    }

    #[test]
    fn automatic_rewrite_requires_enabled_aof_and_positive_percentage() {
        let mut state = PersistenceState::new();
        state.record_aof_size(256);
        state.record_aof_rewrite_base_size(64);

        assert!(!should_auto_rewrite(
            &state,
            AofRewritePolicy {
                enabled: false,
                ..policy()
            }
        ));
        assert!(!should_auto_rewrite(
            &state,
            AofRewritePolicy {
                percentage: 0,
                ..policy()
            }
        ));
    }

    #[test]
    fn automatic_rewrite_requires_no_child_and_size_above_minimum() {
        let mut state = PersistenceState::new();
        state.record_aof_size(64);
        state.record_aof_rewrite_base_size(1);
        assert!(!should_auto_rewrite(&state, policy()));

        state.record_aof_size(65);
        state.begin_rdb_child(41, 100).unwrap();
        assert!(!should_auto_rewrite(&state, policy()));
    }

    #[test]
    fn automatic_rewrite_uses_growth_percentage_from_base_size() {
        let mut state = PersistenceState::new();
        state.record_aof_rewrite_base_size(100);

        state.record_aof_size(199);
        assert!(!should_auto_rewrite(&state, policy()));

        state.record_aof_size(200);
        assert!(should_auto_rewrite(&state, policy()));
    }

    #[test]
    fn automatic_rewrite_treats_zero_base_as_one() {
        let mut state = PersistenceState::new();
        state.record_aof_size(65);

        assert!(should_auto_rewrite(&state, policy()));
    }
}
