use super::user::{AclUser, ACL_ALL_PERMISSION};

impl AclUser {
    /// Returns true when this user exactly matches the factory-default
    /// "fully privileged default user" that servers bootstrap with. Used by
    /// CONFIG REWRITE to skip persisting default so any temporary runtime
    /// command-level tweaks (e.g. Sentinel's `-slaveof -failover`) do not
    /// stick across restarts, while still writing default when the
    /// configuration file or tests have explicitly altered its ACL set.
    pub(crate) fn is_factory_default_privileged(&self) -> bool {
        if self.flags != "on"
            || !self.nopass
            || !self.passwords.is_empty()
            || !self.selectors.is_empty()
            || self.root_selector.allowed_dbs.is_some()
        {
            return false;
        }
        let root = &self.root_selector;
        let has_star_key = root.patterns.len() == 1
            && matches!(
                root.patterns.first(),
                Some(p) if p.pattern == "*" && p.flags == ACL_ALL_PERMISSION
            );
        let has_star_channel = root.pubsub_patterns.len() == 1 && root.pubsub_patterns[0] == "*";
        let has_all_cat = root.categories.len() == 1 && root.categories[0] == "+@all";
        has_star_key && has_star_channel && has_all_cat
    }
}
