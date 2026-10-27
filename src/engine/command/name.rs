use std::fmt;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

/// A command name owned by every specification or resolved command using it.
#[derive(Clone, Debug)]
pub enum CommandName {
    Static(&'static str),
    Shared(Arc<str>),
}

impl CommandName {
    pub fn as_str(&self) -> &str {
        match self {
            Self::Static(name) => name,
            Self::Shared(name) => name,
        }
    }
}

impl PartialEq for CommandName {
    fn eq(&self, other: &Self) -> bool {
        self.as_str() == other.as_str()
    }
}

impl Eq for CommandName {}

impl Hash for CommandName {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.as_str().hash(state);
    }
}

impl fmt::Display for CommandName {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};

    #[test]
    fn static_and_shared_names_compare_and_hash_by_content() {
        const STATIC: CommandName = CommandName::Static("module.command");
        let shared = CommandName::Shared(Arc::from("module.command"));
        assert_eq!(STATIC, shared);
        assert_ne!(shared, CommandName::Shared(Arc::from("module.other")));
        let hash = |name: &CommandName| {
            let mut hasher = DefaultHasher::new();
            name.hash(&mut hasher);
            hasher.finish()
        };
        assert_eq!(hash(&STATIC), hash(&shared));
        assert_eq!(shared.to_string(), "module.command");
    }

    #[test]
    fn cloned_names_share_the_same_allocation_until_final_drop() {
        let owner: Arc<str> = Arc::from("module.command");
        let weak = Arc::downgrade(&owner);
        let name = CommandName::Shared(owner);
        let cloned = name.clone();
        assert_eq!(weak.strong_count(), 2);
        assert!(std::ptr::eq(name.as_str(), cloned.as_str()));
        drop(name);
        assert_eq!(cloned.as_str(), "module.command");
        drop(cloned);
        assert!(weak.upgrade().is_none());
    }
}
