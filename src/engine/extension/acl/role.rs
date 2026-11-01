//! Role registry and effective selector traversal.

use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};

use super::permissions::AclSelector;
use super::user::AclUser;

pub(super) struct EffectiveSelectors<'a, 'b> {
    inner: &'a mut dyn Iterator<Item = &'b AclSelector>,
}

impl<'b> Iterator for EffectiveSelectors<'_, 'b> {
    type Item = &'b AclSelector;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next()
    }
}

pub(crate) fn acl_roles() -> &'static Mutex<HashMap<String, AclUser>> {
    static ROLES: OnceLock<Mutex<HashMap<String, AclUser>>> = OnceLock::new();
    ROLES.get_or_init(|| Mutex::new(HashMap::new()))
}

pub(crate) fn validate_role_name(name: &[u8]) -> Result<(), &'static str> {
    if name.is_empty() {
        return Err("Role names can't be empty");
    }
    for byte in name {
        match byte {
            0..=32 | 127..=255 => {
                return Err("Role names can only contain printable ASCII characters");
            }
            b',' => return Err("Role names can't contain commas"),
            b'"' | b'\'' | b'\\' => return Err("Role names can't contain quotes or backslashes"),
            _ => {}
        }
    }
    Ok(())
}

pub(crate) fn apply_role_rule(
    role: &mut AclUser,
    rule: &str,
    default_allchannels: bool,
) -> Result<(), &'static str> {
    apply_role_rule_bytes(role, rule.as_bytes(), default_allchannels)
}

pub(crate) fn apply_role_rule_bytes(
    role: &mut AclUser,
    rule: &[u8],
    default_allchannels: bool,
) -> Result<(), &'static str> {
    if [
        "on",
        "off",
        "nopass",
        "resetpass",
        "reset",
        "resetroles",
        "skip-sanitize-payload",
        "sanitize-payload",
    ]
    .iter()
    .any(|reserved| rule.eq_ignore_ascii_case(reserved.as_bytes()))
        || matches!(rule.first(), Some(b'>' | b'<' | b'#' | b'!'))
        || rule
            .get(..5)
            .is_some_and(|prefix| prefix.eq_ignore_ascii_case(b"role="))
    {
        return Err("Syntax error");
    }
    if rule.is_empty() {
        return Ok(());
    }
    role.apply_rule_bytes_with_pubsub_default(rule, default_allchannels)
}

impl AclUser {
    /// Visit complete permission bundles, never combining rules across roles.
    /// Registry lock order is users, then roles; role edits follow that order.
    pub(super) fn with_selectors<R>(
        &self,
        action: impl FnOnce(&mut EffectiveSelectors<'_, '_>) -> R,
    ) -> R {
        let own = std::iter::once(&self.root_selector).chain(self.selectors.iter());
        if self.roles.is_empty() {
            return action(&mut EffectiveSelectors {
                inner: &mut own.into_iter(),
            });
        }
        let roles = match acl_roles().lock() {
            Ok(roles) => roles,
            Err(_) => {
                return action(&mut EffectiveSelectors {
                    inner: &mut own.into_iter(),
                })
            }
        };
        let inherited = self
            .roles
            .iter()
            .filter_map(|name| roles.get(name))
            .flat_map(|role| std::iter::once(&role.root_selector).chain(role.selectors.iter()));
        action(&mut EffectiveSelectors {
            inner: &mut own.chain(inherited),
        })
    }

    pub(crate) fn set_roles(
        &mut self,
        spec: &str,
        roles: &HashMap<String, AclUser>,
    ) -> Result<(), &'static str> {
        let mut resolved = Vec::new();
        for name in spec.split(',') {
            if name.is_empty() {
                return Err("Syntax error");
            }
            if !roles.contains_key(name) {
                return Err("The specified ACL role does not exist");
            }
            if !resolved.iter().any(|entry| entry == name) {
                resolved.push(name.to_string());
            }
        }
        self.roles = resolved;
        Ok(())
    }

    pub(crate) fn describe_role(&self, name: &str) -> String {
        let mut parts = vec![name.to_string(), self.root_selector.config_rule(true)];
        for selector in &self.selectors {
            parts.push(selector.config_rule(false));
        }
        parts.join(" ")
    }

    pub(crate) fn describe_role_bytes(&self, name: &str) -> Vec<u8> {
        let mut parts = vec![name.as_bytes().to_vec(), self.root_selector.describe()];
        for selector in &self.selectors {
            parts.push([b"(".as_slice(), &selector.describe(), b")"].concat());
        }
        parts.join(&b' ')
    }
}
