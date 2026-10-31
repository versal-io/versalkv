//! ACL command-category matching and SHA-256 helpers.
//! Built-in command membership is supplied by callers from generated Valkey
//! metadata. This L3 module owns only the fixed built-in category namespace
//! and dynamic Module category membership.

use bytes::Bytes;
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};

pub(crate) const BUILTIN_ACL_CATEGORY_NAMES: &[&str] = &[
    "KEYSPACE",
    "READ",
    "WRITE",
    "SET",
    "SORTEDSET",
    "LIST",
    "HASH",
    "PATHHASH",
    "STRING",
    "BITMAP",
    "HYPERLOGLOG",
    "GEO",
    "STREAM",
    "PUBSUB",
    "ADMIN",
    "FAST",
    "SLOW",
    "BLOCKING",
    "DANGEROUS",
    "CONNECTION",
    "TRANSACTION",
    "SCRIPTING",
];

#[derive(Clone, Copy)]
pub(crate) struct AclCommand<'a> {
    pub(crate) name: &'a str,
    pub(crate) args: &'a [Bytes],
    pub(crate) acl_categories: &'a [&'a str],
}

impl<'a> AclCommand<'a> {
    pub(crate) fn new(name: &'a str, args: &'a [Bytes], acl_categories: &'a [&'a str]) -> Self {
        Self {
            name,
            args,
            acl_categories,
        }
    }

    pub(crate) fn belongs_to(self, category: &str) -> bool {
        if category.eq_ignore_ascii_case("all") {
            return true;
        }
        self.acl_categories
            .iter()
            .any(|candidate| candidate.eq_ignore_ascii_case(category))
            || module_category_contains(category, self.name)
    }
}

/// SHA256 hash encoded as lowercase hexadecimal.
pub(crate) fn sha256_hex(input: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(input);
    let result = hasher.finalize();
    hex::encode(result)
}

pub(crate) fn is_valid_sha256_hex(s: &str) -> bool {
    s.len() == 64
        && s.bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

pub(crate) fn is_builtin_acl_category(category: &str) -> bool {
    BUILTIN_ACL_CATEGORY_NAMES
        .iter()
        .any(|candidate| candidate.eq_ignore_ascii_case(category))
}

fn module_categories() -> &'static Mutex<HashMap<String, Vec<String>>> {
    static CATEGORIES: OnceLock<Mutex<HashMap<String, Vec<String>>>> = OnceLock::new();
    CATEGORIES.get_or_init(|| Mutex::new(HashMap::new()))
}

pub(crate) fn register_module_acl_category(category: &str, command: &str) {
    module_categories()
        .lock()
        .unwrap()
        .entry(category.to_ascii_uppercase())
        .or_default()
        .push(command.to_ascii_uppercase());
}

pub(crate) fn remove_module_acl_category_all(category: &str) {
    module_categories()
        .lock()
        .unwrap()
        .remove(&category.to_ascii_uppercase());
}

pub(crate) fn module_acl_category_commands(category: &str) -> Option<Vec<String>> {
    let mut commands = module_categories()
        .lock()
        .unwrap()
        .get(&category.to_ascii_uppercase())
        .cloned()?;
    commands.sort();
    commands.dedup();
    Some(
        commands
            .into_iter()
            .map(|value| value.to_ascii_lowercase())
            .collect(),
    )
}

pub(crate) fn module_acl_categories() -> Vec<String> {
    let mut categories: Vec<String> = module_categories()
        .lock()
        .unwrap()
        .keys()
        .map(|value| value.to_ascii_lowercase())
        .collect();
    categories.sort();
    categories
}

fn module_category_contains(category: &str, command: &str) -> bool {
    module_categories()
        .lock()
        .unwrap()
        .get(&category.to_ascii_uppercase())
        .is_some_and(|commands| {
            commands
                .iter()
                .any(|candidate| candidate.eq_ignore_ascii_case(command))
        })
}

/// Mirror C `shouldRestrictCmd`: a command is subject to `db=` restriction
/// when it belongs to KEYSPACE/READ/WRITE or declares keys.
pub(crate) fn should_restrict_cmd(cmd_name: &str, acl_categories: &[&str], has_keys: bool) -> bool {
    let command = AclCommand::new(cmd_name, &[], acl_categories);
    has_keys
        || command.belongs_to("KEYSPACE")
        || command.belongs_to("READ")
        || command.belongs_to("WRITE")
}
