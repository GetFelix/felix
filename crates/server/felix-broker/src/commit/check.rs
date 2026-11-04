//! A commit's precondition on a key's version.

/// What a commit requires of one key's version: `None` for no value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VersionCheck {
    pub key: String,
    pub required: Option<u64>,
}
