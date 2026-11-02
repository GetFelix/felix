//! Mounting a stream under an MQTT topic prefix.
//!
//! A mount ties every topic that starts with a prefix to one durable stream
//! (`docs/mqtt-design.md`, "Topic model"). The prefix is a plain byte prefix:
//! `plant/` and `plant` are different mounts, and `plant` also catches
//! `plants/x`.
use super::stream::StreamKey;

/// The longest prefix. The listener's topic limits are configurable and apply
/// to the whole topic, so a prefix this long can leave no room under it.
pub(crate) const MAX_MOUNT_BYTES: usize = 1024;

/// Starts every refusal [`overlap`] and [`needs_durable`] build, so an API
/// handler can tell them from a stream that already exists.
const REFUSAL: &str = "mount refused: ";

/// Whether `prefix` can be a mount: a non-empty topic prefix with no
/// wildcards or control characters, at most [`MAX_MOUNT_BYTES`] bytes, and not
/// in the `$` space MQTT reserves for the broker itself.
pub(crate) fn validate(prefix: &str) -> Result<(), String> {
    if prefix.is_empty() {
        return Err("mount must not be empty".to_string());
    }
    if prefix.len() > MAX_MOUNT_BYTES {
        return Err(format!("mount is longer than {MAX_MOUNT_BYTES} bytes"));
    }
    if prefix.contains(['+', '#']) {
        return Err("mount must not contain the wildcards + or #".to_string());
    }
    if prefix.chars().any(char::is_control) {
        return Err("mount must not contain control characters".to_string());
    }
    if prefix.starts_with('$') {
        return Err("mount must not start with $, which MQTT reserves for the broker".to_string());
    }
    Ok(())
}

/// Whether one prefix is, or begins with, the other. Two mounts of a tenant
/// may not, since a topic under both would have two streams to go to.
pub(crate) fn overlaps(a: &str, b: &str) -> bool {
    a.starts_with(b) || b.starts_with(a)
}

/// The first of `mounted` (a stream and its prefix) that `prefix` overlaps,
/// skipping `own`, the stream being written.
pub(crate) fn find_overlap<'a>(
    prefix: &str,
    own: &StreamKey,
    mounted: impl IntoIterator<Item = (&'a StreamKey, &'a str)>,
) -> Option<(&'a StreamKey, &'a str)> {
    mounted
        .into_iter()
        .filter(|(key, _)| *key != own)
        .find(|(_, other)| overlaps(prefix, other))
}

/// The refusal when `prefix` overlaps the mount `other` of a stream in the
/// same tenant.
pub(crate) fn overlap(
    prefix: &str,
    other_key: &StreamKey,
    other: &str,
) -> crate::store::StoreError {
    crate::store::StoreError::Conflict(format!(
        "{REFUSAL}{prefix:?} overlaps {other:?}, mounted on stream {}/{}",
        other_key.namespace, other_key.stream
    ))
}

/// The refusal when a stream with a mount would not be durable.
pub(crate) fn needs_durable() -> crate::store::StoreError {
    crate::store::StoreError::Conflict(format!(
        "{REFUSAL}only a durable stream can be mounted; clear the mount before making it non-durable"
    ))
}

/// Whether `err` is one of this module's refusals, and its words if so.
pub(crate) fn refusal(err: &crate::store::StoreError) -> Option<&str> {
    match err {
        crate::store::StoreError::Conflict(message) if message.starts_with(REFUSAL) => {
            Some(message.trim_start_matches(REFUSAL))
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests;
