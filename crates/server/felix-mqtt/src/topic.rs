//! The topic name of a PUBLISH, checked again where it is resolved.
//!
//! The decoder already refuses most bad names. What it lets through on
//! purpose is an empty name with a topic alias, which this listener does not
//! offer, so it is refused here.

/// The most bytes a topic name can have: its length is a two-byte field.
pub(crate) const MAX_TOPIC_NAME_BYTES: usize = 65_535;

/// Why a topic name cannot be published to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum InvalidTopicName {
    Empty,
    TooLong,
    Wildcard,
    Nul,
}

/// Whether `topic` is a name a PUBLISH may carry: not empty, at most
/// [`MAX_TOPIC_NAME_BYTES`] bytes, no `+` or `#`, no NUL. It is a `&str`, so
/// it is valid UTF-8 already.
pub(crate) fn check_topic_name(topic: &str) -> Result<(), InvalidTopicName> {
    if topic.is_empty() {
        return Err(InvalidTopicName::Empty);
    }
    if topic.len() > MAX_TOPIC_NAME_BYTES {
        return Err(InvalidTopicName::TooLong);
    }
    if topic.contains(['+', '#']) {
        return Err(InvalidTopicName::Wildcard);
    }
    if topic.contains('\0') {
        return Err(InvalidTopicName::Nul);
    }
    Ok(())
}

#[cfg(test)]
mod tests;
