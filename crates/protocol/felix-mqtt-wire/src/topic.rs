//! Topic name and topic filter rules.

use crate::limits::MAX_FIELD_BYTES;
use crate::protocol::Version;

/// Why a topic name or filter is refused.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum TopicError {
    #[error("is empty")]
    Empty,
    #[error("is longer than {max} bytes")]
    TooLong { max: usize },
    #[error("has more than {max} levels")]
    TooManyLevels { max: usize },
    #[error("contains a wildcard")]
    Wildcard,
    #[error("misuses a wildcard")]
    BadWildcard,
    #[error("contains a control or non-character code point")]
    DiscouragedCharacter,
    #[error("has a malformed shared subscription prefix")]
    BadShare,
}

/// The size bounds a topic is held to.
#[derive(Clone, Copy)]
pub(crate) struct TopicBounds {
    pub(crate) max_bytes: usize,
    pub(crate) max_levels: usize,
}

impl TopicBounds {
    /// What the format allows, for encoding where no listener limit applies.
    pub(crate) const FORMAT: TopicBounds = TopicBounds {
        max_bytes: MAX_FIELD_BYTES,
        max_levels: usize::MAX,
    };
}

/// Splits `$share/<name>/<filter>` into the share name and the filter. `None`
/// when `filter` is not a shared subscription.
pub fn split_shared_filter(filter: &str) -> Option<(&str, &str)> {
    let rest = filter.strip_prefix("$share/")?;
    rest.split_once('/')
}

/// A topic name, as in a `PUBLISH` or a will. Wildcards are forbidden. An empty
/// name is only valid in 5.0 with a topic alias, which the caller checks, so it
/// is reported as `Empty` for the caller to decide.
pub(crate) fn check_topic_name(name: &str, bounds: TopicBounds) -> Result<(), TopicError> {
    if name.is_empty() {
        return Err(TopicError::Empty);
    }
    if name.contains(['+', '#']) {
        return Err(TopicError::Wildcard);
    }
    check_shape(name, bounds)
}

/// A topic filter, as in `SUBSCRIBE` and `UNSUBSCRIBE`. Under 5.0 a filter that
/// starts `$share/` must be a well-formed shared subscription.
pub(crate) fn check_topic_filter(
    filter: &str,
    version: Version,
    bounds: TopicBounds,
) -> Result<(), TopicError> {
    if filter.is_empty() {
        return Err(TopicError::Empty);
    }
    if filter.len() > bounds.max_bytes {
        return Err(TopicError::TooLong {
            max: bounds.max_bytes,
        });
    }
    let matched = if version.is_v5() && filter.starts_with("$share/") {
        let (name, rest) = split_shared_filter(filter).ok_or(TopicError::BadShare)?;
        if name.is_empty() || name.contains(['+', '#']) {
            return Err(TopicError::BadShare);
        }
        rest
    } else {
        filter
    };
    if matched.is_empty() {
        return Err(TopicError::Empty);
    }
    check_shape(matched, bounds)?;
    check_wildcards(matched)
}

fn check_shape(s: &str, bounds: TopicBounds) -> Result<(), TopicError> {
    if s.len() > bounds.max_bytes {
        return Err(TopicError::TooLong {
            max: bounds.max_bytes,
        });
    }
    if s.split('/').count() > bounds.max_levels {
        return Err(TopicError::TooManyLevels {
            max: bounds.max_levels,
        });
    }
    if s.chars().any(is_discouraged) {
        return Err(TopicError::DiscouragedCharacter);
    }
    Ok(())
}

/// `#` is a whole level and the last one; `+` is a whole level.
fn check_wildcards(filter: &str) -> Result<(), TopicError> {
    let mut levels = filter.split('/').peekable();
    while let Some(level) = levels.next() {
        match level {
            "#" if levels.peek().is_some() => return Err(TopicError::BadWildcard),
            "#" | "+" => {}
            _ if level.contains(['+', '#']) => return Err(TopicError::BadWildcard),
            _ => {}
        }
    }
    Ok(())
}

/// The code points the specification says a string should not include: control
/// characters and Unicode non-characters. U+0000 is refused earlier, for every
/// string. A topic is a key and a log line, so topics are held to the
/// stricter reading.
fn is_discouraged(c: char) -> bool {
    let cp = c as u32;
    matches!(cp, 0x01..=0x1F | 0x7F..=0x9F | 0xFDD0..=0xFDEF) || cp & 0xFFFE == 0xFFFE
}

#[cfg(test)]
mod tests;
