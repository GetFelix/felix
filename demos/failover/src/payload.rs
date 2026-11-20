//! The record a producer writes: `run:producer:seq:` and padding.

use std::time::{SystemTime, UNIX_EPOCH};

use crate::ledger::Key;

/// Names this run, so records left by an earlier one on the same stream are
/// ignored rather than counted.
pub(crate) fn run_id() -> String {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_secs());
    format!("{now:x}")
}

pub(crate) fn encode(run: &str, key: Key, size: usize) -> Vec<u8> {
    let mut text = format!("{run}:{}:{}:", key.0, key.1);
    if text.len() < size {
        text.extend(std::iter::repeat_n('.', size - text.len()));
    }
    text.into_bytes()
}

/// The key a payload carries, or `None` if it is another run's or not ours.
pub(crate) fn decode(run: &str, bytes: &[u8]) -> Option<Key> {
    let text = std::str::from_utf8(bytes).ok()?;
    let mut parts = text.splitn(4, ':');
    if parts.next()? != run {
        return None;
    }
    let producer = parts.next()?.parse().ok()?;
    let seq = parts.next()?.parse().ok()?;
    Some((producer, seq))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_payload_round_trips_and_pads() {
        let bytes = encode("abc", (3, 42), 64);
        assert_eq!(bytes.len(), 64);
        assert_eq!(decode("abc", &bytes), Some((3, 42)));
    }

    #[test]
    fn another_runs_record_is_not_ours() {
        assert_eq!(decode("abc", &encode("xyz", (0, 1), 32)), None);
        assert_eq!(decode("abc", b"hello"), None);
    }
}
