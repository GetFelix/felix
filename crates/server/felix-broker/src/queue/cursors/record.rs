//! What one cursor record holds, and when it still stands.
//!
//! The layout is in `docs/storage-format.md` ("Consumer-group cursor
//! values").

use crate::error::{BrokerError, Result};

/// The dead-letter log a cursor record needs beneath it: how long the log was
/// when the record was written, and the generation that wrote the dead
/// letter just below that end. A record with `end` zero needs none.
///
/// A group's dead letter is durable before its cursor passes the record, so
/// every dead letter a cursor passed on is below the log's end when the
/// cursor was written. The generation pins which dead letters those were: a
/// leader syncs its dead letters before a cursor that needs them and before
/// it ships them, so one generation never puts two different records at one
/// offset where a cursor or a replica could see both.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct DeadLetterMark {
    pub(crate) end: u64,
    pub(crate) generation: u64,
}

impl DeadLetterMark {
    /// The mark for a dead-letter log that ends at `end` with this history.
    pub(crate) fn of(end: u64, epochs: &[felix_storage::log::Epoch]) -> Self {
        Self {
            end,
            generation: generation_at(epochs, end),
        }
    }

    /// Whether a dead-letter log ending at `end` with this history holds
    /// what this mark asks for.
    pub(crate) fn held_by(&self, end: u64, epochs: &[felix_storage::log::Epoch]) -> bool {
        self.end == 0 || (end >= self.end && generation_at(epochs, self.end) == self.generation)
    }
}

/// One stored cursor record.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Stored {
    /// Where the group resumes.
    pub(crate) offset: u64,
    /// What the record needs of the dead-letter log.
    pub(crate) requires: DeadLetterMark,
    /// The last position the group was put at without needing a dead letter:
    /// where it was created or sought to, or a commit made with no dead
    /// letters in the log. What the group falls back to when nothing later
    /// stands.
    pub(crate) origin: u64,
}

/// The first byte of a cursor value that carries its dependency.
const DEPENDENT: u8 = 2;
const DEPENDENT_LEN: usize = 33;
const LEGACY_LEN: usize = 8;

impl Stored {
    /// A position that needs no dead letter.
    pub(crate) fn placed(offset: u64) -> Self {
        Self {
            offset,
            requires: DeadLetterMark::default(),
            origin: offset,
        }
    }

    /// The bytes stored. Without `dependencies` only the offset, as builds
    /// before the dependency wrote it and read it.
    pub(crate) fn encode(&self, dependencies: bool) -> bytes::Bytes {
        if !dependencies {
            return bytes::Bytes::copy_from_slice(&self.offset.to_be_bytes());
        }
        let mut out = Vec::with_capacity(DEPENDENT_LEN);
        out.push(DEPENDENT);
        for field in [
            self.offset,
            self.requires.end,
            self.requires.generation,
            self.origin,
        ] {
            out.extend_from_slice(&field.to_be_bytes());
        }
        bytes::Bytes::from(out)
    }

    /// An 8-byte value is a position from before the dependency: it needs
    /// nothing. Anything but the two layouts is refused, since guessing would
    /// put a group at an offset nobody chose.
    pub(crate) fn decode(bytes: &[u8]) -> Result<Self> {
        let word =
            |at: usize| u64::from_be_bytes(bytes[at..at + 8].try_into().expect("eight bytes"));
        match bytes.len() {
            LEGACY_LEN => Ok(Self::placed(word(0))),
            DEPENDENT_LEN if bytes[0] == DEPENDENT => Ok(Self {
                offset: word(1),
                requires: DeadLetterMark {
                    end: word(9),
                    generation: word(17),
                },
                origin: word(25),
            }),
            len => Err(BrokerError::Storage(format!(
                "a consumer group cursor was {len} bytes, not {LEGACY_LEN} or {DEPENDENT_LEN}"
            ))),
        }
    }
}

/// The generation that wrote the record just below `end`, zero for none.
fn generation_at(epochs: &[felix_storage::log::Epoch], end: u64) -> u64 {
    epochs
        .iter()
        .rev()
        .find(|epoch| epoch.start_offset < end)
        .map_or(0, |epoch| epoch.generation)
}
