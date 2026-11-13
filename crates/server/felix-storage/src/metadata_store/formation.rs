//! The formation file, `formation`: what a metadata store knows about who
//! set out to form its group.
//!
//! It holds this member's own mark, whether the member has voted from this
//! store, and the ids of the other members that asked it to record theirs.
//! A member that a peer has recorded, and that holds no mark, lost its
//! volume after it may have voted. One that has voted and finds its log
//! blank lost its log. The rules are in `docs/metadata-raft-design.md`,
//! "Forming the group".
//!
//! The file is written like the member file, through
//! [`super::MemberFile`], and is absent until the first mark or record. A
//! file that does not decode fails the open: read as "no file", it would
//! forget a record another member's vote depends on.

use std::collections::BTreeSet;
use std::io;
use std::path::Path;

use crate::raft_log::state_file;

/// `"FLFM"`.
const MAGIC: u32 = 0x464C_464D;
const VERSION: u16 = 1;
/// magic(4) + version(2) + flags(2) + id count(2) + crc(4)
const HEADER_LEN: usize = 14;
pub(super) const FILE_NAME: &str = "formation";

const FORMING: u16 = 1 << 0;
const REACHED: u16 = 1 << 1;
const KNOWN_FLAGS: u16 = FORMING | REACHED;

/// What the formation file holds. A store without the file holds the
/// default: no mark and no records.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Formation {
    /// This member set out to form the group from this store.
    pub forming: bool,
    /// This member has voted, or may have, from this store. The log keeps
    /// the vote in its own directory, so this is what tells a log that was
    /// emptied from one that never held anything.
    pub reached: bool,
    /// The members that asked this one to record that they set out to
    /// form the group.
    pub formed: BTreeSet<u64>,
}

/// The formation file in `dir`, or the default when the store has none.
pub(super) fn read(dir: &Path) -> io::Result<Formation> {
    let Some(bytes) = state_file::load(dir, FILE_NAME)? else {
        return Ok(Formation::default());
    };
    decode(&bytes).map_err(|why| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("{}: {why}", dir.join(FILE_NAME).display()),
        )
    })
}

pub(super) fn encode(formation: &Formation) -> io::Result<Vec<u8>> {
    let count = u16::try_from(formation.formed.len()).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "over 65535 members recorded as forming",
        )
    })?;
    let mut flags = 0;
    if formation.forming {
        flags |= FORMING;
    }
    if formation.reached {
        flags |= REACHED;
    }
    let ids: Vec<u8> = formation
        .formed
        .iter()
        .flat_map(|id| id.to_be_bytes())
        .collect();
    let mut out = Vec::with_capacity(HEADER_LEN + ids.len());
    out.extend_from_slice(&MAGIC.to_be_bytes());
    out.extend_from_slice(&VERSION.to_be_bytes());
    out.extend_from_slice(&flags.to_be_bytes());
    out.extend_from_slice(&count.to_be_bytes());
    let crc = crate::segment::format::crc32(&[&out, &ids]);
    out.extend_from_slice(&crc.to_be_bytes());
    out.extend_from_slice(&ids);
    Ok(out)
}

/// The formation `bytes` hold, or why they are not a formation file this
/// build reads.
pub(super) fn decode(bytes: &[u8]) -> Result<Formation, String> {
    let damaged = || "the formation file is damaged".to_string();
    let header = bytes.get(..HEADER_LEN).ok_or_else(damaged)?;
    let u16_at = |at: usize| u16::from_be_bytes([header[at], header[at + 1]]);
    if header[0..4] != MAGIC.to_be_bytes() {
        return Err(damaged());
    }
    // Before the checksum: another version may lay the file out another way.
    let version = u16_at(4);
    if version != VERSION {
        return Err(format!(
            "the formation file is format version {version}, and this build reads version \
             {VERSION}"
        ));
    }
    let (flags, count) = (u16_at(6), usize::from(u16_at(8)));
    let body = &bytes[HEADER_LEN..];
    let (ids, rest) = body.as_chunks::<8>();
    let crc = u32::from_be_bytes([header[10], header[11], header[12], header[13]]);
    if ids.len() != count
        || !rest.is_empty()
        || crate::segment::format::crc32(&[&header[..10], body]) != crc
    {
        return Err(damaged());
    }
    if flags & !KNOWN_FLAGS != 0 {
        return Err(format!(
            "the formation file sets flags this build does not know ({flags:#06x})"
        ));
    }
    // Written ascending and without repeats; anything else is no build's.
    // Big-endian ids order as their bytes do.
    if !ids.is_sorted_by(|a, b| a < b) {
        return Err(damaged());
    }
    let formed = ids.iter().map(|id| u64::from_be_bytes(*id)).collect();
    Ok(Formation {
        forming: flags & FORMING != 0,
        reached: flags & REACHED != 0,
        formed,
    })
}
