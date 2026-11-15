//! The placement incarnation file in the data root: the highest control-plane
//! restore incarnation this broker has acknowledged.
//!
//! The control plane sends no target or copy delete after a restore until
//! every broker has acknowledged its new incarnation, and a broker refuses
//! both from an older one. That only holds if the acknowledgement is on disk
//! before it is sent and never goes back, so a raise is synced before it
//! returns and a lower value is never written.

use std::io;
use std::path::Path;

use crate::raft_log::state_file;

/// The file's name in the data root.
pub(crate) const FILE_NAME: &str = "raft-placement-incarnation";
/// `"FLPI"`.
const MAGIC: u32 = 0x464C_5049;
const VERSION: u16 = 1;
/// magic(4) + version(2) + incarnation(8) + crc(4) over the rest.
const ENCODED_LEN: usize = 18;

fn encode(incarnation: u64) -> [u8; ENCODED_LEN] {
    let mut out = [0u8; ENCODED_LEN];
    out[0..4].copy_from_slice(&MAGIC.to_be_bytes());
    out[4..6].copy_from_slice(&VERSION.to_be_bytes());
    out[6..14].copy_from_slice(&incarnation.to_be_bytes());
    let crc = crate::segment::format::crc32(&[&out[..14]]);
    out[14..].copy_from_slice(&crc.to_be_bytes());
    out
}

fn decode(bytes: &[u8]) -> Option<u64> {
    if bytes.len() != ENCODED_LEN {
        return None;
    }
    let crc = u32::from_be_bytes(bytes[14..].try_into().ok()?);
    if crate::segment::format::crc32(&[&bytes[..14]]) != crc
        || u32::from_be_bytes(bytes[0..4].try_into().ok()?) != MAGIC
        || u16::from_be_bytes(bytes[4..6].try_into().ok()?) != VERSION
    {
        return None;
    }
    Some(u64::from_be_bytes(bytes[6..14].try_into().ok()?))
}

/// The highest incarnation acknowledged under `root`, 0 when none was. A
/// file that does not decode is an error: reading it as 0 would let a
/// delete from before a restore through.
pub fn acked_placement_incarnation(root: &Path) -> io::Result<u64> {
    let Some(bytes) = state_file::load(root, FILE_NAME)? else {
        return Ok(0);
    };
    decode(&bytes).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "{}: the placement incarnation file does not decode",
                root.join(FILE_NAME).display()
            ),
        )
    })
}

/// Acknowledge `incarnation` under `root`, synced before it returns. Returns
/// the acknowledged incarnation, which is never below one acknowledged
/// before: a lower `incarnation` writes nothing.
pub fn ack_placement_incarnation(root: &Path, incarnation: u64) -> io::Result<u64> {
    // Two raises racing between the read and the write could leave the lower one.
    static RAISE: std::sync::Mutex<()> = std::sync::Mutex::new(());
    let _raising = RAISE
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let acked = acked_placement_incarnation(root)?;
    if incarnation <= acked {
        return Ok(acked);
    }
    state_file::store(root, FILE_NAME, &[&encode(incarnation)])?;
    Ok(incarnation)
}

/// The incarnation a placement restore writes: above the one being replaced
/// and the one the backup holds. Reusing either would let a target issued
/// after the backup, or a delete told then, pass the version rule again.
pub fn restored_placement_incarnation(current: u64, restored: u64) -> u64 {
    current.max(restored).saturating_add(1)
}

/// Whether the control plane may send a target or a copy delete at
/// `incarnation`: every broker in `brokers` has durably acknowledged it. A
/// broker with no acknowledgement in `acked` counts as 0, as every broker
/// starts: that passes only before the first restore.
pub fn all_acked_placement_incarnation<'a, B: Ord + 'a>(
    incarnation: u64,
    brokers: impl IntoIterator<Item = &'a B>,
    acked: &std::collections::BTreeMap<B, u64>,
) -> bool {
    brokers
        .into_iter()
        .all(|broker| acked.get(broker).copied().unwrap_or(0) >= incarnation)
}

/// Whether a copy delete the control plane told at incarnation `told` is
/// void on a broker that has seen `seen`: its acknowledgement, or the
/// highest incarnation of any membership in the copy's log, whichever is
/// higher. A restore may have placed the copy again since the delete.
pub fn copy_delete_is_void(told: u64, seen: u64) -> bool {
    told < seen
}
