//! `<durable root>/controlplane.view`: what the control plane last told this
//! broker, kept so it can start while the control plane is down.
//!
//! This module owns the envelope only: a header, the body the broker hands
//! it, and a CRC. The body's meaning belongs to the broker. A file that is
//! short, damaged, or written by a format this build does not know reads as
//! absent, never as an error, so the worst a bad file can do is make the
//! broker wait for the control plane as it would without one. See
//! `docs/storage-format.md`, "`controlplane.view`".

use std::path::Path;

use crate::Result;

/// The file's name under the durable root. A file, not a directory, so shard
/// discovery passes over it.
pub const FILE_NAME: &str = "controlplane.view";

/// `"FLCV"`: Felix Control-plane View.
const MAGIC: u32 = 0x464C_4356;
const VERSION: u16 = 1;
/// magic(4) + version(2) + body length(4).
const HEADER_LEN: usize = 10;
const CRC_LEN: usize = 4;

/// Replace the file with one holding `body`, durably: written beside it,
/// synced, renamed over it, and the directory synced. A crash leaves the old
/// file or the new one, never a mix.
pub fn record(root: &Path, body: &[u8]) -> Result<()> {
    crate::run_state::write_atomically(root, FILE_NAME, &encode(body), true)
}

/// Remove the file durably, so no later start can use it. Done before a
/// broker deregisters: the control plane may then move its shards to sets
/// without it, and a view of the old sets must not let it stand or vote.
pub fn forget(root: &Path) -> Result<()> {
    match crate::fs::remove_file(root.join(FILE_NAME)) {
        Ok(()) => {}
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
        Err(err) => return Err(err.into()),
    }
    crate::io::sync_dir(root)?;
    Ok(())
}

/// The body last recorded under `root`, or `None` when there is none this
/// build can trust. A read error is logged and read as absent too: the
/// caller then waits for the control plane, which is always safe.
pub fn read(root: &Path) -> Option<Vec<u8>> {
    let path = root.join(FILE_NAME);
    let bytes = match crate::fs::read(&path) {
        Ok(bytes) => bytes,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return None,
        Err(err) => {
            tracing::warn!(path = %path.display(), error = %err, "could not read the control-plane view; ignoring it");
            return None;
        }
    };
    let body = decode(&bytes);
    if body.is_none() {
        tracing::warn!(
            path = %path.display(),
            len = bytes.len(),
            "the control-plane view does not decode; ignoring it"
        );
    }
    body
}

pub(crate) fn encode(body: &[u8]) -> Vec<u8> {
    let len = u32::try_from(body.len()).expect("a control-plane view under 4 GiB");
    let mut out = Vec::with_capacity(HEADER_LEN + body.len() + CRC_LEN);
    out.extend_from_slice(&MAGIC.to_be_bytes());
    out.extend_from_slice(&VERSION.to_be_bytes());
    out.extend_from_slice(&len.to_be_bytes());
    out.extend_from_slice(body);
    let crc = crate::segment::format::crc32(&[&out]);
    out.extend_from_slice(&crc.to_be_bytes());
    out
}

/// The body, or `None` for anything this build did not write whole.
pub(crate) fn decode(bytes: &[u8]) -> Option<Vec<u8>> {
    if bytes.len() < HEADER_LEN + CRC_LEN {
        return None;
    }
    let magic = u32::from_be_bytes(bytes[0..4].try_into().ok()?);
    let version = u16::from_be_bytes(bytes[4..6].try_into().ok()?);
    let len = usize::try_from(u32::from_be_bytes(bytes[6..10].try_into().ok()?)).ok()?;
    // The length must account for every byte, so a file cut short or
    // with bytes after the CRC is refused before the CRC is read.
    if magic != MAGIC || version != VERSION || bytes.len() != HEADER_LEN + len + CRC_LEN {
        return None;
    }
    let (covered, crc) = bytes.split_at(HEADER_LEN + len);
    if crate::segment::format::crc32(&[covered]) != u32::from_be_bytes(crc.try_into().ok()?) {
        return None;
    }
    Some(covered[HEADER_LEN..].to_vec())
}

#[cfg(test)]
mod tests;
