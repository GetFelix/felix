//! Snapshots between members travel in chunks, as openraft 0.9 sent them.
//!
//! The library now hands over a snapshot in one piece. A member on a
//! release that ran 0.9 only understands the chunk requests, so the sender
//! cuts the piece up ([`super::network`]) and the receiver joins the chunks
//! here before handing the library one piece again.
use super::wire;

/// The most snapshot bytes in one request: 0.9's default chunk size, which
/// the peer route's body limit is sized for.
pub(super) const CHUNK: usize = 3 * 1024 * 1024;

/// The transfer a member is in the middle of receiving.
pub(super) struct Receiving {
    id: String,
    data: Vec<u8>,
}

/// Take one chunk. Returns the whole snapshot once its last chunk arrives.
///
/// A chunk with a new id and offset 0 starts a transfer and drops any older
/// one. A chunk that does not continue the transfer held is refused with the
/// error a 0.9 receiver gave, and the sender starts again.
pub(super) fn receive(
    current: &mut Option<Receiving>,
    chunk: wire::InstallSnapshotRequest,
) -> Result<Option<(wire::SnapshotMeta, Vec<u8>)>, serde_json::Value> {
    let id = chunk.meta.snapshot_id.as_str();
    let continues = current.as_ref().is_some_and(|held| held.id == id);
    if !continues {
        if chunk.offset != 0 {
            return Err(wire::snapshot_mismatch((id, 0), (id, chunk.offset)));
        }
        *current = None;
    }
    let held = current.get_or_insert_with(|| Receiving {
        id: id.to_string(),
        data: Vec::new(),
    });
    // A repeated chunk overwrites what it sent before. One that starts past
    // the bytes held would leave a hole.
    let Some(offset) = usize::try_from(chunk.offset)
        .ok()
        .filter(|offset| *offset <= held.data.len())
    else {
        *current = None;
        return Err(wire::snapshot_mismatch((id, 0), (id, chunk.offset)));
    };
    held.data.truncate(offset);
    held.data.extend_from_slice(&chunk.data);
    if !chunk.done {
        return Ok(None);
    }
    let data = current.take().map(|held| held.data).unwrap_or_default();
    Ok(Some((chunk.meta, data)))
}

/// Cut `data` into the chunk requests of one transfer.
pub(super) fn cut(
    vote: wire::Vote,
    meta: &wire::SnapshotMeta,
    data: &[u8],
    chunk: usize,
) -> impl Iterator<Item = wire::InstallSnapshotRequest> {
    let mut offset = 0;
    let mut finished = false;
    std::iter::from_fn(move || {
        if finished {
            return None;
        }
        let end = (offset + chunk).min(data.len());
        let request = wire::InstallSnapshotRequest {
            vote,
            meta: meta.clone(),
            offset: offset as u64,
            data: data[offset..end].to_vec(),
            done: end == data.len(),
        };
        finished = request.done;
        offset = end;
        Some(request)
    })
}

#[cfg(test)]
mod tests;
