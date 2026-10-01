//! felix-loadgen's payload header (`scenarios/framing.rs`), unchanged: a
//! sequence number and this process's monotonic nanos at publish time.

use std::time::Instant;

const HEADER: usize = 16;

/// A payload of `bytes` bytes (never less than the header) stamped with
/// `seq` and the time since `epoch`.
pub(crate) fn payload(seq: u64, epoch: Instant, bytes: usize) -> Vec<u8> {
    let mut body = vec![0u8; HEADER.max(HEADER + bytes.saturating_sub(HEADER))];
    let len = bytes.max(HEADER);
    body.truncate(len);
    body[0..8].copy_from_slice(&seq.to_be_bytes());
    body[8..16].copy_from_slice(&(epoch.elapsed().as_nanos() as u64).to_be_bytes());
    body
}

pub(crate) fn read_header(payload: &[u8]) -> Option<(u64, u64)> {
    if payload.len() < HEADER {
        return None;
    }
    let seq = u64::from_be_bytes(payload[0..8].try_into().ok()?);
    let t0 = u64::from_be_bytes(payload[8..16].try_into().ok()?);
    Some((seq, t0))
}
