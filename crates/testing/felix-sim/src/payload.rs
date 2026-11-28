//! The payload a simulated client appends: its value, and in a power-loss run
//! padding that lets a read tell an intact record from a damaged one.

use felix_cluster::history::model::FOREIGN_PAYLOAD;

/// The payload of an append of `value`: the value, and with `padded` padding
/// to 1 to 4 KiB, so an append spans several sectors and a cut can lose one
/// sector of it before another that arrived.
#[cfg(test)]
fn payload(value: u64, padded: bool) -> bytes::Bytes {
    let mut out = value.to_le_bytes().to_vec();
    if padded {
        out.extend(padding(value));
    }
    bytes::Bytes::from(out)
}

/// The padding after `value`, a function of the value alone, so a read can
/// tell an intact record from one with damaged bytes.
fn padding(value: u64) -> impl Iterator<Item = u8> {
    let mut mix = value.wrapping_add(0x9e37_79b9_7f4a_7c15);
    mix = (mix ^ (mix >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    mix ^= mix >> 31;
    let len = 1024 - 8 + (mix % 3073) as usize;
    (0..len).map(move |i| (mix >> (8 * (i % 8))) as u8 ^ i as u8)
}

/// The value a payload carries, or [`FOREIGN_PAYLOAD`] for one no client
/// wrote or one with damaged bytes.
pub(crate) fn decode(payload: &[u8]) -> u64 {
    let Some(value) = payload
        .get(..8)
        .and_then(|head| head.try_into().ok())
        .map(u64::from_le_bytes)
    else {
        return FOREIGN_PAYLOAD;
    };
    let rest = &payload[8..];
    if rest.is_empty() || rest.iter().copied().eq(padding(value)) {
        value
    } else {
        FOREIGN_PAYLOAD
    }
}

#[cfg(test)]
mod tests;
