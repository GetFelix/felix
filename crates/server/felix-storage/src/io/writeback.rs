//! How a power loss treats data that was written but not flushed.
//!
//! Shared by the Linux power-loss observer (`power_loss`) and the simulator's
//! in-memory filesystem, so both lose and tear pages by the same rules.

/// The unit a page cache writes back.
pub(crate) const PAGE: usize = 4096;
/// The unit a device writes atomically; a torn page is torn on these.
pub(crate) const SECTOR: usize = 512;

/// How unsynced pages reach the device before the power goes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Writeback {
    /// Any subset of unsynced pages, in any order, each whole, torn, or not at
    /// all. What the kernel is allowed to do.
    AnySubset,
    /// A prefix of each file's unsynced pages, the last one possibly torn.
    /// What an append-only writer usually sees in practice.
    #[cfg_attr(not(any(test, target_os = "linux")), allow(dead_code))]
    InOrder,
}

/// What a file reads back as after the crash, given what its last flush
/// covered and what the page cache held.
pub(crate) fn crash_contents(
    synced: &[u8],
    now: &[u8],
    rng: &mut SplitMix64,
    writeback: Writeback,
) -> Vec<u8> {
    // The size is metadata and travels on its own: it may have reached the
    // device ahead of the data it covers, or not at all.
    let len = if synced.len() == now.len() {
        now.len()
    } else {
        let (low, high) = (synced.len().min(now.len()), synced.len().max(now.len()));
        match rng.below(3) {
            0 => synced.len(),
            1 => now.len(),
            _ => (low + rng.below((high - low) as u64 + 1) as usize).min(high),
        }
    };

    let pages = len.div_ceil(PAGE);
    let dirty: Vec<usize> = (0..pages)
        .filter(|page| page_of(synced, *page) != page_of(now, *page))
        .collect();
    // Under in-order writeback every dirty page before the cut made it, the
    // cut page may be torn, and none after it did.
    let cut = rng.below(dirty.len() as u64 + 1) as usize;

    let mut out = vec![0u8; pages * PAGE];
    for page in 0..pages {
        let old = page_of(synced, page);
        let new = page_of(now, page);
        let target = &mut out[page * PAGE..(page + 1) * PAGE];
        if old == new {
            target.copy_from_slice(&new);
            continue;
        }
        let rank = dirty.iter().position(|p| *p == page).unwrap_or(0);
        let fate = match writeback {
            Writeback::AnySubset => match rng.below(3) {
                0 => Fate::Lost,
                1 => Fate::Written,
                _ => Fate::Torn,
            },
            Writeback::InOrder => match rank.cmp(&cut) {
                std::cmp::Ordering::Less => Fate::Written,
                std::cmp::Ordering::Equal => {
                    if rng.coin() {
                        Fate::Torn
                    } else {
                        Fate::Lost
                    }
                }
                std::cmp::Ordering::Greater => Fate::Lost,
            },
        };
        match fate {
            Fate::Lost => target.copy_from_slice(&old),
            Fate::Written => target.copy_from_slice(&new),
            Fate::Torn => {
                // The leading sectors of the page made it and the rest did not.
                let sectors = 1 + rng.below((PAGE / SECTOR - 1) as u64) as usize;
                let split = sectors * SECTOR;
                target[..split].copy_from_slice(&new[..split]);
                target[split..].copy_from_slice(&old[split..]);
            }
        }
    }
    out.truncate(len);
    out
}

enum Fate {
    Lost,
    Written,
    Torn,
}

/// One page of `bytes`, zero-padded past its end: an unwritten block inside
/// the file size reads as zeros.
pub(crate) fn page_of(bytes: &[u8], page: usize) -> [u8; PAGE] {
    let mut out = [0u8; PAGE];
    let start = page * PAGE;
    if start < bytes.len() {
        let end = bytes.len().min(start + PAGE);
        out[..end - start].copy_from_slice(&bytes[start..end]);
    }
    out
}

/// A small, seedable generator, so a failing crash can be replayed from the
/// seed in its message.
pub(crate) struct SplitMix64(u64);

impl SplitMix64 {
    pub(crate) fn new(seed: u64) -> Self {
        Self(seed)
    }

    pub(crate) fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// Uniform in `0..bound`; zero when `bound` is zero.
    pub(crate) fn below(&mut self, bound: u64) -> u64 {
        if bound == 0 {
            0
        } else {
            self.next_u64() % bound
        }
    }

    pub(crate) fn coin(&mut self) -> bool {
        self.next_u64() & 1 == 1
    }
}

#[cfg(test)]
mod tests;
