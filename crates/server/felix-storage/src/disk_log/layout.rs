//! Mapping a `ShardKey` onto a directory on disk.
//!
//! Tenant, namespace and stream names come from users, so they cannot be pasted
//! into a path unescaped: `../` would escape the data root, and case-insensitive
//! filesystems would collide `Orders` with `orders`. The directory name is
//! therefore a *readable but lossy* rendering of the key plus a hash of the exact
//! key, which restores uniqueness.

use std::path::{Path, PathBuf};

use crate::log::ShardKey;

/// Longest run of a single key component kept in the readable prefix.
const MAX_COMPONENT_CHARS: usize = 32;

/// Full path to a shard's segment directory under `root`, for a stream from
/// before incarnations (incarnation 0).
pub fn shard_dir(root: &Path, shard: &ShardKey) -> PathBuf {
    root.join(shard_dir_name(shard))
}

/// Full path to the segment directory of one incarnation of a shard.
pub fn incarnation_dir(root: &Path, shard: &ShardKey, incarnation: u64) -> PathBuf {
    root.join(incarnation_dir_name(shard, incarnation))
}

/// Directory name for one incarnation of a shard: [`shard_dir_name`], plus
/// the incarnation when it is not 0. Incarnation 0 keeps the name a shard has
/// always had, so nothing on disk moves on upgrade.
pub fn incarnation_dir_name(shard: &ShardKey, incarnation: u64) -> String {
    let name = shard_dir_name(shard);
    if incarnation == 0 {
        name
    } else {
        format!("{name}-{incarnation:016x}")
    }
}

/// Directory name for a shard: readable prefix plus a hash of the exact key.
pub fn shard_dir_name(shard: &ShardKey) -> String {
    // Length-prefixed so that ("a", "bc") and ("ab", "c") cannot hash alike.
    let mut material = Vec::new();
    for part in [
        shard.tenant.as_str(),
        shard.namespace.as_str(),
        shard.stream.as_str(),
    ] {
        material.extend_from_slice(&(part.len() as u64).to_be_bytes());
        material.extend_from_slice(part.as_bytes());
    }
    material.extend_from_slice(&shard.shard.to_be_bytes());

    format!(
        "{}_{}_{}_{}-{:016x}",
        readable(&shard.tenant),
        readable(&shard.namespace),
        readable(&shard.stream),
        shard.shard,
        fnv1a(&material)
    )
}

/// File name for a stream as a whole (its incarnation marker): the readable
/// prefix and a hash of the exact tenant, namespace and stream.
pub fn stream_dir_name(tenant: &str, namespace: &str, stream: &str) -> String {
    let mut material = Vec::new();
    for part in [tenant, namespace, stream] {
        material.extend_from_slice(&(part.len() as u64).to_be_bytes());
        material.extend_from_slice(part.as_bytes());
    }
    format!(
        "{}_{}_{}-{:016x}",
        readable(tenant),
        readable(namespace),
        readable(stream),
        fnv1a(&material)
    )
}

/// The shard and incarnation a directory named `name` holds, if it is one of
/// `tenant/namespace/stream`'s.
///
/// Exact: the shard number is read from the name and the name computed again
/// from it, so a lossy readable prefix shared with another stream never
/// matches.
pub fn stream_shard_of(
    name: &str,
    tenant: &str,
    namespace: &str,
    stream: &str,
) -> Option<(u32, u64)> {
    let key = |shard| ShardKey {
        tenant: tenant.to_string(),
        namespace: namespace.to_string(),
        stream: stream.to_string(),
        shard,
    };
    // `<base>` or `<base>-<incarnation>`, where `<base>` ends `_<shard>-<hash>`.
    let mut candidates = vec![(name, 0)];
    if let Some((base, incarnation)) = name.rsplit_once('-')
        && incarnation.len() == 16
        && let Ok(incarnation) = u64::from_str_radix(incarnation, 16)
    {
        candidates.push((base, incarnation));
    }
    candidates.into_iter().find_map(|(base, incarnation)| {
        let (prefix, _hash) = base.rsplit_once('-')?;
        let (_, shard) = prefix.rsplit_once('_')?;
        let shard: u32 = shard.parse().ok()?;
        (incarnation_dir_name(&key(shard), incarnation) == name).then_some((shard, incarnation))
    })
}

/// The readable part every shard directory of `tenant/namespace/stream`
/// starts with. Lossy, so streams can share it; see [`stream_shard_of`].
pub(crate) fn stream_prefix(tenant: &str, namespace: &str, stream: &str) -> String {
    format!(
        "{}_{}_{}",
        readable(tenant),
        readable(namespace),
        readable(stream)
    )
}

/// The readable prefix of a shard directory's name, if it has the shape of
/// one: the name without `_<shard>-<hash>` and any `-<incarnation>`.
pub(crate) fn prefix_of(name: &str) -> Option<&str> {
    let is_hex16 = |part: &str| part.len() == 16 && part.bytes().all(|b| b.is_ascii_hexdigit());
    let base = match name.rsplit_once('-') {
        Some((base, last)) if is_hex16(last) => match base.rsplit_once('-') {
            // `<prefix>_<shard>-<hash>-<incarnation>`
            Some((inner, hash))
                if is_hex16(hash)
                    && inner
                        .rsplit_once('_')
                        .is_some_and(|(_, shard)| shard.parse::<u32>().is_ok()) =>
            {
                inner
            }
            // `<prefix>_<shard>-<hash>`
            _ => base,
        },
        _ => return None,
    };
    let (prefix, shard) = base.rsplit_once('_')?;
    shard.parse::<u32>().ok()?;
    Some(prefix)
}

/// Human-readable shard identifier used in errors, logs and metrics.
///
/// Unlike the directory name this is not required to be unique or
/// filesystem-safe — it exists so a corruption report names the stream an
/// operator recognises.
pub fn shard_label(shard: &ShardKey) -> String {
    format!(
        "{}/{}/{}/{}",
        shard.tenant, shard.namespace, shard.stream, shard.shard
    )
}

fn readable(component: &str) -> String {
    let mut out: String = component
        .chars()
        .map(|c| if is_safe(c) { c } else { '_' })
        .take(MAX_COMPONENT_CHARS)
        .collect();
    if out.is_empty() {
        out.push('_');
    }
    out
}

/// Characters kept verbatim in a directory name. Everything else becomes `_`.
///
/// `.` is deliberately excluded along with `/`: with no dots in the readable
/// prefix, a name like `..` is not merely escaped but unrepresentable.
fn is_safe(c: char) -> bool {
    c.is_ascii_alphanumeric() || c == '-'
}

/// FNV-1a over the exact key bytes.
///
/// Deliberately not `DefaultHasher`: that is explicitly not stable across Rust
/// releases, and a directory name that changes when the toolchain changes would
/// orphan every existing segment on disk.
fn fnv1a(bytes: &[u8]) -> u64 {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash
}

#[cfg(test)]
mod tests;
