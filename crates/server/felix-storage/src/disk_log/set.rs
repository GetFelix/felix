//! Which replicas this one counts as the shard's set.
//!
//! A replica answers a fence or a batch at the set of an assignment, and a
//! restart must not let it forget which set that was. This file keeps the
//! assignment's generation and members, and while a change is pending the set
//! it is leaving, so both majorities stay checkable. See
//! `docs/replication-design.md`, "Electing without the control plane".
//!
//! A file of its own beside `ballot`: a build that predates it ignores it, so
//! rolling back still opens the shard. A missing file means no set was ever
//! recorded, which is every shard on an older build.
//!
//! Like `ballot` it cannot be rebuilt, so a file that does not decode fails
//! the open instead of reading as no set: a replica that forgot its set would
//! count the wrong majority.
//!
//! The set is written after the ballot when both change, so a set newer than
//! the accepted generation at open is a write that order rules out; the open
//! raises the generation to it rather than trust the older one.
//!
//! The file also keeps this copy's wiped mark apart from the member lists: a
//! newer set may list the copy only as leaving, or not at all, and name it
//! again later, and none of that may end the mark.
//!
//! `DiskLog::accept_set` and the open's reading of the file live here.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::Ordering;

use super::{DiskLog, SetCheck};
use crate::io::sync_dir;
use crate::{Result, StorageError, StorageId};

/// `"FLRS"`.
const MAGIC: u32 = 0x464C_5253;
const VERSION: u16 = 1;
/// magic(4) + version(2) + flags(2), then the set, the set it is leaving and
/// the wiped mark when their flags are set, then a crc(4) over everything
/// before it.
pub(super) const HEADER_LEN: usize = 8;
/// The flags bit for a set being left.
const FLAG_LEAVING: u16 = 1;
/// The flags bit for a wiped mark: node length(2), the node id, learner(1).
const FLAG_WIPED: u16 = 2;
/// Member flags: a storage id follows; the member does not vote; this copy
/// found it lost what it held.
const MEMBER_STORAGE: u8 = 1;
const MEMBER_LEARNER: u8 = 2;
const MEMBER_WIPED: u8 = 4;
/// Only bounds what a damaged count can claim.
pub(super) const MAX_MEMBERS: usize = 64;
/// Node ids are short; this only bounds what a damaged length can claim.
pub(super) const MAX_NODE_LEN: usize = 1024;
/// Makes each write's temporary file its own.
static WRITES: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
const FILE_NAME: &str = "replica-set";
/// Prefix of the temporaries `store` writes: `replica-set.tmp.<pid>.<n>`.
const TEMPORARY_PREFIX: &str = "replica-set.tmp";

/// The accepted generation and the leader it was accepted from, as the open
/// reads them from the ballot.
type Accepted = (u64, Option<Arc<str>>);

/// What the file holds: the set and this copy's wiped mark.
pub(super) type Kept = (ReplicaSet, Option<WipedMark>);

/// A replica in a set: its node id and the storage identity the fence counts
/// it under, when placement named one.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Member {
    pub node: String,
    pub storage: Option<StorageId>,
    /// Listed but not counted: re-admitted and not yet caught up.
    pub learner: bool,
    /// Set by a copy on its own entry when it finds it lost what it held
    /// (a wipe), as against a learner placement staged. Ends only at
    /// re-admission, so a promotion can tell the two apart.
    pub wiped: bool,
}

/// This copy's mark while it is wiped: the node id it answers as, and
/// whether it is still a learner on this log. Held beside the set, not in it,
/// so that only [`DiskLog::admit_member`] ends it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct WipedMark {
    pub(super) node: String,
    pub(super) learner: bool,
}

/// One assignment's replicas.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Membership {
    pub generation: u64,
    pub members: Vec<Member>,
}

/// The set a replica answers at, and the one it is leaving while a change is
/// pending. Cleared by writing a set without it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicaSet {
    pub current: Membership,
    pub leaving: Option<Membership>,
}

impl ReplicaSet {
    /// Fails for a set `decode` would refuse, so the file never holds one.
    pub(super) fn encode(&self, wiped: Option<&WipedMark>) -> Result<Vec<u8>> {
        self.check()?;
        if wiped.is_some_and(|mark| mark.node.len() > MAX_NODE_LEN) {
            return Err(invalid("a wiped mark past the size the file allows"));
        }
        let flags = (u16::from(self.leaving.is_some()) * FLAG_LEAVING)
            | (u16::from(wiped.is_some()) * FLAG_WIPED);
        let mut out = Vec::with_capacity(HEADER_LEN + 64);
        out.extend_from_slice(&MAGIC.to_be_bytes());
        out.extend_from_slice(&VERSION.to_be_bytes());
        out.extend_from_slice(&flags.to_be_bytes());
        self.current.encode_into(&mut out);
        if let Some(leaving) = &self.leaving {
            leaving.encode_into(&mut out);
        }
        if let Some(mark) = wiped {
            out.extend_from_slice(&(mark.node.len() as u16).to_be_bytes());
            out.extend_from_slice(mark.node.as_bytes());
            out.push(u8::from(mark.learner));
        }
        let crc = crate::segment::format::crc32(&[&out]);
        out.extend_from_slice(&crc.to_be_bytes());
        Ok(out)
    }

    pub(super) fn decode(bytes: &[u8]) -> Option<Kept> {
        let body_len = bytes.len().checked_sub(4)?;
        let (body, crc) = bytes.split_at(body_len);
        if body.len() < HEADER_LEN
            || u32::from_be_bytes(body[0..4].try_into().ok()?) != MAGIC
            || u16::from_be_bytes(body[4..6].try_into().ok()?) != VERSION
            || crate::segment::format::crc32(&[body]) != u32::from_be_bytes(crc.try_into().ok()?)
        {
            return None;
        }
        let flags = u16::from_be_bytes(body[6..8].try_into().ok()?);
        if flags & !(FLAG_LEAVING | FLAG_WIPED) != 0 {
            return None;
        }
        let mut rest = &body[HEADER_LEN..];
        let current = Membership::decode_from(&mut rest)?;
        let leaving = if flags & FLAG_LEAVING != 0 {
            Some(Membership::decode_from(&mut rest)?)
        } else {
            None
        };
        let wiped = if flags & FLAG_WIPED != 0 {
            let len = u16::from_be_bytes(take(&mut rest, 2)?.try_into().ok()?) as usize;
            if len > MAX_NODE_LEN {
                return None;
            }
            let node = String::from_utf8(take(&mut rest, len)?.to_vec()).ok()?;
            let learner = match take(&mut rest, 1)?[0] {
                0 => false,
                1 => true,
                _ => return None,
            };
            Some(WipedMark { node, learner })
        } else {
            None
        };
        // Slack would let two byte strings mean one set.
        let set = Self { current, leaving };
        (rest.is_empty() && set.check().is_ok()).then_some((set, wiped))
    }

    /// What both `encode` and `decode` insist on: sizes within the file's
    /// limits, no member listed twice, and a set being left that is older than
    /// the one replacing it.
    fn check(&self) -> Result<()> {
        self.current.check()?;
        if let Some(leaving) = &self.leaving {
            leaving.check()?;
            if leaving.generation >= self.current.generation {
                return Err(invalid(
                    "a set being left must be older than the current one",
                ));
            }
        }
        Ok(())
    }
}

impl DiskLog {
    /// The generation of the replica set this log counts, or zero if it has
    /// never kept one. Lock-free, for the replication hot path.
    pub fn set_generation(&self) -> u64 {
        self.inner.set_generation.load(Ordering::Acquire)
    }

    /// The replica set this log counts, if it has kept one.
    pub fn replica_set(&self) -> Option<ReplicaSet> {
        self.inner.replica_set.read().clone()
    }

    /// Count `set` from here on if it is newer than the set already kept,
    /// persisting it first. A member marked wiped, in the set or the set it
    /// is leaving, marks this copy wiped until [`Self::admit_member`] settles
    /// it, whoever writes a newer set and whether or not that set lists it.
    /// The mark is copied onto the member's entry in each newer set.
    ///
    /// Returns once a raised set is on disk, so a caller that answers at it
    /// afterwards has made a promise that survives a restart. A caller that
    /// also accepts a generation persists the ballot first: the open treats a
    /// set ahead of the ballot as a write that order rules out.
    pub async fn accept_set(&self, mut set: ReplicaSet) -> Result<SetCheck> {
        if let Some(found) = check_set(self.set_generation(), set.current.generation) {
            return Ok(found);
        }
        let inner = Arc::clone(&self.inner);
        tokio::task::spawn_blocking(move || {
            // The ballot's writer lock: it serializes writes per shard, so
            // the file only ever moves forward in generation.
            let _persisted = inner.replica_persisted.lock();
            let known = inner.set_generation.load(Ordering::Acquire);
            if let Some(found) = check_set(known, set.current.generation) {
                return Ok(found);
            }
            let mut wiped = inner.wiped.read().clone();
            let listed = set.leaving.iter().flat_map(|leaving| &leaving.members);
            for member in set.current.members.iter().chain(listed) {
                if !member.wiped {
                    continue;
                }
                match &mut wiped {
                    Some(mark) if mark.node == member.node => mark.learner |= member.learner,
                    _ => {
                        wiped = Some(WipedMark {
                            node: member.node.clone(),
                            learner: member.learner,
                        });
                    }
                }
            }
            if let Some(mark) = &wiped {
                for member in &mut set.current.members {
                    if member.node == mark.node {
                        member.wiped = true;
                        member.learner |= mark.learner;
                    }
                }
            }
            store(&inner.dir, &set, wiped.as_ref())?;
            let generation = set.current.generation;
            // The mark first: a reader that sees the new set sees it too.
            *inner.wiped.write() = wiped;
            *inner.replica_set.write() = Some(set);
            inner.set_generation.store(generation, Ordering::Release);
            Ok(SetCheck::Raised)
        })
        .await
        .map_err(|err| StorageError::Io(std::io::Error::other(err)))?
    }
}

impl DiskLog {
    /// Whether the kept set lists `node` as a learner: `None` with no set or
    /// no such member. A wiped mark naming `node` a learner counts whether
    /// the set lists it or not. Allocation-free, for the replication hot path.
    pub fn member_is_learner(&self, node: &str) -> Option<bool> {
        if self.wiped_mark_on(node) == Some(true) {
            return Some(true);
        }
        self.inner
            .replica_set
            .read()
            .as_ref()?
            .current
            .members
            .iter()
            .find(|member| member.node == node)
            .map(|member| member.learner)
    }

    /// Whether this copy is marked wiped as `node`: it found it lost what it
    /// held, and has not settled since. False with no such mark.
    pub fn member_is_wiped(&self, node: &str) -> bool {
        self.wiped_mark_on(node).is_some()
    }

    /// Whether the wiped mark names `node` a learner, or `None` with no mark
    /// on it.
    fn wiped_mark_on(&self, node: &str) -> Option<bool> {
        self.inner
            .wiped
            .read()
            .as_ref()
            .filter(|mark| mark.node == node)
            .map(|mark| mark.learner)
    }

    /// Count `node` as a voter in the set already kept, at the same
    /// generation, persisting first, and with `settled` drop its wiped mark
    /// too. `false` when there was nothing to change.
    ///
    /// The one way either mark ends: a replica re-admitted by a set change
    /// stays out of every majority until it has caught up, and only the
    /// caller knows when.
    pub async fn admit_member(&self, node: &str, settled: bool) -> Result<bool> {
        let inner = Arc::clone(&self.inner);
        let node = node.to_string();
        tokio::task::spawn_blocking(move || {
            let _persisted = inner.replica_persisted.lock();
            let Some(mut set) = inner.replica_set.read().clone() else {
                return Ok(false);
            };
            let mut wiped = inner.wiped.read().clone();
            let mut changed = false;
            if let Some(member) =
                set.current.members.iter_mut().find(|member| {
                    member.node == node && (member.learner || (settled && member.wiped))
                })
            {
                member.learner = false;
                member.wiped &= !settled;
                changed = true;
            }
            if let Some(mark) = wiped.as_mut().filter(|mark| mark.node == node) {
                changed |= mark.learner || settled;
                mark.learner = false;
                if settled {
                    wiped = None;
                }
            }
            if !changed {
                return Ok(false);
            }
            store(&inner.dir, &set, wiped.as_ref())?;
            *inner.replica_set.write() = Some(set);
            *inner.wiped.write() = wiped;
            Ok(true)
        })
        .await
        .map_err(|err| StorageError::Io(std::io::Error::other(err)))?
    }
}

/// What a set at `generation` is against the one kept at `known`, or `None`
/// when it is newer and needs a write. Zero is no set, so a set at generation
/// zero is never kept.
fn check_set(known: u64, generation: u64) -> Option<SetCheck> {
    match generation.cmp(&known) {
        std::cmp::Ordering::Less => Some(SetCheck::Stale { known }),
        std::cmp::Ordering::Equal => Some(SetCheck::Current),
        std::cmp::Ordering::Greater => None,
    }
}

/// The set a shard opens with, and the accepted generation once the set is
/// taken into account: a set newer than the ballot was written out of order,
/// and taking its generation only refuses more.
pub(super) fn reconcile(
    dir: &Path,
    label: &str,
    accepted: Accepted,
) -> Result<(Option<Kept>, Accepted)> {
    // A shard that never kept a set pays one stat, not a directory scan. A
    // temporary from a crash during the very first write waits for the next
    // open after a set exists; `load` ignores it meanwhile.
    if path_in(dir).exists() {
        discard_leftovers(dir)?;
    }
    let set = load(dir)?;
    let accepted = match &set {
        Some((set, _)) if set.current.generation > accepted.0 => {
            tracing::warn!(
                shard = %label,
                set_generation = set.current.generation,
                accepted_generation = accepted.0,
                "the replica set is newer than the accepted generation; raising it with no leader named"
            );
            (set.current.generation, None)
        }
        _ => accepted,
    };
    Ok((set, accepted))
}

impl Membership {
    /// generation(8) + count(2), then per member: node length(2), the node id,
    /// flags(1) and, when bit 0 is set, the storage id(16).
    fn encode_into(&self, out: &mut Vec<u8>) {
        out.extend_from_slice(&self.generation.to_be_bytes());
        out.extend_from_slice(&(self.members.len() as u16).to_be_bytes());
        for member in &self.members {
            out.extend_from_slice(&(member.node.len() as u16).to_be_bytes());
            out.extend_from_slice(member.node.as_bytes());
            let flags = (u8::from(member.storage.is_some()) * MEMBER_STORAGE)
                | (u8::from(member.learner) * MEMBER_LEARNER)
                | (u8::from(member.wiped) * MEMBER_WIPED);
            out.push(flags);
            if let Some(id) = &member.storage {
                out.extend_from_slice(&id.to_bytes());
            }
        }
    }

    fn decode_from(rest: &mut &[u8]) -> Option<Self> {
        let generation = u64::from_be_bytes(take(rest, 8)?.try_into().ok()?);
        let count = u16::from_be_bytes(take(rest, 2)?.try_into().ok()?) as usize;
        if count > MAX_MEMBERS {
            return None;
        }
        let mut members = Vec::with_capacity(count);
        for _ in 0..count {
            let len = u16::from_be_bytes(take(rest, 2)?.try_into().ok()?) as usize;
            if len > MAX_NODE_LEN {
                return None;
            }
            let node = String::from_utf8(take(rest, len)?.to_vec()).ok()?;
            let flags = take(rest, 1)?[0];
            if flags & !(MEMBER_STORAGE | MEMBER_LEARNER | MEMBER_WIPED) != 0 {
                return None;
            }
            let storage = if flags & MEMBER_STORAGE != 0 {
                Some(StorageId::from_bytes(take(rest, 16)?.try_into().ok()?))
            } else {
                None
            };
            members.push(Member {
                node,
                storage,
                learner: flags & MEMBER_LEARNER != 0,
                wiped: flags & MEMBER_WIPED != 0,
            });
        }
        Some(Self {
            generation,
            members,
        })
    }

    fn check(&self) -> Result<()> {
        let too_long = self.members.iter().any(|m| m.node.len() > MAX_NODE_LEN);
        if self.members.len() > MAX_MEMBERS || too_long {
            return Err(invalid("a replica set past the size the file allows"));
        }
        // Counting a member twice would count a majority that isn't one.
        let mut nodes = std::collections::HashSet::new();
        let mut storage = std::collections::HashSet::new();
        for member in &self.members {
            if !nodes.insert(member.node.as_str()) {
                return Err(invalid("a node listed twice in a replica set"));
            }
            if let Some(id) = member.storage
                && !storage.insert(id)
            {
                return Err(invalid("a storage id listed twice in a replica set"));
            }
        }
        Ok(())
    }
}

fn invalid(what: &str) -> StorageError {
    StorageError::Io(std::io::Error::new(std::io::ErrorKind::InvalidInput, what))
}

fn take<'a>(rest: &mut &'a [u8], n: usize) -> Option<&'a [u8]> {
    let (head, tail) = rest.split_at_checked(n)?;
    *rest = tail;
    Some(head)
}

/// Read a shard's replica set and wiped mark, or `None` if it has never kept
/// a set.
pub(super) fn load(dir: &Path) -> Result<Option<Kept>> {
    let path = path_in(dir);
    match std::fs::read(&path) {
        Ok(bytes) => ReplicaSet::decode(&bytes).map(Some).ok_or_else(|| {
            StorageError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "{} does not decode; it names the replicas this one counts \
                     and cannot be rebuilt from the log",
                    path.display()
                ),
            ))
        }),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(err) => Err(StorageError::Io(err)),
    }
}

/// Write a shard's replica set durably, as `ballot::store` writes a ballot: a
/// crash leaves the old set or the new one. A caller that answers at the set
/// afterwards has made a promise that survives a restart.
///
/// Blocking, with several fsyncs: call it from `spawn_blocking`. It takes no
/// lock, so writers on different shards never wait on each other, and each
/// write has a temporary of its own. Two stores to the same shard are not
/// ordered here (the later rename wins), so the caller must serialize them per
/// shard and persist in generation order, as `ballot`'s caller does under
/// `replica_persisted`.
pub(super) fn store(dir: &Path, set: &ReplicaSet, wiped: Option<&WipedMark>) -> Result<()> {
    use std::io::Write;

    let bytes = set.encode(wiped)?;
    let path = path_in(dir);
    let n = WRITES.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let temporary = dir.join(format!("{TEMPORARY_PREFIX}.{}.{n}", std::process::id()));
    {
        let mut file = std::fs::File::create(&temporary).map_err(StorageError::Io)?;
        file.write_all(&bytes).map_err(StorageError::Io)?;
        crate::io::sync_all(&file).map_err(StorageError::Io)?;
    }
    std::fs::rename(&temporary, &path).map_err(StorageError::Io)?;
    sync_dir(dir).map_err(StorageError::Io)?;
    Ok(())
}

/// Remove temporaries a crashed `store` left. For the open, before any writer
/// runs: a sweep during a write would delete that write's temporary. `load`
/// ignores them either way.
pub(super) fn discard_leftovers(dir: &Path) -> Result<()> {
    for entry in std::fs::read_dir(dir).map_err(StorageError::Io)? {
        let entry = entry.map_err(StorageError::Io)?;
        if entry
            .file_name()
            .to_string_lossy()
            .starts_with(TEMPORARY_PREFIX)
        {
            std::fs::remove_file(entry.path()).map_err(StorageError::Io)?;
        }
    }
    Ok(())
}

fn path_in(dir: &Path) -> PathBuf {
    dir.join(FILE_NAME)
}

#[cfg(test)]
mod tests;
