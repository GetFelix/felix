//! The member file, `member`: which cluster a metadata store belongs to,
//! whether the member's vote is withheld, and the two marks the upgrade
//! from redb uses.
//!
//! The file is replaced whole: a temporary, an fsync, a rename and a
//! directory sync, all before a change returns. A crash leaves the old file
//! or the new one. Nothing can rebuild it, so a file that does not decode
//! fails the open and is never read as "no file": that would give back a
//! withheld vote and forget that the member has started.

use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use super::LOG_DIR;
use crate::raft_log::{snapshot, state_file};

/// `"FLMB"`.
const MAGIC: u32 = 0x464C_4D42;
const VERSION: u16 = 1;
/// magic(4) + version(2) + flags(2) + cluster id length(2) + crc(4)
const HEADER_LEN: usize = 14;
const FILE_NAME: &str = "member";

const VOTE_WITHHELD: u16 = 1 << 0;
const CONVERTED: u16 = 1 << 1;
const STARTED: u16 = 1 << 2;
const KNOWN_FLAGS: u16 = VOTE_WITHHELD | CONVERTED | STARTED;

/// The member file of one metadata store. Cheap to clone; every clone sees
/// the same file.
#[derive(Debug, Clone)]
pub struct MemberFile {
    shared: Arc<Shared>,
}

impl MemberFile {
    /// Open the member file of the store in `store_dir`, which must exist.
    ///
    /// A store with no member file is a new one, and reads as
    /// [`Member::default`]. The member file is the first thing written to a
    /// store, so a directory that holds a log or a snapshot without one has
    /// lost it, and the open fails.
    pub fn open(store_dir: impl Into<PathBuf>) -> io::Result<Self> {
        let dir = store_dir.into();
        let member = read(&dir)?;
        if member.is_none() {
            for name in [LOG_DIR, snapshot::FILE_NAME] {
                if super::present(&dir.join(name))? {
                    return Err(invalid(format!(
                        "{} holds {name} and no member file, so its cluster id, its withheld \
                         vote and its marks are unknown",
                        dir.display()
                    )));
                }
            }
        }
        state_file::remove_temporary(&dir, FILE_NAME)?;
        Ok(Self {
            shared: Arc::new(Shared {
                dir,
                state: parking_lot::Mutex::new(State {
                    member: member.unwrap_or_default(),
                    failed: false,
                }),
            }),
        })
    }

    /// The member as the file holds it.
    pub fn member(&self) -> Member {
        self.shared.state.lock().member.clone()
    }

    /// Record that this store belongs to `cluster_id`. Fails, with nothing
    /// written, when it already belongs to another cluster.
    pub async fn claim_cluster_id(&self, cluster_id: &str) -> io::Result<()> {
        if cluster_id.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "a cluster id cannot be empty",
            ));
        }
        let cluster_id = cluster_id.to_string();
        self.change(move |member, dir| match &member.cluster_id {
            Some(recorded) if *recorded == cluster_id => Ok(()),
            Some(recorded) => Err(invalid(format!(
                "{} belongs to cluster '{recorded}', not '{cluster_id}'",
                dir.display()
            ))),
            None => {
                member.cluster_id = Some(cluster_id);
                Ok(())
            }
        })
        .await
    }

    /// Withhold or give back the member's vote, on disk before this
    /// returns.
    pub async fn set_vote_withheld(&self, withheld: bool) -> io::Result<()> {
        self.change(move |member, _| {
            member.vote_withheld = withheld;
            Ok(())
        })
        .await
    }

    /// Set the started mark, on disk before this returns: from here on the
    /// member may have voted or acknowledged in this store's group, so it
    /// can no longer go back to its redb file alone.
    ///
    /// The Raft node takes the [`Started`] this returns, so a node cannot
    /// be made on a store whose mark is not on disk. A node campaigns on
    /// its own timer, so setting the mark any later would be too late.
    pub async fn mark_started(&self) -> io::Result<Started> {
        self.change(|member, _| {
            member.started = true;
            Ok(())
        })
        .await?;
        Ok(Started(()))
    }

    /// Set or clear the converted mark, on disk before this returns. For
    /// the conversion tool: set while it builds the store, cleared when a
    /// restore takes the store out of use.
    pub async fn set_converted(&self, converted: bool) -> io::Result<()> {
        self.change(move |member, _| {
            member.converted = converted;
            Ok(())
        })
        .await
    }

    /// Apply `change` and write the file if it changed anything.
    async fn change(
        &self,
        change: impl FnOnce(&mut Member, &Path) -> io::Result<()> + Send + 'static,
    ) -> io::Result<()> {
        let shared = Arc::clone(&self.shared);
        felix_common::blocking::spawn(move || {
            let mut state = shared.state.lock();
            if state.failed {
                return Err(io::Error::other(format!(
                    "an earlier write of {} failed, so what is on disk is unknown",
                    shared.dir.join(FILE_NAME).display()
                )));
            }
            let mut member = state.member.clone();
            change(&mut member, &shared.dir)?;
            if member == state.member {
                return Ok(());
            }
            let bytes = encode(&member)?;
            // A failed fsync is not retried: the file may be the old one or
            // the new one, and the caller has to stop.
            if let Err(err) = state_file::store(&shared.dir, FILE_NAME, &[&bytes]) {
                state.failed = true;
                return Err(err);
            }
            state.member = member;
            Ok(())
        })
        .await
        .map_err(io::Error::other)?
    }
}

#[derive(Debug)]
struct Shared {
    dir: PathBuf,
    /// Held across a write, so two changes do not interleave.
    state: parking_lot::Mutex<State>,
}

#[derive(Debug)]
struct State {
    member: Member,
    failed: bool,
}

/// What the member file holds.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Member {
    /// The cluster this store belongs to, once claimed.
    pub cluster_id: Option<String>,
    /// The member must not vote or campaign: it rejoined with an empty
    /// store and may have forgotten a vote.
    pub vote_withheld: bool,
    /// The conversion tool built this store from a redb store.
    pub converted: bool,
    /// The member has started on this store at least once.
    pub started: bool,
}

/// Proof that the started mark is on disk. Only
/// [`MemberFile::mark_started`] makes one.
#[derive(Debug)]
pub struct Started(());

/// The member file in `dir`, or `None` when the store has none. Read-only.
pub(super) fn read(dir: &Path) -> io::Result<Option<Member>> {
    let Some(bytes) = state_file::load(dir, FILE_NAME)? else {
        return Ok(None);
    };
    decode(&bytes)
        .map(Some)
        .map_err(|why| invalid(format!("{}: {why}", dir.join(FILE_NAME).display())))
}

pub(super) fn encode(member: &Member) -> io::Result<Vec<u8>> {
    let cluster_id = member.cluster_id.as_deref().unwrap_or_default();
    let id_len = u16::try_from(cluster_id.len())
        .map_err(|_| invalid("a cluster id is over 65535 bytes".to_string()))?;
    let mut flags = 0;
    for (set, flag) in [
        (member.vote_withheld, VOTE_WITHHELD),
        (member.converted, CONVERTED),
        (member.started, STARTED),
    ] {
        if set {
            flags |= flag;
        }
    }
    let mut out = Vec::with_capacity(HEADER_LEN + cluster_id.len());
    out.extend_from_slice(&MAGIC.to_be_bytes());
    out.extend_from_slice(&VERSION.to_be_bytes());
    out.extend_from_slice(&flags.to_be_bytes());
    out.extend_from_slice(&id_len.to_be_bytes());
    let crc = crate::segment::format::crc32(&[&out, cluster_id.as_bytes()]);
    out.extend_from_slice(&crc.to_be_bytes());
    out.extend_from_slice(cluster_id.as_bytes());
    Ok(out)
}

/// The member `bytes` hold, or why they are not a member file this build
/// reads.
pub(super) fn decode(bytes: &[u8]) -> Result<Member, String> {
    let damaged = || "the member file is damaged".to_string();
    let header = bytes.get(..HEADER_LEN).ok_or_else(damaged)?;
    let u16_at = |at: usize| u16::from_be_bytes([header[at], header[at + 1]]);
    if header[0..4] != MAGIC.to_be_bytes() {
        return Err(damaged());
    }
    // Before the checksum: another version may lay the file out another way.
    let version = u16_at(4);
    if version != VERSION {
        return Err(format!(
            "the member file is format version {version}, and this build reads version {VERSION}"
        ));
    }
    let (flags, id_len) = (u16_at(6), usize::from(u16_at(8)));
    let cluster_id = &bytes[HEADER_LEN..];
    let crc = u32::from_be_bytes([header[10], header[11], header[12], header[13]]);
    if cluster_id.len() != id_len
        || crate::segment::format::crc32(&[&header[..10], cluster_id]) != crc
    {
        return Err(damaged());
    }
    // A flag this build does not know may forbid something it would do.
    if flags & !KNOWN_FLAGS != 0 {
        return Err(format!(
            "the member file sets flags this build does not know ({flags:#06x})"
        ));
    }
    let cluster_id = std::str::from_utf8(cluster_id).map_err(|_| damaged())?;
    Ok(Member {
        cluster_id: (!cluster_id.is_empty()).then(|| cluster_id.to_string()),
        vote_withheld: flags & VOTE_WITHHELD != 0,
        converted: flags & CONVERTED != 0,
        started: flags & STARTED != 0,
    })
}

fn invalid(message: String) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}
