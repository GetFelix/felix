//! The member file, `member`: which cluster a metadata store belongs to,
//! whether the member's vote is withheld, and the two marks the upgrade
//! from redb uses.
//!
//! The file is replaced whole: a temporary, an fsync, a rename and a
//! directory sync, all before a change returns. A crash leaves the old file
//! or the new one. Nothing can rebuild it, so a file that does not decode
//! fails the open and is never read as "no file": that would give back a
//! withheld vote and forget that the member has started.
//!
//! A store is born whole: its directory is built under a temporary name
//! with the member file and an empty `log/` in it, then renamed into place
//! and the data directory synced. So a `store/` that exists always has
//! both, and one that lacks either was damaged after the fact.

use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use super::LOG_DIR;
use super::lock::StoreLock;
use crate::disk_log::crash_point;
use crate::raft_log::state_file;

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
    /// Open the store in `store_dir` for the cluster `cluster_id`, making it
    /// if there is none.
    ///
    /// A new store is created whole, member file and empty log directory
    /// together, and is on disk before this returns, so the log can only be
    /// opened in a store whose member file names its cluster. An existing
    /// store must hold both, and belong to `cluster_id`. An empty
    /// `cluster_id` is refused.
    ///
    /// The store's lock is taken before the member file is read and held
    /// until the last clone is dropped. A store that is already open, in
    /// this process or another, by this path or through a symlink, is
    /// refused with [`io::ErrorKind::ResourceBusy`].
    pub fn open(store_dir: impl Into<PathBuf>, cluster_id: &str) -> io::Result<Self> {
        let dir = store_dir.into();
        if cluster_id.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "a cluster id cannot be empty",
            ));
        }
        if !super::present(&dir)? {
            create(&dir, cluster_id)?;
        }
        // A damaged store is refused before the lock file is made in it.
        existing(&dir)?;
        // Before the file is read for use: every change is written from the
        // copy read here, so a second holder would undo the first's changes.
        let lock = StoreLock::take(&dir)?;
        let member = existing(&dir)?;
        if member.cluster_id != cluster_id {
            return Err(invalid(format!(
                "{} belongs to cluster '{}', not '{cluster_id}'",
                dir.display(),
                member.cluster_id
            )));
        }
        state_file::remove_temporary(&dir, FILE_NAME)?;
        Ok(Self {
            shared: Arc::new(Shared {
                dir,
                state: parking_lot::Mutex::new(State {
                    member,
                    failed: false,
                }),
                _lock: lock,
            }),
        })
    }

    /// Where the store's Raft log lives. The directory exists once
    /// [`Self::open`] has returned.
    pub fn log_dir(&self) -> PathBuf {
        self.shared.dir.join(LOG_DIR)
    }

    /// The member as the file holds it.
    pub fn member(&self) -> Member {
        self.shared.state.lock().member.clone()
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
        Ok(Started {
            store_dir: self.shared.dir.clone(),
        })
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
    /// Released when the last clone is dropped.
    _lock: StoreLock,
}

#[derive(Debug)]
struct State {
    member: Member,
    failed: bool,
}

/// What the member file holds.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Member {
    /// The cluster this store belongs to, for good. Never empty.
    pub cluster_id: String,
    /// The member must not vote or campaign: it rejoined with an empty
    /// store and may have forgotten a vote.
    pub vote_withheld: bool,
    /// The conversion tool built this store from a redb store.
    pub converted: bool,
    /// The member has started on this store at least once.
    pub started: bool,
}

/// Proof that the started mark of one store is on disk. Only
/// [`MemberFile::mark_started`] makes one.
#[derive(Debug)]
pub struct Started {
    store_dir: PathBuf,
}

impl Started {
    /// The store whose mark this proves.
    pub fn store_dir(&self) -> &Path {
        &self.store_dir
    }

    /// Fails unless this proves the mark of the store in `store_dir`, so a
    /// node is not made on one store with another store's proof. The paths
    /// are compared as written.
    pub fn check(&self, store_dir: &Path) -> io::Result<()> {
        if self.store_dir == store_dir {
            return Ok(());
        }
        Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "the started mark is for {}, not {}",
                self.store_dir.display(),
                store_dir.display()
            ),
        ))
    }
}

/// Build the store in `dir` for `cluster_id`: a temporary directory with the
/// member file and an empty log directory, renamed into place, and the
/// parent synced. A crash leaves no store, or the whole one, beside perhaps
/// the temporary, which the next build replaces.
fn create(dir: &Path, cluster_id: &str) -> io::Result<()> {
    let name = dir
        .file_name()
        .ok_or_else(|| invalid(format!("{} is not a directory name", dir.display())))?;
    let parent = match dir.parent() {
        Some(parent) if !parent.as_os_str().is_empty() => parent,
        _ => Path::new("."),
    };
    let mut building = name.to_os_string();
    building.push(".tmp");
    let building = parent.join(building);
    let step = || crash_point::reached(dir);

    // The data directory's own entry must be on disk too, or the store goes
    // with it.
    crate::io::create_dir_all_durable(parent)?;
    if super::present(&building)? {
        crate::fs::remove_dir_all(&building)?;
    }
    step()?;
    crate::fs::create_dir_all(building.join(LOG_DIR))?;
    let member = Member {
        cluster_id: cluster_id.to_string(),
        ..Member::default()
    };
    // Syncs `building`, so the log directory's entry is on disk too.
    state_file::store(&building, FILE_NAME, &[&encode(&member)?])?;
    step()?;
    crate::fs::rename(&building, dir)?;
    step()?;
    crate::io::sync_dir(parent)
}

/// The member of the store in `dir`, which exists. A store is born with its
/// member file and its log directory, so one without either is refused.
pub(super) fn existing(dir: &Path) -> io::Result<Member> {
    let member = read(dir)?.ok_or_else(|| {
        invalid(format!(
            "{} holds no member file, so its cluster id, its withheld vote and its marks \
             are unknown",
            dir.display()
        ))
    })?;
    if !super::present(&dir.join(LOG_DIR))? {
        return Err(invalid(format!(
            "{} has a member file and no {LOG_DIR} directory: its log, and any vote in it, \
             is gone",
            dir.display()
        )));
    }
    Ok(member)
}

/// The member file in `dir`, or `None` when the store has none. Read-only.
fn read(dir: &Path) -> io::Result<Option<Member>> {
    let Some(bytes) = state_file::load(dir, FILE_NAME)? else {
        return Ok(None);
    };
    decode(&bytes)
        .map(Some)
        .map_err(|why| invalid(format!("{}: {why}", dir.join(FILE_NAME).display())))
}

pub(super) fn encode(member: &Member) -> io::Result<Vec<u8>> {
    let cluster_id = member.cluster_id.as_str();
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
    if cluster_id.is_empty() {
        return Err("the member file has no cluster id".to_string());
    }
    Ok(Member {
        cluster_id: cluster_id.to_string(),
        vote_withheld: flags & VOTE_WITHHELD != 0,
        converted: flags & CONVERTED != 0,
        started: flags & STARTED != 0,
    })
}

fn invalid(message: String) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}
