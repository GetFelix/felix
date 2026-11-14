//! A voter of a shard's switch from the hybrid to Raft: its member starts
//! on the directory the hybrid wrote, from the base the control plane
//! recorded. See `docs/per-shard-raft.md`, "Moving a shard from the hybrid
//! to Raft", and [`felix_storage::disk_log::switch`].

use std::collections::BTreeSet;
use std::path::Path;

use felix_storage::disk_log::switch::{NotAVoter, SideDir, SwitchBase};
use felix_storage::log::LogConfig;
use felix_storage::raft_log::{EntryCodec, LogPoint};

use super::codec::ShardCodec;
use super::machine::base_snapshot;
use super::types::{MemberId, ShardTypes};

/// Make `dir` ready for `member`'s start as one of `voters`, the voters the
/// switch recorded with `base`. `hybrid` is the configuration the hybrid
/// opened the directory with. `side` is the shard's dead-letter and
/// cursor logs, which must hold what the seal recorded.
///
/// On `Ok(Ok(()))` the directory opens as a Raft log
/// ([`LogConfig::raft_log`], fsync on commit) and
/// [`super::GroupRuntime::open`] starts the member: its log begins after the
/// base, its state machine has applied the base with `voters` as the
/// membership, and its first election is at a term above the base's.
/// Records up to the base stay readable and are never entries.
///
/// `Ok(Err(_))` leaves the directory as it was: the copy does not hold the
/// base, so it cannot vote and starts as a learner. Calling this again
/// after a crash, or on a directory it already started, gives the same
/// result.
pub async fn start_voter(
    dir: &Path,
    label: &str,
    hybrid: LogConfig,
    base: &SwitchBase,
    side: [SideDir<'_>; 2],
    member: &MemberId,
    voters: &BTreeSet<MemberId>,
) -> felix_storage::Result<Result<(), NotAVoter>> {
    let (vote_for, snapshot) = prepare(base, member, voters)?;
    felix_storage::disk_log::switch::start_voter(
        dir, label, hybrid, base, side, &vote_for, snapshot,
    )
    .await
}

fn prepare(
    base: &SwitchBase,
    member: &MemberId,
    voters: &BTreeSet<MemberId>,
) -> felix_storage::Result<(String, felix_storage::raft_log::StoredSnapshot)> {
    let refused = |why: String| {
        felix_storage::StorageError::Io(std::io::Error::new(std::io::ErrorKind::InvalidInput, why))
    };
    if !voters.contains(member) {
        return Err(refused(format!(
            "{member} is not one of the switch's voters {voters:?}"
        )));
    }
    let term = base
        .term()
        .ok_or_else(|| refused("a switch base at generation 0".to_string()))?;
    let snapshot = base_snapshot(
        LogPoint {
            term,
            index: base.offset,
        },
        voters,
    )
    .map_err(felix_storage::StorageError::Io)?;
    let vote_for = <ShardCodec as EntryCodec<ShardTypes>>::encode_node_id(&ShardCodec, member);
    Ok((vote_for, snapshot))
}
