//! Starting a copy in a re-formed group's new epoch.
//!
//! A voter keeps its base records and gets a purged id, a snapshot id and a
//! saved commit at `first_index - 1` in the base's term, so the first leader
//! appends at the epoch's first index. A learner holding records past the
//! base empties its log and catches up by snapshot. Both get `raft-epoch`
//! and a reservation through `first_index - 1`.
//!
//! A voter whose base is in a kept directory copies that directory's
//! segments in under temporary names and renames them into place. A voter
//! holding records past the base cuts them first, which needs a re-form
//! that accepts the loss when any of them is committed.
//!
//! The steps run under a `reform-building` marker. [`super::CopyRoot::open_copy`]
//! refuses a directory that holds one, and a start run again over a crashed
//! one redoes every step, so a half-built copy is never served. See
//! `docs/per-shard-raft.md`, "Starting the new epoch".

use std::io;
use std::path::{Path, PathBuf};

use bytes::Bytes;

use super::epoch_file::{self, EpochBase, EpochFile};
use super::judge::CopyRoot;
use super::vouch::ReformPlan;
use super::wire::WireStart;
use crate::disk_log::carried_floor::{self, CarriedFloor};
use crate::disk_log::{crash_point, durable_mark};
use crate::log::{AppendOnlyLog, LogConfig};
use crate::raft_log::snapshot::{SnapshotId, StoredSnapshot};
use crate::raft_log::{LogPoint, held, reserved, state_file};
use crate::segment::parse_segment_file_name;

/// Present while a start is under way.
pub(crate) const BUILDING: &str = "reform-building";

/// The suffix of a file copied in from a kept directory and not renamed
/// into place yet.
const COPYING: &str = ".copying";

/// What the copy is in the new epoch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Role {
    Voter,
    Learner,
}

/// One copy's start.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Start {
    /// The copy's `raft-epoch` once started: its last base is the new epoch's.
    pub(crate) epoch: EpochFile,
    pub(crate) role: Role,
    /// Where each term starts in the base, as (first index, term), ascending
    /// and through the base's last log id. A learner keeps its prefix only
    /// where its terms match these.
    pub(crate) base_terms: Vec<(u64, u64)>,
    /// The re-form accepts losing committed entries past the base, so a
    /// voter's saved commit above the first index may be lowered to it.
    pub(crate) accept_loss: AcceptLoss,
    /// A kept directory holding the base, which a voter's log is rebuilt
    /// from in place of its own. It is only read.
    pub(crate) kept: Option<PathBuf>,
    /// The plan's highest floor ([`ReformPlan::floor`]), raised into the
    /// copy's `raft-floor` so the new epoch refuses reads below it even
    /// where the trim that set it is cut or held by no copy.
    pub(crate) floor: Option<CarriedFloor>,
    /// The meta and data of a voter's snapshot at the end of the gap.
    pub(crate) meta: Bytes,
    pub(crate) data: Bytes,
}

/// Folds a voter's snapshot at the gap's end, which the start stores at
/// `first_index - 1`: the copy's stored snapshot with every entry after it
/// through the base applied, and the new voters as the membership. The
/// state machine is not this crate's, so the caller supplies it. Run on the
/// voter's own directory once its log ends at the base, so the fold reads
/// the base and nothing a start cut. See `docs/per-shard-raft.md`, "A
/// voter's snapshot at the gap's end is a fold".
pub trait GapFold: Send + Sync {
    /// The snapshot at `gap_end` for the copy in `dir`, whose log ends at
    /// `base` (`None`: it holds no entry).
    fn fold<'a>(
        &'a self,
        dir: &'a Path,
        config: &'a LogConfig,
        base: Option<LogPoint>,
        gap_end: LogPoint,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = io::Result<StoredSnapshot>> + Send + 'a>>;
}

/// What a copy's start takes from the re-form driver rather than the plan.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CopyStart {
    pub(crate) epoch: EpochFile,
    pub(crate) role: Role,
    pub(crate) base_terms: Vec<(u64, u64)>,
    pub(crate) kept: Option<PathBuf>,
    pub(crate) meta: Bytes,
    pub(crate) data: Bytes,
}

impl Start {
    /// A copy's start under `plan`. The plan's floor and whether a commit
    /// may be lowered come from the plan here, so a driver cannot start a
    /// copy without either. `accepted` is the operator's `--accept-loss`.
    pub(crate) fn of<B>(
        plan: &ReformPlan<B>,
        accepted: bool,
        copy: CopyStart,
    ) -> Result<Self, LossNotAccepted> {
        let CopyStart {
            epoch,
            role,
            base_terms,
            kept,
            meta,
            data,
        } = copy;
        Ok(Self {
            epoch,
            role,
            base_terms,
            accept_loss: AcceptLoss::of(plan, accepted)?,
            kept,
            floor: plan.floor.clone(),
            meta,
            data,
        })
    }
}

/// Whether a start may lower a saved commit. Only made from the plan and
/// the operator's `--accept-loss`, so a plan that needs the loss accepted
/// never starts without it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct AcceptLoss(bool);

/// The plan can drop acknowledged records and the operator did not accept
/// the loss.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct LossNotAccepted;

impl AcceptLoss {
    /// `accepted` is whether the operator named the group with
    /// `--accept-loss`. A plan that does not need it lowers no commit even
    /// when the operator accepted, so an unexpected lower still fails.
    pub(crate) fn of<B>(plan: &ReformPlan<B>, accepted: bool) -> Result<Self, LossNotAccepted> {
        let needs = plan.needs_accept_loss();
        if needs && !accepted {
            return Err(LossNotAccepted);
        }
        Ok(Self(needs))
    }

    #[cfg(test)]
    pub(crate) const fn assumed(accepts: bool) -> Self {
        Self(accepts)
    }

    fn accepts(self) -> bool {
        self.0
    }
}

impl CopyRoot {
    /// Start `dir`, the copy of `shard` (its mark name) a re-form stopped,
    /// in the epoch `bases` ends, as the `start` survey's `wire` says. The
    /// bases come from the shard's `placed` record, never the survey. A
    /// voter's snapshot at the gap's end is what `fold` builds; a learner
    /// does not call it. `member` is this copy's member id as its log
    /// stores a vote for it. Run again over a start that crashed, it redoes every
    /// step. See `docs/per-shard-raft.md`, "What a start needs".
    #[allow(
        clippy::too_many_arguments,
        reason = "each is one fact the start needs"
    )]
    pub async fn start_stopped(
        &self,
        shard: &str,
        dir: &Path,
        incarnation: u64,
        bases: Vec<EpochBase>,
        wire: &WireStart,
        fold: &dyn GapFold,
        member: &str,
        config: &LogConfig,
    ) -> io::Result<()> {
        let label = dir.display();
        let epoch = EpochFile { incarnation, bases };
        let new = epoch.epoch();
        let Some(base) = epoch.bases.last().copied() else {
            return Err(invalid(format!(
                "{label}: a start needs the new epoch's base"
            )));
        };
        if !start_due(dir, new)? {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("{label}: not a copy stopped in the epoch before {new}"),
            ));
        }
        let copy = super::copy_file::load(dir)?
            .ok_or_else(|| invalid(format!("{label}: a stopped copy without a copy id")))?
            .copy;
        if let Some(voter) = wire.voter
            && voter != copy
        {
            return Err(invalid(format!(
                "{label}: the plan's voter is copy {voter:016x}, the directory holds {copy:016x}"
            )));
        }
        let kept = wire
            .kept
            .as_ref()
            .map(|kept| kept_beside(dir, Path::new(kept)))
            .transpose()?;
        if wire.needs_accept_loss && !wire.accepted {
            return Err(invalid(format!(
                "{label}: the plan may lose acknowledged records and the loss was not accepted"
            )));
        }
        let role = if wire.voter.is_some() {
            Role::Voter
        } else {
            Role::Learner
        };
        // Lowered first, as a truncation lowers it: the start can leave the
        // log below the old mark, and an open would then rotate the copy
        // away from the id the seed names.
        let mark = match role {
            Role::Voter => base.base_last.map(|last| LogPoint {
                term: last.term,
                index: last.index,
            }),
            Role::Learner => None,
        };
        self.mark_log()
            .for_copy(std::sync::Arc::from(shard), copy)
            .lower_log(mark)
            .await?;
        let start = Start {
            epoch,
            role,
            // The reports cannot give exact term starts, so a learner
            // holding records always empties.
            base_terms: Vec::new(),
            accept_loss: AcceptLoss(wire.needs_accept_loss),
            kept,
            floor: wire.floor.as_ref().map(CarriedFloor::from),
            meta: Bytes::new(),
            data: Bytes::new(),
        };
        start_with(dir, &start, config, Some(fold), Some(member)).await
    }
}

/// Whether the copy in `dir` is one a start of epoch `epoch` may run on:
/// stopped in the epoch before, or mid-start of this one or of the epoch
/// before's. A served copy, or one already started, is not.
pub fn start_due(dir: &Path, epoch: u64) -> io::Result<bool> {
    if building(dir)? {
        let at = super::group_epoch(dir)?.unwrap_or(0);
        return Ok(at + 1 == epoch || at == epoch);
    }
    Ok(super::stop_file::stopped(dir)?.is_some_and(|stopped| stopped.epoch + 1 == epoch))
}

/// Whether the copy in `dir` is started in `epoch`: it names the epoch and
/// no start is under way. A start writes `raft-epoch` before its last
/// steps, so the epoch alone does not say the start finished.
pub fn started_in(dir: &Path, epoch: u64) -> io::Result<bool> {
    Ok(super::group_epoch(dir)? == Some(epoch) && !building(dir)?)
}

/// `kept` as a directory in `dir`'s `.kept`, the only place a start reads a
/// base from.
fn kept_beside(dir: &Path, kept: &Path) -> io::Result<PathBuf> {
    let beside = super::judge::parent_of(dir)?.join(crate::disk_log::layout::KEPT_DIR);
    if kept.parent() != Some(beside.as_path()) || kept.file_name().is_none() {
        return Err(invalid(format!(
            "{}: kept directory {} is not in {}",
            dir.display(),
            kept.display(),
            beside.display()
        )));
    }
    Ok(kept.to_path_buf())
}

/// Whether a start is under way in `dir`.
pub(crate) fn building(dir: &Path) -> io::Result<bool> {
    Ok(state_file::load(dir, BUILDING)?.is_some())
}

/// Start the copy in `dir`, which no log has open. A voter's log, its own
/// or the one copied from [`Start::kept`], is cut back to the base's last
/// log id, and must then end there in the base's term.
pub(crate) async fn start_copy(dir: &Path, start: &Start, config: &LogConfig) -> io::Result<()> {
    start_with(dir, start, config, None, None).await
}

/// [`start_copy`], with a voter's snapshot at the gap's end from `fold`
/// in place of [`Start::meta`] and [`Start::data`], and a voter's vote
/// raised to the base's term for `vote_for`.
pub(super) async fn start_with(
    dir: &Path,
    start: &Start,
    config: &LogConfig,
    fold: Option<&dyn GapFold>,
    vote_for: Option<&str>,
) -> io::Result<()> {
    let index_spacing_bytes = config.index_spacing_bytes;
    let base = *start
        .epoch
        .bases
        .last()
        .ok_or_else(|| invalid("a start needs the new epoch's base".into()))?;
    let label = dir.display().to_string();
    let step = || crash_point::reached(dir);
    let gap_end = base.first_index.checked_sub(1);

    state_file::store(dir, BUILDING, &[])?;
    step()?;
    // Before the first append, so an offset below the first index is never
    // given out again (#2640).
    if let Some(top) = gap_end
        && reserved::load(dir)? < Some(top)
    {
        reserved::store(dir, top)?;
        step()?;
    }
    let held = held::read(dir, &label, index_spacing_bytes)?;
    let record_last = held.last_record.map(|(index, _)| index);
    let base_last = base.base_last.map(|last| last.index);
    match start.role {
        Role::Learner => {
            let keeps = record_last <= base_last
                && held.purged.is_none()
                && !in_a_gap(&held, &start.epoch)
                && prefix_matches(dir, &label, index_spacing_bytes, &held, &start.base_terms)?;
            if !keeps {
                empty_log(dir)?;
                step()?;
            }
            // An empty learner must not claim a commit: it would count
            // entries it lacks once it opens. Checked on its own so a start
            // cut right after the emptying still clears it when run again.
            if (!keeps || held.last.is_none())
                && crate::disk_log::saved_commit(dir)
                    .map_err(io::Error::other)?
                    .0
                    > 0
            {
                crate::disk_log::store_saved_commit(dir, 0, 0).map_err(io::Error::other)?;
                step()?;
            }
        }
        Role::Voter => {
            // The commit the voter's log claims: the kept directory's when
            // the log comes from there. Read before anything here moves it.
            // Both logs' claims count: the voter's own log is replaced too.
            // A kept directory is read whole before anything here is touched,
            // so a missing or damaged one fails with the voter's log intact.
            let (mut commit, _) = crate::disk_log::saved_commit(dir).map_err(io::Error::other)?;
            if let Some(kept) = &start.kept {
                if !crate::fs::try_exists(kept)? {
                    return Err(invalid(format!(
                        "{label}: kept directory {} is gone",
                        kept.display()
                    )));
                }
                held::read(kept, &label, index_spacing_bytes)?;
                commit = commit.max(
                    crate::disk_log::saved_commit(kept)
                        .map_err(io::Error::other)?
                        .0,
                );
            }
            if commit > base.first_index && !start.accept_loss.accepts() {
                return Err(invalid(format!(
                    "{label}: its saved commit {commit} is past the first index {}; only a \
                     re-form that accepts the loss may lower it",
                    base.first_index
                )));
            }
            let mut record_last = record_last;
            if let Some(kept) = &start.kept {
                // Rebuilding drops what either log holds past the base. The
                // own log's commit counts only for records it holds there:
                // a rerun finds its commit at the first index, over the base.
                let (own, _) = crate::disk_log::saved_commit(dir).map_err(io::Error::other)?;
                let own = if record_last > base_last { own } else { 0 };
                let kept_commit = crate::disk_log::saved_commit(kept)
                    .map_err(io::Error::other)?
                    .0;
                commit = own.max(kept_commit);
                if commit > base.base_end() && !start.accept_loss.accepts() {
                    return Err(invalid(format!(
                        "{label}: records committed through {} are past the base's end {}",
                        commit - 1,
                        base.base_end()
                    )));
                }
                copy_kept(kept, dir, &step)?;
                record_last = held::read(dir, &label, index_spacing_bytes)?
                    .last_record
                    .map(|(index, _)| index);
            }
            if record_last > base_last {
                // A committed record past the base is acknowledged; cutting
                // it is the loss only an accepting re-form may take.
                if commit > base.base_end() && !start.accept_loss.accepts() {
                    return Err(invalid(format!(
                        "{label}: records committed through {} are past the base's end {}",
                        commit - 1,
                        base.base_end()
                    )));
                }
                // An open refuses to cut below its commit; this one is the
                // loss just accepted. A kept copy's was zeroed already.
                if crate::disk_log::saved_commit(dir)
                    .map_err(io::Error::other)?
                    .0
                    > base.base_end()
                {
                    let term = base.base_last.map_or(0, |last| last.term);
                    let generation = crate::raft_log::generation_of(term)?;
                    crate::disk_log::store_saved_commit(dir, base.base_end(), generation)
                        .map_err(io::Error::other)?;
                    step()?;
                }
                cut_to(dir, &label, config, base.base_end()).await?;
                step()?;
            }
            let held = held::read(dir, &label, index_spacing_bytes)?;
            let last = held.last_record.map(|(index, generation)| {
                (
                    index,
                    generation.and_then(crate::raft_log::term_of_generation),
                )
            });
            let want = base.base_last.map(|last| (last.index, Some(last.term)));
            if last != want {
                return Err(invalid(format!(
                    "{label}: the log ends at {last:?}, the base at {want:?}"
                )));
            }
            // An earlier epoch's first append can open its gap and die before
            // its record lands, leaving the tail past the base's end. This
            // epoch's first append would not find its base there, so the
            // empty gap goes. It holds no record: nothing is lost.
            if tail_of(dir, &label, config).await? > base.base_end() {
                let (commit, _) = crate::disk_log::saved_commit(dir).map_err(io::Error::other)?;
                if commit > base.base_end() {
                    let term = base.base_last.map_or(0, |last| last.term);
                    let generation = crate::raft_log::generation_of(term)?;
                    crate::disk_log::store_saved_commit(dir, base.base_end(), generation)
                        .map_err(io::Error::other)?;
                    step()?;
                }
                cut_to(dir, &label, config, base.base_end()).await?;
                step()?;
            }
        }
    }
    if let Some(floor) = &start.floor {
        carried_floor::raise(dir, floor.clone())?;
        step()?;
    }
    // After the floor is raised, so a stream snapshot carries it to a
    // learner that installs from this voter.
    let folded = match (start.role, gap_end, fold) {
        (Role::Voter, Some(index), Some(fold)) => {
            let term = base.base_last.map_or(0, |last| last.term);
            let at = LogPoint { term, index };
            let base_last = base.base_last.map(|last| LogPoint {
                term: last.term,
                index: last.index,
            });
            let folded = fold.fold(dir, config, base_last, at).await?;
            if folded.last != Some(SnapshotId { term, index }) {
                return Err(invalid(format!(
                    "{label}: the snapshot folded for the gap's end {index} is at {:?}",
                    folded.last
                )));
            }
            Some(folded)
        }
        _ => None,
    };
    // Before the purged id: a purged id at the gap's end reads as the base
    // only under the epoch that made the gap.
    epoch_file::store(dir, &start.epoch)?;
    step()?;
    if start.role == Role::Voter
        && let Some(index) = gap_end
    {
        let term = base.base_last.map_or(0, |last| last.term);
        let generation = crate::raft_log::generation_of(term)?;
        crate::raft_log::snapshot::store_in(
            dir,
            &folded.unwrap_or_else(|| StoredSnapshot {
                last: Some(SnapshotId { term, index }),
                meta: start.meta.clone(),
                data: start.data.clone(),
            }),
        )?;
        step()?;
        crate::raft_log::store_purged(dir, index, generation)?;
        step()?;
        crate::disk_log::store_saved_commit(dir, base.first_index, generation)
            .map_err(io::Error::other)?;
        step()?;
        // A voter that never voted (a restored or wiped copy) would
        // campaign at term 1, at or below the base's last term, and openraft
        // refuses to lead under a vote not above its last log id.
        if let Some(member) = vote_for {
            crate::disk_log::raise_saved_vote(dir, generation, member).map_err(io::Error::other)?;
            step()?;
        }
    }
    check_started(dir, start)?;
    state_file::remove(dir, BUILDING)
}

/// The two promises the vouch rule depends on (#2640), checked before the
/// marker goes: a voter's saved commit is at or above the base's end, and
/// every copy is reserved through `first_index - 1`.
pub(super) fn check_started(dir: &Path, start: &Start) -> io::Result<()> {
    let Some(base) = start.epoch.bases.last() else {
        return Ok(());
    };
    let label = dir.display();
    if let Some(top) = base.first_index.checked_sub(1)
        && reserved::load(dir)? < Some(top)
    {
        return Err(invalid(format!(
            "{label}: started without a reservation through {top}"
        )));
    }
    if start.role == Role::Voter {
        let (commit, _) = crate::disk_log::saved_commit(dir).map_err(io::Error::other)?;
        if commit < base.base_end() {
            return Err(invalid(format!(
                "{label}: started with a saved commit of {commit}, below the base's end {}",
                base.base_end()
            )));
        }
    }
    Ok(())
}

/// Whether any record `held` has sits in a gap some re-form left: no
/// record is ever written there, so such a log is not a prefix of the base.
fn in_a_gap(held: &held::Held, epoch: &EpochFile) -> bool {
    let (Some(lowest), Some((last, _))) = (held.lowest, held.last_record) else {
        return false;
    };
    epoch.bases.iter().any(|base| {
        base.base_end() < base.first_index && lowest < base.first_index && last >= base.base_end()
    })
}

/// Whether every record `held` has carries the base's term at its index.
/// Terms never fall along a log, so a stretch with one base term matches
/// when its first and last records both carry it.
fn prefix_matches(
    dir: &Path,
    label: &str,
    index_spacing_bytes: u64,
    held: &held::Held,
    base_terms: &[(u64, u64)],
) -> io::Result<bool> {
    let (Some(lowest), Some((last, _))) = (held.lowest, held.last_record) else {
        return Ok(true);
    };
    let term_at = |index: u64| -> io::Result<Option<u64>> {
        Ok(held::generation_at(dir, label, index_spacing_bytes, index)?
            .flatten()
            .and_then(crate::raft_log::term_of_generation))
    };
    let base_term = |index: u64| {
        base_terms
            .iter()
            .take_while(|(start, _)| *start <= index)
            .last()
            .map(|(_, term)| *term)
    };
    let mut from = lowest;
    while from <= last {
        let to = base_terms
            .iter()
            .map(|(start, _)| *start)
            .find(|start| *start > from)
            .map_or(last, |next| (next - 1).min(last));
        let want = base_term(from);
        if want.is_none() || term_at(from)? != want || term_at(to)? != want {
            return Ok(false);
        }
        from = to + 1;
    }
    Ok(true)
}

/// Remove every segment, index, purged id and snapshot in `dir`, and the
/// forgotten batches and carried floor. The copy file, votes, reservation
/// and markers stay.
fn empty_log(dir: &Path) -> io::Result<()> {
    // No record holds these, so they would outlive the log they describe:
    // the next log would refuse re-sends of batches its voters never forgot.
    // A snapshot install merges into them rather than replacing them.
    state_file::remove(dir, crate::disk_log::forgotten_file_name())?;
    state_file::remove(dir, carried_floor::FILE_NAME)?;
    for entry in crate::fs::read_dir(dir)? {
        let path = entry?.path();
        let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
            continue;
        };
        let log_file = parse_segment_file_name(name).is_some()
            || name.ends_with(".index")
            || name.ends_with(COPYING)
            // A mark naming a segment that is gone would misread the next
            // log's segments as synced or as never synced.
            || name == durable_mark::mark_file_name()
            // Derived from the records, so it goes with them.
            || name == crate::disk_log::producers_file_name()
            || name.starts_with(crate::raft_log::purged::FILE_NAME)
            || name.starts_with(crate::raft_log::snapshot::FILE_NAME);
        if log_file {
            crate::fs::remove_file(&path)?;
        }
    }
    crate::io::sync_dir(dir)
}

/// Rebuild `dir`'s log from `kept`'s segments, durable mark, forgotten
/// batches, carried floor and snapshot. Each file is
/// copied under a temporary name and synced, and only then are they renamed
/// into place, so the directory never holds part of a file under a log
/// file's name. A cut part way leaves the marker, and the start run again
/// empties the directory and copies again. The saved commit goes to zero
/// first: it belongs to the log being replaced, and the kept log's claim is
/// checked by the caller.
fn copy_kept(kept: &Path, dir: &Path, step: &dyn Fn() -> io::Result<()>) -> io::Result<()> {
    if kept == dir {
        return Err(invalid(format!(
            "{}: a copy cannot start from itself as a kept directory",
            dir.display()
        )));
    }
    crate::disk_log::store_saved_commit(dir, 0, 0).map_err(io::Error::other)?;
    empty_log(dir)?;
    step()?;
    // The kept log's forgotten batches and floor come with its records.
    let carried = [
        crate::disk_log::forgotten_file_name(),
        carried_floor::FILE_NAME,
    ];
    let mut names = Vec::new();
    for entry in crate::fs::read_dir(kept)? {
        let name = entry?.file_name();
        let Some(name) = name.to_str() else {
            continue;
        };
        if parse_segment_file_name(name).is_some()
            || name == durable_mark::mark_file_name()
            || carried.contains(&name)
        {
            names.push(name.to_owned());
        }
    }
    names.sort();
    for name in &names {
        let temporary = dir.join(format!("{name}{COPYING}"));
        let bytes = crate::fs::read(kept.join(name))?;
        let mut file = crate::fs::File::create(&temporary)?;
        io::Write::write_all(&mut file, &bytes)?;
        crate::io::sync_all(&file)?;
        step()?;
    }
    for name in &names {
        crate::fs::rename(dir.join(format!("{name}{COPYING}")), dir.join(name))?;
    }
    crate::io::sync_dir(dir)?;
    step()?;
    // The fold at the gap's end starts from the kept copy's snapshot: its
    // log may be purged below it.
    if let Some(snapshot) = crate::raft_log::snapshot::read(kept)? {
        crate::raft_log::snapshot::store_in(dir, &snapshot)?;
        step()?;
    }
    Ok(())
}

/// Cut every record at or after `end` from `dir`'s log.
/// One past the last offset the log in `dir` holds or has opened a gap to.
async fn tail_of(dir: &Path, label: &str, config: &LogConfig) -> io::Result<u64> {
    let log = crate::DiskLog::open(dir, label, config.clone()).map_err(io::Error::other)?;
    let tail = log.tail_offset().await.map_err(io::Error::other);
    let closed = log.close().await.map_err(io::Error::other);
    closed.and(tail)
}

async fn cut_to(dir: &Path, label: &str, config: &LogConfig, end: u64) -> io::Result<()> {
    let log = crate::DiskLog::open(dir, label, config.clone()).map_err(io::Error::other)?;
    let cut = log.truncate(end).await.map_err(io::Error::other);
    let closed = log.close().await.map_err(io::Error::other);
    cut.and(closed)
}

fn invalid(message: String) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}
