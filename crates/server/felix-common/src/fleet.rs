//! Features that only work once every broker has them.
//!
//! Some behaviour changes what one broker expects of another: how a key maps
//! to a shard, what a peer will check before accepting a write. Turning such a
//! feature on while any serving broker predates it splits the fleet in two.
//! So each broker reports the features it implements when it registers, and
//! the control plane tracks which ones every serving broker supports. Support
//! turns nothing on. An operator finalizes a feature once the whole fleet is
//! upgraded, the control plane answers registrations and heartbeats with the
//! enabled set, and a broker uses a feature only once that answer includes
//! it. Until then any broker may be rolled back.
//!
//! Names on the wire are plain strings. A control plane never needs to know
//! what a feature means to intersect sets of them, and a name it has never
//! seen is kept rather than dropped.
//!
//! Finalizing is one-way: from then on the control plane refuses a broker
//! that lacks the feature, so it is never withdrawn by a late-joining older
//! broker. `docs/control-plane.md` ("Fleet features") has the rules.
use std::collections::BTreeSet;
use std::sync::atomic::{AtomicU64, Ordering};

/// A feature a broker may report, named as it goes on the wire.
///
/// Defined as constants here, one per feature, so every crate that gates on
/// one names it the same way.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct FleetFeature(&'static str);

impl FleetFeature {
    /// A feature named `name`. Lowercase `snake_case`, never reused for a
    /// different meaning: a broker that reported the old one would enable
    /// the new.
    pub const fn new(name: &'static str) -> Self {
        Self(name)
    }

    pub const fn name(self) -> &'static str {
        self.0
    }
}

impl std::fmt::Display for FleetFeature {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.0)
    }
}

/// Streams created once this is enabled map keys to shards with jump
/// consistent hashing. A broker reporting it routes by each stream's recorded
/// mapping, so it can serve a jump-hash stream; the control plane only creates
/// one after every broker can. Existing streams keep the mapping they have.
pub const JUMP_HASH_ROUTING: FleetFeature = FleetFeature::new("jump_hash_routing");

/// Every feature this build implements, which is what a broker reports.
///
/// Add a feature here in the same change that makes the broker honour it,
/// never before: the report is a promise that this build behaves that way.
pub const IMPLEMENTED: &[FleetFeature] = &[
    GENERATION_START,
    MAJORITY_ACK,
    LEASE_FREE_READS,
    JUMP_HASH_ROUTING,
    ATOMIC_COMMIT,
    PUBLISHER_PRINCIPAL,
    FENCED_CACHES,
];

/// A leader writes a generation-start record whenever it starts leading a
/// stream shard at a new generation, and its quorum mark counts only records
/// of its own generation. The record needs storage format v4, which an older
/// build refuses, so it waits for the whole fleet. See
/// `docs/replication-design.md` ("The generation-start record").
pub const GENERATION_START: FleetFeature = FleetFeature::new("generation_start");

/// A `Quorum` stream shard acknowledges a write once a majority of its
/// replicas has answered that it holds it at the leader's generation, with
/// neither the control-plane report nor the lease on the write's path. Safe
/// only when every broker fences a majority before it serves a promoted
/// shard, so a broker running with `FELIX_INTERNAL_FENCE=false` does not
/// report it. Takes effect alongside [`GENERATION_START`]. See
/// `docs/replication-design.md` ("Acknowledging by the followers").
pub const MAJORITY_ACK: FleetFeature = FleetFeature::new("majority_ack");

/// A `Quorum` read confirms that this broker still leads the shard with one
/// round of fences at its own generation, answered by a majority after the
/// read began, instead of trusting the lease. Like [`MAJORITY_ACK`] it rests
/// on every broker fencing before it serves a promoted shard, so a broker
/// running with `FELIX_INTERNAL_FENCE=false` does not report it. Takes effect
/// alongside [`MAJORITY_ACK`] and [`GENERATION_START`]. See
/// `docs/replication-design.md` ("Reads without the lease").
pub const LEASE_FREE_READS: FleetFeature = FleetFeature::new("lease_free_reads");

/// [`MAJORITY_ACK`] for `Quorum` caches: a write or counter add is
/// acknowledged once a majority of the cache shard's replicas has answered
/// that it holds it at the leader's generation, and a promoted cache shard
/// never opens on the lease. Safe only when every broker fences a promoted
/// cache shard's cache and counter logs before it serves, which a build
/// without this feature does not, so a broker running with
/// `FELIX_INTERNAL_FENCE=false` does not report it. Takes effect alongside
/// [`MAJORITY_ACK`] and [`GENERATION_START`]. See
/// `docs/replication-design.md` ("Acknowledging by the followers").
pub const FENCED_CACHES: FleetFeature = FleetFeature::new("fenced_caches");

/// A stream shard accepts atomic commits: an event and state updates written
/// as one commit record. The record needs storage format v5 and a replication
/// mark an older build refuses, so it waits for the whole fleet. See
/// `docs/atomic-commit.md`.
pub const ATOMIC_COMMIT: FleetFeature = FleetFeature::new("atomic_commit");

/// A durable stream stores the principal that published each record, and
/// replication ships it. The record needs storage format v6 and a mark an
/// older build refuses, so it waits for the whole fleet. See
/// `docs/protocol.md` ("Publisher").
pub const PUBLISHER_PRINCIPAL: FleetFeature = FleetFeature::new("publisher_principal");

/// The most features one gate tracks. A report longer than this is cut, which
/// only ever leaves features off.
pub const MAX_FEATURES: usize = 64;

/// The features every one of `reports` includes: what a fleet supports. No
/// reports at all means none.
pub fn minimum<'a, I>(reports: I) -> BTreeSet<String>
where
    I: IntoIterator<Item = &'a BTreeSet<String>>,
{
    let mut reports = reports.into_iter();
    let Some(first) = reports.next() else {
        return BTreeSet::new();
    };
    let mut common = first.clone();
    for report in reports {
        common.retain(|name| report.contains(name));
        if common.is_empty() {
            break;
        }
    }
    common
}

/// The features in `required` that `offered` lacks, in order.
pub fn missing<'a>(required: &'a BTreeSet<String>, offered: &BTreeSet<String>) -> Vec<&'a str> {
    required
        .iter()
        .filter(|name| !offered.contains(*name))
        .map(String::as_str)
        .collect()
}

/// One broker's view of which features the fleet has enabled.
///
/// Built from what this broker reported, so it can only ever enable a
/// feature this broker has. Checking it is one atomic load and a scan of a
/// few names, cheap enough for a per-message path.
///
/// Features only turn on. The control plane never disables a finalized
/// feature, so an answer missing one is stale (an instance whose copy lags)
/// and is ignored rather than switching routing back and forth. A broker that registers again starts a
/// new gate from what that registration answers.
#[derive(Debug)]
pub struct FleetGate {
    reported: Box<[String]>,
    enabled: AtomicU64,
}

impl FleetGate {
    /// A gate for a broker reporting `reported`, with nothing enabled yet.
    pub fn new<I, S>(reported: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let reported: BTreeSet<String> = reported.into_iter().map(Into::into).collect();
        Self {
            reported: reported.into_iter().take(MAX_FEATURES).collect(),
            enabled: AtomicU64::new(0),
        }
    }

    /// A gate for exactly what this build implements.
    pub fn for_this_build() -> Self {
        Self::new(IMPLEMENTED.iter().map(|feature| feature.name()))
    }

    /// What this broker reports to the control plane.
    pub fn reported(&self) -> BTreeSet<String> {
        self.reported.iter().cloned().collect()
    }

    /// Whether an operator has enabled `feature` for the fleet, as far as
    /// this broker has been told. Every broker supporting it is not enough.
    pub fn supports(&self, feature: FleetFeature) -> bool {
        self.supports_name(feature.name())
    }

    /// [`Self::supports`] by wire name.
    pub fn supports_name(&self, name: &str) -> bool {
        let enabled = self.enabled.load(Ordering::Acquire);
        self.reported
            .iter()
            .position(|reported| reported == name)
            .is_some_and(|bit| enabled & (1 << bit) != 0)
    }

    /// The features enabled so far.
    pub fn enabled(&self) -> Vec<&str> {
        self.names(self.enabled.load(Ordering::Acquire))
    }

    /// Take the enabled set from a heartbeat. Returns the features this
    /// turned on; a feature missing from `fleet` stays as it was.
    pub fn observe<'a, I>(&self, fleet: I) -> Vec<&str>
    where
        I: IntoIterator<Item = &'a str>,
    {
        let bits = self.bits(fleet);
        let before = self.enabled.fetch_or(bits, Ordering::AcqRel);
        self.names(bits & !before)
    }

    /// Start over from a registration's answer, which is exact where a
    /// heartbeat's may lag. Returns the features this turned off, which is
    /// empty unless the control plane lost its state.
    pub fn restart<'a, I>(&self, fleet: I) -> Vec<&str>
    where
        I: IntoIterator<Item = &'a str>,
    {
        let bits = self.bits(fleet);
        let before = self.enabled.swap(bits, Ordering::AcqRel);
        self.names(before & !bits)
    }

    fn bits<'a, I>(&self, fleet: I) -> u64
    where
        I: IntoIterator<Item = &'a str>,
    {
        fleet
            .into_iter()
            .filter_map(|name| self.reported.iter().position(|reported| reported == name))
            .fold(0, |bits, bit| bits | (1 << bit))
    }

    fn names(&self, bits: u64) -> Vec<&str> {
        self.reported
            .iter()
            .enumerate()
            .filter(|(bit, _)| bits & (1 << bit) != 0)
            .map(|(_, name)| name.as_str())
            .collect()
    }
}

#[cfg(test)]
mod tests;
