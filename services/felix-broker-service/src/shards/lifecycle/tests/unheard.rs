//! A lease-free shard that no majority answered for a lease duration has its
//! readers ended, and serves them again once a majority or the lease is back.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering::SeqCst};
use std::time::Duration;

use felix_replication::driver::WriteFence;

use crate::cluster::lease::LeaseState;
use crate::test_support::leader::{self, Leader};

use super::*;

/// Confirms every round while `answering` is set.
#[derive(Default)]
struct Majority {
    answering: AtomicBool,
}

#[async_trait::async_trait]
impl felix_replication::quorum::LeadershipCheck for Majority {
    async fn confirm(
        &self,
        _key: &ShardKey,
        _generation: u64,
    ) -> Result<(), felix_replication::quorum::QuorumError> {
        if self.answering.load(SeqCst) {
            Ok(())
        } else {
            Err(felix_replication::quorum::QuorumError::LeadershipLost {
                what: "read",
                detail: "no majority answered",
            })
        }
    }
}

struct Unleased {
    ingress: Arc<crate::shards::routing::IngressRouter>,
    lease: Arc<LeaseState>,
    majority: Arc<Majority>,
    shutdown: tokio_util::sync::CancellationToken,
    _fixture: Leader,
}

impl Unleased {
    /// A `Quorum` leader in a fleet that reads by round, with its readers off
    /// the lease and `watch_leadership` running.
    async fn start() -> Self {
        use felix_common::fleet::{GENERATION_START, LEASE_FREE_READS, MAJORITY_ACK};

        let mut fixture = Leader::start().await;
        let fleet = Arc::new(felix_common::fleet::FleetGate::for_this_build());
        fleet.restart(
            [GENERATION_START, MAJORITY_ACK, LEASE_FREE_READS]
                .iter()
                .map(|feature| feature.name()),
        );
        let marks = Arc::new(felix_replication::quorum::QuorumMarks::with_fleet(fleet));
        let majority = Arc::new(Majority::default());
        majority.answering.store(true, SeqCst);
        marks.set_read_check(Arc::clone(&majority) as _, false);

        let lease = Arc::new(LeaseState::on_tokio_clock(Duration::from_secs(4)));
        lease.renew();
        let fence = Arc::clone(fixture.ingress.fence());
        fence.bind_lease(Arc::clone(&lease));
        fence.sessions_without_lease(&quorum(), leader::GENERATION);

        let lifecycle = std::mem::replace(&mut fixture.lifecycle, ShardLifecycle::new("unused"));
        let shutdown = tokio_util::sync::CancellationToken::new();
        tokio::spawn(
            ShardReaders::new(Arc::clone(&fixture.broker)).watch_leadership(
                fence,
                marks,
                Arc::new(tokio::sync::Mutex::new(lifecycle)),
                shutdown.clone(),
            ),
        );
        Self {
            ingress: Arc::clone(&fixture.ingress),
            lease,
            majority,
            shutdown,
            _fixture: fixture,
        }
    }

    fn reads_admitted(&self) -> bool {
        crate::serving::quic::handlers::redirect::redirect_for(
            Some(&self.ingress),
            None,
            leader::TENANT,
            leader::NAMESPACE,
            leader::QUORUM,
            0,
            crate::shards::ShardKind::Stream,
            0,
        )
        .is_none()
    }

    /// Lapse the lease with no majority answering, for long enough that the
    /// probe gives up on the shard.
    async fn go_quiet(&self) {
        self.majority.answering.store(false, SeqCst);
        self.lease.surrender();
        tokio::time::sleep(self.lease.usable() * 2).await;
        assert!(!self.reads_admitted(), "no majority for a lease duration");
    }
}

impl Drop for Unleased {
    fn drop(&mut self) {
        self.shutdown.cancel();
    }
}

fn quorum() -> ShardKey {
    leader::stream_key(leader::QUORUM)
}

/// **A shard ended for silence serves its readers again once a majority
/// answers.** Silence names no newer leader, and the shard's `Quorum` writes
/// go on wherever a majority answers, so refusing its readers for good left
/// a leader acknowledging writes it would not let anyone read.
#[tokio::test(start_paused = true)]
async fn readers_ended_for_silence_come_back_when_a_majority_answers() {
    let unleased = Unleased::start().await;
    assert!(unleased.reads_admitted());
    unleased.go_quiet().await;

    unleased.majority.answering.store(true, SeqCst);
    tokio::time::sleep(unleased.lease.usable() / 2).await;
    assert!(unleased.reads_admitted(), "a majority answered again");
    assert!(
        !unleased
            .ingress
            .fence()
            .is_deposed(&quorum(), leader::GENERATION)
    );
}

/// **The same once the lease is renewed**, with no round confirmed: while it
/// holds no rounds run, and a new assignment reaches the broker the usual way.
#[tokio::test(start_paused = true)]
async fn readers_ended_for_silence_come_back_with_the_lease() {
    let unleased = Unleased::start().await;
    unleased.go_quiet().await;

    unleased.lease.renew();
    tokio::time::sleep(unleased.lease.usable() / 2).await;
    assert!(unleased.reads_admitted(), "the lease is valid again");

    // A later lapse starts the silence clock again rather than finding the
    // old one.
    unleased.lease.surrender();
    tokio::time::sleep(unleased.lease.usable() / 2).await;
    assert!(
        unleased.reads_admitted(),
        "not yet a lease duration of silence"
    );
}

/// A follower naming a newer leader still deposes for good: a round
/// confirmed afterwards does not bring the readers back.
#[tokio::test(start_paused = true)]
async fn a_deposal_for_a_newer_leader_is_not_cleared_by_a_round() {
    let unleased = Unleased::start().await;
    unleased.go_quiet().await;
    unleased
        .ingress
        .fence()
        .depose(&quorum(), leader::GENERATION);

    unleased.majority.answering.store(true, SeqCst);
    unleased.lease.renew();
    tokio::time::sleep(unleased.lease.usable() / 2).await;
    assert!(!unleased.reads_admitted());
}
