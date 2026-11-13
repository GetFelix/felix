//! The links between the members of a Raft simulation. Each member sends
//! through its own [`Wire`], so a cut can be one-way: a request that never
//! arrives, or one that is handled and whose answer is lost.

use std::collections::BTreeSet;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use async_trait::async_trait;
use felix_cluster::history::rng::Rng;
use felix_replication::raft::{GroupRuntime, RaftReply, RaftRequest, RaftTransport, Refused};
use parking_lot::Mutex;

/// Which member can reach which. `(from, to)` in `cut` drops everything
/// `from` sends `to`, answers included.
#[derive(Default)]
pub(crate) struct Links {
    cut: Mutex<BTreeSet<(usize, usize)>>,
    runtimes: OnceLock<Vec<GroupRuntime>>,
}

impl Links {
    /// The runtimes messages are handed to, in member order. Set once, after
    /// every runtime has been started on its wire.
    pub(crate) fn connect(&self, runtimes: Vec<GroupRuntime>) {
        let _ = self.runtimes.set(runtimes);
    }

    pub(crate) fn cut(&self, from: usize, to: usize) {
        self.cut.lock().insert((from, to));
    }

    /// Cut `member` off from everyone, both ways.
    pub(crate) fn isolate(&self, member: usize, members: usize) {
        for other in (0..members).filter(|other| *other != member) {
            self.cut(member, other);
            self.cut(other, member);
        }
    }

    pub(crate) fn heal(&self) {
        self.cut.lock().clear();
    }

    fn is_cut(&self, from: usize, to: usize) -> bool {
        self.cut.lock().contains(&(from, to))
    }
}

/// One member's way out.
pub(crate) struct Wire {
    from: usize,
    links: Arc<Links>,
    rng: Mutex<Rng>,
}

impl Wire {
    pub(crate) fn new(from: usize, links: Arc<Links>, rng: Rng) -> Self {
        Self {
            from,
            links,
            rng: Mutex::new(rng),
        }
    }

    fn delay(&self) -> Duration {
        Duration::from_micros(200 + self.rng.lock().below(4_800))
    }
}

/// The member index in a broker name (`b1` is member 0).
pub(crate) fn member_index(broker: &str) -> Option<usize> {
    broker
        .strip_prefix('b')?
        .parse::<usize>()
        .ok()?
        .checked_sub(1)
}

#[async_trait]
impl RaftTransport for Wire {
    async fn send(&self, request: RaftRequest) -> Result<RaftReply, Refused> {
        let lost = |what: &str| Refused::Unreachable(format!("simulated link: {what}"));
        let to = member_index(&request.to().broker).ok_or_else(|| lost("no such broker"))?;
        tokio::time::sleep(self.delay()).await;
        if self.links.is_cut(self.from, to) {
            return Err(lost("request dropped"));
        }
        let runtime = self
            .links
            .runtimes
            .get()
            .and_then(|runtimes| runtimes.get(to))
            .ok_or_else(|| lost("not connected"))?;
        let answer = runtime.handle(request).await;
        tokio::time::sleep(self.delay()).await;
        // Checked after the member handled it: a one-way cut loses the
        // answer to a request that took effect.
        if self.links.is_cut(to, self.from) {
            return Err(lost("answer dropped"));
        }
        answer
    }
}
