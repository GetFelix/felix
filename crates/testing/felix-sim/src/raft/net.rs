//! The links between the members of a Raft simulation. Each member sends
//! through its own [`Wire`], so a cut can be one-way: a request that never
//! arrives, or one that is handled and whose answer is lost.

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use felix_cluster::history::rng::Rng;
use felix_replication::raft::{GroupRuntime, RaftReply, RaftRequest, RaftTransport, Refused};
use parking_lot::Mutex;

/// Which member can reach which. `(from, to)` in `cut` drops everything
/// `from` sends `to`, answers included. A member in `down` is stopped: it
/// reaches nobody and nobody reaches it, whatever a heal does.
#[derive(Default)]
pub(crate) struct Links {
    cut: Mutex<BTreeSet<(usize, usize)>>,
    down: Mutex<BTreeSet<usize>>,
    runtimes: Mutex<Vec<GroupRuntime>>,
}

impl Links {
    /// The runtimes messages are handed to, in member order, once every
    /// runtime has been started on its wire.
    pub(crate) fn connect(&self, runtimes: Vec<GroupRuntime>) {
        *self.runtimes.lock() = runtimes;
    }

    /// Hand `member`'s messages to `runtime` from now on, as after a restart.
    pub(crate) fn replace(&self, member: usize, runtime: GroupRuntime) {
        self.runtimes.lock()[member] = runtime;
    }

    pub(crate) fn set_down(&self, member: usize, down: bool) {
        let mut stopped = self.down.lock();
        if down {
            stopped.insert(member);
        } else {
            stopped.remove(&member);
        }
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
        let down = self.down.lock();
        down.contains(&from) || down.contains(&to) || self.cut.lock().contains(&(from, to))
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
        let runtime = self.links.runtimes.lock().get(to).cloned();
        let runtime = runtime.ok_or_else(|| lost("not connected"))?;
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
