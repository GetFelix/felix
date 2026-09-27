//! What the proxies do to each directed link right now.

use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use tokio::sync::watch;

use crate::fault::Endpoint;

/// Per directed link, whether traffic is dropped and how late it arrives.
///
/// Shared by every proxy. Directed because an asymmetric partition is the
/// case worth having: `a -> b` dropped says nothing about `b -> a`.
pub(crate) struct Rules {
    links: Mutex<Vec<Link>>,
    /// False while no link is faulted, so a healthy cluster pays one atomic
    /// load per packet rather than a lock.
    any: AtomicBool,
    /// Bumped on every change, for connections that must react to a heal
    /// without waiting for their next byte.
    changed: watch::Sender<u64>,
    /// Packets that passed while some link was faulted without the proxy
    /// knowing whose they were. Each one bypassed whatever fault applied to
    /// its sender, so a test that sees any cannot trust its link faults.
    unattributed: AtomicU64,
}

/// What happens to one packet or chunk.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Verdict {
    Drop,
    /// Deliver, this much later.
    Deliver(Duration),
}

struct Link {
    from: Endpoint,
    to: Endpoint,
    dropped: bool,
    delay: Duration,
}

impl Rules {
    pub(crate) fn new() -> Self {
        Self {
            links: Mutex::new(Vec::new()),
            any: AtomicBool::new(false),
            changed: watch::channel(0).0,
            unattributed: AtomicU64::new(0),
        }
    }

    /// Drop, or stop dropping, everything from `from` to `to`.
    pub(crate) fn set_dropped(&self, from: &Endpoint, to: &Endpoint, dropped: bool) {
        self.update(from, to, |link| link.dropped = dropped);
    }

    /// Hold everything from `from` to `to` for `delay`. Zero clears it.
    pub(crate) fn set_delay(&self, from: &Endpoint, to: &Endpoint, delay: Duration) {
        self.update(from, to, |link| link.delay = delay);
    }

    /// The verdict for traffic from `from` to `to`. An end the proxy could
    /// not attribute (`None`) is on no faulted link, and is counted in
    /// [`Self::unattributed`].
    pub(crate) fn verdict(&self, from: Option<&Endpoint>, to: Option<&Endpoint>) -> Verdict {
        if !self.any.load(Ordering::Acquire) {
            return Verdict::Deliver(Duration::ZERO);
        }
        let (Some(from), Some(to)) = (from, to) else {
            // Delivered rather than dropped: guessing would fault links the
            // test never named. Loud instead, since it quietly weakens the fault.
            if self.unattributed.fetch_add(1, Ordering::Relaxed) == 0 {
                tracing::warn!(
                    ?from,
                    ?to,
                    "link proxy passed a datagram it could not attribute; link faults may not hold",
                );
            }
            return Verdict::Deliver(Duration::ZERO);
        };
        let links = self.links.lock().expect("rules lock");
        match links
            .iter()
            .find(|link| &link.from == from && &link.to == to)
        {
            Some(link) if link.dropped => Verdict::Drop,
            Some(link) => Verdict::Deliver(link.delay),
            None => Verdict::Deliver(Duration::ZERO),
        }
    }

    /// Whether traffic either way between `a` and `b` is being dropped.
    pub(crate) fn severed(&self, a: &Endpoint, b: &Endpoint) -> bool {
        self.verdict(Some(a), Some(b)) == Verdict::Drop
            || self.verdict(Some(b), Some(a)) == Verdict::Drop
    }

    /// How many packets passed a faulted network unattributed so far.
    pub(crate) fn unattributed(&self) -> u64 {
        self.unattributed.load(Ordering::Relaxed)
    }

    /// Resolves after the next change to any link.
    pub(crate) fn subscribe(&self) -> watch::Receiver<u64> {
        self.changed.subscribe()
    }

    fn update(&self, from: &Endpoint, to: &Endpoint, change: impl FnOnce(&mut Link)) {
        let mut links = self.links.lock().expect("rules lock");
        let index = match links
            .iter()
            .position(|link| &link.from == from && &link.to == to)
        {
            Some(index) => index,
            None => {
                links.push(Link {
                    from: from.clone(),
                    to: to.clone(),
                    dropped: false,
                    delay: Duration::ZERO,
                });
                links.len() - 1
            }
        };
        change(&mut links[index]);
        links.retain(|link| link.dropped || !link.delay.is_zero());
        self.any.store(!links.is_empty(), Ordering::Release);
        drop(links);
        self.changed.send_modify(|generation| *generation += 1);
    }
}
