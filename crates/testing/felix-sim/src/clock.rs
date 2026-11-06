//! The clocks the brokers and the control plane read.
//!
//! Tokio's paused time drives timers and sleeps. The lease, record times,
//! TTLs and the control plane's stamps read `felix_common::clock`, which the
//! simulator points here. Each broker and the control plane has its own
//! [`NodeClock`] over the shared virtual time: a lease clock that runs at its
//! own rate, and a wall clock with its own offset. The nemesis changes them
//! mid-run (`nemesis::clocks`).
//!
//! All of them run in one process, so a reading has to know whose code asked.
//! [`on`] and [`as_owner`] mark the code the harness calls into a node with.
//! Under `--cfg tokio_unstable` the runtime's task hooks ([`attribute_tasks`])
//! carry the mark to every task that code spawns, so a broker's background
//! loops read its clock too. Without the hooks spawned tasks would read the
//! shared clock, which is why a run with skew needs the flag. Code no node
//! owns (clients, checkers) reads the shared clock.

use std::cell::{Cell, RefCell};
use std::future::Future;
use std::time::Duration;

use tokio::time::Instant;

/// One part-per-million of clock speed: 1x is `1_000_000`.
pub const PPM: u32 = 1_000_000;

/// Whose clock a reading uses.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Owner {
    ControlPlane,
    /// The broker at this index.
    Broker(usize),
}

impl Owner {
    /// The broker named `node_id` (`b1`, `b2`, ...), as the cluster names them.
    pub fn of_node(node_id: &str) -> Option<Self> {
        let n: usize = node_id.strip_prefix('b')?.parse().ok()?;
        n.checked_sub(1).map(Owner::Broker)
    }
}

impl std::fmt::Display for Owner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Owner::ControlPlane => write!(f, "the control plane"),
            Owner::Broker(index) => write!(f, "b{}", index + 1),
        }
    }
}

/// One node's view of time, against the shared virtual time (`base`).
///
/// The lease clock is monotonic and runs at its own rate: it never steps, and
/// a rate change keeps the drift already accumulated, so it never goes
/// backwards. The wall clock is the shared time plus an offset that a step
/// moves either way, as an NTP correction does. Integer arithmetic, so a
/// reading is the same on every platform.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NodeClock {
    rate_ppm: u32,
    /// Lease-clock drift accumulated before `since`.
    drift_nanos: i128,
    /// The base reading at which `rate_ppm` took effect.
    since: Duration,
    wall_offset_nanos: i128,
}

impl Default for NodeClock {
    fn default() -> Self {
        Self::new(0, PPM)
    }
}

impl NodeClock {
    pub fn new(wall_offset_ms: i64, rate_ppm: u32) -> Self {
        Self {
            rate_ppm,
            drift_nanos: 0,
            since: Duration::ZERO,
            wall_offset_nanos: i128::from(wall_offset_ms) * 1_000_000,
        }
    }

    /// The lease clock at shared time `base`.
    pub fn lease(&self, base: Duration) -> Duration {
        from_nanos(base.as_nanos() as i128 + self.drift_at(base))
    }

    /// The wall clock at shared time `base`, counted from the same zero.
    pub fn wall(&self, base: Duration) -> Duration {
        from_nanos(base.as_nanos() as i128 + self.wall_offset_nanos)
    }

    pub fn rate_ppm(&self) -> u32 {
        self.rate_ppm
    }

    pub fn wall_offset_ms(&self) -> i64 {
        (self.wall_offset_nanos / 1_000_000) as i64
    }

    /// Run the lease clock at `rate_ppm` from `base` on.
    pub fn set_rate(&mut self, base: Duration, rate_ppm: u32) {
        self.drift_nanos = self.drift_at(base);
        self.since = base;
        self.rate_ppm = rate_ppm;
    }

    /// Step the wall clock to `wall_offset_ms` from the shared time.
    pub fn step_wall(&mut self, wall_offset_ms: i64) {
        self.wall_offset_nanos = i128::from(wall_offset_ms) * 1_000_000;
    }

    fn drift_at(&self, base: Duration) -> i128 {
        let running = base.saturating_sub(self.since).as_nanos() as i128;
        self.drift_nanos + running * (i128::from(self.rate_ppm) - i128::from(PPM)) / i128::from(PPM)
    }

    /// What `felix_common::clock::sim` adds to the lease reading to get the
    /// wall reading.
    fn wall_past_lease(&self, base: Duration) -> i128 {
        self.wall_offset_nanos - self.drift_at(base)
    }
}

fn from_nanos(nanos: i128) -> Duration {
    Duration::from_nanos(u64::try_from(nanos.max(0)).unwrap_or(u64::MAX))
}

/// Every node's clock in one run.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Clocks {
    pub controlplane: NodeClock,
    pub brokers: Vec<NodeClock>,
}

impl Clocks {
    pub fn get(&self, owner: Owner) -> NodeClock {
        match owner {
            Owner::ControlPlane => self.controlplane,
            Owner::Broker(index) => self.brokers.get(index).copied().unwrap_or_default(),
        }
    }

    fn get_mut(&mut self, owner: Owner) -> Option<&mut NodeClock> {
        match owner {
            Owner::ControlPlane => Some(&mut self.controlplane),
            Owner::Broker(index) => self.brokers.get_mut(index),
        }
    }
}

thread_local! {
    static START: Cell<Option<Instant>> = const { Cell::new(None) };
    static CLOCKS: RefCell<Clocks> = RefCell::new(Clocks::default());
    /// Whose code is running now. `None` is the harness.
    static CURRENT: Cell<Option<Owner>> = const { Cell::new(None) };
    /// The owner of each task some owner's code spawned.
    #[cfg(tokio_unstable)]
    static TASKS: RefCell<std::collections::HashMap<tokio::task::Id, Owner>> =
        RefCell::new(std::collections::HashMap::new());
}

/// Shared virtual time since [`install`] was called on this thread.
fn base() -> Duration {
    START.with(|start| start.get().map_or(Duration::ZERO, |at| at.elapsed()))
}

/// The lease clock of whoever is running.
fn lease_reading() -> Duration {
    let base = base();
    match CURRENT.get() {
        Some(owner) => CLOCKS.with_borrow(|clocks| clocks.get(owner).lease(base)),
        None => base,
    }
}

fn wall_offset_reading() -> i128 {
    match CURRENT.get() {
        Some(owner) => CLOCKS.with_borrow(|clocks| clocks.get(owner).wall_past_lease(base())),
        None => 0,
    }
}

/// Point `felix_common::clock` at this thread's tokio clock, counting from
/// now, with every node on `clocks`. Call on the runtime a run will use, with
/// its clock paused.
///
/// The readers are plain functions over thread locals, so runs on different
/// threads (parallel tests) each see their own clocks.
pub(crate) fn install(clocks: Clocks) {
    START.with(|start| start.set(Some(Instant::now())));
    CLOCKS.with_borrow_mut(|current| *current = clocks);
    felix_common::clock::sim::install(lease_reading);
    felix_common::clock::sim::install_wall_offset(wall_offset_reading);
}

/// Change `owner`'s clock now, and return it as changed.
pub(crate) fn change(owner: Owner, change: impl FnOnce(&mut NodeClock, Duration)) -> NodeClock {
    let base = base();
    CLOCKS.with_borrow_mut(|clocks| match clocks.get_mut(owner) {
        Some(clock) => {
            change(clock, base);
            *clock
        }
        None => NodeClock::default(),
    })
}

/// Run `work` as `owner`'s code: its clock reads are `owner`'s, and so are
/// those of any task it spawns (under the task hooks).
pub(crate) fn as_owner<R>(owner: Owner, work: impl FnOnce() -> R) -> R {
    struct Restore(Option<Owner>);
    impl Drop for Restore {
        fn drop(&mut self) {
            CURRENT.set(self.0);
        }
    }
    let _restore = Restore(CURRENT.replace(Some(owner)));
    work()
}

/// `fut`, polled as `owner`'s code. See [`as_owner`].
pub(crate) async fn on<F: Future>(owner: Owner, fut: F) -> F::Output {
    let mut fut = std::pin::pin!(fut);
    std::future::poll_fn(|cx| as_owner(owner, || fut.as_mut().poll(cx))).await
}

/// Give each task the owner of the code that spawned it, and run its polls as
/// that owner.
#[cfg(tokio_unstable)]
pub(crate) fn attribute_tasks(builder: &mut tokio::runtime::Builder) {
    builder
        .on_task_spawn(|meta| {
            if let Some(owner) = CURRENT.get() {
                TASKS.with_borrow_mut(|tasks| tasks.insert(meta.id(), owner));
            }
        })
        .on_before_task_poll(|meta| {
            CURRENT.set(TASKS.with_borrow(|tasks| tasks.get(&meta.id()).copied()));
        })
        .on_after_task_poll(|_| CURRENT.set(None))
        .on_task_terminate(|meta| {
            // Also called while the runtime drops, possibly at thread exit.
            let _ = TASKS.try_with(|tasks| tasks.borrow_mut().remove(&meta.id()));
        });
}

#[cfg(test)]
mod tests;
