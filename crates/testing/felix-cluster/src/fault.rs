//! The faults a test can inject into a running cluster, as values.
//!
//! One vocabulary for every family, so faults compose: a test builds the
//! situation it wants out of several (a leader cut off one way from its
//! followers, with its clock running fast) and hands each to
//! [`Cluster::inject`](crate::Cluster::inject). Each is undone by
//! [`Cluster::heal`](crate::Cluster::heal) with the same value, or all at once
//! by [`Cluster::heal_all`](crate::Cluster::heal_all).
//!
//! What each one does to the process is in `docs/cluster-harness.md`.

use std::time::Duration;

/// A process at one end of a link, or whose clock is being skewed.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum Endpoint {
    /// A broker, by node id.
    Node(String),
    /// The control plane. There is one.
    ControlPlane,
}

impl Endpoint {
    /// A broker, by node id.
    pub fn node(node_id: impl Into<String>) -> Self {
        Self::Node(node_id.into())
    }
}

/// Something wrong with the cluster, injected on purpose.
#[derive(Clone, Debug, PartialEq)]
pub enum Fault {
    /// Traffic from `from` to `to` is lost; the other direction still flows.
    ///
    /// Needs a cluster started with
    /// [`ClusterConfig::proxy_links`](crate::ClusterConfig::proxy_links).
    /// Broker to broker is the peer transport (QUIC, so datagrams are
    /// dropped); broker to control plane is HTTP (a black hole: bytes are
    /// accepted and never arrive).
    Drop { from: Endpoint, to: Endpoint },
    /// Traffic from `from` to `to` arrives `by` late. Proxied links only, as
    /// for [`Fault::Drop`].
    Delay {
        from: Endpoint,
        to: Endpoint,
        by: Duration,
    },
    /// `node`'s peer transport refuses its own requests to `peers` at once,
    /// through the broker's test-only partition file. Coarser than
    /// [`Fault::Drop`] (a request fails fast rather than timing out, and
    /// requests *from* `peers` still get answers), and it needs no proxy.
    Refuse { node: String, peers: Vec<String> },
    /// `node` is stopped with `SIGSTOP`: alive, holding every lease and
    /// connection, answering nothing. Unix only.
    Suspend { node: String },
    /// `process` reads a different time. See [`ClockFault`].
    Clock {
        process: Endpoint,
        fault: ClockFault,
    },
    /// `node`'s flushes are slow or fail. See [`FsyncFault`].
    Fsync { node: String, fault: FsyncFault },
}

impl Fault {
    /// Both directions between `a` and `b` dropped: a symmetric partition,
    /// as the two one-way faults it is made of.
    pub fn partition(a: Endpoint, b: Endpoint) -> [Fault; 2] {
        [
            Fault::Drop {
                from: a.clone(),
                to: b.clone(),
            },
            Fault::Drop { from: b, to: a },
        ]
    }
}

/// How a process's clock is wrong.
///
/// The skew applies to everything that process reads through
/// `felix_common::clock`: a broker's lease clock, and the control plane's
/// heartbeat stamps and expiry.
///
/// A broker's lease clock is `CLOCK_BOOTTIME`, which never goes backwards, so
/// [`Cluster::inject`](crate::Cluster::inject) refuses a backward step on a
/// broker, and healing a broker's clock keeps what the fault already moved: a
/// rate goes back to 1x with its drift kept, a forward step stays taken. The
/// control plane's wall clock can be stepped either way, and healing it puts
/// it back on the true clock at once, which is itself a step.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum ClockFault {
    /// Move the clock by this many milliseconds. Steps add up; a step
    /// injected before anything happens is a fixed offset. Backwards only on
    /// the control plane.
    StepMillis(i64),
    /// Run at this multiple of real time from now on: `2.0` is twice as
    /// fast, `0.5` half. Drift so far is kept when the rate changes again.
    Rate(f64),
}

impl ClockFault {
    /// A step forwards.
    pub fn forward(by: Duration) -> Self {
        Self::StepMillis(i64::try_from(by.as_millis()).unwrap_or(i64::MAX))
    }

    /// A step backwards. The control plane only.
    pub fn back(by: Duration) -> Self {
        Self::StepMillis(-i64::try_from(by.as_millis()).unwrap_or(i64::MAX))
    }
}

/// How a broker's disk misbehaves on a flush.
///
/// Every flush the storage crate issues passes the fault first: segment
/// data, indexes, directory entries and the durable mark alike. A failure is
/// reported instead of flushing; nothing is dropped from the page cache.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FsyncFault {
    /// Each flush waits this long first: a slow disk.
    Delay(Duration),
    /// Every flush fails with `EIO` until healed: a disk that has gone bad.
    Fail,
    /// The next flush fails with `EIO` and later ones succeed, the way Linux
    /// reports a writeback error once. What catches code that retries a
    /// failed fsync and trusts the retry.
    FailOnce,
}
