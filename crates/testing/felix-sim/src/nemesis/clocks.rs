//! Clock skew: each node's clocks drawn from the seed, then stepped and
//! re-skewed while the faults run.
//!
//! [`ClockSkew::Bounded`] stays inside what the docs say Felix tolerates:
//!
//! - A lease clock runs within 20% of true speed. Lease safety assumes a
//!   bound on drift rate, and the margins are checked against clocks that
//!   drift by a quarter of the lease (`docs/replication-design.md`, "The
//!   clock assumption, stated precisely").
//! - A wall clock sits within 1 second of true time, so two nodes disagree by
//!   at most the 2 seconds of skew lease-token checks allow (`docs/auth.md`).
//!   The sim's clients claim no leases yet, so no cross-node wall comparison
//!   is exercised; see "Clock skew" in `docs/simulation.md`.
//!
//! [`ClockSkew::Unbounded`] goes past both, to show what breaks there.
//!
//! The schedule has its own stream of the seed, so turning skew on does not
//! move any other fault.

use std::fmt;
use std::time::Duration;

use felix_cluster::history::rng::Rng;

use crate::clock::{Clocks, NodeClock, Owner, PPM};

/// How far from true speed a lease clock may run under
/// [`ClockSkew::Bounded`], in parts per million: 20%.
pub const BOUNDED_RATE_PPM: u32 = PPM / 5;
/// How far a wall clock may sit from true time under [`ClockSkew::Bounded`].
pub const BOUNDED_WALL_MS: u64 = 1_000;
const UNBOUNDED_WALL_MS: u64 = 30_000;

/// How far the nemesis skews clocks.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum ClockSkew {
    /// Every node reads the shared virtual time.
    #[default]
    Off,
    /// Within the bounds the docs claim (module docs).
    Bounded,
    /// Past them: lease clocks at 0.25x to 0.7x or 1.3x to 4x true speed,
    /// wall clocks drawn from up to 30 seconds off.
    Unbounded,
}

impl ClockSkew {
    /// Parse `off`, `bounded` or `unbounded`, as `FELIX_SIM_CLOCKS` gives it.
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "off" => Some(Self::Off),
            "bounded" => Some(Self::Bounded),
            "unbounded" => Some(Self::Unbounded),
            _ => None,
        }
    }

    pub fn name(self) -> &'static str {
        match self {
            Self::Off => "off",
            Self::Bounded => "bounded",
            Self::Unbounded => "unbounded",
        }
    }

    /// A lease clock rate, in parts per million.
    fn rate(self, rng: &mut Rng) -> u32 {
        match self {
            Self::Off => PPM,
            Self::Bounded => between(rng, PPM - BOUNDED_RATE_PPM, PPM + BOUNDED_RATE_PPM),
            // Half slow, half fast, both past a quarter.
            Self::Unbounded if rng.percent(50) => between(rng, PPM / 4, PPM * 7 / 10),
            Self::Unbounded => between(rng, PPM * 13 / 10, PPM * 4),
        }
    }

    /// A wall clock offset, in millis.
    fn wall_offset_ms(self, rng: &mut Rng) -> i64 {
        let bound = match self {
            Self::Off => return 0,
            Self::Bounded => BOUNDED_WALL_MS,
            Self::Unbounded => UNBOUNDED_WALL_MS,
        };
        rng.below(2 * bound + 1) as i64 - bound as i64
    }
}

/// A change to one node's clock.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClockChange {
    /// The wall clock steps to this offset from true time.
    StepWall { to_ms: i64 },
    /// The lease clock runs at this rate from now on.
    Rate { ppm: u32 },
}

/// A clock change and when it lands, counted from the start of the faults.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ClockStep {
    pub at: Duration,
    pub owner: Owner,
    pub change: ClockChange,
}

impl fmt::Display for ClockStep {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.change {
            ClockChange::StepWall { to_ms } => {
                write!(f, "clock: {} wall clock steps to {to_ms:+} ms", self.owner)
            }
            ClockChange::Rate { ppm } => write!(
                f,
                "clock: {} lease clock runs at {:.3}x",
                self.owner,
                f64::from(ppm) / f64::from(PPM)
            ),
        }
    }
}

/// Every node's clock at the start, and the changes the run makes to them.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct ClockPlan {
    pub initial: Clocks,
    pub steps: Vec<ClockStep>,
}

impl ClockPlan {
    /// The clocks for `brokers` brokers under `skew`, with changes until
    /// `window`. Empty under [`ClockSkew::Off`].
    pub fn generate(seed: u64, skew: ClockSkew, brokers: usize, window: Duration) -> Self {
        if skew == ClockSkew::Off {
            return Self {
                initial: Clocks {
                    controlplane: NodeClock::default(),
                    brokers: vec![NodeClock::default(); brokers],
                },
                steps: Vec::new(),
            };
        }
        let mut rng = Rng::new(seed ^ 0x434c_4f43_4b53_0001);
        let draw = |rng: &mut Rng| NodeClock::new(skew.wall_offset_ms(rng), skew.rate(rng));
        let controlplane = draw(&mut rng);
        let brokers_clocks = (0..brokers).map(|_| draw(&mut rng)).collect();
        let secs = |rng: &mut Rng, low: u64, high: u64| {
            rng.between(Duration::from_secs(low), Duration::from_secs(high))
        };
        let mut steps = Vec::new();
        let mut at = secs(&mut rng, 2, 10);
        while at < window {
            let owner = match rng.below(brokers as u64 + 1) as usize {
                0 => Owner::ControlPlane,
                n => Owner::Broker(n - 1),
            };
            let change = if rng.percent(50) {
                ClockChange::StepWall {
                    to_ms: skew.wall_offset_ms(&mut rng),
                }
            } else {
                ClockChange::Rate {
                    ppm: skew.rate(&mut rng),
                }
            };
            steps.push(ClockStep { at, owner, change });
            at += secs(&mut rng, 3, 12);
        }
        Self {
            initial: Clocks {
                controlplane,
                brokers: brokers_clocks,
            },
            steps,
        }
    }

    /// One line per node, for the run's log and fault timeline.
    pub fn describe_initial(&self) -> Vec<String> {
        let owners = std::iter::once(Owner::ControlPlane)
            .chain((0..self.initial.brokers.len()).map(Owner::Broker));
        owners
            .map(|owner| {
                let clock = self.initial.get(owner);
                format!(
                    "clock: {owner} starts with its wall clock {:+} ms off, lease clock at {:.3}x",
                    clock.wall_offset_ms(),
                    f64::from(clock.rate_ppm()) / f64::from(PPM)
                )
            })
            .collect()
    }
}

/// Apply `step` to the running clocks.
pub(crate) fn apply(step: &ClockStep) {
    crate::clock::change(step.owner, |clock, base| match step.change {
        ClockChange::StepWall { to_ms } => clock.step_wall(to_ms),
        ClockChange::Rate { ppm } => clock.set_rate(base, ppm),
    });
}

fn between(rng: &mut Rng, low: u32, high: u32) -> u32 {
    low + rng.below(u64::from(high - low) + 1) as u32
}

#[cfg(test)]
mod tests;
