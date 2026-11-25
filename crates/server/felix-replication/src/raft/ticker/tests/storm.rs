//! Many groups electing at once after a broker dies (#2317), on a mock
//! clock and a modelled disk, through the real election bound.
//!
//! Three brokers, `groups` groups with a member on each. Broker 0 led every
//! group and is gone, so each group's two other members must elect. Each
//! live broker has one disk that does one vote save at a time, and a save
//! costs `fsyncs_per_vote` fsyncs of `fsync_us` each. A member saves its own
//! vote before asking, and a voter saves its vote before answering, as
//! openraft does. Places come from each broker's [`Elections`]. A member let
//! in stands at the next tick, since its leader was last heard well over its
//! election timeout ago, and again each timeout (fixed per member, between
//! `election_min` and `election_max`, as openraft picks it) while it holds
//! the place. Pre-vote runs first.

use std::cmp::Reverse;
use std::collections::BinaryHeap;

use super::*;

/// Microseconds, the clock every event runs on.
const MS: u64 = 1_000;
const TICK: u64 = 50 * MS;
const ELECTION_MIN: u64 = 1_000 * MS;
const ELECTION_MAX: u64 = 2_000 * MS;
const HORIZON: u64 = 180_000 * MS;
/// Who the members voted for before broker 0 died.
const DEAD_LEADER: usize = usize::MAX;

#[derive(Clone, Copy)]
struct Disk {
    fsync_us: u64,
    fsyncs_per_vote: u64,
}

#[derive(Debug, Default)]
struct Outcome {
    /// When the last group was led, or `None` if one was not by the horizon.
    all_led_ms: Option<u64>,
    fsyncs: u64,
    /// Pre-vote rounds started, and vote rounds (own votes saved).
    elections: u64,
    votes: u64,
    highest_term: u64,
}

#[derive(PartialEq, Eq, PartialOrd, Ord)]
enum Event {
    Tick,
    Timeout {
        node: usize,
        armed: u64,
    },
    OwnVoteSaved {
        node: usize,
        term: u64,
        ticket: u64,
        began: u64,
    },
    GrantSaved {
        node: usize,
        candidate: usize,
        term: u64,
        ticket: u64,
        began: u64,
    },
}

struct Node {
    camp: Arc<Member>,
    /// openraft's election timeout for this member.
    timeout: u64,
    term: u64,
    voted: Option<usize>,
    /// Bumped whenever the election timer is turned on or off, so a stale
    /// timeout does nothing.
    armed: u64,
    timer_on: bool,
    /// A granted vote settles the member until then, unless its group leads.
    granted_until: u64,
}

struct Sim {
    disk: Disk,
    nodes: Vec<Node>,
    led: Vec<bool>,
    left: usize,
    brokers: [Elections<Member>; 2],
    saves: [Arc<VoteSaves>; 2],
    disk_free: [u64; 2],
    events: BinaryHeap<Reverse<(u64, u64, Event)>>,
    seq: u64,
    out: Outcome,
}

fn timers(most: usize) -> Timers {
    Timers {
        started: Instant::now(),
        tick: TICK / MS,
        heartbeat: 200,
        peer_deadline: Duration::from_millis(800),
        idle_heartbeat: 2000,
        idle_after: 5000,
        election_min: ELECTION_MIN / MS,
        election_max: ELECTION_MAX / MS,
        lease: ELECTION_MAX / MS,
        max_elections_at_once: most,
        commit_hold: Duration::from_millis(1),
    }
}

impl Sim {
    fn new(groups: usize, disk: Disk, most: usize) -> Self {
        let saves = [Arc::<VoteSaves>::default(), Arc::<VoteSaves>::default()];
        let timers = timers(most);
        let nodes = (0..groups * 2)
            .map(|node| Node {
                // As the ticker orders them: a hash of group and member.
                camp: Member::new(splitmix(node as u64)),
                timeout: ELECTION_MIN + splitmix(!(node as u64)) % (ELECTION_MAX - ELECTION_MIN),
                term: 1,
                voted: Some(DEAD_LEADER),
                armed: 0,
                timer_on: false,
                granted_until: 0,
            })
            .collect();
        let mut sim = Self {
            disk,
            nodes,
            led: vec![false; groups],
            left: groups,
            brokers: [
                Elections::new(&timers, Arc::clone(&saves[0])),
                Elections::new(&timers, Arc::clone(&saves[1])),
            ],
            saves,
            disk_free: [0; 2],
            events: BinaryHeap::new(),
            seq: 0,
            out: Outcome::default(),
        };
        sim.at(0, Event::Tick);
        sim
    }

    fn at(&mut self, time: u64, event: Event) {
        self.seq += 1;
        self.events.push(Reverse((time, self.seq, event)));
    }

    fn broker(node: usize) -> usize {
        node % 2
    }

    fn group_led(&self, node: usize) -> bool {
        self.led[node / 2]
    }

    /// Queue a vote save on `node`'s disk; returns when it ends and its
    /// ticket.
    fn save(&mut self, node: usize, now: u64) -> (u64, u64) {
        let broker = Self::broker(node);
        let ticket = self.saves[broker].began();
        let start = self.disk_free[broker].max(now);
        let done = start + self.disk.fsyncs_per_vote * self.disk.fsync_us;
        self.disk_free[broker] = done;
        self.out.fsyncs += self.disk.fsyncs_per_vote;
        (done, ticket)
    }

    fn saved(&self, node: usize, now: u64, ticket: u64, began: u64) {
        let saves = &self.saves[Self::broker(node)];
        saves.ended(ticket, now / MS, now.max(1), Some(now - began));
    }

    fn run(mut self) -> Outcome {
        while let Some(Reverse((now, _, event))) = self.events.pop() {
            if self.left == 0 || now > HORIZON {
                break;
            }
            match event {
                Event::Tick => self.tick(now),
                Event::Timeout { node, armed } => self.stand(node, armed, now),
                Event::OwnVoteSaved {
                    node,
                    term,
                    ticket,
                    began,
                } => {
                    self.saved(node, now, ticket, began);
                    self.ask(node, term, now);
                }
                Event::GrantSaved {
                    node,
                    candidate,
                    term,
                    ticket,
                    began,
                } => {
                    self.saved(node, now, ticket, began);
                    self.granted(node, candidate, term, now);
                }
            }
        }
        self.out.highest_term = self.nodes.iter().map(|node| node.term).max().unwrap_or(0);
        self.out
    }

    fn tick(&mut self, now: u64) {
        for node in 0..self.nodes.len() {
            let n = &self.nodes[node];
            if !self.led[node / 2] && n.granted_until <= now {
                n.camp.settled.store(false, Ordering::Relaxed);
            }
        }
        for broker in 0..2 {
            let wanting: Vec<_> = (broker..self.nodes.len())
                .step_by(2)
                .filter(|&node| !self.nodes[node].camp.settled())
                .map(|node| Arc::clone(&self.nodes[node].camp))
                .collect();
            let elections = &mut self.brokers[broker];
            elections.settle(now / MS);
            elections.queue(wanting.into_iter(), now / MS);
            elections.admit(now / MS);
        }
        // openraft's own timer, on while the bound holds a place.
        for node in 0..self.nodes.len() {
            let armed = self.nodes[node].camp.armed();
            if armed != self.nodes[node].timer_on {
                self.nodes[node].timer_on = armed;
                self.nodes[node].armed += 1;
                if armed {
                    let armed = self.nodes[node].armed;
                    self.at(now + TICK, Event::Timeout { node, armed });
                }
            }
        }
        self.at(now + TICK, Event::Tick);
    }

    /// The election timeout ran out: a pre-vote round, and if it is
    /// granted, a vote for itself at the next term.
    fn stand(&mut self, node: usize, armed: u64, now: u64) {
        if self.nodes[node].armed != armed || self.group_led(node) {
            return;
        }
        let at = now + self.nodes[node].timeout;
        self.at(at, Event::Timeout { node, armed });
        if self.nodes[node].camp.settled() {
            return;
        }
        self.out.elections += 1;
        // The request to the dead broker fails at once; the peer answers.
        self.nodes[node].camp.stand(2);
        let peer = node ^ 1;
        let next = self.nodes[node].term + 1;
        let (peer_term, peer_voted) = (self.nodes[peer].term, self.nodes[peer].voted);
        let granted = self.nodes[peer].granted_until <= now
            && (next > peer_term || (next == peer_term && peer_voted == Some(node)));
        if !granted {
            self.nodes[node].term = self.nodes[node].term.max(peer_term);
            return;
        }
        self.nodes[node].term = next;
        self.nodes[node].voted = Some(node);
        self.nodes[node].camp.saving.store(true, Ordering::Relaxed);
        let (done, ticket) = self.save(node, now);
        let event = Event::OwnVoteSaved {
            node,
            term: next,
            ticket,
            began: now,
        };
        self.at(done, event);
    }

    /// The candidate's own vote is on disk: it asks its peer, which saves
    /// its vote before it answers.
    fn ask(&mut self, node: usize, term: u64, now: u64) {
        self.nodes[node].camp.saving.store(false, Ordering::Relaxed);
        if self.nodes[node].term != term || self.nodes[node].voted != Some(node) {
            return;
        }
        if self.group_led(node) {
            return;
        }
        self.out.votes += 1;
        self.nodes[node].camp.asks.fetch_add(2, Ordering::Relaxed);
        self.nodes[node].camp.out.fetch_add(1, Ordering::Relaxed);
        let peer = node ^ 1;
        if term > self.nodes[peer].term {
            self.nodes[peer].term = term;
            self.nodes[peer].voted = None;
        }
        if term < self.nodes[peer].term || self.nodes[peer].voted.is_some() {
            let peer_term = self.nodes[peer].term;
            self.nodes[node].camp.out.fetch_sub(1, Ordering::Relaxed);
            self.nodes[node].term = self.nodes[node].term.max(peer_term);
            return;
        }
        self.nodes[peer].voted = Some(node);
        // A member that grants a vote turns its timer off.
        self.nodes[peer].granted_until = now + ELECTION_MAX;
        self.nodes[peer].camp.settled.store(true, Ordering::Relaxed);
        let (done, ticket) = self.save(peer, now);
        let event = Event::GrantSaved {
            node: peer,
            candidate: node,
            term,
            ticket,
            began: now,
        };
        self.at(done, event);
    }

    fn granted(&mut self, _voter: usize, candidate: usize, term: u64, _now: u64) {
        self.nodes[candidate]
            .camp
            .out
            .fetch_sub(1, Ordering::Relaxed);
        let current =
            self.nodes[candidate].term == term && self.nodes[candidate].voted == Some(candidate);
        if !current || self.group_led(candidate) {
            return;
        }
        self.led[candidate / 2] = true;
        self.left -= 1;
        if self.left == 0 {
            self.out.all_led_ms = Some(_now / MS);
        }
        for node in [candidate, candidate ^ 1] {
            self.nodes[node].camp.settled.store(true, Ordering::Relaxed);
        }
    }
}

fn splitmix(seed: u64) -> u64 {
    let mut z = seed.wrapping_add(0x9E37_79B9_7F4A_7C15);
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

/// Fsyncs a vote at a new term costs on a `DiskLog`: the ballot file and
/// its directory. `felix-storage`'s
/// `a_vote_at_a_new_generation_writes_only_the_ballot` pins it.
const VOTE_FSYNCS: u64 = 2;

fn storm(groups: usize, fsync_ms: u64, fsyncs_per_vote: u64, most: usize) -> Outcome {
    let disk = Disk {
        fsync_us: fsync_ms * MS,
        fsyncs_per_vote,
    };
    Sim::new(groups, disk, most).run()
}

/// 2,000 groups lose their leader together on a disk held to 5 ms an
/// fsync. Every group is led, about one election each, at a low term, in
/// about the time the two brokers' disks take to save one vote per group.
/// At four fsyncs a vote it takes about twice as long (see `table`).
#[test]
fn two_thousand_groups_elect_once_each_after_a_broker_dies() {
    let groups = 2000;
    let out = storm(groups, 5, VOTE_FSYNCS, 64);
    let led = out.all_led_ms.expect("every group led");
    assert!(led <= 30_000, "{out:?}");
    assert!(out.highest_term <= 8, "{out:?}");
    assert!(out.elections <= groups as u64 * 3 / 2, "{out:?}");
    assert!(out.fsyncs <= groups as u64 * VOTE_FSYNCS * 5 / 2, "{out:?}");
}

/// The numbers the pull request for #2317 quotes:
/// `cargo test -p felix-replication --lib storm::table -- --ignored --nocapture`.
#[test]
#[ignore = "prints numbers, asserts nothing"]
fn table() {
    for (fsyncs_per_vote, most) in [(4, 64), (VOTE_FSYNCS, 64), (VOTE_FSYNCS, 100_000)] {
        for fsync_ms in [2, 5] {
            for groups in [100, 500, 2000] {
                let out = storm(groups, fsync_ms, fsyncs_per_vote, most);
                println!(
                    "fsyncs/vote {fsyncs_per_vote} bound {most} fsync {fsync_ms} ms groups {groups}: {out:?}"
                );
            }
        }
    }
}
