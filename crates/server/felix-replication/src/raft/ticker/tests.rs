//! The bound on elections at once, against members that only count.

use super::*;

fn elections(most: usize) -> Elections<Member> {
    let timers = Timers {
        started: Instant::now(),
        tick: 50,
        heartbeat: 200,
        idle_heartbeat: 2000,
        idle_after: 1000,
        election_min: 1000,
        election_max: 2000,
        lease: 1500,
        elections: most,
    };
    Elections::new(&timers, Arc::default())
}

/// A member whose wait for its leader ran out.
#[derive(Default)]
struct Member {
    order: u64,
    armed: AtomicBool,
    /// It leads, heard a leader or granted a vote.
    settled: AtomicBool,
    asks: AtomicU64,
    out: AtomicU64,
}

impl Member {
    fn new(order: u64) -> Arc<Self> {
        Arc::new(Self {
            order,
            ..Self::default()
        })
    }

    fn armed(&self) -> bool {
        self.armed.load(Ordering::Relaxed)
    }

    /// It sends a round of two requests; `answered` of them come back.
    fn stand(&self, answered: u64) {
        self.asks.fetch_add(2, Ordering::Relaxed);
        self.out.fetch_add(2 - answered, Ordering::Relaxed);
    }
}

impl Campaigner for Member {
    fn may_stand(&self, _since: u64) -> bool {
        !self.armed() && !self.settled.load(Ordering::Relaxed)
    }
    fn arm(&self, since: u64) -> bool {
        let may = self.may_stand(since);
        self.armed.store(may, Ordering::Relaxed);
        may
    }
    fn disarm(&self) {
        self.armed.store(false, Ordering::Relaxed);
    }
    fn settled(&self) -> bool {
        self.settled.load(Ordering::Relaxed)
    }
    fn asks(&self) -> (u64, u64) {
        (
            self.asks.load(Ordering::Relaxed),
            self.out.load(Ordering::Relaxed),
        )
    }
    fn set_waiting(&self, _waiting: bool) {}
    fn order(&self) -> u64 {
        self.order
    }
}

/// One interval of the ticker: `wanting` came due at `now`.
fn interval(elections: &mut Elections<Member>, wanting: &[Arc<Member>], now: u64) {
    elections.settle(now);
    elections.queue(wanting.iter().cloned(), now);
    elections.admit(now);
}

fn armed(members: &[Arc<Member>]) -> Vec<u64> {
    let armed = members.iter().filter(|member| member.armed());
    armed.map(|member| member.order).collect()
}

/// Ten members come due together under a bound of three: three may stand,
/// the rest wait in their order, and the next one goes in only when an
/// election ends.
#[test]
fn at_most_the_limit_stand_at_once() {
    let mut elections = elections(3);
    let members: Vec<_> = (0..10).rev().map(Member::new).collect();
    interval(&mut elections, &members, 0);
    assert_eq!(armed(&members), [2, 1, 0]);
    assert_eq!(elections.load().waiting, 7);
    // Due again next interval: still queued once.
    interval(&mut elections, &members, 50);
    assert_eq!(armed(&members), [2, 1, 0]);
    assert_eq!(elections.load().waiting, 7);
    members[9].stand(0);
    members[9].settled.store(true, Ordering::Relaxed);
    interval(&mut elections, &[], 100);
    assert_eq!(armed(&members), [3, 2, 1, 0]);
    assert_eq!(elections.load().in_flight, 3);
    assert_eq!(elections.load().peak, 3);
}

/// A member whose round is answered without it leading gives its place
/// back on the second look, and waits behind the others. Groups that
/// cannot win do not hold places from the ones that can.
#[test]
fn a_member_that_lost_waits_at_the_back() {
    let mut elections = elections(1);
    let [a, b] = [Member::new(0), Member::new(1)];
    interval(&mut elections, &[a.clone(), b.clone()], 0);
    assert!(a.armed() && !b.armed());
    a.stand(2);
    interval(&mut elections, &[], 50);
    assert!(a.armed(), "one look is not enough: a vote round may follow");
    interval(&mut elections, &[], 100);
    assert!(!a.armed() && b.armed());
    assert_eq!(elections.load().waiting, 1);
    // `b` cannot win either: its peers are silent. It keeps the place for
    // one election_min after it stood, then `a` gets it again.
    b.stand(0);
    interval(&mut elections, &[], 150);
    interval(&mut elections, &[], 1100);
    assert!(b.armed());
    interval(&mut elections, &[], 1150);
    assert!(a.armed() && !b.armed());
}

/// A member that does not stand within election_max of being let in
/// gives its place back.
#[test]
fn a_member_that_never_stands_gives_its_place_back() {
    let mut elections = elections(1);
    let [a, b] = [Member::new(0), Member::new(1)];
    interval(&mut elections, &[a.clone(), b.clone()], 0);
    interval(&mut elections, &[], 1950);
    assert!(a.armed());
    interval(&mut elections, &[], 2000);
    assert!(!a.armed() && b.armed());
}

/// The limit falls only when a vote save here was slow: to what the disk
/// would have saved in time. Each election that ends gives a place back.
#[test]
fn a_slow_vote_save_lowers_the_limit() {
    let mut elections = elections(64);
    elections.saves.saved(1000, 10);
    elections.settle(1000);
    assert_eq!(elections.limit, 64);
    elections.saves.saved(2000, 250);
    elections.settle(2000);
    assert_eq!(elections.limit, 64, "250 ms is not slow");
    elections.saves.saved(3000, 1000);
    elections.settle(3000);
    assert_eq!(elections.limit, 16);
    // Queued before the limit fell: it says nothing about the new one,
    elections.saves.saved(3500, 900);
    elections.settle(3500);
    assert_eq!(elections.limit, 16);
    // unless it was slower than the save the limit fell for.
    elections.saves.saved(3600, 2000);
    elections.settle(3600);
    assert_eq!(elections.limit, 8);
    let member = Member::new(0);
    interval(&mut elections, std::slice::from_ref(&member), 3650);
    member.settled.store(true, Ordering::Relaxed);
    elections.settle(3700);
    assert_eq!(elections.limit, 9);
}
