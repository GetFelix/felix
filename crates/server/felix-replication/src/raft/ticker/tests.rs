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
        max_elections_at_once: most,
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
    /// Its pre-vote round is answered and it is saving its own vote.
    saving: AtomicBool,
    urgent: bool,
    /// Real vote rounds it started.
    votes: AtomicU64,
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

    fn urgent(order: u64) -> Arc<Self> {
        Arc::new(Self {
            order,
            urgent: true,
            ..Self::default()
        })
    }

    /// It sends a round of two requests; `answered` of them come back.
    fn stand(&self, answered: u64) {
        self.asks.fetch_add(2, Ordering::Relaxed);
        self.out.fetch_add(2 - answered, Ordering::Relaxed);
    }

    /// Its own vote is saved and its real vote requests go out.
    fn vote(&self, answered: u64) {
        self.saving.store(false, Ordering::Relaxed);
        self.votes.fetch_add(1, Ordering::Relaxed);
        self.stand(answered);
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
    fn saving_its_vote(&self) -> bool {
        self.saving.load(Ordering::Relaxed)
    }
    fn urgent(&self) -> bool {
        self.urgent
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

/// Elections here have been losing lately, so slow saves count.
fn losing(elections: &mut Elections<Member>) {
    elections.outcomes.lost = 1_000;
}

/// A vote save of `took` ms ended at `now`, alone on the disk or not.
fn saved(elections: &Elections<Member>, now: u64, took: u64, alone: bool) {
    let ticket = elections.saves.began();
    let ticket = if alone { ticket } else { NOT_ALONE };
    elections
        .saves
        .ended(ticket, now, now * 1000, Some(took * 1000));
}

/// Before any save had the disk to itself, a save over a quarter of
/// election_min is slow: the limit falls to what the disk would have saved
/// in time, and a save that began under the old limit only lowers it
/// further if it was slower than the one it fell for.
#[test]
fn a_slow_vote_save_lowers_the_limit() {
    let mut elections = elections(64);
    losing(&mut elections);
    saved(&elections, 1000, 10, false);
    elections.settle(1000);
    assert_eq!(elections.limit, 64);
    saved(&elections, 2000, 250, false);
    elections.settle(2000);
    assert_eq!(elections.limit, 64, "250 ms is not slow");
    saved(&elections, 3000, 1000, false);
    elections.settle(3000);
    assert_eq!(elections.limit, 16);
    // Queued before the limit fell: it says nothing about the new one,
    saved(&elections, 3500, 900, false);
    elections.settle(3500);
    assert_eq!(elections.limit, 16);
    // unless it was slower than the save the limit fell for.
    saved(&elections, 3600, 1500, false);
    elections.settle(3600);
    assert_eq!(elections.limit, 10);
    elections.settle(3650);
    assert_eq!(elections.limit, 11, "one place back, losing");
}

/// The limit never falls below the floor. Once elections stop losing it
/// climbs back a place an interval, or as many as won.
#[test]
fn the_limit_recovers_fast_once_elections_win() {
    let mut elections = elections(64);
    losing(&mut elections);
    saved(&elections, 1000, 10_000, false);
    elections.settle(1000);
    assert_eq!(elections.limit, FLOOR);
    elections.outcomes = Outcomes::default();
    for n in 1..=10 {
        elections.settle(1000 + n * 50);
    }
    assert_eq!(elections.limit, FLOOR + 10);
    for n in 11..=60 {
        elections.settle(1000 + n * 50);
    }
    assert_eq!(elections.limit, 64);
}

/// While elections lose, a place comes back only every election_min, so a
/// limit at the floor probes rather than saws.
#[test]
fn a_losing_broker_regrows_a_place_every_election_min() {
    let mut elections = elections(64);
    losing(&mut elections);
    saved(&elections, 1000, 10_000, false);
    elections.settle(1000);
    assert_eq!(elections.limit, FLOOR);
    for n in 1..=40 {
        elections.settle(1000 + n * 50);
    }
    assert_eq!(elections.limit, FLOOR + 2);
}

/// A member that never stood in its time lost nothing: it says nothing
/// about the disk, so it does not make the broker count as losing.
#[test]
fn a_member_that_never_stands_is_not_a_lost_election() {
    let mut elections = elections(1);
    let [a, b] = [Member::new(0), Member::new(1)];
    interval(&mut elections, &[a.clone(), b.clone()], 0);
    interval(&mut elections, &[], 2000);
    assert!(!a.armed() && b.armed());
    assert!(!elections.outcomes.losing());
}

/// A bound under the floor is its own floor.
#[test]
fn a_bound_under_the_floor_is_not_lowered() {
    let mut small = elections(3);
    losing(&mut small);
    saved(&small, 1000, 10_000, false);
    small.settle(1000);
    assert_eq!(small.limit, 3);
}

/// A disk whose every save is slow, with nothing else on it, is not
/// overloaded by elections: saves near that pace keep the limit where it
/// is. Only a save well over it is slow.
#[test]
fn a_uniformly_slow_disk_does_not_pin_the_limit() {
    let mut elections = elections(64);
    losing(&mut elections);
    saved(&elections, 1000, 100, true);
    elections.settle(1000);
    for n in 1..=40 {
        let now = 1000 + n * 50;
        saved(&elections, now, 100 + n * 7, false);
        elections.settle(now);
        assert_eq!(elections.limit, 64, "{} ms", 100 + n * 7);
    }
    saved(&elections, 4000, 1000, false);
    elections.settle(4000);
    assert_eq!(elections.limit, 25, "ten times the baseline is slow");
}

/// A burst of slow saves on a fast disk lowers the limit, and it comes
/// back once the saves are fast again.
#[test]
fn a_burst_of_slow_saves_lowers_the_limit_and_it_comes_back() {
    let mut elections = elections(64);
    losing(&mut elections);
    saved(&elections, 1000, 5, true);
    elections.settle(1000);
    saved(&elections, 1050, 200, false);
    elections.settle(1050);
    assert_eq!(elections.limit, 16);
    saved(&elections, 1500, 300, false);
    elections.settle(1500);
    assert_eq!(elections.limit, FLOOR);
    elections.outcomes = Outcomes::default();
    for n in 1..=1500 {
        let now = 1500 + n * 50;
        saved(&elections, now, 6, false);
        elections.settle(now);
    }
    assert_eq!(elections.limit, 64);
}

/// A candidate whose pre-vote round is answered saves its own vote before
/// its real vote requests go out. With nothing out for a few ticks it has
/// not lost, and no other member here may start a vote round meanwhile.
#[test]
fn a_candidate_saving_its_vote_keeps_its_place() {
    let mut elections = elections(1);
    let [a, b] = [Member::new(0), Member::new(1)];
    interval(&mut elections, &[a.clone(), b.clone()], 0);
    a.stand(2);
    a.saving.store(true, Ordering::Relaxed);
    for now in (50..=500).step_by(50) {
        interval(&mut elections, &[], now);
        // `b` starts a vote round as soon as it is let stand.
        if b.armed() && b.votes.load(Ordering::Relaxed) == 0 {
            b.vote(0);
        }
    }
    a.vote(0);
    assert_eq!(
        b.votes.load(Ordering::Relaxed),
        0,
        "two vote rounds at once"
    );
    assert!(a.armed());
}

/// Members of groups that took writes go ahead of idle ones that came due
/// with them or before them.
#[test]
fn members_of_busy_groups_go_first() {
    let mut elections = elections(1);
    let [first, idle, other] = [Member::new(0), Member::new(1), Member::new(2)];
    let busy = Member::urgent(3);
    interval(&mut elections, &[first.clone(), idle.clone()], 0);
    interval(&mut elections, &[other.clone(), busy.clone()], 50);
    assert!(first.armed());
    first.settled.store(true, Ordering::Relaxed);
    interval(&mut elections, &[], 100);
    assert!(busy.armed() && !idle.armed() && !other.armed());
}

/// Saves two hundred times slower than the disk alone, while every
/// election let in wins its first round: a slow disk, not one the
/// elections overload. The limit stays at the bound and the queue drains
/// at its pace.
#[test]
fn slow_saves_while_elections_win_cut_nothing() {
    let mut elections = elections(64);
    saved(&elections, 0, 1, true);
    let members: Vec<_> = (0..200).map(Member::new).collect();
    interval(&mut elections, &members, 0);
    for n in 1..=4 {
        let now = n * 50;
        for member in members.iter().filter(|member| member.armed()) {
            member.stand(2);
            member.settled.store(true, Ordering::Relaxed);
        }
        saved(&elections, now, 200, false);
        interval(&mut elections, &[], now);
        assert_eq!(elections.limit, 64, "interval {n}");
    }
    assert_eq!(elections.load().waiting, 0);
}

/// Once elections win again, each one that won gives a place back, so a
/// limit cut during a storm comes back within a few intervals.
#[test]
fn the_limit_grows_by_the_elections_that_won() {
    let mut elections = elections(64);
    losing(&mut elections);
    saved(&elections, 0, 1, true);
    saved(&elections, 50, 1000, false);
    elections.settle(50);
    assert_eq!(elections.limit, FLOOR);
    elections.outcomes = Outcomes::default();
    let members: Vec<_> = (0..20).map(Member::new).collect();
    interval(&mut elections, &members, 100);
    let let_in = elections.limit;
    assert_eq!(let_in, FLOOR + 1, "one place back with nothing won");
    for member in members.iter().filter(|member| member.armed()) {
        member.settled.store(true, Ordering::Relaxed);
    }
    interval(&mut elections, &[], 150);
    assert_eq!(elections.limit, let_in * 2);
}

/// The disk's queue is the bound, not the places held: while the saves
/// queued here would take over a quarter of election_min to drain, only
/// one member is let in, and more as the queue empties.
#[test]
fn no_one_is_let_in_while_the_disk_is_backed_up() {
    let mut elections = elections(64);
    // 30 saves begin; 10 end 10 ms apart, so the disk does 100 a second.
    let tickets: Vec<_> = (0..30).map(|_| elections.saves.began()).collect();
    for (n, ticket) in tickets.iter().take(10).enumerate() {
        let at = 1_000_000 + n as u64 * 10_000;
        elections.saves.ended(*ticket, at / 1000, at, Some(10_000));
    }
    let members: Vec<_> = (0..100).map(Member::new).collect();
    interval(&mut elections, &members, 0);
    let first = elections.load().in_flight;
    assert!((1..=6).contains(&first), "{first} let in");
    for (n, ticket) in tickets.iter().skip(10).enumerate() {
        let at = 1_100_000 + n as u64 * 10_000;
        elections.saves.ended(*ticket, at / 1000, at, Some(10_000));
    }
    interval(&mut elections, &[], 50);
    let then = elections.load().in_flight;
    assert!(then > first && then < 64, "{then} let in");
}

/// A member whose own vote is being saved has not lost, even before
/// openraft calls it a candidate.
#[test]
fn a_vote_save_under_way_is_counted_for_its_member() {
    let own = Arc::new(AtomicU64::new(0));
    let clock = VoteClock {
        saves: Arc::default(),
        timers: Timers {
            started: Instant::now(),
            tick: 50,
            heartbeat: 200,
            idle_heartbeat: 2000,
            idle_after: 1000,
            election_min: 1000,
            election_max: 2000,
            lease: 1500,
            max_elections_at_once: 1,
        },
        own: Arc::clone(&own),
    };
    use felix_storage::raft_log::VoteTimer;
    let ticket = clock.began();
    assert_eq!(own.load(Ordering::Relaxed), 1);
    clock.ended(ticket, None);
    assert_eq!(own.load(Ordering::Relaxed), 0);
}
