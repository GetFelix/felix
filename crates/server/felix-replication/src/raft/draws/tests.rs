use openraft::rt::AsyncRuntime;
use rand::Rng;

use super::{ShardRuntime, seed_timers};

fn draws() -> Vec<u64> {
    (0..4)
        .map(|_| ShardRuntime::thread_rng().next_u64())
        .collect()
}

#[test]
fn a_seed_repeats_its_draws() {
    let first = {
        let _seed = seed_timers(7);
        draws()
    };
    let second = {
        let _seed = seed_timers(7);
        draws()
    };
    assert_eq!(first, second);
    let other = {
        let _seed = seed_timers(8);
        draws()
    };
    assert_ne!(first, other);
}

#[test]
fn dropping_a_seed_resumes_the_stream_it_replaced() {
    let straight = {
        let _seed = seed_timers(1);
        [draws(), draws()]
    };
    let _outer = seed_timers(1);
    let first = draws();
    {
        let _inner = seed_timers(2);
        draws();
    }
    assert_eq!([first, draws()], straight);
}

#[test]
fn without_a_seed_draws_are_not_repeated() {
    assert_ne!(draws(), draws());
}
