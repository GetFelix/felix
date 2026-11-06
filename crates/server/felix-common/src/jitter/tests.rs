use super::*;

#[test]
fn a_draw_stays_within_its_bound() {
    for max in [0, 1, 7, 1000] {
        for _ in 0..100 {
            assert!(up_to(max) <= max);
        }
    }
    up_to(u64::MAX);
}

#[test]
fn a_seeded_thread_draws_the_same_sequence_every_time() {
    let draws = |seed| {
        std::thread::spawn(move || {
            sim::seed(seed);
            (0..16).map(|_| up_to(1_000_000)).collect::<Vec<_>>()
        })
        .join()
        .unwrap()
    };
    assert_eq!(draws(7), draws(7));
    assert_ne!(draws(7), draws(8));
}
