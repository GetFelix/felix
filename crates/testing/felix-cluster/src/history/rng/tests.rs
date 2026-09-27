use std::time::Duration;

use super::Rng;

/// A printed seed is only useful if it replays the same schedule.
#[test]
fn the_same_seed_gives_the_same_sequence() {
    let mut a = Rng::new(42);
    let mut b = Rng::new(42);
    let first: Vec<u64> = (0..16).map(|_| a.next_u64()).collect();
    let second: Vec<u64> = (0..16).map(|_| b.next_u64()).collect();
    assert_eq!(first, second);
    assert_ne!(Rng::new(43).next_u64(), first[0]);
}

#[test]
fn draws_stay_in_range() {
    let mut rng = Rng::new(7);
    for _ in 0..1000 {
        assert!(rng.below(3) < 3);
        let d = rng.between(Duration::from_millis(10), Duration::from_millis(20));
        assert!((Duration::from_millis(10)..=Duration::from_millis(20)).contains(&d));
    }
}
