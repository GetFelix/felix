use super::resume_offset;

#[test]
fn offsets_stay_below_the_room_and_repeat_for_a_seed() {
    let (mut a, mut b) = (7u64, 7u64);
    for _ in 0..1000 {
        let x = resume_offset(&mut a, 50);
        assert!(x < 50);
        assert_eq!(x, resume_offset(&mut b, 50));
    }
}

#[test]
fn offsets_cover_the_range() {
    let mut state = 3u64;
    let seen: std::collections::HashSet<u64> =
        (0..2000).map(|_| resume_offset(&mut state, 10)).collect();
    assert_eq!(seen.len(), 10);
}

#[test]
fn no_room_resumes_at_zero() {
    let mut state = 1u64;
    assert_eq!(resume_offset(&mut state, 0), 0);
}
