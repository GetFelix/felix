use super::*;

#[test]
fn only_a_segment_with_bytes_past_the_append_needs_a_repair() {
    assert!(needs_repair(Some(101), 100));
    assert!(!needs_repair(Some(100), 100), "nothing past the append");
    assert!(!needs_repair(Some(32), 100), "cut back further");
    assert!(!needs_repair(None, 100), "the name did not survive");
}
