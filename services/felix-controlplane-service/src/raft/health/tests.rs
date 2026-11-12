use super::shown_term;

/// A converted store's terms start at 2^62, which neither a float gauge nor
/// a signed status field holds exactly.
#[test]
fn a_term_past_the_conversion_base_is_shown_without_it() {
    assert_eq!(shown_term(0), 0);
    assert_eq!(shown_term(7), 7);
    assert_eq!(shown_term((1 << 62) - 1), (1 << 62) - 1);
    assert_eq!(shown_term(1 << 62), 0);
    assert_eq!(shown_term((1 << 62) + 7), 7);
}
