use super::{checked_payload_count, min_record_len};
use crate::error::Error;

#[test]
fn a_record_takes_at_least_its_length_prefixes_and_time() {
    assert_eq!(min_record_len(false, false), 4);
    assert_eq!(min_record_len(true, false), 8);
    assert_eq!(min_record_len(false, true), 12);
    assert_eq!(min_record_len(true, true), 16);
}

/// A count is bounded by what the records need at the least, so a frame with
/// block lengths and times cannot make the decoder reserve for more records
/// than it has bytes to hold.
#[test]
fn the_count_bound_follows_the_size_of_a_record() {
    let remaining = 160;
    for (headers, timestamps) in [(false, false), (true, false), (false, true), (true, true)] {
        let min = min_record_len(headers, timestamps);
        let fits = remaining / min;
        assert_eq!(
            checked_payload_count(fits, remaining, min).expect("fits"),
            fits
        );
        assert!(
            matches!(
                checked_payload_count(fits + 1, remaining, min),
                Err(Error::Incomplete)
            ),
            "headers {headers} timestamps {timestamps}"
        );
    }
    // What the plain bound would have let through.
    assert!(checked_payload_count(remaining / 4, remaining, min_record_len(true, true)).is_err());
}
