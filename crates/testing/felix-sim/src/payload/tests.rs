use super::*;

#[test]
fn payloads_round_trip_and_foreign_ones_are_marked() {
    assert_eq!(decode(&42u64.to_le_bytes()), 42);
    assert_eq!(decode(b"short"), FOREIGN_PAYLOAD);
}

#[test]
fn padded_payloads_span_sectors_and_damage_is_marked() {
    for value in 0..64 {
        let padded = payload(value, true);
        assert!((1024..=4096).contains(&padded.len()), "{}", padded.len());
        assert_eq!(decode(&padded), value);
        let mut damaged = padded.to_vec();
        damaged[600] ^= 0xff;
        assert_eq!(decode(&damaged), FOREIGN_PAYLOAD);
        assert_eq!(decode(&payload(value, false)), value);
    }
}
