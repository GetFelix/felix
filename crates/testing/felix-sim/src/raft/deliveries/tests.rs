use super::*;

fn delivered(member: usize, offset: u64, value: u64) -> Delivery {
    Delivery {
        at: 0,
        member,
        list: "list-0".to_string(),
        offset,
        value,
    }
}

/// A log whose offset 2 is a blank entry: no element, and the next one
/// skips it.
fn log_with_a_blank_at_2() -> BTreeMap<String, Vec<Element>> {
    let element = |offset, value, skipped_before| Element {
        offset,
        value,
        skipped_before,
    };
    let log = vec![element(0, 10, 0), element(1, 11, 0), element(3, 13, 1)];
    BTreeMap::from([("list-0".to_string(), log)])
}

#[test]
fn deliveries_of_what_the_log_holds_pass() {
    let deliveries = [
        delivered(0, 0, 10),
        delivered(1, 0, 10),
        delivered(1, 1, 11),
        delivered(2, 3, 13),
    ];
    assert_eq!(
        judge(&deliveries, &log_with_a_blank_at_2()),
        Vec::<String>::new()
    );
}

#[test]
fn a_delivery_at_a_blank_entrys_offset_is_a_finding() {
    let findings = judge(&[delivered(1, 2, 99)], &log_with_a_blank_at_2());
    assert_eq!(findings.len(), 1, "{findings:?}");
    assert!(
        findings[0].contains("value 99 at offset 2, where the log holds no record"),
        "{findings:?}"
    );
}

#[test]
fn a_delivery_the_log_holds_something_else_at_is_a_finding() {
    let findings = judge(&[delivered(0, 1, 12)], &log_with_a_blank_at_2());
    assert_eq!(findings.len(), 1, "{findings:?}");
    assert!(
        findings[0].contains("where the log holds value 11"),
        "{findings:?}"
    );
}

#[test]
fn two_values_delivered_at_one_offset_past_the_final_read_is_a_finding() {
    // Offset 7 is past the final read, so the log does not judge it; the
    // two subscribers disagreeing does.
    let deliveries = [delivered(0, 7, 70), delivered(2, 7, 71)];
    let findings = judge(&deliveries, &log_with_a_blank_at_2());
    assert_eq!(findings.len(), 1, "{findings:?}");
    assert!(
        findings[0].contains("offset 7 was delivered as value 70 by b1 and as value 71 by b3"),
        "{findings:?}"
    );
}

#[test]
fn two_values_at_one_offset_of_a_list_with_no_final_read_is_a_finding() {
    let deliveries = [delivered(0, 0, 1), delivered(1, 0, 2)];
    let findings = judge(&deliveries, &BTreeMap::new());
    assert_eq!(findings.len(), 1, "{findings:?}");
}

#[test]
fn the_same_offset_in_two_lists_is_not_reuse() {
    let mut other = delivered(1, 0, 2);
    other.list = "list-1".to_string();
    assert_eq!(
        judge(&[delivered(0, 0, 1), other], &BTreeMap::new()),
        Vec::<String>::new()
    );
}
