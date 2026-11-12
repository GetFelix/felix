use super::*;

#[test]
fn a_waiver_reads_back_and_is_removed_once() {
    let dir = tempfile::tempdir().expect("tempdir");
    assert_eq!(load(dir.path(), "prod").expect("load"), None);
    let slots = BTreeSet::from([2, 3]);
    let said = form_without(dir.path(), "prod", &slots).expect("write");
    assert!(said.contains("2, 3") && said.contains("prod"), "{said}");
    assert_eq!(load(dir.path(), "prod").expect("load"), Some(slots));
    assert!(remove(dir.path()).expect("remove"));
    assert!(!remove(dir.path()).expect("remove again"));
    assert_eq!(load(dir.path(), "prod").expect("load"), None);
}

#[test]
fn a_waiver_for_another_cluster_is_refused() {
    let dir = tempfile::tempdir().expect("tempdir");
    form_without(dir.path(), "staging", &BTreeSet::from([2])).expect("write");
    let err = load(dir.path(), "prod").expect_err("another cluster's waiver");
    assert!(format!("{err:#}").contains("staging"), "{err:#}");

    std::fs::write(dir.path().join(FILE), b"slot 2").expect("write");
    load(dir.path(), "prod").expect_err("not a waiver");
}

#[test]
fn a_waiver_names_a_cluster_and_a_slot() {
    let dir = tempfile::tempdir().expect("tempdir");
    form_without(dir.path(), "", &BTreeSet::from([2])).expect_err("no cluster id");
    form_without(dir.path(), "prod", &BTreeSet::new()).expect_err("no slot");
    assert_eq!(load(dir.path(), "prod").expect("load"), None);
}

/// A data directory another cluster's member has run in is not this
/// cluster's first start.
#[test]
fn a_store_of_another_cluster_takes_no_waiver() {
    let dir = tempfile::tempdir().expect("tempdir");
    {
        let db = store::open(&dir.path().join(STORE_FILE)).expect("open store");
        store::claim_cluster_id(&db, "staging").expect("claim");
    }
    let err = form_without(dir.path(), "prod", &BTreeSet::from([2])).expect_err("wrong cluster");
    assert!(format!("{err:#}").contains("staging"), "{err:#}");
    form_without(dir.path(), "staging", &BTreeSet::from([2])).expect("same cluster");
}
