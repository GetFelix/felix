use super::*;

#[test]
fn first_difference_names_the_first_event_that_differs() {
    let lines = |items: &[&str]| items.iter().map(|s| s.to_string()).collect::<Vec<_>>();
    let a = lines(&["fault x", "op 1", "op 2"]);
    assert_eq!(first_difference(&a, &a), None);
    let found = first_difference(&a, &lines(&["fault x", "op 1", "op 3"])).unwrap();
    assert!(
        found.contains("event 2") && found.contains("op 3"),
        "{found}"
    );
    let found = first_difference(&a, &lines(&["fault x"])).unwrap();
    assert!(
        found.contains("event 1") && found.contains("ends here"),
        "{found}"
    );
}

#[cfg(feature = "raft")]
#[test]
fn a_deadline_fires_only_when_it_is_not_dropped_in_time() {
    let (fired, seen) = std::sync::mpsc::channel();
    let kept = Deadline::start(Duration::from_millis(50), {
        let fired = fired.clone();
        move || fired.send("kept").unwrap()
    });
    let dropped = Deadline::start(Duration::from_millis(50), move || {
        fired.send("dropped").unwrap()
    });
    drop(dropped);
    assert_eq!(seen.recv_timeout(Duration::from_secs(10)), Ok("kept"));
    drop(kept);
    assert!(seen.recv_timeout(Duration::from_millis(200)).is_err());
}
