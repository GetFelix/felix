use super::*;

#[test]
fn the_check_refuses_a_backward_broker_step() {
    let broker = Endpoint::node("node-0");
    assert!(check_clock_fault(&broker, &ClockFault::back(Duration::from_secs(1))).is_err());
    assert!(check_clock_fault(&broker, &ClockFault::forward(Duration::from_secs(1))).is_ok());
    assert!(check_clock_fault(&broker, &ClockFault::Rate(0.5)).is_ok());
}

#[test]
fn the_control_plane_clock_steps_either_way() {
    let control_plane = Endpoint::ControlPlane;
    for step in [
        ClockFault::back(Duration::from_secs(60)),
        ClockFault::forward(Duration::from_secs(60)),
    ] {
        assert!(check_clock_fault(&control_plane, &step).is_ok());
    }
}

#[test]
fn a_rate_that_is_no_rate_is_refused() {
    for rate in [-1.0, f64::NAN, f64::INFINITY] {
        assert!(check_clock_fault(&Endpoint::ControlPlane, &ClockFault::Rate(rate)).is_err());
    }
}

/// A broker that read a fault file mid-rewrite would see it empty, which for
/// the partition file means no partition. A handle opened before the rewrite
/// stands in for that reader: it must still see all of the old contents.
#[test]
fn a_fault_file_is_replaced_and_never_rewritten_in_place() {
    use std::io::Read;

    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("peer-partition");
    replace_file(&path, "broker-1\nbroker-2").expect("first write");
    let mut reader = std::fs::File::open(&path).expect("open the file a broker is reading");

    replace_file(&path, "broker-1").expect("heal one peer");

    let mut seen = String::new();
    reader.read_to_string(&mut seen).expect("read");
    assert_eq!(
        seen, "broker-1\nbroker-2",
        "the rewrite went through the file a reader had open"
    );
    assert_eq!(std::fs::read_to_string(&path).expect("reread"), "broker-1");
    let left: Vec<_> = std::fs::read_dir(dir.path())
        .expect("list")
        .map(|entry| entry.expect("entry").file_name())
        .collect();
    assert_eq!(left, ["peer-partition"], "the staged file is gone");
}
