use super::*;

#[test]
fn a_broker_clock_cannot_be_stepped_back() {
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
