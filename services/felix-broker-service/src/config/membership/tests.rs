use super::is_host_port;

#[test]
fn a_client_advertise_address_is_a_host_and_a_port() {
    for good in [
        "10.0.0.5:5000",
        "broker-1.felix:5000",
        "[::1]:5000",
        "localhost:1",
    ] {
        assert!(is_host_port(good), "{good}");
    }
    for bad in [
        "broker-1.felix",
        ":5000",
        "broker:port",
        "broker:70000",
        "::1:5000",
    ] {
        assert!(!is_host_port(bad), "{bad}");
    }
}
