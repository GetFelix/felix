- The C ABI gains `felix_client_endpoints` (#1493), which lists the brokers a client would try, seeds included, so a
  caller can see that one seed was enough to discover the cluster. The C conformance harness now covers the
  connection-fault scenarios and the broker-loss ones, and CI's C verdict gates.
