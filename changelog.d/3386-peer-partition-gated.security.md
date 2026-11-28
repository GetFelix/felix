- A release broker no longer reads `FELIX_PEER_PARTITION_FILE`, so an environment variable
  can no longer cut it off from its peers (#3386). Peer-partition injection is compiled
  only into debug builds and builds with felix-broker-service's new `fault-injection`
  feature, as the storage and clock faults already were. The cluster harness builds its
  broker with the feature, refuses a partition against a broker that lacks it, and CI
  checks that a broker built without the feature carries none of it.
