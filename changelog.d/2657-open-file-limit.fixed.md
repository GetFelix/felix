- A shard that cannot open because the broker is out of file descriptors fails with an error that names the open-file limit (#2657).
  It stays unserved without being given up and opens on a later assignment pass once descriptors free up, while the other shards keep serving.
  A Raft member no longer holds its group when it runs out of descriptors reading its kept copies.
  New metric `felix_broker_shard_open_file_limit_total`.
  Test suites raise their own soft open-file limit, so `cargo test` passes under the common default of 1,024 without `ulimit -n`.
