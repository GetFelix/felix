- A shard that cannot open because the broker is out of file descriptors fails with an error that names the open-file limit (#2657).
  It stays unserved without being given up, `inspect` shows the error, writes to it are refused at once instead of waiting out the hold, and it opens on a later assignment pass once descriptors free up, while the other shards keep serving.
  A shard held for 60 seconds is reported to the control plane as failed to open (once), so it can be moved; the retries continue.
  This holds for every step of the open, including the ballot, the generation history and the generation-start record, and for Raft members.
  A Raft member no longer holds its group when it runs out of descriptors reading its kept copies.
  New metric `felix_broker_shard_open_file_limit_total`.
  The felix-replication and broker-service unit test binaries, felix-sim and the felix-cluster harness raise their own soft open-file limit, so their suites pass under the common default of 1,024 without `ulimit -n`.
