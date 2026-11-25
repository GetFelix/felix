- The Raft simulator no longer hands work to real threads (#3126). The
  log's index reservation, the `vouched-unsynced` marker, the root gate
  and the kept-copy scan of a group open went to tokio's blocking pool
  under the simulator, so a run depended on the machine's speed and a
  coverage build could fail a seed the plain build passed. A run that
  starts a blocking-pool thread now fails with a finding, and the finding
  for an opened term missing from the final log says where that log was
  read and which members still hold the term.
