- Under `joint_fence`, a `Quorum` cache's counter updates now wait for a majority of both the old and the new
  replica set while a set change is pending, and the leader reports the change's start only once the counter
  log's start record is on both majorities too. Before, a counter update acknowledged before the cut-over could
  be held by only the leader of the new set once the control plane took it (#1830).
