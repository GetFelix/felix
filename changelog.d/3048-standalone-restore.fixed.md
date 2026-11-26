- A standalone broker's data directory put back from a backup no longer hands out offsets
  it already handed out after the backup was taken (#3048). Run
  `felix-broker rotate-storage-id` on the restored directory with the broker stopped: each
  Raft group then re-forms from the restored records with its next offsets on the
  backup-point restore's time line, under every fsync mode. `rotate-storage-id` and
  `restore-point` leave a `restore-pending` file while they run, and a broker refuses to
  start while it is there. A copy put back without the tool, such as a rolled-back volume
  snapshot, is still not detected.
