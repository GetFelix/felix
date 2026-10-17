- On Linux, under `periodic` or `none` fsync, a broker whose process crashed (`SIGKILL`,
  an OOM kill, a panic) keeps its storage id, so its followers' copies and
  its own leadership survive the restart without a re-copy (#1367). Only a
  machine crash rotates it: `run-state` now records the machine's boot id
  (`/proc/sys/kernel/random/boot_id`), and a start in a different boot, or where either boot is unknown,
  takes a new id as before. Keeping the id first calls `syncfs` on every
  filesystem under the data directory and rotates if that fails. macOS and
  other platforms still rotate after every relaxed crash. A failed fsync forgets the boot, so a crash after one still
  rotates. A VM or container memory-snapshot restore keeps the boot id; use
  `on_commit` or delete `storage-id` for those workflows. A clean stop is written in the layout older releases
  read, so a downgrade after one keeps the id.
