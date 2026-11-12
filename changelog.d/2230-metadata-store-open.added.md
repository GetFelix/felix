- The metadata store opens as one piece, for the control plane's move to the
  segment store (#2230, #2341). `MetadataStore::open` decides the start
  before it writes anything, takes an exclusive lock on `store/lock` (a second
  process, or the same store through a symlink, is refused), and opens the
  log as a Raft log fsynced on commit. It refuses a log purged past its
  snapshot and a saved committed id whose entry is of another term, empties a
  log that a snapshot fully covers, and withholds the member's vote when the
  log is blank beside a snapshot. A purge past the stored snapshot is hidden
  in memory instead of removed, so a restart never finds a log that starts
  after its snapshot, and an append that would leave such a log is an error.
  The control plane does not use the store yet, so nothing changes for a
  running cluster. The rules are in `docs/storage-format.md`.
