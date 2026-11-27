- A Raft group whose relaxed followers lose power re-forms with every acknowledged record
  the disks still hold (#3309). Before a relaxed crash rotates the storage id, the start
  checks the root's marks against the run witness and the control plane's copy of its root
  sequence. When they hold, the marks are kept aside, and each copy that still meets them
  is kept as a power cut, so a re-form counts its vote instead of cutting to the saved
  commit. A directory older than its registration is recorded as a restore.
