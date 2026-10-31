- Storage applies a trim record's floor, the second piece of the conditional
  head trim (#1662). Once a trim is durable, and committed on a `Quorum` log,
  `read_range` below its floor returns `Trimmed` with the trim's headers,
  `DiskLog::readable_base` reports where reads start, and the sealed
  segments below the floor are deleted in the background. The floor is
  derived from the trim records and rebuilt at open, with a new `head` file
  as a cache. `read_range` now returns a `ReadPage` whose `next` says where
  to carry on, since a page can be empty below the tail when only trims
  remain. Nothing above storage writes a trim yet. See
  `docs/durable-storage.md`, "Conditional head trim".
