- A kept shard copy that reclaim holds is now logged with two reservations,
  its own `raft-reserved` and the current copy's, and a damaged file shows as
  `unreadable` (#2744). Before, a copy held for its reservation was not logged
  at all, so the recovery step for a damaged kept `raft-reserved` could not be
  checked. That step in `docs/storage-format.md` now says to delete the damaged
  file only when the current value is at or above the kept one, and to start
  the copy over otherwise.
