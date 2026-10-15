- Placement no longer promotes a follower whose storage changed since its
  leader's last report (#1266). The control plane keeps each broker's
  registered storage identity and the identity each report names for every
  follower, and counts a follower only while the two match. A follower that
  lost its disk comes back under a new identity and is not promoted, even on a
  fresh report. One that only restarted keeps its identity and still counts.
  A follower with no identity on either side (an older broker or control
  plane) is judged by the report's age alone, as before.
- A dead leader's last replica report no longer expires while placement or the
  control plane is slow, which left the shard unplaceable until an operator
  abandoned it (#1250). While the assignment is still at the report's
  generation and the leader is down, the report's age is measured to the
  leader's last heartbeat, for followers whose identity matches. A live
  leader's report still expires. Registration answers 400 for a malformed
  `storage_id`. Postgres migration `0030_storage_identity`; under Raft,
  metadata level 12.
