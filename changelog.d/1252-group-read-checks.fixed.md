- `group_describe` and the dead-letter list now check that the broker still
  leads the shard, as state reads do (#1252). A deposed leader could answer
  them from stale state. They are refused with `shard_unavailable` on a lapsed
  lease, or `leadership_lost` when a round finds a newer leader.
