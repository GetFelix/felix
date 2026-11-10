- New fleet feature `record_generations` (#2167). Once an operator finalizes
  it, each durable stream log starts its next segment in format v9, with every
  record stamped with a generation, and a shard may save a committed vote.
  Both are formats the previous release refuses, so rollback after the
  finalize is refused; nothing changes on disk before it. A new cluster does
  not finalize it on its own, and a single broker stays on the older format.
  A data directory with a v9 segment joins only a fleet that has finalized it.
