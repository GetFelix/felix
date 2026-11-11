- New fleet feature `record_generations` (#2167), groundwork for per-shard
  Raft: it moves durable stream logs onto segment format v9, with every record
  stamped with the generation of the leader that wrote it, and lets a shard
  save a committed vote. Brokers report it and an operator finalizes it once
  `generation_start` is; a new cluster does not finalize it. It is one-way: an
  older broker cannot open a v9 segment. A single broker stays on the older
  format.
