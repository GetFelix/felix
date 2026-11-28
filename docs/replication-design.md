# Shard replication and leader failover (retired)

This page described the hybrid engine: a leader lease issued by the control
plane, log shipping from the leader to its followers, quorum marks, replica
reports, promotion fences and the replica election. That engine was deleted in
0.8, along with the tests this page cited. Its last copy, with every rule and
its reason, is the
[v0.7.0 page](https://github.com/GetFelix/felix/blob/v0.7.0/docs/replication-design.md).
A link from another page to a section of this one means that section of the
v0.7.0 page.

Every durable shard is now a Raft group:

- [`per-shard-raft.md`](per-shard-raft.md) is the engine's design.
- [`replication-engine.md`](replication-engine.md) records why the engine
  changed and the gate it had to pass.
- [`hybrid-retirement.md`](hybrid-retirement.md) lists what was deleted and
  which Raft check stands where each hybrid check stood.
