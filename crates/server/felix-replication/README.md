# felix-replication

How [Felix](https://github.com/GetFelix/felix) brokers talk to each other: the
broker-internal transport (`peer`), and the log replication that runs over it.
A leader ships committed records to each shard's followers, a follower stores
them, and a `Quorum` write waits until a majority holds it.

The broker service wires it in. What it needs from the service -- whether a
shard is served here and at which generation, the write fence a planned move
drains behind, and the token a replica report carries -- comes in through
three small traits, so this crate does not depend on the service.

Not published; it is built into the broker service. AGPL-3.0-only. See
[LICENSING.md](https://github.com/GetFelix/felix/blob/main/LICENSING.md).
