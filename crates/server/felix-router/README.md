# felix-router

Routing for multi-node [Felix](https://github.com/GetFelix/felix) deployments:
which broker serves a shard, from the assignments the control plane publishes,
and whether traffic may cross from one region to another.

The placement decisions themselves are made by the control plane; this crate
only reads them. Region awareness is an allowlist of permitted pairs; explicit
cross-region bridges are not built.

Not published; it is built into the broker service. AGPL-3.0-only. See
[LICENSING.md](https://github.com/GetFelix/felix/blob/main/LICENSING.md).
