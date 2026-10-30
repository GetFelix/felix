- **Breaking: in the Rust `felix-client` crate, `Client` is now the cluster-aware
  client** (#1470). Applications should always use it: it sends each request to the
  broker that owns the shard and reconnects when a broker fails. A plain
  connection to a cluster forwards publishes for shards its broker does not lead,
  which cut in-memory throughput 2.4x and durable quorum-acked throughput 4.4x in
  the v0.6.0 three-node run. The old single-connection type is now `Connection`,
  kept public for tools that address one named broker. The Python, TypeScript, C
  and C# clients already exposed the cluster client under the name `Client`, so
  only Rust callers change.

  Migrating:
  - Rename in this order, or the second rename undoes the first: first `Client`
    to `Connection`, then `ClusterClient` to `Client`.
  - After both, `use felix_client::{Client, Connection}` brings in the
    cluster-aware client (was `ClusterClient`) and the single-connection type
    (was `Client`). `Client::connect(&seeds, server_name, config)` and
    `Client::connect_seeds` keep their arguments.
  - Code that held one broker address should move to `Client` with that address
    as the only seed; a single broker needs nothing else. Use
    `Connection::connect(addr, server_name, config)` only for tools that
    address one named broker.
  - `ClusterSubscription`, `ClusterCacheWatch` and the other `Cluster*` types
    keep their names.
