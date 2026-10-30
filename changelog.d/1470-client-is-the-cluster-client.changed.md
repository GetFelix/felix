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
  - `ClusterClient` becomes `Client`. `Client::connect(&seeds, server_name, config)`
    and `Client::connect_seeds` keep their arguments.
  - The old `Client` becomes `Connection`: `Connection::connect(addr, server_name,
    config)`. Most code that held one broker address should switch to `Client`
    with that address as the only seed instead; a single broker needs nothing else.
  - Rename in imports and signatures: `use felix_client::{Client, ClusterClient}`
    becomes `use felix_client::{Client, Connection}` where both are needed.
  - `ClusterSubscription`, `ClusterCacheWatch` and the other `Cluster*` types keep
    their names.
