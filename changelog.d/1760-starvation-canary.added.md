- A cluster test that catches shard starvation (#1760). A lease canary renews keys in a small
  `Quorum` cache while hundreds of replicated shards are created and appended to, and fails if
  a lease lapses or its p99 drifts far from its idle baseline. Pull requests run it at 300
  streams; `scale-nightly.yml` runs it at 1500 every night and opens an issue labelled
  `scale-nightly` on failure. See `docs/cluster-harness.md` ("Shard starvation").
