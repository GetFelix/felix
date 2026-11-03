- A cluster test that catches shard starvation (#1760). A lease canary renews keys in a small
  `Quorum` cache while hundreds of replicated shards are created and appended to, and fails if
  a lease lapses, a renewal takes longer than the lease's slack, or a read takes over 2 s.
  Pull requests run it at
  600 streams; `scale-nightly.yml` runs it at 1500
  every night and opens an issue labelled `scale-nightly` on failure. See `docs/cluster-harness.md` ("Shard starvation").
