- The control plane's side of a shard's switch to Raft: a seal order
  (`POST /v1/placement/seal/...`), the seal leader's report and give-up
  (`POST /v1/nodes/{node_id}/seal-report`, `seal-give-up`), recorded on the
  shard's assignment as `switch`. One report wins a seal's generation, a
  give-up never lands beside it, and a switched shard takes no hybrid
  write. Metadata level 33; a seal is ordered only once the fleet has
  finalized `shard_raft`, which no build reports yet (#2242).
