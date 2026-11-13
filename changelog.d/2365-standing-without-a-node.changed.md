- The control plane's Raft peer listener now starts before the member's Raft
  node (#2365). Until the node runs, the member answers `standing` with
  `raft: false` if its store holds no Raft state, and 503 on every other
  peer route. `standing` gains `raft`, `node_id` and `member`. A first start
  needs every control-plane member on a release that reports `member`.
