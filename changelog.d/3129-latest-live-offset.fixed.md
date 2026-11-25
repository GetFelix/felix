- On Raft, a `latest` subscription could report a `live_offset` past its
  `start_offset` while the leader held entries it had not committed, and those
  records then arrived live. `live_offset` now stops at the committed tail
  (#3129).
