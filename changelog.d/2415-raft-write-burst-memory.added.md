- The nightly `Raft idle groups` workflow has a write burst step that reports
  what Raft members keep resident after their leaders were busy: memory at the
  end of the burst and after a settle delay, as RSS (#2415).
