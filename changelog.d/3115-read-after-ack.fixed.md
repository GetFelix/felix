- A `stream_read` on Raft right after an acknowledged publish could come back
  empty: the page stopped at the reading broker's own synced offset, which can
  trail a commit a majority already synced. It now reads to the commit (#3115).
