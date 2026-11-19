- A reset or truncation that crashed partway no longer reopens with records
  and a generation history that disagree: old records with no history (which
  switched off the divergence check for them), a history over records the
  reset dropped, or a generation left past the new tail. Both now store a
  `rewind-pending` marker first, and an open that finds it finishes the rewind
  before the log is served (#2579, #2746).
