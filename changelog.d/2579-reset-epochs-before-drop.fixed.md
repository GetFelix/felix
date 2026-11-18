- A non-Raft reset that crashed after it dropped the old records no longer
  reopens with the old generation history, which claimed offsets below the
  new base the copy never held. The reset now empties the history before the
  drop (#2579).
