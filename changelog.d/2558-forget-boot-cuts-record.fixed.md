- A failed flush whose poison marker and run-record rewrite both failed, as on
  a full disk, no longer lets a process restart in the same boot keep the
  storage id. The run record is cut to nothing in place instead, which rotates
  the id on the next start (#2558).
