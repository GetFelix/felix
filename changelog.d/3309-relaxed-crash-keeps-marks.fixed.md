- A relaxed crash keeps the Raft marks it can vouch for, so a re-form counts the votes of
  followers that lost power (#3309). Before a relaxed crash rotates the storage id, the start
  checks the root's marks against the run witness and the control plane's copy of its root
  sequence. When they hold, the marks are kept aside, and each copy that still meets them
  is kept as a power cut instead of a restore. A directory older than its registration is
  recorded as a restore. A leader that restarts alone and votes in a term with no entry
  still makes the re-form cut to the saved commit (#3351).
