- Storage format v9 gains the conditional piece record (bits 31 and 28, an 8-byte tag) for a conditional
  batch of several records on a Raft log, and `log::conditional`, which reads such a log so a batch is seen
  whole or not at all and the read bound stops before an undecided batch (#2751). Nothing writes one yet.
