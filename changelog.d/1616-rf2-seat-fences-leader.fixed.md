- An RF 2 replacement on a `Quorum` stream no longer seats its newcomer while a
  record may be on the leader alone (#1616). Up to the seat, a record could be
  acknowledged on the leader and the leaving copy after the report placement
  read, and the new set held it only on the leader. Placement now fences the
  leader before any seat that leaves an even set and seats the newcomer once
  the drained leader's report shows it holding the whole log, with the same
  short pause in writes as a leader move. Restores that replace a lost copy in
  an RF 2 set do the same.
