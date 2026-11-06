- A replica standing for election could take its ballot on a replica set it
  no longer kept (#1970). It checked the set once, before its pre-votes, and a
  fence or batch from another leader could give its copy a newer set with a
  change pending while the round ran. The candidate now writes its ballot
  only if the copy still keeps the set it checked, and a copy refuses a set
  from a leader it has promised past. Both checks take the same lock, so a
  set and a candidate's ballot never interleave.
