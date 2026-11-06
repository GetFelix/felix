- The simulation's cache linearizability check no longer gives up undecided on a key with many
  unknown-outcome puts. Before the search it drops unknown puts whose value no get or delete
  reported, which cannot change the verdict; keys with a conditional put keep every operation.
