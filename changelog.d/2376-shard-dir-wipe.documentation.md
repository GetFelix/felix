- Operations notes warn never to delete, empty or restore a single shard directory
  (#2376). A broker keeps its storage id and copy trust, so the empty copy can
  count in a promotion and lose an acknowledged write. Replace or wipe the whole
  data directory instead. Version 0.8 removes the hazard.
