- A replicated `Quorum` leader that heard from no majority for a lease
  duration, while its lease was lapsed, refused every reader of the shard for
  good, even after its followers answered again and it went on acknowledging
  writes (#1964). That ending now lasts only until a round at the same
  generation is confirmed. A follower naming a newer
  leader still ends the shard's readers for good.
