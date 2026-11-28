- On a Raft follower that skipped ahead while nobody was subscribed, the first event after a
  leader change now counts every offset below it that holds no event in `skipped_before`
  (#3412). A pass that found only the new term's blank used up the count, so the next event
  counted from the blank alone and a reader saw a gap nobody announced.
