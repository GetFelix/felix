- A publish is answered once it is durable and in the replay ring, before it
  is fanned out, so subscribers no longer add to the publisher's round trip.
  The fanout keeps the batch's commit turn, so delivery order is unchanged. A
  `Quorum` publish is still answered after its quorum wait (#1771).
- Fanout counts queue depth once per publish rather than once per subscriber,
  a lone record goes out in the frame its subscribers share instead of being
  encoded per subscriber, and the subscriber lanes no longer look up a
  labelled metric on every delivery (#1771).
