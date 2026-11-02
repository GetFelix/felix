- A plain QUIC publish is answered once it is durable and in the replay ring,
  before it is fanned out, so the ack no longer waits for the subscriber
  loop. The fanout keeps the batch's commit turn, so delivery order is
  unchanged. `Quorum` streams, the `Block` queue policy, and idempotent,
  forwarded, conditional and Kafka publishes still answer after the fanout.
  On a CPU-bound host subscribers still slow the publisher: the CI fanout
  cell (100 subscribers, 4 vCPUs) is unchanged at about 1,190 acked
  publishes/s (#1771).
- Fanout counts queue depth once per publish rather than once per subscriber,
  a lone record goes out in the frame its subscribers share instead of being
  encoded per subscriber, and the subscriber lanes no longer look up a
  labelled metric on every delivery (#1771).
