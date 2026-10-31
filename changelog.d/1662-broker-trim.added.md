- The broker writes a conditional head trim in process, the third piece of
  #1662. `Broker::trim_before_at` appends the trim record only at the tail
  the caller expects, refuses a floor past the record, a stale no-op and a
  stream holding atomic commits, and drops the replay ring's entries below
  the floor in the record's turn. Every reader now starts at the readable
  base, including group cursors and the Kafka listener's fetch and earliest
  offset, and `CursorTooOld` carries the setting trim's headers.
  `StreamLog::base_offset` is now `physical_base`, beside `readable_base`.
  A trim is answered only once the ring has it, and a no-op on a `Quorum`
  stream waits for the committed mark, and a trim's waits end in
  `TrimTimedOut`. Nothing on the wire writes a trim yet.
