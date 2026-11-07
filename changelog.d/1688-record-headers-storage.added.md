- Storage format v8: a record may carry a key and headers (#1688). Bit 26 of
  the record's length word marks a length-prefixed block at the front of the
  body, which storage keeps as given. A log moves onto a v8 segment only for
  the first record with one, and from then on older builds refuse it. Records without a key or headers are byte for byte
  what v7 wrote, pinned by golden vectors.
