- A forwarded idempotent batch whose records carry no key-and-headers block
  can now be sent between brokers, as the new internal kind
  `PlainSequencedForwardPublish` behind the `PLAIN_SEQUENCED_FORWARDS`
  capability, so it reaches a Raft shard's owner (#2570). Before, the
  sequenced kind could not be encoded without a block, and one with a block
  is refused on Raft. A batch with blocks travels exactly as before.
