- Raft: a re-form from a backup point is blocked until its time line plus gap is far enough
  above the shard's floor and every earlier epoch's first index to clear what that epoch can
  have given out: a full 2^32 with the default gap, about four and a half minutes. Before,
  a restore planned soon after an epoch whose control-plane clock ran ahead, or one planned
  with a smaller gap than an earlier epoch's, could start below offsets that epoch had
  acknowledged and give them out again. An `offset_gap` above 2^40 is now refused, since
  the bound assumes no epoch starts further above the line. The control-plane clocks must
  agree within about four minutes. (#3291)
