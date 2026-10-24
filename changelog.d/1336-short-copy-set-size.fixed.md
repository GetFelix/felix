- A leader that crashed under `Periodic` or `None` no longer answers its
  successor's fence just because the routes now show a set of two (#1336). The
  set can shrink from three to two under the same leader, and a record the
  three acknowledged may be on neither copy. It answers only when the
  generation it led had those same two copies and its own last record is from
  that generation.
