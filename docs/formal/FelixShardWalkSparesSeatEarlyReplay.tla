---- MODULE FelixShardWalkSparesSeatEarlyReplay ----
\* Main's FelixShardWalkSparesSeatEarly.cfg counterexample, replayed as a fixed
\* trace: Init and Next constrained to its states, one step at a time. The
\* walk finds the violation by chance; this checks the behaviour still exists
\* and still breaks AckedHeldByLeader. If a spec change makes a step
\* impossible the replay stops early and the expected violation is missing,
\* so the check fails and the trace needs regenerating.
EXTENDS FelixShard
CONSTANTS a, b, c, d
VARIABLE step

ReplayInit ==
    /\ Init /\ step = 0
    /\ ackGen = {}
    /\ waits = {}
    /\ acked = {}
    /\ answered = (a :> {} @@ b :> {} @@ c :> {} @@ d :> {})
    /\ ballot = (a :> {a} @@ b :> {} @@ c :> {} @@ d :> {})
    /\ bexpiry = (a :> 2 @@ b :> 0 @@ c :> 0 @@ d :> 0)
    /\ bgen = (a :> 1 @@ b :> 0 @@ c :> 0 @@ d :> 0)
    /\ cacked = {}
    /\ canswered = (a :> {} @@ b :> {} @@ c :> {} @@ d :> {})
    /\ cconfirmed = (a :> (a :> 0 @@ b :> 0 @@ c :> 0 @@ d :> 0) @@ b :> (a :> 0 @@ b :> 0 @@ c :> 0 @@ d :> 0) @@ c :> (a :> 0 @@ b :> 0 @@ c :> 0 @@ d :> 0) @@ d :> (a :> 0 @@ b :> 0 @@ c :> 0 @@ d :> 0))
    /\ checked = (a :> {} @@ b :> {} @@ c :> {} @@ d :> {})
    /\ chwm = (a :> 0 @@ b :> 0 @@ c :> 0 @@ d :> 0)
    /\ clock = (a :> 0 @@ b :> 0 @@ c :> 0 @@ d :> 0)
    /\ clog = (a :> <<>> @@ b :> <<>> @@ c :> <<>> @@ d :> <<>>)
    /\ confirmed = (a :> (a :> 0 @@ b :> 0 @@ c :> 0 @@ d :> 0) @@ b :> (a :> 0 @@ b :> 0 @@ c :> 0 @@ d :> 0) @@ c :> (a :> 0 @@ b :> 0 @@ c :> 0 @@ d :> 0) @@ d :> (a :> 0 @@ b :> 0 @@ c :> 0 @@ d :> 0))
    /\ cpExpiry = 2
    /\ cpLearner = {}
    /\ cpNext = {}
    /\ cpNextGen = 0
    /\ cpView = << >>
    /\ cutOver = <<>>
    /\ draining = FALSE
    /\ expect = (a :> (a :> 0 @@ b :> 0 @@ c :> 0 @@ d :> 0) @@ b :> (a :> 0 @@ b :> 0 @@ c :> 0 @@ d :> 0) @@ c :> (a :> 0 @@ b :> 0 @@ c :> 0 @@ d :> 0) @@ d :> (a :> 0 @@ b :> 0 @@ c :> 0 @@ d :> 0))
    /\ fenceIds = (a :> {} @@ b :> {} @@ c :> {} @@ d :> {})
    /\ fencing = (a :> FALSE @@ b :> FALSE @@ c :> FALSE @@ d :> FALSE)
    /\ gaveUp = (a :> 0 @@ b :> 0 @@ c :> 0 @@ d :> 0)
    /\ gen = 1
    /\ halted = {}
    /\ hbAt = (a :> 0 @@ b :> 0 @@ c :> 0 @@ d :> 0)
    /\ hbOut = (a :> FALSE @@ b :> FALSE @@ c :> FALSE @@ d :> FALSE)
    /\ heard = (a :> [ gen |-> 0, holders |-> {}, len |-> 0, drained |-> FALSE, sids |-> (a :> 0 @@ b :> 0 @@ c :> 0 @@ d :> 0), incs |-> (a :> 1 @@ b :> 1 @@ c :> 1 @@ d :> 1), inh |-> (a :> 99 @@ b :> 99 @@ c :> 99 @@ d :> 99), names |-> {} ] @@ b :> [ gen |-> 0, holders |-> {}, len |-> 0, drained |-> FALSE, sids |-> (a :> 0 @@ b :> 0 @@ c :> 0 @@ d :> 0), incs |-> (a :> 1 @@ b :> 1 @@ c :> 1 @@ d :> 1), inh |-> (a :> 99 @@ b :> 99 @@ c :> 99 @@ d :> 99), names |-> {} ] @@ c :> [ gen |-> 0, holders |-> {}, len |-> 0, drained |-> FALSE, sids |-> (a :> 0 @@ b :> 0 @@ c :> 0 @@ d :> 0), incs |-> (a :> 1 @@ b :> 1 @@ c :> 1 @@ d :> 1), inh |-> (a :> 99 @@ b :> 99 @@ c :> 99 @@ d :> 99), names |-> {} ] @@ d :> [ gen |-> 0, holders |-> {}, len |-> 0, drained |-> FALSE, sids |-> (a :> 0 @@ b :> 0 @@ c :> 0 @@ d :> 0), incs |-> (a :> 1 @@ b :> 1 @@ c :> 1 @@ d :> 1), inh |-> (a :> 99 @@ b :> 99 @@ c :> 99 @@ d :> 99), names |-> {} ])
    /\ hwm = (a :> 0 @@ b :> 0 @@ c :> 0 @@ d :> 0)
    /\ inc = (a :> 0 @@ b :> 0 @@ c :> 0 @@ d :> 0)
    /\ inflight = <<>>
    /\ inherited = (a :> {} @@ b :> {} @@ c :> {} @@ d :> {})
    /\ installed = {}
    /\ joinedAt = 0
    /\ joining = {}
    /\ leader = a
    /\ learner = (a :> FALSE @@ b :> FALSE @@ c :> FALSE @@ d :> FALSE)
    /\ leaving = {}
    /\ leftDirty = (a :> 0 @@ b :> 0 @@ c :> 0 @@ d :> 0)
    /\ log = (a :> <<>> @@ b :> <<>> @@ c :> <<>> @@ d :> <<>>)
    /\ marked = (a :> FALSE @@ b :> FALSE @@ c :> FALSE @@ d :> FALSE)
    /\ mine = (a :> {a, b} @@ b :> {a, b} @@ c :> {a, b} @@ d :> {a, b})
    /\ moves = 0
    /\ named = {}
    /\ now = 0
    /\ oldSet = (a :> {} @@ b :> {} @@ c :> {} @@ d :> {})
    /\ opened = {<<1, a>>}
    /\ out = {c, d}
    /\ pending = (a :> 0 @@ b :> 0 @@ c :> 0 @@ d :> 0)
    /\ promised = (a :> 1 @@ b :> 0 @@ c :> 0 @@ d :> 0)
    /\ queued = (a :> 0 @@ b :> 0 @@ c :> 0 @@ d :> 0)
    /\ readmit = {}
    /\ registered = (a :> 1 @@ b :> 1 @@ c :> 1 @@ d :> 1)
    /\ report = [ gen |-> 0, holders |-> {}, len |-> 0, drained |-> FALSE, sids |-> (a :> 0 @@ b :> 0 @@ c :> 0 @@ d :> 0), incs |-> (a :> 1 @@ b :> 1 @@ c :> 1 @@ d :> 1), inh |-> (a :> 99 @@ b :> 99 @@ c :> 99 @@ d :> 99), names |-> {} ]
    /\ restarting = {}
    /\ seen = (a :> (a :> 0 @@ b :> 0 @@ c :> 0 @@ d :> 0) @@ b :> (a :> 0 @@ b :> 0 @@ c :> 0 @@ d :> 0) @@ c :> (a :> 0 @@ b :> 0 @@ c :> 0 @@ d :> 0) @@ d :> (a :> 0 @@ b :> 0 @@ c :> 0 @@ d :> 0))
    /\ seenInc = (a :> (a :> 1 @@ b :> 1 @@ c :> 1 @@ d :> 1) @@ b :> (a :> 1 @@ b :> 1 @@ c :> 1 @@ d :> 1) @@ c :> (a :> 1 @@ b :> 1 @@ c :> 1 @@ d :> 1) @@ d :> (a :> 1 @@ b :> 1 @@ c :> 1 @@ d :> 1))
    /\ setGen = (a :> 0 @@ b :> 0 @@ c :> 0 @@ d :> 0)
    /\ sid = (a :> 0 @@ b :> 0 @@ c :> 0 @@ d :> 0)
    /\ splits = 0
    /\ staged = {}
    /\ staleCommit = FALSE
    /\ stopped = (a :> FALSE @@ b :> FALSE @@ c :> FALSE @@ d :> FALSE)
    /\ successor = a
    /\ ver = 0
    /\ wipedAt = (a :> 0 @@ b :> 0 @@ c :> 0 @@ d :> 0)
    /\ wipes = 0
    /\ writes = 0

Step(k) ==
    CASE
         k = 1 ->
             /\ ballot' = (a :> {a} @@ b :> {b} @@ c :> {} @@ d :> {})
             /\ bexpiry' = (a :> 2 @@ b :> 2 @@ c :> 0 @@ d :> 0)
             /\ bgen' = (a :> 1 @@ b :> 2 @@ c :> 0 @@ d :> 0)
             /\ fencing' = (a :> FALSE @@ b :> TRUE @@ c :> FALSE @@ d :> FALSE)
             /\ gen' = 2
             /\ leader' = b
             /\ promised' = (a :> 1 @@ b :> 2 @@ c :> 0 @@ d :> 0)
             /\ ver' = 1
             /\ UNCHANGED << ackGen, acked, answered, cacked, canswered, cconfirmed, checked, chwm, clock, clog, confirmed, cpExpiry, cpLearner, cpNext, cpNextGen, cpView, cutOver, draining, expect, fenceIds, gaveUp, halted, hbAt, hbOut, heard, hwm, inc, inflight, inherited, installed, joinedAt, joining, learner, leaving, leftDirty, log, marked, mine, moves, named, now, oldSet, opened, out, pending, queued, readmit, registered, report, restarting, seen, seenInc, setGen, sid, splits, staged, staleCommit, stopped, successor, wipedAt, wipes, writes, cpGone >>
      []          k = 2 ->
             /\ queued' = (a :> 1 @@ b :> 0 @@ c :> 0 @@ d :> 0)
             /\ writes' = 1
             /\ UNCHANGED << ackGen, acked, answered, ballot, bexpiry, bgen, cacked, canswered, cconfirmed, checked, chwm, clock, clog, confirmed, cpExpiry, cpLearner, cpNext, cpNextGen, cpView, cutOver, draining, expect, fenceIds, fencing, gaveUp, gen, halted, hbAt, hbOut, heard, hwm, inc, inflight, inherited, installed, joinedAt, joining, leader, learner, leaving, leftDirty, log, marked, mine, moves, named, now, oldSet, opened, out, pending, promised, readmit, registered, report, restarting, seen, seenInc, setGen, sid, splits, staged, staleCommit, stopped, successor, ver, wipedAt, wipes, cpGone >>
      []          k = 3 ->
             /\ hbOut' = (a :> TRUE @@ b :> FALSE @@ c :> FALSE @@ d :> FALSE)
             /\ UNCHANGED << ackGen, acked, answered, ballot, bexpiry, bgen, cacked, canswered, cconfirmed, checked, chwm, clock, clog, confirmed, cpExpiry, cpLearner, cpNext, cpNextGen, cpView, cutOver, draining, expect, fenceIds, fencing, gaveUp, gen, halted, hbAt, heard, hwm, inc, inflight, inherited, installed, joinedAt, joining, leader, learner, leaving, leftDirty, log, marked, mine, moves, named, now, oldSet, opened, out, pending, promised, queued, readmit, registered, report, restarting, seen, seenInc, setGen, sid, splits, staged, staleCommit, stopped, successor, ver, wipedAt, wipes, writes, cpGone >>
      []          k = 4 ->
             /\ hbOut' = (a :> TRUE @@ b :> TRUE @@ c :> FALSE @@ d :> FALSE)
             /\ UNCHANGED << ackGen, acked, answered, ballot, bexpiry, bgen, cacked, canswered, cconfirmed, checked, chwm, clock, clog, confirmed, cpExpiry, cpLearner, cpNext, cpNextGen, cpView, cutOver, draining, expect, fenceIds, fencing, gaveUp, gen, halted, hbAt, heard, hwm, inc, inflight, inherited, installed, joinedAt, joining, leader, learner, leaving, leftDirty, log, marked, mine, moves, named, now, oldSet, opened, out, pending, promised, queued, readmit, registered, report, restarting, seen, seenInc, setGen, sid, splits, staged, staleCommit, stopped, successor, ver, wipedAt, wipes, writes, cpGone >>
      []          k = 5 ->
             /\ clock' = (a :> 1 @@ b :> 1 @@ c :> 2 @@ d :> 2)
             /\ now' = 1
             /\ UNCHANGED << ackGen, acked, answered, ballot, bexpiry, bgen, cacked, canswered, cconfirmed, checked, chwm, clog, confirmed, cpExpiry, cpLearner, cpNext, cpNextGen, cpView, cutOver, draining, expect, fenceIds, fencing, gaveUp, gen, halted, hbAt, hbOut, heard, hwm, inc, inflight, inherited, installed, joinedAt, joining, leader, learner, leaving, leftDirty, log, marked, mine, moves, named, oldSet, opened, out, pending, promised, queued, readmit, registered, report, restarting, seen, seenInc, setGen, sid, splits, staged, staleCommit, stopped, successor, ver, wipedAt, wipes, writes, cpGone >>
      []          k = 6 ->
             /\ bgen' = (a :> 0 @@ b :> 2 @@ c :> 0 @@ d :> 0)
             /\ queued' = (a :> 0 @@ b :> 0 @@ c :> 0 @@ d :> 0)
             /\ UNCHANGED << ackGen, acked, answered, ballot, bexpiry, cacked, canswered, cconfirmed, checked, chwm, clock, clog, confirmed, cpExpiry, cpLearner, cpNext, cpNextGen, cpView, cutOver, draining, expect, fenceIds, fencing, gaveUp, gen, halted, hbAt, hbOut, heard, hwm, inc, inflight, inherited, installed, joinedAt, joining, leader, learner, leaving, leftDirty, log, marked, mine, moves, named, now, oldSet, opened, out, pending, promised, readmit, registered, report, restarting, seen, seenInc, setGen, sid, splits, staged, staleCommit, stopped, successor, ver, wipedAt, wipes, writes, cpGone >>
      []          k = 7 ->
             /\ bexpiry' = (a :> 3 @@ b :> 2 @@ c :> 0 @@ d :> 0)
             /\ bgen' = (a :> 3 @@ b :> 2 @@ c :> 0 @@ d :> 0)
             /\ cpExpiry' = 3
             /\ fencing' = (a :> TRUE @@ b :> TRUE @@ c :> FALSE @@ d :> FALSE)
             /\ gen' = 3
             /\ leader' = a
             /\ promised' = (a :> 3 @@ b :> 2 @@ c :> 0 @@ d :> 0)
             /\ ver' = 2
             /\ UNCHANGED << ackGen, acked, answered, ballot, cacked, canswered, cconfirmed, checked, chwm, clock, clog, confirmed, cpLearner, cpNext, cpNextGen, cpView, cutOver, draining, expect, fenceIds, gaveUp, halted, hbAt, hbOut, heard, hwm, inc, inflight, inherited, installed, joinedAt, joining, learner, leaving, leftDirty, log, marked, mine, moves, named, now, oldSet, opened, out, pending, queued, readmit, registered, report, restarting, seen, seenInc, setGen, sid, splits, staged, staleCommit, stopped, successor, wipedAt, wipes, writes, cpGone >>
      []          k = 8 ->
             /\ hbOut' = (a :> FALSE @@ b :> TRUE @@ c :> FALSE @@ d :> FALSE)
             /\ UNCHANGED << ackGen, acked, answered, ballot, bexpiry, bgen, cacked, canswered, cconfirmed, checked, chwm, clock, clog, confirmed, cpExpiry, cpLearner, cpNext, cpNextGen, cpView, cutOver, draining, expect, fenceIds, fencing, gaveUp, gen, halted, hbAt, heard, hwm, inc, inflight, inherited, installed, joinedAt, joining, leader, learner, leaving, leftDirty, log, marked, mine, moves, named, now, oldSet, opened, out, pending, promised, queued, readmit, registered, report, restarting, seen, seenInc, setGen, sid, splits, staged, staleCommit, stopped, successor, ver, wipedAt, wipes, writes, cpGone >>
      []          k = 9 ->
             /\ answered' = (a :> {b} @@ b :> {} @@ c :> {} @@ d :> {})
             /\ ballot' = (a :> {a} @@ b :> {a} @@ c :> {} @@ d :> {})
             /\ promised' = (a :> 3 @@ b :> 3 @@ c :> 0 @@ d :> 0)
             /\ UNCHANGED << ackGen, acked, bexpiry, bgen, cacked, canswered, cconfirmed, checked, chwm, clock, clog, confirmed, cpExpiry, cpLearner, cpNext, cpNextGen, cpView, cutOver, draining, expect, fenceIds, fencing, gaveUp, gen, halted, hbAt, hbOut, heard, hwm, inc, inflight, inherited, installed, joinedAt, joining, leader, learner, leaving, leftDirty, log, marked, mine, moves, named, now, oldSet, opened, out, pending, queued, readmit, registered, report, restarting, seen, seenInc, setGen, sid, splits, staged, staleCommit, stopped, successor, ver, wipedAt, wipes, writes, cpGone >>
      []          k = 10 ->
             /\ ballot' = (a :> {a} @@ b :> {b} @@ c :> {} @@ d :> {})
             /\ bexpiry' = (a :> 3 @@ b :> 3 @@ c :> 0 @@ d :> 0)
             /\ bgen' = (a :> 3 @@ b :> 4 @@ c :> 0 @@ d :> 0)
             /\ gen' = 4
             /\ leader' = b
             /\ promised' = (a :> 3 @@ b :> 4 @@ c :> 0 @@ d :> 0)
             /\ ver' = 3
             /\ UNCHANGED << ackGen, acked, answered, cacked, canswered, cconfirmed, checked, chwm, clock, clog, confirmed, cpExpiry, cpLearner, cpNext, cpNextGen, cpView, cutOver, draining, expect, fenceIds, fencing, gaveUp, halted, hbAt, hbOut, heard, hwm, inc, inflight, inherited, installed, joinedAt, joining, learner, leaving, leftDirty, log, marked, mine, moves, named, now, oldSet, opened, out, pending, queued, readmit, registered, report, restarting, seen, seenInc, setGen, sid, splits, staged, staleCommit, stopped, successor, wipedAt, wipes, writes, cpGone >>
      []          k = 11 ->
             /\ answered' = (a :> {b} @@ b :> {a} @@ c :> {} @@ d :> {})
             /\ ballot' = (a :> {b} @@ b :> {b} @@ c :> {} @@ d :> {})
             /\ promised' = (a :> 4 @@ b :> 4 @@ c :> 0 @@ d :> 0)
             /\ UNCHANGED << ackGen, acked, bexpiry, bgen, cacked, canswered, cconfirmed, checked, chwm, clock, clog, confirmed, cpExpiry, cpLearner, cpNext, cpNextGen, cpView, cutOver, draining, expect, fenceIds, fencing, gaveUp, gen, halted, hbAt, hbOut, heard, hwm, inc, inflight, inherited, installed, joinedAt, joining, leader, learner, leaving, leftDirty, log, marked, mine, moves, named, now, oldSet, opened, out, pending, queued, readmit, registered, report, restarting, seen, seenInc, setGen, sid, splits, staged, staleCommit, stopped, successor, ver, wipedAt, wipes, writes, cpGone >>
      []          k = 12 ->
             /\ answered' = (a :> {} @@ b :> {a} @@ c :> {} @@ d :> {})
             /\ ballot' = (a :> {a} @@ b :> {b} @@ c :> {} @@ d :> {})
             /\ bgen' = (a :> 5 @@ b :> 4 @@ c :> 0 @@ d :> 0)
             /\ gen' = 5
             /\ leader' = a
             /\ promised' = (a :> 5 @@ b :> 4 @@ c :> 0 @@ d :> 0)
             /\ ver' = 4
             /\ UNCHANGED << ackGen, acked, bexpiry, cacked, canswered, cconfirmed, checked, chwm, clock, clog, confirmed, cpExpiry, cpLearner, cpNext, cpNextGen, cpView, cutOver, draining, expect, fenceIds, fencing, gaveUp, halted, hbAt, hbOut, heard, hwm, inc, inflight, inherited, installed, joinedAt, joining, learner, leaving, leftDirty, log, marked, mine, moves, named, now, oldSet, opened, out, pending, queued, readmit, registered, report, restarting, seen, seenInc, setGen, sid, splits, staged, staleCommit, stopped, successor, wipedAt, wipes, writes, cpGone >>
      []          k = 13 ->
             /\ clock' = (a :> 1 @@ b :> 3 @@ c :> 2 @@ d :> 2)
             /\ now' = 2
             /\ UNCHANGED << ackGen, acked, answered, ballot, bexpiry, bgen, cacked, canswered, cconfirmed, checked, chwm, clog, confirmed, cpExpiry, cpLearner, cpNext, cpNextGen, cpView, cutOver, draining, expect, fenceIds, fencing, gaveUp, gen, halted, hbAt, hbOut, heard, hwm, inc, inflight, inherited, installed, joinedAt, joining, leader, learner, leaving, leftDirty, log, marked, mine, moves, named, oldSet, opened, out, pending, promised, queued, readmit, registered, report, restarting, seen, seenInc, setGen, sid, splits, staged, staleCommit, stopped, successor, ver, wipedAt, wipes, writes, cpGone >>
      []          k = 14 ->
             /\ bgen' = (a :> 5 @@ b :> 0 @@ c :> 0 @@ d :> 0)
             /\ fencing' = (a :> TRUE @@ b :> FALSE @@ c :> FALSE @@ d :> FALSE)
             /\ UNCHANGED << ackGen, acked, answered, ballot, bexpiry, cacked, canswered, cconfirmed, checked, chwm, clock, clog, confirmed, cpExpiry, cpLearner, cpNext, cpNextGen, cpView, cutOver, draining, expect, fenceIds, gaveUp, gen, halted, hbAt, hbOut, heard, hwm, inc, inflight, inherited, installed, joinedAt, joining, leader, learner, leaving, leftDirty, log, marked, mine, moves, named, now, oldSet, opened, out, pending, promised, queued, readmit, registered, report, restarting, seen, seenInc, setGen, sid, splits, staged, staleCommit, stopped, successor, ver, wipedAt, wipes, writes, cpGone >>
      []          k = 15 ->
             /\ answered' = (a :> {} @@ b :> {} @@ c :> {} @@ d :> {})
             /\ bexpiry' = (a :> 3 @@ b :> 5 @@ c :> 0 @@ d :> 0)
             /\ bgen' = (a :> 5 @@ b :> 6 @@ c :> 0 @@ d :> 0)
             /\ cpExpiry' = 4
             /\ fencing' = (a :> TRUE @@ b :> TRUE @@ c :> FALSE @@ d :> FALSE)
             /\ gen' = 6
             /\ leader' = b
             /\ promised' = (a :> 5 @@ b :> 6 @@ c :> 0 @@ d :> 0)
             /\ ver' = 5
             /\ UNCHANGED << ackGen, acked, ballot, cacked, canswered, cconfirmed, checked, chwm, clock, clog, confirmed, cpLearner, cpNext, cpNextGen, cpView, cutOver, draining, expect, fenceIds, gaveUp, halted, hbAt, hbOut, heard, hwm, inc, inflight, inherited, installed, joinedAt, joining, learner, leaving, leftDirty, log, marked, mine, moves, named, now, oldSet, opened, out, pending, queued, readmit, registered, report, restarting, seen, seenInc, setGen, sid, splits, staged, staleCommit, stopped, successor, wipedAt, wipes, writes, cpGone >>
      []          k = 16 ->
             /\ bgen' = (a :> 7 @@ b :> 6 @@ c :> 0 @@ d :> 0)
             /\ gen' = 7
             /\ leader' = a
             /\ promised' = (a :> 7 @@ b :> 6 @@ c :> 0 @@ d :> 0)
             /\ ver' = 6
             /\ UNCHANGED << ackGen, acked, answered, ballot, bexpiry, cacked, canswered, cconfirmed, checked, chwm, clock, clog, confirmed, cpExpiry, cpLearner, cpNext, cpNextGen, cpView, cutOver, draining, expect, fenceIds, fencing, gaveUp, halted, hbAt, hbOut, heard, hwm, inc, inflight, inherited, installed, joinedAt, joining, learner, leaving, leftDirty, log, marked, mine, moves, named, now, oldSet, opened, out, pending, queued, readmit, registered, report, restarting, seen, seenInc, setGen, sid, splits, staged, staleCommit, stopped, successor, wipedAt, wipes, writes, cpGone >>
      []          k = 17 ->
             /\ answered' = (a :> {b} @@ b :> {} @@ c :> {} @@ d :> {})
             /\ ballot' = (a :> {a} @@ b :> {a} @@ c :> {} @@ d :> {})
             /\ promised' = (a :> 7 @@ b :> 7 @@ c :> 0 @@ d :> 0)
             /\ UNCHANGED << ackGen, acked, bexpiry, bgen, cacked, canswered, cconfirmed, checked, chwm, clock, clog, confirmed, cpExpiry, cpLearner, cpNext, cpNextGen, cpView, cutOver, draining, expect, fenceIds, fencing, gaveUp, gen, halted, hbAt, hbOut, heard, hwm, inc, inflight, inherited, installed, joinedAt, joining, leader, learner, leaving, leftDirty, log, marked, mine, moves, named, now, oldSet, opened, out, pending, queued, readmit, registered, report, restarting, seen, seenInc, setGen, sid, splits, staged, staleCommit, stopped, successor, ver, wipedAt, wipes, writes, cpGone >>
      []          k = 18 ->
             /\ fencing' = (a :> FALSE @@ b :> TRUE @@ c :> FALSE @@ d :> FALSE)
             /\ log' = (a :> <<[g |-> 7, id |-> 0, lg |-> 7]>> @@ b :> <<>> @@ c :> <<>> @@ d :> <<>>)
             /\ opened' = {<<1, a>>, <<7, a>>}
             /\ UNCHANGED << ackGen, acked, answered, ballot, bexpiry, bgen, cacked, canswered, cconfirmed, checked, chwm, clock, clog, confirmed, cpExpiry, cpLearner, cpNext, cpNextGen, cpView, cutOver, draining, expect, fenceIds, gaveUp, gen, halted, hbAt, hbOut, heard, hwm, inc, inflight, inherited, installed, joinedAt, joining, leader, learner, leaving, leftDirty, marked, mine, moves, named, now, oldSet, out, pending, promised, queued, readmit, registered, report, restarting, seen, seenInc, setGen, sid, splits, staged, staleCommit, stopped, successor, ver, wipedAt, wipes, writes, cpGone >>
      []          k = 19 ->
             /\ bgen' = (a :> 7 @@ b :> 0 @@ c :> 0 @@ d :> 0)
             /\ fencing' = (a :> FALSE @@ b :> FALSE @@ c :> FALSE @@ d :> FALSE)
             /\ UNCHANGED << ackGen, acked, answered, ballot, bexpiry, cacked, canswered, cconfirmed, checked, chwm, clock, clog, confirmed, cpExpiry, cpLearner, cpNext, cpNextGen, cpView, cutOver, draining, expect, fenceIds, gaveUp, gen, halted, hbAt, hbOut, heard, hwm, inc, inflight, inherited, installed, joinedAt, joining, leader, learner, leaving, leftDirty, log, marked, mine, moves, named, now, oldSet, opened, out, pending, promised, queued, readmit, registered, report, restarting, seen, seenInc, setGen, sid, splits, staged, staleCommit, stopped, successor, ver, wipedAt, wipes, writes, cpGone >>
      []          k = 20 ->
             /\ clock' = (a :> 3 @@ b :> 4 @@ c :> 2 @@ d :> 3)
             /\ now' = 3
             /\ UNCHANGED << ackGen, acked, answered, ballot, bexpiry, bgen, cacked, canswered, cconfirmed, checked, chwm, clog, confirmed, cpExpiry, cpLearner, cpNext, cpNextGen, cpView, cutOver, draining, expect, fenceIds, fencing, gaveUp, gen, halted, hbAt, hbOut, heard, hwm, inc, inflight, inherited, installed, joinedAt, joining, leader, learner, leaving, leftDirty, log, marked, mine, moves, named, oldSet, opened, out, pending, promised, queued, readmit, registered, report, restarting, seen, seenInc, setGen, sid, splits, staged, staleCommit, stopped, successor, ver, wipedAt, wipes, writes, cpGone >>
      []          k = 21 ->
             /\ clock' = (a :> 5 @@ b :> 5 @@ c :> 3 @@ d :> 3)
             /\ now' = 4
             /\ UNCHANGED << ackGen, acked, answered, ballot, bexpiry, bgen, cacked, canswered, cconfirmed, checked, chwm, clog, confirmed, cpExpiry, cpLearner, cpNext, cpNextGen, cpView, cutOver, draining, expect, fenceIds, fencing, gaveUp, gen, halted, hbAt, hbOut, heard, hwm, inc, inflight, inherited, installed, joinedAt, joining, leader, learner, leaving, leftDirty, log, marked, mine, moves, named, oldSet, opened, out, pending, promised, queued, readmit, registered, report, restarting, seen, seenInc, setGen, sid, splits, staged, staleCommit, stopped, successor, ver, wipedAt, wipes, writes, cpGone >>
      []          k = 22 ->
             /\ clock' = (a :> 5 @@ b :> 6 @@ c :> 5 @@ d :> 5)
             /\ now' = 5
             /\ UNCHANGED << ackGen, acked, answered, ballot, bexpiry, bgen, cacked, canswered, cconfirmed, checked, chwm, clog, confirmed, cpExpiry, cpLearner, cpNext, cpNextGen, cpView, cutOver, draining, expect, fenceIds, fencing, gaveUp, gen, halted, hbAt, hbOut, heard, hwm, inc, inflight, inherited, installed, joinedAt, joining, leader, learner, leaving, leftDirty, log, marked, mine, moves, named, oldSet, opened, out, pending, promised, queued, readmit, registered, report, restarting, seen, seenInc, setGen, sid, splits, staged, staleCommit, stopped, successor, ver, wipedAt, wipes, writes, cpGone >>
      []          k = 23 ->
             /\ hbOut' = (a :> FALSE @@ b :> FALSE @@ c :> FALSE @@ d :> FALSE)
             /\ UNCHANGED << ackGen, acked, answered, ballot, bexpiry, bgen, cacked, canswered, cconfirmed, checked, chwm, clock, clog, confirmed, cpExpiry, cpLearner, cpNext, cpNextGen, cpView, cutOver, draining, expect, fenceIds, fencing, gaveUp, gen, halted, hbAt, heard, hwm, inc, inflight, inherited, installed, joinedAt, joining, leader, learner, leaving, leftDirty, log, marked, mine, moves, named, now, oldSet, opened, out, pending, promised, queued, readmit, registered, report, restarting, seen, seenInc, setGen, sid, splits, staged, staleCommit, stopped, successor, ver, wipedAt, wipes, writes, cpGone >>
      []          k = 24 ->
             /\ queued' = (a :> 2 @@ b :> 0 @@ c :> 0 @@ d :> 0)
             /\ writes' = 2
             /\ UNCHANGED << ackGen, acked, answered, ballot, bexpiry, bgen, cacked, canswered, cconfirmed, checked, chwm, clock, clog, confirmed, cpExpiry, cpLearner, cpNext, cpNextGen, cpView, cutOver, draining, expect, fenceIds, fencing, gaveUp, gen, halted, hbAt, hbOut, heard, hwm, inc, inflight, inherited, installed, joinedAt, joining, leader, learner, leaving, leftDirty, log, marked, mine, moves, named, now, oldSet, opened, out, pending, promised, readmit, registered, report, restarting, seen, seenInc, setGen, sid, splits, staged, staleCommit, stopped, successor, ver, wipedAt, wipes, cpGone >>
      []          k = 25 ->
             /\ pending' = (a :> 2 @@ b :> 0 @@ c :> 0 @@ d :> 0)
             /\ queued' = (a :> 0 @@ b :> 0 @@ c :> 0 @@ d :> 0)
             /\ UNCHANGED << ackGen, acked, answered, ballot, bexpiry, bgen, cacked, canswered, cconfirmed, checked, chwm, clock, clog, confirmed, cpExpiry, cpLearner, cpNext, cpNextGen, cpView, cutOver, draining, expect, fenceIds, fencing, gaveUp, gen, halted, hbAt, hbOut, heard, hwm, inc, inflight, inherited, installed, joinedAt, joining, leader, learner, leaving, leftDirty, log, marked, mine, moves, named, now, oldSet, opened, out, promised, readmit, registered, report, restarting, seen, seenInc, setGen, sid, splits, staged, staleCommit, stopped, successor, ver, wipedAt, wipes, writes, cpGone >>
      []          k = 26 ->
             /\ log' = (a :> <<[g |-> 7, id |-> 0, lg |-> 7], [g |-> 7, id |-> 2, lg |-> 7]>> @@ b :> <<>> @@ c :> <<>> @@ d :> <<>>)
             /\ pending' = (a :> 0 @@ b :> 0 @@ c :> 0 @@ d :> 0)
             /\ UNCHANGED << ackGen, acked, answered, ballot, bexpiry, bgen, cacked, canswered, cconfirmed, checked, chwm, clock, clog, confirmed, cpExpiry, cpLearner, cpNext, cpNextGen, cpView, cutOver, draining, expect, fenceIds, fencing, gaveUp, gen, halted, hbAt, hbOut, heard, hwm, inc, inflight, inherited, installed, joinedAt, joining, leader, learner, leaving, leftDirty, marked, mine, moves, named, now, oldSet, opened, out, promised, queued, readmit, registered, report, restarting, seen, seenInc, setGen, sid, splits, staged, staleCommit, stopped, successor, ver, wipedAt, wipes, writes, cpGone >>
      []          k = 27 ->
             /\ hbAt' = (a :> 5 @@ b :> 0 @@ c :> 0 @@ d :> 0)
             /\ hbOut' = (a :> TRUE @@ b :> FALSE @@ c :> FALSE @@ d :> FALSE)
             /\ UNCHANGED << ackGen, acked, answered, ballot, bexpiry, bgen, cacked, canswered, cconfirmed, checked, chwm, clock, clog, confirmed, cpExpiry, cpLearner, cpNext, cpNextGen, cpView, cutOver, draining, expect, fenceIds, fencing, gaveUp, gen, halted, heard, hwm, inc, inflight, inherited, installed, joinedAt, joining, leader, learner, leaving, leftDirty, log, marked, mine, moves, named, now, oldSet, opened, out, pending, promised, queued, readmit, registered, report, restarting, seen, seenInc, setGen, sid, splits, staged, staleCommit, stopped, successor, ver, wipedAt, wipes, writes, cpGone >>
      []          k = 28 ->
             /\ hbOut' = (a :> FALSE @@ b :> FALSE @@ c :> FALSE @@ d :> FALSE)
             /\ UNCHANGED << ackGen, acked, answered, ballot, bexpiry, bgen, cacked, canswered, cconfirmed, checked, chwm, clock, clog, confirmed, cpExpiry, cpLearner, cpNext, cpNextGen, cpView, cutOver, draining, expect, fenceIds, fencing, gaveUp, gen, halted, hbAt, heard, hwm, inc, inflight, inherited, installed, joinedAt, joining, leader, learner, leaving, leftDirty, log, marked, mine, moves, named, now, oldSet, opened, out, pending, promised, queued, readmit, registered, report, restarting, seen, seenInc, setGen, sid, splits, staged, staleCommit, stopped, successor, ver, wipedAt, wipes, writes, cpGone >>
      []          k = 29 ->
             /\ queued' = (a :> 3 @@ b :> 0 @@ c :> 0 @@ d :> 0)
             /\ writes' = 3
             /\ UNCHANGED << ackGen, acked, answered, ballot, bexpiry, bgen, cacked, canswered, cconfirmed, checked, chwm, clock, clog, confirmed, cpExpiry, cpLearner, cpNext, cpNextGen, cpView, cutOver, draining, expect, fenceIds, fencing, gaveUp, gen, halted, hbAt, hbOut, heard, hwm, inc, inflight, inherited, installed, joinedAt, joining, leader, learner, leaving, leftDirty, log, marked, mine, moves, named, now, oldSet, opened, out, pending, promised, readmit, registered, report, restarting, seen, seenInc, setGen, sid, splits, staged, staleCommit, stopped, successor, ver, wipedAt, wipes, cpGone >>
      []          k = 30 ->
             /\ ballot' = (a :> {a} @@ b :> {b} @@ c :> {} @@ d :> {})
             /\ bexpiry' = (a :> 3 @@ b :> 8 @@ c :> 0 @@ d :> 0)
             /\ bgen' = (a :> 7 @@ b :> 8 @@ c :> 0 @@ d :> 0)
             /\ cpExpiry' = 7
             /\ fencing' = (a :> FALSE @@ b :> TRUE @@ c :> FALSE @@ d :> FALSE)
             /\ gen' = 8
             /\ leader' = b
             /\ promised' = (a :> 7 @@ b :> 8 @@ c :> 0 @@ d :> 0)
             /\ ver' = 7
             /\ UNCHANGED << ackGen, acked, answered, cacked, canswered, cconfirmed, checked, chwm, clock, clog, confirmed, cpLearner, cpNext, cpNextGen, cpView, cutOver, draining, expect, fenceIds, gaveUp, halted, hbAt, hbOut, heard, hwm, inc, inflight, inherited, installed, joinedAt, joining, learner, leaving, leftDirty, log, marked, mine, moves, named, now, oldSet, opened, out, pending, queued, readmit, registered, report, restarting, seen, seenInc, setGen, sid, splits, staged, staleCommit, stopped, successor, wipedAt, wipes, writes, cpGone >>
      []          k = 31 ->
             /\ hbOut' = (a :> TRUE @@ b :> FALSE @@ c :> FALSE @@ d :> FALSE)
             /\ UNCHANGED << ackGen, acked, answered, ballot, bexpiry, bgen, cacked, canswered, cconfirmed, checked, chwm, clock, clog, confirmed, cpExpiry, cpLearner, cpNext, cpNextGen, cpView, cutOver, draining, expect, fenceIds, fencing, gaveUp, gen, halted, hbAt, heard, hwm, inc, inflight, inherited, installed, joinedAt, joining, leader, learner, leaving, leftDirty, log, marked, mine, moves, named, now, oldSet, opened, out, pending, promised, queued, readmit, registered, report, restarting, seen, seenInc, setGen, sid, splits, staged, staleCommit, stopped, successor, ver, wipedAt, wipes, writes, cpGone >>
      []          k = 32 ->
             /\ answered' = (a :> {} @@ b :> {} @@ c :> {} @@ d :> {})
             /\ bexpiry' = (a :> 7 @@ b :> 8 @@ c :> 0 @@ d :> 0)
             /\ bgen' = (a :> 9 @@ b :> 8 @@ c :> 0 @@ d :> 0)
             /\ fencing' = (a :> TRUE @@ b :> TRUE @@ c :> FALSE @@ d :> FALSE)
             /\ gen' = 9
             /\ leader' = a
             /\ promised' = (a :> 9 @@ b :> 8 @@ c :> 0 @@ d :> 0)
             /\ queued' = (a :> 0 @@ b :> 0 @@ c :> 0 @@ d :> 0)
             /\ ver' = 8
             /\ UNCHANGED << ackGen, acked, ballot, cacked, canswered, cconfirmed, checked, chwm, clock, clog, confirmed, cpExpiry, cpLearner, cpNext, cpNextGen, cpView, cutOver, draining, expect, fenceIds, gaveUp, halted, hbAt, hbOut, heard, hwm, inc, inflight, inherited, installed, joinedAt, joining, learner, leaving, leftDirty, log, marked, mine, moves, named, now, oldSet, opened, out, pending, readmit, registered, report, restarting, seen, seenInc, setGen, sid, splits, staged, staleCommit, stopped, successor, wipedAt, wipes, writes, cpGone >>
      []          k = 33 ->
             /\ bgen' = (a :> 9 @@ b :> 10 @@ c :> 0 @@ d :> 0)
             /\ gen' = 10
             /\ leader' = b
             /\ promised' = (a :> 9 @@ b :> 10 @@ c :> 0 @@ d :> 0)
             /\ ver' = 9
             /\ UNCHANGED << ackGen, acked, answered, ballot, bexpiry, cacked, canswered, cconfirmed, checked, chwm, clock, clog, confirmed, cpExpiry, cpLearner, cpNext, cpNextGen, cpView, cutOver, draining, expect, fenceIds, fencing, gaveUp, halted, hbAt, hbOut, heard, hwm, inc, inflight, inherited, installed, joinedAt, joining, learner, leaving, leftDirty, log, marked, mine, moves, named, now, oldSet, opened, out, pending, queued, readmit, registered, report, restarting, seen, seenInc, setGen, sid, splits, staged, staleCommit, stopped, successor, wipedAt, wipes, writes, cpGone >>
      []          k = 34 ->
             /\ bgen' = (a :> 11 @@ b :> 10 @@ c :> 0 @@ d :> 0)
             /\ gen' = 11
             /\ leader' = a
             /\ promised' = (a :> 11 @@ b :> 10 @@ c :> 0 @@ d :> 0)
             /\ ver' = 10
             /\ UNCHANGED << ackGen, acked, answered, ballot, bexpiry, cacked, canswered, cconfirmed, checked, chwm, clock, clog, confirmed, cpExpiry, cpLearner, cpNext, cpNextGen, cpView, cutOver, draining, expect, fenceIds, fencing, gaveUp, halted, hbAt, hbOut, heard, hwm, inc, inflight, inherited, installed, joinedAt, joining, learner, leaving, leftDirty, log, marked, mine, moves, named, now, oldSet, opened, out, pending, queued, readmit, registered, report, restarting, seen, seenInc, setGen, sid, splits, staged, staleCommit, stopped, successor, wipedAt, wipes, writes, cpGone >>
      []          k = 35 ->
             /\ bgen' = (a :> 11 @@ b :> 0 @@ c :> 0 @@ d :> 0)
             /\ fencing' = (a :> TRUE @@ b :> FALSE @@ c :> FALSE @@ d :> FALSE)
             /\ UNCHANGED << ackGen, acked, answered, ballot, bexpiry, cacked, canswered, cconfirmed, checked, chwm, clock, clog, confirmed, cpExpiry, cpLearner, cpNext, cpNextGen, cpView, cutOver, draining, expect, fenceIds, gaveUp, gen, halted, hbAt, hbOut, heard, hwm, inc, inflight, inherited, installed, joinedAt, joining, leader, learner, leaving, leftDirty, log, marked, mine, moves, named, now, oldSet, opened, out, pending, promised, queued, readmit, registered, report, restarting, seen, seenInc, setGen, sid, splits, staged, staleCommit, stopped, successor, ver, wipedAt, wipes, writes, cpGone >>
      []          k = 36 ->
             /\ hbOut' = (a :> FALSE @@ b :> FALSE @@ c :> FALSE @@ d :> FALSE)
             /\ UNCHANGED << ackGen, acked, answered, ballot, bexpiry, bgen, cacked, canswered, cconfirmed, checked, chwm, clock, clog, confirmed, cpExpiry, cpLearner, cpNext, cpNextGen, cpView, cutOver, draining, expect, fenceIds, fencing, gaveUp, gen, halted, hbAt, heard, hwm, inc, inflight, inherited, installed, joinedAt, joining, leader, learner, leaving, leftDirty, log, marked, mine, moves, named, now, oldSet, opened, out, pending, promised, queued, readmit, registered, report, restarting, seen, seenInc, setGen, sid, splits, staged, staleCommit, stopped, successor, ver, wipedAt, wipes, writes, cpGone >>
      []          k = 37 ->
             /\ hbOut' = (a :> TRUE @@ b :> FALSE @@ c :> FALSE @@ d :> FALSE)
             /\ UNCHANGED << ackGen, acked, answered, ballot, bexpiry, bgen, cacked, canswered, cconfirmed, checked, chwm, clock, clog, confirmed, cpExpiry, cpLearner, cpNext, cpNextGen, cpView, cutOver, draining, expect, fenceIds, fencing, gaveUp, gen, halted, hbAt, heard, hwm, inc, inflight, inherited, installed, joinedAt, joining, leader, learner, leaving, leftDirty, log, marked, mine, moves, named, now, oldSet, opened, out, pending, promised, queued, readmit, registered, report, restarting, seen, seenInc, setGen, sid, splits, staged, staleCommit, stopped, successor, ver, wipedAt, wipes, writes, cpGone >>
      []          k = 38 ->
             /\ answered' = (a :> {b} @@ b :> {} @@ c :> {} @@ d :> {})
             /\ ballot' = (a :> {a} @@ b :> {a} @@ c :> {} @@ d :> {})
             /\ promised' = (a :> 11 @@ b :> 11 @@ c :> 0 @@ d :> 0)
             /\ UNCHANGED << ackGen, acked, bexpiry, bgen, cacked, canswered, cconfirmed, checked, chwm, clock, clog, confirmed, cpExpiry, cpLearner, cpNext, cpNextGen, cpView, cutOver, draining, expect, fenceIds, fencing, gaveUp, gen, halted, hbAt, hbOut, heard, hwm, inc, inflight, inherited, installed, joinedAt, joining, leader, learner, leaving, leftDirty, log, marked, mine, moves, named, now, oldSet, opened, out, pending, queued, readmit, registered, report, restarting, seen, seenInc, setGen, sid, splits, staged, staleCommit, stopped, successor, ver, wipedAt, wipes, writes, cpGone >>
      []          k = 39 ->
             /\ clock' = (a :> 6 @@ b :> 7 @@ c :> 5 @@ d :> 6)
             /\ now' = 6
             /\ UNCHANGED << ackGen, acked, answered, ballot, bexpiry, bgen, cacked, canswered, cconfirmed, checked, chwm, clog, confirmed, cpExpiry, cpLearner, cpNext, cpNextGen, cpView, cutOver, draining, expect, fenceIds, fencing, gaveUp, gen, halted, hbAt, hbOut, heard, hwm, inc, inflight, inherited, installed, joinedAt, joining, leader, learner, leaving, leftDirty, log, marked, mine, moves, named, oldSet, opened, out, pending, promised, queued, readmit, registered, report, restarting, seen, seenInc, setGen, sid, splits, staged, staleCommit, stopped, successor, ver, wipedAt, wipes, writes, cpGone >>
      []          k = 40 ->
             /\ clock' = (a :> 8 @@ b :> 8 @@ c :> 7 @@ d :> 6)
             /\ now' = 7
             /\ UNCHANGED << ackGen, acked, answered, ballot, bexpiry, bgen, cacked, canswered, cconfirmed, checked, chwm, clog, confirmed, cpExpiry, cpLearner, cpNext, cpNextGen, cpView, cutOver, draining, expect, fenceIds, fencing, gaveUp, gen, halted, hbAt, hbOut, heard, hwm, inc, inflight, inherited, installed, joinedAt, joining, leader, learner, leaving, leftDirty, log, marked, mine, moves, named, oldSet, opened, out, pending, promised, queued, readmit, registered, report, restarting, seen, seenInc, setGen, sid, splits, staged, staleCommit, stopped, successor, ver, wipedAt, wipes, writes, cpGone >>
      []          k = 41 ->
             /\ hbOut' = (a :> FALSE @@ b :> FALSE @@ c :> FALSE @@ d :> FALSE)
             /\ UNCHANGED << ackGen, acked, answered, ballot, bexpiry, bgen, cacked, canswered, cconfirmed, checked, chwm, clock, clog, confirmed, cpExpiry, cpLearner, cpNext, cpNextGen, cpView, cutOver, draining, expect, fenceIds, fencing, gaveUp, gen, halted, hbAt, heard, hwm, inc, inflight, inherited, installed, joinedAt, joining, leader, learner, leaving, leftDirty, log, marked, mine, moves, named, now, oldSet, opened, out, pending, promised, queued, readmit, registered, report, restarting, seen, seenInc, setGen, sid, splits, staged, staleCommit, stopped, successor, ver, wipedAt, wipes, writes, cpGone >>
      []          k = 42 ->
             /\ clock' = (a :> 9 @@ b :> 8 @@ c :> 7 @@ d :> 8)
             /\ now' = 8
             /\ UNCHANGED << ackGen, acked, answered, ballot, bexpiry, bgen, cacked, canswered, cconfirmed, checked, chwm, clog, confirmed, cpExpiry, cpLearner, cpNext, cpNextGen, cpView, cutOver, draining, expect, fenceIds, fencing, gaveUp, gen, halted, hbAt, hbOut, heard, hwm, inc, inflight, inherited, installed, joinedAt, joining, leader, learner, leaving, leftDirty, log, marked, mine, moves, named, oldSet, opened, out, pending, promised, queued, readmit, registered, report, restarting, seen, seenInc, setGen, sid, splits, staged, staleCommit, stopped, successor, ver, wipedAt, wipes, writes, cpGone >>
      []          k = 43 ->
             /\ ballot' = (a :> {a} @@ b :> {b} @@ c :> {} @@ d :> {})
             /\ bexpiry' = (a :> 7 @@ b :> 10 @@ c :> 0 @@ d :> 0)
             /\ bgen' = (a :> 11 @@ b :> 12 @@ c :> 0 @@ d :> 0)
             /\ cpExpiry' = 10
             /\ fencing' = (a :> TRUE @@ b :> TRUE @@ c :> FALSE @@ d :> FALSE)
             /\ gen' = 12
             /\ leader' = b
             /\ promised' = (a :> 11 @@ b :> 12 @@ c :> 0 @@ d :> 0)
             /\ ver' = 11
             /\ UNCHANGED << ackGen, acked, answered, cacked, canswered, cconfirmed, checked, chwm, clock, clog, confirmed, cpLearner, cpNext, cpNextGen, cpView, cutOver, draining, expect, fenceIds, gaveUp, halted, hbAt, hbOut, heard, hwm, inc, inflight, inherited, installed, joinedAt, joining, learner, leaving, leftDirty, log, marked, mine, moves, named, now, oldSet, opened, out, pending, queued, readmit, registered, report, restarting, seen, seenInc, setGen, sid, splits, staged, staleCommit, stopped, successor, wipedAt, wipes, writes, cpGone >>
      []          k = 44 ->
             /\ hbAt' = (a :> 9 @@ b :> 0 @@ c :> 0 @@ d :> 0)
             /\ hbOut' = (a :> TRUE @@ b :> FALSE @@ c :> FALSE @@ d :> FALSE)
             /\ UNCHANGED << ackGen, acked, answered, ballot, bexpiry, bgen, cacked, canswered, cconfirmed, checked, chwm, clock, clog, confirmed, cpExpiry, cpLearner, cpNext, cpNextGen, cpView, cutOver, draining, expect, fenceIds, fencing, gaveUp, gen, halted, heard, hwm, inc, inflight, inherited, installed, joinedAt, joining, leader, learner, leaving, leftDirty, log, marked, mine, moves, named, now, oldSet, opened, out, pending, promised, queued, readmit, registered, report, restarting, seen, seenInc, setGen, sid, splits, staged, staleCommit, stopped, successor, ver, wipedAt, wipes, writes, cpGone >>
      []          k = 45 ->
             /\ bgen' = (a :> 0 @@ b :> 12 @@ c :> 0 @@ d :> 0)
             /\ fencing' = (a :> FALSE @@ b :> TRUE @@ c :> FALSE @@ d :> FALSE)
             /\ UNCHANGED << ackGen, acked, answered, ballot, bexpiry, cacked, canswered, cconfirmed, checked, chwm, clock, clog, confirmed, cpExpiry, cpLearner, cpNext, cpNextGen, cpView, cutOver, draining, expect, fenceIds, gaveUp, gen, halted, hbAt, hbOut, heard, hwm, inc, inflight, inherited, installed, joinedAt, joining, leader, learner, leaving, leftDirty, log, marked, mine, moves, named, now, oldSet, opened, out, pending, promised, queued, readmit, registered, report, restarting, seen, seenInc, setGen, sid, splits, staged, staleCommit, stopped, successor, ver, wipedAt, wipes, writes, cpGone >>
      []          k = 46 ->
             /\ hbOut' = (a :> FALSE @@ b :> FALSE @@ c :> FALSE @@ d :> FALSE)
             /\ UNCHANGED << ackGen, acked, answered, ballot, bexpiry, bgen, cacked, canswered, cconfirmed, checked, chwm, clock, clog, confirmed, cpExpiry, cpLearner, cpNext, cpNextGen, cpView, cutOver, draining, expect, fenceIds, fencing, gaveUp, gen, halted, hbAt, heard, hwm, inc, inflight, inherited, installed, joinedAt, joining, leader, learner, leaving, leftDirty, log, marked, mine, moves, named, now, oldSet, opened, out, pending, promised, queued, readmit, registered, report, restarting, seen, seenInc, setGen, sid, splits, staged, staleCommit, stopped, successor, ver, wipedAt, wipes, writes, cpGone >>
      []          k = 47 ->
             /\ hbAt' = (a :> 9 @@ b :> 8 @@ c :> 0 @@ d :> 0)
             /\ hbOut' = (a :> FALSE @@ b :> TRUE @@ c :> FALSE @@ d :> FALSE)
             /\ UNCHANGED << ackGen, acked, answered, ballot, bexpiry, bgen, cacked, canswered, cconfirmed, checked, chwm, clock, clog, confirmed, cpExpiry, cpLearner, cpNext, cpNextGen, cpView, cutOver, draining, expect, fenceIds, fencing, gaveUp, gen, halted, heard, hwm, inc, inflight, inherited, installed, joinedAt, joining, leader, learner, leaving, leftDirty, log, marked, mine, moves, named, now, oldSet, opened, out, pending, promised, queued, readmit, registered, report, restarting, seen, seenInc, setGen, sid, splits, staged, staleCommit, stopped, successor, ver, wipedAt, wipes, writes, cpGone >>
      []          k = 48 ->
             /\ answered' = (a :> {b} @@ b :> {a} @@ c :> {} @@ d :> {})
             /\ ballot' = (a :> {b} @@ b :> {b} @@ c :> {} @@ d :> {})
             /\ log' = (a :> <<[g |-> 7, id |-> 0, lg |-> 7], [g |-> 7, id |-> 2, lg |-> 7]>> @@ b :> <<[g |-> 7, id |-> 0, lg |-> 7], [g |-> 7, id |-> 2, lg |-> 7]>> @@ c :> <<>> @@ d :> <<>>)
             /\ promised' = (a :> 12 @@ b :> 12 @@ c :> 0 @@ d :> 0)
             /\ UNCHANGED << ackGen, acked, bexpiry, bgen, cacked, canswered, cconfirmed, checked, chwm, clock, clog, confirmed, cpExpiry, cpLearner, cpNext, cpNextGen, cpView, cutOver, draining, expect, fenceIds, fencing, gaveUp, gen, halted, hbAt, hbOut, heard, hwm, inc, inflight, inherited, installed, joinedAt, joining, leader, learner, leaving, leftDirty, marked, mine, moves, named, now, oldSet, opened, out, pending, queued, readmit, registered, report, restarting, seen, seenInc, setGen, sid, splits, staged, staleCommit, stopped, successor, ver, wipedAt, wipes, writes, cpGone >>
      []          k = 49 ->
             /\ fencing' = (a :> FALSE @@ b :> FALSE @@ c :> FALSE @@ d :> FALSE)
             /\ log' = (a :> <<[g |-> 7, id |-> 0, lg |-> 7], [g |-> 7, id |-> 2, lg |-> 7]>> @@ b :> << [g |-> 7, id |-> 0, lg |-> 7], [g |-> 7, id |-> 2, lg |-> 7], [g |-> 12, id |-> 0, lg |-> 12] >> @@ c :> <<>> @@ d :> <<>>)
             /\ opened' = {<<1, a>>, <<7, a>>, <<12, b>>}
             /\ UNCHANGED << ackGen, acked, answered, ballot, bexpiry, bgen, cacked, canswered, cconfirmed, checked, chwm, clock, clog, confirmed, cpExpiry, cpLearner, cpNext, cpNextGen, cpView, cutOver, draining, expect, fenceIds, gaveUp, gen, halted, hbAt, hbOut, heard, hwm, inc, inflight, inherited, installed, joinedAt, joining, leader, learner, leaving, leftDirty, marked, mine, moves, named, now, oldSet, out, pending, promised, queued, readmit, registered, report, restarting, seen, seenInc, setGen, sid, splits, staged, staleCommit, stopped, successor, ver, wipedAt, wipes, writes, cpGone >>
      []          k = 50 ->
             /\ bgen' = (a :> 0 @@ b :> 13 @@ c :> 0 @@ d :> 0)
             /\ gen' = 13
             /\ joinedAt' = 2
             /\ joining' = {c}
             /\ log' = (a :> <<[g |-> 7, id |-> 0, lg |-> 7], [g |-> 7, id |-> 2, lg |-> 7]>> @@ b :> << [g |-> 7, id |-> 0, lg |-> 7], [g |-> 7, id |-> 2, lg |-> 7], [g |-> 12, id |-> 0, lg |-> 12], [g |-> 13, id |-> 0, lg |-> 13] >> @@ c :> <<>> @@ d :> <<>>)
             /\ mine' = (a :> {a, b} @@ b :> {a, b, c} @@ c :> {a, b} @@ d :> {a, b})
             /\ moves' = 1
             /\ opened' = {<<1, a>>, <<7, a>>, <<12, b>>, <<13, b>>}
             /\ out' = {d}
             /\ promised' = (a :> 12 @@ b :> 13 @@ c :> 0 @@ d :> 0)
             /\ ver' = 12
             /\ UNCHANGED << ackGen, acked, answered, ballot, bexpiry, cacked, canswered, cconfirmed, checked, chwm, clock, clog, confirmed, cpExpiry, cpLearner, cpNext, cpNextGen, cpView, cutOver, draining, expect, fenceIds, fencing, gaveUp, halted, hbAt, hbOut, heard, hwm, inc, inflight, inherited, installed, leader, learner, leaving, leftDirty, marked, named, now, oldSet, pending, queued, readmit, registered, report, restarting, seen, seenInc, setGen, sid, splits, staged, staleCommit, stopped, successor, wipedAt, wipes, writes, cpGone >>
      []          k = 51 ->
             /\ confirmed' = (a :> (a :> 0 @@ b :> 0 @@ c :> 0 @@ d :> 0) @@ b :> (a :> 3 @@ b :> 0 @@ c :> 0 @@ d :> 0) @@ c :> (a :> 0 @@ b :> 0 @@ c :> 0 @@ d :> 0) @@ d :> (a :> 0 @@ b :> 0 @@ c :> 0 @@ d :> 0))
             /\ log' = (a :> << [g |-> 7, id |-> 0, lg |-> 7], [g |-> 7, id |-> 2, lg |-> 7], [g |-> 12, id |-> 0, lg |-> 12] >> @@ b :> << [g |-> 7, id |-> 0, lg |-> 7], [g |-> 7, id |-> 2, lg |-> 7], [g |-> 12, id |-> 0, lg |-> 12], [g |-> 13, id |-> 0, lg |-> 13] >> @@ c :> <<>> @@ d :> <<>>)
             /\ promised' = (a :> 13 @@ b :> 13 @@ c :> 0 @@ d :> 0)
             /\ UNCHANGED << ackGen, acked, answered, ballot, bexpiry, bgen, cacked, canswered, cconfirmed, checked, chwm, clock, clog, cpExpiry, cpLearner, cpNext, cpNextGen, cpView, cutOver, draining, expect, fenceIds, fencing, gaveUp, gen, halted, hbAt, hbOut, heard, hwm, inc, inflight, inherited, installed, joinedAt, joining, leader, learner, leaving, leftDirty, marked, mine, moves, named, now, oldSet, opened, out, pending, queued, readmit, registered, report, restarting, seen, seenInc, setGen, sid, splits, staged, staleCommit, stopped, successor, ver, wipedAt, wipes, writes, cpGone >>
      []          k = 52 ->
             /\ confirmed' = (a :> (a :> 0 @@ b :> 0 @@ c :> 0 @@ d :> 0) @@ b :> (a :> 4 @@ b :> 0 @@ c :> 0 @@ d :> 0) @@ c :> (a :> 0 @@ b :> 0 @@ c :> 0 @@ d :> 0) @@ d :> (a :> 0 @@ b :> 0 @@ c :> 0 @@ d :> 0))
             /\ log' = (a :> << [g |-> 7, id |-> 0, lg |-> 7], [g |-> 7, id |-> 2, lg |-> 7], [g |-> 12, id |-> 0, lg |-> 12], [g |-> 13, id |-> 0, lg |-> 13] >> @@ b :> << [g |-> 7, id |-> 0, lg |-> 7], [g |-> 7, id |-> 2, lg |-> 7], [g |-> 12, id |-> 0, lg |-> 12], [g |-> 13, id |-> 0, lg |-> 13] >> @@ c :> <<>> @@ d :> <<>>)
             /\ UNCHANGED << ackGen, acked, answered, ballot, bexpiry, bgen, cacked, canswered, cconfirmed, checked, chwm, clock, clog, cpExpiry, cpLearner, cpNext, cpNextGen, cpView, cutOver, draining, expect, fenceIds, fencing, gaveUp, gen, halted, hbAt, hbOut, heard, hwm, inc, inflight, inherited, installed, joinedAt, joining, leader, learner, leaving, leftDirty, marked, mine, moves, named, now, oldSet, opened, out, pending, promised, queued, readmit, registered, report, restarting, seen, seenInc, setGen, sid, splits, staged, staleCommit, stopped, successor, ver, wipedAt, wipes, writes, cpGone >>
      []          k = 53 ->
             /\ ballot' = (a :> {b} @@ b :> {b} @@ c :> {b} @@ d :> {})
             /\ confirmed' = (a :> (a :> 0 @@ b :> 0 @@ c :> 0 @@ d :> 0) @@ b :> (a :> 4 @@ b :> 0 @@ c :> 1 @@ d :> 0) @@ c :> (a :> 0 @@ b :> 0 @@ c :> 0 @@ d :> 0) @@ d :> (a :> 0 @@ b :> 0 @@ c :> 0 @@ d :> 0))
             /\ log' = (a :> << [g |-> 7, id |-> 0, lg |-> 7], [g |-> 7, id |-> 2, lg |-> 7], [g |-> 12, id |-> 0, lg |-> 12], [g |-> 13, id |-> 0, lg |-> 13] >> @@ b :> << [g |-> 7, id |-> 0, lg |-> 7], [g |-> 7, id |-> 2, lg |-> 7], [g |-> 12, id |-> 0, lg |-> 12], [g |-> 13, id |-> 0, lg |-> 13] >> @@ c :> <<[g |-> 7, id |-> 0, lg |-> 7]>> @@ d :> <<>>)
             /\ promised' = (a :> 13 @@ b :> 13 @@ c :> 13 @@ d :> 0)
             /\ UNCHANGED << ackGen, acked, answered, bexpiry, bgen, cacked, canswered, cconfirmed, checked, chwm, clock, clog, cpExpiry, cpLearner, cpNext, cpNextGen, cpView, cutOver, draining, expect, fenceIds, fencing, gaveUp, gen, halted, hbAt, hbOut, heard, hwm, inc, inflight, inherited, installed, joinedAt, joining, leader, learner, leaving, leftDirty, marked, mine, moves, named, now, oldSet, opened, out, pending, queued, readmit, registered, report, restarting, seen, seenInc, setGen, sid, splits, staged, staleCommit, stopped, successor, ver, wipedAt, wipes, writes, cpGone >>
      []          k = 54 ->
             /\ clock' = (a :> 9 @@ b :> 8 @@ c :> 8 @@ d :> 8)
             /\ now' = 9
             /\ UNCHANGED << ackGen, acked, answered, ballot, bexpiry, bgen, cacked, canswered, cconfirmed, checked, chwm, clog, confirmed, cpExpiry, cpLearner, cpNext, cpNextGen, cpView, cutOver, draining, expect, fenceIds, fencing, gaveUp, gen, halted, hbAt, hbOut, heard, hwm, inc, inflight, inherited, installed, joinedAt, joining, leader, learner, leaving, leftDirty, log, marked, mine, moves, named, oldSet, opened, out, pending, promised, queued, readmit, registered, report, restarting, seen, seenInc, setGen, sid, splits, staged, staleCommit, stopped, successor, ver, wipedAt, wipes, writes, cpGone >>
      []          k = 55 ->
             /\ ackGen' = {<<2, 13>>}
             /\ acked' = {2}
             /\ hwm' = (a :> 0 @@ b :> 4 @@ c :> 0 @@ d :> 0)
             /\ UNCHANGED << answered, ballot, bexpiry, bgen, cacked, canswered, cconfirmed, checked, chwm, clock, clog, confirmed, cpExpiry, cpLearner, cpNext, cpNextGen, cpView, cutOver, draining, expect, fenceIds, fencing, gaveUp, gen, halted, hbAt, hbOut, heard, inc, inflight, inherited, installed, joinedAt, joining, leader, learner, leaving, leftDirty, log, marked, mine, moves, named, now, oldSet, opened, out, pending, promised, queued, readmit, registered, report, restarting, seen, seenInc, setGen, sid, splits, staged, staleCommit, stopped, successor, ver, wipedAt, wipes, writes, cpGone >>
      []          k = 56 ->
             /\ bgen' = (a :> 0 @@ b :> 14 @@ c :> 0 @@ d :> 0)
             /\ confirmed' = (a :> (a :> 0 @@ b :> 0 @@ c :> 0 @@ d :> 0) @@ b :> (a :> 0 @@ b :> 0 @@ c :> 0 @@ d :> 0) @@ c :> (a :> 0 @@ b :> 0 @@ c :> 0 @@ d :> 0) @@ d :> (a :> 0 @@ b :> 0 @@ c :> 0 @@ d :> 0))
             /\ gen' = 14
             /\ joinedAt' = 0
             /\ joining' = {}
             /\ log' = (a :> << [g |-> 7, id |-> 0, lg |-> 7], [g |-> 7, id |-> 2, lg |-> 7], [g |-> 12, id |-> 0, lg |-> 12], [g |-> 13, id |-> 0, lg |-> 13] >> @@ b :> << [g |-> 7, id |-> 0, lg |-> 7], [g |-> 7, id |-> 2, lg |-> 7], [g |-> 12, id |-> 0, lg |-> 12], [g |-> 13, id |-> 0, lg |-> 13], [g |-> 14, id |-> 0, lg |-> 14] >> @@ c :> <<[g |-> 7, id |-> 0, lg |-> 7]>> @@ d :> <<>>)
             /\ opened' = {<<1, a>>, <<7, a>>, <<12, b>>, <<13, b>>, <<14, b>>}
             /\ promised' = (a :> 13 @@ b :> 14 @@ c :> 13 @@ d :> 0)
             /\ ver' = 13
             /\ UNCHANGED << ackGen, acked, answered, ballot, bexpiry, cacked, canswered, cconfirmed, checked, chwm, clock, clog, cpExpiry, cpLearner, cpNext, cpNextGen, cpView, cutOver, draining, expect, fenceIds, fencing, gaveUp, halted, hbAt, hbOut, heard, hwm, inc, inflight, inherited, installed, leader, learner, leaving, leftDirty, marked, mine, moves, named, now, oldSet, out, pending, queued, readmit, registered, report, restarting, seen, seenInc, setGen, sid, splits, staged, staleCommit, stopped, successor, wipedAt, wipes, writes, cpGone >>
      []          k = 57 ->
             /\ queued' = (a :> 0 @@ b :> 4 @@ c :> 0 @@ d :> 0)
             /\ writes' = 4
             /\ UNCHANGED << ackGen, acked, answered, ballot, bexpiry, bgen, cacked, canswered, cconfirmed, checked, chwm, clock, clog, confirmed, cpExpiry, cpLearner, cpNext, cpNextGen, cpView, cutOver, draining, expect, fenceIds, fencing, gaveUp, gen, halted, hbAt, hbOut, heard, hwm, inc, inflight, inherited, installed, joinedAt, joining, leader, learner, leaving, leftDirty, log, marked, mine, moves, named, now, oldSet, opened, out, pending, promised, readmit, registered, report, restarting, seen, seenInc, setGen, sid, splits, staged, staleCommit, stopped, successor, ver, wipedAt, wipes, cpGone >>
      []          k = 58 ->
             /\ hbOut' = (a :> FALSE @@ b :> FALSE @@ c :> FALSE @@ d :> FALSE)
             /\ UNCHANGED << ackGen, acked, answered, ballot, bexpiry, bgen, cacked, canswered, cconfirmed, checked, chwm, clock, clog, confirmed, cpExpiry, cpLearner, cpNext, cpNextGen, cpView, cutOver, draining, expect, fenceIds, fencing, gaveUp, gen, halted, hbAt, heard, hwm, inc, inflight, inherited, installed, joinedAt, joining, leader, learner, leaving, leftDirty, log, marked, mine, moves, named, now, oldSet, opened, out, pending, promised, queued, readmit, registered, report, restarting, seen, seenInc, setGen, sid, splits, staged, staleCommit, stopped, successor, ver, wipedAt, wipes, writes, cpGone >>
      []          k = 59 ->
             /\ hbOut' = (a :> FALSE @@ b :> TRUE @@ c :> FALSE @@ d :> FALSE)
             /\ UNCHANGED << ackGen, acked, answered, ballot, bexpiry, bgen, cacked, canswered, cconfirmed, checked, chwm, clock, clog, confirmed, cpExpiry, cpLearner, cpNext, cpNextGen, cpView, cutOver, draining, expect, fenceIds, fencing, gaveUp, gen, halted, hbAt, heard, hwm, inc, inflight, inherited, installed, joinedAt, joining, leader, learner, leaving, leftDirty, log, marked, mine, moves, named, now, oldSet, opened, out, pending, promised, queued, readmit, registered, report, restarting, seen, seenInc, setGen, sid, splits, staged, staleCommit, stopped, successor, ver, wipedAt, wipes, writes, cpGone >>
      []          k = 60 ->
             /\ cpExpiry' = 11
             /\ hbOut' = (a :> FALSE @@ b :> FALSE @@ c :> FALSE @@ d :> FALSE)
             /\ UNCHANGED << ackGen, acked, answered, ballot, bexpiry, bgen, cacked, canswered, cconfirmed, checked, chwm, clock, clog, confirmed, cpLearner, cpNext, cpNextGen, cpView, cutOver, draining, expect, fenceIds, fencing, gaveUp, gen, halted, hbAt, heard, hwm, inc, inflight, inherited, installed, joinedAt, joining, leader, learner, leaving, leftDirty, log, marked, mine, moves, named, now, oldSet, opened, out, pending, promised, queued, readmit, registered, report, restarting, seen, seenInc, setGen, sid, splits, staged, staleCommit, stopped, successor, ver, wipedAt, wipes, writes, cpGone >>
      []          k = 61 ->
             /\ hwm' = (a :> 4 @@ b :> 4 @@ c :> 0 @@ d :> 0)
             /\ UNCHANGED << ackGen, acked, answered, ballot, bexpiry, bgen, cacked, canswered, cconfirmed, checked, chwm, clock, clog, confirmed, cpExpiry, cpLearner, cpNext, cpNextGen, cpView, cutOver, draining, expect, fenceIds, fencing, gaveUp, gen, halted, hbAt, hbOut, heard, inc, inflight, inherited, installed, joinedAt, joining, leader, learner, leaving, leftDirty, log, marked, mine, moves, named, now, oldSet, opened, out, pending, promised, queued, readmit, registered, report, restarting, seen, seenInc, setGen, sid, splits, staged, staleCommit, stopped, successor, ver, wipedAt, wipes, writes, cpGone >>
      []          k = 62 ->
             /\ pending' = (a :> 0 @@ b :> 4 @@ c :> 0 @@ d :> 0)
             /\ queued' = (a :> 0 @@ b :> 0 @@ c :> 0 @@ d :> 0)
             /\ UNCHANGED << ackGen, acked, answered, ballot, bexpiry, bgen, cacked, canswered, cconfirmed, checked, chwm, clock, clog, confirmed, cpExpiry, cpLearner, cpNext, cpNextGen, cpView, cutOver, draining, expect, fenceIds, fencing, gaveUp, gen, halted, hbAt, hbOut, heard, hwm, inc, inflight, inherited, installed, joinedAt, joining, leader, learner, leaving, leftDirty, log, marked, mine, moves, named, now, oldSet, opened, out, promised, readmit, registered, report, restarting, seen, seenInc, setGen, sid, splits, staged, staleCommit, stopped, successor, ver, wipedAt, wipes, writes, cpGone >>
      []          k = 63 ->
             /\ bgen' = (a :> 0 @@ b :> 15 @@ c :> 0 @@ d :> 0)
             /\ gen' = 15
             /\ joinedAt' = 4
             /\ joining' = {d}
             /\ leaving' = {a}
             /\ log' = (a :> << [g |-> 7, id |-> 0, lg |-> 7], [g |-> 7, id |-> 2, lg |-> 7], [g |-> 12, id |-> 0, lg |-> 12], [g |-> 13, id |-> 0, lg |-> 13] >> @@ b :> << [g |-> 7, id |-> 0, lg |-> 7], [g |-> 7, id |-> 2, lg |-> 7], [g |-> 12, id |-> 0, lg |-> 12], [g |-> 13, id |-> 0, lg |-> 13], [g |-> 14, id |-> 0, lg |-> 14], [g |-> 15, id |-> 0, lg |-> 15] >> @@ c :> <<[g |-> 7, id |-> 0, lg |-> 7]>> @@ d :> <<>>)
             /\ mine' = (a :> {a, b} @@ b :> {a, b, c, d} @@ c :> {a, b} @@ d :> {a, b})
             /\ moves' = 2
             /\ opened' = {<<1, a>>, <<7, a>>, <<12, b>>, <<13, b>>, <<14, b>>, <<15, b>>}
             /\ out' = {}
             /\ promised' = (a :> 13 @@ b :> 15 @@ c :> 13 @@ d :> 0)
             /\ ver' = 14
             /\ UNCHANGED << ackGen, acked, answered, ballot, bexpiry, cacked, canswered, cconfirmed, checked, chwm, clock, clog, confirmed, cpExpiry, cpLearner, cpNext, cpNextGen, cpView, cutOver, draining, expect, fenceIds, fencing, gaveUp, halted, hbAt, hbOut, heard, hwm, inc, inflight, inherited, installed, leader, learner, leftDirty, marked, named, now, oldSet, pending, queued, readmit, registered, report, restarting, seen, seenInc, setGen, sid, splits, staged, staleCommit, stopped, successor, wipedAt, wipes, writes, cpGone >>
      []          k = 64 ->
             /\ confirmed' = (a :> (a :> 0 @@ b :> 0 @@ c :> 0 @@ d :> 0) @@ b :> (a :> 5 @@ b :> 0 @@ c :> 0 @@ d :> 0) @@ c :> (a :> 0 @@ b :> 0 @@ c :> 0 @@ d :> 0) @@ d :> (a :> 0 @@ b :> 0 @@ c :> 0 @@ d :> 0))
             /\ log' = (a :> << [g |-> 7, id |-> 0, lg |-> 7], [g |-> 7, id |-> 2, lg |-> 7], [g |-> 12, id |-> 0, lg |-> 12], [g |-> 13, id |-> 0, lg |-> 13], [g |-> 14, id |-> 0, lg |-> 14] >> @@ b :> << [g |-> 7, id |-> 0, lg |-> 7], [g |-> 7, id |-> 2, lg |-> 7], [g |-> 12, id |-> 0, lg |-> 12], [g |-> 13, id |-> 0, lg |-> 13], [g |-> 14, id |-> 0, lg |-> 14], [g |-> 15, id |-> 0, lg |-> 15] >> @@ c :> <<[g |-> 7, id |-> 0, lg |-> 7]>> @@ d :> <<>>)
             /\ promised' = (a :> 15 @@ b :> 15 @@ c :> 13 @@ d :> 0)
             /\ UNCHANGED << ackGen, acked, answered, ballot, bexpiry, bgen, cacked, canswered, cconfirmed, checked, chwm, clock, clog, cpExpiry, cpLearner, cpNext, cpNextGen, cpView, cutOver, draining, expect, fenceIds, fencing, gaveUp, gen, halted, hbAt, hbOut, heard, hwm, inc, inflight, inherited, installed, joinedAt, joining, leader, learner, leaving, leftDirty, marked, mine, moves, named, now, oldSet, opened, out, pending, queued, readmit, registered, report, restarting, seen, seenInc, setGen, sid, splits, staged, staleCommit, stopped, successor, ver, wipedAt, wipes, writes, cpGone >>
      []          k = 65 ->
             /\ bgen' = (a :> 0 @@ b :> 16 @@ c :> 0 @@ d :> 0)
             /\ confirmed' = (a :> (a :> 0 @@ b :> 0 @@ c :> 0 @@ d :> 0) @@ b :> (a :> 0 @@ b :> 0 @@ c :> 0 @@ d :> 0) @@ c :> (a :> 0 @@ b :> 0 @@ c :> 0 @@ d :> 0) @@ d :> (a :> 0 @@ b :> 0 @@ c :> 0 @@ d :> 0))
             /\ gen' = 16
             /\ joinedAt' = 0
             /\ joining' = {}
             /\ log' = (a :> << [g |-> 7, id |-> 0, lg |-> 7], [g |-> 7, id |-> 2, lg |-> 7], [g |-> 12, id |-> 0, lg |-> 12], [g |-> 13, id |-> 0, lg |-> 13], [g |-> 14, id |-> 0, lg |-> 14] >> @@ b :> << [g |-> 7, id |-> 0, lg |-> 7], [g |-> 7, id |-> 2, lg |-> 7], [g |-> 12, id |-> 0, lg |-> 12], [g |-> 13, id |-> 0, lg |-> 13], [g |-> 14, id |-> 0, lg |-> 14], [g |-> 15, id |-> 0, lg |-> 15], [g |-> 16, id |-> 0, lg |-> 16] >> @@ c :> <<[g |-> 7, id |-> 0, lg |-> 7]>> @@ d :> <<>>)
             /\ mine' = (a :> {a, b} @@ b :> {b, c, d} @@ c :> {a, b} @@ d :> {a, b})
             /\ opened' = {<<1, a>>, <<7, a>>, <<12, b>>, <<13, b>>, <<14, b>>, <<15, b>>, <<16, b>>}
             /\ out' = {a}
             /\ promised' = (a :> 15 @@ b :> 16 @@ c :> 13 @@ d :> 0)
             /\ ver' = 15
             /\ UNCHANGED << ackGen, acked, answered, ballot, bexpiry, cacked, canswered, cconfirmed, checked, chwm, clock, clog, cpExpiry, cpLearner, cpNext, cpNextGen, cpView, cutOver, draining, expect, fenceIds, fencing, gaveUp, halted, hbAt, hbOut, heard, hwm, inc, inflight, inherited, installed, leader, learner, leaving, leftDirty, marked, moves, named, now, oldSet, pending, queued, readmit, registered, report, restarting, seen, seenInc, setGen, sid, splits, staged, staleCommit, stopped, successor, wipedAt, wipes, writes, cpGone >>
      []          k = 66 ->
             /\ clock' = (a :> 9 @@ b :> 10 @@ c :> 9 @@ d :> 9)
             /\ now' = 10
             /\ UNCHANGED << ackGen, acked, answered, ballot, bexpiry, bgen, cacked, canswered, cconfirmed, checked, chwm, clog, confirmed, cpExpiry, cpLearner, cpNext, cpNextGen, cpView, cutOver, draining, expect, fenceIds, fencing, gaveUp, gen, halted, hbAt, hbOut, heard, hwm, inc, inflight, inherited, installed, joinedAt, joining, leader, learner, leaving, leftDirty, log, marked, mine, moves, named, oldSet, opened, out, pending, promised, queued, readmit, registered, report, restarting, seen, seenInc, setGen, sid, splits, staged, staleCommit, stopped, successor, ver, wipedAt, wipes, writes, cpGone >>
      []          k = 67 ->
             /\ bgen' = (a :> 0 @@ b :> 0 @@ c :> 0 @@ d :> 0)
             /\ pending' = (a :> 0 @@ b :> 0 @@ c :> 0 @@ d :> 0)
             /\ UNCHANGED << ackGen, acked, answered, ballot, bexpiry, cacked, canswered, cconfirmed, checked, chwm, clock, clog, confirmed, cpExpiry, cpLearner, cpNext, cpNextGen, cpView, cutOver, draining, expect, fenceIds, fencing, gaveUp, gen, halted, hbAt, hbOut, heard, hwm, inc, inflight, inherited, installed, joinedAt, joining, leader, learner, leaving, leftDirty, log, marked, mine, moves, named, now, oldSet, opened, out, promised, queued, readmit, registered, report, restarting, seen, seenInc, setGen, sid, splits, staged, staleCommit, stopped, successor, ver, wipedAt, wipes, writes, cpGone >>
      []          k = 68 ->
             /\ clock' = (a :> 11 @@ b :> 12 @@ c :> 11 @@ d :> 11)
             /\ now' = 11
             /\ UNCHANGED << ackGen, acked, answered, ballot, bexpiry, bgen, cacked, canswered, cconfirmed, checked, chwm, clog, confirmed, cpExpiry, cpLearner, cpNext, cpNextGen, cpView, cutOver, draining, expect, fenceIds, fencing, gaveUp, gen, halted, hbAt, hbOut, heard, hwm, inc, inflight, inherited, installed, joinedAt, joining, leader, learner, leaving, leftDirty, log, marked, mine, moves, named, oldSet, opened, out, pending, promised, queued, readmit, registered, report, restarting, seen, seenInc, setGen, sid, splits, staged, staleCommit, stopped, successor, ver, wipedAt, wipes, writes, cpGone >>
      []          k = 69 ->
             /\ ballot' = (a :> {b} @@ b :> {b} @@ c :> {b} @@ d :> {d})
             /\ bexpiry' = (a :> 7 @@ b :> 10 @@ c :> 0 @@ d :> 13)
             /\ bgen' = (a :> 0 @@ b :> 0 @@ c :> 0 @@ d :> 17)
             /\ cpExpiry' = 13
             /\ fencing' = (a :> FALSE @@ b :> FALSE @@ c :> FALSE @@ d :> TRUE)
             /\ gen' = 17
             /\ leader' = d
             /\ leaving' = {}
             /\ mine' = (a :> {a, b} @@ b :> {b, c, d} @@ c :> {a, b} @@ d :> {b, c, d})
             /\ promised' = (a :> 15 @@ b :> 16 @@ c :> 13 @@ d :> 17)
             /\ ver' = 16
             /\ UNCHANGED << ackGen, acked, answered, cacked, canswered, cconfirmed, checked, chwm, clock, clog, confirmed, cpLearner, cpNext, cpNextGen, cpView, cutOver, draining, expect, fenceIds, gaveUp, halted, hbAt, hbOut, heard, hwm, inc, inflight, inherited, installed, joinedAt, joining, learner, leftDirty, log, marked, moves, named, now, oldSet, opened, out, pending, queued, readmit, registered, report, restarting, seen, seenInc, setGen, sid, splits, staged, staleCommit, stopped, successor, wipedAt, wipes, writes, cpGone >>
      []          k = 70 ->
             /\ clock' = (a :> 11 @@ b :> 13 @@ c :> 11 @@ d :> 13)
             /\ now' = 12
             /\ UNCHANGED << ackGen, acked, answered, ballot, bexpiry, bgen, cacked, canswered, cconfirmed, checked, chwm, clog, confirmed, cpExpiry, cpLearner, cpNext, cpNextGen, cpView, cutOver, draining, expect, fenceIds, fencing, gaveUp, gen, halted, hbAt, hbOut, heard, hwm, inc, inflight, inherited, installed, joinedAt, joining, leader, learner, leaving, leftDirty, log, marked, mine, moves, named, oldSet, opened, out, pending, promised, queued, readmit, registered, report, restarting, seen, seenInc, setGen, sid, splits, staged, staleCommit, stopped, successor, ver, wipedAt, wipes, writes, cpGone >>
      []          k = 71 ->
             /\ answered' = (a :> {b} @@ b :> {a} @@ c :> {} @@ d :> {c})
             /\ ballot' = (a :> {b} @@ b :> {b} @@ c :> {d} @@ d :> {d})
             /\ log' = (a :> << [g |-> 7, id |-> 0, lg |-> 7], [g |-> 7, id |-> 2, lg |-> 7], [g |-> 12, id |-> 0, lg |-> 12], [g |-> 13, id |-> 0, lg |-> 13], [g |-> 14, id |-> 0, lg |-> 14] >> @@ b :> << [g |-> 7, id |-> 0, lg |-> 7], [g |-> 7, id |-> 2, lg |-> 7], [g |-> 12, id |-> 0, lg |-> 12], [g |-> 13, id |-> 0, lg |-> 13], [g |-> 14, id |-> 0, lg |-> 14], [g |-> 15, id |-> 0, lg |-> 15], [g |-> 16, id |-> 0, lg |-> 16] >> @@ c :> <<[g |-> 7, id |-> 0, lg |-> 7]>> @@ d :> <<[g |-> 7, id |-> 0, lg |-> 7]>>)
             /\ promised' = (a :> 15 @@ b :> 16 @@ c :> 17 @@ d :> 17)
             /\ UNCHANGED << ackGen, acked, bexpiry, bgen, cacked, canswered, cconfirmed, checked, chwm, clock, clog, confirmed, cpExpiry, cpLearner, cpNext, cpNextGen, cpView, cutOver, draining, expect, fenceIds, fencing, gaveUp, gen, halted, hbAt, hbOut, heard, hwm, inc, inflight, inherited, installed, joinedAt, joining, leader, learner, leaving, leftDirty, marked, mine, moves, named, now, oldSet, opened, out, pending, queued, readmit, registered, report, restarting, seen, seenInc, setGen, sid, splits, staged, staleCommit, stopped, successor, ver, wipedAt, wipes, writes, cpGone >>
      []          k = 72 ->
             /\ bgen' = (a :> 0 @@ b :> 0 @@ c :> 0 @@ d :> 0)
             /\ fencing' = (a :> FALSE @@ b :> FALSE @@ c :> FALSE @@ d :> FALSE)
             /\ UNCHANGED << ackGen, acked, answered, ballot, bexpiry, cacked, canswered, cconfirmed, checked, chwm, clock, clog, confirmed, cpExpiry, cpLearner, cpNext, cpNextGen, cpView, cutOver, draining, expect, fenceIds, gaveUp, gen, halted, hbAt, hbOut, heard, hwm, inc, inflight, inherited, installed, joinedAt, joining, leader, learner, leaving, leftDirty, log, marked, mine, moves, named, now, oldSet, opened, out, pending, promised, queued, readmit, registered, report, restarting, seen, seenInc, setGen, sid, splits, staged, staleCommit, stopped, successor, ver, wipedAt, wipes, writes, cpGone >>
      []          k = 73 ->
             /\ clock' = (a :> 12 @@ b :> 14 @@ c :> 13 @@ d :> 13)
             /\ now' = 13
             /\ UNCHANGED << ackGen, acked, answered, ballot, bexpiry, bgen, cacked, canswered, cconfirmed, checked, chwm, clog, confirmed, cpExpiry, cpLearner, cpNext, cpNextGen, cpView, cutOver, draining, expect, fenceIds, fencing, gaveUp, gen, halted, hbAt, hbOut, heard, hwm, inc, inflight, inherited, installed, joinedAt, joining, leader, learner, leaving, leftDirty, log, marked, mine, moves, named, oldSet, opened, out, pending, promised, queued, readmit, registered, report, restarting, seen, seenInc, setGen, sid, splits, staged, staleCommit, stopped, successor, ver, wipedAt, wipes, writes, cpGone >>
      []          k = 74 ->
             /\ ballot' = (a :> {b} @@ b :> {b} @@ c :> {c} @@ d :> {d})
             /\ bexpiry' = (a :> 7 @@ b :> 10 @@ c :> 15 @@ d :> 13)
             /\ bgen' = (a :> 0 @@ b :> 0 @@ c :> 18 @@ d :> 0)
             /\ cpExpiry' = 15
             /\ fencing' = (a :> FALSE @@ b :> FALSE @@ c :> TRUE @@ d :> FALSE)
             /\ gen' = 18
             /\ leader' = c
             /\ mine' = (a :> {a, b} @@ b :> {b, c, d} @@ c :> {b, c, d} @@ d :> {b, c, d})
             /\ promised' = (a :> 15 @@ b :> 16 @@ c :> 18 @@ d :> 17)
             /\ ver' = 17
             /\ UNCHANGED << ackGen, acked, answered, cacked, canswered, cconfirmed, checked, chwm, clock, clog, confirmed, cpLearner, cpNext, cpNextGen, cpView, cutOver, draining, expect, fenceIds, gaveUp, halted, hbAt, hbOut, heard, hwm, inc, inflight, inherited, installed, joinedAt, joining, learner, leaving, leftDirty, log, marked, moves, named, now, oldSet, opened, out, pending, queued, readmit, registered, report, restarting, seen, seenInc, setGen, sid, splits, staged, staleCommit, stopped, successor, wipedAt, wipes, writes, cpGone >>
      []          k = 75 ->
             /\ clock' = (a :> 14 @@ b :> 14 @@ c :> 14 @@ d :> 13)
             /\ now' = 14
             /\ UNCHANGED << ackGen, acked, answered, ballot, bexpiry, bgen, cacked, canswered, cconfirmed, checked, chwm, clog, confirmed, cpExpiry, cpLearner, cpNext, cpNextGen, cpView, cutOver, draining, expect, fenceIds, fencing, gaveUp, gen, halted, hbAt, hbOut, heard, hwm, inc, inflight, inherited, installed, joinedAt, joining, leader, learner, leaving, leftDirty, log, marked, mine, moves, named, oldSet, opened, out, pending, promised, queued, readmit, registered, report, restarting, seen, seenInc, setGen, sid, splits, staged, staleCommit, stopped, successor, ver, wipedAt, wipes, writes, cpGone >>
      []          k = 76 ->
             /\ hbAt' = (a :> 9 @@ b :> 8 @@ c :> 14 @@ d :> 0)
             /\ hbOut' = (a :> FALSE @@ b :> FALSE @@ c :> TRUE @@ d :> FALSE)
             /\ UNCHANGED << ackGen, acked, answered, ballot, bexpiry, bgen, cacked, canswered, cconfirmed, checked, chwm, clock, clog, confirmed, cpExpiry, cpLearner, cpNext, cpNextGen, cpView, cutOver, draining, expect, fenceIds, fencing, gaveUp, gen, halted, heard, hwm, inc, inflight, inherited, installed, joinedAt, joining, leader, learner, leaving, leftDirty, log, marked, mine, moves, named, now, oldSet, opened, out, pending, promised, queued, readmit, registered, report, restarting, seen, seenInc, setGen, sid, splits, staged, staleCommit, stopped, successor, ver, wipedAt, wipes, writes, cpGone >>
      []          k = 77 ->
             /\ clock' = (a :> 16 @@ b :> 14 @@ c :> 15 @@ d :> 14)
             /\ now' = 15
             /\ UNCHANGED << ackGen, acked, answered, ballot, bexpiry, bgen, cacked, canswered, cconfirmed, checked, chwm, clog, confirmed, cpExpiry, cpLearner, cpNext, cpNextGen, cpView, cutOver, draining, expect, fenceIds, fencing, gaveUp, gen, halted, hbAt, hbOut, heard, hwm, inc, inflight, inherited, installed, joinedAt, joining, leader, learner, leaving, leftDirty, log, marked, mine, moves, named, oldSet, opened, out, pending, promised, queued, readmit, registered, report, restarting, seen, seenInc, setGen, sid, splits, staged, staleCommit, stopped, successor, ver, wipedAt, wipes, writes, cpGone >>
      []          k = 78 ->
             /\ answered' = (a :> {b} @@ b :> {a} @@ c :> {d} @@ d :> {c})
             /\ ballot' = (a :> {b} @@ b :> {b} @@ c :> {c} @@ d :> {c})
             /\ promised' = (a :> 15 @@ b :> 16 @@ c :> 18 @@ d :> 18)
             /\ UNCHANGED << ackGen, acked, bexpiry, bgen, cacked, canswered, cconfirmed, checked, chwm, clock, clog, confirmed, cpExpiry, cpLearner, cpNext, cpNextGen, cpView, cutOver, draining, expect, fenceIds, fencing, gaveUp, gen, halted, hbAt, hbOut, heard, hwm, inc, inflight, inherited, installed, joinedAt, joining, leader, learner, leaving, leftDirty, log, marked, mine, moves, named, now, oldSet, opened, out, pending, queued, readmit, registered, report, restarting, seen, seenInc, setGen, sid, splits, staged, staleCommit, stopped, successor, ver, wipedAt, wipes, writes, cpGone >>
      []          k = 79 ->
             /\ answered' = (a :> {b} @@ b :> {} @@ c :> {d} @@ d :> {c})
             /\ bexpiry' = (a :> 7 @@ b :> 16 @@ c :> 15 @@ d :> 13)
             /\ bgen' = (a :> 0 @@ b :> 19 @@ c :> 18 @@ d :> 0)
             /\ cpExpiry' = 17
             /\ fencing' = (a :> FALSE @@ b :> TRUE @@ c :> TRUE @@ d :> FALSE)
             /\ gen' = 19
             /\ leader' = b
             /\ promised' = (a :> 15 @@ b :> 19 @@ c :> 18 @@ d :> 18)
             /\ ver' = 18
             /\ UNCHANGED << ackGen, acked, ballot, cacked, canswered, cconfirmed, checked, chwm, clock, clog, confirmed, cpLearner, cpNext, cpNextGen, cpView, cutOver, draining, expect, fenceIds, gaveUp, halted, hbAt, hbOut, heard, hwm, inc, inflight, inherited, installed, joinedAt, joining, learner, leaving, leftDirty, log, marked, mine, moves, named, now, oldSet, opened, out, pending, queued, readmit, registered, report, restarting, seen, seenInc, setGen, sid, splits, staged, staleCommit, stopped, successor, wipedAt, wipes, writes, cpGone >>
      []          k = 80 ->
             /\ fencing' = (a :> FALSE @@ b :> TRUE @@ c :> FALSE @@ d :> FALSE)
             /\ log' = (a :> << [g |-> 7, id |-> 0, lg |-> 7], [g |-> 7, id |-> 2, lg |-> 7], [g |-> 12, id |-> 0, lg |-> 12], [g |-> 13, id |-> 0, lg |-> 13], [g |-> 14, id |-> 0, lg |-> 14] >> @@ b :> << [g |-> 7, id |-> 0, lg |-> 7], [g |-> 7, id |-> 2, lg |-> 7], [g |-> 12, id |-> 0, lg |-> 12], [g |-> 13, id |-> 0, lg |-> 13], [g |-> 14, id |-> 0, lg |-> 14], [g |-> 15, id |-> 0, lg |-> 15], [g |-> 16, id |-> 0, lg |-> 16] >> @@ c :> <<[g |-> 7, id |-> 0, lg |-> 7], [g |-> 18, id |-> 0, lg |-> 18]>> @@ d :> <<[g |-> 7, id |-> 0, lg |-> 7]>>)
             /\ opened' = { <<1, a>>, <<7, a>>, <<12, b>>, <<13, b>>, <<14, b>>, <<15, b>>, <<16, b>>, <<18, c>> }
             /\ UNCHANGED << ackGen, acked, answered, ballot, bexpiry, bgen, cacked, canswered, cconfirmed, checked, chwm, clock, clog, confirmed, cpExpiry, cpLearner, cpNext, cpNextGen, cpView, cutOver, draining, expect, fenceIds, gaveUp, gen, halted, hbAt, hbOut, heard, hwm, inc, inflight, inherited, installed, joinedAt, joining, leader, learner, leaving, leftDirty, marked, mine, moves, named, now, oldSet, out, pending, promised, queued, readmit, registered, report, restarting, seen, seenInc, setGen, sid, splits, staged, staleCommit, stopped, successor, ver, wipedAt, wipes, writes, cpGone >>

ReplayNext == step < 80 /\ Next /\ step' = step + 1 /\ Step(step + 1)

====
