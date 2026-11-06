---- MODULE FelixShardWalkElectStaleSetReplay ----
\* Main's FelixShardWalkElectStaleSet.cfg counterexample, replayed as a fixed
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
    /\ ballot = (a :> {a} @@ b :> {a} @@ c :> {} @@ d :> {})
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
    /\ promised = (a :> 1 @@ b :> 1 @@ c :> 0 @@ d :> 0)
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
             /\ bgen' = (a :> 2 @@ b :> 0 @@ c :> 0 @@ d :> 0)
             /\ gen' = 2
             /\ joining' = {c}
             /\ log' = (a :> <<[g |-> 2, id |-> 0, lg |-> 2]>> @@ b :> <<>> @@ c :> <<>> @@ d :> <<>>)
             /\ mine' = (a :> {a, b, c} @@ b :> {a, b} @@ c :> {a, b} @@ d :> {a, b})
             /\ moves' = 1
             /\ opened' = {<<1, a>>, <<2, a>>}
             /\ out' = {d}
             /\ promised' = (a :> 2 @@ b :> 1 @@ c :> 0 @@ d :> 0)
             /\ ver' = 1
             /\ UNCHANGED << ackGen, acked, answered, ballot, bexpiry, cacked, canswered, cconfirmed, checked, chwm, clock, clog, confirmed, cpExpiry, cpLearner, cpNext, cpNextGen, cpView, cutOver, draining, expect, fenceIds, fencing, gaveUp, halted, hbAt, hbOut, heard, hwm, inc, inflight, inherited, installed, joinedAt, leader, learner, leaving, leftDirty, marked, named, now, oldSet, pending, queued, readmit, registered, report, restarting, seen, seenInc, setGen, sid, splits, staged, staleCommit, stopped, successor, wipedAt, wipes, writes, cpGone >>
      []          k = 2 ->
             /\ confirmed' = (a :> (a :> 0 @@ b :> 1 @@ c :> 0 @@ d :> 0) @@ b :> (a :> 0 @@ b :> 0 @@ c :> 0 @@ d :> 0) @@ c :> (a :> 0 @@ b :> 0 @@ c :> 0 @@ d :> 0) @@ d :> (a :> 0 @@ b :> 0 @@ c :> 0 @@ d :> 0))
             /\ log' = (a :> <<[g |-> 2, id |-> 0, lg |-> 2]>> @@ b :> <<[g |-> 2, id |-> 0, lg |-> 2]>> @@ c :> <<>> @@ d :> <<>>)
             /\ promised' = (a :> 2 @@ b :> 2 @@ c :> 0 @@ d :> 0)
             /\ UNCHANGED << ackGen, acked, answered, ballot, bexpiry, bgen, cacked, canswered, cconfirmed, checked, chwm, clock, clog, cpExpiry, cpLearner, cpNext, cpNextGen, cpView, cutOver, draining, expect, fenceIds, fencing, gaveUp, gen, halted, hbAt, hbOut, heard, hwm, inc, inflight, inherited, installed, joinedAt, joining, leader, learner, leaving, leftDirty, marked, mine, moves, named, now, oldSet, opened, out, pending, queued, readmit, registered, report, restarting, seen, seenInc, setGen, sid, splits, staged, staleCommit, stopped, successor, ver, wipedAt, wipes, writes, cpGone >>
      []          k = 3 ->
             /\ queued' = (a :> 1 @@ b :> 0 @@ c :> 0 @@ d :> 0)
             /\ writes' = 1
             /\ UNCHANGED << ackGen, acked, answered, ballot, bexpiry, bgen, cacked, canswered, cconfirmed, checked, chwm, clock, clog, confirmed, cpExpiry, cpLearner, cpNext, cpNextGen, cpView, cutOver, draining, expect, fenceIds, fencing, gaveUp, gen, halted, hbAt, hbOut, heard, hwm, inc, inflight, inherited, installed, joinedAt, joining, leader, learner, leaving, leftDirty, log, marked, mine, moves, named, now, oldSet, opened, out, pending, promised, readmit, registered, report, restarting, seen, seenInc, setGen, sid, splits, staged, staleCommit, stopped, successor, ver, wipedAt, wipes, cpGone >>
      []          k = 4 ->
             /\ ballot' = (a :> {a} @@ b :> {b} @@ c :> {} @@ d :> {})
             /\ bexpiry' = (a :> 2 @@ b :> 2 @@ c :> 0 @@ d :> 0)
             /\ bgen' = (a :> 2 @@ b :> 3 @@ c :> 0 @@ d :> 0)
             /\ fencing' = (a :> FALSE @@ b :> TRUE @@ c :> FALSE @@ d :> FALSE)
             /\ promised' = (a :> 2 @@ b :> 3 @@ c :> 0 @@ d :> 0)
             /\ UNCHANGED << ackGen, acked, answered, cacked, canswered, cconfirmed, checked, chwm, clock, clog, confirmed, cpExpiry, cpLearner, cpNext, cpNextGen, cpView, cutOver, draining, expect, fenceIds, gaveUp, gen, halted, hbAt, hbOut, heard, hwm, inc, inflight, inherited, installed, joinedAt, joining, leader, learner, leaving, leftDirty, log, marked, mine, moves, named, now, oldSet, opened, out, pending, queued, readmit, registered, report, restarting, seen, seenInc, setGen, sid, splits, staged, staleCommit, stopped, successor, ver, wipedAt, wipes, writes, cpGone >>
      []          k = 5 ->
             /\ hbOut' = (a :> FALSE @@ b :> TRUE @@ c :> FALSE @@ d :> FALSE)
             /\ UNCHANGED << ackGen, acked, answered, ballot, bexpiry, bgen, cacked, canswered, cconfirmed, checked, chwm, clock, clog, confirmed, cpExpiry, cpLearner, cpNext, cpNextGen, cpView, cutOver, draining, expect, fenceIds, fencing, gaveUp, gen, halted, hbAt, heard, hwm, inc, inflight, inherited, installed, joinedAt, joining, leader, learner, leaving, leftDirty, log, marked, mine, moves, named, now, oldSet, opened, out, pending, promised, queued, readmit, registered, report, restarting, seen, seenInc, setGen, sid, splits, staged, staleCommit, stopped, successor, ver, wipedAt, wipes, writes, cpGone >>
      []          k = 6 ->
             /\ pending' = (a :> 1 @@ b :> 0 @@ c :> 0 @@ d :> 0)
             /\ queued' = (a :> 0 @@ b :> 0 @@ c :> 0 @@ d :> 0)
             /\ UNCHANGED << ackGen, acked, answered, ballot, bexpiry, bgen, cacked, canswered, cconfirmed, checked, chwm, clock, clog, confirmed, cpExpiry, cpLearner, cpNext, cpNextGen, cpView, cutOver, draining, expect, fenceIds, fencing, gaveUp, gen, halted, hbAt, hbOut, heard, hwm, inc, inflight, inherited, installed, joinedAt, joining, leader, learner, leaving, leftDirty, log, marked, mine, moves, named, now, oldSet, opened, out, promised, readmit, registered, report, restarting, seen, seenInc, setGen, sid, splits, staged, staleCommit, stopped, successor, ver, wipedAt, wipes, writes, cpGone >>
      []          k = 7 ->
             /\ ballot' = (a :> {a} @@ b :> {b} @@ c :> {a} @@ d :> {})
             /\ confirmed' = (a :> (a :> 0 @@ b :> 1 @@ c :> 1 @@ d :> 0) @@ b :> (a :> 0 @@ b :> 0 @@ c :> 0 @@ d :> 0) @@ c :> (a :> 0 @@ b :> 0 @@ c :> 0 @@ d :> 0) @@ d :> (a :> 0 @@ b :> 0 @@ c :> 0 @@ d :> 0))
             /\ log' = (a :> <<[g |-> 2, id |-> 0, lg |-> 2]>> @@ b :> <<[g |-> 2, id |-> 0, lg |-> 2]>> @@ c :> <<[g |-> 2, id |-> 0, lg |-> 2]>> @@ d :> <<>>)
             /\ promised' = (a :> 2 @@ b :> 3 @@ c :> 2 @@ d :> 0)
             /\ UNCHANGED << ackGen, acked, answered, bexpiry, bgen, cacked, canswered, cconfirmed, checked, chwm, clock, clog, cpExpiry, cpLearner, cpNext, cpNextGen, cpView, cutOver, draining, expect, fenceIds, fencing, gaveUp, gen, halted, hbAt, hbOut, heard, hwm, inc, inflight, inherited, installed, joinedAt, joining, leader, learner, leaving, leftDirty, marked, mine, moves, named, now, oldSet, opened, out, pending, queued, readmit, registered, report, restarting, seen, seenInc, setGen, sid, splits, staged, staleCommit, stopped, successor, ver, wipedAt, wipes, writes, cpGone >>
      []          k = 8 ->
             /\ clock' = (a :> 0 @@ b :> 1 @@ c :> 2 @@ d :> 0)
             /\ now' = 1
             /\ UNCHANGED << ackGen, acked, answered, ballot, bexpiry, bgen, cacked, canswered, cconfirmed, checked, chwm, clog, confirmed, cpExpiry, cpLearner, cpNext, cpNextGen, cpView, cutOver, draining, expect, fenceIds, fencing, gaveUp, gen, halted, hbAt, hbOut, heard, hwm, inc, inflight, inherited, installed, joinedAt, joining, leader, learner, leaving, leftDirty, log, marked, mine, moves, named, oldSet, opened, out, pending, promised, queued, readmit, registered, report, restarting, seen, seenInc, setGen, sid, splits, staged, staleCommit, stopped, successor, ver, wipedAt, wipes, writes, cpGone >>
      []          k = 9 ->
             /\ answered' = (a :> {} @@ b :> {a} @@ c :> {} @@ d :> {})
             /\ ballot' = (a :> {b} @@ b :> {b} @@ c :> {a} @@ d :> {})
             /\ promised' = (a :> 3 @@ b :> 3 @@ c :> 2 @@ d :> 0)
             /\ UNCHANGED << ackGen, acked, bexpiry, bgen, cacked, canswered, cconfirmed, checked, chwm, clock, clog, confirmed, cpExpiry, cpLearner, cpNext, cpNextGen, cpView, cutOver, draining, expect, fenceIds, fencing, gaveUp, gen, halted, hbAt, hbOut, heard, hwm, inc, inflight, inherited, installed, joinedAt, joining, leader, learner, leaving, leftDirty, log, marked, mine, moves, named, now, oldSet, opened, out, pending, queued, readmit, registered, report, restarting, seen, seenInc, setGen, sid, splits, staged, staleCommit, stopped, successor, ver, wipedAt, wipes, writes, cpGone >>
      []          k = 10 ->
             /\ answered' = (a :> {} @@ b :> {} @@ c :> {} @@ d :> {})
             /\ bexpiry' = (a :> 2 @@ b :> 3 @@ c :> 0 @@ d :> 0)
             /\ cpExpiry' = 3
             /\ gen' = 3
             /\ joining' = {}
             /\ leader' = b
             /\ mine' = (a :> {a, b, c} @@ b :> {a, b, c} @@ c :> {a, b} @@ d :> {a, b})
             /\ ver' = 2
             /\ UNCHANGED << ackGen, acked, ballot, bgen, cacked, canswered, cconfirmed, checked, chwm, clock, clog, confirmed, cpLearner, cpNext, cpNextGen, cpView, cutOver, draining, expect, fenceIds, fencing, gaveUp, halted, hbAt, hbOut, heard, hwm, inc, inflight, inherited, installed, joinedAt, learner, leaving, leftDirty, log, marked, moves, named, now, oldSet, opened, out, pending, promised, queued, readmit, registered, report, restarting, seen, seenInc, setGen, sid, splits, staged, staleCommit, stopped, successor, wipedAt, wipes, writes, cpGone >>
      []          k = 11 ->
             /\ log' = (a :> <<[g |-> 2, id |-> 0, lg |-> 2], [g |-> 2, id |-> 1, lg |-> 2]>> @@ b :> <<[g |-> 2, id |-> 0, lg |-> 2]>> @@ c :> <<[g |-> 2, id |-> 0, lg |-> 2]>> @@ d :> <<>>)
             /\ pending' = (a :> 0 @@ b :> 0 @@ c :> 0 @@ d :> 0)
             /\ staleCommit' = TRUE
             /\ UNCHANGED << ackGen, acked, answered, ballot, bexpiry, bgen, cacked, canswered, cconfirmed, checked, chwm, clock, clog, confirmed, cpExpiry, cpLearner, cpNext, cpNextGen, cpView, cutOver, draining, expect, fenceIds, fencing, gaveUp, gen, halted, hbAt, hbOut, heard, hwm, inc, inflight, inherited, installed, joinedAt, joining, leader, learner, leaving, leftDirty, marked, mine, moves, named, now, oldSet, opened, out, promised, queued, readmit, registered, report, restarting, seen, seenInc, setGen, sid, splits, staged, stopped, successor, ver, wipedAt, wipes, writes, cpGone >>
      []          k = 12 ->
             /\ bgen' = (a :> 0 @@ b :> 3 @@ c :> 0 @@ d :> 0)
             /\ UNCHANGED << ackGen, acked, answered, ballot, bexpiry, cacked, canswered, cconfirmed, checked, chwm, clock, clog, confirmed, cpExpiry, cpLearner, cpNext, cpNextGen, cpView, cutOver, draining, expect, fenceIds, fencing, gaveUp, gen, halted, hbAt, hbOut, heard, hwm, inc, inflight, inherited, installed, joinedAt, joining, leader, learner, leaving, leftDirty, log, marked, mine, moves, named, now, oldSet, opened, out, pending, promised, queued, readmit, registered, report, restarting, seen, seenInc, setGen, sid, splits, staged, staleCommit, stopped, successor, ver, wipedAt, wipes, writes, cpGone >>
      []          k = 13 ->
             /\ answered' = (a :> {} @@ b :> {c} @@ c :> {} @@ d :> {})
             /\ ballot' = (a :> {b} @@ b :> {b} @@ c :> {b} @@ d :> {})
             /\ promised' = (a :> 3 @@ b :> 3 @@ c :> 3 @@ d :> 0)
             /\ UNCHANGED << ackGen, acked, bexpiry, bgen, cacked, canswered, cconfirmed, checked, chwm, clock, clog, confirmed, cpExpiry, cpLearner, cpNext, cpNextGen, cpView, cutOver, draining, expect, fenceIds, fencing, gaveUp, gen, halted, hbAt, hbOut, heard, hwm, inc, inflight, inherited, installed, joinedAt, joining, leader, learner, leaving, leftDirty, log, marked, mine, moves, named, now, oldSet, opened, out, pending, queued, readmit, registered, report, restarting, seen, seenInc, setGen, sid, splits, staged, staleCommit, stopped, successor, ver, wipedAt, wipes, writes, cpGone >>
      []          k = 14 ->
             /\ ballot' = (a :> {a} @@ b :> {b} @@ c :> {b} @@ d :> {})
             /\ bgen' = (a :> 4 @@ b :> 3 @@ c :> 0 @@ d :> 0)
             /\ confirmed' = (a :> (a :> 0 @@ b :> 0 @@ c :> 0 @@ d :> 0) @@ b :> (a :> 0 @@ b :> 0 @@ c :> 0 @@ d :> 0) @@ c :> (a :> 0 @@ b :> 0 @@ c :> 0 @@ d :> 0) @@ d :> (a :> 0 @@ b :> 0 @@ c :> 0 @@ d :> 0))
             /\ fencing' = (a :> TRUE @@ b :> TRUE @@ c :> FALSE @@ d :> FALSE)
             /\ gen' = 4
             /\ leader' = a
             /\ promised' = (a :> 4 @@ b :> 3 @@ c :> 3 @@ d :> 0)
             /\ ver' = 3
             /\ UNCHANGED << ackGen, acked, answered, bexpiry, cacked, canswered, cconfirmed, checked, chwm, clock, clog, cpExpiry, cpLearner, cpNext, cpNextGen, cpView, cutOver, draining, expect, fenceIds, gaveUp, halted, hbAt, hbOut, heard, hwm, inc, inflight, inherited, installed, joinedAt, joining, learner, leaving, leftDirty, log, marked, mine, moves, named, now, oldSet, opened, out, pending, queued, readmit, registered, report, restarting, seen, seenInc, setGen, sid, splits, staged, staleCommit, stopped, successor, wipedAt, wipes, writes, cpGone >>
      []          k = 15 ->
             /\ fencing' = (a :> TRUE @@ b :> FALSE @@ c :> FALSE @@ d :> FALSE)
             /\ log' = (a :> <<[g |-> 2, id |-> 0, lg |-> 2], [g |-> 2, id |-> 1, lg |-> 2]>> @@ b :> <<[g |-> 2, id |-> 0, lg |-> 2], [g |-> 3, id |-> 0, lg |-> 3]>> @@ c :> <<[g |-> 2, id |-> 0, lg |-> 2]>> @@ d :> <<>>)
             /\ opened' = {<<1, a>>, <<2, a>>, <<3, b>>}
             /\ UNCHANGED << ackGen, acked, answered, ballot, bexpiry, bgen, cacked, canswered, cconfirmed, checked, chwm, clock, clog, confirmed, cpExpiry, cpLearner, cpNext, cpNextGen, cpView, cutOver, draining, expect, fenceIds, gaveUp, gen, halted, hbAt, hbOut, heard, hwm, inc, inflight, inherited, installed, joinedAt, joining, leader, learner, leaving, leftDirty, marked, mine, moves, named, now, oldSet, out, pending, promised, queued, readmit, registered, report, restarting, seen, seenInc, setGen, sid, splits, staged, staleCommit, stopped, successor, ver, wipedAt, wipes, writes, cpGone >>
      []          k = 16 ->
             /\ answered' = (a :> {} @@ b :> {} @@ c :> {} @@ d :> {})
             /\ bgen' = (a :> 4 @@ b :> 5 @@ c :> 0 @@ d :> 0)
             /\ fencing' = (a :> TRUE @@ b :> TRUE @@ c :> FALSE @@ d :> FALSE)
             /\ gen' = 5
             /\ leader' = b
             /\ promised' = (a :> 4 @@ b :> 5 @@ c :> 3 @@ d :> 0)
             /\ ver' = 4
             /\ UNCHANGED << ackGen, acked, ballot, bexpiry, cacked, canswered, cconfirmed, checked, chwm, clock, clog, confirmed, cpExpiry, cpLearner, cpNext, cpNextGen, cpView, cutOver, draining, expect, fenceIds, gaveUp, halted, hbAt, hbOut, heard, hwm, inc, inflight, inherited, installed, joinedAt, joining, learner, leaving, leftDirty, log, marked, mine, moves, named, now, oldSet, opened, out, pending, queued, readmit, registered, report, restarting, seen, seenInc, setGen, sid, splits, staged, staleCommit, stopped, successor, wipedAt, wipes, writes, cpGone >>
      []          k = 17 ->
             /\ answered' = (a :> {} @@ b :> {c} @@ c :> {} @@ d :> {})
             /\ promised' = (a :> 4 @@ b :> 5 @@ c :> 5 @@ d :> 0)
             /\ UNCHANGED << ackGen, acked, ballot, bexpiry, bgen, cacked, canswered, cconfirmed, checked, chwm, clock, clog, confirmed, cpExpiry, cpLearner, cpNext, cpNextGen, cpView, cutOver, draining, expect, fenceIds, fencing, gaveUp, gen, halted, hbAt, hbOut, heard, hwm, inc, inflight, inherited, installed, joinedAt, joining, leader, learner, leaving, leftDirty, log, marked, mine, moves, named, now, oldSet, opened, out, pending, queued, readmit, registered, report, restarting, seen, seenInc, setGen, sid, splits, staged, staleCommit, stopped, successor, ver, wipedAt, wipes, writes, cpGone >>
      []          k = 18 ->
             /\ ballot' = (a :> {a} @@ b :> {b} @@ c :> {c} @@ d :> {})
             /\ bexpiry' = (a :> 2 @@ b :> 3 @@ c :> 4 @@ d :> 0)
             /\ bgen' = (a :> 4 @@ b :> 5 @@ c :> 6 @@ d :> 0)
             /\ fencing' = (a :> TRUE @@ b :> TRUE @@ c :> TRUE @@ d :> FALSE)
             /\ gen' = 6
             /\ leader' = c
             /\ mine' = (a :> {a, b, c} @@ b :> {a, b, c} @@ c :> {a, b, c} @@ d :> {a, b})
             /\ promised' = (a :> 4 @@ b :> 5 @@ c :> 6 @@ d :> 0)
             /\ ver' = 5
             /\ UNCHANGED << ackGen, acked, answered, cacked, canswered, cconfirmed, checked, chwm, clock, clog, confirmed, cpExpiry, cpLearner, cpNext, cpNextGen, cpView, cutOver, draining, expect, fenceIds, gaveUp, halted, hbAt, hbOut, heard, hwm, inc, inflight, inherited, installed, joinedAt, joining, learner, leaving, leftDirty, log, marked, moves, named, now, oldSet, opened, out, pending, queued, readmit, registered, report, restarting, seen, seenInc, setGen, sid, splits, staged, staleCommit, stopped, successor, wipedAt, wipes, writes, cpGone >>
      []          k = 19 ->
             /\ bgen' = (a :> 5 @@ b :> 5 @@ c :> 6 @@ d :> 0)
             /\ promised' = (a :> 5 @@ b :> 5 @@ c :> 6 @@ d :> 0)
             /\ UNCHANGED << ackGen, acked, answered, ballot, bexpiry, cacked, canswered, cconfirmed, checked, chwm, clock, clog, confirmed, cpExpiry, cpLearner, cpNext, cpNextGen, cpView, cutOver, draining, expect, fenceIds, fencing, gaveUp, gen, halted, hbAt, hbOut, heard, hwm, inc, inflight, inherited, installed, joinedAt, joining, leader, learner, leaving, leftDirty, log, marked, mine, moves, named, now, oldSet, opened, out, pending, queued, readmit, registered, report, restarting, seen, seenInc, setGen, sid, splits, staged, staleCommit, stopped, successor, ver, wipedAt, wipes, writes, cpGone >>
      []          k = 20 ->
             /\ answered' = (a :> {} @@ b :> {c} @@ c :> {a} @@ d :> {})
             /\ ballot' = (a :> {c} @@ b :> {b} @@ c :> {c} @@ d :> {})
             /\ log' = (a :> <<[g |-> 2, id |-> 0, lg |-> 2], [g |-> 2, id |-> 1, lg |-> 2]>> @@ b :> <<[g |-> 2, id |-> 0, lg |-> 2], [g |-> 3, id |-> 0, lg |-> 3]>> @@ c :> <<[g |-> 2, id |-> 0, lg |-> 2], [g |-> 2, id |-> 1, lg |-> 2]>> @@ d :> <<>>)
             /\ promised' = (a :> 6 @@ b :> 5 @@ c :> 6 @@ d :> 0)
             /\ UNCHANGED << ackGen, acked, bexpiry, bgen, cacked, canswered, cconfirmed, checked, chwm, clock, clog, confirmed, cpExpiry, cpLearner, cpNext, cpNextGen, cpView, cutOver, draining, expect, fenceIds, fencing, gaveUp, gen, halted, hbAt, hbOut, heard, hwm, inc, inflight, inherited, installed, joinedAt, joining, leader, learner, leaving, leftDirty, marked, mine, moves, named, now, oldSet, opened, out, pending, queued, readmit, registered, report, restarting, seen, seenInc, setGen, sid, splits, staged, staleCommit, stopped, successor, ver, wipedAt, wipes, writes, cpGone >>
      []          k = 21 ->
             /\ ballot' = (a :> {a} @@ b :> {b} @@ c :> {c} @@ d :> {})
             /\ bgen' = (a :> 7 @@ b :> 5 @@ c :> 6 @@ d :> 0)
             /\ gen' = 7
             /\ leader' = a
             /\ promised' = (a :> 7 @@ b :> 5 @@ c :> 6 @@ d :> 0)
             /\ ver' = 6
             /\ UNCHANGED << ackGen, acked, answered, bexpiry, cacked, canswered, cconfirmed, checked, chwm, clock, clog, confirmed, cpExpiry, cpLearner, cpNext, cpNextGen, cpView, cutOver, draining, expect, fenceIds, fencing, gaveUp, halted, hbAt, hbOut, heard, hwm, inc, inflight, inherited, installed, joinedAt, joining, learner, leaving, leftDirty, log, marked, mine, moves, named, now, oldSet, opened, out, pending, queued, readmit, registered, report, restarting, seen, seenInc, setGen, sid, splits, staged, staleCommit, stopped, successor, wipedAt, wipes, writes, cpGone >>
      []          k = 22 ->
             /\ answered' = (a :> {} @@ b :> {c} @@ c :> {} @@ d :> {})
             /\ bgen' = (a :> 7 @@ b :> 5 @@ c :> 7 @@ d :> 0)
             /\ promised' = (a :> 7 @@ b :> 5 @@ c :> 7 @@ d :> 0)
             /\ UNCHANGED << ackGen, acked, ballot, bexpiry, cacked, canswered, cconfirmed, checked, chwm, clock, clog, confirmed, cpExpiry, cpLearner, cpNext, cpNextGen, cpView, cutOver, draining, expect, fenceIds, fencing, gaveUp, gen, halted, hbAt, hbOut, heard, hwm, inc, inflight, inherited, installed, joinedAt, joining, leader, learner, leaving, leftDirty, log, marked, mine, moves, named, now, oldSet, opened, out, pending, queued, readmit, registered, report, restarting, seen, seenInc, setGen, sid, splits, staged, staleCommit, stopped, successor, ver, wipedAt, wipes, writes, cpGone >>
      []          k = 23 ->
             /\ fencing' = (a :> TRUE @@ b :> FALSE @@ c :> TRUE @@ d :> FALSE)
             /\ log' = (a :> <<[g |-> 2, id |-> 0, lg |-> 2], [g |-> 2, id |-> 1, lg |-> 2]>> @@ b :> << [g |-> 2, id |-> 0, lg |-> 2], [g |-> 3, id |-> 0, lg |-> 3], [g |-> 5, id |-> 0, lg |-> 5] >> @@ c :> <<[g |-> 2, id |-> 0, lg |-> 2], [g |-> 2, id |-> 1, lg |-> 2]>> @@ d :> <<>>)
             /\ opened' = {<<1, a>>, <<2, a>>, <<3, b>>, <<5, b>>}
             /\ UNCHANGED << ackGen, acked, answered, ballot, bexpiry, bgen, cacked, canswered, cconfirmed, checked, chwm, clock, clog, confirmed, cpExpiry, cpLearner, cpNext, cpNextGen, cpView, cutOver, draining, expect, fenceIds, gaveUp, gen, halted, hbAt, hbOut, heard, hwm, inc, inflight, inherited, installed, joinedAt, joining, leader, learner, leaving, leftDirty, marked, mine, moves, named, now, oldSet, out, pending, promised, queued, readmit, registered, report, restarting, seen, seenInc, setGen, sid, splits, staged, staleCommit, stopped, successor, ver, wipedAt, wipes, writes, cpGone >>
      []          k = 24 ->
             /\ hbOut' = (a :> FALSE @@ b :> FALSE @@ c :> FALSE @@ d :> FALSE)
             /\ UNCHANGED << ackGen, acked, answered, ballot, bexpiry, bgen, cacked, canswered, cconfirmed, checked, chwm, clock, clog, confirmed, cpExpiry, cpLearner, cpNext, cpNextGen, cpView, cutOver, draining, expect, fenceIds, fencing, gaveUp, gen, halted, hbAt, heard, hwm, inc, inflight, inherited, installed, joinedAt, joining, leader, learner, leaving, leftDirty, log, marked, mine, moves, named, now, oldSet, opened, out, pending, promised, queued, readmit, registered, report, restarting, seen, seenInc, setGen, sid, splits, staged, staleCommit, stopped, successor, ver, wipedAt, wipes, writes, cpGone >>
      []          k = 25 ->
             /\ bgen' = (a :> 8 @@ b :> 5 @@ c :> 7 @@ d :> 0)
             /\ promised' = (a :> 8 @@ b :> 5 @@ c :> 7 @@ d :> 0)
             /\ UNCHANGED << ackGen, acked, answered, ballot, bexpiry, cacked, canswered, cconfirmed, checked, chwm, clock, clog, confirmed, cpExpiry, cpLearner, cpNext, cpNextGen, cpView, cutOver, draining, expect, fenceIds, fencing, gaveUp, gen, halted, hbAt, hbOut, heard, hwm, inc, inflight, inherited, installed, joinedAt, joining, leader, learner, leaving, leftDirty, log, marked, mine, moves, named, now, oldSet, opened, out, pending, queued, readmit, registered, report, restarting, seen, seenInc, setGen, sid, splits, staged, staleCommit, stopped, successor, ver, wipedAt, wipes, writes, cpGone >>
      []          k = 26 ->
             /\ hbAt' = (a :> 0 @@ b :> 1 @@ c :> 0 @@ d :> 0)
             /\ hbOut' = (a :> FALSE @@ b :> TRUE @@ c :> FALSE @@ d :> FALSE)
             /\ UNCHANGED << ackGen, acked, answered, ballot, bexpiry, bgen, cacked, canswered, cconfirmed, checked, chwm, clock, clog, confirmed, cpExpiry, cpLearner, cpNext, cpNextGen, cpView, cutOver, draining, expect, fenceIds, fencing, gaveUp, gen, halted, heard, hwm, inc, inflight, inherited, installed, joinedAt, joining, leader, learner, leaving, leftDirty, log, marked, mine, moves, named, now, oldSet, opened, out, pending, promised, queued, readmit, registered, report, restarting, seen, seenInc, setGen, sid, splits, staged, staleCommit, stopped, successor, ver, wipedAt, wipes, writes, cpGone >>
      []          k = 27 ->
             /\ queued' = (a :> 0 @@ b :> 2 @@ c :> 0 @@ d :> 0)
             /\ writes' = 2
             /\ UNCHANGED << ackGen, acked, answered, ballot, bexpiry, bgen, cacked, canswered, cconfirmed, checked, chwm, clock, clog, confirmed, cpExpiry, cpLearner, cpNext, cpNextGen, cpView, cutOver, draining, expect, fenceIds, fencing, gaveUp, gen, halted, hbAt, hbOut, heard, hwm, inc, inflight, inherited, installed, joinedAt, joining, leader, learner, leaving, leftDirty, log, marked, mine, moves, named, now, oldSet, opened, out, pending, promised, readmit, registered, report, restarting, seen, seenInc, setGen, sid, splits, staged, staleCommit, stopped, successor, ver, wipedAt, wipes, cpGone >>
      []          k = 28 ->
             /\ clock' = (a :> 2 @@ b :> 1 @@ c :> 2 @@ d :> 1)
             /\ now' = 2
             /\ UNCHANGED << ackGen, acked, answered, ballot, bexpiry, bgen, cacked, canswered, cconfirmed, checked, chwm, clog, confirmed, cpExpiry, cpLearner, cpNext, cpNextGen, cpView, cutOver, draining, expect, fenceIds, fencing, gaveUp, gen, halted, hbAt, hbOut, heard, hwm, inc, inflight, inherited, installed, joinedAt, joining, leader, learner, leaving, leftDirty, log, marked, mine, moves, named, oldSet, opened, out, pending, promised, queued, readmit, registered, report, restarting, seen, seenInc, setGen, sid, splits, staged, staleCommit, stopped, successor, ver, wipedAt, wipes, writes, cpGone >>
      []          k = 29 ->
             /\ answered' = (a :> {b} @@ b :> {c} @@ c :> {} @@ d :> {})
             /\ ballot' = (a :> {a} @@ b :> {a} @@ c :> {c} @@ d :> {})
             /\ log' = (a :> << [g |-> 2, id |-> 0, lg |-> 2], [g |-> 3, id |-> 0, lg |-> 3], [g |-> 5, id |-> 0, lg |-> 5] >> @@ b :> << [g |-> 2, id |-> 0, lg |-> 2], [g |-> 3, id |-> 0, lg |-> 3], [g |-> 5, id |-> 0, lg |-> 5] >> @@ c :> <<[g |-> 2, id |-> 0, lg |-> 2], [g |-> 2, id |-> 1, lg |-> 2]>> @@ d :> <<>>)
             /\ promised' = (a :> 8 @@ b :> 8 @@ c :> 7 @@ d :> 0)
             /\ UNCHANGED << ackGen, acked, bexpiry, bgen, cacked, canswered, cconfirmed, checked, chwm, clock, clog, confirmed, cpExpiry, cpLearner, cpNext, cpNextGen, cpView, cutOver, draining, expect, fenceIds, fencing, gaveUp, gen, halted, hbAt, hbOut, heard, hwm, inc, inflight, inherited, installed, joinedAt, joining, leader, learner, leaving, leftDirty, marked, mine, moves, named, now, oldSet, opened, out, pending, queued, readmit, registered, report, restarting, seen, seenInc, setGen, sid, splits, staged, staleCommit, stopped, successor, ver, wipedAt, wipes, writes, cpGone >>
      []          k = 30 ->
             /\ bgen' = (a :> 8 @@ b :> 5 @@ c :> 8 @@ d :> 0)
             /\ cpExpiry' = 4
             /\ gen' = 8
             /\ leader' = c
             /\ promised' = (a :> 8 @@ b :> 8 @@ c :> 8 @@ d :> 0)
             /\ ver' = 7
             /\ UNCHANGED << ackGen, acked, answered, ballot, bexpiry, cacked, canswered, cconfirmed, checked, chwm, clock, clog, confirmed, cpLearner, cpNext, cpNextGen, cpView, cutOver, draining, expect, fenceIds, fencing, gaveUp, halted, hbAt, hbOut, heard, hwm, inc, inflight, inherited, installed, joinedAt, joining, learner, leaving, leftDirty, log, marked, mine, moves, named, now, oldSet, opened, out, pending, queued, readmit, registered, report, restarting, seen, seenInc, setGen, sid, splits, staged, staleCommit, stopped, successor, wipedAt, wipes, writes, cpGone >>
      []          k = 31 ->
             /\ pending' = (a :> 0 @@ b :> 2 @@ c :> 0 @@ d :> 0)
             /\ queued' = (a :> 0 @@ b :> 0 @@ c :> 0 @@ d :> 0)
             /\ UNCHANGED << ackGen, acked, answered, ballot, bexpiry, bgen, cacked, canswered, cconfirmed, checked, chwm, clock, clog, confirmed, cpExpiry, cpLearner, cpNext, cpNextGen, cpView, cutOver, draining, expect, fenceIds, fencing, gaveUp, gen, halted, hbAt, hbOut, heard, hwm, inc, inflight, inherited, installed, joinedAt, joining, leader, learner, leaving, leftDirty, log, marked, mine, moves, named, now, oldSet, opened, out, promised, readmit, registered, report, restarting, seen, seenInc, setGen, sid, splits, staged, staleCommit, stopped, successor, ver, wipedAt, wipes, writes, cpGone >>
      []          k = 32 ->
             /\ hbAt' = (a :> 0 @@ b :> 1 @@ c :> 2 @@ d :> 0)
             /\ hbOut' = (a :> FALSE @@ b :> TRUE @@ c :> TRUE @@ d :> FALSE)
             /\ UNCHANGED << ackGen, acked, answered, ballot, bexpiry, bgen, cacked, canswered, cconfirmed, checked, chwm, clock, clog, confirmed, cpExpiry, cpLearner, cpNext, cpNextGen, cpView, cutOver, draining, expect, fenceIds, fencing, gaveUp, gen, halted, heard, hwm, inc, inflight, inherited, installed, joinedAt, joining, leader, learner, leaving, leftDirty, log, marked, mine, moves, named, now, oldSet, opened, out, pending, promised, queued, readmit, registered, report, restarting, seen, seenInc, setGen, sid, splits, staged, staleCommit, stopped, successor, ver, wipedAt, wipes, writes, cpGone >>
      []          k = 33 ->
             /\ clock' = (a :> 2 @@ b :> 3 @@ c :> 4 @@ d :> 2)
             /\ now' = 3
             /\ UNCHANGED << ackGen, acked, answered, ballot, bexpiry, bgen, cacked, canswered, cconfirmed, checked, chwm, clog, confirmed, cpExpiry, cpLearner, cpNext, cpNextGen, cpView, cutOver, draining, expect, fenceIds, fencing, gaveUp, gen, halted, hbAt, hbOut, heard, hwm, inc, inflight, inherited, installed, joinedAt, joining, leader, learner, leaving, leftDirty, log, marked, mine, moves, named, oldSet, opened, out, pending, promised, queued, readmit, registered, report, restarting, seen, seenInc, setGen, sid, splits, staged, staleCommit, stopped, successor, ver, wipedAt, wipes, writes, cpGone >>
      []          k = 34 ->
             /\ ballot' = (a :> {a} @@ b :> {a} @@ c :> {c} @@ d :> {d})
             /\ bexpiry' = (a :> 2 @@ b :> 3 @@ c :> 4 @@ d :> 4)
             /\ bgen' = (a :> 8 @@ b :> 5 @@ c :> 8 @@ d :> 1)
             /\ fencing' = (a :> TRUE @@ b :> FALSE @@ c :> TRUE @@ d :> TRUE)
             /\ promised' = (a :> 8 @@ b :> 8 @@ c :> 8 @@ d :> 1)
             /\ UNCHANGED << ackGen, acked, answered, cacked, canswered, cconfirmed, checked, chwm, clock, clog, confirmed, cpExpiry, cpLearner, cpNext, cpNextGen, cpView, cutOver, draining, expect, fenceIds, gaveUp, gen, halted, hbAt, hbOut, heard, hwm, inc, inflight, inherited, installed, joinedAt, joining, leader, learner, leaving, leftDirty, log, marked, mine, moves, named, now, oldSet, opened, out, pending, queued, readmit, registered, report, restarting, seen, seenInc, setGen, sid, splits, staged, staleCommit, stopped, successor, ver, wipedAt, wipes, writes, cpGone >>
      []          k = 35 ->
             /\ answered' = (a :> {} @@ b :> {c} @@ c :> {} @@ d :> {})
             /\ bexpiry' = (a :> 4 @@ b :> 3 @@ c :> 4 @@ d :> 4)
             /\ bgen' = (a :> 9 @@ b :> 5 @@ c :> 8 @@ d :> 1)
             /\ promised' = (a :> 9 @@ b :> 8 @@ c :> 8 @@ d :> 1)
             /\ UNCHANGED << ackGen, acked, ballot, cacked, canswered, cconfirmed, checked, chwm, clock, clog, confirmed, cpExpiry, cpLearner, cpNext, cpNextGen, cpView, cutOver, draining, expect, fenceIds, fencing, gaveUp, gen, halted, hbAt, hbOut, heard, hwm, inc, inflight, inherited, installed, joinedAt, joining, leader, learner, leaving, leftDirty, log, marked, mine, moves, named, now, oldSet, opened, out, pending, queued, readmit, registered, report, restarting, seen, seenInc, setGen, sid, splits, staged, staleCommit, stopped, successor, ver, wipedAt, wipes, writes, cpGone >>
      []          k = 36 ->
             /\ answered' = (a :> {c} @@ b :> {c} @@ c :> {} @@ d :> {})
             /\ ballot' = (a :> {a} @@ b :> {a} @@ c :> {a} @@ d :> {d})
             /\ promised' = (a :> 9 @@ b :> 8 @@ c :> 9 @@ d :> 1)
             /\ UNCHANGED << ackGen, acked, bexpiry, bgen, cacked, canswered, cconfirmed, checked, chwm, clock, clog, confirmed, cpExpiry, cpLearner, cpNext, cpNextGen, cpView, cutOver, draining, expect, fenceIds, fencing, gaveUp, gen, halted, hbAt, hbOut, heard, hwm, inc, inflight, inherited, installed, joinedAt, joining, leader, learner, leaving, leftDirty, log, marked, mine, moves, named, now, oldSet, opened, out, pending, queued, readmit, registered, report, restarting, seen, seenInc, setGen, sid, splits, staged, staleCommit, stopped, successor, ver, wipedAt, wipes, writes, cpGone >>
      []          k = 37 ->
             /\ log' = (a :> << [g |-> 2, id |-> 0, lg |-> 2], [g |-> 3, id |-> 0, lg |-> 3], [g |-> 5, id |-> 0, lg |-> 5] >> @@ b :> << [g |-> 2, id |-> 0, lg |-> 2], [g |-> 3, id |-> 0, lg |-> 3], [g |-> 5, id |-> 0, lg |-> 5], [g |-> 5, id |-> 2, lg |-> 5] >> @@ c :> <<[g |-> 2, id |-> 0, lg |-> 2], [g |-> 2, id |-> 1, lg |-> 2]>> @@ d :> <<>>)
             /\ pending' = (a :> 0 @@ b :> 0 @@ c :> 0 @@ d :> 0)
             /\ UNCHANGED << ackGen, acked, answered, ballot, bexpiry, bgen, cacked, canswered, cconfirmed, checked, chwm, clock, clog, confirmed, cpExpiry, cpLearner, cpNext, cpNextGen, cpView, cutOver, draining, expect, fenceIds, fencing, gaveUp, gen, halted, hbAt, hbOut, heard, hwm, inc, inflight, inherited, installed, joinedAt, joining, leader, learner, leaving, leftDirty, marked, mine, moves, named, now, oldSet, opened, out, promised, queued, readmit, registered, report, restarting, seen, seenInc, setGen, sid, splits, staged, staleCommit, stopped, successor, ver, wipedAt, wipes, writes, cpGone >>
      []          k = 38 ->
             /\ fencing' = (a :> FALSE @@ b :> FALSE @@ c :> TRUE @@ d :> TRUE)
             /\ log' = (a :> << [g |-> 2, id |-> 0, lg |-> 2], [g |-> 3, id |-> 0, lg |-> 3], [g |-> 5, id |-> 0, lg |-> 5], [g |-> 9, id |-> 0, lg |-> 9] >> @@ b :> << [g |-> 2, id |-> 0, lg |-> 2], [g |-> 3, id |-> 0, lg |-> 3], [g |-> 5, id |-> 0, lg |-> 5], [g |-> 5, id |-> 2, lg |-> 5] >> @@ c :> <<[g |-> 2, id |-> 0, lg |-> 2], [g |-> 2, id |-> 1, lg |-> 2]>> @@ d :> <<>>)
             /\ opened' = {<<1, a>>, <<2, a>>, <<3, b>>, <<5, b>>, <<9, a>>}
             /\ UNCHANGED << ackGen, acked, answered, ballot, bexpiry, bgen, cacked, canswered, cconfirmed, checked, chwm, clock, clog, confirmed, cpExpiry, cpLearner, cpNext, cpNextGen, cpView, cutOver, draining, expect, fenceIds, gaveUp, gen, halted, hbAt, hbOut, heard, hwm, inc, inflight, inherited, installed, joinedAt, joining, leader, learner, leaving, leftDirty, marked, mine, moves, named, now, oldSet, out, pending, promised, queued, readmit, registered, report, restarting, seen, seenInc, setGen, sid, splits, staged, staleCommit, stopped, successor, ver, wipedAt, wipes, writes, cpGone >>
      []          k = 39 ->
             /\ bgen' = (a :> 9 @@ b :> 5 @@ c :> 0 @@ d :> 1)
             /\ fencing' = (a :> FALSE @@ b :> FALSE @@ c :> FALSE @@ d :> TRUE)
             /\ UNCHANGED << ackGen, acked, answered, ballot, bexpiry, cacked, canswered, cconfirmed, checked, chwm, clock, clog, confirmed, cpExpiry, cpLearner, cpNext, cpNextGen, cpView, cutOver, draining, expect, fenceIds, gaveUp, gen, halted, hbAt, hbOut, heard, hwm, inc, inflight, inherited, installed, joinedAt, joining, leader, learner, leaving, leftDirty, log, marked, mine, moves, named, now, oldSet, opened, out, pending, promised, queued, readmit, registered, report, restarting, seen, seenInc, setGen, sid, splits, staged, staleCommit, stopped, successor, ver, wipedAt, wipes, writes, cpGone >>
      []          k = 40 ->
             /\ hbAt' = (a :> 0 @@ b :> 1 @@ c :> 2 @@ d :> 2)
             /\ hbOut' = (a :> FALSE @@ b :> TRUE @@ c :> TRUE @@ d :> TRUE)
             /\ UNCHANGED << ackGen, acked, answered, ballot, bexpiry, bgen, cacked, canswered, cconfirmed, checked, chwm, clock, clog, confirmed, cpExpiry, cpLearner, cpNext, cpNextGen, cpView, cutOver, draining, expect, fenceIds, fencing, gaveUp, gen, halted, heard, hwm, inc, inflight, inherited, installed, joinedAt, joining, leader, learner, leaving, leftDirty, log, marked, mine, moves, named, now, oldSet, opened, out, pending, promised, queued, readmit, registered, report, restarting, seen, seenInc, setGen, sid, splits, staged, staleCommit, stopped, successor, ver, wipedAt, wipes, writes, cpGone >>
      []          k = 41 ->
             /\ ballot' = (a :> {a} @@ b :> {a} @@ c :> {c} @@ d :> {d})
             /\ bexpiry' = (a :> 4 @@ b :> 3 @@ c :> 6 @@ d :> 4)
             /\ bgen' = (a :> 9 @@ b :> 5 @@ c :> 10 @@ d :> 1)
             /\ fencing' = (a :> FALSE @@ b :> FALSE @@ c :> TRUE @@ d :> TRUE)
             /\ promised' = (a :> 9 @@ b :> 8 @@ c :> 10 @@ d :> 1)
             /\ UNCHANGED << ackGen, acked, answered, cacked, canswered, cconfirmed, checked, chwm, clock, clog, confirmed, cpExpiry, cpLearner, cpNext, cpNextGen, cpView, cutOver, draining, expect, fenceIds, gaveUp, gen, halted, hbAt, hbOut, heard, hwm, inc, inflight, inherited, installed, joinedAt, joining, leader, learner, leaving, leftDirty, log, marked, mine, moves, named, now, oldSet, opened, out, pending, queued, readmit, registered, report, restarting, seen, seenInc, setGen, sid, splits, staged, staleCommit, stopped, successor, ver, wipedAt, wipes, writes, cpGone >>
      []          k = 42 ->
             /\ hbOut' = (a :> FALSE @@ b :> TRUE @@ c :> FALSE @@ d :> TRUE)
             /\ UNCHANGED << ackGen, acked, answered, ballot, bexpiry, bgen, cacked, canswered, cconfirmed, checked, chwm, clock, clog, confirmed, cpExpiry, cpLearner, cpNext, cpNextGen, cpView, cutOver, draining, expect, fenceIds, fencing, gaveUp, gen, halted, hbAt, heard, hwm, inc, inflight, inherited, installed, joinedAt, joining, leader, learner, leaving, leftDirty, log, marked, mine, moves, named, now, oldSet, opened, out, pending, promised, queued, readmit, registered, report, restarting, seen, seenInc, setGen, sid, splits, staged, staleCommit, stopped, successor, ver, wipedAt, wipes, writes, cpGone >>
      []          k = 43 ->
             /\ clock' = (a :> 3 @@ b :> 3 @@ c :> 5 @@ d :> 4)
             /\ now' = 4
             /\ UNCHANGED << ackGen, acked, answered, ballot, bexpiry, bgen, cacked, canswered, cconfirmed, checked, chwm, clog, confirmed, cpExpiry, cpLearner, cpNext, cpNextGen, cpView, cutOver, draining, expect, fenceIds, fencing, gaveUp, gen, halted, hbAt, hbOut, heard, hwm, inc, inflight, inherited, installed, joinedAt, joining, leader, learner, leaving, leftDirty, log, marked, mine, moves, named, oldSet, opened, out, pending, promised, queued, readmit, registered, report, restarting, seen, seenInc, setGen, sid, splits, staged, staleCommit, stopped, successor, ver, wipedAt, wipes, writes, cpGone >>
      []          k = 44 ->
             /\ queued' = (a :> 0 @@ b :> 3 @@ c :> 0 @@ d :> 0)
             /\ writes' = 3
             /\ UNCHANGED << ackGen, acked, answered, ballot, bexpiry, bgen, cacked, canswered, cconfirmed, checked, chwm, clock, clog, confirmed, cpExpiry, cpLearner, cpNext, cpNextGen, cpView, cutOver, draining, expect, fenceIds, fencing, gaveUp, gen, halted, hbAt, hbOut, heard, hwm, inc, inflight, inherited, installed, joinedAt, joining, leader, learner, leaving, leftDirty, log, marked, mine, moves, named, now, oldSet, opened, out, pending, promised, readmit, registered, report, restarting, seen, seenInc, setGen, sid, splits, staged, staleCommit, stopped, successor, ver, wipedAt, wipes, cpGone >>
      []          k = 45 ->
             /\ bgen' = (a :> 9 @@ b :> 0 @@ c :> 10 @@ d :> 1)
             /\ queued' = (a :> 0 @@ b :> 0 @@ c :> 0 @@ d :> 0)
             /\ UNCHANGED << ackGen, acked, answered, ballot, bexpiry, cacked, canswered, cconfirmed, checked, chwm, clock, clog, confirmed, cpExpiry, cpLearner, cpNext, cpNextGen, cpView, cutOver, draining, expect, fenceIds, fencing, gaveUp, gen, halted, hbAt, hbOut, heard, hwm, inc, inflight, inherited, installed, joinedAt, joining, leader, learner, leaving, leftDirty, log, marked, mine, moves, named, now, oldSet, opened, out, pending, promised, readmit, registered, report, restarting, seen, seenInc, setGen, sid, splits, staged, staleCommit, stopped, successor, ver, wipedAt, wipes, writes, cpGone >>
      []          k = 46 ->
             /\ answered' = (a :> {c} @@ b :> {c} @@ c :> {b} @@ d :> {})
             /\ ballot' = (a :> {a} @@ b :> {c} @@ c :> {c} @@ d :> {d})
             /\ log' = (a :> << [g |-> 2, id |-> 0, lg |-> 2], [g |-> 3, id |-> 0, lg |-> 3], [g |-> 5, id |-> 0, lg |-> 5], [g |-> 9, id |-> 0, lg |-> 9] >> @@ b :> << [g |-> 2, id |-> 0, lg |-> 2], [g |-> 3, id |-> 0, lg |-> 3], [g |-> 5, id |-> 0, lg |-> 5], [g |-> 5, id |-> 2, lg |-> 5] >> @@ c :> << [g |-> 2, id |-> 0, lg |-> 2], [g |-> 3, id |-> 0, lg |-> 3], [g |-> 5, id |-> 0, lg |-> 5], [g |-> 5, id |-> 2, lg |-> 5] >> @@ d :> <<>>)
             /\ promised' = (a :> 9 @@ b :> 10 @@ c :> 10 @@ d :> 1)
             /\ UNCHANGED << ackGen, acked, bexpiry, bgen, cacked, canswered, cconfirmed, checked, chwm, clock, clog, confirmed, cpExpiry, cpLearner, cpNext, cpNextGen, cpView, cutOver, draining, expect, fenceIds, fencing, gaveUp, gen, halted, hbAt, hbOut, heard, hwm, inc, inflight, inherited, installed, joinedAt, joining, leader, learner, leaving, leftDirty, marked, mine, moves, named, now, oldSet, opened, out, pending, queued, readmit, registered, report, restarting, seen, seenInc, setGen, sid, splits, staged, staleCommit, stopped, successor, ver, wipedAt, wipes, writes, cpGone >>
      []          k = 47 ->
             /\ cpExpiry' = 6
             /\ gen' = 9
             /\ leader' = a
             /\ ver' = 8
             /\ UNCHANGED << ackGen, acked, answered, ballot, bexpiry, bgen, cacked, canswered, cconfirmed, checked, chwm, clock, clog, confirmed, cpLearner, cpNext, cpNextGen, cpView, cutOver, draining, expect, fenceIds, fencing, gaveUp, halted, hbAt, hbOut, heard, hwm, inc, inflight, inherited, installed, joinedAt, joining, learner, leaving, leftDirty, log, marked, mine, moves, named, now, oldSet, opened, out, pending, promised, queued, readmit, registered, report, restarting, seen, seenInc, setGen, sid, splits, staged, staleCommit, stopped, successor, wipedAt, wipes, writes, cpGone >>
      []          k = 48 ->
             /\ fencing' = (a :> FALSE @@ b :> FALSE @@ c :> FALSE @@ d :> TRUE)
             /\ log' = (a :> << [g |-> 2, id |-> 0, lg |-> 2], [g |-> 3, id |-> 0, lg |-> 3], [g |-> 5, id |-> 0, lg |-> 5], [g |-> 9, id |-> 0, lg |-> 9] >> @@ b :> << [g |-> 2, id |-> 0, lg |-> 2], [g |-> 3, id |-> 0, lg |-> 3], [g |-> 5, id |-> 0, lg |-> 5], [g |-> 5, id |-> 2, lg |-> 5] >> @@ c :> << [g |-> 2, id |-> 0, lg |-> 2], [g |-> 3, id |-> 0, lg |-> 3], [g |-> 5, id |-> 0, lg |-> 5], [g |-> 5, id |-> 2, lg |-> 5], [g |-> 10, id |-> 0, lg |-> 10] >> @@ d :> <<>>)
             /\ opened' = {<<1, a>>, <<2, a>>, <<3, b>>, <<5, b>>, <<9, a>>, <<10, c>>}
             /\ UNCHANGED << ackGen, acked, answered, ballot, bexpiry, bgen, cacked, canswered, cconfirmed, checked, chwm, clock, clog, confirmed, cpExpiry, cpLearner, cpNext, cpNextGen, cpView, cutOver, draining, expect, fenceIds, gaveUp, gen, halted, hbAt, hbOut, heard, hwm, inc, inflight, inherited, installed, joinedAt, joining, leader, learner, leaving, leftDirty, marked, mine, moves, named, now, oldSet, out, pending, promised, queued, readmit, registered, report, restarting, seen, seenInc, setGen, sid, splits, staged, staleCommit, stopped, successor, ver, wipedAt, wipes, writes, cpGone >>
      []          k = 49 ->
             /\ hbAt' = (a :> 3 @@ b :> 1 @@ c :> 2 @@ d :> 2)
             /\ hbOut' = (a :> TRUE @@ b :> TRUE @@ c :> FALSE @@ d :> TRUE)
             /\ UNCHANGED << ackGen, acked, answered, ballot, bexpiry, bgen, cacked, canswered, cconfirmed, checked, chwm, clock, clog, confirmed, cpExpiry, cpLearner, cpNext, cpNextGen, cpView, cutOver, draining, expect, fenceIds, fencing, gaveUp, gen, halted, heard, hwm, inc, inflight, inherited, installed, joinedAt, joining, leader, learner, leaving, leftDirty, log, marked, mine, moves, named, now, oldSet, opened, out, pending, promised, queued, readmit, registered, report, restarting, seen, seenInc, setGen, sid, splits, staged, staleCommit, stopped, successor, ver, wipedAt, wipes, writes, cpGone >>
      []          k = 50 ->
             /\ hbAt' = (a :> 3 @@ b :> 1 @@ c :> 5 @@ d :> 2)
             /\ hbOut' = (a :> TRUE @@ b :> TRUE @@ c :> TRUE @@ d :> TRUE)
             /\ UNCHANGED << ackGen, acked, answered, ballot, bexpiry, bgen, cacked, canswered, cconfirmed, checked, chwm, clock, clog, confirmed, cpExpiry, cpLearner, cpNext, cpNextGen, cpView, cutOver, draining, expect, fenceIds, fencing, gaveUp, gen, halted, heard, hwm, inc, inflight, inherited, installed, joinedAt, joining, leader, learner, leaving, leftDirty, log, marked, mine, moves, named, now, oldSet, opened, out, pending, promised, queued, readmit, registered, report, restarting, seen, seenInc, setGen, sid, splits, staged, staleCommit, stopped, successor, ver, wipedAt, wipes, writes, cpGone >>
      []          k = 51 ->
             /\ bgen' = (a :> 10 @@ b :> 0 @@ c :> 10 @@ d :> 1)
             /\ gen' = 10
             /\ joinedAt' = 3
             /\ joining' = {d}
             /\ leaving' = {c}
             /\ log' = (a :> << [g |-> 2, id |-> 0, lg |-> 2], [g |-> 3, id |-> 0, lg |-> 3], [g |-> 5, id |-> 0, lg |-> 5], [g |-> 9, id |-> 0, lg |-> 9], [g |-> 10, id |-> 0, lg |-> 10] >> @@ b :> << [g |-> 2, id |-> 0, lg |-> 2], [g |-> 3, id |-> 0, lg |-> 3], [g |-> 5, id |-> 0, lg |-> 5], [g |-> 5, id |-> 2, lg |-> 5] >> @@ c :> << [g |-> 2, id |-> 0, lg |-> 2], [g |-> 3, id |-> 0, lg |-> 3], [g |-> 5, id |-> 0, lg |-> 5], [g |-> 5, id |-> 2, lg |-> 5], [g |-> 10, id |-> 0, lg |-> 10] >> @@ d :> <<>>)
             /\ mine' = (a :> {a, b, c, d} @@ b :> {a, b, c} @@ c :> {a, b, c} @@ d :> {a, b})
             /\ moves' = 2
             /\ opened' = {<<1, a>>, <<2, a>>, <<3, b>>, <<5, b>>, <<9, a>>, <<10, a>>, <<10, c>>}
             /\ out' = {}
             /\ promised' = (a :> 10 @@ b :> 10 @@ c :> 10 @@ d :> 1)
             /\ ver' = 9
             /\ UNCHANGED << ackGen, acked, answered, ballot, bexpiry, cacked, canswered, cconfirmed, checked, chwm, clock, clog, confirmed, cpExpiry, cpLearner, cpNext, cpNextGen, cpView, cutOver, draining, expect, fenceIds, fencing, gaveUp, halted, hbAt, hbOut, heard, hwm, inc, inflight, inherited, installed, leader, learner, leftDirty, marked, named, now, oldSet, pending, queued, readmit, registered, report, restarting, seen, seenInc, setGen, sid, splits, staged, staleCommit, stopped, successor, wipedAt, wipes, writes, cpGone >>
      []          k = 52 ->
             /\ bgen' = (a :> 10 @@ b :> 0 @@ c :> 10 @@ d :> 0)
             /\ fencing' = (a :> FALSE @@ b :> FALSE @@ c :> FALSE @@ d :> FALSE)
             /\ UNCHANGED << ackGen, acked, answered, ballot, bexpiry, cacked, canswered, cconfirmed, checked, chwm, clock, clog, confirmed, cpExpiry, cpLearner, cpNext, cpNextGen, cpView, cutOver, draining, expect, fenceIds, gaveUp, gen, halted, hbAt, hbOut, heard, hwm, inc, inflight, inherited, installed, joinedAt, joining, leader, learner, leaving, leftDirty, log, marked, mine, moves, named, now, oldSet, opened, out, pending, promised, queued, readmit, registered, report, restarting, seen, seenInc, setGen, sid, splits, staged, staleCommit, stopped, successor, ver, wipedAt, wipes, writes, cpGone >>
      []          k = 53 ->
             /\ hbOut' = (a :> FALSE @@ b :> TRUE @@ c :> TRUE @@ d :> TRUE)
             /\ UNCHANGED << ackGen, acked, answered, ballot, bexpiry, bgen, cacked, canswered, cconfirmed, checked, chwm, clock, clog, confirmed, cpExpiry, cpLearner, cpNext, cpNextGen, cpView, cutOver, draining, expect, fenceIds, fencing, gaveUp, gen, halted, hbAt, heard, hwm, inc, inflight, inherited, installed, joinedAt, joining, leader, learner, leaving, leftDirty, log, marked, mine, moves, named, now, oldSet, opened, out, pending, promised, queued, readmit, registered, report, restarting, seen, seenInc, setGen, sid, splits, staged, staleCommit, stopped, successor, ver, wipedAt, wipes, writes, cpGone >>
      []          k = 54 ->
             /\ queued' = (a :> 0 @@ b :> 0 @@ c :> 4 @@ d :> 0)
             /\ writes' = 4
             /\ UNCHANGED << ackGen, acked, answered, ballot, bexpiry, bgen, cacked, canswered, cconfirmed, checked, chwm, clock, clog, confirmed, cpExpiry, cpLearner, cpNext, cpNextGen, cpView, cutOver, draining, expect, fenceIds, fencing, gaveUp, gen, halted, hbAt, hbOut, heard, hwm, inc, inflight, inherited, installed, joinedAt, joining, leader, learner, leaving, leftDirty, log, marked, mine, moves, named, now, oldSet, opened, out, pending, promised, readmit, registered, report, restarting, seen, seenInc, setGen, sid, splits, staged, staleCommit, stopped, successor, ver, wipedAt, wipes, cpGone >>
      []          k = 55 ->
             /\ bexpiry' = (a :> 4 @@ b :> 3 @@ c :> 6 @@ d :> 6)
             /\ bgen' = (a :> 10 @@ b :> 0 @@ c :> 10 @@ d :> 2)
             /\ fencing' = (a :> FALSE @@ b :> FALSE @@ c :> FALSE @@ d :> TRUE)
             /\ promised' = (a :> 10 @@ b :> 10 @@ c :> 10 @@ d :> 2)
             /\ UNCHANGED << ackGen, acked, answered, ballot, cacked, canswered, cconfirmed, checked, chwm, clock, clog, confirmed, cpExpiry, cpLearner, cpNext, cpNextGen, cpView, cutOver, draining, expect, fenceIds, gaveUp, gen, halted, hbAt, hbOut, heard, hwm, inc, inflight, inherited, installed, joinedAt, joining, leader, learner, leaving, leftDirty, log, marked, mine, moves, named, now, oldSet, opened, out, pending, queued, readmit, registered, report, restarting, seen, seenInc, setGen, sid, splits, staged, staleCommit, stopped, successor, ver, wipedAt, wipes, writes, cpGone >>
      []          k = 56 ->
             /\ bgen' = (a :> 10 @@ b :> 0 @@ c :> 10 @@ d :> 0)
             /\ fencing' = (a :> FALSE @@ b :> FALSE @@ c :> FALSE @@ d :> FALSE)
             /\ UNCHANGED << ackGen, acked, answered, ballot, bexpiry, cacked, canswered, cconfirmed, checked, chwm, clock, clog, confirmed, cpExpiry, cpLearner, cpNext, cpNextGen, cpView, cutOver, draining, expect, fenceIds, gaveUp, gen, halted, hbAt, hbOut, heard, hwm, inc, inflight, inherited, installed, joinedAt, joining, leader, learner, leaving, leftDirty, log, marked, mine, moves, named, now, oldSet, opened, out, pending, promised, queued, readmit, registered, report, restarting, seen, seenInc, setGen, sid, splits, staged, staleCommit, stopped, successor, ver, wipedAt, wipes, writes, cpGone >>
      []          k = 57 ->
             /\ hbOut' = (a :> FALSE @@ b :> FALSE @@ c :> TRUE @@ d :> TRUE)
             /\ UNCHANGED << ackGen, acked, answered, ballot, bexpiry, bgen, cacked, canswered, cconfirmed, checked, chwm, clock, clog, confirmed, cpExpiry, cpLearner, cpNext, cpNextGen, cpView, cutOver, draining, expect, fenceIds, fencing, gaveUp, gen, halted, hbAt, heard, hwm, inc, inflight, inherited, installed, joinedAt, joining, leader, learner, leaving, leftDirty, log, marked, mine, moves, named, now, oldSet, opened, out, pending, promised, queued, readmit, registered, report, restarting, seen, seenInc, setGen, sid, splits, staged, staleCommit, stopped, successor, ver, wipedAt, wipes, writes, cpGone >>
      []          k = 58 ->
             /\ pending' = (a :> 0 @@ b :> 0 @@ c :> 4 @@ d :> 0)
             /\ queued' = (a :> 0 @@ b :> 0 @@ c :> 0 @@ d :> 0)
             /\ UNCHANGED << ackGen, acked, answered, ballot, bexpiry, bgen, cacked, canswered, cconfirmed, checked, chwm, clock, clog, confirmed, cpExpiry, cpLearner, cpNext, cpNextGen, cpView, cutOver, draining, expect, fenceIds, fencing, gaveUp, gen, halted, hbAt, hbOut, heard, hwm, inc, inflight, inherited, installed, joinedAt, joining, leader, learner, leaving, leftDirty, log, marked, mine, moves, named, now, oldSet, opened, out, promised, readmit, registered, report, restarting, seen, seenInc, setGen, sid, splits, staged, staleCommit, stopped, successor, ver, wipedAt, wipes, writes, cpGone >>
      []          k = 59 ->
             /\ queued' = (a :> 5 @@ b :> 0 @@ c :> 0 @@ d :> 0)
             /\ writes' = 5
             /\ UNCHANGED << ackGen, acked, answered, ballot, bexpiry, bgen, cacked, canswered, cconfirmed, checked, chwm, clock, clog, confirmed, cpExpiry, cpLearner, cpNext, cpNextGen, cpView, cutOver, draining, expect, fenceIds, fencing, gaveUp, gen, halted, hbAt, hbOut, heard, hwm, inc, inflight, inherited, installed, joinedAt, joining, leader, learner, leaving, leftDirty, log, marked, mine, moves, named, now, oldSet, opened, out, pending, promised, readmit, registered, report, restarting, seen, seenInc, setGen, sid, splits, staged, staleCommit, stopped, successor, ver, wipedAt, wipes, cpGone >>
      []          k = 60 ->
             /\ hbOut' = (a :> TRUE @@ b :> FALSE @@ c :> TRUE @@ d :> TRUE)
             /\ UNCHANGED << ackGen, acked, answered, ballot, bexpiry, bgen, cacked, canswered, cconfirmed, checked, chwm, clock, clog, confirmed, cpExpiry, cpLearner, cpNext, cpNextGen, cpView, cutOver, draining, expect, fenceIds, fencing, gaveUp, gen, halted, hbAt, heard, hwm, inc, inflight, inherited, installed, joinedAt, joining, leader, learner, leaving, leftDirty, log, marked, mine, moves, named, now, oldSet, opened, out, pending, promised, queued, readmit, registered, report, restarting, seen, seenInc, setGen, sid, splits, staged, staleCommit, stopped, successor, ver, wipedAt, wipes, writes, cpGone >>
      []          k = 61 ->
             /\ confirmed' = (a :> (a :> 0 @@ b :> 0 @@ c :> 0 @@ d :> 0) @@ b :> (a :> 0 @@ b :> 0 @@ c :> 0 @@ d :> 0) @@ c :> (a :> 0 @@ b :> 5 @@ c :> 0 @@ d :> 0) @@ d :> (a :> 0 @@ b :> 0 @@ c :> 0 @@ d :> 0))
             /\ log' = (a :> << [g |-> 2, id |-> 0, lg |-> 2], [g |-> 3, id |-> 0, lg |-> 3], [g |-> 5, id |-> 0, lg |-> 5], [g |-> 9, id |-> 0, lg |-> 9], [g |-> 10, id |-> 0, lg |-> 10] >> @@ b :> << [g |-> 2, id |-> 0, lg |-> 2], [g |-> 3, id |-> 0, lg |-> 3], [g |-> 5, id |-> 0, lg |-> 5], [g |-> 5, id |-> 2, lg |-> 5], [g |-> 10, id |-> 0, lg |-> 10] >> @@ c :> << [g |-> 2, id |-> 0, lg |-> 2], [g |-> 3, id |-> 0, lg |-> 3], [g |-> 5, id |-> 0, lg |-> 5], [g |-> 5, id |-> 2, lg |-> 5], [g |-> 10, id |-> 0, lg |-> 10] >> @@ d :> <<>>)
             /\ UNCHANGED << ackGen, acked, answered, ballot, bexpiry, bgen, cacked, canswered, cconfirmed, checked, chwm, clock, clog, cpExpiry, cpLearner, cpNext, cpNextGen, cpView, cutOver, draining, expect, fenceIds, fencing, gaveUp, gen, halted, hbAt, hbOut, heard, hwm, inc, inflight, inherited, installed, joinedAt, joining, leader, learner, leaving, leftDirty, marked, mine, moves, named, now, oldSet, opened, out, pending, promised, queued, readmit, registered, report, restarting, seen, seenInc, setGen, sid, splits, staged, staleCommit, stopped, successor, ver, wipedAt, wipes, writes, cpGone >>
      []          k = 62 ->
             /\ queued' = (a :> 5 @@ b :> 0 @@ c :> 6 @@ d :> 0)
             /\ writes' = 6
             /\ UNCHANGED << ackGen, acked, answered, ballot, bexpiry, bgen, cacked, canswered, cconfirmed, checked, chwm, clock, clog, confirmed, cpExpiry, cpLearner, cpNext, cpNextGen, cpView, cutOver, draining, expect, fenceIds, fencing, gaveUp, gen, halted, hbAt, hbOut, heard, hwm, inc, inflight, inherited, installed, joinedAt, joining, leader, learner, leaving, leftDirty, log, marked, mine, moves, named, now, oldSet, opened, out, pending, promised, readmit, registered, report, restarting, seen, seenInc, setGen, sid, splits, staged, staleCommit, stopped, successor, ver, wipedAt, wipes, cpGone >>
      []          k = 63 ->
             /\ clock' = (a :> 5 @@ b :> 4 @@ c :> 5 @@ d :> 5)
             /\ now' = 5
             /\ UNCHANGED << ackGen, acked, answered, ballot, bexpiry, bgen, cacked, canswered, cconfirmed, checked, chwm, clog, confirmed, cpExpiry, cpLearner, cpNext, cpNextGen, cpView, cutOver, draining, expect, fenceIds, fencing, gaveUp, gen, halted, hbAt, hbOut, heard, hwm, inc, inflight, inherited, installed, joinedAt, joining, leader, learner, leaving, leftDirty, log, marked, mine, moves, named, oldSet, opened, out, pending, promised, queued, readmit, registered, report, restarting, seen, seenInc, setGen, sid, splits, staged, staleCommit, stopped, successor, ver, wipedAt, wipes, writes, cpGone >>
      []          k = 64 ->
             /\ pending' = (a :> 5 @@ b :> 0 @@ c :> 4 @@ d :> 0)
             /\ queued' = (a :> 0 @@ b :> 0 @@ c :> 6 @@ d :> 0)
             /\ UNCHANGED << ackGen, acked, answered, ballot, bexpiry, bgen, cacked, canswered, cconfirmed, checked, chwm, clock, clog, confirmed, cpExpiry, cpLearner, cpNext, cpNextGen, cpView, cutOver, draining, expect, fenceIds, fencing, gaveUp, gen, halted, hbAt, hbOut, heard, hwm, inc, inflight, inherited, installed, joinedAt, joining, leader, learner, leaving, leftDirty, log, marked, mine, moves, named, now, oldSet, opened, out, promised, readmit, registered, report, restarting, seen, seenInc, setGen, sid, splits, staged, staleCommit, stopped, successor, ver, wipedAt, wipes, writes, cpGone >>
      []          k = 65 ->
             /\ hbOut' = (a :> FALSE @@ b :> FALSE @@ c :> TRUE @@ d :> TRUE)
             /\ UNCHANGED << ackGen, acked, answered, ballot, bexpiry, bgen, cacked, canswered, cconfirmed, checked, chwm, clock, clog, confirmed, cpExpiry, cpLearner, cpNext, cpNextGen, cpView, cutOver, draining, expect, fenceIds, fencing, gaveUp, gen, halted, hbAt, heard, hwm, inc, inflight, inherited, installed, joinedAt, joining, leader, learner, leaving, leftDirty, log, marked, mine, moves, named, now, oldSet, opened, out, pending, promised, queued, readmit, registered, report, restarting, seen, seenInc, setGen, sid, splits, staged, staleCommit, stopped, successor, ver, wipedAt, wipes, writes, cpGone >>
      []          k = 66 ->
             /\ ackGen' = {<<2, 10>>}
             /\ acked' = {2}
             /\ hwm' = (a :> 0 @@ b :> 0 @@ c :> 5 @@ d :> 0)
             /\ UNCHANGED << answered, ballot, bexpiry, bgen, cacked, canswered, cconfirmed, checked, chwm, clock, clog, confirmed, cpExpiry, cpLearner, cpNext, cpNextGen, cpView, cutOver, draining, expect, fenceIds, fencing, gaveUp, gen, halted, hbAt, hbOut, heard, inc, inflight, inherited, installed, joinedAt, joining, leader, learner, leaving, leftDirty, log, marked, mine, moves, named, now, oldSet, opened, out, pending, promised, queued, readmit, registered, report, restarting, seen, seenInc, setGen, sid, splits, staged, staleCommit, stopped, successor, ver, wipedAt, wipes, writes, cpGone >>

ReplayNext == step < 66 /\ Next /\ step' = step + 1 /\ Step(step + 1)

====
