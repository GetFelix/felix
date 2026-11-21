---------------------------- MODULE FelixRaftFormedSet ----------------------------
(* How a formed Raft group's set changes (issue 2905). Placement moves the *)
(* set, each placed broker reports the copy it opened, and the control     *)
(* plane stores a target naming those copies. The group's leader, while a *)
(* majority of its voters live, joins the target: the copies it names     *)
(* become voters, and a majority of them get the group's log; the rest    *)
(* can stay empty. Blank0 is the minority of the first voters that the    *)
(* first membership committed without. A slow or dead leader never joins, *)
(* and the control plane's voter report can lag the group.                *)
(*                                                                         *)
(* A broker's copy can be wiped. It reopens empty under the next copy id,  *)
(* and the old copy votes for nothing again.                               *)
(*                                                                         *)
(* A placed set whose copies are all empty and all named by the target is *)
(* what the first form initializes (FelixRaftFirstForm.tla): here that is  *)
(* a second group beside the formed one, at the same incarnation and       *)
(* epoch. Re-forming in a new epoch is the only sanctioned way to start a  *)
(* new group, and is not modeled.                                          *)
(*                                                                         *)
(* Hold = TRUE is the fix: a move and a target each keep at least half of  *)
(* the new set among the reported voters (Rule = "half"). Rule = "one"     *)
(* keeps only one, which an empty voter defeats. ByCopy = TRUE counts a    *)
(* broker only under the copy the report names, as the code does; FALSE   *)
(* counts brokers, which a wiped voter defeats.                            *)
EXTENDS FiniteSets, Naturals

CONSTANTS Brokers, Sets, Init0, Blank0, Hold, Rule, ByCopy, MaxWipes

ASSUME Sets \subseteq SUBSET Brokers
ASSUME Init0 \in Sets
ASSUME Blank0 \subseteq Init0 /\ 2 * Cardinality(Blank0) < Cardinality(Init0)
ASSUME Rule \in {"half", "one"}

Copies == 0..MaxWipes
None == MaxWipes + 1

VARIABLES
    placed,    \* the set the control plane places
    copy,      \* each broker's current copy id
    data,      \* what each broker's current copy holds: "blank", "g1" or "g2"
    voters,    \* the formed group's committed voters, <<broker, copy>> pairs
    reported,  \* the voters the control plane last heard from a leader
    cpCopy,    \* the copy id the control plane has for each placed broker
    target,    \* the pairs the stored target names, or {}
    wipes      \* wipes so far

vars == <<placed, copy, data, voters, reported, cpCopy, target, wipes>>

Live(p) == copy[p[1]] = p[2] /\ data[p[1]] = "g1"
Open(p) == copy[p[1]] = p[2] /\ data[p[1]] # "g2"
Majority(S, P(_)) == 2 * Cardinality({p \in S : P(p)}) > Cardinality(S)
Counted(p) == IF ByCopy THEN p \in reported ELSE \E c \in Copies : <<p[1], c>> \in reported
KeepsHalf(n, k) == IF Rule = "half" THEN 2 * k >= n ELSE k >= 1

\* A move counts only brokers of the current set, under the copy it has.
MoveKeeps(s) ==
    KeepsHalf(Cardinality(s),
              Cardinality({b \in s \cap placed : cpCopy[b] # None /\ Counted(<<b, cpCopy[b]>>)}))
TargetKeeps(t) == KeepsHalf(Cardinality(t), Cardinality({p \in t : Counted(p)}))

Init ==
    /\ placed = Init0
    /\ copy = [b \in Brokers |-> 0]
    /\ data = [b \in Brokers |-> IF b \in Init0 \ Blank0 THEN "g1" ELSE "blank"]
    /\ voters = {<<b, 0>> : b \in Init0}
    /\ reported = voters
    /\ cpCopy = [b \in Brokers |-> IF b \in Init0 THEN 0 ELSE None]
    /\ target = voters
    /\ wipes = 0

Move(s) ==
    /\ s # placed
    /\ Hold => MoveKeeps(s)
    /\ placed' = s
    /\ cpCopy' = [b \in Brokers |-> IF b \in s \cap placed THEN cpCopy[b] ELSE None]
    /\ target' = {}
    /\ UNCHANGED <<copy, data, voters, reported, wipes>>

ReportCopy(b) ==
    /\ b \in placed
    /\ cpCopy[b] # copy[b]
    /\ cpCopy' = [cpCopy EXCEPT ![b] = copy[b]]
    /\ UNCHANGED <<placed, copy, data, voters, reported, target, wipes>>

\* A refused target leaves the stored one as it was.
StoreTarget ==
    LET t == {<<b, cpCopy[b]>> : b \in placed}
    IN  /\ \A b \in placed : cpCopy[b] # None
        /\ t # target
        /\ Hold => TargetKeeps(t)
        /\ target' = t
        /\ UNCHANGED <<placed, copy, data, voters, reported, cpCopy, wipes>>

\* The leader commits the target: a majority of the old voters and the
\* copies J, a majority of the target's, take part and get the group's log.
\* The target's other copies become voters as they are, possibly empty.
Join(J) ==
    /\ target # {}
    /\ target # voters
    /\ Majority(voters, Live)
    /\ J \subseteq target
    /\ \A p \in J : Open(p)
    /\ Majority(target, LAMBDA p : p \in J)
    /\ voters' = target
    /\ data' = [b \in Brokers |-> IF <<b, copy[b]>> \in J THEN "g1" ELSE data[b]]
    /\ UNCHANGED <<placed, copy, reported, cpCopy, target, wipes>>

Report ==
    /\ Majority(voters, Live)
    /\ reported' = voters
    /\ UNCHANGED <<placed, copy, data, voters, cpCopy, target, wipes>>

Wipe(b) ==
    /\ wipes < MaxWipes
    /\ wipes' = wipes + 1
    /\ copy' = [copy EXCEPT ![b] = copy[b] + 1]
    /\ data' = [data EXCEPT ![b] = "blank"]
    /\ UNCHANGED <<placed, voters, reported, cpCopy, target>>

\* The first form, on a set it should never see: every member empty and
\* named by the stored target under the copy it has.
Form ==
    /\ target # {}
    /\ {p[1] : p \in target} = placed
    /\ \A p \in target : copy[p[1]] = p[2] /\ data[p[1]] = "blank"
    /\ data' = [b \in Brokers |-> IF b \in placed THEN "g2" ELSE data[b]]
    /\ UNCHANGED <<placed, copy, voters, reported, cpCopy, target, wipes>>

Next ==
    \/ \E s \in Sets : Move(s)
    \/ \E b \in Brokers : ReportCopy(b) \/ Wipe(b)
    \/ \E J \in SUBSET target : Join(J)
    \/ StoreTarget \/ Report \/ Form

Spec == Init /\ [][Next]_vars

Pairs == Brokers \X (Copies \cup {None})

TypeOK ==
    /\ placed \in Sets
    /\ copy \in [Brokers -> Copies]
    /\ data \in [Brokers -> {"blank", "g1", "g2"}]
    /\ voters \subseteq Pairs /\ reported \subseteq Pairs /\ target \subseteq Pairs
    /\ cpCopy \in [Brokers -> Copies \cup {None}]

\* One group per shard, incarnation and epoch: no set of empty copies ever
\* forms beside the formed one.
OneGroup == \A b \in Brokers : data[b] # "g2"
==================================================================================
