---------------------------- MODULE FelixRaftFirstForm ----------------------------
(* How a Raft shard's group forms for the first time, with placement able   *)
(* to move before it has (issue 2794). The control plane stores a target    *)
(* for the set it places. Each broker forms only from its own view of       *)
(* placement, which may be stale: the lowest broker of the set it sees asks *)
(* every member of that set, and initializes with that set when each one   *)
(* is blank and the set is the target it sees. A blank member votes for    *)
(* any candidate of term 1. A group's leader needs a majority of the set   *)
(* it initialized with.                                                    *)
(*                                                                         *)
(* Hold = TRUE is the fix: the control plane refuses to move a set whose    *)
(* target is stored until a leader of the group has reported. Hold = FALSE  *)
(* finds two leaders of two different sets in term 1.                       *)
EXTENDS FiniteSets, Naturals

CONSTANTS Brokers, Sets, Hold

ASSUME Sets \subseteq SUBSET Brokers

VARIABLES
    placed,     \* the set the control plane places
    target,     \* the set its stored target names, or {} for none
    reported,   \* a leader of the group has reported to the control plane
    view,       \* each broker's last read of [set, target]
    init,       \* the set each broker initialized with, or {} if blank
    votedFor,   \* each broker's term-1 vote, or 0 for none
    leader      \* the brokers that won term 1

vars == <<placed, target, reported, view, init, votedFor, leader>>

Min(S) == CHOOSE b \in S : \A c \in S : b <= c
Majority(S, T) == 2 * Cardinality(S \cap T) > Cardinality(S)
Blank(b) == init[b] = {} /\ votedFor[b] = 0

Init ==
    /\ placed \in Sets
    /\ target = {}
    /\ reported = FALSE
    /\ view = [b \in Brokers |-> [set |-> placed, target |-> {}]]
    /\ init = [b \in Brokers |-> {}]
    /\ votedFor = [b \in Brokers |-> 0]
    /\ leader = {}

\* Every member of the placed set has named its copy.
StoreTarget ==
    /\ target # placed
    /\ target' = placed
    /\ UNCHANGED <<placed, reported, view, init, votedFor, leader>>

\* Placement moves before the group forms. A move drops the stored target:
\* the new set needs its own.
Move(s) ==
    /\ s # placed
    /\ ~Hold \/ target = {} \/ reported
    /\ placed' = s
    /\ target' = {}
    /\ UNCHANGED <<reported, view, init, votedFor, leader>>

\* A broker reads the feed. One that does not is cut off and keeps its view.
Read(b) ==
    /\ view' = [view EXCEPT ![b] = [set |-> placed, target |-> target]]
    /\ UNCHANGED <<placed, target, reported, init, votedFor, leader>>

\* The ask and the initialize, with the check against its own view.
Form(b) ==
    LET s == view[b].set IN
    /\ b \in s
    /\ b = Min(s)
    /\ view[b].target = s
    /\ \A m \in s : Blank(m)
    /\ init' = [init EXCEPT ![b] = s]
    /\ UNCHANGED <<placed, target, reported, view, votedFor, leader>>

\* A blank member's vote is not gated on any target.
Vote(v, c) ==
    /\ init[c] # {}
    /\ votedFor[v] = 0
    /\ v \in init[c]
    /\ votedFor' = [votedFor EXCEPT ![v] = c]
    /\ UNCHANGED <<placed, target, reported, view, init, leader>>

Win(c) ==
    /\ init[c] # {}
    /\ c \notin leader
    /\ Majority(init[c], {v \in Brokers : votedFor[v] = c})
    /\ leader' = leader \cup {c}
    /\ UNCHANGED <<placed, target, reported, view, init, votedFor>>

\* A leader reports a floor; only a broker the target names is counted.
Report(c) ==
    /\ c \in leader
    /\ target # {}
    /\ c \in target
    /\ reported' = TRUE
    /\ UNCHANGED <<placed, target, view, init, votedFor, leader>>

Next ==
    \/ StoreTarget
    \/ \E s \in Sets : Move(s)
    \/ \E b \in Brokers : Read(b) \/ Form(b) \/ Win(b) \/ Report(b)
    \/ \E v, c \in Brokers : Vote(v, c)

Spec == Init /\ [][Next]_vars

TypeOK ==
    /\ placed \in Sets
    /\ target \in Sets \cup {{}}
    /\ reported \in BOOLEAN
    /\ leader \subseteq Brokers
    /\ votedFor \in [Brokers -> Brokers \cup {0}]

\* Two brokers lead term 1 only if they initialized the same set, which is
\* one group: Raft's single vote per term keeps that to one leader.
OneGroup == \A a, b \in leader : init[a] = init[b]

\* Every formed member formed the same set.
OneFirstSet == \A a, b \in Brokers : init[a] # {} /\ init[b] # {} => init[a] = init[b]
==================================================================================
