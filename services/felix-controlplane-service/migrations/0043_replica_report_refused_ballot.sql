-- A newer ballot the refusing leader's own copy holds for another node (#1951).
-- Placement names the leader again one past it rather than one up. NULL is
-- what every row held before this, and is read as none.
--
-- refused_ballot_at is the marker, as refused_at is for the refusal (0042): the
-- ballot is read only while it matches reported_at_millis, so a ballot left
-- behind by an earlier report is never read with a refusal written by a member
-- that predates the columns.
ALTER TABLE replica_reports
    ADD COLUMN IF NOT EXISTS refused_ballot BIGINT,
    ADD COLUMN IF NOT EXISTS refused_ballot_at BIGINT;
