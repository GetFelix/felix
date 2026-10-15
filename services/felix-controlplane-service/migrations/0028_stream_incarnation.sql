-- Which creation of its name a stream is. 0 is a stream created before the
-- fleet finalized `stream_incarnations`, whose shard directories on the
-- brokers keep the names they always had.
ALTER TABLE streams
    ADD COLUMN IF NOT EXISTS incarnation BIGINT NOT NULL DEFAULT 0;

-- The incarnation of the stream a shard assignment is for, written with the
-- assignment, so a change replayed after the stream was created anew still
-- names the stream it was written for.
ALTER TABLE shard_assignments
    ADD COLUMN IF NOT EXISTS incarnation BIGINT NOT NULL DEFAULT 0;

-- For each deleted stream name, the incarnation through which it is deleted:
-- every incarnation of the name up to it. Written only once incarnations are
-- finalized, one row per name, kept for good. Brokers delete a stream's data
-- only on this word, from the snapshot or from a change's `deleted_through`.
CREATE TABLE IF NOT EXISTS stream_tombstones (
    tenant_id TEXT NOT NULL,
    namespace TEXT NOT NULL,
    stream TEXT NOT NULL,
    incarnation BIGINT NOT NULL,
    PRIMARY KEY (tenant_id, namespace, stream)
);

ALTER TABLE stream_changes
    ADD COLUMN IF NOT EXISTS deleted_through BIGINT;

-- The rules below live in the database rather than the control plane's code,
-- so they hold for every instance writing to it, including one that predates
-- them during a rolling upgrade.
--
-- Whether incarnations are finalized, read under a shared lock the finalize
-- takes exclusively (below): a write that read "not finalized" commits before
-- the finalize does, so nothing is created at 0 after it. The key is "felixinc".
CREATE OR REPLACE FUNCTION felix_stream_incarnations_finalized() RETURNS boolean
LANGUAGE plpgsql AS $$
BEGIN
    PERFORM pg_advisory_xact_lock_shared(7378422764763967075);
    RETURN EXISTS (SELECT 1 FROM fleet_features WHERE feature = 'stream_incarnations');
END;
$$;

CREATE OR REPLACE FUNCTION felix_stream_incarnations_finalize_lock() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    PERFORM pg_advisory_xact_lock(7378422764763967075);
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS fleet_features_stream_incarnations ON fleet_features;
CREATE TRIGGER fleet_features_stream_incarnations
    AFTER INSERT ON fleet_features
    FOR EACH ROW WHEN (NEW.feature = 'stream_incarnations')
    EXECUTE FUNCTION felix_stream_incarnations_finalize_lock();

-- A new stream's incarnation. Before the finalize, 0. After it, the proposed
-- one raised above the name's tombstone, so a name's incarnations only
-- increase. A proposal of 0 comes from an instance that predates
-- incarnations, and is refused rather than creating a stream at 0.
CREATE OR REPLACE FUNCTION felix_stream_incarnation() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE
    deleted BIGINT;
BEGIN
    IF NOT felix_stream_incarnations_finalized() THEN
        NEW.incarnation := 0;
        RETURN NEW;
    END IF;
    IF NEW.incarnation = 0 THEN
        RAISE EXCEPTION 'stream_incarnations is finalized: only a control plane that gives incarnations may create streams'
            USING ERRCODE = 'check_violation';
    END IF;
    SELECT incarnation INTO deleted FROM stream_tombstones
        WHERE tenant_id = NEW.tenant_id AND namespace = NEW.namespace AND stream = NEW.stream;
    NEW.incarnation := GREATEST(NEW.incarnation, COALESCE(deleted + 1, 1));
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS streams_incarnation ON streams;
CREATE TRIGGER streams_incarnation
    BEFORE INSERT ON streams
    FOR EACH ROW EXECUTE FUNCTION felix_stream_incarnation();

-- A deleted stream's tombstone, for every delete including a cascade from its
-- namespace or tenant. None before the finalize: a stream created again at 0
-- then still reopens the deleted one's logs, so its data must stay.
CREATE OR REPLACE FUNCTION felix_stream_tombstone() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF felix_stream_incarnations_finalized() THEN
        INSERT INTO stream_tombstones (tenant_id, namespace, stream, incarnation)
            VALUES (OLD.tenant_id, OLD.namespace, OLD.stream, OLD.incarnation)
            ON CONFLICT (tenant_id, namespace, stream)
            DO UPDATE SET incarnation = GREATEST(stream_tombstones.incarnation, EXCLUDED.incarnation);
    END IF;
    RETURN OLD;
END;
$$;

DROP TRIGGER IF EXISTS streams_tombstone ON streams;
CREATE TRIGGER streams_tombstone
    AFTER DELETE ON streams
    FOR EACH ROW EXECUTE FUNCTION felix_stream_tombstone();

-- A `Deleted` change names the tombstone its delete just wrote. Every writer
-- deletes the row before it records the change, in the same transaction.
CREATE OR REPLACE FUNCTION felix_stream_change_deleted_through() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.op = 'Deleted' AND NEW.deleted_through IS NULL
        AND felix_stream_incarnations_finalized() THEN
        SELECT incarnation INTO NEW.deleted_through FROM stream_tombstones
            WHERE tenant_id = NEW.tenant_id AND namespace = NEW.namespace AND stream = NEW.stream;
    END IF;
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS stream_changes_deleted_through ON stream_changes;
CREATE TRIGGER stream_changes_deleted_through
    BEFORE INSERT ON stream_changes
    FOR EACH ROW EXECUTE FUNCTION felix_stream_change_deleted_through();
