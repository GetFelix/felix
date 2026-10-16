-- Which creation of its name a cache is, as streams have since 0028. 0 is a
-- cache created before the fleet finalized `cache_incarnations`, whose
-- directories on the brokers keep the names they always had.
ALTER TABLE caches
    ADD COLUMN IF NOT EXISTS incarnation BIGINT NOT NULL DEFAULT 0;

-- For each deleted cache name, the incarnation through which it is deleted.
-- Written only once cache incarnations are finalized, one row per name, kept
-- for good. Brokers delete a cache's data only on this word.
CREATE TABLE IF NOT EXISTS cache_tombstones (
    tenant_id TEXT NOT NULL,
    namespace TEXT NOT NULL,
    cache TEXT NOT NULL,
    incarnation BIGINT NOT NULL,
    PRIMARY KEY (tenant_id, namespace, cache)
);

ALTER TABLE cache_changes
    ADD COLUMN IF NOT EXISTS deleted_through BIGINT;

-- In the database for the reason 0028 gives: every instance writing to it
-- follows these rules, including one that predates them.
--
-- Whether cache incarnations are finalized, read under a shared lock the
-- finalize takes exclusively (below), so nothing is created at 0 after it.
-- Its own key ("felixcai"), so a cache write does not wait on the streams'.
CREATE OR REPLACE FUNCTION felix_cache_incarnations_finalized() RETURNS boolean
LANGUAGE plpgsql AS $$
BEGIN
    PERFORM pg_advisory_xact_lock_shared(7378422764763570537);
    RETURN EXISTS (SELECT 1 FROM fleet_features WHERE feature = 'cache_incarnations');
END;
$$;

CREATE OR REPLACE FUNCTION felix_cache_incarnations_finalize_lock() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    PERFORM pg_advisory_xact_lock(7378422764763570537);
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS fleet_features_cache_incarnations ON fleet_features;
CREATE TRIGGER fleet_features_cache_incarnations
    AFTER INSERT ON fleet_features
    FOR EACH ROW WHEN (NEW.feature = 'cache_incarnations')
    EXECUTE FUNCTION felix_cache_incarnations_finalize_lock();

-- A new cache's incarnation. Before the finalize, 0. After it, the proposed
-- one raised above the name's tombstone. A proposal of 0 comes from an
-- instance that predates cache incarnations, and is refused rather than
-- creating a cache at 0.
CREATE OR REPLACE FUNCTION felix_cache_incarnation() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE
    deleted BIGINT;
BEGIN
    IF NOT felix_cache_incarnations_finalized() THEN
        NEW.incarnation := 0;
        RETURN NEW;
    END IF;
    IF NEW.incarnation = 0 THEN
        RAISE EXCEPTION 'cache_incarnations is finalized: only a control plane that gives incarnations may create caches'
            USING ERRCODE = 'check_violation';
    END IF;
    SELECT incarnation INTO deleted FROM cache_tombstones
        WHERE tenant_id = NEW.tenant_id AND namespace = NEW.namespace AND cache = NEW.cache;
    NEW.incarnation := GREATEST(NEW.incarnation, COALESCE(deleted + 1, 1));
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS caches_incarnation ON caches;
CREATE TRIGGER caches_incarnation
    BEFORE INSERT ON caches
    FOR EACH ROW EXECUTE FUNCTION felix_cache_incarnation();

-- A deleted cache's tombstone, for every delete including a cascade from its
-- namespace or tenant. None before the finalize.
CREATE OR REPLACE FUNCTION felix_cache_tombstone() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF felix_cache_incarnations_finalized() THEN
        INSERT INTO cache_tombstones (tenant_id, namespace, cache, incarnation)
            VALUES (OLD.tenant_id, OLD.namespace, OLD.cache, OLD.incarnation)
            ON CONFLICT (tenant_id, namespace, cache)
            DO UPDATE SET incarnation = GREATEST(cache_tombstones.incarnation, EXCLUDED.incarnation);
    END IF;
    RETURN OLD;
END;
$$;

DROP TRIGGER IF EXISTS caches_tombstone ON caches;
CREATE TRIGGER caches_tombstone
    AFTER DELETE ON caches
    FOR EACH ROW EXECUTE FUNCTION felix_cache_tombstone();

-- A `Deleted` change names the tombstone its delete just wrote. Every writer
-- deletes the row before it records the change, in the same transaction.
CREATE OR REPLACE FUNCTION felix_cache_change_deleted_through() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.op = 'Deleted' AND NEW.deleted_through IS NULL
        AND felix_cache_incarnations_finalized() THEN
        SELECT incarnation INTO NEW.deleted_through FROM cache_tombstones
            WHERE tenant_id = NEW.tenant_id AND namespace = NEW.namespace AND cache = NEW.cache;
    END IF;
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS cache_changes_deleted_through ON cache_changes;
CREATE TRIGGER cache_changes_deleted_through
    BEFORE INSERT ON cache_changes
    FOR EACH ROW EXECUTE FUNCTION felix_cache_change_deleted_through();

-- An older instance writes a new cache shard's assignment at incarnation 0
-- and generation 0. As 0028 does for streams, the row takes its cache's
-- incarnation and first generation.
CREATE OR REPLACE FUNCTION felix_cache_shard_assignment_incarnation() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE
    current BIGINT;
BEGIN
    IF EXISTS (SELECT 1 FROM shard_assignments
        WHERE tenant_id = NEW.tenant_id AND namespace = NEW.namespace
          AND kind = NEW.kind AND stream = NEW.stream AND shard = NEW.shard) THEN
        RETURN NEW;
    END IF;
    SELECT incarnation INTO current FROM caches
        WHERE tenant_id = NEW.tenant_id AND namespace = NEW.namespace AND cache = NEW.stream
        FOR KEY SHARE;
    IF current IS NULL OR current = 0 THEN
        RETURN NEW;
    END IF;
    NEW.incarnation := current;
    SELECT next_seq INTO NEW.generation FROM shard_assignment_change_seq FOR UPDATE;
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS shard_assignments_cache_incarnation ON shard_assignments;
CREATE TRIGGER shard_assignments_cache_incarnation
    BEFORE INSERT ON shard_assignments
    FOR EACH ROW WHEN (NEW.incarnation = 0 AND NEW.kind = 'cache')
    EXECUTE FUNCTION felix_cache_shard_assignment_incarnation();

-- The same writer updating an existing cache shard writes incarnation 0 over
-- the row's, and its change would then order below the assignment it
-- replaces. The row keeps its incarnation; the change trigger below copies it.
CREATE OR REPLACE FUNCTION felix_cache_shard_assignment_keep_incarnation() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    NEW.incarnation := OLD.incarnation;
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS shard_assignments_cache_keep_incarnation ON shard_assignments;
CREATE TRIGGER shard_assignments_cache_keep_incarnation
    BEFORE UPDATE ON shard_assignments
    FOR EACH ROW WHEN (NEW.kind = 'cache' AND NEW.incarnation = 0 AND OLD.incarnation <> 0)
    EXECUTE FUNCTION felix_cache_shard_assignment_keep_incarnation();

-- That writer's change takes the row's incarnation and generation, through
-- 0028's function, which reads only the assignment row.
DROP TRIGGER IF EXISTS shard_assignment_changes_cache_incarnation ON shard_assignment_changes;
CREATE TRIGGER shard_assignment_changes_cache_incarnation
    BEFORE INSERT ON shard_assignment_changes
    FOR EACH ROW WHEN (NEW.payload IS NOT NULL AND NEW.kind = 'cache'
        AND COALESCE((NEW.payload->>'incarnation')::BIGINT, 0) = 0)
    EXECUTE FUNCTION felix_shard_change_incarnation();
