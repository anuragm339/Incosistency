-- ============================================================
-- V1__create_mts_tables.sql
-- Creates partitioned mts_summary and mts_store tables,
-- their indexes, and initial partitions for today + 10 days.
-- ============================================================

-- ----------------------------------------------------------------
-- mts_summary: one row per published message key
-- ----------------------------------------------------------------
CREATE TABLE IF NOT EXISTS mts_summary (
    id                 BIGSERIAL,
    message_key        VARCHAR        NOT NULL,
    cluster_id         VARCHAR        NOT NULL,
    msg_offset         BIGINT,
    first_published_at TIMESTAMP      NOT NULL,
    last_published_at  TIMESTAMP      NOT NULL,
    expire_at          TIMESTAMP      NOT NULL,
    total_stores       INT            NOT NULL DEFAULT 0,
    stores_done        INT            NOT NULL DEFAULT 0,
    stores_partial     INT            NOT NULL DEFAULT 0,
    stores_timed_out   INT            NOT NULL DEFAULT 0,
    publish_count      INT            NOT NULL DEFAULT 1,
    state              VARCHAR        NOT NULL DEFAULT 'PENDING',
    inconsistencies    JSONB          NOT NULL DEFAULT '[]',
    created_at         TIMESTAMP      NOT NULL DEFAULT NOW(),
    updated_at         TIMESTAMP      NOT NULL DEFAULT NOW()
) PARTITION BY RANGE (expire_at);

-- ----------------------------------------------------------------
-- mts_store: one row per (message_key, store_number) pair
-- ----------------------------------------------------------------
CREATE TABLE IF NOT EXISTS mts_store (
    id                   BIGSERIAL,
    message_key          VARCHAR   NOT NULL,
    cluster_id           VARCHAR   NOT NULL,
    store_number         VARCHAR   NOT NULL,
    location_id          VARCHAR,
    expire_at            TIMESTAMP NOT NULL,
    expected_pos         JSONB     NOT NULL DEFAULT '[]',
    responded_pos        JSONB     NOT NULL DEFAULT '[]',
    missing_pos          JSONB     NOT NULL DEFAULT '[]',
    pos_statuses         JSONB     NOT NULL DEFAULT '{}',
    state                VARCHAR   NOT NULL DEFAULT 'PENDING',
    inconsistencies      JSONB     NOT NULL DEFAULT '[]',
    last_checkpoint_pct  INT       NOT NULL DEFAULT 0,
    created_at           TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at           TIMESTAMP NOT NULL DEFAULT NOW()
) PARTITION BY RANGE (expire_at);

-- ----------------------------------------------------------------
-- Create daily partitions for today through today + 10 days.
-- Each partition covers one calendar day in UTC.
-- Using generate_series to keep the DDL concise and correct.
-- ----------------------------------------------------------------
DO $$
DECLARE
    partition_date DATE;
    partition_start TIMESTAMP;
    partition_end   TIMESTAMP;
    summary_part    TEXT;
    store_part      TEXT;
BEGIN
    FOR partition_date IN
        SELECT d::DATE
        FROM generate_series(
            CURRENT_DATE,
            CURRENT_DATE + INTERVAL '10 days',
            INTERVAL '1 day'
        ) AS d
    LOOP
        partition_start := partition_date::TIMESTAMP;
        partition_end   := partition_date::TIMESTAMP + INTERVAL '1 day';

        -- mts_summary partition
        summary_part := 'mts_summary_' || TO_CHAR(partition_date, 'YYYY_MM_DD');
        IF NOT EXISTS (
            SELECT 1 FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE c.relname = summary_part
        ) THEN
            EXECUTE format(
                'CREATE TABLE %I PARTITION OF mts_summary FOR VALUES FROM (%L) TO (%L)',
                summary_part, partition_start, partition_end
            );
        END IF;

        -- mts_store partition
        store_part := 'mts_store_' || TO_CHAR(partition_date, 'YYYY_MM_DD');
        IF NOT EXISTS (
            SELECT 1 FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE c.relname = store_part
        ) THEN
            EXECUTE format(
                'CREATE TABLE %I PARTITION OF mts_store FOR VALUES FROM (%L) TO (%L)',
                store_part, partition_start, partition_end
            );
        END IF;
    END LOOP;
END;
$$;

-- ----------------------------------------------------------------
-- Indexes for mts_summary
-- ----------------------------------------------------------------
-- Unique constraint on message_key (business PK)
CREATE UNIQUE INDEX IF NOT EXISTS uidx_mts_summary_message_key
    ON mts_summary (message_key);

-- Lookup by state for cleanup queries
CREATE INDEX IF NOT EXISTS idx_mts_summary_state
    ON mts_summary (state);

-- Lookup by cluster
CREATE INDEX IF NOT EXISTS idx_mts_summary_cluster_id
    ON mts_summary (cluster_id);

-- Expire_at for range scans
CREATE INDEX IF NOT EXISTS idx_mts_summary_expire_at
    ON mts_summary (expire_at);

-- Combined index used by finalizeMessage checks
CREATE INDEX IF NOT EXISTS idx_mts_summary_expire_state
    ON mts_summary (expire_at, state);

-- ----------------------------------------------------------------
-- Indexes for mts_store
-- ----------------------------------------------------------------
-- Unique constraint on (message_key, store_number) – business PK
CREATE UNIQUE INDEX IF NOT EXISTS uidx_mts_store_message_key_store
    ON mts_store (message_key, store_number);

-- Lookup all stores for a message key
CREATE INDEX IF NOT EXISTS idx_mts_store_message_key
    ON mts_store (message_key);

-- Cleanup scheduler: expired rows in active states
CREATE INDEX IF NOT EXISTS idx_mts_store_expire_state
    ON mts_store (expire_at, state)
    WHERE state IN ('PENDING', 'PARTIAL');

-- Lookup by store_number across messages
CREATE INDEX IF NOT EXISTS idx_mts_store_store_number
    ON mts_store (store_number);

-- Lookup by cluster
CREATE INDEX IF NOT EXISTS idx_mts_store_cluster_id
    ON mts_store (cluster_id);
