-- Test table for relais native upsert with automatic list caching.
-- Assigned (non-db_managed) primary key + a @relais_list descriptor. owner_id is
-- the list filter/sort dimension; label is a plain updatable column, so the
-- ON CONFLICT DO UPDATE SET clause is non-empty (upsert applicability gate).

CREATE TABLE IF NOT EXISTS relais_test_upsert_list (
    id       BIGINT NOT NULL,
    owner_id BIGINT NOT NULL DEFAULT 0,
    label    TEXT   NOT NULL DEFAULT '',
    PRIMARY KEY (id)
);
