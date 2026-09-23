-- Test tables for relais conditional and relative writes.
-- relais_test_slots: one row per claimable resource. `state` is an enum stored as
-- text, `holder` / `expires_at` are nullable (a free slot has neither), `counter`
-- is a NOT NULL integer for relative updates, `version` backs compare-and-set,
-- `priority` backs ordering.
-- relais_test_slot_tallies: composite primary key with a relative-update column.

CREATE TABLE IF NOT EXISTS relais_test_slots (
    id         BIGSERIAL PRIMARY KEY,
    group_id   BIGINT      NOT NULL DEFAULT 0,
    state      TEXT        NOT NULL DEFAULT 'free',
    holder     BIGINT,
    expires_at TIMESTAMPTZ,
    counter    INTEGER     NOT NULL DEFAULT 0,
    version    BIGINT      NOT NULL DEFAULT 0,
    priority   INTEGER     NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS relais_test_slot_tallies (
    group_id BIGINT NOT NULL,
    bucket   BIGINT NOT NULL,
    hits     BIGINT NOT NULL DEFAULT 0,
    PRIMARY KEY (group_id, bucket)
);
