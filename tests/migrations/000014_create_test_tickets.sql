-- Test table for enum-typed list filters and sorts.
-- relais_test_tickets: `state` is an enum stored as text; `queue_id` and
-- `weight` are plain integer filters around it.

CREATE TABLE IF NOT EXISTS relais_test_tickets (
    id       BIGSERIAL PRIMARY KEY,
    queue_id BIGINT  NOT NULL DEFAULT 0,
    state    TEXT    NOT NULL DEFAULT 'open'
             CHECK (state IN ('open', 'blocked', 'closed', 'archived')),
    weight   INTEGER NOT NULL DEFAULT 0,
    title    TEXT    NOT NULL DEFAULT ''
);
