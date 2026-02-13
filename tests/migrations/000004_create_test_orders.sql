-- Test table for relais comprehensive field type testing
-- Contains scalar, nullable, enum, string, timestamp, and raw JSON fields
-- Composite fields (address, history, quantities, tags) are FlatBuffer-only

CREATE TABLE IF NOT EXISTS relais_test_orders (
    id BIGSERIAL PRIMARY KEY,
    user_id BIGINT NOT NULL,
    amount INTEGER NOT NULL,
    discount INTEGER,
    is_express BOOLEAN NOT NULL DEFAULT false,
    priority VARCHAR(20) NOT NULL DEFAULT 'low',
    label VARCHAR(200) NOT NULL DEFAULT '',
    metadata TEXT NOT NULL DEFAULT '',
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);