-- Test table for column= annotation mapping tests.
-- Column names intentionally differ from C++ field names.

CREATE TABLE IF NOT EXISTS relais_test_products (
    id BIGSERIAL PRIMARY KEY,
    product_name VARCHAR(100) NOT NULL,
    stock_level INTEGER NOT NULL DEFAULT 0,
    discount_pct INTEGER,
    is_available BOOLEAN NOT NULL DEFAULT true,
    description TEXT NOT NULL DEFAULT '',
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);
