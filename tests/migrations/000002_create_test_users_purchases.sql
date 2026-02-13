-- Test tables for relais cross-invalidation tests
-- Users table (parent) and Purchases table (child with FK)

CREATE TABLE IF NOT EXISTS relais_test_users (
    id BIGSERIAL PRIMARY KEY,
    username VARCHAR(50) NOT NULL,
    email VARCHAR(100) NOT NULL,
    balance INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_relais_test_users_username ON relais_test_users(username);

CREATE TABLE IF NOT EXISTS relais_test_purchases (
    id BIGSERIAL PRIMARY KEY,
    user_id BIGINT NOT NULL REFERENCES relais_test_users(id) ON DELETE CASCADE,
    product_name VARCHAR(100) NOT NULL,
    amount INTEGER NOT NULL,
    status VARCHAR(20) NOT NULL DEFAULT 'pending',
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_relais_test_purchases_user_id ON relais_test_purchases(user_id);
CREATE INDEX IF NOT EXISTS idx_relais_test_purchases_created_at ON relais_test_purchases(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_relais_test_purchases_status ON relais_test_purchases(status);
