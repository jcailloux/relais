-- Test table for relais composite-key LIST caching (keyset cursor over a
-- composite primary key). All-PK junction, mirrors a real member_roles table.

CREATE TABLE IF NOT EXISTS relais_test_composite_list (
    tenant_id BIGINT NOT NULL,
    item_id   BIGINT NOT NULL,
    PRIMARY KEY (tenant_id, item_id)
);