-- Test base table + aggregated VIEW for relais array-column mapping
-- (int8[] -> std::vector<int64_t>, text[] -> std::vector<std::string>).
-- Mirrors the codiga member_role_set pattern: a junction collapsed per owner via
-- array_agg. INNER scan + GROUP BY → an owner with no rows is simply absent from the
-- view (find() miss → empty), and array_agg never yields {NULL}.

CREATE TABLE IF NOT EXISTS relais_test_array_src (
    owner_id BIGINT NOT NULL,
    tag_id   BIGINT NOT NULL,
    label    TEXT   NOT NULL,
    PRIMARY KEY (owner_id, tag_id)
);

CREATE OR REPLACE VIEW relais_test_array_view AS
    SELECT owner_id,
           array_agg(tag_id ORDER BY tag_id) AS tag_ids,
           array_agg(label  ORDER BY tag_id) AS labels
    FROM relais_test_array_src
    GROUP BY owner_id;
