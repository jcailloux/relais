-- Test table with native array columns for the relais array WRITE path
-- (std::vector<int64_t> -> int8[], std::vector<std::string> -> text[]).
-- Round-trips insert/find/update through PgParams::toParam and the read parser.

CREATE TABLE IF NOT EXISTS relais_test_array_rw (
    owner_id BIGINT PRIMARY KEY,
    tag_ids  BIGINT[] NOT NULL,
    labels   TEXT[]   NOT NULL
);
