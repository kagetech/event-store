CREATE SCHEMA IF NOT EXISTS events;

CREATE TABLE IF NOT EXISTS events.test_events (
    id bigserial PRIMARY KEY,
    key <<key_type>> NOT NULL,
    data bytea NOT NULL,
    metadata bytea,
    timestamp timestamp with time zone NOT NULL,
    lsn pg_lsn
);

CREATE INDEX IF NOT EXISTS test_events_lsn_idx ON events.test_events (lsn);

TRUNCATE TABLE events.test_events;
