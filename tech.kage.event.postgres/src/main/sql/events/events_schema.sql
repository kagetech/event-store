CREATE SCHEMA IF NOT EXISTS events;

CREATE TABLE IF NOT EXISTS events.test_events (
    id bigserial PRIMARY KEY,
    key uuid NOT NULL,
    data bytea NOT NULL,
    metadata bytea,
    timestamp timestamp with time zone NOT NULL
);
