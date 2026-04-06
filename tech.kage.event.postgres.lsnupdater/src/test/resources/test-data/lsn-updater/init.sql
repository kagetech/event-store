CREATE SCHEMA IF NOT EXISTS events;

CREATE TABLE IF NOT EXISTS events.test_events (
    id bigserial PRIMARY KEY,
    key uuid NOT NULL,
    data bytea NOT NULL,
    metadata bytea,
    timestamp timestamp with time zone NOT NULL,
    lsn pg_lsn
);

CREATE PUBLICATION event_lsn_publication
FOR TABLES IN SCHEMA events
WITH (publish = 'insert');

SELECT pg_create_logical_replication_slot('event_lsn_updater', 'pgoutput');
