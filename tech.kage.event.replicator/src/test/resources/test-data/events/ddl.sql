CREATE SCHEMA IF NOT EXISTS events;

CREATE TABLE IF NOT EXISTS events.<<topic_name>> (
    id bigserial PRIMARY KEY,
    key <<key_type>> NOT NULL,
    data bytea NOT NULL,
    metadata bytea,
    timestamp timestamp with time zone NOT NULL
);
