CREATE SCHEMA IF NOT EXISTS events;

CREATE TABLE IF NOT EXISTS events.topic_offsets (
    topic text,
    "partition" integer,
    "offset" bigint,

    PRIMARY KEY (topic, "partition")
);
