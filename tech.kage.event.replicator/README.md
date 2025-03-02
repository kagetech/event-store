# PostgreSQL -> Kafka Event Replicator

Replicator of events from PostgreSQL relational tables to Apache Kafka topics with exactly-once semantics (EOS).

Reads topic tables in `event.replicator.event.schema` schema and copies events to Kafka topics with the same name. Stores replication progress in a Kafka topic. Uses Kafka transactions to guarantee EOS.

## Getting started

Given a database schema:

```sql
CREATE SCHEMA IF NOT EXISTS events;

CREATE TABLE IF NOT EXISTS events.test_events (
    id bigserial PRIMARY KEY,
    key uuid NOT NULL,
    data bytea NOT NULL,
    metadata bytea,
    timestamp timestamp with time zone NOT NULL
);
```

run:

```
java -jar tech.kage.event.replicator-$VERSION.jar --spring.datasource.url=jdbc:postgresql://localhost:5432/testdb --spring.datasource.username=postgres --spring.datasource.password=postgres --spring.kafka.bootstrap-servers=localhost:9092
```

## Configuration

The list of supported configuration properties is given below.

**Database configuration**

- `spring.datasource.url` or `DATABASE_URL` environment variable - sets database URL (e.g. "jdbc:postgresql://localhost:5432/testdb"),
- `spring.datasource.username` or `DATABASE_USERNAME` environment variable - sets database username.
- `spring.datasource.password` or `DATABASE_PASSWORD` environment variable - sets database password.

**Kafka configuration**

- `spring.kafka.bootstrap-servers` or `KAFKA_URL` environment variable - sets Kafka address (e.g. "localhost:9092").

**Database polling configuration**

- `event.replicator.event.schema` - database schema containing event tables (set to "events" by default). The names of the event tables need to end with the "_events" suffix to be considered for replication.
- `event.replicator.poll.max.rows` - maximum number of events in one database poll and also the maximum size of a Kafka transaction (set to 100 by default).
- `event.replicator.poll.interval.ms` - polling interval in milliseconds (set to 1000 by default).

**Advanced configuration**

- `spring.task.scheduling.pool.size` - size of a Spring scheduler pool (set to 20 by default, increasing it may be needed, depending on load).
- `spring.kafka.producer.transaction-id-prefix` - sets Kafka producer transaction ID prefix (set to "event-replicator-" by default).
- `spring.kafka.consumer.group-id` - sets Kafka consumer group ID (set to "event-replicator" by default).
- `spring.kafka.consumer.max-poll-records` - this should be set to a value high enough to fit all progress related records in the progress Kafka topic (set to 1000 by default).

## License

This project is released under the [BSD 2-Clause License](LICENSE).
