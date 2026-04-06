# PostgreSQL Event LSN Updater

LSN updater for PostgreSQL event tables using logical replication.

Connects to a PostgreSQL logical replication slot and listens for INSERT operations on tables ending with the `_events` suffix. For each inserted row, updates its `lsn` column with the WAL location (Log Sequence Number) of the corresponding change.

## Getting started

Given a database schema:

```sql
CREATE SCHEMA IF NOT EXISTS events;

CREATE TABLE IF NOT EXISTS events.test_events (
    id bigserial PRIMARY KEY,
    key uuid NOT NULL,
    data bytea NOT NULL,
    metadata bytea,
    timestamp timestamp with time zone NOT NULL,
    lsn pg_lsn
);
```

ensure `wal_level` is set to `logical` in `postgresql.conf` if not already configured:

```
wal_level = logical
```

then create a publication and a logical replication slot:

```sql
CREATE PUBLICATION event_lsn_publication
FOR TABLES IN SCHEMA events
WITH (publish = 'insert');

SELECT pg_create_logical_replication_slot('event_lsn_updater', 'pgoutput');
```

run:

```
java -jar tech.kage.event.postgres.lsnupdater-$VERSION.jar --spring.datasource.url=jdbc:postgresql://localhost:5432/testdb --spring.datasource.username=postgres --spring.datasource.password=postgres
```

## Assumptions

- Event tables follow a fixed schema: `id bigserial PRIMARY KEY` as the first column, followed by `key`, `data`, `metadata`, `timestamp`, and `lsn pg_lsn`. Table names must end with the `_events` suffix.
- Events are immutable — rows are never updated or deleted after insertion.
- The LSN update is eventually consistent. There is a short window after an INSERT during which `lsn` is NULL. Downstream consumers (e.g. event-replicator) query with `WHERE lsn IS NOT NULL` to naturally wait for the LSN to be set.
- The process is designed to fail fast on any unrecoverable error and must be run under a process supervisor (e.g. Kubernetes, systemd) that restarts it automatically. On restart, the replication slot is resumed from its last confirmed position and any pending messages are replayed safely.

## Configuration

The list of supported configuration properties is given below.

**Database configuration**

- `spring.datasource.url` or `DATABASE_URL` environment variable - sets database URL (e.g. "jdbc:postgresql://localhost:5432/testdb").
- `spring.datasource.username` or `DATABASE_USERNAME` environment variable - sets database username.
- `spring.datasource.password` or `DATABASE_PASSWORD` environment variable - sets database password.

**LSN updater configuration**

- `event.lsn.updater.enabled` - enables or disables the LSN updater (set to `true` by default).
- `event.lsn.updater.slot.name` - logical replication slot name (set to "event_lsn_updater" by default).
- `event.lsn.updater.publication.name` - publication name (set to "event_lsn_publication" by default).

## Observability

A health endpoint is available at `http://<host>:<MANAGEMENT_PORT>/actuator/health` (default port: 9191) for use as a Kubernetes liveness probe.

Sample SQL queries useful for monitoring the LSN updater.

**Replication slot status**

```sql
SELECT slot_name, active, restart_lsn, confirmed_flush_lsn,
       pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)) AS retained_wal
FROM pg_replication_slots
WHERE slot_name = 'event_lsn_updater';
```

**Replication lag (bytes behind current WAL position)**

```sql
SELECT slot_name,
       pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn) AS lag_bytes,
       pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)) AS lag_pretty
FROM pg_replication_slots
WHERE slot_name = 'event_lsn_updater';
```

**Rows pending LSN update in a given event table**

```sql
SELECT count(*) AS pending_lsn_count
FROM events.test_events
WHERE lsn IS NULL;
```

**Latest LSN values written**

```sql
SELECT id, lsn, timestamp
FROM events.test_events
WHERE lsn IS NOT NULL
ORDER BY id DESC
LIMIT 10;
```

**Publication tables**

```sql
SELECT schemaname, tablename
FROM pg_publication_tables
WHERE pubname = 'event_lsn_publication';
```

## License

This project is released under the [BSD 2-Clause License](LICENSE).
