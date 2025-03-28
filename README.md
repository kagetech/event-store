# PostgreSQL + Kafka based Event Store

An implementation of an event store based on [PostgreSQL](https://www.postgresql.org/) and [Apache Kafka](https://kafka.apache.org/).

The events are stored in relational database tables and may be replicated to Kafka topics. Provides exactly-once semantics (EOS) as defined in Kafka.

Uses [Apache Avro](https://avro.apache.org/) for payload serialization and stores Avro schemas in [Confluent Schema Registry](https://github.com/confluentinc/schema-registry).

## Getting started

**Database schema:**

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

**Maven configuration:**

```xml
<dependency>
    <groupId>tech.kage.event</groupId>
    <artifactId>tech.kage.event</artifactId>
    <version>1.3.1</version>
</dependency>

<dependency>
    <groupId>tech.kage.event</groupId>
    <artifactId>tech.kage.event.postgres</artifactId>
    <version>1.3.1</version>
</dependency>
```

**module-info.java**

```java
module my.simple.mod {
    requires tech.kage.event;
    requires tech.kage.event.postgres;
}
```

**Schema Registry URL**

```properties
# Schema registry
schema.registry.url=http://localhost:8989
```

**Save an event**

```java
@Autowired
EventStore<UUID, TestPayload> eventStore;

var key = UUID.randomUUID();
var payload = TestPayload.newBuilder().setText("sample payload").build();
var event = Event.from(key, payload);

eventStore.save("sample_topic", event);
```

**Save an encrypted event**

```java
// configure com.google.crypto.tink.Aead bean
@Bean
@Scope(SCOPE_PROTOTYPE)
Aead aead(URI encryptionKey) throws GeneralSecurityException {
    KmsClient kmsClient = ... // configure com.google.crypto.tink.KmsClient

    return kmsClient.get(encryptionKey).getPrimitive(RegistryConfiguration.get(), Aead.class);
}

var encryptionKey = URI.create("test-kms://test-keys/" + key.toString());

eventStore.save("sample_encrypted_topic", event, encryptionKey);
```

**Replicate events to Kafka**

See [Event Replicator](tech.kage.event.replicator).

**Process events**

See [Reactor Kafka Event Store](tech.kage.event.kafka.reactor) and [Kafka Streams Event Store](tech.kage.event.kafka.streams).

## Examples

The [test classes](tech.kage.event.postgres/src/test/java/tech/kage/event/postgres) contain code that may be used for learning how to use this project.

## License

This project is released under the [BSD 2-Clause License](LICENSE).
