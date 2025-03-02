# Reactor Kafka based Event Store

An implementation of an event store based on [Apache Kafka](https://kafka.apache.org/) and [Reactor Kafka](https://projectreactor.io/docs/kafka/release/reference/).

The events are stored in Kafka topics and accessed using Reactor Kafka API. Kafka consumer offsets are stored externally in a relational database table to allow database projections with exactly-once semantics (EOS).

Uses [Apache Avro](https://avro.apache.org/) for payload serialization and stores Avro schemas in [Confluent Schema Registry](https://github.com/confluentinc/schema-registry).

## Getting started

**Database schema (consumer offsets):**

```sql
CREATE SCHEMA IF NOT EXISTS events;

CREATE TABLE IF NOT EXISTS events.topic_offsets (
    topic text,
    "partition" integer,
    "offset" bigint,

    PRIMARY KEY (topic, "partition")
);
```

**Maven configuration:**

```xml
<dependency>
    <groupId>tech.kage.event</groupId>
    <artifactId>tech.kage.event</artifactId>
    <version>1.3.0</version>
</dependency>

<dependency>
    <groupId>tech.kage.event</groupId>
    <artifactId>tech.kage.event.kafka.reactor</artifactId>
    <version>1.3.0</version>
</dependency>
```

**module-info.java**

```java
module my.simple.mod {
    requires tech.kage.event;
    requires tech.kage.event.kafka.reactor;
}
```

**Spring Boot application**

```java
@SpringBootApplication
@Import(ReactorKafkaEventStore.class)
```

**application.properties**

```properties
# Database configuration
spring.r2dbc.url=r2dbc:postgresql://localhost:5432/sample-db

# Kafka configuration
spring.kafka.bootstrap-servers=localhost:9092
spring.kafka.properties.schema.registry.url=http://localhost:8989
spring.kafka.consumer.group-id=sample-projection
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

**Subscribe to an event stream**

```java
@Autowired
ReactorKafkaEventStore<UUID, SpecificRecord> eventStore;

@Autowired
ReactiveTransactionManager transactionManager

TransactionalOperator transactionalOperator = TransactionalOperator.create(transactionManager);

eventStore
        .subscribe("sample_topic")
        .concatMap(event -> event.flatMap(this::processEvent).as(transactionalOperator::transactional))
```

## Examples

The [test classes](src/test/java/tech/kage/event/kafka/reactor) contain code that may be used for learning how to use this project.

## License

This project is released under the [BSD 2-Clause License](../LICENSE).
