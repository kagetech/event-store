# Kafka Streams based Event Store

An implementation of an event store based on [Apache Kafka](https://kafka.apache.org/) and [Kafka Streams](https://kafka.apache.org/documentation/streams/).

The events are stored in Kafka topics and accessed using Kafka Streams API.

Uses [Apache Avro](https://avro.apache.org/) for payload serialization and stores Avro schemas in [Confluent Schema Registry](https://github.com/confluentinc/schema-registry).

## Getting started

**Maven configuration:**

```xml
<dependency>
    <groupId>tech.kage.event</groupId>
    <artifactId>tech.kage.event</artifactId>
    <version>1.3.1</version>
</dependency>

<dependency>
    <groupId>tech.kage.event</groupId>
    <artifactId>tech.kage.event.kafka.streams</artifactId>
    <version>1.3.1</version>
</dependency>
```

**module-info.java**

```java
module my.simple.mod {
    requires tech.kage.event;
    requires tech.kage.event.kafka.streams;
}
```

**application.properties**

```properties
# Kafka configuration
spring.kafka.bootstrap-servers=localhost:9092
spring.kafka.properties.schema.registry.url=http://localhost:8989

kafka.streams.application.id=sample-kafka-streams-event-store
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
KafkaStreamsEventStore<UUID, SpecificRecord> eventStore;

eventStore
        .subscribe("sample_topic")
        ...
```

**Encrypt output events**

```java
eventStore
    .subscribe("input-topic")
    .mapValues(...) // set EventStore.ENCRYPTION_KEY_ID header
    .processValues(() -> eventStore.new EncryptingOutputEventTransformer("encrypted-output-topic"))
    .to("encrypted-output-topic", Produced.valueSerde(Serdes.ByteArray()));
```

## Examples

The [test classes](src/test/java/tech/kage/event/kafka/streams) contain code that may be used for learning how to use this project.

## License

This project is released under the [BSD 2-Clause License](../LICENSE).
