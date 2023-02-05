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
    <version>1.0.0</version>
</dependency>

<dependency>
    <groupId>tech.kage.event</groupId>
    <artifactId>tech.kage.event.kafka.streams</artifactId>
    <version>1.0.0</version>
</dependency>
```

**module-info.java**

```java
module my.simple.mod {
    requires tech.kage.event;
    requires tech.kage.event.kafka.streams;
}
```

**Spring Boot application**

```java
@SpringBootApplication
@Import(KafkaStreamsEventStore.class)
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
EventStore eventStore;

var key = UUID.randomUUID();
var payload = TestPayload.newBuilder().setText("sample payload").build();
var event = Event.from(key, payload);

eventStore.save("sample_topic", event);
```

**Subscribe to an event stream**

```java
@Autowired
KafkaStreamsEventStore eventStore;

eventStore
        .subscribe("sample_topic")
        ...
```

## Examples

The [test classes](src/test/java/tech/kage/event/kafka/streams) contain code that may be used for learning how to use this project.

## License

This project is released under the [BSD 2-Clause License](../LICENSE).
