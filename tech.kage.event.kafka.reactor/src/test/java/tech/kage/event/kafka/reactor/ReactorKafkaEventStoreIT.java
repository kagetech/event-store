/*
 * Copyright (c) 2023-2025, Dariusz Szpakowski
 * 
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 * 
 * 1. Redistributions of source code must retain the above copyright notice, this
 *    list of conditions and the following disclaimer.
 * 
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 *    this list of conditions and the following disclaimer in the documentation
 *    and/or other materials provided with the distribution.
 * 
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
 * SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
 * CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
 * OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package tech.kage.event.kafka.reactor;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import java.util.stream.IntStream;

import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.core.io.Resource;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.transaction.reactive.TransactionalOperator;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import reactor.core.publisher.Flux;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.receiver.ReceiverRecord;
import reactor.test.StepVerifier;
import tech.kage.event.Event;

/**
 * Integration tests for {@link ReactorKafkaEventStore}.
 * 
 * @author Dariusz Szpakowski
 */
@SpringBootTest
@ActiveProfiles("test")
@Testcontainers
class ReactorKafkaEventStoreIT {
    // UUT
    @Autowired
    ReactorKafkaEventStore eventStore;

    @Autowired
    DatabaseClient databaseClient;

    @Autowired
    ReceiverOptions<UUID, SpecificRecord> kafkaReceiverOptions;

    @Autowired
    KafkaAdmin kafkaAdmin;

    @Autowired
    TransactionalOperator transactionalOperator;

    String topic;

    static final Network network = Network.newNetwork();

    @SuppressWarnings("resource")
    @Container
    static final KafkaContainer kafka = new KafkaContainer("apache/kafka-native:3.8.1").withNetwork(network);

    @SuppressWarnings("resource")
    @Container
    static final GenericContainer<?> schemaRegistry = new GenericContainer<>(
            DockerImageName.parse("confluentinc/cp-schema-registry:7.6.1"))
            .dependsOn(kafka)
            .withNetwork(network)
            .withExposedPorts(8081)
            .withEnv("SCHEMA_REGISTRY_HOST_NAME", "schema-registry")
            .withEnv("SCHEMA_REGISTRY_LISTENERS", "http://0.0.0.0:8081")
            .withEnv("SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS",
                    "PLAINTEXT://%s:9092".formatted(kafka.getNetworkAliases().get(0)))
            .waitingFor(Wait.forHttp("/subjects").forStatusCode(200));

    @Container
    @ServiceConnection
    static final PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:15-alpine");

    @DynamicPropertySource
    static void overrideProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);

        registry.add(
                "spring.kafka.properties.schema.registry.url",
                () -> "http://%s:%s".formatted(schemaRegistry.getHost(), schemaRegistry.getFirstMappedPort()));
    }

    @Configuration
    @EnableAutoConfiguration
    @Import(ReactorKafkaEventStore.class)
    static class TestConfiguration {
    }

    @BeforeEach
    void setUp(@Value("classpath:/test-data/events/ddl.sql") Resource ddl) throws IOException {
        topic = "test_" + System.currentTimeMillis() + "_events";

        kafkaAdmin.createOrModifyTopics(TopicBuilder.name(topic).build());

        databaseClient
                .sql(ddl.getContentAsString(StandardCharsets.UTF_8))
                .fetch()
                .rowsUpdated()
                .block();
    }

    @Test
    void savesEventsInKafka() {
        // Given
        var events = IntStream
                .rangeClosed(1, 10)
                .boxed()
                .map(id -> TestPayload.newBuilder().setText("test payload " + id).build())
                .map(payload -> Event.from(UUID.randomUUID(), payload))
                .toList();

        // When
        var savedEvents = Flux.concat(events.stream().map(event -> eventStore.save(topic, event)).toList());

        // Then
        var retrievedEvents = readEventsFromKafka(topic)
                .take(events.size())
                .timeout(Duration.ofSeconds(60));

        StepVerifier
                .create(savedEvents.thenMany(retrievedEvents))
                .expectNextSequence(events)
                .as("retrieves stored events with the same data")
                .verifyComplete();
    }

    @Test
    void returnsStoredEvent() {
        // Given
        var key = UUID.randomUUID();
        var payload = TestPayload.newBuilder().setText("test payload").build();
        var event = Event.from(key, payload);

        // When
        var storedEvent = eventStore.save(topic, event);

        // Then
        StepVerifier
                .create(storedEvent)
                .expectNext(event)
                .as("returns stored event")
                .verifyComplete();
    }

    @Test
    void readsEventsFromKafka() {
        // Given
        var events = IntStream
                .rangeClosed(1, 10)
                .boxed()
                .map(id -> TestPayload.newBuilder().setText("test payload " + id).build())
                .map((SpecificRecord payload) -> Event.from(UUID.randomUUID(), payload))
                .toList();

        var savedEvents = Flux.concat(events.stream().map(event -> eventStore.save(topic, event)).toList());

        var expectedEvents = IntStream
                .range(0, events.size())
                .boxed()
                .map(i -> {
                    var event = events.get(i);
                    var metadata = Map.<String, Object>of("partition", 0, "offset", Long.valueOf(i));

                    return Event.from(event.key(), event.payload(), event.timestamp(), metadata);
                })
                .toList();

        // When
        var retrievedEvents = eventStore
                .subscribe(topic)
                .concatMap(event -> event.as(transactionalOperator::transactional))
                .take(events.size())
                .timeout(Duration.ofSeconds(60));

        // Then
        StepVerifier
                .create(savedEvents.thenMany(retrievedEvents))
                .expectNextSequence(expectedEvents)
                .as("retrieves stored events with the same data")
                .verifyComplete();
    }

    @Test
    void resumesProcessingEventsFromStoredOffset() {
        // Given
        var events = IntStream
                .rangeClosed(1, 10)
                .boxed()
                .map(id -> TestPayload.newBuilder().setText("test payload " + id).build())
                .map((SpecificRecord payload) -> Event.from(UUID.randomUUID(), payload))
                .toList();

        // save 10 events
        var savedEvents = Flux.concat(events.stream().map(event -> eventStore.save(topic, event)).toList());

        // process n - 3 events
        var firstBatch = eventStore
                .subscribe(topic)
                .concatMap(event -> event.as(transactionalOperator::transactional))
                .take(events.size() - 3);

        var expectedEvents = IntStream
                .range(events.size() - 3, events.size())
                .boxed()
                .map(i -> {
                    var event = events.get(i);
                    var metadata = Map.<String, Object>of("partition", 0, "offset", Long.valueOf(i));

                    return Event.from(event.key(), event.payload(), event.timestamp(), metadata);
                })
                .toList();

        // When
        var secondBatch = eventStore
                .subscribe(topic)
                .concatMap(event -> event.as(transactionalOperator::transactional))
                .take(3)
                .timeout(Duration.ofSeconds(60));

        // Then
        StepVerifier
                .create(savedEvents.thenMany(firstBatch).thenMany(secondBatch))
                .expectNextSequence(expectedEvents)
                .as("resumes processing events")
                .verifyComplete();
    }

    private Flux<Event<?>> readEventsFromKafka(String topic) {
        return KafkaReceiver
                .create(kafkaReceiverOptions.assignment(Collections.singleton(new TopicPartition(topic, 0))))
                .receive()
                .map(this::toEvent);
    }

    private Event<?> toEvent(ReceiverRecord<UUID, SpecificRecord> rec) {
        return Event.from(rec.key(), rec.value(), Instant.ofEpochMilli(rec.timestamp()));
    }
}
