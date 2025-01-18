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

package tech.kage.event.kafka.streams;

import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.UUID;
import java.util.stream.IntStream;

import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.stereotype.Component;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
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
 * Integration tests for {@link KafkaStreamsEventStore}.
 * 
 * @author Dariusz Szpakowski
 */
@SpringBootTest
@ActiveProfiles("test")
@Testcontainers
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class KafkaStreamsEventStoreIT {
    // UUT
    @Autowired
    KafkaStreamsEventStore eventStore;

    @Autowired
    ReceiverOptions<UUID, SpecificRecord> kafkaReceiverOptions;

    static final String TEST_EVENTS = "test_events";
    static final String TEST_EVENTS_IN = "test_events_in";
    static final String TEST_EVENTS_OUT = "test_events_out";

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

    @DynamicPropertySource
    static void overrideProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);

        registry.add(
                "spring.kafka.properties.schema.registry.url",
                () -> "http://%s:%s".formatted(schemaRegistry.getHost(), schemaRegistry.getFirstMappedPort()));

    }

    @Component
    static class TestStreamsSubscriber {
        @Autowired
        void init(KafkaAdmin kafkaAdmin, KafkaStreamsEventStore eventStore) {
            kafkaAdmin.createOrModifyTopics(TopicBuilder.name(TEST_EVENTS).build());
            kafkaAdmin.createOrModifyTopics(TopicBuilder.name(TEST_EVENTS_IN).build());
            kafkaAdmin.createOrModifyTopics(TopicBuilder.name(TEST_EVENTS_OUT).build());

            eventStore
                    .subscribe(TEST_EVENTS_IN)
                    .mapValues(Event::payload)
                    .to(TEST_EVENTS_OUT);
        }
    }

    @Configuration
    @EnableAutoConfiguration
    @Import({ KafkaStreamsEventStore.class, TestStreamsSubscriber.class })
    static class TestConfiguration {
        @Bean
        ReceiverOptions<UUID, SpecificRecord> kafkaReceiverOptions(KafkaProperties properties) {
            var props = properties.buildConsumerProperties(null);

            props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
            props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
            props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                    "org.apache.kafka.common.serialization.UUIDDeserializer");
            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                    "io.confluent.kafka.serializers.KafkaAvroDeserializer");
            props.put("specific.avro.reader", "true");
            props.put("value.subject.name.strategy", "io.confluent.kafka.serializers.subject.RecordNameStrategy");

            return ReceiverOptions.create(props);
        }
    }

    @Test
    @Order(1)
    void savesEventsInKafka() {
        // Given
        var events = IntStream
                .rangeClosed(1, 10)
                .boxed()
                .map(id -> TestPayload.newBuilder().setText("test payload " + id).build())
                .map(payload -> Event.from(UUID.randomUUID(), payload))
                .toList();

        // When
        var savedEvents = Flux.concat(events.stream().map(event -> eventStore.save(TEST_EVENTS, event)).toList());

        // Then
        var retrievedEvents = readEventsFromKafka(TEST_EVENTS)
                .take(events.size())
                .timeout(Duration.ofSeconds(60));

        StepVerifier
                .create(savedEvents.thenMany(retrievedEvents))
                .expectNextSequence(events)
                .as("retrieves stored events with the same data")
                .verifyComplete();
    }

    @Test
    @Order(2)
    void returnsStoredEvent() {
        // Given
        var key = UUID.randomUUID();
        var payload = TestPayload.newBuilder().setText("test payload").build();
        var event = Event.from(key, payload);

        // When
        var storedEvent = eventStore.save(TEST_EVENTS, event);

        // Then
        StepVerifier
                .create(storedEvent)
                .expectNext(event)
                .as("returns stored event")
                .verifyComplete();
    }

    @Test
    @Order(3)
    void subscribesToEventStream() {
        // Given
        var events = IntStream
                .rangeClosed(1, 10)
                .boxed()
                .map(id -> TestPayload.newBuilder().setText("test payload " + id).build())
                .map((SpecificRecord payload) -> Event.from(UUID.randomUUID(), payload))
                .toList();

        var savedEvents = Flux.concat(events.stream().map(event -> eventStore.save(TEST_EVENTS_IN, event)).toList());

        // When
        // topology specified in the bean init method

        // Then
        var retrievedEvents = readEventsFromKafka(TEST_EVENTS_OUT)
                .take(events.size())
                .timeout(Duration.ofSeconds(60));

        StepVerifier
                .create(savedEvents.thenMany(retrievedEvents))
                .expectNextSequence(events)
                .as("retrieves stored events with the same data")
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
