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
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.SequencedMap;
import java.util.TreeMap;
import java.util.UUID;
import java.util.stream.IntStream;

import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.kstream.Produced;
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
abstract class KafkaStreamsEventStoreIT<K> {
    // UUT
    @Autowired
    KafkaStreamsEventStore<K, SpecificRecord> eventStore;

    @Autowired
    ReceiverOptions<Object, byte[]> kafkaReceiverOptions;

    @Autowired
    Deserializer<SpecificRecord> kafkaAvroDeserializer;

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
            DockerImageName.parse("confluentinc/cp-schema-registry:7.8.0"))
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
        void init(KafkaAdmin kafkaAdmin, KafkaStreamsEventStore<Object, SpecificRecord> eventStore) {
            kafkaAdmin.createOrModifyTopics(TopicBuilder.name(TEST_EVENTS).build());
            kafkaAdmin.createOrModifyTopics(TopicBuilder.name(TEST_EVENTS_IN).build());
            kafkaAdmin.createOrModifyTopics(TopicBuilder.name(TEST_EVENTS_OUT).build());

            eventStore
                    .subscribe(TEST_EVENTS_IN)
                    .mapValues(KafkaStreamsEventStoreIT::fakeProcessing)
                    .processValues(() -> eventStore.new OutputEventTransformer(TEST_EVENTS_OUT))
                    .to(TEST_EVENTS_OUT, Produced.valueSerde(Serdes.ByteArray()));
        }
    }

    @Configuration
    @EnableAutoConfiguration
    @Import({ KafkaStreamsEventStore.class, TestStreamsSubscriber.class })
    static class TestConfiguration {
        @Bean
        ReceiverOptions<Object, byte[]> kafkaReceiverOptions(KafkaProperties properties) {
            var props = properties.buildConsumerProperties(null);

            props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
            props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
            props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());

            return ReceiverOptions.create(props);
        }
    }

    @Test
    @Order(1)
    void savesEventsInKafka() {
        // Given
        var events = testEvents(10);

        var expectedEventsIterator = events.iterator();

        // When
        var savedEvents = Flux.concat(events.stream().map(event -> eventStore.save(TEST_EVENTS, event)).toList());

        // Then
        var retrievedEvents = KafkaReceiver
                .create(kafkaReceiverOptions.assignment(List.of(new TopicPartition(TEST_EVENTS, 0))))
                .receive()
                .take(events.size())
                .timeout(Duration.ofSeconds(60));

        StepVerifier
                .create(savedEvents.thenMany(retrievedEvents))
                .thenConsumeWhile(message -> {
                    var key = message.key();
                    var payload = kafkaAvroDeserializer.deserialize(null, message.value());
                    var timestamp = Instant.ofEpochMilli(message.timestamp());
                    var metadata = toSequencedMap(message.headers());

                    var expectedEvent = expectedEventsIterator.next();

                    // the expected metadata are sorted by key
                    var expectedMetadata = new TreeMap<>(expectedEvent.metadata());

                    return isEqual(key, expectedEvent.key())
                            && payload.equals(expectedEvent.payload())
                            && timestamp.equals(expectedEvent.timestamp())
                            && isEqualOrdered(metadata, expectedMetadata);
                })
                .as("retrieves stored events with the same data")
                .verifyComplete();
    }

    @Test
    @Order(2)
    void returnsStoredEvent() {
        // Given
        var event = testEvent(123);

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
        var events = testEvents(10);

        var savedEvents = Flux.concat(events.stream().map(event -> eventStore.save(TEST_EVENTS_IN, event)).toList());

        var expectedEventsIterator = events.iterator();

        // When
        // topology specified in the bean init method

        // Then
        var retrievedEvents = KafkaReceiver
                .create(kafkaReceiverOptions.assignment(List.of(new TopicPartition(TEST_EVENTS_OUT, 0))))
                .receive()
                .take(events.size())
                .timeout(Duration.ofSeconds(60));

        StepVerifier
                .create(savedEvents.thenMany(retrievedEvents))
                .thenConsumeWhile(message -> {
                    var key = message.key();
                    var payload = kafkaAvroDeserializer.deserialize(null, message.value());
                    var timestamp = Instant.ofEpochMilli(message.timestamp());
                    var metadata = toSequencedMap(message.headers());

                    var expectedEvent = expectedEventAfterFakeProcessing(expectedEventsIterator.next());

                    // the expected metadata are sorted by key
                    var expectedMetadata = new TreeMap<>(expectedEvent.metadata());

                    return isEqual(key, expectedEvent.key())
                            && payload.equals(expectedEvent.payload())
                            && timestamp.equals(expectedEvent.timestamp())
                            && isEqualOrdered(metadata, expectedMetadata);
                })
                .as("retrieves stored events with the same data")
                .verifyComplete();
    }

    protected List<Event<K, SpecificRecord>> testEvents(int count) {
        return IntStream
                .rangeClosed(1, count)
                .boxed()
                .map(this::testEvent)
                .toList();
    }

    private Event<K, SpecificRecord> testEvent(int offset) {
        return Event.from(
                getTestEventKey(offset),
                TestPayload.newBuilder().setText("test payload " + offset).build(),
                Instant.now(),
                Map.of(
                        "dTest", "meta_value".getBytes(),
                        "zTest", UUID.randomUUID().toString().getBytes(),
                        "bTest", Long.toString(offset).getBytes()));
    }

    protected SequencedMap<String, Object> toSequencedMap(Iterable<Header> headers) {
        var associatedMetadata = new LinkedHashMap<String, Object>();

        for (var header : headers) {
            associatedMetadata.put(header.key(), header.value());
        }

        return associatedMetadata;
    }

    protected boolean isEqualOrdered(Map<String, Object> actual, Map<String, Object> expected) {
        if (actual.size() != expected.size()) {
            return false;
        }

        var actualKeys = actual.keySet().iterator();
        var expectedKeys = expected.keySet().iterator();

        while (actualKeys.hasNext()) {
            var nextActualKey = actualKeys.next();
            var nextExpectedKey = expectedKeys.next();

            var nextActualValue = actual.get(nextActualKey);
            var nextExpectedValue = expected.get(nextExpectedKey);

            if (!nextActualKey.equals(nextExpectedKey)) {
                return false;
            }

            if (nextActualValue instanceof byte[] actualBytes && nextExpectedValue instanceof byte[] expectedBytes) {
                if (!Arrays.equals(actualBytes, expectedBytes)) {
                    return false;
                }
            } else if (!nextActualValue.equals(nextExpectedValue)) {
                return false;
            }
        }

        return true;
    }

    protected boolean isEqual(Object actual, Object expected) {
        if (actual instanceof byte[] actualByteArray && expected instanceof byte[] expectedByteArray) {
            return Arrays.equals(actualByteArray, expectedByteArray);
        } else {
            return actual.equals(expected);
        }
    }

    protected abstract K getTestEventKey(int id);

    private static Event<Object, SpecificRecord> fakeProcessing(Event<Object, SpecificRecord> event) {
        return Event.from(
                event.key(),
                TestPayload.newBuilder().setText(((TestPayload) event.payload()).getText() + " (processed)").build(),
                event.timestamp().plusSeconds(3),
                Map.of("dTest", event.metadata().get("header.dTest")));
    }

    private Event<K, SpecificRecord> expectedEventAfterFakeProcessing(Event<K, SpecificRecord> event) {
        return Event.from(
                event.key(),
                TestPayload.newBuilder().setText(((TestPayload) event.payload()).getText() + " (processed)").build(),
                event.timestamp().plusSeconds(3),
                Map.of("dTest", event.metadata().get("dTest")));
    }
}
