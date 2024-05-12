/*
 * Copyright (c) 2023, Dariusz Szpakowski
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

package tech.kage.event.postgres;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.OffsetDateTime;
import java.util.Map;
import java.util.UUID;

import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.serialization.Deserializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.core.io.Resource;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;
import tech.kage.event.Event;

/**
 * Integration tests for {@link PostgresEventStore}.
 * 
 * @author Dariusz Szpakowski
 */
@SpringBootTest
@ActiveProfiles("test")
@Testcontainers
class PostgresEventStoreIT {
    // UUT
    @Autowired
    PostgresEventStore eventStore;

    @Autowired
    DatabaseClient databaseClient;

    @Autowired
    Deserializer<Object> kafkaAvroDeserializer;

    static final Network network = Network.newNetwork();

    @Container
    @ServiceConnection
    static final PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:15-alpine");

    @SuppressWarnings("resource")
    @Container
    static final KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.1"))
            .withNetwork(network)
            .withKraft();

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
        registry.add(
                "schema.registry.url",
                () -> "http://%s:%s".formatted(schemaRegistry.getHost(), schemaRegistry.getFirstMappedPort()));
    }

    @Configuration
    @EnableAutoConfiguration
    @Import(PostgresEventStore.class)
    static class TestConfiguration {
        private static final String DESERIALIZER_CLASS = "io.confluent.kafka.serializers.KafkaAvroDeserializer";

        @Bean
        Deserializer<Object> kafkaAvroDeserializer(@Value("${schema.registry.url}") String schemaRegistryUrl) {
            var deserializerConfig = Map.of(
                    "value.subject.name.strategy", "io.confluent.kafka.serializers.subject.RecordNameStrategy",
                    "specific.avro.reader", true,
                    "schema.registry.url", schemaRegistryUrl);

            var kafkaAvroDeserializer = getDeserializerInstance();
            kafkaAvroDeserializer.configure(deserializerConfig, false);

            return kafkaAvroDeserializer;
        }

        /**
         * Constructs a new Kafka Avro Deserializer instance. Uses reflection because
         * {@code kafka-avro-serializer} dependency is not compatible with
         * {@code module-info.java} (split package).
         * 
         * @return new Kafka Avro Serializer instance
         */
        @SuppressWarnings("unchecked")
        private Deserializer<Object> getDeserializerInstance() {
            try {
                var ctor = Class.forName(DESERIALIZER_CLASS).getConstructor();

                return (Deserializer<Object>) ctor.newInstance();
            } catch (Exception e) {
                throw new IllegalArgumentException("Unable to instantiate deserializer " + DESERIALIZER_CLASS, e);
            }
        }
    }

    @BeforeEach
    void setUp(@Value("classpath:/test-data/events/ddl.sql") Resource ddl) throws IOException {
        databaseClient
                .sql(ddl.getContentAsString(StandardCharsets.UTF_8))
                .fetch()
                .rowsUpdated()
                .block();
    }

    @Test
    void savesEventInDatabase() {
        // Given
        var topic = "test_events";
        var key = UUID.randomUUID();
        var payload = TestPayload.newBuilder().setText("test payload").build();
        var event = Event.from(key, payload);

        var eventCount = databaseClient
                .sql("SELECT count(*) FROM events.test_events")
                .fetch()
                .one()
                .map(row -> row.get("count"));

        StepVerifier
                .create(eventCount)
                .expectNext(Long.valueOf(0))
                .as("empty events table at test start")
                .verifyComplete();

        // When
        eventStore.save(topic, event).block();

        // Then
        var retrievedEvent = databaseClient
                .sql("SELECT * FROM events.test_events")
                .fetch()
                .one()
                .flatMap(this::toEvent);

        StepVerifier
                .create(retrievedEvent)
                .expectNext(event)
                .as("finds stored event with the same data")
                .verifyComplete();
    }

    @Test
    void returnsStoredEvent() {
        // Given
        var topic = "test_events";
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

    private Mono<Event<?>> toEvent(Map<String, Object> row) {
        var key = (UUID) row.get("key");
        var data = (ByteBuffer) row.get("data");
        var timestamp = (OffsetDateTime) row.get("timestamp");

        return Mono
                .just(data.array())
                .publishOn(Schedulers.boundedElastic())
                .map(bytes -> kafkaAvroDeserializer.deserialize(null, bytes))
                .map(payload -> Event.from(key, (SpecificRecord) payload, timestamp.toInstant()));
    }
}
