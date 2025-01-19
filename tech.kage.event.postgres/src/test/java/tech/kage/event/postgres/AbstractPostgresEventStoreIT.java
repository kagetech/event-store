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

package tech.kage.event.postgres;

import static java.util.stream.Collectors.toMap;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Named.named;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.Arrays;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Stream;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificRecord;
import org.apache.avro.util.Utf8;
import org.apache.kafka.common.serialization.Deserializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.core.io.Resource;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.test.context.ActiveProfiles;

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
abstract class AbstractPostgresEventStoreIT {
    // UUT
    @Autowired
    PostgresEventStore eventStore;

    @Autowired
    DatabaseClient databaseClient;

    @Autowired
    Deserializer<SpecificRecord> kafkaAvroDeserializer;

    static final Schema METADATA_SCHEMA = SchemaBuilder.map().values().bytesType();

    static final DecoderFactory decoderFactory = DecoderFactory.get();
    static final DatumReader<Map<Utf8, ByteBuffer>> metadataReader = new GenericDatumReader<>(METADATA_SCHEMA);

    @Configuration
    @EnableAutoConfiguration
    @Import(PostgresEventStore.class)
    static class TestConfiguration {
        private static final String DESERIALIZER_CLASS = "io.confluent.kafka.serializers.KafkaAvroDeserializer";

        @Bean
        Deserializer<SpecificRecord> kafkaAvroDeserializer(@Value("${schema.registry.url}") String schemaRegistryUrl) {
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
        private Deserializer<SpecificRecord> getDeserializerInstance() {
            try {
                var ctor = Class.forName(DESERIALIZER_CLASS).getConstructor();

                return (Deserializer<SpecificRecord>) ctor.newInstance();
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

    @ParameterizedTest
    @MethodSource("testEvents")
    void savesEventInDatabase(Event<TestPayload> event) {
        // Given
        var topic = "test_events";

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
                .expectNextMatches(
                        nextEvent -> nextEvent.key().equals(event.key())
                                && nextEvent.payload().equals(event.payload())
                                && nextEvent.timestamp().equals(event.timestamp())
                                && isEqual(nextEvent.metadata(), event.metadata()))
                .as("finds stored event with the same data")
                .verifyComplete();
    }

    @ParameterizedTest
    @MethodSource("testEvents")
    void returnsStoredEvent(Event<TestPayload> event) {
        // Given
        var topic = "test_events";

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
    void throwsExceptionWhenInvalidMetadataValueType() {
        // Given
        var topic = "test_events";

        var key = UUID.randomUUID();
        var payload = TestPayload.newBuilder().setText("test payload").build();
        var timestamp = Instant.ofEpochMilli(1736026628567l);
        var metadataWithInvalidValueType = Map.<String, Object>of("meta1", 123);

        var event = Event.from(key, payload, timestamp, metadataWithInvalidValueType);

        // When
        var thrown = assertThrows(Throwable.class, () -> eventStore.save(topic, event).block());

        // Then
        assertThat(thrown)
                .describedAs("thrown exception")
                .isInstanceOf(ClassCastException.class);
    }

    static Stream<Arguments> testEvents() {
        return Stream.of(
                arguments(
                        named(
                                "payload only",
                                Event.from(TestPayload.newBuilder().setText("test payload 1").build()))),
                arguments(
                        named(
                                "key and payload",
                                Event.from(
                                        UUID.fromString("ea09ab50-8564-485f-9363-d4a4b3d6e9ca"),
                                        TestPayload.newBuilder().setText("test payload 2").build()))),
                arguments(
                        named(
                                "key, payload and timestamp",
                                Event.from(
                                        UUID.fromString("58ce74c8-64d3-45d0-a35b-f99e8e551a51"),
                                        TestPayload.newBuilder().setText("test payload 3").build(),
                                        Instant.ofEpochMilli(1736025221442l)))),
                arguments(
                        named(
                                "key, payload, timestamp and metadata",
                                Event.from(
                                        UUID.fromString("ba7b9608-ccae-472c-99cf-b29e038adab1"),
                                        TestPayload.newBuilder().setText("test payload 4").build(),
                                        Instant.ofEpochMilli(1736026528567l),
                                        Map.of(
                                                "meta1", "meta1_value".getBytes(),
                                                "meta2", UUID.randomUUID().toString().getBytes(),
                                                "meta3", Long.toString(123l).getBytes())))));
    }

    private Mono<Event<?>> toEvent(Map<String, Object> row) {
        var key = (UUID) row.get("key");
        var data = (ByteBuffer) row.get("data");
        var metadata = (ByteBuffer) row.get("metadata");
        var timestamp = (OffsetDateTime) row.get("timestamp");

        return Mono
                .just(data.array())
                .publishOn(Schedulers.boundedElastic())
                .map(bytes -> kafkaAvroDeserializer.deserialize(null, bytes))
                .map(payload -> Event.from(key, payload, timestamp.toInstant(), deserialize(metadata)));
    }

    private Map<String, Object> deserialize(ByteBuffer metadata) {
        if (metadata == null) {
            return Map.of();
        }

        try {
            var decoder = decoderFactory.binaryDecoder(metadata.array(), null);

            return metadataReader
                    .read(null, decoder)
                    .entrySet()
                    .stream()
                    .map(entry -> Map.entry(entry.getKey().toString(), entry.getValue().array()))
                    .collect(toMap(Map.Entry::getKey, Map.Entry::getValue));
        } catch (IOException e) {
            throw new UncheckedIOException("Unable to deserialize metadata", e);
        }
    }

    private boolean isEqual(Map<String, Object> actual, Map<String, Object> expected) {
        if (actual.size() != expected.size()) {
            return false;
        }

        return actual
                .entrySet()
                .stream()
                .allMatch(e -> Arrays.equals((byte[]) e.getValue(), (byte[]) expected.get(e.getKey())));
    }
}
