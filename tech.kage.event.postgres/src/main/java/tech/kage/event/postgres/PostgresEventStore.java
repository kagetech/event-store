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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.time.ZoneOffset;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.serialization.Serializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.stereotype.Component;

import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import tech.kage.event.Event;
import tech.kage.event.EventStore;

/**
 * A PostgreSQL-based implementation of {@link EventStore} storing events in
 * relational database tables. Uses Apache Avro for payload serialization and
 * stores Avro schemas in Confluent Schema Registry.
 * 
 * <p>
 * Can be configured in two ways:
 * 
 * <ul>
 * <li>{@code schema.registry.url} configuration property that points to a
 * Confluent Schema Registry instance</li>
 * <li>{@link KafkaProperties} bean for setting any configuration property</li>
 * </ul>
 * 
 * @author Dariusz Szpakowski
 */
@Component
public class PostgresEventStore implements EventStore {
    private static final String INSERT_EVENT_SQL = """
                INSERT INTO events.%s (key, data, timestamp)
                VALUES (:key, :data, :timestamp)
            """;
    private static final String INSERT_EVENT_WITH_METADATA_SQL = """
                INSERT INTO events.%s (key, data, metadata, timestamp)
                VALUES (:key, :data, :metadata, :timestamp)
            """;

    private static final String SERIALIZER_CLASS = "io.confluent.kafka.serializers.KafkaAvroSerializer";

    private final DatabaseClient databaseClient;
    private final Serializer<Object> kafkaAvroSerializer;

    /**
     * Constructs a new {@link PostgresEventStore} instance.
     *
     * @param databaseClient    an instance of {@link DatabaseClient}
     * @param schemaRegistryUrl address pointing to a Confluent Schema Registry
     *                          instance (required if {@link KafkaProperties} bean
     *                          is not available)
     * @param kafkaProperties   {@link KafkaProperties} bean used for configuring
     *                          the serializer (optional)
     */
    PostgresEventStore(
            DatabaseClient databaseClient,
            @Value("${schema.registry.url:#{null}}") String schemaRegistryUrl,
            Optional<KafkaProperties> kafkaProperties) {
        this.databaseClient = databaseClient;

        var serializerConfig = kafkaProperties.isPresent()
                ? kafkaProperties.get().getProperties()
                : Map.of(
                        "schema.registry.url",
                        Objects.requireNonNull(schemaRegistryUrl, "schema.registry.url must be set"),
                        "value.subject.name.strategy", "io.confluent.kafka.serializers.subject.RecordNameStrategy");

        kafkaAvroSerializer = getSerializerInstance();
        kafkaAvroSerializer.configure(serializerConfig, false);
    }

    @Override
    public <T extends SpecificRecord> Mono<Event<T>> save(String topic, Event<T> event) {
        Objects.requireNonNull(topic, "topic must not be null");
        Objects.requireNonNull(event, "event must not be null");

        return Mono
                .fromCallable(() -> kafkaAvroSerializer.serialize(topic, event.payload()))
                .subscribeOn(Schedulers.boundedElastic())
                .flatMap(serialized -> databaseClient
                        .sql(event.metadata().isEmpty()
                                ? INSERT_EVENT_SQL.formatted(topic)
                                : INSERT_EVENT_WITH_METADATA_SQL.formatted(topic))
                        .bind("key", event.key())
                        .bind("data", serialized)
                        .bind("timestamp", event.timestamp().atOffset(ZoneOffset.UTC))
                        .bindValues(
                                event.metadata().isEmpty()
                                        ? Map.of()
                                        : Map.of("metadata", MetadataSerializer.serialize(event.metadata())))
                        .fetch()
                        .rowsUpdated())
                .map(oneInserted -> event);
    }

    /**
     * Constructs a new Kafka Avro Serializer instance. Uses reflection because
     * {@code kafka-avro-serializer} dependency is not compatible with
     * {@code module-info.java} (split package).
     * 
     * @return new Kafka Avro Serializer instance
     */
    @SuppressWarnings("unchecked")
    private Serializer<Object> getSerializerInstance() {
        try {
            var ctor = Class.forName(SERIALIZER_CLASS).getConstructor();

            return (Serializer<Object>) ctor.newInstance();
        } catch (Exception e) {
            throw new IllegalArgumentException("Unable to instantiate serializer " + SERIALIZER_CLASS, e);
        }
    }

    /**
     * Serializer of event metadata to an Avro map.
     */
    private static class MetadataSerializer {
        private static final Schema METADATA_SCHEMA = SchemaBuilder.map().values().bytesType();

        private static final EncoderFactory encoderFactory = EncoderFactory.get();
        private static final DatumWriter<Map<String, ByteBuffer>> writer = new GenericDatumWriter<>(METADATA_SCHEMA);

        /**
         * Serialize the given metadata map to an Avro map.
         * 
         * @param metadata metadata map to serialize
         * 
         * @return bytes representing the serialized Avro map
         */
        private static byte[] serialize(Map<String, Object> metadata) {
            var convertedMetadata = metadata
                    .entrySet()
                    .stream()
                    .map(entry -> Map.entry(entry.getKey(), ByteBuffer.wrap((byte[]) entry.getValue())))
                    .collect(toMap(Map.Entry::getKey, Map.Entry::getValue));

            try (var out = new ByteArrayOutputStream()) {
                var encoder = encoderFactory.directBinaryEncoder(out, null);

                writer.write(convertedMetadata, encoder);

                return out.toByteArray();
            } catch (IOException e) {
                throw new UncheckedIOException("Unable to serialize metadata: " + metadata, e);
            }
        }
    }
}
