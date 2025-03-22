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

import java.net.URI;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.serialization.Serializer;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.stereotype.Component;

import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import tech.kage.event.Event;
import tech.kage.event.EventStore;
import tech.kage.event.crypto.EventEncryptor;
import tech.kage.event.crypto.MetadataSerializer;

/**
 * A PostgreSQL-based implementation of {@link EventStore} storing events in
 * relational database tables. Uses Apache Avro for payload serialization and
 * stores Avro schemas in Confluent Schema Registry.
 * 
 * <p>
 * Can be configured in two ways:
 * 
 * <ol>
 * <li>{@code schema.registry.url} configuration property that points to a
 * Confluent Schema Registry instance</li>
 * <li>{@link KafkaProperties} bean for setting any configuration property</li>
 * </ol>
 * 
 * @param <K> the type of stored events' keys
 * @param <V> the type of stored events' payloads
 * 
 * @author Dariusz Szpakowski
 */
@Component
public class PostgresEventStore<K, V extends SpecificRecord> implements EventStore<K, V> {
    private static final String INSERT_EVENT_SQL = """
                INSERT INTO events.%s (key, data, timestamp)
                VALUES (:key, :data, :timestamp)
            """;
    private static final String INSERT_EVENT_WITH_METADATA_SQL = """
                INSERT INTO events.%s (key, data, metadata, timestamp)
                VALUES (:key, :data, :metadata, :timestamp)
            """;

    private final DatabaseClient databaseClient;
    private final Serializer<SpecificRecord> kafkaAvroSerializer;
    private final EventEncryptor eventEncryptor;

    /**
     * Constructs a new {@link PostgresEventStore} instance.
     *
     * @param databaseClient      an instance of {@link DatabaseClient}
     * @param kafkaAvroSerializer an instance of {@link Serializer}
     * @param eventEncryptor      an instance of {@link EventEncryptor}
     */
    PostgresEventStore(
            DatabaseClient databaseClient,
            Serializer<SpecificRecord> kafkaAvroSerializer,
            EventEncryptor eventEncryptor) {
        this.databaseClient = databaseClient;
        this.kafkaAvroSerializer = kafkaAvroSerializer;
        this.eventEncryptor = eventEncryptor;
    }

    @Override
    public Mono<Event<K, V>> save(String topic, Event<K, V> event) {
        return doSave(topic, event, null);
    }

    @Override
    public Mono<Event<K, V>> save(String topic, Event<K, V> event, URI encryptionKey) {
        Objects.requireNonNull(encryptionKey, "encryptionKey must not be null");

        return doSave(topic, event, encryptionKey);
    }

    private Mono<Event<K, V>> doSave(String topic, Event<K, V> event, URI encryptionKey) {
        Objects.requireNonNull(topic, "topic must not be null");
        Objects.requireNonNull(event, "event must not be null");

        if (event.metadata().containsKey(SOURCE_ID)) {
            throw new IllegalArgumentException(SOURCE_ID + " must not be set in metadata");
        }

        if (event.metadata().containsKey(ENCRYPTION_KEY_ID)) {
            throw new IllegalArgumentException(ENCRYPTION_KEY_ID + " must not be set in metadata");
        }

        return Mono
                .fromCallable(() -> kafkaAvroSerializer.serialize(topic, event.payload()))
                .subscribeOn(Schedulers.boundedElastic())
                .flatMap(serialized -> encryptionKey != null
                        ? Mono.fromCallable(() -> eventEncryptor.encrypt(
                                serialized, event.key(), event.timestamp(), event.metadata(), encryptionKey))
                        : Mono.just(serialized))
                .flatMap(serialized -> databaseClient
                        .sql(event.metadata().isEmpty() && encryptionKey == null
                                ? INSERT_EVENT_SQL.formatted(topic)
                                : INSERT_EVENT_WITH_METADATA_SQL.formatted(topic))
                        .bind("key", event.key())
                        .bind("data", serialized)
                        .bind("timestamp", event.timestamp().atOffset(ZoneOffset.UTC))
                        .bindValues(
                                event.metadata().isEmpty() && encryptionKey == null
                                        ? Map.of()
                                        : Map.of("metadata", prepareMetadataColumn(event.metadata(), encryptionKey)))
                        .fetch()
                        .rowsUpdated())
                .map(oneInserted -> event);
    }

    /**
     * Prepares the metadata column.
     * 
     * @param metadata      source metadata
     * @param encryptionKey encryption key id to store in metadata
     * 
     * @return serialized metadata map with added encryption key id
     */
    private byte[] prepareMetadataColumn(Map<String, Object> metadata, URI encryptionKey) {
        var preparedMetadata = new HashMap<String, Object>(metadata);

        if (encryptionKey != null) {
            preparedMetadata.put(ENCRYPTION_KEY_ID, encryptionKey.toString().getBytes());
        }

        return MetadataSerializer.serialize(preparedMetadata);
    }
}
