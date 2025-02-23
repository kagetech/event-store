/*
 * Copyright (c) 2025, Dariusz Szpakowski
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

import static java.util.Comparator.comparing;
import static java.util.stream.Collectors.toCollection;

import java.net.URI;
import java.security.GeneralSecurityException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.SequencedMap;

import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Component;

import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.kafka.receiver.ReceiverRecord;
import reactor.kafka.sender.SenderRecord;
import tech.kage.event.Event;
import tech.kage.event.EventStore;
import tech.kage.event.crypto.EventEncryptor;

/**
 * Transformer of {@link Event} instances to/from Reactor Kafka Records.
 * 
 * @author Dariusz Szpakowski
 */
@Component
class ReactorKafkaEventTransformer {
    private static final String METADATA_PARTITION = "partition";
    private static final String METADATA_OFFSET = "offset";
    private static final String METADATA_HEADER_PREFIX = "header.";

    private final Serializer<SpecificRecord> kafkaAvroSerializer;
    private final Deserializer<SpecificRecord> kafkaAvroDeserializer;
    private final EventEncryptor eventEncryptor;

    /**
     * Constructs a new {@link ReactorKafkaEventTransformer} instance.
     *
     * @param kafkaAvroSerializer   an instance of {@link Serializer}
     * @param kafkaAvroDeserializer an instance of {@link Deserializer}
     * @param eventEncryptor        an instance of {@link EventEncryptor}
     */
    ReactorKafkaEventTransformer(
            Serializer<SpecificRecord> kafkaAvroSerializer,
            Deserializer<SpecificRecord> kafkaAvroDeserializer,
            EventEncryptor eventEncryptor) {
        this.kafkaAvroSerializer = kafkaAvroSerializer;
        this.kafkaAvroDeserializer = kafkaAvroDeserializer;
        this.eventEncryptor = eventEncryptor;
    }

    /**
     * Transforms the specified {@link Event} into a Reactor Kafka
     * {@link SenderRecord}.
     * 
     * @param event         event to be transformed
     * @param topic         topic used for schema selection
     * @param encryptionKey encryption key to use
     * 
     * @return a {@link SenderRecord} created from the given {@link Event}
     */
    Mono<SenderRecord<Object, byte[], Object>> transform(Event<?, ?> event, String topic, URI encryptionKey) {
        return Mono
                .fromCallable(() -> kafkaAvroSerializer.serialize(topic, event.payload()))
                .subscribeOn(Schedulers.boundedElastic())
                .flatMap(serialized -> encryptionKey != null
                        ? Mono.fromCallable(() -> eventEncryptor.encrypt(
                                serialized, event.key(), event.timestamp(), event.metadata(), encryptionKey))
                        : Mono.just(serialized))
                .map(serialized -> new ProducerRecord<Object, byte[]>(
                        topic,
                        null,
                        event.timestamp().toEpochMilli(),
                        event.key(),
                        serialized,
                        prepareHeaders(event.metadata(), encryptionKey)))
                .map(producerRecord -> SenderRecord.create(producerRecord, null));
    }

    /**
     * Transforms the specified Reactor Kafka {@link ReceiverRecord} into an
     * {@link Event}.
     * 
     * @param receiverRecord Kafka message to be transformed
     * 
     * @return an {@link Event} created from the given {@link ReceiverRecord}
     * 
     * @throws SerializationException if event payload decryption fails
     */
    <K> Event<K, SpecificRecord> transform(ReceiverRecord<K, byte[]> message) {
        var key = message.key();
        var encryptedPayload = message.value();
        var timestamp = Instant.ofEpochMilli(message.timestamp());

        byte[] decryptedPayload;

        try {
            decryptedPayload = eventEncryptor.decrypt(
                    encryptedPayload, key, timestamp, toSequencedMap(message.headers()));
        } catch (GeneralSecurityException e) {
            throw new SerializationException("Error decrypting event payload", e);
        }

        var deserializedPayload = kafkaAvroDeserializer.deserialize(null, decryptedPayload);

        return Event.from(key, deserializedPayload, timestamp, metadata(message));
    }

    private List<Header> prepareHeaders(Map<String, Object> metadata, URI encryptionKey) {
        if (metadata.isEmpty() && encryptionKey == null) {
            return List.of();
        }

        var preparedHeaderList = metadata
                .entrySet()
                .stream()
                .map(e -> new RecordHeader(e.getKey(), (byte[]) e.getValue()))
                .map(Header.class::cast)
                .collect(toCollection(ArrayList::new));

        if (encryptionKey != null) {
            preparedHeaderList.add(new RecordHeader(EventStore.ENCRYPTION_KEY_ID, encryptionKey.toString().getBytes()));
        }

        return preparedHeaderList
                .stream()
                .sorted(comparing(Header::key))
                .toList();
    }

    private SequencedMap<String, Object> toSequencedMap(Iterable<Header> headers) {
        var associatedMetadata = new LinkedHashMap<String, Object>();

        for (var header : headers) {
            associatedMetadata.put(header.key(), header.value());
        }

        return associatedMetadata;
    }

    private Map<String, Object> metadata(ReceiverRecord<?, byte[]> message) {
        var metadataMap = new HashMap<String, Object>();

        metadataMap.put(METADATA_PARTITION, message.partition());
        metadataMap.put(METADATA_OFFSET, message.offset());

        for (var header : message.headers()) {
            metadataMap.put(METADATA_HEADER_PREFIX + header.key(), header.value());
        }

        return Collections.unmodifiableMap(metadataMap);
    }

    @Configuration
    static class Config {
        private static final String VALUE_SUBJECT_NAME_STRATEGY_CONFIG = "value.subject.name.strategy";
        private static final String VALUE_SUBJECT_NAME_STRATEGY = "io.confluent.kafka.serializers.subject.RecordNameStrategy";

        private static final String SPECIFIC_AVRO_READER_CONFIG = "specific.avro.reader";

        private static final String KAFKA_AVRO_SERIALIZER_CLASS = "io.confluent.kafka.serializers.KafkaAvroSerializer";
        private static final String KAFKA_AVRO_DESERIALIZER_CLASS = "io.confluent.kafka.serializers.KafkaAvroDeserializer";

        @Bean
        Serializer<SpecificRecord> kafkaAvroSerializer(KafkaProperties properties) {
            var serializerConfig = new HashMap<>(properties.getProperties());

            serializerConfig.putIfAbsent(VALUE_SUBJECT_NAME_STRATEGY_CONFIG, VALUE_SUBJECT_NAME_STRATEGY);

            // Construct a new Kafka Avro Serializer instance via reflection because
            // kafka-avro-serializer dependency is not compatible with module-info.java
            // (split package).

            @SuppressWarnings("unchecked")
            var kafkaAvroSerializer = (Serializer<SpecificRecord>) getInstance(KAFKA_AVRO_SERIALIZER_CLASS);

            kafkaAvroSerializer.configure(serializerConfig, false);

            return kafkaAvroSerializer;
        }

        @Bean
        Deserializer<SpecificRecord> kafkaAvroDeserializer(KafkaProperties properties) {
            var deserializerConfig = new HashMap<>(properties.getProperties());

            deserializerConfig.putIfAbsent(SPECIFIC_AVRO_READER_CONFIG, "true");
            deserializerConfig.putIfAbsent(VALUE_SUBJECT_NAME_STRATEGY_CONFIG, VALUE_SUBJECT_NAME_STRATEGY);

            // Construct a new Kafka Avro Deserializer instance via reflection because
            // kafka-avro-serializer dependency is not compatible with module-info.java
            // (split package).

            @SuppressWarnings("unchecked")
            var kafkaAvroDeserializer = (Deserializer<SpecificRecord>) getInstance(KAFKA_AVRO_DESERIALIZER_CLASS);

            kafkaAvroDeserializer.configure(deserializerConfig, false);

            return kafkaAvroDeserializer;
        }

        private Object getInstance(String className) {
            try {
                var ctor = Class.forName(className).getConstructor();

                return ctor.newInstance();
            } catch (Exception e) {
                throw new IllegalArgumentException("Unable to instantiate " + className, e);
            }
        }
    }
}
