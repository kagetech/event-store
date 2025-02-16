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

package tech.kage.event.kafka.streams;

import java.security.GeneralSecurityException;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.SequencedMap;

import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.streams.processor.api.FixedKeyRecord;
import org.apache.kafka.streams.processor.api.RecordMetadata;
import org.springframework.stereotype.Component;

import tech.kage.event.Event;
import tech.kage.event.crypto.EventEncryptor;

/**
 * Transformer of Kafka Streams messages to {@link Event} instances.
 * 
 * @author Dariusz Szpakowski
 */
@Component
class KafkaStreamsEventTransformer<K, V extends SpecificRecord> {
    private static final String METADATA_PARTITION = "partition";
    private static final String METADATA_OFFSET = "offset";
    private static final String METADATA_HEADER_PREFIX = "header.";

    private final Deserializer<V> kafkaAvroDeserializer;
    private final EventEncryptor eventEncryptor;

    /**
     * Constructs a new {@link KafkaStreamsEventTransformer} instance.
     *
     * @param kafkaAvroDeserializer an instance of {@link Deserializer}
     * @param eventEncryptor        an instance of {@link EventEncryptor}
     */
    KafkaStreamsEventTransformer(Deserializer<V> kafkaAvroDeserializer, EventEncryptor eventEncryptor) {
        this.kafkaAvroDeserializer = kafkaAvroDeserializer;
        this.eventEncryptor = eventEncryptor;
    }

    /**
     * Transforms the specified Kafka Streams {@link FixedKeyRecord} into an
     * {@link Event}.
     * 
     * @param message        Kafka Streams message to be transformed
     * @param recordMetadata optional with {@link RecordMetadata}
     * 
     * @return an {@link Event} created from the given {@link FixedKeyRecord}
     * 
     * @throws SerializationException if event payload decryption fails
     */
    Event<K, V> transform(FixedKeyRecord<K, byte[]> message, Optional<RecordMetadata> recordMetadata) {
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

        return Event.from(key, deserializedPayload, timestamp, metadata(message, recordMetadata));
    }

    private SequencedMap<String, Object> toSequencedMap(Iterable<Header> headers) {
        var associatedMetadata = new LinkedHashMap<String, Object>();

        for (var header : headers) {
            associatedMetadata.put(header.key(), header.value());
        }

        return associatedMetadata;
    }

    private Map<String, Object> metadata(FixedKeyRecord<?, ?> message, Optional<RecordMetadata> recordMetadata) {
        var metadataMap = new HashMap<String, Object>();

        if (recordMetadata.isPresent()) {
            metadataMap.put(METADATA_PARTITION, recordMetadata.get().partition());
            metadataMap.put(METADATA_OFFSET, recordMetadata.get().offset());
        }

        for (var header : message.headers()) {
            metadataMap.put(METADATA_HEADER_PREFIX + header.key(), header.value());
        }

        return Collections.unmodifiableMap(metadataMap);
    }
}
