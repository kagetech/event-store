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

import static java.util.Comparator.comparing;
import static java.util.stream.Collectors.toCollection;
import static tech.kage.event.EventStore.ENCRYPTION_KEY_ID;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.Serializer;
import org.springframework.stereotype.Component;

import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import tech.kage.event.Event;
import tech.kage.event.crypto.EventEncryptor;

/**
 * Transformer of {@link Event} instances to Kafka records.
 * 
 * @author Dariusz Szpakowski
 */
@Component
class ProducerRecordEventTransformer {
    private final Serializer<SpecificRecord> kafkaAvroSerializer;
    private final EventEncryptor eventEncryptor;

    /**
     * Constructs a new {@link ProducerRecordEventTransformer} instance.
     *
     * @param kafkaAvroSerializer an instance of {@link Serializer}
     * @param eventEncryptor      an instance of {@link EventEncryptor}
     */
    ProducerRecordEventTransformer(Serializer<SpecificRecord> kafkaAvroSerializer, EventEncryptor eventEncryptor) {
        this.kafkaAvroSerializer = kafkaAvroSerializer;
        this.eventEncryptor = eventEncryptor;
    }

    /**
     * Transforms the specified {@link Event} into a Kafka {@link ProducerRecord}.
     * 
     * @param event         event to be transformed
     * @param topic         topic used for schema selection
     * @param encryptionKey encryption key to use
     * 
     * @return a {@link ProducerRecord} created from the given {@link Event}
     */
    Mono<ProducerRecord<UUID, byte[]>> transform(Event<?> event, String topic, URI encryptionKey) {
        return Mono
                .fromCallable(() -> kafkaAvroSerializer.serialize(topic, event.payload()))
                .subscribeOn(Schedulers.boundedElastic())
                .flatMap(serialized -> encryptionKey != null
                        ? eventEncryptor.encrypt(
                                serialized, event.key(), event.timestamp(), event.metadata(), encryptionKey)
                        : Mono.just(serialized))
                .map(serialized -> new ProducerRecord<>(
                        topic,
                        null,
                        event.timestamp().toEpochMilli(),
                        event.key(),
                        serialized,
                        prepareHeaders(event.metadata(), encryptionKey)));
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
            preparedHeaderList.add(new RecordHeader(ENCRYPTION_KEY_ID, encryptionKey.toString().getBytes()));
        }

        return preparedHeaderList
                .stream()
                .sorted(comparing(Header::key))
                .toList();
    }
}
