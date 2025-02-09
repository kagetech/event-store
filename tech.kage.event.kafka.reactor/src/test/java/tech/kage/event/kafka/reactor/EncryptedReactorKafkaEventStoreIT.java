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

import static com.google.crypto.tink.aead.PredefinedAeadParameters.AES256_GCM;
import static org.springframework.beans.factory.config.BeanDefinition.SCOPE_PROTOTYPE;
import static tech.kage.event.EventStore.ENCRYPTION_KEY_ID;

import java.net.URI;
import java.security.GeneralSecurityException;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.SequencedMap;
import java.util.TreeMap;
import java.util.UUID;
import java.util.stream.IntStream;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Scope;

import com.google.crypto.tink.Aead;
import com.google.crypto.tink.KeysetHandle;
import com.google.crypto.tink.RegistryConfiguration;

import reactor.core.publisher.Flux;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.test.StepVerifier;
import tech.kage.event.Event;
import tech.kage.event.crypto.EventEncryptor;

/**
 * Integration tests verifying encryption functionality of
 * {@link ReactorKafkaEventStore}.
 * 
 * @author Dariusz Szpakowski
 */
class EncryptedReactorKafkaEventStoreIT extends ReactorKafkaEventStoreIT {
    @Autowired
    EventEncryptor eventEncryptor;

    static final Map<URI, KeysetHandle> testKms = new HashMap<>();

    static class TestConfiguration extends ReactorKafkaEventStoreIT.TestConfiguration {
        @Bean
        @Scope(SCOPE_PROTOTYPE)
        Aead aead(URI encryptionKey) throws GeneralSecurityException {
            return testKms.get(encryptionKey).getPrimitive(RegistryConfiguration.get(), Aead.class);
        }
    }

    @Test
    void savesEncryptedEventsInKafka() throws GeneralSecurityException {
        // Given
        var events = testEvents(10);

        var keyMap = new HashMap<UUID, URI>();

        for (var event : events) {
            var encryptionKey = URI.create("test-kms://test-keys/" + event.key().toString());

            keyMap.put(event.key(), encryptionKey);

            testKms.putIfAbsent(encryptionKey, KeysetHandle.generateNew(AES256_GCM));
        }

        var expectedEventsIterator = events.iterator();

        // When
        var savedEvents = Flux.concat(
                events.stream().map(event -> eventStore.save(topic, event, keyMap.get(event.key()))).toList());

        // Then
        var retrievedEvents = KafkaReceiver
                .create(kafkaReceiverOptions.assignment(List.of(new TopicPartition(topic, 0))))
                .receive()
                .take(events.size())
                .timeout(Duration.ofSeconds(60));

        StepVerifier
                .create(savedEvents.thenMany(retrievedEvents))
                .thenConsumeWhile(message -> {
                    var key = message.key();
                    var encryptedPayload = message.value();
                    var timestamp = Instant.ofEpochMilli(message.timestamp());
                    var metadata = toSequencedMap(message.headers());

                    byte[] decryptedPayload;

                    try {
                        decryptedPayload = eventEncryptor.decrypt(encryptedPayload, key, timestamp, metadata);
                    } catch (GeneralSecurityException e) {
                        throw new AssertionError("Unable to decrypt event payload", e);
                    }

                    var deserializedPayload = kafkaAvroDeserializer.deserialize(null, decryptedPayload);

                    var expectedEvent = expectedEventsIterator.next();

                    // the expected metadata are sorted by key
                    var expectedMetadata = new TreeMap<>(expectedEvent.metadata());

                    expectedMetadata.put(ENCRYPTION_KEY_ID, keyMap.get(key).toString().getBytes());

                    return key.equals(expectedEvent.key())
                            && deserializedPayload.equals(expectedEvent.payload())
                            && timestamp.equals(expectedEvent.timestamp())
                            && isEqualOrdered(metadata, expectedMetadata);
                })
                .as("retrieves stored encrypted events with the same data")
                .verifyComplete();
    }

    @Test
    void enforcesEventPayloadIntegrity() throws GeneralSecurityException {
        // Given
        var events = testEvents(10);

        var keyMap = new HashMap<UUID, URI>();

        for (var event : events) {
            var encryptionKey = URI.create("test-kms://test-keys/" + event.key().toString());

            keyMap.put(event.key(), encryptionKey);

            testKms.putIfAbsent(encryptionKey, KeysetHandle.generateNew(AES256_GCM));
        }

        var expectedEventsIterator = events.iterator();

        // When
        var savedEvents = Flux.concat(
                events.stream().map(event -> eventStore.save(topic, event, keyMap.get(event.key()))).toList());

        // Then
        var verifiedRetrievedEventPayload = KafkaReceiver
                .create(kafkaReceiverOptions.assignment(List.of(new TopicPartition(topic, 0))))
                .receive()
                .take(events.size())
                .timeout(Duration.ofSeconds(60))
                .map(message -> {
                    var key = message.key();
                    var encryptedPayload = message.value();
                    var timestamp = Instant.ofEpochMilli(message.timestamp());
                    var metadata = toSequencedMap(message.headers());

                    try {
                        return eventEncryptor.decrypt(encryptedPayload, key, timestamp, metadata);
                    } catch (GeneralSecurityException e) {
                        throw new AssertionError("Unable to decrypt event payload", e);
                    }
                });

        StepVerifier
                .create(savedEvents.thenMany(verifiedRetrievedEventPayload))
                .thenConsumeWhile(decryptedPayload -> {
                    var deserializedPayload = kafkaAvroDeserializer.deserialize(null, decryptedPayload);

                    var expectedEventPayload = expectedEventsIterator.next().payload();

                    return deserializedPayload.equals(expectedEventPayload);
                })
                .as("verifies payload integrity")
                .verifyComplete();
    }

    @Test
    void throwsExceptionWhenEventPayloadIntegrityIsViolated() throws GeneralSecurityException {
        // Given
        var events = testEvents(10);

        var keyMap = new HashMap<UUID, URI>();

        for (var event : events) {
            var encryptionKey = URI.create("test-kms://test-keys/" + event.key().toString());

            keyMap.put(event.key(), encryptionKey);

            testKms.putIfAbsent(encryptionKey, KeysetHandle.generateNew(AES256_GCM));
        }

        // When
        var savedEvents = Flux.concat(
                events.stream().map(event -> eventStore.save(topic, event, keyMap.get(event.key()))).toList());

        // Then
        var verifiedRetrievedEventPayload = KafkaReceiver
                .create(kafkaReceiverOptions.assignment(List.of(new TopicPartition(topic, 0))))
                .receive()
                .take(events.size())
                .timeout(Duration.ofSeconds(60))
                .map(message -> {
                    var invalidKey = UUID.randomUUID(); // use invalid key
                    var encryptedPayload = message.value();
                    var timestamp = Instant.ofEpochMilli(message.timestamp());
                    var metadata = toSequencedMap(message.headers());

                    try {
                        return eventEncryptor.decrypt(encryptedPayload, invalidKey, timestamp, metadata);
                    } catch (GeneralSecurityException e) {
                        throw new AssertionError("Unable to decrypt event payload", e);
                    }
                });

        StepVerifier
                .create(savedEvents.thenMany(verifiedRetrievedEventPayload))
                .expectErrorMatches(
                        thrown -> thrown.getCause() instanceof GeneralSecurityException generalSecurityException
                                && generalSecurityException.getMessage().equals("decryption failed"))
                .verify();
    }

    @Test
    void readsEncryptedEventsFromKafka() throws GeneralSecurityException {
        // Given
        var events = testEvents(10);

        var keyMap = new HashMap<UUID, URI>();

        for (var event : events) {
            var encryptionKey = URI.create("test-kms://test-keys/" + event.key().toString());

            keyMap.put(event.key(), encryptionKey);

            testKms.putIfAbsent(encryptionKey, KeysetHandle.generateNew(AES256_GCM));
        }

        var savedEvents = Flux.concat(
                events.stream().map(event -> eventStore.save(topic, event, keyMap.get(event.key()))).toList());

        var expectedEvents = IntStream
                .range(0, events.size())
                .boxed()
                .map(i -> {
                    var event = events.get(i);

                    var metadata = new HashMap<String, Object>();

                    metadata.put("partition", 0);
                    metadata.put("offset", Long.valueOf(i));

                    for (var metadataEntry : event.metadata().entrySet()) {
                        metadata.put("header." + metadataEntry.getKey(), metadataEntry.getValue());
                    }

                    metadata.put("header." + ENCRYPTION_KEY_ID, keyMap.get(event.key()).toString().getBytes());

                    return Event.from(event.key(), event.payload(), event.timestamp(), metadata);
                })
                .toList();

        var expectedEventsIterator = expectedEvents.iterator();

        // When
        var retrievedEvents = eventStore
                .subscribe(topic)
                .concatMap(event -> event.as(transactionalOperator::transactional))
                .take(events.size())
                .timeout(Duration.ofSeconds(60));

        // Then
        StepVerifier
                .create(savedEvents.thenMany(retrievedEvents))
                .thenConsumeWhile(nextEvent -> {
                    var expectedEvent = expectedEventsIterator.next();

                    return nextEvent.key().equals(expectedEvent.key())
                            && nextEvent.payload().equals(expectedEvent.payload())
                            && nextEvent.timestamp().equals(expectedEvent.timestamp())
                            && isEqual(nextEvent.metadata(), expectedEvent.metadata());
                })
                .as("retrieves stored encrypted events with the same data")
                .verifyComplete();
    }

    private SequencedMap<String, Object> toSequencedMap(Iterable<Header> headers) {
        var associatedMetadata = new LinkedHashMap<String, Object>();

        for (var header : headers) {
            associatedMetadata.put(header.key(), header.value());
        }

        return associatedMetadata;
    }
}
