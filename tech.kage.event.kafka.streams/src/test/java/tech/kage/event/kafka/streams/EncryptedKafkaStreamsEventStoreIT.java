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

import static org.springframework.beans.factory.config.BeanDefinition.SCOPE_PROTOTYPE;
import static tech.kage.event.EventStore.ENCRYPTION_KEY_ID;

import java.net.URI;
import java.security.GeneralSecurityException;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;

import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.kstream.Produced;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Scope;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.stereotype.Component;

import com.google.crypto.tink.Aead;
import com.google.crypto.tink.KeysetHandle;
import com.google.crypto.tink.RegistryConfiguration;
import com.google.crypto.tink.aead.PredefinedAeadParameters;

import reactor.core.publisher.Flux;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.test.StepVerifier;
import tech.kage.event.Event;
import tech.kage.event.crypto.EventEncryptor;

/**
 * Integration tests verifying encryption functionality of
 * {@link KafkaStreamsEventStore}.
 * 
 * @author Dariusz Szpakowski
 */
class EncryptedKafkaStreamsEventStoreIT extends UUIDKeyKafkaStreamsEventStoreIT {
    @Autowired
    EventEncryptor eventEncryptor;

    static final String ENCRYPTED_TEST_EVENTS = "encrypted_test_events";
    static final String ENCRYPTED_TEST_EVENTS_IN = "encrypted_test_events_in";
    static final String SECOND_ENCRYPTED_TEST_EVENTS_IN = "second_encrypted_test_events_in";
    static final String ENCRYPTED_TEST_EVENTS_OUT = "encrypted_test_events_out";
    static final String UNENCRYPTED_TEST_EVENTS_OUT = "unencrypted_test_events_out";

    static final Map<URI, KeysetHandle> testKms = new HashMap<>();

    @Component
    static class TestEncryptedStreamsSubscriber {
        @Autowired
        void init(KafkaAdmin kafkaAdmin, KafkaStreamsEventStore<UUID, SpecificRecord> eventStore) {
            kafkaAdmin.createOrModifyTopics(TopicBuilder.name(ENCRYPTED_TEST_EVENTS).build());
            kafkaAdmin.createOrModifyTopics(TopicBuilder.name(ENCRYPTED_TEST_EVENTS_IN).build());
            kafkaAdmin.createOrModifyTopics(TopicBuilder.name(SECOND_ENCRYPTED_TEST_EVENTS_IN).build());
            kafkaAdmin.createOrModifyTopics(TopicBuilder.name(UNENCRYPTED_TEST_EVENTS_OUT).build());
            kafkaAdmin.createOrModifyTopics(TopicBuilder.name(ENCRYPTED_TEST_EVENTS_OUT).build());

            eventStore
                    .subscribe(ENCRYPTED_TEST_EVENTS_IN)
                    .mapValues(Event::payload)
                    .to(UNENCRYPTED_TEST_EVENTS_OUT);

            eventStore
                    .subscribe(SECOND_ENCRYPTED_TEST_EVENTS_IN)
                    .mapValues(EncryptedKafkaStreamsEventStoreIT::fakeProcessing)
                    .processValues(() -> eventStore.new EncryptingOutputEventTransformer(ENCRYPTED_TEST_EVENTS_OUT))
                    .to(ENCRYPTED_TEST_EVENTS_OUT, Produced.valueSerde(Serdes.ByteArray()));
        }
    }

    @Import({ KafkaStreamsEventStore.class, TestStreamsSubscriber.class, TestEncryptedStreamsSubscriber.class })
    static class TestConfig extends KafkaStreamsEventStoreIT.TestConfig {
        @Bean
        @Scope(SCOPE_PROTOTYPE)
        Aead aead(URI encryptionKey) throws GeneralSecurityException {
            return testKms.get(encryptionKey).getPrimitive(RegistryConfiguration.get(), Aead.class);
        }
    }

    @Test
    @Order(4)
    void savesEncryptedEventsInKafka() throws GeneralSecurityException {
        // Given
        var events = testEvents(10);

        var keyMap = new HashMap<UUID, URI>();

        for (var event : events) {
            var encryptionKey = URI.create("test-kms://test-keys/" + event.key().toString());

            keyMap.put(event.key(), encryptionKey);

            testKms.putIfAbsent(encryptionKey, KeysetHandle.generateNew(PredefinedAeadParameters.AES256_GCM));
        }

        var expectedEventsIterator = events.iterator();

        // When
        var savedEvents = Flux.concat(
                events
                        .stream()
                        .map(event -> eventStore.save(ENCRYPTED_TEST_EVENTS, event, keyMap.get(event.key())))
                        .toList());

        // Then
        var retrievedEvents = KafkaReceiver
                .create(kafkaReceiverOptions.assignment(List.of(new TopicPartition(ENCRYPTED_TEST_EVENTS, 0))))
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
    @Order(5)
    void enforcesEventPayloadIntegrity() throws GeneralSecurityException {
        // Given
        var events = testEvents(10);

        var keyMap = new HashMap<UUID, URI>();

        for (var event : events) {
            var encryptionKey = URI.create("test-kms://test-keys/" + event.key().toString());

            keyMap.put(event.key(), encryptionKey);

            testKms.putIfAbsent(encryptionKey, KeysetHandle.generateNew(PredefinedAeadParameters.AES256_GCM));
        }

        var expectedEventsIterator = events.iterator();

        // When
        var savedEvents = Flux.concat(
                events
                        .stream()
                        .map(event -> eventStore.save(ENCRYPTED_TEST_EVENTS, event, keyMap.get(event.key())))
                        .toList());

        // Then
        var verifiedRetrievedEventPayload = KafkaReceiver
                .create(kafkaReceiverOptions.assignment(List.of(new TopicPartition(ENCRYPTED_TEST_EVENTS, 0))))
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
    @Order(6)
    void throwsExceptionWhenEventPayloadIntegrityIsViolated() throws GeneralSecurityException {
        // Given
        var events = testEvents(10);

        var keyMap = new HashMap<UUID, URI>();

        for (var event : events) {
            var encryptionKey = URI.create("test-kms://test-keys/" + event.key().toString());

            keyMap.put(event.key(), encryptionKey);

            testKms.putIfAbsent(encryptionKey, KeysetHandle.generateNew(PredefinedAeadParameters.AES256_GCM));
        }

        // When
        var savedEvents = Flux.concat(
                events
                        .stream()
                        .map(event -> eventStore.save(ENCRYPTED_TEST_EVENTS, event, keyMap.get(event.key())))
                        .toList());

        // Then
        var verifiedRetrievedEventPayload = KafkaReceiver
                .create(kafkaReceiverOptions.assignment(List.of(new TopicPartition(ENCRYPTED_TEST_EVENTS, 0))))
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
    @Order(7)
    void subscribesToEncryptedEventStream() throws GeneralSecurityException {
        // Given
        var events = testEvents(10);

        var keyMap = new HashMap<UUID, URI>();

        for (var event : events) {
            var encryptionKey = URI.create("test-kms://test-keys/" + event.key().toString());

            keyMap.put(event.key(), encryptionKey);

            testKms.putIfAbsent(encryptionKey, KeysetHandle.generateNew(PredefinedAeadParameters.AES256_GCM));
        }

        var savedEvents = Flux.concat(
                events
                        .stream()
                        .map(event -> eventStore.save(ENCRYPTED_TEST_EVENTS_IN, event, keyMap.get(event.key())))
                        .toList());

        var expectedEventsIterator = events.iterator();

        // When
        // topology specified in the bean init method

        // Then
        var retrievedEvents = KafkaReceiver
                .create(kafkaReceiverOptions.assignment(List.of(new TopicPartition(UNENCRYPTED_TEST_EVENTS_OUT, 0))))
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

                    expectedMetadata.put(ENCRYPTION_KEY_ID, keyMap.get(key).toString().getBytes());

                    return key.equals(expectedEvent.key())
                            && payload.equals(expectedEvent.payload())
                            && timestamp.equals(expectedEvent.timestamp())
                            && isEqualOrdered(metadata, expectedMetadata);
                })
                .as("retrieves stored events with the same data")
                .verifyComplete();
    }

    @Test
    @Order(8)
    void allowsEncryptingOutputEventStream() throws GeneralSecurityException {
        // Given
        var events = testEvents(10);

        var keyMap = new HashMap<UUID, URI>();

        for (var event : events) {
            var encryptionKey = URI.create("test-kms://test-keys/" + event.key().toString());

            keyMap.put(event.key(), encryptionKey);

            testKms.putIfAbsent(encryptionKey, KeysetHandle.generateNew(PredefinedAeadParameters.AES256_GCM));
        }

        var savedEvents = Flux.concat(
                events
                        .stream()
                        .map(event -> eventStore.save(SECOND_ENCRYPTED_TEST_EVENTS_IN, event, keyMap.get(event.key())))
                        .toList());

        var expectedEventsIterator = events.iterator();

        // When
        // topology specified in the bean init method

        // Then
        var retrievedEvents = KafkaReceiver
                .create(kafkaReceiverOptions.assignment(List.of(new TopicPartition(ENCRYPTED_TEST_EVENTS_OUT, 0))))
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
                    var encryptionKey = keyMap.get(key).toString().getBytes();

                    byte[] decryptedPayload;

                    try {
                        decryptedPayload = eventEncryptor.decrypt(encryptedPayload, key, timestamp, metadata);
                    } catch (GeneralSecurityException e) {
                        throw new AssertionError("Unable to decrypt event payload", e);
                    }

                    var deserializedPayload = kafkaAvroDeserializer.deserialize(null, decryptedPayload);

                    var expectedEvent = expectedEventAfterFakeProcessing(expectedEventsIterator.next(), encryptionKey);

                    // the expected metadata are sorted by key
                    var expectedMetadata = new TreeMap<>(expectedEvent.metadata());

                    return key.equals(expectedEvent.key())
                            && deserializedPayload.equals(expectedEvent.payload())
                            && timestamp.equals(expectedEvent.timestamp())
                            && isEqualOrdered(metadata, expectedMetadata);
                })
                .as("retrieves stored encrypted events with the same data")
                .verifyComplete();
    }

    private static Event<UUID, SpecificRecord> fakeProcessing(Event<UUID, SpecificRecord> event) {
        return Event.from(
                event.key(),
                TestPayload.newBuilder().setText(((TestPayload) event.payload()).getText() + " (processed)").build(),
                event.timestamp().plusSeconds(3),
                Map.of(
                        ENCRYPTION_KEY_ID, event.metadata().get("header.kid"),
                        "dTest", event.metadata().get("header.dTest")));
    }

    private Event<UUID, SpecificRecord> expectedEventAfterFakeProcessing(Event<UUID, SpecificRecord> event,
            byte[] encryptionKey) {
        return Event.from(
                event.key(),
                TestPayload.newBuilder().setText(((TestPayload) event.payload()).getText() + " (processed)").build(),
                event.timestamp().plusSeconds(3),
                Map.of(
                        ENCRYPTION_KEY_ID, encryptionKey,
                        "dTest", event.metadata().get("dTest")));
    }
}
