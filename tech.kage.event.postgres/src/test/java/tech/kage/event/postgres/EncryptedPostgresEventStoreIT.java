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

package tech.kage.event.postgres;

import static com.google.crypto.tink.aead.PredefinedAeadParameters.AES256_GCM;
import static org.springframework.beans.factory.config.BeanDefinition.SCOPE_PROTOTYPE;
import static tech.kage.event.EventStore.ENCRYPTION_KEY_ID;

import java.net.URI;
import java.nio.ByteBuffer;
import java.security.GeneralSecurityException;
import java.time.OffsetDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Scope;

import com.google.crypto.tink.Aead;
import com.google.crypto.tink.KeysetHandle;
import com.google.crypto.tink.RegistryConfiguration;

import reactor.test.StepVerifier;
import tech.kage.event.Event;
import tech.kage.event.crypto.EventEncryptor;
import tech.kage.event.crypto.MetadataSerializer;

/**
 * Integration tests verifying encryption functionality of
 * {@link PostgresEventStore}.
 * 
 * @author Dariusz Szpakowski
 */
class EncryptedPostgresEventStoreIT extends UUIDKeyPostgresEventStoreIT {
    @Autowired
    EventEncryptor eventEncryptor;

    static final Map<URI, KeysetHandle> testKms = new HashMap<>();

    static class TestConfiguration extends UUIDKeyPostgresEventStoreIT.TestConfiguration {
        @Bean
        @Scope(SCOPE_PROTOTYPE)
        Aead aead(URI encryptionKey) throws GeneralSecurityException {
            return testKms.get(encryptionKey).getPrimitive(RegistryConfiguration.get(), Aead.class);
        }
    }

    @ParameterizedTest
    @MethodSource("testEvents")
    void savesEncryptedEventInDatabase(Event<UUID, TestPayload> event) throws GeneralSecurityException {
        // Given
        var topic = "test_events";
        var encryptionKey = URI.create("test-kms://test-keys/" + event.key().toString());

        testKms.putIfAbsent(encryptionKey, KeysetHandle.generateNew(AES256_GCM));

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

        var expectedKey = event.key();
        var expectedPayload = event.payload();
        var expectedTimestamp = event.timestamp();
        var expectedMetadata = new TreeMap<>(event.metadata()); // the expected metadata are sorted by key

        // include ENCRYPTION_KEY_ID in the expected metadata
        expectedMetadata.put(ENCRYPTION_KEY_ID, encryptionKey.toString().getBytes());

        // When
        eventStore.save(topic, event, encryptionKey).block();

        // Then
        var retrievedEvent = databaseClient
                .sql("SELECT * FROM events.test_events")
                .fetch()
                .one();

        StepVerifier
                .create(retrievedEvent)
                .expectNextMatches(row -> {
                    var key = (UUID) row.get("key");
                    var encryptedPayload = ((ByteBuffer) row.get("data")).array();
                    var metadata = ((ByteBuffer) row.get("metadata")).array();
                    var timestamp = ((OffsetDateTime) row.get("timestamp")).toInstant();

                    var deserializedMetadata = MetadataSerializer.deserialize(metadata);

                    byte[] decryptedPayload;

                    try {
                        decryptedPayload = eventEncryptor.decrypt(
                                encryptedPayload, key, timestamp, deserializedMetadata);
                    } catch (GeneralSecurityException e) {
                        throw new IllegalStateException("Unable to decrypt", e);
                    }

                    var deserializedPayload = kafkaAvroDeserializer.deserialize(null, decryptedPayload);

                    return key.equals(expectedKey)
                            && deserializedPayload.equals(expectedPayload)
                            && timestamp.equals(expectedTimestamp)
                            && isEqualOrdered(deserializedMetadata, expectedMetadata);
                })
                .as("finds stored encrypted event with the same data")
                .verifyComplete();
    }

    @ParameterizedTest
    @MethodSource("testEvents")
    void enforcesEventPayloadIntegrity(Event<UUID, TestPayload> event) throws GeneralSecurityException {
        // Given
        var topic = "test_events";
        var encryptionKey = URI.create("test-kms://test-keys/" + event.key().toString());

        testKms.putIfAbsent(encryptionKey, KeysetHandle.generateNew(AES256_GCM));

        // When
        eventStore.save(topic, event, encryptionKey).block();

        // Then
        var verifiedRetrievedEventPayload = databaseClient
                .sql("SELECT * FROM events.test_events")
                .fetch()
                .one()
                .<byte[]>handle((nextEvent, sink) -> {
                    var key = (UUID) nextEvent.get("key");
                    var encryptedPayload = ((ByteBuffer) nextEvent.get("data")).array();
                    var metadata = ((ByteBuffer) nextEvent.get("metadata")).array();
                    var timestamp = ((OffsetDateTime) nextEvent.get("timestamp")).toInstant();

                    var deserializedMetadata = MetadataSerializer.deserialize(metadata);

                    try {
                        sink.next(eventEncryptor.decrypt(encryptedPayload, key, timestamp, deserializedMetadata));
                    } catch (GeneralSecurityException e) {
                        sink.error(e);
                    }
                });

        StepVerifier
                .create(verifiedRetrievedEventPayload)
                .expectNextMatches(
                        payloadPlaintext -> {
                            var deserializedPayload = kafkaAvroDeserializer.deserialize(null, payloadPlaintext);

                            return deserializedPayload.equals(event.payload());
                        })
                .as("verifies payload integrity")
                .verifyComplete();
    }

    @ParameterizedTest
    @MethodSource("testEvents")
    void throwsExceptionWhenEventPayloadIntegrityIsViolated(Event<UUID, TestPayload> event)
            throws GeneralSecurityException {
        // Given
        var topic = "test_events";
        var encryptionKey = URI.create("test-kms://test-keys/" + event.key().toString());

        testKms.putIfAbsent(encryptionKey, KeysetHandle.generateNew(AES256_GCM));

        // When
        eventStore.save(topic, event, encryptionKey).block();

        // Then
        var verifiedRetrievedEventPayload = databaseClient
                .sql("SELECT * FROM events.test_events")
                .fetch()
                .one()
                .handle((nextEvent, sink) -> {
                    var invalidKey = UUID.randomUUID(); // use invalid key
                    var encryptedPayload = ((ByteBuffer) nextEvent.get("data")).array();
                    var metadata = ((ByteBuffer) nextEvent.get("metadata")).array();
                    var timestamp = ((OffsetDateTime) nextEvent.get("timestamp")).toInstant();

                    var deserializedMetadata = MetadataSerializer.deserialize(metadata);

                    try {
                        sink.next(
                                eventEncryptor.decrypt(encryptedPayload, invalidKey, timestamp, deserializedMetadata));
                    } catch (GeneralSecurityException e) {
                        sink.error(e);
                    }
                });

        StepVerifier
                .create(verifiedRetrievedEventPayload)
                .expectErrorMatches(
                        thrown -> thrown instanceof GeneralSecurityException generalSecurityException
                                && generalSecurityException.getMessage().equals("decryption failed"))
                .verify();
    }
}
