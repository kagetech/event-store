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

package tech.kage.event.crypto;

import static com.google.crypto.tink.aead.PredefinedAeadParameters.AES256_GCM;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Named.named;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.springframework.beans.factory.config.BeanDefinition.SCOPE_PROTOTYPE;
import static tech.kage.event.EventStore.ENCRYPTION_KEY_ID;
import static tech.kage.event.EventStore.SOURCE_ID;

import java.net.URI;
import java.security.GeneralSecurityException;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Stream;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Scope;
import org.springframework.test.context.ActiveProfiles;

import com.google.crypto.tink.Aead;
import com.google.crypto.tink.KeysetHandle;
import com.google.crypto.tink.RegistryConfiguration;

/**
 * Integration tests for {@link EventEncryptor}.
 * 
 * @author Dariusz Szpakowski
 */
@SpringBootTest
@ActiveProfiles("test")
class EventEncryptorIT {
    // UUT
    @Autowired
    EventEncryptor eventEncryptor;

    @Autowired
    ObjectProvider<Aead> aeadProvider;

    static final Map<URI, KeysetHandle> testKms = new HashMap<>();

    @Configuration
    @Import(EventEncryptor.class)
    static class TestConfiguration {
        @Bean
        @Scope(SCOPE_PROTOTYPE)
        Aead aead(URI encryptionKey) throws GeneralSecurityException {
            return testKms.get(encryptionKey).getPrimitive(RegistryConfiguration.get(), Aead.class);
        }
    }

    @ParameterizedTest
    @MethodSource("testEvents")
    void encryptsPayloadAuthenticatedWithMetadata(byte[] payload, Object key, Instant timestamp,
            Map<String, Object> metadata) throws GeneralSecurityException {
        // Given
        var encryptionKey = URI.create("test-kms://test-keys/" + timestamp.toEpochMilli());

        testKms.putIfAbsent(encryptionKey, KeysetHandle.generateNew(AES256_GCM));

        var aead = aeadProvider.getObject(encryptionKey);

        var serializedMetadata = metadata.isEmpty() ? new byte[0] : MetadataSerializer.serialize(metadata);

        var associatedData = eventEncryptor.prepareAssociatedData(key, timestamp, serializedMetadata);

        // When
        var encryptedPayload = eventEncryptor
                .encrypt(payload, key, timestamp, metadata, encryptionKey)
                .block();

        // Then
        var decryptedPayload = aead.decrypt(encryptedPayload, associatedData);

        assertThat(decryptedPayload)
                .describedAs("decrypted payload")
                .isEqualTo(payload);
    }

    @ParameterizedTest
    @MethodSource("testEvents")
    void decryptsEncryptedPayloadAuthenticatedWithMetadata(byte[] payload, Object key, Instant timestamp,
            Map<String, Object> metadata) throws GeneralSecurityException {
        // Given
        var encryptionKey = URI.create("test-kms://test-keys/" + timestamp.toEpochMilli());

        testKms.putIfAbsent(encryptionKey, KeysetHandle.generateNew(AES256_GCM));

        var metadataWithEncryptionKey = new HashMap<>(metadata);

        metadataWithEncryptionKey.put(ENCRYPTION_KEY_ID, encryptionKey.toString().getBytes());

        var encryptedPayload = eventEncryptor
                .encrypt(payload, key, timestamp, metadata, encryptionKey)
                .block();

        // When
        var decryptedPayload = eventEncryptor.decrypt(encryptedPayload, key, timestamp, metadataWithEncryptionKey);

        // Then
        assertThat(decryptedPayload)
                .describedAs("decrypted payload")
                .isEqualTo(payload);
    }

    @ParameterizedTest
    @MethodSource("testEvents")
    void throwsExceptionWhenInvalidEncryptionKey(byte[] payload, Object key, Instant timestamp,
            Map<String, Object> metadata) throws GeneralSecurityException {
        // Given
        var encryptionKey = URI.create("test-kms://test-keys/" + timestamp.toEpochMilli());
        var invalidEncryptionKey = URI.create("test-kms://test-keys/invalid");

        testKms.putIfAbsent(encryptionKey, KeysetHandle.generateNew(AES256_GCM));
        testKms.putIfAbsent(invalidEncryptionKey, KeysetHandle.generateNew(AES256_GCM));

        var metadataWithInvalidKey = new HashMap<>(metadata);

        metadataWithInvalidKey.put(ENCRYPTION_KEY_ID, invalidEncryptionKey.toString().getBytes());

        var encryptedPayload = eventEncryptor
                .encrypt(payload, key, timestamp, metadata, encryptionKey)
                .block();

        // When
        var thrown = assertThrows(Throwable.class,
                () -> eventEncryptor.decrypt(encryptedPayload, key, timestamp, metadataWithInvalidKey));

        // Then
        assertThat(thrown)
                .describedAs("thrown exception")
                .isInstanceOf(GeneralSecurityException.class)
                .hasMessageContaining("decryption failed");
    }

    @ParameterizedTest
    @MethodSource("testEvents")
    void throwsExceptionWhenEventPayloadIntegrityIsViolated(byte[] payload, Object key, Instant timestamp,
            Map<String, Object> metadata) throws GeneralSecurityException {
        // Given
        var invalidKey = UUID.randomUUID(); // use invalid key

        var encryptionKey = URI.create("test-kms://test-keys/" + timestamp.toEpochMilli());

        testKms.putIfAbsent(encryptionKey, KeysetHandle.generateNew(AES256_GCM));

        var metadataWithInvalidKey = new HashMap<>(metadata);

        metadataWithInvalidKey.put(ENCRYPTION_KEY_ID, encryptionKey.toString().getBytes());

        var encryptedPayload = eventEncryptor
                .encrypt(payload, key, timestamp, metadata, encryptionKey)
                .block();

        // When
        var thrown = assertThrows(Throwable.class,
                () -> eventEncryptor.decrypt(encryptedPayload, invalidKey, timestamp, metadataWithInvalidKey));

        // Then
        assertThat(thrown)
                .describedAs("thrown exception")
                .isInstanceOf(GeneralSecurityException.class)
                .hasMessageContaining("decryption failed");
    }

    @ParameterizedTest
    @MethodSource("testEvents")
    void ignoresSourceIdDuringDecryption(byte[] payload, Object key, Instant timestamp, Map<String, Object> metadata)
            throws GeneralSecurityException {
        // Given
        var encryptionKey = URI.create("test-kms://test-keys/" + timestamp.toEpochMilli());

        testKms.putIfAbsent(encryptionKey, KeysetHandle.generateNew(AES256_GCM));

        var metadataWithSourceId = new HashMap<>(metadata);

        metadataWithSourceId.put(ENCRYPTION_KEY_ID, encryptionKey.toString().getBytes());
        metadataWithSourceId.put(SOURCE_ID, "123".getBytes());

        var encryptedPayload = eventEncryptor
                .encrypt(payload, key, timestamp, metadata, encryptionKey)
                .block();

        // When
        var decryptedPayload = eventEncryptor.decrypt(encryptedPayload, key, timestamp, metadataWithSourceId);

        // Then
        assertThat(decryptedPayload)
                .describedAs("decrypted payload")
                .isEqualTo(payload);
    }

    @ParameterizedTest
    @MethodSource("testEvents")
    void returnsPlaintextPayloadWhenEncryptionKeyNotSet(byte[] payload, Object key, Instant timestamp,
            Map<String, Object> metadata) throws GeneralSecurityException {
        // When
        var decryptedPayload = eventEncryptor.decrypt(payload, key, timestamp, metadata);

        // Then
        assertThat(decryptedPayload)
                .describedAs("decrypted payload")
                .isEqualTo(payload);
    }

    static Stream<Arguments> testEvents() {
        return Stream.of(
                arguments(
                        named("test payload 1", "test payload 1".getBytes()),
                        "test-event-1",
                        Instant.ofEpochMilli(1734149827923l),
                        Map.of()),
                arguments(
                        named("test payload 2", "test payload 2".getBytes()),
                        UUID.fromString("23debd32-09cd-4a20-a403-c18793ecd2d2"),
                        Instant.ofEpochMilli(1734174935363l),
                        Map.of(
                                "dTest", "meta_value".getBytes(),
                                "zTest", UUID.fromString("788ee0da-3ca9-4fa1-9d84-3470a067d695").toString().getBytes(),
                                "bTest", "1".getBytes())),
                arguments(
                        named("test payload 3", "test payload 3".getBytes()),
                        "test-event-3".getBytes(),
                        Instant.ofEpochMilli(1734175935363l),
                        Map.of(
                                "dTest", "meta_value".getBytes(),
                                "zTest", UUID.fromString("788ee0da-3ca9-4fa1-9d84-3470a067d695").toString().getBytes(),
                                "bTest", "1".getBytes())));
    }
}
