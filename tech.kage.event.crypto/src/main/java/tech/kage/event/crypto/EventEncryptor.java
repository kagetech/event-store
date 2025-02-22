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

import static tech.kage.event.EventStore.ENCRYPTION_KEY_ID;
import static tech.kage.event.EventStore.SOURCE_ID;

import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import org.springframework.beans.factory.ObjectProvider;
import org.springframework.stereotype.Component;

import com.google.crypto.tink.Aead;
import com.google.crypto.tink.aead.AeadConfig;

import reactor.core.publisher.Mono;
import tech.kage.event.Event;

/**
 * Encryptor of {@link Event}'s payloads authenticated with provided metadata.
 * 
 * @author Dariusz Szpakowski
 */
@Component
public class EventEncryptor {
    private final ObjectProvider<Aead> aeadProvider;

    /**
     * Constructs a new {@link EventEncryptor} instance.
     *
     * @param aeadProvider provider of instances of {@link Aead}
     * 
     * @throws GeneralSecurityException if encryption support initialization fails
     */
    EventEncryptor(ObjectProvider<Aead> aeadProvider) throws GeneralSecurityException {
        this.aeadProvider = aeadProvider;

        AeadConfig.register();
    }

    /**
     * Encrypts the specified {@link Event}'s payload authenticated with provided
     * metadata.
     * 
     * @param payload       plaintext {@code Event}'s payload to encrypt
     * @param key           key included in the associated data
     * @param timestamp     timestamp included in the associated data
     * @param metadata      metadata included in the associated data
     * @param encryptionKey encryption key to use
     * 
     * @return encrypted {@code Event}'s payload
     * 
     * @throws NullPointerException if the specified payload, key, timestamp,
     *                              metadata or encryptionKey is null
     */
    public Mono<byte[]> encrypt(byte[] payload, Object key, Instant timestamp, Map<String, Object> metadata,
            URI encryptionKey) {
        Objects.requireNonNull(payload, "payload must not be null");
        Objects.requireNonNull(key, "key must not be null");
        Objects.requireNonNull(timestamp, "timestamp must not be null");
        Objects.requireNonNull(metadata, "metadata must not be null");
        Objects.requireNonNull(encryptionKey, "encryptionKey must not be null");

        return Mono.fromCallable(
                () -> aeadProvider
                        .getObject(encryptionKey)
                        .encrypt(payload, prepareAssociatedData(key, timestamp, metadata)));
    }

    /**
     * Decrypts the specified {@link Event}'s payload authenticated with provided
     * metadata.
     * 
     * @param payload   ciphertext {@code Event}'s payload to decrypt
     * @param key       the {@code Event}'s key
     * @param timestamp the {@code Event}'s timestamp
     * @param metadata  the {@code Event}'s metadata with included encryption
     *                  key id
     * 
     * @return decrypted {@code Event}'s payload
     * 
     * @throws GeneralSecurityException if decryption fails
     * @throws NullPointerException     if the specified payload, key, timestamp or
     *                                  metadata is null
     */
    public byte[] decrypt(byte[] payload, Object key, Instant timestamp, Map<String, Object> metadata)
            throws GeneralSecurityException {
        Objects.requireNonNull(payload, "payload must not be null");
        Objects.requireNonNull(key, "key must not be null");
        Objects.requireNonNull(timestamp, "timestamp must not be null");
        Objects.requireNonNull(metadata, "metadata must not be null");

        if (!metadata.containsKey(ENCRYPTION_KEY_ID)) {
            return payload;
        }

        var encryptionKey = URI.create(new String((byte[]) metadata.get(ENCRYPTION_KEY_ID)));

        var associatedMetadata = new HashMap<>(metadata);

        // exclude the EventStore.SOURCE_ID and EventStore.ENCRYPTION_KEY_ID keys
        associatedMetadata.remove(SOURCE_ID);
        associatedMetadata.remove(ENCRYPTION_KEY_ID);

        return aeadProvider
                .getObject(encryptionKey)
                .decrypt(payload, prepareAssociatedData(key, timestamp, associatedMetadata));
    }

    private byte[] prepareAssociatedData(Object key, Instant timestamp, Map<String, Object> metadata) {
        var serializedMetadata = metadata.isEmpty() ? new byte[0] : MetadataSerializer.serialize(metadata);

        return prepareAssociatedData(key, timestamp, serializedMetadata);
    }

    byte[] prepareAssociatedData(Object key, Instant timestamp, byte[] metadata) {
        var keyBytes = key instanceof byte[] byteArray ? byteArray : key.toString().getBytes(StandardCharsets.UTF_8);

        return ByteBuffer
                .allocate(keyBytes.length + Long.BYTES + metadata.length) // 1 long for timestamp
                .put(keyBytes)
                .putLong(timestamp.toEpochMilli())
                .put(metadata)
                .array();
    }
}
