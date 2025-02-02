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

package tech.kage.event;

import java.net.URI;

import org.apache.avro.specific.SpecificRecord;

import reactor.core.publisher.Mono;

/**
 * A database focused on storing events.
 * 
 * @author Dariusz Szpakowski
 */
public interface EventStore {
    /**
     * Constant representing the event identifier in the source database.
     */
    static final String SOURCE_ID = "id";

    /**
     * Constant representing the encryption key identifier.
     */
    static final String ENCRYPTION_KEY_ID = "kid";

    /**
     * Saves the specified event in the event store.
     *
     * @param <T>   the event's payload type
     * @param topic topic which is used for grouping events
     * @param event event to be saved
     *
     * @return saved event
     * 
     * @throws NullPointerException     if the specified topic or event is null
     * @throws ClassCastException       if the specified event contains metadata of
     *                                  type different from {@code byte[]}
     * @throws IllegalArgumentException if the specified event contains metadata
     *                                  with key {@code id} or {@code kid}
     */
    <T extends SpecificRecord> Mono<Event<T>> save(String topic, Event<T> event);

    /**
     * Saves the specified event in the event store in its authenticated and
     * encrypted form. The encryption scheme used is Authenticated Encryption with
     * Associated Data (AEAD). The {@code Event}'s payload is encrypted and the key,
     * timestamp and metadata are the associated non-encrypted authenticated data.
     *
     * @param <T>           the event's payload type
     * @param topic         topic which is used for grouping events
     * @param event         event to be saved
     * @param encryptionKey encryption key to use
     *
     * @return saved event in its unencrypted form
     * 
     * @throws NullPointerException     if the specified topic, event or
     *                                  encryptionKey is null
     * @throws ClassCastException       if the specified event contains metadata of
     *                                  type different from {@code byte[]}
     * @throws IllegalArgumentException if the specified event contains metadata
     *                                  with key {@code id} or {@code kid}
     */
    <T extends SpecificRecord> Mono<Event<T>> save(String topic, Event<T> event, URI encryptionKey);
}
