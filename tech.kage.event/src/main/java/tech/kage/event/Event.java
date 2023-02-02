/*
 * Copyright (c) 2023, Dariusz Szpakowski
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

import static java.time.temporal.ChronoUnit.MILLIS;

import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

import org.apache.avro.specific.SpecificRecord;

/**
 * A record that represents an immutable event.
 * 
 * @param <T>       the type of payload
 * @param key       the {@code Event}'s key
 * @param payload   the {@code Event}'s payload
 * @param timestamp the {@code Event}'s timestamp
 *
 * @author Dariusz Szpakowski
 */
public record Event<T extends SpecificRecord>(UUID key, T payload, Instant timestamp) {
    /**
     * Creates an {@link Event} with a given key, payload and timestamp.
     *
     * @param key       the {@code Event}'s key
     * @param payload   the {@code Event}'s payload
     * @param timestamp the {@code Event}'s timestamp
     *
     * @throws NullPointerException if the specified key, payload or timestamp is
     *                              null
     */
    public Event(UUID key, T payload, Instant timestamp) {
        Objects.requireNonNull(key, "key must not be null");
        Objects.requireNonNull(payload, "payload must not be null");
        Objects.requireNonNull(timestamp, "timestamp must not be null");

        this.key = key;
        this.payload = payload;
        this.timestamp = timestamp.truncatedTo(MILLIS);
    }

    /**
     * Creates an {@link Event} with a given payload.
     *
     * @param <T>     the {@code Event}'s payload type
     * @param payload the {@code Event}'s payload
     *
     * @return an {@link Event} with the specified payload, a random key and
     *         timestamp set to current time truncated to milliseconds
     * 
     * @throws NullPointerException if the specified payload is null
     */
    public static <T extends SpecificRecord> Event<T> from(T payload) {
        return from(UUID.randomUUID(), payload);
    }

    /**
     * Creates an {@link Event} with a given key and payload.
     *
     * @param <T>     the {@code Event}'s payload type
     * @param key     the {@code Event}'s key
     * @param payload the {@code Event}'s payload
     *
     * @return an {@link Event} with the specified key and payload and timestamp set
     *         to current time truncated to milliseconds
     * 
     * @throws NullPointerException if the specified key or payload is null
     */
    public static <T extends SpecificRecord> Event<T> from(UUID key, T payload) {
        return from(key, payload, Instant.now());
    }

    /**
     * Creates an {@link Event} with a given key, payload and timestamp.
     *
     * @param <T>       the {@code Event}'s payload type
     * @param key       the {@code Event}'s key
     * @param payload   the {@code Event}'s payload
     * @param timestamp the {@code Event}'s timestamp
     *
     * @return an {@link Event} with the specified key, payload and timestamp
     *         truncated to milliseconds
     * 
     * @throws NullPointerException if the specified key, payload or timestamp is
     *                              null
     */
    public static <T extends SpecificRecord> Event<T> from(UUID key, T payload, Instant timestamp) {
        return new Event<>(key, payload, timestamp);
    }
}
