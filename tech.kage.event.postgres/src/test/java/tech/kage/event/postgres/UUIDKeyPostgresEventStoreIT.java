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

package tech.kage.event.postgres;

import static org.junit.jupiter.api.Named.named;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Stream;

import org.junit.jupiter.params.provider.Arguments;

import tech.kage.event.Event;

/**
 * Integration tests for {@link PostgresEventStore} with default configuration
 * where events have keys of {@code UUID} type.
 * 
 * @author Dariusz Szpakowski
 */
class UUIDKeyPostgresEventStoreIT extends PostgresEventStoreIT<UUID> {
    @Override
    protected String getKeyType() {
        return "uuid";
    }

    static class TestConfiguration extends PostgresEventStoreIT.TestConfiguration {
    }

    static Stream<Arguments> testEvents() {
        return Stream.of(
                arguments(
                        named(
                                "payload only",
                                Event.from(TestPayload.newBuilder().setText("test payload 1").build()))),
                arguments(
                        named(
                                "key and payload",
                                Event.from(
                                        UUID.fromString("ea09ab50-8564-485f-9363-d4a4b3d6e9ca"),
                                        TestPayload.newBuilder().setText("test payload 2").build()))),
                arguments(
                        named(
                                "key, payload and timestamp",
                                Event.from(
                                        UUID.fromString("58ce74c8-64d3-45d0-a35b-f99e8e551a51"),
                                        TestPayload.newBuilder().setText("test payload 3").build(),
                                        Instant.ofEpochMilli(1736025221442l)))),
                arguments(
                        named(
                                "key, payload, timestamp and metadata",
                                Event.from(
                                        UUID.fromString("ba7b9608-ccae-472c-99cf-b29e038adab1"),
                                        TestPayload.newBuilder().setText("test payload 4").build(),
                                        Instant.ofEpochMilli(1736026528567l),
                                        Map.of(
                                                "dTest", "meta_value".getBytes(),
                                                "zTest", UUID.randomUUID().toString().getBytes(),
                                                "bTest", Long.toString(123l).getBytes())))));
    }
}
