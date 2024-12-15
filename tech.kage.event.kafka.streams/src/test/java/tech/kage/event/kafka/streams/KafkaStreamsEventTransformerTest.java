/*
 * Copyright (c) 2024, Dariusz Szpakowski
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

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Named.named;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.verify;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Stream;

import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorContext;
import org.apache.kafka.streams.processor.api.FixedKeyRecord;
import org.apache.kafka.streams.processor.api.RecordMetadata;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import tech.kage.event.Event;
import tech.kage.event.kafka.streams.KafkaStreamsEventStore.EventTransformer;

/**
 * Unit tests of {@link KafkaStreamsEventStore.EventTransformer}.
 * 
 * @author Dariusz Szpakowski
 */
class KafkaStreamsEventTransformerTest {
    EventTransformer eventTransformer = new EventTransformer();

    @ParameterizedTest
    @MethodSource("testMessages")
    void transformsMessagePayloadIntoEvent(
            FixedKeyRecord<UUID, SpecificRecord> message,
            FixedKeyProcessorContext<UUID, Event<SpecificRecord>> context,
            Event<?> expectedEvent) {
        // Given
        eventTransformer.init(context);

        // When
        eventTransformer.process(message);

        // Then
        var messageValueCaptor = ArgumentCaptor.forClass(Event.class);

        verify(message).withValue(messageValueCaptor.capture());

        Event<?> transformedEvent = messageValueCaptor.getValue();

        assertThat(transformedEvent.key())
                .describedAs("transformed event key")
                .isEqualTo(expectedEvent.key());

        assertThat(transformedEvent.payload())
                .describedAs("transformed event payload")
                .isEqualTo(expectedEvent.payload());

        assertThat(transformedEvent.timestamp())
                .describedAs("transformed event timestamp")
                .isEqualTo(expectedEvent.timestamp());

        assertThat(transformedEvent.metadata())
                .describedAs("transformed event metadata")
                .containsAllEntriesOf(expectedEvent.metadata());
    }

    static Stream<Arguments> testMessages() {
        return Stream.of(
                arguments(
                        named(
                                "message without headers",
                                message(
                                        UUID.fromString("bb15137d-8f16-4a19-a023-6845b9d1bead"),
                                        TestPayload.newBuilder().setText("test payload 1").build(),
                                        1734149827923l,
                                        null)),
                        named("[partition=1, offset=5]", context(1, 5)),
                        named(
                                "event without headers",
                                Event.from(
                                        UUID.fromString("bb15137d-8f16-4a19-a023-6845b9d1bead"),
                                        TestPayload.newBuilder().setText("test payload 1").build(),
                                        Instant.ofEpochMilli(1734149827923l),
                                        Map.of("partition", 1, "offset", 5l)))),
                arguments(
                        named(
                                "message with headers",
                                message(
                                        UUID.fromString("23debd32-09cd-4a20-a403-c18793ecd2d2"),
                                        TestPayload.newBuilder().setText("test payload 2").build(),
                                        1734174935363l,
                                        "15".getBytes())),
                        named("[partition=2, offset=8]", context(2, 8)),
                        named(
                                "event with headers",
                                Event.from(
                                        UUID.fromString("23debd32-09cd-4a20-a403-c18793ecd2d2"),
                                        TestPayload.newBuilder().setText("test payload 2").build(),
                                        Instant.ofEpochMilli(1734174935363l),
                                        Map.of("partition", 2, "offset", 8l, "header.id", "15".getBytes())))));
    }

    @SuppressWarnings("unchecked")
    private static FixedKeyRecord<UUID, SpecificRecord> message(
            UUID key,
            SpecificRecord payload,
            long timestamp,
            byte[] id) {
        var message = Mockito.mock(FixedKeyRecord.class);

        given(message.key()).willReturn(key);
        given(message.value()).willReturn(payload);
        given(message.timestamp()).willReturn(timestamp);
        given(message.headers())
                .willReturn(id != null ? new RecordHeaders(List.of(new RecordHeader("id", id))) : new RecordHeaders());

        return message;
    }

    @SuppressWarnings("unchecked")
    private static FixedKeyProcessorContext<UUID, Event<SpecificRecord>> context(int partition, long offset) {
        var recordMetadata = Mockito.mock(RecordMetadata.class);

        given(recordMetadata.partition()).willReturn(partition);
        given(recordMetadata.offset()).willReturn(offset);

        var context = Mockito.mock(FixedKeyProcessorContext.class);

        given(context.recordMetadata()).willReturn(Optional.of(recordMetadata));

        return context;
    }
}
