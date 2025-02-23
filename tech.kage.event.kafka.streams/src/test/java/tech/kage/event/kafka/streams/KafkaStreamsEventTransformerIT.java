/*
 * Copyright (c) 2024-2025, Dariusz Szpakowski
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

import static com.google.crypto.tink.aead.PredefinedAeadParameters.AES256_GCM;
import static java.util.Comparator.comparing;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Named.named;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.mockito.BDDMockito.given;
import static org.springframework.beans.factory.config.BeanDefinition.SCOPE_PROTOTYPE;
import static tech.kage.event.EventStore.ENCRYPTION_KEY_ID;
import static tech.kage.event.EventStore.SOURCE_ID;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.security.GeneralSecurityException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Stream;

import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.processor.api.FixedKeyRecord;
import org.apache.kafka.streams.processor.api.RecordMetadata;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Scope;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.bean.override.convention.TestBean;

import com.google.crypto.tink.Aead;
import com.google.crypto.tink.KeysetHandle;
import com.google.crypto.tink.RegistryConfiguration;

import tech.kage.event.Event;
import tech.kage.event.crypto.EventEncryptor;

/**
 * Integration tests for {@link KafkaStreamsEventTransformer}.
 * 
 * @author Dariusz Szpakowski
 */
@SpringBootTest
@ActiveProfiles("test")
class KafkaStreamsEventTransformerIT {
    // UUT
    @Autowired
    KafkaStreamsEventTransformer<Object, SpecificRecord> eventTransformer;

    @Autowired
    EventEncryptor eventEncryptor;

    @TestBean
    Serializer<SpecificRecord> kafkaAvroSerializer;

    @TestBean
    Deserializer<SpecificRecord> kafkaAvroDeserializer;

    static final String TEST_EVENTS = "test_events";

    static Serializer<SpecificRecord> kafkaAvroSerializer() {
        return new Serializer<>() {
            @Override
            public byte[] serialize(String topic, SpecificRecord data) {
                return KafkaStreamsEventTransformerIT.serialize(data);
            }
        };
    }

    static Deserializer<SpecificRecord> kafkaAvroDeserializer() {
        return new Deserializer<>() {
            @Override
            public SpecificRecord deserialize(String topic, byte[] data) {
                try {
                    return new SpecificDatumReader<SpecificRecord>(TestPayload.SCHEMA$)
                            .read(null, DecoderFactory.get().binaryDecoder(data, null));
                } catch (IOException e) {
                    throw new UncheckedIOException("Unable to deserialize data", e);
                }
            }
        };
    }

    static final Map<URI, KeysetHandle> testKms = new HashMap<>();

    @Configuration
    @Import({ KafkaStreamsEventTransformer.class, EventEncryptor.class })
    static class TestConfiguration {
        @Bean
        @Scope(SCOPE_PROTOTYPE)
        Aead aead(URI encryptionKey) throws GeneralSecurityException {
            return testKms.get(encryptionKey).getPrimitive(RegistryConfiguration.get(), Aead.class);
        }
    }

    @ParameterizedTest
    @MethodSource("testMessages")
    void transformsMessageIntoEvent(
            FixedKeyRecord<Object, byte[]> message,
            Optional<RecordMetadata> recordMetadata,
            Event<?, ?> expectedEvent) {
        // When
        var transformedEvent = eventTransformer.transform(message, recordMetadata);

        // Then
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

    @ParameterizedTest
    @MethodSource("testEncryptedMessages")
    void decryptsAndTransformsMessageIntoEvent(
            FixedKeyRecord<Object, byte[]> message,
            Optional<RecordMetadata> recordMetadata,
            Event<?, ?> expectedEvent) throws GeneralSecurityException {
        // Given
        var encryptionKey = URI.create(new String(message.headers().lastHeader(ENCRYPTION_KEY_ID).value()));

        testKms.putIfAbsent(encryptionKey, KeysetHandle.generateNew(AES256_GCM));

        var encryptedMessage = encrypt(message, encryptionKey);

        // When
        var transformedEvent = eventTransformer.transform(encryptedMessage, recordMetadata);

        // Then
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

    @ParameterizedTest
    @MethodSource("testEncryptedMessages")
    void throwsExceptionWhenInvalidEncryptionKey(
            FixedKeyRecord<Object, byte[]> message,
            Optional<RecordMetadata> recordMetadata,
            Event<?, ?> expectedEvent) throws GeneralSecurityException {
        // Given
        var encryptionKey = URI.create(new String(message.headers().lastHeader(ENCRYPTION_KEY_ID).value()));
        var invalidEncryptionKey = URI.create("test-kms://test-keys/invalid");

        testKms.putIfAbsent(encryptionKey, KeysetHandle.generateNew(AES256_GCM));
        testKms.putIfAbsent(invalidEncryptionKey, KeysetHandle.generateNew(AES256_GCM));

        var encryptedMessage = encrypt(message, invalidEncryptionKey);

        // When
        var thrown = assertThrows(Throwable.class,
                () -> eventTransformer.transform(encryptedMessage, recordMetadata));

        // Then
        assertThat(thrown)
                .describedAs("thrown exception")
                .isInstanceOf(SerializationException.class)
                .hasRootCauseExactlyInstanceOf(GeneralSecurityException.class)
                .hasRootCauseMessage("decryption failed");
    }

    @ParameterizedTest
    @MethodSource("testEncryptedMessages")
    void throwsExceptionWhenEventPayloadIntegrityIsViolated(
            FixedKeyRecord<Object, byte[]> message,
            Optional<RecordMetadata> recordMetadata,
            Event<?, ?> expectedEvent) throws GeneralSecurityException {
        // Given
        var invalidKey = UUID.randomUUID(); // use invalid key

        var encryptionKey = URI.create(new String(message.headers().lastHeader(ENCRYPTION_KEY_ID).value()));

        testKms.putIfAbsent(encryptionKey, KeysetHandle.generateNew(AES256_GCM));

        var encryptedReceiverRecord = encrypt(message, encryptionKey);

        var encryptedMessageWithInvalidKey = message(
                invalidKey,
                encryptedReceiverRecord.value(),
                encryptedReceiverRecord.timestamp(),
                encryptedReceiverRecord.headers());

        // When
        var thrown = assertThrows(Throwable.class,
                () -> eventTransformer.transform(encryptedMessageWithInvalidKey, recordMetadata));

        // Then
        assertThat(thrown)
                .describedAs("thrown exception")
                .isInstanceOf(SerializationException.class)
                .hasRootCauseExactlyInstanceOf(GeneralSecurityException.class)
                .hasRootCauseMessage("decryption failed");
    }

    @ParameterizedTest
    @MethodSource("testEvents")
    void transformsEventIntoMessage(Event<Object, SpecificRecord> event, FixedKeyRecord<?, byte[]> expectedRecord) {
        // When
        var transformedRecord = eventTransformer.transform(event, message(event.key()), TEST_EVENTS);

        // Then
        assertThat(transformedRecord.key())
                .describedAs("transformed record key")
                .isEqualTo(expectedRecord.key());

        assertThat(transformedRecord.value())
                .describedAs("transformed record value")
                .isEqualTo(expectedRecord.value());

        assertThat(transformedRecord.timestamp())
                .describedAs("transformed record timestamp")
                .isEqualTo(expectedRecord.timestamp());

        assertThat(transformedRecord.headers())
                .describedAs("transformed record headers")
                .isEqualTo(expectedRecord.headers());
    }

    @ParameterizedTest
    @MethodSource("testEvents")
    void encryptsAndTransformsEventIntoMessage(Event<Object, SpecificRecord> event,
            FixedKeyRecord<?, byte[]> expectedRecord) throws GeneralSecurityException {
        // Given
        var encryptionKey = URI.create("test-kms://test-keys/" + event.key().toString());

        testKms.putIfAbsent(encryptionKey, KeysetHandle.generateNew(AES256_GCM));

        var metadataWithEncryptionKey = new HashMap<>(event.metadata());
        metadataWithEncryptionKey.put(ENCRYPTION_KEY_ID, encryptionKey.toString().getBytes());

        var expectedHeaderList = new ArrayList<Header>();

        for (var header : expectedRecord.headers()) {
            expectedHeaderList.add(header);
        }

        expectedHeaderList.add(new RecordHeader(ENCRYPTION_KEY_ID, encryptionKey.toString().getBytes()));

        Collections.sort(expectedHeaderList, comparing(Header::key));

        var expectedHeaders = new RecordHeaders(expectedHeaderList);

        // When
        var transformedRecord = eventTransformer.transform(event, message(event.key()), TEST_EVENTS, encryptionKey);

        // Then
        assertThat(transformedRecord.key())
                .describedAs("transformed record key")
                .isEqualTo(expectedRecord.key());

        var decryptedTransformedRecordValue = eventEncryptor.decrypt(
                transformedRecord.value(),
                event.key(),
                event.timestamp(),
                metadataWithEncryptionKey);

        assertThat(decryptedTransformedRecordValue)
                .describedAs("decrypted transformed record value")
                .isEqualTo(expectedRecord.value());

        assertThat(transformedRecord.timestamp())
                .describedAs("transformed record timestamp")
                .isEqualTo(expectedRecord.timestamp());

        assertThat(transformedRecord.headers())
                .describedAs("transformed record headers")
                .isEqualTo(expectedHeaders);
    }

    static Stream<Arguments> testMessages() {
        return Stream.of(
                arguments(
                        named(
                                "message without metadata with UUID key",
                                message(
                                        UUID.fromString("544640f3-5263-4d1b-a554-44d46f2f41d7"),
                                        serialize(TestPayload.newBuilder().setText("no metadata").build()),
                                        1734159827923l,
                                        List.of())),
                        named("no record metadata", Optional.empty()),
                        named(
                                "event without metadata with UUID key",
                                Event.from(
                                        UUID.fromString("544640f3-5263-4d1b-a554-44d46f2f41d7"),
                                        TestPayload.newBuilder().setText("no metadata").build(),
                                        Instant.ofEpochMilli(1734159827923l),
                                        Map.of()))),
                arguments(
                        named(
                                "message without headers with String key",
                                message(
                                        "test-event--1",
                                        serialize(TestPayload.newBuilder().setText("test payload 1").build()),
                                        1734149827923l,
                                        List.of())),
                        named("[partition=1, offset=5]", recordMetadata(1, 5)),
                        named(
                                "event without headers with String key",
                                Event.from(
                                        "test-event--1",
                                        TestPayload.newBuilder().setText("test payload 1").build(),
                                        Instant.ofEpochMilli(1734149827923l),
                                        Map.of("partition", 1, "offset", 5l)))),
                arguments(
                        named(
                                "message with headers with Integer key",
                                message(
                                        2,
                                        serialize(TestPayload.newBuilder().setText("test payload 2").build()),
                                        1734174935363l,
                                        List.of(new RecordHeader(SOURCE_ID, "15".getBytes())))),
                        named("[partition=2, offset=8]", recordMetadata(2, 8)),
                        named(
                                "event with headers with Integer key",
                                Event.from(
                                        2,
                                        TestPayload.newBuilder().setText("test payload 2").build(),
                                        Instant.ofEpochMilli(1734174935363l),
                                        Map.of("partition", 2, "offset", 8l, "header.id", "15".getBytes())))));
    }

    static Stream<Arguments> testEncryptedMessages() {
        return Stream.of(
                arguments(
                        named(
                                "encrypted message without headers",
                                message(
                                        UUID.fromString("bb15137d-8f16-4a19-a023-6845b9d1bead"),
                                        serialize(TestPayload.newBuilder().setText("test payload 1").build()),
                                        1734149827923l,
                                        List.of(
                                                new RecordHeader(
                                                        ENCRYPTION_KEY_ID,
                                                        "test-kms://test-keys/bb15137d-8f16-4a19-a023-6845b9d1bead"
                                                                .getBytes())))),
                        named("[partition=1, offset=5]", recordMetadata(1, 5)),
                        named(
                                "event without headers",
                                Event.from(
                                        UUID.fromString("bb15137d-8f16-4a19-a023-6845b9d1bead"),
                                        TestPayload.newBuilder().setText("test payload 1").build(),
                                        Instant.ofEpochMilli(1734149827923l),
                                        Map.of(
                                                "partition", 1,
                                                "offset", 5l,
                                                "header." + ENCRYPTION_KEY_ID,
                                                "test-kms://test-keys/bb15137d-8f16-4a19-a023-6845b9d1bead"
                                                        .getBytes())))),
                arguments(
                        named(
                                "encrypted message with headers with String key",
                                message(
                                        "test-event-2",
                                        serialize(TestPayload.newBuilder().setText("test payload 2").build()),
                                        1734174935363l,
                                        List.of(
                                                new RecordHeader(SOURCE_ID, "15".getBytes()),
                                                new RecordHeader(
                                                        ENCRYPTION_KEY_ID,
                                                        "test-kms://test-keys/bb15137d-8f16-4a19-a023-6845b9d1bead"
                                                                .getBytes())))),
                        named("[partition=2, offset=8]", recordMetadata(2, 8)),
                        named(
                                "event with headers",
                                Event.from(
                                        "test-event-2",
                                        TestPayload.newBuilder().setText("test payload 2").build(),
                                        Instant.ofEpochMilli(1734174935363l),
                                        Map.of(
                                                "partition", 2,
                                                "offset", 8l,
                                                "header." + SOURCE_ID, "15".getBytes(),
                                                "header." + ENCRYPTION_KEY_ID,
                                                "test-kms://test-keys/bb15137d-8f16-4a19-a023-6845b9d1bead"
                                                        .getBytes())))));
    }

    static Stream<Arguments> testEvents() {
        return Stream.of(
                arguments(
                        named(
                                "event without headers with UUID key",
                                Event.from(
                                        UUID.fromString("bb15137d-8f16-4a19-a023-6845b9d1bead"),
                                        TestPayload.newBuilder().setText("test payload 1").build(),
                                        Instant.ofEpochMilli(1734149827923l),
                                        Map.of())),
                        named(
                                "producerRecord without headers with UUID key",
                                message(
                                        UUID.fromString("bb15137d-8f16-4a19-a023-6845b9d1bead"),
                                        serialize(TestPayload.newBuilder().setText("test payload 1").build()),
                                        1734149827923l,
                                        List.of()))),
                arguments(
                        named(
                                "event with headers with String key",
                                Event.from(
                                        "test-event-2",
                                        TestPayload.newBuilder().setText("test payload 2").build(),
                                        Instant.ofEpochMilli(1734174935363l),
                                        Map.of(
                                                "dTest", "meta_value".getBytes(),
                                                "zTest",
                                                UUID.fromString("788ee0da-3ca9-4fa1-9d84-3470a067d695")
                                                        .toString()
                                                        .getBytes(),
                                                "bTest", "1".getBytes()))),
                        named(
                                "producerRecord with headers with String key",
                                message(
                                        "test-event-2",
                                        serialize(TestPayload.newBuilder().setText("test payload 2").build()),
                                        1734174935363l,
                                        List.of(
                                                new RecordHeader("bTest", "1".getBytes()),
                                                new RecordHeader("dTest", "meta_value".getBytes()),
                                                new RecordHeader("zTest",
                                                        UUID.fromString("788ee0da-3ca9-4fa1-9d84-3470a067d695")
                                                                .toString()
                                                                .getBytes()))))));
    }

    private FixedKeyRecord<Object, ?> message(Object key) {
        return message(key, null, 0, (Headers) null);
    }

    private static FixedKeyRecord<Object, byte[]> message(
            Object key,
            byte[] payload,
            long timestamp,
            List<Header> headers) {
        return message(key, payload, timestamp, new RecordHeaders(headers));
    }

    @SuppressWarnings("unchecked")
    private static <V> FixedKeyRecord<Object, V> message(Object key, V payload, long timestamp, Headers headers) {
        try {
            var ctor = FixedKeyRecord.class.getDeclaredConstructor(Object.class, Object.class, long.class,
                    Headers.class);

            ctor.setAccessible(true);

            return (FixedKeyRecord<Object, V>) ctor.newInstance(key, payload, timestamp, headers);
        } catch (Exception e) {
            throw new IllegalArgumentException("Unable to instantiate FixedKeyRecord", e);
        }
    }

    private static Optional<RecordMetadata> recordMetadata(int partition, long offset) {
        var recordMetadata = Mockito.mock(RecordMetadata.class);

        given(recordMetadata.partition()).willReturn(partition);
        given(recordMetadata.offset()).willReturn(offset);

        return Optional.of(recordMetadata);
    }

    private static byte[] serialize(SpecificRecord payload) {
        try (var out = new ByteArrayOutputStream()) {
            var encoder = EncoderFactory.get().directBinaryEncoder(out, null);

            new GenericDatumWriter<>(TestPayload.SCHEMA$).write(payload, encoder);

            return out.toByteArray();
        } catch (IOException e) {
            throw new UncheckedIOException("Unable to serialize payload: " + payload, e);
        }
    }

    private FixedKeyRecord<Object, byte[]> encrypt(FixedKeyRecord<Object, byte[]> message, URI encryptionKey)
            throws GeneralSecurityException {
        var metadata = new HashMap<String, Object>();

        for (var header : message.headers()) {
            var headerKey = header.key();

            if (!(headerKey.equals(SOURCE_ID) || headerKey.equals(ENCRYPTION_KEY_ID))) {
                metadata.put(header.key(), header.value());
            }
        }

        var encryptedReceiverRecordValue = eventEncryptor
                .encrypt(
                        message.value(),
                        message.key(),
                        Instant.ofEpochMilli(message.timestamp()),
                        metadata,
                        encryptionKey);

        return message(message.key(), encryptedReceiverRecordValue, message.timestamp(), message.headers());
    }
}
