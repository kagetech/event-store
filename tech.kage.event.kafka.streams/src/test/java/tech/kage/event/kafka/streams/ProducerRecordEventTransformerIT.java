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

import static com.google.crypto.tink.aead.PredefinedAeadParameters.AES256_GCM;
import static java.util.Comparator.comparing;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Named.named;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.springframework.beans.factory.config.BeanDefinition.SCOPE_PROTOTYPE;
import static tech.kage.event.EventStore.ENCRYPTION_KEY_ID;

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
import java.util.UUID;
import java.util.stream.Stream;

import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
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
 * Integration tests for {@link ProducerRecordEventTransformer}.
 * 
 * @author Dariusz Szpakowski
 */
@SpringBootTest
@ActiveProfiles("test")
class ProducerRecordEventTransformerIT {
    // UUT
    @Autowired
    ProducerRecordEventTransformer eventTransformer;

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
                return ProducerRecordEventTransformerIT.serialize(data);
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
    @Import({ ProducerRecordEventTransformer.class, EventEncryptor.class })
    static class TestConfiguration {
        @Bean
        @Scope(SCOPE_PROTOTYPE)
        Aead aead(URI encryptionKey) throws GeneralSecurityException {
            return testKms.get(encryptionKey).getPrimitive(RegistryConfiguration.get(), Aead.class);
        }
    }

    @ParameterizedTest
    @MethodSource("testEvents")
    void transformsEventIntoProducerRecord(Event<?> event, ProducerRecord<UUID, byte[]> expectedRecord) {
        // When
        var transformedRecord = eventTransformer.transform(event, TEST_EVENTS, null).block();

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
    void encryptsAndTransformsEventIntoProducerRecord(Event<?> event, ProducerRecord<UUID, byte[]> expectedRecord)
            throws GeneralSecurityException {
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
        var transformedRecord = eventTransformer.transform(event, TEST_EVENTS, encryptionKey).block();

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

    static Stream<Arguments> testEvents() {
        return Stream.of(
                arguments(
                        named(
                                "event without headers",
                                Event.from(
                                        UUID.fromString("bb15137d-8f16-4a19-a023-6845b9d1bead"),
                                        TestPayload.newBuilder().setText("test payload 1").build(),
                                        Instant.ofEpochMilli(1734149827923l),
                                        Map.of())),
                        named(
                                "producerRecord without headers",
                                producerRecord(
                                        UUID.fromString("bb15137d-8f16-4a19-a023-6845b9d1bead"),
                                        serialize(TestPayload.newBuilder().setText("test payload 1").build()),
                                        1734149827923l,
                                        List.of()))),
                arguments(
                        named(
                                "event with headers",
                                Event.from(
                                        UUID.fromString("23debd32-09cd-4a20-a403-c18793ecd2d2"),
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
                                "producerRecord with headers",
                                producerRecord(
                                        UUID.fromString("23debd32-09cd-4a20-a403-c18793ecd2d2"),
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

    private static ProducerRecord<UUID, byte[]> producerRecord(UUID key, byte[] payload, long timestamp,
            List<Header> headers) {
        return new ProducerRecord<>(TEST_EVENTS, null, timestamp, key, payload, headers);
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
}
