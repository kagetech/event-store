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

package tech.kage.event.kafka.reactor;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;
import java.util.Optional;

import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.UUIDDeserializer;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import reactor.kafka.receiver.MicrometerConsumerListener;

/**
 * Tests of Kafka receiver configuration.
 * 
 * @author Dariusz Szpakowski
 */
class KafkaReceiverConfigurationTest {
    // UUT
    ReactorKafkaEventStore.Config config = new ReactorKafkaEventStore.Config();

    @Test
    void createsDefaultReceiverConfiguration() {
        // Given
        var kafkaProperties = new KafkaProperties();

        var expectedIsolationLevel = "read_committed";
        var expectedEnableAutoCommit = "false";
        var expectedAutoOffsetReset = "earliest";
        var expectedKeyDeserializer = StringDeserializer.class;
        var expectedValueDeserializer = ByteArrayDeserializer.class;

        // When
        var receiverOptions = config.kafkaReceiverOptions(kafkaProperties, Optional.empty());

        // Then
        var consumerProperties = receiverOptions.consumerProperties();

        assertThat(consumerProperties.get("isolation.level"))
                .describedAs("isolation level")
                .isEqualTo(expectedIsolationLevel);

        assertThat(consumerProperties.get("enable.auto.commit"))
                .describedAs("enable auto commit")
                .isEqualTo(expectedEnableAutoCommit);

        assertThat(consumerProperties.get("auto.offset.reset"))
                .describedAs("auto offset reset")
                .isEqualTo(expectedAutoOffsetReset);

        assertThat(consumerProperties.get("key.deserializer"))
                .describedAs("key deserializer")
                .isEqualTo(expectedKeyDeserializer);

        assertThat(consumerProperties.get("value.deserializer"))
                .describedAs("value deserializer")
                .isEqualTo(expectedValueDeserializer);

        assertThat(receiverOptions.consumerListener())
                .describedAs("consumer listener")
                .isNull();
    }

    @Test
    void configuresMicrometerMetricsIfMeterRegistryIsAvailable() {
        // Given
        var kafkaProperties = new KafkaProperties();

        // When
        var receiverOptions = config.kafkaReceiverOptions(kafkaProperties, Optional.of(new SimpleMeterRegistry()));

        // Then
        var consumerListener = receiverOptions.consumerListener();

        assertThat(consumerListener)
                .describedAs("consumer listener")
                .isInstanceOf(MicrometerConsumerListener.class);
    }

    @Test
    void allowsOverridingDefaultReceiverConfiguration() {
        // Given
        var kafkaProperties = new KafkaProperties();

        var isolationLevel = KafkaProperties.IsolationLevel.READ_UNCOMMITTED;
        var enableAutoCommit = true;
        var autoOffsetReset = "latest";
        var specificAvroReader = "true";
        var keyDeserializer = UUIDDeserializer.class;
        var valueDeserializer = ByteArrayDeserializer.class;
        var valueSubjectNameStrategy = "io.confluent.kafka.serializers.subject.TopicNameStrategy";

        var consumerConfig = kafkaProperties.getConsumer();

        consumerConfig.setIsolationLevel(isolationLevel);
        consumerConfig.setEnableAutoCommit(enableAutoCommit);
        consumerConfig.setAutoOffsetReset(autoOffsetReset);
        consumerConfig.setKeyDeserializer(keyDeserializer);
        consumerConfig.setValueDeserializer(valueDeserializer);

        kafkaProperties
                .getProperties()
                .putAll(
                        Map.of(
                                "specific.avro.reader", specificAvroReader,
                                "value.subject.name.strategy", valueSubjectNameStrategy));

        var expectedIsolationLevel = "read_committed";
        var expectedEnableAutoCommit = "false";
        var expectedAutoOffsetReset = "earliest";
        var expectedSpecificAvroReader = specificAvroReader;
        var expectedKeyDeserializer = UUIDDeserializer.class;

        // When
        var receiverOptions = config.kafkaReceiverOptions(kafkaProperties, Optional.empty());

        // Then
        var consumerProperties = receiverOptions.consumerProperties();

        assertThat(consumerProperties.get("isolation.level"))
                .describedAs("isolation level")
                .isEqualTo(expectedIsolationLevel);

        assertThat(consumerProperties.get("enable.auto.commit"))
                .describedAs("enable auto commit")
                .isEqualTo(expectedEnableAutoCommit);

        assertThat(consumerProperties.get("auto.offset.reset"))
                .describedAs("auto offset reset")
                .isEqualTo(expectedAutoOffsetReset);

        assertThat(consumerProperties.get("specific.avro.reader"))
                .describedAs("specific avro reader")
                .isEqualTo(expectedSpecificAvroReader);

        assertThat(consumerProperties.get("key.deserializer"))
                .describedAs("key deserializer")
                .isEqualTo(expectedKeyDeserializer);
    }
}
