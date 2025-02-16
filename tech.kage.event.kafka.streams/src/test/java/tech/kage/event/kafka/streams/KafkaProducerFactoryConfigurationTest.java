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

package tech.kage.event.kafka.streams;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Named.named;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import java.util.stream.Stream;

import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;

/**
 * Tests of Kafka producer factory configuration.
 * 
 * @author Dariusz Szpakowski
 */
class KafkaProducerFactoryConfigurationTest {
    // UUT
    KafkaStreamsEventStore.Config config = new KafkaStreamsEventStore.Config();

    @ParameterizedTest
    @MethodSource("testKafkaProperties")
    void createsKafkaProducerFactoryConfig(KafkaProperties kafkaProperties) {
        // Given
        var expectedKeySerializer = StringSerializer.class;
        var expectedValueSerializer = ByteArraySerializer.class;
        var expectedTransactionIdPrefix = kafkaProperties.getProducer().getTransactionIdPrefix();

        // When
        var kafkaProducerFactoryConfig = config.kafkaProducerFactory(kafkaProperties);

        // Then
        var configurationProperties = kafkaProducerFactoryConfig.getConfigurationProperties();

        assertThat(configurationProperties.get("key.serializer"))
                .describedAs("key serializer")
                .isEqualTo(expectedKeySerializer);

        assertThat(configurationProperties.get("value.serializer"))
                .describedAs("value serializer")
                .isEqualTo(expectedValueSerializer);

        assertThat(kafkaProducerFactoryConfig.getTransactionIdPrefix())
                .describedAs("transaction id prefix")
                .isEqualTo(expectedTransactionIdPrefix);
    }

    static Stream<Arguments> testKafkaProperties() {
        return Stream.of(
                arguments(named("null transaction-id-prefix", kafkaPropertiesWithoutTransactionIdPrefix())),
                arguments(named("non-null transaction-id-prefix", kafkaPropertiesWithTransactionIdPrefix())));
    }

    private static KafkaProperties kafkaPropertiesWithoutTransactionIdPrefix() {
        var kafkaProperties = new KafkaProperties();

        kafkaProperties.getProducer().setTransactionIdPrefix(null);

        return kafkaProperties;
    }

    private static KafkaProperties kafkaPropertiesWithTransactionIdPrefix() {
        var kafkaProperties = kafkaPropertiesWithoutTransactionIdPrefix();

        kafkaProperties.getProducer().setTransactionIdPrefix("test-transaction-prefix-");

        return kafkaProperties;
    }
}
