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

package tech.kage.event.kafka.reactor;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;

/**
 * Tests of Kafka receiver configuration.
 * 
 * @author Dariusz Szpakowski
 */
class KafkaReceiverConfigurationTest {
    // UUT
    ReactorKafkaEventStore.Config config = new ReactorKafkaEventStore.Config();

    @Test
    void createsReceiverConfiguration() {
        // Given
        var kafkaProperties = new KafkaProperties();

        var expectedIsolationLevel = "read_committed";
        var expectedEnableAutoCommit = "false";
        var expectedAutoOffsetReset = "earliest";
        var expectedSpecificAvroReader = "true";
        var expectedKeyDeserializer = "org.apache.kafka.common.serialization.UUIDDeserializer";
        var expectedValueDeserializer = "io.confluent.kafka.serializers.KafkaAvroDeserializer";
        var expectedValueSubjectNameStrategy = "io.confluent.kafka.serializers.subject.RecordNameStrategy";

        // When
        var receiverOptions = config.kafkaReceiverOptions(kafkaProperties);

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

        assertThat(consumerProperties.get("value.deserializer"))
                .describedAs("value deserializer")
                .isEqualTo(expectedValueDeserializer);

        assertThat(consumerProperties.get("value.subject.name.strategy"))
                .describedAs("value subject name strategy")
                .isEqualTo(expectedValueSubjectNameStrategy);
    }
}
