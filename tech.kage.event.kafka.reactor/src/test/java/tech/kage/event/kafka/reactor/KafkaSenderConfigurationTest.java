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
 * Tests of Kafka sender configuration.
 * 
 * @author Dariusz Szpakowski
 */
class KafkaSenderConfigurationTest {
    // UUT
    ReactorKafkaEventStore.Config config = new ReactorKafkaEventStore.Config();

    @Test
    void createsSenderConfiguration() {
        // Given
        var kafkaProperties = new KafkaProperties();

        var expectedKeySerializer = "org.apache.kafka.common.serialization.UUIDSerializer";
        var expectedValueSerializer = "io.confluent.kafka.serializers.KafkaAvroSerializer";
        var expectedValueSubjectNameStrategy = "io.confluent.kafka.serializers.subject.RecordNameStrategy";

        // When
        var senderOptions = config.kafkaSenderOptions(kafkaProperties);

        // Then
        var producerProperties = senderOptions.producerProperties();

        assertThat(producerProperties.get("key.serializer"))
                .describedAs("key serializer")
                .isEqualTo(expectedKeySerializer);

        assertThat(producerProperties.get("value.serializer"))
                .describedAs("value serializer")
                .isEqualTo(expectedValueSerializer);

        assertThat(producerProperties.get("value.subject.name.strategy"))
                .describedAs("value subject name strategy")
                .isEqualTo(expectedValueSubjectNameStrategy);
    }
}