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

import java.util.List;

import org.apache.kafka.common.serialization.Serdes.StringSerde;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;

/**
 * Tests of Kafka Streams configuration.
 * 
 * @author Dariusz Szpakowski
 */
class KafkaStreamsConfigurationTest {
    // UUT
    KafkaStreamsEventStore.Config config = new KafkaStreamsEventStore.Config();

    @Test
    void createsDefaultKafkaStreamsConfig() {
        // Given
        var applicationId = "test-app";

        var kafkaProperties = new KafkaProperties();

        kafkaProperties.setBootstrapServers(List.of("localhost:9092"));
        kafkaProperties.getProperties().put("schema.registry.url", "http://localhost:8989");

        var expectedBootstrapServers = "localhost:9092";
        var expectedSchemaRegistryUrl = "http://localhost:8989";
        var expectedKeySerde = StringSerde.class;
        var expectedValueSerde = "io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde";
        var expectedProcessingGuarantee = "exactly_once_v2";
        var expectedValueSubjectNameStrategy = "io.confluent.kafka.serializers.subject.RecordNameStrategy";

        // When
        var kStreamsConfig = config.kStreamsConfig(kafkaProperties, applicationId);

        // Then
        var configurationProperties = kStreamsConfig.asProperties();

        assertThat(configurationProperties.get("application.id"))
                .describedAs("application id")
                .isEqualTo(applicationId);

        assertThat(configurationProperties.get("bootstrap.servers"))
                .describedAs("bootstrap servers")
                .isEqualTo(expectedBootstrapServers);

        assertThat(configurationProperties.get("schema.registry.url"))
                .describedAs("schema registry url")
                .isEqualTo(expectedSchemaRegistryUrl);

        assertThat(configurationProperties.get("default.key.serde"))
                .describedAs("key serde")
                .isEqualTo(expectedKeySerde);

        assertThat(configurationProperties.get("default.value.serde"))
                .describedAs("value serde")
                .isEqualTo(expectedValueSerde);

        assertThat(configurationProperties.get("processing.guarantee"))
                .describedAs("processing guarantee")
                .isEqualTo(expectedProcessingGuarantee);

        assertThat(configurationProperties.get("value.subject.name.strategy"))
                .describedAs("value subject name strategy")
                .isEqualTo(expectedValueSubjectNameStrategy);
    }

    @Test
    void allowsOverridingDefaultKafkaStreamsConfig() {
        // Given
        var applicationId = "test-app";

        var kafkaProperties = new KafkaProperties();

        kafkaProperties.setBootstrapServers(List.of("localhost:9092"));
        kafkaProperties.getProperties().put("schema.registry.url", "http://localhost:8989");
        kafkaProperties.getProperties().put(
                "default.key.serde",
                "org.apache.kafka.common.serialization.Serdes$UUIDSerde");
        kafkaProperties.getProperties().put(
                "value.subject.name.strategy",
                "io.confluent.kafka.serializers.subject.TopicNameStrategy");

        // invalid config values that should be overridden
        kafkaProperties.getProperties().put("application.id", "invalid-application-id");
        kafkaProperties.getProperties().put("processing.guarantee", "invalid-guarantee");
        kafkaProperties.getProperties().put("default.value.serde", "invalid-value-serde");

        var expectedBootstrapServers = "localhost:9092";
        var expectedSchemaRegistryUrl = "http://localhost:8989";
        var expectedKeySerde = "org.apache.kafka.common.serialization.Serdes$UUIDSerde";
        var expectedValueSerde = "io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde";
        var expectedProcessingGuarantee = "exactly_once_v2";
        var expectedValueSubjectNameStrategy = "io.confluent.kafka.serializers.subject.TopicNameStrategy";

        // When
        var kStreamsConfig = config.kStreamsConfig(kafkaProperties, applicationId);

        // Then
        var configurationProperties = kStreamsConfig.asProperties();

        assertThat(configurationProperties.get("application.id"))
                .describedAs("application id")
                .isEqualTo(applicationId);

        assertThat(configurationProperties.get("bootstrap.servers"))
                .describedAs("bootstrap servers")
                .isEqualTo(expectedBootstrapServers);

        assertThat(configurationProperties.get("schema.registry.url"))
                .describedAs("schema registry url")
                .isEqualTo(expectedSchemaRegistryUrl);

        assertThat(configurationProperties.get("default.key.serde"))
                .describedAs("key serde")
                .isEqualTo(expectedKeySerde);

        assertThat(configurationProperties.get("default.value.serde"))
                .describedAs("value serde")
                .isEqualTo(expectedValueSerde);

        assertThat(configurationProperties.get("processing.guarantee"))
                .describedAs("processing guarantee")
                .isEqualTo(expectedProcessingGuarantee);

        assertThat(configurationProperties.get("value.subject.name.strategy"))
                .describedAs("value subject name strategy")
                .isEqualTo(expectedValueSubjectNameStrategy);
    }
}
