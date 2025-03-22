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

package tech.kage.event.postgres;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.serialization.Serializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;

import tech.kage.event.crypto.EventEncryptor;

/**
 * Auto-configuration of {@link PostgresEventStore}.
 * 
 * @author Dariusz Szpakowski
 */
@AutoConfiguration
@Import({ PostgresEventStore.class, EventEncryptor.class })
class PostgresEventStoreAutoConfiguration {
    private static final String SCHEMA_REGISTRY_URL_CONFIG = "schema.registry.url";
    private static final String SCHEMA_REGISTRY_URL_NOT_SET_ERROR = "schema.registry.url must be set";

    private static final String VALUE_SUBJECT_NAME_STRATEGY_CONFIG = "value.subject.name.strategy";
    private static final String VALUE_SUBJECT_NAME_STRATEGY = "io.confluent.kafka.serializers.subject.RecordNameStrategy";

    private static final String KAFKA_AVRO_SERIALIZER_CLASS = "io.confluent.kafka.serializers.KafkaAvroSerializer";

    /**
     * Creates an Avro enabled Kafka {@link Serializer}.
     * 
     * @param schemaRegistryUrl address pointing to a Confluent Schema Registry
     *                          instance (required if {@link KafkaProperties} bean
     *                          is not available)
     * @param kafkaProperties   {@link KafkaProperties} bean used for configuring
     *                          the serializer (optional)
     * 
     * @return an instance of {@link Serializer}
     */
    @Bean
    Serializer<SpecificRecord> kafkaAvroSerializer(
            @Value("${" + SCHEMA_REGISTRY_URL_CONFIG + ":#{null}}") String schemaRegistryUrl,
            Optional<KafkaProperties> kafkaProperties) {
        var serializerConfig = kafkaProperties.isPresent()
                ? kafkaProperties.get().getProperties()
                : Map.of(
                        SCHEMA_REGISTRY_URL_CONFIG,
                        Objects.requireNonNull(schemaRegistryUrl, SCHEMA_REGISTRY_URL_NOT_SET_ERROR),
                        VALUE_SUBJECT_NAME_STRATEGY_CONFIG, VALUE_SUBJECT_NAME_STRATEGY);

        var kafkaAvroSerializer = getSerializerInstance();

        kafkaAvroSerializer.configure(serializerConfig, false);

        return kafkaAvroSerializer;
    }

    /**
     * Constructs a new Kafka Avro Serializer instance. Uses reflection because
     * {@code kafka-avro-serializer} dependency is not compatible with
     * {@code module-info.java} (split package).
     * 
     * @return new Kafka Avro Serializer instance
     */
    @SuppressWarnings("unchecked")
    private Serializer<SpecificRecord> getSerializerInstance() {
        try {
            var ctor = Class.forName(KAFKA_AVRO_SERIALIZER_CLASS).getConstructor();

            return (Serializer<SpecificRecord>) ctor.newInstance();
        } catch (Exception e) {
            throw new IllegalArgumentException("Unable to instantiate serializer " + KAFKA_AVRO_SERIALIZER_CLASS, e);
        }
    }
}
