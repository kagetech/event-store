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

import java.util.HashMap;

import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.StreamsConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.kafka.KafkaAutoConfiguration;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.config.KafkaStreamsConfiguration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.ProducerFactory;

import tech.kage.event.crypto.EventEncryptor;

/**
 * Auto-configuration of {@link KafkaStreamsEventStore}.
 * 
 * @author Dariusz Szpakowski
 */
@AutoConfiguration(before = KafkaAutoConfiguration.class)
@EnableKafkaStreams
@Import({ KafkaStreamsEventStore.class, ProducerRecordEventTransformer.class, KafkaStreamsEventTransformer.class,
        EventEncryptor.class })
class KafkaStreamsEventStoreAutoConfiguration {
    private static final String DEFAULT_VALUE_SERDE_CLASS = "io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde";
    private static final String VALUE_SUBJECT_NAME_STRATEGY_CONFIG = "value.subject.name.strategy";
    private static final String VALUE_SUBJECT_NAME_STRATEGY = "io.confluent.kafka.serializers.subject.RecordNameStrategy";

    private static final String SPECIFIC_AVRO_READER_CONFIG = "specific.avro.reader";

    private static final String KAFKA_AVRO_SERIALIZER_CLASS = "io.confluent.kafka.serializers.KafkaAvroSerializer";
    private static final String KAFKA_AVRO_DESERIALIZER_CLASS = "io.confluent.kafka.serializers.KafkaAvroDeserializer";

    @Bean
    ProducerFactory<?, ?> kafkaProducerFactory(KafkaProperties properties) {
        var props = properties.buildProducerProperties(null);

        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);

        var factory = new DefaultKafkaProducerFactory<>(props);

        var transactionIdPrefix = properties.getProducer().getTransactionIdPrefix();

        if (transactionIdPrefix != null) {
            factory.setTransactionIdPrefix(transactionIdPrefix);
        }

        return factory;
    }

    @Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
    KafkaStreamsConfiguration kStreamsConfig(
            KafkaProperties properties,
            @Value(value = "${kafka.streams.application.id}") String applicationId) {
        var props = new HashMap<String, Object>(properties.getProperties());

        props.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, String.join(",", properties.getBootstrapServers()));
        props.putIfAbsent(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, DEFAULT_VALUE_SERDE_CLASS);
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
        props.putIfAbsent(VALUE_SUBJECT_NAME_STRATEGY_CONFIG, VALUE_SUBJECT_NAME_STRATEGY);

        return new KafkaStreamsConfiguration(props);
    }

    @Bean
    Serializer<SpecificRecord> kafkaAvroSerializer(KafkaProperties properties) {
        var serializerConfig = new HashMap<>(properties.getProperties());

        serializerConfig.putIfAbsent(VALUE_SUBJECT_NAME_STRATEGY_CONFIG, VALUE_SUBJECT_NAME_STRATEGY);

        // Construct a new Kafka Avro Serializer instance via reflection because
        // kafka-avro-serializer dependency is not compatible with module-info.java
        // (split package).

        @SuppressWarnings("unchecked")
        var kafkaAvroSerializer = (Serializer<SpecificRecord>) getInstance(KAFKA_AVRO_SERIALIZER_CLASS);

        kafkaAvroSerializer.configure(serializerConfig, false);

        return kafkaAvroSerializer;
    }

    @Bean
    Deserializer<SpecificRecord> kafkaAvroDeserializer(KafkaProperties properties) {
        var deserializerConfig = new HashMap<>(properties.getProperties());

        deserializerConfig.putIfAbsent(SPECIFIC_AVRO_READER_CONFIG, "true");
        deserializerConfig.putIfAbsent(VALUE_SUBJECT_NAME_STRATEGY_CONFIG, VALUE_SUBJECT_NAME_STRATEGY);

        // Construct a new Kafka Avro Deserializer instance via reflection because
        // kafka-avro-serializer dependency is not compatible with module-info.java
        // (split package).

        @SuppressWarnings("unchecked")
        var kafkaAvroDeserializer = (Deserializer<SpecificRecord>) getInstance(KAFKA_AVRO_DESERIALIZER_CLASS);

        kafkaAvroDeserializer.configure(deserializerConfig, false);

        return kafkaAvroDeserializer;
    }

    private Object getInstance(String className) {
        try {
            var ctor = Class.forName(className).getConstructor();

            return ctor.newInstance();
        } catch (Exception e) {
            throw new IllegalArgumentException("Unable to instantiate " + className, e);
        }
    }
}
