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

package tech.kage.event.kafka.reactor;

import java.util.HashMap;
import java.util.Optional;

import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.kafka.KafkaAutoConfiguration;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;

import io.micrometer.core.instrument.MeterRegistry;
import reactor.kafka.receiver.MicrometerConsumerListener;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;
import tech.kage.event.crypto.EventEncryptor;

/**
 * Auto-configuration of {@link ReactorKafkaEventStore}.
 * 
 * @author Dariusz Szpakowski
 */
@AutoConfiguration(before = KafkaAutoConfiguration.class)
@Import({ ReactorKafkaEventStore.class, ReactorKafkaEventTransformer.class, EventEncryptor.class })
class ReactorKafkaEventStoreAutoConfiguration {
    private static final String VALUE_SUBJECT_NAME_STRATEGY_CONFIG = "value.subject.name.strategy";
    private static final String VALUE_SUBJECT_NAME_STRATEGY = "io.confluent.kafka.serializers.subject.RecordNameStrategy";

    private static final String SPECIFIC_AVRO_READER_CONFIG = "specific.avro.reader";

    private static final String KAFKA_AVRO_SERIALIZER_CLASS = "io.confluent.kafka.serializers.KafkaAvroSerializer";
    private static final String KAFKA_AVRO_DESERIALIZER_CLASS = "io.confluent.kafka.serializers.KafkaAvroDeserializer";

    @Bean
    KafkaSender<Object, byte[]> kafkaSender(SenderOptions<Object, byte[]> senderOptions) {
        return KafkaSender.create(senderOptions);
    }

    @Bean
    SenderOptions<Object, byte[]> kafkaSenderOptions(KafkaProperties properties) {
        var props = properties.buildProducerProperties(null);

        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);

        return SenderOptions.create(props);
    }

    @Bean
    ReceiverOptions<Object, byte[]> kafkaReceiverOptions(
            KafkaProperties properties,
            Optional<MeterRegistry> meterRegistry) {
        var props = properties.buildConsumerProperties(null);

        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);

        return ReceiverOptions
                .<Object, byte[]>create(props)
                .consumerListener(
                        meterRegistry.isPresent()
                                ? new MicrometerConsumerListener(meterRegistry.get())
                                : null);
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
