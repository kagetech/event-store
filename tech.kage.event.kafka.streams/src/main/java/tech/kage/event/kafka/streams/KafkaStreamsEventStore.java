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

package tech.kage.event.kafka.streams;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.processor.api.ContextualFixedKeyProcessor;
import org.apache.kafka.streams.processor.api.FixedKeyRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.config.KafkaStreamsConfiguration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.stereotype.Component;

import reactor.core.publisher.Mono;
import tech.kage.event.Event;
import tech.kage.event.EventStore;

/**
 * A Kafka-based implementation of {@link EventStore} storing events in Kafka
 * topics and allowing processing them using Kafka Streams APIs. Uses Apache
 * Avro for payload serialization and stores Avro schemas in Confluent Schema
 * Registry.
 * 
 * <p>
 * Requires the following configuration properties to be set (example values):
 * 
 * <ul>
 * <li>{@code spring.kafka.bootstrap-servers=localhost:9092}</li>
 * <li>{@code spring.kafka.properties.schema.registry.url=http://localhost:8989}</li>
 * <li>{@code kafka.streams.application.id=sample-kafka-streams-event-store}</li>
 * </ul>
 * 
 * The value of {@code kafka.streams.application.id} defines Kafka Streams
 * application id, {@code spring.kafka.bootstrap-servers} points to a Kafka
 * cluster and {@code spring.kafka.properties.schema.registry.url} points to a
 * Confluent Schema Registry instance.
 * 
 * @author Dariusz Szpakowski
 */
@Component
public class KafkaStreamsEventStore implements EventStore {
    private final KafkaTemplate<UUID, Object> kafkaTemplate;
    private final StreamsBuilder streamsBuilder;

    private Map<String, KStream<UUID, Event<SpecificRecord>>> streams = new ConcurrentHashMap<>();

    /**
     * Constructs a new {@link KafkaStreamsEventStore} instance.
     *
     * @param kafkaTemplate  an instance of {@link KafkaTemplate}
     * @param streamsBuilder an instance of {@link StreamsBuilder}
     */
    KafkaStreamsEventStore(KafkaTemplate<UUID, Object> kafkaTemplate, StreamsBuilder streamsBuilder) {
        this.kafkaTemplate = kafkaTemplate;
        this.streamsBuilder = streamsBuilder;
    }

    @Override
    public <T extends SpecificRecord> Mono<Event<T>> save(String topic, Event<T> event) {
        Objects.requireNonNull(topic, "topic must not be null");
        Objects.requireNonNull(event, "event must not be null");

        return Mono
                .fromFuture(
                        kafkaTemplate.send(
                                new ProducerRecord<>(
                                        topic,
                                        null,
                                        event.timestamp().toEpochMilli(),
                                        event.key(),
                                        event.payload())))
                .then(Mono.just(event));
    }

    /**
     * Subscribes to a stream of events in a given topic.
     * 
     * @param topic topic to subscribe to
     * 
     * @return {@link KStream} of events
     */
    public KStream<UUID, Event<SpecificRecord>> subscribe(String topic) {
        Objects.requireNonNull(topic, "topic must not be null");

        return streams.computeIfAbsent(
                topic,
                key -> streamsBuilder
                        .stream(topic, Consumed.<UUID, SpecificRecord>as(topic + "-input"))
                        .processValues(EventTransformer::new, Named.as(topic + "-event_transformer")));
    }

    /**
     * Transformer of Kafka Streams Records into {@link Event} instances.
     */
    class EventTransformer extends ContextualFixedKeyProcessor<UUID, SpecificRecord, Event<SpecificRecord>> {
        @Override
        public void process(FixedKeyRecord<UUID, SpecificRecord> message) {
            context().forward(
                    message.withValue(
                            Event.from(
                                    message.key(),
                                    message.value(),
                                    Instant.ofEpochMilli(message.timestamp()))));
        }
    }

    @Configuration
    @EnableKafkaStreams
    static class Config {
        private static final String SCHEMA_REGISTRY_URL_CONFIG = "schema.registry.url";

        private static final String KEY_SERIALIZER_CLASS = "org.apache.kafka.common.serialization.UUIDSerializer";
        private static final String VALUE_SERIALIZER_CLASS = "io.confluent.kafka.serializers.KafkaAvroSerializer";

        private static final String DEFAULT_VALUE_SERDE_CLASS = "io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde";
        private static final String VALUE_SUBJECT_NAME_STRATEGY_CONFIG = "value.subject.name.strategy";
        private static final String VALUE_SUBJECT_NAME_STRATEGY = "io.confluent.kafka.serializers.subject.RecordNameStrategy";

        @Bean
        ProducerFactory<?, ?> kafkaProducerFactory(KafkaProperties properties) {
            var props = properties.buildProducerProperties();

            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, KEY_SERIALIZER_CLASS);
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, VALUE_SERIALIZER_CLASS);
            props.put(VALUE_SUBJECT_NAME_STRATEGY_CONFIG, VALUE_SUBJECT_NAME_STRATEGY);

            var factory = new DefaultKafkaProducerFactory<>(props);

            var transactionIdPrefix = properties.getProducer().getTransactionIdPrefix();

            if (transactionIdPrefix != null) {
                factory.setTransactionIdPrefix(transactionIdPrefix);
            }

            return factory;
        }

        @Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
        KafkaStreamsConfiguration kStreamsConfig(
                @Value(value = "${kafka.streams.application.id}") String applicationId,
                @Value(value = "${spring.kafka.bootstrap-servers}") String bootstrapServers,
                @Value(value = "${spring.kafka.properties.schema.registry.url}") String schemaRegistryUrl) {
            var props = new HashMap<String, Object>();

            props.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
            props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.UUID().getClass().getName());
            props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, DEFAULT_VALUE_SERDE_CLASS);
            props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
            props.put(SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
            props.put(VALUE_SUBJECT_NAME_STRATEGY_CONFIG, VALUE_SUBJECT_NAME_STRATEGY);

            return new KafkaStreamsConfiguration(props);
        }
    }
}
