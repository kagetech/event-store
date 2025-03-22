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

import java.net.URI;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.processor.api.ContextualFixedKeyProcessor;
import org.apache.kafka.streams.processor.api.FixedKeyRecord;
import org.springframework.kafka.core.KafkaTemplate;
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
 * @param <K> the type of stored events' keys
 * @param <V> the type of stored events' payloads
 * 
 * @author Dariusz Szpakowski
 */
@Component
public class KafkaStreamsEventStore<K, V extends SpecificRecord> implements EventStore<K, V> {
    private final KafkaTemplate<Object, byte[]> kafkaTemplate;
    private final ProducerRecordEventTransformer producerRecordEventTransformer;
    private final KafkaStreamsEventTransformer<K, V> kafkaStreamsEventTransformer;
    private final StreamsBuilder streamsBuilder;

    private Map<String, KStream<K, Event<K, V>>> streams = new ConcurrentHashMap<>();

    /**
     * Constructs a new {@link KafkaStreamsEventStore} instance.
     *
     * @param kafkaTemplate                  an instance of {@link KafkaTemplate}
     * @param producerRecordEventTransformer an instance of
     *                                       {@link ProducerRecordEventTransformer}
     * @param kafkaStreamsEventTransformer   an instance of
     *                                       {@link KafkaStreamsEventTransformer}
     * @param streamsBuilder                 an instance of {@link StreamsBuilder}
     */
    KafkaStreamsEventStore(
            KafkaTemplate<Object, byte[]> kafkaTemplate,
            ProducerRecordEventTransformer producerRecordEventTransformer,
            KafkaStreamsEventTransformer<K, V> kafkaStreamsEventTransformer,
            StreamsBuilder streamsBuilder) {
        this.kafkaTemplate = kafkaTemplate;
        this.producerRecordEventTransformer = producerRecordEventTransformer;
        this.kafkaStreamsEventTransformer = kafkaStreamsEventTransformer;
        this.streamsBuilder = streamsBuilder;
    }

    @Override
    public Mono<Event<K, V>> save(String topic, Event<K, V> event) {
        return doSave(topic, event, null);
    }

    @Override
    public Mono<Event<K, V>> save(String topic, Event<K, V> event, URI encryptionKey) {
        Objects.requireNonNull(encryptionKey, "encryptionKey must not be null");

        return doSave(topic, event, encryptionKey);
    }

    private Mono<Event<K, V>> doSave(String topic, Event<K, V> event, URI encryptionKey) {
        Objects.requireNonNull(topic, "topic must not be null");
        Objects.requireNonNull(event, "event must not be null");

        return producerRecordEventTransformer
                .transform(event, topic, encryptionKey)
                .map(kafkaTemplate::send)
                .flatMap(Mono::fromFuture)
                .thenReturn(event);
    }

    /**
     * Subscribes to a stream of events in a given topic.
     * 
     * @param topic topic to subscribe to
     * 
     * @return {@link KStream} of events
     * 
     * @throws NullPointerException if the specified topic is null
     */
    public KStream<K, Event<K, V>> subscribe(String topic) {
        Objects.requireNonNull(topic, "topic must not be null");

        return streams.computeIfAbsent(
                topic,
                key -> streamsBuilder
                        .stream(topic, Consumed.<K, byte[]>as(topic + "-input").withValueSerde(Serdes.ByteArray()))
                        .processValues(InputEventTransformer::new, Named.as(topic + "-input_event_transformer")));
    }

    /**
     * Transformer of Kafka Streams Records into {@link Event} instances.
     */
    class InputEventTransformer extends ContextualFixedKeyProcessor<K, byte[], Event<K, V>> {
        @Override
        public void process(FixedKeyRecord<K, byte[]> message) {
            context().forward(
                    message.withValue(
                            kafkaStreamsEventTransformer.transform(message, context().recordMetadata())));
        }
    }

    /**
     * Transformer of {@link Event} instances into Kafka Streams Records.
     */
    public class OutputEventTransformer extends ContextualFixedKeyProcessor<K, Event<K, V>, byte[]> {
        protected final String topic;

        OutputEventTransformer(String topic) {
            this.topic = topic;
        }

        @Override
        public void process(FixedKeyRecord<K, Event<K, V>> message) {
            context().forward(kafkaStreamsEventTransformer.transform(message.value(), message, topic));
        }
    }

    /**
     * Transformer of {@link Event} instances into encrypted Kafka Streams Records.
     */
    public class EncryptingOutputEventTransformer extends OutputEventTransformer {
        EncryptingOutputEventTransformer(String topic) {
            super(topic);
        }

        @Override
        public void process(FixedKeyRecord<K, Event<K, V>> message) {
            var encryptionKeyBytes = Objects.requireNonNull(
                    (byte[]) message.value().metadata().get(ENCRYPTION_KEY_ID),
                    "encryptionKey must not be null");

            var encryptionKey = URI.create(new String(encryptionKeyBytes));

            context().forward(
                    kafkaStreamsEventTransformer.transform(message.value(), message, topic, encryptionKey));
        }
    }
}
