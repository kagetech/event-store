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

import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;

import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListOffsetsOptions;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.stereotype.Component;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.annotation.PreDestroy;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.MicrometerConsumerListener;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.receiver.ReceiverPartition;
import reactor.kafka.receiver.ReceiverRecord;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;
import reactor.kafka.sender.SenderRecord;
import tech.kage.event.Event;
import tech.kage.event.EventStore;

/**
 * A Kafka-based implementation of {@link EventStore} storing events in Kafka
 * topics and allowing subscribing to them. Stores offsets in a relational
 * database table. Implemented using Reactor Kafka. Uses Apache Avro for payload
 * serialization and stores Avro schemas in Confluent Schema Registry.
 * 
 * <p>
 * Requires the following configuration properties to be set (example values):
 * 
 * <ul>
 * <li>{@code spring.kafka.bootstrap-servers=localhost:9092}</li>
 * <li>{@code spring.kafka.properties.schema.registry.url=http://localhost:8989}</li>
 * </ul>
 * 
 * The value of {@code spring.kafka.bootstrap-servers} points to a Kafka cluster
 * and {@code spring.kafka.properties.schema.registry.url} points to a Confluent
 * Schema Registry instance.
 * 
 * @author Dariusz Szpakowski
 */
@Component
public class ReactorKafkaEventStore implements EventStore {
    private static final String SELECT_OFFSET_SQL = """
                SELECT "offset"
                FROM %s.topic_offsets
                WHERE topic = :topic AND partition = :partition
            """;

    private static final String INSERT_OFFSET_SQL = """
                INSERT INTO %s.topic_offsets (topic, "partition", "offset")
                VALUES (:topic, :partition, -1)
            """;

    private static final String UPDATE_OFFSET_SQL = """
                UPDATE %s.topic_offsets
                SET "offset" = :offset
                WHERE topic = :topic AND partition = :partition
            """;

    private static final String TOPIC_COLUMN = "topic";
    private static final String PARTITION_COLUMN = "partition";
    private static final String OFFSET_COLUMN = "offset";

    private static final String METADATA_PARTITION = "partition";
    private static final String METADATA_OFFSET = "offset";
    private static final String METADATA_HEADER_PREFIX = "header.";

    private static final String MICROMETER_LAG_GAUGE_NAME = "event.store.consumer.lag";
    private static final String MICROMETER_LAG_GAUGE_DESC = "The difference between the latest event in the source topic partition and the latest processed event from that topic partition";

    private static final String MICROMETER_TAG_TOPIC = "topic";
    private static final String MICROMETER_TAG_PARTITION = "partition";
    private static final String MICROMETER_TAG_GROUP_ID = "group.id";
    private static final String MICROMETER_TAG_CLIENT_ID = "client.id";

    private final KafkaSender<UUID, SpecificRecord> kafkaSender;
    private final ReceiverOptions<UUID, SpecificRecord> kafkaReceiverOptions;
    private final DatabaseClient databaseClient;
    private final Optional<MeterRegistry> meterRegistry;

    /**
     * Kafka admin client used for retrieving partition's latest offset. Used only
     * if {@code meterRegistry} is available.
     */
    private AdminClient adminClient;

    /**
     * Configuration property defining the name of the database schema with event
     * offset tables.
     */
    @Value("${event.schema:events}")
    private String eventSchema;

    /**
     * Map of latest processed partition offsets used for monitoring consumer lag.
     */
    private Map<TopicPartition, Long> lastOffsets = new ConcurrentHashMap<>();

    /**
     * Constructs a new {@link ReactorKafkaEventStore} instance.
     *
     * @param kafkaSender          an instance of {@link KafkaSender}
     * @param kafkaReceiverOptions an instance of {@link ReceiverOptions}
     * @param databaseClient       an instance of {@link DatabaseClient}
     * @param meterRegistry        optional instance of {@link MeterRegistry}
     */
    ReactorKafkaEventStore(
            KafkaSender<UUID, SpecificRecord> kafkaSender,
            ReceiverOptions<UUID, SpecificRecord> kafkaReceiverOptions,
            DatabaseClient databaseClient,
            Optional<MeterRegistry> meterRegistry) {
        this.kafkaSender = kafkaSender;
        this.kafkaReceiverOptions = kafkaReceiverOptions;
        this.databaseClient = databaseClient;
        this.meterRegistry = meterRegistry;
    }

    /**
     * Opens an admin client connection to Kafka if {@code meterRegistry} is
     * available.
     *
     * @param kafkaAdmin an instance of {@link KafkaAdmin}
     */
    @Autowired
    void init(KafkaAdmin kafkaAdmin) {
        if (meterRegistry.isPresent()) {
            adminClient = AdminClient.create(kafkaAdmin.getConfigurationProperties());
        }
    }

    /**
     * Close Kafka admin client if it was open.
     */
    @PreDestroy
    void destroy() {
        if (adminClient != null) {
            adminClient.close();
        }
    }

    @Override
    public <T extends SpecificRecord> Mono<Event<T>> save(String topic, Event<T> event) {
        Objects.requireNonNull(topic, "topic must not be null");
        Objects.requireNonNull(event, "event must not be null");

        return kafkaSender
                .send(Mono.just(
                        SenderRecord.create(
                                new ProducerRecord<>(
                                        topic,
                                        null,
                                        event.timestamp().toEpochMilli(),
                                        event.key(),
                                        event.payload(),
                                        event.metadata()
                                                .entrySet()
                                                .stream()
                                                .map(e -> new RecordHeader(e.getKey(), (byte[]) e.getValue()))
                                                .map(Header.class::cast)
                                                .toList()),
                                null)))
                .then(Mono.just(event));
    }

    @Override
    public <T extends SpecificRecord> Mono<Event<T>> save(String topic, Event<T> event, URI encryptionKey) {
        throw new UnsupportedOperationException("Unimplemented method 'save'");
    }

    /**
     * Subscribes to a stream of events in a given topic. Stores offsets in a
     * relational database table.
     * 
     * <p>
     * <strong>Intended usage:</strong>
     * 
     * <pre>
     * eventStore
     *         .subscribe("test_events")
     *         .concatMap(event -> event.flatMap(this::processEvent).as(transactionalOperator::transactional))
     * </pre>
     * 
     * @param topic topic to subscribe to
     * 
     * @return {@link Flux} of events concatenated with offset saving {@link Mono}
     */
    public Flux<Mono<Event<SpecificRecord>>> subscribe(String topic) {
        Objects.requireNonNull(topic, "topic must not be null");

        return KafkaReceiver
                .create(
                        kafkaReceiverOptions
                                .consumerProperty(ConsumerConfig.CLIENT_ID_CONFIG, clientId(topic))
                                .commitInterval(Duration.ZERO)
                                .commitBatchSize(0)
                                .addAssignListener(partitions -> handlePartitionsAssignment(topic, partitions))
                                .subscription(List.of(topic)))
                .receive()
                .map(this::consume);
    }

    /**
     * Handles partition assignment by seeking the last processed offset and
     * configuring Micrometer consumer lag metric.
     * 
     * @param topic              topic which owns the assigned partitions
     * @param receiverPartitions list of assigned partitions
     */
    private void handlePartitionsAssignment(String topic, Collection<ReceiverPartition> receiverPartitions) {
        if (meterRegistry.isPresent()) {
            var outdatedGauges = meterRegistry
                    .get()
                    .find(MICROMETER_LAG_GAUGE_NAME)
                    .tag(MICROMETER_TAG_TOPIC, topic)
                    .gauges();

            for (var gauge : outdatedGauges) {
                meterRegistry.get().remove(gauge.getId());
            }
        }

        lastOffsets.entrySet().removeIf(entry -> entry.getKey().topic().equals(topic));

        for (var receiverPartition : receiverPartitions) {
            var topicPartition = receiverPartition.topicPartition();
            var partition = topicPartition.partition();

            var lastOffset = getLastOffset(topicPartition);

            lastOffsets.put(topicPartition, lastOffset);

            receiverPartition.seek(lastOffset + 1);

            if (meterRegistry.isPresent()) {
                Gauge
                        .builder(MICROMETER_LAG_GAUGE_NAME, this, consumer -> consumer.computeLag(topicPartition))
                        .description(MICROMETER_LAG_GAUGE_DESC)
                        .tags(MICROMETER_TAG_TOPIC, topic)
                        .tags(MICROMETER_TAG_PARTITION, Integer.toString(partition))
                        .tags(MICROMETER_TAG_GROUP_ID, kafkaReceiverOptions.groupId())
                        .tags(MICROMETER_TAG_CLIENT_ID, clientId(topic))
                        .register(meterRegistry.get());
            }
        }
    }

    /**
     * Retrieves the last offset for a given Kafka topic partition.
     * 
     * @param topicPartition topic partition for which the last offset will be
     *                       retrieved
     * 
     * @return Kafka topic partition offset of the last processed event
     */
    private long getLastOffset(TopicPartition topicPartition) {
        return databaseClient
                .sql(SELECT_OFFSET_SQL.formatted(eventSchema))
                .bind(TOPIC_COLUMN, topicPartition.topic())
                .bind(PARTITION_COLUMN, topicPartition.partition())
                .map(r -> (Long) r.get(OFFSET_COLUMN))
                .one()
                .switchIfEmpty(Mono.defer(() -> initializeOffsetInfo(topicPartition)))
                .block();
    }

    /**
     * Initializes offset information for a given topic partition.
     * 
     * @param topicPartition topic partition for which the offset information is
     *                       initialized
     * 
     * @return -1 as the current value of the last offset for a given topic
     *         partition
     */
    private Mono<Long> initializeOffsetInfo(TopicPartition topicPartition) {
        return databaseClient
                .sql(INSERT_OFFSET_SQL.formatted(eventSchema))
                .bind(TOPIC_COLUMN, topicPartition.topic())
                .bind(PARTITION_COLUMN, topicPartition.partition())
                .fetch()
                .rowsUpdated()
                .thenReturn(-1l);
    }

    /**
     * Consumes a given event by saving its offset.
     * 
     * @param event event to consume
     * 
     * @return consumed event
     */
    private Mono<Event<SpecificRecord>> consume(ReceiverRecord<UUID, SpecificRecord> event) {
        return saveOffset(event).map(ReactorKafkaEventStore::transform);
    }

    /**
     * Saves offset for a given event.
     * 
     * @param event event whose offset is saved
     * 
     * @return passed event
     */
    private Mono<ReceiverRecord<UUID, SpecificRecord>> saveOffset(ReceiverRecord<UUID, SpecificRecord> event) {
        return databaseClient
                .sql(UPDATE_OFFSET_SQL.formatted(eventSchema))
                .bind(TOPIC_COLUMN, event.topic())
                .bind(PARTITION_COLUMN, event.partition())
                .bind(OFFSET_COLUMN, event.offset())
                .fetch()
                .rowsUpdated()
                .doOnNext(rowsUpdated -> updateLastOffset(event))
                .thenReturn(event);
    }

    private void updateLastOffset(ReceiverRecord<UUID, SpecificRecord> event) {
        lastOffsets.put(new TopicPartition(event.topic(), event.partition()), event.offset());
    }

    private String clientId(String topic) {
        return "consumer-" + topic;
    }

    static Event<SpecificRecord> transform(ReceiverRecord<UUID, SpecificRecord> event) {
        return Event.from(event.key(), event.value(), timestamp(event), metadata(event));
    }

    private static Instant timestamp(ReceiverRecord<UUID, SpecificRecord> event) {
        return Instant.ofEpochMilli(event.timestamp());
    }

    private static Map<String, Object> metadata(ReceiverRecord<UUID, SpecificRecord> event) {
        var metadataMap = new HashMap<String, Object>();

        metadataMap.put(METADATA_PARTITION, event.partition());
        metadataMap.put(METADATA_OFFSET, event.offset());

        for (var header : event.headers()) {
            metadataMap.put(METADATA_HEADER_PREFIX + header.key(), header.value());
        }

        return Collections.unmodifiableMap(metadataMap);
    }

    /**
     * Computes the consumer lag, i.e. the difference between the latest event in
     * the source topic partition and the latest processed event from that topic
     * partition.
     * 
     * @param topicPartition topic partition for which the consumer lag will be
     *                       computed
     * 
     * @return computed consumer lag
     */
    private double computeLag(TopicPartition topicPartition) {
        try {
            var topicLatestOffset = adminClient
                    .listOffsets(
                            Map.of(topicPartition, OffsetSpec.latest()),
                            new ListOffsetsOptions(IsolationLevel.READ_COMMITTED))
                    .partitionResult(topicPartition)
                    .get()
                    .offset();

            // subtract 2 (one for commit record and one because the latest offset indicates
            // the next offset)

            return topicLatestOffset - lastOffsets.get(topicPartition) - 2;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();

            throw new IllegalStateException("Unable to compute consumer lag for " + topicPartition, e);
        } catch (ExecutionException e) {
            throw new IllegalStateException("Unable to compute consumer lag for " + topicPartition, e);
        }
    }

    @Configuration
    static class Config {
        private static final String KEY_SERIALIZER_CLASS = "org.apache.kafka.common.serialization.UUIDSerializer";
        private static final String VALUE_SERIALIZER_CLASS = "io.confluent.kafka.serializers.KafkaAvroSerializer";

        private static final String KEY_DESERIALIZER_CLASS = "org.apache.kafka.common.serialization.UUIDDeserializer";
        private static final String VALUE_DESERIALIZER_CLASS = "io.confluent.kafka.serializers.KafkaAvroDeserializer";

        private static final String VALUE_SUBJECT_NAME_STRATEGY_CONFIG = "value.subject.name.strategy";
        private static final String VALUE_SUBJECT_NAME_STRATEGY = "io.confluent.kafka.serializers.subject.RecordNameStrategy";

        private static final String SPECIFIC_AVRO_READER_CONFIG = "specific.avro.reader";

        @Bean
        KafkaSender<UUID, SpecificRecord> kafkaSender(SenderOptions<UUID, SpecificRecord> senderOptions) {
            return KafkaSender.create(senderOptions);
        }

        @Bean
        SenderOptions<UUID, SpecificRecord> kafkaSenderOptions(KafkaProperties properties) {
            var props = properties.buildProducerProperties(null);

            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, KEY_SERIALIZER_CLASS);
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, VALUE_SERIALIZER_CLASS);
            props.putIfAbsent(VALUE_SUBJECT_NAME_STRATEGY_CONFIG, VALUE_SUBJECT_NAME_STRATEGY);

            return SenderOptions.create(props);
        }

        @Bean
        ReceiverOptions<UUID, SpecificRecord> kafkaReceiverOptions(
                KafkaProperties properties,
                Optional<MeterRegistry> meterRegistry) {
            var props = properties.buildConsumerProperties(null);

            props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
            props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
            props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, KEY_DESERIALIZER_CLASS);
            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, VALUE_DESERIALIZER_CLASS);
            props.putIfAbsent(SPECIFIC_AVRO_READER_CONFIG, "true");
            props.putIfAbsent(VALUE_SUBJECT_NAME_STRATEGY_CONFIG, VALUE_SUBJECT_NAME_STRATEGY);

            return ReceiverOptions
                    .<UUID, SpecificRecord>create(props)
                    .consumerListener(
                            meterRegistry.isPresent()
                                    ? new MicrometerConsumerListener(meterRegistry.get())
                                    : null);
        }
    }
}
