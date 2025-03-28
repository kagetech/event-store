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

package tech.kage.event.replicator.entity;

import static java.util.Comparator.comparing;
import static java.util.stream.Collectors.toCollection;
import static org.assertj.core.api.Assertions.assertThat;
import static tech.kage.event.EventStore.SOURCE_ID;
import static tech.kage.event.replicator.entity.EventReplicator.PROGRESS_TOPIC;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.IntStream;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.core.io.Resource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.ActiveProfiles;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

import io.micrometer.core.instrument.MeterRegistry;
import tech.kage.event.crypto.MetadataSerializer;

/**
 * Integration tests for {@link EventReplicatorWorker}.
 * 
 * @author Dariusz Szpakowski
 */
@SpringBootTest
@ActiveProfiles("test")
@Testcontainers
abstract class EventReplicatorWorkerIT<K> {
    @Autowired
    JdbcTemplate jdbcTemplate;

    @Autowired
    KafkaTemplate<byte[], byte[]> kafkaTemplate;

    @Autowired
    ConsumerFactory<byte[], byte[]> consumerFactory;

    @Autowired
    MeterRegistry meterRegistry;

    @Autowired
    Environment environment;

    @Value("${event.replicator.poll.max.rows:100}")
    private int maxPollRows;

    @Value("${event.replicator.event.schema:events}")
    private String eventSchema;

    private String topic;

    @Container
    @ServiceConnection
    static final PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:15-alpine");

    @Container
    @ServiceConnection
    static final KafkaContainer kafka = new KafkaContainer("apache/kafka-native:3.8.1");

    @Configuration
    @EnableAutoConfiguration
    static class TestConfiguration {
    }

    @BeforeEach
    void setUp(@Value("classpath:/test-data/events/ddl.sql") Resource ddl) throws IOException {
        topic = "test_" + System.currentTimeMillis() + "_events";

        jdbcTemplate.execute(
                ddl.getContentAsString(StandardCharsets.UTF_8)
                        .replace("<<topic_name>>", topic)
                        .replace("<<key_type>>", getKeyType()));
    }

    @Test
    void copiesEventsFromDatabaseToKafkaInSinglePoll() {
        // Given
        var lastId = 0;

        var worker = new EventReplicatorWorker(
                jdbcTemplate, kafkaTemplate, meterRegistry, environment, eventSchema, topic, lastId);

        var sourceEvents = IntStream
                .rangeClosed(1, maxPollRows - 1)
                .boxed()
                .map(this::testEvent)
                .toList();

        for (var event : sourceEvents) {
            insertEvent(topic, event);
        }

        // When
        worker.run();

        // Then
        var replicatedEvents = readRecordsFromKafka(topic);

        assertThat(replicatedEvents)
                .describedAs("replicated events")
                .zipSatisfy(sourceEvents, (replicatedEvent, sourceEvent) -> {
                    assertThat(replicatedEvent.key())
                            .describedAs("replicated event key")
                            .isEqualTo(keyToBytes(sourceEvent.key()));

                    assertThat(replicatedEvent.value())
                            .describedAs("replicated event data")
                            .isEqualTo(sourceEvent.data());

                    assertThat(replicatedEvent.timestamp())
                            .describedAs("replicated event timestamp")
                            .isEqualTo(sourceEvent.timestamp().toEpochMilli());

                    var expectedHeaderList = sourceEvent
                            .metadata()
                            .entrySet()
                            .stream()
                            .map(e -> new RecordHeader(e.getKey(), (byte[]) e.getValue()))
                            .map(Header.class::cast)
                            .collect(toCollection(ArrayList::new));

                    expectedHeaderList.add(new RecordHeader(SOURCE_ID, Long.toString(sourceEvent.id()).getBytes()));

                    var expectedHeaders = new RecordHeaders(
                            expectedHeaderList
                                    .stream()
                                    .sorted(comparing(Header::key))
                                    .toList());

                    assertThat(replicatedEvent.headers())
                            .describedAs("replicated event headers")
                            .isEqualTo(expectedHeaders);
                });
    }

    @Test
    void copiesEventsFromDatabaseToKafkaInMultiplePolls() {
        // Given
        var lastId = 0;

        var worker = new EventReplicatorWorker(
                jdbcTemplate, kafkaTemplate, meterRegistry, environment, eventSchema, topic, lastId);

        var sourceEvents = IntStream
                .rangeClosed(1, maxPollRows + 1)
                .boxed()
                .map(this::testEvent)
                .toList();

        for (var event : sourceEvents) {
            insertEvent(topic, event);
        }

        // When
        worker.run();

        // Then
        var replicatedEvents = readRecordsFromKafka(topic);

        assertThat(replicatedEvents)
                .describedAs("replicated events")
                .zipSatisfy(sourceEvents, (replicatedEvent, sourceEvent) -> {
                    assertThat(replicatedEvent.key())
                            .describedAs("replicated event key")
                            .isEqualTo(keyToBytes(sourceEvent.key()));

                    assertThat(replicatedEvent.value())
                            .describedAs("replicated event data")
                            .isEqualTo(sourceEvent.data());

                    assertThat(replicatedEvent.timestamp())
                            .describedAs("replicated event timestamp")
                            .isEqualTo(sourceEvent.timestamp().toEpochMilli());

                    var expectedHeaderList = sourceEvent
                            .metadata()
                            .entrySet()
                            .stream()
                            .map(e -> new RecordHeader(e.getKey(), (byte[]) e.getValue()))
                            .map(Header.class::cast)
                            .collect(toCollection(ArrayList::new));

                    expectedHeaderList.add(new RecordHeader(SOURCE_ID, Long.toString(sourceEvent.id()).getBytes()));

                    var expectedHeaders = new RecordHeaders(
                            expectedHeaderList
                                    .stream()
                                    .sorted(comparing(Header::key))
                                    .toList());

                    assertThat(replicatedEvent.headers())
                            .describedAs("replicated event headers")
                            .isEqualTo(expectedHeaders);
                });
    }

    @Test
    void resumesCopyingEventsFromGivenId() {
        // Given
        var lastId = 5;

        var worker = new EventReplicatorWorker(
                jdbcTemplate, kafkaTemplate, meterRegistry, environment, eventSchema, topic, lastId);

        var sourceEvents = IntStream
                .rangeClosed(1, 11)
                .boxed()
                .map(this::testEvent)
                .toList();

        for (var event : sourceEvents) {
            insertEvent(topic, event);
        }

        var expectedEvents = sourceEvents.subList(lastId, sourceEvents.size());

        // When
        worker.run();

        // Then
        var replicatedEvents = readRecordsFromKafka(topic);

        assertThat(replicatedEvents)
                .describedAs("replicated events")
                .zipSatisfy(expectedEvents, (replicatedEvent, sourceEvent) -> {
                    assertThat(replicatedEvent.key())
                            .describedAs("replicated event key")
                            .isEqualTo(keyToBytes(sourceEvent.key()));

                    assertThat(replicatedEvent.value())
                            .describedAs("replicated event data")
                            .isEqualTo(sourceEvent.data());

                    assertThat(replicatedEvent.timestamp())
                            .describedAs("replicated event timestamp")
                            .isEqualTo(sourceEvent.timestamp().toEpochMilli());

                    var expectedHeaderList = sourceEvent
                            .metadata()
                            .entrySet()
                            .stream()
                            .map(e -> new RecordHeader(e.getKey(), (byte[]) e.getValue()))
                            .map(Header.class::cast)
                            .collect(toCollection(ArrayList::new));

                    expectedHeaderList.add(new RecordHeader(SOURCE_ID, Long.toString(sourceEvent.id()).getBytes()));

                    var expectedHeaders = new RecordHeaders(
                            expectedHeaderList
                                    .stream()
                                    .sorted(comparing(Header::key))
                                    .toList());

                    assertThat(replicatedEvent.headers())
                            .describedAs("replicated event headers")
                            .isEqualTo(expectedHeaders);
                });
    }

    @Test
    void storesLastReplicatedEventId() {
        // Given
        var lastId = 8;
        var expectedLastId = 23;

        var worker = new EventReplicatorWorker(
                jdbcTemplate, kafkaTemplate, meterRegistry, environment, eventSchema, topic, lastId);

        var sourceEvents = IntStream
                .rangeClosed(1, expectedLastId)
                .boxed()
                .map(this::testEvent)
                .toList();

        for (var event : sourceEvents) {
            insertEvent(topic, event);
        }

        // When
        worker.run();

        // Then
        var storedLastId = 0l;

        var progressRecords = readRecordsFromKafka(PROGRESS_TOPIC);

        for (var consumerRecord : progressRecords) {
            if (topic.contains(stringFromBytes(consumerRecord.key()))) {
                storedLastId = longFromBytes(consumerRecord.value());
            }
        }

        assertThat(storedLastId)
                .describedAs("stored last id")
                .isEqualTo(expectedLastId);
    }

    @Test
    void monitorsReplicationLag() {
        // Given
        var lastId = 5;
        var lastSourceId = 13;
        var expectedReplicationLag = 8;

        var sourceEvents = IntStream
                .rangeClosed(1, lastSourceId)
                .boxed()
                .map(this::testEvent)
                .toList();

        for (var event : sourceEvents) {
            insertEvent(topic, event);
        }

        // When
        new EventReplicatorWorker(jdbcTemplate, kafkaTemplate, meterRegistry, environment, eventSchema, topic, lastId);

        // Then
        var replicationLag = meterRegistry.find("event.replicator.lag").tag("topic", topic).gauge().value();

        assertThat(replicationLag)
                .describedAs("replication lag")
                .isEqualTo(expectedReplicationLag);
    }

    private void insertEvent(String topic, EventData event) {
        jdbcTemplate
                .update(
                        "INSERT INTO events." + topic + " (key, data, metadata, timestamp) VALUES (?, ?, ?, ?)",
                        event.key(),
                        event.data(),
                        MetadataSerializer.serialize(event.metadata()),
                        event.timestamp().atOffset(ZoneOffset.UTC));
    }

    private ConsumerRecords<byte[], byte[]> readRecordsFromKafka(String topic) {
        try (var consumer = consumerFactory.createConsumer()) {
            var topicPartition = List.of(new TopicPartition(topic, 0));

            consumer.assign(topicPartition);
            consumer.seekToBeginning(topicPartition);

            return consumer.poll(Duration.ofSeconds(60));
        }
    }

    private String stringFromBytes(byte[] bytes) {
        return new String(bytes, StandardCharsets.UTF_8);
    }

    private long longFromBytes(byte[] bytes) {
        return ByteBuffer
                .allocate(Long.BYTES)
                .put(bytes)
                .flip()
                .getLong();
    }

    private EventData testEvent(int id) {
        return new EventData(
                id,
                getTestEventKey(id),
                ("test payload " + id).getBytes(),
                Map.of(
                        "dTest", "meta_value".getBytes(),
                        "zTest", UUID.randomUUID().toString().getBytes(),
                        "bTest", Long.toString(id).getBytes()),
                Instant.now());
    }

    protected abstract String getKeyType();

    protected abstract K getTestEventKey(int id);

    protected byte[] keyToBytes(Object key) {
        return key.toString().getBytes();
    }

    protected record EventData(long id, Object key, byte[] data, Map<String, Object> metadata, Instant timestamp) {
    }
}
