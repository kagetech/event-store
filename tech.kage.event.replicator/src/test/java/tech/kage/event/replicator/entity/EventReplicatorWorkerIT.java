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

package tech.kage.event.replicator.entity;

import static org.assertj.core.api.Assertions.assertThat;
import static tech.kage.event.replicator.entity.EventReplicator.PROGRESS_TOPIC;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.UUID;
import java.util.stream.IntStream;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.core.io.Resource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.util.FileCopyUtils;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

/**
 * Integration tests for {@link EventReplicatorWorker}.
 * 
 * @author Dariusz Szpakowski
 */
@SpringBootTest
@ActiveProfiles("test")
@Testcontainers
class EventReplicatorWorkerIT {
    @Autowired
    JdbcTemplate jdbcTemplate;

    @Autowired
    KafkaTemplate<String, byte[]> kafkaTemplate;

    @Autowired
    ConsumerFactory<String, byte[]> consumerFactory;

    @Autowired
    Environment environment;

    @Value("${event.replicator.poll.max.rows:100}")
    private int maxPollRows;

    @Value("${event.replicator.event.schema:events}")
    private String eventSchema;

    private String topic;

    @Container
    static final PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:15-alpine");

    @SuppressWarnings("resource")
    @Container
    static final KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.1"))
            .withKraft();

    @DynamicPropertySource
    static void overrideProperties(DynamicPropertyRegistry registry) {
        registry.add(
                "spring.datasource.url",
                () -> "jdbc:postgresql://%s:%s/%s".formatted(postgres.getHost(), postgres.getFirstMappedPort(),
                        postgres.getDatabaseName()));
        registry.add("spring.datasource.username", () -> postgres.getUsername());
        registry.add("spring.datasource.password", () -> postgres.getPassword());

        registry.add("spring.kafka.bootstrap-servers", () -> kafka.getBootstrapServers());
    }

    @Configuration
    @EnableAutoConfiguration
    static class TestConfiguration {
    }

    @BeforeEach
    void setUp(@Value("classpath:/test-data/events/ddl.sql") Resource ddl) {
        topic = "test_" + System.currentTimeMillis() + "_events";

        jdbcTemplate.execute(getContentAsString(ddl).replace("events.test_events", "events." + topic));
    }

    private String getContentAsString(Resource resource) {
        try (Reader reader = new InputStreamReader(resource.getInputStream(), StandardCharsets.UTF_8)) {
            return FileCopyUtils.copyToString(reader);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Test
    void copiesEventsFromDatabaseToKafkaInSinglePoll() {
        // Given
        var lastId = 0;

        var worker = new EventReplicatorWorker(jdbcTemplate, kafkaTemplate, environment, eventSchema, topic, lastId);

        var sourceEvents = IntStream
                .rangeClosed(1, maxPollRows - 1)
                .boxed()
                .map(id -> new EventData(id, UUID.randomUUID(), ("test payload " + id).getBytes(), Instant.now()))
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
                            .isEqualTo(sourceEvent.key().toString());

                    assertThat(replicatedEvent.value())
                            .describedAs("replicated event data")
                            .isEqualTo(sourceEvent.data());

                    assertThat(replicatedEvent.timestamp())
                            .describedAs("replicated event timestamp")
                            .isEqualTo(sourceEvent.timestamp().toEpochMilli());
                });
    }

    @Test
    void copiesEventsFromDatabaseToKafkaInMultiplePolls() {
        // Given
        var lastId = 0;

        var worker = new EventReplicatorWorker(jdbcTemplate, kafkaTemplate, environment, eventSchema, topic, lastId);

        var sourceEvents = IntStream
                .rangeClosed(1, maxPollRows + 1)
                .boxed()
                .map(id -> new EventData(id, UUID.randomUUID(), ("test payload " + id).getBytes(), Instant.now()))
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
                            .isEqualTo(sourceEvent.key().toString());

                    assertThat(replicatedEvent.value())
                            .describedAs("replicated event data")
                            .isEqualTo(sourceEvent.data());

                    assertThat(replicatedEvent.timestamp())
                            .describedAs("replicated event timestamp")
                            .isEqualTo(sourceEvent.timestamp().toEpochMilli());
                });
    }

    @Test
    void resumesCopyingEventsFromGivenId() {
        // Given
        var lastId = 5;

        var worker = new EventReplicatorWorker(jdbcTemplate, kafkaTemplate, environment, eventSchema, topic, lastId);

        var sourceEvents = IntStream
                .rangeClosed(1, 11)
                .boxed()
                .map(id -> new EventData(id, UUID.randomUUID(), ("test payload " + id).getBytes(), Instant.now()))
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
                            .isEqualTo(sourceEvent.key().toString());

                    assertThat(replicatedEvent.value())
                            .describedAs("replicated event data")
                            .isEqualTo(sourceEvent.data());

                    assertThat(replicatedEvent.timestamp())
                            .describedAs("replicated event timestamp")
                            .isEqualTo(sourceEvent.timestamp().toEpochMilli());
                });
    }

    @Test
    void storesLastReplicatedEventId() {
        // Given
        var lastId = 8;
        var expectedLastId = 23;

        var worker = new EventReplicatorWorker(jdbcTemplate, kafkaTemplate, environment, eventSchema, topic, lastId);

        var sourceEvents = IntStream
                .rangeClosed(1, expectedLastId)
                .boxed()
                .map(id -> new EventData(id, UUID.randomUUID(), ("test payload " + id).getBytes(), Instant.now()))
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
            if (topic.contains(consumerRecord.key())) {
                storedLastId = longFromBytes(consumerRecord.value());
            }
        }

        assertThat(storedLastId)
                .describedAs("stored last id")
                .isEqualTo(expectedLastId);
    }

    private void insertEvent(String topic, EventData event) {
        jdbcTemplate
                .update(
                        "INSERT INTO events." + topic + " (id, key, data, timestamp) VALUES (?, ?, ?, ?)",
                        event.id(), event.key(), event.data(), event.timestamp().atOffset(ZoneOffset.UTC));
    }

    private ConsumerRecords<String, byte[]> readRecordsFromKafka(String topic) {
        try (var consumer = consumerFactory.createConsumer()) {
            var topicPartition = List.of(new TopicPartition(topic, 0));

            consumer.assign(topicPartition);
            consumer.seekToBeginning(topicPartition);

            return consumer.poll(Duration.ofSeconds(60));
        }
    }

    private long longFromBytes(byte[] bytes) {
        return ByteBuffer
                .allocate(Long.BYTES)
                .put(bytes)
                .flip()
                .getLong();
    }

    private record EventData(long id, UUID key, byte[] data, Instant timestamp) {
    }
}
