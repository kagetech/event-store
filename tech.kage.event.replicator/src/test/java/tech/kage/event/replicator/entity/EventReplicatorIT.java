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

import static java.util.stream.Collectors.toMap;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static tech.kage.event.replicator.entity.EventReplicator.PROGRESS_TOPIC;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;
import java.util.stream.LongStream;

import org.assertj.core.groups.Tuple;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.mockito.ArgumentCaptor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.core.env.Environment;
import org.springframework.core.io.Resource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

import io.micrometer.core.instrument.MeterRegistry;

/**
 * Integration tests for {@link EventReplicator}.
 * 
 * @author Dariusz Szpakowski
 */
@SpringBootTest
@ActiveProfiles("test")
@Testcontainers
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class EventReplicatorIT {
    // UUT
    @Autowired
    EventReplicator eventReplicator;

    @Autowired
    KafkaAdmin kafkaAdmin;

    @Autowired
    JdbcTemplate jdbcTemplate;

    @Autowired
    KafkaTemplate<String, byte[]> kafkaTemplate;

    @Autowired
    ConsumerFactory<String, byte[]> consumerFactory;

    @Autowired
    MeterRegistry meterRegistry;

    @Autowired
    Environment environment;

    @Value("${event.replicator.poll.interval.ms:1000}")
    int pollInterval;

    @MockitoBean
    TaskScheduler taskScheduler;

    Map<String, Long> topicLastIds;

    @Container
    @ServiceConnection
    static final PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:15-alpine");

    @Container
    @ServiceConnection
    static final KafkaContainer kafka = new KafkaContainer("apache/kafka-native:3.8.1");

    @Configuration
    @EnableScheduling
    @EnableAutoConfiguration
    @Import(EventReplicator.class)
    static class TestConfiguration {
    }

    @BeforeEach
    void setUp(@Value("classpath:/test-data/events/ddl.sql") Resource ddl) throws IOException {
        topicLastIds = LongStream
                .rangeClosed(1, 10)
                .boxed()
                .map(id -> Map.entry("test_" + id + System.currentTimeMillis() + "_events", id))
                .collect(toMap(Map.Entry::getKey, Map.Entry::getValue));

        // create source event tables
        for (var topic : topicLastIds.keySet()) {
            jdbcTemplate.execute(
                    ddl.getContentAsString(StandardCharsets.UTF_8)
                            .replace("<<topic_name>>", topic)
                            .replace("<<key_type>>", "uuid"));
        }
    }

    @AfterEach
    void tearDown() {
        // drop source event tables
        for (var topic : topicLastIds.keySet()) {
            jdbcTemplate.execute("drop table events." + topic);
        }
    }

    @Test
    @Order(1)
    void preparesReplicatedEventTopics() {
        // When
        eventReplicator.init(kafkaAdmin, jdbcTemplate, consumerFactory, meterRegistry, environment, pollInterval, true);

        // Then
        assertThatCode(() -> kafkaAdmin.describeTopics(topicLastIds.keySet().toArray(String[]::new)))
                .doesNotThrowAnyException();
    }

    @Test
    @Order(2)
    void schedulesEventReplicatorWorkers() {
        // Given
        // store initial progress
        kafkaTemplate.executeInTransaction(kafkaTransaction -> {
            for (var topicEntry : topicLastIds.entrySet()) {
                kafkaTransaction.send(PROGRESS_TOPIC, topicEntry.getKey(), longToBytes(topicEntry.getValue()));
            }

            return 0;
        });

        var expectedTopicLastIds = topicLastIds
                .entrySet()
                .stream()
                .map(topicEntry -> new Tuple(topicEntry.getKey(), topicEntry.getValue()))
                .toList();

        var expectedDelay = Duration.ofMillis(pollInterval);

        // When
        eventReplicator.init(kafkaAdmin, jdbcTemplate, consumerFactory, meterRegistry, environment, pollInterval, true);

        // Then
        var taskCaptor = ArgumentCaptor.forClass(Runnable.class);

        verify(taskScheduler, times(expectedTopicLastIds.size()))
                .scheduleWithFixedDelay(taskCaptor.capture(), eq(expectedDelay));

        assertThat(taskCaptor.getAllValues())
                .describedAs("scheduled tasks type")
                .hasOnlyElementsOfType(EventReplicatorWorker.class);

        assertThat(taskCaptor.getAllValues())
                .describedAs("scheduled tasks")
                .extracting("replicatedTopic", "lastId")
                .containsExactlyInAnyOrderElementsOf(expectedTopicLastIds);
    }

    @Test
    @Order(3)
    void throwsExceptionWhenProgressTopicIsFilled(
            @Value("${spring.kafka.consumer.max-poll-records}") int kafkaConsumerMaxPollRecords) {
        // Given
        // progress topic is filled
        kafkaTemplate.executeInTransaction(kafkaTransaction -> {
            for (var i = 0; i <= kafkaConsumerMaxPollRecords; i++) {
                kafkaTransaction.send(PROGRESS_TOPIC, "test_topic", longToBytes(0l));
            }

            return 0;
        });

        // When
        var thrown = Assertions.assertThrows(
                IllegalStateException.class,
                () -> eventReplicator.init(
                        kafkaAdmin, jdbcTemplate, consumerFactory, meterRegistry, environment, pollInterval, true));

        // Then
        assertThat(thrown.getMessage())
                .describedAs("thrown exception")
                .contains("kafkaConsumerMaxPollRecords");
    }

    private byte[] longToBytes(long val) {
        return ByteBuffer
                .allocate(Long.BYTES)
                .putLong(val)
                .array();
    }
}
