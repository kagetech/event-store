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

import static java.util.stream.Collectors.toList;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.TopicConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.env.Environment;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.stereotype.Component;

/**
 * Component responsible for coordination of event replication process. Uses
 * event replication workers ({@link EventReplicatorWorker}) to replicate single
 * topics.
 * 
 * @author Dariusz Szpakowski
 */
@Component
class EventReplicator {
    private static final Logger log = LoggerFactory.getLogger(EventReplicator.class);

    /**
     * SQL query used for reading the names of available event tables.
     */
    private static final String SELECT_TOPICS_SQL = "SELECT tablename FROM pg_tables WHERE schemaname = '%s'";

    /**
     * The suffix that all event tables need to have to be considered for
     * replication.
     */
    private static final String TOPIC_SUFFIX = "_events";

    /**
     * The name of Kafka topic used for storing replication progress.
     */
    static final String PROGRESS_TOPIC = "_event_replicator_progress";

    /**
     * The name of NOOP message key. NOOP messages are used to ensure that
     * {@code PROGRESS_TOPIC} is not empty.
     */
    private static final String NOOP_KEY = "noop";

    private final KafkaTemplate<String, byte[]> kafkaTemplate;
    private final TaskScheduler taskScheduler;

    /**
     * Configuration property defining the name of the database schema with event
     * tables.
     */
    @Value("${event.replicator.event.schema:events}")
    private String eventSchema;

    /**
     * Configuration property defining the maximum number of records polled from
     * Kafka {@code PROGRESS_TOPIC}. This must be set to a value high enough to
     * allow reading all records in one poll.
     */
    @Value("${spring.kafka.consumer.max-poll-records}")
    private int kafkaConsumerMaxPollRecords;

    /**
     * Constructs a new {@link EventReplicator} instance.
     *
     * @param kafkaTemplate an instance of {@link KafkaTemplate}
     * @param taskScheduler an instance of {@link TaskScheduler}
     */
    EventReplicator(KafkaTemplate<String, byte[]> kafkaTemplate, TaskScheduler taskScheduler) {
        this.kafkaTemplate = kafkaTemplate;
        this.taskScheduler = taskScheduler;
    }

    /**
     * Initializes the process of event replication and schedules event replicator
     * workers.
     *
     * @param kafkaAdmin      an instance of {@link KafkaAdmin}
     * @param jdbcTemplate    an instance of {@link JdbcTemplate}
     * @param consumerFactory an instance of {@link ConsumerFactory}
     * @param environment     an instance of {@link Environment}
     * @param pollInterval    configuration property defining polling interval
     */
    @Autowired
    void init(
            KafkaAdmin kafkaAdmin,
            JdbcTemplate jdbcTemplate,
            ConsumerFactory<String, byte[]> consumerFactory,
            Environment environment,
            @Value("${event.replicator.poll.interval.ms:1000}") int pollInterval,
            @Value("${event.replicator.autostart:true}") boolean autostart) {
        // check if autostart is disabled
        if (!autostart) {
            return;
        }

        // create progress Kafka topic
        kafkaAdmin.createOrModifyTopics(TopicBuilder.name(PROGRESS_TOPIC).partitions(1).compact().build());

        log.info("Reading last replicated event id");

        // read last replicated event ids (map topic->id)
        var lastIds = readLastIds(getReplicatedTopics(jdbcTemplate), consumerFactory);

        // for each replicated topic
        for (var lastId : lastIds.entrySet()) {
            // create replicated Kafka topic
            kafkaAdmin.createOrModifyTopics(
                    TopicBuilder.name(lastId.getKey()).config(TopicConfig.RETENTION_MS_CONFIG, "-1").build());

            // schedule a new worker for the replicated topic
            taskScheduler.scheduleWithFixedDelay(
                    new EventReplicatorWorker(
                            jdbcTemplate,
                            kafkaTemplate,
                            environment,
                            eventSchema,
                            lastId.getKey(),
                            lastId.getValue()),
                    Duration.ofMillis(pollInterval));
        }

        log.info("Processing events...");
    }

    /**
     * Reads a list of replicated topics by reading a list of tables with
     * {@code TOPIC_SUFFIX} suffix in the {@code eventSchema} database schema.
     * 
     * @param jdbcTemplate an instance of {@link JdbcTemplate}
     * 
     * @return list of replicated topics
     */
    private List<String> getReplicatedTopics(JdbcTemplate jdbcTemplate) {
        return jdbcTemplate
                .queryForList(SELECT_TOPICS_SQL.formatted(eventSchema))
                .stream()
                .map(topic -> (String) topic.get("tablename"))
                .filter(topic -> topic.endsWith(TOPIC_SUFFIX))
                .collect(toList());
    }

    /**
     * Reads a list of replicated topics by reading a list of tables with
     * {@code TOPIC_SUFFIX} suffix in the {@code eventSchema} database schema.
     * 
     * @param replicatedTopics a list of replicated topics
     * @param consumerFactory  an instance of {@link ConsumerFactory}
     * 
     * @return map of topics with the ids of the last replicated events
     */
    private Map<String, Long> readLastIds(
            List<String> replicatedTopics,
            ConsumerFactory<String, byte[]> consumerFactory) {
        var lastIds = new HashMap<String, Long>();

        for (var topic : replicatedTopics) {
            lastIds.put(topic, 0l);
        }

        var topicPartition = List.of(new TopicPartition(PROGRESS_TOPIC, 0));

        // send noop so that we know there is something in PROGRESS_TOPIC
        kafkaTemplate.executeInTransaction(kafka -> kafka.send(PROGRESS_TOPIC, NOOP_KEY, null));

        try (var consumer = consumerFactory.createConsumer()) {
            consumer.assign(topicPartition);
            consumer.seekToBeginning(topicPartition);

            // wait as long as needed, all data must fit in one poll
            var consumerRecords = consumer.poll(Duration.ofMillis(Long.MAX_VALUE));

            var consumerRecordsCount = 0;

            for (var consumerRecord : consumerRecords) {
                consumerRecordsCount++;

                if (replicatedTopics.contains(consumerRecord.key())) {
                    lastIds.put(consumerRecord.key(), longFromBytes(consumerRecord.value()));
                }
            }

            if (consumerRecordsCount >= kafkaConsumerMaxPollRecords) {
                throw new IllegalStateException(
                        "consumerRecordsCount: %d >= kafkaConsumerMaxPollRecords: %d"
                                .formatted(consumerRecordsCount, kafkaConsumerMaxPollRecords));
            }
        }

        return lastIds;
    }

    private long longFromBytes(byte[] bytes) {
        return ByteBuffer
                .allocate(Long.BYTES)
                .put(bytes)
                .flip()
                .getLong();
    }
}
