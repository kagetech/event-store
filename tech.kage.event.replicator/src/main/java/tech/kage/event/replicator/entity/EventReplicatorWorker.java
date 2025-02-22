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
import static tech.kage.event.EventStore.SOURCE_ID;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.springframework.core.env.Environment;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import tech.kage.event.crypto.MetadataSerializer;

/**
 * Worker component responsible for replicating events from a single event table
 * to a single Kafka topic.
 * 
 * @author Dariusz Szpakowski
 */
class EventReplicatorWorker implements Runnable {
    /**
     * SQL query used for selecting the events for replication.
     */
    private static final String SELECT_EVENT_SQL = """
                SELECT *
                FROM %s.%s
                WHERE id > %s
                ORDER BY id
                LIMIT %s
            """;

    /**
     * SQL query used for selecting the id of the last event.
     */
    private static final String SELECT_LAST_EVENT_ID_SQL = "SELECT last_value FROM %s.%s_id_seq";

    /**
     * Configuration property defining the maximum number of events replicated in
     * one batch.
     */
    private static final String MAX_ROWS_PROPERTY = "event.replicator.poll.max.rows";

    /**
     * The name of Micrometer event replication lag gauge.
     */
    private static final String MICROMETER_LAG_GAUGE_NAME = "event.replicator.lag";

    /**
     * The description of Micrometer event replication lag gauge.
     */
    private static final String MICROMETER_LAG_GAUGE_DESC = "The difference between the latest event in the source topic and the latest replicated event";

    /**
     * The name of Micrometer topic tag.
     */
    private static final String MICROMETER_TAG_TOPIC = "topic";

    private final JdbcTemplate jdbcTemplate;
    private final KafkaTemplate<byte[], byte[]> kafkaTemplate;

    private final String eventSchema;
    private final String replicatedTopic;
    private final int maxRows;

    private long lastId;

    /**
     * Constructs a new {@link EventReplicatorWorker} instance.
     *
     * @param jdbcTemplate    an instance of {@link JdbcTemplate}
     * @param kafkaTemplate   an instance of {@link KafkaTemplate}
     * @param meterRegistry   an instance of {@link MeterRegistry}
     * @param environment     an instance of {@link Environment}
     * @param eventSchema     the name of the event schema
     * @param replicatedTopic the name of the replicated topic
     * @param lastId          the id of the last replicated event in the replicated
     *                        topic
     */
    EventReplicatorWorker(
            JdbcTemplate jdbcTemplate,
            KafkaTemplate<byte[], byte[]> kafkaTemplate,
            MeterRegistry meterRegistry,
            Environment environment,
            String eventSchema,
            String replicatedTopic,
            long lastId) {
        this.jdbcTemplate = jdbcTemplate;
        this.kafkaTemplate = kafkaTemplate;

        this.eventSchema = eventSchema;
        this.replicatedTopic = replicatedTopic;
        this.maxRows = environment.getProperty(MAX_ROWS_PROPERTY, Integer.class, 100);

        this.lastId = lastId;

        Gauge
                .builder(MICROMETER_LAG_GAUGE_NAME, this, worker -> worker.computeLag())
                .description(MICROMETER_LAG_GAUGE_DESC)
                .tags(MICROMETER_TAG_TOPIC, replicatedTopic)
                .register(meterRegistry);
    }

    /**
     * Replicate any outstanding events and quit once done.
     */
    @Override
    public void run() {
        while (true) {
            var updatedLastId = pollAndSendBatch(eventSchema, replicatedTopic, lastId, maxRows);

            if (updatedLastId != null) {
                lastId = updatedLastId;
            } else {
                // no more events found so we are done
                return;
            }
        }
    }

    /**
     * Polls the event table and sends read events to the Kafka topic with the same
     * name.
     * 
     * @param schema    the name of the event schema
     * @param topic     the name of the replicated topic
     * @param lastId    the id of the last replicated event in the replicated
     *                  topic
     * @param batchSize maximum number of events replicated in one batch.
     * 
     * @return id of the last replicated event or null if no events were found
     */
    private Long pollAndSendBatch(String schema, String topic, long lastId, int batchSize) {
        // select the events for replication
        var eventList = jdbcTemplate.queryForList(SELECT_EVENT_SQL.formatted(schema, topic, lastId, batchSize));

        if (eventList.isEmpty()) {
            return null;
        }

        // send events to Kafka and update progress topic in one transaction
        return kafkaTemplate.executeInTransaction(kafka -> {
            Long newLastId = null;

            for (var event : eventList) {
                newLastId = (Long) event.get("id");

                var key = event.get("key");
                var data = (byte[]) event.get("data");
                var metadata = (byte[]) event.get("metadata");
                var timestamp = (Timestamp) event.get("timestamp");
                var headers = toHeaders(newLastId, metadata);

                // send event to Kafka topic
                kafka.send(new ProducerRecord<>(topic, null, timestamp.getTime(), serializeKey(key), data, headers));
            }

            // update progress topic
            kafka.send(EventReplicator.PROGRESS_TOPIC, stringToBytes(replicatedTopic), longToBytes(newLastId));

            return newLastId;
        });
    }

    private byte[] serializeKey(Object key) {
        if (key instanceof byte[] keyBytes) {
            return keyBytes;
        }

        return stringToBytes(key.toString());
    }

    private List<Header> toHeaders(Long id, byte[] metadata) {
        return Stream
                .concat(
                        Stream.of(Map.entry(SOURCE_ID, Long.toString(id).getBytes())),
                        MetadataSerializer.deserialize(metadata).entrySet().stream())
                .map(e -> new RecordHeader(e.getKey().toString(), (byte[]) e.getValue()))
                .map(Header.class::cast)
                .sorted(comparing(Header::key)) // sort again after adding the id header
                .toList();
    }

    private byte[] stringToBytes(String str) {
        return str.getBytes(StandardCharsets.UTF_8);
    }

    private byte[] longToBytes(long val) {
        return ByteBuffer
                .allocate(Long.BYTES)
                .putLong(val)
                .array();
    }

    /**
     * Computes the event replication lag, i.e. the difference between the latest
     * event in the source topic and the latest replicated event.
     * 
     * @return computed replication lag
     */
    private double computeLag() {
        var lastSourceId = jdbcTemplate.queryForObject(
                SELECT_LAST_EVENT_ID_SQL.formatted(eventSchema, replicatedTopic),
                Long.class);

        return lastSourceId - lastId;
    }
}
