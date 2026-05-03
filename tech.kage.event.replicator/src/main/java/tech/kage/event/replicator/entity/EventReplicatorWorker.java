/*
 * Copyright (c) 2023-2026, Dariusz Szpakowski
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
import static tech.kage.event.EventStore.SOURCE_LSN;

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
 * <p>
 * The replication cursor is a {@code (lsn, id)} pair compared via Postgres
 * row-value semantics. The {@code id} component is required because rows
 * inserted in a single transaction share the same commit LSN; an LSN-only
 * cursor would silently skip same-LSN siblings whenever a transaction's row
 * count crosses the batch {@code LIMIT}.
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
                WHERE lsn IS NOT NULL AND (lsn, id) > ('%s'::pg_lsn, %d)
                ORDER BY lsn, id
                LIMIT %s
            """;

    /**
     * SQL query used for computing the replication lag as WAL byte distance.
     */
    private static final String SELECT_LAG_SQL = "SELECT MAX(lsn) - '%s'::pg_lsn FROM %s.%s WHERE lsn IS NOT NULL";

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
    private static final String MICROMETER_LAG_GAUGE_DESC = "The WAL byte distance between the latest event in the source topic and the latest replicated event";

    /**
     * The name of Micrometer topic tag.
     */
    private static final String MICROMETER_TAG_TOPIC = "topic";

    private final JdbcTemplate jdbcTemplate;
    private final KafkaTemplate<byte[], byte[]> kafkaTemplate;

    private final String eventSchema;
    private final String replicatedTopic;
    private final int maxRows;

    private Cursor lastCursor;

    /**
     * Constructs a new {@link EventReplicatorWorker} instance.
     *
     * @param jdbcTemplate    an instance of {@link JdbcTemplate}
     * @param kafkaTemplate   an instance of {@link KafkaTemplate}
     * @param meterRegistry   an instance of {@link MeterRegistry}
     * @param environment     an instance of {@link Environment}
     * @param eventSchema     the name of the event schema
     * @param replicatedTopic the name of the replicated topic
     * @param lastCursor      the cursor of the last replicated event in the
     *                        replicated topic
     */
    EventReplicatorWorker(
            JdbcTemplate jdbcTemplate,
            KafkaTemplate<byte[], byte[]> kafkaTemplate,
            MeterRegistry meterRegistry,
            Environment environment,
            String eventSchema,
            String replicatedTopic,
            Cursor lastCursor) {
        this.jdbcTemplate = jdbcTemplate;
        this.kafkaTemplate = kafkaTemplate;

        this.eventSchema = eventSchema;
        this.replicatedTopic = replicatedTopic;
        this.maxRows = environment.getProperty(MAX_ROWS_PROPERTY, Integer.class, 100);

        this.lastCursor = lastCursor;

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
            var updatedLastCursor = pollAndSendBatch(eventSchema, replicatedTopic, lastCursor, maxRows);

            if (updatedLastCursor != null) {
                lastCursor = updatedLastCursor;
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
     * @param schema     the name of the event schema
     * @param topic      the name of the replicated topic
     * @param lastCursor the cursor of the last replicated event in the replicated
     *                   topic
     * @param batchSize  maximum number of events replicated in one batch.
     * 
     * @return cursor of the last replicated event or null if no events were found
     */
    private Cursor pollAndSendBatch(String schema, String topic, Cursor lastCursor, int batchSize) {
        // select the events for replication
        var eventList = jdbcTemplate.queryForList(
                SELECT_EVENT_SQL.formatted(schema, topic, lastCursor.lsn(), lastCursor.id(), batchSize));

        if (eventList.isEmpty()) {
            return null;
        }

        // send events to Kafka and update progress topic in one transaction
        return kafkaTemplate.executeInTransaction(kafka -> {
            Cursor updatedLastCursor = null;

            for (var event : eventList) {
                var id = (Long) event.get("id");
                var lsn = event.get("lsn").toString();

                updatedLastCursor = new Cursor(lsn, id);

                var key = event.get("key");
                var data = (byte[]) event.get("data");
                var metadata = (byte[]) event.get("metadata");
                var timestamp = (Timestamp) event.get("timestamp");
                var headers = toHeaders(id, lsn, metadata);

                // send event to Kafka topic
                kafka.send(new ProducerRecord<>(topic, null, timestamp.getTime(), serializeKey(key), data, headers));
            }

            // update progress topic
            kafka.send(EventReplicator.PROGRESS_TOPIC, stringToBytes(replicatedTopic),
                    stringToBytes(updatedLastCursor.toProgressValue()));

            return updatedLastCursor;
        });
    }

    private byte[] serializeKey(Object key) {
        if (key instanceof byte[] keyBytes) {
            return keyBytes;
        }

        return stringToBytes(key.toString());
    }

    private List<Header> toHeaders(Long id, String lsn, byte[] metadata) {
        return Stream
                .concat(
                        Stream.of(
                                Map.entry(SOURCE_ID, Long.toString(id).getBytes()),
                                Map.entry(SOURCE_LSN, lsn.getBytes())),
                        MetadataSerializer.deserialize(metadata).entrySet().stream())
                .map(e -> new RecordHeader(e.getKey().toString(), (byte[]) e.getValue()))
                .map(Header.class::cast)
                .sorted(comparing(Header::key)) // sort again after adding the id and lsn headers
                .toList();
    }

    private byte[] stringToBytes(String str) {
        return str.getBytes(StandardCharsets.UTF_8);
    }

    /**
     * Computes the event replication lag as WAL byte distance between the latest
     * event in the source topic and the latest replicated event.
     * 
     * @return computed replication lag in WAL bytes
     */
    private double computeLag() {
        var lag = jdbcTemplate.queryForObject(
                SELECT_LAG_SQL.formatted(lastCursor.lsn(), eventSchema, replicatedTopic),
                Long.class);

        return lag != null ? lag : 0;
    }

    /**
     * Replication cursor pointing at the last successfully replicated row.
     *
     * <p>
     * Persisted to the progress Kafka topic as {@code "<lsn>:<id>"}. The pair is
     * compared via Postgres row-value semantics so consumers resume correctly
     * across same-LSN siblings produced by a single transaction.
     *
     * @param lsn the row's commit LSN in canonical Postgres text form (e.g.
     *            {@code "0/12A4B5C"})
     * @param id  the row's bigserial identifier
     */
    record Cursor(String lsn, long id) {
        /**
         * Initial cursor value used for a topic with no recorded progress.
         */
        static final Cursor INITIAL = new Cursor("0/0", 0L);

        /**
         * Encodes this cursor for the progress Kafka topic.
         * 
         * @return canonical {@code "<lsn>:<id>"} text form of this cursor
         */
        String toProgressValue() {
            return lsn + ":" + id;
        }

        /**
         * Decodes a cursor from a progress-topic value.
         * 
         * @param value progress-topic value in the {@code "<lsn>:<id>"} form
         * 
         * @return cursor decoded from {@code value}
         * 
         * @throws IllegalStateException if the value is not in the expected
         *                               {@code "<lsn>:<id>"} form
         */
        static Cursor fromProgressValue(String value) {
            var parts = value.split(":");

            if (parts.length != 2) {
                throw new IllegalStateException(
                        "Invalid progress-topic cursor value (expected 'lsn:id'): " + value);
            }

            return new Cursor(parts[0], Long.parseLong(parts[1]));
        }
    }
}
