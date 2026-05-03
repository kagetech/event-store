/*
 * Copyright (c) 2026, Dariusz Szpakowski
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

package tech.kage.event.postgres.lsnupdater.entity;

import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.postgresql.PGConnection;
import org.postgresql.PGProperty;
import org.postgresql.replication.PGReplicationStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.jdbc.DataSourceProperties;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import tech.kage.event.postgres.lsnupdater.entity.PgOutputMessageParser.BeginMessage;
import tech.kage.event.postgres.lsnupdater.entity.PgOutputMessageParser.CommitMessage;
import tech.kage.event.postgres.lsnupdater.entity.PgOutputMessageParser.InsertMessage;
import tech.kage.event.postgres.lsnupdater.entity.PgOutputMessageParser.RelationMessage;

/**
 * Component that uses PostgreSQL logical replication to update LSN columns in
 * events tables.
 * Connects to an existing replication slot and processes BEGIN/INSERT/COMMIT
 * operations to set the LSN value of every inserted row to its transaction's
 * commit LSN.
 * 
 * <h2>Why commit LSN, not insert LSN</h2>
 * INSERT records in the WAL are themselves strictly monotonic — Postgres uses
 * a single global WAL log. However, the pgoutput protocol delivers transactions
 * to the consumer in commit order, not WAL-write order. So the sequence of
 * per-INSERT WAL positions <em>as seen by this component</em> can decrease
 * across consecutive transactions: txn A's INSERT at WAL=100 may arrive after
 * txn B's INSERT at WAL=110 because B committed first.
 * Stamping with the per-INSERT WAL position would therefore produce
 * non-monotonic LSN values in the events table; consumers using
 * {@code WHERE lsn > :cursor} would silently skip rows whose LSN regressed.
 * Stamping with the commit LSN (BEGIN.final_lsn) instead gives all rows in a
 * transaction the same LSN and guarantees monotonicity across transactions.
 * 
 * <h2>Correctness invariants</h2>
 * <ul>
 * <li>Messages are processed sequentially in a single thread — the UPDATE for
 * row N always commits before the UPDATE for row N+1 begins.</li>
 * <li>Each UPDATE runs with auto-commit via {@link JdbcTemplate}. The
 * replication slot is only advanced after the UPDATE has been committed.
 * This guarantees that a crash at any point results in at-least-once
 * delivery, never data loss.</li>
 * <li>Replay after crash is idempotent — re-processing an INSERT writes
 * the same LSN value to the same row.</li>
 * <li>Any processing error terminates the process (fail-fast). An external
 * supervisor is expected to restart it.</li>
 * </ul>
 * 
 * @author Dariusz Szpakowski
 */
@Component
@ConditionalOnProperty(name = "event.lsn.updater.enabled", matchIfMissing = true)
class LsnUpdater {
    private static final Logger log = LoggerFactory.getLogger(LsnUpdater.class);

    /**
     * SQL query template for updating LSN column.
     */
    private static final String UPDATE_LSN_SQL = "UPDATE %s.%s SET lsn = ?::pg_lsn WHERE id = ?";

    /**
     * The suffix that all event tables need to have.
     */
    private static final String TOPIC_SUFFIX = "_events";

    private final PgOutputMessageParser parser;
    private final JdbcTemplate jdbcTemplate;
    private final DataSourceProperties dataSourceProperties;

    /**
     * Configuration property defining the replication slot name.
     */
    @Value("${event.lsn.updater.slot.name:event_lsn_updater}")
    private String slotName;

    /**
     * Configuration property defining the publication name.
     */
    @Value("${event.lsn.updater.publication.name:event_lsn_publication}")
    private String publicationName;

    private Thread replicationThread;
    private Connection replicationConnection;
    private PGReplicationStream replicationStream;

    /**
     * Commit LSN of the currently-open transaction in the replication stream;
     * {@code null} when not inside a transaction. Set on BEGIN, used to stamp
     * each INSERT in the txn, cleared on COMMIT.
     */
    private Long currentTxnLsn;

    /**
     * Constructs a new {@link LsnUpdater} instance.
     *
     * @param parser               an instance of {@link PgOutputMessageParser}
     * @param jdbcTemplate         an instance of {@link JdbcTemplate}
     * @param dataSourceProperties an instance of {@link DataSourceProperties}
     */
    LsnUpdater(PgOutputMessageParser parser, JdbcTemplate jdbcTemplate, DataSourceProperties dataSourceProperties) {
        this.parser = parser;
        this.jdbcTemplate = jdbcTemplate;
        this.dataSourceProperties = dataSourceProperties;
    }

    /**
     * Starts the LSN updater after component initialization.
     */
    @PostConstruct
    void init() throws SQLException {
        log.info("Starting LSN updater with slot: {}, publication: {}", slotName, publicationName);

        connectToReplicationSlot();

        replicationThread = new Thread(this::runReplication, "lsn-updater");

        replicationThread.setDaemon(false);
        replicationThread.setUncaughtExceptionHandler((thread, throwable) -> {
            log.error("LSN updater thread crashed, terminating process", throwable);

            exit(1);
        });

        replicationThread.start();
    }

    /**
     * Stops the LSN updater before component destruction.
     */
    @PreDestroy
    void destroy() throws Exception {
        log.info("Stopping LSN updater");

        replicationThread.interrupt();
        replicationThread.join(5000);

        try {
            replicationStream.close();
        } finally {
            replicationConnection.close();
        }
    }

    /**
     * Main replication loop running in a background thread.
     */
    private void runReplication() {
        try {
            while (!Thread.currentThread().isInterrupted()) {
                var message = replicationStream.readPending();

                if (message == null) {
                    TimeUnit.MILLISECONDS.sleep(10L);
                    continue;
                }

                processMessage(message);

                // Send feedback to server
                var lastRecv = replicationStream.getLastReceiveLSN();

                replicationStream.setAppliedLSN(lastRecv);
                replicationStream.setFlushedLSN(lastRecv);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            throw new IllegalStateException("LSN updater replication loop failed", e);
        }
    }

    /**
     * Connects to the replication slot.
     */
    private void connectToReplicationSlot() throws SQLException {
        var props = new Properties();

        PGProperty.USER.set(props, dataSourceProperties.getUsername());
        PGProperty.PASSWORD.set(props, dataSourceProperties.getPassword());
        PGProperty.ASSUME_MIN_SERVER_VERSION.set(props, "10.0");
        PGProperty.REPLICATION.set(props, "database");
        PGProperty.PREFER_QUERY_MODE.set(props, "simple");
        PGProperty.CONNECT_TIMEOUT.set(props, "10");
        PGProperty.SOCKET_TIMEOUT.set(props, "30");

        replicationConnection = DriverManager.getConnection(dataSourceProperties.getUrl(), props);

        var pgConnection = replicationConnection.unwrap(PGConnection.class);

        replicationStream = pgConnection
                .getReplicationAPI()
                .replicationStream()
                .logical()
                .withSlotName(slotName)
                .withSlotOption("proto_version", "1")
                .withSlotOption("publication_names", publicationName)
                .start();

        log.info("Connected to replication slot: {}", slotName);
    }

    /**
     * Processes a replication message.
     *
     * @param message replication payload buffer read from the stream
     */
    private void processMessage(ByteBuffer message) {
        var parsed = parser.parse(message);

        switch (parsed) {
            case BeginMessage begin -> currentTxnLsn = begin.finalLsn();
            case InsertMessage insert -> processInsert(insert);
            case CommitMessage commit -> {
                if (currentTxnLsn == null || currentTxnLsn.longValue() != commit.commitLsn()) {
                    throw new IllegalStateException(
                            "COMMIT commitLsn=" + commit.commitLsn()
                                    + " does not match open transaction's BEGIN.finalLsn=" + currentTxnLsn);
                }
                currentTxnLsn = null;
            }
            case RelationMessage relation -> {
                // Relation metadata is cached inside the parser; nothing to do here.
            }
            case null -> {
                // Unhandled message type; pgoutput protocol allows skipping.
            }
        }
    }

    /**
     * Processes an INSERT message and updates the LSN column with the current
     * transaction's commit LSN.
     * 
     * @param insert parsed INSERT message
     */
    private void processInsert(InsertMessage insert) {
        var relationInfo = insert.relation();

        // Only process tables ending with _events suffix
        if (!relationInfo.table().endsWith(TOPIC_SUFFIX)) {
            return;
        }

        if (currentTxnLsn == null) {
            throw new IllegalStateException(
                    "INSERT for " + relationInfo.schema() + "." + relationInfo.table()
                            + " received outside of an open transaction (no BEGIN seen)");
        }

        // Update the LSN column
        updateLsn(relationInfo.schema(), relationInfo.table(), insert.id(), lsnToText(currentTxnLsn));
    }

    /**
     * Updates the LSN column for a specific row.
     *
     * @param schema schema containing the target table
     * @param table  target table name
     * @param id     row identifier to update
     * @param lsn    WAL location to persist in the row
     */
    private void updateLsn(String schema, String table, long id, String lsn) {
        var sql = UPDATE_LSN_SQL.formatted(schema, table);

        var rowsUpdated = jdbcTemplate.update(sql, lsn, id);

        if (rowsUpdated != 1) {
            throw new IllegalStateException("Expected exactly one row updated for "
                    + schema + "." + table + " id=" + id + ", but got " + rowsUpdated);
        }

        log.debug("Updated LSN for {}.{} id={} to {}", schema, table, id, lsn);
    }

    /**
     * Formats a 64-bit LSN as Postgres' canonical {@code "X/Y"} text form (high
     * 32 bits / low 32 bits in uppercase hexadecimal).
     * 
     * @param lsn 64-bit WAL location
     * 
     * @return canonical {@code "X/Y"} text form of the LSN
     */
    private static String lsnToText(long lsn) {
        return String.format("%X/%X", lsn >>> 32, lsn & 0xFFFFFFFFL);
    }

    // Added for testability
    protected void exit(int code) {
        System.exit(code);
    }
}
