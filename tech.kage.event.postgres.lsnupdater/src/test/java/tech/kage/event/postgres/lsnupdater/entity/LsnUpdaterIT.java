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

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;

import javax.sql.DataSource;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.TestPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * Integration tests for {@link LsnUpdater}.
 * 
 * @author Dariusz Szpakowski
 */
@SpringBootTest
@ActiveProfiles("test")
@TestPropertySource(properties = "event.lsn.updater.enabled=true")
@Testcontainers
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class LsnUpdaterIT {
    // UUT
    @Autowired
    LsnUpdater lsnUpdater;

    @Autowired
    JdbcTemplate jdbcTemplate;

    @Autowired
    DataSource dataSource;

    @SuppressWarnings("resource")
    @Container
    @ServiceConnection
    static final PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:15-alpine")
            .withCommand("postgres", "-c", "wal_level=logical")
            .withInitScript("test-data/lsn-updater/init.sql");

    @DynamicPropertySource
    static void dataSourceProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.datasource.password", postgres::getPassword);
    }

    @Configuration
    @EnableAutoConfiguration
    @Import({ LsnUpdater.class, PgOutputMessageParser.class })
    static class TestConfiguration {
    }

    @AfterEach
    void tearDown() {
        jdbcTemplate.execute("TRUNCATE TABLE events.test_events");
    }

    @Test
    @Order(1)
    void updatesLsnForInsertedRow() {
        // Given
        // LsnUpdater is running via Spring context

        // When
        var id = jdbcTemplate.queryForObject("""
                INSERT INTO events.test_events (key, data, metadata, timestamp)
                VALUES ('11111111-1111-1111-1111-111111111111', decode('AA', 'hex'), null, now())
                RETURNING id
                """, Long.class);

        // Then
        await()
                .atMost(Duration.ofSeconds(10))
                .pollInterval(Duration.ofMillis(100))
                .untilAsserted(() -> {
                    var lsn = jdbcTemplate.queryForObject(
                            "SELECT lsn::text FROM events.test_events WHERE id = ?",
                            String.class,
                            id);

                    assertThat(lsn)
                            .describedAs("lsn for id %d", id)
                            .isNotNull();
                });
    }

    @Test
    @Order(2)
    void stampsAllRowsOfMultiInsertTxnWithSameCommitLsn() throws SQLException {
        // Given
        // LsnUpdater is running via Spring context. We drive an explicit
        // transaction with three separate INSERT statements to simulate the
        // downstream usage pattern of composing multiple save() calls under
        // one TransactionalOperator.
        long id1;
        long id2;
        long id3;

        // When
        try (var conn = dataSource.getConnection()) {
            conn.setAutoCommit(false);

            id1 = insertOneAndReturnId(conn, "22222222-2222-2222-2222-222222222222", (byte) 0xBB);
            id2 = insertOneAndReturnId(conn, "33333333-3333-3333-3333-333333333333", (byte) 0xCC);
            id3 = insertOneAndReturnId(conn, "44444444-4444-4444-4444-444444444444", (byte) 0xDD);

            conn.commit();
        }

        // Then
        await()
                .atMost(Duration.ofSeconds(10))
                .pollInterval(Duration.ofMillis(100))
                .untilAsserted(() -> {
                    var lsnValues = jdbcTemplate.queryForList(
                            "SELECT lsn::text FROM events.test_events WHERE id IN (?, ?, ?) ORDER BY id",
                            String.class,
                            id1, id2, id3);

                    assertThat(lsnValues)
                            .describedAs("lsns for rows of multi-insert txn")
                            .hasSize(3)
                            .doesNotContainNull();

                    assertThat(lsnValues)
                            .describedAs("all rows of one txn share the same commit LSN")
                            .containsOnly(lsnValues.get(0));
                });
    }

    @Test
    @Order(3)
    void stampsCommitLsnNotInsertLsnUnderConcurrentTxns() throws SQLException {
        // Given
        // Drive a commit-reorder pattern: txn A inserts first (lower WAL
        // position) but commits last; txn B inserts second but commits first.
        // With commit-LSN stamping, the row inserted by B (committed first)
        // must have a smaller lsn than the row inserted by A. With insert-LSN
        // stamping the relation would be reversed.
        long idA;
        long idB;

        // When
        try (var connA = dataSource.getConnection(); var connB = dataSource.getConnection()) {
            connA.setAutoCommit(false);
            connB.setAutoCommit(false);

            // 1. Conn-A: INSERT (writes WAL first).
            idA = insertOneAndReturnId(connA, "55555555-5555-5555-5555-555555555555", (byte) 0xAA);

            // 2. Conn-B: INSERT (writes WAL after A).
            idB = insertOneAndReturnId(connB, "66666666-6666-6666-6666-666666666666", (byte) 0xBB);

            // 3. Conn-B: COMMIT (commits before A despite later INSERT).
            connB.commit();

            // 4. Conn-A: COMMIT (commits after B).
            connA.commit();
        }

        // Then
        var finalIdA = idA;
        var finalIdB = idB;

        await()
                .atMost(Duration.ofSeconds(10))
                .pollInterval(Duration.ofMillis(100))
                .untilAsserted(() -> {
                    var lsnB = jdbcTemplate.queryForObject(
                            "SELECT lsn::text FROM events.test_events WHERE id = ?",
                            String.class,
                            finalIdB);
                    var lsnA = jdbcTemplate.queryForObject(
                            "SELECT lsn::text FROM events.test_events WHERE id = ?",
                            String.class,
                            finalIdA);

                    assertThat(lsnA).describedAs("lsn for row inserted by A").isNotNull();
                    assertThat(lsnB).describedAs("lsn for row inserted by B").isNotNull();

                    var bIsLessThanA = jdbcTemplate.queryForObject(
                            "SELECT (?::pg_lsn < ?::pg_lsn)",
                            Boolean.class,
                            lsnB, lsnA);

                    assertThat(bIsLessThanA)
                            .describedAs("lsn of B's row (committed first) < lsn of A's row — got B=%s, A=%s",
                                    lsnB, lsnA)
                            .isTrue();
                });
    }

    @Test
    @Order(4)
    void stopsProcessingAfterDestroy() throws Exception {
        // Given
        // LsnUpdater is running via Spring context

        // When
        lsnUpdater.destroy();

        // Then
        var id = jdbcTemplate.queryForObject("""
                INSERT INTO events.test_events (key, data, metadata, timestamp)
                VALUES ('77777777-7777-7777-7777-777777777777', decode('EE', 'hex'), null, now())
                RETURNING id
                """, Long.class);

        await()
                .during(Duration.ofSeconds(2))
                .atMost(Duration.ofSeconds(3))
                .pollInterval(Duration.ofMillis(100))
                .untilAsserted(() -> {
                    var lsn = jdbcTemplate.queryForObject(
                            "SELECT lsn::text FROM events.test_events WHERE id = ?",
                            String.class,
                            id);

                    assertThat(lsn)
                            .describedAs("lsn for id %d after destroy", id)
                            .isNull();
                });
    }

    private long insertOneAndReturnId(Connection conn, String key, byte data) throws SQLException {
        try (var stmt = conn.prepareStatement("""
                INSERT INTO events.test_events (key, data, metadata, timestamp)
                VALUES (?::uuid, ?, null, now())
                RETURNING id
                """)) {
            stmt.setString(1, key);
            stmt.setBytes(2, new byte[] { data });

            try (var rs = stmt.executeQuery()) {
                rs.next();

                return rs.getLong(1);
            }
        }
    }
}
