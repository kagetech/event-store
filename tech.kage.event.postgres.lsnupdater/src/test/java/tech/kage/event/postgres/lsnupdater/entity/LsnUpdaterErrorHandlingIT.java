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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.verify;

import java.nio.ByteBuffer;
import java.time.Duration;

import javax.sql.DataSource;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.DataSourceProperties;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.annotation.DirtiesContext.ClassMode;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import tech.kage.event.postgres.lsnupdater.entity.PgOutputMessageParser.InsertInfo;
import tech.kage.event.postgres.lsnupdater.entity.PgOutputMessageParser.MessageInfo;
import tech.kage.event.postgres.lsnupdater.entity.PgOutputMessageParser.MessageType;
import tech.kage.event.postgres.lsnupdater.entity.PgOutputMessageParser.RelationInfo;

/**
 * Integration tests for {@link LsnUpdater} error handling.
 * 
 * @author Dariusz Szpakowski
 */
@SpringBootTest
@ActiveProfiles("test")
@Testcontainers
@DirtiesContext(classMode = ClassMode.AFTER_EACH_TEST_METHOD)
class LsnUpdaterErrorHandlingIT {
    // UUT
    @Autowired
    TestableLsnUpdater lsnUpdater;

    @MockitoBean
    PgOutputMessageParser parser;

    @MockitoBean
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
    static class TestConfiguration {
        @Bean
        TestableLsnUpdater lsnUpdater(
                PgOutputMessageParser parser,
                JdbcTemplate jdbcTemplate,
                DataSourceProperties dataSourceProperties) {
            return new TestableLsnUpdater(parser, jdbcTemplate, dataSourceProperties);
        }
    }

    @Test
    void terminatesProcessWhenParserThrows() {
        // Given
        given(parser.parse(any())).willThrow(new IllegalStateException("parse error"));

        // When
        insertTestEvent("11111111-1111-1111-1111-111111111111");

        // Then
        await()
                .atMost(Duration.ofSeconds(10))
                .pollInterval(Duration.ofMillis(100))
                .untilAsserted(() -> assertThat(lsnUpdater.getLastExitCode())
                        .describedAs("exit code")
                        .isEqualTo(1));
    }

    @Test
    void terminatesProcessWhenInsertHasNullId() {
        // Given
        given(parser.parse(any())).willAnswer(invocation -> {
            ByteBuffer buf = invocation.getArgument(0);
            byte type = buf.get(buf.position());

            if (type == 'I') {
                return new MessageInfo(
                        MessageType.INSERT,
                        new RelationInfo(1, "events", "test_events"),
                        new InsertInfo(1, null));
            }

            return null;
        });

        // When
        insertTestEvent("22222222-2222-2222-2222-222222222222");

        // Then
        await()
                .atMost(Duration.ofSeconds(10))
                .pollInterval(Duration.ofMillis(100))
                .untilAsserted(() -> assertThat(lsnUpdater.getLastExitCode())
                        .describedAs("exit code")
                        .isEqualTo(1));
    }

    @Test
    void terminatesProcessWhenUpdateLsnAffectsUnexpectedNumberOfRows() {
        // Given
        given(jdbcTemplate.update(anyString(), any(), any())).willReturn(0);

        given(parser.parse(any())).willAnswer(invocation -> {
            ByteBuffer buf = invocation.getArgument(0);
            byte type = buf.get(buf.position());

            if (type == 'I') {
                return new MessageInfo(
                        MessageType.INSERT,
                        new RelationInfo(1, "events", "test_events"),
                        new InsertInfo(1, 123L));
            }

            return null;
        });

        // When
        insertTestEvent("44444444-4444-4444-4444-444444444444");

        // Then
        await()
                .atMost(Duration.ofSeconds(10))
                .pollInterval(Duration.ofMillis(100))
                .untilAsserted(() -> assertThat(lsnUpdater.getLastExitCode())
                        .describedAs("exit code")
                        .isEqualTo(1));
    }

    @Test
    void continuesProcessingWhenNonEventsTableInsertReceived() {
        // Given
        given(parser.parse(any())).willAnswer(invocation -> {
            ByteBuffer buf = invocation.getArgument(0);
            byte type = buf.get(buf.position());

            if (type == 'I') {
                return new MessageInfo(
                        MessageType.INSERT,
                        new RelationInfo(1, "events", "some_other_table"),
                        new InsertInfo(1, 123L));
            }

            return null;
        });

        // When
        insertTestEvent("33333333-3333-3333-3333-333333333333");

        // Then
        await()
                .atMost(Duration.ofSeconds(10))
                .pollInterval(Duration.ofMillis(100))
                .untilAsserted(() -> verify(parser, atLeast(1)).parse(any()));

        await()
                .during(Duration.ofSeconds(2))
                .atMost(Duration.ofSeconds(3))
                .pollInterval(Duration.ofMillis(100))
                .untilAsserted(() -> assertThat(lsnUpdater.getLastExitCode())
                        .describedAs("exit code")
                        .isEqualTo(-1));
    }

    private void insertTestEvent(String key) {
        new JdbcTemplate(dataSource).execute(
                "INSERT INTO events.test_events (key, data, metadata, timestamp) VALUES ('%s', decode('AA', 'hex'), null, now())"
                        .formatted(key));
    }

    static class TestableLsnUpdater extends LsnUpdater {
        private volatile int lastExitCode = -1;

        TestableLsnUpdater(
                PgOutputMessageParser parser,
                JdbcTemplate jdbcTemplate,
                DataSourceProperties dataSourceProperties) {
            super(parser, jdbcTemplate, dataSourceProperties);
        }

        @Override
        protected void exit(int code) {
            this.lastExitCode = code;
        }

        int getLastExitCode() {
            return lastExitCode;
        }
    }
}
