/*
 * Copyright (c) 2025, Dariusz Szpakowski
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
import static org.springframework.beans.factory.config.BeanDefinition.SCOPE_PROTOTYPE;

import javax.sql.DataSource;

import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Scope;
import org.springframework.test.context.ActiveProfiles;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import com.zaxxer.hikari.HikariDataSource;

/**
 * Integration tests for {@link LockManager}.
 * 
 * @author Dariusz Szpakowski
 */
@SpringBootTest
@ActiveProfiles("test")
@Testcontainers
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class LockManagerIT {
    @Autowired
    ObjectProvider<DataSource> dataSourceProvider;

    @Container
    @ServiceConnection
    static final PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:15-alpine");

    @Configuration
    @Import(LockManager.class)
    static class TestConfiguration {
        @Bean(name = "lockManagerDataSource")
        @Scope(SCOPE_PROTOTYPE)
        DataSource lockManagerDataSource() {
            var dataSource = DataSourceBuilder.create().type(HikariDataSource.class).build();

            dataSource.setJdbcUrl(postgres.getJdbcUrl());
            dataSource.setUsername(postgres.getUsername());
            dataSource.setPassword(postgres.getPassword());
            dataSource.setMaximumPoolSize(1);
            dataSource.setMinimumIdle(1);

            return dataSource;
        }
    }

    @Test
    @Order(1)
    void successfullyAcquiresLock() {
        // Given
        var dataSource = dataSourceProvider.getObject();

        var lockManager = new LockManager(dataSource);

        // When
        var locked = lockManager.acquireLock();

        // Then
        assertThat(locked)
                .describedAs("lock acquired")
                .isTrue();
    }

    @Test
    @Order(2)
    void failsToAcquireLockWhenLockAcquiredByAnotherInstance() {
        // Given
        var anotherDataSource = dataSourceProvider.getObject();
        var anotherLockManager = new LockManager(anotherDataSource);

        // When
        var locked = anotherLockManager.acquireLock();

        // Then
        assertThat(locked)
                .describedAs("lock acquired")
                .isFalse();
    }
}
