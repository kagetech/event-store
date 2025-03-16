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

import javax.sql.DataSource;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

/**
 * Component providing locking functionality to ensure that there is only one
 * instance running.
 * 
 * @author Dariusz Szpakowski
 */
@Component
class LockManager {
    /**
     * SQL query used for calling pg_try_advisory_lock PostgreSQL function.
     */
    private static final String ACQUIRE_LOCK_SQL = "SELECT pg_try_advisory_lock(?)";

    /**
     * The name of the lock.
     */
    private static final String LOCK_NAME = "_event_replicator_lock";

    private final JdbcTemplate jdbcTemplate;

    /**
     * Constructs a new {@link LockManager} instance.
     *
     * @param lockManagerDataSource an instance of {@link DataSource}
     */
    LockManager(@Qualifier("lockManagerDataSource") DataSource lockManagerDataSource) {
        jdbcTemplate = new JdbcTemplate(lockManagerDataSource);
    }

    /**
     * Tries to acquire the lock.
     * 
     * @return {@code true} if lock acquired, {@code false} otherwise
     */
    boolean acquireLock() {
        var lockKey = getLockKey(LOCK_NAME);

        return jdbcTemplate.queryForObject(ACQUIRE_LOCK_SQL, Boolean.class, lockKey);
    }

    /**
     * Creates a 64-bit integer from the lock string hash code (higher bits) and
     * length (lower bits).
     * 
     * @param lock lock string to be transformed
     * 
     * @return a 64-bit integer generated from the given lock string
     */
    private long getLockKey(String lock) {
        return ((long) lock.hashCode() << 32) | (lock.length() & 0xFFFFFFFFl);
    }
}
