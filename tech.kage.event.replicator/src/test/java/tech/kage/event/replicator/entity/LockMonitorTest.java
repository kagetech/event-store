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
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import org.junit.jupiter.api.Test;

/**
 * Tests of {@link LockMonitor}.
 * 
 * @author Dariusz Szpakowski
 */
class LockMonitorTest {
    @Test
    void monitorsLockValidity() {
        // Given
        var lockManager = mock(LockManager.class);
        var lockMonitor = new LockMonitor(lockManager);

        given(lockManager.acquireLock()).willReturn(true);

        // When
        lockMonitor.run();

        // Then
        verify(lockManager).acquireLock();
    }

    @Test
    void shutsDownWhenLockCannotBeAcquired() {
        // Given
        var lockManager = mock(LockManager.class);
        var testableLockMonitor = new TestableLockMonitor(lockManager); // for testing System.exit()

        given(lockManager.acquireLock()).willReturn(false);

        // When
        testableLockMonitor.run();

        // Then
        assertThat(testableLockMonitor.getLastExitCode())
                .describedAs("exit code")
                .isEqualTo(1);
    }

    // Testable subclass to intercept System.exit
    static class TestableLockMonitor extends LockMonitor {
        private int lastExitCode = -1;

        TestableLockMonitor(LockManager lockManager) {
            super(lockManager);
        }

        @Override
        protected void exit(int code) {
            this.lastExitCode = code; // Store instead of exiting
        }

        int getLastExitCode() {
            return lastExitCode;
        }
    }
}
