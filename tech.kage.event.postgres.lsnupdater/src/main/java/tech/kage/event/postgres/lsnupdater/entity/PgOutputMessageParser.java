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
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.springframework.stereotype.Component;

/**
 * Parser for PostgreSQL pgoutput binary protocol messages.
 * Handles BEGIN, COMMIT, RELATION and INSERT messages, skips other message
 * types.
 * 
 * <p>
 * The parser assumes a fixed event table schema where {@code id} is always
 * the first column and is of type {@code bigserial} (bigint). Column metadata
 * from RELATION messages is not validated — only the relation ID, schema, and
 * table name are extracted.
 * 
 * <p>
 * The message format is documented in the <a href=
 * "https://www.postgresql.org/docs/current/protocol-logicalrep-message-formats.html">PostgreSQL
 * Logical Replication Message Formats</a> documentation.
 * 
 * @author Dariusz Szpakowski
 */
@Component
class PgOutputMessageParser {
    /**
     * Message type byte for BEGIN.
     */
    private static final byte MESSAGE_TYPE_BEGIN = 'B';

    /**
     * Message type byte for COMMIT.
     */
    private static final byte MESSAGE_TYPE_COMMIT = 'C';

    /**
     * Message type byte for RELATION (table metadata).
     */
    private static final byte MESSAGE_TYPE_RELATION = 'R';

    /**
     * Message type byte for INSERT.
     */
    private static final byte MESSAGE_TYPE_INSERT = 'I';

    /**
     * Mapping of relation IDs to schema/table names.
     */
    private final Map<Integer, RelationInfo> relations = new ConcurrentHashMap<>();

    /**
     * Parses a pgoutput message from the given ByteBuffer.
     * 
     * @param buffer the ByteBuffer containing the message
     * 
     * @return parsed message, or null if message type is not handled
     */
    PgOutputMessage parse(ByteBuffer buffer) {
        if (buffer.remaining() < 1) {
            return null;
        }

        var messageType = buffer.get();

        return switch (messageType) {
            case MESSAGE_TYPE_BEGIN -> parseBegin(buffer);
            case MESSAGE_TYPE_COMMIT -> parseCommit(buffer);
            case MESSAGE_TYPE_RELATION -> parseRelation(buffer);
            case MESSAGE_TYPE_INSERT -> parseInsert(buffer);
            default -> null;
        };
    }

    /**
     * Parses a BEGIN message.
     * Format: final_lsn (8 bytes, big-endian), commit_timestamp (8 bytes,
     * big-endian), xid (4 bytes, big-endian).
     * 
     * <p>
     * In pgoutput v1 with non-streaming transactions, {@code final_lsn} equals
     * the eventual commit LSN of the transaction. The walsender knows this value
     * before emitting BEGIN because the entire transaction has already been
     * decoded from the WAL.
     * 
     * @param buffer byte buffer containing the BEGIN message payload
     * 
     * @return parsed BEGIN message
     */
    private BeginMessage parseBegin(ByteBuffer buffer) {
        buffer.order(ByteOrder.BIG_ENDIAN);

        var finalLsn = buffer.getLong();
        var commitTimestamp = buffer.getLong();
        var xid = buffer.getInt();

        return new BeginMessage(finalLsn, commitTimestamp, xid);
    }

    /**
     * Parses a COMMIT message.
     * Format: flags (1 byte), commit_lsn (8 bytes, big-endian), end_lsn (8 bytes,
     * big-endian), commit_timestamp (8 bytes, big-endian).
     * 
     * @param buffer byte buffer containing the COMMIT message payload
     * 
     * @return parsed COMMIT message
     */
    private CommitMessage parseCommit(ByteBuffer buffer) {
        var flags = buffer.get();

        buffer.order(ByteOrder.BIG_ENDIAN);

        var commitLsn = buffer.getLong();
        var endLsn = buffer.getLong();
        var commitTimestamp = buffer.getLong();

        return new CommitMessage(flags, commitLsn, endLsn, commitTimestamp);
    }

    /**
     * Parses a RELATION message.
     * Format: relationId (4 bytes, big-endian), namespace (null-terminated string),
     * relation name (null-terminated string), followed by column metadata
     * (ignored).
     * 
     * @param buffer byte buffer containing the RELATION message payload
     * 
     * @return parsed RELATION message
     */
    private RelationMessage parseRelation(ByteBuffer buffer) {
        buffer.order(ByteOrder.BIG_ENDIAN);

        var relationId = buffer.getInt();
        var namespace = readNullTerminatedString(buffer);
        var relationName = readNullTerminatedString(buffer);

        // Column metadata is ignored (we assume id is always the first column)

        var relationInfo = new RelationInfo(relationId, namespace, relationName);

        relations.put(relationId, relationInfo);

        return new RelationMessage(relationInfo);
    }

    /**
     * Parses an INSERT message.
     * Format: relationId (4 bytes, big-endian), new tuple data
     * 
     * @param buffer byte buffer containing the INSERT message payload
     * 
     * @return parsed INSERT message
     */
    private InsertMessage parseInsert(ByteBuffer buffer) {
        buffer.order(ByteOrder.BIG_ENDIAN);

        var relationId = buffer.getInt();

        var relationInfo = relations.get(relationId);

        if (relationInfo == null) {
            throw new IllegalStateException(
                    "Received INSERT for relation OID " + relationId + " before corresponding RELATION message");
        }

        var idValue = parseIdValue(buffer);

        return new InsertMessage(relationInfo, idValue);
    }

    /**
     * Parses the id value from tuple data in the buffer.
     * Assumes id is always the first column and NOT NULL bigserial (bigint).
     * Format: 'N' (1 byte) for new tuple, column count (2 bytes, big-endian),
     * for each column: format indicator (1 byte: 't' = text), value length
     * (4 bytes, big-endian), value bytes (text format).
     * 
     * @param buffer byte buffer containing tuple data
     * 
     * @return the id value
     */
    private long parseIdValue(ByteBuffer buffer) {
        var tupleType = buffer.get();

        if (tupleType != 'N') { // 'N' for new tuple
            throw new IllegalStateException("Unexpected INSERT tuple type: " + (char) tupleType);
        }

        buffer.order(ByteOrder.BIG_ENDIAN);

        buffer.getShort(); // skip column count

        var firstColumnFormat = buffer.get();

        if (firstColumnFormat != 't') { // 't' for text
            throw new IllegalStateException("Unexpected INSERT first column format: " + (char) firstColumnFormat);
        }

        var valueLength = buffer.getInt();
        var valueBytes = new byte[valueLength];

        buffer.get(valueBytes);

        return Long.parseLong(new String(valueBytes, StandardCharsets.UTF_8));
    }

    /**
     * Reads a null-terminated string from the buffer.
     * 
     * @param buffer byte buffer positioned at the start of a null-terminated string
     * 
     * @return decoded UTF-8 string without the null terminator
     */
    private String readNullTerminatedString(ByteBuffer buffer) {
        var start = buffer.position();
        var length = 0;

        while (buffer.get() != 0) {
            length++;
        }

        buffer.position(start);

        var bytes = new byte[length];

        buffer.get(bytes);
        buffer.get(); // skip null terminator

        return new String(bytes, StandardCharsets.UTF_8);
    }

    /**
     * Parsed pgoutput message.
     */
    sealed interface PgOutputMessage permits BeginMessage, CommitMessage, RelationMessage, InsertMessage {
    }

    /**
     * BEGIN message marking the start of a transaction in the replication stream.
     * 
     * @param finalLsn        commit LSN of the transaction (known up front because
     *                        the walsender has already decoded the COMMIT record)
     * @param commitTimestamp commit timestamp in microseconds since 2000-01-01 UTC
     * @param xid             transaction id
     */
    record BeginMessage(long finalLsn, long commitTimestamp, int xid) implements PgOutputMessage {
    }

    /**
     * COMMIT message marking the end of a transaction in the replication stream.
     * 
     * @param flags           commit flags (must be 0 in pgoutput v1)
     * @param commitLsn       LSN of the commit record (must equal the matching
     *                        BEGIN's finalLsn)
     * @param endLsn          LSN immediately after the commit record
     * @param commitTimestamp commit timestamp in microseconds since 2000-01-01 UTC
     */
    record CommitMessage(byte flags, long commitLsn, long endLsn, long commitTimestamp) implements PgOutputMessage {
    }

    /**
     * RELATION message describing a table's schema.
     * 
     * @param relation parsed relation metadata (cached for use by subsequent
     *                 INSERT messages referencing the same relation OID)
     */
    record RelationMessage(RelationInfo relation) implements PgOutputMessage {
    }

    /**
     * INSERT message describing a single inserted row.
     * 
     * @param relation relation the row was inserted into
     * @param id       parsed id column value
     */
    record InsertMessage(RelationInfo relation, long id) implements PgOutputMessage {
    }

    /**
     * Information about a database relation (table).
     * 
     * @param relationId internal relation identifier from pgoutput
     * @param schema     relation schema name
     * @param table      relation table name
     */
    record RelationInfo(int relationId, String schema, String table) {
    }
}
