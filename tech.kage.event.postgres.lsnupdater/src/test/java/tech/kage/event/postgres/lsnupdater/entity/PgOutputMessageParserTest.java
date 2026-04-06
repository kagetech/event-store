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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Named.named;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import tech.kage.event.postgres.lsnupdater.entity.PgOutputMessageParser.InsertInfo;
import tech.kage.event.postgres.lsnupdater.entity.PgOutputMessageParser.MessageInfo;
import tech.kage.event.postgres.lsnupdater.entity.PgOutputMessageParser.MessageType;
import tech.kage.event.postgres.lsnupdater.entity.PgOutputMessageParser.RelationInfo;

/**
 * Tests of {@link PgOutputMessageParser}.
 * 
 * @author Dariusz Szpakowski
 */
class PgOutputMessageParserTest {
    // UUT
    PgOutputMessageParser parser = new PgOutputMessageParser();

    @Test
    void parsesRelationMessages() throws IOException {
        // Given
        var buffer = buildRelationMessage(12345, "events", "user_events", "id", "key", "data");

        var expected = new MessageInfo(
                MessageType.RELATION,
                new RelationInfo(12345, "events", "user_events"),
                null);

        // When
        var result = parser.parse(buffer);

        // Then
        assertThat(result).isEqualTo(expected);
    }

    @ParameterizedTest
    @MethodSource("testInsertMessagesWithKnownRelation")
    void parsesInsertMessagesWithKnownRelation(ByteBuffer relationBuffer, ByteBuffer insertBuffer, MessageInfo expected)
            throws IOException {
        // Given
        parser.parse(relationBuffer); // Parse RELATION message first to register relation metadata in the parser

        // When
        var result = parser.parse(insertBuffer);

        // Then
        assertThat(result).isEqualTo(expected);
    }

    static Stream<Arguments> testInsertMessagesWithKnownRelation() throws IOException {
        return Stream.of(
                arguments(
                        named("insert with id at first position",
                                buildRelationMessage(100, "events", "test_events", "id", "key", "data")),
                        buildInsertMessage(100, "42"),
                        new MessageInfo(
                                MessageType.INSERT,
                                new RelationInfo(100, "events", "test_events"),
                                new InsertInfo(100, 42L))),
                arguments(
                        named("insert with large id value",
                                buildRelationMessage(300, "events", "big_events", "id")),
                        buildInsertMessage(300, "9223372036854775807"),
                        new MessageInfo(
                                MessageType.INSERT,
                                new RelationInfo(300, "events", "big_events"),
                                new InsertInfo(300, Long.MAX_VALUE))));
    }

    @Test
    void handlesMultipleRelationsAndInserts() throws IOException {
        // Given
        var relation1 = buildRelationMessage(1, "events", "events_a", "id", "data");
        var relation2 = buildRelationMessage(2, "events", "events_b", "id", "key");

        // Parse RELATION messages first to register relation metadata in the parser
        parser.parse(relation1);
        parser.parse(relation2);

        var inserts = List.of(buildInsertMessage(1, "100"), buildInsertMessage(2, "200"));

        var expected = List.of(
                new MessageInfo(
                        MessageType.INSERT,
                        new RelationInfo(1, "events", "events_a"),
                        new InsertInfo(1, 100L)),
                new MessageInfo(
                        MessageType.INSERT,
                        new RelationInfo(2, "events", "events_b"),
                        new InsertInfo(2, 200L)));

        // When
        var results = inserts.stream().map(parser::parse).toList();

        // Then
        assertThat(results).isEqualTo(expected);
    }

    @Test
    void failsWhenInsertIsReceivedBeforeRelationMessage() throws IOException {
        // Given
        var insertBuffer = buildInsertMessage(999, "42");

        // When
        var thrown = assertThrows(Throwable.class, () -> parser.parse(insertBuffer));

        // Then
        assertThat(thrown)
                .describedAs("thrown exception")
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("relation OID 999");
    }

    @Test
    void failsWhenInsertTupleTypeIsUnexpected() throws IOException {
        // Given
        parser.parse(buildRelationMessage(100, "events", "test_events", "id", "key", "data"));

        var insertBuffer = buildInsertMessage(100, "42");

        insertBuffer.put(5, (byte) 'O'); // replace tuple type 'N' with unsupported value

        // When
        var thrown = assertThrows(Throwable.class, () -> parser.parse(insertBuffer));

        // Then
        assertThat(thrown)
                .describedAs("thrown exception")
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("tuple type");
    }

    @Test
    void failsWhenInsertFirstColumnFormatIsUnexpected() throws IOException {
        // Given
        parser.parse(buildRelationMessage(100, "events", "test_events", "id", "key", "data"));

        var insertBuffer = buildInsertMessage(100, "42");

        insertBuffer.put(8, (byte) 'n'); // replace column format 't' with unsupported value

        // When
        var thrown = assertThrows(Throwable.class, () -> parser.parse(insertBuffer));

        // Then
        assertThat(thrown)
                .describedAs("thrown exception")
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("first column format");
    }

    @ParameterizedTest
    @MethodSource("testUnknownMessages")
    void handlesUnknownMessages(ByteBuffer buffer) {
        // When
        var result = parser.parse(buffer);

        // Then
        assertThat(result).isNull();
    }

    static Stream<Arguments> testUnknownMessages() {
        return Stream.of(
                arguments(named("empty buffer", ByteBuffer.allocate(0))),
                arguments(named("unknown message type 'X'", ByteBuffer.allocate(1).put((byte) 'X').flip())),
                arguments(named("BEGIN message", ByteBuffer.allocate(1).put((byte) 'B').flip())),
                arguments(named("COMMIT message", ByteBuffer.allocate(1).put((byte) 'C').flip())));
    }

    /**
     * Builds a RELATION message buffer.
     * Format: 'R' (1 byte), relationId (4 bytes), namespace (null-terminated),
     * relation name (null-terminated), replica identity (1 byte),
     * column count (2 bytes), for each column: flags (1 byte),
     * name (null-terminated), type OID (4 bytes), atttypmod (4 bytes)
     */
    private static ByteBuffer buildRelationMessage(int relationId, String namespace, String relationName,
            String... columnNames) throws IOException {
        var out = new ByteArrayOutputStream();

        out.write('R'); // message type

        // relation ID (4 bytes, big-endian)
        out.write(toBytes(relationId));

        // namespace (null-terminated)
        out.write(namespace.getBytes(StandardCharsets.UTF_8));
        out.write(0);

        // relation name (null-terminated)
        out.write(relationName.getBytes(StandardCharsets.UTF_8));
        out.write(0);

        // replica identity (1 byte)
        out.write('d');

        // column count (2 bytes, big-endian)
        out.write((columnNames.length >> 8) & 0xFF);
        out.write(columnNames.length & 0xFF);

        // columns
        for (var columnName : columnNames) {
            out.write(0); // flags
            out.write(columnName.getBytes(StandardCharsets.UTF_8));
            out.write(0); // null terminator
            out.write(toBytes(20)); // type OID (20 = int8/bigint)
            out.write(toBytes(-1)); // atttypmod
        }

        var bytes = out.toByteArray();

        return ByteBuffer.wrap(bytes);
    }

    /**
     * Builds an INSERT message buffer with id as the first column.
     * Format: 'I' (1 byte), relationId (4 bytes), 'N' (1 byte for new tuple),
     * column count (2 bytes), for id column: 't' (1 byte), value length (4
     * bytes), value bytes
     */
    private static ByteBuffer buildInsertMessage(int relationId, String idValue) throws IOException {
        var out = new ByteArrayOutputStream();

        out.write('I'); // message type

        // relation ID (4 bytes, big-endian)
        out.write(toBytes(relationId));

        // 'N' for new tuple
        out.write('N');

        // column count (2 bytes, big-endian) - just 1 column (id)
        out.write(0);
        out.write(1);

        // id column
        out.write('t'); // text format
        var idBytes = idValue.getBytes(StandardCharsets.UTF_8);
        out.write(toBytes(idBytes.length));
        out.write(idBytes);

        var bytes = out.toByteArray();

        return ByteBuffer.wrap(bytes);
    }

    /**
     * Converts an int to 4 bytes in big-endian order.
     */
    private static byte[] toBytes(int value) {
        return ByteBuffer.allocate(4).order(ByteOrder.BIG_ENDIAN).putInt(value).array();
    }
}
