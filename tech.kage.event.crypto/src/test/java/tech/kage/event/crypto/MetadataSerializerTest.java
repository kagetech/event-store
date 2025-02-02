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

package tech.kage.event.crypto;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

import org.junit.jupiter.api.Test;

/**
 * Tests of {@link MetadataSerializer}.
 * 
 * @author Dariusz Szpakowski
 */
class MetadataSerializerTest {
    @Test
    void readsSerializedData() {
        // Given
        var metadata = Map.<String, Object>of(
                "dTest", "meta_value".getBytes(),
                "zTest", UUID.fromString("788ee0da-3ca9-4fa1-9d84-3470a067d695").toString().getBytes(),
                "bTest", "1".getBytes());

        var serializedMetadata = MetadataSerializer.serialize(metadata);

        // When
        var deserializedMetadata = MetadataSerializer.deserialize(serializedMetadata);

        // Then
        assertThat(deserializedMetadata)
                .describedAs("deserialized metadata")
                .containsAllEntriesOf(metadata);
    }

    @Test
    void sortsDataDuringSerialization() {
        // Given
        var metadata = Map.<String, Object>of(
                "dTest", "meta_value".getBytes(),
                "zTest", UUID.fromString("788ee0da-3ca9-4fa1-9d84-3470a067d695").toString().getBytes(),
                "bTest", "1".getBytes());

        var expectedMetadata = new LinkedHashMap<String, Object>();

        expectedMetadata.put("bTest", "1".getBytes());
        expectedMetadata.put("dTest", "meta_value".getBytes());
        expectedMetadata.put("zTest", UUID.fromString("788ee0da-3ca9-4fa1-9d84-3470a067d695").toString().getBytes());

        // When
        var serializedMetadata = MetadataSerializer.serialize(metadata);

        // Then
        var deserializedMetadata = MetadataSerializer.deserialize(serializedMetadata);

        assertThat(deserializedMetadata)
                .describedAs("deserialized metadata")
                .containsExactlyEntriesOf(expectedMetadata);
    }
}
