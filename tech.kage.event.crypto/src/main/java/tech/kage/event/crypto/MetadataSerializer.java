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

import static java.util.stream.Collectors.toMap;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.SequencedMap;
import java.util.TreeMap;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.util.Utf8;

import tech.kage.event.Event;

/**
 * Serializer/deserializer of {@link Event}'s metadata to/from an Avro map.
 * 
 * @author Dariusz Szpakowski
 */
public class MetadataSerializer {
    private static final Schema METADATA_SCHEMA = SchemaBuilder.map().values().bytesType();

    private static final EncoderFactory encoderFactory = EncoderFactory.get();
    private static final DatumWriter<Map<String, ByteBuffer>> writer = new GenericDatumWriter<>(METADATA_SCHEMA);

    private static final DecoderFactory decoderFactory = DecoderFactory.get();
    private static final DatumReader<SequencedMap<Utf8, ByteBuffer>> reader = new GenericDatumReader<>(METADATA_SCHEMA);

    private MetadataSerializer() {
        // hide the default constructor
    }

    /**
     * Serialize the given metadata map to an Avro map.
     * 
     * @param metadata metadata map to serialize
     * 
     * @return bytes representing the serialized Avro map
     */
    public static byte[] serialize(Map<String, Object> metadata) {
        var convertedSortedMetadata = metadata
                .entrySet()
                .stream()
                .map(entry -> Map.entry(entry.getKey(), ByteBuffer.wrap((byte[]) entry.getValue())))
                .collect(toMap(Map.Entry::getKey, Map.Entry::getValue, (e1, e2) -> e1, TreeMap::new)); // sorted by key

        try (var out = new ByteArrayOutputStream()) {
            var encoder = encoderFactory.directBinaryEncoder(out, null);

            writer.write(convertedSortedMetadata, encoder);

            return out.toByteArray();
        } catch (IOException e) {
            throw new UncheckedIOException("Unable to serialize metadata: " + metadata, e);
        }
    }

    /**
     * Deserialize the given Avro map to event metadata.
     * 
     * @param metadata bytes representing the serialized Avro map with metadata
     * 
     * @return deserialized metadata map keeping the input order
     */
    public static SequencedMap<String, Object> deserialize(byte[] metadata) {
        try {
            var decoder = decoderFactory.binaryDecoder(metadata, null);

            return reader
                    .read(new LinkedHashMap<>(), decoder) // use LinkedHashMap to keep the order of metadata items
                    .entrySet()
                    .stream()
                    .map(entry -> Map.entry(entry.getKey().toString(), entry.getValue().array()))
                    .collect(toMap(Map.Entry::getKey, Map.Entry::getValue, (e1, e2) -> e1, LinkedHashMap::new));
        } catch (IOException e) {
            throw new UncheckedIOException("Unable to deserialize metadata: " + metadata, e);
        }
    }
}
