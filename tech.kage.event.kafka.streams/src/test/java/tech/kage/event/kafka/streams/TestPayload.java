package tech.kage.event.kafka.streams;

import org.apache.avro.message.BinaryMessageDecoder;
import org.apache.avro.message.BinaryMessageEncoder;
import org.apache.avro.message.SchemaStore;
import org.apache.avro.specific.SpecificData;

@org.apache.avro.specific.AvroGenerated
public class TestPayload extends org.apache.avro.specific.SpecificRecordBase {
    private static final long serialVersionUID = -9108752311682307236L;

    public static final org.apache.avro.Schema SCHEMA$ = new org.apache.avro.Schema.Parser().parse(
            "{\"type\":\"record\",\"name\":\"TestPayload\",\"namespace\":\"tech.kage.event.kafka.streams\",\"fields\":[{\"name\":\"text\",\"type\":{\"type\":\"string\",\"avro.java.string\":\"String\"}}]}");

    public static org.apache.avro.Schema getClassSchema() {
        return SCHEMA$;
    }

    private static final SpecificData MODEL$ = new SpecificData();

    private static final BinaryMessageEncoder<TestPayload> ENCODER = new BinaryMessageEncoder<>(MODEL$, SCHEMA$);

    private static final BinaryMessageDecoder<TestPayload> DECODER = new BinaryMessageDecoder<>(MODEL$, SCHEMA$);

    /**
     * Return the BinaryMessageEncoder instance used by this class.
     * 
     * @return the message encoder used by this class
     */
    public static BinaryMessageEncoder<TestPayload> getEncoder() {
        return ENCODER;
    }

    /**
     * Return the BinaryMessageDecoder instance used by this class.
     * 
     * @return the message decoder used by this class
     */
    public static BinaryMessageDecoder<TestPayload> getDecoder() {
        return DECODER;
    }

    /**
     * Create a new BinaryMessageDecoder instance for this class that uses the
     * specified {@link SchemaStore}.
     * 
     * @param resolver a {@link SchemaStore} used to find schemas by fingerprint
     * @return a BinaryMessageDecoder instance for this class backed by the given
     *         SchemaStore
     */
    public static BinaryMessageDecoder<TestPayload> createDecoder(SchemaStore resolver) {
        return new BinaryMessageDecoder<>(MODEL$, SCHEMA$, resolver);
    }

    /**
     * Serializes this TestPayload to a ByteBuffer.
     * 
     * @return a buffer holding the serialized data for this instance
     * @throws java.io.IOException if this instance could not be serialized
     */
    public java.nio.ByteBuffer toByteBuffer() throws java.io.IOException {
        return ENCODER.encode(this);
    }

    /**
     * Deserializes a TestPayload from a ByteBuffer.
     * 
     * @param b a byte buffer holding serialized data for an instance of this class
     * @return a TestPayload instance decoded from the given buffer
     * @throws java.io.IOException if the given bytes could not be deserialized into
     *                             an instance of this class
     */
    public static TestPayload fromByteBuffer(
            java.nio.ByteBuffer b) throws java.io.IOException {
        return DECODER.decode(b);
    }

    private java.lang.String text;

    /**
     * Default constructor. Note that this does not initialize fields
     * to their default values from the schema. If that is desired then
     * one should use <code>newBuilder()</code>.
     */
    public TestPayload() {
    }

    /**
     * All-args constructor.
     * 
     * @param text The new value for text
     */
    public TestPayload(java.lang.String text) {
        this.text = text;
    }

    @Override
    public org.apache.avro.specific.SpecificData getSpecificData() {
        return MODEL$;
    }

    @Override
    public org.apache.avro.Schema getSchema() {
        return SCHEMA$;
    }

    // Used by DatumWriter. Applications should not call.
    @Override
    public java.lang.Object get(int field$) {
        switch (field$) {
            case 0:
                return text;
            default:
                throw new IndexOutOfBoundsException("Invalid index: " + field$);
        }
    }

    // Used by DatumReader. Applications should not call.
    @Override
    public void put(int field$, java.lang.Object value$) {
        switch (field$) {
            case 0:
                text = value$ != null ? value$.toString() : null;
                break;
            default:
                throw new IndexOutOfBoundsException("Invalid index: " + field$);
        }
    }

    /**
     * Gets the value of the 'text' field.
     * 
     * @return The value of the 'text' field.
     */
    public java.lang.String getText() {
        return text;
    }

    /**
     * Sets the value of the 'text' field.
     * 
     * @param value the value to set.
     */
    public void setText(java.lang.String value) {
        this.text = value;
    }

    /**
     * Creates a new TestPayload RecordBuilder.
     * 
     * @return A new TestPayload RecordBuilder
     */
    public static tech.kage.event.kafka.streams.TestPayload.Builder newBuilder() {
        return new tech.kage.event.kafka.streams.TestPayload.Builder();
    }

    /**
     * Creates a new TestPayload RecordBuilder by copying an existing Builder.
     * 
     * @param other The existing builder to copy.
     * @return A new TestPayload RecordBuilder
     */
    public static tech.kage.event.kafka.streams.TestPayload.Builder newBuilder(
            tech.kage.event.kafka.streams.TestPayload.Builder other) {
        if (other == null) {
            return new tech.kage.event.kafka.streams.TestPayload.Builder();
        } else {
            return new tech.kage.event.kafka.streams.TestPayload.Builder(other);
        }
    }

    /**
     * Creates a new TestPayload RecordBuilder by copying an existing TestPayload
     * instance.
     * 
     * @param other The existing instance to copy.
     * @return A new TestPayload RecordBuilder
     */
    public static tech.kage.event.kafka.streams.TestPayload.Builder newBuilder(
            tech.kage.event.kafka.streams.TestPayload other) {
        if (other == null) {
            return new tech.kage.event.kafka.streams.TestPayload.Builder();
        } else {
            return new tech.kage.event.kafka.streams.TestPayload.Builder(other);
        }
    }

    /**
     * RecordBuilder for TestPayload instances.
     */
    @org.apache.avro.specific.AvroGenerated
    public static class Builder extends org.apache.avro.specific.SpecificRecordBuilderBase<TestPayload> {

        private java.lang.String text;

        /** Creates a new Builder */
        private Builder() {
            super(SCHEMA$, MODEL$);
        }

        /**
         * Creates a Builder by copying an existing Builder.
         * 
         * @param other The existing Builder to copy.
         */
        private Builder(tech.kage.event.kafka.streams.TestPayload.Builder other) {
            super(other);
            if (isValidValue(fields()[0], other.text)) {
                this.text = data().deepCopy(fields()[0].schema(), other.text);
                fieldSetFlags()[0] = other.fieldSetFlags()[0];
            }
        }

        /**
         * Creates a Builder by copying an existing TestPayload instance
         * 
         * @param other The existing instance to copy.
         */
        private Builder(tech.kage.event.kafka.streams.TestPayload other) {
            super(SCHEMA$, MODEL$);
            if (isValidValue(fields()[0], other.text)) {
                this.text = data().deepCopy(fields()[0].schema(), other.text);
                fieldSetFlags()[0] = true;
            }
        }

        /**
         * Gets the value of the 'text' field.
         * 
         * @return The value.
         */
        public java.lang.String getText() {
            return text;
        }

        /**
         * Sets the value of the 'text' field.
         * 
         * @param value The value of 'text'.
         * @return This builder.
         */
        public tech.kage.event.kafka.streams.TestPayload.Builder setText(java.lang.String value) {
            validate(fields()[0], value);
            this.text = value;
            fieldSetFlags()[0] = true;
            return this;
        }

        /**
         * Checks whether the 'text' field has been set.
         * 
         * @return True if the 'text' field has been set, false otherwise.
         */
        public boolean hasText() {
            return fieldSetFlags()[0];
        }

        /**
         * Clears the value of the 'text' field.
         * 
         * @return This builder.
         */
        public tech.kage.event.kafka.streams.TestPayload.Builder clearText() {
            text = null;
            fieldSetFlags()[0] = false;
            return this;
        }

        @Override
        public TestPayload build() {
            try {
                TestPayload payload = new TestPayload();
                payload.text = fieldSetFlags()[0] ? this.text : (java.lang.String) defaultValue(fields()[0]);
                return payload;
            } catch (org.apache.avro.AvroMissingFieldException e) {
                throw e;
            } catch (java.lang.Exception e) {
                throw new org.apache.avro.AvroRuntimeException(e);
            }
        }
    }

    @SuppressWarnings("unchecked")
    private static final org.apache.avro.io.DatumWriter<TestPayload> WRITER$ = (org.apache.avro.io.DatumWriter<TestPayload>) MODEL$
            .createDatumWriter(SCHEMA$);

    @Override
    public void writeExternal(java.io.ObjectOutput out)
            throws java.io.IOException {
        WRITER$.write(this, SpecificData.getEncoder(out));
    }

    @SuppressWarnings("unchecked")
    private static final org.apache.avro.io.DatumReader<TestPayload> READER$ = (org.apache.avro.io.DatumReader<TestPayload>) MODEL$
            .createDatumReader(SCHEMA$);

    @Override
    public void readExternal(java.io.ObjectInput in)
            throws java.io.IOException {
        READER$.read(this, SpecificData.getDecoder(in));
    }

    @Override
    protected boolean hasCustomCoders() {
        return true;
    }

    @Override
    public void customEncode(org.apache.avro.io.Encoder out)
            throws java.io.IOException {
        out.writeString(this.text);

    }

    @Override
    public void customDecode(org.apache.avro.io.ResolvingDecoder in)
            throws java.io.IOException {
        org.apache.avro.Schema.Field[] fieldOrder = in.readFieldOrderIfDiff();
        if (fieldOrder == null) {
            this.text = in.readString();

        } else {
            for (int i = 0; i < 1; i++) {
                switch (fieldOrder[i].pos()) {
                    case 0:
                        this.text = in.readString();
                        break;

                    default:
                        throw new java.io.IOException("Corrupt ResolvingDecoder.");
                }
            }
        }
    }
}
