package com.yahoo.bullet.dsl.serializer.pulsar;

import com.yahoo.bullet.dsl.BulletDSLConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.schema.SchemaInfo;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;

/**
 * A custom Pulsar schema for serializing/deserializing Avro records.
 *
 * Used for only avro records. Not a traditional pulsar schema.
 *
 * Does not support SchemaInfo. needs testing.
 */
@Slf4j
public class AvroSchema implements Schema<GenericRecord> {

    public static final String AVRO_CLASS_NAME = "avro.class.name";

    private final DatumReader<GenericRecord> reader = new GenericDatumReader<>();
    private final DatumWriter<GenericRecord> writer = new GenericDatumWriter<>();
    private BinaryDecoder decoder;
    private BinaryEncoder encoder;

    /**
     * kinda needs a constructor...
     *
     * either the Avro class or bullet config as an arg
     */
    public AvroSchema(BulletDSLConfig config) {
        org.apache.avro.Schema schema = getSchemaFromClassName(config.getRequiredConfigAs(AVRO_CLASS_NAME, String.class));
        if (schema == null) {
            throw new RuntimeException("Could not find avro schema.");
        }
        reader.setSchema(schema);
        writer.setSchema(schema);
    }

    @SuppressWarnings("unchecked")
    private org.apache.avro.Schema getSchemaFromClassName(String className) {
        if (className == null) {
            return null;
        }
        try {
            Class<? extends GenericRecord> cls = (Class<? extends GenericRecord>) Class.forName(className);
            GenericRecord avro = cls.getConstructor().newInstance();
            return avro.getSchema();
        } catch (Exception e) {
            log.error("Could not get avro schema from class name: " + className, e);
            return null;
        }
    }

    @Override
    public byte[] encode(GenericRecord genericRecord) {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        encoder = EncoderFactory.get().binaryEncoder(outputStream, encoder);
        try {
            writer.write(genericRecord, encoder);
            encoder.flush();
            return outputStream.toByteArray();
        } catch (Exception e) {
            throw new SerializationException("Failed to serialize avro record.", e);
        }
    }

    @Override
    public GenericRecord decode(byte[] bytes) {
        ByteArrayInputStream inputStream = new ByteArrayInputStream(bytes);
        decoder = DecoderFactory.get().binaryDecoder(inputStream, decoder);
        try {
            return reader.read(null, decoder);
        } catch (Exception e) {
            throw new SerializationException("Failed to deserialize avro record.", e);
        }
    }

    @Override
    public SchemaInfo getSchemaInfo() {
        return null;
    }
}
