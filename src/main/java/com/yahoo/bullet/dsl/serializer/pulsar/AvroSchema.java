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
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.InputStream;

/**
 * A custom Pulsar schema for serializing/deserializing Avro records.
 */
@Slf4j
public class AvroSchema implements Schema<GenericRecord> {

    public static final String AVRO_CLASS_NAME = "bullet.dsl.connector.pulsar.avro.class.name";
    public static final String AVRO_SCHEMA_FILE = "bullet.dsl.connector.pulsar.avro.schema.file";

    private static final SchemaInfo SCHEMA_INFO = new SchemaInfo();

    static {
        SCHEMA_INFO.setName("AvroSchema");
        SCHEMA_INFO.setSchema(new byte[0]);
        SCHEMA_INFO.setType(SchemaType.NONE);
    }

    private final DatumReader<GenericRecord> reader = new GenericDatumReader<>();
    private final DatumWriter<GenericRecord> writer = new GenericDatumWriter<>();
    private BinaryDecoder decoder;
    private BinaryEncoder encoder;

    /**
     * Creates a Schema for serializing/deserializing Avro records with the given schema.
     *
     * @param config The config to get the Avro schema from.
     */
    public AvroSchema(BulletDSLConfig config) {
        org.apache.avro.Schema schema = getSchemaFromClassName(config.getAs(AVRO_CLASS_NAME, String.class));
        if (schema == null) {
            schema = getSchemaFromFile(config.getAs(AVRO_SCHEMA_FILE, String.class));
        }
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

    private org.apache.avro.Schema getSchemaFromFile(String file) {
        if (file == null) {
            return null;
        }
        try {
            InputStream is = this.getClass().getResourceAsStream("/" + file);
            return is != null ? new org.apache.avro.Schema.Parser().parse(is) : new org.apache.avro.Schema.Parser().parse(new File(file));
        } catch (Exception e) {
            log.error("Could not get avro schema from schema file: " + file, e);
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
            throw new RuntimeException("Failed to serialize avro record.", e);
        }
    }

    @Override
    public GenericRecord decode(byte[] bytes) {
        ByteArrayInputStream inputStream = new ByteArrayInputStream(bytes);
        decoder = DecoderFactory.get().binaryDecoder(inputStream, decoder);
        try {
            return reader.read(null, decoder);
        } catch (Exception e) {
            throw new RuntimeException("Failed to deserialize avro record.", e);
        }
    }

    @Override
    public SchemaInfo getSchemaInfo() {
        return SCHEMA_INFO;
    }
}
