/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.dsl.serializer.kafka;

import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
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
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.InputStream;
import java.util.Map;

/**
 * A Kafka Serializer/Deserializer that serializes/deserializes avro records. <br><br>
 * This class requires that an avro record class or schema file be provided as properties in the BulletDSLConfig as either
 * "bullet.dsl.connector.kafka.avro.class.name" or "bullet.dsl.connector.kafka.avro.schema.file".
 * If both properties are present, only the avro record class will be used.
 */
@Slf4j
public class AvroSerializerDeserializer implements Serializer<GenericRecord>, Deserializer<GenericRecord> {

    public static final String AVRO_CLASS_NAME = "avro.class.name";
    public static final String AVRO_SCHEMA_FILE = "avro.schema.file";

    private final DatumReader<GenericRecord> reader = new GenericDatumReader<>();
    private final DatumWriter<GenericRecord> writer = new GenericDatumWriter<>();
    private BinaryDecoder decoder;
    private BinaryEncoder encoder;

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        Schema schema = getSchemaFromClassName((String) configs.get(AVRO_CLASS_NAME));
        if (schema == null) {
            schema = getSchemaFromFile((String) configs.get(AVRO_SCHEMA_FILE));
        }
        if (schema == null) {
            throw new RuntimeException("Could not find avro schema.");
        }
        reader.setSchema(schema);
        writer.setSchema(schema);
    }

    @SuppressWarnings("unchecked")
    private Schema getSchemaFromClassName(String className) {
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

    private Schema getSchemaFromFile(String file) {
        if (file == null) {
            return null;
        }
        try {
            InputStream is = this.getClass().getResourceAsStream("/" + file);
            return is != null ? new Schema.Parser().parse(is) : new Schema.Parser().parse(new File(file));
        } catch (Exception e) {
            log.error("Could not get avro schema from schema file: " + file, e);
            return null;
        }
    }

    @Override
    public GenericRecord deserialize(String topic, byte[] data) {
        ByteArrayInputStream inputStream = new ByteArrayInputStream(data);
        decoder = DecoderFactory.get().binaryDecoder(inputStream, decoder);
        try {
            return reader.read(null, decoder);
        } catch (Exception e) {
            throw new SerializationException("Failed to deserialize avro record.", e);
        }
    }

    @Override
    public byte[] serialize(String topic, GenericRecord genericRecord) {
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
    public void close() {
    }
}
