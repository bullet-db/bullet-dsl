/*
 *  Copyright 2019, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.dsl.deserializer;

import com.yahoo.bullet.common.BulletConfig;
import com.yahoo.bullet.dsl.BulletDSLConfig;
import com.yahoo.bullet.dsl.BulletDSLException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;

import java.io.ByteArrayInputStream;

/**
 * A {@link BulletDeserializer} that uses Avro deserialization.
 */
public class AvroDeserializer extends BulletDeserializer {

    private static final long serialVersionUID = 4832970047084142383L;
    private DatumReader<GenericRecord> reader;
    private BinaryDecoder decoder;

    /**
     * Constructs an AvroDeserializer from a given configuration.
     *
     * @param bulletConfig The configuration that specifies the avro schema file or record class.
     */
    public AvroDeserializer(BulletConfig bulletConfig) {
        super(bulletConfig);
    }

    private void initialize() throws BulletDSLException {
        String schemaFile = config.getAs(BulletDSLConfig.DESERIALIZER_AVRO_SCHEMA_FILE, String.class);
        String className = config.getAs(BulletDSLConfig.DESERIALIZER_AVRO_CLASS_NAME, String.class);
        Schema schema = schemaFile != null ? new Schema.Parser().parse(schemaFile) : getSchemaFromClassName(className);
        reader = new GenericDatumReader<>(schema);
    }

    @Override
    public Object deserialize(Object object) throws BulletDSLException {
        if (reader == null) {
            initialize();
        }
        return deserialize((byte[]) object);
    }

    private GenericRecord deserialize(byte[] bytes) throws BulletDSLException {
        ByteArrayInputStream inputStream = new ByteArrayInputStream(bytes);
        decoder = DecoderFactory.get().binaryDecoder(inputStream, decoder);
        try {
            return reader.read(null, decoder);
        } catch (Exception e) {
            throw new BulletDSLException("Failed to deserialize avro record.", e);
        }
    }

    @SuppressWarnings("unchecked")
    private Schema getSchemaFromClassName(String className) throws BulletDSLException {
        try {
            Class<? extends GenericRecord> cls = (Class<? extends GenericRecord>) Class.forName(className);
            GenericRecord avro = cls.getConstructor().newInstance();
            return avro.getSchema();
        } catch (Exception e) {
            throw new BulletDSLException("Could not get avro schema from class name: " + className, e);
        }
    }
}
