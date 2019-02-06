/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.dsl.serializer.pulsar;

import com.yahoo.bullet.common.SerializerDeserializer;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;

import java.io.Serializable;

/**
 * A custom Pulsar schema that uses Java serialization to encode/decode {@link Serializable} objects.
 */
public class JavaSchema implements Schema<Serializable> {

    private static final SchemaInfo SCHEMA_INFO = new SchemaInfo();

    static {
        SCHEMA_INFO.setName("JavaSchema");
        SCHEMA_INFO.setSchema(new byte[0]);
        SCHEMA_INFO.setType(SchemaType.NONE);
    }

    @Override
    public byte[] encode(Serializable message) {
        return SerializerDeserializer.toBytes(message);
    }

    @Override
    public Serializable decode(byte[] bytes) {
        return SerializerDeserializer.fromBytes(bytes);
    }

    @Override
    public SchemaInfo getSchemaInfo() {
        return SCHEMA_INFO;
    }
}
