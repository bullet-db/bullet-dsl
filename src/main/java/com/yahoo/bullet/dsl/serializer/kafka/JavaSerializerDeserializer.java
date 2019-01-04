/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.dsl.serializer.kafka;

import com.yahoo.bullet.common.SerializerDeserializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

import java.io.Serializable;
import java.util.Map;

/**
 * A Kafka Serializer/Deserializer that uses Java serialization to serialize/deserialize {@link Serializable} objects.
 */
public class JavaSerializerDeserializer implements Serializer<Serializable>, Deserializer<Serializable> {

    @Override
    public Serializable deserialize(String topic, byte[] data) {
        return SerializerDeserializer.fromBytes(data);
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
    }

    @Override
    public byte[] serialize(String topic, Serializable obj) {
        return SerializerDeserializer.toBytes(obj);
    }

    @Override
    public void close() {
    }
}
