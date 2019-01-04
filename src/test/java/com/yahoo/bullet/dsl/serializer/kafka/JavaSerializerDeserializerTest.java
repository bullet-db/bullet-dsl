/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.dsl.serializer.kafka;

import org.junit.Assert;
import org.testng.annotations.Test;

public class JavaSerializerDeserializerTest {

    @Test
    public void testKafkaSerializerDeserializer() {
        JavaSerializerDeserializer serdes = new JavaSerializerDeserializer();

        // coverage
        serdes.configure(null, false);

        byte[] bytes = serdes.serialize(null, "hello world!");
        Assert.assertNotNull(bytes);

        String message = (String) serdes.deserialize(null, bytes);
        Assert.assertEquals(message, "hello world!");

        // coverage
        serdes.close();
    }
}
