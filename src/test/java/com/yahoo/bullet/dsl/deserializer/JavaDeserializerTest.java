/*
 *  Copyright 2019, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.dsl.deserializer;

import com.yahoo.bullet.common.SerializerDeserializer;
import com.yahoo.bullet.dsl.BulletDSLConfig;
import org.junit.Assert;
import org.testng.annotations.Test;

public class JavaDeserializerTest {

    @Test
    public void testDeserialize() {
        JavaDeserializer deserializer = new JavaDeserializer(new BulletDSLConfig());

        byte[] bytes = SerializerDeserializer.toBytes("hello world!");
        Assert.assertNotNull(bytes);

        String message = (String) deserializer.deserialize(bytes);
        Assert.assertEquals(message, "hello world!");
    }
}
