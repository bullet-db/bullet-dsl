/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.dsl.serializer.pulsar;

import org.junit.Assert;
import org.testng.annotations.Test;

public class JavaSchemaTest {

    @Test
    public void testPulsarSchema() {
        JavaSchema schema = new JavaSchema();

        byte[] bytes = schema.encode("hello world!");
        Assert.assertNotNull(bytes);

        String message = (String) schema.decode(bytes);
        Assert.assertEquals(message, "hello world!");

        Assert.assertNull(schema.getSchemaInfo());
    }
}
