/*
 *  Copyright 2019, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.dsl.deserializer;

import org.testng.Assert;
import org.testng.annotations.Test;

public class IdentityDeserializerTest {
    @Test
    public void testDeserialize() {
        IdentityDeserializer deserializer = new IdentityDeserializer(null);
        Assert.assertEquals(deserializer.deserialize("foo"), "foo");
        Assert.assertEquals(deserializer.deserialize(1L), 1L);
    }
}
