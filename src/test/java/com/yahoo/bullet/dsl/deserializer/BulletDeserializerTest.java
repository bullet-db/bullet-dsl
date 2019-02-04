/*
 *  Copyright 2019, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.dsl.deserializer;

import com.yahoo.bullet.dsl.BulletDSLConfig;
import org.testng.Assert;
import org.testng.annotations.Test;

public class BulletDeserializerTest {

    @Test
    public void testFrom() {
        BulletDSLConfig config = new BulletDSLConfig();
        config.set(BulletDSLConfig.DESERIALIZER_CLASS_NAME, JavaDeserializer.class.getName());

        Assert.assertTrue(BulletDeserializer.from(config) instanceof JavaDeserializer);
    }
}
