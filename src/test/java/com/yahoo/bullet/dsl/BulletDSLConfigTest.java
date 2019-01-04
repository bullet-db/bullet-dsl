/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.dsl;

import org.testng.Assert;
import org.testng.annotations.Test;

public class BulletDSLConfigTest {

    @Test
    public void testConstructors() {
        BulletDSLConfig config = new BulletDSLConfig();
        Assert.assertEquals(config.get(BulletDSLConfig.RECORD_PROVIDER_CLASS_NAME), "com.yahoo.bullet.record.AvroBulletRecordProvider");

        config = new BulletDSLConfig("test_config.yaml");
        Assert.assertEquals(config.get(BulletDSLConfig.RECORD_PROVIDER_CLASS_NAME), "com.yahoo.bullet.record.SimpleBulletRecordProvider");
    }
}
