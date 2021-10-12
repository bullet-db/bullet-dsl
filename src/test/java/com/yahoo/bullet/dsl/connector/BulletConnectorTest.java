/*
 *  Copyright 2018, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.dsl.connector;

import com.yahoo.bullet.dsl.BulletDSLConfig;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class BulletConnectorTest {

    private BulletDSLConfig config;

    @BeforeMethod
    public void init() {
        config = new BulletDSLConfig("test_connector_config.yaml");
    }

    @Test
    public void testFromKafkaConnector() throws Exception {
        config.set(BulletDSLConfig.CONNECTOR_CLASS_NAME, "com.yahoo.bullet.dsl.connector.KafkaConnector");

        BulletConnector connector = BulletConnector.from(config);
        Assert.assertTrue(connector instanceof KafkaConnector);
    }

    @Test
    public void testFromPulsarConnector() {
        config.set(BulletDSLConfig.CONNECTOR_CLASS_NAME, "com.yahoo.bullet.dsl.connector.PulsarConnector");

        BulletConnector connector = BulletConnector.from(config);
        Assert.assertTrue(connector instanceof PulsarConnector);
    }

    @Test(expectedExceptions = RuntimeException.class)
    public void testThrow() {
        BulletConnector.from(config);
    }
}
