/*
 *  Copyright 2018, Yahoo Inc.
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
        Assert.assertEquals(config.get(BulletDSLConfig.RECORD_PROVIDER_CLASS_NAME), "com.yahoo.bullet.record.avro.TypedAvroBulletRecordProvider");

        config = new BulletDSLConfig("test_config.yaml");
        Assert.assertEquals(config.get(BulletDSLConfig.RECORD_PROVIDER_CLASS_NAME), "com.yahoo.bullet.record.simple.TypedSimpleBulletRecordProvider");

        config = new BulletDSLConfig("src/main/resources/bullet_dsl_defaults.yaml");
        Assert.assertEquals(config.get(BulletDSLConfig.CONNECTOR_KAFKA_GROUP_ID), "bullet-consumer-group");
    }

    @Test(expectedExceptions = IllegalStateException.class)
    public void testAvroDeserializerFieldsNotPresent() {
        BulletDSLConfig config = new BulletDSLConfig();
        config.set(BulletDSLConfig.DESERIALIZER_CLASS_NAME, BulletDSLConfig.AVRO_DESERIALIZER_CLASS_NAME);
        config.validate();
    }

    @Test
    public void testAvroDeserializerSchemaFilePresent() {
        BulletDSLConfig config = new BulletDSLConfig();
        config.set(BulletDSLConfig.DESERIALIZER_CLASS_NAME, BulletDSLConfig.AVRO_DESERIALIZER_CLASS_NAME);
        config.set(BulletDSLConfig.DESERIALIZER_AVRO_SCHEMA_FILE, "");
        config.validate();
    }

    @Test
    public void testAvroDeserializerClassNamePresent() {
        BulletDSLConfig config = new BulletDSLConfig();
        config.set(BulletDSLConfig.DESERIALIZER_CLASS_NAME, BulletDSLConfig.AVRO_DESERIALIZER_CLASS_NAME);
        config.set(BulletDSLConfig.DESERIALIZER_AVRO_CLASS_NAME, DummyAvro.class.getName());
        config.validate();
    }

    @Test
    public void testReadFileAsResource() {
        BulletDSLConfig config = new BulletDSLConfig();
        config.set(BulletDSLConfig.DESERIALIZER_AVRO_SCHEMA_FILE, BulletDSLConfig.FILE_PREFIX + "schemas/empty.json");
        config.validate();
        Assert.assertEquals(config.getAs(BulletDSLConfig.DESERIALIZER_AVRO_SCHEMA_FILE, String.class), "{}");
    }

    @Test
    public void testReadFileAsPath() {
        BulletDSLConfig config = new BulletDSLConfig();
        config.set(BulletDSLConfig.DESERIALIZER_AVRO_SCHEMA_FILE, BulletDSLConfig.FILE_PREFIX + "src/test/resources/schemas/empty.json");
        config.validate();
        Assert.assertEquals(config.getAs(BulletDSLConfig.DESERIALIZER_AVRO_SCHEMA_FILE, String.class), "{}");
    }

    @Test(expectedExceptions = RuntimeException.class, expectedExceptionsMessageRegExp = "Could not read file: does-not-exist")
    public void testReadFileDoesNotExist() {
        BulletDSLConfig config = new BulletDSLConfig();
        config.set(BulletDSLConfig.DESERIALIZER_AVRO_SCHEMA_FILE, BulletDSLConfig.FILE_PREFIX + "does-not-exist");
        config.validate();
    }
}
