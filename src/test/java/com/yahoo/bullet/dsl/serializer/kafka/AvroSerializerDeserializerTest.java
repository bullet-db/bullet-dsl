/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.dsl.serializer.kafka;

import com.yahoo.bullet.dsl.DummyAvro;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.errors.SerializationException;
import org.junit.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Collections;

public class AvroSerializerDeserializerTest {
    private AvroSerializerDeserializer serdes;

    @BeforeMethod
    public void setup() {
        serdes = new AvroSerializerDeserializer();
    }

    @Test
    public void testWithClassName() {
        serdes.configure(Collections.singletonMap(AvroSerializerDeserializer.AVRO_CLASS_NAME, DummyAvro.class.getName()), false);

        DummyAvro dummyAvro = new DummyAvro();
        dummyAvro.setMyBool(true);
        dummyAvro.setMyInt(1);
        dummyAvro.setMyLong(2L);
        dummyAvro.setMyFloat(3.0f);
        dummyAvro.setMyDouble(4.0);
        dummyAvro.setMyString("5.0");

        DummyAvro another = new DummyAvro();
        another.setMyBool(false);
        another.setMyInt(2);
        another.setMyLong(3L);
        another.setMyFloat(4.0f);
        another.setMyDouble(5.0);
        another.setMyString("6.0");

        dummyAvro.setMyDummyAvro(another);

        byte[] bytes = serdes.serialize(null, dummyAvro);
        Assert.assertNotNull(bytes);

        GenericRecord message = serdes.deserialize(null, bytes);

        Assert.assertEquals(message.get("myBool"), true);
        Assert.assertEquals(message.get("myInt"), 1);
        Assert.assertEquals(message.get("myLong"), 2L);
        Assert.assertEquals(message.get("myFloat"), 3.0f);
        Assert.assertEquals(message.get("myDouble"), 4.0);
        Assert.assertEquals(message.get("myString").toString(), "5.0");
        Assert.assertNotNull(message.get("myDummyAvro"));

        GenericRecord myDummyAvro = (GenericRecord) message.get("myDummyAvro");
        Assert.assertEquals(myDummyAvro.get("myBool"), false);
        Assert.assertEquals(myDummyAvro.get("myInt"), 2);
        Assert.assertEquals(myDummyAvro.get("myLong"), 3L);
        Assert.assertEquals(myDummyAvro.get("myFloat"), 4.0f);
        Assert.assertEquals(myDummyAvro.get("myDouble"), 5.0);
        Assert.assertEquals(myDummyAvro.get("myString").toString(), "6.0");
        Assert.assertNull(myDummyAvro.get("myDummyAvro"));

        serdes.close();
    }

    @Test
    public void testWithSchemaFile() {
        serdes.configure(Collections.singletonMap(AvroSerializerDeserializer.AVRO_SCHEMA_FILE, "src/test/avro/DummyAvro.avsc"), false);

        DummyAvro dummyAvro = new DummyAvro();
        dummyAvro.setMyBool(true);
        dummyAvro.setMyInt(1);
        dummyAvro.setMyLong(2L);
        dummyAvro.setMyFloat(3.0f);
        dummyAvro.setMyDouble(4.0);
        dummyAvro.setMyString("5.0");

        DummyAvro another = new DummyAvro();
        another.setMyBool(false);
        another.setMyInt(2);
        another.setMyLong(3L);
        another.setMyFloat(4.0f);
        another.setMyDouble(5.0);
        another.setMyString("6.0");

        dummyAvro.setMyDummyAvro(another);

        byte[] bytes = serdes.serialize(null, dummyAvro);
        Assert.assertNotNull(bytes);

        GenericRecord message = serdes.deserialize(null, bytes);

        Assert.assertEquals(message.get("myBool"), true);
        Assert.assertEquals(message.get("myInt"), 1);
        Assert.assertEquals(message.get("myLong"), 2L);
        Assert.assertEquals(message.get("myFloat"), 3.0f);
        Assert.assertEquals(message.get("myDouble"), 4.0);
        Assert.assertEquals(message.get("myString").toString(), "5.0");
        Assert.assertNotNull(message.get("myDummyAvro"));

        GenericRecord myDummyAvro = (GenericRecord) message.get("myDummyAvro");
        Assert.assertEquals(myDummyAvro.get("myBool"), false);
        Assert.assertEquals(myDummyAvro.get("myInt"), 2);
        Assert.assertEquals(myDummyAvro.get("myLong"), 3L);
        Assert.assertEquals(myDummyAvro.get("myFloat"), 4.0f);
        Assert.assertEquals(myDummyAvro.get("myDouble"), 5.0);
        Assert.assertEquals(myDummyAvro.get("myString").toString(), "6.0");
        Assert.assertNull(myDummyAvro.get("myDummyAvro"));

        serdes.close();
    }

    @Test(expectedExceptions = RuntimeException.class, expectedExceptionsMessageRegExp = "Could not find avro schema\\.")
    public void testAvroSerializerDeserializerNoSchema() {
        serdes.configure(Collections.emptyMap(), false);
    }

    @Test(expectedExceptions = RuntimeException.class, expectedExceptionsMessageRegExp = "Could not find avro schema\\.")
    public void testConfigureMissingClass() {
        serdes.configure(Collections.singletonMap(AvroSerializerDeserializer.AVRO_CLASS_NAME, ""), false);
    }

    @Test(expectedExceptions = RuntimeException.class, expectedExceptionsMessageRegExp = "Could not find avro schema\\.")
    public void testConfigureMissingFile() {
        serdes.configure(Collections.singletonMap(AvroSerializerDeserializer.AVRO_SCHEMA_FILE, ""), false);
    }

    @Test(expectedExceptions = SerializationException.class, expectedExceptionsMessageRegExp = "Failed to serialize avro record\\.")
    public void testWrongAvroSerialize() {
        serdes.configure(Collections.singletonMap(AvroSerializerDeserializer.AVRO_SCHEMA_FILE, "src/test/avro/SmartAvro.avsc"), false);

        DummyAvro dummyAvro = new DummyAvro();
        dummyAvro.setMyBool(true);
        dummyAvro.setMyInt(1);
        dummyAvro.setMyLong(2L);
        dummyAvro.setMyFloat(3.0f);
        dummyAvro.setMyDouble(4.0);
        dummyAvro.setMyString("5.0");
        dummyAvro.setMyDummyAvro(null);

        serdes.serialize(null, dummyAvro);
    }

    @Test(expectedExceptions = SerializationException.class, expectedExceptionsMessageRegExp = "Failed to deserialize avro record\\.")
    public void testWrongAvroDeserialize() {
        serdes.configure(Collections.singletonMap(AvroSerializerDeserializer.AVRO_SCHEMA_FILE, "src/test/avro/DummyAvro.avsc"), false);

        DummyAvro dummyAvro = new DummyAvro();
        dummyAvro.setMyBool(true);
        dummyAvro.setMyInt(1);
        dummyAvro.setMyLong(2L);
        dummyAvro.setMyFloat(3.0f);
        dummyAvro.setMyDouble(4.0);
        dummyAvro.setMyString("5.0");

        DummyAvro another = new DummyAvro();
        another.setMyBool(false);
        another.setMyInt(2);
        another.setMyLong(3L);
        another.setMyFloat(4.0f);
        another.setMyDouble(5.0);
        another.setMyString("6.0");

        dummyAvro.setMyDummyAvro(another);

        byte[] bytes = serdes.serialize(null, dummyAvro);
        Assert.assertNotNull(bytes);

        serdes.configure(Collections.singletonMap(AvroSerializerDeserializer.AVRO_SCHEMA_FILE, "src/test/avro/SmartAvro.avsc"), false);

        serdes.deserialize(null, bytes);
    }
}
