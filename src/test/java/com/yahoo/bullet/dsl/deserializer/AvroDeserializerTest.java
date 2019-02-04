/*
 *  Copyright 2019, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.dsl.deserializer;

import com.yahoo.bullet.dsl.BulletDSLConfig;
import com.yahoo.bullet.dsl.BulletDSLException;
import com.yahoo.bullet.dsl.ListsAvro;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.ByteArrayOutputStream;
import java.util.Collections;

public class AvroDeserializerTest {

    private ListsAvro listsAvro = new ListsAvro();
    private byte[] listsAvroBytes;

    @BeforeClass
    public void setup() throws Exception {
        listsAvro.setMyBoolList(Collections.singletonList(false));
        listsAvro.setMyIntList(Collections.singletonList(14));
        listsAvro.setMyLongList(Collections.singletonList(15L));
        listsAvro.setMyFloatList(Collections.singletonList(16.0f));
        listsAvro.setMyDoubleList(Collections.singletonList(17.0));
        listsAvro.setMyStringList(Collections.singletonList("18"));
        listsAvro.setMyBoolMapList(Collections.singletonList(Collections.singletonMap("s", true)));
        listsAvro.setMyIntMapList(Collections.singletonList(Collections.singletonMap("t", 20)));
        listsAvro.setMyLongMapList(Collections.singletonList(Collections.singletonMap("u", 21L)));
        listsAvro.setMyFloatMapList(Collections.singletonList(Collections.singletonMap("v", 22.0f)));
        listsAvro.setMyDoubleMapList(Collections.singletonList(Collections.singletonMap("w", 23.0)));
        listsAvro.setMyStringMapList(Collections.singletonList(Collections.singletonMap("x", "24")));

        DatumWriter<GenericRecord> writer = new GenericDatumWriter<>(ListsAvro.getClassSchema());
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(outputStream, null);
        writer.write(listsAvro, encoder);
        encoder.flush();

        listsAvroBytes = outputStream.toByteArray();
    }

    @Test
    public void testDeserializeWithSchemaFile() throws Exception {
        BulletDSLConfig config = new BulletDSLConfig();
        config.set(BulletDSLConfig.DESERIALIZER_AVRO_SCHEMA_FILE, BulletDSLConfig.FILE_PREFIX + "src/test/avro/ListsAvro.avsc");

        AvroDeserializer deserializer = new AvroDeserializer(config);

        GenericRecord record = (GenericRecord) deserializer.deserialize(listsAvroBytes);

        Assert.assertEquals(record.get("myBoolList"), listsAvro.getMyBoolList());
        Assert.assertEquals(record.get("myIntList"), listsAvro.getMyIntList());
        Assert.assertEquals(record.get("myLongList"), listsAvro.getMyLongList());
        Assert.assertEquals(record.get("myFloatList"), listsAvro.getMyFloatList());
        Assert.assertEquals(record.get("myDoubleList"), listsAvro.getMyDoubleList());
        Assert.assertEquals(record.get("myStringList"), listsAvro.getMyStringList());
        Assert.assertEquals(record.get("myBoolMapList"), listsAvro.getMyBoolMapList());
        Assert.assertEquals(record.get("myIntMapList"), listsAvro.getMyIntMapList());
        Assert.assertEquals(record.get("myLongMapList"), listsAvro.getMyLongMapList());
        Assert.assertEquals(record.get("myFloatMapList"), listsAvro.getMyFloatMapList());
        Assert.assertEquals(record.get("myDoubleMapList"), listsAvro.getMyDoubleMapList());
        Assert.assertEquals(record.get("myStringMapList"), listsAvro.getMyStringMapList());
        Assert.assertNull(record.get("dne"));

        // branch coverage
        deserializer.deserialize(listsAvroBytes);
    }

    @Test
    public void testDeserializeWithClassName() throws Exception {
        BulletDSLConfig config = new BulletDSLConfig();
        config.set(BulletDSLConfig.DESERIALIZER_AVRO_CLASS_NAME, ListsAvro.class.getName());

        AvroDeserializer deserializer = new AvroDeserializer(config);

        GenericRecord record = (GenericRecord) deserializer.deserialize(listsAvroBytes);

        Assert.assertEquals(record.get("myBoolList"), listsAvro.getMyBoolList());
        Assert.assertEquals(record.get("myIntList"), listsAvro.getMyIntList());
        Assert.assertEquals(record.get("myLongList"), listsAvro.getMyLongList());
        Assert.assertEquals(record.get("myFloatList"), listsAvro.getMyFloatList());
        Assert.assertEquals(record.get("myDoubleList"), listsAvro.getMyDoubleList());
        Assert.assertEquals(record.get("myStringList"), listsAvro.getMyStringList());
        Assert.assertEquals(record.get("myBoolMapList"), listsAvro.getMyBoolMapList());
        Assert.assertEquals(record.get("myIntMapList"), listsAvro.getMyIntMapList());
        Assert.assertEquals(record.get("myLongMapList"), listsAvro.getMyLongMapList());
        Assert.assertEquals(record.get("myFloatMapList"), listsAvro.getMyFloatMapList());
        Assert.assertEquals(record.get("myDoubleMapList"), listsAvro.getMyDoubleMapList());
        Assert.assertEquals(record.get("myStringMapList"), listsAvro.getMyStringMapList());
        Assert.assertNull(record.get("dne"));

        // branch coverage
        deserializer.deserialize(listsAvroBytes);
    }

    @Test(expectedExceptions = BulletDSLException.class, expectedExceptionsMessageRegExp = "Could not get avro schema from class name: .*")
    public void testDeserializeWithBadClassName() throws Exception {
        BulletDSLConfig config = new BulletDSLConfig();
        config.set(BulletDSLConfig.DESERIALIZER_AVRO_CLASS_NAME, AvroDeserializerTest.class.getName());

        AvroDeserializer deserializer = new AvroDeserializer(config);
        deserializer.deserialize(null);
    }

    @Test(expectedExceptions = BulletDSLException.class, expectedExceptionsMessageRegExp = "Failed to deserialize avro record\\.")
    public void testDeserializeWrongAvro() throws Exception {
        BulletDSLConfig config = new BulletDSLConfig();
        config.set(BulletDSLConfig.DESERIALIZER_AVRO_SCHEMA_FILE, BulletDSLConfig.FILE_PREFIX + "src/test/avro/DummyAvro.avsc");

        AvroDeserializer deserializer = new AvroDeserializer(config);

        deserializer.deserialize(listsAvroBytes);
    }
}
