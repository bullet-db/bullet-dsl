/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.dsl.converter;

import com.yahoo.bullet.dsl.BulletDSLConfig;
import com.yahoo.bullet.dsl.BulletDSLException;
import com.yahoo.bullet.dsl.DummyAvro;
import com.yahoo.bullet.dsl.ListsAvro;
import com.yahoo.bullet.dsl.MapsAvro;
import com.yahoo.bullet.record.BulletRecord;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AvroBulletRecordConverterTest {

    @Test
    public void testConvertWithoutSchema() throws Exception {
        DummyAvro dummyAvro = new DummyAvro();
        dummyAvro.setMyBool(true);
        dummyAvro.setMyDouble(0.12);
        dummyAvro.setMyFloat(3.45f);
        dummyAvro.setMyInt(678);
        dummyAvro.setMyIntList(Collections.singletonList(910));
        dummyAvro.setMyLong(1112L);
        dummyAvro.setMyString("1314");
        dummyAvro.setMyStringMap(Collections.singletonMap("1516", "1718"));
        dummyAvro.setMyBytes(ByteBuffer.allocate(1));
        dummyAvro.setMyDummyAvro(new DummyAvro());

        AvroBulletRecordConverter recordConverter = new AvroBulletRecordConverter();
        BulletRecord record = recordConverter.convert(dummyAvro);

        Assert.assertEquals(record.get("myBool"), dummyAvro.getMyBool());
        Assert.assertEquals(record.get("myDouble"), dummyAvro.getMyDouble());
        Assert.assertEquals(record.get("myFloat"), dummyAvro.getMyFloat());
        Assert.assertEquals(record.get("myInt"), dummyAvro.getMyInt());
        Assert.assertEquals(record.get("myIntList"), dummyAvro.getMyIntList());
        Assert.assertEquals(record.get("myLong"), dummyAvro.getMyLong());
        Assert.assertEquals(record.get("myString"), dummyAvro.getMyString());
        Assert.assertEquals(record.get("myStringMap"), dummyAvro.getMyStringMap());

        // no type-checking without schema
        Assert.assertNotNull(record.get("myBytes"));
        Assert.assertNotNull(record.get("myDummyAvro"));
    }

    @Test
    public void testSchema() throws Exception {
        AvroBulletRecordConverter recordConverter = new AvroBulletRecordConverter("schemas/dummy.json");

        DummyAvro dummyAvro = new DummyAvro();
        dummyAvro.setMyBool(true);
        dummyAvro.setMyLong(10L);
        dummyAvro.setMyFloat(20.0f);
        dummyAvro.setMyDouble(30.0);

        BulletRecord record = recordConverter.convert(dummyAvro);
        Assert.assertEquals(record.get("myBool"), true);
        Assert.assertEquals(record.get("myLong"), 10L);
        Assert.assertEquals(record.get("myFloat"), 20.0f);
        Assert.assertEquals(record.get("myDouble"), 30.0);
    }

    @Test(expectedExceptions = BulletDSLException.class, expectedExceptionsMessageRegExp = "Could not convert field: \\{name: myBytes, reference: myBytes, type: BOOLEAN, subtype: null\\}")
    public void testSchemaWrongType() throws Exception {
        AvroBulletRecordConverter recordConverter = new AvroBulletRecordConverter("schemas/dummy.json");

        DummyAvro dummyAvro = new DummyAvro();
        dummyAvro.setMyBytes(ByteBuffer.allocate(1));

        recordConverter.convert(dummyAvro);
    }

    @Test
    public void testSchemaExtractFromList() throws Exception {
        AvroBulletRecordConverter recordConverter = new AvroBulletRecordConverter("schemas/dummy.json");

        List<Integer> myIntList = Arrays.asList(0, 1, 2, 3, 4);

        DummyAvro dummyAvro = new DummyAvro();
        dummyAvro.setMyIntList(myIntList);

        BulletRecord record = recordConverter.convert(dummyAvro);

        // reference -> myIntList.2
        Assert.assertEquals(record.get("bbb"), myIntList.get(2));
    }

    @Test
    public void testSchemaExtractFromMap() throws Exception {
        AvroBulletRecordConverter recordConverter = new AvroBulletRecordConverter("schemas/dummy.json");

        DummyAvro dummyAvro = new DummyAvro();
        dummyAvro.setMyStringMap(Collections.singletonMap("aaa", "hello"));

        BulletRecord record = recordConverter.convert(dummyAvro);

        Assert.assertEquals(record.get("myStringMap"), dummyAvro.getMyStringMap());
        Assert.assertEquals(record.get("aaa"), "hello");
    }

    @Test
    public void testSchemaExtractFromAvroRecord() throws Exception {
        // test config constructor while we're at it
        BulletDSLConfig config = new BulletDSLConfig();
        config.set(BulletDSLConfig.RECORD_CONVERTER_SCHEMA_FILE, "schemas/dummy.json");

        AvroBulletRecordConverter recordConverter = new AvroBulletRecordConverter(config);

        DummyAvro dummyAvro = new DummyAvro();
        DummyAvro another = new DummyAvro();
        another.setMyInt(100);
        dummyAvro.setMyDummyAvro(another);

        BulletRecord record = recordConverter.convert(dummyAvro);

        Assert.assertEquals(record.get("myDummyInt"), 100);
    }

    @Test(expectedExceptions = BulletDSLException.class, expectedExceptionsMessageRegExp = "Could not convert field: \\{name: null, reference: myIntList, type: RECORD, subtype: null\\}")
    public void testSchemaRecordFromNotRecord() throws Exception {
        AvroBulletRecordConverter recordConverter = new AvroBulletRecordConverter("schemas/dummyrecord.json");

        DummyAvro dummyAvro = new DummyAvro();
        dummyAvro.setMyIntList(Collections.emptyList());

        recordConverter.convert(dummyAvro);
    }

    @Test
    public void testSchemaRecordFromMap() throws Exception {
        AvroBulletRecordConverter recordConverter = new AvroBulletRecordConverter("schemas/dummyrecord.json");

        Map<String, String> myStringMap = new HashMap<>();
        myStringMap.put("aaa", "hello");
        myStringMap.put("bbb", "world");
        myStringMap.put("ccc", "!");
        myStringMap.put("ddd", null);

        DummyAvro dummyAvro = new DummyAvro();
        dummyAvro.setMyStringMap(myStringMap);

        BulletRecord record = recordConverter.convert(dummyAvro);
        Assert.assertEquals(record.fieldCount(), 3);
        Assert.assertEquals(record.get("aaa"), "hello");
        Assert.assertEquals(record.get("bbb"), "world");
        Assert.assertEquals(record.get("ccc"), "!");
    }

    @Test
    public void testSchemaRecordFromAvroRecord() throws Exception {
        AvroBulletRecordConverter recordConverter = new AvroBulletRecordConverter("schemas/dummyrecord.json");

        DummyAvro another = new DummyAvro();
        another.setMyBool(true);
        another.setMyInt(1);
        another.setMyLong(2L);
        another.setMyFloat(3.0f);
        another.setMyDouble(4.0);
        another.setMyString("5.0");
        another.setMyDummyAvro(new DummyAvro()); // not safe but force set when using RECORD

        DummyAvro dummyAvro = new DummyAvro();
        dummyAvro.setMyDummyAvro(another);

        BulletRecord record = recordConverter.convert(dummyAvro);
        Assert.assertEquals(record.fieldCount(), 7);
        Assert.assertEquals(record.get("myBool"), true);
        Assert.assertEquals(record.get("myInt"), 1);
        Assert.assertEquals(record.get("myLong"), 2L);
        Assert.assertEquals(record.get("myFloat"), 3.0f);
        Assert.assertEquals(record.get("myDouble"), 4.0);
        Assert.assertEquals(record.get("myString"), "5.0");
        Assert.assertTrue(record.get("myDummyAvro") instanceof DummyAvro);
    }

    @Test
    public void testMaps() throws Exception {
        MapsAvro mapsAvro = new MapsAvro();
        mapsAvro.setMyBoolMap(Collections.singletonMap("a", false));
        mapsAvro.setMyIntMap(Collections.singletonMap("b", 2));
        mapsAvro.setMyLongMap(Collections.singletonMap("c", 3L));
        mapsAvro.setMyFloatMap(Collections.singletonMap("d", 4.0f));
        mapsAvro.setMyDoubleMap(Collections.singletonMap("e", 5.0));
        mapsAvro.setMyStringMap(Collections.singletonMap("f", "6"));
        mapsAvro.setMyBoolMapMap(Collections.singletonMap("g", Collections.singletonMap("h", true)));
        mapsAvro.setMyIntMapMap(Collections.singletonMap("i", Collections.singletonMap("j", 8)));
        mapsAvro.setMyLongMapMap(Collections.singletonMap("k", Collections.singletonMap("l", 9L)));
        mapsAvro.setMyFloatMapMap(Collections.singletonMap("m", Collections.singletonMap("n", 10.0f)));
        mapsAvro.setMyDoubleMapMap(Collections.singletonMap("o", Collections.singletonMap("p", 11.0)));
        mapsAvro.setMyStringMapMap(Collections.singletonMap("q", Collections.singletonMap("r", "12")));

        BulletRecord record = new AvroBulletRecordConverter().convert(mapsAvro);

        Assert.assertEquals(record.get("myBoolMap"), mapsAvro.getMyBoolMap());
        Assert.assertEquals(record.get("myIntMap"), mapsAvro.getMyIntMap());
        Assert.assertEquals(record.get("myLongMap"), mapsAvro.getMyLongMap());
        Assert.assertEquals(record.get("myFloatMap"), mapsAvro.getMyFloatMap());
        Assert.assertEquals(record.get("myDoubleMap"), mapsAvro.getMyDoubleMap());
        Assert.assertEquals(record.get("myStringMap"), mapsAvro.getMyStringMap());
        Assert.assertEquals(record.get("myBoolMapMap"), mapsAvro.getMyBoolMapMap());
        Assert.assertEquals(record.get("myIntMapMap"), mapsAvro.getMyIntMapMap());
        Assert.assertEquals(record.get("myLongMapMap"), mapsAvro.getMyLongMapMap());
        Assert.assertEquals(record.get("myFloatMapMap"), mapsAvro.getMyFloatMapMap());
        Assert.assertEquals(record.get("myDoubleMapMap"), mapsAvro.getMyDoubleMapMap());
        Assert.assertEquals(record.get("myStringMapMap"), mapsAvro.getMyStringMapMap());
        Assert.assertNull(record.get("dne"));
    }

    @Test
    public void testLists() throws Exception {
        ListsAvro listsAvro = new ListsAvro();
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

        BulletRecord record = new AvroBulletRecordConverter().convert(listsAvro);

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
    }

    @Test
    public void testDeserializeClassName() throws Exception {
        BulletDSLConfig config = new BulletDSLConfig();
        config.set(BulletDSLConfig.RECORD_CONVERTER_AVRO_DESERIALIZE_ENABLE, true);
        config.set(BulletDSLConfig.RECORD_CONVERTER_AVRO_CLASS_NAME, ListsAvro.class.getName());

        AvroBulletRecordConverter recordConverter = new AvroBulletRecordConverter(config);

        ListsAvro listsAvro = new ListsAvro();
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

        SpecificDatumWriter<ListsAvro> writer = new SpecificDatumWriter<>(ListsAvro.class);
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        Encoder encoder = EncoderFactory.get().binaryEncoder(stream, null);
        writer.write(listsAvro, encoder);
        encoder.flush();

        BulletRecord record = recordConverter.convert(stream.toByteArray());

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
    }

    @Test
    public void testDeserializeSchemaFile() throws Exception {
        BulletDSLConfig config = new BulletDSLConfig();
        config.set(BulletDSLConfig.RECORD_CONVERTER_AVRO_DESERIALIZE_ENABLE, true);
        config.set(BulletDSLConfig.RECORD_CONVERTER_AVRO_SCHEMA_FILE, "src/test/avro/ListsAvro.avsc");

        AvroBulletRecordConverter recordConverter = new AvroBulletRecordConverter(config);

        ListsAvro listsAvro = new ListsAvro();
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

        SpecificDatumWriter<ListsAvro> writer = new SpecificDatumWriter<>(ListsAvro.class);
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        Encoder encoder = EncoderFactory.get().binaryEncoder(stream, null);
        writer.write(listsAvro, encoder);
        encoder.flush();

        BulletRecord record = recordConverter.convert(stream.toByteArray());

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
    }

    @Test(expectedExceptions = BulletDSLException.class, expectedExceptionsMessageRegExp = "Could not find avro schema\\.")
    public void testDeserializeNoSchema() throws Exception {
        BulletDSLConfig config = new BulletDSLConfig();
        config.set(BulletDSLConfig.RECORD_CONVERTER_AVRO_DESERIALIZE_ENABLE, true);
        config.set(BulletDSLConfig.RECORD_CONVERTER_AVRO_CLASS_NAME, "");
        config.set(BulletDSLConfig.RECORD_CONVERTER_AVRO_SCHEMA_FILE, "");

        new AvroBulletRecordConverter(config);
    }

    @Test(expectedExceptions = BulletDSLException.class, expectedExceptionsMessageRegExp = "Failed to deserialize avro record\\.")
    public void testDeserializeWrongAvro() throws Exception {
        BulletDSLConfig config = new BulletDSLConfig();
        config.set(BulletDSLConfig.RECORD_CONVERTER_AVRO_DESERIALIZE_ENABLE, true);
        config.set(BulletDSLConfig.RECORD_CONVERTER_AVRO_CLASS_NAME, DummyAvro.class.getName());

        AvroBulletRecordConverter recordConverter = new AvroBulletRecordConverter(config);

        ListsAvro listsAvro = new ListsAvro();
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

        SpecificDatumWriter<ListsAvro> writer = new SpecificDatumWriter<>(ListsAvro.class);
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        Encoder encoder = EncoderFactory.get().binaryEncoder(stream, null);
        writer.write(listsAvro, encoder);
        encoder.flush();

        recordConverter.convert(stream.toByteArray());
    }
}
