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
import org.testng.Assert;
import org.testng.annotations.Test;

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

        AvroBulletRecordConverter recordConverter = new AvroBulletRecordConverter();
        BulletRecord record = recordConverter.convert(dummyAvro);

        Assert.assertEquals(record.typedGet("myBool").getValue(), dummyAvro.getMyBool());
        Assert.assertEquals(record.typedGet("myDouble").getValue(), dummyAvro.getMyDouble());
        Assert.assertEquals(record.typedGet("myFloat").getValue(), dummyAvro.getMyFloat());
        Assert.assertEquals(record.typedGet("myInt").getValue(), dummyAvro.getMyInt());
        Assert.assertEquals(record.typedGet("myIntList").getValue(), dummyAvro.getMyIntList());
        Assert.assertEquals(record.typedGet("myLong").getValue(), dummyAvro.getMyLong());
        Assert.assertEquals(record.typedGet("myString").getValue(), dummyAvro.getMyString());
        Assert.assertEquals(record.typedGet("myStringMap").getValue(), dummyAvro.getMyStringMap());
        Assert.assertFalse(record.hasField("myBytes"));
        Assert.assertFalse(record.hasField("myDummyAvro"));
    }

    @Test(expectedExceptions = ClassCastException.class, expectedExceptionsMessageRegExp = "java\\.nio\\.HeapByteBuffer cannot be cast to java\\.io\\.Serializable")
    public void testConvertWithoutSchemaNotSerializable() throws Exception {
        DummyAvro dummyAvro = new DummyAvro();
        dummyAvro.setMyBytes(ByteBuffer.allocate(1));

        new AvroBulletRecordConverter().convert(dummyAvro);
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
        Assert.assertEquals(record.typedGet("myBool").getValue(), true);
        Assert.assertEquals(record.typedGet("myLong").getValue(), 10L);
        Assert.assertEquals(record.typedGet("myFloat").getValue(), 20.0f);
        Assert.assertEquals(record.typedGet("myDouble").getValue(), 30.0);
    }

    @Test(expectedExceptions = BulletDSLException.class, expectedExceptionsMessageRegExp = "Could not convert field: \\{name: myBytes, reference: myBytes, type: BOOLEAN\\}")
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
        Assert.assertEquals(record.typedGet("bbb").getValue(), myIntList.get(2));
    }

    @Test
    public void testSchemaExtractFromMap() throws Exception {
        AvroBulletRecordConverter recordConverter = new AvroBulletRecordConverter("schemas/dummy.json");

        DummyAvro dummyAvro = new DummyAvro();
        dummyAvro.setMyStringMap(Collections.singletonMap("aaa", "hello"));

        BulletRecord record = recordConverter.convert(dummyAvro);

        Assert.assertEquals(record.typedGet("myStringMap").getValue(), dummyAvro.getMyStringMap());
        Assert.assertEquals(record.typedGet("aaa").getValue(), "hello");
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

        Assert.assertEquals(record.typedGet("myDummyInt").getValue(), 100);
    }

    @Test(expectedExceptions = BulletDSLException.class, expectedExceptionsMessageRegExp = "Could not convert field: \\{name: null, reference: myIntList, type: null\\}")
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
        Assert.assertEquals(record.typedGet("aaa").getValue(), "hello");
        Assert.assertEquals(record.typedGet("bbb").getValue(), "world");
        Assert.assertEquals(record.typedGet("ccc").getValue(), "!");
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

        DummyAvro dummyAvro = new DummyAvro();
        dummyAvro.setMyDummyAvro(another);

        BulletRecord record = recordConverter.convert(dummyAvro);
        Assert.assertEquals(record.fieldCount(), 6);
        Assert.assertEquals(record.typedGet("myBool").getValue(), true);
        Assert.assertEquals(record.typedGet("myInt").getValue(), 1);
        Assert.assertEquals(record.typedGet("myLong").getValue(), 2L);
        Assert.assertEquals(record.typedGet("myFloat").getValue(), 3.0f);
        Assert.assertEquals(record.typedGet("myDouble").getValue(), 4.0);
        Assert.assertEquals(record.typedGet("myString").getValue(), "5.0");
    }

    @Test(expectedExceptions = BulletDSLException.class, expectedExceptionsMessageRegExp = "Could not convert field: \\{name: null, reference: myDummyAvro, type: null\\}")
    public void testSchemaRecordFromAvroRecordNotSerializable() throws Exception {
        AvroBulletRecordConverter recordConverter = new AvroBulletRecordConverter("schemas/dummyrecord.json");

        DummyAvro another = new DummyAvro();
        another.setMyDummyAvro(new DummyAvro()); // not serializable

        DummyAvro dummyAvro = new DummyAvro();
        dummyAvro.setMyDummyAvro(another);

        recordConverter.convert(dummyAvro);
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

        Assert.assertEquals(record.typedGet("myBoolMap").getValue(), mapsAvro.getMyBoolMap());
        Assert.assertEquals(record.typedGet("myIntMap").getValue(), mapsAvro.getMyIntMap());
        Assert.assertEquals(record.typedGet("myLongMap").getValue(), mapsAvro.getMyLongMap());
        Assert.assertEquals(record.typedGet("myFloatMap").getValue(), mapsAvro.getMyFloatMap());
        Assert.assertEquals(record.typedGet("myDoubleMap").getValue(), mapsAvro.getMyDoubleMap());
        Assert.assertEquals(record.typedGet("myStringMap").getValue(), mapsAvro.getMyStringMap());
        Assert.assertEquals(record.typedGet("myBoolMapMap").getValue(), mapsAvro.getMyBoolMapMap());
        Assert.assertEquals(record.typedGet("myIntMapMap").getValue(), mapsAvro.getMyIntMapMap());
        Assert.assertEquals(record.typedGet("myLongMapMap").getValue(), mapsAvro.getMyLongMapMap());
        Assert.assertEquals(record.typedGet("myFloatMapMap").getValue(), mapsAvro.getMyFloatMapMap());
        Assert.assertEquals(record.typedGet("myDoubleMapMap").getValue(), mapsAvro.getMyDoubleMapMap());
        Assert.assertEquals(record.typedGet("myStringMapMap").getValue(), mapsAvro.getMyStringMapMap());
        Assert.assertFalse(record.hasField("dne"));
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

        Assert.assertEquals(record.typedGet("myBoolList").getValue(), listsAvro.getMyBoolList());
        Assert.assertEquals(record.typedGet("myIntList").getValue(), listsAvro.getMyIntList());
        Assert.assertEquals(record.typedGet("myLongList").getValue(), listsAvro.getMyLongList());
        Assert.assertEquals(record.typedGet("myFloatList").getValue(), listsAvro.getMyFloatList());
        Assert.assertEquals(record.typedGet("myDoubleList").getValue(), listsAvro.getMyDoubleList());
        Assert.assertEquals(record.typedGet("myStringList").getValue(), listsAvro.getMyStringList());
        Assert.assertEquals(record.typedGet("myBoolMapList").getValue(), listsAvro.getMyBoolMapList());
        Assert.assertEquals(record.typedGet("myIntMapList").getValue(), listsAvro.getMyIntMapList());
        Assert.assertEquals(record.typedGet("myLongMapList").getValue(), listsAvro.getMyLongMapList());
        Assert.assertEquals(record.typedGet("myFloatMapList").getValue(), listsAvro.getMyFloatMapList());
        Assert.assertEquals(record.typedGet("myDoubleMapList").getValue(), listsAvro.getMyDoubleMapList());
        Assert.assertEquals(record.typedGet("myStringMapList").getValue(), listsAvro.getMyStringMapList());
        Assert.assertFalse(record.hasField("dne"));
    }
}
