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
import com.yahoo.bullet.record.avro.TypedAvroBulletRecordProvider;
import com.yahoo.bullet.typesystem.Type;
import com.yahoo.bullet.typesystem.TypedObject;
import lombok.AllArgsConstructor;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.util.Utf8;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;

public class AvroBulletRecordConverterTest {
    private static final Schema SCHEMA;
    static {
        SCHEMA = SchemaBuilder.record("Test").namespace("foo").fields()
                    .name("string").type().optional().stringType()
                    .name("int").type().optional().intType()
                    .name("mapOfString").type().optional().map().values().stringType()
                    .name("mapOfDouble").type().optional().map().values().doubleType()
                    .name("mapOfMapOfString").type().optional().map().values().map().values().stringType()
                    .name("listOfString").type().optional().array().items().stringType()
                    .name("listOfBoolean").type().optional().array().items().booleanType()
                    .name("listOfMapOfString").type().optional().array().items().map().values().stringType()
                    .name("unsupportedUnion").type().unionOf()
                        .nullType().and()
                        .record("record")
                        .fields()
                            .name("stringField").type().optional().stringType()
                            .endRecord()
                        .endUnion()
                        .nullDefault()
                    .endRecord();
    }

    @AllArgsConstructor
    private static class Field {
        String name;
        Object value;
    }

    private static GenericRecord make(Field... fields) {
        GenericRecordBuilder builder = new GenericRecordBuilder(SCHEMA);
        for (Field field : fields) {
            builder.set(field.name, field.value);
        }
        return builder.build();
    }
    
    private static AvroBulletRecordConverter fixingConverter() throws Exception {
        BulletDSLConfig config = new BulletDSLConfig();
        config.set(BulletDSLConfig.RECORD_CONVERTER_AVRO_STRING_TYPE_FIX_ENABLE, true);
        config.validate();
        return new AvroBulletRecordConverter(config);
    }

    @Test
    public void testConvertWithoutSchema() throws Exception {
        DummyAvro dummyAvro = new DummyAvro();
        dummyAvro.setMyBool(true);
        dummyAvro.setMyDouble(0.12);
        dummyAvro.setMyFloat(3.45f);
        dummyAvro.setMyInt(678);
        dummyAvro.setMyIntList(singletonList(910));
        dummyAvro.setMyLong(1112L);
        dummyAvro.setMyString("1314");
        dummyAvro.setMyStringMap(singletonMap("1516", "1718"));

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
        dummyAvro.setMyStringMap(singletonMap("aaa", "hello"));

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
        dummyAvro.setMyIntList(emptyList());

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
        mapsAvro.setMyBoolMap(singletonMap("a", false));
        mapsAvro.setMyIntMap(singletonMap("b", 2));
        mapsAvro.setMyLongMap(singletonMap("c", 3L));
        mapsAvro.setMyFloatMap(singletonMap("d", 4.0f));
        mapsAvro.setMyDoubleMap(singletonMap("e", 5.0));
        mapsAvro.setMyStringMap(singletonMap("f", "6"));
        mapsAvro.setMyBoolMapMap(singletonMap("g", singletonMap("h", true)));
        mapsAvro.setMyIntMapMap(singletonMap("i", singletonMap("j", 8)));
        mapsAvro.setMyLongMapMap(singletonMap("k", singletonMap("l", 9L)));
        mapsAvro.setMyFloatMapMap(singletonMap("m", singletonMap("n", 10.0f)));
        mapsAvro.setMyDoubleMapMap(singletonMap("o", singletonMap("p", 11.0)));
        mapsAvro.setMyStringMapMap(singletonMap("q", singletonMap("r", "12")));

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
        listsAvro.setMyBoolList(singletonList(false));
        listsAvro.setMyIntList(singletonList(14));
        listsAvro.setMyLongList(singletonList(15L));
        listsAvro.setMyFloatList(singletonList(16.0f));
        listsAvro.setMyDoubleList(singletonList(17.0));
        listsAvro.setMyStringList(singletonList("18"));
        listsAvro.setMyBoolMapList(singletonList(singletonMap("s", true)));
        listsAvro.setMyIntMapList(singletonList(singletonMap("t", 20)));
        listsAvro.setMyLongMapList(singletonList(singletonMap("u", 21L)));
        listsAvro.setMyFloatMapList(singletonList(singletonMap("v", 22.0f)));
        listsAvro.setMyDoubleMapList(singletonList(singletonMap("w", 23.0)));
        listsAvro.setMyStringMapList(singletonList(singletonMap("x", "24")));

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

    @Test
    public void testStringFixing() throws Exception {
        GenericRecord input = make(new Field("string", new Utf8("foo")));
        BulletRecord actual = fixingConverter().convert(input);
        BulletRecord expected = new TypedAvroBulletRecordProvider().getInstance();
        expected.typedSet("string", new TypedObject(Type.STRING, "foo"));
        Assert.assertEquals(actual, expected);
    }

    @Test
    public void testNonStringFixing() throws Exception {
        GenericRecord input = make(new Field("int", 1));
        BulletRecord actual = fixingConverter().convert(input);
        BulletRecord expected = new TypedAvroBulletRecordProvider().getInstance();
        expected.typedSet("int", new TypedObject(Type.INTEGER, 1));
        Assert.assertEquals(actual, expected);
    }

    @Test
    public void testMapStringFixing() throws Exception {
        Map<Utf8, Utf8> stringMap = new HashMap<>();
        stringMap.put(new Utf8("foo"), new Utf8("bar"));
        stringMap.put(null, new Utf8("baz"));
        GenericRecord input = make(new Field("mapOfString", stringMap));
        BulletRecord actual = fixingConverter().convert(input);

        BulletRecord expected = new TypedAvroBulletRecordProvider().getInstance();
        HashMap<String, String> expectedMap = new HashMap<>();
        expectedMap.put("foo", "bar");
        expectedMap.put(null, "baz");
        expected.typedSet("mapOfString", new TypedObject(Type.STRING_MAP, expectedMap));
        Assert.assertEquals(actual, expected);
    }

    @Test
    public void testMapOfMapStringFixing() throws Exception {
        Map<Utf8, Map<CharSequence, Utf8>> stringMapMap = new HashMap<>();
        Map<CharSequence, Utf8> stringMap = new HashMap<>();
        stringMap.put(new Utf8("foo"), new Utf8("bar"));
        stringMap.put(null, new Utf8("baz"));
        stringMap.put("qux", new Utf8("norf"));
        stringMapMap.put(new Utf8("boo"), stringMap);
        stringMapMap.put(null, new HashMap<>());

        GenericRecord input = make(new Field("mapOfMapOfString", stringMapMap));
        BulletRecord actual = fixingConverter().convert(input);

        BulletRecord expected = new TypedAvroBulletRecordProvider().getInstance();
        HashMap<String, Map<String, String>> expectedStringMapMap = new HashMap<>();
        HashMap<String, String> expectedStringMap = new HashMap<>();
        expectedStringMap.put("foo", "bar");
        expectedStringMap.put(null, "baz");
        expectedStringMap.put("qux", "norf");
        expectedStringMapMap.put("boo", expectedStringMap);
        expectedStringMapMap.put(null, new HashMap<>());
        expected.typedSet("mapOfMapOfString", new TypedObject(Type.STRING_MAP_MAP, expectedStringMapMap));
        Assert.assertEquals(actual, expected);
    }

    @Test
    public void testListStringFixing() throws Exception {
        List<Utf8> stringList = new ArrayList<>();
        stringList.add(new Utf8("foo"));
        stringList.add(new Utf8("bar"));
        stringList.add(null);

        GenericRecord input = make(new Field("listOfString", stringList));
        BulletRecord actual = fixingConverter().convert(input);

        BulletRecord expected = new TypedAvroBulletRecordProvider().getInstance();
        ArrayList<String> expectedList = new ArrayList<>();
        expectedList.add("foo");
        expectedList.add("bar");
        expectedList.add(null);
        expected.typedSet("listOfString", new TypedObject(Type.STRING_LIST, expectedList));
        Assert.assertEquals(actual, expected);
    }

    @Test
    public void testListOfListStringFixing() throws Exception {
        List<Map<CharSequence, CharSequence>> stringMapList = new ArrayList<>();
        Map<CharSequence, CharSequence> stringMap = new HashMap<>();
        stringMap.put(new Utf8("foo"), new Utf8("bar"));
        stringMap.put(null, new Utf8("baz"));
        stringMap.put("qux", new Utf8("norf"));
        stringMapList.add(stringMap);
        stringMapList.add(null);

        GenericRecord input = make(new Field("listOfMapOfString", stringMapList));
        BulletRecord actual = fixingConverter().convert(input);

        BulletRecord expected = new TypedAvroBulletRecordProvider().getInstance();
        ArrayList<Map<String, String>> expectedStringMapList = new ArrayList<>();
        HashMap<String, String> expectedStringMap = new HashMap<>();
        expectedStringMap.put("foo", "bar");
        expectedStringMap.put(null, "baz");
        expectedStringMap.put("qux", "norf");
        expectedStringMapList.add(expectedStringMap);
        expectedStringMapList.add(null);
        expected.typedSet("listOfMapOfString", new TypedObject(Type.STRING_MAP_LIST, expectedStringMapList));
        Assert.assertEquals(actual, expected);
    }

    @Test(expectedExceptions = UnsupportedOperationException.class, expectedExceptionsMessageRegExp = ".*null or unknown.*")
    public void testUnsupportedStringFixing() throws Exception {
        Schema unionSchema = SCHEMA.getField("unsupportedUnion").schema();
        int index = unionSchema.getIndexNamed("foo.record");
        GenericRecord nested = new GenericData.Record(unionSchema.getTypes().get(index));
        nested.put("stringField", new Utf8("bar"));
        GenericRecord input = make (new Field("unsupportedUnion", nested));

        fixingConverter().convert(input);
    }
}
