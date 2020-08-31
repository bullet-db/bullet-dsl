/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.dsl.converter;

import com.yahoo.bullet.dsl.BulletDSLConfig;
import com.yahoo.bullet.dsl.BulletDSLException;
import com.yahoo.bullet.record.BulletRecord;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MapBulletRecordConverterTest {

    @Test(expectedExceptions = BulletDSLException.class)
    public void testBuildWithSchemaErrors() throws Exception {
        new MapBulletRecordConverter("schemas/empty.json");
    }

    @Test
    public void testConvertWithoutSchema() throws Exception {
        MapBulletRecordConverter recordConverter = new MapBulletRecordConverter();

        Map<String, Object> map = new HashMap<>();
        map.put("myBool", true);
        map.put("myInt", 123);
        map.put("myLong", 456L);
        map.put("myFloat", 7.89f);
        map.put("myDouble", 0.12);
        map.put("myString", "345");
        map.put("myNull", null);
        map.put("myNull2", null);

        // converts all fields from map
        BulletRecord record = recordConverter.convert(map);

        Assert.assertEquals(record.typedGet("myBool").getValue(), map.get("myBool"));
        Assert.assertEquals(record.typedGet("myInt").getValue(), map.get("myInt"));
        Assert.assertEquals(record.typedGet("myLong").getValue(), map.get("myLong"));
        Assert.assertEquals(record.typedGet("myFloat").getValue(), map.get("myFloat"));
        Assert.assertEquals(record.typedGet("myDouble").getValue(), map.get("myDouble"));
        Assert.assertEquals(record.typedGet("myString").getValue(), map.get("myString"));
        Assert.assertFalse(record.hasField("dne"));

        // does not add null fields
        Assert.assertEquals(record.fieldCount(), 6);
    }

    @Test
    public void testConvertWithoutSchemaUsingConfigConstructor() throws Exception {
        BulletDSLConfig config = new BulletDSLConfig();
        MapBulletRecordConverter recordConverter = new MapBulletRecordConverter(config);

        Map<String, Object> map = new HashMap<>();
        map.put("myBool", true);
        map.put("myInt", 123);
        map.put("myLong", 456L);
        map.put("myFloat", 7.89f);
        map.put("myDouble", 0.12);
        map.put("myString", "345");
        map.put("myNull", null);
        map.put("myNull2", null);

        // converts all fields from map
        BulletRecord record = recordConverter.convert(map);

        Assert.assertEquals(record.typedGet("myBool").getValue(), map.get("myBool"));
        Assert.assertEquals(record.typedGet("myInt").getValue(), map.get("myInt"));
        Assert.assertEquals(record.typedGet("myLong").getValue(), map.get("myLong"));
        Assert.assertEquals(record.typedGet("myFloat").getValue(), map.get("myFloat"));
        Assert.assertEquals(record.typedGet("myDouble").getValue(), map.get("myDouble"));
        Assert.assertEquals(record.typedGet("myString").getValue(), map.get("myString"));
        Assert.assertFalse(record.hasField("dne"));

        // does not add null fields
        Assert.assertEquals(record.fieldCount(), 6);
    }

    @Test
    public void testConvertWithSchema() throws Exception {
        MapBulletRecordConverter recordConverter = new MapBulletRecordConverter("schemas/all.json");

        Map<String, Object> map = new HashMap<>();
        map.put("myBool", true);
        map.put("myInt", 123);
        map.put("myLong", 456L);
        map.put("myFloat", 7.89f);
        map.put("myDouble", 0.12);
        map.put("myString", "345");
        map.put("dne", 0);

        // converts all fields from map that are in the schema.
        BulletRecord record = recordConverter.convert(map);

        Assert.assertEquals(record.typedGet("myBool").getValue(), map.get("myBool"));
        Assert.assertEquals(record.typedGet("myInt").getValue(), map.get("myInt"));
        Assert.assertEquals(record.typedGet("myLong").getValue(), map.get("myLong"));
        Assert.assertEquals(record.typedGet("myFloat").getValue(), map.get("myFloat"));
        Assert.assertEquals(record.typedGet("myDouble").getValue(), map.get("myDouble"));
        Assert.assertEquals(record.typedGet("myString").getValue(), map.get("myString"));
        Assert.assertFalse(record.hasField("dne"));

        // does not add null for missing fields
        Assert.assertEquals(record.fieldCount(), 6);
    }

    @Test
    public void testConvertRecord() throws Exception {
        MapBulletRecordConverter recordConverter = new MapBulletRecordConverter("schemas/record1.json");

        Map<String, Object> map = new HashMap<>();
        map.put("myBool", true);
        map.put("myInt", 123);
        map.put("myLong", 456L);
        map.put("myFloat", 7.89f);
        map.put("myDouble", 0.12);
        map.put("myString", "345");
        map.put("myNull", null);
        map.put("myNull2", null);

        Map<String, Object> mapRecord = new HashMap<>();
        mapRecord.put("data", map);

        // flattens "data"
        BulletRecord record = recordConverter.convert(mapRecord);

        Assert.assertEquals(record.typedGet("myBool").getValue(), map.get("myBool"));
        Assert.assertEquals(record.typedGet("myInt").getValue(), map.get("myInt"));
        Assert.assertEquals(record.typedGet("myLong").getValue(), map.get("myLong"));
        Assert.assertEquals(record.typedGet("myFloat").getValue(), map.get("myFloat"));
        Assert.assertEquals(record.typedGet("myDouble").getValue(), map.get("myDouble"));
        Assert.assertEquals(record.typedGet("myString").getValue(), map.get("myString"));

        // does not add null fields
        Assert.assertEquals(record.fieldCount(), 6);
    }

    @Test
    public void testNestedConvertRecord() throws Exception {
        MapBulletRecordConverter recordConverter = new MapBulletRecordConverter("schemas/record2.json");

        Map<String, Object> mapRecord = new HashMap<>();
        Map<String, Object> data = new HashMap<>();

        mapRecord.put("data", data);

        // tries to flatten "data.aaa.bbb" but aaa is missing
        BulletRecord record = recordConverter.convert(mapRecord);

        Assert.assertEquals(record.fieldCount(), 0);

        // tries to flatten "data.aaa.bbb" but bbb is missing
        Map<String, Object> aaa = new HashMap<>();
        data.put("aaa", aaa);

        record = recordConverter.convert(mapRecord);

        Assert.assertEquals(record.fieldCount(), 0);

        // flattens "data.aaa.bbb" but bbb is empty
        Map<String, Object> bbb = new HashMap<>();

        aaa.put("bbb", bbb);

        record = recordConverter.convert(mapRecord);

        Assert.assertEquals(record.fieldCount(), 0);

        // flattens "data.aaa.bbb"
        bbb.put("myBool", true);
        bbb.put("myInt", 123);
        bbb.put("myLong", 456L);
        bbb.put("myFloat", 7.89f);
        bbb.put("myDouble", 0.12);
        bbb.put("myString", "345");

        record = recordConverter.convert(mapRecord);

        Assert.assertEquals(record.typedGet("myBool").getValue(), bbb.get("myBool"));
        Assert.assertEquals(record.typedGet("myInt").getValue(), bbb.get("myInt"));
        Assert.assertEquals(record.typedGet("myLong").getValue(), bbb.get("myLong"));
        Assert.assertEquals(record.typedGet("myFloat").getValue(), bbb.get("myFloat"));
        Assert.assertEquals(record.typedGet("myDouble").getValue(), bbb.get("myDouble"));
        Assert.assertEquals(record.typedGet("myString").getValue(), bbb.get("myString"));
    }

    @Test
    public void testExtractFromList() throws Exception {
        MapBulletRecordConverter recordConverter = new MapBulletRecordConverter("schemas/dummy.json");

        List<Integer> myIntList = Arrays.asList(0, 1, 2, 3, 4);

        BulletRecord record = recordConverter.convert(Collections.singletonMap("myIntList", myIntList));

        Assert.assertEquals(record.typedGet("bbb").getValue(), myIntList.get(2));
    }

    @Test
    public void testMaps() throws Exception {
        Map<String, Object> map = new HashMap<>();
        map.put("myBoolMap", Collections.singletonMap("a", false));
        map.put("myIntMap", Collections.singletonMap("b", 2));
        map.put("myLongMap", Collections.singletonMap("c", 3L));
        map.put("myFloatMap", Collections.singletonMap("d", 4.0f));
        map.put("myDoubleMap", Collections.singletonMap("e", 5.0));
        map.put("myStringMap", Collections.singletonMap("f", "6"));
        map.put("myBoolMapMap", Collections.singletonMap("g", Collections.singletonMap("h", true)));
        map.put("myIntMapMap", Collections.singletonMap("i", Collections.singletonMap("j", 8)));
        map.put("myLongMapMap", Collections.singletonMap("k", Collections.singletonMap("l", 9L)));
        map.put("myFloatMapMap", Collections.singletonMap("m", Collections.singletonMap("n", 10.0f)));
        map.put("myDoubleMapMap", Collections.singletonMap("o", Collections.singletonMap("p", 11.0)));
        map.put("myStringMapMap", Collections.singletonMap("q", Collections.singletonMap("r", "12")));

        // Loads schema using class loader
        MapBulletRecordConverter recordConverter = new MapBulletRecordConverter("schemas/all.json");
        BulletRecord record = recordConverter.convert(map);

        Assert.assertEquals(record.typedGet("myBoolMap").getValue(), map.get("myBoolMap"));
        Assert.assertEquals(record.typedGet("myIntMap").getValue(), map.get("myIntMap"));
        Assert.assertEquals(record.typedGet("myLongMap").getValue(), map.get("myLongMap"));
        Assert.assertEquals(record.typedGet("myFloatMap").getValue(), map.get("myFloatMap"));
        Assert.assertEquals(record.typedGet("myDoubleMap").getValue(), map.get("myDoubleMap"));
        Assert.assertEquals(record.typedGet("myStringMap").getValue(), map.get("myStringMap"));
        Assert.assertEquals(record.typedGet("myBoolMapMap").getValue(), map.get("myBoolMapMap"));
        Assert.assertEquals(record.typedGet("myIntMapMap").getValue(), map.get("myIntMapMap"));
        Assert.assertEquals(record.typedGet("myLongMapMap").getValue(), map.get("myLongMapMap"));
        Assert.assertEquals(record.typedGet("myFloatMapMap").getValue(), map.get("myFloatMapMap"));
        Assert.assertEquals(record.typedGet("myDoubleMapMap").getValue(), map.get("myDoubleMapMap"));
        Assert.assertEquals(record.typedGet("myStringMapMap").getValue(), map.get("myStringMapMap"));
        Assert.assertFalse(record.hasField("dne"));
    }

    @Test
    public void testLists() throws Exception {
        Map<String, Object> map = new HashMap<>();
        map.put("myBoolList", Collections.singletonList(false));
        map.put("myIntList", Collections.singletonList(14));
        map.put("myLongList", Collections.singletonList(15L));
        map.put("myFloatList", Collections.singletonList(16.0f));
        map.put("myDoubleList", Collections.singletonList(17.0));
        map.put("myStringList", Collections.singletonList("18"));
        map.put("myBoolMapList", Collections.singletonList(Collections.singletonMap("s", true)));
        map.put("myIntMapList", Collections.singletonList(Collections.singletonMap("t", 20)));
        map.put("myLongMapList", Collections.singletonList(Collections.singletonMap("u", 21L)));
        map.put("myFloatMapList", Collections.singletonList(Collections.singletonMap("v", 22.0f)));
        map.put("myDoubleMapList", Collections.singletonList(Collections.singletonMap("w", 23.0)));
        map.put("myStringMapList", Collections.singletonList(Collections.singletonMap("x", "24")));

        // Loads schema using class loader
        MapBulletRecordConverter recordConverter = new MapBulletRecordConverter("schemas/all.json");
        BulletRecord record = recordConverter.convert(map);

        Assert.assertEquals(record.typedGet("myBoolList").getValue(), map.get("myBoolList"));
        Assert.assertEquals(record.typedGet("myIntList").getValue(), map.get("myIntList"));
        Assert.assertEquals(record.typedGet("myLongList").getValue(), map.get("myLongList"));
        Assert.assertEquals(record.typedGet("myFloatList").getValue(), map.get("myFloatList"));
        Assert.assertEquals(record.typedGet("myDoubleList").getValue(), map.get("myDoubleList"));
        Assert.assertEquals(record.typedGet("myStringList").getValue(), map.get("myStringList"));
        Assert.assertEquals(record.typedGet("myBoolMapList").getValue(), map.get("myBoolMapList"));
        Assert.assertEquals(record.typedGet("myIntMapList").getValue(), map.get("myIntMapList"));
        Assert.assertEquals(record.typedGet("myLongMapList").getValue(), map.get("myLongMapList"));
        Assert.assertEquals(record.typedGet("myFloatMapList").getValue(), map.get("myFloatMapList"));
        Assert.assertEquals(record.typedGet("myDoubleMapList").getValue(), map.get("myDoubleMapList"));
        Assert.assertEquals(record.typedGet("myStringMapList").getValue(), map.get("myStringMapList"));
        Assert.assertFalse(record.hasField("dne"));
    }
}
