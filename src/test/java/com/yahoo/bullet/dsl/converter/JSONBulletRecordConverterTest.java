/*
 *  Copyright 2021, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.dsl.converter;

import com.yahoo.bullet.dsl.BulletDSLConfig;
import com.yahoo.bullet.dsl.BulletDSLException;
import com.yahoo.bullet.record.BulletRecord;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;

public class JSONBulletRecordConverterTest {

    @Test(expectedExceptions = BulletDSLException.class)
    public void testBuildWithSchemaErrors() throws Exception {
        new JSONBulletRecordConverter("schemas/empty.json");
    }

    @Test
    public void testConvertWithoutSchema() throws Exception {
        JSONBulletRecordConverter recordConverter = new JSONBulletRecordConverter();

        String json = "{'myBool':true,'myInt':123,'myLong':456,'myFloat':7.89,'myDouble':0.12,'myString':'345'}";

        BulletRecord record = recordConverter.convert(json);

        // Converts numbers as double without schema because of gson
        Assert.assertEquals(record.typedGet("myBool").getValue(), true);
        Assert.assertEquals(record.typedGet("myInt").getValue(), 123.0);
        Assert.assertEquals(record.typedGet("myLong").getValue(), 456.0);
        Assert.assertEquals(record.typedGet("myFloat").getValue(), 7.89);
        Assert.assertEquals(record.typedGet("myDouble").getValue(), 0.12);
        Assert.assertEquals(record.typedGet("myString").getValue(), "345");
        Assert.assertFalse(record.hasField("dne"));

        // does not add null fields
        Assert.assertEquals(record.fieldCount(), 6);
    }

    @Test
    public void testConvertWithoutSchemaUsingConfigConstructor() throws Exception {
        BulletDSLConfig config = new BulletDSLConfig();
        JSONBulletRecordConverter recordConverter = new JSONBulletRecordConverter(config);

        String json = "{'myBool':true,'myInt':123,'myLong':456,'myFloat':7.89,'myDouble':0.12,'myString':'345','myNull':null,'myNull2':null}";

        BulletRecord record = recordConverter.convert(json);

        // Converts numbers as double without schema because of gson
        Assert.assertEquals(record.typedGet("myBool").getValue(), true);
        Assert.assertEquals(record.typedGet("myInt").getValue(), 123.0);
        Assert.assertEquals(record.typedGet("myLong").getValue(), 456.0);
        Assert.assertEquals(record.typedGet("myFloat").getValue(), 7.89);
        Assert.assertEquals(record.typedGet("myDouble").getValue(), 0.12);
        Assert.assertEquals(record.typedGet("myString").getValue(), "345");
        Assert.assertFalse(record.hasField("dne"));

        // does not add null fields
        Assert.assertEquals(record.fieldCount(), 6);
    }

    @Test
    public void testConvertWithSchema() throws Exception {
        JSONBulletRecordConverter recordConverter = new JSONBulletRecordConverter("schemas/all.json");

        String json = "{'myBool':true,'myInt':123,'myLong':456,'myFloat':7.89,'myDouble':0.12,'myString':'345','dne':0}";

        BulletRecord record = recordConverter.convert(json);

        Assert.assertEquals(record.typedGet("myBool").getValue(), true);
        Assert.assertEquals(record.typedGet("myInt").getValue(), 123);
        Assert.assertEquals(record.typedGet("myLong").getValue(), 456L);
        Assert.assertEquals(record.typedGet("myFloat").getValue(), 7.89f);
        Assert.assertEquals(record.typedGet("myDouble").getValue(), 0.12);
        Assert.assertEquals(record.typedGet("myString").getValue(), "345");
        Assert.assertFalse(record.hasField("dne"));

        // does not add null for missing fields
        Assert.assertEquals(record.fieldCount(), 6);
    }
    @Test
    public void testConvertRecord() throws Exception {
        JSONBulletRecordConverter recordConverter = new JSONBulletRecordConverter("schemas/record1.json");

        String json = "{'data':{'myBool':true,'myInt':123,'myLong':456,'myFloat':7.89,'myDouble':0.12,'myString':'345','myNull':null,'myNull2':null}}";

        // flattens "data" but does not have schema of nested fields so gson will default convert to double
        BulletRecord record = recordConverter.convert(json);

        Assert.assertEquals(record.typedGet("myBool").getValue(), true);
        Assert.assertEquals(record.typedGet("myInt").getValue(), 123.0);
        Assert.assertEquals(record.typedGet("myLong").getValue(), 456.0);
        Assert.assertEquals(record.typedGet("myFloat").getValue(), 7.89);
        Assert.assertEquals(record.typedGet("myDouble").getValue(), 0.12);
        Assert.assertEquals(record.typedGet("myString").getValue(), "345");

        // does not add null fields
        Assert.assertEquals(record.fieldCount(), 6);
    }

    @Test
    public void testNestedConvertRecord() throws Exception {
        JSONBulletRecordConverter recordConverter = new JSONBulletRecordConverter("schemas/record2.json");

        Map<String, Object> mapRecord = new HashMap<>();
        Map<String, Object> data = new HashMap<>();

        mapRecord.put("data", data);

        String json = "{'data':{}}";

        // tries to flatten "data.aaa.bbb" but aaa is missing
        BulletRecord record = recordConverter.convert(json);

        Assert.assertEquals(record.fieldCount(), 0);

        // tries to flatten "data.aaa.bbb" but bbb is missing
        json = "{'data':{'aaa':{}}}";

        record = recordConverter.convert(json);

        Assert.assertEquals(record.fieldCount(), 0);

        // flattens "data.aaa.bbb" but bbb is empty
        json = "{'data':{'aaa':{'bbb':{}}}}";

        record = recordConverter.convert(json);

        Assert.assertEquals(record.fieldCount(), 0);

        // flattens "data.aaa.bbb" but does not have schema of nested fields so gson will default convert to double
        json = "{'data':{'aaa':{'bbb':{'myBool':true,'myInt':123,'myLong':456,'myFloat':7.89,'myDouble':0.12,'myString':'345'}}}}";

        record = recordConverter.convert(json);

        Assert.assertEquals(record.typedGet("myBool").getValue(), true);
        Assert.assertEquals(record.typedGet("myInt").getValue(), 123.0);
        Assert.assertEquals(record.typedGet("myLong").getValue(), 456.0);
        Assert.assertEquals(record.typedGet("myFloat").getValue(), 7.89);
        Assert.assertEquals(record.typedGet("myDouble").getValue(), 0.12);
        Assert.assertEquals(record.typedGet("myString").getValue(), "345");
    }

    @Test
    public void testExtractFromList() throws Exception {
        JSONBulletRecordConverter recordConverter = new JSONBulletRecordConverter("schemas/dummy.json");

        String json = "{'myIntList':[0,1,2,3,4]}";

        BulletRecord record = recordConverter.convert(json);

        Assert.assertEquals(record.typedGet("bbb").getValue(), 2);
    }

    @Test
    public void testMaps() throws Exception {
        String json = "{'myLongMapMap':{'k':{'l':9}},'myStringMapMap':{'q':{'r':'12'}},'myBoolMap':{'a':false}," +
                       "'myDoubleMapMap':{'o':{'p':11.0}},'myDoubleMap':{'e':5.0},'myFloatMapMap':{'m':{'n':10.0}}," +
                       "'myLongMap':{'c':3},'myFloatMap':{'d':4.0},'myBoolMapMap':{'g':{'h':true}}," +
                       "'myIntMapMap':{'i':{'j':8}},'myIntMap':{'b':2},'myStringMap':{'f':'6'}," +
                       "'myFloatMapList':[{'v':22.0},null],'myIntList':[null,14]}";

        // Loads schema using class loader
        JSONBulletRecordConverter recordConverter = new JSONBulletRecordConverter("schemas/all.json");
        BulletRecord record = recordConverter.convert(json);

        Assert.assertEquals(record.typedGet("myBoolMap").getValue(), singletonMap("a", false));
        Assert.assertEquals(record.typedGet("myIntMap").getValue(), singletonMap("b", 2));
        Assert.assertEquals(record.typedGet("myLongMap").getValue(), singletonMap("c", 3L));
        Assert.assertEquals(record.typedGet("myFloatMap").getValue(), singletonMap("d", 4.0f));
        Assert.assertEquals(record.typedGet("myDoubleMap").getValue(), singletonMap("e", 5.0));
        Assert.assertEquals(record.typedGet("myStringMap").getValue(), singletonMap("f", "6"));
        Assert.assertEquals(record.typedGet("myBoolMapMap").getValue(), singletonMap("g", singletonMap("h", true)));
        Assert.assertEquals(record.typedGet("myIntMapMap").getValue(), singletonMap("i", singletonMap("j", 8)));
        Assert.assertEquals(record.typedGet("myLongMapMap").getValue(), singletonMap("k", singletonMap("l", 9L)));
        Assert.assertEquals(record.typedGet("myFloatMapMap").getValue(), singletonMap("m", singletonMap("n", 10.0f)));
        Assert.assertEquals(record.typedGet("myDoubleMapMap").getValue(), singletonMap("o", singletonMap("p", 11.0)));
        Assert.assertEquals(record.typedGet("myStringMapMap").getValue(), singletonMap("q", singletonMap("r", "12")));
        Assert.assertEquals(record.typedGet("myFloatMapList").getValue(), asList(singletonMap("v", 22.0f), null));
        Assert.assertEquals(record.typedGet("myIntList").getValue(), asList(null, 14));
        Assert.assertFalse(record.hasField("dne"));
    }

    @Test
    public void testLists() throws Exception {
        String json = "{'myLongMapList':[{'u':21}],'myFloatList':[16.0],'myFloatMapList':[{'v':22.0}],'myIntList':[14]," +
                       "'myIntMapList':[{'t':20}],'myLongList':[15],'myBoolList':[false],'myBoolMapList':[{'s':true}]," +
                       "'myStringMapList':[{'x':'24'}],'myDoubleMapList':[{'w':23.0}],'myStringList':['18'],'myDoubleList':[17.0]," +
                       "'myLongMap':{'c':3,'d':null},'myFloatMapMap':{'m':{'n':10.0},'o':null}}" ;

        // Loads schema using class loader
        JSONBulletRecordConverter recordConverter = new JSONBulletRecordConverter("schemas/all.json");
        BulletRecord record = recordConverter.convert(json);

        Assert.assertEquals(record.typedGet("myBoolList").getValue(), singletonList(false));
        Assert.assertEquals(record.typedGet("myIntList").getValue(), singletonList(14));
        Assert.assertEquals(record.typedGet("myLongList").getValue(), singletonList(15L));
        Assert.assertEquals(record.typedGet("myFloatList").getValue(), singletonList(16.0f));
        Assert.assertEquals(record.typedGet("myDoubleList").getValue(), singletonList(17.0));
        Assert.assertEquals(record.typedGet("myStringList").getValue(), singletonList("18"));
        Assert.assertEquals(record.typedGet("myBoolMapList").getValue(), singletonList(singletonMap("s", true)));
        Assert.assertEquals(record.typedGet("myIntMapList").getValue(), singletonList(singletonMap("t", 20)));
        Assert.assertEquals(record.typedGet("myLongMapList").getValue(), singletonList(singletonMap("u", 21L)));
        Assert.assertEquals(record.typedGet("myDoubleMapList").getValue(), singletonList(singletonMap("w", 23.0)));
        Assert.assertEquals(record.typedGet("myStringMapList").getValue(), singletonList(singletonMap("x", "24")));
        Map<String, Map<String, Float>> expected = new HashMap<>();
        expected.put("m", singletonMap("n", 10.0f));
        expected.put("o", null);
        Assert.assertEquals(record.typedGet("myFloatMapMap").getValue(), expected);
        Assert.assertFalse(record.hasField("dne"));
    }
}
