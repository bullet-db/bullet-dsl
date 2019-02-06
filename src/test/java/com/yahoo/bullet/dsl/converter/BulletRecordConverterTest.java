/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.dsl.converter;

import com.yahoo.bullet.common.BulletConfig;
import com.yahoo.bullet.dsl.BulletDSLConfig;
import com.yahoo.bullet.dsl.BulletDSLException;
import com.yahoo.bullet.dsl.DummyAvro;
import com.yahoo.bullet.record.BulletRecord;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BulletRecordConverterTest {

    private static class MockBulletRecordConverter extends BulletRecordConverter {
        MockBulletRecordConverter(BulletConfig bulletConfig) {
            super(bulletConfig);
        }

        @Override
        protected Object get(Object object, String base) {
            return null;
        }
    }

    private BulletDSLConfig config;

    @BeforeMethod
    public void init() {
        config = new BulletDSLConfig();
    }

    @Test
    public void testPOJOWithSchema() throws Exception {
        config.set(BulletDSLConfig.RECORD_CONVERTER_CLASS_NAME, POJOBulletRecordConverter.class.getName());
        config.set(BulletDSLConfig.RECORD_CONVERTER_SCHEMA_FILE, "schemas/foo.json");
        config.set(BulletDSLConfig.RECORD_CONVERTER_POJO_CLASS_NAME, POJOBulletRecordConverterTest.Foo.class.getName());

        BulletRecordConverter converter = BulletRecordConverter.from(config);
        Assert.assertTrue(converter instanceof POJOBulletRecordConverter);

        POJOBulletRecordConverterTest.Foo foo = new POJOBulletRecordConverterTest.Foo();
        BulletRecord record = BulletRecordConverter.from(config).convert(foo);

        Assert.assertNull(record.get("myExcludedInt"));
        Assert.assertEquals(record.get("myInt"), 123);
        Assert.assertEquals(record.get("myLong"), 456L);
        Assert.assertEquals(record.get("myBool"), true);
        Assert.assertEquals(record.get("myString"), "789");
        Assert.assertEquals(record.get("myDouble"), 0.12);
        Assert.assertEquals(record.get("myFloat"), 3.45f);
        Assert.assertNull(record.get("bar"));
        Assert.assertEquals(record.get("myIntMap"), foo.getMyIntMap());
        Assert.assertEquals(record.get("myIntList"), foo.getMyIntList());
    }

    @Test
    public void testPOJONoSchema() throws Exception {
        config.set(BulletDSLConfig.RECORD_CONVERTER_CLASS_NAME, POJOBulletRecordConverter.class.getName());
        config.set(BulletDSLConfig.RECORD_CONVERTER_SCHEMA_FILE, null);
        config.set(BulletDSLConfig.RECORD_CONVERTER_POJO_CLASS_NAME, POJOBulletRecordConverterTest.Foo.class.getName());

        BulletRecordConverter converter = BulletRecordConverter.from(config);
        Assert.assertTrue(converter instanceof POJOBulletRecordConverter);

        POJOBulletRecordConverterTest.Foo foo = new POJOBulletRecordConverterTest.Foo();
        BulletRecord record = converter.convert(foo);

        Assert.assertEquals(record.get("myInt"), 123);
        Assert.assertEquals(record.get("myLong"), 456L);
        Assert.assertEquals(record.get("myBool"), true);
        Assert.assertEquals(record.get("myString"), "789");
        Assert.assertEquals(record.get("myDouble"), 0.12);
        Assert.assertEquals(record.get("myFloat"), 3.45f);
        Assert.assertEquals(record.get("myExcludedInt"), 678);
        Assert.assertNull(record.get("bar"));
        Assert.assertEquals(record.get("myIntMap"), foo.getMyIntMap());
        Assert.assertEquals(record.get("myIntList"), foo.getMyIntList());
    }

    @Test(expectedExceptions = RuntimeException.class)
    public void testPOJOMissingClassnameException() {
        config.set(BulletDSLConfig.RECORD_CONVERTER_CLASS_NAME, POJOBulletRecordConverter.class.getName());
        config.set(BulletDSLConfig.RECORD_CONVERTER_POJO_CLASS_NAME, "");

        BulletRecordConverter.from(config);
    }

    @Test
    public void testMap() throws Exception {
        config.set(BulletDSLConfig.RECORD_CONVERTER_CLASS_NAME, MapBulletRecordConverter.class.getName());
        config.set(BulletDSLConfig.RECORD_CONVERTER_SCHEMA_FILE, "schemas/all.json");

        BulletRecordConverter converter = BulletRecordConverter.from(config);
        Assert.assertTrue(converter instanceof MapBulletRecordConverter);

        Map<String, Object> map = new HashMap<>();
        map.put("myBool", true);
        map.put("myInt", 123);
        map.put("myLong", 456L);
        map.put("myFloat", 7.89f);
        map.put("myDouble", 0.12);
        map.put("myString", "345");
        map.put("myExcludedInt", 678);

        BulletRecord record = converter.convert(map);

        Assert.assertEquals(record.get("myBool"), map.get("myBool"));
        Assert.assertEquals(record.get("myInt"), map.get("myInt"));
        Assert.assertEquals(record.get("myLong"), map.get("myLong"));
        Assert.assertEquals(record.get("myFloat"), map.get("myFloat"));
        Assert.assertEquals(record.get("myDouble"), map.get("myDouble"));
        Assert.assertEquals(record.get("myString"), map.get("myString"));
        Assert.assertNull(record.get("myExcludedInt"));
        Assert.assertNull(record.get("dne"));
    }

    @Test(expectedExceptions = BulletDSLException.class, expectedExceptionsMessageRegExp = "Could not convert field: \\{name: myBool, reference: myBool, type: BOOLEAN, subtype: null\\}")
    public void testMapWrongType() throws Exception {
        config.set(BulletDSLConfig.RECORD_CONVERTER_CLASS_NAME, MapBulletRecordConverter.class.getName());
        config.set(BulletDSLConfig.RECORD_CONVERTER_SCHEMA_FILE, "schemas/all.json");

        BulletRecordConverter converter = BulletRecordConverter.from(config);
        Assert.assertTrue(converter instanceof MapBulletRecordConverter);

        Map<String, Object> map = new HashMap<>();
        map.put("myBool", 123);

        converter.convert(map);
    }

    @Test
    public void testAvro() throws Exception {
        config.set(BulletDSLConfig.RECORD_CONVERTER_CLASS_NAME, AvroBulletRecordConverter.class.getName());

        BulletRecordConverter converter = BulletRecordConverter.from(config);
        Assert.assertTrue(converter instanceof AvroBulletRecordConverter);

        DummyAvro dummyAvro = new DummyAvro();
        dummyAvro.setMyBool(false);
        dummyAvro.setMyDouble(0.12);
        dummyAvro.setMyFloat(3.45f);
        dummyAvro.setMyInt(678);
        dummyAvro.setMyIntList(Collections.singletonList(910));
        dummyAvro.setMyLong(1112L);
        dummyAvro.setMyString("1314");
        dummyAvro.setMyStringMap(Collections.singletonMap("1516", "1718"));

        BulletRecord record = converter.convert(dummyAvro);

        Assert.assertEquals(record.get("myBool"), dummyAvro.getMyBool());
        Assert.assertEquals(record.get("myDouble"), dummyAvro.getMyDouble());
        Assert.assertEquals(record.get("myFloat"), dummyAvro.getMyFloat());
        Assert.assertEquals(record.get("myInt"), dummyAvro.getMyInt());
        Assert.assertEquals(record.get("myIntList"), dummyAvro.getMyIntList());
        Assert.assertEquals(record.get("myLong"), dummyAvro.getMyLong());
        Assert.assertEquals(record.get("myString"), dummyAvro.getMyString());
        Assert.assertEquals(record.get("myStringMap"), dummyAvro.getMyStringMap());
        Assert.assertNull(record.get("myDummyAvro"));
    }

    @Test
    public void testGetField() {
        BulletRecordConverter converter = new MockBulletRecordConverter(null);

        List<String> list = Arrays.asList("hello", "world");
        Assert.assertEquals(converter.getField(list, "0"), "hello");
        Assert.assertEquals(converter.getField(list, "1"), "world");

        Map<String, String> map = Collections.singletonMap("hello", "world");
        Assert.assertEquals(converter.getField(map, "hello"), "world");
        Assert.assertNull(converter.getField(map, "world"));

        Assert.assertNull(converter.getField(0, "0"));
    }
}
